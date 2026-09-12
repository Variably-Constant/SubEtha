//! `PeerDirectory` - the shared topology substrate behind the
//! automatic [`AdaptiveRing`](crate::AdaptiveRing).
//!
//! One small MMF region (file / named-shm / anonymous, mirroring the
//! ring backings' three locales) that every process attached to the
//! same ring maps. It carries the facts the automatic behavior needs
//! to be cross-process instead of per-process:
//!
//! - **Peer slot claims.** Producers and consumers claim / release
//!   slot ids in shared bitmaps, so ids are unique across processes
//!   and slots recycle on release. Registration in any process is
//!   visible to every process.
//! - **Ring publication.** `published()` is the count of per-producer
//!   ring backings whose files exist and are fully initialized.
//!   A grower creates the backing files first, then advances the
//!   count (Release); openers that observe the count (Acquire) can
//!   open the files without racing initialization.
//! - **Topology epoch.** One shared counter bumped on every peer /
//!   publication change. Hot paths compare it against a process-local
//!   cache - one relaxed load per op while the topology is stable -
//!   and run the sync slow path only on change.
//! - **MPMC ring ownership.** A per-ring owner table replaces the
//!   modulus partition so the consumer set can grow and shrink at
//!   runtime. Each per-producer ring is drained by exactly one
//!   consumer slot (the Lamport cores are single-reader); ownership
//!   moves only by (a) CAS-claim of an unowned ring, (b) the current
//!   owner handing off after a rebalance request, or (c) takeover of
//!   a slot whose process is gone (pid liveness probe) or whose slot
//!   was released. (a)-(c) all keep the single-reader invariant: no
//!   two live consumers ever pop the same ring concurrently.
//!
//! The region is fixed-size: [`PRODUCER_SLOT_CEILING`] and
//! [`CONSUMER_SLOT_CEILING`] are substrate-wide architectural
//! ceilings (like the Vyukov 56-byte slot) rather than per-ring tunables -
//! per-ring limits come from the caller's declared
//! [`RingContract`](crate::ring_contract::RingContract), and only a declared
//! contract makes registration fallible. Slots recycle on release, so
//! the ceilings bound concurrent peers, not lifetime attachments.

use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomOrd};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_ring::RingError;

/// Substrate-wide ceiling on concurrently-claimed producer slots per
/// ring. Backing files are created on demand, so the ceiling costs
/// only directory bytes (one owner word + one bitmap bit per slot),
/// not ring storage.
pub const PRODUCER_SLOT_CEILING: usize = 4096;

/// Substrate-wide ceiling on concurrently-claimed consumer slots per
/// ring. Sized past the ring contract's expressible `u8` bound.
pub const CONSUMER_SLOT_CEILING: usize = 256;

/// "No consumer" marker in the ring owner table.
pub const OWNER_NONE: u16 = u16::MAX;

/// Pid word of a consumer slot that held a claim and gave it up. Distinct
/// from the zero a slot carries before it is ever claimed: a claim that
/// was released names a consumer that has finished with its rings, while
/// zero names one that never registered and may still be draining.
const SLOT_RELEASED: u64 = u64::MAX;

const DIR_MAGIC: u32 = 0x5045_4552; // `PEER` in ASCII

const P_WORDS: usize = PRODUCER_SLOT_CEILING / 64;
const C_WORDS: usize = CONSUMER_SLOT_CEILING / 64;

#[repr(C)]
struct DirHeader {
    magic: AtomicU32,
    _rsvd: u32,
    /// Topology epoch: bumped on every claim / release / publish.
    epoch: AtomicU64,
    /// Per-producer ring backings that exist and are initialized.
    published: AtomicU32,
    active_producers: AtomicU32,
    active_consumers: AtomicU32,
    _pad: u32,
}

const OFF_P_BITMAP: usize = std::mem::size_of::<DirHeader>();
const OFF_C_BITMAP: usize = OFF_P_BITMAP + P_WORDS * 8;
const OFF_C_PIDS: usize = OFF_C_BITMAP + C_WORDS * 8;
const OFF_P_PIDS: usize = OFF_C_PIDS + CONSUMER_SLOT_CEILING * 8;
const OFF_OWNERS: usize = OFF_P_PIDS + PRODUCER_SLOT_CEILING * 8;

/// Total mapped size of a peer directory region.
pub const fn peer_directory_size() -> usize {
    OFF_OWNERS + PRODUCER_SLOT_CEILING * 8
}

/// Backing-store owner; mirrors the ordering region's pattern.
#[allow(dead_code)]
enum DirBacking {
    Anon(MmapMut),
    File(File, MmapMut),
    Shm(crate::shm_file::ShmFile),
}

/// The mapped peer directory in any of the three locales.
pub struct PeerDirectory {
    _backing: DirBacking,
    raw_ptr: *mut u8,
}

unsafe impl Send for PeerDirectory {}
unsafe impl Sync for PeerDirectory {}

/// Zero the claims / counters and stamp the magic. Called exactly
/// once per region lifetime (creator side); attachers never re-init.
unsafe fn init_dir_layout(ptr: *mut u8) {
    unsafe {
        std::ptr::write_bytes(ptr, 0, peer_directory_size());
        // Owner table starts all-OWNER_NONE, not zero.
        let owners = ptr.add(OFF_OWNERS) as *mut u64;
        let none = pack_owner(OWNER_NONE, OWNER_NONE);
        for i in 0..PRODUCER_SLOT_CEILING {
            std::ptr::write(owners.add(i), none);
        }
        let header = &*(ptr as *const DirHeader);
        header.magic.store(DIR_MAGIC, AtomOrd::Release);
    }
}

#[inline]
const fn pack_owner(owner: u16, pending: u16) -> u64 {
    (owner as u64) | ((pending as u64) << 16)
}

#[inline]
const fn unpack_owner(word: u64) -> (u16, u16) {
    (word as u16, (word >> 16) as u16)
}

impl PeerDirectory {
    /// Anonymous in-process directory (the Anon ring locale).
    pub fn create_anon() -> Result<Self, RingError> {
        let mut mmap = MmapOptions::new().len(peer_directory_size()).map_anon()?;
        unsafe { init_dir_layout(mmap.as_mut_ptr()) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self { _backing: DirBacking::Anon(mmap), raw_ptr })
    }

    /// File-backed directory at `path`, initialized by the creator.
    /// Obtain the file-backed directory at `path`, initializing it only if it
    /// does not yet exist. Attaching leaves live claims in place; use
    /// [`reset`](Self::reset) to deliberately wipe them.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, RingError> {
        let total = peer_directory_size();
        let (file, mut mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            total,
            |ptr| unsafe { init_dir_layout(ptr) },
            |ptr| {
                let header = unsafe { &*(ptr as *const DirHeader) };
                header.magic.load(AtomOrd::Acquire) == DIR_MAGIC
            },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, RingError::LayoutMismatch))?;
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self { _backing: DirBacking::File(file, mmap), raw_ptr })
    }

    /// Reinitialize the directory at `path`, discarding every claim a live peer
    /// holds. For a caller that knows it owns the path.
    pub fn reset(path: impl AsRef<Path>) -> Result<Self, RingError> {
        let total = peer_directory_size();
        let (file, mut mmap) =
            crate::mmf_attach::reset(path.as_ref(), total, |ptr| unsafe { init_dir_layout(ptr) })
                .map_err(|e| RingError::IoError(e.kind()))?;
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self { _backing: DirBacking::File(file, mmap), raw_ptr })
    }

    /// Open an existing file-backed directory; validates the magic
    /// and never re-initializes (live claims survive the attach).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RingError> {
        let total = peer_directory_size();
        let file = crate::region_file::open_existing(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(RingError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let header = unsafe { &*(mmap.as_ptr() as *const DirHeader) };
        if header.magic.load(AtomOrd::Acquire) != DIR_MAGIC {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = mmap.as_ptr() as *mut u8;
        Ok(Self { _backing: DirBacking::File(file, mmap), raw_ptr })
    }

    /// Named-shm directory. `create_or_open` semantics: the region is
    /// initialized only when its magic is absent, so racing attachers
    /// never wipe live claims.
    pub fn create_or_open_shm(name: &str) -> Result<Self, RingError> {
        Self::create_or_open_shm_in(name, crate::shm_file::ShmNamespace::Session)
    }

    /// As [`create_or_open_shm`](Self::create_or_open_shm), naming the
    /// region in `namespace`.
    ///
    /// A directory carries the slot claims and the published ring count,
    /// so it belongs in the same namespace as the ring backings it
    /// describes. Backings named machine-wide with a session-scoped
    /// directory leave each side reading its own copy: both creates
    /// succeed, neither sees the other's claims, and the published count
    /// stays zero.
    pub fn create_or_open_shm_in(
        name: &str,
        namespace: crate::shm_file::ShmNamespace,
    ) -> Result<Self, RingError> {
        Self::create_or_open_shm_secured(name, namespace, None)
    }

    /// As [`create_or_open_shm_in`](Self::create_or_open_shm_in), with
    /// `sddl` as the security descriptor a create applies. A directory
    /// reached across Windows sessions needs the same descriptor as the
    /// backings it describes, or the peer resolves its name and is
    /// refused its contents.
    pub fn create_or_open_shm_secured(
        name: &str,
        namespace: crate::shm_file::ShmNamespace,
        sddl: Option<&str>,
    ) -> Result<Self, RingError> {
        let mut shm = crate::shm_file::ShmFile::create_or_open_named_secured(
            name, peer_directory_size(), namespace, sddl,
        ).map_err(|e| RingError::IoError(e.kind()))?;
        if shm.len() < peer_directory_size() {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        let dir = Self { _backing: DirBacking::Shm(shm), raw_ptr };
        let header = dir.header();
        if header.magic
            .compare_exchange(0, 1, AtomOrd::AcqRel, AtomOrd::Acquire)
            .is_ok()
        {
            // We won the init race: full layout write, then publish
            // the real magic (attachers spin below until it lands).
            unsafe { init_dir_layout(raw_ptr) };
        } else {
            let mut spins = 0u32;
            while dir.header().magic.load(AtomOrd::Acquire) != DIR_MAGIC {
                std::hint::spin_loop();
                spins += 1;
                if spins > 100_000_000 {
                    return Err(RingError::LayoutMismatch);
                }
            }
        }
        Ok(dir)
    }

    #[inline]
    fn header(&self) -> &DirHeader {
        unsafe { &*(self.raw_ptr as *const DirHeader) }
    }

    #[inline]
    fn bitmap_word(&self, base: usize, i: usize) -> &AtomicU64 {
        unsafe { &*(self.raw_ptr.add(base + i * 8) as *const AtomicU64) }
    }

    #[inline]
    fn owner_word(&self, ring: usize) -> &AtomicU64 {
        debug_assert!(ring < PRODUCER_SLOT_CEILING);
        unsafe { &*(self.raw_ptr.add(OFF_OWNERS + ring * 8) as *const AtomicU64) }
    }

    #[inline]
    fn pid_word(&self, slot: usize) -> &AtomicU64 {
        debug_assert!(slot < CONSUMER_SLOT_CEILING);
        unsafe { &*(self.raw_ptr.add(OFF_C_PIDS + slot * 8) as *const AtomicU64) }
    }

    #[inline]
    fn producer_pid_word(&self, slot: usize) -> &AtomicU64 {
        debug_assert!(slot < PRODUCER_SLOT_CEILING);
        unsafe { &*(self.raw_ptr.add(OFF_P_PIDS + slot * 8) as *const AtomicU64) }
    }

    /// Current topology epoch. Hot paths compare this against a
    /// process-local cache; equality means nothing changed.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.header().epoch.load(AtomOrd::Acquire)
    }

    /// Bump the topology epoch (any peer / publication change).
    pub fn bump_epoch(&self) -> u64 {
        self.header().epoch.fetch_add(1, AtomOrd::AcqRel) + 1
    }

    /// Ring backings published (files exist + initialized).
    #[inline]
    pub fn published(&self) -> usize {
        self.header().published.load(AtomOrd::Acquire) as usize
    }

    /// Advance the published-ring count to `to` after creating the
    /// backing files for every slot below it. Monotone max, so
    /// concurrent growers publishing different highs converge.
    pub fn publish_rings(&self, to: usize) {
        self.header().published.fetch_max(to as u32, AtomOrd::AcqRel);
        self.bump_epoch();
    }

    /// Live producer count across all attached processes.
    #[inline]
    pub fn active_producers(&self) -> usize {
        self.header().active_producers.load(AtomOrd::Acquire) as usize
    }

    /// Live consumer count across all attached processes.
    #[inline]
    pub fn active_consumers(&self) -> usize {
        self.header().active_consumers.load(AtomOrd::Acquire) as usize
    }

    /// Claim the lowest free producer slot. `None` only at the
    /// substrate ceiling ([`PRODUCER_SLOT_CEILING`] concurrent
    /// producers).
    /// The count rises before the bit is taken, and a claim that finds
    /// no free slot puts it back. Counting after the bit would let a
    /// peer publish its claim to the bitmap and not yet to the count,
    /// and a shape chosen from the count in that window is narrower
    /// than the set of peers it then has to carry - which costs an
    /// item. This order can only overstate, and a shape too wide for
    /// its peers carries them correctly.
    pub fn claim_producer_slot(&self) -> Option<usize> {
        self.header().active_producers.fetch_add(1, AtomOrd::AcqRel);
        let Some(slot) = self.claim_bit(OFF_P_BITMAP, P_WORDS) else {
            self.header().active_producers.fetch_sub(1, AtomOrd::AcqRel);
            return None;
        };
        self.producer_pid_word(slot).store(std::process::id() as u64, AtomOrd::Release);
        self.bump_epoch();
        Some(slot)
    }

    /// Release a producer slot claimed by
    /// [`claim_producer_slot`](Self::claim_producer_slot).
    pub fn release_producer_slot(&self, slot: usize) {
        if slot < PRODUCER_SLOT_CEILING {
            self.producer_pid_word(slot).store(0, AtomOrd::Release);
        }
        if self.release_bit(OFF_P_BITMAP, P_WORDS, slot) {
            self.header().active_producers.fetch_sub(1, AtomOrd::AcqRel);
            self.bump_epoch();
        }
    }

    /// Release every peer slot whose recorded process is gone
    /// (crashed / exited without unregistering). Called from the
    /// topology sync slow path only - it probes at most one pid per
    /// claimed slot. Rings owned by reaped consumer slots become
    /// claimable via the normal takeover / claim paths.
    pub fn reap_dead_peers(&self) {
        for slot in self.claimed_slots(OFF_P_BITMAP, P_WORDS) {
            let pid = self.producer_pid_word(slot).load(AtomOrd::Acquire);
            if names_a_process(pid) && pid != std::process::id() as u64
                && !process_alive(pid as u32)
            {
                #[cfg(debug_assertions)]
                crate::ring_trace::note(
                    crate::ring_trace::What::ReapedProducer,
                    usize::MAX,
                    slot,
                    (pid & 0xFFFF) as usize,
                );
                self.release_producer_slot(slot);
            }
        }
        for slot in self.claimed_slots(OFF_C_BITMAP, C_WORDS) {
            let pid = self.pid_word(slot).load(AtomOrd::Acquire);
            if names_a_process(pid) && pid != std::process::id() as u64
                && !process_alive(pid as u32)
            {
                #[cfg(debug_assertions)]
                crate::ring_trace::note(
                    crate::ring_trace::What::Reaped,
                    usize::MAX,
                    slot,
                    (pid & 0xFFFF) as usize,
                );
                self.release_consumer_slot(slot);
            }
        }
    }

    /// How many producer and consumer slots the bitmaps say are claimed.
    ///
    /// The counters [`active_producers`](Self::active_producers) and
    /// [`active_consumers`](Self::active_consumers) are what the shape
    /// policy reads, and they are maintained separately from the bitmaps
    /// a claim actually sets. A trace that records both says whether a
    /// surprising count is a counter that disagrees with the slots, or
    /// slots that genuinely were not claimed yet.
    /// Counted by popcount rather than by collecting the slots: this
    /// runs beside every shape decision in a debug build, and it is
    /// read against the count by an observer that needs its two reads
    /// close together to see the window between them at all.
    pub fn claimed_populations(&self) -> (usize, usize) {
        (
            self.bitmap_population(OFF_P_BITMAP, P_WORDS),
            self.bitmap_population(OFF_C_BITMAP, C_WORDS),
        )
    }

    fn bitmap_population(&self, base: usize, words: usize) -> usize {
        (0..words)
            .map(|w| self.bitmap_word(base, w).load(AtomOrd::Acquire).count_ones() as usize)
            .sum()
    }

    fn claimed_slots(&self, base: usize, words: usize) -> Vec<usize> {
        let mut slots = Vec::new();
        for w in 0..words {
            let mut word = self.bitmap_word(base, w).load(AtomOrd::Acquire);
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                slots.push(w * 64 + bit);
                word &= word - 1;
            }
        }
        slots
    }

    /// Claim the lowest free consumer slot, recording the claiming
    /// process id for the crash-takeover liveness probe. Counted before
    /// the bit for the reason
    /// [`claim_producer_slot`](Self::claim_producer_slot) gives.
    pub fn claim_consumer_slot(&self) -> Option<usize> {
        self.header().active_consumers.fetch_add(1, AtomOrd::AcqRel);
        let Some(slot) = self.claim_bit(OFF_C_BITMAP, C_WORDS) else {
            self.header().active_consumers.fetch_sub(1, AtomOrd::AcqRel);
            return None;
        };
        self.pid_word(slot).store(std::process::id() as u64, AtomOrd::Release);
        self.bump_epoch();
        Some(slot)
    }

    /// Release a consumer slot. The caller transfers its ring
    /// ownership out first (it is the single owner, so direct owner
    /// writes are safe), then releases.
    pub fn release_consumer_slot(&self, slot: usize) {
        if slot >= CONSUMER_SLOT_CEILING {
            return;
        }
        self.pid_word(slot).store(SLOT_RELEASED, AtomOrd::Release);
        if self.release_bit(OFF_C_BITMAP, C_WORDS, slot) {
            self.header().active_consumers.fetch_sub(1, AtomOrd::AcqRel);
            self.bump_epoch();
        }
    }

    /// Assert the rule the reaper rests on: a consumer slot whose bit is
    /// clear names no process, because a release writes the sentinel
    /// while the bit is still set. A slot that breaks it is one some
    /// release wrote in the other order, and the reaper would then be
    /// free to reclaim a slot whose holder is still running.
    ///
    /// Call it with the churn quiesced - every thread that claims or
    /// releases joined. Under live churn a slot legitimately passes
    /// through states this rejects.
    #[cfg(debug_assertions)]
    pub fn assert_free_slots_name_no_process(&self) {
        for slot in 0..CONSUMER_SLOT_CEILING {
            if self.consumer_slot_claimed(slot) {
                continue;
            }
            let pid = self.pid_word(slot).load(AtomOrd::Acquire);
            assert!(
                !names_a_process(pid),
                "consumer slot {slot} is free but still names process \
                 {pid}: some release cleared the bit before writing the \
                 sentinel, which lets the reaper free a live claim",
            );
        }
    }

    /// Whether `slot` currently holds a consumer claim.
    pub fn consumer_slot_claimed(&self, slot: usize) -> bool {
        if slot >= CONSUMER_SLOT_CEILING {
            return false;
        }
        let word = self.bitmap_word(OFF_C_BITMAP, slot / 64).load(AtomOrd::Acquire);
        word & (1u64 << (slot % 64)) != 0
    }

    /// Dense list of currently-claimed consumer slots (rebalance
    /// input). Snapshot semantics: claims racing the scan are picked
    /// up by the next epoch-triggered rebalance.
    pub fn claimed_consumer_slots(&self) -> Vec<u16> {
        let mut slots = Vec::new();
        for w in 0..C_WORDS {
            let mut word = self.bitmap_word(OFF_C_BITMAP, w).load(AtomOrd::Acquire);
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                slots.push((w * 64 + bit) as u16);
                word &= word - 1;
            }
        }
        slots
    }

    /// `(owner, pending)` for one ring's owner-table entry.
    #[inline]
    pub fn ring_owner(&self, ring: usize) -> (u16, u16) {
        unpack_owner(self.owner_word(ring).load(AtomOrd::Acquire))
    }

    /// CAS-claim an unowned ring for `me`. The only ownership entry
    /// point that does not go through the current owner, and it
    /// requires owner == OWNER_NONE, so the single-reader invariant
    /// holds by construction.
    pub fn try_claim_ring(&self, ring: usize, me: u16) -> bool {
        let cur = pack_owner(OWNER_NONE, OWNER_NONE);
        self.owner_word(ring)
            .compare_exchange(cur, pack_owner(me, OWNER_NONE),
                              AtomOrd::AcqRel, AtomOrd::Acquire)
            .is_ok()
    }

    /// Request that `ring` move to `target`. The current owner
    /// applies the handoff on its next scan ([`Self::apply_handoff`]);
    /// until then it keeps draining, so no items strand.
    pub fn request_handoff(&self, ring: usize, target: u16) {
        let word = self.owner_word(ring);
        let mut cur = word.load(AtomOrd::Acquire);
        loop {
            let (owner, _) = unpack_owner(cur);
            if owner == target || owner == OWNER_NONE {
                return; // already there / claimable directly
            }
            match word.compare_exchange(cur, pack_owner(owner, target),
                                        AtomOrd::AcqRel, AtomOrd::Acquire) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Owner-side handoff: if `me` owns `ring` and a handoff is
    /// pending, transfer ownership and return the new owner. Called
    /// from the owner's own pop scan - the single-writer transfer.
    pub fn apply_handoff(&self, ring: usize, me: u16) -> Option<u16> {
        let word = self.owner_word(ring);
        let cur = word.load(AtomOrd::Acquire);
        let (owner, pending) = unpack_owner(cur);
        if owner != me || pending == OWNER_NONE {
            return None;
        }
        match word.compare_exchange(cur, pack_owner(pending, OWNER_NONE),
                                    AtomOrd::AcqRel, AtomOrd::Acquire) {
            Ok(_) => {
                self.bump_epoch();
                Some(pending)
            }
            Err(_) => None,
        }
    }

    /// Direct ownership transfer by the current owner (unregister
    /// path: the leaving consumer parcels its rings out itself).
    pub fn transfer_ring(&self, ring: usize, me: u16, to: u16) {
        let word = self.owner_word(ring);
        let mut cur = word.load(AtomOrd::Acquire);
        loop {
            let (owner, _) = unpack_owner(cur);
            if owner != me {
                return;
            }
            match word.compare_exchange(cur, pack_owner(to, OWNER_NONE),
                                        AtomOrd::AcqRel, AtomOrd::Acquire) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Crash takeover: steal `ring` from `dead_owner` when that owner
    /// released its consumer slot, or still holds one whose recorded
    /// process is gone. An owner that never claimed a slot, or whose
    /// process is alive, is left alone, so each core keeps a single
    /// reader.
    pub fn try_takeover(&self, ring: usize, dead_owner: u16, me: u16) -> bool {
        let slot = dead_owner as usize;
        let pid = self.pid_word(slot).load(AtomOrd::Acquire);
        if pid != SLOT_RELEASED {
            if !self.consumer_slot_claimed(slot) {
                return false;
            }
            if pid == std::process::id() as u64 || pid == 0 || process_alive(pid as u32) {
                return false;
            }
        }
        let word = self.owner_word(ring);
        let cur = word.load(AtomOrd::Acquire);
        let (owner, _) = unpack_owner(cur);
        if owner != dead_owner {
            return false;
        }
        let swapped = word.compare_exchange(
            cur, pack_owner(me, OWNER_NONE), AtomOrd::AcqRel, AtomOrd::Acquire,
        ).is_ok();
        if swapped {
            self.bump_epoch();
        }
        swapped
    }

    fn claim_bit(&self, base: usize, words: usize) -> Option<usize> {
        for w in 0..words {
            let word = self.bitmap_word(base, w);
            let mut cur = word.load(AtomOrd::Acquire);
            loop {
                if cur == u64::MAX {
                    break; // word full; next word
                }
                let bit = (!cur).trailing_zeros() as usize;
                match word.compare_exchange(cur, cur | (1u64 << bit),
                                            AtomOrd::AcqRel, AtomOrd::Acquire) {
                    Ok(_) => return Some(w * 64 + bit),
                    Err(actual) => cur = actual,
                }
            }
        }
        None
    }

    fn release_bit(&self, base: usize, words: usize, slot: usize) -> bool {
        if slot >= words * 64 {
            return false;
        }
        let word = self.bitmap_word(base, slot / 64);
        let mask = 1u64 << (slot % 64);
        word.fetch_and(!mask, AtomOrd::AcqRel) & mask != 0
    }
}

/// Whether a slot's pid word names a process at all.
///
/// Zero is a slot never claimed, `SLOT_RELEASED` one that was given up.
/// A slot reads one of those two for as long as its claimer sits between
/// taking the bitmap bit and storing its own pid, because the claim
/// writes the bit first. Nothing but a release writes either value, and
/// a release writes it while the bit is still set, so a slot whose bit
/// is clear always carries one of them and a slot abandoned by a crash
/// always carries a real pid. Acting on a slot that names no process
/// therefore frees a claim whose holder is still running, and the next
/// claim hands that same slot to a second thread.
fn names_a_process(pid: u64) -> bool {
    pid != 0 && pid != SLOT_RELEASED
}

/// Whether the OS process `pid` is alive. Used only by the crash
/// takeover; a false alive reading (pid reuse) is the benign direction (the
/// ring stays with the stale owner until an explicit release).
#[cfg(unix)]
pub(crate) fn process_alive(pid: u32) -> bool {
    // A value that does not fit a positive pid names no process. Passed
    // to kill as it stands, u32::MAX is -1, which names every process the
    // caller may signal and so answers alive, and a zero names the
    // caller's own group; either would keep a corrupt slot forever.
    let pid = match libc::pid_t::try_from(pid) {
        Ok(p) if p > 0 => p,
        Ok(_not_positive) => return false,
        Err(_past_pid_t) => return false,
    };
    let rc = unsafe { libc::kill(pid, 0) };
    rc == 0
        || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
pub(crate) fn process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            // Access denied still proves existence; a missing pid
            // yields ERROR_INVALID_PARAMETER instead.
            return std::io::Error::last_os_error().raw_os_error()
                == Some(5 /* ERROR_ACCESS_DENIED */);
        }
        let mut code: u32 = 0;
        let ok = GetExitCodeProcess(handle, &mut code);
        CloseHandle(handle);
        ok != 0 && code == STILL_ACTIVE as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_slots_claim_release_recycle() {
        let dir = PeerDirectory::create_anon().unwrap();
        assert_eq!(dir.claim_producer_slot(), Some(0));
        assert_eq!(dir.claim_producer_slot(), Some(1));
        assert_eq!(dir.claim_producer_slot(), Some(2));
        assert_eq!(dir.active_producers(), 3);
        dir.release_producer_slot(1);
        assert_eq!(dir.active_producers(), 2);
        // Lowest free slot recycles.
        assert_eq!(dir.claim_producer_slot(), Some(1));
        assert_eq!(dir.active_producers(), 3);
    }

    /// While peers are arriving, the count a shape decision reads is
    /// never below the number of slots the bitmaps say are claimed.
    /// Reading it lower picks a shape too narrow for the peers already
    /// registered, and a single-producer shape under several producers
    /// costs an item.
    ///
    /// The observer reads the bitmaps first and the count second, which
    /// is the order that makes the claim ordering observable: a claim
    /// raises the count and then takes the bit, so a bit seen at some
    /// instant was counted before it, and a count read after that
    /// instant must include it. Reading the count first would let a
    /// claim land between the two halves and fail a sound
    /// implementation.
    ///
    /// Departures are left out on purpose. A release clears the bit and
    /// then lowers the count, so the pair legitimately reads the other
    /// way round for that window, and the arrival path is where the
    /// shape is chosen too narrow.
    ///
    /// Correct code cannot fail this: the invariant holds at every
    /// instant, so there is no sample that breaks it. Catching a
    /// reintroduction is the part that depends on landing in the
    /// window - measured on the Linux gate host, the counted-second
    /// order failed 11 of 16 runs while this order passed 11 of 11.
    #[test]
    fn an_arriving_peer_is_counted_before_it_is_visible_in_the_bitmap() {
        use std::sync::atomic::{AtomicBool, AtomicI64, Ordering as O};

        // Sized to the producer ceiling: no releases, so every claim has
        // to have a slot to take. The window an observer is trying to
        // land in is two instructions wide, so the run needs thousands
        // of arrivals rather than a handful to be worth running at all.
        const THREADS: usize = 4;
        const PER_THREAD: usize = PRODUCER_SLOT_CEILING / (THREADS * 2);

        let dir = std::sync::Arc::new(PeerDirectory::create_anon().unwrap());
        let worst = std::sync::Arc::new(AtomicI64::new(i64::MAX));
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS + 1));

        let mut arriving = Vec::new();
        for _ in 0..THREADS {
            let dir = dir.clone();
            let start = start.clone();
            arriving.push(std::thread::spawn(move || {
                let mut mine = Vec::new();
                start.wait();
                for _ in 0..PER_THREAD {
                    if let Some(p) = dir.claim_producer_slot() {
                        mine.push(p);
                    }
                }
                mine
            }));
        }

        // Sampling runs for as long as peers are arriving, rather than
        // for a fixed number of reads that may all fall before or after
        // the arrivals.
        let dir_o = dir.clone();
        let worst_o = worst.clone();
        let done_o = done.clone();
        let start_o = start.clone();
        let observer = std::thread::spawn(move || {
            start_o.wait();
            while !done_o.load(O::Relaxed) {
                for _ in 0..64 {
                    let (claimed, _) = dir_o.claimed_populations();
                    let counted = dir_o.active_producers() as i64;
                    worst_o.fetch_min(counted - claimed as i64, O::Relaxed);
                }
            }
        });

        let mut claimed_slots = Vec::new();
        for t in arriving {
            claimed_slots.extend(t.join().unwrap());
        }
        done.store(true, O::Relaxed);
        observer.join().unwrap();

        let worst = worst.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            worst >= 0,
            "the producer count read {worst} below the claimed slots; a shape \
             chosen from that count is narrower than the peers registered",
        );
        assert_eq!(
            dir.active_producers(),
            claimed_slots.len(),
            "every claim that returned a slot is counted once",
        );
        for slot in claimed_slots {
            dir.release_producer_slot(slot);
        }
        assert_eq!(dir.active_producers(), 0, "and every release gives it back");
    }

    #[test]
    fn epoch_bumps_on_every_topology_change() {
        let dir = PeerDirectory::create_anon().unwrap();
        let e0 = dir.epoch();
        let p = dir.claim_producer_slot().unwrap();
        assert!(dir.epoch() > e0);
        let e1 = dir.epoch();
        dir.release_producer_slot(p);
        assert!(dir.epoch() > e1);
        let e2 = dir.epoch();
        dir.publish_rings(4);
        assert!(dir.epoch() > e2);
        assert_eq!(dir.published(), 4);
        // publish is monotone max.
        dir.publish_rings(2);
        assert_eq!(dir.published(), 4);
    }

    #[test]
    fn ring_ownership_claim_handoff_transfer() {
        let dir = PeerDirectory::create_anon().unwrap();
        assert_eq!(dir.ring_owner(0), (OWNER_NONE, OWNER_NONE));
        assert!(dir.try_claim_ring(0, 3));
        assert!(!dir.try_claim_ring(0, 5), "claimed ring must reject CAS");
        assert_eq!(dir.ring_owner(0), (3, OWNER_NONE));

        // Rebalance request parks as pending until the owner applies.
        dir.request_handoff(0, 7);
        assert_eq!(dir.ring_owner(0), (3, 7));
        assert_eq!(dir.apply_handoff(0, 5), None, "non-owner cannot apply");
        assert_eq!(dir.apply_handoff(0, 3), Some(7));
        assert_eq!(dir.ring_owner(0), (7, OWNER_NONE));

        // Unregister-path direct transfer by the owner.
        dir.transfer_ring(0, 7, 2);
        assert_eq!(dir.ring_owner(0), (2, OWNER_NONE));
    }

    #[test]
    fn takeover_requires_dead_or_released_owner() {
        let dir = PeerDirectory::create_anon().unwrap();
        let slot = dir.claim_consumer_slot().unwrap() as u16;
        assert!(dir.try_claim_ring(0, slot));
        // The claiming slot belongs to this live process: never stolen.
        assert!(!dir.try_takeover(0, slot, 9));
        // Released slot: takeover permitted.
        dir.release_consumer_slot(slot as usize);
        assert!(dir.try_takeover(0, slot, 9));
        assert_eq!(dir.ring_owner(0), (9, OWNER_NONE));
    }

    /// A claim takes the bitmap bit first and stores its pid second, so
    /// a slot that has been claimed and released once spends the gap
    /// between those two writes claimed with the released sentinel still
    /// in its pid word. The reaper walks claimed slots, and that
    /// sentinel is neither zero nor this process, so a reaper that acts
    /// on it frees a slot whose holder is still running - and the next
    /// claim then hands the same slot to a second thread, putting two
    /// readers on a core whose pop is single-reader by contract.
    #[test]
    fn reaper_spares_a_slot_claimed_but_not_yet_identified() {
        let dir = PeerDirectory::create_anon().unwrap();
        let slot = dir.claim_consumer_slot().unwrap();
        dir.release_consumer_slot(slot);
        assert_eq!(dir.pid_word(slot).load(AtomOrd::Acquire), SLOT_RELEASED);

        // Exactly the state a claimer is in between its two writes.
        assert_eq!(dir.claim_bit(OFF_C_BITMAP, C_WORDS), Some(slot));
        dir.reap_dead_peers();
        assert!(
            dir.consumer_slot_claimed(slot),
            "reaper freed a slot whose claimer had not yet stored its pid",
        );

        // A slot abandoned by a crash still carries a real pid, so the
        // reaper must keep reclaiming those.
        dir.pid_word(slot).store(u32::MAX as u64, AtomOrd::Release);
        dir.reap_dead_peers();
        assert!(
            !dir.consumer_slot_claimed(slot),
            "reaper left a slot held by a process that is gone",
        );
    }

    /// A consumer that drains through `try_claim_ring` owns rings without
    /// ever claiming a consumer slot, which is what every in-process
    /// consumer does. Such an owner is live and popping, so its rings are
    /// not available for takeover; stealing one puts a second reader on a
    /// single-reader core and both drain the same slots.
    #[test]
    fn an_owner_that_never_claimed_a_slot_is_not_taken_over() {
        let dir = PeerDirectory::create_anon().unwrap();
        let owner: u16 = 3;
        assert!(!dir.consumer_slot_claimed(owner as usize));
        assert!(dir.try_claim_ring(0, owner), "ring claim needs no slot");

        assert!(
            !dir.try_takeover(0, owner, 9),
            "an owner with no slot claim is live, not departed"
        );
        assert_eq!(dir.ring_owner(0), (owner, OWNER_NONE));

        // The same owner having held and released a slot is departed.
        let slot = dir.claim_consumer_slot().unwrap();
        dir.release_consumer_slot(slot);
        assert!(dir.try_claim_ring(1, slot as u16));
        assert!(dir.try_takeover(1, slot as u16, 9));
        assert_eq!(dir.ring_owner(1), (9, OWNER_NONE));
    }

    #[test]
    fn consumer_slots_record_pid_and_enumerate_densely() {
        let dir = PeerDirectory::create_anon().unwrap();
        let a = dir.claim_consumer_slot().unwrap();
        let b = dir.claim_consumer_slot().unwrap();
        let c = dir.claim_consumer_slot().unwrap();
        assert_eq!((a, b, c), (0, 1, 2));
        dir.release_consumer_slot(b);
        assert_eq!(dir.claimed_consumer_slots(), vec![0u16, 2]);
        assert_eq!(dir.active_consumers(), 2);
    }

    #[test]
    fn file_backed_directory_shares_claims_across_handles() {
        let base = std::env::temp_dir()
            .join(format!("subetha_peerdir_{}", std::process::id()));
        let path = base.with_extension("peers.bin");
        let dir_a = PeerDirectory::create(&path).unwrap();
        let p = dir_a.claim_producer_slot().unwrap();
        dir_a.publish_rings(p + 1);

        let dir_b = PeerDirectory::open(&path).unwrap();
        assert_eq!(dir_b.active_producers(), 1);
        assert_eq!(dir_b.published(), p + 1);
        assert_eq!(dir_b.claim_producer_slot(), Some(p + 1));
        assert_eq!(dir_a.active_producers(), 2, "claim visible both ways");

        drop(dir_a);
        drop(dir_b);
        std::fs::remove_file(&path).ok();
    }
}
