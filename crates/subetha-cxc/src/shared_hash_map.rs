//! `SharedHashMap<K, V>` - cross-process open-addressed hash map
//! backed by a single MMF file.
//!
//! # Why open addressing?
//!
//! All storage is inline. No allocator, no pointer indirection. Each
//! slot lives in its own cache line; the entire table is a flat
//! array in the MMF. Robin Hood, linear, and quadratic probing all
//! work; we use **linear probing** because it's the most cache-
//! friendly on modern CPUs (sequential access dominates probe-
//! variance on speculative-prefetch architectures).
//!
//! # Stable hashing
//!
//! `std::hash::BuildHasher` uses a per-process random seed for DoS
//! resistance, which would make keys irreproducible across
//! processes. We use **FNV-1a** over the key bytes - fast, deps-free,
//! and deterministic across processes / runs / OSes.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | MapHeader (64B)           |
//! |   magic, capacity, count  |
//! |   key_size, value_size    |
//! +---------------------------+
//! | Slot[0]  (64B cache line) |
//! |   state (empty/occ/ts)    |
//! |   version (SeqLock)       |
//! |   hash (cached)           |
//! |   payload [u8; 48]: K + V |
//! | Slot[1] ...               |
//! +---------------------------+
//! ```
//!
//! # Protocol
//!
//! ## Insert
//! 1. Hash key (FNV-1a; a hash of 0 is remapped to 1, since 0 marks
//!    a slot whose contents are not yet published).
//! 2. Read the header's `removals` count, then probe from
//!    `hash % capacity`, linearly.
//! 3. At each slot:
//!    - **Empty**: the key is absent. CAS the first tombstone passed,
//!      else this Empty, to Occupied. If `removals` has moved since step
//!      2, give the slot back as a tombstone and start again; otherwise
//!      SeqLock-write `(K, V)`, and only then store the hash as the
//!      publish; bump `count`. Return Inserted.
//!    - **Occupied & hash 0**: a writer holds the slot and has not
//!      published it. Spin until the hash lands, then compare; a claim
//!      given back is a tombstone.
//!    - **Occupied & hash matches & key matches**: take the slot's lock,
//!      confirm it is still the key's published entry, write V, release
//!      (state unchanged). Return Updated. An entry removed in between
//!      sends the probe back to step 2.
//!    - **Occupied & no match**: probe next slot.
//!    - **Tombstone**: track the first one and keep probing (a later
//!      slot may hold the key).
//!
//! ## Get
//! 1. Hash key, probe linearly.
//! 2. **Empty**: key absent (probe always terminates at Empty).
//! 3. **Occupied & hash 0**: forming; spin until published.
//! 4. **Occupied & hash matches**: SeqLock-read; if K matches, return V.
//! 5. **Tombstone or hash mismatch**: continue probing.
//!
//! ## Remove
//! 1. Find key (same probe).
//! 2. Take the slot's lock and confirm it is still the key's published
//!    entry; bump `removals`, CAS state Occupied → Tombstone, clear the
//!    hash to 0, release. `count.fetch_sub(1)`.
//!
//! # Concurrency
//!
//! A slot is published in two steps that readers observe in order:
//! the payload lands under the SeqLock, then the hash lands as the
//! Release. A prober that finds a slot Occupied with hash 0 has
//! caught a writer between its claim and its publish, and waits
//! rather than concluding the slot holds a different key. The state
//! byte's CAS decides who owns a slot for its claim; the SeqLock
//! version is claimed by CAS even→odd for every write, so two writers
//! updating one key take turns rather than overlapping. Readers never
//! observe a torn key+value.
//!
//! Every write to a published entry (an update, `swap`,
//! `compare_exchange`, `remove`) takes the slot's lock and confirms
//! the state, hash and key under it, so it acts on the entry it
//! matched or on nothing, and a value leaves the map exactly once.
//!
//! One key holds one slot. Two inserts of a new key that walk the
//! same chain meet at its first Empty or first tombstone, unless a
//! remove opens a slot one of them has already passed: the other can
//! then claim that slot while the first claims further on. A remove
//! therefore bumps `removals` before its tombstone lands, and an
//! insert whose claim finds the count moved since its walk began gives
//! the claim back and walks again, which brings it to the other's
//! entry.
//!
//! # Capacity and load factor
//!
//! Fixed at create time. Recommend `capacity = 2 * expected_max`
//! to keep load factor below 0.5; linear probing degrades sharply
//! above 0.7. `insert` returns `MapError::Full` when the probe
//! chain saturates.
//!
//! # If you need dynamic sizing, use `SharedUniversal`
//!
//! `SharedHashMap` deliberately leaves resize-on-grow unimplemented.
//! Cross-process resize requires the same reader-coordination
//! machinery as MMF-backed migration (atomic file rename, reader
//! re-open signaling). Rather than reinvent that machinery inside
//! `SharedHashMap`, callers who need a dynamically-resizing hash
//! map should use [`crate::shared_universal::SharedUniversal<T>`]
//! configured with hash-map-only backings. The migration mechanism
//! handles cross-process resize correctly, with the reader-side
//! generation-bump protocol that makes wrap-around safe.

use std::fs::File;
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const MAP_MAGIC: u32 = 0x4150_484D;
pub const MAP_PAYLOAD_BYTES: usize = 48;

pub const SLOT_EMPTY: u8 = 0;
pub const SLOT_OCCUPIED: u8 = 1;
pub const SLOT_TOMBSTONE: u8 = 2;

#[repr(C, align(64))]
pub struct MapHeader {
    pub magic: u32,
    pub capacity: u32,
    pub count: AtomicU64,
    pub key_size: u32,
    pub value_size: u32,
    /// Monotonic counter of tombstones currently in the table.
    /// Bumped by `remove`, zeroed by `compact`. Used by callers
    /// (e.g. `SharedLRUCache`) to decide when to compact.
    pub tombstones: AtomicU64,
    /// Removes so far, each counted before its tombstone lands. An insert
    /// of a new key reads it before its walk and again after its claim,
    /// and gives the claim back when it moved.
    pub removals: AtomicU64,
    _pad: [u8; 24],
}

#[repr(C, align(64))]
pub struct MapSlot {
    pub state: AtomicU8,
    _pad1: [u8; 3],
    pub version: AtomicU32,
    pub hash: AtomicU64,
    pub payload: [u8; MAP_PAYLOAD_BYTES],
}

const _: () = {
    assert!(size_of::<MapHeader>() == 64);
    assert!(size_of::<MapSlot>() == 64);
};

pub const fn map_file_size(capacity: usize) -> usize {
    size_of::<MapHeader>() + capacity * size_of::<MapSlot>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapError {
    Full,
    PayloadTooLarge,
    LayoutMismatch,
    /// [`compare_exchange`](SharedHashMap::compare_exchange) found no
    /// entry for the key.
    KeyAbsent,
    IoError(std::io::ErrorKind),
}

/// A slot's hash word while its contents are not yet published: the
/// claim has landed and the payload has not. Probers wait on it.
pub const HASH_UNSET: u64 = 0;

impl From<std::io::Error> for MapError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Updated,
}

/// Where a probe left the key: in a slot claimed by this call, or in a
/// slot that already held it, whose value is carried out.
enum Placed<V> {
    New,
    Existing(V),
}

/// One probe pass's verdict.
enum Probe<V> {
    Placed(Placed<V>),
    /// A claim went to another writer or was given back, or the key's
    /// entry was removed under the probe; probe again.
    Restart,
    Full,
}

/// What an occupied slot says about the key a probe is placing.
enum Found<V> {
    /// The slot holds the key: its value, the one replaced when the probe
    /// overwrites.
    Here(V),
    /// The slot holds another key.
    Elsewhere,
    /// The slot stopped being occupied while the probe waited on it, a
    /// claim given back or a remove, and is a tombstone the probe may take.
    Vacated,
    /// The slot held the key when the probe matched its hash and no longer
    /// did under its lock.
    Moved,
}

/// How a claim on a free slot ended.
enum Claim {
    Placed,
    /// Another writer took the slot first.
    Lost,
    /// A remove landed during the walk, so the slot was given back.
    GaveBack,
}

/// FNV-1a 64-bit over a byte slice. Deterministic across processes
/// (unlike `std::hash::BuildHasher` which uses per-process random
/// seeds for DoS resistance).
#[inline]
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

pub struct SharedHashMap<K: Copy + Eq + 'static, V: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    /// `capacity - 1`, valid only when `cap_is_pow2`.
    cap_mask: usize,
    /// True when `capacity` is a power of two, so slot reduction can use
    /// `& cap_mask` instead of a `% capacity` hardware DIV. The probe
    /// loop reduces twice per step (start + each probe), so this removes
    /// the DIV from the hash-map hot path when capacity is pow2.
    cap_is_pow2: bool,
    _phantom: PhantomData<(K, V)>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<K: Copy + Eq + Send + 'static, V: Copy + Send + 'static> Send for SharedHashMap<K, V> {}
unsafe impl<K: Copy + Eq + Sync + 'static, V: Copy + Sync + 'static> Sync for SharedHashMap<K, V> {}

impl<K: Copy + Eq + Send + Sync + 'static, V: Copy + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for SharedHashMap<K, V>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<K: Copy + Eq + 'static, V: Copy + 'static> SharedHashMap<K, V> {
    fn check_layout() -> Result<(), MapError> {
        if size_of::<K>() + size_of::<V>() > MAP_PAYLOAD_BYTES {
            return Err(MapError::PayloadTooLarge);
        }
        Ok(())
    }

    /// Obtain the map at `path`, initializing an empty one if the path does
    /// not yet exist and attaching to it if it does. Attaching leaves live
    /// entries in place; a region built with a different capacity or
    /// payload type is a `LayoutMismatch`. [`reset`](Self::reset)
    /// reinitializes.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, MapError> {
        Self::check_layout()?;
        assert!(capacity >= 2);
        let total = map_file_size(capacity);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            total,
            |ptr| unsafe { Self::init_region(ptr, capacity) },
            |ptr| unsafe { (*(ptr as *const MapHeader)).magic == MAP_MAGIC },
        )
        .map_err(|e| {
            if crate::mmf_attach::is_size_mismatch(&e) {
                MapError::LayoutMismatch
            } else {
                // The raw OS error, before it is thrown away. MapError
                // is Copy and carries only an ErrorKind, and ErrorKind
                // is a lossy projection: StorageFull is reached from
                // more than one Win32 code, and which one it is decides
                // where to look. A caller holding the MapError cannot
                // recover it, so it is said here or not at all.
                eprintln!(
                    "subetha-cxc: the region at {} ({total} bytes) could not be obtained: \
                     {e} (kind {:?}, os error {:?})",
                    path.as_ref().display(),
                    e.kind(),
                    e.raw_os_error(),
                );
                MapError::from(e)
            }
        })?;
        Self::from_region(file, mmap, capacity)
    }

    /// Truncate the map at `path` and initialize a fresh empty one,
    /// discarding whatever entries a live peer holds. For a caller that
    /// knows it owns the path.
    pub fn reset(path: impl AsRef<Path>, capacity: usize) -> Result<Self, MapError> {
        Self::check_layout()?;
        assert!(capacity >= 2);
        let total = map_file_size(capacity);
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), total, |ptr| unsafe {
            Self::init_region(ptr, capacity)
        })?;
        Self::from_region(file, mmap, capacity)
    }

    /// Lay out an empty map: header fields first, magic last, because
    /// attachers spin on the magic and must not observe it before the
    /// layout fields are in place. The zeroed region is already the valid
    /// slot array (`SLOT_EMPTY`, version 0, hash 0) and the valid `count`
    /// and `tombstones` of 0.
    ///
    /// # Safety
    /// `ptr` addresses at least `map_file_size(capacity)` writable zeroed
    /// bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize) {
        let hdr = ptr as *mut MapHeader;
        unsafe {
            (*hdr).capacity = capacity as u32;
            (*hdr).key_size = size_of::<K>() as u32;
            (*hdr).value_size = size_of::<V>() as u32;
            std::ptr::write_volatile(&raw mut (*hdr).magic, MAP_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one whose layout does not
    /// match this type at this capacity.
    fn from_region(file: File, mmap: MmapMut, capacity: usize) -> Result<Self, MapError> {
        let hdr = unsafe { &*(mmap.as_ptr() as *const MapHeader) };
        if hdr.magic != MAP_MAGIC
            || hdr.capacity != capacity as u32
            || hdr.key_size != size_of::<K>() as u32
            || hdr.value_size != size_of::<V>() as u32
        {
            return Err(MapError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity,
            cap_mask: capacity.wrapping_sub(1),
            cap_is_pow2: capacity.is_power_of_two(),
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, MapError> {
        Self::check_layout()?;
        let total = map_file_size(expected_capacity);
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(MapError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::from_region(file, mmap, expected_capacity)
    }

    /// Reduce an index into `[0, capacity)`. Uses `& cap_mask` when
    /// capacity is a power of two (the common case) - removing the
    /// `% capacity` hardware DIV the linear-probe loop would otherwise
    /// run on every step - and falls back to the modulo otherwise.
    /// A bit-mask is required (not Lemire-style multiply-shift) because
    /// linear probing needs consecutive indices to map to consecutive
    /// slots with wraparound.
    #[inline]
    fn wrap(&self, i: usize) -> usize {
        if self.cap_is_pow2 { i & self.cap_mask } else { i % self.capacity }
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    pub fn len(&self) -> usize {
        self.header().count.load(Ordering::Acquire) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    fn header(&self) -> &MapHeader {
        unsafe { &*(self.mmap.as_ptr() as *const MapHeader) }
    }

    fn slot(&self, idx: usize) -> &MapSlot {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<MapHeader>()) };
        unsafe { &*(base.add(idx * size_of::<MapSlot>()) as *const MapSlot) }
    }

    /// FNV-1a over the key bytes, with 0 remapped to 1: 0 is
    /// [`HASH_UNSET`], the mark of a claimed slot not yet published.
    fn hash_key(k: &K) -> u64 {
        let bytes = unsafe {
            std::slice::from_raw_parts(k as *const K as *const u8, size_of::<K>())
        };
        match fnv1a_64(bytes) {
            HASH_UNSET => 1,
            h => h,
        }
    }

    /// Take a slot's SeqLock for writing: CAS the version from even to
    /// odd, spinning while another writer holds it. Returns the odd
    /// version to release with [`unlock_slot`](Self::unlock_slot).
    #[inline]
    fn lock_slot(&self, slot: &MapSlot) -> u32 {
        loop {
            let v = slot.version.load(Ordering::Acquire);
            if v & 1 == 0
                && slot
                    .version
                    .compare_exchange_weak(v, v + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return v + 1;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn unlock_slot(&self, slot: &MapSlot, held: u32) {
        slot.version.store(held + 1, Ordering::Release);
    }

    #[inline]
    fn payload_ptr(&self, slot_idx: usize) -> *mut u8 {
        unsafe {
            self.mmap
                .as_ptr()
                .add(size_of::<MapHeader>())
                .add(slot_idx * size_of::<MapSlot>())
                .add(std::mem::offset_of!(MapSlot, payload)) as *mut u8
        }
    }

    /// Copy `(k, v)` into a slot's payload region; the caller holds the
    /// slot's SeqLock.
    unsafe fn copy_payload(&self, slot_idx: usize, k: &K, v: &V) {
        let base = self.payload_ptr(slot_idx);
        unsafe {
            // Layout: key bytes then value bytes.
            std::ptr::copy_nonoverlapping(k as *const K as *const u8, base, size_of::<K>());
            std::ptr::copy_nonoverlapping(
                v as *const V as *const u8,
                base.add(size_of::<K>()),
                size_of::<V>(),
            );
        }
    }

    /// SeqLock-write a (K, V) pair into a slot's payload region. Writers
    /// to one slot take turns on the lock rather than overlapping.
    fn write_payload(&self, slot_idx: usize, k: &K, v: &V) {
        let slot = self.slot(slot_idx);
        let held = self.lock_slot(slot);
        unsafe { self.copy_payload(slot_idx, k, v) };
        self.unlock_slot(slot, held);
    }

    /// Publish a freshly claimed slot: the payload under the lock, then
    /// the hash as the Release a prober waits on.
    fn publish_claimed(&self, slot_idx: usize, h: u64, k: &K, v: &V) {
        self.write_payload(slot_idx, k, v);
        self.slot(slot_idx).hash.store(h, Ordering::Release);
    }

    /// A claimed slot's hash, waiting out a writer that has claimed the
    /// slot and not yet published it. Every probe reads through this, so
    /// no probe concludes "a different key" from a slot still forming.
    #[inline]
    fn published_hash(&self, slot: &MapSlot) -> u64 {
        loop {
            let h = slot.hash.load(Ordering::Acquire);
            if h != HASH_UNSET {
                return h;
            }
            // The claim may have been abandoned into a tombstone in the
            // meantime; a tombstone is not a key either way.
            if slot.state.load(Ordering::Acquire) != SLOT_OCCUPIED {
                return HASH_UNSET;
            }
            std::hint::spin_loop();
        }
    }

    /// SeqLock-read a (K, V) pair from a slot. Spins on odd version.
    fn read_payload(&self, slot_idx: usize) -> (K, V) {
        let slot = self.slot(slot_idx);
        loop {
            let v1 = slot.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let mut k = std::mem::MaybeUninit::<K>::uninit();
            let mut v = std::mem::MaybeUninit::<V>::uninit();
            let src = unsafe {
                self.mmap.as_ptr()
                    .add(size_of::<MapHeader>())
                    .add(slot_idx * size_of::<MapSlot>())
                    .add(std::mem::offset_of!(MapSlot, payload))
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src, k.as_mut_ptr() as *mut u8, size_of::<K>(),
                );
                std::ptr::copy_nonoverlapping(
                    src.add(size_of::<K>()),
                    v.as_mut_ptr() as *mut u8,
                    size_of::<V>(),
                );
            }
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                return unsafe { (k.assume_init(), v.assume_init()) };
            }
        }
    }

    /// Insert or update. Returns `Inserted` for a new key,
    /// `Updated` when an existing key's value was overwritten,
    /// `Err(Full)` if the table has no slot for the key (probed
    /// every slot without finding Empty, a key match, or a
    /// reclaimable tombstone).
    ///
    /// # Tombstone reuse
    ///
    /// Insert tracks the first tombstone seen during the probe.
    /// If the probe terminates at an Empty (key absent) and a
    /// tombstone was seen, the tombstone slot is reclaimed instead
    /// of consuming the Empty. This eliminates the need for an
    /// explicit `compact()` call in steady-state insert/remove
    /// workloads. `compact()` is still useful for bulk reclamation
    /// in workloads that don't naturally trigger reuse (e.g. a
    /// long insert-only period after heavy removes).
    pub fn insert(&self, key: K, value: V) -> Result<InsertOutcome, MapError> {
        let r = self.insert_inner(key, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_INSERT,
            if matches!(r, Err(MapError::Full)) { 1 } else { 0 },
        );
        r
    }

    fn insert_inner(&self, key: K, value: V) -> Result<InsertOutcome, MapError> {
        match self.place(key, value, true)? {
            Placed::New => Ok(InsertOutcome::Inserted),
            Placed::Existing(_) => Ok(InsertOutcome::Updated),
        }
    }

    /// The one probe every insert shares: find `key`'s slot or claim one
    /// for it. With `overwrite`, a present key takes `value` and the value
    /// it had is returned; without it, a present key is left alone and its
    /// value returned. The probe starts again when a tombstone claim goes
    /// to another writer, since what that writer placed may be this very
    /// key; when a claim is given back; and when the key's entry is
    /// removed between its hash matching and its lock.
    fn place(&self, key: K, value: V, overwrite: bool) -> Result<Placed<V>, MapError> {
        let h = Self::hash_key(&key);
        let start = self.wrap(h as usize);
        loop {
            match self.probe_once(h, start, &key, &value, overwrite) {
                Probe::Placed(p) => return Ok(p),
                Probe::Restart => continue,
                Probe::Full => return Err(MapError::Full),
            }
        }
    }

    /// One pass of the linear probe from `start`. Tracks the first
    /// tombstone seen so a probe that ends at an Empty claims the
    /// tombstone in preference to the Empty.
    fn probe_once(&self, h: u64, start: usize, key: &K, value: &V, overwrite: bool) -> Probe<V> {
        // Compared again once a slot is claimed; see `claim`. The walk's
        // loads are SeqCst so they fall in one order with the claims and
        // with the count's reads and bumps.
        let removals = self.header().removals.load(Ordering::SeqCst);
        let mut first_tombstone: Option<usize> = None;
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let mut state = slot.state.load(Ordering::SeqCst);
            if state == SLOT_EMPTY {
                // End of the chain: the key is absent. Claim the tracked
                // tombstone, else this Empty.
                if let Some(tomb_idx) = first_tombstone {
                    return match self.claim(tomb_idx, SLOT_TOMBSTONE, removals, h, key, value) {
                        Claim::Placed => Probe::Placed(Placed::New),
                        Claim::Lost | Claim::GaveBack => Probe::Restart,
                    };
                }
                match self.claim(idx, SLOT_EMPTY, removals, h, key, value) {
                    Claim::Placed => return Probe::Placed(Placed::New),
                    Claim::GaveBack => return Probe::Restart,
                    // The Empty went to another writer; see what it became.
                    Claim::Lost => state = slot.state.load(Ordering::SeqCst),
                }
            }
            if state == SLOT_TOMBSTONE {
                // Remember the first and keep probing, since a later slot
                // may hold the key.
                if first_tombstone.is_none() {
                    first_tombstone = Some(idx);
                }
                continue;
            }
            match self.present(idx, h, key, value, overwrite) {
                Found::Here(existing) => return Probe::Placed(Placed::Existing(existing)),
                Found::Moved => return Probe::Restart,
                Found::Vacated => {
                    if first_tombstone.is_none() {
                        first_tombstone = Some(idx);
                    }
                }
                Found::Elsewhere => {}
            }
        }
        // Every slot walked with no key and no Empty: the tracked
        // tombstone is the last chance.
        match first_tombstone {
            Some(tomb_idx) => match self.claim(tomb_idx, SLOT_TOMBSTONE, removals, h, key, value) {
                Claim::Placed => Probe::Placed(Placed::New),
                Claim::Lost | Claim::GaveBack => Probe::Restart,
            },
            None => Probe::Full,
        }
    }

    /// What the occupied slot `idx` says about `key`, waiting out a writer
    /// still publishing it. With `overwrite`, a slot holding the key takes
    /// `value` under the slot's lock, once the lock shows the slot still
    /// holds the key: a remove and another key's claim can land between
    /// the hash matching and the lock.
    fn present(&self, idx: usize, h: u64, key: &K, value: &V, overwrite: bool) -> Found<V> {
        let slot = self.slot(idx);
        match self.published_hash(slot) {
            HASH_UNSET => return Found::Vacated,
            published if published != h => return Found::Elsewhere,
            _ => {}
        }
        if !overwrite {
            let (k, existing) = self.read_payload(idx);
            return if k == *key { Found::Here(existing) } else { Found::Elsewhere };
        }
        let held = self.lock_slot(slot);
        let (k, existing) = unsafe { self.read_payload_locked(idx) };
        let found = if k != *key {
            Found::Elsewhere
        } else if !self.holds(slot, h) {
            Found::Moved
        } else {
            #[cfg(test)]
            crate::test_races::pause_point();
            unsafe { self.copy_payload(idx, key, value) };
            Found::Here(existing)
        };
        self.unlock_slot(slot, held);
        found
    }

    /// Whether a slot is a published entry with hash `h`. Read under the
    /// slot's lock, a true answer holds until the lock is released, since
    /// a remove takes the lock before it tombstones.
    #[inline]
    fn holds(&self, slot: &MapSlot, h: u64) -> bool {
        slot.state.load(Ordering::Acquire) == SLOT_OCCUPIED && slot.hash.load(Ordering::Acquire) == h
    }

    /// Claim the free slot `idx`, found in state `from`, for a new entry,
    /// and publish the entry unless a remove has landed since the walk
    /// began. That remove may have opened a slot the walk passed while
    /// another key held it, where another insert of this key can land
    /// without passing this claim, so the claim is given back as a
    /// tombstone and the probe walks again.
    fn claim(&self, idx: usize, from: u8, removals: u64, h: u64, key: &K, value: &V) -> Claim {
        let slot = self.slot(idx);
        if slot
            .state
            .compare_exchange(from, SLOT_OCCUPIED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Claim::Lost;
        }
        if from == SLOT_TOMBSTONE {
            // A tombstone written by this code already carries HASH_UNSET;
            // one left by an older map file still names the removed key,
            // so it is cleared here before the payload lands.
            slot.hash.store(HASH_UNSET, Ordering::Release);
        }
        if self.header().removals.load(Ordering::SeqCst) != removals {
            slot.state.store(SLOT_TOMBSTONE, Ordering::SeqCst);
            if from == SLOT_EMPTY {
                self.header().tombstones.fetch_add(1, Ordering::AcqRel);
            }
            return Claim::GaveBack;
        }
        self.publish_claimed(idx, h, key, value);
        self.header().count.fetch_add(1, Ordering::AcqRel);
        if from == SLOT_TOMBSTONE {
            self.release_tombstone();
        }
        Claim::Placed
    }

    /// Count one tombstone fewer, for one claimed back into an entry. The
    /// loop never takes the count below zero, whatever raced it there.
    fn release_tombstone(&self) {
        loop {
            let cur = self.header().tombstones.load(Ordering::Acquire);
            if cur == 0 {
                break;
            }
            if self
                .header()
                .tombstones
                .compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Look up a key. Returns `None` if absent.
    pub fn get(&self, key: &K) -> Option<V> {
        let r = self.get_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Internal lookup; no sidecar observation. Used by `get`,
    /// `contains_key`, and `remove` so each public entry point
    /// pushes its own semantic op_kind without double-counting.
    fn get_inner(&self, key: &K) -> Option<V> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return None;
            }
            if state == SLOT_OCCUPIED && self.published_hash(slot) == h {
                let (k, v) = self.read_payload(idx);
                if k == *key {
                    return Some(v);
                }
            }
            // Tombstone or mismatch: continue probing.
        }
        None
    }

    /// Insert `key` only if it is absent: `Ok(None)` when this call
    /// placed it, `Ok(Some(existing))` when a value was already present
    /// and nothing was written. Two callers racing on one absent key
    /// resolve to exactly one `Ok(None)`; the other reads the winner's
    /// value, because a slot claimed but not yet published is waited on
    /// rather than mistaken for a different key, and a claim is given back
    /// when a remove lands during its walk.
    pub fn insert_if_absent(&self, key: K, value: V) -> Result<Option<V>, MapError> {
        let r = self.insert_if_absent_inner(key, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_INSERT,
            if matches!(r, Err(MapError::Full)) { 1 } else { 0 },
        );
        r
    }

    fn insert_if_absent_inner(&self, key: K, value: V) -> Result<Option<V>, MapError> {
        match self.place(key, value, false)? {
            Placed::New => Ok(None),
            Placed::Existing(existing) => Ok(Some(existing)),
        }
    }

    /// Insert or replace, returning what was replaced: `Ok(None)` when the
    /// key was absent and this call placed it, `Ok(Some(old))` when it
    /// replaced `old`. The old value is read and overwritten under the
    /// slot's lock, so no other writer lands between them, and a value
    /// leaves the map exactly once, through `swap`,
    /// [`remove`](Self::remove) or a [`compare_exchange`](Self::compare_exchange)
    /// that succeeds.
    pub fn swap(&self, key: K, value: V) -> Result<Option<V>, MapError> {
        let r = self.place(key, value, true);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_INSERT,
            if matches!(r, Err(MapError::Full)) { 1 } else { 0 },
        );
        match r? {
            Placed::New => Ok(None),
            Placed::Existing(old) => Ok(Some(old)),
        }
    }

    /// Replace `key`'s value with `new` only if its current value is
    /// byte-for-byte `expected`: `Ok(Ok(()))` on the swap, `Ok(Err(current))`
    /// when the value differed and nothing was written,
    /// `Err(KeyAbsent)` when the key has no entry. The comparison and
    /// the write happen under the slot's SeqLock, so no other writer
    /// lands between them. Bytes, not `PartialEq`: a `V` with padding
    /// or a NaN inside compares as its bits.
    pub fn compare_exchange(&self, key: &K, expected: V, new: V) -> Result<Result<(), V>, MapError> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return Err(MapError::KeyAbsent);
            }
            if state != SLOT_OCCUPIED || self.published_hash(slot) != h {
                continue;
            }
            #[cfg(test)]
            crate::test_races::pause_point();
            let held = self.lock_slot(slot);
            // Under the lock the payload is stable: read it directly.
            let (k, current) = unsafe { self.read_payload_locked(idx) };
            if k != *key {
                self.unlock_slot(slot, held);
                continue;
            }
            // A remove may have taken the entry between the hash matching
            // and the lock, and another writer claimed the slot since; the
            // entry is gone, not mismatched.
            if !self.holds(slot, h) {
                self.unlock_slot(slot, held);
                return Err(MapError::KeyAbsent);
            }
            let matches = unsafe {
                let a = std::slice::from_raw_parts(&current as *const V as *const u8, size_of::<V>());
                let b = std::slice::from_raw_parts(&expected as *const V as *const u8, size_of::<V>());
                a == b
            };
            if matches {
                unsafe { self.copy_payload(idx, key, &new) };
                self.unlock_slot(slot, held);
                return Ok(Ok(()));
            }
            self.unlock_slot(slot, held);
            return Ok(Err(current));
        }
        Err(MapError::KeyAbsent)
    }

    /// Read a slot's payload while holding its SeqLock, so no validation
    /// loop is needed.
    ///
    /// # Safety
    /// The caller holds the slot's lock from [`lock_slot`](Self::lock_slot).
    unsafe fn read_payload_locked(&self, slot_idx: usize) -> (K, V) {
        let src = self.payload_ptr(slot_idx);
        let mut k = std::mem::MaybeUninit::<K>::uninit();
        let mut v = std::mem::MaybeUninit::<V>::uninit();
        unsafe {
            std::ptr::copy_nonoverlapping(src, k.as_mut_ptr() as *mut u8, size_of::<K>());
            std::ptr::copy_nonoverlapping(
                src.add(size_of::<K>()),
                v.as_mut_ptr() as *mut u8,
                size_of::<V>(),
            );
            (k.assume_init(), v.assume_init())
        }
    }

    /// True if `key` is present.
    pub fn contains_key(&self, key: &K) -> bool {
        let r = self.get_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_CONTAINS,
            if r.is_none() { 2 } else { 0 },
        );
        r.is_some()
    }

    /// Remove a key. Returns the value if present: the one the entry held
    /// when it went, since the value is read and the entry tombstoned
    /// under the slot's lock.
    pub fn remove(&self, key: &K) -> Option<V> {
        let r = self.remove_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_REMOVE,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn remove_inner(&self, key: &K) -> Option<V> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY { return None; }
            if state != SLOT_OCCUPIED || self.published_hash(slot) != h {
                continue;
            }
            let held = self.lock_slot(slot);
            let (k, v) = unsafe { self.read_payload_locked(idx) };
            if k != *key {
                self.unlock_slot(slot, held);
                continue;
            }
            // Another remove took the entry between the hash matching and
            // the lock.
            if !self.holds(slot, h) {
                self.unlock_slot(slot, held);
                return None;
            }
            #[cfg(test)]
            crate::test_races::pause_point();
            // Counted before the tombstone lands, so an insert of a new key
            // whose walk passed this entry sees the count move before it
            // publishes.
            self.header().removals.fetch_add(1, Ordering::SeqCst);
            // Under the lock this fails only against a remove from a
            // process of an earlier release, which takes no lock.
            let tombstoned = slot
                .state
                .compare_exchange(SLOT_OCCUPIED, SLOT_TOMBSTONE, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
            if tombstoned {
                // A tombstone names no key, so a later claim of this slot
                // is seen as forming from its first instant rather than as
                // the removed entry.
                slot.hash.store(HASH_UNSET, Ordering::Release);
            }
            self.unlock_slot(slot, held);
            if !tombstoned {
                return None;
            }
            self.header().count.fetch_sub(1, Ordering::AcqRel);
            self.header().tombstones.fetch_add(1, Ordering::AcqRel);
            return Some(v);
        }
        None
    }

    /// Clear the entire map. Marks every slot Empty and resets both
    /// the live count and the tombstone counter to 0. Not
    /// concurrency-safe vs concurrent insert/remove - callers should
    /// ensure no other writers are active when calling this.
    pub fn clear(&self) {
        for i in 0..self.capacity {
            let slot = self.slot(i);
            slot.state.store(SLOT_EMPTY, Ordering::Release);
        }
        self.header().count.store(0, Ordering::Release);
        self.header().tombstones.store(0, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::hash_map::OP_CLEAR, 0);
    }

    /// Current tombstone count (slots marked dead by `remove` that
    /// have not yet been reclaimed by `compact`).
    #[inline]
    pub fn tombstone_count(&self) -> usize {
        self.header().tombstones.load(Ordering::Acquire) as usize
    }

    /// Heuristic: returns `true` if tombstones occupy at least
    /// `threshold_fraction` of capacity. Callers typically pass
    /// `0.30` (30 %) - past that, linear-probe chains stretch out
    /// and lookup/insert latency degrades sharply. Cheap O(1).
    pub fn should_compact(&self, threshold_fraction: f64) -> bool {
        debug_assert!(
            (0.0..=1.0).contains(&threshold_fraction),
            "threshold_fraction must be in [0, 1]; got {threshold_fraction}",
        );
        let tombs = self.tombstone_count() as f64;
        tombs / self.capacity as f64 >= threshold_fraction
    }

    /// Reclaim tombstones via in-place rebuild. Returns the number
    /// of slots reclaimed.
    ///
    /// # What it does
    ///
    /// Snapshots every Occupied slot into a `Vec<(K, V)>`, resets
    /// every slot to Empty (zeroing both counters), then re-inserts
    /// each snapshotted pair via the normal probe. Since no
    /// tombstones remain, every key lands as close to its ideal
    /// slot as the live keys permit - probe chains shrink back to
    /// the no-deletion baseline.
    ///
    /// # Concurrency
    ///
    /// **Not concurrency-safe with `insert` / `remove`.** The caller
    /// must guarantee no other writer (in any process holding an
    /// MMF handle to the same file) is mutating the map during
    /// `compact`. Readers calling `get` will see a transient empty
    /// state mid-rebuild and may return spurious `None` for keys
    /// that are about to be re-inserted; if that is unacceptable,
    /// serialize readers too.
    ///
    /// # Cost
    ///
    /// O(capacity) for the snapshot + reset, O(live_count *
    /// avg_probe) for re-insert. Allocates a temporary `Vec<(K, V)>`
    /// sized to the live count. For a 1 M-slot map at 50 % load,
    /// expect ~tens of milliseconds.
    pub fn compact(&self) -> Result<usize, MapError> {
        self.ring_sidecar
            .push_op(crate::sidecar_ops::hash_map::OP_COMPACT, 0);
        let mut live: Vec<(K, V)> = Vec::with_capacity(self.len());
        let mut reclaimed = 0usize;
        for i in 0..self.capacity {
            let slot = self.slot(i);
            let s = slot.state.load(Ordering::Acquire);
            if s == SLOT_OCCUPIED {
                live.push(self.read_payload(i));
            } else if s == SLOT_TOMBSTONE {
                reclaimed += 1;
            }
        }
        for i in 0..self.capacity {
            let slot = self.slot(i);
            slot.state.store(SLOT_EMPTY, Ordering::Release);
        }
        self.header().count.store(0, Ordering::Release);
        self.header().tombstones.store(0, Ordering::Release);
        for (k, v) in live {
            // Re-insert under single-writer contract: cannot race,
            // and `Full` is impossible because the live set fit in
            // the table before compaction.
            self.insert(k, v)?;
        }
        Ok(reclaimed)
    }

    /// Walk and collect every (K, V) pair published when the walk reached
    /// its slot. An insert claims its slot before it writes the payload,
    /// so a slot claimed and not yet published is passed over rather than
    /// collected with the zeroed payload of an insert in flight; a caller
    /// that must see every entry snapshots again once the writers have
    /// returned.
    pub fn snapshot(&self) -> Vec<(K, V)> {
        let mut out = Vec::with_capacity(self.len());
        for i in 0..self.capacity {
            let slot = self.slot(i);
            if slot.state.load(Ordering::Acquire) == SLOT_OCCUPIED && slot.hash.load(Ordering::Acquire) != HASH_UNSET {
                out.push(self.read_payload(i));
            }
        }
        out
    }

    /// Current load factor (count / capacity).
    pub fn load_factor(&self) -> f64 {
        self.len() as f64 / self.capacity as f64
    }

    pub fn flush(&self) -> Result<(), MapError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), MapError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_races::{settle, stopped, PARK};
    use std::sync::Arc;
    use std::thread;

    /// A file for one test, removed when the test ends; declared before the
    /// map so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-hashmap-{name}-{}.bin", std::process::id()))
    }

    #[test]
    fn create_initial_empty() {
        let p = tmp("init");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 32).unwrap();
        assert_eq!(m.capacity(), 32);
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        assert_eq!(m.get(&42), None);
    }

    #[test]
    fn insert_and_get_round_trip() {
        let p = tmp("rt");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        assert_eq!(m.insert(1, 100).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(2, 200).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(3, 300).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.len(), 3);
        assert_eq!(m.get(&1), Some(100));
        assert_eq!(m.get(&2), Some(200));
        assert_eq!(m.get(&3), Some(300));
        assert_eq!(m.get(&999), None);
    }

    /// A second create attaches to the live map with its entries intact;
    /// reset is what strips them.
    #[test]
    fn second_create_attaches_and_keeps_entries() {
        let p = tmp("attach");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        m.insert(7, 777).unwrap();

        let m2: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        assert_eq!(m2.get(&7), Some(777), "attach lost a live entry");
        assert_eq!(m2.len(), 1);

        // Windows refuses to truncate a mapped file, so every handle goes
        // before the reset.
        drop(m);
        drop(m2);
        let fresh: SharedHashMap<u32, u64> = SharedHashMap::reset(&p, 32).unwrap();
        assert_eq!(fresh.len(), 0);
        assert_eq!(fresh.get(&7), None, "reset left an entry behind");
        drop(fresh);
    }

    /// Attaching with a different capacity or key type is refused, and a
    /// larger capacity than the region on disk is refused at once rather
    /// than waited on as a creator still initializing.
    #[test]
    fn create_refuses_a_mismatched_region() {
        let p = tmp("mismatch");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        assert!(matches!(
            SharedHashMap::<u32, u64>::create(&p, 16),
            Err(MapError::LayoutMismatch),
        ));
        assert!(matches!(
            SharedHashMap::<u64, u64>::create(&p, 32),
            Err(MapError::LayoutMismatch),
        ));
        let started = std::time::Instant::now();
        assert!(matches!(
            SharedHashMap::<u32, u64>::create(&p, 64),
            Err(MapError::LayoutMismatch),
        ));
        assert!(
            started.elapsed() < crate::mmf_attach::INIT_WAIT / 2,
            "a region published at a smaller capacity was waited on for {:?}",
            started.elapsed()
        );
        drop(m);
        std::fs::remove_file(&p).expect("the map file is unmapped and removable");
    }

    #[test]
    fn duplicate_insert_updates_value() {
        let p = tmp("dup");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.insert(7, 100).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(7, 200).unwrap(), InsertOutcome::Updated);
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(&7), Some(200));
    }

    #[test]
    fn remove_returns_value_and_decrements_count() {
        let p = tmp("rm");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(&1), None);
        assert_eq!(m.get(&2), Some(20));
        assert_eq!(m.remove(&999), None);
    }

    #[test]
    fn tombstone_does_not_break_probing() {
        let p = tmp("tomb");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 8).unwrap();
        // Force collisions by using keys that hash close together.
        // Insert several keys, remove the middle one, verify later
        // keys remain findable past the tombstone.
        for k in 0..6u32 { m.insert(k, k * 10).unwrap(); }
        // Remove a middle key.
        m.remove(&2);
        for k in [0u32, 1, 3, 4, 5] {
            assert_eq!(m.get(&k), Some(k * 10),
                "key {k} should still be findable past the tombstone");
        }
        assert_eq!(m.get(&2), None);
    }

    #[test]
    fn full_map_returns_error_on_new_key() {
        let p = tmp("full");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 4).unwrap();
        m.insert(1, 1).unwrap();
        m.insert(2, 2).unwrap();
        m.insert(3, 3).unwrap();
        m.insert(4, 4).unwrap();
        assert_eq!(m.insert(5, 5).err(), Some(MapError::Full));
        // Update of existing key still works.
        assert_eq!(m.insert(1, 100).unwrap(), InsertOutcome::Updated);
    }

    #[test]
    fn clear_resets_to_empty() {
        let p = tmp("clear");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        for k in 0..5u32 { m.insert(k, k).unwrap(); }
        assert_eq!(m.len(), 5);
        m.clear();
        assert_eq!(m.len(), 0);
        assert_eq!(m.get(&0), None);
        m.insert(99, 99).unwrap();
        assert_eq!(m.get(&99), Some(99));
    }

    #[test]
    fn snapshot_collects_all_present_pairs() {
        let p = tmp("snap");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();
        m.remove(&2);
        let mut snap = m.snapshot();
        snap.sort();
        assert_eq!(snap, vec![(1, 10), (3, 30)]);
    }

    /// An insert claims its slot before it writes the payload and
    /// publishes the hash, so a snapshot that trusted the state alone
    /// would collect the zeroed payload of an insert in flight. A
    /// claimed, unpublished slot is passed over; once published it is
    /// collected.
    #[test]
    fn a_snapshot_passes_over_a_slot_claimed_but_not_yet_published() {
        let p = tmp("claimed");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        m.insert(1, 10).unwrap();

        // A second insert held between its claim and its publish, as a
        // writer descheduled there leaves it.
        let idx = (0..m.capacity())
            .find(|&i| m.slot(i).state.load(Ordering::Acquire) == SLOT_EMPTY)
            .expect("a free slot");
        m.slot(idx)
            .state
            .compare_exchange(SLOT_EMPTY, SLOT_OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
            .expect("the claim");
        assert_eq!(m.snapshot(), vec![(1, 10)], "a slot an insert has claimed and not yet written is not an entry");

        m.publish_claimed(idx, SharedHashMap::<u32, u32>::hash_key(&2), &2, &20);
        m.header().count.fetch_add(1, Ordering::AcqRel);
        let mut snap = m.snapshot();
        snap.sort();
        assert_eq!(snap, vec![(1, 10), (2, 20)], "published, it is collected");
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        let reader: SharedHashMap<u32, u32> = SharedHashMap::open(&p, 16).unwrap();
        writer.insert(42, 4242).unwrap();
        assert_eq!(reader.get(&42), Some(4242));
        reader.insert(7, 77).unwrap();
        assert_eq!(writer.get(&7), Some(77));
        writer.remove(&42);
        assert_eq!(reader.get(&42), None);
    }

    #[test]
    fn concurrent_inserters_all_keys_present() {
        let p = tmp("concurrent");
        let m: Arc<SharedHashMap<u32, u32>> = Arc::new(SharedHashMap::create(&p, 1024).unwrap());
        let n_threads = 4;
        let per_thread = 100u32;
        let mut handles = vec![];
        for t in 0..n_threads {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    let key = (t as u32) * per_thread + i;
                    m.insert(key, key * 10).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(m.len(), n_threads * per_thread as usize);
        for t in 0..n_threads as u32 {
            for i in 0..per_thread {
                let key = t * per_thread + i;
                assert_eq!(m.get(&key), Some(key * 10),
                    "key {key} should be present with value {}", key * 10);
            }
        }
    }

    #[test]
    fn struct_key_and_value_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(C)]
        struct UserId { realm: u32, user: u32 }
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Session { token: u64, expires_us: u64 }
        let p = tmp("struct");
        let m: SharedHashMap<UserId, Session> = SharedHashMap::create(&p, 32).unwrap();
        let k = UserId { realm: 1, user: 42 };
        let v = Session { token: 0xDEAD_BEEF, expires_us: 9_999_999_999 };
        m.insert(k, v).unwrap();
        assert_eq!(m.get(&k), Some(v));
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // sizeof signal only
        struct BigKey([u8; 64]);
        impl Copy for BigKey {}
        impl Clone for BigKey { fn clone(&self) -> Self { *self } }
        impl PartialEq for BigKey { fn eq(&self, _: &Self) -> bool { true } }
        impl Eq for BigKey {}
        let p = tmp("too-large");
        let r = SharedHashMap::<BigKey, u32>::create(&p, 4);
        assert_eq!(r.err(), Some(MapError::PayloadTooLarge));
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
            for k in 0..5u32 { m.insert(k, k * 100).unwrap(); }
            m.remove(&2);
            m.flush().unwrap();
        }
        let m2: SharedHashMap<u32, u32> = SharedHashMap::open(&p, 16).unwrap();
        assert_eq!(m2.len(), 4);
        assert_eq!(m2.get(&0), Some(0));
        assert_eq!(m2.get(&1), Some(100));
        assert_eq!(m2.get(&2), None);
        assert_eq!(m2.get(&3), Some(300));
        assert_eq!(m2.get(&4), Some(400));
    }

    #[test]
    fn fnv1a_64_is_deterministic() {
        // Sanity: same input always produces the same hash.
        let h1 = fnv1a_64(b"adaptive-prims");
        let h2 = fnv1a_64(b"adaptive-prims");
        assert_eq!(h1, h2);
        // And different inputs hash differently.
        let h3 = fnv1a_64(b"ADAPTIVE-PRIMS");
        assert_ne!(h1, h3);
    }

    #[test]
    fn load_factor_reports_correctly() {
        let p = tmp("load");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 10).unwrap();
        for k in 0..3u32 { m.insert(k, k).unwrap(); }
        assert_eq!(m.load_factor(), 0.3);
    }

    #[test]
    fn remove_bumps_tombstone_counter() {
        let p = tmp("tomb-counter");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.tombstone_count(), 0);
        for k in 0..5u32 { m.insert(k, k).unwrap(); }
        assert_eq!(m.tombstone_count(), 0);
        m.remove(&1);
        m.remove(&3);
        assert_eq!(m.tombstone_count(), 2);
        // Removing an absent key leaves the tombstone count alone.
        m.remove(&999);
        assert_eq!(m.tombstone_count(), 2);
    }

    #[test]
    fn compact_on_empty_map_is_noop() {
        let p = tmp("compact-empty");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.compact().unwrap(), 0);
        assert_eq!(m.len(), 0);
        assert_eq!(m.tombstone_count(), 0);
        // Map remains fully usable.
        m.insert(7, 70).unwrap();
        assert_eq!(m.get(&7), Some(70));
    }

    #[test]
    fn compact_reclaims_tombstones() {
        let p = tmp("compact-reclaim");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        for k in 0..10u32 { m.insert(k, k * 10).unwrap(); }
        for k in [0u32, 2, 4, 6, 8] { m.remove(&k); }
        assert_eq!(m.tombstone_count(), 5);
        let reclaimed = m.compact().unwrap();
        assert_eq!(reclaimed, 5);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), 5);
    }

    #[test]
    fn compact_preserves_all_live_pairs() {
        let p = tmp("compact-preserve");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        // Insert 20, remove 10, compact, verify the remaining 10
        // are all present with their original values.
        for k in 0..20u32 { m.insert(k, (k as u64) * 1000).unwrap(); }
        for k in (0..20u32).filter(|k| k % 2 == 0) { m.remove(&k); }
        let pre: Vec<(u32, u64)> = {
            let mut s = m.snapshot();
            s.sort();
            s
        };
        m.compact().unwrap();
        let post: Vec<(u32, u64)> = {
            let mut s = m.snapshot();
            s.sort();
            s
        };
        assert_eq!(pre, post,
            "compact must preserve every live (K, V) pair exactly");
        // And every preserved key is still findable by lookup.
        for (k, v) in &post {
            assert_eq!(m.get(k), Some(*v));
        }
    }

    #[test]
    fn compact_reclaims_after_heavy_churn() {
        // Many insert/remove cycles accumulate tombstones in the
        // probe path because insert probes past tombstones if the
        // tombstone-reuse path is not exercised. Compact must
        // reclaim every dead slot exactly.
        let p = tmp("compact-churn");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 64).unwrap();
        for k in 0..32u32 { m.insert(k, k).unwrap(); }
        for round in 0..16u32 {
            m.remove(&round);
            let new_key = 100 + round;
            m.insert(new_key, new_key).unwrap();
        }
        assert_eq!(m.tombstone_count(), 16);
        let live_before = m.len();
        let reclaimed = m.compact().unwrap();
        assert_eq!(reclaimed, 16);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), live_before);
        for round in 0..16u32 {
            assert_eq!(m.get(&round), None);
            let new_key = 100 + round;
            assert_eq!(m.get(&new_key), Some(new_key));
        }
    }

    #[test]
    fn tombstone_reuse_avoids_full_after_remove() {
        // 8-slot table, fill it, remove one key. The next insert
        // of a new key reuses the tombstone slot instead of
        // returning Full. This validates the tombstone-reuse-on-
        // insert path.
        let p = tmp("reuse-avoids-full");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 8).unwrap();
        for k in 0..8u32 { m.insert(k, k).unwrap(); }
        m.remove(&3);
        assert_eq!(m.tombstone_count(), 1);
        // Without reuse this returns Full (7 live + 1 tombstone
        // in 8 slots). With reuse it succeeds and the tombstone
        // counter drops to 0.
        assert!(m.insert(99, 99).is_ok(),
            "tombstone reuse must let insert succeed");
        assert_eq!(m.get(&99), Some(99));
        assert_eq!(m.tombstone_count(), 0,
            "successful tombstone reuse must decrement the counter");
    }

    #[test]
    fn compact_still_useful_for_remove_heavy_workload() {
        // Insert N, remove most without re-inserting. Tombstones
        // accumulate because there is no insert to trigger reuse.
        // compact() bulk-reclaims them. This covers workloads
        // that lack the insert pressure to trigger reuse
        // naturally.
        let p = tmp("compact-still-useful");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 32).unwrap();
        for k in 0..20u32 { m.insert(k, k).unwrap(); }
        for k in 0..15u32 { m.remove(&k); }
        assert_eq!(m.tombstone_count(), 15);
        let reclaimed = m.compact().unwrap();
        assert_eq!(reclaimed, 15);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), 5);
    }

    #[test]
    fn should_compact_threshold_logic() {
        let p = tmp("should-compact");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 100).unwrap();
        // 0 tombstones / 100 capacity = 0.0
        assert!(!m.should_compact(0.01));
        // Insert 50, remove 30 → 30 tombstones / 100 = 0.30.
        for k in 0..50u32 { m.insert(k, k).unwrap(); }
        for k in 0..30u32 { m.remove(&k); }
        assert_eq!(m.tombstone_count(), 30);
        assert!(m.should_compact(0.30));
        assert!(m.should_compact(0.29));
        assert!(!m.should_compact(0.31));
    }

    #[test]
    fn compact_persists_across_reopen() {
        let p = tmp("compact-disk");
        {
            let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
            for k in 0..6u32 { m.insert(k, k * 10).unwrap(); }
            for k in [0u32, 2, 4] { m.remove(&k); }
            m.compact().unwrap();
            m.flush().unwrap();
        }
        let m2: SharedHashMap<u32, u32> = SharedHashMap::open(&p, 16).unwrap();
        assert_eq!(m2.len(), 3);
        assert_eq!(m2.tombstone_count(), 0);
        for k in [1u32, 3, 5] {
            assert_eq!(m2.get(&k), Some(k * 10));
        }
        for k in [0u32, 2, 4] {
            assert_eq!(m2.get(&k), None);
        }
    }

    /// Many writers racing insert_if_absent on a single absent key: exactly one
    /// places it, every other reads the winner's value, and the table ends
    /// with a single entry - a claimed slot not yet published is waited on,
    /// never mistaken for a different key and planted a second time.
    #[test]
    fn racing_insert_if_absent_on_one_key_admits_exactly_one_writer() {
        use std::sync::{Arc, Barrier};
        let p = tmp("ifabsent-race");
        let m: Arc<SharedHashMap<u64, u64>> = Arc::new(SharedHashMap::create(&p, 64).unwrap());
        for round in 0..200u64 {
            let key = 1_000_000 + round;
            let writers = 8;
            let barrier = Arc::new(Barrier::new(writers));
            let handles: Vec<_> = (0..writers as u64)
                .map(|w| {
                    let m = Arc::clone(&m);
                    let b = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        b.wait();
                        m.insert_if_absent(key, w).unwrap()
                    })
                })
                .collect();
            let outcomes: Vec<Option<u64>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let placed: Vec<usize> = outcomes.iter().enumerate().filter(|(_, o)| o.is_none()).map(|(i, _)| i).collect();
            assert_eq!(placed.len(), 1, "round {round}: exactly one writer places the key, got {outcomes:?}");
            let winner = placed[0] as u64;
            for o in outcomes.iter().flatten() {
                assert_eq!(*o, winner, "round {round}: a loser reads the winner's value");
            }
            assert_eq!(m.get(&key), Some(winner));
            assert_eq!(m.len(), 1, "round {round}: one key, one slot");
            // Through a tombstone as well, which is the path that reads a
            // stale hash unless it is cleared on remove.
            assert_eq!(m.remove(&key), Some(winner));
            assert_eq!(m.len(), 0);
        }
        drop(m);
    }

    #[test]
    fn insert_if_absent_leaves_a_present_key_alone() {
        let p = tmp("ifabsent-present");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.insert_if_absent(7, 70).unwrap(), None);
        assert_eq!(m.insert_if_absent(7, 71).unwrap(), Some(70));
        assert_eq!(m.get(&7), Some(70), "the present value is not overwritten");
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn compare_exchange_swaps_only_on_the_expected_bytes() {
        let p = tmp("cas");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.compare_exchange(&1, 0, 5), Err(MapError::KeyAbsent));
        m.insert(1, 10).unwrap();
        assert_eq!(m.compare_exchange(&1, 99, 11), Ok(Err(10)), "a mismatch reports the current value");
        assert_eq!(m.get(&1), Some(10), "and writes nothing");
        assert_eq!(m.compare_exchange(&1, 10, 11), Ok(Ok(())));
        assert_eq!(m.get(&1), Some(11));
        m.remove(&1);
        assert_eq!(m.compare_exchange(&1, 11, 12), Err(MapError::KeyAbsent));
    }

    /// Writers contending on a single key's value through compare_exchange
    /// serialize through the slot lock: every successful swap saw the value
    /// it replaced, so the final count of successes equals the increments
    /// applied - a versioned publish with no lost update.
    #[test]
    fn concurrent_compare_exchange_loses_no_update() {
        use std::sync::Arc;
        let p = tmp("cas-race");
        let m: Arc<SharedHashMap<u32, u64>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        m.insert(1, 0).unwrap();
        let per_thread = 2_000u64;
        let threads = 6;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..per_thread {
                        loop {
                            let cur = m.get(&1).unwrap();
                            if m.compare_exchange(&1, cur, cur + 1).unwrap().is_ok() {
                                break;
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.get(&1), Some(per_thread * threads as u64));
        drop(m);
    }

    /// Two writers updating one key through insert take turns on the slot
    /// lock, so a reader validating each value's two halves against each
    /// other never sees them from different writes.
    #[test]
    fn concurrent_updates_of_one_key_never_tear() {
        use std::sync::atomic::{AtomicBool, Ordering as O};
        use std::sync::Arc;
        let p = tmp("update-tear");
        let m: Arc<SharedHashMap<u32, [u64; 4]>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        m.insert(1, [0, 0xAAAA, 0, !0]).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = (0..3u64)
            .map(|w| {
                let m = Arc::clone(&m);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut n = 0u64;
                    while !stop.load(O::Relaxed) {
                        let v = (w << 32) | n;
                        m.insert(1, [v, v ^ 0xAAAA, v.rotate_left(7), !v]).unwrap();
                        n += 1;
                    }
                })
            })
            .collect();
        let start = std::time::Instant::now();
        let mut reads = 0u64;
        while start.elapsed() < std::time::Duration::from_millis(300) {
            let [a, b, c, d] = m.get(&1).unwrap();
            assert_eq!(b, a ^ 0xAAAA, "torn value");
            assert_eq!(c, a.rotate_left(7), "torn value");
            assert_eq!(d, !a, "torn value");
            reads += 1;
        }
        stop.store(true, O::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        assert!(reads > 0);
        drop(m);
    }

    /// Two inserts of one new key, racing a remove in the key's chain,
    /// place it once. The chain runs X at the key's home, the tombstone Y
    /// left, a slot a writer has claimed and not yet published, then
    /// Empty. A walks past X and waits on the claimed slot; X is removed;
    /// B walks the tombstone X left and waits there too; the claimed slot
    /// is then published as a third key. A has tracked the tombstone after
    /// X and B the one X left, so each reaches Empty with a free slot of
    /// its own and no claim of the other's behind it; only the removal
    /// count sends one of them back to find the other's entry.
    #[test]
    fn inserts_of_one_key_racing_a_remove_in_its_chain_place_it_once() {
        use std::sync::Barrier;
        let p = tmp("ifabsent-across-remove");
        let m: Arc<SharedHashMap<u64, u64>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        let key = 1u64;
        let home = |k: u64| m.wrap(SharedHashMap::<u64, u64>::hash_key(&k) as usize);
        let s = home(key);
        let mut sharing = (2u64..).filter(|&k| home(k) == s);
        let (x, y, z) = (sharing.next().unwrap(), sharing.next().unwrap(), sharing.next().unwrap());
        m.insert(x, 10).unwrap();
        m.insert(y, 20).unwrap();
        assert_eq!(m.remove(&y), Some(20), "y sat after x and leaves its tombstone there");
        let claimed = m.wrap(s + 2);
        m.slot(claimed)
            .state
            .compare_exchange(SLOT_EMPTY, SLOT_OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
            .expect("the slot after the tombstone is free");

        let start = |value: u64| {
            let m = Arc::clone(&m);
            let running = Arc::new(Barrier::new(2));
            let go = Arc::clone(&running);
            let insert = thread::spawn(move || {
                go.wait();
                m.insert_if_absent(key, value).unwrap()
            });
            running.wait();
            thread::sleep(PARK);
            insert
        };
        let a = start(100);
        assert_eq!(m.remove(&x), Some(10));
        let b = start(200);
        m.publish_claimed(claimed, SharedHashMap::<u64, u64>::hash_key(&z), &z, &30);
        m.header().count.fetch_add(1, Ordering::AcqRel);

        let (a, b) = (a.join().unwrap(), b.join().unwrap());
        assert_eq!(
            [a, b].iter().filter(|r| r.is_none()).count(),
            1,
            "exactly one insert places the key: A got {a:?}, B got {b:?}"
        );
        let winner = if a.is_none() { 100 } else { 200 };
        assert_eq!(a.or(b), Some(winner), "the other insert reads the winner's value");
        assert_eq!(m.snapshot().iter().filter(|(k, _)| *k == key).count(), 1, "the key holds one slot");
        assert_eq!(m.get(&key), Some(winner));
    }

    /// A remove that has matched its key and not yet tombstoned it holds
    /// the slot's lock, so a compare_exchange of the key waits on it and
    /// then finds the entry gone: the value leaves the map once, through
    /// the remove. A remove that tombstoned outside the lock would let the
    /// compare_exchange land inside its window and hand the value back a
    /// second time.
    #[test]
    fn a_remove_stopped_before_its_tombstone_holds_off_a_compare_exchange() {
        let p = tmp("remove-vs-exchange");
        let m: Arc<SharedHashMap<u64, u64>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        m.insert(7, 100).unwrap();
        let (pause, remove) = stopped({
            let m = Arc::clone(&m);
            move || m.remove(&7)
        });
        let exchange = {
            let m = Arc::clone(&m);
            thread::spawn(move || m.compare_exchange(&7, 100, 200))
        };
        settle(&exchange);
        assert!(!exchange.is_finished(), "the compare_exchange waits on the slot lock the stopped remove holds");
        pause.release();
        assert_eq!(remove.join().unwrap(), Some(100), "the remove hands back the value the entry held when it went");
        assert_eq!(exchange.join().unwrap(), Err(MapError::KeyAbsent), "the compare_exchange finds the entry gone");
        assert_eq!(m.get(&7), None);
        assert_eq!(m.len(), 0);
    }

    /// A compare_exchange that has matched its hash and not yet taken the
    /// slot's lock finds, under the lock, whether the slot still holds its
    /// key: a remove of the key and another key's claim of the slot, not
    /// yet published, land in between, and the claim leaves the key's
    /// bytes in place. A compare_exchange that checked only the slot's
    /// state under the lock would match those bytes and report the swap on
    /// an entry already removed.
    #[test]
    fn a_compare_exchange_stopped_before_its_lock_finds_no_entry_after_a_remove_and_a_claim() {
        let p = tmp("exchange-vs-claim");
        let m: Arc<SharedHashMap<u64, u64>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        let home = |k: u64| m.wrap(SharedHashMap::<u64, u64>::hash_key(&k) as usize);
        let a = 1u64;
        let b = (2u64..).find(|&k| home(k) == home(a)).unwrap();
        m.insert(a, 100).unwrap();
        let (pause, exchange) = stopped({
            let m = Arc::clone(&m);
            move || m.compare_exchange(&a, 100, 200)
        });
        assert_eq!(m.remove(&a), Some(100));
        let slot = home(a);
        m.slot(slot)
            .state
            .compare_exchange(SLOT_TOMBSTONE, SLOT_OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
            .expect("the remove left a's slot a tombstone");
        pause.release();
        assert_eq!(exchange.join().unwrap(), Err(MapError::KeyAbsent), "the compare_exchange finds the entry gone");
        m.publish_claimed(slot, SharedHashMap::<u64, u64>::hash_key(&b), &b, &20);
        m.header().count.fetch_add(1, Ordering::AcqRel);
        assert_eq!(m.get(&a), None);
        assert_eq!(m.get(&b), Some(20));
        assert_eq!(m.len(), 1);
    }

    /// A swap that has matched its key and not yet written holds the slot's
    /// lock, so a remove of the key waits on it and takes the value the
    /// swap put there, and the swap hands back the one it replaced: each
    /// value leaves the map once. An update that wrote outside the lock
    /// would let the remove take the old value inside its window, then
    /// write the new one into the tombstone, where it never leaves.
    #[test]
    fn a_swap_stopped_before_its_write_holds_off_a_remove() {
        let p = tmp("swap-vs-remove");
        let m: Arc<SharedHashMap<u64, u64>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        m.insert(7, 100).unwrap();
        let (pause, swap) = stopped({
            let m = Arc::clone(&m);
            move || m.swap(7, 200)
        });
        let remove = {
            let m = Arc::clone(&m);
            thread::spawn(move || m.remove(&7))
        };
        settle(&remove);
        assert!(!remove.is_finished(), "the remove waits on the slot lock the stopped swap holds");
        pause.release();
        let swapped = swap.join().unwrap();
        assert!(swapped == Ok(Some(100)), "the swap hands back the value it replaced: {swapped:?}");
        assert_eq!(remove.join().unwrap(), Some(200), "the remove hands back the value the swap put there");
        assert_eq!(m.get(&7), None);
        assert_eq!(m.len(), 0);
    }

    /// An update of a key that has matched it and not yet written holds
    /// the slot's lock, so a remove of the key and an insert of another
    /// key into the slot the remove leaves wait on it: the update lands on
    /// its own key's entry, and the other key's entry only ever holds the
    /// other key. An update that wrote outside the lock would let the
    /// remove and the insert land inside its window, then write its key's
    /// bytes under the other key's hash, where neither key is found again.
    #[test]
    fn an_update_stopped_before_its_write_holds_off_another_keys_claim_of_its_slot() {
        let p = tmp("update-vs-reclaim");
        let m: Arc<SharedHashMap<u64, u64>> = Arc::new(SharedHashMap::create(&p, 16).unwrap());
        let home = |k: u64| m.wrap(SharedHashMap::<u64, u64>::hash_key(&k) as usize);
        let a = 1u64;
        let b = (2u64..).find(|&k| home(k) == home(a)).unwrap();
        m.insert(a, 10).unwrap();
        let (pause, update) = stopped({
            let m = Arc::clone(&m);
            move || m.insert(a, 11).unwrap()
        });
        let reclaim = {
            let m = Arc::clone(&m);
            thread::spawn(move || {
                let removed = m.remove(&a);
                m.insert(b, 20).unwrap();
                removed
            })
        };
        settle(&reclaim);
        assert!(!reclaim.is_finished(), "the remove waits on the slot lock the stopped update holds");
        pause.release();
        assert_eq!(update.join().unwrap(), InsertOutcome::Updated);
        assert_eq!(reclaim.join().unwrap(), Some(11), "the remove takes the value the update wrote");
        assert_eq!(m.get(&a), None);
        assert_eq!(m.get(&b), Some(20));
        for i in 0..m.capacity() {
            let slot = m.slot(i);
            if slot.state.load(Ordering::Acquire) == SLOT_OCCUPIED {
                let k = m.read_payload(i).0;
                assert_eq!(
                    slot.hash.load(Ordering::Acquire),
                    SharedHashMap::<u64, u64>::hash_key(&k),
                    "slot {i} holds key {k} under another key's hash"
                );
            }
        }
        assert_eq!(m.len(), 1);
    }
}
