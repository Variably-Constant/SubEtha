//! `SpscRingCore` - Lamport 1983 single-producer / single-consumer
//! ring backed by a memory-mapped file.
//!
//! This is the SPSC-specialised counterpart of
//! [`SharedRing`](crate::SharedRing). Where `SharedRing` carries
//! the Vyukov MPMC protocol (per-slot sequence number, CAS on the
//! producer / consumer counters), `SpscRingCore` strips the protocol
//! down to its SPSC minimum: a head counter the producer owns, a
//! tail counter the consumer owns, and payload-only slots.
//!
//! # Per-op atomic budget
//!
//! Push:
//!  1. `head.load(Relaxed)` - owner-private; no cross-thread contention.
//!  2. `tail.load(Acquire)` - read the consumer's position to check full.
//!  3. write payload (non-atomic memcpy into the slot).
//!  4. `head.store(head + 1, Release)` - publish to the consumer.
//!
//! That is **one Acquire load + one Release store** of cross-thread-
//! visible atomics, plus one owner-private Relaxed load. Vyukov MPMC
//! on the same shape needs four cross-thread atomics (load + CAS on
//! `producer_seq`, then load + store on the slot's sequence number).
//! Halving the atomic budget is where the Lamport SPSC win comes from.
//!
//! Pop mirrors push.
//!
//! # False sharing
//!
//! `head` and `tail` live on separate 64-byte cache lines. The
//! producer writes head every push; the consumer writes tail every
//! pop. Co-locating them would invalidate the peer's cache line on
//! every op and crater throughput.
//!
//! # Crash recovery
//!
//! Producer crash: if the sole producer dies between writing payload
//! and the Release-store on `head`, the head counter never advances
//! and the consumer sees no new item. The slot at `head % cap`
//! contains partial / garbage bytes, but the consumer never reads
//! it because head was not published. There is no stuck-slot
//! pathology to heal - unlike `SharedRing`'s Vyukov protocol, the
//! producer never "claims" a slot before publishing.
//!
//! Consumer crash: same shape; tail does not advance, head keeps
//! growing, ring fills, producer eventually returns `Full`.
//!
//! In SPSC there is no second producer to take over from the dead
//! one, so producer-side recovery is "restart the sole producer".
//! No heal_stuck_slot equivalent is needed or possible here.

use std::cell::UnsafeCell;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_ring::RingError;

/// Magic number identifying a Lamport SPSC ring header. ASCII
/// "SPSC" + version byte.
pub const SPSC_MAGIC: u64 = 0x5350_5343_0000_0001;

/// Each slot is exactly one cache line. Payload-only (no per-slot
/// atomic), so the full 64 bytes are available to the caller.
pub const SPSC_SLOT_SIZE: usize = 64;

/// Payload bytes per slot.
pub const SPSC_PAYLOAD_BYTES: usize = SPSC_SLOT_SIZE;

/// Header layout for a Lamport SPSC ring. Three cache lines:
/// metadata, then producer-owned `head`, then consumer-owned `tail`.
/// Separate cache lines for `head` and `tail` eliminate false
/// sharing between producer and consumer hot paths.
#[repr(C, align(64))]
pub struct SpscHeader {
    pub magic: u64,
    pub capacity: u64,
    pub slot_size: u64,
    /// Pad metadata line out to 64 bytes.
    _pad_meta: [u8; 64 - 24],
    /// Producer-owned head counter; consumer reads via Acquire.
    pub head: AtomicU64,
    _pad_head: [u8; 64 - 8],
    /// Consumer-owned tail counter; producer reads via Acquire.
    pub tail: AtomicU64,
    _pad_tail: [u8; 64 - 8],
}

#[repr(C, align(64))]
pub struct SpscSlot {
    pub payload: UnsafeCell<[u8; SPSC_PAYLOAD_BYTES]>,
}

unsafe impl Sync for SpscSlot {}

/// Total file size for a ring of `capacity` payload slots, including
/// the three-cache-line header.
pub const fn spsc_ring_file_size(capacity: usize) -> usize {
    std::mem::size_of::<SpscHeader>() + capacity * SPSC_SLOT_SIZE
}

/// Caller-owned memory a ring can be laid out in: huge / large pages,
/// or any region. The ring takes ownership (keeping it mapped) and
/// writes its header + slots into the region's bytes. Implemented for
/// `HugepageRegion` (Linux), `LargePageRegion` / `LargePageSection`
/// (Windows) - so a ring can sit on 2 MB / 1 GB pages and shed the TLB
/// pressure of thousands of 4 KB pages, which matters once you have
/// many rings or one very large one.
/// The ring header is `align(64)`, so the region base must be
/// 64-byte aligned. Page-backed regions (huge / large pages, mmap)
/// satisfy this by construction; a hand-rolled region must align its
/// buffer or the constructor returns
/// [`RingError::LayoutMismatch`](crate::shared_ring::RingError).
pub trait RegionOwner: Send + Sync + 'static {
    /// Pointer to the start of the region (must be 64-byte aligned).
    fn region_ptr(&mut self) -> *mut u8;
    /// Region length in bytes (must be >= the ring's file size).
    fn region_len(&self) -> usize;
}

/// Cache-line / header alignment every ring layout requires.
const REGION_ALIGN: usize = 64;

/// Backing-store discriminator for `SpscRingCore`. The variant
/// holds the underlying memory owner so it stays alive for the
/// lifetime of the ring; the raw byte access goes through
/// `SpscRingCore::raw_ptr`. The held values are intentionally
/// never read directly (lifetime extension only).
#[allow(dead_code)]
enum SpscBacking {
    /// Anonymous in-process memory.
    Anon(MmapMut),
    /// File-backed (cross-process via page cache).
    File(File, MmapMut),
    /// Named RAM-resident shared memory (cross-process, no page cache).
    Shm(crate::shm_file::ShmFile),
    /// Caller-owned region (huge / large pages, or any `RegionOwner`).
    Region(Box<dyn RegionOwner>),
}

/// Lamport SPSC ring core. Used as the storage backing
/// [`SharedRingSpsc`](crate::SharedRingSpsc); applications normally
/// reach for the typed `Producer` / `Consumer` halves rather than
/// this raw core.
pub struct SpscRingCore {
    /// Owns the underlying memory; never accessed directly after
    /// construction (raw_ptr captures the pointer once).
    _backing: SpscBacking,
    /// Stable byte pointer into the backing for the lifetime of self.
    /// Header lives at byte 0; slots start at offset
    /// `size_of::<SpscHeader>()`. Total mapped size is
    /// `spsc_ring_file_size(capacity)`.
    raw_ptr: *mut u8,
    capacity: usize,
}

unsafe impl Send for SpscRingCore {}
unsafe impl Sync for SpscRingCore {}

fn init_spsc_layout(mmap: &mut MmapMut, capacity: usize) {
    unsafe { init_spsc_layout_raw(mmap.as_mut_ptr(), capacity) };
}

/// Backing-agnostic layout init. Writes the header and zeroes the
/// payload slots at the given raw pointer. Caller guarantees that
/// `ptr` points to at least `spsc_ring_file_size(capacity)` bytes
/// of mutable, suitably-aligned memory.
unsafe fn init_spsc_layout_raw(ptr: *mut u8, capacity: usize) {
    let header_ptr = ptr as *mut SpscHeader;
    unsafe {
        std::ptr::write(header_ptr, SpscHeader {
            magic: SPSC_MAGIC,
            capacity: capacity as u64,
            slot_size: SPSC_SLOT_SIZE as u64,
            _pad_meta: [0; 64 - 24],
            head: AtomicU64::new(0),
            _pad_head: [0; 64 - 8],
            tail: AtomicU64::new(0),
            _pad_tail: [0; 64 - 8],
        });
    }
    let slots_base = unsafe { ptr.add(std::mem::size_of::<SpscHeader>()) };
    for i in 0..capacity {
        let slot_ptr = unsafe { slots_base.add(i * SPSC_SLOT_SIZE) as *mut SpscSlot };
        unsafe {
            std::ptr::write(slot_ptr, SpscSlot {
                payload: UnsafeCell::new([0; SPSC_PAYLOAD_BYTES]),
            });
        }
    }
}

impl SpscRingCore {
    /// Anonymous in-memory ring (in-process only). Fastest construction;
    /// skips file create + ftruncate + first-page-fault.
    pub fn create_anon(capacity: usize) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = spsc_ring_file_size(capacity);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        init_spsc_layout(&mut mmap, capacity);
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: SpscBacking::Anon(mmap),
            raw_ptr, capacity,
        })
    }

    /// File-backed ring; cross-process visibility via the OS page cache.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = spsc_ring_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        init_spsc_layout(&mut mmap, capacity);
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: SpscBacking::File(file, mmap),
            raw_ptr, capacity,
        })
    }

    /// Open an existing file-backed ring. Validates magic + capacity.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, RingError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = spsc_ring_file_size(expected_capacity);
        let actual_len = file.metadata()?.len();
        if (actual_len as usize) < total {
            return Err(RingError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let header = unsafe { &*(mmap.as_ptr() as *const SpscHeader) };
        if header.magic != SPSC_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != SPSC_SLOT_SIZE as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: SpscBacking::File(file, mmap),
            raw_ptr, capacity: expected_capacity,
        })
    }

    /// Build a fresh ring on top of a named RAM-resident
    /// shared-memory backing. Cross-process visible via the
    /// `logical_name` of the underlying `ShmFile`; never touches the
    /// page cache. The `ShmFile` must be sized to at least
    /// `spsc_ring_file_size(capacity)` bytes.
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = spsc_ring_file_size(capacity);
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        // Initialize the layout in the shared region.
        let slice = shm.as_mut_slice();
        let raw_ptr = slice.as_mut_ptr();
        unsafe {
            init_spsc_layout_raw(raw_ptr, capacity);
        }
        Ok(Self {
            _backing: SpscBacking::Shm(shm),
            raw_ptr, capacity,
        })
    }

    /// Open an existing named ShmFs-backed ring. Validates magic +
    /// capacity. Does NOT re-initialize the layout - the layout must
    /// already be present from a prior `create_from_shm` on the same
    /// logical name.
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile,
        expected_capacity: usize,
    ) -> Result<Self, RingError> {
        let total = spsc_ring_file_size(expected_capacity);
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        let slice = shm.as_mut_slice();
        let raw_ptr = slice.as_mut_ptr();
        let header = unsafe { &*(raw_ptr as *const SpscHeader) };
        if header.magic != SPSC_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != SPSC_SLOT_SIZE as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        Ok(Self {
            _backing: SpscBacking::Shm(shm),
            raw_ptr, capacity: expected_capacity,
        })
    }

    /// Build a fresh ring laid out in caller-owned memory (huge / large
    /// pages, or any [`RegionOwner`]). The region must be at least
    /// `spsc_ring_file_size(capacity)` bytes; the ring owns it for its
    /// lifetime so the pages stay mapped.
    pub fn create_in_region<R: RegionOwner>(
        mut region: R, capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        if region.region_len() < spsc_ring_file_size(capacity) {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = region.region_ptr();
        if !(raw_ptr as usize).is_multiple_of(REGION_ALIGN) {
            return Err(RingError::LayoutMismatch);
        }
        unsafe { init_spsc_layout_raw(raw_ptr, capacity) };
        Ok(Self {
            _backing: SpscBacking::Region(Box::new(region)),
            raw_ptr, capacity,
        })
    }

    /// Attach to an existing ring already laid out in `region` - e.g. a
    /// `LargePageSection` another process created under the same name.
    /// Validates the header and does NOT re-initialise.
    pub fn open_in_region<R: RegionOwner>(
        mut region: R, expected_capacity: usize,
    ) -> Result<Self, RingError> {
        if region.region_len() < spsc_ring_file_size(expected_capacity) {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = region.region_ptr();
        if !(raw_ptr as usize).is_multiple_of(REGION_ALIGN) {
            return Err(RingError::LayoutMismatch);
        }
        let header = unsafe { &*(raw_ptr as *const SpscHeader) };
        if header.magic != SPSC_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != SPSC_SLOT_SIZE as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        Ok(Self {
            _backing: SpscBacking::Region(Box::new(region)),
            raw_ptr, capacity: expected_capacity,
        })
    }

    /// Capacity in slots (always a power of 2).
    pub fn capacity(&self) -> usize { self.capacity }

    fn header(&self) -> &SpscHeader {
        unsafe { &*(self.raw_ptr as *const SpscHeader) }
    }

    fn slot(&self, idx: usize) -> &SpscSlot {
        let slots_base = unsafe {
            self.raw_ptr.add(std::mem::size_of::<SpscHeader>())
        };
        let masked = idx & (self.capacity - 1);
        unsafe { &*(slots_base.add(masked * SPSC_SLOT_SIZE) as *const SpscSlot) }
    }

    /// Producer's published index. Cross-thread visible.
    pub fn head(&self) -> u64 { self.header().head.load(Ordering::Acquire) }

    /// Consumer's published index. Cross-thread visible.
    pub fn tail(&self) -> u64 { self.header().tail.load(Ordering::Acquire) }

    /// The producer's publish signal: the head counter the
    /// consumer-side monitor-wait arms on. The producer's
    /// Release-store to this atom on every push is the wake.
    pub fn head_signal(&self) -> &AtomicU64 {
        &self.header().head
    }

    /// Number of items waiting (`head - tail`).
    pub fn approx_len(&self) -> usize {
        let h = self.head();
        let t = self.tail();
        h.saturating_sub(t) as usize
    }

    /// SPSC push. **Caller is the sole producer** (enforced by the
    /// `Producer` newtype that owns this ring via `Arc`). Lamport
    /// pattern: read tail to check fullness, write payload, Release-
    /// store head to publish.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        if payload.len() > SPSC_PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let header = self.header();
        let head = header.head.load(Ordering::Relaxed);
        let tail = header.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= self.capacity as u64 {
            return Err(RingError::Full);
        }
        let slot = self.slot(head as usize);
        // Copy stays on `ptr::copy_nonoverlapping`: at one-line
        // sizes the baseline inlined movups codegen beats the
        // dispatched wide-register kernel by ~25% (the dispatch
        // branch + call cost more than the lanes save; measured by
        // examples/cacheline_probe.rs).
        unsafe {
            let dst = (*slot.payload.get()).as_mut_ptr();
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
            if payload.len() < SPSC_PAYLOAD_BYTES {
                std::ptr::write_bytes(
                    dst.add(payload.len()), 0,
                    SPSC_PAYLOAD_BYTES - payload.len(),
                );
            }
        }
        header.head.store(head + 1, Ordering::Release);
        // The slot line's next reader is the consumer core; demote
        // it toward the shared LLC (NOP without CLDEMOTE support).
        crate::cache_ops::cldemote(slot as *const SpscSlot as *const u8);
        Ok(())
    }

    /// SPSC pop. **Caller is the sole consumer.** Lamport pattern:
    /// read head to check emptiness, read payload, Release-store tail
    /// to free the slot.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        if out.len() < SPSC_PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let header = self.header();
        let tail = header.tail.load(Ordering::Relaxed);
        let head = header.head.load(Ordering::Acquire);
        if tail == head {
            return Err(RingError::Empty);
        }
        let slot = self.slot(tail as usize);
        unsafe {
            let src = (*slot.payload.get()).as_ptr();
            std::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), SPSC_PAYLOAD_BYTES);
        }
        header.tail.store(tail + 1, Ordering::Release);
        // The freed slot's next toucher is the producer core.
        crate::cache_ops::cldemote(slot as *const SpscSlot as *const u8);
        Ok(SPSC_PAYLOAD_BYTES)
    }

    /// Peek the next slot WITHOUT copying or releasing it. Returns
    /// a [`PeekedSlot`] guard that derefs to `&[u8]` pointing
    /// directly into the mapped region. Caller passes this slice to
    /// downstream consumers (e.g. quinn's `SendStream::write_all`)
    /// without an intermediate copy. When done, call
    /// [`PeekedSlot::confirm`] to advance the consumer position and
    /// release the slot. Drop without confirming leaves the slot
    /// in place; the next `peek_slot` returns it again.
    ///
    /// Returns `None` when the ring is empty. **Caller is the sole
    /// consumer.**
    pub fn peek_slot(&self) -> Option<PeekedSlot<'_>> {
        let header = self.header();
        let tail = header.tail.load(Ordering::Relaxed);
        let head = header.head.load(Ordering::Acquire);
        if tail == head {
            return None;
        }
        let slot = self.slot(tail as usize);
        let payload_ptr = unsafe { (*slot.payload.get()).as_ptr() };
        let payload_slice = unsafe {
            std::slice::from_raw_parts(payload_ptr, SPSC_PAYLOAD_BYTES)
        };
        Some(PeekedSlot {
            ring: self,
            tail,
            payload: payload_slice,
        })
    }

    /// Force any dirty MMF pages to disk. Only meaningful for the
    /// file-backed mode; no-op on anonymous and ShmFs mappings
    /// (which never touch disk).
    pub fn flush(&self) -> Result<(), RingError> {
        match &self._backing {
            SpscBacking::File(_, mmap) => {
                mmap.flush()?;
            }
            SpscBacking::Anon(_)
            | SpscBacking::Shm(_)
            | SpscBacking::Region(_) => {
                // No disk to flush to (region-backed rings live in
                // huge / large pages or other caller-owned RAM).
            }
        }
        Ok(())
    }
}

/// Zero-copy view into the next consumer slot of an [`SpscRingCore`].
///
/// Derefs to `&[u8]` pointing INTO the mapped region; pass that
/// slice directly to downstream consumers (network egress, file
/// writers) without an intermediate stack copy. Call
/// [`PeekedSlot::confirm`] when done to release the slot;
/// dropping without confirming leaves the slot in place.
pub struct PeekedSlot<'a> {
    ring: &'a SpscRingCore,
    tail: u64,
    payload: &'a [u8],
}

impl<'a> PeekedSlot<'a> {
    /// The slot's payload bytes. Same as the `Deref` impl; explicit
    /// method form for clarity at call sites.
    pub fn as_slice(&self) -> &[u8] { self.payload }

    /// Length of the payload region (always [`SPSC_PAYLOAD_BYTES`]).
    pub fn len(&self) -> usize { self.payload.len() }

    /// Whether the payload is empty (always false for a valid peek;
    /// method exists for clippy's `len_without_is_empty`).
    pub fn is_empty(&self) -> bool { self.payload.is_empty() }

    /// Release the slot, advancing the consumer position.
    pub fn confirm(self) {
        let header = self.ring.header();
        header.tail.store(self.tail + 1, Ordering::Release);
    }
}

impl<'a> std::ops::Deref for PeekedSlot<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] { self.payload }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_thread_round_trip() {
        let ring = SpscRingCore::create_anon(8).unwrap();
        let payload = [0xABu8; SPSC_PAYLOAD_BYTES];
        ring.try_push(&payload).unwrap();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        ring.try_pop(&mut out).unwrap();
        assert_eq!(out, payload);
        assert_eq!(ring.try_pop(&mut out).unwrap_err(), RingError::Empty);
    }

    #[test]
    fn shm_round_trip() {
        use crate::shm_file::ShmFile;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let name = format!("spsc_shm_rt_{}_{}", std::process::id(), nonce);
        let capacity = 8;
        let size = spsc_ring_file_size(capacity);
        let shm = ShmFile::create_or_open_named(&name, size)
            .expect("shm create");
        let ring = SpscRingCore::create_from_shm(shm, capacity).unwrap();

        let payload = [0xCDu8; SPSC_PAYLOAD_BYTES];
        ring.try_push(&payload).unwrap();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        ring.try_pop(&mut out).unwrap();
        assert_eq!(out, payload);
    }

    #[test]
    fn shm_cross_handle_visibility() {
        use crate::shm_file::ShmFile;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let name = format!("spsc_shm_xshare_{}_{}", std::process::id(), nonce);
        let capacity = 8;
        let size = spsc_ring_file_size(capacity);

        // Producer side: create the ring (initialises layout).
        let shm_a = ShmFile::create_or_open_named(&name, size).expect("shm A");
        let producer_ring = SpscRingCore::create_from_shm(shm_a, capacity).unwrap();

        // Consumer side: open the SAME named region; layout already
        // initialised so use open_from_shm.
        let shm_b = ShmFile::create_or_open_named(&name, size).expect("shm B");
        let consumer_ring = SpscRingCore::open_from_shm(shm_b, capacity).unwrap();

        // Push via A, pop via B - cross-handle visibility through
        // the shared RAM region.
        let payload = [0x42u8; SPSC_PAYLOAD_BYTES];
        producer_ring.try_push(&payload).unwrap();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        consumer_ring.try_pop(&mut out).unwrap();
        assert_eq!(out, payload);
    }

    #[test]
    fn fills_to_capacity_then_full() {
        let ring = SpscRingCore::create_anon(4).unwrap();
        for i in 0..4u8 {
            ring.try_push(&[i; SPSC_PAYLOAD_BYTES]).unwrap();
        }
        assert_eq!(
            ring.try_push(&[99u8; SPSC_PAYLOAD_BYTES]).unwrap_err(),
            RingError::Full,
        );
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        ring.try_pop(&mut out).unwrap();
        ring.try_push(&[99u8; SPSC_PAYLOAD_BYTES]).unwrap();
    }

    #[test]
    fn two_thread_high_volume_round_trip() {
        let ring = Arc::new(SpscRingCore::create_anon(64).unwrap());
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        const N: u32 = 100_000;

        let producer = thread::spawn(move || {
            for i in 0..N {
                let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                buf[..4].copy_from_slice(&i.to_le_bytes());
                while ring_p.try_push(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut out = [0u8; SPSC_PAYLOAD_BYTES];
            let mut sum: u64 = 0;
            let mut received: u32 = 0;
            while received < N {
                if ring_c.try_pop(&mut out).is_ok() {
                    sum += u32::from_le_bytes(out[..4].try_into().unwrap()) as u64;
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            sum
        });

        producer.join().unwrap();
        let sum = consumer.join().unwrap();
        let expected: u64 = (0..N).map(u64::from).sum();
        assert_eq!(sum, expected);
    }

    #[test]
    fn peek_drop_without_confirm_leaves_item_in_place() {
        let ring = SpscRingCore::create_anon(8).unwrap();
        let payload = [0x5Au8; SPSC_PAYLOAD_BYTES];
        ring.try_push(&payload).unwrap();

        // Peek and drop WITHOUT confirming: the slot must stay.
        {
            let peek = ring.peek_slot().unwrap();
            assert_eq!(peek.as_slice(), &payload[..]);
        }
        assert_eq!(ring.approx_len(), 1,
                   "dropping a peek must not consume the slot");

        // The next peek returns the same item; confirming releases it.
        let peek = ring.peek_slot().unwrap();
        assert_eq!(peek.as_slice(), &payload[..]);
        peek.confirm();
        assert!(ring.peek_slot().is_none());
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        assert_eq!(ring.try_pop(&mut out).unwrap_err(), RingError::Empty);
    }

    /// A 64-byte-aligned heap region exercises the `RegionOwner` wiring
    /// with no huge-page privilege needed. The element type forces the
    /// Vec's buffer onto a cache-line boundary, matching what page-
    /// backed regions give for free. The huge-page (Linux) and large-
    /// page (Windows) backings travel the exact same `create_in_region`
    /// path, proven end to end in `examples/large_page_ring.rs`.
    #[repr(C, align(64))]
    #[derive(Clone, Copy)]
    struct Block64([u8; 64]);

    struct HeapRegion {
        blocks: Vec<Block64>,
    }
    impl HeapRegion {
        fn new(bytes: usize) -> Self {
            Self { blocks: vec![Block64([0u8; 64]); bytes.div_ceil(64)] }
        }
    }
    impl RegionOwner for HeapRegion {
        fn region_ptr(&mut self) -> *mut u8 {
            self.blocks.as_mut_ptr() as *mut u8
        }
        fn region_len(&self) -> usize { self.blocks.len() * 64 }
    }

    #[test]
    fn create_in_region_round_trips() {
        let cap = 16usize;
        let region = HeapRegion::new(spsc_ring_file_size(cap));
        let ring = SpscRingCore::create_in_region(region, cap).unwrap();
        assert_eq!(ring.capacity(), cap);

        // Three full laps, so head/tail wrap past capacity and the
        // region-laid-out slots are reused in place.
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        for round in 0..3u64 {
            for i in 0..cap as u64 {
                let v = round * cap as u64 + i;
                let mut p = [0u8; SPSC_PAYLOAD_BYTES];
                p[..8].copy_from_slice(&v.to_le_bytes());
                ring.try_push(&p).unwrap();
            }
            for i in 0..cap as u64 {
                ring.try_pop(&mut out).unwrap();
                let got = u64::from_le_bytes(out[..8].try_into().unwrap());
                assert_eq!(got, round * cap as u64 + i);
            }
        }
    }

    #[test]
    fn create_in_region_rejects_short_region() {
        // One cache line short of the layout the ring needs.
        let cap = 16usize;
        let short = spsc_ring_file_size(cap) - 64;
        let region = HeapRegion::new(short);
        assert!(region.region_len() < spsc_ring_file_size(cap));
        // SpscRingCore is not Debug, so match rather than unwrap_err.
        assert!(matches!(
            SpscRingCore::create_in_region(region, cap),
            Err(RingError::LayoutMismatch),
        ));
    }

    #[test]
    fn open_in_region_attaches_to_initialised_layout() {
        // Lay a ring out in a region, push an item, then attach a second
        // handle to the SAME bytes via open_in_region (no re-init) and
        // drain through it - the cross-process LargePageSection path in
        // miniature, with a heap region standing in for the section.
        let cap = 8usize;
        let bytes = spsc_ring_file_size(cap);
        // 64-byte-aligned backing both views map (a named section in
        // miniature; two processes would each hold their own view).
        let mut whole: Vec<Block64> = vec![Block64([0u8; 64]); bytes.div_ceil(64)];
        let base = whole.as_mut_ptr() as *mut u8;
        unsafe { init_spsc_layout_raw(base, cap) };

        // Two non-owning views over the same bytes (this is what two
        // processes mapping one named section would each hold).
        struct ViewRegion { ptr: *mut u8, len: usize }
        unsafe impl Send for ViewRegion {}
        unsafe impl Sync for ViewRegion {}
        impl RegionOwner for ViewRegion {
            fn region_ptr(&mut self) -> *mut u8 { self.ptr }
            fn region_len(&self) -> usize { self.len }
        }

        let producer = SpscRingCore::open_in_region(
            ViewRegion { ptr: base, len: bytes }, cap,
        ).unwrap();
        let consumer = SpscRingCore::open_in_region(
            ViewRegion { ptr: base, len: bytes }, cap,
        ).unwrap();

        let payload = [0x42u8; SPSC_PAYLOAD_BYTES];
        producer.try_push(&payload).unwrap();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        consumer.try_pop(&mut out).unwrap();
        assert_eq!(out, payload);
        // `whole` is declared before the views, so scope order drops it
        // LAST - the backing bytes outlive both ring handles.
    }

    #[test]
    fn open_round_trips_with_file() {
        let p = std::env::temp_dir().join(format!(
            "subetha-test-spsc-{}.bin", std::process::id(),
        ));
        std::fs::remove_file(&p).ok();
        {
            let _r = SpscRingCore::create(&p, 16).unwrap();
        }
        let r2 = SpscRingCore::open(&p, 16).unwrap();
        assert_eq!(r2.capacity(), 16);
        std::fs::remove_file(&p).ok();
    }
}
