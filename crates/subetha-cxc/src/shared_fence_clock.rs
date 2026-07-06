//! `SharedFenceClock` - Hybrid Logical Clock (HLC) lifted to
//! cross-process MMF.
//!
//! Each participating process registers an HLC slot in a shared
//! table and publishes its `(physical_us, logical)` HLC there on
//! every meaningful event. Any reader can walk the table to compute
//! `global_fence = max(all slots)` - the timestamp at which all
//! process events are causally observable. That fence is exactly
//! what distributed snapshot isolation needs.
//!
//! # Why HLC instead of vector clocks
//!
//! Vector clocks give exact causal ordering but cost O(N) per event
//! (each event has to update N-dim coordinate). HLC gives total
//! order that respects causality with two u64 fields per process,
//! bounded difference from physical clock skew, and O(1) per event.
//! For cross-process snapshots over modest N (say <256 processes),
//! HLC's tradeoff dominates VC.
//!
//! # HLC update rules (Kulkarni et al.)
//!
//! - `tick`:
//!   - `wall = now()`, `new_phys = max(prev_phys, wall)`
//!   - `new_log = if new_phys == prev_phys { prev_log + 1 } else { 0 }`
//! - `merge(remote)`:
//!   - `new_phys = max(prev_phys, remote_phys, wall)`
//!   - `new_log = max(prev_log, remote_log) + 1` when both equal new_phys
//!   - `= prev_log + 1` when only prev equals new_phys
//!   - `= remote_log + 1` when only remote equals new_phys
//!   - `= 0` when wall strictly dominates
//!
//! # Layout
//!
//! ONE MMF file: `<base>.bin` with `HlcHeader` (64B) +
//! `HlcSlot[capacity]` (64B each, one cache line so cross-process
//! writes don't false-share).
//!
//! # Race tolerance
//!
//! Per-slot writes are: `physical.store(Release)` then
//! `logical.store(Release)`. A reader may observe a fresh physical
//! with stale logical (or vice versa). That's HLC-safe because:
//! - physical is monotonically non-decreasing
//! - logical only increases at a given physical
//! - the only invariant is total order, which lexicographic
//!   `(physical, logical)` preserves even with one-field skew
//!
//! For strict torn-write protection, wrap with SeqLock; we omit it
//! here because HLC's coarse-granularity guarantees absorb the
//! single-cycle window.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const FENCE_CLOCK_MAGIC: u64 = 0x4150_5546_434C_4B30;

/// HLC value: `(physical_us, logical)`. Total order is lexicographic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hlc {
    pub physical_us: u64,
    pub logical: u64,
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.physical_us.cmp(&other.physical_us) {
            std::cmp::Ordering::Equal => self.logical.cmp(&other.logical),
            other => other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceClockError {
    Full,
    LayoutMismatch,
    InvalidSlot,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for FenceClockError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub const EMPTY_PID: u32 = 0;

#[repr(C, align(64))]
pub struct HlcHeader {
    pub magic: u64,
    pub capacity: u64,
    pub global_fence_physical: AtomicU64,
    pub global_fence_logical: AtomicU64,
    pub last_fence_epoch: AtomicU64,
    /// Shared cross-process cached wall clock. Each process publishes its
    /// own 250 us-fresh local cache here via `fetch_max`, so every process
    /// mapping this MMF reads the freshest timestamp across all of them -
    /// reconciling the per-process cache *phase* on a single host (where
    /// the hardware clock is shared, so there is no real skew, only phase).
    /// Monotonic by construction. Occupies 8 of the former 16 pad bytes, so
    /// the header size and magic are unchanged.
    pub cached_us: AtomicU64,
    _pad: [u8; 8],
}

#[repr(C, align(64))]
pub struct HlcSlot {
    pub pid: AtomicU32,
    _pad1: [u8; 4],
    pub physical_us: AtomicU64,
    pub logical: AtomicU64,
    pub last_updated_us: AtomicU64,
    _pad2: [u8; 32],
}

const _: () = {
    assert!(std::mem::size_of::<HlcHeader>() == 64);
    assert!(std::mem::size_of::<HlcSlot>() == 64);
};

pub const fn fence_clock_file_size(capacity: usize) -> usize {
    std::mem::size_of::<HlcHeader>() + capacity * std::mem::size_of::<HlcSlot>()
}

pub struct SharedFenceClock {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedFenceClock {}
unsafe impl Sync for SharedFenceClock {}

impl subetha_sidecar::AdaptiveInstance for SharedFenceClock {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}


impl SharedFenceClock {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, FenceClockError> {
        assert!(capacity >= 1);
        // Start the background clock updater so `now_us` (a cached read) is
        // populated before the first tick.
        crate::cached_clock::start();
        let total = fence_clock_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut HlcHeader;
        unsafe {
            std::ptr::write(hdr, HlcHeader {
                magic: FENCE_CLOCK_MAGIC,
                capacity: capacity as u64,
                global_fence_physical: AtomicU64::new(0),
                global_fence_logical: AtomicU64::new(0),
                last_fence_epoch: AtomicU64::new(0),
                cached_us: AtomicU64::new(0),
                _pad: [0; 8],
            });
        }
        for i in 0..capacity {
            let slot_ptr = unsafe {
                mmap.as_mut_ptr()
                    .add(std::mem::size_of::<HlcHeader>())
                    .add(i * std::mem::size_of::<HlcSlot>())
            } as *mut HlcSlot;
            unsafe {
                std::ptr::write(slot_ptr, HlcSlot {
                    pid: AtomicU32::new(EMPTY_PID),
                    _pad1: [0; 4],
                    physical_us: AtomicU64::new(0),
                    logical: AtomicU64::new(0),
                    last_updated_us: AtomicU64::new(0),
                    _pad2: [0; 32],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, FenceClockError> {
        crate::cached_clock::start();
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = fence_clock_file_size(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(FenceClockError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const HlcHeader) };
        if hdr.magic != FENCE_CLOCK_MAGIC || hdr.capacity != expected_capacity as u64 {
            return Err(FenceClockError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    /// Current shared cross-process cached wall clock (microseconds). This
    /// is the freshest cache any process on the host has published to the
    /// MMF; it advances when any process ticks. One atomic load.
    #[inline]
    pub fn shared_clock_us(&self) -> u64 {
        self.header().cached_us.load(Ordering::Acquire)
    }

    fn header(&self) -> &HlcHeader {
        unsafe { &*(self.mmap.as_ptr() as *const HlcHeader) }
    }

    /// Cross-process cached wall clock. Reads this process's 250 us-fresh
    /// local cache and the MMF-shared value; if the local cache is fresher
    /// (this process advanced its phase first), it publishes it with
    /// `fetch_max` and returns it, otherwise it returns the shared value.
    /// The result is therefore the freshest across every process on the
    /// host, monotonic, and contended only ~once per refresh interval per
    /// process (the load is the common case; the write is rare). Cross-host
    /// skew is reconciled separately by [`merge`](Self::merge).
    #[inline]
    fn shared_now_us(&self) -> u64 {
        let local = crate::cached_clock::now_us();
        let cached = &self.header().cached_us;
        let shared = cached.load(Ordering::Acquire);
        if local > shared {
            cached.fetch_max(local, Ordering::AcqRel);
            local
        } else {
            shared
        }
    }

    fn slot(&self, idx: usize) -> &HlcSlot {
        assert!(idx < self.capacity, "slot index {idx} out of range {}", self.capacity);
        let base = unsafe {
            self.mmap.as_ptr().add(std::mem::size_of::<HlcHeader>())
        };
        unsafe {
            &*(base.add(idx * std::mem::size_of::<HlcSlot>()) as *const HlcSlot)
        }
    }

    /// Register the calling process. CAS-claims the first empty slot;
    /// returns slot index.
    pub fn register(&self, pid: u32) -> Result<usize, FenceClockError> {
        assert!(pid != EMPTY_PID, "pid must be != EMPTY_PID");
        for i in 0..self.capacity {
            let slot = self.slot(i);
            if slot.pid.compare_exchange(
                EMPTY_PID, pid, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                slot.physical_us.store(0, Ordering::Release);
                slot.logical.store(0, Ordering::Release);
                slot.last_updated_us.store(self.shared_now_us(), Ordering::Release);
                return Ok(i);
            }
        }
        Err(FenceClockError::Full)
    }

    /// Release a slot so another process can claim it.
    pub fn unregister(&self, idx: usize) {
        if idx >= self.capacity { return; }
        let slot = self.slot(idx);
        slot.physical_us.store(0, Ordering::Release);
        slot.logical.store(0, Ordering::Release);
        slot.pid.store(EMPTY_PID, Ordering::Release);
    }

    /// Local internal-event HLC tick. Advances this slot's HLC per
    /// the standard rules.
    pub fn tick(&self, idx: usize) -> Hlc {
        let slot = self.slot(idx);
        let wall = self.shared_now_us();
        let prev_phys = slot.physical_us.load(Ordering::Acquire);
        let prev_log = slot.logical.load(Ordering::Acquire);
        let new_phys = prev_phys.max(wall);
        let new_log = if new_phys == prev_phys { prev_log + 1 } else { 0 };
        slot.physical_us.store(new_phys, Ordering::Release);
        slot.logical.store(new_log, Ordering::Release);
        slot.last_updated_us.store(wall, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::fence_clock::OP_TICK, 0);
        Hlc { physical_us: new_phys, logical: new_log }
    }

    /// Merge a remote HLC (e.g., received in a message) into this
    /// slot. Returns the new local HLC.
    pub fn merge(&self, idx: usize, remote: Hlc) -> Hlc {
        let slot = self.slot(idx);
        let wall = self.shared_now_us();
        let prev_phys = slot.physical_us.load(Ordering::Acquire);
        let prev_log = slot.logical.load(Ordering::Acquire);
        let new_phys = prev_phys.max(remote.physical_us).max(wall);
        let new_log = if new_phys == prev_phys && new_phys == remote.physical_us {
            prev_log.max(remote.logical) + 1
        } else if new_phys == prev_phys {
            prev_log + 1
        } else if new_phys == remote.physical_us {
            remote.logical + 1
        } else {
            0
        };
        slot.physical_us.store(new_phys, Ordering::Release);
        slot.logical.store(new_log, Ordering::Release);
        slot.last_updated_us.store(wall, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::fence_clock::OP_MERGE, 0);
        Hlc { physical_us: new_phys, logical: new_log }
    }

    /// Read this slot's current HLC.
    pub fn get_local(&self, idx: usize) -> Hlc {
        let slot = self.slot(idx);
        let h = Hlc {
            physical_us: slot.physical_us.load(Ordering::Acquire),
            logical: slot.logical.load(Ordering::Acquire),
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::fence_clock::OP_GET_LOCAL, 0);
        h
    }

    /// Walk all slots and compute `max(slot.hlc)` across all non-vacant
    /// slots. This is the global fence: the timestamp at which all
    /// peers' events are observable.
    pub fn compute_global_fence(&self) -> Hlc {
        let mut max = Hlc { physical_us: 0, logical: 0 };
        for i in 0..self.capacity {
            let slot = self.slot(i);
            if slot.pid.load(Ordering::Acquire) == EMPTY_PID { continue; }
            let h = Hlc {
                physical_us: slot.physical_us.load(Ordering::Acquire),
                logical: slot.logical.load(Ordering::Acquire),
            };
            if h > max { max = h; }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::fence_clock::OP_COMPUTE_FENCE, 0);
        max
    }

    /// Publish the computed global fence to the header so other
    /// processes can read it via `read_global_fence` at O(1).
    pub fn publish_global_fence(&self) -> Hlc {
        let fence = self.compute_global_fence();
        let hdr = self.header();
        hdr.global_fence_physical.store(fence.physical_us, Ordering::Release);
        hdr.global_fence_logical.store(fence.logical, Ordering::Release);
        hdr.last_fence_epoch.fetch_add(1, Ordering::Release);
        fence
    }

    /// Read the most-recently-published global fence (O(1); set by a
    /// publisher process / coordinator).
    pub fn read_global_fence(&self) -> Hlc {
        let hdr = self.header();
        Hlc {
            physical_us: hdr.global_fence_physical.load(Ordering::Acquire),
            logical: hdr.global_fence_logical.load(Ordering::Acquire),
        }
    }

    /// Snapshot a specific slot (returns None when vacant).
    pub fn slot_snapshot(&self, idx: usize) -> Option<HlcSlotSnapshot> {
        if idx >= self.capacity { return None; }
        let slot = self.slot(idx);
        let pid = slot.pid.load(Ordering::Acquire);
        if pid == EMPTY_PID { return None; }
        Some(HlcSlotSnapshot {
            pid,
            hlc: Hlc {
                physical_us: slot.physical_us.load(Ordering::Acquire),
                logical: slot.logical.load(Ordering::Acquire),
            },
            last_updated_us: slot.last_updated_us.load(Ordering::Acquire),
        })
    }

    /// Total fence-publish epochs (counter bumped by `publish_global_fence`).
    pub fn fence_epoch(&self) -> u64 {
        self.header().last_fence_epoch.load(Ordering::Acquire)
    }

    pub fn flush(&self) -> Result<(), FenceClockError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), FenceClockError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlcSlotSnapshot {
    pub pid: u32,
    pub hlc: Hlc,
    pub last_updated_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-fenceclock-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let c = SharedFenceClock::create(&p, 4).unwrap();
        assert_eq!(c.capacity(), 4);
        assert_eq!(c.compute_global_fence(), Hlc { physical_us: 0, logical: 0 });
        for i in 0..4 { assert!(c.slot_snapshot(i).is_none()); }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn register_returns_distinct_slot_indices() {
        let p = tmp("reg");
        let c = SharedFenceClock::create(&p, 4).unwrap();
        let s0 = c.register(1001).unwrap();
        let s1 = c.register(1002).unwrap();
        assert_ne!(s0, s1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn register_fails_when_table_is_full() {
        let p = tmp("full");
        let c = SharedFenceClock::create(&p, 2).unwrap();
        c.register(1).unwrap();
        c.register(2).unwrap();
        assert_eq!(c.register(3).err(), Some(FenceClockError::Full));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn tick_advances_physical_and_resets_logical() {
        let p = tmp("tick");
        let c = SharedFenceClock::create(&p, 2).unwrap();
        let idx = c.register(1).unwrap();
        let h1 = c.tick(idx);
        thread::sleep(std::time::Duration::from_millis(2));
        let h2 = c.tick(idx);
        assert!(h2 > h1, "second tick should be strictly greater");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn tick_increments_logical_when_physical_unchanged() {
        let p = tmp("logical");
        let c = SharedFenceClock::create(&p, 2).unwrap();
        let idx = c.register(1).unwrap();
        // Force physical to a very high value so subsequent ticks
        // within the same microsecond keep the same physical and bump
        // logical.
        let slot = c.slot(idx);
        slot.physical_us.store(u64::MAX / 2, Ordering::Release);
        slot.logical.store(0, Ordering::Release);
        let h0 = c.tick(idx);
        let h1 = c.tick(idx);
        assert_eq!(h0.physical_us, h1.physical_us);
        assert_eq!(h1.logical, h0.logical + 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn merge_picks_max_physical_and_increments_logical() {
        let p = tmp("merge");
        let c = SharedFenceClock::create(&p, 2).unwrap();
        let idx = c.register(1).unwrap();
        // Set local to a known value.
        let slot = c.slot(idx);
        slot.physical_us.store(1000, Ordering::Release);
        slot.logical.store(5, Ordering::Release);
        // Merge a remote with higher physical.
        let remote = Hlc { physical_us: 2000, logical: 3 };
        // Note: now_us() will likely dominate both (it's wall time).
        // To test the merge logic isolated from wall, the remote
        // physical must exceed both prev and now_us().
        let remote_far_future = Hlc { physical_us: u64::MAX / 2, logical: 7 };
        let merged = c.merge(idx, remote_far_future);
        assert_eq!(merged.physical_us, u64::MAX / 2);
        assert_eq!(merged.logical, 8);
        // (the older merge is not observable here; the local
        // dropped <- u64::MAX/2 / 8 was the test signal.)
        let _remote = remote;
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compute_global_fence_returns_max_of_all_slots() {
        let p = tmp("global");
        let c = SharedFenceClock::create(&p, 4).unwrap();
        let a = c.register(10).unwrap();
        let b = c.register(20).unwrap();
        let d = c.register(30).unwrap();
        // Manually set each slot to a known HLC.
        c.slot(a).physical_us.store(100, Ordering::Release);
        c.slot(a).logical.store(5, Ordering::Release);
        c.slot(b).physical_us.store(200, Ordering::Release);
        c.slot(b).logical.store(0, Ordering::Release);
        c.slot(d).physical_us.store(150, Ordering::Release);
        c.slot(d).logical.store(9, Ordering::Release);
        let fence = c.compute_global_fence();
        assert_eq!(fence, Hlc { physical_us: 200, logical: 0 });
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn publish_and_read_global_fence_round_trip() {
        let p = tmp("publish");
        let c = SharedFenceClock::create(&p, 2).unwrap();
        let idx = c.register(1).unwrap();
        c.slot(idx).physical_us.store(7777, Ordering::Release);
        c.slot(idx).logical.store(3, Ordering::Release);
        let published = c.publish_global_fence();
        let read = c.read_global_fence();
        assert_eq!(read, published);
        assert_eq!(read, Hlc { physical_us: 7777, logical: 3 });
        assert_eq!(c.fence_epoch(), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_fence_visible() {
        let p = tmp("cross-handle");
        let owner = SharedFenceClock::create(&p, 4).unwrap();
        let observer = SharedFenceClock::open(&p, 4).unwrap();
        let idx = owner.register(42).unwrap();
        owner.slot(idx).physical_us.store(5555, Ordering::Release);
        owner.slot(idx).logical.store(1, Ordering::Release);
        let owner_fence = owner.publish_global_fence();
        let observer_fence = observer.read_global_fence();
        assert_eq!(owner_fence, observer_fence);
        // Observer can also recompute directly.
        assert_eq!(observer.compute_global_fence(), Hlc { physical_us: 5555, logical: 1 });
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_ticks_remain_monotonic() {
        let p = tmp("monotonic");
        let c = Arc::new(SharedFenceClock::create(&p, 8).unwrap());
        let mut handles = vec![];
        for t in 0..4 {
            let c = c.clone();
            handles.push(thread::spawn(move || {
                let idx = c.register(1000 + t as u32).unwrap();
                let mut prev = Hlc { physical_us: 0, logical: 0 };
                for _ in 0..50 {
                    let cur = c.tick(idx);
                    assert!(cur > prev, "tick must produce strictly greater HLC");
                    prev = cur;
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let fence = c.compute_global_fence();
        assert!(fence.physical_us > 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn unregister_clears_slot() {
        let p = tmp("unreg");
        let c = SharedFenceClock::create(&p, 4).unwrap();
        let idx = c.register(999).unwrap();
        c.tick(idx);
        assert!(c.slot_snapshot(idx).is_some());
        c.unregister(idx);
        assert!(c.slot_snapshot(idx).is_none());
        // Slot can be re-claimed.
        let idx2 = c.register(1000).unwrap();
        assert_eq!(idx, idx2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_fence_survives_reopen() {
        let p = tmp("disk");
        {
            let c = SharedFenceClock::create(&p, 4).unwrap();
            let idx = c.register(100).unwrap();
            c.slot(idx).physical_us.store(1234, Ordering::Release);
            c.slot(idx).logical.store(7, Ordering::Release);
            c.publish_global_fence();
            c.flush().unwrap();
        }
        let c2 = SharedFenceClock::open(&p, 4).unwrap();
        assert_eq!(c2.read_global_fence(), Hlc { physical_us: 1234, logical: 7 });
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn hlc_ord_is_lexicographic() {
        let a = Hlc { physical_us: 100, logical: 5 };
        let b = Hlc { physical_us: 100, logical: 6 };
        let c = Hlc { physical_us: 101, logical: 0 };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
        assert_eq!(a.max(c), c);
    }
}
