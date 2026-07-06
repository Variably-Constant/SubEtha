//! `PriorityFanout` - tiered work queue with O(1) priority selection.
//!
//! Composes K [`SharedRing`]s (one per priority
//! level) with a single [`SharedAtomicU64`]
//! bitmap of active priorities. Consumers find the highest non-empty
//! priority in one CLZ instruction; producers route by priority tag.
//!
//! # Why O(1) priority selection matters
//!
//! Naive priority queues either:
//! - Use ONE ring with a priority field and scan linearly: O(K) per drain.
//! - Use a binary heap: O(log K) per submit AND drain, plus heap reorg
//!   under contention is hard to make lock-free.
//!
//! PriorityFanout pays O(1) on both sides: one fetch_or to set a bit
//! on submit, one CLZ + bit-clear on drain. The bitmap fits in a
//! single cache line so it's contention-friendly under load.
//!
//! # Layout
//!
//! K+1 MMF files for K priority levels:
//! - `<base>.bitmap.bin`            - SharedAtomicU64 active-priority bits
//! - `<base>.prio0.bin`             - SharedRing for priority 0 (lowest)
//! - `<base>.prio1.bin`             - SharedRing for priority 1
//! - ...
//! - `<base>.prio{K-1}.bin`         - SharedRing for priority K-1 (highest)
//!
//! # Protocol (the bitmap-as-hint pattern)
//!
//! - **Producer** `submit(prio, payload)`:
//!   1. `ring[prio].try_push(payload)?`
//!   2. `bitmap.fetch_or(1 << prio, AcqRel)` (idempotent; safe even
//!      if already set)
//!
//! - **Consumer** `try_drain_highest`:
//!   1. Load bitmap; if zero, return Empty.
//!   2. `highest = 63 - bitmap.leading_zeros()` (one CLZ).
//!   3. `ring[highest].try_pop(buf)`:
//!      - Ok: return (highest, payload).
//!      - Err(Empty): another consumer drained it first; clear bit and
//!        retry the scan.
//!
//! The bitmap is a HINT (set after push, cleared on observed-empty
//! pop), not a source of truth; the ring is authoritative.
//!
//! # Race analysis
//!
//! - Producer pushed but hasn't set bit: consumer transiently sees
//!   Empty for that priority; next consumer call sees the bit
//!   (producer sets it after push). Acceptable: weakly-consistent
//!   fairness.
//! - Consumer cleared bit; concurrent producer set it via fetch_or:
//!   producer's set overrides the clear; consumer's clear was for a
//!   real Empty observation at one point in time. No item is ever
//!   lost; bit may be transiently inconsistent with ring state.
//!
//! # Capacity
//!
//! Up to 64 priorities (one u64 bit per priority). For more, switch
//! to a `Vec<u64>` bitmap and walk the words.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::shared_atomic::{SharedAtomicError, SharedAtomicU64};
use crate::shared_ring::{RingError, SharedRing, PAYLOAD_BYTES};

/// Maximum number of priority levels (one bit per priority in the
/// u64 bitmap).
pub const MAX_PRIORITIES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutError {
    Ring(RingError),
    Atomic(SharedAtomicError),
    PriorityOutOfBounds,
    NPrioritiesOutOfBounds,
    Empty,
}

impl From<RingError> for FanoutError {
    fn from(e: RingError) -> Self { Self::Ring(e) }
}
impl From<SharedAtomicError> for FanoutError {
    fn from(e: SharedAtomicError) -> Self { Self::Atomic(e) }
}

fn bitmap_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.bitmap.bin"));
    p
}
fn prio_path(base: &Path, prio: usize) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.prio{prio}.bin"));
    p
}

pub struct PriorityFanout {
    rings: Vec<Arc<SharedRing>>,
    bitmap: Arc<SharedAtomicU64>,
    n_priorities: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl subetha_sidecar::AdaptiveInstance for PriorityFanout {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl PriorityFanout {
    /// Create a fanout with `n_priorities` levels (0..n; 0 = lowest,
    /// n-1 = highest), each ring sized to `ring_capacity` slots. All
    /// rings have the same capacity; configure based on the expected
    /// burst per priority.
    pub fn create(
        base_path: impl AsRef<Path>,
        n_priorities: usize,
        ring_capacity: usize,
    ) -> Result<Self, FanoutError> {
        if n_priorities == 0 || n_priorities > MAX_PRIORITIES {
            return Err(FanoutError::NPrioritiesOutOfBounds);
        }
        let base = base_path.as_ref();
        let mut rings = Vec::with_capacity(n_priorities);
        for i in 0..n_priorities {
            rings.push(Arc::new(SharedRing::create(prio_path(base, i), ring_capacity)?));
        }
        let bitmap = Arc::new(SharedAtomicU64::create(bitmap_path(base), 0)?);
        Ok(Self {
            rings, bitmap, n_priorities,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing fanout. Pass the SAME `n_priorities` and
    /// `ring_capacity` the creator used.
    pub fn open(
        base_path: impl AsRef<Path>,
        n_priorities: usize,
        ring_capacity: usize,
    ) -> Result<Self, FanoutError> {
        if n_priorities == 0 || n_priorities > MAX_PRIORITIES {
            return Err(FanoutError::NPrioritiesOutOfBounds);
        }
        let base = base_path.as_ref();
        let mut rings = Vec::with_capacity(n_priorities);
        for i in 0..n_priorities {
            rings.push(Arc::new(SharedRing::open(prio_path(base, i), ring_capacity)?));
        }
        let bitmap = Arc::new(SharedAtomicU64::open(bitmap_path(base)).map_err(|_| {
            // First touch may need create if a stale ring file exists; just
            // surface the atomic error.
            FanoutError::Atomic(SharedAtomicError::LayoutMismatch)
        })?);
        Ok(Self {
            rings, bitmap, n_priorities,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn n_priorities(&self) -> usize { self.n_priorities }

    /// Submit a payload at the given priority level. `priority` must
    /// be in `0..n_priorities`. Returns `Err(Ring(Full))` when that
    /// priority's ring is full.
    pub fn submit(&self, priority: usize, payload: &[u8]) -> Result<(), FanoutError> {
        if priority >= self.n_priorities {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::priority_fanout::OP_SUBMIT, 1);
            return Err(FanoutError::PriorityOutOfBounds);
        }
        let r = self.rings[priority].try_push(payload);
        if r.is_ok() {
            self.bitmap.fetch_or(1u64 << priority, Ordering::AcqRel);
        }
        self.ring_sidecar.push_op(
            crate::sidecar_ops::priority_fanout::OP_SUBMIT,
            if r.is_err() { 1 } else { 0 },
        );
        r?;
        Ok(())
    }

    /// Drain ONE item from the highest non-empty priority. Returns
    /// the priority of the item that was drained. Returns
    /// `Err(Empty)` only when ALL rings are empty.
    pub fn try_drain_highest(&self, out: &mut [u8]) -> Result<usize, FanoutError> {
        // Bounded retry: in the absolute worst case we scan and clear
        // every priority bit once. Don't loop forever; a malicious
        // producer churning the bitmap is otherwise able to starve
        // the caller.
        for _ in 0..(self.n_priorities * 2) {
            let bitmap = self.bitmap.load(Ordering::Acquire);
            if bitmap == 0 {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::priority_fanout::OP_DRAIN_HIGHEST, 2);
                return Err(FanoutError::Empty);
            }
            // highest = position of highest set bit (63 - leading_zeros for u64).
            let highest = 63 - bitmap.leading_zeros() as usize;
            if highest >= self.n_priorities {
                // Spurious bit above our range; clear it.
                self.bitmap.fetch_and(!(1u64 << highest), Ordering::AcqRel);
                continue;
            }
            match self.rings[highest].try_pop(out) {
                Ok(_) => {
                    // Eager hint clear: when we just drained the last
                    // observable item, clear the bit so observers see
                    // a fresh bitmap without waiting for the next
                    // empty-pop attempt.
                    if self.rings[highest].approx_len() == 0 {
                        self.bitmap.fetch_and(!(1u64 << highest), Ordering::AcqRel);
                    }
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::priority_fanout::OP_DRAIN_HIGHEST, 0);
                    return Ok(highest);
                }
                Err(RingError::Empty) => {
                    // Bit was stale; clear and retry next-highest.
                    self.bitmap.fetch_and(!(1u64 << highest), Ordering::AcqRel);
                    continue;
                }
                Err(e) => {
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::priority_fanout::OP_DRAIN_HIGHEST, 1);
                    return Err(FanoutError::Ring(e));
                }
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::priority_fanout::OP_DRAIN_HIGHEST, 2);
        Err(FanoutError::Empty)
    }

    /// Drain ONE item from a specific priority. Returns
    /// `Err(Ring(Empty))` when that ring is empty. Useful for
    /// dedicated workers that only handle a specific class.
    pub fn try_drain_priority(
        &self, priority: usize, out: &mut [u8]
    ) -> Result<(), FanoutError> {
        if priority >= self.n_priorities {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::priority_fanout::OP_DRAIN_PRIORITY, 1);
            return Err(FanoutError::PriorityOutOfBounds);
        }
        let r = self.rings[priority].try_pop(out);
        // Best-effort hint update: if the ring is now empty, clear the bit.
        // This is purely an optimization; bitmap-as-hint correctness doesn't
        // require it.
        if self.rings[priority].approx_len() == 0 {
            self.bitmap.fetch_and(!(1u64 << priority), Ordering::AcqRel);
        }
        self.ring_sidecar.push_op(
            crate::sidecar_ops::priority_fanout::OP_DRAIN_PRIORITY,
            if matches!(&r, Err(RingError::Empty)) { 2 } else if r.is_err() { 1 } else { 0 },
        );
        r?;
        Ok(())
    }

    /// Snapshot the current active-priority bitmap.
    #[inline]
    pub fn active_priorities(&self) -> u64 {
        self.bitmap.load(Ordering::Acquire)
    }

    /// Highest currently-active priority (None when all empty).
    pub fn highest_active_priority(&self) -> Option<usize> {
        let b = self.active_priorities();
        if b == 0 { return None; }
        let h = 63 - b.leading_zeros() as usize;
        if h < self.n_priorities { Some(h) } else { None }
    }

    /// Approximate pending count for a specific priority (each ring's
    /// own approx_len).
    pub fn approx_pending(&self, priority: usize) -> Option<usize> {
        if priority >= self.n_priorities { return None; }
        Some(self.rings[priority].approx_len())
    }

    /// Approximate total pending across all priorities.
    pub fn approx_total_pending(&self) -> usize {
        self.rings.iter().map(|r| r.approx_len()).sum()
    }

    pub const PAYLOAD_BYTES: usize = PAYLOAD_BYTES;

    /// Sync the bitmap and all rings to disk.
    pub fn flush(&self) -> Result<(), FanoutError> {
        self.bitmap.flush()?;
        for r in &self.rings { r.flush()?; }
        Ok(())
    }

    /// Non-blocking flush of the bitmap and all rings. Delegates to
    /// each inner primitive's flush_async.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), FanoutError> {
        self.bitmap.flush_async()?;
        for r in &self.rings { r.flush_async()?; }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as O};
    use std::sync::Barrier;
    use std::thread;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-fanout-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path, n: usize) {
        std::fs::remove_file(bitmap_path(base)).ok();
        for i in 0..n {
            std::fs::remove_file(prio_path(base, i)).ok();
        }
    }

    fn payload_of(v: u32) -> [u8; PAYLOAD_BYTES] {
        let mut b = [0u8; PAYLOAD_BYTES];
        b[0..4].copy_from_slice(&v.to_le_bytes());
        b
    }
    fn unpack(b: &[u8]) -> u32 {
        u32::from_le_bytes(b[0..4].try_into().unwrap())
    }

    #[test]
    fn create_initial_bitmap_is_zero() {
        let base = tmp_base("init");
        let f = PriorityFanout::create(&base, 4, 8).unwrap();
        assert_eq!(f.active_priorities(), 0);
        assert_eq!(f.highest_active_priority(), None);
        cleanup(&base, 4);
    }

    #[test]
    fn submit_sets_priority_bit() {
        let base = tmp_base("sub");
        let f = PriorityFanout::create(&base, 4, 8).unwrap();
        f.submit(2, &payload_of(42)).unwrap();
        assert_eq!(f.active_priorities() & 0b0100, 0b0100);
        assert_eq!(f.highest_active_priority(), Some(2));
        cleanup(&base, 4);
    }

    #[test]
    fn drain_highest_returns_highest_first() {
        let base = tmp_base("hi-first");
        let f = PriorityFanout::create(&base, 4, 8).unwrap();
        f.submit(0, &payload_of(10)).unwrap();
        f.submit(2, &payload_of(30)).unwrap();
        f.submit(1, &payload_of(20)).unwrap();
        f.submit(3, &payload_of(40)).unwrap();
        let mut buf = [0u8; PAYLOAD_BYTES];
        let p3 = f.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p3, 3);
        assert_eq!(unpack(&buf), 40);
        let p2 = f.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p2, 2);
        assert_eq!(unpack(&buf), 30);
        let p1 = f.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p1, 1);
        let p0 = f.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p0, 0);
        assert_eq!(f.try_drain_highest(&mut buf).err(), Some(FanoutError::Empty));
        cleanup(&base, 4);
    }

    #[test]
    fn drain_within_priority_preserves_fifo() {
        let base = tmp_base("fifo");
        let f = PriorityFanout::create(&base, 2, 16).unwrap();
        for i in 0..5u32 { f.submit(1, &payload_of(i)).unwrap(); }
        for i in 0..5u32 {
            let mut buf = [0u8; PAYLOAD_BYTES];
            let p = f.try_drain_highest(&mut buf).unwrap();
            assert_eq!(p, 1);
            assert_eq!(unpack(&buf), i);
        }
        cleanup(&base, 2);
    }

    #[test]
    fn priority_out_of_bounds_rejected() {
        let base = tmp_base("oob");
        let f = PriorityFanout::create(&base, 4, 8).unwrap();
        assert_eq!(f.submit(4, &payload_of(0)).err(),
            Some(FanoutError::PriorityOutOfBounds));
        assert_eq!(f.submit(100, &payload_of(0)).err(),
            Some(FanoutError::PriorityOutOfBounds));
        cleanup(&base, 4);
    }

    #[test]
    fn try_drain_priority_targets_specific_ring() {
        let base = tmp_base("specific");
        let f = PriorityFanout::create(&base, 3, 8).unwrap();
        f.submit(0, &payload_of(100)).unwrap();
        f.submit(2, &payload_of(300)).unwrap();
        let mut buf = [0u8; PAYLOAD_BYTES];
        f.try_drain_priority(0, &mut buf).unwrap();
        assert_eq!(unpack(&buf), 100);
        // Priority 1 is empty.
        assert_eq!(f.try_drain_priority(1, &mut buf).err(),
            Some(FanoutError::Ring(RingError::Empty)));
        // Priority 2 still has the item.
        f.try_drain_priority(2, &mut buf).unwrap();
        assert_eq!(unpack(&buf), 300);
        cleanup(&base, 3);
    }

    #[test]
    fn full_ring_returns_error() {
        let base = tmp_base("full");
        let f = PriorityFanout::create(&base, 2, 4).unwrap();
        for i in 0..4u32 { f.submit(0, &payload_of(i)).unwrap(); }
        match f.submit(0, &payload_of(99)) {
            Err(FanoutError::Ring(RingError::Full)) => {}
            other => panic!("expected Ring(Full), got {other:?}"),
        }
        cleanup(&base, 2);
    }

    #[test]
    fn cross_handle_priority_visible() {
        let base = tmp_base("cross-handle");
        let producer = PriorityFanout::create(&base, 4, 8).unwrap();
        let consumer = PriorityFanout::open(&base, 4, 8).unwrap();
        producer.submit(3, &payload_of(777)).unwrap();
        assert_eq!(consumer.highest_active_priority(), Some(3));
        let mut buf = [0u8; PAYLOAD_BYTES];
        let p = consumer.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p, 3);
        assert_eq!(unpack(&buf), 777);
        // Producer sees consumer's drain.
        assert_eq!(producer.highest_active_priority(), None);
        cleanup(&base, 4);
    }

    #[test]
    fn highest_priority_drained_first_under_interleaved_submits() {
        let base = tmp_base("interleave");
        let f = PriorityFanout::create(&base, 4, 16).unwrap();
        // Interleave: drain after every submit; highest always wins.
        f.submit(0, &payload_of(1)).unwrap();
        f.submit(1, &payload_of(2)).unwrap();
        let mut buf = [0u8; PAYLOAD_BYTES];
        assert_eq!(f.try_drain_highest(&mut buf).unwrap(), 1);
        f.submit(3, &payload_of(3)).unwrap();
        assert_eq!(f.try_drain_highest(&mut buf).unwrap(), 3);
        f.submit(2, &payload_of(4)).unwrap();
        assert_eq!(f.try_drain_highest(&mut buf).unwrap(), 2);
        assert_eq!(f.try_drain_highest(&mut buf).unwrap(), 0);
        cleanup(&base, 4);
    }

    #[test]
    fn concurrent_producers_route_to_correct_priorities() {
        let base = tmp_base("concurrent-prod");
        let f = Arc::new(PriorityFanout::create(&base, 4, 256).unwrap());
        let n_threads = 4;
        let per_thread = 32;
        let barrier = Arc::new(Barrier::new(n_threads));
        let mut handles = vec![];
        for t in 0..n_threads {
            let f = f.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                // Each thread submits at priority = its index.
                for i in 0..per_thread {
                    while f.submit(t, &payload_of(i as u32)).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        // Drain everything; count per priority.
        let mut counts = [0u32; 4];
        let mut buf = [0u8; PAYLOAD_BYTES];
        while let Ok(p) = f.try_drain_highest(&mut buf) {
            counts[p] += 1;
        }
        for c in counts.iter() {
            assert_eq!(*c, per_thread as u32);
        }
        cleanup(&base, 4);
    }

    #[test]
    fn observer_sees_bitmap_update_during_workload() {
        let base = tmp_base("observer-bitmap");
        let f = Arc::new(PriorityFanout::create(&base, 8, 64).unwrap());
        // Submit synchronously BEFORE spawning the observer loop so
        // we test the visibility property, not a scheduler race
        // between two unsynchronised threads.
        f.submit(5, &payload_of(1)).unwrap();
        let f2 = f.clone();
        let stop = Arc::new(AtomicU32::new(0));
        let stop2 = stop.clone();
        let producer = thread::spawn(move || {
            for _ in 0..100 {
                if stop2.load(O::Acquire) == 1 { break; }
                // Producer keeps pumping; full-fanout errors are
                // valid (consumer side may be slow).
                f2.submit(5, &payload_of(1)).ok();
                std::thread::yield_now();
            }
        });
        // Observer should see bit 5 set immediately (it was set by
        // the pre-spawn submit). The loop allows for transient
        // unfairness on heavily-loaded CI runners.
        let mut saw = false;
        for _ in 0..10_000 {
            if (f.active_priorities() & (1 << 5)) != 0 {
                saw = true;
                break;
            }
            std::thread::yield_now();
        }
        stop.store(1, O::Release);
        producer.join().unwrap();
        assert!(saw, "observer never saw bit 5 set");
        cleanup(&base, 8);
    }

    #[test]
    fn n_priorities_out_of_bounds_rejected_at_create() {
        let base = tmp_base("oob-create");
        assert_eq!(
            PriorityFanout::create(&base, 0, 8).err(),
            Some(FanoutError::NPrioritiesOutOfBounds),
        );
        assert_eq!(
            PriorityFanout::create(&base, MAX_PRIORITIES + 1, 8).err(),
            Some(FanoutError::NPrioritiesOutOfBounds),
        );
        // 64 (= MAX_PRIORITIES) is allowed.
        let f = PriorityFanout::create(&base, MAX_PRIORITIES, 4).unwrap();
        assert_eq!(f.n_priorities(), MAX_PRIORITIES);
        cleanup(&base, MAX_PRIORITIES);
    }

    #[test]
    fn disk_persistence_bitmap_and_rings_survive_reopen() {
        let base = tmp_base("disk");
        {
            let f = PriorityFanout::create(&base, 4, 8).unwrap();
            f.submit(2, &payload_of(2222)).unwrap();
            f.submit(0, &payload_of(0000)).unwrap();
            f.flush().unwrap();
        }
        let f2 = PriorityFanout::open(&base, 4, 8).unwrap();
        assert_eq!(f2.highest_active_priority(), Some(2));
        let mut buf = [0u8; PAYLOAD_BYTES];
        let p = f2.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p, 2);
        assert_eq!(unpack(&buf), 2222);
        let p = f2.try_drain_highest(&mut buf).unwrap();
        assert_eq!(p, 0);
        assert_eq!(unpack(&buf), 0);
        cleanup(&base, 4);
    }
}
