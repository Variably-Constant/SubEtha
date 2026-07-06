//! `EpochBarrier` - multi-process phase synchronization with
//! heartbeat-driven dead-peer exclusion.
//!
//! Composes one [`SharedAtomicU64`] (the
//! packed state) with an external
//! [`HeartbeatTable`] (the live-peer source).
//! Releases when all LIVE peers (per the heartbeat) have called
//! `wait` at the current epoch.
//!
//! # Why this exists
//!
//! Standard barriers (`std::sync::Barrier`, MPI_Barrier) require a
//! known fixed participant count. In a distributed setting that's
//! brittle: one crashed process and the whole barrier deadlocks.
//! EpochBarrier reads the live peer count from the heartbeat table
//! each scan, so a dead peer (whose heartbeat has lapsed beyond the
//! grace window) is automatically excluded. The barrier releases as
//! soon as the surviving peers reach it.
//!
//! # State encoding
//!
//! ONE SharedAtomicU64 holds both the current epoch and the arrived
//! count, packed as `(epoch << 32) | arrived`. This makes the entire
//! protocol single-atomic: register-as-arrived and release-the-
//! barrier are both single CAS operations, so there's no ordering
//! puzzle between two separate atomics.
//!
//! # Protocol
//!
//! `wait(my_epoch)`:
//! 1. Load packed state. If `cur_epoch > my_epoch`, the epoch has
//!    already passed; return.
//! 2. If `cur_epoch < my_epoch`, we're early; yield and retry.
//! 3. If `cur_epoch == my_epoch`, CAS to (cur_epoch, arrived + 1) to
//!    register ourselves. On CAS failure, retry.
//! 4. Wait loop: load state; if `cur_epoch > my_epoch`, return
//!    (released by some peer). Otherwise check whether
//!    `arrived >= live_peer_count`; if so, try CAS to
//!    (cur_epoch + 1, 0) to release. Backoff between checks.
//!
//! # Quorum variant
//!
//! `wait_quorum(my_epoch, quorum)` is identical except the release
//! threshold is `arrived >= quorum` rather than `>= live_peer_count`.
//! Useful when the exact participant set is uncertain (e.g.,
//! 2-phase-commit prepare needing majority commitment).
//!
//! # Capacity
//!
//! The packed encoding gives 32 bits to the epoch counter (4B
//! barriers per primitive lifetime) and 32 bits to the arrived
//! count (4B participants per epoch). Real-world deployments are
//! bounded by both far below 32 bits.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::heartbeat::{HeartbeatTable, EMPTY_PID};
use crate::shared_atomic::{SharedAtomicError, SharedAtomicU64};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierError {
    Atomic(SharedAtomicError),
    EpochTooFarBehind,
    Timeout,
    NoLivePeers,
}

impl From<SharedAtomicError> for BarrierError {
    fn from(e: SharedAtomicError) -> Self { Self::Atomic(e) }
}

fn state_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.state.bin"));
    p
}

#[inline]
fn pack(epoch: u32, arrived: u32) -> u64 {
    ((epoch as u64) << 32) | (arrived as u64)
}
#[inline]
fn unpack(state: u64) -> (u32, u32) {
    ((state >> 32) as u32, state as u32)
}

/// Default grace window for treating a heartbeat slot as live. A
/// slot is live when `global_epoch - slot.last_seen_epoch <= grace`.
pub const DEFAULT_BARRIER_GRACE_EPOCHS: u64 = 3;

pub struct EpochBarrier {
    state: Arc<SharedAtomicU64>,
    heartbeat: Arc<HeartbeatTable>,
    grace_epochs: u64,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl subetha_sidecar::AdaptiveInstance for EpochBarrier {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl EpochBarrier {
    /// Create a new EpochBarrier at `base_path`. Borrows an existing
    /// HeartbeatTable for live-peer counting; `grace_epochs` controls
    /// how stale a slot can be before it counts as dead.
    pub fn create(
        base_path: impl AsRef<Path>,
        heartbeat: Arc<HeartbeatTable>,
        grace_epochs: u64,
    ) -> Result<Self, BarrierError> {
        let base = base_path.as_ref();
        let state = Arc::new(SharedAtomicU64::create(state_path(base), 0)?);
        Ok(Self {
            state, heartbeat, grace_epochs,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing EpochBarrier.
    pub fn open(
        base_path: impl AsRef<Path>,
        heartbeat: Arc<HeartbeatTable>,
        grace_epochs: u64,
    ) -> Result<Self, BarrierError> {
        let base = base_path.as_ref();
        let state = Arc::new(SharedAtomicU64::open(state_path(base))?);
        Ok(Self {
            state, heartbeat, grace_epochs,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Count live peers from the heartbeat table (slots with
    /// `last_seen_epoch >= global_epoch - grace`).
    pub fn live_peer_count(&self) -> u32 {
        let global = self.heartbeat.global_epoch();
        let cap = self.heartbeat.capacity();
        let mut count = 0u32;
        for i in 0..cap {
            if let Some(snap) = self.heartbeat.snapshot(i) {
                if snap.pid == EMPTY_PID { continue; }
                if global.saturating_sub(snap.last_seen_epoch) <= self.grace_epochs {
                    count += 1;
                }
            }
        }
        count
    }

    /// Wait for ALL live peers to reach `my_epoch`. Blocks; uses an
    /// adaptive spin / yield / sleep backoff between releaser checks.
    pub fn wait(&self, my_epoch: u32) -> Result<(), BarrierError> {
        let r = self.wait_inner(my_epoch, None, None);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::liveness::OP_WAIT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    /// Wait with a quorum threshold instead of all-live. Releases
    /// when `arrived >= quorum`.
    pub fn wait_quorum(&self, my_epoch: u32, quorum: u32) -> Result<(), BarrierError> {
        let r = self.wait_inner(my_epoch, Some(quorum), None);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::liveness::OP_WAIT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    /// Wait with a deadline. Returns `Err(Timeout)` if the deadline
    /// passes before release.
    pub fn wait_timeout(
        &self, my_epoch: u32, timeout: Duration,
    ) -> Result<(), BarrierError> {
        self.wait_inner(my_epoch, None, Some(Instant::now() + timeout))
    }

    /// Wait with a deadline AND a quorum threshold.
    pub fn wait_quorum_timeout(
        &self, my_epoch: u32, quorum: u32, timeout: Duration,
    ) -> Result<(), BarrierError> {
        self.wait_inner(my_epoch, Some(quorum), Some(Instant::now() + timeout))
    }

    fn wait_inner(
        &self,
        my_epoch: u32,
        quorum: Option<u32>,
        deadline: Option<Instant>,
    ) -> Result<(), BarrierError> {
        // Registration loop: bump arrived at my_epoch, or fast-exit
        // when the epoch has already passed.
        loop {
            if let Some(d) = deadline
                && Instant::now() >= d { return Err(BarrierError::Timeout); }
            let state = self.state.load(Ordering::Acquire);
            let (cur_epoch, cur_arrived) = unpack(state);
            if cur_epoch > my_epoch {
                return Ok(());
            }
            if cur_epoch < my_epoch {
                thread::yield_now();
                continue;
            }
            let new = pack(cur_epoch, cur_arrived.saturating_add(1));
            if self.state.compare_exchange(
                state, new, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                break;
            }
        }

        // Release-wait loop: act as the releaser when the threshold
        // is reached, otherwise back off and retry.
        let mut spins = 0u32;
        loop {
            if let Some(d) = deadline
                && Instant::now() >= d { return Err(BarrierError::Timeout); }
            let state = self.state.load(Ordering::Acquire);
            let (cur_epoch, cur_arrived) = unpack(state);
            if cur_epoch > my_epoch {
                return Ok(());
            }
            let threshold = match quorum {
                Some(q) => q,
                None => {
                    let live = self.live_peer_count();
                    if live == 0 { return Err(BarrierError::NoLivePeers); }
                    live
                }
            };
            if cur_arrived >= threshold {
                let new = pack(cur_epoch.saturating_add(1), 0);
                if self.state.compare_exchange(
                    state, new, Ordering::AcqRel, Ordering::Acquire,
                ).is_ok() {
                    return Ok(());
                }
                continue;
            }
            spins += 1;
            if spins < 32 {
                std::hint::spin_loop();
            } else if spins < 256 {
                thread::yield_now();
            } else {
                thread::sleep(Duration::from_micros(50));
            }
        }
    }

    /// Current epoch (next one to be waited on).
    pub fn current_epoch(&self) -> u32 {
        unpack(self.state.load(Ordering::Acquire)).0
    }

    /// Currently arrived count at the current epoch.
    pub fn arrived_count(&self) -> u32 {
        unpack(self.state.load(Ordering::Acquire)).1
    }

    /// Snapshot (epoch, arrived).
    pub fn snapshot(&self) -> (u32, u32) {
        unpack(self.state.load(Ordering::Acquire))
    }

    pub fn flush(&self) -> Result<(), BarrierError> {
        self.state.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), BarrierError> {
        self.state.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::HeartbeatTable;
    use std::sync::Barrier as StdBarrier;
    use std::sync::atomic::{AtomicU32, Ordering as O};

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-barrier-{name}-{pid}"));
        p
    }

    fn tmp_hb(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-barrier-hb-{name}-{pid}.bin"));
        p
    }

    fn cleanup(base: &Path, hb: &Path) {
        std::fs::remove_file(state_path(base)).ok();
        std::fs::remove_file(hb).ok();
    }

    fn make(name: &str, n_slots: usize, grace: u64) -> (PathBuf, PathBuf, Arc<HeartbeatTable>, EpochBarrier) {
        let base = tmp_base(name);
        let hb_path = tmp_hb(name);
        let hb = Arc::new(HeartbeatTable::create(&hb_path, n_slots).unwrap());
        let barrier = EpochBarrier::create(&base, hb.clone(), grace).unwrap();
        (base, hb_path, hb, barrier)
    }

    #[test]
    fn create_initial_state_is_zero() {
        let (base, hb_path, _hb, barrier) = make("init", 4, 3);
        assert_eq!(barrier.current_epoch(), 0);
        assert_eq!(barrier.arrived_count(), 0);
        cleanup(&base, &hb_path);
    }

    #[test]
    fn single_peer_releases_immediately() {
        let (base, hb_path, hb, barrier) = make("solo", 4, 3);
        let s = hb.register(1001).unwrap();
        hb.beat(s);
        assert_eq!(barrier.live_peer_count(), 1);
        barrier.wait(0).unwrap();
        assert_eq!(barrier.current_epoch(), 1);
        cleanup(&base, &hb_path);
    }

    #[test]
    fn three_peers_all_arrive_releases() {
        let (base, hb_path, hb, barrier) = make("three", 4, 10);
        let slots: Vec<usize> = (0..3).map(|i| {
            let s = hb.register(1000 + i as u32).unwrap();
            hb.beat(s);
            s
        }).collect();
        let _slots = slots;
        let barrier = Arc::new(barrier);
        let arrived = Arc::new(AtomicU32::new(0));
        let sync = Arc::new(StdBarrier::new(3));
        let mut handles = vec![];
        for _ in 0..3 {
            let b = barrier.clone();
            let a = arrived.clone();
            let s = sync.clone();
            handles.push(thread::spawn(move || {
                s.wait();
                b.wait(0).unwrap();
                a.fetch_add(1, O::AcqRel);
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(arrived.load(O::Acquire), 3);
        assert_eq!(barrier.current_epoch(), 1);
        cleanup(&base, &hb_path);
    }

    #[test]
    fn early_arriver_waits_for_late_arriver() {
        let (base, hb_path, hb, barrier) = make("late", 4, 10);
        for i in 0..2 {
            let s = hb.register(2000 + i).unwrap();
            hb.beat(s);
        }
        let barrier = Arc::new(barrier);

        let b1 = barrier.clone();
        let early = thread::spawn(move || {
            let start = Instant::now();
            b1.wait(0).unwrap();
            start.elapsed()
        });
        thread::sleep(Duration::from_millis(20));
        let b2 = barrier.clone();
        let late = thread::spawn(move || {
            b2.wait(0).unwrap();
        });
        let elapsed = early.join().unwrap();
        late.join().unwrap();
        assert!(elapsed >= Duration::from_millis(15),
            "early arriver should have waited ~20ms, got {elapsed:?}");
        cleanup(&base, &hb_path);
    }

    #[test]
    fn quorum_releases_at_threshold_below_total() {
        let (base, hb_path, hb, barrier) = make("quorum", 8, 10);
        for i in 0..5 {
            let s = hb.register(3000 + i).unwrap();
            hb.beat(s);
        }
        let barrier = Arc::new(barrier);

        let sync = Arc::new(StdBarrier::new(3));
        let mut handles = vec![];
        for _ in 0..3 {
            let b = barrier.clone();
            let s = sync.clone();
            handles.push(thread::spawn(move || {
                s.wait();
                b.wait_quorum(0, 3).unwrap();
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(barrier.current_epoch(), 1);
        cleanup(&base, &hb_path);
    }

    #[test]
    fn wait_timeout_returns_timeout_when_not_enough_arrive() {
        let (base, hb_path, hb, barrier) = make("timeout", 4, 10);
        for i in 0..2 {
            let s = hb.register(4000 + i).unwrap();
            hb.beat(s);
        }
        let start = Instant::now();
        let r = barrier.wait_timeout(0, Duration::from_millis(30));
        let elapsed = start.elapsed();
        assert_eq!(r.err(), Some(BarrierError::Timeout));
        assert!(elapsed >= Duration::from_millis(25));
        cleanup(&base, &hb_path);
    }

    #[test]
    fn epoch_passed_returns_immediately() {
        let (base, hb_path, hb, barrier) = make("passed", 4, 10);
        let _val = hb.register(5000).unwrap();
        hb.beat(0);
        barrier.wait(0).unwrap();
        assert_eq!(barrier.current_epoch(), 1);
        let start = Instant::now();
        barrier.wait(0).unwrap();
        assert!(start.elapsed() < Duration::from_millis(5));
        cleanup(&base, &hb_path);
    }

    #[test]
    fn multiple_epochs_in_sequence() {
        let (base, hb_path, hb, barrier) = make("seq", 4, 10);
        for i in 0..2 {
            let s = hb.register(6000 + i).unwrap();
            hb.beat(s);
        }
        let barrier = Arc::new(barrier);
        let mut handles = vec![];
        for _ in 0..2 {
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                for e in 0..5u32 { b.wait(e).unwrap(); }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(barrier.current_epoch(), 5);
        cleanup(&base, &hb_path);
    }

    #[test]
    fn dead_peer_does_not_block_barrier() {
        let (base, hb_path, hb, barrier) = make("dead-peer", 4, 1);
        let live_slot = hb.register(7000).unwrap();
        let _dead_slot = hb.register(7001).unwrap();
        hb.beat(live_slot);
        for _ in 0..5 { hb.tick_global_epoch(); }
        hb.beat(live_slot);
        assert_eq!(barrier.live_peer_count(), 1,
            "dead peer should be excluded by grace_epochs");
        let start = Instant::now();
        barrier.wait(0).unwrap();
        assert!(start.elapsed() < Duration::from_millis(50));
        cleanup(&base, &hb_path);
    }

    #[test]
    fn cross_handle_barrier_state_visible() {
        let (base, hb_path, hb, barrier_a) = make("cross", 4, 10);
        let _val = hb.register(8000).unwrap();
        hb.beat(0);
        let barrier_b = EpochBarrier::open(&base, hb.clone(), 10).unwrap();
        barrier_a.wait(0).unwrap();
        assert_eq!(barrier_b.current_epoch(), 1);
        cleanup(&base, &hb_path);
    }

    #[test]
    fn no_live_peers_returns_error() {
        let (base, hb_path, _hb, barrier) = make("no-peers", 4, 1);
        let r = barrier.wait(0);
        assert_eq!(r.err(), Some(BarrierError::NoLivePeers));
        cleanup(&base, &hb_path);
    }

    #[test]
    fn arrived_resets_at_each_epoch() {
        let (base, hb_path, hb, barrier) = make("reset", 4, 10);
        for i in 0..2 {
            let s = hb.register(9000 + i).unwrap();
            hb.beat(s);
        }
        let barrier = Arc::new(barrier);
        let sync = Arc::new(StdBarrier::new(2));
        let mut handles = vec![];
        for _ in 0..2 {
            let b = barrier.clone();
            let s = sync.clone();
            handles.push(thread::spawn(move || {
                s.wait();
                b.wait(0).unwrap();
                b.wait(1).unwrap();
                b.wait(2).unwrap();
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(barrier.current_epoch(), 3);
        assert_eq!(barrier.arrived_count(), 0,
            "arrived must reset to 0 after final release");
        cleanup(&base, &hb_path);
    }
}
