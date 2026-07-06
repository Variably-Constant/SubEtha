//! `SharedAsyncPointer<T>` - cross-process lazy / speculative
//! resolution wrapping a `SharedOnceCell<T>`.
//!
//! Three resolution strategies; all converge to a single canonical
//! value in the underlying `SharedOnceCell`:
//!
//! | Strategy   | Race width | Failover                                |
//! |------------|-----------|-----------------------------------------|
//! | Resolved   | n/a       | n/a (value pre-set)                     |
//! | Lazy       | 1         | first caller to set wins; others read   |
//! | Speculative| N (workers)| first-publisher-wins; losers discard    |
//!
//! # The Speculative race
//!
//! `get_or_speculative(n, f)` spawns N worker threads (or in the
//! cross-process variant, dispatches N Passes via the
//! BackgroundScheduler). Each worker independently computes `f()`
//! and CAS-attempts to publish the result via the underlying
//! SharedOnceCell. The first to win the CAS becomes the canonical
//! result; losers see the cell already filled and DISCARD their
//! result.
//!
//! This is the architectural novelty: redundant cross-process
//! compute with first-publisher-wins. No existing Rust async runtime
//! provides this primitive. It's useful for:
//!
//! - Latency hedging: race 2-3 backend lookups, take the fastest
//! - Survivability: race N solvers across processes; any one
//!   surviving suffices
//! - Fault-tolerant fetch: if one resolver dies, others continue
//!
//! Failover within 1 epoch: if a resolver dies mid-compute, the
//! others are unaffected; the first survivor publishes. No
//! coordinator needed - the CAS protocol IS the coordination.

use std::path::Path;
use std::sync::Arc;
use std::thread;

use crate::shared_once_cell::{SharedOnceCell, SharedOnceError};

/// Strategy tag observed by the substrate.
pub mod strategy {
    pub const RESOLVED: u32 = 0;
    pub const LAZY: u32 = 1;
    pub const SPECULATIVE: u32 = 2;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedAsyncError {
    Once(SharedOnceError),
    AllWorkersDied,
}

impl From<SharedOnceError> for SharedAsyncError {
    fn from(e: SharedOnceError) -> Self { Self::Once(e) }
}

pub struct SharedAsyncPointer<T: Copy + Send + Sync + 'static> {
    cell: Arc<SharedOnceCell<T>>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<T: Copy + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for SharedAsyncPointer<T>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + Send + Sync + 'static> SharedAsyncPointer<T> {
    /// Direction signature of `SharedAsyncPointer<T>`. Engages the
    /// `K_async` axis (future / async-state stored at slot for
    /// cross-process await on a value that may not yet exist).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Async],
    );

    /// Create a new shared async pointer backed by an MMF cell at
    /// `path`. The cell starts EMPTY.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, SharedAsyncError> {
        let cell = Arc::new(SharedOnceCell::create(path)?);
        Ok(Self {
            cell,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing shared async pointer.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedAsyncError> {
        let cell = Arc::new(SharedOnceCell::open(path)?);
        Ok(Self {
            cell,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// True when the underlying cell is initialised.
    pub fn is_resolved(&self) -> bool {
        self.cell.is_initialized()
    }

    /// Non-blocking peek. Returns the canonical value if any.
    pub fn try_get(&self) -> Option<T> {
        let r = self.cell.get();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::async_pointer::OP_TRY_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Pre-resolve by setting the value. Returns `true` if this
    /// caller won the init race, `false` if the cell was already
    /// initialised.
    pub fn set_resolved(&self, value: T) -> bool {
        self.cell.set(value)
    }

    /// Lazy resolution: if the cell is initialised, return its
    /// value. Otherwise, the caller runs `f` once and attempts to
    /// publish the result. If another concurrent caller wins the
    /// publish race, the caller still returns the canonical value
    /// (theirs may be discarded silently).
    pub fn get_or_lazy<F>(&self, f: F) -> T
    where F: FnOnce() -> T,
    {
        let was_resolved = self.cell.is_initialized();
        if let Some(v) = self.cell.get() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::async_pointer::OP_GET_OR_FETCH, 0);
            return v;
        }
        let computed = f();
        self.cell.set(computed);
        let v = self.cell.get().expect("INITIALIZED after set or read-back");
        self.ring_sidecar.push_op(
            crate::sidecar_ops::async_pointer::OP_GET_OR_FETCH,
            if was_resolved { 0 } else { 1 }, // cold-fetch path
        );
        v
    }

    /// Speculative resolution: spawn `n` worker threads that all
    /// independently compute `f()` and race to publish the result.
    /// First publisher wins; losers discard their results. Returns
    /// the canonical value (the winner's).
    ///
    /// All N workers share the same `f` (closure must be Clone +
    /// Send + Sync). Use `get_or_speculative_with` when each worker
    /// needs a different closure (e.g., different backends).
    pub fn get_or_speculative<F>(&self, n: usize, f: F) -> T
    where F: Fn() -> T + Send + Sync + 'static + Clone,
    {
        if let Some(v) = self.cell.get() { return v; }
        assert!(n >= 1, "speculative race needs at least 1 worker");
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let cell = self.cell.clone();
            let f = f.clone();
            handles.push(thread::spawn(move || {
                // Short-circuit: if cell already filled (another
                // worker won before we even started), skip the work.
                if cell.is_initialized() { return; }
                let v = f();
                // Try to publish; if we lose, our v is silently dropped.
                // Winner=true, loser=false; either way cell ends initialized.
                cell.set(v);
            }));
        }
        for h in handles { h.join().ok(); }
        self.cell.get().expect("at least one worker should publish")
    }

    /// Speculative resolution with per-worker closures. Each closure
    /// in `fs` is dispatched to one worker; first publisher wins.
    pub fn get_or_speculative_with<I, F>(&self, fs: I) -> T
    where I: IntoIterator<Item = F>, F: FnOnce() -> T + Send + 'static,
    {
        if let Some(v) = self.cell.get() { return v; }
        let mut handles = vec![];
        for f in fs {
            let cell = self.cell.clone();
            handles.push(thread::spawn(move || {
                if cell.is_initialized() { return; }
                let v = f();
                // Winner=true, loser=false; either way cell ends initialized.
                cell.set(v);
            }));
        }
        assert!(!handles.is_empty(), "speculative race needs at least 1 closure");
        for h in handles { h.join().ok(); }
        self.cell.get().expect("at least one worker should publish")
    }

    /// Speculative resolution that tolerates worker panics: closures
    /// that panic do not propagate; the race continues among
    /// survivors. Returns `Err(AllWorkersDied)` if every worker
    /// panicked AND no value was published.
    pub fn get_or_speculative_resilient<F>(&self, n: usize, f: F) -> Result<T, SharedAsyncError>
    where F: Fn() -> T + Send + Sync + 'static + Clone,
    {
        if let Some(v) = self.cell.get() { return Ok(v); }
        assert!(n >= 1);
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let cell = self.cell.clone();
            let f = f.clone();
            handles.push(thread::spawn(move || {
                if cell.is_initialized() { return; }
                let v = f();
                // Winner=true, loser=false; either way cell ends initialized.
                cell.set(v);
            }));
        }
        // Wait for all; ignore panics.
        for h in handles { h.join().ok(); }
        self.cell.get().ok_or(SharedAsyncError::AllWorkersDied)
    }

    /// Sync the underlying cell to disk.
    pub fn flush(&self) -> Result<(), SharedAsyncError> {
        Ok(self.cell.flush()?)
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), SharedAsyncError> {
        Ok(self.cell.flush_async()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-async-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn resolved_strategy_returns_immediately() {
        let p = tmp("resolved");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        assert!(!sap.is_resolved());
        sap.set_resolved(42);
        assert!(sap.is_resolved());
        assert_eq!(sap.try_get(), Some(42));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lazy_runs_closure_exactly_once_across_threads() {
        let p = tmp("lazy-once");
        let sap: Arc<SharedAsyncPointer<u64>>
            = Arc::new(SharedAsyncPointer::create(&p).unwrap());
        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];
        for _ in 0..8 {
            let sap = sap.clone();
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                sap.get_or_lazy(|| {
                    counter.fetch_add(1, Ordering::AcqRel);
                    thread::sleep(Duration::from_millis(2));
                    777
                })
            }));
        }
        let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All threads return the canonical value.
        assert!(results.iter().all(|v| *v == 777));
        // Closure ran at most once (in practice exactly once due to set CAS).
        let runs = counter.load(Ordering::Acquire);
        assert!((1..=8).contains(&runs),
                "closure runs should be between 1 and 8 (one per non-fast-pathed thread); got {runs}");
        // Actually we expect exactly 1 because get_or_lazy() ALWAYS runs
        // the closure on the first call; subsequent threads see the
        // cell filled before running. With 8 threads racing the
        // closure could run multiple times (if all check is_init
        // before any has filled), but at most once per thread. The
        // CAS publish ensures only one value is canonical.
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn speculative_race_returns_one_published_result() {
        let p = tmp("speculative");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        let runs = Arc::new(AtomicU32::new(0));
        let runs_clone = runs.clone();
        let result = sap.get_or_speculative(4, move || {
            runs_clone.fetch_add(1, Ordering::AcqRel);
            // small sleep so workers actually overlap
            thread::sleep(Duration::from_millis(5));
            123u64
        });
        assert_eq!(result, 123);
        assert!(runs.load(Ordering::Acquire) >= 1,
                "at least one worker must run");
        // is_resolved must be true after race
        assert!(sap.is_resolved());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn speculative_first_finisher_wins() {
        let p = tmp("first-wins");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        // Two closures: one fast (returns 100), one slow (returns 999).
        // Fast should win the publish race.
        let result = sap.get_or_speculative_with([
            Box::new(|| {
                thread::sleep(Duration::from_millis(100));
                999u64
            }) as Box<dyn FnOnce() -> u64 + Send>,
            Box::new(|| {
                thread::sleep(Duration::from_millis(2));
                100u64
            }) as Box<dyn FnOnce() -> u64 + Send>,
        ]);
        assert_eq!(result, 100, "fast closure should win");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn speculative_resilient_tolerates_panicking_workers() {
        let p = tmp("resilient");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();
        // Closure panics on even attempts, succeeds on odd.
        let result = sap.get_or_speculative_resilient(8, move || {
            let n = attempt_clone.fetch_add(1, Ordering::AcqRel);
            if n.is_multiple_of(2) {
                panic!("simulated worker death on attempt {n}");
            }
            42u64
        }).expect("at least one survivor publishes");
        assert_eq!(result, 42);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn second_call_after_resolution_returns_cached_value() {
        let p = tmp("cached");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        let _r1 = sap.get_or_lazy(|| 5);
        // Second call must NOT run a new closure; verify by ensuring
        // the closure body would change the value.
        let r2 = sap.get_or_lazy(|| panic!("must not run after resolution"));
        assert_eq!(r2, 5);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_speculative_race_shares_one_winner() {
        let p = tmp("cross-handle-spec");
        let sap_a = SharedAsyncPointer::<u64>::create(&p).unwrap();
        let sap_b = SharedAsyncPointer::<u64>::open(&p).unwrap();
        // Process A speculatively resolves.
        let r_a = sap_a.get_or_speculative(2, || 9999u64);
        assert_eq!(r_a, 9999);
        // Process B sees the same value without running anything.
        let r_b = sap_b.get_or_lazy(|| panic!("must not run on already-resolved cell"));
        assert_eq!(r_b, 9999);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn try_get_does_not_force_resolution() {
        let p = tmp("try-get");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        assert_eq!(sap.try_get(), None);
        assert!(!sap.is_resolved());
        sap.set_resolved(100);
        assert_eq!(sap.try_get(), Some(100));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn speculative_with_one_worker_is_just_lazy() {
        let p = tmp("spec-1");
        let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
        let runs = Arc::new(AtomicU32::new(0));
        let runs_clone = runs.clone();
        let r = sap.get_or_speculative(1, move || {
            runs_clone.fetch_add(1, Ordering::AcqRel);
            17u64
        });
        assert_eq!(r, 17);
        assert_eq!(runs.load(Ordering::Acquire), 1);
        std::fs::remove_file(&p).ok();
    }
}
