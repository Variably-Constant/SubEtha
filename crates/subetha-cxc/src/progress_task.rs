//! `ProgressTask<R>` - distributed work with live cross-process
//! progress visibility.
//!
//! Composes [`SharedAtomicU64`] (the progress
//! counter), [`SharedAtomicU64`] (the total /
//! denominator), [`SharedAtomicBool`] (the
//! done flag), and [`SharedCell<R>`](crate::SharedCell) (the result
//! payload). The work closure receives a [`ProgressReporter`] handle
//! that increments the progress counter as it proceeds; any OTHER
//! process or thread can call `fraction_complete` / `current_progress`
//! / `is_done` / `read_result` at O(1) atomic cost without blocking
//! or polling a result queue.
//!
//! # Why this exists
//!
//! Long-running jobs (ETL pipelines, batch processing, big builds)
//! benefit from out-of-band progress visibility: a separate dashboard
//! / CLI / supervisor wants to know "47% done, ETA 3min" without
//! drilling into the worker's log file or waiting on a result ring.
//! Naive approaches use a separate progress channel, a counter file
//! that the worker overwrites, or a stat-on-tempfile heuristic. With
//! ProgressTask, the counter is one atomic load away in shared
//! memory; updates are sub-nanosecond.
//!
//! # Four files per task
//!
//! - `<base>.progress.bin` - SharedAtomicU64, monotonically advancing
//! - `<base>.total.bin`    - SharedAtomicU64, the denominator
//! - `<base>.done.bin`     - SharedAtomicBool, set true on completion
//! - `<base>.result.bin`   - `SharedCell<R>`, written once at completion
//!
//! Pass the BASE PATH (without extension) to `create` / `open`; the
//! wrapper appends the extensions.
//!
//! # Composition with the scheduler
//!
//! ProgressTask is INDEPENDENT of `BackgroundScheduler`; it works
//! standalone via `run` / `spawn`. To integrate with the scheduler,
//! a Pass closure can `ProgressTask::open(base)` to obtain a handle
//! and call `begin(total)` to get a reporter. The worker side only
//! needs the base path; the observer side only needs `open` + the
//! read APIs.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::shared_atomic::{SharedAtomicBool, SharedAtomicError, SharedAtomicU64};
use crate::shared_cell::{SharedCell, SharedCellError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressTaskError {
    Atomic(SharedAtomicError),
    Cell(SharedCellError),
}

impl From<SharedAtomicError> for ProgressTaskError {
    fn from(e: SharedAtomicError) -> Self { Self::Atomic(e) }
}
impl From<SharedCellError> for ProgressTaskError {
    fn from(e: SharedCellError) -> Self { Self::Cell(e) }
}

fn progress_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.progress.bin"));
    p
}
fn total_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.total.bin"));
    p
}
fn done_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.done.bin"));
    p
}
fn result_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.result.bin"));
    p
}

/// Cross-process reporter handle passed to the work closure. Each
/// call to `advance` is a single atomic fetch_add (Relaxed ordering;
/// progress is observational, not a synchronization point).
pub struct ProgressReporter {
    progress: Arc<SharedAtomicU64>,
}

impl ProgressReporter {
    /// Add `n` to the progress counter. Returns the previous value.
    /// Use `Relaxed` because progress is observational; the `done`
    /// flag carries the happens-before edge.
    #[inline]
    pub fn advance(&self, n: u64) -> u64 {
        self.progress.fetch_add(n, Ordering::Relaxed)
    }

    /// Replace the progress counter with `v`. Useful when the work
    /// reports absolute progress (e.g., bytes processed) rather than
    /// per-step deltas.
    #[inline]
    pub fn set(&self, v: u64) {
        self.progress.store(v, Ordering::Relaxed);
    }

    /// Read the current progress counter.
    #[inline]
    pub fn current(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }
}

pub struct ProgressTask<R: Copy + 'static> {
    progress: Arc<SharedAtomicU64>,
    total: Arc<SharedAtomicU64>,
    done: Arc<SharedAtomicBool>,
    result: Arc<SharedCell<R>>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<R: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for ProgressTask<R> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<R: Copy + 'static> ProgressTask<R> {
    /// Create a new ProgressTask at `base_path`. Allocates four MMF
    /// files; initialises progress=0, total=0, done=false, result=
    /// `initial_result`.
    pub fn create(
        base_path: impl AsRef<Path>,
        initial_result: R,
    ) -> Result<Self, ProgressTaskError> {
        let base = base_path.as_ref();
        let progress = Arc::new(SharedAtomicU64::create(progress_path(base), 0)?);
        let total = Arc::new(SharedAtomicU64::create(total_path(base), 0)?);
        let done = Arc::new(SharedAtomicBool::create(done_path(base), false)?);
        let result = Arc::new(SharedCell::<R>::create(result_path(base))?);
        result.set(initial_result);
        Ok(Self {
            progress, total, done, result,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing ProgressTask at `base_path`. All four MMF
    /// files must exist.
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self, ProgressTaskError> {
        let base = base_path.as_ref();
        let progress = Arc::new(SharedAtomicU64::open(progress_path(base))?);
        let total = Arc::new(SharedAtomicU64::open(total_path(base))?);
        let done = Arc::new(SharedAtomicBool::open(done_path(base))?);
        let result = Arc::new(SharedCell::<R>::open(result_path(base))?);
        Ok(Self {
            progress, total, done, result,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Begin a new task run with the given `total` denominator.
    /// Resets progress=0, done=false, and publishes `total`. Returns
    /// a ProgressReporter for the work closure to advance.
    pub fn begin(&self, total: u64) -> ProgressReporter {
        self.progress.store(0, Ordering::Relaxed);
        self.done.store(false, Ordering::Release);
        self.total.store(total, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::progress::OP_ADVANCE, 0);
        ProgressReporter { progress: self.progress.clone() }
    }

    /// Mark the task complete and publish the final result. Other
    /// processes observing `is_done` will see true after this call;
    /// `read_result` returns the published value.
    pub fn complete(&self, result: R) {
        self.result.set(result);
        // Release ordering: pair with Acquire load of `done` to give
        // observers happens-before visibility of the result cell.
        self.done.store(true, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::progress::OP_COMPLETE, 0);
    }

    /// Current fraction in [0.0, 1.0]. Returns 0.0 when total is 0
    /// (no task has begun); clamped at 1.0 to avoid >100% display.
    pub fn fraction_complete(&self) -> f64 {
        let p = self.progress.load(Ordering::Relaxed);
        let t = self.total.load(Ordering::Acquire);
        if t == 0 { return 0.0; }
        (p as f64 / t as f64).min(1.0)
    }

    /// Current progress counter value.
    #[inline]
    pub fn current_progress(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }

    /// Total denominator for the current run (0 when no run is active).
    #[inline]
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Acquire)
    }

    /// True when `complete` has been called for the current run.
    #[inline]
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Read the most-recently-published result. Returns None when
    /// `is_done` is false (a result MAY still be there from a prior
    /// completed run, but the current run is not yet finished).
    pub fn read_result(&self) -> Option<R> {
        let r = if self.is_done() {
            Some(self.result.get())
        } else {
            None
        };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::progress::OP_READ,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Read the result cell unconditionally. Useful for reading the
    /// initial value before any run, or for sampling a stale value.
    #[inline]
    pub fn peek_result(&self) -> R {
        self.result.get()
    }

    /// Run the work closure synchronously on the calling thread.
    /// Calls `begin(total)` to set up the reporter, runs the closure,
    /// publishes the result via `complete`, and returns the result.
    pub fn run<F>(&self, total: u64, work: F) -> R
    where F: FnOnce(&ProgressReporter) -> R
    {
        let reporter = self.begin(total);
        let r = work(&reporter);
        self.complete(r);
        r
    }

    /// Spawn the work closure on a background thread. Returns a
    /// JoinHandle so the caller can wait if desired; the task's
    /// completion is also visible via `is_done`.
    pub fn spawn<F>(self: &Arc<Self>, total: u64, work: F) -> JoinHandle<()>
    where
        F: FnOnce(&ProgressReporter) -> R + Send + 'static,
        R: Send + Sync,
    {
        let me = self.clone();
        thread::spawn(move || {
            me.run(total, work);
        })
    }

    /// Sync all four files to disk.
    pub fn flush(&self) -> Result<(), ProgressTaskError> {
        self.progress.flush()?;
        self.total.flush()?;
        self.done.flush()?;
        self.result.flush()?;
        Ok(())
    }

    /// Non-blocking flush of all four files. Delegates to each inner
    /// primitive's flush_async.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), ProgressTaskError> {
        self.progress.flush_async()?;
        self.total.flush_async()?;
        self.done.flush_async()?;
        self.result.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-progress-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        std::fs::remove_file(progress_path(base)).ok();
        std::fs::remove_file(total_path(base)).ok();
        std::fs::remove_file(done_path(base)).ok();
        std::fs::remove_file(result_path(base)).ok();
    }

    #[test]
    fn create_initial_state_is_zero() {
        let base = tmp_base("init");
        let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        assert_eq!(t.current_progress(), 0);
        assert_eq!(t.total(), 0);
        assert!(!t.is_done());
        assert_eq!(t.fraction_complete(), 0.0);
        assert_eq!(t.read_result(), None);
        cleanup(&base);
    }

    #[test]
    fn run_advances_progress_and_publishes_result() {
        let base = tmp_base("run");
        let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        let r = t.run(100, |reporter| {
            for _ in 0..100 {
                reporter.advance(1);
            }
            42
        });
        assert_eq!(r, 42);
        assert!(t.is_done());
        assert_eq!(t.read_result(), Some(42));
        assert_eq!(t.fraction_complete(), 1.0);
        cleanup(&base);
    }

    #[test]
    fn observer_sees_monotonic_progress_advance() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let base = tmp_base("observe");
        let t: Arc<ProgressTask<u64>>
            = Arc::new(ProgressTask::create(&base, 0).unwrap());
        let go = Arc::new(AtomicBool::new(false));

        let t_w = t.clone();
        let go_w = go.clone();
        let worker = thread::spawn(move || {
            t_w.run(50, |reporter| {
                go_w.store(true, Ordering::Release);
                for _ in 0..50 {
                    reporter.advance(1);
                    thread::sleep(Duration::from_micros(100));
                }
                7777
            });
        });
        // Wait until worker has begun (avoid race on the initial value).
        while !go.load(Ordering::Acquire) { thread::yield_now(); }

        let mut observed = Vec::new();
        for _ in 0..20 {
            observed.push(t.current_progress());
            thread::sleep(Duration::from_micros(150));
        }
        worker.join().unwrap();
        // Monotonic non-decreasing.
        for w in observed.windows(2) {
            assert!(w[0] <= w[1], "progress went backwards: {observed:?}");
        }
        assert!(t.is_done());
        assert_eq!(t.read_result(), Some(7777));
        cleanup(&base);
    }

    #[test]
    fn cross_handle_observer_sees_worker_state() {
        let base = tmp_base("cross-handle");
        let worker_h: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        let observer_h: ProgressTask<u64> = ProgressTask::open(&base).unwrap();
        worker_h.run(10, |r| { r.advance(10); 999 });
        assert!(observer_h.is_done());
        assert_eq!(observer_h.read_result(), Some(999));
        assert_eq!(observer_h.fraction_complete(), 1.0);
        cleanup(&base);
    }

    #[test]
    fn fraction_clamps_when_progress_overshoots_total() {
        let base = tmp_base("overshoot");
        let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        let r = t.begin(10);
        r.advance(15);
        assert_eq!(t.fraction_complete(), 1.0);
        cleanup(&base);
    }

    #[test]
    fn read_result_returns_none_before_done() {
        let base = tmp_base("not-done");
        let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        let _r = t.begin(100);
        assert!(!t.is_done());
        assert_eq!(t.read_result(), None);
        // peek bypasses the done check.
        assert_eq!(t.peek_result(), 0);
        cleanup(&base);
    }

    #[test]
    fn second_run_resets_progress_and_done() {
        let base = tmp_base("two-runs");
        let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        t.run(5, |r| { r.advance(5); 100 });
        assert!(t.is_done());
        assert_eq!(t.current_progress(), 5);

        // Begin a new run; done resets, progress resets, total updates.
        let _r = t.begin(10);
        assert!(!t.is_done());
        assert_eq!(t.current_progress(), 0);
        assert_eq!(t.total(), 10);
        cleanup(&base);
    }

    #[test]
    fn spawn_background_completes_eventually() {
        let base = tmp_base("spawn");
        let t: Arc<ProgressTask<u64>>
            = Arc::new(ProgressTask::create(&base, 0).unwrap());
        let h = t.spawn(20, |r| {
            for _ in 0..20 {
                r.advance(1);
                thread::sleep(Duration::from_micros(100));
            }
            314
        });
        h.join().unwrap();
        assert!(t.is_done());
        assert_eq!(t.read_result(), Some(314));
        cleanup(&base);
    }

    #[test]
    fn concurrent_observers_all_see_consistent_completion() {
        let base = tmp_base("multi-observer");
        let t: Arc<ProgressTask<u64>>
            = Arc::new(ProgressTask::create(&base, 0).unwrap());
        let n_observers = 4;

        let t_w = t.clone();
        let worker = thread::spawn(move || {
            t_w.run(100, |r| {
                for _ in 0..100 {
                    r.advance(1);
                    thread::sleep(Duration::from_micros(50));
                }
                5555
            });
        });

        let mut handles = vec![];
        for _ in 0..n_observers {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                let mut last = 0u64;
                while !t.is_done() {
                    let cur = t.current_progress();
                    assert!(cur >= last, "observer saw progress regress");
                    last = cur;
                    thread::sleep(Duration::from_micros(75));
                }
                t.read_result()
            }));
        }
        worker.join().unwrap();
        for h in handles {
            assert_eq!(h.join().unwrap(), Some(5555));
        }
        cleanup(&base);
    }

    #[test]
    fn struct_result_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Stats { processed: u64, errors: u32, skipped: u32 }
        let base = tmp_base("struct");
        let t: ProgressTask<Stats> = ProgressTask::create(
            &base, Stats { processed: 0, errors: 0, skipped: 0 },
        ).unwrap();
        let r = t.run(50, |reporter| {
            for _ in 0..50 { reporter.advance(1); }
            Stats { processed: 50, errors: 2, skipped: 1 }
        });
        assert_eq!(r, Stats { processed: 50, errors: 2, skipped: 1 });
        assert_eq!(t.read_result(), Some(r));
        cleanup(&base);
    }

    #[test]
    fn disk_persistence_completed_task_survives_reopen() {
        let base = tmp_base("disk");
        {
            let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
            t.run(8, |r| { r.advance(8); 8888 });
            t.flush().unwrap();
        }
        let t2: ProgressTask<u64> = ProgressTask::open(&base).unwrap();
        assert!(t2.is_done());
        assert_eq!(t2.read_result(), Some(8888));
        assert_eq!(t2.current_progress(), 8);
        assert_eq!(t2.total(), 8);
        cleanup(&base);
    }

    #[test]
    fn reporter_set_replaces_progress() {
        let base = tmp_base("set");
        let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
        let r = t.begin(1000);
        r.advance(100);
        assert_eq!(t.current_progress(), 100);
        r.set(500);  // jump
        assert_eq!(t.current_progress(), 500);
        assert_eq!(t.fraction_complete(), 0.5);
        cleanup(&base);
    }
}
