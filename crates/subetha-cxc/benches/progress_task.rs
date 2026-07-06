//! Bench: ProgressTask<R> vs the naive in-process progress-counter
//! pattern (Arc<AtomicU64> progress + Arc<AtomicU64> total +
//! Arc<AtomicBool> done + Arc<Mutex<R>> result).
//!
//! Architectural claim: ProgressTask matches the in-process baseline
//! on hot reader paths (the dominant observer cost) AND adds
//! cross-process visibility + disk persistence at the same cost,
//! which the in-process baseline cannot provide.
//!
//! Workloads:
//! - hot observer read (current_progress / fraction_complete / is_done)
//! - reporter.advance hot loop (worker tight inner loop)
//! - full run cycle: begin + N advances + complete (end-to-end)
//! - contended: 1 worker advancing + 4 observers reading

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::ProgressTask;

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-progress-{name}-{pid}"));
    p
}

fn cleanup_base(base: &std::path::Path) {
    for ext in ["progress", "total", "done", "result"] {
        let mut p = base.to_path_buf();
        let stem = p.file_name().unwrap().to_string_lossy().to_string();
        p.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&p).ok();
    }
}

// =========================================================
// Naive in-process baseline
// =========================================================

#[derive(Clone)]
struct NaiveProgress<R: Copy + Default> {
    progress: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    result: Arc<Mutex<R>>,
}

impl<R: Copy + Default + Send + 'static> NaiveProgress<R> {
    fn new() -> Self {
        Self {
            progress: Arc::new(AtomicU64::new(0)),
            total: Arc::new(AtomicU64::new(0)),
            done: Arc::new(AtomicBool::new(false)),
            result: Arc::new(Mutex::new(R::default())),
        }
    }
    fn begin(&self, total: u64) {
        self.progress.store(0, Ordering::Relaxed);
        self.done.store(false, Ordering::Release);
        self.total.store(total, Ordering::Release);
    }
    fn advance(&self, n: u64) -> u64 {
        self.progress.fetch_add(n, Ordering::Relaxed)
    }
    fn complete(&self, r: R) {
        *self.result.lock().unwrap() = r;
        self.done.store(true, Ordering::Release);
    }
    fn current_progress(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }
    fn fraction_complete(&self) -> f64 {
        let p = self.progress.load(Ordering::Relaxed);
        let t = self.total.load(Ordering::Acquire);
        if t == 0 { return 0.0; }
        (p as f64 / t as f64).min(1.0)
    }
    #[allow(dead_code)] // available for callers; bench doesn't exercise it
    fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
    #[allow(dead_code)] // available for callers; bench doesn't exercise it
    fn read_result(&self) -> Option<R> {
        if self.done.load(Ordering::Acquire) {
            Some(*self.result.lock().unwrap())
        } else {
            None
        }
    }
}

// =========================================================
// Observer hot path: current_progress
// =========================================================

fn observer_current_progress(c: &mut Criterion) {
    let base = tmp_base("obs");
    let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
    let r = t.begin(1000);
    r.advance(500);
    c.bench_function("progress.current/mmf", |b| {
        b.iter(|| black_box(t.current_progress()));
    });
    drop(t);
    cleanup_base(&base);

    let n: NaiveProgress<u64> = NaiveProgress::new();
    n.begin(1000);
    n.advance(500);
    c.bench_function("progress.current/naive_arc_atomic", |b| {
        b.iter(|| black_box(n.current_progress()));
    });
}

// =========================================================
// Observer fraction_complete: load progress + load total + divide
// =========================================================

fn observer_fraction(c: &mut Criterion) {
    let base = tmp_base("frac");
    let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
    let r = t.begin(1000);
    r.advance(500);
    c.bench_function("progress.fraction/mmf", |b| {
        b.iter(|| black_box(t.fraction_complete()));
    });
    drop(t);
    cleanup_base(&base);

    let n: NaiveProgress<u64> = NaiveProgress::new();
    n.begin(1000);
    n.advance(500);
    c.bench_function("progress.fraction/naive_arc_atomic", |b| {
        b.iter(|| black_box(n.fraction_complete()));
    });
}

// =========================================================
// Worker tight loop: reporter.advance
// =========================================================

fn worker_advance(c: &mut Criterion) {
    let base = tmp_base("adv");
    let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
    let r = t.begin(u64::MAX);
    c.bench_function("progress.advance/mmf", |b| {
        b.iter(|| r.advance(black_box(1)));
    });
    drop(t);
    cleanup_base(&base);

    let n: NaiveProgress<u64> = NaiveProgress::new();
    n.begin(u64::MAX);
    c.bench_function("progress.advance/naive_arc_atomic", |b| {
        b.iter(|| n.advance(black_box(1)));
    });
}

// =========================================================
// Full run cycle: begin + N advances + complete
// =========================================================

fn run_cycle(c: &mut Criterion) {
    const N: u64 = 100;
    let base = tmp_base("cycle");
    let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
    c.bench_function("progress.cycle_100/mmf", |b| {
        b.iter(|| {
            t.run(N, |reporter| {
                for _ in 0..N { reporter.advance(1); }
                42
            })
        });
    });
    drop(t);
    cleanup_base(&base);

    let n: NaiveProgress<u64> = NaiveProgress::new();
    c.bench_function("progress.cycle_100/naive_arc_atomic", |b| {
        b.iter(|| {
            n.begin(N);
            for _ in 0..N { n.advance(1); }
            n.complete(42);
        });
    });
}

// Multi-observer concurrent correctness is covered by the source
// unit test `concurrent_observers_all_see_consistent_completion`
// (1 worker + 4 observers, asserts monotonic progress + consistent
// completion view across all observers). A microbench of contended
// 1w+4r via per-iter thread::spawn is dominated by Windows
// thread-creation cost, so this bench is single-threaded.

criterion_group!(benches,
    observer_current_progress,
    observer_fraction,
    worker_advance,
    run_cycle,
);
criterion_main!(benches);
