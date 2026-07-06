//! Bench: SharedFenceClock vs in-process HLC (Mutex<Hlc>) and the
//! naive "just use SystemTime" approach (which is NOT HLC but is the
//! sloppy baseline most code uses for cross-process timestamps).
//!
//! Architectural claim: SharedFenceClock provides the HLC properties
//! (monotonic + bounded skew + causality-respecting) across processes
//! at the cost of one atomic-pair load+store per tick, comparable to
//! Mutex<Hlc> on the uncontended path and dominating it under
//! contention. The Mutex<Hlc> baseline cannot extend across
//! processes; SystemTime can but loses the logical-counter
//! causality property entirely.
//!
//! Workloads:
//! - tick (hot per-event)
//! - get_local (hot reader)
//! - compute_global_fence (walks N slots)
//! - read_global_fence (O(1) header read)

use std::hint::black_box;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{Hlc, SharedFenceClock};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-fenceclock-{name}-{pid}.bin"));
    p
}

// =========================================================
// In-process Mutex<Hlc> baseline
// =========================================================

struct MutexHlc {
    inner: Mutex<Hlc>,
}

impl MutexHlc {
    fn new() -> Self { Self { inner: Mutex::new(Hlc { physical_us: 0, logical: 0 }) } }
    fn tick(&self) -> Hlc {
        let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64;
        let mut g = self.inner.lock().unwrap();
        let new_phys = g.physical_us.max(wall);
        let new_log = if new_phys == g.physical_us { g.logical + 1 } else { 0 };
        *g = Hlc { physical_us: new_phys, logical: new_log };
        *g
    }
    fn get(&self) -> Hlc { *self.inner.lock().unwrap() }
}

// =========================================================
// Naive SystemTime baseline (not HLC; just wall time)
// =========================================================

fn naive_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

// =========================================================
// tick (per-event hot path)
// =========================================================

fn tick_hot(c: &mut Criterion) {
    let p = tmp("tick");
    let clk = SharedFenceClock::create(&p, 2).unwrap();
    let idx = clk.register(1).unwrap();
    c.bench_function("fenceclock.tick/mmf", |b| {
        b.iter(|| black_box(clk.tick(idx)));
    });
    drop(clk);
    std::fs::remove_file(&p).ok();

    let m = MutexHlc::new();
    c.bench_function("fenceclock.tick/mutex_hlc", |b| {
        b.iter(|| black_box(m.tick()));
    });

    c.bench_function("fenceclock.tick/naive_systemtime", |b| {
        b.iter(|| black_box(naive_now()));
    });
}

// =========================================================
// get_local (hot reader)
// =========================================================

fn get_local_hot(c: &mut Criterion) {
    let p = tmp("get");
    let clk = SharedFenceClock::create(&p, 2).unwrap();
    let idx = clk.register(1).unwrap();
    clk.tick(idx);
    c.bench_function("fenceclock.get_local/mmf", |b| {
        b.iter(|| black_box(clk.get_local(idx)));
    });
    drop(clk);
    std::fs::remove_file(&p).ok();

    let m = MutexHlc::new();
    m.tick();
    c.bench_function("fenceclock.get/mutex_hlc", |b| {
        b.iter(|| black_box(m.get()));
    });
}

// =========================================================
// compute_global_fence (walks N slots)
// =========================================================

fn compute_fence(c: &mut Criterion) {
    let p = tmp("compute");
    let clk = SharedFenceClock::create(&p, 64).unwrap();
    for i in 0..16 {
        let idx = clk.register(1000 + i as u32).unwrap();
        clk.tick(idx);
    }
    c.bench_function("fenceclock.compute_fence_16_slots/mmf", |b| {
        b.iter(|| black_box(clk.compute_global_fence()));
    });
    drop(clk);
    std::fs::remove_file(&p).ok();
}

// =========================================================
// read_global_fence (O(1) header read)
// =========================================================

fn read_fence(c: &mut Criterion) {
    let p = tmp("read-fence");
    let clk = SharedFenceClock::create(&p, 4).unwrap();
    let idx = clk.register(1).unwrap();
    clk.tick(idx);
    clk.publish_global_fence();
    c.bench_function("fenceclock.read_fence/mmf", |b| {
        b.iter(|| black_box(clk.read_global_fence()));
    });
    drop(clk);
    std::fs::remove_file(&p).ok();
}

// Multi-thread tick correctness is covered by the source-level
// concurrent unit tests. A per-iter thread::spawn microbench is
// dominated by Windows thread-creation cost.

criterion_group!(benches,
    tick_hot,
    get_local_hot,
    compute_fence,
    read_fence,
);
criterion_main!(benches);
