//! Bench: SharedAsyncPointer vs std::sync::OnceLock + manual hedging.
//!
//! Architectural claim: SharedAsyncPointer provides three resolution
//! strategies (Resolved / Lazy / Speculative) over an MMF-backed cell
//! with cross-process visibility. The speculative race is the
//! distinctive primitive: first-publisher-wins across N workers, with
//! survivor-tolerant variants.
//!
//! Contenders:
//! - already-resolved peek (try_get) vs OnceLock::get
//! - get_or_lazy vs OnceLock::get_or_init (single-thread compute path)
//! - get_or_speculative N=2 (fast + slow worker) vs sequential fast-then-slow
//! - get_or_speculative N=4 same-speed (redundant-compute overhead measurement)

use std::hint::black_box;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedAsyncPointer;

/// Monotonic per-process counter for unique bench-file suffixes.
/// Suffixes are pure decimal integers so the resulting paths contain
/// no characters reserved by Windows filesystems.
static UNIQUE_SUFFIX: AtomicU64 = AtomicU64::new(0);

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-async-bench-{name}-{pid}.bin"));
    p
}

fn unique_tmp_path(prefix: &str) -> std::path::PathBuf {
    let n = UNIQUE_SUFFIX.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let mut p = std::env::temp_dir();
    p.push(format!("subetha-async-bench-{prefix}-{pid}-{n}.bin"));
    p
}

// =========================================================
// Resolved (pre-set) fast-path peek.
//
// Compares an already-initialized SharedAsyncPointer::try_get against
// std::sync::OnceLock::get. Both should be ~1 atomic load.
// =========================================================

fn resolved_peek(c: &mut Criterion) {
    let p = tmp_path("resolved-peek");
    let sap: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
    sap.set_resolved(12345);

    c.bench_function("shared_async.resolved_peek/mmf", |b| {
        b.iter(|| black_box(sap.try_get()));
    });

    let lock: OnceLock<u64> = OnceLock::new();
    lock.set(12345).unwrap();
    c.bench_function("shared_async.resolved_peek/oncelock", |b| {
        b.iter(|| black_box(lock.get().copied()));
    });

    std::fs::remove_file(&p).ok();
}

// =========================================================
// Lazy single-thread compute + publish.
//
// Both shapes do: check, miss, compute, publish, return. The
// MMF-backed SharedAsyncPointer also writes to disk-mapped storage,
// so it should be slightly slower than the in-memory OnceLock.
// =========================================================

fn lazy_first_compute(c: &mut Criterion) {
    c.bench_function("shared_async.lazy_first_compute/mmf", |b| {
        b.iter_with_setup(
            || {
                let p = unique_tmp_path("lazy");
                let sap: SharedAsyncPointer<u64> =
                    SharedAsyncPointer::create(&p).unwrap();
                (sap, p)
            },
            |(sap, path)| {
                let v = sap.get_or_lazy(|| black_box(99u64));
                black_box(v);
                // Release the MMF mapping before delete; on Windows
                // the OS may otherwise hold the file lock.
                drop(sap);
                std::fs::remove_file(&path).ok();
            },
        );
    });

    c.bench_function("shared_async.lazy_first_compute/oncelock", |b| {
        b.iter_with_setup(
            OnceLock::<u64>::new,
            |lock| {
                let v = *lock.get_or_init(|| black_box(99u64));
                black_box(v);
            },
        );
    });
}

// =========================================================
// Speculative race for latency hedging.
//
// Workload: 2 workers, one finishes in 2 ms, one in 20 ms.
// SharedAsyncPointer's get_or_speculative races them; sequential
// baseline runs the slow path first then short-circuits.
//
// The interesting comparison is "latency-hedged vs slow-path-only".
// =========================================================

fn speculative_hedging(c: &mut Criterion) {
    c.bench_function("shared_async.speculative_2_hedged/mmf", |b| {
        b.iter_with_setup(
            || {
                let p = unique_tmp_path("spec2");
                let sap: SharedAsyncPointer<u64> =
                    SharedAsyncPointer::create(&p).unwrap();
                (sap, p)
            },
            |(sap, path)| {
                let v = sap.get_or_speculative_with([
                    Box::new(|| {
                        thread::sleep(Duration::from_millis(20));
                        999u64
                    }) as Box<dyn FnOnce() -> u64 + Send>,
                    Box::new(|| {
                        thread::sleep(Duration::from_millis(2));
                        100u64
                    }) as Box<dyn FnOnce() -> u64 + Send>,
                ]);
                black_box(v);
                drop(sap);
                std::fs::remove_file(&path).ok();
            },
        );
    });

    c.bench_function("shared_async.speculative_2_hedged/sequential_slow", |b| {
        b.iter(|| {
            // Sequential baseline: take the slow path, no hedging.
            thread::sleep(Duration::from_millis(20));
            black_box(999u64);
        });
    });

    c.bench_function("shared_async.speculative_2_hedged/sequential_fast", |b| {
        b.iter(|| {
            // Sequential baseline: assume we KNEW which path was fast.
            // This is the "perfect oracle" lower bound for any
            // hedging primitive.
            thread::sleep(Duration::from_millis(2));
            black_box(100u64);
        });
    });
}

// =========================================================
// Speculative same-speed overhead.
//
// All N workers run the same-cost closure. The bench measures the
// overhead of redundant compute when there is NO hedging benefit
// (the workers are interchangeable). The fastest-wins still applies
// but expected wall time approaches the single-worker time.
// =========================================================

fn speculative_overhead(c: &mut Criterion) {
    c.bench_function("shared_async.speculative_4_same/mmf", |b| {
        b.iter_with_setup(
            || {
                let p = unique_tmp_path("spec4");
                let sap: SharedAsyncPointer<u64> =
                    SharedAsyncPointer::create(&p).unwrap();
                (sap, p)
            },
            |(sap, path)| {
                let v = sap.get_or_speculative(4, || {
                    thread::sleep(Duration::from_millis(2));
                    42u64
                });
                black_box(v);
                drop(sap);
                std::fs::remove_file(&path).ok();
            },
        );
    });

    c.bench_function("shared_async.speculative_4_same/lazy_single", |b| {
        b.iter_with_setup(
            || {
                let p = unique_tmp_path("lazy1");
                let sap: SharedAsyncPointer<u64> =
                    SharedAsyncPointer::create(&p).unwrap();
                (sap, p)
            },
            |(sap, path)| {
                let v = sap.get_or_lazy(|| {
                    thread::sleep(Duration::from_millis(2));
                    42u64
                });
                black_box(v);
                drop(sap);
                std::fs::remove_file(&path).ok();
            },
        );
    });
}

criterion_group!(
    benches,
    resolved_peek,
    lazy_first_compute,
    speculative_hedging,
    speculative_overhead,
);
criterion_main!(benches);
