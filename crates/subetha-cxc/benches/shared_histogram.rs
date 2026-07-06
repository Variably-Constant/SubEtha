//! Bench: SharedHistogram vs Mutex<Vec<u64>> baseline.
//!
//! Architectural claim: per-bucket AtomicU64 fetch_add beats
//! Mutex-guarded Vec<u64> on every workload because different
//! buckets contend on different cache lines (true parallelism)
//! while Mutex<Vec> serializes ALL recorders regardless of which
//! bucket they hit.
//!
//! Workloads:
//! - record single value (hot path)
//! - count_for_bucket (observer)
//! - 4-thread concurrent record into mixed buckets
//! - percentile computation

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedHistogram;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-histogram-{name}-{pid}.bin"));
    p
}

const LATENCY_BOUNDS: &[u64] = &[10, 100, 1_000, 10_000, 100_000, 1_000_000];

struct MutexHistogram {
    counters: Mutex<Vec<u64>>,
    boundaries: Vec<u64>,
}

impl MutexHistogram {
    fn new(bounds: &[u64]) -> Self {
        Self {
            counters: Mutex::new(vec![0u64; bounds.len() + 1]),
            boundaries: bounds.to_vec(),
        }
    }
    fn bucket_for(&self, v: u64) -> usize {
        self.boundaries.partition_point(|&b| b <= v)
    }
    fn record(&self, v: u64) {
        let idx = self.bucket_for(v);
        self.counters.lock().unwrap()[idx] += 1;
    }
    fn count(&self, idx: usize) -> u64 {
        self.counters.lock().unwrap()[idx]
    }
}

// =========================================================
// record single
// =========================================================

fn record_single(c: &mut Criterion) {
    let p = tmp("record");
    let h = SharedHistogram::create(&p, LATENCY_BOUNDS).unwrap();
    c.bench_function("histogram.record/mmf", |b| {
        b.iter(|| black_box(h.record(black_box(500))));
    });
    drop(h);
    std::fs::remove_file(&p).ok();

    let m = MutexHistogram::new(LATENCY_BOUNDS);
    c.bench_function("histogram.record/mutex_vec", |b| {
        b.iter(|| m.record(black_box(500)));
    });
}

// =========================================================
// count observer
// =========================================================

fn count_observer(c: &mut Criterion) {
    let p = tmp("count");
    let h = SharedHistogram::create(&p, LATENCY_BOUNDS).unwrap();
    for _ in 0..1000 { h.record(500); }
    c.bench_function("histogram.count/mmf", |b| {
        b.iter(|| black_box(h.count(black_box(3)).unwrap()));
    });
    drop(h);
    std::fs::remove_file(&p).ok();

    let m = MutexHistogram::new(LATENCY_BOUNDS);
    for _ in 0..1000 { m.record(500); }
    c.bench_function("histogram.count/mutex_vec", |b| {
        b.iter(|| black_box(m.count(black_box(3))));
    });
}

// Multi-thread record correctness is covered by the source-level
// tests. A per-iter thread::spawn microbench is dominated by
// Windows thread-creation cost.

// =========================================================
// percentile
// =========================================================

fn percentile_compute(c: &mut Criterion) {
    let p = tmp("percentile");
    let h = SharedHistogram::create(&p, LATENCY_BOUNDS).unwrap();
    for i in 0..10_000u64 {
        let v = match i % 100 {
            0..=80 => 5,
            81..=95 => 50,
            _ => 5000,
        };
        h.record(v);
    }
    c.bench_function("histogram.percentile_p99/mmf", |b| {
        b.iter(|| black_box(h.percentile(black_box(0.99))));
    });
    drop(h);
    std::fs::remove_file(&p).ok();
}

criterion_group!(benches,
    record_single,
    count_observer,
    percentile_compute,
);
criterion_main!(benches);
