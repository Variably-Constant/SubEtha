//! Bench: SharedAtomicU64 vs std::sync::atomic::AtomicU64.
//!
//! Architectural claim: SharedAtomic backs the atomic with an MMF
//! so multiple processes share the same value via cache coherence;
//! the per-op cost should be near-identical to std's in-process
//! atomic (the mmap pointer-deref is one extra load).
//!
//! Workloads:
//! - load (Acquire)
//! - store (Release)
//! - fetch_add (AcqRel)
//! - compare_exchange (AcqRel / Acquire)

use std::hint::black_box;
use std::sync::atomic::{AtomicU64 as StdAtomicU64, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_cxc::shared_atomic::SharedAtomicU64;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-atomic-{name}-{pid}.bin"));
    p
}

fn load_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("atomic.load");

    let std_a = StdAtomicU64::new(42);
    g.bench_function("native_std_atomicu64", |b| {
        b.iter(|| black_box(std_a.load(Ordering::Acquire)));
    });

    let path = tmp("load");
    let shared = SharedAtomicU64::create(&path, 42).unwrap();
    g.bench_function("shared_atomicu64", |b| {
        b.iter(|| black_box(shared.load(Ordering::Acquire)));
    });
    drop(shared);
    std::fs::remove_file(&path).ok();

    g.finish();
}

fn fetch_add_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("atomic.fetch_add");

    let std_a = StdAtomicU64::new(0);
    g.bench_function("native_std_atomicu64", |b| {
        b.iter(|| black_box(std_a.fetch_add(1, Ordering::AcqRel)));
    });

    let path = tmp("fetch-add");
    let shared = SharedAtomicU64::create(&path, 0).unwrap();
    g.bench_function("shared_atomicu64", |b| {
        b.iter(|| black_box(shared.fetch_add(1, Ordering::AcqRel)));
    });
    drop(shared);
    std::fs::remove_file(&path).ok();

    g.finish();
}

fn cas_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("atomic.compare_exchange");

    let std_a = StdAtomicU64::new(0);
    g.bench_function("native_std_atomicu64", |b| {
        let mut expected = 0u64;
        b.iter(|| {
            let new = expected.wrapping_add(1);
            let r = std_a.compare_exchange(
                expected, new,
                Ordering::AcqRel, Ordering::Acquire,
            );
            expected = r.unwrap_or_else(|v| v);
            black_box(expected)
        });
    });

    let path = tmp("cas");
    let shared = SharedAtomicU64::create(&path, 0).unwrap();
    g.bench_function("shared_atomicu64", |b| {
        let mut expected = 0u64;
        b.iter(|| {
            let new = expected.wrapping_add(1);
            let r = shared.compare_exchange(
                expected, new,
                Ordering::AcqRel, Ordering::Acquire,
            );
            expected = r.unwrap_or_else(|v| v);
            black_box(expected)
        });
    });
    drop(shared);
    std::fs::remove_file(&path).ok();

    g.finish();
}

criterion_group!(benches, load_workload, fetch_add_workload, cas_workload);
criterion_main!(benches);
