//! Bench: SharedOnceCell vs std::sync::OnceLock baseline.
//!
//! Architectural claim: SharedOnceCell adds MMF-backed
//! cross-process semantics at near-identical per-op cost vs
//! std::sync::OnceLock for the get-after-init hot path.
//!
//! Workloads:
//! - get on initialized cell (hot path)
//! - get_or_init when already initialized (no closure run)

use std::hint::black_box;
use std::sync::OnceLock;

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_cxc::shared_once_cell::SharedOnceCell;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-once-{name}-{pid}.bin"));
    p
}

fn get_initialized(c: &mut Criterion) {
    let mut g = c.benchmark_group("once_cell.get_initialized");

    let std_lock: OnceLock<u64> = OnceLock::new();
    std_lock.set(42).unwrap();
    g.bench_function("native_std_oncelock", |b| {
        b.iter(|| black_box(std_lock.get().copied()));
    });

    let path = tmp("get-init");
    let shared: SharedOnceCell<u64> = SharedOnceCell::create(&path).unwrap();
    shared.set(42);
    g.bench_function("shared_once_cell", |b| {
        b.iter(|| black_box(shared.get()));
    });
    drop(shared);
    std::fs::remove_file(&path).ok();

    g.finish();
}

fn get_or_init_already_set(c: &mut Criterion) {
    let mut g = c.benchmark_group("once_cell.get_or_init_already_set");

    let std_lock: OnceLock<u64> = OnceLock::new();
    std_lock.set(42).unwrap();
    g.bench_function("native_std_oncelock", |b| {
        b.iter(|| black_box(std_lock.get_or_init(|| panic!("init shouldn't run"))));
    });

    let path = tmp("get-or-init");
    let shared: SharedOnceCell<u64> = SharedOnceCell::create(&path).unwrap();
    shared.set(42);
    g.bench_function("shared_once_cell", |b| {
        b.iter(|| black_box(shared.get_or_init(|| panic!("init shouldn't run"))));
    });
    drop(shared);
    std::fs::remove_file(&path).ok();

    g.finish();
}

criterion_group!(benches, get_initialized, get_or_init_already_set);
criterion_main!(benches);
