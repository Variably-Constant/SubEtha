//! Bench: SharedVec<T> vs Mutex<Vec<T>> (the textbook in-process
//! shared-vec pattern) and RwLock<Vec<T>> (the reader-optimized
//! variant).
//!
//! Architectural claim: SharedVec provides indexable cross-process
//! storage at lock-free atomic + SeqLock cost per slot. Mutex<Vec>
//! pays a lock+unlock for every push and get; RwLock<Vec> matches
//! readers but loses writers to writer-lock acquisition cost. Plus
//! both in-process baselines are cross-process-impossible.
//!
//! Workloads:
//! - push_back (hot producer)
//! - get(i) for stable i (hot observer; the dominant access pattern)
//! - mixed push+get (interleaved producer/consumer)
//! - 4-thread concurrent push

use std::hint::black_box;
use std::sync::{Mutex, RwLock};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedVec;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-vec-{name}-{pid}.bin"));
    p
}

// =========================================================
// push_back
// =========================================================

fn push_back(c: &mut Criterion) {
    let p = tmp("push");
    let v: SharedVec<u32> = SharedVec::create(&p, 1_000_000).unwrap();
    c.bench_function("vec.push/mmf", |b| {
        b.iter(|| {
            if v.push_back(black_box(7)).is_err() {
                v.clear();
            }
        });
    });
    drop(v);
    std::fs::remove_file(&p).ok();

    let m: Mutex<Vec<u32>> = Mutex::new(Vec::with_capacity(1_000_000));
    c.bench_function("vec.push/mutex_vec", |b| {
        b.iter(|| m.lock().unwrap().push(black_box(7)));
    });

    let rw: RwLock<Vec<u32>> = RwLock::new(Vec::with_capacity(1_000_000));
    c.bench_function("vec.push/rwlock_vec", |b| {
        b.iter(|| rw.write().unwrap().push(black_box(7)));
    });
}

// =========================================================
// get (observer hot path)
// =========================================================

fn get_observer(c: &mut Criterion) {
    let p = tmp("get");
    let v: SharedVec<u32> = SharedVec::create(&p, 256).unwrap();
    for i in 0..100u32 { v.push_back(i).unwrap(); }
    c.bench_function("vec.get/mmf_seqlock", |b| {
        b.iter(|| black_box(v.get(50)));
    });
    drop(v);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<u32>> = Mutex::new((0..100u32).collect());
    c.bench_function("vec.get/mutex_vec", |b| {
        b.iter(|| {
            let g = mv.lock().unwrap();
            black_box(g.get(50).copied())
        });
    });

    let rw: RwLock<Vec<u32>> = RwLock::new((0..100u32).collect());
    c.bench_function("vec.get/rwlock_vec", |b| {
        b.iter(|| {
            let g = rw.read().unwrap();
            black_box(g.get(50).copied())
        });
    });
}

// =========================================================
// len (hot observer; this is what dashboards poll)
// =========================================================

fn len_observer(c: &mut Criterion) {
    let p = tmp("len");
    let v: SharedVec<u32> = SharedVec::create(&p, 256).unwrap();
    for i in 0..42u32 { v.push_back(i).unwrap(); }
    c.bench_function("vec.len/mmf", |b| {
        b.iter(|| black_box(v.len()));
    });
    drop(v);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<u32>> = Mutex::new((0..42u32).collect());
    c.bench_function("vec.len/mutex_vec", |b| {
        b.iter(|| black_box(mv.lock().unwrap().len()));
    });
}

// Multi-thread push correctness is covered by source-level
// unit tests. A per-iter thread::spawn microbench is dominated
// by Windows thread-creation cost.

criterion_group!(benches,
    push_back,
    get_observer,
    len_observer,
);
criterion_main!(benches);
