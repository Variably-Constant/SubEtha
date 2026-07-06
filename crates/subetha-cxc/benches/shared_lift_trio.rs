//! Bench: SharedCell / SharedAtomic / SharedOnceCell vs their
//! in-process equivalents (parking_lot::RwLock, std::sync::atomic,
//! once_cell::sync::OnceCell).
//!
//! The architectural claim isn't "faster than in-process"; it's
//! "comparable to in-process AND works cross-process AND persists
//! to disk." The in-process versions provide none of (2) or (3).

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use once_cell::sync::OnceCell as StdOnceCell;
use parking_lot::RwLock;

use subetha_cxc::{SharedAtomicU64, SharedCell, SharedOnceCell};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-trio-{name}-{pid}.bin"));
    p
}

// =========================================================
// SharedCell vs parking_lot::RwLock cell
// =========================================================

fn cell_get_set(c: &mut Criterion) {
    let path = tmp("cell");
    let sc: SharedCell<u64> = SharedCell::create(&path).unwrap();
    sc.set(42);
    c.bench_function("shared_cell.get/mmf_seqlock", |b| {
        b.iter(|| black_box(sc.get()));
    });
    c.bench_function("shared_cell.set/mmf_seqlock", |b| {
        b.iter(|| sc.set(black_box(99)));
    });
    drop(sc); std::fs::remove_file(&path).ok();

    let rw = RwLock::new(42u64);
    c.bench_function("shared_cell.get/parking_lot_rwlock", |b| {
        b.iter(|| black_box(*rw.read()));
    });
    c.bench_function("shared_cell.set/parking_lot_rwlock", |b| {
        b.iter(|| *rw.write() = black_box(99));
    });
}

// =========================================================
// SharedAtomicU64 vs std::sync::atomic::AtomicU64
// =========================================================

fn atomic_ops(c: &mut Criterion) {
    let path = tmp("atomic");
    let sa = SharedAtomicU64::create(&path, 0).unwrap();
    c.bench_function("shared_atomic.fetch_add/mmf", |b| {
        b.iter(|| sa.fetch_add(black_box(1), Ordering::AcqRel));
    });
    c.bench_function("shared_atomic.load/mmf", |b| {
        b.iter(|| black_box(sa.load(Ordering::Acquire)));
    });
    drop(sa); std::fs::remove_file(&path).ok();

    let a = AtomicU64::new(0);
    c.bench_function("shared_atomic.fetch_add/std", |b| {
        b.iter(|| a.fetch_add(black_box(1), Ordering::AcqRel));
    });
    c.bench_function("shared_atomic.load/std", |b| {
        b.iter(|| black_box(a.load(Ordering::Acquire)));
    });
}

// =========================================================
// SharedAtomicU64 contention vs std AtomicU64
// =========================================================

fn atomic_contention(c: &mut Criterion) {
    let path = tmp("atomic-contention");
    let sa = Arc::new(SharedAtomicU64::create(&path, 0).unwrap());
    c.bench_function("shared_atomic.contention_4_threads/mmf", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let sa = sa.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..250 { sa.fetch_add(1, Ordering::AcqRel); }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });

    let a = Arc::new(AtomicU64::new(0));
    c.bench_function("shared_atomic.contention_4_threads/std", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let a = a.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..250 { a.fetch_add(1, Ordering::AcqRel); }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
}

// =========================================================
// SharedOnceCell vs once_cell::sync::OnceCell (warm-path)
// =========================================================

fn once_warm_path(c: &mut Criterion) {
    let path = tmp("once");
    let sc: SharedOnceCell<u64> = SharedOnceCell::create(&path).unwrap();
    sc.set(42);
    c.bench_function("shared_once.get_warm/mmf", |b| {
        b.iter(|| black_box(sc.get()));
    });
    drop(sc); std::fs::remove_file(&path).ok();

    let o: StdOnceCell<u64> = StdOnceCell::new();
    o.set(42).unwrap();
    c.bench_function("shared_once.get_warm/std_oncecell", |b| {
        b.iter(|| black_box(o.get()));
    });
}

criterion_group!(benches, cell_get_set, atomic_ops, atomic_contention, once_warm_path);
criterion_main!(benches);
