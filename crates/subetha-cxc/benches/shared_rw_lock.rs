//! Bench: SharedRWLock vs std::sync::RwLock and parking_lot::RwLock.
//!
//! Architectural claim: SharedRWLock provides cross-process
//! reader-writer semantics at comparable per-op cost to in-process
//! RW locks, plus writer-priority semantics that prevent reader
//! starvation.

use std::hint::black_box;
use std::sync::{Arc, RwLock};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use parking_lot::RwLock as PlRwLock;

use subetha_cxc::SharedRWLock;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-rwlock-{name}-{pid}.bin"));
    p
}

fn try_read_uncontended(c: &mut Criterion) {
    let p = tmp("try-read");
    let l = SharedRWLock::create(&p).unwrap();
    c.bench_function("rwlock.try_read/mmf", |b| {
        b.iter(|| {
            let g = l.try_read_lock().unwrap();
            black_box(&g);
        });
    });
    drop(l);
    std::fs::remove_file(&p).ok();

    let std_l = RwLock::new(0u64);
    c.bench_function("rwlock.try_read/std", |b| {
        b.iter(|| {
            let g = std_l.try_read().unwrap();
            black_box(&g);
        });
    });

    let pl_l = PlRwLock::new(0u64);
    c.bench_function("rwlock.try_read/parking_lot", |b| {
        b.iter(|| {
            let g = pl_l.try_read().unwrap();
            black_box(&g);
        });
    });
}

fn try_write_uncontended(c: &mut Criterion) {
    let p = tmp("try-write");
    let l = SharedRWLock::create(&p).unwrap();
    c.bench_function("rwlock.try_write/mmf", |b| {
        b.iter(|| {
            let g = l.try_write_lock().unwrap();
            black_box(&g);
        });
    });
    drop(l);
    std::fs::remove_file(&p).ok();

    let std_l = RwLock::new(0u64);
    c.bench_function("rwlock.try_write/std", |b| {
        b.iter(|| {
            let g = std_l.try_write().unwrap();
            black_box(&g);
        });
    });

    let pl_l = PlRwLock::new(0u64);
    c.bench_function("rwlock.try_write/parking_lot", |b| {
        b.iter(|| {
            let g = pl_l.try_write().unwrap();
            black_box(&g);
        });
    });
}

fn concurrent_readers_4t(c: &mut Criterion) {
    const N_READS: u32 = 100;

    let p = tmp("concurrent-r");
    let l = Arc::new(SharedRWLock::create(&p).unwrap());
    c.bench_function("rwlock.concurrent_readers_4t/mmf", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let l = l.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..N_READS {
                        let g = l.read_lock();
                        black_box(&g);
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
    drop(l);
    std::fs::remove_file(&p).ok();

    let std_l = Arc::new(RwLock::new(0u64));
    c.bench_function("rwlock.concurrent_readers_4t/std", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let l = std_l.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..N_READS {
                        let g = l.read().unwrap();
                        black_box(&g);
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
}

criterion_group!(benches,
    try_read_uncontended,
    try_write_uncontended,
    concurrent_readers_4t,
);
criterion_main!(benches);
