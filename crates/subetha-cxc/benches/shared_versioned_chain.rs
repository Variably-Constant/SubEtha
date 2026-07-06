//! Bench: SharedVersionedChain<T> vs `Mutex<Vec<(u64, T)>>` linear
//! scan baseline.
//!
//! Architectural claim: cross-process MVCC linked list with O(1)
//! push and snapshot-version read. Mutex baseline does linear
//! scan under lock.
//!
//! Workloads:
//! - push (CAS prepend)
//! - read_at (walk newest-first to find <=snapshot)
//! - current (head read)
//! - len

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedVersionedChain;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-vchain-{name}-{pid}.bin"));
    p
}

fn push_hot(c: &mut Criterion) {
    let p = tmp("push");
    c.bench_function("vchain.push/mmf", |b| {
        b.iter_batched(
            || {
                std::fs::remove_file(&p).ok();
                SharedVersionedChain::<u64>::create(&p, 256).unwrap()
            },
            |ch| {
                ch.push(black_box(1), black_box(42)).expect("push");
            },
            criterion::BatchSize::PerIteration,
        );
    });
    std::fs::remove_file(&p).ok();

    c.bench_function("vchain.push/mutex_vec", |b| {
        b.iter_batched(
            || Mutex::new(Vec::<(u64, u64)>::with_capacity(256)),
            |m| {
                m.lock().unwrap().push((black_box(1), black_box(42)));
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

fn read_at_hot(c: &mut Criterion) {
    let p = tmp("read-at");
    let ch: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 256).unwrap();
    for v in 0..100u64 { ch.push(v, v * 10).unwrap(); }
    c.bench_function("vchain.read_at/mmf", |b| {
        b.iter(|| black_box(ch.read_at(black_box(50))));
    });
    drop(ch);
    std::fs::remove_file(&p).ok();

    let m: Mutex<Vec<(u64, u64)>> = Mutex::new((0..100u64).map(|v| (v, v * 10)).collect());
    c.bench_function("vchain.read_at/mutex_vec_scan", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(g.iter().rev().find(|(v, _)| *v <= 50).copied())
        });
    });
}

fn current_hot(c: &mut Criterion) {
    let p = tmp("current");
    let ch: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 16).unwrap();
    ch.push(42, 100).unwrap();
    c.bench_function("vchain.current/mmf", |b| {
        b.iter(|| black_box(ch.current()));
    });
    drop(ch);
    std::fs::remove_file(&p).ok();
}

fn len_hot(c: &mut Criterion) {
    let p = tmp("len");
    let ch: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 16).unwrap();
    for v in 0..10u64 { ch.push(v, v).unwrap(); }
    c.bench_function("vchain.len/mmf", |b| {
        b.iter(|| black_box(ch.len()));
    });
    drop(ch);
    std::fs::remove_file(&p).ok();
}

criterion_group!(benches, push_hot, read_at_hot, current_hot, len_hot);
criterion_main!(benches);
