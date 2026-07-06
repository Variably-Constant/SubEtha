//! Bench: SharedTreiberStack vs Mutex<Vec<T>> (textbook in-process
//! stack baseline) and crossbeam_queue::SegQueue (lock-free
//! reference).

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedTreiberStack;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-stack-{name}-{pid}.bin"));
    p
}

fn push_hot(c: &mut Criterion) {
    let p = tmp("push");
    let s: SharedTreiberStack<u64> = SharedTreiberStack::create(&p, 1 << 20).unwrap();
    c.bench_function("stack.push/mmf", |b| {
        b.iter(|| {
            if s.push(black_box(42)).is_err() {
                while s.pop().is_some() {}
            }
        });
    });
    drop(s);
    std::fs::remove_file(&p).ok();

    let m: Mutex<Vec<u64>> = Mutex::new(Vec::with_capacity(1 << 20));
    c.bench_function("stack.push/mutex_vec", |b| {
        b.iter(|| m.lock().unwrap().push(black_box(42)));
    });
}

fn push_pop_cycle(c: &mut Criterion) {
    let p = tmp("cycle");
    let s: SharedTreiberStack<u64> = SharedTreiberStack::create(&p, 16).unwrap();
    c.bench_function("stack.push_pop_cycle/mmf", |b| {
        b.iter(|| {
            s.push(black_box(42)).unwrap();
            black_box(s.pop().unwrap())
        });
    });
    drop(s);
    std::fs::remove_file(&p).ok();

    let m: Mutex<Vec<u64>> = Mutex::new(Vec::with_capacity(16));
    c.bench_function("stack.push_pop_cycle/mutex_vec", |b| {
        b.iter(|| {
            m.lock().unwrap().push(black_box(42));
            black_box(m.lock().unwrap().pop().unwrap())
        });
    });
}

// Multi-thread push/pop correctness is covered by source-level
// unit tests. A per-iter thread::spawn microbench is dominated
// by Windows thread-creation cost.

criterion_group!(benches, push_hot, push_pop_cycle);
criterion_main!(benches);
