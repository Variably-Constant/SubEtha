//! Bench: SharedCell SeqLock vs RwLock<T> baseline.
//!
//! Architectural claim: SharedCell uses a SeqLock protocol over
//! MMF, so single-writer/multi-reader workloads have no reader
//! contention (vs RwLock's reader-acquire serialization). For
//! small T the SeqLock retry is rare under single writer.
//!
//! Workloads:
//! - get hot (single read)
//! - set hot (single write)
//! - get vs RwLock::read
//! - set vs RwLock::write

use std::hint::black_box;
use std::sync::RwLock;

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_cxc::shared_cell::SharedCell;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-cell-{name}-{pid}.bin"));
    p
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct State {
    epoch: u64,
    counter: u64,
    flags: u32,
    pad: u32,
}

fn get_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("cell.get");

    let rw = RwLock::new(State { epoch: 1, counter: 100, flags: 0, pad: 0 });
    g.bench_function("native_rwlock_struct", |b| {
        b.iter(|| {
            let s = rw.read().unwrap();
            black_box(*s)
        });
    });

    let path = tmp("get");
    let cell: SharedCell<State> = SharedCell::create(&path).unwrap();
    cell.set(State { epoch: 1, counter: 100, flags: 0, pad: 0 });
    g.bench_function("shared_cell_struct", |b| {
        b.iter(|| black_box(cell.get()));
    });
    drop(cell);
    std::fs::remove_file(&path).ok();

    g.finish();
}

fn set_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("cell.set");

    let rw = RwLock::new(State { epoch: 0, counter: 0, flags: 0, pad: 0 });
    g.bench_function("native_rwlock_struct", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = i.wrapping_add(1);
            let mut s = rw.write().unwrap();
            *s = State { epoch: i, counter: i * 10, flags: 0, pad: 0 };
        });
    });

    let path = tmp("set");
    let cell: SharedCell<State> = SharedCell::create(&path).unwrap();
    g.bench_function("shared_cell_struct", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = i.wrapping_add(1);
            cell.set(State { epoch: i, counter: i * 10, flags: 0, pad: 0 });
        });
    });
    drop(cell);
    std::fs::remove_file(&path).ok();

    g.finish();
}

criterion_group!(benches, get_workload, set_workload);
criterion_main!(benches);
