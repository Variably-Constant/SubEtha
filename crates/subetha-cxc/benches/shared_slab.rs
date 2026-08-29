//! Bench: `SharedSlab<T>` vs `Mutex<Vec<T>>` and `RwLock<Vec<T>>` for a
//! record too large for a `SharedVec` slot.
//!
//! Architectural claim: a slab reads and writes a record of any size
//! under a per-slot SeqLock, so a reader pays one atomic load, the copy,
//! and a second load - and never contends with a reader of another slot.
//! The lock baselines pay a lock+unlock per access and serialize every
//! slot against every other.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. Every arm does the same work: read or write one 168-byte record by
//!    index. The slab arm pays its SeqLock; the lock arms pay their
//!    lock. Neither carries anything the other does not need.
//! 2. `Vec<Record>` is pre-sized to the slab's capacity, so no arm pays
//!    a growth reallocation inside the measured loop, and the lock arms
//!    index a plain `Vec` with no indirection the slab does not also
//!    have.
//! 3. The record is 168 bytes, chosen because it is PAST
//!    `VEC_PAYLOAD_BYTES` (52) - the case a `SharedVec` cannot carry at
//!    all, which is the reason the slab exists. A 4-byte record would
//!    measure `SharedVec`'s territory and flatter the slab's stride.
//!
//! What the numbers do NOT show: both lock baselines are
//! cross-process-impossible, and multi-reader scaling, where SeqLock
//! reads of distinct slots do not contend and a `Mutex` serialises them.

use std::hint::black_box;
use std::sync::{Mutex, RwLock};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedSlab;

const CAPACITY: usize = 65_536;
const PROBE: usize = 4_211;

/// Past `VEC_PAYLOAD_BYTES` on purpose: this is the record shape a
/// `SharedVec` slot cannot hold.
#[derive(Clone, Copy)]
#[repr(C)]
struct Record {
    id: u64,
    payload: [u8; 160],
}

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-slab-{name}-{pid}.bin"));
    p
}

fn record(id: u64) -> Record {
    Record { id, payload: [7u8; 160] }
}

fn get_observer(c: &mut Criterion) {
    let p = tmp("get");
    let slab: SharedSlab<Record> = SharedSlab::create(&p, CAPACITY).unwrap();
    slab.set(PROBE, record(PROBE as u64)).unwrap();
    c.bench_function("slab.get/mmf_seqlock", |b| {
        b.iter(|| black_box(slab.get(black_box(PROBE))));
    });
    drop(slab);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<Record>> = Mutex::new(vec![record(0); CAPACITY]);
    c.bench_function("slab.get/mutex_vec", |b| {
        b.iter(|| {
            let g = mv.lock().unwrap();
            black_box(g[black_box(PROBE)])
        });
    });

    let rw: RwLock<Vec<Record>> = RwLock::new(vec![record(0); CAPACITY]);
    c.bench_function("slab.get/rwlock_vec", |b| {
        b.iter(|| {
            let g = rw.read().unwrap();
            black_box(g[black_box(PROBE)])
        });
    });
}

fn set_writer(c: &mut Criterion) {
    let p = tmp("set");
    let slab: SharedSlab<Record> = SharedSlab::create(&p, CAPACITY).unwrap();
    c.bench_function("slab.set/mmf_seqlock", |b| {
        b.iter(|| slab.set(black_box(PROBE), black_box(record(1))).unwrap());
    });
    drop(slab);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<Record>> = Mutex::new(vec![record(0); CAPACITY]);
    c.bench_function("slab.set/mutex_vec", |b| {
        b.iter(|| {
            let mut g = mv.lock().unwrap();
            g[black_box(PROBE)] = black_box(record(1));
        });
    });

    let rw: RwLock<Vec<Record>> = RwLock::new(vec![record(0); CAPACITY]);
    c.bench_function("slab.set/rwlock_vec", |b| {
        b.iter(|| {
            let mut g = rw.write().unwrap();
            g[black_box(PROBE)] = black_box(record(1));
        });
    });
}

/// Scattered access, which is what a caller-indexed store actually does:
/// ids come from a log or a snapshot, not from a sweep. The stride is
/// coprime with the capacity so it visits every slot without repeating.
fn scattered(c: &mut Criterion) {
    let p = tmp("scatter");
    let slab: SharedSlab<Record> = SharedSlab::create(&p, CAPACITY).unwrap();
    for i in (0..CAPACITY).step_by(7) {
        slab.set(i, record(i as u64)).unwrap();
    }
    let mut i = 0usize;
    c.bench_function("slab.scattered_get/mmf_seqlock", |b| {
        b.iter(|| {
            i = (i + 4_099) % CAPACITY;
            black_box(slab.get(i))
        });
    });
    drop(slab);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<Record>> = Mutex::new(vec![record(0); CAPACITY]);
    let mut j = 0usize;
    c.bench_function("slab.scattered_get/mutex_vec", |b| {
        b.iter(|| {
            j = (j + 4_099) % CAPACITY;
            let g = mv.lock().unwrap();
            black_box(g[j])
        });
    });
}

criterion_group!(benches, get_observer, set_writer, scattered);
criterion_main!(benches);
