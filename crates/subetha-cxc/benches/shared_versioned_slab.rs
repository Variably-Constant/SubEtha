//! Bench: `SharedVersionedSlab<T, D>` vs the `SharedSlab<T>` it composes.
//!
//! Architectural claim: a pinned scan reads the record version it
//! pinned while the writer keeps overwriting the slot. The contender is
//! the same slab holding the bare record, so the numbers say exactly
//! what the version chain costs and nothing else.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. Both arms are the same slab at the same capacity holding the same
//!    168-byte record. The versioned arm's slot is a chain of four
//!    stamped copies where the plain arm's is one record, which is the
//!    difference under test - a wider slot, an epoch advance per write,
//!    and a visibility walk per read.
//! 2. Neither arm carries surplus work: both are created once outside
//!    the loop, and neither allocates inside a measured iteration.
//! 3. The versioned write is measured on a chain with room and again on
//!    a chain that is full and must sweep before every push, because a
//!    write that never sweeps would hide what the bound costs.
//!
//! What the numbers do NOT show: the plain slab has no answer to a
//! scan that must not see a concurrent overwrite. It is not a slower
//! way of doing the same thing; it does a different thing.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedSlab, SharedVersionedSlab};

const CAPACITY: usize = 65_536;
const PROBE: usize = 4_211;
const DEPTH: usize = 4;

/// Past `VEC_PAYLOAD_BYTES` and without `Default`, which the slab does
/// not ask for.
#[derive(Clone, Copy)]
#[repr(C)]
struct Record {
    id: u64,
    payload: [u8; 160],
}

fn record(id: u64) -> Record {
    Record { id, payload: [7u8; 160] }
}

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join(format!("subetha-bench-vslab-{name}-{}", std::process::id()));
    match std::fs::remove_dir_all(&d) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("stale bench directory {} not removed: {e}", d.display()),
    }
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Remove an arm's directory once its slab is dropped; a refusal is a bench
/// bug (a mapping still alive), not something to leave behind quietly.
fn remove(d: &std::path::Path) {
    std::fs::remove_dir_all(d).expect("the arm's slab is dropped and its directory removable");
}

fn versioned(name: &str) -> (std::path::PathBuf, SharedVersionedSlab<Record, DEPTH>) {
    let d = dir(name);
    let s = SharedVersionedSlab::create(d.join("s.bin"), CAPACITY, d.join("e.bin"), 16).unwrap();
    (d, s)
}

fn plain(name: &str) -> (std::path::PathBuf, SharedSlab<Record>) {
    let d = dir(name);
    let s = SharedSlab::create(d.join("s.bin"), CAPACITY).unwrap();
    (d, s)
}

fn set(c: &mut Criterion) {
    // Every push after the first three lands on a full chain and sweeps
    // the versions no pin can reach, which with no pin held is all of
    // them but the head.
    let (d, s) = versioned("set");
    let mut n = 0u64;
    c.bench_function("vslab.set/versioned_full_chain_sweeps", |b| {
        b.iter(|| {
            n += 1;
            s.set(black_box(PROBE), black_box(record(n))).unwrap();
        });
    });
    drop(s);
    remove(&d);

    // The unmeasured setup sweeps the slot, so every measured push
    // finds room and pays no sweep.
    let (d, s) = versioned("set_room");
    let mut n = 0u64;
    c.bench_function("vslab.set/versioned_with_room", |b| {
        b.iter_batched(
            || {
                s.sweep_slot(PROBE).unwrap();
            },
            |()| {
                n += 1;
                s.set(black_box(PROBE), black_box(record(n))).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(s);
    remove(&d);

    let (d, p) = plain("set");
    let mut n = 0u64;
    c.bench_function("vslab.set/plain", |b| {
        b.iter(|| {
            n += 1;
            p.set(black_box(PROBE), black_box(record(n))).unwrap();
        });
    });
    drop(p);
    remove(&d);
}

fn get(c: &mut Criterion) {
    let (d, s) = versioned("get");
    s.set(PROBE, record(1)).unwrap();
    let pin = s.pin().unwrap();
    for n in 2..=(DEPTH as u64) {
        s.set(PROBE, record(n)).unwrap();
    }
    c.bench_function("vslab.get/versioned_head_of_full_chain", |b| {
        b.iter(|| black_box(s.get(black_box(PROBE)).unwrap().map(|r| r.id)));
    });
    c.bench_function("vslab.get_at/versioned_oldest_of_full_chain", |b| {
        b.iter(|| black_box(s.get_at(black_box(PROBE), &pin).unwrap().map(|r| r.id)));
    });
    // A pin taken after the chain was built sees the head, so the walk
    // stops at the first version: the other end of the same read.
    let head_pin = s.pin().unwrap();
    c.bench_function("vslab.get_at/versioned_head_of_full_chain", |b| {
        b.iter(|| black_box(s.get_at(black_box(PROBE), &head_pin).unwrap().map(|r| r.id)));
    });
    drop(head_pin);
    drop(pin);
    drop(s);
    remove(&d);

    let (d, p) = plain("get");
    p.set(PROBE, record(1)).unwrap();
    c.bench_function("vslab.get/plain", |b| {
        b.iter(|| black_box(p.get(black_box(PROBE)).unwrap().id));
    });
    drop(p);
    remove(&d);
}

criterion_group!(benches, set, get);
criterion_main!(benches);
