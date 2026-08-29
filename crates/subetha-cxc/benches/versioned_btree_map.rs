//! Bench: `VersionedBTreeMap` vs the `SharedBTreeMap` it composes.
//!
//! Architectural claim: a scan reads a fixed view while writers keep
//! running. The contender is the same tree without versioning, so the
//! numbers say exactly what the snapshot costs and nothing else.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. Both arms are the same B-tree at the same node capacity. The
//!    versioned arm's value is `Versioned<u64>` where the plain arm's
//!    is `u64`, which is the difference under test - a wider node, an
//!    epoch advance per write, and a visibility filter per read.
//! 2. Neither arm carries surplus work: both are created once outside
//!    the loop, pre-populated identically, and neither allocates
//!    inside a measured iteration.
//! 3. The range arms walk the same key span at the same limit. The
//!    versioned range is measured on a tree with NO tombstones and
//!    again with a quarter of its entries superseded, because the
//!    filter is free when there is nothing to filter and measuring
//!    only the clean case would hide it.
//!
//! What the numbers do NOT show: the plain tree has no answer to a
//! scan that must not see a concurrent write. It is not a slower way
//! of doing the same thing; it does a different thing.

use std::hint::black_box;
use std::ops::Bound;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedBTreeMap, VersionedBTreeMap};

const NODES: usize = 1 << 14;
const ENTRIES: u64 = 4_000;

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join(format!("subetha-bench-vbt-{name}-{}", std::process::id()));
    std::fs::remove_dir_all(&d).ok();
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn versioned(name: &str) -> (std::path::PathBuf, VersionedBTreeMap<u64, u64>) {
    let d = dir(name);
    let m = VersionedBTreeMap::create(d.join("t.bin"), NODES, d.join("e.bin"), 16).unwrap();
    (d, m)
}

fn plain(name: &str) -> (std::path::PathBuf, SharedBTreeMap<u64, u64>) {
    let d = dir(name);
    let m = SharedBTreeMap::create(d.join("t.bin"), NODES).unwrap();
    (d, m)
}

fn insert(c: &mut Criterion) {
    let (d, m) = versioned("insert");
    let mut k = 0u64;
    c.bench_function("vbtree.insert/versioned", |b| {
        b.iter(|| {
            k = (k + 1) % ENTRIES;
            m.insert(black_box(k), black_box(k)).unwrap();
        });
    });
    drop(m);
    std::fs::remove_dir_all(&d).ok();

    let (d, p) = plain("insert");
    let mut k = 0u64;
    c.bench_function("vbtree.insert/plain", |b| {
        b.iter(|| {
            k = (k + 1) % ENTRIES;
            p.insert(black_box(k), black_box(k)).unwrap();
        });
    });
    drop(p);
    std::fs::remove_dir_all(&d).ok();
}

fn get(c: &mut Criterion) {
    let (d, m) = versioned("get");
    for i in 0..ENTRIES {
        m.insert(i, i).unwrap();
    }
    c.bench_function("vbtree.get/versioned", |b| {
        b.iter(|| black_box(m.get(&black_box(ENTRIES / 2))));
    });
    let pin = m.pin().unwrap();
    c.bench_function("vbtree.get_at/versioned_pinned", |b| {
        b.iter(|| black_box(m.get_at(&black_box(ENTRIES / 2), &pin)));
    });
    drop(pin);
    drop(m);
    std::fs::remove_dir_all(&d).ok();

    let (d, p) = plain("get");
    for i in 0..ENTRIES {
        p.insert(i, i).unwrap();
    }
    c.bench_function("vbtree.get/plain", |b| {
        b.iter(|| black_box(p.get(&black_box(ENTRIES / 2))));
    });
    drop(p);
    std::fs::remove_dir_all(&d).ok();
}

/// A range over the whole key space, clean and then with a quarter of
/// the entries superseded so the visibility filter has work to do.
fn range(c: &mut Criterion) {
    let (d, m) = versioned("range");
    for i in 0..ENTRIES {
        m.insert(i, i).unwrap();
    }
    let pin = m.pin().unwrap();
    c.bench_function("vbtree.range/versioned_no_tombstones", |b| {
        b.iter(|| {
            black_box(m.range_at(Bound::Unbounded, Bound::Unbounded, 1024, &pin).len())
        });
    });
    drop(pin);
    for i in (0..ENTRIES).step_by(4) {
        m.remove(&i).unwrap();
    }
    let pin = m.pin().unwrap();
    c.bench_function("vbtree.range/versioned_quarter_tombstoned", |b| {
        b.iter(|| {
            black_box(m.range_at(Bound::Unbounded, Bound::Unbounded, 1024, &pin).len())
        });
    });
    drop(pin);
    drop(m);
    std::fs::remove_dir_all(&d).ok();

    let (d, p) = plain("range");
    for i in 0..ENTRIES {
        p.insert(i, i).unwrap();
    }
    c.bench_function("vbtree.range/plain", |b| {
        b.iter(|| black_box(p.range(Bound::Unbounded, Bound::Unbounded, 1024).len()));
    });
    drop(p);
    std::fs::remove_dir_all(&d).ok();
}

criterion_group!(benches, insert, get, range);
criterion_main!(benches);
