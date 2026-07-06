//! Bench: SharedBTreeMap vs Mutex<BTreeMap>.
//!
//! Architectural claim: SharedBTreeMap is the substrate's ordered-map
//! primitive - a cross-process, lock-free-read sorted key/value map that an
//! in-process `BTreeMap` cannot be. Each node packs up to B=15 sorted keys
//! in a contiguous array (fanout 16), so a lookup touches ~log_16(N) nodes
//! and each node's binary search reads one prefetcher-friendly key run
//! rather than chasing scattered single-cache-line nodes. The textbook
//! `Mutex<BTreeMap>` contender measures the same operations; the mmf adds
//! cross-process visibility, lock-free reads, and disk persistence.
//!
//! Workloads:
//! - get_hit / get_miss (100 keys, warm: matches small-map use)
//! - iter_ascending (100 elements)
//! - get_hit_100k (large map, pseudo-random keys: exposes cache locality)

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedBTreeMap;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-btree-{name}-{pid}.bin"));
    p
}

// Deterministic key spread (LCG) so large-map lookups don't all hit the
// same hot nodes; keeps the bench reproducible (no RNG).
#[inline]
fn next_key(state: &mut u32, modulo: u32) -> u32 {
    *state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
    (*state >> 8) % modulo
}

// =========================================================
// get_hit / get_miss at 100 keys (warm)
// =========================================================

fn get_small(c: &mut Criterion) {
    let path = tmp_path("get-small");
    let bt: SharedBTreeMap<u32, u32> = SharedBTreeMap::create(&path, 256).unwrap();
    for k in 0..100u32 { bt.insert(k, k.wrapping_mul(10)).unwrap(); }
    c.bench_function("btree.get_hit/mmf", |b| {
        b.iter(|| black_box(bt.get(black_box(&50))));
    });
    c.bench_function("btree.get_miss/mmf", |b| {
        b.iter(|| black_box(bt.get(black_box(&9999))));
    });
    drop(bt);
    std::fs::remove_file(&path).ok();

    let m: Mutex<BTreeMap<u32, u32>> =
        Mutex::new((0..100u32).map(|k| (k, k.wrapping_mul(10))).collect());
    c.bench_function("btree.get_hit/mutex_btreemap", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(g.get(black_box(&50)).copied())
        });
    });
    c.bench_function("btree.get_miss/mutex_btreemap", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(g.get(black_box(&9999)).copied())
        });
    });
}

// =========================================================
// iter_ascending (100 elements)
// =========================================================

fn iter_small(c: &mut Criterion) {
    let path = tmp_path("iter");
    let bt: SharedBTreeMap<u32, u32> = SharedBTreeMap::create(&path, 256).unwrap();
    for k in 0..100u32 { bt.insert(k, k).unwrap(); }
    c.bench_function("btree.iter_ascending/mmf", |b| {
        b.iter(|| black_box(bt.iter_ascending()));
    });
    drop(bt);
    std::fs::remove_file(&path).ok();

    let m: Mutex<BTreeMap<u32, u32>> =
        Mutex::new((0..100u32).map(|k| (k, k)).collect());
    c.bench_function("btree.iter_ascending/mutex_btreemap", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(g.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>())
        });
    });
}

// =========================================================
// get_hit at 100k keys (large; pseudo-random lookups)
// =========================================================

fn get_large(c: &mut Criterion) {
    const N: u32 = 100_000;
    let path = tmp_path("get-large");
    // ~N/4 nodes is ample (each node holds up to 15 keys); round up.
    let bt: SharedBTreeMap<u32, u32> =
        SharedBTreeMap::create(&path, (N / 3) as usize).unwrap();
    for k in 0..N { bt.insert(k, k).unwrap(); }
    let mut st = 0x1234_5678u32;
    c.bench_function("btree.get_hit_100k/mmf", |b| {
        b.iter(|| {
            let key = next_key(&mut st, N);
            black_box(bt.get(black_box(&key)))
        });
    });
    drop(bt);
    std::fs::remove_file(&path).ok();

    let m: Mutex<BTreeMap<u32, u32>> =
        Mutex::new((0..N).map(|k| (k, k)).collect());
    let mut st = 0x1234_5678u32;
    c.bench_function("btree.get_hit_100k/mutex_btreemap", |b| {
        b.iter(|| {
            let key = next_key(&mut st, N);
            let g = m.lock().unwrap();
            black_box(g.get(black_box(&key)).copied())
        });
    });
}

criterion_group!(benches,
    get_small,
    iter_small,
    get_large,
);
criterion_main!(benches);
