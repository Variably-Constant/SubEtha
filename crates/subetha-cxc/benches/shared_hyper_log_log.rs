//! Bench: SharedHyperLogLog vs Mutex<HashSet<Vec<u8>>>.
//!
//! Architectural claim: HLL trades exactness for fixed-size memory
//! (constant 2^p bytes regardless of cardinality) AND ~1 atomic op
//! per insert. HashSet pays unbounded memory growth + lock + hash
//! + bucket lookup per insert.

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedHyperLogLog;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-hll-{name}-{pid}.bin"));
    p
}

fn insert_single(c: &mut Criterion) {
    // Pre-built key cycle so neither contender pays format!()+alloc
    // inside b.iter. HashSet pre-populated so re-inserts hit the
    // idempotent already-present path, matching HLL's idempotent
    // fetch_max semantics.
    const KEY_CYCLE: usize = 64;
    let keys: Vec<Vec<u8>> = (0..KEY_CYCLE)
        .map(|i| format!("item-{i:04}").into_bytes()).collect();

    let p = tmp("ins");
    let h = SharedHyperLogLog::create(&p, 12).unwrap();
    let mut i = 0usize;
    c.bench_function("hll.insert/mmf", |b| {
        b.iter(|| {
            i = (i + 1) % KEY_CYCLE;
            h.insert(black_box(&keys[i]))
        });
    });
    drop(h);
    std::fs::remove_file(&p).ok();

    let s: Mutex<HashSet<Vec<u8>>> = Mutex::new({
        let mut s = HashSet::with_capacity(KEY_CYCLE);
        for k in &keys { s.insert(k.clone()); }
        s
    });
    let mut i = 0usize;
    c.bench_function("hll.insert/mutex_hashset", |b| {
        b.iter(|| {
            i = (i + 1) % KEY_CYCLE;
            s.lock().unwrap().insert(keys[i].clone())
        });
    });
}

fn estimate_after_1000(c: &mut Criterion) {
    let p = tmp("est");
    let h = SharedHyperLogLog::create(&p, 12).unwrap();
    for i in 0..1000u32 { h.insert(format!("k{i:05}").as_bytes()); }
    c.bench_function("hll.estimate/mmf", |b| {
        b.iter(|| black_box(h.estimate()));
    });
    drop(h);
    std::fs::remove_file(&p).ok();

    let s: Mutex<HashSet<Vec<u8>>> = Mutex::new({
        let mut s = HashSet::new();
        for i in 0..1000u32 { s.insert(format!("k{i:05}").into_bytes()); }
        s
    });
    c.bench_function("hll.estimate/mutex_hashset_len", |b| {
        b.iter(|| black_box(s.lock().unwrap().len()));
    });
}

fn storage_witness(c: &mut Criterion) {
    let hll_p12_bytes = 64 + (1usize << 12);
    let hashset_avg_bytes_per_entry = std::mem::size_of::<Vec<u8>>() + 8;
    let hashset_at_1k = hashset_avg_bytes_per_entry * 1000;
    let hashset_at_1m = hashset_avg_bytes_per_entry * 1_000_000;
    eprintln!("[storage] HLL p=12 (any cardinality)   = {hll_p12_bytes} bytes");
    eprintln!("[storage] HashSet at 1K items          ~= {hashset_at_1k} bytes ({}x)",
        hashset_at_1k as f64 / hll_p12_bytes as f64);
    eprintln!("[storage] HashSet at 1M items          ~= {hashset_at_1m} bytes ({}x)",
        hashset_at_1m as f64 / hll_p12_bytes as f64);
    c.bench_function("hll.storage_witness", |b| {
        b.iter(|| black_box(hll_p12_bytes + hashset_at_1k + hashset_at_1m));
    });
}

criterion_group!(benches,
    insert_single,
    estimate_after_1000,
    storage_witness,
);
criterion_main!(benches);
