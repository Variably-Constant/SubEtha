//! Bench: SharedBloomFilter vs Mutex<HashSet<Vec<u8>>>.
//!
//! Architectural claim: at the cost of a tunable false-positive
//! rate, SharedBloomFilter trades exactness for huge memory
//! reduction AND constant-time membership checks regardless of
//! set size. The HashSet baseline grows allocation per item and
//! pays a hash+lock+lookup per query.
//!
//! Workloads:
//! - insert single item
//! - contains hot (membership query)
//! - 1000 inserts batch
//! - 1000 contains batch
//! - storage density comparison

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedBloomFilter;

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-bloom-{name}-{pid}"));
    p
}

fn cleanup_base(base: &std::path::Path) {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    for ext in ["bloom", "bits"] {
        p.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&p).ok();
    }
}

// =========================================================
// insert single
// =========================================================

fn insert_single(c: &mut Criterion) {
    // Pre-built key cycle so neither contender pays format!()+alloc
    // inside b.iter. Both primitives now measure pure hash+insert.
    const KEY_CYCLE: usize = 64;
    let keys: Vec<Vec<u8>> = (0..KEY_CYCLE)
        .map(|i| format!("item-{i:04}").into_bytes()).collect();

    let base = tmp_base("ins");
    let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(10_000, 0.01);
    let b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
    let mut i = 0usize;
    c.bench_function("bloom.insert/mmf", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) % KEY_CYCLE;
            b.insert(black_box(&keys[i])).unwrap()
        });
    });
    drop(b);
    cleanup_base(&base);

    // Pre-fill the HashSet so re-inserts hit the idempotent
    // already-present path. Mirrors the bloom which re-sets
    // already-set bits on the cycle: both measure hash+lookup.
    let h: Mutex<HashSet<Vec<u8>>> = Mutex::new({
        let mut s = HashSet::with_capacity(KEY_CYCLE);
        for k in &keys { s.insert(k.clone()); }
        s
    });
    let mut i = 0usize;
    c.bench_function("bloom.insert/mutex_hashset", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) % KEY_CYCLE;
            h.lock().unwrap().insert(keys[i].clone())
        });
    });
}

// =========================================================
// contains hot
// =========================================================

fn contains_hot(c: &mut Criterion) {
    let base = tmp_base("get");
    let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(1_000, 0.01);
    let b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
    let item = b"hello-world";
    b.insert(item).unwrap();
    c.bench_function("bloom.contains_hit/mmf", |b_iter| {
        b_iter.iter(|| black_box(b.contains(black_box(item)).unwrap()));
    });
    c.bench_function("bloom.contains_miss/mmf", |b_iter| {
        b_iter.iter(|| black_box(b.contains(black_box(b"not-there-at-all")).unwrap()));
    });
    drop(b);
    cleanup_base(&base);

    let item_vec = item.to_vec();
    let h: Mutex<HashSet<Vec<u8>>> = Mutex::new({
        let mut s = HashSet::new();
        s.insert(item_vec.clone());
        s
    });
    let item = &item_vec;
    c.bench_function("bloom.contains_hit/mutex_hashset", |b_iter| {
        b_iter.iter(|| {
            let g = h.lock().unwrap();
            black_box(g.contains(item))
        });
    });
    c.bench_function("bloom.contains_miss/mutex_hashset", |b_iter| {
        b_iter.iter(|| {
            let g = h.lock().unwrap();
            black_box(g.contains(b"not-there-at-all".as_slice()))
        });
    });
}

// =========================================================
// Batch insert 1000 + batch contains 1000
// =========================================================

fn batch_workload(c: &mut Criterion) {
    const N: u32 = 1000;

    c.bench_function("bloom.batch_insert_1000/mmf", |b_iter| {
        let base = tmp_base("batch-ins");
        let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(N as usize, 0.01);
        let b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
        b_iter.iter(|| {
            b.clear();
            for i in 0..N {
                b.insert(format!("item-{i:04}").as_bytes()).unwrap();
            }
        });
        drop(b);
        cleanup_base(&base);
    });

    c.bench_function("bloom.batch_insert_1000/mutex_hashset", |b_iter| {
        let h: Mutex<HashSet<Vec<u8>>> = Mutex::new(HashSet::new());
        b_iter.iter(|| {
            h.lock().unwrap().clear();
            for i in 0..N {
                h.lock().unwrap().insert(format!("item-{i:04}").into_bytes());
            }
        });
    });

    c.bench_function("bloom.batch_contains_1000/mmf", |b_iter| {
        let base = tmp_base("batch-cont");
        let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(N as usize, 0.01);
        let b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
        for i in 0..N { b.insert(format!("item-{i:04}").as_bytes()).unwrap(); }
        b_iter.iter(|| {
            for i in 0..N {
                black_box(b.contains(format!("item-{i:04}").as_bytes()).unwrap());
            }
        });
        drop(b);
        cleanup_base(&base);
    });

    c.bench_function("bloom.batch_contains_1000/mutex_hashset", |b_iter| {
        let h: Mutex<HashSet<Vec<u8>>> = Mutex::new({
            let mut s = HashSet::new();
            for i in 0..N { s.insert(format!("item-{i:04}").into_bytes()); }
            s
        });
        b_iter.iter(|| {
            for i in 0..N {
                let g = h.lock().unwrap();
                black_box(g.contains(&format!("item-{i:04}").into_bytes()));
            }
        });
    });
}

// =========================================================
// Storage density witness
// =========================================================

fn storage_density(c: &mut Criterion) {
    let n_items = 10_000usize;
    let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(n_items, 0.01);
    let bloom_bytes = n_bits.div_ceil(8);
    // HashSet<Vec<u8>>: per-Vec is ~24 bytes (ptr, len, cap) + the
    // actual byte content. Assume 16-byte items.
    let item_size = 16;
    let hashset_bytes_per = std::mem::size_of::<Vec<u8>>() + item_size;
    let hashset_total = hashset_bytes_per * n_items;
    eprintln!("[storage] SharedBloomFilter(n={n_items}, FPR=0.01) = {bloom_bytes} bytes ({n_bits} bits, {n_hashes} hashes)");
    eprintln!("[storage] HashSet<Vec<u8>>(n={n_items}, 16-byte items) ~= {hashset_total} bytes ({}x larger)",
        hashset_total as f64 / bloom_bytes as f64);
    c.bench_function("bloom.storage_witness", |b_iter| {
        b_iter.iter(|| black_box(bloom_bytes + hashset_total));
    });
}

criterion_group!(benches,
    insert_single,
    contains_hot,
    batch_workload,
    storage_density,
);
criterion_main!(benches);
