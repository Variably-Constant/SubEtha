//! Bench: SharedBlockedBloomFilter vs the standard SharedBloomFilter.
//!
//! Architectural claim: a blocked Bloom filter packs every probe for one
//! item into a single 512-bit (one cache-line) block, so `contains` touches
//! ONE line regardless of `n_hashes`. The standard filter scatters its
//! `n_hashes` probes across the whole bit array - up to `n_hashes` separate
//! cache lines per query. That difference is invisible while the whole
//! filter fits in cache (every line is warm); it pays off at the blocked
//! filter's design point - a large-scale membership filter that exceeds L2,
//! where the standard filter's scattered probes miss cache `n_hashes` times
//! per query and the blocked filter misses at most once. Both are
//! false-negative-free; the contender measured is the per-query cache-miss
//! count. The blocked variant trades a small bit-budget margin (1.15x) for
//! that locality.
//!
//! Workloads (blocked vs standard, identical suggested config):
//! - insert single item (small filter; both warm - measures hash+set)
//! - contains over a 4M-item filter with cache-cold spread queries
//!   (the locality test)

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedBlockedBloomFilter, SharedBloomFilter};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-bblock-{name}-{pid}.bin"));
    p
}

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-bstd-{name}-{pid}"));
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

// Deterministic key spread (LCG) so consecutive queries land in different
// regions of the (large) filter and their probe lines stay cache-cold.
// Even results map to a present key (< n_items), odd to an absent key.
#[inline]
fn next_query(state: &mut u32, n_items: u32) -> [u8; 4] {
    *state = state.wrapping_mul(2_654_435_761).wrapping_add(1);
    let idx = *state % n_items; // all-hits: isolates the locality cost (no early-bail)
    idx.to_le_bytes()
}

// =========================================================
// insert single (small filter)
// =========================================================

fn insert_single(c: &mut Criterion) {
    const KEY_CYCLE: usize = 64;
    let keys: Vec<Vec<u8>> = (0..KEY_CYCLE)
        .map(|i| format!("item-{i:04}").into_bytes()).collect();
    let (n_bits, n_hashes) = SharedBlockedBloomFilter::suggest_config(10_000, 0.01);

    let path = tmp_path("ins");
    let bb = SharedBlockedBloomFilter::create(&path, n_bits, n_hashes).unwrap();
    let mut i = 0usize;
    c.bench_function("blocked.insert/mmf", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) % KEY_CYCLE;
            bb.insert(black_box(&keys[i]));
        });
    });
    drop(bb);
    std::fs::remove_file(&path).ok();

    let base = tmp_base("ins");
    let std_b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
    let mut i = 0usize;
    c.bench_function("blocked.insert/standard_bloom", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) % KEY_CYCLE;
            std_b.insert(black_box(&keys[i])).unwrap();
        });
    });
    drop(std_b);
    cleanup_base(&base);
}

// =========================================================
// contains at scale: 4M-item filter, cache-cold spread queries
// =========================================================

fn contains_scale(c: &mut Criterion) {
    const N_ITEMS: u32 = 16_000_000; // filter ~19 MB at FPR 0.01 - exceeds L3
    let (n_bits, n_hashes) = SharedBlockedBloomFilter::suggest_config(N_ITEMS as usize, 0.01);

    let path = tmp_path("scale");
    let bb = SharedBlockedBloomFilter::create(&path, n_bits, n_hashes).unwrap();
    for i in 0..N_ITEMS { bb.insert(&i.to_le_bytes()); }
    let mut st = 0x1357_9bdfu32;
    c.bench_function("blocked.contains_scale_16m/mmf", |b_iter| {
        b_iter.iter(|| {
            let key = next_query(&mut st, N_ITEMS);
            black_box(bb.contains(black_box(&key)))
        });
    });
    drop(bb);
    std::fs::remove_file(&path).ok();

    let base = tmp_base("scale");
    let std_b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
    for i in 0..N_ITEMS { std_b.insert(&i.to_le_bytes()).unwrap(); }
    let mut st = 0x1357_9bdfu32;
    c.bench_function("blocked.contains_scale_16m/standard_bloom", |b_iter| {
        b_iter.iter(|| {
            let key = next_query(&mut st, N_ITEMS);
            black_box(std_b.contains(black_box(&key)).unwrap())
        });
    });
    drop(std_b);
    cleanup_base(&base);
}

// =========================================================
// memory witness: blocked margin vs standard
// =========================================================

fn storage_density(c: &mut Criterion) {
    let n_items = 4_000_000usize;
    let (blk_bits, _) = SharedBlockedBloomFilter::suggest_config(n_items, 0.01);
    let (std_bits, _) = SharedBloomFilter::suggest_config(n_items, 0.01);
    let blk_bytes = blk_bits.div_ceil(8);
    let std_bytes = std_bits.div_ceil(8);
    eprintln!("[storage] blocked(n={n_items}, FPR=0.01) = {blk_bytes} bytes; standard = {std_bytes} bytes ({:.2}x the bits for one-line locality)",
        blk_bits as f64 / std_bits as f64);
    c.bench_function("blocked.storage_witness", |b_iter| {
        b_iter.iter(|| black_box(blk_bytes + std_bytes));
    });
}

criterion_group!(benches,
    insert_single,
    contains_scale,
    storage_density,
);
criterion_main!(benches);
