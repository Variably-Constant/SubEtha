//! Fair A/B: standard vs cache-blocked Bloom filter.
//!
//!     cargo run --release --example blocked_bloom_compare -- <n_items> <n_queries>
//!
//! Both filters are built at the SAME (n_bits, k) from the standard
//! formula, sized large enough to exceed L3 so the cache behavior is what
//! is measured (a standard filter touches k scattered cache lines per op;
//! the blocked filter touches one). Reports insert + query throughput and
//! the achieved false-positive rate for each, so the speed win is weighed
//! against the FPR trade.

use std::hint::black_box;
use std::time::Instant;
use subetha_cxc::shared_blocked_bloom_filter::SharedBlockedBloomFilter;
use subetha_cxc::shared_bloom_filter::SharedBloomFilter;

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(20_000_000);
    let q: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5_000_000);

    // Each filter sized for the SAME target FPR (1%) by its own formula:
    // the blocked filter spends ~2x the bits to offset per-block variance.
    // So this measures the speed win at equal FPR, and the memory it costs.
    let (std_bits, std_k) = SharedBloomFilter::suggest_config(n, 0.01);
    let (blk_bits, blk_k) = SharedBlockedBloomFilter::suggest_config(n, 0.01);
    eprintln!(
        "config: {n} items, q={q}; standard {} MiB (k={std_k}), blocked {} MiB (k={blk_k})",
        std_bits / 8 / (1024 * 1024),
        blk_bits / 8 / (1024 * 1024),
    );

    let mut tmp = std::env::temp_dir();
    tmp.push(format!("bbf_std_{}.bin", std::process::id()));
    let mut tmp_b = std::env::temp_dir();
    tmp_b.push(format!("bbf_blk_{}.bin", std::process::id()));

    let std_bf = SharedBloomFilter::create(&tmp, std_bits, std_k).unwrap();
    let blk_bf = SharedBlockedBloomFilter::create(&tmp_b, blk_bits, blk_k).unwrap();

    // --- insert n items into each (same keys) ---
    let t = Instant::now();
    for i in 0..n as u64 {
        std_bf.insert(black_box(&i.to_le_bytes())).unwrap();
    }
    let std_ins = t.elapsed().as_nanos() as f64 / n as f64;
    let t = Instant::now();
    for i in 0..n as u64 {
        blk_bf.insert(black_box(&i.to_le_bytes()));
    }
    let blk_ins = t.elapsed().as_nanos() as f64 / n as f64;

    // --- random queries (cache-cold: keys jump across the whole filter) ---
    let mut s = 0x1234_5678_9abc_def1u64;
    let mut hits = 0u64;
    let t = Instant::now();
    for _ in 0..q {
        let key = (xorshift(&mut s) % n as u64).to_le_bytes();
        if std_bf.contains(black_box(&key)).unwrap() {
            hits += 1;
        }
    }
    let std_q = t.elapsed().as_nanos() as f64 / q as f64;
    black_box(hits);

    let mut s = 0x1234_5678_9abc_def1u64;
    let mut hits = 0u64;
    let t = Instant::now();
    for _ in 0..q {
        let key = (xorshift(&mut s) % n as u64).to_le_bytes();
        if blk_bf.contains(black_box(&key)) {
            hits += 1;
        }
    }
    let blk_q = t.elapsed().as_nanos() as f64 / q as f64;
    black_box(hits);

    // --- false-positive rate (query absent keys) ---
    let measure_fpr = |present: &dyn Fn(&[u8]) -> bool| -> f64 {
        let mut fp = 0u64;
        let trials = 2_000_000u64;
        for i in 0..trials {
            let key = (10_000_000_000u64 + i).to_le_bytes();
            if present(&key) {
                fp += 1;
            }
        }
        fp as f64 / trials as f64
    };
    let std_fpr = measure_fpr(&|k| std_bf.contains(k).unwrap());
    let blk_fpr = measure_fpr(&|k| blk_bf.contains(k));

    eprintln!("              insert ns/op   query ns/op    FPR");
    eprintln!("  standard:   {std_ins:>10.1}   {std_q:>10.1}   {std_fpr:.4}");
    eprintln!("  blocked:    {blk_ins:>10.1}   {blk_q:>10.1}   {blk_fpr:.4}");
    eprintln!(
        "  speedup:    insert {:.2}x   query {:.2}x   (FPR {:.2}x)",
        std_ins / blk_ins,
        std_q / blk_q,
        blk_fpr / std_fpr.max(1e-9),
    );

    std::fs::remove_file(&tmp).ok();
    std::fs::remove_file(&tmp_b).ok();
    // standard bloom also wrote a sibling .bits file.
    std::fs::remove_file(format!("{}.bits.bin", tmp.display())).ok();
}
