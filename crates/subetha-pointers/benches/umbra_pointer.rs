//! Bench: `UmbraPointer<T>` content-prefix skip-on-mismatch vs naive
//! `Arc<T>` dereference. Workload: 1024 candidate pointers, scan for
//! a target value via equality. The prefix-mismatch case is the
//! common path in HashMap bucket chains and dedup scans.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_pointers::umbra_pointer::{ArcUmbra, UmbraPointer};

const N: usize = 1024;

fn build_arcs() -> (Vec<Arc<u64>>, u64) {
    let arcs: Vec<Arc<u64>> = (0..N as u64).map(Arc::new).collect();
    let target = arcs[N - 1].clone();
    (arcs, *target)
}

fn build_umbras() -> (Vec<ArcUmbra<u64>>, u32, u64) {
    let mut umbras = Vec::with_capacity(N);
    let mut target_prefix = 0u32;
    let mut target_value = 0u64;
    for i in 0..N as u64 {
        let arc = Arc::new(i);
        // Use the low 32 bits of the value as the prefix - simulates
        // a content-derived prefix where most prefixes are distinct.
        let prefix = i as u32;
        let u = UmbraPointer::from_arc(arc, prefix);
        if (i as usize) == N - 1 {
            target_prefix = prefix;
            target_value = i;
        }
        umbras.push(u);
    }
    (umbras, target_prefix, target_value)
}

// =========================================================
// Scan for a value that exists at the END (late match).
// =========================================================

fn scan_late_match(c: &mut Criterion) {
    let (arcs, target) = build_arcs();
    c.bench_function("umbra_ptr.scan_late_match/native_arc_deref", |b| {
        b.iter(|| {
            let mut found: Option<u64> = None;
            for arc in arcs.iter() {
                // Forced deref every entry - no shortcut.
                if **arc == black_box(target) {
                    found = Some(**arc);
                    break;
                }
            }
            black_box(found)
        });
    });

    let (umbras, target_prefix, target_value) = build_umbras();
    c.bench_function("umbra_ptr.scan_late_match/umbra_prefix_shortcircuit", |b| {
        b.iter(|| {
            let mut found: Option<u64> = None;
            for u in umbras.iter() {
                if u.ptr().matches_prefix(black_box(target_prefix)) {
                    // Only deref when prefix matches.
                    if *u.value() == target_value {
                        found = Some(*u.value());
                        break;
                    }
                }
            }
            black_box(found)
        });
    });
}

// =========================================================
// Scan for a value that does NOT exist (full miss).
// =========================================================

fn scan_full_miss(c: &mut Criterion) {
    let (arcs, _) = build_arcs();
    let miss: u64 = N as u64 + 9999;
    c.bench_function("umbra_ptr.scan_full_miss/native_arc_deref", |b| {
        b.iter(|| {
            let mut found = false;
            for arc in arcs.iter() {
                if **arc == black_box(miss) { found = true; break; }
            }
            black_box(found)
        });
    });

    let (umbras, _, _) = build_umbras();
    let miss_prefix: u32 = N as u32 + 9999;
    c.bench_function("umbra_ptr.scan_full_miss/umbra_prefix_shortcircuit", |b| {
        b.iter(|| {
            let mut found = false;
            for u in umbras.iter() {
                if u.ptr().matches_prefix(black_box(miss_prefix)) {
                    // Never matches.
                    if *u.value() == miss { found = true; break; }
                }
            }
            black_box(found)
        });
    });
}

// =========================================================
// Cache discipline: how many cache lines are touched per scan?
// Native_arc must touch every Arc's heap allocation (N cache lines).
// Umbra touches only the contiguous Vec of UmbraPointer slots
// (N/4 cache lines at 16B per slot, one cache line = 4 slots).
// =========================================================

fn scan_cache_pressure(c: &mut Criterion) {
    // Build N umbras and N arcs but interleave the heap allocations
    // so each Arc's target lives on a separate cache line. Realistic
    // for typical small-heap-object workloads.
    let arcs: Vec<Arc<u64>> = (0..N as u64).map(Arc::new).collect();
    let miss: u64 = N as u64 + 9999;
    c.bench_function("umbra_ptr.scan_cache_pressure/native_arc_full_deref", |b| {
        b.iter(|| {
            let mut sum: u64 = 0;
            for arc in arcs.iter() {
                sum = sum.wrapping_add(**arc);
            }
            // miss never matches; force the loop to complete.
            black_box(sum.wrapping_add(miss))
        });
    });

    let umbras: Vec<ArcUmbra<u64>> = (0..N as u64).map(|i| {
        let arc = Arc::new(i);
        UmbraPointer::from_arc(arc, i as u32)
    }).collect();
    let miss_prefix = N as u32 + 9999;
    c.bench_function("umbra_ptr.scan_cache_pressure/umbra_prefix_only", |b| {
        b.iter(|| {
            let mut matches = 0u32;
            for u in umbras.iter() {
                if u.ptr().matches_prefix(black_box(miss_prefix)) {
                    matches += 1;
                }
            }
            black_box(matches)
        });
    });
}

// =========================================================
// Scattered large targets - the workload where Umbra's prefix
// shortcircuit actually wins. Each target is a full cache line,
// allocated separately, then the access order is shuffled so the
// prefetcher cannot pre-load.
// =========================================================

fn scan_scattered_cache_miss(c: &mut Criterion) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // 64-byte payload guarantees each deref touches a full cache line.
    #[derive(Clone)]
    #[allow(dead_code)]
    struct CacheLineBlob {
        marker: u64,
        pad: [u64; 7],
    }

    // Scatter allocation: interleave heap allocations of unrelated
    // sizes so consecutive Arc<CacheLineBlob>'s do not share cache lines.
    let mut arcs: Vec<Arc<CacheLineBlob>> = Vec::with_capacity(N);
    let mut _scatter: Vec<Box<[u8; 4096]>> = Vec::with_capacity(N);
    for i in 0..N as u64 {
        _scatter.push(Box::new([0u8; 4096]));
        arcs.push(Arc::new(CacheLineBlob { marker: i, pad: [0; 7] }));
    }
    // Build Umbras with hash-based prefix.
    let umbras: Vec<ArcUmbra<CacheLineBlob>> = arcs.iter().cloned().map(|a| {
        let mut h = DefaultHasher::new();
        a.marker.hash(&mut h);
        let prefix = h.finish() as u32;
        UmbraPointer::from_arc(a, prefix)
    }).collect();

    // Shuffle the access pattern so prefetching fails.
    let access_order: Vec<usize> = {
        let mut order: Vec<usize> = (0..N).collect();
        // Simple pseudo-shuffle via byte reversal of index.
        order.sort_by_key(|&i| (i as u32).reverse_bits());
        order
    };

    let miss_prefix = 0xBADBADBA_u32;
    let miss_marker = u64::MAX;

    c.bench_function("umbra_ptr.scattered_miss/native_arc_deref", |b| {
        b.iter(|| {
            let mut hits = 0u32;
            for &i in &access_order {
                if arcs[i].marker == black_box(miss_marker) { hits += 1; }
            }
            black_box(hits)
        });
    });

    c.bench_function("umbra_ptr.scattered_miss/umbra_prefix_shortcircuit", |b| {
        b.iter(|| {
            let mut hits = 0u32;
            for &i in &access_order {
                if umbras[i].ptr().matches_prefix(black_box(miss_prefix))
                    && umbras[i].value().marker == miss_marker { hits += 1; }
            }
            black_box(hits)
        });
    });
}

criterion_group!(
    benches,
    scan_late_match,
    scan_full_miss,
    scan_cache_pressure,
    scan_scattered_cache_miss,
);
criterion_main!(benches);
