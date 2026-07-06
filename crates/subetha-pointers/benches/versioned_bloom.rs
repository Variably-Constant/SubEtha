//! Bench: VersionedPointer, VersionedChain, BloomPointer,
//! BloomCascade, HlcVersionedPointer, VectorClockPointer.
//!
//! Measures the architectural-claim case for each primitive:
//! visibility checks for VersionedPointer / HlcVersionedPointer,
//! time-travel reads through VersionedChain, miss-rate amortisation
//! for BloomPointer and BloomCascade, and causal-vs-concurrent
//! classification for VectorClock. (The upstream merkle_ptr group is
//! omitted; that type is not part of this crate.)

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_pointers::bloom_pointer::{BloomCascade, BloomPointer};
use subetha_pointers::versioned_pointer::{
    HlcVersionedPointer, HybridLogicalClock, VectorClock, VectorClockPointer,
    VersionedChain, VersionedPointer,
};

// =========================================================
// VersionedPointer: snapshot-isolation visibility scan over
// 1024 pointers, half visible at the query snapshot.
// =========================================================

fn versioned_visibility(c: &mut Criterion) {
    const N: usize = 1024;
    let raw_versions: Vec<u64> = (0..N as u64).collect();
    let versioned: Vec<VersionedPointer<u64>> = (0..N as u64)
        .map(|i| VersionedPointer::new(Arc::new(i), i))
        .collect();
    let snapshot = (N / 2) as u64;

    c.bench_function("versioned.visibility_scan/native_u64_compare", |b| {
        b.iter(|| {
            let mut visible = 0u32;
            for &v in raw_versions.iter() {
                if v <= black_box(snapshot) { visible += 1; }
            }
            black_box(visible)
        });
    });
    c.bench_function("versioned.visibility_scan/versioned_pointer", |b| {
        b.iter(|| {
            let mut visible = 0u32;
            for p in versioned.iter() {
                if p.visible_at(black_box(snapshot)) { visible += 1; }
            }
            black_box(visible)
        });
    });
}

// =========================================================
// VersionedChain: time-travel read at a deep historical version,
// with BTreeMap baseline (`range(..=snap).next_back()`).
// =========================================================

fn versioned_chain_time_travel(c: &mut Criterion) {
    let chain = VersionedChain::<u64>::new();
    let mut tree: std::collections::BTreeMap<u64, u64> =
        std::collections::BTreeMap::new();
    for v in 1..=100u64 {
        chain.push(v * 10, v);
        tree.insert(v, v * 10);
    }

    c.bench_function("versioned.chain/read_at_head_chain", |b| {
        b.iter(|| black_box(chain.read_at(black_box(100))));
    });
    c.bench_function("versioned.chain/read_at_head_btreemap", |b| {
        b.iter(|| {
            let r = tree.range(..=black_box(100u64)).next_back().map(|(_, v)| *v);
            black_box(r)
        });
    });
    c.bench_function("versioned.chain/read_at_mid_chain", |b| {
        b.iter(|| black_box(chain.read_at(black_box(50))));
    });
    c.bench_function("versioned.chain/read_at_mid_btreemap", |b| {
        b.iter(|| {
            let r = tree.range(..=black_box(50u64)).next_back().map(|(_, v)| *v);
            black_box(r)
        });
    });
    c.bench_function("versioned.chain/read_at_root_chain", |b| {
        b.iter(|| black_box(chain.read_at(black_box(1))));
    });
    c.bench_function("versioned.chain/read_at_root_btreemap", |b| {
        b.iter(|| {
            let r = tree.range(..=black_box(1u64)).next_back().map(|(_, v)| *v);
            black_box(r)
        });
    });
}

// =========================================================
// BloomPointer at SUGGESTED_CAPACITY (8 keys per subset).
// 1024 candidate pointers, scan for a key not present in any.
// =========================================================

fn bloom_skip_vs_deref(c: &mut Criterion) {
    const N: usize = 1024;
    const KEYS_PER_SUBSET: u64 = 8;  // == Bloom64::SUGGESTED_CAPACITY
    let mut bps: Vec<BloomPointer<Vec<u64>>> = Vec::with_capacity(N);
    let mut native: Vec<Arc<Vec<u64>>> = Vec::with_capacity(N);
    for i in 0..N as u64 {
        let v: Arc<Vec<u64>> = Arc::new(
            (i * KEYS_PER_SUBSET..i * KEYS_PER_SUBSET + KEYS_PER_SUBSET).collect()
        );
        let keys: Vec<u64> = v.iter().copied().collect();
        bps.push(BloomPointer::from_keys(v.clone(), keys));
        native.push(v);
    }
    let miss_key = 999_999u64;
    c.bench_function("bloom_ptr.miss_query/native_arc_scan", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for arc in native.iter() {
                if arc.contains(black_box(&miss_key)) { found += 1; }
            }
            black_box(found)
        });
    });
    c.bench_function("bloom_ptr.miss_query/bloom_shortcircuit", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for bp in bps.iter() {
                if bp.might_contain(black_box(&miss_key))
                    && bp.target().contains(&miss_key) { found += 1; }
            }
            black_box(found)
        });
    });
}

// =========================================================
// BloomCascade at the transition regime (32 keys per subset):
// saturates Bloom64 but fits BloomFine. Native vs single-level
// vs two-level cascade.
// =========================================================

fn bloom_cascade_layered(c: &mut Criterion) {
    const N: usize = 1024;
    const KEYS_PER_SUBSET: u64 = 32;  // saturates Bloom64, fits BloomFine
    let mut native: Vec<Arc<Vec<u64>>> = Vec::with_capacity(N);
    let mut single: Vec<BloomPointer<Vec<u64>>> = Vec::with_capacity(N);
    let mut cascades: Vec<BloomCascade<Vec<u64>>> = Vec::with_capacity(N);
    for i in 0..N as u64 {
        let v: Arc<Vec<u64>> = Arc::new(
            (i * KEYS_PER_SUBSET..i * KEYS_PER_SUBSET + KEYS_PER_SUBSET).collect()
        );
        let keys: Vec<u64> = v.iter().copied().collect();
        native.push(v.clone());
        single.push(BloomPointer::from_keys(v.clone(), keys.clone()));
        cascades.push(BloomCascade::from_keys(v, keys));
    }
    let miss_key = 999_999u64;

    c.bench_function("bloom_cascade.miss_query/native_arc_scan", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for arc in native.iter() {
                if arc.contains(black_box(&miss_key)) { found += 1; }
            }
            black_box(found)
        });
    });
    c.bench_function("bloom_cascade.miss_query/single_level", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for bp in single.iter() {
                if bp.might_contain(black_box(&miss_key))
                    && bp.target().contains(&miss_key) { found += 1; }
            }
            black_box(found)
        });
    });
    c.bench_function("bloom_cascade.miss_query/two_level_cascade", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for bc in cascades.iter() {
                if bc.cascade_check(black_box(&miss_key)).might_contain()
                    && bc.target().contains(&miss_key) { found += 1; }
            }
            black_box(found)
        });
    });
}

// =========================================================
// HLC tie-breaking: 1024 events across 16 physical ticks
// (~64/tick); snapshot mid-tick at HLC(8, 32).
// =========================================================

fn hlc_tie_breaking_scan(c: &mut Criterion) {
    const N: u64 = 1024;
    const EVENTS_PER_TICK: u64 = 64;
    let events: Vec<HlcVersionedPointer<u64>> = (0..N)
        .map(|i| {
            let physical = i / EVENTS_PER_TICK;
            let logical = i % EVENTS_PER_TICK;
            HlcVersionedPointer::new(
                Arc::new(i),
                HybridLogicalClock::new(physical, logical),
            )
        })
        .collect();
    let snapshot = HybridLogicalClock::new(8, 32);

    let raw_tuples: Vec<(u64, u64)> = events
        .iter()
        .map(|e| (e.clock().physical, e.clock().logical))
        .collect();
    let snap_tuple = (snapshot.physical, snapshot.logical);

    c.bench_function("hlc.tie_breaking_scan/native_tuple_compare", |b| {
        b.iter(|| {
            let mut visible = 0u32;
            for &t in raw_tuples.iter() {
                if t <= black_box(snap_tuple) { visible += 1; }
            }
            black_box(visible)
        });
    });
    c.bench_function("hlc.tie_breaking_scan/hlc_pointer", |b| {
        b.iter(|| {
            let mut visible = 0u32;
            for p in events.iter() {
                if p.visible_at(black_box(snapshot)) { visible += 1; }
            }
            black_box(visible)
        });
    });

    // single_u64_lossy: physical timestamps only; overcounts at the
    // snapshot's tick (correctness divergence, measured for cost).
    let degraded: Vec<u64> = events.iter().map(|e| e.clock().physical).collect();
    c.bench_function("hlc.tie_breaking_scan/single_u64_lossy", |b| {
        b.iter(|| {
            let mut visible = 0u32;
            for &v in degraded.iter() {
                if v <= black_box(snapshot.physical) { visible += 1; }
            }
            black_box(visible)
        });
    });
}

// =========================================================
// VectorClockPointer: causal-vs-concurrent classification vs a
// max-element compare baseline that collapses concurrency.
// =========================================================

fn vector_clock_causal_classification(c: &mut Criterion) {
    const N: usize = 1024;
    const NODES: usize = 3;

    let pairs: Vec<(VectorClock<NODES>, VectorClock<NODES>)> = (0..N)
        .map(|i| {
            let a = VectorClock::<NODES> {
                clock: [
                    (i % 50) as u64,
                    ((i * 7) % 50) as u64,
                    ((i * 13) % 50) as u64,
                ],
            };
            let b = VectorClock::<NODES> {
                clock: [
                    ((i + 5) % 50) as u64,
                    ((i * 7 + 3) % 50) as u64,
                    ((i * 13).wrapping_sub(1) % 50) as u64,
                ],
            };
            (a, b)
        })
        .collect();

    c.bench_function("vector_clock.causal_classify/vector_clock_cmp", |b| {
        b.iter(|| {
            let mut concurrent = 0u32;
            let mut ordered = 0u32;
            for (a, b_clk) in pairs.iter() {
                match a.causal_cmp(b_clk) {
                    None => concurrent += 1,
                    Some(_) => ordered += 1,
                }
            }
            black_box((concurrent, ordered))
        });
    });
    c.bench_function("vector_clock.causal_classify/native_max_compare", |b| {
        b.iter(|| {
            let mut a_lt_b = 0u32;
            for (a, b_clk) in pairs.iter() {
                let max_a = a.clock.iter().copied().max().unwrap_or(0);
                let max_b = b_clk.clock.iter().copied().max().unwrap_or(0);
                if max_a < max_b { a_lt_b += 1; }
            }
            black_box(a_lt_b)
        });
    });

    let snapshot = VectorClock::<NODES> { clock: [50, 50, 50] };
    let events: Vec<VectorClockPointer<u64, NODES>> = (0..N as u64)
        .map(|i| {
            VectorClockPointer::new(
                Arc::new(i),
                VectorClock::<NODES> {
                    clock: [
                        (i % 50),
                        ((i * 7) % 50),
                        ((i * 13) % 50),
                    ],
                },
            )
        })
        .collect();
    c.bench_function("vector_clock.causal_classify/vector_clock_pointer_read_at", |b| {
        b.iter(|| {
            let mut visible = 0u32;
            for p in events.iter() {
                if p.read_at(black_box(snapshot)).is_some() { visible += 1; }
            }
            black_box(visible)
        });
    });
}

// =========================================================
// BloomCascade at BloomFine SUGGESTED_CAPACITY (64 keys/subset).
// =========================================================

fn bloom_skip_vs_deref_large(c: &mut Criterion) {
    const N: usize = 128;
    const KEYS_PER_SUBSET: u64 = 64;  // == BloomFine::SUGGESTED_CAPACITY
    let mut native: Vec<Arc<Vec<u64>>> = Vec::with_capacity(N);
    let mut cascades: Vec<BloomCascade<Vec<u64>>> = Vec::with_capacity(N);
    for i in 0..N as u64 {
        let v: Arc<Vec<u64>> = Arc::new(
            (i * KEYS_PER_SUBSET..i * KEYS_PER_SUBSET + KEYS_PER_SUBSET).collect()
        );
        let keys: Vec<u64> = v.iter().copied().collect();
        native.push(v.clone());
        cascades.push(BloomCascade::from_keys(v, keys));
    }
    let miss_key = 9_999_999u64;
    c.bench_function("bloom_ptr.miss_large_subset/native_arc_scan", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for arc in native.iter() {
                if arc.contains(black_box(&miss_key)) { found += 1; }
            }
            black_box(found)
        });
    });
    c.bench_function("bloom_ptr.miss_large_subset/bloom_cascade_shortcircuit", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for bc in cascades.iter() {
                if bc.cascade_check(black_box(&miss_key)).might_contain()
                    && bc.target().contains(&miss_key) { found += 1; }
            }
            black_box(found)
        });
    });
}

// =========================================================
// Expensive-deref workload: Vec<String>::contains over 16
// 32-char strings (cache-miss per byte compare) vs Bloom's
// u64 hash + 4 bit tests staying in L1.
// =========================================================

fn bloom_expensive_deref(c: &mut Criterion) {
    const N: usize = 512;
    const KEYS_PER_SUBSET: usize = 8;
    fn pad32(s: String) -> String {
        let mut out = s;
        while out.len() < 32 { out.push('x'); }
        out.truncate(32);
        out
    }
    let mut native: Vec<Arc<Vec<String>>> = Vec::with_capacity(N);
    let mut bps: Vec<BloomPointer<Vec<String>>> = Vec::with_capacity(N);
    for i in 0..N {
        let strings: Vec<String> = (0..KEYS_PER_SUBSET)
            .map(|j| pad32(format!("session-{i:04}-{j:02}-data")))
            .collect();
        let v: Arc<Vec<String>> = Arc::new(strings);
        let keys: Vec<String> = v.iter().cloned().collect();
        bps.push(BloomPointer::from_keys(v.clone(), keys));
        native.push(v);
    }
    let miss_key = pad32(String::from("no-such-session-zzzzzz"));
    c.bench_function("bloom_expensive_deref/native_string_scan", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for arc in native.iter() {
                if arc.contains(black_box(&miss_key)) { found += 1; }
            }
            black_box(found)
        });
    });
    c.bench_function("bloom_expensive_deref/bloom_shortcircuit", |b| {
        b.iter(|| {
            let mut found = 0u32;
            for bp in bps.iter() {
                if bp.might_contain(black_box(&miss_key))
                    && bp.target().contains(&miss_key) { found += 1; }
            }
            black_box(found)
        });
    });
}

criterion_group!(
    benches,
    versioned_visibility,
    versioned_chain_time_travel,
    bloom_skip_vs_deref,
    bloom_skip_vs_deref_large,
    bloom_cascade_layered,
    bloom_expensive_deref,
    hlc_tie_breaking_scan,
    vector_clock_causal_classification,
);
criterion_main!(benches);
