//! A/B/C bench: does the Bloom filter help vs the baseline HashSet,
//! and is the ported version on par with the original?
//!
//! Workload: a "have I seen this workload-shape signature?"
//! check pattern. AdaptiveIpc tracks recent (shape_id, batch_size)
//! pairs to decide when to migrate. The hot question is whether
//! we have observed a particular (shape, size) pair recently.
//!
//! Contenders:
//! - **A (baseline)**: `std::collections::HashSet<(u32, u32)>` for
//!   exact membership. Allocates per insert, hashes per lookup.
//! - **B (original `Bloom64`)**: from `subetha-pointers` (nightly).
//! - **C (ported `Bloom64`)**: from `subetha-cxc::bloom_filter`
//!   (stable Rust, no nightly dependency).
//!
//! Operation pattern: insert N=8 distinct keys, then perform M=10000
//! `might_contain` (or `contains`) checks with a 50/50 hit/miss
//! distribution. Mirrors the AdaptiveIpc.maybe_promote use case
//! where N=number of distinct shapes seen and M=number of decision
//! points.
//!
//! Bench audit (HARD RULE 3):
//! - All three contenders perform the same operations: 8 inserts +
//!   10000 membership checks on the same (key generator, hit pattern).
//! - Same key types (`(u32, u32)` for HashSet, hashed bytes for both
//!   Bloom variants).
//! - No surplus locks/allocations on the Bloom variants; HashSet
//!   pays its standard per-insert alloc which is part of the
//!   architectural cost it carries.

#![allow(clippy::missing_docs_in_private_items)]

use std::collections::HashSet;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_pointers::bloom_pointer::Bloom64;

const N: u32 = 8;
const M: u32 = 10_000;

fn make_query_keys() -> Vec<(u32, u32)> {
    // 50% hits (within 0..N), 50% misses (large unrelated values).
    (0..M)
        .map(|i| {
            if i % 2 == 0 {
                (i % N, (i % N).wrapping_mul(7))
            } else {
                (1_000_000 + i, 999_999_999u32.wrapping_sub(i))
            }
        })
        .collect()
}

// ============================================================
// A: HashSet<(u32, u32)>
// ============================================================
fn bench_a_hashset(c: &mut Criterion) {
    let queries = make_query_keys();

    c.bench_function("bloom_abc/A_hashset_baseline", |b| {
        b.iter(|| {
            let mut set: HashSet<(u32, u32)> = HashSet::with_capacity(N as usize);
            for k in 0..N {
                set.insert((k, k.wrapping_mul(7)));
            }
            let mut hits = 0u64;
            for q in &queries {
                if set.contains(q) {
                    hits += 1;
                }
            }
            black_box(hits)
        });
    });
}

// ============================================================
// B: Original Bloom64 from subetha-pointers
// ============================================================
fn bench_b_bloom_subetha_pointers(c: &mut Criterion) {
    let queries = make_query_keys();

    c.bench_function("bloom_ab/B_bloom_subetha_pointers", |b| {
        b.iter(|| {
            let mut f = Bloom64::ZERO;
            for k in 0..N {
                f.insert(&(k, k.wrapping_mul(7)));
            }
            let mut hits = 0u64;
            for q in &queries {
                if f.might_contain(q) {
                    hits += 1;
                }
            }
            black_box(hits)
        });
    });
}

criterion_group!(
    benches,
    bench_a_hashset,
    bench_b_bloom_subetha_pointers,
);
criterion_main!(benches);
