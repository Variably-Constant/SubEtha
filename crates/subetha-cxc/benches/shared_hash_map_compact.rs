//! Bench: SharedHashMap compact() amortised over a churning
//! insert+remove workload, vs Mutex<HashMap<u32, u32>> (which has
//! no tombstone problem because std::HashMap re-uses buckets after
//! delete).
//!
//! Architectural claim: SharedHashMap with periodic compact at 30%
//! tombstone threshold matches std HashMap throughput on the same
//! workload, despite paying for in-place rebuild - because the
//! amortised compaction cost is small compared to lookup speedup
//! from a fresh probe chain.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedHashMap, InsertOutcome};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-map-compact-{name}-{pid}.bin"));
    p
}

fn compact_overhead(c: &mut Criterion) {
    // Setup: 1024-slot map with 256 inserts then 192 removes ->
    // 192 tombstones (~18% of capacity). Bench the compact call.
    let p = tmp("overhead");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 1024).unwrap();
    c.bench_function("map.compact_192_tombstones/mmf", |b| {
        b.iter_with_setup(
            || {
                // Reset table to fresh state with 192 tombstones.
                m.clear();
                for k in 0..256u32 {
                    m.insert(k, k).unwrap();
                }
                for k in 0..192u32 {
                    m.remove(&k);
                }
            },
            |_| black_box(m.compact().unwrap()),
        );
    });
    drop(m);
    std::fs::remove_file(&p).ok();
}

fn churn_workload_with_compact(c: &mut Criterion) {
    // Churning workload: insert N, then in a loop remove the
    // oldest + insert a new key. Periodically compact when
    // tombstones cross 30% of capacity.
    const CAP: usize = 256;
    const LIVE: u32 = 64;
    const ROUNDS: u32 = 200;

    let p = tmp("churn-mmf");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, CAP).unwrap();
    c.bench_function("map.churn_with_compact/mmf", |b| {
        b.iter_with_setup(
            || {
                m.clear();
                for k in 0..LIVE { m.insert(k, k).unwrap(); }
            },
            |_| {
                for (round, next_key) in (0..ROUNDS).zip(LIVE..) {
                    let oldest = round % LIVE;
                    m.remove(&oldest);
                    m.insert(black_box(next_key), next_key).expect("map insert failed");
                    if m.should_compact(0.30) {
                        m.compact().unwrap();
                    }
                }
            },
        );
    });
    drop(m);
    std::fs::remove_file(&p).ok();

    let h: Mutex<HashMap<u32, u32>> = Mutex::new(HashMap::with_capacity(CAP));
    c.bench_function("map.churn_with_compact/mutex_hashmap", |b| {
        b.iter_with_setup(
            || {
                let mut g = h.lock().unwrap();
                g.clear();
                for k in 0..LIVE { g.insert(k, k); }
            },
            |_| {
                for (round, next_key) in (0..ROUNDS).zip(LIVE..) {
                    let oldest = round % LIVE;
                    let mut g = h.lock().unwrap();
                    g.remove(&oldest);
                    g.insert(black_box(next_key), next_key);
                }
            },
        );
    });
}

fn should_compact_query(c: &mut Criterion) {
    let p = tmp("should");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 1024).unwrap();
    for k in 0..512u32 { m.insert(k, k).unwrap(); }
    for k in 0..300u32 { m.remove(&k); }
    c.bench_function("map.should_compact_query/mmf", |b| {
        b.iter(|| black_box(m.should_compact(black_box(0.30))));
    });
    drop(m);
    std::fs::remove_file(&p).ok();
}

fn insert_with_tombstone_bookkeeping(c: &mut Criterion) {
    // Make sure the new tombstones.fetch_add in remove and the
    // new header field don't measurably regress remove latency.
    let p = tmp("remove-cost");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 1 << 16).unwrap();
    let mut next_key = 0u32;
    c.bench_function("map.remove_with_tombstone_bookkeeping/mmf", |b| {
        b.iter(|| {
            // Insert + remove to exercise the bumped path. Both
            // operations include the new tombstone counter store.
            m.insert(next_key, next_key).ok();
            m.remove(&next_key);
            next_key = next_key.wrapping_add(1);
        });
    });
    drop(m);
    std::fs::remove_file(&p).ok();
}

// Suppress the unused-import warning on InsertOutcome; the type
// belongs in the file's import set for symmetry with the other
// bench files in this crate.
#[allow(dead_code)]
fn _unused() {
    let _: InsertOutcome = InsertOutcome::Inserted;
}

criterion_group!(benches,
    compact_overhead,
    churn_workload_with_compact,
    should_compact_query,
    insert_with_tombstone_bookkeeping,
);
criterion_main!(benches);
