//! Bench: what a whole-store scan costs when the store is split into
//! `k` single-writer lanes instead of one tree.
//!
//! This is a DECISION bench, not a bench of a shipped primitive. Writer
//! lanes would let `k` statements write a versioned index without
//! contending, at the price of every ordered read becoming a `k`-way
//! merge. The merge is the cost that decides whether lanes are worth
//! building, so it is measured before the primitive exists.
//!
//! The contender is the same total data in one `VersionedBTreeMap`, and
//! both arms drain the whole store in chunks of the same limit, so the
//! numbers say what the merge costs and nothing else.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. Every arm holds the SAME entries and the same total node
//!    capacity: `NODES` split `NODES / k` per lane. A `k`-lane arm is
//!    not given more arena than the single tree.
//! 2. Neither arm carries surplus work: every tree is created and
//!    populated outside the measured loop, and every arm drains with
//!    the same chunk limit through the same resume rule.
//! 3. Keys are distributed round-robin across lanes, so every lane
//!    spans the whole key range. That is the WORST case for a merge -
//!    maximum interleaving, every lane contributing at every point -
//!    and it is also the realistic one, because a statement claims
//!    whichever lane is free rather than one chosen by key.
//!
//! The frontier rule under test: a lane that returns a cursor may still
//! hold keys past it, so the merged output can only be trusted up to
//! the SMALLEST cursor any lane reported. Emitting past that would
//! publish a key before a smaller one that a lane has not yet been
//! asked for. A lane whose cursor is `None` walked to the end of the
//! range and bounds nothing.
//!
//! What the scan numbers do NOT show is the write side, and what the
//! write side is actually being compared against.
//!
//! `SharedBTreeMap` is single-writer - its own docs state that
//! concurrent `insert` / `remove` need external coordination and that
//! two simultaneous writers corrupt the node structure - and
//! `VersionedBTreeMap` inherits that. So lanes are not an optimization
//! over a tree that already takes concurrent writers. They are the
//! alternative to serializing every writing statement behind one
//! global mutex over the whole index. That is the trade these numbers
//! price: an ordered scan costs more, and writers stop queueing.
//!
//! Where the serialization goes instead is measured here too. Every
//! lane shares one epoch table, so `advance` and `pin` are contended
//! where the trees are not, and lanes are only worth having if that
//! costs less than the mutex they replace. The write arms put `k`
//! concurrent writers against a mutex-serialized single tree.
//!
//! Both write arms spawn the same threads and perform the same number
//! of inserts inside the timed region, so thread startup is charged to
//! both equally. The mutex arm takes the lock per insert, which is the
//! finest granularity a caller could use; holding it for a whole
//! statement would serialize harder still.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hint::black_box;
use std::ops::Bound;
use std::sync::Mutex;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{LanedVersionedMap, PinGuard, VersionedBTreeMap};

const NODES: usize = 1 << 14;
const ENTRIES: u64 = 4_000;
const CHUNK: usize = 1_024;

type Lane = VersionedBTreeMap<u64, u64>;

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join(format!("subetha-bench-lanes-{name}-{}", std::process::id()));
    match std::fs::remove_dir_all(&d) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("stale bench directory {} not removed: {e}", d.display()),
    }
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Remove an arm's directory once its maps are dropped; a refusal is a bench
/// bug (a mapping still alive), not something to leave behind quietly.
fn remove(d: &std::path::Path) {
    std::fs::remove_dir_all(d).expect("the arm's maps are dropped and its directory removable");
}

/// `k` lanes over one shared epoch table, holding `ENTRIES` round-robin.
fn build(name: &str, k: usize) -> (std::path::PathBuf, Vec<Lane>) {
    let d = dir(name);
    let epochs = d.join("shared.epochs");
    let per_lane_nodes = NODES / k;
    let lanes: Vec<Lane> = (0..k)
        .map(|i| {
            VersionedBTreeMap::create(d.join(format!("lane{i}.bin")), per_lane_nodes, &epochs, 16)
                .unwrap()
        })
        .collect();
    for i in 0..ENTRIES {
        lanes[(i as usize) % k].insert(i, i).unwrap();
    }
    (d, lanes)
}

/// Drain the whole store in chunks, merging the lanes in key order.
///
/// Each round asks every lane for the next chunk, cuts every lane at the
/// smallest cursor reported, merges what is left, and resumes past the
/// frontier so the drain cannot stall.
///
/// The merge is a k-way selection over lane results that are ALREADY in
/// key order, not a re-sort of their concatenation. A re-sort would
/// charge the lane arm `O(n log n)` work that a real implementation
/// would never do - the lanes hand back ordered runs - and would report
/// the merge as more expensive than it is.
fn drain_merged(lanes: &[Lane], pin: &PinGuard<'_>) -> usize {
    let mut low: Bound<u64> = Bound::Unbounded;
    let mut total = 0usize;
    let mut per_lane: Vec<Vec<(u64, u64)>> = vec![Vec::new(); lanes.len()];
    loop {
        let mut frontier: Option<u64> = None;
        for (slot, l) in per_lane.iter_mut().zip(lanes) {
            let lb = match &low {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Excluded(k) => Bound::Excluded(k),
                Bound::Included(k) => Bound::Included(k),
            };
            let (r, c) = l.range_at_with_cursor(lb, Bound::Unbounded, CHUNK, pin);
            if let Some(c) = c {
                frontier = Some(frontier.map_or(c, |f: u64| f.min(c)));
            }
            *slot = r;
        }
        // Cut each ordered run at the frontier. Every lane result is
        // ascending, so the cut is a truncate, not a filter.
        if let Some(f) = frontier {
            for rows in &mut per_lane {
                let end = rows.partition_point(|(k, _)| *k <= f);
                rows.truncate(end);
            }
        }
        // k-way merge over the ordered runs, heap-based so the cost is
        // O(n log k). A linear scan for the smallest head is O(n * k),
        // which would charge the widest arm for the merge strategy
        // rather than for having lanes.
        let mut idx = vec![0usize; per_lane.len()];
        let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
        for (i, rows) in per_lane.iter().enumerate() {
            if let Some((k, _)) = rows.first() {
                heap.push(Reverse((*k, i)));
            }
        }
        while let Some(Reverse((_, i))) = heap.pop() {
            total += 1;
            idx[i] += 1;
            if let Some((k, _)) = per_lane[i].get(idx[i]) {
                heap.push(Reverse((*k, i)));
            }
        }
        match frontier {
            // Every lane reached the end of the range: nothing remains.
            None => return total,
            Some(f) => low = Bound::Excluded(f),
        }
    }
}

/// The baseline a caller has today: one tree, drained with the chunked
/// resume the `VersionedBTreeMap` docs prescribe, with no merge
/// scaffolding at all. Running the `k = 1` case through `drain_merged`
/// instead would charge the baseline for machinery it does not use and
/// flatter every lane arm.
fn drain_plain(lane: &Lane, pin: &PinGuard<'_>) -> usize {
    let mut low: Bound<u64> = Bound::Unbounded;
    let mut total = 0usize;
    loop {
        let lb = match &low {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Excluded(k) => Bound::Excluded(k),
            Bound::Included(k) => Bound::Included(k),
        };
        let (rows, cursor) = lane.range_at_with_cursor(lb, Bound::Unbounded, CHUNK, pin);
        total += rows.len();
        match cursor {
            None => return total,
            Some(c) => low = Bound::Excluded(c),
        }
    }
}

fn whole_store_scan(c: &mut Criterion) {
    let (d, lanes) = build("plain", 1);
    let pin = lanes[0].pin().unwrap();
    let drained = drain_plain(&lanes[0], &pin);
    assert_eq!(drained, ENTRIES as usize, "the plain drain lost rows");
    c.bench_function("lanes.whole_store_range_at/plain_single_tree", |b| {
        b.iter(|| black_box(drain_plain(&lanes[0], &pin)));
    });
    drop(pin);
    drop(lanes);
    remove(&d);

    for k in [1usize, 4, 8, 16] {
        let (d, lanes) = build(&format!("k{k}"), k);
        let pin = lanes[0].pin().unwrap();
        let drained = drain_merged(&lanes, &pin);
        assert_eq!(
            drained, ENTRIES as usize,
            "k={k} drained {drained} of {ENTRIES}: the merge lost rows"
        );
        c.bench_function(&format!("lanes.whole_store_range_at/k{k}"), |b| {
            b.iter(|| black_box(drain_merged(&lanes, &pin)));
        });
        drop(pin);
        drop(lanes);
        remove(&d);
    }
}

/// `k` writers against `k` lanes, and the same `k` writers against one
/// tree behind a mutex - the coordination lanes replace.
///
/// Each writer rewrites its OWN key range every pass, so a key is
/// updated in place rather than added: the arenas stay bounded and the
/// measurement is steady-state write throughput, not arena growth.
///
/// The per-writer work is deliberately large. Spawning four OS threads
/// costs a few hundred microseconds, and at a few hundred inserts per
/// writer that swamps the write work entirely - both arms then measure
/// thread startup and report a tie whatever the contention is. At
/// `PASSES * KEYS` inserts each the startup is a small fraction and the
/// arms can actually differ.
fn concurrent_writes(c: &mut Criterion) {
    const WRITERS: usize = 4;
    const KEYS: u64 = 500;
    const PASSES: u64 = 50;

    let d = dir("wr-lanes");
    let laned: LanedVersionedMap<u64, u64> =
        LanedVersionedMap::create(&d, WRITERS, NODES / WRITERS, 16).unwrap();
    c.bench_function("lanes.concurrent_write/lanes_4", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                std::thread::scope(|s| {
                    for w in 0..WRITERS {
                        let map = &laned;
                        s.spawn(move || {
                            let lane = map.claim_lane().expect("a writer per lane");
                            let base = (w as u64) * 10_000;
                            for p in 0..PASSES {
                                for k in 0..KEYS {
                                    lane.insert(black_box(base + k), p).unwrap();
                                }
                            }
                        });
                    }
                });
            }
            start.elapsed()
        });
    });
    drop(laned);
    remove(&d);

    // Empty, and the same total node capacity the lanes get between
    // them, so the two arms start in the same state.
    let d = dir("wr-mutex");
    let tree: Lane =
        VersionedBTreeMap::create(d.join("t.bin"), NODES, d.join("e.bin"), 16).unwrap();
    let guarded = Mutex::new(tree);
    c.bench_function("lanes.concurrent_write/one_tree_under_mutex", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                std::thread::scope(|s| {
                    for w in 0..WRITERS {
                        let m = &guarded;
                        s.spawn(move || {
                            let base = (w as u64) * 10_000;
                            for p in 0..PASSES {
                                for k in 0..KEYS {
                                    m.lock().unwrap().insert(black_box(base + k), p).unwrap();
                                }
                            }
                        });
                    }
                });
            }
            start.elapsed()
        });
    });
    drop(guarded);
    remove(&d);
}

criterion_group!(benches, whole_store_scan, concurrent_writes);
criterion_main!(benches);
