//! Bench: `HolderTable` vs the `AtomicU32` refcount it replaces.
//!
//! Architectural claim: a slot stamped with its holder's pid can be
//! asked whether that holder is still running. A count cannot, so a
//! holder that dies leaves the count high forever. The bench says what
//! that costs.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. Both arms do the same thing: take a hold, then drop it. The
//!    refcount arm is `fetch_add` then `fetch_sub`, which is the whole
//!    of what a refcount can do; the table arm scans for a free slot,
//!    CASes it, stamps a pid, publishes and releases.
//! 2. Neither arm carries surplus work. The table is allocated once
//!    outside the loop, as is the counter.
//! 3. The scan cost depends on how full the table is, so claim is
//!    measured on an empty table and again with it three-quarters
//!    held, and both are reported. Measuring only the empty case would
//!    hide the scan the design pays for.
//!
//! What the numbers do NOT show: the reap. A refcount has no
//! equivalent, because there is nothing in it to probe - which is the
//! reason the table exists and the reason a bench cannot express its
//! value.

use std::hint::black_box;
use std::sync::atomic::{AtomicU32, Ordering};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedEpochs;

const CAPACITY: usize = 64;

/// A zeroed, correctly aligned table, taken from a real consumer's
/// allocation rather than a hand-built one.
fn table(capacity: usize) -> SharedEpochs {
    SharedEpochs::create_anon(capacity).unwrap()
}

fn claim_release_empty(c: &mut Criterion) {
    let e = table(CAPACITY);
    let t = e.pins();
    c.bench_function("holder.claim_release/table_empty", |b| {
        b.iter(|| {
            let slot = t.claim(black_box(7)).unwrap();
            t.release(slot);
        });
    });

    let count = AtomicU32::new(0);
    c.bench_function("holder.claim_release/atomic_refcount", |b| {
        b.iter(|| {
            count.fetch_add(1, Ordering::AcqRel);
            count.fetch_sub(1, Ordering::AcqRel);
        });
    });
}

/// The scan is what a free-slot search costs, and it costs most when
/// the table is nearly full. Reported beside the empty figure.
fn claim_release_loaded(c: &mut Criterion) {
    let e = table(CAPACITY);
    let t = e.pins();
    let held: Vec<usize> = (0..CAPACITY * 3 / 4).map(|i| t.claim(i as u64 + 1).unwrap()).collect();
    c.bench_function("holder.claim_release/table_three_quarters_held", |b| {
        b.iter(|| {
            let slot = t.claim(black_box(7)).unwrap();
            t.release(slot);
        });
    });
    for s in held {
        t.release(s);
    }
}

/// Reading how many holders there are: a scan of the slots against one
/// atomic load.
fn live_count(c: &mut Criterion) {
    let e = table(CAPACITY);
    let t = e.pins();
    let held: Vec<usize> = (0..CAPACITY / 2).map(|i| t.claim(i as u64 + 1).unwrap()).collect();
    c.bench_function("holder.live/table", |b| {
        b.iter(|| black_box(t.live()));
    });
    for s in held {
        t.release(s);
    }

    let count = AtomicU32::new(32);
    c.bench_function("holder.live/atomic_refcount", |b| {
        b.iter(|| black_box(count.load(Ordering::Acquire)));
    });
}

criterion_group!(benches, claim_release_empty, claim_release_loaded, live_count);
criterion_main!(benches);
