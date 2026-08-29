//! Bench: `SharedEpochs` vs a process-local `Mutex<Vec<(Epoch, u32)>>`
//! pin set - the design it replaces.
//!
//! Architectural claim: the shared pin table costs more per pin than a
//! process-local mutex over a sorted vector, and buys the one property
//! that design cannot have: a pin taken in one process is visible to a
//! reclaimer in another. The bench exists to say what that costs, not
//! to claim the shared table is faster.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. The baseline is not a strawman. It is the exact shape the
//!    replaced implementation used - a `Mutex` over a vector of
//!    `(epoch, refcount)` kept sorted, taken on pin and release only,
//!    with `first()` as the horizon. Both arms do the same three
//!    operations.
//! 2. Neither arm carries surplus work. The baseline allocates its
//!    vector once outside the loop; the shared arm maps its table once
//!    outside the loop. Neither pays setup inside a measured iteration.
//! 3. The horizon arms are measured at a fixed number of live pins,
//!    because the shared table scans its slots (O(capacity)) where the
//!    baseline reads `first()` (O(1)). Measuring the horizon with an
//!    empty table would hide exactly the cost the slot scan imposes,
//!    so it is measured with the table half full and again with it
//!    empty, and both numbers are reported.
//!
//! What the numbers do NOT show: cross-process visibility, which is the
//! whole reason for the shared table and which the baseline cannot do
//! at any price; and the dead-owner reap, which has no baseline at all
//! because a process-local pin set cannot outlive its process.

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedEpochs;

const CAPACITY: usize = 64;

/// The process-local pin set the shared table replaces: a mutex over a
/// small sorted vector of `(epoch, refcount)`, taken on pin and release
/// only.
struct LocalPins {
    now: std::sync::atomic::AtomicU64,
    pins: Mutex<Vec<(u64, u32)>>,
}

impl LocalPins {
    fn new() -> Self {
        Self {
            now: std::sync::atomic::AtomicU64::new(0),
            pins: Mutex::new(Vec::with_capacity(CAPACITY)),
        }
    }

    fn advance(&self) -> u64 {
        self.now.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1
    }

    fn pin(&self) -> u64 {
        let at = self.now.load(std::sync::atomic::Ordering::Acquire);
        let mut pins = self.pins.lock().unwrap();
        match pins.binary_search_by_key(&at, |(e, _)| *e) {
            Ok(i) => pins[i].1 += 1,
            Err(i) => pins.insert(i, (at, 1)),
        }
        at
    }

    fn release(&self, at: u64) {
        let mut pins = self.pins.lock().unwrap();
        if let Ok(i) = pins.binary_search_by_key(&at, |(e, _)| *e) {
            pins[i].1 -= 1;
            if pins[i].1 == 0 {
                pins.remove(i);
            }
        }
    }

    fn reclaim_horizon(&self) -> u64 {
        let pins = self.pins.lock().unwrap();
        pins.first().map_or_else(
            || self.now.load(std::sync::atomic::Ordering::Acquire),
            |(e, _)| *e,
        )
    }
}

fn advance(c: &mut Criterion) {
    let shared = SharedEpochs::create_anon(CAPACITY).unwrap();
    c.bench_function("epochs.advance/shared_mmf", |b| {
        b.iter(|| black_box(shared.advance()));
    });

    let local = LocalPins::new();
    c.bench_function("epochs.advance/local_mutex_vec", |b| {
        b.iter(|| black_box(local.advance()));
    });
}

/// One pin taken and released, which is what a scan pays at its
/// boundaries.
fn pin_release(c: &mut Criterion) {
    let shared = SharedEpochs::create_anon(CAPACITY).unwrap();
    c.bench_function("epochs.pin_release/shared_mmf", |b| {
        b.iter(|| {
            let g = shared.pin().unwrap();
            black_box(g.epoch())
        });
    });

    let local = LocalPins::new();
    c.bench_function("epochs.pin_release/local_mutex_vec", |b| {
        b.iter(|| {
            let at = local.pin();
            black_box(at);
            local.release(at);
        });
    });
}

/// The horizon with nothing pinned: the shared table still scans every
/// slot, the baseline reads `first()`.
fn horizon_idle(c: &mut Criterion) {
    let shared = SharedEpochs::create_anon(CAPACITY).unwrap();
    c.bench_function("epochs.horizon_idle/shared_mmf", |b| {
        b.iter(|| black_box(shared.reclaim_horizon()));
    });

    let local = LocalPins::new();
    c.bench_function("epochs.horizon_idle/local_mutex_vec", |b| {
        b.iter(|| black_box(local.reclaim_horizon()));
    });
}

/// The horizon with the table half full, which is where the slot scan
/// costs what it costs. Reported beside the idle figure rather than
/// instead of it.
fn horizon_loaded(c: &mut Criterion) {
    let shared = SharedEpochs::create_anon(CAPACITY).unwrap();
    let mut held = Vec::new();
    for _ in 0..CAPACITY / 2 {
        shared.advance();
        held.push(shared.pin().unwrap());
    }
    c.bench_function("epochs.horizon_loaded/shared_mmf", |b| {
        b.iter(|| black_box(shared.reclaim_horizon()));
    });
    drop(held);

    let local = LocalPins::new();
    let mut local_held = Vec::new();
    for _ in 0..CAPACITY / 2 {
        local.advance();
        local_held.push(local.pin());
    }
    c.bench_function("epochs.horizon_loaded/local_mutex_vec", |b| {
        b.iter(|| black_box(local.reclaim_horizon()));
    });
    for at in local_held {
        local.release(at);
    }
}

criterion_group!(benches, advance, pin_release, horizon_idle, horizon_loaded);
criterion_main!(benches);
