//! Bench: HeartbeatTable beat / snapshot / mark_in_flight /
//! register-unregister vs `Mutex<Vec<HeartbeatRecord>>` and
//! `Vec<Mutex<HeartbeatRecord>>` baselines.
//!
//! The architectural claim: every per-slot op is a lock-free atomic
//! on a cache-line-aligned slot. No cross-slot false-sharing.
//! Cross-process visibility for free via the MMF substrate. The
//! in-process mutex baselines pay a lock per op AND cannot do
//! cross-process at any cost.
//!
//! Workloads:
//! - beat single slot (the per-process hot path)
//! - snapshot single slot (the watchdog/observer hot path)
//! - mark_in_flight single bit (work-tracking hot path)
//! - register + unregister cycle (claim path)

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::HeartbeatTable;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-hb-{name}-{pid}.bin"));
    p
}

// Naive baselines.

#[derive(Clone, Copy, Default)]
struct NaiveRec {
    pid: u32,
    last_seen_epoch: u64,
    in_flight_bitmap: u64,
}

// One mutex over the whole table (textbook naive global-lock shape).
struct MutexNaiveTable {
    inner: Mutex<(u64, Vec<NaiveRec>)>,  // (global_epoch, slots)
}

impl MutexNaiveTable {
    fn new(cap: usize) -> Self {
        Self { inner: Mutex::new((0, vec![NaiveRec::default(); cap])) }
    }
    fn beat(&self, idx: usize) {
        let mut g = self.inner.lock().unwrap();
        let global = g.0;
        g.1[idx].last_seen_epoch = global;
    }
    fn snapshot(&self, idx: usize) -> NaiveRec {
        self.inner.lock().unwrap().1[idx]
    }
    fn mark_in_flight(&self, idx: usize, bit: u8) {
        let mut g = self.inner.lock().unwrap();
        g.1[idx].in_flight_bitmap |= 1u64 << bit;
    }
    fn register(&self, pid: u32) -> Option<usize> {
        let mut g = self.inner.lock().unwrap();
        for (i, s) in g.1.iter_mut().enumerate() {
            if s.pid == 0 { s.pid = pid; return Some(i); }
        }
        None
    }
    fn unregister(&self, idx: usize) {
        let mut g = self.inner.lock().unwrap();
        g.1[idx] = NaiveRec::default();
    }
}

// Per-slot mutex (textbook fine-grain shape; the mutex equivalent of
// per-slot atomics, with one Mutex per cache-line-aligned slot).
struct PerSlotMutexTable {
    global_epoch: Mutex<u64>,
    slots: Vec<Mutex<NaiveRec>>,
}

impl PerSlotMutexTable {
    fn new(cap: usize) -> Self {
        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap { slots.push(Mutex::new(NaiveRec::default())); }
        Self { global_epoch: Mutex::new(0), slots }
    }
    fn beat(&self, idx: usize) {
        let global = *self.global_epoch.lock().unwrap();
        self.slots[idx].lock().unwrap().last_seen_epoch = global;
    }
    fn snapshot(&self, idx: usize) -> NaiveRec {
        *self.slots[idx].lock().unwrap()
    }
    fn mark_in_flight(&self, idx: usize, bit: u8) {
        self.slots[idx].lock().unwrap().in_flight_bitmap |= 1u64 << bit;
    }
}

// =========================================================
// beat single slot (per-process hot path)
// =========================================================

fn beat_single(c: &mut Criterion) {
    let path = tmp("beat");
    let t = HeartbeatTable::create(&path, 64).unwrap();
    let s = t.register(1000).unwrap();
    c.bench_function("heartbeat.beat/mmf", |b| {
        b.iter(|| t.beat(black_box(s)));
    });
    drop(t);
    std::fs::remove_file(&path).ok();

    let naive = MutexNaiveTable::new(64);
    naive.register(1000);
    c.bench_function("heartbeat.beat/mutex_naive", |b| {
        b.iter(|| naive.beat(black_box(0)));
    });

    let per_slot = PerSlotMutexTable::new(64);
    c.bench_function("heartbeat.beat/per_slot_mutex", |b| {
        b.iter(|| per_slot.beat(black_box(0)));
    });
}

// =========================================================
// snapshot single slot (watchdog/observer hot path)
// =========================================================

fn snapshot_single(c: &mut Criterion) {
    let path = tmp("snap");
    let t = HeartbeatTable::create(&path, 64).unwrap();
    let s = t.register(2000).unwrap();
    t.beat(s);
    c.bench_function("heartbeat.snapshot/mmf_seqlock", |b| {
        b.iter(|| black_box(t.snapshot(black_box(s))));
    });
    drop(t);
    std::fs::remove_file(&path).ok();

    let naive = MutexNaiveTable::new(64);
    naive.register(2000);
    c.bench_function("heartbeat.snapshot/mutex_naive", |b| {
        b.iter(|| black_box(naive.snapshot(black_box(0))));
    });

    let per_slot = PerSlotMutexTable::new(64);
    c.bench_function("heartbeat.snapshot/per_slot_mutex", |b| {
        b.iter(|| black_box(per_slot.snapshot(black_box(0))));
    });
}

// =========================================================
// mark_in_flight single bit (work-tracking hot path)
// =========================================================

fn mark_in_flight_bench(c: &mut Criterion) {
    let path = tmp("inflight");
    let t = HeartbeatTable::create(&path, 64).unwrap();
    let s = t.register(3000).unwrap();
    let mut bit = 0u8;
    c.bench_function("heartbeat.mark_in_flight/mmf", |b| {
        b.iter(|| {
            bit = (bit + 1) & 63;
            t.mark_in_flight(black_box(s), black_box(bit))
        });
    });
    drop(t);
    std::fs::remove_file(&path).ok();

    let naive = MutexNaiveTable::new(64);
    naive.register(3000);
    let mut bit = 0u8;
    c.bench_function("heartbeat.mark_in_flight/mutex_naive", |b| {
        b.iter(|| {
            bit = (bit + 1) & 63;
            naive.mark_in_flight(black_box(0), black_box(bit))
        });
    });

    let per_slot = PerSlotMutexTable::new(64);
    let mut bit = 0u8;
    c.bench_function("heartbeat.mark_in_flight/per_slot_mutex", |b| {
        b.iter(|| {
            bit = (bit + 1) & 63;
            per_slot.mark_in_flight(black_box(0), black_box(bit))
        });
    });
}

// =========================================================
// register + unregister cycle (claim path; mutating)
// =========================================================
//
// Pre-audit (c): register mutates the table. Without unregister
// per iter, the table fills up after `capacity` iters and the
// rest become Err. We pair register+unregister to keep the table
// empty across iters, measuring the round-trip cost.

fn register_unregister_cycle(c: &mut Criterion) {
    let path = tmp("reg");
    let t = HeartbeatTable::create(&path, 16).unwrap();
    let mut pid = 5000u32;
    c.bench_function("heartbeat.register_unregister/mmf", |b| {
        b.iter(|| {
            pid = pid.wrapping_add(1);
            let s = t.register(black_box(pid)).unwrap();
            t.unregister(s);
        });
    });
    drop(t);
    std::fs::remove_file(&path).ok();

    let naive = MutexNaiveTable::new(16);
    let mut pid = 5000u32;
    c.bench_function("heartbeat.register_unregister/mutex_naive", |b| {
        b.iter(|| {
            pid = pid.wrapping_add(1);
            let s = naive.register(black_box(pid)).unwrap();
            naive.unregister(s);
        });
    });
}

criterion_group!(benches,
    beat_single,
    snapshot_single,
    mark_in_flight_bench,
    register_unregister_cycle,
);
criterion_main!(benches);
