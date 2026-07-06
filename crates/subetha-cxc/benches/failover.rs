//! Bench: FailoverWatchdog scan vs a naive `Vec<Mutex<u64>>`
//! heartbeat scan baseline. The architectural claim: a subetha
//! watchdog scans N MMF-backed heartbeat slots with one atomic
//! load each + cross-process visibility, where the naive baseline
//! pays a mutex lock per slot AND can only run in-process.
//!
//! Workloads:
//! - scan with 64 slots, all alive (no reclaim)
//! - scan with 64 slots, 4 dead (reclaim path)
//! - scan with 1024 slots, all alive (large table)
//! - iter_in_flight_bits over a sparse u64 bitmap

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{FailoverWatchdog, HeartbeatTable};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-failover-{name}-{pid}.bin"));
    p
}

// Naive in-process heartbeat-scan baseline: one mutex per slot
// guarding (pid, last_seen_epoch, in_flight_bitmap). The naive
// shape every "lockable per-slot heartbeat" implementation lands
// on without lock-free per-slot atomics.
struct NaiveHeartbeatSlot {
    pid: u32,
    last_seen_epoch: u64,
    in_flight_bitmap: u64,
}

struct NaiveHeartbeatTable {
    slots: Vec<Mutex<NaiveHeartbeatSlot>>,
    global_epoch: Mutex<u64>,
}

impl NaiveHeartbeatTable {
    fn new(n_slots: usize) -> Self {
        let mut slots = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            slots.push(Mutex::new(NaiveHeartbeatSlot {
                pid: 0, last_seen_epoch: 0, in_flight_bitmap: 0,
            }));
        }
        Self { slots, global_epoch: Mutex::new(0) }
    }
    fn register(&self, idx: usize, pid: u32) {
        let mut s = self.slots[idx].lock().unwrap();
        s.pid = pid;
        s.in_flight_bitmap = 1u64 << (idx % 64);
    }
    fn beat(&self, idx: usize) {
        let g = *self.global_epoch.lock().unwrap();
        let mut s = self.slots[idx].lock().unwrap();
        s.last_seen_epoch = g;
    }
    fn tick(&self) -> u64 {
        let mut g = self.global_epoch.lock().unwrap();
        *g += 1;
        *g
    }
    fn scan(&self, grace: u64) -> usize {
        let new_epoch = self.tick();
        let mut dead = 0;
        for slot in &self.slots {
            let s = slot.lock().unwrap();
            if s.pid == 0 { continue; }
            let lag = new_epoch.saturating_sub(s.last_seen_epoch);
            if lag > grace && s.in_flight_bitmap != 0 {
                dead += 1;
            }
        }
        dead
    }
}

// =========================================================
// scan with 64 slots, all alive
// =========================================================

fn scan_64_all_alive(c: &mut Criterion) {
    let path = tmp("scan-64-alive");
    let t = HeartbeatTable::create(&path, 64).unwrap();
    let mut slots = vec![];
    for i in 0..64 {
        let s = t.register(1000 + i as u32).unwrap();
        t.mark_in_flight(s, (i % 64) as u8);
        t.beat(s);
        slots.push(s);
    }
    let w = FailoverWatchdog::with_grace(&t, 1_000_000);  // never dead
    c.bench_function("failover.scan_64_alive/mmf", |b| {
        b.iter(|| black_box(w.scan()));
    });
    drop(t);
    std::fs::remove_file(&path).ok();

    let naive = NaiveHeartbeatTable::new(64);
    for i in 0..64 {
        naive.register(i, 1000 + i as u32);
        naive.beat(i);
    }
    c.bench_function("failover.scan_64_alive/mutex_naive", |b| {
        b.iter(|| black_box(naive.scan(1_000_000)));
    });
}

// =========================================================
// scan with 64 slots, 4 dead
// =========================================================

fn scan_64_some_dead(c: &mut Criterion) {
    let path = tmp("scan-64-dead");
    let t = HeartbeatTable::create(&path, 64).unwrap();
    let mut slots = vec![];
    for i in 0..64 {
        let s = t.register(1000 + i as u32).unwrap();
        t.mark_in_flight(s, (i % 64) as u8);
        t.beat(s);
        slots.push(s);
    }
    // Tick global epoch 5 times so all slots lag by 5.
    for _ in 0..5 { t.tick_global_epoch(); }
    // Beat every slot except slots 0..4 - those become dead with grace=2.
    for &s in &slots[4..] { t.beat(s); }
    let w = FailoverWatchdog::with_grace(&t, 2);
    c.bench_function("failover.scan_64_4dead/mmf", |b| {
        b.iter(|| black_box(w.scan()));
    });
    drop(t);
    std::fs::remove_file(&path).ok();
}

// =========================================================
// scan with 1024 slots, all alive (large-table cost)
// =========================================================

fn scan_1024_all_alive(c: &mut Criterion) {
    let path = tmp("scan-1024");
    let t = HeartbeatTable::create(&path, 1024).unwrap();
    for i in 0..1024u32 {
        let s = t.register(10000 + i).unwrap();
        t.mark_in_flight(s, (i % 64) as u8);
        t.beat(s);
    }
    let w = FailoverWatchdog::with_grace(&t, 1_000_000);
    c.bench_function("failover.scan_1024_alive/mmf", |b| {
        b.iter(|| black_box(w.scan()));
    });
    drop(t);
    std::fs::remove_file(&path).ok();
}

// =========================================================
// iter_in_flight_bits utility
// =========================================================

fn iter_in_flight_bits_bench(c: &mut Criterion) {
    // Sparse bitmap: bits at 0, 7, 13, 31, 47, 63 set.
    let bm: u64 = (1 << 0) | (1 << 7) | (1 << 13) | (1 << 31) | (1 << 47) | (1 << 63);
    c.bench_function("failover.iter_in_flight_bits_sparse", |b_iter| {
        b_iter.iter(|| {
            let mut sum = 0u32;
            for b in FailoverWatchdog::iter_in_flight_bits(black_box(bm)) {
                sum = sum.wrapping_add(b as u32);
            }
            black_box(sum)
        });
    });
}

criterion_group!(benches,
    scan_64_all_alive,
    scan_64_some_dead,
    scan_1024_all_alive,
    iter_in_flight_bits_bench,
);
criterion_main!(benches);
