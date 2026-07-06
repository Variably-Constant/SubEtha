//! Bench: EpochBarrier vs std::sync::Barrier (the in-process
//! baseline most code reaches for).
//!
//! Architectural claim: EpochBarrier handles N participants across
//! processes AND tolerates dead peers via heartbeat exclusion, at
//! a per-wait cost comparable to or better than std::sync::Barrier's
//! Mutex+Condvar pattern. The in-process baseline cannot do either
//! capability at any cost.
//!
//! Workloads:
//! - current_epoch observer (hot read)
//! - wait_quorum(0, 1) single-participant immediate-release fast path
//! - live_peer_count heartbeat scan
//! - 4-thread barrier release (pre-spawned workers; gate-sync only)
//!
//! Pre-spawned worker pattern: a per-iter `thread::spawn` would be
//! dominated by Windows thread-creation cost (~50-100 us). Instead
//! workers are spawned once and signaled via a cyclic
//! `std::sync::Barrier` (kick_gate + done_gate). Both contenders
//! pay the same two std::sync::Barrier ops so the ratio comparison
//! isolates the EpochBarrier-vs-std::sync::Barrier cost.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Barrier as StdBarrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{EpochBarrier, HeartbeatTable};

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-barrier-{name}-{pid}"));
    p
}
fn tmp_hb(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-barrier-hb-{name}-{pid}.bin"));
    p
}
fn cleanup_base(base: &std::path::Path, hb: &std::path::Path) {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.state.bin"));
    std::fs::remove_file(&p).ok();
    std::fs::remove_file(hb).ok();
}

// =========================================================
// current_epoch observer
// =========================================================

fn observer_current_epoch(c: &mut Criterion) {
    let base = tmp_base("obs");
    let hb_p = tmp_hb("obs");
    let hb = Arc::new(HeartbeatTable::create(&hb_p, 4).unwrap());
    let barrier = EpochBarrier::create(&base, hb.clone(), 10).unwrap();
    c.bench_function("barrier.current_epoch/mmf", |b| {
        b.iter(|| black_box(barrier.current_epoch()));
    });
    drop(barrier);
    drop(hb);
    cleanup_base(&base, &hb_p);
}

// =========================================================
// wait_quorum(0, 1) single-participant immediate release
// =========================================================

fn wait_quorum_single(c: &mut Criterion) {
    let base = tmp_base("wq1");
    let hb_p = tmp_hb("wq1");
    let hb = Arc::new(HeartbeatTable::create(&hb_p, 4).unwrap());
    let s = hb.register(99000).unwrap();
    hb.beat(s);
    let barrier = EpochBarrier::create(&base, hb.clone(), 100).unwrap();
    c.bench_function("barrier.wait_quorum_1/mmf", |b_iter| {
        b_iter.iter(|| {
            let e = barrier.current_epoch();
            barrier.wait_quorum(e, 1).unwrap();
        });
    });
    drop(barrier);
    drop(hb);
    cleanup_base(&base, &hb_p);
}

// =========================================================
// live_peer_count heartbeat scan (8 slots, 4 live)
// =========================================================

fn live_peer_count_scan(c: &mut Criterion) {
    let base = tmp_base("lpc");
    let hb_p = tmp_hb("lpc");
    let hb = Arc::new(HeartbeatTable::create(&hb_p, 8).unwrap());
    for i in 0..4 {
        let s = hb.register(50000 + i).unwrap();
        hb.beat(s);
    }
    let barrier = EpochBarrier::create(&base, hb.clone(), 100).unwrap();
    c.bench_function("barrier.live_peer_count/mmf", |b| {
        b.iter(|| black_box(barrier.live_peer_count()));
    });
    drop(barrier);
    drop(hb);
    cleanup_base(&base, &hb_p);
}

// =========================================================
// 4-thread barrier release (pre-spawned workers)
// =========================================================

fn release_4_threads(c: &mut Criterion) {
    // ---- EpochBarrier ----
    let base = tmp_base("rel4");
    let hb_p = tmp_hb("rel4");
    let hb = Arc::new(HeartbeatTable::create(&hb_p, 4).unwrap());
    for i in 0..4 {
        let s = hb.register(10000 + i as u32).unwrap();
        hb.beat(s);
    }
    let barrier = Arc::new(EpochBarrier::create(&base, hb.clone(), 100).unwrap());
    let kick_gate = Arc::new(StdBarrier::new(5));   // 4 workers + main
    let done_gate = Arc::new(StdBarrier::new(5));
    let shutdown = Arc::new(AtomicBool::new(false));
    let next_epoch = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];
    for _ in 0..4 {
        let b = barrier.clone();
        let kg = kick_gate.clone();
        let dg = done_gate.clone();
        let sd = shutdown.clone();
        let ne = next_epoch.clone();
        handles.push(thread::spawn(move || {
            loop {
                kg.wait();
                if sd.load(Ordering::Acquire) { break; }
                let e = ne.load(Ordering::Acquire);
                b.wait(e).unwrap();
                dg.wait();
            }
        }));
    }

    c.bench_function("barrier.release_4t/mmf", |b_iter| {
        b_iter.iter(|| {
            next_epoch.store(barrier.current_epoch(), Ordering::Release);
            kick_gate.wait();
            done_gate.wait();
        });
    });

    shutdown.store(true, Ordering::Release);
    kick_gate.wait();
    for h in handles { h.join().unwrap(); }
    drop(barrier);
    drop(hb);
    cleanup_base(&base, &hb_p);

    // ---- std::sync::Barrier ----
    // Pre-spawn 4 std-barrier workers and gate them identically.
    let std_b = Arc::new(StdBarrier::new(4));
    let kick_gate2 = Arc::new(StdBarrier::new(5));
    let done_gate2 = Arc::new(StdBarrier::new(5));
    let shutdown2 = Arc::new(AtomicBool::new(false));
    let mut handles2 = vec![];
    for _ in 0..4 {
        let b = std_b.clone();
        let kg = kick_gate2.clone();
        let dg = done_gate2.clone();
        let sd = shutdown2.clone();
        handles2.push(thread::spawn(move || {
            loop {
                kg.wait();
                if sd.load(Ordering::Acquire) { break; }
                b.wait();
                dg.wait();
            }
        }));
    }
    c.bench_function("barrier.release_4t/std_barrier", |b_iter| {
        b_iter.iter(|| {
            kick_gate2.wait();
            done_gate2.wait();
        });
    });
    shutdown2.store(true, Ordering::Release);
    kick_gate2.wait();
    for h in handles2 { h.join().unwrap(); }
}

criterion_group!(benches,
    observer_current_epoch,
    wait_quorum_single,
    live_peer_count_scan,
    release_4_threads,
);
criterion_main!(benches);
