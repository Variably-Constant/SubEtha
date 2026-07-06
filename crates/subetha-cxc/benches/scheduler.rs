//! Bench: BackgroundScheduler submit + recv + watchdog_scan hot
//! paths vs `mpsc::SyncSender<Pass>` and a direct closure call.
//!
//! The scheduler's architectural claim: cross-process Pass
//! dispatch with auto-failover (via heartbeat scan), at per-op
//! cost competitive with the in-process mpsc baseline. The
//! cross-process visibility, durable ring (file IS the queue),
//! and failover-watchdog are what neither baseline can do at any
//! cost.
//!
//! Workloads (worker pre-spawned via BackgroundScheduler::start):
//! - submitter.submit (encode + ring push)
//! - collector.try_recv on a pre-drained result (ring pop + decode)
//! - watchdog_scan with 8-slot heartbeat
//! - mpsc::SyncSender<Pass>::send (in-process baseline)

use std::hint::black_box;
use std::sync::mpsc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{BackgroundScheduler, Pass, pass_registry};

fn tmp(name: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let mut s = std::env::temp_dir();
    let pid = std::process::id();
    s.push(format!("subetha-bench-sched-{name}-{pid}-submit.bin"));
    let mut r = std::env::temp_dir();
    r.push(format!("subetha-bench-sched-{name}-{pid}-result.bin"));
    let mut h = std::env::temp_dir();
    h.push(format!("subetha-bench-sched-{name}-{pid}-hb.bin"));
    (s, r, h)
}

fn cleanup(paths: &(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)) {
    std::fs::remove_file(&paths.0).ok();
    std::fs::remove_file(&paths.1).ok();
    std::fs::remove_file(&paths.2).ok();
}

const BENCH_CLOSURE_ID: u32 = 0xA000_0001;

// =========================================================
// submit hot path (encode + ring push, with worker draining)
// =========================================================

fn submit_hot(c: &mut Criterion) {
    pass_registry::register(BENCH_CLOSURE_ID, |args| Ok(args.to_vec()));

    let paths = tmp("submit");
    let sched = BackgroundScheduler::start(
        &paths.0, &paths.1, &paths.2, 1024, 8,
    ).unwrap();
    let submitter = sched.submitter();
    let collector = sched.collector();

    c.bench_function("sched.submit/mmf", |b| {
        b.iter(|| {
            submitter.submit(black_box(&Pass {
                closure_id: BENCH_CLOSURE_ID,
                args: vec![1, 2, 3, 4],
            })).ok();
            // Opportunistic drain so the result-ring doesn't fill.
            collector.try_recv().ok();
        });
    });
    drop(sched);
    pass_registry::unregister(BENCH_CLOSURE_ID);
    cleanup(&paths);

    // mpsc baseline: synchronous send with a worker thread draining.
    let (tx, rx) = mpsc::sync_channel::<Pass>(1024);
    let drainer = std::thread::spawn(move || {
        while rx.recv().is_ok() {}
    });
    c.bench_function("sched.submit/mpsc_sync_channel", |b| {
        b.iter(|| {
            tx.send(black_box(Pass {
                closure_id: BENCH_CLOSURE_ID,
                args: vec![1, 2, 3, 4],
            })).unwrap();
        });
    });
    drop(tx);
    drainer.join().unwrap();
}

// =========================================================
// recv hot path (ring pop + decode)
// =========================================================

fn recv_hot(c: &mut Criterion) {
    pass_registry::register(BENCH_CLOSURE_ID, |args| Ok(args.to_vec()));

    let paths = tmp("recv");
    let sched = BackgroundScheduler::start(
        &paths.0, &paths.1, &paths.2, 1024, 8,
    ).unwrap();
    let submitter = sched.submitter();
    let collector = sched.collector();
    let submitter_for_iter = sched.submitter();

    c.bench_function("sched.try_recv/mmf", |b| {
        b.iter(|| {
            // Keep the worker fed.
            submitter_for_iter.submit(&Pass {
                closure_id: BENCH_CLOSURE_ID,
                args: vec![5, 6, 7, 8],
            }).ok();
            black_box(collector.try_recv().ok());
        });
    });
    drop(submitter);
    drop(sched);
    pass_registry::unregister(BENCH_CLOSURE_ID);
    cleanup(&paths);
}

// =========================================================
// watchdog_scan (heartbeat scan for failover)
// =========================================================

fn watchdog_scan_hot(c: &mut Criterion) {
    pass_registry::register(BENCH_CLOSURE_ID, |args| Ok(args.to_vec()));

    let paths = tmp("watchdog");
    let sched = BackgroundScheduler::start(
        &paths.0, &paths.1, &paths.2, 64, 8,
    ).unwrap();
    c.bench_function("sched.watchdog_scan/mmf", |b| {
        b.iter(|| black_box(sched.watchdog_scan()));
    });
    drop(sched);
    pass_registry::unregister(BENCH_CLOSURE_ID);
    cleanup(&paths);
}

criterion_group!(benches,
    submit_hot,
    recv_hot,
    watchdog_scan_hot,
);
criterion_main!(benches);
