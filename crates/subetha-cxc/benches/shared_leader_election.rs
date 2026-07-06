//! Bench: SharedLeaderElection (try_claim, beat, tick) vs no native
//! cross-process leader-election baseline (the std equivalent
//! requires a coordinator process + IPC). Compared against pure
//! AtomicU32::compare_exchange for the underlying mechanism cost.

use std::hint::black_box;
use std::sync::atomic::{AtomicU32, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_cxc::shared_leader_election::SharedLeaderElection;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-leader-{name}-{pid}.bin"));
    p
}

fn try_claim_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("leader.try_claim");

    // Baseline: pure AtomicU32 CAS loop with the same shape as
    // try_claim_leadership but without the leader-election semantics.
    let cas_baseline = AtomicU32::new(0);
    g.bench_function("baseline_atomic_cas_loop", |b| {
        let my_pid = 42u32;
        b.iter(|| {
            let cur = cas_baseline.load(Ordering::Acquire);
            if cur == my_pid { black_box(true) } else {
                let r = cas_baseline.compare_exchange(
                    cur, my_pid,
                    Ordering::AcqRel, Ordering::Acquire,
                );
                black_box(r.is_ok())
            }
        });
    });

    let path = tmp("try-claim");
    let e = SharedLeaderElection::create(&path).unwrap();
    // Pre-claim so the bench measures the idempotent-success fast path.
    e.try_claim_leadership(42, 3);
    g.bench_function("shared_leader_try_claim_idempotent", |b| {
        b.iter(|| black_box(e.try_claim_leadership(42, 3)));
    });
    drop(e);
    std::fs::remove_file(&path).ok();

    g.finish();
}

fn heartbeat_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("leader.heartbeat");

    let path = tmp("beat");
    let e = SharedLeaderElection::create(&path).unwrap();
    e.try_claim_leadership(42, 3);

    g.bench_function("beat_as_leader_hot", |b| {
        b.iter(|| black_box(e.beat_as_leader(42)));
    });

    g.bench_function("tick_epoch", |b| {
        b.iter(|| black_box(e.tick_epoch()));
    });

    drop(e);
    std::fs::remove_file(&path).ok();

    g.finish();
}

criterion_group!(benches, try_claim_workload, heartbeat_workload);
criterion_main!(benches);
