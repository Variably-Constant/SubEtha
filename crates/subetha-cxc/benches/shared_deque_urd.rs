//! Bench: `SharedDequeUrd` (per-thief mailbox, push-based) vs
//! `SharedDeque` (Chase-Lev, pull-based) across two shapes:
//!
//! - **single thief**: one drain thread. Chase-Lev's CAS on
//!   `top` is uncontended; URD's per-mailbox spin is a serialized
//!   ping-pong (owner-spins-on-EMPTY, thief-spins-on-READY). URD's
//!   architectural feature does not pay off here.
//! - **multi-thief N=4**: four drain threads. Chase-Lev's `top`
//!   CAS hammers under contention (failed CASes scale with N); URD
//!   delivers items into per-thief mailboxes with zero CAS
//!   contention. The architectural win zone.
//!
//! Both bench shapes use the publish-then-wait-for-drain pattern:
//! the K-item batch is dispatched, then the timed window includes
//! a drain catch-up wait so the wall-clock measures dispatch +
//! delivery, not pure producer throughput.
//!
//! # Bench-audit notes
//!
//! - URD uses `MAILBOX_ITEMS = 3` items per mailbox publish; the
//!   K=64 burst becomes ceil(64 / 3) = 22 round-robin publishes
//!   for the single-thief case (every publish targets mailbox 0)
//!   and 6 publishes per mailbox for the N=4 case (round-robin
//!   distributes 22 lines across 4 mailboxes).
//! - Chase-Lev pushes 64 items individually. The thief(s) call
//!   `steal()` in a tight loop.

#![allow(clippy::missing_docs_in_private_items)]

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{
    LineItem, SharedDeque, SharedDequeUrd, UrdDrain, MAILBOX_ITEMS,
};

const K_BURST: usize = 64;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-urd-{name}-{pid}.bin"));
    p
}

// =========================================================
// Single-thief: 1 drain thread.
// =========================================================

fn single_thief(c: &mut Criterion) {
    // --- URD: 1 mailbox, 1 drain thread.
    let urd_path = tmp("urd-st");
    let urd = Arc::new(SharedDequeUrd::create(&urd_path, 1).expect("urd create"));
    let stop = Arc::new(AtomicBool::new(false));
    let drained_urd = Arc::new(AtomicU64::new(0));
    let urd_drain_h = {
        let urd = Arc::clone(&urd);
        let stop = Arc::clone(&stop);
        let d = Arc::clone(&drained_urd);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match urd.drain_mailbox(0) {
                    UrdDrain::Success(r) => {
                        d.fetch_add(r.n_items as u64, Ordering::AcqRel);
                    }
                    UrdDrain::Empty => std::hint::spin_loop(),
                }
            }
        })
    };

    c.bench_function("shared_deque_urd.single_thief/subetha_urd_64", |b| {
        let mut batch: Vec<LineItem> = Vec::with_capacity(MAILBOX_ITEMS);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained_urd.load(Ordering::Acquire);
                let target_count = baseline + K_BURST as u64;
                let start = Instant::now();
                let mut emitted = 0usize;
                while emitted < K_BURST {
                    let want = MAILBOX_ITEMS.min(K_BURST - emitted);
                    batch.clear();
                    for j in 0..want {
                        let id = iter_idx * (K_BURST as u64) + (emitted + j) as u64;
                        batch.push(LineItem::new(&(id as u32).to_le_bytes()).unwrap());
                    }
                    loop {
                        match urd.publish_to(0, &batch) {
                            Ok(n) => { emitted += n; break; }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                }
                while drained_urd.load(Ordering::Acquire) < target_count {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                black_box(emitted);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    urd_drain_h.join().expect("urd drain");
    drop(urd);
    std::fs::remove_file(&urd_path).ok();

    // --- Chase-Lev: capacity 1024, 1 drain thread.
    let cl_path = tmp("cl-st");
    let cl_owner = Arc::new(SharedDeque::<u64>::create(&cl_path, 1024).expect("cl create"));
    let cl_thief = Arc::new(SharedDeque::<u64>::open_as_thief(&cl_path).expect("cl thief"));
    let stop = Arc::new(AtomicBool::new(false));
    let drained_cl = Arc::new(AtomicU64::new(0));
    let cl_drain_h = {
        let thief = Arc::clone(&cl_thief);
        let stop = Arc::clone(&stop);
        let d = Arc::clone(&drained_cl);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if thief.steal().is_some() {
                    d.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
        })
    };

    c.bench_function("shared_deque_urd.single_thief/subetha_chase_lev_64", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained_cl.load(Ordering::Acquire);
                let target_count = baseline + K_BURST as u64;
                let start = Instant::now();
                for i in 0..K_BURST as u64 {
                    let id = iter_idx * (K_BURST as u64) + i;
                    while cl_owner.push(&id).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while drained_cl.load(Ordering::Acquire) < target_count {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    cl_drain_h.join().expect("cl drain");
    drop(cl_thief);
    drop(cl_owner);
    std::fs::remove_file(&cl_path).ok();
}

// =========================================================
// Multi-thief N=4: 4 drain threads sharing the load.
// =========================================================

fn multi_thief_4(c: &mut Criterion) {
    // --- URD: 4 mailboxes, 4 drain threads (each on its own
    // mailbox). Owner distributes round-robin.
    let urd_path = tmp("urd-mt4");
    let urd = Arc::new(SharedDequeUrd::create(&urd_path, 4).expect("urd create"));
    let stop = Arc::new(AtomicBool::new(false));
    let drained_urd = Arc::new(AtomicU64::new(0));
    let mut urd_drain_hs = Vec::with_capacity(4);
    for mb in 0..4 {
        let urd = Arc::clone(&urd);
        let stop = Arc::clone(&stop);
        let d = Arc::clone(&drained_urd);
        urd_drain_hs.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match urd.drain_mailbox(mb) {
                    UrdDrain::Success(r) => {
                        d.fetch_add(r.n_items as u64, Ordering::AcqRel);
                    }
                    UrdDrain::Empty => std::hint::spin_loop(),
                }
            }
        }));
    }

    c.bench_function("shared_deque_urd.multi_thief_4/subetha_urd_64", |b| {
        let mut batch: Vec<LineItem> = Vec::with_capacity(MAILBOX_ITEMS);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained_urd.load(Ordering::Acquire);
                let target_count = baseline + K_BURST as u64;
                let start = Instant::now();
                let mut emitted = 0usize;
                while emitted < K_BURST {
                    let want = MAILBOX_ITEMS.min(K_BURST - emitted);
                    batch.clear();
                    for j in 0..want {
                        let id = iter_idx * (K_BURST as u64) + (emitted + j) as u64;
                        batch.push(LineItem::new(&(id as u32).to_le_bytes()).unwrap());
                    }
                    loop {
                        match urd.publish_round_robin(&batch) {
                            Ok((_, n)) => { emitted += n; break; }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                }
                while drained_urd.load(Ordering::Acquire) < target_count {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                black_box(emitted);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    for h in urd_drain_hs { h.join().expect("urd drain"); }
    drop(urd);
    std::fs::remove_file(&urd_path).ok();

    // --- Chase-Lev: capacity 1024, 4 drain threads stealing the
    // single shared head. Each failed CAS retries via the
    // `steal()` Retry path.
    let cl_path = tmp("cl-mt4");
    let cl_owner = Arc::new(SharedDeque::<u64>::create(&cl_path, 1024).expect("cl create"));
    let stop = Arc::new(AtomicBool::new(false));
    let drained_cl = Arc::new(AtomicU64::new(0));
    let mut cl_drain_hs = Vec::with_capacity(4);
    for _ in 0..4 {
        let path = cl_path.clone();
        let stop = Arc::clone(&stop);
        let d = Arc::clone(&drained_cl);
        cl_drain_hs.push(std::thread::spawn(move || {
            let thief = SharedDeque::<u64>::open_as_thief(&path).expect("cl thief");
            while !stop.load(Ordering::Acquire) {
                if thief.steal().is_some() {
                    d.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
        }));
    }

    c.bench_function("shared_deque_urd.multi_thief_4/subetha_chase_lev_64", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained_cl.load(Ordering::Acquire);
                let target_count = baseline + K_BURST as u64;
                let start = Instant::now();
                for i in 0..K_BURST as u64 {
                    let id = iter_idx * (K_BURST as u64) + i;
                    while cl_owner.push(&id).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while drained_cl.load(Ordering::Acquire) < target_count {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    for h in cl_drain_hs { h.join().expect("cl drain"); }
    drop(cl_owner);
    std::fs::remove_file(&cl_path).ok();
}

criterion_group!(benches, single_thief, multi_thief_4);
criterion_main!(benches);
