//! Bench: all four MMF-deque variants against each other on the
//! producer-fast workload shape.
//!
//! Contenders (all use single-drain-thread + K=64 items per iter):
//!
//! - [`SharedDequeLoh`]: LCRQ-on-LIFO Hybrid; hot path is
//!   [`SharedDequeLoh::publish_batch`] (one Mutex acquire + one
//!   `tail.fetch_add(K)` + K Release-stores).
//! - [`SharedDequeKhpd`]: publication-line; hot path is
//!   [`SharedDequeKhpd::publish_batch`] (one Mutex acquire +
//!   `K/LINE_ITEMS` publication lines).
//! - [`SharedDeque`]: Chase-Lev; per-item push (one Release-store on
//!   `bottom` per call).
//! - [`SharedDequeUrd`]: per-thief mailbox + WAITPKG or PAUSE-spin;
//!   hot path is `publish_to(0, &batch)` repeated `K / MAILBOX_ITEMS`
//!   times against a single thief mailbox.
//! - `Mutex<VecDeque<u64>>`: contention-tolerant baseline.
//!
//! The producer-fast bench measures the pure producer-side
//! throughput: K=64 items dispatched per iter, drain runs in the
//! background, and the wait-for-drain-catch-up happens via
//! `iter_custom` OUTSIDE the timed window. This isolates the
//! per-item amortization shape that LOH targets.
//!
//! # Bench-audit notes
//!
//! - All five contenders ferry an 8-byte payload (a `u64` in
//!   little-endian) per item; the three byte-oriented variants share
//!   the [`LineItem`] type so per-item marshal cost matches.
//! - LOH uses its canonical hot-path API
//!   [`SharedDequeLoh::publish_batch`] which bypasses the local LIFO
//!   and pays exactly one Mutex acquire + one
//!   `tail.fetch_add(K)` + K Release-stores per call. This is the
//!   path that exercises the architectural lever; staging via
//!   `push` would defeat the amortization.
//! - KHPD uses `publish_batch` similarly so the comparison reflects
//!   each variant's canonical batch API.
//! - URD's `publish_to(0, &batch)` is capped at `MAILBOX_ITEMS = 3`
//!   per call, so K=64 becomes ceil(64/3) = 22 publishes into the
//!   single mailbox. This is the same shape used in the URD
//!   single-thief bench.
//! - Same drain shape: one drain thread per backend that spin-yields
//!   between empty reads.
//! - Same `iter_custom` + drain-completion pattern so per-iter
//!   wall-clock measures pure producer throughput.
//! - URD's architectural win zone is multi-thief contention (see
//!   the dedicated `shared_deque_urd` bench). In this single-thief
//!   comparison URD's batched publish gives a meaningful read but
//!   does not exercise its strongest win zone.

#![allow(clippy::missing_docs_in_private_items)]

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use parking_lot::Mutex;

use subetha_cxc::{
    KhlSteal, KhpdSteal, LineItem, LohSteal, SharedDeque, SharedDequeFcl,
    SharedDequeKhl, SharedDequeKhpd, SharedDequeLoh, SharedDequeUrd, UrdDrain,
    MAILBOX_ITEMS,
};

const K_BURST: usize = 64;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-loh-{name}-{pid}.bin"));
    p
}

/// LOH producer-fast: one `publish_batch(K)` per iter; drain thread
/// consumes slots off the back.
fn loh_producer_fast(c: &mut Criterion) {
    let path = tmp("loh-pf");
    // flush_threshold = usize::MAX so the per-item LIFO path never
    // auto-flushes; the bench drives publish_batch directly.
    let deque = Arc::new(
        SharedDequeLoh::create(&path, 1024, usize::MAX).expect("create"),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let deque = Arc::clone(&deque);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match deque.steal() {
                    LohSteal::Success(_) => {
                        drained.fetch_add(1, Ordering::AcqRel);
                    }
                    LohSteal::Empty | LohSteal::Retry => {
                        std::hint::spin_loop();
                    }
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/subetha_loh_64", |b| {
        // Caller-side scratch buffer for the K=64 batch. Allocated
        // once outside the timed loop; the bench's per-iter cost
        // measures only the publish_batch path.
        let mut batch: Vec<LineItem> = Vec::with_capacity(K_BURST);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                // Canonical LOH producer-fast shape: build the K
                // items in a caller-side buffer, then call
                // publish_batch() ONCE to migrate them with one
                // Mutex acquire + one `tail.fetch_add(K)` + K
                // Release-stores on per-slot sequence numbers.
                // The amortization lever is "one producer-counter
                // atomic per K items"; publish_batch is the path
                // that exercises it.
                batch.clear();
                for i in 0..K_BURST as u32 {
                    let id = (iter_idx as u32) * (K_BURST as u32) + i;
                    batch.push(LineItem::new(&id.to_le_bytes()).unwrap());
                }

                let start = Instant::now();
                loop {
                    match deque.publish_batch(&batch) {
                        Ok(_) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
                total += start.elapsed();

                // Drain catch-up OUTSIDE the timed window.
                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("drain");
    drop(deque);
    std::fs::remove_file(&path).ok();
}

/// Fcl (Fat Chase-Lev, counter-only with K_inner=3) producer-fast:
/// the K_gating middle-ground primitive - Chase-Lev's counter-only
/// protocol with 3 items per cache-line slot. No per-slot atomic.
/// Per K=64: 1 top load + 22 cache-line writes + 1 Release fence +
/// 1 Relaxed bottom store = 24 ops total, of which 22 are writes.
fn fcl_producer_fast(c: &mut Criterion) {
    let path = tmp("fcl-pf");
    let owner = Arc::new(SharedDequeFcl::create(&path, 1024).expect("create"));
    let thief = Arc::new(SharedDequeFcl::open(&path).expect("thief"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let thief = Arc::clone(&thief);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match thief.steal_slot() {
                    Some(fat) => {
                        drained.fetch_add(fat.n_items as u64, Ordering::AcqRel);
                    }
                    None => std::hint::spin_loop(),
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/subetha_fcl_64", |b| {
        let mut batch: Vec<LineItem> = Vec::with_capacity(K_BURST);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                batch.clear();
                for i in 0..K_BURST as u32 {
                    let id = (iter_idx as u32) * (K_BURST as u32) + i;
                    batch.push(LineItem::new(&id.to_le_bytes()).unwrap());
                }

                let start = Instant::now();
                loop {
                    match owner.publish_batch(&batch) {
                        Ok(_) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
                total += start.elapsed();

                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("drain");
    drop(thief);
    drop(owner);
    std::fs::remove_file(&path).ok();
}

/// KHL (K-axis Hierarchical LCRQ, the SubEtha-native hybrid) producer-
/// fast: one publish_batch(K=64) pays 22 slot Release-stores + 1
/// Release-store on owner-private tail. Pulls KHPD's per-slot
/// amortization + LOH's per-batch amortization + Chase-Lev's owner-
/// private counter all at once.
fn khl_producer_fast(c: &mut Criterion) {
    let path = tmp("khl-pf");
    let deque = Arc::new(SharedDequeKhl::create(&path, 1024).expect("create"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let deque = Arc::clone(&deque);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match deque.steal_slot() {
                    KhlSteal::Success(r) => {
                        drained.fetch_add(r.n_items as u64, Ordering::AcqRel);
                    }
                    KhlSteal::Empty | KhlSteal::Retry => {
                        std::hint::spin_loop();
                    }
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/subetha_khl_64", |b| {
        let mut batch: Vec<LineItem> = Vec::with_capacity(K_BURST);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                batch.clear();
                for i in 0..K_BURST as u32 {
                    let id = (iter_idx as u32) * (K_BURST as u32) + i;
                    batch.push(LineItem::new(&id.to_le_bytes()).unwrap());
                }

                let start = Instant::now();
                loop {
                    match deque.publish_batch(&batch) {
                        Ok(_) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
                total += start.elapsed();

                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("drain");
    drop(deque);
    std::fs::remove_file(&path).ok();
}

/// KHPD producer-fast: stage K items, publish in `K/LINE_ITEMS`
/// publication lines via publish_batch; drain thread consumes lines.
fn khpd_producer_fast(c: &mut Criterion) {
    let path = tmp("khpd-pf");
    let deque = Arc::new(SharedDequeKhpd::create(&path, 1024).expect("create"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let deque = Arc::clone(&deque);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match deque.steal_line() {
                    KhpdSteal::Success(r) => {
                        drained.fetch_add(r.n_items as u64, Ordering::AcqRel);
                    }
                    KhpdSteal::Empty | KhpdSteal::Retry => {
                        std::hint::spin_loop();
                    }
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/subetha_khpd_64", |b| {
        let mut batch: Vec<LineItem> = Vec::with_capacity(K_BURST);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                batch.clear();
                for i in 0..K_BURST as u32 {
                    let id = (iter_idx as u32) * (K_BURST as u32) + i;
                    batch.push(LineItem::new(&id.to_le_bytes()).unwrap());
                }

                let start = Instant::now();
                loop {
                    match deque.publish_batch(&batch) {
                        Ok(_) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
                total += start.elapsed();

                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("drain");
    drop(deque);
    std::fs::remove_file(&path).ok();
}

/// URD producer-fast: single mailbox, single drain thread, K items
/// per iter delivered in ceil(K / MAILBOX_ITEMS) `publish_to(0, ..)`
/// calls.
fn urd_producer_fast(c: &mut Criterion) {
    let path = tmp("urd-pf");
    let deque = Arc::new(SharedDequeUrd::create(&path, 1).expect("urd create"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let deque = Arc::clone(&deque);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match deque.drain_mailbox(0) {
                    UrdDrain::Success(r) => {
                        drained.fetch_add(r.n_items as u64, Ordering::AcqRel);
                    }
                    UrdDrain::Empty => std::hint::spin_loop(),
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/subetha_urd_64", |b| {
        let mut batch: Vec<LineItem> = Vec::with_capacity(MAILBOX_ITEMS);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                let start = Instant::now();
                let mut emitted = 0usize;
                while emitted < K_BURST {
                    let want = MAILBOX_ITEMS.min(K_BURST - emitted);
                    batch.clear();
                    for j in 0..want {
                        let id = iter_idx * (K_BURST as u64) + (emitted + j) as u64;
                        batch.push(
                            LineItem::new(&(id as u32).to_le_bytes()).unwrap(),
                        );
                    }
                    loop {
                        match deque.publish_to(0, &batch) {
                            Ok(n) => {
                                emitted += n;
                                break;
                            }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                }
                total += start.elapsed();

                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(emitted);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("urd drain");
    drop(deque);
    std::fs::remove_file(&path).ok();
}

/// Chase-Lev SharedDeque producer-fast: push K items per iter.
fn chase_lev_producer_fast(c: &mut Criterion) {
    let path = tmp("cl-pf");
    let owner = Arc::new(SharedDeque::<u64>::create(&path, 1024).expect("create"));
    let thief = Arc::new(SharedDeque::<u64>::open_as_thief(&path).expect("thief"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let thief = Arc::clone(&thief);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if thief.steal().is_some() {
                    drained.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/subetha_chase_lev_64", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                let start = Instant::now();
                for i in 0..K_BURST as u64 {
                    let id = iter_idx * (K_BURST as u64) + i;
                    while owner.push(&id).is_err() {
                        std::hint::spin_loop();
                    }
                }
                total += start.elapsed();

                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("drain");
    drop(thief);
    drop(owner);
    std::fs::remove_file(&path).ok();
}

/// `Mutex<VecDeque<u64>>` baseline.
fn mutex_vecdeque_producer_fast(c: &mut Criterion) {
    let deque: Arc<Mutex<VecDeque<u64>>> =
        Arc::new(Mutex::new(VecDeque::with_capacity(4096)));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_h = {
        let deque = Arc::clone(&deque);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let v = deque.lock().pop_front();
                if v.is_some() {
                    drained.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
        })
    };

    c.bench_function("shared_deque_loh.producer_fast/mutex_vecdeque_64", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                let start = Instant::now();
                {
                    let mut g = deque.lock();
                    for i in 0..K_BURST as u64 {
                        g.push_back(iter_idx * (K_BURST as u64) + i);
                    }
                }
                total += start.elapsed();

                while drained.load(Ordering::Acquire) < target {
                    std::hint::spin_loop();
                }
                black_box(K_BURST);
            }
            total
        });
    });

    stop.store(true, Ordering::Release);
    drain_h.join().expect("drain");
}

criterion_group!(
    benches,
    fcl_producer_fast,
    khl_producer_fast,
    loh_producer_fast,
    khpd_producer_fast,
    urd_producer_fast,
    chase_lev_producer_fast,
    mutex_vecdeque_producer_fast
);
criterion_main!(benches);
