//! Bench: `SharedDequeKhpd` (publication-line cache-line amortization)
//! vs `SharedDeque` (Chase-Lev) on the producer-fast workload shape.
//!
//! The KHPD primitive batches `LINE_ITEMS = 3` items per cache-line
//! transfer; the Chase-Lev primitive pushes one item per Release-
//! store on its `bottom` index. The producer-fast bench measures
//! the pure producer-side throughput: K=64 items dispatched per
//! iter, drain runs in the background, and the wait-for-drain-catch-up
//! happens via `iter_custom` OUTSIDE the timed window. This isolates
//! the per-item amortization shape that KHPD targets.
//!
//! # Bench-audit notes
//!
//! - All three contenders ferry an 8-byte payload (a `u64` in
//!   little-endian) per item. KHPD's [`LineItem`] holds 16 bytes;
//!   only the first 8 are used here so the per-item payload size
//!   matches the others.
//! - Same drain shape: one drain thread per backend that spin-yields
//!   between empty reads.
//! - Same `iter_custom` + drain-completion pattern so per-iter
//!   wall-clock measures pure producer throughput.

#![allow(clippy::missing_docs_in_private_items)]

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use parking_lot::Mutex;

use subetha_cxc::{
    LineItem, SharedDeque, SharedDequeKhpd, KhpdSteal,
};

const K_BURST: usize = 64;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-khpd-{name}-{pid}.bin"));
    p
}

/// KHPD producer-fast: stage K items, publish in `K/LINE_ITEMS`
/// batches, drain thread consumes lines off the back.
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

    c.bench_function("shared_deque_khpd.producer_fast/subetha_khpd_64", |b| {
        // Caller-side scratch buffer for the K=64 batch. Allocated
        // once outside the timed loop; the bench's per-iter cost
        // measures only the publish_batch path.
        let mut batch: Vec<LineItem> = Vec::with_capacity(K_BURST);
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for iter_idx in 0..iters {
                let baseline = drained.load(Ordering::Acquire);
                let target = baseline + K_BURST as u64;

                // Canonical KHPD producer-fast shape: build the K
                // items in a caller-side buffer, then call
                // publish_batch() ONCE to publish all of them into
                // ceil(K/LINE_ITEMS) publication lines under one
                // Mutex acquire + one `tail.fetch_add(n_lines)`. The
                // amortization lever is "one Release-store on the
                // line state word publishes LINE_ITEMS items
                // together"; publish_batch is the path that
                // exercises it.
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

    c.bench_function("shared_deque_khpd.producer_fast/subetha_chase_lev_64", |b| {
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
    let deque: Arc<Mutex<VecDeque<u64>>> = Arc::new(Mutex::new(VecDeque::with_capacity(4096)));

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

    c.bench_function("shared_deque_khpd.producer_fast/mutex_vecdeque_64", |b| {
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
    khpd_producer_fast,
    chase_lev_producer_fast,
    mutex_vecdeque_producer_fast
);
criterion_main!(benches);
