//! Multi-thief contention bench: produce K=64 items, drain through
//! N=4 concurrent thieves.
//!
//! Shapes per contender:
//!
//! - **Shared-head primitives** (Chase-Lev / KHL): 4 thieves race
//!   on a shared head via CAS. This is the contention regime.
//! - **Per-mailbox primitives** (URD): 4 thieves drain their own
//!   mailboxes; producer fans out across mailboxes. No CAS
//!   contention on the consumer side.
//!
//! The bench measures **producer-call wall time only** via
//! `iter_custom`; the drain catch-up wait sits OUTSIDE the timed
//! window. Each iter produces K=64 items and waits until all 64
//! items have been observed by the union of N=4 drain threads.
//!
//! ## Bench-audit notes
//!
//! - Same K=64 items per iter for every contender.
//! - Same 4 drain threads per contender. For shared-head primitives
//!   all 4 share the same head; for per-mailbox primitives each
//!   thief gets its own mailbox.
//! - Same `iter_custom` + drain-catch-up shape so per-iter
//!   wall-clock measures pure producer throughput.
//! - Producer pushes the whole batch per iter; drain throughput is
//!   the bottleneck under heavy contention, but the producer's
//!   batch publish cost is what we are measuring.

#![allow(clippy::missing_docs_in_private_items)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_cxc::{
    KhlSteal, LineItem, SharedDeque, SharedDequeKhl, SharedDequeUrd, UrdDrain,
    MAILBOX_ITEMS,
};

const K_BURST: usize = 64;
const N_THIEVES: usize = 4;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-multi-thief-{name}-{pid}.bin"));
    p
}

/// KHL with 4 thieves contending on the shared head.
fn khl_multi_thief(c: &mut Criterion) {
    let path = tmp("khl");
    let deque = Arc::new(SharedDequeKhl::create(&path, 1024).expect("create"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_handles: Vec<_> = (0..N_THIEVES)
        .map(|_| {
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
        })
        .collect();

    c.bench_function("multi_thief.k64_n4/subetha_khl", |b| {
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
    for h in drain_handles {
        h.join().expect("drain");
    }
    drop(deque);
    std::fs::remove_file(&path).ok();
}

/// URD with 4 per-thief mailboxes. Producer fans out via
/// `publish_to(target, ..)` round-robin; each thief drains its own
/// mailbox.
fn urd_multi_thief(c: &mut Criterion) {
    let path = tmp("urd");
    let deque = Arc::new(SharedDequeUrd::create(&path, N_THIEVES).expect("create"));

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_handles: Vec<_> = (0..N_THIEVES)
        .map(|mb_idx| {
            let deque = Arc::clone(&deque);
            let stop = Arc::clone(&stop);
            let drained = Arc::clone(&drained);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match deque.drain_mailbox(mb_idx) {
                        UrdDrain::Success(r) => {
                            drained.fetch_add(r.n_items as u64, Ordering::AcqRel);
                        }
                        UrdDrain::Empty => std::hint::spin_loop(),
                    }
                }
            })
        })
        .collect();

    c.bench_function("multi_thief.k64_n4/subetha_urd", |b| {
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
                        match deque.publish_round_robin(&batch) {
                            Ok((_, n)) => {
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
    for h in drain_handles {
        h.join().expect("drain");
    }
    drop(deque);
    std::fs::remove_file(&path).ok();
}

/// Chase-Lev with 4 thieves contending on the shared top via CAS.
/// Producer pushes K items one at a time.
fn chase_lev_multi_thief(c: &mut Criterion) {
    let path = tmp("cl");
    let owner = Arc::new(SharedDeque::<u64>::create(&path, 1024).expect("create"));
    let thieves: Vec<_> = (0..N_THIEVES)
        .map(|_| {
            Arc::new(
                SharedDeque::<u64>::open_as_thief(&path).expect("open as thief"),
            )
        })
        .collect();

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let drain_handles: Vec<_> = thieves
        .iter()
        .map(|thief| {
            let thief = Arc::clone(thief);
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
        })
        .collect();

    c.bench_function("multi_thief.k64_n4/subetha_chase_lev", |b| {
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
    for h in drain_handles {
        h.join().expect("drain");
    }
    drop(thieves);
    drop(owner);
    std::fs::remove_file(&path).ok();
}

criterion_group!(
    benches,
    urd_multi_thief,
    khl_multi_thief,
    chase_lev_multi_thief,
);
criterion_main!(benches);
