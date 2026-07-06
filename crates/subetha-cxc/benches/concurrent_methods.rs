//! Bench: cross-thread throughput comparison across concurrency
//! primitives.
//!
//! Six contenders, all running the same SPSC pattern: one producer
//! thread pushes K=10000 u64 items, one consumer thread drains
//! them all, criterion measures wall-clock time from push-start
//! to drain-complete.
//!
//! - `AdaptiveIpc<u64>` (SubEtha, MMF-backed, cross-process visible)
//! - `SharedRing` direct (SubEtha, MMF-backed, no AdaptiveIpc layer)
//! - `crossbeam_channel::bounded(1024)` (in-process MPMC)
//! - `crossbeam_channel::unbounded` (in-process MPMC, no flow control)
//! - `std::sync::mpsc::sync_channel(1024)` (stdlib bounded MPSC)
//! - `parking_lot::Mutex<VecDeque<u64>>` (mutex baseline)
//!
//! ## Bench audit
//!
//! Same payload type (u64 = 8 bytes) across all contenders. Same
//! producer / consumer thread pair pre-spawned (no thread-startup
//! cost in the timed window). Same K=10000 items per iteration.
//! The MMF contenders pay Marshal encode + atomic-protocol cost;
//! the channel contenders carry typed `u64` natively. That's the
//! honest cost difference between "cross-process visible" and
//! "in-process only". Both are reported.

#![allow(clippy::missing_docs_in_private_items)]

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use crossbeam_channel as cbc;
use parking_lot::Mutex;

use subetha_cxc::{
    AdaptiveIpc, Channel, MmfWorkloadShape, SharedRing,
};
use subetha_cxc::shared_ring::PAYLOAD_BYTES;

const K_ITEMS: u64 = 10_000;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha-bench-concurrent-{name}-{pid}-{nonce}"));
    p
}

// ============================================================
// Contender 1: AdaptiveIpc<u64>  (SubEtha, MMF, cross-process)
// ============================================================
fn bench_adaptive_ipc(c: &mut Criterion) {
    let path = tmp("adaptive");
    let shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(&path, shape, 16384, 1).expect("create"),
    );

    c.bench_function("concurrent_methods/spsc_10k/adaptive_ipc", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // Spawn drain thread (cheap; mmap is reused).
                let ipc_c = Arc::clone(&ipc);
                let stop = Arc::new(AtomicBool::new(false));
                let counted = Arc::new(AtomicU64::new(0));
                let stop_c = Arc::clone(&stop);
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while !stop_c.load(Ordering::Acquire) {
                        match ipc_c.recv() {
                            Ok(v) => {
                                black_box(v);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                });
                let start = Instant::now();
                for i in 0..K_ITEMS {
                    while ipc.send(&i).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                drain.join().ok();
            }
            total
        });
    });

    drop(ipc);
    // cleanup
    for suffix in &[".v0.bin", ".v1.bin", ".ctl.bin"] {
        let mut p = path.clone();
        let s = p.file_name().map(|s| s.to_owned()).unwrap_or_default();
        p.set_file_name(format!("{}{suffix}", s.to_string_lossy()));
        std::fs::remove_file(&p).ok();
    }
}

// ============================================================
// Contender 2: SharedRing direct (SubEtha, MMF, no Adaptive)
// ============================================================
fn bench_shared_ring_direct(c: &mut Criterion) {
    let path = tmp("ring");
    let ring = Arc::new(SharedRing::create(&path, 16384).expect("create"));

    c.bench_function("concurrent_methods/spsc_10k/shared_ring_direct", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let ring_c = Arc::clone(&ring);
                let stop = Arc::new(AtomicBool::new(false));
                let counted = Arc::new(AtomicU64::new(0));
                let stop_c = Arc::clone(&stop);
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    let mut out = [0u8; PAYLOAD_BYTES];
                    while !stop_c.load(Ordering::Acquire) {
                        if ring_c.try_pop(&mut out).is_ok() {
                            black_box(&out);
                            counted_c.fetch_add(1, Ordering::AcqRel);
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });
                let start = Instant::now();
                let mut buf = [0u8; PAYLOAD_BYTES];
                for i in 0..K_ITEMS {
                    buf[..8].copy_from_slice(&i.to_le_bytes());
                    while ring.try_push(&buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                drain.join().ok();
            }
            total
        });
    });

    drop(ring);
    std::fs::remove_file(&path).ok();
}

// ============================================================
// Contender 3: crossbeam_channel::bounded
// ============================================================
fn bench_crossbeam_bounded(c: &mut Criterion) {
    c.bench_function("concurrent_methods/spsc_10k/crossbeam_bounded", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = cbc::bounded::<u64>(16384);
                let counted = Arc::new(AtomicU64::new(0));
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while let Ok(v) = rx.recv() {
                        black_box(v);
                        counted_c.fetch_add(1, Ordering::AcqRel);
                    }
                });
                let start = Instant::now();
                for i in 0..K_ITEMS {
                    tx.send(i).expect("send");
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                drop(tx);
                drain.join().ok();
            }
            total
        });
    });
}

// ============================================================
// Contender 4: crossbeam_channel::unbounded
// ============================================================
fn bench_crossbeam_unbounded(c: &mut Criterion) {
    c.bench_function("concurrent_methods/spsc_10k/crossbeam_unbounded", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = cbc::unbounded::<u64>();
                let counted = Arc::new(AtomicU64::new(0));
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while let Ok(v) = rx.recv() {
                        black_box(v);
                        counted_c.fetch_add(1, Ordering::AcqRel);
                    }
                });
                let start = Instant::now();
                for i in 0..K_ITEMS {
                    tx.send(i).expect("send");
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                drop(tx);
                drain.join().ok();
            }
            total
        });
    });
}

// ============================================================
// Contender 5: std::sync::mpsc::sync_channel
// ============================================================
fn bench_std_sync_channel(c: &mut Criterion) {
    c.bench_function("concurrent_methods/spsc_10k/std_sync_channel", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(16384);
                let counted = Arc::new(AtomicU64::new(0));
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while let Ok(v) = rx.recv() {
                        black_box(v);
                        counted_c.fetch_add(1, Ordering::AcqRel);
                    }
                });
                let start = Instant::now();
                for i in 0..K_ITEMS {
                    tx.send(i).expect("send");
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                drop(tx);
                drain.join().ok();
            }
            total
        });
    });
}

// ============================================================
// Contender 6: parking_lot::Mutex<VecDeque<u64>>
// ============================================================
fn bench_mutex_vecdeque(c: &mut Criterion) {
    c.bench_function("concurrent_methods/spsc_10k/mutex_vecdeque", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let q = Arc::new(Mutex::new(VecDeque::<u64>::with_capacity(16384)));
                let stop = Arc::new(AtomicBool::new(false));
                let counted = Arc::new(AtomicU64::new(0));
                let q_c = Arc::clone(&q);
                let stop_c = Arc::clone(&stop);
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while !stop_c.load(Ordering::Acquire) {
                        let popped = q_c.lock().pop_front();
                        match popped {
                            Some(v) => {
                                black_box(v);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            }
                            None => std::hint::spin_loop(),
                        }
                    }
                });
                let start = Instant::now();
                for i in 0..K_ITEMS {
                    q.lock().push_back(i);
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                drain.join().ok();
            }
            total
        });
    });
}

// ============================================================
// Contender 7: Channel<u64> (typed-intent direct, SubEtha)
// ============================================================
fn bench_channel_direct(c: &mut Criterion) {
    let path = tmp("channel");
    let shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let chan: Arc<Channel<u64>> =
        Arc::new(Channel::create(&path, shape, 16384).expect("create"));

    c.bench_function("concurrent_methods/spsc_10k/channel_direct", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let chan_c = Arc::clone(&chan);
                let stop = Arc::new(AtomicBool::new(false));
                let counted = Arc::new(AtomicU64::new(0));
                let stop_c = Arc::clone(&stop);
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while !stop_c.load(Ordering::Acquire) {
                        match chan_c.recv() {
                            Ok(v) => {
                                black_box(v);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                });
                let start = Instant::now();
                for i in 0..K_ITEMS {
                    while chan.send(&i).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while counted.load(Ordering::Acquire) < K_ITEMS {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                drain.join().ok();
            }
            total
        });
    });

    drop(chan);
    std::fs::remove_file(&path).ok();
}

// ============================================================
// MPMC stress test: 4 producers + 4 consumers
// ============================================================
const STRESS_PRODUCERS: usize = 4;
const STRESS_CONSUMERS: usize = 4;
const STRESS_PER_PRODUCER: u64 = 2500;
const STRESS_TOTAL: u64 = STRESS_PER_PRODUCER * STRESS_PRODUCERS as u64;

fn bench_stress_shared_ring(c: &mut Criterion) {
    let path = tmp("stress_ring");
    let ring = Arc::new(SharedRing::create(&path, 16384).expect("create"));

    c.bench_function("concurrent_methods/mpmc_4p_4c_10k/shared_ring", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let counted = Arc::new(AtomicU64::new(0));
                let stop = Arc::new(AtomicBool::new(false));
                let consumers: Vec<_> = (0..STRESS_CONSUMERS).map(|_| {
                    let ring_c = Arc::clone(&ring);
                    let stop_c = Arc::clone(&stop);
                    let counted_c = Arc::clone(&counted);
                    thread::spawn(move || {
                        let mut out = [0u8; PAYLOAD_BYTES];
                        while !stop_c.load(Ordering::Acquire) {
                            if ring_c.try_pop(&mut out).is_ok() {
                                black_box(&out);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                    })
                }).collect();
                let start = Instant::now();
                let producers: Vec<_> = (0..STRESS_PRODUCERS).map(|pid| {
                    let ring_p = Arc::clone(&ring);
                    thread::spawn(move || {
                        let mut buf = [0u8; PAYLOAD_BYTES];
                        for i in 0..STRESS_PER_PRODUCER {
                            let v = (pid as u64) * STRESS_PER_PRODUCER + i;
                            buf[..8].copy_from_slice(&v.to_le_bytes());
                            while ring_p.try_push(&buf).is_err() {
                                std::hint::spin_loop();
                            }
                        }
                    })
                }).collect();
                for p in producers { p.join().ok(); }
                while counted.load(Ordering::Acquire) < STRESS_TOTAL {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                for c in consumers { c.join().ok(); }
            }
            total
        });
    });

    drop(ring);
    std::fs::remove_file(&path).ok();
}

fn bench_stress_crossbeam_bounded(c: &mut Criterion) {
    c.bench_function("concurrent_methods/mpmc_4p_4c_10k/crossbeam_bounded", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = cbc::bounded::<u64>(16384);
                let counted = Arc::new(AtomicU64::new(0));
                let consumers: Vec<_> = (0..STRESS_CONSUMERS).map(|_| {
                    let rx_c = rx.clone();
                    let counted_c = Arc::clone(&counted);
                    thread::spawn(move || {
                        while let Ok(v) = rx_c.recv() {
                            black_box(v);
                            counted_c.fetch_add(1, Ordering::AcqRel);
                        }
                    })
                }).collect();
                let start = Instant::now();
                let producers: Vec<_> = (0..STRESS_PRODUCERS).map(|pid| {
                    let tx_p = tx.clone();
                    thread::spawn(move || {
                        for i in 0..STRESS_PER_PRODUCER {
                            let v = (pid as u64) * STRESS_PER_PRODUCER + i;
                            tx_p.send(v).expect("send");
                        }
                    })
                }).collect();
                for p in producers { p.join().ok(); }
                while counted.load(Ordering::Acquire) < STRESS_TOTAL {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                drop(tx);
                for c in consumers { c.join().ok(); }
            }
            total
        });
    });
}

fn bench_stress_adaptive_ipc(c: &mut Criterion) {
    let path = tmp("stress_adapt");
    let shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: STRESS_PRODUCERS,
        n_consumers: STRESS_CONSUMERS,
    };
    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(&path, shape, 16384, STRESS_CONSUMERS)
            .expect("create"),
    );

    c.bench_function("concurrent_methods/mpmc_4p_4c_10k/adaptive_ipc", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let counted = Arc::new(AtomicU64::new(0));
                let stop = Arc::new(AtomicBool::new(false));
                let consumers: Vec<_> = (0..STRESS_CONSUMERS).map(|_| {
                    let ipc_c = Arc::clone(&ipc);
                    let stop_c = Arc::clone(&stop);
                    let counted_c = Arc::clone(&counted);
                    thread::spawn(move || {
                        while !stop_c.load(Ordering::Acquire) {
                            if let Ok(v) = ipc_c.recv() {
                                black_box(v);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                    })
                }).collect();
                let start = Instant::now();
                let producers: Vec<_> = (0..STRESS_PRODUCERS).map(|pid| {
                    let ipc_p = Arc::clone(&ipc);
                    thread::spawn(move || {
                        for i in 0..STRESS_PER_PRODUCER {
                            let v = (pid as u64) * STRESS_PER_PRODUCER + i;
                            while ipc_p.send(&v).is_err() {
                                std::hint::spin_loop();
                            }
                        }
                    })
                }).collect();
                for p in producers { p.join().ok(); }
                while counted.load(Ordering::Acquire) < STRESS_TOTAL {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                for c in consumers { c.join().ok(); }
            }
            total
        });
    });

    drop(ipc);
    for suffix in &[".ring.bin", ".deque.bin", ".ctl.bin"] {
        let mut p = path.clone();
        let s = p.file_name().map(|s| s.to_owned()).unwrap_or_default();
        p.set_file_name(format!("{}{suffix}", s.to_string_lossy()));
        std::fs::remove_file(&p).ok();
    }
}

criterion_group!(
    benches,
    bench_adaptive_ipc,
    bench_shared_ring_direct,
    bench_channel_direct,
    bench_crossbeam_bounded,
    bench_crossbeam_unbounded,
    bench_std_sync_channel,
    bench_mutex_vecdeque,
    bench_stress_shared_ring,
    bench_stress_crossbeam_bounded,
    bench_stress_adaptive_ipc,
);
criterion_main!(benches);
