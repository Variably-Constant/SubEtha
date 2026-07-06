//! Bench: SharedRing (MMF-backed lock-free MPMC ring) vs
//! crossbeam-channel + std::sync::mpsc + std anonymous pipe.
//!
//! The architectural claim: SharedRing's lock-free protocol over a
//! cache-line-aligned MMF layout matches or beats in-process
//! channels for the SPSC and MPMC cases, AND it works cross-process
//! AND it persists to disk - all from the same mechanism. The
//! competitors don't offer that.
//!
//! # Safety / cost discipline
//!
//! The multi-thread benches PRE-SPAWN producer and consumer threads
//! ONCE per bench function, then coordinate per-iteration work via
//! `std::sync::Barrier`. The naive `b.iter(|| thread::spawn(...))`
//! pattern spawns 2-8 OS threads PER iteration; criterion runs tens
//! of thousands of iterations, so the naive pattern would create
//! 100k+ OS threads and exhaust the kernel thread table. The barrier
//! pattern keeps OS thread creation at exactly N (= producer count +
//! consumer count) per bench function.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedRing, PAYLOAD_BYTES};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-{name}-{pid}.bin"));
    p
}

/// Pre-spawn N producer threads + M consumer threads, return a
/// handle that lets the main thread trigger one batch per
/// iteration. RAII: on drop, signals stop and joins cleanly.
///
/// Each worker is closed over an `Arc<dyn Fn(usize) + Send + Sync>`
/// so the per-iter work logic is captured once at spawn time. The
/// closure receives the worker's role-local id (0..n_prod for
/// producers; 0..n_cons for consumers).
struct ProdConsPool {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl ProdConsPool {
    fn spawn<P, C>(n_prod: usize, n_cons: usize, prod_fn: P, cons_fn: C) -> Self
    where
        P: Fn(usize) + Send + Sync + 'static,
        C: Fn(usize) + Send + Sync + 'static,
    {
        let total = n_prod + n_cons;
        let start = Arc::new(Barrier::new(total + 1));
        let done = Arc::new(Barrier::new(total + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let prod = Arc::new(prod_fn);
        let cons = Arc::new(cons_fn);
        let mut handles = Vec::with_capacity(total);
        for pid in 0..n_prod {
            let start = start.clone();
            let done = done.clone();
            let stop = stop.clone();
            let prod = prod.clone();
            handles.push(thread::spawn(move || loop {
                start.wait();
                if stop.load(Ordering::Acquire) { break; }
                prod(pid);
                done.wait();
            }));
        }
        for cid in 0..n_cons {
            let start = start.clone();
            let done = done.clone();
            let stop = stop.clone();
            let cons = cons.clone();
            handles.push(thread::spawn(move || loop {
                start.wait();
                if stop.load(Ordering::Acquire) { break; }
                cons(cid);
                done.wait();
            }));
        }
        Self { start, done, stop, handles }
    }

    fn run_one_batch(&self) {
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for ProdConsPool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Wake parked workers; they check stop, see true, exit
        // without entering done.wait(), so main does NOT wait on
        // done barrier here.
        self.start.wait();
        let handles = std::mem::take(&mut self.handles);
        for h in handles {
            h.join().expect("worker thread panicked");
        }
    }
}

// =========================================================
// SPSC round-trip: one push + one pop per iter. Single-threaded;
// no thread::spawn anywhere; was already safe.
// =========================================================

fn spsc_round_trip(c: &mut Criterion) {
    let path = tmp("spsc-rt");
    let ring = SharedRing::create(&path, 1024).unwrap();
    let payload = [0xABu8; 16];
    let mut buf = [0u8; PAYLOAD_BYTES];
    c.bench_function("shared_ring.spsc_round_trip/subetha", |b| {
        b.iter(|| {
            ring.try_push(black_box(&payload)).unwrap();
            ring.try_pop(black_box(&mut buf)).unwrap();
        });
    });
    drop(ring);
    std::fs::remove_file(&path).ok();

    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(1024);
    c.bench_function("shared_ring.spsc_round_trip/crossbeam_channel", |b| {
        b.iter(|| {
            tx.send(black_box(payload)).unwrap();
            black_box(rx.recv().unwrap());
        });
    });

    let (tx, rx) = std::sync::mpsc::sync_channel::<[u8; 16]>(1024);
    c.bench_function("shared_ring.spsc_round_trip/std_mpsc_sync", |b| {
        b.iter(|| {
            tx.send(black_box(payload)).unwrap();
            black_box(rx.recv().unwrap());
        });
    });
}

// =========================================================
// SPSC throughput: producer thread + consumer thread, PRE-SPAWNED.
// =========================================================

fn spsc_throughput(c: &mut Criterion) {
    const N: usize = 10_000;

    let path = tmp("spsc-tp");
    let ring = Arc::new(SharedRing::create(&path, 4096).unwrap());
    let ring_for_p = ring.clone();
    let ring_for_c = ring.clone();
    let pool = ProdConsPool::spawn(
        1, 1,
        move |_pid| {
            let payload = [0u8; 16];
            for _ in 0..N {
                while ring_for_p.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        },
        move |_cid| {
            let mut buf = [0u8; PAYLOAD_BYTES];
            let mut n = 0;
            while n < N {
                if ring_for_c.try_pop(&mut buf).is_ok() { n += 1; }
            }
        },
    );
    c.bench_function("shared_ring.spsc_throughput/subetha_10k", |b| {
        b.iter(|| pool.run_one_batch());
    });
    drop(pool);
    drop(ring);
    std::fs::remove_file(&path).ok();

    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(4096);
    let tx_c = tx.clone();
    let rx_c = rx.clone();
    let pool = ProdConsPool::spawn(
        1, 1,
        move |_pid| {
            for _ in 0..N { tx_c.send([0u8; 16]).unwrap(); }
        },
        move |_cid| {
            for _ in 0..N { let _ = rx_c.recv().unwrap(); }
        },
    );
    c.bench_function("shared_ring.spsc_throughput/crossbeam_10k", |b| {
        b.iter(|| pool.run_one_batch());
    });
    drop(pool);
    drop(tx); drop(rx);
}

// =========================================================
// MPMC scaling: 4 producers + 4 consumers, PRE-SPAWNED.
// =========================================================

fn mpmc_4_4(c: &mut Criterion) {
    // Low-core skip-gate. 4 producers + 4 consumers + main = 9
    // threads; producers use `std::hint::spin_loop()` busy-wait
    // when the ring is full. On hosts with < 4 logical CPUs the
    // 9-thread schedule oversubscribes catastrophically and the
    // spin loops never make collective progress, producing
    // apparent livelock that runs for tens of minutes per
    // iteration.
    const REQUIRED_CORES: usize = 4;
    let avail = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if avail < REQUIRED_CORES {
        eprintln!(
            "[skip] shared_ring::mpmc_4_4: needs >= {REQUIRED_CORES} \
             logical CPUs (4p + 4c + main); host has {avail}. \
             Run on a >= 4-core machine to capture these numbers."
        );
        return;
    }

    const PER_PRODUCER: usize = 2_500;
    const TOTAL: usize = PER_PRODUCER * 4;

    let path = tmp("mpmc-4-4");
    let ring = Arc::new(SharedRing::create(&path, 4096).unwrap());
    // Per-iter consumed counter. Workers and main coordinate via:
    // main resets to 0 before signaling start; consumers count
    // pops and return when consumed >= TOTAL.
    let consumed = Arc::new(AtomicUsize::new(0));
    let ring_for_p = ring.clone();
    let ring_for_c = ring.clone();
    let consumed_for_c = consumed.clone();
    let pool = ProdConsPool::spawn(
        4, 4,
        move |_pid| {
            let payload = [0u8; 16];
            for _ in 0..PER_PRODUCER {
                while ring_for_p.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        },
        move |_cid| {
            let mut buf = [0u8; PAYLOAD_BYTES];
            loop {
                if consumed_for_c.load(Ordering::Acquire) >= TOTAL {
                    return;
                }
                if ring_for_c.try_pop(&mut buf).is_ok() {
                    consumed_for_c.fetch_add(1, Ordering::AcqRel);
                }
            }
        },
    );
    c.bench_function("shared_ring.mpmc_4_4/subetha", |b| {
        b.iter(|| {
            consumed.store(0, Ordering::Release);
            pool.run_one_batch();
        });
    });
    drop(pool);
    drop(ring);
    std::fs::remove_file(&path).ok();

    // Crossbeam variant. Cannot pre-spawn the same way because
    // crossbeam consumers detect end-of-stream via `tx` being
    // dropped; with pre-spawn the tx persists. Use a per-iter
    // remaining-count atomic that consumers check, identical to
    // the mmf path.
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(4096);
    let cb_consumed = Arc::new(AtomicUsize::new(0));
    let tx_for_p = tx.clone();
    let rx_for_c = rx.clone();
    let cb_consumed_for_c = cb_consumed.clone();
    let pool = ProdConsPool::spawn(
        4, 4,
        move |_pid| {
            for _ in 0..PER_PRODUCER { tx_for_p.send([0u8; 16]).unwrap(); }
        },
        move |_cid| {
            loop {
                if cb_consumed_for_c.load(Ordering::Acquire) >= TOTAL {
                    return;
                }
                if rx_for_c.try_recv().is_ok() {
                    cb_consumed_for_c.fetch_add(1, Ordering::AcqRel);
                }
            }
        },
    );
    c.bench_function("shared_ring.mpmc_4_4/crossbeam", |b| {
        b.iter(|| {
            cb_consumed.store(0, Ordering::Release);
            pool.run_one_batch();
        });
    });
    drop(pool);
    drop(tx); drop(rx);
}

criterion_group!(benches, spsc_round_trip, spsc_throughput, mpmc_4_4);
criterion_main!(benches);
