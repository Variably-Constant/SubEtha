//! Bench: `SharedDeque<u64>` (MMF-backed Chase-Lev work-stealing
//! deque) vs the in-process alternatives.
//!
//! Contenders:
//! - `subetha_cxc::SharedDeque` (this crate's MMF Chase-Lev).
//! - `crossbeam_deque::Worker` + `Stealer` (in-process Chase-Lev;
//!   the closest apples-to-apples comparison and the reference
//!   implementation of the protocol).
//! - `parking_lot::Mutex<VecDeque<u64>>` (OS-native lock baseline;
//!   represents the textbook implementation users reach for without
//!   lock-free primitives).
//! - `subetha_cxc::SharedRing` (the existing MMF MPMC primitive;
//!   tells us whether Chase-Lev's asymmetric protocol beats the
//!   symmetric MPMC ring on the same workload).
//!
//! # Bench fairness audit
//!
//! - All four primitives push a u64 payload.
//! - `SharedDeque`'s marshal of `u64::to_le_bytes` is the same byte
//!   cost as `crossbeam_deque`'s direct slot store on little-endian
//!   targets (compiler emits one MOV either way).
//! - Capacity is 4096 for every contender that takes a capacity;
//!   `VecDeque` is pre-allocated with `with_capacity(4096)` so
//!   reallocation is not on the hot path.
//! - For multi-thread benches, workers are PRE-SPAWNED with a
//!   `Barrier`-coordinated batch protocol so we don't create
//!   100k+ OS threads over the bench's iteration count.
//!
//! # Safety / cost discipline
//!
//! The multi-thread benches pre-spawn one producer thread and one or
//! more consumer threads ONCE per bench function and coordinate
//! per-iter work via `std::sync::Barrier`. The naive
//! `b.iter(|| thread::spawn(...))` pattern creates N threads per
//! iteration, which under criterion's iteration count produces tens
//! of thousands of OS threads and exhausts the kernel thread table.

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use parking_lot::Mutex;

use subetha_cxc::{SharedDeque, SharedRing, PAYLOAD_BYTES};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-deque-{name}-{pid}.bin"));
    p
}

// =========================================================
// Pre-spawned producer/consumer pool. Mirrors `ProdConsPool` in
// shared_ring.rs but with the distinction that the producer's
// closure is FnOnce-state-owning (so it can move !Sync state like
// crossbeam_deque::Worker into the producer thread), while consumers
// share captured state through Arc.
// =========================================================

struct ProdConsPool {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl ProdConsPool {
    /// Spawn one producer thread (its `prod_fn` MUST be ready to run
    /// the same per-iter work N times; the closure captures all the
    /// producer-only state) and `n_cons` consumer threads.
    fn spawn<P, C>(n_cons: usize, prod_fn: P, cons_fn: C) -> Self
    where
        P: FnMut() + Send + 'static,
        C: Fn(usize) + Send + Sync + 'static,
    {
        let total = 1 + n_cons;
        let start = Arc::new(Barrier::new(total + 1));
        let done = Arc::new(Barrier::new(total + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(total);

        // Producer.
        let start_p = start.clone();
        let done_p = done.clone();
        let stop_p = stop.clone();
        let mut prod = prod_fn;
        handles.push(thread::spawn(move || loop {
            start_p.wait();
            if stop_p.load(Ordering::Acquire) { break; }
            prod();
            done_p.wait();
        }));

        // Consumers.
        let cons = Arc::new(cons_fn);
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
        self.start.wait();
        let handles = std::mem::take(&mut self.handles);
        for h in handles { h.join().expect("worker panicked"); }
    }
}

// =========================================================
// Microbenches: single-thread push then pop on the owner end.
// =========================================================

fn st_push_pop(c: &mut Criterion) {
    // SubEtha SharedDeque.
    let path = tmp("st-pushpop");
    let dq = SharedDeque::<u64>::create(&path, 4096).unwrap();
    c.bench_function("shared_deque.st_push_pop/subetha_mmf", |b| {
        b.iter(|| {
            dq.push(black_box(&42u64)).unwrap();
            black_box(dq.pop().unwrap());
        });
    });
    drop(dq);
    std::fs::remove_file(&path).ok();

    // crossbeam_deque in-process Chase-Lev.
    let w: crossbeam_deque::Worker<u64> = crossbeam_deque::Worker::new_lifo();
    c.bench_function("shared_deque.st_push_pop/crossbeam_deque", |b| {
        b.iter(|| {
            w.push(black_box(42u64));
            black_box(w.pop().unwrap());
        });
    });

    // parking_lot::Mutex<VecDeque>.
    let m: Mutex<VecDeque<u64>> = Mutex::new(VecDeque::with_capacity(4096));
    c.bench_function("shared_deque.st_push_pop/mutex_vecdeque", |b| {
        b.iter(|| {
            m.lock().push_back(black_box(42u64));
            black_box(m.lock().pop_back().unwrap());
        });
    });

    // SubEtha SharedRing (MPMC, for cross-SubEtha comparison).
    let path = tmp("st-pushpop-ring");
    let ring = SharedRing::create(&path, 4096).unwrap();
    let payload = [0u8; 16];
    let mut buf = [0u8; PAYLOAD_BYTES];
    c.bench_function("shared_deque.st_push_pop/subetha_shared_ring", |b| {
        b.iter(|| {
            ring.try_push(black_box(&payload)).unwrap();
            ring.try_pop(black_box(&mut buf)).unwrap();
        });
    });
    drop(ring);
    std::fs::remove_file(&path).ok();
}

// =========================================================
// SPSC throughput: 1 owner pushes N, 1 thief drains N.
// =========================================================

fn one_owner_one_thief(c: &mut Criterion) {
    const N: usize = 10_000;

    // SubEtha SharedDeque.
    let path = tmp("1o1t-mmf");
    let owner = Arc::new(SharedDeque::<u64>::create(&path, 4096).unwrap());
    let thief = Arc::new(SharedDeque::<u64>::open_as_thief(&path).unwrap());
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let owner_h = owner.clone();
        let thief_h = thief.clone();
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let pool = ProdConsPool::spawn(
            1,
            move || {
                for i in 0..N as u64 {
                    while owner_h.push(&i).is_err() { std::hint::spin_loop(); }
                }
            },
            move |_| loop {
                if consumed_loop.load(Ordering::Acquire) >= N { return; }
                if thief_h.steal().is_some() {
                    consumed_loop.fetch_add(1, Ordering::AcqRel);
                }
            },
        );
        c.bench_function("shared_deque.1owner_1thief/subetha_mmf_10k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }
    drop(thief); drop(owner);
    std::fs::remove_file(&path).ok();

    // crossbeam_deque in-process: Worker moves to producer, Stealer
    // clones to consumer.
    let worker: crossbeam_deque::Worker<u64> = crossbeam_deque::Worker::new_lifo();
    let stealer = worker.stealer();
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let stealer_loop = stealer.clone();
        let pool = ProdConsPool::spawn(
            1,
            move || {
                for i in 0..N as u64 { worker.push(i); }
            },
            move |_| loop {
                if consumed_loop.load(Ordering::Acquire) >= N { return; }
                if let crossbeam_deque::Steal::Success(_) = stealer_loop.steal() {
                    consumed_loop.fetch_add(1, Ordering::AcqRel);
                }
            },
        );
        c.bench_function("shared_deque.1owner_1thief/crossbeam_deque_10k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }

    // Mutex<VecDeque>.
    let m: Arc<Mutex<VecDeque<u64>>> = Arc::new(Mutex::new(VecDeque::with_capacity(4096)));
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let m_prod = m.clone();
        let m_cons = m.clone();
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let pool = ProdConsPool::spawn(
            1,
            move || {
                for i in 0..N as u64 { m_prod.lock().push_back(i); }
            },
            move |_| loop {
                if consumed_loop.load(Ordering::Acquire) >= N { return; }
                if m_cons.lock().pop_front().is_some() {
                    consumed_loop.fetch_add(1, Ordering::AcqRel);
                }
            },
        );
        c.bench_function("shared_deque.1owner_1thief/mutex_vecdeque_10k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }

    // SubEtha SharedRing.
    let path = tmp("1o1t-ring");
    let ring = Arc::new(SharedRing::create(&path, 4096).unwrap());
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let ring_prod = ring.clone();
        let ring_cons = ring.clone();
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let pool = ProdConsPool::spawn(
            1,
            move || {
                let payload = [0u8; 16];
                for _ in 0..N {
                    while ring_prod.try_push(&payload).is_err() { std::hint::spin_loop(); }
                }
            },
            move |_| {
                let mut buf = [0u8; PAYLOAD_BYTES];
                loop {
                    if consumed_loop.load(Ordering::Acquire) >= N { return; }
                    if ring_cons.try_pop(&mut buf).is_ok() {
                        consumed_loop.fetch_add(1, Ordering::AcqRel);
                    }
                }
            },
        );
        c.bench_function("shared_deque.1owner_1thief/subetha_shared_ring_10k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }
    drop(ring);
    std::fs::remove_file(&path).ok();
}

// =========================================================
// 1 owner + 4 thieves: the Chase-Lev work-stealing workload.
// =========================================================

fn one_owner_four_thieves(c: &mut Criterion) {
    const REQUIRED_CORES: usize = 4;
    let avail = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    if avail < REQUIRED_CORES {
        eprintln!(
            "[skip] shared_deque::one_owner_four_thieves: needs >= {REQUIRED_CORES} \
             logical CPUs; host has {avail}."
        );
        return;
    }
    const N: usize = 8_000;

    // SubEtha SharedDeque.
    let path = tmp("1o4t-mmf");
    let owner = Arc::new(SharedDeque::<u64>::create(&path, 4096).unwrap());
    let thieves: Vec<Arc<SharedDeque<u64>>> = (0..4)
        .map(|_| Arc::new(SharedDeque::<u64>::open_as_thief(&path).unwrap()))
        .collect();
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let owner_h = owner.clone();
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let thieves_cons = thieves.clone();
        let pool = ProdConsPool::spawn(
            4,
            move || {
                for i in 0..N as u64 {
                    while owner_h.push(&i).is_err() { std::hint::spin_loop(); }
                }
            },
            move |cid| {
                let h = &thieves_cons[cid];
                loop {
                    if consumed_loop.load(Ordering::Acquire) >= N { return; }
                    if h.steal().is_some() {
                        consumed_loop.fetch_add(1, Ordering::AcqRel);
                    }
                }
            },
        );
        c.bench_function("shared_deque.1owner_4thieves/subetha_mmf_8k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }
    drop(thieves); drop(owner);
    std::fs::remove_file(&path).ok();

    // crossbeam_deque: Worker moves to producer, Stealers clone to
    // each of the 4 thieves.
    let worker: crossbeam_deque::Worker<u64> = crossbeam_deque::Worker::new_lifo();
    let stealers: Vec<crossbeam_deque::Stealer<u64>> = (0..4).map(|_| worker.stealer()).collect();
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let stealers_cons = stealers.clone();
        let pool = ProdConsPool::spawn(
            4,
            move || {
                for i in 0..N as u64 { worker.push(i); }
            },
            move |cid| {
                let s = &stealers_cons[cid];
                loop {
                    if consumed_loop.load(Ordering::Acquire) >= N { return; }
                    if let crossbeam_deque::Steal::Success(_) = s.steal() {
                        consumed_loop.fetch_add(1, Ordering::AcqRel);
                    }
                }
            },
        );
        c.bench_function("shared_deque.1owner_4thieves/crossbeam_deque_8k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }
    drop(stealers);

    // Mutex<VecDeque>.
    let m: Arc<Mutex<VecDeque<u64>>> = Arc::new(Mutex::new(VecDeque::with_capacity(4096)));
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let m_prod = m.clone();
        let m_cons = m.clone();
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let pool = ProdConsPool::spawn(
            4,
            move || {
                for i in 0..N as u64 { m_prod.lock().push_back(i); }
            },
            move |_cid| loop {
                if consumed_loop.load(Ordering::Acquire) >= N { return; }
                if m_cons.lock().pop_front().is_some() {
                    consumed_loop.fetch_add(1, Ordering::AcqRel);
                }
            },
        );
        c.bench_function("shared_deque.1owner_4thieves/mutex_vecdeque_8k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }

    // SubEtha SharedRing (MPMC ring; 4 thieves all contend on
    // `consumer_seq`, which is the architectural distinction).
    let path = tmp("1o4t-ring");
    let ring = Arc::new(SharedRing::create(&path, 4096).unwrap());
    let consumed = Arc::new(AtomicUsize::new(0));
    {
        let ring_prod = ring.clone();
        let ring_cons = ring.clone();
        let consumed_h = consumed.clone();
        let consumed_loop = consumed.clone();
        let pool = ProdConsPool::spawn(
            4,
            move || {
                let payload = [0u8; 16];
                for _ in 0..N {
                    while ring_prod.try_push(&payload).is_err() { std::hint::spin_loop(); }
                }
            },
            move |_cid| {
                let mut buf = [0u8; PAYLOAD_BYTES];
                loop {
                    if consumed_loop.load(Ordering::Acquire) >= N { return; }
                    if ring_cons.try_pop(&mut buf).is_ok() {
                        consumed_loop.fetch_add(1, Ordering::AcqRel);
                    }
                }
            },
        );
        c.bench_function("shared_deque.1owner_4thieves/subetha_shared_ring_8k", |b| {
            b.iter(|| {
                consumed_h.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }
    drop(ring);
    std::fs::remove_file(&path).ok();
}

criterion_group!(benches, st_push_pop, one_owner_one_thief, one_owner_four_thieves);
criterion_main!(benches);
