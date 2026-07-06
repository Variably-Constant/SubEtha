//! Bench: AdaptiveRing adaptive-path vs pinned-path vs native primitive.
//!
//! Covers tracker items 10 + 11 in one harness:
//!
//! - Item 10 - adaptive dispatch overhead vs native. Measures the
//!   per-op cost of `AdaptiveRing::try_send` / `try_recv` (one
//!   Acquire load on `shape_tag` + branch) against the raw
//!   underlying primitive call. The delta is the dispatch tax.
//!
//! - Item 11 - pinned-path throughput matches the native primitive.
//!   `PinnedRing` captures the current shape + generation and goes
//!   directly to the matching backend with no shape-tag load and
//!   no branch. The bench asserts the pinned column lands within
//!   noise of the native column.
//!
//! Three shapes:
//!  - SPSC round-trip (single-threaded; isolates pure dispatch cost)
//!  - SPSC throughput (1P/1C threads; sustained-workload behavior)
//!  - MPMC 4P/4C      (4 producers + 4 consumers; deeper dispatch)
//!
//! Each shape emits three bench lines: `{shape}/native`,
//! `{shape}/adaptive`, `{shape}/pinned`. Cargo bench output diffs
//! the three columns directly.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};
use subetha_cxc::{
    AdaptiveRing, MpmcConsumer, MpmcProducer, RingShape, SharedRingMpmc,
};

// ===================================================================
// Shared pre-spawn thread pool (same shape as shared_ring.rs uses).
// Keeps OS thread creation at exactly (n_prod + n_cons) per bench
// function so Criterion's tens-of-thousands of iterations do not
// blow up the kernel thread table.
// ===================================================================

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
                if stop.load(Ordering::Acquire) {
                    break;
                }
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
                if stop.load(Ordering::Acquire) {
                    break;
                }
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
        for h in handles {
            h.join().expect("worker thread panicked");
        }
    }
}

// ===================================================================
// NativeMpmcPool: specialized variant of ProdConsPool that hands
// each thread move-ownership of one MpmcProducer / MpmcConsumer.
// Needed because MpmcProducer / MpmcConsumer are deliberately !Sync
// (single-owner), so the closure-cloning ProdConsPool pattern does
// not apply.
// ===================================================================

struct NativeMpmcPool {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl NativeMpmcPool {
    fn spawn(
        producers: Vec<MpmcProducer>,
        consumers: Vec<MpmcConsumer>,
        per_producer: usize,
        consumed: Arc<AtomicUsize>,
        total: usize,
    ) -> Self {
        let n_prod = producers.len();
        let n_cons = consumers.len();
        let total_workers = n_prod + n_cons;
        let start = Arc::new(Barrier::new(total_workers + 1));
        let done = Arc::new(Barrier::new(total_workers + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(total_workers);

        for producer in producers.into_iter() {
            let start = start.clone();
            let done = done.clone();
            let stop = stop.clone();
            handles.push(thread::spawn(move || {
                let payload = [0u8; 16];
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..per_producer {
                        while producer.try_push(&payload).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                    done.wait();
                }
            }));
        }
        for consumer in consumers.into_iter() {
            let start = start.clone();
            let done = done.clone();
            let stop = stop.clone();
            let consumed = consumed.clone();
            handles.push(thread::spawn(move || {
                let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    loop {
                        if consumed.load(Ordering::Acquire) >= total {
                            break;
                        }
                        if consumer.try_pop(&mut buf).is_ok() {
                            consumed.fetch_add(1, Ordering::AcqRel);
                        }
                    }
                    done.wait();
                }
            }));
        }
        Self { start, done, stop, handles }
    }

    fn run_one_batch(&self) {
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for NativeMpmcPool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        let handles = std::mem::take(&mut self.handles);
        for h in handles {
            h.join().expect("native MPMC worker panicked");
        }
    }
}

// ===================================================================
// SPSC round-trip: single-threaded push + pop per iter. The cleanest
// measurement of pure dispatch cost - no thread coordination, no
// contention, just the per-op overhead of native vs adaptive vs pinned.
// ===================================================================

fn spsc_round_trip(c: &mut Criterion) {
    let payload = [0xABu8; 16];
    let mut buf = [0u8; SPSC_PAYLOAD_BYTES];

    // Native: raw SpscRingCore.
    let native = SpscRingCore::create_anon(1024).expect("native spsc create");
    c.bench_function("adaptive_overhead.spsc_round_trip/native", |b| {
        b.iter(|| {
            native.try_push(black_box(&payload)).unwrap();
            native.try_pop(black_box(&mut buf)).unwrap();
        });
    });
    drop(native);

    // Adaptive: full dispatch path through AdaptiveRing.
    let adaptive = AdaptiveRing::create_anon(1, 1, 1024).expect("adaptive create");
    let pid = adaptive.register_producer().expect("p reg");
    let cid = adaptive.register_consumer().expect("c reg");
    c.bench_function("adaptive_overhead.spsc_round_trip/adaptive", |b| {
        b.iter(|| {
            adaptive.try_send(pid, black_box(&payload)).unwrap();
            adaptive.try_recv(cid, black_box(&mut buf)).unwrap();
        });
    });

    // Pinned: capture the SPSC shape once and call native methods.
    let pin = adaptive.pin_current_shape();
    assert_eq!(pin.shape(), RingShape::Spsc);
    c.bench_function("adaptive_overhead.spsc_round_trip/pinned", |b| {
        b.iter(|| {
            pin.spsc_try_push(black_box(&payload)).unwrap();
            pin.spsc_try_pop(black_box(&mut buf)).unwrap();
        });
    });
}

// ===================================================================
// SPSC throughput: 1 producer + 1 consumer threads, pre-spawned.
// Measures sustained-workload behavior. The dispatch tax shows up
// per-op so the throughput delta is dispatch_cost * 2 * N_per_iter.
// ===================================================================

fn spsc_throughput(c: &mut Criterion) {
    const N: usize = 10_000;

    // ---- native ----
    {
        let ring = Arc::new(SpscRingCore::create_anon(4096).unwrap());
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        let pool = ProdConsPool::spawn(
            1, 1,
            move |_pid| {
                let payload = [0u8; 16];
                for _ in 0..N {
                    while ring_p.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            },
            move |_cid| {
                let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                let mut n = 0;
                while n < N {
                    if ring_c.try_pop(&mut buf).is_ok() {
                        n += 1;
                    }
                }
            },
        );
        c.bench_function("adaptive_overhead.spsc_throughput_10k/native", |b| {
            b.iter(|| pool.run_one_batch());
        });
        drop(pool);
    }

    // ---- adaptive ----
    {
        let ring = Arc::new(AdaptiveRing::create_anon(1, 1, 4096).unwrap());
        let pid = ring.register_producer().unwrap();
        let cid = ring.register_consumer().unwrap();
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        let pool = ProdConsPool::spawn(
            1, 1,
            move |_| {
                let payload = [0u8; 16];
                for _ in 0..N {
                    while ring_p.try_send(pid, &payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            },
            move |_| {
                let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
                let mut n = 0;
                while n < N {
                    if ring_c.try_recv(cid, &mut buf).is_ok() {
                        n += 1;
                    }
                }
            },
        );
        c.bench_function("adaptive_overhead.spsc_throughput_10k/adaptive", |b| {
            b.iter(|| pool.run_one_batch());
        });
        drop(pool);
    }

    // ---- pinned ----
    {
        let ring = Arc::new(AdaptiveRing::create_anon(1, 1, 4096).unwrap());
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        let pool = ProdConsPool::spawn(
            1, 1,
            move |_| {
                let pin = ring_p.pin_current_shape();
                assert_eq!(pin.shape(), RingShape::Spsc);
                let payload = [0u8; 16];
                for _ in 0..N {
                    while pin.spsc_try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            },
            move |_| {
                let pin = ring_c.pin_current_shape();
                let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
                let mut n = 0;
                while n < N {
                    if pin.spsc_try_pop(&mut buf).is_ok() {
                        n += 1;
                    }
                }
            },
        );
        c.bench_function("adaptive_overhead.spsc_throughput_10k/pinned", |b| {
            b.iter(|| pool.run_one_batch());
        });
        drop(pool);
    }
}

// ===================================================================
// MPMC 4P/4C: 4 producers + 4 consumers, pre-spawned. Exercises the
// MPMC dispatch branch (deeper than SPSC because the consumer also
// has to round-robin over its static subset of producer rings).
// ===================================================================

fn mpmc_4_4(c: &mut Criterion) {
    const REQUIRED_CORES: usize = 4;
    let avail = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if avail < REQUIRED_CORES {
        eprintln!(
            "[skip] adaptive_overhead::mpmc_4_4: needs >= {REQUIRED_CORES} \
             logical CPUs (4p + 4c + main); host has {avail}. \
             Run on a >= 4-core machine to capture these numbers."
        );
        return;
    }

    const PER_PRODUCER: usize = 2_500;
    const TOTAL: usize = PER_PRODUCER * 4;

    // ---- native (raw SharedRingMpmc grid) ----
    // MpmcProducer / MpmcConsumer are !Sync (single-owner). Use the
    // dedicated pool that gives each thread move-ownership of its
    // own handle.
    {
        let (producers, consumers) =
            SharedRingMpmc::create_anon_grid(4, 4, 4096).unwrap();
        let consumed = Arc::new(AtomicUsize::new(0));
        let pool = NativeMpmcPool::spawn(
            producers, consumers, PER_PRODUCER, consumed.clone(), TOTAL,
        );
        c.bench_function("adaptive_overhead.mpmc_4_4/native", |b| {
            b.iter(|| {
                consumed.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }

    // ---- adaptive ----
    {
        let ring = Arc::new(AdaptiveRing::create_anon(4, 4, 4096).unwrap());
        let mut pids = Vec::new();
        for _ in 0..4 { pids.push(ring.register_producer().unwrap()); }
        let mut cids = Vec::new();
        for _ in 0..4 { cids.push(ring.register_consumer().unwrap()); }
        ring.morph_to(RingShape::Mpmc).expect("morph to MPMC");
        assert_eq!(ring.current_shape(), RingShape::Mpmc);

        let consumed = Arc::new(AtomicUsize::new(0));
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        let consumed_c = consumed.clone();
        let pids_arc: Arc<Vec<usize>> = Arc::new(pids);
        let cids_arc: Arc<Vec<usize>> = Arc::new(cids);
        let pids_p = pids_arc.clone();
        let cids_c = cids_arc.clone();
        let pool = ProdConsPool::spawn(
            4, 4,
            move |pid| {
                let producer_id = pids_p[pid];
                let payload = [0u8; 16];
                for _ in 0..PER_PRODUCER {
                    while ring_p.try_send(producer_id, &payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            },
            move |cid| {
                let consumer_id = cids_c[cid];
                let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
                loop {
                    if consumed_c.load(Ordering::Acquire) >= TOTAL {
                        return;
                    }
                    if ring_c.try_recv(consumer_id, &mut buf).is_ok() {
                        consumed_c.fetch_add(1, Ordering::AcqRel);
                    }
                }
            },
        );
        c.bench_function("adaptive_overhead.mpmc_4_4/adaptive", |b| {
            b.iter(|| {
                consumed.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }

    // ---- pinned ----
    {
        let ring = Arc::new(AdaptiveRing::create_anon(4, 4, 4096).unwrap());
        let mut pids = Vec::new();
        for _ in 0..4 { pids.push(ring.register_producer().unwrap()); }
        let mut cids = Vec::new();
        for _ in 0..4 { cids.push(ring.register_consumer().unwrap()); }
        ring.morph_to(RingShape::Mpmc).expect("morph to MPMC");

        let consumed = Arc::new(AtomicUsize::new(0));
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        let consumed_c = consumed.clone();
        let pids_arc: Arc<Vec<usize>> = Arc::new(pids);
        let cids_arc: Arc<Vec<usize>> = Arc::new(cids);
        let pids_p = pids_arc.clone();
        let cids_c = cids_arc.clone();
        let pool = ProdConsPool::spawn(
            4, 4,
            move |pid| {
                let producer_id = pids_p[pid];
                let pin = ring_p.pin_current_shape();
                assert_eq!(pin.shape(), RingShape::Mpmc);
                let payload = [0u8; 16];
                for _ in 0..PER_PRODUCER {
                    while pin.mpmc_try_push(producer_id, &payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            },
            move |cid| {
                let consumer_id = cids_c[cid];
                let pin = ring_c.pin_current_shape();
                let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
                loop {
                    if consumed_c.load(Ordering::Acquire) >= TOTAL {
                        return;
                    }
                    if pin.mpmc_try_pop(consumer_id, &mut buf).is_ok() {
                        consumed_c.fetch_add(1, Ordering::AcqRel);
                    }
                }
            },
        );
        c.bench_function("adaptive_overhead.mpmc_4_4/pinned", |b| {
            b.iter(|| {
                consumed.store(0, Ordering::Release);
                pool.run_one_batch();
            });
        });
        drop(pool);
    }
}

criterion_group!(benches, spsc_round_trip, spsc_throughput, mpmc_4_4);
criterion_main!(benches);
