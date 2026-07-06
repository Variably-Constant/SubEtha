//! Criterion bench: CapacityAdaptiveRing per-op latency and
//! steady-state throughput across (locale, shape, capacity, P, C).
//!
//! Companion to the broader throughput sweep (the `bench_throughput`
//! example run per matrix cell). The throughput sweep feeds the
//! whole-workload markdown tables the wiki publishes; this criterion
//! bench gives per-op latency with mean / stddev / outliers for
//! regression tracking.
//!
//! The pre-spawn pattern (from `benches/shared_ring.rs`) keeps
//! OS-thread count at exactly N producers + M consumers across
//! all criterion iterations: spawning a fresh thread per iter
//! would create 100k+ OS threads through the bench's many
//! iterations.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use subetha_cxc::adaptive_ring::RingShape;
use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;

/// Batch size per criterion iteration. Each iter the producer
/// pool publishes BATCH items total and the consumer pool drains
/// BATCH items total. Criterion measures wall time per iter ÷
/// BATCH for the per-op latency.
const BATCH_PER_ITER: u64 = 4096;

struct CapAdaptivePool {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl CapAdaptivePool {
    fn spawn(
        ring: Arc<CapacityAdaptiveRing>,
        n_p: usize,
        n_c: usize,
        items_per_iter: u64,
    ) -> Self {
        let total = n_p + n_c;
        let start = Arc::new(Barrier::new(total + 1));
        let done = Arc::new(Barrier::new(total + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let drained = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(total);
        let items_per_producer = items_per_iter / n_p as u64;
        for pid in 0..n_p {
            let r = Arc::clone(&ring);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                let payload = [0u8; 56];
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..items_per_producer {
                        while r.try_send(pid, &payload).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                    done.wait();
                }
            }));
        }
        for cid in 0..n_c {
            let r = Arc::clone(&ring);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            let stop = Arc::clone(&stop);
            let d = Arc::clone(&drained);
            handles.push(thread::spawn(move || {
                let mut buf = [0u8; 64];
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let start_count = d.load(Ordering::Acquire);
                    loop {
                        if r.try_recv(cid, &mut buf).is_ok() {
                            let now = d.fetch_add(1, Ordering::AcqRel) + 1;
                            if now - start_count >= items_per_iter / n_c as u64 {
                                break;
                            }
                        } else {
                            std::hint::spin_loop();
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

impl Drop for CapAdaptivePool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for h in self.handles.drain(..) {
            drop(h.join());
        }
    }
}

fn bench_cap_adaptive_anon_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_adaptive_ring/anon/spsc/1P1C");
    group.throughput(Throughput::Elements(BATCH_PER_ITER));
    for cap in [256usize, 1024, 4096, 16384] {
        let ring = Arc::new(
            CapacityAdaptiveRing::create_anon(1, 1, cap).expect("create_anon"),
        );
        ring.register_producer().expect("register producer");
        ring.register_consumer().expect("register consumer");
        let pool = CapAdaptivePool::spawn(Arc::clone(&ring), 1, 1, BATCH_PER_ITER);
        group.bench_function(format!("cap={cap}"), |b| {
            b.iter(|| {
                pool.run_one_batch();
                black_box(&pool);
            });
        });
        drop(pool);
    }
    group.finish();
}

fn bench_cap_adaptive_anon_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_adaptive_ring/anon/mpsc/4P1C");
    group.throughput(Throughput::Elements(BATCH_PER_ITER));
    for cap in [1024usize, 4096, 16384] {
        let ring = Arc::new(
            CapacityAdaptiveRing::create_anon(4, 1, cap).expect("create_anon"),
        );
        for _ in 0..4 {
            ring.register_producer().expect("register producer");
        }
        ring.register_consumer().expect("register consumer");
        ring.ring_handle()
            .morph_to(RingShape::Mpsc)
            .expect("morph_to mpsc");
        let pool = CapAdaptivePool::spawn(Arc::clone(&ring), 4, 1, BATCH_PER_ITER);
        group.bench_function(format!("cap={cap}"), |b| {
            b.iter(|| {
                pool.run_one_batch();
                black_box(&pool);
            });
        });
        drop(pool);
    }
    group.finish();
}

fn bench_cap_adaptive_anon_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_adaptive_ring/anon/mpmc/4P4C");
    group.throughput(Throughput::Elements(BATCH_PER_ITER));
    for cap in [1024usize, 4096, 16384] {
        let ring = Arc::new(
            CapacityAdaptiveRing::create_anon(4, 4, cap).expect("create_anon"),
        );
        for _ in 0..4 {
            ring.register_producer().expect("register producer");
        }
        for _ in 0..4 {
            ring.register_consumer().expect("register consumer");
        }
        ring.ring_handle()
            .morph_to(RingShape::Mpmc)
            .expect("morph_to mpmc");
        let pool = CapAdaptivePool::spawn(Arc::clone(&ring), 4, 4, BATCH_PER_ITER);
        group.bench_function(format!("cap={cap}"), |b| {
            b.iter(|| {
                pool.run_one_batch();
                black_box(&pool);
            });
        });
        drop(pool);
    }
    group.finish();
}

fn bench_cap_adaptive_anon_vyukov(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_adaptive_ring/anon/vyukov/4P4C");
    group.throughput(Throughput::Elements(BATCH_PER_ITER));
    for cap in [1024usize, 4096, 16384] {
        let ring = Arc::new(
            CapacityAdaptiveRing::create_anon(4, 4, cap).expect("create_anon"),
        );
        for _ in 0..4 {
            ring.register_producer().expect("register producer");
        }
        for _ in 0..4 {
            ring.register_consumer().expect("register consumer");
        }
        ring.ring_handle()
            .morph_to(RingShape::Vyukov)
            .expect("morph_to vyukov");
        let pool = CapAdaptivePool::spawn(Arc::clone(&ring), 4, 4, BATCH_PER_ITER);
        group.bench_function(format!("cap={cap}"), |b| {
            b.iter(|| {
                pool.run_one_batch();
                black_box(&pool);
            });
        });
        drop(pool);
    }
    group.finish();
}

/// Cost of a single morph (allocate new backing + mirror reg
/// counts + push to stale + swap active). Per-iter cost is the
/// morph alone; producers / consumers are idle so we isolate the
/// morph path.
fn bench_cap_adaptive_morph_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_adaptive_ring/morph_only");
    group.sample_size(20);
    for cap in [256usize, 4096, 65536] {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, cap).expect("create_anon");
        ring.register_producer().expect("register producer");
        ring.register_consumer().expect("register consumer");
        let mut toggle = false;
        group.bench_function(format!("anon/cap={cap}->{}", cap * 2), |b| {
            b.iter(|| {
                let target = if toggle { cap } else { cap * 2 };
                toggle = !toggle;
                ring.morph_capacity_to(target).expect("morph");
                black_box(&ring);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_cap_adaptive_anon_spsc,
    bench_cap_adaptive_anon_mpsc,
    bench_cap_adaptive_anon_mpmc,
    bench_cap_adaptive_anon_vyukov,
    bench_cap_adaptive_morph_only,
);
criterion_main!(benches);
