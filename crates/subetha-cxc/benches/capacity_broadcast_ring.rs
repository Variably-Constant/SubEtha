//! Criterion bench: CapacityBroadcastRing per-op latency and
//! steady-state throughput across (locale, capacity, N
//! subscribers).
//!
//! Wraps SharedBroadcastRing (1P / NC fan-out). Per-iter:
//! producer publishes BATCH items; every subscriber drains
//! BATCH items independently. Per-op cost is wall time per iter
//! / BATCH.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use subetha_cxc::capacity_broadcast_ring::CapacityBroadcastRing;

const BATCH_PER_ITER: u64 = 4096;

struct CapBroadcastPool {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl CapBroadcastPool {
    fn spawn(
        ring: Arc<CapacityBroadcastRing>,
        consumer_ids: Vec<usize>,
        items_per_iter: u64,
    ) -> Self {
        let n_c = consumer_ids.len();
        let total = 1 + n_c;
        let start = Arc::new(Barrier::new(total + 1));
        let done = Arc::new(Barrier::new(total + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(total);
        // Producer
        {
            let r = Arc::clone(&ring);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                let payload = [0u8; 52];
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..items_per_iter {
                        while r.try_push(&payload).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                    done.wait();
                }
            }));
        }
        // Consumers
        for cid in consumer_ids {
            let r = Arc::clone(&ring);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                let mut buf = [0u8; 64];
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..items_per_iter {
                        while r.try_recv(cid, &mut buf).is_err() {
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

impl Drop for CapBroadcastPool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for h in self.handles.drain(..) {
            drop(h.join());
        }
    }
}

fn bench_cap_broadcast_anon(c: &mut Criterion) {
    for n_c in [2usize, 4, 8] {
        let mut group = c.benchmark_group(format!("capacity_broadcast_ring/anon/1P{n_c}C"));
        group.throughput(Throughput::Elements(BATCH_PER_ITER));
        for cap in [256usize, 1024, 4096, 16384] {
            let ring = Arc::new(
                CapacityBroadcastRing::create_anon(cap).expect("create_anon"),
            );
            let ids: Vec<usize> = (0..n_c)
                .map(|_| ring.register_consumer().expect("register consumer"))
                .collect();
            let pool = CapBroadcastPool::spawn(Arc::clone(&ring), ids, BATCH_PER_ITER);
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
}

fn bench_cap_broadcast_morph_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_broadcast_ring/morph_only");
    group.sample_size(20);
    for cap in [256usize, 4096, 65536] {
        let ring = CapacityBroadcastRing::create_anon(cap).expect("create_anon");
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
    bench_cap_broadcast_anon,
    bench_cap_broadcast_morph_only,
);
criterion_main!(benches);
