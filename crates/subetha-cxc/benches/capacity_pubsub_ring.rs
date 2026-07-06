//! Criterion bench: CapacityPubSubRing per-op latency and
//! steady-state throughput (anon locale, varying N subscribers).
//!
//! Wraps PubSubRing (1P / NC absolute-position fan-out). The
//! producer uses per-backing back-pressure to avoid wrapping any
//! backing (otherwise late-arriving subscribers would lose
//! data). Per-iter: producer publishes BATCH items; every
//! subscriber drains BATCH items via try_next, advancing through
//! the chain of backings as needed.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use subetha_cxc::capacity_pubsub_ring::{CapacityPubSubRing, CapacityPubSubSubscriber};
use subetha_cxc::protocol_pubsub::PubSubReadError;

const BATCH_PER_ITER: u64 = 4096;

struct CapPubSubPool {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl CapPubSubPool {
    fn spawn(
        ring: Arc<CapacityPubSubRing>,
        subscribers: Vec<CapacityPubSubSubscriber>,
        capacity: usize,
        items_per_iter: u64,
    ) -> Self {
        let n_c = subscribers.len();
        let total = 1 + n_c;
        let start = Arc::new(Barrier::new(total + 1));
        let done = Arc::new(Barrier::new(total + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(total);
        // Producer with per-backing back-pressure
        {
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
                    for _ in 0..items_per_iter {
                        loop {
                            let active = r.ring_handle();
                            if active.head() < capacity as u64 {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                        r.publish(&payload);
                    }
                    done.wait();
                }
            }));
        }
        // Subscribers
        for mut sub in subscribers {
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
                    let mut got = 0u64;
                    while got < items_per_iter {
                        match sub.try_next(&mut buf) {
                            Ok(()) => got += 1,
                            Err(PubSubReadError::Pending) => std::hint::spin_loop(),
                            Err(PubSubReadError::Lost) => {
                                eprintln!("cap-pubsub bench subscriber lost");
                                return;
                            }
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

impl Drop for CapPubSubPool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for h in self.handles.drain(..) {
            drop(h.join());
        }
    }
}

fn bench_cap_pubsub_anon(c: &mut Criterion) {
    for n_c in [2usize, 4, 8] {
        let mut group = c.benchmark_group(format!("capacity_pubsub_ring/anon/1P{n_c}C"));
        group.throughput(Throughput::Elements(BATCH_PER_ITER));
        // Use larger capacities than other primitives because
        // per-backing back-pressure bounds producer to cap items
        // per backing; at low cap producer stalls dominate.
        for cap in [4096usize, 16384, 65536] {
            let ring = CapacityPubSubRing::create_anon(cap).expect("create_anon");
            let subs: Vec<_> = (0..n_c).map(|_| ring.subscribe_from_oldest()).collect();
            let pool = CapPubSubPool::spawn(Arc::clone(&ring), subs, cap, BATCH_PER_ITER);
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

fn bench_cap_pubsub_morph_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_pubsub_ring/morph_only");
    group.sample_size(20);
    for cap in [256usize, 4096, 65536] {
        let ring = CapacityPubSubRing::create_anon(cap).expect("create_anon");
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
    bench_cap_pubsub_anon,
    bench_cap_pubsub_morph_only,
);
criterion_main!(benches);
