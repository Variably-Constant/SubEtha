//! 10,000 async subscribers on a fixed thread pool - no tokio, no
//! thread per subscriber. The load-bearing proof for the unified async
//! substrate: a bounded executor (`TaskPool`) drives N awaiting tasks,
//! each parked on its own ring, woken by a producer's push that fires
//! the task's `Waker` directly. OS-agnostic; this runs on Windows and
//! Linux with identical code.
//!
//! A thread-per-subscriber design (what the spawn-a-thread async
//! adapter does) would need N OS threads here. This uses
//! `executor_workers + producer_threads` - a small constant - for any
//! N. The program prints both counts so the contrast is visible.
//!
//! Run:
//!     cargo run --release --example async_subscriber_scale -p subetha-cxc

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use subetha_cxc::task_pool::TaskPool;
use subetha_cxc::waker_ring::{WakerProducer, WakerRing};

const N_SUBSCRIBERS: usize = 10_000;
const ITEMS_EACH: u64 = 50;
const RING_CAP: usize = 8;
const PRODUCER_THREADS: usize = 4;

fn main() {
    let workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let total_items = N_SUBSCRIBERS as u64 * ITEMS_EACH;

    println!("async substrate scale test (no tokio, no thread-per-subscriber)");
    println!("{N_SUBSCRIBERS} subscribers, {ITEMS_EACH} items each = {total_items} items");
    println!("executor workers: {workers}, producer threads: {PRODUCER_THREADS}");
    println!("OS threads for the async work: {} (a thread-per-subscriber design needs {N_SUBSCRIBERS})\n",
             workers + PRODUCER_THREADS);

    let pool = TaskPool::new(workers);
    let received = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));

    // One ring per subscriber. The consumer half drives an async task on
    // the pool; the producer half is owned by a producer thread.
    let mut producers: Vec<WakerProducer> = Vec::with_capacity(N_SUBSCRIBERS);
    for _ in 0..N_SUBSCRIBERS {
        let (p, c) = WakerRing::create_anon_pair(RING_CAP).expect("pair");
        producers.push(p);
        let received = Arc::clone(&received);
        let checksum = Arc::clone(&checksum);
        pool.spawn(async move {
            let mut local = 0u64;
            for _ in 0..ITEMS_EACH {
                let item = c.recv().await;
                local = local.wrapping_add(u64::from_le_bytes(item[..8].try_into().unwrap()));
                received.fetch_add(1, Ordering::Relaxed);
            }
            checksum.fetch_add(local, Ordering::AcqRel);
        });
    }

    // Hand each producer thread a disjoint slice of the rings, so every
    // SPSC ring still has exactly one producer. Each pushes ITEMS_EACH
    // to each of its rings; each push wakes one suspended task.
    let t0 = Instant::now();
    let chunk = N_SUBSCRIBERS.div_ceil(PRODUCER_THREADS);
    let producers = Arc::new(producers);
    let mut handles = Vec::new();
    for t in 0..PRODUCER_THREADS {
        let producers = Arc::clone(&producers);
        let lo = t * chunk;
        let hi = ((t + 1) * chunk).min(N_SUBSCRIBERS);
        handles.push(std::thread::spawn(move || {
            let mut payload = [0u8; 16];
            for sub in lo..hi {
                let prod = &producers[sub];
                for seq in 0..ITEMS_EACH {
                    let v = ((sub as u64) << 32) | seq;
                    payload[..8].copy_from_slice(&v.to_le_bytes());
                    while prod.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Wait for every task to finish receiving.
    while received.load(Ordering::Acquire) < total_items {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    pool.shutdown();

    // Integrity: the global checksum must equal the sum of every
    // (sub << 32 | seq) that was sent.
    let mut expected = 0u64;
    for sub in 0..N_SUBSCRIBERS as u64 {
        for seq in 0..ITEMS_EACH {
            expected = expected.wrapping_add((sub << 32) | seq);
        }
    }
    let got = checksum.load(Ordering::Acquire);
    assert_eq!(got, expected, "integrity: every item delivered exactly once");

    println!("delivered {total_items} items across {N_SUBSCRIBERS} async subscribers in {elapsed:?}");
    println!("{:.2} M items/s, integrity OK (checksum matched)",
             total_items as f64 / elapsed.as_secs_f64() / 1e6);
    println!("served by {} OS threads, not {N_SUBSCRIBERS} - no thread per subscriber.",
             workers + PRODUCER_THREADS);
}
