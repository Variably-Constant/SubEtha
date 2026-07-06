//! Bench (harness = false): the scaling the async substrate unlocks -
//! N awaiting consumers driven by a fixed thread pool instead of N OS
//! threads.
//!
//! Async on this substrate is not a lower-latency path (see
//! `async_overhead`); its win is structural. A consumer that `await`s a
//! ring is a suspended task, not a parked thread, so one bounded
//! executor drives an unbounded number of them. This bench delivers the
//! same item stream two ways and reports the OS-thread cost of each:
//!
//! - **fixed_pool**: a `TaskPool` of `available_parallelism` workers
//!   drives N awaiting tasks. Thread count is constant in N.
//! - **thread_per_consumer**: N OS threads, each running `block_on` on
//!   one consumer's `recv()` future. Thread count grows with N.
//!
//! Bench audit: both contenders deliver the same item count to N
//! consumers over the SAME `WakerRing` primitive and the SAME `recv()`
//! future, with the SAME producer threads pushing the SAME payloads.
//! The only difference is the driver - a fixed pool of tasks vs one
//! thread per consumer. Integrity is checked on both: a global checksum
//! must equal the sum of every `(sub << 32 | seq)` sent.
//!
//! Run: cargo bench --bench async_fanout -p subetha-cxc

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use subetha_cxc::reactor::block_on;
use subetha_cxc::task_pool::TaskPool;
use subetha_cxc::waker_ring::{WakerConsumer, WakerProducer, WakerRing};

const ITEMS_EACH: u64 = 50;
const RING_CAP: usize = 8;
const PRODUCER_THREADS: usize = 4;

fn workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Expected checksum for `n` consumers each receiving `ITEMS_EACH`
/// values of the form `(sub << 32 | seq)`.
fn expected_checksum(n: usize) -> u64 {
    let mut expected = 0u64;
    for sub in 0..n as u64 {
        for seq in 0..ITEMS_EACH {
            expected = expected.wrapping_add((sub << 32) | seq);
        }
    }
    expected
}

/// Drive `PRODUCER_THREADS` producer threads over disjoint slices of the
/// per-consumer producers, each pushing `ITEMS_EACH` to each ring. Every
/// SPSC ring keeps exactly one producer. Returns when all are pushed.
fn drive_producers(producers: Arc<Vec<WakerProducer>>, n: usize) {
    let chunk = n.div_ceil(PRODUCER_THREADS);
    let mut handles = Vec::new();
    for t in 0..PRODUCER_THREADS {
        let producers = Arc::clone(&producers);
        let lo = t * chunk;
        let hi = ((t + 1) * chunk).min(n);
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
}

/// Fixed-pool async: a `TaskPool` of `workers()` threads drives N
/// awaiting tasks. Thread count is `workers + PRODUCER_THREADS`,
/// constant in N.
fn run_fixed_pool(n: usize) {
    let w = workers();
    let pool = TaskPool::new(w);
    let received = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let total = n as u64 * ITEMS_EACH;

    let mut producers: Vec<WakerProducer> = Vec::with_capacity(n);
    for _ in 0..n {
        let (p, c) = WakerRing::create_anon_pair(RING_CAP).expect("pair");
        producers.push(p);
        let received = Arc::clone(&received);
        let checksum = Arc::clone(&checksum);
        pool.spawn(async move {
            let mut local = 0u64;
            for _ in 0..ITEMS_EACH {
                let item = c.recv().await;
                local =
                    local.wrapping_add(u64::from_le_bytes(item[..8].try_into().unwrap()));
                received.fetch_add(1, Ordering::Relaxed);
            }
            checksum.fetch_add(local, Ordering::AcqRel);
        });
    }

    let producers = Arc::new(producers);
    let t0 = Instant::now();
    drive_producers(Arc::clone(&producers), n);
    while received.load(Ordering::Acquire) < total {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    pool.shutdown();

    assert_eq!(
        checksum.load(Ordering::Acquire),
        expected_checksum(n),
        "fixed_pool integrity: every item delivered exactly once"
    );
    report("fixed_pool", n, total, w + PRODUCER_THREADS, elapsed);
}

/// Thread-per-consumer: N OS threads, each `block_on`-driving one
/// consumer's `recv()` future. Thread count is `N + PRODUCER_THREADS`,
/// linear in N.
fn run_thread_per_consumer(n: usize) {
    let received = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let total = n as u64 * ITEMS_EACH;

    let mut producers: Vec<WakerProducer> = Vec::with_capacity(n);
    let mut consumers: Vec<WakerConsumer> = Vec::with_capacity(n);
    for _ in 0..n {
        let (p, c) = WakerRing::create_anon_pair(RING_CAP).expect("pair");
        producers.push(p);
        consumers.push(c);
    }

    let mut consumer_threads = Vec::with_capacity(n);
    for c in consumers {
        let received = Arc::clone(&received);
        let checksum = Arc::clone(&checksum);
        consumer_threads.push(std::thread::spawn(move || {
            let local = block_on(async move {
                let mut s = 0u64;
                for _ in 0..ITEMS_EACH {
                    let item = c.recv().await;
                    s = s.wrapping_add(u64::from_le_bytes(
                        item[..8].try_into().unwrap(),
                    ));
                    received.fetch_add(1, Ordering::Relaxed);
                }
                s
            });
            checksum.fetch_add(local, Ordering::AcqRel);
        }));
    }

    let producers = Arc::new(producers);
    let t0 = Instant::now();
    drive_producers(Arc::clone(&producers), n);
    for h in consumer_threads {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed();

    assert_eq!(
        checksum.load(Ordering::Acquire),
        expected_checksum(n),
        "thread_per_consumer integrity: every item delivered exactly once"
    );
    report("thread_per_consumer", n, total, n + PRODUCER_THREADS, elapsed);
}

fn report(label: &str, n: usize, total: u64, os_threads: usize, elapsed: std::time::Duration) {
    println!(
        "  {label:<20} N={n:>7}  {total:>9} items  {os_threads:>7} OS threads  \
         {:>7.2} M items/s  ({:.1} ms)",
        total as f64 / elapsed.as_secs_f64() / 1e6,
        elapsed.as_secs_f64() * 1e3,
    );
}

fn main() {
    println!("=== async fanout: fixed pool vs thread-per-consumer ===");
    println!("(ITEMS_EACH={ITEMS_EACH}, RING_CAP={RING_CAP}, producer threads={PRODUCER_THREADS})\n");

    println!("fixed_pool async (thread count constant in N):");
    for &n in &[1_000usize, 10_000, 100_000] {
        run_fixed_pool(n);
    }

    println!("\nthread_per_consumer (thread count linear in N):");
    for &n in &[1_000usize, 10_000] {
        run_thread_per_consumer(n);
    }

    println!(
        "\nfixed_pool serves any N on {} OS threads; thread_per_consumer needs N + {PRODUCER_THREADS}.",
        workers() + PRODUCER_THREADS
    );
}
