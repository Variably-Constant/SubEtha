//! Worked-example demo of the three blocking ring wrappers:
//! [`BlockingSpscRing`], [`BlockingMpscRing`], [`BlockingMpmcRing`].
//!
//! Each section runs intra-process (producer + consumer on separate
//! threads in one binary) with a controlled producer cadence so the
//! consumer actually parks on the cross-process waker primitive
//! instead of always finding items already in the ring.
//!
//! Reports per-shape:
//!   - total items shipped (= integrity check)
//!   - elapsed wall clock + throughput
//!   - approximate consumer-park count (recv_blocking calls whose
//!     wall time crossed the spin-loop threshold)
//!
//! Run:
//!     cargo run --release --example blocking_ring_demo
//!
//! For the *cross-process* shape (two binaries via file-backed MMF)
//! see `waker_xproc_producer.rs` + `waker_xproc_consumer.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::{
    BlockingMpmcRing, BlockingMpscRing, BlockingSpscRing,
};

const RING_CAPACITY: usize = 64;
const PRODUCER_PAUSE: Duration = Duration::from_micros(200);
const PRODUCER_BATCH: u64 = 16;
const PARK_THRESHOLD: Duration = Duration::from_micros(50);

fn main() {
    println!("=== Blocking ring demo (3 shapes) ===");
    println!("  capacity per ring:   {RING_CAPACITY}");
    println!("  producer pause:      {:?} every {PRODUCER_BATCH} items", PRODUCER_PAUSE);
    println!();

    demo_spsc(50_000);
    println!();
    demo_mpsc(50_000, 4);
    println!();
    demo_mpmc(50_000, 4, 2);
    println!();

    println!("=== All three blocking ring shapes verified ===");
}

fn demo_spsc(n_items: u64) {
    println!("--- BlockingSpscRing (1P / 1C) ---");
    let ring = Arc::new(
        BlockingSpscRing::create_anon(RING_CAPACITY).expect("spsc create"),
    );
    let parks = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let r_prod = Arc::clone(&ring);
    let producer = thread::spawn(move || {
        for i in 0..n_items {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            r_prod
                .send_blocking(&payload, Some(Duration::from_secs(5)))
                .expect("spsc send");
            if (i + 1) % PRODUCER_BATCH == 0 {
                thread::sleep(PRODUCER_PAUSE);
            }
        }
    });
    let r_cons = Arc::clone(&ring);
    let parks_c = Arc::clone(&parks);
    let consumer = thread::spawn(move || {
        let mut buf = [0u8; 64];
        let mut got: Vec<u64> = Vec::with_capacity(n_items as usize);
        for _ in 0..n_items {
            let t_enter = Instant::now();
            r_cons
                .recv_blocking(&mut buf, Some(Duration::from_secs(5)))
                .expect("spsc recv");
            if t_enter.elapsed() >= PARK_THRESHOLD {
                parks_c.fetch_add(1, Ordering::Relaxed);
            }
            got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        got
    });

    producer.join().unwrap();
    let got = consumer.join().unwrap();
    let elapsed = t0.elapsed();
    let parks = parks.load(Ordering::Relaxed);

    let expected: Vec<u64> = (0..n_items).collect();
    assert_eq!(got, expected, "SPSC FIFO broke");
    assert!(parks > 0, "SPSC: zero consumer parks; wake path not exercised");

    println!("  items:           {n_items}");
    println!("  elapsed:         {elapsed:?}");
    println!(
        "  throughput:      {:.2} M items/s",
        n_items as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
    println!(
        "  consumer parks:  {parks} ({:.1}% of recvs)",
        parks as f64 / n_items as f64 * 100.0,
    );
}

fn demo_mpsc(n_items: u64, n_producers: usize) {
    println!("--- BlockingMpscRing ({n_producers}P / 1C) ---");
    let (producers, consumer) =
        BlockingMpscRing::create_anon_pool(n_producers, RING_CAPACITY).expect("mpsc create");
    let per_prod = n_items / n_producers as u64;
    let total = per_prod * n_producers as u64;
    let parks = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let prod_handles: Vec<_> = producers
        .into_iter()
        .enumerate()
        .map(|(pid, p)| {
            thread::spawn(move || {
                for i in 0..per_prod {
                    let val = (pid as u64) * 1_000_000_000 + i;
                    let mut payload = [0u8; 56];
                    payload[..8].copy_from_slice(&val.to_le_bytes());
                    p.send_blocking(&payload, Some(Duration::from_secs(5)))
                        .expect("mpsc send");
                    if (i + 1) % PRODUCER_BATCH == 0 {
                        thread::sleep(PRODUCER_PAUSE);
                    }
                }
            })
        })
        .collect();

    let parks_c = Arc::clone(&parks);
    let consumer_handle = thread::spawn(move || {
        let mut buf = [0u8; 64];
        let mut got: Vec<u64> = Vec::with_capacity(total as usize);
        for _ in 0..total {
            let t_enter = Instant::now();
            consumer
                .recv_blocking(&mut buf, Some(Duration::from_secs(10)))
                .expect("mpsc recv");
            if t_enter.elapsed() >= PARK_THRESHOLD {
                parks_c.fetch_add(1, Ordering::Relaxed);
            }
            got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        got
    });

    for h in prod_handles {
        h.join().unwrap();
    }
    let mut got = consumer_handle.join().unwrap();
    let elapsed = t0.elapsed();
    let parks = parks.load(Ordering::Relaxed);

    got.sort_unstable();
    let mut expected: Vec<u64> = Vec::with_capacity(total as usize);
    for pid in 0..n_producers as u64 {
        for i in 0..per_prod {
            expected.push(pid * 1_000_000_000 + i);
        }
    }
    expected.sort_unstable();
    assert_eq!(got, expected, "MPSC delivery mismatch");
    assert!(parks > 0, "MPSC: zero consumer parks; wake path not exercised");

    println!("  items:           {total} ({n_producers} producers x {per_prod})");
    println!("  elapsed:         {elapsed:?}");
    println!(
        "  throughput:      {:.2} M items/s",
        total as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
    println!(
        "  consumer parks:  {parks} ({:.1}% of recvs)",
        parks as f64 / total as f64 * 100.0,
    );
}

fn demo_mpmc(n_items: u64, n_producers: usize, n_consumers: usize) {
    println!("--- BlockingMpmcRing ({n_producers}P / {n_consumers}C) ---");
    let (producers, consumers) =
        BlockingMpmcRing::create_anon_grid(n_producers, n_consumers, RING_CAPACITY)
            .expect("mpmc create");
    let per_prod = n_items / n_producers as u64;
    let total = per_prod * n_producers as u64;
    let parks = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let prod_handles: Vec<_> = producers
        .into_iter()
        .enumerate()
        .map(|(pid, p)| {
            thread::spawn(move || {
                for i in 0..per_prod {
                    let val = (pid as u64) * 1_000_000_000 + i;
                    let mut payload = [0u8; 56];
                    payload[..8].copy_from_slice(&val.to_le_bytes());
                    p.send_blocking(&payload, Some(Duration::from_secs(5)))
                        .expect("mpmc send");
                    if (i + 1) % PRODUCER_BATCH == 0 {
                        thread::sleep(PRODUCER_PAUSE);
                    }
                }
            })
        })
        .collect();

    let cons_handles: Vec<_> = consumers
        .into_iter()
        .enumerate()
        .map(|(cid, c)| {
            let parks_c = Arc::clone(&parks);
            let per_consumer = compute_consumer_share(n_producers, per_prod, cid, n_consumers);
            thread::spawn(move || {
                let mut buf = [0u8; 64];
                let mut got: Vec<u64> = Vec::with_capacity(per_consumer as usize);
                for _ in 0..per_consumer {
                    let t_enter = Instant::now();
                    c.recv_blocking(&mut buf, Some(Duration::from_secs(10)))
                        .expect("mpmc recv");
                    if t_enter.elapsed() >= PARK_THRESHOLD {
                        parks_c.fetch_add(1, Ordering::Relaxed);
                    }
                    got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
                }
                got
            })
        })
        .collect();

    for h in prod_handles {
        h.join().unwrap();
    }
    let mut all: Vec<u64> = cons_handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let elapsed = t0.elapsed();
    let parks = parks.load(Ordering::Relaxed);

    all.sort_unstable();
    let mut expected: Vec<u64> = Vec::with_capacity(total as usize);
    for pid in 0..n_producers as u64 {
        for i in 0..per_prod {
            expected.push(pid * 1_000_000_000 + i);
        }
    }
    expected.sort_unstable();
    assert_eq!(all, expected, "MPMC delivery mismatch");
    assert!(parks > 0, "MPMC: zero consumer parks; wake path not exercised");

    println!("  items:           {total} ({n_producers} producers x {per_prod}, {n_consumers} consumers)");
    println!("  elapsed:         {elapsed:?}");
    println!(
        "  throughput:      {:.2} M items/s",
        total as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
    println!(
        "  total parks:     {parks} ({:.1}% of recvs)",
        parks as f64 / total as f64 * 100.0,
    );
}

/// Each consumer `cid` owns rings `cid, cid + M, cid + 2M, ...`, so
/// its share of items is `per_prod * count(rings_assigned)`.
fn compute_consumer_share(
    n_producers: usize,
    per_prod: u64,
    cid: usize,
    n_consumers: usize,
) -> u64 {
    let mut count: u64 = 0;
    let mut idx = cid;
    while idx < n_producers {
        count += per_prod;
        idx += n_consumers;
    }
    count
}
