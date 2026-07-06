//! SPSC throughput shootout: SubEtha's SharedRing SPSC fast path + MPMC path
//! against the in-process Rust channel field - crossbeam_channel, flume,
//! rtrb (SPSC-specialized), and std::sync::mpsc::sync_channel.
//!
//! 1 producer, 1 consumer, 16-byte payloads, N items. The producer
//! busy-waits on full; the consumer busy-waits on empty. Same shape
//! as the bench in benches/shared_ring.rs but in a one-shot example
//! so we can iterate fast without Criterion's measurement framing.
//!
//! Reports total elapsed + per-item ns + items/s for each variant
//! plus the ratio against the SHARED_RING.md baseline.
//!
//! Run with:
//!     cargo run --release --example spsc_shootout

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use subetha_cxc::shared_ring::PAYLOAD_BYTES;
use subetha_cxc::{SharedRing, SharedRingSpsc};
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

const N: u64 = 1_000_000;
const CAPACITY: usize = 4096;

const TRIALS: usize = 5;

fn best_of(label: &str, mut f: impl FnMut() -> f64) -> f64 {
    let mut best = 0.0f64;
    for trial in 0..TRIALS {
        let throughput = f();
        if throughput > best { best = throughput; }
        eprintln!("  [{label} trial {trial}]: {:.2} M items/s",
                  throughput / 1e6);
    }
    best
}

fn main() {
    println!("SPSC shootout: {N} items, 16-byte payloads, ring capacity {CAPACITY}");
    println!("(producer + consumer on separate threads; busy-spin on full/empty)");
    println!("Best-of-{TRIALS} per variant; one warmup pass first.");
    println!();

    // Warmup: prime caches + scheduler. Print the number rather than
    // discarding it so the value carries an observable side-effect.
    eprintln!("  [warmup mpmc anon]: {:.2} M items/s",
              bench_subetha_mpmc_anon() / 1e6);
    eprintln!("  [warmup crossbeam]: {:.2} M items/s",
              bench_crossbeam() / 1e6);
    eprintln!("  [warmup flume]: {:.2} M items/s", bench_flume() / 1e6);
    eprintln!("  [warmup rtrb]: {:.2} M items/s", bench_rtrb() / 1e6);
    eprintln!("  [warmup std mpsc]: {:.2} M items/s", bench_std_mpsc() / 1e6);

    let lamport   = best_of("lamport pair",  bench_lamport_pair);
    let mpmc_anon = best_of("mpmc anon",  bench_subetha_mpmc_anon);
    let spsc_anon = best_of("spsc anon",  bench_subetha_spsc_anon);
    let mpmc_file = best_of("mpmc file",  bench_subetha_mpmc_file);
    let spsc_file = best_of("spsc file",  bench_subetha_spsc_file);
    let cb        = best_of("crossbeam",  bench_crossbeam);
    let flume_r   = best_of("flume",      bench_flume);
    let rtrb_r    = best_of("rtrb",       bench_rtrb);
    let stdmpsc_r = best_of("std mpsc",   bench_std_mpsc);

    println!();
    println!("=== Results (items/s, higher is better) ===");
    println!("-- SubEtha --");
    print_row("SharedRingSpsc::create_anon_pair (Lamport)", lamport);
    print_row("SharedRing MPMC (anon, try_push/try_pop)", mpmc_anon);
    print_row("SharedRing SPSC (anon, try_push_spsc/try_pop_spsc)", spsc_anon);
    print_row("SharedRing MPMC (file, try_push/try_pop)", mpmc_file);
    print_row("SharedRing SPSC (file, try_push_spsc/try_pop_spsc)", spsc_file);
    println!("-- in-process channel field --");
    print_row("crossbeam_channel::bounded(4096)", cb);
    print_row("flume::bounded(4096)", flume_r);
    print_row("rtrb::RingBuffer (SPSC-specialized)", rtrb_r);
    print_row("std::sync::mpsc::sync_channel(4096)", stdmpsc_r);
    println!();

    // The SubEtha Lamport SPSC pair against each in-process channel contender.
    // > 1 means the SubEtha SPSC ring is faster on this host.
    println!("SubEtha Lamport SPSC pair vs the channel field (>1 means SubEtha wins):");
    println!("  vs crossbeam_channel:  {:.2}x", lamport / cb);
    println!("  vs flume:              {:.2}x", lamport / flume_r);
    println!("  vs rtrb (SPSC peer):   {:.2}x", lamport / rtrb_r);
    println!("  vs std::sync::mpsc:    {:.2}x", lamport / stdmpsc_r);
    println!();
    println!("Internal comparisons:");
    println!("  Lamport pair vs Vyukov SPSC fast path:  {:.2}x", lamport / spsc_anon);
    println!("  SPSC fast path vs MPMC (anon):          {:.2}x", spsc_anon / mpmc_anon);
    println!("  SPSC fast path vs MPMC (file):          {:.2}x", spsc_file / mpmc_file);
    println!("  Anon vs file backing (MPMC):            {:.2}x", mpmc_anon / mpmc_file);
}

fn bench_lamport_pair() -> f64 {
    let (producer, consumer) = SharedRingSpsc::create_anon_pair(CAPACITY).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || {
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        while !stop_c.load(Ordering::Acquire) {
            if consumer.try_pop(&mut out).is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while consumer.try_pop(&mut out).is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while producer.try_push(&payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    consumer_thread.join().ok();

    N as f64 / elapsed.as_secs_f64()
}

fn print_row(label: &str, items_per_sec: f64) {
    let ns_per_item = 1e9 / items_per_sec;
    let m_items_per_sec = items_per_sec / 1e6;
    println!("  {label:<48}  {ns_per_item:>6.1} ns/item  ({m_items_per_sec:>5.2} M items/s)");
}

fn bench_subetha_mpmc_anon() -> f64 {
    let ring = Arc::new(SharedRing::create_anon(CAPACITY).unwrap());
    run_subetha_bench(ring, |r, payload| r.try_push(payload), |r, out| r.try_pop(out))
}

fn bench_subetha_spsc_anon() -> f64 {
    let ring = Arc::new(SharedRing::create_anon(CAPACITY).unwrap());
    run_subetha_bench(
        ring,
        |r, payload| r.try_push_spsc(payload),
        |r, out| r.try_pop_spsc(out),
    )
}

fn bench_subetha_mpmc_file() -> f64 {
    let path = std::env::temp_dir().join("subetha_spsc_shootout_mpmc.bin");
    std::fs::remove_file(&path).ok();
    let ring = Arc::new(SharedRing::create(&path, CAPACITY).unwrap());
    let r = run_subetha_bench(ring, |r, p| r.try_push(p), |r, o| r.try_pop(o));
    std::fs::remove_file(&path).ok();
    r
}

fn bench_subetha_spsc_file() -> f64 {
    let path = std::env::temp_dir().join("subetha_spsc_shootout_spsc.bin");
    std::fs::remove_file(&path).ok();
    let ring = Arc::new(SharedRing::create(&path, CAPACITY).unwrap());
    let r = run_subetha_bench(
        ring,
        |r, p| r.try_push_spsc(p),
        |r, o| r.try_pop_spsc(o),
    );
    std::fs::remove_file(&path).ok();
    r
}

fn run_subetha_bench<P, C>(
    ring: Arc<SharedRing>,
    push: P,
    pop: C,
) -> f64
where
    P: Fn(&SharedRing, &[u8]) -> Result<(), subetha_cxc::RingError> + Send + Sync + 'static,
    C: Fn(&SharedRing, &mut [u8]) -> Result<usize, subetha_cxc::RingError> + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let ring_c = ring.clone();
    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer = thread::spawn(move || {
        let mut buf = [0u8; PAYLOAD_BYTES];
        while !stop_c.load(Ordering::Acquire) {
            if pop(&ring_c, &mut buf).is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        // Drain remainder after stop.
        while pop(&ring_c, &mut buf).is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while push(&ring, &payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    consumer.join().ok();

    N as f64 / elapsed.as_secs_f64()
}

fn bench_crossbeam() -> f64 {
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let rx_c = rx.clone();
    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer = thread::spawn(move || {
        while !stop_c.load(Ordering::Acquire) {
            if rx_c.try_recv().is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while rx_c.try_recv().is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while tx.try_send(payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    drop(tx);
    consumer.join().ok();

    N as f64 / elapsed.as_secs_f64()
}

// flume: bounded MPSC/MPMC channel. Same [u8; 16] payload, same CAPACITY,
// same busy-spin on full/empty as the crossbeam contender.
fn bench_flume() -> f64 {
    let (tx, rx) = flume::bounded::<[u8; 16]>(CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let rx_c = rx.clone();
    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer = thread::spawn(move || {
        while !stop_c.load(Ordering::Acquire) {
            if rx_c.try_recv().is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while rx_c.try_recv().is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while tx.try_send(payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    drop(tx);
    consumer.join().ok();

    N as f64 / elapsed.as_secs_f64()
}

// rtrb: SPSC-specialized real-time ring buffer - the closest peer to the
// Lamport SPSC pair. Split Producer/Consumer halves, same payload/capacity/spin.
fn bench_rtrb() -> f64 {
    let (mut producer, mut consumer) = rtrb::RingBuffer::<[u8; 16]>::new(CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || {
        while !stop_c.load(Ordering::Acquire) {
            if consumer.pop().is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while consumer.pop().is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while producer.push(payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    consumer_thread.join().ok();

    N as f64 / elapsed.as_secs_f64()
}

// std::sync::mpsc::sync_channel: the standard-library bounded channel - the
// baseline every Rust developer already has. Same payload/capacity/spin.
fn bench_std_mpsc() -> f64 {
    let (tx, rx) = std::sync::mpsc::sync_channel::<[u8; 16]>(CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer = thread::spawn(move || {
        while !stop_c.load(Ordering::Acquire) {
            if rx.try_recv().is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while rx.try_recv().is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while tx.try_send(payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    drop(tx);
    consumer.join().ok();

    N as f64 / elapsed.as_secs_f64()
}
