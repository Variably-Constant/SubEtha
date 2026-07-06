//! MPSC + MPMC shootout: composed (N Lamport SPSC rings) vs
//! Vyukov MPMC (existing SharedRing) vs crossbeam_channel.
//!
//! Two benchmarks back-to-back:
//!
//! 1. **MPSC: 4 producers, 1 consumer, 250k items each (1M total).**
//!    Composed `SharedRingMpsc::create_anon_pool(4, ...)` vs
//!    `SharedRing` (Vyukov MPMC, used as MPSC) vs
//!    `crossbeam_channel::bounded` (MPSC).
//!
//! 2. **MPMC: 4 producers, 4 consumers, 250k items each.**
//!    Composed `SharedRingMpmc::create_anon_grid(4, 4, ...)` (one
//!    ring per producer, each consumer drains one) vs `SharedRing`
//!    Vyukov MPMC vs `crossbeam_channel::bounded` (MPMC).
//!
//! Best-of-5 trials per variant with a warmup pass first to prime
//! caches + scheduler.
//!
//! Run with:
//!     cargo run --release --example mpmc_shootout

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use subetha_cxc::shared_ring::PAYLOAD_BYTES;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;
use subetha_cxc::{SharedRing, SharedRingMpmc, SharedRingMpsc, SharedRingMpscFifo};

const PER_PRODUCER: u64 = 250_000;
const N_PRODUCERS: usize = 4;
const N_CONSUMERS_MPMC: usize = 4;
const TOTAL: u64 = PER_PRODUCER * N_PRODUCERS as u64;
const CAPACITY: usize = 4096;
const TRIALS: usize = 5;

fn main() {
    println!("MPSC + MPMC shootout: {} producers x {} items each = {} total",
             N_PRODUCERS, PER_PRODUCER, TOTAL);
    println!("Best-of-{TRIALS} per variant; one warmup pass first.");
    println!();

    eprintln!("[warmup composed mpsc N=4]: {:.2} M items/s",
              bench_composed_mpsc(4) / 1e6);
    eprintln!("[warmup composed mpmc]: {:.2} M items/s",
              bench_composed_mpmc() / 1e6);

    println!("--- MPSC crossover sweep: N in [2, 4, 8] producers -> 1 consumer ---");
    let mpsc2_composed = best_of("composed mpsc N=2", || bench_composed_mpsc(2));
    let mpsc2_fifo     = best_of("fifo mpsc N=2",     || bench_fifo_mpsc(2));
    let mpsc2_vyukov   = best_of("vyukov mpsc N=2",   || bench_vyukov_as_mpsc(2));
    let mpsc2_cb       = best_of("cb mpsc N=2",       || bench_crossbeam_mpsc(2));
    let mpsc4_composed = best_of("composed mpsc N=4", || bench_composed_mpsc(4));
    let mpsc4_fifo     = best_of("fifo mpsc N=4",     || bench_fifo_mpsc(4));
    let mpsc4_vyukov   = best_of("vyukov mpsc N=4",   || bench_vyukov_as_mpsc(4));
    let mpsc4_cb       = best_of("cb mpsc N=4",       || bench_crossbeam_mpsc(4));
    let mpsc8_composed = best_of("composed mpsc N=8", || bench_composed_mpsc(8));
    let mpsc8_fifo     = best_of("fifo mpsc N=8",     || bench_fifo_mpsc(8));
    let mpsc8_vyukov   = best_of("vyukov mpsc N=8",   || bench_vyukov_as_mpsc(8));
    let mpsc8_cb       = best_of("cb mpsc N=8",       || bench_crossbeam_mpsc(8));
    println!();

    println!("--- MPMC: {} producers -> {} consumers ---",
             N_PRODUCERS, N_CONSUMERS_MPMC);
    let mpmc_composed = best_of("composed mpmc", bench_composed_mpmc);
    let mpmc_vyukov   = best_of("vyukov mpmc",   bench_vyukov_mpmc);
    let mpmc_cb       = best_of("cb mpmc",       bench_crossbeam_mpmc);
    println!();

    println!("=== MPSC Results (items/s, higher is better) ===");
    println!();
    for (n, composed, fifo, vyukov, cb) in [
        (2, mpsc2_composed, mpsc2_fifo, mpsc2_vyukov, mpsc2_cb),
        (4, mpsc4_composed, mpsc4_fifo, mpsc4_vyukov, mpsc4_cb),
        (8, mpsc8_composed, mpsc8_fifo, mpsc8_vyukov, mpsc8_cb),
    ] {
        println!("  N={n} producers -> 1 consumer:");
        print_row("SharedRingMpsc (composed N Lamport rings)", composed);
        print_row("SharedRingMpscFifo (1 Vyukov ring, relaxed cons)", fifo);
        print_row("SharedRing (Vyukov MPMC, used as MPSC)", vyukov);
        print_row("crossbeam_channel::bounded (MPSC)", cb);
        let ratio = composed / fifo;
        let winner = if ratio > 1.0 {
            format!("Composed wins by {:.2}x", ratio)
        } else {
            format!("Fifo wins by {:.2}x", 1.0 / ratio)
        };
        println!("    Composed vs Fifo:        {ratio:.2}x  ({winner})");
        println!("    Composed vs crossbeam:   {:.2}x", composed / cb);
        println!("    Fifo vs crossbeam:       {:.2}x", fifo / cb);
        println!();
    }

    println!("MPSC crossover analysis:");
    let n2_winner = if mpsc2_composed > mpsc2_fifo { "Composed" } else { "Fifo" };
    let n4_winner = if mpsc4_composed > mpsc4_fifo { "Composed" } else { "Fifo" };
    let n8_winner = if mpsc8_composed > mpsc8_fifo { "Composed" } else { "Fifo" };
    println!("  N=2: {n2_winner} wins  | N=4: {n4_winner} wins  | N=8: {n8_winner} wins");
    println!();

    println!("=== MPMC Results (items/s, higher is better) ===");
    print_row("SharedRingMpmc (composed N x M Lamport rings)", mpmc_composed);
    print_row("SharedRing (Vyukov MPMC)", mpmc_vyukov);
    print_row("crossbeam_channel::bounded (MPMC)", mpmc_cb);
    println!("  Composed vs Vyukov MPMC: {:.2}x", mpmc_composed / mpmc_vyukov);
    println!("  Composed vs crossbeam:   {:.2}x", mpmc_composed / mpmc_cb);
}

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

fn print_row(label: &str, items_per_sec: f64) {
    let ns_per_item = 1e9 / items_per_sec;
    let m = items_per_sec / 1e6;
    println!("  {label:<48}  {ns_per_item:>6.1} ns/item  ({m:>5.2} M items/s)");
}

fn bench_composed_mpsc(n_producers: usize) -> f64 {
    let total = PER_PRODUCER * n_producers as u64;
    let (producers, consumer) =
        SharedRingMpsc::create_anon_pool(n_producers, CAPACITY).unwrap();
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
    let producer_handles: Vec<_> = producers
        .into_iter()
        .map(|p| {
            thread::spawn(move || {
                let payload = [0xABu8; SPSC_PAYLOAD_BYTES];
                for _ in 0..PER_PRODUCER {
                    while p.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < total {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    consumer_thread.join().ok();
    total as f64 / elapsed.as_secs_f64()
}

fn bench_fifo_mpsc(n_producers: usize) -> f64 {
    let total = PER_PRODUCER * n_producers as u64;
    let (producers, consumer) =
        SharedRingMpscFifo::create_anon_pool(n_producers, CAPACITY).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || {
        let mut out = [0u8; PAYLOAD_BYTES];
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
    let producer_handles: Vec<_> = producers
        .into_iter()
        .map(|p| {
            thread::spawn(move || {
                let payload = [0xABu8; 16];
                for _ in 0..PER_PRODUCER {
                    while p.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < total {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    consumer_thread.join().ok();
    total as f64 / elapsed.as_secs_f64()
}

fn bench_vyukov_as_mpsc(n_producers: usize) -> f64 {
    let total = PER_PRODUCER * n_producers as u64;
    let ring = Arc::new(SharedRing::create_anon(CAPACITY).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let ring_c = ring.clone();
    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || {
        let mut out = [0u8; PAYLOAD_BYTES];
        while !stop_c.load(Ordering::Acquire) {
            if ring_c.try_pop(&mut out).is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while ring_c.try_pop(&mut out).is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    let producer_handles: Vec<_> = (0..n_producers)
        .map(|_| {
            let ring_p = ring.clone();
            thread::spawn(move || {
                let payload = [0xABu8; 16];
                for _ in 0..PER_PRODUCER {
                    while ring_p.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < total {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    consumer_thread.join().ok();
    total as f64 / elapsed.as_secs_f64()
}

fn bench_crossbeam_mpsc(n_producers: usize) -> f64 {
    let total = PER_PRODUCER * n_producers as u64;
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let rx_c = rx.clone();
    let stop_c = stop.clone();
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || {
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
    let producer_handles: Vec<_> = (0..n_producers)
        .map(|_| {
            let tx_p = tx.clone();
            thread::spawn(move || {
                let payload = [0xABu8; 16];
                for _ in 0..PER_PRODUCER {
                    while tx_p.try_send(payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < total {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    drop(tx);
    consumer_thread.join().ok();
    total as f64 / elapsed.as_secs_f64()
}

fn bench_composed_mpmc() -> f64 {
    let (producers, consumers) =
        SharedRingMpmc::create_anon_grid(N_PRODUCERS, N_CONSUMERS_MPMC, CAPACITY).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = consumers
        .into_iter()
        .map(|c| {
            let stop_c = stop.clone();
            let consumed_c = consumed.clone();
            thread::spawn(move || {
                let mut out = [0u8; SPSC_PAYLOAD_BYTES];
                while !stop_c.load(Ordering::Acquire) {
                    if c.try_pop(&mut out).is_ok() {
                        consumed_c.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::hint::spin_loop();
                    }
                }
                while c.try_pop(&mut out).is_ok() {
                    consumed_c.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let t0 = Instant::now();
    let producer_handles: Vec<_> = producers
        .into_iter()
        .map(|p| {
            thread::spawn(move || {
                let payload = [0xABu8; SPSC_PAYLOAD_BYTES];
                for _ in 0..PER_PRODUCER {
                    while p.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < TOTAL {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    for h in consumer_handles { h.join().ok(); }
    TOTAL as f64 / elapsed.as_secs_f64()
}

fn bench_vyukov_mpmc() -> f64 {
    let ring = Arc::new(SharedRing::create_anon(CAPACITY).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..N_CONSUMERS_MPMC)
        .map(|_| {
            let ring_c = ring.clone();
            let stop_c = stop.clone();
            let consumed_c = consumed.clone();
            thread::spawn(move || {
                let mut out = [0u8; PAYLOAD_BYTES];
                while !stop_c.load(Ordering::Acquire) {
                    if ring_c.try_pop(&mut out).is_ok() {
                        consumed_c.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::hint::spin_loop();
                    }
                }
                while ring_c.try_pop(&mut out).is_ok() {
                    consumed_c.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let t0 = Instant::now();
    let producer_handles: Vec<_> = (0..N_PRODUCERS)
        .map(|_| {
            let ring_p = ring.clone();
            thread::spawn(move || {
                let payload = [0xABu8; 16];
                for _ in 0..PER_PRODUCER {
                    while ring_p.try_push(&payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < TOTAL {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    for h in consumer_handles { h.join().ok(); }
    TOTAL as f64 / elapsed.as_secs_f64()
}

fn bench_crossbeam_mpmc() -> f64 {
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..N_CONSUMERS_MPMC)
        .map(|_| {
            let rx_c = rx.clone();
            let stop_c = stop.clone();
            let consumed_c = consumed.clone();
            thread::spawn(move || {
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
            })
        })
        .collect();

    let t0 = Instant::now();
    let producer_handles: Vec<_> = (0..N_PRODUCERS)
        .map(|_| {
            let tx_p = tx.clone();
            thread::spawn(move || {
                let payload = [0xABu8; 16];
                for _ in 0..PER_PRODUCER {
                    while tx_p.try_send(payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in producer_handles { h.join().ok(); }
    while consumed.load(Ordering::Acquire) < TOTAL {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    drop(tx);
    for h in consumer_handles { h.join().ok(); }
    TOTAL as f64 / elapsed.as_secs_f64()
}
