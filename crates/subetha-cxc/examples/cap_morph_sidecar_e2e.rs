//! End-to-end demo of `CapacityAdaptiveRingSidecar`.
//!
//! Runs a producer/consumer workload while the
//! `CapacityAdaptiveRingSidecar` watches the ring's fill ratio
//! and grows / shrinks the underlying capacity automatically.
//! The default policy doubles capacity when fill_ratio >= 0.85
//! and halves it when fill_ratio <= 0.10, both gated by a
//! 100 ms hysteresis cooldown.
//!
//! The demo intentionally creates a "burst then drain" workload:
//!
//! 1. Producer pushes a burst of items faster than the consumer
//!    can drain - fill_ratio crosses the grow threshold - sidecar
//!    grows capacity.
//! 2. After the burst, producer pauses; consumer continues -
//!    fill_ratio crosses the shrink threshold - sidecar shrinks
//!    capacity.
//! 3. Repeat several cycles.
//!
//! The harness prints capacity changes as they happen so a human
//! can SEE the sidecar's decisions reflected in the ring state.
//! At the end it asserts at least one grow and one shrink fired
//! to prove the auto-morph path is wired end-to-end.
//!
//! Run:
//!     cargo run --release --example cap_morph_sidecar_e2e

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::capacity_adaptive_ring::{
    CapacityAdaptiveRing, CapacityAdaptiveRingSidecar, DefaultCapacityPolicy,
};

const INITIAL_CAPACITY: usize = 256;
const N_CYCLES: usize = 6;
const BURST_ITEMS: u64 = 8_000;
const DRAIN_PAUSE_MS: u64 = 250;

fn main() {
    println!("=== CapacityAdaptiveRingSidecar E2E ===");
    println!("Initial capacity: {INITIAL_CAPACITY}");
    println!("Cycles: {N_CYCLES} (each: producer bursts {BURST_ITEMS} items,");
    println!("then pauses {DRAIN_PAUSE_MS}ms while consumer drains)");
    println!();

    let ring = Arc::new(
        CapacityAdaptiveRing::create_anon(1, 1, INITIAL_CAPACITY)
            .expect("create_anon"),
    );
    ring.register_producer().expect("register producer");
    ring.register_consumer().expect("register consumer");

    // Spawn the sidecar. Default policy: grow at >=85% fill,
    // shrink at <=10%, 100 ms hysteresis between morphs.
    let policy = DefaultCapacityPolicy {
        grow_at: 0.75,
        shrink_at: 0.10,
        min_capacity: 64,
        max_capacity: 8192,
        hysteresis: Duration::from_millis(50),
    };
    let sidecar = CapacityAdaptiveRingSidecar::spawn(
        Arc::clone(&ring),
        policy,
        Duration::from_millis(10),
    );

    // Capacity-change observer: a second thread polls
    // current_capacity() and prints transitions.
    let stop_obs = Arc::new(AtomicBool::new(false));
    let stop_obs_c = Arc::clone(&stop_obs);
    let ring_obs = Arc::clone(&ring);
    let t0 = Instant::now();
    let obs_h = thread::spawn(move || {
        let mut last_cap = ring_obs.current_capacity();
        println!("[{:6.3}s] start cap = {last_cap}", t0.elapsed().as_secs_f64());
        while !stop_obs_c.load(Ordering::Acquire) {
            let now_cap = ring_obs.current_capacity();
            if now_cap != last_cap {
                let direction = if now_cap > last_cap { "GROW" } else { "SHRINK" };
                println!(
                    "[{:6.3}s] {direction} {} -> {}",
                    t0.elapsed().as_secs_f64(),
                    last_cap,
                    now_cap
                );
                last_cap = now_cap;
            }
            thread::sleep(Duration::from_millis(5));
        }
    });

    // Producer: cycles of burst-then-pause.
    let r_prod = Arc::clone(&ring);
    let producer = thread::spawn(move || {
        for cycle in 0..N_CYCLES {
            for i in 0..BURST_ITEMS {
                let mut payload = [0u8; 56];
                payload[..8].copy_from_slice(&((cycle as u64) << 32 | i).to_le_bytes());
                while r_prod.try_send(0, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
            // Pause to let the consumer drain. The drop in
            // fill_ratio is what triggers the SHRINK decision.
            thread::sleep(Duration::from_millis(DRAIN_PAUSE_MS));
        }
    });

    // Consumer: slow drain (sleep between batches) so the ring
    // fills up enough during a burst for the grow threshold to
    // cross. The slow drain is intentional - real workloads
    // have consumers that are slower than producers.
    let r_cons = Arc::clone(&ring);
    let total_items = N_CYCLES as u64 * BURST_ITEMS;
    let drained = Arc::new(AtomicU64::new(0));
    let drained_c = Arc::clone(&drained);
    let consumer = thread::spawn(move || {
        let mut buf = [0u8; 64];
        let mut count: u64 = 0;
        while count < total_items {
            // Drain a small batch then sleep briefly.
            for _ in 0..32 {
                if count >= total_items {
                    break;
                }
                while r_cons.try_recv(0, &mut buf).is_err() {
                    std::hint::spin_loop();
                }
                count += 1;
            }
            drained_c.store(count, Ordering::Release);
            thread::sleep(Duration::from_micros(50));
        }
    });

    producer.join().expect("producer thread");
    consumer.join().expect("consumer thread");

    // Let the sidecar see the final drained state and apply one
    // more shrink decision if applicable.
    thread::sleep(Duration::from_millis(150));

    stop_obs.store(true, Ordering::Release);
    obs_h.join().expect("observer thread");

    let morphs = sidecar.morphs_triggered();
    sidecar.shutdown();

    println!();
    println!("=== Result ===");
    println!("  total items produced/consumed:  {total_items}");
    println!("  drained:                        {}", drained.load(Ordering::Acquire));
    println!("  final capacity:                 {}", ring.current_capacity());
    println!("  sidecar morphs triggered:       {morphs}");
    println!();

    assert_eq!(
        drained.load(Ordering::Acquire),
        total_items,
        "all items must be drained"
    );
    assert!(
        morphs >= 2,
        "sidecar must have triggered at least one grow and one shrink (observed {morphs})"
    );
    println!("PASS - sidecar auto-morphed the capacity {morphs} times");
}
