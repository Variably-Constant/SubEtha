//! E2E demonstration of the AdaptiveRing sidecar policy.
//!
//! No manual `ring.morph_to(...)` calls. The sidecar scans peer
//! counts every 10ms, the `DefaultRingShapePolicy` picks the
//! cheapest shape that fits, and the sidecar issues the morph
//! when the policy returns a target that differs from the
//! current shape.
//!
//! Lifecycle the example exercises:
//!  1. Spawn ring + sidecar. Initial shape Spsc.
//!  2. Register 1 producer + 1 consumer + push some items.
//!     Sidecar observes 1P/1C; no morph.
//!  3. Register a 2nd producer. Sidecar morphs Spsc -> Mpsc.
//!  4. Register a 2nd consumer. Sidecar morphs Mpsc -> Mpmc.
//!  5. Push items through the 2P/2C grid.
//!  6. Drop a consumer (unregister + thread exits). Sidecar
//!     morphs Mpmc -> Mpsc.
//!  7. Drop a producer. Sidecar morphs Mpsc -> Spsc.
//!  8. Final: assert produced == consumed.
//!
//! Run with:
//!     cargo run --release --example adaptive_ring_sidecar

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::{
    AdaptiveRing, AdaptiveRingSidecar, DefaultRingShapePolicy, RingShape,
};

const ITEMS_PER_STAGE: u64 = 50_000;
const RING_CAPACITY: usize = 1024;

fn main() {
    println!("=== AdaptiveRing + sidecar auto-morph E2E ===");
    println!("(zero manual morph_to calls; sidecar drives all morphs from peer counts)");
    println!();

    let ring = Arc::new(
        AdaptiveRing::create_anon(4, 4, RING_CAPACITY).expect("ring create"),
    );

    let policy = DefaultRingShapePolicy {
        hysteresis: Duration::from_millis(20),
    };
    let sidecar = AdaptiveRingSidecar::spawn(
        ring.clone(),
        policy,
        Duration::from_millis(10),
    );

    let produced = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let sum_produced = Arc::new(AtomicU64::new(0));
    let sum_consumed = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    println!("[init] shape={:?}", ring.current_shape());

    // 1 producer + 1 consumer attached. Sidecar holds the initial SPSC.
    println!();
    println!("[stage 1] register 1 producer + 1 consumer; expect SPSC");
    let p0_id = ring.register_producer().expect("p0 register");
    let c0_id = ring.register_consumer().expect("c0 register");
    let stop_c0 = Arc::new(AtomicBool::new(false));
    let cons0 = spawn_consumer(
        ring.clone(), c0_id, consumed.clone(),
        sum_consumed.clone(), stop_c0.clone(),
    );
    let prod0_done = spawn_producer(
        ring.clone(), p0_id, ITEMS_PER_STAGE,
        produced.clone(), sum_produced.clone(),
    );
    prod0_done.join().expect("p0 done");
    wait_until_drained(&consumed, ITEMS_PER_STAGE);
    thread::sleep(Duration::from_millis(50));
    println!("    shape after 1P/1C activity = {:?}", ring.current_shape());

    // Add a 2nd producer. Sidecar should morph to MPSC.
    println!();
    println!("[stage 2] register 2nd producer; expect sidecar morph -> MPSC");
    let p1_id = ring.register_producer().expect("p1 register");
    wait_for_shape(&ring, RingShape::Mpsc, Duration::from_secs(2));
    println!("    shape = {:?}", ring.current_shape());

    let prod0_again = spawn_producer(
        ring.clone(), p0_id, ITEMS_PER_STAGE,
        produced.clone(), sum_produced.clone(),
    );
    let prod1 = spawn_producer(
        ring.clone(), p1_id, ITEMS_PER_STAGE,
        produced.clone(), sum_produced.clone(),
    );
    prod0_again.join().expect("p0 again");
    prod1.join().expect("p1");
    wait_until_drained(&consumed, 3 * ITEMS_PER_STAGE);

    // Add a 2nd consumer. Sidecar should morph to MPMC.
    println!();
    println!("[stage 3] register 2nd consumer; expect sidecar morph -> MPMC");
    stop_c0.store(true, Ordering::Release);
    cons0.join().expect("c0");
    stop_c0.store(false, Ordering::Release);

    let c1_id = ring.register_consumer().expect("c1 register");
    wait_for_shape(&ring, RingShape::Mpmc, Duration::from_secs(2));
    println!("    shape = {:?}", ring.current_shape());

    let stop_consumers = Arc::new(AtomicBool::new(false));
    let cons0_b = spawn_consumer(
        ring.clone(), c0_id, consumed.clone(),
        sum_consumed.clone(), stop_consumers.clone(),
    );
    let cons1 = spawn_consumer(
        ring.clone(), c1_id, consumed.clone(),
        sum_consumed.clone(), stop_consumers.clone(),
    );
    let prod0_three = spawn_producer(
        ring.clone(), p0_id, ITEMS_PER_STAGE,
        produced.clone(), sum_produced.clone(),
    );
    let prod1_two = spawn_producer(
        ring.clone(), p1_id, ITEMS_PER_STAGE,
        produced.clone(), sum_produced.clone(),
    );
    prod0_three.join().expect("p0 three");
    prod1_two.join().expect("p1 two");
    wait_until_drained(&consumed, 5 * ITEMS_PER_STAGE);

    // Drop a consumer. Sidecar should morph MPMC -> MPSC.
    println!();
    println!("[stage 4] unregister 1 consumer; expect sidecar morph -> MPSC");
    stop_consumers.store(true, Ordering::Release);
    cons1.join().expect("c1");
    ring.unregister_consumer(c1_id);
    wait_for_shape(&ring, RingShape::Mpsc, Duration::from_secs(2));
    println!("    shape = {:?}", ring.current_shape());

    // Drop a producer. Sidecar should morph MPSC -> SPSC.
    println!();
    println!("[stage 5] unregister 1 producer; expect sidecar morph -> SPSC");
    ring.unregister_producer(p1_id);
    wait_for_shape(&ring, RingShape::Spsc, Duration::from_secs(2));
    println!("    shape = {:?}", ring.current_shape());

    // Final drain.
    stop_consumers.store(false, Ordering::Release);
    let final_drain = spawn_consumer(
        ring.clone(), c0_id, consumed.clone(),
        sum_consumed.clone(), stop_consumers.clone(),
    );
    let final_push = spawn_producer(
        ring.clone(), p0_id, ITEMS_PER_STAGE,
        produced.clone(), sum_produced.clone(),
    );
    final_push.join().expect("final push");
    wait_until_drained(&consumed, 6 * ITEMS_PER_STAGE);
    stop_consumers.store(true, Ordering::Release);
    final_drain.join().expect("final drain");
    cons0_b.join().expect("c0 b");

    let morphs = sidecar.morphs_triggered();
    let elapsed = start.elapsed();
    sidecar.shutdown();

    let total_produced = produced.load(Ordering::Acquire);
    let total_consumed = consumed.load(Ordering::Acquire);

    println!();
    println!("=== Result ===");
    println!("  total elapsed:           {elapsed:?}");
    println!("  total produced:          {total_produced}");
    println!("  total consumed:          {total_consumed}");
    println!("  sidecar morphs issued:   {morphs}");
    println!("  expected morph sequence: SPSC -> MPSC -> MPMC -> MPSC -> SPSC (4 morphs)");
    assert_eq!(
        total_produced, total_consumed,
        "INTEGRITY FAIL: produced != consumed across morph lifecycle",
    );
    assert_eq!(
        sum_produced.load(Ordering::Acquire),
        sum_consumed.load(Ordering::Acquire),
        "INTEGRITY FAIL: per-item sums diverge across morph lifecycle",
    );
    assert!(morphs >= 4,
            "sidecar should have triggered at least 4 morphs; got {morphs}");
    println!("  integrity:               PASS");
    println!("    sum_produced == sum_consumed: every item arrived exactly once");
    println!("    sidecar drove all morphs from peer-count observations");
}

fn spawn_producer(
    ring: Arc<AdaptiveRing>,
    producer_id: usize,
    items: u64,
    produced: Arc<AtomicU64>,
    sum_produced: Arc<AtomicU64>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buf = [0u8; 56];
        for i in 0..items {
            buf[..8].copy_from_slice(&i.to_le_bytes());
            buf[8] = producer_id as u8;
            while ring.try_send(producer_id, &buf).is_err() {
                std::hint::spin_loop();
            }
            produced.fetch_add(1, Ordering::Relaxed);
            sum_produced.fetch_add(i, Ordering::Relaxed);
        }
    })
}

fn spawn_consumer(
    ring: Arc<AdaptiveRing>,
    consumer_id: usize,
    consumed: Arc<AtomicU64>,
    sum_consumed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        while !stop.load(Ordering::Acquire) {
            if ring.try_recv(consumer_id, &mut out).is_ok() {
                let v = u64::from_le_bytes(out[..8].try_into().unwrap());
                consumed.fetch_add(1, Ordering::Relaxed);
                sum_consumed.fetch_add(v, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        while ring.try_recv(consumer_id, &mut out).is_ok() {
            let v = u64::from_le_bytes(out[..8].try_into().unwrap());
            consumed.fetch_add(1, Ordering::Relaxed);
            sum_consumed.fetch_add(v, Ordering::Relaxed);
        }
    })
}

fn wait_until_drained(consumed: &AtomicU64, target: u64) {
    while consumed.load(Ordering::Acquire) < target {
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_shape(ring: &AdaptiveRing, target: RingShape, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while ring.current_shape() != target {
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for shape {:?}; current = {:?}",
                target, ring.current_shape(),
            );
        }
        thread::sleep(Duration::from_millis(5));
    }
}
