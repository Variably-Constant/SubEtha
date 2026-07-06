//! End-to-end demonstration of `CapacityAdaptiveRing`.
//!
//! Runs a producer and a consumer on separate OS threads, a
//! sustained workload of N items, and a third thread driving
//! capacity morphs (grow + shrink + grow + shrink) WHILE the
//! producer is pushing and the consumer is draining. The morph
//! cadence is tight (microsecond-scale sleep between morphs) to
//! maximise exposure of the producer-vs-morph race window the
//! stale-list design closes. Asserts no item is lost or duplicated
//! by sorting the consumed-vector and comparing against the
//! complete `0..N_ITEMS` ID set.
//!
//! Run with:
//!     cargo run --release --example capacity_morph_e2e

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;

const N_ITEMS: u64 = 100_000;
const INITIAL_CAPACITY: usize = 256;
const MORPH_TARGETS: [usize; 6] = [1024, 4096, 1024, 256, 64, 512];
/// Microsecond-scale gap between morphs so the producer/consumer
/// see many morphs during the run. 200 microseconds is dense
/// enough to drive the stale-list cleanup path repeatedly within
/// a 100k-item run.
const MORPH_INTERVAL_US: u64 = 200;

fn main() {
    println!("=== CapacityAdaptiveRing E2E ===");
    println!();
    println!("Workload: {N_ITEMS} items, 1 producer + 1 consumer,");
    println!("morph cycle through capacities: {MORPH_TARGETS:?}");
    println!("(target {MORPH_INTERVAL_US}us between morphs during the run)");
    println!();

    let ring = Arc::new(
        CapacityAdaptiveRing::create_anon(1, 1, INITIAL_CAPACITY)
            .expect("create anon capacity-adaptive ring"),
    );
    ring.register_producer().expect("register producer");
    ring.register_consumer().expect("register consumer");

    let morphs_completed = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();

    let r_prod = Arc::clone(&ring);
    let producer = thread::spawn(move || {
        for i in 0..N_ITEMS {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            while r_prod.try_send(0, &payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    let r_cons = Arc::clone(&ring);
    let consumer = thread::spawn(move || {
        let mut got: Vec<u64> = Vec::with_capacity(N_ITEMS as usize);
        let mut buf = [0u8; 64];
        while got.len() < N_ITEMS as usize {
            if r_cons.try_recv(0, &mut buf).is_ok() {
                let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                got.push(v);
            } else {
                std::hint::spin_loop();
            }
        }
        got
    });

    // Morph thread: loop the MORPH_TARGETS sequence at tight
    // microsecond cadence, terminating when the parent drops its
    // Arc to the ring (strong_count <= 2 means only this thread +
    // the parent's about-to-be-dropped reference remain).
    let r_morph = Arc::clone(&ring);
    let morphs_c = Arc::clone(&morphs_completed);
    let morpher = thread::spawn(move || {
        loop {
            for target in MORPH_TARGETS {
                thread::sleep(Duration::from_micros(MORPH_INTERVAL_US));
                r_morph.morph_capacity_to(target).expect("morph succeeds");
                morphs_c.fetch_add(1, Ordering::Relaxed);
            }
            if Arc::strong_count(&r_morph) <= 2 {
                return;
            }
        }
    });

    producer.join().expect("producer thread");
    let got = consumer.join().expect("consumer thread");
    drop(ring); // releases morpher's strong_count check
    morpher.join().expect("morpher thread");

    let elapsed = t0.elapsed();

    // Integrity: every item ID in 0..N_ITEMS appeared exactly
    // once. Sort-then-compare is the correct check for this design
    // because cross-backing order is not strictly preserved (items
    // a producer pushed to OLD via a pre-swap ArcSwap snapshot can
    // be consumed AFTER items it pushed to NEW post-swap). Within
    // each backing the SPSC FIFO is preserved; sort proves "no
    // loss + no dup" which is the load-bearing invariant.
    let mut sorted = got.clone();
    sorted.sort_unstable();
    let expected: Vec<u64> = (0..N_ITEMS).collect();
    let integrity_ok = sorted == expected;

    // Reordering audit. With 1P/1C SPSC, global FIFO IS
    // preserved across morphs (the single producer's pushes are
    // sequential, and the consumer drains stale backings oldest-
    // first then active, so items pushed via a pre-swap ArcSwap
    // snapshot are popped before items pushed via a post-swap
    // snapshot). Expected value: 0. A non-zero count here would
    // indicate a bug in the stale-list ordering, not a design
    // trade-off. With multi-producer the same loop would report
    // a non-zero count (per-producer FIFO preserved, cross-
    // producer interleave weakens across morphs); that's the
    // case for a multi-producer test, not this 1P one.
    let mut regressions = 0u64;
    let mut max_seen = 0u64;
    for &v in &got {
        if v < max_seen {
            regressions += 1;
        } else {
            max_seen = v;
        }
    }

    println!();
    println!("=== Result ===");
    println!("  total elapsed:        {elapsed:?}");
    println!("  throughput:           {:.2} M items/s",
             N_ITEMS as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    println!("  morphs completed:     {}", morphs_completed.load(Ordering::Relaxed));
    println!("  items produced:       {N_ITEMS}");
    println!("  items consumed:       {}", got.len());
    println!("  cross-backing reorderings: {regressions} (expected 0 for 1P/1C; non-zero here is a bug)");
    assert_eq!(regressions, 0,
               "1P/1C must preserve global FIFO across morphs; observed reorderings is a bug");
    println!();
    println!("  integrity:            {}",
             if integrity_ok { "PASS - every ID 0..N appeared exactly once across morphs" }
             else { "FAIL - id set mismatch" });

    assert!(integrity_ok, "capacity-morph integrity check failed");
}
