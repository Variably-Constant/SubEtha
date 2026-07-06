//! End-to-end demonstration of the SharedRing stuck-slot recovery
//! protocol. Simulates the crashed-producer pathology, observes
//! the consumer hanging on Empty, runs the sidecar-equivalent
//! recovery, and observes the consumer drain past the recovered
//! slot.
//!
//! What is the stuck-slot pathology?
//! ---------------------------------
//! Vyukov MPMC (what SharedRing implements) has a narrow liveness
//! window. A producer claims a slot by CAS-ing `producer_seq` from
//! `pos` to `pos + 1`. It then writes the payload and does the
//! Release-store on `slot.sequence` to publish. If the producer
//! crashes between the CAS and the Release-store, the slot is
//! "claimed but never published":
//!
//!   * `producer_seq` has advanced past `pos`.
//!   * `slot[pos % cap].sequence` is still `pos` (initial value).
//!   * Consumer at `pos` reads sequence, sees `pos != pos + 1`,
//!     decides `Empty`, spins forever.
//!   * Future pushes land at `pos + 1`, `pos + 2`, ... so the ring
//!     keeps working, but the consumer is stuck behind the hole.
//!
//! The recovery protocol:
//!  1. Sidecar's observation analysis detects elevated Empty rate
//!     while `producer_seq > consumer_seq` (this demo simulates
//!     that decision with a fixed wait threshold).
//!  2. Sidecar calls `next_stuck_slot(from)` to walk the claimed-
//!     but-undrained window and find the stuck position.
//!  3. Sidecar calls `heal_stuck_slot(pos)`. One atomic CAS
//!     advances `slot.sequence` from `pos` to `pos + 1`.
//!  4. Consumer drains the healed slot on its next `try_pop`.
//!
//! Hot-path cost: zero. The recovery method is dormant until the
//! sidecar wakes it up.
//!
//! Run with:
//!     cargo run --release --example stuck_slot_recovery

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::shared_ring::PAYLOAD_BYTES;
use subetha_cxc::SharedRing;

fn main() {
    println!("=== Stuck-slot recovery E2E ===");
    println!();

    // Build a ring with three healthy items plus one stuck slot at
    // position 3. The stuck slot mimics a producer that crashed
    // between CAS-claiming and Release-publishing.
    let ring = Arc::new(SharedRing::create_anon(8).unwrap());

    for i in 0..3u32 {
        let mut buf = [0u8; PAYLOAD_BYTES];
        buf[..4].copy_from_slice(&i.to_le_bytes());
        ring.try_push(&buf).expect("normal push");
    }
    println!("[setup] pushed 3 healthy items at positions 0, 1, 2");

    // Inject the stuck slot at pos=3 by advancing producer_seq
    // without writing the slot's Release-store. This is exactly
    // the post-crash state of a producer that died after the
    // claim CAS but before publishing.
    let header = ring.header();
    let claim_pos = header.producer_seq.fetch_add(1, Ordering::AcqRel);
    println!("[setup] simulated crashed producer claimed slot {claim_pos} \
              (producer_seq -> {}, slot never published)", claim_pos + 1);
    assert_eq!(claim_pos, 3);
    println!("[setup] producer_seq = {}, consumer_seq = {}, approx_len = {}",
             ring.producer_seq(), ring.consumer_seq(), ring.approx_len());
    println!();

    // Start a consumer that drains the ring. It will drain the
    // three healthy items, then spin on Empty against the stuck
    // slot at position 3.
    let drained = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let ring_for_consumer = ring.clone();
    let drained_for_consumer = drained.clone();
    let stop_for_consumer = stop.clone();

    let consumer = thread::spawn(move || {
        let mut out = [0u8; PAYLOAD_BYTES];
        let mut empty_streak: u64 = 0;
        let mut max_empty_streak: u64 = 0;
        while !stop_for_consumer.load(Ordering::Acquire) {
            match ring_for_consumer.try_pop(&mut out) {
                Ok(_) => {
                    let v = u32::from_le_bytes(out[..4].try_into().unwrap());
                    let n = drained_for_consumer.fetch_add(1, Ordering::AcqRel);
                    println!("[consumer] drained item #{n}: payload value = {v} \
                              (after {empty_streak} consecutive Empty results)");
                    empty_streak = 0;
                }
                Err(_) => {
                    empty_streak += 1;
                    if empty_streak > max_empty_streak {
                        max_empty_streak = empty_streak;
                    }
                    if empty_streak.is_power_of_two() && empty_streak >= 1024 {
                        println!("[consumer] STUCK: {empty_streak} consecutive Empty \
                                  results, but approx_len = {}",
                                 ring_for_consumer.approx_len());
                    }
                    std::hint::spin_loop();
                }
            }
        }
        max_empty_streak
    });

    // Wait until the consumer has drained the three healthy items
    // and is clearly stuck against the missing item at position 3.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && drained.load(Ordering::Acquire) < 3 {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        drained.load(Ordering::Acquire),
        3,
        "consumer should have drained the 3 healthy items",
    );
    thread::sleep(Duration::from_millis(50));
    let stuck_pre = drained.load(Ordering::Acquire);
    println!();
    println!("[sidecar] consumer is wedged at {stuck_pre} drained \
              (approx_len reports {} items waiting)", ring.approx_len());
    println!();

    // Run the recovery protocol the sidecar would run in production:
    // scan for the stuck slot, then heal it.
    let scan_from = ring.consumer_seq();
    println!("[sidecar] scanning [consumer_seq={}, producer_seq={}) \
              for stuck slots", scan_from, ring.producer_seq());
    let stuck_pos = ring.next_stuck_slot(scan_from)
        .expect("sidecar must find the stuck slot");
    println!("[sidecar] found stuck slot at position {stuck_pos}");

    let healed = ring.heal_stuck_slot(stuck_pos).unwrap();
    println!("[sidecar] heal_stuck_slot({stuck_pos}) returned {healed} \
              (true = CAS pos->pos+1 succeeded)");
    assert!(healed, "heal must succeed on a genuinely stuck slot");
    println!();

    // Push one more item to confirm the ring is fully functional
    // after the heal, and wait for the consumer to drain everything.
    let mut buf = [0u8; PAYLOAD_BYTES];
    buf[..4].copy_from_slice(&99u32.to_le_bytes());
    ring.try_push(&buf).expect("post-heal push");
    println!("[main] pushed follow-up item (value=99) at new position");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && drained.load(Ordering::Acquire) < 5 {
        thread::sleep(Duration::from_millis(10));
    }

    stop.store(true, Ordering::Release);
    let max_empty_streak = consumer.join().unwrap();
    let total_drained = drained.load(Ordering::Acquire);
    println!();
    println!("=== Result ===");
    println!("  items drained:           {total_drained}  (expected 5)");
    println!("  max consecutive Empty:   {max_empty_streak}  (peak while wedged)");
    assert_eq!(total_drained, 5,
               "consumer should have drained 3 healthy + 1 healed + 1 follow-up");
    println!("  heal_stuck_slot:         actually unblocked the consumer");
    println!("  hot-path overhead:       zero (no per-op recovery check)");
}
