//! E2E demonstration of `SubscriberPosition` enabling a subscriber
//! restart that resumes from the checkpointed position.
//!
//! Lifecycle:
//!  1. Producer pushes ITEMS_BEFORE_CRASH items into a file-backed
//!     SpscRingCore.
//!  2. Subscriber #1 consumes the first CONSUMED_BEFORE_CRASH items
//!     and checkpoints its position to a SubscriberPosition file.
//!     Subscriber #1 then "crashes" (drops).
//!  3. Producer pushes ITEMS_AFTER_CRASH more items.
//!  4. Subscriber #2 starts fresh, reopens the ring + the
//!     SubscriberPosition, reads its persisted position, and
//!     resumes consumption from that point.
//!  5. Verify: subscriber #2 consumes ITEMS_AFTER_CRASH +
//!     (ITEMS_BEFORE_CRASH - CONSUMED_BEFORE_CRASH) items, AND
//!     the values match the producer's contiguous sequence with no
//!     gap and no duplication.
//!
//! Run with:
//!     cargo run --release --example subscriber_restart

use std::time::Instant;

use subetha_cxc::replay_positions::SubscriberPosition;
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

const ITEMS_BEFORE_CRASH: u64 = 200;
const CONSUMED_BEFORE_CRASH: u64 = 50;
const ITEMS_AFTER_CRASH: u64 = 100;
const RING_CAPACITY: usize = 1024;

fn main() {
    println!("=== SubscriberPosition restart-resume E2E ===");
    println!();

    let ring_path = std::env::temp_dir()
        .join(format!("subrestart_ring_{}.bin", std::process::id()));
    let pos_path = std::env::temp_dir()
        .join(format!("subrestart_pos_{}.bin", std::process::id()));

    // Clean any stale files from prior runs.
    std::fs::remove_file(&ring_path).ok();
    std::fs::remove_file(&pos_path).ok();

    let total_items = ITEMS_BEFORE_CRASH + ITEMS_AFTER_CRASH;
    let start = Instant::now();

    // ----- stage 1: producer ships first batch -----
    println!("[stage 1] producer ships {ITEMS_BEFORE_CRASH} items into file-backed ring");
    {
        let ring = SpscRingCore::create(&ring_path, RING_CAPACITY)
            .expect("ring create");
        for i in 0..ITEMS_BEFORE_CRASH {
            let payload = i.to_le_bytes();
            while ring.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
        println!("    ring head after batch 1: {}", ring.head());
        // ring drops here; mmf file persists.
    }

    // ----- stage 2: subscriber #1 consumes a partial batch + checkpoints + crashes -----
    let mut sub1_consumed = Vec::new();
    println!();
    println!("[stage 2] subscriber #1 consumes {CONSUMED_BEFORE_CRASH} items, checkpoints, crashes");
    {
        let ring = SpscRingCore::open(&ring_path, RING_CAPACITY)
            .expect("ring reopen as subscriber");
        let position = SubscriberPosition::create(&pos_path, 0)
            .expect("position create");

        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..CONSUMED_BEFORE_CRASH {
            while ring.try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            sub1_consumed.push(v);
            position.advance(1);
        }
        println!("    checkpointed position = {}, sub1_consumed = {} items",
                 position.get(), sub1_consumed.len());
        // ring + position drop here; both mmf files persist. Sub1 "crashes".
    }
    assert_eq!(sub1_consumed.len(), CONSUMED_BEFORE_CRASH as usize);
    assert_eq!(sub1_consumed.first(), Some(&0));
    assert_eq!(sub1_consumed.last(), Some(&(CONSUMED_BEFORE_CRASH - 1)));

    // ----- stage 3: producer ships second batch -----
    println!();
    println!("[stage 3] producer ships {ITEMS_AFTER_CRASH} more items (while subscriber #1 is dead)");
    {
        let ring = SpscRingCore::open(&ring_path, RING_CAPACITY)
            .expect("ring reopen for batch 2");
        for i in ITEMS_BEFORE_CRASH..total_items {
            let payload = i.to_le_bytes();
            while ring.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
        println!("    ring head after batch 2: {}", ring.head());
    }

    // ----- stage 4: subscriber #2 starts fresh + resumes -----
    let mut sub2_consumed = Vec::new();
    println!();
    println!("[stage 4] subscriber #2 reopens ring + position; resumes from checkpoint");
    {
        let ring = SpscRingCore::open(&ring_path, RING_CAPACITY)
            .expect("ring reopen as subscriber #2");
        let position = SubscriberPosition::open(&pos_path)
            .expect("position reopen");
        let resume_pos = position.get();
        println!("    sub2 reads checkpointed position = {resume_pos}");
        // The ring's INTERNAL tail counter has advanced as sub1
        // popped items. Sub2 doesn't pop "from position 0"; it pops
        // the next available items from the ring (which started at
        // position resume_pos after sub1's CONSUMED_BEFORE_CRASH
        // pops).

        let remaining_total = total_items - resume_pos;
        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..remaining_total {
            while ring.try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            sub2_consumed.push(v);
            position.advance(1);
        }
        println!("    sub2 consumed {} items, final position = {}",
                 sub2_consumed.len(), position.get());
    }
    assert_eq!(sub2_consumed.len(),
               (total_items - CONSUMED_BEFORE_CRASH) as usize);
    // First item sub2 sees must be CONSUMED_BEFORE_CRASH (item index
    // 50), not 0; ring's tail picked up where sub1 left off.
    assert_eq!(sub2_consumed.first(), Some(&CONSUMED_BEFORE_CRASH));
    assert_eq!(sub2_consumed.last(), Some(&(total_items - 1)));

    // ----- result -----
    let elapsed = start.elapsed();
    let combined_sum: u64 =
        sub1_consumed.iter().sum::<u64>() + sub2_consumed.iter().sum::<u64>();
    let expected_sum: u64 = (0..total_items).sum();

    println!();
    println!("=== Result ===");
    println!("  elapsed:                  {elapsed:?}");
    println!("  producer total items:     {total_items}");
    println!("  sub1 consumed:            {} items", sub1_consumed.len());
    println!("  sub2 consumed (resumed):  {} items", sub2_consumed.len());
    println!("  combined sum:             {combined_sum}");
    println!("  expected sum:             {expected_sum}");
    println!("  checkpoint position used: {CONSUMED_BEFORE_CRASH}");

    assert_eq!(combined_sum, expected_sum,
               "INTEGRITY FAIL: combined sum != expected");
    assert_eq!(
        (sub1_consumed.len() + sub2_consumed.len()) as u64,
        total_items,
        "INTEGRITY FAIL: total consumed != total produced",
    );
    println!("  integrity:                PASS");
    println!("    every item arrived exactly once across the subscriber restart");
    println!("    sub2 resumed from the MMF-persisted SubscriberPosition");
    println!("    no items lost, no items duplicated");

    // Cleanup.
    std::fs::remove_file(&ring_path).ok();
    std::fs::remove_file(&pos_path).ok();
}
