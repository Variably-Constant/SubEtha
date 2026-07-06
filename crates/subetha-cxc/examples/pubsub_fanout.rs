//! E2E demonstration of `PubSubRing`: one producer fans out to
//! three subscribers, each with its own MMF-resident position. One
//! subscriber lags deliberately to verify position independence.
//!
//! Lifecycle:
//!  1. Construct a PubSubRing with sufficient capacity.
//!  2. Create three SubscriberPositions backing three subscribers.
//!  3. Producer publishes N_ITEMS items as values 0..N_ITEMS.
//!  4. Subscribers A and B drain every item (each in its own thread).
//!  5. Subscriber C uses skip() to drain only the even-indexed
//!     items, demonstrating per-subscriber position control.
//!  6. Verify: A and B each consume all N_ITEMS with the full sum;
//!     C consumes N_ITEMS / 2 with the even-only sum.
//!
//! Run with:
//!     cargo run --release --example pubsub_fanout

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use subetha_cxc::protocol_pubsub::{
    PubSubReadError, PubSubRing, PubSubSubscriber, PUBSUB_PAYLOAD_BYTES,
};
use subetha_cxc::replay_positions::SubscriberPosition;

const N_ITEMS: u64 = 10_000;
const CAPACITY: usize = 16_384;

fn main() {
    println!("=== PubSubRing fanout E2E (1 producer, 3 subscribers) ===");
    println!();

    let ring = Arc::new(PubSubRing::create_anon(CAPACITY).expect("create"));

    let pos_a_path = std::env::temp_dir()
        .join(format!("pubsub_pos_a_{}.bin", std::process::id()));
    let pos_b_path = std::env::temp_dir()
        .join(format!("pubsub_pos_b_{}.bin", std::process::id()));
    let pos_c_path = std::env::temp_dir()
        .join(format!("pubsub_pos_c_{}.bin", std::process::id()));
    std::fs::remove_file(&pos_a_path).ok();
    std::fs::remove_file(&pos_b_path).ok();
    std::fs::remove_file(&pos_c_path).ok();

    let sub_a = Arc::new(PubSubSubscriber::new(
        ring.clone(),
        SubscriberPosition::create(&pos_a_path, 0).expect("pos a"),
    ));
    let sub_b = Arc::new(PubSubSubscriber::new(
        ring.clone(),
        SubscriberPosition::create(&pos_b_path, 0).expect("pos b"),
    ));
    let sub_c = Arc::new(PubSubSubscriber::new(
        ring.clone(),
        SubscriberPosition::create(&pos_c_path, 0).expect("pos c"),
    ));

    let producer_done = Arc::new(AtomicBool::new(false));
    let sum_a = Arc::new(AtomicU64::new(0));
    let sum_b = Arc::new(AtomicU64::new(0));
    let sum_c = Arc::new(AtomicU64::new(0));
    let count_a = Arc::new(AtomicU64::new(0));
    let count_b = Arc::new(AtomicU64::new(0));
    let count_c = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // Subscriber A: drain every item.
    let sub_a_handle = spawn_full_drain(
        "A", sub_a.clone(), sum_a.clone(), count_a.clone(),
        producer_done.clone(), N_ITEMS,
    );

    // Subscriber B: drain every item (independent of A).
    let sub_b_handle = spawn_full_drain(
        "B", sub_b.clone(), sum_b.clone(), count_b.clone(),
        producer_done.clone(), N_ITEMS,
    );

    // Subscriber C: even-position-only drain. Uses skip() to step
    // past odd positions.
    let sub_c_handle = {
        let sub = sub_c.clone();
        let sum = sum_c.clone();
        let count = count_c.clone();
        let done = producer_done.clone();
        let ring_for_c = ring.clone();
        thread::spawn(move || {
            let mut buf = [0u8; PUBSUB_PAYLOAD_BYTES];
            while count.load(Ordering::Acquire) < N_ITEMS / 2 {
                let pos = sub.position();
                if pos % 2 == 1 {
                    sub.skip(1);
                    continue;
                }
                match sub.try_next(&mut buf) {
                    Ok(()) => {
                        let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                        sum.fetch_add(v, Ordering::Relaxed);
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(PubSubReadError::Pending) => {
                        if done.load(Ordering::Acquire)
                            && sub.position() >= ring_for_c.head() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    Err(PubSubReadError::Lost) => panic!("C should not lose"),
                }
            }
        })
    };

    // Producer publishes N_ITEMS items.
    for i in 0..N_ITEMS {
        let mut payload = [0u8; PUBSUB_PAYLOAD_BYTES];
        payload[..8].copy_from_slice(&i.to_le_bytes());
        ring.publish(&payload);
    }
    producer_done.store(true, Ordering::Release);

    sub_a_handle.join().expect("A");
    sub_b_handle.join().expect("B");
    sub_c_handle.join().expect("C");
    let elapsed = start.elapsed();

    let consumed_a = count_a.load(Ordering::Acquire);
    let consumed_b = count_b.load(Ordering::Acquire);
    let consumed_c = count_c.load(Ordering::Acquire);
    let sum_a_v = sum_a.load(Ordering::Acquire);
    let sum_b_v = sum_b.load(Ordering::Acquire);
    let sum_c_v = sum_c.load(Ordering::Acquire);

    let expected_full_sum: u64 = (0..N_ITEMS).sum();
    let expected_even_sum: u64 = (0..N_ITEMS).filter(|i| i % 2 == 0).sum();

    println!("=== Result ===");
    println!("  elapsed:              {elapsed:?}");
    println!("  producer items:       {N_ITEMS}");
    println!("  sub A consumed:       {consumed_a}  sum: {sum_a_v}  expected: {expected_full_sum}");
    println!("  sub B consumed:       {consumed_b}  sum: {sum_b_v}  expected: {expected_full_sum}");
    println!("  sub C consumed:       {consumed_c}  sum: {sum_c_v}  expected: {expected_even_sum}");

    assert_eq!(consumed_a, N_ITEMS, "A must consume all items");
    assert_eq!(consumed_b, N_ITEMS, "B must consume all items");
    assert_eq!(consumed_c, N_ITEMS / 2, "C must consume half (even positions)");
    assert_eq!(sum_a_v, expected_full_sum, "A sum mismatch");
    assert_eq!(sum_b_v, expected_full_sum, "B sum mismatch");
    assert_eq!(sum_c_v, expected_even_sum, "C sum mismatch");
    println!("  integrity:            PASS");
    println!("    one publisher fanned out to three subscribers");
    println!("    each subscriber tracked its own MMF-resident position");
    println!("    sub C exercised skip() to consume only even-indexed positions");

    drop(sub_a);
    drop(sub_b);
    drop(sub_c);
    std::fs::remove_file(&pos_a_path).ok();
    std::fs::remove_file(&pos_b_path).ok();
    std::fs::remove_file(&pos_c_path).ok();
}

fn spawn_full_drain(
    name: &'static str,
    sub: Arc<PubSubSubscriber>,
    sum: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    target: u64,
) -> thread::JoinHandle<()> {
    let ring = sub.ring().clone();
    thread::spawn(move || {
        let mut buf = [0u8; PUBSUB_PAYLOAD_BYTES];
        while count.load(Ordering::Acquire) < target {
            match sub.try_next(&mut buf) {
                Ok(()) => {
                    let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    sum.fetch_add(v, Ordering::Relaxed);
                    count.fetch_add(1, Ordering::Relaxed);
                }
                Err(PubSubReadError::Pending) => {
                    if done.load(Ordering::Acquire)
                        && sub.position() >= ring.head() {
                        break;
                    }
                    std::hint::spin_loop();
                }
                Err(PubSubReadError::Lost) => {
                    panic!("subscriber {name} should not lose under this capacity");
                }
            }
        }
    })
}
