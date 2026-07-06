//! E2E: the QoS ordering DECLARATION path on an UNSTAMPED ring.
//!
//! Two producer threads and one consumer thread stream items
//! through an `AdaptiveRing` NON-STOP while a QoS-aware sidecar
//! (`spawn_with_qos` with `QosRingShapePolicy`) watches the
//! `QosPolicy`. Mid-traffic the driver declares
//! `Ordering::GlobalFifo`; the sidecar responds by morphing the
//! ring to the Vyukov shape - the proven global-FIFO structure -
//! because this ring carries no ordering stamps (the stamped merge
//! path is exercised by `ordering_xproc_*` and
//! `ordering_modes_compare`).
//!
//! The morph itself moves no data: the old composed backing becomes
//! the stale backing and the consumer's pop path drains it first,
//! so neither producers nor the consumer pause for the transition
//! and the backlog cannot overflow the new shape.
//!
//! Assertions:
//! - the sidecar (not the driver) performs the morph, triggered
//!   purely by the declaration;
//! - zero items lost across the whole run, with all three traffic
//!   threads live through the flip;
//! - per-producer FIFO holds for every consumed item, before and
//!   after the morph (Vyukov preserves it within its global
//!   ordering).
//!
//! Run with:
//!     cargo run --release --example qos_ordering_morph

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomOrd};
use std::time::{Duration, Instant};

use subetha_cxc::qos_policy::{Ordering, QosPolicy};
use subetha_cxc::{
    AdaptiveRing, AdaptiveRingSidecar, DefaultOrderingPolicy,
    QosRingShapePolicy, RingShape,
};

const N_PER_PRODUCER: u64 = 200_000;
const CAPACITY: usize = 1024;

fn main() {
    println!("=== QoS ordering declaration -> sidecar Vyukov morph E2E ===");
    println!();

    let ring = Arc::new(AdaptiveRing::create_anon(2, 1, CAPACITY).expect("create"));
    let p0 = ring.register_producer().expect("p0");
    let p1 = ring.register_producer().expect("p1");
    let _c0 = ring.register_consumer().expect("c0");
    assert!(!ring.is_stamped(), "this E2E exercises the UNSTAMPED declaration path");

    // Counts say 2P/1C, so the policy's counts-based default is
    // MPSC; let it settle there before traffic starts.
    let qos = Arc::new(QosPolicy::streaming_default());
    let sidecar = AdaptiveRingSidecar::spawn_with_qos(
        ring.clone(),
        QosRingShapePolicy {
            qos: qos.clone(),
            hysteresis: Duration::from_millis(10),
        },
        DefaultOrderingPolicy::default(),
        qos.clone(),
        Duration::from_millis(5),
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while ring.current_shape() != RingShape::Mpsc && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(ring.current_shape(), RingShape::Mpsc,
               "sidecar must settle on the counts-based MPSC shape first");
    println!("[setup] sidecar settled on {:?} from peer counts (2P/1C)",
             ring.current_shape());

    // ----- traffic: 2 producers + 1 consumer, all non-stop -----
    // Payload: [producer_id; 8][seq; 8]. Producers follow the shape
    // tag per push; the consumer's pop path walks the stale backing
    // first, so the morph is invisible to all three threads.
    let produced = Arc::new(AtomicU64::new(0));
    let mut producer_handles = Vec::new();
    for producer_id in [p0, p1] {
        let ring_p = ring.clone();
        let produced_p = produced.clone();
        producer_handles.push(std::thread::spawn(move || {
            let mut payload = [0u8; 16];
            payload[..8].copy_from_slice(&(producer_id as u64).to_le_bytes());
            for seq in 0..N_PER_PRODUCER {
                payload[8..].copy_from_slice(&seq.to_le_bytes());
                while ring_p.try_send(producer_id, &payload).is_err() {
                    std::hint::spin_loop();
                }
                produced_p.fetch_add(1, AtomOrd::Relaxed);
            }
        }));
    }

    let ring_c = ring.clone();
    let consumer = std::thread::spawn(move || {
        let total = 2 * N_PER_PRODUCER;
        let mut consumed = 0u64;
        let mut last_seq = [-1i64; 2];
        let mut post_morph_items = 0u64;
        let mut out = [0u8; 64];
        while consumed < total {
            match ring_c.try_recv(0, &mut out) {
                Ok(_) => {
                    let producer =
                        u64::from_le_bytes(out[..8].try_into().unwrap()) as usize;
                    let seq =
                        u64::from_le_bytes(out[8..16].try_into().unwrap()) as i64;
                    consumed += 1;
                    assert!(seq > last_seq[producer],
                            "per-producer FIFO violated: producer {producer} seq {seq} after {}",
                            last_seq[producer]);
                    last_seq[producer] = seq;
                    if ring_c.current_shape() == RingShape::Vyukov {
                        post_morph_items += 1;
                    }
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
        (consumed, post_morph_items)
    });

    // ----- the runtime declaration, mid-traffic -----
    while produced.load(AtomOrd::Acquire) < N_PER_PRODUCER / 2 {
        std::hint::spin_loop();
    }
    let morphs_before = sidecar.morphs_triggered();
    println!("[flip] {} items produced; declaring Ordering::GlobalFifo",
             produced.load(AtomOrd::Acquire));
    qos.set_ordering(Ordering::GlobalFifo);

    let deadline = Instant::now() + Duration::from_secs(10);
    while ring.current_shape() != RingShape::Vyukov && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(ring.current_shape(), RingShape::Vyukov,
               "sidecar must morph to Vyukov on the GlobalFifo declaration");
    assert!(sidecar.morphs_triggered() > morphs_before,
            "the morph must come from the sidecar, not the driver");
    println!("[morph] sidecar morphed to {:?} with all traffic threads live \
              (sidecar morphs: {})",
             ring.current_shape(), sidecar.morphs_triggered());

    // ----- join + integrity -----
    for handle in producer_handles {
        handle.join().expect("producer thread");
    }
    let (consumed, post_morph_items) = consumer.join().expect("consumer thread");
    sidecar.shutdown();

    let total_produced = 2 * N_PER_PRODUCER;
    println!();
    println!("=== Result ===");
    println!("  produced:           {total_produced}");
    println!("  consumed:           {consumed}");
    println!("  post-morph items:   {post_morph_items}");

    assert_eq!(consumed, total_produced,
               "INTEGRITY FAIL: items lost across the QoS-driven morph");
    assert!(post_morph_items > 0,
            "the flip must land mid-traffic (no post-morph items seen)");
    assert!(ring.is_empty(), "nothing may remain in any backing");
    println!("  integrity:          PASS");
    println!("    declaration -> sidecar morph -> zero loss -> per-producer FIFO held");
}
