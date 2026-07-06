//! E2E demonstration of the `QosPolicy` framework driving real
//! substrate adaptations.
//!
//! Shows a driver loop that:
//!  1. Holds a `LocaleAdaptiveRing` at the default Anon locale.
//!  2. Mutates a `QosPolicy` at runtime: streaming -> reliable
//!     pubsub -> persistent log.
//!  3. After each mutation, snapshots the QoS and consults
//!     `recommends_locale_change()` against the current locale.
//!  4. When a change is recommended, calls
//!     `ring.migrate_to(recommended)` to flip the substrate's
//!     locale to match the policy.
//!  5. Verifies that every QoS change actually produced the matching
//!     locale flip + a corresponding pin generation bump.
//!
//! This is the pattern a real sidecar uses to bridge declarative
//! QoS settings into imperative substrate morphs. The example does
//! it inline for E2E observability.
//!
//! Run with:
//!     cargo run --release --example qos_driven_morph

use std::sync::Arc;
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::qos_policy::{Durability, History, QosPolicy, Reliability};
use subetha_cxc::{LocaleAdaptiveRing, RingShape};

const ITEMS_PER_QOS_STAGE: u64 = 5_000;
const CAPACITY: usize = 256;

fn main() {
    println!("=== QoS-driven substrate morph E2E ===");
    println!("(QosPolicy snapshot -> recommended_locale -> migrate_to)");
    println!();

    let base_path = std::env::temp_dir()
        .join(format!("qos_morph_e2e_{}", std::process::id()));
    let ring = Arc::new(
        LocaleAdaptiveRing::create(&base_path, 1, 1, CAPACITY)
            .expect("locale ring create"),
    );
    ring.register_producer().expect("p");
    ring.register_consumer().expect("c");

    let qos = Arc::new(QosPolicy::streaming_default());

    let start = Instant::now();
    let mut produced_count = 0u64;
    let mut produced_sum = 0u64;
    let mut consumed_count = 0u64;
    let mut consumed_sum = 0u64;

    // ----- helper: run one QoS stage -----
    let stage = |label: &str,
                     d: Durability,
                     r: Reliability,
                     h: History,
                     lat: Duration,
                     produced_count: &mut u64,
                     produced_sum: &mut u64,
                     consumed_count: &mut u64,
                     consumed_sum: &mut u64| {
        println!();
        println!("[stage {label}] set QoS to durability={d:?} reliability={r:?} history={h:?} latency={lat:?}");
        qos.set_durability(d);
        qos.set_reliability(r);
        qos.set_history(h);
        qos.set_max_latency(lat);

        let snap = qos.snapshot();
        let current = ring.current_locale();
        match snap.recommends_locale_change(current) {
            Some(target) => {
                println!("    QoS snapshot recommends locale: {current:?} -> {target:?}");
                let gen_before = ring.locale_generation();
                ring.migrate_to(target).expect("migrate");
                println!("    locale_generation: {gen_before} -> {}, locale = {:?}",
                         ring.locale_generation(), ring.current_locale());
                assert_eq!(ring.current_locale(), target);
            }
            None => {
                println!("    QoS snapshot recommends NO locale change (current = recommended = {current:?})");
            }
        }

        // Round-trip items through the active locale + pin chain.
        let pin_locale = ring.pin_current_locale();
        let adaptive = match snap.durability {
            Durability::Volatile => pin_locale.as_anon().expect("anon"),
            Durability::Transient => pin_locale.as_shmfs().expect("shmfs"),
            Durability::Persistent => pin_locale.as_file().expect("file"),
        };
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for i in 0..ITEMS_PER_QOS_STAGE {
            let payload = i.to_le_bytes();
            while pin_shape.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            *produced_count += 1;
            *produced_sum += i;
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            *consumed_sum += u64::from_le_bytes(buf[..8].try_into().unwrap());
            *consumed_count += 1;
        }
        println!("    round-tripped {ITEMS_PER_QOS_STAGE} via four-axis pin chain at {:?}",
                 ring.current_locale());
    };

    // ----- run three QoS stages -----
    stage(
        "1 streaming",
        Durability::Volatile, Reliability::BestEffort,
        History::KeepLastN(1024), Duration::from_millis(100),
        &mut produced_count, &mut produced_sum,
        &mut consumed_count, &mut consumed_sum,
    );
    stage(
        "2 reliable_pubsub",
        Durability::Transient, Reliability::Reliable,
        History::KeepAll, Duration::from_secs(1),
        &mut produced_count, &mut produced_sum,
        &mut consumed_count, &mut consumed_sum,
    );
    stage(
        "3 persistent_log",
        Durability::Persistent, Reliability::Reliable,
        History::KeepAll, Duration::from_secs(5),
        &mut produced_count, &mut produced_sum,
        &mut consumed_count, &mut consumed_sum,
    );

    // ----- result -----
    let elapsed = start.elapsed();
    let final_gen = ring.locale_generation();
    println!();
    println!("=== Result ===");
    println!("  elapsed:                 {elapsed:?}");
    println!("  produced count:          {produced_count}");
    println!("  consumed count:          {consumed_count}");
    println!("  produced sum:            {produced_sum}");
    println!("  consumed sum:            {consumed_sum}");
    println!("  QoS-driven locale morphs: {final_gen}");
    println!("  final QoS:               {:?}", qos.snapshot());

    assert_eq!(produced_count, consumed_count,
               "INTEGRITY FAIL: count mismatch");
    assert_eq!(produced_sum, consumed_sum,
               "INTEGRITY FAIL: sum mismatch");
    assert_eq!(final_gen, 2,
               "expected exactly 2 locale morphs (Anon stays at Anon, then -> ShmFs, then -> File)");
    println!("  integrity:               PASS");
    println!("    every item arrived exactly once across 3 QoS-driven stages");
    println!("    QoS snapshot drove every locale morph automatically");
    println!("    four-axis pin chain composed at each new locale without ceremony");
}
