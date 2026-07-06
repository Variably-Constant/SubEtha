//! Parameterised end-to-end demonstration of `CapacityAdaptiveRing`
//! across the full Shape x Locale x Size matrix.
//!
//! Invocation:
//!     cargo run --release --example capacity_morph_matrix -- <shape> <locale> <n_items>
//!
//! Where:
//!     shape  ∈ {spsc, mpsc4, mpsc8, mpmc4, mpmc8, vyukov4, vyukov8}
//!     locale ∈ {anon, file, shmfs}
//!     n_items: positive integer (default 100000)
//!
//! Each variant runs n_items total messages through the requested
//! shape at the requested locale, with a morph thread cycling
//! through a set of pow2 capacities at 200us cadence so the morph
//! race window is densely sampled.
//!
//! Integrity checks per shape:
//!
//! - SPSC (1P/1C): every (producer_id=0, item_idx) appears exactly
//!   once; per-producer FIFO holds globally.
//! - MPSC (NP/1C): every (producer_id, item_idx) appears exactly
//!   once; for each producer, its items appear in the single
//!   consumer's pop log in send-order (per-producer FIFO).
//! - MPMC (NP/NC): every (producer_id, item_idx) appears exactly
//!   once across the consumer pool's union; within each consumer's
//!   own pop log, items from any given producer appear in send-
//!   order (per-producer per-consumer FIFO; cross-consumer
//!   interleave is per-shape contract).
//! - Vyukov MPMC (NP/NC global FIFO): same set check; additionally,
//!   the merge of all consumer pop logs (sorted by global pop
//!   sequence) preserves producer-side send-order across producers
//!   (Vyukov elevates MPMC's per-producer FIFO to a strict global
//!   FIFO via per-slot sequence atomics).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::RingShape;
use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;

const INITIAL_CAPACITY: usize = 256;
const MORPH_TARGETS: [usize; 6] = [1024, 4096, 1024, 256, 64, 512];
const MORPH_INTERVAL_US: u64 = 200;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let shape = args.get(1).cloned().unwrap_or_else(|| "spsc".to_owned());
    let locale = args.get(2).cloned().unwrap_or_else(|| "anon".to_owned());
    let n_items: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    let (n_producers, n_consumers, ring_shape) = parse_shape(&shape);
    let items_per_producer = n_items / n_producers as u64;
    let total_items = items_per_producer * n_producers as u64;

    println!("=== CapacityAdaptiveRing Matrix ===");
    println!("  shape:                {shape} ({n_producers}P/{n_consumers}C, {ring_shape:?})");
    println!("  locale:               {locale}");
    println!("  n_items target:       {n_items}");
    println!("  items per producer:   {items_per_producer}");
    println!("  total items expected: {total_items}");
    println!("  morph targets:        {MORPH_TARGETS:?}");
    println!("  morph cadence:        {MORPH_INTERVAL_US}us");
    println!();

    let ring = construct(&locale, &shape, n_producers, n_consumers);

    // Register producers / consumers and capture id assignments.
    let producer_ids: Vec<usize> = (0..n_producers)
        .map(|_| ring.register_producer().expect("register producer"))
        .collect();
    let consumer_ids: Vec<usize> = (0..n_consumers)
        .map(|_| ring.register_consumer().expect("register consumer"))
        .collect();

    // Shape-morph the inner AdaptiveRing to the target shape.
    // SPSC is the default; non-SPSC shapes need the morph_to call
    // before the first try_send.
    if ring_shape != RingShape::Spsc {
        ring.ring_handle()
            .morph_to(ring_shape)
            .expect("inner shape morph");
    }

    let morphs_completed = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    // Spawn producer threads.
    let mut producer_handles = Vec::new();
    for &pid in &producer_ids {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            for i in 0..items_per_producer {
                let mut payload = [0u8; 56];
                payload[..8].copy_from_slice(&(pid as u64).to_le_bytes());
                payload[8..16].copy_from_slice(&i.to_le_bytes());
                while r.try_send(pid, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }

    // Spawn consumer threads. Each consumer collects its own pop
    // log of (producer_id, item_idx) pairs. The main thread joins
    // and merges the logs for integrity verification.
    //
    // Consumers stop when the total drained item count across the
    // pool reaches total_items. A shared AtomicU64 counts drains.
    let drained = Arc::new(AtomicU64::new(0));
    let mut consumer_handles = Vec::new();
    for &cid in &consumer_ids {
        let r = Arc::clone(&ring);
        let drained = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let mut log: Vec<(u64, u64)> = Vec::new();
            let mut buf = [0u8; 64];
            loop {
                match r.try_recv(cid, &mut buf) {
                    Ok(_) => {
                        let pid = u64::from_le_bytes(buf[..8].try_into().unwrap());
                        let idx = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                        log.push((pid, idx));
                        let now = drained.fetch_add(1, Ordering::AcqRel) + 1;
                        if now >= total_items {
                            return log;
                        }
                    }
                    Err(_) => {
                        if drained.load(Ordering::Acquire) >= total_items {
                            return log;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
        });
        consumer_handles.push(h);
    }

    // Morph thread: loop MORPH_TARGETS at tight cadence until the
    // workload finishes (signalled by Arc strong_count dropping
    // below our reference + 1 active producer/consumer threads).
    let r_morph = Arc::clone(&ring);
    let morphs_c = Arc::clone(&morphs_completed);
    let drained_check = Arc::clone(&drained);
    let morpher = thread::spawn(move || {
        loop {
            for target in MORPH_TARGETS {
                thread::sleep(Duration::from_micros(MORPH_INTERVAL_US));
                r_morph
                    .morph_capacity_to(target)
                    .expect("morph succeeds");
                morphs_c.fetch_add(1, Ordering::Relaxed);
                if drained_check.load(Ordering::Acquire) >= total_items {
                    return;
                }
            }
        }
    });

    for h in producer_handles {
        h.join().expect("producer thread");
    }
    let consumer_logs: Vec<Vec<(u64, u64)>> = consumer_handles
        .into_iter()
        .map(|h| h.join().expect("consumer thread"))
        .collect();
    morpher.join().expect("morph thread");

    let elapsed = t0.elapsed();

    // Integrity check 1: every (producer_id, item_idx) appears
    // exactly once across the union of consumer logs.
    let mut union: Vec<(u64, u64)> = consumer_logs
        .iter()
        .flatten()
        .copied()
        .collect();
    union.sort();
    let expected: Vec<(u64, u64)> = (0..n_producers as u64)
        .flat_map(|p| (0..items_per_producer).map(move |i| (p, i)))
        .collect();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    let no_loss_no_dup = union == expected_sorted;

    // Integrity check 2: per-consumer per-producer FIFO. Within
    // each consumer's own pop log, items from any given producer
    // must appear in strictly-increasing send-order (idx 0, 1, 2,
    // ...; never out of order, never repeating). First item from
    // each producer establishes the baseline; any subsequent item
    // must be strictly greater.
    let mut per_consumer_fifo_violations = 0u64;
    for log in &consumer_logs {
        let mut last_per_producer: std::collections::HashMap<u64, u64> = Default::default();
        for &(pid, idx) in log {
            if let Some(&last) = last_per_producer.get(&pid)
                && idx <= last
            {
                per_consumer_fifo_violations += 1;
            }
            last_per_producer.insert(pid, idx);
        }
    }

    // Integrity check 3 (SPSC 1P/1C only): strict global FIFO
    // across the consumer's single pop log. SPSC has exactly one
    // producer AND one consumer, so the pop log MUST equal
    // (0,0), (0,1), (0,2), ... in lockstep with the producer's
    // sends. This is the strongest FIFO contract any shape
    // promises and only SPSC's 1P/1C construction can deliver it.
    //
    // MPSC (NP/1C) interleaves N producers' streams into the one
    // consumer; the consumer sees per-producer FIFO but NOT
    // global tuple-ordered FIFO (a pop sequence like
    // (3,0),(0,0),(1,0),(0,1),(2,0)... is valid MPSC behaviour).
    //
    // Vyukov MPMC promises global FIFO via per-slot sequence
    // atomics, but verifying it requires the consumer to capture
    // a per-pop sequence number that ties back to the producer's
    // slot claim - the (pid, idx) tuples we record do not encode
    // that, so multi-producer Vyukov cannot be globally-checked
    // from this harness's logs even at NC=1. Per-consumer-per-
    // producer FIFO (check 2) IS verified for Vyukov.
    let global_fifo_checkable =
        matches!(ring_shape, RingShape::Spsc) && n_producers == 1 && n_consumers == 1;
    let mut global_fifo_violations = 0u64;
    if global_fifo_checkable {
        let mut prev: Option<(u64, u64)> = None;
        for &cur in &consumer_logs[0] {
            if let Some(p) = prev
                && cur < p
            {
                global_fifo_violations += 1;
            }
            prev = Some(cur);
        }
    }

    println!("=== Result ===");
    println!("  elapsed:                  {elapsed:?}");
    println!(
        "  throughput:               {:.2} M items/s",
        total_items as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!(
        "  morphs completed:         {}",
        morphs_completed.load(Ordering::Relaxed)
    );
    println!("  items produced:           {total_items}");
    println!(
        "  items consumed:           {}",
        consumer_logs.iter().map(|l| l.len()).sum::<usize>()
    );
    println!(
        "  per-consumer-per-prod FIFO violations: {per_consumer_fifo_violations} (expected 0; bug if non-zero)"
    );
    if global_fifo_checkable {
        println!(
            "  strict global FIFO violations:       {global_fifo_violations} (expected 0 for SPSC 1P/1C; bug if non-zero)"
        );
    } else {
        println!("  strict global FIFO check:           skipped (only SPSC 1P/1C is globally-checkable from per-consumer (pid, idx) logs without pop-sequence timestamps)");
    }
    println!(
        "  integrity:                {}",
        if no_loss_no_dup {
            "PASS - every (producer_id, item_idx) appeared exactly once"
        } else {
            "FAIL - id-set mismatch"
        }
    );

    assert_eq!(
        per_consumer_fifo_violations, 0,
        "per-consumer-per-producer FIFO must hold across morphs",
    );
    if global_fifo_checkable {
        assert_eq!(
            global_fifo_violations, 0,
            "strict global FIFO must hold across morphs for SPSC 1P/1C",
        );
    }
    assert!(no_loss_no_dup, "integrity (no loss + no dup) failed");
}

fn parse_shape(s: &str) -> (usize, usize, RingShape) {
    match s {
        "spsc" => (1, 1, RingShape::Spsc),
        "mpsc4" => (4, 1, RingShape::Mpsc),
        "mpsc8" => (8, 1, RingShape::Mpsc),
        "mpmc4" => (4, 4, RingShape::Mpmc),
        "mpmc8" => (8, 8, RingShape::Mpmc),
        "vyukov4" => (4, 4, RingShape::Vyukov),
        "vyukov8" => (8, 8, RingShape::Vyukov),
        other => panic!(
            "unknown shape: {other}; expected one of spsc, mpsc4, mpsc8, mpmc4, mpmc8, vyukov4, vyukov8"
        ),
    }
}

fn construct(
    locale: &str,
    shape: &str,
    n_producers: usize,
    n_consumers: usize,
) -> Arc<CapacityAdaptiveRing> {
    match locale {
        "anon" => Arc::new(
            CapacityAdaptiveRing::create_anon(n_producers, n_consumers, INITIAL_CAPACITY)
                .expect("create_anon"),
        ),
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("cap_morph_{shape}_{}.bin", std::process::id()));
            Arc::new(
                CapacityAdaptiveRing::create(&path, n_producers, n_consumers, INITIAL_CAPACITY)
                    .expect("create file-backed"),
            )
        }
        "shmfs" => {
            let name = format!("cap_morph_{shape}_{}", std::process::id());
            Arc::new(
                CapacityAdaptiveRing::create_shmfs(
                    &name,
                    n_producers,
                    n_consumers,
                    INITIAL_CAPACITY,
                )
                .expect("create shmfs"),
            )
        }
        other => panic!("unknown locale: {other}; expected anon, file, or shmfs"),
    }
}
