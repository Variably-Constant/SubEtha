//! Parameterised end-to-end demonstration of
//! `CapacityPubSubRing` across the Subscribers x Locale x Size
//! matrix.
//!
//! Invocation:
//!     cargo run --release --example capacity_pubsub_morph_matrix -- <subs> <locale> <n_items>
//!
//! Subscribers all start with `subscribe_from_oldest` so each
//! one drains every published item across the chain of backings
//! the morph thread accumulates. Each subscriber's pop log must
//! equal 0..n_items in strict send-order (pubsub preserves
//! per-subscriber FIFO; with a single producer this is also
//! strict global FIFO per subscriber).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::capacity_pubsub_ring::CapacityPubSubRing;
use subetha_cxc::protocol_pubsub::PubSubReadError;

const INITIAL_CAPACITY: usize = 256;
const MORPH_TARGETS: [usize; 6] = [1024, 4096, 1024, 256, 64, 512];
const MORPH_INTERVAL_US: u64 = 200;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let subs: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let locale = args.get(2).cloned().unwrap_or_else(|| "anon".to_owned());
    let n_items: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    println!("=== CapacityPubSubRing Matrix ===");
    println!("  subscribers:          {subs}");
    println!("  locale:               {locale}");
    println!("  n_items:              {n_items}");
    println!("  morph targets:        {MORPH_TARGETS:?}");
    println!("  morph cadence:        {MORPH_INTERVAL_US}us");
    println!();

    let ring = construct(&locale);

    // Subscribe all subscribers FIRST so they see history from
    // the start (subscribe_from_oldest captures the chain head
    // at construction).
    let subscribers: Vec<_> = (0..subs)
        .map(|_| ring.subscribe_from_oldest())
        .collect();

    let drained_total = Arc::new(AtomicU64::new(0));
    let morphs_completed = Arc::new(AtomicU64::new(0));
    let producer_published = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();

    // Single producer publishes n_items items, each tagged with
    // its absolute index. Per-backing back-pressure: never let
    // the active backing's head reach cap, because the next
    // publish would wrap and overwrite slot 0 - which late-
    // arriving subscribers will need when they cross into this
    // backing. The morph thread eventually rolls to a fresh
    // backing, freeing the producer to continue there.
    let r_prod = Arc::clone(&ring);
    let pub_count = Arc::clone(&producer_published);
    let producer = thread::spawn(move || {
        for i in 0..n_items {
            loop {
                let active = r_prod.ring_handle();
                let head = active.head();
                let cap = active.capacity() as u64;
                if head < cap {
                    break;
                }
                std::hint::spin_loop();
            }
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            r_prod.publish(&payload);
            pub_count.store(i + 1, Ordering::Release);
        }
    });

    // Subscriber threads each drain forward via try_next, looping
    // until they've received exactly n_items items. Each
    // subscriber bumps its own consumed-count atomic so the
    // producer's KeepAll back-pressure loop can see it.
    let mut subscriber_handles = Vec::new();
    for (sub_i, mut sub) in subscribers.into_iter().enumerate() {
        let drained = Arc::clone(&drained_total);
        let h = thread::spawn(move || {
            let mut log: Vec<u64> = Vec::with_capacity(n_items as usize);
            let mut buf = [0u8; 64];
            while (log.len() as u64) < n_items {
                match sub.try_next(&mut buf) {
                    Ok(()) => {
                        let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                        log.push(v);
                    }
                    Err(PubSubReadError::Pending) => {
                        std::hint::spin_loop();
                    }
                    Err(PubSubReadError::Lost) => {
                        println!(
                            "SUB {sub_i} LOST at log_len={} backing_idx={} position={}",
                            log.len(),
                            sub.backing_idx(),
                            sub.position()
                        );
                        break;
                    }
                }
            }
            drained.fetch_add(log.len() as u64, Ordering::AcqRel);
            log
        });
        subscriber_handles.push(h);
    }

    // Morph thread: cycle MORPH_TARGETS at 200us cadence.
    // After every full MORPH_TARGETS cycle, invoke gc() so the
    // chain does not grow unboundedly as subscribers advance
    // through stale backings. Each subscriber's try_next drops
    // its temporary Arc when the call returns, so gc's
    // strong_count == 1 check correctly reclaims backings that
    // no subscriber is currently inside. Worst-case: a
    // subscriber holding an Arc on its current backing prevents
    // gc of that backing - which is correct, gc skips it.
    let r_morph = Arc::clone(&ring);
    let morphs_c = Arc::clone(&morphs_completed);
    let drained_check = Arc::clone(&drained_total);
    let target_total_drained = n_items * subs as u64;
    let morpher = thread::spawn(move || {
        loop {
            for target in MORPH_TARGETS {
                thread::sleep(Duration::from_micros(MORPH_INTERVAL_US));
                r_morph
                    .morph_capacity_to(target)
                    .expect("morph succeeds");
                morphs_c.fetch_add(1, Ordering::Relaxed);
                if drained_check.load(Ordering::Acquire) >= target_total_drained {
                    return;
                }
            }
        }
    });

    producer.join().expect("producer thread");
    let sub_logs: Vec<Vec<u64>> = subscriber_handles
        .into_iter()
        .map(|h| h.join().expect("subscriber thread"))
        .collect();
    morpher.join().expect("morph thread");

    let elapsed = t0.elapsed();

    // Integrity checks.
    let lengths_ok = sub_logs.iter().all(|log| log.len() as u64 == n_items);

    let expected: Vec<u64> = (0..n_items).collect();
    let mut fifo_violations = 0u64;
    let mut content_violations = 0u64;
    for log in &sub_logs {
        if log != &expected {
            content_violations += 1;
        }
        let mut prev: Option<u64> = None;
        for &v in log {
            if let Some(p) = prev
                && v <= p
            {
                fifo_violations += 1;
            }
            prev = Some(v);
        }
    }

    println!("=== Result ===");
    println!("  elapsed:                  {elapsed:?}");
    println!(
        "  throughput (per-sub):     {:.2} M items/s",
        n_items as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!(
        "  morphs completed:         {}",
        morphs_completed.load(Ordering::Relaxed)
    );
    println!(
        "  chain length at end:      {}",
        ring.chain_len()
    );
    println!("  total items produced:     {n_items}");
    println!(
        "  total items consumed:     {}",
        sub_logs.iter().map(|l| l.len()).sum::<usize>()
    );
    println!(
        "  per-sub length check:     {} ({} subs got exactly {n_items})",
        if lengths_ok { "PASS" } else { "FAIL" },
        sub_logs.iter().filter(|l| l.len() as u64 == n_items).count()
    );
    println!(
        "  per-sub content check:    {} ({content_violations} subs have wrong contents)",
        if content_violations == 0 { "PASS" } else { "FAIL" }
    );
    println!(
        "  per-sub FIFO violations:  {fifo_violations} (expected 0; bug if non-zero)"
    );
    println!(
        "  integrity:                {}",
        if lengths_ok && content_violations == 0 && fifo_violations == 0 {
            "PASS - every subscriber observed the full 0..n_items stream in send-order"
        } else {
            "FAIL"
        }
    );

    assert!(lengths_ok, "every subscriber must receive exactly n_items items");
    assert_eq!(content_violations, 0, "every subscriber must receive the same 0..n_items stream");
    assert_eq!(fifo_violations, 0, "per-subscriber FIFO must hold across morphs");
}

fn construct(locale: &str) -> Arc<CapacityPubSubRing> {
    match locale {
        "anon" => CapacityPubSubRing::create_anon(INITIAL_CAPACITY)
            .expect("create_anon"),
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("cap_pubsub_{}.bin", std::process::id()));
            CapacityPubSubRing::create(&path, INITIAL_CAPACITY)
                .expect("create file-backed")
        }
        "shmfs" => {
            let name = format!("cap_pubsub_{}", std::process::id());
            CapacityPubSubRing::create_shmfs(&name, INITIAL_CAPACITY)
                .expect("create shmfs")
        }
        other => panic!("unknown locale: {other}; expected anon, file, or shmfs"),
    }
}
