//! Parameterised end-to-end demonstration of
//! `CapacityBroadcastRing` across the Subscribers x Locale x Size
//! matrix.
//!
//! Invocation:
//!     cargo run --release --example capacity_broadcast_morph_matrix -- <subs> <locale> <n_items>
//!
//! Where:
//!     subs   ∈ {2, 4, 8}        (number of fan-out subscribers)
//!     locale ∈ {anon, file, shmfs}
//!     n_items: positive integer (default 100000)
//!
//! Workload: 1 producer publishes n_items messages tagged with
//! their absolute index 0..n_items. All N subscribers drain
//! independently and concurrently while a morph thread cycles
//! capacity through MORPH_TARGETS at 200us cadence. Each
//! subscriber must observe every published item in strict
//! send-order (broadcast preserves per-subscriber FIFO, and with
//! a single producer this is also strict global FIFO per
//! subscriber).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::capacity_broadcast_ring::CapacityBroadcastRing;

const INITIAL_CAPACITY: usize = 256;
const MORPH_TARGETS: [usize; 6] = [1024, 4096, 1024, 256, 64, 512];
const MORPH_INTERVAL_US: u64 = 200;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let subs: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let locale = args.get(2).cloned().unwrap_or_else(|| "anon".to_owned());
    let n_items: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    println!("=== CapacityBroadcastRing Matrix ===");
    println!("  subscribers:          {subs}");
    println!("  locale:               {locale}");
    println!("  n_items:              {n_items}");
    println!("  morph targets:        {MORPH_TARGETS:?}");
    println!("  morph cadence:        {MORPH_INTERVAL_US}us");
    println!();

    let ring = construct(&locale);

    let consumer_ids: Vec<usize> = (0..subs)
        .map(|_| ring.register_consumer().expect("register consumer"))
        .collect();

    let drained_per_sub = Arc::new(AtomicU64::new(0));
    let morphs_completed = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    // Single producer thread publishes n_items items tagged with
    // their index. Spins on Full until a consumer drains; this is
    // the back-pressure shape broadcast offers (slowest consumer
    // bounds producer rate when at-capacity).
    let r_prod = Arc::clone(&ring);
    let producer = thread::spawn(move || {
        for i in 0..n_items {
            let mut payload = [0u8; 52];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            while r_prod.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    // Subscriber threads each pull n_items items into their own
    // log. Each subscriber sees the full stream independently.
    let mut consumer_handles = Vec::new();
    for &cid in &consumer_ids {
        let r = Arc::clone(&ring);
        let drained_total = Arc::clone(&drained_per_sub);
        let h = thread::spawn(move || {
            let mut log: Vec<u64> = Vec::with_capacity(n_items as usize);
            let mut buf = [0u8; 64];
            while (log.len() as u64) < n_items {
                if r.try_recv(cid, &mut buf).is_ok() {
                    let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    log.push(v);
                } else {
                    std::hint::spin_loop();
                }
            }
            drained_total.fetch_add(log.len() as u64, Ordering::AcqRel);
            log
        });
        consumer_handles.push(h);
    }

    // Morph thread: cycle MORPH_TARGETS at 200us until producer
    // finishes AND every subscriber has fully drained.
    let r_morph = Arc::clone(&ring);
    let morphs_c = Arc::clone(&morphs_completed);
    let drained_check = Arc::clone(&drained_per_sub);
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
    let sub_logs: Vec<Vec<u64>> = consumer_handles
        .into_iter()
        .map(|h| h.join().expect("subscriber thread"))
        .collect();
    morpher.join().expect("morph thread");

    let elapsed = t0.elapsed();

    // Integrity check 1: every subscriber received exactly n_items.
    let lengths_ok = sub_logs.iter().all(|log| log.len() as u64 == n_items);

    // Integrity check 2: every subscriber's log == 0..n_items
    // (no loss, no dup, in send-order).
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
        "  total items produced:     {}",
        n_items
    );
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

fn construct(locale: &str) -> Arc<CapacityBroadcastRing> {
    match locale {
        "anon" => Arc::new(
            CapacityBroadcastRing::create_anon(INITIAL_CAPACITY)
                .expect("create_anon"),
        ),
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("cap_broadcast_{}.bin", std::process::id()));
            Arc::new(
                CapacityBroadcastRing::create(&path, INITIAL_CAPACITY)
                    .expect("create file-backed"),
            )
        }
        "shmfs" => {
            let name = format!("cap_broadcast_{}", std::process::id());
            Arc::new(
                CapacityBroadcastRing::create_shmfs(&name, INITIAL_CAPACITY)
                    .expect("create shmfs"),
            )
        }
        other => panic!("unknown locale: {other}; expected anon, file, or shmfs"),
    }
}
