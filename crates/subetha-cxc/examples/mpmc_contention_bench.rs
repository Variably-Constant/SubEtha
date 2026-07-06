//! Vyukov MPMC contention throughput - the A/B vehicle for the
//! `producer_seq` / `consumer_seq` false-sharing fix.
//!
//! The slot-permutation investigation found that MPMC throughput
//! collapses with contention because every producer CASes the shared
//! `producer_seq` and every consumer CASes `consumer_seq`, and those
//! two counters share one `RingHeader` cache line - so each producer
//! CAS and each consumer CAS invalidate the other side's copy of the
//! line (false sharing). The SPSC ring already separates its `head`
//! and `tail` onto distinct lines; the Vyukov header did not.
//!
//! This bench measures throughput at contention levels
//! (n_producers = n_consumers in {1,4,8}) so the same binary, run
//! before and after the header fix, quantifies the gain. Interleaved
//! rounds remove the thermal-drift-vs-order confound; pinned threads
//! remove scheduler variance; integrity is exact count + payload sum.
//!
//! `--single <cap> <level> <total>` runs one configuration so
//! `perf stat` can attribute cache events to it.
//!
//! Run:
//!     cargo run --release --example mpmc_contention_bench -p subetha-cxc

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use subetha_cxc::cpu_affinity::pin_current_thread_to_core;
use subetha_cxc::shared_ring::{SharedRing, PAYLOAD_BYTES};

const CAPACITY: usize = 1024;
const LEVELS: [usize; 3] = [1, 4, 8]; // n_producers = n_consumers
const TOTAL: u64 = 6_000_000;
const REPS: usize = 7;

struct LevelStat {
    level: usize,
    median: f64,
    p25: f64,
    p75: f64,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

/// One MPMC measurement: `level` producers + `level` consumers, pinned,
/// released together by a barrier; returns (M items/s, integrity-ok).
fn measure_mpmc(cap: usize, level: usize, total: u64) -> (f64, bool) {
    let ring = Arc::new(SharedRing::create_anon(cap).unwrap());
    let per_prod = total / level as u64;
    let actual_total = per_prod * level as u64;

    let barrier = Arc::new(Barrier::new(level * 2 + 1));
    let consumed = Arc::new(AtomicUsize::new(0));
    let sum = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for k in 0..level {
        let r = Arc::clone(&ring);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            pin_current_thread_to_core(k);
            let mut buf = [0u8; PAYLOAD_BYTES];
            b.wait();
            for i in 0..per_prod {
                let v = ((k as u64) << 32) | i;
                buf[..8].copy_from_slice(&v.to_le_bytes());
                while r.try_push(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    for k in 0..level {
        let r = Arc::clone(&ring);
        let b = Arc::clone(&barrier);
        let consumed = Arc::clone(&consumed);
        let sum = Arc::clone(&sum);
        handles.push(thread::spawn(move || {
            pin_current_thread_to_core(level + k);
            let mut out = [0u8; PAYLOAD_BYTES];
            let mut local_sum = 0u64;
            b.wait();
            loop {
                if consumed.load(Ordering::Acquire) >= actual_total as usize {
                    break;
                }
                if r.try_pop(&mut out).is_ok() {
                    let v = u64::from_le_bytes(out[..8].try_into().unwrap());
                    local_sum = local_sum.wrapping_add(v);
                    consumed.fetch_add(1, Ordering::AcqRel);
                }
            }
            sum.fetch_add(local_sum, Ordering::AcqRel);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed().as_secs_f64();

    let mut expected_sum = 0u64;
    for k in 0..level as u64 {
        for i in 0..per_prod {
            expected_sum = expected_sum.wrapping_add((k << 32) | i);
        }
    }
    let ok = consumed.load(Ordering::Acquire) == actual_total as usize
        && sum.load(Ordering::Acquire) == expected_sum;
    (actual_total as f64 / elapsed / 1e6, ok)
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--single") {
        let cap: usize = argv[2].parse().unwrap();
        let level: usize = argv[3].parse().unwrap();
        let total: u64 = argv[4].parse().unwrap();
        let (t, ok) = measure_mpmc(cap, level, total);
        println!("single cap={cap} level={level} total={total} ok={ok} {t:.2} M items/s");
        return;
    }

    println!("Vyukov MPMC contention throughput (capacity {CAPACITY}, {REPS} interleaved rounds,");
    println!("{TOTAL} items/measurement, pinned). level = n_producers = n_consumers.\n");

    let mut data: Vec<Vec<f64>> = vec![Vec::new(); LEVELS.len()];
    let mut all_ok = true;
    for round in 0..=REPS {
        for (li, &level) in LEVELS.iter().enumerate() {
            let (t, ok) = measure_mpmc(CAPACITY, level, TOTAL);
            all_ok &= ok;
            if round > 0 {
                data[li].push(t);
            }
        }
    }

    println!("integrity (exact count + payload sum) held everywhere: {all_ok}\n");
    let mut stats = Vec::new();
    for (li, &level) in LEVELS.iter().enumerate() {
        let mut v = data[li].clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        stats.push(LevelStat {
            level,
            median: pct(&v, 0.5),
            p25: pct(&v, 0.25),
            p75: pct(&v, 0.75),
        });
    }
    for s in &stats {
        println!("  {:>2}P/{:>2}C:  {:>7.2} M/s  IQR[{:>6.2}..{:>6.2}]",
                 s.level, s.level, s.median, s.p25, s.p75);
    }
}
