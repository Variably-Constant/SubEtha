//! Vyukov shared-counter MPMC vs the sharded (per-producer SPSC lanes)
//! MPMC, under contention - the "shard the counter" lever.
//!
//! The false-sharing fix took the Vyukov ring from ~5 to ~10 M/s at
//! 8P/8C, but that is still ~10x slower than 1P/1C because every
//! producer CASes the single shared `producer_seq` line. The sharded
//! design (`SharedRingMpmc`: N independent Lamport SPSC rings, M
//! consumers partitioning them) removes that counter entirely - each
//! producer is the sole writer of its own ring, no CAS at all. It
//! trades global cross-producer FIFO for per-producer FIFO.
//!
//! Fair comparison: SAME total buffer (Vyukov gets one ring of
//! `TOTAL_CAP`; the sharded grid gets `n_prod` shards of
//! `TOTAL_CAP / n_prod` each), SAME producer/consumer thread counts,
//! pinned, interleaved rounds, exact count + sum integrity.
//!
//! Run:
//!     cargo run --release --example mpmc_sharded_compare -p subetha-cxc

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use subetha_cxc::cpu_affinity::pin_current_thread_to_core;
use subetha_cxc::shared_ring::{SharedRing, PAYLOAD_BYTES};
use subetha_cxc::mpmc_ring::SharedRingMpmc;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

const CONFIGS: [(usize, usize); 4] = [(1, 1), (4, 4), (8, 8), (8, 2)];
const TOTAL_CAP: usize = 1024;
const TOTAL: u64 = 6_000_000;
const REPS: usize = 7;

fn pct(sorted: &[f64], p: f64) -> f64 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn expected_sum(np: usize, per_prod: u64) -> u64 {
    let mut s = 0u64;
    for k in 0..np as u64 {
        for i in 0..per_prod {
            s = s.wrapping_add((k << 32) | i);
        }
    }
    s
}

/// Vyukov shared-ring MPMC: np producers + nc consumers on ONE ring.
fn measure_vyukov(np: usize, nc: usize, total: u64) -> (f64, bool) {
    let ring = Arc::new(SharedRing::create_anon(TOTAL_CAP).unwrap());
    let per_prod = total / np as u64;
    let actual_total = per_prod * np as u64;
    let barrier = Arc::new(Barrier::new(np + nc + 1));
    let consumed = Arc::new(AtomicUsize::new(0));
    let sum = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for k in 0..np {
        let (r, b) = (Arc::clone(&ring), Arc::clone(&barrier));
        handles.push(thread::spawn(move || {
            pin_current_thread_to_core(k);
            let mut buf = [0u8; PAYLOAD_BYTES];
            b.wait();
            for i in 0..per_prod {
                buf[..8].copy_from_slice(&(((k as u64) << 32) | i).to_le_bytes());
                while r.try_push(&buf).is_err() { std::hint::spin_loop(); }
            }
        }));
    }
    for k in 0..nc {
        let (r, b) = (Arc::clone(&ring), Arc::clone(&barrier));
        let (consumed, sum) = (Arc::clone(&consumed), Arc::clone(&sum));
        handles.push(thread::spawn(move || {
            pin_current_thread_to_core(np + k);
            let mut out = [0u8; PAYLOAD_BYTES];
            let mut local = 0u64;
            b.wait();
            loop {
                if consumed.load(Ordering::Acquire) >= actual_total as usize { break; }
                if r.try_pop(&mut out).is_ok() {
                    local = local.wrapping_add(u64::from_le_bytes(out[..8].try_into().unwrap()));
                    consumed.fetch_add(1, Ordering::AcqRel);
                }
            }
            sum.fetch_add(local, Ordering::AcqRel);
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    for h in handles { h.join().unwrap(); }
    let elapsed = t0.elapsed().as_secs_f64();
    let ok = consumed.load(Ordering::Acquire) == actual_total as usize
        && sum.load(Ordering::Acquire) == expected_sum(np, per_prod);
    (actual_total as f64 / elapsed / 1e6, ok)
}

/// Sharded MPMC: np per-producer SPSC rings, nc consumers partitioning
/// them. Same total buffer as the Vyukov arm.
fn measure_sharded(np: usize, nc: usize, total: u64) -> (f64, bool) {
    let shard_cap = TOTAL_CAP / np;
    assert!(shard_cap.is_power_of_two() && shard_cap >= 2,
            "TOTAL_CAP must split into pow2 shards for np={np}");
    let (producers, consumers) =
        SharedRingMpmc::create_anon_grid(np, nc, shard_cap).unwrap();
    let per_prod = total / np as u64;
    let actual_total = per_prod * np as u64;
    let barrier = Arc::new(Barrier::new(np + nc + 1));
    let consumed = Arc::new(AtomicUsize::new(0));
    let sum = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for (k, p) in producers.into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            pin_current_thread_to_core(k);
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            b.wait();
            for i in 0..per_prod {
                buf[..8].copy_from_slice(&(((k as u64) << 32) | i).to_le_bytes());
                while p.try_push(&buf).is_err() { std::hint::spin_loop(); }
            }
        }));
    }
    for (k, c) in consumers.into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        let (consumed, sum) = (Arc::clone(&consumed), Arc::clone(&sum));
        handles.push(thread::spawn(move || {
            pin_current_thread_to_core(np + k);
            let mut out = [0u8; SPSC_PAYLOAD_BYTES];
            let mut local = 0u64;
            b.wait();
            loop {
                if consumed.load(Ordering::Acquire) >= actual_total as usize { break; }
                if c.try_pop(&mut out).is_ok() {
                    local = local.wrapping_add(u64::from_le_bytes(out[..8].try_into().unwrap()));
                    consumed.fetch_add(1, Ordering::AcqRel);
                }
            }
            sum.fetch_add(local, Ordering::AcqRel);
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    for h in handles { h.join().unwrap(); }
    let elapsed = t0.elapsed().as_secs_f64();
    let ok = consumed.load(Ordering::Acquire) == actual_total as usize
        && sum.load(Ordering::Acquire) == expected_sum(np, per_prod);
    (actual_total as f64 / elapsed / 1e6, ok)
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--single") {
        let kind = argv[2].as_str();
        let np: usize = argv[3].parse().unwrap();
        let nc: usize = argv[4].parse().unwrap();
        let total: u64 = argv[5].parse().unwrap();
        let (t, ok) = match kind {
            "vyukov" => measure_vyukov(np, nc, total),
            "sharded" => measure_sharded(np, nc, total),
            _ => panic!("kind must be vyukov|sharded"),
        };
        println!("single {kind} {np}P/{nc}C total={total} ok={ok} {t:.2} M items/s");
        return;
    }

    println!("MPMC: Vyukov shared-counter vs sharded per-producer lanes (same total buffer)");
    println!("total buffer {TOTAL_CAP} slots, {TOTAL} items/measurement, {REPS} interleaved");
    println!("rounds, pinned. sharded trades global FIFO for per-producer FIFO.\n");

    // [config_idx] -> (vyukov samples, sharded samples)
    let mut vy: Vec<Vec<f64>> = vec![Vec::new(); CONFIGS.len()];
    let mut sh: Vec<Vec<f64>> = vec![Vec::new(); CONFIGS.len()];
    let mut all_ok = true;
    for round in 0..=REPS {
        for (ci, &(np, nc)) in CONFIGS.iter().enumerate() {
            let (tv, okv) = measure_vyukov(np, nc, TOTAL);
            let (ts, oks) = measure_sharded(np, nc, TOTAL);
            all_ok &= okv && oks;
            if round > 0 {
                vy[ci].push(tv);
                sh[ci].push(ts);
            }
        }
    }

    println!("integrity (exact count + sum) held in every cell: {all_ok}\n");
    println!("{:<10} {:>14} {:>14} {:>10}", "config", "vyukov M/s", "sharded M/s", "speedup");
    for (ci, &(np, nc)) in CONFIGS.iter().enumerate() {
        let mut v = vy[ci].clone();
        let mut s = sh[ci].clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let (vm, sm) = (pct(&v, 0.5), pct(&s, 0.5));
        println!("{:<10} {:>14.2} {:>14.2} {:>9.2}x",
                 format!("{np}P/{nc}C"), vm, sm, sm / vm);
    }
}
