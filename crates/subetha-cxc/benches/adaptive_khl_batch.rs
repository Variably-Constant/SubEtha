//! A/B: AdaptiveIpc<u64> batched round-trip - the KHL batch fast path
//! (what `build_adaptive` now wires for >=2-item batches) vs the
//! per-item paths it could replace.
//!
//! Bench audit (stated before the numbers):
//!  - Feature-exercising: arm `khl_batch` calls `send_batch` (routes to
//!    KHL: 3 items/Release-store + shared recv surplus); arm
//!    `ring_per_item` calls `send` per item (the active ring backing,
//!    `send_batch`'s old behavior when the workload sits on the ring).
//!  - No self-inflicted handicap: identical u64 items, identical batch
//!    size, identical capacity, identical `recv` drain; the only
//!    difference is the producer path. Integrity asserted (sum) on both.
//!  - Sized for the workload: a sweep over batch size, capacity sized
//!    to hold one batch.
//!
//! The decision this settles: routing batches to KHL must BEAT the ring
//! per-item path (the default active backing), not just the deque, or
//! the wiring should gate on a deque-shaped workload.
//!
//! Run: cargo bench --bench adaptive_khl_batch -p subetha-cxc

use std::time::Instant;

use subetha_cxc::{AdaptiveIpc, MmfWorkloadShape};

fn tmp(tag: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("subetha-khl-bench-{tag}-{pid}-{nonce}"))
}

/// Round-trip `iters` batches of `batch` u64 items, sending via
/// `send_batch` (KHL) and draining via `recv`. Returns M items/s.
fn run_khl_batch(batch: usize, iters: usize) -> f64 {
    let path = tmp("khl");
    let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
    let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 1024, 1).expect("create");
    let items: Vec<u64> = (0..batch as u64).collect();
    let expected: u64 = items.iter().sum::<u64>() * iters as u64;
    let mut got_sum = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        ipc.send_batch(&items).expect("send_batch");
        let mut drained = 0;
        while drained < batch {
            if let Ok(v) = ipc.recv() {
                got_sum = got_sum.wrapping_add(v);
                drained += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    assert_eq!(got_sum, expected, "khl_batch integrity");
    (batch * iters) as f64 / secs / 1e6
}

/// Same round-trip but the producer sends each item via `send` (the
/// active ring backing - `send_batch`'s old per-item behavior).
fn run_ring_per_item(batch: usize, iters: usize) -> f64 {
    let path = tmp("ring");
    let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
    let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 1024, 1).expect("create");
    let items: Vec<u64> = (0..batch as u64).collect();
    let expected: u64 = items.iter().sum::<u64>() * iters as u64;
    let mut got_sum = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        for it in &items {
            while ipc.send(it).is_err() {
                std::hint::spin_loop();
            }
        }
        let mut drained = 0;
        while drained < batch {
            if let Ok(v) = ipc.recv() {
                got_sum = got_sum.wrapping_add(v);
                drained += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    assert_eq!(got_sum, expected, "ring_per_item integrity");
    (batch * iters) as f64 / secs / 1e6
}

fn main() {
    println!("=== AdaptiveIpc<u64> batched round-trip: KHL batch vs ring per-item ===\n");
    println!("  {:<8} {:>16} {:>16} {:>10}", "batch", "khl_batch M/s", "ring_item M/s", "ratio");
    for &batch in &[2usize, 4, 8, 16, 64, 256] {
        let iters = (2_000_000 / batch).max(2000);
        let khl = run_khl_batch(batch, iters);
        let ring = run_ring_per_item(batch, iters);
        println!("  {:<8} {:>16.2} {:>16.2} {:>9.2}x", batch, khl, ring, khl / ring);
    }
}
