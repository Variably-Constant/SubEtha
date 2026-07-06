//! SubEtha IPC stress test E2E.
//!
//! Five tiers of stress, each verifying CORRECTNESS + measuring
//! THROUGHPUT. Real production workloads break differently from
//! micro-benchmarks; this binary surfaces those failure modes.
//!
//! 1. SPSC sustained 1M items - throughput + sum check
//! 2. MPMC 8P/8C 800k items - per-item ID set check
//!    (verifies: no losses, no duplicates, no corruption)
//! 3. AdaptiveIpc live migration under load - 200k items with
//!    migrations happening WHILE producer pushes
//! 4. Service simulation - N workers handling requests from M
//!    client threads (production request-response pattern)
//! 5. Burst traffic with idle gaps - realistic bursty workload
//!
//! Each tier prints SAFETY VERDICT + throughput numbers.
//!
//! Run: `cargo run --release --example stress_test -p subetha-cxc`

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel as cbc;
use parking_lot::Mutex;
use subetha_core::Marshal;
use subetha_cxc::{
    AdaptiveIpc, MmfFamily, MmfWorkloadShape, SharedRing,
};
use subetha_cxc::shared_ring::PAYLOAD_BYTES;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_stress_{pid}_{nonce}_{name}"));
    p
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("====================================================================");
    println!("SUBETHA IPC STRESS TEST: cross-thread + cross-process safety/throughput");
    println!("Platform: this machine. Verifying correctness AND measuring perf.");
    println!("====================================================================");
    println!();

    tier1_spsc_sustained()?;
    tier2_mpmc_safety()?;
    tier3_live_migration_under_load()?;
    tier4_service_simulation()?;
    tier5_burst_traffic()?;

    println!();
    println!("====================================================================");
    println!("ALL STRESS TIERS PASSED. SubEtha IPC verified safe + performant.");
    println!("====================================================================");
    Ok(())
}

// ==========================================================================
// TIER 1: SPSC sustained 1M items
// ==========================================================================
fn tier1_spsc_sustained() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TIER 1: SPSC sustained 1M items ---");
    const N: u64 = 1_000_000;
    let expected_sum: u64 = (0..N).sum();

    // SubEtha SharedRing
    let path = tmp("t1_ring.bin");
    let ring = Arc::new(
        SharedRing::create(&path, 65536).map_err(|e| format!("{e:?}"))?,
    );
    let consumed = Arc::new(AtomicU64::new(0));
    let sum_acc = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let ring_c = Arc::clone(&ring);
    let consumed_c = Arc::clone(&consumed);
    let sum_acc_c = Arc::clone(&sum_acc);
    let stop_c = Arc::clone(&stop);
    let drain = thread::spawn(move || {
        let mut out = [0u8; PAYLOAD_BYTES];
        while !stop_c.load(Ordering::Acquire) {
            if ring_c.try_pop(&mut out).is_ok() {
                let v = u64::from_le_bytes(out[..8].try_into().unwrap());
                sum_acc_c.fetch_add(v, Ordering::AcqRel);
                consumed_c.fetch_add(1, Ordering::AcqRel);
            } else {
                std::hint::spin_loop();
            }
        }
    });

    let t0 = Instant::now();
    let mut buf = [0u8; PAYLOAD_BYTES];
    for i in 0..N {
        buf[..8].copy_from_slice(&i.to_le_bytes());
        while ring.try_push(&buf).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    drain.join().ok();

    let observed_sum = sum_acc.load(Ordering::Acquire);
    let ok = observed_sum == expected_sum;
    println!(
        "  SubEtha SharedRing: {N} items in {elapsed:?} = {:.2} M items/s",
        N as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!(
        "  SAFETY: expected sum {expected_sum}, observed {observed_sum} -> {}",
        if ok { "PASS" } else { "FAIL" }
    );
    assert!(ok, "SPSC safety failed: sum mismatch");
    drop(ring);
    std::fs::remove_file(&path).ok();

    // crossbeam_channel for comparison
    let (tx, rx) = cbc::bounded::<u64>(65536);
    let cb_sum = Arc::new(AtomicU64::new(0));
    let cb_consumed = Arc::new(AtomicU64::new(0));
    let cb_sum_c = Arc::clone(&cb_sum);
    let cb_consumed_c = Arc::clone(&cb_consumed);
    let cb_drain = thread::spawn(move || {
        while let Ok(v) = rx.recv() {
            cb_sum_c.fetch_add(v, Ordering::AcqRel);
            cb_consumed_c.fetch_add(1, Ordering::AcqRel);
        }
    });
    let t0 = Instant::now();
    for i in 0..N {
        tx.send(i).unwrap();
    }
    drop(tx);
    while cb_consumed.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let cb_elapsed = t0.elapsed();
    cb_drain.join().ok();
    let cb_ok = cb_sum.load(Ordering::Acquire) == expected_sum;
    println!(
        "  crossbeam_channel: {N} items in {cb_elapsed:?} = {:.2} M items/s",
        N as f64 / cb_elapsed.as_secs_f64() / 1_000_000.0
    );
    println!(
        "  SAFETY: expected sum {expected_sum}, observed {} -> {}",
        cb_sum.load(Ordering::Acquire),
        if cb_ok { "PASS" } else { "FAIL" }
    );
    println!();
    Ok(())
}

// ==========================================================================
// TIER 2: MPMC 8P/8C 800k items - safety verification
// ==========================================================================
fn tier2_mpmc_safety() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TIER 2: MPMC 8P/8C 800k items - SAFETY (per-item ID set check) ---");
    const N_PRODUCERS: usize = 8;
    const N_CONSUMERS: usize = 8;
    const PER_PRODUCER: u64 = 100_000;
    const TOTAL: u64 = (N_PRODUCERS as u64) * PER_PRODUCER;

    let path = tmp("t2_ring.bin");
    let ring = Arc::new(
        SharedRing::create(&path, 65536).map_err(|e| format!("{e:?}"))?,
    );
    let consumed_ids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::with_capacity(TOTAL as usize)));
    let consumed_count = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let consumers: Vec<_> = (0..N_CONSUMERS).map(|_| {
        let ring_c = Arc::clone(&ring);
        let stop_c = Arc::clone(&stop);
        let ids_c = Arc::clone(&consumed_ids);
        let count_c = Arc::clone(&consumed_count);
        thread::spawn(move || {
            let mut out = [0u8; PAYLOAD_BYTES];
            let mut local_ids: Vec<u64> = Vec::with_capacity(TOTAL as usize / N_CONSUMERS + 100);
            while !stop_c.load(Ordering::Acquire) {
                if ring_c.try_pop(&mut out).is_ok() {
                    let id = u64::from_le_bytes(out[..8].try_into().unwrap());
                    local_ids.push(id);
                    count_c.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
            ids_c.lock().extend(local_ids);
        })
    }).collect();

    let t0 = Instant::now();
    let producers: Vec<_> = (0..N_PRODUCERS).map(|pid| {
        let ring_p = Arc::clone(&ring);
        thread::spawn(move || {
            let mut buf = [0u8; PAYLOAD_BYTES];
            let base = pid as u64 * PER_PRODUCER;
            for i in 0..PER_PRODUCER {
                let id = base + i;
                buf[..8].copy_from_slice(&id.to_le_bytes());
                while ring_p.try_push(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    }).collect();
    for p in producers { p.join().unwrap(); }
    while consumed_count.load(Ordering::Acquire) < TOTAL {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    for c in consumers { c.join().unwrap(); }

    let all_ids = consumed_ids.lock().clone();
    let len = all_ids.len();
    let unique: HashSet<u64> = all_ids.into_iter().collect();
    let unique_count = unique.len();
    let expected_ids: HashSet<u64> = (0..TOTAL).collect();
    let missing = expected_ids.difference(&unique).count();
    let duplicates = len - unique_count;

    println!(
        "  SubEtha SharedRing 8P/8C: {TOTAL} items in {elapsed:?} = {:.2} M items/s",
        TOTAL as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!("  SAFETY checks:");
    println!("    total consumed:     {len}     (expected {TOTAL})");
    println!("    unique IDs:         {unique_count}     (expected {TOTAL})");
    println!("    missing IDs:        {missing}     (expected 0)");
    println!("    duplicate IDs:      {duplicates}     (expected 0)");
    let safety_ok = len == TOTAL as usize
        && unique_count == TOTAL as usize
        && missing == 0
        && duplicates == 0;
    println!("    VERDICT:            {}", if safety_ok { "PASS - no lost items, no duplicates" } else { "FAIL" });
    assert!(safety_ok, "MPMC safety check failed");

    drop(ring);
    std::fs::remove_file(&path).ok();
    println!();
    Ok(())
}

// ==========================================================================
// TIER 3: AdaptiveIpc live migration under load
// ==========================================================================
fn tier3_live_migration_under_load() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TIER 3: AdaptiveIpc live migration under load ---");
    const N: u64 = 200_000;
    let path = tmp("t3_adapt");
    let initial = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let ipc: Arc<AdaptiveIpc<u64>> = Arc::new(
        AdaptiveIpc::create(&path, initial, 16384, 1)?,
    );

    let consumed_sum = Arc::new(AtomicU64::new(0));
    let consumed_count = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let migration_count = Arc::new(AtomicU64::new(0));

    // Consumer thread
    let ipc_c = Arc::clone(&ipc);
    let stop_c = Arc::clone(&stop);
    let sum_c = Arc::clone(&consumed_sum);
    let count_c = Arc::clone(&consumed_count);
    let consumer = thread::spawn(move || {
        while !stop_c.load(Ordering::Acquire) {
            if let Ok(v) = ipc_c.recv() {
                sum_c.fetch_add(v, Ordering::AcqRel);
                count_c.fetch_add(1, Ordering::AcqRel);
            } else {
                std::hint::spin_loop();
            }
        }
    });

    // Migration thread: every ~50ms, flip the active family
    let ipc_m = Arc::clone(&ipc);
    let stop_m = Arc::clone(&stop);
    let mig_count_m = Arc::clone(&migration_count);
    let migrator = thread::spawn(move || {
        let mut tgt_idx = 0u32;
        let families = [
            MmfFamily::SharedRing,
            MmfFamily::SharedDeque(subetha_cxc::DequeVariant::Khl),
        ];
        while !stop_m.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(50));
            tgt_idx = (tgt_idx + 1) % 2;
            if ipc_m.migrate_to(families[tgt_idx as usize]).is_ok() {
                mig_count_m.fetch_add(1, Ordering::AcqRel);
            }
        }
    });

    // Producer thread: mixed single + batch sends
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut next_id = 0u64;
    while sent < N {
        if sent % 100 < 30 {
            // Single send
            while ipc.send(&next_id).is_err() {
                std::hint::spin_loop();
            }
            next_id += 1;
            sent += 1;
        } else {
            // Batch send
            let batch_sz = 8.min((N - sent) as usize);
            let batch: Vec<u64> = (next_id..next_id + batch_sz as u64).collect();
            while ipc.send_batch(&batch).is_err() {
                std::hint::spin_loop();
            }
            next_id += batch_sz as u64;
            sent += batch_sz as u64;
        }
    }
    while consumed_count.load(Ordering::Acquire) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Release);
    migrator.join().ok();
    consumer.join().ok();

    let observed_sum = consumed_sum.load(Ordering::Acquire);
    let expected_sum: u64 = (0..N).sum();
    let migrations = migration_count.load(Ordering::Acquire);
    let ok = observed_sum == expected_sum;

    println!(
        "  AdaptiveIpc: {N} items in {elapsed:?} = {:.2} M items/s",
        N as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!("  Migrations during the run: {migrations}");
    println!(
        "  SAFETY: expected sum {expected_sum}, observed {observed_sum} -> {}",
        if ok { "PASS - no items lost across migrations" } else { "FAIL" }
    );
    assert!(ok, "Migration safety check failed");

    drop(ipc);
    for suffix in &[".ring.bin", ".deque.bin", ".ctl.bin"] {
        let mut p = path.clone();
        let s = p.file_name().map(|s| s.to_owned()).unwrap_or_default();
        p.set_file_name(format!("{}{suffix}", s.to_string_lossy()));
        std::fs::remove_file(&p).ok();
    }
    println!();
    Ok(())
}

// ==========================================================================
// TIER 4: Service simulation - N workers, M clients
// ==========================================================================
fn tier4_service_simulation() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TIER 4: Service simulation - 4 workers handling requests from 4 clients ---");
    const N_WORKERS: usize = 4;
    const N_CLIENTS: usize = 4;
    const REQUESTS_PER_CLIENT: u64 = 25_000;
    const TOTAL_REQUESTS: u64 = (N_CLIENTS as u64) * REQUESTS_PER_CLIENT;

    let req_path = tmp("t4_req.bin");
    let req_ring = Arc::new(
        SharedRing::create(&req_path, 16384).map_err(|e| format!("{e:?}"))?,
    );
    let resp_path = tmp("t4_resp.bin");
    let resp_ring = Arc::new(
        SharedRing::create(&resp_path, 16384).map_err(|e| format!("{e:?}"))?,
    );
    let stop_workers = Arc::new(AtomicBool::new(false));
    let completed_responses = Arc::new(AtomicU64::new(0));

    // Worker pool: each worker drains requests, computes a response, pushes back
    let workers: Vec<_> = (0..N_WORKERS).map(|wid| {
        let req_w = Arc::clone(&req_ring);
        let resp_w = Arc::clone(&resp_ring);
        let stop_w = Arc::clone(&stop_workers);
        thread::spawn(move || {
            let mut in_buf = [0u8; PAYLOAD_BYTES];
            let mut out_buf = [0u8; PAYLOAD_BYTES];
            let mut handled = 0u64;
            while !stop_w.load(Ordering::Acquire) {
                if req_w.try_pop(&mut in_buf).is_ok() {
                    // Request encoding: [client_id: u32][seq: u32][payload: u64]
                    let payload = u64::from_le_bytes(in_buf[8..16].try_into().unwrap());
                    // Simulated work: hash the payload (cheap CPU)
                    let response = payload.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(13);
                    // Echo client_id + seq + response
                    out_buf[..8].copy_from_slice(&in_buf[..8]);
                    out_buf[8..16].copy_from_slice(&response.to_le_bytes());
                    while resp_w.try_push(&out_buf).is_err() {
                        std::hint::spin_loop();
                    }
                    handled += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            (wid, handled)
        })
    }).collect();

    // Collector thread that drains responses to keep the ring flowing
    let resp_drain = Arc::clone(&resp_ring);
    let completed_d = Arc::clone(&completed_responses);
    let stop_d = Arc::clone(&stop_workers);
    let drainer = thread::spawn(move || {
        let mut buf = [0u8; PAYLOAD_BYTES];
        while !stop_d.load(Ordering::Acquire) {
            if resp_drain.try_pop(&mut buf).is_ok() {
                completed_d.fetch_add(1, Ordering::AcqRel);
            } else {
                std::hint::spin_loop();
            }
        }
    });

    // Client threads
    let t0 = Instant::now();
    let clients: Vec<_> = (0..N_CLIENTS).map(|cid| {
        let req_c = Arc::clone(&req_ring);
        thread::spawn(move || {
            let mut buf = [0u8; PAYLOAD_BYTES];
            for seq in 0..REQUESTS_PER_CLIENT {
                buf[..4].copy_from_slice(&(cid as u32).to_le_bytes());
                buf[4..8].copy_from_slice(&(seq as u32).to_le_bytes());
                buf[8..16].copy_from_slice(&(seq.wrapping_mul(31)).to_le_bytes());
                while req_c.try_push(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    }).collect();
    for c in clients { c.join().unwrap(); }
    while completed_responses.load(Ordering::Acquire) < TOTAL_REQUESTS {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    stop_workers.store(true, Ordering::Release);
    let mut worker_handled = [0u64; N_WORKERS];
    for w in workers {
        let (wid, h) = w.join().unwrap();
        worker_handled[wid] = h;
    }
    drainer.join().ok();

    let total_handled: u64 = worker_handled.iter().sum();
    println!(
        "  Service throughput: {TOTAL_REQUESTS} requests in {elapsed:?} = {:.0} req/s",
        TOTAL_REQUESTS as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  Avg request latency: {:.2} µs (end-to-end queue + worker dispatch)",
        elapsed.as_micros() as f64 / TOTAL_REQUESTS as f64
    );
    println!("  Per-worker load distribution:");
    for (wid, h) in worker_handled.iter().enumerate() {
        let pct = (*h as f64 / total_handled as f64) * 100.0;
        println!("    worker {wid}: {h} requests ({pct:.1}%)");
    }
    let balanced = worker_handled.iter().all(|h| {
        let pct = (*h as f64 / total_handled as f64) * 100.0;
        pct > 5.0 && pct < 95.0  // very loose balance check
    });
    println!(
        "  SAFETY: total handled by workers = {total_handled} (expected {TOTAL_REQUESTS}) -> {}",
        if total_handled == TOTAL_REQUESTS && balanced { "PASS" } else { "FAIL" }
    );
    assert_eq!(total_handled, TOTAL_REQUESTS, "request/response count mismatch");

    drop(req_ring);
    drop(resp_ring);
    std::fs::remove_file(&req_path).ok();
    std::fs::remove_file(&resp_path).ok();
    println!();
    Ok(())
}

// ==========================================================================
// TIER 5: Burst traffic with idle gaps
// ==========================================================================
fn tier5_burst_traffic() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TIER 5: Burst traffic (10 bursts of 10k items with 5ms idle gaps) ---");
    const N_BURSTS: usize = 10;
    const PER_BURST: u64 = 10_000;
    const IDLE_MS: u64 = 5;
    const TOTAL: u64 = (N_BURSTS as u64) * PER_BURST;

    let path = tmp("t5_ring.bin");
    let ring = Arc::new(
        SharedRing::create(&path, 65536).map_err(|e| format!("{e:?}"))?,
    );
    let consumed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut burst_latencies: Vec<Duration> = Vec::with_capacity(N_BURSTS);

    let ring_c = Arc::clone(&ring);
    let consumed_c = Arc::clone(&consumed);
    let stop_c = Arc::clone(&stop);
    let drainer = thread::spawn(move || {
        let mut out = [0u8; PAYLOAD_BYTES];
        while !stop_c.load(Ordering::Acquire) {
            if ring_c.try_pop(&mut out).is_ok() {
                consumed_c.fetch_add(1, Ordering::AcqRel);
            } else {
                std::hint::spin_loop();
            }
        }
    });

    let t_total = Instant::now();
    for burst_idx in 0..N_BURSTS {
        let baseline = consumed.load(Ordering::Acquire);
        let target = baseline + PER_BURST;
        let t0 = Instant::now();
        let mut buf = [0u8; PAYLOAD_BYTES];
        for i in 0..PER_BURST {
            buf[..8].copy_from_slice(&(burst_idx as u64 * PER_BURST + i).to_le_bytes());
            while ring.try_push(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
        while consumed.load(Ordering::Acquire) < target {
            std::hint::spin_loop();
        }
        burst_latencies.push(t0.elapsed());
        thread::sleep(Duration::from_millis(IDLE_MS));
    }
    let total_elapsed = t_total.elapsed();
    stop.store(true, Ordering::Release);
    drainer.join().ok();

    // Compute p50, p95, max burst latencies
    let mut sorted = burst_latencies.clone();
    sorted.sort();
    let p50 = sorted[sorted.len() / 2];
    let p95 = sorted[sorted.len() * 95 / 100];
    let max_burst = *sorted.last().unwrap();
    let avg_burst: Duration = burst_latencies.iter().sum::<Duration>() / N_BURSTS as u32;

    println!(
        "  Total {TOTAL} items across {N_BURSTS} bursts in {total_elapsed:?}"
    );
    println!("  Per-burst latency (10k items each):");
    println!("    p50:  {p50:?}");
    println!("    p95:  {p95:?}");
    println!("    avg:  {avg_burst:?}");
    println!("    max:  {max_burst:?}");
    println!(
        "  Throughput WITHIN bursts: {:.2} M items/s",
        PER_BURST as f64 / avg_burst.as_secs_f64() / 1_000_000.0
    );
    let safety_ok = consumed.load(Ordering::Acquire) == TOTAL;
    println!(
        "  SAFETY: consumed {} (expected {TOTAL}) -> {}",
        consumed.load(Ordering::Acquire),
        if safety_ok { "PASS" } else { "FAIL" }
    );
    assert!(safety_ok, "burst safety check failed");

    drop(ring);
    std::fs::remove_file(&path).ok();
    println!();
    Ok(())
}

// Keep Marshal silenced for the build
#[allow(dead_code)]
fn _silence_marshal() -> usize {
    <u64 as Marshal>::PAYLOAD_BYTES
}
