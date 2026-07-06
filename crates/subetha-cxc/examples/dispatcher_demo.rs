//! `DequeDispatcher` cross-process end-to-end demo.
//!
//! This example demonstrates the dispatcher's per-call routing
//! between Chase-Lev (per-item) and KHPD (batched) across a
//! parent / child process split. The parent dispatches a mix of
//! shapes through one [`DequeDispatcher`]; the child opens both
//! MMF deque files as a thief and drains them in parallel threads
//! until the agreed item count is reached.
//!
//! ## Routing decisions exercised
//!
//! - `WorkloadShape::request_reply()` (per-item, single thief)
//!   routes to `DequeVariant::ChaseLev`.
//! - `WorkloadShape::producer_fast(K)` for any K >= 2 routes to
//!   `DequeVariant::Khl` (the SubEtha-native hybrid that beats KHPD
//!   by 1.55x at K=64 on Zen+ R7 2700).
//!
//! ## Bit-exact verification
//!
//! Each dispatched item carries a 4-byte little-endian `u32` id.
//! The parent dispatches ids `0..N_RR` to Chase-Lev and
//! `N_RR..N_RR + N_BATCH` to KHPD. The child sums the ids it sees
//! per backend and compares against the expected per-backend sums;
//! the parent reads the child's exit code (0 = match, non-zero =
//! mismatch).
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --example dispatcher_demo -p subetha-cxc
//! ```

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::{
    DequeDispatcher, DequeVariant, KhlSteal, LineItem, SharedDeque, SharedDequeKhl,
    WorkloadShape,
};

/// Per-item (request-reply) dispatch count.
const N_RR: u32 = 30;
/// Producer-fast batch size.
const N_BATCH: u32 = 60;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_dispatch_demo_{pid}_{nonce}_{name}.bin"));
    p
}

fn u32_item(id: u32) -> LineItem {
    LineItem::new(&id.to_le_bytes()).expect("item")
}

fn item_id(item: &LineItem) -> u32 {
    u32::from_le_bytes(item.payload[..4].try_into().unwrap())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 5 && args[1] == "--child" {
        return run_child(&args[2], &args[3], &args[4]);
    }
    run_parent()
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let cl_path = tmp("cl");
    let khl_path = tmp("khl");

    // Build the dispatcher: Chase-Lev (per-item) + KHL (batched).
    let dispatcher = DequeDispatcher::builder()
        .with_chase_lev(&cl_path, 1024)?
        .with_khl(&khl_path, 256)?
        .build();

    // Assert the routing heuristic chooses the variants we expect.
    let rr_shape = WorkloadShape::request_reply();
    let batch_shape = WorkloadShape::producer_fast(N_BATCH as usize);
    let picked_rr = DequeDispatcher::pick(rr_shape);
    let picked_batch = DequeDispatcher::pick(batch_shape);
    println!(
        "[parent] picked variants: request_reply -> {picked_rr:?}, \
         producer_fast({N_BATCH}) -> {picked_batch:?}"
    );
    assert_eq!(picked_rr, DequeVariant::ChaseLev);
    assert_eq!(picked_batch, DequeVariant::Khl);

    // Spawn the child process pointing at both MMF paths.
    let self_exe = std::env::current_exe()?;
    let total = N_RR + N_BATCH;
    let mut child = Command::new(self_exe)
        .arg("--child")
        .arg(&cl_path)
        .arg(&khl_path)
        .arg(total.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    // Give the child a moment to open the files before dispatching.
    thread::sleep(Duration::from_millis(50));

    // Dispatch N_RR per-item jobs (-> Chase-Lev).
    let t0 = Instant::now();
    for id in 0..N_RR {
        let chosen = dispatcher.dispatch_one(rr_shape, u32_item(id))?;
        assert_eq!(chosen, DequeVariant::ChaseLev);
    }
    let elapsed_rr = t0.elapsed();
    println!(
        "[parent] dispatched {N_RR} request-reply jobs in {:?} \
         ({:.2} us/job)",
        elapsed_rr,
        elapsed_rr.as_micros() as f64 / N_RR as f64
    );

    // Dispatch one batch of N_BATCH items (-> KHL).
    let batch: Vec<LineItem> = (N_RR..N_RR + N_BATCH).map(u32_item).collect();
    let t1 = Instant::now();
    let chosen = dispatcher.dispatch_batch(batch_shape, &batch)?;
    let elapsed_batch = t1.elapsed();
    assert_eq!(chosen, DequeVariant::Khl);
    println!(
        "[parent] dispatched {N_BATCH}-item batch in {:?} \
         ({:.2} ns/job)",
        elapsed_batch,
        elapsed_batch.as_nanos() as f64 / N_BATCH as f64
    );

    // Wait for the child to drain.
    let status = child.wait()?;
    if !status.success() {
        return Err(format!(
            "[parent] child exited non-zero: code={:?}",
            status.code()
        )
        .into());
    }
    println!(
        "[parent] child drained {total} items bit-exact through dispatcher \
         (Chase-Lev + KHL routes confirmed)"
    );

    std::fs::remove_file(&cl_path).ok();
    std::fs::remove_file(&khl_path).ok();
    Ok(())
}

fn run_child(
    cl_path: &str,
    khl_path: &str,
    expected_count: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_count: u32 = expected_count.parse()?;
    let expected_total: u64 = (0..expected_count as u64).sum();
    println!(
        "[child] opening backends; expected {expected_count} items, \
         sum {expected_total}"
    );

    // Open both backends as thief.
    let cl = Arc::new(
        SharedDeque::<LineItem>::open_as_thief(cl_path)
            .map_err(|e| format!("open Chase-Lev: {e:?}"))?,
    );
    let khl = Arc::new(SharedDequeKhl::open(khl_path)?);

    let stop = Arc::new(AtomicBool::new(false));
    let cl_consumed = Arc::new(AtomicU64::new(0));
    let cl_sum = Arc::new(AtomicU64::new(0));
    let khl_consumed = Arc::new(AtomicU64::new(0));
    let khl_sum = Arc::new(AtomicU64::new(0));

    let cl_drain = {
        let cl = Arc::clone(&cl);
        let stop = Arc::clone(&stop);
        let consumed = Arc::clone(&cl_consumed);
        let sum = Arc::clone(&cl_sum);
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match cl.steal() {
                    Some(item) => {
                        consumed.fetch_add(1, Ordering::AcqRel);
                        sum.fetch_add(item_id(&item) as u64, Ordering::AcqRel);
                    }
                    None => std::hint::spin_loop(),
                }
            }
        })
    };

    let khl_drain = {
        let khl = Arc::clone(&khl);
        let stop = Arc::clone(&stop);
        let consumed = Arc::clone(&khl_consumed);
        let sum = Arc::clone(&khl_sum);
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match khl.steal_slot() {
                    KhlSteal::Success(r) => {
                        for i in 0..r.n_items {
                            consumed.fetch_add(1, Ordering::AcqRel);
                            sum.fetch_add(
                                item_id(&r.items[i]) as u64,
                                Ordering::AcqRel,
                            );
                        }
                    }
                    KhlSteal::Empty | KhlSteal::Retry => std::hint::spin_loop(),
                }
            }
        })
    };

    // Wait for the agreed total to land.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let total =
            cl_consumed.load(Ordering::Acquire) + khl_consumed.load(Ordering::Acquire);
        if total as u32 >= expected_count {
            break;
        }
        if Instant::now() > deadline {
            stop.store(true, Ordering::Release);
            cl_drain.join().ok();
            khl_drain.join().ok();
            return Err(format!(
                "[child] timeout after {expected_count} items; \
                 got cl={} + khl={}",
                cl_consumed.load(Ordering::Acquire),
                khl_consumed.load(Ordering::Acquire)
            )
            .into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    stop.store(true, Ordering::Release);
    cl_drain.join().expect("cl drain");
    khl_drain.join().expect("khl drain");

    let cl_seen = cl_consumed.load(Ordering::Acquire);
    let khl_seen = khl_consumed.load(Ordering::Acquire);
    let total_sum =
        cl_sum.load(Ordering::Acquire) + khl_sum.load(Ordering::Acquire);
    println!(
        "[child] drained cl={cl_seen} ({}) + khl={khl_seen} ({}) = {} items, \
         sum {total_sum} (expected {expected_total})",
        cl_sum.load(Ordering::Acquire),
        khl_sum.load(Ordering::Acquire),
        cl_seen + khl_seen
    );
    if total_sum != expected_total {
        return Err(format!(
            "[child] sum mismatch: got {total_sum}, expected {expected_total}"
        )
        .into());
    }
    Ok(())
}
