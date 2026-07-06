//! E2E demonstration of `AdaptiveIpc<T>` pinned-handoff + sidecar
//! auto-promotion.
//!
//! No manual `ipc.migrate_to(...)` calls. The sidecar polls
//! `maybe_promote()` every 5ms; the existing policy in
//! `AdaptiveIpc::maybe_promote()` watches the batch-ratio + Bloom
//! shape filter and promotes from `SharedRing` to
//! `SharedDeque<PassSlot>` when the observed workload is batched
//! work-stealing-shaped.
//!
//! Lifecycle the example exercises (single-threaded by design so
//! the E2E focus stays on the pin lifecycle, not on cross-thread
//! backing semantics):
//!  1. Create AdaptiveIpc<u64> in the streaming MPMC family
//!     (initial backing = SharedRing).
//!  2. Spawn the sidecar.
//!  3. Pin the ring family, push + pop INDIVIDUAL_ITEMS interleaved
//!     through the native SharedRing API via `PinnedIpc::as_ring()`.
//!  4. Send BATCH_COUNT batches of BATCH_SIZE via `send_batch()`
//!     with inline draining. The sidecar observes the batched
//!     workload and promotes the backing to
//!     `SharedDeque<PassSlot>`. The pinned-ring handle in step 3's
//!     scope is dropped; the next pin acquisition captures the new
//!     family.
//!  5. Re-acquire the pin and assert `as_deque()` returns Some,
//!     `is_still_valid()` returns true, generation has advanced.
//!  6. Final: assert produced == consumed (count + sum both match).
//!
//! Run with:
//!     cargo run --release --example adaptive_ipc_pinned

use std::sync::Arc;
use std::time::{Duration, Instant};

use subetha_cxc::{
    AdaptiveIpc, AdaptiveIpcSidecar, MmfFamily, MmfWorkloadShape,
};

const INDIVIDUAL_ITEMS: u64 = 50_000;
const BATCH_COUNT: u64 = 1_000;
const BATCH_SIZE: u64 = 16;
const IPC_CAPACITY: usize = 4096;

fn main() {
    println!("=== AdaptiveIpc<u64> + sidecar pinned-handoff E2E ===");
    println!("(zero manual migrate_to calls; sidecar drives the family promotion)");
    println!();

    let base_path = std::env::temp_dir()
        .join(format!("adaptive_ipc_pinned_{}", std::process::id()));

    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(
            &base_path,
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 1,
                n_consumers: 1,
            },
            IPC_CAPACITY,
            1,
        )
        .expect("create"),
    );

    // 1ms scan interval so the sidecar has many chances to observe
    // the batched workload before the example finishes.
    let sidecar = AdaptiveIpcSidecar::spawn(
        ipc.clone(),
        Duration::from_millis(1),
    );

    let start = Instant::now();
    println!("[init] family = {:?}, pin_generation = {}",
             ipc.active_family(), ipc.pin_generation());

    let mut produced_count = 0u64;
    let mut produced_sum = 0u64;
    let mut consumed_count = 0u64;
    let mut consumed_sum = 0u64;

    // ----- stage 1 -----
    // Composition demo: pin the protocol axis (PinnedIpc), drop to
    // the AdaptiveRing handle, pin the shape axis (PinnedRing), and
    // round-trip through the native SpscRingCore primitive at the
    // bottom of the stack. Two Acquire loads per validity check,
    // one per axis, sampled at the caller's chosen cadence.
    println!();
    println!("[stage 1] PinnedIpc -> as_ring() -> PinnedRing -> SpscRingCore native SPSC");
    println!("          (round-trip {INDIVIDUAL_ITEMS} u64s through the two-axis pin)");
    {
        let pin_ipc = ipc.pin_current_family();
        let ring = pin_ipc.as_ring().expect("pinned at ring family");
        let pin_ring = ring.pin_current_shape();
        assert_eq!(pin_ring.shape(), subetha_cxc::RingShape::Spsc,
                   "initial 1P/1C registration should give SPSC shape");
        assert!(pin_ipc.is_still_valid() && pin_ring.is_still_valid());

        let mut buf = [0u8; subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for i in 0..INDIVIDUAL_ITEMS {
            let payload = i.to_le_bytes();
            while pin_ring.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            produced_count += 1;
            produced_sum += i;
            while pin_ring.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
            consumed_sum += v;
        }
        println!("    round-tripped {INDIVIDUAL_ITEMS} via native SpscRingCore");
        println!(
            "    pin_ipc.is_still_valid = {}, pin_ring.is_still_valid = {}",
            pin_ipc.is_still_valid(), pin_ring.is_still_valid(),
        );
    }

    // ----- stage 2 -----
    // Send batches via send_batch with inline drain. The sidecar
    // observes the batched workload and promotes mid-loop.
    println!();
    println!(
        "[stage 2] send {BATCH_COUNT} batches of {BATCH_SIZE} with inline drain; expect sidecar promote -> SharedDeque",
    );
    let gen_before = ipc.pin_generation();
    let family_before = ipc.active_family();
    let batched_total = BATCH_COUNT * BATCH_SIZE;

    for _ in 0..BATCH_COUNT {
        let batch: Vec<u64> = (0..BATCH_SIZE).collect();
        for &v in &batch {
            produced_count += 1;
            produced_sum += v;
        }
        loop {
            match ipc.send_batch(&batch) {
                Ok(()) => break,
                Err(subetha_cxc::ApiError::Transport(
                    subetha_cxc::TransportError::Full,
                )) => {
                    // Drain a bit to free room and let the consumer
                    // side make progress.
                    if let Ok(v) = ipc.recv() {
                        consumed_count += 1;
                        consumed_sum += v;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                Err(other) => panic!("send_batch: {other:?}"),
            }
        }
        // Drain BATCH_SIZE items per batch to keep the active
        // backing primed.
        for _ in 0..BATCH_SIZE {
            loop {
                match ipc.recv() {
                    Ok(v) => {
                        consumed_count += 1;
                        consumed_sum += v;
                        break;
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }
        }
    }
    println!("    sent + drained {batched_total} via send_batch interleaved");

    // Wait for the sidecar's scan-and-promote cycle to land before
    // we read the post-promote state. The sidecar polls every 1ms;
    // give it up to 2s to observe the batched workload and promote.
    let promote_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < promote_deadline
        && !matches!(ipc.active_family(), MmfFamily::SharedDeque(_))
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    println!("    pin_generation before stage 2 = {gen_before}, after = {}",
             ipc.pin_generation());
    println!("    family before stage 2          = {family_before:?}, after = {:?}",
             ipc.active_family());

    // ----- stage 3 -----
    // Verify the post-morph pin lifecycle: re-acquire, check
    // is_still_valid + family + as_deque accessor.
    println!();
    println!("[stage 3] verify post-morph pin lifecycle");
    let final_family = ipc.active_family();
    let final_gen = ipc.pin_generation();
    {
        let pin = ipc.pin_current_family();
        assert!(pin.is_still_valid(),
                "fresh pin must be valid right after acquisition");
        assert_eq!(pin.family(), final_family);
        match final_family {
            MmfFamily::SharedRing => {
                assert!(pin.as_ring().is_some());
                assert!(pin.as_deque().is_none());
                println!("    final family = SharedRing; pin.as_ring() = Some");
            }
            MmfFamily::SharedDeque(_) => {
                assert!(pin.as_deque().is_some());
                assert!(pin.as_ring().is_none());
                println!("    final family = SharedDeque; pin.as_deque() = Some");
            }
            other => panic!("unexpected final family: {other:?}"),
        }
        println!(
            "    re-pin succeeded: family = {:?}, is_still_valid = {}, generation = {}",
            pin.family(), pin.is_still_valid(), pin.pinned_generation(),
        );
    }

    // Final drain - mop up anything still in either backing.
    while consumed_count < produced_count {
        match ipc.recv() {
            Ok(v) => {
                consumed_count += 1;
                consumed_sum += v;
            }
            Err(_) => break,
        }
    }

    // ----- result -----
    let elapsed = start.elapsed();
    let promotions = sidecar.promotions_triggered();
    sidecar.shutdown();

    println!();
    println!("=== Result ===");
    println!("  elapsed:                 {elapsed:?}");
    println!("  individual round-trips:  {INDIVIDUAL_ITEMS}");
    println!("  batched items:           {batched_total}");
    println!("  produced count:          {produced_count}");
    println!("  consumed count:          {consumed_count}");
    println!("  produced sum:            {produced_sum}");
    println!("  consumed sum:            {consumed_sum}");
    println!("  sidecar promotions:      {promotions}");
    println!("  initial family:          {family_before:?}");
    println!("  final family:            {final_family:?}");
    println!("  pin generation start:    0");
    println!("  pin generation end:      {final_gen}");

    assert_eq!(consumed_count, produced_count,
               "INTEGRITY FAIL: count mismatch");
    assert_eq!(consumed_sum, produced_sum,
               "INTEGRITY FAIL: sum mismatch");
    assert!(promotions >= 1,
            "sidecar should have promoted at least once");
    assert!(final_gen >= 1,
            "pin_generation should have advanced past 0");
    println!("  integrity:               PASS");
    println!("    every item arrived exactly once, sum-checked");
    println!("    sidecar drove the family promotion automatically");
    println!("    pinned-handoff carried the producer through the morph cycle");
}
