//! Cross-process capacity-morph E2E: the consumer / orchestrator.
//!
//! Proves the capacity morph survives a REAL cross-process consumer
//! end to end. Two OS processes; the data path is file-backed shared
//! memory (mmap), the morph coordination is a shared-memory control
//! atomic. The producer (`capacity_morph_xproc_producer`, spawned
//! here) walks a capacity ladder, each rung a fresh file-backed
//! `AdaptiveRing`, publishing the current `(capacity, epoch)` into the
//! control atomic. This consumer follows the control, opens each
//! backing by its deterministic name, and drains them in epoch order,
//! asserting the global sequence arrives EXACTLY ONCE, IN ORDER,
//! across every morph boundary and across the process boundary.
//!
//! This is the genuine cross-process test the in-process morph
//! experiments could not give: the morph creates a NEW shared backing
//! a separate process must discover and switch to, mid-stream, with
//! zero loss and zero reorder.
//!
//! Run (build the producer first):
//!     cargo build --release --example capacity_morph_xproc_producer -p subetha-cxc
//!     cargo run   --release --example capacity_morph_xproc           -p subetha-cxc

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use subetha_cxc::shared_atomic::SharedAtomicU64;
use subetha_cxc::adaptive_ring::AdaptiveRing;

const ITEMS_PER_EPOCH: u64 = 100_000;
const N_EPOCHS: u64 = 6; // capacity ladder 64 .. 2048
const NOT_READY: u64 = u64::MAX;

fn backing_path(prefix: &str, cap: usize, epoch: u64) -> String {
    format!("{prefix}_c{cap}_e{epoch}")
}

fn unpack(v: u64) -> (usize, u64) {
    ((v >> 32) as usize, v & 0xFFFF_FFFF)
}

fn tmp(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_capmorph_{pid}_{nonce}_{tag}"));
    p
}

fn cleanup_backing(prefix: &str, cap: usize, epoch: u64) {
    let base = backing_path(prefix, cap, epoch);
    for suffix in [".spsc.bin", ".vyukov.bin", ".ordering.bin"] {
        std::fs::remove_file(format!("{base}{suffix}")).ok();
    }
    for i in 0..1 {
        std::fs::remove_file(format!("{base}.mpsc.{i}.bin")).ok();
        std::fs::remove_file(format!("{base}.mpmc.{i}.bin")).ok();
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cross-process capacity-morph E2E (two OS processes, file-backed shared memory)");
    println!("producer walks a {N_EPOCHS}-rung capacity ladder (64..2048), morphing to a fresh");
    println!("backing each rung; this consumer follows the shared control atomic and drains");
    println!("each backing in order, asserting global exactly-once + FIFO across the boundary.");
    println!();

    let control_path = tmp("control");
    let ring_prefix = tmp("ring");
    let ring_prefix_str = ring_prefix.display().to_string();

    // Create the control atomic (the producer opens it). NOT_READY
    // until the producer publishes the first backing.
    let control = SharedAtomicU64::create(&control_path, NOT_READY)
        .map_err(|e| format!("create control: {e:?}"))?;

    // Spawn the real producer process.
    let self_exe = std::env::current_exe()?;
    let producer_exe = self_exe.parent().unwrap().join(format!(
        "capacity_morph_xproc_producer{}",
        std::env::consts::EXE_SUFFIX
    ));
    let mut child = Command::new(&producer_exe)
        .arg(format!("{}", control_path.display()))
        .arg(&ring_prefix_str)
        .arg(format!("{ITEMS_PER_EPOCH}"))
        .arg(format!("{N_EPOCHS}"))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    // Wait for the first backing to be published.
    let start = Instant::now();
    loop {
        if control.load(Ordering::Acquire) != NOT_READY {
            break;
        }
        if start.elapsed() > Duration::from_secs(30) {
            return Err("producer never published the first backing".into());
        }
        std::hint::spin_loop();
    }

    let total = ITEMS_PER_EPOCH * N_EPOCHS;
    let mut expected = 0u64;
    let mut my_epoch = 0u64;
    let mut my_cap = 64usize;
    let mut ring = AdaptiveRing::open(backing_path(&ring_prefix_str, my_cap, my_epoch), 1, 1, my_cap)
        .map_err(|e| format!("open backing e0: {e:?}"))?;
    ring.register_consumer().map_err(|e| format!("register: {e:?}"))?;

    let mut out = [0u8; 64];
    let t0 = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(120);
    while expected < total {
        match ring.try_recv(0, &mut out) {
            Ok(_) => {
                let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                assert_eq!(seq, expected,
                           "integrity: global FIFO/exactly-once across the morph (epoch {my_epoch})");
                expected += 1;
            }
            Err(_) => {
                if Instant::now() > deadline {
                    return Err(format!("drain stalled at {expected}/{total}").into());
                }
                let (cap, field) = unpack(control.load(Ordering::Acquire));
                // Producer ahead of my epoch (or done, cap==0) means
                // this backing is final - drain it fully, then switch.
                let producer_ahead = cap == 0 || field > my_epoch;
                if producer_ahead {
                    // Confirm truly drained (the producer committed
                    // every push before morphing, so an Empty here is
                    // real), then advance one rung.
                    if ring.try_recv(0, &mut out).is_err() {
                        my_epoch += 1;
                        if my_epoch >= N_EPOCHS {
                            break;
                        }
                        my_cap = 64usize << my_epoch;
                        ring = AdaptiveRing::open(
                            backing_path(&ring_prefix_str, my_cap, my_epoch), 1, 1, my_cap,
                        ).map_err(|e| format!("open backing e{my_epoch}: {e:?}"))?;
                        ring.register_consumer().map_err(|e| format!("register: {e:?}"))?;
                    } else {
                        let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                        assert_eq!(seq, expected, "integrity: global FIFO");
                        expected += 1;
                    }
                } else {
                    std::hint::spin_loop(); // producer still on my epoch
                }
            }
        }
    }
    let elapsed = t0.elapsed();
    child.wait()?;

    assert_eq!(expected, total,
               "integrity: every item delivered exactly once ({expected} of {total})");

    println!();
    println!("delivered {expected} items across {N_EPOCHS} capacity-morph epochs (64..2048),");
    println!("cross-process, EXACTLY ONCE and IN ORDER. {:.1} M items/s end-to-end.",
             expected as f64 / elapsed.as_secs_f64() / 1e6);

    // Cleanup.
    for e in 0..N_EPOCHS {
        cleanup_backing(&ring_prefix_str, 64usize << e, e);
    }
    std::fs::remove_file(&control_path).ok();
    println!("all integrity assertions held across the process boundary");
    Ok(())
}
