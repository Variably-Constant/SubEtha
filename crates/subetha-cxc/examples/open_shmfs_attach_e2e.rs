//! Multi-process proof that `AdaptiveRing::open_shmfs` attaches to a
//! populated shared-memory region WITHOUT wiping it - the capability a
//! late-joining worker needs and that `create_shmfs` cannot provide
//! (it re-lays-out every backing, zeroing whatever the creator enqueued).
//!
//! The parent process creates the region, enqueues a known snapshot of
//! N items, then spawns a worker process that attaches and must pop the
//! same N items back in order. A `--buggy` worker instead calls
//! `create_shmfs` (the wrong tool for a late attach) and is expected to
//! find the region wiped - the negative control that makes the fix
//! visible.
//!
//! Run (the parent spawns the worker itself):
//!   cargo run --release -p subetha-cxc --example open_shmfs_attach_e2e
//!   cargo run --release -p subetha-cxc --example open_shmfs_attach_e2e -- --buggy

use std::error::Error;
use subetha_cxc::adaptive_ring::AdaptiveRing;

type BoxErr = Box<dyn Error + Send + Sync>;

const CAPACITY: usize = 1024;
const N_ITEMS: u64 = 500;

fn main() -> Result<(), BoxErr> {
    let argv: Vec<String> = std::env::args().collect();

    // Worker role: the parent re-spawns this same binary with "worker".
    if let Some(pos) = argv.iter().position(|a| a == "worker") {
        let name = argv.get(pos + 1).cloned().unwrap_or_default();
        let buggy = argv.iter().any(|a| a == "--buggy");
        return worker(&name, buggy);
    }

    // Parent role.
    let buggy = argv.iter().any(|a| a == "--buggy");
    let name = format!("subetha_open_shmfs_e2e_{}", std::process::id());

    // Create the region and enqueue the snapshot the worker must recover.
    let ring = AdaptiveRing::create_shmfs(&name, 1, 1, CAPACITY)
        .map_err(|e| format!("create_shmfs: {e:?}"))?;
    {
        let pin = ring.pin_current_shape();
        for seq in 0..N_ITEMS {
            pin.spsc_try_push(&seq.to_le_bytes())
                .map_err(|e| format!("parent push {seq}: {e:?}"))?;
        }
    }
    println!("parent: enqueued {N_ITEMS} items into shmfs region '{name}'");

    // Spawn the worker, holding `ring` alive so the region stays mapped.
    let self_exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(&self_exe);
    cmd.arg("worker").arg(&name);
    if buggy {
        cmd.arg("--buggy");
    }
    let status = cmd.status()?;
    drop(ring); // release the region only after the worker has finished

    let ok = status.success();
    println!(
        "\nRESULT open_shmfs_attach: worker_exit={} mode={} -> {}",
        status.code().unwrap_or(-1),
        if buggy { "buggy(create_shmfs)" } else { "open_shmfs" },
        if buggy {
            // In buggy mode we EXPECT the worker to find the region wiped,
            // so a worker failure is the demonstrated data loss.
            if ok { "UNEXPECTED PASS (buggy worker saw data?!)" } else { "PASS: create_shmfs wiped the snapshot, as the fix documents" }
        } else if ok {
            "PASS: open_shmfs attached and recovered every item"
        } else {
            "FAIL: open_shmfs lost data"
        }
    );

    // Exit non-zero only when the real (open_shmfs) path fails.
    if !buggy && !ok {
        std::process::exit(1);
    }
    Ok(())
}

fn worker(name: &str, buggy: bool) -> Result<(), BoxErr> {
    // The whole point: attach with open_shmfs (no re-init). The --buggy
    // control uses create_shmfs, which re-lays-out the region.
    let ring = if buggy {
        AdaptiveRing::create_shmfs(name, 1, 1, CAPACITY)
            .map_err(|e| format!("worker create_shmfs: {e:?}"))?
    } else {
        AdaptiveRing::open_shmfs(name, 1, 1, CAPACITY)
            .map_err(|e| format!("worker open_shmfs: {e:?}"))?
    };
    let pin = ring.pin_current_shape();

    let mut got = 0u64;
    let mut buf = [0u8; 64];
    for expect in 0..N_ITEMS {
        match pin.spsc_try_pop(&mut buf) {
            Ok(n) if n >= 8 => {
                let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                if seq != expect {
                    eprintln!("worker: order/value mismatch at {expect}: got {seq}");
                    std::process::exit(1);
                }
                got += 1;
            }
            _ => break, // ring empty (wiped, or drained early)
        }
    }

    if got == N_ITEMS {
        println!("worker: recovered all {got} items in order (attach preserved the snapshot)");
        Ok(())
    } else {
        eprintln!("worker: recovered only {got}/{N_ITEMS} items (region was re-initialised / wiped)");
        std::process::exit(1);
    }
}
