//! Cross-process capacity-morph E2E: the producer PROCESS.
//!
//! Drives a real capacity morph across the process boundary. Each
//! "epoch" is a fresh file-backed `AdaptiveRing` at the next ladder
//! capacity; the producer pushes a contiguous range of the global
//! sequence into it, then morphs by creating the next backing and
//! publishing the new `(capacity, epoch)` into a shared-memory
//! control atomic the consumer follows. The consumer (the
//! orchestrator that spawned this process) opens each backing by the
//! deterministic name and drains them in epoch order, so the global
//! sequence is delivered exactly once, in order, ACROSS the morph
//! boundary and ACROSS the process boundary.
//!
//! Args: `<control_path> <ring_prefix> <items_per_epoch> <n_epochs>`
//!
//! The capacity ladder doubles from 64: epoch e uses capacity
//! `64 << e`. The shared control u64 packs `(capacity << 32) | epoch`;
//! `u64::MAX` is the not-ready sentinel and `cap == 0` is the done
//! signal.

use std::sync::atomic::Ordering;

use subetha_cxc::shared_atomic::SharedAtomicU64;
use subetha_cxc::adaptive_ring::AdaptiveRing;

const PAYLOAD: usize = 56;

fn backing_path(prefix: &str, cap: usize, epoch: u64) -> String {
    format!("{prefix}_c{cap}_e{epoch}")
}

fn pack(cap: usize, epoch: u64) -> u64 {
    ((cap as u64) << 32) | epoch
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err(format!(
            "usage: {} <control_path> <ring_prefix> <items_per_epoch> <n_epochs>",
            args[0]
        )
        .into());
    }
    let control_path = &args[1];
    let prefix = &args[2];
    let items_per_epoch: u64 = args[3].parse()?;
    let n_epochs: u64 = args[4].parse()?;

    // Open the control atomic the orchestrator created.
    let control = {
        let mut attempt = 0;
        loop {
            match SharedAtomicU64::open(control_path) {
                Ok(c) => break c,
                Err(_) if attempt < 400 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(e) => return Err(format!("open control: {e:?}").into()),
            }
        }
    };

    let mut global_seq = 0u64;
    let mut payload = [0u8; PAYLOAD];
    for epoch in 0..n_epochs {
        let cap = 64usize << epoch;
        let path = backing_path(prefix, cap, epoch);
        // Create this epoch's backing (file-backed, cross-process
        // visible) and register the single producer.
        let ring = AdaptiveRing::create(&path, 1, 1, cap)
            .map_err(|e| format!("create backing e{epoch}: {e:?}"))?;
        ring.register_producer().map_err(|e| format!("register: {e:?}"))?;
        // Publish: this backing is now the current one - the morph
        // is observable to the consumer the instant this store lands.
        control.store(pack(cap, epoch), Ordering::Release);

        for _ in 0..items_per_epoch {
            payload[..8].copy_from_slice(&global_seq.to_le_bytes());
            while ring.try_send(0, &payload).is_err() {
                std::hint::spin_loop();
            }
            global_seq += 1;
        }
        // The next iteration's create + control store IS the morph;
        // the producer never pushes to this backing again, so the
        // consumer can drain it to completion and switch.
    }

    // Done signal: cap == 0.
    control.store(pack(0, n_epochs), Ordering::Release);
    println!("producer: pushed {global_seq} items across {n_epochs} morph epochs");
    Ok(())
}
