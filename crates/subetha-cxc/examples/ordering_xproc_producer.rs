//! Cross-process ordering E2E: one producer PROCESS.
//!
//! Attaches to a stamped file-backed `AdaptiveRing` another process
//! created (`ordering_xproc_consumer` is the normal orchestrator,
//! which spawns several of these), morphs its process-local shape
//! view to MPSC, and streams `n_items` stamped payloads through its
//! own producer ring as fast as the ring accepts them.
//!
//! Payload layout: `[producer_id; 8][seq; 8]` - the consumer uses
//! the pair to assert per-producer FIFO and zero loss, and the
//! stamps (prepended transparently by the stamped push) to assert
//! global order once the merge flag flips.
//!
//! Args: `<path_prefix> <producer_id> <n_items> <max_producers>`
//!
//! Standalone run (after a consumer created the ring):
//!     cargo run --release --example ordering_xproc_producer -- \
//!         /tmp/ordering_e2e 0 100000 4

use std::time::Instant;

use subetha_cxc::{AdaptiveRing, RingShape};

const CAPACITY: usize = 16384;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err(format!(
            "usage: {} <path_prefix> <producer_id> <n_items> <max_producers>",
            args[0],
        ).into());
    }
    let prefix = &args[1];
    let producer_id: usize = args[2].parse()?;
    let n_items: u64 = args[3].parse()?;
    let max_producers: usize = args[4].parse()?;

    // Attach: validates the ring backings AND the ordering region
    // (magic checked, stamp kind adopted from the creator, nothing
    // re-initialised).
    let ring = AdaptiveRing::open(prefix, max_producers, 1, CAPACITY)
        .map_err(|e| format!("open ring: {e:?}"))?
        .with_ordering_stamps()
        .map_err(|e| format!("open ordering region: {e:?}"))?;
    // The shape tag is process-local; mirror the consumer's MPSC
    // view so the pinned push runs the same backing.
    ring.morph_to(RingShape::Mpsc).map_err(|e| format!("morph: {e:?}"))?;
    let pin = ring.pin_current_shape();

    let mut payload = [0u8; 16];
    payload[..8].copy_from_slice(&(producer_id as u64).to_le_bytes());

    let t0 = Instant::now();
    for seq in 0..n_items {
        payload[8..].copy_from_slice(&seq.to_le_bytes());
        while pin.stamped_try_push(producer_id, &payload).is_err() {
            std::hint::spin_loop();
        }
    }
    let elapsed = t0.elapsed();
    // Clean exit: retire the producer slot so strict-merge
    // consumers never wait on this process's silence.
    ring.retire_producer(producer_id).map_err(|e| format!("retire: {e:?}"))?;

    // Machine-readable line the orchestrator parses for the
    // producer-side push rate (includes backpressure spin time).
    println!(
        "producer {} pushed {} items in {:?} ({:.1} ns/push, stamp_kind {:?})",
        producer_id,
        n_items,
        elapsed,
        elapsed.as_nanos() as f64 / n_items as f64,
        ring.stamp_kind().expect("stamped ring"),
    );
    Ok(())
}
