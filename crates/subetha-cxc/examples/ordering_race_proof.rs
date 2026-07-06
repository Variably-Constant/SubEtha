//! Proof harness: does the `MergeByStamp` merge with `SharedCounter`
//! stamps actually deliver items out of stamp order under multiple
//! producers, and does `MergeStrict` not?
//!
//! This forces the stamp kind to `SharedCounter` explicitly (so it does
//! not depend on the host lacking an invariant TSC) and runs P real
//! producer PROCESSES into one stamped ring. The consumer pops via the
//! production `ordered_try_pop_with_stamp` and reports TWO independent
//! inversion counts for each mode:
//!
//!   - `ring.inversions()`  - the LIBRARY's own counter, bumped inside
//!     `note_stamp`/`record_inversion` whenever a merged pop's stamp is
//!     below the previous one. Not the harness's measurement.
//!   - observed here        - the harness re-checks `stamp < last` on the
//!     delivered stream and captures example (higher -> lower) pairs.
//!
//! Expected: MergeByStamp shows inversions > 0 (both counts agree),
//! MergeStrict shows 0 - same host, same workload, only the mode
//! differs. That contrast isolates the defect to the non-strict merge.
//!
//! Run: `cargo run --release --example ordering_race_proof`

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use subetha_cxc::reorder::AdaptiveOrderedReceiver;
use subetha_cxc::{AdaptiveRing, OrderingMode, RingShape, StampKind};

const N_ITEMS_PER_PRODUCER: u64 = 50_000;
const CAPACITY: usize = 16_384;
const PRODUCERS: usize = 4;

fn tmp_prefix(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "subetha_raceproof_{}_{}_{name}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    p
}

// Force SharedCounter so the config under test is identical on every
// host (invariant-TSC or not).
fn build_ring(prefix: &std::path::Path, mode: OrderingMode) -> Result<AdaptiveRing, String> {
    let ring = AdaptiveRing::create(prefix, PRODUCERS, 1, CAPACITY)
        .map_err(|e| format!("create: {e:?}"))?;
    let ring = ring
        .with_ordering_stamps_kind(StampKind::SharedCounter)
        .map_err(|e| format!("stamps: {e:?}"))?;
    ring.morph_to(RingShape::Mpsc).map_err(|e| format!("morph: {e:?}"))?;
    ring.set_ordering_mode(mode).map_err(|e| format!("mode: {e:?}"))?;
    Ok(ring)
}

fn open_ring(prefix: &str) -> Result<AdaptiveRing, String> {
    let ring = AdaptiveRing::open(std::path::Path::new(prefix), PRODUCERS, 1, CAPACITY)
        .map_err(|e| format!("open: {e:?}"))?;
    let ring = ring
        .with_ordering_stamps_kind(StampKind::SharedCounter)
        .map_err(|e| format!("stamps: {e:?}"))?;
    ring.morph_to(RingShape::Mpsc).map_err(|e| format!("morph: {e:?}"))?;
    Ok(ring)
}

fn run_child(prefix: &str, producer_id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let ring = open_ring(prefix).map_err(|e| e.to_string())?;
    let pin = ring.pin_current_shape();
    let mut payload = [0u8; 16];
    payload[..8].copy_from_slice(&(producer_id as u64).to_le_bytes());
    for seq in 0..N_ITEMS_PER_PRODUCER {
        payload[8..].copy_from_slice(&seq.to_le_bytes());
        while pin.stamped_try_push(producer_id, &payload).is_err() {
            std::hint::spin_loop();
        }
    }
    ring.retire_producer(producer_id).map_err(|e| format!("retire: {e:?}"))?;
    Ok(())
}

fn run_mode(self_exe: &std::path::Path, mode: OrderingMode, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    let total = PRODUCERS as u64 * N_ITEMS_PER_PRODUCER;
    let prefix = tmp_prefix(label);
    let ring = build_ring(&prefix, mode)?;
    let pin = ring.pin_current_shape();

    let mut children = Vec::new();
    for producer_id in 0..PRODUCERS {
        children.push(
            Command::new(self_exe)
                .arg("--child")
                .arg(prefix.display().to_string())
                .arg(producer_id.to_string())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
    }

    let mut out = [0u8; 64];
    let mut consumed = 0u64;
    let mut last_stamp = 0u64;
    let mut have_last = false;
    let mut observed_inversions = 0u64;
    let mut examples: Vec<(u64, u64)> = Vec::new();
    while consumed < total {
        match pin.ordered_try_pop_with_stamp(0, &mut out) {
            Ok((_n, stamp)) => {
                if have_last && stamp < last_stamp {
                    observed_inversions += 1;
                    if examples.len() < 5 {
                        examples.push((last_stamp, stamp));
                    }
                }
                last_stamp = stamp;
                have_last = true;
                consumed += 1;
            }
            Err(_) => std::hint::spin_loop(),
        }
    }
    for mut child in children {
        let status = child.wait()?;
        assert!(status.success(), "producer failed: {status:?}");
        if let Some(mut so) = child.stdout.take() {
            let mut s = String::new();
            so.read_to_string(&mut s).ok();
        }
    }

    let lib = ring.inversions();
    println!(
        "{label:>12} (SharedCounter): library ring.inversions()={lib}  harness-observed={observed_inversions}"
    );
    if !examples.is_empty() {
        let shown: Vec<String> = examples.iter().map(|(h, l)| format!("{h}->{l}")).collect();
        println!("             out-of-order stamp pairs (higher delivered before lower): {}", shown.join(", "));
    }

    drop(ring);
    for suffix in [".spsc.bin", ".vyukov.bin", ".ordering.bin"]
        .iter()
        .map(|s| s.to_string())
        .chain((0..PRODUCERS).map(|i| format!(".mpsc.{i}.bin")))
        .chain((0..PRODUCERS).map(|i| format!(".mpmc.{i}.bin")))
    {
        let mut p = prefix.as_os_str().to_owned();
        p.push(&suffix);
        std::fs::remove_file(PathBuf::from(p)).ok();
    }
    Ok(())
}

// The AUTOMATIC path: AdaptiveOrderedReceiver auto-selects the exact
// strategy (reorder buffer for this producer count) and must deliver
// zero out-of-order items.
fn run_adaptive(self_exe: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let total = PRODUCERS as u64 * N_ITEMS_PER_PRODUCER;
    let prefix = tmp_prefix("adaptive");
    let ring = build_ring(&prefix, OrderingMode::MergeByStamp)?;
    let mut rx = AdaptiveOrderedReceiver::new(&ring, 0);
    let strategy = rx.strategy();

    let mut children = Vec::new();
    for producer_id in 0..PRODUCERS {
        children.push(
            Command::new(self_exe)
                .arg("--child")
                .arg(prefix.display().to_string())
                .arg(producer_id.to_string())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
    }

    let mut out = [0u8; 64];
    let mut delivered = 0u64;
    let mut last = 0u64;
    let mut have = false;
    let mut inv = 0u64;
    let mut idle = 0u64;
    while delivered < total {
        match rx.try_recv(&mut out) {
            Some((_n, stamp)) => {
                if have && stamp < last {
                    inv += 1;
                }
                last = stamp;
                have = true;
                delivered += 1;
                idle = 0;
            }
            None => {
                idle += 1;
                if idle > 100_000 {
                    // ring drained: release the buffered tail in order
                    if let Some((_n, stamp)) = rx.flush(&mut out) {
                        if have && stamp < last {
                            inv += 1;
                        }
                        last = stamp;
                        have = true;
                        delivered += 1;
                    }
                    idle = 0;
                }
            }
        }
    }
    for mut child in children {
        let status = child.wait()?;
        assert!(status.success(), "producer failed: {status:?}");
    }
    let corr = rx.corrections();
    println!(
        "AdaptiveOrderedReceiver: strategy={strategy}  delivered={delivered}  out-of-order={inv}  window-grows={corr}"
    );
    drop(ring);
    for suffix in [".spsc.bin", ".vyukov.bin", ".ordering.bin"]
        .iter()
        .map(|s| s.to_string())
        .chain((0..PRODUCERS).map(|i| format!(".mpsc.{i}.bin")))
        .chain((0..PRODUCERS).map(|i| format!(".mpmc.{i}.bin")))
    {
        let mut p = prefix.as_os_str().to_owned();
        p.push(&suffix);
        std::fs::remove_file(PathBuf::from(p)).ok();
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("--child") {
        return run_child(&args[2], args[3].parse()?);
    }
    let self_exe = std::env::current_exe()?;
    println!("=== ordering race proof: SharedCounter stamps, {PRODUCERS} producers -> 1 consumer ===");
    println!("{N_ITEMS_PER_PRODUCER} items/producer; both counts should be 0 for an exact mode.\n");
    let rounds = 6;
    for r in 1..=rounds {
        println!("--- round {r}/{rounds} ---");
        run_mode(&self_exe, OrderingMode::MergeByStamp, "MergeByStamp")?;
        run_mode(&self_exe, OrderingMode::MergeStrict, "MergeStrict")?;
        run_adaptive(&self_exe)?;
    }
    println!("\nMergeByStamp inversions > 0 (library counter AND harness agree) = real out-of-order delivery.");
    println!("MergeStrict = 0 across all rounds = the watermark gate is exact. Same host, same workload.");
    Ok(())
}
