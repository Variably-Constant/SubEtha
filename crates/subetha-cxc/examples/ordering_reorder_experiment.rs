//! Experiment: can a consumer-side reorder buffer make the best-effort
//! `MergeByStamp` (merge_tsc) path EXACT while keeping its throughput
//! advantage over `MergeStrict`?
//!
//! Setup mirrors `ordering_modes_compare`: P real producer PROCESSES
//! stream stamped items into a file-backed `AdaptiveRing` in
//! `MergeByStamp` mode; this process drains via
//! `ordered_try_pop_with_stamp`. On a host without invariant TSC the
//! stamps fall back to `SharedCounter` and the merge can emit a smaller
//! stamp late under producer lag (the cross-core reservation-store
//! race). We feed the popped stream through a bounded min-heap reorder
//! buffer of window `W` and count how many emitted stamps still descend
//! (residual inversions) and what the consumer pays per item.
//!
//! W = 0 is the raw merge_tsc baseline (no reorder). For each W the
//! consumer holds up to W popped items in a min-by-stamp heap before
//! emitting the heap minimum; late-arriving lower stamps within W pops
//! are reordered ahead of already-buffered higher ones. If W exceeds
//! the observed reorder distance, residual inversions reach zero.
//!
//! Compare the printed consumer ns/item against the merge_tsc and
//! merge_strict rows from `ordering_modes_compare` on the same host.
//!
//! Run: `cargo run --release --example ordering_reorder_experiment`

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

use subetha_cxc::reorder::ReorderingReceiver;
use subetha_cxc::{AdaptiveRing, OrderingMode, RingShape};

const N_ITEMS_PER_PRODUCER: u64 = 50_000;
const CAPACITY: usize = 16_384;
const WINDOWS: [usize; 6] = [0, 2, 4, 8, 16, 64];

// The reorder buffer under test is the production
// `subetha_cxc::reorder::ReorderBuffer`; its correctness (exact
// delivery for window >= displacement, adaptive growth, hole handling)
// is covered by that module's unit tests. This example measures its
// THROUGHPUT on the real ring against the MergeStrict baseline.

fn tmp_prefix(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "subetha_reorder_{}_{}_{name}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    p
}

fn build_ring(
    prefix: &std::path::Path,
    producers: usize,
    mode: OrderingMode,
) -> Result<AdaptiveRing, String> {
    let ring = AdaptiveRing::create(prefix, producers, 1, CAPACITY)
        .map_err(|e| format!("create: {e:?}"))?;
    let ring = ring.with_ordering_stamps().map_err(|e| format!("stamps: {e:?}"))?;
    ring.morph_to(RingShape::Mpsc).map_err(|e| format!("morph: {e:?}"))?;
    ring.set_ordering_mode(mode).map_err(|e| format!("mode: {e:?}"))?;
    Ok(ring)
}

fn open_ring(prefix: &str, producers: usize) -> Result<AdaptiveRing, String> {
    let ring = AdaptiveRing::open(std::path::Path::new(prefix), producers, 1, CAPACITY)
        .map_err(|e| format!("open: {e:?}"))?;
    // Adopt the creator's stamp kind from the region header, or
    // stamped_try_push has no stamped backing and always errors.
    let ring = ring.with_ordering_stamps().map_err(|e| format!("stamps: {e:?}"))?;
    ring.morph_to(RingShape::Mpsc).map_err(|e| format!("morph: {e:?}"))?;
    Ok(ring)
}

fn run_child(prefix: &str, producer_id: usize, producers: usize) -> Result<(), Box<dyn std::error::Error>> {
    let ring = open_ring(prefix, producers).map_err(|e| e.to_string())?;
    let pin = ring.pin_current_shape();
    let mut payload = [0u8; 16];
    payload[..8].copy_from_slice(&(producer_id as u64).to_le_bytes());
    for seq in 0..N_ITEMS_PER_PRODUCER {
        payload[8..].copy_from_slice(&seq.to_le_bytes());
        while pin.stamped_try_push(producer_id, &payload).is_err() {
            std::hint::spin_loop();
        }
    }
    if ring.stamp_kind().is_some() {
        ring.retire_producer(producer_id).map_err(|e| format!("retire: {e:?}"))?;
    }
    Ok(())
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let self_exe = std::env::current_exe()?;
    let producers = 4usize;
    let total: u64 = producers as u64 * N_ITEMS_PER_PRODUCER;

    println!("=== reorder-buffer experiment: merge_tsc (MergeByStamp) + consumer-side min-heap ===");
    println!("{producers} producers -> 1 consumer, {N_ITEMS_PER_PRODUCER} items each, 16-byte payload");
    let probe = build_ring(&tmp_prefix("probe"), producers, OrderingMode::MergeByStamp)?;
    println!("stamp_kind on this host: {:?}", probe.stamp_kind());
    drop(probe);
    println!();
    println!("| window W | consumer ns/item | residual inversions | ring inversions |");
    println!("|---:|---:|---:|---:|");

    for &w in WINDOWS.iter() {
        let prefix = tmp_prefix(&format!("w{w}"));
        let ring = build_ring(&prefix, producers, OrderingMode::MergeByStamp)?;
        let pin = ring.pin_current_shape();

        let mut children = Vec::new();
        for producer_id in 0..producers {
            children.push(
                Command::new(&self_exe)
                    .arg("--child")
                    .arg(prefix.display().to_string())
                    .arg(producer_id.to_string())
                    .arg(producers.to_string())
                    .stderr(Stdio::inherit())
                    .spawn()?,
            );
        }

        // Consumer: pop best-effort MergeByStamp, feed the min-by-stamp
        // reorder buffer of window w, emit in stamp order, count
        // residual inversions on the EMITTED stream.
        let mut deliver = [0u8; 64];
        // Drive delivery through the ergonomic ReorderingReceiver. Fixed
        // window per row (cap == floor) so this measures the residual-
        // vs-W curve; production uses the adaptive default.
        let mut rx = ReorderingReceiver::with_window(&pin, 0, w, w);
        let mut emitted = 0u64;
        let mut t_first: Option<Instant> = None;
        while emitted < total {
            if rx.try_recv(&mut deliver).is_some() {
                t_first.get_or_insert_with(Instant::now);
                emitted += 1;
            } else if emitted >= total - w as u64 {
                // ring drained to the window tail; flush the rest in order
                while rx.flush(&mut deliver).is_some() {
                    emitted += 1;
                }
            }
        }
        let drain = t_first.expect("at least one delivery").elapsed();
        let residual = rx.corrections();
        assert_eq!(emitted, total, "emitted count mismatch");

        for mut child in children {
            let status = child.wait()?;
            assert!(status.success(), "producer failed: {status:?}");
            let mut s = String::new();
            if let Some(mut so) = child.stdout.take() {
                so.read_to_string(&mut s).ok();
            }
        }

        let ns_per = drain.as_nanos() as f64 / total as f64;
        let ring_inv = ring.inversions();
        let wlabel = if w == 0 { "0 (raw)".to_string() } else { w.to_string() };
        println!("| {wlabel:>8} | {ns_per:>8.1} | {residual:>8} | {ring_inv:>8} |");

        drop(ring);
        for suffix in [".spsc.bin", ".vyukov.bin", ".ordering.bin"]
            .iter()
            .map(|s| s.to_string())
            .chain((0..producers).map(|i| format!(".mpsc.{i}.bin")))
            .chain((0..producers).map(|i| format!(".mpmc.{i}.bin")))
        {
            let mut p = prefix.as_os_str().to_owned();
            p.push(&suffix);
            std::fs::remove_file(PathBuf::from(p)).ok();
        }
    }
    // Apples-to-apples baseline: MergeStrict (exact via the watermark
    // gate) in the SAME harness, straight pop, no reorder buffer - the
    // cost of exactness-by-waiting to compare against the reorder rows.
    {
        let prefix = tmp_prefix("strict");
        let ring = build_ring(&prefix, producers, OrderingMode::MergeStrict)?;
        let pin = ring.pin_current_shape();
        let mut children = Vec::new();
        for producer_id in 0..producers {
            children.push(
                Command::new(&self_exe)
                    .arg("--child")
                    .arg(prefix.display().to_string())
                    .arg(producer_id.to_string())
                    .arg(producers.to_string())
                    .stderr(Stdio::inherit())
                    .spawn()?,
            );
        }
        let mut out = [0u8; 64];
        let mut consumed = 0u64;
        let mut last = 0u64;
        let mut residual = 0u64;
        let mut t_first: Option<Instant> = None;
        while consumed < total {
            match pin.ordered_try_pop_with_stamp(0, &mut out) {
                Ok((_n, stamp)) => {
                    t_first.get_or_insert_with(Instant::now);
                    if stamp < last {
                        residual += 1;
                    }
                    last = stamp;
                    consumed += 1;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
        let drain = t_first.expect("at least one pop").elapsed();
        for mut child in children {
            let status = child.wait()?;
            assert!(status.success(), "producer failed: {status:?}");
        }
        let ns_per = drain.as_nanos() as f64 / total as f64;
        let ring_inv = ring.inversions();
        println!("| {:>8} | {ns_per:>8.1} | {residual:>8} | {ring_inv:>8} |", "STRICT");
        drop(ring);
        for suffix in [".spsc.bin", ".vyukov.bin", ".ordering.bin"]
            .iter()
            .map(|s| s.to_string())
            .chain((0..producers).map(|i| format!(".mpsc.{i}.bin")))
            .chain((0..producers).map(|i| format!(".mpmc.{i}.bin")))
        {
            let mut p = prefix.as_os_str().to_owned();
            p.push(&suffix);
            std::fs::remove_file(PathBuf::from(p)).ok();
        }
    }

    println!();
    println!("W rows = merge_tsc (MergeByStamp) + consumer-side reorder buffer of window W");
    println!("STRICT row = MergeStrict (exact via watermark gate), no reorder, same harness");
    println!("residual = out-of-order stamps AFTER the strategy (must be 0 for correctness)");
    println!("ring inversions = raw merge_tsc inversions the buffer had to correct");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("--child") => run_child(&args[2], args[3].parse()?, args[4].parse()?),
        _ => run_parent(),
    }
}
