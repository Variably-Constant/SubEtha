//! Cross-process bench: the full ordering mode ladder.
//!
//! Six contenders, each a file-backed `AdaptiveRing` drained by this
//! process while P real producer PROCESSES stream into it:
//!
//! | contender | substrate | ordering guarantee | consumer asserts |
//! |---|---|---|---|
//! | `composed` | unstamped MPSC | per-producer FIFO | per-producer seq monotone |
//! | `stamped` | stamps, mode Unordered | per-producer FIFO + inversion metric | per-producer seq monotone; inversions reported |
//! | `merge_tsc` | stamps, MergeByStamp | global FIFO within stamp skew | best-effort: merged-order inversions reported, not asserted |
//! | `merge_strict` | stamps, MergeStrict | exact global FIFO (in-flight gate) | non-decreasing stamps + zero inversions |
//! | `merge_exact` | SharedCounter stamps, MergeStrict | exact global FIFO, total order | STRICTLY increasing stamps + zero inversions |
//! | `vyukov` | unstamped Vyukov shape | exact global FIFO (CAS) | per-producer seq monotone |
//!
//! Bench-audit notes (each contender, per the audit discipline):
//! - every contender invokes its PINNED production hot path
//!   (`mpsc_try_pop` / `ordered_try_pop_with_stamp` /
//!   `vyukov_try_pop`; pushes mirror it) - no shortcut paths;
//! - all contenders move the same 16-byte `[producer; 8][seq; 8]`
//!   payload through the same 16384-slot rings sized for the same
//!   P-producer / 1-consumer workload; the stamped rows add only
//!   the 8-byte stamp the mode requires;
//! - the consumer-side check IS the feature: a contender with an EXACT
//!   guarantee that cannot uphold it fails the run (non-zero exit, its
//!   child producers killed first so none is orphaned) instead of
//!   posting a number. Best-effort modes (`merge_tsc`, "within stamp
//!   skew") report their merged-order inversion count rather than
//!   asserting zero.
//!
//! Reported per row: consumer-side ns/item (drain throughput, the
//! "consumers pay" column of the mode ladder) and the mean
//! producer-side ns/push from the child processes (the "producers
//! pay" column; the stamped-vs-composed delta is the stamp
//! overhead). Both 1P/1C and 4P/1C sweeps run by default.
//!
//! Output: `docs/ordering_modes_results.json` (the committed chart
//! pair is rendered from this JSON).
//!
//! Run: `cargo run --release --example ordering_modes_compare`

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use subetha_cxc::{
    AdaptiveRing, OrderingMode, PinnedRing, RingShape, StampKind,
};

const N_ITEMS_PER_PRODUCER: u64 = 50_000;
const CAPACITY: usize = 16384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Contender {
    Composed,
    Stamped,
    MergeTsc,
    MergeStrict,
    MergeExact,
    Vyukov,
}

impl Contender {
    const ALL: [Contender; 6] = [
        Contender::Composed,
        Contender::Stamped,
        Contender::MergeTsc,
        Contender::MergeStrict,
        Contender::MergeExact,
        Contender::Vyukov,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Composed => "composed",
            Self::Stamped => "stamped",
            Self::MergeTsc => "merge_tsc",
            Self::MergeStrict => "merge_strict",
            Self::MergeExact => "merge_exact",
            Self::Vyukov => "vyukov",
        }
    }

    fn from_name(name: &str) -> Self {
        Self::ALL.into_iter()
            .find(|c| c.name() == name)
            .unwrap_or_else(|| panic!("unknown contender: {name}"))
    }

    fn stamped(self) -> bool {
        matches!(self, Self::Stamped | Self::MergeTsc | Self::MergeStrict | Self::MergeExact)
    }

    /// Explicit stamp kind override; `None` = the host default
    /// (invariant-TSC rdtsc on this hardware).
    fn stamp_kind(self) -> Option<StampKind> {
        match self {
            Self::MergeExact => Some(StampKind::SharedCounter),
            _ => None,
        }
    }

    fn mode(self) -> OrderingMode {
        match self {
            Self::MergeTsc => OrderingMode::MergeByStamp,
            Self::MergeStrict | Self::MergeExact => OrderingMode::MergeStrict,
            _ => OrderingMode::Unordered,
        }
    }

    fn shape(self) -> RingShape {
        match self {
            Self::Vyukov => RingShape::Vyukov,
            _ => RingShape::Mpsc,
        }
    }

    fn guarantee(self) -> &'static str {
        match self {
            Self::Composed => "per-producer FIFO",
            Self::Stamped => "per-producer FIFO + inversion metric",
            Self::MergeTsc => "global FIFO within stamp skew",
            Self::MergeStrict => "exact global FIFO (in-flight gate)",
            Self::MergeExact => "exact global FIFO, total stamp order",
            Self::Vyukov => "exact global FIFO (shared CAS)",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RunResult {
    name: String,
    producers: usize,
    items_total: u64,
    consumer_ns_per_item: f64,
    producer_ns_per_push_mean: f64,
    inversions: u64,
    stamp_kind: String,
    guarantee: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Report {
    machine: String,
    timestamp: u64,
    payload_bytes: u32,
    capacity: u32,
    items_per_producer: u64,
    runs: Vec<RunResult>,
}

fn tmp_prefix(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "subetha_ordcmp_{}_{}_{name}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    p
}

fn build_ring(
    contender: Contender,
    prefix: &std::path::Path,
    producers: usize,
) -> Result<AdaptiveRing, String> {
    let ring = AdaptiveRing::create(prefix, producers, 1, CAPACITY)
        .map_err(|e| format!("create: {e:?}"))?;
    let ring = if contender.stamped() {
        match contender.stamp_kind() {
            Some(kind) => ring.with_ordering_stamps_kind(kind),
            None => ring.with_ordering_stamps(),
        }
        .map_err(|e| format!("stamps: {e:?}"))?
    } else {
        ring
    };
    ring.morph_to(contender.shape()).map_err(|e| format!("morph: {e:?}"))?;
    if contender.stamped() {
        ring.set_ordering_mode(contender.mode())
            .map_err(|e| format!("mode: {e:?}"))?;
    }
    Ok(ring)
}

fn open_ring(
    contender: Contender,
    prefix: &str,
    producers: usize,
) -> Result<AdaptiveRing, String> {
    let ring = AdaptiveRing::open(prefix, producers, 1, CAPACITY)
        .map_err(|e| format!("open: {e:?}"))?;
    let ring = if contender.stamped() {
        // Adopts the creator's stamp kind from the region header.
        ring.with_ordering_stamps().map_err(|e| format!("stamps: {e:?}"))?
    } else {
        ring
    };
    ring.morph_to(contender.shape()).map_err(|e| format!("morph: {e:?}"))?;
    Ok(ring)
}

#[inline]
fn pinned_push(
    pin: &PinnedRing<'_>,
    contender: Contender,
    producer_id: usize,
    payload: &[u8],
) -> bool {
    let result = if contender.stamped() {
        pin.stamped_try_push(producer_id, payload)
    } else if contender == Contender::Vyukov {
        pin.vyukov_try_push(payload)
    } else {
        pin.mpsc_try_push(producer_id, payload)
    };
    result.is_ok()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("--child") {
        return run_child(&args[2], &args[3], args[4].parse()?, args[5].parse()?);
    }
    run_parent()
}

fn run_child(
    contender_name: &str,
    prefix: &str,
    producer_id: usize,
    producers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let contender = Contender::from_name(contender_name);
    let ring = open_ring(contender, prefix, producers)?;
    let pin = ring.pin_current_shape();

    let mut payload = [0u8; 16];
    payload[..8].copy_from_slice(&(producer_id as u64).to_le_bytes());
    let t0 = Instant::now();
    for seq in 0..N_ITEMS_PER_PRODUCER {
        payload[8..].copy_from_slice(&seq.to_le_bytes());
        while !pinned_push(&pin, contender, producer_id, &payload) {
            std::hint::spin_loop();
        }
    }
    let elapsed = t0.elapsed();
    // Clean exit: retire the producer slot so MergeStrict consumers
    // stop waiting on this process's silence.
    if contender.stamped() {
        ring.retire_producer(producer_id).map_err(|e| format!("retire: {e:?}"))?;
    }
    // Machine-readable producer-side rate (the parent averages
    // these). Includes backpressure spin time by design: that IS
    // the producer-side cost of the mode under a saturating stream.
    println!("{:.2}", elapsed.as_nanos() as f64 / N_ITEMS_PER_PRODUCER as f64);
    Ok(())
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ordering mode ladder: cross-process bench ===");
    println!("{N_ITEMS_PER_PRODUCER} items/producer, 16-byte payload, {CAPACITY}-slot rings");
    println!();

    let self_exe = std::env::current_exe()?;
    let mut runs: Vec<RunResult> = Vec::new();

    for producers in [1usize, 4] {
        println!("--- {producers}P / 1C ---");
        for contender in Contender::ALL {
            let total_items = producers as u64 * N_ITEMS_PER_PRODUCER;
            let prefix = tmp_prefix(&format!("{}_{producers}p", contender.name()));
            let ring = build_ring(contender, &prefix, producers)?;
            let pin = ring.pin_current_shape();

            let mut children = Vec::new();
            for producer_id in 0..producers {
                children.push(
                    Command::new(&self_exe)
                        .arg("--child")
                        .arg(contender.name())
                        .arg(prefix.display().to_string())
                        .arg(producer_id.to_string())
                        .arg(producers.to_string())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::inherit())
                        .spawn()?,
                );
            }

            // ----- drain + per-contender assertion -----
            let mut out = [0u8; 64];
            let mut consumed = 0u64;
            let mut last_seq = vec![-1i64; producers];
            let mut last_stamp = 0u64;
            let mut merged_inversions = 0u64;
            let mut t_first_pop: Option<Instant> = None;
            while consumed < total_items {
                let popped_stamp: Option<u64> = if contender.stamped() {
                    match pin.ordered_try_pop_with_stamp(0, &mut out) {
                        Ok((_n, stamp)) => Some(stamp),
                        Err(_) => {
                            std::hint::spin_loop();
                            continue;
                        }
                    }
                } else {
                    let popped = if contender == Contender::Vyukov {
                        pin.vyukov_try_pop(&mut out)
                    } else {
                        pin.mpsc_try_pop(&mut out)
                    };
                    match popped {
                        Ok(_) => None,
                        Err(_) => {
                            std::hint::spin_loop();
                            continue;
                        }
                    }
                };
                t_first_pop.get_or_insert_with(Instant::now);
                consumed += 1;

                // Universal assertion: per-producer FIFO.
                let producer =
                    u64::from_le_bytes(out[..8].try_into().unwrap()) as usize;
                let seq = u64::from_le_bytes(out[8..16].try_into().unwrap()) as i64;
                assert!(seq > last_seq[producer],
                        "[{}] per-producer FIFO violated: producer {producer} seq {seq} after {}",
                        contender.name(), last_seq[producer]);
                last_seq[producer] = seq;

                // Mode-specific check: each contender is held to the
                // guarantee it actually claims, not a stronger one.
                if let Some(stamp) = popped_stamp {
                    match contender.mode() {
                        OrderingMode::Unordered => {}
                        // MergeStrict claims EXACT global FIFO. Any
                        // out-of-order stamp is a real violation: kill
                        // the producer children first so the early
                        // return cannot orphan a spinning process, then
                        // fail the run with a non-zero exit.
                        OrderingMode::MergeStrict => {
                            let ordered = if contender == Contender::MergeExact {
                                stamp > last_stamp
                            } else {
                                stamp >= last_stamp
                            };
                            if !ordered {
                                for child in &mut children {
                                    child.kill().ok();
                                }
                                return Err(format!(
                                    "[{}] exact global FIFO violated: {last_stamp} -> {stamp}",
                                    contender.name(),
                                ).into());
                            }
                            last_stamp = stamp;
                        }
                        // MergeByStamp claims only "global FIFO within
                        // stamp skew". Without the freshness guard (time
                        // stamps) or the watermark gate (MergeStrict), a
                        // lagging producer can make the k-way merge emit
                        // a smaller stamp late - this is within the
                        // documented contract, so count it as the skew
                        // metric rather than failing the run.
                        OrderingMode::MergeByStamp => {
                            if stamp < last_stamp {
                                merged_inversions += 1;
                            }
                            last_stamp = stamp;
                        }
                    }
                }
            }
            let drain_elapsed = t_first_pop
                .expect("at least one pop happened")
                .elapsed();

            let mut producer_rates = Vec::new();
            for mut child in children {
                let status = child.wait()?;
                assert!(status.success(),
                        "[{}] producer process failed: {status:?}", contender.name());
                let mut s = String::new();
                child.stdout.take().expect("piped stdout").read_to_string(&mut s)?;
                producer_rates.push(s.trim().parse::<f64>()?);
            }

            let inversions = ring.inversions();
            // Only MergeStrict promises zero inversions. MergeByStamp
            // (merge_tsc) is best-effort "within stamp skew": report the
            // count, do not fail the run.
            if contender.mode() == OrderingMode::MergeStrict {
                assert_eq!(inversions, 0,
                           "[{}] exact global FIFO must observe zero inversions",
                           contender.name());
            }
            if merged_inversions > 0 {
                println!(
                    "  {:<13} note: {merged_inversions} merged-order inversion(s) within stamp skew (best-effort mode)",
                    contender.name(),
                );
            }

            let consumer_ns = drain_elapsed.as_nanos() as f64 / total_items as f64;
            let producer_ns =
                producer_rates.iter().sum::<f64>() / producer_rates.len() as f64;
            println!(
                "  {:<13} {:>8.1} ns/item consume   {:>8.1} ns/push produce   inversions {}",
                contender.name(), consumer_ns, producer_ns, inversions,
            );
            runs.push(RunResult {
                name: contender.name().to_string(),
                producers,
                items_total: total_items,
                consumer_ns_per_item: consumer_ns,
                producer_ns_per_push_mean: producer_ns,
                inversions,
                stamp_kind: ring.stamp_kind()
                    .map(|k| format!("{k:?}"))
                    .unwrap_or_else(|| "none".to_string()),
                guarantee: contender.guarantee().to_string(),
            });

            drop(ring);
            for suffix in std::iter::once(".spsc.bin".to_string())
                .chain(std::iter::once(".vyukov.bin".to_string()))
                .chain(std::iter::once(".ordering.bin".to_string()))
                .chain((0..producers).map(|i| format!(".mpsc.{i}.bin")))
                .chain((0..producers).map(|i| format!(".mpmc.{i}.bin")))
            {
                let mut p = prefix.as_os_str().to_owned();
                p.push(&suffix);
                std::fs::remove_file(PathBuf::from(p)).ok();
            }
        }
        println!();
    }

    let report = Report {
        machine: format!("{} ({})", std::env::consts::OS, std::env::consts::ARCH),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        payload_bytes: 16,
        capacity: CAPACITY as u32,
        items_per_producer: N_ITEMS_PER_PRODUCER,
        runs,
    };
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let docs_dir = std::path::Path::new(manifest_dir)
        .parent().ok_or("no crates dir")?
        .parent().ok_or("no workspace dir")?
        .join("docs");
    std::fs::create_dir_all(&docs_dir).ok();
    let json_path = docs_dir.join("ordering_modes_results.json");
    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(&json_path, &json)?;
    println!("[json] wrote {} ({} bytes)", json_path.display(), json.len());
    Ok(())
}
