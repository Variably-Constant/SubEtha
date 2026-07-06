//! Cross-process ordering E2E: the consumer / orchestrator process.
//!
//! Creates a STAMPED file-backed `AdaptiveRing`, spawns N
//! `ordering_xproc_producer` PROCESSES (real processes, not
//! threads), and drains their concurrent streams in two phases:
//!
//! 1. **Unordered** (the composed default): per-producer FIFO only.
//!    The consumer counts cross-producer inversions through the
//!    shared header counter and reports the rate - the runtime
//!    signal that makes the invisible ordering property observable.
//!    With N concurrent producer processes this phase MUST observe
//!    inversions (asserted).
//! 2. Mid-traffic the consumer flips the MMF-resident ordering flag
//!    to `MergeByStamp` - one Release store, no drain, no data
//!    movement, in-flight backlog retroactively ordered - and keeps
//!    draining. From the flip point the consumer asserts MONOTONE
//!    STAMPS on every pop and zero new inversions.
//!
//! Across both phases: zero items lost (every producer's full
//! sequence accounted for) and per-producer FIFO never violated.
//!
//! Pass `--no-flip` to stay in the unordered mode for the whole run
//! (the detection-layer measurement: inversion rate under the
//! composed interleave, no ordering claim).
//!
//! Args: `[--producers N] [--items M] [--no-flip]`
//!
//! Run (builds both binaries first):
//!     cargo build --release --example ordering_xproc_producer
//!     cargo run --release --example ordering_xproc_consumer
//!
//! Works identically on Windows and Linux/WSL: the data path is
//! pure MMF + atomics, and the ordering flag lives in the shared
//! region rather than process-local state.

use std::process::{Child, Command, Stdio};
use std::time::Instant;

use subetha_cxc::{AdaptiveRing, OrderingMode, RingShape, STAMPED_PAYLOAD_BYTES};

const CAPACITY: usize = 16384;

struct PerProducer {
    last_seq: i64,
    count: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut producers = 4usize;
    let mut items_per_producer = 100_000u64;
    let mut flip = true;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--producers" => {
                producers = args.get(i + 1).ok_or("--producers needs a value")?.parse()?;
                i += 2;
            }
            "--items" => {
                items_per_producer =
                    args.get(i + 1).ok_or("--items needs a value")?.parse()?;
                i += 2;
            }
            "--no-flip" => {
                flip = false;
                i += 1;
            }
            other => return Err(format!("unknown arg: {other}").into()),
        }
    }
    let total_items = producers as u64 * items_per_producer;

    println!("=== Cross-process ordering E2E ===");
    println!("{producers} producer PROCESSES x {items_per_producer} items; flip mid-traffic: {flip}");
    println!();

    // Create the stamped ring; producers attach by prefix.
    let prefix = std::env::temp_dir().join(format!(
        "subetha_ordering_e2e_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    let ring = AdaptiveRing::create(&prefix, producers, 1, CAPACITY)
        .map_err(|e| format!("create: {e:?}"))?
        .with_ordering_stamps()
        .map_err(|e| format!("ordering: {e:?}"))?;
    ring.morph_to(RingShape::Mpsc).map_err(|e| format!("morph: {e:?}"))?;
    println!("[setup] stamped ring at {} (stamp kind {:?})",
             prefix.display(), ring.stamp_kind().expect("stamped"));

    // The producer binary sits next to this one in the examples
    // output dir.
    let self_exe = std::env::current_exe()?;
    let producer_exe = self_exe
        .parent()
        .ok_or("no parent dir")?
        .join(format!("ordering_xproc_producer{}", std::env::consts::EXE_SUFFIX));
    if !producer_exe.exists() {
        return Err(format!(
            "{} not built; run `cargo build --release --example ordering_xproc_producer` first",
            producer_exe.display(),
        ).into());
    }

    let mut children: Vec<Child> = Vec::new();
    for producer_id in 0..producers {
        children.push(
            Command::new(&producer_exe)
                .arg(prefix.display().to_string())
                .arg(producer_id.to_string())
                .arg(items_per_producer.to_string())
                .arg(producers.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
    }
    println!("[spawn] {} producer processes attached", children.len());

    // ----- drain -----
    let pin = ring.pin_current_shape();
    let mut stats: Vec<PerProducer> = (0..producers)
        .map(|_| PerProducer { last_seq: -1, count: 0 })
        .collect();
    let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
    let mut consumed = 0u64;
    let flip_at = if flip { total_items / 2 } else { u64::MAX };
    let mut flipped_at_inversions = None;
    let mut post_flip_last_stamp = 0u64;
    let mut post_flip_monotone_violations = 0u64;
    let t0 = Instant::now();

    while consumed < total_items {
        match pin.ordered_try_pop_with_stamp(0, &mut out) {
            Ok((_n, stamp)) => {
                let producer =
                    u64::from_le_bytes(out[..8].try_into().unwrap()) as usize;
                let seq = u64::from_le_bytes(out[8..16].try_into().unwrap()) as i64;
                let s = &mut stats[producer];
                assert!(seq > s.last_seq,
                        "per-producer FIFO violated: producer {producer} seq {seq} after {}",
                        s.last_seq);
                s.last_seq = seq;
                s.count += 1;
                consumed += 1;

                if flipped_at_inversions.is_some() {
                    if stamp < post_flip_last_stamp {
                        post_flip_monotone_violations += 1;
                    }
                    post_flip_last_stamp = stamp;
                }

                if consumed == flip_at {
                    // The ordered switch, mid-traffic: one Release
                    // store on the MMF-resident flag. The backlog
                    // already in the rings merges retroactively.
                    let inversions_now = ring.inversions();
                    ring.set_ordering_mode(OrderingMode::MergeByStamp)
                        .map_err(|e| format!("flip: {e:?}"))?;
                    flipped_at_inversions = Some(inversions_now);
                    println!(
                        "[flip] at item {consumed}: inversions so far = {inversions_now} \
                         ({:.0}/sec); ordering mode -> MergeByStamp",
                        inversions_now as f64 / t0.elapsed().as_secs_f64(),
                    );
                }
            }
            Err(_) => std::hint::spin_loop(),
        }
    }
    let elapsed = t0.elapsed();

    // ----- producer-side reports -----
    for child in &mut children {
        let status = child.wait()?;
        assert!(status.success(), "producer process failed: {status:?}");
    }
    for mut child in children {
        if let Some(stdout) = child.stdout.take() {
            use std::io::Read;
            let mut s = String::new();
            std::io::BufReader::new(stdout).read_to_string(&mut s).ok();
            for line in s.lines() {
                println!("[producer] {line}");
            }
        }
    }

    // ----- results + assertions -----
    let final_inversions = ring.inversions();
    println!();
    println!("=== Result ===");
    println!("  consumed:                 {consumed} / {total_items}");
    println!("  elapsed:                  {elapsed:?} ({:.1} ns/item)",
             elapsed.as_nanos() as f64 / total_items as f64);
    println!("  total inversions:         {final_inversions}");

    assert_eq!(consumed, total_items, "INTEGRITY FAIL: count mismatch");
    for (producer, s) in stats.iter().enumerate() {
        assert_eq!(s.count, items_per_producer,
                   "producer {producer} delivered {} of {items_per_producer}", s.count);
        assert_eq!(s.last_seq, items_per_producer as i64 - 1,
                   "producer {producer} final seq wrong");
    }
    println!("  zero loss:                PASS (every producer sequence complete)");

    if let Some(at_flip) = flipped_at_inversions {
        assert!(at_flip > 0,
                "the composed interleave must show inversions before the flip \
                 ({producers} concurrent producers, round-robin drain)");
        println!("  inversions before flip:   {at_flip} (> 0 as expected for composed)");
        assert_eq!(final_inversions, at_flip,
                   "merged pops must add ZERO inversions after the flip");
        println!("  inversions after flip:    0 (PASS)");
        assert_eq!(post_flip_monotone_violations, 0,
                   "stamps must be monotone from the flip point");
        println!("  post-flip stamp order:    monotone (PASS)");
    } else {
        assert!(final_inversions > 0,
                "the composed interleave must show inversions \
                 ({producers} concurrent producers, round-robin drain)");
        println!("  inversion rate:           {:.0}/sec (report-only run)",
                 final_inversions as f64 / elapsed.as_secs_f64());
    }

    // Cleanup the backing files (the ring's mmaps must release
    // before the removes succeed on Windows).
    drop(ring);
    for suffix in std::iter::once(".spsc.bin".to_string())
        .chain(std::iter::once(".vyukov.bin".to_string()))
        .chain(std::iter::once(".ordering.bin".to_string()))
        .chain((0..producers).map(|i| format!(".mpsc.{i}.bin")))
        .chain((0..producers).map(|i| format!(".mpmc.{i}.bin")))
    {
        let mut p = prefix.as_os_str().to_owned();
        p.push(&suffix);
        std::fs::remove_file(std::path::PathBuf::from(p)).ok();
    }

    println!();
    println!("E2E PASS");
    Ok(())
}
