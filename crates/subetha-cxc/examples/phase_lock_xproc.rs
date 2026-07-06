//! Cross-process phase-locked-waiting latency bench.
//!
//! Elevates the in-process `phase_lock_probe` to the same rigor as
//! the flagship cross-process IPC bench (`cross_process_compare`): a
//! REAL two-process setup over a file-backed `BlockingSpscRing`, with
//! machine-readable JSON output. A child process is the producer
//! (paced at a regular period); the parent is the consumer, running
//! `recv_blocking` with phase-locking OFF then ON. Wake-to-item
//! latency is measured one-way across the process boundary via the
//! system clock (`SystemTime`, which is global, unlike the
//! process-local `Instant`).
//!
//! This proves the phase-locked-waiting win holds across the actual
//! OS process boundary - not just between two threads - the same way
//! the IPC bench proves the no-syscall data path across processes.
//!
//! Run: `cargo run --release --example phase_lock_xproc -p subetha-cxc`
//! Child entry: `... phase_lock_xproc --child-producer <path> <period_ns> <n>`
//!
//! Output: `docs/phase_lock_xproc_results.json`.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use subetha_cxc::BlockingSpscRing;
use subetha_cxc::ordering::read_tsc;

const CAPACITY: usize = 1024;
const PAYLOAD: usize = 56;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseRow {
    period_us: u64,
    config: String,
    n: u64,
    p50_ns: u64,
    p99_ns: u64,
    mean_ns: u64,
    predictive_catches: u64,
    speedup_p50_vs_off: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseReport {
    machine: String,
    timestamp: u64,
    transport: String,
    payload_bytes: u32,
    rows: Vec<PhaseRow>,
}

/// Wall-clock seconds for the report timestamp only (a date, not a
/// latency).
fn wall_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// TSC cycles per nanosecond, calibrated against the monotonic clock.
/// The invariant TSC is a single system-wide counter, so a cycle
/// stamp taken in the producer process is directly comparable to one
/// read in the consumer process - the high-resolution cross-process
/// clock the latency measurement needs (SystemTime is too coarse).
fn calibrate_cycles_per_ns() -> f64 {
    let t0 = Instant::now();
    let c0 = read_tsc();
    while t0.elapsed() < Duration::from_millis(200) {
        std::hint::spin_loop();
    }
    let elapsed_ns = t0.elapsed().as_nanos() as f64;
    let c1 = read_tsc();
    (c1.wrapping_sub(c0)) as f64 / elapsed_ns
}

fn busy_until(t: Instant) {
    while Instant::now() < t {
        std::hint::spin_loop();
    }
}

/// Child: open the ring and produce at a regular cadence, stamping
/// each item with the system-clock publish time + sequence number.
fn run_producer(path: &str, period_ns: u64, n: u64) -> Result<(), Box<dyn std::error::Error>> {
    // The parent creates the ring; give it a moment, then open.
    let ring = {
        let mut attempt = 0;
        loop {
            match BlockingSpscRing::open(path, CAPACITY) {
                Ok(r) => break r,
                Err(_) if attempt < 200 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => return Err(format!("child open: {e:?}").into()),
            }
        }
    };
    let start = Instant::now();
    let period = Duration::from_nanos(period_ns);
    for i in 0..n {
        busy_until(start + period * (i as u32));
        let mut payload = [0u8; PAYLOAD];
        // Stamp the system-wide TSC at publish (high-res, comparable
        // across the process boundary).
        payload[..8].copy_from_slice(&read_tsc().to_le_bytes());
        payload[8..16].copy_from_slice(&i.to_le_bytes());
        while ring.try_push(&payload).is_err() {
            std::hint::spin_loop();
        }
    }
    Ok(())
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() as f64 - 1.0) * p).round()) as usize]
}

fn tmp_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_phaselock_{pid}_{nonce}_{tag}"));
    p
}

/// Parent: run one (period, config) cell - create the ring, spawn
/// the producer child, consume N items, measure one-way latency.
fn run_cell(
    period_us: u64,
    n: u64,
    phase_on: bool,
    cycles_per_ns: f64,
) -> Result<PhaseRow, Box<dyn std::error::Error>> {
    let base = tmp_path(&format!("p{period_us}_{}", if phase_on { "on" } else { "off" }));
    let ring = BlockingSpscRing::create(&base, CAPACITY)
        .map_err(|e| format!("create: {e:?}"))?;
    ring.set_phase_locking(phase_on);

    let self_exe = env::current_exe()?;
    let mut child = std::process::Command::new(self_exe)
        .arg("--child-producer")
        .arg(format!("{}", base.display()))
        .arg(format!("{}", period_us * 1000))
        .arg(format!("{n}"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    let mut latencies: Vec<u64> = Vec::with_capacity(n as usize);
    let mut buf = [0u8; 64];
    for expected in 0..n {
        ring.recv_blocking(&mut buf, Some(Duration::from_secs(30)))
            .map_err(|e| format!("recv: {e:?}"))?;
        let observe = read_tsc();
        let publish = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        assert_eq!(seq, expected, "integrity: cross-process FIFO");
        let cycles = observe.wrapping_sub(publish);
        latencies.push((cycles as f64 / cycles_per_ns) as u64);
    }
    child.wait()?;

    let pred = ring.phase_predictive_catches();
    latencies.sort_unstable();
    let mean = latencies.iter().sum::<u64>() / latencies.len().max(1) as u64;
    drop(ring);
    // Best-effort cleanup of the backing files.
    for suffix in [".ring.bin", ".cw.bin", ".pw.bin"] {
        let mut p = base.as_os_str().to_owned();
        p.push(suffix);
        std::fs::remove_file(PathBuf::from(p)).ok();
    }

    Ok(PhaseRow {
        period_us,
        config: if phase_on { "phase-on" } else { "phase-off" }.into(),
        n,
        p50_ns: percentile(&latencies, 0.50),
        p99_ns: percentile(&latencies, 0.99),
        mean_ns: mean,
        predictive_catches: pred,
        speedup_p50_vs_off: 1.0, // filled in after the OFF row is known
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("--child-producer") {
        return run_producer(&args[2], args[3].parse()?, args[4].parse()?);
    }

    println!("cross-process phase-locked-waiting latency bench");
    println!("two OS processes over a file-backed BlockingSpscRing; one-way wake-to-item");
    println!("latency via the system clock; phase-locking OFF vs ON. methodology mirrors");
    println!("the cross_process_compare IPC bench (real child process, JSON output).");
    println!();

    let cycles_per_ns = calibrate_cycles_per_ns();
    println!("calibrated TSC: {cycles_per_ns:.3} cycles/ns");
    println!();
    let periods_us = [50u64, 200, 1000];
    let mut rows: Vec<PhaseRow> = Vec::new();
    for &period_us in &periods_us {
        let n = (400_000u64 / period_us).clamp(800, 8000);
        let off = run_cell(period_us, n, false, cycles_per_ns)?;
        let mut on = run_cell(period_us, n, true, cycles_per_ns)?;
        on.speedup_p50_vs_off = if on.p50_ns == 0 {
            0.0
        } else {
            off.p50_ns as f64 / on.p50_ns as f64
        };
        // Audit (engagement): ON must fire the predictor at these
        // periods (the consumer cleanly waits); OFF never does.
        assert_eq!(off.predictive_catches, 0, "OFF must not phase-lock");
        assert!(on.predictive_catches > n / 4,
                "ON must engage the predictor at period {period_us}us (got {})",
                on.predictive_catches);
        rows.push(off);
        rows.push(on);
    }

    println!("{:<9} {:<11} {:>7} {:>10} {:>10} {:>10} {:>11} {:>9}",
             "period", "config", "n", "p50 ns", "p99 ns", "mean ns", "pred-catch", "p50 x");
    for r in &rows {
        println!("{:<9} {:<11} {:>7} {:>10} {:>10} {:>10} {:>11} {:>8.1}x",
                 format!("{}us", r.period_us), r.config, r.n,
                 r.p50_ns, r.p99_ns, r.mean_ns, r.predictive_catches,
                 r.speedup_p50_vs_off);
    }

    let report = PhaseReport {
        machine: format!("{} ({})", std::env::consts::OS, std::env::consts::ARCH),
        timestamp: wall_secs(),
        transport: "file-backed BlockingSpscRing, cross-process".into(),
        payload_bytes: PAYLOAD as u32,
        rows,
    };
    let docs_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("docs"))
        .ok_or("docs dir")?;
    std::fs::create_dir_all(&docs_dir).ok();
    let json_path = docs_dir.join("phase_lock_xproc_results.json");
    std::fs::write(&json_path, serde_json::to_string_pretty(&report)?)?;
    println!();
    println!("wrote {}", json_path.display());
    println!("all integrity + audit assertions held");
    Ok(())
}
