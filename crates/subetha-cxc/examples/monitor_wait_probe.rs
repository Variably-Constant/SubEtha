//! Probe + wake-latency A/B for the monitor-wait tier.
//!
//! Prints which instruction family the host runs
//! (WAITPKG / MWAITX / none), then measures cross-thread WAKE
//! LATENCY - storer's `Release`-store to waiter's return - over
//! `--rounds` (default 2,000) round trips for three contenders:
//!
//! | contender | what the waiter runs |
//! |---|---|
//! | `spin` | `PAUSE` loop on the atomic (the latency floor; burns the core) |
//! | `monitor` | `monitor_wait_u32` (this tier; light sleep, free wake) |
//! | `park` | `CrossProcessWaker` try_park/wait with the monitor tier disabled-by-budget... see below |
//!
//! The `park` contender measures the production kernel-park path
//! by setting a 1-cycle monitor budget through the public
//! `monitor_wait_u32` entry being bypassed - the waiter calls
//! `CrossProcessWaker::wait` with `SUBETHA_MONITOR_WAIT_CYCLES=1`
//! exported by the harness re-running itself, so what's measured
//! is the futex / `_umtx_op` / `WaitOnAddress` round trip the tier
//! is being compared against. Without the env override the same
//! waker path measures the INTEGRATED ladder (monitor first, park
//! after budget), reported as `waker-integrated`.
//!
//! Timing uses the TSC on both threads (the host's invariant-TSC
//! probe is printed; cycles convert to ns via a measured
//! cycles-per-ns calibration against std::time).
//!
//! Run: cargo run --release --example monitor_wait_probe [-- --rounds N]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::ordering::read_tsc;
use subetha_cxc::{
    has_invariant_tsc, monitor_wait_budget_cycles, monitor_wait_kind,
    monitor_wait_u32, CrossProcessWaker,
};

const WARMUP: u64 = 200;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rounds: u64 = args
        .iter()
        .position(|a| a == "--rounds")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);

    let cycles_per_ns = calibrate_tsc();
    println!("=== monitor-wait probe ===");
    println!("  kind:            {:?}", monitor_wait_kind());
    println!("  budget:          {} cycles", monitor_wait_budget_cycles());
    println!("  invariant TSC:   {}", has_invariant_tsc());
    println!("  TSC rate:        {cycles_per_ns:.2} cycles/ns");
    println!("  rounds:          {rounds}");
    println!();

    let spin = bench_wake_latency(rounds, WaiterKind::Spin);
    report("spin (PAUSE loop)", &spin, cycles_per_ns);

    if monitor_wait_kind().is_some() {
        let monitor = bench_wake_latency(rounds, WaiterKind::Monitor);
        report("monitor tier", &monitor, cycles_per_ns);
    } else {
        println!("  monitor tier: UNAVAILABLE on this host (skipped)");
    }

    let waker = bench_wake_latency(rounds, WaiterKind::Waker);
    let label = if monitor_wait_kind().is_some()
        && monitor_wait_budget_cycles() > 1
    {
        "waker-integrated (monitor tier + park)"
    } else {
        "waker kernel park (monitor tier off)"
    };
    report(label, &waker, cycles_per_ns);
}

#[derive(Clone, Copy, PartialEq)]
enum WaiterKind {
    Spin,
    Monitor,
    Waker,
}

struct Stats {
    samples: Vec<u64>, // cycles
}

/// One round: waiter arms and signals READY; storer spins on READY,
/// records t0 (TSC), fires the wake; waiter records t1 (TSC) on
/// return. Wake latency = t1 - t0 on the shared invariant TSC.
fn bench_wake_latency(rounds: u64, kind: WaiterKind) -> Stats {
    let value = Arc::new(AtomicU32::new(0));
    let ready = Arc::new(AtomicU32::new(0));
    let t0_cell = Arc::new(AtomicU64::new(0));
    let waker = Arc::new(CrossProcessWaker::create_anon(4).expect("waker"));

    let total = rounds + WARMUP;
    let v = Arc::clone(&value);
    let r = Arc::clone(&ready);
    let t0c = Arc::clone(&t0_cell);
    let wk = Arc::clone(&waker);
    let waiter = std::thread::spawn(move || -> Vec<u64> {
        let mut samples = Vec::with_capacity(rounds as usize);
        for round in 0..total {
            let expected = round as u32;
            let target = expected + 1;
            // Arm per-kind, then signal ready.
            match kind {
                WaiterKind::Spin => {
                    r.store(1, Ordering::Release);
                    while v.load(Ordering::Acquire) != target {
                        std::hint::spin_loop();
                    }
                }
                WaiterKind::Monitor => {
                    r.store(1, Ordering::Release);
                    // Budget generous; the wake should land far
                    // inside it. Loop guards spurious budget expiry.
                    while v.load(Ordering::Acquire) != target {
                        monitor_wait_u32(&v, expected, 30_000_000);
                    }
                }
                WaiterKind::Waker => {
                    let token = wk.try_park(target as u64).expect("park slot");
                    r.store(1, Ordering::Release);
                    // Double-check then wait: the standard protocol.
                    // wait() releases the slot itself; the
                    // already-satisfied branch releases explicitly.
                    if v.load(Ordering::Acquire) != target {
                        wk.wait(token, Some(Duration::from_secs(5))).ok();
                    } else {
                        wk.release(token);
                    }
                    while v.load(Ordering::Acquire) != target {
                        std::hint::spin_loop();
                    }
                }
            }
            let t1 = read_tsc();
            if round >= WARMUP {
                samples.push(t1.wrapping_sub(t0c.load(Ordering::Acquire)));
            }
        }
        samples
    });

    for round in 0..total {
        // Wait for the waiter to arm.
        while ready.load(Ordering::Acquire) != 1 {
            std::hint::spin_loop();
        }
        ready.store(0, Ordering::Release);
        // Let the waiter actually reach its wait instruction (the
        // arm-to-wait window): a short fixed delay so the measured
        // path is the WAIT-side wake, not the still-spinning check.
        let spin_until = read_tsc().wrapping_add(20_000);
        while read_tsc().wrapping_sub(spin_until) > i64::MAX as u64 {
            std::hint::spin_loop();
        }
        let t0 = read_tsc();
        t0_cell.store(t0, Ordering::Release);
        value.store(round as u32 + 1, Ordering::Release);
        if kind == WaiterKind::Waker {
            waker.wake_up_to(u64::MAX);
        }
    }

    Stats {
        samples: waiter.join().expect("waiter thread"),
    }
}

fn report(label: &str, stats: &Stats, cycles_per_ns: f64) {
    let mut s = stats.samples.clone();
    s.sort_unstable();
    let n = s.len();
    let to_ns = |c: u64| c as f64 / cycles_per_ns;
    let sum: u128 = s.iter().map(|&v| v as u128).sum();
    println!(
        "  {label:<42} min {:>8.0} ns  p50 {:>8.0} ns  p99 {:>9.0} ns  max {:>10.0} ns  avg {:>8.0} ns",
        to_ns(s[0]),
        to_ns(s[n / 2]),
        to_ns(s[(n * 99 / 100).min(n - 1)]),
        to_ns(s[n - 1]),
        to_ns((sum / n as u128) as u64),
    );
}

/// Measure TSC cycles per nanosecond against std::time over 50ms.
fn calibrate_tsc() -> f64 {
    let wall0 = Instant::now();
    let tsc0 = read_tsc();
    std::thread::sleep(Duration::from_millis(50));
    let tsc1 = read_tsc();
    let wall = wall0.elapsed();
    (tsc1.wrapping_sub(tsc0)) as f64 / wall.as_nanos() as f64
}
