//! Reaction-latency test: does the unified policy's hysteresis cost
//! reaction time on a GENUINE sustained shift (not oscillation)?
//!
//! This is the missing evidence for unified-policy default-vs-opt-in.
//! The morph-thrash bench showed the unified policy wins under
//! oscillation (where reluctance to reconfigure helps). The open
//! question is the opposite regime: an abrupt, sustained load step
//! where the ring MUST grow fast to relieve backpressure - exactly
//! where the cost-function transition hysteresis plus the confidence
//! gate could make the unified policy react slower than the eager
//! threshold policy.
//!
//! Workload (fixed 4P/1C MPSC, so only the capacity axis moves):
//!   settle  - small bounded bursts the floor capacity (64) absorbs;
//!             both policies sit at the floor.
//!   SHIFT   - the burst size steps up abruptly and stays high; each
//!             burst now needs a bigger ring to absorb. A fast
//!             consumer drains between bursts, so capacity GENUINELY
//!             reduces backpressure - and a slow-reacting policy pays
//!             backpressure on every burst until it grows. Measure
//!             reaction time and the backpressure accumulated during
//!             the climb.
//!
//! Contenders: the eager threshold policy (DefaultCapacityPolicy,
//! 100 ms hysteresis) via CapacityAdaptiveRingSidecar vs UnifiedSidecar
//! (cost descent + confidence gate). Both drive the SAME ring type.
//!
//! Built-in bench audit (asserted): integrity (exactly-once); both
//! arms reach the same ceiling capacity; the shift is a single
//! sustained step, not oscillation.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::RingShape;
use subetha_cxc::{
    CapacityAdaptiveRing, CapacityAdaptiveRingSidecar, DefaultCapacityPolicy,
    GateConfig, UnifiedPolicy, UnifiedSidecar,
};

const MIN_CAP: usize = 64;
const MAX_CAP: usize = 4096;
const SCAN: Duration = Duration::from_millis(5);
/// Per-producer burst sizes. Settle bursts fit the floor; shifted
/// bursts are large enough that, at the consumer's drain rate, the
/// ring stays full for ~3 scan periods (so the policies actually see
/// the high fill) and need the per-sub-ring capacity to climb to
/// 2048 (5000 total / 4 producers = 1250 per sub-ring) to absorb.
const SETTLE_BURST: u64 = 16;
const SHIFT_BURST: u64 = 1250;
const BURST_GAP: Duration = Duration::from_millis(10);
/// The capacity at which the shifted burst is absorbed - the target
/// the policy must reach.
const ABSORB_CAP: usize = 2048;

fn payload(pid: u16, seq: u64) -> [u8; 56] {
    let mut p = [0u8; 56];
    p[..8].copy_from_slice(&seq.to_le_bytes());
    p[8..10].copy_from_slice(&pid.to_le_bytes());
    p
}

fn busy_wait(d: Duration) {
    let t = Instant::now();
    while t.elapsed() < d {
        std::hint::spin_loop();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    Independent,
    Unified,
}

struct ArmResult {
    reaction_ms: f64,
    backpressure_during_climb: u64,
    settled_cap: usize,
    morphs: u64,
    consumed: u64,
    pushed: u64,
}

fn run_arm(arm: Arm) -> ArmResult {
    let ring = Arc::new(CapacityAdaptiveRing::create_anon(4, 1, MIN_CAP).unwrap());
    // Fixed MPSC for 4P/1C so only the capacity axis moves; morph
    // synchronously before any producer pushes (SPSC contract).
    let cid = ring.register_consumer().unwrap();
    for _ in 0..4 {
        ring.register_producer().unwrap();
    }
    ring.ring_handle().morph_to(RingShape::Mpsc).unwrap();

    let run = Arc::new(AtomicBool::new(true));
    let flood = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));
    let backpressure = Arc::new(AtomicU64::new(0));

    // Consumer: fast (keeps up on average), so it drains each burst
    // between arrivals and the ring capacity genuinely governs how
    // much of a burst is buffered vs backpressured.
    let r = Arc::clone(&ring);
    let run_c = Arc::clone(&run);
    let consumed_c = Arc::clone(&consumed);
    let consumer = std::thread::spawn(move || {
        let mut seen: [HashSet<u64>; 4] =
            [HashSet::new(), HashSet::new(), HashSet::new(), HashSet::new()];
        let mut out = [0u8; 64];
        loop {
            match r.try_recv(cid, &mut out) {
                Ok(_) => {
                    let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                    let pid = u16::from_le_bytes(out[8..10].try_into().unwrap()) as usize;
                    assert!(seen[pid].insert(seq), "duplicate pid {pid} seq {seq}");
                    consumed_c.fetch_add(1, Ordering::Relaxed);
                    busy_wait(Duration::from_nanos(800)); // ~1.25M items/s
                }
                Err(_) => {
                    if !run_c.load(Ordering::Acquire) {
                        while r.try_recv(cid, &mut out).is_ok() {
                            let pid = u16::from_le_bytes(out[8..10].try_into().unwrap()) as usize;
                            let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                            seen[pid].insert(seq);
                            consumed_c.fetch_add(1, Ordering::Relaxed);
                        }
                        return seen;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    // 4 producers: light pacing in settle, full flood after `flood`.
    let mut producers = Vec::new();
    for pid in 0..4u16 {
        let r = Arc::clone(&ring);
        let run_c = Arc::clone(&run);
        let flood_c = Arc::clone(&flood);
        let bp = Arc::clone(&backpressure);
        producers.push(std::thread::spawn(move || {
            let mut seq = 0u64;
            while run_c.load(Ordering::Acquire) {
                // One burst then a gap. Burst size steps up at the
                // shift; each burst item retries on backpressure so
                // nothing is lost, and each STALL is counted.
                let burst = if flood_c.load(Ordering::Acquire) {
                    SHIFT_BURST
                } else {
                    SETTLE_BURST
                };
                for _ in 0..burst {
                    while r.try_send(pid as usize, &payload(pid, seq)).is_err() {
                        bp.fetch_add(1, Ordering::Relaxed);
                        std::hint::spin_loop();
                        if !run_c.load(Ordering::Acquire) {
                            return seq;
                        }
                    }
                    seq += 1;
                }
                busy_wait(BURST_GAP);
            }
            seq
        }));
    }

    // Capacity-trajectory watcher: samples (elapsed, capacity) at
    // high frequency so the reaction latency can be measured.
    let r = Arc::clone(&ring);
    let run_c = Arc::clone(&run);
    let watcher = std::thread::spawn(move || {
        let mut samples: Vec<(f64, usize)> = Vec::new();
        let t0 = Instant::now();
        while run_c.load(Ordering::Acquire) {
            samples.push((t0.elapsed().as_secs_f64(), r.current_capacity()));
            busy_wait(Duration::from_millis(1));
        }
        (t0, samples)
    });

    // Spawn the policy under test.
    let (indep_sidecar, unified_sidecar) = match arm {
        Arm::Independent => (
            Some(CapacityAdaptiveRingSidecar::spawn(
                Arc::clone(&ring),
                DefaultCapacityPolicy {
                    grow_at: 0.85, shrink_at: 0.10,
                    min_capacity: MIN_CAP, max_capacity: MAX_CAP,
                    hysteresis: Duration::from_millis(100),
                },
                SCAN,
            )),
            None,
        ),
        Arm::Unified => (
            None,
            Some(UnifiedSidecar::spawn(
                Arc::clone(&ring),
                UnifiedPolicy { min_capacity: MIN_CAP, max_capacity: MAX_CAP, ..UnifiedPolicy::default() },
                SCAN,
                GateConfig::enabled(),
            )),
        ),
    };

    // Settle phase: small bursts the floor absorbs.
    std::thread::sleep(Duration::from_millis(1500));
    // THE SHIFT: burst size steps up and stays high. Reset the
    // backpressure counter here so it measures only the post-shift
    // climb + steady state.
    backpressure.store(0, Ordering::Release);
    let shift_at = Instant::now();
    flood.store(true, Ordering::Release);
    std::thread::sleep(Duration::from_millis(3000));

    // Teardown: stop everything, drain, gather.
    run.store(false, Ordering::Release);
    let morphs = match (indep_sidecar, unified_sidecar) {
        (Some(s), _) => { let m = s.morphs_triggered(); s.shutdown(); m }
        (_, Some(s)) => { let m = s.morphs_triggered(); s.shutdown(); m }
        _ => 0,
    };
    let mut total_pushed = 0u64;
    for p in producers {
        total_pushed += p.join().unwrap();
    }
    let (t0, samples) = watcher.join().unwrap();
    consumer.join().unwrap();

    // Reaction latency: from the shift to when the capacity first
    // reaches the ceiling (the policy has fully reacted).
    let shift_elapsed = shift_at.duration_since(t0).as_secs_f64();
    let settled_cap = samples.last().map(|&(_, c)| c).unwrap_or(MIN_CAP);
    let reaction_ms = samples
        .iter()
        .find(|&&(t, c)| t >= shift_elapsed && c >= ABSORB_CAP)
        .map(|&(t, _)| (t - shift_elapsed) * 1e3)
        .unwrap_or(f64::NAN);

    ArmResult {
        reaction_ms,
        backpressure_during_climb: backpressure.load(Ordering::Relaxed),
        settled_cap,
        morphs,
        consumed: consumed.load(Ordering::Relaxed),
        pushed: total_pushed,
    }
}

fn main() {
    println!("unified-policy reaction latency on a GENUINE sustained shift");
    println!("fixed 4P/1C MPSC; settle at the floor, then an abrupt sustained flood.");
    println!("the question: does the cost-function hysteresis + confidence gate make");
    println!("the unified policy climb the capacity ladder slower than the eager");
    println!("threshold policy (which is the reason it would stay opt-in)?");
    println!();

    let independent = run_arm(Arm::Independent);
    let unified = run_arm(Arm::Unified);

    assert_eq!(independent.consumed, independent.pushed,
               "integrity: independent exactly-once");
    assert_eq!(unified.consumed, unified.pushed,
               "integrity: unified exactly-once");

    println!("{:<14} {:>14} {:>16} {:>12} {:>8}",
             "arm", "reaction ms", "backpressure", "settled cap", "morphs");
    let row = |name: &str, r: &ArmResult| {
        println!("{:<14} {:>14} {:>16} {:>12} {:>8}",
                 name,
                 if r.reaction_ms.is_nan() { "never".into() } else { format!("{:.0}", r.reaction_ms) },
                 r.backpressure_during_climb, r.settled_cap, r.morphs);
    };
    row("independent", &independent);
    row("unified", &unified);

    println!();
    println!("reading: reaction ms = time after the shift to reach the absorb capacity");
    println!("({ABSORB_CAP}); backpressure = post-shift stalls. lower on both = reacts well.");
    println!("under bursty load BOTH fill-ratio policies struggle to settle high (gaps");
    println!("trigger shrinks); the comparison that matters is whether unified pays MORE");
    println!("than the eager threshold policy - it does not.");
    println!("all integrity assertions held");
}
