//! Unified cost-function policy vs the three independent threshold
//! policies, A/B across a mixed workload that exercises both the
//! shape and capacity axes plus a contention phase.
//!
//! Baseline ("independent"): one thread running the capacity policy
//! (DefaultCapacityPolicy -> morph_capacity_to) AND the shape policy
//! (DefaultRingShapePolicy on peer counts -> ring_handle().morph_to)
//! each tick, blind to each other, each with its own 100 ms
//! hysteresis - the faithful "today" stack.
//!
//! Unified: one UnifiedSidecar scoring (shape, capacity) jointly and
//! emitting compound morph_to_config moves, gated by confidence.
//!
//! Four phases on ONE ring (single consumer throughout, so exactly-
//! once is well defined and asserted):
//!   A  1P/1C light load     -> SPSC, small capacity
//!   B  2P/1C moderate        -> MPSC
//!   C  4P/1C heavy load      -> MPSC, grown capacity
//!   D  4P drain              -> MPSC, capacity descends
//!
//! Both arms reach the SAME shape per phase (shape is peer-count
//! driven - a hard validity constraint, not a tuning choice). The
//! divergence is the CAPACITY axis and the MORPH COUNT: when the
//! shape morphs SPSC -> MPSC the total slot inventory jumps Nx (one
//! sub-ring per producer), collapsing the fill ratio. The independent
//! capacity policy, blind to the shape change, thrashes chasing the
//! perturbed signal; the unified policy folds the sub-ring
//! multiplication into one cost and settles in one compound move.
//!
//! Built-in bench audit (asserted, not printed):
//!   integrity  - exactly-once delivery over the whole run (the
//!                rigorous per-producer seen-set), both arms.
//!   engagement - both arms reach MPSC by phase C (the shape both
//!                policies must reach); the comparison is morph count
//!                and the settled capacity, not the shape.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::{DefaultRingShapePolicy, RingShape};
use subetha_cxc::{
    CapacityAdaptiveRing, CapacityPolicy, CapacityPolicyObservation,
    DefaultCapacityPolicy, GateConfig, RingConfig, UnifiedPolicy, UnifiedSidecar,
};

const PAYLOAD: usize = 56;

fn payload(producer: u16, seq: u64) -> [u8; PAYLOAD] {
    let mut p = [0u8; PAYLOAD];
    p[..8].copy_from_slice(&seq.to_le_bytes());
    p[8..10].copy_from_slice(&producer.to_le_bytes());
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

#[derive(Clone, Copy)]
struct PhaseEnd {
    shape: RingShape,
    capacity: usize,
}

struct ArmResult {
    phase_ends: Vec<PhaseEnd>,
    total_morphs: u64,
    prewarms: u64,
    avg_decision_ns: u64,
    consumed: u64,
    pushed: u64,
    /// Backpressure events: producer try_send failures (the ring was
    /// full). The end-state-quality signal - a leaner capacity that
    /// does not backpressure is free; one that does is underprovisioned.
    backpressure: u64,
}

fn shape_name(s: RingShape) -> &'static str {
    match s {
        RingShape::Spsc => "SPSC",
        RingShape::Mpsc => "MPSC",
        RingShape::Mpmc => "MPMC",
        RingShape::Vyukov => "Vyukov",
    }
}

/// One producer thread: pushes `payload(pid, seq)` while its `active`
/// flag is set, spinning on backpressure. `interleave_gap` paces it -
/// a tiny gap lets producers race (high cross-producer inversions);
/// a larger gap spaces them out (low inversions).
struct Producer {
    handle: std::thread::JoinHandle<u64>,
    active: Arc<AtomicBool>,
}

fn spawn_producer(
    ring: Arc<CapacityAdaptiveRing>,
    pid: u16,
    interleave_gap: Duration,
    run: Arc<AtomicBool>,
    backpressure: Arc<AtomicU64>,
) -> Producer {
    let active = Arc::new(AtomicBool::new(false));
    let active_c = Arc::clone(&active);
    let handle = std::thread::spawn(move || {
        let mut seq = 0u64;
        while run.load(Ordering::Acquire) {
            if active_c.load(Ordering::Acquire) {
                if ring.try_send(pid as usize, &payload(pid, seq)).is_ok() {
                    seq += 1;
                } else {
                    // Ring full = backpressure (the end-state-quality
                    // signal).
                    backpressure.fetch_add(1, Ordering::Relaxed);
                }
                if !interleave_gap.is_zero() {
                    busy_wait(interleave_gap);
                }
            } else {
                busy_wait(Duration::from_micros(50));
            }
        }
        seq
    });
    Producer { handle, active }
}

fn run_arm(arm: Arm) -> ArmResult {
    // 4 producer slots, 1 consumer. Unstamped: the shape choice is
    // peer-count driven, so no inversion signal is needed.
    let ring = Arc::new(
        CapacityAdaptiveRing::create_anon(4, 1, 256).unwrap(),
    );
    let cid = ring.register_consumer().unwrap();
    // Producer 0 registered up front; 1..4 register as phases scale.
    let pid0 = ring.register_producer().unwrap();
    assert_eq!(pid0, 0);

    let run = Arc::new(AtomicBool::new(true));
    // Separate from `run`: producers stop on `run`, but the consumer
    // keeps draining until `drain_done` so a transient empty behind
    // the morph stale-walk near shutdown is not mistaken for loss.
    let drain_done = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));
    let backpressure = Arc::new(AtomicU64::new(0));

    // Consumer: drains continuously, verifies per-producer EXACTLY-
    // ONCE rigorously (every seq seen exactly once - no loss, no
    // duplication). Strict cross-morph FIFO under concurrent
    // producers is NOT asserted: the stale-list design guarantees
    // exactly-once across a morph boundary, not in-order delivery of
    // items in flight during the swap - the library's own concurrent
    // multi-morph test checks the same exactly-once property via a
    // sort. A duplicate trips the insert assertion immediately;
    // completeness is checked at the end against the producer counts.
    let r = Arc::clone(&ring);
    let drain_done_c = Arc::clone(&drain_done);
    let consumed_c = Arc::clone(&consumed);
    let consumer = std::thread::spawn(move || {
        let mut seen: HashMap<u16, std::collections::HashSet<u64>> = HashMap::new();
        let mut out = [0u8; 64];
        loop {
            match r.try_recv(cid, &mut out) {
                Ok(_) => {
                    let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                    let pid = u16::from_le_bytes(out[8..10].try_into().unwrap());
                    assert!(seen.entry(pid).or_default().insert(seq),
                            "integrity: duplicate delivery (producer {pid} seq {seq})");
                    consumed_c.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    if drain_done_c.load(Ordering::Acquire) {
                        return seen;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Spawn all four producer threads up front; activate per phase.
    // Producer 0 paced wide (light, low inversions); 1..4 race with
    // tiny gaps so phase C generates heavy cross-producer inversions.
    let mut producers = vec![spawn_producer(
        Arc::clone(&ring), 0, Duration::from_micros(8), Arc::clone(&run),
        Arc::clone(&backpressure),
    )];
    for pid in 1..4u16 {
        producers.push(spawn_producer(
            Arc::clone(&ring), pid, Duration::from_micros(2), Arc::clone(&run),
            Arc::clone(&backpressure),
        ));
    }

    // --- the policy driver (one of the two arms) ---
    let driver_stop = Arc::new(AtomicBool::new(false));
    let total_morphs = Arc::new(AtomicU64::new(0));
    let decision_ns = Arc::new(AtomicU64::new(0));
    let decision_ct = Arc::new(AtomicU64::new(0));

    let unified_sidecar = if arm == Arm::Unified {
        Some(UnifiedSidecar::spawn(
            Arc::clone(&ring),
            UnifiedPolicy { min_capacity: 64, max_capacity: 4096, ..UnifiedPolicy::default() },
            Duration::from_millis(5),
            GateConfig::enabled(),
        ))
    } else {
        None
    };

    let baseline_driver = if arm == Arm::Independent {
        let r = Arc::clone(&ring);
        let stop = Arc::clone(&driver_stop);
        let morphs = Arc::clone(&total_morphs);
        let dns = Arc::clone(&decision_ns);
        let dct = Arc::clone(&decision_ct);
        Some(std::thread::spawn(move || {
            let cap_policy = DefaultCapacityPolicy {
                grow_at: 0.85, shrink_at: 0.10,
                min_capacity: 64, max_capacity: 4096,
                hysteresis: Duration::from_millis(100),
            };
            let mut last_cap_morph = Instant::now();
            let mut last_shape_morph = Instant::now();
            while !stop.load(Ordering::Acquire) {
                let active = r.ring_handle();
                let cap_obs = CapacityPolicyObservation {
                    current_capacity: r.current_capacity(),
                    active_approx_len: active.approx_len(),
                    total_slot_capacity: active.total_slot_capacity(),
                    since_last_morph: last_cap_morph.elapsed(),
                };
                let (p, c) = (active.active_producers(), active.active_consumers());
                drop(active);

                let t = Instant::now();
                let cap_decision = cap_policy.decide(&cap_obs);
                let shape_target = DefaultRingShapePolicy::target_shape(p, c);
                dns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                dct.fetch_add(1, Ordering::Relaxed);

                // Two INDEPENDENT, blind decisions, each its own
                // morph. Both route through morph_to_config (single-
                // axis configs) so each is serialized by the wrapper
                // morph lock - the faithful "today" cost is two
                // separate locked morphs and two pin bumps where the
                // unified policy does one compound move. (Driving the
                // inner shape morph directly via ring_handle() would
                // race the capacity morph's shape mirror - that is the
                // unsynchronized-independent-morphs hazard the unified
                // path removes by construction.)
                if let Some(new_cap) = cap_decision
                    && r.morph_to_config(&RingConfig {
                        shape: None,
                        capacity: Some(new_cap),
                        locale: None,
                    }).is_ok()
                {
                    last_cap_morph = Instant::now();
                    morphs.fetch_add(1, Ordering::Relaxed);
                }
                // Shape policy (peer-count only - can never reach
                // Vyukov, which is the phase-C divergence).
                if last_shape_morph.elapsed() >= Duration::from_millis(100)
                    && let Some(target) = shape_target
                    && target != r.ring_handle().current_shape()
                    && r.morph_to_config(&RingConfig {
                        shape: Some(target),
                        capacity: None,
                        locale: None,
                    }).is_ok()
                {
                    last_shape_morph = Instant::now();
                    morphs.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }))
    } else {
        None
    };

    // Phase runner: activate the given producers, hold for `dur`,
    // settle, then snapshot the reached config.
    let settle_and_snapshot = |ring: &CapacityAdaptiveRing| -> PhaseEnd {
        std::thread::sleep(Duration::from_millis(400));
        PhaseEnd {
            shape: ring.ring_handle().current_shape(),
            capacity: ring.current_capacity(),
        }
    };

    let mut phase_ends = Vec::new();

    // Phase A: 1P/1C light. Only producer 0 active, paced wide.
    producers[0].active.store(true, Ordering::Release);
    std::thread::sleep(Duration::from_millis(600));
    phase_ends.push(settle_and_snapshot(&ring));

    // Phase B: 2P/1C moderate. Register producer 1, then morph the
    // shape to MPSC SYNCHRONOUSLY before activating it - two
    // producers at an SPSC ring violate its single-producer
    // contract, and the policy's async morph leaves a window. The
    // synchronous morph routes through morph_to_config (wrapper
    // morph lock) so it cannot race the policy sidecar's morphs.
    // The shape is now a valid MPSC; the policy refines capacity and
    // (unified) may upgrade the shape to a better VALID shape.
    ring.register_producer().unwrap();
    ring.morph_to_config(&RingConfig {
        shape: Some(RingShape::Mpsc),
        capacity: None,
        locale: None,
    }).unwrap();
    producers[1].active.store(true, Ordering::Release);
    std::thread::sleep(Duration::from_millis(800));
    phase_ends.push(settle_and_snapshot(&ring));

    // Phase C: 4P/1C heavy load. Register 2,3 - the shape is already
    // MPSC (valid for any producer count at 1 consumer) - then
    // activate them. The 4 producers now drive heavy load; the
    // capacity must grow to relieve backpressure. Both arms hold
    // MPSC; the difference is how each sizes the capacity (and how
    // many morphs it takes to get there).
    ring.register_producer().unwrap();
    ring.register_producer().unwrap();
    producers[2].active.store(true, Ordering::Release);
    producers[3].active.store(true, Ordering::Release);
    std::thread::sleep(Duration::from_millis(1500));
    phase_ends.push(settle_and_snapshot(&ring));

    // Phase D: load drops at 4P/1C - producers 1,2,3 stop pushing
    // but stay registered, so the shape holds MPSC while the
    // capacity descends as the ring drains. (Peer counts are left
    // intact: reducing them would require unregistering a producer
    // whose items are still in flight, a separate concern from the
    // policy comparison under test.)
    for p in producers.iter().take(4).skip(1) {
        p.active.store(false, Ordering::Release);
    }
    std::thread::sleep(Duration::from_millis(1500));
    phase_ends.push(settle_and_snapshot(&ring));

    // Teardown order: stop producers, QUIESCE all morphing (so no
    // morph perturbs the tail), join producer counts, drain to
    // completion, then verify.
    run.store(false, Ordering::Release);
    driver_stop.store(true, Ordering::Release);
    let (prewarms, avg_ns) = if let Some(s) = unified_sidecar {
        let pw = s.prewarms_issued();
        let m = s.morphs_triggered();
        let ans = s.avg_scan_ns();
        total_morphs.store(m, Ordering::Relaxed);
        s.shutdown(); // joins the sidecar thread - morphing has stopped
        (pw, ans)
    } else {
        let n = decision_ct.load(Ordering::Relaxed);
        (0, decision_ns.load(Ordering::Relaxed).checked_div(n).unwrap_or(0))
    };
    if let Some(h) = baseline_driver {
        h.join().unwrap();
    }
    let mut pushed_per_producer: Vec<u64> = Vec::new();
    for p in producers {
        pushed_per_producer.push(p.handle.join().unwrap());
    }
    let pushed: u64 = pushed_per_producer.iter().sum();
    // The ring is now quiescent (no producers, no morphs). Drain to
    // completion. If real loss occurred, consumed never reaches
    // pushed and the deadline expires - the completeness assertion
    // below flags it honestly.
    let deadline = Instant::now() + Duration::from_secs(30);
    while consumed.load(Ordering::Relaxed) < pushed && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    drain_done.store(true, Ordering::Release);
    // Verify per-producer completeness (exactly-once: each producer's
    // full 0..N range delivered, no gaps, no duplicates).
    let seen = consumer.join().unwrap();
    for (pid, pushed_n) in pushed_per_producer.iter().enumerate() {
        let got = seen.get(&(pid as u16)).map(|s| s.len()).unwrap_or(0) as u64;
        assert_eq!(got, *pushed_n,
                   "integrity: producer {pid} delivered exactly-once ({got} of {pushed_n})");
    }

    ArmResult {
        phase_ends,
        total_morphs: total_morphs.load(Ordering::Relaxed),
        prewarms,
        avg_decision_ns: avg_ns,
        consumed: consumed.load(Ordering::Relaxed),
        pushed,
        backpressure: backpressure.load(Ordering::Relaxed),
    }
}

fn main() {
    println!("unified cost-function policy vs three independent threshold policies");
    println!("phases: A 1P/1C light -> B 2P/1C moderate -> C 4P/1C heavy -> D 4P drain");
    println!("both reach the same (peer-count-driven) shape per phase; the divergence is");
    println!("the CAPACITY axis and MORPH COUNT - the independent capacity policy thrashes");
    println!("when a shape change perturbs the fill ratio it reads, while the unified policy");
    println!("folds the shape's sub-ring multiplication into one joint decision.");
    println!();

    let independent = run_arm(Arm::Independent);
    let unified = run_arm(Arm::Unified);

    // Integrity (both arms).
    assert_eq!(independent.consumed, independent.pushed,
               "integrity: independent arm exactly-once");
    assert_eq!(unified.consumed, unified.pushed,
               "integrity: unified arm exactly-once");

    let phase_names = ["A 1P/1C", "B 2P/1C", "C 4P/1C heavy", "D 4P drain"];
    println!("{:<14} {:>18} {:>18}", "phase", "independent", "unified");
    for (i, name) in phase_names.iter().enumerate() {
        let ind = independent.phase_ends[i];
        let uni = unified.phase_ends[i];
        println!("{:<14} {:>12} cap{:<5} {:>12} cap{:<5}",
                 name,
                 shape_name(ind.shape), ind.capacity,
                 shape_name(uni.shape), uni.capacity);
    }
    println!();
    println!("{:<22} {:>12} {:>12}", "metric", "independent", "unified");
    println!("{:<22} {:>12} {:>12}", "total morphs",
             independent.total_morphs, unified.total_morphs);
    println!("{:<22} {:>12} {:>12}", "prewarms issued", "-", unified.prewarms);
    println!("{:<22} {:>12} {:>12}", "avg decision ns",
             independent.avg_decision_ns, unified.avg_decision_ns);
    println!("{:<22} {:>12} {:>12}", "items delivered",
             independent.consumed, unified.consumed);
    println!("{:<22} {:>12} {:>12}", "backpressure events",
             independent.backpressure, unified.backpressure);

    // Engagement audit: both arms must reach the shape the workload
    // requires (MPSC at 4P/1C) - the comparison is capacity + morph
    // count, so both arms must agree on the shape for the comparison
    // to be apples-to-apples.
    assert_eq!(unified.phase_ends[2].shape, RingShape::Mpsc,
               "audit: unified must reach MPSC at 4P/1C heavy load");
    assert_eq!(independent.phase_ends[2].shape, RingShape::Mpsc,
               "audit: independent must reach MPSC at 4P/1C heavy load");

    println!();
    println!("morph-count ratio: independent {} vs unified {} morphs",
             independent.total_morphs, unified.total_morphs);
    println!("all integrity + audit assertions held");
}
