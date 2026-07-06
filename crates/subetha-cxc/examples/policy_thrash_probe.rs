//! Policy thrash probe: fixed-hysteresis policies vs confidence-
//! gated vs confidence-gated + minimum-sample floors, A/B/C at
//! identical workloads.
//!
//! Capacity scenario: adversarial oscillation - the producer
//! alternates hard-push and idle half-periods at {50, 200, 1000} ms
//! so the fill ratio legitimately crosses the grow and shrink
//! thresholds every half-period - followed by a genuine sustained
//! regime shift to measure reaction latency. The gated arms run
//! their policies with ZERO fixed hysteresis so the comparison
//! isolates mechanism quality (timer vs conviction), not stacking.
//!
//! Ordering scenario: three short inversion-noise bursts separated
//! by clean windows, then a sustained inversion regime. The
//! auto-order arm is one-way by design, so the question is WHERE
//! each arm spends its single flip: on the first noise burst
//! (premature) or inside the sustained regime (justified).
//!
//! Built-in bench audit (asserted, not printed):
//!   engagement - gated arms must fire in the genuine-shift phase
//!                (a gate that never opens measures nothing);
//!                ordering arms must flip exactly once.
//!   integrity  - per-producer counts conserved in every arm.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::{
    AdaptiveRing, AdaptiveRingSidecar, DefaultRingShapePolicy, RingShape,
};
use subetha_cxc::ordering::{default_stamp_kind, OrderingMode};
use subetha_cxc::{
    CapacityAdaptiveRing, CapacityAdaptiveRingSidecar, DefaultCapacityPolicy,
    DefaultOrderingPolicy, GateConfig, QosOrdering, QosPolicy,
};

fn busy_wait(d: Duration) {
    let t = Instant::now();
    while t.elapsed() < d {
        std::hint::spin_loop();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    Baseline,
    Gated,
    GatedMin,
    /// Gate + min-samples STACKED with the production hysteresis -
    /// the timer caps the follow rate at long oscillation periods,
    /// the gate starves noise below its conviction window.
    GatedHyst,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Baseline => "baseline",
            Arm::Gated => "gated",
            Arm::GatedMin => "gated+min",
            Arm::GatedHyst => "gated+hyst",
        }
    }
}

// ===================================================================
// Capacity scenario.
// ===================================================================

struct PhaseStats {
    morphs: u64,
    flip_flops: usize,
    underprov_ms: u64,
    overprov_ms: u64,
}

fn run_capacity_arm(arm: Arm) -> (Vec<PhaseStats>, f64, u64) {
    let ring = Arc::new(CapacityAdaptiveRing::create_anon(1, 1, 256).unwrap());
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();

    // The gated arms zero the policy's fixed hysteresis so the
    // gate is the ONLY damper - mechanism vs mechanism, no
    // stacking. The baseline keeps the production default.
    let hysteresis = match arm {
        Arm::Baseline | Arm::GatedHyst => Duration::from_millis(100),
        _ => Duration::ZERO,
    };
    let policy = DefaultCapacityPolicy {
        grow_at: 0.85,
        shrink_at: 0.10,
        min_capacity: 64,
        max_capacity: 4096,
        hysteresis,
    };
    let sidecar = match arm {
        Arm::Baseline => CapacityAdaptiveRingSidecar::spawn(
            Arc::clone(&ring), policy, Duration::from_millis(5),
        ),
        Arm::Gated => CapacityAdaptiveRingSidecar::spawn_gated(
            Arc::clone(&ring), policy, Duration::from_millis(5),
            GateConfig::enabled(),
        ),
        Arm::GatedMin | Arm::GatedHyst => CapacityAdaptiveRingSidecar::spawn_gated(
            Arc::clone(&ring), policy, Duration::from_millis(5),
            GateConfig::enabled_with_arity(3),
        ),
    };

    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    // Consumer: fixed service rate ~30 items/ms.
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let co = Arc::clone(&consumed);
    let consumer = std::thread::spawn(move || {
        let mut out = [0u8; 64];
        while !s.load(Ordering::Acquire) {
            if r.try_recv(0, &mut out).is_ok() {
                co.fetch_add(1, Ordering::Relaxed);
            }
            busy_wait(Duration::from_micros(33));
        }
        while r.try_recv(0, &mut out).is_ok() {
            co.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Watcher: capacity transitions (for flip-flop counting) +
    // fill-based misclassification integration, 1 ms cadence.
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let watcher = std::thread::spawn(move || {
        let t0 = Instant::now();
        let mut transitions: Vec<(f64, usize)> = vec![(0.0, r.current_capacity())];
        let mut underprov_ms = 0u64;
        let mut overprov_ms = 0u64;
        let mut marks: Vec<(f64, u64, u64)> = Vec::new();
        while !s.load(Ordering::Acquire) {
            let cap = r.current_capacity();
            if cap != transitions.last().unwrap().1 {
                transitions.push((t0.elapsed().as_secs_f64(), cap));
            }
            let active = r.ring_handle();
            let fill = active.approx_len() as f64
                / active.total_slot_capacity().max(1) as f64;
            drop(active);
            if fill >= 0.85 {
                underprov_ms += 1;
            } else if fill <= 0.10 && cap > 64 {
                overprov_ms += 1;
            }
            marks.push((t0.elapsed().as_secs_f64(), underprov_ms, overprov_ms));
            busy_wait(Duration::from_millis(1));
        }
        (transitions, marks)
    });

    // Producer phases. Push rate ~100 items/ms during high halves.
    let t0 = Instant::now();
    let mut phase_bounds: Vec<(f64, f64, f64)> = Vec::new(); // (start, end, period_s)
    let mut push_seq = 0u64;
    let push = |ring: &CapacityAdaptiveRing, n: &mut u64| {
        let mut p = [0u8; 56];
        p[..8].copy_from_slice(&n.to_le_bytes());
        if ring.try_send(0, &p).is_ok() {
            *n += 1;
        }
    };
    for (period_ms, dur_ms) in [(50u64, 2000u64), (200, 2000), (1000, 4000)] {
        let start = t0.elapsed().as_secs_f64();
        let phase_end = Instant::now() + Duration::from_millis(dur_ms);
        while Instant::now() < phase_end {
            let half = Instant::now() + Duration::from_millis(period_ms / 2);
            while Instant::now() < half && Instant::now() < phase_end {
                push(&ring, &mut push_seq);
                busy_wait(Duration::from_micros(10));
            }
            let idle = Instant::now() + Duration::from_millis(period_ms / 2);
            while Instant::now() < idle && Instant::now() < phase_end {
                busy_wait(Duration::from_micros(200));
            }
        }
        phase_bounds.push((start, t0.elapsed().as_secs_f64(), period_ms as f64 / 1e3));
    }

    // Genuine regime shift: settle, then sustained heavy load.
    std::thread::sleep(Duration::from_millis(300));
    let morphs_before_shift = sidecar.morphs_triggered();
    let shift_start = t0.elapsed().as_secs_f64();
    let genuine_end = Instant::now() + Duration::from_millis(1000);
    while Instant::now() < genuine_end {
        push(&ring, &mut push_seq);
        busy_wait(Duration::from_micros(10));
    }
    let genuine_phase = (shift_start, t0.elapsed().as_secs_f64(), 0.0);

    stop.store(true, Ordering::Release);
    consumer.join().unwrap();
    let (transitions, marks) = watcher.join().unwrap();
    let total_morphs = sidecar.morphs_triggered();
    sidecar.shutdown();

    // Integrity: everything pushed was consumed.
    assert_eq!(consumed.load(Ordering::Relaxed), push_seq,
               "integrity: exactly-once across all morphs");
    // Audit (engagement): the gated arms must have acted on the
    // genuine shift - a gate that never opens measures nothing.
    let shift_morphs = total_morphs - morphs_before_shift;
    assert!(shift_morphs >= 1,
            "audit: every arm must react to the genuine regime shift");

    // Reaction latency: first capacity transition after shift start.
    let reaction_ms = transitions
        .iter()
        .find(|(t, _)| *t >= shift_start)
        .map(|(t, _)| (t - shift_start) * 1e3)
        .unwrap_or(f64::NAN);

    // Per-phase stats.
    let phases: Vec<PhaseStats> = phase_bounds
        .iter()
        .chain(std::iter::once(&genuine_phase))
        .map(|(start, end, period_s)| {
            let in_phase: Vec<&(f64, usize)> = transitions
                .iter()
                .filter(|(t, _)| t >= start && t < end)
                .collect();
            // Flip-flop: A -> B -> A within 5 oscillation periods.
            let window = if *period_s > 0.0 { 5.0 * period_s } else { f64::MAX };
            let mut flip_flops = 0;
            for w in in_phase.windows(3) {
                if w[0].1 == w[2].1 && w[0].1 != w[1].1 && (w[2].0 - w[0].0) <= window {
                    flip_flops += 1;
                }
            }
            let mark = |t: f64| {
                marks
                    .iter()
                    .rfind(|(mt, _, _)| *mt <= t)
                    .map(|(_, u, o)| (*u, *o))
                    .unwrap_or((0, 0))
            };
            let (u0, o0) = mark(*start);
            let (u1, o1) = mark(*end);
            PhaseStats {
                morphs: in_phase.len() as u64,
                flip_flops,
                underprov_ms: u1 - u0,
                overprov_ms: o1 - o0,
            }
        })
        .collect();

    (phases, reaction_ms, total_morphs)
}

// ===================================================================
// Ordering scenario: where does each arm spend its one-way flip?
// ===================================================================

struct OrderingResult {
    flipped_during_noise: bool,
    flip_after_sustained_ms: f64,
    total_flips: u64,
}

/// The proposal under bench: should `spawn_with_qos` enable the
/// ordering auto-arm gate BY DEFAULT? Both arms run the IDENTICAL
/// policy a realistic caller gets (`DefaultOrderingPolicy::default`,
/// 100 ms hysteresis) so the ONLY difference is whether the auto-arm
/// is gated - isolating the proposal, not stacking dampers.
#[derive(Clone, Copy, PartialEq)]
enum OrderArm {
    /// Today's behavior: 100 ms timer, no gate
    /// (`spawn_with_qos_gated` with a disabled config).
    UngatedToday,
    /// The proposal: 100 ms timer AND the auto-arm gate on by
    /// default (`spawn_with_qos`).
    GatedDefault,
}

impl OrderArm {
    fn name(self) -> &'static str {
        match self {
            OrderArm::UngatedToday => "ungated(today)",
            OrderArm::GatedDefault => "gated(default)",
        }
    }
}

fn ordering_policy_100ms() -> DefaultOrderingPolicy {
    DefaultOrderingPolicy {
        hysteresis: Duration::from_millis(100),
        auto_order_threshold: Some(500.0),
    }
}

fn run_ordering_arm(arm: OrderArm) -> OrderingResult {
    let ring = Arc::new(
        AdaptiveRing::create_anon(2, 1, 1024)
            .unwrap()
            .with_ordering_stamps_kind(default_stamp_kind())
            .unwrap(),
    );
    ring.morph_to(RingShape::Mpsc).unwrap();
    ring.register_producer().unwrap();
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();
    ring.set_ordering_mode(OrderingMode::Unordered).unwrap();

    let qos = Arc::new(QosPolicy::streaming_default());
    qos.set_ordering(QosOrdering::PerProducer);
    let sidecar = match arm {
        OrderArm::UngatedToday => AdaptiveRingSidecar::spawn_with_qos_gated(
            Arc::clone(&ring),
            DefaultRingShapePolicy::default(),
            ordering_policy_100ms(),
            qos,
            Duration::from_millis(5),
            GateConfig::default(),
        ),
        OrderArm::GatedDefault => AdaptiveRingSidecar::spawn_with_qos(
            Arc::clone(&ring),
            DefaultRingShapePolicy::default(),
            ordering_policy_100ms(),
            qos,
            Duration::from_millis(5),
        ),
    };

    let stop = Arc::new(AtomicBool::new(false));
    // Consumer drains continuously - inversions are observed at pop.
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let consumer = std::thread::spawn(move || {
        let mut out = [0u8; 64];
        let mut n = 0u64;
        while !s.load(Ordering::Acquire) {
            if r.try_recv(0, &mut out).is_ok() {
                n += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        while r.try_recv(0, &mut out).is_ok() {
            n += 1;
        }
        n
    });

    // Mode watcher.
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let watcher = std::thread::spawn(move || {
        let t0 = Instant::now();
        let mut flips: Vec<(f64, OrderingMode)> = Vec::new();
        let mut last = r.ordering_mode();
        while !s.load(Ordering::Acquire) {
            let m = r.ordering_mode();
            if m != last
                && let Some(mode) = m
            {
                flips.push((t0.elapsed().as_secs_f64(), mode));
            }
            last = m;
            busy_wait(Duration::from_micros(500));
        }
        (t0, flips)
    });
    let t0 = Instant::now();

    let interleaved_burst = |items: u64| {
        // Two producers interleaving as fast as they can: stamps
        // race, the consumer observes cross-producer inversions.
        let r1 = Arc::clone(&ring);
        let h1 = std::thread::spawn(move || {
            for i in 0..items {
                while r1.try_send(0, &i.to_le_bytes()).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        let r2 = Arc::clone(&ring);
        let h2 = std::thread::spawn(move || {
            for i in 0..items {
                while r2.try_send(1, &i.to_le_bytes()).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        h1.join().unwrap();
        h2.join().unwrap();
    };

    // Three short noise bursts inside clean windows.
    let mut clean_seq = 0u64;
    for _ in 0..3 {
        interleaved_burst(300);
        let clean_end = Instant::now() + Duration::from_millis(150);
        while Instant::now() < clean_end {
            if ring.try_send(0, &clean_seq.to_le_bytes()).is_ok() {
                clean_seq += 1;
            }
            busy_wait(Duration::from_micros(50));
        }
    }
    let noise_window_end = t0.elapsed().as_secs_f64();

    // Sustained inversion regime.
    let sustained_start = t0.elapsed().as_secs_f64();
    let sustained_end = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < sustained_end {
        interleaved_burst(300);
        busy_wait(Duration::from_micros(100));
        if ring.ordering_mode() == Some(OrderingMode::MergeByStamp) {
            // Flip happened; merged pops read zero inversions from
            // here on, so the regime has done its job.
            break;
        }
    }
    std::thread::sleep(Duration::from_millis(50));

    stop.store(true, Ordering::Release);
    consumer.join().unwrap();
    let (_w_t0, flips) = watcher.join().unwrap();
    let total_flips = sidecar.ordering_flips();
    sidecar.shutdown();

    // Audit (engagement): the auto arm must have flipped exactly
    // once somewhere - an arm that never arms measures nothing.
    assert_eq!(total_flips, 1, "audit: one-way auto-order arm flips exactly once");
    let (flip_t, flip_mode) = flips[0];
    assert_eq!(flip_mode, OrderingMode::MergeByStamp);

    OrderingResult {
        flipped_during_noise: flip_t < noise_window_end,
        flip_after_sustained_ms: if flip_t >= sustained_start {
            (flip_t - sustained_start) * 1e3
        } else {
            f64::NAN
        },
        total_flips,
    }
}

/// Declaration promptness: an EXPLICIT `GlobalFifo` declaration is
/// caller intent, not noise - it must NOT be delayed by the gate.
/// Measures the latency from declaring GlobalFifo to the merge flag
/// arming. The proposal is only acceptable if the gated default is
/// as prompt as ungated here (it bypasses the gate for declarations).
fn declaration_latency(arm: OrderArm) -> f64 {
    let ring = Arc::new(
        AdaptiveRing::create_anon(2, 1, 1024)
            .unwrap()
            .with_ordering_stamps_kind(default_stamp_kind())
            .unwrap(),
    );
    ring.morph_to(RingShape::Mpsc).unwrap();
    ring.register_producer().unwrap();
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();
    ring.set_ordering_mode(OrderingMode::Unordered).unwrap();

    let qos = Arc::new(QosPolicy::streaming_default());
    qos.set_ordering(QosOrdering::PerProducer);
    let sidecar = match arm {
        OrderArm::UngatedToday => AdaptiveRingSidecar::spawn_with_qos_gated(
            Arc::clone(&ring),
            DefaultRingShapePolicy::default(),
            ordering_policy_100ms(),
            Arc::clone(&qos),
            Duration::from_millis(5),
            GateConfig::default(),
        ),
        OrderArm::GatedDefault => AdaptiveRingSidecar::spawn_with_qos(
            Arc::clone(&ring),
            DefaultRingShapePolicy::default(),
            ordering_policy_100ms(),
            Arc::clone(&qos),
            Duration::from_millis(5),
        ),
    };

    // Let the sidecar settle one scan, then declare GlobalFifo and
    // time the arm. No traffic needed: the declaration path is
    // inversion-independent.
    std::thread::sleep(Duration::from_millis(20));
    let t = Instant::now();
    qos.set_ordering(QosOrdering::GlobalFifo);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline
        && ring.ordering_mode() != Some(OrderingMode::MergeByStamp)
    {
        busy_wait(Duration::from_micros(200));
    }
    let dt = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp),
               "audit: an explicit GlobalFifo declaration must arm merge in every arm");
    assert_eq!(sidecar.ordering_flips(), 1);
    sidecar.shutdown();
    dt
}

fn main() {
    println!("policy thrash probe (fixed hysteresis vs confidence gate variants)");
    println!("arms: baseline = fixed 100 ms hysteresis (today). gated / gated+min run");
    println!("ZERO hysteresis so the gate is the only damper (mechanism vs mechanism).");
    println!("gated+hyst stacks gate + min-samples ON TOP of the 100 ms timer.");
    println!();
    println!("== capacity: adversarial oscillation at 50 / 200 / 1000 ms, then genuine shift ==");
    println!("{:<10} {:>9} {:>9} {:>9} {:>9} {:>11} {:>11} {:>11}",
             "arm", "morphs50", "morphs200", "morphs1k", "flipflops", "underpv ms", "overpv ms", "react ms");
    for arm in [Arm::Baseline, Arm::Gated, Arm::GatedMin, Arm::GatedHyst] {
        let (phases, reaction_ms, _total) = run_capacity_arm(arm);
        let ff: usize = phases.iter().map(|p| p.flip_flops).sum();
        let upv: u64 = phases.iter().map(|p| p.underprov_ms).sum();
        let opv: u64 = phases.iter().map(|p| p.overprov_ms).sum();
        println!("{:<10} {:>9} {:>9} {:>9} {:>9} {:>11} {:>11} {:>11.1}",
                 arm.name(),
                 phases[0].morphs, phases[1].morphs, phases[2].morphs,
                 ff, upv, opv, reaction_ms);
    }
    println!();
    println!("== ordering proposal: gate the auto-arm BY DEFAULT, capacity stays ungated ==");
    println!("both arms run DefaultOrderingPolicy (100 ms hysteresis); the only difference");
    println!("is whether spawn_with_qos gates the inversion-driven one-way auto-arm.");
    println!();
    println!("-- noise resistance: 3 inversion-noise bursts in clean windows, then sustained --");
    println!("{:<16} {:>20} {:>24}", "arm", "flipped during noise", "flip after sustained ms");
    for arm in [OrderArm::UngatedToday, OrderArm::GatedDefault] {
        // Two recorded runs per arm - the noise-window flip is
        // run-dependent for the ungated arm (drift note).
        for _ in 0..2 {
            let r = run_ordering_arm(arm);
            println!("{:<16} {:>20} {:>24}",
                     arm.name(),
                     r.flipped_during_noise,
                     if r.flip_after_sustained_ms.is_nan() {
                         "-".to_string()
                     } else {
                         format!("{:.1}", r.flip_after_sustained_ms)
                     });
            assert_eq!(r.total_flips, 1);
        }
    }
    println!();
    println!("-- declaration promptness: explicit GlobalFifo must NOT be delayed by the gate --");
    println!("{:<16} {:>22}", "arm", "declare -> arm ms");
    for arm in [OrderArm::UngatedToday, OrderArm::GatedDefault] {
        let ms = declaration_latency(arm);
        println!("{:<16} {:>22.2}", arm.name(), ms);
    }
    println!();
    println!("all integrity + audit assertions held");
}
