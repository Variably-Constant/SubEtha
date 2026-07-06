//! Warm-backing pre-allocation vs cold capacity morphs, A/B at
//! identical workloads.
//!
//! Phase A - morph wall latency up the capacity ladder, cold vs
//!           warm, at all three locales (anon / file / shmfs).
//! Phase B - saturated mid-burst grow: producer stall and burst
//!           completion time, cold vs warm, anon + file.
//! Phase C - sidecar prediction hit rate under ramped load cycles
//!           with the default policy's trend bands.
//!
//! Built-in bench audit (asserted, not just printed):
//!   engagement - every warm arm asserts its morph consumed the
//!                prewarmed backing (warm_hits delta == 1); every
//!                cold arm asserts zero warm hits.
//!   integrity  - every arm drains and sequence-checks its items.
//!   sizing     - Phase B asserts the ring is actually saturated
//!                (fill > 0.85) when the morph fires; the scenario
//!                is "grow under backpressure", not an idle swap.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::{
    CapacityAdaptiveRing, CapacityAdaptiveRingSidecar, CapacityPolicy,
    CapacityPolicyObservation, DefaultCapacityPolicy,
};

const PAYLOAD: usize = 56;

fn seq_payload(i: u64) -> [u8; PAYLOAD] {
    let mut p = [0u8; PAYLOAD];
    p[..8].copy_from_slice(&i.to_le_bytes());
    p
}

#[derive(Clone, Copy, PartialEq)]
enum Locale {
    Anon,
    File,
    Shm,
}

impl Locale {
    fn name(self) -> &'static str {
        match self {
            Locale::Anon => "anon",
            Locale::File => "file",
            Locale::Shm => "shmfs",
        }
    }
}

fn make_ring(locale: Locale, capacity: usize, uniq: u64) -> CapacityAdaptiveRing {
    match locale {
        Locale::Anon => CapacityAdaptiveRing::create_anon(1, 1, capacity).unwrap(),
        Locale::File => {
            let base = std::env::temp_dir()
                .join(format!("subetha_mlp_{}_{}", std::process::id(), uniq));
            CapacityAdaptiveRing::create(base, 1, 1, capacity).unwrap()
        }
        Locale::Shm => CapacityAdaptiveRing::create_shmfs(
            &format!("subetha_mlp_{}_{}", std::process::id(), uniq),
            1,
            1,
            capacity,
        )
        .unwrap(),
    }
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

/// Microsecond-accurate pacing. `thread::sleep` rounds to the
/// scheduler quantum on Windows (1-15 ms), which is useless for
/// per-item rates; spin against the monotonic clock instead.
fn busy_wait(d: Duration) {
    let t = Instant::now();
    while t.elapsed() < d {
        std::hint::spin_loop();
    }
}

// ===================================================================
// Phase A: ladder morph wall latency, cold vs warm, per locale.
// ===================================================================

fn phase_a_one(locale: Locale, from: usize, to: usize, warm: bool, uniq: u64) -> f64 {
    let ring = make_ring(locale, from, uniq);
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();
    for i in 0..32u64 {
        ring.try_send(0, &seq_payload(i)).unwrap();
    }
    if warm {
        ring.prewarm(to).unwrap();
        assert_eq!(ring.warm_capacity(), Some(to), "audit: prewarm must be cached");
    }
    let t = Instant::now();
    ring.morph_capacity_to(to).unwrap();
    let dt = t.elapsed().as_secs_f64() * 1e6;

    // Audit (engagement): warm arm consumed the prediction, cold
    // arm never touched the cache.
    assert_eq!(ring.warm_hits(), u64::from(warm), "audit: engagement");
    // Integrity: every in-flight item drains in send order.
    let mut out = [0u8; 64];
    for i in 0..32u64 {
        let n = ring.try_recv(0, &mut out).expect("in-flight item present");
        assert!(n >= 8);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), i, "integrity");
    }
    assert!(ring.try_recv(0, &mut out).is_err(), "integrity: exactly 32 items");
    dt
}

fn phase_a() {
    println!("== Phase A: morph wall latency (us), cold vs warm, best-of-5 median [min..max] ==");
    println!("{:<6} {:>7}->{:<7} {:>26} {:>26} {:>7}", "locale", "from", "to", "cold us", "warm us", "ratio");
    let mut uniq = 0u64;
    for locale in [Locale::Anon, Locale::File, Locale::Shm] {
        for (from, to) in [(256usize, 512usize), (512, 1024), (1024, 2048), (2048, 4096), (4096, 8192), (8192, 16384)] {
            let mut cold = Vec::new();
            let mut warm = Vec::new();
            // Interleave arms so page-cache / allocator state drifts
            // hit both equally.
            for _ in 0..5 {
                uniq += 1;
                cold.push(phase_a_one(locale, from, to, false, uniq));
                uniq += 1;
                warm.push(phase_a_one(locale, from, to, true, uniq));
            }
            let (cmin, cmax) = (cold.iter().cloned().fold(f64::MAX, f64::min), cold.iter().cloned().fold(0.0, f64::max));
            let (wmin, wmax) = (warm.iter().cloned().fold(f64::MAX, f64::min), warm.iter().cloned().fold(0.0, f64::max));
            let cmed = median(&mut cold);
            let wmed = median(&mut warm);
            println!(
                "{:<6} {:>7}->{:<7} {:>10.1} [{:>6.1}..{:>7.1}] {:>10.1} [{:>6.1}..{:>7.1}] {:>6.2}x",
                locale.name(), from, to, cmed, cmin, cmax, wmed, wmin, wmax, cmed / wmed,
            );
        }
    }
}

// ===================================================================
// Phase B: saturated mid-burst grow. Steady stream under capacity,
// then a burst floods the ring; the grow morph is the relief path.
// Cold pays allocation on the critical path; warm swaps immediately.
// ===================================================================

struct PhaseBResult {
    morph_us: f64,
    burst_ms: f64,
    stall_total_ms: f64,
    stall_max_ms: f64,
    fill_at_morph: f64,
}

fn phase_b_one(locale: Locale, warm: bool, uniq: u64) -> PhaseBResult {
    const FROM: usize = 8192;
    const TO: usize = 65536;
    const BURST: u64 = 120_000;

    let ring = Arc::new(make_ring(locale, FROM, uniq));
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let pushed = Arc::new(AtomicU64::new(0));
    let burst_go = Arc::new(AtomicBool::new(false));
    let burst_done_ms = Arc::new(AtomicU64::new(0));
    let stall_total_us = Arc::new(AtomicU64::new(0));
    let stall_max_us = Arc::new(AtomicU64::new(0));

    // Consumer: fixed service rate (~1 item/us) - fast enough to
    // keep the steady phase near-empty, far too slow to absorb the
    // burst without queueing depth. Verifies strict sequence.
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let consumer = std::thread::spawn(move || {
        let mut expect = 0u64;
        let mut out = [0u8; 64];
        loop {
            match r.try_recv(0, &mut out) {
                Ok(_) => {
                    let got = u64::from_le_bytes(out[..8].try_into().unwrap());
                    assert_eq!(got, expect, "integrity: strict SPSC sequence");
                    expect += 1;
                    for _ in 0..220 {
                        std::hint::spin_loop();
                    }
                }
                Err(_) => {
                    if s.load(Ordering::Acquire) {
                        return expect;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Producer: steady trickle, then the burst at full speed.
    let r = Arc::clone(&ring);
    let go = Arc::clone(&burst_go);
    let pu = Arc::clone(&pushed);
    let bdm = Arc::clone(&burst_done_ms);
    let stu = Arc::clone(&stall_total_us);
    let smu = Arc::clone(&stall_max_us);
    let producer = std::thread::spawn(move || {
        let mut i = 0u64;
        // Steady phase: ~200 items/ms, well under the consumer rate.
        while !go.load(Ordering::Acquire) {
            if r.try_send(0, &seq_payload(i)).is_ok() {
                i += 1;
                pu.store(i, Ordering::Release);
            }
            busy_wait(Duration::from_micros(5));
        }
        // Burst phase: BURST items at full speed; stalls timed.
        let burst_t0 = Instant::now();
        let end = i + BURST;
        while i < end {
            if r.try_send(0, &seq_payload(i)).is_ok() {
                i += 1;
                pu.store(i, Ordering::Release);
            } else {
                let st = Instant::now();
                while r.try_send(0, &seq_payload(i)).is_err() {
                    std::hint::spin_loop();
                }
                let stall = st.elapsed().as_micros() as u64;
                stu.fetch_add(stall, Ordering::Relaxed);
                smu.fetch_max(stall, Ordering::Relaxed);
                i += 1;
                pu.store(i, Ordering::Release);
            }
        }
        bdm.store(burst_t0.elapsed().as_micros() as u64, Ordering::Release);
    });

    // Steady phase 150 ms; warm arm pre-builds during it (off the
    // critical path, exactly where the sidecar's idle tick sits).
    std::thread::sleep(Duration::from_millis(120));
    if warm {
        ring.prewarm(TO).unwrap();
        assert_eq!(ring.warm_capacity(), Some(TO));
    }
    std::thread::sleep(Duration::from_millis(30));

    // Fire the burst, wait for saturation, then morph = relief.
    burst_go.store(true, Ordering::Release);
    let fill_at_morph;
    loop {
        let active = ring.ring_handle();
        let fill = active.approx_len() as f64 / active.total_slot_capacity() as f64;
        if fill > 0.85 {
            fill_at_morph = fill;
            break;
        }
        std::hint::spin_loop();
    }
    let t = Instant::now();
    ring.morph_capacity_to(TO).unwrap();
    let morph_us = t.elapsed().as_secs_f64() * 1e6;

    // Audit (engagement + sizing).
    assert_eq!(ring.warm_hits(), u64::from(warm), "audit: engagement");
    assert!(fill_at_morph > 0.85, "audit: sizing - morph must fire under saturation");

    producer.join().unwrap();
    // Let the consumer fully drain, then stop it.
    let total = pushed.load(Ordering::Acquire);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let active = ring.ring_handle();
        let state_empty = active.approx_len() == 0;
        if state_empty || Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Release);
    let consumed = consumer.join().unwrap();
    assert_eq!(consumed, total, "integrity: every pushed item consumed");

    PhaseBResult {
        morph_us,
        burst_ms: burst_done_ms.load(Ordering::Acquire) as f64 / 1e3,
        stall_total_ms: stall_total_us.load(Ordering::Acquire) as f64 / 1e3,
        stall_max_ms: stall_max_us.load(Ordering::Acquire) as f64 / 1e3,
        fill_at_morph,
    }
}

fn phase_b() {
    println!();
    println!("== Phase B: saturated burst grow 8192 -> 65536 (120k-item burst), 3 trials each ==");
    println!("{:<6} {:<5} {:>10} {:>10} {:>12} {:>10} {:>6}", "locale", "arm", "morph us", "burst ms", "stall tot ms", "stall max", "fill");
    let mut uniq = 10_000u64;
    for locale in [Locale::Anon, Locale::File] {
        for warm in [false, true] {
            for _ in 0..3 {
                uniq += 1;
                let r = phase_b_one(locale, warm, uniq);
                println!(
                    "{:<6} {:<5} {:>10.1} {:>10.2} {:>12.2} {:>10.2} {:>5.2}",
                    locale.name(),
                    if warm { "warm" } else { "cold" },
                    r.morph_us, r.burst_ms, r.stall_total_ms, r.stall_max_ms, r.fill_at_morph,
                );
            }
        }
    }
}

// ===================================================================
// Phase C: sidecar prediction hit rate under ramped load cycles.
// ===================================================================

fn phase_c() {
    println!();
    println!("== Phase C: sidecar prediction (DefaultCapacityPolicy bands), ramped cycles ==");
    let ring = Arc::new(CapacityAdaptiveRing::create_anon(1, 1, 256).unwrap());
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();
    let policy = DefaultCapacityPolicy {
        min_capacity: 64,
        max_capacity: 16384,
        ..DefaultCapacityPolicy::default()
    };
    // Sanity: the default policy's bands are live (predict ahead of
    // decide). One observation in the trend band must predict.
    assert_eq!(
        policy.predict(&CapacityPolicyObservation {
            current_capacity: 256,
            active_approx_len: 180,
            total_slot_capacity: 256,
            since_last_morph: Duration::ZERO,
        }),
        Some(512),
        "audit: trend band must be in front of the decide threshold",
    );
    let sidecar = CapacityAdaptiveRingSidecar::spawn(
        Arc::clone(&ring),
        policy,
        Duration::from_millis(5),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let pushed = Arc::new(AtomicU64::new(0));
    let popped = Arc::new(AtomicU64::new(0));
    let po = Arc::clone(&popped);
    let consumer = std::thread::spawn(move || {
        let mut out = [0u8; 64];
        while !s.load(Ordering::Acquire) {
            if r.try_recv(0, &mut out).is_ok() {
                po.fetch_add(1, Ordering::Relaxed);
                // Service rate ~20 items/ms: slower than the
                // producer's ramp so fill genuinely climbs.
                busy_wait(Duration::from_micros(50));
            } else {
                busy_wait(Duration::from_micros(50));
            }
        }
        // Final drain.
        while r.try_recv(0, &mut out).is_ok() {
            po.fetch_add(1, Ordering::Relaxed);
        }
    });

    // 8 ramp cycles: production ~25 items/ms vs consumption
    // ~20 items/ms = fill climbs ~5 items/ms - slow enough that
    // the trend band spans multiple 5 ms scan ticks (the
    // predictable trend the bands are built for). Then pause and
    // let the consumer drain back down so shrink trends fire too.
    let mut i = 0u64;
    for _cycle in 0..8 {
        let ramp_end = Instant::now() + Duration::from_millis(220);
        while Instant::now() < ramp_end {
            let active = ring.ring_handle();
            let fill = active.approx_len() as f64 / active.total_slot_capacity() as f64;
            drop(active);
            if fill < 0.92 && ring.try_send(0, &seq_payload(i)).is_ok() {
                i += 1;
                pushed.store(i, Ordering::Release);
            }
            busy_wait(Duration::from_micros(40));
        }
        std::thread::sleep(Duration::from_millis(350));
    }
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);
    consumer.join().unwrap();

    let morphs = sidecar.morphs_triggered();
    let prewarms = sidecar.prewarms_issued();
    let hits = ring.warm_hits();
    sidecar.shutdown();
    assert_eq!(popped.load(Ordering::Acquire), pushed.load(Ordering::Acquire),
               "integrity: every pushed item consumed across all morphs");

    println!("items pushed/consumed: {}", pushed.load(Ordering::Acquire));
    println!("morphs triggered:      {morphs}");
    println!("prewarms issued:       {prewarms}");
    println!("warm hits:             {hits}");
    if morphs > 0 {
        println!("prediction hit rate:   {:.0}%", 100.0 * hits as f64 / morphs as f64);
    }
    let held = ring.warm_capacity();
    println!(
        "warm slot at end:      {:?} (~{} KiB held speculatively)",
        held,
        held.map(|c| c * 64 / 1024).unwrap_or(0),
    );
    ring.clear_warm();
}

fn main() {
    println!("morph latency probe (warm-backing pre-allocation A/B)");
    println!("host note: absolute numbers drift with page-cache state; the");
    println!("cold/warm ratio and orderings are the signal.");
    println!();
    phase_a();
    phase_b();
    phase_c();
    println!();
    println!("all integrity + audit assertions held");
}
