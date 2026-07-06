//! Compound multi-axis morphs vs sequential single-axis morphs vs
//! build-beside-and-repatch, A/B/C at identical workloads.
//!
//! Scenario per trial: quiet SPSC steady state, then a burst that
//! needs BOTH more queueing depth (capacity x8) and more producers
//! (shape -> MPMC); after the transition, four producers flood the
//! ring and the consumer drains everything with per-producer FIFO
//! checks.
//!
//! Contenders for the same (capacity x8 + shape MPMC) transition:
//!   sequential - capacity morph, then the second policy's earliest
//!                legal action one hysteresis window later (100 ms,
//!                the default policy cooldown), then the in-place
//!                shape morph. The cadence IS the cost under test.
//!   compound   - one morph_to_config carrying both axes.
//!   repatch    - prewarm_config builds the full target during the
//!                steady phase; the morph consumes it = swap only.
//!
//! Built-in bench audit (asserted, not printed):
//!   engagement - compound/repatch assert ONE pin invalidation and
//!                repatch asserts the warm hit; sequential asserts
//!                its two distinct invalidation events.
//!   endpoint   - every arm asserts the identical final config
//!                (capacity 8192, shape Mpmc) before metrics count.
//!   integrity  - per-producer strict FIFO + exactly-once across
//!                the whole trial, every arm.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::RingShape;
use subetha_cxc::{BackingTarget, CapacityAdaptiveRing, RingConfig};

const PAYLOAD: usize = 56;
const BURST_PER_PRODUCER: u64 = 20_000;

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
    Sequential,
    Compound,
    Repatch,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Sequential => "sequential",
            Arm::Compound => "compound",
            Arm::Repatch => "repatch",
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Locale {
    Anon,
    File,
}

impl Locale {
    fn name(self) -> &'static str {
        match self {
            Locale::Anon => "anon",
            Locale::File => "file",
        }
    }
}

struct TrialResult {
    time_to_stable_ms: f64,
    prewarm_build_ms: f64,
    stale_pops: u64,
    cap_pin_invalidations: u64,
    shape_pin_invalidations: u64,
    burst_drain_ms: f64,
    items_total: u64,
}

fn run_trial(locale: Locale, arm: Arm, uniq: u64) -> TrialResult {
    let ring = Arc::new(match locale {
        Locale::Anon => CapacityAdaptiveRing::create_anon(4, 1, 1024).unwrap(),
        Locale::File => {
            let base = std::env::temp_dir()
                .join(format!("subetha_cmp_{}_{}", std::process::id(), uniq));
            CapacityAdaptiveRing::create(base, 4, 1, 1024).unwrap()
        }
    });
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let producers_done = Arc::new(AtomicU64::new(0));

    // Consumer: drains continuously, asserts strict per-producer
    // FIFO, counts everything.
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let consumer = std::thread::spawn(move || {
        let mut next_seq: HashMap<u16, u64> = HashMap::new();
        let mut total = 0u64;
        let mut out = [0u8; 64];
        loop {
            match r.try_recv(0, &mut out) {
                Ok(_) => {
                    let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                    let pid = u16::from_le_bytes(out[8..10].try_into().unwrap());
                    let want = next_seq.entry(pid).or_insert(0);
                    assert_eq!(seq, *want,
                               "integrity: per-producer strict FIFO (producer {pid})");
                    *want += 1;
                    total += 1;
                }
                Err(_) => {
                    if s.load(Ordering::Acquire) {
                        return (total, next_seq);
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Pin observer: holds a capacity pin + a shape pin on the
    // pinned backing; counts distinct invalidation events at each
    // level. The capacity level dominates (a swap re-pins both).
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let cap_inv = Arc::new(AtomicU64::new(0));
    let shape_inv = Arc::new(AtomicU64::new(0));
    let ci = Arc::clone(&cap_inv);
    let si = Arc::clone(&shape_inv);
    let observer = std::thread::spawn(move || {
        while !s.load(Ordering::Acquire) {
            let cap_pin = r.pin_current_capacity();
            let backing = Arc::clone(cap_pin.ring());
            let shape_pin = backing.pin_current_shape();
            loop {
                if s.load(Ordering::Acquire) {
                    return;
                }
                if !cap_pin.is_still_valid() {
                    ci.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                if !shape_pin.is_still_valid() {
                    si.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                busy_wait(Duration::from_micros(20));
            }
        }
    });

    // Producer 0: paced stream across the whole trial (~100/ms).
    let r = Arc::clone(&ring);
    let s = Arc::clone(&stop);
    let pd = Arc::clone(&producers_done);
    let producer0 = std::thread::spawn(move || {
        let mut seq = 0u64;
        while !s.load(Ordering::Acquire) {
            if r.try_send(0, &payload(0, seq)).is_ok() {
                seq += 1;
            }
            busy_wait(Duration::from_micros(10));
        }
        pd.fetch_add(seq, Ordering::Relaxed);
    });

    // Steady phase. The repatch arm builds its target here - on
    // sidecar-idle-equivalent time, off the morph lock.
    std::thread::sleep(Duration::from_millis(50));
    let target = RingConfig {
        shape: Some(RingShape::Mpmc),
        capacity: Some(8192),
        locale: None,
    };
    let mut prewarm_build_ms = 0.0;
    if arm == Arm::Repatch {
        let t = Instant::now();
        ring.prewarm_config(&target).unwrap();
        prewarm_build_ms = t.elapsed().as_secs_f64() * 1e3;
        assert_eq!(ring.warm_capacity(), Some(8192));
    }
    std::thread::sleep(Duration::from_millis(50));

    // The burst hits: transition per arm, timed.
    let t0 = Instant::now();
    match arm {
        Arm::Sequential => {
            ring.morph_capacity_to(8192).unwrap();
            // The second policy's earliest legal action arrives one
            // hysteresis window later (default policy cooldown).
            std::thread::sleep(Duration::from_millis(100));
            ring.ring_handle().morph_to(RingShape::Mpmc).unwrap();
        }
        Arm::Compound | Arm::Repatch => {
            ring.morph_to_config(&target).unwrap();
        }
    }
    let time_to_stable_ms = t0.elapsed().as_secs_f64() * 1e3;

    // Audit (endpoint equivalence + engagement).
    assert_eq!(ring.current_capacity(), 8192, "audit: endpoint capacity");
    assert_eq!(ring.ring_handle().current_shape(), RingShape::Mpmc,
               "audit: endpoint shape");
    assert_eq!(ring.warm_hits(), u64::from(arm == Arm::Repatch),
               "audit: engagement - only repatch consumes the warm slot");
    assert_eq!(ring.pin_generation(), 1,
               "audit: exactly one wrapper pin bump in every arm");

    // Burst: three more producers flood the MPMC ring.
    let mut burst_handles = Vec::new();
    for pid in 1..4u16 {
        ring.register_producer().unwrap();
        let r = Arc::clone(&ring);
        burst_handles.push(std::thread::spawn(move || {
            for seq in 0..BURST_PER_PRODUCER {
                while r.try_send(pid as usize, &payload(pid, seq)).is_err() {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    let burst_t0 = Instant::now();
    for h in burst_handles {
        h.join().unwrap();
    }
    // Wait for the consumer to drain everything in flight, then stop.
    let deadline = Instant::now() + Duration::from_secs(30);
    while ring.ring_handle().approx_len() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    std::thread::sleep(Duration::from_millis(20));
    let burst_drain_ms = burst_t0.elapsed().as_secs_f64() * 1e3;
    stop.store(true, Ordering::Release);
    producer0.join().unwrap();
    observer.join().unwrap();
    let (consumed, per_producer) = consumer.join().unwrap();

    // Integrity: exactly-once over every producer's full stream.
    let pushed0 = producers_done.load(Ordering::Relaxed);
    let expected = pushed0 + 3 * BURST_PER_PRODUCER;
    assert_eq!(consumed, expected, "integrity: exactly-once across the trial");
    for pid in 1..4u16 {
        assert_eq!(per_producer.get(&pid).copied().unwrap_or(0),
                   BURST_PER_PRODUCER,
                   "integrity: producer {pid} fully drained");
    }

    TrialResult {
        time_to_stable_ms,
        prewarm_build_ms,
        stale_pops: ring.stale_pops(),
        cap_pin_invalidations: cap_inv.load(Ordering::Relaxed),
        shape_pin_invalidations: shape_inv.load(Ordering::Relaxed),
        burst_drain_ms,
        items_total: consumed,
    }
}

fn three_axis_demonstration() {
    println!();
    println!("== three-axis transition (capacity x8 + shape MPMC + locale anon->file) ==");
    println!("(single-axis APIs cannot express a locale change on this wrapper at");
    println!(" all - the locale was construction-fixed; the compound config is the");
    println!(" first path that reaches this configuration)");
    let dir = std::env::temp_dir()
        .join(format!("subetha_cmp3_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ring = CapacityAdaptiveRing::create_anon(4, 1, 1024).unwrap();
    ring.register_producer().unwrap();
    ring.register_consumer().unwrap();
    for i in 0..100u64 {
        ring.try_send(0, &payload(0, i)).unwrap();
    }
    let t = Instant::now();
    ring.morph_to_config(&RingConfig {
        shape: Some(RingShape::Mpmc),
        capacity: Some(8192),
        locale: Some(BackingTarget::File(dir.join("triple"))),
    })
    .unwrap();
    let dt = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(ring.current_capacity(), 8192);
    assert_eq!(ring.ring_handle().current_shape(), RingShape::Mpmc);
    assert_eq!(ring.pin_generation(), 1);
    let mut out = [0u8; 64];
    let mut got = 0u64;
    while ring.try_recv(0, &mut out).is_ok() {
        let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
        assert_eq!(seq, got, "integrity: FIFO across the 3-axis transition");
        got += 1;
    }
    assert_eq!(got, 100);
    println!("3-axis transition: {dt:.2} ms, one pin bump, 100/100 items in order");
    drop(ring);
    drop(std::fs::remove_dir_all(&dir));
}

fn main() {
    println!("compound morph probe (sequential vs compound vs repatch A/B/C)");
    println!("host note: absolute numbers drift; ratios + orderings are the signal.");
    println!("sequential includes the 100 ms inter-policy cadence BY DESIGN - that");
    println!("cadence is the cost the compound transition eliminates.");
    println!();
    println!("{:<6} {:<10} {:>12} {:>10} {:>10} {:>8} {:>8} {:>10}",
             "locale", "arm", "stable ms", "build ms", "stale pop", "cap inv", "shp inv", "drain ms");
    let mut uniq = 0u64;
    for locale in [Locale::Anon, Locale::File] {
        for arm in [Arm::Sequential, Arm::Compound, Arm::Repatch] {
            for _ in 0..3 {
                uniq += 1;
                let r = run_trial(locale, arm, uniq);
                println!(
                    "{:<6} {:<10} {:>12.3} {:>10.3} {:>10} {:>8} {:>8} {:>10.2}",
                    locale.name(), arm.name(),
                    r.time_to_stable_ms, r.prewarm_build_ms, r.stale_pops,
                    r.cap_pin_invalidations, r.shape_pin_invalidations,
                    r.burst_drain_ms,
                );
                assert!(r.items_total > 0);
            }
        }
    }
    three_axis_demonstration();
    println!();
    println!("all integrity + audit assertions held");
}
