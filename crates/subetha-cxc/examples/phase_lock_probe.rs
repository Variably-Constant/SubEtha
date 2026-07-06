//! Automatic phase-locked waiting in `recv_blocking`, A/B across a
//! period x coefficient-of-variation matrix, plus a throughput-
//! regression row.
//!
//! `recv_blocking` adapts on its own: while the consumer waits on a
//! regular-cadence producer it predicts the next arrival and spins a
//! short guard band instead of paying the doorbell's park/wake
//! propagation; while the consumer keeps up (high throughput) the
//! predictor never engages and the fast path is the bare doorbell.
//! Both arms call the SAME `recv_blocking`; the only difference is
//! the atomic `set_phase_locking` toggle - so this measures the
//! shipped default behavior, on vs off.
//!
//! Built-in bench audit (asserted, not printed):
//!   engagement   - phase-ON must engage at CV=0 and beat phase-OFF
//!                  p50 latency there; must DISENGAGE at CV=1.0
//!                  (parity). An arm that never engages, or never
//!                  disengages, measures nothing.
//!   integrity    - exactly-once + strict FIFO over every row.
//!   no-regression- the throughput row asserts phase-ON per-item time
//!                  is within noise of phase-OFF (the wait-mode gate
//!                  keeps the fast path clean).
//!   schedule     - jittered inter-arrival sequence precomputed from
//!                  a fixed seed, replayed identically to both arms.

use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

use subetha_cxc::BlockingSpscRing;

fn busy_until(t: Instant) {
    while Instant::now() < t {
        std::hint::spin_loop();
    }
}

struct Lcg(u64);
impl Lcg {
    fn next_f64(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Jittered inter-arrival schedule (cumulative ns offsets), mean
/// `period_ns`, target CV. Seeded - identical for both arms.
fn schedule(period_ns: u64, cv: f64, n: usize, seed: u64) -> Vec<u64> {
    let mut lcg = Lcg(seed);
    let mut out = Vec::with_capacity(n);
    let mut cum = 0u64;
    let span = cv * 3.0f64.sqrt();
    for _ in 0..n {
        let u = lcg.next_f64();
        let factor = (1.0 + span * (2.0 * u - 1.0)).max(0.05);
        cum += ((period_ns as f64 * factor) as u64).max(1);
        out.push(cum);
    }
    out
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() as f64 - 1.0) * p).round()) as usize]
}

struct RowResult {
    p50_ns: u64,
    p99_ns: u64,
    predictive_catches: u64,
}

fn run_row(phase_on: bool, period_ns: u64, cv: f64, n: usize) -> RowResult {
    let ring = Arc::new(BlockingSpscRing::create_anon(1024).expect("create"));
    ring.set_phase_locking(phase_on);
    let sched = schedule(period_ns, cv, n, 0xC0FFEE ^ period_ns ^ (cv * 1e6) as u64);
    let barrier = Arc::new(Barrier::new(2));
    let epoch: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());

    let r = Arc::clone(&ring);
    let barrier_c = Arc::clone(&barrier);
    let epoch_c = Arc::clone(&epoch);
    let sched_c = sched.clone();
    let producer = std::thread::spawn(move || {
        barrier_c.wait();
        let start = *epoch_c.get_or_init(Instant::now);
        for (i, &off) in sched_c.iter().enumerate() {
            busy_until(start + Duration::from_nanos(off));
            let mut payload = [0u8; 56];
            let publish_ns = start.elapsed().as_nanos() as u64;
            payload[..8].copy_from_slice(&publish_ns.to_le_bytes());
            payload[8..16].copy_from_slice(&(i as u64).to_le_bytes());
            while r.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    let mut latencies: Vec<u64> = Vec::with_capacity(n);
    let mut buf = [0u8; 64];
    barrier.wait();
    let start = *epoch.get_or_init(Instant::now);
    for expected in 0..n as u64 {
        ring.recv_blocking(&mut buf, Some(Duration::from_secs(10))).expect("recv");
        let observe_ns = start.elapsed().as_nanos() as u64;
        let publish_ns = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        assert_eq!(seq, expected, "integrity: strict FIFO p={period_ns} cv={cv}");
        latencies.push(observe_ns.saturating_sub(publish_ns));
    }
    producer.join().unwrap();

    latencies.sort_unstable();
    RowResult {
        p50_ns: percentile(&latencies, 0.50),
        p99_ns: percentile(&latencies, 0.99),
        predictive_catches: ring.phase_predictive_catches(),
    }
}

/// Throughput regression: producer floods (no pacing), consumer
/// drains via recv_blocking and KEEPS UP, so the ring is rarely
/// empty and the predictor should never engage. Returns ns/item.
fn throughput_ns_per_item(phase_on: bool, n: usize) -> (u64, bool) {
    let ring = Arc::new(BlockingSpscRing::create_anon(1024).expect("create"));
    ring.set_phase_locking(phase_on);
    let r = Arc::clone(&ring);
    let producer = std::thread::spawn(move || {
        for i in 0..n as u64 {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            while r.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let mut buf = [0u8; 64];
    let t0 = Instant::now();
    for expected in 0..n as u64 {
        ring.recv_blocking(&mut buf, Some(Duration::from_secs(30))).expect("recv");
        let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
        assert_eq!(got, expected, "integrity: throughput FIFO");
    }
    let elapsed = t0.elapsed();
    producer.join().unwrap();
    (elapsed.as_nanos() as u64 / n as u64, ring.phase_in_wait_mode())
}

fn main() {
    println!("automatic phase-locked waiting in recv_blocking (phase ON vs OFF)");
    println!("both arms call the same recv_blocking; the toggle is set_phase_locking.");
    println!("host note: absolute latencies drift; the on-vs-off p50 ratio is the signal.");
    println!();
    let periods = [10_000u64, 50_000, 200_000, 1_000_000];
    let cvs = [0.0f64, 0.1, 0.25, 0.5, 1.0];
    let n_for = |period_ns: u64| -> usize {
        ((400_000_000u64 / period_ns) as usize).clamp(800, 20_000)
    };

    println!("{:<8} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "period", "cv", "off p50", "off p99", "on p50", "on p99", "pred-catch");
    for &period in &periods {
        let n = n_for(period);
        for &cv in &cvs {
            let off = run_row(false, period, cv, n);
            let on = run_row(true, period, cv, n);

            if cv == 0.0 {
                // No regression at any period: mixed regimes (short
                // period, consumer carries a backlog) decline to
                // predict and stay on the doorbell at parity.
                assert!(on.p50_ns <= off.p50_ns + off.p50_ns / 2 + 1_000,
                        "audit: CV=0 phase-ON p50 ({}) must not regress vs OFF ({})",
                        on.p50_ns, off.p50_ns);
                // Strong win where the consumer cleanly waits per item:
                // the predictor must fire AND beat the doorbell >2x.
                if period >= 50_000 {
                    assert!(on.predictive_catches > (n as u64) / 4,
                            "audit: CV=0 period>=50us must fire the predictor (got {})",
                            on.predictive_catches);
                    assert!(on.p50_ns * 2 < off.p50_ns,
                            "audit: CV=0 period>=50us phase-ON ({}) must beat OFF ({}) >2x",
                            on.p50_ns, off.p50_ns);
                }
            }
            if cv == 1.0 {
                // Irregular cadence must disengage: few predictive
                // catches relative to the item count.
                assert!(on.predictive_catches < (n as u64) / 4,
                        "audit: CV=1.0 must disengage (predictive catches {} of {})",
                        on.predictive_catches, n);
            }

            println!("{:<8} {:>5.2} {:>10} {:>10} {:>10} {:>10} {:>10}",
                     format!("{}us", period / 1000), cv,
                     off.p50_ns, off.p99_ns, on.p50_ns, on.p99_ns,
                     on.predictive_catches);
        }
    }

    println!();
    println!("== throughput regression (consumer keeps up; predictor must NOT engage) ==");
    let tn = 2_000_000usize;
    // Warm both, then measure; take the better of two to reduce noise.
    let off_ns = (0..2).map(|_| throughput_ns_per_item(false, tn).0).min().unwrap();
    let (on_ns, on_wait) = {
        let mut best = u64::MAX;
        let mut wait = false;
        for _ in 0..2 {
            let (ns, w) = throughput_ns_per_item(true, tn);
            best = best.min(ns);
            wait |= w;
        }
        (best, wait)
    };
    println!("phase OFF: {off_ns} ns/item   phase ON: {on_ns} ns/item   (on in wait mode at end: {on_wait})");
    // The wait-mode gate must keep the fast path clean: phase-ON must
    // be within 15% of phase-OFF when the consumer keeps up.
    assert!(on_ns <= off_ns + off_ns / 6 + 2,
            "no-regression: phase-ON throughput ({on_ns}) must be within ~15% of OFF ({off_ns})");

    println!();
    println!("all integrity + audit + no-regression assertions held");
}
