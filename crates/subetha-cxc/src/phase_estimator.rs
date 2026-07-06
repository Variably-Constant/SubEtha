//! Consumer-local arrival-phase estimator for predictive waiting.
//!
//! A blocking consumer normally parks on a doorbell and pays a
//! park/wake syscall round-trip per item. When the producer arrives
//! on a regular cadence, the consumer can instead PREDICT the next
//! arrival and spin through a small guard band at exactly that
//! moment, catching the item by polling and skipping the syscall.
//! [`PhaseEstimator`] is the prediction: it tracks the inter-arrival
//! period (EWMA) and its coefficient of variation, and engages only
//! when the cadence is regular enough that prediction beats the
//! doorbell.
//!
//! Entirely consumer-local: one struct on the consumer's stack, no
//! shared state, no atomics, O(1) per observed arrival. It is fed the
//! arrival timestamps the consumer already has (the `Instant` at each
//! successful pop), so it adds no new shared fields to the ring.
//!
//! The estimator is built on a monotonic [`Instant`] clock, so the
//! TSC-wraparound hazard of a raw-counter estimator does not arise.

use std::time::{Duration, Instant};

/// Tuning for a [`PhaseEstimator`].
#[derive(Debug, Clone, Copy)]
pub struct PhaseConfig {
    /// EWMA weight for the period and CV updates (higher = faster
    /// adaptation, noisier estimate).
    pub alpha: f64,
    /// Engage prediction once the CV drops below this AND the
    /// minimum sample count is met.
    pub cv_engage: f64,
    /// Disengage once the CV rises above this (hysteresis: strictly
    /// greater than `cv_engage` so the mode cannot flap at one
    /// threshold).
    pub cv_disengage: f64,
    /// Minimum observed inter-arrivals before prediction is eligible.
    pub min_samples: u64,
}

impl Default for PhaseConfig {
    fn default() -> Self {
        Self {
            alpha: 0.2,
            cv_engage: 0.25,
            cv_disengage: 0.40,
            min_samples: 16,
        }
    }
}

/// Tracks the producer's arrival cadence from the consumer side.
pub struct PhaseEstimator {
    cfg: PhaseConfig,
    /// EWMA inter-arrival period, in nanoseconds. `None` until the
    /// first delta is observed.
    period_ns: Option<f64>,
    /// EWMA of the relative period error `|delta - period| / period`
    /// - the coefficient of variation.
    cv: f64,
    /// Timestamp of the most recent arrival.
    last_arrival: Option<Instant>,
    /// Number of inter-arrivals (deltas) observed.
    samples: u64,
    engaged: bool,
}

impl PhaseEstimator {
    pub fn new(cfg: PhaseConfig) -> Self {
        Self {
            cfg,
            period_ns: None,
            cv: 1.0,
            last_arrival: None,
            samples: 0,
            engaged: false,
        }
    }

    /// Record an arrival observed at `now`. O(1); updates the period
    /// EWMA, the CV, the sample count, and the engaged state (with
    /// hysteresis).
    pub fn record(&mut self, now: Instant) {
        if let Some(last) = self.last_arrival {
            let delta_ns = now.saturating_duration_since(last).as_nanos() as f64;
            match self.period_ns {
                Some(p) if p > 0.0 => {
                    let rel_err = (delta_ns - p).abs() / p;
                    self.cv = (1.0 - self.cfg.alpha) * self.cv + self.cfg.alpha * rel_err;
                    self.period_ns =
                        Some((1.0 - self.cfg.alpha) * p + self.cfg.alpha * delta_ns);
                }
                _ => {
                    // First delta seeds the period; CV stays at its
                    // pessimistic initial value until a second delta
                    // gives a comparison.
                    self.period_ns = Some(delta_ns);
                }
            }
            self.samples += 1;
            self.update_engaged();
        }
        self.last_arrival = Some(now);
    }

    fn update_engaged(&mut self) {
        if self.engaged {
            if self.cv > self.cfg.cv_disengage || self.samples < self.cfg.min_samples {
                self.engaged = false;
            }
        } else if self.cv < self.cfg.cv_engage && self.samples >= self.cfg.min_samples {
            self.engaged = true;
        }
    }

    /// Predicted timestamp of the next arrival, or `None` if no
    /// period is known yet.
    pub fn predict_next(&self) -> Option<Instant> {
        match (self.last_arrival, self.period_ns) {
            (Some(last), Some(p)) if p > 0.0 => {
                Some(last + Duration::from_nanos(p as u64))
            }
            _ => None,
        }
    }

    /// Whether prediction is currently engaged (regular enough
    /// cadence, enough samples).
    pub fn engaged(&self) -> bool {
        self.engaged
    }

    /// Current EWMA period estimate.
    pub fn period(&self) -> Option<Duration> {
        self.period_ns.map(|p| Duration::from_nanos(p as u64))
    }

    /// Current coefficient-of-variation estimate.
    pub fn cv(&self) -> f64 {
        self.cv
    }

    /// Number of inter-arrivals observed.
    pub fn samples(&self) -> u64 {
        self.samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed a synthetic arrival series built off one base instant.
    fn feed(est: &mut PhaseEstimator, base: Instant, offsets_ns: &[u64]) {
        for &off in offsets_ns {
            est.record(base + Duration::from_nanos(off));
        }
    }

    #[test]
    fn perfect_period_converges_and_engages() {
        let mut est = PhaseEstimator::new(PhaseConfig::default());
        let base = Instant::now();
        let offsets: Vec<u64> = (0..40).map(|i| i * 10_000).collect(); // exact 10us
        feed(&mut est, base, &offsets);
        assert!(est.engaged(), "a perfectly periodic series must engage");
        let p = est.period().unwrap().as_nanos() as f64;
        assert!((p - 10_000.0).abs() / 10_000.0 < 0.05,
                "period within 5% of 10us, got {p}ns");
        assert!(est.cv() < 0.05, "CV must be near zero, got {}", est.cv());
    }

    #[test]
    fn mild_jitter_still_engages_within_5pct() {
        let mut est = PhaseEstimator::new(PhaseConfig::default());
        let base = Instant::now();
        // 10us period with deterministic +/-5% jitter (CV ~ 0.05).
        let mut t = 0u64;
        let mut offsets = Vec::new();
        for i in 0..64u64 {
            let jit = if i % 2 == 0 { 9_500 } else { 10_500 };
            t += jit;
            offsets.push(t);
        }
        feed(&mut est, base, &offsets);
        assert!(est.engaged(), "mild jitter must still engage");
        let p = est.period().unwrap().as_nanos() as f64;
        assert!((p - 10_000.0).abs() / 10_000.0 < 0.05,
                "period within 5%, got {p}ns");
    }

    #[test]
    fn high_variance_does_not_engage() {
        let mut est = PhaseEstimator::new(PhaseConfig::default());
        let base = Instant::now();
        // Alternating 2us / 18us = mean 10us, CV ~ 0.8.
        let mut t = 0u64;
        let mut offsets = Vec::new();
        for i in 0..64u64 {
            t += if i % 2 == 0 { 2_000 } else { 18_000 };
            offsets.push(t);
        }
        feed(&mut est, base, &offsets);
        assert!(!est.engaged(), "high-variance cadence must not engage");
    }

    #[test]
    fn disengages_when_cadence_breaks_down() {
        let mut est = PhaseEstimator::new(PhaseConfig::default());
        let base = Instant::now();
        // First settle into a clean 10us cadence...
        let clean: Vec<u64> = (0..40).map(|i| i * 10_000).collect();
        feed(&mut est, base, &clean);
        assert!(est.engaged());
        // ...then a burst of wild jitter must disengage (hysteresis
        // means it takes a few samples, not one).
        let mut t = 40 * 10_000u64;
        let mut chaos = Vec::new();
        for i in 0..40u64 {
            t += if i % 2 == 0 { 1_000 } else { 30_000 };
            chaos.push(t);
        }
        feed(&mut est, base, &chaos);
        assert!(!est.engaged(), "broken cadence must disengage");
    }

    #[test]
    fn does_not_engage_before_min_samples() {
        let cfg = PhaseConfig { min_samples: 20, ..PhaseConfig::default() };
        let mut est = PhaseEstimator::new(cfg);
        let base = Instant::now();
        let few: Vec<u64> = (0..10).map(|i| i * 10_000).collect();
        feed(&mut est, base, &few);
        assert!(!est.engaged(), "must not engage before min_samples deltas");
        assert!(est.samples() < 20);
    }

    #[test]
    fn predict_next_is_last_plus_period() {
        let mut est = PhaseEstimator::new(PhaseConfig::default());
        let base = Instant::now();
        let offsets: Vec<u64> = (0..40).map(|i| i * 10_000).collect();
        feed(&mut est, base, &offsets);
        let last = base + Duration::from_nanos(39 * 10_000);
        let predicted = est.predict_next().unwrap();
        let expected = last + est.period().unwrap();
        let diff = predicted.saturating_duration_since(expected)
            + expected.saturating_duration_since(predicted);
        assert!(diff < Duration::from_nanos(100), "prediction = last + period");
    }
}
