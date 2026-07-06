//! Confidence gating for sidecar policy decisions.
//!
//! Fixed-interval hysteresis treats every policy recommendation the
//! same: wait out the cooldown, then act on the latest scan. Under
//! oscillating load that thrashes (each half-period legitimately
//! crosses a threshold), and under noisy signals it acts on
//! single-scan spikes.
//!
//! [`ConfidenceGate`] replaces the fixed timer with conviction
//! dynamics: a scalar `c in [floor, 1]` grows logistically
//! (`c += rate * c * (1 - c)`) each scan the policy repeats the
//! SAME recommendation, collapses multiplicatively (`c *= shock`)
//! when the recommendation changes or an external regime-shift
//! signal arrives, and the decision fires only when `c` crosses
//! `threshold` with at least `min_samples` consecutive agreeing
//! scans. After firing, conviction resets to the floor so the next
//! decision needs fresh evidence. The effective hysteresis adapts
//! to signal stability: a steady recommendation passes in a handful
//! of scans, an oscillating one never accumulates conviction at
//! all.
//!
//! The gate is generic over the recommendation type so one
//! implementation serves the capacity (usize), shape (RingShape),
//! ordering (OrderingMode), and locale (Locale) sidecars.

/// Tuning for a [`ConfidenceGate`]. The default is DISABLED, which
/// makes every gated sidecar reproduce the ungated behavior
/// exactly - enabling the gate is an explicit, per-spawn choice.
#[derive(Debug, Clone, Copy)]
pub struct GateConfig {
    /// Master switch. `false` = pass every recommendation through
    /// untouched (today's semantics).
    pub enabled: bool,
    /// Logistic growth rate per agreeing scan. At the default 0.9
    /// a recommendation must hold for 5 consecutive scans to carry
    /// conviction from the floor across the threshold.
    pub rate: f32,
    /// Multiplier applied to conviction on a recommendation change
    /// or an external regime-shift signal.
    pub shock: f32,
    /// Conviction level at which a held recommendation fires.
    pub threshold: f32,
    /// Lower clamp and post-fire reset for conviction. Nonzero
    /// because the logistic map's fixed point at 0 is absorbing -
    /// conviction parked at exactly 0 never grows again.
    pub floor: f32,
    /// Minimum consecutive agreeing scans before a decision is
    /// eligible regardless of conviction. 0 disables the sample
    /// gate. [`min_samples_for_arity`] derives the recommended
    /// floor from the decision's arity.
    pub min_samples: u32,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: 0.9,
            shock: 0.25,
            threshold: 0.7,
            floor: 0.05,
            min_samples: 0,
        }
    }
}

impl GateConfig {
    /// An enabled gate with default dynamics and no sample floor.
    pub fn enabled() -> Self {
        Self { enabled: true, ..Self::default() }
    }

    /// An enabled gate with the sample floor derived from the
    /// decision's arity via [`min_samples_for_arity`].
    pub fn enabled_with_arity(k: u32) -> Self {
        Self {
            enabled: true,
            min_samples: min_samples_for_arity(k),
            ..Self::default()
        }
    }
}

/// Recommended minimum sample count for a decision over a k-way
/// signal: `2 * ceil(log2(k))`, with k clamped to at least 2.
/// A binary decision needs 2 agreeing samples, a 3-or-4-way
/// decision 4, an 8-way decision 6.
pub fn min_samples_for_arity(k: u32) -> u32 {
    let k = k.max(2);
    let ceil_log2 = 32 - (k - 1).leading_zeros();
    2 * ceil_log2
}

/// Per-sidecar-loop conviction state. One instance per gated
/// decision axis; lives on the sidecar thread, no shared state.
pub struct ConfidenceGate<T: PartialEq + Copy> {
    cfg: GateConfig,
    c: f32,
    held: Option<T>,
    samples: u32,
}

impl<T: PartialEq + Copy> ConfidenceGate<T> {
    pub fn new(cfg: GateConfig) -> Self {
        Self { cfg, c: cfg.floor, held: None, samples: 0 }
    }

    /// Feed one scan's recommendation. Returns the decision the
    /// sidecar may act on this scan: the recommendation itself
    /// when the gate is disabled, otherwise only once conviction
    /// and the sample floor are both satisfied.
    pub fn observe(&mut self, recommendation: Option<T>) -> Option<T> {
        if !self.cfg.enabled {
            return recommendation;
        }
        match (recommendation, self.held) {
            (None, _) => {
                // Recommendation withdrawn: the signal no longer
                // supports acting. Collapse conviction; forget the
                // held target.
                self.c = (self.c * self.cfg.shock).max(self.cfg.floor);
                self.held = None;
                self.samples = 0;
                None
            }
            (Some(r), Some(h)) if r == h => {
                self.samples = self.samples.saturating_add(1);
                self.c = (self.c + self.cfg.rate * self.c * (1.0 - self.c)).min(1.0);
                if self.c >= self.cfg.threshold
                    && self.samples >= self.cfg.min_samples.max(1)
                {
                    // Fire, then demand fresh evidence for the
                    // next decision.
                    self.c = self.cfg.floor;
                    self.held = None;
                    self.samples = 0;
                    Some(r)
                } else {
                    None
                }
            }
            (Some(r), _) => {
                // New or REVERSED recommendation. A reversal is
                // direct evidence of oscillation - collapse
                // conviction and start counting for the new target.
                // First sighting never fires.
                self.c = (self.c * self.cfg.shock).max(self.cfg.floor);
                self.held = Some(r);
                self.samples = 1;
                None
            }
        }
    }

    /// External regime-shift signal (peer-count change, fill jump,
    /// inversion-rate discontinuity): collapse conviction so the
    /// gate demands fresh agreement under the new regime before
    /// acting.
    pub fn shock(&mut self) {
        if self.cfg.enabled {
            self.c = (self.c * self.cfg.shock).max(self.cfg.floor);
            self.samples = 0;
        }
    }

    /// Current conviction (observability).
    pub fn confidence(&self) -> f32 {
        self.c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_gate_is_passthrough() {
        let mut g: ConfidenceGate<u32> = ConfidenceGate::new(GateConfig::default());
        assert_eq!(g.observe(Some(7)), Some(7), "disabled = today's semantics");
        assert_eq!(g.observe(None), None);
        assert_eq!(g.observe(Some(9)), Some(9));
    }

    #[test]
    fn steady_recommendation_fires_after_logistic_crossing() {
        let mut g: ConfidenceGate<u32> = ConfidenceGate::new(GateConfig::enabled());
        let mut fired_at = None;
        for scan in 1..=20 {
            if g.observe(Some(512)).is_some() {
                fired_at = Some(scan);
                break;
            }
        }
        // floor 0.05, rate 0.9: 0.05 -> 0.093 -> 0.169 -> 0.295
        // -> 0.482 -> 0.707 >= threshold on the 6th observation
        // (first sighting resets, five agreements grow).
        assert_eq!(fired_at, Some(6),
                   "default dynamics fire on the 6th consecutive agreeing scan");
        // Post-fire: conviction reset; an immediate repeat must NOT
        // fire on the next scan.
        assert_eq!(g.observe(Some(512)), None,
                   "post-fire decisions need fresh conviction");
    }

    #[test]
    fn oscillating_recommendation_never_fires() {
        let mut g: ConfidenceGate<u32> = ConfidenceGate::new(GateConfig::enabled());
        for _ in 0..100 {
            assert_eq!(g.observe(Some(512)), None);
            assert_eq!(g.observe(Some(128)), None,
                       "each reversal collapses conviction - oscillation starves the gate");
        }
        assert!(g.confidence() < 0.2);
    }

    #[test]
    fn withdrawal_collapses_conviction() {
        let mut g: ConfidenceGate<u32> = ConfidenceGate::new(GateConfig::enabled());
        for _ in 0..4 {
            g.observe(Some(512));
        }
        let before = g.confidence();
        g.observe(None);
        assert!(g.confidence() < before * 0.5,
                "withdrawal must shock conviction down");
        // The previously-held target starts over.
        let mut fired = false;
        for _ in 0..3 {
            fired |= g.observe(Some(512)).is_some();
        }
        assert!(!fired, "post-withdrawal the target re-earns conviction from scratch");
    }

    #[test]
    fn external_shock_resets_progress() {
        let mut g: ConfidenceGate<u32> = ConfidenceGate::new(GateConfig::enabled());
        for _ in 0..4 {
            g.observe(Some(512));
        }
        g.shock();
        assert!(g.confidence() <= 0.2);
        assert_eq!(g.observe(Some(512)), None,
                   "agreement after a regime shift starts a fresh climb");
    }

    #[test]
    fn min_samples_gate_delays_even_full_conviction() {
        let cfg = GateConfig { min_samples: 10, ..GateConfig::enabled() };
        let mut g: ConfidenceGate<u32> = ConfidenceGate::new(cfg);
        let mut fired_at = None;
        for scan in 1..=20 {
            if g.observe(Some(512)).is_some() {
                fired_at = Some(scan);
                break;
            }
        }
        assert_eq!(fired_at, Some(10),
                   "the sample floor binds when it exceeds the conviction crossing");
    }

    #[test]
    fn arity_derived_sample_floors() {
        assert_eq!(min_samples_for_arity(2), 2);
        assert_eq!(min_samples_for_arity(3), 4);
        assert_eq!(min_samples_for_arity(4), 4);
        assert_eq!(min_samples_for_arity(8), 6);
        assert_eq!(min_samples_for_arity(64), 12);
        assert_eq!(min_samples_for_arity(0), 2, "arity clamps to binary");
    }
}
