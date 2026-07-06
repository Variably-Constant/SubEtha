//! Gilbert-Elliott burst-loss model fitted online from the loss trace, giving
//! a REAL mean burst length instead of a jitter-ratio heuristic.
//!
//! Wireless loss is bursty - a fade or a collision drops several frames in a
//! row - so the loss process is well modelled by a two-state Markov chain: a
//! Good state (no loss) and a Bad state (loss), with `p = P(Good -> Bad)` and
//! `r = P(Bad -> Good)`. Fitting `(p, r)` from the observed loss sequence
//! yields the mean burst length `1 / r` (the interleave depth needed to spread
//! a burst across blocks so FEC recovers it) and the steady-state loss
//! `p / (p + r)`.
//!
//! The fit uses Gilbert's moment method on two statistics maintained online:
//! the marginal loss rate `pi_B = E[X]` and the lag-1 autocorrelation of the
//! loss indicator `rho1`. For the two-state chain the second eigenvalue is
//! `1 - p - r`, which equals `rho1`, so:
//!
//! ```text
//!   pi_B = p / (p + r)            (marginal loss rate)
//!   rho1 = 1 - p - r              (lag-1 autocorrelation)
//!   => p + r = 1 - rho1
//!   => p = pi_B * (1 - rho1)
//!   => r = (1 - pi_B) * (1 - rho1)
//!   => mean burst length = 1 / r = 1 / [(1 - pi_B)(1 - rho1)]
//! ```
//!
//! Independent loss (`rho1 = 0`) gives a mean burst near 1 (single drops);
//! correlated loss (`rho1 -> 1`) gives a long mean burst - exactly the signal
//! the interleaver needs.

/// Minimum samples before the fit is trusted - the autocorrelation needs a
/// population, and a handful of losses do not pin down `(p, r)`.
const MIN_SAMPLES: u64 = 100;

/// Minimum loss events before the fit is trusted - autocorrelation of an
/// almost-all-zero trace is dominated by noise.
const MIN_LOSSES: u64 = 8;

/// Cap on the reported mean burst length (the interleaver clamps to 16 anyway;
/// this just keeps a near-degenerate fit from returning a huge number).
const MAX_MEAN_BURST: f64 = 64.0;

/// Online Gilbert-Elliott fit from the binary loss trace.
#[derive(Debug, Clone, Default)]
pub struct BurstModel {
    n: u64,
    losses: u64,
    /// Count of adjacent samples that were BOTH losses (for `E[X_t X_{t+1}]`).
    pairs: u64,
    have_prev: bool,
    prev_lost: bool,
}

impl BurstModel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one loss-trace sample: `true` = the packet/shard was lost.
    pub fn observe(&mut self, lost: bool) {
        self.n += 1;
        if lost {
            self.losses += 1;
            if self.have_prev && self.prev_lost {
                self.pairs += 1;
            }
        }
        self.have_prev = true;
        self.prev_lost = lost;
    }

    /// Marginal loss rate `pi_B = E[X]`.
    fn loss_rate(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.losses as f64 / self.n as f64
        }
    }

    /// Lag-1 autocorrelation of the loss indicator, or `None` if undefined
    /// (degenerate loss rate). Clamped to `0..1`: negative correlation is
    /// treated as independent (`0`), since the model has no anti-burst regime.
    fn lag1_autocorr(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        let pi = self.loss_rate();
        let var = pi * (1.0 - pi);
        if var <= 0.0 {
            return None;
        }
        let exx = self.pairs as f64 / (self.n - 1) as f64;
        Some(((exx - pi * pi) / var).clamp(0.0, 0.999))
    }

    /// Fitted `(p, r)` transition probabilities, or `None` before a trustworthy
    /// fit.
    pub fn fit(&self) -> Option<(f64, f64)> {
        if self.n < MIN_SAMPLES || self.losses < MIN_LOSSES {
            return None;
        }
        let pi = self.loss_rate();
        if pi <= 0.0 || pi >= 1.0 {
            return None;
        }
        let rho1 = self.lag1_autocorr()?;
        let one_minus = 1.0 - rho1;
        let p = pi * one_minus;
        let r = (1.0 - pi) * one_minus;
        if r <= 0.0 {
            return None;
        }
        Some((p, r))
    }

    /// Mean burst length `1 / r` (consecutive losses), or `None` before a fit.
    pub fn mean_burst_len(&self) -> Option<f64> {
        self.fit().map(|(_p, r)| (1.0 / r).clamp(1.0, MAX_MEAN_BURST))
    }

    /// Steady-state loss `p / (p + r)` (equals the marginal loss rate by
    /// construction), or `None` before a fit.
    pub fn steady_loss(&self) -> Option<f64> {
        self.fit().map(|(p, r)| p / (p + r))
    }

    /// Samples folded so far (diagnostics).
    pub fn samples(&self) -> u64 {
        self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive the model with a synthetic two-state Markov trace of known
    /// `(p, r)` and confirm the fit recovers the mean burst length within
    /// tolerance. A simple xorshift keeps it deterministic without `rand`.
    fn run_gilbert(p: f64, r: f64, n: u64, seed: u64) -> BurstModel {
        let mut m = BurstModel::new();
        let mut state_bad = false;
        let mut x = seed | 1;
        let mut next = || {
            // xorshift64 -> uniform in [0, 1).
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 11) as f64 / (1u64 << 53) as f64
        };
        for _ in 0..n {
            if state_bad {
                m.observe(true);
                if next() < r {
                    state_bad = false;
                }
            } else {
                m.observe(false);
                if next() < p {
                    state_bad = true;
                }
            }
        }
        m
    }

    #[test]
    fn recovers_bursty_mean_length() {
        // p = 0.02 (rarely enter a burst), r = 0.2 (bursts ~5 long).
        let m = run_gilbert(0.02, 0.2, 200_000, 0x1234_5678);
        let mean = m.mean_burst_len().expect("fitted");
        assert!(
            (3.5..=7.0).contains(&mean),
            "mean burst {mean} should recover ~5 (1/r = 1/0.2)"
        );
    }

    #[test]
    fn independent_loss_has_unit_burst() {
        // p = r path of an independent Bernoulli(0.1): enter and leave the bad
        // state at the same rate, so bursts are ~1 (no correlation).
        let m = run_gilbert(0.1, 0.9, 200_000, 0xdead_beef);
        let mean = m.mean_burst_len().expect("fitted");
        assert!(
            mean < 1.6,
            "independent loss mean burst {mean} should be near 1"
        );
    }

    #[test]
    fn distinguishes_bursty_from_independent() {
        let bursty = run_gilbert(0.02, 0.2, 200_000, 1).mean_burst_len().unwrap();
        let indep = run_gilbert(0.1, 0.9, 200_000, 2).mean_burst_len().unwrap();
        assert!(
            bursty > indep * 2.0,
            "bursty {bursty} must clearly exceed independent {indep}"
        );
    }

    #[test]
    fn withholds_before_enough_data() {
        let mut m = BurstModel::new();
        for _ in 0..50 {
            m.observe(false);
        }
        m.observe(true);
        assert!(m.mean_burst_len().is_none(), "withheld before MIN_SAMPLES / MIN_LOSSES");
    }
}
