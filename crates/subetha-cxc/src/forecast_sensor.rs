//! Item 16: Sprout-style stochastic forecast of the deliverable rate.
//!
//! Sprout (Winstein, Sivaraman & Balakrishnan, NSDI 2013) treats a cellular /
//! variable bottleneck as a rate process with uncertainty and forecasts a
//! CONSERVATIVE lower bound on what it will deliver over the next tick, so a
//! sender can pre-size its window ahead of a dip rather than react after the loss
//! a dip causes. This is the receiver-side estimator: it is fed the delivered
//! bytes per measurement interval and tracks the rate with a one-dimensional
//! Kalman filter (the rate is a random walk under process noise `q`; each
//! observation is the rate plus measurement noise `r`), then forecasts the
//! 5th-percentile deliverable rate for the next tick as
//! `mean - 1.645 * sqrt(predicted variance)`, floored at zero.
//!
//! Unlike the passive BtlBw (item 6), which is a windowed-MAX of the PAST
//! delivery rate, this is a forward-looking, conservative LOWER bound: when the
//! observed rate jumps around, the filter's variance widens and the forecast
//! drops at once - leading the dip - so the controller arms protection before the
//! loss materialises. The noises scale with the current rate estimate, so one
//! filter spans a bottleneck that varies by an order of magnitude.

/// The one-sided z-score for a 5th-percentile (95 % one-sided) lower bound.
const Z_5TH: f64 = 1.645;
/// Process-noise fraction: the rate may drift this much per tick (a random
/// walk), so the forecast variance grows by `(Q_FRAC * rate)^2` each tick.
const Q_FRAC: f64 = 0.25;
/// Measurement-noise fraction: a single interval's observed rate is this noisy
/// relative to the rate, so the filter does not chase every sample.
const R_FRAC: f64 = 0.15;

/// Receiver-side Sprout-style rate forecaster.
#[derive(Debug, Clone)]
pub struct ArrivalForecast {
    /// Current rate estimate (bytes/s).
    rate: f64,
    /// Estimate variance ((bytes/s)^2).
    var: f64,
    initialized: bool,
}

impl Default for ArrivalForecast {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrivalForecast {
    pub fn new() -> Self {
        Self {
            rate: 0.0,
            var: 0.0,
            initialized: false,
        }
    }

    /// Feed one measurement: `bytes` delivered over `interval_s` seconds. The
    /// first observation seeds the filter; later ones run the Kalman
    /// predict / update with rate-scaled process and measurement noise.
    pub fn observe(&mut self, bytes: u64, interval_s: f64) {
        if interval_s <= 0.0 {
            return;
        }
        let z = bytes as f64 / interval_s;
        if !self.initialized {
            self.rate = z;
            // Seed the variance from the measurement-noise scale so the first
            // forecast is already a sensible lower bound, not zero.
            self.var = (R_FRAC * z).powi(2);
            self.initialized = true;
            return;
        }
        // Predict: the rate is a random walk, so the variance grows by the
        // process noise (scaled to the current estimate).
        let q = (Q_FRAC * self.rate).powi(2);
        let var_pred = self.var + q;
        // Update against the observation, whose noise scales with its own size.
        let r = (R_FRAC * z).powi(2).max(1.0);
        let k = var_pred / (var_pred + r);
        self.rate += k * (z - self.rate);
        self.var = (1.0 - k) * var_pred;
    }

    /// The smoothed mean rate estimate (bytes/s).
    pub fn mean_bps(&self) -> f64 {
        self.rate
    }

    /// The forecast: the 5th-percentile deliverable rate over the next tick
    /// (bytes/s), `mean - 1.645 * sqrt(var + process noise)`, floored at zero.
    /// A conservative lower bound the sender can size to without overshooting.
    pub fn forecast_bps(&self) -> f64 {
        if !self.initialized {
            return 0.0;
        }
        let q = (Q_FRAC * self.rate).powi(2);
        let var_next = self.var + q;
        (self.rate - Z_5TH * var_next.sqrt()).max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A steady rate: the forecast converges to a conservative bound just below
    /// the rate, never above it.
    #[test]
    fn steady_rate_forecasts_a_conservative_lower_bound() {
        let mut f = ArrivalForecast::new();
        // 100 kB every 0.1 s = 1 MB/s, repeated.
        for _ in 0..50 {
            f.observe(100_000, 0.1);
        }
        let mean = f.mean_bps();
        let fc = f.forecast_bps();
        assert!((mean - 1e6).abs() < 5e4, "mean ~ 1 MB/s, got {}", mean);
        assert!(fc < mean, "the forecast is a conservative lower bound");
        assert!(fc > 0.5e6, "but not absurdly low on a steady rate, got {}", fc);
    }

    /// The forecast is always at or below the mean (never optimistic).
    #[test]
    fn forecast_never_exceeds_the_mean() {
        let mut f = ArrivalForecast::new();
        for i in 0..40 {
            // A noisy rate around 2 MB/s.
            let b = if i % 2 == 0 { 180_000 } else { 220_000 };
            f.observe(b, 0.1);
            assert!(f.forecast_bps() <= f.mean_bps() + 1.0, "never optimistic");
        }
    }

    /// A rate that collapses (a cellular dip): the forecast drops toward the new
    /// low rate, and the variance spike makes it lead the mean down.
    #[test]
    fn a_rate_dip_pulls_the_forecast_down() {
        let mut f = ArrivalForecast::new();
        for _ in 0..30 {
            f.observe(100_000, 0.1); // 1 MB/s
        }
        let fc_before = f.forecast_bps();
        // The bottleneck collapses to 0.2 MB/s.
        for _ in 0..15 {
            f.observe(20_000, 0.1);
        }
        let fc_after = f.forecast_bps();
        assert!(
            fc_after < fc_before * 0.6,
            "the forecast dropped after the dip: {fc_before} -> {fc_after}"
        );
        assert!(fc_after < 0.4e6, "toward the new low rate, got {fc_after}");
    }

    /// Uninitialised: no forecast yet.
    #[test]
    fn no_forecast_before_any_observation() {
        let f = ArrivalForecast::new();
        assert_eq!(f.forecast_bps(), 0.0);
    }
}
