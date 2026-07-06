//! Link-type fingerprint from the SHAPE of the RTT distribution - no new
//! packets, no OS wireless read.
//!
//! 802.11 MAC retransmission splits the round-trip time into two clusters: a
//! frame that succeeds on the first transmit returns quickly, a frame that is
//! retried (collision, weak signal) returns a contention-window-and-retry later.
//! So a Wi-Fi hop makes the RTT distribution **bimodal**, while a wired link is
//! tight and **unimodal**. Sarle's bimodality coefficient
//! `b = (skewness^2 + 1) / kurtosis` captures exactly this: `b > 5/9` indicates
//! bimodality (a uniform distribution sits at `5/9`, a normal near `1/3`, and a
//! two-peaked distribution above `5/9`).
//!
//! Because the coefficient reads the END-TO-END RTT, a Wi-Fi hop ANYWHERE on
//! the path shows up - so a wired host can detect that its peer is on Wi-Fi,
//! filling the `Link` class when the local OS wireless read is unavailable.
//!
//! The four central moments are maintained online (Pebay's streaming update),
//! so the fingerprint costs O(1) per RTT sample and no storage.

/// Minimum RTT samples before the bimodality coefficient is trusted - the
/// third and fourth moments need a population to be stable.
const MIN_SAMPLES: u64 = 30;

/// Sarle's bimodality threshold: a uniform distribution sits exactly here, a
/// unimodal one below, a bimodal one above.
const SARLE_THRESHOLD: f64 = 5.0 / 9.0;

/// Streaming RTT-distribution shape estimator.
#[derive(Debug, Clone, Default)]
pub struct RttShape {
    n: u64,
    mean: f64,
    m2: f64,
    m3: f64,
    m4: f64,
}

impl RttShape {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one RTT sample (microseconds) into the running moments via Pebay's
    /// streaming update of the first four central moments.
    pub fn observe(&mut self, rtt_us: f64) {
        let n1 = self.n as f64;
        self.n += 1;
        let n = self.n as f64;
        let delta = rtt_us - self.mean;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let term1 = delta * delta_n * n1;
        self.mean += delta_n;
        self.m4 += term1 * delta_n2 * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n2 * self.m2
            - 4.0 * delta_n * self.m3;
        self.m3 += term1 * delta_n * (n - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;
    }

    /// Sarle's bimodality coefficient `b = (skewness^2 + 1) / kurtosis`, or
    /// `None` until `MIN_SAMPLES` samples and a non-degenerate spread. `b` is
    /// bounded `0..=1`; above `SARLE_THRESHOLD` the distribution is bimodal.
    pub fn bimodality(&self) -> Option<f64> {
        if self.n < MIN_SAMPLES || self.m2 <= 0.0 {
            return None;
        }
        let n = self.n as f64;
        // Population skewness and (non-excess) kurtosis from the central moments.
        let skewness = n.sqrt() * self.m3 / self.m2.powf(1.5);
        let kurtosis = n * self.m4 / (self.m2 * self.m2);
        if kurtosis <= 0.0 {
            return None;
        }
        Some((skewness * skewness + 1.0) / kurtosis)
    }

    /// Confidence in `0..=1` that the path carries a Wi-Fi hop, from how far the
    /// bimodality coefficient sits above Sarle's threshold. 0 below threshold
    /// (unimodal - wired) or before enough samples; rising to 1 as the RTT
    /// distribution becomes strongly two-peaked (Wi-Fi MAC retransmission).
    pub fn wifi_confidence(&self) -> f32 {
        match self.bimodality() {
            Some(b) if b > SARLE_THRESHOLD => {
                (((b - SARLE_THRESHOLD) / (1.0 - SARLE_THRESHOLD)).clamp(0.0, 1.0)) as f32
            }
            _ => 0.0,
        }
    }

    /// Samples folded so far (diagnostics).
    pub fn samples(&self) -> u64 {
        self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tight unimodal distribution (a wired link: one cluster of RTTs) sits
    /// below Sarle's threshold, so the Wi-Fi confidence is zero.
    #[test]
    fn unimodal_is_not_wifi() {
        let mut s = RttShape::new();
        // A narrow triangular bump around 1000 us - one mode.
        let center = [990.0, 995.0, 1000.0, 1005.0, 1010.0];
        let weight = [1, 3, 6, 3, 1];
        for _ in 0..8 {
            for (v, w) in center.iter().zip(weight) {
                for _ in 0..w {
                    s.observe(*v);
                }
            }
        }
        let b = s.bimodality().expect("enough samples");
        assert!(b < SARLE_THRESHOLD, "unimodal b={b} should be below 5/9");
        assert_eq!(s.wifi_confidence(), 0.0, "unimodal -> not Wi-Fi");
    }

    /// A two-peaked distribution (a Wi-Fi link: a fast first-transmit cluster
    /// and a slow retried cluster) sits above Sarle's threshold, so the Wi-Fi
    /// confidence is positive.
    #[test]
    fn bimodal_is_wifi() {
        let mut s = RttShape::new();
        // Two clusters: ~1000 us first-tx, ~6000 us retried.
        for _ in 0..60 {
            s.observe(1000.0);
            s.observe(6000.0);
        }
        let b = s.bimodality().expect("enough samples");
        assert!(b > SARLE_THRESHOLD, "bimodal b={b} should be above 5/9");
        assert!(s.wifi_confidence() > 0.0, "bimodal -> Wi-Fi confidence");
    }

    /// Before enough samples the coefficient is withheld (the higher moments are
    /// not yet stable), so the fingerprint never fires on a handful of RTTs.
    #[test]
    fn withholds_until_enough_samples() {
        let mut s = RttShape::new();
        for _ in 0..(MIN_SAMPLES - 1) {
            s.observe(1000.0);
        }
        assert!(s.bimodality().is_none(), "withheld before MIN_SAMPLES");
        assert_eq!(s.wifi_confidence(), 0.0);
    }
}
