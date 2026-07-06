//! Item 13: WBest available-bandwidth estimator (receiver side).
//!
//! WBest (Li, Claypool & Kinicki, "WBest: a Bandwidth Estimation Tool for IEEE
//! 802.11 Wireless Networks", LCN 2008) estimates the available bandwidth on a
//! path whose last hop is wireless, in two non-flooding stages:
//!
//!  1. **Packet pairs -> effective capacity.** `n` back-to-back probe *pairs*
//!     are sent; the receiver measures each pair's dispersion (the inter-arrival
//!     of the two probes). A pair of `b`-bit packets `d` seconds apart implies an
//!     instantaneous rate `b / d`; the **median** over the `n` pairs is the
//!     effective capacity `C_e`. The median rejects the contention noise a wireless
//!     hop injects into any single pair.
//!  2. **Packet train -> available bandwidth.** An `m`-packet *train* is then sent
//!     at `C_e`; the receiver measures its mean reception-dispersion rate `R`
//!     (total bits over the span). The available bandwidth is
//!     `A_b = C_e * (2 - C_e / R)`, clamped to `[0, C_e]`. With no cross traffic
//!     the train keeps pace (`R = C_e`) and `A_b = C_e`; as cross traffic slows
//!     the train, `R` falls and `A_b` shrinks toward zero.
//!
//! `C_e` enters the available-bandwidth equation squared, so a capacity error is
//! amplified - which is why stage 1 takes the median and why the result is
//! cross-checked against the passive BBR `BtlBw` (item 6) at the call site rather
//! than trusted blind.
//!
//! This estimator is the receiver-side measurement half: it is fed each probe's
//! arrival time and reports `C_e` / `A_b`. The sender-side probe emission and the
//! feedback of the result live in the transport.

/// Receiver-side WBest measurement state for one probe round.
#[derive(Debug, Clone)]
pub struct WBestEstimator {
    /// Probe packet size in bits (the padded on-wire size both stages use).
    packet_bits: f64,
    /// Dispersions (microseconds) of completed packet pairs.
    pair_disp_us: Vec<f64>,
    /// Arrival of a pair's first probe (idx 0) awaiting its second (idx 1).
    pending_pair_us: Option<f64>,
    /// Train arrival span: first / last arrival and the count, so the mean
    /// dispersion is `(last - first) / (count - 1)`.
    train_first_us: Option<f64>,
    train_last_us: f64,
    train_count: u32,
}

impl WBestEstimator {
    /// New estimator for a probe packet of `packet_bytes` on-wire bytes.
    pub fn new(packet_bytes: usize) -> Self {
        Self {
            packet_bits: (packet_bytes as f64) * 8.0,
            pair_disp_us: Vec::new(),
            pending_pair_us: None,
            train_first_us: None,
            train_last_us: 0.0,
            train_count: 0,
        }
    }

    /// Clear all measurements for a fresh probe round.
    pub fn reset(&mut self) {
        self.pair_disp_us.clear();
        self.pending_pair_us = None;
        self.train_first_us = None;
        self.train_last_us = 0.0;
        self.train_count = 0;
    }

    /// Feed one stage-1 pair probe: `idx` 0 is the pair's first packet, 1 the
    /// second. A complete pair records one dispersion. `arrival_us` is the
    /// receive timestamp (a monotonic microsecond clock).
    pub fn on_pair_probe(&mut self, idx: u8, arrival_us: f64) {
        if idx == 0 {
            self.pending_pair_us = Some(arrival_us);
        } else if let Some(first) = self.pending_pair_us.take() {
            let disp = arrival_us - first;
            // A non-positive or implausibly tiny dispersion is a coalesced
            // arrival (the two probes were batched by the NIC / loopback); it
            // carries no rate information, so it is discarded.
            if disp > 0.5 {
                self.pair_disp_us.push(disp);
            }
        }
    }

    /// Feed one stage-2 train probe arrival.
    pub fn on_train_probe(&mut self, arrival_us: f64) {
        if self.train_first_us.is_none() {
            self.train_first_us = Some(arrival_us);
        }
        self.train_last_us = arrival_us;
        self.train_count += 1;
    }

    /// Effective capacity `C_e` in bits/s: the median of `packet_bits / dispersion`
    /// over the completed pairs. `None` until at least one pair has landed.
    pub fn effective_capacity_bps(&self) -> Option<f64> {
        if self.pair_disp_us.is_empty() {
            return None;
        }
        let mut rates: Vec<f64> = self
            .pair_disp_us
            .iter()
            .map(|d| self.packet_bits * 1e6 / d)
            .collect();
        rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = rates.len() / 2;
        Some(if rates.len() % 2 == 1 {
            rates[mid]
        } else {
            0.5 * (rates[mid - 1] + rates[mid])
        })
    }

    /// The train's mean reception rate `R` in bits/s: total bits over the span.
    /// `None` until at least two train probes have landed.
    pub fn train_rate_bps(&self) -> Option<f64> {
        let first = self.train_first_us?;
        if self.train_count < 2 {
            return None;
        }
        let span_us = self.train_last_us - first;
        if span_us <= 0.0 {
            return None;
        }
        let mean_disp_us = span_us / (self.train_count as f64 - 1.0);
        Some(self.packet_bits * 1e6 / mean_disp_us)
    }

    /// Available bandwidth `A_b = C_e (2 - C_e / R)` in bits/s, clamped to
    /// `[0, C_e]`. `None` until both stages have enough samples.
    pub fn available_bps(&self) -> Option<f64> {
        let c = self.effective_capacity_bps()?;
        let r = self.train_rate_bps()?;
        if r <= 0.0 {
            return None;
        }
        let a = c * (2.0 - c / r);
        Some(a.clamp(0.0, c))
    }

    /// How many complete pairs and train probes have landed (probe-round
    /// progress, so the transport knows when the estimate is ready).
    pub fn samples(&self) -> (usize, u32) {
        (self.pair_disp_us.len(), self.train_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 1500-byte (12000-bit) packet at 120 us dispersion is exactly 100 Mbit/s;
    /// the median over noisy pairs recovers the capacity.
    #[test]
    fn effective_capacity_is_median_of_pair_rates() {
        let mut e = WBestEstimator::new(1500);
        // Five pairs: three at 120 us (100 Mbit/s) and two outliers; the median
        // ignores the outliers.
        for (i, disp) in [120.0, 60.0, 120.0, 480.0, 120.0].iter().enumerate() {
            e.on_pair_probe(0, (i as f64) * 1000.0);
            e.on_pair_probe(1, (i as f64) * 1000.0 + disp);
        }
        let c = e.effective_capacity_bps().unwrap();
        // Median dispersion is 120 us -> 100 Mbit/s.
        assert!((c - 100e6).abs() < 1e6, "C_e ~ 100 Mbit/s, got {}", c / 1e6);
    }

    /// A coalesced pair (zero / negative dispersion) carries no rate and is
    /// dropped, not counted as infinite capacity.
    #[test]
    fn coalesced_pairs_are_discarded() {
        let mut e = WBestEstimator::new(1500);
        e.on_pair_probe(0, 1000.0);
        e.on_pair_probe(1, 1000.0); // zero dispersion - dropped
        assert_eq!(e.samples().0, 0, "a coalesced pair records nothing");
        assert!(e.effective_capacity_bps().is_none());
    }

    /// With the train keeping full pace (R = C_e) the path is idle and the
    /// available bandwidth equals the capacity.
    #[test]
    fn idle_path_gives_available_equal_to_capacity() {
        let mut e = WBestEstimator::new(1500);
        for i in 0..4 {
            e.on_pair_probe(0, (i as f64) * 1000.0);
            e.on_pair_probe(1, (i as f64) * 1000.0 + 120.0);
        }
        // Train at the same 120 us dispersion -> R = C_e.
        for i in 0..8 {
            e.on_train_probe((i as f64) * 120.0);
        }
        let c = e.effective_capacity_bps().unwrap();
        let a = e.available_bps().unwrap();
        assert!((a - c).abs() < 1e6, "idle path: A_b ~ C_e, got {}", a / 1e6);
    }

    /// Cross traffic that slows the train to 80 % of capacity (R = 0.8 C_e) gives
    /// `A_b = C_e(2 - 1/0.8) = 0.75 C_e`.
    #[test]
    fn loaded_path_shrinks_available_per_formula() {
        let mut e = WBestEstimator::new(1500);
        for i in 0..4 {
            e.on_pair_probe(0, (i as f64) * 1000.0);
            e.on_pair_probe(1, (i as f64) * 1000.0 + 120.0);
        }
        // Train dispersion 150 us -> R = 80 Mbit/s = 0.8 * 100 Mbit/s.
        for i in 0..8 {
            e.on_train_probe((i as f64) * 150.0);
        }
        let c = e.effective_capacity_bps().unwrap();
        let a = e.available_bps().unwrap();
        let expected = c * (2.0 - c / (0.8 * c));
        assert!((a - expected).abs() < 1e6, "A_b = 0.75 C_e, got {}", a / 1e6);
        assert!(a < c, "a loaded path has less available than capacity");
    }

    /// A train slowed past half capacity saturates the formula to zero (clamped).
    #[test]
    fn saturated_path_clamps_available_to_zero() {
        let mut e = WBestEstimator::new(1500);
        for i in 0..4 {
            e.on_pair_probe(0, (i as f64) * 1000.0);
            e.on_pair_probe(1, (i as f64) * 1000.0 + 120.0);
        }
        // Train dispersion 300 us -> R = 40 Mbit/s = 0.4 * C_e < 0.5 C_e.
        for i in 0..8 {
            e.on_train_probe((i as f64) * 300.0);
        }
        assert_eq!(e.available_bps().unwrap(), 0.0, "saturated path clamps to 0");
    }
}
