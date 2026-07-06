//! Loss-class sensor: congestion-vs-wireless loss differentiation.
//!
//! A lost shard means two very different things. A *congestion* drop says the
//! path is overfull: raise parity broadly and ease off the gas. A *wireless*
//! drop is a random radio hit on an otherwise-fine path: recover it locally
//! with FEC / interleaving and do NOT back off. Treating one as the other is
//! the classic mistake - over-driving a congested path, or needlessly throttling
//! a clean one - so the controller wants to know which it is.
//!
//! This sensor is a hybrid of two end-to-end loss-differentiation algorithms
//! from Cen, Cosman & Voelker, "End-to-end differentiation of congestion and
//! wireless losses" (IEEE/ACM Trans. Networking 11(5), 2003):
//!
//!  - **mBiaz** (inter-arrival). `T_min` is the minimum packet inter-arrival
//!    seen. The paper's wireless window for a gap of `n` is `[(n+1) * T_min,
//!    (n+1.25) * T_min)` - the 1.25 upper factor is the modified-Biaz tuning
//!    (Fig. 2), tightening the original Biaz `[(n+1) * T_min, (n+2) * T_min)` to
//!    cut congestion misclassification. The bridge ships a block as one GSO
//!    burst, so its shards arrive back-to-back and a SUB-window spacing is a
//!    batched arrival, not evidence of loss type. So here Biaz votes congestion
//!    only when the spacing is at or above the window (a genuine queuing delay);
//!    in-window and burst spacing are left to Spike.
//!  - **Spike** (relative one-way trip time). With `rtt_min` / `rtt_max` the
//!    min / max ROTT seen, the path is in a congestion *spike* when its ROTT
//!    rises above `rtt_min + alpha * (rtt_max - rtt_min)` and leaves it when it
//!    falls below `rtt_min + beta * (rtt_max - rtt_min)`, with `alpha = 1/2`,
//!    `beta = 1/3` (the paper's values; the hysteresis keeps the state from
//!    flapping). A loss inside the spike is congestion; outside it is wireless.
//!    The ROTT is receiver-minus-sender timestamps; a constant clock offset
//!    cancels in the min / max range, exact on same-machine and low-skew links.
//!
//! The hybrid calls a loss *congestion* when EITHER signal flags it - Biaz sees
//! queuing delay or the path is in a Spike - and *wireless* only when neither
//! does. Congestion is the costlier miss - the paper notes a congestion loss
//! mistaken for wireless "will not be reduced when the network is congested" -
//! so the tie breaks to congestion.
//!
//! The sensor holds no clock and does no I/O: the caller supplies the
//! inter-arrival and ROTT (microseconds), so it is deterministic and
//! exhaustively testable with synthetic traces.

/// One classified loss event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossClass {
    /// A random wireless drop: recover locally (FEC / interleave), do not
    /// inflate the effective-loss the controller uses to pace.
    Wireless,
    /// A congestion drop: raise parity broadly and consider pacing.
    Congestion,
}

/// mBiaz wireless-window upper factor (modified Biaz, Fig. 2): the window ends
/// at `(n + 1.25) * T_min`, and spacing at or above it is genuine queuing delay
/// (congestion). The original Biaz used `n + 2`; 1.25 cuts congestion
/// misclassification.
const BIAZ_UPPER: f64 = 1.25;
/// EWMA gain for the recent-congestion share the 2-bit class code reads.
const CONGESTION_GAIN: f64 = 1.0 / 8.0;
/// Minimum standing-queue delay (microseconds) for the path to count as
/// congested. A genuine queue adds at least ~1 ms of standing delay; sub-
/// millisecond excursions are scheduling / clock / decode-backlog jitter, which
/// has no real queue.
const MIN_QUEUE_US: f64 = 1000.0;
/// Samples in the recent-ROTT-min window. The congestion signal is the recent
/// FLOOR (fastest recent packet) rising above the all-time floor (RTprop) - a
/// real standing queue raises every packet, including the fastest, while a decode
/// / scheduling backlog only inflates the SLOW packets, never the recent min, so
/// this rejects the backlog that the receiver's own processing adds to the ROTT.
const SPIKE_WINDOW: usize = 32;
/// Fraction of [`MIN_QUEUE_US`] below which the queue is judged drained (Spike
/// leave), giving hysteresis so the congestion state does not flap at the edge.
const SPIKE_LEAVE_FRAC: f64 = 0.5;

/// Stateful loss differentiator. `observe_interarrival` / `observe_owd` feed it
/// the running timing; `classify` runs the hybrid on a detected loss.
#[derive(Debug)]
pub struct LossClassSensor {
    /// Minimum inter-arrival seen (`T_min`), microseconds.
    t_min: f64,
    /// Minimum ROTT seen (`RTprop` baseline), microseconds.
    rtt_min: f64,
    /// The last [`SPIKE_WINDOW`] ROTT samples, for the recent-floor (windowed
    /// min) the congestion detector compares against `rtt_min`.
    recent_owd: std::collections::VecDeque<f64>,
    /// Whether the path is currently in a congestion spike (hysteresis).
    in_spike: bool,
    /// EWMA of per-loss class (1 = congestion, 0 = wireless).
    congestion_ewma: f64,
    /// Losses classified so far (0 = the code is "no loss yet").
    losses_seen: u64,
}

impl Default for LossClassSensor {
    fn default() -> Self {
        Self::new()
    }
}

impl LossClassSensor {
    /// A fresh sensor with no timing baseline yet.
    pub fn new() -> Self {
        Self {
            t_min: f64::INFINITY,
            rtt_min: f64::INFINITY,
            recent_owd: std::collections::VecDeque::new(),
            in_spike: false,
            congestion_ewma: 0.0,
            losses_seen: 0,
        }
    }

    /// Record one packet inter-arrival (microseconds) to update `T_min`. Zero
    /// or negative spacings (duplicate / reordered arrivals) are ignored.
    pub fn observe_interarrival(&mut self, ia_us: f64) {
        if ia_us > 0.0 && ia_us < self.t_min {
            self.t_min = ia_us;
        }
    }

    /// Record one relative one-way trip time (microseconds) to update the RTprop
    /// baseline and the recent-floor congestion detector.
    pub fn observe_owd(&mut self, owd_us: f64) {
        if owd_us < self.rtt_min {
            self.rtt_min = owd_us;
        }
        self.recent_owd.push_back(owd_us);
        while self.recent_owd.len() > SPIKE_WINDOW {
            self.recent_owd.pop_front();
        }
        // The standing-queue delay is how far the recent FLOOR (the fastest of
        // the last `SPIKE_WINDOW` packets) sits above the all-time floor RTprop.
        // A real queue raises every packet including the fastest; a decode /
        // scheduling backlog inflates only the slow packets, leaving the recent
        // min at the true floor - so this reads the queue, not the backlog.
        let recent_min = self
            .recent_owd
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let queue = recent_min - self.rtt_min;
        if !self.in_spike && queue > MIN_QUEUE_US {
            self.in_spike = true;
        } else if self.in_spike && queue < MIN_QUEUE_US * SPIKE_LEAVE_FRAC {
            self.in_spike = false;
        }
    }

    /// Classify a loss of `gap` consecutive packets given the inter-arrival
    /// (microseconds) measured across it. mBiaz calls it wireless when the
    /// spacing fits the gap's wireless window; Spike calls it wireless when the
    /// path is not in a congestion spike; the hybrid is wireless only when both
    /// agree, else congestion.
    pub fn classify(&mut self, gap: u32, interarrival_us: f64) -> LossClass {
        let n = gap.max(1) as f64;
        // mBiaz's wireless window is `[(n+1)*T_min, (n+1.25)*T_min)`. The bridge
        // ships a block as one GSO super-buffer, so its shards arrive back-to-
        // back and a SUB-window inter-arrival is a batched arrival, not evidence
        // of loss type. So Biaz only votes CONGESTION when the spacing is ABOVE
        // the window `(n+1.25)*T_min` - a genuine queuing delay; in-window or
        // burst spacing is left to Spike. A loss is congestion when EITHER Biaz
        // sees queuing OR the path is in a congestion spike, and wireless only
        // when neither does (the conservative tie-break to congestion).
        let biaz_congestion = self.t_min.is_finite()
            && self.t_min > 0.0
            && interarrival_us >= (n + BIAZ_UPPER) * self.t_min;
        let class = if biaz_congestion || self.in_spike {
            LossClass::Congestion
        } else {
            LossClass::Wireless
        };
        let sample = if class == LossClass::Congestion { 1.0 } else { 0.0 };
        self.congestion_ewma += (sample - self.congestion_ewma) * CONGESTION_GAIN;
        self.losses_seen += 1;
        class
    }

    /// Spread (max - min, microseconds) of the recent ROTT window - the path's
    /// current delay variation, which bounds how late a reordered packet can
    /// arrive. A consumer uses it to set a reorder-tolerant retransmit grace so
    /// jitter is not mistaken for loss. Zero before two samples.
    pub fn recent_owd_spread_us(&self) -> f64 {
        if self.recent_owd.len() < 2 {
            return 0.0;
        }
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for &v in &self.recent_owd {
            lo = lo.min(v);
            hi = hi.max(v);
        }
        hi - lo
    }

    /// Share of recent loss classified congestion (0..=1); 0 before any loss.
    pub fn congestion_fraction(&self) -> f32 {
        if self.losses_seen == 0 {
            0.0
        } else {
            self.congestion_ewma as f32
        }
    }

    /// 2-bit class code for the `Loss` frame: 0 = no loss yet, 1 = wireless,
    /// 2 = congestion, 3 = mixed (recent loss split between the two).
    pub fn class_code(&self) -> u8 {
        if self.losses_seen == 0 {
            0
        } else if self.congestion_ewma < 0.25 {
            1
        } else if self.congestion_ewma > 0.75 {
            2
        } else {
            3
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pure last-hop-wireless pattern: steady spacing (so `T_min` is the
    /// spacing), flat ROTT (no spike), and single-packet gaps whose
    /// inter-arrival is ~2 * T_min - the wireless signature `(n+1) * T_min`
    /// with n = 1. Every loss must classify wireless.
    #[test]
    fn pure_wireless_gap_pattern_classifies_wireless() {
        let mut s = LossClassSensor::new();
        // Steady 1000us spacing establishes T_min, flat 5000us ROTT (no spike).
        for _ in 0..50 {
            s.observe_interarrival(1000.0);
            s.observe_owd(5000.0);
        }
        // A single-packet gap (n=1) whose inter-arrival is 2*T_min: wireless.
        for _ in 0..20 {
            assert_eq!(s.classify(1, 2000.0), LossClass::Wireless);
        }
        assert_eq!(s.class_code(), 1, "class code = wireless");
        assert!(s.congestion_fraction() < 0.1, "low congestion fraction");
    }

    /// A pure congestion pattern: a ROTT spike well above the range midpoint
    /// puts the path in the spike state, so every loss classifies congestion
    /// regardless of inter-arrival.
    #[test]
    fn pure_congestion_rott_spike_classifies_congestion() {
        let mut s = LossClassSensor::new();
        // Establish a ROTT range, then spike high (queue building).
        for i in 0..50u64 {
            s.observe_interarrival(1000.0);
            s.observe_owd(5000.0 + i as f64 * 200.0); // climbing toward a spike
        }
        // A loss during the spike is congestion even with a "wireless-looking"
        // inter-arrival, because Spike overrides via the conservative hybrid.
        for _ in 0..20 {
            assert_eq!(s.classify(1, 2000.0), LossClass::Congestion);
        }
        assert_eq!(s.class_code(), 2, "class code = congestion");
        assert!(s.congestion_fraction() > 0.9, "high congestion fraction");
    }

    /// Spike hysteresis: the path enters the spike above the alpha threshold and
    /// only leaves below the beta threshold, so a ROTT between the two holds the
    /// previous state.
    #[test]
    fn recent_owd_spread_tracks_jitter() {
        let mut s = LossClassSensor::new();
        // A flat ROTT has zero spread (no jitter -> a tight reorder grace).
        for _ in 0..40 {
            s.observe_owd(5000.0);
        }
        assert_eq!(s.recent_owd_spread_us(), 0.0, "flat ROTT has no spread");
        // Jitter widens the spread (a looser reorder grace is warranted).
        for i in 0..40 {
            s.observe_owd(5000.0 + (i % 8) as f64 * 1000.0);
        }
        assert!(
            s.recent_owd_spread_us() >= 6000.0,
            "jitter must widen the spread, got {}",
            s.recent_owd_spread_us()
        );
    }

    #[test]
    fn spike_state_has_hysteresis() {
        let mut s = LossClassSensor::new();
        // RTprop baseline 5000us. A SUSTAINED 2000us queue (> MIN_QUEUE 1000us)
        // raises the recent floor and enters the spike.
        for _ in 0..40 {
            s.observe_owd(5000.0);
        }
        for _ in 0..40 {
            s.observe_owd(7000.0);
        }
        assert_eq!(s.classify(1, 0.0), LossClass::Congestion, "sustained queue enters spike");
        // Partial drain to a 700us queue (between the 500us leave floor and the
        // 1000us enter floor): hysteresis holds the spike.
        for _ in 0..40 {
            s.observe_owd(5700.0);
        }
        assert_eq!(s.classify(1, 0.0), LossClass::Congestion, "hysteresis holds spike");
        // Full drain back to RTprop (queue 0 < the 500us leave floor): leaves.
        for _ in 0..40 {
            s.observe_owd(5000.0);
        }
        assert_eq!(s.classify(1, 0.0), LossClass::Wireless, "drained queue leaves spike");
    }

    /// The hybrid is conservative: mBiaz saying wireless does NOT override a
    /// congestion spike - both must agree on wireless.
    #[test]
    fn hybrid_breaks_ties_to_congestion() {
        let mut s = LossClassSensor::new();
        for _ in 0..40 {
            s.observe_interarrival(1000.0);
        }
        // Force a spike with a sustained ms-scale queue (RTprop 1000us, then a
        // sustained 6000us: a 5000us standing queue > the MIN_QUEUE floor).
        for _ in 0..40 {
            s.observe_owd(1000.0);
        }
        for _ in 0..40 {
            s.observe_owd(6000.0);
        }
        // mBiaz alone would say wireless (2*T_min spacing), but the spike wins.
        assert_eq!(s.classify(1, 2000.0), LossClass::Congestion);
    }

    /// The mBiaz window scales with the gap: a gap of n=3 puts the wireless
    /// window upper bound at (3+1.25)*T_min, and only spacing above it is read
    /// as congestion. Sub-window (burst) spacing defers to Spike.
    #[test]
    fn biaz_window_tracks_gap_size() {
        let mut s = LossClassSensor::new();
        for _ in 0..40 {
            s.observe_interarrival(1000.0);
            s.observe_owd(5000.0); // flat, no spike
        }
        // gap=3: the wireless window ends at (3+1.25)*T_min = 4250.
        // In-window spacing (4100) with no spike -> wireless.
        assert_eq!(s.classify(3, 4100.0), LossClass::Wireless);
        // Sub-window (burst-like) spacing defers to Spike; no spike -> wireless.
        assert_eq!(s.classify(3, 2000.0), LossClass::Wireless);
        // Above-window spacing is genuine queuing delay -> congestion.
        assert_eq!(s.classify(3, 5000.0), LossClass::Congestion);
    }
}
