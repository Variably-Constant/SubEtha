//! Temporal sensing: a purely in-band channel estimator built from
//! send / receive timing alone.
//!
//! Given a stream of `(send_ts, recv_ts)` observations (microseconds) it
//! produces three signals the adaptive controller fuses with loss and
//! radio sensors:
//!
//!  - **Inter-arrival jitter** (RFC 3550 style): variability of arrival
//!    spacing, a proxy for queueing noise and burstiness.
//!  - **One-way-delay (OWD) trend**: the slope of OWD over a recent
//!    window. The absolute OWD carries the (unknown) clock offset between
//!    the two hosts, but the *slope* cancels it, so a rising trend means
//!    "the queue is building" - congestion-driven loss is imminent -
//!    regardless of unsynchronized clocks. This is the WebRTC Google
//!    Congestion Control mechanism (trendline over one-way delay).
//!  - **Mean inter-arrival**: the baseline spacing the jitter is measured
//!    against.
//!
//! The estimator holds no clock and does no I/O; the caller supplies
//! timestamps, so it is deterministic and exhaustively testable with
//! synthetic traces.

use std::collections::VecDeque;

/// Rolling estimator over send/receive timing.
#[derive(Debug)]
pub struct TemporalSensor {
    /// `(send_ts, recv_ts)` of the previous observation.
    prev: Option<(u64, u64)>,
    /// EWMA mean inter-arrival (microseconds).
    mean_interarrival: f64,
    /// RFC 3550 interarrival jitter estimate (microseconds).
    jitter: f64,
    /// Recent `(recv_ts, owd)` samples for the trend slope, bounded to
    /// `window_cap`.
    owd_window: VecDeque<(u64, f64)>,
    window_cap: usize,
    /// A LONGER `(recv_ts, owd)` window for the clock-skew estimate. Skew is a
    /// slow, stable quantity (a fixed crystal-frequency difference), so it is
    /// measured over many round trips - long enough that the linear drift rises
    /// above the per-packet jitter that swamps it on a short window.
    skew_window: VecDeque<(u64, f64)>,
    skew_cap: usize,
}

impl Default for TemporalSensor {
    fn default() -> Self {
        Self::new(64)
    }
}

impl TemporalSensor {
    /// Create a sensor whose OWD trend is computed over the last
    /// `window` samples (clamped to at least 2).
    pub fn new(window: usize) -> Self {
        Self {
            prev: None,
            mean_interarrival: 0.0,
            jitter: 0.0,
            owd_window: VecDeque::new(),
            window_cap: window.max(2),
            skew_window: VecDeque::new(),
            // ~16x the trend window: enough round trips that the clock drift
            // accumulates above the jitter, while still bounded.
            skew_cap: (window.max(2)).saturating_mul(16).max(256),
        }
    }

    /// Record one observation. `send_ts` and `recv_ts` are microseconds;
    /// `recv_ts` uses the receiver's clock, `send_ts` the sender's. Only
    /// their *differences* are used, so a constant clock offset between
    /// the two is harmless.
    pub fn observe(&mut self, send_ts: u64, recv_ts: u64) {
        // OWD carries the clock offset; the trend slope removes it.
        let owd = recv_ts as f64 - send_ts as f64;
        if let Some((psend, precv)) = self.prev {
            // Inter-arrival on the receive side.
            let interarrival = recv_ts.wrapping_sub(precv) as f64;
            self.mean_interarrival += (interarrival - self.mean_interarrival) / 16.0;
            // RFC 3550 jitter: D is the change in transit time between
            // consecutive packets; J tracks |D| with a 1/16 gain.
            let d = (recv_ts as f64 - precv as f64) - (send_ts as f64 - psend as f64);
            self.jitter += (d.abs() - self.jitter) / 16.0;
        }
        self.prev = Some((send_ts, recv_ts));
        self.owd_window.push_back((recv_ts, owd));
        while self.owd_window.len() > self.window_cap {
            self.owd_window.pop_front();
        }
        self.skew_window.push_back((recv_ts, owd));
        while self.skew_window.len() > self.skew_cap {
            self.skew_window.pop_front();
        }
    }

    /// Current interarrival jitter (microseconds).
    pub fn jitter_micros(&self) -> f64 {
        self.jitter
    }

    /// Mean interarrival spacing (microseconds).
    pub fn interarrival_micros(&self) -> f64 {
        self.mean_interarrival
    }

    /// Slope of OWD over the window: microseconds of delay added per
    /// microsecond of wall time. Positive means the queue is building
    /// (congestion-driven loss is coming); near zero is a steady link;
    /// negative means the queue is draining. Clock-offset-invariant.
    pub fn owd_trend(&self) -> f64 {
        let n = self.owd_window.len();
        if n < 2 {
            return 0.0;
        }
        // Least-squares slope of owd (y) vs recv_ts (x). Shift x by the
        // first sample so the magnitudes stay small and well-conditioned.
        let x0 = self.owd_window.front().unwrap().0;
        let (mut sx, mut sy, mut sxx, mut sxy) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
        for &(rx, owd) in &self.owd_window {
            let x = (rx - x0) as f64;
            sx += x;
            sy += owd;
            sxx += x * x;
            sxy += x * owd;
        }
        let nf = n as f64;
        let denom = nf * sxx - sx * sx;
        if denom.abs() < f64::EPSILON {
            0.0
        } else {
            (nf * sxy - sx * sy) / denom
        }
    }

    /// Estimated clock skew: the slope of the line lying BELOW all
    /// `(recv_ts, owd)` samples (Moon-Skelly-Towsley). The minimum OWD for each
    /// time is the queue-free path, whose drift is purely the relative clock
    /// rate; queueing only ever adds delay ABOVE that line. Computed from the
    /// lower convex hull of the window (the queue-free minimum points), whose
    /// least-squares slope is the skew. Same units as `owd_trend`.
    pub fn skew(&self) -> f64 {
        let n = self.skew_window.len();
        if n < 3 {
            return 0.0;
        }
        let x0 = self.skew_window.front().unwrap().0;
        // Lower convex hull (monotone chain) over (t, owd): the queue-free
        // minimum boundary. Samples arrive in recv_ts order, so they are
        // already sorted by t.
        let mut hull: Vec<(f64, f64)> = Vec::new();
        for &(rx, owd) in &self.skew_window {
            let p = ((rx - x0) as f64, owd);
            while hull.len() >= 2 {
                let a = hull[hull.len() - 2];
                let b = hull[hull.len() - 1];
                // Pop on a clockwise / collinear turn so the chain hugs the
                // lower boundary; a queue spike above it is popped, leaving the
                // minimum-delay points.
                let cross = (b.0 - a.0) * (p.1 - a.1) - (b.1 - a.1) * (p.0 - a.0);
                if cross <= 0.0 {
                    hull.pop();
                } else {
                    break;
                }
            }
            hull.push(p);
        }
        let m = hull.len();
        if m < 2 {
            return 0.0;
        }
        // Moon's LP optimum (the below-all line highest in sum) is the lower
        // hull's supporting line at the mean time - i.e. the slope of the hull
        // edge spanning the centroid. This ignores a high queue spike at a
        // window endpoint (which is on the hull boundary but not the skew
        // line), where a least-squares fit over all hull vertices would be
        // tilted by it.
        let t_mean = self.skew_window.iter().map(|&(rx, _)| (rx - x0) as f64).sum::<f64>()
            / n as f64;
        let mut k = 0;
        while k + 2 < m && hull[k + 1].0 < t_mean {
            k += 1;
        }
        let (a, b) = (hull[k], hull[k + 1]);
        let dt = b.0 - a.0;
        if dt.abs() < f64::EPSILON {
            0.0
        } else {
            (b.1 - a.1) / dt
        }
    }

    /// OWD trend with the clock skew removed: `owd_trend - skew`. On a steady
    /// link a relative clock drift makes `owd_trend` read a false rising (or
    /// falling) trend; subtracting the skew leaves only genuine queue
    /// variation, so a steady-but-skewed link reads ~0.
    pub fn owd_trend_debiased(&self) -> f64 {
        self.owd_trend() - self.skew()
    }

    /// Number of OWD samples currently in the window.
    pub fn samples(&self) -> usize {
        self.owd_window.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steady_link_has_low_jitter_and_flat_trend() {
        let mut s = TemporalSensor::new(64);
        // Constant 1000us spacing, constant 5000us OWD (any clock offset).
        let offset = 1_000_000u64;
        for i in 0..100u64 {
            let send = i * 1000;
            let recv = send + 5000 + offset;
            s.observe(send, recv);
        }
        assert!(s.jitter_micros() < 1.0, "jitter {}", s.jitter_micros());
        assert!(s.owd_trend().abs() < 1e-6, "trend {}", s.owd_trend());
        assert!((s.interarrival_micros() - 1000.0).abs() < 50.0);
    }

    #[test]
    fn rising_owd_yields_positive_trend() {
        let mut s = TemporalSensor::new(64);
        // OWD grows 50us per packet (queue building).
        for i in 0..100u64 {
            let send = i * 1000;
            let recv = send + 5000 + i * 50;
            s.observe(send, recv);
        }
        assert!(s.owd_trend() > 0.0, "expected positive trend, got {}", s.owd_trend());
    }

    #[test]
    fn draining_owd_yields_negative_trend() {
        let mut s = TemporalSensor::new(64);
        for i in 0..100u64 {
            let send = i * 1000;
            // Start high, drain 40us per packet.
            let recv = send + 5000 + (100 - i) * 40;
            s.observe(send, recv);
        }
        assert!(s.owd_trend() < 0.0, "expected negative trend, got {}", s.owd_trend());
    }

    #[test]
    fn jittery_arrivals_raise_jitter() {
        let mut s = TemporalSensor::new(64);
        // Alternating transit time -> nonzero D each step.
        for i in 0..100u64 {
            let send = i * 1000;
            let wobble = if i % 2 == 0 { 0 } else { 800 };
            let recv = send + 5000 + wobble;
            s.observe(send, recv);
        }
        assert!(s.jitter_micros() > 100.0, "jitter {}", s.jitter_micros());
    }

    #[test]
    fn clock_skew_de_biases_the_trend() {
        let mut s = TemporalSensor::new(64);
        let offset = 1_000_000u64;
        // Steady link (constant true OWD), but the receive clock runs fast:
        // OWD drifts +10 us per 1000 us of send time (a 1% relative skew).
        for i in 0..100u64 {
            let send = i * 1000;
            let recv = send + 5000 + send / 100 + offset;
            s.observe(send, recv);
        }
        // The raw trend reads the skew as a (false) rising queue.
        assert!(s.owd_trend() > 1e-4, "raw trend sees the skew: {}", s.owd_trend());
        // The skew estimate recovers it, so the de-biased trend is ~flat.
        assert!(s.skew() > 1e-4, "skew recovered: {}", s.skew());
        assert!(
            s.owd_trend_debiased().abs() < 1e-4,
            "de-biased trend flat: {}",
            s.owd_trend_debiased()
        );
    }

    #[test]
    fn no_skew_leaves_trend_flat() {
        let mut s = TemporalSensor::new(64);
        for i in 0..100u64 {
            // Constant OWD, no skew.
            s.observe(i * 1000, i * 1000 + 5000 + 1_000_000);
        }
        assert!(s.skew().abs() < 1e-5, "no skew: {}", s.skew());
        assert!(s.owd_trend_debiased().abs() < 1e-5, "flat: {}", s.owd_trend_debiased());
    }

    #[test]
    fn real_queue_trend_survives_de_biasing() {
        let mut s = TemporalSensor::new(64);
        // No clock skew. The queue genuinely builds, but dips to a flat
        // baseline every fourth packet - so the lower-hull (minimum) line is
        // flat (skew ~0) while the mean trend rises. The de-biased trend must
        // keep the real queue signal.
        for i in 0..100u64 {
            let send = i * 1000;
            let queue = if i % 4 == 0 { 0 } else { i * 30 };
            s.observe(send, send + 5000 + queue + 1_000_000);
        }
        assert!(s.skew().abs() < 0.01, "flat baseline -> low skew: {}", s.skew());
        assert!(
            s.owd_trend_debiased() > 0.0,
            "real queue trend survives: {}",
            s.owd_trend_debiased()
        );
    }

    #[test]
    fn window_is_bounded() {
        let mut s = TemporalSensor::new(8);
        for i in 0..100u64 {
            s.observe(i * 1000, i * 1000 + 5000);
        }
        assert_eq!(s.samples(), 8);
    }
}
