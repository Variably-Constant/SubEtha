//! Item 17: LEO periodic-handover detection from the OWD trace.
//!
//! A low-earth-orbit link (Starlink) hands the user terminal between satellites
//! on a fixed cadence - ~15 s - and each handover is a delay spike. The cadence
//! is the tell: autocorrelate the one-way-delay trace and a strong peak at a lag
//! in the LEO range means the link is LEO, and the next spike is predictable one
//! cycle ahead, so protection can be pre-armed before the handover lands instead
//! of recovered from after.
//!
//! OWD samples arrive irregularly (one per heartbeat / data packet), so they are
//! binned into fixed-width time bins (the mean OWD per bin) and the binned series
//! is autocorrelated. The lag with the strongest normalized autocorrelation, if
//! it clears a confidence floor and falls in the LEO period band, is the detected
//! period. `secs_to_next_spike` then projects the next cycle boundary from the
//! last in-trace peak.

use std::collections::VecDeque;

/// Bin width for the resampled OWD series (milliseconds). The period is resolved
/// to this granularity; 500 ms gives a clean ~15 s cycle without a huge buffer.
const BIN_MS: u64 = 500;
/// Keep this many bins (the autocorrelation window). 120 bins x 500 ms = 60 s,
/// enough for several ~15 s cycles.
const MAX_BINS: usize = 120;
/// Period band that counts as a LEO handover cadence (seconds).
const LEO_PERIOD_MIN_S: f64 = 4.0;
const LEO_PERIOD_MAX_S: f64 = 20.0;
/// Normalized-autocorrelation floor for a confident period detection.
const CONF_FLOOR: f64 = 0.40;

/// Detects a periodic OWD cadence (a LEO handover cycle) from binned OWD.
#[derive(Debug, Clone)]
pub struct PeriodicitySensor {
    bins: VecDeque<f64>,
    cur_bin_start_us: u64,
    cur_sum: f64,
    cur_n: u32,
    started: bool,
}

impl Default for PeriodicitySensor {
    fn default() -> Self {
        Self::new()
    }
}

impl PeriodicitySensor {
    pub fn new() -> Self {
        Self {
            bins: VecDeque::with_capacity(MAX_BINS),
            cur_bin_start_us: 0,
            cur_sum: 0.0,
            cur_n: 0,
            started: false,
        }
    }

    /// Feed one OWD sample (`owd_us`) observed at monotonic time `t_us`. Samples
    /// accumulate into the current time bin; a bin closes (its mean is pushed)
    /// once `BIN_MS` has elapsed.
    pub fn observe(&mut self, owd_us: f64, t_us: u64) {
        if !self.started {
            self.cur_bin_start_us = t_us;
            self.started = true;
        }
        // Close however many whole bins have elapsed; a gap with no samples
        // pushes the last bin's mean forward so the series stays evenly spaced.
        while t_us >= self.cur_bin_start_us + BIN_MS * 1000 {
            let v = if self.cur_n > 0 {
                self.cur_sum / self.cur_n as f64
            } else {
                *self.bins.back().unwrap_or(&owd_us)
            };
            self.push_bin(v);
            self.cur_bin_start_us += BIN_MS * 1000;
            self.cur_sum = 0.0;
            self.cur_n = 0;
        }
        self.cur_sum += owd_us;
        self.cur_n += 1;
    }

    fn push_bin(&mut self, v: f64) {
        if self.bins.len() == MAX_BINS {
            self.bins.pop_front();
        }
        self.bins.push_back(v);
    }

    /// The detected period in seconds and its normalized-autocorrelation
    /// confidence (0..=1), or `None` until enough bins span at least two cycles
    /// in the LEO band with a confident peak.
    pub fn detected_period(&self) -> Option<(f64, f64)> {
        let n = self.bins.len();
        // Need at least two full max-period cycles to trust a peak.
        let min_bins = (2.0 * LEO_PERIOD_MIN_S * 1000.0 / BIN_MS as f64) as usize;
        if n < min_bins {
            return None;
        }
        let mean = self.bins.iter().sum::<f64>() / n as f64;
        let var: f64 = self.bins.iter().map(|x| (x - mean).powi(2)).sum();
        if var <= 0.0 {
            return None;
        }
        let lag_min = (LEO_PERIOD_MIN_S * 1000.0 / BIN_MS as f64).round() as usize;
        let lag_max = ((LEO_PERIOD_MAX_S * 1000.0 / BIN_MS as f64).round() as usize).min(n / 2);
        let xs: Vec<f64> = self.bins.iter().copied().collect();
        let (mut best_lag, mut best_r) = (0usize, 0.0f64);
        for lag in lag_min..=lag_max {
            let mut acc = 0.0;
            for i in 0..(n - lag) {
                acc += (xs[i] - mean) * (xs[i + lag] - mean);
            }
            let r = acc / var;
            if r > best_r {
                best_r = r;
                best_lag = lag;
            }
        }
        if best_lag == 0 || best_r < CONF_FLOOR {
            return None;
        }
        Some((best_lag as f64 * BIN_MS as f64 / 1000.0, best_r))
    }

    /// Seconds until the next predicted handover spike, given the detected period
    /// and the most recent in-trace peak bin. `None` if no period is detected.
    pub fn secs_to_next_spike(&self) -> Option<f64> {
        let (period_s, _) = self.detected_period()?;
        let period_bins = (period_s * 1000.0 / BIN_MS as f64).round() as usize;
        if period_bins == 0 {
            return None;
        }
        // The last cycle's peak bin (the highest OWD in the most recent period).
        let n = self.bins.len();
        let start = n.saturating_sub(period_bins);
        let xs: Vec<f64> = self.bins.iter().copied().collect();
        let peak_off = (start..n)
            .max_by(|&a, &b| xs[a].partial_cmp(&xs[b]).unwrap())
            .unwrap_or(n - 1);
        // Bins since that peak; the next spike is one period after it.
        let since = (n - 1).saturating_sub(peak_off);
        let to_next = period_bins.saturating_sub(since);
        Some(to_next as f64 * BIN_MS as f64 / 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed a synthetic OWD trace with a clean period and confirm the sensor
    /// recovers it. Period `p_s`, amplitude on a baseline, over `cycles` cycles.
    fn feed_periodic(s: &mut PeriodicitySensor, p_s: f64, cycles: usize) {
        let dt_us = 100_000u64; // a sample every 100 ms
        let n = (cycles as f64 * p_s * 1e6 / dt_us as f64) as u64;
        for i in 0..n {
            let t = i * dt_us;
            let phase = (t as f64 / 1e6) / p_s * std::f64::consts::TAU;
            // A 40 ms baseline OWD with a 60 ms periodic handover bump.
            let owd = 40_000.0 + 60_000.0 * (phase.sin().max(0.0)).powi(4);
            s.observe(owd, t);
        }
    }

    #[test]
    fn detects_a_clean_periodic_cadence() {
        let mut s = PeriodicitySensor::new();
        feed_periodic(&mut s, 15.0, 4);
        let (period, conf) = s.detected_period().expect("a period is detected");
        assert!((period - 15.0).abs() <= 1.0, "period ~ 15 s, got {period}");
        assert!(conf > CONF_FLOOR, "confident, got {conf}");
    }

    #[test]
    fn detects_a_shorter_cadence_too() {
        let mut s = PeriodicitySensor::new();
        feed_periodic(&mut s, 6.0, 6);
        let (period, _) = s.detected_period().expect("a period is detected");
        assert!((period - 6.0).abs() <= 1.0, "period ~ 6 s, got {period}");
    }

    #[test]
    fn a_flat_trace_has_no_period() {
        let mut s = PeriodicitySensor::new();
        for i in 0..400 {
            s.observe(40_000.0, i * 100_000); // constant OWD
        }
        assert!(s.detected_period().is_none(), "a flat OWD has no cadence");
    }

    #[test]
    fn predicts_the_next_spike_within_a_period() {
        let mut s = PeriodicitySensor::new();
        feed_periodic(&mut s, 10.0, 5);
        let to_next = s.secs_to_next_spike().expect("a spike is predicted");
        assert!(
            (0.0..=10.0).contains(&to_next),
            "next spike within one 10 s period, got {to_next}"
        );
    }
}
