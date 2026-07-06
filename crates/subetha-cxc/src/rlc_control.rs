//! Adaptive control for the sliding-window RLC code: turn the fused channel
//! assessment ([`crate::fusion::SensorSnapshot`]) into RLC coding parameters -
//! the window size, the repair cadence (code rate), and the coefficient
//! density - then hold them steady with immediate-up / conservative-down
//! hysteresis so the knobs do not flap on a noisy channel.
//!
//! This is the RLC counterpart of [`crate::fusion::raw_target`], which maps the
//! same assessment to block-RS `(parity_r, interleave_depth)`. Both consume
//! [`crate::fusion::effective_loss`] and [`crate::fusion::is_clean`], so the two
//! codes judge the channel identically and differ only in the parameter map:
//!
//!  - **Rate from loss (`R = T / (T + B)`).** One repair every `step` source
//!    symbols is `T = step` source to `B = 1` repair, code rate
//!    `R = step / (step + 1)` and redundancy `1 / (step + 1)`. To cover an
//!    effective loss `p` with a safety margin `m`, the redundancy must satisfy
//!    `1 / (step + 1) >= m * p`, i.e. `step <= 1 / (m * p) - 1`. Higher loss
//!    shrinks `step` (more repairs, lower rate); lighter loss grows it toward
//!    the lightest-protection cap. This is the QUIC-FEC adaptive-rate result:
//!    the redundancy tracks the measured loss instead of a fixed code rate.
//!  - **Window from burst length.** A repair over a window of `w` source
//!    symbols, emitted every `step`, gives `w / step` repairs covering any one
//!    symbol. Recovering a burst of `b` consecutive losses needs at least `b`
//!    independent repairs spanning it, so `w >= b * step`. The mean burst length
//!    is `burstiness * 16` (the [`crate::burst_model_sensor`] Gilbert-Elliott
//!    fit, normalized into the snapshot), so the window scales with the real
//!    fitted burst, not a fixed depth.
//!  - **Density from burstiness / congestion.** A denser coefficient vector
//!    (higher `dt`) makes each repair touch more source symbols, raising the
//!    recovery probability per repair at more compute cost. Bursty or congested
//!    loss leans dense; light isolated loss can stay sparser.
//!  - **Disable-on-clean.** A provably-clean link ([`crate::fusion::is_clean`])
//!    turns coding off entirely: QUIC-FEC found FEC *hurts* a clean / bulk path
//!    (the redundancy is pure overhead and competes with the data for the
//!    bottleneck), so the RLC path ships data only and leans on the ARQ floor
//!    until the controller re-arms on the first sign of loss.

use crate::fusion::{is_clean, SensorSnapshot};
use crate::rlc_fec::DEFAULT_DT;

/// Safety margin on the rate law: provision redundancy for `RATE_MARGIN` times
/// the measured effective loss, so a momentary spike above the mean is still
/// covered rather than NAK'd.
/// Base redundancy margin on the rate law at near-zero round trip (a loopback /
/// IPC path where a NAK is nearly free, so a light code that leans on ARQ is
/// fine).
const BASE_RATE_MARGIN: f32 = 1.4;
/// How much the rate margin grows per millisecond of round trip. On a real
/// network a NAK costs a round trip, so heavier FEC (a smaller step) that
/// recovers in-window without a retransmit is worth the extra redundancy - the
/// streaming-codes "provision FEC to the latency budget" result, which only
/// shows up off-loopback (validated on the LAN: at near-zero RTT a light step=6
/// beats static, but at LAN RTT it loses to the heavier static step=4).
const RTT_MARGIN_SLOPE: f32 = 0.8;
/// Ceiling on the rate margin, so a very high round trip does not drive the code
/// to its heaviest rate for a modest loss.
const MAX_RATE_MARGIN: f32 = 2.8;

/// The rate-law redundancy margin for a measured round trip: base at near-zero
/// RTT, growing with RTT (an expensive NAK is worth avoiding with more FEC).
fn rate_margin_for_rtt(rtt_ms: f32) -> f32 {
    (BASE_RATE_MARGIN + RTT_MARGIN_SLOPE * rtt_ms.max(0.0)).min(MAX_RATE_MARGIN)
}

/// Lightest protection: at most one repair per [`STEP_MAX`] source symbols
/// (code rate `STEP_MAX / (STEP_MAX + 1)` ~= 0.94). Reached as loss approaches
/// zero (but not clean, where coding turns off entirely).
const STEP_MAX: u16 = 16;
/// Heaviest protection: one repair per source symbol (code rate 1/2). Reached
/// under very high effective loss.
const STEP_MIN: u16 = 1;

/// Smallest coding window: enough overlap for isolated-loss recovery.
const WINDOW_MIN: u16 = 8;
/// Largest coding window. Bounded so the decoder's Gaussian-elimination cost
/// per repair stays small and the receiver's recovery horizon comfortably
/// exceeds it.
const WINDOW_MAX: u16 = 64;
/// Window provisioning factor over the bare burst-span estimate `mean_burst *
/// step`, so a burst slightly longer than the fitted mean is still in scope.
const WINDOW_BURST_SAFETY: f32 = 1.5;

/// Floor on effective loss for the rate law, so a barely-lossy-but-not-clean
/// channel does not divide by zero (it clamps to the lightest protection
/// anyway).
const MIN_EFFECTIVE_LOSS: f32 = 1.0e-3;

/// The RLC coding configuration the controller selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RlcDecision {
    /// Whether the RLC code is active at all. `false` is the disable-on-clean
    /// state: ship source symbols only and lean on the ARQ floor.
    pub coding_on: bool,
    /// Sliding-window size: how many recent source symbols a repair spans.
    pub window: u16,
    /// Source symbols between repairs. Code rate is `step / (step + 1)`.
    pub step: u16,
    /// Coefficient density threshold (0..=15): each coefficient is nonzero with
    /// probability `(dt + 1) / 16`.
    pub dt: u8,
}

/// The RLC parameters the sensors call for at the base (near-zero-RTT) rate
/// margin, before any hysteresis.
pub fn rlc_target(s: &SensorSnapshot) -> RlcDecision {
    rlc_target_with_margin(s, BASE_RATE_MARGIN)
}

/// The RLC parameters at an explicit rate-law `margin` (the controller scales it
/// by the measured round trip; see `rate_margin_for_rtt`).
pub fn rlc_target_with_margin(s: &SensorSnapshot, margin: f32) -> RlcDecision {
    if is_clean(s) {
        // Disable-on-clean: coding off, but keep nominal knobs so re-arming
        // (which copies these) starts from a sane window / cadence.
        return RlcDecision {
            coding_on: false,
            window: WINDOW_MIN,
            step: STEP_MAX,
            dt: DEFAULT_DT,
        };
    }
    // The RLC FEC provisions its RATE against the measured LOSS RATE - the one
    // channel signal that is reliably measured here (the Gilbert-Elliott fit's
    // marginal loss). In principle congestion loss should bias the rate LIGHTER
    // (QUIC-FEC: FEC on a congested/bulk path steals bottleneck bandwidth and
    // deepens the queue), but acting on that needs a TRUSTWORTHY queue signal:
    // this transport's one-way-trip time folds in the receiver's own decode
    // backlog (it stamps arrival at process time and decodes synchronously), so
    // under loss it reads a false congestion, and a false positive that lightened
    // the rate would collapse recovery. So the congestion share is measured and
    // reported but does NOT drive the rate; the rate tracks the loss rate, which
    // is robust. Density and window carry the burst signal, which IS reliable.
    let loss_rate = s.loss.max(MIN_EFFECTIVE_LOSS);
    // Rate law R = T/(T+B): redundancy 1/(step+1) >= margin * loss_rate, so
    // step <= 1/(margin * loss_rate) - 1. Heavier loss -> smaller step; a larger
    // margin (a more expensive round trip) also shrinks step.
    let step = ((1.0 / (margin.max(1.0) * loss_rate) - 1.0).floor() as i32)
        .clamp(STEP_MIN as i32, STEP_MAX as i32) as u16;
    // Window spans the fitted burst: w >= mean_burst * step, with a margin, and
    // at least 2*step so consecutive repairs overlap.
    let mean_burst = (s.burstiness * 16.0).max(1.0);
    let span = (mean_burst * step as f32 * WINDOW_BURST_SAFETY).ceil() as u32;
    let window = span
        .max(2 * step as u32)
        .clamp(WINDOW_MIN as u32, WINDOW_MAX as u32) as u16;
    // Denser coefficients under BURSTY loss: a base 0.5 density plus half the
    // burstiness, mapped onto 0..=15 and floored at 4 (a too-sparse repair over a
    // small window can miss the lost symbol entirely). Density tracks burstiness
    // alone - congestion does not make a loss more recoverable, and denser
    // repairs over a congested path are the wrong response (less FEC, not more).
    let density = (0.5 + 0.5 * s.burstiness).clamp(0.0, 1.0);
    let dt = ((density * 15.0).round() as u8).clamp(4, 15);
    RlcDecision { coding_on: true, window, step, dt }
}

/// Whether `a` is strictly more protective than `b`: coding turning on, a
/// smaller step (more repairs), a larger window (longer reach), or denser
/// coefficients. Escalations in any of these directions are applied at once;
/// only de-escalations wait out the hold.
fn more_protective(a: &RlcDecision, b: &RlcDecision) -> bool {
    // `a` must have coding on to be more protective at all; then it is more
    // protective if `b`'s coding is off, or any knob in `a` is set more
    // aggressively (smaller step, larger window, denser coefficients).
    a.coding_on
        && (!b.coding_on || a.step < b.step || a.window > b.window || a.dt > b.dt)
}

/// Immediate-up / conservative-down controller for the RLC parameters. It
/// raises protection (coding on, smaller step, larger window, denser
/// coefficients) the instant the sensors call for it - cheap insurance against
/// loss - but only lowers it after `hold` consecutive ticks that all want less,
/// so a brief quiet spell does not strip protection. Turning coding OFF
/// entirely (disable-on-clean) is the riskiest de-escalation, so it needs the
/// longer `clean_hold` sustained-clean window, exactly as the block-RS policy
/// guards the drop to Passthrough.
#[derive(Debug, Clone)]
pub struct RlcController {
    state: RlcDecision,
    down_streak: u32,
    hold: u32,
    clean_hold: u32,
    /// Measured round trip (ms); scales the rate-law margin so an expensive NAK
    /// buys heavier FEC. Zero (the default) is the near-zero-RTT base margin.
    rtt_ms: f32,
    /// Latency-priority floor: never relax FEC below this baseline protection
    /// (and never disable coding). disable-on-clean and an over-light rate save
    /// redundancy bandwidth on a quiet assessment but pay an ARQ round trip on
    /// the next loss, which head-of-line-stalls in-order delivery. For the
    /// latency-priority code (RLC in the unified transport) the step is clamped at
    /// `floor_step` (the configured baseline) so an isolated loss always recovers
    /// in-window; the controller may still escalate HEAVIER under high loss. Zero
    /// disables the floor (the default bulk behaviour with disable-on-clean).
    floor_step: u16,
    /// Latency-priority floor on the coding WINDOW: never shrink the window below
    /// this baseline span. A clean assessment otherwise collapses the window to
    /// the minimum, and a short window cannot span a loss CLUSTER even at heavy
    /// redundancy (too few repairs reach back over the burst), so clustered losses
    /// fall to ARQ. Keeping the baseline span lets the in-window repairs cover a
    /// burst. Zero disables the floor.
    floor_window: u16,
}

impl RlcController {
    /// A controller starting from active coding at the given parameters, with
    /// `hold` lower-demand ticks required before relaxing a knob and
    /// `4 * hold` sustained-clean ticks before disabling coding entirely.
    pub fn new(window: u16, step: u16, dt: u8, hold: u32) -> Self {
        let hold = hold.max(1);
        Self {
            state: RlcDecision { coding_on: true, window, step, dt },
            down_streak: 0,
            hold,
            clean_hold: hold.saturating_mul(4).max(hold),
            rtt_ms: 0.0,
            floor_step: 0,
            floor_window: 0,
        }
    }

    /// Like [`new`](Self::new) but with an explicit `clean_hold`.
    pub fn with_holds(window: u16, step: u16, dt: u8, hold: u32, clean_hold: u32) -> Self {
        let hold = hold.max(1);
        Self {
            state: RlcDecision { coding_on: true, window, step, dt },
            down_streak: 0,
            hold,
            clean_hold: clean_hold.max(hold),
            rtt_ms: 0.0,
            floor_step: 0,
            floor_window: 0,
        }
    }

    /// Set the measured round trip (ms), which scales the rate-law margin.
    pub fn set_rtt_ms(&mut self, rtt_ms: f32) {
        self.rtt_ms = rtt_ms.max(0.0);
    }

    /// Enable the latency-priority floor at the current baseline step: the
    /// controller never relaxes FEC lighter than this (nor disables coding), so an
    /// isolated loss always has an in-window repair and never falls to an ARQ
    /// round trip that stalls in-order delivery. The controller may still escalate
    /// HEAVIER under high loss. Call once at construction, before any feedback.
    pub fn set_latency_floor(&mut self) {
        self.floor_step = self.state.step.max(1);
        self.floor_window = self.state.window;
    }

    /// The current coding configuration.
    pub fn current(&self) -> RlcDecision {
        self.state
    }

    /// Fold one channel assessment and return the configuration to apply.
    pub fn decide(&mut self, s: &SensorSnapshot) -> RlcDecision {
        let mut t = rlc_target_with_margin(s, rate_margin_for_rtt(self.rtt_ms));
        // Latency-priority floor: a clean / low-loss assessment would disable
        // coding or relax the rate too light, so the next loss falls to an ARQ
        // round trip that head-of-line-stalls the in-order stream. Hold coding on
        // and clamp the step at the baseline (never lighter), so an isolated loss
        // always recovers in-window; escalation to a HEAVIER step under high loss
        // still applies (the clamp only caps the light side).
        if self.floor_step > 0 {
            t.coding_on = true;
            t.step = t.step.min(self.floor_step);
            t.window = t.window.max(self.floor_window);
        }
        if more_protective(&t, &self.state) {
            // Escalate every knob that wants more protection, immediately.
            self.state.coding_on |= t.coding_on;
            if t.coding_on {
                self.state.step = self.state.step.min(t.step);
                self.state.window = self.state.window.max(t.window);
                self.state.dt = self.state.dt.max(t.dt);
            }
            self.down_streak = 0;
        } else if t == self.state {
            self.down_streak = 0;
        } else {
            // Lower demand: only relax after a sustained quiet run. Disabling
            // coding (the riskiest step) needs the longer clean_hold window;
            // relaxing a knob while coding stays on uses the shorter hold.
            self.down_streak += 1;
            let threshold = if !t.coding_on { self.clean_hold } else { self.hold };
            if self.down_streak >= threshold {
                self.state = t;
                self.down_streak = 0;
            }
        }
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clean() -> SensorSnapshot {
        SensorSnapshot::default()
    }

    fn lossy(loss: f32, burstiness: f32) -> SensorSnapshot {
        SensorSnapshot { loss, burstiness, ..SensorSnapshot::default() }
    }

    #[test]
    fn clean_link_disables_coding() {
        let d = rlc_target(&clean());
        assert!(!d.coding_on, "a provably-clean link must disable RLC coding");
    }

    #[test]
    fn heavier_loss_shrinks_step_toward_more_repairs() {
        // The rate law: more loss -> smaller step (more repairs, lower rate).
        let light = rlc_target(&lossy(0.02, 0.0));
        let heavy = rlc_target(&lossy(0.30, 0.0));
        assert!(light.coding_on && heavy.coding_on);
        assert!(
            heavy.step < light.step,
            "heavier loss must lower step (more repairs): heavy {} vs light {}",
            heavy.step,
            light.step,
        );
        // 30% loss provisioned at 1.4x margin needs redundancy ~0.42 -> step ~1.
        assert!(heavy.step <= 2, "30% loss should be near the heaviest rate, got step {}", heavy.step);
    }

    #[test]
    fn rate_law_covers_effective_loss() {
        // The provisioned redundancy 1/(step+1) must be at least the loss it is
        // covering (the whole point of the rate law), at a representative loss.
        for &loss in &[0.05f32, 0.10, 0.20] {
            let d = rlc_target(&lossy(loss, 0.0));
            let redundancy = 1.0 / (d.step as f32 + 1.0);
            assert!(
                redundancy >= loss,
                "redundancy {redundancy} must cover loss {loss} (step {})",
                d.step,
            );
        }
    }

    #[test]
    fn longer_bursts_grow_the_window() {
        // burstiness encodes mean_burst / 16: a longer fitted burst must widen
        // the window so enough repairs span the burst.
        let short = rlc_target(&lossy(0.10, 0.1)); // mean burst ~1.6
        let long = rlc_target(&lossy(0.10, 0.6)); // mean burst ~9.6
        assert!(
            long.window > short.window,
            "longer bursts must widen the window: long {} vs short {}",
            long.window,
            short.window,
        );
    }

    #[test]
    fn density_rises_with_burstiness() {
        // Clustered (bursty) loss calls for denser repairs; congestion does not
        // touch density (it lightens the rate instead).
        let mild = rlc_target(&lossy(0.10, 0.0));
        let bursty = rlc_target(&lossy(0.10, 0.9));
        assert!(bursty.dt > mild.dt, "burstiness must raise density: {} vs {}", bursty.dt, mild.dt);
    }

    #[test]
    fn higher_rtt_provisions_heavier_fec() {
        // The same loss, at a higher round trip, gets a smaller step (heavier
        // FEC): an expensive NAK is worth avoiding with more in-window recovery.
        let s = lossy(0.10, 0.0);
        let light = rlc_target_with_margin(&s, rate_margin_for_rtt(0.0)); // ~loopback
        let heavy = rlc_target_with_margin(&s, rate_margin_for_rtt(2.0)); // real network
        assert!(
            heavy.step < light.step,
            "a higher round trip must shrink step (heavier FEC): {} vs {}",
            heavy.step,
            light.step,
        );
        // And the margin is monotonic in RTT, capped.
        assert!(rate_margin_for_rtt(5.0) >= rate_margin_for_rtt(1.0));
        assert!(rate_margin_for_rtt(1000.0) <= MAX_RATE_MARGIN + 1e-6);
    }

    #[test]
    fn rate_tracks_loss_not_congestion_classification() {
        // The rate provisions against the measured loss rate and is robust to the
        // congestion classification (whose timing signal is unreliable on this
        // transport): the same measured loss yields the same rate whether classed
        // wireless or congestion, so a false-high congestion reading cannot
        // collapse the code.
        let wireless = rlc_target(&SensorSnapshot {
            loss: 0.20,
            congestion_fraction: 0.0,
            ..SensorSnapshot::default()
        });
        let congested = rlc_target(&SensorSnapshot {
            loss: 0.20,
            congestion_fraction: 0.9,
            ..SensorSnapshot::default()
        });
        assert_eq!(
            congested.step, wireless.step,
            "the rate must track the loss rate, not the congestion classification",
        );
    }

    #[test]
    fn controller_escalates_immediately_on_loss() {
        let mut c = RlcController::new(16, STEP_MAX, DEFAULT_DT, 8);
        // A loss spike must shrink step (raise protection) on the very first tick.
        let before = c.current().step;
        let d = c.decide(&lossy(0.25, 0.5));
        assert!(d.coding_on, "must keep coding on under loss");
        assert!(
            d.step < before,
            "must raise protection (smaller step) at once: {} -> {}",
            before,
            d.step,
        );
    }

    #[test]
    fn controller_holds_protection_through_a_blip() {
        let mut c = RlcController::with_holds(16, 4, 15, 4, 16);
        c.decide(&lossy(0.25, 0.5)); // escalate
        let escalated = c.current();
        // One clean tick must NOT immediately relax protection.
        let d = c.decide(&clean());
        assert_eq!(d.step, escalated.step, "a single clean tick must not relax step");
        assert!(d.coding_on, "a single clean tick must not disable coding");
    }

    #[test]
    fn controller_disables_coding_only_after_sustained_clean() {
        let mut c = RlcController::with_holds(16, 4, 15, 2, 6);
        c.decide(&lossy(0.25, 0.5)); // escalate, coding on
        // Fewer than clean_hold clean ticks: coding stays on.
        for i in 0..5 {
            let d = c.decide(&clean());
            assert!(d.coding_on, "coding dropped too early at clean tick {i}");
        }
        // The clean_hold-th sustained-clean tick disables coding.
        let d = c.decide(&clean());
        assert!(!d.coding_on, "sustained clean must finally disable coding");
    }

    #[test]
    fn latency_floor_holds_baseline_through_clean() {
        // The latency-priority floor holds FEC on AND never relaxes the step
        // lighter than the baseline (4) through a sustained-clean run - the fix
        // for the ARQ-latency cliff where the controller would otherwise relax to
        // an over-light rate (or disable coding) and pay an ARQ round trip on the
        // next loss.
        let mut c = RlcController::with_holds(16, 4, 15, 2, 4);
        c.set_latency_floor();
        c.decide(&lossy(0.25, 0.5)); // escalate under loss (step drops below 4)
        for i in 0..20 {
            let d = c.decide(&clean());
            assert!(d.coding_on, "floor must hold FEC on at clean tick {i}");
            assert!(d.step <= 4, "floor must not relax lighter than baseline 4 (got {})", d.step);
            assert!(d.window >= 16, "floor must not shrink window below baseline 16 (got {})", d.window);
        }
        // The floor still escalates HEAVIER than the baseline under high loss.
        let heavy = c.decide(&lossy(0.30, 0.5));
        assert!(heavy.step < 4, "floor must still escalate heavier under loss (got {})", heavy.step);
        // The same sustained-clean run disables coding WITHOUT the floor (baseline).
        let mut base = RlcController::with_holds(16, 4, 15, 2, 4);
        base.decide(&lossy(0.25, 0.5));
        let mut disabled = false;
        for _ in 0..20 {
            if !base.decide(&clean()).coding_on {
                disabled = true;
                break;
            }
        }
        assert!(disabled, "default controller must disable-on-clean (the baseline)");
    }
}
