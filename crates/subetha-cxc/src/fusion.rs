//! Sensor fusion: turn loss / burstiness / delay-trend readings into a
//! coding decision, behind a swappable [`FusionPolicy`] so the
//! arbitration strategy is chosen empirically rather than hard-coded.
//!
//! The hard question - when sensors disagree, who wins, and with what
//! hysteresis so the level does not oscillate - is settled by scoring
//! candidate policies over synthetic traces with [`score_policy`] and
//! picking the best, then confirming on real links. The score rewards
//! fast escalation (cover loss before it hurts) while penalizing
//! oscillation (level flapping) and average parity overhead.

use crate::control_table::CodingLevel;

/// A fused snapshot of the channel from all sensors.
#[derive(Debug, Clone, Copy, Default)]
pub struct SensorSnapshot {
    /// Measured loss fraction, 0..=1 (in-band ground truth).
    pub loss: f32,
    /// Burstiness, 0..=1: how clustered the loss is (radio / temporal).
    pub burstiness: f32,
    /// One-way-delay trend (delay added per unit time). Positive means
    /// the queue is building - congestion-driven loss is imminent.
    pub owd_trend: f32,
    /// Link stress, 0..=1, from the platform link sensor (low signal /
    /// high interface drop rate). A feed-forward predictor of loss.
    pub link_stress: f32,
    /// Path shift, 0..=1, from the peer's echoed TTL: high just after a
    /// hop-count change (a router-level re-route). A feed-forward predictor
    /// of the loss a path change often brings.
    pub path_shift: f32,
    /// ECN Congestion-Experienced rate, 0..=1, from the peer's echoed TOS.
    /// An AQM router marks CE before it tail-drops, so this leads loss the
    /// way a rising delay trend does.
    pub ecn_ce: f32,
    /// Share of recent loss the peer classed congestion (0..=1), from its
    /// `loss_class` report (Biaz + Spike). The congestion share drives parity
    /// up broadly; the wireless share is left to local FEC and does not inflate
    /// effective loss.
    pub congestion_fraction: f32,
    /// Reverse-path (feedback) loss share (0..=1): the fraction of the peer's
    /// feedback the sender missed. Lost feedback impairs ARQ, so a lossy reverse
    /// path nudges FEC to carry more (less reliance on the round trip).
    pub rev_loss: f32,
    /// Self-induced queue delay in milliseconds: `RTT_now - RTprop` from the
    /// BBR path model. A sustained value above [`QUEUE_BLOAT_MS`] is bufferbloat
    /// WE are causing - the rising delay is our own standing queue, not external
    /// congestion, so the answer is to pace down (drain the queue), not to add
    /// FEC parity (which only deepens it).
    pub queue_delay_ms: f32,
    /// Estimated Wi-Fi backhaul-hop count (0..=3) behind the first hop. Each
    /// extra hop is a shared-medium retransmit that raises expected loss, so
    /// more hops bias parity up.
    pub backhaul_hops: u8,
}

/// The coding configuration a policy selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlDecision {
    pub level: CodingLevel,
    pub parity_r: u8,
    pub interleave_depth: u8,
}

/// Trend slope above which the queue is judged to be building.
const OWD_RISING: f32 = 0.02;

/// Link-stress below this is treated as a clean link (the sensor reports
/// tiny nonzero values even on a healthy interface).
const CLEAN_STRESS_EPS: f32 = 0.02;

/// How much the congestion share of measured loss adds to effective loss
/// (parity up, broadly). A wireless drop stays at the base FEC level; a
/// congestion drop drives more protection, since under-protecting a congested
/// path - or over-driving it - is the costlier miss.
const CONGESTION_PARITY_WEIGHT: f32 = 0.5;

/// Standing-queue delay (ms) above which a rising delay trend is judged self-
/// induced bufferbloat rather than external congestion. ~25 ms of queue we are
/// causing means pace down (the flow window drains it); adding FEC parity would
/// only add wire traffic and deepen the queue.
pub const QUEUE_BLOAT_MS: f32 = 25.0;

/// Effective-loss bias per estimated Wi-Fi backhaul hop. Each shared-medium
/// retransmit hop raises expected loss, so parity arms a little higher behind a
/// mesh repeater even before the loss reaches shard accounting.
const BACKHAUL_HOP_PARITY: f32 = 0.03;

/// Whether the path is provably clean: no measured loss, no clustered loss,
/// no rising delay trend, and no feed-forward stress (link / path-shift / ECN /
/// reverse-loss / backhaul). A clean link calls for zero protection - the
/// block-RS path ships at Passthrough, the RLC path disables coding - with ARQ
/// the floor if a rare drop slips through before the controller re-arms. Both
/// codes share this predicate so "clean" means the same thing to each.
pub fn is_clean(s: &SensorSnapshot) -> bool {
    s.loss <= 0.0
        && s.burstiness <= 0.0
        && s.owd_trend <= OWD_RISING
        && s.link_stress < CLEAN_STRESS_EPS
        && s.path_shift < CLEAN_STRESS_EPS
        && s.ecn_ce < CLEAN_STRESS_EPS
        && s.rev_loss < CLEAN_STRESS_EPS
        && s.backhaul_hops == 0
}

/// The effective loss the controller protects against: measured loss plus the
/// feed-forward predictors (congestion share, rising delay trend, path shift,
/// backhaul hops, link stress, ECN, reverse loss), clamped to `0..=1`. A rising
/// delay trend that is OUR OWN standing queue (self-induced bufferbloat) does
/// NOT add protection - the flow-window pacer drains it; adding redundancy would
/// only deepen the queue - so that bump is suppressed above [`QUEUE_BLOAT_MS`].
/// Both the block-RS parity map and the RLC rate law consume this single number,
/// so the two codes assess the channel identically and differ only in how they
/// translate it into coding parameters.
pub fn effective_loss(s: &SensorSnapshot) -> f32 {
    let rising = s.owd_trend > OWD_RISING;
    let shifting = s.path_shift > 0.5;
    let self_induced_bloat = s.queue_delay_ms > QUEUE_BLOAT_MS;
    let rising_bump = if rising && !self_induced_bloat { 0.05 } else { 0.0 };
    (s.loss
        + s.loss * s.congestion_fraction * CONGESTION_PARITY_WEIGHT
        + rising_bump
        + if shifting { 0.05 } else { 0.0 }
        + s.backhaul_hops as f32 * BACKHAUL_HOP_PARITY
        + s.link_stress * 0.1
        + s.ecn_ce * 0.1
        + s.rev_loss * 0.1)
        .min(1.0)
}

/// The configuration the sensors alone call for, before any policy-level
/// hysteresis or timing. Feed-forward: a rising delay trend bumps the
/// level up even while measured loss is still low.
pub fn raw_target(s: &SensorSnapshot) -> ControlDecision {
    // A provably-clean link calls for zero protection (Passthrough): the block
    // ships its data shards with no FEC encode and no parity datagrams; ARQ
    // stays the floor if a rare drop slips through before the controller re-arms.
    if is_clean(s) {
        return ControlDecision {
            level: CodingLevel::Passthrough,
            parity_r: 0,
            interleave_depth: 1,
        };
    }
    // Feed-forward: a rising delay trend, a stressed link, a path shift, and
    // ECN congestion all pre-emptively add protection, as if measured loss
    // were higher, before the loss they predict has materialized.
    let effective_loss = effective_loss(s);
    let parity_r = ((effective_loss * 8.0).ceil() as u8 + 1).clamp(1, 6);
    // Interleave to the burst length the burstiness term encodes - the jitter
    // ratio (heuristic) or the Gilbert-Elliott mean burst / 16 (the burst
    // model). Linear, no gate: a real fitted mean burst of 3-4 must still
    // interleave, which the old `> 0.5` gate (depth 8+) silently dropped.
    let interleave_depth = ((s.burstiness * 16.0).round() as u8).clamp(1, 16);
    let level = if interleave_depth > 1 {
        CodingLevel::Interleave
    } else {
        CodingLevel::Fec
    };
    ControlDecision { level, parity_r, interleave_depth }
}

/// A strategy that maps a sensor snapshot to a coding decision, carrying
/// whatever state (hysteresis counters, last level) it needs.
pub trait FusionPolicy {
    /// Short identifier for scoring output.
    fn name(&self) -> &'static str;
    /// Decide the coding configuration for this snapshot.
    fn decide(&mut self, s: &SensorSnapshot) -> ControlDecision;
}

/// Jump straight to the sensors' raw target every tick - maximally
/// responsive, but flaps when sensors are noisy.
#[derive(Debug, Default)]
pub struct MaxOfSensors;

impl FusionPolicy for MaxOfSensors {
    fn name(&self) -> &'static str {
        "max-of-sensors"
    }
    fn decide(&mut self, s: &SensorSnapshot) -> ControlDecision {
        raw_target(s)
    }
}

/// Raise the level immediately (cheap insurance), but only lower it after
/// `hold` consecutive ticks that all call for a lower level - so a brief
/// dip does not drop protection and the level does not oscillate.
#[derive(Debug)]
pub struct ImmediateUpConservativeDown {
    level: CodingLevel,
    parity_r: u8,
    interleave_depth: u8,
    down_streak: u32,
    hold: u32,
    clean_hold: u32,
}

impl ImmediateUpConservativeDown {
    /// `hold` is the number of consecutive lower-demand ticks required
    /// before de-escalating between FEC levels. Dropping all the way to
    /// Passthrough (zero parity) is the riskiest de-escalation, so it
    /// requires a longer sustained-clean window, `clean_hold`, defaulting
    /// to `4 * hold`.
    pub fn new(hold: u32) -> Self {
        let hold = hold.max(1);
        Self::with_holds(hold, hold.saturating_mul(4))
    }

    /// Like [`new`](Self::new) but with an explicit `clean_hold` (the
    /// sustained-clean streak required before dropping to Passthrough).
    pub fn with_holds(hold: u32, clean_hold: u32) -> Self {
        let hold = hold.max(1);
        Self {
            level: CodingLevel::Fec,
            parity_r: 2,
            interleave_depth: 1,
            down_streak: 0,
            hold,
            clean_hold: clean_hold.max(hold),
        }
    }
}

impl FusionPolicy for ImmediateUpConservativeDown {
    fn name(&self) -> &'static str {
        "immediate-up-conservative-down"
    }
    fn decide(&mut self, s: &SensorSnapshot) -> ControlDecision {
        let t = raw_target(s);
        let up = (t.level as u8) > (self.level as u8)
            || t.parity_r > self.parity_r
            || t.interleave_depth > self.interleave_depth;
        if up {
            // Escalate immediately and reset the down streak.
            self.level = t.level.max_level(self.level);
            self.parity_r = self.parity_r.max(t.parity_r);
            self.interleave_depth = self.interleave_depth.max(t.interleave_depth);
            self.down_streak = 0;
        } else if t == current(self) {
            self.down_streak = 0;
        } else {
            // Lower demand: only step down after a sustained quiet run.
            // Dropping to Passthrough (zero parity) gives up all protection,
            // so it needs the longer `clean_hold` confidence window; steps
            // between FEC levels use the shorter `hold`.
            self.down_streak += 1;
            let threshold = if t.level == CodingLevel::Passthrough {
                self.clean_hold
            } else {
                self.hold
            };
            if self.down_streak >= threshold {
                self.level = t.level;
                self.parity_r = t.parity_r;
                self.interleave_depth = t.interleave_depth;
                self.down_streak = 0;
            }
        }
        current(self)
    }
}

fn current(p: &ImmediateUpConservativeDown) -> ControlDecision {
    ControlDecision {
        level: p.level,
        parity_r: p.parity_r,
        interleave_depth: p.interleave_depth,
    }
}

impl CodingLevel {
    /// The higher of two levels.
    pub fn max_level(self, other: CodingLevel) -> CodingLevel {
        if (self as u8) >= (other as u8) {
            self
        } else {
            other
        }
    }
}

/// Score of a policy over a trace: lower is better.
#[derive(Debug, Clone, Copy)]
pub struct PolicyScore {
    /// Number of times the level changed (oscillation; lower is better).
    pub level_changes: u32,
    /// Mean parity shards (overhead; lower is better).
    pub mean_parity: f32,
    /// Ticks from the first high-loss sample until the level first
    /// reaches `Interleave` (responsiveness; lower is better). `u32::MAX`
    /// if it never escalated while loss was high.
    pub escalation_lag: u32,
}

/// Run `policy` over `trace` and score it. `loss_threshold` defines a
/// "high loss" sample for the escalation-lag measurement.
pub fn score_policy(
    policy: &mut dyn FusionPolicy,
    trace: &[SensorSnapshot],
    loss_threshold: f32,
) -> PolicyScore {
    let mut level_changes = 0u32;
    let mut parity_sum = 0u64;
    let mut prev: Option<CodingLevel> = None;
    let mut first_high: Option<usize> = None;
    let mut escalation_lag = u32::MAX;
    for (i, s) in trace.iter().enumerate() {
        if first_high.is_none() && s.loss >= loss_threshold {
            first_high = Some(i);
        }
        let d = policy.decide(s);
        parity_sum += d.parity_r as u64;
        if let Some(p) = prev
            && p != d.level
        {
            level_changes += 1;
        }
        prev = Some(d.level);
        if escalation_lag == u32::MAX
            && let Some(fh) = first_high
            && d.level as u8 >= CodingLevel::Interleave as u8
        {
            escalation_lag = (i - fh) as u32;
        }
    }
    PolicyScore {
        level_changes,
        mean_parity: parity_sum as f32 / trace.len().max(1) as f32,
        escalation_lag,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Clean -> bursty-loss spike -> clean again.
    fn spike_trace() -> Vec<SensorSnapshot> {
        let mut t = Vec::new();
        for _ in 0..40 {
            t.push(SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        }
        for _ in 0..40 {
            t.push(SensorSnapshot { loss: 0.2, burstiness: 0.7, owd_trend: 0.05, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        }
        for _ in 0..40 {
            t.push(SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        }
        t
    }

    /// Loss flaps on/off every tick - the oscillation stress test.
    fn flapping_trace() -> Vec<SensorSnapshot> {
        (0..80)
            .map(|i| {
                if i % 2 == 0 {
                    SensorSnapshot { loss: 0.25, burstiness: 0.6, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 }
                } else {
                    SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 }
                }
            })
            .collect()
    }

    #[test]
    fn both_escalate_fast_on_a_spike() {
        let spike = spike_trace();
        let mut a = MaxOfSensors;
        let mut b = ImmediateUpConservativeDown::new(8);
        let sa = score_policy(&mut a, &spike, 0.1);
        let sb = score_policy(&mut b, &spike, 0.1);
        assert!(sa.escalation_lag <= 1, "max-of-sensors lag {}", sa.escalation_lag);
        assert!(sb.escalation_lag <= 1, "immediate-up lag {}", sb.escalation_lag);
    }

    #[test]
    fn conservative_down_suppresses_flapping() {
        let flap = flapping_trace();
        let mut a = MaxOfSensors;
        let mut b = ImmediateUpConservativeDown::new(8);
        let sa = score_policy(&mut a, &flap, 0.1);
        let sb = score_policy(&mut b, &flap, 0.1);
        // The conservative-down policy must oscillate far less.
        assert!(
            sb.level_changes < sa.level_changes,
            "immediate-up flapped {} vs max {}",
            sb.level_changes,
            sa.level_changes
        );
    }

    #[test]
    fn raw_target_scales_parity_and_interleave_with_loss() {
        // A provably-clean link calls for Passthrough (zero parity).
        let clean = raw_target(&SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        assert_eq!(clean.parity_r, 0);
        assert_eq!(clean.level, CodingLevel::Passthrough);
        assert_eq!(clean.interleave_depth, 1);
        let lossy = raw_target(&SensorSnapshot { loss: 0.3, burstiness: 0.8, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        assert!(lossy.parity_r >= 3, "parity {}", lossy.parity_r);
        assert!(lossy.interleave_depth >= 2, "depth {}", lossy.interleave_depth);
        assert_eq!(lossy.level, CodingLevel::Interleave);
    }

    #[test]
    fn rising_delay_trend_preempts_before_loss() {
        // No loss yet, but the queue is visibly building.
        let d = raw_target(&SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.1, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        assert_eq!(d.level, CodingLevel::Fec, "rising trend keeps FEC engaged");
    }

    #[test]
    fn link_stress_preempts_parity_before_loss() {
        // No measured loss, but the link sensor reports a degraded link:
        // parity must rise pre-emptively over the clean case.
        let clean =
            raw_target(&SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        let stressed =
            raw_target(&SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.9, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 });
        assert!(
            stressed.parity_r > clean.parity_r,
            "link stress must raise parity: {} vs {}",
            stressed.parity_r,
            clean.parity_r
        );
    }

    #[test]
    fn path_shift_and_ecn_each_pre_arm_parity() {
        // The clean link is Passthrough.
        assert_eq!(raw_target(&clean()).parity_r, 0);
        // A hop-count shift (a router-level re-route) must lift parity off the
        // clean floor before any loss is measured.
        let shifted = raw_target(&SensorSnapshot {
            path_shift: 1.0,
            ..clean()
        });
        assert!(
            shifted.parity_r >= 1,
            "path shift arms parity pre-emptively: {}",
            shifted.parity_r
        );
        // ECN Congestion-Experienced, marked by an AQM router before it drops,
        // does the same.
        let congested = raw_target(&SensorSnapshot {
            ecn_ce: 0.5,
            ..clean()
        });
        assert!(
            congested.parity_r >= 1,
            "ECN-CE arms parity pre-emptively: {}",
            congested.parity_r
        );
    }

    #[test]
    fn congestion_share_raises_parity_over_wireless() {
        // The SAME measured loss, classed wireless vs congestion. The wireless
        // case stays at the base FEC level (recover locally); the congestion
        // case must raise parity (broad protection), since over-driving or
        // under-protecting a congested path is the costlier miss.
        let wireless = raw_target(&SensorSnapshot {
            loss: 0.2,
            congestion_fraction: 0.0,
            ..clean()
        });
        let congestion = raw_target(&SensorSnapshot {
            loss: 0.2,
            congestion_fraction: 1.0,
            ..clean()
        });
        assert!(
            congestion.parity_r > wireless.parity_r,
            "congestion loss must raise parity over wireless: {} vs {}",
            congestion.parity_r,
            wireless.parity_r
        );
    }

    #[test]
    fn reverse_loss_arms_fec_off_the_clean_floor() {
        // A lossy reverse path (feedback) with no measured forward loss must not
        // sit at Passthrough: lost feedback impairs ARQ, so FEC carries more
        // defensively rather than relying on a round trip that is dropping.
        let d = raw_target(&SensorSnapshot {
            rev_loss: 0.3,
            ..clean()
        });
        assert_ne!(d.level, CodingLevel::Passthrough, "reverse loss must arm FEC");
        assert!(d.parity_r >= 1, "reverse loss lifts parity off the clean floor");
    }

    #[test]
    fn self_induced_bloat_suppresses_delay_parity_bump() {
        // A rising delay trend normally pre-arms parity. But when the rising
        // delay is our OWN standing queue (self-induced bufferbloat), adding FEC
        // would only add wire traffic and deepen the queue - the flow-window
        // pacer drains it instead. So the same rising trend must NOT bump parity
        // once queue_delay crosses the bloat threshold.
        let rising_external =
            raw_target(&SensorSnapshot { owd_trend: 0.1, queue_delay_ms: 0.0, ..clean() });
        let rising_self_induced =
            raw_target(&SensorSnapshot { owd_trend: 0.1, queue_delay_ms: 50.0, ..clean() });
        assert!(
            rising_self_induced.parity_r < rising_external.parity_r,
            "self-induced bloat suppresses the delay-driven parity bump: {} vs {}",
            rising_self_induced.parity_r,
            rising_external.parity_r
        );
    }

    #[test]
    fn backhaul_hops_arm_parity_off_the_clean_floor() {
        // A detected Wi-Fi backhaul hop is not a clean link - each shared-medium
        // retransmit hop raises expected loss - so parity arms off the floor and
        // never falls as hops rise.
        assert_eq!(raw_target(&clean()).level, CodingLevel::Passthrough);
        let one_hop = raw_target(&SensorSnapshot { backhaul_hops: 1, ..clean() });
        assert_ne!(one_hop.level, CodingLevel::Passthrough, "a backhaul hop arms FEC");
        assert!(one_hop.parity_r >= 1, "a backhaul hop lifts parity off the floor");
        let three_hop = raw_target(&SensorSnapshot { backhaul_hops: 3, ..clean() });
        assert!(three_hop.parity_r >= one_hop.parity_r, "more hops never lower parity");
    }

    fn clean() -> SensorSnapshot {
        SensorSnapshot { loss: 0.0, burstiness: 0.0, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 }
    }
    fn lossy() -> SensorSnapshot {
        SensorSnapshot { loss: 0.15, burstiness: 0.2, owd_trend: 0.0, link_stress: 0.0, path_shift: 0.0, ecn_ce: 0.0, congestion_fraction: 0.0, rev_loss: 0.0, queue_delay_ms: 0.0, backhaul_hops: 0 }
    }

    #[test]
    fn sustained_clean_drops_to_passthrough_after_clean_hold() {
        let mut p = ImmediateUpConservativeDown::with_holds(2, 10);
        // The first feedback is clean but the policy starts at Fec; it must
        // NOT drop to Passthrough until clean_hold consecutive clean ticks.
        for i in 0..9 {
            let d = p.decide(&clean());
            assert_ne!(d.level, CodingLevel::Passthrough, "dropped too early at tick {i}");
            assert!(d.parity_r >= 1, "lost protection too early at tick {i}");
        }
        // The clean_hold-th clean tick crosses the confidence window.
        let d = p.decide(&clean());
        assert_eq!(d.level, CodingLevel::Passthrough, "should reach Passthrough");
        assert_eq!(d.parity_r, 0, "Passthrough is zero parity");
    }

    #[test]
    fn passthrough_re_arms_instantly_on_loss() {
        let mut p = ImmediateUpConservativeDown::with_holds(2, 4);
        for _ in 0..6 {
            p.decide(&clean());
        }
        assert_eq!(p.decide(&clean()).level, CodingLevel::Passthrough);
        // First lossy tick must re-arm parity immediately - no hold.
        let d = p.decide(&lossy());
        assert!(d.parity_r >= 1, "must re-arm parity on the first loss tick");
        assert_ne!(d.level, CodingLevel::Passthrough, "must leave Passthrough at once");
    }

    #[test]
    fn brief_clean_run_never_drops_protection() {
        // Clean for fewer than clean_hold ticks, then loss: protection must
        // never have dropped to Passthrough.
        let mut p = ImmediateUpConservativeDown::with_holds(2, 20);
        for _ in 0..10 {
            let d = p.decide(&clean());
            assert!(d.parity_r >= 1, "must keep protection during a brief clean run");
        }
        let d = p.decide(&lossy());
        assert!(d.parity_r >= 1);
    }
}
