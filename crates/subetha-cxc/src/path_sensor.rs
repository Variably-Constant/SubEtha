//! Path sensing from the peer's TTL / ECN observations.
//!
//! The receiver reads the IP TTL and ECN bits off every datagram (a passive
//! cmsg, no protocol cost) and echoes them back in a [`PathFrame`]. The
//! sender feeds that stream here to derive two feed-forward signals the
//! adaptive controller fuses alongside loss and delay-trend:
//!
//!  - **Path shift**: a change in hop count means a router-level path change
//!    (a re-route, a link failover). It often precedes a throughput change,
//!    so it pre-arms protection before loss materializes. The signal spikes
//!    to 1.0 on the change and decays.
//!  - **ECN-CE rate**: an AQM router marks Congestion-Experienced *before*
//!    it tail-drops. A rising CE rate is a direct "queue is building" signal
//!    that, like a rising delay trend, calls for protection ahead of loss.
//!
//! The estimator holds no clock and does no I/O - the caller supplies each
//! `(hop_count, ecn)` observation - so it is deterministic and exhaustively
//! testable with synthetic traces.
//!
//! [`PathFrame`]: crate::control_frame::PathFrame

/// The two-bit ECN codepoint marking Congestion Experienced (RFC 3168). An
/// AQM router sets this on a packet it would otherwise have to drop.
pub const ECN_CE: u8 = 0b11;

/// Common initial IP TTL values, smallest first. Hosts start a packet at one
/// of these and every router decrements by one, so the smallest of these that
/// is at least the observed TTL is the likely origin, and the difference is
/// the hop count.
const INITIAL_TTLS: [u8; 3] = [64, 128, 255];

/// Derive a hop count from an observed TTL: pick the smallest standard
/// initial TTL not below the observed value, and subtract.
pub fn hop_count_from_ttl(ttl: u8) -> u8 {
    for &init in &INITIAL_TTLS {
        if ttl <= init {
            return init - ttl;
        }
    }
    0
}

/// Rolling estimator over the peer's path observations.
#[derive(Debug, Default)]
pub struct PathSensor {
    /// Last hop count seen, to detect a change.
    last_hop_count: Option<u8>,
    /// Decaying path-shift signal: 1.0 on a hop-count change, then fading.
    shift: f32,
    /// EWMA of the Congestion-Experienced marking rate, 0..=1.
    ce_rate: f32,
    /// Last raw `(ttl, ecn, hop_count)` echoed, for diagnostics.
    last: Option<(u8, u8, u8)>,
}

impl PathSensor {
    /// A fresh sensor with no observations.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one `(hop_count, ecn)` observation echoed by the peer.
    pub fn observe(&mut self, ttl: u8, ecn: u8, hop_count: u8) {
        let changed = self.last_hop_count.is_some_and(|p| p != hop_count);
        self.last_hop_count = Some(hop_count);
        // A hop-count change spikes the shift signal; an unchanged path lets
        // it decay back toward zero.
        if changed {
            self.shift = 1.0;
        } else {
            self.shift *= 0.85;
        }
        let ce = if ecn & 0b11 == ECN_CE { 1.0 } else { 0.0 };
        self.ce_rate += (ce - self.ce_rate) * 0.2;
        self.last = Some((ttl, ecn, hop_count));
    }

    /// Path-shift signal, 0..=1: high just after a router-level path change.
    pub fn path_shift(&self) -> f32 {
        self.shift
    }

    /// Congestion-Experienced marking rate, 0..=1: a queue-building signal.
    pub fn ecn_ce(&self) -> f32 {
        self.ce_rate
    }

    /// Last observed `(ttl, ecn, hop_count)`, for diagnostics / telemetry.
    pub fn last(&self) -> Option<(u8, u8, u8)> {
        self.last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hop_count_inference_matches_known_initials() {
        // Linux origin (64) eleven hops away.
        assert_eq!(hop_count_from_ttl(53), 11);
        // Windows origin (128) eleven hops away.
        assert_eq!(hop_count_from_ttl(117), 11);
        // Router origin (255), one hop.
        assert_eq!(hop_count_from_ttl(254), 1);
        // Direct (no decrement) from a 64-init host.
        assert_eq!(hop_count_from_ttl(64), 0);
    }

    #[test]
    fn steady_path_keeps_shift_low() {
        let mut s = PathSensor::new();
        for _ in 0..20 {
            s.observe(53, 0, 11);
        }
        assert!(s.path_shift() < 0.05, "steady path -> shift ~0");
    }

    #[test]
    fn hop_count_change_spikes_then_decays() {
        let mut s = PathSensor::new();
        for _ in 0..10 {
            s.observe(53, 0, 11);
        }
        // A re-route: hop count jumps 11 -> 14.
        s.observe(50, 0, 14);
        assert!(s.path_shift() > 0.9, "path shift spikes on hop-count change");
        // Settling back: the signal decays over subsequent steady samples.
        for _ in 0..10 {
            s.observe(50, 0, 14);
        }
        assert!(s.path_shift() < 0.2, "shift decays once the path is steady");
    }

    #[test]
    fn ce_marking_rate_rises_with_congestion() {
        let mut s = PathSensor::new();
        for _ in 0..20 {
            s.observe(53, 0, 11); // ECT, no congestion
        }
        assert!(s.ecn_ce() < 0.05, "no CE -> rate ~0");
        for _ in 0..20 {
            s.observe(53, ECN_CE, 11); // congestion experienced
        }
        assert!(s.ecn_ce() > 0.8, "sustained CE -> rate climbs toward 1");
    }
}
