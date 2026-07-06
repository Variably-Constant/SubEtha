//! BBR-style passive path model: bottleneck bandwidth, round-trip
//! propagation delay, and the bandwidth-delay product, all recovered from
//! the ACK stream the reliable-UDP sender already drives - no probe traffic.
//!
//! The model follows the two-filter structure of BBR (Cardwell, Cheng, Gunn,
//! Yeganeh, Jacobson, *BBR: Congestion-Based Congestion Control*, ACM Queue
//! 2016) and its delivery-rate estimator (Cheng, Cardwell, Yeganeh, Jacobson,
//! *Delivery Rate Estimation*, `draft-cheng-iccrg-delivery-rate-estimation`):
//!
//! - **`BtlBw`** is a windowed *maximum* of the delivery rate. A bottleneck
//!   queue can delay delivery but cannot make data arrive faster than the
//!   link carries it, so the peak delivery rate over a window of round-trips
//!   is the true bottleneck capacity. The max filter rejects the
//!   under-estimates a filling or draining queue injects.
//! - **`RTprop`** is a windowed *minimum* of the round-trip time. A queue
//!   inflates RTT, so the minimum over a long window is the queue-free
//!   propagation delay.
//!
//! Because a queue moves the two estimates in opposite directions (it lifts
//! RTT but never lifts the delivery rate), the max-rate / min-RTT pair
//! separates capacity from delay from a single passive ACK stream. Their
//! product is the bandwidth-delay product `BDP = BtlBw * RTprop`, the
//! in-flight window that keeps the bottleneck busy without standing queue.
//!
//! The estimator is fed connection-level samples: each ACK reports the
//! cumulative `delivered` count and the time, and the rate sample is the
//! delivered delta over the time delta (the `ack_elapsed` rate of the
//! delivery-rate draft). The window for `BtlBw` tracks ~10 round-trips of the
//! current `RTprop`, BBR's round-counted bandwidth window, clamped to a sane
//! range so a brief idle does not discard the estimate.
//!
//! It is a pure model: [`PathModel::on_ack`] takes the cumulative delivered
//! count, a timestamp, and an RTT, so a synthetic ACK trace exercises it
//! deterministically and the live sender feeds it from real feedback.

use std::collections::VecDeque;

/// `RTprop` is held over a 10-second window, matching BBR's `RTpropFilterLen`:
/// long enough to span the lulls between an application's bursts, short enough
/// that a genuine route change is adopted within seconds.
const RTPROP_WINDOW_US: u64 = 10_000_000;

/// `BtlBw`'s window is ~10 round-trips of the current `RTprop` (BBR's
/// `BtlBwFilterLen`), so the bandwidth estimate spans enough round-trips to
/// see the bottleneck's peak but adapts when capacity drops.
const BW_WINDOW_RTTS: u64 = 10;

/// Floor on the `BtlBw` window so a brief application-limited idle (a short
/// gap with no delivery) does not age out the last good bandwidth sample.
const BW_WINDOW_MIN_US: u64 = 200_000;

/// Ceiling on the `BtlBw` window: even at a very large `RTprop` the bandwidth
/// estimate should not stretch past the `RTprop` window itself.
const BW_WINDOW_MAX_US: u64 = 10_000_000;

/// Floor on the delivery-rate *sample* window. Each rate sample spans at least
/// one `RTprop` but never less than this, so a burst of coalesced ACKs (many
/// blocks reported at once after the receiver was briefly silent, or after the
/// unpaced sender bursts into its socket buffer) is averaged over a meaningful
/// interval instead of dividing a large delivered jump by a near-zero gap.
const BW_SAMPLE_FLOOR_US: u64 = 20_000;

/// Most backhaul hops the mesh-hop detector reports (8x throughput reduction).
const MAX_BACKHAUL_HOPS: u8 = 3;

/// `mcs_norm` above which the first hop is judged healthy for mesh detection:
/// the local radio is fine, so a low end-to-end `BtlBw` is a downstream
/// backhaul hop, not a weak first hop.
const MESH_MCS_HEALTHY: f32 = 0.5;

/// Congestion share above which a low `BtlBw` is judged congestion, not a mesh
/// hop: a congested shared link shows the rising-delay, congestion-classed loss
/// the item-3 classifier flags, where a structural backhaul hop does not.
const MESH_MAX_CONGESTION: f32 = 0.5;

/// Sliding-window maximum over timestamped samples, via a monotonic deque:
/// samples are kept in decreasing-value, increasing-time order, so the front
/// is always the current maximum. A new sample evicts every older sample no
/// greater than it (a newer-or-equal value dominates them for all future
/// windows), so the deque stays short and `get` is O(1). This is the exact
/// windowed-max filter BBR uses for `BtlBw`.
struct WindowedMax {
    samples: VecDeque<(u64, u64)>,
    window_us: u64,
}

impl WindowedMax {
    fn new(window_us: u64) -> Self {
        Self { samples: VecDeque::new(), window_us }
    }

    /// Record `value` at `now_us` (timestamps must be non-decreasing).
    fn push(&mut self, now_us: u64, value: u64) {
        while let Some(&(_, back)) = self.samples.back() {
            if back <= value {
                self.samples.pop_back();
            } else {
                break;
            }
        }
        self.samples.push_back((now_us, value));
        self.expire(now_us);
    }

    /// Drop samples older than the window relative to `now_us`. The front is
    /// both the oldest and the maximum, so expiring it promotes the next
    /// largest in-window sample.
    fn expire(&mut self, now_us: u64) {
        let cutoff = now_us.saturating_sub(self.window_us);
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn set_window(&mut self, window_us: u64) {
        self.window_us = window_us;
    }

    fn get(&self) -> u64 {
        self.samples.front().map(|&(_, v)| v).unwrap_or(0)
    }
}

/// Sliding-window minimum, the mirror of [`WindowedMax`]: samples kept in
/// increasing-value, increasing-time order so the front is the current
/// minimum. Used for `RTprop`.
struct WindowedMin {
    samples: VecDeque<(u64, u64)>,
    window_us: u64,
}

impl WindowedMin {
    fn new(window_us: u64) -> Self {
        Self { samples: VecDeque::new(), window_us }
    }

    fn push(&mut self, now_us: u64, value: u64) {
        while let Some(&(_, back)) = self.samples.back() {
            if back >= value {
                self.samples.pop_back();
            } else {
                break;
            }
        }
        self.samples.push_back((now_us, value));
        let cutoff = now_us.saturating_sub(self.window_us);
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn get(&self) -> u64 {
        self.samples.front().map(|&(_, v)| v).unwrap_or(0)
    }
}

/// Passive BBR path model. Holds the windowed `BtlBw` / `RTprop` estimates and
/// exposes them plus the derived BDP. Sized in *blocks* of `block_bytes` so
/// the sender can read the BDP directly as a flow-window target.
pub struct PathModel {
    block_bytes: u64,
    btlbw: WindowedMax,
    rtprop: WindowedMin,
    /// Anchor `(delivered_blocks, ack_time_us, newest_send_us)` of the current
    /// rate-sample window: the delivered count, the ACK arrival time, and the
    /// send time of the newest delivered block, all as of the last emitted
    /// sample. The window grows from here until at least one `RTprop` (floored)
    /// has elapsed; the rate divides by `max(ack_window, send_span)` so neither
    /// a burst of coalesced ACKs nor an in-order frontier leap (a retransmit
    /// unblocking a backlog) can fabricate a peak. `None` until the first ACK
    /// anchors it.
    sample_anchor: Option<(u64, u64, u64)>,
    /// Smoothed recent RTT (SRTT, microseconds); the "RTT_now" the standing-
    /// queue / bufferbloat estimate compares against `RTprop`. 0 until the
    /// first RTT sample.
    srtt_us: u64,
    /// Running sum and count of RTT samples, for the mean RTT under load - the
    /// sustained-latency metric a bufferbloat pacer is judged by (the min RTT
    /// alone only shows the best moment).
    rtt_sum_us: u64,
    rtt_n: u64,
}

impl PathModel {
    /// New model whose BDP is reported in blocks of `block_bytes` (the data
    /// payload per block: `k * item_bytes`, excluding parity and headers, so
    /// the estimate is goodput, not wire rate).
    pub fn new(block_bytes: usize) -> Self {
        Self {
            block_bytes: (block_bytes as u64).max(1),
            btlbw: WindowedMax::new(BW_WINDOW_MIN_US),
            rtprop: WindowedMin::new(RTPROP_WINDOW_US),
            sample_anchor: None,
            srtt_us: 0,
            rtt_sum_us: 0,
            rtt_n: 0,
        }
    }

    /// Fold in one ACK: `delivered_blocks` is the cumulative count the
    /// receiver has delivered, `now_us` the arrival time, `rtt_us` the
    /// round-trip time the just-delivered block measured (0 if none), and
    /// `newest_send_us` the send time of the newest block this ACK delivered.
    ///
    /// A delivery-rate sample is emitted only once the window from the anchor
    /// spans at least one `RTprop` (floored at `BW_SAMPLE_FLOOR_US`); its
    /// rate is the delivered bytes over `max(ack_window, send_span)`. Anchoring
    /// at the last emitted sample - not the previous ACK - averages a run of
    /// coalesced ACKs over the real interval they cover; dividing by the
    /// send-span (the spread of send times across the delivered blocks) caps an
    /// in-order frontier leap - a retransmit unblocking a buffered backlog - at
    /// the rate the blocks were actually sent. Neither can fabricate a peak for
    /// the max filter to latch onto. This is BBR's `max(ack_elapsed,
    /// send_elapsed)` delivery-rate guard, per round trip, on an unpaced sender.
    ///
    /// `delivered_blocks` must be non-decreasing and `now_us` monotonic.
    pub fn on_ack(&mut self, delivered_blocks: u64, now_us: u64, rtt_us: u64, newest_send_us: u64) {
        if rtt_us > 0 {
            self.rtprop.push(now_us, rtt_us);
            // Smoothed recent RTT (RFC 6298 SRTT, alpha = 1/8) - the "RTT_now"
            // the standing-queue estimate compares against RTprop.
            self.srtt_us = if self.srtt_us == 0 {
                rtt_us
            } else {
                self.srtt_us - (self.srtt_us >> 3) + (rtt_us >> 3)
            };
            self.rtt_sum_us += rtt_us;
            self.rtt_n += 1;
            // Track ~10 round-trips of the current RTprop, clamped.
            let window =
                (BW_WINDOW_RTTS * self.rtprop.get()).clamp(BW_WINDOW_MIN_US, BW_WINDOW_MAX_US);
            self.btlbw.set_window(window);
            self.btlbw.expire(now_us);
        }
        match self.sample_anchor {
            None => self.sample_anchor = Some((delivered_blocks, now_us, newest_send_us)),
            Some((anchor_d, anchor_t, anchor_send)) => {
                let min_window = self.rtprop.get().max(BW_SAMPLE_FLOOR_US);
                if delivered_blocks > anchor_d && now_us >= anchor_t + min_window {
                    let delta = delivered_blocks - anchor_d;
                    let data = delta.saturating_mul(self.block_bytes);
                    // Data cannot be delivered faster than it was sent: divide by
                    // the larger of the ACK window and the send span.
                    let ack_window = now_us - anchor_t;
                    let send_span = newest_send_us.saturating_sub(anchor_send);
                    let interval = ack_window.max(send_span).max(1);
                    let rate = data.saturating_mul(1_000_000) / interval;
                    self.btlbw.push(now_us, rate);
                    self.sample_anchor = Some((delivered_blocks, now_us, newest_send_us));
                }
            }
        }
    }

    /// Bottleneck bandwidth estimate in bits per second.
    pub fn btlbw_bps(&self) -> u64 {
        self.btlbw.get().saturating_mul(8)
    }

    /// Round-trip propagation delay estimate in microseconds (0 until the
    /// first RTT sample).
    pub fn rtprop_us(&self) -> u64 {
        self.rtprop.get()
    }

    /// Smoothed recent round-trip time in microseconds (SRTT, 0 until the first
    /// RTT sample) - the "RTT_now" of the standing-queue estimate.
    pub fn rtt_now_us(&self) -> u64 {
        self.srtt_us
    }

    /// Mean RTT in microseconds across all samples - the sustained latency under
    /// load. A bufferbloat pacer is judged by how far this sits below the
    /// un-paced mean (the min RTT alone only shows the best moment).
    pub fn rtt_mean_us(&self) -> u64 {
        self.rtt_sum_us / self.rtt_n.max(1)
    }

    /// Self-induced queue delay in microseconds: `RTT_now - RTprop`. A
    /// sustained value above ~25 ms during our own transfer is bufferbloat we
    /// are causing - the signal to pace down rather than blast. 0 before the
    /// first RTT sample, and clamped at 0 (the smoothed RTT can dip a hair
    /// below the windowed-min RTprop between samples).
    pub fn queue_delay_us(&self) -> u64 {
        self.srtt_us.saturating_sub(self.rtprop.get())
    }

    /// Bandwidth-delay product in bytes (`BtlBw * RTprop`).
    pub fn bdp_bytes(&self) -> u64 {
        self.btlbw.get().saturating_mul(self.rtprop.get()) / 1_000_000
    }

    /// Bandwidth-delay product in blocks - the in-flight window that keeps the
    /// bottleneck busy with no standing queue.
    pub fn bdp_blocks(&self) -> u64 {
        self.bdp_bytes() / self.block_bytes
    }

    /// Estimated number of Wi-Fi backhaul hops (0..=3) behind the first hop.
    ///
    /// A single-radio repeater receives then retransmits on the SAME channel;
    /// carrier-sense self-interference roughly halves throughput per hop. So
    /// with `nominal_bps` the single-hop PHY rate (the first-hop MCS, item 5)
    /// and `BtlBw` the measured end-to-end bottleneck (item 6),
    /// `round(log2(nominal / BtlBw))` is the backhaul-hop count - 2x for one
    /// hop, 4x for two, 8x for three.
    ///
    /// Gated so real congestion does not read as a mesh hop: the first hop must
    /// be healthy (`mcs_norm` high - the local radio is fine, so the reduction
    /// is downstream) AND the loss must NOT be congestion-classed
    /// (`congestion_fraction` low). A single-radio repeater's penalty is a
    /// structural bandwidth halving with no extra loss, whereas a congested
    /// shared link shows the rising-delay, congestion-classed loss the item-3
    /// classifier flags - so the loss class, not an RTT-inflation proxy, is the
    /// discriminator (real repeaters add bandwidth penalty, not latency). This
    /// answers what TTL cannot - an L2-bridged repeater does not decrement the
    /// IP TTL, but its performance signature is unmistakable.
    pub fn backhaul_hops(&self, nominal_bps: u64, mcs_norm: f32, congestion_fraction: f32) -> u8 {
        let btlbw = self.btlbw_bps();
        if btlbw == 0 || nominal_bps <= btlbw {
            // No bandwidth estimate yet, or no halving at all (the bottleneck is
            // at least the single-hop rate - certainly not behind a repeater).
            return 0;
        }
        let hops = (nominal_bps as f64 / btlbw as f64)
            .log2()
            .round()
            .clamp(0.0, MAX_BACKHAUL_HOPS as f64) as u8;
        let first_hop_healthy = mcs_norm > MESH_MCS_HEALTHY;
        let not_congested = congestion_fraction < MESH_MAX_CONGESTION;
        if hops >= 1 && first_hop_healthy && not_congested {
            hops
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A steady stream at a known rate and RTT recovers `BtlBw` within
    /// tolerance and `RTprop` exactly (min filter, within one sample), and the
    /// BDP is their product. This is the synthetic-trace proof.
    #[test]
    fn recovers_known_rate_and_rtprop() {
        // 1000-byte blocks, 10 blocks every 1000 us = 10_000 bytes / 1 ms =
        // 1e7 bytes/s = 80 Mbit/s. Fixed RTT 5 ms.
        let mut m = PathModel::new(1000);
        let mut delivered = 0u64;
        let mut now = 0u64;
        for _ in 0..200 {
            delivered += 10;
            now += 1000;
            m.on_ack(delivered, now, 5000, now);
        }
        let bps = m.btlbw_bps();
        assert!(
            (78_000_000..=82_000_000).contains(&bps),
            "BtlBw {bps} bps not within tolerance of 80 Mbit/s"
        );
        assert_eq!(m.rtprop_us(), 5000, "RTprop is the exact min RTT");
        // BDP = 1e7 bytes/s * 5e-3 s = 50_000 bytes = 50 blocks.
        assert_eq!(m.bdp_blocks(), 50, "BDP in blocks = BtlBw * RTprop");
    }

    /// A queue that inflates RTT must NOT lower `BtlBw` (the max filter holds
    /// the bottleneck peak) and must NOT lift `RTprop` (the min filter holds
    /// the bloat-free path). This is the capacity/delay separation.
    #[test]
    fn queue_does_not_corrupt_estimates() {
        let mut m = PathModel::new(1000);
        let mut delivered = 0u64;
        let mut now = 0u64;
        // Warm up at the true path: rate 1e7 B/s, RTT 5 ms.
        for _ in 0..50 {
            delivered += 10;
            now += 1000;
            m.on_ack(delivered, now, 5000, now);
        }
        let bw_before = m.btlbw_bps();
        // A standing queue forms: same delivery rate, RTT climbs to 20 ms.
        for _ in 0..50 {
            delivered += 10;
            now += 1000;
            m.on_ack(delivered, now, 20_000, now);
        }
        assert_eq!(m.rtprop_us(), 5000, "RTprop unmoved by the queue (min filter)");
        assert!(
            m.btlbw_bps() >= bw_before,
            "BtlBw not lowered by the queue (max filter)"
        );
    }

    /// A capacity drop is adopted once the old high-rate samples age out of
    /// the `BtlBw` window.
    #[test]
    fn adopts_lower_capacity_after_window() {
        let mut m = PathModel::new(1000);
        let mut delivered = 0u64;
        let mut now = 0u64;
        // Fast: 1e7 B/s at 5 ms RTT. The BtlBw window is
        // max(10 * RTprop, 200 ms floor) = 200 ms, so the last fast sample
        // (at t = 50 ms) ages out only after now passes 50 ms + 200 ms.
        for _ in 0..50 {
            delivered += 10;
            now += 1000;
            m.on_ack(delivered, now, 5000, now);
        }
        let fast = m.btlbw_bps();
        // Capacity halves: 5 blocks per ms. Run well past the 200 ms window
        // (to now = 350 ms, comfortably past the 250 ms ageout boundary).
        for _ in 0..300 {
            delivered += 5;
            now += 1000;
            m.on_ack(delivered, now, 5000, now);
        }
        let slow = m.btlbw_bps();
        assert!(slow < fast, "BtlBw drops once the fast samples age out");
        assert!(
            (38_000_000..=42_000_000).contains(&slow),
            "BtlBw {slow} bps tracks the halved 40 Mbit/s capacity"
        );
    }

    /// Backhaul-hop count is `round(log2(nominal / BtlBw))`, clamped 0..=3, but
    /// only when the first hop is healthy AND the loss is not congestion-classed
    /// - so a congested shared link does not read as a mesh hop.
    #[test]
    fn backhaul_hops_from_capacity_ratio_gated() {
        let mut m = PathModel::new(1000);
        let mut d = 0u64;
        let mut t = 0u64;
        // Establish BtlBw = 1e7 B/s = 8e7 bit/s.
        for _ in 0..100 {
            d += 10;
            t += 1000;
            m.on_ack(d, t, 5000, t);
        }
        let btlbw = m.btlbw_bps();
        assert!((78_000_000..=82_000_000).contains(&btlbw));
        // nominal = 2x BtlBw, first hop healthy, loss not congestion-classed:
        // one backhaul hop (the repeater halves throughput).
        assert_eq!(m.backhaul_hops(2 * btlbw, 0.9, 0.0), 1);
        assert_eq!(m.backhaul_hops(4 * btlbw, 0.9, 0.0), 2, "4x -> two hops");
        assert_eq!(m.backhaul_hops(16 * btlbw, 0.9, 0.0), 3, "8x+ clamps at three");
        // Gate 1: a weak first hop (low mcs) means the loss is local.
        assert_eq!(m.backhaul_hops(2 * btlbw, 0.3, 0.0), 0, "weak first hop is not a hop");
        // Gate 2: congestion-classed loss reads as congestion, not a mesh hop.
        assert_eq!(m.backhaul_hops(2 * btlbw, 0.9, 0.9), 0, "congestion is not a mesh hop");
        // No halving at all -> zero hops.
        assert_eq!(m.backhaul_hops(btlbw, 0.9, 0.0), 0);
    }

    /// No RTT samples (RTT always 0) leaves `RTprop` and the BDP at zero
    /// without panicking - the model degrades cleanly before the first
    /// round-trip is measured.
    #[test]
    fn no_rtt_samples_is_safe() {
        let mut m = PathModel::new(1000);
        m.on_ack(10, 1000, 0, 1000);
        m.on_ack(20, 2000, 0, 2000);
        assert_eq!(m.rtprop_us(), 0);
        assert_eq!(m.bdp_blocks(), 0);
    }

    /// A run of coalesced ACKs that reports a large delivered jump over a
    /// near-zero inter-ACK gap must NOT explode `BtlBw`. The anchor-based
    /// minimum window means the burst alone (before the window elapses) emits
    /// nothing, and the eventual sample divides by the real window, not the
    /// near-zero gap - so the estimate stays bounded instead of latching a
    /// divide-by-tiny peak.
    #[test]
    fn coalesced_burst_does_not_explode_btlbw() {
        let mut m = PathModel::new(1000);
        m.on_ack(0, 0, 5000, 0); // anchor = (0, 0, 0), RTprop = 5 ms
        // 1000 blocks reported just 1 us later (a coalesced ACK / socket-buffer
        // burst). The window from the anchor is 1 us, far below the floor, so
        // no sample is emitted - the per-ACK divide-by-near-zero never happens.
        m.on_ack(1000, 1, 5000, 1);
        assert_eq!(
            m.btlbw_bps(),
            0,
            "a sub-window coalesced burst emits no rate sample"
        );
        // A full window later a single bounded sample emits - the delivered
        // bytes over the real window, never the 1e15-ish explosion a per-ACK
        // 1 us gap would have produced.
        m.on_ack(1100, 25_000, 5000, 25_000);
        let bps = m.btlbw_bps();
        assert!(
            (1..1_000_000_000).contains(&bps),
            "after the window a bounded sample emits (got {bps} bps), not a divide-by-near-zero peak"
        );
    }

    /// The standing-queue estimate is zero at the propagation RTT and rises to
    /// the queue depth when a deep buffer fills, while `RTprop` holds the
    /// bloat-free minimum. This is the bufferbloat signal item 7 reads.
    #[test]
    fn queue_delay_tracks_standing_queue() {
        let mut m = PathModel::new(1000);
        let mut d = 0u64;
        let mut t = 0u64;
        // Establish RTprop at 5 ms with steady delivery.
        for _ in 0..30 {
            d += 10;
            t += 1000;
            m.on_ack(d, t, 5000, t);
        }
        assert_eq!(m.rtprop_us(), 5000);
        assert!(m.queue_delay_us() < 2000, "no standing queue at the propagation RTT");
        // A deep queue forms: RTT climbs to 60 ms and stays there.
        for _ in 0..60 {
            d += 10;
            t += 1000;
            m.on_ack(d, t, 60_000, t);
        }
        assert_eq!(m.rtprop_us(), 5000, "RTprop still the bloat-free minimum");
        let qd = m.queue_delay_us();
        assert!(qd > 40_000, "queue delay {qd} us reflects the ~55 ms standing queue");
    }

    /// An in-order frontier leap - a head-of-line loss stalls `ack_through`,
    /// then a retransmit unblocks a buffered backlog so the delivered count
    /// jumps hundreds of blocks in one ACK window - must be capped at the rate
    /// the blocks were actually SENT, not the narrow ACK window the leap landed
    /// in. This is the contaminant that fabricated a 45 Gbit/s `BtlBw` over
    /// real lossy Wi-Fi.
    #[test]
    fn frontier_leap_capped_by_send_span() {
        let mut m = PathModel::new(1000);
        m.on_ack(0, 0, 5000, 0); // anchor: delivered 0, sent at t=0
        // 500 buffered blocks unblock at once: delivered leaps 0 -> 500 inside a
        // 20 ms ACK window, but those blocks were SENT over 100 ms. The ACK
        // window alone would read 500 * 1000 B / 20 ms = 2.5e7 B/s; the send
        // span caps it at 500 * 1000 B / 100 ms = 5e6 B/s = 40 Mbit/s.
        m.on_ack(500, 20_000, 5000, 100_000);
        let bps = m.btlbw_bps();
        assert!(
            (38_000_000..=42_000_000).contains(&bps),
            "leap rate {bps} bps capped by the 100 ms send span, not the 20 ms ACK window"
        );
    }
}
