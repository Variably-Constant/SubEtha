//! BBR congestion control for the RLC transport's send path.
//!
//! A from-the-spec implementation of BBR (Bottleneck Bandwidth and
//! Round-trip propagation time), Cardwell et al., following:
//!
//! - `draft-cardwell-iccrg-bbr-congestion-control` (the state machine,
//!   the pacing-rate / cwnd formulas, the full-pipe detection), and
//! - `draft-cheng-iccrg-delivery-rate-estimation` (the per-packet
//!   rate-sample bookkeeping).
//!
//! This is the BBRv1 model: the bottleneck is characterised by two
//! quantities the sender can measure, `BtlBw` (the windowed-MAX of the
//! delivery rate) and `RTprop` (the windowed-MIN of the round-trip
//! time), and the sender PACES at `pacing_gain * BtlBw` while bounding
//! in-flight to `cwnd_gain * BDP` (BDP = BtlBw * RTprop). That keeps the
//! bottleneck queue near-empty: throughput at the bottleneck rate with
//! minimal standing queue, which is the whole point, low latency under
//! bufferbloat where a loss-based controller fills the buffer.
//!
//! # Why the rate sampler matters (the part a naive version gets wrong)
//!
//! The delivery rate must NOT be `acked_bytes / ack_arrival_interval`:
//! when a frontier hole fills, the receiver delivers a backlog at once,
//! the ACKs arrive compressed, and that ratio spikes to many times the
//! true link rate. The spec's fix (followed here) snapshots, PER PACKET
//! at send time, the connection's `delivered` count and the time it was
//! last updated; on ACK the rate is `delivered_delta /
//! max(send_elapsed, ack_elapsed)`. Taking the MAX of the send-side and
//! ack-side elapsed intervals makes the estimate robust to both ACK
//! compression (ack_elapsed too small) and send bursts (send_elapsed too
//! small). This is exactly what an earlier hand-rolled
//! windowed-max-of-raw-ACK-delta got wrong.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Startup pacing/cwnd gain `2/ln(2) ~= 2.885`: the smallest gain that
/// doubles the sending rate each round trip, an exponential search for
/// the bottleneck bandwidth.
const STARTUP_GAIN: f64 = 2.0 / std::f64::consts::LN_2;
/// Drain pacing gain `ln(2)/2 ~= 0.35` (the inverse of the startup gain):
/// drains the queue the startup overshoot built, in about one round.
const DRAIN_PACING_GAIN: f64 = std::f64::consts::LN_2 / 2.0;
/// Steady-state in-flight headroom: cwnd = 2 * BDP tolerates delayed/
/// aggregated ACKs without starving the pipe.
const CWND_GAIN: f64 = 2.0;
/// ProbeBW pacing-gain cycle, one phase per RTprop: probe UP at 1.25x for
/// one round to look for more bandwidth, drain at 0.75x the next round,
/// then cruise at 1.0x for six rounds.
const PROBE_BW_GAINS: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
/// Startup is done once BtlBw fails to grow by >=25% for this many rounds
/// (the pipe is full, further probing only builds queue).
const FULL_BW_THRESH: f64 = 1.25;
const FULL_BW_COUNT: u32 = 3;
/// RTprop windowed-min length: a min RTT older than this is stale (the
/// path may have changed), so ProbeRTT re-measures it.
const MIN_RTT_FILTER_LEN: Duration = Duration::from_secs(10);
/// BtlBw windowed-max length, in round trips.
const BW_FILTER_ROUNDS: u64 = 10;
/// ProbeRTT: every `PROBE_RTT_INTERVAL`, hold cwnd at `PROBE_RTT_CWND`
/// for `PROBE_RTT_DURATION` to drain the queue and read a clean RTprop.
const PROBE_RTT_INTERVAL: Duration = Duration::from_secs(10);
const PROBE_RTT_DURATION: Duration = Duration::from_millis(200);
const PROBE_RTT_CWND_PKTS: u64 = 4;
/// Floor on cwnd so the pipe never fully empties.
const MIN_PIPE_CWND_PKTS: u64 = 4;
/// 1% pacing discount, so the sender never quite outpaces the bottleneck.
const PACING_MARGIN: f64 = 0.01;

/// Per-packet rate-sample snapshot, stored by the caller alongside each
/// in-flight packet and handed back when that packet is delivered.
#[derive(Clone, Copy, Debug)]
pub struct PacketSample {
    /// `C.delivered` at the moment this packet was sent.
    delivered: u64,
    /// `C.delivered_time` at the moment this packet was sent.
    delivered_time: Instant,
    /// `C.first_sent_time` at the moment this packet was sent.
    first_sent_time: Instant,
    /// When this packet was sent.
    sent_time: Instant,
    /// Whether the connection was application-limited when this was sent.
    is_app_limited: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Startup,
    Drain,
    ProbeBw,
    ProbeRtt,
}

/// BBR sender-side state.
pub struct Bbr {
    // --- delivery-rate-estimation connection state ---
    /// Total payload bytes the receiver has delivered over the connection.
    delivered: u64,
    /// Wall-clock time `delivered` was last updated.
    delivered_time: Instant,
    /// Send time of the packet most recently marked delivered.
    first_sent_time: Instant,
    /// Payload bytes in flight (sent, not yet delivered), drives the
    /// app-limited detection and the cwnd gate.
    inflight: u64,

    // --- estimates ---
    /// BtlBw: windowed-max of the delivery rate (bytes/s). The deque holds
    /// `(round, rate)`; the front is the current max.
    bw_filter: VecDeque<(u64, f64)>,
    btlbw_bps: f64,
    round_count: u64,
    /// The `delivered` count at which the current round ends (round trip).
    next_round_delivered: u64,
    /// RTprop: windowed-min RTT and when it was stamped.
    min_rtt: Duration,
    min_rtt_stamp: Instant,

    // --- state machine ---
    state: State,
    pacing_gain: f64,
    cwnd_gain: f64,
    /// Startup full-pipe detection.
    full_bw: f64,
    full_bw_count: u32,
    filled_pipe: bool,
    /// ProbeBW gain-cycle phase + when it last advanced.
    cycle_index: usize,
    cycle_stamp: Instant,
    /// ProbeRTT scheduling.
    probe_rtt_done_stamp: Option<Instant>,
    last_probe_rtt: Instant,

    /// Per-packet payload size (fixed in this transport) for cwnd-in-packets.
    packet_bytes: u64,
}

impl Bbr {
    pub fn new(now: Instant, packet_bytes: u64) -> Self {
        let mut bbr = Self {
            delivered: 0,
            delivered_time: now,
            first_sent_time: now,
            inflight: 0,
            bw_filter: VecDeque::new(),
            btlbw_bps: 0.0,
            round_count: 0,
            next_round_delivered: 0,
            min_rtt: Duration::from_secs(10),
            min_rtt_stamp: now,
            state: State::Startup,
            pacing_gain: STARTUP_GAIN,
            cwnd_gain: STARTUP_GAIN,
            full_bw: 0.0,
            full_bw_count: 0,
            filled_pipe: false,
            cycle_index: 0,
            cycle_stamp: now,
            probe_rtt_done_stamp: None,
            last_probe_rtt: now,
            packet_bytes: packet_bytes.max(1),
        };
        bbr.enter_startup();
        bbr
    }

    fn enter_startup(&mut self) {
        self.state = State::Startup;
        self.pacing_gain = STARTUP_GAIN;
        self.cwnd_gain = STARTUP_GAIN;
    }

    fn enter_drain(&mut self) {
        self.state = State::Drain;
        self.pacing_gain = DRAIN_PACING_GAIN;
        self.cwnd_gain = STARTUP_GAIN;
    }

    fn enter_probe_bw(&mut self, now: Instant) {
        self.state = State::ProbeBw;
        self.cwnd_gain = CWND_GAIN;
        self.cycle_index = 0;
        self.pacing_gain = PROBE_BW_GAINS[0];
        self.cycle_stamp = now;
    }

    /// Snapshot the rate-sample state for a packet about to be sent. The
    /// caller stores the returned [`PacketSample`] with the packet and
    /// returns it on delivery via [`on_ack`](Self::on_ack).
    pub fn on_send(&mut self, now: Instant, app_limited: bool) -> PacketSample {
        if self.inflight == 0 {
            // Restart from idle: reset the rate-sample interval origin.
            self.first_sent_time = now;
            self.delivered_time = now;
        }
        self.inflight += self.packet_bytes;
        PacketSample {
            delivered: self.delivered,
            delivered_time: self.delivered_time,
            first_sent_time: self.first_sent_time,
            sent_time: now,
            is_app_limited: app_limited,
        }
    }

    /// Process an ACK delivering `samples` (the snapshots of every packet
    /// newly known-received on this ACK, both the cumulative-ACK advance
    /// and any newly SACKed ids). The RTT is taken from the most recently
    /// sent acked packet, per the spec; picking that packet by its `delivered`
    /// snapshot also makes the rate sample robust to retransmits (a resent
    /// symbol keeps its old, low `delivered`, so it is never picked).
    pub fn on_ack(&mut self, now: Instant, samples: &[PacketSample]) {
        if samples.is_empty() {
            return;
        }
        let n = samples.len() as u64;
        let delivered_bytes = n * self.packet_bytes;
        self.delivered += delivered_bytes;
        self.delivered_time = now;
        self.inflight = self.inflight.saturating_sub(delivered_bytes);

        // The "most recently sent" delivered packet: max prior-delivered.
        let p = samples
            .iter()
            .max_by_key(|s| s.delivered)
            .copied()
            .expect("non-empty");
        self.first_sent_time = p.sent_time;

        // RTprop windowed-min: RTT of the most recently sent acked packet.
        let rtt = now.saturating_duration_since(p.sent_time);
        if rtt > Duration::ZERO {
            let expired = now.saturating_duration_since(self.min_rtt_stamp) > MIN_RTT_FILTER_LEN;
            if rtt <= self.min_rtt || expired {
                self.min_rtt = rtt;
                self.min_rtt_stamp = now;
            }
        }

        // Round-trip accounting (BBRUpdateRound): a round ends when we ACK a
        // packet that was SENT at or after the `delivered` mark captured at the
        // previous round start - i.e. roughly one RTT, one BDP of delivery. The
        // mark is the acked packet's OWN `delivered` snapshot, NOT the running
        // cumulative; using the cumulative ticked a round per ACK batch, making
        // the BtlBw window far shorter than its intended 10 round trips so the
        // peak expired and the estimate decayed.
        let round_start = p.delivered >= self.next_round_delivered;
        if round_start {
            self.next_round_delivered = self.delivered;
            self.round_count += 1;
        }

        // Delivery-rate sample = delivered / max(send_elapsed, ack_elapsed),
        // gated on a reliable interval (>= min RTT). An app-limited sample
        // understates the bandwidth, so it only RAISES the max filter.
        let send_elapsed = p.sent_time.saturating_duration_since(p.first_sent_time);
        let ack_elapsed = now.saturating_duration_since(p.delivered_time);
        let interval = send_elapsed.max(ack_elapsed);
        let rs_delivered = self.delivered - p.delivered;
        if interval >= self.min_rtt && interval > Duration::ZERO {
            let rate = rs_delivered as f64 / interval.as_secs_f64();
            if !p.is_app_limited || rate >= self.btlbw_bps {
                self.update_btlbw(rate);
            }
        }

        if !self.filled_pipe {
            self.check_full_pipe(round_start, p.is_app_limited);
        }
        self.update_state(now);
    }

    fn update_btlbw(&mut self, rate: f64) {
        // Windowed max over BW_FILTER_ROUNDS rounds: drop samples older
        // than the window, then the running max is the front-most peak.
        let floor = self.round_count.saturating_sub(BW_FILTER_ROUNDS);
        while let Some(&(r, _)) = self.bw_filter.front() {
            if r < floor {
                self.bw_filter.pop_front();
            } else {
                break;
            }
        }
        // Maintain a monotonically-decreasing deque of candidates (the
        // classic sliding-window-maximum structure).
        while let Some(&(_, v)) = self.bw_filter.back() {
            if v <= rate {
                self.bw_filter.pop_back();
            } else {
                break;
            }
        }
        self.bw_filter.push_back((self.round_count, rate));
        self.btlbw_bps = self.bw_filter.front().map(|&(_, v)| v).unwrap_or(rate);
    }

    fn check_full_pipe(&mut self, round_start: bool, is_app_limited: bool) {
        if self.filled_pipe || !round_start || is_app_limited {
            return;
        }
        if self.btlbw_bps >= self.full_bw * FULL_BW_THRESH {
            self.full_bw = self.btlbw_bps;
            self.full_bw_count = 0;
            return;
        }
        self.full_bw_count += 1;
        if self.full_bw_count >= FULL_BW_COUNT {
            self.filled_pipe = true;
        }
    }

    fn update_state(&mut self, now: Instant) {
        // ProbeRTT is due periodically regardless of state.
        if self.state != State::ProbeRtt
            && now.saturating_duration_since(self.last_probe_rtt) > PROBE_RTT_INTERVAL
        {
            self.state = State::ProbeRtt;
            self.pacing_gain = 1.0;
            self.cwnd_gain = 1.0;
            self.probe_rtt_done_stamp = None;
            return;
        }
        match self.state {
            State::Startup => {
                if self.filled_pipe {
                    self.enter_drain();
                }
            }
            State::Drain => {
                // Once in-flight has drained to about a BDP, cruise.
                if self.inflight <= self.bdp_bytes() {
                    self.enter_probe_bw(now);
                }
            }
            State::ProbeBw => {
                // Advance the gain cycle one phase per RTprop.
                if now.saturating_duration_since(self.cycle_stamp) >= self.min_rtt {
                    self.cycle_index = (self.cycle_index + 1) % PROBE_BW_GAINS.len();
                    self.pacing_gain = PROBE_BW_GAINS[self.cycle_index];
                    self.cycle_stamp = now;
                }
            }
            State::ProbeRtt => {
                // Hold the reduced cwnd for PROBE_RTT_DURATION once in-flight
                // has fallen to the floor, then resume.
                if self.probe_rtt_done_stamp.is_none()
                    && self.inflight <= PROBE_RTT_CWND_PKTS * self.packet_bytes
                {
                    self.probe_rtt_done_stamp = Some(now + PROBE_RTT_DURATION);
                }
                if let Some(done) = self.probe_rtt_done_stamp
                    && now >= done
                {
                    self.last_probe_rtt = now;
                    self.min_rtt_stamp = now;
                    if self.filled_pipe {
                        self.enter_probe_bw(now);
                    } else {
                        self.enter_startup();
                    }
                }
            }
        }
    }

    fn bdp_bytes(&self) -> u64 {
        (self.btlbw_bps * self.min_rtt.as_secs_f64()) as u64
    }

    /// Target pacing rate in bytes/second: `pacing_gain * BtlBw`, minus the
    /// 1% margin. Zero until the first reliable bandwidth sample lands
    /// (the caller should then fall back to its own flow control).
    pub fn pacing_rate_bps(&self) -> f64 {
        self.pacing_gain * self.btlbw_bps * (1.0 - PACING_MARGIN)
    }

    /// Target congestion window in bytes: `cwnd_gain * BDP`, floored at
    /// `MIN_PIPE_CWND` and reduced to `PROBE_RTT_CWND` during ProbeRTT.
    pub fn cwnd_bytes(&self) -> u64 {
        if self.state == State::ProbeRtt {
            return PROBE_RTT_CWND_PKTS * self.packet_bytes;
        }
        let target = (self.cwnd_gain * self.bdp_bytes() as f64) as u64;
        target.max(MIN_PIPE_CWND_PKTS * self.packet_bytes)
    }

    /// Whether a usable bandwidth estimate exists yet.
    pub fn has_estimate(&self) -> bool {
        self.btlbw_bps > 0.0
    }

    /// BtlBw estimate (bytes/s), telemetry.
    pub fn btlbw_bps(&self) -> f64 {
        self.btlbw_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive `bbr` through a realistic pipelined link: `link_bps` bottleneck,
    /// `rtt` propagation, a `cwnd_pkts` sliding window. Packets enter the
    /// bottleneck serialised at the link rate (so delivery is link-paced) and
    /// are acked one `rtt` after delivery. Events are processed in time order,
    /// exactly as on a real connection (send and ack interleave). `compress`
    /// optionally batches all acks in a round into one instant (ACK
    /// compression) to test the sampler's robustness.
    fn simulate(link_bps: f64, rtt: Duration, pkt: u64, n: u64, compress: bool) -> Bbr {
        let t0 = Instant::now();
        let mut bbr = Bbr::new(t0, pkt);
        bbr.min_rtt = rtt;
        let serialize = Duration::from_secs_f64(pkt as f64 / link_bps); // bottleneck time/pkt
        // Event-ordered sim: maintain the next free bottleneck time and a
        // queue of (deliver_time, sample). Keep ~cwnd packets in flight.
        let cwnd = ((link_bps * rtt.as_secs_f64() / pkt as f64) as u64 + 2).max(4);
        let mut next_bottleneck = t0;
        let mut inflight: std::collections::VecDeque<(Instant, PacketSample)> =
            std::collections::VecDeque::new();
        let mut sent = 0u64;
        let mut now = t0;
        while sent < n || !inflight.is_empty() {
            // Send while the window has room and packets remain.
            while sent < n && (inflight.len() as u64) < cwnd {
                let s = bbr.on_send(now, false);
                // Bottleneck serialises: this packet is delivered when the link
                // is free, plus the propagation delay.
                let deliver = next_bottleneck.max(now) + serialize;
                next_bottleneck = deliver;
                inflight.push_back((deliver + rtt, s)); // ack one rtt after delivery
                sent += 1;
            }
            // Advance to the next ack.
            let Some(&(ack_t, _)) = inflight.front() else { break };
            now = ack_t;
            if compress {
                // Deliver every packet whose ack is due now in one batch.
                let mut batch = Vec::new();
                while let Some(&(t, s)) = inflight.front() {
                    if t <= now {
                        batch.push(s);
                        inflight.pop_front();
                    } else {
                        break;
                    }
                }
                bbr.on_ack(now, &batch);
            } else {
                let (_, s) = inflight.pop_front().unwrap();
                bbr.on_ack(now, std::slice::from_ref(&s));
            }
        }
        bbr
    }

    /// The sampler recovers the true link rate from a clean pipelined stream.
    #[test]
    fn rate_sampler_recovers_link_rate() {
        // 10 Mbit/s = 1.25 MB/s, RTT 50 ms, 1000-byte packets.
        let mbit = simulate(1.25e6, Duration::from_millis(50), 1000, 2000, false).btlbw_bps()
            * 8.0
            / 1e6;
        assert!((8.0..=12.5).contains(&mbit), "btlbw {mbit:.1} mbit/s off true 10");
    }

    /// ACK compression (a whole round acked in one instant) must NOT inflate
    /// btlbw above the true link rate: the send_elapsed term bounds it.
    #[test]
    fn compressed_ack_burst_does_not_inflate() {
        let mbit = simulate(1.25e6, Duration::from_millis(50), 1000, 2000, true).btlbw_bps()
            * 8.0
            / 1e6;
        assert!(mbit < 15.0, "compressed ACKs inflated btlbw to {mbit:.1} mbit/s (true 10)");
    }

    /// A full run fills the pipe and leaves Startup for ProbeBW.
    #[test]
    fn startup_fills_pipe_then_leaves() {
        let bbr = simulate(1.25e6, Duration::from_millis(20), 1000, 4000, false);
        assert!(bbr.filled_pipe, "never detected a full pipe");
        assert!(
            matches!(bbr.state, State::ProbeBw | State::ProbeRtt),
            "did not reach steady state: {:?}",
            bbr.state
        );
    }

    #[test]
    fn pacing_rate_is_gain_times_btlbw() {
        let t0 = Instant::now();
        let mut bbr = Bbr::new(t0, 1000);
        bbr.btlbw_bps = 1_000_000.0; // 1 MB/s
        bbr.pacing_gain = 1.25;
        let r = bbr.pacing_rate_bps();
        assert!((r - 1_250_000.0 * 0.99).abs() < 1.0, "pacing rate {r}");
    }
}
