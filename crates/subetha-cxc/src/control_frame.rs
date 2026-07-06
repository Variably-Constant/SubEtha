//! The control plane as a QUIC-style frame container.
//!
//! A single `CONTROL` datagram carries a sequence of type-tagged,
//! length-prefixed frames. Both endpoints emit `CONTROL` datagrams holding
//! whatever frames they have to report, so the channel is symmetric: an ACK
//! from the receiver and a TIMING beat from the sender are the same packet
//! shape, just different frames.
//!
//! The point of the framing is extensibility without a version bump. A new
//! between-endpoint signal - a hop-count delta, an ECN-CE marking, a peer's
//! link class - becomes a new [`FrameType`], not a new fixed layout. Frames
//! a peer does not recognize are length-skipped, so old and new builds
//! interoperate by ignoring each other's unknown frames rather than
//! mis-parsing the rest of the packet.
//!
//! Wire shape:
//!
//! ```text
//! [PKT_CONTROL] ( [frame_type:u8] [length:varint] [payload: length bytes] )*
//! ```
//!
//! Integers wider than a byte use the QUIC variable-length encoding
//! (RFC 9000 section 16): the top two bits of the first byte select a
//! 1/2/4/8-byte form, so a small value costs one byte. Byte-sized fields are
//! written raw. The codec is pure and does no I/O, so it is exhaustively
//! testable against synthetic frame sequences.

/// Packet-type tag for a control datagram (vs `PKT_DATA`). Distinct from the
/// retired fixed `PKT_FEEDBACK` / `PKT_HEARTBEAT` tags, which this container
/// subsumes.
pub const PKT_CONTROL: u8 = 4;

/// Frame type tags. Stable on the wire; append new variants, never renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Cumulative ack frontier (receiver -> sender).
    Ack = 0x01,
    /// Selective negative ack: a block and its missing-shard bitmap.
    Nak = 0x02,
    /// Fused loss / burstiness / delay-trend readings (receiver -> sender).
    Loss = 0x03,
    /// Sender clock beat plus the peer beat being echoed, for RTT and OWD.
    Timing = 0x04,
    /// Source-ring shape telemetry (the legacy heartbeat payload).
    Ring = 0x05,
    /// The peer's observed TTL / ECN / hop-count of THIS endpoint's packets.
    Path = 0x06,
    /// The peer's link class and normalized quality.
    Link = 0x07,
    /// Highest peer sequence the sender of this frame has seen, for
    /// bidirectional (forward vs reverse) loss accounting.
    LossAcct = 0x08,
    /// The peer's observed path MTU.
    Pmtu = 0x09,
    /// One member of an active bandwidth probe train (packet-pair / chirp).
    BwProbe = 0x0A,
    /// A mini-traceroute marker riding the control stream at a chosen TTL.
    Trace = 0x0B,
    /// The receiver's WBest available-bandwidth estimate (reverse-reported so the
    /// sender can cross-check its passive BtlBw).
    AvailBw = 0x0C,
    /// The receiver's Sprout-style forecast of the next-tick deliverable rate
    /// (5th-percentile lower bound), so the sender pre-sizes ahead of a dip.
    Forecast = 0x0D,
    /// The receiver's detected LEO handover cadence and seconds-to-next-spike, so
    /// the sender pre-arms protection one cycle ahead of a periodic delay spike.
    Periodicity = 0x0E,
}

impl FrameType {
    /// Map a wire tag to a known frame type, or `None` for an unrecognized
    /// (length-skippable) frame.
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0x01 => Self::Ack,
            0x02 => Self::Nak,
            0x03 => Self::Loss,
            0x04 => Self::Timing,
            0x05 => Self::Ring,
            0x06 => Self::Path,
            0x07 => Self::Link,
            0x08 => Self::LossAcct,
            0x09 => Self::Pmtu,
            0x0A => Self::BwProbe,
            0x0B => Self::Trace,
            0x0C => Self::AvailBw,
            0x0D => Self::Forecast,
            0x0E => Self::Periodicity,
            _ => return None,
        })
    }
}

/// Cumulative ack frontier: the next block the receiver still needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AckFrame {
    pub ack_through: u32,
}

/// Selective NAK: which shards of which block are still missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NakFrame {
    pub block: u32,
    pub mask: u32,
}

/// Fused channel readings the receiver reports to the sender's controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LossFrame {
    pub loss_x255: u8,
    pub burstiness_x255: u8,
    pub owd_trend_class: u8,
    /// Loss-class code (0 = no loss, 1 = wireless, 2 = congestion, 3 = mixed)
    /// from the receiver's [`crate::loss_class_sensor`], so the sender's
    /// controller can treat congestion and wireless loss differently.
    pub loss_class: u8,
}

/// Sender clock beat. `echo_ts` reflects the peer's last `send_ts` back, so
/// either end can compute RTT; `send_ts` alone drives the OWD-trend slope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimingFrame {
    pub send_ts: u64,
    pub echo_ts: u64,
}

/// Source-ring shape telemetry: the legacy heartbeat payload, now a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RingFrame {
    pub fill_pct: u8,
    pub ring_kind: u8,
    pub producers: u8,
    pub consumers: u8,
    pub trend: u8,
    pub flags: u8,
}

/// The peer's view of THIS endpoint's packets: the TTL it saw, the ECN bits,
/// and the hop count it derived from the TTL. A change in `hop_count` is a
/// router-level path shift, often visible before throughput moves.
///
/// AccECN (item 15): `ce_count` / `ect_count` are the peer's CUMULATIVE counts of
/// our CE-marked and ECN-capable packets, so the sender derives a graded
/// `ce_rate = delta_CE / delta_ECT` between frames instead of reading a single
/// CE bit. An AQM marks CE before it tail-drops, so a rising rate leads loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PathFrame {
    pub ttl: u8,
    pub ecn: u8,
    pub hop_count: u8,
    pub ce_count: u64,
    pub ect_count: u64,
}

/// The peer's link class and a normalized 0..=255 quality (RSSI / RSRP /
/// link-rate). A class change (wifi -> cellular) is a handoff announcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LinkFrame {
    pub class: u8,
    pub quality: u8,
}

/// Link-class enum carried in [`LinkFrame::class`].
pub mod link_class {
    pub const UNKNOWN: u8 = 0;
    pub const LOOPBACK: u8 = 1;
    pub const WIRED: u8 = 2;
    pub const WIFI: u8 = 3;
    pub const CELLULAR: u8 = 4;
}

/// Bidirectional control-plane loss accounting. `seq` is the count of control
/// packets this endpoint has SENT; `last_recv_seq` is the count it has RECEIVED
/// from the peer. Pairing the two separates forward-path loss (the peer did not
/// get your packets: your `seq` minus the peer's reported `last_recv_seq`) from
/// reverse-path loss (you did not get the peer's: the peer's `seq` minus your
/// `last_recv_seq`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LossAcctFrame {
    pub seq: u32,
    pub last_recv_seq: u32,
}

/// The peer's observed path MTU. A drop (1500 -> ~1280) flags a lower-MTU
/// link engaging, e.g. a cellular handoff; the frame size should track it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PmtuFrame {
    pub pmtu: u16,
}

/// One member of a bandwidth-probe train. The receiver measures inter-arrival
/// dilation across a train sharing `probe_id` to estimate available bandwidth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BwProbeFrame {
    pub probe_id: u8,
    pub idx: u8,
    pub send_ts: u64,
}

/// A mini-traceroute marker: a control packet emitted at a reduced IP TTL so
/// an intermediate router replies with ICMP TimeExceeded, exposing per-hop
/// RTT without a separate probe flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceFrame {
    pub hop_ttl: u8,
    pub probe_id: u8,
}

/// The receiver's WBest available-bandwidth estimate, reverse-reported so the
/// sender can cross-check its passive BtlBw. Carried in kbit/s so a multi-Gbit
/// estimate fits a varint without floating point on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AvailBwFrame {
    /// Available bandwidth in kbit/s (0 = no estimate yet).
    pub avail_kbps: u64,
    /// Effective capacity in kbit/s (the WBest stage-1 median), for the
    /// cross-check against the passive BtlBw.
    pub capacity_kbps: u64,
}

/// The receiver's Sprout-style forecast (item 16): the 5th-percentile deliverable
/// rate it predicts for the next tick, so the sender pre-sizes its window ahead
/// of a dip instead of reacting after the loss the dip causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ForecastFrame {
    /// Forecast deliverable rate in kbit/s (0 = no forecast yet).
    pub forecast_kbps: u64,
}

/// The receiver's LEO handover-cadence detection (item 17): the detected period
/// and the time to the next predicted delay spike, both in deciseconds (0.1 s),
/// plus a confidence, so the sender pre-arms one cycle ahead. `period_ds == 0`
/// means no cadence detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeriodicityFrame {
    pub period_ds: u64,
    pub secs_to_spike_ds: u64,
    pub confidence_x255: u8,
}

/// A decoded control packet: every frame is optional, so a packet carries
/// exactly the signals its sender had to report. Probe and trace frames may
/// repeat (a train), so they are collected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlPacket {
    pub ack: Option<AckFrame>,
    pub nak: Option<NakFrame>,
    pub loss: Option<LossFrame>,
    pub timing: Option<TimingFrame>,
    pub ring: Option<RingFrame>,
    pub path: Option<PathFrame>,
    pub link: Option<LinkFrame>,
    pub loss_acct: Option<LossAcctFrame>,
    pub pmtu: Option<PmtuFrame>,
    pub bw_probe: Vec<BwProbeFrame>,
    pub trace: Vec<TraceFrame>,
    pub avail_bw: Option<AvailBwFrame>,
    pub forecast: Option<ForecastFrame>,
    pub periodicity: Option<PeriodicityFrame>,
}

impl ControlPacket {
    /// An empty packet (no frames).
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` if the packet carries no frames at all (nothing to send).
    pub fn is_empty(&self) -> bool {
        self.ack.is_none()
            && self.nak.is_none()
            && self.loss.is_none()
            && self.timing.is_none()
            && self.ring.is_none()
            && self.path.is_none()
            && self.link.is_none()
            && self.loss_acct.is_none()
            && self.pmtu.is_none()
            && self.bw_probe.is_empty()
            && self.trace.is_empty()
    }
}

/// `true` if `buf` is a control datagram.
pub fn is_control(buf: &[u8]) -> bool {
    !buf.is_empty() && buf[0] == PKT_CONTROL
}

// --- QUIC variable-length integer codec (RFC 9000 section 16) ---

/// Append `v` to `out` in the smallest QUIC varint form. Values must fit 62
/// bits (every field here does); a wider value is clamped to the 62-bit max
/// rather than corrupting the stream.
fn put_varint(out: &mut Vec<u8>, v: u64) {
    const MAX62: u64 = (1 << 62) - 1;
    let v = v.min(MAX62);
    if v < (1 << 6) {
        out.push(v as u8);
    } else if v < (1 << 14) {
        out.push(0x40 | (v >> 8) as u8);
        out.push(v as u8);
    } else if v < (1 << 30) {
        out.push(0x80 | (v >> 24) as u8);
        out.extend_from_slice(&(v as u32).to_be_bytes()[1..]);
    } else {
        out.push(0xC0 | (v >> 56) as u8);
        out.extend_from_slice(&v.to_be_bytes()[1..]);
    }
}

/// Read a QUIC varint at `pos`, returning `(value, next_pos)` or `None` if
/// the buffer is too short for the encoded length.
fn get_varint(buf: &[u8], pos: usize) -> Option<(u64, usize)> {
    let first = *buf.get(pos)?;
    let len = 1usize << (first >> 6);
    if pos + len > buf.len() {
        return None;
    }
    let mut v = (first & 0x3F) as u64;
    for &b in &buf[pos + 1..pos + len] {
        v = (v << 8) | b as u64;
    }
    Some((v, pos + len))
}

// --- frame body helpers: write a [type][len][payload] frame into `out` ---

/// Write one frame: its type tag, the varint length of `body`, then `body`.
fn put_frame(out: &mut Vec<u8>, ty: FrameType, body: &[u8]) {
    out.push(ty as u8);
    put_varint(out, body.len() as u64);
    out.extend_from_slice(body);
}

/// Pad an encoded control datagram up to `target_len` bytes by appending one
/// unknown-type frame (which the decoder length-skips). An active bandwidth
/// probe rides a known, large datagram so its inter-arrival dispersion is a
/// capacity measurement at that packet size; this is how it reaches that size
/// without inventing a payload the peer must understand. No-op when the gap is
/// too small to hold the padding frame's 3-byte header plus a 64-byte body
/// (the threshold that keeps the length varint exactly two bytes, so the final
/// datagram is exactly `target_len`).
pub fn pad_control_to(buf: &mut Vec<u8>, target_len: usize) {
    const PAD_TYPE: u8 = 0x7F;
    const HEADER: usize = 3; // PAD_TYPE + a 2-byte varint length
    if target_len < buf.len() + HEADER + 64 {
        return;
    }
    let body_len = target_len - buf.len() - HEADER;
    buf.push(PAD_TYPE);
    put_varint(buf, body_len as u64);
    buf.resize(buf.len() + body_len, 0);
}

/// Encode a control packet into a fresh datagram buffer.
pub fn encode_control(p: &ControlPacket) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(PKT_CONTROL);
    let mut body = Vec::with_capacity(16);

    if let Some(f) = p.ack {
        body.clear();
        put_varint(&mut body, f.ack_through as u64);
        put_frame(&mut out, FrameType::Ack, &body);
    }
    if let Some(f) = p.nak {
        body.clear();
        put_varint(&mut body, f.block as u64);
        put_varint(&mut body, f.mask as u64);
        put_frame(&mut out, FrameType::Nak, &body);
    }
    if let Some(f) = p.loss {
        put_frame(
            &mut out,
            FrameType::Loss,
            &[f.loss_x255, f.burstiness_x255, f.owd_trend_class, f.loss_class],
        );
    }
    if let Some(f) = p.timing {
        body.clear();
        put_varint(&mut body, f.send_ts);
        put_varint(&mut body, f.echo_ts);
        put_frame(&mut out, FrameType::Timing, &body);
    }
    if let Some(f) = p.ring {
        put_frame(
            &mut out,
            FrameType::Ring,
            &[
                f.fill_pct,
                f.ring_kind,
                f.producers,
                f.consumers,
                f.trend,
                f.flags,
            ],
        );
    }
    if let Some(f) = p.path {
        body.clear();
        body.extend_from_slice(&[f.ttl, f.ecn, f.hop_count]);
        put_varint(&mut body, f.ce_count);
        put_varint(&mut body, f.ect_count);
        put_frame(&mut out, FrameType::Path, &body);
    }
    if let Some(f) = p.link {
        put_frame(&mut out, FrameType::Link, &[f.class, f.quality]);
    }
    if let Some(f) = p.loss_acct {
        body.clear();
        put_varint(&mut body, f.seq as u64);
        put_varint(&mut body, f.last_recv_seq as u64);
        put_frame(&mut out, FrameType::LossAcct, &body);
    }
    if let Some(f) = p.pmtu {
        body.clear();
        put_varint(&mut body, f.pmtu as u64);
        put_frame(&mut out, FrameType::Pmtu, &body);
    }
    for f in &p.bw_probe {
        body.clear();
        body.push(f.probe_id);
        body.push(f.idx);
        put_varint(&mut body, f.send_ts);
        put_frame(&mut out, FrameType::BwProbe, &body);
    }
    for f in &p.trace {
        put_frame(&mut out, FrameType::Trace, &[f.hop_ttl, f.probe_id]);
    }
    if let Some(f) = p.avail_bw {
        body.clear();
        put_varint(&mut body, f.avail_kbps);
        put_varint(&mut body, f.capacity_kbps);
        put_frame(&mut out, FrameType::AvailBw, &body);
    }
    if let Some(f) = p.forecast {
        body.clear();
        put_varint(&mut body, f.forecast_kbps);
        put_frame(&mut out, FrameType::Forecast, &body);
    }
    if let Some(f) = p.periodicity {
        body.clear();
        put_varint(&mut body, f.period_ds);
        put_varint(&mut body, f.secs_to_spike_ds);
        body.push(f.confidence_x255);
        put_frame(&mut out, FrameType::Periodicity, &body);
    }
    out
}

/// Decode a control datagram. Unknown frame types are length-skipped; a
/// frame whose declared length runs past the buffer aborts the parse and
/// returns whatever was decoded up to that point. Returns `None` only if the
/// packet is not a control datagram.
pub fn decode_control(buf: &[u8]) -> Option<ControlPacket> {
    if !is_control(buf) {
        return None;
    }
    let mut p = ControlPacket::new();
    let mut pos = 1usize;
    while pos < buf.len() {
        let ty = buf[pos];
        pos += 1;
        let (len, next) = match get_varint(buf, pos) {
            Some(v) => v,
            None => break,
        };
        pos = next;
        let end = pos + len as usize;
        if end > buf.len() {
            break;
        }
        let body = &buf[pos..end];
        match FrameType::from_u8(ty) {
            Some(FrameType::Ack) => {
                if let Some((v, _)) = get_varint(body, 0) {
                    p.ack = Some(AckFrame {
                        ack_through: v as u32,
                    });
                }
            }
            Some(FrameType::Nak) => {
                if let Some((block, q)) = get_varint(body, 0)
                    && let Some((mask, _)) = get_varint(body, q)
                {
                    p.nak = Some(NakFrame {
                        block: block as u32,
                        mask: mask as u32,
                    });
                }
            }
            Some(FrameType::Loss) if body.len() >= 3 => {
                p.loss = Some(LossFrame {
                    loss_x255: body[0],
                    burstiness_x255: body[1],
                    owd_trend_class: body[2],
                    // Tolerate a 3-byte Loss frame (loss_class absent -> 0), so
                    // the codec stays forward-compatible like the frame skip.
                    loss_class: body.get(3).copied().unwrap_or(0),
                });
            }
            Some(FrameType::Timing) => {
                if let Some((send_ts, q)) = get_varint(body, 0)
                    && let Some((echo_ts, _)) = get_varint(body, q)
                {
                    p.timing = Some(TimingFrame { send_ts, echo_ts });
                }
            }
            Some(FrameType::Ring) if body.len() >= 6 => {
                p.ring = Some(RingFrame {
                    fill_pct: body[0],
                    ring_kind: body[1],
                    producers: body[2],
                    consumers: body[3],
                    trend: body[4],
                    flags: body[5],
                });
            }
            Some(FrameType::Path) if body.len() >= 3 => {
                // The two AccECN counters are optional varints after the fixed
                // three bytes (a peer that does not send them reads as 0).
                let (ce_count, n1) = get_varint(body, 3).unwrap_or((0, 3));
                let (ect_count, _) = get_varint(body, n1).unwrap_or((0, n1));
                p.path = Some(PathFrame {
                    ttl: body[0],
                    ecn: body[1],
                    hop_count: body[2],
                    ce_count,
                    ect_count,
                });
            }
            Some(FrameType::Link) if body.len() >= 2 => {
                p.link = Some(LinkFrame {
                    class: body[0],
                    quality: body[1],
                });
            }
            Some(FrameType::LossAcct) => {
                if let Some((seq, n)) = get_varint(body, 0)
                    && let Some((lrs, _)) = get_varint(body, n)
                {
                    p.loss_acct = Some(LossAcctFrame {
                        seq: seq as u32,
                        last_recv_seq: lrs as u32,
                    });
                }
            }
            Some(FrameType::Pmtu) => {
                if let Some((v, _)) = get_varint(body, 0) {
                    p.pmtu = Some(PmtuFrame { pmtu: v as u16 });
                }
            }
            Some(FrameType::BwProbe) if body.len() >= 2 => {
                if let Some((send_ts, _)) = get_varint(body, 2) {
                    p.bw_probe.push(BwProbeFrame {
                        probe_id: body[0],
                        idx: body[1],
                        send_ts,
                    });
                }
            }
            Some(FrameType::Trace) if body.len() >= 2 => {
                p.trace.push(TraceFrame {
                    hop_ttl: body[0],
                    probe_id: body[1],
                });
            }
            Some(FrameType::AvailBw) => {
                if let Some((avail, n)) = get_varint(body, 0)
                    && let Some((cap, _)) = get_varint(body, n)
                {
                    p.avail_bw = Some(AvailBwFrame {
                        avail_kbps: avail,
                        capacity_kbps: cap,
                    });
                }
            }
            Some(FrameType::Forecast) => {
                if let Some((fc, _)) = get_varint(body, 0) {
                    p.forecast = Some(ForecastFrame { forecast_kbps: fc });
                }
            }
            Some(FrameType::Periodicity) => {
                if let Some((period, n1)) = get_varint(body, 0)
                    && let Some((to_spike, n2)) = get_varint(body, n1)
                    && n2 < body.len()
                {
                    p.periodicity = Some(PeriodicityFrame {
                        period_ds: period,
                        secs_to_spike_ds: to_spike,
                        confidence_x255: body[n2],
                    });
                }
            }
            // Known-but-malformed (too short) or unknown frame: length-skip.
            _ => {}
        }
        pos = end;
    }
    Some(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_each_length_class() {
        for v in [0u64, 1, 63, 64, 16383, 16384, (1 << 30) - 1, 1 << 30, (1u64 << 62) - 1] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            let (got, end) = get_varint(&b, 0).expect("decode");
            assert_eq!(got, v, "value {v} round-trip");
            assert_eq!(end, b.len(), "consumed all bytes for {v}");
        }
    }

    #[test]
    fn varint_uses_minimal_encoding() {
        let mut b = Vec::new();
        put_varint(&mut b, 63);
        assert_eq!(b.len(), 1, "6-bit value is one byte");
        b.clear();
        put_varint(&mut b, 64);
        assert_eq!(b.len(), 2, "14-bit value is two bytes");
    }

    #[test]
    fn full_packet_round_trips_every_frame() {
        let p = ControlPacket {
            ack: Some(AckFrame { ack_through: 70_000 }),
            nak: Some(NakFrame {
                block: 12,
                mask: 0b1011,
            }),
            loss: Some(LossFrame {
                loss_x255: 40,
                burstiness_x255: 200,
                owd_trend_class: 2,
                loss_class: 2,
            }),
            timing: Some(TimingFrame {
                send_ts: 1_234_567,
                echo_ts: 1_234_000,
            }),
            ring: Some(RingFrame {
                fill_pct: 30,
                ring_kind: 1,
                producers: 2,
                consumers: 3,
                trend: 1,
                flags: 1,
            }),
            path: Some(PathFrame {
                ttl: 53,
                ecn: 0b11,
                hop_count: 11,
                ce_count: 4242,
                ect_count: 99999,
            }),
            link: Some(LinkFrame {
                class: link_class::WIFI,
                quality: 180,
            }),
            loss_acct: Some(LossAcctFrame {
                seq: 6000,
                last_recv_seq: 5000,
            }),
            pmtu: Some(PmtuFrame { pmtu: 1280 }),
            bw_probe: vec![
                BwProbeFrame {
                    probe_id: 7,
                    idx: 0,
                    send_ts: 999,
                },
                BwProbeFrame {
                    probe_id: 7,
                    idx: 1,
                    send_ts: 1099,
                },
            ],
            trace: vec![TraceFrame {
                hop_ttl: 5,
                probe_id: 7,
            }],
            avail_bw: Some(AvailBwFrame {
                avail_kbps: 45_000,
                capacity_kbps: 100_000,
            }),
            forecast: Some(ForecastFrame {
                forecast_kbps: 38_500,
            }),
            periodicity: Some(PeriodicityFrame {
                period_ds: 150,
                secs_to_spike_ds: 42,
                confidence_x255: 200,
            }),
        };
        let wire = encode_control(&p);
        assert_eq!(wire[0], PKT_CONTROL);
        let got = decode_control(&wire).expect("decode");
        assert_eq!(got, p, "full packet round-trips");
    }

    #[test]
    fn padding_reaches_exact_size_and_still_decodes() {
        let mut p = ControlPacket::new();
        p.bw_probe.push(BwProbeFrame {
            probe_id: 3,
            idx: 1,
            send_ts: 42,
        });
        let mut wire = encode_control(&p);
        pad_control_to(&mut wire, 1400);
        assert_eq!(wire.len(), 1400, "padded to the exact target size");
        let got = decode_control(&wire).expect("decode");
        assert_eq!(got.bw_probe, p.bw_probe, "the probe survives the padding");
        assert!(got.avail_bw.is_none(), "the pad frame is skipped, not misread");
    }

    #[test]
    fn empty_packet_is_just_the_tag() {
        let p = ControlPacket::new();
        assert!(p.is_empty());
        let wire = encode_control(&p);
        assert_eq!(wire, vec![PKT_CONTROL]);
        assert_eq!(decode_control(&wire).unwrap(), p);
    }

    #[test]
    fn unknown_frame_is_skipped_not_fatal() {
        // Hand-build: a real ACK, then an unknown frame type 0x7F with a
        // 4-byte body, then a real LINK. The unknown one must be skipped and
        // both known frames decoded.
        let mut wire = vec![PKT_CONTROL];
        wire.push(FrameType::Ack as u8);
        put_varint(&mut wire, 1);
        wire.push(9); // ack_through = 9 (fits 6-bit varint)
        wire.push(0x7F); // unknown frame type
        put_varint(&mut wire, 4);
        wire.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        wire.push(FrameType::Link as u8);
        put_varint(&mut wire, 2);
        wire.extend_from_slice(&[link_class::CELLULAR, 99]);

        let p = decode_control(&wire).expect("decode");
        assert_eq!(p.ack, Some(AckFrame { ack_through: 9 }));
        assert_eq!(
            p.link,
            Some(LinkFrame {
                class: link_class::CELLULAR,
                quality: 99
            })
        );
    }

    #[test]
    fn truncated_frame_length_aborts_cleanly() {
        // A frame that claims 10 bytes but the buffer ends early: the parse
        // keeps what came before and does not panic.
        let mut wire = vec![PKT_CONTROL];
        wire.push(FrameType::Ack as u8);
        put_varint(&mut wire, 1);
        wire.push(5);
        wire.push(FrameType::Pmtu as u8);
        put_varint(&mut wire, 10); // lies: only a couple bytes follow
        wire.extend_from_slice(&[0x01, 0x02]);
        let p = decode_control(&wire).expect("decode");
        assert_eq!(p.ack, Some(AckFrame { ack_through: 5 }));
        assert_eq!(p.pmtu, None, "truncated frame dropped");
    }

    #[test]
    fn non_control_datagram_returns_none() {
        assert!(decode_control(&[1, 2, 3]).is_none());
        assert!(decode_control(&[]).is_none());
        assert!(!is_control(&[2]));
        assert!(is_control(&[PKT_CONTROL]));
    }
}
