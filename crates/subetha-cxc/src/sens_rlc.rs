//! Sens-O-Matic transport carrying the sliding-window RLC erasure code.
//! Sens-O-Matic is the reliable FEC-UDP protocol; the erasure code is its
//! swappable detail (like a cipher suite). This module is the variant that
//! carries the sliding-window Random Linear Code ([`crate::rlc_fec`]): it ships
//! items as source symbols with interleaved RLC repair symbols, recovering an
//! isolated loss without a retransmit round trip. A NAK-driven ARQ floor
//! guarantees eventual delivery for losses the coding window cannot cover. The
//! public types are `SensOMaticRlcSender` / `SensOMaticRlcReceiver`.
//!
//! This is the adaptive, low-latency-primary code; the protocol's other code is
//! block Cauchy Reed-Solomon ([`crate::udp_bridge`], the `SensOMaticRs*` types).
//! Both deliver every item in order; the difference is the erasure code: block
//! RS waits for the rest of a block to recover (often firing a wasted retransmit
//! first), while the sliding-window RLC recovers from the next repair. The two
//! share the GF(2^8) field and the committed SIMD multiply ladder.
//!
//! Wire formats (first byte is the packet type):
//!
//! ```text
//! DATA     [10] [source_id u32-le] [send_us u32-le] [symbol bytes]
//! REPAIR   [11] [repair_key u32-le] [first_source_id u32-le]
//!               [window_size u16-le] [dt u8] [repair payload]
//! NAK      [12] [missing source_id u32-le]*        (receiver -> sender)
//! ACK      [13] [delivered_through u32-le]          (receiver -> sender)
//! FEEDBACK [14] [loss_q8 u8] [burst_q8 u8] [cong_q8 u8]  (receiver -> sender)
//! ```
//!
//! A symbol is a fixed `symbol_len` buffer holding a `u16` length prefix, the
//! item bytes, then zero padding, so the receiver strips padding exactly.
//!
//! The `send_us` field is the sender's microseconds-since-start stamp, which the
//! receiver differences against its own arrival clock to recover a relative
//! one-way trip time for the [`crate::loss_class_sensor`] Spike arm (a constant
//! clock offset cancels in the min/max range). It rides the DATA header, not the
//! coded symbol, so repair linear combinations are unaffected.
//!
//! The FEEDBACK frame is the adaptive-control feedback path: the receiver fits
//! the loss rate, the Gilbert-Elliott mean burst length, and the congestion
//! share of recent loss, quantizes each into a byte, and ships them back; the
//! sender turns them into a [`crate::fusion::SensorSnapshot`] and runs the
//! [`crate::rlc_control::RlcController`] to retune the window, the repair
//! cadence (code rate), and the coefficient density - the sensing-driven half of
//! the adaptive RLC primary.

use crate::burst_model_sensor::BurstModel;
use crate::fusion::SensorSnapshot;
use crate::loss_class_sensor::LossClassSensor;
use crate::rlc_control::RlcController;
use crate::rlc_fec::{RepairSymbol, RlcDecoder, RlcEncoder};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

const PKT_RLC_DATA: u8 = 10;
const PKT_RLC_REPAIR: u8 = 11;
const PKT_RLC_NAK: u8 = 12;
const PKT_RLC_ACK: u8 = 13;
const PKT_RLC_FEEDBACK: u8 = 14;
/// Path-validation pair (Slice 4): the receiver sends a `PATH_CHALLENGE`
/// (`[type][8 conn-id][8 nonce]`) to a candidate new peer address; the sender
/// echoes the nonce in a `PATH_RESPONSE` of the same shape, proving it can
/// receive at the new address. Cleartext-framed (it carries no payload to
/// protect) but the nonce is unpredictable, so an off-path attacker cannot
/// forge a response for a challenge it never saw.
const PKT_RLC_PATH_CHALLENGE: u8 = 18;
const PKT_RLC_PATH_RESPONSE: u8 = 19;

/// PATH_CHALLENGE / PATH_RESPONSE body: type byte + connection id (u64) + an
/// 8-byte nonce.
const PATH_FRAME_LEN: usize = 1 + 8 + 8;

/// Anti-amplification factor (RFC 9000 §8): until a new peer address validates,
/// the receiver sends at most this multiple of the bytes it received from that
/// address, so a spoofed source address cannot turn the receiver into a
/// reflector toward a victim.
const AMPLIFICATION_FACTOR: u64 = 3;

/// How long the receiver waits for a `PATH_RESPONSE` before declaring the new
/// address unreachable and reverting to the previous one (a spoofed move never
/// answers; a genuine migration answers within a round trip).
const CHALLENGE_TIMEOUT: Duration = Duration::from_millis(500);
/// TLS handshake flight (cleartext, before keys exist) + its ack, and the AEAD
/// envelope `[17][pn u64-le][sealed inner datagram + tag]` for the data phase.
#[cfg(feature = "tls")]
const PKT_RLC_CRYPTO: u8 = 15;
#[cfg(feature = "tls")]
const PKT_RLC_CRYPTO_ACK: u8 = 16;
#[cfg(feature = "tls")]
const PKT_RLC_SECURE: u8 = 17;

/// DATA header: type byte + connection id (u64) + source id (u32) + send-
/// timestamp (u32). The connection id decouples the session from the 4-tuple, so
/// a client that rebinds (NAT / interface change) keeps its session: the
/// receiver routes by the id, not the address.
const DATA_HDR: usize = 1 + 8 + 4 + 4;

/// Packet-pair (dispersion) capacity-probe tuning.
/// `PAIR_RING_CAP` - how many recent above-floor consecutive-id gaps to keep.
/// `PAIR_GAP_FLOOR_US` - reject gaps tighter than this as NAPI/GRO batch noise:
///   a 12us gap for a ~1.5kB packet implies ~985 Mbit, physically impossible on
///   the target paths, so anything tighter is a same-softirq-poll artifact.
/// `PAIR_PERCENTILE_NUM/DEN` - the low percentile of the floored gaps to read
///   the bottleneck dispersion from (25th: just above the batch noise, at the
///   tight-pair cluster, robust to a few sub-dispersion jitter readings).
const PAIR_RING_CAP: usize = 512;
const PAIR_GAP_FLOOR_US: f64 = 12.0;
const PAIR_PERCENTILE_NUM: usize = 25;
const PAIR_PERCENTILE_DEN: usize = 100;
/// Safe band for the NAK-adapted cruise fraction. The cliff sits at ~0.79x the
/// raw capacity, so `PAIR_FRACTION_HI` stays just under it (push there only when
/// the FEC reports the path is clean); `PAIR_FRACTION_LO` is the widest margin
/// the controller backs off to under heavy loss. Anchored to the loss-
/// independent capacity, a fraction in this band is always a sub-cliff pace.
const PAIR_FRACTION_LO: f64 = 0.62;
const PAIR_FRACTION_HI: f64 = 0.78;

/// Sliding-window length for the loss rate the controller provisions FEC against
/// (recent source-id outcomes). ~0.4s of symbols on a 30 Mbit link - reactive to
/// a real change in loss, robust to a single recovered-symbol delivery burst.
const LOSS_WINDOW: usize = 1024;
/// Denominator floor for the windowed loss rate: until the window holds this many
/// samples, a few startup losses divide by this (not by the tiny actual count),
/// so a cold-start cluster cannot read as catastrophic loss and slam FEC to max.
const LOSS_WINDOW_MIN_FILL: usize = 256;

/// Derive a per-connection id from the wall clock and the local port - unique
/// enough to tell one session from another on a receiver. (A production server
/// facing untrusted peers would draw it from a CSPRNG; a single session does not
/// need that.)
fn derive_conn_id(local_port: u16) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = nanos ^ ((local_port as u64) << 48);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Socket buffer sizing so a send burst does not overflow the kernel UDP queue
/// and manufacture loss on a fast link.
const SOCK_BUF: usize = 4 * 1024 * 1024;

/// Best-effort enlarge a socket's send / receive buffers.
fn set_buffers(sock: &UdpSocket) {
    let s = socket2::SockRef::from(sock);
    s.set_recv_buffer_size(SOCK_BUF).ok();
    s.set_send_buffer_size(SOCK_BUF).ok();
}

/// Send one datagram, treating `WouldBlock`/EAGAIN - a full send buffer or
/// qdisc, i.e. transient back-pressure - as "retry shortly" rather than a fatal
/// error, the way a production UDP stack (quinn) does. When the kernel send
/// buffer is smaller than the in-flight window (a default-configured host caps
/// `SO_SNDBUF` at `wmem_max`, often ~208 KB), a burst overruns the buffer and
/// EAGAIN is the correct back-pressure signal, not a failure. Bounded so a
/// genuinely wedged path still surfaces an error.
fn send_with_retry(
    sock: &crate::dgram::DgramSock,
    wire: &[u8],
    peer: SocketAddr,
) -> io::Result<()> {
    let start = Instant::now();
    let mut backoff = Duration::from_micros(20);
    loop {
        match sock.send_to(wire, peer) {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if start.elapsed() > Duration::from_secs(30) {
                    return Err(e);
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(1));
            }
            Err(e) => return Err(e),
        }
    }
}

/// GSO-batch variant of [`send_with_retry`]: ships `batch` as one `sendmsg`,
/// retrying on transient `WouldBlock`/EAGAIN back-pressure.
fn send_gso_with_retry(
    sock: &crate::dgram::DgramSock,
    batch: &[u8],
    seg: u16,
    peer: SocketAddr,
) -> io::Result<()> {
    let start = Instant::now();
    let mut backoff = Duration::from_micros(20);
    loop {
        match sock.send_gso(batch, seg, peer) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if start.elapsed() > Duration::from_secs(30) {
                    return Err(e);
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(1));
            }
            Err(e) => return Err(e),
        }
    }
}

// Kernel RX timestamps, source-address decoding, and the `recvmsg`-based
// receive that carries the `SO_TIMESTAMPNS` arrival time now live in the
// `dgram` backend (so they apply to both the plain-UDP and io_uring paths)
// and are reached through `DgramSock::recv_with_kts`.

/// AEAD-seal `inner` into a `PKT_RLC_SECURE` wire datagram - a type byte, the
/// 64-bit packet number, then the ciphertext-with-tag. The whole inner datagram
/// (type byte and all) is encrypted, so the FEC stays over cleartext and the
/// wire reveals only the packet number.
#[cfg(feature = "tls")]
fn secure_wrap(crypto: &crate::rlc_crypto::CryptoState, inner: &[u8]) -> io::Result<Vec<u8>> {
    let mut payload = inner.to_vec();
    let pn = crypto
        .seal(&mut payload)
        .map_err(io::Error::other)?;
    let mut wire = Vec::with_capacity(9 + payload.len());
    wire.push(PKT_RLC_SECURE);
    wire.extend_from_slice(&pn.to_le_bytes());
    wire.extend_from_slice(&payload);
    Ok(wire)
}

/// Open a `PKT_RLC_SECURE` datagram into its cleartext inner datagram, or `None`
/// if it is not a sealed frame or fails to authenticate.
#[cfg(feature = "tls")]
fn secure_unwrap(crypto: &crate::rlc_crypto::CryptoState, pkt: &[u8]) -> Option<Vec<u8>> {
    if pkt.len() < 9 || pkt[0] != PKT_RLC_SECURE {
        return None;
    }
    let pn = u64::from_le_bytes(pkt[1..9].try_into().ok()?);
    let mut payload = pkt[9..].to_vec();
    let n = crypto.open(pn, &mut payload).ok()?;
    payload.truncate(n);
    Some(payload)
}

/// Drive the TLS handshake to completion over `sock`, carrying each level's
/// `write_hs` flight in a reliable (retransmitted, acked, in-order) cleartext
/// `PKT_RLC_CRYPTO` exchange. The client sends first; the server (peer `None`
/// initially) learns its peer from the first flight. Returns the peer once the
/// 1-RTT keys are derived on this side.
#[cfg(feature = "tls")]
pub(crate) fn drive_handshake(
    sock: &crate::dgram::DgramSock,
    mut peer: Option<SocketAddr>,
    crypto: &mut crate::rlc_crypto::CryptoState,
    is_client: bool,
) -> io::Result<SocketAddr> {
    let mut next_send_seq = 0u32;
    let mut next_recv_seq = 0u32;
    let mut outbox: Vec<(u32, Vec<u8>)> = Vec::new();
    if is_client {
        for f in crypto.take_outgoing() {
            outbox.push((next_send_seq, f));
            next_send_seq += 1;
        }
    }
    let start = Instant::now();
    let mut last_send: Option<Instant> = None;
    let mut buf = vec![0u8; 8192];
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "tls handshake timeout"));
        }
        // (Re)transmit the unacked flights every 100ms (and immediately the
        // first time / right after producing new ones).
        if let Some(p) = peer
            && last_send.map(|t| t.elapsed() > Duration::from_millis(100)).unwrap_or(true)
        {
            for (seq, f) in &outbox {
                let mut pkt = Vec::with_capacity(5 + f.len());
                pkt.push(PKT_RLC_CRYPTO);
                pkt.extend_from_slice(&seq.to_le_bytes());
                pkt.extend_from_slice(f);
                sock.send_to(&pkt, p)?;
            }
            last_send = Some(Instant::now());
        }
        match sock.recv_from(&mut buf) {
            Ok((n, from)) if n >= 5 => {
                peer.get_or_insert(from);
                let seq = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
                match buf[0] {
                    PKT_RLC_CRYPTO if seq <= next_recv_seq => {
                        let mut ack = Vec::with_capacity(5);
                        ack.push(PKT_RLC_CRYPTO_ACK);
                        ack.extend_from_slice(&seq.to_le_bytes());
                        sock.send_to(&ack, from)?;
                        if seq == next_recv_seq {
                            crypto
                                .read_handshake(&buf[5..n])
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                            next_recv_seq += 1;
                            for f in crypto.take_outgoing() {
                                outbox.push((next_send_seq, f));
                                next_send_seq += 1;
                            }
                            last_send = None; // send the new flights at once
                        }
                    }
                    PKT_RLC_CRYPTO_ACK => {
                        outbox.retain(|(s, _)| *s != seq);
                    }
                    _ => {}
                }
            }
            Ok(_) => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => {}
            Err(e) => return Err(e),
        }
        if crypto.is_complete() && outbox.is_empty() {
            // Keep acking for a short grace so a peer retransmitting its final
            // flight (because our ack was lost) still converges - but exit the
            // instant the peer sends a non-handshake frame, since that proves it
            // got our ack and moved to data (and avoids a long window where its
            // early data frames would be dropped here).
            let grace = Instant::now();
            while grace.elapsed() < Duration::from_millis(200) {
                match sock.recv_from(&mut buf) {
                    Ok((n, from)) if n >= 5 && buf[0] == PKT_RLC_CRYPTO => {
                        let seq = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
                        let mut ack = Vec::with_capacity(5);
                        ack.push(PKT_RLC_CRYPTO_ACK);
                        ack.extend_from_slice(&seq.to_le_bytes());
                        sock.send_to(&ack, from)?;
                    }
                    Ok((n, _)) if n >= 1 && buf[0] != PKT_RLC_CRYPTO_ACK => break,
                    Ok(_) => {}
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {}
                    Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => {}
                    Err(e) => return Err(e),
                }
            }
            return peer.ok_or_else(|| io::Error::other("no peer"));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The connection id of a DATA / REPAIR inner frame (the `u64` after the type
/// byte), or `None` for any other frame. Used to route a session by id rather
/// than 4-tuple, so it survives a peer address change.
fn frame_conn_id(inner: &[u8]) -> Option<u64> {
    if inner.len() >= 9 && (inner[0] == PKT_RLC_DATA || inner[0] == PKT_RLC_REPAIR) {
        Some(u64::from_le_bytes(inner[1..9].try_into().ok()?))
    } else {
        None
    }
}

/// Bytes a symbol reserves for the `u16` item-length prefix.
const LEN_PREFIX: usize = 2;

/// Pack `item` into a fixed `symbol_len` symbol: a `u16` length prefix, the
/// item bytes, then zero padding. `item` must fit `symbol_len - 2`.
fn pack_symbol(item: &[u8], symbol_len: usize) -> Vec<u8> {
    debug_assert!(item.len() + LEN_PREFIX <= symbol_len);
    let mut sym = vec![0u8; symbol_len];
    sym[0..2].copy_from_slice(&(item.len() as u16).to_le_bytes());
    sym[2..2 + item.len()].copy_from_slice(item);
    sym
}

/// Recover the item bytes from a symbol (strip the length prefix and padding).
fn unpack_symbol(sym: &[u8]) -> Vec<u8> {
    if sym.len() < LEN_PREFIX {
        return Vec::new();
    }
    let len = u16::from_le_bytes([sym[0], sym[1]]) as usize;
    let end = (LEN_PREFIX + len).min(sym.len());
    sym[LEN_PREFIX..end].to_vec()
}

/// Sender side of the RLC transport.
pub struct SensOMaticRlcSender {
    sock: crate::dgram::DgramSock,
    peer: SocketAddr,
    enc: RlcEncoder,
    symbol_len: usize,
    /// BBR congestion control: paces the send rate to the measured bottleneck
    /// bandwidth and bounds in-flight to the BDP, keeping the bottleneck queue
    /// shallow (low latency under bufferbloat). Fed the per-symbol rate-sample
    /// snapshots in `bbr_samples` on send, and the delivered set on each ACK.
    bbr: crate::bbr::Bbr,
    bbr_samples: HashMap<u32, crate::bbr::PacketSample>,
    /// Source symbols held for ARQ retransmission, keyed by source id.
    sent: BTreeMap<u32, Vec<u8>>,
    /// Last transmit instant per source id, for RETRANSMIT SUPPRESSION. The
    /// receiver re-NAKs a still-missing id every ~1ms, but a retransmit takes a
    /// round trip to be confirmed - so an un-suppressed sender resends the same
    /// symbol ~RTT/1ms (~30x) before the ACK clears it, a self-amplifying flood
    /// that drives congestion collapse under loss. A symbol is only (re)sent if
    /// it has not been sent within ~1.2 RTT.
    last_tx: HashMap<u32, Instant>,
    /// Highest contiguous source id the receiver has delivered.
    acked_through: u32,
    /// Max source symbols in flight (sent but not yet delivered) before the
    /// sender paces, so a burst cannot overrun the receiver or the kernel
    /// socket buffer and manufacture loss. Used as the static cap, and as the
    /// bootstrap window before BBR has a bandwidth estimate when `bbr_cwnd`.
    flow_window: u32,
    /// When set, the in-flight bound is BBR's dynamic congestion window
    /// (`cwnd_gain * BtlBw * RTprop`) instead of the static `flow_window`, so
    /// the window self-sizes to the path's BDP and ProbeBW grows it to find
    /// more bandwidth (escaping the static-window fixed point). Paired with
    /// pacing (below) so the cwnd-bounded sends spread over the RTT instead of
    /// bursting and manufacturing loss.
    bbr_cwnd: bool,
    /// Static rate pacing: spread the in-flight window evenly over the RTT
    /// (one symbol every `min_rtt / window`) instead of ack-clocked bursting.
    /// This lets a larger `flow_window` fill the path BDP without bursting the
    /// shallow bottleneck buffer (the unpaced cliff) - the way a paced sender
    /// keeps instantaneous queue depth low while in-flight rises to the BDP.
    /// Distinct from `bbr_cwnd` (which paces at BBR's measured rate); when both
    /// are set, BBR wins.
    paced: bool,
    /// Fixed-rate pacing target in BYTES/sec (0 = off). When set, the sender
    /// paces the wire at exactly this rate regardless of window/RTT - the
    /// OFFENSIVE FEC-push lever: drive the wire toward the path's raw capacity
    /// (past a loss-based controller's conservative operating point), and let
    /// the FEC recover whatever the bottleneck drops near the ceiling. Takes
    /// priority over `bbr_cwnd` and `paced`; pair with a large `flow_window` so
    /// the window does not gate before the rate does.
    pace_bps: f64,
    /// Adaptive FEC-push: auto-tune `pace_bps` as a closed loop instead of a
    /// fixed target. Probes the rate UP while the path absorbs what is paced
    /// (delivered rate keeps up, the FEC recovering the induced loss), and backs
    /// off to the delivered rate the moment the path cannot keep up - so it
    /// fills the headroom a loss-based controller leaves AND survives a path
    /// drop (which sinks the static pacer). The offensive use of FEC: probe
    /// harder than BBR because the coding absorbs the probe loss.
    adaptive_push: bool,
    /// Last time the adaptive loop adjusted `pace_bps`.
    last_push_adapt: Instant,
    /// Adaptive `pace_bps` clamps (bytes/sec): floor so a transient stall cannot
    /// collapse the rate to zero, ceiling so a probe cannot run away unbounded.
    push_min_bps: f64,
    push_max_bps: f64,
    /// Latest forward-loss fraction the RECEIVER measured and fed back over the
    /// control plane (FEEDBACK frame). The adaptive push drives the rate from
    /// THIS real measurement against the FEC's recovery capacity, not from an
    /// inferred ack-frontier rate (which lags and backs off prematurely).
    fb_loss: f64,
    /// Latest delivered (goodput) rate in BYTES/sec the RECEIVER measured and
    /// fed back. The ground-truth path signal: it plateaus at the path capacity
    /// (the bufferless cliff shows as a rate plateau, not a usable loss
    /// gradient), so the adaptive push paces just above it to fill the path
    /// without the runaway overshoot the binary loss signal caused.
    fb_rate_bps: f64,
    /// Latest CONGESTION fraction the receiver's Biaz/Spike loss classifier fed
    /// back (the RFC 9265 signal): the share of loss attributable to congestion
    /// (delay-correlated) rather than random/path loss. The offensive push fills
    /// THROUGH random loss (the FEC recovers it) but yields to congestion loss
    /// (the FEC must not hide it), so this gates rate growth vs back-off.
    fb_cong: f64,
    /// Best delivered (goodput) rate seen, BYTES/sec - the find-then-cruise
    /// estimate of the path capacity. The push probes the pace up to locate the
    /// cliff (where goodput stops rising / collapses), then cruises BELOW it;
    /// a goodput collapse vs this best is the overshoot signal that triggers a
    /// hard cut. Decays slowly so a transient high does not pin the cruise rate.
    max_delivered_bps: f64,
    /// Packet-pair CAPACITY estimate (BYTES/sec) the receiver measured from the
    /// tightest consecutive-id arrival gap and fed back. Independent of loss
    /// (the bottleneck imposes the gap regardless of drops), so the adaptive
    /// push CRUISES just under it - no rate probing into the sharp cliff, which
    /// is what every loss-confounded signal collapsed on. 0 until measured.
    fb_capacity_bps: f64,
    /// Source-symbol counter for the packet-pair PROBE: every `PAIR_PROBE_EVERY`
    /// symbols the sender ships the next one back-to-back (skips its pacing gap)
    /// so the receiver sees a tight pair and can read the bottleneck dispersion.
    pair_probe_ctr: u32,
    /// Cruise target as a fraction of the measured RAW capacity. The bufferless
    /// cliff sits at ~0.79x the raw rate, so this stays below it; default 0.70
    /// (proven-safe pace under the cliff), overridable via `SUBETHA_PAIR_FRACTION`
    /// for path tuning. Clamped to [0.30, 0.78] so it can never target the cliff.
    push_fraction: f64,
    /// Post-cut cooldown (adaptive steps): after a goodput collapse the push
    /// CRUISES (holds the cut rate, no probe) for this many steps before gently
    /// probing again, so it does not sawtooth straight back into the sharp cliff
    /// - on a knife-edge path, re-probing every RTT just re-collapses.
    push_cooldown: u32,
    /// NAK'd source ids received since the last adaptive step - a NAK is a
    /// direct FEC-MISS signal (the coding could not recover a loss), so this
    /// drives the coordinated loss-aware control: NAKs => parity too thin =>
    /// raise parity AND back off the pace together.
    naks_recv_window: u32,
    /// Source symbols sent since the last adaptive step (the NAK-rate
    /// denominator).
    sent_window: u32,
    /// Earliest instant the next symbol may be sent, when pacing at BBR's
    /// target rate (`pacing_gain * BtlBw`). `None` disables pacing (static mode).
    next_send: Option<Instant>,
    /// UDP GSO (`UDP_SEGMENT`) batching of the steady-state DATA path: when on,
    /// consecutive equal-size sealed DATA datagrams accumulate in `gso_buf` and
    /// ship in one `sendmsg` (the kernel slices them), collapsing the per-symbol
    /// syscall + stack-traversal cost. The batch flushes on a repair boundary
    /// (repairs are a different size), when it reaches `gso_max` segments, before
    /// a flow-window wait, and at drain - so FEC timing is unchanged (a repair
    /// still ships immediately after its source window).
    gso: bool,
    /// Accumulated sealed DATA datagrams (each exactly `gso_seg` bytes).
    gso_buf: Vec<u8>,
    /// Segments currently buffered in `gso_buf`.
    gso_n: usize,
    /// The uniform sealed-DATA datagram size, learned on the first enqueue.
    gso_seg: usize,
    /// Max segments per GSO batch: `min(64, 65535 / gso_seg)` so the super-buffer
    /// fits one IP datagram and stays within UDP's 64-segment ceiling.
    gso_max: usize,
    /// Monotonic start, for the per-DATA send-timestamp the receiver turns into
    /// a relative one-way trip time.
    start: Instant,
    /// The sensing-driven controller: each FEEDBACK frame retunes the encoder's
    /// window / cadence / density through it.
    controller: RlcController,
    /// When set, the controller is held at its initial parameters (the static
    /// baseline for an adaptive-vs-static A/B); FEEDBACK is still pumped for
    /// telemetry but never changes the coding.
    static_params: bool,
    /// Diagnostic: when `SUBETHA_FEC_DEBUG` is set, log each coding-parameter
    /// decision the sensing controller makes (loss/burst/cong in -> window/step/
    /// density/coding_on out) so the under-loss behaviour can be read directly.
    fec_debug: bool,
    /// Times the live coding parameters actually changed (telemetry).
    adapt_count: u64,
    /// FEEDBACK frames received (telemetry).
    feedback_recv: u64,
    /// One in-flight RTprop probe: the source id and the instant it was sent.
    /// When an ACK advances past the probe id, `now - probe_time` is an RTT
    /// sample (delivery round trip), min-filtered into `min_rtt_us`.
    probe: Option<(u32, Instant)>,
    /// Minimum observed delivery round trip (microseconds), the RTprop estimate
    /// the controller uses to weight FEC against ARQ: when the round trip is
    /// expensive a NAK costs more, so heavier FEC (a smaller step) is worth it.
    min_rtt_us: u64,
    /// Last time the cumulative ACK advanced, and the last RTO-driven
    /// retransmit, for sender-side recovery of a stall the receiver cannot NAK
    /// (a frontier hole it never learned exists - the same root cause as the
    /// end-of-stream tail, but mid-stream when the sender is flow-blocked).
    last_ack_advance: Instant,
    last_rto_rtx: Instant,
    /// Connection id stamped into every DATA / REPAIR, so the session survives a
    /// local-address change ([`migrate`](Self::migrate)).
    conn_id: u64,
    /// Slice 4 proactive migration. The OS path-event observer (item 12) fires
    /// the instant the kernel re-routes / an interface roams / the path MTU
    /// drops - ahead of any loss. When its event count advances past
    /// `last_event_count`, the sender migrates PROACTIVELY (rebinds + lets the
    /// receiver pre-validate the new path) so the switch is covered before the
    /// old path fails - the migration QUIC's reactive design cannot do.
    net_obs: Option<crate::net_events::NetEventObserver>,
    last_event_count: u64,
    /// Migrations triggered by a path event rather than an explicit call.
    proactive_migrations: u64,
    /// Optional TLS state: when present, the handshake has run and every data
    /// datagram is AEAD-sealed / opened with the 1-RTT keys.
    #[cfg(feature = "tls")]
    crypto: Option<crate::rlc_crypto::CryptoState>,
}

impl SensOMaticRlcSender {
    /// Bind a local socket, connect to `peer`, and code over `symbol_len`-byte
    /// symbols with a window of `window` source symbols, one repair every
    /// `step` symbols, at density threshold `dt`.
    pub fn bind<A: ToSocketAddrs>(
        local: A,
        peer: SocketAddr,
        window: usize,
        step: usize,
        dt: u8,
        symbol_len: usize,
    ) -> io::Result<Self> {
        let sock = UdpSocket::bind(local)?;
        sock.set_nonblocking(true)?;
        set_buffers(&sock);
        let conn_id = derive_conn_id(sock.local_addr().map(|a| a.port()).unwrap_or(0));
        // Auto-detect the datagram backend (io_uring where available, plain
        // UDP otherwise); the transport's hot loop is unchanged.
        let sock = crate::dgram::DgramSock::wrap(sock);
        Ok(Self {
            sock,
            peer,
            enc: RlcEncoder::new(window, step, dt, symbol_len),
            symbol_len,
            bbr: crate::bbr::Bbr::new(Instant::now(), (symbol_len + DATA_HDR) as u64),
            bbr_samples: HashMap::new(),
            sent: BTreeMap::new(),
            last_tx: HashMap::new(),
            acked_through: 0,
            // The flow window caps OUTSTANDING (sent-but-not-yet-received)
            // symbols, which is what paces the sender so a burst cannot overrun
            // the kernel receive buffer and manufacture loss. Crucially this
            // counts genuinely-unconfirmed symbols (holes + on-wire), NOT
            // symbols ahead of the in-order delivery frontier - so a single hole
            // costs one slot, not the whole window, and the sender keeps the
            // pipe full while the receiver buffers out-of-order and recovers.
            flow_window: 128,
            bbr_cwnd: false,
            paced: false,
            pace_bps: 0.0,
            adaptive_push: false,
            last_push_adapt: Instant::now(),
            push_min_bps: 0.0,
            push_max_bps: f64::INFINITY,
            fb_loss: 0.0,
            fb_rate_bps: 0.0,
            fb_cong: 0.0,
            max_delivered_bps: 0.0,
            fb_capacity_bps: 0.0,
            pair_probe_ctr: 0,
            push_fraction: std::env::var("SUBETHA_PAIR_FRACTION")
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.70)
                .clamp(0.30, 0.78),
            push_cooldown: 0,
            naks_recv_window: 0,
            sent_window: 0,
            next_send: None,
            gso: false,
            gso_buf: Vec::new(),
            gso_n: 0,
            gso_seg: 0,
            gso_max: 1,
            start: Instant::now(),
            // The controller starts from the same initial parameters as the
            // encoder, so an adaptive run and a static run begin identically and
            // diverge only as feedback arrives. `hold = 8` ticks of lower demand
            // before relaxing a knob keeps the parameters from flapping.
            controller: RlcController::new(window as u16, step as u16, dt.min(15), 8),
            static_params: false,
            fec_debug: std::env::var("SUBETHA_FEC_DEBUG").is_ok(),
            adapt_count: 0,
            feedback_recv: 0,
            probe: None,
            min_rtt_us: u64::MAX,
            last_ack_advance: Instant::now(),
            last_rto_rtx: Instant::now(),
            conn_id,
            net_obs: None,
            last_event_count: 0,
            proactive_migrations: 0,
            #[cfg(feature = "tls")]
            crypto: None,
        })
    }

    /// Swap the datagram socket for one the caller already built (e.g. a demux
    /// socket the unified endpoint shares across both codes). The replacement
    /// should be connected to `peer` and non-blocking; the session conn id from
    /// the original bind is kept (it only needs to be consistent per session).
    pub fn set_sock(&mut self, sock: crate::dgram::DgramSock) {
        self.sock = sock;
    }

    /// Arm the optional TLS record layer as the client. Call
    /// [`handshake`](Self::handshake) before sending; every data datagram is
    /// then AEAD-protected.
    #[cfg(feature = "tls")]
    pub fn with_tls_client(mut self, cfg: std::sync::Arc<rustls::ClientConfig>) -> io::Result<Self> {
        self.crypto = Some(
            crate::rlc_crypto::CryptoState::new_client(cfg)
                .map_err(io::Error::other)?,
        );
        Ok(self)
    }

    /// Run the TLS handshake (no-op when TLS is not armed). Blocks until the
    /// 1-RTT keys are derived.
    #[cfg(feature = "tls")]
    pub fn handshake(&mut self) -> io::Result<()> {
        if let Some(crypto) = self.crypto.as_mut() {
            drive_handshake(&self.sock, Some(self.peer), crypto, true)?;
        }
        Ok(())
    }

    /// Send one inner datagram to the peer, AEAD-sealing it first when TLS is on.
    fn wire_send(&mut self, inner: &[u8]) -> io::Result<()> {
        #[cfg(feature = "tls")]
        if let Some(c) = &self.crypto {
            let wire = secure_wrap(c, inner)?;
            return send_with_retry(&self.sock, &wire, self.peer);
        }
        send_with_retry(&self.sock, inner, self.peer)
    }

    /// Compute the on-wire bytes for `inner` (AEAD-sealed when TLS is armed) and
    /// either append them to the GSO batch (when GSO is on and the sealed size
    /// matches the batch's) or send immediately. Each sealed datagram carries
    /// its own packet number + tag, so a GSO batch of them is independently
    /// openable by the receiver - the per-packet-AEAD framing is what makes the
    /// batch legal. The batch flushes when it reaches `gso_max` segments.
    fn enqueue_wire(&mut self, inner: &[u8]) -> io::Result<()> {
        #[cfg(feature = "tls")]
        let wire: Vec<u8> = match &self.crypto {
            Some(c) => secure_wrap(c, inner)?,
            None => inner.to_vec(),
        };
        #[cfg(not(feature = "tls"))]
        let wire: Vec<u8> = inner.to_vec();
        if self.gso {
            if self.gso_seg == 0 {
                self.gso_seg = wire.len();
                self.gso_max = (65535 / self.gso_seg.max(1)).clamp(1, 64);
            }
            if wire.len() == self.gso_seg {
                self.gso_buf.extend_from_slice(&wire);
                self.gso_n += 1;
                if self.gso_n >= self.gso_max {
                    self.flush_gso()?;
                }
                return Ok(());
            }
            // A differently-sized datagram cannot share the batch: flush the
            // accumulated segments, then send this one on its own.
            self.flush_gso()?;
        }
        send_with_retry(&self.sock, &wire, self.peer)
    }

    /// Ship the accumulated GSO batch in one `sendmsg` (the kernel slices it into
    /// `gso_n` datagrams of `gso_seg` bytes) and reset the batch. A no-op when
    /// GSO is off or the batch is empty.
    fn flush_gso(&mut self) -> io::Result<()> {
        if self.gso_n == 0 {
            return Ok(());
        }
        match send_gso_with_retry(&self.sock, &self.gso_buf, self.gso_seg as u16, self.peer) {
            Ok(()) => {
                self.gso_buf.clear();
                self.gso_n = 0;
                Ok(())
            }
            Err(_) => {
                // The egress cannot segment (a virtio NIC with
                // `tx-udp-segmentation` fixed-off and GSO disabled returns EIO):
                // send this batch one datagram at a time and disable GSO for the
                // rest of the session, so the lever degrades to the plain path
                // instead of failing. Take the buffer to avoid borrowing self
                // both mutably (the sends) and immutably (the slice).
                let buf = std::mem::take(&mut self.gso_buf);
                let seg = self.gso_seg.max(1);
                let mut off = 0;
                while off < buf.len() {
                    let end = (off + seg).min(buf.len());
                    send_with_retry(&self.sock, &buf[off..end], self.peer)?;
                    off = end;
                }
                self.gso = false;
                self.gso_n = 0;
                Ok(())
            }
        }
    }

    /// Retransmission timeout: a few RTTs, floored so a near-zero RTT estimate
    /// (loopback) does not retransmit faster than the receiver can respond.
    fn rto(&self) -> Duration {
        let rtt_us = self.min_rtt_us.clamp(1_000, 100_000);
        Duration::from_micros((3 * rtt_us).clamp(30_000, 300_000))
    }

    /// Set the in-flight flow-control window (source symbols); the default 1024.
    pub fn with_flow_window(mut self, window: u32) -> Self {
        self.flow_window = window.max(1);
        self
    }

    /// Drive the in-flight bound from BBR's dynamic congestion window
    /// (`cwnd_gain * BtlBw * RTprop`) instead of the static `flow_window`. The
    /// window self-sizes to the path BDP and ProbeBW grows it to discover more
    /// bandwidth, so the sender fills a high-BDP WAN path instead of pinning at
    /// the static window. `flow_window` is the bootstrap window until BBR's
    /// first bandwidth sample lands.
    pub fn with_bbr_cwnd(mut self, on: bool) -> Self {
        self.bbr_cwnd = on;
        self
    }

    /// Enable UDP GSO (`UDP_SEGMENT`) batching of the steady-state DATA path.
    /// Equal-size sealed DATA datagrams accumulate and ship in one `sendmsg`,
    /// cutting the per-symbol syscall cost. Repairs still ship immediately after
    /// their source window (the batch flushes first), so FEC timing is unchanged.
    pub fn with_gso(mut self, on: bool) -> Self {
        self.gso = on;
        self
    }

    /// Enable static rate pacing: spread the in-flight window evenly over the
    /// RTT instead of ack-clocked bursting, so a larger `flow_window` can fill
    /// the path BDP without bursting the shallow bottleneck buffer (the unpaced
    /// cliff). Pair with a `flow_window` sized to the BDP.
    pub fn with_paced(mut self, on: bool) -> Self {
        self.paced = on;
        self
    }

    /// Pace the wire at a fixed `mbit_per_s` (the offensive FEC-push lever):
    /// drive throughput toward the path's raw capacity, past where a loss-based
    /// controller backs off, and let the FEC recover the loss from operating
    /// near the ceiling. Pair with a large `flow_window` and FEC parity (a low
    /// `step`) so the window does not gate and the induced loss is recoverable.
    pub fn with_pace_mbit(mut self, mbit_per_s: f64) -> Self {
        self.pace_bps = (mbit_per_s * 1.0e6 / 8.0).max(0.0);
        self
    }

    /// Enable the adaptive FEC-push closed loop, starting at `start_mbit` and
    /// auto-tuning between `min_mbit` and `max_mbit`. The loop probes the rate up
    /// while the delivered rate keeps pace (the FEC absorbing the induced loss)
    /// and backs off to the delivered rate when the path cannot keep up - filling
    /// the headroom a loss-based controller leaves while surviving a path drop.
    pub fn with_adaptive_push(mut self, start_mbit: f64, min_mbit: f64, max_mbit: f64) -> Self {
        self.adaptive_push = true;
        self.pace_bps = (start_mbit * 1.0e6 / 8.0).max(1.0);
        self.push_min_bps = (min_mbit * 1.0e6 / 8.0).max(1.0);
        self.push_max_bps = (max_mbit * 1.0e6 / 8.0).max(self.push_min_bps);
        self
    }

    /// One step of the adaptive FEC-push loop, driven by the control plane's
    /// ground-truth signal: the receiver's REAL delivered (goodput) rate. On a
    /// bufferless path the loss signal is binary (zero below the cliff, a
    /// catastrophic burst at it) and useless for probing, but the delivered rate
    /// PLATEAUS at the path capacity, so it is safe to ride. Pace just above the
    /// delivered rate to fill the path (the FEC recovers the small probe loss);
    /// because the delivered rate cannot exceed the path, the loop self-limits
    /// instead of running away. The receiver's measured loss is the safety
    /// brake: when it nears the FEC's recovery capacity (~1/step), drop the probe
    /// so the push never outruns what the coding can save. Adapts ~once per RTT.
    fn adapt_push_rate(&mut self) {
        if !self.adaptive_push {
            return;
        }
        let now = Instant::now();
        let dt = now.duration_since(self.last_push_adapt).as_secs_f64();
        let rtt_s = (self.min_rtt_us.min(200_000)) as f64 / 1.0e6;
        if dt < rtt_s.max(0.005) {
            return;
        }
        self.last_push_adapt = now;
        // The NAK rate (FEC misses since the last step) is the cliff-proximity
        // signal for the RATE: a burst of misses past the FEC's recovery capacity
        // means we overshot the cliff, so back the cruise fraction off. The CODING
        // itself (window / step / density / disable-on-clean) is NOT sized here -
        // the sensing controller in apply_feedback owns it, provisioning the FEC
        // PROACTIVELY from the receiver's directly-measured loss and burstiness
        // rather than reactively from these post-miss NAKs. Rate and coding are
        // orthogonal knobs; this loop drives only the pace.
        let nak_rate = self.naks_recv_window as f64 / self.sent_window.max(1) as f64;
        self.naks_recv_window = 0;
        self.sent_window = 0;

        // RATE: packet-pair CRUISE (the loss-independent controller). The path
        // is a sharp goodput CLIFF (clean below it, collapse on it) well under
        // the raw link rate, and every loss-derived rate signal (goodput, NAKs,
        // the congestion classifier) is confounded - random loss looks exactly
        // like a cliff overshoot, so a controller driven by them cuts when it
        // should hold and spirals. The receiver's packet-pair CAPACITY estimate
        // is the one signal random loss cannot confound: the bottleneck imposes
        // the consecutive-id dispersion gap regardless of how many packets drop.
        // Cruise just UNDER that measured capacity and let the FEC cover the
        // residual loss - never probe into the cliff. (fb_cong stays captured
        // for telemetry only; the path's classifier mis-reads netem loss.)
        if self.fb_capacity_bps > 0.0 {
            // PRIMARY: cruise at a FRACTION of the measured RAW capacity. The
            // pair dispersion reads the raw bottleneck rate (~the link), but the
            // operational cliff sits below it (the bufferless bottleneck collapses
            // before raw capacity), so the fraction stays under it.
            //
            // The fraction itself adapts within a SAFE BAND from the FEC-miss
            // (NAK) signal: push toward the cliff when the coding is comfortably
            // covering (the path is clean, spend the headroom), back off when it
            // strains (loss is eating into the FEC, widen the margin). Because the
            // cruise is anchored to the loss-INDEPENDENT capacity and the fraction
            // is clamped to a band that never reaches the cliff, this cannot
            // spiral - the worst case is the bottom of the band, still a safe
            // sub-cliff pace. That is the difference from every loss-confounded
            // controller that collapsed: bounded fine-tuning, not unbounded chase.
            // Climb only when the path is genuinely clean (FEC misses near zero);
            // HOLD at the start fraction under moderate loss the FEC is covering;
            // back off ONLY on a near-cliff NAK spike (> 2% of sends missed), not
            // on routine FEC-recoverable loss - backing off there just sheds
            // throughput the coding was handling fine.
            if nak_rate < 0.002 {
                self.push_fraction = (self.push_fraction + 0.004).min(PAIR_FRACTION_HI);
            } else if nak_rate > 0.02 {
                self.push_fraction = (self.push_fraction - 0.006).max(PAIR_FRACTION_LO);
            }
            let target = (self.fb_capacity_bps * self.push_fraction)
                .clamp(self.push_min_bps, self.push_max_bps);
            self.pace_bps += 0.25 * (target - self.pace_bps);
        } else {
            // BOOTSTRAP (no capacity sample yet, ~first feedback interval):
            // find-then-cruise on goodput until the first packet-pair lands.
            if self.fb_rate_bps > self.max_delivered_bps {
                self.max_delivered_bps = self.fb_rate_bps;
            } else {
                self.max_delivered_bps *= 0.999;
            }
            if self.fb_rate_bps > 0.0 && self.fb_rate_bps < self.max_delivered_bps * 0.7 {
                self.pace_bps = (self.pace_bps * 0.6).max(self.push_min_bps);
                self.max_delivered_bps *= 0.9;
                self.push_cooldown = 16;
            } else if self.push_cooldown > 0 {
                self.push_cooldown -= 1;
            } else {
                self.pace_bps =
                    (self.pace_bps * 1.01).clamp(self.push_min_bps, self.push_max_bps);
            }
        }
    }

    /// Migrate to a fresh local socket (a new ephemeral port) - the protocol-side
    /// effect of a NAT rebinding or an interface switch. The connection id is
    /// unchanged, so the receiver routes the session to the new address and keeps
    /// delivering; in-flight state (the retransmit buffer, the flow window, the
    /// TLS keys) is untouched, so no data is lost across the move.
    pub fn migrate(&mut self) -> io::Result<()> {
        let new_sock = UdpSocket::bind("0.0.0.0:0")?;
        new_sock.set_nonblocking(true)?;
        set_buffers(&new_sock);
        self.sock = crate::dgram::DgramSock::wrap(new_sock);
        // Announce the new address at once by retransmitting the most recent
        // in-flight symbol from the new socket, so the receiver migrates (and
        // resumes acking the new address) before the flow window can block
        // waiting on acks that would otherwise go to the abandoned socket.
        let last = self
            .sent
            .iter()
            .next_back()
            .map(|(&sid, sym)| (sid, sym.clone()));
        if let Some((sid, sym)) = last {
            // Migration announcement: recovery traffic, sent unpaced - flush the
            // GSO batch so the announce reaches the new path at once.
            self.send_data(sid, &sym)?;
            self.flush_gso()?;
        }
        Ok(())
    }

    /// The connection id stamped into every DATA / REPAIR.
    pub fn conn_id(&self) -> u64 {
        self.conn_id
    }

    /// The sender's current local socket address (changes across a migration).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// The datagram backend this sender's wire I/O resolved to (io_uring
    /// where available, else plain UDP).
    pub fn dgram_backend(&self) -> crate::dgram::DgramBackend {
        self.sock.backend()
    }

    /// Arm the OS path-event observer (item 12) so a route / carrier / MTU change
    /// drives a PROACTIVE migration. `iface` names the interface to watch; `None`
    /// auto-detects the first non-loopback up interface. With the observer armed,
    /// [`poll_path_event`](Self::poll_path_event) (called from `send_item`)
    /// rebinds the moment the kernel announces a path change - before loss.
    pub fn with_path_observer(mut self, iface: Option<String>) -> Self {
        let obs = crate::net_events::NetEventObserver::start(iface);
        self.last_event_count = obs.event_count();
        self.net_obs = Some(obs);
        self
    }

    /// Synthesise a path event, as if the OS had announced a route / carrier
    /// change - drives the `--sim-path-event` demo and the tests on a host where
    /// flapping a real interface is impractical. The production trigger is the
    /// armed observer firing on a real OS event.
    pub fn inject_path_event(&self) {
        if let Some(obs) = &self.net_obs {
            obs.inject_event();
        }
    }

    /// If the path-event observer has fired since the last check, migrate
    /// proactively (the new path is then pre-validated by the receiver before the
    /// old one fails). Returns whether a proactive migration was performed.
    pub fn poll_path_event(&mut self) -> io::Result<bool> {
        let count = match &self.net_obs {
            Some(obs) => obs.event_count(),
            None => return Ok(false),
        };
        if count > self.last_event_count {
            self.last_event_count = count;
            self.migrate()?;
            self.proactive_migrations += 1;
            return Ok(true);
        }
        Ok(false)
    }

    /// Migrations triggered by a path event (item-12 sensing) rather than an
    /// explicit [`migrate`](Self::migrate) call. Telemetry.
    pub fn proactive_migrations(&self) -> u64 {
        self.proactive_migrations
    }

    /// Pin the coding parameters at their initial values, ignoring feedback for
    /// retuning (the static baseline for an adaptive-vs-static comparison).
    /// Feedback is still received and counted, just never applied.
    pub fn with_static_params(mut self) -> Self {
        self.static_params = true;
        self
    }

    /// Latency-priority FEC: keep a light repair floor on at all times instead of
    /// disabling coding on a clean assessment, so an isolated loss recovers
    /// in-window rather than via an ARQ round trip that head-of-line-stalls the
    /// in-order stream. The right policy for the latency-priority code (the RLC
    /// leg of the unified transport, where the loss-driven switch hands bulk /
    /// high-loss traffic to block-RS and keeps RLC for the low-latency regime).
    pub fn with_latency_priority(mut self) -> Self {
        self.controller.set_latency_floor();
        self
    }

    /// The live `(window, step, dt, coding_on)` coding parameters (telemetry).
    pub fn coding_params(&self) -> (u16, u16, u8, bool) {
        let (w, s, d) = self.enc.params();
        (w as u16, s as u16, d, self.enc.coding_on())
    }

    /// Times the live coding parameters changed under feedback (telemetry).
    pub fn adapt_count(&self) -> u64 {
        self.adapt_count
    }

    /// FEEDBACK frames received from the receiver (telemetry).
    pub fn feedback_recv(&self) -> u64 {
        self.feedback_recv
    }

    /// The forward-loss fraction the receiver last fed back over the FEEDBACK
    /// frame (0.0..=1.0). The unified endpoint reads this to drive the
    /// loss-driven RLC -> RS code switch.
    pub fn fb_loss(&self) -> f64 {
        self.fb_loss
    }

    /// The RTprop estimate (milliseconds), the minimum delivery round trip seen,
    /// or `-1.0` before the first sample (telemetry).
    pub fn rtt_ms(&self) -> f32 {
        if self.min_rtt_us == u64::MAX {
            -1.0
        } else {
            self.min_rtt_us as f32 / 1000.0
        }
    }

    /// The next source id that will be assigned (i.e. the count of items sent).
    pub fn next_source_id(&self) -> u32 {
        self.enc.next_source_id()
    }

    /// Re-base this sender's source-id stream to `base` for a cross-code resync.
    /// The unified layer calls this when handing the stream BACK to RLC after
    /// another code carried the ids in between: RLC's running source id has
    /// diverged from the global item index (it only advanced for RLC-phase items),
    /// so the next source symbol must be re-aligned to the global boundary `base`.
    /// The encoder re-bases (next id `base`, fresh window), the retransmit buffer
    /// and per-id timers drop (the old tail was delivered by the other code), and
    /// the delivery frontier moves to `base` so the (now empty) flow window admits
    /// the resumed stream immediately. BBR's path estimate is kept; only its
    /// per-id sample map is cleared.
    pub fn skip_to(&mut self, base: u32) {
        self.enc.rebase_to(base);
        self.acked_through = base;
        self.sent.clear();
        self.last_tx.clear();
        self.bbr_samples.clear();
    }

    /// Current in-flight symbol bound. With `bbr_cwnd`, this is BBR's congestion
    /// window (`cwnd_gain * BtlBw * RTprop`) in symbols once a bandwidth sample
    /// exists, bootstrapping from `flow_window` until then; otherwise the static
    /// `flow_window`. A 4-symbol floor keeps the pipe from fully draining.
    fn effective_window(&self) -> u32 {
        if self.bbr_cwnd && self.bbr.has_estimate() {
            let packet = (self.symbol_len + DATA_HDR) as u64;
            ((self.bbr.cwnd_bytes() / packet.max(1)) as u32).max(4)
        } else {
            self.flow_window
        }
    }

    /// Ship one item: pack it into a source symbol, send the data datagram and
    /// any interleaved repair, then drain incoming NAK / ACK so retransmits and
    /// window trimming happen promptly.
    /// True when the flow window is full, so the next [`Self::send_item`] would
    /// block in its pacing wait. The unified layer polls this to escape an
    /// extreme-loss stall (a window that will not clear because RLC cannot decode
    /// the loss) by migrating to RS, rather than blocking here while the warmup
    /// keeps the loss-driven switch from ever evaluating.
    pub fn flow_blocked(&self) -> bool {
        self.sent.len() as u32 >= self.effective_window()
    }

    /// The receiver's cumulative in-order delivery frontier (every source id below
    /// it has been delivered). The unified layer reads this to size a cross-code
    /// handover: it is the boundary up to which RLC has delivered, so RS resends
    /// the un-acked tail `[acked_through, items_sent)` from there.
    pub fn acked_through(&self) -> u32 {
        self.acked_through
    }

    /// Service inbound ACKs/SACKs and flush any queued (re)transmits once. The
    /// unified layer calls this in its flow-block escape loop so a window can
    /// still clear (and retransmits still reach the wire) while it decides
    /// whether to switch codes.
    pub fn pump_once(&mut self) -> io::Result<()> {
        self.flush_gso()?;
        self.pump()?;
        self.flush_gso()?;
        Ok(())
    }

    pub fn send_item(&mut self, item: &[u8]) -> io::Result<()> {
        // Proactive migration (item 12): if the OS announced a path change since
        // the last item, migrate now - before loss - so the receiver pre-validates
        // the new path while the old one still carries data. A no-op when no
        // observer is armed or none has fired.
        self.poll_path_event()?;
        // Flow control: pace while too many symbols are outstanding (sent but
        // not yet confirmed received), so a burst never overruns the receiver or
        // the kernel socket buffer. `sent` holds exactly the unconfirmed symbols
        // (a SACK releases each as it is received), so its length is the
        // outstanding count - a hole costs one slot, not the whole window.
        // The in-flight bound is BBR's dynamic cwnd when enabled (re-read each
        // iteration so an ACK that grows BtlBw immediately relaxes the gate),
        // else the static flow_window.
        let start = Instant::now();
        while self.sent.len() as u32 >= self.effective_window() {
            // Blocked on the window: get any GSO-batched source symbols on the
            // wire so they can be acked (and the window can clear), then pump,
            // then flush again so any retransmits the pump queued go out too -
            // otherwise a retransmit stuck in the batch could deadlock the wait.
            self.flush_gso()?;
            self.pump()?;
            self.flush_gso()?;
            if self.sent.len() as u32 >= self.effective_window() {
                std::thread::sleep(Duration::from_micros(50));
            }
            if start.elapsed() > Duration::from_secs(60) {
                break;
            }
        }
        // Adaptive FEC-push: retune the pacing rate from the delivered-rate
        // signal (~once per RTT) before pacing this symbol.
        self.adapt_push_rate();
        // Packet-pair PROBE: every Nth symbol under the adaptive push, ship this
        // one WITHOUT its pacing gap so it lands back-to-back with the previous -
        // a tight pair from which the receiver reads the bottleneck dispersion
        // (capacity). Roughly 6% of symbols; the cruise targets 82% of capacity,
        // so the small over-rate from the un-paced symbol stays under the cliff.
        // next_send is left untouched, so the following symbol re-paces from the
        // prior due instant and the average rate recovers.
        const PAIR_PROBE_EVERY: u32 = 16;
        let pair_probe = self.adaptive_push && {
            self.pair_probe_ctr = self.pair_probe_ctr.wrapping_add(1);
            self.pair_probe_ctr.is_multiple_of(PAIR_PROBE_EVERY)
        };
        // Fixed-rate pacing (the offensive FEC-push lever): pace the wire at the
        // configured rate regardless of window/RTT, driving throughput toward the
        // path's raw capacity past where a loss-based controller backs off. The
        // FEC recovers the loss from operating near the ceiling. Spin-pace for
        // precision; clamp banked credit to one interval so a stall cannot
        // re-burst.
        if self.pace_bps > 0.0 && !pair_probe {
            let packet = (self.symbol_len + DATA_HDR) as f64;
            let interval = Duration::from_secs_f64(packet / self.pace_bps);
            let due = self.next_send.unwrap_or_else(Instant::now);
            loop {
                let now = Instant::now();
                if now >= due {
                    break;
                }
                if due - now > Duration::from_millis(1) {
                    self.pump()?;
                    std::thread::sleep(Duration::from_micros(200));
                } else {
                    std::hint::spin_loop();
                }
            }
            let floor = Instant::now().checked_sub(interval).unwrap_or(due);
            self.next_send = Some(due.max(floor) + interval);
        } else if self.bbr_cwnd {
            let rate = self.bbr.pacing_rate_bps();
            if rate > 0.0 {
                let packet = (self.symbol_len + DATA_HDR) as f64;
                let interval = Duration::from_secs_f64(packet / rate);
                let due = self.next_send.unwrap_or_else(Instant::now);
                // Spin-pace: thread::sleep is too coarse at line rate (~20us per
                // packet), so spin for sub-ms waits and only sleep (pumping ACKs)
                // for longer ones. Tight pacing keeps in-flight near 1*BDP, so
                // the 2*BDP cwnd headroom that lets ProbeBW's 1.25x probe fit is
                // never bursted into all at once (which manufactured the loss).
                loop {
                    let now = Instant::now();
                    if now >= due {
                        break;
                    }
                    if due - now > Duration::from_millis(1) {
                        self.pump()?;
                        std::thread::sleep(Duration::from_micros(400));
                    } else {
                        std::hint::spin_loop();
                    }
                }
                // Clamp banked credit to one interval so a stall cannot re-burst.
                let floor = Instant::now().checked_sub(interval).unwrap_or(due);
                self.next_send = Some(due.max(floor) + interval);
            }
        } else if self.paced && self.min_rtt_us != u64::MAX {
            // Static pacing: spread the window evenly over the RTT - one symbol
            // every `RTT / window` - so a window sized to the BDP fills the path
            // without bursting the shallow bottleneck buffer (the unpaced cliff
            // at >512 in-flight). The pace rate rises with the window, so
            // `flow_window` alone tunes the target rate (window * pkt / RTT).
            let win = self.effective_window().max(1) as u64;
            let interval = Duration::from_micros((self.min_rtt_us / win).max(1));
            let due = self.next_send.unwrap_or_else(Instant::now);
            loop {
                let now = Instant::now();
                if now >= due {
                    break;
                }
                if due - now > Duration::from_millis(1) {
                    self.pump()?;
                    std::thread::sleep(Duration::from_micros(200));
                } else {
                    std::hint::spin_loop();
                }
            }
            let floor = Instant::now().checked_sub(interval).unwrap_or(due);
            self.next_send = Some(due.max(floor) + interval);
        }
        self.send_item_now(item)
    }

    /// The send body shared by [`Self::send_item`] (which waits on the flow
    /// window + paces first) and [`Self::try_send_item`] (which does neither):
    /// pack the item as a source symbol, enqueue it, ship it plus any due repair,
    /// and service ACKs.
    fn send_item_now(&mut self, item: &[u8]) -> io::Result<()> {
        let sym = pack_symbol(item, self.symbol_len);
        let (sid, repair) = self.enc.push_source(&sym);
        self.sent.insert(sid, sym.clone());
        self.sent_window = self.sent_window.saturating_add(1);
        // Feed BBR the per-symbol rate-sample snapshot (consumed when this symbol
        // is delivered, in the ACK handler). BBR measures and exposes BtlBw /
        // RTprop as telemetry; the flow window governs the in-flight bound.
        // Driving pace/cwnd from BBR on this pace-limited FEC flow under-measures
        // the bottleneck and oscillates (the open work is bead SubEtha-i4f).
        let sample = self.bbr.on_send(Instant::now(), false);
        self.bbr_samples.insert(sid, sample);
        // Start an RTprop probe if none is in flight: time this id from send to
        // the ACK that delivers it.
        if self.probe.is_none() {
            self.probe = Some((sid, Instant::now()));
        }
        self.send_data(sid, &sym)?;
        self.last_tx.insert(sid, Instant::now());
        if let Some(r) = repair {
            self.send_repair(&r)?;
        }
        self.pump()?;
        Ok(())
    }

    /// Like [`Self::send_item`] but NEVER blocks on the flow window: returns
    /// `Ok(false)` without sending when the window is full, so the CALLER owns the
    /// wait (and can escape to a stronger code instead of stalling inside a
    /// blocking send the unified layer cannot see). `Ok(true)` when the item was
    /// sent. Pacing is skipped (the unified caller drives cadence and leaves
    /// pacing off); a `poll_path_event` still runs so proactive migration holds.
    pub fn try_send_item(&mut self, item: &[u8]) -> io::Result<bool> {
        self.poll_path_event()?;
        if self.flow_blocked() {
            self.pump()?;
            return Ok(false);
        }
        self.send_item_now(item)?;
        Ok(true)
    }

    /// BBR's measured bottleneck-bandwidth estimate (bytes/s), telemetry.
    pub fn btlbw_bps(&self) -> f64 {
        self.bbr.btlbw_bps()
    }

    /// Retransmit `sid` only if it has not been (re)sent within ~1.2 RTT - the
    /// retransmit-suppression guard that stops the same still-missing symbol from
    /// being resent on every ~1ms NAK round before its previous copy can be
    /// acked a round trip later. Returns whether it was actually sent.
    fn retransmit_if_due(&mut self, sid: u32, sym: &[u8]) -> io::Result<bool> {
        let rtt_us = self.min_rtt_us.clamp(15_000, 200_000);
        let cooldown = Duration::from_micros(rtt_us * 6 / 5);
        let due = self
            .last_tx
            .get(&sid)
            .map(|t| t.elapsed() >= cooldown)
            .unwrap_or(true);
        if due {
            self.send_data(sid, sym)?;
            self.last_tx.insert(sid, Instant::now());
        }
        Ok(due)
    }

    fn send_data(&mut self, sid: u32, sym: &[u8]) -> io::Result<()> {
        let send_us = self.start.elapsed().as_micros() as u32;
        let mut pkt = Vec::with_capacity(DATA_HDR + sym.len());
        pkt.push(PKT_RLC_DATA);
        pkt.extend_from_slice(&self.conn_id.to_le_bytes());
        pkt.extend_from_slice(&sid.to_le_bytes());
        pkt.extend_from_slice(&send_us.to_le_bytes());
        pkt.extend_from_slice(sym);
        self.enqueue_wire(&pkt)?;
        Ok(())
    }

    fn send_repair(&mut self, r: &RepairSymbol) -> io::Result<()> {
        // A repair is a different size than a DATA datagram, so it cannot ride
        // the DATA GSO batch: flush the accumulated source symbols first, so the
        // repair still ships immediately after the window it protects (FEC timing
        // unchanged), then send the repair on its own.
        self.flush_gso()?;
        let mut pkt = Vec::with_capacity(20 + r.payload.len());
        pkt.push(PKT_RLC_REPAIR);
        pkt.extend_from_slice(&self.conn_id.to_le_bytes());
        pkt.extend_from_slice(&r.repair_key.to_le_bytes());
        pkt.extend_from_slice(&r.first_source_id.to_le_bytes());
        pkt.extend_from_slice(&r.window_size.to_le_bytes());
        pkt.push(r.dt);
        pkt.extend_from_slice(&r.payload);
        // Repairs are paced: they are the bulk flow whose FEC escalation must
        // not overflow the bottleneck queue.
        self.wire_send(&pkt)?;
        Ok(())
    }

    /// Drain pending NAK / ACK / FEEDBACK datagrams: retransmit NAK'd source
    /// symbols, advance the acked frontier (trimming the coding window and the
    /// ARQ hold), and retune the coding from each FEEDBACK through the
    /// controller. The buffer holds a full 16-source-id NAK (1 + 16*4 = 65).
    pub fn pump(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 256];
        loop {
            match self.sock.recv_from(&mut buf) {
                Ok((n, _)) if n >= 1 => {
                    // Open the AEAD envelope when TLS is on; the handlers always
                    // see the cleartext inner datagram. A non-sealed frame under
                    // TLS (a stray handshake retransmit) is skipped.
                    #[cfg(feature = "tls")]
                    if self.crypto.is_some() {
                        let inner = self.crypto.as_ref().and_then(|c| secure_unwrap(c, &buf[..n]));
                        if let Some(inner) = inner {
                            self.handle_pump_frame(&inner)?;
                        }
                        continue;
                    }
                    self.handle_pump_frame(&buf[..n])?;
                }
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => break,
                // See poll(): a spurious Windows UDP ConnectionReset from an ICMP
                // port-unreachable is transient, not fatal.
                Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => break,
                Err(e) => return Err(e),
            }
        }
        // RTO recovery: if the cumulative ACK has not advanced for a few RTTs,
        // the in-order frontier is stuck on a symbol the receiver cannot NAK -
        // a frontier hole it never learned exists (the same root cause as the
        // end-of-stream tail, but mid-stream while the sender is flow-blocked
        // and so never reaches drain_until_acked). Retransmit the lowest unacked
        // symbols sender-side, the way TCP retransmits on an RTO, so the stall
        // self-heals instead of hanging on a NAK that can never come.
        if !self.sent.is_empty()
            && self.last_ack_advance.elapsed() >= self.rto()
            && self.last_rto_rtx.elapsed() >= self.rto()
        {
            self.retransmit_unacked()?;
            self.last_rto_rtx = Instant::now();
        }
        Ok(())
    }

    /// Dispatch one (already-decrypted) NAK / ACK / FEEDBACK frame.
    fn handle_pump_frame(&mut self, m: &[u8]) -> io::Result<()> {
        let n = m.len();
        match m[0] {
            PKT_RLC_NAK => {
                let mut off = 1;
                while off + 4 <= n {
                    let sid = u32::from_le_bytes([m[off], m[off + 1], m[off + 2], m[off + 3]]);
                    off += 4;
                    // A NAK is an FEC miss: count it for the coordinated
                    // loss-aware control (parity-up / pace-down) signal.
                    self.naks_recv_window = self.naks_recv_window.saturating_add(1);
                    if let Some(sym) = self.sent.get(&sid).cloned() {
                        // NAK retransmit, suppressed if the same id was resent
                        // within the last ~1.2 RTT (its prior copy is still in
                        // flight) - this is what stops the NAK amplification flood.
                        self.retransmit_if_due(sid, &sym)?;
                    }
                }
            }
            PKT_RLC_ACK if n >= 5 => {
                // Cumulative in-order received frontier, then an optional
                // 64-bit SACK bitmap of received ids above it.
                let through = u32::from_le_bytes([m[1], m[2], m[3], m[4]]);
                // RTprop: when this ACK delivers the in-flight probe id,
                // min-filter `now - probe_time` into the RTT estimate.
                if let Some((psid, ptime)) = self.probe
                    && through > psid
                {
                    let s = ptime.elapsed().as_micros() as u64;
                    self.min_rtt_us = self.min_rtt_us.min(s);
                    self.probe = None;
                }
                // Collect the rate-sample snapshots of every symbol newly
                // known-received on this ACK (cumulative advance + SACK), to
                // feed BBR's delivery-rate estimator one batch per ACK.
                let mut delivered: Vec<crate::bbr::PacketSample> = Vec::new();
                if through > self.acked_through {
                    for sid in self.acked_through..through {
                        if let Some(s) = self.bbr_samples.remove(&sid) {
                            delivered.push(s);
                        }
                    }
                    self.acked_through = through;
                    self.enc.forget_below(through);
                    self.sent.retain(|&sid, _| sid >= through);
                    self.last_tx.retain(|&sid, _| sid >= through);
                    self.last_ack_advance = Instant::now();
                }
                // Release each SACK'd (received-above-the-hole) symbol from the
                // retransmit buffer so it stops counting against the outstanding
                // window - this is what lets a hole cost one slot, not the window.
                if n >= 13 {
                    let sack = u64::from_le_bytes([
                        m[5], m[6], m[7], m[8], m[9], m[10], m[11], m[12],
                    ]);
                    for i in 0..64u32 {
                        if sack & (1u64 << i) != 0 {
                            let sid = through.wrapping_add(1 + i);
                            self.sent.remove(&sid);
                            if let Some(s) = self.bbr_samples.remove(&sid) {
                                delivered.push(s);
                            }
                        }
                    }
                }
                if !delivered.is_empty() {
                    self.bbr.on_ack(Instant::now(), &delivered);
                }
            }
            PKT_RLC_FEEDBACK if n >= 4 => {
                self.feedback_recv += 1;
                self.apply_feedback(m[1], m[2], m[3]);
                // The receiver's real delivered (goodput) rate rides along when
                // present (control-plane ground truth for the adaptive push).
                if n >= 6 {
                    let rate_mbit = u16::from_le_bytes([m[4], m[5]]) as f64;
                    self.fb_rate_bps = rate_mbit * 1.0e6 / 8.0;
                }
                // Packet-pair CAPACITY (loss-independent): the bottleneck
                // dispersion the receiver read from the tightest consecutive-id
                // gap. The adaptive push cruises just under it instead of
                // probing the cliff. 0 means "not measured yet" - ignore it.
                if n >= 8 {
                    let cap_mbit = u16::from_le_bytes([m[6], m[7]]) as f64;
                    if cap_mbit > 0.0 {
                        self.fb_capacity_bps = cap_mbit * 1.0e6 / 8.0;
                    }
                }
            }
            PKT_RLC_PATH_CHALLENGE if n >= PATH_FRAME_LEN => {
                // The receiver is validating this (just-migrated) address. Echo
                // the connection id + nonce verbatim so it can confirm we hold
                // the session here and lift its anti-amplification cap.
                let mut resp = Vec::with_capacity(PATH_FRAME_LEN);
                resp.push(PKT_RLC_PATH_RESPONSE);
                resp.extend_from_slice(&m[1..PATH_FRAME_LEN]);
                // Path-validation control: unpaced.
                self.wire_send(&resp)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Turn one quantized FEEDBACK triple into a channel assessment, run the
    /// controller, and apply any new coding parameters to the live encoder. A
    /// no-op when pinned static.
    fn apply_feedback(&mut self, loss_q8: u8, burst_q8: u8, cong_q8: u8) {
        // Capture the receiver's real measured loss AND congestion fraction for
        // the adaptive-push loop FIRST - they drive it even when the coding is
        // static. fb_cong (the Biaz/Spike classifier's congestion share) is the
        // RFC 9265 signal: the FEC recovers RANDOM loss (push through it), but
        // the rate must still YIELD to CONGESTION loss (the FEC must not hide
        // it). Random loss => keep filling; congestion loss => back off.
        self.fb_loss = loss_q8 as f64 / 255.0;
        self.fb_cong = cong_q8 as f64 / 255.0;
        // The static baseline never changes the coding. Otherwise the sensing
        // controller below owns the CODING (window / step / density / disable-on-
        // clean) from the fused loss + burstiness + congestion signal - INCLUDING
        // in adaptive-push mode, where the packet-pair loop owns only the RATE
        // (pace). The two compose without fighting: pace and coding are orthogonal
        // knobs, so the push fills the wire while the sensing controller sizes the
        // FEC to the channel the receiver actually measured (proactive provision
        // from real loss + burst, not reactive from post-miss NAKs).
        if self.static_params {
            return;
        }
        let snapshot = SensorSnapshot {
            loss: loss_q8 as f32 / 255.0,
            burstiness: burst_q8 as f32 / 255.0,
            // KEEP the congestion term in the FEC sizing even though the
            // classifier is unreliable as a CONGESTION signal here. Measured on
            // the WAN path: neutralizing it regressed under-loss throughput (5%
            // 306->281, 8% 291->237). FEC strength and rate are coupled through
            // the NAK signal - the rate controller backs off on NAK spikes, so
            // the extra parity the congestion term provisions functions as MARGIN
            // that keeps FEC misses (and thus NAKs, and thus rate backoff) down.
            // The over-provision is net-positive, not waste. (Hard data, not
            // theory: lighter parity -> more misses -> more backoff -> lower rate.)
            congestion_fraction: cong_q8 as f32 / 255.0,
            ..SensorSnapshot::default()
        };
        // Weight FEC against ARQ by the measured round trip: an expensive NAK
        // (high RTprop) makes heavier FEC worth it.
        if self.min_rtt_us != u64::MAX {
            self.controller.set_rtt_ms(self.min_rtt_us as f32 / 1000.0);
        }
        let d = self.controller.decide(&snapshot);
        let (cw, cs, cd) = self.enc.params();
        let changed =
            cw != d.window as usize || cs != d.step as usize || cd != d.dt || self.enc.coding_on() != d.coding_on;
        if changed {
            self.enc.set_params(d.window as usize, d.step as usize, d.dt);
            self.enc.set_coding(d.coding_on);
            self.adapt_count += 1;
            if self.fec_debug {
                eprintln!(
                    "FECDEC loss={:.3} burst={:.3} cong={:.3} -> win={} step={} dt={} coding={}",
                    snapshot.loss,
                    snapshot.burstiness,
                    snapshot.congestion_fraction,
                    d.window,
                    d.step,
                    d.dt,
                    d.coding_on,
                );
            }
        }
    }

    /// Pump NAK / ACK until the receiver has delivered every source id below
    /// `total`, or the timeout elapses. Returns whether full delivery was acked.
    ///
    /// End-of-stream delivery is SENDER-driven, not NAK-driven. A NAK-only
    /// scheme cannot recover a lost tail: the receiver only NAKs gaps below
    /// its `highest_seen`, and when the final source symbols AND any trailing
    /// repair are all lost, `highest_seen` never reaches them - so the
    /// receiver requests nothing and delivery deadlocks until this timeout.
    /// (The matrix bench surfaced this as an intermittent ~96%-complete stall
    /// under loss.) The sender knows exactly which ids are still unacked, so
    /// it retransmits them on an RTT-paced cadence until the cumulative ACK
    /// reaches `total`, the way TCP retransmits unacked data on an RTO.
    pub fn drain_until_acked(&mut self, total: u32, timeout: Duration) -> io::Result<bool> {
        // Get the final partial GSO batch on the wire before draining, else its
        // source symbols are never sent and delivery cannot complete.
        self.flush_gso()?;
        let start = Instant::now();
        let mut last_retx = Instant::now();
        while self.acked_through < total {
            if start.elapsed() > timeout {
                return Ok(false);
            }
            self.pump()?;
            if last_retx.elapsed() >= self.tail_rtx_interval() {
                self.retransmit_unacked()?;
                // Tail retransmits route through the DATA path (enqueue); flush
                // so they actually reach the wire on this drain tick.
                self.flush_gso()?;
                last_retx = Instant::now();
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(true)
    }

    /// Retransmit the lowest still-unacked source symbols - the ones blocking
    /// the receiver's in-order frontier. Only a small batch per call (the
    /// frontier plus a few, `sent` being an ordered BTreeMap), so this recovers
    /// the tail / a small hole cluster without itself adding a flood to an
    /// already-congested link; as each cumulative ACK advances, the next batch
    /// is exposed. Idempotent at the receiver (it dedups by source id).
    fn retransmit_unacked(&mut self) -> io::Result<()> {
        const TAIL_RTX_BATCH: usize = 8;
        let ids: Vec<u32> = self.sent.keys().copied().take(TAIL_RTX_BATCH).collect();
        for sid in ids {
            if let Some(sym) = self.sent.get(&sid).cloned() {
                // Tail / RTO retransmit, suppressed if its prior copy is still in
                // flight (same guard as the NAK path).
                self.retransmit_if_due(sid, &sym)?;
            }
        }
        Ok(())
    }

    /// RTT-paced retransmit cadence for sender-driven tail recovery: ~2x the
    /// measured round trip, clamped so it neither floods (a lower bound well
    /// above one RTT) nor stalls a long time before the drain timeout.
    fn tail_rtx_interval(&self) -> Duration {
        // Clamp before doubling: min_rtt_us is u64::MAX until the first RTT
        // sample lands, so cap it first to avoid an overflow in `2 * rtt_us`.
        let rtt_us = self.min_rtt_us.clamp(1_000, 100_000);
        Duration::from_micros((2 * rtt_us).clamp(20_000, 200_000))
    }
}

/// Receiver side of the RLC transport.
pub struct SensOMaticRlcReceiver {
    sock: crate::dgram::DgramSock,
    dec: RlcDecoder,
    symbol_len: usize,
    /// Next source id to deliver (everything below is delivered, in order).
    delivered_through: u32,
    /// `delivered_through` snapshot at the last FEEDBACK, so the feedback can
    /// carry the receiver's REAL delivered (goodput) rate over the control
    /// plane. That rate is the ground-truth signal the sender's adaptive push
    /// rides: the cliff shows as a delivered-rate plateau, unlike binary loss.
    last_fb_delivered: u32,
    /// Highest source id seen on any DATA / REPAIR, to know a gap is real.
    highest_seen: u32,
    peer: Option<SocketAddr>,
    last_nak: Instant,
    /// Telemetry: source symbols recovered by RLC (no retransmit needed) and
    /// NAKs sent (the ARQ floor).
    rlc_recovered: u64,
    naks_sent: u64,
    /// Diagnostic loss injection: drop this percent of incoming DATA datagrams
    /// (seeded) to exercise RLC recovery on a lossless link.
    drop_pct: u32,
    drop_rng: u64,
    /// Gilbert-Elliott burst-loss injection (per-10000 transition probabilities
    /// `p` Good->Bad and `r` Bad->Good). Mean burst `10000 / r`, steady loss
    /// `p / (p + r)`. `r = 0` disables it (the Bernoulli `drop_pct` path is used
    /// instead). The erasure is a deterministic function of the SOURCE ID, not
    /// of arrival timing, so two different codes (adaptive vs static) experience
    /// the identical loss process - a fair A/B - and a retransmit of an erased
    /// id is never erased a second time, so ARQ always converges.
    ge_loss_p: u32,
    ge_loss_r: u32,
    ge_bad: bool,
    /// Next source id whose erasure decision is undecided; the chain advances in
    /// id order, memoizing each decision in `ge_decided`.
    ge_pos: u32,
    ge_decided: HashMap<u32, bool>,
    /// Erased ids whose first transmission has already been dropped, so a
    /// retransmit passes through.
    ge_dropped_once: BTreeSet<u32>,
    /// Lifetime loss accounting (stable telemetry, vs the recency-weighted EWMA
    /// the controller consumes): source ids that were lost, over ids delivered.
    total_lost: u64,
    total_delivered: u64,
    /// First kernel RX timestamp (nanoseconds, `SO_TIMESTAMPNS`) seen, so the
    /// per-packet arrival the congestion detector reads is a small offset from
    /// it. `None` until the first stamped datagram (or always, where the kernel
    /// timestamp is unavailable and the drain-loop clock is used instead).
    kts_base_ns: Option<i128>,
    /// Monotonic start, for the relative one-way trip time the Spike arm reads.
    start: Instant,
    /// Arrival time (microseconds since `start`) of the previous DATA, stamped in
    /// the receive drain loop BEFORE any decode, for inter-arrival spacing and
    /// the relative one-way trip time - so neither is polluted by the Gaussian
    /// solve, which runs once after the whole batch is drained.
    last_arrival_us: Option<f64>,
    /// Last DATA source id seen, for the packet-pair (dispersion) capacity
    /// probe: when the next consecutive id arrives, the gap between the two is
    /// the bottleneck's transmission time for one packet - which the bottleneck
    /// imposes regardless of how many OTHER packets are dropped, so it measures
    /// path capacity INDEPENDENTLY of loss (the one signal random loss cannot
    /// confound). The sender ships occasional back-to-back pairs to drive it.
    last_data_sid: Option<u32>,
    /// Recent consecutive-id arrival gaps (microseconds) ABOVE the NAPI floor,
    /// a circular window. The raw distribution is bimodal: a near-zero mass from
    /// NAPI/GRO batching (multiple packets drained in one softirq poll share a
    /// timestamp) and the real bottleneck-dispersion cluster at ~18-26us. The
    /// `min` is poison (it grabs the batch noise); a LOW PERCENTILE of the
    /// floor-filtered gaps isolates the true dispersion robustly. Capacity =
    /// wire_bytes*8 / percentile_gap. Empty until enough gaps land.
    pair_ring: Vec<f64>,
    pair_ring_pos: usize,
    /// Diagnostic: when `SUBETHA_PAIR_DEBUG` is set, log every consecutive-id gap
    /// so the true dispersion distribution can be read (and NAPI-batch noise vs
    /// real bottleneck spacing separated). Empty / unused otherwise.
    pair_debug: bool,
    pair_gap_log: Vec<f64>,
    /// Last FEEDBACK send instant (rate-limited like the ACK).
    last_feedback: Instant,
    /// Gilbert-Elliott fit of the forward loss trace -> mean burst length, fed
    /// in delivery order as the contiguous frontier advances.
    burst_model: BurstModel,
    /// Congestion-vs-wireless loss differentiation (Biaz + Spike) -> congestion
    /// share of recent loss.
    loss_class: LossClassSensor,
    /// EWMA of the per-source-id lost indicator (1 = lost, 0 = arrived),
    /// folded in delivery order: the measured forward loss rate.
    loss_ewma: f32,
    /// Sliding window of the last `LOSS_WINDOW` lost-indicators, and the count of
    /// losses within it. The loss rate the controller provisions FEC against is
    /// `lost / max(len, LOSS_WINDOW_MIN_FILL)`. A WINDOWED rate (not the burst
    /// model's cumulative `losses/n`, which is anchored by startup samples and
    /// never forgets - it read 44% during a 6% transfer and only relaxed at the
    /// very end) tracks the true sustained loss quickly; the MIN_FILL denominator
    /// suppresses the cold-start spike that otherwise drives FEC straight to max.
    loss_window: VecDeque<bool>,
    loss_window_lost: u32,
    /// Source ids the FEC recovered (no original DATA needed). Pending the
    /// deferred loss accounting: a FEC-recovered id whose original DATA later
    /// arrives was merely reordered, not lost.
    fec_recovered: BTreeSet<u32>,
    /// Source ids whose original DATA arrived (any time), to distinguish a
    /// reordered-then-recovered id from a genuinely-lost one.
    data_arrived: BTreeSet<u32>,
    /// Delivered ids awaiting loss accounting, `(sid, account_at_us, was_fec,
    /// was_nak)`: the fold into the burst model / loss rate is deferred by the
    /// reorder grace so a late original can correct a reorder mis-counted as loss.
    loss_pending: VecDeque<(u32, f64, bool, bool)>,
    nakd: BTreeSet<u32>,
    /// When each currently-missing id was first detected as a gap (microseconds
    /// since `start`), so a NAK waits out a reorder-tolerance grace before
    /// declaring it lost - on a jittery path a packet may simply be reordered,
    /// not dropped, and NAKing it early counts a false loss.
    gap_since: BTreeMap<u32, f64>,
    /// FEEDBACK frames sent (telemetry).
    feedback_sent: u64,
    /// The connection id this receiver is bound to (learned from the first DATA),
    /// and how many times the session migrated to a new peer address - the count
    /// is the proof the connection survived a 4-tuple change.
    session_cid: Option<u64>,
    migrations: u64,
    /// Slice 4 path validation. When the session appears at a NEW address the
    /// receiver migrates optimistically (it keeps delivering - the AEAD / id
    /// already authenticate the frame) but marks the new address unvalidated and
    /// challenges it: `pending_challenge` holds `(addr, nonce, sent_at)` and the
    /// peer is trusted for reachability only once a matching `PATH_RESPONSE`
    /// returns. `peer_validated` gates the anti-amplification cap; `prev_peer`
    /// is the address to revert to if validation times out (a spoofed move).
    peer_validated: bool,
    pending_challenge: Option<(SocketAddr, u64, Instant)>,
    prev_peer: Option<SocketAddr>,
    /// Monotonic nonce source for challenges (mixed through splitmix64 so the
    /// emitted nonce is not a guessable counter).
    challenge_seq: u64,
    /// Anti-amplification accounting toward the unvalidated address: the receiver
    /// will not send more than `AMPLIFICATION_FACTOR x` the bytes it has received
    /// from that address until the path validates.
    unval_recv_bytes: u64,
    unval_sent_bytes: u64,
    /// Successful path validations and validation timeouts (reverts). Telemetry.
    path_validations: u64,
    path_validation_failures: u64,
    /// Optional TLS state (server side): when present, every data datagram is
    /// AEAD-sealed / opened with the 1-RTT keys.
    #[cfg(feature = "tls")]
    crypto: Option<crate::rlc_crypto::CryptoState>,
}

impl SensOMaticRlcReceiver {
    /// Bind a receiver over `symbol_len`-byte symbols.
    pub fn bind<A: ToSocketAddrs>(local: A, symbol_len: usize) -> io::Result<Self> {
        let sock = UdpSocket::bind(local)?;
        sock.set_nonblocking(true)?;
        set_buffers(&sock);
        // Auto-detect the datagram backend (io_uring where available, plain
        // UDP otherwise); kernel RX timestamps are enabled by the wrapper.
        let sock = crate::dgram::DgramSock::wrap(sock);
        Ok(Self {
            sock,
            // Horizon is sized to the CODING window (the adaptive RLC window
            // caps at 64), not the flow window: a sliding-window repair can only
            // span its own window, so a gap older than ~one window has no repair
            // covering it and is unrecoverable by RLC regardless of horizon -
            // that gap is the ARQ floor's job. 128 = 2x the max window keeps the
            // Gaussian solve small (the dominant per-repair cost under loss).
            dec: RlcDecoder::new(symbol_len).with_horizon(128),
            symbol_len,
            delivered_through: 0,
            last_fb_delivered: 0,
            highest_seen: 0,
            peer: None,
            last_nak: Instant::now(),
            rlc_recovered: 0,
            naks_sent: 0,
            drop_pct: 0,
            drop_rng: 0,
            ge_loss_p: 0,
            ge_loss_r: 0,
            ge_bad: false,
            ge_pos: 0,
            ge_decided: HashMap::new(),
            ge_dropped_once: BTreeSet::new(),
            total_lost: 0,
            total_delivered: 0,
            loss_window: VecDeque::with_capacity(LOSS_WINDOW),
            loss_window_lost: 0,
            kts_base_ns: None,
            last_data_sid: None,
            pair_ring: Vec::with_capacity(PAIR_RING_CAP),
            pair_ring_pos: 0,
            pair_debug: std::env::var("SUBETHA_PAIR_DEBUG").is_ok(),
            pair_gap_log: Vec::new(),
            start: Instant::now(),
            last_arrival_us: None,
            last_feedback: Instant::now(),
            burst_model: BurstModel::new(),
            loss_class: LossClassSensor::new(),
            loss_ewma: 0.0,
            fec_recovered: BTreeSet::new(),
            data_arrived: BTreeSet::new(),
            loss_pending: VecDeque::new(),
            nakd: BTreeSet::new(),
            gap_since: BTreeMap::new(),
            feedback_sent: 0,
            session_cid: None,
            migrations: 0,
            peer_validated: true,
            pending_challenge: None,
            prev_peer: None,
            challenge_seq: 0,
            unval_recv_bytes: 0,
            unval_sent_bytes: 0,
            path_validations: 0,
            path_validation_failures: 0,
            #[cfg(feature = "tls")]
            crypto: None,
        })
    }

    /// How many times the session migrated to a new peer address (connection-id
    /// routing surviving a 4-tuple change). Telemetry.
    pub fn migrations(&self) -> u64 {
        self.migrations
    }

    /// How many new peer addresses were confirmed reachable by the
    /// PATH_CHALLENGE / PATH_RESPONSE exchange (Slice 4). Telemetry.
    pub fn path_validations(&self) -> u64 {
        self.path_validations
    }

    /// How many candidate addresses failed to answer the challenge within
    /// `CHALLENGE_TIMEOUT` and were reverted (a spoofed move). Telemetry.
    pub fn path_validation_failures(&self) -> u64 {
        self.path_validation_failures
    }

    /// The peer address the receiver is currently routing the session to.
    pub fn peer(&self) -> Option<SocketAddr> {
        self.peer
    }

    /// Swap the datagram socket for one the caller already built (a demux
    /// socket the unified endpoint shares across both codes).
    pub fn set_sock(&mut self, sock: crate::dgram::DgramSock) {
        self.sock = sock;
    }

    /// Arm the optional TLS record layer as the server. Call
    /// [`handshake`](Self::handshake) before polling for data.
    #[cfg(feature = "tls")]
    pub fn with_tls_server(mut self, cfg: std::sync::Arc<rustls::ServerConfig>) -> io::Result<Self> {
        self.crypto = Some(
            crate::rlc_crypto::CryptoState::new_server(cfg)
                .map_err(io::Error::other)?,
        );
        Ok(self)
    }

    /// Run the TLS handshake as the server (no-op when TLS is not armed). Blocks
    /// until the client connects and the 1-RTT keys are derived; learns the peer.
    #[cfg(feature = "tls")]
    pub fn handshake(&mut self) -> io::Result<()> {
        if let Some(crypto) = self.crypto.as_mut() {
            let peer = drive_handshake(&self.sock, None, crypto, false)?;
            self.peer = Some(peer);
        }
        Ok(())
    }

    /// Send one inner datagram to the learned peer, AEAD-sealing it when TLS is
    /// on. While the peer address is unvalidated (a migration in flight) the send
    /// is held to the anti-amplification cap: at most `AMPLIFICATION_FACTOR x` the
    /// bytes received from that address, so a spoofed move cannot make the
    /// receiver flood a victim. A dropped send is silently skipped - validation
    /// completes within a round trip and the cap lifts.
    fn wire_send_to_peer(&mut self, inner: &[u8]) -> io::Result<()> {
        let Some(peer) = self.peer else { return Ok(()) };
        if !self.peer_validated {
            let budget = self.unval_recv_bytes.saturating_mul(AMPLIFICATION_FACTOR);
            if self.unval_sent_bytes.saturating_add(inner.len() as u64) > budget {
                return Ok(());
            }
            self.unval_sent_bytes = self.unval_sent_bytes.saturating_add(inner.len() as u64);
        }
        #[cfg(feature = "tls")]
        if let Some(c) = &self.crypto {
            let wire = secure_wrap(c, inner)?;
            return send_with_retry(&self.sock, &wire, peer);
        }
        send_with_retry(&self.sock, inner, peer)
    }

    /// The bound local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// The datagram backend this receiver's wire I/O resolved to (io_uring
    /// where available, else plain UDP).
    pub fn dgram_backend(&self) -> crate::dgram::DgramBackend {
        self.sock.backend()
    }

    /// Inject diagnostic DATA loss: drop `pct` percent of incoming data
    /// datagrams (seeded, reproducible) to exercise RLC recovery on loopback.
    pub fn with_debug_loss(mut self, pct: u32, seed: u64) -> Self {
        self.drop_pct = pct.min(100);
        self.drop_rng = seed | 1;
        self
    }

    /// Inject Gilbert-Elliott BURST loss: a two-state chain with per-10000
    /// transition probabilities `p` (Good->Bad) and `r` (Bad->Good), dropping
    /// every DATA datagram in the Bad state. Mean burst `10000 / r`, steady loss
    /// `p / (p + r)`. The same channel the block-RS receiver injects, so an
    /// adaptive-RLC-vs-static-block-RS A/B runs over an identical loss process.
    pub fn with_gilbert_loss(mut self, p_per_10k: u32, r_per_10k: u32, seed: u64) -> Self {
        self.ge_loss_p = p_per_10k;
        self.ge_loss_r = r_per_10k.max(1);
        self.drop_rng = seed | 1;
        self
    }

    /// Source symbols recovered by RLC without a retransmit (telemetry).
    pub fn rlc_recovered(&self) -> u64 {
        self.rlc_recovered
    }

    /// NAKs sent - the ARQ floor for losses the coding window could not cover.
    pub fn naks_sent(&self) -> u64 {
        self.naks_sent
    }

    /// FEEDBACK frames sent to the sender (telemetry).
    pub fn feedback_sent(&self) -> u64 {
        self.feedback_sent
    }

    /// The receiver's current measured channel assessment: `(loss, mean_burst,
    /// congestion_fraction)`. `loss` here is the STABLE lifetime loss rate (lost
    /// ids over delivered) for honest telemetry; the controller is fed the
    /// recency-weighted EWMA instead. Mean burst is `-1.0` before the
    /// Gilbert-Elliott fit converges.
    pub fn channel_estimate(&self) -> (f32, f32, f32) {
        let mean_burst = self.burst_model.mean_burst_len().map(|m| m as f32).unwrap_or(-1.0);
        let loss = if self.total_delivered == 0 {
            0.0
        } else {
            self.total_lost as f32 / self.total_delivered as f32
        };
        (loss, mean_burst, self.loss_class.congestion_fraction())
    }

    /// Bernoulli diagnostic drop (call-based): a fresh draw per new DATA arrival.
    fn roll_drop(&mut self) -> bool {
        if self.drop_pct == 0 {
            return false;
        }
        self.drop_rng = self
            .drop_rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.drop_rng >> 33) % 100) < self.drop_pct as u64
    }

    /// Gilbert-Elliott erasure decision for the FIRST transmission of `sid`. The
    /// two-state chain is advanced in source-id order and memoized, so the
    /// erasure pattern is a deterministic function of the id sequence and the
    /// seed - identical across codes - and a retransmit of an erased id (already
    /// dropped once) passes through so ARQ converges.
    fn ge_erase_first(&mut self, sid: u32) -> bool {
        while self.ge_pos <= sid {
            let bad = self.ge_bad;
            self.drop_rng = self
                .drop_rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let roll = ((self.drop_rng >> 33) as u32) % 10000;
            if self.ge_bad {
                if roll < self.ge_loss_r {
                    self.ge_bad = false;
                }
            } else if roll < self.ge_loss_p {
                self.ge_bad = true;
            }
            self.ge_decided.insert(self.ge_pos, bad);
            self.ge_pos += 1;
        }
        if *self.ge_decided.get(&sid).unwrap_or(&false) && !self.ge_dropped_once.contains(&sid) {
            self.ge_dropped_once.insert(sid);
            return true;
        }
        false
    }

    /// Re-base this receiver's delivery to start at `base` for a cross-code
    /// resync. The unified layer calls this when the stream returns to RLC after
    /// another code carried the ids in between: the in-order frontier moves to
    /// `base`, the decoder drops every stored symbol (the abandoned pre-`base`
    /// range), and the gap / NAK / loss-accounting tracking for ids below `base`
    /// is cleared so the receiver never NAKs a hole that will not be filled over
    /// RLC (the other code delivered those ids), nor replays a stale buffered tail.
    pub fn skip_to(&mut self, base: u32) {
        self.delivered_through = base;
        self.highest_seen = base;
        self.dec.rebase_to(base);
        self.gap_since.retain(|&sid, _| sid >= base);
        self.nakd.retain(|&sid| sid >= base);
        self.data_arrived.retain(|&sid| sid >= base);
        self.fec_recovered.retain(|&sid| sid >= base);
        self.loss_pending.retain(|&(sid, ..)| sid >= base);
    }

    /// Read whatever has arrived, recover and deliver in-order items, and NAK a
    /// stalled gap the coding window did not fill.
    pub fn poll(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        // Room for the inner DATA/REPAIR plus the AEAD envelope (type + pn + tag)
        // when TLS is on.
        let mut buf = vec![0u8; self.symbol_len + 64];
        let mut received = 0usize;
        // Drain pass: pull every available datagram, stamping its arrival time
        // and storing it, with NO Gaussian solve in the loop - so the arrival
        // stamp the congestion classifier reads reflects the network, not the
        // decode backlog. The single recovery pass runs after the drain.
        loop {
            match self.sock.recv_with_kts(&mut buf) {
                Ok((n, from, kts)) if n >= 1 => {
                    // Prefer the kernel RX timestamp (offset by the first one to
                    // keep the magnitude small); fall back to the drain-loop
                    // clock where it is unavailable.
                    let arrival_us = match kts {
                        Some(ns) => {
                            let base = *self.kts_base_ns.get_or_insert(ns);
                            (ns - base) as f64 / 1000.0
                        }
                        None => self.start.elapsed().as_micros() as f64,
                    };
                    // Open the AEAD envelope when TLS is on; the FEC sees the
                    // cleartext inner datagram. A non-sealed frame (a stray
                    // handshake retransmit) is skipped. The connection-id routing
                    // (which sets / migrates `self.peer`) runs on the cleartext.
                    #[cfg(feature = "tls")]
                    if self.crypto.is_some() {
                        let inner = self.crypto.as_ref().and_then(|c| secure_unwrap(c, &buf[..n]));
                        if let Some(inner) = inner
                            && self.route_and_process(&inner, from, arrival_us)
                        {
                            received += 1;
                        }
                        continue;
                    }
                    if self.route_and_process(&buf[..n], from, arrival_us) {
                        received += 1;
                    }
                }
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => break,
                // Windows surfaces an ICMP port-unreachable (e.g. a stale ack to
                // a peer that just migrated off its old socket) as a spurious
                // ConnectionReset on UDP recv - transient, not fatal.
                Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => break,
                Err(e) => return Err(e),
            }
        }
        // One recovery pass over the whole drained batch (repairs were stored,
        // not solved, during the drain).
        let recovered = self.dec.recover();
        for s in &recovered {
            self.fec_recovered.insert(*s);
        }
        self.rlc_recovered += recovered.len() as u64;
        let now_us = self.start.elapsed().as_micros() as f64;
        // Reorder grace (microseconds): how long to wait before declaring a gap
        // lost, scaled to the path's recent delay spread (jitter), floored 1ms.
        let grace = (2.0 * self.loss_class.recent_owd_spread_us()).clamp(1000.0, 50_000.0);
        self.deliver(&mut out, now_us, grace);
        self.flush_loss_accounting(now_us);
        // Path validation (Slice 4): retire a stale challenge (revert a spoofed
        // move) and (re)issue the outstanding one now that the drain has credited
        // the anti-amplification budget.
        self.expire_stale_challenge();
        self.maybe_send_challenge()?;
        self.maybe_nak()?;
        self.send_ack()?;
        self.maybe_feedback()?;
        // Idle backoff: the socket is non-blocking, so when nothing arrived and
        // nothing delivered, yield briefly instead of busy-spinning the caller.
        if received == 0 && out.is_empty() {
            std::thread::sleep(Duration::from_micros(100));
        }
        Ok(out)
    }

    /// Route an inner frame by its connection id (setting / migrating `peer`),
    /// then process it. Returns `false` for a frame whose connection id does not
    /// match this session (a foreign datagram), so it is not counted as received.
    fn route_and_process(&mut self, inner: &[u8], from: SocketAddr, arrival_us: f64) -> bool {
        // A PATH_RESPONSE answers an outstanding challenge: it is routed by id
        // and by the nonce, not delivered as data.
        if inner.first() == Some(&PKT_RLC_PATH_RESPONSE) {
            self.handle_path_response(inner, from);
            return true;
        }
        match frame_conn_id(inner) {
            Some(cid) => match self.session_cid {
                None => {
                    self.session_cid = Some(cid);
                    self.peer = Some(from);
                }
                Some(s) if s == cid => {
                    // Same session, new address -> the peer rebound. Migrate
                    // optimistically (keep delivering - the id / AEAD already
                    // authenticate the frame) but mark the address unvalidated
                    // and challenge it before trusting it for our own sends.
                    if self.peer != Some(from) {
                        self.prev_peer = self.peer;
                        self.peer = Some(from);
                        self.migrations += 1;
                        self.begin_path_validation(from);
                    }
                }
                Some(_) => return false,
            },
            None => {
                self.peer = Some(from);
            }
        }
        // Anti-amplification: count bytes received from an as-yet-unvalidated
        // peer, so our reply budget tracks what the address actually sent us.
        if !self.peer_validated && self.peer == Some(from) {
            self.unval_recv_bytes = self.unval_recv_bytes.saturating_add(inner.len() as u64);
        }
        self.process(inner, arrival_us);
        true
    }

    /// Begin validating a new candidate peer address: mark it unvalidated, reset
    /// the anti-amplification accounting, and arm an outstanding challenge with a
    /// fresh nonce. The challenge itself is emitted from [`poll`](Self::poll)
    /// (see [`maybe_send_challenge`](Self::maybe_send_challenge)) once the drain
    /// has credited the received bytes, so the very first challenge fits the cap.
    fn begin_path_validation(&mut self, addr: SocketAddr) {
        self.peer_validated = false;
        self.unval_recv_bytes = 0;
        self.unval_sent_bytes = 0;
        let nonce = self.next_challenge_nonce();
        self.pending_challenge = Some((addr, nonce, Instant::now()));
    }

    /// An unguessable challenge nonce. Seeded from the high-resolution monotonic
    /// clock (never on the wire) mixed through splitmix64, so an off-path
    /// attacker that can read the cleartext connection id still cannot predict
    /// the value it would have to echo.
    fn next_challenge_nonce(&mut self) -> u64 {
        self.challenge_seq = self.challenge_seq.wrapping_add(1);
        let entropy = self.start.elapsed().as_nanos() as u64;
        let mut x = entropy
            ^ self.challenge_seq.rotate_left(32)
            ^ self.session_cid.unwrap_or(0);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }

    /// (Re)send the outstanding PATH_CHALLENGE `[type][conn-id][nonce]`. Called
    /// once per poll while a challenge is pending, so a lost challenge is
    /// retransmitted; subject to the anti-amplification cap via
    /// [`wire_send_to_peer`](Self::wire_send_to_peer).
    fn maybe_send_challenge(&mut self) -> io::Result<()> {
        let Some((addr, nonce, _)) = self.pending_challenge else {
            return Ok(());
        };
        if self.peer != Some(addr) {
            return Ok(());
        }
        let mut pkt = Vec::with_capacity(PATH_FRAME_LEN);
        pkt.push(PKT_RLC_PATH_CHALLENGE);
        pkt.extend_from_slice(&self.session_cid.unwrap_or(0).to_le_bytes());
        pkt.extend_from_slice(&nonce.to_le_bytes());
        self.wire_send_to_peer(&pkt)
    }

    /// Validate an incoming PATH_RESPONSE: it must carry this session's id, come
    /// from the address under challenge, and echo the exact nonce. On a match the
    /// address is confirmed reachable and the anti-amplification cap is lifted.
    fn handle_path_response(&mut self, inner: &[u8], from: SocketAddr) {
        if inner.len() < PATH_FRAME_LEN {
            return;
        }
        let cid = u64::from_le_bytes(inner[1..9].try_into().unwrap());
        let nonce = u64::from_le_bytes(inner[9..17].try_into().unwrap());
        if self.session_cid != Some(cid) {
            return;
        }
        if let Some((addr, want, _)) = self.pending_challenge
            && addr == from
            && want == nonce
        {
            self.peer_validated = true;
            self.pending_challenge = None;
            self.prev_peer = None;
            self.path_validations += 1;
        }
    }

    /// Expire an unanswered challenge: if no PATH_RESPONSE arrived within
    /// [`CHALLENGE_TIMEOUT`], the move was spoofed (a genuine peer answers within
    /// a round trip), so revert to the previous address and lift the cap.
    fn expire_stale_challenge(&mut self) {
        if let Some((_, _, sent)) = self.pending_challenge
            && sent.elapsed() > CHALLENGE_TIMEOUT
        {
            if let Some(prev) = self.prev_peer {
                self.peer = Some(prev);
            }
            self.pending_challenge = None;
            self.prev_peer = None;
            self.peer_validated = true;
            self.path_validation_failures += 1;
        }
    }

    fn process(&mut self, pkt: &[u8], arrival_us: f64) {
        match pkt[0] {
            PKT_RLC_DATA if pkt.len() >= DATA_HDR + self.symbol_len => {
                let sid = u32::from_le_bytes([pkt[9], pkt[10], pkt[11], pkt[12]]);
                let send_us = u32::from_le_bytes([pkt[13], pkt[14], pkt[15], pkt[16]]);
                // Inject diagnostic loss. The Gilbert-Elliott path erases per the
                // deterministic id-indexed chain (retransmits pass); the Bernoulli
                // path draws per new arrival. Either way a retransmit always gets
                // through, so the ARQ floor converges.
                let erase = if self.ge_loss_r > 0 {
                    self.ge_erase_first(sid)
                } else {
                    !self.dec.has(sid) && self.roll_drop()
                };
                if erase {
                    return;
                }
                // The original DATA for this id arrived (possibly reordered, after
                // the FEC already recovered it): record it so the deferred loss
                // accounting does not count a reordered id as a loss.
                self.data_arrived.insert(sid);
                // Feed the congestion classifier with the drain-loop arrival
                // stamp: inter-arrival spacing (Biaz) and the relative one-way
                // trip time (Spike). A constant clock offset cancels in the Spike
                // min/max range.
                if let Some(prev) = self.last_arrival_us {
                    self.loss_class.observe_interarrival(arrival_us - prev);
                }
                self.loss_class.observe_owd(arrival_us - send_us as f64);
                // A forward jump past the highest seen reveals a gap whose width
                // is the consecutive-loss count Biaz / Spike classify.
                let prev_highest = self.highest_seen;
                if let Some(prev) = self.last_arrival_us
                    && sid > prev_highest + 1
                {
                    self.loss_class.classify(sid - prev_highest - 1, arrival_us - prev);
                }
                // Packet-pair dispersion: when this id is exactly one past the
                // last DATA, the gap to it is the bottleneck's per-packet
                // transmission time (the sender ships occasional back-to-back
                // pairs to surface the tight gaps). Keep the minimum - the
                // bottleneck imposes it regardless of how many OTHER ids drop,
                // so it measures capacity independently of loss.
                if self.last_data_sid == Some(sid.wrapping_sub(1))
                    && let Some(prev) = self.last_arrival_us
                {
                    let gap = arrival_us - prev;
                    // Keep only gaps above the NAPI-batch floor; the near-zero
                    // mass (same-poll arrivals) would poison a min/low-percentile
                    // read of the true bottleneck dispersion.
                    if gap >= PAIR_GAP_FLOOR_US {
                        if self.pair_ring.len() < PAIR_RING_CAP {
                            self.pair_ring.push(gap);
                        } else {
                            self.pair_ring[self.pair_ring_pos] = gap;
                            self.pair_ring_pos = (self.pair_ring_pos + 1) % PAIR_RING_CAP;
                        }
                    }
                    if self.pair_debug && gap > 0.0 {
                        self.pair_gap_log.push(gap);
                    }
                }
                self.last_data_sid = Some(sid);
                self.last_arrival_us = Some(arrival_us);
                self.highest_seen = self.highest_seen.max(sid);
                // Store only - the batched recovery pass after the drain solves.
                self.dec.on_source(sid, &pkt[DATA_HDR..DATA_HDR + self.symbol_len]);
            }
            PKT_RLC_REPAIR if pkt.len() >= 20 => {
                let repair_key = u32::from_le_bytes([pkt[9], pkt[10], pkt[11], pkt[12]]);
                let first_source_id = u32::from_le_bytes([pkt[13], pkt[14], pkt[15], pkt[16]]);
                let window_size = u16::from_le_bytes([pkt[17], pkt[18]]);
                let dt = pkt[19];
                let payload = pkt[20..].to_vec();
                if payload.len() != self.symbol_len {
                    return;
                }
                self.highest_seen = self
                    .highest_seen
                    .max(first_source_id.wrapping_add(window_size as u32).saturating_sub(1));
                // Store the repair only; the batched recovery pass solves.
                self.dec.add_repair(RepairSymbol {
                    repair_key,
                    first_source_id,
                    window_size,
                    dt,
                    payload,
                });
            }
            _ => {}
        }
    }

    fn deliver(&mut self, out: &mut Vec<Vec<u8>>, now_us: f64, grace: f64) {
        while let Some(sym) = self.dec.get(self.delivered_through) {
            out.push(unpack_symbol(sym));
            // Deliver the data immediately (no added latency), but DEFER the loss
            // accounting by the reorder grace: a FEC-recovered id whose original
            // arrives within the grace was reordered, not lost.
            let sid = self.delivered_through;
            let was_fec = self.fec_recovered.contains(&sid);
            let was_nak = self.nakd.contains(&sid);
            self.loss_pending.push_back((sid, now_us + grace, was_fec, was_nak));
            // The injector's per-id memo is only needed until the id is
            // delivered; prune it so a long-lived flow does not grow unbounded.
            self.ge_decided.remove(&sid);
            self.ge_dropped_once.remove(&sid);
            self.delivered_through = self.delivered_through.wrapping_add(1);
        }
        // Everything below the delivery frontier is done; free it.
        self.dec.forget_below(self.delivered_through);
    }

    /// Fold the deferred loss accounting for delivered ids whose reorder grace
    /// has elapsed, in delivery order. An id is lost when it had to be NAK'd, or
    /// the FEC recovered it AND its original DATA never arrived (a recovered id
    /// whose original later arrived was merely reordered).
    fn flush_loss_accounting(&mut self, now_us: f64) {
        while let Some(&(sid, account_at, was_fec, was_nak)) = self.loss_pending.front() {
            if account_at > now_us {
                break;
            }
            self.loss_pending.pop_front();
            let lost = was_nak || (was_fec && !self.data_arrived.contains(&sid));
            self.burst_model.observe(lost);
            // Smooth (1/128) so the loss the sender provisions the RATE against
            // tracks the SUSTAINED loss, not per-burst spikes (the burst length,
            // which the WINDOW provisions against, comes from the burst model).
            let x = if lost { 1.0 } else { 0.0 };
            self.loss_ewma += (x - self.loss_ewma) * (1.0 / 128.0);
            // Sliding-window loss counter: push this outcome, evict the oldest.
            self.loss_window.push_back(lost);
            if lost {
                self.loss_window_lost += 1;
            }
            if self.loss_window.len() > LOSS_WINDOW
                && let Some(true) = self.loss_window.pop_front()
            {
                self.loss_window_lost -= 1;
            }
            self.total_delivered += 1;
            if lost {
                self.total_lost += 1;
            }
            self.fec_recovered.remove(&sid);
            self.data_arrived.remove(&sid);
            self.nakd.remove(&sid);
        }
    }

    /// NAK the missing source ids still blocking delivery. Rate-limited (one
    /// round per interval) so a stalled gap does not flood the sender, but a
    /// whole batch of gaps is requested per round so ARQ recovers them in
    /// parallel rather than one per round trip. The interval gives the RLC
    /// window a chance to recover the gap first (FEC-primary, ARQ-fallback).
    fn maybe_nak(&mut self) -> io::Result<()> {
        // 1ms between NAK rounds: long enough that the FEC window gets many
        // repairs to recover a hole first (FEC-primary), short enough that a
        // hole the window cannot cover is retransmitted before the sender's
        // flow runway is exhausted waiting on it.
        if self.last_nak.elapsed() < Duration::from_millis(1) {
            return Ok(());
        }
        let now_us = self.start.elapsed().as_micros() as f64;
        // Reorder tolerance: a gap is only NAK'd after it has been missing for
        // this long, so a merely-reordered packet (which arrives within the
        // path's delay spread) is not declared lost and NAK'd as a false loss.
        // Scales with the recent ROTT spread (the jitter), floored at 1ms.
        let grace = (2.0 * self.loss_class.recent_owd_spread_us()).clamp(1000.0, 50_000.0);
        let mut missing = Vec::new();
        // Scan from the delivery frontier up to a bounded window: the lowest
        // gaps are the ones blocking delivery, and the window covers the reorder
        // span without an unbounded scan when delivery is deeply stalled.
        let scan_end = self.highest_seen.min(self.delivered_through.wrapping_add(1024));
        let mut sid = self.delivered_through;
        while sid <= scan_end {
            if self.dec.has(sid) {
                self.gap_since.remove(&sid);
            } else {
                let first = *self.gap_since.entry(sid).or_insert(now_us);
                if now_us - first >= grace && missing.len() < 16 {
                    missing.push(sid);
                }
            }
            sid = sid.wrapping_add(1);
        }
        // Drop gap records below the delivery frontier (delivered = not a gap).
        let dt = self.delivered_through;
        self.gap_since.retain(|&k, _| k >= dt);
        if missing.is_empty() {
            return Ok(());
        }
        if self.peer.is_some() {
            let mut pkt = Vec::with_capacity(1 + 4 * missing.len());
            pkt.push(PKT_RLC_NAK);
            for &m in &missing {
                pkt.extend_from_slice(&m.to_le_bytes());
                // A NAK'd id is a loss the coding window did not cover; record it
                // so the loss trace counts it even once the retransmit arrives.
                self.nakd.insert(m);
            }
            self.wire_send_to_peer(&pkt)?;
            self.naks_sent += 1;
            self.last_nak = Instant::now();
        }
        Ok(())
    }

    fn send_ack(&mut self) -> io::Result<()> {
        if self.peer.is_some() {
            // Cumulative in-order received frontier plus a 64-bit SACK bitmap of
            // ids received ABOVE the current hole (`delivered_through` is the
            // first missing id). The sender releases each SACK'd id from its
            // retransmit buffer, so a hole does not stall the outstanding window.
            let mut sack = 0u64;
            for i in 0..64u32 {
                if self.dec.has(self.delivered_through.wrapping_add(1 + i)) {
                    sack |= 1u64 << i;
                }
            }
            let mut pkt = Vec::with_capacity(13);
            pkt.push(PKT_RLC_ACK);
            pkt.extend_from_slice(&self.delivered_through.to_le_bytes());
            pkt.extend_from_slice(&sack.to_le_bytes());
            self.wire_send_to_peer(&pkt)?;
        }
        Ok(())
    }

    /// Periodically ship the fused channel assessment back to the sender so its
    /// controller can retune the coding. Rate-limited to one frame per interval;
    /// each signal is quantized to a byte (so a fully-decayed estimate rounds to
    /// an exact zero, the disable-on-clean trigger). Sent on every interval, not
    /// only under loss, so a clean spell is reported and coding eventually winds
    /// down.
    /// Windowed forward-loss rate the controller provisions FEC against. The
    /// MIN_FILL denominator suppresses a cold-start spike (a few early losses
    /// divide by the floor, not the tiny actual count).
    fn windowed_loss(&self) -> f32 {
        self.loss_window_lost as f32 / self.loss_window.len().max(LOSS_WINDOW_MIN_FILL) as f32
    }

    fn maybe_feedback(&mut self) -> io::Result<()> {
        if self.last_feedback.elapsed() < Duration::from_millis(10) || self.peer.is_none() {
            return Ok(());
        }
        // The loss the sender provisions the RATE against is a WINDOWED rate over
        // recent deliveries. The burst model's `p/(p+r)` reduces algebraically to
        // the CUMULATIVE `losses/n` - anchored by startup samples, it never
        // forgets, so an early loss cluster (or small-sample noise) read 44% loss
        // during a true-6% transfer and held FEC at its heaviest until the very
        // end. The windowed rate tracks the sustained loss quickly; the burst
        // model still supplies burstiness (mean burst) below.
        let loss = self.windowed_loss();
        let loss_q8 = (loss.clamp(0.0, 1.0) * 255.0).round() as u8;
        // burstiness is mean_burst / 16, the convention the snapshot and the RLC
        // window map both use; before the fit converges it reads 0 (isolated).
        let burstiness = self
            .burst_model
            .mean_burst_len()
            .map(|m| (m as f32 / 16.0).clamp(0.0, 1.0))
            .unwrap_or(0.0);
        let burst_q8 = (burstiness * 255.0).round() as u8;
        let cong_q8 = (self.loss_class.congestion_fraction().clamp(0.0, 1.0) * 255.0).round() as u8;
        // The receiver's REAL delivered (goodput) rate since the last feedback:
        // source symbols delivered in-order over the interval. This is the
        // ground-truth signal the sender's adaptive push rides - it plateaus at
        // the path capacity (unlike the binary loss signal), so it is safe to
        // probe against. Quantized to a u16 Mbit/s (0..65535).
        let interval_s = self.last_feedback.elapsed().as_secs_f64().max(1e-3);
        let delivered = self.delivered_through.wrapping_sub(self.last_fb_delivered) as f64;
        let rate_mbit =
            (delivered * self.symbol_len as f64 * 8.0 / interval_s / 1.0e6).clamp(0.0, 65535.0);
        let rate_q16 = (rate_mbit.round() as u16).to_le_bytes();
        self.last_fb_delivered = self.delivered_through;
        // Packet-pair CAPACITY estimate (Mbit/s): on-wire bytes / the tightest
        // consecutive-id gap. The bottleneck imposes that gap independently of
        // loss, so this is the one rate signal random loss cannot confound - the
        // sender cruises just under it. 0 until a pair has been seen. Decay the
        // gap upward slightly so the estimate tracks down if the path slows.
        let wire_bytes = (self.symbol_len + 70) as f64;
        let cap_mbit = if self.pair_ring.len() >= 32 {
            let mut s = self.pair_ring.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = (s.len() * PAIR_PERCENTILE_NUM / PAIR_PERCENTILE_DEN).min(s.len() - 1);
            let gap = s[idx].max(PAIR_GAP_FLOOR_US);
            (wire_bytes * 8.0 / gap).clamp(0.0, 65535.0)
        } else {
            0.0
        };
        let cap_q16 = (cap_mbit.round() as u16).to_le_bytes();
        // Diagnostic: dump the consecutive-id gap distribution every ~4k samples
        // so the true bottleneck dispersion can be separated from NAPI-batch
        // noise. Percentiles in microseconds; the implied capacity (Mbit/s) for
        // a few of them lets the right floor / estimator be chosen empirically.
        if self.pair_debug && self.pair_gap_log.len() >= 4000 {
            let mut g = std::mem::take(&mut self.pair_gap_log);
            g.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let n = g.len();
            let pc = |p: f64| g[((n as f64 * p) as usize).min(n - 1)];
            let cap = |us: f64| if us > 0.0 { wire_bytes * 8.0 / us } else { 0.0 };
            eprintln!(
                "PAIRGAP n={n} us[min={:.1} p1={:.1} p5={:.1} p10={:.1} p25={:.1} p50={:.1} p75={:.1}] \
                 cap_at_p25={:.0} EST_FED={cap_mbit:.0} Mbit (floored p25, ring={})",
                pc(0.0), pc(0.01), pc(0.05), pc(0.10), pc(0.25), pc(0.50), pc(0.75),
                cap(pc(0.25)), self.pair_ring.len(),
            );
        }
        let pkt = [
            PKT_RLC_FEEDBACK,
            loss_q8,
            burst_q8,
            cong_q8,
            rate_q16[0],
            rate_q16[1],
            cap_q16[0],
            cap_q16[1],
        ];
        self.wire_send_to_peer(&pkt)?;
        self.feedback_sent += 1;
        self.last_feedback = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// Telemetry a loopback round-trip returns: RLC recoveries and NAKs (the
    /// receiver's ARQ floor), plus the sender's adaptation count and feedback
    /// received.
    struct RoundTrip {
        recovered: u64,
        naks: u64,
        adapt_count: u64,
        feedback_recv: u64,
    }

    /// How loss is injected on the receiver: a flat Bernoulli percent, or a
    /// Gilbert-Elliott burst chain `(p, r)` per-10000.
    #[derive(Clone, Copy)]
    enum Loss {
        Bernoulli(u32),
        Gilbert(u32, u32),
    }

    /// Real loopback sockets, real UDP datagrams, with `loss` injected on the
    /// receiver. When `static_params`, the sender is pinned at its initial code
    /// (the static baseline); otherwise the sensing feedback adapts it.
    /// Delivery must be exact and in order regardless.
    fn run_loopback(n: u64, loss: Loss, seed: u64, static_params: bool) -> RoundTrip {
        let item_len = 32usize;
        let symbol_len = 64usize;
        let (addr_tx, addr_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let rx = std::thread::spawn(move || {
            let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", symbol_len).unwrap();
            recv = match loss {
                Loss::Bernoulli(pct) => recv.with_debug_loss(pct, seed),
                Loss::Gilbert(p, r) => recv.with_gilbert_loss(p, r, seed),
            };
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let mut got: Vec<u64> = Vec::new();
            let start = Instant::now();
            while (got.len() as u64) < n {
                if start.elapsed() > Duration::from_secs(20) {
                    break;
                }
                for item in recv.poll().unwrap() {
                    got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
                }
            }
            for _ in 0..50 {
                recv.poll().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            done_tx.send(()).ok();
            (got, recv.rlc_recovered(), recv.naks_sent())
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send =
                SensOMaticRlcSender::bind("127.0.0.1:0", recv_addr, 16, 2, 15, symbol_len).unwrap();
            if static_params {
                send = send.with_static_params();
            }
            for i in 0..n {
                let mut item = vec![0u8; item_len];
                item[..8].copy_from_slice(&i.to_le_bytes());
                send.send_item(&item).unwrap();
            }
            send.drain_until_acked(n as u32, Duration::from_secs(15)).unwrap();
            done_rx.recv_timeout(Duration::from_secs(20)).ok();
            (send.adapt_count(), send.feedback_recv())
        });

        let (got, recovered, naks) = rx.join().unwrap();
        let (adapt_count, feedback_recv) = tx.join().unwrap();
        let expected: Vec<u64> = (0..n).collect();
        assert_eq!(got, expected, "RLC transport must deliver every item in order");
        RoundTrip { recovered, naks, adapt_count, feedback_recv }
    }

    #[test]
    fn loopback_clean() {
        // Exact in-order delivery on a clean link is asserted inside the
        // harness (got == expected). `recovered` / `naks` are telemetry only:
        // even a clean loopback drops a few datagrams under the send burst
        // (kernel socket-buffer pressure), which the RLC + ARQ floor absorbs,
        // so neither is asserted to be zero.
        run_loopback(500, Loss::Bernoulli(0), 1, false);
    }

    #[test]
    fn loopback_isolated_loss_recovers_via_rlc() {
        // ~6% isolated-ish loss: the dense window recovers most without ARQ.
        let rt = run_loopback(800, Loss::Bernoulli(6), 7, false);
        assert!(rt.recovered > 0, "RLC must recover losses without a retransmit");
    }

    #[test]
    fn loopback_heavy_loss_arq_floor() {
        // Pinned at the static initial code (window 16), a Gilbert-Elliott
        // channel with mean burst 25 (r=400 -> 10000/400) exceeds the window, so
        // the longest bursts CANNOT be FEC-recovered and must fall to the ARQ
        // floor. Deterministic erasure passes retransmits, so ARQ converges and
        // delivery is exact (asserted inside the harness).
        let rt = run_loopback(600, Loss::Gilbert(100, 400), 1234, true);
        assert!(rt.naks > 0, "bursts beyond the static window must hit the ARQ floor");
    }

    #[test]
    fn loopback_adaptive_feedback_retunes_the_code() {
        // The sensing-driven half end to end over real sockets: under loss
        // heavier than the initial code provisions, the receiver fits the
        // channel, feeds it back, and the controller escalates the live code at
        // once (25% loss -> step 1, tighter than the initial step 2), so the
        // retune is deterministic regardless of how fast the stream completes.
        let rt = run_loopback(1500, Loss::Bernoulli(25), 99, false);
        assert!(rt.feedback_recv > 0, "feedback must reach the adaptive sender");
        assert!(
            rt.adapt_count > 0,
            "loss heavier than the initial code must retune it at least once",
        );
    }

    #[test]
    fn loopback_gilbert_burst_delivers_exactly() {
        // A Gilbert-Elliott burst channel (mean burst 10000/250 = 40, steady
        // loss 80/(80+250) ~= 24%): adaptive coding plus the ARQ floor still
        // deliver every item in order (asserted inside the harness).
        let rt = run_loopback(800, Loss::Gilbert(80, 250), 2024, false);
        // A bursty channel this heavy needs the ARQ floor for the longest bursts.
        assert!(rt.naks > 0, "long bursts beyond the window must hit the ARQ floor");
    }

    /// The client rebinds its socket mid-stream (a NAT rebinding / interface
    /// switch); the receiver follows the connection id to the new address and
    /// still delivers every item in order.
    #[test]
    fn loopback_connection_survives_migration() {
        let (item_len, symbol_len, n) = (32usize, 64usize, 2000u64);
        let (addr_tx, addr_rx) = mpsc::channel();

        let rx = std::thread::spawn(move || {
            let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", symbol_len).unwrap();
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let mut got: Vec<u64> = Vec::new();
            let start = Instant::now();
            while (got.len() as u64) < n {
                if start.elapsed() > Duration::from_secs(20) {
                    break;
                }
                for item in recv.poll().unwrap() {
                    got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
                }
            }
            for _ in 0..50 {
                recv.poll().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            (got, recv.migrations())
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send =
                SensOMaticRlcSender::bind("127.0.0.1:0", recv_addr, 16, 2, 15, symbol_len).unwrap();
            for i in 0..n {
                // Rebind to a new local port halfway through - the receiver must
                // follow the connection id, not the 4-tuple.
                if i == n / 2 {
                    send.migrate().unwrap();
                }
                let mut item = vec![0u8; item_len];
                item[..8].copy_from_slice(&i.to_le_bytes());
                send.send_item(&item).unwrap();
            }
            send.drain_until_acked(n as u32, Duration::from_secs(15)).unwrap();
        });

        let (got, migrations) = rx.join().unwrap();
        tx.join().unwrap();
        assert_eq!(
            got,
            (0..n).collect::<Vec<_>>(),
            "every item must be delivered across the migration"
        );
        assert!(
            migrations >= 1,
            "the receiver must have followed the connection to the new address"
        );
    }

    /// Slice 4: an OS path event (item 12) drives a PROACTIVE migration, and the
    /// receiver validates the new address by challenge / response before trusting
    /// it - every item still delivered, the move validated, no revert.
    #[test]
    fn loopback_proactive_migration_validates_the_new_path() {
        let (item_len, symbol_len, n) = (32usize, 64usize, 2000u64);
        let (addr_tx, addr_rx) = mpsc::channel();

        let rx = std::thread::spawn(move || {
            let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", symbol_len).unwrap();
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let mut got: Vec<u64> = Vec::new();
            let start = Instant::now();
            while (got.len() as u64) < n {
                if start.elapsed() > Duration::from_secs(20) {
                    break;
                }
                for item in recv.poll().unwrap() {
                    got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
                }
            }
            for _ in 0..50 {
                recv.poll().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            (
                got,
                recv.migrations(),
                recv.path_validations(),
                recv.path_validation_failures(),
            )
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send = SensOMaticRlcSender::bind("127.0.0.1:0", recv_addr, 16, 2, 15, symbol_len)
                .unwrap()
                .with_path_observer(None);
            for i in 0..n {
                // Mid-stream, synthesize an OS path event; the next send migrates
                // proactively and the receiver pre-validates the new address.
                if i == n / 2 {
                    send.inject_path_event();
                }
                let mut item = vec![0u8; item_len];
                item[..8].copy_from_slice(&i.to_le_bytes());
                send.send_item(&item).unwrap();
            }
            send.drain_until_acked(n as u32, Duration::from_secs(15)).unwrap();
            send.proactive_migrations()
        });

        let (got, migrations, validations, failures) = rx.join().unwrap();
        let proactive = tx.join().unwrap();
        assert_eq!(
            got,
            (0..n).collect::<Vec<_>>(),
            "every item delivered across the proactive migration"
        );
        assert!(migrations >= 1, "the receiver followed the connection to the new address");
        assert!(validations >= 1, "the new path was validated by challenge / response");
        assert_eq!(failures, 0, "a genuine migration must not fail validation");
        assert!(proactive >= 1, "the path event must have driven a proactive migration");
    }

    /// Slice 4 security property: a forged DATA frame from an unrelated address
    /// (correct connection id, but an address that cannot answer the challenge)
    /// must NOT permanently hijack the session - the receiver challenges the new
    /// address, gets no response, and reverts to the real peer, which keeps
    /// delivering.
    #[test]
    fn spoofed_move_fails_validation_and_reverts() {
        let symbol_len = 64usize;
        let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", symbol_len).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let mut send = SensOMaticRlcSender::bind("127.0.0.1:0", recv_addr, 16, 2, 15, symbol_len).unwrap();
        let real_peer = send.local_addr().unwrap();
        let cid = send.conn_id();

        // Deliver a few items so the receiver is bound to the real peer.
        for i in 0u64..20 {
            let mut item = vec![0u8; 16];
            item[..8].copy_from_slice(&i.to_le_bytes());
            send.send_item(&item).unwrap();
        }
        let start = Instant::now();
        let mut got = 0u64;
        while got < 20 && start.elapsed() < Duration::from_secs(5) {
            got += recv.poll().unwrap().len() as u64;
        }
        assert_eq!(recv.peer(), Some(real_peer), "bound to the real peer first");

        // An attacker socket forges a DATA frame with the right connection id.
        let attacker = UdpSocket::bind("127.0.0.1:0").unwrap();
        attacker.set_nonblocking(true).unwrap();
        let attacker_addr = attacker.local_addr().unwrap();
        let mut forged = Vec::new();
        forged.push(PKT_RLC_DATA);
        forged.extend_from_slice(&cid.to_le_bytes());
        forged.extend_from_slice(&9999u32.to_le_bytes()); // some source id
        forged.extend_from_slice(&0u32.to_le_bytes()); // send-ts
        forged.extend_from_slice(&vec![0u8; symbol_len]);
        attacker.send_to(&forged, recv_addr).unwrap();

        // The receiver migrates optimistically to the attacker address and
        // challenges it. The attacker never answers (it drains and ignores any
        // challenge), so after the timeout the receiver must revert.
        let start = Instant::now();
        while start.elapsed() < CHALLENGE_TIMEOUT + Duration::from_millis(300) {
            recv.poll().ok();
            // Drain the attacker socket so its buffer does not fill; never reply.
            let mut b = [0u8; 256];
            while attacker.recv_from(&mut b).is_ok() {}
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            recv.path_validations() == 0,
            "the spoofed address must never validate"
        );
        assert!(
            recv.path_validation_failures() >= 1,
            "the unanswered challenge must time out as a failure"
        );
        assert_ne!(
            recv.peer(),
            Some(attacker_addr),
            "the receiver must not be left pointing at the spoofed address"
        );
        assert_eq!(recv.peer(), Some(real_peer), "it reverts to the real peer");
    }

    /// Real loopback sockets with the TLS record layer on: the client and server
    /// run the TLS 1.3 handshake over the transport, then every data datagram is
    /// AEAD-sealed. Delivery must be exact and in order through the encryption.
    #[cfg(feature = "tls")]
    #[test]
    fn loopback_tls_handshake_and_encrypted_delivery() {
        use crate::rlc_crypto;
        let (cert, key) = rlc_crypto::self_signed_cert().expect("cert");
        let scfg = rlc_crypto::server_config(&cert, &key).expect("server cfg");
        let ccfg = rlc_crypto::client_config(&cert).expect("client cfg");
        let (item_len, symbol_len, n) = (32usize, 64usize, 500u64);
        let (addr_tx, addr_rx) = mpsc::channel();

        let rx = std::thread::spawn(move || {
            let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", symbol_len)
                .unwrap()
                .with_tls_server(scfg)
                .unwrap();
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            recv.handshake().expect("server handshake");
            let mut got: Vec<u64> = Vec::new();
            let start = Instant::now();
            while (got.len() as u64) < n {
                if start.elapsed() > Duration::from_secs(20) {
                    break;
                }
                for item in recv.poll().unwrap() {
                    got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
                }
            }
            for _ in 0..50 {
                recv.poll().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            got
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send = SensOMaticRlcSender::bind("127.0.0.1:0", recv_addr, 16, 2, 15, symbol_len)
                .unwrap()
                .with_tls_client(ccfg)
                .unwrap();
            send.handshake().expect("client handshake");
            for i in 0..n {
                let mut item = vec![0u8; item_len];
                item[..8].copy_from_slice(&i.to_le_bytes());
                send.send_item(&item).unwrap();
            }
            send.drain_until_acked(n as u32, Duration::from_secs(15)).unwrap();
        });

        let got = rx.join().unwrap();
        tx.join().unwrap();
        assert_eq!(
            got,
            (0..n).collect::<Vec<_>>(),
            "TLS transport must deliver every item in order through the encryption"
        );
    }
}
