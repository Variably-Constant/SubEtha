//! Slice 5: stream multiplexing over one connection.
//!
//! A [`StreamMuxSender`] / [`StreamMuxReceiver`] pair carries many independent
//! byte streams over a single UDP socket and a single connection id. Each stream
//! owns its own symbol space and is reassembled independently, so a loss on one
//! stream never blocks delivery on another - the cross-stream head-of-line
//! blocking that a single ordered transport suffers is gone by construction.
//!
//! Reliability is two-layered and **selective per stream**:
//!
//!  - A [`Protection::Protected`] stream rides the sliding-window RLC code (the
//!    same [`crate::rlc_fec`] engine the single-stream transport uses), so most
//!    losses are repaired forward with no round trip - the right choice for a
//!    latency-critical stream.
//!  - A [`Protection::Bulk`] stream carries no repairs; it relies on ARQ alone
//!    (the receiver NAKs missing symbols), the throughput-efficient choice for
//!    background data where a round trip of recovery latency is fine.
//!
//! [`recommend_protection`] picks between them from the measured loss, so the
//! sensing plane drives the per-stream scheme just as it drives the single-stream
//! coding parameters.
//!
//! Flow control is two-level: a per-stream window caps each stream's outstanding
//! (sent-but-unacked) symbols so one stream cannot starve the others, and a
//! connection window caps the total across all streams so the aggregate cannot
//! overrun the receiver.
//!
//! Framing keeps every non-final symbol exactly one `symbol_len`, so the FEC
//! always codes over equal-size symbols and a forward-repaired middle symbol is
//! reassembled verbatim. A stream's final (possibly short) symbol and its byte
//! length ride a reliable `STREAM_FIN` frame, decoupled from the data symbol's
//! recovery, so a lost-then-repaired tail never loses its length or its fin.
//!
//! The connection id keeps every frame routable, so this layer composes with the
//! connection-level concerns of the single-stream transport (the same id model
//! the migration of Slices 3 / 4 routes on); the multiplexing here is orthogonal
//! to that connection layer and leaves it untouched.

use crate::rlc_fec::{RepairSymbol, RlcDecoder, RlcEncoder};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

const STREAM: u8 = 20;
const STREAM_REPAIR: u8 = 21;
const STREAM_NAK: u8 = 22;
const STREAM_ACK: u8 = 23;
const STREAM_FIN: u8 = 24;

/// STREAM header: type + connection id (u64) + stream id (u32) + source id (u32).
/// A fixed `symbol_len` symbol follows.
const STREAM_HDR: usize = 1 + 8 + 4 + 4;
/// STREAM_REPAIR header: type + connection id + stream id + repair key + first
/// source id + window size (u16) + density threshold (u8). One symbol follows.
const REPAIR_HDR: usize = 1 + 8 + 4 + 4 + 4 + 2 + 1;
/// STREAM_FIN: type + connection id + stream id + final source id + final length.
const FIN_LEN: usize = 1 + 8 + 4 + 4 + 2;
/// STREAM_ACK: type + connection id + stream id + cumulative delivered frontier.
const ACK_LEN: usize = 1 + 8 + 4 + 4;

/// Per-stream forward-error-correction policy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Protection {
    /// Sliding-window RLC repairs ride alongside the stream - losses are repaired
    /// forward with no round trip (latency-critical streams).
    Protected,
    /// No repairs; ARQ alone recovers losses (throughput streams).
    Bulk,
}

/// Choose a per-stream protection from the measured loss rate: above ~1% the ARQ
/// round trip a Bulk stream pays per loss is worth the forward repair overhead.
pub fn recommend_protection(loss: f32) -> Protection {
    if loss > 0.01 {
        Protection::Protected
    } else {
        Protection::Bulk
    }
}

/// Bytes delivered from one stream by a [`StreamMuxReceiver::poll`].
#[derive(Clone, Debug)]
pub struct StreamData {
    /// The stream the bytes belong to.
    pub stream_id: u32,
    /// Contiguous bytes delivered in order (may be empty when only `fin` lands).
    pub data: Vec<u8>,
    /// Whether this delivery completes the stream.
    pub fin: bool,
}

/// Derive a per-connection id from the wall clock and local port (the same shape
/// the single-stream transport uses, so the two are interchangeable on the wire).
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

fn set_buffers(sock: &UdpSocket) {
    let s = socket2::SockRef::from(sock);
    s.set_recv_buffer_size(4 * 1024 * 1024).ok();
    s.set_send_buffer_size(4 * 1024 * 1024).ok();
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

struct SendStream {
    protection: Protection,
    enc: Option<RlcEncoder>,
    /// Unacked full symbols held for ARQ, keyed by source id.
    sent: BTreeMap<u32, Vec<u8>>,
    next_source_id: u32,
    /// Highest contiguous source id the receiver has delivered (per-stream ack).
    acked_through: u32,
    /// Bytes buffered but not yet a full symbol (only the final symbol is short).
    pending: Vec<u8>,
    /// Once finished: the final source id and the final symbol's real length.
    fin_info: Option<(u32, u16)>,
}

impl SendStream {
    fn outstanding(&self) -> u32 {
        self.sent.len() as u32
    }
    fn fully_acked(&self) -> bool {
        match self.fin_info {
            Some((final_sid, _)) => self.acked_through > final_sid,
            None => false,
        }
    }
}

/// Sender side of the stream multiplexer.
pub struct StreamMuxSender {
    sock: UdpSocket,
    peer: SocketAddr,
    cid: u64,
    symbol_len: usize,
    streams: HashMap<u32, SendStream>,
    /// Cap on total outstanding symbols across all streams.
    conn_window: u32,
    /// Cap on outstanding symbols per stream.
    per_stream_window: u32,
    /// RLC parameters applied to a Protected stream's encoder.
    rlc_window: usize,
    rlc_step: usize,
    rlc_dt: u8,
    /// Last time finished-but-unacked STREAM_FINs were resent (rate limit).
    last_fin_resend: Instant,
}

impl StreamMuxSender {
    /// Bind a local socket and connect to `peer`, coding over `symbol_len`-byte
    /// symbols. `conn_window` caps total outstanding symbols; `per_stream_window`
    /// caps each stream's share.
    pub fn bind<A: ToSocketAddrs>(
        local: A,
        peer: SocketAddr,
        symbol_len: usize,
        conn_window: u32,
        per_stream_window: u32,
    ) -> io::Result<Self> {
        let sock = UdpSocket::bind(local)?;
        sock.set_nonblocking(true)?;
        set_buffers(&sock);
        let cid = derive_conn_id(sock.local_addr().map(|a| a.port()).unwrap_or(0));
        Ok(Self {
            sock,
            peer,
            cid,
            symbol_len,
            streams: HashMap::new(),
            conn_window: conn_window.max(1),
            per_stream_window: per_stream_window.max(1),
            rlc_window: 32,
            rlc_step: 4,
            rlc_dt: 15,
            last_fin_resend: Instant::now(),
        })
    }

    /// The connection id stamped into every frame.
    pub fn conn_id(&self) -> u64 {
        self.cid
    }

    /// The FEC policy a stream was opened with, if it exists.
    pub fn stream_protection(&self, stream_id: u32) -> Option<Protection> {
        self.streams.get(&stream_id).map(|s| s.protection)
    }

    /// The sender's local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Open a stream with the given FEC policy. Re-opening keeps the existing
    /// stream (the policy is fixed at first open).
    pub fn open_stream(&mut self, stream_id: u32, protection: Protection) {
        let (w, s, d) = (self.rlc_window, self.rlc_step, self.rlc_dt);
        let symbol_len = self.symbol_len;
        self.streams.entry(stream_id).or_insert_with(|| SendStream {
            protection,
            enc: match protection {
                Protection::Protected => Some(RlcEncoder::new(w, s, d, symbol_len)),
                Protection::Bulk => None,
            },
            sent: BTreeMap::new(),
            next_source_id: 0,
            acked_through: 0,
            pending: Vec::new(),
            fin_info: None,
        });
    }

    fn total_outstanding(&self) -> u32 {
        self.streams.values().map(|s| s.outstanding()).sum()
    }

    /// Write `data` on `stream_id`. Full symbols are emitted immediately; a
    /// partial tail is buffered until the next write or `fin`. With `fin` set the
    /// stream is closed: the buffered tail goes out as the final (short) symbol
    /// and a reliable STREAM_FIN announces its index and length.
    pub fn write(&mut self, stream_id: u32, data: &[u8], fin: bool) -> io::Result<()> {
        if !self.streams.contains_key(&stream_id) {
            self.open_stream(stream_id, Protection::Bulk);
        }
        let chunk = self.symbol_len;
        // Append to the per-stream pending buffer, then drain full symbols.
        self.streams.get_mut(&stream_id).unwrap().pending.extend_from_slice(data);
        loop {
            let have = self.streams.get(&stream_id).unwrap().pending.len();
            if have < chunk {
                break;
            }
            let sym: Vec<u8> = self
                .streams
                .get_mut(&stream_id)
                .unwrap()
                .pending
                .drain(..chunk)
                .collect();
            self.send_symbol(stream_id, &sym)?;
        }
        if fin {
            // Flush the (possibly empty, possibly short) tail as the final symbol.
            let tail: Vec<u8> = std::mem::take(&mut self.streams.get_mut(&stream_id).unwrap().pending);
            let final_len = tail.len() as u16;
            let final_sid = self.streams.get(&stream_id).unwrap().next_source_id;
            self.send_symbol(stream_id, &tail)?;
            self.streams.get_mut(&stream_id).unwrap().fin_info = Some((final_sid, final_len));
            self.wire_fin(stream_id, final_sid, final_len)?;
        }
        Ok(())
    }

    /// Emit one symbol (padded to `symbol_len`), pacing against both flow windows.
    fn send_symbol(&mut self, stream_id: u32, payload: &[u8]) -> io::Result<()> {
        let start = Instant::now();
        loop {
            self.pump()?;
            let per_stream_ok = self
                .streams
                .get(&stream_id)
                .map(|s| s.outstanding() < self.per_stream_window)
                .unwrap_or(true);
            let conn_ok = self.total_outstanding() < self.conn_window;
            if per_stream_ok && conn_ok {
                break;
            }
            if start.elapsed() > Duration::from_secs(60) {
                break;
            }
            std::thread::sleep(Duration::from_micros(50));
        }

        let mut sym = vec![0u8; self.symbol_len];
        sym[..payload.len()].copy_from_slice(payload);

        let stream = self.streams.get_mut(&stream_id).unwrap();
        let sid = stream.next_source_id;
        stream.next_source_id += 1;
        stream.sent.insert(sid, sym.clone());
        let repair = stream.enc.as_mut().and_then(|e| e.push_source(&sym).1);

        self.wire_stream(stream_id, sid, &sym)?;
        if let Some(r) = repair {
            self.wire_repair(stream_id, &r)?;
        }
        Ok(())
    }

    fn wire_stream(&self, stream_id: u32, sid: u32, sym: &[u8]) -> io::Result<()> {
        let mut pkt = Vec::with_capacity(STREAM_HDR + sym.len());
        pkt.push(STREAM);
        pkt.extend_from_slice(&self.cid.to_le_bytes());
        pkt.extend_from_slice(&stream_id.to_le_bytes());
        pkt.extend_from_slice(&sid.to_le_bytes());
        pkt.extend_from_slice(sym);
        self.sock.send_to(&pkt, self.peer)?;
        Ok(())
    }

    fn wire_fin(&self, stream_id: u32, final_sid: u32, final_len: u16) -> io::Result<()> {
        let mut pkt = Vec::with_capacity(FIN_LEN);
        pkt.push(STREAM_FIN);
        pkt.extend_from_slice(&self.cid.to_le_bytes());
        pkt.extend_from_slice(&stream_id.to_le_bytes());
        pkt.extend_from_slice(&final_sid.to_le_bytes());
        pkt.extend_from_slice(&final_len.to_le_bytes());
        self.sock.send_to(&pkt, self.peer)?;
        Ok(())
    }

    fn wire_repair(&self, stream_id: u32, r: &RepairSymbol) -> io::Result<()> {
        let mut pkt = Vec::with_capacity(REPAIR_HDR + r.payload.len());
        pkt.push(STREAM_REPAIR);
        pkt.extend_from_slice(&self.cid.to_le_bytes());
        pkt.extend_from_slice(&stream_id.to_le_bytes());
        pkt.extend_from_slice(&r.repair_key.to_le_bytes());
        pkt.extend_from_slice(&r.first_source_id.to_le_bytes());
        pkt.extend_from_slice(&r.window_size.to_le_bytes());
        pkt.push(r.dt);
        pkt.extend_from_slice(&r.payload);
        self.sock.send_to(&pkt, self.peer)?;
        Ok(())
    }

    /// Drain incoming NAK / ACK frames: retransmit NAK'd symbols and advance each
    /// stream's acked frontier (which frees the flow windows). Also resends the
    /// STREAM_FIN of any finished-but-unacked stream on a slow cadence, so a
    /// terminator lost while other streams are still being written still
    /// converges (not only once the final `flush` begins).
    pub fn pump(&mut self) -> io::Result<()> {
        let mut buf = vec![0u8; self.symbol_len + STREAM_HDR + 64];
        loop {
            match self.sock.recv_from(&mut buf) {
                Ok((n, _)) if n >= 1 => self.handle_feedback(&buf[..n])?,
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => break,
                Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => break,
                Err(e) => return Err(e),
            }
        }
        if self.last_fin_resend.elapsed() >= Duration::from_millis(5) {
            let pending: Vec<(u32, u32, u16)> = self
                .streams
                .iter()
                .filter(|(_, s)| s.fin_info.is_some() && !s.fully_acked())
                .map(|(&id, s)| {
                    let (fs, fl) = s.fin_info.unwrap();
                    (id, fs, fl)
                })
                .collect();
            for (id, fs, fl) in pending {
                self.wire_fin(id, fs, fl)?;
            }
            self.last_fin_resend = Instant::now();
        }
        Ok(())
    }

    fn handle_feedback(&mut self, m: &[u8]) -> io::Result<()> {
        if m.len() < 13 {
            return Ok(());
        }
        let cid = u64::from_le_bytes(m[1..9].try_into().unwrap());
        if cid != self.cid {
            return Ok(());
        }
        let stream_id = u32::from_le_bytes(m[9..13].try_into().unwrap());
        match m[0] {
            STREAM_NAK => {
                let mut off = 13;
                let mut want = Vec::new();
                while off + 4 <= m.len() {
                    want.push(u32::from_le_bytes(m[off..off + 4].try_into().unwrap()));
                    off += 4;
                }
                let mut resend_fin = false;
                for sid in want {
                    let frame = self
                        .streams
                        .get(&stream_id)
                        .and_then(|s| s.sent.get(&sid))
                        .cloned();
                    if let Some(sym) = frame {
                        self.wire_stream(stream_id, sid, &sym)?;
                    }
                    // A NAK at or past the final symbol may mean the FIN was lost.
                    if let Some((final_sid, _)) =
                        self.streams.get(&stream_id).and_then(|s| s.fin_info)
                        && sid >= final_sid
                    {
                        resend_fin = true;
                    }
                }
                if resend_fin
                    && let Some((final_sid, final_len)) =
                        self.streams.get(&stream_id).and_then(|s| s.fin_info)
                {
                    self.wire_fin(stream_id, final_sid, final_len)?;
                }
            }
            STREAM_ACK if m.len() >= ACK_LEN => {
                let through = u32::from_le_bytes(m[13..17].try_into().unwrap());
                if let Some(s) = self.streams.get_mut(&stream_id)
                    && through > s.acked_through
                {
                    s.acked_through = through;
                    s.sent.retain(|&sid, _| sid >= through);
                    if let Some(e) = s.enc.as_mut() {
                        e.forget_below(through);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Block until every stream is fully acked (all flow windows empty and each
    /// finished stream's frontier past its final symbol) or the timeout elapses.
    /// Resends each finished stream's STREAM_FIN so a lost terminator converges.
    pub fn flush(&mut self, timeout: Duration) -> io::Result<bool> {
        let start = Instant::now();
        loop {
            self.pump()?;
            let pending: Vec<(u32, u32, u16)> = self
                .streams
                .iter()
                .filter(|(_, s)| !s.fully_acked() && s.fin_info.is_some())
                .map(|(&id, s)| {
                    let (fs, fl) = s.fin_info.unwrap();
                    (id, fs, fl)
                })
                .collect();
            let done = self.total_outstanding() == 0
                && self.streams.values().all(|s| s.fin_info.is_none() || s.fully_acked());
            if done || start.elapsed() >= timeout {
                return Ok(done);
            }
            for (id, fs, fl) in pending {
                self.wire_fin(id, fs, fl)?;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

struct RecvStream {
    protection: Protection,
    dec: Option<RlcDecoder>,
    /// Reassembly buffer: source id -> full symbol bytes.
    chunks: BTreeMap<u32, Vec<u8>>,
    /// Next source id to deliver (everything below is delivered).
    delivered_through: u32,
    /// First-seen time of each gap, for NAK timing.
    gap_since: BTreeMap<u32, Instant>,
    highest_seen: u32,
    /// Once the FIN is known: the final source id and the final symbol's length.
    final_info: Option<(u32, u16)>,
    fin_delivered: bool,
}

/// Receiver side of the stream multiplexer.
pub struct StreamMuxReceiver {
    sock: UdpSocket,
    peer: Option<SocketAddr>,
    cid: Option<u64>,
    symbol_len: usize,
    streams: HashMap<u32, RecvStream>,
    last_nak: Instant,
    /// Diagnostic per-stream loss injection (seeded by source id).
    debug_loss: HashMap<u32, (u32, u64)>,
    /// `(stream, source id)` pairs already dropped once, so injected loss erases
    /// only the first transmission of a symbol (a recoverable loss); the
    /// retransmit / a later copy passes, as a real one-off loss would.
    dropped_once: HashSet<(u32, u32)>,
    /// Total symbols recovered by FEC across all streams (telemetry).
    fec_recovered: u64,
    naks_sent: u64,
}

impl StreamMuxReceiver {
    /// Bind a receiver over `symbol_len`-byte symbols.
    pub fn bind<A: ToSocketAddrs>(local: A, symbol_len: usize) -> io::Result<Self> {
        let sock = UdpSocket::bind(local)?;
        sock.set_nonblocking(true)?;
        set_buffers(&sock);
        Ok(Self {
            sock,
            peer: None,
            cid: None,
            symbol_len,
            streams: HashMap::new(),
            last_nak: Instant::now(),
            debug_loss: HashMap::new(),
            dropped_once: HashSet::new(),
            fec_recovered: 0,
            naks_sent: 0,
        })
    }

    /// The bound local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Total symbols recovered by FEC (no retransmit) across all streams.
    pub fn fec_recovered(&self) -> u64 {
        self.fec_recovered
    }

    /// NAK frames sent across all streams.
    pub fn naks_sent(&self) -> u64 {
        self.naks_sent
    }

    /// The FEC policy a stream is being received with, if it exists.
    pub fn stream_protection(&self, stream_id: u32) -> Option<Protection> {
        self.streams.get(&stream_id).map(|s| s.protection)
    }

    /// Declare a stream's FEC policy on the receive side (so a Protected stream
    /// runs an RLC decoder). A stream the sender opens before this is seen
    /// defaults to Bulk.
    pub fn expect_stream(&mut self, stream_id: u32, protection: Protection) {
        self.stream_entry(stream_id, protection);
    }

    /// Inject seeded diagnostic loss on a stream (percent of arriving symbols),
    /// to exercise recovery on loopback.
    pub fn with_stream_loss(mut self, stream_id: u32, pct: u32, seed: u64) -> Self {
        self.debug_loss.insert(stream_id, (pct.min(100), seed | 1));
        self
    }

    fn stream_entry(&mut self, stream_id: u32, protection: Protection) -> &mut RecvStream {
        let symbol_len = self.symbol_len;
        self.streams.entry(stream_id).or_insert_with(|| RecvStream {
            protection,
            dec: match protection {
                Protection::Protected => Some(RlcDecoder::new(symbol_len).with_horizon(128)),
                Protection::Bulk => None,
            },
            chunks: BTreeMap::new(),
            delivered_through: 0,
            gap_since: BTreeMap::new(),
            highest_seen: 0,
            final_info: None,
            fin_delivered: false,
        })
    }

    fn stream_entry_for(&mut self, stream_id: u32) -> &mut RecvStream {
        if !self.streams.contains_key(&stream_id) {
            self.stream_entry(stream_id, Protection::Bulk);
        }
        self.streams.get_mut(&stream_id).unwrap()
    }

    /// Receive any pending datagrams, run FEC recovery, and return the bytes that
    /// became deliverable on each stream (each stream independently in order).
    pub fn poll(&mut self) -> io::Result<Vec<StreamData>> {
        let mut buf = vec![0u8; self.symbol_len + REPAIR_HDR + 64];
        loop {
            match self.sock.recv_from(&mut buf) {
                Ok((n, from)) if n >= 1 => {
                    self.ingest(&buf[..n], from);
                }
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => break,
                Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => break,
                Err(e) => return Err(e),
            }
        }
        let mut out = Vec::new();
        let stream_ids: Vec<u32> = self.streams.keys().copied().collect();
        for sid in stream_ids {
            self.recover_stream(sid);
            if let Some(d) = self.deliver_stream(sid) {
                out.push(d);
            }
        }
        self.maybe_nak()?;
        self.send_acks()?;
        if out.is_empty() {
            std::thread::sleep(Duration::from_micros(100));
        }
        Ok(out)
    }

    /// Parse one datagram into its stream.
    fn ingest(&mut self, pkt: &[u8], from: SocketAddr) {
        if pkt.len() < 13 {
            return;
        }
        let cid = u64::from_le_bytes(pkt[1..9].try_into().unwrap());
        match self.cid {
            None => {
                self.cid = Some(cid);
                self.peer = Some(from);
            }
            Some(c) if c == cid => self.peer = Some(from),
            Some(_) => return,
        }
        let stream_id = u32::from_le_bytes(pkt[9..13].try_into().unwrap());
        match pkt[0] {
            STREAM if pkt.len() >= STREAM_HDR + self.symbol_len => {
                let sid = u32::from_le_bytes(pkt[13..17].try_into().unwrap());
                if self.drop_symbol(stream_id, sid) {
                    return;
                }
                let sym = pkt[STREAM_HDR..STREAM_HDR + self.symbol_len].to_vec();
                let st = self.stream_entry_for(stream_id);
                st.highest_seen = st.highest_seen.max(sid);
                st.gap_since.remove(&sid);
                if sid >= st.delivered_through {
                    st.chunks.entry(sid).or_insert(sym.clone());
                }
                if let Some(d) = st.dec.as_mut() {
                    d.on_source(sid, &sym);
                }
            }
            STREAM_REPAIR if pkt.len() >= REPAIR_HDR + self.symbol_len => {
                let repair_key = u32::from_le_bytes(pkt[13..17].try_into().unwrap());
                let first_source_id = u32::from_le_bytes(pkt[17..21].try_into().unwrap());
                let window_size = u16::from_le_bytes(pkt[21..23].try_into().unwrap());
                let dt = pkt[23];
                let payload = pkt[REPAIR_HDR..REPAIR_HDR + self.symbol_len].to_vec();
                let st = self.stream_entry_for(stream_id);
                if let Some(d) = st.dec.as_mut() {
                    d.add_repair(RepairSymbol {
                        repair_key,
                        first_source_id,
                        window_size,
                        dt,
                        payload,
                    });
                }
            }
            STREAM_FIN if pkt.len() >= FIN_LEN => {
                let final_sid = u32::from_le_bytes(pkt[13..17].try_into().unwrap());
                let final_len = u16::from_le_bytes(pkt[17..19].try_into().unwrap());
                let st = self.stream_entry_for(stream_id);
                st.final_info = Some((final_sid, final_len));
                st.highest_seen = st.highest_seen.max(final_sid);
            }
            _ => {}
        }
    }

    fn recover_stream(&mut self, stream_id: u32) {
        let Some(st) = self.streams.get_mut(&stream_id) else {
            return;
        };
        let Some(dec) = st.dec.as_mut() else {
            return;
        };
        let recovered = dec.recover();
        for sid in &recovered {
            if *sid >= st.delivered_through
                && let Some(sym) = dec.get(*sid)
            {
                st.chunks.entry(*sid).or_insert_with(|| sym.to_vec());
                st.gap_since.remove(sid);
            }
        }
        self.fec_recovered += recovered.len() as u64;
    }

    /// Deliver the contiguous prefix of a stream that has become available. The
    /// final symbol is trimmed to the length the STREAM_FIN announced. The
    /// highest buffered symbol is HELD while it is unknown whether it is the
    /// final one (a symbol is known non-final only once a higher one is seen or
    /// the reliable STREAM_FIN places the end past it) - otherwise the final
    /// symbol could be delivered full-length and without its fin.
    fn deliver_stream(&mut self, stream_id: u32) -> Option<StreamData> {
        let st = self.streams.get_mut(&stream_id)?;
        let mut data = Vec::new();
        let mut fin = false;
        loop {
            let s = st.delivered_through;
            let is_final = st.final_info.map(|(fs, _)| s == fs).unwrap_or(false);
            let known_non_final = match st.final_info {
                Some((fs, _)) => s < fs,
                None => s < st.highest_seen,
            };
            if !(is_final || known_non_final) {
                break;
            }
            let Some(sym) = st.chunks.remove(&s) else {
                break;
            };
            if is_final {
                let len = st.final_info.unwrap().1 as usize;
                data.extend_from_slice(&sym[..len.min(sym.len())]);
                st.delivered_through += 1;
                fin = true;
                st.fin_delivered = true;
                break;
            }
            data.extend_from_slice(&sym);
            st.delivered_through += 1;
        }
        if let Some(d) = st.dec.as_mut() {
            d.forget_below(st.delivered_through);
        }
        if data.is_empty() && !fin {
            return None;
        }
        Some(StreamData {
            stream_id,
            data,
            fin,
        })
    }

    /// NAK the lowest missing source ids of each stalled stream (rate-limited),
    /// so a gap the FEC cannot repair forward is retransmitted via ARQ.
    fn maybe_nak(&mut self) -> io::Result<()> {
        if self.last_nak.elapsed() < Duration::from_millis(2) {
            return Ok(());
        }
        let Some(peer) = self.peer else {
            return Ok(());
        };
        let cid = self.cid.unwrap_or(0);
        let now = Instant::now();
        let mut sent_any = false;
        let stream_ids: Vec<u32> = self.streams.keys().copied().collect();
        for stream_id in stream_ids {
            let st = self.streams.get_mut(&stream_id).unwrap();
            let mut missing = Vec::new();
            let mut sid = st.delivered_through;
            while sid <= st.highest_seen && missing.len() < 16 {
                if !st.chunks.contains_key(&sid) {
                    let first = *st.gap_since.entry(sid).or_insert(now);
                    // 2ms grace: let the FEC repair a Protected gap before ARQ.
                    if now.duration_since(first) >= Duration::from_millis(2) {
                        missing.push(sid);
                    }
                }
                sid += 1;
            }
            if missing.is_empty() {
                continue;
            }
            let mut pkt = Vec::with_capacity(13 + 4 * missing.len());
            pkt.push(STREAM_NAK);
            pkt.extend_from_slice(&cid.to_le_bytes());
            pkt.extend_from_slice(&stream_id.to_le_bytes());
            for m in missing {
                pkt.extend_from_slice(&m.to_le_bytes());
            }
            self.sock.send_to(&pkt, peer)?;
            self.naks_sent += 1;
            sent_any = true;
        }
        if sent_any {
            self.last_nak = now;
        }
        Ok(())
    }

    /// Send each stream's cumulative delivered frontier so the sender frees its
    /// per-stream and connection flow windows.
    fn send_acks(&mut self) -> io::Result<()> {
        let Some(peer) = self.peer else {
            return Ok(());
        };
        let cid = self.cid.unwrap_or(0);
        let frontiers: Vec<(u32, u32)> = self
            .streams
            .iter()
            .map(|(&id, s)| (id, s.delivered_through))
            .collect();
        for (stream_id, through) in frontiers {
            let mut pkt = Vec::with_capacity(ACK_LEN);
            pkt.push(STREAM_ACK);
            pkt.extend_from_slice(&cid.to_le_bytes());
            pkt.extend_from_slice(&stream_id.to_le_bytes());
            pkt.extend_from_slice(&through.to_le_bytes());
            self.sock.send_to(&pkt, peer)?;
        }
        Ok(())
    }

    /// Seeded diagnostic drop decision for `with_stream_loss`. A retransmit (a
    /// source id already buffered or delivered) always passes, so ARQ converges.
    fn drop_symbol(&mut self, stream_id: u32, sid: u32) -> bool {
        let Some((pct, seed)) = self.debug_loss.get(&stream_id).copied() else {
            return false;
        };
        if pct == 0 {
            return false;
        }
        if let Some(st) = self.streams.get(&stream_id)
            && (st.chunks.contains_key(&sid) || sid < st.delivered_through)
        {
            return false;
        }
        // Erase only the first transmission of a symbol: a deterministic per-sid
        // draw decides if it is unlucky, but once dropped it is recorded so the
        // retransmit passes - a recoverable one-off loss, not a black hole.
        if self.dropped_once.contains(&(stream_id, sid)) {
            return false;
        }
        let mut x = seed ^ (sid as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 27;
        if ((x % 100) as u32) < pct {
            self.dropped_once.insert((stream_id, sid));
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// Two streams - one Protected (RLC, with injected loss), one Bulk (ARQ) -
    /// run concurrently over one connection and each is delivered exactly and in
    /// order. The protected stream recovers losses forward (fec_recovered > 0).
    #[test]
    fn two_streams_deliver_independently_with_selective_fec() {
        let symbol_len = 256usize;
        let per_stream = 20_000usize;
        let (addr_tx, addr_rx) = mpsc::channel();

        let expected1: Vec<u8> = (0..per_stream).map(|i| (i % 251) as u8).collect();
        let expected2: Vec<u8> = (0..per_stream).map(|i| ((i * 7 + 3) % 251) as u8).collect();
        let e1 = expected1.clone();
        let e2 = expected2.clone();

        let rx = std::thread::spawn(move || {
            let mut recv = StreamMuxReceiver::bind("127.0.0.1:0", symbol_len)
                .unwrap()
                .with_stream_loss(1, 12, 0xC0DE);
            recv.expect_stream(1, Protection::Protected);
            recv.expect_stream(2, Protection::Bulk);
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let (mut got1, mut got2) = (Vec::new(), Vec::new());
            let (mut fin1, mut fin2) = (false, false);
            let start = Instant::now();
            while !(fin1 && fin2) {
                if start.elapsed() > Duration::from_secs(30) {
                    break;
                }
                for d in recv.poll().unwrap() {
                    match d.stream_id {
                        1 => {
                            got1.extend_from_slice(&d.data);
                            fin1 |= d.fin;
                        }
                        2 => {
                            got2.extend_from_slice(&d.data);
                            fin2 |= d.fin;
                        }
                        _ => {}
                    }
                }
            }
            for _ in 0..50 {
                recv.poll().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            (got1, got2, fin1, fin2, recv.fec_recovered())
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send =
                StreamMuxSender::bind("127.0.0.1:0", recv_addr, symbol_len, 256, 64).unwrap();
            send.open_stream(1, Protection::Protected);
            send.open_stream(2, Protection::Bulk);
            let chunk = 300usize;
            let (mut o1, mut o2) = (0usize, 0usize);
            while o1 < e1.len() || o2 < e2.len() {
                if o1 < e1.len() {
                    let end = (o1 + chunk).min(e1.len());
                    send.write(1, &e1[o1..end], end == e1.len()).unwrap();
                    o1 = end;
                }
                if o2 < e2.len() {
                    let end = (o2 + chunk).min(e2.len());
                    send.write(2, &e2[o2..end], end == e2.len()).unwrap();
                    o2 = end;
                }
            }
            send.flush(Duration::from_secs(25)).unwrap();
        });

        let (got1, got2, fin1, fin2, fec) = rx.join().unwrap();
        tx.join().unwrap();
        assert!(fin1 && fin2, "both streams must finish");
        assert_eq!(got1, expected1, "protected stream 1 delivered exactly");
        assert_eq!(got2, expected2, "bulk stream 2 delivered exactly");
        assert!(fec > 0, "the protected stream must have recovered losses via FEC");
    }

    /// No cross-stream head-of-line blocking: stream 1 takes heavy loss and lags,
    /// but stream 2 (clean) delivers completely and independently - its
    /// delivery frontier runs ahead while stream 1 is still recovering.
    #[test]
    fn a_stalled_stream_does_not_block_another() {
        let symbol_len = 128usize;
        let n = 6_000usize;
        let (addr_tx, addr_rx) = mpsc::channel();
        let expected2: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let e2 = expected2.clone();

        let rx = std::thread::spawn(move || {
            // Stream 1 is Bulk with 30% loss (ARQ-only, so it lags badly); stream
            // 2 is clean. Stream 2 must complete without waiting for stream 1.
            let mut recv = StreamMuxReceiver::bind("127.0.0.1:0", symbol_len)
                .unwrap()
                .with_stream_loss(1, 30, 0xBEEF);
            recv.expect_stream(1, Protection::Bulk);
            recv.expect_stream(2, Protection::Bulk);
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let mut got2 = Vec::new();
            let mut fin2 = false;
            let start = Instant::now();
            while !fin2 {
                if start.elapsed() > Duration::from_secs(30) {
                    break;
                }
                for d in recv.poll().unwrap() {
                    if d.stream_id == 2 {
                        got2.extend_from_slice(&d.data);
                        fin2 |= d.fin;
                    }
                }
            }
            for _ in 0..50 {
                recv.poll().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            (got2, fin2)
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send =
                StreamMuxSender::bind("127.0.0.1:0", recv_addr, symbol_len, 512, 256).unwrap();
            send.open_stream(1, Protection::Bulk);
            send.open_stream(2, Protection::Bulk);
            // Heavy data on stream 1 (lossy), interleaved with stream 2 (clean).
            let chunk = 100usize;
            let (mut o1, mut o2) = (0usize, 0usize);
            let one: Vec<u8> = (0..n).map(|i| (i % 13) as u8).collect();
            while o1 < one.len() || o2 < e2.len() {
                if o1 < one.len() {
                    let end = (o1 + chunk).min(one.len());
                    send.write(1, &one[o1..end], end == one.len()).unwrap();
                    o1 = end;
                }
                if o2 < e2.len() {
                    let end = (o2 + chunk).min(e2.len());
                    send.write(2, &e2[o2..end], end == e2.len()).unwrap();
                    o2 = end;
                }
            }
            send.flush(Duration::from_secs(25)).unwrap();
        });

        let (got2, fin2) = rx.join().unwrap();
        tx.join().unwrap();
        assert!(fin2, "the clean stream finished despite the other stalling");
        assert_eq!(got2, expected2, "the clean stream delivered exactly");
    }
}
