//! Schema-aware structural compression wrapped around the reliable-UDP
//! transport at the item boundary.
//!
//! The sender compresses each slot with a learned [`SchemaTemplate`]
//! before it enters the FEC encoder (the TX egress-gate: with per-block
//! shard sizing the smaller item becomes a smaller datagram). The receiver
//! decompresses after FEC reassembly, before delivery (the RX
//! read-modifier).
//!
//! Each transport item is tagged: a one-byte tag distinguishes a template
//! descriptor from a compressed slot, so the template is negotiated in-band
//! and can be **re-sent mid-stream** when the slot schema drifts. The
//! template descriptor is far larger than the 16-byte wire heartbeat, so it
//! rides as its own FEC/ARQ-reliable, in-order control item rather than on
//! the heartbeat plane.
//!
//! With re-learning enabled ([`with_relearn`](CompressedSender::with_relearn)),
//! the sender tracks the escape rate (slots that violate the template ship
//! in full) and, when it crosses a threshold over a window, re-learns the
//! template from recent slots and ships the new one in-band - so a drifting
//! schema is handled without a delivery gap.
//!
//! It is **exact**: the codec escapes any slot that violates the template,
//! so delivery is byte-identical to the uncompressed path. This wraps, and
//! does not modify, [`ReliableUdpSender`] / [`ReliableUdpReceiver`]; it
//! composes with sharding (one template per shard).

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use crate::schema_codec::SchemaTemplate;
use crate::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};

/// Item tags. A data item is a compressed slot; a template item is a
/// serialized [`SchemaTemplate`] that re-points the receiver's decoder.
const TAG_DATA: u8 = 0;
const TAG_TEMPLATE: u8 = 1;
/// A coalesced batch: `[TAG_BATCH][u16 len][cslot][u16 len][cslot]...`,
/// packing many compressed slots into one MTU-sized item so the datagram
/// rate stops being the bottleneck on small slots.
const TAG_BATCH: u8 = 2;

/// Sending half: tags and compresses each slot, ships the template first,
/// and (optionally) re-learns it mid-stream.
pub struct CompressedSender {
    inner: ReliableUdpSender,
    tpl: SchemaTemplate,
    item: Vec<u8>,
    started: bool,
    relearn: bool,
    window: usize,
    threshold_pct: u32,
    recent: VecDeque<Vec<u8>>,
    escapes: usize,
    since_check: usize,
    relearns: usize,
    coalesce: bool,
    batch_target: usize,
    batch: Vec<u8>,
    cslot: Vec<u8>,
}

impl CompressedSender {
    /// Wrap an existing sender with a learned template.
    pub fn new(inner: ReliableUdpSender, tpl: SchemaTemplate) -> Self {
        let cap = tpl.width() + 16;
        Self {
            inner,
            tpl,
            item: Vec::with_capacity(cap),
            started: false,
            relearn: false,
            window: 0,
            threshold_pct: 0,
            recent: VecDeque::new(),
            escapes: 0,
            since_check: 0,
            relearns: 0,
            coalesce: false,
            batch_target: 0,
            batch: Vec::new(),
            cslot: Vec::with_capacity(cap),
        }
    }

    /// How many times the template has been re-learned mid-stream (the
    /// observable signal that the schema drifted and adaptation fired).
    pub fn relearns(&self) -> usize {
        self.relearns
    }

    /// Coalesce many compressed slots into one transport item of up to
    /// `target` bytes before the FEC encoder, so the datagram rate stops
    /// bounding throughput on small slots (the stream bridges do the same,
    /// 256 slots per socket write). `target` should be near the FEC
    /// `max_item`. A partial batch flushes on [`flush`](Self::flush) or a
    /// re-learn. Off by default (one slot per item, lowest latency).
    pub fn with_coalesce(mut self, target: usize) -> Self {
        self.coalesce = true;
        self.batch_target = target.max(8);
        self.batch = Vec::with_capacity(self.batch_target + 8);
        self.batch.push(TAG_BATCH);
        self
    }

    /// Ship the accumulated batch (if any) as one item and reset it.
    fn flush_batch(&mut self) -> io::Result<()> {
        if self.batch.len() > 1 {
            self.inner.send_item(&self.batch)?;
            self.batch.clear();
            self.batch.push(TAG_BATCH);
        }
        Ok(())
    }

    /// Bind a fresh sender to `peer` with `k`+`r` FEC and a `max_item`
    /// FEC payload (must hold the tagged serialized template and a
    /// worst-case tagged escaped slot), wrapped with `tpl`.
    pub fn bind(
        peer: SocketAddr,
        k: usize,
        r: usize,
        max_item: usize,
        tpl: SchemaTemplate,
    ) -> io::Result<Self> {
        let inner = ReliableUdpSender::bind("0.0.0.0:0", peer, k, r, max_item)?;
        Ok(Self::new(inner, tpl))
    }

    /// Enable adaptive re-learning: keep the last `window` slots, and when
    /// the escape rate over a window exceeds `threshold_pct`, re-learn the
    /// template from those slots and ship the new one in-band.
    pub fn with_relearn(mut self, window: usize, threshold_pct: u32) -> Self {
        self.relearn = true;
        self.window = window.max(64);
        self.threshold_pct = threshold_pct.min(100);
        self.recent = VecDeque::with_capacity(self.window);
        self
    }

    fn send_template(&mut self) -> io::Result<()> {
        self.item.clear();
        self.item.push(TAG_TEMPLATE);
        self.item.extend_from_slice(&self.tpl.serialize());
        self.inner.send_item(&self.item)
    }

    fn relearn_now(&mut self) -> io::Result<()> {
        let width = self.tpl.width();
        let sample: Vec<&[u8]> = self.recent.iter().map(|s| s.as_slice()).collect();
        self.tpl = SchemaTemplate::learn(&sample, width);
        self.relearns += 1;
        self.send_template()
    }

    /// Compress `slot` and ship it (tagged). The first call ships the
    /// template; with re-learning on, a drifting schema triggers a new
    /// template in-band.
    pub fn send_item(&mut self, slot: &[u8]) -> io::Result<()> {
        if !self.started {
            self.send_template()?;
            self.started = true;
        }
        if self.relearn {
            if self.recent.len() == self.window {
                self.recent.pop_front();
            }
            self.recent.push_back(slot.to_vec());
        }
        self.cslot.clear();
        self.tpl.encode(slot, &mut self.cslot);
        let escaped = SchemaTemplate::is_escape(&self.cslot);

        if self.coalesce {
            // Flush first if this slot would overflow the batch target,
            // then frame it as `[u16 len][cslot]` into the batch.
            if self.batch.len() + 2 + self.cslot.len() > self.batch_target && self.batch.len() > 1 {
                self.flush_batch()?;
            }
            self.batch
                .extend_from_slice(&(self.cslot.len() as u16).to_le_bytes());
            self.batch.extend_from_slice(&self.cslot);
        } else {
            self.item.clear();
            self.item.push(TAG_DATA);
            self.item.extend_from_slice(&self.cslot);
            self.inner.send_item(&self.item)?;
        }

        if self.relearn {
            if escaped {
                self.escapes += 1;
            }
            self.since_check += 1;
            if self.since_check >= self.window {
                if self.escapes * 100 > self.threshold_pct as usize * self.window {
                    // Flush old-template slots before the new template so
                    // ordering holds at the receiver.
                    self.flush_batch()?;
                    self.relearn_now()?;
                }
                self.escapes = 0;
                self.since_check = 0;
            }
        }
        Ok(())
    }

    /// True while the FEC flow window is full (delegates to the inner
    /// sender). Pair with [`pump_feedback`](Self::pump_feedback).
    pub fn flow_blocked(&self) -> bool {
        self.inner.flow_blocked()
    }

    /// Drain inbound feedback (acks / NAKs), advancing the flow window.
    pub fn pump_feedback(&mut self) -> io::Result<()> {
        self.inner.pump_feedback()
    }

    /// Flush the pending coalesce batch, then the final partial FEC block.
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_batch()?;
        self.inner.flush()
    }

    /// Block until every shipped block is acknowledged or `timeout`.
    pub fn drain_until_acked(&mut self, timeout: Duration) -> io::Result<bool> {
        self.inner.drain_until_acked(timeout)
    }
}

/// Receiving half: applies tagged template updates, decompresses tagged
/// data items into full slots.
pub struct CompressedReceiver {
    inner: ReliableUdpReceiver,
    tpl: Option<SchemaTemplate>,
}

impl CompressedReceiver {
    /// Wrap an existing receiver.
    pub fn new(inner: ReliableUdpReceiver) -> Self {
        Self { inner, tpl: None }
    }

    /// Bind a fresh receiver on `local`, wrapped.
    pub fn bind(local: SocketAddr) -> io::Result<Self> {
        Ok(Self::new(ReliableUdpReceiver::bind(local)?))
    }

    /// Poll the transport, returning decompressed slots in stream order. A
    /// template item updates the decoder and emits no slot; a data item is
    /// decompressed with the current template.
    pub fn poll(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let raw = self.inner.poll()?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            if item.is_empty() {
                continue;
            }
            let (tag, payload) = (item[0], &item[1..]);
            if tag == TAG_TEMPLATE {
                // A malformed descriptor leaves the current template in
                // place rather than dropping into an un-templated state.
                if let Some(t) = SchemaTemplate::deserialize(payload) {
                    self.tpl = Some(t);
                }
            } else if tag == TAG_BATCH {
                // `[u16 len][cslot]...` - split and decode each slot.
                if let Some(t) = &self.tpl {
                    let mut off = 0usize;
                    while off + 2 <= payload.len() {
                        let len = u16::from_le_bytes([payload[off], payload[off + 1]]) as usize;
                        off += 2;
                        if off + len > payload.len() {
                            break;
                        }
                        let mut slot = vec![0u8; t.width()];
                        t.decode(&payload[off..off + len], &mut slot);
                        out.push(slot);
                        off += len;
                    }
                }
            } else if let Some(t) = &self.tpl {
                // TAG_DATA: a single compressed slot.
                let mut slot = vec![0u8; t.width()];
                t.decode(payload, &mut slot);
                out.push(slot);
            }
        }
        Ok(out)
    }

    /// Drive tail-ARQ feedback when idle (delegates to the inner
    /// receiver).
    pub fn nudge_feedback(&mut self) -> io::Result<()> {
        self.inner.nudge_feedback()
    }

    /// Inject diagnostic loss on the inner receiver (test surface).
    pub fn with_debug_loss(mut self, pct: u32, seed: u64) -> Self {
        self.inner = self.inner.with_debug_loss(pct, seed);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_deque_khpd::{FatLineItem, LineItem};
    use std::net::{IpAddr, Ipv4Addr};
    use std::thread;
    use subetha_core::Marshal;

    struct Rng(u64);
    impl Rng {
        fn new(s: u64) -> Self {
            Self(s | 1)
        }
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn byte(&mut self) -> u8 {
            (self.next() >> 24) as u8
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// `phase` shifts which bytes are constant, so a template learned in
    /// phase 0 escapes heavily in phase 1 until the sender re-learns.
    fn slots(n: usize, seed: u64, phase: u8) -> Vec<[u8; 64]> {
        let mut rng = Rng::new(seed);
        let mut out = Vec::with_capacity(n);
        let mut id = 0u32;
        for _ in 0..n {
            let cnt = 1 + rng.below(3) as usize;
            let mut items = Vec::with_capacity(cnt);
            for _ in 0..cnt {
                let mut b = [0u8; 16];
                b[0] = rng.below(16) as u8;
                b[1] = phase; // phase byte: constant within a phase
                b[4..8].copy_from_slice(&id.to_le_bytes());
                id = id.wrapping_add(1);
                for x in b.iter_mut().skip(8) {
                    *x = rng.byte();
                }
                items.push(LineItem::new(&b).unwrap());
            }
            let fat = FatLineItem::from_items(&items).unwrap();
            let mut s = [0u8; 64];
            fat.marshal(&mut s);
            out.push(s);
        }
        out
    }

    fn run_at_loss(loss: u32, port: u16) {
        let total = 4000usize;
        let data = slots(total, 0x99, 0);
        let sample: Vec<&[u8]> = data.iter().step_by(7).map(|s| s.as_slice()).collect();
        let tpl = SchemaTemplate::learn(&sample, 64);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let mut recv = CompressedReceiver::bind(addr).unwrap();
        if loss > 0 {
            recv = recv.with_debug_loss(loss, 0x1234);
        }
        let expect = data.clone();
        let rx = thread::spawn(move || -> bool {
            let mut got: Vec<Vec<u8>> = Vec::with_capacity(total);
            let start = std::time::Instant::now();
            while got.len() < total {
                if start.elapsed() > Duration::from_secs(60) {
                    return false;
                }
                for s in recv.poll().unwrap_or_default() {
                    got.push(s);
                }
            }
            for _ in 0..50 {
                recv.nudge_feedback().ok();
                thread::sleep(Duration::from_millis(2));
            }
            got.len() == total && got.iter().zip(expect.iter()).all(|(a, b)| a.as_slice() == &b[..])
        });

        thread::sleep(Duration::from_millis(150));
        let mut send = CompressedSender::bind(addr, 8, 2, 256, tpl).unwrap();
        for s in &data {
            while send.flow_blocked() {
                send.pump_feedback().ok();
                if send.flow_blocked() {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            send.send_item(s).unwrap();
        }
        send.flush().unwrap();
        let acked = send.drain_until_acked(Duration::from_secs(60)).unwrap();
        let ok = rx.join().unwrap();
        assert!(ok, "byte-exact delivery failed at {loss}% loss");
        assert!(acked, "not fully acked at {loss}% loss");
    }

    #[test]
    fn compressed_exact_clean() {
        run_at_loss(0, 25410);
    }

    #[test]
    fn compressed_exact_15pct_loss() {
        run_at_loss(15, 25411);
    }

    #[test]
    fn compressed_exact_30pct_loss() {
        run_at_loss(30, 25412);
    }

    /// A mid-stream schema drift: the second half of the stream uses a
    /// different phase byte, so the phase-0 template escapes until the
    /// sender re-learns. Delivery stays byte-exact across the drift.
    #[test]
    fn relearn_keeps_exact_across_schema_drift() {
        let half = 3000usize;
        let mut data = slots(half, 0x21, 0);
        data.extend(slots(half, 0x22, 7)); // drift: different phase byte
        let total = data.len();
        let sample: Vec<&[u8]> = data[..half].iter().step_by(5).map(|s| s.as_slice()).collect();
        let tpl = SchemaTemplate::learn(&sample, 64);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 25420);

        let mut recv = CompressedReceiver::bind(addr).unwrap();
        let expect = data.clone();
        let rx = thread::spawn(move || -> bool {
            let mut got: Vec<Vec<u8>> = Vec::with_capacity(total);
            let start = std::time::Instant::now();
            while got.len() < total {
                if start.elapsed() > Duration::from_secs(60) {
                    return false;
                }
                for s in recv.poll().unwrap_or_default() {
                    got.push(s);
                }
            }
            let ok = got.len() == total
                && got.iter().zip(expect.iter()).all(|(a, b)| a.as_slice() == &b[..]);
            // Keep nudging tail-ARQ feedback so the sender's final blocks ack.
            for _ in 0..50 {
                recv.nudge_feedback().ok();
                thread::sleep(Duration::from_millis(2));
            }
            ok
        });

        thread::sleep(Duration::from_millis(150));
        // Small window + low threshold so the drift triggers a re-learn.
        let mut send = CompressedSender::bind(addr, 8, 2, 256, tpl)
            .unwrap()
            .with_relearn(256, 20);
        for s in &data {
            while send.flow_blocked() {
                send.pump_feedback().ok();
                if send.flow_blocked() {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            send.send_item(s).unwrap();
        }
        send.flush().unwrap();
        let acked = send.drain_until_acked(Duration::from_secs(60)).unwrap();
        assert!(rx.join().unwrap(), "byte-exact across schema drift failed");
        assert!(acked, "not fully acked");
    }

    /// Coalescing packs many compressed slots per item; delivery stays
    /// byte-exact including FEC recovery on the batched items under loss.
    #[test]
    fn coalesce_exact_with_loss() {
        let total = 6000usize;
        let data = slots(total, 0x77, 0);
        let sample: Vec<&[u8]> = data.iter().step_by(7).map(|s| s.as_slice()).collect();
        let tpl = SchemaTemplate::learn(&sample, 64);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 25430);

        let mut recv = CompressedReceiver::bind(addr).unwrap().with_debug_loss(15, 0x55);
        let expect = data.clone();
        let rx = thread::spawn(move || -> bool {
            let mut got: Vec<Vec<u8>> = Vec::with_capacity(total);
            let start = std::time::Instant::now();
            while got.len() < total {
                if start.elapsed() > Duration::from_secs(60) {
                    return false;
                }
                for s in recv.poll().unwrap_or_default() {
                    got.push(s);
                }
            }
            let ok = got.len() == total
                && got.iter().zip(expect.iter()).all(|(a, b)| a.as_slice() == &b[..]);
            for _ in 0..50 {
                recv.nudge_feedback().ok();
                thread::sleep(Duration::from_millis(2));
            }
            ok
        });

        thread::sleep(Duration::from_millis(150));
        let mut send = CompressedSender::bind(addr, 8, 2, 1400, tpl)
            .unwrap()
            .with_coalesce(1200);
        for s in &data {
            while send.flow_blocked() {
                send.pump_feedback().ok();
                if send.flow_blocked() {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            send.send_item(s).unwrap();
        }
        send.flush().unwrap();
        let acked = send.drain_until_acked(Duration::from_secs(60)).unwrap();
        assert!(rx.join().unwrap(), "coalesced byte-exact failed");
        assert!(acked, "coalesced not fully acked");
    }
}
