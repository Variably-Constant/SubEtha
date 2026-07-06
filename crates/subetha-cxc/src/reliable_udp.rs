//! Sens-O-Matic protocol: a reliable-UDP transport, FEC-primary,
//! ARQ-fallback.
//!
//! The coding and wire format for Sens-O-Matic, the sighted,
//! forward-correcting reliable-UDP transport. The socket layer that
//! drives it lives in [`crate::udp_bridge`].
//!
//! This is the encryption-free reliable datagram layer that gives a
//! trusted-network bridge ordered, lossless delivery over `UdpSocket`
//! without TLS. Reliability comes from two mechanisms, in priority
//! order:
//!
//!  1. **FEC (primary).** Source items are grouped into blocks of `k`
//!     shards and shipped with `r` Cauchy Reed-Solomon parity shards
//!     ([`crate::fec`]). Up to `r` losses per block are reconstructed by
//!     the receiver with **no retransmit round-trip**.
//!  2. **ARQ (fallback).** When a block loses MORE than `r` shards - the
//!     rare burst FEC cannot cover - the receiver NAKs the missing shard
//!     indices and the sender retransmits exactly those.
//!
//! The parity rate `r` is **automatic**: the receiver reports its
//! measured loss fraction on every feedback packet and the sender raises
//! or lowers `r` for subsequent blocks so FEC carries the common case
//! (small `r` on a clean LAN, larger `r` on lossy Wi-Fi) and ARQ stays a
//! fallback.
//!
//! The protocol is transport-agnostic: [`Encoder`] turns items into
//! datagrams and [`Decoder`] turns datagrams back into ordered items,
//! both over byte slices. A real socket or a deterministic lossy channel
//! plugs in identically, which is what lets the FEC/ARQ behavior be
//! proven without a network.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::fec::RsCode;
use crate::loss_class_sensor::LossClassSensor;
use crate::temporal_sensor::TemporalSensor;
use crate::tower::SegmentCode;

/// Packet type tag (first wire byte). Data datagrams use this tag; the
/// control plane (ACK / NAK / loss / timing / ring / path / link / ...) rides
/// the framed `PKT_CONTROL` container in [`crate::control_frame`].
const PKT_DATA: u8 = 1;

/// Fixed data-packet header length: `type(1) block_id(4) shard_index(1)
/// k(1) r(1) flags(1)`.
pub const DATA_HEADER: usize = 9;

/// `flags` bit: this shard is a parity shard (index `>= k`).
const FLAG_PARITY: u8 = 0b0000_0001;

/// `flags` bit: this block is a tower outer-parity block - fire-and-forget
/// cross-block redundancy used opportunistically by the receiver, never
/// ARQ-tracked (ARQ on the data blocks is the correctness floor).
const FLAG_OUTER: u8 = 0b0000_0010;

/// `flags` bit: this datagram is an ARQ retransmit. A data shard arriving
/// with this flag for the first time means its original was dropped, so the
/// receiver counts it as a wire loss even though ARQ recovered it - the
/// signal that lets the loss estimator see drops Passthrough hides behind ARQ.
const FLAG_RETRANSMIT: u8 = 0b0000_0100;

/// High bit set on an outer-parity block id, separating it from the
/// sequential data-block id space. The low bits encode
/// `(segment << 8) | outer_index`.
const OUTER_ID_BIT: u32 = 0x8000_0000;

/// Maximum shards per block (`k + r`); keeps the received-bitmap in one
/// `u32`.
pub const MAX_SHARDS: usize = 32;

/// Per-data-shard payload prefix: the real item length in bytes.
const ITEM_LEN_PREFIX: usize = 2;

/// Sentinel `nak_block` meaning "no retransmit requested".
pub const NAK_NONE: u32 = u32::MAX;

/// Whether the reordering guard subtracts spurious-retransmit false recoveries
/// (the D-SACK signal) from the loss estimate. Default on; `SUBETHA_REORDER_GUARD=0`
/// disables the subtraction for the A/B baseline that shows reordering inflating
/// the loss estimate without it. Read once and cached.
fn reorder_guard_enabled() -> bool {
    static EN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *EN.get_or_init(|| {
        std::env::var("SUBETHA_REORDER_GUARD")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

/// The receiver-side control state - ack frontier, selective NAK, and the
/// fused channel readings - that the bridge carries as `Ack` / `Nak` / `Loss`
/// frames in a [`crate::control_frame`] CONTROL packet. Kept as a struct
/// because it is the form the sender's controller already consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Feedback {
    /// Next block the receiver still needs (everything below is
    /// delivered); the sender frees retransmit state below this.
    pub ack_through: u32,
    /// Block whose missing shards should be retransmitted, or
    /// [`NAK_NONE`].
    pub nak_block: u32,
    /// Bitmap of MISSING shard indices in `nak_block`.
    pub nak_mask: u32,
    /// Estimated loss fraction scaled to `0..=255`.
    pub loss_x255: u8,
    /// Estimated burstiness scaled to `0..=255` (clustering of loss).
    pub burstiness_x255: u8,
    /// One-way-delay trend class: 0 = falling, 1 = flat, 2 = rising.
    pub owd_trend_class: u8,
    /// Loss-class code (0 = no loss, 1 = wireless, 2 = congestion, 3 = mixed)
    /// from the receiver's [`crate::loss_class_sensor`].
    pub loss_class: u8,
}

/// Returns `true` if `buf` is a tower outer-parity datagram.
pub fn is_outer_datagram(buf: &[u8]) -> bool {
    buf.len() > DATA_HEADER && buf[0] == PKT_DATA && (buf[8] & FLAG_OUTER) != 0
}

/// Returns `true` if `buf` is a data datagram (vs feedback).
pub fn is_data(buf: &[u8]) -> bool {
    !buf.is_empty() && buf[0] == PKT_DATA
}

/// A built block held by the sender for possible ARQ retransmission.
struct PendingBlock {
    k: u8,
    r: u8,
    shard_len: usize,
    /// `k + r` shard payloads (data first, then parity).
    shards: Vec<Vec<u8>>,
}

impl PendingBlock {
    fn datagram(&self, block_id: u32, idx: usize) -> Vec<u8> {
        self.datagram_flagged(block_id, idx, 0)
    }

    fn datagram_flagged(&self, block_id: u32, idx: usize, extra_flags: u8) -> Vec<u8> {
        let mut pkt = Vec::with_capacity(DATA_HEADER + self.shard_len);
        pkt.push(PKT_DATA);
        pkt.extend_from_slice(&block_id.to_le_bytes());
        pkt.push(idx as u8);
        pkt.push(self.k);
        pkt.push(self.r);
        let parity = if idx >= self.k as usize { FLAG_PARITY } else { 0 };
        pkt.push(parity | extra_flags);
        pkt.extend_from_slice(&self.shards[idx]);
        pkt
    }
}

/// Sender side: groups items into FEC-protected blocks and answers
/// ARQ retransmit requests.
pub struct Encoder {
    k: usize,
    /// Current parity count; adapts to reported loss between
    /// [`r_min`](Self::r_min) and [`r_max`](Self::r_max).
    r: usize,
    r_min: usize,
    r_max: usize,
    /// Usable payload bytes per shard (item + length prefix).
    shard_len: usize,
    next_block: u32,
    /// Highest `ack_through` reported by the receiver; below this every
    /// block is delivered.
    acked_through: u32,
    /// Max blocks in flight (sent but not yet acked) before the producer
    /// should apply backpressure; matches the receiver's window.
    flow_window: u32,
    /// Items accumulated for the block under construction.
    staged: Vec<Vec<u8>>,
    /// Built-but-unacked blocks, keyed by block id, for ARQ.
    pending: BTreeMap<u32, PendingBlock>,
    /// Tower outer code dimensions: `(d, r_outer)`; `(0, 0)` = disabled.
    tower_d: usize,
    tower_r_outer: usize,
    /// Data-block infos (the `k` data shards concatenated) accumulated for
    /// the current segment.
    seg_infos: Vec<Vec<u8>>,
    /// Current segment id.
    seg_id: u32,
    /// Blocks sealed at zero parity (Passthrough); telemetry that proves the
    /// controller actually dropped FEC off the wire on a clean link.
    passthrough_blocks: u64,
    /// Blocks sealed with parity (r >= 1); telemetry counterpart.
    fec_blocks: u64,
}

impl Encoder {
    /// Create an encoder. `k` data shards per block, initial `r` parity
    /// shards (clamped to `r_min..=r_max`), `max_item` largest item
    /// byte length.
    pub fn new(k: usize, r: usize, max_item: usize) -> Self {
        // r_min = 0 lets the fusion controller drop to zero parity
        // (CodingLevel::Passthrough) on a provably-clean link: the block
        // ships its k data shards with no FEC encode and no parity datagrams,
        // and ARQ remains the reliability floor. The controller only selects
        // r=0 after a sustained-clean confidence window and re-arms to r>=1
        // the instant loss, burstiness, or link stress appears.
        let r_min = 0;
        // Cap parity so k + r never exceeds MAX_SHARDS (the per-block received/NAK
        // bitmap is a u32, and `1 << idx` for idx >= 32 overflows). saturating_sub
        // with a 0 floor means k == MAX_SHARDS yields r_max = 0 (Passthrough,
        // ARQ-only) rather than a 1 that would overflow the bitmap. The full
        // k + r = MAX_SHARDS is decode-sound (Cauchy over GF(256); see
        // fec::tests::recovery_k16_r16_high_parity), so the only ceiling is the
        // bitmap - a high-loss block can provision parity up to it.
        let r_max = MAX_SHARDS.saturating_sub(k);
        Self {
            k,
            r: r.clamp(r_min, r_max),
            r_min,
            r_max,
            shard_len: max_item + ITEM_LEN_PREFIX,
            next_block: 0,
            acked_through: 0,
            flow_window: 256,
            staged: Vec::with_capacity(k),
            pending: BTreeMap::new(),
            tower_d: 0,
            tower_r_outer: 0,
            seg_infos: Vec::new(),
            seg_id: 0,
            passthrough_blocks: 0,
            fec_blocks: 0,
        }
    }

    /// Blocks sealed at zero parity (Passthrough) so far, and blocks sealed
    /// with parity. A nonzero first value proves FEC actually switched off on
    /// the wire; the ratio shows how much of the stream rode unprotected.
    pub fn coding_counts(&self) -> (u64, u64) {
        (self.passthrough_blocks, self.fec_blocks)
    }

    /// Enable the tower outer code: every `d` data blocks ship with
    /// `r_outer` fire-and-forget outer-parity blocks that recover whole
    /// lost data blocks without a retransmit. `(0, _)` or `(_, 0)`
    /// disables it.
    pub fn enable_tower(&mut self, d: usize, r_outer: usize) {
        if d == 0 || r_outer == 0 || d + r_outer > MAX_SHARDS {
            self.tower_d = 0;
            self.tower_r_outer = 0;
        } else {
            self.tower_d = d;
            self.tower_r_outer = r_outer;
        }
        self.seg_infos.clear();
    }

    /// Set the in-flight flow window (blocks sent but not yet acked).
    /// Match this to the receiver's [`Decoder::with_window`].
    pub fn with_flow_window(mut self, blocks: u32) -> Self {
        self.flow_window = blocks.max(1);
        self
    }

    /// Adjust the in-flight flow window at runtime - the bufferbloat pacer
    /// shrinks it toward the BDP to drain a self-induced queue, and restores it
    /// when the queue clears. The receiver's window is the hard ceiling, so the
    /// pacer only ever clamps DOWN from the configured maximum.
    pub fn set_flow_window(&mut self, blocks: u32) {
        self.flow_window = blocks.max(1);
    }

    /// Current in-flight flow window (blocks).
    pub fn flow_window(&self) -> u32 {
        self.flow_window
    }

    /// Blocks sent but not yet acked by the receiver.
    pub fn in_flight(&self) -> u32 {
        self.next_block.wrapping_sub(self.acked_through)
    }

    /// `true` when the producer should pause sending new blocks until an
    /// ack frees window space (keeps the receiver's bounded window from
    /// dropping far-ahead blocks).
    pub fn flow_blocked(&self) -> bool {
        self.in_flight() >= self.flow_window
    }

    /// Largest item this encoder accepts.
    pub fn max_item(&self) -> usize {
        self.shard_len - ITEM_LEN_PREFIX
    }

    /// Current parity count.
    pub fn parity(&self) -> usize {
        self.r
    }

    /// The id the NEXT sealed block will take; the block just sealed by a
    /// non-empty [`push`](Self::push) / [`flush`](Self::flush) is this minus
    /// one. Lets the sender record a per-block send time for RTT sampling.
    pub fn next_block_id(&self) -> u32 {
        self.next_block
    }

    /// Stage `item` for transmission. Returns the datagrams to send when
    /// the staged set reaches `k` items (a full block); otherwise an
    /// empty vec. Call [`flush`](Self::flush) to force a short final
    /// block.
    pub fn push(&mut self, item: &[u8]) -> Vec<Vec<u8>> {
        debug_assert!(item.len() <= self.max_item());
        // Stage the unpadded shard (length prefix + item). seal_block pads
        // every shard in the block to the block's largest item - so a block
        // of small (e.g. schema-compressed) items ships small datagrams.
        let mut shard = Vec::with_capacity(ITEM_LEN_PREFIX + item.len());
        shard.extend_from_slice(&(item.len() as u16).to_le_bytes());
        shard.extend_from_slice(item);
        self.staged.push(shard);
        if self.staged.len() == self.k {
            self.seal_block()
        } else {
            Vec::new()
        }
    }

    /// Force the staged items (fewer than `k`) into a final padded
    /// block. Returns its datagrams, or empty if nothing is staged.
    pub fn flush(&mut self) -> Vec<Vec<u8>> {
        let mut out = if self.staged.is_empty() {
            Vec::new()
        } else {
            self.seal_block()
        };
        // Seal a partial final segment so its blocks get tower protection
        // too (otherwise a whole-block loss in the tail segment has no
        // outer parity to recover from).
        if self.tower_d > 0 && !self.seg_infos.is_empty() {
            out.extend(self.seal_segment());
        }
        out
    }

    fn seal_block(&mut self) -> Vec<Vec<u8>> {
        // Per-block shard length: the largest staged shard in this block,
        // so a block of small items ships small datagrams. The tower's
        // cross-block outer code needs uniform blocks across a segment, so
        // when it is enabled the fixed maximum is used instead. The decoder
        // reads each block's shard length from the datagram size, so no
        // header field is required.
        let block_shard_len = if self.tower_d > 0 {
            self.shard_len
        } else {
            self.staged
                .iter()
                .map(|s| s.len())
                .max()
                .unwrap_or(ITEM_LEN_PREFIX)
                .max(ITEM_LEN_PREFIX)
        };
        for s in &mut self.staged {
            s.resize(block_shard_len, 0);
        }
        // Pad with zero-length items up to k data shards.
        while self.staged.len() < self.k {
            let mut pad = vec![0u8; block_shard_len];
            pad[0..2].copy_from_slice(&0u16.to_le_bytes());
            self.staged.push(pad);
        }
        let r = self.r;
        let mut shards: Vec<Vec<u8>> = std::mem::take(&mut self.staged);
        // Capture this block's info (the k data shards) for the tower,
        // before parity is appended.
        let tower_info = if self.tower_d > 0 {
            Some(shards.concat())
        } else {
            None
        };
        // Passthrough (r=0): ship the k data shards with no parity encode.
        // ARQ recovers any dropped data shard; the controller only reaches
        // r=0 on a sustained-clean link.
        if r == 0 {
            self.passthrough_blocks += 1;
        } else {
            self.fec_blocks += 1;
        }
        if r > 0 {
            let mut parity: Vec<Vec<u8>> = vec![vec![0u8; block_shard_len]; r];
            {
                let code = RsCode::new(self.k, r).expect("valid k,r");
                let data_refs: Vec<&[u8]> = shards.iter().map(|s| s.as_slice()).collect();
                let mut par_refs: Vec<&mut [u8]> =
                    parity.iter_mut().map(|s| s.as_mut_slice()).collect();
                code.encode(&data_refs, &mut par_refs).expect("encode");
            }
            shards.extend(parity);
        }
        let block_id = self.next_block;
        self.next_block += 1;
        let pb = PendingBlock {
            k: self.k as u8,
            r: r as u8,
            shard_len: block_shard_len,
            shards,
        };
        let mut datagrams: Vec<Vec<u8>> =
            (0..self.k + r).map(|i| pb.datagram(block_id, i)).collect();
        self.pending.insert(block_id, pb);
        self.staged = Vec::with_capacity(self.k);
        // Tower: accumulate this block's info; emit outer-parity blocks
        // when the segment fills.
        if let Some(info) = tower_info {
            self.seg_infos.push(info);
            if self.seg_infos.len() == self.tower_d {
                datagrams.extend(self.seal_segment());
            }
        }
        datagrams
    }

    /// Compute and emit the segment's outer-parity blocks (fire-and-forget:
    /// not added to `pending`, so they are never retransmitted - ARQ on the
    /// data blocks is the floor).
    fn seal_segment(&mut self) -> Vec<Vec<u8>> {
        let r_outer = self.tower_r_outer;
        let infos = std::mem::take(&mut self.seg_infos);
        // Use the ACTUAL block count: a full segment has `tower_d`, the
        // final partial segment (flushed) has fewer. The count is encoded
        // in the outer id so the receiver protects partial segments too.
        let d = infos.len();
        if d == 0 || r_outer == 0 {
            return Vec::new();
        }
        let info_len = infos[0].len();
        let seg = SegmentCode::new(d, r_outer).expect("valid d,r_outer");
        let mut outer: Vec<Vec<u8>> = vec![vec![0u8; info_len]; r_outer];
        {
            let dref: Vec<&[u8]> = infos.iter().map(|v| v.as_slice()).collect();
            let mut pref: Vec<&mut [u8]> = outer.iter_mut().map(|v| v.as_mut_slice()).collect();
            seg.encode(&dref, &mut pref).expect("outer encode");
        }
        let seg_id = self.seg_id;
        self.seg_id += 1;
        let r = self.r;
        let mut out = Vec::new();
        for (oidx, oinfo) in outer.into_iter().enumerate() {
            // The outer info is k data shards; inner-encode it like any
            // block so it survives shard loss on the wire too.
            let mut oshards: Vec<Vec<u8>> =
                oinfo.chunks(self.shard_len).map(|c| c.to_vec()).collect();
            let mut oparity: Vec<Vec<u8>> = vec![vec![0u8; self.shard_len]; r];
            {
                let code = RsCode::new(self.k, r).expect("valid k,r");
                let dref: Vec<&[u8]> = oshards.iter().map(|s| s.as_slice()).collect();
                let mut pref: Vec<&mut [u8]> =
                    oparity.iter_mut().map(|s| s.as_mut_slice()).collect();
                code.encode(&dref, &mut pref).expect("inner encode outer");
            }
            oshards.extend(oparity);
            let opb = PendingBlock {
                k: self.k as u8,
                r: r as u8,
                shard_len: self.shard_len,
                shards: oshards,
            };
            // Self-describing id: bit31 = OUTER, bits27-30 = d (1..15),
            // bits24-26 = r_outer (1..7), bits8-23 = segment, bits0-7 =
            // outer index. The receiver learns the segment structure from
            // the wire, no out-of-band config.
            let oid = OUTER_ID_BIT
                | ((d as u32) << 27)
                | ((r_outer as u32) << 24)
                | (seg_id << 8)
                | oidx as u32;
            for i in 0..self.k + r {
                out.push(opb.datagram_flagged(oid, i, FLAG_OUTER));
            }
        }
        out
    }

    /// Set the parity shards per new block, clamped to the encoder's
    /// `[r_min, r_max]`. The fusion controller drives this from the
    /// control table; the encoder no longer self-adapts parity.
    pub fn set_parity(&mut self, r: usize) {
        self.r = r.clamp(self.r_min, self.r_max);
    }

    /// Set parity to at least `floor` (the fusion controller's burst / feed-forward
    /// signal) AND enough to FEC-recover a `loss` fraction of THIS block: to
    /// recover a fraction p of the k + r shards, r / (k + r) >= p, i.e.
    /// r >= p * k / (1 - p). A 20% margin covers a spike above the mean. Capped at
    /// `r_max` (the bitmap ceiling). Without this, parity tracked only the
    /// controller's modest floor and a high-loss block fell to ARQ round trips
    /// instead of recovering in-FEC; this lets block-RS provision to the loss the
    /// way the sliding-window RLC rate law already does.
    pub fn set_parity_covering(&mut self, floor: usize, loss: f32) {
        let p = (loss * 1.2).clamp(0.0, 0.95);
        let cover = (p * self.k as f32 / (1.0 - p)).ceil() as usize;
        self.r = floor.max(cover).clamp(self.r_min, self.r_max);
    }

    /// Apply receiver feedback: free acked blocks and return any ARQ
    /// retransmit datagrams. Parity adaptation is the controller's job
    /// (see [`set_parity`](Self::set_parity)), not this method's.
    pub fn on_feedback(&mut self, fb: &Feedback) -> Vec<Vec<u8>> {
        if fb.ack_through > self.acked_through {
            self.acked_through = fb.ack_through;
        }
        // Free everything the receiver has fully delivered.
        let acked: Vec<u32> = self
            .pending
            .range(..fb.ack_through)
            .map(|(&id, _)| id)
            .collect();
        for id in acked {
            self.pending.remove(&id);
        }
        // ARQ: retransmit the requested missing shards.
        let mut out = Vec::new();
        if fb.nak_block != NAK_NONE
            && let Some(pb) = self.pending.get(&fb.nak_block)
        {
            let n = pb.shards.len();
            for idx in 0..n {
                if fb.nak_mask & (1 << idx) != 0 {
                    out.push(pb.datagram_flagged(fb.nak_block, idx, FLAG_RETRANSMIT));
                }
            }
        }
        out
    }

    /// Number of unacked blocks held for ARQ.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// The oldest unacked block id - the one the receiver's in-order frontier
    /// is waiting on - or `None` if everything is acked.
    pub fn oldest_pending(&self) -> Option<u32> {
        self.pending.keys().next().copied()
    }

    /// Retransmit datagrams (flagged `RETRANSMIT`) for the `k` DATA shards of
    /// one pending block - a liveness probe that also pre-positions the block
    /// the receiver's frontier is stalled on. Empty if the block is already
    /// acked.
    pub fn probe_block(&self, block_id: u32) -> Vec<Vec<u8>> {
        match self.pending.get(&block_id) {
            Some(pb) => (0..pb.k as usize)
                .map(|idx| pb.datagram_flagged(block_id, idx, FLAG_RETRANSMIT))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Retransmit datagrams (flagged `RETRANSMIT`) for the `k` DATA shards of
    /// EVERY pending block, oldest-first - the proactive burst on link recovery
    /// that resends the whole unacked window WITHOUT waiting for the receiver's
    /// NAKs (the sender already holds the exact unacked set, so no estimation
    /// is needed). The receiver dedups any datagram it already has via its
    /// D-SACK / false-recovery path, so over-resending is safe. `k` data shards
    /// per block suffice to decode a fully-lost block; any shard still missing
    /// after the burst is recovered by the normal reactive NAK.
    pub fn retransmit_all_data(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        // BTreeMap iterates in key order, i.e. oldest block first.
        for (&id, pb) in &self.pending {
            for idx in 0..pb.k as usize {
                out.push(pb.datagram_flagged(id, idx, FLAG_RETRANSMIT));
            }
        }
        out
    }
}

/// One block being reassembled on the receiver.
struct RxBlock {
    k: usize,
    r: usize,
    shard_len: usize,
    /// Received bitmap: bit `i` set means shard `i` is present.
    mask: AtomicU32,
    /// Bitmap of shards whose first arrival was an ARQ retransmit (their
    /// original was dropped) - the wire-loss evidence for the estimator.
    retransmitted: u32,
    /// Bitmap of positions where the original (non-retransmit) shard arrived
    /// AFTER an ARQ retransmit had already filled the slot. A duplicate of an
    /// already-recovered shard is the D-SACK signal (RFC 2883): "significant
    /// reordering followed by a false (unnecessary) retransmission", so the
    /// shard was reordered (late), not lost, and the retransmit-counted loss
    /// was a false positive the estimator subtracts (reordering vs loss per
    /// RACK-TLP, RFC 8985).
    false_recovery: u32,
    shards: Vec<Option<Vec<u8>>>,
    decoded: bool,
}

impl RxBlock {
    fn new(k: usize, r: usize, shard_len: usize) -> Self {
        Self {
            k,
            r,
            shard_len,
            mask: AtomicU32::new(0),
            retransmitted: 0,
            false_recovery: 0,
            shards: vec![None; k + r],
            decoded: false,
        }
    }

    #[inline]
    fn count(&self) -> u32 {
        self.mask.load(Ordering::Relaxed).count_ones()
    }
}

/// Receiver side: reassembles blocks, FEC-recovers losses, emits items
/// in order, and produces ARQ feedback.
pub struct Decoder {
    window: BTreeMap<u32, RxBlock>,
    /// Next block id to deliver; everything below is delivered.
    next_deliver: AtomicU32,
    /// Highest block id seen, for stall detection.
    highest_seen: u32,
    /// Highest DATA block fully decoded. Genuine gaps (blocks needing a
    /// retransmit) sit only below this: a later block fully arrived, so
    /// the missing one's shards are lost, not in flight. On a clean link
    /// this tracks the delivery frontier, so the selective-NAK gap scan is
    /// empty - that cost is paid only under real loss, not every poll.
    highest_decoded: u32,
    /// Rolling loss accounting.
    total_expected: u64,
    total_missing: u64,
    /// Highest loss estimate (0..=255) reached over the receiver's lifetime.
    /// Diagnostics for the reordering guard.
    peak_loss: u8,
    /// Lifetime count of D-SACK false recoveries detected: spurious
    /// retransmissions whose reordered original later arrived, which the guard
    /// excludes from the loss estimate. A nonzero value on a reorder-carrying
    /// link is the guard firing on real reordered traffic. Diagnostics.
    false_recoveries: u64,
    /// Max blocks retained before forcing progress / NAK.
    window_cap: usize,
    /// Timing estimator fed by sender heartbeats (OWD trend, jitter).
    temporal: TemporalSensor,
    /// Loss differentiator (congestion vs wireless): fed shard inter-arrivals
    /// and heartbeat ROTT, consulted when a block delivers with loss.
    loss_class: LossClassSensor,
    /// Gilbert-Elliott burst-loss fit: fed each delivered block's per-shard
    /// original-loss trace, it yields a REAL mean burst length. When
    /// `use_ge_burst` is set the reported burstiness is derived from it
    /// (interleave at least the mean burst), instead of the jitter-ratio
    /// heuristic - the A/B knob.
    burst_model: crate::burst_model_sensor::BurstModel,
    use_ge_burst: bool,
    /// Receiver-clock microseconds of the previous data-shard arrival, for the
    /// inter-arrival the loss differentiator's Biaz test needs (`None` until a
    /// timestamped shard arrives via [`Decoder::on_packet_at`]).
    last_data_recv_us: Option<u64>,
    /// Most recent data-shard inter-arrival (microseconds), classified against
    /// the loss gap when a block delivers.
    last_interarrival_us: f64,
    /// Tower segment structure, learned from outer block ids (`0` until
    /// the first outer block arrives).
    tower_d: usize,
    tower_r_outer: usize,
    /// Inner block geometry, learned from received data blocks.
    inner_k: usize,
    inner_shard_len: usize,
    /// Decoded data-block infos (k data shards concatenated), kept for
    /// tower recovery until delivered.
    data_infos: BTreeMap<u32, Vec<u8>>,
    /// Reassembly buffers for in-flight outer-parity blocks.
    outer_rx: BTreeMap<u32, RxBlock>,
    /// Recovered outer infos per segment: `seg_id -> (outer_idx -> info)`.
    seg_outer: BTreeMap<u32, BTreeMap<u32, Vec<u8>>>,
    /// Actual data-block count per segment (a partial final segment has
    /// fewer than `tower_d`).
    seg_d: BTreeMap<u32, usize>,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    /// Create a receiver with a default 256-block reassembly window -
    /// deep enough to keep the wire full across the ack round-trip while a
    /// gap recovers in the background (the sender pipelines new blocks and
    /// the receiver buffers them out of order, draining in order once the
    /// gap is recovered).
    pub fn new() -> Self {
        Self::with_window(256)
    }

    /// Create a receiver bounding the reassembly window to `window_cap`
    /// blocks. A sender should use a matching
    /// [`Encoder::with_flow_window`] so it never transmits beyond what
    /// the receiver will buffer.
    pub fn with_window(window_cap: usize) -> Self {
        Self {
            window: BTreeMap::new(),
            next_deliver: AtomicU32::new(0),
            highest_seen: 0,
            highest_decoded: 0,
            total_expected: 0,
            total_missing: 0,
            peak_loss: 0,
            false_recoveries: 0,
            window_cap: window_cap.max(1),
            temporal: TemporalSensor::default(),
            loss_class: LossClassSensor::new(),
            burst_model: crate::burst_model_sensor::BurstModel::new(),
            use_ge_burst: false,
            last_data_recv_us: None,
            last_interarrival_us: 0.0,
            tower_d: 0,
            tower_r_outer: 0,
            inner_k: 0,
            inner_shard_len: 0,
            data_infos: BTreeMap::new(),
            outer_rx: BTreeMap::new(),
            seg_outer: BTreeMap::new(),
            seg_d: BTreeMap::new(),
        }
    }

    /// The configured reassembly-window bound, in blocks.
    pub fn window_cap(&self) -> usize {
        self.window_cap
    }

    /// Feed a sender heartbeat's `(send_ts, recv_ts)` pair (microseconds)
    /// to the timing estimator, so the next feedback reports the OWD
    /// trend and jitter-derived burstiness.
    pub fn on_heartbeat(&mut self, send_ts: u64, recv_ts: u64) {
        self.temporal.observe(send_ts, recv_ts);
        // The relative one-way trip time (clock offset cancels in the Spike
        // min/max range) feeds the loss differentiator's Spike (ROTT) input.
        self.loss_class.observe_owd(recv_ts as f64 - send_ts as f64);
    }

    /// Current OWD trend slope from the timing estimator (raw, skew-inclusive).
    pub fn owd_trend(&self) -> f64 {
        self.temporal.owd_trend()
    }

    /// Estimated clock skew (the Moon-Skelly-Towsley lower-hull slope) and the
    /// skew-corrected OWD trend the controller actually consumes (telemetry).
    pub fn owd_skew(&self) -> f64 {
        self.temporal.skew()
    }

    pub fn owd_trend_debiased(&self) -> f64 {
        self.temporal.owd_trend_debiased()
    }

    /// Highest loss estimate (0..=255) the receiver has reached (telemetry).
    pub fn peak_loss_x255(&self) -> u8 {
        self.peak_loss
    }

    /// Drive the reported burstiness from the Gilbert-Elliott burst model (a
    /// real mean burst length) instead of the jitter-ratio heuristic - the A/B
    /// knob for confirming the model beats the heuristic at sizing interleave.
    pub fn set_ge_burst(&mut self, on: bool) {
        self.use_ge_burst = on;
    }

    /// Fitted mean burst length (consecutive lost shards) from the
    /// Gilbert-Elliott model, or -1 before the fit converges (telemetry / A/B).
    pub fn mean_burst_len(&self) -> f32 {
        self.burst_model.mean_burst_len().map(|m| m as f32).unwrap_or(-1.0)
    }

    /// Lifetime count of D-SACK false recoveries the reordering guard detected:
    /// spurious retransmissions whose reordered original later arrived. Zero on
    /// a clean link; a nonzero value on a reorder-carrying link is the guard
    /// firing on real reordered traffic (RFC 2883 / RFC 8985).
    pub fn false_recovery_count(&self) -> u64 {
        self.false_recoveries
    }

    /// Block id the receiver next needs (everything below is delivered).
    pub fn next_needed(&self) -> u32 {
        self.next_deliver.load(Ordering::Relaxed)
    }

    /// Ingest one data datagram. Returns any items that became
    /// deliverable, in stream order. Non-data datagrams yield nothing.
    pub fn on_packet(&mut self, buf: &[u8]) -> Vec<Vec<u8>> {
        self.ingest(buf, None)
    }

    /// Like [`on_packet`](Self::on_packet) but with the datagram's receiver-
    /// clock arrival time (microseconds), which feeds the loss differentiator's
    /// inter-arrival (Biaz) input. The socket layer supplies it; callers that do
    /// not time arrivals use [`on_packet`](Self::on_packet) and the
    /// differentiator falls back to its Spike (ROTT) signal alone.
    pub fn on_packet_at(&mut self, buf: &[u8], recv_us: u64) -> Vec<Vec<u8>> {
        self.ingest(buf, Some(recv_us))
    }

    fn ingest(&mut self, buf: &[u8], recv_us: Option<u64>) -> Vec<Vec<u8>> {
        if !is_data(buf) || buf.len() < DATA_HEADER {
            return Vec::new();
        }
        let block_id = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
        let shard_index = buf[5] as usize;
        let k = buf[6] as usize;
        let r = buf[7] as usize;
        let is_retransmit = buf[8] & FLAG_RETRANSMIT != 0;
        let payload = &buf[DATA_HEADER..];
        // r == 0 is the Passthrough block: k data shards, no parity. It is a
        // valid shape (the block completes when all k data shards arrive, via
        // ARQ if any drop), so it is NOT rejected here.
        if k == 0 || k + r > MAX_SHARDS || shard_index >= k + r {
            return Vec::new();
        }
        // Tower outer-parity blocks live in a separate id space; they are
        // handled opportunistically to recover whole-lost data blocks.
        if block_id & OUTER_ID_BIT != 0 {
            self.handle_outer(block_id, shard_index, k, r, payload);
            return self.drain_in_order();
        }
        // A timestamped DATA-shard arrival feeds the loss differentiator's
        // inter-arrival input (Biaz `T_min` / `T_i`). Outer-parity shards are
        // excluded above, so this is the data-stream spacing the LDA expects.
        if let Some(now) = recv_us {
            if let Some(prev) = self.last_data_recv_us {
                let ia = now.wrapping_sub(prev) as f64;
                self.last_interarrival_us = ia;
                self.loss_class.observe_interarrival(ia);
            }
            self.last_data_recv_us = Some(now);
        }
        if self.inner_k == 0 {
            self.inner_k = k;
            self.inner_shard_len = payload.len();
        }
        // Ignore packets for already-delivered blocks (duplicates /
        // late ARQ).
        if block_id < self.next_deliver.load(Ordering::Relaxed) {
            return Vec::new();
        }
        // Bound the reassembly window: refuse blocks too far ahead of
        // the delivery frontier. The sender's flow window
        // ([`Encoder::in_flight`]) keeps it from outrunning this, so in
        // correct operation this guard only fires under a bug or a
        // hostile peer - it caps memory either way.
        let next = self.next_deliver.load(Ordering::Relaxed);
        if block_id >= next.saturating_add(self.window_cap as u32) {
            return Vec::new();
        }
        if block_id > self.highest_seen {
            self.highest_seen = block_id;
        }
        let shard_len = payload.len();
        let blk = self
            .window
            .entry(block_id)
            .or_insert_with(|| RxBlock::new(k, r, shard_len));
        // FEC operates symbol-wise across equal-length shards; reject a
        // packet whose shape disagrees with the block it joins.
        if blk.shard_len != shard_len || blk.k != k || blk.r != r {
            return self.drain_in_order();
        }
        let bit = 1u32 << shard_index;
        if blk.mask.load(Ordering::Relaxed) & bit == 0 {
            blk.mask.fetch_or(bit, Ordering::Relaxed);
            blk.shards[shard_index] = Some(payload.to_vec());
            // First arrival via ARQ retransmit: its original was dropped, so
            // record it as wire loss for the estimator (otherwise a drop that
            // ARQ recovered at Passthrough would be invisible).
            if is_retransmit {
                blk.retransmitted |= bit;
            }
        } else if !is_retransmit && (blk.retransmitted & bit) != 0 {
            // The original arrives AFTER its ARQ retransmit already filled this
            // slot - a duplicate of an already-recovered shard. That is the
            // D-SACK signal (RFC 2883): reordering followed by a spurious
            // retransmission, NOT a loss. Mark it so the estimator discounts
            // the retransmit it counted. The slot keeps the retransmit's bytes
            // (identical to the original), so delivery is unchanged.
            blk.false_recovery |= bit;
        }
        // FEC-decode as soon as k of k+r shards are present.
        let mut decoded_info: Option<Vec<u8>> = None;
        if !blk.decoded && blk.count() as usize >= blk.k {
            // r == 0 is Passthrough: no parity to recover from, so the block
            // is complete exactly when all k data shards have arrived (ARQ
            // fills any gap before count reaches k). r > 0 uses RS erasure
            // decoding to rebuild missing shards from parity.
            let recovered = if blk.r == 0 {
                (0..blk.k).all(|i| blk.shards[i].is_some())
            } else {
                RsCode::new(blk.k, blk.r)
                    .expect("valid k,r")
                    .decode(&mut blk.shards)
                    .is_ok()
            };
            if recovered {
                blk.decoded = true;
                // Concatenate the k data shards into the block info with one
                // allocation and k memcpys (extend_from_slice), not a clone
                // of each shard plus a byte-by-byte flatten - this is the
                // receiver's hottest per-block path.
                let mut info = Vec::with_capacity(blk.k * blk.shard_len);
                for i in 0..blk.k {
                    if let Some(s) = &blk.shards[i] {
                        info.extend_from_slice(s);
                    }
                }
                decoded_info = Some(info);
            }
        }
        // Keep every decoded block's info available for tower recovery of
        // a neighbor in the same segment (bounded to the window by the
        // prune in `drain_in_order`).
        if let Some(info) = decoded_info {
            self.data_infos.insert(block_id, info);
            self.highest_decoded = self.highest_decoded.max(block_id);
        }
        self.drain_in_order()
    }

    /// Reassemble an outer-parity block; on inner-decode, record its info
    /// for the segment so a whole-lost data block can be reconstructed.
    fn handle_outer(&mut self, oid: u32, shard_index: usize, k: usize, r: usize, payload: &[u8]) {
        let d = ((oid >> 27) & 0xF) as usize;
        let r_outer = ((oid >> 24) & 0x7) as usize;
        let seg_id = (oid >> 8) & 0xFFFF;
        let oidx = oid & 0xFF;
        if d == 0 || r_outer == 0 {
            return;
        }
        // `tower_d` tracks the FULL segment size (for segment-id math);
        // `seg_d` records this segment's actual data-block count, which is
        // smaller for the final partial segment.
        self.tower_d = d.max(self.tower_d);
        self.tower_r_outer = r_outer;
        self.seg_d.insert(seg_id, d);
        let shard_len = payload.len();
        let blk = self
            .outer_rx
            .entry(oid)
            .or_insert_with(|| RxBlock::new(k, r, shard_len));
        if blk.shard_len != shard_len || blk.k != k || blk.r != r {
            return;
        }
        let bit = 1u32 << shard_index;
        if blk.mask.load(Ordering::Relaxed) & bit == 0 {
            blk.mask.fetch_or(bit, Ordering::Relaxed);
            blk.shards[shard_index] = Some(payload.to_vec());
        }
        if !blk.decoded && blk.count() as usize >= blk.k {
            let code = RsCode::new(blk.k, blk.r).expect("valid k,r");
            if code.decode(&mut blk.shards).is_ok() {
                blk.decoded = true;
                let mut info = Vec::with_capacity(blk.k * blk.shard_len);
                for i in 0..blk.k {
                    if let Some(s) = &blk.shards[i] {
                        info.extend_from_slice(s);
                    }
                }
                self.outer_rx.remove(&oid);
                self.seg_outer.entry(seg_id).or_default().insert(oidx, info);
            }
        }
    }

    /// Attempt to reconstruct a whole-lost data block from its segment's
    /// surviving blocks plus outer parity. On success, inserts a decoded
    /// block into the window so [`drain_in_order`] delivers it. Returns
    /// `true` if the block was recovered.
    fn try_tower_recover(&mut self, block_id: u32) -> bool {
        let big_d = self.tower_d;
        let r_outer = self.tower_r_outer;
        if big_d == 0 || r_outer == 0 || self.inner_k == 0 {
            return false;
        }
        // Segment id / base use the full segment size; the segment's
        // actual data-block count may be smaller (partial final segment).
        let seg_id = block_id / big_d as u32;
        let base = seg_id * big_d as u32;
        let d = match self.seg_d.get(&seg_id) {
            Some(&d) => d,
            None => return false,
        };
        let idx_in_seg = (block_id - base) as usize;
        if idx_in_seg >= d {
            return false;
        }
        let outers = match self.seg_outer.get(&seg_id) {
            Some(m) => m,
            None => return false,
        };
        // Gather the d data infos and the r_outer outer infos.
        let mut blocks: Vec<Option<Vec<u8>>> = Vec::with_capacity(d + r_outer);
        for i in 0..d {
            blocks.push(self.data_infos.get(&(base + i as u32)).cloned());
        }
        for j in 0..r_outer {
            blocks.push(outers.get(&(j as u32)).cloned());
        }
        if blocks.iter().filter(|b| b.is_some()).count() < d {
            return false;
        }
        let code = match SegmentCode::new(d, r_outer) {
            Ok(c) => c,
            Err(_) => return false,
        };
        if code.decode(&mut blocks).is_err() {
            return false;
        }
        let info = match blocks[idx_in_seg].take() {
            Some(v) => v,
            None => return false,
        };
        // Split the recovered info back into k data shards and inject a
        // ready-to-deliver block.
        let k = self.inner_k;
        let shard_len = self.inner_shard_len.max(1);
        if info.len() != k * shard_len {
            return false;
        }
        let mut rb = RxBlock::new(k, 0, shard_len);
        for i in 0..k {
            rb.shards[i] = Some(info[i * shard_len..(i + 1) * shard_len].to_vec());
            rb.mask.fetch_or(1u32 << i, Ordering::Relaxed);
        }
        rb.decoded = true;
        self.data_infos.insert(block_id, info);
        self.highest_decoded = self.highest_decoded.max(block_id);
        self.window.insert(block_id, rb);
        true
    }

    /// Deliver every contiguous decoded block starting at
    /// `next_deliver`.
    fn drain_in_order(&mut self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let id = self.next_deliver.load(Ordering::Relaxed);
            let ready = matches!(self.window.get(&id), Some(b) if b.decoded);
            if !ready {
                // Head block missing or undecoded: try tower recovery
                // (reconstruct it from its segment's outer parity) before
                // stalling. ARQ remains the fallback if this fails.
                if !self.window.contains_key(&id) && self.try_tower_recover(id) {
                    continue;
                }
                break;
            }
            let blk = self.window.remove(&id).unwrap();
            // Loss = data shards that did NOT arrive directly and had to be
            // recovered: FEC-reconstructed (a data position never received, so
            // absent from the mask) plus ARQ-retransmitted (received, but only
            // after its original dropped). Parity shards are redundancy, not
            // loss, so they are excluded - counting them made a clean link read
            // as r/(k+r) loss and pinned FEC on. The counters decay per block
            // (~32-block window) so the estimate follows the CURRENT link and
            // falls back to zero - and the controller back to Passthrough -
            // once loss clears.
            let data_mask: u32 = if blk.k >= 32 { u32::MAX } else { (1u32 << blk.k) - 1 };
            let data_present = (blk.mask.load(Ordering::Relaxed) & data_mask).count_ones() as u64;
            let fec_recovered = (blk.k as u64).saturating_sub(data_present);
            // A retransmit whose original later arrived (false_recovery) was a
            // spurious retransmission from reordering, not a drop; exclude it
            // so reordering does not inflate the estimate and needlessly arm
            // FEC (RACK-TLP reordering-vs-loss, RFC 8985). A retransmit with no
            // late original is a genuine loss and still counts. The guard's
            // subtraction is the A/B knob; the baseline counts every retransmit.
            let arq_recovered = if reorder_guard_enabled() {
                (blk.retransmitted & !blk.false_recovery & data_mask).count_ones() as u64
            } else {
                (blk.retransmitted & data_mask).count_ones() as u64
            };
            // Count the D-SACK false recoveries this block carried (the guard
            // firing on real reordered traffic), whether or not the subtraction
            // knob is on, so the count reflects detection on the wire.
            self.false_recoveries += (blk.false_recovery & data_mask).count_ones() as u64;
            self.total_expected = (self.total_expected * 31 / 32) + blk.k as u64;
            self.total_missing = (self.total_missing * 31 / 32) + fec_recovered + arq_recovered;
            // Differentiate this block's loss congestion-vs-wireless (Biaz +
            // Spike hybrid) so the sender treats the two regimes differently.
            // The gap is the real lost-shard count (false recoveries already
            // excluded from arq_recovered above).
            let gap = (fec_recovered + arq_recovered) as u32;
            if gap > 0 {
                let ia = self.last_interarrival_us;
                self.loss_class.classify(gap, ia);
            }
            // Feed the Gilbert-Elliott burst model the block's per-shard
            // original-loss trace in shard order: a shard received on its first
            // transmission is `mask & !retransmitted`; everything else (FEC-
            // reconstructed or ARQ-retried) was originally lost. At interleave
            // depth 1 this is the wire loss order, so the fit sees the native
            // burst structure.
            let first_tx = blk.mask.load(Ordering::Relaxed) & !blk.retransmitted;
            for i in 0..(blk.k + blk.r) {
                self.burst_model.observe(first_tx & (1u32 << i) == 0);
            }
            // Track the peak loss estimate (telemetry).
            let cur_loss = self
                .total_missing
                .saturating_mul(255)
                .checked_div(self.total_expected)
                .unwrap_or(0)
                .min(255) as u8;
            self.peak_loss = self.peak_loss.max(cur_loss);
            for i in 0..blk.k {
                let shard = blk.shards[i].as_ref().expect("decoded data shard");
                let item_len =
                    u16::from_le_bytes([shard[0], shard[1]]) as usize;
                if item_len > 0 {
                    let end = (ITEM_LEN_PREFIX + item_len).min(shard.len());
                    out.push(shard[ITEM_LEN_PREFIX..end].to_vec());
                }
            }
            self.next_deliver.store(id + 1, Ordering::Relaxed);
        }
        // Bound bookkeeping to the reassembly window.
        let nd = self.next_deliver.load(Ordering::Relaxed);
        let keep_from = nd.saturating_sub(self.window_cap as u32);
        self.data_infos.retain(|&id, _| id >= keep_from);
        if self.tower_d > 0 {
            let keep_seg = (keep_from / self.tower_d as u32).saturating_sub(1);
            self.seg_outer.retain(|&s, _| s >= keep_seg);
            self.seg_d.retain(|&s, _| s >= keep_seg);
            self.outer_rx
                .retain(|&oid, _| ((oid >> 8) & 0xFFFF) >= keep_seg);
        }
        out
    }

    /// Produce a feedback packet: always an ACK of the delivery
    /// frontier, plus a NAK for the oldest stalled block.
    ///
    /// `drive_arq` requests an unconditional NAK of the head block when
    /// it is present but undecoded. A receiver sets it on a recv timeout
    /// (no fresh data) so the LAST block - which has no newer block to
    /// trigger a NAK - still recovers from tail loss. With `drive_arq`
    /// false the NAK only fires once a newer block has arrived, which
    /// avoids NAKing a block whose shards may still be in flight.
    pub fn feedback(&self, drive_arq: bool) -> Feedback {
        let ack_through = self.next_deliver.load(Ordering::Relaxed);
        let (mut nak_block, mut nak_mask) = (NAK_NONE, 0u32);
        // The block we are waiting on is `ack_through`. We chase it once
        // it is overdue: a newer block arrived, or the caller is draining
        // a stalled tail.
        let overdue = drive_arq || self.highest_seen > ack_through;
        if overdue {
            match self.window.get(&ack_through) {
                // Partially received: NAK only the missing shards.
                Some(blk) if !blk.decoded => {
                    let present = blk.mask.load(Ordering::Relaxed);
                    let full = if blk.k + blk.r >= 32 {
                        u32::MAX
                    } else {
                        (1u32 << (blk.k + blk.r)) - 1
                    };
                    nak_block = ack_through;
                    nak_mask = full & !present;
                }
                // Entirely missing (zero shards) while later blocks have
                // arrived OR the caller is draining the tail: request ALL
                // of its shards. The sender clamps the mask to the
                // block's real shard count (and ignores a block it does
                // not hold). Without this, a head or tail block that
                // loses every shard can never be re-requested and
                // delivery deadlocks.
                None => {
                    nak_block = ack_through;
                    nak_mask = u32::MAX;
                }
                _ => {}
            }
        }
        let loss = self
            .total_missing
            .saturating_mul(255)
            .checked_div(self.total_expected)
            .unwrap_or(0)
            .min(255) as u8;
        // Burstiness proxy: jitter relative to the mean inter-arrival.
        // Steady spacing -> ~0; clustered arrivals (bursts) -> toward 1.
        let mean_ia = self.temporal.interarrival_micros().max(1.0);
        let heuristic = (self.temporal.jitter_micros() / mean_ia).clamp(0.0, 1.0);
        // With the Gilbert-Elliott model enabled, derive burstiness from the
        // REAL mean burst length (`mean_burst / 16` maps through the sender's
        // interleave mapping `depth = burstiness * 16` to `depth = mean_burst`),
        // falling back to the jitter heuristic until the fit converges.
        let burstiness = if self.use_ge_burst {
            self.burst_model
                .mean_burst_len()
                .map(|mb| (mb / 16.0).clamp(0.0, 1.0))
                .unwrap_or(heuristic)
        } else {
            heuristic
        };
        // Clock-skew-corrected: a relative clock drift makes the raw OWD slope
        // read a false rising / falling trend; the skew estimate removes it, so
        // only genuine queueing reaches the controller.
        let trend = self.temporal.owd_trend_debiased();
        let owd_trend_class = if trend > 0.02 {
            2
        } else if trend < -0.02 {
            0
        } else {
            1
        };
        Feedback {
            ack_through,
            nak_block,
            nak_mask,
            loss_x255: loss,
            burstiness_x255: (burstiness * 255.0) as u8,
            owd_trend_class,
            loss_class: self.loss_class.class_code(),
        }
    }

    /// Enumerate EVERY gap the reassembly window is holding, as
    /// `(block_id, missing_shard_mask)`, so a caller can NAK them all in
    /// one feedback cycle instead of one-gap-per-round-trip serial
    /// recovery. A block received in part returns its still-missing shards;
    /// a block not seen at all returns `u32::MAX` (the sender clamps the
    /// mask to the block's real shard count). Gaps strictly below
    /// `highest_seen` are always overdue - a later block has arrived, so
    /// this one's shards are lost, not merely in flight. The block AT
    /// `highest_seen` (the tail) is included only when `drive_tail` is set,
    /// matching [`feedback`](Self::feedback)'s single-NAK overdue rule: the
    /// tail has no newer block to prove its shards should have arrived, so
    /// it is NAK'd only on a recv-timeout drain. The drain ALSO re-requests
    /// the head block when `next_deliver` has advanced AT OR ABOVE
    /// `highest_seen` - the case where every shard of the next expected
    /// (tail) block was lost, so it was never "seen" and sits above the
    /// `[next_deliver, highest_seen)` sweep. Without that, delivery
    /// deadlocks on a tail block whose whole datagrams were dropped. At
    /// most `max` gaps are returned (nearest the delivery frontier first),
    /// bounding the feedback burst; the rest are picked up on the next
    /// cycle.
    pub fn missing_blocks(&self, max: usize, drive_tail: bool) -> Vec<(u32, u32)> {
        let nd = self.next_deliver.load(Ordering::Relaxed);
        let hi = self.highest_seen;
        let mut gaps = Vec::new();
        let mut id = nd;
        // Genuine gaps sit only below the highest DECODED block: a later
        // block fully arrived, proving this one's shards are lost rather
        // than still in flight. On a clean link `highest_decoded` tracks
        // the delivery frontier, so this loop does nothing - the O(window)
        // scan that dominated the receiver is now paid only under real
        // loss, not on every poll.
        while id <= self.highest_decoded && gaps.len() < max {
            self.push_gap(id, &mut gaps);
            id = id.saturating_add(1);
        }
        // Under a drain, chase the block we are BLOCKED on: the tail at
        // `highest_seen` (nd <= hi), or the never-seen head above it
        // (nd > hi, every shard of the tail block lost). `nd.max(hi)`
        // selects whichever it is; a fully-lost tail block returns
        // `u32::MAX` (request all shards) so it cannot deadlock delivery.
        if drive_tail && gaps.len() < max {
            self.push_gap(nd.max(hi), &mut gaps);
        }
        gaps
    }

    /// Append `(block_id, missing_mask)` to `gaps` if `block_id` is a gap
    /// (received-but-undecoded, or entirely unseen). A decoded block is
    /// not a gap and is skipped.
    fn push_gap(&self, id: u32, gaps: &mut Vec<(u32, u32)>) {
        match self.window.get(&id) {
            Some(blk) if !blk.decoded => {
                let present = blk.mask.load(Ordering::Relaxed);
                let full = if blk.k + blk.r >= 32 {
                    u32::MAX
                } else {
                    (1u32 << (blk.k + blk.r)) - 1
                };
                gaps.push((id, full & !present));
            }
            None => gaps.push((id, u32::MAX)),
            _ => {}
        }
    }

    /// Blocks currently held in the reassembly window.
    pub fn window_len(&self) -> usize {
        self.window.len()
    }

    /// Give up on the current head block (a gap held past its recovery
    /// deadline) and advance delivery past it, returning any items that
    /// become deliverable. This is the partial-reliability escape hatch:
    /// it skips an unrecoverable gap so the stream is not blocked forever,
    /// at the cost of those items. The caller decides the deadline; the
    /// transport holds the gap and recovers it via FEC/ARQ until then.
    pub fn skip_head(&mut self) -> Vec<Vec<u8>> {
        let id = self.next_deliver.load(Ordering::Relaxed);
        self.window.remove(&id);
        self.data_infos.remove(&id);
        self.next_deliver.store(id + 1, Ordering::Relaxed);
        self.drain_in_order()
    }

    /// Diagnostic snapshot of the block currently blocking in-order
    /// delivery: `(block_id, received_shards, k, decoded)`, or `None`
    /// when that block has not been seen at all (no shard received yet).
    pub fn head_status(&self) -> Option<(u32, u32, usize, bool)> {
        let id = self.next_deliver.load(Ordering::Relaxed);
        self.window
            .get(&id)
            .map(|b| (id, b.count(), b.k, b.decoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-block adaptive shard length: a block of small items ships
    /// datagrams sized to the item, not to `max_item`, so schema
    /// compression actually reaches the wire. A block sizes to its largest
    /// member, and all shards of a block are equal length (the FEC matrix
    /// requires it). The decoder reads each block's length from the
    /// datagram size, so no header field is added.
    #[test]
    fn per_block_shard_len_sizes_datagrams_to_items() {
        // Generous max_item; small items must NOT be padded up to it.
        let mut enc = Encoder::new(8, 2, 256);
        let mut dgrams = Vec::new();
        for _ in 0..8 {
            dgrams.extend(enc.push(&[7u8; 38]));
        }
        assert_eq!(dgrams.len(), 10, "k+r datagrams per block");
        let dlen = dgrams[0].len();
        assert_eq!(
            dlen,
            DATA_HEADER + ITEM_LEN_PREFIX + 38,
            "datagram sized to the 38B item, not max_item(256)"
        );
        assert!(
            dgrams.iter().all(|d| d.len() == dlen),
            "all shards of a block are equal length"
        );

        // A block of larger items ships proportionally larger datagrams.
        let mut enc2 = Encoder::new(8, 2, 256);
        let mut big = Vec::new();
        for _ in 0..8 {
            big.extend(enc2.push(&[9u8; 200]));
        }
        assert_eq!(big[0].len(), DATA_HEADER + ITEM_LEN_PREFIX + 200);
        assert!(big[0].len() > dlen, "bigger items ship bigger datagrams");

        // A mixed-size block sizes to its largest member.
        let mut enc3 = Encoder::new(8, 2, 256);
        let mut mixed = Vec::new();
        for n in [10usize, 50, 20, 40, 30, 12, 8, 25] {
            mixed.extend(enc3.push(&vec![1u8; n]));
        }
        assert_eq!(
            mixed[0].len(),
            DATA_HEADER + ITEM_LEN_PREFIX + 50,
            "block sizes to its 50B max member"
        );
    }

    /// Deterministic LCG so loss / reorder patterns are reproducible.
    struct Lcg(u64);
    impl Lcg {
        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) as u32
        }
        /// `true` with probability `pct/100`.
        fn drop(&mut self, pct: u32) -> bool {
            self.next_u32() % 100 < pct
        }
    }

    /// Drive `n` items end-to-end through a channel that drops `loss_pct`
    /// of DATA datagrams, with ARQ feedback flowing back. Asserts every
    /// item is delivered exactly once, in order.
    fn round_trip(n: usize, k: usize, r: usize, loss_pct: u32, seed: u64) {
        let mut enc = Encoder::new(k, r, 8);
        let mut dec = Decoder::new();
        let mut rng = Lcg(seed);
        let mut delivered: Vec<u64> = Vec::new();

        // Outstanding datagrams from sender to receiver.
        let mut wire: Vec<Vec<u8>> = Vec::new();
        let send = |wire: &mut Vec<Vec<u8>>, pkts: Vec<Vec<u8>>| wire.extend(pkts);

        for i in 0..n as u64 {
            send(&mut wire, enc.push(&i.to_le_bytes()));
        }
        send(&mut wire, enc.flush());

        // Pump: deliver (lossily) sender->receiver, feed feedback back,
        // until the receiver has everything or we give up.
        let mut rounds = 0;
        while delivered.len() < n {
            rounds += 1;
            assert!(rounds < 10_000, "no convergence: {} / {n}", delivered.len());
            let batch = std::mem::take(&mut wire);
            for pkt in batch {
                if rng.drop(loss_pct) {
                    continue; // packet lost on the wire
                }
                for item in dec.on_packet(&pkt) {
                    delivered.push(u64::from_le_bytes(item.try_into().unwrap()));
                }
            }
            // Receiver feedback -> sender (feedback never lost here, so
            // ARQ can always make progress; FEC handles the data loss).
            // Each pump drives ARQ so a stalled tail is re-requested.
            let fb = dec.feedback(true);
            send(&mut wire, enc.on_feedback(&fb));
            if wire.is_empty() && delivered.len() < n {
                // Nothing in flight but still missing: re-request.
                let fb = dec.feedback(true);
                send(&mut wire, enc.on_feedback(&fb));
                if wire.is_empty() {
                    panic!("stalled with {} / {n} delivered", delivered.len());
                }
            }
        }
        let expected: Vec<u64> = (0..n as u64).collect();
        assert_eq!(delivered, expected, "ordered exactly-once delivery");
    }

    #[test]
    fn clean_channel_delivers_all() {
        round_trip(100, 8, 2, 0, 1);
    }

    // k + r must be <= MAX_SHARDS (32): the per-block received bitmap is a u32,
    // and `1 << idx` for idx >= 32 overflows (a panic in debug, a wrapped mask in
    // release -> blocks never complete). k=16 leaves room for r up to 16 (50%
    // redundancy), enough for the extreme-loss regime the crossover targets.
    #[test]
    fn rs_k16_r8_clean() {
        round_trip(100, 16, 8, 0, 1);
    }

    #[test]
    fn rs_k16_r16_clean() {
        round_trip(100, 16, 16, 0, 1);
    }

    #[test]
    fn rs_k16_r8_loss30() {
        round_trip(2000, 16, 8, 30, 7);
    }

    // k == MAX_SHARDS leaves no room for parity: the encoder must clamp r to 0
    // (Passthrough, ARQ-only) rather than emit k + r = 33 shards, which would
    // overflow the u32 bitmap (`1 << 32`). Before the r_max fix this panicked /
    // stalled; now the clean link delivers via the ARQ floor.
    #[test]
    fn rs_k32_clamps_to_passthrough() {
        round_trip(100, 32, 5, 0, 1);
    }

    #[test]
    fn fec_recovers_light_loss_without_arq() {
        // ~10% loss with r=3 over k=8 is within FEC budget most blocks;
        // delivery must still be exact.
        round_trip(200, 8, 3, 10, 7);
    }

    #[test]
    fn arq_recovers_heavy_loss() {
        // 35% loss exceeds any sane parity budget on many blocks; ARQ
        // must carry the rest.
        round_trip(150, 8, 2, 35, 42);
    }

    #[test]
    fn tiny_blocks_and_flush() {
        // n not a multiple of k exercises the padded final block.
        round_trip(5, 4, 2, 0, 3);
        round_trip(13, 8, 2, 15, 99);
    }

    #[test]
    fn heartbeat_feeds_owd_trend() {
        // A genuinely building queue must push the reported trend class to
        // "rising" (2). It climbs but dips to a flat baseline periodically -
        // a CLEAN linear rise would be indistinguishable from clock skew and
        // is removed by the skew correction, so the queue must touch baseline.
        let mut dec = Decoder::new();
        for i in 0..40u64 {
            let send = i * 1000;
            let queue = if i % 4 == 0 { 0 } else { i * 60 };
            let recv = send + 5000 + queue;
            dec.on_heartbeat(send, recv);
        }
        assert!(dec.owd_trend() > 0.0);
        assert_eq!(dec.feedback(true).owd_trend_class, 2, "rising trend reported");
    }

    #[test]
    fn tail_loss_recovered_by_timeout_arq() {
        // Drop ALL parity (and one data shard) of the FINAL block - more
        // than r losses, and no newer block exists to trigger a NAK.
        // Only timeout-driven ARQ (`drive_arq`) can recover it.
        let k = 4;
        let r = 2;
        let mut enc = Encoder::new(k, r, 8);
        let mut dec = Decoder::new();
        let n = 4; // exactly one block
        let mut datagrams = Vec::new();
        for i in 0..n as u64 {
            datagrams.extend(enc.push(&i.to_le_bytes()));
        }
        datagrams.extend(enc.flush());
        // First pass: deliver only data shards 0,1,2 (drop shard 3 and
        // both parity) - block has 3 of 4, cannot FEC-decode.
        let mut delivered: Vec<u64> = Vec::new();
        for pkt in &datagrams {
            let idx = pkt[5];
            if idx <= 2 {
                for it in dec.on_packet(pkt) {
                    delivered.push(u64::from_le_bytes(it.try_into().unwrap()));
                }
            }
        }
        assert!(delivered.is_empty(), "block not yet recoverable");
        // No newer block: a non-driving feedback must NOT NAK.
        assert_eq!(dec.feedback(false).nak_block, u32::MAX);
        // Timeout-driven feedback NAKs the stalled head.
        let fb = dec.feedback(true);
        assert_eq!(fb.nak_block, 0);
        let arq = enc.on_feedback(&fb);
        assert!(!arq.is_empty(), "sender retransmits the missing shards");
        for pkt in &arq {
            for it in dec.on_packet(pkt) {
                delivered.push(u64::from_le_bytes(it.try_into().unwrap()));
            }
        }
        assert_eq!(delivered, vec![0, 1, 2, 3], "tail recovered via ARQ");
    }

    #[test]
    fn missing_head_block_recovered_by_whole_block_nak() {
        // A middle block that loses ALL its shards must still be
        // re-requested once a later block arrives, or delivery deadlocks
        // (the cross-host Direction-2 failure).
        let (k, r) = (4usize, 2usize);
        let mut enc = Encoder::new(k, r, 8);
        let mut dec = Decoder::new();
        let mut blocks: Vec<Vec<Vec<u8>>> = Vec::new();
        for i in 0..12u64 {
            let b = enc.push(&i.to_le_bytes());
            if !b.is_empty() {
                blocks.push(b);
            }
        }
        assert_eq!(blocks.len(), 3, "12 items / k=4 = 3 blocks");

        let mut delivered: Vec<u64> = Vec::new();
        let feed = |dec: &mut Decoder, pkts: &[Vec<u8>], out: &mut Vec<u64>| {
            for p in pkts {
                for it in dec.on_packet(p) {
                    out.push(u64::from_le_bytes(it.try_into().unwrap()));
                }
            }
        };
        // Deliver block 0, DROP all of block 1, deliver block 2.
        feed(&mut dec, &blocks[0], &mut delivered);
        feed(&mut dec, &blocks[2], &mut delivered);
        assert_eq!(delivered, vec![0, 1, 2, 3], "only block 0 deliverable");
        assert_eq!(dec.head_status(), None, "block 1 missing entirely");

        // Non-drive feedback must now request the whole missing block 1.
        let fb = dec.feedback(false);
        assert_eq!(fb.nak_block, 1);
        assert_eq!(fb.nak_mask, u32::MAX, "request all shards of the lost block");
        let rtx = enc.on_feedback(&fb);
        assert!(!rtx.is_empty(), "sender retransmits the whole block");
        feed(&mut dec, &rtx, &mut delivered);
        assert_eq!(delivered, (0..12).collect::<Vec<_>>(), "blocks 1 and 2 delivered");
    }

    #[test]
    fn fully_lost_tail_block_recovered_by_drain_nak() {
        // Whole-datagram loss at the TAIL via the selective-NAK path the
        // bridge uses (`missing_blocks`). Deliver block 0, then lose EVERY
        // shard of the final block 1: `next_deliver` advances to 1 while
        // `highest_seen` stays 0, so block 1 sits ABOVE the
        // [next_deliver, highest_seen) sweep. The drain must still
        // re-request it or delivery deadlocks on the tail - the cross-host
        // 30%-loss TIMEOUT this guards against.
        let (k, r) = (4usize, 2usize);
        let mut enc = Encoder::new(k, r, 8);
        let mut dec = Decoder::new();
        let mut blocks: Vec<Vec<Vec<u8>>> = Vec::new();
        for i in 0..8u64 {
            let b = enc.push(&i.to_le_bytes());
            if !b.is_empty() {
                blocks.push(b);
            }
        }
        assert_eq!(blocks.len(), 2, "8 items / k=4 = 2 blocks");

        let mut delivered: Vec<u64> = Vec::new();
        let feed = |dec: &mut Decoder, pkts: &[Vec<u8>], out: &mut Vec<u64>| {
            for p in pkts {
                for it in dec.on_packet(p) {
                    out.push(u64::from_le_bytes(it.try_into().unwrap()));
                }
            }
        };
        // Deliver block 0 fully; DROP every shard of the tail block 1.
        feed(&mut dec, &blocks[0], &mut delivered);
        assert_eq!(delivered, vec![0, 1, 2, 3], "block 0 delivered, tail unseen");

        // Without a drain the unseen tail is not chased (shards could still
        // be in flight); under a drain it MUST be re-requested in full.
        assert!(
            dec.missing_blocks(64, false).is_empty(),
            "no drain: unseen tail not yet re-requested"
        );
        assert_eq!(
            dec.missing_blocks(64, true),
            vec![(1, u32::MAX)],
            "drain re-requests the whole lost tail block"
        );

        // Sender retransmits block 1; delivery completes to the tail.
        let fb = Feedback {
            ack_through: 1,
            nak_block: 1,
            nak_mask: u32::MAX,
            loss_x255: 0,
            burstiness_x255: 0,
            owd_trend_class: 1,
            loss_class: 0,
        };
        let rtx = enc.on_feedback(&fb);
        assert!(!rtx.is_empty(), "sender retransmits the lost tail block");
        feed(&mut dec, &rtx, &mut delivered);
        assert_eq!(
            delivered,
            (0..8).collect::<Vec<_>>(),
            "tail recovered, all delivered"
        );
    }

    #[test]
    fn tower_recovers_whole_lost_block_without_arq() {
        // A whole data block is erased (every shard). With the tower on,
        // the receiver reconstructs it from the segment's surviving blocks
        // plus outer parity - no NAK, no ARQ, delivered straight from
        // on_packet.
        let (k, r) = (4usize, 2usize);
        let (d, r_outer) = (4usize, 2usize);
        let mut enc = Encoder::new(k, r, 8);
        enc.enable_tower(d, r_outer);
        let mut dec = Decoder::new();
        let n = (d * k) as u64; // one full segment
        let mut wire: Vec<Vec<u8>> = Vec::new();
        for i in 0..n {
            wire.extend(enc.push(&i.to_le_bytes()));
        }
        let mut delivered: Vec<u64> = Vec::new();
        for pkt in &wire {
            let bid = u32::from_le_bytes([pkt[1], pkt[2], pkt[3], pkt[4]]);
            let is_outer = bid & 0x8000_0000 != 0;
            // Erase the ENTIRE second data block (id 1).
            if !is_outer && bid == 1 {
                continue;
            }
            for it in dec.on_packet(pkt) {
                delivered.push(u64::from_le_bytes(it.try_into().unwrap()));
            }
        }
        assert_eq!(
            delivered,
            (0..n).collect::<Vec<_>>(),
            "tower reconstructed the whole-lost block with no ARQ"
        );
    }

    #[test]
    fn window_cap_bounds_far_ahead_blocks() {
        // A receiver with a 4-block window must refuse a block 10 ahead
        // of the delivery frontier (memory bound / backpressure).
        let mut dec = Decoder::with_window(4);
        let mut enc = Encoder::new(4, 1, 8);
        // Build block id 10 by sealing 10 blocks; keep only its packets.
        let mut far = Vec::new();
        for b in 0..=10u64 {
            let pkts = {
                let mut last = Vec::new();
                for i in 0..4u64 {
                    last = enc.push(&(b * 4 + i).to_le_bytes());
                }
                last
            };
            if b == 10 {
                far = pkts;
            }
        }
        assert!(!far.is_empty(), "sealed block 10");
        for pkt in &far {
            assert!(dec.on_packet(pkt).is_empty());
        }
        assert_eq!(dec.window_len(), 0, "block 10 refused by the 4-block window");
        assert_eq!(dec.window_cap(), 4);
    }

    #[test]
    fn flow_window_tracks_in_flight() {
        let mut enc = Encoder::new(8, 2, 8).with_flow_window(3);
        assert_eq!(enc.in_flight(), 0);
        for blk in 1..=4u64 {
            for i in 0..8u64 {
                enc.push(&(blk * 100 + i).to_le_bytes());
            }
            assert_eq!(enc.in_flight(), blk as u32);
        }
        assert!(enc.flow_blocked(), "4 in flight exceeds the 3-block window");
        // Receiver acks through block 3 (delivered 0,1,2): two remain.
        enc.on_feedback(&Feedback {
            ack_through: 3,
            nak_block: NAK_NONE,
            nak_mask: 0,
            loss_x255: 0,
            burstiness_x255: 0,
            owd_trend_class: 1,
            loss_class: 0,
        });
        assert_eq!(enc.in_flight(), 1);
        assert!(!enc.flow_blocked());
    }

    #[test]
    fn proactive_retransmit_resends_unacked_oldest_first() {
        let k = 8usize;
        let mut enc = Encoder::new(k, 2, 8);
        // Seal three blocks (0, 1, 2); none acked yet.
        for blk in 0..3u64 {
            for i in 0..k as u64 {
                enc.push(&(blk * 100 + i).to_le_bytes());
            }
        }
        assert_eq!(enc.pending_len(), 3);
        assert_eq!(
            enc.oldest_pending(),
            Some(0),
            "block 0 is the frontier the receiver needs first"
        );
        // A probe of one block is its k data shards, retransmit-flagged.
        let probe = enc.probe_block(0);
        assert_eq!(probe.len(), k, "probe is the k data shards of the block");
        assert!(
            probe[0][8] & FLAG_RETRANSMIT != 0,
            "probe datagrams are retransmit-flagged for the D-SACK path"
        );
        assert!(enc.probe_block(99).is_empty(), "no probe for an unknown / acked block");
        // The recovery burst is the k data shards of every pending block,
        // oldest-first.
        let burst = enc.retransmit_all_data();
        assert_eq!(burst.len(), 3 * k, "k data shards per pending block");
        let lead = u32::from_le_bytes([burst[0][1], burst[0][2], burst[0][3], burst[0][4]]);
        assert_eq!(lead, 0, "burst leads with the oldest unacked block");
        // After the receiver acks through block 1 (delivered block 0), the
        // burst shrinks to the still-unacked blocks.
        enc.on_feedback(&Feedback {
            ack_through: 1,
            nak_block: NAK_NONE,
            nak_mask: 0,
            loss_x255: 0,
            burstiness_x255: 0,
            owd_trend_class: 1,
            loss_class: 0,
        });
        assert_eq!(enc.oldest_pending(), Some(1));
        assert_eq!(
            enc.retransmit_all_data().len(),
            2 * k,
            "the acked block is dropped from the burst"
        );
    }

    #[test]
    fn parity_is_controller_driven_not_self_adapting() {
        let mut enc = Encoder::new(8, 1, 8);
        assert_eq!(enc.parity(), 1);
        // on_feedback must NOT change parity any more - that is the
        // fusion controller's job via set_parity.
        enc.on_feedback(&Feedback {
            ack_through: 0,
            nak_block: NAK_NONE,
            nak_mask: 0,
            loss_x255: (0.25 * 255.0) as u8,
            burstiness_x255: 0,
            owd_trend_class: 1,
            loss_class: 0,
        });
        assert_eq!(enc.parity(), 1, "feedback no longer self-adapts parity");
        enc.set_parity(3);
        assert_eq!(enc.parity(), 3, "controller sets parity");
        enc.set_parity(99);
        // r_max is now the bitmap ceiling MAX_SHARDS - k (k=8 -> 24), not the old
        // fixed 8, so a high-loss block can provision parity up to k + r = 32.
        assert_eq!(enc.parity(), MAX_SHARDS - 8, "clamped to r_max = MAX_SHARDS - k");
    }

    #[test]
    fn reordered_original_after_retransmit_excluded_from_loss() {
        // A shard reordered on the wire: its premature ARQ retransmit arrives
        // and fills the slot first, then the late original arrives. Receiving
        // the same shard twice is the D-SACK signal (RFC 2883) - reordering,
        // not loss - so the estimator must not count it.
        let mut enc = Encoder::new(4, 0, 8); // r=0 Passthrough: ARQ-only recovery
        let mut dec = Decoder::new();
        let mut dgrams = Vec::new();
        for i in 0..4u64 {
            dgrams.extend(enc.push(&i.to_le_bytes()));
        }
        assert_eq!(dgrams.len(), 4, "k=4 r=0 -> 4 data datagrams");

        // Shard 0 original.
        dec.on_packet(&dgrams[0]);
        // Shard 1 arrives FIRST as an ARQ retransmit (premature NAK), filling
        // the slot and counting as a wire loss.
        let mut rtx1 = dgrams[1].clone();
        rtx1[8] |= FLAG_RETRANSMIT;
        dec.on_packet(&rtx1);
        // The late ORIGINAL of shard 1 now arrives: the D-SACK duplicate.
        let out = dec.on_packet(&dgrams[1]);
        assert!(out.is_empty(), "block still incomplete (2 of 4)");
        // Complete the block with the remaining originals; it decodes/delivers.
        dec.on_packet(&dgrams[2]);
        let delivered = dec.on_packet(&dgrams[3]);
        let got: Vec<u64> = delivered
            .iter()
            .map(|it| u64::from_le_bytes(it.as_slice().try_into().unwrap()))
            .collect();
        assert_eq!(got, vec![0, 1, 2, 3], "in-order byte-exact delivery preserved");

        // The reordered shard's retransmit was a spurious retransmission, so
        // the loss estimate - and its running peak - stay at zero.
        assert_eq!(
            dec.feedback(false).loss_x255,
            0,
            "reordering not counted as loss"
        );
        assert_eq!(dec.peak_loss_x255(), 0, "peak loss stays zero under reordering");
        assert_eq!(
            dec.false_recovery_count(),
            1,
            "the guard detected exactly one D-SACK false recovery"
        );
    }

    #[test]
    fn genuine_retransmit_without_original_counts_as_loss() {
        // A shard whose original is truly lost: only its ARQ retransmit
        // arrives, with no late original to follow. That is a real drop
        // (RACK-TLP keeps it a loss, RFC 8985), so the estimator still counts
        // it - the reordering guard must not suppress genuine loss.
        let mut enc = Encoder::new(4, 0, 8);
        let mut dec = Decoder::new();
        let mut dgrams = Vec::new();
        for i in 0..4u64 {
            dgrams.extend(enc.push(&i.to_le_bytes()));
        }
        dec.on_packet(&dgrams[0]);
        dec.on_packet(&dgrams[1]);
        dec.on_packet(&dgrams[2]);
        // Shard 3's original was dropped; only its retransmit arrives.
        let mut rtx3 = dgrams[3].clone();
        rtx3[8] |= FLAG_RETRANSMIT;
        let delivered = dec.on_packet(&rtx3);
        assert_eq!(delivered.len(), 4, "block completes via the retransmit");
        assert!(
            dec.feedback(false).loss_x255 > 0,
            "a real drop recovered by ARQ is still counted as loss"
        );
        assert_eq!(
            dec.false_recovery_count(),
            0,
            "a genuine drop is not a D-SACK false recovery"
        );
    }
}
