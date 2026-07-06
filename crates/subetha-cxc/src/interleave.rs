//! Block interleaving: a sender-side transmit-order permutation that
//! converts a burst loss into a spread loss the per-block FEC can
//! recover.
//!
//! Without interleaving, the k+r datagrams of one block ship
//! consecutively, so a burst that drops B consecutive packets removes B
//! shards from one block - and if B > r, that block is unrecoverable.
//! With interleave depth D, the datagrams of D blocks ship column-major:
//! shard 0 of blocks 0..D, then shard 1 of blocks 0..D, and so on. The
//! datagrams of any one block are then spaced D apart on the wire, so a
//! burst of up to D consecutive losses removes at most ONE shard from
//! each block - back inside FEC's r-parity budget.
//!
//! This is purely a sender-side reordering. The receiver routes every
//! datagram by its `(block_id, shard_index)` header
//! ([`crate::reliable_udp::Decoder`]), so it reassembles correctly
//! regardless of arrival order and needs no de-interleave logic.
//!
//! Cost: the interleaver buffers up to D blocks before emitting the
//! first datagram, trading D-block latency for burst tolerance. D is a
//! control-table knob ([`crate::control_table::ControlTable`]); D = 1 is
//! pass-through with no added latency.

/// Buffers up to `depth` blocks of datagrams and emits them column-major
/// so each block's shards are spaced `depth` apart on the wire.
#[derive(Debug)]
pub struct Interleaver {
    depth: usize,
    /// Up to `depth` blocks, each a list of that block's datagrams.
    pending: Vec<Vec<Vec<u8>>>,
}

impl Interleaver {
    /// Create an interleaver of the given depth (clamped to at least 1).
    pub fn new(depth: usize) -> Self {
        Self {
            depth: depth.max(1),
            pending: Vec::new(),
        }
    }

    /// Current interleave depth.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Change the interleave depth. Any buffered blocks are flushed
    /// first (returned to the caller) so the depth change does not
    /// reorder a partially-staged group.
    pub fn set_depth(&mut self, depth: usize) -> Vec<Vec<u8>> {
        let flushed = self.flush();
        self.depth = depth.max(1);
        flushed
    }

    /// Number of blocks currently buffered.
    pub fn buffered_blocks(&self) -> usize {
        self.pending.len()
    }

    /// Stage one block's datagrams. Returns the interleaved datagrams
    /// ready to transmit: empty until `depth` blocks are buffered, then
    /// the whole interleaved group. At depth 1 the block is returned
    /// immediately (pass-through, no added latency).
    pub fn add_block(&mut self, datagrams: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        if datagrams.is_empty() {
            return Vec::new();
        }
        self.pending.push(datagrams);
        if self.pending.len() >= self.depth {
            self.emit()
        } else {
            Vec::new()
        }
    }

    /// Emit whatever blocks are buffered (a short final group),
    /// interleaved. Empty if nothing is staged.
    pub fn flush(&mut self) -> Vec<Vec<u8>> {
        if self.pending.is_empty() {
            Vec::new()
        } else {
            self.emit()
        }
    }

    /// Column-major emit of the buffered blocks, then clear.
    fn emit(&mut self) -> Vec<Vec<u8>> {
        let mut blocks = std::mem::take(&mut self.pending);
        let max_len = blocks.iter().map(|b| b.len()).max().unwrap_or(0);
        let total: usize = blocks.iter().map(|b| b.len()).sum();
        let mut out = Vec::with_capacity(total);
        // For each shard column, emit that column's datagram from every block
        // that has one. Block i's shards land at output positions separated by
        // the number of blocks, so a burst of <= depth consecutive losses hits
        // at most one shard per block. The datagram is MOVED out, not cloned:
        // `blocks` is owned here and dropped on return, so swapping in an empty
        // Vec transfers ownership with no per-datagram allocation or byte copy.
        for col in 0..max_len {
            for block in blocks.iter_mut() {
                if col < block.len() {
                    out.push(std::mem::take(&mut block[col]));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `n` "blocks" of `shards` tiny datagrams each; each datagram
    /// encodes (block_index, shard_index) in its first two bytes so the
    /// permutation and burst properties are checkable.
    fn make_blocks(n: usize, shards: usize) -> Vec<Vec<Vec<u8>>> {
        (0..n)
            .map(|b| (0..shards).map(|s| vec![b as u8, s as u8]).collect())
            .collect()
    }

    #[test]
    fn depth_one_is_passthrough() {
        let mut il = Interleaver::new(1);
        let blocks = make_blocks(1, 5);
        let out = il.add_block(blocks[0].clone());
        assert_eq!(out, blocks[0], "depth 1 emits the block immediately");
        assert_eq!(il.buffered_blocks(), 0);
    }

    #[test]
    fn emits_when_depth_reached_and_is_a_permutation() {
        let depth = 4;
        let shards = 6;
        let blocks = make_blocks(depth, shards);
        let mut il = Interleaver::new(depth);
        let mut out = Vec::new();
        for (i, b) in blocks.iter().enumerate() {
            let emitted = il.add_block(b.clone());
            if i < depth - 1 {
                assert!(emitted.is_empty(), "no emit before depth reached");
            } else {
                out = emitted;
            }
        }
        // Output is a permutation of all input datagrams (no loss/dup).
        let mut got = out.clone();
        let mut want: Vec<Vec<u8>> = blocks.into_iter().flatten().collect();
        got.sort();
        want.sort();
        assert_eq!(got, want, "interleave is a permutation of the input");
    }

    /// The core property: with depth D, any window of D consecutive
    /// emitted datagrams contains at most ONE shard from any block.
    fn burst_property(depth: usize, shards: usize) {
        let blocks = make_blocks(depth, shards);
        let mut il = Interleaver::new(depth);
        let mut out = Vec::new();
        for b in &blocks {
            out.extend(il.add_block(b.clone()));
        }
        out.extend(il.flush());
        assert_eq!(out.len(), depth * shards);
        // Slide a window of `depth` and count per-block hits.
        for start in 0..=out.len() - depth {
            let mut per_block = vec![0u32; depth];
            for pkt in &out[start..start + depth] {
                per_block[pkt[0] as usize] += 1;
            }
            assert!(
                per_block.iter().all(|&c| c <= 1),
                "depth={depth} shards={shards} window@{start}: a block lost >1 shard to a burst of {depth}"
            );
        }
    }

    #[test]
    fn burst_of_depth_hits_at_most_one_shard_per_block() {
        burst_property(4, 6);
        burst_property(8, 10);
        burst_property(3, 3);
        burst_property(16, 8);
    }

    #[test]
    fn set_depth_flushes_pending() {
        let mut il = Interleaver::new(4);
        let blocks = make_blocks(2, 5); // fewer than depth
        il.add_block(blocks[0].clone());
        let flushed = il.add_block(blocks[1].clone());
        assert!(flushed.is_empty(), "2 of 4 staged, nothing emitted yet");
        let out = il.set_depth(2);
        assert_eq!(out.len(), 10, "changing depth flushes the 2 staged blocks");
        assert_eq!(il.depth(), 2);
        assert_eq!(il.buffered_blocks(), 0);
    }
}

/// Gilbert-Elliott burst-loss A/B: interleaving must cut the ARQ load it
/// would otherwise take to recover bursts, by spreading each burst so the
/// per-block FEC absorbs it. Both depths deliver exactly (ARQ is the
/// floor); the metric is how many retransmits each needed.
#[cfg(test)]
mod gilbert_elliott {
    use super::Interleaver;
    use crate::reliable_udp::{Decoder, Encoder};

    /// Two-state burst channel. Probabilities are per-1000.
    struct Ge {
        bad: bool,
        rng: u64,
        p_gb: u32, // good -> bad
        p_bg: u32, // bad -> good (mean burst length = 1000 / p_bg)
        p_b: u32,  // loss while bad
        p_g: u32,  // loss while good
    }

    impl Ge {
        fn new(seed: u64) -> Self {
            Self { bad: false, rng: seed | 1, p_gb: 30, p_bg: 200, p_b: 900, p_g: 0 }
        }
        fn rand(&mut self) -> u32 {
            self.rng = self
                .rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.rng >> 33) as u32
        }
        /// Advance the state, then decide loss for one datagram.
        fn drop(&mut self) -> bool {
            if self.bad {
                if self.rand() % 1000 < self.p_bg {
                    self.bad = false;
                }
            } else if self.rand() % 1000 < self.p_gb {
                self.bad = true;
            }
            let p = if self.bad { self.p_b } else { self.p_g };
            self.rand() % 1000 < p
        }
    }

    /// Encode `n` items at the given interleave depth, push the
    /// interleaved stream through the GE channel, and drive ARQ (also
    /// lossy) to completion. Returns (delivered-exactly, retransmits).
    fn run(depth: usize, n: u64, seed: u64) -> (bool, usize) {
        let (k, r) = (8usize, 2usize);
        let mut enc = Encoder::new(k, r, 8);
        let mut il = Interleaver::new(depth);
        let mut dec = Decoder::new();

        // Build the interleaved wire order (all blocks sealed before any
        // feedback, so parity r stays constant at 2 throughout).
        let mut wire: Vec<Vec<u8>> = Vec::new();
        for i in 0..n {
            let block = enc.push(&i.to_le_bytes());
            if !block.is_empty() {
                wire.extend(il.add_block(block));
            }
        }
        let tail = enc.flush();
        if !tail.is_empty() {
            wire.extend(il.add_block(tail));
        }
        wire.extend(il.flush());

        let mut ge = Ge::new(seed);
        let mut delivered: Vec<u64> = Vec::new();
        for pkt in &wire {
            if ge.drop() {
                continue;
            }
            for it in dec.on_packet(pkt) {
                delivered.push(u64::from_le_bytes(it.try_into().unwrap()));
            }
        }

        // ARQ loop: retransmits also traverse the GE channel.
        let mut retransmits = 0usize;
        let mut rounds = 0u32;
        while (delivered.len() as u64) < n {
            rounds += 1;
            assert!(rounds < 20_000, "no convergence at depth {depth}");
            let fb = dec.feedback(true);
            for pkt in enc.on_feedback(&fb) {
                retransmits += 1;
                if ge.drop() {
                    continue;
                }
                for it in dec.on_packet(&pkt) {
                    delivered.push(u64::from_le_bytes(it.try_into().unwrap()));
                }
            }
        }
        let ok = delivered == (0..n).collect::<Vec<_>>();
        (ok, retransmits)
    }

    #[test]
    fn interleaving_cuts_arq_under_bursty_loss() {
        let n = 240;
        let seed = 0x00C0_FFEE_1234_5678;
        let (ok1, rtx1) = run(1, n, seed);
        let (ok8, rtx8) = run(8, n, seed);
        assert!(ok1 && ok8, "both deliver exactly via the ARQ floor");
        assert!(
            rtx8 < rtx1,
            "interleaving must cut ARQ under bursts: depth8={rtx8} retransmits vs depth1={rtx1}"
        );
    }
}
