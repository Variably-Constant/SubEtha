//! Cross-block (segment) outer code: the second FEC rung.
//!
//! The inner code ([`crate::fec`]) protects shards *within* a block. The
//! tower adds an outer code *across* blocks: a segment of `d` data blocks
//! ships with `r_outer` parity blocks, each a Cauchy Reed-Solomon
//! combination of the `d` data blocks. A burst that erases an *entire*
//! block - every shard, which the inner per-block code cannot recover and
//! which a long-enough burst defeats even with interleaving - is
//! reconstructed from the surviving blocks in its segment, with no
//! retransmit round-trip.
//!
//! Each block is treated as one outer symbol: its information is the `k`
//! inner data shards concatenated (the inner parity shards are
//! re-derivable), so an outer symbol is `k * shard_len` bytes. The outer
//! code recovers up to `r_outer` fully-lost blocks as long as at least
//! `d` of the `d + r_outer` blocks in the segment survived.
//!
//! ARQ remains the correctness floor; the tower is a latency optimization
//! that recovers whole-block losses before ARQ has to.

use crate::fec::{FecError, RsCode};

/// A systematic Cauchy-RS outer code over whole blocks: `d` data blocks
/// plus `r_outer` parity blocks per segment.
#[derive(Debug, Clone)]
pub struct SegmentCode {
    code: RsCode,
    d: usize,
    r_outer: usize,
}

impl SegmentCode {
    /// Build a `(d, r_outer)` segment code. `d >= 1`, `r_outer >= 1`,
    /// `d + r_outer <= 256`.
    pub fn new(d: usize, r_outer: usize) -> Result<Self, FecError> {
        Ok(Self {
            code: RsCode::new(d, r_outer)?,
            d,
            r_outer,
        })
    }

    /// Data blocks per segment.
    pub fn d(&self) -> usize {
        self.d
    }

    /// Outer parity blocks per segment.
    pub fn r_outer(&self) -> usize {
        self.r_outer
    }

    /// Compute the `r_outer` outer-parity block payloads from the `d`
    /// data-block payloads. Every block payload (data and parity) must be
    /// the same length (`k * shard_len`).
    pub fn encode(
        &self,
        data_blocks: &[&[u8]],
        parity_blocks: &mut [&mut [u8]],
    ) -> Result<(), FecError> {
        self.code.encode(data_blocks, parity_blocks)
    }

    /// Recover missing data-block payloads in place. `blocks` has
    /// `d + r_outer` entries (data blocks first, then outer parity);
    /// `Some` = present (inner-recovered), `None` = whole block lost. On
    /// success every data slot `0..d` is `Some`. Returns `TooFewShards`
    /// if fewer than `d` blocks survived (ARQ must cover the rest).
    pub fn decode(&self, blocks: &mut [Option<Vec<u8>>]) -> Result<(), FecError> {
        self.code.decode(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `d` data blocks of `block_len` bytes with recognizable
    /// content, compute the outer parity, then verify that dropping every
    /// pattern of up to `r_outer` WHOLE blocks recovers the data exactly.
    fn exhaustive_whole_block_recovery(d: usize, r_outer: usize, block_len: usize) {
        let seg = SegmentCode::new(d, r_outer).expect("segment code");
        let data: Vec<Vec<u8>> = (0..d)
            .map(|i| {
                (0..block_len)
                    .map(|b| ((i * 251 + b * 13 + 5) & 0xFF) as u8)
                    .collect()
            })
            .collect();
        let mut parity: Vec<Vec<u8>> = vec![vec![0u8; block_len]; r_outer];
        {
            let dref: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let mut pref: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
            seg.encode(&dref, &mut pref).expect("encode");
        }
        let n = d + r_outer;
        let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
        for lost in 1..=r_outer {
            for mask in 0u32..(1 << n) {
                if mask.count_ones() as usize != lost {
                    continue;
                }
                let mut blocks: Vec<Option<Vec<u8>>> = all
                    .iter()
                    .enumerate()
                    .map(|(i, b)| if mask & (1 << i) != 0 { None } else { Some(b.clone()) })
                    .collect();
                seg.decode(&mut blocks).expect("decode");
                for i in 0..d {
                    assert_eq!(
                        blocks[i].as_ref().unwrap(),
                        &data[i],
                        "d={d} r_outer={r_outer} mask={mask:b}: block {i} mismatch"
                    );
                }
            }
        }
    }

    #[test]
    fn recover_whole_blocks_d4_r2() {
        exhaustive_whole_block_recovery(4, 2, 80);
    }

    #[test]
    fn recover_whole_blocks_d8_r2() {
        exhaustive_whole_block_recovery(8, 2, 40);
    }

    #[test]
    fn recover_whole_blocks_d6_r3() {
        exhaustive_whole_block_recovery(6, 3, 32);
    }

    #[test]
    fn too_many_lost_blocks_is_reported() {
        let seg = SegmentCode::new(4, 2).expect("code");
        let len = 32;
        let data: Vec<Vec<u8>> = (0..4).map(|_| vec![7u8; len]).collect();
        let mut parity: Vec<Vec<u8>> = vec![vec![0u8; len]; 2];
        {
            let dref: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let mut pref: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
            seg.encode(&dref, &mut pref).expect("encode");
        }
        // Lose 3 of 6 whole blocks (more than r_outer=2): ARQ territory.
        let mut blocks: Vec<Option<Vec<u8>>> =
            vec![None, None, None, Some(data[3].clone()), Some(parity[0].clone()), Some(parity[1].clone())];
        assert_eq!(seg.decode(&mut blocks), Err(FecError::TooFewShards));
    }
}
