//! Intra-packet salvage: recover a packet the kernel would discard for a
//! CRC failure, instead of treating it as a total loss.
//!
//! The standard stack drops a whole frame for a single bad bit. If the
//! corrupt frame is captured anyway (a driver's bad-FCS passthrough, or -
//! since that is hardware-gated - injected corruption for validation),
//! this tier repairs the few bad bytes and delivers the packet.
//!
//! Rather than a full Reed-Solomon *error* decoder (unknown error
//! positions, expensive Berlekamp-Massey), it follows the Maranello
//! approach: a payload is split into `b` blocks each carrying a checksum,
//! plus `r` Reed-Solomon parity blocks. On receive, a block whose
//! checksum fails is a *located* error - an **erasure** - so the existing
//! erasure code ([`crate::fec`]) reconstructs it. Up to `r` corrupt
//! blocks per packet are salvaged. The path is identical whether the
//! corrupt bytes arrive from a real bad-FCS frame or from injected
//! corruption, so it is fully testable without special hardware.

use crate::fec::{FecError, RsCode};

/// Bytes of checksum stored per block.
const CSUM_BYTES: usize = 4;

/// FNV-1a 32-bit hash, used as a per-block corruption detector. A single
/// flipped bit changes it with overwhelming probability.
fn checksum(block: &[u8]) -> u32 {
    let mut h = 0x811c_9dc5u32;
    for &byte in block {
        h = (h ^ byte as u32).wrapping_mul(0x0100_0193);
    }
    h
}

/// Splits a packet into `b` data blocks protected by `r` Reed-Solomon
/// parity blocks plus a per-block checksum, and salvages a corrupt packet
/// by reconstructing the checksum-flagged blocks.
#[derive(Debug, Clone)]
pub struct PacketSalvage {
    code: RsCode,
    b: usize,
    r: usize,
    block_len: usize,
}

impl PacketSalvage {
    /// Build a salvage code: `b` data blocks, `r` parity blocks, each
    /// `block_len` bytes.
    pub fn new(b: usize, r: usize, block_len: usize) -> Result<Self, FecError> {
        if block_len == 0 {
            return Err(FecError::BadShardLen);
        }
        Ok(Self {
            code: RsCode::new(b, r)?,
            b,
            r,
            block_len,
        })
    }

    /// Bytes of payload one packet protects.
    pub fn payload_len(&self) -> usize {
        self.b * self.block_len
    }

    /// Total protected-packet length (blocks + parity + checksums).
    pub fn protected_len(&self) -> usize {
        (self.b + self.r) * (self.block_len + CSUM_BYTES)
    }

    /// Encode `payload` (exactly [`payload_len`](Self::payload_len) bytes)
    /// into a protected packet: `b + r` blocks each followed by its
    /// checksum.
    pub fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, FecError> {
        if payload.len() != self.payload_len() {
            return Err(FecError::BadShardLen);
        }
        let mut blocks: Vec<Vec<u8>> = payload
            .chunks(self.block_len)
            .map(|c| c.to_vec())
            .collect();
        let mut parity: Vec<Vec<u8>> = vec![vec![0u8; self.block_len]; self.r];
        {
            let dref: Vec<&[u8]> = blocks.iter().map(|v| v.as_slice()).collect();
            let mut pref: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
            self.code.encode(&dref, &mut pref)?;
        }
        blocks.extend(parity);
        let mut out = Vec::with_capacity(self.protected_len());
        for blk in &blocks {
            out.extend_from_slice(blk);
            out.extend_from_slice(&checksum(blk).to_le_bytes());
        }
        Ok(out)
    }

    /// Salvage a (possibly corrupt) protected packet back to the original
    /// payload. Blocks whose checksum fails are reconstructed via parity.
    /// Returns `None` if more than `r` blocks are corrupt (beyond the
    /// salvage budget) or the input is malformed.
    pub fn decode(&self, protected: &[u8]) -> Option<Vec<u8>> {
        let n = self.b + self.r;
        let stride = self.block_len + CSUM_BYTES;
        if protected.len() != n * stride {
            return None;
        }
        let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        let mut corrupt = 0usize;
        for i in 0..n {
            let base = i * stride;
            let blk = &protected[base..base + self.block_len];
            let csum = u32::from_le_bytes(
                protected[base + self.block_len..base + stride].try_into().ok()?,
            );
            if checksum(blk) == csum {
                shards.push(Some(blk.to_vec()));
            } else {
                shards.push(None); // located error -> erasure
                corrupt += 1;
            }
        }
        if corrupt > self.r {
            return None; // beyond the salvage budget
        }
        self.code.decode(&mut shards).ok()?;
        let mut payload = Vec::with_capacity(self.payload_len());
        for shard in shards.iter().take(self.b) {
            payload.extend_from_slice(shard.as_ref()?);
        }
        Some(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_payload(b: usize, block_len: usize) -> Vec<u8> {
        (0..b * block_len).map(|i| (i * 7 + 3) as u8).collect()
    }

    #[test]
    fn clean_packet_round_trips() {
        let s = PacketSalvage::new(8, 2, 16).unwrap();
        let payload = sample_payload(8, 16);
        let packet = s.encode(&payload).unwrap();
        assert_eq!(s.decode(&packet), Some(payload));
    }

    #[test]
    fn salvages_corruption_within_budget() {
        let s = PacketSalvage::new(8, 2, 16).unwrap();
        let payload = sample_payload(8, 16);
        let mut packet = s.encode(&payload).unwrap();
        // Corrupt bytes inside two different data blocks (== r budget).
        packet[3] ^= 0xFF; // block 0
        let stride = 16 + CSUM_BYTES;
        packet[2 * stride + 5] ^= 0x80; // block 2
        assert_eq!(s.decode(&packet), Some(payload), "two corrupt blocks salvaged");
    }

    #[test]
    fn corruption_beyond_budget_is_unrecoverable() {
        let s = PacketSalvage::new(8, 2, 16).unwrap();
        let payload = sample_payload(8, 16);
        let mut packet = s.encode(&payload).unwrap();
        let stride = 16 + CSUM_BYTES;
        // Corrupt three blocks (> r=2).
        packet[1] ^= 0xFF;
        packet[stride + 1] ^= 0xFF;
        packet[2 * stride + 1] ^= 0xFF;
        assert_eq!(s.decode(&packet), None, "beyond budget cannot be salvaged");
    }

    #[test]
    fn corrupt_checksum_treats_block_as_erasure() {
        let s = PacketSalvage::new(6, 2, 16).unwrap();
        let payload = sample_payload(6, 16);
        let mut packet = s.encode(&payload).unwrap();
        // Corrupt only a checksum: the block is good but flagged - still
        // recovered from parity.
        let stride = 16 + CSUM_BYTES;
        packet[stride + 16] ^= 0xFF; // block 1's checksum byte
        assert_eq!(s.decode(&packet), Some(payload));
    }

    #[test]
    fn exhaustive_single_block_corruption() {
        // Corrupting any one block must always salvage.
        let (b, r, bl) = (8usize, 2usize, 12usize);
        let s = PacketSalvage::new(b, r, bl).unwrap();
        let payload = sample_payload(b, bl);
        let base = s.encode(&payload).unwrap();
        let stride = bl + CSUM_BYTES;
        for blk in 0..(b + r) {
            let mut packet = base.clone();
            packet[blk * stride] ^= 0xAA;
            assert_eq!(s.decode(&packet), Some(payload.clone()), "block {blk} corrupted");
        }
    }
}
