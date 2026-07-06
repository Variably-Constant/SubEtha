//! Schema-aware structural compression for fixed-width bridge slots.
//!
//! The bridge ships fixed-width `repr(C)` slots, and measurement on the
//! real slot types shows 40-75% of every structured slot is bytes that
//! never carry information: padding, reserved fields, stable high bytes
//! of small enums, and counts. Those byte positions are *constant across
//! the stream*. This codec learns which positions are constant (a
//! template negotiated once, the "mask in one packet"), then ships only
//! the bytes at the varying positions. The receiver scatters them back
//! into the template.
//!
//! It is **exact**, not lossy: a slot whose supposedly-constant position
//! actually differs (the escape) is shipped in full under an escape flag,
//! so round-trip is byte-identical for any input, and the constant model
//! is a throughput optimization that can never corrupt.
//!
//! It is **cache-resident**: encode is a linear walk of a precomputed
//! constant-position list (the escape check) plus a linear gather of a
//! precomputed varying-position list; both lists are small `u16` vectors
//! that stay in L1. There is no per-slot allocation. Stream-level
//! parallelism rides the shard threads, one template per shard.

/// A learned constant-position template for a fixed-width slot stream.
///
/// `template` holds the constant baseline (varying positions are zero);
/// `var_pos` lists the byte positions that vary (shipped per slot);
/// `const_pos` is the complement (checked for the escape). Both position
/// lists are sorted ascending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaTemplate {
    width: usize,
    template: Vec<u8>,
    var_pos: Vec<u16>,
    const_pos: Vec<u16>,
}

/// Compact-record flag byte.
const FLAG_COMPACT: u8 = 0;
const FLAG_ESCAPE: u8 = 1;

impl SchemaTemplate {
    /// Learn a template from a sample of real slots, all `width` bytes. A
    /// byte position is constant if and only if it is identical in every
    /// sample slot; its value is recorded in the template. An empty
    /// sample yields the identity template (every position varies, encode
    /// is a pass-through plus one flag byte).
    pub fn learn(sample: &[&[u8]], width: usize) -> Self {
        let mut template = vec![0u8; width];
        let mut is_const = vec![false; width];
        if let Some(first) = sample.first() {
            assert!(first.len() >= width, "sample slot shorter than width");
            template.copy_from_slice(&first[..width]);
            is_const.iter_mut().for_each(|c| *c = true);
            for s in &sample[1..] {
                assert!(s.len() >= width, "sample slot shorter than width");
                for p in 0..width {
                    if s[p] != template[p] {
                        is_const[p] = false;
                    }
                }
            }
        }
        let var_pos: Vec<u16> = (0..width)
            .filter(|&p| !is_const[p])
            .map(|p| p as u16)
            .collect();
        let const_pos: Vec<u16> = (0..width)
            .filter(|&p| is_const[p])
            .map(|p| p as u16)
            .collect();
        // Zero the varying positions in the template so decode can scatter
        // into a clean baseline.
        for &p in &var_pos {
            template[p as usize] = 0;
        }
        Self {
            width,
            template,
            var_pos,
            const_pos,
        }
    }

    /// The fixed slot width this template encodes.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Number of varying (shipped) byte positions per compact slot.
    pub fn varying(&self) -> usize {
        self.var_pos.len()
    }

    /// Number of constant (elided) byte positions per compact slot.
    pub fn constant(&self) -> usize {
        self.const_pos.len()
    }

    /// Compact size of a non-escaped slot: one flag byte plus the varying
    /// bytes.
    pub fn compact_len(&self) -> usize {
        1 + self.var_pos.len()
    }

    /// Did this compact record take the escape path (the slot violated the
    /// template and shipped in full)? A rising escape rate is the signal to
    /// re-learn the template.
    pub fn is_escape(compact: &[u8]) -> bool {
        compact.first() == Some(&FLAG_ESCAPE)
    }

    /// Does `slot` match the constant template at every constant position?
    /// When false, the slot must be escaped (shipped in full).
    #[inline]
    fn matches_template(&self, slot: &[u8]) -> bool {
        self.const_pos
            .iter()
            .all(|&p| slot[p as usize] == self.template[p as usize])
    }

    /// Encode one `width`-byte slot, appending the compact record to
    /// `out`. Returns the number of bytes appended. Exact: a slot that
    /// differs at a constant position is escaped in full.
    #[inline]
    pub fn encode(&self, slot: &[u8], out: &mut Vec<u8>) -> usize {
        debug_assert_eq!(slot.len(), self.width);
        if self.matches_template(slot) {
            out.push(FLAG_COMPACT);
            for &p in &self.var_pos {
                out.push(slot[p as usize]);
            }
            1 + self.var_pos.len()
        } else {
            out.push(FLAG_ESCAPE);
            out.extend_from_slice(&slot[..self.width]);
            1 + self.width
        }
    }

    /// Decode one compact record from the front of `inp` into `out` (which
    /// must be at least `width` bytes). Returns the number of bytes
    /// consumed from `inp`. Inverse of [`encode`](Self::encode).
    #[inline]
    pub fn decode(&self, inp: &[u8], out: &mut [u8]) -> usize {
        debug_assert!(out.len() >= self.width);
        match inp[0] {
            FLAG_COMPACT => {
                out[..self.width].copy_from_slice(&self.template);
                for (i, &p) in self.var_pos.iter().enumerate() {
                    out[p as usize] = inp[1 + i];
                }
                1 + self.var_pos.len()
            }
            _ => {
                out[..self.width].copy_from_slice(&inp[1..1 + self.width]);
                1 + self.width
            }
        }
    }

    /// Serialize the template for the handshake (the "mask in one
    /// packet"): `width: u16`, `n_var: u16`, the varying positions
    /// (`u16` each), then the `width`-byte constant template. Both ends
    /// reconstruct an identical codec from these bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + 2 * self.var_pos.len() + self.width);
        out.extend_from_slice(&(self.width as u16).to_le_bytes());
        out.extend_from_slice(&(self.var_pos.len() as u16).to_le_bytes());
        for &p in &self.var_pos {
            out.extend_from_slice(&p.to_le_bytes());
        }
        out.extend_from_slice(&self.template);
        out
    }

    /// Reconstruct a template from [`serialize`](Self::serialize) bytes.
    /// Returns `None` if the buffer is malformed.
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let width = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        let n_var = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;
        let pos_end = 4 + 2 * n_var;
        if bytes.len() < pos_end + width {
            return None;
        }
        let mut var_pos = Vec::with_capacity(n_var);
        for i in 0..n_var {
            let off = 4 + 2 * i;
            var_pos.push(u16::from_le_bytes([bytes[off], bytes[off + 1]]));
        }
        let template = bytes[pos_end..pos_end + width].to_vec();
        let var_set: std::collections::HashSet<u16> = var_pos.iter().copied().collect();
        let const_pos: Vec<u16> = (0..width as u16).filter(|p| !var_set.contains(p)).collect();
        Some(Self {
            width,
            template,
            var_pos,
            const_pos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_deque_khpd::{FatLineItem, LineItem};
    use subetha_core::Marshal;

    /// xorshift64 - dep-free, reproducible.
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

    fn fatline_slots(n: usize, rng: &mut Rng) -> Vec<[u8; 64]> {
        let mut out = Vec::with_capacity(n);
        let mut id = 0u32;
        for _ in 0..n {
            let cnt = 1 + rng.below(3) as usize;
            let mut items = Vec::with_capacity(cnt);
            for _ in 0..cnt {
                let mut b = [0u8; 16];
                b[0] = rng.below(16) as u8;
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

    /// Round-trip is byte-exact on real marshaled slots, AND the constant
    /// model actually compresses.
    #[test]
    fn roundtrip_exact_on_real_fatline_slots() {
        let mut rng = Rng::new(0xabcd);
        let slots = fatline_slots(5000, &mut rng);
        let sample: Vec<&[u8]> = slots.iter().take(1000).map(|s| s.as_slice()).collect();
        let tpl = SchemaTemplate::learn(&sample, 64);
        assert!(
            tpl.constant() >= 12,
            "must find at least the 12 pad/reserved bytes constant, got {}",
            tpl.constant()
        );

        let mut wire = Vec::new();
        for s in &slots {
            tpl.encode(s, &mut wire);
        }
        let mut cursor = 0usize;
        let mut buf = [0u8; 64];
        for (i, s) in slots.iter().enumerate() {
            let used = tpl.decode(&wire[cursor..], &mut buf);
            cursor += used;
            assert_eq!(&buf[..], &s[..], "slot {i} round-trip mismatch");
        }
        assert_eq!(cursor, wire.len(), "consumed the whole wire stream");

        let ratio = wire.len() as f64 / (slots.len() * 64) as f64;
        assert!(
            ratio < 0.75,
            "constant-elision must shrink the stream, got ratio {ratio:.3}"
        );
    }

    /// A slot that violates the learned template (a "constant" position
    /// changes) is escaped and still round-trips exactly.
    #[test]
    fn escape_preserves_exactness() {
        let zero = [0u8; 16];
        let sample: Vec<&[u8]> = (0..8).map(|_| zero.as_slice()).collect();
        let tpl = SchemaTemplate::learn(&sample, 16);
        assert_eq!(tpl.constant(), 16, "all-zero sample makes every position constant");

        let mut odd = [0u8; 16];
        odd[3] = 0xff; // violates the all-constant template -> must escape
        let mut wire = Vec::new();
        let n = tpl.encode(&odd, &mut wire);
        assert_eq!(n, 1 + 16, "violating slot is escaped in full");
        let mut buf = [0u8; 16];
        let used = tpl.decode(&wire, &mut buf);
        assert_eq!(used, n);
        assert_eq!(buf, odd, "escaped slot round-trips exactly");
    }

    /// The serialized template reconstructs an identical codec (the
    /// handshake contract).
    #[test]
    fn serialize_roundtrips_codec() {
        let mut rng = Rng::new(0x1357);
        let slots = fatline_slots(500, &mut rng);
        let sample: Vec<&[u8]> = slots.iter().map(|s| s.as_slice()).collect();
        let tpl = SchemaTemplate::learn(&sample, 64);
        let bytes = tpl.serialize();
        let back = SchemaTemplate::deserialize(&bytes).expect("deserialize");
        assert_eq!(tpl, back, "serialized template reconstructs identically");
    }
}
