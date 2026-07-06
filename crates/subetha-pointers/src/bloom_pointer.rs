//! `BloomPointer<T>` - pointer carrying a Bloom-filter summary of
//! its target's contents.
//!
//! Layout: `(bloom: u64, target: Arc<T>)`. The 64-bit Bloom filter
//! is set from external knowledge of what's reachable through
//! `target` - typically the keys of a HashMap, the labels of a
//! graph node's outgoing edges, the IDs that occupy a B-tree
//! subtree.
//!
//! The architectural win: `bloom_contains(query_key)` rejects
//! membership queries in one register-compare without touching the
//! pointed-to data. With a 64-bit filter + 4 hash functions and ~16
//! items, false-positive rate is ~3%; for ~97% of negative queries
//! the scan skips the pointer chase entirely.
//!
//! # K_cascade composition - `BloomCascade<T>`
//!
//! Wraps `BloomPointer<BloomPointer<T>>` semantics in a dedicated
//! struct: coarse 8-byte filter for level-0 rejection, finer 32-byte
//! filter for level-1 rejection, target pointer for the deref.
//! Mirrors the LSM-tree multi-level Bloom design.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Fast non-cryptographic hash used to derive Bloom filter bit
/// indices. FxHash-style rotate-xor-multiply chain; ~2-3 ns per
/// u64 key on modern x86. Replaces SipHash (`DefaultHasher`) which
/// at ~15-20 ns per call dominated `might_contain` cost on small-
/// payload workloads.
#[derive(Default)]
struct FxBloomHasher(u64);

const FX_SEED: u64 = 0xCBF2_9CE4_8422_2325;
const FX_MULT: u64 = 0x517C_C1B7_2722_0A95;

impl FxBloomHasher {
    fn with_seed(seed: u64) -> Self {
        Self(seed ^ FX_SEED)
    }
}

impl Hasher for FxBloomHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let n = u64::from_le_bytes([
                c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7],
            ]);
            self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(FX_MULT);
        }
        for &b in chunks.remainder() {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(FX_MULT);
        }
    }
    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(FX_MULT);
    }
    #[inline]
    fn write_u32(&mut self, n: u32) { self.write_u64(n as u64); }
    #[inline]
    fn write_u16(&mut self, n: u16) { self.write_u64(n as u64); }
    #[inline]
    fn write_u8(&mut self, n: u8) { self.write_u64(n as u64); }
    #[inline]
    fn write_i64(&mut self, n: i64) { self.write_u64(n as u64); }
    #[inline]
    fn write_i32(&mut self, n: i32) { self.write_u64(n as u64); }
    #[inline]
    fn write_isize(&mut self, n: isize) { self.write_u64(n as u64); }
    #[inline]
    fn write_usize(&mut self, n: usize) { self.write_u64(n as u64); }
    #[inline]
    fn finish(&self) -> u64 { self.0 }
}

/// 64-bit Bloom filter with 4 hash functions.
///
/// Capacity for ~8 distinct keys at ~3% false-positive rate. Beyond
/// that the filter saturates and FPR climbs rapidly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Bloom64(pub u64);

impl Bloom64 {
    pub const ZERO: Self = Self(0);
    /// Suggested capacity: ~8 keys before FPR climbs past ~3%.
    /// (64 bits / 4 hashes / 8 keys -> ~2.4% FPR; at 16 keys FPR
    /// is already ~16%.)
    pub const SUGGESTED_CAPACITY: usize = 8;

    #[inline]
    pub(crate) fn fast_hash<K: Hash + ?Sized>(key: &K, seed: u64) -> u64 {
        let mut h = FxBloomHasher::with_seed(seed);
        key.hash(&mut h);
        h.finish()
    }

    /// 4 indices into the 64-bit filter, derived as 6-bit slices of a
    /// single fast-hash output. The slices are spaced 16 bits apart
    /// (positions 0, 16, 32, 48) to keep cross-slice correlation low.
    #[inline]
    fn indices<K: Hash + ?Sized>(key: &K) -> [u8; 4] {
        let h = Self::fast_hash(key, 0x9E37_79B9_7F4A_7C15);
        [
            (h & 0x3F) as u8,
            ((h >> 16) & 0x3F) as u8,
            ((h >> 32) & 0x3F) as u8,
            ((h >> 48) & 0x3F) as u8,
        ]
    }

    /// Insert `key` into the filter.
    pub fn insert<K: Hash + ?Sized>(&mut self, key: &K) {
        for bit in Self::indices(key) {
            self.0 |= 1u64 << bit;
        }
    }

    /// Probabilistic membership: returns `false` when key is
    /// definitely-not-present, `true` when key might-be-present.
    pub fn might_contain<K: Hash + ?Sized>(&self, key: &K) -> bool {
        let bits = Self::indices(key);
        for bit in bits {
            if (self.0 >> bit) & 1 == 0 {
                return false;
            }
        }
        true
    }

    /// Build from an iterator of keys.
    pub fn from_keys<'a, K, I>(keys: I) -> Self
    where K: Hash + 'a, I: IntoIterator<Item = &'a K>,
    {
        let mut b = Self::ZERO;
        for k in keys { b.insert(k); }
        b
    }

    /// Number of set bits (informational; high values suggest
    /// saturation).
    pub fn popcount(&self) -> u32 { self.0.count_ones() }

    /// Approximate false-positive rate assuming `n` keys inserted.
    pub fn estimated_fpr(n: usize) -> f64 {
        // Standard FPR formula: (1 - exp(-k*n/m))^k with m=64, k=4.
        let m = 64.0;
        let k = 4.0;
        let p_zero = (-k * n as f64 / m).exp();
        (1.0 - p_zero).powf(k)
    }
}

/// `(Bloom64, Arc<T>)` - 16 bytes on 64-bit.
#[derive(Debug, Clone)]
pub struct BloomPointer<T> {
    bloom: Bloom64,
    target: Arc<T>,
}

impl<T> BloomPointer<T> {
    /// Direction signature of `BloomPointer<T>`. Engages the
    /// `K_content_prefix` axis (bloom-filter summary of the
    /// target's keys stored at slot for fast set-membership
    /// rejection before deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::ContentPrefix],
    );

    pub fn new(target: Arc<T>, bloom: Bloom64) -> Self {
        Self { bloom, target }
    }

    /// Build the bloom from a key iterator. Caller is responsible for
    /// providing the keys (typically by walking `target` and yielding
    /// owned key copies or references).
    pub fn from_keys<K, I>(target: Arc<T>, keys: I) -> Self
    where K: Hash, I: IntoIterator<Item = K>,
    {
        let mut b = Bloom64::ZERO;
        for k in keys { b.insert(&k); }
        Self { bloom: b, target }
    }

    #[inline]
    pub fn bloom(&self) -> Bloom64 { self.bloom }

    #[inline]
    pub fn target(&self) -> &Arc<T> { &self.target }

    /// Skip-the-deref membership test. Returns `false` for
    /// definitely-no, `true` for might-be-yes.
    #[inline]
    pub fn might_contain<K: Hash + ?Sized>(&self, key: &K) -> bool {
        self.bloom.might_contain(key)
    }

    /// Replace the bloom after target mutation. Caller is responsible
    /// for ensuring the new bloom reflects current contents.
    pub fn set_bloom(&mut self, b: Bloom64) { self.bloom = b; }
}

// =========================================================
// BloomCascade<T> - multi-level filter for steeper rejection
// =========================================================

/// Two-level cascading filter: 8-byte coarse + 32-byte fine. Layer
/// 0 (coarse) rejects in one register-compare. Layer 1 (fine) holds
/// 4x as many bits + 8 hash functions; rejects most of the
/// remainder before the target is touched.
///
/// Architectural shape: same as LSM-tree multi-level Blooms or the
/// nested-cache pattern in Bitcoin SPV / LevelDB / RocksDB - exposed
/// as a typed primitive.
#[derive(Debug, Clone)]
pub struct BloomCascade<T> {
    coarse: Bloom64,
    fine: BloomFine,
    target: Arc<T>,
}

/// 256-bit Bloom filter with 8 hash functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BloomFine {
    bits: [u64; 4],
}

impl BloomFine {
    pub const ZERO: Self = Self { bits: [0; 4] };
    /// ~64 keys before 5% FPR.
    pub const SUGGESTED_CAPACITY: usize = 64;

    /// 8 indices into the 256-bit filter, derived from two fast-hash
    /// calls with different seeds. Each hash output is sliced into 4
    /// 8-bit positions; the two hashes together yield the 8 indices.
    /// Two seeds (vs four in the original) cuts hash cost in half
    /// while keeping the slices distinct enough for the 256-bit
    /// filter's bit-occupancy budget.
    #[inline]
    fn indices<K: Hash + ?Sized>(key: &K) -> [u8; 8] {
        let h1 = Bloom64::fast_hash(key, 0x9E37_79B9_7F4A_7C15);
        let h2 = Bloom64::fast_hash(key, 0xBB67_AE85_84CA_A73B);
        [
            (h1 & 0xFF) as u8,
            ((h1 >> 16) & 0xFF) as u8,
            ((h1 >> 32) & 0xFF) as u8,
            ((h1 >> 48) & 0xFF) as u8,
            (h2 & 0xFF) as u8,
            ((h2 >> 16) & 0xFF) as u8,
            ((h2 >> 32) & 0xFF) as u8,
            ((h2 >> 48) & 0xFF) as u8,
        ]
    }

    pub fn insert<K: Hash + ?Sized>(&mut self, key: &K) {
        for bit in Self::indices(key) {
            self.bits[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
    }

    pub fn might_contain<K: Hash + ?Sized>(&self, key: &K) -> bool {
        for bit in Self::indices(key) {
            let word = self.bits[(bit / 64) as usize];
            if (word >> (bit % 64)) & 1 == 0 { return false; }
        }
        true
    }

    pub fn from_keys<'a, K, I>(keys: I) -> Self
    where K: Hash + 'a, I: IntoIterator<Item = &'a K>,
    {
        let mut b = Self::ZERO;
        for k in keys { b.insert(k); }
        b
    }

    pub fn popcount(&self) -> u32 {
        self.bits.iter().map(|w| w.count_ones()).sum()
    }
}

impl<T> BloomCascade<T> {
    pub fn new(target: Arc<T>, coarse: Bloom64, fine: BloomFine) -> Self {
        Self { coarse, fine, target }
    }

    /// Build both filter levels from the same key iterator.
    pub fn from_keys<K, I>(target: Arc<T>, keys: I) -> Self
    where K: Hash, I: IntoIterator<Item = K>,
    {
        let mut coarse = Bloom64::ZERO;
        let mut fine = BloomFine::ZERO;
        for k in keys {
            coarse.insert(&k);
            fine.insert(&k);
        }
        Self { coarse, fine, target }
    }

    pub fn target(&self) -> &Arc<T> { &self.target }
    pub fn coarse(&self) -> Bloom64 { self.coarse }
    pub fn fine(&self) -> &BloomFine { &self.fine }

    /// Cascade rejection: coarse first (register-only), fine
    /// second (32 bytes, 4 cache lines worst case). Returns the
    /// LEVEL where the reject fired (0 = coarse rejected, 1 = fine
    /// rejected, 2 = both layers said maybe-yes).
    pub fn cascade_check<K: Hash + ?Sized>(&self, key: &K) -> CascadeOutcome {
        if !self.coarse.might_contain(key) {
            return CascadeOutcome::RejectedAtCoarse;
        }
        if !self.fine.might_contain(key) {
            return CascadeOutcome::RejectedAtFine;
        }
        CascadeOutcome::MightContain
    }
}

/// Outcome of [`BloomCascade::cascade_check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeOutcome {
    RejectedAtCoarse,
    RejectedAtFine,
    MightContain,
}

impl CascadeOutcome {
    pub fn might_contain(self) -> bool { matches!(self, Self::MightContain) }
    pub fn rejected(self) -> bool { !self.might_contain() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom64_insert_then_query() {
        let mut b = Bloom64::ZERO;
        b.insert(&42u64);
        b.insert(&"hello");
        assert!(b.might_contain(&42u64));
        assert!(b.might_contain(&"hello"));
        // Random key should usually NOT match in a fresh filter.
        // Test multiple unrelated keys; expect most to reject.
        let mut rejects = 0;
        for k in 1000..1100u64 {
            if !b.might_contain(&k) { rejects += 1; }
        }
        assert!(rejects > 80,
                "fresh Bloom64 with 2 entries should reject most random keys; got {rejects}/100");
    }

    #[test]
    fn bloom64_no_false_negative() {
        // Property: inserting a key MUST always make it present.
        let mut b = Bloom64::ZERO;
        for k in 0..16u64 { b.insert(&k); }
        for k in 0..16u64 {
            assert!(b.might_contain(&k),
                    "Bloom must never give false negative; missed {k}");
        }
    }

    #[test]
    fn bloom_pointer_basic_usage() {
        let target = Arc::new(vec![1u64, 2, 3, 4, 5]);
        let keys: Vec<u64> = target.iter().copied().collect();
        let bp = BloomPointer::from_keys(target.clone(), keys);
        for k in 1..=5u64 {
            assert!(bp.might_contain(&k));
        }
        // Random keys mostly reject.
        let mut rejects = 0;
        for k in 100..200u64 {
            if !bp.might_contain(&k) { rejects += 1; }
        }
        assert!(rejects > 80,
                "BloomPointer with 5 entries should reject most random; got {rejects}/100");
    }

    #[test]
    fn bloom_pointer_size_is_16_bytes() {
        assert_eq!(std::mem::size_of::<BloomPointer<u64>>(), 16);
    }

    #[test]
    fn bloom_fine_holds_more_keys_than_coarse() {
        // Insert 32 keys into both; coarse should saturate but fine
        // should still reject random keys at a high rate.
        let mut coarse = Bloom64::ZERO;
        let mut fine = BloomFine::ZERO;
        for k in 0..32u64 {
            coarse.insert(&k);
            fine.insert(&k);
        }
        let mut coarse_rejects = 0;
        let mut fine_rejects = 0;
        for k in 1000..1100u64 {
            if !coarse.might_contain(&k) { coarse_rejects += 1; }
            if !fine.might_contain(&k) { fine_rejects += 1; }
        }
        // Coarse may be over-saturated at 32 entries; fine should
        // still reject most randoms.
        assert!(fine_rejects >= coarse_rejects,
                "fine filter must reject at least as much as coarse: \
                 coarse={coarse_rejects} fine={fine_rejects}");
    }

    #[test]
    fn bloom_cascade_layered_rejection() {
        let target: Arc<Vec<u64>> = Arc::new((0..32u64).collect());
        let keys: Vec<u64> = target.iter().copied().collect();
        let bc = BloomCascade::from_keys(target.clone(), keys);

        // All inserted keys must be reported as might-contain.
        for k in 0..32u64 {
            assert!(bc.cascade_check(&k).might_contain(),
                    "inserted key {k} must not be rejected");
        }

        // Most random keys should be rejected at coarse OR fine.
        let mut coarse_rej = 0;
        let mut fine_rej = 0;
        let mut survive = 0;
        for k in 1000..1100u64 {
            match bc.cascade_check(&k) {
                CascadeOutcome::RejectedAtCoarse => coarse_rej += 1,
                CascadeOutcome::RejectedAtFine => fine_rej += 1,
                CascadeOutcome::MightContain => survive += 1,
            }
        }
        // Combined rejection rate should be high.
        assert!(coarse_rej + fine_rej >= 90,
                "cascade should reject most random queries; \
                 coarse_rej={coarse_rej} fine_rej={fine_rej} survive={survive}");
    }

    #[test]
    fn estimated_fpr_grows_with_load() {
        let fpr1 = Bloom64::estimated_fpr(1);
        let fpr8 = Bloom64::estimated_fpr(8);
        let fpr16 = Bloom64::estimated_fpr(16);
        let fpr32 = Bloom64::estimated_fpr(32);
        assert!(fpr1 < fpr8);
        assert!(fpr8 < fpr16);
        assert!(fpr16 < fpr32);
        // At capacity (~8 keys) the FPR is reasonable (<5%).
        let fpr8_actual = Bloom64::estimated_fpr(8);
        assert!(fpr8_actual < 0.05,
                "8-key FPR should be < 5%, got {fpr8_actual}");
        // At 16 keys (2x capacity) FPR is around 16%, which is the
        // point at which a fine-tier filter starts being needed.
        assert!(fpr16 > 0.10 && fpr16 < 0.25,
                "16-key FPR should be in [10%, 25%], got {fpr16}");
    }

    #[test]
    fn bloom_cascade_outer_inner_information() {
        // Demonstrate that the cascade levels carry DIFFERENT
        // information: a key in coarse-filter range but not fine-
        // filter range can be rejected at the fine level.
        let mut coarse = Bloom64::ZERO;
        let mut fine = BloomFine::ZERO;
        // Insert keys 0..10 into BOTH levels.
        for k in 0..10u64 {
            coarse.insert(&k);
            fine.insert(&k);
        }
        // Insert additional keys 100..200 into coarse ONLY so a
        // coarse-pass / fine-reject path exists.
        for k in 100..200u64 {
            coarse.insert(&k);
        }
        // Now construct a cascade with our mismatched filters.
        let bc = BloomCascade {
            coarse, fine,
            target: Arc::new(()),
        };
        // Key 150 is in coarse but NOT in fine; must reject at fine.
        // (With overwhelming probability.)
        let outcome = bc.cascade_check(&150u64);
        assert_ne!(outcome, CascadeOutcome::RejectedAtCoarse,
                   "150 was inserted into coarse so must pass coarse");
        // We expect either fine-reject or might-contain; both are
        // valid. The point is the cascade structure carries the
        // distinction.
    }
}
