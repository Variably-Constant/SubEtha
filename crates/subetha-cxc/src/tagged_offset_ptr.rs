//! `TaggedOffsetPtr<T, const TAG_BITS: u32>` - high-bit-stealing
//! variant of [`OffsetPtr`](crate::OffsetPtr).
//!
//! Steals the TOP `TAG_BITS` bits of the u32 index for a small type
//! tag, leaving `(32 - TAG_BITS)` bits of index space.
//!
//! # Why high-bit stealing
//!
//! Classical tagged pointers steal the LOW bits because aligned
//! pointers have low bits guaranteed zero. We work with INDICES,
//! not addresses, so alignment is irrelevant. The natural free
//! bits in an index are the HIGH bits, because most regions don't
//! fill all 4 billion u32 slots. With `TAG_BITS = 4` you still
//! get 268M slots and 16 type IDs - plenty for most data
//! structures.
//!
//! # Typical sizes
//!
//! | TAG_BITS | Max tag | Max index | Typical use |
//! |---|---|---|---|
//! | 1 | 1 | 2.1B | Generation parity / dirty bit |
//! | 2 | 3 | 1.07B | 4-state machine |
//! | 3 | 7 | 537M | 8-color / 8-type discriminator |
//! | 4 | 15 | 268M | 16 node types in a tree |
//! | 8 | 255 | 16.7M | 256 distinct kinds; still huge index space |
//!
//! # Bit layout
//!
//! ```text
//!   bit 31                            bit 0
//!   [TAG_BITS][         32 - TAG_BITS         ]
//!     tag              index
//! ```
//!
//! NIL is `u32::MAX` (all-ones, both tag and index saturated).
//! Distinguishable from any meaningful `(tag, index)` pair as long
//! as the caller doesn't create one with tag == max_tag AND index ==
//! max_index. For safety, use `NIL` constant rather than constructing
//! all-ones manually.
//!
//! # Integration with SharedRegion
//!
//! Pass `ptr.index()` to `SharedRegion::get` / `set`. The tag bits
//! are caller-managed: type discriminator, state flag, color,
//! generation parity, whatever the data structure encodes.

use std::marker::PhantomData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaggedPtrError {
    TagOutOfRange,
    IndexOutOfRange,
}

/// A 32-bit position-independent pointer with `TAG_BITS` high bits
/// reserved for a caller-defined tag. Packs `(tag, index)` into one
/// `u32`. Cross-process safe: same raw bits resolve to the same
/// `(tag, index)` in every process.
#[derive(Debug)]
#[repr(C)]
pub struct TaggedOffsetPtr<T, const TAG_BITS: u32> {
    packed: u32,
    _phantom: PhantomData<T>,
}

impl<T, const TAG_BITS: u32> Clone for TaggedOffsetPtr<T, TAG_BITS> {
    fn clone(&self) -> Self { *self }
}
impl<T, const TAG_BITS: u32> Copy for TaggedOffsetPtr<T, TAG_BITS> {}
impl<T, const TAG_BITS: u32> PartialEq for TaggedOffsetPtr<T, TAG_BITS> {
    fn eq(&self, other: &Self) -> bool { self.packed == other.packed }
}
impl<T, const TAG_BITS: u32> Eq for TaggedOffsetPtr<T, TAG_BITS> {}
impl<T, const TAG_BITS: u32> std::hash::Hash for TaggedOffsetPtr<T, TAG_BITS> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.packed.hash(state);
    }
}

impl<T, const TAG_BITS: u32> TaggedOffsetPtr<T, TAG_BITS> {
    // Compile-time bounds check: TAG_BITS must be 0..=31. At 32 we'd
    // need a 33-bit shift which is UB; at >=32 the index space is
    // zero which has no useful meaning. This const item is evaluated
    // when the type is instantiated, blocking invalid TAG_BITS at
    // monomorphisation time.
    const _ASSERT_TAG_BITS: () = assert!(
        TAG_BITS <= 31,
        "TAG_BITS must be in 0..=31 (32 would leave no index bits)",
    );

    /// All-ones bit pattern serving as a NIL sentinel.
    pub const NIL: Self = Self { packed: u32::MAX, _phantom: PhantomData };

    /// Maximum tag value: `(1 << TAG_BITS) - 1`. Returns 0 when
    /// TAG_BITS == 0.
    pub const fn max_tag() -> u32 {
        if TAG_BITS == 0 { 0 } else { (1u32 << TAG_BITS) - 1 }
    }

    /// Maximum index value: `(1 << (32 - TAG_BITS)) - 1`. Returns
    /// `u32::MAX` when TAG_BITS == 0.
    pub const fn max_index() -> u32 {
        let idx_bits = 32 - TAG_BITS;
        if idx_bits == 32 { u32::MAX } else { (1u32 << idx_bits) - 1 }
    }

    /// Bit mask covering the index portion of the packed word.
    #[inline]
    pub const fn index_mask() -> u32 { Self::max_index() }

    /// Bit shift for the tag (== 32 - TAG_BITS).
    #[inline]
    pub const fn tag_shift() -> u32 { 32 - TAG_BITS }

    /// Construct from `(index, tag)`. Panics if either component
    /// exceeds its range. Use [`try_new`](Self::try_new) for
    /// fallible construction.
    pub fn new(index: u32, tag: u32) -> Self {
        // Force the const-eval ASSERT to fire if TAG_BITS is invalid.
        let _: () = Self::_ASSERT_TAG_BITS;
        assert!(
            tag <= Self::max_tag(),
            "tag {tag} exceeds MAX_TAG {} (TAG_BITS={TAG_BITS})",
            Self::max_tag(),
        );
        assert!(
            index <= Self::max_index(),
            "index {index} exceeds MAX_INDEX {} (TAG_BITS={TAG_BITS})",
            Self::max_index(),
        );
        let packed = if TAG_BITS == 0 {
            // Edge case: TAG_BITS=0 means no tag bits; the whole
            // word is the index.
            index
        } else {
            (tag << Self::tag_shift()) | index
        };
        Self { packed, _phantom: PhantomData }
    }

    /// Fallible construction. Returns `Err(TagOutOfRange)` or
    /// `Err(IndexOutOfRange)` instead of panicking.
    pub fn try_new(index: u32, tag: u32) -> Result<Self, TaggedPtrError> {
        if tag > Self::max_tag() { return Err(TaggedPtrError::TagOutOfRange); }
        if index > Self::max_index() { return Err(TaggedPtrError::IndexOutOfRange); }
        let packed = if TAG_BITS == 0 {
            index
        } else {
            (tag << Self::tag_shift()) | index
        };
        Ok(Self { packed, _phantom: PhantomData })
    }

    /// Construct from the raw packed `u32` representation. Useful
    /// for deserialisation. Caller is responsible for ensuring the
    /// raw value is meaningful for the chosen `TAG_BITS`.
    #[inline]
    pub const fn from_raw(packed: u32) -> Self {
        Self { packed, _phantom: PhantomData }
    }

    /// Extract the raw packed `u32`. Useful for serialisation.
    #[inline]
    pub const fn raw(self) -> u32 { self.packed }

    /// Extract the index portion.
    #[inline]
    pub fn index(self) -> u32 { self.packed & Self::index_mask() }

    /// Extract the tag portion.
    #[inline]
    pub fn tag(self) -> u32 {
        if TAG_BITS == 0 { 0 } else { self.packed >> Self::tag_shift() }
    }

    /// Return a new pointer with the same index but a different tag.
    pub fn with_tag(self, new_tag: u32) -> Self {
        Self::new(self.index(), new_tag)
    }

    /// Return a new pointer with the same tag but a different index.
    pub fn with_index(self, new_index: u32) -> Self {
        Self::new(new_index, self.tag())
    }

    /// True when the pointer is the NIL sentinel.
    #[inline]
    pub fn is_nil(self) -> bool { self.packed == u32::MAX }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_bits_4_max_tag_and_index() {
        type P = TaggedOffsetPtr<u64, 4>;
        assert_eq!(P::max_tag(), 15);  // 2^4 - 1
        assert_eq!(P::max_index(), (1u32 << 28) - 1);  // 268_435_455
        assert_eq!(P::tag_shift(), 28);
    }

    #[test]
    fn tag_bits_0_degenerates_to_offset_ptr() {
        type P = TaggedOffsetPtr<u64, 0>;
        assert_eq!(P::max_tag(), 0);
        assert_eq!(P::max_index(), u32::MAX);
        // Only valid tag is 0.
        let p = P::new(123, 0);
        assert_eq!(p.index(), 123);
        assert_eq!(p.tag(), 0);
    }

    #[test]
    fn pack_unpack_round_trip_tag_bits_4() {
        type P = TaggedOffsetPtr<u64, 4>;
        let p = P::new(42, 7);
        assert_eq!(p.index(), 42);
        assert_eq!(p.tag(), 7);
    }

    #[test]
    fn pack_unpack_round_trip_tag_bits_8() {
        type P = TaggedOffsetPtr<u64, 8>;
        assert_eq!(P::max_tag(), 255);
        assert_eq!(P::max_index(), (1u32 << 24) - 1);
        let p = P::new(99_999, 200);
        assert_eq!(p.index(), 99_999);
        assert_eq!(p.tag(), 200);
    }

    #[test]
    fn raw_round_trip() {
        type P = TaggedOffsetPtr<u64, 4>;
        let p = P::new(42, 7);
        let raw = p.raw();
        let q = P::from_raw(raw);
        assert_eq!(p, q);
        assert_eq!(q.index(), 42);
        assert_eq!(q.tag(), 7);
    }

    #[test]
    fn with_tag_keeps_index() {
        type P = TaggedOffsetPtr<u64, 4>;
        let p = P::new(42, 7);
        let q = p.with_tag(3);
        assert_eq!(q.index(), 42);
        assert_eq!(q.tag(), 3);
    }

    #[test]
    fn with_index_keeps_tag() {
        type P = TaggedOffsetPtr<u64, 4>;
        let p = P::new(42, 7);
        let q = p.with_index(100);
        assert_eq!(q.index(), 100);
        assert_eq!(q.tag(), 7);
    }

    #[test]
    fn try_new_rejects_oversized_tag() {
        type P = TaggedOffsetPtr<u64, 4>;
        assert_eq!(P::try_new(0, 16).err(), Some(TaggedPtrError::TagOutOfRange));
        assert_eq!(P::try_new(0, 999).err(), Some(TaggedPtrError::TagOutOfRange));
    }

    #[test]
    fn try_new_rejects_oversized_index() {
        type P = TaggedOffsetPtr<u64, 4>;
        let max = P::max_index();
        assert!(P::try_new(max, 0).is_ok());
        assert_eq!(P::try_new(max + 1, 0).err(), Some(TaggedPtrError::IndexOutOfRange));
    }

    #[test]
    #[should_panic(expected = "tag")]
    fn new_panics_on_oversized_tag() {
        type P = TaggedOffsetPtr<u64, 4>;
        let _p = P::new(0, 999);
    }

    #[test]
    #[should_panic(expected = "index")]
    fn new_panics_on_oversized_index() {
        type P = TaggedOffsetPtr<u64, 4>;
        let _p = P::new(u32::MAX, 0);
    }

    #[test]
    fn nil_is_all_ones_and_detectable() {
        type P = TaggedOffsetPtr<u64, 4>;
        let n = P::NIL;
        assert!(n.is_nil());
        assert_eq!(n.raw(), u32::MAX);
        let p = P::new(0, 0);
        assert!(!p.is_nil());
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        type P = TaggedOffsetPtr<u64, 4>;
        let a = P::new(5, 1);
        let b = P::new(5, 1);
        let c = P::new(5, 2);
        let d = P::new(6, 1);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        let mut s = HashSet::new();
        s.insert(a);
        assert!(s.contains(&b));
        assert!(!s.contains(&c));
        assert!(!s.contains(&d));
    }

    #[test]
    fn boundary_index_at_max_for_tag_bits_4() {
        type P = TaggedOffsetPtr<u64, 4>;
        let max_idx = P::max_index();
        let p = P::new(max_idx, 0);
        assert_eq!(p.index(), max_idx);
        assert_eq!(p.tag(), 0);
        // Max tag with max index.
        let max_tag = P::max_tag();
        let p2 = P::new(max_idx, max_tag);
        assert_eq!(p2.index(), max_idx);
        assert_eq!(p2.tag(), max_tag);
    }

    #[test]
    fn integration_with_shared_region_via_index_extraction() {
        use crate::SharedRegion;
        use std::path::PathBuf;

        let mut p: PathBuf = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-tagged-region-{pid}.bin"));

        // SharedRegion holding heterogeneous nodes; tag discriminates
        // 4 node kinds (Leaf=0, Internal=1, Tombstone=2, Sentinel=3).
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Node { key: u64, value: u64 }

        let r: SharedRegion<Node> = SharedRegion::create(&p, 16).unwrap();
        // Allocate a slot; wrap the returned OffsetPtr's index in
        // a TaggedOffsetPtr<Node, 2> with tag=1 (Internal).
        let inner = r.allocate(Node { key: 42, value: 100 }).unwrap();
        type P = TaggedOffsetPtr<Node, 2>;
        let tagged = P::new(inner.index, 1);
        assert_eq!(tagged.tag(), 1);
        // Resolve back: use the index to query SharedRegion.
        let n = r.get(crate::OffsetPtr::new(tagged.index())).unwrap();
        assert_eq!(n, Node { key: 42, value: 100 });
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_process_position_independence_via_raw_bits() {
        // The same packed u32 resolves to the same (index, tag) in
        // any process. Demonstrated by raw round-trip; in real use
        // the u32 is the cross-process-stable representation.
        type P = TaggedOffsetPtr<u64, 4>;
        let producer = P::new(1234, 9);
        let raw = producer.raw();
        // (any other process would do: P::from_raw(raw))
        let consumer = P::from_raw(raw);
        assert_eq!(consumer.index(), 1234);
        assert_eq!(consumer.tag(), 9);
    }

    #[test]
    fn tag_bits_1_dirty_bit_pattern() {
        // A common use: 1 bit for a dirty/clean state flag.
        type P = TaggedOffsetPtr<u64, 1>;
        assert_eq!(P::max_tag(), 1);
        assert_eq!(P::max_index(), i32::MAX as u32);  // 2^31 - 1
        let clean = P::new(42, 0);
        let dirty = clean.with_tag(1);
        assert_eq!(dirty.index(), clean.index());
        assert_ne!(dirty, clean);
        assert_eq!(dirty.tag(), 1);
    }
}
