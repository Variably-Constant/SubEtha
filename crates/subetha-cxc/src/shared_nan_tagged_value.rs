//! `SharedNaNTaggedValue` - NaN-boxed value where the pointer
//! payload is a [`TaggedOffsetPtr`].
//!
//! Composition primitive built on
//! [`SharedNaNValue`] +
//! [`TaggedOffsetPtr`].
//! Two-level discrimination in a single 64-bit cross-process word:
//!
//! - **Outer tag** (NaN exponent + qNaN bit + 3-bit tag in the
//!   mantissa): the broad type (f64 / nil / i32 / u32 / bool /
//!   OffsetPtr / **TaggedOffsetPtr**).
//! - **Inner tag** (top `N` bits of the index payload when the
//!   outer tag is TaggedOffsetPtr): the fine type within the
//!   pointer family - e.g. `0=Leaf`, `1=Internal`, `2=Tombstone`
//!   in a B-tree.
//!
//! # Encoding
//!
//! Uses the tag-5 reserved slot from [`SharedNaNValue`]:
//! - Outer prefix 0xFFF8 (boxed marker) + outer tag 5
//!   (TaggedOffsetPtr variant).
//! - Lower 32 bits hold the `TaggedOffsetPtr<_, TAG_BITS>::raw()`
//!   packed value.
//! - `TAG_BITS` is type-statically known at construction AND
//!   extraction (caller passes it as a const generic on the
//!   `as_tagged_offset_ptr` call).
//!
//! # Why this composition matters
//!
//! Dynamic-language-style heterogeneous values + fine-grained
//! typed-pointer discrimination in 64 bits total, with zero heap
//! allocation and position-independent across processes. This is
//! strictly more expressive than V8/SpiderMonkey NaN boxing (which
//! is single-process) AND than naive `Box<dyn Trait>` polymorphism
//! (which costs ~24 bytes per slot for box+vtable plus a heap
//! allocation).
//!
//! # Use cases
//!
//! - Heterogeneous graph nodes:
//!   `SharedHashMap<u64, SharedNaNTaggedValue>` where each value
//!   can be a scalar OR a pointer into a typed region with
//!   multiple node kinds (Leaf/Internal/Tombstone, etc.).
//! - JIT-compiled scripting where typed-pointer variants
//!   distinguish object shapes.
//! - Tagged-union values inside `SharedRegion<SharedNaNTaggedValue>`
//!   for embedded JSON-style state.

use crate::shared_nan_value::{
    SharedNaNValue, BOXED_MASK, BOXED_PREFIX,
    TAG_MASK, TAG_SHIFT, TAG_TAGGED_OFFSET_PTR,
};
// Test code references NaNValueType separately; pull it in where used.
#[cfg(test)]
use crate::shared_nan_value::NaNValueType;
use crate::shared_region::OffsetPtr;
use crate::tagged_offset_ptr::TaggedOffsetPtr;

/// 64-bit composite value: SharedNaNValue + TaggedOffsetPtr variant.
/// Layout-compatible with `SharedNaNValue` (same boxed-prefix mask,
/// same tag positions); adds tag-5 as a TaggedOffsetPtr slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct SharedNaNTaggedValue {
    raw: u64,
}

#[inline]
const fn pack(tag: u64, payload: u64) -> u64 {
    BOXED_PREFIX | (tag << TAG_SHIFT) | (payload & 0x0000_FFFF_FFFF_FFFF)
}

impl SharedNaNTaggedValue {
    /// The nil value (same encoding as SharedNaNValue::NIL).
    pub const NIL: Self = Self { raw: pack(crate::shared_nan_value::TAG_NIL, 0) };

    // ===== Pass-through constructors for NaNValue variants =====

    pub fn from_f64(v: f64) -> Self {
        Self { raw: SharedNaNValue::from_f64(v).raw() }
    }
    pub const fn from_i32(v: i32) -> Self {
        Self { raw: SharedNaNValue::from_i32(v).raw() }
    }
    pub const fn from_u32(v: u32) -> Self {
        Self { raw: SharedNaNValue::from_u32(v).raw() }
    }
    pub const fn from_bool(v: bool) -> Self {
        Self { raw: SharedNaNValue::from_bool(v).raw() }
    }
    pub fn from_offset_ptr<T>(p: OffsetPtr<T>) -> Self {
        Self { raw: SharedNaNValue::from_offset_ptr(p).raw() }
    }

    /// Construct from a TaggedOffsetPtr. The inner TAG_BITS is
    /// type-statically known; the caller must remember it (or
    /// type-erase it at the same call site) to extract.
    pub fn from_tagged_offset_ptr<T, const TAG_BITS: u32>(
        p: TaggedOffsetPtr<T, TAG_BITS>,
    ) -> Self {
        Self { raw: pack(TAG_TAGGED_OFFSET_PTR, p.raw() as u64) }
    }

    // ===== Raw & conversion =====

    #[inline]
    pub const fn from_raw(raw: u64) -> Self { Self { raw } }
    #[inline]
    pub const fn raw(self) -> u64 { self.raw }

    /// Reinterpret as a SharedNaNValue (loses the TaggedOffsetPtr
    /// discriminator IF the value is tag-5; the resulting NaNValue's
    /// `type_tag()` will report `Reserved(5)`).
    pub fn to_nan_value(self) -> SharedNaNValue {
        SharedNaNValue::from_raw(self.raw)
    }

    /// Lift a SharedNaNValue into a SharedNaNTaggedValue. Always
    /// preserves the value because NaNTaggedValue is a strict
    /// superset of NaNValue's encoding.
    pub fn from_nan_value(v: SharedNaNValue) -> Self {
        Self { raw: v.raw() }
    }

    // ===== Type queries =====

    #[inline]
    fn is_boxed(self) -> bool { (self.raw & BOXED_MASK) == BOXED_PREFIX }

    #[inline]
    fn tag(self) -> u64 { (self.raw >> TAG_SHIFT) & TAG_MASK }

    pub fn type_tag(self) -> NaNTaggedType {
        if !self.is_boxed() { return NaNTaggedType::F64; }
        match self.tag() {
            crate::shared_nan_value::TAG_NIL => NaNTaggedType::Nil,
            crate::shared_nan_value::TAG_I32 => NaNTaggedType::I32,
            crate::shared_nan_value::TAG_U32 => NaNTaggedType::U32,
            crate::shared_nan_value::TAG_BOOL => NaNTaggedType::Bool,
            crate::shared_nan_value::TAG_OFFSET_PTR => NaNTaggedType::OffsetPtr,
            crate::shared_nan_value::TAG_TAGGED_OFFSET_PTR
                => NaNTaggedType::TaggedOffsetPtr,
            other => NaNTaggedType::Reserved(other),
        }
    }

    pub fn is_f64(self) -> bool { self.to_nan_value().is_f64() }
    pub fn is_nil(self) -> bool { self.to_nan_value().is_nil() }
    pub fn is_i32(self) -> bool { self.to_nan_value().is_i32() }
    pub fn is_u32(self) -> bool { self.to_nan_value().is_u32() }
    pub fn is_bool(self) -> bool { self.to_nan_value().is_bool() }
    pub fn is_offset_ptr(self) -> bool { self.to_nan_value().is_offset_ptr() }
    pub fn is_tagged_offset_ptr(self) -> bool {
        self.is_boxed() && self.tag() == TAG_TAGGED_OFFSET_PTR
    }

    // ===== Extractors =====

    pub fn as_f64(self) -> Option<f64> { self.to_nan_value().as_f64() }
    pub fn as_i32(self) -> Option<i32> { self.to_nan_value().as_i32() }
    pub fn as_u32(self) -> Option<u32> { self.to_nan_value().as_u32() }
    pub fn as_bool(self) -> Option<bool> { self.to_nan_value().as_bool() }
    pub fn as_offset_ptr<T>(self) -> Option<OffsetPtr<T>> {
        self.to_nan_value().as_offset_ptr()
    }

    /// Extract a TaggedOffsetPtr. Caller specifies T and TAG_BITS;
    /// these must match what was used at construction (the type
    /// is erased in the boxed value but the bits are stable).
    pub fn as_tagged_offset_ptr<T, const TAG_BITS: u32>(
        self,
    ) -> Option<TaggedOffsetPtr<T, TAG_BITS>> {
        if self.is_tagged_offset_ptr() {
            Some(TaggedOffsetPtr::<T, TAG_BITS>::from_raw(
                (self.raw & 0xFFFF_FFFF) as u32,
            ))
        } else { None }
    }
}

impl Default for SharedNaNTaggedValue {
    fn default() -> Self { Self::NIL }
}

/// Discriminator for SharedNaNTaggedValue. Superset of
/// [`NaNValueType`](crate::NaNValueType) with one additional variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NaNTaggedType {
    F64,
    Nil,
    I32,
    U32,
    Bool,
    OffsetPtr,
    TaggedOffsetPtr,
    Reserved(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifts_nan_value_losslessly() {
        let nv = SharedNaNValue::from_i32(42);
        let ntv = SharedNaNTaggedValue::from_nan_value(nv);
        assert_eq!(ntv.raw(), nv.raw());
        assert_eq!(ntv.as_i32(), Some(42));
    }

    #[test]
    fn passthrough_constructors_match_nan_value() {
        // Each pass-through constructor produces the same bits as
        // SharedNaNValue's equivalent.
        for (a, b) in [
            (SharedNaNTaggedValue::from_i32(7).raw(),
             SharedNaNValue::from_i32(7).raw()),
            (SharedNaNTaggedValue::from_u32(99).raw(),
             SharedNaNValue::from_u32(99).raw()),
            (SharedNaNTaggedValue::from_bool(true).raw(),
             SharedNaNValue::from_bool(true).raw()),
            (SharedNaNTaggedValue::NIL.raw(),
             SharedNaNValue::NIL.raw()),
        ] {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn tagged_offset_ptr_round_trip() {
        let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(42, 7);
        let v = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        assert!(v.is_tagged_offset_ptr());
        assert_eq!(v.type_tag(), NaNTaggedType::TaggedOffsetPtr);
        let p2: TaggedOffsetPtr<u64, 4> = v.as_tagged_offset_ptr().unwrap();
        assert_eq!(p, p2);
        assert_eq!(p2.index(), 42);
        assert_eq!(p2.tag(), 7);
    }

    #[test]
    fn tagged_offset_ptr_with_different_tag_bits_round_trip() {
        let p: TaggedOffsetPtr<u32, 8> = TaggedOffsetPtr::new(1000, 200);
        let v = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        let p2: TaggedOffsetPtr<u32, 8> = v.as_tagged_offset_ptr().unwrap();
        assert_eq!(p2.index(), 1000);
        assert_eq!(p2.tag(), 200);
    }

    #[test]
    fn other_variants_still_work_without_tagged_ptr() {
        let f = SharedNaNTaggedValue::from_f64(2.5);
        assert_eq!(f.as_f64(), Some(2.5));
        assert!(!f.is_tagged_offset_ptr());

        let i = SharedNaNTaggedValue::from_i32(-7);
        assert_eq!(i.as_i32(), Some(-7));
        assert!(!i.is_tagged_offset_ptr());

        let n = SharedNaNTaggedValue::NIL;
        assert!(n.is_nil());
        assert!(!n.is_tagged_offset_ptr());
    }

    #[test]
    fn nil_variant_pass_through() {
        let n = SharedNaNTaggedValue::NIL;
        assert!(n.is_nil());
        assert_eq!(n.type_tag(), NaNTaggedType::Nil);
        assert_eq!(SharedNaNTaggedValue::default(), n);
    }

    #[test]
    fn raw_round_trip() {
        let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(99, 3);
        let v = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        let raw = v.raw();
        let v2 = SharedNaNTaggedValue::from_raw(raw);
        assert_eq!(v, v2);
        let p2: TaggedOffsetPtr<u64, 4> = v2.as_tagged_offset_ptr().unwrap();
        assert_eq!(p2.index(), 99);
        assert_eq!(p2.tag(), 3);
    }

    #[test]
    fn wrong_extractor_returns_none() {
        let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(42, 7);
        let v = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        assert_eq!(v.as_i32(), None);
        assert_eq!(v.as_f64(), None);
        assert_eq!(v.as_bool(), None);
        assert_eq!(v.as_offset_ptr::<u64>(), None);
    }

    #[test]
    fn to_nan_value_loses_tagged_discriminator_but_preserves_bits() {
        let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(42, 7);
        let v = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        let nv = v.to_nan_value();
        // The NaNValue sees tag-5 as Reserved.
        assert_eq!(nv.type_tag(), NaNValueType::Reserved(5));
        // But the bits are preserved.
        assert_eq!(nv.raw(), v.raw());
    }

    #[test]
    fn cross_process_via_shared_hash_map_with_typed_pointer_kinds() {
        use crate::{SharedHashMap, SharedRegion};
        let mut shm_p = std::env::temp_dir();
        shm_p.push(format!("subetha-nantagged-shm-{}.bin", std::process::id()));
        let mut reg_p = std::env::temp_dir();
        reg_p.push(format!("subetha-nantagged-reg-{}.bin", std::process::id()));

        // Region holding heterogeneous nodes; tag (TAG_BITS=2)
        // discriminates 4 kinds: 0=Leaf, 1=Internal, 2=Tombstone,
        // 3=Sentinel.
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Node { key: u64, value: u64 }
        let region: SharedRegion<Node> = SharedRegion::create(&reg_p, 16).unwrap();

        let m: SharedHashMap<u32, SharedNaNTaggedValue>
            = SharedHashMap::create(&shm_p, 32).unwrap();

        // Allocate one node as Internal (tag=1).
        let inner = region.allocate(Node { key: 42, value: 100 }).unwrap();
        let tagged: TaggedOffsetPtr<Node, 2> = TaggedOffsetPtr::new(inner.index, 1);
        m.insert(0, SharedNaNTaggedValue::from_tagged_offset_ptr(tagged)).unwrap();
        // Also store other variants in adjacent keys.
        m.insert(1, SharedNaNTaggedValue::from_i32(7)).unwrap();
        m.insert(2, SharedNaNTaggedValue::from_f64(2.5)).unwrap();
        m.insert(3, SharedNaNTaggedValue::NIL).unwrap();

        // Retrieve and dispatch by type.
        let v0 = m.get(&0).unwrap();
        assert_eq!(v0.type_tag(), NaNTaggedType::TaggedOffsetPtr);
        let p: TaggedOffsetPtr<Node, 2> = v0.as_tagged_offset_ptr().unwrap();
        assert_eq!(p.tag(), 1); // Internal kind
        let n = region.get(OffsetPtr::new(p.index())).unwrap();
        assert_eq!(n, Node { key: 42, value: 100 });

        assert_eq!(m.get(&1).unwrap().as_i32(), Some(7));
        assert_eq!(m.get(&2).unwrap().as_f64(), Some(2.5));
        assert!(m.get(&3).unwrap().is_nil());

        std::fs::remove_file(&shm_p).ok();
        std::fs::remove_file(&reg_p).ok();
    }

    #[test]
    fn size_is_8_bytes() {
        assert_eq!(std::mem::size_of::<SharedNaNTaggedValue>(), 8);
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(1, 2);
        let a = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        let b = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
        let c = SharedNaNTaggedValue::from_tagged_offset_ptr(
            TaggedOffsetPtr::<u64, 4>::new(1, 3));
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut s = HashSet::new();
        s.insert(a);
        assert!(s.contains(&b));
        assert!(!s.contains(&c));
    }
}
