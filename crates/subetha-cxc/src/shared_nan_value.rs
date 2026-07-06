//! `SharedNaNValue` - 64-bit NaN-boxed heterogeneous value cell.
//!
//! Packs `f64 | i32 | u32 | bool | nil | OffsetPtr<T>` into a single
//! `u64`, distinguishing types via the IEEE 754 NaN bit patterns the
//! FPU never produces during normal computation.
//!
//! # Encoding
//!
//! ```text
//!   bit 63        bit 51    bits 50-48    bits 47-0
//!   [sign=1][exp=0x7FF][qNaN=1][ tag(3) ][   payload (48)   ]
//! ```
//!
//! The boxed prefix is `0xFFF8_0000_0000_0000` (sign=1 + all-ones
//! exponent + qNaN bit). Real float NaNs from computation usually
//! have sign=0, so we don't collide with them. To be safe, every
//! `from_f64(NaN)` canonicalises the bit pattern to
//! `0x7FF8_0000_0000_0000` (positive canonical qNaN) so the stored
//! bits never look boxed when they aren't.
//!
//! # Type tags (3 bits, 8 slots; 6 used, 2 reserved)
//!
//! | tag | meaning   | payload encoding              |
//! |-----|-----------|-------------------------------|
//! | 0   | nil       | payload bits ignored (all 0)  |
//! | 1   | i32       | low 32 bits                   |
//! | 2   | u32       | low 32 bits                   |
//! | 3   | bool      | low 1 bit                     |
//! | 4   | OffsetPtr | low 32 bits = index           |
//! | 5   | reserved  | for TaggedOffsetPtr           |
//! | 6   | reserved  |                               |
//! | 7   | reserved  |                               |
//!
//! # Cross-process angle
//!
//! When tag = 4 (OffsetPtr), the payload is a 32-bit INDEX, not a
//! virtual address. Same `u64` bit pattern resolves to the same
//! pointer in every process that maps the underlying SharedRegion.
//! That's what makes this primitive cross-process safe where V8 /
//! SpiderMonkey NaN boxing is single-process only.
//!
//! # Use cases
//!
//! - Cross-process scripting / dynamic-language interpreters.
//! - Heterogeneous config maps:
//!   `SharedHashMap<K, SharedNaNValue>` where V can be int/float/
//!   bool/ptr without per-variant storage.
//! - Tagged-union slots in shared state.
//! - Weakly-typed message payloads in event streams.

use crate::shared_region::OffsetPtr;

/// Mask covering the boxed-marker prefix (top 13 bits).
pub const BOXED_MASK: u64 = 0xFFF8_0000_0000_0000;
/// The exact bit pattern that marks a boxed value.
pub const BOXED_PREFIX: u64 = 0xFFF8_0000_0000_0000;
/// Bit position of the type tag.
pub const TAG_SHIFT: u64 = 48;
/// Mask for the 3-bit tag once shifted into low bits.
pub const TAG_MASK: u64 = 0x7;
/// Mask for the 48-bit payload.
pub const PAYLOAD_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

/// Canonical positive qNaN. Any NaN input to `from_f64` is rewritten
/// to this so we never accidentally write a bit pattern that looks
/// boxed.
pub const CANONICAL_QNAN: u64 = 0x7FF8_0000_0000_0000;

// Tag constants.
pub const TAG_NIL: u64 = 0;
pub const TAG_I32: u64 = 1;
pub const TAG_U32: u64 = 2;
pub const TAG_BOOL: u64 = 3;
pub const TAG_OFFSET_PTR: u64 = 4;
pub const TAG_TAGGED_OFFSET_PTR: u64 = 5;  // reserved for TaggedOffsetPtr

/// Discriminator for a SharedNaNValue's payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NaNValueType {
    F64,
    Nil,
    I32,
    U32,
    Bool,
    OffsetPtr,
    Reserved(u64),
}

/// 64-bit NaN-boxed value. Stores one of: f64, i32, u32, bool, nil,
/// or `OffsetPtr<T>`. Discriminated via the IEEE 754 NaN bit pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct SharedNaNValue {
    raw: u64,
}

#[inline]
const fn pack(tag: u64, payload: u64) -> u64 {
    BOXED_PREFIX | (tag << TAG_SHIFT) | (payload & PAYLOAD_MASK)
}

impl SharedNaNValue {
    /// The nil value.
    pub const NIL: Self = Self { raw: pack(TAG_NIL, 0) };

    // ----- constructors -----

    /// Wrap an `f64`. NaN inputs are canonicalised to the positive
    /// canonical qNaN so the resulting bits never look boxed.
    pub fn from_f64(v: f64) -> Self {
        if v.is_nan() {
            // Force any NaN to a known-not-boxed pattern.
            Self { raw: CANONICAL_QNAN }
        } else {
            Self { raw: v.to_bits() }
        }
    }

    pub const fn from_i32(v: i32) -> Self {
        // Cast to u64 with sign extension limited to low 32 bits;
        // mask to fit in 32 bits.
        Self { raw: pack(TAG_I32, (v as u32) as u64) }
    }

    pub const fn from_u32(v: u32) -> Self {
        Self { raw: pack(TAG_U32, v as u64) }
    }

    pub const fn from_bool(v: bool) -> Self {
        Self { raw: pack(TAG_BOOL, v as u64) }
    }

    /// Wrap an OffsetPtr by storing its 32-bit index. The T parameter
    /// is type-erased; callers reconstruct it on extraction.
    pub fn from_offset_ptr<T>(p: OffsetPtr<T>) -> Self {
        Self { raw: pack(TAG_OFFSET_PTR, p.index as u64) }
    }

    /// Construct from raw bits. Useful for serialisation /
    /// cross-process passing.
    #[inline]
    pub const fn from_raw(raw: u64) -> Self { Self { raw } }

    /// Get the raw u64 representation.
    #[inline]
    pub const fn raw(self) -> u64 { self.raw }

    // ----- type queries -----

    #[inline]
    fn is_boxed(self) -> bool {
        (self.raw & BOXED_MASK) == BOXED_PREFIX
    }

    #[inline]
    fn tag(self) -> u64 {
        (self.raw >> TAG_SHIFT) & TAG_MASK
    }

    pub fn type_tag(self) -> NaNValueType {
        if !self.is_boxed() { return NaNValueType::F64; }
        match self.tag() {
            TAG_NIL => NaNValueType::Nil,
            TAG_I32 => NaNValueType::I32,
            TAG_U32 => NaNValueType::U32,
            TAG_BOOL => NaNValueType::Bool,
            TAG_OFFSET_PTR => NaNValueType::OffsetPtr,
            other => NaNValueType::Reserved(other),
        }
    }

    pub fn is_f64(self) -> bool { !self.is_boxed() }
    pub fn is_nil(self) -> bool { self.is_boxed() && self.tag() == TAG_NIL }
    pub fn is_i32(self) -> bool { self.is_boxed() && self.tag() == TAG_I32 }
    pub fn is_u32(self) -> bool { self.is_boxed() && self.tag() == TAG_U32 }
    pub fn is_bool(self) -> bool { self.is_boxed() && self.tag() == TAG_BOOL }
    pub fn is_offset_ptr(self) -> bool {
        self.is_boxed() && self.tag() == TAG_OFFSET_PTR
    }

    // ----- extractors -----

    pub fn as_f64(self) -> Option<f64> {
        if self.is_f64() { Some(f64::from_bits(self.raw)) } else { None }
    }

    pub fn as_i32(self) -> Option<i32> {
        if self.is_i32() {
            Some((self.raw & 0xFFFF_FFFF) as u32 as i32)
        } else { None }
    }

    pub fn as_u32(self) -> Option<u32> {
        if self.is_u32() {
            Some((self.raw & 0xFFFF_FFFF) as u32)
        } else { None }
    }

    pub fn as_bool(self) -> Option<bool> {
        if self.is_bool() { Some((self.raw & 1) != 0) } else { None }
    }

    pub fn as_offset_ptr<T>(self) -> Option<OffsetPtr<T>> {
        if self.is_offset_ptr() {
            Some(OffsetPtr::new((self.raw & 0xFFFF_FFFF) as u32))
        } else { None }
    }
}

impl Default for SharedNaNValue {
    fn default() -> Self { Self::NIL }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_region::OffsetPtr;

    #[test]
    fn nil_round_trip() {
        let v = SharedNaNValue::NIL;
        assert!(v.is_nil());
        assert_eq!(v.type_tag(), NaNValueType::Nil);
        assert_eq!(SharedNaNValue::default(), SharedNaNValue::NIL);
    }

    #[test]
    fn i32_round_trip_positive_and_negative() {
        for v in [0i32, 1, -1, 42, -42, i32::MAX, i32::MIN] {
            let n = SharedNaNValue::from_i32(v);
            assert!(n.is_i32(), "{v} should be i32");
            assert_eq!(n.as_i32(), Some(v));
            assert_eq!(n.type_tag(), NaNValueType::I32);
        }
    }

    #[test]
    fn u32_round_trip() {
        for v in [0u32, 1, 42, u32::MAX, u32::MAX / 2] {
            let n = SharedNaNValue::from_u32(v);
            assert!(n.is_u32());
            assert_eq!(n.as_u32(), Some(v));
            assert_eq!(n.type_tag(), NaNValueType::U32);
        }
    }

    #[test]
    fn bool_round_trip() {
        let t = SharedNaNValue::from_bool(true);
        let f = SharedNaNValue::from_bool(false);
        assert!(t.is_bool() && f.is_bool());
        assert_eq!(t.as_bool(), Some(true));
        assert_eq!(f.as_bool(), Some(false));
    }

    #[test]
    fn f64_round_trip_normal_values() {
        for v in [0.0f64, 1.0, -1.0, std::f64::consts::PI, 1e100, -1e-100,
                  f64::INFINITY, f64::NEG_INFINITY] {
            let n = SharedNaNValue::from_f64(v);
            assert!(n.is_f64(), "{v} should be f64");
            assert_eq!(n.as_f64(), Some(v));
            assert_eq!(n.type_tag(), NaNValueType::F64);
        }
    }

    #[test]
    fn f64_nan_canonicalised() {
        // A specific NaN input gets canonicalised; we lose the
        // specific bit pattern but the result is still .is_nan().
        let n = SharedNaNValue::from_f64(f64::NAN);
        assert!(n.is_f64());
        let extracted = n.as_f64().unwrap();
        assert!(extracted.is_nan());
        assert_eq!(n.raw(), CANONICAL_QNAN);
    }

    #[test]
    fn f64_with_sign_1_nan_doesnt_collide_with_boxed() {
        // Construct a "boxed-looking" NaN by hand. from_f64 should
        // detect it as NaN and canonicalise.
        let evil = f64::from_bits(0xFFF8_FFFF_FFFF_FFFF);
        assert!(evil.is_nan());
        let n = SharedNaNValue::from_f64(evil);
        assert!(n.is_f64());
        // After canonicalisation it's the positive canonical qNaN.
        assert_eq!(n.raw(), CANONICAL_QNAN);
    }

    #[test]
    fn offset_ptr_round_trip() {
        let p: OffsetPtr<u64> = OffsetPtr::new(42);
        let n = SharedNaNValue::from_offset_ptr(p);
        assert!(n.is_offset_ptr());
        let p2: OffsetPtr<u64> = n.as_offset_ptr().unwrap();
        assert_eq!(p, p2);
        assert_eq!(p2.index, 42);
    }

    #[test]
    fn offset_ptr_phantom_type_erased_then_reconstructed() {
        // T is erased in the boxed value; caller reconstructs with
        // any T at extraction time.
        let p: OffsetPtr<u64> = OffsetPtr::new(0xABCD);
        let n = SharedNaNValue::from_offset_ptr(p);
        // Extract as a different T.
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct Foo { x: u32 }
        let p2: OffsetPtr<Foo> = n.as_offset_ptr().unwrap();
        assert_eq!(p2.index, 0xABCD);
    }

    #[test]
    fn raw_round_trip_preserves_value() {
        let n = SharedNaNValue::from_i32(-7);
        let raw = n.raw();
        let restored = SharedNaNValue::from_raw(raw);
        assert_eq!(n, restored);
        assert_eq!(restored.as_i32(), Some(-7));
    }

    #[test]
    fn type_queries_are_mutually_exclusive() {
        let i = SharedNaNValue::from_i32(42);
        assert!(i.is_i32());
        assert!(!i.is_u32());
        assert!(!i.is_bool());
        assert!(!i.is_f64());
        assert!(!i.is_nil());
        assert!(!i.is_offset_ptr());
    }

    #[test]
    fn wrong_type_extractor_returns_none() {
        let i = SharedNaNValue::from_i32(42);
        assert_eq!(i.as_f64(), None);
        assert_eq!(i.as_u32(), None);
        assert_eq!(i.as_bool(), None);
        assert_eq!(i.as_offset_ptr::<u64>(), None);
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let a = SharedNaNValue::from_i32(42);
        let b = SharedNaNValue::from_i32(42);
        let c = SharedNaNValue::from_i32(43);
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut s = HashSet::new();
        s.insert(a);
        assert!(s.contains(&b));
        assert!(!s.contains(&c));
    }

    #[test]
    fn cross_process_via_shared_hash_map() {
        // Demonstrate: store heterogeneous V in SharedHashMap<K, NaNValue>.
        use crate::SharedHashMap;
        let mut p = std::env::temp_dir();
        p.push(format!("subetha-nan-shm-{}.bin", std::process::id()));
        let m: SharedHashMap<u32, SharedNaNValue>
            = SharedHashMap::create(&p, 16).unwrap();
        m.insert(0, SharedNaNValue::from_i32(42)).unwrap();
        m.insert(1, SharedNaNValue::from_f64(2.5)).unwrap();
        m.insert(2, SharedNaNValue::from_bool(true)).unwrap();
        m.insert(3, SharedNaNValue::NIL).unwrap();
        m.insert(4, SharedNaNValue::from_offset_ptr::<u64>(OffsetPtr::new(99))).unwrap();
        assert_eq!(m.get(&0).unwrap().as_i32(), Some(42));
        assert_eq!(m.get(&1).unwrap().as_f64(), Some(2.5));
        assert_eq!(m.get(&2).unwrap().as_bool(), Some(true));
        assert!(m.get(&3).unwrap().is_nil());
        let ptr: OffsetPtr<u64> = m.get(&4).unwrap().as_offset_ptr().unwrap();
        assert_eq!(ptr.index, 99);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn size_is_8_bytes() {
        assert_eq!(std::mem::size_of::<SharedNaNValue>(), 8);
    }

    #[test]
    fn reserved_tags_decode_as_reserved() {
        let raw = pack(6, 0);  // tag 6 is reserved
        let n = SharedNaNValue::from_raw(raw);
        assert_eq!(n.type_tag(), NaNValueType::Reserved(6));
    }
}
