//! `RawUmbraPointer` - the content-prefix pointer at an element size
//! chosen at run time.
//!
//! [`SharedUmbraPointer`](crate::shared_umbra_pointer::SharedUmbraPointer)
//! is generic over what it points at, which a caller binding through C
//! cannot name. The pointer itself never needed that type: it is a slot
//! index, a four-byte prefix of the value, and seven bytes of caller tag.
//! Only resolving it touches the value, and that takes the element size as
//! an argument here.
//!
//! # Why the prefix is the point
//!
//! Comparing prefixes answers "these definitely differ" without reading
//! the region at all. A prefix match is not equality - it is four bytes of
//! agreement - so a match means resolve and compare, while a mismatch
//! means skip without a dereference. That is the whole saving, and it is
//! why [`prefix_eq`](RawUmbraPointer::prefix_eq) is deliberately not
//! called an equality test.
//!
//! The prefix is the first four bytes of the value as stored, so two
//! callers agree on it only if they agree on the value's byte layout.
//! Anything with padding, a pointer, or a different endianness is a value
//! whose prefix means nothing across that boundary.

use crate::raw_region::RawRegion;
use crate::shared_region::RegionError;

/// The index that means the pointer aims at nothing.
pub const RAW_UMBRA_NIL: u32 = u32::MAX;

/// Bytes a caller may carry alongside the pointer, after the tag byte.
pub const RAW_UMBRA_EXT_BYTES: usize = 7;

/// The tag meaning no caller extension is present.
pub const RAW_UMBRA_EXT_NONE: u8 = 0;

/// Why an operation on the pointer could not be carried out.
#[derive(Debug)]
pub enum RawUmbraError {
    /// A value slice was not the size the region was built for.
    WrongSize { expected: usize, found: usize },
    /// The extension payload was longer than [`RAW_UMBRA_EXT_BYTES`].
    ExtTooLong { found: usize },
    /// The pointer aims at nothing.
    Nil,
    Region(RegionError),
}

impl From<RegionError> for RawUmbraError {
    fn from(e: RegionError) -> Self {
        Self::Region(e)
    }
}

/// A content-prefix pointer: where the value is, four bytes of what it
/// starts with, and room for a caller tag.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawUmbraPointer {
    target: u32,
    prefix: u32,
    ext_tag: u8,
    ext_payload: [u8; RAW_UMBRA_EXT_BYTES],
}

/// The first four bytes of `value`, as they are stored.
///
/// A value shorter than four bytes is padded with zeros, so a two-byte
/// value and the same two bytes followed by two zeros share a prefix.
/// That costs a dereference on a collision and never a wrong answer.
pub fn raw_content_prefix(value: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = value.len().min(4);
    buf[..n].copy_from_slice(&value[..n]);
    u32::from_le_bytes(buf)
}

impl RawUmbraPointer {
    /// A pointer at nothing.
    pub const NIL: Self = Self {
        target: RAW_UMBRA_NIL,
        prefix: 0,
        ext_tag: RAW_UMBRA_EXT_NONE,
        ext_payload: [0; RAW_UMBRA_EXT_BYTES],
    };

    /// A pointer at `target` carrying `prefix`, with no extension.
    pub fn new(target: u32, prefix: u32) -> Self {
        Self { target, prefix, ext_tag: RAW_UMBRA_EXT_NONE, ext_payload: [0; RAW_UMBRA_EXT_BYTES] }
    }

    /// Store `value` in `region` and point at it, taking the prefix from
    /// the value's own first four bytes.
    pub fn allocate(region: &RawRegion, value: &[u8]) -> Result<Self, RawUmbraError> {
        let expected = region.layout().slot_size;
        if value.len() != expected {
            return Err(RawUmbraError::WrongSize { expected, found: value.len() });
        }
        let prefix = raw_content_prefix(value);
        let index = region.allocate(value)?;
        Ok(Self::new(index, prefix))
    }

    /// Store `value` in `region` and point at it, with a prefix the caller
    /// chooses rather than one taken from the bytes.
    ///
    /// For a value whose first four bytes do not discriminate - a record
    /// that opens with a common header, say - a caller-chosen prefix is
    /// what makes the filter worth having.
    pub fn allocate_with_prefix(
        region: &RawRegion,
        value: &[u8],
        prefix: u32,
    ) -> Result<Self, RawUmbraError> {
        let expected = region.layout().slot_size;
        if value.len() != expected {
            return Err(RawUmbraError::WrongSize { expected, found: value.len() });
        }
        let index = region.allocate(value)?;
        Ok(Self::new(index, prefix))
    }

    pub fn target(self) -> u32 {
        self.target
    }

    pub fn prefix(self) -> u32 {
        self.prefix
    }

    pub fn is_nil(self) -> bool {
        self.target == RAW_UMBRA_NIL
    }

    /// Whether two pointers could aim at equal values. A false answer is
    /// certain; a true answer means resolve both and compare.
    pub fn prefix_eq(self, other: Self) -> bool {
        self.prefix == other.prefix
    }

    /// Whether this pointer could aim at a value starting with `query`.
    pub fn matches_prefix(self, query: u32) -> bool {
        self.prefix == query
    }

    /// Attach a caller tag and up to [`RAW_UMBRA_EXT_BYTES`] of payload.
    ///
    /// A tag of [`RAW_UMBRA_EXT_NONE`] is accepted and means the payload
    /// is not to be read; the bytes are still stored, since refusing them
    /// would make the tag and the payload disagree about what happened.
    pub fn set_ext(&mut self, tag: u8, payload: &[u8]) -> Result<(), RawUmbraError> {
        if payload.len() > RAW_UMBRA_EXT_BYTES {
            return Err(RawUmbraError::ExtTooLong { found: payload.len() });
        }
        self.ext_tag = tag;
        self.ext_payload = [0; RAW_UMBRA_EXT_BYTES];
        self.ext_payload[..payload.len()].copy_from_slice(payload);
        Ok(())
    }

    /// Drop any caller tag and zero its payload.
    pub fn clear_ext(&mut self) {
        self.ext_tag = RAW_UMBRA_EXT_NONE;
        self.ext_payload = [0; RAW_UMBRA_EXT_BYTES];
    }

    pub fn ext_tag(self) -> u8 {
        self.ext_tag
    }

    /// The seven payload bytes, whatever the tag says. A caller that has
    /// not checked the tag is reading bytes whose meaning nobody agreed.
    pub fn ext_payload(&self) -> &[u8; RAW_UMBRA_EXT_BYTES] {
        &self.ext_payload
    }

    /// Read the value this points at into `out`.
    pub fn resolve(self, region: &RawRegion, out: &mut [u8]) -> Result<(), RawUmbraError> {
        if self.is_nil() {
            return Err(RawUmbraError::Nil);
        }
        let expected = region.layout().slot_size;
        if out.len() != expected {
            return Err(RawUmbraError::WrongSize { expected, found: out.len() });
        }
        region.get(self.target, out)?;
        Ok(())
    }

    /// The sixteen bytes of this pointer, for a caller that stores it in a
    /// structure of its own.
    pub fn to_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..4].copy_from_slice(&self.target.to_le_bytes());
        out[4..8].copy_from_slice(&self.prefix.to_le_bytes());
        out[8] = self.ext_tag;
        out[9..].copy_from_slice(&self.ext_payload);
        out
    }

    /// The inverse of [`to_bytes`](Self::to_bytes).
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let mut ext_payload = [0u8; RAW_UMBRA_EXT_BYTES];
        ext_payload.copy_from_slice(&bytes[9..]);
        Self {
            target: u32::from_le_bytes(bytes[..4].try_into().expect("four bytes")),
            prefix: u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes")),
            ext_tag: bytes[8],
            ext_payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw_treiber_stack::ElementLayout;
    use std::path::{Path, PathBuf};

    fn layout(slot: usize) -> ElementLayout {
        ElementLayout { slot_size: slot, alignment: 8, tag: 0x554D_4252_4150_5452 }
    }

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha_raw_umbra_{name}_{}.bin", std::process::id()));
        p
    }

    fn cleanup(p: &Path) {
        match std::fs::remove_file(p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("could not clear {}: {e}", p.display()),
        }
    }

    #[test]
    fn a_pointer_resolves_to_what_was_stored() {
        let path = scratch("resolve");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");

        let value = 0x1122_3344_5566_7788u64.to_le_bytes();
        let p = RawUmbraPointer::allocate(&region, &value).expect("allocate");
        assert!(!p.is_nil());

        let mut out = [0u8; 8];
        p.resolve(&region, &mut out).expect("resolve");
        assert_eq!(out, value);

        cleanup(&path);
    }

    #[test]
    fn the_prefix_is_the_values_own_first_four_bytes() {
        let path = scratch("prefix");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");

        let value = 0x1122_3344_5566_7788u64.to_le_bytes();
        let p = RawUmbraPointer::allocate(&region, &value).expect("allocate");
        // Little-endian, so the first four stored bytes are the low word.
        assert_eq!(p.prefix(), 0x5566_7788);
        assert!(p.matches_prefix(0x5566_7788));
        assert!(!p.matches_prefix(0x1122_3344));

        cleanup(&path);
    }

    #[test]
    fn a_prefix_mismatch_rules_equality_out_without_a_dereference() {
        let path = scratch("filter");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");

        let a = RawUmbraPointer::allocate(&region, &1u64.to_le_bytes()).expect("a");
        let b = RawUmbraPointer::allocate(&region, &2u64.to_le_bytes()).expect("b");
        let c = RawUmbraPointer::allocate(&region, &1u64.to_le_bytes()).expect("c");

        assert!(!a.prefix_eq(b), "different values, so the filter rejects without reading");

        // Equal prefixes mean maybe equal. The contract is that a true
        // answer sends the caller to the region, not that it settles it.
        assert!(a.prefix_eq(c));
        let mut left = [0u8; 8];
        let mut right = [0u8; 8];
        a.resolve(&region, &mut left).expect("resolve a");
        c.resolve(&region, &mut right).expect("resolve c");
        assert_eq!(left, right, "and here the deref confirms it");

        cleanup(&path);
    }

    #[test]
    fn two_different_values_can_share_a_prefix_and_that_is_not_a_fault() {
        let path = scratch("collide");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");

        // Same low word, different high word: the prefix cannot tell them
        // apart, and the pointer does not claim to.
        let a = RawUmbraPointer::allocate(&region, &0x0000_0000_AAAA_AAAAu64.to_le_bytes())
            .expect("a");
        let b = RawUmbraPointer::allocate(&region, &0xFFFF_FFFF_AAAA_AAAAu64.to_le_bytes())
            .expect("b");
        assert!(a.prefix_eq(b), "prefixes agree");

        let mut left = [0u8; 8];
        let mut right = [0u8; 8];
        a.resolve(&region, &mut left).expect("a");
        b.resolve(&region, &mut right).expect("b");
        assert_ne!(left, right, "the values do not, which the deref shows");

        cleanup(&path);
    }

    #[test]
    fn a_caller_chosen_prefix_overrides_the_content_one() {
        let path = scratch("chosen");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");

        // Two values sharing a leading header, discriminated by a prefix
        // the caller picks instead.
        let one = 0xAAAA_0000_DEAD_BEEFu64.to_le_bytes();
        let two = 0xBBBB_0000_DEAD_BEEFu64.to_le_bytes();
        let a = RawUmbraPointer::allocate_with_prefix(&region, &one, 1).expect("a");
        let b = RawUmbraPointer::allocate_with_prefix(&region, &two, 2).expect("b");

        assert!(!a.prefix_eq(b), "the chosen prefixes discriminate");
        // Where the content prefix would not have.
        assert_eq!(raw_content_prefix(&one), raw_content_prefix(&two));

        cleanup(&path);
    }

    #[test]
    fn a_value_of_the_wrong_size_is_refused_rather_than_padded() {
        let path = scratch("sizes");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");
        assert!(matches!(
            RawUmbraPointer::allocate(&region, &[0u8; 4]),
            Err(RawUmbraError::WrongSize { expected: 8, found: 4 })
        ));
        let p = RawUmbraPointer::allocate(&region, &[0u8; 8]).expect("p");
        let mut small = [0u8; 4];
        assert!(matches!(
            p.resolve(&region, &mut small),
            Err(RawUmbraError::WrongSize { expected: 8, found: 4 })
        ));
        cleanup(&path);
    }

    #[test]
    fn resolving_a_nil_pointer_says_so_rather_than_reading_slot_zero() {
        let path = scratch("nil");
        cleanup(&path);
        let region = RawRegion::create(&path, 8, layout(8)).expect("region");
        // Something is in slot 0, so a nil that fell through to a read
        // would return it and look like a hit.
        RawUmbraPointer::allocate(&region, &7u64.to_le_bytes()).expect("occupy slot 0");

        let mut out = [0u8; 8];
        assert!(RawUmbraPointer::NIL.is_nil());
        assert!(matches!(
            RawUmbraPointer::NIL.resolve(&region, &mut out),
            Err(RawUmbraError::Nil)
        ));
        assert_eq!(out, [0u8; 8], "and it wrote nothing");

        cleanup(&path);
    }

    #[test]
    fn the_extension_carries_a_tag_and_seven_bytes_and_refuses_an_eighth() {
        let mut p = RawUmbraPointer::new(3, 0xABCD);
        assert_eq!(p.ext_tag(), RAW_UMBRA_EXT_NONE);

        p.set_ext(9, &[1, 2, 3]).expect("three bytes fit");
        assert_eq!(p.ext_tag(), 9);
        assert_eq!(&p.ext_payload()[..3], &[1, 2, 3]);
        assert_eq!(&p.ext_payload()[3..], &[0; 4], "the rest is zeroed, not left over");

        p.set_ext(9, &[4; RAW_UMBRA_EXT_BYTES]).expect("seven bytes fit");
        assert!(matches!(
            p.set_ext(9, &[5; 8]),
            Err(RawUmbraError::ExtTooLong { found: 8 })
        ));
        // The refused call changed nothing.
        assert_eq!(p.ext_payload(), &[4; RAW_UMBRA_EXT_BYTES]);

        p.clear_ext();
        assert_eq!(p.ext_tag(), RAW_UMBRA_EXT_NONE);
        assert_eq!(p.ext_payload(), &[0; RAW_UMBRA_EXT_BYTES]);
    }

    #[test]
    fn a_pointer_survives_a_round_trip_through_its_own_bytes() {
        let mut p = RawUmbraPointer::new(42, 0xDEAD_BEEF);
        p.set_ext(200, &[9, 8, 7, 6, 5, 4, 3]).expect("seven bytes");
        let back = RawUmbraPointer::from_bytes(p.to_bytes());
        assert_eq!(back, p);
        assert_eq!(back.target(), 42);
        assert_eq!(back.prefix(), 0xDEAD_BEEF);
        assert_eq!(back.ext_tag(), 200);
        assert_eq!(back.ext_payload(), &[9, 8, 7, 6, 5, 4, 3]);

        // And nil survives too, which is the value most likely to be
        // stored and read back by a caller walking a structure.
        assert!(RawUmbraPointer::from_bytes(RawUmbraPointer::NIL.to_bytes()).is_nil());
    }

    #[test]
    fn a_short_value_pads_its_prefix_with_zeros() {
        // Documented rather than incidental: a two-byte value and the same
        // two bytes followed by two zeros share a prefix, which costs a
        // dereference and never a wrong answer.
        assert_eq!(raw_content_prefix(&[0xAA, 0xBB]), raw_content_prefix(&[0xAA, 0xBB, 0, 0]));
        assert_eq!(raw_content_prefix(&[]), 0);
        // Beyond four bytes nothing more is read.
        assert_eq!(
            raw_content_prefix(&[1, 2, 3, 4, 5, 6]),
            raw_content_prefix(&[1, 2, 3, 4, 9, 9])
        );
    }
}
