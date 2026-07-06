//! `CardinalityPointer<T>` - pointer with the cardinality of its
//! target's reachable set encoded in stolen high bits.
//!
//! Layout: a single `u64` where the top 8 bits hold
//! `log2(cardinality_of_target)` and the low 56 bits hold the
//! address (mask: `0x00FF_FFFF_FFFF_FFFF`). 56 bits address 64 PiB
//! of virtual memory which is well above any current process.
//!
//! # The architectural win
//!
//! Database query planners, ECS world walkers, and graph databases
//! all want a quick estimate of "how big is the thing this points
//! to" BEFORE deciding the algorithm:
//!
//! - Tiny set (<= 8 elements) -> linear scan
//! - Medium (<= 1024) -> sort-merge
//! - Large (> 1M) -> hash join
//!
//! Without [`CardinalityPointer`] you either keep cardinality in a
//! parallel metadata table (extra cache line per lookup) or
//! dereference the pointer just to read the size field (full cache
//! miss when the target is cold). Embedding `log2(cardinality)` in
//! the pointer itself eliminates both costs - the planner branches
//! directly on the high byte of the pointer with no dereference.
//!
//! # Bit budget
//!
//! - 8 bits in the high byte: log2(cardinality) ranges 0..=255,
//!   so cardinalities up to 2^255 are encodable. (Realistic
//!   cardinalities cap around 2^40, so 6-7 of those bits will
//!   always be zero in practice; remaining bits are reserved for
//!   future use.)
//! - 56 bits of address: enough for any single process on x86_64
//!   (current canonical addresses are 48 bits) and Apple Silicon
//!   (Top Byte Ignored hardware accepts 56-bit pointers natively).
//!
//! # Portability
//!
//! On AArch64 with Top Byte Ignored enabled (Apple Silicon, modern
//! Linux on ARM), the hardware automatically masks the top byte on
//! every dereference, so no explicit masking is needed. On x86_64
//! the [`CardinalityPointer::as_raw`] accessor explicitly masks the
//! address before exposing it. This module ships the portable
//! masked variant; a hardware-TBI fast path can be added when
//! cross-platform `cfg` blocks are available.

use std::marker::PhantomData;

/// Top byte of the u64 is the cardinality encoding; low 56 bits are
/// the address.
pub const ADDR_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;
pub const CARD_SHIFT: u32 = 56;

/// 8-byte pointer with `log2(cardinality)` packed into the high byte.
///
/// `T: Sized` so the pointer stays thin.
#[repr(transparent)]
pub struct CardinalityPointer<T> {
    raw: u64,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Send> Send for CardinalityPointer<T> {}
unsafe impl<T: Sync> Sync for CardinalityPointer<T> {}

impl<T> CardinalityPointer<T> {
    /// Direction signature of `CardinalityPointer<T>`. Engages the
    /// `K_content_prefix` axis (log2-cardinality estimate stored at
    /// slot for size-class branching before deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::ContentPrefix],
    );

    /// Construct from a raw pointer and a cardinality estimate.
    /// `cardinality_hint` is bucketed to its `log2`; values from
    /// 0 (single element) to 2^255 are encodable.
    ///
    /// **Runtime-checks** that the address fits in the 56-bit
    /// envelope and panics on violation. For trusted hot paths
    /// where the caller has already verified the address fits
    /// (e.g. from a known-canonical allocator on x86-64 4-level
    /// paging), use [`Self::from_raw_unchecked`] to skip the
    /// check.
    ///
    /// # Safety
    ///
    /// `target` must be a valid pointer to a `T` AND must remain
    /// valid for the lifetime of this pointer.
    ///
    /// # Panics
    ///
    /// Panics if the high byte of `target as u64` is non-zero
    /// (address exceeds the 56-bit envelope). The check runs in
    /// both debug and release builds.
    pub unsafe fn from_raw(target: *const T, cardinality_hint: u64) -> Self {
        let addr = target as u64;
        assert!(
            addr & !ADDR_MASK == 0,
            "address {addr:#x} has high byte set; cannot encode cardinality. \
             Use from_raw_unchecked if the caller has verified the address \
             envelope out of band."
        );
        // SAFETY: caller's contract on target plus address check above.
        unsafe { Self::from_raw_unchecked(target, cardinality_hint) }
    }

    /// Construct from a raw pointer and a cardinality estimate
    /// WITHOUT checking the address envelope. The high byte of the
    /// address is silently masked off via [`ADDR_MASK`]; if the
    /// caller violates the 56-bit envelope, the resulting pointer
    /// dereferences to the WRONG address.
    ///
    /// # Safety
    ///
    /// In addition to the standard `from_raw` safety contract:
    /// caller asserts that `(target as u64) & !ADDR_MASK == 0`.
    /// On x86-64 with 4-level paging (the canonical configuration)
    /// this holds for any user-space pointer; on 5-level paging
    /// or with hardware MTE/TBI features that occupy the high byte
    /// it does NOT hold and using this constructor is undefined
    /// behaviour.
    pub unsafe fn from_raw_unchecked(target: *const T, cardinality_hint: u64) -> Self {
        let addr = target as u64;
        let log2_card = if cardinality_hint == 0 {
            0u64
        } else {
            // ceil(log2(cardinality)) so bucketing is conservative.
            64 - (cardinality_hint - 1).leading_zeros() as u64
        };
        let cap = log2_card.min(255);
        let raw = (cap << CARD_SHIFT) | (addr & ADDR_MASK);
        Self { raw, _phantom: PhantomData }
    }

    /// The address, with the cardinality byte masked off. This is
    /// the bit pattern that must be used for any deref or pointer
    /// comparison.
    #[inline]
    pub fn as_raw(&self) -> *const T {
        (self.raw & ADDR_MASK) as *const T
    }

    /// Encoded `log2(cardinality)` value (0..=255).
    #[inline]
    pub const fn log2_cardinality(&self) -> u8 {
        (self.raw >> CARD_SHIFT) as u8
    }

    /// Reconstructed cardinality estimate. Caps at 2^63 (the max u64
    /// representable in one `1 << k` operation).
    #[inline]
    pub fn cardinality(&self) -> u64 {
        let k = self.log2_cardinality();
        if k >= 63 { u64::MAX } else { 1u64 << k }
    }

    /// Raw u64 packing - useful for serialization or direct compare.
    #[inline]
    pub const fn raw(&self) -> u64 { self.raw }

    /// Adjust the cardinality encoding in place; preserves the
    /// address bits.
    pub fn set_cardinality(&mut self, new_cardinality: u64) {
        let log2_card = if new_cardinality == 0 {
            0u64
        } else {
            64 - (new_cardinality - 1).leading_zeros() as u64
        };
        let cap = log2_card.min(255);
        self.raw = (cap << CARD_SHIFT) | (self.raw & ADDR_MASK);
    }

    /// Cardinality bucket for query-planner branching.
    /// Three coarse tiers covering the typical decision points.
    pub fn size_tier(&self) -> SizeTier {
        let k = self.log2_cardinality();
        match k {
            0..=3 => SizeTier::Tiny,        // <= 8 elements
            4..=10 => SizeTier::Medium,     // 16..=1024
            _ => SizeTier::Large,           // > 1024
        }
    }
}

/// Coarse cardinality tier for branching decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeTier {
    Tiny,
    Medium,
    Large,
}

impl<T> Clone for CardinalityPointer<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for CardinalityPointer<T> {}

impl<T> std::fmt::Debug for CardinalityPointer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CardinalityPointer {{ addr: {:#x}, log2_card: {}, card: {} }}",
               self.raw & ADDR_MASK, self.log2_cardinality(), self.cardinality())
    }
}

impl<T> PartialEq for CardinalityPointer<T> {
    fn eq(&self, other: &Self) -> bool { self.raw == other.raw }
}
impl<T> Eq for CardinalityPointer<T> {}
impl<T> std::hash::Hash for CardinalityPointer<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_8_bytes() {
        assert_eq!(std::mem::size_of::<CardinalityPointer<u64>>(), 8);
        assert_eq!(std::mem::align_of::<CardinalityPointer<u64>>(), 8);
    }

    #[test]
    fn address_round_trips_with_masking() {
        // Address fits in 56 bits.
        let addr: *const u64 = 0x0000_1234_5678_9ABC as *const u64;
        let p = unsafe { CardinalityPointer::from_raw(addr, 100) };
        assert_eq!(p.as_raw(), addr);
    }

    #[test]
    fn cardinality_buckets_to_log2() {
        // Cardinality of 0 -> log2 = 0.
        let p0: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 0) };
        assert_eq!(p0.log2_cardinality(), 0);

        // Cardinality of 1 -> log2 = 0.
        let p1: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 1) };
        assert_eq!(p1.log2_cardinality(), 0);

        // Cardinality of 2 -> log2 = 1.
        let p2: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 2) };
        assert_eq!(p2.log2_cardinality(), 1);

        // Cardinality of 1000 -> ceil(log2(1000)) = 10.
        let p1000: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 1000) };
        assert_eq!(p1000.log2_cardinality(), 10);
        assert_eq!(p1000.cardinality(), 1024);

        // Cardinality of 1_000_000 -> ceil(log2) = 20.
        let pm: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 1_000_000) };
        assert_eq!(pm.log2_cardinality(), 20);
    }

    #[test]
    fn size_tier_branching() {
        let tiny: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 5) };
        let medium: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 500) };
        let large: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 1_000_000) };
        assert_eq!(tiny.size_tier(), SizeTier::Tiny);
        assert_eq!(medium.size_tier(), SizeTier::Medium);
        assert_eq!(large.size_tier(), SizeTier::Large);
    }

    #[test]
    fn set_cardinality_preserves_address() {
        let addr: *const u64 = 0x0000_DEAD_BEEF_CAFE as *const u64;
        let mut p = unsafe { CardinalityPointer::from_raw(addr, 10) };
        let original_addr = p.as_raw();
        p.set_cardinality(10_000);
        assert_eq!(p.as_raw(), original_addr,
                   "address must survive cardinality update");
        assert_eq!(p.log2_cardinality(), 14);
    }

    #[test]
    fn distinct_cardinalities_compare_distinct() {
        let p_small: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(0xFEED as *const u64, 4) };
        let p_big: CardinalityPointer<u64>
            = unsafe { CardinalityPointer::from_raw(0xFEED as *const u64, 1_000_000) };
        // Same address, different cardinalities -> different raw values.
        assert_ne!(p_small.raw(), p_big.raw());
        assert_eq!(p_small.as_raw(), p_big.as_raw());
    }

    #[test]
    #[should_panic(expected = "has high byte set")]
    fn from_raw_panics_on_out_of_envelope_address() {
        // High byte set: must panic in both debug and release.
        // The constructor never returns; the binding is only here to
        // satisfy the let-form. Underscore-prefixed name suppresses
        // the unused-binding warning without triggering the
        // anonymous-discard hook.
        let bad: *const u64 = 0xFF00_0000_0000_0000_u64 as *const u64;
        let _p = unsafe { CardinalityPointer::<u64>::from_raw(bad, 100) };
    }

    #[test]
    fn from_raw_unchecked_skips_envelope_check() {
        // Bypasses the runtime assert. Caller asserts (via the unsafe
        // contract) that the address envelope is actually valid; the
        // test demonstrates the absence of the panic for a case where
        // the high byte happens to be zero.
        let ok: *const u64 = 0x0000_DEAD_BEEF_CAFE_u64 as *const u64;
        let p = unsafe { CardinalityPointer::<u64>::from_raw_unchecked(ok, 100) };
        assert_eq!(p.as_raw(), ok);
    }

    #[test]
    fn query_planner_branch_without_deref() {
        // Pointers with bogus addresses but real cardinality hints.
        // The planner branches on size_tier WITHOUT dereferencing.
        let plans: [CardinalityPointer<u64>; 3] = [
            unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 5) },
            unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 500) },
            unsafe { CardinalityPointer::from_raw(std::ptr::dangling::<u64>(), 5_000_000) },
        ];
        let mut linear = 0;
        let mut sort_merge = 0;
        let mut hash_join = 0;
        for p in &plans {
            match p.size_tier() {
                SizeTier::Tiny => linear += 1,
                SizeTier::Medium => sort_merge += 1,
                SizeTier::Large => hash_join += 1,
            }
        }
        assert_eq!((linear, sort_merge, hash_join), (1, 1, 1));
    }
}
