//! `AxisSignature` - direction-signature catalog for the SubEtha
//! design cube, spanning both the concurrent-data-structure domain
//! (deque variants) and the exotic-pointer domain.
//!
//! Every variant in the MMF-deque family AND every exotic pointer
//! type has a constrained **direction signature**: the set of axis
//! values it engages at a non-default value. The dispatcher routes
//! per call by satisfying the workload's required signature against
//! the available variants' signatures.
//!
//! This module names the axes of the design cube and defines the
//! `AxisMask` bitmask type that lets variants declare their
//! engagement on each axis. The dispatcher tests
//! `provided.satisfies(required)` to pick a routable variant.
//!
//! ## The deque-domain axes (bits 0..=5)
//!
//! 1. **K_inner**: items per slot. 1 or 3.
//! 2. **K_outer**: slots per producer-counter atomic. 1 or K.
//! 3. **K_consumer**: mailboxes per thief. shared or N per-thief.
//! 4. **K_counter_share**: producer counter ownership. shared or owner-private.
//! 5. **K_radius**: coherence distance of publish. Local or Distant (CPUID-dispatched).
//! 6. **K_gating**: synchronisation granularity. counter-only or per-slot.
//!
//! ## The pointer-domain axes (bits 6..=12)
//!
//! 7. **K_stride**: stride encoded in shift count (kstep-style).
//! 8. **K_segmented**: multi-segment address space (k-tower-style).
//! 9. **K_content_prefix**: content-summary stored at slot (umbra,
//!    bloom, cardinality).
//! 10. **K_type_tag**: type discriminant stored at slot (self-desc).
//! 11. **K_version**: version metadata stored at slot.
//! 12. **K_async**: future / async-state stored at slot.
//! 13. **K_bounds**: runtime bounds metadata at slot (cheri-style).

#![allow(clippy::missing_errors_doc)]

use core::fmt;

/// The axes of the SubEtha design cube. Bits 0..=5 are the
/// concurrent-data-structure (deque) axes; bits 6..=12 are the
/// exotic-pointer axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Axis {
    /// Items per slot (K_inner). Deque-domain.
    Inner,
    /// Slots per producer-counter atomic (K_outer). Deque-domain.
    Outer,
    /// Mailboxes per thief (K_consumer). Deque-domain.
    Consumer,
    /// Producer counter ownership (K_counter_share). Deque-domain.
    CounterShare,
    /// Coherence distance of publish (K_radius). Deque-domain.
    Radius,
    /// Per-slot atomic vs counter-only (K_gating). Deque-domain.
    Gating,
    /// Stride encoded in shift count (K_stride). Pointer-domain.
    Stride,
    /// Multi-segment address space (K_segmented). Pointer-domain.
    Segmented,
    /// Content-summary stored at slot (K_content_prefix).
    /// Pointer-domain. Engaged by umbra / bloom / cardinality.
    ContentPrefix,
    /// Type discriminant stored at slot (K_type_tag). Pointer-domain.
    TypeTag,
    /// Version metadata stored at slot (K_version). Pointer-domain.
    Version,
    /// Future / async-state stored at slot (K_async). Pointer-domain.
    Async,
    /// Runtime bounds metadata stored at slot (K_bounds).
    /// Pointer-domain.
    Bounds,
}

impl Axis {
    /// All axes in canonical order.
    pub const ALL: [Axis; 13] = [
        Axis::Inner,
        Axis::Outer,
        Axis::Consumer,
        Axis::CounterShare,
        Axis::Radius,
        Axis::Gating,
        Axis::Stride,
        Axis::Segmented,
        Axis::ContentPrefix,
        Axis::TypeTag,
        Axis::Version,
        Axis::Async,
        Axis::Bounds,
    ];

    /// Bit position of this axis in the packed `AxisMask` `u16`
    /// representation. Bits 0..=5 are deque-domain; bits 6..=12 are
    /// pointer-domain.
    #[inline(always)]
    pub const fn bit(self) -> u16 {
        match self {
            Axis::Inner => 0,
            Axis::Outer => 1,
            Axis::Consumer => 2,
            Axis::CounterShare => 3,
            Axis::Radius => 4,
            Axis::Gating => 5,
            Axis::Stride => 6,
            Axis::Segmented => 7,
            Axis::ContentPrefix => 8,
            Axis::TypeTag => 9,
            Axis::Version => 10,
            Axis::Async => 11,
            Axis::Bounds => 12,
        }
    }
}

impl fmt::Display for Axis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Axis::Inner => "K_inner",
            Axis::Outer => "K_outer",
            Axis::Consumer => "K_consumer",
            Axis::CounterShare => "K_counter_share",
            Axis::Radius => "K_radius",
            Axis::Gating => "K_gating",
            Axis::Stride => "K_stride",
            Axis::Segmented => "K_segmented",
            Axis::ContentPrefix => "K_content_prefix",
            Axis::TypeTag => "K_type_tag",
            Axis::Version => "K_version",
            Axis::Async => "K_async",
            Axis::Bounds => "K_bounds",
        };
        f.write_str(name)
    }
}

/// A bitmask over the design-cube axes. Bit `i` set means the
/// corresponding axis is engaged at its non-default value.
///
/// The direction signature for a variant is its `AxisMask`: which
/// axes it sets to non-default values. A workload's required
/// signature is also an `AxisMask`: which axes it needs the
/// transport (or pointer type) to handle non-trivially.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AxisMask(u16);

impl AxisMask {
    /// The mask covering every defined axis. Bits 0..=12 are valid.
    const VALID_BITS: u16 = (1u16 << 13) - 1;

    /// The empty signature (no axes engaged). Corresponds to the
    /// origin corner of the cube (Chase-Lev's signature).
    pub const EMPTY: AxisMask = AxisMask(0);

    /// All defined axes engaged.
    pub const ALL: AxisMask = AxisMask(Self::VALID_BITS);

    /// Build a signature from a slice of engaged axes.
    pub const fn from_axes(axes: &[Axis]) -> Self {
        let mut bits = 0u16;
        let mut i = 0;
        while i < axes.len() {
            bits |= 1u16 << axes[i].bit();
            i += 1;
        }
        AxisMask(bits)
    }

    /// Build from a raw `u16`. Bits outside the valid range are
    /// masked off.
    pub const fn from_bits(bits: u16) -> Self {
        AxisMask(bits & Self::VALID_BITS)
    }

    /// Return the raw `u16` representation.
    #[inline(always)]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// `true` if axis `a` is engaged in this signature.
    #[inline(always)]
    pub const fn contains(self, a: Axis) -> bool {
        (self.0 >> a.bit()) & 1 == 1
    }

    /// Count of engaged axes (popcount of the bitmask).
    #[inline(always)]
    pub const fn count(self) -> u32 {
        self.0.count_ones()
    }

    /// Union of two signatures.
    #[inline(always)]
    pub const fn union(self, other: AxisMask) -> AxisMask {
        AxisMask(self.0 | other.0)
    }

    /// Intersection of two signatures.
    #[inline(always)]
    pub const fn intersection(self, other: AxisMask) -> AxisMask {
        AxisMask(self.0 & other.0)
    }

    /// `true` if this signature is a superset of `other` (i.e. every
    /// axis required by `other` is engaged in `self`).
    ///
    /// `provided.satisfies(required)` means the variant `provided`
    /// can transport the workload `required`.
    #[inline(always)]
    pub const fn satisfies(self, other: AxisMask) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Hamming distance between two signatures: how many axes
    /// differ. This is the cube-distance the dispatcher pays when
    /// routing a workload to a non-perfect-match variant.
    #[inline(always)]
    pub const fn distance(self, other: AxisMask) -> u32 {
        (self.0 ^ other.0).count_ones()
    }
}

impl fmt::Display for AxisMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        f.write_str("{")?;
        for a in Axis::ALL {
            if self.contains(a) {
                if !first {
                    f.write_str(", ")?;
                }
                fmt::Display::fmt(&a, f)?;
                first = false;
            }
        }
        f.write_str("}")
    }
}

/// Cross-axis fusions a variant implements. Each fusion is an
/// `AxisMask` of two or three axes whose state is packed into one
/// atomic word (for ≤8 B fusion) or one 64-byte cache-line publish
/// (for MOVDIR64B-based corner fusion).
///
/// Terminology: a 2-axis fusion packs two axes' state into one
/// atomic word; a 3-axis fusion publishes three axes' state in one
/// instruction; a 4-axis fusion is the maximum on a single cache
/// line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fusion {
    /// The axes packed into the fused atomic word.
    pub axes: AxisMask,
}

impl Fusion {
    /// Build a 2-axis fusion.
    pub const fn pair(a: Axis, b: Axis) -> Self {
        Self {
            axes: AxisMask::from_axes(&[a, b]),
        }
    }

    /// Build a 3-axis fusion.
    pub const fn triple(a: Axis, b: Axis, c: Axis) -> Self {
        Self {
            axes: AxisMask::from_axes(&[a, b, c]),
        }
    }

    /// Number of axes engaged in this fusion (2 for pairs, 3 for
    /// triples, etc.).
    #[inline(always)]
    pub const fn axis_count(self) -> u32 {
        self.axes.count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_signature_contains_nothing() {
        assert!(!AxisMask::EMPTY.contains(Axis::Inner));
        assert_eq!(AxisMask::EMPTY.count(), 0);
    }

    #[test]
    fn all_signature_contains_everything() {
        for a in Axis::ALL {
            assert!(AxisMask::ALL.contains(a));
        }
        assert_eq!(AxisMask::ALL.count(), Axis::ALL.len() as u32);
    }

    #[test]
    fn from_axes_packs_bits_correctly() {
        let s = AxisMask::from_axes(&[Axis::Inner, Axis::Gating]);
        assert!(s.contains(Axis::Inner));
        assert!(s.contains(Axis::Gating));
        assert!(!s.contains(Axis::Outer));
        assert_eq!(s.count(), 2);
    }

    #[test]
    fn satisfies_is_superset_check() {
        let chase_lev = AxisMask::EMPTY;
        let khl = AxisMask::from_axes(&[
            Axis::Inner,
            Axis::Outer,
            Axis::CounterShare,
            Axis::Radius,
            Axis::Gating,
        ]);
        let request_reply = AxisMask::EMPTY;
        let producer_fast = AxisMask::from_axes(&[Axis::Inner, Axis::Outer]);

        assert!(chase_lev.satisfies(request_reply));
        assert!(khl.satisfies(request_reply));
        assert!(khl.satisfies(producer_fast));
        assert!(!chase_lev.satisfies(producer_fast));
    }

    #[test]
    fn distance_counts_differing_axes() {
        let a = AxisMask::from_axes(&[Axis::Inner, Axis::Outer]);
        let b = AxisMask::from_axes(&[Axis::Inner, Axis::Gating]);
        // Outer and Gating differ; Inner agrees.
        assert_eq!(a.distance(b), 2);
        assert_eq!(a.distance(a), 0);
        assert_eq!(
            AxisMask::EMPTY.distance(AxisMask::ALL),
            Axis::ALL.len() as u32
        );
    }

    #[test]
    fn pair_fusion_has_axis_count_two() {
        let f = Fusion::pair(Axis::Inner, Axis::Gating);
        assert_eq!(f.axis_count(), 2);
    }

    #[test]
    fn triple_fusion_has_axis_count_three() {
        let f = Fusion::triple(Axis::Radius, Axis::Inner, Axis::Gating);
        assert_eq!(f.axis_count(), 3);
    }

    #[test]
    fn display_renders_axis_names() {
        let s = AxisMask::from_axes(&[Axis::Inner, Axis::Radius]);
        let rendered = format!("{s}");
        assert!(rendered.contains("K_inner"));
        assert!(rendered.contains("K_radius"));
    }
}
