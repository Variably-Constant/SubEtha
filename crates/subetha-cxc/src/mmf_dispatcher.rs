//! `MmfDispatcher` - per-call routing across the MMF primitive
//! families.
//!
//! Generalizes the [`DequeDispatcher`] pattern (WorkloadShape ->
//! deque-variant selection) to the cross-family case: a workload may
//! want a streaming MPMC ring, a work-stealing deque, or a key-value
//! map, and the dispatcher picks between them by signature
//! containment.
//!
//! ## The three MMF families
//!
//! | Family | Workload pattern | Signature axes engaged |
//! |---|---|---|
//! | [`SharedRing`](crate::SharedRing) | MPMC streaming queue, arrival order | (none - cube origin for streaming) |
//! | [`SharedDeque`](crate::SharedDeque) family (6 variants) | Single owner, multi-thief work-stealing | union of deque-domain axes |
//! | [`SharedHashMap`](crate::SharedHashMap) | MPMC key-value lookup | `K_content_prefix` (hash-keyed) |
//!
//! Family signatures are unions of their member variants' signatures.
//! [`DequeDispatcher`] handles the within-family pick once
//! `MmfDispatcher` has selected the deque family.
//!
//! ## Routing decision
//!
//! 1. Compute the workload's required signature.
//! 2. If `K_content_prefix` is required -> [`MmfFamily::SharedHashMap`].
//! 3. If any deque-domain axis is required -> [`MmfFamily::SharedDeque`]
//!    (delegate within-family to [`DequeDispatcher::pick`]).
//! 4. Otherwise -> [`MmfFamily::SharedRing`].
//!
//! The signature-based path
//! ([`MmfDispatcher::pick_by_signature`]) and the categorical path
//! ([`MmfDispatcher::pick`]) agree on every canonical workload shape;
//! they co-exist so downstream callers can pick the style that fits.

#![allow(clippy::missing_errors_doc)]

use subetha_core::{Axis, AxisMask};

use crate::dispatch_deque::{DequeDispatcher, DequeVariant, WorkloadShape};

/// The three MMF primitive families the dispatcher routes across.
/// `SharedDeque` carries the within-family variant so callers see the
/// full routing decision in one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MmfFamily {
    /// MPMC streaming queue. Maps to [`SharedRing`](crate::SharedRing).
    SharedRing,
    /// Single-owner, multi-thief work-stealing deque. The variant is
    /// the within-family pick from [`DequeDispatcher`].
    SharedDeque(DequeVariant),
    /// MPMC key-value lookup. Maps to
    /// [`SharedHashMap`](crate::SharedHashMap).
    SharedHashMap,
}

impl MmfFamily {
    /// Union signature of this family: which axes any variant in the
    /// family engages at a non-default value.
    pub const fn signature(self) -> AxisMask {
        match self {
            Self::SharedRing => AxisMask::EMPTY,
            Self::SharedDeque(v) => v.signature(),
            Self::SharedHashMap => AxisMask::from_axes(&[Axis::ContentPrefix]),
        }
    }
}

/// Caller-supplied workload shape spanning all three families. Each
/// arm carries the parameters the dispatcher needs to route within
/// its family.
#[derive(Debug, Clone, Copy)]
pub enum MmfWorkloadShape {
    /// MPMC streaming queue (arrival order). Caller specifies the
    /// expected producer + consumer counts; the dispatcher routes
    /// to [`MmfFamily::SharedRing`] in every case (the ring is the
    /// MPMC-streaming family's only member).
    StreamingMpmc {
        /// Number of producers expected to push concurrently.
        n_producers: usize,
        /// Number of consumers expected to drain concurrently.
        n_consumers: usize,
    },
    /// Work-stealing access (single owner, multi-thief). Carries the
    /// deque-family [`WorkloadShape`] for the within-family pick.
    WorkStealing(WorkloadShape),
    /// Key-value lookup. Caller specifies the expected reader /
    /// writer concurrency; the dispatcher routes to
    /// [`MmfFamily::SharedHashMap`] in every case.
    KeyValueLookup {
        /// Number of readers expected to look up concurrently.
        n_readers: usize,
        /// Number of writers expected to insert / update concurrently.
        n_writers: usize,
    },
}

impl MmfWorkloadShape {
    /// The direction signature this workload requires of its MMF
    /// transport: which axes the chosen family must engage.
    pub const fn required_signature(&self) -> AxisMask {
        match self {
            Self::StreamingMpmc { .. } => AxisMask::EMPTY,
            Self::WorkStealing(shape) => shape.required_signature(),
            Self::KeyValueLookup { .. } => {
                AxisMask::from_axes(&[Axis::ContentPrefix])
            }
        }
    }
}

/// MMF cross-family routing dispatcher. Stateless and zero-sized;
/// the picks are pure functions of the workload shape.
pub struct MmfDispatcher;

impl MmfDispatcher {
    /// Categorical pick: route by the workload kind tag.
    pub fn pick(workload: MmfWorkloadShape) -> MmfFamily {
        match workload {
            MmfWorkloadShape::StreamingMpmc { .. } => MmfFamily::SharedRing,
            MmfWorkloadShape::WorkStealing(shape) => {
                MmfFamily::SharedDeque(DequeDispatcher::pick(shape))
            }
            MmfWorkloadShape::KeyValueLookup { .. } => MmfFamily::SharedHashMap,
        }
    }

    /// Signature-set pick: family selection honors the workload's
    /// kind tag (the caller's declared intent); the within-family
    /// pick uses signature containment via
    /// [`DequeDispatcher::pick_by_signature`]. Agrees with
    /// [`pick`](Self::pick) at the family level on every canonical
    /// workload shape; differs only in which within-family routing
    /// path it delegates to.
    pub fn pick_by_signature(workload: MmfWorkloadShape) -> MmfFamily {
        match workload {
            MmfWorkloadShape::StreamingMpmc { .. } => MmfFamily::SharedRing,
            MmfWorkloadShape::WorkStealing(shape) => MmfFamily::SharedDeque(
                DequeDispatcher::pick_by_signature(shape),
            ),
            MmfWorkloadShape::KeyValueLookup { .. } => MmfFamily::SharedHashMap,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_routes_to_shared_ring() {
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 4,
            n_consumers: 4,
        };
        assert_eq!(MmfDispatcher::pick(shape), MmfFamily::SharedRing);
        assert_eq!(
            MmfDispatcher::pick_by_signature(shape),
            MmfFamily::SharedRing,
        );
    }

    #[test]
    fn work_stealing_routes_to_shared_deque_family() {
        let shape = MmfWorkloadShape::WorkStealing(WorkloadShape::producer_fast(64));
        assert_eq!(
            MmfDispatcher::pick(shape),
            MmfFamily::SharedDeque(DequeVariant::Khl),
        );
        assert_eq!(
            MmfDispatcher::pick_by_signature(shape),
            MmfFamily::SharedDeque(DequeVariant::Khl),
        );
    }

    #[test]
    fn multi_thief_work_stealing_routes_to_urd() {
        let shape = MmfWorkloadShape::WorkStealing(WorkloadShape::fan_out(4, 64));
        assert_eq!(
            MmfDispatcher::pick(shape),
            MmfFamily::SharedDeque(DequeVariant::Urd),
        );
        assert_eq!(
            MmfDispatcher::pick_by_signature(shape),
            MmfFamily::SharedDeque(DequeVariant::Urd),
        );
    }

    #[test]
    fn key_value_routes_to_shared_hash_map() {
        let shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: 4,
            n_writers: 2,
        };
        assert_eq!(MmfDispatcher::pick(shape), MmfFamily::SharedHashMap);
        assert_eq!(
            MmfDispatcher::pick_by_signature(shape),
            MmfFamily::SharedHashMap,
        );
    }

    #[test]
    fn signature_pick_agrees_with_categorical_pick_across_shapes() {
        let shapes = [
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 1,
                n_consumers: 1,
            },
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 4,
                n_consumers: 4,
            },
            MmfWorkloadShape::WorkStealing(WorkloadShape::request_reply()),
            MmfWorkloadShape::WorkStealing(WorkloadShape::producer_fast(4)),
            MmfWorkloadShape::WorkStealing(WorkloadShape::producer_fast(64)),
            MmfWorkloadShape::WorkStealing(WorkloadShape::fan_out(2, 16)),
            MmfWorkloadShape::WorkStealing(WorkloadShape::fan_out(4, 64)),
            MmfWorkloadShape::KeyValueLookup {
                n_readers: 1,
                n_writers: 1,
            },
            MmfWorkloadShape::KeyValueLookup {
                n_readers: 8,
                n_writers: 4,
            },
        ];
        for shape in shapes {
            let categorical = MmfDispatcher::pick(shape);
            let signature = MmfDispatcher::pick_by_signature(shape);
            assert_eq!(
                categorical, signature,
                "shape {shape:?}: categorical {categorical:?}, signature {signature:?}",
            );
        }
    }

    #[test]
    fn family_signatures_distinguish_families() {
        assert_ne!(
            MmfFamily::SharedRing.signature(),
            MmfFamily::SharedHashMap.signature(),
        );
        assert_ne!(
            MmfFamily::SharedRing.signature(),
            MmfFamily::SharedDeque(DequeVariant::Khl).signature(),
        );
        assert_ne!(
            MmfFamily::SharedDeque(DequeVariant::Khl).signature(),
            MmfFamily::SharedHashMap.signature(),
        );
    }

    #[test]
    fn shared_hash_map_engages_content_prefix() {
        assert!(MmfFamily::SharedHashMap.signature().contains(Axis::ContentPrefix));
    }

    #[test]
    fn shared_ring_signature_is_empty() {
        assert_eq!(MmfFamily::SharedRing.signature(), AxisMask::EMPTY);
    }

    #[test]
    fn key_value_workload_requires_content_prefix() {
        let shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: 1,
            n_writers: 1,
        };
        assert!(shape.required_signature().contains(Axis::ContentPrefix));
    }

    #[test]
    fn streaming_workload_has_empty_required_signature() {
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 4,
            n_consumers: 4,
        };
        assert_eq!(shape.required_signature(), AxisMask::EMPTY);
    }
}
