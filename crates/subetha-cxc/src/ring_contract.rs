//! `RingContract` - the declared operation envelope for a ring.
//!
//! A [`RingContract`] is the USER OVERRIDE on the otherwise fully
//! automatic ring: peer-count ceilings, an ordering contract, and a
//! capacity bound, declared as one validated artifact every attaching
//! party agrees on. An `AdaptiveRing` WITHOUT a declared contract is
//! unbounded - peers grow the ring on demand and registration never
//! fails; declaring a contract is the only thing that makes
//! `TooManyProducers` / `TooManyConsumers` possible.
//!
//! (Lineage: path expressions - Campbell & Habermann, 1974 - declare
//! the legal operation histories of a shared type, with enforcement
//! derived rather than hand-written. This module is the
//! counter-compilable fragment of that idea, expressed entirely as
//! data the rings already track.)
//!
//! Two jobs:
//!
//! 1. **One validated pin.** The peer ceilings
//!    ([`from_counts`](RingContract::from_counts)) and the ordering /
//!    capacity constraints live in a single declared artifact instead
//!    of scattered flags.
//! 2. **Give the adaptive policy a feasible-region filter.** A policy
//!    proposes a candidate `(shape, capacity)`; [`permits_config`]
//!    rejects any move that would violate the declared envelope, so an
//!    aggressive auto-morph cannot break the contract by construction.
//!
//! Enforcement cost on the hot path is zero - the contract is consulted
//! only at attach time and at policy-tick time. The
//! ordering-contract-to-shape rule is where it earns its keep: the
//! sharded [`Mpmc`](RingShape::Mpmc) shape (per-producer lanes)
//! delivers only per-producer FIFO, so it is illegal under a `Fifo`
//! (global-total-order) contract - which is exactly the
//! `GlobalFifo -> Vyukov` rule the QoS shape policy also applies,
//! derived here from the declared contract.
//!
//! [`permits_config`]: RingContract::permits_config

use crate::adaptive_ring::RingShape;

/// The ordering envelope a ring's consumers may observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingContract {
    /// Global total order across all producers (the strongest).
    Fifo,
    /// Per-producer FIFO; items from different producers may interleave
    /// arbitrarily, but each producer's items arrive in push order.
    FifoPerProducer,
    /// Bounded reordering: an item is delivered at most `k` positions
    /// from its global arrival order.
    KOutOfOrder(u32),
    /// No ordering guarantee.
    Unordered,
}

/// Declared operation envelope for a ring. The two count bounds use
/// `0` to mean "unbounded".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingContract {
    /// Max concurrently-registered producers (`0` = unbounded).
    pub max_concurrent_push: u8,
    /// Max concurrently-registered consumers (`0` = unbounded).
    pub max_concurrent_pop: u8,
    /// The ordering envelope the consumers may observe.
    pub ordering: OrderingContract,
    /// Optional hard ceiling on ring capacity (`None` = unbounded).
    pub capacity_bound: Option<u32>,
}

impl RingContract {
    /// A contract that PINS the peer counts: registration past
    /// `max_producers` / `max_consumers` returns `TooManyProducers` /
    /// `TooManyConsumers` instead of growing the ring. No ordering
    /// constraint, no capacity bound. The common fixed-topology
    /// declaration.
    pub fn from_counts(max_producers: usize, max_consumers: usize) -> Self {
        Self {
            max_concurrent_push: max_producers.min(u8::MAX as usize) as u8,
            max_concurrent_pop: max_consumers.min(u8::MAX as usize) as u8,
            ordering: OrderingContract::Unordered,
            capacity_bound: None,
        }
    }

    /// The fully-unbounded contract: any peer counts, any capacity,
    /// no ordering constraint. The DEFAULT for rings with no declared
    /// contract - registration never fails under it (peers grow the
    /// ring on demand up to the substrate slot ceilings).
    pub fn unbounded() -> Self {
        Self {
            max_concurrent_push: 0,
            max_concurrent_pop: 0,
            ordering: OrderingContract::Unordered,
            capacity_bound: None,
        }
    }

    /// May another producer attach, given `active` are registered?
    #[inline]
    pub fn permits_producer(&self, active: usize) -> bool {
        self.max_concurrent_push == 0 || active < self.max_concurrent_push as usize
    }

    /// May another consumer attach, given `active` are registered?
    #[inline]
    pub fn permits_consumer(&self, active: usize) -> bool {
        self.max_concurrent_pop == 0 || active < self.max_concurrent_pop as usize
    }

    /// Is a ring of `capacity` slots legal under the contract?
    #[inline]
    pub fn permits_capacity(&self, capacity: usize) -> bool {
        match self.capacity_bound {
            None => true,
            Some(bound) => capacity <= bound as usize,
        }
    }

    /// Is `shape` legal under the ordering contract? Global total order
    /// is preserved only by the single-stream [`Spsc`](RingShape::Spsc)
    /// and the shared-sequence [`Vyukov`](RingShape::Vyukov); the
    /// partitioned per-producer-lane shapes
    /// ([`Mpsc`](RingShape::Mpsc), [`Mpmc`](RingShape::Mpmc)) interleave
    /// producers, so both are illegal under a `Fifo` contract. Every
    /// other contract permits every shape (`FifoPerProducer` is exactly
    /// what the lanes deliver).
    #[inline]
    pub fn permits_shape(&self, shape: RingShape) -> bool {
        match self.ordering {
            OrderingContract::Fifo => {
                matches!(shape, RingShape::Spsc | RingShape::Vyukov)
            }
            _ => true,
        }
    }

    /// Feasible-region oracle for an adaptive policy: is a candidate
    /// `(shape, capacity)` configuration legal? A policy filters every
    /// move it proposes through this, so an aggressive auto-morph
    /// cannot violate the declared envelope.
    #[inline]
    pub fn permits_config(&self, shape: RingShape, capacity: usize) -> bool {
        self.permits_shape(shape) && self.permits_capacity(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_counts_pins_the_peer_ceilings() {
        let g = RingContract::from_counts(4, 2);
        assert_eq!(g.max_concurrent_push, 4);
        assert_eq!(g.max_concurrent_pop, 2);
        assert_eq!(g.ordering, OrderingContract::Unordered);
        assert_eq!(g.capacity_bound, None);
        // The pin: ids 0..4 permitted, a 5th producer rejected.
        assert!(g.permits_producer(0) && g.permits_producer(3));
        assert!(!g.permits_producer(4));
        assert!(g.permits_consumer(0) && g.permits_consumer(1));
        assert!(!g.permits_consumer(2));
    }

    #[test]
    fn unbounded_counts_permit_any() {
        let g = RingContract {
            max_concurrent_push: 0,
            max_concurrent_pop: 0,
            ordering: OrderingContract::Unordered,
            capacity_bound: None,
        };
        assert!(g.permits_producer(1000));
        assert!(g.permits_consumer(1000));
    }

    #[test]
    fn fifo_permits_only_order_preserving_shapes() {
        let fifo = RingContract {
            max_concurrent_push: 0,
            max_concurrent_pop: 0,
            ordering: OrderingContract::Fifo,
            capacity_bound: None,
        };
        // Single-stream and shared-sequence preserve global total order.
        assert!(fifo.permits_shape(RingShape::Spsc));
        assert!(fifo.permits_shape(RingShape::Vyukov));
        // Both partitioned per-producer-lane shapes reorder producers.
        assert!(!fifo.permits_shape(RingShape::Mpsc));
        assert!(!fifo.permits_shape(RingShape::Mpmc));
    }

    #[test]
    fn relaxed_contracts_permit_every_shape() {
        for ordering in [
            OrderingContract::FifoPerProducer,
            OrderingContract::KOutOfOrder(8),
            OrderingContract::Unordered,
        ] {
            let g = RingContract {
                max_concurrent_push: 0,
                max_concurrent_pop: 0,
                ordering,
                capacity_bound: None,
            };
            for shape in [
                RingShape::Spsc,
                RingShape::Mpsc,
                RingShape::Mpmc,
                RingShape::Vyukov,
            ] {
                assert!(g.permits_shape(shape), "{ordering:?} should permit {shape:?}");
            }
        }
    }

    #[test]
    fn capacity_bound_enforced() {
        let g = RingContract {
            max_concurrent_push: 0,
            max_concurrent_pop: 0,
            ordering: OrderingContract::Unordered,
            capacity_bound: Some(1024),
        };
        assert!(g.permits_capacity(512) && g.permits_capacity(1024));
        assert!(!g.permits_capacity(2048));
        // permits_config combines shape + capacity.
        assert!(g.permits_config(RingShape::Mpmc, 1024));
        assert!(!g.permits_config(RingShape::Mpmc, 2048));
    }

    #[test]
    fn fifo_with_capacity_combined_in_permits_config() {
        let g = RingContract {
            max_concurrent_push: 4,
            max_concurrent_pop: 4,
            ordering: OrderingContract::Fifo,
            capacity_bound: Some(4096),
        };
        // Mpmc illegal regardless of capacity under Fifo.
        assert!(!g.permits_config(RingShape::Mpmc, 1024));
        // Vyukov legal at/under the capacity bound, illegal above it.
        assert!(g.permits_config(RingShape::Vyukov, 4096));
        assert!(!g.permits_config(RingShape::Vyukov, 8192));
    }
}
