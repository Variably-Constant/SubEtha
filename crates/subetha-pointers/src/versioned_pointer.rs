//! Versioned/MVCC pointers - time-travel addressing for snapshot
//! isolation, immutable trees, and distributed clocks.
//!
//! Three pointer flavours sharing a common shape `(version, target)`:
//!
//! | Type                     | Version | Use case                                |
//! |--------------------------|---------|-----------------------------------------|
//! | [`VersionedPointer<T>`]  | u64     | Local MVCC, snapshot isolation          |
//! | [`HlcVersionedPointer<T>`] | (u64 physical, u64 logical) | Distributed (CockroachDB-style HLC) |
//! | [`VectorClockPointer<T, N>`] | `[u64; N]` per-node | Per-node causal ordering (Riak-style) |
//!
//! Plus a [`VersionedChain<T>`] linked-list of `VersionedNode<T>`s
//! that retains all historical versions for time-travel queries.
//!
//! # The K_temporal / K_cascade interplay
//!
//! `VersionedPointer<T>` is a single-version snapshot.
//! `VersionedPointer<VersionedPointer<T>>` is the K_cascade = 2 case:
//! outer carries coarse (physical) time, inner carries fine (logical)
//! counter. This is exactly the Hybrid Logical Clock pattern that
//! CockroachDB and Spanner use - here exposed as a first-class
//! typed primitive via [`HlcVersionedPointer<T>`].

use std::cmp::Ordering;
use std::sync::Arc;

// =========================================================
// Monotonic VersionedPointer<T>
// =========================================================

/// Pointer + monotonic u64 version. Used for snapshot-isolation
/// reads: visible at a query snapshot when `self.version <= snapshot`.
#[derive(Debug, Clone)]
pub struct VersionedPointer<T> {
    version: u64,
    target: Arc<T>,
}

impl<T> VersionedPointer<T> {
    /// Direction signature of `VersionedPointer<T>`. Engages the
    /// `K_version` axis (u64 version counter stored at slot for
    /// ABA-safe CAS and MVCC visibility checks).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Version],
    );

    pub const fn new(target: Arc<T>, version: u64) -> Self {
        Self { version, target }
    }

    #[inline]
    pub const fn version(&self) -> u64 { self.version }

    #[inline]
    pub fn target(&self) -> &Arc<T> { &self.target }

    /// True when this pointer is observable at `snapshot_version`.
    /// Standard MVCC visibility: visible if its version is at or
    /// before the snapshot.
    #[inline]
    pub const fn visible_at(&self, snapshot_version: u64) -> bool {
        self.version <= snapshot_version
    }

    /// Return the target if it is visible at `snapshot_version`.
    pub fn read_at(&self, snapshot_version: u64) -> Option<&T> {
        if self.visible_at(snapshot_version) {
            Some(&self.target)
        } else {
            None
        }
    }

    /// Replace the target with a new version. Returns the previous
    /// version. Panics if `new_version <= self.version` because
    /// MVCC requires monotonic version growth.
    pub fn replace(&mut self, new_target: Arc<T>, new_version: u64) -> u64 {
        assert!(
            new_version > self.version,
            "MVCC version must be strictly monotonic: {} -> {}",
            self.version, new_version
        );
        let old = self.version;
        self.version = new_version;
        self.target = new_target;
        old
    }
}

impl<T> PartialEq for VersionedPointer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && Arc::ptr_eq(&self.target, &other.target)
    }
}

impl<T> PartialOrd for VersionedPointer<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.version.cmp(&other.version))
    }
}

// =========================================================
// HybridLogicalClock + HlcVersionedPointer<T>
// =========================================================

/// Hybrid Logical Clock: (physical timestamp, logical counter) pair.
/// Combines wall-clock time (microsecond resolution typical) with a
/// per-node monotonic counter that breaks ties between events
/// recorded in the same physical instant.
///
/// This is the K_cascade = 2 case of versioning: the outer level
/// (physical) is coarse; the inner level (logical) refines tied
/// physical timestamps. Same architectural pattern as Umbra prefix
/// + actual content, applied to time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HybridLogicalClock {
    pub physical: u64,
    pub logical: u64,
}

impl HybridLogicalClock {
    pub const fn new(physical: u64, logical: u64) -> Self {
        Self { physical, logical }
    }

    /// Construct from the current wall clock (microseconds since
    /// UNIX epoch), with logical counter starting at 0.
    pub fn now() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        Self { physical: now, logical: 0 }
    }

    /// Advance: increment logical counter (same physical) or jump to
    /// `new_physical` if larger. Used by an HLC source to record a
    /// new event.
    pub fn advance(&self, new_physical: u64) -> Self {
        if new_physical > self.physical {
            Self { physical: new_physical, logical: 0 }
        } else {
            Self { physical: self.physical, logical: self.logical + 1 }
        }
    }

    /// Merge with a received event: take max physical; advance logical
    /// if needed. This is the receiver-side HLC update.
    pub fn merge(&self, received: &Self, local_physical: u64) -> Self {
        let max_phys = self.physical.max(received.physical).max(local_physical);
        let new_logical = if max_phys == self.physical && max_phys == received.physical {
            self.logical.max(received.logical) + 1
        } else if max_phys == self.physical {
            self.logical + 1
        } else if max_phys == received.physical {
            received.logical + 1
        } else {
            0
        };
        Self { physical: max_phys, logical: new_logical }
    }
}

impl PartialOrd for HybridLogicalClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HybridLogicalClock {
    fn cmp(&self, other: &Self) -> Ordering {
        // Lexicographic: physical first, then logical.
        self.physical.cmp(&other.physical)
            .then(self.logical.cmp(&other.logical))
    }
}

/// Pointer + HLC. Composes [`VersionedPointer`] with the cascade
/// structure of `(physical, logical)`. Two-level rejection on
/// `visible_at`: physical mismatch rejects fast, logical compares
/// only when physical ties.
#[derive(Debug, Clone)]
pub struct HlcVersionedPointer<T> {
    clock: HybridLogicalClock,
    target: Arc<T>,
}

impl<T> HlcVersionedPointer<T> {
    /// Direction signature of `HlcVersionedPointer<T>`. Engages the
    /// `K_version` axis (hybrid-logical-clock version stored at
    /// slot for distributed MVCC).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Version],
    );

    pub const fn new(target: Arc<T>, clock: HybridLogicalClock) -> Self {
        Self { clock, target }
    }

    #[inline]
    pub const fn clock(&self) -> HybridLogicalClock { self.clock }

    #[inline]
    pub fn target(&self) -> &Arc<T> { &self.target }

    pub fn visible_at(&self, snapshot: HybridLogicalClock) -> bool {
        self.clock <= snapshot
    }

    pub fn read_at(&self, snapshot: HybridLogicalClock) -> Option<&T> {
        if self.visible_at(snapshot) { Some(&self.target) } else { None }
    }
}

// =========================================================
// VectorClock + VectorClockPointer<T, N>
// =========================================================

/// Per-node monotonic counters. `N` is the number of nodes in the
/// system; `clock[i]` is node `i`'s observed event count. Causal
/// ordering: `a` causally precedes `b` when every component of `a`
/// is <= the corresponding component of `b`, with at least one
/// strict less-than.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorClock<const N: usize> {
    pub clock: [u64; N],
}

impl<const N: usize> VectorClock<N> {
    pub const fn zero() -> Self { Self { clock: [0; N] } }

    pub fn increment(&mut self, node_idx: usize) {
        self.clock[node_idx] += 1;
    }

    /// Causal-order check: returns `Less` when self happens-before
    /// other (every component <= and at least one <), `Greater` when
    /// other happens-before self, `Equal` when identical, `None`
    /// when concurrent (incomparable).
    pub fn causal_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut all_le = true;
        let mut all_ge = true;
        let mut strict = false;
        for i in 0..N {
            match self.clock[i].cmp(&other.clock[i]) {
                Ordering::Less    => { all_ge = false; strict = true; }
                Ordering::Greater => { all_le = false; strict = true; }
                Ordering::Equal => {}
            }
        }
        match (all_le, all_ge, strict) {
            (true, true, false) => Some(Ordering::Equal),
            (true, false, true) => Some(Ordering::Less),
            (false, true, true) => Some(Ordering::Greater),
            _ => None,
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        let mut out = Self::zero();
        for i in 0..N {
            out.clock[i] = self.clock[i].max(other.clock[i]);
        }
        out
    }
}

/// Pointer + vector clock. Useful for distributed CRDT-style
/// snapshot reads where causal-but-concurrent updates must be
/// surfaced rather than ordered.
#[derive(Debug, Clone)]
pub struct VectorClockPointer<T, const N: usize> {
    clock: VectorClock<N>,
    target: Arc<T>,
}

impl<T, const N: usize> VectorClockPointer<T, N> {
    pub const fn new(target: Arc<T>, clock: VectorClock<N>) -> Self {
        Self { clock, target }
    }

    pub fn clock(&self) -> VectorClock<N> { self.clock }
    pub fn target(&self) -> &Arc<T> { &self.target }

    /// Returns the target if it causally precedes or equals
    /// `snapshot`. Returns `None` for concurrent / future events.
    pub fn read_at(&self, snapshot: VectorClock<N>) -> Option<&T> {
        match self.clock.causal_cmp(&snapshot) {
            Some(Ordering::Less | Ordering::Equal) => Some(&self.target),
            _ => None,
        }
    }
}

// =========================================================
// VersionedChain<T> - MVCC linked list of historical versions
// =========================================================

/// Linked list of `(version, value)` nodes ordered newest-first.
/// Time-travel reads walk the chain until they find a version <=
/// the query snapshot.
pub struct VersionedChain<T: Clone> {
    head: parking_lot::RwLock<Option<Arc<VersionNode<T>>>>,
}

struct VersionNode<T> {
    version: u64,
    value: T,
    older: Option<Arc<VersionNode<T>>>,
}

impl<T: Clone> VersionedChain<T> {
    pub fn new() -> Self {
        Self { head: parking_lot::RwLock::new(None) }
    }

    /// Add a new version to the head. `new_version` must strictly
    /// exceed the current head's version.
    pub fn push(&self, value: T, new_version: u64) {
        let mut h = self.head.write();
        if let Some(cur) = h.as_ref() {
            assert!(
                new_version > cur.version,
                "MVCC chain version must be strictly monotonic: {} -> {}",
                cur.version, new_version
            );
        }
        let older = h.take();
        *h = Some(Arc::new(VersionNode { version: new_version, value, older }));
    }

    /// Read the value visible at `snapshot_version`. Walks back
    /// through history until a node with version <= snapshot is found.
    pub fn read_at(&self, snapshot_version: u64) -> Option<T> {
        let h = self.head.read();
        let mut cur = h.clone();
        while let Some(node) = cur {
            if node.version <= snapshot_version {
                return Some(node.value.clone());
            }
            cur = node.older.clone();
        }
        None
    }

    /// Current (latest) version and value.
    pub fn current(&self) -> Option<(u64, T)> {
        self.head.read().as_ref().map(|n| (n.version, n.value.clone()))
    }

    /// Chain length (number of retained versions).
    pub fn len(&self) -> usize {
        let h = self.head.read();
        let mut cur = h.clone();
        let mut n = 0;
        while let Some(node) = cur {
            n += 1;
            cur = node.older.clone();
        }
        n
    }

    pub fn is_empty(&self) -> bool { self.head.read().is_none() }
}

impl<T: Clone> Default for VersionedChain<T> {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versioned_pointer_visibility() {
        let p = VersionedPointer::new(Arc::new("hello".to_string()), 100);
        assert!(p.visible_at(100));
        assert!(p.visible_at(101));
        assert!(!p.visible_at(99));
        assert_eq!(p.read_at(99), None);
        assert_eq!(p.read_at(150).map(|s| s.as_str()), Some("hello"));
    }

    #[test]
    fn versioned_pointer_replace_enforces_monotonic() {
        let mut p = VersionedPointer::new(Arc::new(1u64), 10);
        let old = p.replace(Arc::new(2), 11);
        assert_eq!(old, 10);
        assert_eq!(p.version(), 11);
        assert_eq!(**p.target(), 2);
    }

    #[test]
    #[should_panic(expected = "monotonic")]
    fn versioned_pointer_replace_rejects_non_monotonic() {
        let mut p = VersionedPointer::new(Arc::new(1u64), 10);
        let _val = p.replace(Arc::new(2), 5);
    }

    #[test]
    fn hlc_advances_logical_within_physical() {
        let h0 = HybridLogicalClock::new(1000, 0);
        let h1 = h0.advance(1000);
        assert_eq!(h1.physical, 1000);
        assert_eq!(h1.logical, 1);
        let h2 = h1.advance(1001);
        assert_eq!(h2.physical, 1001);
        assert_eq!(h2.logical, 0);
    }

    #[test]
    fn hlc_lexicographic_ordering() {
        let a = HybridLogicalClock::new(100, 5);
        let b = HybridLogicalClock::new(100, 7);
        let c = HybridLogicalClock::new(101, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
        assert!(c > a);
    }

    #[test]
    fn hlc_merge_takes_max_then_bumps_logical() {
        let local = HybridLogicalClock::new(100, 5);
        let received = HybridLogicalClock::new(100, 8);
        let merged = local.merge(&received, 100);
        assert_eq!(merged.physical, 100);
        assert_eq!(merged.logical, 9);

        let received2 = HybridLogicalClock::new(200, 0);
        let merged2 = local.merge(&received2, 100);
        assert_eq!(merged2.physical, 200);
        assert_eq!(merged2.logical, 1);
    }

    #[test]
    fn hlc_pointer_visibility() {
        let snapshot = HybridLogicalClock::new(1000, 5);
        let p1 = HlcVersionedPointer::new(Arc::new(1u64), HybridLogicalClock::new(999, 99));
        let p2 = HlcVersionedPointer::new(Arc::new(2u64), HybridLogicalClock::new(1000, 5));
        let p3 = HlcVersionedPointer::new(Arc::new(3u64), HybridLogicalClock::new(1000, 6));
        assert_eq!(p1.read_at(snapshot).copied(), Some(1));
        assert_eq!(p2.read_at(snapshot).copied(), Some(2));
        assert_eq!(p3.read_at(snapshot), None);
    }

    #[test]
    fn vector_clock_causal_ordering() {
        // 3-node system.
        let a = VectorClock::<3> { clock: [1, 0, 0] };
        let b = VectorClock::<3> { clock: [1, 1, 0] };
        let c = VectorClock::<3> { clock: [0, 0, 1] };
        // a happens-before b (b extends a).
        assert_eq!(a.causal_cmp(&b), Some(Ordering::Less));
        assert_eq!(b.causal_cmp(&a), Some(Ordering::Greater));
        // a equal to itself.
        assert_eq!(a.causal_cmp(&a), Some(Ordering::Equal));
        // a and c are concurrent.
        assert_eq!(a.causal_cmp(&c), None);
        assert_eq!(c.causal_cmp(&a), None);
    }

    #[test]
    fn vector_clock_merge_takes_pointwise_max() {
        let a = VectorClock::<3> { clock: [1, 0, 0] };
        let b = VectorClock::<3> { clock: [0, 0, 1] };
        let m = a.merge(&b);
        assert_eq!(m.clock, [1, 0, 1]);
    }

    #[test]
    fn vector_clock_pointer_concurrent_read_is_none() {
        let snapshot = VectorClock::<3> { clock: [1, 1, 1] };
        let visible = VectorClockPointer::new(
            Arc::new(1u64),
            VectorClock::<3> { clock: [1, 0, 0] },
        );
        let future = VectorClockPointer::new(
            Arc::new(2u64),
            VectorClock::<3> { clock: [2, 0, 0] },
        );
        assert!(visible.read_at(snapshot).is_some());
        assert!(future.read_at(snapshot).is_none());
    }

    #[test]
    fn versioned_chain_basic_push_and_read() {
        let chain = VersionedChain::<u64>::new();
        chain.push(10, 1);
        chain.push(20, 2);
        chain.push(30, 3);
        assert_eq!(chain.len(), 3);
        // Time-travel reads.
        assert_eq!(chain.read_at(0), None);
        assert_eq!(chain.read_at(1), Some(10));
        assert_eq!(chain.read_at(2), Some(20));
        assert_eq!(chain.read_at(3), Some(30));
        assert_eq!(chain.read_at(99), Some(30));
        assert_eq!(chain.current(), Some((3, 30)));
    }

    #[test]
    #[should_panic(expected = "monotonic")]
    fn versioned_chain_push_rejects_non_monotonic() {
        let chain = VersionedChain::<u64>::new();
        chain.push(10, 5);
        chain.push(20, 3);  // earlier; must panic
    }

    #[test]
    fn versioned_cascade_outer_inner_pattern() {
        // K_cascade=2 demonstration: VersionedPointer<VersionedPointer<T>>.
        // Outer carries a coarse version, inner carries a finer version
        // (same shape as HLC but using monotonic instead of (phys, logic)).
        let inner = VersionedPointer::new(Arc::new(42u64), 100);
        let outer = VersionedPointer::new(Arc::new(inner.clone()), 1);

        // Outer-visible at coarse snapshot 5.
        assert!(outer.visible_at(5));
        let inner_ref = outer.read_at(5).unwrap();
        // Inner check is at the finer scale.
        assert!(inner_ref.visible_at(101));
        assert!(!inner_ref.visible_at(99));
    }
}
