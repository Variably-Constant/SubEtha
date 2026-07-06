//! `QosPolicy`: DDS-inspired Quality-of-Service knobs that sidecar
//! policies read as input alongside peer counts and workload shape.
//!
//! Mirrors the DDS (Data Distribution Service) QoS model that
//! publishers and subscribers negotiate at connection time: in DDS
//! the knobs are static; in this substrate they are runtime-mutable
//! atomics, so a sidecar can re-read them every scan and adapt the
//! shape / locale / protocol decisions accordingly.
//!
//! # Knobs
//!
//! - [`Durability`]: Volatile (no persistence), Transient (in-memory
//!   cross-process), Persistent (disk-backed). Maps to a
//!   recommended `Locale` for `LocaleAdaptiveRing`.
//! - [`Reliability`]: BestEffort (drop on backpressure) vs Reliable
//!   (backpressure-block sender until consumer catches up).
//! - [`History`]: KeepLastN (bounded buffer of latest N items) vs
//!   KeepAll (capacity-bounded, no automatic dropping). Drives the
//!   recommended ring capacity.
//! - [`max_latency`](QosPolicy::max_latency): caller's wish on
//!   delivery latency; sidecar policies factor this when choosing
//!   between batched (lower throughput-cost-per-item) and
//!   single-item (lower latency) dispatch.
//!
//! All four knobs are mutable at runtime via atomic stores. The
//! sidecar reads them on every scan cycle.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomOrd};
use std::time::Duration;

use crate::locale_adaptive_ring::Locale;

/// Where the substrate stores bytes for the substrate's contract
/// with the durability knob.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// In-process anonymous memory; data evaporates on process exit.
    Volatile = 0,
    /// Cross-process RAM-resident shared memory; survives the
    /// producer's exit if another holder keeps the named region
    /// alive, but is not on disk.
    Transient = 1,
    /// File-backed mmap; the bytes hit the page cache and can
    /// persist to disk via flush.
    Persistent = 2,
}

impl Durability {
    /// Map a durability setting to the matching `Locale` member.
    pub fn recommended_locale(self) -> Locale {
        match self {
            Self::Volatile => Locale::Anon,
            Self::Transient => Locale::ShmFs,
            Self::Persistent => Locale::File,
        }
    }

    fn from_u32(tag: u32) -> Self {
        match tag {
            0 => Self::Volatile,
            1 => Self::Transient,
            2 => Self::Persistent,
            _ => panic!("QosPolicy.durability corrupted: {tag}"),
        }
    }
}

/// Whether the substrate drops items under backpressure or blocks
/// the sender until the consumer catches up.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reliability {
    /// Drop items when the ring is full; sender's `try_send`
    /// returns `Err(Full)` immediately. Lowest latency on the send
    /// side; lossy under backpressure.
    BestEffort = 0,
    /// Block-spin the sender (or async-yield) until the ring has
    /// capacity. Lossless; latency rises under backpressure.
    Reliable = 1,
}

impl Reliability {
    fn from_u32(tag: u32) -> Self {
        match tag {
            0 => Self::BestEffort,
            1 => Self::Reliable,
            _ => panic!("QosPolicy.reliability corrupted: {tag}"),
        }
    }
}

/// History depth policy: how many items the substrate retains for
/// late-joining subscribers (or for replay).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum History {
    /// Keep at most the last N items. Older items get dropped to
    /// make room when the buffer fills.
    KeepLastN(u32),
    /// Keep every item up to ring capacity. When capacity is
    /// reached, the reliability policy decides what happens.
    KeepAll,
}

/// Whether the consumer cares about cross-producer delivery order.
///
/// Ordering need is semantic - it lives in the application, not in
/// the traffic - so the substrate never auto-changes a correctness
/// property on a heuristic. The caller DECLARES the need here; the
/// sidecar acts on the declaration: an unstamped
/// [`AdaptiveRing`](crate::AdaptiveRing) morphs to the Vyukov shape
/// (the proven global-FIFO structure), a stamped ring flips its
/// merge flag (the cheap ordered switch). What the substrate
/// observes on its own is the cross-producer inversion RATE, which
/// it reports - and acts on only when the caller pre-authorized an
/// automatic response via an `auto_order` threshold.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    /// Items from one producer arrive in that producer's push
    /// order; no guarantee across producers. The composed shapes'
    /// native (and cheapest) guarantee.
    PerProducer = 0,
    /// Items arrive in global push order across all producers.
    GlobalFifo = 1,
}

impl Ordering {
    fn from_u32(tag: u32) -> Self {
        match tag {
            0 => Self::PerProducer,
            1 => Self::GlobalFifo,
            _ => panic!("QosPolicy.ordering corrupted: {tag}"),
        }
    }
}

impl History {
    /// Recommended ring capacity for this history setting. Caller
    /// can clamp / round up to a power of 2 as needed (the ring
    /// primitives require pow2 capacity).
    pub fn recommended_capacity(self) -> usize {
        match self {
            Self::KeepLastN(n) => (n as usize).next_power_of_two().max(16),
            // KeepAll has no inherent ceiling; pick a reasonable
            // default that the caller can override.
            Self::KeepAll => 1024,
        }
    }

    fn encode(self) -> u64 {
        match self {
            Self::KeepLastN(n) => (1u64 << 32) | u64::from(n),
            Self::KeepAll => 0,
        }
    }

    fn decode(raw: u64) -> Self {
        if raw == 0 {
            Self::KeepAll
        } else {
            let n = (raw & 0xFFFF_FFFF) as u32;
            Self::KeepLastN(n)
        }
    }
}

/// Runtime-mutable QoS policy. Sidecar policies read these atomics
/// on every scan; setters publish with `Release`, readers consume
/// with `Acquire`.
pub struct QosPolicy {
    durability_atom: AtomicU32,
    reliability_atom: AtomicU32,
    history_atom: AtomicU64,
    max_latency_nanos: AtomicU64,
    ordering_atom: AtomicU32,
}

impl QosPolicy {
    /// Construct with the four original knobs explicitly. The
    /// ordering knob starts at [`Ordering::PerProducer`] (the
    /// composed shapes' native guarantee); declare
    /// [`Ordering::GlobalFifo`] via
    /// [`set_ordering`](Self::set_ordering).
    pub fn new(
        durability: Durability,
        reliability: Reliability,
        history: History,
        max_latency: Duration,
    ) -> Self {
        Self {
            durability_atom: AtomicU32::new(durability as u32),
            reliability_atom: AtomicU32::new(reliability as u32),
            history_atom: AtomicU64::new(history.encode()),
            max_latency_nanos: AtomicU64::new(
                max_latency.as_nanos().min(u64::MAX as u128) as u64,
            ),
            ordering_atom: AtomicU32::new(Ordering::PerProducer as u32),
        }
    }

    /// Default policy: Volatile, BestEffort, KeepLastN(1024),
    /// max_latency = 100ms. Reasonable for streaming workloads.
    pub fn streaming_default() -> Self {
        Self::new(
            Durability::Volatile,
            Reliability::BestEffort,
            History::KeepLastN(1024),
            Duration::from_millis(100),
        )
    }

    /// Reliable-pubsub default: Transient, Reliable, KeepAll,
    /// max_latency = 1s. Reasonable for cross-process pub/sub where
    /// every message matters.
    pub fn reliable_pubsub_default() -> Self {
        Self::new(
            Durability::Transient,
            Reliability::Reliable,
            History::KeepAll,
            Duration::from_secs(1),
        )
    }

    /// Persistent-log default: Persistent, Reliable, KeepAll,
    /// max_latency = 5s. Reasonable for durable event logs.
    pub fn persistent_log_default() -> Self {
        Self::new(
            Durability::Persistent,
            Reliability::Reliable,
            History::KeepAll,
            Duration::from_secs(5),
        )
    }

    /// Current durability setting.
    pub fn durability(&self) -> Durability {
        Durability::from_u32(self.durability_atom.load(AtomOrd::Acquire))
    }

    /// Current reliability setting.
    pub fn reliability(&self) -> Reliability {
        Reliability::from_u32(self.reliability_atom.load(AtomOrd::Acquire))
    }

    /// Current history setting.
    pub fn history(&self) -> History {
        History::decode(self.history_atom.load(AtomOrd::Acquire))
    }

    /// Current max-latency wish.
    pub fn max_latency(&self) -> Duration {
        Duration::from_nanos(self.max_latency_nanos.load(AtomOrd::Acquire))
    }

    /// Current ordering declaration.
    pub fn ordering(&self) -> Ordering {
        Ordering::from_u32(self.ordering_atom.load(AtomOrd::Acquire))
    }

    /// Replace the durability knob. Sidecars see the change on the
    /// next scan.
    pub fn set_durability(&self, durability: Durability) {
        self.durability_atom.store(durability as u32, AtomOrd::Release);
    }

    /// Replace the reliability knob.
    pub fn set_reliability(&self, reliability: Reliability) {
        self.reliability_atom.store(reliability as u32, AtomOrd::Release);
    }

    /// Replace the history knob.
    pub fn set_history(&self, history: History) {
        self.history_atom.store(history.encode(), AtomOrd::Release);
    }

    /// Replace the max-latency wish.
    pub fn set_max_latency(&self, max_latency: Duration) {
        self.max_latency_nanos.store(
            max_latency.as_nanos().min(u64::MAX as u128) as u64,
            AtomOrd::Release,
        );
    }

    /// Replace the ordering declaration. Sidecars see the change on
    /// the next scan and act per the routing in
    /// [`Ordering`]'s docs (Vyukov morph for unstamped rings, merge
    /// flag for stamped rings).
    pub fn set_ordering(&self, ordering: Ordering) {
        self.ordering_atom.store(ordering as u32, AtomOrd::Release);
    }

    /// Snapshot: read all five knobs in one method for sidecar use.
    /// Each load is independently Acquire-ordered; the snapshot is
    /// NOT a consistent point-in-time view across all five (the
    /// substrate does not need that property).
    pub fn snapshot(&self) -> QosSnapshot {
        QosSnapshot {
            durability: self.durability(),
            reliability: self.reliability(),
            history: self.history(),
            max_latency: self.max_latency(),
            ordering: self.ordering(),
        }
    }
}

impl Default for QosPolicy {
    fn default() -> Self { Self::streaming_default() }
}

/// Point-in-time snapshot of a `QosPolicy`. Useful for passing to
/// sidecar policy decisions or for inspecting current settings
/// without holding a reference to the live atomics.
#[derive(Debug, Clone, Copy)]
pub struct QosSnapshot {
    pub durability: Durability,
    pub reliability: Reliability,
    pub history: History,
    pub max_latency: Duration,
    pub ordering: Ordering,
}

impl QosSnapshot {
    /// Whether this snapshot recommends a locale change relative to
    /// the currently-active locale.
    pub fn recommends_locale_change(self, current: Locale) -> Option<Locale> {
        let recommended = self.durability.recommended_locale();
        if recommended == current {
            None
        } else {
            Some(recommended)
        }
    }

    /// Whether this snapshot's ordering declaration differs from
    /// the guarantee the caller currently provides. Mirrors
    /// [`recommends_locale_change`](Self::recommends_locale_change):
    /// `Some(declared)` means the sidecar should act (Vyukov morph
    /// or merge-flag flip, per the ring's stampedness).
    pub fn recommends_ordering_change(self, current: Ordering) -> Option<Ordering> {
        if self.ordering == current {
            None
        } else {
            Some(self.ordering)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load_with_sensible_values() {
        let qos = QosPolicy::default();
        assert_eq!(qos.durability(), Durability::Volatile);
        assert_eq!(qos.reliability(), Reliability::BestEffort);
        assert!(matches!(qos.history(), History::KeepLastN(_)));
        assert_eq!(qos.max_latency(), Duration::from_millis(100));
    }

    #[test]
    fn setters_update_atomic_fields() {
        let qos = QosPolicy::default();
        qos.set_durability(Durability::Persistent);
        qos.set_reliability(Reliability::Reliable);
        qos.set_history(History::KeepAll);
        qos.set_max_latency(Duration::from_secs(10));

        let snap = qos.snapshot();
        assert_eq!(snap.durability, Durability::Persistent);
        assert_eq!(snap.reliability, Reliability::Reliable);
        assert!(matches!(snap.history, History::KeepAll));
        assert_eq!(snap.max_latency, Duration::from_secs(10));
    }

    #[test]
    fn durability_maps_to_locale() {
        assert_eq!(Durability::Volatile.recommended_locale(), Locale::Anon);
        assert_eq!(Durability::Transient.recommended_locale(), Locale::ShmFs);
        assert_eq!(Durability::Persistent.recommended_locale(), Locale::File);
    }

    #[test]
    fn history_recommended_capacity_powers_of_two() {
        assert_eq!(History::KeepLastN(100).recommended_capacity(), 128);
        assert_eq!(History::KeepLastN(1).recommended_capacity(), 16);
        assert_eq!(History::KeepLastN(2048).recommended_capacity(), 2048);
        assert_eq!(History::KeepAll.recommended_capacity(), 1024);
    }

    #[test]
    fn snapshot_recommends_locale_change_only_when_different() {
        let qos = QosPolicy::default();  // Volatile -> Anon
        let snap = qos.snapshot();
        assert_eq!(snap.recommends_locale_change(Locale::Anon), None);
        assert_eq!(snap.recommends_locale_change(Locale::File), Some(Locale::Anon));
        assert_eq!(snap.recommends_locale_change(Locale::ShmFs), Some(Locale::Anon));
    }

    #[test]
    fn ordering_knob_round_trips() {
        let qos = QosPolicy::default();
        assert_eq!(qos.ordering(), Ordering::PerProducer,
                   "ordering must default to the composed shapes' native guarantee");
        qos.set_ordering(Ordering::GlobalFifo);
        assert_eq!(qos.ordering(), Ordering::GlobalFifo);
        assert_eq!(qos.snapshot().ordering, Ordering::GlobalFifo);
        qos.set_ordering(Ordering::PerProducer);
        assert_eq!(qos.snapshot().ordering, Ordering::PerProducer);
    }

    #[test]
    fn snapshot_recommends_ordering_change_only_when_different() {
        let qos = QosPolicy::default();
        let snap = qos.snapshot();
        assert_eq!(snap.recommends_ordering_change(Ordering::PerProducer), None);
        assert_eq!(snap.recommends_ordering_change(Ordering::GlobalFifo),
                   Some(Ordering::PerProducer));

        qos.set_ordering(Ordering::GlobalFifo);
        let snap = qos.snapshot();
        assert_eq!(snap.recommends_ordering_change(Ordering::PerProducer),
                   Some(Ordering::GlobalFifo));
        assert_eq!(snap.recommends_ordering_change(Ordering::GlobalFifo), None);
    }

    #[test]
    fn presets_match_expected_combos() {
        let streaming = QosPolicy::streaming_default();
        assert_eq!(streaming.durability(), Durability::Volatile);
        assert_eq!(streaming.reliability(), Reliability::BestEffort);

        let pubsub = QosPolicy::reliable_pubsub_default();
        assert_eq!(pubsub.durability(), Durability::Transient);
        assert_eq!(pubsub.reliability(), Reliability::Reliable);
        assert!(matches!(pubsub.history(), History::KeepAll));

        let log = QosPolicy::persistent_log_default();
        assert_eq!(log.durability(), Durability::Persistent);
        assert_eq!(log.reliability(), Reliability::Reliable);
    }
}
