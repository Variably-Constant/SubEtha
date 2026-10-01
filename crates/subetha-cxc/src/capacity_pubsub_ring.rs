//! `CapacityPubSubRing`: runtime-resizable wrapper around
//! [`PubSubRing`] that adds the capacity-axis morph to the
//! pub/sub (many-producer, many-subscriber absolute-position) primitive.
//!
//! Sibling of [`CapacityAdaptiveRing`](crate::CapacityAdaptiveRing)
//! and [`CapacityBroadcastRing`](crate::CapacityBroadcastRing).
//! `CapacityPubSubRing` morphs PubSubRing's slot count at runtime
//! under a chain-of-backings invariant: producers publish to the
//! most-recent backing; subscribers carry their own backing and
//! position and drain each backing in turn before advancing to the
//! next.
//!
//! # The chain
//!
//! Each backing links the one a morph put after it. A morph makes the
//! new backing the one publishes go to, seals the old one, waits for the
//! publishes still inside it to return, and only then links the new
//! backing behind it. A subscriber that finds a successor linked and its
//! own backing empty at its position has therefore drained that backing
//! for good, and crosses into the successor at position 0.
//!
//! # Per-subscriber position tracking
//!
//! Pub/sub's per-subscriber position state already lives outside
//! the ring (in `PubSubSubscriber::position` /
//! [`SubscriberPosition`](crate::replay_positions::SubscriberPosition)),
//! so the capacity-morph wrapper threads each subscriber through
//! the chain of historical backings as the active one rolls
//! forward. A [`CapacityPubSubSubscriber`] holds the backing it reads
//! and its position within that backing. Holding the backing keeps it,
//! and every backing after it, alive until the subscriber moves on.
//!
//! # Chain pruning
//!
//! The wrapper keeps the oldest backing a new
//! [`subscribe_from_oldest`](CapacityPubSubRing::subscribe_from_oldest)
//! starts from. [`gc`](CapacityPubSubRing::gc) moves it forward past
//! every backing no subscriber holds, releasing them. A subscriber holds
//! its own backing, so a gc never takes one from under it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use subetha_core::{SwapCell, SwapCellOption};

use crate::protocol_pubsub::{PubSubReadError, PubSubRing};
use crate::warm_slot::WarmSlot;

/// Errors returned by capacity-morph operations on a pubsub ring.
#[derive(Debug)]
pub enum PubSubCapacityMorphError {
    /// Target capacity is not a power of two, or less than 2.
    InvalidCapacity,
    /// I/O error during backing allocation.
    Io(std::io::Error),
}

impl From<std::io::Error> for PubSubCapacityMorphError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}

impl std::fmt::Display for PubSubCapacityMorphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCapacity => write!(f, "capacity must be pow2 >= 2"),
            Self::Io(e) => write!(f, "io error during pubsub morph: {e}"),
        }
    }
}

impl std::error::Error for PubSubCapacityMorphError {}

/// One backing in the chain.
struct Backing {
    ring: Arc<PubSubRing>,
    /// 0 for the ring's first backing, one more for each morph after it.
    generation: u64,
    /// The backing a morph put after this one, linked once every publish
    /// into this one has returned.
    next: SwapCellOption<Backing>,
    /// Set by the morph that replaced this backing. A publish that finds
    /// it set once it has announced itself backs out and publishes into
    /// the new backing instead.
    sealed: AtomicBool,
    /// Publishes into this backing that have announced themselves and
    /// not yet returned.
    writers: AtomicUsize,
}

impl Backing {
    fn new(ring: Arc<PubSubRing>, generation: u64) -> Self {
        Self {
            ring,
            generation,
            next: SwapCellOption::empty(),
            sealed: AtomicBool::new(false),
            writers: AtomicUsize::new(0),
        }
    }
}

/// Runtime-resizable pubsub ring.
pub struct CapacityPubSubRing {
    /// The backing publishes go to.
    active: SwapCell<Backing>,
    /// The oldest backing kept for a new subscriber from the start. It
    /// links every backing after it, so it holds the chain from here to
    /// the active backing; [`gc`](Self::gc) moves it forward.
    oldest: SwapCell<Backing>,
    /// Bumped on every morph for caller-polled pin invalidation.
    pin_generation: AtomicU64,
    /// Locale source for morph-allocated backings.
    backing_source: PubSubBackingSource,
    /// Monotonic morph counter for path / shm-name uniqueness.
    morph_seq: AtomicU64,
    /// One-slot warm cache: a fully constructed backing at a
    /// predicted capacity, built off the morph's critical path by
    /// [`prewarm`](Self::prewarm). Same design as
    /// `CapacityAdaptiveRing`'s warm cache.
    warm: WarmSlot<usize, Arc<PubSubRing>>,
    /// Successful warm-cache hits consumed by `morph_capacity_to`.
    warm_hits: AtomicU64,
}

enum PubSubBackingSource {
    Anon,
    File(PathBuf),
    Shm(String),
}

impl CapacityPubSubRing {
    fn with_first(ring: PubSubRing, backing_source: PubSubBackingSource, morph_seq: u64) -> Arc<Self> {
        let first = Arc::new(Backing::new(Arc::new(ring), 0));
        Arc::new(Self {
            active: SwapCell::from_arc(Arc::clone(&first)),
            oldest: SwapCell::from_arc(first),
            pin_generation: AtomicU64::new(0),
            backing_source,
            morph_seq: AtomicU64::new(morph_seq),
            warm: WarmSlot::new(),
            warm_hits: AtomicU64::new(0),
        })
    }

    /// Anon (in-process) capacity-adaptive pubsub ring.
    pub fn create_anon(
        initial_capacity: usize,
    ) -> Result<Arc<Self>, PubSubCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        let ring = PubSubRing::create_anon(initial_capacity)?;
        Ok(Self::with_first(ring, PubSubBackingSource::Anon, 0))
    }

    /// File-backed capacity-adaptive pubsub ring.
    pub fn create(
        base_path: impl AsRef<Path>,
        initial_capacity: usize,
    ) -> Result<Arc<Self>, PubSubCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        let base = base_path.as_ref().to_path_buf();
        let path = path_for_capacity_seq(&base, initial_capacity, 0);
        let ring = PubSubRing::create(&path, initial_capacity)?;
        Ok(Self::with_first(ring, PubSubBackingSource::File(base), 1))
    }

    /// ShmFs (named shared memory) capacity-adaptive pubsub ring.
    pub fn create_shmfs(
        name_prefix: &str,
        initial_capacity: usize,
    ) -> Result<Arc<Self>, PubSubCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        let name = format!("{name_prefix}_cap_{initial_capacity}_g0");
        let total = crate::protocol_pubsub::pubsub_ring_file_size(initial_capacity);
        let shm = crate::shm_file::ShmFile::create_named_secured(
            &name, total, crate::shm_file::ShmNamespace::Session, None,
        )?;
        let ring = PubSubRing::create_from_shm(shm, initial_capacity)?;
        Ok(Self::with_first(ring, PubSubBackingSource::Shm(name_prefix.to_owned()), 1))
    }

    /// Current capacity of the active backing.
    pub fn current_capacity(&self) -> usize {
        self.active.load().ring.capacity()
    }

    /// Current pin generation.
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Publish a payload to the currently-active backing. Returns
    /// the absolute position assigned within that backing (not
    /// globally unique across backings - subscribers identify
    /// items via payload contents, not position). Any number of
    /// threads may publish at once.
    ///
    /// A publish announces itself on the backing before it writes and
    /// backs out when a morph has sealed that backing, so every publish
    /// that lands in a backing has returned before a morph links the
    /// backing's successor. A subscriber crosses into the successor only
    /// then, so no publish lands behind a subscriber that has moved on.
    pub fn publish(&self, payload: &[u8]) -> u64 {
        loop {
            let backing = self.active.load();
            backing.writers.fetch_add(1, Ordering::SeqCst);
            if backing.sealed.load(Ordering::SeqCst) {
                backing.writers.fetch_sub(1, Ordering::Release);
                continue;
            }
            let position = backing.ring.publish(payload);
            backing.writers.fetch_sub(1, Ordering::Release);
            return position;
        }
    }

    /// Subscribe to the stream from the currently active backing's
    /// current head. The subscriber drains forward from there,
    /// crossing into newly-morphed backings as it catches up.
    /// "From now" semantics: late joiners do not see history
    /// from before they subscribed.
    pub fn subscribe_from_now(self: &Arc<Self>) -> CapacityPubSubSubscriber {
        let backing = self.active.load_full();
        let position = backing.ring.head();
        CapacityPubSubSubscriber { backing, position }
    }

    /// Subscribe starting from the beginning of the oldest
    /// backing currently in the chain. The subscriber drains
    /// every item from every backing oldest-to-newest, crossing
    /// chain entries as it catches up. Used when a subscriber
    /// needs to replay the full available history.
    pub fn subscribe_from_oldest(self: &Arc<Self>) -> CapacityPubSubSubscriber {
        CapacityPubSubSubscriber { backing: self.oldest.load_full(), position: 0 }
    }

    /// Morph the active backing's capacity. Allocates a fresh
    /// backing at `new_capacity`, makes it the one publishes go to,
    /// bumps pin_generation, and links it behind the old backing once
    /// the publishes still inside the old one have returned.
    /// Subscribers reading from older chain entries continue
    /// undisturbed; they advance into the new backing
    /// individually as their try_next catches up.
    ///
    /// Concurrent morphs each land: a morph that finds another's
    /// backing put in place first tries again from that backing, reusing
    /// the backing it built.
    pub fn morph_capacity_to(
        &self,
        new_capacity: usize,
    ) -> Result<(), PubSubCapacityMorphError> {
        if !new_capacity.is_power_of_two() || new_capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        let mut built: Option<(Arc<PubSubRing>, bool)> = None;
        loop {
            let old = self.active.load_full();
            if old.ring.capacity() == new_capacity {
                return Ok(());
            }
            // Warm-cache probe: a prediction matching the morph target
            // skips allocation entirely; a mismatch stays cached and
            // the cold path runs unchanged.
            let (ring, from_warm) = match built.take() {
                Some(built) => built,
                None => match self.warm.take(&new_capacity) {
                    Some(ring) => (ring, true),
                    None => (self.build_backing(new_capacity)?, false),
                },
            };
            let new = Arc::new(Backing::new(Arc::clone(&ring), old.generation + 1));
            if self.active.compare_and_set(&old, Arc::clone(&new)).is_err() {
                built = Some((ring, from_warm));
                continue;
            }
            if from_warm {
                self.warm_hits.fetch_add(1, Ordering::Relaxed);
            }
            self.pin_generation.fetch_add(1, Ordering::AcqRel);
            // A publish that announced itself on the old backing before
            // the seal writes there and returns; every later one finds the
            // seal and publishes into the new backing.
            old.sealed.store(true, Ordering::SeqCst);
            while old.writers.load(Ordering::SeqCst) != 0 {
                std::thread::yield_now();
            }
            old.next.store(Some(new));
            return Ok(());
        }
    }

    /// Construct a fresh backing at `capacity`, at the wrapper's
    /// locale, with a unique per-build name. Shared by the cold
    /// morph path and [`prewarm`](Self::prewarm).
    fn build_backing(
        &self,
        capacity: usize,
    ) -> Result<Arc<PubSubRing>, PubSubCapacityMorphError> {
        let seq = self.morph_seq.fetch_add(1, Ordering::AcqRel);
        let ring = match &self.backing_source {
            PubSubBackingSource::Anon => PubSubRing::create_anon(capacity)?,
            PubSubBackingSource::File(base) => {
                let path = path_for_capacity_seq(base, capacity, seq);
                PubSubRing::create(&path, capacity)?
            }
            PubSubBackingSource::Shm(prefix) => {
                let name = format!("{prefix}_cap_{capacity}_g{seq}");
                let total = crate::protocol_pubsub::pubsub_ring_file_size(capacity);
                let shm = crate::shm_file::ShmFile::create_named_secured(
                    &name, total, crate::shm_file::ShmNamespace::Session, None,
                )?;
                PubSubRing::create_from_shm(shm, capacity)?
            }
        };
        Ok(Arc::new(ring))
    }

    /// Speculatively build a backing at `capacity` into the
    /// one-slot warm cache, off the morph's critical path.
    /// The next `morph_capacity_to(capacity)` consumes it and
    /// skips allocation. Re-prewarming the cached capacity is a
    /// no-op; a different capacity replaces the slot.
    pub fn prewarm(&self, capacity: usize) -> Result<(), PubSubCapacityMorphError> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        if self.warm.holds(&capacity) {
            return Ok(());
        }
        let ring = self.build_backing(capacity)?;
        self.warm.store(capacity, ring);
        Ok(())
    }

    /// Capacity currently held in the warm cache, if any.
    pub fn warm_capacity(&self) -> Option<usize> {
        self.warm.key()
    }

    /// Number of morphs that consumed a warm-cache prediction.
    pub fn warm_hits(&self) -> u64 {
        self.warm_hits.load(Ordering::Relaxed)
    }

    /// Drop any cached prediction, releasing its memory (and its
    /// file / shm region for non-anon locales).
    pub fn clear_warm(&self) {
        self.warm.clear();
    }

    /// Garbage-collect stale backings from the front of the
    /// chain: moves the oldest kept backing forward past every
    /// backing that nothing but the chain holds, so no subscriber
    /// reads it. The active backing is never passed. Returns the
    /// number of stale backings reclaimed.
    pub fn gc(&self) -> usize {
        let mut reclaimed = 0;
        loop {
            let oldest = self.oldest.load_full();
            let Some(next) = oldest.next.load_full() else {
                return reclaimed;
            };
            // Held by the wrapper and this call alone: no subscriber
            // reads it, and none reaches it through an older backing.
            if Arc::strong_count(&oldest) > 2 {
                return reclaimed;
            }
            if self.oldest.compare_and_set(&oldest, next).is_ok() {
                reclaimed += 1;
            }
        }
    }

    /// Direct access to the currently-active [`PubSubRing`].
    pub fn ring_handle(&self) -> Arc<PubSubRing> {
        Arc::clone(&self.active.load().ring)
    }

    /// Number of backings currently in the chain (active + any
    /// not-yet-gc'd stale entries).
    pub fn chain_len(&self) -> usize {
        let oldest = self.oldest.load().generation;
        let active = self.active.load().generation;
        (active.saturating_sub(oldest) + 1) as usize
    }

    /// Sum of capacities across every backing currently in the
    /// chain. Used by KeepAll-style producers to bound in-flight
    /// items to actual buffering room: any item the producer
    /// publishes is held in some backing until the slowest
    /// subscriber catches up; with at most `chain_total_capacity()`
    /// in-flight items, no backing wraps past a subscriber's
    /// position before that subscriber drains it.
    pub fn chain_total_capacity(&self) -> usize {
        let mut backing = self.oldest.load_full();
        let active = self.active.load_full();
        let mut total = 0;
        loop {
            total += backing.ring.capacity();
            match backing.next.load_full() {
                Some(next) => backing = next,
                None => break,
            }
        }
        if backing.generation < active.generation {
            // A morph has made `active` the target and not yet linked it
            // behind the backing the walk ended on.
            total += active.ring.capacity();
        }
        total
    }
}

/// Subscriber-side handle to a [`CapacityPubSubRing`]. Holds the
/// backing it reads and its position within that backing, and
/// advances through the chain as it catches up.
pub struct CapacityPubSubSubscriber {
    backing: Arc<Backing>,
    position: u64,
}

impl CapacityPubSubSubscriber {
    /// Try to read the next payload. On `Ok`, the subscriber
    /// advances its position by 1. On `Pending` at a stale
    /// backing's head, transparently advances to the next backing
    /// in the chain and retries; on `Pending` at the active
    /// backing's head, returns `Pending` (no more data right
    /// now).
    pub fn try_next(&mut self, out: &mut [u8]) -> Result<(), PubSubReadError> {
        loop {
            // A successor is linked only once every publish into this
            // backing has returned, so with one seen before the read, an
            // empty read has drained this backing for good.
            let next = self.backing.next.load_full();
            match self.backing.ring.read_at(self.position, out) {
                Ok(()) => {
                    self.position += 1;
                    return Ok(());
                }
                Err(PubSubReadError::Pending) => match next {
                    Some(next) => {
                        self.backing = next;
                        self.position = 0;
                    }
                    None => return Err(PubSubReadError::Pending),
                },
                err => return err,
            }
        }
    }

    /// The generation of the backing this subscriber reads: 0 for the
    /// ring's first backing, one more for each morph after it.
    pub fn backing_idx(&self) -> u64 { self.backing.generation }

    /// Current position within the current backing.
    pub fn position(&self) -> u64 { self.position }
}

/// Compose the per-morph pubsub file path.
fn path_for_capacity_seq(base: &Path, capacity: usize, seq: u64) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".cap_{capacity}_g{seq}.bin"));
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(out: &[u8]) -> u64 {
        u64::from_le_bytes(out[..8].try_into().expect("an 8-byte item"))
    }

    #[test]
    fn prewarm_hit_consumes_cache_and_subscribers_cross_chain() {
        let ring = CapacityPubSubRing::create_anon(64).unwrap();
        let mut sub = ring.subscribe_from_oldest();
        ring.publish(&7u64.to_le_bytes());

        ring.prewarm(256).unwrap();
        assert_eq!(ring.warm_capacity(), Some(256));
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.warm_hits(), 1, "morph must consume the prediction");
        assert_eq!(ring.warm_capacity(), None, "the slot is one-shot");
        assert_eq!(ring.current_capacity(), 256);
        assert_eq!(ring.chain_len(), 2);

        // Published-pre-morph item reads from the stale chain
        // entry; a post-morph publish lands on the warm backing
        // and the subscriber crosses into it.
        ring.publish(&9u64.to_le_bytes());
        let mut out = [0u8; 64];
        sub.try_next(&mut out).unwrap();
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);
        sub.try_next(&mut out).unwrap();
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 9);
    }

    #[test]
    fn prewarm_mismatch_stays_cached() {
        let ring = CapacityPubSubRing::create_anon(64).unwrap();
        ring.prewarm(512).unwrap();
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.warm_hits(), 0);
        assert_eq!(ring.warm_capacity(), Some(512));
        ring.morph_capacity_to(512).unwrap();
        assert_eq!(ring.warm_hits(), 1);
        assert_eq!(ring.warm_capacity(), None);
    }

    #[test]
    fn prewarm_rejects_non_pow2_and_clear_drops() {
        let ring = CapacityPubSubRing::create_anon(64).unwrap();
        assert!(matches!(
            ring.prewarm(100),
            Err(PubSubCapacityMorphError::InvalidCapacity)
        ));
        ring.prewarm(128).unwrap();
        ring.clear_warm();
        assert_eq!(ring.warm_capacity(), None);
    }

    /// A subscriber partway through the old backing keeps it through a
    /// gc, reads the rest of it, and crosses into the new one in order.
    #[test]
    fn gc_keeps_a_backing_a_subscriber_has_not_drained() {
        let ring = CapacityPubSubRing::create_anon(4).unwrap();
        let mut sub = ring.subscribe_from_oldest();
        ring.publish(&1u64.to_le_bytes());
        ring.publish(&2u64.to_le_bytes());
        ring.morph_capacity_to(8).unwrap();
        ring.publish(&3u64.to_le_bytes());
        let mut out = [0u8; 64];
        sub.try_next(&mut out).expect("the first item");
        assert_eq!(item(&out), 1);
        assert_eq!(ring.gc(), 0, "the subscriber still reads the old backing");
        for want in [2u64, 3] {
            sub.try_next(&mut out).expect("the subscriber reads on in order");
            assert_eq!(item(&out), want);
        }
    }

    /// A subscriber on the new backing keeps its place when gc reclaims
    /// the drained backing behind it.
    #[test]
    fn a_subscriber_keeps_its_place_when_gc_reclaims_behind_it() {
        let ring = CapacityPubSubRing::create_anon(4).unwrap();
        let mut sub = ring.subscribe_from_oldest();
        ring.publish(&1u64.to_le_bytes());
        ring.morph_capacity_to(8).unwrap();
        ring.publish(&2u64.to_le_bytes());
        let mut out = [0u8; 64];
        for want in [1u64, 2] {
            sub.try_next(&mut out).expect("the items so far");
            assert_eq!(item(&out), want);
        }
        assert_eq!(ring.gc(), 1, "the drained old backing is reclaimed");
        ring.publish(&3u64.to_le_bytes());
        sub.try_next(&mut out).expect("the subscriber reads what follows");
        assert_eq!(item(&out), 3);
    }
}
