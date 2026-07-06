//! `CapacityPubSubRing`: runtime-resizable wrapper around
//! [`PubSubRing`] that adds the capacity-axis morph to the
//! pub/sub (1P/NC absolute-position) primitive.
//!
//! Sibling of [`CapacityAdaptiveRing`](crate::CapacityAdaptiveRing)
//! and [`CapacityBroadcastRing`](crate::CapacityBroadcastRing).
//! `CapacityPubSubRing` morphs PubSubRing's slot count at runtime
//! under a chain-of-backings invariant: the producer publishes to
//! the most-recent backing; subscribers carry their own
//! (backing_idx, position) state and drain each backing in turn
//! before advancing to the next.
//!
//! # Per-subscriber position tracking
//!
//! Pub/sub's per-subscriber position state already lives outside
//! the ring (in `PubSubSubscriber::position` /
//! [`SubscriberPosition`](crate::replay_positions::SubscriberPosition)),
//! so the capacity-morph wrapper threads each subscriber through
//! the chain of historical backings as the active one rolls
//! forward. A [`CapacityPubSubSubscriber`] holds:
//!
//! - `cap_ring: Arc<CapacityPubSubRing>` to see the chain
//! - `backing_idx: u64` - which backing in the chain we are
//!   currently reading from
//! - `position: u64` - position within `backings[backing_idx]`
//!
//! On `try_next()`, the subscriber reads at its current
//! `(backing_idx, position)`. On `Pending` AND when not on the
//! most-recent backing, it advances `backing_idx` and resets
//! `position` to 0 (every stale backing's prior content drains
//! before the subscriber crosses into the next).
//!
//! # Chain pruning
//!
//! Chain entries grow append-only across morphs. A separate
//! `gc()` method walks the chain and drops the oldest entries
//! whose strong count is 1 (only the chain itself holds the
//! Arc) - i.e. no subscriber is currently reading from them. The
//! producer continues publishing to the active end of the chain
//! during gc; the morph guard ensures gc and morph are mutually
//! exclusive.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::protocol_pubsub::{PubSubReadError, PubSubRing};

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

/// Runtime-resizable pubsub ring.
pub struct CapacityPubSubRing {
    /// Chain of all backings ever allocated, oldest-first. The
    /// active backing is always at the end (index chain.len() -
    /// 1). Append-only; pruning happens via [`gc`](Self::gc).
    chain: Mutex<Vec<Arc<PubSubRing>>>,
    /// Cached observable capacity of the active backing.
    capacity_atom: AtomicU64,
    /// Bumped on every morph for caller-polled pin invalidation.
    pin_generation: AtomicU64,
    /// Locale source for morph-allocated backings.
    backing_source: PubSubBackingSource,
    /// Monotonic morph counter for path / shm-name uniqueness.
    morph_seq: AtomicU64,
    /// Serialises morph callers (and gc) so the chain mutations
    /// are atomic with respect to each other.
    morph_lock: Mutex<()>,
    /// One-slot warm cache: a fully constructed backing at a
    /// predicted capacity, built off the morph lock by
    /// [`prewarm`](Self::prewarm). Same design as
    /// `CapacityAdaptiveRing`'s warm cache.
    warm: Mutex<Option<(usize, Arc<PubSubRing>)>>,
    /// Successful warm-cache hits consumed by `morph_capacity_to`.
    warm_hits: AtomicU64,
}

unsafe impl Send for CapacityPubSubRing {}
unsafe impl Sync for CapacityPubSubRing {}

enum PubSubBackingSource {
    Anon,
    File(PathBuf),
    Shm(String),
}

impl CapacityPubSubRing {
    /// Anon (in-process) capacity-adaptive pubsub ring.
    pub fn create_anon(
        initial_capacity: usize,
    ) -> Result<Arc<Self>, PubSubCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        let ring = PubSubRing::create_anon(initial_capacity)?;
        Ok(Arc::new(Self {
            chain: Mutex::new(vec![Arc::new(ring)]),
            capacity_atom: AtomicU64::new(initial_capacity as u64),
            pin_generation: AtomicU64::new(0),
            backing_source: PubSubBackingSource::Anon,
            morph_seq: AtomicU64::new(0),
            morph_lock: Mutex::new(()),
            warm: Mutex::new(None),
            warm_hits: AtomicU64::new(0),
        }))
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
        Ok(Arc::new(Self {
            chain: Mutex::new(vec![Arc::new(ring)]),
            capacity_atom: AtomicU64::new(initial_capacity as u64),
            pin_generation: AtomicU64::new(0),
            backing_source: PubSubBackingSource::File(base),
            morph_seq: AtomicU64::new(1),
            morph_lock: Mutex::new(()),
            warm: Mutex::new(None),
            warm_hits: AtomicU64::new(0),
        }))
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
        let shm = crate::shm_file::ShmFile::create_or_open_named(&name, total)?;
        let ring = PubSubRing::create_from_shm(shm, initial_capacity)?;
        Ok(Arc::new(Self {
            chain: Mutex::new(vec![Arc::new(ring)]),
            capacity_atom: AtomicU64::new(initial_capacity as u64),
            pin_generation: AtomicU64::new(0),
            backing_source: PubSubBackingSource::Shm(name_prefix.to_owned()),
            morph_seq: AtomicU64::new(1),
            morph_lock: Mutex::new(()),
            warm: Mutex::new(None),
            warm_hits: AtomicU64::new(0),
        }))
    }

    /// Current capacity of the active backing.
    pub fn current_capacity(&self) -> usize {
        self.capacity_atom.load(Ordering::Acquire) as usize
    }

    /// Current pin generation.
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Publish a payload to the currently-active backing. Returns
    /// the absolute position assigned within that backing (not
    /// globally unique across backings - subscribers identify
    /// items via payload contents, not position).
    ///
    /// The chain lock is held through the inner publish call so a
    /// concurrent morph cannot slip in and make this publish land
    /// in a backing that just became stale. Subscribers walk the
    /// chain oldest-to-newest and only advance forward; if a
    /// publish landed in a now-stale backing past where any
    /// subscriber had already advanced, those items would be
    /// silently lost. Holding the lock through publish prevents
    /// that.
    pub fn publish(&self, payload: &[u8]) -> u64 {
        let chain = self.chain.lock();
        chain.last().expect("chain always has at least one backing").publish(payload)
    }

    /// Subscribe to the stream from the CURRENT active backing's
    /// current head. The subscriber drains forward from there,
    /// crossing into newly-morphed backings as it catches up.
    /// "From now" semantics: late joiners do NOT see history
    /// from before they subscribed.
    pub fn subscribe_from_now(self: &Arc<Self>) -> CapacityPubSubSubscriber {
        let chain = self.chain.lock();
        let backing_idx = (chain.len() - 1) as u64;
        let active = &chain[backing_idx as usize];
        let position = active.head();
        drop(chain);
        CapacityPubSubSubscriber {
            cap_ring: Arc::clone(self),
            backing_idx,
            position,
        }
    }

    /// Subscribe starting from the beginning of the OLDEST
    /// backing currently in the chain. The subscriber drains
    /// every item from every backing oldest-to-newest, crossing
    /// chain entries as it catches up. Used when a subscriber
    /// needs to replay the full available history.
    pub fn subscribe_from_oldest(self: &Arc<Self>) -> CapacityPubSubSubscriber {
        CapacityPubSubSubscriber {
            cap_ring: Arc::clone(self),
            backing_idx: 0,
            position: 0,
        }
    }

    /// Morph the active backing's capacity. Allocates a fresh
    /// backing at `new_capacity`, appends it to the chain,
    /// bumps pin_generation, and publishes the new active end.
    /// Subscribers reading from older chain entries continue
    /// undisturbed; they advance into the new backing
    /// individually as their try_next catches up.
    pub fn morph_capacity_to(
        &self,
        new_capacity: usize,
    ) -> Result<(), PubSubCapacityMorphError> {
        let _morph_guard = self.morph_lock.lock();

        if !new_capacity.is_power_of_two() || new_capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }

        let current = self.capacity_atom.load(Ordering::Acquire) as usize;
        if current == new_capacity {
            return Ok(());
        }

        // Warm-cache probe: a prediction matching the morph target
        // skips allocation entirely; a mismatch stays cached and
        // the cold path runs unchanged.
        let warm_hit = {
            let mut warm = self.warm.lock();
            warm.take_if(|(cap, _)| *cap == new_capacity)
        };
        let new_ring = match warm_hit {
            Some((_, ring)) => {
                self.warm_hits.fetch_add(1, Ordering::Relaxed);
                ring
            }
            None => self.build_backing(new_capacity)?,
        };

        {
            let mut chain = self.chain.lock();
            chain.push(new_ring);
        }
        self.pin_generation.fetch_add(1, Ordering::AcqRel);
        self.capacity_atom
            .store(new_capacity as u64, Ordering::Release);

        Ok(())
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
                let shm = crate::shm_file::ShmFile::create_or_open_named(&name, total)?;
                PubSubRing::create_from_shm(shm, capacity)?
            }
        };
        Ok(Arc::new(ring))
    }

    /// Speculatively build a backing at `capacity` into the
    /// one-slot warm cache, off the morph lock's critical path.
    /// The next `morph_capacity_to(capacity)` consumes it and
    /// skips allocation. Re-prewarming the cached capacity is a
    /// no-op; a different capacity replaces the slot.
    pub fn prewarm(&self, capacity: usize) -> Result<(), PubSubCapacityMorphError> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PubSubCapacityMorphError::InvalidCapacity);
        }
        if self.warm.lock().as_ref().map(|(c, _)| *c) == Some(capacity) {
            return Ok(());
        }
        let ring = self.build_backing(capacity)?;
        *self.warm.lock() = Some((capacity, ring));
        Ok(())
    }

    /// Capacity currently held in the warm cache, if any.
    pub fn warm_capacity(&self) -> Option<usize> {
        self.warm.lock().as_ref().map(|(c, _)| *c)
    }

    /// Number of morphs that consumed a warm-cache prediction.
    pub fn warm_hits(&self) -> u64 {
        self.warm_hits.load(Ordering::Relaxed)
    }

    /// Drop any cached prediction, releasing its memory (and its
    /// file / shm region for non-anon locales).
    pub fn clear_warm(&self) {
        *self.warm.lock() = None;
    }

    /// Garbage-collect stale backings from the front of the
    /// chain. Drops the oldest contiguous run of backings whose
    /// strong-count is 1 (only the chain itself holds them; no
    /// subscriber is currently reading them). The active backing
    /// is never dropped even when it has strong-count 1 - that
    /// would lose the producer's target. Returns the number of
    /// stale backings reclaimed.
    pub fn gc(&self) -> usize {
        let _morph_guard = self.morph_lock.lock();
        let mut chain = self.chain.lock();
        let mut reclaimed = 0;
        while chain.len() > 1 && Arc::strong_count(&chain[0]) == 1 {
            chain.remove(0);
            reclaimed += 1;
        }
        reclaimed
    }

    /// Direct access to the currently-active [`PubSubRing`].
    pub fn ring_handle(&self) -> Arc<PubSubRing> {
        let chain = self.chain.lock();
        chain.last().expect("chain non-empty").clone()
    }

    /// Number of backings currently in the chain (active + any
    /// not-yet-gc'd stale entries).
    pub fn chain_len(&self) -> usize {
        self.chain.lock().len()
    }

    /// Sum of capacities across every backing currently in the
    /// chain. Used by KeepAll-style producers to bound in-flight
    /// items to actual buffering room: any item the producer
    /// publishes is held in SOME backing until the slowest
    /// subscriber catches up; with at most `chain_total_capacity()`
    /// in-flight items, no backing wraps past a subscriber's
    /// position before that subscriber drains it.
    pub fn chain_total_capacity(&self) -> usize {
        let chain = self.chain.lock();
        chain.iter().map(|r| r.capacity()).sum()
    }
}

/// Subscriber-side handle to a [`CapacityPubSubRing`]. Holds its
/// own backing_idx + position within that backing and advances
/// through the chain as it catches up.
pub struct CapacityPubSubSubscriber {
    cap_ring: Arc<CapacityPubSubRing>,
    backing_idx: u64,
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
            let (backing, is_latest) = {
                let chain = self.cap_ring.chain.lock();
                let len = chain.len();
                let idx = self.backing_idx as usize;
                if idx >= len {
                    return Err(PubSubReadError::Pending);
                }
                let b = Arc::clone(&chain[idx]);
                (b, idx == len - 1)
            };

            match backing.read_at(self.position, out) {
                Ok(()) => {
                    self.position += 1;
                    return Ok(());
                }
                Err(PubSubReadError::Pending) => {
                    if !is_latest {
                        // Caught up to a stale backing's head;
                        // cross into the next entry of the chain.
                        self.backing_idx += 1;
                        self.position = 0;
                        continue;
                    }
                    return Err(PubSubReadError::Pending);
                }
                err => return err,
            }
        }
    }

    /// Current backing chain index this subscriber is reading.
    pub fn backing_idx(&self) -> u64 { self.backing_idx }

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
}
