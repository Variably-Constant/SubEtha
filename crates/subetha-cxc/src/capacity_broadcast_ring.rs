//! `CapacityBroadcastRing`: runtime-resizable wrapper around
//! [`SharedBroadcastRing`] that adds the capacity-axis morph to
//! the broadcast (1P/NC fan-out) primitive.
//!
//! Sibling of [`CapacityAdaptiveRing`](crate::CapacityAdaptiveRing)
//! which morphs the SPSC / MPSC / MPMC / Vyukov family.
//! `CapacityBroadcastRing` morphs SharedBroadcastRing's slot count
//! at runtime under the same stale-list invariant: producers only
//! ever write to the active backing; subscribers walk the stale
//! list oldest-first before falling through to active, so the
//! per-subscriber position state baked into each
//! `SharedBroadcastRing` continues to advance through the stale
//! ring until that backing is fully drained by every subscriber.
//!
//! # Per-subscriber position tracking
//!
//! Broadcast's distinguishing property: every registered consumer
//! reads every slot independently. Each `SharedBroadcastRing`
//! already tracks per-consumer positions in its header
//! (`consumer_seqs[MAX_CONSUMERS]`), so the per-stale-ring
//! position tracker the capacity-morph wrapper needs is provided
//! by the underlying primitive at zero extra cost. Consumers walk
//! every stale backing at their own pace; a stale entry is
//! reclaimed when [`SharedBroadcastRing::is_fully_drained`]
//! returns true (every active consumer's seq has caught up to the
//! frozen producer seq).
//!
//! # Consumer registration model
//!
//! The wrapper tracks a monotonic consumer count (`n_consumers`)
//! and mirrors that many `register_consumer()` calls onto each
//! new backing at morph time. Consumers register in
//! `0..n_consumers` order and never unregister - this matches the
//! capacity-morph use case (subscribers join, capacity grows /
//! shrinks under load, no subscriber churn). A future iteration
//! could carry an explicit per-slot bitmap to support
//! unregister-and-rejoin without renumbering, but the simple
//! grow-only model fits the in-scope tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::shared_broadcast_ring::{BroadcastError, SharedBroadcastRing};

/// Errors returned by capacity-morph operations on a broadcast
/// ring.
#[derive(Debug)]
pub enum BroadcastCapacityMorphError {
    /// Target capacity is not a power of two, or less than 2.
    InvalidCapacity,
    /// Underlying broadcast ring allocation failed during the
    /// morph. The active backing is unchanged.
    Broadcast(BroadcastError),
    /// I/O error during file / shmfs backing creation.
    Io(std::io::Error),
}

impl From<BroadcastError> for BroadcastCapacityMorphError {
    fn from(e: BroadcastError) -> Self { Self::Broadcast(e) }
}

impl From<std::io::Error> for BroadcastCapacityMorphError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}

impl std::fmt::Display for BroadcastCapacityMorphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCapacity => write!(f, "capacity must be pow2 >= 2"),
            Self::Broadcast(e) => write!(f, "broadcast ring error during morph: {e:?}"),
            Self::Io(e) => write!(f, "io error during morph: {e}"),
        }
    }
}

impl std::error::Error for BroadcastCapacityMorphError {}

/// Runtime-resizable broadcast ring. See module-level docs for
/// the morph protocol. Hot-path `try_push` / `try_recv` perform
/// one ArcSwap load (~5-10 ns) and delegate to the active backing
/// (try_push) or walk the snapshot's stale list then fall through
/// to active (try_recv). No mutex acquisition on the steady-state
/// path; the morph swaps a fresh `BroadcastRingState` atomically.
pub struct CapacityBroadcastRing {
    /// Combined active + stale list behind a single ArcSwap. The
    /// snapshot's atomicity gives FIFO-correct combined view
    /// across a morph; producers go straight to `state.active`,
    /// subscribers walk `state.stale` then fall through.
    state: ArcSwap<BroadcastRingState>,
    /// Bumped on every successful morph for caller-polled pin
    /// invalidation.
    pin_generation: AtomicU64,
    /// Cached observable capacity; tracks the active backing.
    capacity_atom: AtomicU64,
    /// Locale source for morph-allocated backings.
    backing_source: BroadcastBackingSource,
    /// Monotonic morph counter. Used to disambiguate file paths /
    /// shm names so morphs cycling through the same capacity do
    /// not collide on the prior backing's name.
    morph_seq: AtomicU64,
    /// Monotonic count of consumers registered against the wrapper.
    /// The morph mirrors this many registrations onto each new
    /// backing in order so consumer_idx assignments stay in
    /// lockstep across morphs. Grow-only by design.
    n_consumers: AtomicU64,
    /// Serialises concurrent `morph_capacity_to` callers.
    morph_lock: Mutex<()>,
    /// One-slot warm cache: a fully constructed backing at a
    /// predicted capacity, built off the morph lock by
    /// [`prewarm`](Self::prewarm). Same design as
    /// `CapacityAdaptiveRing`'s warm cache.
    warm: Mutex<Option<(usize, Arc<SharedBroadcastRing>)>>,
    /// Successful warm-cache hits consumed by `morph_capacity_to`.
    warm_hits: AtomicU64,
}

unsafe impl Send for CapacityBroadcastRing {}
unsafe impl Sync for CapacityBroadcastRing {}

/// Atomic snapshot of the broadcast ring's active backing +
/// stale list. Same shape as `CapacityAdaptiveRing::RingState`;
/// the wrapper's hot path performs one ArcSwap load to capture
/// both simultaneously, eliminating the mutex acquisition that
/// the prior design needed for FIFO-correctness combined-snapshot.
struct BroadcastRingState {
    active: Arc<SharedBroadcastRing>,
    stale: Vec<Arc<SharedBroadcastRing>>,
}

/// Locale source for capacity-morph-allocated broadcast backings.
enum BroadcastBackingSource {
    Anon,
    File(PathBuf),
    Shm(String),
}

impl CapacityBroadcastRing {
    /// Anon (in-process) capacity-adaptive broadcast ring.
    pub fn create_anon(
        initial_capacity: usize,
    ) -> Result<Self, BroadcastCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
        }
        let ring = SharedBroadcastRing::create_anon(initial_capacity)?;
        Ok(Self {
            state: ArcSwap::from(Arc::new(BroadcastRingState {
                active: Arc::new(ring),
                stale: Vec::new(),
            })),
            pin_generation: AtomicU64::new(0),
            capacity_atom: AtomicU64::new(initial_capacity as u64),
            backing_source: BroadcastBackingSource::Anon,
            morph_seq: AtomicU64::new(0),
            n_consumers: AtomicU64::new(0),
            morph_lock: Mutex::new(()),
            warm: Mutex::new(None),
            warm_hits: AtomicU64::new(0),
        })
    }

    /// File-backed capacity-adaptive broadcast ring. New backings
    /// allocate at `{base}.cap_{N}_g{morph_seq}.bin`.
    pub fn create(
        base_path: impl AsRef<Path>,
        initial_capacity: usize,
    ) -> Result<Self, BroadcastCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
        }
        let base = base_path.as_ref().to_path_buf();
        let path = path_for_capacity_seq(&base, initial_capacity, 0);
        let ring = SharedBroadcastRing::create(&path, initial_capacity)?;
        Ok(Self {
            state: ArcSwap::from(Arc::new(BroadcastRingState {
                active: Arc::new(ring),
                stale: Vec::new(),
            })),
            pin_generation: AtomicU64::new(0),
            capacity_atom: AtomicU64::new(initial_capacity as u64),
            backing_source: BroadcastBackingSource::File(base),
            morph_seq: AtomicU64::new(1),
            n_consumers: AtomicU64::new(0),
            morph_lock: Mutex::new(()),
            warm: Mutex::new(None),
            warm_hits: AtomicU64::new(0),
        })
    }

    /// ShmFs (named shared memory) capacity-adaptive broadcast
    /// ring. New backings allocate at `{prefix}_cap_{N}_g{seq}`.
    pub fn create_shmfs(
        name_prefix: &str,
        initial_capacity: usize,
    ) -> Result<Self, BroadcastCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
        }
        let name = format!("{name_prefix}_cap_{initial_capacity}_g0");
        let total = crate::shared_broadcast_ring::broadcast_file_size(initial_capacity);
        let shm = crate::shm_file::ShmFile::create_or_open_named(&name, total)?;
        let ring = SharedBroadcastRing::create_from_shm(shm, initial_capacity)?;
        Ok(Self {
            state: ArcSwap::from(Arc::new(BroadcastRingState {
                active: Arc::new(ring),
                stale: Vec::new(),
            })),
            pin_generation: AtomicU64::new(0),
            capacity_atom: AtomicU64::new(initial_capacity as u64),
            backing_source: BroadcastBackingSource::Shm(name_prefix.to_owned()),
            morph_seq: AtomicU64::new(1),
            n_consumers: AtomicU64::new(0),
            morph_lock: Mutex::new(()),
            warm: Mutex::new(None),
            warm_hits: AtomicU64::new(0),
        })
    }

    /// Current capacity of the active backing.
    pub fn current_capacity(&self) -> usize {
        self.capacity_atom.load(Ordering::Acquire) as usize
    }

    /// Current pin generation.
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Register a consumer against the active backing. Returns
    /// the assigned consumer_idx (matches what the active backing
    /// itself returned). The wrapper tracks the count so each
    /// subsequent morph mirrors the same number of registrations
    /// in order onto the new backing. Consumers join "from now"
    /// against existing stale backings: they get no slot there,
    /// so try_recv against those backings returns InvalidConsumer
    /// which is treated as Empty by the wrapper's stale-walk.
    pub fn register_consumer(&self) -> Result<usize, BroadcastError> {
        let idx = self.state.load().active.register_consumer()?;
        self.n_consumers.fetch_add(1, Ordering::AcqRel);
        Ok(idx)
    }

    /// Hot-path push. One ArcSwap load + the active backing's
    /// native `try_push`.
    #[inline]
    pub fn try_push(&self, payload: &[u8]) -> Result<(), BroadcastError> {
        self.state.load().active.try_push(payload)
    }

    /// Hot-path recv. Walks the stale list oldest-first; falls
    /// through to active when every stale entry returns empty or
    /// the consumer has no slot in that stale.
    ///
    /// FIFO ordering invariant: one ArcSwap load gives a
    /// consistent snapshot of BOTH stale and active. A concurrent
    /// morph either fully precedes or fully follows this load -
    /// it never slips between two separate observations.
    #[inline]
    pub fn try_recv(
        &self,
        consumer_idx: usize,
        out: &mut [u8],
    ) -> Result<usize, BroadcastError> {
        // Per-stale-ring spin discipline (FIFO correctness):
        // see CapacityAdaptiveRing::try_recv for the full rationale.
        // For broadcast specifically: SharedBroadcastRing.try_recv
        // returns `Err(Empty)` when this consumer's seq >= producer
        // seq, but a producer mid-write under the SeqLock is also
        // observable to the consumer as `Empty` until the version
        // commits. `is_fully_drained()` checks whether every active
        // consumer's seq has caught up to producer_seq - the
        // "no mid-claim" condition - so spin until either we get
        // an item or the stale ring is fully drained for this
        // consumer.
        let state = self.state.load();
        for ring in &state.stale {
            loop {
                match ring.try_recv(consumer_idx, out) {
                    Ok(n) => return Ok(n),
                    Err(_) => {
                        if ring.lag(consumer_idx) == 0 {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
        }
        state.active.try_recv(consumer_idx, out)
    }

    /// Morph the broadcast ring's capacity to `new_capacity`.
    pub fn morph_capacity_to(
        &self,
        new_capacity: usize,
    ) -> Result<(), BroadcastCapacityMorphError> {
        let _morph_guard = self.morph_lock.lock();

        if !new_capacity.is_power_of_two() || new_capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
        }

        let old_state = self.state.load_full();
        let old = Arc::clone(&old_state.active);
        let old_capacity = self.capacity_atom.load(Ordering::Acquire) as usize;
        if old_capacity == new_capacity {
            return Ok(());
        }

        // Warm-cache probe: a prediction matching the morph target
        // skips allocation entirely; a mismatch stays cached and
        // the cold path runs unchanged.
        let warm_hit = {
            let mut warm = self.warm.lock();
            warm.take_if(|(cap, _)| *cap == new_capacity)
        };
        let new = match warm_hit {
            Some((_, ring)) => {
                self.warm_hits.fetch_add(1, Ordering::Relaxed);
                ring
            }
            None => self.build_backing(new_capacity)?,
        };

        // Mirror n_consumers registrations onto the new backing
        // in order so consumer_idx assignments stay in lockstep.
        // Surfacing failures (NoConsumerSlot) loudly via ? so
        // the morph fails fast if the new backing is undersized.
        let n = self.n_consumers.load(Ordering::Acquire) as usize;
        for _ in 0..n {
            new.register_consumer()?;
        }

        self.pin_generation.fetch_add(1, Ordering::AcqRel);

        // Build the new state in one shot: prune fully-drained
        // stale entries, append the prior active, publish
        // atomically. Subscribers reading via `self.state.load()`
        // see either the pre-morph snapshot or the post-morph
        // snapshot, never a half-state.
        let mut new_stale: Vec<Arc<SharedBroadcastRing>> = old_state
            .stale
            .iter()
            .filter(|r| !r.is_fully_drained())
            .cloned()
            .collect();
        new_stale.push(old);
        let new_state = BroadcastRingState { active: new, stale: new_stale };
        self.state.store(Arc::new(new_state));
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
    ) -> Result<Arc<SharedBroadcastRing>, BroadcastCapacityMorphError> {
        let seq = self.morph_seq.fetch_add(1, Ordering::AcqRel);
        let ring = match &self.backing_source {
            BroadcastBackingSource::Anon => {
                SharedBroadcastRing::create_anon(capacity)?
            }
            BroadcastBackingSource::File(base) => {
                let path = path_for_capacity_seq(base, capacity, seq);
                SharedBroadcastRing::create(&path, capacity)?
            }
            BroadcastBackingSource::Shm(prefix) => {
                let name = format!("{prefix}_cap_{capacity}_g{seq}");
                let total = crate::shared_broadcast_ring::broadcast_file_size(capacity);
                let shm = crate::shm_file::ShmFile::create_or_open_named(&name, total)?;
                SharedBroadcastRing::create_from_shm(shm, capacity)?
            }
        };
        Ok(Arc::new(ring))
    }

    /// Speculatively build a backing at `capacity` into the
    /// one-slot warm cache, off the morph lock's critical path.
    /// The next `morph_capacity_to(capacity)` consumes it and
    /// skips allocation. Re-prewarming the cached capacity is a
    /// no-op; a different capacity replaces the slot.
    pub fn prewarm(&self, capacity: usize) -> Result<(), BroadcastCapacityMorphError> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
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

    /// Pin the current capacity backing for a hot loop.
    pub fn pin_current_capacity(&self) -> PinnedBroadcastCapacity<'_> {
        let captured_gen = self.pin_generation.load(Ordering::Acquire);
        let ring = Arc::clone(&self.state.load().active);
        let capacity = self.capacity_atom.load(Ordering::Acquire) as usize;
        PinnedBroadcastCapacity {
            parent: self,
            pinned_generation: captured_gen,
            ring,
            capacity,
            _not_sync: std::marker::PhantomData,
        }
    }

    /// Direct access to the active [`SharedBroadcastRing`].
    pub fn ring_handle(&self) -> Arc<SharedBroadcastRing> {
        Arc::clone(&self.state.load().active)
    }
}

/// Pinned snapshot of a `CapacityBroadcastRing`'s current capacity
/// backing.
pub struct PinnedBroadcastCapacity<'a> {
    parent: &'a CapacityBroadcastRing,
    pinned_generation: u64,
    ring: Arc<SharedBroadcastRing>,
    capacity: usize,
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl<'a> PinnedBroadcastCapacity<'a> {
    pub fn is_still_valid(&self) -> bool {
        self.parent.pin_generation.load(Ordering::Acquire) == self.pinned_generation
    }
    pub fn capacity(&self) -> usize { self.capacity }
    pub fn generation(&self) -> u64 { self.pinned_generation }
    pub fn ring(&self) -> &Arc<SharedBroadcastRing> { &self.ring }
}

/// Compose the per-morph broadcast file path.
fn path_for_capacity_seq(base: &Path, capacity: usize, seq: u64) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".cap_{capacity}_g{seq}.bin"));
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prewarm_hit_consumes_cache_and_broadcast_works() {
        let ring = CapacityBroadcastRing::create_anon(64).unwrap();
        let idx = ring.register_consumer().unwrap();
        ring.try_push(&7u64.to_le_bytes()).unwrap();

        ring.prewarm(256).unwrap();
        assert_eq!(ring.warm_capacity(), Some(256));
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.warm_hits(), 1, "morph must consume the prediction");
        assert_eq!(ring.warm_capacity(), None, "the slot is one-shot");
        assert_eq!(ring.current_capacity(), 256);

        // In-flight item drains from the stale backing; a fresh
        // push lands on the warm backing and drains too.
        ring.try_push(&9u64.to_le_bytes()).unwrap();
        let mut out = [0u8; 64];
        let n = ring.try_recv(idx, &mut out).unwrap();
        assert!(n >= 8);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);
        let n = ring.try_recv(idx, &mut out).unwrap();
        assert!(n >= 8);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 9);
    }

    #[test]
    fn prewarm_mismatch_stays_cached() {
        let ring = CapacityBroadcastRing::create_anon(64).unwrap();
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
        let ring = CapacityBroadcastRing::create_anon(64).unwrap();
        assert!(matches!(
            ring.prewarm(100),
            Err(BroadcastCapacityMorphError::InvalidCapacity)
        ));
        ring.prewarm(128).unwrap();
        ring.clear_warm();
        assert_eq!(ring.warm_capacity(), None);
    }
}
