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
//! frozen producer seq) and the stale list is the entry's last
//! holder: the producer pushes into the active backing of the state
//! it loaded, so a backing a loaded snapshot still names stays on
//! the list for the push that snapshot may yet deliver.
//!
//! # Consumer registration model
//!
//! The wrapper hands out consumer indices itself, in order, from a
//! count (`n_consumers`), and consumers never unregister - this
//! matches the capacity-morph use case (subscribers join, capacity
//! grows / shrinks under load, no subscriber churn). A registrant
//! claims its index on the backing active when it registers, reading
//! from that backing's producer position on. A backing a morph makes
//! active is settled before anything is pushed into it or read from
//! it: every index handed out so far is claimed there, at its start.
//! A registrant that the settle's count missed registered after the
//! backing became active, so that backing is where it starts, and
//! each consumer reads one unbroken run of what the producer pushed.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};

use subetha_core::SwapCell;

use crate::shared_broadcast_ring::{BroadcastError, SharedBroadcastRing, MAX_CONSUMERS};
use crate::warm_slot::WarmSlot;

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
/// one SwapCell load and delegate to the active backing
/// (try_push) or walk the snapshot's stale list then fall through
/// to active (try_recv). Nothing takes a lock, the morph included:
/// it swaps a fresh `BroadcastRingState` in atomically.
pub struct CapacityBroadcastRing {
    /// Combined active + stale list behind a single SwapCell. The
    /// snapshot's atomicity gives FIFO-correct combined view
    /// across a morph; producers go straight to `state.active`,
    /// subscribers walk `state.stale` then fall through.
    state: SwapCell<BroadcastRingState>,
    /// Bumped on every successful morph for caller-polled pin
    /// invalidation.
    pin_generation: AtomicU64,
    /// Locale source for morph-allocated backings.
    backing_source: BroadcastBackingSource,
    /// Monotonic morph counter. Used to disambiguate file paths /
    /// shm names so morphs cycling through the same capacity do
    /// not collide on the prior backing's name.
    morph_seq: AtomicU64,
    /// Consumer indices handed out so far; the next registrant's index.
    /// A settle claims every index below it on the backing it settles.
    /// Grow-only by design.
    n_consumers: AtomicU64,
    /// One-slot warm cache: a fully constructed backing at a
    /// predicted capacity, built off the morph's critical path by
    /// [`prewarm`](Self::prewarm). Same design as
    /// `CapacityAdaptiveRing`'s warm cache.
    warm: WarmSlot<usize, Arc<SharedBroadcastRing>>,
    /// Successful warm-cache hits consumed by `morph_capacity_to`.
    warm_hits: AtomicU64,
}

unsafe impl Send for CapacityBroadcastRing {}
unsafe impl Sync for CapacityBroadcastRing {}

/// Atomic snapshot of the broadcast ring's active backing +
/// stale list. Same shape as `CapacityAdaptiveRing::RingState`;
/// the wrapper's hot path performs one SwapCell load to capture
/// both at once, which is what keeps a subscriber's walk
/// FIFO-correct across a morph. A morph publishes its state with a
/// compare-and-swap against the one it read.
struct BroadcastRingState {
    active: Arc<SharedBroadcastRing>,
    stale: Vec<Arc<SharedBroadcastRing>>,
    /// The active backing's capacity.
    capacity: usize,
    /// Whether every consumer index handed out before this state's
    /// first push has a slot on its active backing. False from the
    /// morph that publishes the state until the first push, read or pin
    /// on it settles it.
    settled: AtomicBool,
}

/// Locale source for capacity-morph-allocated broadcast backings.
enum BroadcastBackingSource {
    Anon,
    File(PathBuf),
    Shm(String),
}

impl CapacityBroadcastRing {
    /// The wrapper around its first backing.
    fn around(
        ring: SharedBroadcastRing,
        capacity: usize,
        backing_source: BroadcastBackingSource,
        morph_seq: u64,
    ) -> Self {
        Self {
            // The first backing has no backing before it, so every
            // registrant's own claim is its slot there.
            state: SwapCell::from_arc(Arc::new(BroadcastRingState {
                active: Arc::new(ring),
                stale: Vec::new(),
                capacity,
                settled: AtomicBool::new(true),
            })),
            pin_generation: AtomicU64::new(0),
            backing_source,
            morph_seq: AtomicU64::new(morph_seq),
            n_consumers: AtomicU64::new(0),
            warm: WarmSlot::new(),
            warm_hits: AtomicU64::new(0),
        }
    }

    /// Anon (in-process) capacity-adaptive broadcast ring.
    pub fn create_anon(
        initial_capacity: usize,
    ) -> Result<Self, BroadcastCapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
        }
        let ring = SharedBroadcastRing::create_anon(initial_capacity)?;
        Ok(Self::around(ring, initial_capacity, BroadcastBackingSource::Anon, 0))
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
        Ok(Self::around(ring, initial_capacity, BroadcastBackingSource::File(base), 1))
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
        let shm = crate::shm_file::ShmFile::create_named_secured(
            &name, total, crate::shm_file::ShmNamespace::Session, None,
        )?;
        let ring = SharedBroadcastRing::create_from_shm(shm, initial_capacity)?;
        Ok(Self::around(
            ring,
            initial_capacity,
            BroadcastBackingSource::Shm(name_prefix.to_owned()),
            1,
        ))
    }

    /// Current capacity of the active backing, read from the same
    /// state the active backing is.
    pub fn current_capacity(&self) -> usize {
        self.state.load().capacity
    }

    /// Current pin generation.
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Register a consumer and return its index, the next one the
    /// wrapper hands out. The consumer reads from the active backing's
    /// producer position on, and every backing a later morph makes
    /// active from its first item. It has no slot on the backings
    /// already stale, and the stale walk passes over them.
    pub fn register_consumer(&self) -> Result<usize, BroadcastError> {
        let idx = self.n_consumers.fetch_add(1, Ordering::SeqCst) as usize;
        if idx >= MAX_CONSUMERS {
            // Every index below the table's end is held and none is
            // given back, so no later registrant can take one.
            self.n_consumers.fetch_sub(1, Ordering::SeqCst);
            return Err(BroadcastError::NoConsumerSlot);
        }
        // Pairs with the fence in `settle`: a state published after the
        // one loaded here is settled with this index in its count.
        fence(Ordering::SeqCst);
        let state = self.state.load();
        state.active.claim_consumer_slot(idx);
        #[cfg(test)]
        crate::test_races::pause_point();
        Ok(idx)
    }

    /// Claim every index handed out so far on `state`'s active backing,
    /// once, before its first push or read. A slot a registrant
    /// already claimed there stays as it is.
    fn settle(&self, state: &BroadcastRingState) {
        if state.settled.load(Ordering::Acquire) {
            return;
        }
        // Pairs with the fence in `register_consumer`.
        fence(Ordering::SeqCst);
        let handed_out = (self.n_consumers.load(Ordering::SeqCst) as usize).min(MAX_CONSUMERS);
        for idx in 0..handed_out {
            state.active.claim_consumer_slot(idx);
        }
        state.settled.store(true, Ordering::Release);
    }

    /// Hot-path push. One SwapCell load + the active backing's
    /// native `try_push`, after the settle a new backing takes once.
    #[inline]
    pub fn try_push(&self, payload: &[u8]) -> Result<(), BroadcastError> {
        let state = self.state.load();
        self.settle(&state);
        state.active.try_push(payload)
    }

    /// Hot-path recv. Walks the stale list oldest-first; falls
    /// through to active when every stale entry returns empty or
    /// the consumer has no slot in that stale.
    ///
    /// FIFO ordering invariant: one SwapCell load gives a
    /// consistent snapshot of both stale and active. A concurrent
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
                    // Empty is the wait for an in-flight claim to commit;
                    // no lag left for this consumer ends it.
                    Err(BroadcastError::Empty) => {
                        if ring.lag(consumer_idx) == 0 {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    // A consumer that registered after this backing went
                    // stale has no slot on it, and nothing there is its.
                    Err(BroadcastError::InvalidConsumer) if consumer_idx < MAX_CONSUMERS => break,
                    Err(e) => return Err(e),
                }
            }
        }
        self.settle(&state);
        state.active.try_recv(consumer_idx, out)
    }

    /// Morph the broadcast ring's capacity to `new_capacity`.
    /// Concurrent morphs each land in turn: a morph whose state
    /// another replaced first decides again from the replacement.
    pub fn morph_capacity_to(
        &self,
        new_capacity: usize,
    ) -> Result<(), BroadcastCapacityMorphError> {
        if !new_capacity.is_power_of_two() || new_capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
        }
        loop {
            let old_state = self.state.load_full();
            if old_state.capacity == new_capacity {
                return Ok(());
            }
            let old = Arc::clone(&old_state.active);

            // Warm-cache probe: a prediction matching the morph target
            // skips allocation entirely; a mismatch stays cached and
            // the cold path runs unchanged.
            let (new, from_warm) = match self.warm.take(&new_capacity) {
                Some(ring) => (ring, true),
                None => (self.build_backing(new_capacity)?, false),
            };
            // The consumers' slots on the new backing are claimed by its
            // settle, before its first push or read, with every
            // registration up to then in the count.
            #[cfg(test)]
            crate::test_races::pause_point();

            // Build the new state in one shot: prune the stale list,
            // append the prior active, publish atomically. Subscribers
            // reading via `self.state.load()` see either the pre-morph
            // snapshot or the post-morph snapshot, never a half-state.
            //
            // A stale entry leaves the list only when every subscriber
            // has drained it and this list is its last holder. The
            // producer pushes into the active backing of the state it
            // loaded, and that load can predate this morph and the one
            // before it; its snapshot holds the state, which holds the
            // backing, so a backing some snapshot can still reach has a
            // strong count above one and stays on the list for the push
            // that has not landed yet. With this list the sole holder,
            // no push can arrive and the drain is final.
            let mut new_stale: Vec<Arc<SharedBroadcastRing>> = old_state
                .stale
                .iter()
                .filter(|r| !(r.is_fully_drained() && Arc::strong_count(r) == 1))
                .cloned()
                .collect();
            new_stale.push(old);
            let new_state = Arc::new(BroadcastRingState {
                active: new,
                stale: new_stale,
                capacity: new_capacity,
                settled: AtomicBool::new(false),
            });
            if self.state.compare_and_set(&old_state, new_state).is_ok() {
                if from_warm {
                    self.warm_hits.fetch_add(1, Ordering::Relaxed);
                }
                self.pin_generation.fetch_add(1, Ordering::AcqRel);
                return Ok(());
            }
            // Another morph replaced the state first. The backing built
            // here was registered for a state that is gone, so it drops,
            // and the next pass decides again from the state in place.
        }
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
                let shm = crate::shm_file::ShmFile::create_named_secured(
                    &name, total, crate::shm_file::ShmNamespace::Session, None,
                )?;
                SharedBroadcastRing::create_from_shm(shm, capacity)?
            }
        };
        Ok(Arc::new(ring))
    }

    /// Speculatively build a backing at `capacity` into the
    /// one-slot warm cache, off the morph's critical path.
    /// The next `morph_capacity_to(capacity)` consumes it and
    /// skips allocation. Re-prewarming the cached capacity is a
    /// no-op; a different capacity replaces the slot.
    pub fn prewarm(&self, capacity: usize) -> Result<(), BroadcastCapacityMorphError> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(BroadcastCapacityMorphError::InvalidCapacity);
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

    /// Pin the current capacity backing for a hot loop. The backing is
    /// settled first, so a push straight into it misses no consumer.
    pub fn pin_current_capacity(&self) -> PinnedBroadcastCapacity<'_> {
        let captured_gen = self.pin_generation.load(Ordering::Acquire);
        let state = self.state.load();
        self.settle(&state);
        let ring = Arc::clone(&state.active);
        let capacity = state.capacity;
        PinnedBroadcastCapacity {
            parent: self,
            pinned_generation: captured_gen,
            ring,
            capacity,
            _not_sync: std::marker::PhantomData,
        }
    }

    /// Direct access to the active [`SharedBroadcastRing`], settled
    /// first, so a push straight into it misses no consumer.
    pub fn ring_handle(&self) -> Arc<SharedBroadcastRing> {
        let state = self.state.load();
        self.settle(&state);
        Arc::clone(&state.active)
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

    /// The producer pushes into the active backing of the state it
    /// loaded, and that push can land after two morphs: one that made
    /// the backing stale and one that would have pruned it as drained.
    /// The snapshot the producer holds is what keeps the backing on
    /// the stale list, so every subscriber's stale walk still reaches
    /// the late item.
    #[test]
    fn a_late_push_into_a_snapshot_two_morphs_old_reaches_every_subscriber() {
        let ring = CapacityBroadcastRing::create_anon(64).unwrap();
        let first = ring.register_consumer().unwrap();
        let second = ring.register_consumer().unwrap();

        // The producer's view of the ring, loaded before either morph
        // exactly as `try_push` loads it.
        let held = ring.state.load();
        ring.morph_capacity_to(128).expect("the first morph");
        ring.morph_capacity_to(256).expect("the second morph");

        held.active
            .try_push(&7u64.to_le_bytes())
            .expect("the late push lands in the backing the producer loaded");
        drop(held);

        let mut out = [0u8; 64];
        for idx in [first, second] {
            assert!(
                ring.try_recv(idx, &mut out).is_ok(),
                "subscriber {idx} cannot reach the item pushed into a backing two \
                 morphs old: the second morph pruned that backing from the stale \
                 list while the producer still held the state naming it"
            );
            assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);
            assert!(ring.try_recv(idx, &mut out).is_err(), "nothing else was pushed");
        }
    }

    /// A consumer that registers while a morph has built its backing and
    /// not yet published it keeps a slot on that backing: an item pushed
    /// after the morph reaches it, and the next registrant is handed an
    /// index of its own.
    #[test]
    fn a_registration_inside_a_morph_keeps_its_slot_on_the_new_backing() {
        let ring = Arc::new(CapacityBroadcastRing::create_anon(4).unwrap());
        let morphing = Arc::clone(&ring);
        let (pause, morph) = crate::test_races::stopped(move || morphing.morph_capacity_to(8));
        let early = ring.register_consumer().expect("a registration inside the morph");
        pause.release();
        morph.join().expect("the morph thread").expect("the morph");
        assert_eq!(ring.current_capacity(), 8);
        let late = ring.register_consumer().expect("a registration after the morph");
        assert_ne!(late, early, "two consumers were handed one index");
        ring.try_push(&7u64.to_le_bytes()).expect("a push after the morph");
        let mut out = [0u8; 64];
        for idx in [early, late] {
            if let Err(e) = ring.try_recv(idx, &mut out) {
                panic!("consumer {idx} missed the item pushed after the morph: {e:?}");
            }
            assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);
        }
    }

    /// A consumer stopped after it has claimed its slot on the active
    /// backing, while a morph publishes a new one: an item pushed after
    /// the morph reaches it.
    #[test]
    fn a_registration_a_morph_overtakes_reads_the_new_backing() {
        let ring = Arc::new(CapacityBroadcastRing::create_anon(4).unwrap());
        let registering = Arc::clone(&ring);
        let (pause, registrant) = crate::test_races::stopped(move || registering.register_consumer());
        ring.morph_capacity_to(8).expect("the morph");
        pause.release();
        let idx = registrant.join().expect("the registrant thread").expect("the registration");
        ring.try_push(&7u64.to_le_bytes()).expect("a push after the morph");
        let mut out = [0u8; 64];
        if let Err(e) = ring.try_recv(idx, &mut out) {
            panic!("the consumer the morph overtook missed the item pushed after it: {e:?}");
        }
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);
    }
}
