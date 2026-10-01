//! `CapacityAdaptiveRing`: runtime-resizable wrapper around
//! [`AdaptiveRing`] that adds capacity-axis
//! morphing to the polymorphic substrate.
//!
//! Where [`AdaptiveRing`] morphs between shapes
//! (SPSC / MPSC / MPMC / Vyukov) at a fixed capacity and
//! [`LocaleAdaptiveRing`](crate::LocaleAdaptiveRing) morphs between
//! locales (Anon / File / ShmFs) at a fixed capacity,
//! `CapacityAdaptiveRing` morphs the capacity itself: callers (or a
//! sidecar policy) call [`morph_capacity_to`](CapacityAdaptiveRing::morph_capacity_to)
//! with a new power-of-two slot count, the substrate allocates a
//! fresh underlying ring at the new size, keeps the old backing on a
//! stale list the consumer drains before it reads the new one, bumps
//! a pin generation so outstanding pinned handles invalidate, and
//! atomically swaps the active backing.
//!
//! # Why a fourth axis
//!
//! Shape morph addresses "the number of producers / consumers
//! changed at runtime". Locale morph addresses "we need to migrate
//! the bytes between Anon / File / ShmFs storage tiers". Capacity
//! morph addresses "the workload's queueing depth requirement
//! exceeds (or falls below) the ring's slot count, and we want to
//! grow (or shrink) without re-creating the whole ring from
//! scratch". A sidecar that observes producer-side backpressure
//! events or consumer-side starvation drives the morph; user code
//! also calls `morph_capacity_to` directly when the application
//! has out-of-band knowledge of expected load.
//!
//! # Constraints
//!
//! - **Power-of-two capacity preserved.** New capacity must be a
//!   power of two and at least 2. The slot-index calculation stays
//!   `hash & (capacity - 1)` = one `and` instruction. Non-pow2 sizes
//!   return [`CapacityMorphError::InvalidCapacity`].
//! - **Grow and shrink both succeed unconditionally.** In-flight
//!   items physically stay in the old (larger or smaller)
//!   AdaptiveRing as part of the stale list; the new capacity
//!   governs only items the producer pushes after the morph. The
//!   consumer's `try_recv` walks the stale list oldest-first then
//!   falls through to active, so every in-flight item still
//!   drains in send-order across the morph boundary. The
//!   `CannotShrinkInFlight` enum variant is preserved for API
//!   stability but is never returned by this implementation.
//! - **Pin invalidation is caller-polled.** Outstanding
//!   [`PinnedCapacity`] handles observe the generation bump on the
//!   next `is_still_valid()` call. Hot loops sample at whatever
//!   cadence fits their latency budget; the substrate does not
//!   push.
//! - **Morphs land one after another.** A morph publishes its new
//!   state (active backing, stale list, capacity, locale) with one
//!   compare-and-swap against the state it read, so two concurrent
//!   callers of `morph_capacity_to` never both build on one state:
//!   the one that loses the swap decides again from the winner's
//!   state and lands after it. Producer / consumer hot-path ops are
//!   not serialized against the morph - they keep dispatching via
//!   the state the SwapCell holds.
//! - **Consumer is sole reader of every backing.** Producers only
//!   write to active; morphs never read from any backing. This is
//!   what keeps the per-backing SPSC/MPSC/MPMC contract intact
//!   across morphs - exactly one reader touches each
//!   `SpscRingCore`, even when the active backing changes.
//!
//! # Cross-process and cross-host
//!
//! For in-process and cross-thread use, the
//! [`create_anon`](CapacityAdaptiveRing::create_anon) constructor
//! holds the active ring in a [`SwapCell`]; the morph is one
//! atomic store on the active pointer plus a push onto the stale
//! list. The consumer's `try_recv` walks the stale list before
//! reading from active, picking up every in-flight item in
//! send-order without ever racing the morph thread.
//!
//! For file-backed cross-process use,
//! [`create`](CapacityAdaptiveRing::create) names the initial
//! backing `{base}.cap_{N}.bin` and every morph's backing
//! `{base}.cap_{N}_g{seq}.bin` (the [`Shm`](BackingTarget::Shm)
//! locale uses `{prefix}_cap_{N}_g{seq}`); each backing is a full
//! [`AdaptiveRing`], so a second process attaches to any one of them
//! by that name through [`AdaptiveRing::open`]. The wrapper itself
//! is per-process: a morph swaps this process's active pointer and
//! never reaches into a peer, and the morph sequence is process-local
//! (two processes each calling `morph_capacity_to` would mint
//! different `seq` numbers, hence different files). The cross-process
//! pattern is therefore one owner per backing: the morphing process
//! creates each backing, the application publishes which backing is
//! active (a shared control value the reader polls), and the reader
//! process opens each successive backing as it becomes active,
//! draining the prior one to empty before switching. The
//! `capacity_morph_xproc` example drives exactly this - two
//! processes, a shared control atomic, every item delivered once and
//! in order across each resize.
//!
//! Over a QUIC / TCP bridge the ring's bytes are ferried as fixed
//! 64-byte slots regardless of either side's ring size, so ring
//! capacity is per-host independent: a capacity morph on one host
//! needs no coordination with the peer for correctness. The bridges
//! carry the ring's data on their stream; they carry no
//! capacity-morph control signal.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};

use subetha_core::SwapCell;

use crate::adaptive_ring::{AdaptiveError, AdaptiveRing, RingShape};
use crate::ordering::{default_stamp_kind, OrderingMode, StampKind};
use crate::shared_ring::RingError;
use crate::warm_slot::WarmSlot;

/// Locale target for a capacity wrapper's backings. Public mirror
/// of the construction-time locale choice, used by
/// [`RingConfig`] to retarget the locale as part of a compound
/// morph: subsequent backings (and prewarms) allocate at the new
/// locale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackingTarget {
    /// In-process anonymous mmap backings.
    Anon,
    /// File-backed; per-morph file at `{base}.cap_{N}_g{seq}.bin`.
    File(PathBuf),
    /// Named-shm backings at `{prefix}_cap_{N}_g{seq}`.
    Shm(String),
}

/// Compound morph target for [`CapacityAdaptiveRing::morph_to_config`].
/// Every axis is optional; `None` keeps the current value. One
/// compound morph builds a single fresh backing at the combined target,
/// carries the registered ids once, bumps the pin generation once, and
/// appends the displaced active to the stale list once - however
/// many axes changed.
#[derive(Debug, Clone, Default)]
pub struct RingConfig {
    /// Target shape (`None` = keep the active backing's shape).
    pub shape: Option<RingShape>,
    /// Target capacity, pow2 >= 2 (`None` = keep).
    pub capacity: Option<usize>,
    /// Target locale (`None` = keep). Setting this retargets the
    /// wrapper's locale for this morph and every subsequent morph
    /// / prewarm.
    pub locale: Option<BackingTarget>,
}

/// Errors returned by capacity-morph operations.
#[derive(Debug)]
pub enum CapacityMorphError {
    /// Target capacity is not a power of two, or is less than 2.
    InvalidCapacity,
    /// Never returned. Shrinks always succeed: in-flight items stay
    /// physically in the old AdaptiveRing as part of the stale list
    /// and the consumer drains them through `try_recv`'s stale-walk,
    /// so there is nothing for a shrink to refuse. The variant is
    /// part of the public enum and kept so matches stay exhaustive.
    CannotShrinkInFlight { in_flight: usize, new_capacity: usize },
    /// Underlying ring allocation, push, or pop failed during the
    /// morph. The active backing is unchanged.
    Ring(RingError),
    /// Carrying a registered producer or consumer id onto the new
    /// backing failed (a producer's ring could not be made there).
    Adaptive(AdaptiveError),
}

impl From<RingError> for CapacityMorphError {
    fn from(e: RingError) -> Self { Self::Ring(e) }
}

impl From<AdaptiveError> for CapacityMorphError {
    fn from(e: AdaptiveError) -> Self { Self::Adaptive(e) }
}

impl std::fmt::Display for CapacityMorphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCapacity => write!(f, "capacity must be pow2 >= 2"),
            Self::CannotShrinkInFlight { in_flight, new_capacity } => write!(
                f,
                "shrink rejected: {in_flight} in-flight items exceed new capacity {new_capacity}",
            ),
            Self::Ring(e) => write!(f, "ring error during morph: {e:?}"),
            Self::Adaptive(e) => write!(f, "adaptive ring error during morph: {e:?}"),
        }
    }
}

impl std::error::Error for CapacityMorphError {}

/// Runtime-resizable adaptive ring.
///
/// See the module-level docs for the morph protocol. Hot-path
/// `try_send` / `try_recv` calls hop through one SwapCell load plus
/// the active [`AdaptiveRing`]'s dispatch.
pub struct CapacityAdaptiveRing {
    /// Combined active + stale list behind a single SwapCell. Hot-
    /// path `try_send` / `try_recv` performs one SwapCell load
    /// (~5-10 ns) and delegates to the active backing's native
    /// dispatch; no mutex acquisition in steady state. Morph
    /// builds a fresh `RingState { active: new, stale: prune(old.stale) ++ old.active }`
    /// and atomic-swaps it; the FIFO-correctness combined-snapshot
    /// is given for free by the single atomic load.
    state: SwapCell<RingState>,
    /// Bumped on every successful morph so outstanding
    /// [`PinnedCapacity`] handles invalidate.
    pin_generation: AtomicU64,
    /// max_producers configured at construction; mirrored on every
    /// newly-allocated backing during a morph.
    max_producers: usize,
    /// max_consumers configured at construction; mirrored on every
    /// newly-allocated backing during a morph.
    max_consumers: usize,
    /// Monotonic morph counter. Bumped on every morph ahead of the
    /// new backing is allocated so the new path / shm-name is
    /// unique even when callers cycle through the same capacities
    /// (e.g. 256 -> 1024 -> 256 -> 1024 -> ...). File-backed and
    /// shmfs locales both need this because the prior backing's
    /// file / shm region is still mapped from the stale list, and
    /// attempting to create another at the same name fails on
    /// Windows in particular.
    morph_seq: AtomicU64,
    /// Stamp kind when the wrapper was constructed via a
    /// `*_stamped` constructor; mirrored (and seeded) onto every
    /// morph-allocated backing so the ordering axis survives
    /// capacity morphs.
    stamped: Option<StampKind>,
    /// One-slot warm cache: a fully constructed (and stamped, when
    /// the wrapper is stamped) backing at a predicted
    /// (capacity, locale), built off the morph's critical path by
    /// [`prewarm`](Self::prewarm) / [`prewarm_config`](Self::prewarm_config).
    /// The morph takes it when both key components match the morph
    /// target, skipping allocation + mapping + zeroing on the
    /// critical path. Shape is deliberately kept out of the key:
    /// fresh backings start SPSC and the swap path's shape morph
    /// on an empty backing costs microseconds. A wrong prediction
    /// stays in the slot until the next prewarm replaces it or
    /// the wrapper drops.
    warm: WarmSlot<(usize, BackingTarget), Arc<AdaptiveRing>>,
    /// Successful warm-cache hits consumed by capacity morphs.
    warm_hits: AtomicU64,
    /// Items the consumer popped from stale (post-morph) backings
    /// rather than the active one. Observability for transition
    /// cost; incremented only on the stale-walk pop path, never on
    /// the steady-state active path.
    stale_pops: AtomicU64,
}

/// Atomic snapshot of the ring's active backing + stale list.
/// Held behind a `SwapCell` on [`CapacityAdaptiveRing`] so the
/// hot path is a single Acquire load: producers go straight to
/// `state.active`, consumers walk `state.stale` then fall
/// through to `state.active`. Morph constructs a new `RingState`
/// and swaps the whole thing atomically.
struct RingState {
    /// The currently-active backing. Producers write here.
    active: Arc<AdaptiveRing>,
    /// Post-morph backings the consumer is still draining;
    /// oldest-first. The next morph prunes an entry that is empty
    /// and that this list alone holds. A producer writes to the
    /// `active` of the state it loaded, which may be a state a
    /// morph has since replaced; while that snapshot lives, the
    /// backing it names stays here so the write it carries is
    /// still drained.
    stale: Vec<Arc<AdaptiveRing>>,
    /// The active backing's capacity.
    capacity: usize,
    /// Where this state's morphs and prewarms build backings. A
    /// compound morph with a locale axis retargets it.
    locale: BackingTarget,
    /// Set once the state that replaced this one has been settled: a
    /// registration that lands on `active` after that is taken back and
    /// made again on the state in place. A reshaped copy of the state
    /// names the same backing and shares it.
    registrations_sealed: Arc<AtomicBool>,
    /// The seal of the backing this state's morph replaced, which the
    /// settle sets before it reads that backing's registrations.
    predecessor_sealed: Option<Arc<AtomicBool>>,
    /// Whether the registrations on the backing this state replaced are
    /// carried onto `active`. False from the morph that publishes the
    /// state until the first registration, send, receive or pin on it
    /// settles it.
    settled: AtomicBool,
}

unsafe impl Send for CapacityAdaptiveRing {}
unsafe impl Sync for CapacityAdaptiveRing {}

impl CapacityAdaptiveRing {
    /// The wrapper around its first backing.
    fn around(
        ring: AdaptiveRing,
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
        locale: BackingTarget,
        stamped: Option<StampKind>,
    ) -> Self {
        Self {
            // The first backing replaced none, so it has nothing to carry.
            state: SwapCell::from_arc(Arc::new(RingState {
                active: Arc::new(ring),
                stale: Vec::new(),
                capacity,
                locale,
                registrations_sealed: Arc::new(AtomicBool::new(false)),
                predecessor_sealed: None,
                settled: AtomicBool::new(true),
            })),
            pin_generation: AtomicU64::new(0),
            max_producers,
            max_consumers,
            morph_seq: AtomicU64::new(0),
            stamped,
            warm: WarmSlot::new(),
            warm_hits: AtomicU64::new(0),
            stale_pops: AtomicU64::new(0),
        }
    }

    /// Anon (in-process) capacity-adaptive ring. The active backing
    /// is an anonymous mmap; subsequent morphs allocate fresh anon
    /// mmaps at the new capacity and drop the prior one once
    /// stragglers drain.
    pub fn create_anon(
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let ring = AdaptiveRing::create_anon(
            max_producers,
            max_consumers,
            initial_capacity,
        )?;
        Ok(Self::around(
            ring, max_producers, max_consumers, initial_capacity, BackingTarget::Anon, None,
        ))
    }

    /// As [`create_anon`](Self::create_anon) with ordering stamps
    /// on the backing (and on every backing subsequent capacity
    /// morphs allocate). See
    /// [`AdaptiveRing::with_ordering_stamps`].
    pub fn create_anon_stamped(
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let kind = default_stamp_kind();
        let ring = AdaptiveRing::create_anon(
            max_producers, max_consumers, initial_capacity,
        )?
        .with_ordering_stamps_kind(kind)
        .map_err(CapacityMorphError::Ring)?;
        Ok(Self::around(
            ring, max_producers, max_consumers, initial_capacity, BackingTarget::Anon, Some(kind),
        ))
    }

    /// File-backed capacity-adaptive ring. The active backing is
    /// `{base_path}.cap_{initial_capacity}.bin`; morphs allocate
    /// fresh files at the morph target's suffix and drop the prior
    /// file once stragglers drain.
    pub fn create(
        base_path: impl AsRef<Path>,
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let base = base_path.as_ref().to_path_buf();
        let path = path_for_capacity(&base, initial_capacity);
        let ring = AdaptiveRing::create(
            &path,
            max_producers,
            max_consumers,
            initial_capacity,
        )?;
        Ok(Self::around(
            ring, max_producers, max_consumers, initial_capacity, BackingTarget::File(base), None,
        ))
    }

    /// Attach to the file-backed capacity-adaptive ring another handle
    /// created under `base_path` with the same counts and initial
    /// capacity, without re-initializing its backing. With `stamped` the
    /// backing's ordering region is attached and the creator's stamp
    /// kind adopted. Morph state stays per handle: this handle starts at
    /// the initial capacity whatever the creator morphed to.
    pub fn open(
        base_path: impl AsRef<Path>,
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
        stamped: bool,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let base = base_path.as_ref().to_path_buf();
        let path = path_for_capacity(&base, initial_capacity);
        let ring = AdaptiveRing::open(&path, max_producers, max_consumers, initial_capacity)?;
        let (ring, kind) = if stamped {
            let ring = ring.with_ordering_stamps().map_err(CapacityMorphError::Ring)?;
            let kind = ring.stamp_kind();
            (ring, kind)
        } else {
            (ring, None)
        };
        Ok(Self::around(
            ring, max_producers, max_consumers, initial_capacity, BackingTarget::File(base), kind,
        ))
    }

    /// As [`create`](Self::create) with ordering stamps on the
    /// backing and every morph-allocated successor.
    pub fn create_stamped(
        base_path: impl AsRef<Path>,
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let kind = default_stamp_kind();
        let base = base_path.as_ref().to_path_buf();
        let path = path_for_capacity(&base, initial_capacity);
        let ring = AdaptiveRing::create(
            &path, max_producers, max_consumers, initial_capacity,
        )?
        .with_ordering_stamps_kind(kind)
        .map_err(CapacityMorphError::Ring)?;
        Ok(Self::around(
            ring, max_producers, max_consumers, initial_capacity, BackingTarget::File(base), Some(kind),
        ))
    }

    /// ShmFs (named shared memory) capacity-adaptive ring. The
    /// active backing is named `{name_prefix}_cap_{initial_capacity}`;
    /// morphs allocate fresh named-shm regions at the morph
    /// target's suffix and drop the prior region once stragglers
    /// drain. Cross-process visible: another process opens the
    /// same logical ring by constructing a CapacityAdaptiveRing
    /// with the same `name_prefix`.
    ///
    /// Each backing is this ring's alone, so its shared-memory names go
    /// when the ring drops it, unlike a plain shm-backed
    /// [`AdaptiveRing`]'s, which outlive every handle.
    pub fn create_shmfs(
        name_prefix: &str,
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let name = format!("{name_prefix}_cap_{initial_capacity}");
        let ring = AdaptiveRing::create_shmfs(
            &name,
            max_producers,
            max_consumers,
            initial_capacity,
        )?
        .removing_names_on_drop();
        Ok(Self::around(
            ring,
            max_producers,
            max_consumers,
            initial_capacity,
            BackingTarget::Shm(name_prefix.to_owned()),
            None,
        ))
    }

    /// As [`create_shmfs`](Self::create_shmfs) with ordering stamps
    /// on the backing and every morph-allocated successor.
    pub fn create_shmfs_stamped(
        name_prefix: &str,
        max_producers: usize,
        max_consumers: usize,
        initial_capacity: usize,
    ) -> Result<Self, CapacityMorphError> {
        if !initial_capacity.is_power_of_two() || initial_capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let kind = default_stamp_kind();
        let name = format!("{name_prefix}_cap_{initial_capacity}");
        let ring = AdaptiveRing::create_shmfs(
            &name, max_producers, max_consumers, initial_capacity,
        )?
        .removing_names_on_drop()
        .with_ordering_stamps_kind(kind)
        .map_err(CapacityMorphError::Ring)?;
        Ok(Self::around(
            ring,
            max_producers,
            max_consumers,
            initial_capacity,
            BackingTarget::Shm(name_prefix.to_owned()),
            Some(kind),
        ))
    }

    /// Current capacity of the active backing, read from the same
    /// state the active backing is.
    pub fn current_capacity(&self) -> usize {
        self.state.load().capacity
    }

    /// Current pin generation. Pinned handles capture this at pin
    /// time; a different live value means the pin is stale.
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Register a producer on the active backing and return its id. The
    /// id stays the producer's across morphs: each backing a morph makes
    /// active carries the registrations of the one it replaced, and a
    /// registration that lands on a backing already replaced is made
    /// again on the backing in place.
    pub fn register_producer(&self) -> Result<usize, AdaptiveError> {
        loop {
            let state = self.state.load_full();
            self.settle(&state);
            #[cfg(test)]
            crate::test_races::pause_point();
            let id = state.active.register_producer()?;
            // Pairs with the fence in `settle`: either the seal is seen
            // here, or the settle that set it sees this id and carries it.
            fence(Ordering::SeqCst);
            if !state.registrations_sealed.load(Ordering::SeqCst) {
                return Ok(id);
            }
            state.active.unregister_producer(id);
        }
    }

    /// Register a consumer on the active backing and return its id,
    /// which stays the consumer's across morphs as a producer's does.
    pub fn register_consumer(&self) -> Result<usize, AdaptiveError> {
        loop {
            let state = self.state.load_full();
            self.settle(&state);
            let id = state.active.register_consumer()?;
            // Pairs with the fence in `settle`, as in `register_producer`.
            fence(Ordering::SeqCst);
            if !state.registrations_sealed.load(Ordering::SeqCst) {
                return Ok(id);
            }
            state.active.unregister_consumer(id);
        }
    }

    /// Carry the registrations of the backing `state`'s morph replaced
    /// onto its active backing, once, before anything registers on,
    /// sends into, receives from or pins it. The replaced backing is
    /// sealed first, so a registration that lands there afterwards is
    /// made again on this state instead. A producer whose ring cannot
    /// be made here keeps its claim, so the shape still counts it, and
    /// is named on stderr: its sends to this backing are refused.
    fn settle(&self, state: &RingState) {
        if state.settled.load(Ordering::Acquire) {
            return;
        }
        if let (Some(seal), Some(replaced)) = (&state.predecessor_sealed, state.stale.last()) {
            seal.store(true, Ordering::SeqCst);
            // Pairs with the fence in `register_producer` and
            // `register_consumer`.
            fence(Ordering::SeqCst);
            for id in replaced.producer_ids() {
                if let Err(e) = state.active.carry_producer(id) {
                    eprintln!(
                        "subetha: producer {id} could not be carried onto the backing a capacity \
                         morph made active, so what it sends there is refused: {e:?}"
                    );
                }
            }
            for id in replaced.consumer_ids() {
                if let Err(e) = state.active.carry_consumer(id) {
                    eprintln!(
                        "subetha: consumer {id} could not be carried onto the backing a capacity \
                         morph made active: {e:?}"
                    );
                }
            }
        }
        state.settled.store(true, Ordering::Release);
    }

    /// Hot-path push. One SwapCell load + the active backing's
    /// dispatched `try_send`, after the settle a new backing takes once.
    #[inline]
    pub fn try_send(
        &self,
        producer_id: usize,
        payload: &[u8],
    ) -> Result<(), RingError> {
        let state = self.state.load();
        self.settle(&state);
        state.active.try_send(producer_id, payload)
    }

    /// Hot-path pop. Walks the stale backing list oldest-first,
    /// returning the first non-empty backing's item; falls through
    /// to the active backing when every stale entry is empty.
    ///
    /// The consumer is the only reader of every backing, stale and
    /// active. Producers write only to the active backing. This keeps
    /// the SPSC contract on each backing's `SpscRingCore`: exactly one
    /// consumer touches it, across morphs.
    ///
    /// FIFO ordering invariant: the stale list and the active backing
    /// come from one `RingState` snapshot, so a morph cannot land
    /// between reading the stale list and loading the active backing.
    /// Read apart, the consumer could take from the new active backing
    /// while the old one sits unread at the tail of the stale list,
    /// which reorders items pushed to the old backing behind items
    /// pushed to the new one.
    #[inline]
    pub fn try_recv(
        &self,
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<usize, RingError> {
        // One SwapCell load gives us a consistent snapshot of
        // both stale and active. The wrapper does no mutex
        // acquisition on the hot path.
        //
        // Per-stale-ring spin discipline (FIFO correctness):
        // walking a stale ring may observe `Err(Empty)` in two
        // distinct cases:
        //
        //   (a) ring's consumer_seq >= producer_seq - truly
        //       drained for this consumer; safe to advance.
        //   (b) ring's consumer_seq < producer_seq and the slot
        //       at consumer_seq is mid-claim (producer has CAS'd
        //       producer_seq forward but not yet stored the
        //       payload, or another consumer is mid-claim on the
        //       same slot under MPMC) - so it is not empty; advancing now
        //       and reading from `active` would let this consumer
        //       consume a higher-producer-index item from `active`
        //       before the lower-producer-index item from this
        //       stale ring becomes available, violating per-
        //       consumer per-producer FIFO.
        //
        // On Err from a stale ring, check `is_empty()`
        // (which compares producer_seq == consumer_seq, rather than
        // slot-sequence). If truly empty, advance. Otherwise spin
        // and retry on the same stale ring until the in-flight
        // claim commits (bounded by producer commit latency).
        let state = self.state.load();
        for ring in &state.stale {
            loop {
                match ring.try_recv(consumer_id, out) {
                    Ok(n) => {
                        self.stale_pops.fetch_add(1, Ordering::Relaxed);
                        return Ok(n);
                    }
                    // Empty is the wait for an in-flight claim to commit;
                    // the ring reporting itself drained ends it.
                    Err(RingError::Empty) => {
                        if ring.is_empty() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        self.settle(&state);
        state.active.try_recv(consumer_id, out)
    }

    /// Morph the ring's capacity to `new_capacity`. Allocates a
    /// fresh backing at the new size, bumps `pin_generation`,
    /// stashes the old backing onto the `stale` list (the
    /// consumer drains it via `try_recv`'s stale-walk), and
    /// atomic-swaps the active pointer. Concurrent morphs each land
    /// in turn: a morph whose state another replaced first decides
    /// again from the replacement. Hot-path ops are not blocked.
    ///
    /// Critically the morph leaves the old backing undrained - that
    /// would race against the consumer's concurrent `try_recv` on
    /// the same backing, violating the per-backing
    /// SPSC/MPSC/MPMC contract (two consumers on an SPSC ring is
    /// undefined behavior). Instead the old backing stays
    /// reachable via the stale list; the consumer is the sole
    /// reader and pops every in-flight item via `try_recv`'s
    /// stale-walk-then-active pattern.
    ///
    /// Shrink always succeeds. In-flight items physically remain
    /// in the old (larger) backing as part of the stale list; the
    /// new capacity governs only items the producer pushes after
    /// the morph. Memory holds both old + new backings until the
    /// consumer drains old and no loaded snapshot names it any
    /// more, at which point the next morph prunes the empty old
    /// entry from the stale list.
    pub fn morph_capacity_to(
        &self,
        new_capacity: usize,
    ) -> Result<(), CapacityMorphError> {
        self.morph_to_config(&RingConfig {
            capacity: Some(new_capacity),
            ..RingConfig::default()
        })
    }

    /// Compound morph: change any subset of {shape, capacity,
    /// locale} in a single transition. Builds one fresh backing at
    /// the combined target (warm-cache hit when
    /// [`prewarm_config`](Self::prewarm_config) predicted it),
    /// seeds stamps, carries the registered ids once, applies the
    /// target shape to the empty new backing, bumps the pin
    /// generation once, and appends the displaced active to the
    /// stale list once - however many axes changed. A sequential
    /// walk of the same axes pays each of those costs per axis.
    ///
    /// Special cases:
    /// - Every axis already at target: no-op, no generation bump.
    /// - Shape-only change (capacity + locale unchanged):
    ///   delegates to the active backing's in-place shape morph
    ///   (all four shape protocols are pre-allocated inside
    ///   `AdaptiveRing`), so no fresh backing is built, the
    ///   wrapper pin stays valid, and in-flight items stay put.
    /// - A locale axis retargets the wrapper's [`BackingTarget`]
    ///   for this morph and every subsequent morph / prewarm.
    pub fn morph_to_config(
        &self,
        target: &RingConfig,
    ) -> Result<(), CapacityMorphError> {
        loop {
            let old_state = self.state.load_full();
            // A state is settled before it is replaced, so the backing
            // its settle carries from is never one a prune has taken.
            self.settle(&old_state);
            let old = Arc::clone(&old_state.active);
            let old_capacity = old_state.capacity;
            let old_shape = old.current_shape();
            let old_locale = old_state.locale.clone();

            let new_capacity = target.capacity.unwrap_or(old_capacity);
            if !new_capacity.is_power_of_two() || new_capacity < 2 {
                return Err(CapacityMorphError::InvalidCapacity);
            }
            let new_shape = target.shape.unwrap_or(old_shape);
            let new_locale =
                target.locale.clone().unwrap_or_else(|| old_locale.clone());

            if new_capacity == old_capacity
                && new_shape == old_shape
                && new_locale == old_locale
            {
                return Ok(());
            }

            // Shape-only: in-place morph on the active backing. No
            // fresh backing, no wrapper pin invalidation, no stale
            // entry - AdaptiveRing pre-allocates all four shape
            // protocols and handles its own transition. A copy of the
            // state goes in after it, so a morph that read the state
            // before this reshape finds it replaced and decides again
            // from the reshaped backing.
            if new_capacity == old_capacity && new_locale == old_locale {
                old.morph_to(new_shape).map_err(CapacityMorphError::Ring)?;
                let reshaped = RingState {
                    active: Arc::clone(&old),
                    stale: old_state.stale.clone(),
                    capacity: old_capacity,
                    locale: old_locale,
                    registrations_sealed: Arc::clone(&old_state.registrations_sealed),
                    predecessor_sealed: old_state.predecessor_sealed.clone(),
                    // Settled above, with the same backing.
                    settled: AtomicBool::new(true),
                };
                if self.publish_state(&old_state, reshaped) {
                    return Ok(());
                }
                continue;
            }

            // Warm-cache probe: a prediction matching the morph
            // target's (capacity, locale) skips allocation + mapping +
            // zeroing entirely; a mismatch stays cached for a later
            // morph and the cold path below runs unchanged.
            let (new, from_warm) = match self.warm.take(&(new_capacity, new_locale.clone())) {
                Some(ring) => (ring, true),
                None => (self.build_backing(new_capacity, &new_locale)?, false),
            };

            // Seed the ordering axis at swap time - counters move
            // continuously, so seeding cannot happen at build time.
            // The fresh region inherits the old one's counter stamps
            // and live mode flag, keeping stamps monotone across the
            // swap for warm and cold builds alike.
            if self.stamped.is_some()
                && let (Some(new_region), Some(old_region)) =
                    (new.ordering_region(), old.ordering_region())
            {
                new_region.seed_from(old_region);
            }

            // Carry the registered ids so the new backing accepts ops
            // against the same ids the old one accepted. One that lands
            // on the old backing after this read is carried by the new
            // state's settle.
            for id in old.producer_ids() {
                new.carry_producer(id)?;
            }
            for id in old.consumer_ids() {
                new.carry_consumer(id)?;
            }

            // Apply the target shape to the (empty, unobserved) new
            // backing. Fresh and warm backings both start SPSC, so one
            // call covers the keep-shape mirror and the compound shape
            // axis; registration counts alone never trigger a shape
            // morph, and skipping this would silently drop an MPSC /
            // MPMC / Vyukov ring back to SPSC on the new backing.
            if new.current_shape() != new_shape {
                new.morph_to(new_shape).map_err(CapacityMorphError::Ring)?;
            }

            // Build the new state in one shot: prune the old stale
            // list, append the prior active onto the end, then publish
            // atomically. Producers and consumers reading via
            // `self.state.load()` see either the full old state or the
            // full new state - never a half-state where active and
            // stale disagree.
            //
            // A stale entry leaves the list only when it is drained and
            // this list is its last holder. A producer pushes into the
            // active backing of the state it loaded, and that load can
            // predate this morph and the one before it; its snapshot
            // holds the RingState, which holds the backing, so a
            // backing some snapshot can still reach has a strong count
            // above one and stays reachable to the consumer's stale walk
            // for the push that has not landed yet. With this list the
            // sole holder, no push can arrive and the emptiness is
            // final.
            let mut new_stale: Vec<Arc<AdaptiveRing>> = old_state
                .stale
                .iter()
                .filter(|r| !(r.is_empty() && Arc::strong_count(r) == 1))
                .cloned()
                .collect();
            new_stale.push(old);
            let new_state = RingState {
                active: new,
                stale: new_stale,
                capacity: new_capacity,
                locale: new_locale,
                registrations_sealed: Arc::new(AtomicBool::new(false)),
                predecessor_sealed: Some(Arc::clone(&old_state.registrations_sealed)),
                settled: AtomicBool::new(false),
            };
            #[cfg(test)]
            crate::test_races::pause_point();
            if self.publish_state(&old_state, new_state) {
                if from_warm {
                    self.warm_hits.fetch_add(1, Ordering::Relaxed);
                }
                // Bump the pin generation so outstanding pins invalidate.
                self.pin_generation.fetch_add(1, Ordering::AcqRel);
                return Ok(());
            }
            // Another morph replaced the state first. The backing built
            // here was shaped for a state that is gone, so it drops, and
            // the next pass decides again from the state in place.
        }
    }

    /// Put `new` in place of `old`: false when another morph replaced
    /// `old` first.
    fn publish_state(&self, old: &Arc<RingState>, new: RingState) -> bool {
        self.state.compare_and_set(old, Arc::new(new)).is_ok()
    }

    /// Construct (and stamp, when the wrapper is stamped) a fresh
    /// backing at `capacity`, at the wrapper's locale, with a
    /// unique per-build name. Shared by the cold morph path and
    /// [`prewarm`](Self::prewarm).
    fn build_backing(
        &self,
        capacity: usize,
        locale: &BackingTarget,
    ) -> Result<Arc<AdaptiveRing>, CapacityMorphError> {
        // Bump the morph sequence ahead of allocating so file paths
        // and shm names are unique even when callers cycle through
        // the same capacities (the prior backing's file / shm
        // region is still mapped from the stale list and cannot
        // share its name with a new backing). Speculative builds
        // that are never consumed burn a sequence number; gaps are
        // harmless because the value only disambiguates names.
        let seq = self.morph_seq.fetch_add(1, Ordering::AcqRel);
        let mut ring = match locale {
            BackingTarget::Anon => AdaptiveRing::create_anon(
                self.max_producers,
                self.max_consumers,
                capacity,
            )?,
            BackingTarget::File(base) => AdaptiveRing::create(
                path_for_capacity_seq(base, capacity, seq),
                self.max_producers,
                self.max_consumers,
                capacity,
            )?,
            BackingTarget::Shm(prefix) => AdaptiveRing::create_shmfs(
                &format!("{prefix}_cap_{capacity}_g{seq}"),
                self.max_producers,
                self.max_consumers,
                capacity,
            )?
            .removing_names_on_drop(),
        };
        // Stamp at build time: stamping consumes the ring by
        // value, so a cached warm backing must already carry its
        // stamps. Seeding from the live region happens at swap
        // time in `morph_to_config`.
        if let Some(kind) = self.stamped {
            ring = ring
                .with_ordering_stamps_kind(kind)
                .map_err(CapacityMorphError::Ring)?;
        }
        Ok(Arc::new(ring))
    }

    /// Speculatively build a backing at `capacity` (current
    /// locale) into the one-slot warm cache, off the morph's
    /// critical path. The next morph targeting that capacity
    /// consumes it and skips allocation + mapping + zeroing.
    pub fn prewarm(&self, capacity: usize) -> Result<(), CapacityMorphError> {
        self.prewarm_config(&RingConfig {
            capacity: Some(capacity),
            ..RingConfig::default()
        })
    }

    /// Speculatively build a backing at `target`'s (capacity,
    /// locale) into the one-slot warm cache, off the morph's
    /// critical path - the build half of a build-beside-and-
    /// repatch transition: the following
    /// [`morph_to_config`](Self::morph_to_config) at the same
    /// target consumes it and pays only the swap. The shape axis
    /// is ignored here: the swap path shapes the empty backing in
    /// microseconds. The slot holds exactly one prediction, so a
    /// second prewarm at a different target evicts the first;
    /// re-prewarming the cached (capacity, locale) is a no-op.
    pub fn prewarm_config(
        &self,
        target: &RingConfig,
    ) -> Result<(), CapacityMorphError> {
        let capacity = target
            .capacity
            .unwrap_or_else(|| self.current_capacity());
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(CapacityMorphError::InvalidCapacity);
        }
        let locale = target
            .locale
            .clone()
            .unwrap_or_else(|| self.state.load().locale.clone());
        if self.warm.holds(&(capacity, locale.clone())) {
            return Ok(());
        }
        // Concurrent prewarms race benignly: the last store wins.
        let ring = self.build_backing(capacity, &locale)?;
        self.warm.store((capacity, locale), ring);
        Ok(())
    }

    /// Capacity currently held in the warm cache, if any.
    pub fn warm_capacity(&self) -> Option<usize> {
        self.warm.key().map(|(capacity, _)| capacity)
    }

    /// Number of morphs that consumed a warm-cache prediction.
    pub fn warm_hits(&self) -> u64 {
        self.warm_hits.load(Ordering::Relaxed)
    }

    /// Items the consumer popped from stale (post-morph) backings
    /// rather than the active one, since construction. The
    /// transition-cost observability counterpart to `warm_hits`.
    pub fn stale_pops(&self) -> u64 {
        self.stale_pops.load(Ordering::Relaxed)
    }

    /// Drop any cached prediction, releasing its memory (and its
    /// file / shm region for non-anon locales).
    pub fn clear_warm(&self) {
        self.warm.clear();
    }

    /// Pin the current capacity backing for a hot loop. The
    /// returned [`PinnedCapacity`] exposes the underlying
    /// [`AdaptiveRing`] directly and validates
    /// against the pin generation via
    /// [`is_still_valid`](PinnedCapacity::is_still_valid). The backing
    /// is settled first, so every registered id is on it.
    pub fn pin_current_capacity(&self) -> PinnedCapacity<'_> {
        let captured_gen = self.pin_generation.load(Ordering::Acquire);
        let state = self.state.load();
        self.settle(&state);
        let ring = Arc::clone(&state.active);
        let capacity = state.capacity;
        PinnedCapacity {
            parent: self,
            pinned_generation: captured_gen,
            ring,
            capacity,
            _not_sync: std::marker::PhantomData,
        }
    }

    /// Direct access to the active [`AdaptiveRing`], settled first, so
    /// every registered id is on it.
    /// Override hatch for callers that want the shape-axis surface
    /// on top of the capacity-axis morphing.
    pub fn ring_handle(&self) -> Arc<AdaptiveRing> {
        let state = self.state.load();
        self.settle(&state);
        Arc::clone(&state.active)
    }

    /// Whether this wrapper's backings carry ordering stamps.
    pub fn is_stamped(&self) -> bool {
        self.stamped.is_some()
    }

    /// Live ordering mode of the active backing (`None` when
    /// unstamped).
    pub fn ordering_mode(&self) -> Option<OrderingMode> {
        self.state.load().active.ordering_mode()
    }

    /// Flip the ordering mode across the active backing and every
    /// stale backing still draining, so the consumer's
    /// stale-walk-then-active pop applies one consistent discipline.
    /// Cross-backing order note: producers only ever write to the
    /// active backing, so every stale item predates every active
    /// item - the stale-oldest-first walk composes with per-backing
    /// stamp merging into global stamp order across the morph
    /// boundary (within the stamp source's skew window).
    pub fn set_ordering_mode(&self, mode: OrderingMode) -> Result<(), RingError> {
        let state = self.state.load();
        for ring in &state.stale {
            ring.set_ordering_mode(mode)?;
        }
        state.active.set_ordering_mode(mode)
    }

    /// Cross-producer inversions observed on the active backing.
    /// Continuous across capacity morphs: each morph seeds the
    /// fresh region's counter from the old one.
    pub fn inversions(&self) -> u64 {
        self.state.load().active.inversions()
    }
}

/// Pinned snapshot of a [`CapacityAdaptiveRing`]'s current
/// capacity backing. `Send` (an Arc lifetime extension), `!Sync`
/// (single-owner-at-a-time semantics via [`std::cell::Cell`]
/// marker).
pub struct PinnedCapacity<'a> {
    parent: &'a CapacityAdaptiveRing,
    pinned_generation: u64,
    ring: Arc<AdaptiveRing>,
    capacity: usize,
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl<'a> PinnedCapacity<'a> {
    /// Whether this pin's capacity backing is still the active
    /// backing. One Acquire load on the parent's generation atom.
    pub fn is_still_valid(&self) -> bool {
        self.parent.pin_generation.load(Ordering::Acquire) == self.pinned_generation
    }

    /// Capacity captured at pin time.
    pub fn capacity(&self) -> usize { self.capacity }

    /// Pin generation captured at pin time.
    pub fn generation(&self) -> u64 { self.pinned_generation }

    /// Direct access to the pinned [`AdaptiveRing`].
    pub fn ring(&self) -> &Arc<AdaptiveRing> { &self.ring }
}

/// Compose the file path for a given capacity (initial-backing
/// form). Used at constructor time when no morph has happened yet
/// so no per-morph sequence number exists.
fn path_for_capacity(base: &Path, capacity: usize) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".cap_{capacity}.bin"));
    PathBuf::from(s)
}

/// Compose the per-morph file path. The `seq` disambiguates
/// successive morphs that revisit the same capacity (e.g. cycling
/// 256 -> 1024 -> 256 -> 1024 ...) so the prior backing's file
/// can sit in the stale list while the new one allocates without
/// a path collision.
fn path_for_capacity_seq(base: &Path, capacity: usize, seq: u64) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".cap_{capacity}_g{seq}.bin"));
    PathBuf::from(s)
}

// ===================================================================
// Sidecar capacity policy: automatic morphing based on fill-ratio
// observations, mirroring the shape-morph
// `AdaptiveRingSidecar` / `DefaultRingShapePolicy` design.
// ===================================================================

/// A snapshot of the capacity-adaptive ring's observable state
/// passed to a [`CapacityPolicy`] on every sidecar scan.
#[derive(Debug, Clone, Copy)]
pub struct CapacityPolicyObservation {
    /// Current capacity of the active backing (slots per sub-ring,
    /// per the underlying SPSC / MPSC / MPMC / Vyukov shape).
    pub current_capacity: usize,
    /// Approximate item count across every sub-ring of the active
    /// backing right now (sum over per-producer rings for composed
    /// shapes; a single ring's depth for SPSC / Vyukov).
    pub active_approx_len: usize,
    /// Total slot inventory the producer can fill before back-
    /// pressure. Equals `current_capacity` for SPSC / Vyukov;
    /// `current_capacity * n_sub_rings` for composed shapes.
    pub total_slot_capacity: usize,
    /// Time since the last successful capacity morph. Used by the
    /// policy to suppress thrashing via hysteresis.
    pub since_last_morph: std::time::Duration,
}

impl CapacityPolicyObservation {
    /// Convenience accessor: `active_approx_len / total_slot_capacity`
    /// clamped to `[0.0, 1.0]`. Policy logic typically branches on
    /// this against a `grow_at` upper threshold and a `shrink_at`
    /// lower threshold.
    pub fn fill_ratio(&self) -> f64 {
        if self.total_slot_capacity == 0 {
            return 0.0;
        }
        let ratio = self.active_approx_len as f64 / self.total_slot_capacity as f64;
        if ratio > 1.0 { 1.0 } else { ratio }
    }
}

/// Policy that decides when (and to what new capacity) the sidecar
/// should grow / shrink the
/// [`CapacityAdaptiveRing`]. Returning `Some(new_capacity)`
/// triggers `morph_capacity_to(new_capacity)`. Returning `None`
/// leaves the capacity alone.
pub trait CapacityPolicy: Send + Sync + 'static {
    fn decide(&self, observation: &CapacityPolicyObservation) -> Option<usize>;

    /// Capacity the policy expects `decide` to request soon, used
    /// by the sidecar to pre-build the backing off the morph
    /// lock's critical path ([`CapacityAdaptiveRing::prewarm`]).
    /// Purely speculative: a prediction never changes what the
    /// ring morphs to, only how fast the morph executes when the
    /// prediction was right. The default returns `None`, so
    /// existing policy impls keep their behavior unchanged.
    fn predict(&self, _observation: &CapacityPolicyObservation) -> Option<usize> {
        None
    }
}

/// Default capacity policy: fill-ratio with hysteresis.
///
/// On every scan the sidecar computes `fill_ratio = approx_len /
/// total_capacity`. If `fill_ratio >= grow_at`, the policy doubles
/// the capacity (up to `max_capacity`). If `fill_ratio <=
/// shrink_at`, the policy halves the capacity (down to
/// `min_capacity`). Otherwise it returns `None`.
///
/// Suppressed for `since_last_morph < hysteresis` to prevent
/// thrashing under bursty load. Default hysteresis 100 ms matches
/// the shape-morph policy.
pub struct DefaultCapacityPolicy {
    /// Upper fill-ratio that triggers a grow. Default 0.85.
    pub grow_at: f64,
    /// Lower fill-ratio that triggers a shrink. Default 0.10.
    pub shrink_at: f64,
    /// Minimum allowed capacity (pow2 >= 2). Default 64.
    pub min_capacity: usize,
    /// Maximum allowed capacity (pow2). Default 65536.
    pub max_capacity: usize,
    /// Cooldown after each morph. Default 100 ms.
    pub hysteresis: std::time::Duration,
}

impl Default for DefaultCapacityPolicy {
    fn default() -> Self {
        Self {
            grow_at: 0.85,
            shrink_at: 0.10,
            min_capacity: 64,
            max_capacity: 65536,
            hysteresis: std::time::Duration::from_millis(100),
        }
    }
}

impl CapacityPolicy for DefaultCapacityPolicy {
    fn decide(&self, obs: &CapacityPolicyObservation) -> Option<usize> {
        if obs.since_last_morph < self.hysteresis {
            return None;
        }
        let ratio = obs.fill_ratio();
        if ratio >= self.grow_at && obs.current_capacity < self.max_capacity {
            Some((obs.current_capacity * 2).min(self.max_capacity))
        } else if ratio <= self.shrink_at && obs.current_capacity > self.min_capacity {
            Some((obs.current_capacity / 2).max(self.min_capacity))
        } else {
            None
        }
    }

    /// Predicts the doubled capacity once the fill ratio crosses
    /// 75% of the grow threshold, and the halved capacity once it
    /// falls under 150% of the shrink threshold - the trend bands
    /// in front of the decide thresholds. Deliberately left ungated
    /// on hysteresis: the cooldown window after a morph is exactly
    /// the right time to build the next predicted backing.
    fn predict(&self, obs: &CapacityPolicyObservation) -> Option<usize> {
        let ratio = obs.fill_ratio();
        if ratio >= self.grow_at * 0.75 && obs.current_capacity < self.max_capacity {
            Some((obs.current_capacity * 2).min(self.max_capacity))
        } else if ratio <= self.shrink_at * 1.5 && obs.current_capacity > self.min_capacity {
            Some((obs.current_capacity / 2).max(self.min_capacity))
        } else {
            None
        }
    }
}

/// Background scanner thread that drives capacity morphs on a
/// [`CapacityAdaptiveRing`] from a [`CapacityPolicy`].
///
/// `spawn` starts the thread; `shutdown` stops it. The thread
/// scans every `scan_interval`, builds a
/// [`CapacityPolicyObservation`], asks the policy, and calls
/// [`CapacityAdaptiveRing::morph_capacity_to`] on `Some(new_capacity)`
/// responses. Successful morphs increment the per-sidecar
/// `morphs_triggered` counter.
pub struct CapacityAdaptiveRingSidecar {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    morphs_triggered: Arc<AtomicU64>,
    prewarms_issued: Arc<AtomicU64>,
}

impl CapacityAdaptiveRingSidecar {
    /// Spawn a sidecar thread that morphs `ring` according to
    /// `policy` decisions sampled every `scan_interval`.
    ///
    /// Prediction wiring: when `policy.predict` names the same
    /// target on two consecutive scans (a sustained trend, not a
    /// one-scan blip), the sidecar pre-builds that backing via
    /// [`CapacityAdaptiveRing::prewarm`] - off the morph's critical
    /// path, on this thread's idle time - so the eventual
    /// `decide`-driven morph consumes it instead of allocating on the
    /// critical path. A policy whose `predict` returns `None` (the
    /// trait default) never prewarms.
    pub fn spawn<P: CapacityPolicy>(
        ring: Arc<CapacityAdaptiveRing>,
        policy: P,
        scan_interval: std::time::Duration,
    ) -> Self {
        Self::spawn_gated(ring, policy, scan_interval, crate::policy_gate::GateConfig::default())
    }

    /// As [`spawn`](Self::spawn) with a confidence gate between the
    /// policy's recommendation and the morph. With
    /// `gate_cfg.enabled == false` (the default) behavior is
    /// identical to `spawn`. Enabled, a recommendation must hold
    /// across consecutive scans until conviction crosses the
    /// gate's threshold (and any sample floor); recommendation
    /// reversals, peer-count changes, and fill-ratio jumps collapse
    /// conviction, so oscillating load starves the gate instead of
    /// thrashing the ring.
    pub fn spawn_gated<P: CapacityPolicy>(
        ring: Arc<CapacityAdaptiveRing>,
        policy: P,
        scan_interval: std::time::Duration,
        gate_cfg: crate::policy_gate::GateConfig,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let morphs_triggered = Arc::new(AtomicU64::new(0));
        let prewarms_issued = Arc::new(AtomicU64::new(0));

        let stop_c = Arc::clone(&stop);
        let morphs_c = Arc::clone(&morphs_triggered);
        let prewarms_c = Arc::clone(&prewarms_issued);
        let handle = std::thread::spawn(move || {
            let mut last_morph = std::time::Instant::now();
            let mut last_predicted: Option<usize> = None;
            let mut gate = crate::policy_gate::ConfidenceGate::new(gate_cfg);
            let mut last_peers = (0usize, 0usize);
            let mut last_fill = 0.0f64;
            let mut first_scan = true;
            while !stop_c.load(Ordering::Acquire) {
                let active = ring.ring_handle();
                let obs = CapacityPolicyObservation {
                    current_capacity: ring.current_capacity(),
                    active_approx_len: active.approx_len(),
                    total_slot_capacity: active.total_slot_capacity(),
                    since_last_morph: last_morph.elapsed(),
                };
                let peers = (active.active_producers(), active.active_consumers());
                drop(active);

                // Regime-shift signals collapse conviction: the
                // workload changed character, so any accumulated
                // agreement belongs to the old regime.
                let fill = obs.fill_ratio();
                if !first_scan {
                    if peers != last_peers {
                        gate.shock();
                    }
                    if (fill - last_fill).abs() > 0.5 {
                        gate.shock();
                    }
                }
                last_peers = peers;
                last_fill = fill;
                first_scan = false;

                if let Some(new_cap) = gate.observe(policy.decide(&obs))
                    && ring.morph_capacity_to(new_cap).is_ok()
                {
                    last_morph = std::time::Instant::now();
                    morphs_c.fetch_add(1, Ordering::Relaxed);
                }
                match policy.predict(&obs) {
                    Some(target) if target != ring.current_capacity() => {
                        // Two consecutive scans naming the same
                        // target = a sustained trend; build it.
                        if last_predicted == Some(target)
                            && ring.warm_capacity() != Some(target)
                            && ring.prewarm(target).is_ok()
                        {
                            prewarms_c.fetch_add(1, Ordering::Relaxed);
                        }
                        last_predicted = Some(target);
                    }
                    _ => last_predicted = None,
                }
                std::thread::sleep(scan_interval);
            }
        });

        Self { handle: Some(handle), stop, morphs_triggered, prewarms_issued }
    }

    /// Number of successful morphs triggered by this sidecar
    /// since `spawn`.
    pub fn morphs_triggered(&self) -> u64 {
        self.morphs_triggered.load(Ordering::Relaxed)
    }

    /// Number of speculative backings this sidecar pre-built via
    /// `predict` trends since `spawn`.
    pub fn prewarms_issued(&self) -> u64 {
        self.prewarms_issued.load(Ordering::Relaxed)
    }

    /// Stop the sidecar thread and wait for it to exit.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            drop(h.join());
        }
    }
}

impl Drop for CapacityAdaptiveRingSidecar {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            drop(h.join());
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_anon_rejects_non_pow2() {
        let r = CapacityAdaptiveRing::create_anon(1, 1, 100);
        assert!(matches!(r, Err(CapacityMorphError::InvalidCapacity)));
    }

    #[test]
    fn create_anon_rejects_capacity_below_two() {
        let r = CapacityAdaptiveRing::create_anon(1, 1, 1);
        assert!(matches!(r, Err(CapacityMorphError::InvalidCapacity)));
    }

    #[test]
    fn anon_round_trip_after_create() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        let payload = [0xAAu8; 56];
        ring.try_send(0, &payload).unwrap();
        let mut out = [0u8; 64];
        let n = ring.try_recv(0, &mut out).unwrap();
        assert!(n >= 56);
        assert_eq!(&out[..56], &payload[..]);
    }

    #[test]
    fn morph_grow_preserves_in_flight_items() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        // Push 10 distinct items.
        for i in 0..10u64 {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }

        // Grow to 256 slots.
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.current_capacity(), 256);
        assert_eq!(ring.pin_generation(), 1);

        // Drain and verify every original item is present.
        let mut got = Vec::new();
        let mut out = [0u8; 64];
        while ring.try_recv(0, &mut out).is_ok() {
            let v = u64::from_le_bytes(out[..8].try_into().unwrap());
            got.push(v);
        }
        got.sort();
        assert_eq!(got, (0..10u64).collect::<Vec<_>>());
    }

    #[test]
    fn morph_shrink_with_room_succeeds() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 256).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        // 5 items in a 256-slot ring.
        for i in 0..5u64 {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }

        // Shrink to 64; 5 items fit easily.
        ring.morph_capacity_to(64).unwrap();
        assert_eq!(ring.current_capacity(), 64);

        let mut got = Vec::new();
        let mut out = [0u8; 64];
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        got.sort();
        assert_eq!(got, (0..5u64).collect::<Vec<_>>());
    }

    #[test]
    fn morph_shrink_with_more_in_flight_than_new_capacity_succeeds() {
        // Under the stale-list design, shrinks always succeed:
        // in-flight items physically stay in the old (larger)
        // AdaptiveRing as part of the stale list and the
        // consumer drains them via try_recv's stale-walk. The
        // new capacity governs only items pushed after the morph.
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        // Fill to 40 items in the 64-slot ring.
        for i in 0..40u64 {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }

        // Shrink to 16 slots. With the old in-flight items
        // sitting in the stale list, this succeeds without
        // touching them.
        ring.morph_capacity_to(16).expect("shrink succeeds via stale list");
        assert_eq!(ring.current_capacity(), 16);
        assert_eq!(ring.pin_generation(), 1);

        // The consumer drains all 40 original items via the
        // stale-list walk in try_recv. Order is send-order
        // because the producer pushed sequentially into the
        // original SPSC ring; the consumer pops in the same
        // order via try_recv's stale-first dispatch.
        let mut got = Vec::new();
        let mut out = [0u8; 64];
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        assert_eq!(got, (0..40u64).collect::<Vec<_>>(),
                   "all 40 original items drained via stale list in send-order");
    }

    #[test]
    fn morph_to_same_capacity_is_noop() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        let gen_before = ring.pin_generation();
        ring.morph_capacity_to(64).unwrap();
        assert_eq!(ring.pin_generation(), gen_before);
    }

    #[test]
    fn morph_rejects_non_pow2_target() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        let r = ring.morph_capacity_to(100);
        assert!(matches!(r, Err(CapacityMorphError::InvalidCapacity)));
    }

    #[test]
    fn pin_invalidates_after_morph() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        let pin = ring.pin_current_capacity();
        assert!(pin.is_still_valid());
        assert_eq!(pin.capacity(), 64);

        ring.morph_capacity_to(128).unwrap();
        assert!(!pin.is_still_valid());

        let pin2 = ring.pin_current_capacity();
        assert!(pin2.is_still_valid());
        assert_eq!(pin2.capacity(), 128);
    }

    #[test]
    fn multiple_grow_morphs_increment_generation_correctly() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 4).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        assert_eq!(ring.pin_generation(), 0);
        ring.morph_capacity_to(8).unwrap();
        assert_eq!(ring.pin_generation(), 1);
        ring.morph_capacity_to(16).unwrap();
        assert_eq!(ring.pin_generation(), 2);
        ring.morph_capacity_to(64).unwrap();
        assert_eq!(ring.pin_generation(), 3);
        assert_eq!(ring.current_capacity(), 64);
    }

    #[test]
    fn stamped_capacity_morph_preserves_ordering_axis() {
        let ring = CapacityAdaptiveRing::create_anon_stamped(2, 1, 64).unwrap();
        assert!(ring.is_stamped());
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();

        // Items pushed pre-morph...
        for i in 0..6u64 {
            let mut payload = [0u8; 48];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "the live mode flag must follow the capacity morph");
        // ...and post-morph items keep monotone stamps (the fresh
        // region is seeded from the old one).
        for i in 6..10u64 {
            let mut payload = [0u8; 48];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }

        // Stale-first walk + per-backing merge = send order.
        let mut out = [0u8; 64];
        let mut got = Vec::new();
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        assert_eq!(got, (0..10u64).collect::<Vec<_>>(),
                   "ordering must hold across the capacity morph boundary");
        assert_eq!(ring.inversions(), 0);
    }

    #[test]
    fn cross_thread_concurrent_send_recv_through_morphs() {
        use std::thread;

        let ring = Arc::new(CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap());
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        let n = 5_000u64;
        let r_prod = Arc::clone(&ring);
        let prod = thread::spawn(move || {
            for i in 0..n {
                let mut payload = [0u8; 56];
                payload[..8].copy_from_slice(&i.to_le_bytes());
                while r_prod.try_send(0, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let r_cons = Arc::clone(&ring);
        let cons = thread::spawn(move || {
            let mut got = Vec::with_capacity(n as usize);
            let mut out = [0u8; 64];
            while got.len() < n as usize {
                if r_cons.try_recv(0, &mut out).is_ok() {
                    got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
                }
            }
            got
        });

        // Morph thread: grow 64 -> 128 -> 256 -> 128 -> 64 during the run.
        // Shrinks always succeed under the stale-list design (items in
        // flight stay in the prior backing and the consumer drains them
        // via try_recv's stale-walk), so this loop never retries.
        let r_morph = Arc::clone(&ring);
        let morph = thread::spawn(move || {
            let targets = [128usize, 256, 128, 64];
            for t in targets {
                std::thread::sleep(std::time::Duration::from_micros(500));
                r_morph.morph_capacity_to(t).expect("morph succeeds");
            }
        });

        prod.join().unwrap();
        morph.join().unwrap();
        let mut got = cons.join().unwrap();
        got.sort();
        let expected: Vec<u64> = (0..n).collect();
        assert_eq!(got, expected);
    }

    // ============================================================
    // Warm-backing pre-allocation
    // ============================================================

    #[test]
    fn prewarm_hit_consumes_cache_and_morph_works() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        ring.prewarm(256).unwrap();
        assert_eq!(ring.warm_capacity(), Some(256));
        assert_eq!(ring.warm_hits(), 0);

        for i in 0..10u64 {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }

        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.warm_hits(), 1, "the morph must consume the prediction");
        assert_eq!(ring.warm_capacity(), None, "the slot is one-shot");
        assert_eq!(ring.current_capacity(), 256);
        assert_eq!(ring.pin_generation(), 1);

        // Post-hit ring is fully functional: in-flight items drain
        // in send order and new pushes land on the warm backing.
        ring.try_send(0, &[0xBBu8; 56]).unwrap();
        let mut out = [0u8; 64];
        let mut got = Vec::new();
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        assert_eq!(got.len(), 11);
        assert_eq!(&got[..10], &(0..10u64).collect::<Vec<_>>()[..]);
    }

    #[test]
    fn prewarm_mismatch_stays_cached_and_cold_path_runs() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        ring.prewarm(512).unwrap();
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.warm_hits(), 0, "mismatched prediction must not be consumed");
        assert_eq!(ring.warm_capacity(), Some(512), "mismatch stays cached");
        assert_eq!(ring.current_capacity(), 256);

        ring.morph_capacity_to(512).unwrap();
        assert_eq!(ring.warm_hits(), 1, "the cached 512 serves the later morph");
        assert_eq!(ring.warm_capacity(), None);
    }

    #[test]
    fn prewarm_rejects_non_pow2() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        assert!(matches!(ring.prewarm(100), Err(CapacityMorphError::InvalidCapacity)));
        assert!(matches!(ring.prewarm(1), Err(CapacityMorphError::InvalidCapacity)));
        assert_eq!(ring.warm_capacity(), None);
    }

    #[test]
    fn prewarm_same_capacity_is_idempotent_and_clear_drops() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.prewarm(128).unwrap();
        ring.prewarm(128).unwrap();
        assert_eq!(ring.warm_capacity(), Some(128));
        ring.prewarm(256).unwrap();
        assert_eq!(ring.warm_capacity(), Some(256), "new prediction replaces the old");
        ring.clear_warm();
        assert_eq!(ring.warm_capacity(), None);
    }

    #[test]
    fn warm_morph_preserves_stamps_and_shape() {
        let ring = CapacityAdaptiveRing::create_anon_stamped(2, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();
        ring.ring_handle()
            .morph_to(crate::adaptive_ring::RingShape::Mpsc)
            .unwrap();

        for i in 0..6u64 {
            let mut payload = [0u8; 48];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }

        ring.prewarm(256).unwrap();
        ring.morph_capacity_to(256).unwrap();
        assert_eq!(ring.warm_hits(), 1);
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "live mode flag must follow a warm-hit morph");
        assert_eq!(ring.ring_handle().current_shape(),
                   crate::adaptive_ring::RingShape::Mpsc,
                   "shape must be mirrored onto the warm backing");

        for i in 6..10u64 {
            let mut payload = [0u8; 48];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &payload).unwrap();
        }
        let mut out = [0u8; 64];
        let mut got = Vec::new();
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        assert_eq!(got, (0..10u64).collect::<Vec<_>>(),
                   "send order must hold across a warm-hit morph (seeded stamps)");
        assert_eq!(ring.inversions(), 0);
    }

    #[test]
    fn warm_morph_file_locale_round_trips() {
        let dir = std::env::temp_dir().join(format!(
            "subetha_warm_file_{}", std::process::id(),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("warm_probe");
        {
            let ring = CapacityAdaptiveRing::create(&base, 1, 1, 64).unwrap();
            ring.register_producer().unwrap();
            ring.register_consumer().unwrap();
            for i in 0..5u64 {
                let mut payload = [0u8; 56];
                payload[..8].copy_from_slice(&i.to_le_bytes());
                ring.try_send(0, &payload).unwrap();
            }
            ring.prewarm(128).unwrap();
            ring.morph_capacity_to(128).unwrap();
            assert_eq!(ring.warm_hits(), 1);
            // Cycle back down through a second prewarm at a
            // previously-used capacity - the per-build sequence
            // number keeps the file names unique.
            ring.prewarm(64).unwrap();
            ring.morph_capacity_to(64).unwrap();
            assert_eq!(ring.warm_hits(), 2);
            let mut out = [0u8; 64];
            let mut got = Vec::new();
            while ring.try_recv(0, &mut out).is_ok() {
                got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
            }
            assert_eq!(got, (0..5u64).collect::<Vec<_>>());
        }
        drop(std::fs::remove_dir_all(&dir));
    }

    #[test]
    fn default_policy_predict_bands() {
        let policy = DefaultCapacityPolicy::default(); // grow 0.85 / shrink 0.10
        let obs = |len: usize, cap: usize| CapacityPolicyObservation {
            current_capacity: cap,
            active_approx_len: len,
            total_slot_capacity: cap,
            since_last_morph: std::time::Duration::ZERO,
        };
        // 0.70 fill >= 0.6375 trend band -> predict double.
        assert_eq!(policy.predict(&obs(716, 1024)), Some(2048));
        // 0.50 fill sits between the bands -> no prediction.
        assert_eq!(policy.predict(&obs(512, 1024)), None);
        // 0.14 fill <= 0.15 trend band -> predict half.
        assert_eq!(policy.predict(&obs(143, 1024)), Some(512));
        // Caps respected at the ladder ends.
        assert_eq!(policy.predict(&obs(60000, 65536)), None);
        assert_eq!(policy.predict(&obs(0, 64)), None);
        // predict ignores hysteresis (decide does not).
        let fresh = CapacityPolicyObservation {
            since_last_morph: std::time::Duration::ZERO,
            ..obs(716, 1024)
        };
        assert_eq!(policy.decide(&fresh), None, "decide is hysteresis-gated");
        assert_eq!(policy.predict(&fresh), Some(2048), "predict is not");
    }

    #[test]
    fn policy_without_predict_override_never_prewarms() {
        struct GrowOnly;
        impl CapacityPolicy for GrowOnly {
            fn decide(&self, obs: &CapacityPolicyObservation) -> Option<usize> {
                (obs.fill_ratio() >= 0.85).then_some(obs.current_capacity * 2)
            }
        }
        let ring = Arc::new(CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap());
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        let sidecar = CapacityAdaptiveRingSidecar::spawn(
            Arc::clone(&ring), GrowOnly, std::time::Duration::from_millis(2),
        );
        // Hold fill high enough that a predicting policy is sure
        // to act; the trait-default one must not.
        for _ in 0..50 {
            ring.try_send(0, &[0u8; 56]).ok();
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(sidecar.prewarms_issued(), 0,
                   "trait-default predict() must keep today's behavior");
        sidecar.shutdown();
    }

    #[test]
    fn sidecar_prewarms_on_sustained_trend_then_morph_hits_warm() {
        // Deterministic test policy: predict fires in a band below
        // the decide threshold, so the test controls each stage by
        // fill level alone.
        struct Banded;
        impl CapacityPolicy for Banded {
            fn decide(&self, obs: &CapacityPolicyObservation) -> Option<usize> {
                (obs.fill_ratio() >= 0.85).then_some(obs.current_capacity * 2)
            }
            fn predict(&self, obs: &CapacityPolicyObservation) -> Option<usize> {
                (obs.fill_ratio() >= 0.60).then_some(obs.current_capacity * 2)
            }
        }
        let ring = Arc::new(CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap());
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        let sidecar = CapacityAdaptiveRingSidecar::spawn(
            Arc::clone(&ring), Banded, std::time::Duration::from_millis(2),
        );

        // Stage 1: fill into the predict band (45/64 = 0.70) and
        // wait for the sustained-trend prewarm.
        for _ in 0..45 {
            ring.try_send(0, &[0u8; 56]).unwrap();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while ring.warm_capacity() != Some(128) {
            assert!(std::time::Instant::now() < deadline,
                    "sidecar must prewarm 128 from the sustained trend");
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(sidecar.prewarms_issued() >= 1);

        // Stage 2: push over the decide threshold (56/64 = 0.875)
        // and wait for the morph to consume the warm backing.
        for _ in 0..11 {
            ring.try_send(0, &[0u8; 56]).unwrap();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while ring.current_capacity() != 128 {
            assert!(std::time::Instant::now() < deadline,
                    "sidecar must morph to 128 once fill crosses decide");
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(ring.warm_hits(), 1,
                   "the sidecar-driven morph must consume the prewarmed backing");
        sidecar.shutdown();

        // Integrity: every pushed item drains.
        let mut out = [0u8; 64];
        let mut n = 0;
        while ring.try_recv(0, &mut out).is_ok() {
            n += 1;
        }
        assert_eq!(n, 56);
    }

    // ============================================================
    // Compound multi-axis morphs
    // ============================================================

    #[test]
    fn compound_capacity_plus_shape_is_one_generation() {
        let ring = CapacityAdaptiveRing::create_anon(4, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        for i in 0..10u64 {
            let mut p = [0u8; 56];
            p[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &p).unwrap();
        }

        ring.morph_to_config(&RingConfig {
            shape: Some(RingShape::Mpmc),
            capacity: Some(512),
            locale: None,
        })
        .unwrap();
        assert_eq!(ring.pin_generation(), 1,
                   "two axes, ONE pin invalidation");
        assert_eq!(ring.current_capacity(), 512);
        assert_eq!(ring.ring_handle().current_shape(), RingShape::Mpmc);

        // Three more producers join post-morph and everything
        // drains exactly once.
        for _ in 0..3 {
            ring.register_producer().unwrap();
        }
        for pid in 1..4usize {
            let mut p = [0u8; 56];
            p[..8].copy_from_slice(&(100 + pid as u64).to_le_bytes());
            ring.try_send(pid, &p).unwrap();
        }
        let mut out = [0u8; 64];
        let mut got = Vec::new();
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        got.sort();
        let mut expected: Vec<u64> = (0..10).collect();
        expected.extend([101, 102, 103]);
        assert_eq!(got, expected);
    }

    #[test]
    fn shape_only_config_morphs_in_place_without_pin_bump() {
        let ring = CapacityAdaptiveRing::create_anon(4, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        ring.try_send(0, &[0x11u8; 56]).unwrap();

        let active_before = ring.ring_handle();
        ring.morph_to_config(&RingConfig {
            shape: Some(RingShape::Mpsc),
            ..RingConfig::default()
        })
        .unwrap();
        assert_eq!(ring.pin_generation(), 0,
                   "in-place shape morph must not invalidate the capacity pin");
        assert!(Arc::ptr_eq(&active_before, &ring.ring_handle()),
                "active backing must be the same instance");
        assert_eq!(ring.ring_handle().current_shape(), RingShape::Mpsc);
        let mut out = [0u8; 64];
        assert!(ring.try_recv(0, &mut out).is_ok(),
                "in-flight item survives the in-place shape morph");
    }

    #[test]
    fn config_noop_when_every_axis_matches() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.morph_to_config(&RingConfig::default()).unwrap();
        ring.morph_to_config(&RingConfig {
            shape: Some(RingShape::Spsc),
            capacity: Some(64),
            locale: Some(BackingTarget::Anon),
        })
        .unwrap();
        assert_eq!(ring.pin_generation(), 0);
    }

    #[test]
    fn compound_locale_change_drains_across_locales() {
        let dir = std::env::temp_dir().join(format!(
            "subetha_compound_locale_{}", std::process::id(),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("compound");
        {
            let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
            ring.register_producer().unwrap();
            ring.register_consumer().unwrap();
            for i in 0..8u64 {
                let mut p = [0u8; 56];
                p[..8].copy_from_slice(&i.to_le_bytes());
                ring.try_send(0, &p).unwrap();
            }

            // Capacity + locale in one transition: anon -> file.
            ring.morph_to_config(&RingConfig {
                shape: None,
                capacity: Some(256),
                locale: Some(BackingTarget::File(base.clone())),
            })
            .unwrap();
            assert_eq!(ring.pin_generation(), 1);
            assert_eq!(ring.current_capacity(), 256);

            // Items pushed pre-morph (anon) and post-morph (file)
            // drain in send order across the locale boundary.
            ring.try_send(0, &{
                let mut p = [0u8; 56];
                p[..8].copy_from_slice(&99u64.to_le_bytes());
                p
            }).unwrap();
            let mut out = [0u8; 64];
            let mut got = Vec::new();
            while ring.try_recv(0, &mut out).is_ok() {
                got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
            }
            let mut expected: Vec<u64> = (0..8).collect();
            expected.push(99);
            assert_eq!(got, expected);

            // Subsequent morphs allocate at the retargeted locale.
            ring.morph_capacity_to(512).unwrap();
            let file_backings: Vec<_> = std::fs::read_dir(&dir).unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().contains("cap_512"))
                .collect();
            assert!(!file_backings.is_empty(),
                    "post-retarget morphs must allocate file backings");
        }
        drop(std::fs::remove_dir_all(&dir));
    }

    #[test]
    fn repatch_prewarm_config_full_target_hits() {
        let ring = CapacityAdaptiveRing::create_anon(4, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        let target = RingConfig {
            shape: Some(RingShape::Mpmc),
            capacity: Some(1024),
            locale: None,
        };
        ring.prewarm_config(&target).unwrap();
        assert_eq!(ring.warm_capacity(), Some(1024));
        ring.morph_to_config(&target).unwrap();
        assert_eq!(ring.warm_hits(), 1,
                   "repatch must consume the full-target prediction");
        assert_eq!(ring.ring_handle().current_shape(), RingShape::Mpmc);
        assert_eq!(ring.current_capacity(), 1024);
    }

    #[test]
    fn warm_key_locale_mismatch_is_cold() {
        let dir = std::env::temp_dir().join(format!(
            "subetha_warm_locale_key_{}", std::process::id(),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        {
            let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
            // Prewarm at the current (anon) locale...
            ring.prewarm(256).unwrap();
            // ...then morph to the same capacity at another
            // locale: the key mismatch must force the cold path.
            ring.morph_to_config(&RingConfig {
                shape: None,
                capacity: Some(256),
                locale: Some(BackingTarget::File(dir.join("keyed"))),
            })
            .unwrap();
            assert_eq!(ring.warm_hits(), 0,
                       "an anon-built backing must never serve a file-locale morph");
            assert_eq!(ring.warm_capacity(), Some(256),
                       "the mismatched prediction stays cached");
            ring.clear_warm();
        }
        drop(std::fs::remove_dir_all(&dir));
    }

    #[test]
    fn stale_pops_counts_transition_items() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        for i in 0..7u64 {
            let mut p = [0u8; 56];
            p[..8].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &p).unwrap();
        }
        ring.morph_capacity_to(256).unwrap();
        ring.try_send(0, &[0x22u8; 56]).unwrap();
        let mut out = [0u8; 64];
        while ring.try_recv(0, &mut out).is_ok() {}
        assert_eq!(ring.stale_pops(), 7,
                   "exactly the pre-morph items traverse the stale walk");
    }

    /// A producer pushes into the active backing of the state it
    /// loaded, and that push can land after two morphs: one that made
    /// the backing stale and one that would have pruned it as empty.
    /// The snapshot the producer holds is what keeps the backing on
    /// the stale list, so the consumer's stale walk still reaches the
    /// late item.
    #[test]
    fn a_late_push_into_a_snapshot_two_morphs_old_is_still_delivered() {
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 64).unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();

        // The producer's view of the ring, loaded before either morph
        // exactly as `try_send` loads it.
        let held = ring.state.load();
        ring.morph_capacity_to(128).expect("the first morph");
        ring.morph_capacity_to(256).expect("the second morph");

        let mut payload = [0u8; 56];
        payload[..8].copy_from_slice(&7u64.to_le_bytes());
        held.active
            .try_send(0, &payload)
            .expect("the late push lands in the backing the producer loaded");
        drop(held);

        let mut out = [0u8; 64];
        assert!(
            ring.try_recv(0, &mut out).is_ok(),
            "the item pushed into a backing two morphs old is unreachable: the \
             second morph pruned that backing from the stale list while the \
             producer still held the state naming it"
        );
        assert_eq!(&out[..8], &7u64.to_le_bytes());
        assert!(ring.try_recv(0, &mut out).is_err(), "nothing else was pushed");
    }

    /// One item from each of two producers after a morph: the backing in
    /// place counts both, so it is not a single-producer ring with two
    /// writers, and both items come out.
    fn sends_from_both_arrive(ring: &CapacityAdaptiveRing, first: usize, late: usize, consumer: usize) {
        assert_ne!(first, late, "two producers were handed one id");
        for (id, value) in [(first, 1u64), (late, 2u64)] {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&value.to_le_bytes());
            if let Err(e) = ring.try_send(id, &payload) {
                panic!("producer {id}'s send after the morph was refused: {e:?}");
            }
        }
        assert_eq!(
            ring.ring_handle().active_producers(),
            2,
            "the backing in place counts one producer while two send into it"
        );
        let mut got = Vec::new();
        let mut out = [0u8; 64];
        while ring.try_recv(consumer, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        got.sort_unstable();
        assert_eq!(got, vec![1, 2], "an item a producer sent after the morph is missing");
    }

    /// A producer that registers while a morph has built its backing and
    /// not yet published it is carried onto that backing.
    #[test]
    fn a_producer_registering_inside_a_morph_is_carried_onto_the_new_backing() {
        let ring = Arc::new(CapacityAdaptiveRing::create_anon(2, 1, 64).unwrap());
        let first = ring.register_producer().unwrap();
        let consumer = ring.register_consumer().unwrap();
        let morphing = Arc::clone(&ring);
        let (pause, morph) = crate::test_races::stopped(move || morphing.morph_capacity_to(128));
        let late = ring.register_producer().expect("a registration inside the morph");
        pause.release();
        morph.join().expect("the morph thread").expect("the morph");
        sends_from_both_arrive(&ring, first, late, consumer);
    }

    /// A producer stopped before it registers, while a morph publishes a
    /// backing and a handle on it settles it: the registration lands on
    /// the backing already replaced, finds it sealed, and is made again on
    /// the backing in place.
    #[test]
    fn a_producer_registering_on_a_replaced_backing_registers_on_the_new_one() {
        let ring = Arc::new(CapacityAdaptiveRing::create_anon(2, 1, 64).unwrap());
        let first = ring.register_producer().unwrap();
        let consumer = ring.register_consumer().unwrap();
        let registering = Arc::clone(&ring);
        let (pause, registrant) = crate::test_races::stopped(move || registering.register_producer());
        ring.morph_capacity_to(128).expect("the morph");
        assert_eq!(ring.ring_handle().active_producers(), 1, "only the first producer is registered yet");
        pause.release();
        let late = registrant.join().expect("the registrant thread").expect("the registration");
        sends_from_both_arrive(&ring, first, late, consumer);
    }
}
