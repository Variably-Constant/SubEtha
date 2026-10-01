//! Sidecar control plane for adaptive primitives.
//!
//! One background thread per detected NUMA node; each thread polls
//! the registered primitive instances bound to its node:
//!
//! 1. Drains each instance's [`ObservationRing`] into a per-instance
//!    [`InstanceStats`] accumulator.
//! 2. Asks the instance's [`Policy`] whether a strategy migration is
//!    warranted.
//! 3. If yes, calls the instance's [`AdaptiveInstance::apply_migration`],
//!    whose default sets the tag with [`HandshakeHeader::set_tag`], or
//!    sets the tag itself for a raw registration.
//!
//! A primitive whose change of strategy moves data can do the move in
//! its own `apply_migration`. `subetha_cxc::AdaptiveIpc` moves between
//! families outside the sidecar, in `migrate_to`, which its
//! `maybe_promote` calls.
//!
//! # Safety model
//!
//! Registration takes raw pointers to the user's `HandshakeHeader` and
//! `ObservationRing`. The contract is:
//!
//! - The user must keep these alive until `unregister` returns.
//! - `unregister` blocks until any in-flight scan finishes, so the user
//!   can drop the underlying memory immediately after.
//!
//! The [`SidecarBox<T>`] wrapper enforces this contract by holding a
//! `Box<T>` (stable address) alongside an auto-unregistering
//! [`SidecarHandle`].

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use subetha_core::SwapCell;
use subetha_core::{HandshakeHeader, ObservationRing};
use once_cell::sync::Lazy;

pub mod bench_safe;

/// Stable identifier for a registered primitive instance.
pub type InstanceId = u32;

/// Number of op_kind slots the InstanceStats tracks. Primitives use
/// `op_kind` values 1..N to identify per-op-kind buckets (e.g., get
/// vs set for `SharedCell`; insert/get/remove for `SharedHashMap`).
/// Op kind 0 is reserved for "unspecified".
pub const N_OP_KINDS: usize = 8;

/// Maximum distinct producer thread ids tracked per op kind. Picked
/// to be cheap (4*8*4 = 128 bytes per instance) while sufficient for
/// the policy decisions that read this - once cardinality crosses 1,
/// the policy migrates regardless of the exact count.
pub const MAX_TRACKED_THREADS_PER_KIND: usize = 4;

/// Aggregated statistics for one registered instance.
///
/// Updated by the sidecar each poll cycle from drained observations.
#[derive(Debug, Clone, Copy)]
pub struct InstanceStats {
    pub ops_observed: u64,
    pub total_latency_ticks: u64,
    pub contention_ops: u64,
    /// Per-op-kind counts. Index by `Observation.op_kind` (clamped to
    /// the valid range). Primitives that adapt based on a ratio of
    /// op kinds (e.g., reads vs writes) consume these.
    pub op_kind_counts: [u64; N_OP_KINDS],
    /// Microseconds from registration to the last scan that drained an
    /// observation from this instance.
    pub last_drain_us: u64,
    /// Number of migrations the sidecar has triggered on this instance:
    /// the scans whose policy returned a tag other than the current one.
    /// Used by bench harnesses to measure adaptation-latency convergence.
    pub migrations_triggered: u64,
    /// Per-op-kind distinct-thread-id cache. Filled lazily by the
    /// drain as observations arrive. Once a slot fills with a tid
    /// that doesn't match any earlier slot, the corresponding count
    /// in `per_op_kind_distinct_count` increments.
    pub per_op_kind_distinct_threads: [[u32; MAX_TRACKED_THREADS_PER_KIND]; N_OP_KINDS],
    /// Distinct thread count observed per op_kind (saturates at
    /// `MAX_TRACKED_THREADS_PER_KIND + 1` - meaning "more than the
    /// slot table can hold"). `>= 2` is the typical multi-producer
    /// or multi-consumer detection threshold for primitive policies.
    pub per_op_kind_distinct_count: [u8; N_OP_KINDS],
}

impl Default for InstanceStats {
    fn default() -> Self {
        Self {
            ops_observed: 0,
            total_latency_ticks: 0,
            contention_ops: 0,
            op_kind_counts: [0; N_OP_KINDS],
            last_drain_us: 0,
            migrations_triggered: 0,
            per_op_kind_distinct_threads: [[0; MAX_TRACKED_THREADS_PER_KIND]; N_OP_KINDS],
            per_op_kind_distinct_count: [0; N_OP_KINDS],
        }
    }
}

impl InstanceStats {
    pub fn average_latency_ticks(&self) -> u64 {
        self.total_latency_ticks.checked_div(self.ops_observed).unwrap_or(0)
    }

    pub fn contention_rate(&self) -> f64 {
        if self.ops_observed == 0 {
            0.0
        } else {
            self.contention_ops as f64 / self.ops_observed as f64
        }
    }

    /// Total ops observed across all op kinds. Equals `ops_observed`
    /// for primitives that always set a non-zero op_kind on their
    /// observations.
    pub fn op_kind_total(&self) -> u64 {
        self.op_kind_counts.iter().sum()
    }

    /// Ratio of one op kind to the total of two op kinds. Returns 0.0
    /// when both counts are zero (avoids divide-by-zero in policies).
    pub fn ratio_of(&self, kind: u16, total_kinds: &[u16]) -> f64 {
        let k = (kind as usize).min(N_OP_KINDS - 1);
        let kind_count = self.op_kind_counts[k];
        let total: u64 = total_kinds.iter()
            .map(|&i| self.op_kind_counts[(i as usize).min(N_OP_KINDS - 1)])
            .sum();
        if total == 0 {
            0.0
        } else {
            kind_count as f64 / total as f64
        }
    }

    /// Distinct producer-thread count observed for one op kind.
    ///
    /// `>= 2` indicates true multi-producer (or multi-consumer for the
    /// recv-side op kind) usage on this primitive. Saturates at
    /// `MAX_TRACKED_THREADS_PER_KIND + 1`.
    pub fn distinct_threads_for(&self, kind: u16) -> u8 {
        let k = (kind as usize).min(N_OP_KINDS - 1);
        self.per_op_kind_distinct_count[k]
    }

    /// True when the given op kind has been observed from more than
    /// one distinct producer thread.
    pub fn is_multi_thread_for(&self, kind: u16) -> bool {
        self.distinct_threads_for(kind) >= 2
    }
}

/// Internal helper: record a thread_id against `(op_kind, stats)`,
/// updating `per_op_kind_distinct_threads` + `per_op_kind_distinct_count`
/// when the tid hasn't been seen for that op kind. No-op when tid is 0
/// (the unspecified sentinel) or the saturation cap is already hit.
#[inline]
fn record_thread_for_op(
    stats: &mut InstanceStats,
    op_kind: u16,
    tid: u32,
) {
    if tid == 0 {
        return;
    }
    let k = (op_kind as usize).min(N_OP_KINDS - 1);
    let count = stats.per_op_kind_distinct_count[k];
    if (count as usize) > MAX_TRACKED_THREADS_PER_KIND {
        // Already saturated: we know cardinality > MAX_TRACKED; we
        // don't add slots beyond the cache size, and the count remains
        // pinned at the saturation value.
        return;
    }
    let slots = &mut stats.per_op_kind_distinct_threads[k];
    // Linear scan over the populated slots; tids are inserted in
    // arrival order so the count is exactly the number of populated
    // slots when below saturation.
    let n = (count as usize).min(MAX_TRACKED_THREADS_PER_KIND);
    if slots[..n].contains(&tid) {
        return;
    }
    // New tid: either append to the cache (when there's room) or just
    // bump the saturated count to MAX_TRACKED_THREADS_PER_KIND + 1 (when full).
    if n < MAX_TRACKED_THREADS_PER_KIND {
        slots[n] = tid;
        stats.per_op_kind_distinct_count[k] = (n as u8) + 1;
    } else {
        // Saturation transition: count moves from the cap to cap + 1; we
        // know "more threads than the cache can hold" without
        // remembering which ones.
        stats.per_op_kind_distinct_count[k] = (MAX_TRACKED_THREADS_PER_KIND as u8) + 1;
    }
}

/// Decides when and how to migrate a primitive instance's strategy.
///
/// Called by the sidecar after each scan iteration that observed
/// at least one new op. Return `Some(new_tag)` to install a new
/// strategy via [`HandshakeHeader::set_tag`]; `None` to leave it alone.
pub trait Policy: Send + Sync + 'static {
    fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32>;
}

/// Convenient policy that always returns the same tag (testing).
pub struct FixedPolicy(pub u32);
impl Policy for FixedPolicy {
    fn decide(&self, _stats: &InstanceStats, _current_tag: u32) -> Option<u32> {
        Some(self.0)
    }
}

/// Policy that never migrates. The policy of every `subetha-cxc`
/// primitive that registers without an adaptation rule of its own.
pub struct NoMigrationPolicy;
impl Policy for NoMigrationPolicy {
    fn decide(&self, _stats: &InstanceStats, _current_tag: u32) -> Option<u32> {
        None
    }
}

struct Registration {
    header: NonNull<HandshakeHeader>,
    ring: NonNull<ObservationRing>,
    /// Optional pointer to the registered instance via its trait object.
    /// When present, the sidecar calls `apply_migration` on it after the
    /// policy returns a new tag; when absent (raw registration without
    /// a known instance), the sidecar falls back to `header.set_tag`.
    instance: Option<NonNull<dyn AdaptiveInstance>>,
    policy: Box<dyn Policy>,
    /// Written by the node's scan thread alone and read whole by
    /// [`Sidecar::stats`] from any thread.
    stats: SwapCell<InstanceStats>,
    registered_at: Instant,
}

// SAFETY: The contract requires `header`, `ring`, and the optional
// `instance` pointer to be valid for the lifetime of this Registration
// (i.e., until unregister returns). The sidecar touches them only while
// counted in the slot's `users`, which unregister waits out.
unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

/// Safety cap on drained observations per scan iteration per instance.
///
/// A scan drains the whole ring when it can (no sampling bias from FIFO
/// order), and a ring holds 4096. Producers pushing while the scan
/// drains can keep a ring from emptying; the cap stops one busy
/// instance from holding the scan and starving the others.
///
/// Worst-case per-scan cost: 8192 observations * ~10 ns drain cost =
/// ~80 us per instance, which an `unregister` of that instance waits
/// out. With 100 instances this caps a single scan loop at ~8 ms.
const DRAIN_SAFETY_CAP: usize = 8192;

/// Sidecar poll interval. Trade-off: shorter = faster reaction to
/// transitions; longer = less CPU spent on cold/idle instances.
const POLL_INTERVAL: Duration = Duration::from_micros(200);

/// A slot with no registration.
const SLOT_FREE: u32 = 0;
/// A registration being written into the slot by the register that
/// claimed it.
const SLOT_FILLING: u32 = 1;
/// A registration the node's scan and [`Sidecar::stats`] may enter.
const SLOT_LIVE: u32 = 2;
/// A registration being removed: nothing new enters, and the unregister
/// that set it waits for what is inside to leave.
const SLOT_RETIRING: u32 = 3;

/// One registration's place in a node.
struct Slot {
    state: AtomicU32,
    /// Scans and stats reads inside the registration, plus readers
    /// backing out after finding the slot not live.
    users: AtomicU32,
    reg: UnsafeCell<MaybeUninit<Registration>>,
}

// SAFETY: `reg` is written only by the register that moved `state` from
// SLOT_FREE to SLOT_FILLING, read only by a caller counted in `users`
// that then saw SLOT_LIVE, and dropped only by the unregister that moved
// SLOT_LIVE to SLOT_RETIRING and then saw `users` at zero.
unsafe impl Sync for Slot {}

impl Slot {
    fn new() -> Self {
        Self {
            state: AtomicU32::new(SLOT_FREE),
            users: AtomicU32::new(0),
            reg: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Enter the slot's registration, or `None` when it holds no live
    /// one. The count goes up before the state is read, and unregister
    /// sets the state before it reads the count, both sequentially
    /// consistent, so an unregister either waits for this caller or this
    /// caller sees it retiring.
    fn enter(&self) -> Option<Entered<'_>> {
        self.users.fetch_add(1, Ordering::SeqCst);
        if self.state.load(Ordering::SeqCst) == SLOT_LIVE {
            Some(Entered(self))
        } else {
            self.users.fetch_sub(1, Ordering::Release);
            None
        }
    }

    /// Remove the slot's live registration, waiting for every caller
    /// inside it to leave. False when the slot held none.
    fn retire(&self) -> bool {
        if self
            .state
            .compare_exchange(SLOT_LIVE, SLOT_RETIRING, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        while self.users.load(Ordering::SeqCst) != 0 {
            thread::yield_now();
        }
        // SAFETY: the slot was live, so `reg` is initialized; it is
        // retiring, so nothing new enters, and `users` reached zero, so
        // nothing is inside.
        unsafe { (*self.reg.get()).assume_init_drop() };
        self.state.store(SLOT_FREE, Ordering::Release);
        true
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if *self.state.get_mut() == SLOT_LIVE {
            // SAFETY: a live slot holds an initialized registration, and
            // `&mut self` means nothing else can reach it.
            unsafe { self.reg.get_mut().assume_init_drop() };
        }
    }
}

/// A caller counted inside a live slot.
struct Entered<'a>(&'a Slot);

impl Entered<'_> {
    fn registration(&self) -> &Registration {
        // SAFETY: `enter` saw the slot live after counting this caller,
        // and an unregister waits for the count before dropping it.
        unsafe { (*self.0.reg.get()).assume_init_ref() }
    }
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        self.0.users.fetch_sub(1, Ordering::Release);
    }
}

/// Slots per segment of a node's registrations.
const SLOTS_PER_SEGMENT: usize = 64;

/// A run of slots. A node's segments are appended with a compare-and-
/// swap as registrations outgrow them and are freed with the node.
struct Segment {
    slots: [Slot; SLOTS_PER_SEGMENT],
    next: AtomicPtr<Segment>,
}

impl Segment {
    fn new() -> Self {
        Self { slots: std::array::from_fn(|_| Slot::new()), next: AtomicPtr::new(ptr::null_mut()) }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // Freed one at a time rather than by each segment dropping the
        // next, so a long chain does not recurse.
        let mut next = std::mem::replace(self.next.get_mut(), ptr::null_mut());
        while !next.is_null() {
            // SAFETY: every `next` came from Box::into_raw in
            // `claim_slot` and is owned by the segment before it.
            let mut segment = unsafe { Box::from_raw(next) };
            next = std::mem::replace(segment.next.get_mut(), ptr::null_mut());
        }
    }
}

/// A single node's registrations and scanning thread. One per NUMA
/// node.
///
/// The node's thread is the only consumer of every observation ring
/// registered here, so no two scans ever pop one ring together;
/// [`Sidecar::scan_now`] asks this thread for a scan and waits for it
/// rather than draining a ring itself.
struct NodeSidecar {
    first: Segment,
    /// Scans `scan_now` has asked for.
    requested: AtomicU64,
    /// The last request a finished scan covers. A scan covers every
    /// request made before it read `requested`.
    completed: AtomicU64,
    /// The node's scan thread, for `scan_now` to wake. Set once when the
    /// thread is spawned.
    thread: AtomicPtr<Thread>,
    /// Set as the node's scan thread returns or unwinds.
    exited: AtomicBool,
}

impl NodeSidecar {
    fn new() -> Self {
        Self {
            first: Segment::new(),
            requested: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            thread: AtomicPtr::new(ptr::null_mut()),
            exited: AtomicBool::new(false),
        }
    }

    fn segments(&self) -> impl Iterator<Item = &Segment> {
        let mut at: *const Segment = &self.first;
        std::iter::from_fn(move || {
            if at.is_null() {
                return None;
            }
            // SAFETY: `at` is the node's first segment or one a
            // `claim_slot` appended; segments live as long as the node.
            let segment = unsafe { &*at };
            at = segment.next.load(Ordering::Acquire);
            Some(segment)
        })
    }

    fn slot(&self, index: usize) -> Option<&Slot> {
        self.segments()
            .nth(index / SLOTS_PER_SEGMENT)
            .map(|segment| &segment.slots[index % SLOTS_PER_SEGMENT])
    }

    /// Take a free slot for a registration being written, appending a
    /// segment when every slot is taken. Returns its index.
    fn claim_slot(&self) -> (usize, &Slot) {
        let mut base = 0;
        let mut segment: &Segment = &self.first;
        loop {
            for (i, slot) in segment.slots.iter().enumerate() {
                if slot
                    .state
                    .compare_exchange(SLOT_FREE, SLOT_FILLING, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return (base + i, slot);
                }
            }
            base += SLOTS_PER_SEGMENT;
            let next = segment.next.load(Ordering::Acquire);
            segment = if next.is_null() {
                let fresh = Box::into_raw(Box::new(Segment::new()));
                match segment.next.compare_exchange(
                    ptr::null_mut(),
                    fresh,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    // SAFETY: `fresh` is now owned by `segment.next`.
                    Ok(_) => unsafe { &*fresh },
                    Err(appended) => {
                        // SAFETY: `fresh` was not installed, so this is
                        // its only owner; `appended` is another
                        // register's segment, owned by `segment.next`.
                        drop(unsafe { Box::from_raw(fresh) });
                        unsafe { &*appended }
                    }
                }
            } else {
                // SAFETY: an appended segment lives as long as the node.
                unsafe { &*next }
            };
        }
    }

    fn wake(&self) {
        let thread = self.thread.load(Ordering::Acquire);
        if !thread.is_null() {
            // SAFETY: set once from a boxed Thread that lives as long as
            // the node.
            unsafe { &*thread }.unpark();
        }
    }

    fn is_own_thread(&self) -> bool {
        let thread = self.thread.load(Ordering::Acquire);
        // SAFETY: as in `wake`.
        !thread.is_null() && unsafe { &*thread }.id() == thread::current().id()
    }
}

impl Drop for NodeSidecar {
    fn drop(&mut self) {
        let thread = std::mem::replace(self.thread.get_mut(), ptr::null_mut());
        if !thread.is_null() {
            // SAFETY: set once from Box::into_raw in `Sidecar::new`.
            drop(unsafe { Box::from_raw(thread) });
        }
    }
}

/// Sets its node's `exited` flag when the scan thread returns or
/// unwinds, so a `scan_now` waiting on the node stops waiting.
struct ExitMark<'a>(&'a AtomicBool);

impl Drop for ExitMark<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// The sidecar singleton. Internally a pool of one `NodeSidecar` per
/// detected NUMA node; each node has its own scanning thread and slots
/// of registered primitive instances. Registration routes by
/// `current_numa_node()` so cross-NUMA cache traffic on the scan path
/// stays minimal. InstanceId encodes (node_index, slot) so unregister
/// + stats can find the right node's slot.
pub struct Sidecar {
    nodes: Vec<NodeSidecar>,
    shutdown: Arc<AtomicBool>,
    /// The scan threads, boxed: stored once after they are spawned and
    /// taken once by whichever of `Drop` and the exit hook stops them.
    join_handles: AtomicPtr<Vec<JoinHandle<()>>>,
    /// Currently-registered instance count. Incremented in
    /// `register_raw`, decremented in `unregister`. Read via
    /// [`Sidecar::instance_count`].
    instance_count: AtomicUsize,
    /// Hard cap on simultaneously-registered instances. When
    /// `register_raw` would cross this, it panics with a diagnostic.
    /// The default ([`DEFAULT_MAX_INSTANCES`]) covers all realistic
    /// production workloads; raise via [`Sidecar::set_max_instances`]
    /// when intentional heavy registration is needed.
    max_instances: AtomicUsize,
}

/// Default hard cap on registered instances. Sized to fail fast on
/// the "bench creates a SidecarBox per `b.iter()`" mistake, which
/// exhausts the host near 94k registrations: this cap refuses an
/// order of magnitude before that, while leaving room above the
/// 10..1000 range production workloads sit in.
pub const DEFAULT_MAX_INSTANCES: usize = 10_000;

/// Number of bits in InstanceId reserved for the node index (upper).
const NODE_ID_BITS: u32 = 8;
/// Mask for the slot portion of InstanceId (lower).
const SLOT_MASK: u32 = (1 << (32 - NODE_ID_BITS)) - 1;

fn pack_id(node: u32, slot: u32) -> InstanceId {
    (node << (32 - NODE_ID_BITS)) | (slot & SLOT_MASK)
}

fn unpack_id(id: InstanceId) -> (u32, u32) {
    (id >> (32 - NODE_ID_BITS), id & SLOT_MASK)
}

impl Sidecar {
    fn new() -> Arc<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let num_nodes = numa_node_count().max(1) as usize;
        let mut nodes = Vec::with_capacity(num_nodes);
        for _ in 0..num_nodes {
            nodes.push(NodeSidecar::new());
        }
        let sidecar = Arc::new(Self {
            nodes,
            shutdown: shutdown.clone(),
            join_handles: AtomicPtr::new(ptr::null_mut()),
            instance_count: AtomicUsize::new(0),
            max_instances: AtomicUsize::new(DEFAULT_MAX_INSTANCES),
        });

        let mut handles = Vec::with_capacity(num_nodes);
        for node_idx in 0..num_nodes {
            let runner = sidecar.clone();
            let handle = thread::Builder::new()
                .name(format!("subetha-sidecar-node{node_idx}"))
                .spawn(move || runner.run_loop_for_node(node_idx))
                .expect("failed to spawn subetha-sidecar node thread");
            let thread = Box::into_raw(Box::new(handle.thread().clone()));
            sidecar.nodes[node_idx].thread.store(thread, Ordering::Release);
            handles.push(handle);
        }
        sidecar.join_handles.store(Box::into_raw(Box::new(handles)), Ordering::Release);

        sidecar
    }

    fn run_loop_for_node(self: Arc<Self>, node_idx: usize) {
        let node = &self.nodes[node_idx];
        let _exit = ExitMark(&node.exited);
        while !self.shutdown.load(Ordering::Acquire) {
            let covered = node.requested.load(Ordering::Acquire);
            Self::scan_node(node);
            node.completed.store(covered, Ordering::Release);
            if node.requested.load(Ordering::Acquire) == covered {
                thread::park_timeout(POLL_INTERVAL);
            }
        }
    }

    /// One pass over a node's registrations. Runs on the node's own
    /// scan thread only, which makes that thread the sole consumer of
    /// each ring.
    fn scan_node(node: &NodeSidecar) {
        for segment in node.segments() {
            for slot in &segment.slots {
                if let Some(entered) = slot.enter() {
                    Self::scan_registration(entered.registration());
                }
            }
        }
    }

    fn scan_registration(reg: &Registration) {
        let ring = unsafe { reg.ring.as_ref() };
        let header = unsafe { reg.header.as_ref() };
        let mut drained_ops: u64 = 0;
        let mut drained_lat: u64 = 0;
        let mut drained_cont: u64 = 0;
        let mut drained_kinds: [u64; N_OP_KINDS] = [0; N_OP_KINDS];
        // Per-scan dedupe of (op_kind, tid) pairs. Bounded at
        // N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND so even a burst of
        // distinct threads costs O(constant) per scan.
        const DEDUPE_CAP: usize = N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND;
        let mut tid_dedupe: [(u16, u32); DEDUPE_CAP] = [(0, 0); DEDUPE_CAP];
        let mut tid_dedupe_len: usize = 0;
        for _ in 0..DRAIN_SAFETY_CAP {
            let Some(obs) = ring.pop() else { break };
            drained_ops += 1;
            drained_lat = drained_lat.saturating_add(obs.latency_ticks);
            if obs.flags & 1 != 0 {
                drained_cont += 1;
            }
            let k = (obs.op_kind as usize).min(N_OP_KINDS - 1);
            drained_kinds[k] = drained_kinds[k].saturating_add(1);
            // Inline dedupe of (op_kind, tid) pairs.
            if obs.producer_thread_id != 0 && tid_dedupe_len < DEDUPE_CAP {
                let pair = (obs.op_kind, obs.producer_thread_id);
                let seen = tid_dedupe[..tid_dedupe_len].contains(&pair);
                if !seen {
                    tid_dedupe[tid_dedupe_len] = pair;
                    tid_dedupe_len += 1;
                }
            }
        }
        if drained_ops == 0 {
            return;
        }
        let mut s = *reg.stats.load();
        s.ops_observed = s.ops_observed.saturating_add(drained_ops);
        s.total_latency_ticks = s.total_latency_ticks.saturating_add(drained_lat);
        s.contention_ops = s.contention_ops.saturating_add(drained_cont);
        for (slot, drained) in s.op_kind_counts.iter_mut().zip(drained_kinds.iter()) {
            *slot = slot.saturating_add(*drained);
        }
        // Fold deduped (op_kind, tid) pairs into per-op-kind
        // distinct-thread tracking on the stats struct.
        for &(op_kind, tid) in tid_dedupe[..tid_dedupe_len].iter() {
            record_thread_for_op(&mut s, op_kind, tid);
        }
        s.last_drain_us = Instant::now().duration_since(reg.registered_at).as_micros() as u64;
        let current_tag = header.tag();
        if let Some(new_tag) = reg.policy.decide(&s, current_tag)
            && new_tag != current_tag {
                if let Some(inst_ptr) = reg.instance {
                    let inst = unsafe { &*inst_ptr.as_ptr() };
                    inst.apply_migration(new_tag);
                } else {
                    header.set_tag(new_tag);
                }
                s.migrations_triggered = s.migrations_triggered.saturating_add(1);
            }
        reg.stats.store(Arc::new(s));
    }

    /// Take the scan threads' handles, once.
    fn take_join_handles(&self) -> Vec<JoinHandle<()>> {
        let handles = self.join_handles.swap(ptr::null_mut(), Ordering::AcqRel);
        if handles.is_null() {
            Vec::new()
        } else {
            // SAFETY: stored once from Box::into_raw in `new`, and the
            // swap took it out, so this is its only owner.
            *unsafe { Box::from_raw(handles) }
        }
    }

    /// Stop the scan threads and wait for them, reporting one that ended
    /// by panic: a caller at teardown has nowhere to hand it.
    fn stop_scans(&self, when: &str) {
        self.shutdown.store(true, Ordering::Release);
        for node in &self.nodes {
            node.wake();
        }
        for h in self.take_join_handles() {
            if h.join().is_err() {
                eprintln!("subetha-sidecar: a scan worker ended by panic before {when}");
            }
        }
    }

    /// Register a primitive instance.
    ///
    /// # Safety
    ///
    /// `header`, `ring`, and (when provided) `instance` must remain
    /// valid until `unregister(id)` returns for the returned `id`.
    /// Prefer [`SidecarBox`] which enforces this invariant automatically.
    pub unsafe fn register_raw(
        &self,
        header: NonNull<HandshakeHeader>,
        ring: NonNull<ObservationRing>,
        instance: Option<NonNull<dyn AdaptiveInstance>>,
        policy: Box<dyn Policy>,
    ) -> InstanceId {
        // Hard cap enforced before any allocation. Panics with a
        // diagnostic identifying the likely cause; the diagnostic
        // text is part of the API surface and tested below.
        let cap = self.max_instances.load(Ordering::Acquire);
        let prev = self.instance_count.fetch_add(1, Ordering::AcqRel);
        if prev >= cap {
            self.instance_count.fetch_sub(1, Ordering::AcqRel);
            panic!(
                "subetha-sidecar: instance cap ({cap}) exceeded.\n\
                 Likely cause: SidecarBox<Adaptive*> is being created \
                 inside a tight loop (criterion b.iter(), test fixture, \
                 or runaway production code). Move construction outside \
                 the loop and reuse the instance, or call \
                 Sidecar::set_max_instances() if the load is intentional."
            );
        }
        // Arm the ring now that a consumer (this sidecar) is taking
        // ownership of draining it. Until this point producers skip every
        // push, so raw `create()` handles pay nothing for observation.
        // SAFETY: `ring` is valid per this function's safety contract.
        unsafe { ring.as_ref().arm(); }

        let reg = Registration {
            header,
            ring,
            instance,
            policy,
            stats: SwapCell::new(InstanceStats::default()),
            registered_at: Instant::now(),
        };

        // Route by current NUMA node; clamp to available nodes.
        let node_idx = (current_numa_node() as usize) % self.nodes.len();
        let (slot_idx, slot) = self.nodes[node_idx].claim_slot();
        // SAFETY: `claim_slot` moved the slot to SLOT_FILLING, so this
        // register is the only writer and no reader enters until the
        // state below.
        unsafe { (*slot.reg.get()).write(reg) };
        slot.state.store(SLOT_LIVE, Ordering::Release);
        pack_id(node_idx as u32, slot_idx as u32)
    }

    /// Remove a registered instance.
    ///
    /// Waits until a scan or stats read inside the registration has left
    /// it, so the caller can safely drop the underlying header/ring
    /// memory immediately after this returns.
    pub fn unregister(&self, id: InstanceId) {
        let (node_idx, slot_idx) = unpack_id(id);
        let Some(slot) = self.nodes.get(node_idx as usize).and_then(|n| n.slot(slot_idx as usize))
        else {
            return;
        };
        if slot.retire() {
            self.instance_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Currently-registered instance count.
    pub fn instance_count(&self) -> usize {
        self.instance_count.load(Ordering::Acquire)
    }

    /// Configured maximum simultaneously-registered instances. See
    /// [`DEFAULT_MAX_INSTANCES`] for the default and
    /// [`Self::set_max_instances`] to change it.
    pub fn max_instances(&self) -> usize {
        self.max_instances.load(Ordering::Acquire)
    }

    /// Raise or lower the instance cap. Intentional heavy-registration
    /// workloads (e.g., a server that legitimately wants > 10,000
    /// adaptive primitives live at once) should call this once at
    /// startup. The cap belongs to this `Sidecar`; call it on
    /// [`global()`] for the process-wide sidecar.
    pub fn set_max_instances(&self, cap: usize) {
        self.max_instances.store(cap, Ordering::Release);
    }

    /// Snapshot the stats for a registered instance, as the last scan
    /// that drained it left them.
    pub fn stats(&self, id: InstanceId) -> Option<InstanceStats> {
        let (node_idx, slot_idx) = unpack_id(id);
        let slot = self.nodes.get(node_idx as usize)?.slot(slot_idx as usize)?;
        let entered = slot.enter()?;
        Some(*entered.registration().stats.load())
    }

    /// Have every node scan now and wait for it. Useful for tests where
    /// we don't want to wait for the poll interval. Each node's own
    /// thread does the scan, so when this returns, an observation pushed
    /// before the call has been drained and counted.
    ///
    /// Called from a node's own scan thread, as a policy or migration
    /// might, it does not wait on that node, whose scan is the one
    /// running. After the sidecar has stopped its threads it returns at
    /// once.
    pub fn scan_now(&self) {
        for node in &self.nodes {
            if node.is_own_thread() || node.exited.load(Ordering::Acquire) {
                continue;
            }
            let target = node.requested.fetch_add(1, Ordering::AcqRel) + 1;
            node.wake();
            while node.completed.load(Ordering::Acquire) < target
                && !node.exited.load(Ordering::Acquire)
            {
                thread::yield_now();
            }
        }
    }

    /// Number of NUMA-pinned sidecar threads in this pool.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

}

impl Drop for Sidecar {
    fn drop(&mut self) {
        // A scan worker that panicked is reported rather than raised: a
        // panic here would abort an unwind already in progress.
        self.stop_scans("shutdown");
    }
}

static GLOBAL: Lazy<Arc<Sidecar>> = Lazy::new(|| {
    let s = Sidecar::new();
    // A static is never dropped at exit, so an `atexit` callback
    // signals shutdown and joins the sidecar threads before process
    // teardown rather than leaving the OS to end them mid-scan.
    register_sidecar_atexit();
    s
});

/// Registers the atexit callback, once per process.
fn register_sidecar_atexit() {
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED.call_once(|| {
        // SAFETY: `atexit` accepts an `extern "C" fn()` callback that
        // the CRT invokes from the main thread during normal process
        // teardown (after `main` returns, before final OS exit). The
        // callback we register only touches `GLOBAL` (a static),
        // which outlives the call by construction.
        unsafe {
            unsafe extern "C" {
                fn atexit(cb: extern "C" fn()) -> i32;
            }
            atexit(sidecar_atexit_shutdown);
        }
    });
}

/// atexit-registered callback: signal sidecar shutdown + join the
/// per-NUMA scanning threads. Runs on the main thread during normal
/// process teardown so the OS doesn't have to TerminateThread the
/// sidecar workers mid-action.
extern "C" fn sidecar_atexit_shutdown() {
    // A library the process unloads as it exits runs this after Windows
    // has ended every other thread, the scan threads among them: joining
    // one then panics, and a slot one was inside never empties. Nothing
    // is left to stop.
    if other_threads_have_ended() {
        return;
    }
    if let Some(sidecar) = Lazy::get(&GLOBAL) {
        sidecar.stop_scans("process exit");
        // With the scan threads joined, clear the registry, so any other
        // static drop chain that touches the sidecar finds it empty rather
        // than holding raw pointers from a leaked SidecarBox.
        for node in &sidecar.nodes {
            for segment in node.segments() {
                for slot in &segment.slots {
                    slot.retire();
                }
            }
        }
    }
}

/// Whether the process has ended every thread but the caller's, which
/// Windows does as a process exits, before it unloads the process's
/// libraries.
#[cfg(target_os = "windows")]
fn other_threads_have_ended() -> bool {
    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn RtlDllShutdownInProgress() -> u8;
    }
    // SAFETY: takes nothing and reads a flag the loader sets.
    unsafe { RtlDllShutdownInProgress() != 0 }
}

/// Elsewhere a process runs its exit callbacks, a library's included,
/// while its other threads still run.
#[cfg(not(target_os = "windows"))]
fn other_threads_have_ended() -> bool {
    false
}

/// Get the process-wide sidecar singleton.
pub fn global() -> Arc<Sidecar> {
    GLOBAL.clone()
}

/// Number of NUMA nodes detected on this host. A `Sidecar` spawns one
/// scan thread per node it reports.
///
/// On Windows this calls `GetNumaHighestNodeNumber`. On other platforms
/// it returns 1 (no NUMA awareness). Returns at least 1.
pub fn numa_node_count() -> u32 {
    #[cfg(target_os = "windows")]
    {
        // SAFETY: GetNumaHighestNodeNumber takes a pointer to a ULONG
        // and writes the highest node number through it. No allocation.
        unsafe {
            let mut highest: u32 = 0;
            unsafe extern "system" {
                fn GetNumaHighestNodeNumber(HighestNodeNumber: *mut u32) -> i32;
            }
            // Link against kernel32.lib (auto-linked on MSVC targets).
            let result = GetNumaHighestNodeNumber(&mut highest);
            if result == 0 {
                // A zero `BOOL` means failure; fall back to 1 node.
                1
            } else {
                highest.saturating_add(1)
            }
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        1
    }
}

/// The NUMA node of the processor the calling thread is running on.
/// `register_raw` files a registration under that node's scan thread,
/// modulo the nodes [`numa_node_count`] reports.
///
/// Uses `GetCurrentProcessorNumberEx` + `GetNumaProcessorNodeEx` on
/// Windows: these two work across processor groups (Windows splits
/// logical processors into groups of up to 64), so the >64-logical-
/// processor case (dual-socket servers, large core-count workstations)
/// is handled correctly. `GetNumaProcessorNode`, which is capped at
/// processor 255, is not used.
pub fn current_numa_node() -> u32 {
    #[cfg(target_os = "windows")]
    {
        // PROCESSOR_NUMBER per
        // https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-processor_number
        // sized layout: Group (`USHORT`) + Number (`BYTE`) + Reserved (`BYTE`) = 4 bytes.
        #[repr(C)]
        #[derive(Default, Clone, Copy)]
        struct ProcessorNumber {
            group: u16,
            number: u8,
            reserved: u8,
        }
        // SAFETY: GetCurrentProcessorNumberEx writes through &mut, no allocation.
        // GetNumaProcessorNodeEx reads from the struct and writes one u16 out.
        unsafe {
            unsafe extern "system" {
                fn GetCurrentProcessorNumberEx(ProcNumber: *mut ProcessorNumber);
                fn GetNumaProcessorNodeEx(
                    Processor: *const ProcessorNumber,
                    NodeNumber: *mut u16,
                ) -> i32;
            }
            let mut proc = ProcessorNumber::default();
            GetCurrentProcessorNumberEx(&mut proc);
            let mut node: u16 = 0;
            if GetNumaProcessorNodeEx(&proc, &mut node) != 0 {
                // u16::MAX (0xFFFF) is a documented "no node" sentinel
                // returned by the API for un-NUMA-classified procs;
                // route those to node 0.
                if node == u16::MAX { 0 } else { node as u32 }
            } else {
                0
            }
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        // The physical package of the processor that last ran this
        // thread, from /proc/self/stat and sysfs; 0 when either is
        // missing. `numa_node_count` reports one node off Windows, so
        // every registration there lands on node 0 either way.
        current_numa_node_linux()
    }
}

#[cfg(not(target_os = "windows"))]
fn current_numa_node_linux() -> u32 {
    use std::fs;
    // Field 39 of /proc/self/stat is the processor that last ran this
    // thread.
    let stat = match fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    // /proc/self/stat fields are space-separated past the comm field
    // (which is parenthesized). Skip past the closing paren.
    let after_comm = match stat.rfind(')') {
        Some(i) => &stat[i + 1..],
        None => return 0,
    };
    // Last-scheduled CPU is field 39 in proc(5); we count fields from
    // after_comm (which is at field 3 boundary because pid, comm are 1-2).
    let cpu = match after_comm.split_whitespace().nth(36) {
        Some(s) => match s.parse::<u32>() { Ok(v) => v, Err(_) => return 0 },
        None => return 0,
    };
    let path = format!(
        "/sys/devices/system/cpu/cpu{cpu}/topology/physical_package_id"
    );
    match fs::read_to_string(&path) {
        Ok(s) => s.trim().parse::<u32>().unwrap_or(0),
        Err(_) => 0,
    }
}

/// RAII handle that auto-unregisters its instance on drop.
pub struct SidecarHandle {
    id: InstanceId,
    sidecar: Arc<Sidecar>,
}

impl SidecarHandle {
    pub fn id(&self) -> InstanceId {
        self.id
    }

    pub fn stats(&self) -> Option<InstanceStats> {
        self.sidecar.stats(self.id)
    }
}

impl Drop for SidecarHandle {
    fn drop(&mut self) {
        self.sidecar.unregister(self.id);
    }
}

/// Trait implemented by adaptive primitive instances that opt into
/// sidecar observation. The Box guarantees stable addresses for the
/// header and ring.
pub trait AdaptiveInstance: Send + Sync + 'static {
    fn header(&self) -> &HandshakeHeader;
    fn ring(&self) -> &ObservationRing;
    fn make_policy(&self) -> Box<dyn Policy>;

    /// Called by the sidecar when the policy returns a new strategy
    /// tag. Default implementation: just set the tag on the header.
    /// Primitives that need heavier migration (data-layout swap)
    /// override this to perform the swap before (or after) updating
    /// the tag.
    fn apply_migration(&self, new_tag: u32) {
        self.header().set_tag(new_tag);
    }
}

/// Boxed primitive + auto-unregistering sidecar handle.
///
/// `Drop` order is well-defined: handle drops first (blocks on scan,
/// then clears the registry slot), then the box drops (frees the
/// header/ring memory). No raw-pointer-after-free race.
pub struct SidecarBox<T: AdaptiveInstance> {
    // Field order is drop order: handle drops before inner.
    handle: SidecarHandle,
    inner: Box<T>,
}

impl<T: AdaptiveInstance> SidecarBox<T> {
    pub fn new(value: T) -> Self {
        let inner = Box::new(value);
        // SAFETY: Box guarantees stable address until inner is dropped.
        // Field references and the instance pointer are valid as long
        // as inner is alive. SidecarHandle::drop runs before inner::drop,
        // calling unregister(), which blocks until any in-flight scan
        // finishes.
        let header = NonNull::from(inner.header());
        let ring = NonNull::from(inner.ring());
        let instance_ref: &dyn AdaptiveInstance = &*inner;
        let instance_ptr: *const dyn AdaptiveInstance = instance_ref;
        let instance = unsafe {
            NonNull::new_unchecked(instance_ptr as *mut dyn AdaptiveInstance)
        };
        let policy = inner.make_policy();
        let sidecar = global();
        let id = unsafe { sidecar.register_raw(header, ring, Some(instance), policy) };
        Self {
            handle: SidecarHandle { id, sidecar },
            inner,
        }
    }

    pub fn id(&self) -> InstanceId {
        self.handle.id
    }

    pub fn stats(&self) -> Option<InstanceStats> {
        self.handle.stats()
    }
}

impl<T: AdaptiveInstance> std::ops::Deref for SidecarBox<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use subetha_core::Observation;

    /// A bare instance with just header + ring for sidecar testing.
    struct BareInstance {
        header: HandshakeHeader,
        ring: ObservationRing,
    }

    impl BareInstance {
        fn new() -> Self {
            Self {
                header: HandshakeHeader::new(),
                ring: ObservationRing::new(),
            }
        }
    }

    impl AdaptiveInstance for BareInstance {
        fn header(&self) -> &HandshakeHeader { &self.header }
        fn ring(&self) -> &ObservationRing { &self.ring }
        fn make_policy(&self) -> Box<dyn Policy> { Box::new(NoMigrationPolicy) }
    }

    /// Policy that escalates the tag whenever average latency > threshold.
    struct EscalatingPolicy {
        threshold_ticks: u64,
        escalate_to: u32,
    }

    impl Policy for EscalatingPolicy {
        fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32> {
            if stats.average_latency_ticks() > self.threshold_ticks && current_tag < self.escalate_to {
                Some(self.escalate_to)
            } else {
                None
            }
        }
    }

    struct EscalatingInstance {
        header: HandshakeHeader,
        ring: ObservationRing,
    }

    impl EscalatingInstance {
        fn new() -> Self {
            Self {
                header: HandshakeHeader::new(),
                ring: ObservationRing::new(),
            }
        }
    }

    impl AdaptiveInstance for EscalatingInstance {
        fn header(&self) -> &HandshakeHeader { &self.header }
        fn ring(&self) -> &ObservationRing { &self.ring }
        fn make_policy(&self) -> Box<dyn Policy> {
            Box::new(EscalatingPolicy {
                threshold_ticks: 500,
                escalate_to: 2,
            })
        }
    }

    #[test]
    fn register_unregister_balances() {
        let s = global();
        let inst = Box::new(BareInstance::new());
        let header = NonNull::from(inst.header());
        let ring = NonNull::from(inst.ring());
        let id = unsafe {
            s.register_raw(header, ring, None, Box::new(NoMigrationPolicy))
        };
        assert!(s.stats(id).is_some());
        s.unregister(id);
        assert!(s.stats(id).is_none());
        drop(inst);
    }

    #[test]
    fn sidecar_drains_observations() {
        let inst = SidecarBox::new(BareInstance::new());

        // Push 10 observations.
        for i in 0..10 {
            assert!(inst.ring.push(Observation {
                instance_id: 0,
                op_kind: 1,
                flags: 0,
                latency_ticks: 100 + i,
                ..Observation::ZERO
            }));
        }

        // Force a scan.
        global().scan_now();

        let stats = inst.stats().expect("instance should be registered");
        assert_eq!(stats.ops_observed, 10);
        assert!(stats.total_latency_ticks >= 1000);
    }

    #[test]
    fn policy_migrates_strategy_when_threshold_crossed() {
        let inst = SidecarBox::new(EscalatingInstance::new());
        assert_eq!(inst.header().tag(), 0);

        // Push observations with latency well above threshold (500).
        for _ in 0..50 {
            inst.ring.push(Observation {
                instance_id: 0,
                op_kind: 1,
                flags: 0,
                latency_ticks: 5000,
                ..Observation::ZERO
            });
        }

        global().scan_now();

        // EscalatingPolicy should have set tag to 2.
        assert_eq!(inst.header().tag(), 2,
                   "policy should have escalated tag to 2 after high-latency observations");
    }

    #[test]
    fn unregister_blocks_safe_drop() {
        // This is the load-bearing race-safety test. We register an
        // instance, push observations, drop the SidecarBox while the
        // sidecar may be mid-scan, and rely on the unregister-blocks-on-
        // scan contract to prevent use-after-free.
        for _ in 0..50 {
            let inst = SidecarBox::new(BareInstance::new());
            // Push observations to make the sidecar dereference our pointers.
            for _ in 0..100 {
                inst.ring.push(Observation {
                    instance_id: 0,
                    op_kind: 1,
                    flags: 0,
                    latency_ticks: 10,
                    ..Observation::ZERO
                });
            }
            // Drop while sidecar may be scanning. If unregister doesn't
            // block correctly, this leads to use-after-free under TSAN/ASAN.
            drop(inst);
        }
    }

    #[test]
    fn fixed_policy_sets_tag_immediately() {
        struct Inst { h: HandshakeHeader, r: ObservationRing }
        impl AdaptiveInstance for Inst {
            fn header(&self) -> &HandshakeHeader { &self.h }
            fn ring(&self) -> &ObservationRing { &self.r }
            fn make_policy(&self) -> Box<dyn Policy> { Box::new(FixedPolicy(7)) }
        }
        let inst = SidecarBox::new(Inst {
            h: HandshakeHeader::new(),
            r: ObservationRing::new(),
        });

        inst.r.push(Observation { instance_id: 0, op_kind: 0, flags: 0, latency_ticks: 1, ..Observation::ZERO });
        global().scan_now();
        assert_eq!(inst.h.tag(), 7);
    }

    #[test]
    fn instance_count_tracks_register_and_unregister() {
        // Use a local Sidecar so this test does not interfere with
        // the global one used by other tests.
        let s = Sidecar::new();
        let start = s.instance_count();

        let inst = Box::new(BareInstance::new());
        let header = NonNull::from(inst.header());
        let ring = NonNull::from(inst.ring());
        let id = unsafe {
            s.register_raw(header, ring, None, Box::new(NoMigrationPolicy))
        };
        assert_eq!(s.instance_count(), start + 1);

        s.unregister(id);
        assert_eq!(s.instance_count(), start);
    }

    #[test]
    fn cap_panic_message_is_actionable() {
        // Build a Sidecar with a tiny cap and verify the panic
        // message names the actual cap value and mentions the
        // diagnostic guidance about loops / b.iter() / set_max_instances.
        let s = Sidecar::new();
        s.set_max_instances(2);
        assert_eq!(s.max_instances(), 2);

        // Register up to the cap (no panic).
        let inst1 = Box::new(BareInstance::new());
        let id1 = unsafe {
            s.register_raw(
                NonNull::from(inst1.header()),
                NonNull::from(inst1.ring()),
                None,
                Box::new(NoMigrationPolicy),
            )
        };
        let inst2 = Box::new(BareInstance::new());
        let id2 = unsafe {
            s.register_raw(
                NonNull::from(inst2.header()),
                NonNull::from(inst2.ring()),
                None,
                Box::new(NoMigrationPolicy),
            )
        };

        // Third must panic with the documented diagnostic.
        let inst3 = Box::new(BareInstance::new());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unsafe {
                s.register_raw(
                    NonNull::from(inst3.header()),
                    NonNull::from(inst3.ring()),
                    None,
                    Box::new(NoMigrationPolicy),
                )
            }
        }));
        let payload = result.expect_err("must panic when over cap");
        let msg = payload.downcast_ref::<String>().map(String::as_str)
            .or_else(|| payload.downcast_ref::<&'static str>().copied())
            .expect("panic payload must be a string");
        assert!(msg.contains("instance cap (2) exceeded"),
                "panic must name the cap value: {msg}");
        assert!(msg.contains("b.iter()") || msg.contains("loop"),
                "panic must hint at b.iter() / loop misuse: {msg}");
        assert!(msg.contains("set_max_instances"),
                "panic must mention the escape hatch: {msg}");

        // Failed register must not have incremented the count past cap.
        assert_eq!(s.instance_count(), 2,
                   "count must roll back on cap-rejected register");

        // Cleanup so test does not leak.
        s.unregister(id1);
        s.unregister(id2);
    }

    /// Instances registered on one sidecar, past three segments' worth.
    const MANY_INSTANCES: usize = 3 * SLOTS_PER_SEGMENT + 8;

    /// Each registration keeps its own stats however many segments the
    /// node grows to, and an unregistered slot is reused.
    #[test]
    fn registrations_past_one_segment_keep_their_own_stats() {
        let s = Sidecar::new();
        let instances: Vec<Box<BareInstance>> =
            (0..MANY_INSTANCES).map(|_| Box::new(BareInstance::new())).collect();
        let ids: Vec<InstanceId> = instances
            .iter()
            .map(|inst| unsafe {
                s.register_raw(
                    NonNull::from(inst.header()),
                    NonNull::from(inst.ring()),
                    None,
                    Box::new(NoMigrationPolicy),
                )
            })
            .collect();
        for (i, inst) in instances.iter().enumerate() {
            for _ in 0..(i % 7 + 1) {
                assert!(inst.ring().push(Observation { op_kind: 1, latency_ticks: 1, ..Observation::ZERO }));
            }
        }
        s.scan_now();
        for (i, id) in ids.iter().enumerate() {
            let observed = s.stats(*id).expect("the instance is registered").ops_observed;
            assert_eq!(observed, (i % 7 + 1) as u64, "instance {i} counts its own observations");
        }
        s.unregister(ids[5]);
        assert!(s.stats(ids[5]).is_none(), "an unregistered id reads nothing");
        let again = unsafe {
            s.register_raw(
                NonNull::from(instances[5].header()),
                NonNull::from(instances[5].ring()),
                None,
                Box::new(NoMigrationPolicy),
            )
        };
        if unpack_id(again).0 == unpack_id(ids[5]).0 {
            assert_eq!(again, ids[5], "the freed slot on the node is taken first");
        } else {
            s.unregister(again);
        }
        for id in ids {
            s.unregister(id);
        }
        assert_eq!(s.instance_count(), 0);
    }

    /// Set in the child that `a_process_using_the_global_sidecar_exits_without_writing_to_stderr`
    /// starts, which uses the global sidecar and exits.
    const EXIT_CHILD_VAR: &str = "SUBETHA_SIDECAR_EXIT_CHILD";

    #[test]
    fn a_process_using_the_global_sidecar_exits_without_writing_to_stderr() {
        if std::env::var_os(EXIT_CHILD_VAR).is_some() {
            let inst = SidecarBox::new(BareInstance::new());
            inst.ring.push(Observation { op_kind: 1, latency_ticks: 1, ..Observation::ZERO });
            global().scan_now();
            return;
        }
        let out = std::process::Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "tests::a_process_using_the_global_sidecar_exits_without_writing_to_stderr",
                "--test-threads=1",
            ])
            .env(EXIT_CHILD_VAR, "1")
            .output()
            .expect("the child runs");
        assert!(out.status.success(), "the child failed: {out:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.is_empty(), "the child wrote to stderr: {stderr:?}");
    }

    /// Threads calling `scan_now` beside the node's own scan thread.
    const RACING_SCANNERS: usize = 4;
    /// Observations the producer offers while the scans race.
    const RACED_PUSHES: u64 = 200_000;

    /// A ring has one consumer, so however many scans run at once, each
    /// observation the ring accepted is counted exactly once. Several
    /// threads call `scan_now` in a loop beside the node's own scan
    /// thread while a producer pushes.
    #[test]
    fn concurrent_scans_count_each_observation_once() {
        let s = Sidecar::new();
        let inst = Box::new(BareInstance::new());
        let id = unsafe {
            s.register_raw(
                NonNull::from(inst.header()),
                NonNull::from(inst.ring()),
                None,
                Box::new(NoMigrationPolicy),
            )
        };
        let producing = AtomicBool::new(true);
        let accepted = thread::scope(|scope| {
            for _ in 0..RACING_SCANNERS {
                scope.spawn(|| {
                    while producing.load(Ordering::Acquire) {
                        s.scan_now();
                    }
                });
            }
            let mut accepted = 0u64;
            for _ in 0..RACED_PUSHES {
                let pushed = inst.ring().push(Observation {
                    instance_id: 0,
                    op_kind: 1,
                    flags: 0,
                    latency_ticks: 1,
                    ..Observation::ZERO
                });
                if pushed {
                    accepted += 1;
                }
            }
            producing.store(false, Ordering::Release);
            accepted
        });
        s.scan_now();
        let observed = s.stats(id).expect("the instance is registered").ops_observed;
        s.unregister(id);
        assert_eq!(observed, accepted, "each accepted observation is counted once");
    }
}
