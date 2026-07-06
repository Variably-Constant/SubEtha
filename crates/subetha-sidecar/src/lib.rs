//! Sidecar control plane for adaptive primitives.
//!
//! One background thread per detected NUMA node; each thread polls
//! the registered primitive instances bound to its node:
//!
//! 1. Drains each instance's [`ObservationRing`] into a per-instance
//!    [`InstanceStats`] accumulator.
//! 2. Asks the instance's [`Policy`] whether a strategy migration is
//!    warranted.
//! 3. If yes, calls [`HandshakeHeader::set_tag`] to install the new
//!    strategy.
//!
//! Heavy migrations (data swap) are NOT handled here; primitives
//! that need them invoke their own migration logic from within the
//! policy callback (e.g. `subetha-cxc::AdaptiveIpc::migrate_to`).
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

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use subetha_core::{HandshakeHeader, ObservationRing};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};

pub mod bench_safe;

/// Stable identifier for a registered primitive instance.
pub type InstanceId = u32;

/// Number of op_kind slots the InstanceStats tracks. Primitives use
/// `op_kind` values 1..=N to identify per-op-kind buckets (e.g., load
/// vs store for `AdaptiveCell`; insert/get/remove/snapshot for the
/// snapshot map). Op kind 0 is reserved for "unspecified".
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
    pub last_seen_us_ago: u64,
    /// Number of migrations the sidecar has triggered on this instance
    /// (apply_migration calls that resulted in a tag change). Used by
    /// bench harnesses to measure adaptation-latency convergence.
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
            last_seen_us_ago: 0,
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
    /// recv-side op kind) usage on this primitive - the right signal
    /// for a ChannelPolicy promoting SPSC → MPMC, an AdaptiveCell
    /// noticing multi-writer churn, etc. Saturates at
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
    // bump the saturated count to MAX+1 (when full).
    if n < MAX_TRACKED_THREADS_PER_KIND {
        slots[n] = tid;
        stats.per_op_kind_distinct_count[k] = (n as u8) + 1;
    } else {
        // Saturation transition: count moves from MAX to MAX+1; we
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

/// Convenient policy that never migrates (default for primitives that
/// haven't shipped their adaptation logic yet).
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
    stats: Mutex<InstanceStats>,
    registered_at: Instant,
    last_observation_at: Mutex<Option<Instant>>,
}

// SAFETY: The contract requires `header`, `ring`, and the optional
// `instance` pointer to be valid for the lifetime of this Registration
// (i.e., until unregister returns). The sidecar accesses them only
// while holding a read lock on the instances vec, which blocks
// unregister.
unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

/// Safety cap on drained observations per scan iteration per instance.
///
/// In normal operation the sidecar drains the entire ring on each scan
/// (no sampling bias from FIFO order) - the ring's natural capacity
/// (4096 slots) bounds the work. This cap is the catastrophe-mode
/// limit: if a misconfigured ring ever exceeds this number of slots,
/// the cap kicks in to keep one busy instance from starving the
/// others.
///
/// Worst-case per-scan cost: 4096 observations * ~10 ns drain cost =
/// ~40 us per instance, holding the read lock that long. With 100
/// instances this caps a single scan loop at ~4 ms.
const DRAIN_SAFETY_CAP: usize = 8192;

/// Sidecar poll interval. Trade-off: shorter = faster reaction to
/// transitions; longer = less CPU spent on cold/idle instances.
const POLL_INTERVAL: Duration = Duration::from_micros(200);

/// A single node's instance vec + scanning thread. One per NUMA node.
struct NodeSidecar {
    instances: RwLock<Vec<Option<Registration>>>,
}

impl NodeSidecar {
    fn new() -> Self {
        Self { instances: RwLock::new(Vec::new()) }
    }
}

/// The sidecar singleton. Internally a pool of one `NodeSidecar` per
/// detected NUMA node; each node has its own scanning thread + Vec of
/// registered primitive instances. Registration routes by
/// `current_numa_node()` so cross-NUMA cache traffic on the scan path
/// stays minimal. InstanceId encodes (node_index, slot) so unregister
/// + stats can find the right node's vec.
pub struct Sidecar {
    nodes: Vec<NodeSidecar>,
    shutdown: Arc<AtomicBool>,
    join_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Currently-registered instance count (monotonic over the lifetime
    /// of this Sidecar). Incremented in `register_raw`, decremented in
    /// `unregister`. Read via [`Sidecar::instance_count`].
    instance_count: AtomicUsize,
    /// Hard cap on simultaneously-registered instances. When
    /// `register_raw` would cross this, it panics with a diagnostic.
    /// The default ([`DEFAULT_MAX_INSTANCES`]) covers all realistic
    /// production workloads; raise via [`Sidecar::set_max_instances`]
    /// when intentional heavy registration is needed.
    max_instances: AtomicUsize,
}

/// Default hard cap on registered instances. Calibrated for the
/// "bench mistakenly creates SidecarBox per b.iter()" case that
/// previously crashed the host at ~94k registrations; the cap fires
/// long before that. Production workloads almost always sit in the
/// 10..1000 range.
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
            join_handles: Mutex::new(Vec::with_capacity(num_nodes)),
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
            handles.push(handle);
        }
        *sidecar.join_handles.lock() = handles;

        sidecar
    }

    fn run_loop_for_node(self: Arc<Self>, node_idx: usize) {
        while !self.shutdown.load(Ordering::Acquire) {
            self.scan_node(node_idx);
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn scan_node(&self, node_idx: usize) {
        let Some(node) = self.nodes.get(node_idx) else { return };
        let guard = node.instances.read();
        Self::scan_instances(&guard);
    }

    fn scan_instances(instances: &[Option<Registration>]) {
        for reg_opt in instances.iter() {
            let Some(reg) = reg_opt else { continue };
            let ring = unsafe { reg.ring.as_ref() };
            let header = unsafe { reg.header.as_ref() };
            let mut drained_ops: u64 = 0;
            let mut drained_lat: u64 = 0;
            let mut drained_cont: u64 = 0;
            let mut drained_kinds: [u64; N_OP_KINDS] = [0; N_OP_KINDS];
            // Per-scan dedupe of (op_kind, tid) pairs. Bounded at
            // N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND so even a
            // burst of distinct threads costs O(constant) per scan
            // instead of saturating the stats-lock window.
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
                continue;
            }
            let stats_snapshot = {
                let mut s = reg.stats.lock();
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
                let now = Instant::now();
                *reg.last_observation_at.lock() = Some(now);
                s.last_seen_us_ago = now
                    .duration_since(reg.registered_at)
                    .as_micros() as u64;
                *s
            };
            let current_tag = header.tag();
            if let Some(new_tag) = reg.policy.decide(&stats_snapshot, current_tag)
                && new_tag != current_tag {
                    if let Some(inst_ptr) = reg.instance {
                        let inst = unsafe { &*inst_ptr.as_ptr() };
                        inst.apply_migration(new_tag);
                    } else {
                        header.set_tag(new_tag);
                    }
                    let mut s = reg.stats.lock();
                    s.migrations_triggered = s.migrations_triggered.saturating_add(1);
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
            stats: Mutex::new(InstanceStats::default()),
            registered_at: Instant::now(),
            last_observation_at: Mutex::new(None),
        };

        // Route by current NUMA node; clamp to available nodes.
        let node_idx = (current_numa_node() as usize) % self.nodes.len();
        let node = &self.nodes[node_idx];
        let mut guard = node.instances.write();

        // Find a vacant slot or push at the end.
        for (slot_idx, slot) in guard.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(reg);
                return pack_id(node_idx as u32, slot_idx as u32);
            }
        }
        let slot_idx = guard.len();
        guard.push(Some(reg));
        pack_id(node_idx as u32, slot_idx as u32)
    }

    /// Remove a registered instance.
    ///
    /// Blocks until any in-flight scan iteration finishes, so the caller
    /// can safely drop the underlying header/ring memory immediately
    /// after this returns.
    pub fn unregister(&self, id: InstanceId) {
        let (node_idx, slot_idx) = unpack_id(id);
        let Some(node) = self.nodes.get(node_idx as usize) else { return };
        let mut guard = node.instances.write();
        if let Some(slot) = guard.get_mut(slot_idx as usize)
            && slot.is_some() {
                *slot = None;
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
    /// startup. The cap is per-process; the global Sidecar inherits
    /// it via [`global()`].
    pub fn set_max_instances(&self, cap: usize) {
        self.max_instances.store(cap, Ordering::Release);
    }

    /// Snapshot the stats for a registered instance.
    pub fn stats(&self, id: InstanceId) -> Option<InstanceStats> {
        let (node_idx, slot_idx) = unpack_id(id);
        let node = self.nodes.get(node_idx as usize)?;
        let guard = node.instances.read();
        guard.get(slot_idx as usize)?.as_ref().map(|r| *r.stats.lock())
    }

    /// Force one scan iteration synchronously across all NUMA nodes.
    /// Useful for tests where we don't want to wait for the poll interval.
    pub fn scan_now(&self) {
        for node_idx in 0..self.nodes.len() {
            self.scan_node(node_idx);
        }
    }

    /// Number of NUMA-pinned sidecar threads in this pool.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

}

impl Drop for Sidecar {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.join_handles.lock());
        for h in handles {
            // Worker panic on shutdown is non-fatal.
            h.join().ok();
        }
    }
}

static GLOBAL: Lazy<Arc<Sidecar>> = Lazy::new(|| {
    let s = Sidecar::new();
    // Register an `atexit` callback that signals shutdown + joins
    // the sidecar threads before process teardown. Without this, the
    // `static Lazy<Arc<Sidecar>>` never drops at exit (Rust statics
    // with non-trivial Drop aren't run for late-initialised Lazy);
    // the OS terminates sidecar threads mid-action, occasionally
    // producing STATUS_ACCESS_VIOLATION at process exit when their
    // parking_lot/crossbeam TLS state races with the main thread's
    // CRT shutdown.
    register_sidecar_atexit();
    s
});

/// One-shot registration of the atexit callback. Idempotent across
/// processes that re-initialise the Lazy (e.g., on fork + re-exec).
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
    if let Some(sidecar) = Lazy::get(&GLOBAL) {
        sidecar.shutdown.store(true, Ordering::Release);
        let handles: Vec<JoinHandle<()>> = std::mem::take(
            &mut *sidecar.join_handles.lock(),
        );
        for h in handles {
            h.join().ok();
        }
        // After joining the scan threads, also clear the registry so
        // that any other static drop chain that touches Sidecar sees
        // an empty state instead of dangling raw pointers from leaked
        // SidecarBox<T> instances. (Tests may leak via panic; tear-
        // down code must tolerate it.)
        for node in &sidecar.nodes {
            let mut g = node.instances.write();
            for slot in g.iter_mut() {
                *slot = None;
            }
        }
        eprintln!("[subetha-sidecar atexit] shutdown complete");
    }
}

/// Get the process-wide sidecar singleton.
pub fn global() -> Arc<Sidecar> {
    GLOBAL.clone()
}

/// Number of NUMA nodes detected on this host. Used by the (in-progress)
/// per-NUMA sidecar sharding to decide how many sidecar threads to spawn.
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
                // BOOL FALSE means failure; fall back to 1 node.
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

/// Per-NUMA-node sidecar sharding scaffolding. The default global
/// sidecar handles all instances; multi-sidecar deployment with
/// per-NUMA pinning would spawn one Sidecar per node and route
/// registrations by spawn-thread affinity. The detection function
/// [`numa_node_count`] surfaces the topology; the routing layer plugs
/// in here when load testing on a multi-socket host motivates it.
///
/// Uses `GetCurrentProcessorNumberEx` + `GetNumaProcessorNodeEx` on
/// Windows: these two work across processor groups (Windows splits
/// logical processors into groups of up to 64), so the >64-logical-
/// processor case (dual-socket servers, large core-count workstations)
/// is handled correctly. The legacy `GetNumaProcessorNode` (capped at
/// processor 255) is no longer called.
pub fn current_numa_node() -> u32 {
    #[cfg(target_os = "windows")]
    {
        // PROCESSOR_NUMBER per
        // https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-processor_number
        // sized layout: Group (USHORT) + Number (BYTE) + Reserved (BYTE) = 4 bytes.
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
        // Linux: read /sys/devices/system/cpu/cpu<id>/topology/physical_package_id
        // for the current CPU. sched_getcpu() returns the logical CPU index;
        // /sys exposes the NUMA mapping. Fall back to node 0 if any step
        // fails (no sysfs, container without /sys, etc).
        current_numa_node_linux()
    }
}

#[cfg(not(target_os = "windows"))]
fn current_numa_node_linux() -> u32 {
    use std::fs;
    // libc::sched_getcpu would be the direct call but we avoid the libc
    // dep by reading /proc/self/stat field 39 (last_cpu) or by parsing
    // /sys/.../cpu<N>/topology. For portability across kernels we read
    // /proc/self/stat which exposes the last-scheduled CPU.
    let stat = match fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    // /proc/self/stat fields are space-separated AFTER the comm field
    // (which is parenthesised). Skip past the closing paren.
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
    // ORDER MATTERS: handle drops before inner.
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
        // message names the actual cap value AND mentions the
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

        // Failed register must NOT have incremented the count past cap.
        assert_eq!(s.instance_count(), 2,
                   "count must roll back on cap-rejected register");

        // Cleanup so test does not leak.
        s.unregister(id1);
        s.unregister(id2);
    }
}
