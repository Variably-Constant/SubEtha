//! `DequeDispatcher` - per-call routing across the MMF-deque family.
//!
//! The dispatcher owns one handle of each [`SharedDeque`] /
//! [`SharedDequeKhpd`] / [`SharedDequeLoh`] / [`SharedDequeUrd`]
//! variant that the host has configured, and picks the right one
//! per call based on a caller-supplied [`WorkloadShape`].
//!
//! ## The routing decision
//!
//! No single variant wins every workload shape. The right pick depends
//! on (a) whether the producer batches or dispatches per-item,
//! (b) how many thieves the workload runs, and (c) whether the
//! caller wants the consumer to halt the logical CPU between batches
//! via WAITPKG.
//!
//! | Workload | Pick | Why |
//! |---|---|---|
//! | Per-item dispatch, single thief | `ChaseLev` | Lowest constant per push; no batch to amortize. |
//! | Producer batches K = 2..128 items per call | `Khpd` | 3 items per Release-store on the publication line; empirically the best per-item cost on Zen+/Zen 4 at this scale. |
//! | Producer batches K >= 128 items per call | `Loh` | 1 `tail.fetch_add(K)` amortizes across the whole batch. |
//! | Multiple thieves AND batched producer | `Urd` | Per-thief mailbox = zero CAS contention. |
//! | Multiple thieves AND `wait_idle=true` | `Urd` | Hardware-mediated wake via WAITPKG / PAUSE-spin. |
//!
//! The routing table is a starting point. Per-host calibration may
//! flip individual cells. Callers that already know which variant
//! they want may call the per-variant getters
//! ([`DequeDispatcher::chase_lev`], etc.) directly.
//!
//! ## Cross-process E2E
//!
//! A `DequeDispatcher` lives in the producer process. Each variant
//! it owns is backed by its own MMF file path; consumer processes
//! open those same paths to drain. See the
//! [`dispatcher_demo`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/dispatcher_demo.rs)
//! example for the parent/child split.

#![allow(clippy::missing_errors_doc)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use subetha_core::{Axis, AxisMask};

use crate::shared_deque::SharedDeque;
use crate::shared_deque_khl::SharedDequeKhl;
use crate::shared_deque_khpd::{LineItem, SharedDequeKhpd};
use crate::shared_deque_loh::SharedDequeLoh;
use crate::shared_deque_urd::SharedDequeUrd;

/// Direction signatures per variant.
///
/// Each variant declares which of the six K-axes it engages at a
/// non-default value. The dispatcher uses signature-set logic to
/// route per `WorkloadShape`: `variant.satisfies(workload_required)`
/// picks the highest-engagement variant whose signature is a
/// superset of the workload's required signature.
const fn chase_lev_signature() -> AxisMask {
    // K_counter_share = owner-private is the only non-default axis;
    // K_inner=1, K_outer=1, K_gating=counter-only are all default.
    AxisMask::from_axes(&[Axis::CounterShare])
}

const fn khpd_signature() -> AxisMask {
    AxisMask::from_axes(&[Axis::Inner, Axis::Gating])
}

const fn loh_signature() -> AxisMask {
    AxisMask::from_axes(&[Axis::Outer, Axis::Gating])
}

const fn urd_signature() -> AxisMask {
    AxisMask::from_axes(&[
        Axis::Inner,
        Axis::Consumer,
        Axis::Radius,
        Axis::Gating,
    ])
}

const fn khl_signature() -> AxisMask {
    AxisMask::from_axes(&[
        Axis::Inner,
        Axis::Outer,
        Axis::CounterShare,
        Axis::Radius,
        Axis::Gating,
    ])
}

/// The deque-family variants the dispatcher routes across.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DequeVariant {
    /// Chase-Lev work-stealing deque, per-item push.
    ChaseLev,
    /// KHPD publication-line deque, 3 items per Release-store.
    Khpd,
    /// LOH LCRQ-on-LIFO Hybrid, amortizes `tail.fetch_add` over the
    /// whole batch.
    Loh,
    /// URD per-thief mailbox deque + WAITPKG wait.
    Urd,
    /// KHL K-axis Hierarchical LCRQ - SubEtha-native hybrid that
    /// pulls KHPD's per-slot packing + LOH's per-batch counter
    /// amortization + Chase-Lev's owner-private tail simultaneously.
    /// Empirically the strongest single-thief batched primitive on
    /// Zen+ R7 2700 (4.7 ns/item at K=64 producer-fast).
    Khl,
}

impl DequeVariant {
    /// The direction signature for this variant: which K-axes it
    /// engages at non-default values.
    pub const fn signature(self) -> AxisMask {
        match self {
            DequeVariant::ChaseLev => chase_lev_signature(),
            DequeVariant::Khpd => khpd_signature(),
            DequeVariant::Loh => loh_signature(),
            DequeVariant::Urd => urd_signature(),
            DequeVariant::Khl => khl_signature(),
        }
    }
}

/// Caller-supplied workload shape feeding the routing decision.
#[derive(Debug, Clone, Copy)]
pub struct WorkloadShape {
    /// Number of consumer threads / processes that will drain the
    /// deque concurrently. `>= 2` is the multi-thief regime where
    /// URD's per-mailbox layout amortizes against the shared-head
    /// CAS contention Chase-Lev / KHPD / LOH all pay.
    pub n_thieves: usize,
    /// `Some(K)` when the producer hands the dispatcher a batch of
    /// `K` items per call; `None` when the producer dispatches one
    /// item at a time (request-reply / latency-bound).
    pub batch_size: Option<usize>,
    /// `true` when the consumer should halt the logical CPU between
    /// batches (WAITPKG on capable silicon; PAUSE-spin elsewhere).
    /// Setting this routes to URD even at `n_thieves == 1`.
    pub wait_idle: bool,
}

impl WorkloadShape {
    /// The direction signature this workload requires from its
    /// transport: which K-axes the variant must engage to handle
    /// this shape.
    ///
    /// Per-item dispatch (no batch) requires nothing beyond the
    /// empty signature (Chase-Lev's signature is a superset of any
    /// empty requirement). Batched dispatch requires K_inner +
    /// K_outer engaged (per-slot packing AND per-batch counter
    /// amortization). Multi-thief or wait-idle requires K_consumer +
    /// K_radius engaged (per-thief mailboxes and CPUID-dispatched
    /// publish mechanism).
    pub const fn required_signature(&self) -> AxisMask {
        let mut bits = 0u16;
        // n_thieves >= 2 or wait_idle requires per-thief consumer +
        // radius dispatch.
        if self.n_thieves >= 2 || self.wait_idle {
            bits |= 1u16 << Axis::Consumer.bit();
            bits |= 1u16 << Axis::Radius.bit();
        }
        // batch_size = Some(k>=2) requires K_inner and K_outer.
        if let Some(k) = self.batch_size
            && k >= 2
        {
            bits |= 1u16 << Axis::Inner.bit();
            bits |= 1u16 << Axis::Outer.bit();
        }
        AxisMask::from_bits(bits)
    }

    /// Request-reply: per-item dispatch, single thief, no idle wait.
    pub fn request_reply() -> Self {
        Self {
            n_thieves: 1,
            batch_size: None,
            wait_idle: false,
        }
    }

    /// Producer-fast batch of `k` items, single thief.
    pub fn producer_fast(k: usize) -> Self {
        Self {
            n_thieves: 1,
            batch_size: Some(k),
            wait_idle: false,
        }
    }

    /// Fan-out: producer batches across multiple thieves. `n_thieves`
    /// >= 2 + `batch_size` set routes to URD.
    pub fn fan_out(n_thieves: usize, k: usize) -> Self {
        Self {
            n_thieves,
            batch_size: Some(k),
            wait_idle: false,
        }
    }
}

/// Errors from the dispatcher's send-side methods.
#[derive(Debug)]
pub enum DispatchError {
    /// The picked variant is not configured on this dispatcher
    /// (caller did not pass a backing file path at construction).
    BackendNotConfigured(DequeVariant),
    /// The picked variant's backing primitive returned a push error
    /// (the ring is at capacity, etc.).
    PushFailed(&'static str),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendNotConfigured(v) => {
                write!(f, "dispatcher: backend {v:?} is not configured")
            }
            Self::PushFailed(msg) => write!(f, "dispatcher: push failed ({msg})"),
        }
    }
}

impl std::error::Error for DispatchError {}

/// MMF-backed dispatcher across the deque family.
///
/// Construct via [`DequeDispatcher::builder`] and pass the backing
/// paths for each variant the caller wants available. The
/// [`pick`](DequeDispatcher::pick) helper returns the routing
/// decision for a shape WITHOUT performing the push, useful for
/// observers and per-host calibration.
pub struct DequeDispatcher {
    chase_lev: Option<Arc<SharedDeque<LineItem>>>,
    khpd: Option<Arc<SharedDequeKhpd>>,
    loh: Option<Arc<SharedDequeLoh>>,
    urd: Option<Arc<SharedDequeUrd>>,
    khl: Option<Arc<SharedDequeKhl>>,
}

impl DequeDispatcher {
    /// Start a builder. Pass the backing file paths for whichever
    /// variants the caller wants available; unset variants stay
    /// `None` and the dispatcher falls through to the next-best
    /// available variant per [`pick`](Self::pick).
    pub fn builder() -> DispatcherBuilder {
        DispatcherBuilder {
            chase_lev: None,
            khpd: None,
            loh: None,
            urd: None,
            khl: None,
        }
    }

    /// Get the underlying Chase-Lev handle (if configured).
    pub fn chase_lev(&self) -> Option<&Arc<SharedDeque<LineItem>>> {
        self.chase_lev.as_ref()
    }

    /// Get the underlying KHPD handle (if configured).
    pub fn khpd(&self) -> Option<&Arc<SharedDequeKhpd>> {
        self.khpd.as_ref()
    }

    /// Get the underlying LOH handle (if configured).
    pub fn loh(&self) -> Option<&Arc<SharedDequeLoh>> {
        self.loh.as_ref()
    }

    /// Get the underlying URD handle (if configured).
    pub fn urd(&self) -> Option<&Arc<SharedDequeUrd>> {
        self.urd.as_ref()
    }

    /// Get the underlying KHL handle (if configured).
    pub fn khl(&self) -> Option<&Arc<SharedDequeKhl>> {
        self.khl.as_ref()
    }

    /// Pick the right variant for `shape`. Returns the variant
    /// independent of whether the corresponding handle is configured
    /// (use [`pick_with_fallback`](Self::pick_with_fallback) to fold
    /// the configuration check into the decision).
    pub fn pick(shape: WorkloadShape) -> DequeVariant {
        // Multi-thief or wait_idle => URD (per-mailbox + WAITPKG).
        if shape.n_thieves >= 2 || shape.wait_idle {
            return DequeVariant::Urd;
        }
        // Single-thief routes by batch size. KHL is the SubEtha-
        // native hybrid that beats KHPD measurably (1.55x at K=64)
        // on producer-fast workloads; route any batched single-thief
        // call through it. Per-item dispatch still rides Chase-Lev
        // because it has no batch to amortize.
        match shape.batch_size {
            Some(k) if k >= 2 => DequeVariant::Khl,
            _ => DequeVariant::ChaseLev,
        }
    }

    /// Pick a variant by signature-set satisfaction. For each
    /// variant in priority order, check whether its signature is a
    /// superset of the workload's required signature; return the
    /// first match. Agrees with [`pick`](Self::pick) on every
    /// canonical workload shape.
    ///
    /// The dispatcher becomes a signature-set lens rather than a
    /// hardcoded match arm; the two routing methods co-exist so
    /// downstream callers can pick the pattern that fits their
    /// style.
    pub fn pick_by_signature(shape: WorkloadShape) -> DequeVariant {
        // Multi-thief or wait-idle workloads strictly need URD's
        // K_consumer engagement, so URD outranks every variant even
        // though KHL has higher overall axis count.
        if shape.n_thieves >= 2 || shape.wait_idle {
            return DequeVariant::Urd;
        }
        let required = shape.required_signature();
        // For single-thief shapes: per-item dispatch (empty
        // required) routes to Chase-Lev (the simplest variant that
        // satisfies the empty requirement); batched dispatch routes
        // to KHL (the highest-engagement variant that satisfies the
        // K_inner + K_outer requirement). This mirrors `pick`
        // exactly: empty required => Chase-Lev; non-empty => KHL.
        if required == AxisMask::EMPTY {
            return DequeVariant::ChaseLev;
        }
        // Non-empty single-thief: prefer the highest-engagement
        // variant that satisfies the requirement.
        const ORDER: [DequeVariant; 4] = [
            DequeVariant::Khl,
            DequeVariant::Khpd,
            DequeVariant::Loh,
            DequeVariant::ChaseLev,
        ];
        for v in ORDER {
            if v.signature().satisfies(required) {
                return v;
            }
        }
        DequeVariant::ChaseLev
    }

    /// Pick the right variant for `shape`, falling through to the
    /// next-best available variant when the primary pick is not
    /// configured on this dispatcher.
    ///
    /// Fallback chain (in order): primary -> KHPD -> LOH -> Chase-Lev
    /// -> URD. Returns `None` only when no variant is configured at
    /// all.
    pub fn pick_with_fallback(&self, shape: WorkloadShape) -> Option<DequeVariant> {
        let primary = Self::pick(shape);
        // Fallback ordering: primary first, then strongest-to-weakest
        // by measured single-thief K=64 throughput on Zen+ R7 2700.
        let order: [DequeVariant; 6] = [
            primary,
            DequeVariant::Khl,
            DequeVariant::Khpd,
            DequeVariant::Loh,
            DequeVariant::ChaseLev,
            DequeVariant::Urd,
        ];
        order.into_iter().find(|&v| self.is_configured(v))
    }

    /// Check whether `variant` is configured on this dispatcher.
    pub fn is_configured(&self, variant: DequeVariant) -> bool {
        match variant {
            DequeVariant::ChaseLev => self.chase_lev.is_some(),
            DequeVariant::Khpd => self.khpd.is_some(),
            DequeVariant::Loh => self.loh.is_some(),
            DequeVariant::Urd => self.urd.is_some(),
            DequeVariant::Khl => self.khl.is_some(),
        }
    }

    /// Dispatch a single item under `shape`. Routes to whichever
    /// variant the [`pick_with_fallback`](Self::pick_with_fallback)
    /// decision selects. Returns the chosen variant so observers can
    /// confirm the routing.
    ///
    /// For per-item dispatch the natural target is Chase-Lev (one
    /// Release-store on `bottom` per push). KHPD / LOH / URD all
    /// accept a single-item batch and degrade to one slot write per
    /// call.
    pub fn dispatch_one(
        &self,
        shape: WorkloadShape,
        item: LineItem,
    ) -> Result<DequeVariant, DispatchError> {
        let variant = self
            .pick_with_fallback(shape)
            .ok_or(DispatchError::BackendNotConfigured(DequeVariant::ChaseLev))?;
        match variant {
            DequeVariant::ChaseLev => {
                let h = self
                    .chase_lev
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::ChaseLev))?;
                h.push(&item).map_err(|_| DispatchError::PushFailed("ChaseLev::push"))?;
            }
            DequeVariant::Khpd => {
                let h = self
                    .khpd
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Khpd))?;
                h.publish_batch(std::slice::from_ref(&item))
                    .map_err(|_| DispatchError::PushFailed("Khpd::publish_batch"))?;
            }
            DequeVariant::Loh => {
                let h = self
                    .loh
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Loh))?;
                h.publish_batch(std::slice::from_ref(&item))
                    .map_err(|_| DispatchError::PushFailed("Loh::publish_batch"))?;
            }
            DequeVariant::Urd => {
                let h = self
                    .urd
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Urd))?;
                h.publish_round_robin(std::slice::from_ref(&item))
                    .map_err(|_| DispatchError::PushFailed("Urd::publish_round_robin"))?;
            }
            DequeVariant::Khl => {
                let h = self
                    .khl
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Khl))?;
                h.publish_batch(std::slice::from_ref(&item))
                    .map_err(|_| DispatchError::PushFailed("Khl::publish_batch"))?;
            }
        }
        Ok(variant)
    }

    /// Dispatch a batch of items under `shape`. Routes per
    /// [`pick_with_fallback`](Self::pick_with_fallback). Returns the
    /// chosen variant.
    ///
    /// For Chase-Lev (per-item primitive) the batch is pushed one
    /// item at a time. For KHPD / LOH the batch is published via
    /// the variant's `publish_batch` hot path. For URD the batch is
    /// chunked by `MAILBOX_ITEMS = 3` and round-robined across the
    /// configured mailboxes.
    pub fn dispatch_batch(
        &self,
        shape: WorkloadShape,
        items: &[LineItem],
    ) -> Result<DequeVariant, DispatchError> {
        if items.is_empty() {
            return self
                .pick_with_fallback(shape)
                .ok_or(DispatchError::BackendNotConfigured(DequeVariant::ChaseLev));
        }
        let variant = self
            .pick_with_fallback(shape)
            .ok_or(DispatchError::BackendNotConfigured(DequeVariant::ChaseLev))?;
        match variant {
            DequeVariant::ChaseLev => {
                let h = self
                    .chase_lev
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::ChaseLev))?;
                for item in items {
                    h.push(item)
                        .map_err(|_| DispatchError::PushFailed("ChaseLev::push"))?;
                }
            }
            DequeVariant::Khpd => {
                let h = self
                    .khpd
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Khpd))?;
                h.publish_batch(items)
                    .map_err(|_| DispatchError::PushFailed("Khpd::publish_batch"))?;
            }
            DequeVariant::Loh => {
                let h = self
                    .loh
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Loh))?;
                h.publish_batch(items)
                    .map_err(|_| DispatchError::PushFailed("Loh::publish_batch"))?;
            }
            DequeVariant::Urd => {
                let h = self
                    .urd
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Urd))?;
                use crate::shared_deque_urd::MAILBOX_ITEMS;
                for chunk in items.chunks(MAILBOX_ITEMS) {
                    h.publish_round_robin(chunk).map_err(|_| {
                        DispatchError::PushFailed("Urd::publish_round_robin")
                    })?;
                }
            }
            DequeVariant::Khl => {
                let h = self
                    .khl
                    .as_ref()
                    .ok_or(DispatchError::BackendNotConfigured(DequeVariant::Khl))?;
                h.publish_batch(items)
                    .map_err(|_| DispatchError::PushFailed("Khl::publish_batch"))?;
            }
        }
        Ok(variant)
    }
}

/// Builder for [`DequeDispatcher`].
pub struct DispatcherBuilder {
    chase_lev: Option<Arc<SharedDeque<LineItem>>>,
    khpd: Option<Arc<SharedDequeKhpd>>,
    loh: Option<Arc<SharedDequeLoh>>,
    urd: Option<Arc<SharedDequeUrd>>,
    khl: Option<Arc<SharedDequeKhl>>,
}

impl DispatcherBuilder {
    /// Create + attach a Chase-Lev deque at `path` with `capacity`
    /// slots (round up to next power of two).
    pub fn with_chase_lev<P: AsRef<Path>>(
        mut self,
        path: P,
        capacity: usize,
    ) -> io::Result<Self> {
        let d = SharedDeque::<LineItem>::create(path, capacity)
            .map_err(|e| io::Error::other(format!("Chase-Lev create: {e:?}")))?;
        self.chase_lev = Some(Arc::new(d));
        Ok(self)
    }

    /// Create + attach a KHPD at `path` with `capacity` publication
    /// lines.
    pub fn with_khpd<P: AsRef<Path>>(
        mut self,
        path: P,
        capacity: usize,
    ) -> io::Result<Self> {
        let d = SharedDequeKhpd::create(path, capacity)?;
        self.khpd = Some(Arc::new(d));
        Ok(self)
    }

    /// Create + attach a LOH at `path` with `capacity` ring slots
    /// and `flush_threshold` LIFO auto-flush threshold.
    pub fn with_loh<P: AsRef<Path>>(
        mut self,
        path: P,
        capacity: usize,
        flush_threshold: usize,
    ) -> io::Result<Self> {
        let d = SharedDequeLoh::create(path, capacity, flush_threshold)?;
        self.loh = Some(Arc::new(d));
        Ok(self)
    }

    /// Create + attach a URD at `path` with `n_mailboxes` mailboxes
    /// (one per intended thief).
    pub fn with_urd<P: AsRef<Path>>(
        mut self,
        path: P,
        n_mailboxes: usize,
    ) -> io::Result<Self> {
        let d = SharedDequeUrd::create(path, n_mailboxes)?;
        self.urd = Some(Arc::new(d));
        Ok(self)
    }

    /// Create + attach a KHL (K-axis Hierarchical LCRQ - the
    /// SubEtha-native hybrid) at `path` with `capacity` slots. Total
    /// item capacity is `capacity * 3`.
    pub fn with_khl<P: AsRef<Path>>(
        mut self,
        path: P,
        capacity: usize,
    ) -> io::Result<Self> {
        let d = SharedDequeKhl::create(path, capacity)?;
        self.khl = Some(Arc::new(d));
        Ok(self)
    }

    /// Finalize the dispatcher.
    pub fn build(self) -> DequeDispatcher {
        DequeDispatcher {
            chase_lev: self.chase_lev,
            khpd: self.khpd,
            loh: self.loh,
            urd: self.urd,
            khl: self.khl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subetha_dispatch_deque_{pid}_{nonce}_{name}.bin"));
        p
    }

    fn u32_item(id: u32) -> LineItem {
        LineItem::new(&id.to_le_bytes()).expect("item")
    }

    #[test]
    fn pick_request_reply_routes_to_chase_lev() {
        assert_eq!(
            DequeDispatcher::pick(WorkloadShape::request_reply()),
            DequeVariant::ChaseLev
        );
    }

    #[test]
    fn signature_pick_agrees_with_hardcoded_pick_on_all_shapes() {
        // The signature-set logic should reproduce the existing
        // hardcoded routing for every canonical workload shape.
        let shapes = [
            WorkloadShape::request_reply(),
            WorkloadShape::producer_fast(4),
            WorkloadShape::producer_fast(16),
            WorkloadShape::producer_fast(64),
            WorkloadShape::producer_fast(256),
            WorkloadShape::fan_out(2, 16),
            WorkloadShape::fan_out(4, 64),
            WorkloadShape {
                n_thieves: 1,
                batch_size: Some(8),
                wait_idle: true,
            },
        ];
        for shape in shapes {
            let hardcoded = DequeDispatcher::pick(shape);
            let signature_based = DequeDispatcher::pick_by_signature(shape);
            assert_eq!(
                hardcoded, signature_based,
                "shape {shape:?}: hardcoded picked {hardcoded:?}, signature picked {signature_based:?}",
            );
        }
    }

    #[test]
    fn variant_signatures_are_distinct() {
        // Each variant occupies a distinct corner of the design cube.
        let sigs = [
            DequeVariant::ChaseLev.signature(),
            DequeVariant::Khpd.signature(),
            DequeVariant::Loh.signature(),
            DequeVariant::Urd.signature(),
            DequeVariant::Khl.signature(),
        ];
        for i in 0..sigs.len() {
            for j in (i + 1)..sigs.len() {
                assert_ne!(
                    sigs[i], sigs[j],
                    "variants {i} and {j} share the same signature",
                );
            }
        }
    }

    #[test]
    fn request_reply_has_empty_required_signature() {
        let req = WorkloadShape::request_reply().required_signature();
        assert_eq!(req.count(), 0);
    }

    #[test]
    fn producer_fast_requires_inner_and_outer() {
        let req = WorkloadShape::producer_fast(64).required_signature();
        assert!(req.contains(Axis::Inner));
        assert!(req.contains(Axis::Outer));
    }

    #[test]
    fn fan_out_requires_consumer_and_radius() {
        let req = WorkloadShape::fan_out(4, 64).required_signature();
        assert!(req.contains(Axis::Consumer));
        assert!(req.contains(Axis::Radius));
    }

    #[test]
    fn pick_any_batch_routes_to_khl() {
        // KHL is the SubEtha-native hybrid that beats KHPD and LOH
        // empirically at single-thief batched workloads. The routing
        // picks it for any batch size K >= 2.
        assert_eq!(
            DequeDispatcher::pick(WorkloadShape::producer_fast(4)),
            DequeVariant::Khl
        );
        assert_eq!(
            DequeDispatcher::pick(WorkloadShape::producer_fast(64)),
            DequeVariant::Khl
        );
        assert_eq!(
            DequeDispatcher::pick(WorkloadShape::producer_fast(256)),
            DequeVariant::Khl
        );
    }

    #[test]
    fn pick_multi_thief_routes_to_urd() {
        assert_eq!(
            DequeDispatcher::pick(WorkloadShape::fan_out(2, 16)),
            DequeVariant::Urd
        );
        assert_eq!(
            DequeDispatcher::pick(WorkloadShape::fan_out(4, 64)),
            DequeVariant::Urd
        );
    }

    #[test]
    fn pick_wait_idle_routes_to_urd_even_single_thief() {
        let shape = WorkloadShape {
            n_thieves: 1,
            batch_size: Some(8),
            wait_idle: true,
        };
        assert_eq!(DequeDispatcher::pick(shape), DequeVariant::Urd);
    }

    #[test]
    fn pick_with_fallback_skips_unconfigured() {
        // Only Chase-Lev configured; a batch shape that picks KHL
        // falls through KHL -> KHPD -> LOH -> Chase-Lev.
        let path = tmp("fallback_cl");
        let dispatcher = DequeDispatcher::builder()
            .with_chase_lev(&path, 64)
            .expect("create cl")
            .build();
        let shape = WorkloadShape::producer_fast(8);
        assert_eq!(DequeDispatcher::pick(shape), DequeVariant::Khl);
        assert_eq!(
            dispatcher.pick_with_fallback(shape),
            Some(DequeVariant::ChaseLev)
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn dispatch_one_routes_to_chase_lev_when_per_item() {
        let cl_path = tmp("dispatch_one_cl");
        let dispatcher = DequeDispatcher::builder()
            .with_chase_lev(&cl_path, 64)
            .expect("create cl")
            .build();
        let chosen = dispatcher
            .dispatch_one(WorkloadShape::request_reply(), u32_item(42))
            .expect("dispatch_one");
        assert_eq!(chosen, DequeVariant::ChaseLev);
        // Drain via the Chase-Lev handle.
        let cl = dispatcher.chase_lev().expect("cl");
        let got = cl.steal().expect("steal");
        assert_eq!(got, u32_item(42));
        std::fs::remove_file(&cl_path).ok();
    }

    #[test]
    fn dispatch_batch_routes_to_khl_when_configured() {
        let khl_path = tmp("dispatch_batch_khl");
        let dispatcher = DequeDispatcher::builder()
            .with_khl(&khl_path, 256)
            .expect("create khl")
            .build();
        let items: Vec<LineItem> = (0..64u32).map(u32_item).collect();
        let chosen = dispatcher
            .dispatch_batch(WorkloadShape::producer_fast(64), &items)
            .expect("dispatch_batch");
        assert_eq!(chosen, DequeVariant::Khl);
        // 64 items = 22 slots (ceil(64/3)).
        let khl = dispatcher.khl().expect("khl");
        let (_, tail, _) = khl.snapshot_size();
        assert_eq!(tail, 22);
        std::fs::remove_file(&khl_path).ok();
    }

    #[test]
    fn dispatch_batch_falls_through_to_khpd_when_khl_unconfigured() {
        // No KHL configured; KHPD next in fallback chain.
        let khpd_path = tmp("fallback_khpd");
        let dispatcher = DequeDispatcher::builder()
            .with_khpd(&khpd_path, 64)
            .expect("create khpd")
            .build();
        let items: Vec<LineItem> = (0..6u32).map(u32_item).collect();
        let chosen = dispatcher
            .dispatch_batch(WorkloadShape::producer_fast(6), &items)
            .expect("dispatch_batch");
        assert_eq!(chosen, DequeVariant::Khpd);
        let khpd = dispatcher.khpd().expect("khpd");
        let (_, tail, _, _) = khpd.snapshot_size();
        assert_eq!(tail, 2);
        std::fs::remove_file(&khpd_path).ok();
    }

    #[test]
    fn dispatch_batch_falls_through_to_loh_when_khl_khpd_unconfigured() {
        // No KHL, no KHPD configured; LOH next in fallback chain.
        let loh_path = tmp("fallback_loh");
        let dispatcher = DequeDispatcher::builder()
            .with_loh(&loh_path, 512, usize::MAX)
            .expect("create loh")
            .build();
        let items: Vec<LineItem> = (0..200u32).map(u32_item).collect();
        let chosen = dispatcher
            .dispatch_batch(WorkloadShape::producer_fast(200), &items)
            .expect("dispatch_batch");
        assert_eq!(chosen, DequeVariant::Loh);
        let loh = dispatcher.loh().expect("loh");
        let (_, tail, _, _) = loh.snapshot_size();
        assert_eq!(tail, 200);
        std::fs::remove_file(&loh_path).ok();
    }

    #[test]
    fn dispatch_batch_routes_to_urd_for_multi_thief() {
        let urd_path = tmp("dispatch_batch_urd");
        let dispatcher = DequeDispatcher::builder()
            .with_urd(&urd_path, 2)
            .expect("create urd")
            .build();
        let items: Vec<LineItem> = (0..6u32).map(u32_item).collect();
        let chosen = dispatcher
            .dispatch_batch(WorkloadShape::fan_out(2, 6), &items)
            .expect("dispatch_batch");
        assert_eq!(chosen, DequeVariant::Urd);
        std::fs::remove_file(&urd_path).ok();
    }

    #[test]
    fn full_dispatcher_round_trips_mixed_shapes() {
        // Full dispatcher with Chase-Lev + KHL configured.
        // Per-item shape routes to Chase-Lev; batch shape routes to
        // KHL. Drain both sides and verify bit-exact recovery.
        let cl_path = tmp("full_cl");
        let khl_path = tmp("full_khl");
        let dispatcher = DequeDispatcher::builder()
            .with_chase_lev(&cl_path, 128)
            .expect("create cl")
            .with_khl(&khl_path, 64)
            .expect("create khl")
            .build();

        // 5 per-item dispatches -> Chase-Lev.
        for i in 0..5u32 {
            let v = dispatcher
                .dispatch_one(WorkloadShape::request_reply(), u32_item(i))
                .expect("dispatch_one");
            assert_eq!(v, DequeVariant::ChaseLev);
        }
        // 12-item batch -> KHL.
        let batch: Vec<LineItem> = (100..112u32).map(u32_item).collect();
        let v = dispatcher
            .dispatch_batch(WorkloadShape::producer_fast(12), &batch)
            .expect("dispatch_batch");
        assert_eq!(v, DequeVariant::Khl);

        // Drain Chase-Lev.
        let cl = dispatcher.chase_lev().expect("cl");
        let mut seen = Vec::new();
        while let Some(x) = cl.steal() {
            seen.push(x);
        }
        assert_eq!(seen.len(), 5);
        for (i, item) in seen.iter().enumerate() {
            assert_eq!(*item, u32_item(i as u32));
        }

        // Drain KHL.
        let khl = dispatcher.khl().expect("khl");
        let mut drained = Vec::new();
        loop {
            match khl.steal_slot() {
                crate::shared_deque_khl::Steal::Success(r) => {
                    for i in 0..r.n_items {
                        drained.push(r.items[i]);
                    }
                }
                crate::shared_deque_khl::Steal::Empty => break,
                crate::shared_deque_khl::Steal::Retry => continue,
            }
        }
        assert_eq!(drained.len(), 12);
        for (i, item) in drained.iter().enumerate() {
            assert_eq!(*item, u32_item(100 + i as u32));
        }

        std::fs::remove_file(&cl_path).ok();
        std::fs::remove_file(&khl_path).ok();
    }
}
