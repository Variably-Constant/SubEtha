//! `AdaptiveIpc<T>`: runtime profile-and-migrate IPC, kernel-bypass
//! preserved end-to-end, hot path optimised to ~zero overhead vs
//! direct dispatch.
//!
//! The optimisation pattern (named by an external scheduler-agent
//! finding and verified locally):
//!
//! - **Wrong**: `arc_swap::ArcSwapOption<Arc<dyn MessageTransport>>`
//!   per slot, hot path = ArcSwap::load_full + Arc clone + vtable
//!   indirect call. Measured: **+18-30 ns/op vs direct**, +163%.
//! - **Right**: pre-allocate both possible backings as concrete
//!   types in the struct, use an `AtomicU32` tag to select which
//!   is active, dispatch through a static enum-style `match`.
//!   On x86 TSO the Acquire-load lowers to a plain MOV; the match
//!   is a `cmp+jmp` that branch-predicts on the rare-migration
//!   common case; `#[inline]` collapses the wrapper into a direct
//!   call. Measured: **~0 ns/op vs direct**, statistically
//!   identical noise.
//!
//! The architectural property (kernel-bypass through live family
//! migration via two MMF backings + MMF-resident control flag)
//! is preserved. The migration handoff is one `mmap()` for the
//! new backing at construction (both backings pre-mmap'd) plus a
//! single `Release`-store on the MMF-resident control atom when
//! the dispatcher decides to flip. No syscalls on the per-op path.
//!
//! Per-op cost on Zen+ R7 2700 (measured by `concurrent_methods`
//! bench): within noise of `SharedRing::try_push` direct, despite
//! providing runtime family migration + profile counters.

#![allow(clippy::missing_errors_doc)]

use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use parking_lot::Mutex;
use subetha_core::Marshal;

use crate::adaptive_ring::{AdaptiveRing, RingShape};
use crate::api::{map_waker, wait_heal, ApiError};
use crate::cross_process_waker::{CrossProcessWaker, MAX_WAITERS_DEFAULT};
use crate::message_transport::{PassSlot, TransportError};
use crate::mmf_dispatcher::{MmfDispatcher, MmfFamily, MmfWorkloadShape};
use crate::ordering::{monotonic_nanos, OrderingMode};
use crate::qos_policy::Ordering as QosOrdering;
use crate::shared_atomic::{SharedAtomicU32, SharedAtomicU64};
use crate::shared_deque::SharedDeque;
use crate::shared_deque_khl::{SharedDequeKhl, Steal as KhlSteal};
use crate::shared_deque_khpd::{LineItem, KHPD_ITEM_BYTES};
use crate::shared_ring::PAYLOAD_BYTES;

/// Tag values for the `AtomicU32` active-backing flag.
const TAG_RING: u32 = 0;
const TAG_DEQUE: u32 = 1;

/// Profile counters tracked at every send. `AdaptiveIpc::maybe_promote`
/// inspects these and decides whether to migrate.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProfileSnapshot {
    /// Total individual sends since last reset.
    pub total_sends: u64,
    /// Total batched sends since last reset.
    pub batch_sends: u64,
    /// Sum of all batch sizes seen.
    pub batch_size_sum: u64,
    /// Maximum batch size observed.
    pub max_batch_size: u64,
}

impl ProfileSnapshot {
    /// Average batch size across observed batched calls (1 if none).
    pub fn avg_batch_size(&self) -> u64 {
        self.batch_size_sum.checked_div(self.batch_sends).unwrap_or(1)
    }

    /// Ratio of batched calls to total calls in [0.0, 1.0].
    pub fn batch_ratio(&self) -> f64 {
        let total = self.total_sends + self.batch_sends;
        if total == 0 {
            0.0
        } else {
            self.batch_sends as f64 / total as f64
        }
    }

    /// Infer a workload shape from observed patterns.
    pub fn inferred_shape(&self, n_consumers: usize) -> MmfWorkloadShape {
        if self.batch_ratio() >= 0.5 || self.max_batch_size >= 8 {
            MmfWorkloadShape::WorkStealing(
                crate::dispatch_deque::WorkloadShape {
                    n_thieves: n_consumers,
                    batch_size: Some(self.avg_batch_size().max(2) as usize),
                    wait_idle: false,
                },
            )
        } else {
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 1,
                n_consumers,
            }
        }
    }
}

/// Runtime profile-and-migrate IPC endpoint with **zero-overhead
/// hot path**. Both possible backings (a `SharedRing` and a
/// `SharedDeque<PassSlot>`) are pre-allocated at construction;
/// an `AtomicU32` tag selects which is active. Migration is a
/// single Release-store on the MMF-resident control atom.
pub struct AdaptiveIpc<T: Marshal + Copy + 'static> {
    /// Cross-process control flag indexing the active backing.
    /// Lives in its own small MMF so cross-process consumers see
    /// the same flag.
    control: Arc<SharedAtomicU32>,
    /// Cross-process pin generation. Bumped on every successful
    /// `migrate_to`. Pinned-handle holders capture this value at
    /// pin time and call `is_still_valid()` to see whether a
    /// migration has happened. MMF-resident so remote pin holders
    /// (cross-process consumers) see invalidation through the same
    /// kernel-bypass channel as the control flag.
    pin_generation: Arc<SharedAtomicU64>,
    /// Pre-allocated `AdaptiveRing` backing (used when tag = TAG_RING).
    /// Composes the shape-axis morph protocol underneath the
    /// protocol-axis migration: while the IPC family is `SharedRing`,
    /// this AdaptiveRing morphs across SPSC/MPSC/MPMC/Vyukov shapes
    /// driven by its own sidecar based on observed peer counts.
    /// Default-on; callers wanting Vyukov-only behavior can reach
    /// through `ring_handle()` and `morph_to(RingShape::Vyukov)`.
    ring: AdaptiveRing,
    /// Pre-allocated `SharedDeque<PassSlot>` backing (used when tag
    /// = TAG_DEQUE). The `PassSlot` newtype carries the payload
    /// bytes in a position-independent layout.
    deque: SharedDeque<PassSlot>,
    /// Optional KHL (3-items-per-cache-line) batched-send fast path.
    /// `Some` only when `T::PAYLOAD_BYTES <= KHPD_ITEM_BYTES` (16),
    /// since KHL's `LineItem` slot is 16 bytes. A `send_batch` of >= 2
    /// items routes here at runtime - KHL publishes 3 items per
    /// Release-store (measured ~3x cheaper per item than the Chase-Lev
    /// deque on batched producers). Drained by `recv` alongside the
    /// ring + deque. Independent of the ring<->deque migration tag: it
    /// is a parallel side-backing, not a migration target.
    khl: Option<Arc<SharedDequeKhl>>,
    /// Surplus from a KHL slot steal. `steal_slot` returns up to 3
    /// `LineItem`s per slot; `recv` yields one item per call, so the
    /// 0..=2 surplus items wait here for the next `recv`. Shared (any
    /// consumer drains it), so a stopped consumer never strands items.
    khl_surplus: Mutex<Vec<LineItem>>,
    /// Base path stem. Backings live at `{base_path}.ring.bin` +
    /// `{base_path}.deque.bin`; control flag at `{base_path}.ctl.bin`;
    /// pin generation at `{base_path}.pingen.bin`.
    base_path: PathBuf,
    /// Profile counters as separate atomics so the hot path pays
    /// exactly ONE `fetch_add` per send.
    total_sends_atom: AtomicU64,
    batch_sends_atom: AtomicU64,
    batch_size_sum_atom: AtomicU64,
    max_batch_size_atom: AtomicU64,
    /// Bloom64 of observed workload-shape signatures. Every send /
    /// send_batch sets the bits derived from its shape signature
    /// (e.g. (shape_kind, log2_batch_size_bucket)); `maybe_promote`
    /// consults this for O(1) pattern recognition instead of
    /// re-deriving from the per-call counters.
    shape_bloom_atom: AtomicU64,
    /// Declared consumer count (used by `inferred_shape` when the
    /// profile suggests a migration).
    n_consumers: usize,
    /// Pre-authorized automatic ordering response: when set (via
    /// [`create_with_ordering`](Self::create_with_ordering)), the
    /// sidecar's `maybe_promote` poll arms the stamped ring's merge
    /// flag once the observed inversion rate (inversions/sec)
    /// crosses this threshold. `None` = report-only.
    auto_order_threshold: Option<f64>,
    /// Inversion-rate bookkeeping for the auto-order check.
    last_inversions_atom: AtomicU64,
    last_inversion_check_nanos: AtomicU64,
    /// Producer fires on send; a blocking / awaiting recv waits here.
    consumer_waker: Arc<CrossProcessWaker>,
    /// Consumer fires on recv; a blocking / awaiting send waits here.
    producer_waker: Arc<CrossProcessWaker>,
    /// The awaiting consumer's `Waker`, fired directly (in-process).
    recv_slot: Arc<Mutex<Option<Waker>>>,
    /// The awaiting producer's `Waker`.
    send_slot: Arc<Mutex<Option<Waker>>>,
    /// Monotonic send / recv counters: the keys blocking waiters park
    /// on, backing-independent so they survive a ring<->deque migration.
    published: AtomicU64,
    consumed: AtomicU64,
    /// Set once a recv / send blocks or awaits. Gates the wake signal so
    /// a pure-sync endpoint pays nothing for the async machinery.
    has_recv_waiter: AtomicBool,
    has_send_waiter: AtomicBool,
    _phantom: PhantomData<T>,
}

impl<T: Marshal + Copy + 'static> AdaptiveIpc<T> {
    /// Create a new AdaptiveIpc at `base_path` with an initial
    /// workload shape. Both backings (ring + deque) are
    /// pre-allocated; the initial shape selects which one is
    /// active at start.
    pub fn create(
        base_path: impl Into<PathBuf>,
        initial_shape: MmfWorkloadShape,
        capacity: usize,
        n_consumers: usize,
    ) -> Result<Self, ApiError> {
        if T::PAYLOAD_BYTES > PAYLOAD_BYTES {
            return Err(ApiError::PayloadTooLarge);
        }
        let base_path: PathBuf = base_path.into();
        let ctl_path = control_path_for(&base_path);
        let deque_path = deque_path_for(&base_path);
        let pingen_path = pingen_path_for(&base_path);

        let control = Arc::new(
            SharedAtomicU32::create(&ctl_path, 0)
                .map_err(|e| ApiError::Io(std::io::Error::other(format!("control: {e:?}"))))?,
        );
        let pin_generation = Arc::new(
            SharedAtomicU64::create(&pingen_path, 0)
                .map_err(|e| ApiError::Io(std::io::Error::other(format!("pingen: {e:?}"))))?,
        );
        // Sizing HINT only (the ring grows past it on demand):
        // pre-allocate one backing per expected consumer so
        // AdaptiveIpc's single-producer flow plus any callers that
        // register additional producers through
        // `ring_handle().register_producer()` start with warm
        // backings instead of growing on first registration.
        let max_producers = n_consumers.max(1);
        let ring_prefix = ring_path_prefix_for(&base_path);
        let ring = AdaptiveRing::create(
            &ring_prefix,
            max_producers,
            n_consumers.max(1),
            capacity,
        )
        .map_err(|e| ApiError::Io(std::io::Error::other(format!("ring: {e:?}"))))?;
        // Register one producer + n_consumers consumers so the
        // shape-axis sidecar sees the right initial peer counts.
        ring.register_producer()
            .map_err(|e| ApiError::Io(std::io::Error::other(format!("ring register_producer: {e:?}"))))?;
        for _ in 0..n_consumers.max(1) {
            ring.register_consumer()
                .map_err(|e| ApiError::Io(std::io::Error::other(format!("ring register_consumer: {e:?}"))))?;
        }
        let deque = SharedDeque::<PassSlot>::create(&deque_path, capacity)?;
        // KHL batched fast path, allocated only when the payload fits
        // KHL's 16-byte LineItem slot. Item capacity = capacity * 3.
        let khl = if T::PAYLOAD_BYTES <= KHPD_ITEM_BYTES {
            Some(Arc::new(
                SharedDequeKhl::create(khl_path_for(&base_path), capacity)
                    .map_err(|e| ApiError::Io(std::io::Error::other(format!("khl: {e:?}"))))?,
            ))
        } else {
            None
        };

        let initial_family = MmfDispatcher::pick(initial_shape);
        let initial_tag = match initial_family {
            MmfFamily::SharedRing => TAG_RING,
            MmfFamily::SharedDeque(_) => TAG_DEQUE,
            MmfFamily::SharedHashMap => {
                return Err(ApiError::WrongFamily {
                    wanted: "SharedRing or SharedDeque",
                    got: initial_family,
                });
            }
        };
        control.store(initial_tag, Ordering::Release);

        let consumer_waker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).map_err(|e| {
                ApiError::Io(std::io::Error::other(format!("consumer waker: {e:?}")))
            })?,
        );
        let producer_waker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).map_err(|e| {
                ApiError::Io(std::io::Error::other(format!("producer waker: {e:?}")))
            })?,
        );

        Ok(Self {
            control,
            pin_generation,
            ring,
            deque,
            khl,
            khl_surplus: Mutex::new(Vec::new()),
            base_path,
            total_sends_atom: AtomicU64::new(0),
            batch_sends_atom: AtomicU64::new(0),
            batch_size_sum_atom: AtomicU64::new(0),
            max_batch_size_atom: AtomicU64::new(0),
            shape_bloom_atom: AtomicU64::new(0),
            n_consumers,
            auto_order_threshold: None,
            last_inversions_atom: AtomicU64::new(0),
            last_inversion_check_nanos: AtomicU64::new(monotonic_nanos()),
            consumer_waker,
            producer_waker,
            recv_slot: Arc::new(Mutex::new(None)),
            send_slot: Arc::new(Mutex::new(None)),
            published: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            has_recv_waiter: AtomicBool::new(false),
            has_send_waiter: AtomicBool::new(false),
            _phantom: PhantomData,
        })
    }

    /// As [`create`](Self::create) with the ordering axis wired in:
    /// the inner `AdaptiveRing` is constructed STAMPED (push stamps
    /// plus the cross-process ordering header), the `ordering`
    /// declaration is applied immediately, and `auto_order` - when
    /// set - pre-authorizes the sidecar's `maybe_promote` poll to
    /// arm the merge flag once the observed inversion rate crosses
    /// the threshold (inversions/sec).
    ///
    /// The payload cap is unchanged: `T::PAYLOAD_BYTES <= 56` was
    /// already the `AdaptiveIpc` contract (the Vyukov backing's
    /// slot size), and the stamped slot leaves the same 56 bytes.
    pub fn create_with_ordering(
        base_path: impl Into<PathBuf>,
        initial_shape: MmfWorkloadShape,
        capacity: usize,
        n_consumers: usize,
        ordering: QosOrdering,
        auto_order: Option<f64>,
    ) -> Result<Self, ApiError> {
        if T::PAYLOAD_BYTES > PAYLOAD_BYTES {
            return Err(ApiError::PayloadTooLarge);
        }
        let base_path: PathBuf = base_path.into();
        let ctl_path = control_path_for(&base_path);
        let deque_path = deque_path_for(&base_path);
        let pingen_path = pingen_path_for(&base_path);

        let control = Arc::new(
            SharedAtomicU32::create(&ctl_path, 0)
                .map_err(|e| ApiError::Io(std::io::Error::other(format!("control: {e:?}"))))?,
        );
        let pin_generation = Arc::new(
            SharedAtomicU64::create(&pingen_path, 0)
                .map_err(|e| ApiError::Io(std::io::Error::other(format!("pingen: {e:?}"))))?,
        );
        let max_producers = n_consumers.max(1);
        let ring_prefix = ring_path_prefix_for(&base_path);
        let ring = AdaptiveRing::create(
            &ring_prefix,
            max_producers,
            n_consumers.max(1),
            capacity,
        )
        .map_err(|e| ApiError::Io(std::io::Error::other(format!("ring: {e:?}"))))?
        .with_ordering_stamps()
        .map_err(|e| ApiError::Io(std::io::Error::other(format!("ordering: {e:?}"))))?;
        ring.register_producer()
            .map_err(|e| ApiError::Io(std::io::Error::other(format!("ring register_producer: {e:?}"))))?;
        for _ in 0..n_consumers.max(1) {
            ring.register_consumer()
                .map_err(|e| ApiError::Io(std::io::Error::other(format!("ring register_consumer: {e:?}"))))?;
        }
        let deque = SharedDeque::<PassSlot>::create(&deque_path, capacity)?;
        // KHL batched fast path, allocated only when the payload fits
        // KHL's 16-byte LineItem slot. Item capacity = capacity * 3.
        let khl = if T::PAYLOAD_BYTES <= KHPD_ITEM_BYTES {
            Some(Arc::new(
                SharedDequeKhl::create(khl_path_for(&base_path), capacity)
                    .map_err(|e| ApiError::Io(std::io::Error::other(format!("khl: {e:?}"))))?,
            ))
        } else {
            None
        };

        let initial_family = MmfDispatcher::pick(initial_shape);
        let initial_tag = match initial_family {
            MmfFamily::SharedRing => TAG_RING,
            MmfFamily::SharedDeque(_) => TAG_DEQUE,
            MmfFamily::SharedHashMap => {
                return Err(ApiError::WrongFamily {
                    wanted: "SharedRing or SharedDeque",
                    got: initial_family,
                });
            }
        };
        control.store(initial_tag, Ordering::Release);

        let consumer_waker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).map_err(|e| {
                ApiError::Io(std::io::Error::other(format!("consumer waker: {e:?}")))
            })?,
        );
        let producer_waker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).map_err(|e| {
                ApiError::Io(std::io::Error::other(format!("producer waker: {e:?}")))
            })?,
        );

        let ipc = Self {
            control,
            pin_generation,
            ring,
            deque,
            khl,
            khl_surplus: Mutex::new(Vec::new()),
            base_path,
            total_sends_atom: AtomicU64::new(0),
            batch_sends_atom: AtomicU64::new(0),
            batch_size_sum_atom: AtomicU64::new(0),
            max_batch_size_atom: AtomicU64::new(0),
            shape_bloom_atom: AtomicU64::new(0),
            n_consumers,
            auto_order_threshold: auto_order,
            last_inversions_atom: AtomicU64::new(0),
            last_inversion_check_nanos: AtomicU64::new(monotonic_nanos()),
            consumer_waker,
            producer_waker,
            recv_slot: Arc::new(Mutex::new(None)),
            send_slot: Arc::new(Mutex::new(None)),
            published: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            has_recv_waiter: AtomicBool::new(false),
            has_send_waiter: AtomicBool::new(false),
            _phantom: PhantomData,
        };
        ipc.set_ordering(ordering)?;
        Ok(ipc)
    }

    /// Apply an ordering declaration at runtime. Routing follows
    /// the substrate's two paths:
    ///
    /// - **Stamped ring** (constructed via
    ///   [`create_with_ordering`](Self::create_with_ordering)):
    ///   `GlobalFifo` flips the merge flag ON
    ///   (`OrderingMode::MergeByStamp` - the cheap ordered switch,
    ///   retroactive over the backlog), `PerProducer` flips it OFF.
    /// - **Unstamped ring** (plain [`create`](Self::create)):
    ///   `GlobalFifo` morphs the ring to the Vyukov shape (the
    ///   proven global-FIFO structure); `PerProducer` morphs back
    ///   to the counts-based composed shape.
    pub fn set_ordering(&self, ordering: QosOrdering) -> Result<(), ApiError> {
        if self.ring.is_stamped() {
            let mode = match ordering {
                QosOrdering::GlobalFifo => OrderingMode::MergeByStamp,
                QosOrdering::PerProducer => OrderingMode::Unordered,
            };
            self.ring.set_ordering_mode(mode).map_err(map_ring_err)?;
            return Ok(());
        }
        match ordering {
            QosOrdering::GlobalFifo => {
                if self.ring.current_shape() != RingShape::Vyukov {
                    self.ring.morph_to(RingShape::Vyukov).map_err(map_ring_err)?;
                }
            }
            QosOrdering::PerProducer => {
                // Back to the automatic counts-based shape: undo the
                // GlobalFifo pin and let the ring re-track its live
                // peer counts (now and on every future change).
                self.ring.resume_auto_shape();
            }
        }
        Ok(())
    }

    /// The ordering guarantee currently provided, derived from the
    /// live substrate state (merge flag for stamped rings, shape
    /// for unstamped ones).
    pub fn ordering(&self) -> QosOrdering {
        if self.ring.is_stamped() {
            match self.ring.ordering_mode() {
                Some(OrderingMode::Unordered) | None => QosOrdering::PerProducer,
                Some(_) => QosOrdering::GlobalFifo,
            }
        } else if self.ring.current_shape() == RingShape::Vyukov {
            QosOrdering::GlobalFifo
        } else {
            QosOrdering::PerProducer
        }
    }

    /// Cross-producer inversions the stamped ring has observed
    /// (0 for unstamped rings).
    pub fn inversions(&self) -> u64 {
        self.ring.inversions()
    }

    /// Send one item. Hot path: read tag (Acquire on MMF-resident
    /// atom, no kernel touch, plain MOV on x86), match-dispatch to
    /// pre-allocated concrete backing, push, record send. No Arc
    /// clone, no vtable lookup.
    ///
    /// **Type-specialized fast paths are dispatched automatically at
    /// compile time** via `TypeId::of::<T>()` constant comparisons.
    /// For `T = u64`, the branch monomorphizes to a direct call to
    /// [`send_u64`](Self::send_u64), guaranteeing the 8-byte
    /// stack-buffer path instead of the generic 56-byte `Marshal`
    /// buffer. The A/B harness
    /// (`benches/adaptive_send_specialized_ab.rs`) measures the two
    /// paths within noise on the current toolchain (~1.05x on Zen+
    /// R7 2700: generic 2.01 ms vs specialized 1.92 ms) - LLVM
    /// already inlines the generic `u64` Marshal path to equivalent
    /// code, so the branch's value is the small-buffer GUARANTEE
    /// across toolchains, not a measured win on this host.
    /// For other `T`, the branch monomorphizes away to the generic
    /// path.
    #[inline]
    pub fn send(&self, item: &T) -> Result<(), ApiError> {
        // Compile-time specialization: TypeId::of is a const fn so
        // this comparison is a known constant in each monomorphization
        // and LLVM eliminates the dead branch.
        if core::any::TypeId::of::<T>() == core::any::TypeId::of::<u64>() {
            // SAFETY: TypeId equality guarantees T is u64; the
            // transmute reads the same bytes that a `*item: T` read
            // would. Verified by the type check above.
            let val: u64 = unsafe { *(item as *const T as *const u64) };
            return self.send_u64(val);
        }
        let tag = self.control.load(Ordering::Acquire);
        let mut buf = [0u8; PAYLOAD_BYTES];
        item.marshal(&mut buf[..T::PAYLOAD_BYTES]);
        match tag {
            TAG_RING => {
                self.ring.try_send(0, &buf[..T::PAYLOAD_BYTES])
                    .map_err(map_ring_err)?;
            }
            TAG_DEQUE => {
                let mut slot = PassSlot([0u8; PAYLOAD_BYTES]);
                slot.0[..T::PAYLOAD_BYTES]
                    .copy_from_slice(&buf[..T::PAYLOAD_BYTES]);
                self.deque.push(&slot)?;
            }
            _ => return Err(ApiError::Transport(TransportError::Other)),
        }
        self.total_sends_atom.fetch_add(1, Ordering::Relaxed);
        self.signal_consumer();
        Ok(())
    }

    /// Specialized `u64` send fast path. The `T: Marshal` indirection
    /// is eliminated, the payload buffer is exactly 8 bytes (not 56),
    /// and the `SharedRing` / `SharedDeque` dispatch sees a concrete
    /// known-size payload that LLVM can inline directly.
    ///
    /// Same wire format as `send(&u64_value)`: receivers see the same
    /// 8-byte payload prefix in the slot. Use this when sending
    /// homogeneous u64 streams (tokens, sequence numbers, message
    /// IDs) where the generic `Marshal` path is overhead. `send`
    /// itself auto-routes here via a `TypeId`-monomorphised branch
    /// when `T = u64`, so callers rarely need to name `send_u64`
    /// directly.
    #[inline]
    pub fn send_u64(&self, item: u64) -> Result<(), ApiError> {
        let tag = self.control.load(Ordering::Acquire);
        let buf = item.to_le_bytes();
        match tag {
            TAG_RING => {
                self.ring.try_send(0, &buf)
                    .map_err(map_ring_err)?;
            }
            TAG_DEQUE => {
                let mut slot = PassSlot([0u8; PAYLOAD_BYTES]);
                slot.0[..8].copy_from_slice(&buf);
                self.deque.push(&slot)?;
            }
            _ => return Err(ApiError::Transport(TransportError::Other)),
        }
        self.total_sends_atom.fetch_add(1, Ordering::Relaxed);
        self.signal_consumer();
        Ok(())
    }

    /// Send a batch of items. All-or-nothing: this returns `Ok` only
    /// after every item is in the backing. The implementation
    /// re-reads the active tag per item so a migration that lands
    /// mid-batch routes the remaining items to the new backing
    /// rather than the old; it spins on `Full` (backpressure) the
    /// same way single `send` callers spin on `Err`, and propagates
    /// any non-`Full` error immediately.
    ///
    /// The atomic-or-spin guarantee is what makes naive caller loops
    /// of the form `while send_batch(&b).is_err() { spin }` safe:
    /// without it, partial-success-then-Err returns would prompt the
    /// caller to retry the whole batch, double-sending the items
    /// that already landed.
    pub fn send_batch(&self, items: &[T]) -> Result<(), ApiError> {
        if items.is_empty() {
            return Ok(());
        }
        // KHL batched fast path (additive: the per-item path below is
        // unchanged). A batch of >= 2 items whose payload fits KHL's
        // 16-byte LineItem (`khl` is `Some` only then) routes to KHL,
        // which publishes 3 items per Release-store. Items land in the
        // khl side-backing, drained by `recv` alongside ring + deque.
        if items.len() >= 2
            && let Some(khl) = self.khl.as_ref()
        {
            self.publish_batch_khl(khl, items)?;
            self.record_batch_profile(items.len() as u64);
            return Ok(());
        }
        let mut buf = [0u8; PAYLOAD_BYTES];
        let mut sent = 0usize;
        while sent < items.len() {
            items[sent].marshal(&mut buf[..T::PAYLOAD_BYTES]);
            let tag = self.control.load(Ordering::Acquire);
            let result: Result<(), ApiError> = match tag {
                TAG_RING => self
                    .ring
                    .try_send(0, &buf[..T::PAYLOAD_BYTES])
                    .map_err(map_ring_err),
                TAG_DEQUE => {
                    let mut slot = PassSlot([0u8; PAYLOAD_BYTES]);
                    slot.0[..T::PAYLOAD_BYTES]
                        .copy_from_slice(&buf[..T::PAYLOAD_BYTES]);
                    self.deque.push(&slot).map_err(ApiError::from)
                }
                _ => return Err(ApiError::Transport(TransportError::Other)),
            };
            match result {
                Ok(()) => sent += 1,
                Err(ApiError::Transport(TransportError::Full)) => {
                    std::hint::spin_loop();
                }
                Err(e) => return Err(e),
            }
        }
        let len = items.len() as u64;
        self.batch_sends_atom.fetch_add(1, Ordering::Relaxed);
        self.batch_size_sum_atom.fetch_add(len, Ordering::Relaxed);
        let mut cur = self.max_batch_size_atom.load(Ordering::Relaxed);
        while len > cur {
            match self.max_batch_size_atom.compare_exchange_weak(
                cur,
                len,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
        // Bloom-track the batched shape: kind=1, bucket = log2(len).
        let log2_bucket = (64u32 - len.leading_zeros()).saturating_sub(1);
        let mut bloom = subetha_pointers::bloom_pointer::Bloom64(
            self.shape_bloom_atom.load(Ordering::Relaxed),
        );
        bloom.insert(&(1u32, log2_bucket));
        self.shape_bloom_atom.store(bloom.0, Ordering::Relaxed);
        Ok(())
    }

    /// Publish a batch through the KHL side-backing, marshalling each
    /// item into a 16-byte `LineItem` and publishing one slot
    /// (`KHL_ITEMS_PER_SLOT` items) per `publish_batch` call from a
    /// stack array (no allocation). Spins on backpressure (a partial
    /// publish) exactly as the per-item `send_batch` path spins on
    /// `Full`.
    fn publish_batch_khl(&self, khl: &SharedDequeKhl, items: &[T]) -> Result<(), ApiError> {
        use crate::shared_deque_khl::KHL_ITEMS_PER_SLOT;
        let mut chunk = [LineItem::default(); KHL_ITEMS_PER_SLOT];
        let mut i = 0;
        while i < items.len() {
            let n = (items.len() - i).min(KHL_ITEMS_PER_SLOT);
            for j in 0..n {
                let mut lb = [0u8; KHPD_ITEM_BYTES];
                items[i + j].marshal(&mut lb[..T::PAYLOAD_BYTES]);
                chunk[j] = LineItem::new(&lb).map_err(|_| ApiError::PayloadTooLarge)?;
            }
            let mut done = 0;
            while done < n {
                match khl.publish_batch(&chunk[done..n]) {
                    Ok(c) => done += c,
                    Err(_) => return Err(ApiError::Transport(TransportError::Other)),
                }
                if done < n {
                    std::hint::spin_loop();
                }
            }
            i += n;
        }
        Ok(())
    }

    /// Record a batch in the profile counters. Duplicated from the
    /// per-item `send_batch` tail so the KHL fast path leaves that path
    /// byte-for-byte unchanged.
    fn record_batch_profile(&self, len: u64) {
        self.batch_sends_atom.fetch_add(1, Ordering::Relaxed);
        self.batch_size_sum_atom.fetch_add(len, Ordering::Relaxed);
        let mut cur = self.max_batch_size_atom.load(Ordering::Relaxed);
        while len > cur {
            match self.max_batch_size_atom.compare_exchange_weak(
                cur,
                len,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
        let log2_bucket = (64u32 - len.leading_zeros()).saturating_sub(1);
        let mut bloom = subetha_pointers::bloom_pointer::Bloom64(
            self.shape_bloom_atom.load(Ordering::Relaxed),
        );
        bloom.insert(&(1u32, log2_bucket));
        self.shape_bloom_atom.store(bloom.0, Ordering::Relaxed);
    }

    /// Drain one item from the KHL side-backing. Returns the buffered
    /// surplus from a prior multi-item slot steal first; otherwise
    /// steals one slot (up to `KHL_ITEMS_PER_SLOT` items), returns the
    /// first, and buffers the rest in the shared surplus so any
    /// consumer drains them (no stranding). `Retry` is transient and
    /// retried a bounded number of times before falling through.
    fn recv_from_khl(&self) -> Result<Option<T>, ApiError> {
        let Some(khl) = self.khl.as_ref() else {
            return Ok(None);
        };
        {
            let mut surplus = self.khl_surplus.lock();
            if let Some(item) = surplus.pop() {
                return Ok(Some(Self::unmarshal_line(&item)?));
            }
        }
        for _ in 0..4 {
            match khl.steal_slot() {
                KhlSteal::Success(res) => {
                    let n = res.n_items;
                    if n == 0 {
                        return Ok(None);
                    }
                    if n > 1 {
                        let mut surplus = self.khl_surplus.lock();
                        // Push items[1..n] reversed so `pop` yields them
                        // in producer order.
                        for k in (1..n).rev() {
                            surplus.push(res.items[k]);
                        }
                    }
                    return Ok(Some(Self::unmarshal_line(&res.items[0])?));
                }
                KhlSteal::Empty => return Ok(None),
                KhlSteal::Retry => continue,
            }
        }
        Ok(None)
    }

    #[inline]
    fn unmarshal_line(item: &LineItem) -> Result<T, ApiError> {
        let bytes = item.bytes();
        Ok(T::unmarshal(&bytes[..T::PAYLOAD_BYTES])?)
    }

    /// Receive one item. Drains the KHL side-backing first (batched
    /// sends land there), then BOTH ring/deque backings: the inactive
    /// (stale) backing first, then the active one.
    #[inline]
    pub fn recv(&self) -> Result<T, ApiError> {
        if let Some(v) = self.recv_from_khl()? {
            self.signal_producer();
            return Ok(v);
        }
        let active = self.control.load(Ordering::Acquire);
        let stale = if active == TAG_RING { TAG_DEQUE } else { TAG_RING };
        if let Some(v) = self.try_recv_from(stale)? {
            self.signal_producer();
            return Ok(v);
        }
        match self.try_recv_from(active)? {
            Some(v) => {
                self.signal_producer();
                Ok(v)
            }
            None => Err(ApiError::Transport(TransportError::Empty)),
        }
    }

    #[inline]
    fn try_recv_from(&self, tag: u32) -> Result<Option<T>, ApiError> {
        // 64-byte buffer: AdaptiveRing's SPSC/MPSC/MPMC backings
        // expose a 64-byte payload slot (Lamport slot is
        // payload-only); the Vyukov shape uses 56 bytes. Sizing the
        // buffer at the larger of the two covers every shape the
        // AdaptiveRing can morph through. PassSlot for the deque
        // path is 56 bytes per `PAYLOAD_BYTES`.
        let mut out = [0u8; crate::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES];
        match tag {
            TAG_RING => match self.ring.try_recv(0, &mut out) {
                Ok(n) => Ok(Some(T::unmarshal(&out[..n.min(out.len())])?)),
                Err(_) => Ok(None),
            },
            TAG_DEQUE => match self.deque.steal() {
                Some(slot) => Ok(Some(T::unmarshal(
                    &slot.0[..PAYLOAD_BYTES.min(T::PAYLOAD_BYTES.max(1))],
                )?)),
                None => Ok(None),
            },
            _ => Err(ApiError::Transport(TransportError::Other)),
        }
    }

    /// Wake whoever waits to RECEIVE: the awaiting task's `Waker` and
    /// any thread parked in `recv_blocking`. The `published` counter is
    /// the park key, advanced on every send regardless of backing.
    fn signal_consumer(&self) {
        if !self.has_recv_waiter.load(Ordering::Relaxed) {
            return;
        }
        let n = self.published.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(w) = self.recv_slot.lock().take() {
            w.wake();
        }
        self.consumer_waker.wake_up_to(n);
    }

    /// Wake whoever waits to SEND.
    fn signal_producer(&self) {
        if !self.has_send_waiter.load(Ordering::Relaxed) {
            return;
        }
        let n = self.consumed.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(w) = self.send_slot.lock().take() {
            w.wake();
        }
        self.producer_waker.wake_up_to(n);
    }

    /// Blocking send: parks the calling thread until the active backing
    /// accepts the item (or `timeout` elapses). `None` waits forever.
    pub fn send_blocking(
        &self,
        item: &T,
        timeout: Option<Duration>,
    ) -> Result<(), ApiError> {
        self.has_send_waiter.store(true, Ordering::Relaxed);
        let deadline = timeout.map(|d| std::time::Instant::now() + d);
        loop {
            match self.send(item) {
                Ok(()) => return Ok(()),
                Err(ApiError::Transport(TransportError::Full)) => {}
                Err(e) => return Err(e),
            }
            let seen = self.consumed.load(Ordering::Acquire);
            let token = self.producer_waker.try_park(seen + 1).map_err(map_waker)?;
            match self.send(item) {
                Ok(()) => {
                    self.producer_waker.release(token);
                    return Ok(());
                }
                Err(ApiError::Transport(TransportError::Full)) => {}
                Err(e) => {
                    self.producer_waker.release(token);
                    return Err(e);
                }
            }
            wait_heal(&self.producer_waker, token, deadline)?;
        }
    }

    /// Blocking recv: parks the calling thread until an item arrives (or
    /// `timeout` elapses). `None` waits forever.
    pub fn recv_blocking(&self, timeout: Option<Duration>) -> Result<T, ApiError> {
        self.has_recv_waiter.store(true, Ordering::Relaxed);
        let deadline = timeout.map(|d| std::time::Instant::now() + d);
        loop {
            match self.recv() {
                Ok(v) => return Ok(v),
                Err(ApiError::Transport(TransportError::Empty)) => {}
                Err(e) => return Err(e),
            }
            let seen = self.published.load(Ordering::Acquire);
            let token = self.consumer_waker.try_park(seen + 1).map_err(map_waker)?;
            match self.recv() {
                Ok(v) => {
                    self.consumer_waker.release(token);
                    return Ok(v);
                }
                Err(ApiError::Transport(TransportError::Empty)) => {}
                Err(e) => {
                    self.consumer_waker.release(token);
                    return Err(e);
                }
            }
            wait_heal(&self.consumer_waker, token, deadline)?;
        }
    }

    /// Async send. Resolves once the item is accepted, suspending the
    /// task while the active backing is full.
    pub fn send_async(&self, item: &T) -> AdaptiveSendFut<'_, T> {
        self.has_send_waiter.store(true, Ordering::Relaxed);
        AdaptiveSendFut { ipc: self, item: *item }
    }

    /// Async recv. Resolves to the next item, suspending while empty.
    pub fn recv_async(&self) -> AdaptiveRecvFut<'_, T> {
        self.has_recv_waiter.store(true, Ordering::Relaxed);
        AdaptiveRecvFut { ipc: self }
    }

    /// Read the profile counters (snapshot).
    pub fn profile_snapshot(&self) -> ProfileSnapshot {
        ProfileSnapshot {
            total_sends: self.total_sends_atom.load(Ordering::Relaxed),
            batch_sends: self.batch_sends_atom.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum_atom.load(Ordering::Relaxed),
            max_batch_size: self.max_batch_size_atom.load(Ordering::Relaxed),
        }
    }

    /// Currently active family.
    pub fn active_family(&self) -> MmfFamily {
        match self.control.load(Ordering::Acquire) {
            TAG_RING => MmfFamily::SharedRing,
            TAG_DEQUE => MmfFamily::SharedDeque(
                crate::dispatch_deque::DequeVariant::Khl,
            ),
            _ => MmfFamily::SharedRing,
        }
    }

    /// Explicitly migrate to `target_family`. Both backings are
    /// pre-allocated; migration is a single Release-store on the
    /// MMF-resident control atom. ZERO kernel touch.
    ///
    /// The pin_generation is bumped BEFORE the family-tag store so
    /// pinned-handle holders see invalidation on their next
    /// `is_still_valid()` check at or after the migration boundary.
    pub fn migrate_to(&self, target_family: MmfFamily) -> Result<(), ApiError> {
        let new_tag = match target_family {
            MmfFamily::SharedRing => TAG_RING,
            MmfFamily::SharedDeque(_) => TAG_DEQUE,
            MmfFamily::SharedHashMap => {
                return Err(ApiError::WrongFamily {
                    wanted: "SharedRing or SharedDeque",
                    got: target_family,
                });
            }
        };
        let current_tag = self.control.load(Ordering::Acquire);
        if current_tag == new_tag {
            return Ok(());
        }
        self.pin_generation.fetch_add(1, Ordering::AcqRel);
        self.control.store(new_tag, Ordering::Release);
        Ok(())
    }

    /// Current pin generation. Pinned handles capture this at pin
    /// time; a non-equal current value means the pin is stale and
    /// the holder should release + re-acquire via
    /// [`pin_current_family`](Self::pin_current_family).
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Direct access to the composed `AdaptiveRing` backing.
    ///
    /// The override hatch for callers who want shape-axis control
    /// without going through the pin protocol: register additional
    /// producers / consumers, call `morph_to(RingShape::Vyukov)` to
    /// lock global-FIFO behavior, attach a separate
    /// `AdaptiveRingSidecar`, etc. The IPC-level family migration
    /// continues to work independently on top.
    pub fn ring_handle(&self) -> &AdaptiveRing {
        &self.ring
    }

    /// Pin the current family and return a [`PinnedIpc`] handle
    /// exposing typed access to the active backing.
    ///
    /// Hot-path use: call once, then drive ops through `as_ring()`
    /// or `as_deque()` for as long as `is_still_valid()` returns
    /// `true`. On `false`, release this pin and call
    /// `pin_current_family()` again to capture the new family.
    pub fn pin_current_family(&self) -> PinnedIpc<'_, T> {
        let captured_gen = self.pin_generation.load(Ordering::Acquire);
        let tag = self.control.load(Ordering::Acquire);
        let family = match tag {
            TAG_RING => MmfFamily::SharedRing,
            TAG_DEQUE => MmfFamily::SharedDeque(
                crate::dispatch_deque::DequeVariant::Khl,
            ),
            _ => MmfFamily::SharedRing,
        };
        PinnedIpc {
            parent: self,
            pinned_generation: captured_gen,
            family,
            _not_sync: PhantomData,
        }
    }

    /// Inspect the current profile and migrate to the dispatcher's
    /// preferred family if it differs from the active family.
    ///
    /// The decision uses TWO signals in production:
    /// 1. Profile counters (`total_sends`, `batch_sends`,
    ///    `batch_size_sum`, `max_batch_size`) for quantitative
    ///    history.
    /// 2. The `Bloom64` shape filter for O(1) qualitative pattern
    ///    detection (verified 2.92x faster than `HashSet` for this
    ///    use case, see `benches/bloom_filter_ab.rs`).
    ///
    /// The Bloom check rejects calls where no batched shape has
    /// ever been observed (early exit without re-deriving from
    /// counters); when the Bloom says "might-have-been-batched",
    /// the counter-based inference runs.
    pub fn maybe_promote(&self) -> Result<Option<MmfFamily>, ApiError> {
        self.maybe_auto_order();
        let snap = self.profile_snapshot();
        let total_events = snap.total_sends + snap.batch_sends;
        if total_events < 8 {
            return Ok(None);
        }
        // Bloom fast-reject: if we haven't seen any batched shape
        // recently, skip the migration analysis entirely.
        let bloom = subetha_pointers::bloom_pointer::Bloom64(
            self.shape_bloom_atom.load(Ordering::Relaxed),
        );
        // log2_bucket can be 1..=10 typically for our workloads.
        let any_batched = (1..=10).any(|b| {
            bloom.might_contain(&(1u32, b))
        });
        if !any_batched && self.active_family() == MmfFamily::SharedRing {
            // No batched shapes observed AND we are already on the
            // streaming family - nothing to migrate to.
            return Ok(None);
        }
        let target_shape = snap.inferred_shape(self.n_consumers);
        let target_family = MmfDispatcher::pick(target_shape);
        let active = self.active_family();
        if target_family != active {
            self.migrate_to(target_family)?;
            return Ok(Some(target_family));
        }
        Ok(None)
    }

    /// The pre-authorized automatic ordering response: when an
    /// `auto_order` threshold was configured at construction and
    /// the stamped ring is still `Unordered`, compute the inversion
    /// rate since the previous poll and arm `MergeByStamp` once it
    /// crosses the threshold. One-way by design - merged pops read
    /// zero inversions, so there is no symmetric disarm signal; the
    /// caller disarms via [`set_ordering`](Self::set_ordering).
    fn maybe_auto_order(&self) {
        let Some(threshold) = self.auto_order_threshold else { return };
        if self.ring.ordering_mode() != Some(OrderingMode::Unordered) {
            return;
        }
        let now = monotonic_nanos();
        let then = self.last_inversion_check_nanos.swap(now, Ordering::AcqRel);
        let inversions = self.ring.inversions();
        let last = self.last_inversions_atom.swap(inversions, Ordering::AcqRel);
        let elapsed_secs = (now.saturating_sub(then) as f64 / 1e9).max(1e-9);
        let rate = inversions.saturating_sub(last) as f64 / elapsed_secs;
        if rate > threshold {
            self.ring.set_ordering_mode(OrderingMode::MergeByStamp).ok();
        }
    }
}

impl<T: Marshal + Copy + 'static> Drop for AdaptiveIpc<T> {
    fn drop(&mut self) {
        let deque_p = deque_path_for(&self.base_path);
        let ctl_p = control_path_for(&self.base_path);
        let pingen_p = pingen_path_for(&self.base_path);
        std::fs::remove_file(&deque_p).ok();
        std::fs::remove_file(&ctl_p).ok();
        std::fs::remove_file(&pingen_p).ok();
        if self.khl.is_some() {
            std::fs::remove_file(khl_path_for(&self.base_path)).ok();
        }

        // AdaptiveRing's file-backed constructor lays its files out
        // as: `{prefix}.spsc.bin`, `{prefix}.mpsc.{i}.bin`,
        // `{prefix}.mpmc.{i}.bin`, `{prefix}.vyukov.bin`. Mirror that
        // here so cleanup is exhaustive.
        let ring_prefix = ring_path_prefix_for(&self.base_path);
        let max_producers = self.ring.max_producers();
        std::fs::remove_file(with_suffix(&ring_prefix, ".spsc.bin")).ok();
        std::fs::remove_file(with_suffix(&ring_prefix, ".vyukov.bin")).ok();
        std::fs::remove_file(with_suffix(&ring_prefix, ".ordering.bin")).ok();
        for i in 0..max_producers {
            std::fs::remove_file(
                with_suffix(&ring_prefix, &format!(".mpsc.{i}.bin")),
            ).ok();
            std::fs::remove_file(
                with_suffix(&ring_prefix, &format!(".mpmc.{i}.bin")),
            ).ok();
        }
    }
}

fn with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

fn map_ring_err(e: crate::shared_ring::RingError) -> ApiError {
    match e {
        crate::shared_ring::RingError::Full => {
            ApiError::Transport(TransportError::Full)
        }
        crate::shared_ring::RingError::Empty => {
            ApiError::Transport(TransportError::Empty)
        }
        crate::shared_ring::RingError::PayloadTooLarge => {
            ApiError::Transport(TransportError::PayloadTooLarge)
        }
        _ => ApiError::Transport(TransportError::Other),
    }
}

/// Future from [`AdaptiveIpc::recv_async`].
pub struct AdaptiveRecvFut<'a, T: Marshal + Copy + 'static> {
    ipc: &'a AdaptiveIpc<T>,
}

impl<'a, T: Marshal + Copy + 'static> Future for AdaptiveRecvFut<'a, T> {
    type Output = Result<T, ApiError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ipc = self.ipc;
        match ipc.recv() {
            Ok(v) => return Poll::Ready(Ok(v)),
            Err(ApiError::Transport(TransportError::Empty)) => {}
            Err(e) => return Poll::Ready(Err(e)),
        }
        *ipc.recv_slot.lock() = Some(cx.waker().clone());
        match ipc.recv() {
            Ok(v) => Poll::Ready(Ok(v)),
            Err(ApiError::Transport(TransportError::Empty)) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

/// Future from [`AdaptiveIpc::send_async`].
pub struct AdaptiveSendFut<'a, T: Marshal + Copy + 'static> {
    ipc: &'a AdaptiveIpc<T>,
    item: T,
}

impl<'a, T: Marshal + Copy + 'static> Future for AdaptiveSendFut<'a, T> {
    type Output = Result<(), ApiError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Read-only access through the pin's Deref; the future never
        // moves its fields, so no `Unpin` bound on `T` is needed.
        match self.ipc.send(&self.item) {
            Ok(()) => return Poll::Ready(Ok(())),
            Err(ApiError::Transport(TransportError::Full)) => {}
            Err(e) => return Poll::Ready(Err(e)),
        }
        *self.ipc.send_slot.lock() = Some(cx.waker().clone());
        match self.ipc.send(&self.item) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(ApiError::Transport(TransportError::Full)) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

fn control_path_for(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_stem().map(|s| s.to_owned()).unwrap_or_default();
    p.set_file_name(format!("{}.ctl.bin", stem.to_string_lossy()));
    p
}

fn ring_path_prefix_for(base: &Path) -> PathBuf {
    // Returns the path PREFIX (no `.bin` suffix) that AdaptiveRing's
    // file-backed constructor appends its per-shape suffixes to.
    // The resulting files are
    // `{stem}.ring.spsc.bin` / `{stem}.ring.mpsc.{i}.bin` / etc.
    let mut p = base.to_path_buf();
    let stem = p.file_stem().map(|s| s.to_owned()).unwrap_or_default();
    p.set_file_name(format!("{}.ring", stem.to_string_lossy()));
    p
}

fn khl_path_for(base: &Path) -> PathBuf {
    let stem = base.file_stem().map(|s| s.to_owned()).unwrap_or_default();
    base.with_file_name(format!("{}.khl.bin", stem.to_string_lossy()))
}

fn deque_path_for(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_stem().map(|s| s.to_owned()).unwrap_or_default();
    p.set_file_name(format!("{}.deque.bin", stem.to_string_lossy()));
    p
}

fn pingen_path_for(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_stem().map(|s| s.to_owned()).unwrap_or_default();
    p.set_file_name(format!("{}.pingen.bin", stem.to_string_lossy()));
    p
}

// ===================================================================
// PinnedIpc: typed handle pinned to one family of the parent
// AdaptiveIpc. Hot path bypasses the runtime dispatch on send() and
// exposes the full SharedRing / SharedDeque API surface directly.
// ===================================================================

/// Handle pinned to one family of the parent [`AdaptiveIpc`].
///
/// Captures the active family + pin_generation at pin time. Holders
/// drive ops through [`as_ring`](Self::as_ring) or
/// [`as_deque`](Self::as_deque) for as long as
/// [`is_still_valid`](Self::is_still_valid) returns `true`. On
/// `false` (a migration has happened) the holder releases this pin
/// and calls [`AdaptiveIpc::pin_current_family`] to capture the new
/// family.
///
/// `!Send + !Sync` (via [`PhantomData<Cell<()>>`]): the pin captures
/// the active family at pin time and cannot safely cross a thread
/// boundary because the parent may migrate concurrently. Each thread
/// that wants pinned access acquires its own pin.
pub struct PinnedIpc<'a, T: Marshal + Copy + 'static> {
    parent: &'a AdaptiveIpc<T>,
    pinned_generation: u64,
    family: MmfFamily,
    _not_sync: PhantomData<Cell<()>>,
}

impl<'a, T: Marshal + Copy + 'static> PinnedIpc<'a, T> {
    /// Family this pin was captured at.
    pub fn family(&self) -> MmfFamily { self.family }

    /// Pin generation captured at pin time.
    pub fn pinned_generation(&self) -> u64 { self.pinned_generation }

    /// One Acquire load on the parent's pin_generation. Returns
    /// `true` while the pin is current; `false` if a migration has
    /// happened and the caller should release + re-acquire.
    pub fn is_still_valid(&self) -> bool {
        self.parent.pin_generation.load(Ordering::Acquire)
            == self.pinned_generation
    }

    /// Typed handle to the active `AdaptiveRing` backing.
    ///
    /// Returns `Some(&AdaptiveRing)` when the pinned family is
    /// `SharedRing`, `None` otherwise. The composition pattern:
    /// chain into `pin_current_shape()` on the returned handle to
    /// drop one more axis level and reach the shape-pinned native
    /// primitive (`PinnedRing<'_>`), then call e.g.
    /// `.spsc_try_push()` for the SPSC fast path. Two Acquire loads
    /// total per validity check (one per axis), each at the
    /// caller's chosen cadence.
    pub fn as_ring(&self) -> Option<&AdaptiveRing> {
        match self.family {
            MmfFamily::SharedRing => Some(&self.parent.ring),
            _ => None,
        }
    }

    /// Typed handle to the active `SharedDeque<PassSlot>` backing.
    pub fn as_deque(&self) -> Option<&SharedDeque<PassSlot>> {
        match self.family {
            MmfFamily::SharedDeque(_) => Some(&self.parent.deque),
            _ => None,
        }
    }
}

// ===================================================================
// AdaptiveIpcSidecar: background thread that drives maybe_promote()
// on a timer. Mirrors AdaptiveRingSidecar.
// ===================================================================

/// Background scanner thread that drives family promotions on an
/// [`AdaptiveIpc`] by polling [`AdaptiveIpc::maybe_promote`] on a
/// timer.
///
/// `spawn` starts the thread; `shutdown` stops it cleanly. The
/// thread polls every `scan_interval`, calls `maybe_promote()`, and
/// counts a promotion when the call returns `Ok(Some(_))`.
pub struct AdaptiveIpcSidecar {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    promotions_triggered: Arc<AtomicU64>,
}

impl AdaptiveIpcSidecar {
    /// Spawn a sidecar thread that polls `ipc.maybe_promote()` every
    /// `scan_interval`. Each successful promotion is counted in
    /// `promotions_triggered`.
    pub fn spawn<T: Marshal + Copy + Send + Sync + 'static>(
        ipc: Arc<AdaptiveIpc<T>>,
        scan_interval: std::time::Duration,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let promotions_triggered = Arc::new(AtomicU64::new(0));

        let stop_c = stop.clone();
        let promotions_c = promotions_triggered.clone();
        let handle = std::thread::spawn(move || {
            while !stop_c.load(Ordering::Acquire) {
                if let Ok(Some(_)) = ipc.maybe_promote() {
                    promotions_c.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(scan_interval);
            }
        });

        Self {
            handle: Some(handle),
            stop,
            promotions_triggered,
        }
    }

    /// Number of successful promotions the sidecar has issued since
    /// spawn.
    pub fn promotions_triggered(&self) -> u64 {
        self.promotions_triggered.load(Ordering::Acquire)
    }

    /// Stop the scanner thread and join it.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.join().expect("sidecar thread panicked");
        }
    }
}

impl Drop for AdaptiveIpcSidecar {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.join().ok();
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch_deque::DequeVariant;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subetha_adaptive_{pid}_{nonce}_{name}"));
        p
    }

    #[test]
    fn create_and_send_round_trip_in_initial_family() {
        let path = tmp("init");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        assert_eq!(ipc.active_family(), MmfFamily::SharedRing);
        ipc.send(&111).expect("send");
        ipc.send(&222).expect("send");
        let a = ipc.recv().expect("recv");
        let b = ipc.recv().expect("recv");
        assert_eq!(a, 111);
        assert_eq!(b, 222);
    }

    #[test]
    fn migrate_to_changes_active_family_kernel_bypass() {
        let path = tmp("migrate");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        assert_eq!(ipc.active_family(), MmfFamily::SharedRing);
        ipc.migrate_to(MmfFamily::SharedDeque(DequeVariant::Khl))
            .expect("migrate");
        assert_eq!(
            ipc.active_family(),
            MmfFamily::SharedDeque(DequeVariant::Khl)
        );
        ipc.send(&333).expect("send post-migrate");
        let v = ipc.recv().expect("recv post-migrate");
        assert_eq!(v, 333);
    }

    #[test]
    fn maybe_promote_observes_batches_and_migrates_to_work_stealing() {
        let path = tmp("auto_promote");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 256, 1)
            .expect("create");
        assert_eq!(ipc.active_family(), MmfFamily::SharedRing);
        for _ in 0..10 {
            let batch: Vec<u64> = (0..16).collect();
            ipc.send_batch(&batch).expect("batch");
        }
        let snap = ipc.profile_snapshot();
        assert!(snap.batch_ratio() > 0.5);
        let promoted = ipc.maybe_promote().expect("promote");
        assert!(promoted.is_some());
        assert!(matches!(
            ipc.active_family(),
            MmfFamily::SharedDeque(_)
        ));
    }

    #[test]
    fn drain_after_migration_reads_from_both_backings() {
        let path = tmp("drain");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        for i in 0..3u64 {
            ipc.send(&i).expect("send pre");
        }
        ipc.migrate_to(MmfFamily::SharedDeque(DequeVariant::Khl))
            .expect("migrate");
        for i in 100..103u64 {
            ipc.send(&i).expect("send post");
        }
        let mut seen = Vec::new();
        for _ in 0..6 {
            let v = ipc.recv().expect("recv");
            seen.push(v);
        }
        assert_eq!(seen.iter().sum::<u64>(), 306);
    }

    #[test]
    fn profile_snapshot_tracks_single_and_batch_sends() {
        let path = tmp("profile");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 256, 1)
            .expect("create");
        ipc.send(&1).expect("send");
        ipc.send(&2).expect("send");
        let batch: Vec<u64> = (0..8).collect();
        ipc.send_batch(&batch).expect("batch");
        let snap = ipc.profile_snapshot();
        assert_eq!(snap.total_sends, 2);
        assert_eq!(snap.batch_sends, 1);
        assert_eq!(snap.batch_size_sum, 8);
        assert_eq!(snap.max_batch_size, 8);
    }

    // A >16-byte Marshal type: forces `khl = None` (payload exceeds
    // KHL's 16-byte LineItem), exercising the payload gate.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Big32([u8; 32]);
    unsafe impl Marshal for Big32 {
        const PAYLOAD_BYTES: usize = 32;
        fn marshal(&self, dst: &mut [u8]) {
            dst[..32].copy_from_slice(&self.0);
        }
        fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
            if src.len() < 32 {
                return Err(subetha_core::MarshalError::ShortBuffer {
                    expected: 32,
                    got: src.len(),
                });
            }
            let mut b = [0u8; 32];
            b.copy_from_slice(&src[..32]);
            Ok(Big32(b))
        }
    }

    fn drain_all_u64(ipc: &AdaptiveIpc<u64>, n: usize) -> Vec<u64> {
        let mut got = Vec::with_capacity(n);
        let mut spins = 0u64;
        while got.len() < n {
            match ipc.recv() {
                Ok(v) => got.push(v),
                Err(_) => {
                    spins += 1;
                    assert!(spins < 200_000_000, "recv stalled before draining all items");
                    std::hint::spin_loop();
                }
            }
        }
        got
    }

    #[test]
    fn khl_batch_send_round_trips_through_side_backing() {
        let path = tmp("khl_rt");
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 256, 1).expect("create");
        assert!(ipc.khl.is_some(), "u64 (8 bytes) fits KHL's 16-byte slot");
        let batch: Vec<u64> = (0..6).collect();
        ipc.send_batch(&batch).expect("batch");
        let mut got = drain_all_u64(&ipc, 6);
        got.sort_unstable();
        assert_eq!(got, batch, "every batched item received exactly once");
    }

    #[test]
    fn khl_surplus_buffers_partial_slots() {
        // 7 items pack into KHL slots of 3 + 3 + 1, so recv drains the
        // 0..=2 surplus from the shared buffer across calls.
        let path = tmp("khl_surplus");
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1).expect("create");
        let batch: Vec<u64> = (10..17).collect();
        ipc.send_batch(&batch).expect("batch");
        let mut got = drain_all_u64(&ipc, 7);
        got.sort_unstable();
        assert_eq!(got, batch);
    }

    #[test]
    fn khl_mixed_single_and_batch_all_received() {
        // Singles route to ring/deque; the batch routes to khl. recv
        // drains all sources.
        let path = tmp("khl_mixed");
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 256, 1).expect("create");
        ipc.send(&1).expect("single");
        ipc.send(&2).expect("single");
        let batch: Vec<u64> = (100..108).collect();
        ipc.send_batch(&batch).expect("batch");
        let mut expected: Vec<u64> = vec![1, 2];
        expected.extend(&batch);
        expected.sort_unstable();
        let mut got = drain_all_u64(&ipc, expected.len());
        got.sort_unstable();
        assert_eq!(got, expected);
    }

    #[test]
    fn khl_large_batch_integrity() {
        let path = tmp("khl_large");
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 512, 1).expect("create");
        let batch: Vec<u64> = (0..300).collect();
        ipc.send_batch(&batch).expect("batch");
        let mut got = drain_all_u64(&ipc, 300);
        got.sort_unstable();
        assert_eq!(got, batch);
    }

    #[test]
    fn khl_payload_gate_large_type_uses_no_khl() {
        let path = tmp("khl_gate");
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 };
        let ipc: AdaptiveIpc<Big32> = AdaptiveIpc::create(&path, shape, 64, 1).expect("create");
        assert!(ipc.khl.is_none(), "32-byte payload exceeds KHL's 16-byte slot");
        let batch: Vec<Big32> = (0..5u8).map(|i| Big32([i; 32])).collect();
        ipc.send_batch(&batch).expect("batch via existing per-item path");
        let mut got = Vec::new();
        let mut spins = 0u64;
        while got.len() < 5 {
            match ipc.recv() {
                Ok(v) => got.push(v),
                Err(_) => {
                    spins += 1;
                    assert!(spins < 200_000_000, "recv stalled");
                    std::hint::spin_loop();
                }
            }
        }
        got.sort_by_key(|b| b.0[0]);
        assert_eq!(got, batch, ">16-byte batch round-trips via the per-item path");
    }

    #[test]
    fn khl_multi_consumer_no_loss_or_dup() {
        use std::sync::atomic::{AtomicU64, Ordering as AOrd};
        let path = tmp("khl_multi");
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 4 };
        let ipc = Arc::new(AdaptiveIpc::<u64>::create(&path, shape, 512, 4).expect("create"));
        const N: u64 = 600;
        let batch: Vec<u64> = (0..N).collect();
        ipc.send_batch(&batch).expect("batch");
        let received = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let ipc = Arc::clone(&ipc);
            let received = Arc::clone(&received);
            let checksum = Arc::clone(&checksum);
            handles.push(std::thread::spawn(move || loop {
                if received.load(AOrd::Acquire) >= N {
                    break;
                }
                match ipc.recv() {
                    Ok(v) => {
                        checksum.fetch_add(v, AOrd::AcqRel);
                        received.fetch_add(1, AOrd::AcqRel);
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(AOrd::Acquire), N, "exactly N items received");
        assert_eq!(
            checksum.load(AOrd::Acquire),
            (0..N).sum::<u64>(),
            "every item exactly once, none lost or duplicated"
        );
    }

    #[test]
    fn rejects_kv_map_family_at_construction() {
        let path = tmp("reject_kv");
        let shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: 1,
            n_writers: 1,
        };
        let result = AdaptiveIpc::<u64>::create(&path, shape, 64, 1);
        match result {
            Err(ApiError::WrongFamily { .. }) => {}
            Err(other) => panic!("expected WrongFamily, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn pin_captures_family_and_generation() {
        let path = tmp("pin_capture");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        let gen_before = ipc.pin_generation();
        let pin = ipc.pin_current_family();
        assert_eq!(pin.family(), MmfFamily::SharedRing);
        assert_eq!(pin.pinned_generation(), gen_before);
        assert!(pin.is_still_valid());
    }

    #[test]
    fn migration_invalidates_outstanding_pin() {
        let path = tmp("pin_invalidate");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        let pin = ipc.pin_current_family();
        assert!(pin.is_still_valid());
        ipc.migrate_to(MmfFamily::SharedDeque(DequeVariant::Khl))
            .expect("migrate");
        assert!(!pin.is_still_valid(),
                "pin must invalidate on migration");
        let pin2 = ipc.pin_current_family();
        assert_eq!(pin2.family(), MmfFamily::SharedDeque(DequeVariant::Khl));
        assert!(pin2.is_still_valid());
    }

    #[test]
    fn migrate_to_same_family_does_not_bump_generation() {
        let path = tmp("pin_noop");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        let pin = ipc.pin_current_family();
        let gen_before = pin.pinned_generation();
        ipc.migrate_to(MmfFamily::SharedRing).expect("noop migrate");
        assert_eq!(ipc.pin_generation(), gen_before,
                   "no-op migrate must not bump generation");
        assert!(pin.is_still_valid(),
                "no-op migrate must not invalidate pin");
    }

    #[test]
    fn pinned_as_ring_round_trip() {
        let path = tmp("pin_ring_rt");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        let pin = ipc.pin_current_family();
        let ring = pin.as_ring().expect("pinned at ring family");
        assert!(pin.as_deque().is_none(),
                "as_deque must return None when pinned at ring family");

        // Exercise the composition pattern: chain protocol-axis pin
        // (PinnedIpc::as_ring) into the shape-axis pin
        // (AdaptiveRing::pin_current_shape) and reach the native
        // SPSC primitive through the shape-pinned handle. The
        // initial shape of a 1P/1C registration is SPSC.
        let shape_pin = ring.pin_current_shape();
        assert_eq!(shape_pin.shape(), crate::RingShape::Spsc);
        assert!(shape_pin.is_still_valid());

        let payload = 0xDEADBEEFu64.to_le_bytes();
        shape_pin.spsc_try_push(&payload).expect("native SPSC push");
        let mut buf = [0u8; crate::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES];
        let n = shape_pin.spsc_try_pop(&mut buf).expect("native SPSC pop");
        assert!(n >= 8);
        assert_eq!(&buf[..8], &payload);
    }

    #[test]
    fn pinned_as_deque_round_trip() {
        let path = tmp("pin_deque_rt");
        let shape = MmfWorkloadShape::WorkStealing(
            crate::dispatch_deque::WorkloadShape {
                n_thieves: 1,
                batch_size: Some(4),
                wait_idle: false,
            },
        );
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        let pin = ipc.pin_current_family();
        let deque = pin.as_deque().expect("pinned at deque family");
        assert!(pin.as_ring().is_none(),
                "as_ring must return None when pinned at deque family");

        let mut slot = PassSlot([0u8; PAYLOAD_BYTES]);
        slot.0[..8].copy_from_slice(&7777u64.to_le_bytes());
        deque.push(&slot).expect("native push");
        let popped = deque.steal().expect("native steal");
        let val = u64::from_le_bytes(popped.0[..8].try_into().unwrap());
        assert_eq!(val, 7777);
    }

    #[test]
    fn create_with_ordering_applies_declaration_and_round_trips() {
        let path = tmp("ordering_create");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create_with_ordering(
            &path, shape, 64, 1, QosOrdering::GlobalFifo, None,
        ).expect("create");
        assert!(ipc.ring_handle().is_stamped());
        assert_eq!(ipc.ordering(), QosOrdering::GlobalFifo);
        assert_eq!(ipc.ring_handle().ordering_mode(),
                   Some(OrderingMode::MergeByStamp));

        // The stamp layer is transparent at the typed API surface.
        ipc.send(&777).expect("send");
        ipc.send(&888).expect("send");
        assert_eq!(ipc.recv().expect("recv"), 777);
        assert_eq!(ipc.recv().expect("recv"), 888);

        // Runtime withdrawal flips the merge flag off.
        ipc.set_ordering(QosOrdering::PerProducer).expect("withdraw");
        assert_eq!(ipc.ordering(), QosOrdering::PerProducer);
        assert_eq!(ipc.ring_handle().ordering_mode(),
                   Some(OrderingMode::Unordered));
    }

    #[test]
    fn set_ordering_on_unstamped_ring_routes_through_vyukov_morph() {
        let path = tmp("ordering_unstamped");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create(&path, shape, 64, 1)
            .expect("create");
        assert!(!ipc.ring_handle().is_stamped());
        assert_eq!(ipc.ordering(), QosOrdering::PerProducer);

        ipc.set_ordering(QosOrdering::GlobalFifo).expect("declare");
        assert_eq!(ipc.ring_handle().current_shape(), RingShape::Vyukov,
                   "unstamped GlobalFifo declaration must morph to Vyukov");
        assert_eq!(ipc.ordering(), QosOrdering::GlobalFifo);
        ipc.send(&5).expect("send through Vyukov");
        assert_eq!(ipc.recv().expect("recv"), 5);

        ipc.set_ordering(QosOrdering::PerProducer).expect("withdraw");
        assert_ne!(ipc.ring_handle().current_shape(), RingShape::Vyukov,
                   "withdrawal must walk the Vyukov morph back");
    }

    #[test]
    fn auto_order_arms_merge_on_observed_inversion_rate() {
        let path = tmp("auto_order");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 2,
        };
        let ipc: AdaptiveIpc<u64> = AdaptiveIpc::create_with_ordering(
            &path, shape, 64, 2, QosOrdering::PerProducer, Some(1.0),
        ).expect("create");
        let ring = ipc.ring_handle();
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::Unordered));

        // Manufacture a cross-producer inversion: a second producer
        // pushes first (older stamp in ring 1), then producer 0, and
        // the round-robin drain pops newer-then-older.
        ring.morph_to(crate::RingShape::Mpsc).expect("morph");
        ring.register_producer().expect("p1");
        ring.try_send(1, &1u64.to_le_bytes()).expect("send p1");
        ring.try_send(0, &2u64.to_le_bytes()).expect("send p0");
        let mut out = [0u8; crate::ordering::STAMPED_PAYLOAD_BYTES];
        ring.try_recv(0, &mut out).expect("pop 1");
        ring.try_recv(0, &mut out).expect("pop 2");
        assert!(ring.inversions() >= 1, "interleave must register an inversion");

        // The pre-authorized response: maybe_promote's auto-order
        // check sees the rate spike and arms the merge.
        ipc.maybe_promote().expect("promote poll");
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "auto_order threshold crossing must arm MergeByStamp");
        assert_eq!(ipc.ordering(), QosOrdering::GlobalFifo);
    }

    #[test]
    fn sidecar_auto_promotes_on_observed_batches() {
        let path = tmp("sidecar_promote");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let ipc = Arc::new(
            AdaptiveIpc::<u64>::create(&path, shape, 256, 1)
                .expect("create"),
        );
        assert_eq!(ipc.active_family(), MmfFamily::SharedRing);
        let sidecar = AdaptiveIpcSidecar::spawn(
            ipc.clone(),
            std::time::Duration::from_millis(5),
        );

        // Drive a batched workload pattern the policy promotes on.
        for _ in 0..10 {
            let batch: Vec<u64> = (0..16).collect();
            ipc.send_batch(&batch).expect("batch");
        }
        // Drain a few items so the deque path stays usable.
        for _ in 0..16 { ipc.recv().ok(); }

        // Give the sidecar a window to scan + promote.
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline
            && !matches!(ipc.active_family(), MmfFamily::SharedDeque(_))
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert!(matches!(ipc.active_family(), MmfFamily::SharedDeque(_)),
                "sidecar should have promoted to SharedDeque");
        assert!(sidecar.promotions_triggered() >= 1,
                "sidecar should have recorded at least one promotion");
        sidecar.shutdown();
    }
}
