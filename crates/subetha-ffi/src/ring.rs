//! The adaptive ring through the C ABI: one handle per ring, byte payloads,
//! producer and consumer ids from registration, a try and a waiting form of
//! push and pop, and the file-locale unlink.
//!
//! Waiting parks on a cross-process waker beside the ring: a push wakes one
//! parked consumer, a pop wakes one parked producer. In the file and
//! shared-memory locales the wakers live beside the backings
//! (`<prefix>.cwaker.bin` / `<prefix>.pwaker.bin`, `{name}_cwaker` /
//! `{name}_pwaker`), so a process that attaches wakes and is woken across
//! the boundary.
//!
//! Mode: strict attaches nothing and starts no thread; the ring still
//! morphs on registration and on the pop path, which is the ring's own
//! behavior and needs no pump. Managed attaches a shape sidecar that scans
//! at the interval the caller sets and morphs from the default shape policy.
//! There is nothing for a strict-mode caller to pump on a ring, so nothing
//! to under-pump and nothing to count.

use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::{
    AdaptiveRing, AdaptiveRingSidecar, DefaultRingShapePolicy, RingShape, UnlinkReport,
};
use subetha_cxc::ring_holders::LastHolder;
use subetha_cxc::cross_process_waker::{
    waker_region_size, CrossProcessWaker, WakerError, MAX_WAITERS_DEFAULT,
};
use subetha_cxc::frame_ring::{FrameClass, LayoutHint};
use subetha_cxc::ordering::{OrderingMode, StampKind};
use subetha_cxc::ring_contract::{OrderingContract, RingContract};
use subetha_cxc::shm_file::{ShmFile, ShmNamespace};

use crate::batch::{run_pop_many, run_push_many};

use crate::error::{
    adaptive_code, fail, last_holder_code, ring_code, waker_code, SUBETHA_E_BUFFER_TOO_SMALL,
    SUBETHA_E_DESTROYED,
    SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_INVALID_UTF8, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_RING_WAKER_FULL, SUBETHA_E_TIMEOUT, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object};
use crate::runtime::{
    entry, issue, require_initialized, resolve_mode, with_ring, SUBETHA_MODE_MANAGED,
};

/// Bytes one slot holds. A pop buffer must be at least this large.
pub const SUBETHA_RING_SLOT_BYTES: usize = 64;
/// The largest payload every shape accepts. A push between this and
/// `SUBETHA_RING_SLOT_BYTES` succeeds only while the ring is in its
/// single-producer single-consumer shape, and is otherwise refused with
/// `SUBETHA_E_RING_PAYLOAD_TOO_LARGE`.
pub const SUBETHA_RING_PAYLOAD_MAX: usize = 56;

/// Ring shape: one producer, one consumer.
pub const SUBETHA_RING_SHAPE_SPSC: u32 = 0;
/// Ring shape: several producers, one consumer.
pub const SUBETHA_RING_SHAPE_MPSC: u32 = 1;
/// Ring shape: several producers, several consumers.
pub const SUBETHA_RING_SHAPE_MPMC: u32 = 2;
/// Ring shape: the Vyukov queue, global FIFO across producers.
pub const SUBETHA_RING_SHAPE_VYUKOV: u32 = 3;

/// Shared-memory names resolve per session (`Local\` on Windows).
pub const SUBETHA_SHM_SESSION: u32 = 0;
/// Shared-memory names resolve machine-wide (`Global\` on Windows).
pub const SUBETHA_SHM_MACHINE: u32 = 1;

/// The shape sidecar's scan cadence when a managed-mode caller names
/// none, in microseconds.
///
/// The cadence is the added round-trip latency of managed mode, so this
/// is a latency budget rather than a tuned optimum: measured on Linux
/// and FreeBSD from 250 to 10000 microseconds, a request/response round
/// trip costs exactly one interval and a streaming caller costs nothing,
/// with no knee anywhere in that range. A quarter of a millisecond is
/// small against a network hop and against most process-to-process round
/// trips, and costs a few percent of the scanning CPU that a ten-times
/// shorter cadence would.
pub const SUBETHA_SCAN_INTERVAL_DEFAULT_US: u64 = 250;

/// Wait without a deadline.
pub const SUBETHA_WAIT_FOREVER: i64 = -1;

/// The largest frame that rides inside a slot; a larger one spills to the
/// ring's payload region, which exists from the first spill on.
pub const SUBETHA_RING_FRAME_INLINE_BUDGET: usize = 51;
/// Bytes per block of the payload region the ring creates on its first
/// spilled frame. A frame larger than this is refused with
/// `SUBETHA_E_RING_PAYLOAD_TOO_LARGE`.
pub const SUBETHA_RING_FRAME_DEFAULT_BLOCK: usize = 8192;

/// The frame rode inside the slot.
pub const SUBETHA_FRAME_INLINE: u32 = 0;
/// The frame's bytes live in the payload region; the slot carried the offset.
pub const SUBETHA_FRAME_OFFSET: u32 = 1;

/// Inline when the frame fits the budget, else the region.
pub const SUBETHA_LAYOUT_AUTO: u32 = 0;
/// Inline only; a frame past the budget is refused.
pub const SUBETHA_LAYOUT_FORCE_INLINE: u32 = 1;
/// The region even for a frame that would fit inline.
pub const SUBETHA_LAYOUT_FORCE_OFFSET: u32 = 2;

/// No ordering stamps: the ring's payload is the whole slot and its
/// ordering is per-producer FIFO.
pub const SUBETHA_STAMPS_NONE: u32 = 0;
/// Stamps from the host's default source: the TSC where it is invariant,
/// the shared counter on x86 without one, the monotonic clock elsewhere.
/// On an open, whatever the creator chose.
pub const SUBETHA_STAMPS_DEFAULT: u32 = 1;
/// Stamps from the invariant TSC.
pub const SUBETHA_STAMPS_TSC: u32 = 2;
/// Stamps from one shared counter: a total order at the price of one
/// contended increment per push.
pub const SUBETHA_STAMPS_SHARED_COUNTER: u32 = 3;
/// Stamps from the system's monotonic clock, in nanoseconds.
pub const SUBETHA_STAMPS_MONOTONIC: u32 = 4;

/// Ordering mode: the partition pop, per-producer FIFO; stamps only feed
/// the inversion counter.
pub const SUBETHA_ORDERING_UNORDERED: u32 = 0;
/// Ordering mode: a k-way merge by stamp over the ring heads, global FIFO
/// within the stamp source's skew window, under a single drainer.
pub const SUBETHA_ORDERING_MERGE_BY_STAMP: u32 = 1;
/// Ordering mode: the merge plus the per-producer watermark gate, exact
/// global FIFO at the cost of waiting on the slowest producer.
pub const SUBETHA_ORDERING_MERGE_STRICT: u32 = 2;

/// Contract ordering: no guarantee.
pub const SUBETHA_CONTRACT_UNORDERED: u32 = 0;
/// Contract ordering: each producer's items in push order.
pub const SUBETHA_CONTRACT_FIFO_PER_PRODUCER: u32 = 1;
/// Contract ordering: one global order across every producer; the
/// per-producer-lane shape is then refused.
pub const SUBETHA_CONTRACT_FIFO: u32 = 2;
/// Contract ordering: an item lands at most `k_out_of_order` positions
/// from its global arrival order.
pub const SUBETHA_CONTRACT_K_OUT_OF_ORDER: u32 = 3;

/// A declared contract for a ring: peer ceilings, an ordering envelope,
/// a capacity ceiling. A zero everywhere is the unbounded contract, which
/// is what a ring has when none is declared: registration then grows the
/// ring and never fails.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_ring_contract {
    /// Producers that may be registered at once; zero is unbounded.
    pub max_producers: u8,
    /// Consumers that may be registered at once; zero is unbounded.
    pub max_consumers: u8,
    /// One of the `SUBETHA_CONTRACT_` constants.
    pub ordering: u32,
    /// The `k` of `SUBETHA_CONTRACT_K_OUT_OF_ORDER`; ignored otherwise.
    pub k_out_of_order: u32,
    /// A ceiling on the capacity; zero is none.
    pub capacity_bound: u32,
}

/// Options every ring constructor takes. A C initializer that names the
/// first three fields and leaves the rest zero asks for no stamps, no
/// contract, the lazily created frame region and no security descriptor.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct subetha_ring_options {
    /// `SUBETHA_MODE_STRICT`, `SUBETHA_MODE_MANAGED`, or `SUBETHA_MODE_DEFAULT`.
    pub mode: u32,
    /// Waiter slots in each of the ring's two wakers; zero takes the
    /// library's default of 32.
    pub max_waiters: u32,
    /// Managed mode only: how often the shape sidecar scans, in
    /// microseconds. Zero takes `SUBETHA_SCAN_INTERVAL_DEFAULT_US`.
    /// Ignored in strict mode.
    ///
    /// This value is the added round-trip latency of managed mode: a
    /// request and its response each wait for a scan, so a caller doing
    /// request/response pays one interval per round trip. Streaming
    /// callers pay nothing. Measured on Linux and FreeBSD across 250 to
    /// 10000 microseconds, the extra time per round trip equals the
    /// interval at every point.
    pub scan_interval_us: u64,
    /// One of the `SUBETHA_STAMPS_` constants. A stamped ring carries an
    /// ordering region beside its backings, pays 8 bytes of every slot
    /// for the stamp, never takes the Vyukov shape, and refuses frames.
    pub stamps: u32,
    /// The ring's declared contract; all zero declares none.
    pub contract: subetha_ring_contract,
    /// Bytes per block of the adaptive ring's frame payload region, which
    /// is created at construction when this and `frame_blocks` are both
    /// non-zero; a frame up to this size then travels past the slot. Zero
    /// leaves the region to be created at
    /// `SUBETHA_RING_FRAME_DEFAULT_BLOCK` bytes on the first spilled
    /// frame. Set with `frame_blocks` or not at all.
    pub frame_block: u64,
    /// Blocks in the frame payload region; see `frame_block`.
    pub frame_blocks: u32,
    /// Holder slots in the ring's holder table, which is the number of
    /// handles that may hold the backings open at once. Zero takes no
    /// hold at all, which is the behavior of a ring that names no
    /// holder table: the backings outlive every handle. A file-backed
    /// ring only; see `last_holder`.
    pub max_holders: u32,
    /// `SUBETHA_LAST_HOLDER_KEEP` or `SUBETHA_LAST_HOLDER_UNLINK`: what
    /// becomes of the backings when the last holder releases. Unlinking
    /// needs `max_holders`, and the library names no default for it, so
    /// asking to unlink with `max_holders` zero is refused rather than
    /// given a number nobody chose. Only a file-backed ring has files to
    /// remove; anonymous and shared-memory rings refuse a hold.
    pub last_holder: u32,
    /// The security descriptor, as an SDDL string, for every shared-memory
    /// region this object creates; null applies the platform's default,
    /// which on Windows admits the creating account and administrators.
    /// A region that must be mapped from another Windows session needs
    /// one. Ignored where the object is not in shared memory and on
    /// platforms without descriptors.
    pub shm_sddl: *const c_char,
}

impl Default for subetha_ring_options {
    fn default() -> Self {
        Self {
            mode: 0,
            max_waiters: 0,
            scan_interval_us: 0,
            stamps: 0,
            contract: subetha_ring_contract::default(),
            frame_block: 0,
            frame_blocks: 0,
            max_holders: 0,
            last_holder: 0,
            shm_sddl: std::ptr::null(),
        }
    }
}

/// The security descriptor the options carry, or `None` when the field is
/// null.
///
/// # Safety
/// `options.shm_sddl` is null or a NUL-terminated UTF-8 string that
/// outlives the returned reference.
pub(crate) unsafe fn sddl_of<'a>(options: &subetha_ring_options) -> Result<Option<&'a str>, i32> {
    if options.shm_sddl.is_null() {
        return Ok(None);
    }
    Ok(Some(unsafe { text(options.shm_sddl, "shm_sddl") }?))
}

/// The frame region geometry the options ask for: `None` for the lazily
/// created default, `Some((block, blocks))` when both fields are set; one
/// without the other is refused.
pub(crate) fn frame_geometry(options: &subetha_ring_options) -> Result<Option<(usize, usize)>, i32> {
    match (options.frame_block, options.frame_blocks) {
        (0, 0) => Ok(None),
        (block, blocks) if block != 0 && blocks != 0 => match usize::try_from(block) {
            Ok(block) => Ok(Some((block, blocks as usize))),
            Err(e) => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("frame_block {block} does not fit this platform: {e}"))),
        },
        _ => Err(fail(SUBETHA_E_INVALID_ARGUMENT, "frame_block and frame_blocks are set together or not at all")),
    }
}

/// A snapshot of a ring's state.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_ring_stats {
    /// One of the `SUBETHA_RING_SHAPE_` constants.
    pub shape: u32,
    /// The mode this handle was created in, strict or managed.
    pub mode: u32,
    /// Producers currently registered.
    pub active_producers: u32,
    /// Consumers currently registered.
    pub active_consumers: u32,
    /// The producer count the ring was sized for; registration grows past it.
    pub max_producers: u32,
    /// The consumer count the ring was sized for.
    pub max_consumers: u32,
    /// Slots per backing.
    pub capacity: u64,
    /// Items in the current shape's backing, read without a lock.
    pub approx_len: u64,
    /// Shape morphs the ring refused because the previous shape still held
    /// a backlog.
    pub morph_refusals: u64,
    /// Ordering-mode flips the ring refused.
    pub mode_refusals: u64,
    /// Waits that found every waiter slot in use and returned
    /// `SUBETHA_E_RING_WAKER_FULL` instead of parking.
    pub waker_full: u64,
    /// Morphs the managed-mode sidecar issued; zero in strict mode.
    pub sidecar_morphs: u64,
    /// Cross-producer inversions observed at pop since the ordering
    /// region was created; zero on an unstamped ring.
    pub inversions: u64,
    /// One of the `SUBETHA_STAMPS_` constants, never the default: the
    /// source the stamps actually come from, or none.
    pub stamps: u32,
    /// The live ordering mode, one of the `SUBETHA_ORDERING_` constants;
    /// unordered on an unstamped ring.
    pub ordering_mode: u32,
}

/// What an unlink found.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_unlink_report {
    /// Files removed.
    pub removed: u64,
    /// Files the prefix names that were not present.
    pub missing: u64,
    /// Files whose removal the OS refused; the first is named in the detail.
    pub failed: u64,
}

/// The target every parked waiter registers and every wake reaches: any
/// item counts, so the sequence has no meaning here beyond zero.
const WAKE_ANY: u64 = 0;

pub(crate) struct RingObject {
    pub(crate) ring: Arc<AdaptiveRing>,
    pub(crate) consumer_waker: Arc<CrossProcessWaker>,
    pub(crate) producer_waker: Arc<CrossProcessWaker>,
    sidecar: Mutex<Option<AdaptiveRingSidecar>>,
    mode: u32,
    closing: AtomicBool,
    waker_full: AtomicU64,
    /// Frames taken from the ring for a consumer whose buffer had no room,
    /// kept by consumer id until a call with room.
    held: Mutex<HashMap<u32, Vec<u8>>>,
    /// The pollable notifiers attached to this ring by any process; every
    /// push signals them.
    pub(crate) notifiers: subetha_cxc::cross_process_notifier::NotifierSet,
}

/// `last_holder`: the backings outlive the last handle that held them,
/// which is what a ring with no holder table does.
pub const SUBETHA_LAST_HOLDER_KEEP: u32 = 0;
/// `last_holder`: the last handle to release removes every backing the
/// ring's prefix names, including the holder table itself.
pub const SUBETHA_LAST_HOLDER_UNLINK: u32 = 1;

/// Where an object's backings live, which decides how the wakers the ABI
/// keeps beside it are built.
pub(crate) enum Locale<'a> {
    Anon,
    File(&'a Path),
    Shm { name: &'a str, namespace: ShmNamespace, create: bool, sddl: Option<&'a str> },
}

pub(crate) fn waker_capacity(max_waiters: u32) -> usize {
    if max_waiters == 0 {
        MAX_WAITERS_DEFAULT
    } else {
        max_waiters as usize
    }
}

/// The notifier set an object keeps beside its backings: in this process
/// for an anonymous object, the record `<prefix>.notify.bin` beside a
/// file, the region `{name}_notify` in shared memory.
pub(crate) fn notifiers_for(locale: &Locale<'_>) -> Result<subetha_cxc::cross_process_notifier::NotifierSet, i32> {
    use subetha_cxc::cross_process_notifier::NotifierSet;
    match locale {
        Locale::Anon => Ok(NotifierSet::anon()),
        Locale::File(prefix) => NotifierSet::file(prefix).map_err(crate::error::notify_code),
        Locale::Shm { name, namespace, sddl, .. } => NotifierSet::shm(name, *namespace, *sddl).map_err(crate::error::notify_code),
    }
}

/// The consumer waker and the producer waker an object keeps beside its
/// backings: anonymous, `<prefix>.cwaker.bin` and `.pwaker.bin`, or
/// `{name}_cwaker` and `_pwaker` in shared memory.
pub(crate) fn wakers_for(locale: &Locale<'_>, max_waiters: u32) -> Result<(CrossProcessWaker, CrossProcessWaker), i32> {
    Ok((
        waker_for(locale, max_waiters, ".cwaker.bin", "_cwaker")?,
        waker_for(locale, max_waiters, ".pwaker.bin", "_pwaker")?,
    ))
}

/// One waker beside an object's backings, named by `file_suffix` in the
/// file locale and `shm_suffix` in shared memory.
pub(crate) fn waker_for(
    locale: &Locale<'_>,
    max_waiters: u32,
    file_suffix: &str,
    shm_suffix: &str,
) -> Result<CrossProcessWaker, i32> {
    let capacity = waker_capacity(max_waiters);
    match locale {
        Locale::Anon => CrossProcessWaker::create_anon(capacity).map_err(waker_code),
        Locale::File(prefix) => file_waker(prefix, file_suffix, capacity),
        Locale::Shm { name, namespace, create, sddl } => shm_waker(name, shm_suffix, *namespace, capacity, *create, *sddl),
    }
}

/// The named shared-memory region an object's backing lives in, created
/// with `sddl` as its security descriptor or opened.
pub(crate) fn shm_region(name: &str, size: usize, namespace: ShmNamespace, sddl: Option<&str>) -> Result<ShmFile, i32> {
    ShmFile::create_or_open_named_secured(name, size, namespace, sddl)
        .map_err(|e| fail(SUBETHA_E_RING_IO, format!("shared-memory region {name}: {e}")))
}

fn file_waker(prefix: &Path, suffix: &str, capacity: usize) -> Result<CrossProcessWaker, i32> {
    let mut p = prefix.as_os_str().to_owned();
    p.push(suffix);
    CrossProcessWaker::create(PathBuf::from(p), capacity).map_err(waker_code)
}

fn shm_waker(
    name: &str,
    suffix: &str,
    namespace: ShmNamespace,
    capacity: usize,
    create: bool,
    sddl: Option<&str>,
) -> Result<CrossProcessWaker, i32> {
    let region = ShmFile::create_or_open_named_secured(
        &format!("{name}{suffix}"),
        waker_region_size(capacity),
        namespace,
        sddl,
    )
    .map_err(|e| fail(SUBETHA_E_RING_IO, format!("waker region {name}{suffix}: {e}")))?;
    if create {
        CrossProcessWaker::create_from_shm(region, capacity).map_err(waker_code)
    } else {
        CrossProcessWaker::open_from_shm(region, capacity).map_err(waker_code)
    }
}

/// The stamp kind a `SUBETHA_STAMPS_` constant names; `None` for the
/// default, which the ring resolves itself.
pub(crate) fn stamp_kind(stamps: u32) -> Result<Option<StampKind>, i32> {
    match stamps {
        SUBETHA_STAMPS_DEFAULT => Ok(None),
        SUBETHA_STAMPS_TSC => Ok(Some(StampKind::Tsc)),
        SUBETHA_STAMPS_SHARED_COUNTER => Ok(Some(StampKind::SharedCounter)),
        SUBETHA_STAMPS_MONOTONIC => Ok(Some(StampKind::Monotonic)),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("stamps {other} is not a stamp source"))),
    }
}

fn stamps_constant(kind: StampKind) -> u32 {
    match kind {
        StampKind::Tsc => SUBETHA_STAMPS_TSC,
        StampKind::SharedCounter => SUBETHA_STAMPS_SHARED_COUNTER,
        StampKind::Monotonic => SUBETHA_STAMPS_MONOTONIC,
    }
}

pub(crate) fn ordering_constant(mode: OrderingMode) -> u32 {
    match mode {
        OrderingMode::Unordered => SUBETHA_ORDERING_UNORDERED,
        OrderingMode::MergeByStamp => SUBETHA_ORDERING_MERGE_BY_STAMP,
        OrderingMode::MergeStrict => SUBETHA_ORDERING_MERGE_STRICT,
    }
}

pub(crate) fn ordering_mode(mode: u32) -> Result<OrderingMode, i32> {
    match mode {
        SUBETHA_ORDERING_UNORDERED => Ok(OrderingMode::Unordered),
        SUBETHA_ORDERING_MERGE_BY_STAMP => Ok(OrderingMode::MergeByStamp),
        SUBETHA_ORDERING_MERGE_STRICT => Ok(OrderingMode::MergeStrict),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("ordering mode {other} is not a mode"))),
    }
}

/// The contract a `subetha_ring_contract` declares; `None` when every
/// field is zero.
fn contract_from(c: &subetha_ring_contract) -> Result<Option<RingContract>, i32> {
    let ordering = match c.ordering {
        SUBETHA_CONTRACT_UNORDERED => OrderingContract::Unordered,
        SUBETHA_CONTRACT_FIFO_PER_PRODUCER => OrderingContract::FifoPerProducer,
        SUBETHA_CONTRACT_FIFO => OrderingContract::Fifo,
        SUBETHA_CONTRACT_K_OUT_OF_ORDER => OrderingContract::KOutOfOrder(c.k_out_of_order),
        other => return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("contract ordering {other} is not an ordering"))),
    };
    if c.max_producers == 0 && c.max_consumers == 0 && ordering == OrderingContract::Unordered && c.capacity_bound == 0 {
        return Ok(None);
    }
    Ok(Some(RingContract {
        max_concurrent_push: c.max_producers,
        max_concurrent_pop: c.max_consumers,
        ordering,
        capacity_bound: if c.capacity_bound == 0 { None } else { Some(c.capacity_bound) },
    }))
}

/// Apply the stamps, the contract and the frame region geometry the
/// options ask for to a ring a constructor just built or opened.
pub(crate) fn shape_ring(ring: AdaptiveRing, options: &subetha_ring_options) -> Result<AdaptiveRing, i32> {
    let ring = if options.stamps == SUBETHA_STAMPS_NONE {
        ring
    } else {
        match stamp_kind(options.stamps)? {
            None => ring.with_ordering_stamps().map_err(ring_code)?,
            Some(kind) => ring.with_ordering_stamps_kind(kind).map_err(ring_code)?,
        }
    };
    let ring = match contract_from(&options.contract)? {
        Some(contract) => ring.with_contract(contract),
        None => ring,
    };
    Ok(match frame_geometry(options)? {
        Some((block, blocks)) => ring.with_frames(block, blocks),
        None => ring,
    })
}

/// Take a hold on the ring's backings when the options ask for one.
///
/// The two fields are read together because either alone names an
/// incomplete request: a count with nothing to do at the end, or an end
/// state with no table to record holders in.
fn hold_ring(
    ring: AdaptiveRing,
    locale: &Locale<'_>,
    options: &subetha_ring_options,
) -> Result<AdaptiveRing, i32> {
    let on_last = match options.last_holder {
        SUBETHA_LAST_HOLDER_KEEP => LastHolder::Keep,
        SUBETHA_LAST_HOLDER_UNLINK => LastHolder::Unlink,
        other => {
            return Err(fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("last_holder {other} names no last-holder disposition"),
            ))
        }
    };
    if options.max_holders == 0 {
        if on_last == LastHolder::Unlink {
            return Err(fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "unlinking on the last holder needs max_holders; the library names no default",
            ));
        }
        return Ok(ring);
    }
    if !matches!(locale, Locale::File(_)) {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "max_holders needs a file-backed ring; only its backings are files a holder can remove",
        ));
    }
    // The wakers and the notifier record are this layer's, kept beside
    // the ring's backings and invisible to it. Handed over so the last
    // holder takes the whole prefix rather than the ring's share of it.
    ring.with_last_holder(
        options.max_holders as usize,
        on_last,
        &[".cwaker.bin", ".pwaker.bin", ".notify.bin"],
    )
        .map_err(last_holder_code)
}

impl RingObject {
    fn build(ring: AdaptiveRing, locale: Locale<'_>, options: subetha_ring_options) -> Result<Self, i32> {
        let mode = resolve_mode(options.mode)?;
        let ring = shape_ring(ring, &options)?;
        let ring = hold_ring(ring, &locale, &options)?;
        let (consumer_waker, producer_waker) = wakers_for(&locale, options.max_waiters)?;
        let notifiers = notifiers_for(&locale)?;
        let ring = Arc::new(ring);
        let sidecar = if mode == SUBETHA_MODE_MANAGED {
            let scan = if options.scan_interval_us == 0 {
                SUBETHA_SCAN_INTERVAL_DEFAULT_US
            } else {
                options.scan_interval_us
            };
            Some(AdaptiveRingSidecar::spawn(
                Arc::clone(&ring),
                DefaultRingShapePolicy::default(),
                Duration::from_micros(scan),
            ))
        } else {
            None
        };
        Ok(Self {
            ring,
            consumer_waker: Arc::new(consumer_waker),
            producer_waker: Arc::new(producer_waker),
            sidecar: Mutex::new(sidecar),
            mode,
            closing: AtomicBool::new(false),
            waker_full: AtomicU64::new(0),
            held: Mutex::new(HashMap::new()),
            notifiers,
        })
    }

    /// An anonymous ring for the handle table's own tests.
    #[cfg(test)]
    pub(crate) fn anon_for_tests() -> Self {
        let ring = AdaptiveRing::create_anon(1, 1, 64).expect("an anonymous ring");
        Self::build(
            ring,
            Locale::Anon,
            subetha_ring_options { mode: crate::runtime::SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() },
        )
        .expect("an anonymous ring object")
    }

    /// Wake every parked waiter so a destroy can proceed; each returns
    /// `SUBETHA_E_DESTROYED`.
    pub(crate) fn interrupt(&self) {
        self.closing.store(true, Ordering::Release);
        self.consumer_waker.wake_all();
        self.producer_waker.wake_all();
    }

    fn try_push(&self, producer_id: usize, payload: &[u8]) -> i32 {
        match self.ring.try_send(producer_id, payload) {
            Ok(()) => {
                self.consumer_waker.wake_one_up_to(WAKE_ANY);
                self.notifiers.signal();
                SUBETHA_OK
            }
            Err(e) => ring_code(e),
        }
    }

    fn try_pop(&self, consumer_id: usize, out: &mut [u8]) -> Result<usize, i32> {
        match self.ring.try_recv(consumer_id, out) {
            Ok(n) => {
                self.producer_waker.wake_one_up_to(WAKE_ANY);
                Ok(n)
            }
            Err(e) => Err(ring_code(e)),
        }
    }

    /// Park on `waker` until a wake or the deadline. `Ok(true)` means woken,
    /// `Ok(false)` means the item was already there on the re-check.
    fn park(&self, waker: &CrossProcessWaker, deadline: Option<Instant>, ready: impl Fn() -> bool) -> Result<bool, i32> {
        let token = match waker.try_park(WAKE_ANY) {
            Ok(t) => t,
            Err(WakerError::Full) => {
                self.waker_full.fetch_add(1, Ordering::AcqRel);
                return Err(fail(SUBETHA_E_RING_WAKER_FULL, "every waiter slot is parked"));
            }
            Err(e) => return Err(waker_code(e)),
        };
        // A wake that landed between the failed try and the park is caught
        // here; the slot goes back without entering the kernel.
        if ready() {
            waker.release(token);
            return Ok(false);
        }
        if self.closing.load(Ordering::Acquire) {
            waker.release(token);
            return Err(fail(SUBETHA_E_DESTROYED, "the ring is being destroyed"));
        }
        let remaining = match deadline {
            None => None,
            Some(d) => match d.checked_duration_since(Instant::now()) {
                Some(r) if !r.is_zero() => Some(r),
                _ => {
                    waker.release(token);
                    return Err(fail(SUBETHA_E_TIMEOUT, "the timeout elapsed"));
                }
            },
        };
        match waker.wait(token, remaining) {
            Ok(()) => Ok(true),
            Err(WakerError::Timeout) => Err(fail(SUBETHA_E_TIMEOUT, "the timeout elapsed")),
            Err(e) => Err(waker_code(e)),
        }
    }

    fn pop_wait(&self, consumer_id: usize, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        loop {
            match self.try_pop(consumer_id, out) {
                Ok(n) => return Ok(n),
                Err(code) if code == crate::error::SUBETHA_E_RING_EMPTY => {}
                Err(code) => return Err(code),
            }
            if self.closing.load(Ordering::Acquire) {
                return Err(fail(SUBETHA_E_DESTROYED, "the ring is being destroyed"));
            }
            self.park(&self.consumer_waker, deadline, || !self.ring.is_empty())?;
        }
    }

    fn push_wait(&self, producer_id: usize, payload: &[u8], deadline: Option<Instant>) -> i32 {
        loop {
            match self.try_push(producer_id, payload) {
                SUBETHA_OK => return SUBETHA_OK,
                code if code == crate::error::SUBETHA_E_RING_FULL => {}
                code => return code,
            }
            if self.closing.load(Ordering::Acquire) {
                return fail(SUBETHA_E_DESTROYED, "the ring is being destroyed");
            }
            let room = || self.ring.approx_len() < self.ring.total_slot_capacity();
            if let Err(code) = self.park(&self.producer_waker, deadline, room) {
                return code;
            }
        }
    }

    fn send_frame(&self, producer_id: usize, payload: &[u8], hint: LayoutHint) -> Result<u32, i32> {
        match self.ring.send_frame_as(producer_id, payload, hint) {
            Ok(class) => {
                self.consumer_waker.wake_one_up_to(WAKE_ANY);
                self.notifiers.signal();
                Ok(match class {
                    FrameClass::Inline => SUBETHA_FRAME_INLINE,
                    FrameClass::Offset => SUBETHA_FRAME_OFFSET,
                })
            }
            Err(e) => Err(ring_code(e)),
        }
    }

    /// Take the next frame for `consumer_id` into `out`. A frame held from an
    /// earlier call that had no room comes first. A frame larger than `out`
    /// is kept for this consumer and reported as `SUBETHA_E_BUFFER_TOO_SMALL`
    /// with its size in `Err`'s companion, so nothing is lost.
    fn recv_frame(&self, consumer_id: u32, out: &mut [u8]) -> Result<usize, (i32, usize)> {
        let held = self.held.lock().remove(&consumer_id);
        let frame = match held {
            Some(frame) => frame,
            None => {
                let mut frame = Vec::new();
                match self.ring.recv_frame(consumer_id as usize, &mut frame) {
                    Ok(_) => {
                        self.producer_waker.wake_one_up_to(WAKE_ANY);
                        frame
                    }
                    Err(e) => return Err((ring_code(e), 0)),
                }
            }
        };
        if frame.len() > out.len() {
            let needed = frame.len();
            self.held.lock().insert(consumer_id, frame);
            return Err((
                fail(
                    SUBETHA_E_BUFFER_TOO_SMALL,
                    format!("the frame is {needed} bytes; it is held for consumer {consumer_id} until a call with room"),
                ),
                needed,
            ));
        }
        out[..frame.len()].copy_from_slice(&frame);
        Ok(frame.len())
    }

    fn recv_frame_wait(&self, consumer_id: u32, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, (i32, usize)> {
        loop {
            match self.recv_frame(consumer_id, out) {
                Ok(n) => return Ok(n),
                Err((code, _)) if code == crate::error::SUBETHA_E_RING_EMPTY => {}
                Err(e) => return Err(e),
            }
            if self.closing.load(Ordering::Acquire) {
                return Err((fail(SUBETHA_E_DESTROYED, "the ring is being destroyed"), 0));
            }
            if let Err(code) = self.park(&self.consumer_waker, deadline, || !self.ring.is_empty()) {
                return Err((code, 0));
            }
        }
    }

    fn send_frame_wait(&self, producer_id: usize, payload: &[u8], hint: LayoutHint, deadline: Option<Instant>) -> Result<u32, i32> {
        loop {
            match self.send_frame(producer_id, payload, hint) {
                Ok(class) => return Ok(class),
                Err(code) if code == crate::error::SUBETHA_E_RING_FULL => {}
                Err(code) => return Err(code),
            }
            if self.closing.load(Ordering::Acquire) {
                return Err(fail(SUBETHA_E_DESTROYED, "the ring is being destroyed"));
            }
            let room = || self.ring.approx_len() < self.ring.total_slot_capacity();
            self.park(&self.producer_waker, deadline, room)?;
        }
    }

    fn stats(&self) -> subetha_ring_stats {
        let shape = match self.ring.current_shape() {
            RingShape::Spsc => SUBETHA_RING_SHAPE_SPSC,
            RingShape::Mpsc => SUBETHA_RING_SHAPE_MPSC,
            RingShape::Mpmc => SUBETHA_RING_SHAPE_MPMC,
            RingShape::Vyukov => SUBETHA_RING_SHAPE_VYUKOV,
        };
        let sidecar_morphs = self.sidecar.lock().as_ref().map_or(0, |s| s.morphs_triggered());
        let stamps = self
            .ring
            .ordering_region()
            .map_or(SUBETHA_STAMPS_NONE, |region| stamps_constant(region.stamp_kind()));
        let ordering_mode = self
            .ring
            .ordering_mode()
            .map_or(SUBETHA_ORDERING_UNORDERED, ordering_constant);
        subetha_ring_stats {
            shape,
            mode: self.mode,
            active_producers: self.ring.active_producers() as u32,
            active_consumers: self.ring.active_consumers() as u32,
            max_producers: self.ring.max_producers() as u32,
            max_consumers: self.ring.max_consumers() as u32,
            capacity: self.ring.sub_ring_capacity() as u64,
            approx_len: self.ring.approx_len() as u64,
            morph_refusals: self.ring.morph_refusals(),
            mode_refusals: self.ring.mode_refusals(),
            waker_full: self.waker_full.load(Ordering::Acquire),
            sidecar_morphs,
            inversions: self.ring.inversions(),
            stamps,
            ordering_mode,
        }
    }

    fn try_pop_stamped(&self, consumer_id: usize, out: &mut [u8]) -> Result<(usize, u64), i32> {
        match self.ring.try_recv_with_stamp(consumer_id, out) {
            Ok(taken) => {
                self.producer_waker.wake_one_up_to(WAKE_ANY);
                Ok(taken)
            }
            Err(e) => Err(ring_code(e)),
        }
    }

    fn pop_wait_stamped(&self, consumer_id: usize, out: &mut [u8], deadline: Option<Instant>) -> Result<(usize, u64), i32> {
        loop {
            match self.try_pop_stamped(consumer_id, out) {
                Ok(taken) => return Ok(taken),
                Err(code) if code == crate::error::SUBETHA_E_RING_EMPTY => {}
                Err(code) => return Err(code),
            }
            if self.closing.load(Ordering::Acquire) {
                return Err(fail(SUBETHA_E_DESTROYED, "the ring is being destroyed"));
            }
            self.park(&self.consumer_waker, deadline, || !self.ring.is_empty())?;
        }
    }
}

impl Drop for RingObject {
    fn drop(&mut self) {
        // The sidecar's own Drop joins its thread and reports a panic on
        // stderr; taking it here makes that order explicit before the ring
        // it scans goes away.
        drop(self.sidecar.lock().take());
    }
}

/// Read a NUL-terminated UTF-8 argument.
///
/// # Safety
/// `s` is null or a NUL-terminated string.
pub(crate) unsafe fn text<'a>(s: *const c_char, what: &str) -> Result<&'a str, i32> {
    if s.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} is null")));
    }
    // SAFETY: the caller guarantees a NUL-terminated string.
    let c = unsafe { CStr::from_ptr(s) };
    c.to_str().map_err(|e| fail(SUBETHA_E_INVALID_UTF8, format!("{what} is not UTF-8: {e}")))
}

/// A ring capacity: a power of two of at least 2.
pub(crate) fn checked_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity < 2 || !capacity.is_power_of_two() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("capacity {capacity} is not a power of two of at least 2")));
    }
    Ok(capacity as usize)
}

fn counts(max_producers: u32, max_consumers: u32, capacity: u32) -> Result<(usize, usize, usize), i32> {
    if max_producers == 0 || max_consumers == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_producers and max_consumers must be at least 1"));
    }
    Ok((max_producers as usize, max_consumers as usize, checked_capacity(capacity)?))
}

/// The byte slice a `data, len` pair names; `len` zero with a null `data`
/// is the empty slice.
///
/// # Safety
/// `data` is null or points to `len` readable bytes.
pub(crate) unsafe fn bytes<'a>(data: *const u8, len: usize) -> Result<&'a [u8], i32> {
    if data.is_null() {
        if len != 0 {
            return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "data is null with a non-zero len"));
        }
        return Ok(&[]);
    }
    // SAFETY: the caller guarantees `len` readable bytes at `data`.
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

/// The writable slice an `out, cap` pair names, which must hold at least
/// `at_least` bytes; `out_len` must not be null.
///
/// # Safety
/// `out` is null or points to `cap` writable bytes.
pub(crate) unsafe fn out_buffer<'a>(out: *mut u8, cap: usize, out_len: *mut usize, at_least: usize) -> Result<&'a mut [u8], i32> {
    if out.is_null() || out_len.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out or out_len is null"));
    }
    if cap < at_least {
        return Err(fail(SUBETHA_E_BUFFER_TOO_SMALL, format!("{at_least} bytes needed, {cap} given")));
    }
    // SAFETY: the caller guarantees `cap` writable bytes at `out`.
    Ok(unsafe { std::slice::from_raw_parts_mut(out, cap) })
}

/// Copy `source` into an `out, cap` pair, reporting the byte count through
/// `len` whether or not it fitted. A caller who passes a null buffer, or one
/// too small, is told the size it needs and nothing is copied.
///
/// Distinct from [`out_buffer`]: that one refuses before naming a size, which
/// suits a caller holding a buffer it sized from the item. This one suits a
/// caller who cannot know the size until the call produces it, and so asks
/// twice.
///
/// # Safety
/// `out` is null or points to `cap` writable bytes; `len` is null or a valid
/// pointer.
pub(crate) unsafe fn copy_out(
    source: &[u8],
    out: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Result<(), i32> {
    if len.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a length pointer is null"));
    }
    // Checked non-null; the caller guarantees it is writable.
    unsafe { *len = source.len() };
    if out.is_null() || cap < source.len() {
        return Err(SUBETHA_E_BUFFER_TOO_SMALL);
    }
    // The caller guarantees `cap` writable bytes, and cap >= len here.
    unsafe { std::ptr::copy_nonoverlapping(source.as_ptr(), out, source.len()) };
    Ok(())
}

/// Write what an unlink found into the caller's report and turn a refusal
/// into `SUBETHA_E_RING_IO` naming the first one.
///
/// # Safety
/// `report` is null or a valid pointer.
pub(crate) unsafe fn finish_unlink(found: UnlinkReport, report: *mut subetha_unlink_report) -> i32 {
    if !report.is_null() {
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe {
            *report = subetha_unlink_report {
                removed: found.removed as u64,
                missing: found.missing as u64,
                failed: found.failed as u64,
            }
        };
    }
    if found.failed > 0 {
        let first = found
            .first_failure
            .as_ref()
            .map(|(p, k, t)| format!("{}: {k:?}: {t}", p.display()))
            .unwrap_or_else(|| "a removal was refused".to_owned());
        return fail(SUBETHA_E_RING_IO, format!("{} removal(s) refused; first: {first}", found.failed));
    }
    SUBETHA_OK
}

/// `<base><suffix>` as a path.
pub(crate) fn with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
}

/// The shared-memory namespace a `SUBETHA_SHM_` constant names.
pub(crate) fn namespace(ns: u32) -> Result<ShmNamespace, i32> {
    match ns {
        SUBETHA_SHM_SESSION => Ok(ShmNamespace::Session),
        SUBETHA_SHM_MACHINE => Ok(ShmNamespace::Machine),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("namespace {other} is not a namespace"))),
    }
}

/// The options a constructor was handed, once `options` and `out` are
/// known to be non-null.
///
/// # Safety
/// `options` and `out` are null or valid pointers.
pub(crate) unsafe fn read_options(options: *const subetha_ring_options, out: *mut subetha_handle) -> Result<subetha_ring_options, i32> {
    if options.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "options is null"));
    }
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    // SAFETY: checked non-null; the caller guarantees it points at options.
    Ok(unsafe { *options })
}

/// The deadline a `timeout_ms` names: none for `SUBETHA_WAIT_FOREVER`.
pub(crate) fn deadline_from(timeout_ms: i64) -> Result<Option<Instant>, i32> {
    if timeout_ms == SUBETHA_WAIT_FOREVER {
        Ok(None)
    } else if timeout_ms < 0 {
        Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("timeout_ms {timeout_ms} is negative and not SUBETHA_WAIT_FOREVER")))
    } else {
        Ok(Some(Instant::now() + Duration::from_millis(timeout_ms as u64)))
    }
}

/// Create a ring in anonymous memory, reachable from this process only.
/// `capacity` is slots per backing and must be a power of two of at least 2.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_create_anon(
    max_producers: u32,
    max_consumers: u32,
    capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let (mp, mc, cap) = match counts(max_producers, max_consumers, capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match AdaptiveRing::create_anon(mp, mc, cap) {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match RingObject::build(ring, Locale::Anon, options) {
            Ok(object) => unsafe { issue(Object::Ring(object), out) },
            Err(code) => code,
        }
    })
}

/// Create a file-backed ring under `path_prefix`, or attach to one that
/// exists there with the same sizing. The backings are named
/// `<path_prefix>.spsc.bin`, `.mpsc.<i>.bin`, `.mpmc.<i>.bin`,
/// `.vyukov.bin`, `.peers.bin`, plus `.cwaker.bin` and `.pwaker.bin` for
/// the waiters; a frame region and an ordering region may appear beside
/// them later.
///
/// # Safety
/// `path_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_create(
    path_prefix: *const c_char,
    max_producers: u32,
    max_consumers: u32,
    capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let prefix = match unsafe { text(path_prefix, "path_prefix") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let (mp, mc, cap) = match counts(max_producers, max_consumers, capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match AdaptiveRing::create(prefix, mp, mc, cap) {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match RingObject::build(ring, Locale::File(prefix), options) {
            Ok(object) => unsafe { issue(Object::Ring(object), out) },
            Err(code) => code,
        }
    })
}

/// Attach to a file-backed ring another process created under
/// `path_prefix`. Fails with `SUBETHA_E_RING_LAYOUT_MISMATCH` when a backing
/// is absent or was created with another capacity.
///
/// # Safety
/// `path_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_open(
    path_prefix: *const c_char,
    max_producers: u32,
    max_consumers: u32,
    expected_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let prefix = match unsafe { text(path_prefix, "path_prefix") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let (mp, mc, cap) = match counts(max_producers, max_consumers, expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match AdaptiveRing::open(prefix, mp, mc, cap) {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match RingObject::build(ring, Locale::File(prefix), options) {
            Ok(object) => unsafe { issue(Object::Ring(object), out) },
            Err(code) => code,
        }
    })
}

/// Create a ring in named shared memory under `name`, in the namespace
/// `SUBETHA_SHM_SESSION` or `SUBETHA_SHM_MACHINE`. The regions are named
/// `{name}_spsc`, `{name}_mpsc_<i>`, `{name}_mpmc_<i>`, `{name}_vyukov`,
/// plus `{name}_cwaker` and `{name}_pwaker`.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_create_shm(
    name: *const c_char,
    max_producers: u32,
    max_consumers: u32,
    capacity: u32,
    shm_namespace: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let name = match unsafe { text(name, "name") } {
            Ok(n) => n,
            Err(code) => return code,
        };
        let ns = match namespace(shm_namespace) {
            Ok(ns) => ns,
            Err(code) => return code,
        };
        let (mp, mc, cap) = match counts(max_producers, max_consumers, capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let sddl = match unsafe { sddl_of(&options) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        let ring = match AdaptiveRing::create_shmfs_secured(name, mp, mc, cap, ns, sddl) {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match RingObject::build(ring, Locale::Shm { name, namespace: ns, create: true, sddl }, options) {
            Ok(object) => unsafe { issue(Object::Ring(object), out) },
            Err(code) => code,
        }
    })
}

/// Attach to a ring another process created in named shared memory. The
/// namespace must match the creator's: a mismatch resolves a different,
/// empty set of regions rather than failing.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_open_shm(
    name: *const c_char,
    max_producers: u32,
    max_consumers: u32,
    expected_capacity: u32,
    shm_namespace: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let name = match unsafe { text(name, "name") } {
            Ok(n) => n,
            Err(code) => return code,
        };
        let ns = match namespace(shm_namespace) {
            Ok(ns) => ns,
            Err(code) => return code,
        };
        let (mp, mc, cap) = match counts(max_producers, max_consumers, expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let sddl = match unsafe { sddl_of(&options) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        let ring = match AdaptiveRing::open_shmfs_secured(name, mp, mc, cap, ns, sddl) {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match RingObject::build(ring, Locale::Shm { name, namespace: ns, create: false, sddl }, options) {
            Ok(object) => unsafe { issue(Object::Ring(object), out) },
            Err(code) => code,
        }
    })
}

/// Register as a producer and receive the id every push takes. Past the
/// creation hint the ring grows a backing for the new producer.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_register_producer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_ring(handle, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match r.ring.register_producer() {
            Ok(id) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = id as u32 };
                SUBETHA_OK
            }
            Err(e) => adaptive_code(e),
        }
    })
}

/// Register as a consumer and receive the id every pop takes.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_register_consumer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_ring(handle, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match r.ring.register_consumer() {
            Ok(id) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = id as u32 };
                SUBETHA_OK
            }
            Err(e) => adaptive_code(e),
        }
    })
}

/// Give a producer id back. The ring re-morphs to the live peer counts.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_ring_unregister_producer(handle: subetha_handle, producer_id: u32) -> i32 {
    with_ring(handle, |r| {
        r.ring.unregister_producer(producer_id as usize);
        SUBETHA_OK
    })
}

/// Give a consumer id back. The ring re-morphs to the live peer counts.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_ring_unregister_consumer(handle: subetha_handle, consumer_id: u32) -> i32 {
    with_ring(handle, |r| {
        r.ring.unregister_consumer(consumer_id as usize);
        SUBETHA_OK
    })
}

/// Push `len` bytes from `data` without waiting. `SUBETHA_E_RING_FULL` when
/// there is no room; `SUBETHA_E_RING_PAYLOAD_TOO_LARGE` above the slot.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_try_push(
    handle: subetha_handle,
    producer_id: u32,
    data: *const u8,
    len: usize,
) -> i32 {
    with_ring(handle, |r| {
        if data.is_null() && len != 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "data is null with a non-zero len");
        }
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        // SAFETY: the caller guarantees `len` readable bytes at `data`.
        let payload: &[u8] = if len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(data, len) } };
        r.try_push(producer_id as usize, payload)
    })
}

/// Pop one item into `out` without waiting. The ring carries fixed slots:
/// a push zero-fills the slot past its payload and a pop yields the whole
/// slot, so `out_len` is the slot's size and the payload's own length does
/// not survive the round trip; a caller that needs it carries it inside
/// the payload. `cap` must be at least `SUBETHA_RING_SLOT_BYTES`.
/// `SUBETHA_E_RING_EMPTY` when there is nothing to take.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_try_pop(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_ring(handle, |r| {
        if out.is_null() || out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out or out_len is null");
        }
        if cap < SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_BUFFER_TOO_SMALL, format!("{SUBETHA_RING_SLOT_BYTES} bytes needed, {cap} given"));
        }
        // SAFETY: the caller guarantees `cap` writable bytes at `out`.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, cap) };
        match r.try_pop(consumer_id as usize, buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Push `count` payloads of `len` bytes each, the first at `items` and
/// each next one `stride` bytes on, under one handle lookup and one panic
/// guard. `out_done` receives how many landed: a full ring stops the run
/// early, and the caller comes back with the rest. The code is
/// `SUBETHA_OK` when at least one landed, and the refusal's own when the
/// first push was the one that failed.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_try_push_many(
    handle: subetha_handle,
    producer_id: u32,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_ring(handle, |r| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        // SAFETY: the caller guarantees the array and the count word.
        unsafe { run_push_many(items, stride, len, count, out_done, |payload| r.try_push(producer_id as usize, payload)) }
    })
}

/// Pop up to `count` slots, the first written at `out` and each next one
/// `stride` bytes on, under one handle lookup and one panic guard;
/// `stride` is at least `SUBETHA_RING_SLOT_BYTES`. `out_done` receives how
/// many were taken: an empty ring stops the run early. The code is
/// `SUBETHA_OK` when at least one was taken, and `SUBETHA_E_RING_EMPTY`
/// when none was.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_try_pop_many(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_ring(handle, |r| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, SUBETHA_RING_SLOT_BYTES, count, out_done, |buf| {
                match r.try_pop(consumer_id as usize, buf) {
                    Ok(_) => SUBETHA_OK,
                    Err(code) => code,
                }
            })
        }
    })
}

/// Push, parking until there is room or `timeout_ms` elapses
/// (`SUBETHA_E_TIMEOUT`). `SUBETHA_WAIT_FOREVER` waits without a deadline.
/// `SUBETHA_E_RING_WAKER_FULL` means every waiter slot was in use and
/// nothing was pushed; `SUBETHA_E_DESTROYED` means the ring was destroyed
/// while this call waited.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_push_wait(
    handle: subetha_handle,
    producer_id: u32,
    data: *const u8,
    len: usize,
    timeout_ms: i64,
) -> i32 {
    with_ring(handle, |r| {
        if data.is_null() && len != 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "data is null with a non-zero len");
        }
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        // SAFETY: the caller guarantees `len` readable bytes at `data`.
        let payload: &[u8] = if len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(data, len) } };
        r.push_wait(producer_id as usize, payload, deadline)
    })
}

/// Pop, parking until an item arrives or `timeout_ms` elapses
/// (`SUBETHA_E_TIMEOUT`); the slot semantics are `subetha_ring_try_pop`'s.
/// `SUBETHA_WAIT_FOREVER` waits without a deadline.
/// `SUBETHA_E_RING_WAKER_FULL` means every waiter slot was in use and
/// nothing was taken; `SUBETHA_E_DESTROYED` means the ring was destroyed
/// while this call waited.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_pop_wait(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_ring(handle, |r| {
        if out.is_null() || out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out or out_len is null");
        }
        if cap < SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_BUFFER_TOO_SMALL, format!("{SUBETHA_RING_SLOT_BYTES} bytes needed, {cap} given"));
        }
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        // SAFETY: the caller guarantees `cap` writable bytes at `out`.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, cap) };
        match r.pop_wait(consumer_id as usize, buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Pop on a stamped ring, also yielding the item's stamp into `out_stamp`,
/// so a consumer can check the order it paid for instead of trusting it.
/// Otherwise `subetha_ring_try_pop`. `SUBETHA_E_RING_NOT_STAMPED` on a ring
/// built without stamps; under a merge mode `SUBETHA_E_RING_NOT_DRAINER`
/// while another consumer holds the drainer lease.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` and `out_stamp` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_try_pop_stamped(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_stamp: *mut u64,
) -> i32 {
    with_ring(handle, |r| {
        if out_stamp.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_stamp is null");
        }
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match r.try_pop_stamped(consumer_id as usize, buf) {
            Ok((n, stamp)) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_len = n;
                    *out_stamp = stamp;
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// `subetha_ring_try_pop_stamped`, parking until an item arrives or
/// `timeout_ms` elapses; the waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` and `out_stamp` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_pop_wait_stamped(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_stamp: *mut u64,
    timeout_ms: i64,
) -> i32 {
    with_ring(handle, |r| {
        if out_stamp.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_stamp is null");
        }
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match r.pop_wait_stamped(consumer_id as usize, buf, deadline) {
            Ok((n, stamp)) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_len = n;
                    *out_stamp = stamp;
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Set the ordering mode of a stamped ring, one of the `SUBETHA_ORDERING_`
/// constants. The mode lives in the ring's ordering region, so every
/// process attached reads the same value; a flip takes effect on the next
/// pop with no drain. `SUBETHA_E_RING_NOT_STAMPED` on a ring built without
/// stamps.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_ring_set_ordering_mode(handle: subetha_handle, mode: u32) -> i32 {
    with_ring(handle, |r| {
        let mode = match ordering_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        match r.ring.set_ordering_mode(mode) {
            Ok(()) => SUBETHA_OK,
            Err(e) => ring_code(e),
        }
    })
}

/// Under `SUBETHA_ORDERING_MERGE_STRICT` an idle producer refreshes its
/// watermark now and then so the merge does not wait on it; a producer
/// that pushes steadily needs no refresh. `SUBETHA_E_RING_NOT_STAMPED` on
/// a ring built without stamps.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_ring_refresh_watermark(handle: subetha_handle, producer_id: u32) -> i32 {
    with_ring(handle, |r| match r.ring.refresh_watermark(producer_id as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => ring_code(e),
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the ring
/// and parks again unless it finds what it waited for. For a caller that
/// changed something a waiter would want to see.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_ring_wake_all(handle: subetha_handle) -> i32 {
    with_ring(handle, |r| {
        r.consumer_waker.wake_all();
        r.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_read_stats(handle: subetha_handle, out: *mut subetha_ring_stats) -> i32 {
    with_ring(handle, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = r.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove every file a file-backed ring under `path_prefix` names,
/// including the two waker files, so no later process attaches to it. Live
/// handles keep their mappings until destroyed; on Windows a mapped file
/// cannot be removed and that refusal is counted. `max_producers` is the
/// creation hint, the floor for how many per-producer backings are
/// looked for. The counts land in `report` when it is not null. Returns
/// `SUBETHA_E_RING_IO` when any removal was refused, with the first refusal
/// named in the detail; a missing file is not a refusal.
///
/// # Safety
/// `path_prefix` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_unlink(
    path_prefix: *const c_char,
    max_producers: u32,
    report: *mut subetha_unlink_report,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let prefix = match unsafe { text(path_prefix, "path_prefix") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = AdaptiveRing::unlink(prefix, max_producers as usize);
        for suffix in [".cwaker.bin", ".pwaker.bin"] {
            found.remove(with_suffix(prefix, suffix));
        }
        for path in subetha_cxc::cross_process_notifier::NotifyRecordHandle::file_paths_under(prefix) {
            found.remove(path);
        }
        unsafe { finish_unlink(found, report) }
    })
}

/// Attach a pollable notifier to the ring `ring` names for this process
/// to watch: every push through the ABI in any process on the ring
/// signals it. The new handle is of kind `SUBETHA_KIND_NOTIFIER`;
/// `subetha_notifier_native` hands out its file descriptor or event
/// `HANDLE`, `subetha_notifier_drain` clears it, and destroying the handle
/// detaches it. Beside a file-backed ring the notifier is
/// `<prefix>.notify.<index>` on Unix; the record every process reads is
/// `<prefix>.notify.bin`, removed by `subetha_ring_unlink`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_notifier(ring: subetha_handle, out: *mut subetha_handle) -> i32 {
    with_ring(ring, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match r.notifiers.attach() {
            Ok(notifier) => unsafe { issue(Object::Notifier(crate::notifier::NotifierObject::new(notifier)), out) },
            Err(e) => crate::error::notify_code(e),
        }
    })
}

fn hint_from(hint: u32) -> Result<LayoutHint, i32> {
    match hint {
        SUBETHA_LAYOUT_AUTO => Ok(LayoutHint::Auto),
        SUBETHA_LAYOUT_FORCE_INLINE => Ok(LayoutHint::ForceInline),
        SUBETHA_LAYOUT_FORCE_OFFSET => Ok(LayoutHint::ForceOffset),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("layout hint {other} is not a hint"))),
    }
}

/// Send a frame of any length up to the payload region's block size.
/// Up to `SUBETHA_RING_FRAME_INLINE_BUDGET` bytes ride inside the slot; a
/// larger frame lands in the payload region, created on the first spill,
/// and the slot carries its offset. `layout_hint` is one of the
/// `SUBETHA_LAYOUT_` constants; the path taken lands in `out_class` when it
/// is not null. Frames and ordering stamps both claim the slot head, so a
/// ring built with stamps refuses this with `SUBETHA_E_RING_LAYOUT_MISMATCH`.
/// Every shape carries frames the same way.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_class` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_send_frame(
    handle: subetha_handle,
    producer_id: u32,
    data: *const u8,
    len: usize,
    layout_hint: u32,
    out_class: *mut u32,
) -> i32 {
    with_ring(handle, |r| {
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let hint = match hint_from(layout_hint) {
            Ok(h) => h,
            Err(code) => return code,
        };
        match r.send_frame(producer_id as usize, payload, hint) {
            Ok(class) => {
                if !out_class.is_null() {
                    // SAFETY: checked non-null; the caller guarantees it is writable.
                    unsafe { *out_class = class };
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// `subetha_ring_send_frame`, parking until there is room or `timeout_ms`
/// elapses; the waiting contract is `subetha_ring_push_wait`'s.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_class` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_send_frame_wait(
    handle: subetha_handle,
    producer_id: u32,
    data: *const u8,
    len: usize,
    layout_hint: u32,
    timeout_ms: i64,
    out_class: *mut u32,
) -> i32 {
    with_ring(handle, |r| {
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let hint = match hint_from(layout_hint) {
            Ok(h) => h,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match r.send_frame_wait(producer_id as usize, payload, hint, deadline) {
            Ok(class) => {
                if !out_class.is_null() {
                    // SAFETY: checked non-null; the caller guarantees it is writable.
                    unsafe { *out_class = class };
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Receive one frame into `out`, its length into `out_len`; a frame
/// carries its own length, unlike a slot. A frame larger than `cap` is not
/// lost: it is kept for this consumer id, the call returns
/// `SUBETHA_E_BUFFER_TOO_SMALL` with the size needed in `out_len`, and the
/// next call for the same consumer with room yields it. Not available on
/// a ring built with ordering stamps. `SUBETHA_E_RING_EMPTY` when there is
/// nothing to take.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_recv_frame(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_ring(handle, |r| {
        let buf = match unsafe { out_buffer(out, cap, out_len, 0) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match r.recv_frame(consumer_id, buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err((code, needed)) => {
                // SAFETY: as above.
                unsafe { *out_len = needed };
                code
            }
        }
    })
}

/// `subetha_ring_recv_frame`, parking until a frame arrives or `timeout_ms`
/// elapses; the waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_recv_frame_wait(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_ring(handle, |r| {
        let buf = match unsafe { out_buffer(out, cap, out_len, 0) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match r.recv_frame_wait(consumer_id, buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err((code, needed)) => {
                // SAFETY: as above.
                unsafe { *out_len = needed };
                code
            }
        }
    })
}

/// A shared-memory ring's names are released by the OS with the last
/// handle that maps them: on Unix the creating handle unlinks each name
/// when it is destroyed, on Windows the section vanishes when the last
/// handle closes. There is nothing to remove by name, so this reports zero
/// removed and succeeds; it exists so the two locales have the same
/// lifecycle calls.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_unlink_shm(
    name: *const c_char,
    shm_namespace: u32,
    report: *mut subetha_unlink_report,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if let Err(code) = unsafe { text(name, "name") } {
            return code;
        }
        if let Err(code) = namespace(shm_namespace) {
            return code;
        }
        if !report.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *report = subetha_unlink_report::default() };
        }
        crate::error::set_detail("shared-memory names are released with the last handle; nothing to remove by name");
        SUBETHA_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use subetha_cxc::ordering::STAMPED_PAYLOAD_BYTES;
    use subetha_cxc::shared_ring::PAYLOAD_BYTES;
    use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

    #[test]
    fn the_exported_sizes_are_the_rings_own() {
        assert_eq!(SUBETHA_RING_SLOT_BYTES, SPSC_PAYLOAD_BYTES);
        assert_eq!(SUBETHA_RING_PAYLOAD_MAX, PAYLOAD_BYTES);
        assert_eq!(SUBETHA_RING_PAYLOAD_MAX, STAMPED_PAYLOAD_BYTES);
        assert_eq!(SUBETHA_RING_FRAME_INLINE_BUDGET, AdaptiveRing::FRAME_INLINE_BUDGET);
        assert_eq!(SUBETHA_RING_FRAME_DEFAULT_BLOCK, AdaptiveRing::FRAME_DEFAULT_BLOCK_SIZE);
        assert_eq!(SUBETHA_FRAME_INLINE, FrameClass::Inline as u32);
        assert_eq!(SUBETHA_FRAME_OFFSET, FrameClass::Offset as u32);
    }

    #[test]
    fn a_frame_past_the_slot_round_trips_and_a_short_buffer_holds_it() {
        let object = RingObject::anon_for_tests();
        let cid = object.ring.register_consumer().unwrap() as u32;
        let pid = object.ring.register_producer().unwrap();
        let big: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        assert_eq!(object.send_frame(pid, &big, LayoutHint::Auto).unwrap(), SUBETHA_FRAME_OFFSET);
        assert_eq!(object.send_frame(pid, b"small", LayoutHint::Auto).unwrap(), SUBETHA_FRAME_INLINE);

        let mut short = [0u8; 16];
        let (code, needed) = object.recv_frame(cid, &mut short).unwrap_err();
        assert_eq!(code, SUBETHA_E_BUFFER_TOO_SMALL);
        assert_eq!(needed, big.len());
        let mut room = vec![0u8; 4096];
        assert_eq!(object.recv_frame(cid, &mut room).unwrap(), big.len(), "the held frame comes first");
        assert_eq!(&room[..big.len()], &big[..]);
        assert_eq!(object.recv_frame(cid, &mut room).unwrap(), 5);
        assert_eq!(&room[..5], b"small");
        assert_eq!(object.recv_frame(cid, &mut room).unwrap_err().0, crate::error::SUBETHA_E_RING_EMPTY);
    }

    #[test]
    fn a_notifier_is_signaled_by_a_push_and_quiet_after_a_drain() {
        let object = RingObject::anon_for_tests();
        let pid = object.ring.register_producer().unwrap();
        let notifier = object.notifiers.attach().unwrap();
        assert!(!notifier.wait(20), "nothing pushed yet");
        assert_eq!(object.try_push(pid, b"x"), SUBETHA_OK);
        assert!(notifier.wait(1000), "the push signaled the notifier");
        notifier.drain();
        assert!(!notifier.is_signaled());
        drop(notifier);
        assert_eq!(object.notifiers.attached(), 0);
        assert_eq!(object.try_push(pid, b"y"), SUBETHA_OK, "a push with nothing attached is a push");
    }

    #[test]
    fn a_push_wakes_a_parked_pop_and_a_timeout_is_reported() {
        let object = RingObject::anon_for_tests();
        let cid = object.ring.register_consumer().unwrap();
        let pid = object.ring.register_producer().unwrap();
        let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
        let deadline = Some(Instant::now() + Duration::from_millis(30));
        assert_eq!(object.pop_wait(cid, &mut buf, deadline).unwrap_err(), SUBETHA_E_TIMEOUT);

        let object = Arc::new(object);
        let consumer = {
            let object = Arc::clone(&object);
            std::thread::spawn(move || {
                let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
                let n = object.pop_wait(cid, &mut buf, None).unwrap();
                buf[..n].to_vec()
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(object.try_push(pid, b"hello"), SUBETHA_OK);
        let got = consumer.join().unwrap();
        assert_eq!(got.len(), SUBETHA_RING_SLOT_BYTES, "a pop yields the whole slot");
        assert_eq!(&got[..5], b"hello");
        assert!(got[5..].iter().all(|b| *b == 0), "the slot past the payload is zero");
    }

    #[test]
    fn a_destroy_interrupt_releases_a_waiter_with_its_own_code() {
        let object = Arc::new(RingObject::anon_for_tests());
        let cid = object.ring.register_consumer().unwrap();
        let waiter = {
            let object = Arc::clone(&object);
            std::thread::spawn(move || {
                let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
                object.pop_wait(cid, &mut buf, None)
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        object.interrupt();
        assert_eq!(waiter.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }

    /// Managed mode takes the default cadence when the caller names
    /// none, and strict mode ignores the field entirely.
    ///
    /// Zero is "use the default" rather than a refusal: the cadence is
    /// the added round-trip latency of managed mode, and a caller who
    /// has no opinion about that number is better served by a measured
    /// one than by an error.
    #[test]
    fn managed_mode_takes_a_default_interval_and_strict_needs_none() {
        let ring = AdaptiveRing::create_anon(1, 1, 64).unwrap();
        let defaulted = RingObject::build(
            ring,
            Locale::Anon,
            subetha_ring_options { mode: SUBETHA_MODE_MANAGED, max_waiters: 0, scan_interval_us: 0, ..Default::default() },
        )
        .expect("zero takes the default rather than being refused");
        assert_eq!(defaulted.stats().mode, SUBETHA_MODE_MANAGED);
        assert!(
            defaulted.sidecar.lock().is_some(),
            "a defaulted cadence still attaches the sidecar"
        );
        drop(defaulted);
        let ring = AdaptiveRing::create_anon(1, 1, 64).unwrap();
        let managed = RingObject::build(
            ring,
            Locale::Anon,
            subetha_ring_options { mode: SUBETHA_MODE_MANAGED, max_waiters: 0, scan_interval_us: 1_000, ..Default::default() },
        )
        .unwrap();
        assert_eq!(managed.stats().mode, SUBETHA_MODE_MANAGED);
        assert!(managed.sidecar.lock().is_some(), "managed mode attaches the sidecar");
        drop(managed);
        let strict = RingObject::anon_for_tests();
        assert!(strict.sidecar.lock().is_none(), "strict mode starts nothing");
    }
}
