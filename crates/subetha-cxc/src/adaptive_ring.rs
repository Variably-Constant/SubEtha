//! `AdaptiveRing` - shape-morphing ring with a pinned-handle layer.
//!
//! Single typed ring primitive that morphs its protocol shape at
//! runtime based on observed peer counts, plus a pinned-handle
//! layer that drops to near-native primitive speed once the
//! shape stabilises.
//!
//! # Two execution paths
//!
//! - [`AdaptiveRing::try_send`] / [`AdaptiveRing::try_recv`] do
//!   the full atomic dispatch: one Acquire load on the shape tag,
//!   one branch to the matching backend, then the backend's native
//!   op. Cost ~3-5 ns above the underlying primitive. Used when
//!   the shape is uncertain or the caller does not want to manage
//!   a pin lifetime.
//! - [`AdaptiveRing::pin_current_shape`] returns a
//!   [`PinnedRing<'_>`] handle that exposes the current backend
//!   directly. Hot-loop cost matches the underlying primitive
//!   ([`SpscRingCore`], [`SharedRingMpsc`](crate::SharedRingMpsc),
//!   [`SharedRingMpmc`](crate::SharedRingMpmc), or [`SharedRing`])
//!   plus one Acquire load when the caller calls
//!   [`PinnedRing::is_still_valid`].
//!
//! # Morph trigger
//!
//! AUTOMATIC by default: every [`AdaptiveRing::register_producer`] /
//! [`AdaptiveRing::register_consumer`] / unregister re-morphs the
//! shape to the live peer counts (read from the shared peer
//! directory, so registrations in OTHER processes propagate through
//! the topology epoch the hot paths poll), and registration past
//! the construction sizing GROWS the per-producer backings on
//! demand. An explicit [`AdaptiveRing::morph_to`] (or
//! [`AdaptiveRing::pin_shape`]) is the user override that pins the
//! shape; a declared [`AdaptiveRing::with_contract`] ceiling is the
//! only thing that makes registration fallible. Every morph bumps a
//! generation counter that invalidates outstanding pins; pin
//! holders see [`PinnedRing::is_still_valid`] return `false` and
//! re-acquire through the adaptive layer.

use std::cell::Cell;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use arc_swap::ArcSwap;

use crate::frame_ring::{FrameClass, LayoutHint};
use crate::frame_region::FrameRegion;
use crate::peer_directory::{
    PeerDirectory, CONSUMER_SLOT_CEILING, OWNER_NONE,
};

use crate::ordering::{
    default_stamp_kind, ordering_region_size, stamp_now, OrderingMode,
    OrderingRegion, StampKind, STAMPED_PAYLOAD_BYTES, STAMP_BYTES,
};
use crate::qos_policy::Ordering as QosOrdering;
use crate::shared_ring::{RingError, SharedRing, PAYLOAD_BYTES};
use crate::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

/// Grace window (in drainer epochs) before a silent merge drainer
/// becomes preemptible. The sidecar ticks one epoch per scan, so
/// the default tolerates three missed scans.
pub const DRAINER_GRACE_EPOCHS: u64 = 3;

/// The four ring shapes this primitive can host. Stored in the
/// shape tag as the discriminant `u8`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingShape {
    /// 1 producer + 1 consumer, Lamport SPSC core. Cheapest shape.
    Spsc = 0,
    /// N producers + 1 consumer, composed N Lamport SPSC rings.
    Mpsc = 1,
    /// N producers + M consumers, composed N x M Lamport grid.
    Mpmc = 2,
    /// Vyukov MPMC override; preserves global FIFO across producers.
    Vyukov = 3,
}

impl RingShape {
    fn from_u8(tag: u8) -> Self {
        match tag {
            0 => Self::Spsc,
            1 => Self::Mpsc,
            2 => Self::Mpmc,
            3 => Self::Vyukov,
            _ => panic!("AdaptiveRing shape_tag corrupted: {tag}"),
        }
    }
}

/// Shape-morphing ring with all four backing protocols pre-
/// allocated so morphs do not allocate on the hot path.
///
/// **Caller contract on construction**: `max_producers` and
/// `max_consumers` are SIZING HINTS - the per-producer backings
/// pre-allocated up front. Registration past them GROWS the ring
/// on demand (new backings, published through the shared peer
/// directory) and never fails unless the caller declared a
/// [`with_contract`](AdaptiveRing::with_contract) ceiling - the
/// explicit pin is the only source of `TooMany*` errors. Growth
/// happens on the registration slow path; steady-state ops pay one
/// relaxed epoch load.
/// Sentinel for "no stale shape pending" in `stale_shape_tag`.
const STALE_NONE: u8 = u8::MAX;

pub struct AdaptiveRing {
    /// Current shape; one Acquire load per dispatched op.
    shape_tag: AtomicU8,

    /// The previous shape whose backing may still hold a backlog
    /// after a morph. Producers never touch it again (they follow
    /// `shape_tag`); the consumer's pop path drains it FIRST (the
    /// stale walk) so a morph never moves data and never needs
    /// target capacity. Stays set until the NEXT morph (which
    /// requires it drained), giving producer pushes that straddled
    /// the tag flip a wide grace window to land somewhere the
    /// consumer still looks. `STALE_NONE` = nothing pending.
    stale_shape_tag: AtomicU8,

    /// Bumped on every successful morph. Pinned handles capture
    /// this value at pin time and compare on `is_still_valid`.
    pin_generation: AtomicU64,

    /// Shared payload region for the self-describing frame path
    /// ([`send_frame`](Self::send_frame) / [`recv_frame`](Self::recv_frame)).
    /// Records too large to inline in a ring slot spill here as
    /// concurrently-allocated blocks; the descriptor in the slot then
    /// carries the block index. Lazily created on the first oversized
    /// frame so rings that never send large payloads pay nothing.
    /// One region serves every shape (SPSC / MPSC / MPMC / Vyukov)
    /// because its allocator is multi-producer / multi-consumer safe.
    frame_region: OnceLock<Arc<FrameRegion>>,

    /// SPSC backing: one Lamport SPSC ring.
    spsc: Arc<SpscRingCore>,

    /// MPSC backing: factory + producer/consumer handles for the
    /// N-producer single-consumer composed shape.
    mpsc: Arc<MpscBacking>,

    /// MPMC backing: factory + producer/consumer handles for the
    /// N x M composed grid.
    mpmc: Arc<MpmcBacking>,

    /// Vyukov MPMC backing (global-FIFO override).
    vyukov: Arc<SharedRing>,

    /// Sizing HINTS captured at construction: how many per-producer
    /// backings are pre-allocated up front. NOT ceilings - the ring
    /// grows past them on demand. A ceiling exists only when the
    /// caller declares one via [`with_contract`](Self::with_contract).
    max_producers: usize,
    max_consumers: usize,

    /// Per-sub-ring slot capacity, kept for on-demand growth
    /// (grown backings are created at the same capacity).
    capacity: usize,

    /// The shared peer directory: cross-process slot claims, ring
    /// publication, MPMC ring ownership, and the topology epoch the
    /// hot paths poll.
    directory: Arc<PeerDirectory>,

    /// Last directory epoch this process synced its arrays + shape
    /// to. `u64::MAX` = never synced (first op syncs).
    synced_epoch: AtomicU64,

    /// Serialises in-process growth (file creation + array swap).
    grow_lock: parking_lot::Mutex<()>,

    /// Whether the composed shape auto-morphs to the active peer counts
    /// on every register / unregister (the default). Cleared by
    /// [`pin_shape`](Self::pin_shape) or an explicit
    /// [`morph_to`](Self::morph_to) when the caller commits to a fixed
    /// shape - the only cases where the automatic reshape is suppressed.
    shape_auto: AtomicBool,

    /// Declared ring contract - the user override. `None` (the
    /// default) means UNBOUNDED: registration never fails, peers grow
    /// the ring on demand. Set via
    /// [`with_contract`](Self::with_contract); its ceilings are the
    /// only source of `TooMany*` errors. Read at attach time
    /// ([`register_producer`](Self::register_producer)) and by policies
    /// as a feasible-region filter; never on the hot path.
    contract: Option<crate::ring_contract::RingContract>,

    /// Ordering substrate, present only on rings constructed via
    /// [`with_ordering_stamps`](Self::with_ordering_stamps). Fixed
    /// at construction: a runtime stamping toggle would change slot
    /// interpretation under in-flight unstamped items. The MERGE
    /// flag inside the region stays runtime-dynamic because stamps
    /// are always present once this is `Some`.
    ordering: Option<Arc<OrderingState>>,

    /// Where the ring backings live; lets `with_ordering_stamps`
    /// place (or open) the ordering region at the matching locale.
    backing_id: BackingId,

    /// Sidecar handshake + observation ring (inversion events ride
    /// these to the sidecar's drain).
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for AdaptiveRing {}
unsafe impl Sync for AdaptiveRing {}

impl subetha_sidecar::AdaptiveInstance for AdaptiveRing {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

/// Locale identity captured at construction so the ordering region
/// can be created (or opened) next to the ring backings.
enum BackingId {
    Anon,
    File { prefix: std::path::PathBuf, created: bool },
    Shm { prefix: String },
}

/// Ordering state attached to a stamped ring: the shared region
/// plus process-local per-consumer inversion bookkeeping.
struct OrderingState {
    region: OrderingRegion,
    /// Per-consumer last-popped stamp + the mode it was popped
    /// under. Cache-line padded so partitioned MPMC consumers do
    /// not false-share.
    seen: Vec<SeenLine>,
}

#[repr(align(64))]
struct SeenLine {
    stamp: AtomicU64,
    mode_tag: AtomicU32,
    /// Last drainer-lease generation this consumer verified its
    /// lease at. Per-pop verification is one load of the region's
    /// quiet generation line compared against this consumer-local
    /// value; only a change runs the full lease handshake on the
    /// stamp-hot header line. `u64::MAX` = never verified.
    lease_gen: AtomicU64,
}

impl SeenLine {
    fn new() -> Self {
        Self {
            stamp: AtomicU64::new(0),
            mode_tag: AtomicU32::new(OrderingMode::Unordered as u32),
            lease_gen: AtomicU64::new(u64::MAX),
        }
    }
}

/// Drainer-lease token for this process + consumer slot.
#[inline]
fn drainer_token(consumer_id: usize) -> u64 {
    ((std::process::id() as u64) << 32) | (consumer_id as u64 & 0xFFFF_FFFF)
}

struct MpscBacking {
    /// Per-producer rings behind an `ArcSwap` so producer growth
    /// appends without stopping traffic: one guarded load per op
    /// while unpinned, and pinned handles capture the `Arc` at pin
    /// time (growth bumps the pin generation).
    rings: ArcSwap<Vec<Arc<SpscRingCore>>>,
    next_drain: AtomicUsize,
}

struct MpmcBacking {
    rings: ArcSwap<Vec<Arc<SpscRingCore>>>,
    /// Per-consumer round-robin cursors. Index by consumer_id.
    /// Each entry is cache-line aligned to keep one consumer's
    /// writes from invalidating another consumer's L1 line. Sized
    /// to [`CONSUMER_SLOT_CEILING`] so consumer slots grow / shrink
    /// with no reallocation.
    consumer_cursors: Vec<PaddedCursor>,
}

/// Cache-line-aligned `AtomicUsize` wrapper. Used for MPMC consumer
/// round-robin cursors so per-consumer writes do not pollute the
/// L1 cache lines of sibling consumers. The second field rate-limits
/// that consumer's crash-takeover pid probes.
#[repr(align(64))]
struct PaddedCursor(AtomicUsize, AtomicUsize);

fn consumer_cursor_table() -> Vec<PaddedCursor> {
    (0..CONSUMER_SLOT_CEILING)
        .map(|_| PaddedCursor(AtomicUsize::new(0), AtomicUsize::new(0)))
        .collect()
}

/// Allocate one huge / large page region sized for a ring backing of
/// `bytes`. Linux uses anonymous 2 MB hugepages (`MAP_HUGETLB`);
/// Windows uses a `MEM_LARGE_PAGES` region. Both implement
/// [`RegionOwner`](crate::spsc_ring::RegionOwner), so the ring's
/// `create_in_region` accepts either. Returns `Err` when hugepages are
/// unavailable (no reservation / privilege) so the caller can fall back
/// to a standard backing. Only this allocation is platform-gated; the
/// `create_hugepage` layout that consumes it is shared.
#[cfg(target_os = "linux")]
fn hugepage_region(bytes: usize) -> std::io::Result<crate::hugepages::HugepageRegion> {
    use crate::hugepages::{HugepageRegion, HugepageSize, HUGEPAGE_2MB};
    let pages = bytes.div_ceil(HUGEPAGE_2MB).max(1);
    HugepageRegion::allocate(pages, HugepageSize::Mb2)
}

#[cfg(windows)]
fn hugepage_region(bytes: usize) -> std::io::Result<crate::large_pages::LargePageRegion> {
    use crate::large_pages::{enable_lock_memory_privilege, LargePageRegion};
    // Enabling the privilege is a precondition; `allocate` rounds
    // `bytes` up to a whole number of large pages internally.
    enable_lock_memory_privilege()?;
    LargePageRegion::allocate(bytes)
}

#[cfg(any(target_os = "freebsd", target_os = "macos"))]
fn hugepage_region(bytes: usize) -> std::io::Result<crate::super_pages::SuperPageRegion> {
    // Superpage-backed: FreeBSD `MAP_ALIGNED_SUPER` (a transparent hint
    // with no pre-reserved pool), macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`
    // (the Darwin anonymous-superpage overload). `allocate` rounds `bytes`
    // up to a whole number of 2 MB superpages and returns Err only when the
    // aligned mapping cannot be made (or on Apple Silicon, which has no
    // userspace superpage API), so the caller falls back to `create_anon`.
    crate::super_pages::SuperPageRegion::allocate(bytes)
}

impl AdaptiveRing {
    /// Construct an adaptive ring with all backings pre-allocated.
    ///
    /// `max_producers` and `max_consumers` size the composed
    /// MPSC + MPMC backings; runtime peer registration past these
    /// maxima is rejected. Initial shape is [`RingShape::Spsc`].
    pub fn create_anon(
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(max_producers >= 1, "max_producers must be >= 1");
        assert!(max_consumers >= 1, "max_consumers must be >= 1");

        let spsc = Arc::new(SpscRingCore::create_anon(capacity)?);

        let mpsc_rings: Vec<Arc<SpscRingCore>> = (0..max_producers)
            .map(|_| SpscRingCore::create_anon(capacity).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let mpsc = Arc::new(MpscBacking {
            rings: ArcSwap::from_pointee(mpsc_rings),
            next_drain: AtomicUsize::new(0),
        });

        let mpmc_rings: Vec<Arc<SpscRingCore>> = (0..max_producers)
            .map(|_| SpscRingCore::create_anon(capacity).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let mpmc = Arc::new(MpmcBacking {
            rings: ArcSwap::from_pointee(mpmc_rings),
            consumer_cursors: consumer_cursor_table(),
        });

        let vyukov = Arc::new(SharedRing::create_anon(capacity)?);

        let directory = Arc::new(PeerDirectory::create_anon()?);
        directory.publish_rings(max_producers);

        Ok(Self {
            shape_tag: AtomicU8::new(RingShape::Spsc as u8),
            stale_shape_tag: AtomicU8::new(STALE_NONE),
            pin_generation: AtomicU64::new(0),
            frame_region: OnceLock::new(),
            spsc,
            mpsc,
            mpmc,
            vyukov,
            max_producers,
            max_consumers,
            capacity,
            directory,
            synced_epoch: AtomicU64::new(u64::MAX),
            grow_lock: parking_lot::Mutex::new(()),
            contract: None,
            shape_auto: AtomicBool::new(true),
            ordering: None,
            backing_id: BackingId::Anon,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Hugepage / large-page-backed adaptive ring (opt-in). Every
    /// backing (SPSC, each MPSC + MPMC producer ring, Vyukov) is laid
    /// out in its own huge / large page region instead of standard 4 KB
    /// pages, cutting TLB pressure for large rings: a 16 MB ring fits in
    /// a handful of 2 MB hugepages instead of thousands of 4 KB pages.
    ///
    /// Cross-platform: Linux `MAP_HUGETLB`, Windows `MEM_LARGE_PAGES`,
    /// FreeBSD `MAP_ALIGNED_SUPER`, macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`;
    /// only the per-backing region allocation is platform-gated, the
    /// compose-and-wire logic is shared with `create_anon`.
    ///
    /// Requires a hugepage reservation (Linux `vm.nr_hugepages`) or the
    /// `SeLockMemoryPrivilege` (Windows); FreeBSD and macOS need no
    /// reservation (superpages are a transparent / on-demand hint, macOS
    /// x86_64 only). Returns `Err` when the backing cannot be allocated so
    /// the caller can fall back to `create_anon`.
    #[cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))]
    pub fn create_hugepage(
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(max_producers >= 1, "max_producers must be >= 1");
        assert!(max_consumers >= 1, "max_consumers must be >= 1");

        let spsc_bytes = crate::spsc_ring::spsc_ring_file_size(capacity);
        let vyukov_bytes = crate::shared_ring::ring_file_size(capacity);

        let spsc = Arc::new(SpscRingCore::create_in_region(
            hugepage_region(spsc_bytes)?, capacity)?);

        let mut mpsc_rings = Vec::with_capacity(max_producers);
        for _ in 0..max_producers {
            mpsc_rings.push(Arc::new(SpscRingCore::create_in_region(
                hugepage_region(spsc_bytes)?, capacity)?));
        }
        let mpsc = Arc::new(MpscBacking {
            rings: ArcSwap::from_pointee(mpsc_rings),
            next_drain: AtomicUsize::new(0),
        });

        let mut mpmc_rings = Vec::with_capacity(max_producers);
        for _ in 0..max_producers {
            mpmc_rings.push(Arc::new(SpscRingCore::create_in_region(
                hugepage_region(spsc_bytes)?, capacity)?));
        }
        let mpmc = Arc::new(MpmcBacking {
            rings: ArcSwap::from_pointee(mpmc_rings),
            consumer_cursors: consumer_cursor_table(),
        });

        let vyukov = Arc::new(SharedRing::create_in_region(
            hugepage_region(vyukov_bytes)?, capacity)?);

        let directory = Arc::new(PeerDirectory::create_anon()?);
        directory.publish_rings(max_producers);

        Ok(Self {
            shape_tag: AtomicU8::new(RingShape::Spsc as u8),
            stale_shape_tag: AtomicU8::new(STALE_NONE),
            pin_generation: AtomicU64::new(0),
            frame_region: OnceLock::new(),
            spsc,
            mpsc,
            mpmc,
            vyukov,
            max_producers,
            max_consumers,
            capacity,
            directory,
            synced_epoch: AtomicU64::new(u64::MAX),
            grow_lock: parking_lot::Mutex::new(()),
            contract: None,
            shape_auto: AtomicBool::new(true),
            ordering: None,
            // The hugepage backing is anonymous (no path / name); reuse
            // the Anon id so ordering-region creation stays uniform.
            // Backings grown past the pre-allocated hint use standard
            // anonymous pages (hugepage regions are pre-reserved).
            backing_id: BackingId::Anon,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// File-backed adaptive ring. One file per backing (SPSC,
    /// each MPSC producer ring, each MPMC producer ring, Vyukov),
    /// named `<path_prefix>.{role}.bin` /
    /// `<path_prefix>.mpsc.{i}.bin` / `<path_prefix>.mpmc.{i}.bin`.
    pub fn create(
        path_prefix: impl AsRef<Path>,
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(max_producers >= 1 && max_consumers >= 1);
        let base = path_prefix.as_ref();

        let spsc_path = with_suffix(base, ".spsc.bin");
        let spsc = Arc::new(SpscRingCore::create(&spsc_path, capacity)?);

        let mut mpsc_rings = Vec::with_capacity(max_producers);
        for i in 0..max_producers {
            let p = with_suffix(base, &format!(".mpsc.{i}.bin"));
            mpsc_rings.push(Arc::new(SpscRingCore::create(&p, capacity)?));
        }
        let mpsc = Arc::new(MpscBacking {
            rings: ArcSwap::from_pointee(mpsc_rings),
            next_drain: AtomicUsize::new(0),
        });

        let mut mpmc_rings = Vec::with_capacity(max_producers);
        for i in 0..max_producers {
            let p = with_suffix(base, &format!(".mpmc.{i}.bin"));
            mpmc_rings.push(Arc::new(SpscRingCore::create(&p, capacity)?));
        }
        let mpmc = Arc::new(MpmcBacking {
            rings: ArcSwap::from_pointee(mpmc_rings),
            consumer_cursors: consumer_cursor_table(),
        });

        let vyukov_path = with_suffix(base, ".vyukov.bin");
        let vyukov = Arc::new(SharedRing::create(&vyukov_path, capacity)?);

        let directory = Arc::new(
            PeerDirectory::create(with_suffix(base, ".peers.bin"))?,
        );
        directory.publish_rings(max_producers);

        Ok(Self {
            shape_tag: AtomicU8::new(RingShape::Spsc as u8),
            stale_shape_tag: AtomicU8::new(STALE_NONE),
            pin_generation: AtomicU64::new(0),
            frame_region: OnceLock::new(),
            spsc,
            mpsc,
            mpmc,
            vyukov,
            max_producers,
            max_consumers,
            capacity,
            directory,
            synced_epoch: AtomicU64::new(u64::MAX),
            grow_lock: parking_lot::Mutex::new(()),
            contract: None,
            shape_auto: AtomicBool::new(true),
            ordering: None,
            backing_id: BackingId::File {
                prefix: base.to_path_buf(),
                created: true,
            },
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing file-backed adaptive ring created by
    /// another process via [`AdaptiveRing::create`] with the same
    /// `path_prefix` + sizing. Validates each backing's magic +
    /// capacity; does NOT re-initialize any layout, so in-flight
    /// items in the creator's backings survive the attach.
    ///
    /// The shape tag + pin generation are process-local: each
    /// process morphs / pins its own view. Cross-process callers
    /// coordinate the active shape out-of-band (or follow the
    /// creator's sidecar) and call [`AdaptiveRing::morph_to`] to
    /// the agreed shape before pinning.
    pub fn open(
        path_prefix: impl AsRef<Path>,
        max_producers: usize,
        max_consumers: usize,
        expected_capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(max_producers >= 1 && max_consumers >= 1);
        let base = path_prefix.as_ref();

        // The peer directory is the source of truth for how many
        // per-producer backings exist RIGHT NOW - the creator's
        // hint may have grown since. The caller's count args stay
        // as pre-open floor hints only.
        let directory = Arc::new(
            PeerDirectory::open(with_suffix(base, ".peers.bin"))?,
        );
        let n_rings = directory.published().max(1);

        let spsc_path = with_suffix(base, ".spsc.bin");
        let spsc = Arc::new(SpscRingCore::open(&spsc_path, expected_capacity)?);

        let mut mpsc_rings = Vec::with_capacity(n_rings);
        for i in 0..n_rings {
            let p = with_suffix(base, &format!(".mpsc.{i}.bin"));
            mpsc_rings.push(Arc::new(SpscRingCore::open(&p, expected_capacity)?));
        }
        let mpsc = Arc::new(MpscBacking {
            rings: ArcSwap::from_pointee(mpsc_rings),
            next_drain: AtomicUsize::new(0),
        });

        let mut mpmc_rings = Vec::with_capacity(n_rings);
        for i in 0..n_rings {
            let p = with_suffix(base, &format!(".mpmc.{i}.bin"));
            mpmc_rings.push(Arc::new(SpscRingCore::open(&p, expected_capacity)?));
        }
        let mpmc = Arc::new(MpmcBacking {
            rings: ArcSwap::from_pointee(mpmc_rings),
            consumer_cursors: consumer_cursor_table(),
        });

        let vyukov_path = with_suffix(base, ".vyukov.bin");
        let vyukov = Arc::new(SharedRing::open(&vyukov_path, expected_capacity)?);

        Ok(Self {
            shape_tag: AtomicU8::new(RingShape::Spsc as u8),
            stale_shape_tag: AtomicU8::new(STALE_NONE),
            pin_generation: AtomicU64::new(0),
            frame_region: OnceLock::new(),
            spsc,
            mpsc,
            mpmc,
            vyukov,
            max_producers,
            max_consumers,
            capacity: expected_capacity,
            directory,
            synced_epoch: AtomicU64::new(u64::MAX),
            grow_lock: parking_lot::Mutex::new(()),
            contract: None,
            shape_auto: AtomicBool::new(true),
            ordering: None,
            backing_id: BackingId::File {
                prefix: base.to_path_buf(),
                created: false,
            },
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Construct an AdaptiveRing whose four backings live in named
    /// RAM-resident shared memory regions (the ShmFs locale).
    /// Cross-process visible; never touches the page cache.
    ///
    /// `name_prefix` becomes part of each backing's logical shm
    /// name: `{prefix}_spsc`, `{prefix}_mpsc_{i}`,
    /// `{prefix}_mpmc_{i}`, `{prefix}_vyukov`. The same prefix on
    /// another process resolves to the same shared memory.
    pub fn create_shmfs(
        name_prefix: &str,
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(max_producers >= 1, "max_producers must be >= 1");
        assert!(max_consumers >= 1, "max_consumers must be >= 1");

        let spsc_size = crate::spsc_ring::spsc_ring_file_size(capacity);
        let vyukov_size = crate::shared_ring::ring_file_size(capacity);

        // SPSC backing.
        let spsc_shm = crate::shm_file::ShmFile::create_or_open_named(
            &format!("{name_prefix}_spsc"), spsc_size,
        ).map_err(|_| RingError::PayloadTooLarge)?;
        let spsc = Arc::new(SpscRingCore::create_from_shm(spsc_shm, capacity)?);

        // MPSC backings.
        let mut mpsc_rings = Vec::with_capacity(max_producers);
        for i in 0..max_producers {
            let shm = crate::shm_file::ShmFile::create_or_open_named(
                &format!("{name_prefix}_mpsc_{i}"), spsc_size,
            ).map_err(|_| RingError::PayloadTooLarge)?;
            mpsc_rings.push(Arc::new(SpscRingCore::create_from_shm(shm, capacity)?));
        }
        let mpsc = Arc::new(MpscBacking {
            rings: ArcSwap::from_pointee(mpsc_rings),
            next_drain: AtomicUsize::new(0),
        });

        // MPMC backings (one ring per producer; consumers partition).
        let mut mpmc_rings = Vec::with_capacity(max_producers);
        for i in 0..max_producers {
            let shm = crate::shm_file::ShmFile::create_or_open_named(
                &format!("{name_prefix}_mpmc_{i}"), spsc_size,
            ).map_err(|_| RingError::PayloadTooLarge)?;
            mpmc_rings.push(Arc::new(SpscRingCore::create_from_shm(shm, capacity)?));
        }
        let mpmc = Arc::new(MpmcBacking {
            rings: ArcSwap::from_pointee(mpmc_rings),
            consumer_cursors: consumer_cursor_table(),
        });

        // Vyukov backing.
        let vyukov_shm = crate::shm_file::ShmFile::create_or_open_named(
            &format!("{name_prefix}_vyukov"), vyukov_size,
        ).map_err(|_| RingError::PayloadTooLarge)?;
        let vyukov = Arc::new(SharedRing::create_from_shm(vyukov_shm, capacity)?);

        let directory = Arc::new(PeerDirectory::create_or_open_shm(
            &format!("{name_prefix}_peers"),
        )?);
        directory.publish_rings(max_producers);

        Ok(Self {
            shape_tag: AtomicU8::new(RingShape::Spsc as u8),
            stale_shape_tag: AtomicU8::new(STALE_NONE),
            pin_generation: AtomicU64::new(0),
            frame_region: OnceLock::new(),
            spsc, mpsc, mpmc, vyukov,
            max_producers, max_consumers,
            capacity,
            directory,
            synced_epoch: AtomicU64::new(u64::MAX),
            grow_lock: parking_lot::Mutex::new(()),
            contract: None,
            shape_auto: AtomicBool::new(true),
            ordering: None,
            backing_id: BackingId::Shm { prefix: name_prefix.to_owned() },
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Attach the ordering substrate: every subsequent push carries
    /// an 8-byte stamp in slot bytes `[0..8)` and the payload cap
    /// drops to [`STAMPED_PAYLOAD_BYTES`] (56 - the same 8 bytes
    /// Vyukov spends on its per-slot sequence atom). Pops through
    /// [`try_recv`](Self::try_recv) (and the pinned
    /// [`ordered_try_pop`](PinnedRing::ordered_try_pop)) strip the
    /// stamp and hand back payload bytes only.
    ///
    /// Stamping is FIXED at construction - call this before any
    /// traffic. The merge flag inside the ordering region stays
    /// runtime-dynamic via
    /// [`set_ordering_mode`](Self::set_ordering_mode).
    ///
    /// Stamp-kind selection: invariant-TSC `rdtsc` when the CPUID
    /// probe passes, the shared counter on x86 without an invariant
    /// TSC, the monotonic clock on non-x86 hosts. Rings opened with
    /// [`AdaptiveRing::open`] adopt the creator's stamp kind from
    /// the region header (validated, never re-initialised).
    ///
    /// A stamped ring never morphs to [`RingShape::Vyukov`]: the
    /// stamped 64-byte slot layout does not fit Vyukov's 56-byte
    /// slots, and the `GlobalFifo` declaration on a stamped ring is
    /// served by the merge flag instead of the Vyukov morph.
    pub fn with_ordering_stamps(self) -> Result<Self, RingError> {
        self.with_ordering_stamps_impl(None)
    }

    /// As [`with_ordering_stamps`](Self::with_ordering_stamps) with
    /// an explicit stamp kind. `StampKind::SharedCounter` is the
    /// exactness opt-in: stamps form a total order at the price of
    /// one contended `fetch_add` per push. Opening an existing
    /// region with a kind that does not match the creator's returns
    /// [`RingError::LayoutMismatch`].
    pub fn with_ordering_stamps_kind(self, kind: StampKind) -> Result<Self, RingError> {
        self.with_ordering_stamps_impl(Some(kind))
    }

    fn with_ordering_stamps_impl(
        mut self,
        kind: Option<StampKind>,
    ) -> Result<Self, RingError> {
        if self.ordering.is_some() {
            return Ok(self);
        }
        if self.current_shape() == RingShape::Vyukov {
            return Err(RingError::LayoutMismatch);
        }
        // Stamp lines are sized to the substrate producer-slot
        // ceiling, not the construction hint, so producer growth
        // never needs an ordering-region resize. Untouched lines
        // stay as never-faulted pages; every hot operation indexes
        // by producer id and the merge gates scan only the
        // published slot count.
        let lines = crate::peer_directory::PRODUCER_SLOT_CEILING;
        let region = match &self.backing_id {
            BackingId::Anon => OrderingRegion::create_anon(
                lines,
                kind.unwrap_or_else(default_stamp_kind),
            )?,
            BackingId::File { prefix, created } => {
                let path = with_suffix(prefix, ".ordering.bin");
                if *created {
                    OrderingRegion::create(
                        &path,
                        lines,
                        kind.unwrap_or_else(default_stamp_kind),
                    )?
                } else {
                    let region = OrderingRegion::open(&path, lines)?;
                    if let Some(k) = kind
                        && region.stamp_kind() != k
                    {
                        return Err(RingError::LayoutMismatch);
                    }
                    region
                }
            }
            BackingId::Shm { prefix } => {
                let size = ordering_region_size(lines);
                let shm = crate::shm_file::ShmFile::create_or_open_named(
                    &format!("{prefix}_ordering"),
                    size,
                ).map_err(|e| RingError::IoError(e.kind()))?;
                OrderingRegion::create_shm(
                    shm,
                    lines,
                    kind.unwrap_or_else(default_stamp_kind),
                )?
            }
        };
        let seen = (0..CONSUMER_SLOT_CEILING).map(|_| SeenLine::new()).collect();
        self.ordering = Some(Arc::new(OrderingState { region, seen }));
        Ok(self)
    }

    /// Current shape.
    pub fn current_shape(&self) -> RingShape {
        RingShape::from_u8(self.shape_tag.load(Ordering::Acquire))
    }

    /// Peek the next slot of the internal SPSC backing without
    /// copying or releasing. Returns `None` when the active shape
    /// is not SPSC OR when the ring is empty. Used by zero-copy
    /// egress paths (e.g. the bridge primitives' `write_all` flow)
    /// when the active shape supports peek-direct.
    ///
    /// The returned [`PeekedSpscSlot`] derefs to `&[u8]` pointing
    /// INTO the SPSC backing's mmap region. Caller passes that
    /// slice straight to downstream consumers, then calls
    /// [`PeekedSpscSlot::confirm`] to release the slot.
    pub fn peek_spsc_slot(&self) -> Option<PeekedSpscSlot<'_>> {
        if self.current_shape() != RingShape::Spsc {
            return None;
        }
        self.spsc.peek_slot().map(|inner| PeekedSpscSlot { inner })
    }

    /// Shape-aware emptiness check across every backing this ring
    /// currently uses.
    ///
    /// - SPSC: the single SPSC backing's head==tail.
    /// - MPSC: every per-producer SPSC sub-ring is empty.
    /// - MPMC: every per-producer SPSC sub-ring in the grid is
    ///   empty (cross-consumer claims are committed by sub-ring
    ///   pops, so an empty grid means every slot has been
    ///   consumed).
    /// - Vyukov: producer_seq == consumer_seq.
    ///
    /// Used by capacity-morph wrappers to decide whether a stale
    /// backing can be dropped. Conservative: a value returning
    /// `true` is guaranteed empty at the moment of observation
    /// across all sub-rings; concurrent producers writing into the
    /// active shape during the check cannot affect a stale-only
    /// caller because producers only target whichever Arc the
    /// wrapper's ArcSwap currently points at.
    pub fn is_empty(&self) -> bool {
        if let Some(stale) = self.stale_shape()
            && !self.backing_is_empty(stale)
        {
            return false;
        }
        self.backing_is_empty(self.current_shape())
    }

    /// Shape-aware approximate item count across every backing
    /// currently in use (sum for composed shapes; single ring for
    /// SPSC / Vyukov). Used by sidecar policies to compute fill
    /// ratio and decide whether to grow / shrink capacity.
    pub fn approx_len(&self) -> usize {
        let stale_len = match self.stale_shape() {
            Some(stale) if stale != self.current_shape() => {
                self.backing_approx_len(stale)
            }
            _ => 0,
        };
        stale_len + self.backing_approx_len(self.current_shape())
    }

    fn backing_approx_len(&self, shape: RingShape) -> usize {
        match shape {
            RingShape::Spsc => self.spsc.approx_len(),
            RingShape::Mpsc => self.mpsc.rings.load().iter().map(|r| r.approx_len()).sum(),
            RingShape::Mpmc => self.mpmc.rings.load().iter().map(|r| r.approx_len()).sum(),
            RingShape::Vyukov => self.vyukov.approx_len(),
        }
    }

    /// Capacity of a single underlying sub-ring (per-producer slot
    /// count). Composed shapes have N or N*M such sub-rings; the
    /// total slot inventory is `sub_ring_capacity() * n_sub_rings`.
    /// For SPSC / Vyukov this is the ring's full capacity.
    pub fn sub_ring_capacity(&self) -> usize {
        match self.current_shape() {
            RingShape::Spsc => self.spsc.capacity(),
            RingShape::Mpsc => self.mpsc.rings.load().first().map(|r| r.capacity()).unwrap_or(0),
            RingShape::Mpmc => self.mpmc.rings.load().first().map(|r| r.capacity()).unwrap_or(0),
            RingShape::Vyukov => self.vyukov.capacity(),
        }
    }

    /// Total slot inventory across every sub-ring this AdaptiveRing
    /// currently owns. For SPSC / Vyukov this is the same as
    /// `sub_ring_capacity()`. For MPSC / MPMC it is
    /// `sub_ring_capacity() * n_sub_rings`.
    pub fn total_slot_capacity(&self) -> usize {
        match self.current_shape() {
            RingShape::Spsc => self.spsc.capacity(),
            RingShape::Mpsc => self.mpsc.rings.load().iter().map(|r| r.capacity()).sum(),
            RingShape::Mpmc => self.mpmc.rings.load().iter().map(|r| r.capacity()).sum(),
            RingShape::Vyukov => self.vyukov.capacity(),
        }
    }

    /// Current pin generation. Pinned handles capture this at pin
    /// time; a non-equal current value means the pin is stale.
    pub fn pin_generation(&self) -> u64 {
        self.pin_generation.load(Ordering::Acquire)
    }

    /// Number of per-producer backings this ring pre-allocated at
    /// construction. A HINT, not a ceiling: registration past it
    /// grows the backings on demand.
    pub fn max_producers(&self) -> usize { self.max_producers }

    /// Consumer-count hint captured at construction. Consumer slots
    /// are claimed dynamically up to the substrate ceiling.
    pub fn max_consumers(&self) -> usize { self.max_consumers }

    /// Per-producer backings currently published (pre-allocated +
    /// grown), shared across every attached process.
    pub fn published_producers(&self) -> usize {
        self.directory.published()
    }

    /// The ring's effective contract. UNBOUNDED unless the caller
    /// declared one via [`with_contract`](Self::with_contract) - a
    /// declared contract is the ONLY thing that makes registration
    /// fallible; the default grows on demand.
    pub fn contract(&self) -> crate::ring_contract::RingContract {
        self.contract.unwrap_or_else(crate::ring_contract::RingContract::unbounded)
    }

    /// Declare an explicit ring contract (builder; consumes self,
    /// like [`with_ordering_stamps`](Self::with_ordering_stamps)).
    /// The contract's count bounds become the attach-time admission
    /// check and its ordering / capacity constraints become the
    /// feasible-region filter a policy consults.
    pub fn with_contract(mut self, contract: crate::ring_contract::RingContract) -> Self {
        self.contract = Some(contract);
        self
    }

    /// Map a policy's proposed shape to the nearest contract-legal one,
    /// so an auto-morph cannot violate the declared ordering contract
    /// by construction. A `Fifo` contract forbids the partitioned
    /// per-producer-lane shapes ([`Mpsc`](RingShape::Mpsc),
    /// [`Mpmc`](RingShape::Mpmc), which interleave producers); the
    /// order-preserving substitute is [`Vyukov`](RingShape::Vyukov) on
    /// an unstamped ring. A stamped ring keeps the proposed shape - its
    /// global order is served by the `MergeStrict` flag, not a Vyukov
    /// morph (whose 56-byte slots do not fit the stamped 64-byte
    /// layout). Under the default (unbounded) contract this is the
    /// identity, so non-declaring rings are unaffected.
    pub fn contract_filtered_shape(&self, target: RingShape) -> RingShape {
        if self.contract().permits_shape(target) {
            return target;
        }
        if self.ordering.is_none() {
            RingShape::Vyukov
        } else {
            target
        }
    }

    /// Re-morph the composed shape to the current active peer counts
    /// (read from the shared directory, so registrations in OTHER
    /// processes drive this process's shape too). Called from every
    /// register / unregister and from the topology sync slow path -
    /// no background thread required. Suppressed when the caller
    /// pinned the shape ([`pin_shape`](Self::pin_shape) or an
    /// explicit [`morph_to`](Self::morph_to)), and never disturbs a
    /// `Vyukov` shape - that is an ordering decision, not a count
    /// decision. Returns `false` only when a needed morph is blocked
    /// on an undrained stale backlog (the caller leaves the epoch
    /// unsynced so the next op retries).
    fn reshape_for_counts(&self) -> bool {
        if !self.shape_auto.load(Ordering::Relaxed)
            || self.current_shape() == RingShape::Vyukov
        {
            return true;
        }
        let p = self.directory.active_producers();
        let c = self.directory.active_consumers();
        if let Some(target) = DefaultRingShapePolicy::target_shape(p, c)
            && target != self.current_shape()
        {
            return self.morph_shape(self.contract_filtered_shape(target)).is_ok();
        }
        true
    }

    /// Pin the composed shape: stop the automatic reshape-on-register so
    /// the ring holds whatever shape it currently has. The user override
    /// for callers that want a fixed shape. An explicit
    /// [`morph_to`](Self::morph_to) pins implicitly.
    pub fn pin_shape(&self) {
        self.shape_auto.store(false, Ordering::Relaxed);
    }

    /// Resume the automatic shape (undo [`pin_shape`](Self::pin_shape)
    /// / an explicit morph) and re-track the live peer counts. Unlike
    /// the automatic reshape - which never disturbs a Vyukov shape -
    /// this explicit resume DOES morph a Vyukov ring back to the
    /// counts-based composed shape (that is what resuming means).
    pub fn resume_auto_shape(&self) {
        self.shape_auto.store(true, Ordering::Relaxed);
        let p = self.directory.active_producers();
        let c = self.directory.active_consumers();
        if let Some(target) = DefaultRingShapePolicy::target_shape(p, c) {
            self.morph_shape(self.contract_filtered_shape(target)).ok();
        }
    }

    /// Whether the composed shape auto-morphs to the active peer counts
    /// (the default). `false` after [`pin_shape`](Self::pin_shape) or an
    /// explicit [`morph_to`](Self::morph_to).
    pub fn shape_is_auto(&self) -> bool {
        self.shape_auto.load(Ordering::Relaxed)
    }

    /// One relaxed load on the shared topology epoch; on change, run
    /// the sync slow path (grow local arrays, reshape). Called at the
    /// top of every adaptive-path op so cross-process registrations
    /// propagate with no background thread.
    #[inline]
    fn ensure_synced(&self) {
        let e = self.directory.epoch();
        if e != self.synced_epoch.load(Ordering::Relaxed) {
            self.sync_topology(e);
        }
    }

    #[cold]
    fn sync_topology(&self, epoch: u64) {
        self.directory.reap_dead_peers();
        let arrays_ok = self.refresh_local_arrays().is_ok();
        let shape_ok = self.reshape_for_counts();
        if arrays_ok && shape_ok {
            // Reaping / a racing registrant may have advanced the
            // epoch since `epoch` was read; store the STALE value so
            // the next op re-syncs to the newer state.
            self.synced_epoch.store(epoch, Ordering::Relaxed);
        }
    }

    /// Open (or, for the grower, create) local handles for every
    /// published per-producer backing this process has not mapped
    /// yet. Growth bumps the pin generation so outstanding pins
    /// re-acquire and see the new backings.
    fn refresh_local_arrays(&self) -> Result<(), RingError> {
        let published = self.directory.published();
        if self.mpsc.rings.load().len() >= published {
            return Ok(());
        }
        let _guard = self.grow_lock.lock();
        let cur_mpsc = self.mpsc.rings.load_full();
        let cur_mpmc = self.mpmc.rings.load_full();
        if cur_mpsc.len() >= published {
            return Ok(());
        }
        let mut mpsc_new = (*cur_mpsc).clone();
        let mut mpmc_new = (*cur_mpmc).clone();
        for i in cur_mpsc.len()..published {
            let (a, b) = self.open_ring_backing(i)?;
            mpsc_new.push(a);
            mpmc_new.push(b);
        }
        self.mpsc.rings.store(Arc::new(mpsc_new));
        self.mpmc.rings.store(Arc::new(mpmc_new));
        self.pin_generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Open the published backing pair for producer slot `i` created
    /// by another process (file / shm locales; anonymous backings are
    /// single-instance so their published set is always local).
    fn open_ring_backing(
        &self,
        i: usize,
    ) -> Result<(Arc<SpscRingCore>, Arc<SpscRingCore>), RingError> {
        match &self.backing_id {
            BackingId::File { prefix, .. } => {
                let a = SpscRingCore::open(
                    with_suffix(prefix, &format!(".mpsc.{i}.bin")), self.capacity)?;
                let b = SpscRingCore::open(
                    with_suffix(prefix, &format!(".mpmc.{i}.bin")), self.capacity)?;
                Ok((Arc::new(a), Arc::new(b)))
            }
            BackingId::Shm { prefix } => {
                let size = crate::spsc_ring::spsc_ring_file_size(self.capacity);
                let shm_a = crate::shm_file::ShmFile::create_or_open_named(
                    &format!("{prefix}_mpsc_{i}"), size,
                ).map_err(|e| RingError::IoError(e.kind()))?;
                let shm_b = crate::shm_file::ShmFile::create_or_open_named(
                    &format!("{prefix}_mpmc_{i}"), size,
                ).map_err(|e| RingError::IoError(e.kind()))?;
                let a = SpscRingCore::create_from_shm(shm_a, self.capacity)?;
                let b = SpscRingCore::create_from_shm(shm_b, self.capacity)?;
                Ok((Arc::new(a), Arc::new(b)))
            }
            // Anonymous backings cannot be published by a peer: any
            // growth on this instance created them locally already.
            BackingId::Anon => Err(RingError::LayoutMismatch),
        }
    }

    /// Create the backing pair for a NEW producer slot `i` (the
    /// grower path; this process claimed the slot, so it is the
    /// single creator by construction).
    fn create_ring_backing(
        &self,
        i: usize,
    ) -> Result<(Arc<SpscRingCore>, Arc<SpscRingCore>), RingError> {
        match &self.backing_id {
            BackingId::Anon => {
                let a = SpscRingCore::create_anon(self.capacity)?;
                let b = SpscRingCore::create_anon(self.capacity)?;
                Ok((Arc::new(a), Arc::new(b)))
            }
            BackingId::File { prefix, .. } => {
                let a = SpscRingCore::create(
                    with_suffix(prefix, &format!(".mpsc.{i}.bin")), self.capacity)?;
                let b = SpscRingCore::create(
                    with_suffix(prefix, &format!(".mpmc.{i}.bin")), self.capacity)?;
                Ok((Arc::new(a), Arc::new(b)))
            }
            BackingId::Shm { prefix } => {
                let size = crate::spsc_ring::spsc_ring_file_size(self.capacity);
                let shm_a = crate::shm_file::ShmFile::create_or_open_named(
                    &format!("{prefix}_mpsc_{i}"), size,
                ).map_err(|e| RingError::IoError(e.kind()))?;
                let shm_b = crate::shm_file::ShmFile::create_or_open_named(
                    &format!("{prefix}_mpmc_{i}"), size,
                ).map_err(|e| RingError::IoError(e.kind()))?;
                let a = SpscRingCore::create_from_shm(shm_a, self.capacity)?;
                let b = SpscRingCore::create_from_shm(shm_b, self.capacity)?;
                Ok((Arc::new(a), Arc::new(b)))
            }
        }
    }

    /// Grow the per-producer backings so slots `< want` all exist:
    /// create the missing backing pairs, append them to the local
    /// arrays, then publish the new count (Release) so other
    /// processes open them on their next epoch sync.
    fn grow_rings_to(&self, want: usize) -> Result<(), RingError> {
        let _guard = self.grow_lock.lock();
        let published = self.directory.published();
        let cur_mpsc = self.mpsc.rings.load_full();
        let cur_mpmc = self.mpmc.rings.load_full();
        let mut mpsc_new = (*cur_mpsc).clone();
        let mut mpmc_new = (*cur_mpmc).clone();
        // Open backings other processes published first, then create
        // this grower's new ones.
        for i in cur_mpsc.len()..published {
            let (a, b) = self.open_ring_backing(i)?;
            mpsc_new.push(a);
            mpmc_new.push(b);
        }
        for i in published..want {
            let (a, b) = self.create_ring_backing(i)?;
            mpsc_new.push(a);
            mpmc_new.push(b);
        }
        if mpsc_new.len() > cur_mpsc.len() {
            self.mpsc.rings.store(Arc::new(mpsc_new));
            self.mpmc.rings.store(Arc::new(mpmc_new));
            self.pin_generation.fetch_add(1, Ordering::AcqRel);
        }
        if want > published {
            self.directory.publish_rings(want);
        }
        Ok(())
    }

    /// Register a new producer. Returns its `producer_id` - a shared
    /// slot claim visible to every attached process. Registration
    /// GROWS the ring on demand (new per-producer backings past the
    /// construction hint) and auto-morphs the composed shape to the
    /// new peer counts; it fails only under a caller-declared
    /// contract ceiling ([`with_contract`](Self::with_contract)) or at
    /// the substrate slot ceiling
    /// ([`PRODUCER_SLOT_CEILING`](crate::peer_directory::PRODUCER_SLOT_CEILING)
    /// CONCURRENT producers). The id stays valid until
    /// [`unregister_producer`](Self::unregister_producer).
    pub fn register_producer(&self) -> Result<usize, AdaptiveError> {
        let slot = self.directory.claim_producer_slot()
            .ok_or(AdaptiveError::TooManyProducers)?;
        if let Some(g) = self.contract
            && !g.permits_producer(self.directory.active_producers() - 1)
        {
            self.directory.release_producer_slot(slot);
            return Err(AdaptiveError::TooManyProducers);
        }
        if slot >= self.directory.published()
            && self.grow_rings_to(slot + 1).is_err()
        {
            self.directory.release_producer_slot(slot);
            return Err(AdaptiveError::GrowthFailed);
        }
        self.ensure_synced();
        self.reshape_for_counts();
        Ok(slot)
    }

    /// Unregister a producer slot. Caller passes the id returned
    /// from [`register_producer`](Self::register_producer). The slot
    /// recycles; its backing (and any undrained backlog) stays until
    /// the consumer drains it.
    pub fn unregister_producer(&self, producer_id: usize) {
        self.directory.release_producer_slot(producer_id);
        self.reshape_for_counts();
    }

    /// Register a new consumer. Returns its `consumer_id` - a shared
    /// slot claim visible to every attached process. Rebalances MPMC
    /// ring ownership toward the new consumer set and auto-morphs
    /// the shape. Fails only under a caller-declared contract ceiling
    /// or at the substrate consumer-slot ceiling
    /// ([`CONSUMER_SLOT_CEILING`]).
    pub fn register_consumer(&self) -> Result<usize, AdaptiveError> {
        let slot = self.directory.claim_consumer_slot()
            .ok_or(AdaptiveError::TooManyConsumers)?;
        if let Some(g) = self.contract
            && !g.permits_consumer(self.directory.active_consumers() - 1)
        {
            self.directory.release_consumer_slot(slot);
            return Err(AdaptiveError::TooManyConsumers);
        }
        self.rebalance_ownership();
        self.ensure_synced();
        self.reshape_for_counts();
        Ok(slot)
    }

    /// Unregister a consumer slot. The leaving consumer transfers
    /// its MPMC ring ownership to the remaining consumers itself
    /// (it is the single owner, so the direct transfer is safe),
    /// then releases the slot.
    pub fn unregister_consumer(&self, consumer_id: usize) {
        let me = consumer_id as u16;
        let remaining: Vec<u16> = self.directory.claimed_consumer_slots()
            .into_iter()
            .filter(|s| *s != me)
            .collect();
        let n = self.directory.published();
        for r in 0..n {
            let (owner, _) = self.directory.ring_owner(r);
            if owner == me {
                match remaining.get(r % remaining.len().max(1)) {
                    Some(to) => self.directory.transfer_ring(r, me, *to),
                    None => self.directory.transfer_ring(r, me, OWNER_NONE),
                }
            }
        }
        self.directory.release_consumer_slot(consumer_id);
        self.reshape_for_counts();
    }

    /// Spread MPMC ring ownership round-robin over the CURRENT
    /// consumer set: unowned rings are claimed directly for their
    /// target; owned rings get a pending handoff their current
    /// owner applies on its next pop scan (single-writer transfer,
    /// so two consumers never drain one Lamport ring concurrently).
    fn rebalance_ownership(&self) {
        let slots = self.directory.claimed_consumer_slots();
        if slots.is_empty() {
            return;
        }
        let n = self.directory.published();
        for r in 0..n {
            let desired = slots[r % slots.len()];
            let (owner, pending) = self.directory.ring_owner(r);
            if owner == desired {
                continue;
            }
            if owner == OWNER_NONE {
                self.directory.try_claim_ring(r, desired);
            } else if pending != desired {
                self.directory.request_handoff(r, desired);
            }
        }
    }

    /// Current active producer count (shared across processes).
    pub fn active_producers(&self) -> usize {
        self.directory.active_producers()
    }

    /// Current active consumer count (shared across processes).
    pub fn active_consumers(&self) -> usize {
        self.directory.active_consumers()
    }

    /// Whether this ring carries ordering stamps.
    pub fn is_stamped(&self) -> bool {
        self.ordering.is_some()
    }

    /// Stamp kind, when stamped.
    pub fn stamp_kind(&self) -> Option<StampKind> {
        self.ordering.as_ref().map(|o| o.region.stamp_kind())
    }

    /// Current ordering mode, when stamped. The mode atom lives in
    /// the MMF-resident ordering region, so every process attached
    /// to the ring reads the same value - deliberately unlike the
    /// process-local shape tag.
    pub fn ordering_mode(&self) -> Option<OrderingMode> {
        self.ordering.as_ref().map(|o| o.region.mode())
    }

    /// Flip the ordering mode. The ordered switch is one `Release`
    /// store: Off->On retroactively orders the in-flight backlog
    /// (stamps were already in the slots), On->Off is immediate. No
    /// drain, no data movement, and outstanding pins stay valid -
    /// the pinned pop consults the mode atom on every call.
    pub fn set_ordering_mode(&self, mode: OrderingMode) -> Result<(), RingError> {
        let ord = self.ordering.as_ref().ok_or(RingError::NotStamped)?;
        ord.region.set_mode(mode);
        Ok(())
    }

    /// Cross-producer inversions observed at pop since the ordering
    /// region was created. Shared across processes.
    pub fn inversions(&self) -> u64 {
        self.ordering.as_ref().map(|o| o.region.inversions()).unwrap_or(0)
    }

    /// Watermark heartbeat for an idle producer (MergeStrict
    /// liveness). See [`OrderingRegion::refresh_watermark`].
    pub fn refresh_watermark(&self, producer_id: usize) -> Result<(), RingError> {
        let ord = self.ordering.as_ref().ok_or(RingError::NotStamped)?;
        if producer_id >= ord.region.max_producers() {
            return Err(RingError::PayloadTooLarge);
        }
        ord.region.refresh_watermark(producer_id);
        Ok(())
    }

    /// Terminal producer retirement: MergeStrict consumers stop
    /// waiting on this producer slot's silence permanently. Call on
    /// clean producer exit; the slot must not push afterwards. See
    /// [`OrderingRegion::retire_producer`].
    pub fn retire_producer(&self, producer_id: usize) -> Result<(), RingError> {
        let ord = self.ordering.as_ref().ok_or(RingError::NotStamped)?;
        if producer_id >= ord.region.max_producers() {
            return Err(RingError::PayloadTooLarge);
        }
        ord.region.retire_producer(producer_id);
        Ok(())
    }

    /// Voluntarily release the merge-drainer lease held by this
    /// process + consumer slot. Returns `Ok(false)` when the lease
    /// was not held.
    pub fn release_drainer(&self, consumer_id: usize) -> Result<bool, RingError> {
        let ord = self.ordering.as_ref().ok_or(RingError::NotStamped)?;
        Ok(ord.region.release_drainer(drainer_token(consumer_id)))
    }

    /// Advance the drainer-lease epoch (dead-drainer takeover after
    /// [`DRAINER_GRACE_EPOCHS`] missed beats). The QoS-aware sidecar
    /// ticks this once per scan; standalone callers tick it
    /// themselves, mirroring `OwnerLease::tick_epoch`.
    pub fn tick_drainer_epoch(&self) -> Result<u64, RingError> {
        let ord = self.ordering.as_ref().ok_or(RingError::NotStamped)?;
        Ok(ord.region.tick_drainer_epoch())
    }

    /// Direct access to the ordering region for composing wrappers:
    /// capacity morphs seed the fresh backing's region from the old
    /// one so counter stamps stay monotone across the swap, and
    /// E2E harnesses read watermarks / the drainer token directly.
    pub fn ordering_region(&self) -> Option<&OrderingRegion> {
        self.ordering.as_ref().map(|o| &o.region)
    }

    /// Adaptive-path push. One Acquire load on the shape tag, one
    /// branch, then the native push on the matching backend.
    ///
    /// `producer_id` selects the producer ring for MPSC / MPMC
    /// shapes. For SPSC and Vyukov shapes the id is ignored (except
    /// on stamped rings, where it selects the producer's stamp line
    /// and must stay below `max_producers`).
    ///
    /// On stamped rings the payload cap is
    /// [`STAMPED_PAYLOAD_BYTES`] and the stamp is prepended
    /// transparently; the matching `try_recv` strips it.
    pub fn try_send(&self, producer_id: usize, payload: &[u8]) -> Result<(), RingError> {
        self.ensure_synced();
        let shape = RingShape::from_u8(self.shape_tag.load(Ordering::Acquire));
        if let Some(ord) = &self.ordering {
            return self.stamped_send_inner(ord, shape, producer_id, payload);
        }
        match shape {
            RingShape::Spsc => self.spsc.try_push(payload),
            RingShape::Mpsc => {
                let rings = self.mpsc.rings.load();
                let ring = rings.get(producer_id)
                    .ok_or(RingError::PayloadTooLarge)?; // misuse: producer_id out of range
                ring.try_push(payload)
            }
            RingShape::Mpmc => {
                let rings = self.mpmc.rings.load();
                let ring = rings.get(producer_id)
                    .ok_or(RingError::PayloadTooLarge)?;
                ring.try_push(payload)
            }
            RingShape::Vyukov => self.vyukov.try_push(payload),
        }
    }

    /// Adaptive-path pop. `consumer_id` selects the consumer's
    /// round-robin partition for the MPMC shape. For SPSC, MPSC,
    /// and Vyukov shapes the id is ignored (one consumer).
    ///
    /// On stamped rings this is the ordering-aware pop: the stamp
    /// is stripped (callers see payload bytes only, `Ok(56)`), the
    /// inversion counter runs, and when the ordering mode is
    /// `MergeByStamp` / `MergeStrict` the pop k-way-merges ring
    /// heads by stamp under the single-drainer lease.
    pub fn try_recv(&self, consumer_id: usize, out: &mut [u8]) -> Result<usize, RingError> {
        self.ensure_synced();
        let shape = RingShape::from_u8(self.shape_tag.load(Ordering::Acquire));
        if let Some(ord) = &self.ordering {
            return self.ordered_recv_inner(ord, shape, consumer_id, out)
                .map(|(n, _stamp)| n);
        }
        // Stale walk: the previous shape's backlog drains first so
        // a morph never strands (or reorders ahead of) in-flight
        // items.
        if let Some(stale) = self.stale_shape()
            && stale != shape
            && Self::may_walk_stale(stale, consumer_id)
            && let Ok(n) = self.shape_pop(stale, consumer_id, out)
        {
            return Ok(n);
        }
        self.shape_pop(shape, consumer_id, out)
    }

    /// Largest record stored inline in a ring slot by the frame path.
    /// Conservative across shapes: the smallest slot payload (Vyukov's
    /// [`PAYLOAD_BYTES`] = 56) minus the 5-byte frame header (a class
    /// byte plus a `u32` length), so an inlined record fits any shape's
    /// slot no matter how the ring morphs.
    pub const FRAME_INLINE_BUDGET: usize = PAYLOAD_BYTES - 5;

    /// Block size of the lazily-created payload region. A frame larger
    /// than both the inline budget and this is rejected with
    /// [`RingError::PayloadTooLarge`]; size the region explicitly with
    /// [`with_frames`](Self::with_frames) for larger records.
    pub const FRAME_DEFAULT_BLOCK_SIZE: usize = 8192;

    /// Pre-create and size the frame payload region. Optional: the
    /// region is otherwise created lazily at
    /// [`FRAME_DEFAULT_BLOCK_SIZE`](Self::FRAME_DEFAULT_BLOCK_SIZE)
    /// the first time a record is too large to inline. No-op if the
    /// region already exists. Returns the ring for chaining.
    pub fn with_frames(self, block_size: usize, block_count: usize) -> Self {
        self.frame_region.get_or_init(|| {
            Arc::new(
                FrameRegion::create_anon(block_size, block_count)
                    .expect("frame region create"),
            )
        });
        self
    }

    fn frame_region(&self) -> &FrameRegion {
        self.frame_region.get_or_init(|| {
            let blocks = self.spsc.capacity().max(16);
            Arc::new(
                FrameRegion::create_anon(Self::FRAME_DEFAULT_BLOCK_SIZE, blocks)
                    .expect("frame region create"),
            )
        })
    }

    /// Frame-path send: carries any payload size on whatever shape the
    /// ring is in. Records up to
    /// [`FRAME_INLINE_BUDGET`](Self::FRAME_INLINE_BUDGET) go inline in
    /// the ring slot; larger ones spill to the shared payload region
    /// and the slot carries the block index. Returns which path the
    /// record took. `producer_id` selects the backing ring for MPSC /
    /// MPMC exactly as [`try_send`](Self::try_send). The same call
    /// works at every shape because the descriptor rides the slot and
    /// the region is multi-producer / multi-consumer safe.
    ///
    /// Not available on stamped (ordering) rings - frames and stamps
    /// both claim the slot head, so they are mutually exclusive;
    /// returns [`RingError::LayoutMismatch`] there.
    pub fn send_frame(&self, producer_id: usize, payload: &[u8])
        -> Result<FrameClass, RingError>
    {
        self.send_frame_as(producer_id, payload, LayoutHint::Auto)
    }

    /// [`send_frame`](Self::send_frame) with an explicit layout
    /// override ([`LayoutHint::ForceInline`] / [`LayoutHint::ForceOffset`]).
    pub fn send_frame_as(&self, producer_id: usize, payload: &[u8], hint: LayoutHint)
        -> Result<FrameClass, RingError>
    {
        if self.ordering.is_some() {
            return Err(RingError::LayoutMismatch);
        }
        let inline = match hint {
            LayoutHint::ForceInline => {
                if payload.len() > Self::FRAME_INLINE_BUDGET {
                    return Err(RingError::PayloadTooLarge);
                }
                true
            }
            LayoutHint::ForceOffset => false,
            LayoutHint::Auto => payload.len() <= Self::FRAME_INLINE_BUDGET,
        };
        let len = payload.len() as u32;
        if inline {
            // [class:u8][len:u32][payload bytes]; fits the 56-byte
            // Vyukov slot, so it fits every shape's slot.
            let mut buf = [0u8; PAYLOAD_BYTES];
            buf[0] = FrameClass::Inline as u8;
            buf[1..5].copy_from_slice(&len.to_le_bytes());
            buf[5..5 + payload.len()].copy_from_slice(payload);
            self.try_send(producer_id, &buf[..5 + payload.len()])?;
            Ok(FrameClass::Inline)
        } else {
            let region = self.frame_region();
            if payload.len() > region.block_size() {
                return Err(RingError::PayloadTooLarge);
            }
            let idx = region.alloc().ok_or(RingError::Full)?;
            region.write_block(idx, payload);
            // [class:u8][len:u32][block_idx:u32]
            let mut buf = [0u8; 9];
            buf[0] = FrameClass::Offset as u8;
            buf[1..5].copy_from_slice(&len.to_le_bytes());
            buf[5..9].copy_from_slice(&idx.to_le_bytes());
            match self.try_send(producer_id, &buf) {
                Ok(()) => Ok(FrameClass::Offset),
                Err(e) => {
                    // Descriptor push failed (ring full): return the
                    // block so it is not leaked.
                    region.free(idx);
                    Err(e)
                }
            }
        }
    }

    /// Frame-path recv: counterpart to [`send_frame`](Self::send_frame).
    /// Clears `out` and
    /// fills it with the record's payload, transparently reading the
    /// payload region and freeing its block for offset records. Returns
    /// which path the record took. `consumer_id` selects the consumer
    /// partition for MPMC as [`try_recv`](Self::try_recv). Not available
    /// on stamped rings.
    pub fn recv_frame(&self, consumer_id: usize, out: &mut Vec<u8>)
        -> Result<FrameClass, RingError>
    {
        if self.ordering.is_some() {
            return Err(RingError::LayoutMismatch);
        }
        // SPSC_PAYLOAD_BYTES (64) holds any shape's slot.
        let mut slot = [0u8; SPSC_PAYLOAD_BYTES];
        self.try_recv(consumer_id, &mut slot)?;
        let len = u32::from_le_bytes([slot[1], slot[2], slot[3], slot[4]]) as usize;
        out.clear();
        if slot[0] == FrameClass::Inline as u8 {
            out.extend_from_slice(&slot[5..5 + len]);
            Ok(FrameClass::Inline)
        } else {
            let idx = u32::from_le_bytes([slot[5], slot[6], slot[7], slot[8]]);
            let region = self.frame_region();
            region.read_block_into(idx, len, out);
            region.free(idx);
            Ok(FrameClass::Offset)
        }
    }

    /// As [`try_recv`](Self::try_recv) on a stamped ring, also
    /// returning the popped item's stamp. This is how consumers
    /// assert the ordering guarantee they paid for (monotone stamps
    /// under the merge modes) instead of trusting it. Returns
    /// [`RingError::NotStamped`] on unstamped rings.
    pub fn try_recv_with_stamp(
        &self,
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<(usize, u64), RingError> {
        let ord = self.ordering.as_ref().ok_or(RingError::NotStamped)?;
        let shape = RingShape::from_u8(self.shape_tag.load(Ordering::Acquire));
        self.ordered_recv_inner(ord, shape, consumer_id, out)
    }

    /// Stamped push: issue the producer's next stamp, lay out
    /// `[stamp; 8][payload; <=56]` and push to the active Lamport
    /// backing. The watermark advances whether the push lands or
    /// returns `Full` - a stamp that failed to publish will never
    /// appear, so advancing keeps the MergeStrict in-flight gate
    /// live.
    fn stamped_send_inner(
        &self,
        ord: &OrderingState,
        shape: RingShape,
        producer_id: usize,
        payload: &[u8],
    ) -> Result<(), RingError> {
        if payload.len() > STAMPED_PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        if producer_id >= ord.region.max_producers() {
            return Err(RingError::PayloadTooLarge);
        }
        let mpsc_guard;
        let mpmc_guard;
        let ring: &SpscRingCore = match shape {
            RingShape::Spsc => &self.spsc,
            RingShape::Mpsc => {
                mpsc_guard = self.mpsc.rings.load();
                mpsc_guard.get(producer_id).ok_or(RingError::PayloadTooLarge)?
            }
            RingShape::Mpmc => {
                mpmc_guard = self.mpmc.rings.load();
                mpmc_guard.get(producer_id).ok_or(RingError::PayloadTooLarge)?
            }
            // Stamped rings never run the Vyukov backing: the
            // stamped 64-byte slot layout does not fit its 56-byte
            // slots. morph_to rejects the transition, so this arm is
            // a defensive layout error, not a reachable path.
            RingShape::Vyukov => return Err(RingError::LayoutMismatch),
        };
        let stamp = ord.region.next_stamp(producer_id);
        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        buf[..STAMP_BYTES].copy_from_slice(&stamp.to_le_bytes());
        buf[STAMP_BYTES..STAMP_BYTES + payload.len()].copy_from_slice(payload);
        let result = ring.try_push(&buf[..STAMP_BYTES + payload.len()]);
        ord.region.publish_watermark(producer_id, stamp);
        result
    }

    /// Stamped pop: strip the stamp, run the inversion counter, and
    /// dispatch per the live ordering mode. Returns the payload
    /// length and the popped stamp.
    fn ordered_recv_inner(
        &self,
        ord: &OrderingState,
        shape: RingShape,
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<(usize, u64), RingError> {
        if consumer_id >= CONSUMER_SLOT_CEILING {
            return Err(RingError::PayloadTooLarge);
        }
        if out.len() < STAMPED_PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let mode = ord.region.mode();
        match mode {
            OrderingMode::Unordered => {
                let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                // Stale walk first (a stamped ring's backings are
                // all Lamport shapes, so the stale pop is the same
                // stamped slot layout).
                let popped = self.stale_shape()
                    .filter(|stale| *stale != shape)
                    .filter(|stale| Self::may_walk_stale(*stale, consumer_id))
                    .and_then(|stale| {
                        self.shape_pop(stale, consumer_id, &mut buf).ok()
                    });
                if popped.is_none() {
                    match shape {
                        RingShape::Spsc => self.spsc.try_pop(&mut buf),
                        RingShape::Mpsc => self.mpsc_pop(&mut buf),
                        RingShape::Mpmc => self.mpmc_pop(consumer_id, &mut buf),
                        RingShape::Vyukov => Err(RingError::LayoutMismatch),
                    }?;
                }
                let stamp = u64::from_le_bytes(
                    buf[..STAMP_BYTES].try_into().unwrap(),
                );
                self.note_stamp(ord, consumer_id, mode, stamp);
                out[..STAMPED_PAYLOAD_BYTES]
                    .copy_from_slice(&buf[STAMP_BYTES..]);
                Ok((STAMPED_PAYLOAD_BYTES, stamp))
            }
            OrderingMode::MergeByStamp | OrderingMode::MergeStrict => {
                // Always hold the single-drainer lease: consumers can
                // JOIN at runtime, so a static 1-consumer bypass would
                // leave a leaseless drainer racing the new joiner's
                // leased one. Per-pop verification must stay OFF the
                // stamp-hot header line (producers fetch_add it every
                // push; each extra consumer load of it costs a cache
                // transfer): one load of the quiet lease-generation
                // line, compared to a consumer-local cache, and only a
                // change (claim / takeover / release / epoch tick)
                // runs the full lease handshake.
                let seen = &ord.seen[consumer_id];
                let lease_gen_now = ord.region.lease_generation();
                if seen.lease_gen.load(Ordering::Relaxed) != lease_gen_now {
                    if !ord.region.try_acquire_drainer(
                        drainer_token(consumer_id),
                        DRAINER_GRACE_EPOCHS,
                    ) {
                        return Err(RingError::NotDrainer);
                    }
                    seen.lease_gen.store(lease_gen_now, Ordering::Relaxed);
                }
                // Stale walk: merge within the stale shape's rings
                // until that backlog drains, then merge the active
                // shape. Stale items predate active items (producers
                // switched at the tag flip), so stale-first keeps
                // global stamp order across the morph boundary.
                if let Some(stale) = self.stale_shape()
                    && stale != shape
                {
                    match self.merge_pop(ord, stale, consumer_id, mode, out) {
                        Ok(result) => return Ok(result),
                        Err(RingError::Empty) => {}
                        Err(e) => return Err(e),
                    }
                }
                self.merge_pop(ord, shape, consumer_id, mode, out)
            }
        }
    }


    /// K-way min-stamp merge over the active shape's ring heads:
    /// peek every non-empty ring, pick the minimum stamp, confirm
    /// exactly that slot, leave every other head unconsumed.
    ///
    /// Three release gates sit between the scan and the confirm:
    ///
    /// - **In-flight gate** (both merge modes): a producer whose
    ///   `issued` stamp (or reservation floor) is ahead of its
    ///   `watermark` holds exactly one stamp in its
    ///   reserve-stamp-push window; if it undercuts the candidate,
    ///   the pop returns `Empty` until the publish lands (or the
    ///   push's `Full` failure advances the watermark). This is the
    ///   only bound that survives producer descheduling - the
    ///   window stretches to scheduler quanta under preemption or
    ///   virtualization, far past any fixed time guard.
    /// - **Freshness guard** (time-based stamps, both merge modes,
    ///   only when at least one ring is empty): a candidate younger
    ///   than the guard window may be raced by a stamp a producer
    ///   has not even RESERVED yet (cross-core clock skew); the
    ///   merge re-peeks until the candidate ages out (bounded by
    ///   the guard, ~2us).
    /// - **Watermark gate** (`MergeStrict` only): every EMPTY
    ///   in-use ring's watermark must have reached the candidate,
    ///   closing the not-yet-reserved case with zero time-semantics
    ///   assumptions. This couples release latency to the slowest
    ///   producer: idle producers heartbeat via
    ///   [`refresh_watermark`](OrderingRegion::refresh_watermark)
    ///   and exiting producers call
    ///   [`retire_producer`](OrderingRegion::retire_producer), or
    ///   the strict consumer stalls on their silence by design.
    fn merge_pop(
        &self,
        ord: &OrderingState,
        shape: RingShape,
        consumer_id: usize,
        mode: OrderingMode,
        out: &mut [u8],
    ) -> Result<(usize, u64), RingError> {
        // Snapshot the composed arrays (they live behind an ArcSwap
        // for producer growth); the SPSC arm borrows directly.
        let mpsc_guard;
        let mpmc_guard;
        let rings: &[Arc<SpscRingCore>] = match shape {
            RingShape::Spsc => std::slice::from_ref(&self.spsc),
            RingShape::Mpsc => {
                mpsc_guard = self.mpsc.rings.load();
                mpsc_guard.as_slice()
            }
            RingShape::Mpmc => {
                mpmc_guard = self.mpmc.rings.load();
                mpmc_guard.as_slice()
            }
            RingShape::Vyukov => &[],
        };
        if rings.is_empty() {
            return Err(RingError::LayoutMismatch);
        }
        // The release gates cover every producer slot that has ever
        // stamped: the PUBLISHED slot count, not the region's
        // ceiling-sized line array (whose untouched tail would cost
        // thousands of loads per pop).
        let gate_lines = self.directory.published()
            .min(ord.region.max_producers());
        let kind = ord.region.stamp_kind();
        loop {
            // Scalar min scan: the per-ring peek atomics dominate
            // the cost of each pass, so the comparison work is not
            // the bottleneck at realistic producer counts.
            let mut best: Option<(usize, u64)> = None;
            let mut any_empty = false;
            for (i, ring) in rings.iter().enumerate() {
                match ring.peek_slot() {
                    Some(peek) => {
                        let s = u64::from_le_bytes(
                            peek[..STAMP_BYTES].try_into().unwrap(),
                        );
                        if best.is_none_or(|(_, bs)| s < bs) {
                            best = Some((i, s));
                        }
                    }
                    None => any_empty = true,
                }
            }
            let Some((idx, stamp)) = best else {
                return Err(RingError::Empty);
            };

            for line in 0..gate_lines {
                if ord.region.in_flight_below(line, stamp) {
                    return Err(RingError::Empty);
                }
            }
            if mode == OrderingMode::MergeStrict {
                for line in 0..gate_lines {
                    // In-use slot (has ever stamped; retirement
                    // saturates the watermark so retired slots
                    // always pass) whose ring is empty: its
                    // watermark must have reached the candidate.
                    if ord.region.issued(line) != 0
                        && rings[line.min(rings.len() - 1)].approx_len() == 0
                        && ord.region.watermark(line) < stamp
                    {
                        return Err(RingError::Empty);
                    }
                }
            }
            if any_empty
                && let Some(guard) = kind.freshness_guard()
                && stamp_now(kind).wrapping_sub(stamp) < guard
            {
                std::hint::spin_loop();
                continue;
            }

            let peek = rings[idx].peek_slot().expect(
                "single drainer holds the lease; a peeked head cannot vanish",
            );
            let confirmed_stamp = u64::from_le_bytes(
                peek[..STAMP_BYTES].try_into().unwrap(),
            );
            out[..STAMPED_PAYLOAD_BYTES].copy_from_slice(&peek[STAMP_BYTES..]);
            peek.confirm();
            self.note_stamp(ord, consumer_id, mode, confirmed_stamp);
            return Ok((STAMPED_PAYLOAD_BYTES, confirmed_stamp));
        }
    }

    /// Per-consumer inversion accounting. A pop whose stamp
    /// undercuts the previous pop's stamp is one cross-producer
    /// inversion: the counter bumps in the shared header and an
    /// observation rides the sidecar ring. Mode transitions reset
    /// the baseline so the retroactive reordering of the backlog at
    /// an Off->On flip is not miscounted.
    fn note_stamp(
        &self,
        ord: &OrderingState,
        consumer_id: usize,
        mode: OrderingMode,
        stamp: u64,
    ) {
        let line = &ord.seen[consumer_id];
        if line.mode_tag.swap(mode as u32, Ordering::Relaxed) != mode as u32 {
            line.stamp.store(0, Ordering::Relaxed);
        }
        let last = line.stamp.load(Ordering::Relaxed);
        if stamp < last {
            ord.region.record_inversion();
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ordering::OP_ORDER_INVERSION, 0);
        }
        line.stamp.store(stamp, Ordering::Relaxed);
    }

    fn mpsc_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        let rings = self.mpsc.rings.load();
        self.mpsc_pop_in(&rings, out)
    }

    fn mpsc_pop_in(
        &self,
        rings: &[Arc<SpscRingCore>],
        out: &mut [u8],
    ) -> Result<usize, RingError> {
        let n = rings.len();
        if n == 0 {
            return Err(RingError::Empty);
        }
        let start = self.mpsc.next_drain.load(Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(bytes) = rings[idx].try_pop(out) {
                self.mpsc.next_drain.store((idx + 1) % n, Ordering::Relaxed);
                return Ok(bytes);
            }
        }
        Err(RingError::Empty)
    }

    /// MPMC pop through the shared ownership table: this consumer
    /// drains exactly the rings whose owner entry names its slot
    /// (single-reader invariant), CAS-claims unowned rings on sight
    /// (so unregistered-consumer flows keep working), applies
    /// pending rebalance handoffs from its own scan (single-writer
    /// transfer), and - rate-limited - takes over rings whose owner
    /// process died.
    fn mpmc_pop(&self, consumer_id: usize, out: &mut [u8]) -> Result<usize, RingError> {
        let rings = self.mpmc.rings.load();
        self.mpmc_pop_in(&rings, consumer_id, out)
    }

    fn mpmc_pop_in(
        &self,
        rings: &[Arc<SpscRingCore>],
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<usize, RingError> {
        let cursor_line = self.mpmc.consumer_cursors.get(consumer_id)
            .ok_or(RingError::PayloadTooLarge)?;
        let me = consumer_id as u16;
        let n = rings.len();
        if n == 0 {
            return Err(RingError::Empty);
        }
        let start = cursor_line.0.load(Ordering::Relaxed) % n;
        // First stuck ring (owned elsewhere, has items): the crash-
        // takeover candidate when the whole scan comes up empty.
        let mut stuck: Option<(usize, u16)> = None;
        for i in 0..n {
            let idx = (start + i) % n;
            let (owner, pending) = self.directory.ring_owner(idx);
            if owner == me {
                if pending != OWNER_NONE
                    && self.directory.apply_handoff(idx, me).is_some()
                {
                    continue; // handed off; not ours to drain anymore
                }
                if let Ok(bytes) = rings[idx].try_pop(out) {
                    cursor_line.0.store((idx + 1) % n, Ordering::Relaxed);
                    return Ok(bytes);
                }
            } else if owner == OWNER_NONE {
                if self.directory.try_claim_ring(idx, me)
                    && let Ok(bytes) = rings[idx].try_pop(out)
                {
                    cursor_line.0.store((idx + 1) % n, Ordering::Relaxed);
                    return Ok(bytes);
                }
            } else if stuck.is_none() && rings[idx].approx_len() > 0 {
                stuck = Some((idx, owner));
            }
        }
        // Crash takeover, rate-limited: the pid probe is a syscall,
        // so only every 1024th empty scan per consumer attempts it.
        if let Some((idx, owner)) = stuck {
            let probes = cursor_line.1.fetch_add(1, Ordering::Relaxed);
            if probes % 1024 == 1023
                && self.directory.try_takeover(idx, owner, me)
                && let Ok(bytes) = rings[idx].try_pop(out)
            {
                cursor_line.0.store((idx + 1) % n, Ordering::Relaxed);
                return Ok(bytes);
            }
        }
        Err(RingError::Empty)
    }

    /// Pin the current shape and return a [`PinnedRing`] that
    /// exposes the matching backend at native speed. The composed
    /// arrays are captured at pin time (zero per-op indirection);
    /// producer growth bumps the pin generation, so pin holders see
    /// [`PinnedRing::is_still_valid`] `== false` and re-pin to pick
    /// up new backings.
    pub fn pin_current_shape(&self) -> PinnedRing<'_> {
        self.ensure_synced();
        let captured_gen = self.pin_generation.load(Ordering::Acquire);
        let shape = RingShape::from_u8(self.shape_tag.load(Ordering::Acquire));
        PinnedRing {
            parent: self,
            pinned_generation: captured_gen,
            shape,
            mpsc_rings: self.mpsc.rings.load_full(),
            mpmc_rings: self.mpmc.rings.load_full(),
            _not_sync: PhantomData,
        }
    }

    /// Trigger a shape morph. NO data moves: the old shape's
    /// backing becomes the STALE backing, producers follow the new
    /// `shape_tag` immediately, and the consumer's pop path drains
    /// the stale backlog first (the stale walk) before reading from
    /// the new shape. This is what makes morphing safe under
    /// saturating traffic - there is no transfer to overflow the
    /// target's capacity and no second drainer racing the live
    /// consumer (each backing keeps exactly one reader).
    ///
    /// The stale marker stays set until the NEXT morph, which
    /// requires the backlog drained ([`RingError::StaleBacklog`]
    /// otherwise - the sidecar's scan loop simply retries). Keeping
    /// it set gives a producer whose push straddled the tag flip a
    /// wide grace window: its item lands in the old backing, which
    /// the consumer still walks.
    ///
    /// Bumps `pin_generation` so outstanding pins see
    /// `is_still_valid() == false` and re-acquire. Pinned NATIVE
    /// pops (`spsc_try_pop` etc.) are shape-direct and do not walk
    /// the stale backing; consumers that pop through pins across
    /// morphs use [`AdaptiveRing::try_recv`] or
    /// [`PinnedRing::ordered_try_pop`], which do.
    ///
    /// An explicit `morph_to` is a USER shape decision, so it pins
    /// the shape (suppresses the automatic count-driven reshape)
    /// until [`resume_auto_shape`](Self::resume_auto_shape).
    pub fn morph_to(&self, new_shape: RingShape) -> Result<(), RingError> {
        self.shape_auto.store(false, Ordering::Relaxed);
        self.morph_shape(new_shape)
    }

    /// The morph mechanism, shared by the public (pinning)
    /// [`morph_to`](Self::morph_to), the automatic count-driven
    /// reshape, and policy sidecars.
    pub(crate) fn morph_shape(&self, new_shape: RingShape) -> Result<(), RingError> {
        let old_shape = RingShape::from_u8(self.shape_tag.load(Ordering::Acquire));
        if old_shape == new_shape {
            return Ok(());
        }

        // A stamped ring never runs the Vyukov backing: the stamped
        // 64-byte slot layout ([stamp; 8][payload; 56]) does not fit
        // Vyukov's 56-byte slots. The GlobalFifo declaration on a
        // stamped ring is served by the merge flag
        // (set_ordering_mode) instead of this morph.
        if self.ordering.is_some() && new_shape == RingShape::Vyukov {
            return Err(RingError::LayoutMismatch);
        }

        // One stale backing at a time: the prior morph's backlog
        // must be drained before another shape change.
        let prior_stale = self.stale_shape_tag.load(Ordering::Acquire);
        if prior_stale != STALE_NONE
            && !self.backing_is_empty(RingShape::from_u8(prior_stale))
        {
            return Err(RingError::StaleBacklog);
        }

        // Bump the pin generation so existing pins see
        // is_still_valid() == false on their next check; then
        // publish old-as-stale before the new tag so a pop that
        // observes the new shape also sees the stale marker.
        self.pin_generation.fetch_add(1, Ordering::AcqRel);
        self.stale_shape_tag.store(old_shape as u8, Ordering::Release);
        self.shape_tag.store(new_shape as u8, Ordering::Release);
        Ok(())
    }

    /// Whether one shape's backing holds no items right now.
    fn backing_is_empty(&self, shape: RingShape) -> bool {
        match shape {
            RingShape::Spsc => self.spsc.approx_len() == 0,
            RingShape::Mpsc => self.mpsc.rings.load().iter().all(|r| r.approx_len() == 0),
            RingShape::Mpmc => self.mpmc.rings.load().iter().all(|r| r.approx_len() == 0),
            RingShape::Vyukov => self.vyukov.approx_len() == 0,
        }
    }

    /// The stale shape still draining after the last morph, if any.
    fn stale_shape(&self) -> Option<RingShape> {
        let tag = self.stale_shape_tag.load(Ordering::Acquire);
        if tag == STALE_NONE {
            None
        } else {
            Some(RingShape::from_u8(tag))
        }
    }

    /// Whether `consumer_id` may drain a stale backing of `shape`.
    /// Single-reader backings (SPSC, MPSC) are walked by consumer 0
    /// only; the MPMC grid partitions per consumer and Vyukov pops
    /// are CAS-safe for any consumer.
    fn may_walk_stale(shape: RingShape, consumer_id: usize) -> bool {
        match shape {
            RingShape::Spsc | RingShape::Mpsc => consumer_id == 0,
            RingShape::Mpmc | RingShape::Vyukov => true,
        }
    }

    /// Unstamped pop from one shape's backing.
    fn shape_pop(
        &self,
        shape: RingShape,
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<usize, RingError> {
        match shape {
            RingShape::Spsc => self.spsc.try_pop(out),
            RingShape::Mpsc => self.mpsc_pop(out),
            RingShape::Mpmc => self.mpmc_pop(consumer_id, out),
            RingShape::Vyukov => self.vyukov.try_pop(out),
        }
    }
}

fn with_suffix(base: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(suffix);
    std::path::PathBuf::from(s)
}

/// Error type for AdaptiveRing registration / morph operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveError {
    /// `register_producer` refused: a caller-declared contract
    /// ceiling ([`AdaptiveRing::with_contract`]) or the substrate
    /// slot ceiling. Never returned by the unpinned default below
    /// the substrate ceiling - registration grows the ring instead.
    TooManyProducers,
    /// `register_consumer` refused: same two cases on the consumer
    /// axis.
    TooManyConsumers,
    /// Producer registration claimed a slot but creating / opening
    /// the grown backing failed (I/O); the slot was released.
    GrowthFailed,
}

/// Handle pinned to one shape of the parent [`AdaptiveRing`].
/// Hot-path ops bypass the adaptive dispatch and call the native
/// backend directly. The pin holder periodically calls
/// [`is_still_valid`](Self::is_still_valid) to check whether a
/// morph has invalidated this pin; on `false` the caller releases
/// and re-acquires via [`AdaptiveRing::pin_current_shape`].
pub struct PinnedRing<'a> {
    parent: &'a AdaptiveRing,
    pinned_generation: u64,
    shape: RingShape,
    /// Composed arrays captured at pin time: pinned ops index these
    /// directly (native speed, no ArcSwap load per op). Producer
    /// growth invalidates the pin, so a re-pin picks up new rings.
    mpsc_rings: Arc<Vec<Arc<SpscRingCore>>>,
    mpmc_rings: Arc<Vec<Arc<SpscRingCore>>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<'a> PinnedRing<'a> {
    /// Shape this pin was captured at.
    pub fn shape(&self) -> RingShape { self.shape }

    /// Monitor-wait HINT for the consumer side of `shape`: an atom
    /// whose Release-store accompanies (or is) the next publish a
    /// pop is waiting for. Arm `crate::monitor_wait::monitor_wait_u64`
    /// on it instead of burning a raw spin loop - on Windows the
    /// scheduler deschedules and migrates pure spinners (measured
    /// 1.7-2.7 us one-way for a cross-process spin ping-pong that
    /// runs in ~100-300 ns under Linux/FreeBSD on comparable
    /// silicon), while a monitor-armed waiter wakes on the store
    /// itself.
    ///
    /// Contract: this is a HINT, not a wake guarantee - on the
    /// multi-line shapes (MPSC/MPMC) it covers producer line 0
    /// only, and on Vyukov it covers the slot at the CURRENT
    /// consumer position (recompute after each pop). Callers must
    /// keep their waits budget-bounded and re-poll, which
    /// `monitor_wait_u64`'s budget enforces.
    pub fn recv_signal(&self, shape: RingShape) -> &AtomicU64 {
        match shape {
            RingShape::Spsc => self.parent.spsc.head_signal(),
            RingShape::Mpsc => self.mpsc_rings[0].head_signal(),
            RingShape::Mpmc => self.mpmc_rings[0].head_signal(),
            RingShape::Vyukov => self.parent.vyukov.next_pop_signal(),
        }
    }

    /// One Acquire load on the parent's `pin_generation`. Returns
    /// `true` while the pin is current; `false` if a morph has
    /// happened and the caller should release + re-acquire.
    pub fn is_still_valid(&self) -> bool {
        self.parent.pin_generation.load(Ordering::Acquire) == self.pinned_generation
    }

    /// Native SPSC push. Caller assumes single-producer ownership
    /// and ensures pin validity is checked at meaningful intervals.
    pub fn spsc_try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        self.parent.spsc.try_push(payload)
    }

    /// Native SPSC pop.
    pub fn spsc_try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.parent.spsc.try_pop(out)
    }

    /// MPSC push to a specific producer ring (captured at pin time).
    pub fn mpsc_try_push(&self, producer_id: usize, payload: &[u8]) -> Result<(), RingError> {
        let ring = self.mpsc_rings.get(producer_id)
            .ok_or(RingError::PayloadTooLarge)?;
        ring.try_push(payload)
    }

    /// MPSC pop (round-robin across the producer rings captured at
    /// pin time; a grown producer set invalidates the pin).
    pub fn mpsc_try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.parent.mpsc_pop_in(&self.mpsc_rings, out)
    }

    /// MPMC push to a specific producer ring (captured at pin time).
    pub fn mpmc_try_push(&self, producer_id: usize, payload: &[u8]) -> Result<(), RingError> {
        let ring = self.mpmc_rings.get(producer_id)
            .ok_or(RingError::PayloadTooLarge)?;
        ring.try_push(payload)
    }

    /// MPMC pop for a specific consumer. Ownership is consulted live
    /// from the shared directory (correctness under consumer joins /
    /// leaves); the ring array is the pin-time capture.
    pub fn mpmc_try_pop(&self, consumer_id: usize, out: &mut [u8]) -> Result<usize, RingError> {
        self.parent.mpmc_pop_in(&self.mpmc_rings, consumer_id, out)
    }

    /// Vyukov MPMC push.
    pub fn vyukov_try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        self.parent.vyukov.try_push(payload)
    }

    /// Vyukov MPMC pop.
    pub fn vyukov_try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.parent.vyukov.try_pop(out)
    }

    /// Stamped push through the pinned shape: the producer's next
    /// stamp is prepended and the payload cap is
    /// [`STAMPED_PAYLOAD_BYTES`]. Requires a ring constructed with
    /// [`AdaptiveRing::with_ordering_stamps`]; returns
    /// [`RingError::NotStamped`] otherwise.
    pub fn stamped_try_push(
        &self,
        producer_id: usize,
        payload: &[u8],
    ) -> Result<(), RingError> {
        let ord = self.parent.ordering.as_ref().ok_or(RingError::NotStamped)?;
        self.parent.stamped_send_inner(ord, self.shape, producer_id, payload)
    }

    /// Ordering-aware pop through the pinned shape. The pin stays
    /// valid across ordering-mode flips - this call reads the
    /// MMF-resident mode atom every time (one Acquire load, a plain
    /// MOV on x86 TSO) and dispatches accordingly: partition pop +
    /// inversion counter under `Unordered`, k-way min-stamp merge
    /// under `MergeByStamp` / `MergeStrict`. Returns payload bytes
    /// only (`Ok(56)`).
    pub fn ordered_try_pop(
        &self,
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<usize, RingError> {
        let ord = self.parent.ordering.as_ref().ok_or(RingError::NotStamped)?;
        self.parent
            .ordered_recv_inner(ord, self.shape, consumer_id, out)
            .map(|(n, _stamp)| n)
    }

    /// As [`ordered_try_pop`](Self::ordered_try_pop), also returning
    /// the popped stamp so hot-loop consumers can assert the
    /// ordering guarantee they paid for.
    pub fn ordered_try_pop_with_stamp(
        &self,
        consumer_id: usize,
        out: &mut [u8],
    ) -> Result<(usize, u64), RingError> {
        let ord = self.parent.ordering.as_ref().ok_or(RingError::NotStamped)?;
        self.parent.ordered_recv_inner(ord, self.shape, consumer_id, out)
    }
}

/// Zero-copy peek into AdaptiveRing's SPSC backing. Wraps the
/// underlying [`PeekedSlot`](crate::spsc_ring::PeekedSlot) so the
/// AdaptiveRing crate boundary owns the type. Same semantics:
/// derefs to `&[u8]`, call `confirm` to release the slot.
pub struct PeekedSpscSlot<'a> {
    inner: crate::spsc_ring::PeekedSlot<'a>,
}

impl<'a> PeekedSpscSlot<'a> {
    pub fn as_slice(&self) -> &[u8] { self.inner.as_slice() }
    pub fn len(&self) -> usize { self.inner.len() }
    pub fn is_empty(&self) -> bool { self.inner.is_empty() }
    pub fn confirm(self) { self.inner.confirm() }
}

impl<'a> std::ops::Deref for PeekedSpscSlot<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] { &self.inner }
}

/// SPSC payload size for the SPSC / MPSC / MPMC backings (Lamport
/// slot is 64B payload-only).
pub const ADAPTIVE_SPSC_PAYLOAD_BYTES: usize = SPSC_PAYLOAD_BYTES;

/// Vyukov payload size for the Vyukov backing (56B; 8B is the
/// per-slot sequence atomic).
pub const ADAPTIVE_VYUKOV_PAYLOAD_BYTES: usize = PAYLOAD_BYTES;

// ===================================================================
// Sidecar shape policy: automatic morphing based on peer-count
// observations.
// ===================================================================

/// A snapshot of the ring's observable state passed to a policy
/// on every sidecar scan.
#[derive(Debug, Clone, Copy)]
pub struct PolicyObservation {
    pub active_producers: usize,
    pub active_consumers: usize,
    pub current_shape: RingShape,
    pub since_last_morph: std::time::Duration,
    /// Whether the ring carries ordering stamps. Shape policies
    /// consult this because the GlobalFifo declaration routes
    /// differently: unstamped rings morph to Vyukov, stamped rings
    /// flip the merge flag (the ordering policy's job) and must
    /// stay on the composed shapes.
    pub stamped: bool,
}

/// Policy that decides when (and to what shape) the sidecar
/// should morph the ring.
///
/// The sidecar scanner calls `decide` on every scan tick with the
/// current peer counts + shape + cooldown since the last morph.
/// Returning `Some(new_shape)` triggers a `morph_to(new_shape)`.
/// Returning `None` leaves the shape alone.
pub trait RingShapePolicy: Send + Sync + 'static {
    fn decide(&self, observation: &PolicyObservation) -> Option<RingShape>;
}

/// Default policy: pick the cheapest shape that fits the current
/// peer counts, with a fixed hysteresis interval after each morph
/// to prevent thrashing under rapid peer-count oscillation.
///
/// Mapping (when `since_last_morph >= hysteresis`):
///
/// | producers | consumers | shape  |
/// |-----------|-----------|--------|
/// |     1     |     1     | `Spsc` |
/// |    >=2    |     1     | `Mpsc` |
/// |     *     |    >=2    | `Mpmc` |
///
/// While `since_last_morph < hysteresis`, returns `None` even if
/// the target shape differs. While either peer count is 0 the
/// policy also returns `None` (no point morphing an empty ring).
pub struct DefaultRingShapePolicy {
    pub hysteresis: std::time::Duration,
}

impl Default for DefaultRingShapePolicy {
    fn default() -> Self {
        Self { hysteresis: std::time::Duration::from_millis(100) }
    }
}

impl DefaultRingShapePolicy {
    pub fn target_shape(producers: usize, consumers: usize) -> Option<RingShape> {
        match (producers, consumers) {
            (0, _) | (_, 0) => None,
            (1, 1) => Some(RingShape::Spsc),
            (_, 1) => Some(RingShape::Mpsc),
            (_, _) => Some(RingShape::Mpmc),
        }
    }
}

impl RingShapePolicy for DefaultRingShapePolicy {
    fn decide(&self, obs: &PolicyObservation) -> Option<RingShape> {
        if obs.since_last_morph < self.hysteresis {
            return None;
        }
        let target = Self::target_shape(obs.active_producers, obs.active_consumers)?;
        if target == obs.current_shape {
            None
        } else {
            Some(target)
        }
    }
}

/// QoS-aware shape policy: consumes the
/// [`Ordering`](crate::qos_policy::Ordering) declaration on a
/// [`QosPolicy`](crate::qos_policy::QosPolicy) alongside the peer
/// counts.
///
/// Decision matrix (after the hysteresis cooldown):
///
/// | declaration | ring | decision |
/// |---|---|---|
/// | `GlobalFifo` | unstamped | morph to `Vyukov` (the proven global-FIFO structure) |
/// | `GlobalFifo` | stamped | counts-based composed shape; the ordering axis is served by the merge flag, which the [`OrderingPolicy`] flips |
/// | `PerProducer` | either | counts-based default (which also walks an earlier Vyukov morph back once the declaration is withdrawn) |
pub struct QosRingShapePolicy {
    pub qos: Arc<crate::qos_policy::QosPolicy>,
    pub hysteresis: std::time::Duration,
}

impl QosRingShapePolicy {
    pub fn new(qos: Arc<crate::qos_policy::QosPolicy>) -> Self {
        Self { qos, hysteresis: std::time::Duration::from_millis(100) }
    }
}

impl RingShapePolicy for QosRingShapePolicy {
    fn decide(&self, obs: &PolicyObservation) -> Option<RingShape> {
        if obs.since_last_morph < self.hysteresis {
            return None;
        }
        let target = match self.qos.ordering() {
            QosOrdering::GlobalFifo if !obs.stamped => Some(RingShape::Vyukov),
            _ => DefaultRingShapePolicy::target_shape(
                obs.active_producers,
                obs.active_consumers,
            ),
        }?;
        if target == obs.current_shape {
            None
        } else {
            Some(target)
        }
    }
}

/// A snapshot of a stamped ring's ordering-relevant state passed to
/// an [`OrderingPolicy`] on every sidecar scan.
#[derive(Debug, Clone, Copy)]
pub struct OrderingPolicyObservation {
    /// Inversions per second observed since the previous scan
    /// (delta of the shared inversion counter over the scan
    /// interval).
    pub inversions_per_sec: f64,
    /// Live ordering mode.
    pub current_mode: OrderingMode,
    /// The caller's QoS declaration.
    pub declared: QosOrdering,
    pub active_producers: usize,
    pub active_consumers: usize,
    /// Time since the last mode flip this sidecar issued.
    pub since_last_change: std::time::Duration,
}

/// Policy that decides when (and to which mode) the sidecar flips
/// a stamped ring's ordering flag. Mirrors [`RingShapePolicy`]:
/// `Some(mode)` triggers `set_ordering_mode(mode)`, `None` leaves
/// the flag alone.
pub trait OrderingPolicy: Send + Sync + 'static {
    fn decide(&self, observation: &OrderingPolicyObservation) -> Option<OrderingMode>;
}

/// Default ordering policy.
///
/// - Acts on the QoS declaration always: `GlobalFifo` arms
///   `MergeByStamp`; withdrawing to `PerProducer` disarms back to
///   `Unordered` (only when `auto_order_threshold` is unset - see
///   below).
/// - Acts on the inversion rate only when the caller pre-authorized
///   an automatic response by setting `auto_order_threshold`
///   (inversions/sec): under a `PerProducer` declaration, a rate
///   above the threshold arms `MergeByStamp`. The auto arm is
///   one-way - merged pops read zero inversions by construction, so
///   there is no symmetric signal to disarm on; disarming is the
///   caller's call (QoS declaration or an explicit
///   `set_ordering_mode`).
pub struct DefaultOrderingPolicy {
    pub hysteresis: std::time::Duration,
    pub auto_order_threshold: Option<f64>,
}

impl Default for DefaultOrderingPolicy {
    fn default() -> Self {
        Self {
            hysteresis: std::time::Duration::from_millis(100),
            auto_order_threshold: None,
        }
    }
}

impl OrderingPolicy for DefaultOrderingPolicy {
    fn decide(&self, obs: &OrderingPolicyObservation) -> Option<OrderingMode> {
        if obs.since_last_change < self.hysteresis {
            return None;
        }
        match obs.declared {
            QosOrdering::GlobalFifo => {
                if obs.current_mode == OrderingMode::Unordered {
                    Some(OrderingMode::MergeByStamp)
                } else {
                    None
                }
            }
            QosOrdering::PerProducer => {
                match self.auto_order_threshold {
                    Some(threshold) => {
                        if obs.current_mode == OrderingMode::Unordered
                            && obs.inversions_per_sec > threshold
                        {
                            Some(OrderingMode::MergeByStamp)
                        } else {
                            None
                        }
                    }
                    None => {
                        if obs.current_mode != OrderingMode::Unordered {
                            Some(OrderingMode::Unordered)
                        } else {
                            None
                        }
                    }
                }
            }
        }
    }
}

/// Background scanner thread that drives shape morphs on an
/// [`AdaptiveRing`] from a [`RingShapePolicy`].
///
/// `spawn` starts the thread; `shutdown` stops it. The thread
/// scans every `scan_interval`, builds a [`PolicyObservation`],
/// asks the policy, and calls [`AdaptiveRing::morph_to`] on
/// `Some(new_shape)` responses.
pub struct AdaptiveRingSidecar {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    morphs_triggered: Arc<std::sync::atomic::AtomicU64>,
    ordering_flips: Arc<std::sync::atomic::AtomicU64>,
}

impl AdaptiveRingSidecar {
    /// Spawn a sidecar thread that morphs `ring` according to
    /// `policy` decisions sampled every `scan_interval`.
    pub fn spawn<P: RingShapePolicy>(
        ring: Arc<AdaptiveRing>,
        policy: P,
        scan_interval: std::time::Duration,
    ) -> Self {
        Self::spawn_gated(
            ring,
            policy,
            scan_interval,
            crate::policy_gate::GateConfig::default(),
        )
    }

    /// As [`spawn`](Self::spawn) with a confidence gate between
    /// the shape policy's recommendation and the morph. Disabled
    /// (the default config) reproduces `spawn` exactly.
    pub fn spawn_gated<P: RingShapePolicy>(
        ring: Arc<AdaptiveRing>,
        policy: P,
        scan_interval: std::time::Duration,
        gate_cfg: crate::policy_gate::GateConfig,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let morphs_triggered = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let stop_c = stop.clone();
        let morphs_c = morphs_triggered.clone();
        let handle = std::thread::spawn(move || {
            let mut last_morph = std::time::Instant::now();
            let mut gate = crate::policy_gate::ConfidenceGate::new(gate_cfg);
            while !stop_c.load(Ordering::Acquire) {
                let obs = PolicyObservation {
                    active_producers: ring.active_producers(),
                    active_consumers: ring.active_consumers(),
                    current_shape: ring.current_shape(),
                    since_last_morph: last_morph.elapsed(),
                    stamped: ring.is_stamped(),
                };
                // Policy-driven morphs go through the internal morph:
                // a sidecar IS an automatic driver, so it must not
                // pin the shape the way an explicit morph_to does.
                if let Some(new_shape) = gate
                    .observe(policy.decide(&obs).map(|s| ring.contract_filtered_shape(s)))
                    && ring.morph_shape(new_shape).is_ok()
                {
                    last_morph = std::time::Instant::now();
                    morphs_c.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(scan_interval);
            }
        });

        Self {
            handle: Some(handle),
            stop,
            morphs_triggered,
            ordering_flips: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Spawn a sidecar that consults BOTH axes every scan tick: the
    /// shape policy (peer counts + the QoS ordering declaration, via
    /// [`QosRingShapePolicy`] or any custom [`RingShapePolicy`]) and
    /// the ordering policy (declaration + observed inversion rate).
    ///
    /// Per tick, on a stamped ring the sidecar additionally:
    /// - computes inversions/sec from the shared counter's delta,
    /// - ticks the drainer-lease epoch so a dead merge drainer
    ///   becomes preemptible after [`DRAINER_GRACE_EPOCHS`] scans,
    /// - applies the ordering policy's decision via
    ///   `set_ordering_mode` (counted in
    ///   [`ordering_flips`](Self::ordering_flips)).
    ///
    /// The shape axis is UNGATED by default: capacity-class morphs
    /// are cheap to reverse (the warm-backing path makes them
    /// microsecond-scale), so tracking load faithfully beats
    /// deliberating. The ordering AUTO-arm is GATED by default: the
    /// inversion-rate-driven `Unordered -> MergeByStamp` flip is
    /// one-way (merged pops read zero inversions, so there is no
    /// symmetric signal to walk it back), and a one-way decision
    /// taken on a single noisy scan is unrecoverable. The gate
    /// makes the auto-arm demand sustained inversions before it
    /// commits. Explicit caller declarations (`GlobalFifo` arm,
    /// declaration withdrawal) are NOT noise and fire immediately -
    /// only the auto-detected arm is deliberated.
    ///
    /// `spawn_with_qos_gated` overrides both axes with one explicit
    /// config (disabled reproduces the fully-ungated behavior).
    pub fn spawn_with_qos<P: RingShapePolicy, O: OrderingPolicy>(
        ring: Arc<AdaptiveRing>,
        shape_policy: P,
        ordering_policy: O,
        qos: Arc<crate::qos_policy::QosPolicy>,
        scan_interval: std::time::Duration,
    ) -> Self {
        Self::spawn_with_qos_core(
            ring,
            shape_policy,
            ordering_policy,
            qos,
            scan_interval,
            crate::policy_gate::GateConfig::default(),
            crate::policy_gate::GateConfig::enabled_with_arity(2),
        )
    }

    /// As [`spawn_with_qos`](Self::spawn_with_qos) with confidence
    /// gates on BOTH axes set from one explicit config - a shape
    /// gate and an ordering-auto-arm gate (each accumulates its own
    /// conviction; a peer-count change shocks both). `GateConfig::default()`
    /// (disabled) reproduces the fully-ungated sidecar; an enabled
    /// config gates the shape morph AND the ordering auto-arm.
    /// Explicit ordering declarations always fire immediately
    /// regardless of config - the gate governs the auto-detected
    /// arm only.
    pub fn spawn_with_qos_gated<P: RingShapePolicy, O: OrderingPolicy>(
        ring: Arc<AdaptiveRing>,
        shape_policy: P,
        ordering_policy: O,
        qos: Arc<crate::qos_policy::QosPolicy>,
        scan_interval: std::time::Duration,
        gate_cfg: crate::policy_gate::GateConfig,
    ) -> Self {
        Self::spawn_with_qos_core(
            ring,
            shape_policy,
            ordering_policy,
            qos,
            scan_interval,
            gate_cfg,
            gate_cfg,
        )
    }

    fn spawn_with_qos_core<P: RingShapePolicy, O: OrderingPolicy>(
        ring: Arc<AdaptiveRing>,
        shape_policy: P,
        ordering_policy: O,
        qos: Arc<crate::qos_policy::QosPolicy>,
        scan_interval: std::time::Duration,
        shape_gate_cfg: crate::policy_gate::GateConfig,
        order_gate_cfg: crate::policy_gate::GateConfig,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let morphs_triggered = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ordering_flips = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let stop_c = stop.clone();
        let morphs_c = morphs_triggered.clone();
        let flips_c = ordering_flips.clone();
        let handle = std::thread::spawn(move || {
            let mut last_morph = std::time::Instant::now();
            let mut last_flip = std::time::Instant::now();
            let mut last_inversions = ring.inversions();
            let mut last_scan = std::time::Instant::now();
            let mut shape_gate = crate::policy_gate::ConfidenceGate::new(shape_gate_cfg);
            let mut order_gate = crate::policy_gate::ConfidenceGate::new(order_gate_cfg);
            let mut last_peers = (0usize, 0usize);
            let mut first_scan = true;
            while !stop_c.load(Ordering::Acquire) {
                let obs = PolicyObservation {
                    active_producers: ring.active_producers(),
                    active_consumers: ring.active_consumers(),
                    current_shape: ring.current_shape(),
                    since_last_morph: last_morph.elapsed(),
                    stamped: ring.is_stamped(),
                };
                let peers = (obs.active_producers, obs.active_consumers);
                if !first_scan && peers != last_peers {
                    shape_gate.shock();
                    order_gate.shock();
                }
                last_peers = peers;
                first_scan = false;

                if let Some(new_shape) = shape_gate
                    .observe(shape_policy.decide(&obs).map(|s| ring.contract_filtered_shape(s)))
                    && ring.morph_shape(new_shape).is_ok()
                {
                    last_morph = std::time::Instant::now();
                    morphs_c.fetch_add(1, Ordering::Relaxed);
                }

                if let Some(current_mode) = ring.ordering_mode() {
                    ring.tick_drainer_epoch().ok();

                    let now_inversions = ring.inversions();
                    let elapsed = last_scan.elapsed().as_secs_f64().max(1e-9);
                    let rate = now_inversions
                        .saturating_sub(last_inversions) as f64 / elapsed;
                    last_inversions = now_inversions;
                    last_scan = std::time::Instant::now();

                    let declared = qos.ordering();
                    let ord_obs = OrderingPolicyObservation {
                        inversions_per_sec: rate,
                        current_mode,
                        declared,
                        active_producers: obs.active_producers,
                        active_consumers: obs.active_consumers,
                        since_last_change: last_flip.elapsed(),
                    };
                    let decision = ordering_policy
                        .decide(&ord_obs)
                        .filter(|m| *m != current_mode);

                    // The auto-arm is the one-way, noise-prone
                    // decision: a `PerProducer` declaration (no
                    // global-order intent) that the inversion rate
                    // nonetheless pushes to `MergeByStamp`. That is
                    // the only ordering decision the gate governs.
                    // An explicit `GlobalFifo` arm and any disarm
                    // are caller intent, not noise - they bypass the
                    // gate and fire immediately.
                    let is_auto_arm = declared == QosOrdering::PerProducer
                        && decision == Some(OrderingMode::MergeByStamp);
                    let gated = if is_auto_arm {
                        order_gate.observe(decision)
                    } else {
                        decision
                    };
                    if let Some(new_mode) = gated
                        && ring.set_ordering_mode(new_mode).is_ok()
                    {
                        last_flip = std::time::Instant::now();
                        flips_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
                std::thread::sleep(scan_interval);
            }
        });

        Self {
            handle: Some(handle),
            stop,
            morphs_triggered,
            ordering_flips,
        }
    }

    /// Number of successful morph_to calls the sidecar has issued
    /// since spawn.
    pub fn morphs_triggered(&self) -> u64 {
        self.morphs_triggered.load(Ordering::Acquire)
    }

    /// Number of ordering-mode flips this sidecar has issued since
    /// spawn (always 0 for [`spawn`](Self::spawn)).
    pub fn ordering_flips(&self) -> u64 {
        self.ordering_flips.load(Ordering::Acquire)
    }

    /// Stop the scanner thread and join it.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.join().ok();
        }
    }
}

impl Drop for AdaptiveRingSidecar {
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

    #[test]
    fn create_starts_in_spsc_shape() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        assert_eq!(ring.current_shape(), RingShape::Spsc);
        assert_eq!(ring.pin_generation(), 0);
    }

    #[test]
    fn adaptive_dispatch_round_trip_each_shape() {
        for shape in [RingShape::Spsc, RingShape::Mpsc, RingShape::Mpmc, RingShape::Vyukov] {
            let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
            ring.shape_tag.store(shape as u8, Ordering::Release);
            // 56 = ADAPTIVE_VYUKOV_PAYLOAD_BYTES, the smaller of the two
            // backings' slot sizes (Vyukov's 8B per-slot sequence eats
            // 8 of the 64B slot; Lamport gets the full 64B).
            let payload = [0xCDu8; 56];
            ring.try_send(0, &payload).unwrap();
            let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            let n = ring.try_recv(0, &mut out).unwrap();
            assert!(n > 0, "shape {:?} delivered zero bytes", shape);
            assert_eq!(&out[..payload.len()], &payload[..]);
        }
    }

    #[test]
    fn pin_captures_shape_and_generation() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        let pinned = ring.pin_current_shape();
        assert_eq!(pinned.shape(), RingShape::Spsc);
        assert!(pinned.is_still_valid());
        // Re-pin: still valid because no morph happened.
        let pinned = ring.pin_current_shape();
        assert!(pinned.is_still_valid());
    }

    #[test]
    fn morph_invalidates_outstanding_pin() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        let pinned = ring.pin_current_shape();
        assert!(pinned.is_still_valid());

        ring.morph_to(RingShape::Mpsc).unwrap();
        assert!(!pinned.is_still_valid(),
                "pin must invalidate after morph_to");
        assert_eq!(ring.current_shape(), RingShape::Mpsc);
    }

    #[test]
    fn morph_to_same_shape_is_no_op() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        let gen_before = ring.pin_generation();
        ring.morph_to(RingShape::Spsc).unwrap();
        let gen_after = ring.pin_generation();
        assert_eq!(gen_before, gen_after,
                   "morph_to(same shape) must not bump pin_generation");
    }

    #[test]
    fn morph_preserves_in_flight_items_via_stale_walk() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();

        // Push 3 items via the SPSC shape.
        for i in 0..3u32 {
            let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&i.to_le_bytes());
            ring.try_send(0, &buf).unwrap();
        }

        // Morph to MPSC: no data moves; the SPSC backing becomes
        // the stale backing and the pop path drains it first.
        ring.morph_to(RingShape::Mpsc).unwrap();
        assert_eq!(ring.current_shape(), RingShape::Mpsc);
        assert_eq!(ring.approx_len(), 3,
                   "the stale backlog must stay visible through approx_len");

        // New traffic lands in the new shape while the backlog is
        // still pending; the stale walk delivers old-before-new.
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        buf[..4].copy_from_slice(&99u32.to_le_bytes());
        ring.try_send(0, &buf).unwrap();

        let mut seen = Vec::new();
        let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        while ring.try_recv(0, &mut out).is_ok() {
            seen.push(u32::from_le_bytes(out[..4].try_into().unwrap()));
        }
        assert_eq!(seen, vec![0u32, 1, 2, 99],
                   "stale backlog must drain before post-morph items");
        assert!(ring.is_empty());
    }

    #[test]
    fn frame_round_trip_all_shapes() {
        use crate::frame_ring::FrameClass;
        for shape in [RingShape::Spsc, RingShape::Mpsc, RingShape::Mpmc, RingShape::Vyukov] {
            let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
            if shape != RingShape::Spsc {
                ring.morph_to(shape).unwrap();
            }
            let small = b"small inline payload".to_vec();
            let large = vec![0xABu8; 4000];
            assert_eq!(ring.send_frame(0, &small).unwrap(), FrameClass::Inline,
                       "{shape:?} small should inline");
            assert_eq!(ring.send_frame(0, &large).unwrap(), FrameClass::Offset,
                       "{shape:?} large should offset");
            let mut out = Vec::new();
            assert_eq!(ring.recv_frame(0, &mut out).unwrap(), FrameClass::Inline);
            assert_eq!(out, small, "{shape:?} small round-trip");
            assert_eq!(ring.recv_frame(0, &mut out).unwrap(), FrameClass::Offset);
            assert_eq!(out, large, "{shape:?} large round-trip");
        }
    }

    #[test]
    fn frame_survives_morph() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        // SPSC: one inline, one offset.
        ring.send_frame(0, b"pre-morph small").unwrap();
        ring.send_frame(0, &vec![1u8; 3000]).unwrap();
        // Morph to MPSC: the SPSC backing becomes stale and drains
        // first; the frame descriptors and region blocks are
        // shape-independent so the records survive the morph intact.
        ring.morph_to(RingShape::Mpsc).unwrap();
        ring.send_frame(0, b"post-morph small").unwrap();
        ring.send_frame(0, &vec![2u8; 3000]).unwrap();
        let mut out = Vec::new();
        ring.recv_frame(0, &mut out).unwrap();
        assert_eq!(out, b"pre-morph small");
        ring.recv_frame(0, &mut out).unwrap();
        assert_eq!(out, vec![1u8; 3000]);
        ring.recv_frame(0, &mut out).unwrap();
        assert_eq!(out, b"post-morph small");
        ring.recv_frame(0, &mut out).unwrap();
        assert_eq!(out, vec![2u8; 3000]);
    }

    #[test]
    fn frame_override_and_limits() {
        use crate::frame_ring::{FrameClass, LayoutHint};
        let ring = AdaptiveRing::create_anon(2, 2, 64).unwrap();
        let mut out = Vec::new();
        // ForceOffset spills a small payload to the region.
        assert_eq!(ring.send_frame_as(0, b"tiny", LayoutHint::ForceOffset).unwrap(),
                   FrameClass::Offset);
        assert_eq!(ring.recv_frame(0, &mut out).unwrap(), FrameClass::Offset);
        assert_eq!(out, b"tiny");
        // ForceInline rejects an over-budget payload.
        let big = vec![0u8; AdaptiveRing::FRAME_INLINE_BUDGET + 1];
        assert_eq!(ring.send_frame_as(0, &big, LayoutHint::ForceInline).unwrap_err(),
                   RingError::PayloadTooLarge);
        // Auto inlines exactly at the budget.
        let at = vec![7u8; AdaptiveRing::FRAME_INLINE_BUDGET];
        assert_eq!(ring.send_frame(0, &at).unwrap(), FrameClass::Inline);
        ring.recv_frame(0, &mut out).unwrap();
        assert_eq!(out, at);
    }

    #[test]
    fn frame_rejected_on_stamped_ring() {
        // Frames and ordering stamps both claim the slot head, so the
        // frame path is refused on a stamped ring.
        let ring = AdaptiveRing::create_anon(2, 2, 64)
            .unwrap()
            .with_ordering_stamps()
            .unwrap();
        assert_eq!(ring.send_frame(0, b"x").unwrap_err(), RingError::LayoutMismatch);
        let mut out = Vec::new();
        assert_eq!(ring.recv_frame(0, &mut out).unwrap_err(), RingError::LayoutMismatch);
    }

    #[test]
    fn frame_vyukov_two_thread_mixed_size() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrd};
        use std::thread;

        const PER: u32 = 5_000;
        const PRODUCERS: u32 = 2;
        let total = (PER * PRODUCERS) as usize;

        // Vyukov is the true-MPMC shape (one SharedRing, per-slot
        // sequence CAS), safe for many producers AND many consumers
        // with no partitioning. This exercises the shared payload
        // region under concurrent alloc (producers) and free
        // (consumers) at once.
        let ring = Arc::new(AdaptiveRing::create_anon(2, 2, 256).unwrap());
        ring.morph_to(RingShape::Vyukov).unwrap();
        // Each item carries its global id so a consumer can verify the
        // record regardless of which consumer drained it.
        let seen: Arc<Vec<AtomicBool>> =
            Arc::new((0..total).map(|_| AtomicBool::new(false)).collect());
        let received = Arc::new(AtomicUsize::new(0));

        let mut prods = Vec::new();
        for p in 0..PRODUCERS {
            let ring = ring.clone();
            prods.push(thread::spawn(move || {
                for i in 0..PER {
                    let id = p * PER + i;
                    let len = (id as usize % 200) + 4; // 4..203, crosses the budget
                    let mut payload = vec![0u8; len];
                    payload[0..4].copy_from_slice(&id.to_le_bytes());
                    for k in 4..len {
                        payload[k] = id.wrapping_add(k as u32) as u8;
                    }
                    while ring.send_frame(p as usize, &payload).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        let mut cons = Vec::new();
        for c in 0..2usize {
            let ring = ring.clone();
            let seen = seen.clone();
            let received = received.clone();
            cons.push(thread::spawn(move || {
                let mut out = Vec::new();
                while received.load(AtOrd::Acquire) < total {
                    if ring.recv_frame(c, &mut out).is_ok() {
                        let id = u32::from_le_bytes(out[0..4].try_into().unwrap());
                        let len = (id as usize % 200) + 4;
                        assert_eq!(out.len(), len, "id {id} length");
                        for k in 4..len {
                            assert_eq!(out[k], id.wrapping_add(k as u32) as u8,
                                       "id {id} byte {k}");
                        }
                        let already = seen[id as usize].swap(true, AtOrd::AcqRel);
                        assert!(!already, "id {id} delivered twice");
                        received.fetch_add(1, AtOrd::AcqRel);
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        for p in prods { p.join().unwrap(); }
        for c in cons { c.join().unwrap(); }
        assert_eq!(received.load(AtOrd::Acquire), total);
        assert!(seen.iter().all(|b| b.load(AtOrd::Acquire)),
                "every id delivered exactly once");
    }

    #[test]
    fn second_morph_blocked_until_stale_backlog_drains() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        ring.try_send(0, &[7u8; 8]).unwrap();
        ring.morph_to(RingShape::Mpsc).unwrap();

        // The SPSC backlog has not drained; another morph must wait.
        assert_eq!(ring.morph_to(RingShape::Mpmc).unwrap_err(),
                   RingError::StaleBacklog);

        let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        ring.try_recv(0, &mut out).unwrap();
        // Drained: the next morph proceeds.
        ring.morph_to(RingShape::Mpmc).unwrap();
        assert_eq!(ring.current_shape(), RingShape::Mpmc);
    }

    #[test]
    fn register_producer_grows_past_hint_and_recycles_slots() {
        let ring = AdaptiveRing::create_anon(3, 1, 64).unwrap();
        let id0 = ring.register_producer().unwrap();
        let id1 = ring.register_producer().unwrap();
        let id2 = ring.register_producer().unwrap();
        assert_eq!((id0, id1, id2), (0, 1, 2));

        // Past the construction hint the ring GROWS instead of
        // erroring: a 4th producer gets slot 3 and a live backing.
        let id3 = ring.register_producer().unwrap();
        assert_eq!(id3, 3);
        assert_eq!(ring.published_producers(), 4);
        ring.try_send(id3, &7u64.to_le_bytes()).unwrap();
        let mut out = [0u8; 64];
        // 4P/0C: no consumer registered, shape stays wherever the
        // counts left it; the adaptive pop still drains slot 3's
        // backing via the current shape + stale walk.
        let _c = ring.register_consumer().unwrap();
        let n = ring.try_recv(0, &mut out).unwrap();
        assert!(n >= 8, "popped record too short: {n}");
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);

        // Unregister frees the SLOT (bitmap claim): the next register
        // reuses id 1 without colliding with the still-live 2 and 3.
        ring.unregister_producer(id1);
        assert_eq!(ring.register_producer().unwrap(), 1);

        // Errors exist ONLY under a caller-declared contract pin.
        let pinned = AdaptiveRing::create_anon(2, 1, 64)
            .unwrap()
            .with_contract(crate::ring_contract::RingContract::from_counts(2, 1));
        pinned.register_producer().unwrap();
        pinned.register_producer().unwrap();
        assert_eq!(pinned.register_producer().unwrap_err(),
                   AdaptiveError::TooManyProducers);
    }

    #[test]
    fn default_policy_target_shape_per_peer_count() {
        // Idle either side -> no target.
        assert_eq!(DefaultRingShapePolicy::target_shape(0, 1), None);
        assert_eq!(DefaultRingShapePolicy::target_shape(1, 0), None);
        assert_eq!(DefaultRingShapePolicy::target_shape(0, 0), None);
        // 1P/1C -> SPSC
        assert_eq!(DefaultRingShapePolicy::target_shape(1, 1), Some(RingShape::Spsc));
        // NP/1C -> MPSC
        assert_eq!(DefaultRingShapePolicy::target_shape(2, 1), Some(RingShape::Mpsc));
        assert_eq!(DefaultRingShapePolicy::target_shape(8, 1), Some(RingShape::Mpsc));
        // */NC (NC >= 2) -> MPMC
        assert_eq!(DefaultRingShapePolicy::target_shape(1, 2), Some(RingShape::Mpmc));
        assert_eq!(DefaultRingShapePolicy::target_shape(4, 4), Some(RingShape::Mpmc));
    }

    #[test]
    fn default_policy_returns_none_during_hysteresis() {
        let policy = DefaultRingShapePolicy {
            hysteresis: std::time::Duration::from_secs(1),
        };
        let obs = PolicyObservation {
            active_producers: 4,
            active_consumers: 4,
            current_shape: RingShape::Spsc,
            since_last_morph: std::time::Duration::from_millis(50),
            stamped: false,
        };
        // Target would be MPMC, but hysteresis says wait.
        assert_eq!(policy.decide(&obs), None);
    }

    #[test]
    fn default_policy_returns_target_after_hysteresis() {
        let policy = DefaultRingShapePolicy {
            hysteresis: std::time::Duration::from_millis(10),
        };
        let obs = PolicyObservation {
            active_producers: 4,
            active_consumers: 4,
            current_shape: RingShape::Spsc,
            since_last_morph: std::time::Duration::from_secs(1),
            stamped: false,
        };
        assert_eq!(policy.decide(&obs), Some(RingShape::Mpmc));
    }

    #[test]
    fn default_policy_returns_none_when_target_equals_current() {
        let policy = DefaultRingShapePolicy::default();
        let obs = PolicyObservation {
            active_producers: 1,
            active_consumers: 1,
            current_shape: RingShape::Spsc,
            since_last_morph: std::time::Duration::from_secs(1),
            stamped: false,
        };
        assert_eq!(policy.decide(&obs), None);
    }

    #[test]
    fn shape_tracks_peer_counts_and_sidecar_stays_idle() {
        let ring = Arc::new(AdaptiveRing::create_anon(4, 4, 64).unwrap());
        let policy = DefaultRingShapePolicy {
            hysteresis: std::time::Duration::from_millis(5),
        };
        let sidecar = AdaptiveRingSidecar::spawn(
            ring.clone(),
            policy,
            std::time::Duration::from_millis(10),
        );

        // Register 1P+1C -> SPSC (already the initial shape).
        let _p0 = ring.register_producer().unwrap();
        let _c0 = ring.register_consumer().unwrap();
        assert_eq!(ring.current_shape(), RingShape::Spsc);

        // The register path itself morphs SYNCHRONOUSLY - no scan
        // interval to wait out, no sidecar required.
        let _p1 = ring.register_producer().unwrap();
        assert_eq!(ring.current_shape(), RingShape::Mpsc,
                   "2nd producer registration must morph to MPSC immediately");

        let _c1 = ring.register_consumer().unwrap();
        assert_eq!(ring.current_shape(), RingShape::Mpmc,
                   "2nd consumer registration must morph to MPMC immediately");

        // The sidecar observed a ring whose shape already tracked its
        // counts at every scan: it never had a correction to make.
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert_eq!(sidecar.morphs_triggered(), 0,
                   "register-path morphs left the sidecar nothing to do");

        // Leaves shrink the shape too: back down to 1P/1C -> SPSC
        // (the stale walk drains the composed backings; empty here).
        ring.unregister_consumer(1);
        ring.unregister_producer(1);
        assert_eq!(ring.current_shape(), RingShape::Spsc,
                   "unregister must morph back down automatically");

        sidecar.shutdown();
    }

    #[test]
    fn pinned_native_paths_match_adaptive_paths() {
        let ring = AdaptiveRing::create_anon(4, 4, 64).unwrap();
        let pinned = ring.pin_current_shape();
        assert_eq!(pinned.shape(), RingShape::Spsc);

        let payload = [0xAAu8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        pinned.spsc_try_push(&payload).unwrap();
        let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        let n = pinned.spsc_try_pop(&mut out).unwrap();
        assert_eq!(n, ADAPTIVE_SPSC_PAYLOAD_BYTES);
        assert_eq!(out, payload);
        assert!(pinned.is_still_valid());
    }

    // ===============================================================
    // Ordering-axis tests
    // ===============================================================

    fn stamped_anon(
        max_producers: usize,
        max_consumers: usize,
        kind: StampKind,
    ) -> AdaptiveRing {
        AdaptiveRing::create_anon(max_producers, max_consumers, 64)
            .unwrap()
            .with_ordering_stamps_kind(kind)
            .unwrap()
    }

    #[test]
    fn stamped_round_trip_strips_stamp_and_caps_payload() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        assert!(ring.is_stamped());
        assert_eq!(ring.stamp_kind(), Some(StampKind::SharedCounter));
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::Unordered));

        // 57 bytes exceed the stamped cap.
        let too_big = [0u8; STAMPED_PAYLOAD_BYTES + 1];
        assert_eq!(ring.try_send(0, &too_big).unwrap_err(),
                   RingError::PayloadTooLarge);

        let payload = [0xC3u8; STAMPED_PAYLOAD_BYTES];
        ring.try_send(0, &payload).unwrap();
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        let n = ring.try_recv(0, &mut out).unwrap();
        assert_eq!(n, STAMPED_PAYLOAD_BYTES,
                   "stamped recv returns payload bytes only");
        assert_eq!(out, payload, "the stamp must be stripped, not leak into the payload");
    }

    #[test]
    fn unstamped_ring_rejects_ordering_calls() {
        let ring = AdaptiveRing::create_anon(2, 1, 64).unwrap();
        assert!(!ring.is_stamped());
        assert_eq!(ring.inversions(), 0);
        assert_eq!(ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap_err(),
                   RingError::NotStamped);
        assert_eq!(ring.refresh_watermark(0).unwrap_err(), RingError::NotStamped);
        let pinned = ring.pin_current_shape();
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        assert_eq!(pinned.ordered_try_pop(0, &mut out).unwrap_err(),
                   RingError::NotStamped);
        assert_eq!(pinned.stamped_try_push(0, &[1u8; 8]).unwrap_err(),
                   RingError::NotStamped);
    }

    #[test]
    fn stamped_ring_rejects_vyukov_morph_and_vyukov_ring_rejects_stamps() {
        let ring = stamped_anon(2, 1, StampKind::Monotonic);
        assert_eq!(ring.morph_to(RingShape::Vyukov).unwrap_err(),
                   RingError::LayoutMismatch);

        let vyukov_first = AdaptiveRing::create_anon(2, 1, 64).unwrap();
        vyukov_first.morph_to(RingShape::Vyukov).unwrap();
        assert!(matches!(
            vyukov_first.with_ordering_stamps(),
            Err(RingError::LayoutMismatch)
        ));
    }

    #[test]
    fn synthetic_interleave_fires_inversion_counter() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();

        // Producer 1 pushes FIRST (older stamp lands in ring 1),
        // then producer 0 (newer stamp in ring 0). The round-robin
        // drain starts at ring 0, so the consumer pops newer-then-
        // older: exactly one cross-producer inversion.
        ring.try_send(1, &1u64.to_le_bytes()).unwrap();
        ring.try_send(0, &2u64.to_le_bytes()).unwrap();

        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        ring.try_recv(0, &mut out).unwrap();
        assert_eq!(ring.inversions(), 0, "first pop has no predecessor");
        ring.try_recv(0, &mut out).unwrap();
        assert_eq!(ring.inversions(), 1,
                   "older-after-newer must count as one inversion");
    }

    #[test]
    fn merge_mode_delivers_global_stamp_order() {
        let ring = stamped_anon(4, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();

        // Interleave 32 items across 4 producers in a single thread:
        // counter stamps make the push order the global order.
        for i in 0..32u64 {
            let producer = (i % 4) as usize;
            ring.try_send(producer, &i.to_le_bytes()).unwrap();
        }

        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        for expected in 0..32u64 {
            let n = ring.try_recv(0, &mut out).unwrap();
            assert_eq!(n, STAMPED_PAYLOAD_BYTES);
            let got = u64::from_le_bytes(out[..8].try_into().unwrap());
            assert_eq!(got, expected,
                       "merge pop must deliver global push order");
        }
        assert_eq!(ring.try_recv(0, &mut out).unwrap_err(), RingError::Empty);
        assert_eq!(ring.inversions(), 0,
                   "merged pops must observe zero inversions");
    }

    #[test]
    fn flag_flip_orders_backlog_retroactively_without_loss() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();

        // Backlog pushed UNDER Unordered, interleaved so the
        // round-robin drain would invert.
        for i in 0..16u64 {
            let producer = ((i + 1) % 2) as usize;
            ring.try_send(producer, &i.to_le_bytes()).unwrap();
        }

        // Pop two items unordered; the second is an inversion on
        // this interleave (ring 0 holds the odd/newer stamps).
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        let mut popped = Vec::new();
        for _ in 0..2 {
            ring.try_recv(0, &mut out).unwrap();
            popped.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        let inversions_before_flip = ring.inversions();
        assert!(inversions_before_flip > 0,
                "unordered interleave must show inversions before the flip");

        // The ordered switch: one store, no drain, retroactive over
        // the 14-item backlog because the stamps were already there.
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();
        let mut merged = Vec::new();
        while let Ok(_n) = ring.try_recv(0, &mut out) {
            merged.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }

        // Zero loss across the transition...
        let mut all = popped.clone();
        all.extend(&merged);
        all.sort_unstable();
        assert_eq!(all, (0..16u64).collect::<Vec<_>>(),
                   "no item may be lost across the mode flip");
        // ...and the post-flip stream is globally ordered (strictly
        // increasing payload sequence = strictly increasing stamps).
        for pair in merged.windows(2) {
            assert!(pair[0] < pair[1],
                    "post-flip pops must be globally ordered: {merged:?}");
        }
        assert_eq!(ring.inversions(), inversions_before_flip,
                   "the flip itself and merged pops must add zero inversions");
    }

    #[test]
    fn merge_strict_blocks_on_in_flight_stamp_then_releases() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();
        ring.set_ordering_mode(OrderingMode::MergeStrict).unwrap();
        let region = ring.ordering_region().unwrap();

        // Producer 1 stamps but stalls before pushing (the
        // stamp-to-publish window): issued advances, watermark
        // does not.
        let stalled_stamp = region.next_stamp(1);
        // Producer 0 stamps later and publishes.
        ring.try_send(0, &42u64.to_le_bytes()).unwrap();

        // In-flight gate: producer 0's visible item must NOT
        // release while producer 1 holds a smaller in-flight stamp.
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        assert_eq!(ring.try_recv(0, &mut out).unwrap_err(), RingError::Empty,
                   "strict merge must hold the candidate while a smaller stamp is in flight");

        // The stalled push resolves as Full-equivalent: the
        // watermark advances to the issued stamp ("this will never
        // publish"), clearing the in-flight gate. The strict
        // watermark gate still holds the candidate (producer 1's
        // empty ring has not vouched past the candidate's stamp)...
        region.publish_watermark(1, stalled_stamp);
        assert_eq!(ring.try_recv(0, &mut out).unwrap_err(), RingError::Empty,
                   "strict watermark gate must hold until the silent producer vouches");
        // ...until the idle producer heartbeats its watermark past
        // the candidate.
        ring.refresh_watermark(1).unwrap();
        let n = ring.try_recv(0, &mut out).unwrap();
        assert_eq!(n, STAMPED_PAYLOAD_BYTES);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 42);
    }

    #[test]
    fn merge_by_stamp_in_flight_gate_blocks_descheduled_producer() {
        // The WSL-discovered case: a producer reserves/stamps, then
        // stalls (preemption) before publishing. MergeByStamp must
        // hold any larger candidate until the publish lands - a
        // fixed freshness window cannot bound a deschedule.
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();
        let region = ring.ordering_region().unwrap();

        let stalled = region.next_stamp(1); // stamped, never pushed
        ring.try_send(0, &9u64.to_le_bytes()).unwrap();

        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        assert_eq!(ring.try_recv(0, &mut out).unwrap_err(), RingError::Empty,
                   "MergeByStamp must gate on in-flight stamps too");
        region.publish_watermark(1, stalled); // the stall resolves
        let n = ring.try_recv(0, &mut out).unwrap();
        assert_eq!(n, STAMPED_PAYLOAD_BYTES);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 9);
    }

    #[test]
    fn merge_strict_retired_producer_stops_gating() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();
        ring.set_ordering_mode(OrderingMode::MergeStrict).unwrap();

        // Producer 1 pushes once (its slot is in-use), the item is
        // consumed, and the producer goes silent with an old
        // watermark.
        ring.try_send(1, &1u64.to_le_bytes()).unwrap();
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        ring.try_recv(0, &mut out).unwrap();

        // Producer 0's newer item is gated on producer 1's silence.
        ring.try_send(0, &2u64.to_le_bytes()).unwrap();
        assert_eq!(ring.try_recv(0, &mut out).unwrap_err(), RingError::Empty,
                   "strict couples release to the slowest in-use producer");

        // Clean exit: retirement saturates the slot's watermark and
        // the candidate releases - permanently, no heartbeat needed.
        ring.retire_producer(1).unwrap();
        let n = ring.try_recv(0, &mut out).unwrap();
        assert_eq!(n, STAMPED_PAYLOAD_BYTES);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 2);
    }

    #[test]
    fn multi_consumer_merge_enforces_single_drainer() {
        let ring = stamped_anon(2, 2, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpmc).unwrap();
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();

        for i in 0..4u64 {
            ring.try_send((i % 2) as usize, &i.to_le_bytes()).unwrap();
        }

        // Consumer 0 pops first and thereby auto-acquires the lease.
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        ring.try_recv(0, &mut out).unwrap();
        // Consumer 1 is locked out while consumer 0 holds the lease.
        assert_eq!(ring.try_recv(1, &mut out).unwrap_err(),
                   RingError::NotDrainer);
        // Voluntary release hands the drain over.
        assert!(ring.release_drainer(0).unwrap());
        let n = ring.try_recv(1, &mut out).unwrap();
        assert_eq!(n, STAMPED_PAYLOAD_BYTES);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 1,
                   "the new drainer continues in global stamp order");
        // And consumer 0 is now locked out in turn.
        assert_eq!(ring.try_recv(0, &mut out).unwrap_err(),
                   RingError::NotDrainer);
    }

    #[test]
    fn mode_flip_does_not_invalidate_pins() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        ring.morph_to(RingShape::Mpsc).unwrap();
        let pinned = ring.pin_current_shape();
        assert!(pinned.is_still_valid());

        // Interleaved stamped pushes through the pin.
        pinned.stamped_try_push(1, &1u64.to_le_bytes()).unwrap();
        pinned.stamped_try_push(0, &2u64.to_le_bytes()).unwrap();

        // Flip the merge flag under the live pin: the pin survives
        // (no generation bump) and the pinned pop consults the mode
        // atom, so the next pops come out merged.
        ring.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();
        assert!(pinned.is_still_valid(),
                "ordering-mode flips must not invalidate pins");

        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        pinned.ordered_try_pop(0, &mut out).unwrap();
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 1,
                   "pinned merge pop must deliver stamp order");
        pinned.ordered_try_pop(0, &mut out).unwrap();
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 2);
    }

    #[test]
    fn stamped_items_survive_shape_morphs() {
        let ring = stamped_anon(2, 1, StampKind::SharedCounter);
        for i in 0..3u64 {
            ring.try_send(0, &i.to_le_bytes()).unwrap();
        }
        ring.morph_to(RingShape::Mpsc).unwrap();
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        let mut got = Vec::new();
        while ring.try_recv(0, &mut out).is_ok() {
            got.push(u64::from_le_bytes(out[..8].try_into().unwrap()));
        }
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2],
                   "stamped slots must transfer intact across shape morphs");
    }

    #[test]
    fn stamped_file_ring_open_adopts_creator_kind_and_shares_mode() {
        let mut prefix = std::env::temp_dir();
        prefix.push(format!(
            "subetha_stamped_open_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos()).unwrap_or(0),
        ));

        let creator = AdaptiveRing::create(&prefix, 2, 1, 64)
            .unwrap()
            .with_ordering_stamps_kind(StampKind::SharedCounter)
            .unwrap();
        creator.set_ordering_mode(OrderingMode::MergeByStamp).unwrap();
        creator.try_send(0, &7u64.to_le_bytes()).unwrap();

        let opener = AdaptiveRing::open(&prefix, 2, 1, 64)
            .unwrap()
            .with_ordering_stamps()
            .unwrap();
        assert_eq!(opener.stamp_kind(), Some(StampKind::SharedCounter),
                   "opener must adopt the creator's stamp kind");
        assert_eq!(opener.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "the mode flag must be cross-process (region-resident)");
        // Explicit mismatched kind on open is a layout error.
        assert!(matches!(
            AdaptiveRing::open(&prefix, 2, 1, 64)
                .unwrap()
                .with_ordering_stamps_kind(StampKind::Monotonic),
            Err(RingError::LayoutMismatch)
        ));

        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        let n = opener.try_recv(0, &mut out).unwrap();
        assert_eq!(n, STAMPED_PAYLOAD_BYTES);
        assert_eq!(u64::from_le_bytes(out[..8].try_into().unwrap()), 7);

        drop(creator);
        drop(opener);
        for suffix in [".spsc.bin", ".mpsc.0.bin", ".mpsc.1.bin",
                       ".mpmc.0.bin", ".mpmc.1.bin", ".vyukov.bin",
                       ".ordering.bin"] {
            let mut p = prefix.as_os_str().to_owned();
            p.push(suffix);
            std::fs::remove_file(std::path::PathBuf::from(p)).ok();
        }
    }

    #[test]
    fn qos_shape_policy_decision_matrix() {
        let qos = Arc::new(crate::qos_policy::QosPolicy::default());
        let policy = QosRingShapePolicy {
            qos: qos.clone(),
            hysteresis: std::time::Duration::from_millis(0),
        };
        let obs = |shape, stamped| PolicyObservation {
            active_producers: 2,
            active_consumers: 1,
            current_shape: shape,
            since_last_morph: std::time::Duration::from_secs(1),
            stamped,
        };

        // PerProducer: counts-based default (2P/1C -> MPSC).
        assert_eq!(policy.decide(&obs(RingShape::Spsc, false)),
                   Some(RingShape::Mpsc));
        assert_eq!(policy.decide(&obs(RingShape::Mpsc, false)), None);

        // GlobalFifo + unstamped: Vyukov morph.
        qos.set_ordering(crate::qos_policy::Ordering::GlobalFifo);
        assert_eq!(policy.decide(&obs(RingShape::Mpsc, false)),
                   Some(RingShape::Vyukov));
        assert_eq!(policy.decide(&obs(RingShape::Vyukov, false)), None);

        // GlobalFifo + stamped: shape stays counts-based composed
        // (the merge flag serves the declaration).
        assert_eq!(policy.decide(&obs(RingShape::Spsc, true)),
                   Some(RingShape::Mpsc));
        assert_eq!(policy.decide(&obs(RingShape::Mpsc, true)), None);

        // Withdrawing the declaration walks Vyukov back.
        qos.set_ordering(crate::qos_policy::Ordering::PerProducer);
        assert_eq!(policy.decide(&obs(RingShape::Vyukov, false)),
                   Some(RingShape::Mpsc));

        // Hysteresis suppresses everything.
        let cold = QosRingShapePolicy {
            qos: qos.clone(),
            hysteresis: std::time::Duration::from_secs(10),
        };
        let mut o = obs(RingShape::Spsc, false);
        o.since_last_morph = std::time::Duration::from_millis(1);
        assert_eq!(cold.decide(&o), None);
    }

    #[test]
    fn default_ordering_policy_decision_matrix() {
        let obs = |mode, declared, rate, since_ms| OrderingPolicyObservation {
            inversions_per_sec: rate,
            current_mode: mode,
            declared,
            active_producers: 2,
            active_consumers: 1,
            since_last_change: std::time::Duration::from_millis(since_ms),
        };
        let declarative = DefaultOrderingPolicy {
            hysteresis: std::time::Duration::from_millis(0),
            auto_order_threshold: None,
        };
        // GlobalFifo declaration arms the merge.
        assert_eq!(
            declarative.decide(&obs(
                OrderingMode::Unordered, QosOrdering::GlobalFifo, 0.0, 500)),
            Some(OrderingMode::MergeByStamp),
        );
        assert_eq!(
            declarative.decide(&obs(
                OrderingMode::MergeByStamp, QosOrdering::GlobalFifo, 0.0, 500)),
            None,
        );
        // Withdrawal disarms (no auto threshold).
        assert_eq!(
            declarative.decide(&obs(
                OrderingMode::MergeByStamp, QosOrdering::PerProducer, 0.0, 500)),
            Some(OrderingMode::Unordered),
        );

        let auto = DefaultOrderingPolicy {
            hysteresis: std::time::Duration::from_millis(0),
            auto_order_threshold: Some(100.0),
        };
        // Below threshold: report-only.
        assert_eq!(
            auto.decide(&obs(
                OrderingMode::Unordered, QosOrdering::PerProducer, 50.0, 500)),
            None,
        );
        // Above threshold: pre-authorized arm.
        assert_eq!(
            auto.decide(&obs(
                OrderingMode::Unordered, QosOrdering::PerProducer, 250.0, 500)),
            Some(OrderingMode::MergeByStamp),
        );
        // Auto arm is one-way: PerProducer + armed + auto -> stay.
        assert_eq!(
            auto.decide(&obs(
                OrderingMode::MergeByStamp, QosOrdering::PerProducer, 0.0, 500)),
            None,
        );

        // Hysteresis suppresses both paths.
        let cold = DefaultOrderingPolicy {
            hysteresis: std::time::Duration::from_secs(10),
            auto_order_threshold: Some(1.0),
        };
        assert_eq!(
            cold.decide(&obs(
                OrderingMode::Unordered, QosOrdering::GlobalFifo, 1e6, 1)),
            None,
        );
    }

    #[test]
    fn sidecar_spawn_with_qos_flips_merge_flag_on_declaration() {
        let ring = Arc::new(stamped_anon(2, 1, StampKind::SharedCounter));
        ring.morph_to(RingShape::Mpsc).unwrap();
        let _p0 = ring.register_producer().unwrap();
        let _p1 = ring.register_producer().unwrap();
        let _c0 = ring.register_consumer().unwrap();

        let qos = Arc::new(crate::qos_policy::QosPolicy::default());
        let sidecar = AdaptiveRingSidecar::spawn_with_qos(
            ring.clone(),
            QosRingShapePolicy {
                qos: qos.clone(),
                hysteresis: std::time::Duration::from_millis(5),
            },
            DefaultOrderingPolicy {
                hysteresis: std::time::Duration::from_millis(5),
                auto_order_threshold: None,
            },
            qos.clone(),
            std::time::Duration::from_millis(10),
        );

        // Declare GlobalFifo: on this STAMPED ring the sidecar must
        // flip the merge flag, never morph to Vyukov.
        qos.set_ordering(crate::qos_policy::Ordering::GlobalFifo);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline
            && ring.ordering_mode() != Some(OrderingMode::MergeByStamp)
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "sidecar must arm the merge flag on the GlobalFifo declaration");
        assert_eq!(ring.current_shape(), RingShape::Mpsc,
                   "stamped ring must stay composed (no Vyukov morph)");
        assert!(sidecar.ordering_flips() >= 1);

        // Withdraw the declaration: the sidecar disarms.
        qos.set_ordering(crate::qos_policy::Ordering::PerProducer);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline
            && ring.ordering_mode() != Some(OrderingMode::Unordered)
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::Unordered),
                   "sidecar must disarm when the declaration is withdrawn");
        sidecar.shutdown();
    }

    #[test]
    fn default_sidecar_gates_auto_arm_but_still_opens_on_sustained_inversions() {
        // The default `spawn_with_qos` now enables the ordering
        // auto-arm gate. This proves the gate OPENS under genuinely
        // sustained inversions (a one-way arm that never opened
        // would be useless): two producers race in Unordered mode,
        // the consumer observes cross-producer inversions, the
        // auto threshold pre-authorizes, and the gate commits the
        // single MergeByStamp flip once conviction accrues.
        let ring = Arc::new(stamped_anon(2, 1, StampKind::SharedCounter));
        ring.morph_to(RingShape::Mpsc).unwrap();
        ring.register_producer().unwrap();
        ring.register_producer().unwrap();
        ring.register_consumer().unwrap();
        ring.set_ordering_mode(OrderingMode::Unordered).unwrap();

        let qos = Arc::new(crate::qos_policy::QosPolicy::default());
        qos.set_ordering(crate::qos_policy::Ordering::PerProducer);
        let sidecar = AdaptiveRingSidecar::spawn_with_qos(
            ring.clone(),
            DefaultRingShapePolicy::default(),
            DefaultOrderingPolicy {
                hysteresis: std::time::Duration::from_millis(0),
                auto_order_threshold: Some(50.0),
            },
            qos.clone(),
            std::time::Duration::from_millis(5),
        );

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_c = stop.clone();
        let r = ring.clone();
        let consumer = std::thread::spawn(move || {
            let mut out = [0u8; 64];
            while !stop_c.load(Ordering::Acquire) {
                r.try_recv(0, &mut out).ok();
            }
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
        let mut seq = 0u64;
        while std::time::Instant::now() < deadline
            && ring.ordering_mode() != Some(OrderingMode::MergeByStamp)
        {
            ring.try_send(0, &seq.to_le_bytes()).ok();
            ring.try_send(1, &seq.to_le_bytes()).ok();
            seq += 1;
        }
        stop.store(true, Ordering::Release);
        consumer.join().unwrap();

        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "the gated auto-arm must still commit under sustained inversions");
        assert_eq!(sidecar.ordering_flips(), 1,
                   "the one-way auto-arm fires exactly once");
        sidecar.shutdown();
    }
}
