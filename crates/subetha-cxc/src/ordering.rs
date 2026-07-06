//! Ordering substrate for [`AdaptiveRing`](crate::AdaptiveRing):
//! push stamps, the cross-process ordering header, per-producer
//! watermarks, and the single-drainer lease.
//!
//! The composed MPSC / MPMC shapes give per-producer FIFO only.
//! This module is what turns global FIFO into a consumer-side
//! discipline on those shapes: every push carries an 8-byte stamp
//! in slot bytes `[0..8)`, and a consumer that k-way-merges ring
//! heads by stamp delivers items in global stamp order without the
//! Vyukov data structure's shared-CAS cost on the producer side.
//!
//! # Stamp sources
//!
//! - [`StampKind::Tsc`]: `rdtsc` per push (~20 cycles, zero
//!   coherence traffic). Selected only when the invariant-TSC probe
//!   passes: CPUID leaf `0x8000_0007` EDX bit 8, which both Intel
//!   ("Invariant TSC available if 1", SDM CPUID reference) and AMD
//!   ("TSC runs at constant rate with P/T states and does not stop
//!   in deep C-states", APM `8000_0007h` EDX) define identically.
//!   The probe first confirms the extended leaf exists via CPUID
//!   `0x8000_0000`. Cross-core skew on one socket is nanoseconds;
//!   the merge treats it as the documented approximation window.
//! - [`StampKind::SharedCounter`]: `fetch_add` on a shared atom in
//!   the ordering header. Exact total order, but every producer
//!   pays the contended-cache-line cost the composed shapes
//!   otherwise avoid. Opt-in for callers that need exactness and
//!   accept the contention.
//! - [`StampKind::Monotonic`]: system-wide monotonic clock
//!   (`CLOCK_MONOTONIC` on unix, `QueryPerformanceCounter` on
//!   Windows). The non-x86 fallback; also valid on x86.
//!
//! Per-producer stamp monotonicity is enforced at the stamp site:
//! the issued stamp is `max(source_now, last_issued + 1)`, so a
//! producer thread migrating across cores with slightly-skewed TSC
//! reads still emits strictly increasing stamps.
//!
//! # The ordering region
//!
//! One small shared region per stamped ring, separate from the ring
//! backings, holding the header below plus one cache line per
//! producer slot:
//!
//! ```text
//! +------------------------------------------------+
//! | OrderingHeader (one cache line)                |
//! |   magic: u64                                   |
//! |   mode: AtomicU32 (0=Unordered, 1=MergeByStamp,|
//! |         2=MergeStrict)                         |
//! |   stamp_kind: u32 (0=Tsc, 1=SharedCounter,     |
//! |         2=Monotonic)                           |
//! |   inversions: AtomicU64                        |
//! |   shared_stamp: AtomicU64 (counter mode)       |
//! |   drainer_token: AtomicU64                     |
//! |   drainer_heartbeat: AtomicU64                 |
//! |   drainer_epoch: AtomicU64                     |
//! +------------------------------------------------+
//! | ProducerLine[0]: issued + watermark (64B)      |
//! | ProducerLine[1]: ...                           |
//! | ... max_producers lines ...                    |
//! +------------------------------------------------+
//! ```
//!
//! File locale: `<prefix>.ordering.bin`. ShmFs locale:
//! `{prefix}_ordering` named region. Anon: in-process page. The
//! region is MMF-resident on purpose: the ordered-switch flag must
//! be visible to every process attached to the ring, unlike the
//! process-local `shape_tag`.
//!
//! # Watermarks (MergeStrict)
//!
//! `ProducerLine.watermark` is the producer's last PUBLISHED stamp,
//! stored with `Release` after the ring push. Items inside a ring
//! are stamp-ordered per producer, so a non-empty ring's head bounds
//! everything that producer has in flight. An EMPTY ring's producer
//! may hold a stamped-but-unpublished item, bounded below by its
//! watermark: any future item from producer `j` has stamp
//! `> watermark[j]`. The strict release gate is therefore
//! `candidate <= min(watermark[j])` over empty, in-use rings. Idle
//! producers refresh their watermark (a heartbeat) so the gate does
//! not couple consumer latency to producer silence forever.
//!
//! # Drainer lease
//!
//! With M concurrent consumers, "global FIFO delivery" is
//! meaningless downstream - two concurrent pops race regardless of
//! pop order - so merge mode implies ONE active drainer. The lease
//! lives in the header (`drainer_token` + heartbeat + epoch) and
//! follows the [`OwnerLease`](crate::OwnerLease) claim protocol
//! (CAS-claim when free, heartbeat-grace takeover when the holder
//! goes silent), embedded here so every locale - including Anon and
//! ShmFs, which `OwnerLease`'s file backing cannot serve - gets the
//! same mechanism from the same region.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomOrd};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_ring::RingError;

/// Magic number identifying an ordering region. ASCII "ORDR" + version.
pub const ORDERING_MAGIC: u64 = 0x4F52_4452_0000_0001;

/// Payload bytes available per slot in stamped mode: the stamp
/// costs 8 of the 64 Lamport slot bytes, leaving 56 - exactly the
/// Vyukov payload size, since Vyukov spends the same 8 bytes on its
/// per-slot sequence atom.
pub const STAMPED_PAYLOAD_BYTES: usize = 56;

/// Stamp width at the front of every stamped slot.
pub const STAMP_BYTES: usize = 8;

/// Freshness guard for TSC stamps during a merge pop: when at least
/// one ring is empty, a candidate younger than this many cycles may
/// be raced by a stamped-but-unpublished item from the empty ring's
/// producer (the stamp-to-publish window). The merge re-peeks until
/// the candidate ages past the guard. ~2-3us on contemporary cores;
/// orders of magnitude above any producer's stamp-to-publish latency.
pub const TSC_FRESHNESS_GUARD_CYCLES: u64 = 8192;

/// Freshness guard for Monotonic stamps, in nanoseconds. Same role
/// as [`TSC_FRESHNESS_GUARD_CYCLES`].
pub const MONOTONIC_FRESHNESS_GUARD_NANOS: u64 = 2_000;

/// How a stamped ring's consumer side interprets stamps.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingMode {
    /// Existing partition pop. Stamps are read after the pop to
    /// drive the inversion counter; ordering guarantee stays
    /// per-producer FIFO.
    Unordered = 0,
    /// K-way min-stamp merge over the ring heads. Global FIFO
    /// within the stamp source's skew window (freshness-guarded for
    /// time-based stamps). Single active drainer.
    MergeByStamp = 1,
    /// As `MergeByStamp`, plus the per-producer watermark gate:
    /// a candidate releases only once no empty in-use ring can
    /// still produce a smaller stamp. Exact global FIFO for every
    /// stamp kind, at the cost of slowest-producer latency coupling.
    MergeStrict = 2,
}

impl OrderingMode {
    fn from_u32(tag: u32) -> Self {
        match tag {
            0 => Self::Unordered,
            1 => Self::MergeByStamp,
            2 => Self::MergeStrict,
            _ => panic!("OrderingHeader.mode corrupted: {tag}"),
        }
    }
}

/// Which clock the stamps come from. Fixed at region creation;
/// openers read it from the header so every process attached to the
/// ring stamps from the same source.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StampKind {
    /// `rdtsc` behind the invariant-TSC probe.
    Tsc = 0,
    /// `fetch_add` on the header's `shared_stamp` atom.
    SharedCounter = 1,
    /// System-wide monotonic clock in nanoseconds.
    Monotonic = 2,
}

impl StampKind {
    fn from_u32(tag: u32) -> Option<Self> {
        match tag {
            0 => Some(Self::Tsc),
            1 => Some(Self::SharedCounter),
            2 => Some(Self::Monotonic),
            _ => None,
        }
    }

    /// Whether stamps carry time semantics (enables the freshness
    /// guard in `MergeByStamp`).
    pub fn has_time_semantics(self) -> bool {
        matches!(self, Self::Tsc | Self::Monotonic)
    }

    /// The freshness-guard window for this stamp kind, in the stamp
    /// unit. `None` for counter stamps (no time semantics; exactness
    /// comes from `MergeStrict`'s watermark gate instead).
    pub fn freshness_guard(self) -> Option<u64> {
        match self {
            Self::Tsc => Some(TSC_FRESHNESS_GUARD_CYCLES),
            Self::Monotonic => Some(MONOTONIC_FRESHNESS_GUARD_NANOS),
            Self::SharedCounter => None,
        }
    }
}

/// Pick the default stamp kind for this host: TSC when the
/// invariant probe passes, the shared counter on x86 without an
/// invariant TSC, and the monotonic clock everywhere else.
pub fn default_stamp_kind() -> StampKind {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_invariant_tsc() {
            StampKind::Tsc
        } else {
            StampKind::SharedCounter
        }
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        StampKind::Monotonic
    }
}

/// Invariant-TSC probe: CPUID leaf `0x8000_0007` EDX bit 8, after
/// confirming the leaf exists via CPUID `0x8000_0000` (the maximum
/// extended function leaf, per the Intel SDM CPUID reference; AMD
/// defines the same bit as TscInvariant in APM `8000_0007h` EDX).
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub fn has_invariant_tsc() -> bool {
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::__cpuid;
    #[cfg(target_arch = "x86")]
    use core::arch::x86::__cpuid;

    let max_extended = __cpuid(0x8000_0000).eax;
    if max_extended < 0x8000_0007 {
        return false;
    }
    let power = __cpuid(0x8000_0007);
    (power.edx & (1 << 8)) != 0
}

/// AArch64's generic timer (`CNTVCT_EL0`) is constant-rate by
/// architecture - the invariant property holds by construction.
#[cfg(target_arch = "aarch64")]
pub fn has_invariant_tsc() -> bool {
    true
}

/// Non-x86 / non-aarch64 hosts have no architected counter; the
/// probe is always false.
#[cfg(not(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "aarch64"
)))]
pub fn has_invariant_tsc() -> bool {
    false
}

/// Raw TSC read. Callers go through the [`StampKind`] stamp
/// plumbing; exposed for the merge pop's freshness-guard "now"
/// reads.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
pub fn read_tsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(target_arch = "x86")]
    unsafe {
        core::arch::x86::_rdtsc()
    }
}

/// AArch64: the virtual counter, EL0-readable, constant-rate,
/// system-wide - the architected analog of the invariant TSC.
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn read_tsc() -> u64 {
    let v: u64;
    unsafe {
        core::arch::asm!(
            "mrs {v}, cntvct_el0",
            v = out(reg) v,
            options(nomem, nostack, preserves_flags),
        );
    }
    v
}

/// Counter frequency in Hz (`CNTFRQ_EL0`): converts cycle budgets
/// to wall time on aarch64 (typically 24 MHz - 1 GHz, unlike the
/// GHz-rate x86 TSC).
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn counter_frequency_hz() -> u64 {
    let v: u64;
    unsafe {
        core::arch::asm!(
            "mrs {v}, cntfrq_el0",
            v = out(reg) v,
            options(nomem, nostack, preserves_flags),
        );
    }
    v
}

#[cfg(not(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "aarch64"
)))]
#[inline]
pub fn read_tsc() -> u64 {
    monotonic_nanos()
}

/// System-wide monotonic clock in nanoseconds. Comparable across
/// processes on the same boot.
#[cfg(unix)]
#[inline]
pub fn monotonic_nanos() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_MONOTONIC) failed");
    (ts.tv_sec as u64).wrapping_mul(1_000_000_000).wrapping_add(ts.tv_nsec as u64)
}

/// System-wide monotonic clock in nanoseconds via
/// `QueryPerformanceCounter` (system-wide, comparable across
/// processes on the same boot).
#[cfg(windows)]
#[inline]
pub fn monotonic_nanos() -> u64 {
    use windows_sys::Win32::System::Performance::{
        QueryPerformanceCounter, QueryPerformanceFrequency,
    };
    use std::sync::OnceLock;
    static FREQ: OnceLock<i64> = OnceLock::new();
    let freq = *FREQ.get_or_init(|| {
        let mut f: i64 = 0;
        let ok = unsafe { QueryPerformanceFrequency(&mut f) };
        assert!(ok != 0 && f > 0, "QueryPerformanceFrequency failed");
        f
    });
    let mut count: i64 = 0;
    let ok = unsafe { QueryPerformanceCounter(&mut count) };
    assert!(ok != 0, "QueryPerformanceCounter failed");
    // Split the conversion to avoid overflowing the intermediate
    // product: whole seconds first, then the sub-second remainder.
    let secs = (count as u64) / (freq as u64);
    let rem = (count as u64) % (freq as u64);
    secs.wrapping_mul(1_000_000_000)
        .wrapping_add(rem.wrapping_mul(1_000_000_000) / (freq as u64))
}

/// "Now" in the units of the given stamp kind. Counter stamps have
/// no clock; callers never ask (the freshness guard is `None`).
#[inline]
pub(crate) fn stamp_now(kind: StampKind) -> u64 {
    match kind {
        StampKind::Tsc => read_tsc(),
        StampKind::Monotonic => monotonic_nanos(),
        StampKind::SharedCounter => 0,
    }
}

/// Ordering header. One cache line at offset 0 of the region.
#[repr(C, align(64))]
pub struct OrderingHeader {
    pub magic: u64,
    /// Active [`OrderingMode`], cross-process visible. The ordered
    /// switch is one `Release` store here; the in-flight backlog is
    /// retroactively ordered because the stamps were already there.
    pub mode: AtomicU32,
    /// [`StampKind`] discriminant. Written once at creation; openers
    /// adopt it.
    pub stamp_kind: u32,
    /// Cross-producer inversions observed at pop. The runtime signal
    /// that converts the invisible ordering property into
    /// "inversions/sec observed".
    pub inversions: AtomicU64,
    /// Stamp counter for [`StampKind::SharedCounter`].
    pub shared_stamp: AtomicU64,
    /// Drainer lease: `(pid << 32) | consumer_id`, 0 = unleased.
    pub drainer_token: AtomicU64,
    /// Drainer heartbeat: last `drainer_epoch` value the holder
    /// confirmed liveness at.
    pub drainer_heartbeat: AtomicU64,
    /// Global epoch for heartbeat-grace takeover. Ticked by the
    /// sidecar (or any caller); mirrors `OwnerLease::tick_epoch`.
    pub drainer_epoch: AtomicU64,
    _pad: [u8; 8],
}

/// Second header line: the drainer-lease GENERATION, alone on its
/// own cache line. Bumped only on lease claim / takeover / release
/// and on epoch ticks - all rare events - so a merge drainer's
/// per-pop lease verification is one load of a line that is NEVER
/// written in steady state (an L1 hit with zero coherence traffic),
/// instead of loads on the first header line that every
/// SharedCounter push fetch_adds. Measured on the Zen3 KVM guest:
/// per-pop loads of that stamp-hot line cost a cache-to-cache
/// transfer each (~130 ns) and tripled the merge rungs.
#[repr(C, align(64))]
pub struct LeaseGenLine {
    pub lease_generation: AtomicU64,
    _pad: [u8; 56],
}

/// Per-producer ordering state. One cache line per producer slot so
/// one producer's stamp bookkeeping never invalidates a sibling's
/// L1 line.
#[repr(C, align(64))]
pub struct ProducerLine {
    /// Last ISSUED stamp (monotonicity floor: the next stamp is
    /// `max(source_now, issued + 1)`).
    pub issued: AtomicU64,
    /// Last PUBLISHED stamp (the MergeStrict watermark). `Release`-
    /// stored after the ring push; 0 = this producer slot has never
    /// published or refreshed.
    pub watermark: AtomicU64,
    _pad: [u8; 48],
}

/// Total region size for `max_producers` producer slots.
pub const fn ordering_region_size(max_producers: usize) -> usize {
    std::mem::size_of::<OrderingHeader>()
        + std::mem::size_of::<LeaseGenLine>()
        + max_producers * std::mem::size_of::<ProducerLine>()
}

/// Backing-store owner for an ordering region; mirrors the ring
/// backings' lifetime-extension pattern.
#[allow(dead_code)]
enum OrderingBacking {
    /// In-process anonymous page.
    Anon(MmapMut),
    /// File-backed (cross-process via page cache).
    File(File, MmapMut),
    /// Named RAM-resident shared memory.
    Shm(crate::shm_file::ShmFile),
}

/// The mapped ordering region: header + producer lines, in any of
/// the three locales.
pub struct OrderingRegion {
    _backing: OrderingBacking,
    raw_ptr: *mut u8,
    max_producers: usize,
    kind: StampKind,
}

unsafe impl Send for OrderingRegion {}
unsafe impl Sync for OrderingRegion {}

unsafe fn init_ordering_layout(ptr: *mut u8, max_producers: usize, kind: StampKind) {
    let header_ptr = ptr as *mut OrderingHeader;
    unsafe {
        std::ptr::write(header_ptr, OrderingHeader {
            magic: ORDERING_MAGIC,
            mode: AtomicU32::new(OrderingMode::Unordered as u32),
            stamp_kind: kind as u32,
            inversions: AtomicU64::new(0),
            shared_stamp: AtomicU64::new(0),
            drainer_token: AtomicU64::new(0),
            drainer_heartbeat: AtomicU64::new(0),
            drainer_epoch: AtomicU64::new(0),
            _pad: [0; 8],
        });
    }
    let gen_ptr = unsafe {
        ptr.add(std::mem::size_of::<OrderingHeader>()) as *mut LeaseGenLine
    };
    unsafe {
        std::ptr::write(gen_ptr, LeaseGenLine {
            lease_generation: AtomicU64::new(0),
            _pad: [0; 56],
        });
    }
    let lines_base = unsafe {
        ptr.add(std::mem::size_of::<OrderingHeader>()
            + std::mem::size_of::<LeaseGenLine>())
    };
    for i in 0..max_producers {
        let line_ptr = unsafe {
            lines_base.add(i * std::mem::size_of::<ProducerLine>()) as *mut ProducerLine
        };
        unsafe {
            std::ptr::write(line_ptr, ProducerLine {
                issued: AtomicU64::new(0),
                watermark: AtomicU64::new(0),
                _pad: [0; 48],
            });
        }
    }
}

impl OrderingRegion {
    /// Anonymous in-process region, initialised.
    pub fn create_anon(max_producers: usize, kind: StampKind) -> Result<Self, RingError> {
        let total = ordering_region_size(max_producers);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        unsafe { init_ordering_layout(mmap.as_mut_ptr(), max_producers, kind) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: OrderingBacking::Anon(mmap),
            raw_ptr,
            max_producers,
            kind,
        })
    }

    /// File-backed region at `path`, initialised.
    pub fn create(
        path: impl AsRef<Path>,
        max_producers: usize,
        kind: StampKind,
    ) -> Result<Self, RingError> {
        let total = ordering_region_size(max_producers);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        unsafe { init_ordering_layout(mmap.as_mut_ptr(), max_producers, kind) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: OrderingBacking::File(file, mmap),
            raw_ptr,
            max_producers,
            kind,
        })
    }

    /// Open an existing file-backed region. Validates the magic and
    /// adopts the creator's stamp kind; does NOT re-initialise, so
    /// the live mode flag, counters, and watermarks survive the
    /// attach.
    pub fn open(
        path: impl AsRef<Path>,
        max_producers: usize,
    ) -> Result<Self, RingError> {
        let total = ordering_region_size(max_producers);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(RingError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let header = unsafe { &*(mmap.as_ptr() as *const OrderingHeader) };
        if header.magic != ORDERING_MAGIC {
            return Err(RingError::LayoutMismatch);
        }
        let kind = StampKind::from_u32(header.stamp_kind)
            .ok_or(RingError::LayoutMismatch)?;
        let raw_ptr = mmap.as_ptr() as *mut u8;
        Ok(Self {
            _backing: OrderingBacking::File(file, mmap),
            raw_ptr,
            max_producers,
            kind,
        })
    }

    /// Named-shm region, initialised. Mirrors the ring backings'
    /// `create_from_shm` semantics (the creator initialises).
    pub fn create_shm(
        shm: crate::shm_file::ShmFile,
        max_producers: usize,
        kind: StampKind,
    ) -> Result<Self, RingError> {
        let total = ordering_region_size(max_producers);
        let mut shm = shm;
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        unsafe { init_ordering_layout(raw_ptr, max_producers, kind) };
        Ok(Self {
            _backing: OrderingBacking::Shm(shm),
            raw_ptr,
            max_producers,
            kind,
        })
    }

    /// Stamp kind this region was created with.
    pub fn stamp_kind(&self) -> StampKind { self.kind }

    /// Number of producer lines.
    pub fn max_producers(&self) -> usize { self.max_producers }

    pub(crate) fn header(&self) -> &OrderingHeader {
        unsafe { &*(self.raw_ptr as *const OrderingHeader) }
    }

    pub(crate) fn line(&self, producer_id: usize) -> &ProducerLine {
        assert!(producer_id < self.max_producers,
                "producer_id {producer_id} out of range (max {})", self.max_producers);
        let lines_base = unsafe {
            self.raw_ptr.add(std::mem::size_of::<OrderingHeader>()
                + std::mem::size_of::<LeaseGenLine>())
        };
        unsafe {
            &*(lines_base.add(producer_id * std::mem::size_of::<ProducerLine>())
                as *const ProducerLine)
        }
    }

    fn lease_gen_line(&self) -> &LeaseGenLine {
        unsafe {
            &*(self.raw_ptr.add(std::mem::size_of::<OrderingHeader>())
                as *const LeaseGenLine)
        }
    }

    /// The drainer-lease generation: bumped on every lease claim /
    /// takeover / release and on every epoch tick, and NEVER written
    /// otherwise. A merge drainer verifies its lease per pop with one
    /// load of this quiet line (compared against a consumer-local
    /// cache) and runs the full lease handshake only on change - the
    /// per-pop path never touches the stamp-hot first header line.
    #[inline]
    pub fn lease_generation(&self) -> u64 {
        self.lease_gen_line().lease_generation.load(AtomOrd::Acquire)
    }

    fn bump_lease_generation(&self) {
        self.lease_gen_line().lease_generation.fetch_add(1, AtomOrd::AcqRel);
    }

    /// Current ordering mode. One Acquire load.
    pub fn mode(&self) -> OrderingMode {
        OrderingMode::from_u32(self.header().mode.load(AtomOrd::Acquire))
    }

    /// Flip the ordering mode. Off->On is immediate and retroactive:
    /// the in-flight backlog merges in stamp order because the
    /// stamps were already in the slots. On->Off is immediate.
    /// No drain, no data movement.
    pub fn set_mode(&self, mode: OrderingMode) {
        self.header().mode.store(mode as u32, AtomOrd::Release);
    }

    /// Cross-producer inversions observed since creation.
    pub fn inversions(&self) -> u64 {
        self.header().inversions.load(AtomOrd::Relaxed)
    }

    pub(crate) fn record_inversion(&self) {
        self.header().inversions.fetch_add(1, AtomOrd::Relaxed);
    }

    /// Issue the next stamp for `producer_id`: strictly increasing
    /// per producer regardless of source skew.
    ///
    /// Two-phase: the producer RESERVES first (`issued = floor`, a
    /// lower bound for the upcoming stamp, with the watermark still
    /// behind), then reads the clock and stores the real stamp.
    /// The reservation is what makes the merge's in-flight gate
    /// airtight against preemption: from the very first store, any
    /// merge candidate above the floor blocks until this push
    /// publishes (or fails and finalizes the watermark) - even if
    /// the producer is descheduled between the clock read and the
    /// store, or between the stamp and the push. A fixed freshness
    /// window cannot give that bound; deschedule latency is
    /// unbounded.
    #[inline]
    pub(crate) fn next_stamp(&self, producer_id: usize) -> u64 {
        let line = self.line(producer_id);
        let floor = line.issued.load(AtomOrd::Relaxed) + 1;
        line.issued.store(floor, AtomOrd::Release);
        let raw = match self.kind {
            StampKind::Tsc => read_tsc(),
            StampKind::Monotonic => monotonic_nanos(),
            StampKind::SharedCounter => {
                self.header().shared_stamp.fetch_add(1, AtomOrd::Relaxed) + 1
            }
        };
        let stamp = raw.max(floor);
        line.issued.store(stamp, AtomOrd::Release);
        stamp
    }

    /// Publish `stamp` as producer `producer_id`'s watermark. Called
    /// after the ring push so an Acquire reader that sees the
    /// watermark also sees the published slot. Also called when the
    /// push returns `Full`: that stamp will never publish, so
    /// advancing the watermark restores `issued == watermark` (the
    /// "nothing in flight" state the MergeStrict gate keys on).
    #[inline]
    pub(crate) fn publish_watermark(&self, producer_id: usize, stamp: u64) {
        self.line(producer_id).watermark.store(stamp, AtomOrd::Release);
    }

    /// MergeStrict in-flight gate: `true` when producer
    /// `producer_id` currently holds a stamped-but-unpublished item
    /// whose stamp undercuts `candidate`. Producers stamp
    /// immediately before pushing and finalize the watermark right
    /// after (success or `Full`), so `issued != watermark` brackets
    /// exactly the stamp-to-publish window.
    #[inline]
    pub(crate) fn in_flight_below(&self, producer_id: usize, candidate: u64) -> bool {
        let line = self.line(producer_id);
        let issued = line.issued.load(AtomOrd::Acquire);
        if issued >= candidate {
            return false;
        }
        issued != line.watermark.load(AtomOrd::Acquire)
    }

    /// Watermark heartbeat for an idle producer: advances the
    /// watermark to a fresh stamp WITHOUT pushing, so MergeStrict
    /// consumers stop waiting on this producer's silence. Only call
    /// from the producer's own thread between pushes (never while a
    /// stamped item is awaiting publish - the refresh would claim
    /// "nothing below this stamp is in flight" while one is).
    pub fn refresh_watermark(&self, producer_id: usize) {
        let line = self.line(producer_id);
        let raw = match self.kind {
            StampKind::Tsc => read_tsc(),
            StampKind::Monotonic => monotonic_nanos(),
            // Counter stamps: an idle producer cannot mint a fresh
            // counter value without consuming one; bump the shared
            // counter so the watermark is a real "nothing below
            // this" bound.
            StampKind::SharedCounter => {
                self.header().shared_stamp.fetch_add(1, AtomOrd::Relaxed) + 1
            }
        };
        let floor = line.issued.load(AtomOrd::Relaxed) + 1;
        let stamp = raw.max(floor);
        line.issued.store(stamp, AtomOrd::Release);
        line.watermark.store(stamp, AtomOrd::Release);
    }

    /// Read producer `producer_id`'s watermark.
    pub fn watermark(&self, producer_id: usize) -> u64 {
        self.line(producer_id).watermark.load(AtomOrd::Acquire)
    }

    /// Read producer `producer_id`'s last issued stamp (or
    /// reservation floor while a stamp is being issued).
    pub fn issued(&self, producer_id: usize) -> u64 {
        self.line(producer_id).issued.load(AtomOrd::Acquire)
    }

    /// Terminal producer retirement: publishes `u64::MAX` as the
    /// slot's issued stamp + watermark, declaring "this producer
    /// will never stamp again". MergeStrict consumers stop waiting
    /// on the slot's silence permanently (any candidate passes its
    /// watermark gate) and the in-flight gate reads it as clean.
    /// A producer MUST NOT push after retiring its slot - the
    /// monotonicity floor is saturated.
    pub fn retire_producer(&self, producer_id: usize) {
        let line = self.line(producer_id);
        line.issued.store(u64::MAX, AtomOrd::Release);
        line.watermark.store(u64::MAX, AtomOrd::Release);
    }

    /// Seed this region's stamp state from another region so stamps
    /// stay monotone across a backing swap (capacity morphs allocate
    /// a fresh region; counter stamps would otherwise restart at 1).
    pub fn seed_from(&self, other: &OrderingRegion) {
        self.header().shared_stamp.store(
            other.header().shared_stamp.load(AtomOrd::Acquire),
            AtomOrd::Release,
        );
        self.header().inversions.store(
            other.header().inversions.load(AtomOrd::Relaxed),
            AtomOrd::Relaxed,
        );
        let n = self.max_producers.min(other.max_producers);
        for i in 0..n {
            self.line(i).issued.store(
                other.line(i).issued.load(AtomOrd::Relaxed),
                AtomOrd::Relaxed,
            );
            self.line(i).watermark.store(
                other.line(i).watermark.load(AtomOrd::Acquire),
                AtomOrd::Release,
            );
        }
        self.set_mode(other.mode());
    }

    // ---------------------------------------------------------------
    // Drainer lease (OwnerLease claim protocol embedded in the
    // header so all three locales share one mechanism).
    // ---------------------------------------------------------------

    /// Try to become (or confirm being) the active merge drainer.
    /// Token layout: `(pid << 32) | consumer_id`, never 0.
    ///
    /// Succeeds when (a) unleased, (b) the caller already holds the
    /// lease (heartbeat refreshed), or (c) the current holder's
    /// heartbeat is more than `grace_epochs` behind the global
    /// epoch (dead-drainer takeover).
    pub fn try_acquire_drainer(&self, token: u64, grace_epochs: u64) -> bool {
        assert!(token != 0, "drainer token 0 is reserved for unleased");
        let header = self.header();
        loop {
            let cur = header.drainer_token.load(AtomOrd::Acquire);
            if cur == token {
                // Holder fast path must be WRITE-FREE in steady state:
                // the heartbeat shares a cache line with the shared
                // stamp counter producers fetch_add on every push, so
                // an unconditional store here forces that line
                // exclusive per pop and collapses producer throughput.
                // The epoch only advances on sidecar scans; refresh
                // the heartbeat only when it actually moved.
                let global = header.drainer_epoch.load(AtomOrd::Acquire);
                if header.drainer_heartbeat.load(AtomOrd::Relaxed) != global {
                    header.drainer_heartbeat.store(global, AtomOrd::Release);
                }
                return true;
            }
            let can_claim = if cur == 0 {
                true
            } else {
                let beat = header.drainer_heartbeat.load(AtomOrd::Acquire);
                let global = header.drainer_epoch.load(AtomOrd::Acquire);
                global.saturating_sub(beat) > grace_epochs
            };
            if !can_claim {
                return false;
            }
            if header.drainer_token.compare_exchange(
                cur, token, AtomOrd::AcqRel, AtomOrd::Acquire,
            ).is_ok() {
                let global = header.drainer_epoch.load(AtomOrd::Acquire);
                header.drainer_heartbeat.store(global, AtomOrd::Release);
                // Ownership changed: invalidate every consumer's
                // cached per-pop lease verification.
                self.bump_lease_generation();
                return true;
            }
            std::hint::spin_loop();
        }
    }

    /// Voluntarily release the drainer lease. Returns `false` when
    /// the caller did not hold it.
    pub fn release_drainer(&self, token: u64) -> bool {
        let released = self.header().drainer_token
            .compare_exchange(token, 0, AtomOrd::AcqRel, AtomOrd::Acquire)
            .is_ok();
        if released {
            self.bump_lease_generation();
        }
        released
    }

    /// Current drainer token (0 = unleased).
    pub fn current_drainer(&self) -> u64 {
        self.header().drainer_token.load(AtomOrd::Acquire)
    }

    /// Refresh the drainer heartbeat. Returns `false` when the
    /// caller no longer holds the lease.
    pub fn drainer_beat(&self, token: u64) -> bool {
        let header = self.header();
        if header.drainer_token.load(AtomOrd::Acquire) != token {
            return false;
        }
        let global = header.drainer_epoch.load(AtomOrd::Acquire);
        header.drainer_heartbeat.store(global, AtomOrd::Release);
        true
    }

    /// Advance the global drainer epoch (caller-driven, typically
    /// the sidecar's scan tick). A holder whose heartbeat falls more
    /// than `grace_epochs` behind becomes preemptible. Also bumps the
    /// lease generation so the holder's next pop re-runs the full
    /// handshake and refreshes its heartbeat (the liveness proof).
    pub fn tick_drainer_epoch(&self) -> u64 {
        let epoch = self.header().drainer_epoch.fetch_add(1, AtomOrd::AcqRel) + 1;
        self.bump_lease_generation();
        epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_one_cache_line_and_lines_are_padded() {
        assert_eq!(std::mem::size_of::<OrderingHeader>(), 64);
        assert_eq!(std::mem::size_of::<LeaseGenLine>(), 64);
        assert_eq!(std::mem::size_of::<ProducerLine>(), 64);
        // Header line + lease-generation line + producer lines.
        assert_eq!(ordering_region_size(4), 64 + 64 + 4 * 64);
    }

    #[test]
    fn lease_generation_bumps_on_claim_release_and_tick_only() {
        let region = OrderingRegion::create_anon(2, StampKind::SharedCounter).unwrap();
        let g0 = region.lease_generation();
        assert!(region.try_acquire_drainer(7, 3));
        let g1 = region.lease_generation();
        assert!(g1 > g0, "claim must bump the generation");
        // Holder fast path: no bump (the whole point - per-pop
        // verification stays on the quiet line).
        assert!(region.try_acquire_drainer(7, 3));
        assert_eq!(region.lease_generation(), g1);
        region.tick_drainer_epoch();
        let g2 = region.lease_generation();
        assert!(g2 > g1, "epoch tick must bump so the holder re-beats");
        assert!(region.release_drainer(7));
        assert!(region.lease_generation() > g2, "release must bump");
    }

    #[test]
    fn probe_runs_without_fault_and_tsc_reads_advance() {
        // The probe itself must execute on every host. On hosts where
        // it passes, two TSC reads spaced by real work must advance.
        let invariant = has_invariant_tsc();
        if invariant {
            let a = read_tsc();
            let mut spin = 0u64;
            for i in 0..10_000u64 { spin = spin.wrapping_add(i); }
            std::hint::black_box(spin);
            let b = read_tsc();
            assert!(b > a, "TSC must advance across real work: {a} -> {b}");
        }
    }

    #[test]
    fn monotonic_clock_advances() {
        let a = monotonic_nanos();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = monotonic_nanos();
        assert!(b > a, "monotonic clock must advance: {a} -> {b}");
    }

    #[test]
    fn default_kind_matches_probe_chain() {
        let kind = default_stamp_kind();
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if has_invariant_tsc() {
                assert_eq!(kind, StampKind::Tsc);
            } else {
                assert_eq!(kind, StampKind::SharedCounter);
            }
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        assert_eq!(kind, StampKind::Monotonic);
    }

    #[test]
    fn stamps_strictly_increase_per_producer_every_kind() {
        for kind in [StampKind::Tsc, StampKind::SharedCounter, StampKind::Monotonic] {
            let region = OrderingRegion::create_anon(2, kind).unwrap();
            let mut last = 0u64;
            for _ in 0..10_000 {
                let s = region.next_stamp(0);
                assert!(s > last, "{kind:?} stamp must strictly increase: {last} -> {s}");
                last = s;
            }
        }
    }

    #[test]
    fn counter_stamps_are_globally_unique_across_producers() {
        let region = OrderingRegion::create_anon(4, StampKind::SharedCounter).unwrap();
        let mut seen = std::collections::HashSet::new();
        for p in 0..4 {
            for _ in 0..100 {
                assert!(seen.insert(region.next_stamp(p)),
                        "counter stamps must never repeat");
            }
        }
    }

    #[test]
    fn watermark_publishes_and_refreshes() {
        let region = OrderingRegion::create_anon(2, StampKind::Monotonic).unwrap();
        assert_eq!(region.watermark(0), 0);
        let s = region.next_stamp(0);
        region.publish_watermark(0, s);
        assert_eq!(region.watermark(0), s);
        region.refresh_watermark(0);
        assert!(region.watermark(0) > s, "refresh must advance the watermark");
    }

    #[test]
    fn mode_flips_round_trip() {
        let region = OrderingRegion::create_anon(1, StampKind::Monotonic).unwrap();
        assert_eq!(region.mode(), OrderingMode::Unordered);
        region.set_mode(OrderingMode::MergeByStamp);
        assert_eq!(region.mode(), OrderingMode::MergeByStamp);
        region.set_mode(OrderingMode::MergeStrict);
        assert_eq!(region.mode(), OrderingMode::MergeStrict);
        region.set_mode(OrderingMode::Unordered);
        assert_eq!(region.mode(), OrderingMode::Unordered);
    }

    #[test]
    fn file_region_open_validates_and_adopts_kind() {
        let p = std::env::temp_dir().join(format!(
            "subetha-ordering-open-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos()).unwrap_or(0),
        ));
        {
            let creator =
                OrderingRegion::create(&p, 3, StampKind::SharedCounter).unwrap();
            creator.set_mode(OrderingMode::MergeByStamp);
            let s = creator.next_stamp(1);
            creator.publish_watermark(1, s);
        }
        let opened = OrderingRegion::open(&p, 3).unwrap();
        assert_eq!(opened.stamp_kind(), StampKind::SharedCounter,
                   "opener must adopt the creator's stamp kind");
        assert_eq!(opened.mode(), OrderingMode::MergeByStamp,
                   "open must not re-initialise the live mode flag");
        assert!(opened.watermark(1) > 0,
                "open must not wipe watermarks");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_rejects_garbage() {
        let p = std::env::temp_dir().join(format!(
            "subetha-ordering-garbage-{}.bin", std::process::id(),
        ));
        std::fs::write(&p, vec![0u8; ordering_region_size(2)]).unwrap();
        assert!(matches!(
            OrderingRegion::open(&p, 2),
            Err(RingError::LayoutMismatch)
        ));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn drainer_lease_claim_refresh_release_takeover() {
        let region = OrderingRegion::create_anon(1, StampKind::Monotonic).unwrap();
        // Drainer ids are (pid << 32) | tid; tid 0 on both here.
        let a = 100u64 << 32;
        let b = 200u64 << 32;

        // Free -> A claims.
        assert!(region.try_acquire_drainer(a, 3));
        assert_eq!(region.current_drainer(), a);
        // A re-acquires (idempotent + heartbeat refresh).
        assert!(region.try_acquire_drainer(a, 3));
        // B cannot claim while A beats.
        assert!(!region.try_acquire_drainer(b, 3));
        // A releases; B claims.
        assert!(region.release_drainer(a));
        assert!(region.try_acquire_drainer(b, 3));
        // Stale-heartbeat takeover: tick past grace without B beating.
        for _ in 0..5 { region.tick_drainer_epoch(); }
        assert!(region.try_acquire_drainer(a, 3),
                "stale drainer must be preemptible after grace epochs");
        assert_eq!(region.current_drainer(), a);
        // Beat from the deposed holder fails.
        assert!(!region.drainer_beat(b));
        assert!(region.drainer_beat(a));
    }

    #[test]
    fn seed_from_carries_counter_and_watermarks() {
        let old = OrderingRegion::create_anon(2, StampKind::SharedCounter).unwrap();
        for _ in 0..50 { old.next_stamp(0); }
        let w = old.next_stamp(1);
        old.publish_watermark(1, w);
        old.set_mode(OrderingMode::MergeStrict);

        let fresh = OrderingRegion::create_anon(2, StampKind::SharedCounter).unwrap();
        fresh.seed_from(&old);
        assert_eq!(fresh.mode(), OrderingMode::MergeStrict);
        assert_eq!(fresh.watermark(1), w);
        let next = fresh.next_stamp(0);
        assert!(next > w, "seeded counter must continue past the old region's stamps");
    }
}
