//! `SharedRing<P>` - cross-thread / cross-process lock-free MPMC ring
//! backed by a memory-mapped file.
//!
//! One mechanism gives you THREE deployment modes:
//!
//! 1. **Cross-thread**: multiple threads in one process map the same
//!    file; lock-free CAS handles concurrency.
//! 2. **Cross-process**: multiple processes open the same file via
//!    [`SharedRing::open`]; the OS page-cache aliases them onto the
//!    same physical pages.
//! 3. **Disk-persistent**: the MMF is backed by a real file; the
//!    kernel writes dirty pages to disk on its own schedule, plus
//!    [`SharedRing::flush`] forces a sync when the caller wants
//!    durability.
//!
//! The same byte layout serves all three.
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | RingHeader  (64B aligned)   |  producer_seq, consumer_seq,
//! |                             |  capacity, slot_size, magic
//! +-----------------------------+
//! | Slot[0] (64B cache line)    |  state + sequence + payload
//! | Slot[1]                     |
//! | ...                         |
//! | Slot[capacity - 1]          |
//! +-----------------------------+
//! ```
//!
//! Each slot is exactly one cache line (64 bytes). The state field
//! advances through EMPTY -> CLAIMED_BY_PRODUCER -> PUBLISHED ->
//! CLAIMED_BY_CONSUMER -> EMPTY in a closed loop.
//!
//! # Concurrency protocol
//!
//! Producers:
//! 1. Read `producer_seq` (atomic).
//! 2. Compute `slot_idx = producer_seq % capacity`.
//! 3. Read slot's sequence number; if it doesn't equal `producer_seq`,
//!    the ring is full (slot still holds an unconsumed value). Retry
//!    or fail.
//! 4. CAS `producer_seq` from S to S+1. On success, the slot is ours
//!    to write; copy payload, then store slot.sequence = S+1 (release).
//!
//! Consumers:
//! 1. Read `consumer_seq`.
//! 2. `slot_idx = consumer_seq % capacity`.
//! 3. Acquire-load slot.sequence; must equal `consumer_seq + 1`
//!    (means producer published). Otherwise empty.
//! 4. CAS `consumer_seq` from S to S+1. On success, read payload,
//!    then store slot.sequence = S + capacity (releases the slot
//!    for the next producer that will use it at producer_seq =
//!    S + capacity).
//!
//! This is the classic Vyukov MPMC bounded-queue protocol.

use std::cell::UnsafeCell;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

/// Magic number to detect a valid ring header. ASCII 'APMF' + version.
pub const RING_MAGIC: u64 = 0x4150_4D46_0000_0001;

/// Each slot is exactly one cache line.
pub const SLOT_SIZE: usize = 64;

/// Payload bytes per slot = SLOT_SIZE - sizeof(sequence: u64).
pub const PAYLOAD_BYTES: usize = SLOT_SIZE - std::mem::size_of::<u64>();

/// Header layout: three cache lines so the two hot counters never
/// false-share. Line 0 is read-mostly metadata (plus the
/// rarely-written `epoch`); `producer_seq` and `consumer_seq` each get
/// their own line. Every producer CASes `producer_seq` and every
/// consumer CASes `consumer_seq`; co-locating them on one line made
/// each side's CAS invalidate the other side's copy, serializing the
/// producer and consumer coherence traffic under contention. The SPSC
/// ring separates `head`/`tail` for exactly this reason.
#[repr(C, align(64))]
pub struct RingHeader {
    pub magic: u64,
    pub capacity: u64,
    pub slot_size: u64,
    /// Epoch counter; advanced by the watchdog every scan tick.
    /// Heartbeats compare against this to detect liveness. Read-mostly
    /// from the ring's perspective, so it shares the metadata line.
    pub epoch: AtomicU64,
    /// Pad the metadata line out to 64 bytes so `producer_seq` starts
    /// its own cache line.
    _pad_meta: [u8; 64 - 32],
    /// Producer-owned enqueue counter; sole occupant of its line.
    pub producer_seq: AtomicU64,
    _pad_prod: [u8; 64 - 8],
    /// Consumer-owned dequeue counter; sole occupant of its line.
    pub consumer_seq: AtomicU64,
    _pad_cons: [u8; 64 - 8],
}

#[repr(C, align(64))]
pub struct Slot {
    pub sequence: AtomicU64,
    pub payload: UnsafeCell<[u8; PAYLOAD_BYTES]>,
}

unsafe impl Sync for Slot {}

/// Compute the total MMF size for a ring of `capacity` slots.
pub const fn ring_file_size(capacity: usize) -> usize {
    std::mem::size_of::<RingHeader>() + capacity * SLOT_SIZE
}

/// Compile-time-enforced single-producer / single-consumer ring,
/// backed by the Lamport 1983 SPSC core in
/// [`crate::spsc_ring::SpscRingCore`].
///
/// The [`SharedRing`] type exposes MPMC ops (`try_push` /
/// `try_pop`) plus SPSC fast-path ops on the same Vyukov-protocol
/// storage. The fast paths still pay for the per-slot sequence
/// number that MPMC needs - four cross-thread atomics per push.
///
/// `SharedRingSpsc` is the dedicated SPSC primitive. It uses a
/// different on-disk layout (Lamport: head + tail counters on
/// separate cache lines, payload-only slots, no per-slot sequence
/// number) and pays only **one Acquire load + one Release store**
/// of cross-thread atomics per op. On Zen+ R7 2700 with 100k items
/// the Lamport core lands roughly 2x the throughput of the Vyukov
/// SPSC fast path, and ~7x crossbeam_channel.
///
/// The constructor returns an owned ([`Producer`], [`Consumer`])
/// pair; neither half implements `Clone`, both are `Send` and
/// `!Sync`. The compiler guarantees at most one thread holds the
/// `Producer` (single producer), at most one thread holds the
/// `Consumer` (single consumer). The SPSC contract that backs the
/// no-CAS Lamport pattern is enforced statically.
///
/// Internally the pair shares one [`SpscRingCore`](crate::spsc_ring::SpscRingCore)
/// via [`Arc`](std::sync::Arc). The two halves call the core's `try_push` /
/// `try_pop` directly; no per-op cost vs the raw core. The only
/// overhead is the `Arc` clone at construction.
///
/// **No stuck-slot recovery needed.** The Lamport protocol does not
/// have the claimed-but-never-published pathology Vyukov has. The
/// producer writes payload then Release-stores `head` to publish in
/// a single observable transition; a producer crash between payload
/// write and Release-store leaves `head` unchanged and the slot
/// uncommitted - the consumer never reads it because head was not
/// advanced.
pub struct SharedRingSpsc;

/// Sole-producer handle on a [`SharedRingSpsc`] pair. `Send` so it
/// can be moved to a producer thread; `!Sync` so it cannot be
/// shared across threads (which would violate the SPSC contract).
/// Not `Clone`: a second producer is statically impossible.
pub struct Producer {
    inner: std::sync::Arc<crate::spsc_ring::SpscRingCore>,
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

/// Sole-consumer handle on a [`SharedRingSpsc`] pair. Same
/// `Send + !Sync + !Clone` shape as [`Producer`], mirroring the
/// SPSC contract on the read side.
pub struct Consumer {
    inner: std::sync::Arc<crate::spsc_ring::SpscRingCore>,
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl SharedRingSpsc {
    /// Anonymous (in-process, no file) SPSC pair. Skips file
    /// create + ftruncate + first-page-fault cost.
    pub fn create_anon_pair(capacity: usize) -> Result<(Producer, Consumer), RingError> {
        let ring = std::sync::Arc::new(
            crate::spsc_ring::SpscRingCore::create_anon(capacity)?,
        );
        Ok((
            Producer { inner: ring.clone(), _not_sync: std::marker::PhantomData },
            Consumer { inner: ring,         _not_sync: std::marker::PhantomData },
        ))
    }

    /// File-backed SPSC pair. Cross-process visibility available
    /// via [`SharedRingSpsc::open_pair`] on the same path.
    pub fn create_pair(
        path: impl AsRef<Path>,
        capacity: usize,
    ) -> Result<(Producer, Consumer), RingError> {
        let ring = std::sync::Arc::new(
            crate::spsc_ring::SpscRingCore::create(path, capacity)?,
        );
        Ok((
            Producer { inner: ring.clone(), _not_sync: std::marker::PhantomData },
            Consumer { inner: ring,         _not_sync: std::marker::PhantomData },
        ))
    }

    /// Open an existing file-backed ring and return an SPSC pair.
    /// Caller's responsibility to ensure only one producer + one
    /// consumer attach to the underlying file across all processes;
    /// the type system enforces this within one process, not across.
    pub fn open_pair(
        path: impl AsRef<Path>,
        expected_capacity: usize,
    ) -> Result<(Producer, Consumer), RingError> {
        let ring = std::sync::Arc::new(
            crate::spsc_ring::SpscRingCore::open(path, expected_capacity)?,
        );
        Ok((
            Producer { inner: ring.clone(), _not_sync: std::marker::PhantomData },
            Consumer { inner: ring,         _not_sync: std::marker::PhantomData },
        ))
    }
}

impl Producer {
    /// Push one payload. Forwards to
    /// [`SpscRingCore::try_push`](crate::spsc_ring::SpscRingCore::try_push).
    /// The SPSC contract is type-system-enforced because there is
    /// exactly one `Producer` in existence per pair (`!Sync + !Clone`).
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        self.inner.try_push(payload)
    }

    /// Capacity of the underlying ring (always a power of 2).
    pub fn capacity(&self) -> usize { self.inner.capacity() }

    /// Current head (producer's published position).
    pub fn head(&self) -> u64 { self.inner.head() }
}

impl Consumer {
    /// Pop one payload into `out`. Forwards to
    /// [`SpscRingCore::try_pop`](crate::spsc_ring::SpscRingCore::try_pop).
    /// The SPSC contract is type-system-enforced because there is
    /// exactly one `Consumer` in existence per pair (`!Sync + !Clone`).
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.inner.try_pop(out)
    }

    /// Capacity of the underlying ring (always a power of 2).
    pub fn capacity(&self) -> usize { self.inner.capacity() }

    /// Current tail (consumer's published position).
    pub fn tail(&self) -> u64 { self.inner.tail() }
}

/// Defer the file-backed MMF setup until first use.
///
/// **When to reach for this:** speculative channel construction
/// where the consumer may or may not ever send/recv (per-connection
/// channels that some connections never use, conditional code paths,
/// option-types that hold a ring "just in case"). Construction is
/// free; the file create + ftruncate + mmap + init cost is paid
/// once on the first [`try_push`](LazySharedRing::try_push) or
/// [`try_pop`](LazySharedRing::try_pop) call.
///
/// **When NOT to reach for this:** in-process-only one-shots
/// (use [`SharedRing::create_anon`] instead, which skips the file
/// entirely), or hot paths that always send (the lazy branch costs
/// one extra atomic load per op vs holding `&SharedRing` directly).
///
/// **Hot-path tip:** materialise once outside your loop and reuse
/// the returned `&SharedRing` reference so the lazy branch lives
/// outside the inner loop.
pub struct LazySharedRing {
    path: std::path::PathBuf,
    capacity: usize,
    inner: std::sync::OnceLock<SharedRing>,
}

impl LazySharedRing {
    /// Construct a lazy ring. No syscalls; just stores the path and
    /// capacity for the deferred create.
    pub fn new(path: impl Into<std::path::PathBuf>, capacity: usize) -> Self {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        Self {
            path: path.into(),
            capacity,
            inner: std::sync::OnceLock::new(),
        }
    }

    /// Materialise the inner ring, paying the setup cost on the
    /// first call and returning the cached reference thereafter.
    pub fn get(&self) -> Result<&SharedRing, RingError> {
        if let Some(ring) = self.inner.get() {
            return Ok(ring);
        }
        let ring = SharedRing::create(&self.path, self.capacity)?;
        // OnceLock::set returns Err if a concurrent caller already
        // populated it; either way the subsequent get() returns
        // whichever instance won the race.
        match self.inner.set(ring) {
            Ok(()) => Ok(self.inner.get().expect("OnceLock just populated")),
            Err(_lost) => Ok(self.inner.get().expect("another thread populated")),
        }
    }

    /// Whether the underlying ring has been materialised yet.
    pub fn is_initialised(&self) -> bool {
        self.inner.get().is_some()
    }

    /// Forwarded [`SharedRing::try_push`]; materialises on first call.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        self.get()?.try_push(payload)
    }

    /// Forwarded [`SharedRing::try_pop`]; materialises on first call.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.get()?.try_pop(out)
    }
}

/// Initialise the Vyukov ring layout in a freshly-mapped buffer.
/// Sets the header magic + capacity + counters, then writes each
/// slot's sequence number to its index (Vyukov: slot[i] is ready
/// for producer i).
///
/// Shared by [`SharedRing::create`] (file-backed),
/// [`SharedRing::create_anon`] (anonymous), and the lazy
/// initialiser triggered on first use.
fn init_ring_layout(mmap: &mut MmapMut, capacity: usize) {
    unsafe { init_ring_layout_raw(mmap.as_mut_ptr(), capacity) };
}

/// Backing-agnostic layout init. Writes the Vyukov header and slot
/// sequence array at the given raw pointer. Caller guarantees that
/// `ptr` points to at least `ring_file_size(capacity)` bytes of
/// mutable, suitably-aligned memory.
unsafe fn init_ring_layout_raw(ptr: *mut u8, capacity: usize) {
    let header_ptr = ptr as *mut RingHeader;
    unsafe {
        std::ptr::write(header_ptr, RingHeader {
            magic: RING_MAGIC,
            capacity: capacity as u64,
            slot_size: SLOT_SIZE as u64,
            epoch: AtomicU64::new(0),
            _pad_meta: [0; 64 - 32],
            producer_seq: AtomicU64::new(0),
            _pad_prod: [0; 64 - 8],
            consumer_seq: AtomicU64::new(0),
            _pad_cons: [0; 64 - 8],
        });
    }
    let slots_base = unsafe { ptr.add(std::mem::size_of::<RingHeader>()) };
    for i in 0..capacity {
        let slot_ptr = unsafe { slots_base.add(i * SLOT_SIZE) as *mut Slot };
        unsafe {
            std::ptr::write(slot_ptr, Slot {
                sequence: AtomicU64::new(i as u64),
                payload: UnsafeCell::new([0; PAYLOAD_BYTES]),
            });
        }
    }
}

/// Cross-thread / cross-process / disk-persistent MPMC ring.
///
/// Payloads must fit in [`PAYLOAD_BYTES`]; larger items must be
/// chunked by the caller.
///
/// `_file` is `None` for rings created via [`SharedRing::create_anon`]
/// (anonymous in-memory mapping, in-process only) and `Some` for
/// file-backed rings.
/// Backing-store discriminator for `SharedRing`. Holds the
/// underlying memory owner so it stays alive for the lifetime of
/// the ring; raw byte access goes through `SharedRing::raw_ptr`.
/// The held values are intentionally never read directly (lifetime
/// extension only).
#[allow(dead_code)]
enum SharedRingBacking {
    /// Anonymous in-process memory.
    Anon(MmapMut),
    /// File-backed (cross-process via page cache).
    File(File, MmapMut),
    /// Named RAM-resident shared memory (cross-process, no page cache).
    Shm(crate::shm_file::ShmFile),
    /// Caller-owned region (huge / large pages, or any `RegionOwner`).
    Region(Box<dyn crate::spsc_ring::RegionOwner>),
}

pub struct SharedRing {
    _backing: SharedRingBacking,
    raw_ptr: *mut u8,
    capacity: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedRing {}
unsafe impl Sync for SharedRing {}

impl subetha_sidecar::AdaptiveInstance for SharedRing {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError {
    /// Ring is full; producer cannot insert.
    Full,
    /// Ring is empty; consumer cannot drain.
    Empty,
    /// File-backed mapping exists but the magic / capacity does not
    /// match the requested layout.
    LayoutMismatch,
    /// Payload exceeds [`PAYLOAD_BYTES`].
    PayloadTooLarge,
    /// The operation requires ordering stamps but the ring was not
    /// constructed with `with_ordering_stamps()`.
    NotStamped,
    /// Merge-mode pop on a multi-consumer ring requires the drainer
    /// lease and another consumer currently holds it. The caller
    /// backs off and retries; when the holder releases (or its
    /// heartbeat goes stale past the grace window) a later pop
    /// acquires the lease automatically.
    NotDrainer,
    /// A shape morph was requested while the previous shape's
    /// backing still holds an undrained backlog. The consumer
    /// drains it through the normal pop path (the stale walk);
    /// retry the morph once it has caught up - the sidecar's scan
    /// loop does exactly that.
    StaleBacklog,
    /// I/O error opening or mapping the file.
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for RingError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

impl SharedRing {
    /// Create or initialise a new ring backed by `path`. `capacity`
    /// must be a power of two. The file is truncated to the exact
    /// size needed. Use [`SharedRing::open`] to attach to an
    /// existing ring without re-initialising.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = ring_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        // No warm-up here: init below writes every slot line, so the
        // pages get touched either way and the populate syscall is
        // pure overhead (measured +2.2 ms on a 32 MiB ring).
        init_ring_layout(&mut mmap, capacity);
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: SharedRingBacking::File(file, mmap),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Create an anonymous in-memory ring with no backing file. Same
    /// byte layout + concurrency protocol as [`SharedRing::create`],
    /// but the mapping is private to this process so cross-process
    /// visibility is not available.
    ///
    /// **Use when:** one-shot scripts, in-process pipelines, tests
    /// that do not need cross-process or disk-persistent semantics.
    /// Skips the file create + ftruncate + first-page-fault cost
    /// `create` pays (~600 us on Zen+ R7 2700 / Windows 11), so
    /// short-lived sessions amortise much faster.
    ///
    /// **Do NOT use when:** another process needs to attach to the
    /// same ring (use [`SharedRing::create`] + [`SharedRing::open`]
    /// for that path), or when durability across restart matters.
    pub fn create_anon(capacity: usize) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = ring_file_size(capacity);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        init_ring_layout(&mut mmap, capacity);
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: SharedRingBacking::Anon(mmap),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Build a fresh ring on top of a named RAM-resident
    /// shared-memory backing. Cross-process visible via the
    /// `logical_name` of the underlying `ShmFile`; never touches the
    /// page cache. The `ShmFile` must be sized to at least
    /// `ring_file_size(capacity)` bytes.
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = ring_file_size(capacity);
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        let slice = shm.as_mut_slice();
        let raw_ptr = slice.as_mut_ptr();
        unsafe { init_ring_layout_raw(raw_ptr, capacity) };
        Ok(Self {
            _backing: SharedRingBacking::Shm(shm),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing named ShmFs-backed ring. Validates magic +
    /// capacity. Does NOT re-initialize.
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile,
        expected_capacity: usize,
    ) -> Result<Self, RingError> {
        let total = ring_file_size(expected_capacity);
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        let slice = shm.as_mut_slice();
        let raw_ptr = slice.as_mut_ptr();
        let header = unsafe { &*(raw_ptr as *const RingHeader) };
        if header.magic != RING_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != SLOT_SIZE as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        Ok(Self {
            _backing: SharedRingBacking::Shm(shm),
            raw_ptr, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Build a fresh Vyukov MPMC ring laid out in caller-owned memory
    /// (huge / large pages, or any
    /// [`RegionOwner`](crate::spsc_ring::RegionOwner)). The region must
    /// hold at least `ring_file_size(capacity)` bytes; the ring owns it
    /// for its lifetime so the pages stay mapped. This is the global-
    /// FIFO MPMC primitive on large pages; the sharded grid
    /// (`SharedRingMpmc::create_grid_in_region`) is the per-producer-FIFO
    /// counterpart.
    pub fn create_in_region<R: crate::spsc_ring::RegionOwner>(
        mut region: R, capacity: usize,
    ) -> Result<Self, RingError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        if region.region_len() < ring_file_size(capacity) {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = region.region_ptr();
        if !(raw_ptr as usize).is_multiple_of(std::mem::align_of::<RingHeader>()) {
            return Err(RingError::LayoutMismatch);
        }
        unsafe { init_ring_layout_raw(raw_ptr, capacity) };
        Ok(Self {
            _backing: SharedRingBacking::Region(Box::new(region)),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Attach to an existing Vyukov ring already laid out in `region`
    /// (e.g. a named `LargePageSection` another process created).
    /// Validates the header; does NOT re-initialise.
    pub fn open_in_region<R: crate::spsc_ring::RegionOwner>(
        mut region: R, expected_capacity: usize,
    ) -> Result<Self, RingError> {
        if region.region_len() < ring_file_size(expected_capacity) {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = region.region_ptr();
        if !(raw_ptr as usize).is_multiple_of(std::mem::align_of::<RingHeader>()) {
            return Err(RingError::LayoutMismatch);
        }
        let header = unsafe { &*(raw_ptr as *const RingHeader) };
        if header.magic != RING_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != SLOT_SIZE as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        Ok(Self {
            _backing: SharedRingBacking::Region(Box::new(region)),
            raw_ptr, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Wrap this ring in a [`LazySharedRing`] so subsequent attaches
    /// at the same path can be deferred until first use. The eagerly-
    /// constructed ring stays valid; this helper just hands you the
    /// type's lazy constructor for symmetry.
    pub fn into_lazy(path: impl Into<std::path::PathBuf>, capacity: usize) -> LazySharedRing {
        LazySharedRing::new(path, capacity)
    }

    /// Open an existing ring at `path`. Validates magic + capacity.
    /// Returns [`RingError::LayoutMismatch`] when the file's size
    /// does not match a ring of `expected_capacity` slots, OR when
    /// the on-disk header reports different magic / capacity.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, RingError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = ring_file_size(expected_capacity);
        // File-size pre-check: refuse to map past EOF so callers
        // get a clean LayoutMismatch instead of the OS's
        // PermissionDenied / EINVAL.
        let actual_len = file.metadata()?.len();
        if (actual_len as usize) < total {
            return Err(RingError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        // The opener's first traffic pass otherwise faults per 4 KiB
        // across the whole ring; populate in one call instead.
        crate::mmf_warm::warm_mmap(&mut mmap);
        let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
        if header.magic != RING_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != SLOT_SIZE as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _backing: SharedRingBacking::File(file, mmap),
            raw_ptr, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    pub fn header(&self) -> &RingHeader {
        unsafe { &*(self.raw_ptr as *const RingHeader) }
    }

    #[inline]
    fn slot(&self, idx: usize) -> &Slot {
        let slots_base = unsafe {
            self.raw_ptr.add(std::mem::size_of::<RingHeader>())
        };
        unsafe { &*(slots_base.add((idx & (self.capacity - 1)) * SLOT_SIZE) as *const Slot) }
    }

    /// Try to push `payload` into the ring. Returns `Err(Full)` when
    /// the ring is full.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        if payload.len() > PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let header = self.header();
        loop {
            let pos = header.producer_seq.load(Ordering::Relaxed);
            let slot = self.slot(pos as usize);
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as i64 - pos as i64;
            if diff == 0 {
                // Slot is ours to claim; CAS producer_seq forward.
                // Write-intent prefetch: the CAS needs the line in
                // Modified state; requesting it now collapses the
                // upgrade the RMW pays after the Relaxed load above
                // brought it in Shared.
                crate::cache_ops::prefetchw(
                    &header.producer_seq as *const _ as *const u8,
                );
                if header.producer_seq.compare_exchange_weak(
                    pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed,
                ).is_ok() {
                    // Write payload. Plain `ptr::copy_nonoverlapping`
                    // on purpose: at one-line sizes the inlined
                    // baseline codegen beats a dispatched wide-
                    // register kernel (examples/cacheline_probe.rs).
                    unsafe {
                        let dst = (*slot.payload.get()).as_mut_ptr();
                        std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
                        if payload.len() < PAYLOAD_BYTES {
                            std::ptr::write_bytes(
                                dst.add(payload.len()), 0,
                                PAYLOAD_BYTES - payload.len(),
                            );
                        }
                    }
                    // Publish: bump sequence so consumer sees it.
                    slot.sequence.store(pos + 1, Ordering::Release);
                    // The slot line's next reader is the consumer on
                    // another core: demote it toward the shared LLC
                    // (NOP on silicon without CLDEMOTE).
                    crate::cache_ops::cldemote(slot as *const Slot as *const u8);
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::ring::OP_PUSH, 0);
                    return Ok(());
                }
                // CAS lost; retry.
            } else if diff < 0 {
                // Slot still holds an unconsumed value; ring full.
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ring::OP_PUSH, 1); // contention/full
                return Err(RingError::Full);
            } else {
                // Another producer raced ahead; retry.
                std::hint::spin_loop();
            }
        }
    }

    /// Single-producer fast path: skip the CAS on `producer_seq`.
    ///
    /// **Caller contract:** the caller guarantees only one thread / one
    /// process is calling [`try_push_spsc`](Self::try_push_spsc) on
    /// this ring at a time. Concurrent producers will corrupt the
    /// counter; use [`try_push`](Self::try_push) for MPMC.
    ///
    /// Saves the `compare_exchange_weak` on `producer_seq` that the
    /// MPMC path needs to defend against racing producers. Two atomics
    /// per push (1 Acquire load on the slot's sequence + 1 Release
    /// store on the slot's sequence) plus one Release store on
    /// `producer_seq`, vs the MPMC path's 1 load + 1 CAS + 1 load + 1
    /// store. Net: ~25% less atomic traffic per push.
    ///
    /// Also skips the per-op `Observation` push to the sidecar ring.
    /// Use [`try_push`](Self::try_push) when you want sidecar
    /// observability on the hot path.
    pub fn try_push_spsc(&self, payload: &[u8]) -> Result<(), RingError> {
        if payload.len() > PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let header = self.header();
        let pos = header.producer_seq.load(Ordering::Relaxed);
        let slot = self.slot(pos as usize);
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != pos {
            // Slot still holds an unconsumed value (seq < pos+1 means
            // we lapped the consumer). Ring is full.
            return Err(RingError::Full);
        }
        unsafe {
            let dst = (*slot.payload.get()).as_mut_ptr();
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
            if payload.len() < PAYLOAD_BYTES {
                std::ptr::write_bytes(
                    dst.add(payload.len()), 0,
                    PAYLOAD_BYTES - payload.len(),
                );
            }
        }
        // Bump producer_seq with a single Relaxed store; we are the
        // sole producer so no other thread can race against this CAS.
        // The Release on slot.sequence below carries the happens-before
        // edge for both the payload write and the producer_seq update.
        header.producer_seq.store(pos + 1, Ordering::Relaxed);
        slot.sequence.store(pos + 1, Ordering::Release);
        // Next reader of this line is the consumer on another core.
        crate::cache_ops::cldemote(slot as *const Slot as *const u8);
        Ok(())
    }

    /// Single-consumer fast path: skip the CAS on `consumer_seq`.
    ///
    /// **Caller contract:** the caller guarantees only one thread / one
    /// process is calling [`try_pop_spsc`](Self::try_pop_spsc) on this
    /// ring at a time. Concurrent consumers will corrupt the counter;
    /// use [`try_pop`](Self::try_pop) for MPMC.
    ///
    /// Same mirror-image savings as
    /// [`try_push_spsc`](Self::try_push_spsc): two atomics + one
    /// Release store per pop, no CAS, no sidecar observation push.
    pub fn try_pop_spsc(&self, out: &mut [u8]) -> Result<usize, RingError> {
        if out.len() < PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let header = self.header();
        let pos = header.consumer_seq.load(Ordering::Relaxed);
        let slot = self.slot(pos as usize);
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != pos + 1 {
            // Producer hasn't published this slot yet.
            return Err(RingError::Empty);
        }
        unsafe {
            let src = (*slot.payload.get()).as_ptr();
            std::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), PAYLOAD_BYTES);
        }
        // Sole consumer: Relaxed store on consumer_seq is fine; the
        // Release on slot.sequence below carries the happens-before
        // edge that frees this slot for the next producer.
        header.consumer_seq.store(pos + 1, Ordering::Relaxed);
        slot.sequence.store(pos + self.capacity as u64, Ordering::Release);
        // The freed slot's next toucher is the producer.
        crate::cache_ops::cldemote(slot as *const Slot as *const u8);
        Ok(PAYLOAD_BYTES)
    }

    /// The publish signal for the consumer's NEXT pop: the
    /// sequence atom of the slot at the current consumer position.
    /// A producer publishing that slot Release-stores this exact
    /// atom, so a monitor-wait armed on it wakes on the publish.
    /// Recompute after every successful pop - the position (and
    /// therefore the slot) advances.
    pub fn next_pop_signal(&self) -> &AtomicU64 {
        let pos = self.header().consumer_seq.load(Ordering::Relaxed);
        &self.slot(pos as usize).sequence
    }

    /// Try to pop one payload into `out`. On success, returns the
    /// number of bytes written. On `Err(Empty)`, the ring is empty.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        if out.len() < PAYLOAD_BYTES {
            return Err(RingError::PayloadTooLarge);
        }
        let header = self.header();
        loop {
            let pos = header.consumer_seq.load(Ordering::Relaxed);
            let slot = self.slot(pos as usize);
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as i64 - (pos + 1) as i64;
            if diff == 0 {
                // Slot is ready for us; CAS consumer_seq forward.
                // Write-intent prefetch ahead of the RMW (see
                // try_push).
                crate::cache_ops::prefetchw(
                    &header.consumer_seq as *const _ as *const u8,
                );
                if header.consumer_seq.compare_exchange_weak(
                    pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed,
                ).is_ok() {
                    // Read payload.
                    unsafe {
                        let src = (*slot.payload.get()).as_ptr();
                        std::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), PAYLOAD_BYTES);
                    }
                    // Release the slot for the producer who will use
                    // it at position pos + capacity.
                    slot.sequence.store(pos + self.capacity as u64, Ordering::Release);
                    // The freed slot's next toucher is the producer.
                    crate::cache_ops::cldemote(slot as *const Slot as *const u8);
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::ring::OP_POP, 0);
                    return Ok(PAYLOAD_BYTES);
                }
                // CAS lost; retry.
            } else if diff < 0 {
                // No item yet.
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ring::OP_POP, 2); // empty
                return Err(RingError::Empty);
            } else {
                // Producer raced ahead by more than one; retry.
                std::hint::spin_loop();
            }
        }
    }

    /// Force the underlying file's dirty pages to disk. Only
    /// meaningful for file-backed rings; no-op for anonymous and
    /// ShmFs-backed rings (which never touch disk).
    pub fn flush(&self) -> Result<(), RingError> {
        match &self._backing {
            SharedRingBacking::File(_, mmap) => {
                mmap.flush()?;
            }
            SharedRingBacking::Anon(_)
            | SharedRingBacking::Shm(_)
            | SharedRingBacking::Region(_) => {}
        }
        Ok(())
    }

    /// Non-blocking flush; lets the OS schedule the writeback. Only
    /// meaningful for file-backed rings; no-op otherwise.
    pub fn flush_async(&self) -> Result<(), RingError> {
        match &self._backing {
            SharedRingBacking::File(_, mmap) => {
                mmap.flush_async()?;
            }
            SharedRingBacking::Anon(_)
            | SharedRingBacking::Shm(_)
            | SharedRingBacking::Region(_) => {}
        }
        Ok(())
    }

    /// Current producer sequence number (monotonic; wraps via
    /// modulo-capacity on slot index).
    pub fn producer_seq(&self) -> u64 {
        self.header().producer_seq.load(Ordering::Acquire)
    }

    /// Current consumer sequence number.
    pub fn consumer_seq(&self) -> u64 {
        self.header().consumer_seq.load(Ordering::Acquire)
    }

    /// Approximate items waiting to be drained.
    pub fn approx_len(&self) -> usize {
        let p = self.producer_seq();
        let c = self.consumer_seq();
        p.saturating_sub(c) as usize
    }

    /// Find the first slot in the claimed-but-undrained window
    /// `[consumer_seq, producer_seq)` whose sequence number is stuck
    /// at `pos` instead of having advanced to `pos + 1` (published).
    /// Returns `Some(pos)` for the first stuck position, `None` if
    /// every claimed slot has been published.
    ///
    /// **Use for:** sidecar-driven recovery from a producer that
    /// crashed between claiming a slot (CAS on `producer_seq`) and
    /// publishing it (Release-store on `slot.sequence`). The window
    /// where a crash leaves a permanent hole is narrow but real for
    /// any Vyukov MPMC; this is the scan that finds those holes.
    ///
    /// **Hot-path cost:** zero. This method is only called by the
    /// sidecar when its Empty-observation analysis decides a ring is
    /// stuck. `try_push` and `try_pop` never touch it.
    ///
    /// **Scan cost:** O(producer_seq - consumer_seq) in the worst
    /// case (typically small; if the window is large the ring is
    /// already saturated and the scan dominates nothing).
    pub fn next_stuck_slot(&self, from: u64) -> Option<u64> {
        let producer_seq = self.header().producer_seq.load(Ordering::Acquire);
        let consumer_seq = self.header().consumer_seq.load(Ordering::Acquire);
        let start = from.max(consumer_seq);
        for pos in start..producer_seq {
            let slot = self.slot(pos as usize);
            let seq = slot.sequence.load(Ordering::Acquire);
            // Stuck: producer CAS'd producer_seq forward but never
            // published the Release-store on slot.sequence.
            if seq == pos {
                return Some(pos);
            }
        }
        None
    }

    /// Heal a slot stuck in the claimed-but-never-published state by
    /// advancing its sequence number from `pos` to `pos + 1`. The
    /// next consumer at this position drains the slot in normal
    /// `try_pop` order; its payload bytes are whatever the dying
    /// producer happened to write before crashing (or initial zeros
    /// if the producer crashed before any payload write).
    ///
    /// **Caller contract:** the caller must independently confirm
    /// that the producer which claimed this slot will never publish
    /// it (process dead, lease expired, application-level timeout
    /// elapsed). `SharedRing` does not record per-slot producer
    /// identity, so this method cannot make that determination on
    /// its own. Calling without dead-producer confirmation will
    /// data-race a live producer that is about to publish; the
    /// race is benign for the CAS itself (the producer's Release
    /// publishes the same value `pos + 1` we are trying to publish,
    /// so the CAS just returns `Ok(false)`) but the consumer drains
    /// a slot the producer never finished writing.
    ///
    /// **Where the dead-producer signal comes from:** the canonical
    /// signal is [`HeartbeatTable`](crate::HeartbeatTable) +
    /// [`FailoverWatchdog`](crate::FailoverWatchdog). Register each
    /// producer with a heartbeat; the watchdog declares a process
    /// dead when its heartbeat goes stale beyond the grace period,
    /// then walks the rings that producer touched and calls
    /// `heal_stuck_slot(pos)` for each stuck position
    /// [`next_stuck_slot`](Self::next_stuck_slot) returns.
    ///
    /// **Returns:** `Ok(true)` if the slot was stuck and is now
    /// healed (CAS succeeded; consumer can drain it).
    /// `Ok(false)` if the slot was not stuck (sequence already at
    /// `pos + 1` or beyond, or `pos` outside the
    /// `[consumer_seq, producer_seq)` window). Returns `Err` only
    /// on `PayloadTooLarge` style protocol misuse.
    ///
    /// **Hot-path cost:** zero. Only invoked from sidecar recovery.
    /// The heal itself is one atomic CAS on the slot's sequence
    /// number; no payload write, no other state touched.
    pub fn heal_stuck_slot(&self, pos: u64) -> Result<bool, RingError> {
        let header = self.header();
        let producer_seq = header.producer_seq.load(Ordering::Acquire);
        let consumer_seq = header.consumer_seq.load(Ordering::Acquire);
        if pos < consumer_seq || pos >= producer_seq {
            // Outside the claimed-but-undrained window. Either the
            // slot is already drained, or producer_seq never claimed
            // pos.
            return Ok(false);
        }
        let slot = self.slot(pos as usize);
        // CAS from `pos` (claimed, never published) to `pos + 1`
        // (published). If a live producer races and publishes
        // concurrently, the producer's Release-store wrote `pos + 1`
        // first; our CAS sees `pos + 1` (not `pos`) and returns
        // Err -> Ok(false). No data loss in either branch.
        match slot.sequence.compare_exchange(
            pos,
            pos + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ring::OP_PUSH, 4); // bit 2 = healed-tombstone marker
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-test-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_open_round_trip() {
        let p = tmp_path("create-open");
        {
            let _r = SharedRing::create(&p, 16).unwrap();
        }
        // Reopen with same capacity.
        let r2 = SharedRing::open(&p, 16).unwrap();
        assert_eq!(r2.capacity(), 16);
        std::fs::remove_file(&p).ok();
    }

    /// Simulate a producer crashing between claim and publish: take
    /// over the slot manually using direct atomic ops, leaving
    /// producer_seq advanced but slot.sequence stuck at `pos`.
    fn create_stuck_slot(ring: &SharedRing, pos: u64) {
        let header = ring.header();
        // Force producer_seq to pos+1 (as if a producer claimed
        // the slot and then died).
        header.producer_seq.store(pos + 1, Ordering::Release);
        // slot.sequence stays at `pos` (its initial Vyukov value
        // for the pos-th producer): a producer "claimed" it but
        // never published. This is exactly the post-crash state.
        assert_eq!(
            ring.slot(pos as usize).sequence.load(Ordering::Acquire),
            pos,
            "test setup: slot.sequence must still be at initial value",
        );
    }

    #[test]
    fn stuck_slot_blocks_consumer_then_heal_unblocks() {
        // E2E: deliberate stuck slot, consumer hangs, heal unblocks.
        let ring = SharedRing::create_anon(8).unwrap();
        create_stuck_slot(&ring, 0);

        // Consumer at pos=0 sees Empty even though producer_seq says
        // there's an item; this is the stuck-slot pathology.
        let mut out = [0u8; PAYLOAD_BYTES];
        assert_eq!(ring.try_pop(&mut out).unwrap_err(), RingError::Empty);
        assert_eq!(ring.try_pop(&mut out).unwrap_err(), RingError::Empty);
        assert_eq!(
            ring.approx_len(),
            1,
            "producer_seq advanced past 0 but consumer sees 0",
        );

        // Sidecar discovers the stuck slot via the scan.
        let stuck = ring.next_stuck_slot(0).expect("scan must find pos=0");
        assert_eq!(stuck, 0);

        // Heal: caller confirmed the producer is dead via its own
        // heartbeat machinery (out of band for this test).
        assert!(ring.heal_stuck_slot(stuck).unwrap());

        // Consumer now drains the healed slot.
        let n = ring.try_pop(&mut out).expect("healed slot must drain");
        assert_eq!(n, PAYLOAD_BYTES);
        assert_eq!(ring.consumer_seq(), 1, "consumer advanced past the heal");

        // No more stuck slots.
        assert!(ring.next_stuck_slot(0).is_none());

        // Ring fully functional after heal: pushes and pops work
        // through the rest of the lap.
        for i in 1..5u8 {
            ring.try_push(&[i; 8]).unwrap();
            ring.try_pop(&mut out).unwrap();
        }
    }

    #[test]
    fn heal_non_stuck_slot_returns_false() {
        let ring = SharedRing::create_anon(4).unwrap();
        ring.try_push(&[1u8; 8]).unwrap();
        // Slot 0 has been pushed (sequence == 1, not 0).
        assert!(!ring.heal_stuck_slot(0).unwrap(),
                "heal of an already-published slot must be a no-op");

        // Out-of-window position: producer_seq is 1, so pos=5 is
        // beyond the claimed window.
        assert!(!ring.heal_stuck_slot(5).unwrap(),
                "heal of out-of-window position must be a no-op");
    }

    #[test]
    fn heal_loses_race_with_concurrent_producer_publish() {
        // Build a scenario where the heal CAS observes the producer
        // already published: the slot's sequence is pos+1 when the
        // heal tries the CAS pos -> pos+1, so CAS fails and the
        // method returns Ok(false). No payload was clobbered.
        let ring = SharedRing::create_anon(4).unwrap();
        // Producer pushes pos=0 properly (sequence becomes 1).
        ring.try_push(&[0xCDu8; 8]).unwrap();

        // Heal sees seq=1, not 0, so CAS fails -> Ok(false).
        assert!(!ring.heal_stuck_slot(0).unwrap());

        // Consumer drains the original published payload, NOT a
        // tombstone: heal did not corrupt the slot.
        let mut out = [0u8; PAYLOAD_BYTES];
        ring.try_pop(&mut out).unwrap();
        assert_eq!(&out[..8], &[0xCDu8; 8]);
    }

    #[test]
    fn next_stuck_slot_scans_only_claimed_window() {
        let ring = SharedRing::create_anon(8).unwrap();
        // Empty window: no stuck slots possible.
        assert!(ring.next_stuck_slot(0).is_none());

        // Push two normal items. No stuck slots yet.
        ring.try_push(&[1u8; 8]).unwrap();
        ring.try_push(&[2u8; 8]).unwrap();
        assert!(ring.next_stuck_slot(0).is_none());

        // Stick slot 2 (producer claimed but never published).
        create_stuck_slot(&ring, 2);
        // Window is [0, 3); slots 0 and 1 are published (drainable),
        // slot 2 is stuck. Scanner should land on 2.
        assert_eq!(ring.next_stuck_slot(0), Some(2));
    }

    #[test]
    fn spsc_fast_path_round_trip() {
        // Sole producer + sole consumer in two threads; verifies the
        // CAS-free fast paths preserve the same byte layout and
        // ordering guarantees as the MPMC path.
        let ring = std::sync::Arc::new(SharedRing::create_anon(16).unwrap());
        let ring_p = ring.clone();
        let ring_c = ring.clone();
        const N: u32 = 1_000;

        let producer = thread::spawn(move || {
            for i in 0..N {
                let mut buf = [0u8; PAYLOAD_BYTES];
                buf[..4].copy_from_slice(&i.to_le_bytes());
                while ring_p.try_push_spsc(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut out = [0u8; PAYLOAD_BYTES];
            let mut sum: u64 = 0;
            let mut received = 0u32;
            while received < N {
                if ring_c.try_pop_spsc(&mut out).is_ok() {
                    sum += u32::from_le_bytes(out[..4].try_into().unwrap()) as u64;
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            sum
        });

        producer.join().unwrap();
        let sum = consumer.join().unwrap();
        let expected: u64 = (0..N).map(|i| i as u64).sum();
        assert_eq!(sum, expected, "SPSC fast-path lost or duplicated items");
    }

    #[test]
    fn spsc_fast_path_reports_full_on_lap() {
        // Sole producer fills the ring without a consumer; the SPSC
        // path must return Full once we've published `capacity`
        // items and reach the slot the consumer hasn't drained yet.
        let ring = SharedRing::create_anon(4).unwrap();
        for i in 0..4u8 {
            ring.try_push_spsc(&[i; 8]).unwrap();
        }
        assert_eq!(
            ring.try_push_spsc(&[99u8; 8]).unwrap_err(),
            RingError::Full,
        );
        // After consuming one slot via the SPSC pop, push works again.
        let mut out = [0u8; PAYLOAD_BYTES];
        ring.try_pop_spsc(&mut out).unwrap();
        ring.try_push_spsc(&[99u8; 8]).unwrap();
    }

    #[test]
    fn anon_ring_pushes_and_pops() {
        // Anon mode does not touch the filesystem; same byte layout
        // and concurrency protocol so push/pop round-trips work.
        let ring = SharedRing::create_anon(8).unwrap();
        assert_eq!(ring.capacity(), 8);
        let payload = [0xAB; PAYLOAD_BYTES];
        ring.try_push(&payload).unwrap();
        let mut out = [0u8; PAYLOAD_BYTES];
        let n = ring.try_pop(&mut out).unwrap();
        assert_eq!(n, PAYLOAD_BYTES);
        assert_eq!(out, payload);
        // Second pop on empty ring returns Empty.
        assert_eq!(ring.try_pop(&mut out).unwrap_err(), RingError::Empty);
    }

    #[test]
    fn anon_ring_fills_to_capacity() {
        let ring = SharedRing::create_anon(4).unwrap();
        for i in 0..4u32 {
            let mut p = [0u8; PAYLOAD_BYTES];
            p[..4].copy_from_slice(&i.to_le_bytes());
            ring.try_push(&p).unwrap();
        }
        // Fifth push must fail with Full, matching file-backed behaviour.
        assert_eq!(ring.try_push(&[0u8; PAYLOAD_BYTES]).unwrap_err(), RingError::Full);
    }

    #[test]
    fn lazy_ring_defers_setup_until_first_use() {
        let p = tmp_path("lazy-defer");
        let lazy = LazySharedRing::new(&p, 8);
        // is_initialised stays false until something forces materialisation.
        assert!(!lazy.is_initialised());
        // First try_push triggers create.
        lazy.try_push(&[1u8; 8]).unwrap();
        assert!(lazy.is_initialised());
        // Subsequent pop reads back the same byte.
        let mut out = [0u8; PAYLOAD_BYTES];
        let n = lazy.try_pop(&mut out).unwrap();
        assert_eq!(n, PAYLOAD_BYTES);
        assert_eq!(&out[..1], &[1u8]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lazy_ring_never_materialises_when_unused() {
        let p = tmp_path("lazy-never-used");
        let lazy = LazySharedRing::new(&p, 8);
        // Drop without calling any forwarded method.
        drop(lazy);
        // The path must not exist - lazy never created the file.
        assert!(!p.exists(), "lazy ring touched the filesystem despite no use");
    }

    #[test]
    fn lazy_ring_get_caches_reference() {
        let p = tmp_path("lazy-cache");
        let lazy = LazySharedRing::new(&p, 8);
        let r1 = lazy.get().unwrap() as *const SharedRing;
        let r2 = lazy.get().unwrap() as *const SharedRing;
        // Second .get() must return the same materialised instance.
        assert_eq!(r1, r2, "OnceLock returned different instances across calls");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_rejects_wrong_capacity() {
        let p = tmp_path("wrong-cap");
        let _r = SharedRing::create(&p, 16).unwrap();
        match SharedRing::open(&p, 32) {
            Err(RingError::LayoutMismatch) => {}
            other => panic!("expected LayoutMismatch, got {:?}",
                            other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn single_thread_push_pop_round_trip() {
        let p = tmp_path("spsc-rt");
        let r = SharedRing::create(&p, 8).unwrap();
        for i in 0..8u8 {
            let payload = [i, i, i, i];
            r.try_push(&payload).unwrap();
        }
        // Ring should now be full.
        assert_eq!(r.try_push(&[42; 4]).unwrap_err(), RingError::Full);

        let mut buf = [0u8; PAYLOAD_BYTES];
        for i in 0..8u8 {
            let n = r.try_pop(&mut buf).unwrap();
            assert_eq!(n, PAYLOAD_BYTES);
            assert_eq!(&buf[..4], &[i, i, i, i]);
        }
        // Now empty.
        assert_eq!(r.try_pop(&mut buf).unwrap_err(), RingError::Empty);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn mpmc_concurrent_push_pop_preserves_count() {
        let p = tmp_path("mpmc");
        let r = std::sync::Arc::new(SharedRing::create(&p, 1024).unwrap());
        let producers = 4;
        let consumers = 4;
        let per_producer = 5_000usize;
        let total = producers * per_producer;

        let mut handles = vec![];
        for pid in 0..producers {
            let r = r.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_producer {
                    let v = ((pid as u32) << 24) | (i as u32);
                    let bytes = v.to_le_bytes();
                    while r.try_push(&bytes).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        let consumed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..consumers {
            let r = r.clone();
            let consumed = consumed.clone();
            handles.push(thread::spawn(move || {
                let mut buf = [0u8; PAYLOAD_BYTES];
                loop {
                    if consumed.load(std::sync::atomic::Ordering::Acquire) >= total {
                        return;
                    }
                    if r.try_pop(&mut buf).is_ok() {
                        consumed.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(consumed.load(std::sync::atomic::Ordering::Acquire), total);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_data_survives_reopen() {
        let p = tmp_path("disk-persist");
        {
            let r = SharedRing::create(&p, 4).unwrap();
            r.try_push(&[1, 2, 3, 4]).unwrap();
            r.try_push(&[5, 6, 7, 8]).unwrap();
            r.flush().unwrap();
        }
        // Reopen; data should still be there.
        let r2 = SharedRing::open(&p, 4).unwrap();
        let mut buf = [0u8; PAYLOAD_BYTES];
        let _val = r2.try_pop(&mut buf).unwrap();
        assert_eq!(&buf[..4], &[1, 2, 3, 4]);
        let _val = r2.try_pop(&mut buf).unwrap();
        assert_eq!(&buf[..4], &[5, 6, 7, 8]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_in_process_sees_writes() {
        // Two SharedRing handles to the same file in one process: a
        // proxy for cross-process behaviour (they map the same pages).
        let p = tmp_path("cross-handle");
        let producer = SharedRing::create(&p, 16).unwrap();
        let consumer = SharedRing::open(&p, 16).unwrap();
        producer.try_push(b"abc").unwrap();
        let mut buf = [0u8; PAYLOAD_BYTES];
        let _val = consumer.try_pop(&mut buf).unwrap();
        assert_eq!(&buf[..3], b"abc");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn approx_len_tracks_outstanding() {
        let p = tmp_path("approx-len");
        let r = SharedRing::create(&p, 16).unwrap();
        assert_eq!(r.approx_len(), 0);
        r.try_push(&[1]).unwrap();
        r.try_push(&[2]).unwrap();
        r.try_push(&[3]).unwrap();
        assert_eq!(r.approx_len(), 3);
        let mut buf = [0u8; PAYLOAD_BYTES];
        let _val = r.try_pop(&mut buf).unwrap();
        assert_eq!(r.approx_len(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_rejected() {
        let p = tmp_path("payload-too-large");
        let r = SharedRing::create(&p, 4).unwrap();
        let oversized = vec![0u8; PAYLOAD_BYTES + 1];
        assert_eq!(r.try_push(&oversized).unwrap_err(), RingError::PayloadTooLarge);
        std::fs::remove_file(&p).ok();
    }

    /// 64-byte-aligned heap region for exercising the Vyukov
    /// `create_in_region` / `open_in_region` path with no huge-page
    /// privilege. The aligned element type matches what page-backed
    /// regions give for free.
    #[repr(C, align(64))]
    #[derive(Clone, Copy)]
    struct Block64([u8; 64]);

    #[test]
    fn create_in_region_round_trips() {
        let cap = 16usize;
        let bytes = ring_file_size(cap);
        let mut blocks = vec![Block64([0u8; 64]); bytes.div_ceil(64)];

        struct R { ptr: *mut u8, len: usize }
        unsafe impl Send for R {}
        unsafe impl Sync for R {}
        impl crate::spsc_ring::RegionOwner for R {
            fn region_ptr(&mut self) -> *mut u8 { self.ptr }
            fn region_len(&self) -> usize { self.len }
        }

        let ring = SharedRing::create_in_region(
            R { ptr: blocks.as_mut_ptr() as *mut u8, len: bytes }, cap,
        ).unwrap();
        assert_eq!(ring.capacity(), cap);

        // Two laps so producer_seq / consumer_seq wrap past capacity and
        // the Vyukov slot sequence numbers cycle in the region's bytes.
        let mut out = [0u8; PAYLOAD_BYTES];
        for round in 0..2u64 {
            for i in 0..cap as u64 {
                let v = round * cap as u64 + i;
                let mut buf = [0u8; PAYLOAD_BYTES];
                buf[..8].copy_from_slice(&v.to_le_bytes());
                ring.try_push(&buf).unwrap();
            }
            for i in 0..cap as u64 {
                ring.try_pop(&mut out).unwrap();
                assert_eq!(
                    u64::from_le_bytes(out[..8].try_into().unwrap()),
                    round * cap as u64 + i,
                );
            }
        }
        // `blocks` declared before `ring`, so scope order drops it last.
    }

    #[test]
    fn open_in_region_attaches_to_initialised_layout() {
        // One backing, two views: producer lays the Vyukov ring out and
        // pushes; a second handle opens the SAME bytes via
        // open_in_region (no re-init) and drains - the cross-process
        // LargePageSection attach in miniature.
        let cap = 8usize;
        let bytes = ring_file_size(cap);
        let mut blocks = vec![Block64([0u8; 64]); bytes.div_ceil(64)];
        let base = blocks.as_mut_ptr() as *mut u8;
        unsafe { init_ring_layout_raw(base, cap) };

        struct View { ptr: *mut u8, len: usize }
        unsafe impl Send for View {}
        unsafe impl Sync for View {}
        impl crate::spsc_ring::RegionOwner for View {
            fn region_ptr(&mut self) -> *mut u8 { self.ptr }
            fn region_len(&self) -> usize { self.len }
        }

        let producer = SharedRing::open_in_region(
            View { ptr: base, len: bytes }, cap,
        ).unwrap();
        let consumer = SharedRing::open_in_region(
            View { ptr: base, len: bytes }, cap,
        ).unwrap();

        let mut buf = [0u8; PAYLOAD_BYTES];
        buf[..4].copy_from_slice(&0xABCD_u32.to_le_bytes());
        producer.try_push(&buf).unwrap();
        let mut out = [0u8; PAYLOAD_BYTES];
        consumer.try_pop(&mut out).unwrap();
        assert_eq!(out[..4], buf[..4]);
        // `blocks` declared first, so it drops after both views.
    }
}
