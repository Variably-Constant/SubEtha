//! `SharedDequeKhpd` - K-axis Hierarchical Publication Deque, MMF-backed.
//!
//! Companion primitive to [`SharedDeque`](crate::SharedDeque) (the
//! Chase-Lev work-stealing deque) for workloads where the producer
//! batches and the per-line transfer cost dominates the round-trip.
//!
//! ## The amortization lever
//!
//! Chase-Lev pays one cache-line bounce per single-item handoff
//! between owner and thief. KHPD packs `LINE_ITEMS = 3` items into
//! one 64-byte cache line and atomically publishes them with a
//! single Release-store on the line's state word. A thief takes the
//! whole line in one CAS, reading all three items in a single
//! cache-line transfer. The architectural saving is one Release-
//! store per item amortized over three items - measured at 1.16x
//! producer-side throughput vs Chase-Lev on a Zen+ R7 2700 when the
//! workload uses the batch publish API.
//!
//! ## Layout
//!
//! ```text
//! +-----------------------------+
//! | KhpdHeader (128B)           |  magic, capacity, owner_pid,
//! |                             |  tail on its own line,
//! |                             |  head on its own line
//! +-----------------------------+
//! | PublicationLine[0]  (64B)   |  state (8B) + 3 LineItems (48B)
//! | PublicationLine[1]          |  + 8B padding
//! | ...                         |
//! | PublicationLine[capacity-1] |
//! +-----------------------------+
//! ```
//!
//! Each `PublicationLine` is exactly one cache line so adjacent
//! lines never share coherence-traffic lines. `state` is an
//! `AtomicU64` packed as `(epoch: u32 << 32) | (n_items: u16 << 16)
//! | claim: u16`; the publisher writes the line items in place and
//! issues one Release-store on `state` with `claim = CLAIM_BIT` and
//! `n_items` set. The claimer reads `state` Acquire, validates the
//! epoch matches its head, CAS-takes the head, then reads the line
//! items and releases the slot by storing `STATE_EMPTY` for the next
//! round's producer.
//!
//! Each `LineItem` is a 16-byte byte-oriented payload. Callers
//! marshal their own value into the payload at publish time and
//! unmarshal it at steal time. SubEtha's [`Marshal`](subetha_core::Marshal)
//! trait is the recommended packing contract.
//!
//! ## When to use this vs `SharedDeque`
//!
//! - **`SharedDeque<T>` (Chase-Lev)** - per-item dispatch and steal,
//!   strict LIFO at the owner, optimal at low per-item batch size.
//! - **`SharedDequeKhpd` (this primitive)** - producer batches
//!   multiple items per publication line. Beats Chase-Lev by ~16%
//!   on producer-side throughput when the workload calls
//!   [`publish`](SharedDequeKhpd::publish) with several staged items.
//!   On per-item dispatch (one stage + one publish per call), KHPD
//!   gives back the amortization win and may underperform.

#![allow(clippy::missing_errors_doc)]

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering, fence};

use memmap2::{MmapMut, MmapOptions};

/// Magic byte sequence marking a valid KHPD file. ASCII 'WKHP' + ver.
pub const KHPD_MAGIC: u64 = 0x574B_4850_0000_0001;

/// Cache-line size; one publication line per cache line.
pub const KHPD_LINE_SIZE: usize = 64;

/// Items per publication line. State (8 B) + 3 * 16 = 56 B; 8 B
/// trailing padding rounds the line to 64.
pub const LINE_ITEMS: usize = 3;

/// Bytes per [`LineItem`] payload. Callers marshal their value into
/// these 16 bytes (and unmarshal at steal time).
pub const KHPD_ITEM_BYTES: usize = 16;

/// `state` packed-bit-field layout: epoch in the top 32 bits,
/// `n_items` in the next 16, `claim` in the bottom 16.
const STATE_EMPTY: u64 = 0;
const CLAIM_BIT: u64 = 1;

/// File header. Cache-line aligned. `head` and `tail` each get
/// their own cache line to prevent producer and consumer counters
/// from invalidating each other.
#[repr(C, align(64))]
pub struct KhpdHeader {
    /// Magic constant.
    pub magic: u64,
    /// Number of publication lines; always a power of two.
    pub capacity: u64,
    /// Pid of the owner process; informational. Cleared on
    /// `close_owner()`.
    pub owner_pid: AtomicU64,
    /// Epoch counter advanced on owner shutdown.
    pub epoch: AtomicU64,
    /// Padding to push `tail` to its own cache line.
    pub _pad_meta: [u8; 24],
    /// Producer counter. Owner `fetch_add(1)` per published line.
    pub tail: AtomicI64,
    /// Padding to push `head` to its own line.
    pub _pad_tail: [u8; 56],
    /// Consumer counter. Thieves CAS this to claim a line.
    pub head: AtomicI64,
    /// Padding to round the header to two whole cache lines after
    /// `head`.
    pub _pad_head: [u8; 56],
}

/// One item carried in a publication line. 16 bytes.
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LineItem {
    /// Byte-oriented payload. Callers marshal in / unmarshal out;
    /// the KHPD layer treats this as an opaque 16-byte slot.
    pub payload: [u8; KHPD_ITEM_BYTES],
}

impl LineItem {
    /// Build a line item from a caller-supplied byte slice. The
    /// slice must be at most [`KHPD_ITEM_BYTES`] bytes; shorter
    /// slices are zero-padded on the right.
    pub fn new(bytes: &[u8]) -> Result<Self, PushError> {
        if bytes.len() > KHPD_ITEM_BYTES {
            return Err(PushError::PayloadTooLarge);
        }
        let mut item = Self::default();
        item.payload[..bytes.len()].copy_from_slice(bytes);
        Ok(item)
    }

    /// Borrow the 16-byte payload.
    pub fn bytes(&self) -> &[u8; KHPD_ITEM_BYTES] {
        &self.payload
    }
}

/// 64-byte cache-line-sized payload carrying up to [`LINE_ITEMS`] =
/// 3 [`LineItem`] payloads plus a count. The deque-family hybrid
/// [`SharedDequeFcl`](crate::SharedDequeFcl) uses this as the slot
/// type for counter-only Chase-Lev with `K_inner = 3`: each push
/// publishes 3 items in one cache-line write, with NO per-slot
/// atomic and ONE owner-private `bottom` store amortized across the
/// whole batch.
///
/// Layout:
/// - `n_items` (4 B): count of valid items in `items` (1..=3)
/// - `reserved` (4 B): caller-use tag / cache-line alignment
/// - `items` (48 B): the [`LineItem`] payloads
/// - `_pad` (8 B): tail padding to round to exactly 64 B
#[repr(C, align(64))]
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct FatLineItem {
    /// Number of valid items in `items` (1..=[`LINE_ITEMS`]).
    pub n_items: u32,
    /// Reserved for caller use (variant tag / numa hint / etc.).
    pub reserved: u32,
    /// Up to [`LINE_ITEMS`] caller payloads.
    pub items: [LineItem; LINE_ITEMS],
    /// Trailing padding to round the struct to exactly 64 B.
    pub _pad: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<FatLineItem>() == 64);

impl FatLineItem {
    /// Build a fat item from a slice of up to [`LINE_ITEMS`]
    /// [`LineItem`] values. Returns [`PushError::TooManyItems`] if
    /// the slice has more than [`LINE_ITEMS`] elements.
    pub fn from_items(items: &[LineItem]) -> Result<Self, PushError> {
        if items.len() > LINE_ITEMS {
            return Err(PushError::TooManyItems);
        }
        let mut fat = Self {
            n_items: items.len() as u32,
            ..Self::default()
        };
        fat.items[..items.len()].copy_from_slice(items);
        Ok(fat)
    }

    /// Borrow the valid items (`&items[..n_items]`).
    pub fn live_items(&self) -> &[LineItem] {
        let n = (self.n_items as usize).min(LINE_ITEMS);
        &self.items[..n]
    }
}

// SAFETY: `FatLineItem` is `#[repr(C, align(64))]` with explicitly
// laid out fields (n_items: u32 + reserved: u32 + items: [LineItem;
// 3] + _pad: [u8; 8] = 64 bytes), no padding holes, every field is
// itself byte-portable. The bytes are position-independent across
// address spaces; round-trip is a memcpy.
unsafe impl subetha_core::Marshal for FatLineItem {
    const PAYLOAD_BYTES: usize = 64;

    fn marshal(&self, dst: &mut [u8]) {
        // SAFETY: `Self` has size 64, layout is repr(C) with no
        // padding holes (asserted via the const above).
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, 64)
        };
        dst[..64].copy_from_slice(bytes);
    }

    fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
        if src.len() < 64 {
            return Err(subetha_core::MarshalError::ShortBuffer {
                expected: 64,
                got: src.len(),
            });
        }
        let mut out = Self::default();
        // SAFETY: same layout justification as `marshal`.
        let dst_bytes = unsafe {
            std::slice::from_raw_parts_mut(&mut out as *mut Self as *mut u8, 64)
        };
        dst_bytes.copy_from_slice(&src[..64]);
        Ok(out)
    }
}

// SAFETY: `LineItem` is `#[repr(C, align(8))]` over a single
// `[u8; KHPD_ITEM_BYTES]` payload field. The bytes are position-
// independent across address spaces; round-trip is a memcpy.
unsafe impl subetha_core::Marshal for LineItem {
    const PAYLOAD_BYTES: usize = KHPD_ITEM_BYTES;

    fn marshal(&self, dst: &mut [u8]) {
        dst[..KHPD_ITEM_BYTES].copy_from_slice(&self.payload);
    }

    fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
        if src.len() < KHPD_ITEM_BYTES {
            return Err(subetha_core::MarshalError::ShortBuffer {
                expected: KHPD_ITEM_BYTES,
                got: src.len(),
            });
        }
        let mut payload = [0u8; KHPD_ITEM_BYTES];
        payload.copy_from_slice(&src[..KHPD_ITEM_BYTES]);
        Ok(Self { payload })
    }
}

/// One publication line: state + `LINE_ITEMS` items + padding.
#[repr(C, align(64))]
pub struct PublicationLine {
    /// `(epoch:32) << 32 | (n_items:16) << 16 | claim:16`.
    /// `claim` = 0 (empty), 1 (READY for claim).
    pub state: AtomicU64,
    /// Inline items.
    pub items: [LineItem; LINE_ITEMS],
    /// Trailing padding to round to 64 bytes.
    pub _pad: [u8; 8],
}

/// Total file size for a KHPD with `capacity` publication lines.
pub const fn khpd_file_size(capacity: usize) -> usize {
    std::mem::size_of::<KhpdHeader>() + capacity * KHPD_LINE_SIZE
}

/// Outcome of [`SharedDequeKhpd::publish`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// Ring at capacity; consumer has not caught up.
    Full,
    /// Items count exceeds [`LINE_ITEMS`].
    TooManyItems,
    /// Payload exceeds [`KHPD_ITEM_BYTES`].
    PayloadTooLarge,
}

/// Outcome of [`SharedDequeKhpd::steal_line`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Steal {
    /// Got a publication line; carries up to [`LINE_ITEMS`] items.
    Success(StealResult),
    /// Ring was empty (head >= tail).
    Empty,
    /// Lost the CAS race on `head` to a competing thief, or the
    /// publisher has not finished writing this line yet.
    Retry,
}

/// Result of a successful steal: the publication line's items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StealResult {
    /// How many of the `items` slots are filled.
    pub n_items: usize,
    /// The items (only `items[..n_items]` are valid).
    pub items: [LineItem; LINE_ITEMS],
}

/// MMF-backed K-axis Hierarchical Publication Deque. Single owner,
/// arbitrarily many thieves.
pub struct SharedDequeKhpd {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    capacity_mask: i64,
    /// Owner-side staging buffer. Items accumulate here until the
    /// caller calls [`publish`](Self::publish) to flush the buffer
    /// into one or more publication lines. `Mutex` is uncontended
    /// on the hot path (only the owner stages).
    pending: Mutex<Vec<LineItem>>,
}

// SAFETY: all fields are Send. Mmap handle is Send + Sync per
// memmap2. Every line access goes through the per-line state-atomic
// protocol; the `pending` Mutex linearises owner-side accesses.
unsafe impl Send for SharedDequeKhpd {}
// SAFETY: same justification as the Send impl directly above.
unsafe impl Sync for SharedDequeKhpd {}

impl SharedDequeKhpd {
    /// Create a fresh KHPD file. `capacity` rounds up to the next
    /// power of two; minimum 2.
    pub fn create<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        let capacity = capacity.max(2).next_power_of_two();
        let size = khpd_file_size(capacity);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;
        file.set_len(size as u64)?;

        // SAFETY: `map_mut` is unsafe because the kernel cannot
        // prevent another process from truncating the file. This
        // call site upholds the soundness contract by writing only
        // through the KHPD per-line state-atomic protocol; file
        // size is fixed by `set_len` above and never shrunk for the
        // lifetime of any mapping.
        let mut mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };

        let header_ptr = mmap.as_mut_ptr() as *mut KhpdHeader;
        // SAFETY: mmap is page-aligned (well above the 64-byte
        // alignment KhpdHeader requires); the map covers
        // `khpd_file_size(capacity)` bytes by construction.
        unsafe {
            (*header_ptr).magic = KHPD_MAGIC;
            (*header_ptr).capacity = capacity as u64;
            (*header_ptr).owner_pid = AtomicU64::new(std::process::id() as u64);
            (*header_ptr).epoch = AtomicU64::new(0);
            std::ptr::write_bytes((*header_ptr)._pad_meta.as_mut_ptr(), 0, 24);
            (*header_ptr).tail = AtomicI64::new(0);
            std::ptr::write_bytes((*header_ptr)._pad_tail.as_mut_ptr(), 0, 56);
            (*header_ptr).head = AtomicI64::new(0);
            std::ptr::write_bytes((*header_ptr)._pad_head.as_mut_ptr(), 0, 56);
        }

        // Zero the lines (state == 0 == STATE_EMPTY).
        let lines_start = std::mem::size_of::<KhpdHeader>();
        // SAFETY: lines_start..lines_start + capacity*KHPD_LINE_SIZE
        // is the unwritten tail of the map.
        unsafe {
            std::ptr::write_bytes(
                mmap.as_mut_ptr().add(lines_start),
                0,
                capacity * KHPD_LINE_SIZE,
            );
        }

        mmap.flush()?;

        Ok(Self {
            _file: file,
            mmap,
            capacity,
            capacity_mask: (capacity as i64) - 1,
            pending: Mutex::new(Vec::with_capacity(LINE_ITEMS)),
        })
    }

    /// Open an existing KHPD file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        let size = file.metadata()?.len() as usize;
        if size < std::mem::size_of::<KhpdHeader>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "khpd file too small",
            ));
        }
        // SAFETY: same protocol-only-access justification as
        // `create`.
        let mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };
        let header_ptr = mmap.as_ptr() as *const KhpdHeader;
        // SAFETY: map size verified to cover header.
        let (magic, capacity) =
            unsafe { ((*header_ptr).magic, (*header_ptr).capacity as usize) };
        if magic != KHPD_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("khpd magic mismatch {magic:#x}"),
            ));
        }
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("khpd capacity {capacity} not pow2 >= 2"),
            ));
        }
        if size < khpd_file_size(capacity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "khpd file size {size} below expected {}",
                    khpd_file_size(capacity)
                ),
            ));
        }
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            capacity_mask: (capacity as i64) - 1,
            pending: Mutex::new(Vec::with_capacity(LINE_ITEMS)),
        })
    }

    /// Capacity in publication lines (always a power of two).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Owner pid at create time, or 0 after `close_owner()`.
    pub fn owner_pid(&self) -> u64 {
        self.header().owner_pid.load(Ordering::Acquire)
    }

    /// Advance epoch + zero the owner pid on shutdown.
    pub fn close_owner(&self) {
        self.header().owner_pid.store(0, Ordering::Release);
        self.header().epoch.fetch_add(1, Ordering::Release);
    }

    fn header(&self) -> &KhpdHeader {
        // SAFETY: header is at the start of the map; mmap is
        // page-aligned.
        unsafe { &*(self.mmap.as_ptr() as *const KhpdHeader) }
    }

    fn line_ptr(&self, idx: i64) -> *mut PublicationLine {
        let line_idx = (idx & self.capacity_mask) as usize;
        let off = std::mem::size_of::<KhpdHeader>() + line_idx * KHPD_LINE_SIZE;
        // SAFETY: `line_idx` < capacity; `off` is in-bounds + 64-byte aligned.
        unsafe { self.mmap.as_ptr().add(off) as *mut PublicationLine }
    }

    /// Snapshot `(head, tail, ring_size_lines, pending_items)`.
    pub fn snapshot_size(&self) -> (i64, i64, i64, usize) {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        let tail = h.tail.load(Ordering::Acquire);
        let pending = self
            .pending
            .try_lock()
            .map(|g| g.len())
            .unwrap_or(0);
        (head, tail, tail - head, pending)
    }

    /// Owner-side stage. Adds one item to the pending buffer.
    /// Returns the running pending count (so the caller can decide
    /// to flush at [`LINE_ITEMS`]). **Only the owner process may
    /// stage.**
    pub fn stage(&self, item: LineItem) -> Result<usize, PushError> {
        let mut p = self.pending.lock().expect("KHPD pending poisoned");
        p.push(item);
        Ok(p.len())
    }

    /// Owner-side single-call batch publish. Bypasses the
    /// [`stage`](Self::stage)/[`publish`](Self::publish) pair so the
    /// caller pays only ONE Mutex acquire per batch instead of one
    /// per staged item. This is the canonical hot-path API: the
    /// caller hands in a slice of [`LineItem`] values and the method
    /// publishes them into `ceil(items.len() / LINE_ITEMS)`
    /// publication lines with one `tail.fetch_add(n_lines)` plus
    /// one Release-store per line.
    ///
    /// Returns the number of LINES published.
    pub fn publish_batch(&self, items: &[LineItem]) -> Result<usize, PushError> {
        if items.is_empty() {
            return Ok(0);
        }
        // Hold migration_lock-equivalent: serialise against other
        // owner-side publishes by going through the same Mutex the
        // staged path uses.
        let _g = self.pending.lock().expect("KHPD pending poisoned");
        let n_lines = items.len().div_ceil(LINE_ITEMS);
        let h = self.header();
        let head_snap = h.head.load(Ordering::Acquire);
        let tail_snap = h.tail.load(Ordering::Relaxed);
        if (tail_snap - head_snap + n_lines as i64) > self.capacity as i64 {
            return Err(PushError::Full);
        }
        let base = h.tail.fetch_add(n_lines as i64, Ordering::AcqRel);

        // No PREFETCHW here: empirical 30-second criterion bench on
        // Zen+ R7 2700 measured a 12% regression vs the unprefetched
        // path (p = 0.01). KHPD's publication lines are L1d-warm from
        // the prior iteration of `publish_batch`; explicit prefetch
        // pollutes the prefetch queue without payoff. The architectural
        // lever is preserved for Chase-Lev and LOH where the slot line
        // is cold per push.

        let mut it = items.iter();
        for line_i in 0..n_lines {
            let idx = base + line_i as i64;
            let line = self.line_ptr(idx);
            // SAFETY: line is in-bounds + aligned.
            unsafe {
                loop {
                    let st = (*line).state.load(Ordering::Acquire);
                    if st == STATE_EMPTY { break; }
                    std::hint::spin_loop();
                }
                let mut n_filled = 0usize;
                for slot in 0..LINE_ITEMS {
                    match it.next() {
                        Some(item) => {
                            (*line).items[slot] = *item;
                            n_filled += 1;
                        }
                        None => break,
                    }
                }
                let new_state =
                    ((idx as u64) << 32) | ((n_filled as u64) << 16) | CLAIM_BIT;
                (*line).state.store(new_state, Ordering::Release);
            }
        }
        Ok(n_lines)
    }

    /// Owner-side publish. Drains the pending buffer into one or
    /// more publication lines ([`LINE_ITEMS`] items per line). Each
    /// line takes one `tail.fetch_add(1)` plus one Release-store on
    /// the line's state. Returns the number of LINES published.
    pub fn publish(&self) -> Result<usize, PushError> {
        let mut p = self.pending.lock().expect("KHPD pending poisoned");
        if p.is_empty() {
            return Ok(0);
        }
        let total = p.len();
        let n_lines = total.div_ceil(LINE_ITEMS);
        let h = self.header();
        let head_snap = h.head.load(Ordering::Acquire);
        let tail_snap = h.tail.load(Ordering::Relaxed);
        if (tail_snap - head_snap + n_lines as i64) > self.capacity as i64 {
            return Err(PushError::Full);
        }
        let base = h.tail.fetch_add(n_lines as i64, Ordering::AcqRel);

        let mut item_iter = p.drain(..);
        for line_i in 0..n_lines {
            let idx = base + line_i as i64;
            let line = self.line_ptr(idx);
            // Spin-wait for the slot to be reusable. The consumer's
            // release stores `STATE_EMPTY` (= 0); the producer at
            // `idx` spins until state == 0 for this slot's next
            // round.
            //
            // SAFETY: line is in-bounds + aligned.
            unsafe {
                loop {
                    let st = (*line).state.load(Ordering::Acquire);
                    if st == STATE_EMPTY {
                        break;
                    }
                    std::hint::spin_loop();
                }

                // Fill items.
                let mut n_filled = 0usize;
                for i in 0..LINE_ITEMS {
                    match item_iter.next() {
                        Some(item) => {
                            (*line).items[i] = item;
                            n_filled += 1;
                        }
                        None => break,
                    }
                }
                // Pack state: (epoch:32 from idx) | (n_filled:16) |
                // claim:16 == CLAIM_BIT (READY).
                let new_state =
                    ((idx as u64) << 32) | ((n_filled as u64) << 16) | CLAIM_BIT;
                (*line).state.store(new_state, Ordering::Release);
            }
        }
        Ok(n_lines)
    }

    /// Thief-side. Claim one publication line.
    pub fn steal_line(&self) -> Steal {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        fence(Ordering::SeqCst);
        let tail = h.tail.load(Ordering::Acquire);
        if head >= tail {
            return Steal::Empty;
        }
        let line = self.line_ptr(head);
        // SAFETY: line is in-bounds + aligned.
        let state = unsafe { (*line).state.load(Ordering::Acquire) };
        // Validate: state's epoch matches our head and CLAIM_BIT is
        // set.
        let expected_epoch = (head as u64) << 32;
        if state & 0xFFFF_FFFF_0000_0000 != expected_epoch {
            // Publisher has not written this line for our round yet.
            return Steal::Retry;
        }
        if state & CLAIM_BIT == 0 {
            // Line empty (no items for this round). Skip.
            return Steal::Retry;
        }
        // CAS head to claim.
        let won = h
            .head
            .compare_exchange(head, head + 1, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok();
        if !won {
            return Steal::Retry;
        }
        // We own the line; read items and release the slot for the
        // next round.
        //
        // SAFETY: line is in-bounds + aligned; the CAS established
        // exclusive read access for this round.
        let result = unsafe {
            let n_items = ((state >> 16) & 0xFFFF) as usize;
            let n_items = n_items.min(LINE_ITEMS);
            StealResult {
                n_items,
                items: (*line).items,
            }
        };
        // Release the slot: store STATE_EMPTY so the next round's
        // producer (at idx = head + capacity) sees the slot ready.
        //
        // SAFETY: still our slot; the Release synchronises with the
        // next producer's Acquire-spin in `publish`.
        unsafe {
            (*line).state.store(STATE_EMPTY, Ordering::Release);
        }
        Steal::Success(result)
    }

    /// Force any dirty pages to disk.
    pub fn flush_to_disk(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as O};
    use std::thread;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subetha_khpd_{pid}_{nonce}_{name}.bin"));
        p
    }

    fn u32_item(id: u32) -> LineItem {
        LineItem::new(&id.to_le_bytes()).expect("item")
    }

    fn item_id(item: &LineItem) -> u32 {
        u32::from_le_bytes(item.payload[..4].try_into().unwrap())
    }

    #[test]
    fn create_open_round_trips_header() {
        let path = temp_path("create_open");
        let _d = SharedDequeKhpd::create(&path, 8).expect("create");
        let o = SharedDequeKhpd::open(&path).expect("open");
        assert_eq!(o.capacity(), 8);
        assert_eq!(o.owner_pid(), std::process::id() as u64);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = temp_path("badmagic");
        std::fs::write(&path, vec![0u8; 8192]).expect("seed");
        assert!(SharedDequeKhpd::open(&path).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn stage_then_publish_writes_one_line() {
        let path = temp_path("stage_publish");
        let d = SharedDequeKhpd::create(&path, 4).expect("create");
        d.stage(u32_item(1)).expect("stage 1");
        d.stage(u32_item(2)).expect("stage 2");
        let lines = d.publish().expect("publish");
        assert_eq!(lines, 1);
        let (_, tail, sz, pending) = d.snapshot_size();
        assert_eq!(tail, 1);
        assert_eq!(sz, 1);
        assert_eq!(pending, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_spans_multiple_lines() {
        let path = temp_path("multi_line");
        let d = SharedDequeKhpd::create(&path, 4).expect("create");
        // 7 items: 3 + 3 + 1 = 3 lines.
        for i in 1..=7u32 {
            d.stage(u32_item(i)).expect("stage");
        }
        let lines = d.publish().expect("publish");
        assert_eq!(lines, 3);
        let (_, tail, sz, _) = d.snapshot_size();
        assert_eq!(tail, 3);
        assert_eq!(sz, 3);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn steal_returns_items_in_publication_order() {
        let path = temp_path("fifo");
        let d = SharedDequeKhpd::create(&path, 4).expect("create");
        for i in 1..=5u32 {
            d.stage(u32_item(i)).expect("stage");
        }
        d.publish().expect("publish");
        // Line 0 carries (1, 2, 3); line 1 carries (4, 5).
        loop {
            match d.steal_line() {
                Steal::Success(r) => {
                    assert_eq!(r.n_items, 3);
                    assert_eq!(item_id(&r.items[0]), 1);
                    assert_eq!(item_id(&r.items[1]), 2);
                    assert_eq!(item_id(&r.items[2]), 3);
                    break;
                }
                Steal::Empty | Steal::Retry => std::thread::yield_now(),
            }
        }
        loop {
            match d.steal_line() {
                Steal::Success(r) => {
                    assert_eq!(r.n_items, 2);
                    assert_eq!(item_id(&r.items[0]), 4);
                    assert_eq!(item_id(&r.items[1]), 5);
                    break;
                }
                Steal::Empty | Steal::Retry => std::thread::yield_now(),
            }
        }
        loop {
            match d.steal_line() {
                Steal::Empty => break,
                Steal::Retry => continue,
                Steal::Success(_) => panic!("unexpected success after drain"),
            }
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn oversize_payload_rejected() {
        let big = vec![0u8; KHPD_ITEM_BYTES + 1];
        let err = LineItem::new(&big).expect_err("oversize");
        assert_eq!(err, PushError::PayloadTooLarge);
    }

    #[test]
    fn ring_full_at_capacity_returns_full() {
        let path = temp_path("full");
        let d = SharedDequeKhpd::create(&path, 2).expect("create");
        // Fill the ring (2 publication lines * 3 items = 6 items).
        for i in 1..=6u32 {
            d.stage(u32_item(i)).expect("stage");
        }
        d.publish().expect("publish 2 lines");
        // Stage more + publish; ring is full.
        d.stage(u32_item(7)).expect("stage 7");
        let err = d.publish().expect_err("publish past capacity");
        assert_eq!(err, PushError::Full);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn close_owner_zeros_pid_and_advances_epoch() {
        let path = temp_path("close");
        let d = SharedDequeKhpd::create(&path, 2).expect("create");
        let before = d.header().epoch.load(O::Acquire);
        d.close_owner();
        assert_eq!(d.owner_pid(), 0);
        assert_eq!(d.header().epoch.load(O::Acquire), before + 1);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn concurrent_thieves_no_double_take() {
        // Stress: 5000 items via repeated stage + publish; 2 thieves
        // race to drain. Every item must be consumed exactly once.
        let path = temp_path("stress");
        let d = Arc::new(SharedDequeKhpd::create(&path, 64).expect("create"));
        let n = 5_000usize;
        let consumed = Arc::new(AtomicUsize::new(0));
        let sum = Arc::new(AtomicUsize::new(0));

        let mut thieves = Vec::new();
        for _ in 0..2 {
            let d = Arc::clone(&d);
            let consumed = Arc::clone(&consumed);
            let sum = Arc::clone(&sum);
            thieves.push(thread::spawn(move || {
                while consumed.load(O::Relaxed) < n {
                    match d.steal_line() {
                        Steal::Success(r) => {
                            for i in 0..r.n_items {
                                consumed.fetch_add(1, O::Relaxed);
                                sum.fetch_add(item_id(&r.items[i]) as usize, O::Relaxed);
                            }
                        }
                        Steal::Empty | Steal::Retry => std::thread::yield_now(),
                    }
                }
            }));
        }

        // Publisher: stage LINE_ITEMS, publish, repeat until n items.
        let mut pushed = 0usize;
        while pushed < n {
            let want = LINE_ITEMS.min(n - pushed);
            for _ in 0..want {
                d.stage(u32_item(pushed as u32)).expect("stage");
                pushed += 1;
            }
            loop {
                match d.publish() {
                    Ok(_) => break,
                    Err(PushError::Full) => {
                        std::thread::yield_now();
                    }
                    Err(other) => panic!("publish: {other:?}"),
                }
            }
        }

        for t in thieves {
            t.join().expect("thief");
        }
        let expected: usize = (0..n).sum();
        assert_eq!(
            sum.load(O::Relaxed),
            expected,
            "stress sum mismatch (expected every item consumed exactly once)"
        );
        std::fs::remove_file(&path).ok();
    }
}
