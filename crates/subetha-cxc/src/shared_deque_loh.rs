//! `SharedDequeLoh` - LCRQ-on-LIFO Hybrid deque, MMF-backed.
//!
//! Sibling to [`SharedDeque`](crate::SharedDeque) (Chase-Lev) and
//! [`SharedDequeKhpd`](crate::SharedDequeKhpd) (publication-line). LOH
//! targets the producer-fast burst shape: owner-side push goes into a
//! process-private LIFO with *no atomic*; the migration step drains a
//! batch into a Vyukov-sequence-number ring with one
//! `tail.fetch_add(N)` plus N Release-stores. Thieves race on `head`
//! via CAS with a sequence-number check that pre-validates the slot.
//!
//! ## Why this shape
//!
//! Chase-Lev pays one Release-store on `bottom` per item and the
//! steal-side CAS on `top` per claimed item. For per-item request-
//! reply that matches the cost of one cache-line bounce; for a
//! workload that publishes many items per coherence interval
//! (parallel-for fan-out, fork-join leaves) the per-item bookkeeping
//! is unamortized. LOH amortizes by letting the owner stage many
//! items in a private heap (no atomic) and pay one ring-tail update
//! per migration batch.
//!
//! Trade-offs vs Chase-Lev MMF:
//!
//! - **Owner push** drops from one Release-store on `bottom` per item
//!   to a plain `Vec::push` (~3 ns).
//! - **Migration** is one `tail.fetch_add(batch)` plus `batch`
//!   Release-stores on per-slot sequence numbers.
//! - **Thief steal** is one CAS on `head` (same shape as Chase-Lev's
//!   `top` CAS) plus a sequence-number check on the slot. The
//!   wasted-ticket race that pure-XADD LCRQ exhibits is avoided by
//!   gating the CAS on `head < tail`.
//!
//! Where LOH wins per the cost model: bursty dispatch where the
//! per-burst migration amortizes over many items per cache-line
//! bounce. Where LOH does NOT win: single-item request-reply,
//! because there's no batching to amortize against.
//!
//! ## Hot path API: [`publish_batch`](SharedDequeLoh::publish_batch)
//!
//! The canonical producer-fast API takes a slice of [`LineItem`] and
//! migrates the whole batch in one shot. It bypasses the local LIFO
//! entirely, paying exactly one Mutex acquire + one
//! `tail.fetch_add(items.len())` + `items.len()` Release-stores for
//! the call. This is the path that exercises the amortization lever
//! and is the shape benchmarks measure.
//!
//! The [`push`](SharedDequeLoh::push) /
//! [`flush`](SharedDequeLoh::flush) pair is still exposed for callers
//! that want to stage items incrementally and migrate later
//! (autoflushes at a configurable threshold). Per-item `push` does
//! NOT exercise the amortization lever; it pays the same Mutex on
//! every staged item.
//!
//! ## Layout
//!
//! ```text
//! +-----------------------------+
//! | LohHeader (128B)            |  magic, capacity, owner_pid,
//! |                             |  epoch, tail on its own cache
//! |                             |  line, head on its own cache line
//! +-----------------------------+
//! | LcrqJobSlot[0]  (64B)       |  sequence (8B) + LineItem (16B)
//! | LcrqJobSlot[1]              |  + 40B trailing padding
//! | ...                         |
//! | LcrqJobSlot[capacity-1]     |
//! +-----------------------------+
//! ```
//!
//! Each slot is exactly one cache line so adjacent slots never share
//! coherence-traffic lines. The `LineItem` payload is the same
//! byte-oriented 16-byte struct
//! [`SharedDequeKhpd`](crate::SharedDequeKhpd) and
//! [`SharedDequeUrd`](crate::SharedDequeUrd) use, re-exported via
//! [`crate::LineItem`] so consumers can ferry the same byte pattern
//! across all three primitives without re-marshalling.
//!
//! ## When to use this vs `SharedDeque` / `SharedDequeKhpd`
//!
//! - **`SharedDeque<T>` (Chase-Lev)**: per-item dispatch and steal,
//!   strict LIFO at the owner; lowest constant when there is no
//!   batching.
//! - **`SharedDequeKhpd`**: producer packs `LINE_ITEMS = 3` items per
//!   publication line and publishes them with one Release-store on
//!   `state`. The win zone is "K items per call where K is a small
//!   multiple of 3."
//! - **`SharedDequeLoh` (this primitive)**: producer batches K items
//!   per call and pays one `tail.fetch_add(K)` plus K Release-stores.
//!   The win zone is "K items per call where the producer wants to
//!   amortize the producer-counter atomic over an arbitrary batch
//!   size."

#![allow(clippy::missing_errors_doc)]

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering, fence};

use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;

use crate::shared_deque_khpd::LineItem;

/// Prefetch the cache line at `slot` with write-intent (the M-state
/// hint). Emits `PREFETCHW` directly via inline asm on x86_64
/// because Rust's stable `_mm_prefetch` only exposes the T0/T1/T2/
/// NTA hints (S-state targets), which force a publisher write to
/// pay an RFO coherence upgrade. `PREFETCHW` brings the line to
/// M-state directly so the publisher's payload Release-store costs
/// one cycle instead of a cross-core RFO. The instruction is a NOP
/// on x86_64 CPUs without the `PRFCHW` feature flag (3DNow-era
/// AMD has it natively; Intel since Broadwell), so it is safe to
/// unconditionally emit on x86_64.
///
/// On non-x86_64 architectures this compiles to a no-op.
#[inline(always)]
fn prefetch_slot(slot: *const LcrqJobSlot) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `prefetchw` is a hardware hint and never faults on
        // unmapped memory; the CPU silently ignores invalid
        // addresses. `nostack` + `preserves_flags` lets the
        // optimizer schedule freely around the asm.
        unsafe {
            core::arch::asm!(
                "prefetchw [{ptr}]",
                ptr = in(reg) slot,
                options(nostack, preserves_flags),
            );
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        _ = slot;
    }
}

/// Magic byte sequence marking a valid LOH file. Reads as ASCII
/// "WLOH" + version. Distinct from the Chase-Lev / KHPD / URD magics
/// so a file-confusion is rejected at open time.
pub const LOH_MAGIC: u64 = 0x574C_4F48_0000_0001;

/// One slot is exactly one cache line.
pub const LOH_SLOT_SIZE: usize = 64;

/// Default LIFO soft cap. Push past this returns
/// [`PushError::LifoFull`]; caller must `flush()` or back off.
pub const DEFAULT_LIFO_CAP: usize = 256;

/// File header. Cache-line aligned. `head` and `tail` each get their
/// own cache line so the producer-side `tail.fetch_add` does not
/// invalidate the consumer-side `head` line.
#[repr(C, align(64))]
pub struct LohHeader {
    /// Magic constant.
    pub magic: u64,
    /// Number of ring slots; always a power of two.
    pub capacity: u64,
    /// Pid of the owner process; informational. Cleared on
    /// `close_owner()`.
    pub owner_pid: AtomicU64,
    /// Epoch counter advanced by the owner on shutdown.
    pub epoch: AtomicU64,
    /// Padding to push `tail` to its own cache line.
    pub _pad_meta: [u8; 24],
    /// Producer counter. Owner `fetch_add(batch_size)` during
    /// migration to claim a contiguous block of slots.
    pub tail: AtomicI64,
    /// Padding to push `head` to its own cache line.
    pub _pad_tail: [u8; 56],
    /// Consumer counter. Thieves CAS this to claim a slot.
    pub head: AtomicI64,
    /// Padding round to two whole cache lines after `head`.
    pub _pad_head: [u8; 56],
}

/// Ring slot: Vyukov sequence + byte-oriented [`LineItem`] payload.
/// Fixed shape, 64 bytes, process-portable.
#[repr(C, align(64))]
pub struct LcrqJobSlot {
    /// Vyukov-style sequence number gating payload access:
    /// - On creation: `seq == idx` (slot empty, ready to publish).
    /// - After producer Release-store: `seq == idx + 1` (published,
    ///   consumer may read).
    /// - After consumer Release-store: `seq == idx + capacity`
    ///   (consumed, ready for next round at `idx + capacity`).
    pub sequence: AtomicI64,
    /// Caller's byte-oriented payload.
    pub item: LineItem,
    /// Trailing padding rounding the slot to 64 bytes.
    pub _pad: [u8; 40],
}

/// Total file size for a ring with `capacity` slots, including the
/// header.
pub const fn loh_file_size(capacity: usize) -> usize {
    std::mem::size_of::<LohHeader>() + capacity * LOH_SLOT_SIZE
}

/// Outcome of [`SharedDequeLoh::push`] / [`SharedDequeLoh::flush`] /
/// [`SharedDequeLoh::publish_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// Ring at capacity; consumer hasn't caught up. Caller may spin,
    /// back off, or report upstream pressure.
    Full,
    /// Owner-side LIFO at its soft cap; caller must `flush()` or
    /// back off before pushing more.
    LifoFull,
}

/// Outcome of [`SharedDequeLoh::steal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Steal {
    /// Got a slot's payload.
    Success(StealResult),
    /// Ring empty (no published item past `head`).
    Empty,
    /// CAS lost to a competing thief, or the publisher's Release on
    /// the sequence number is missing from the slot snapshot; outer
    /// loop should retry.
    Retry,
}

/// Payload returned by a successful steal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StealResult {
    /// The slot's 16-byte byte-oriented payload.
    pub item: LineItem,
}

/// MMF-backed LOH deque. Single owner (the process that created the
/// file); arbitrarily many thieves across processes.
pub struct SharedDequeLoh {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    capacity_mask: i64,
    flush_threshold: usize,
    lifo_cap: usize,
    /// Owner-side LIFO. `parking_lot::Mutex` is uncontended on the
    /// hot path because, by protocol, only the originator thread
    /// pushes; the Mutex exists so [`SharedDequeLoh`] can be shared
    /// as `Arc<SharedDequeLoh>` between the originator and a
    /// flush-trigger thread without losing `Sync`. The
    /// [`Self::publish_batch`] hot path bypasses this Mutex entirely
    /// (it does not touch the LIFO), so the batched-publish
    /// throughput is set by `tail.fetch_add` cost only.
    local_lifo: Mutex<Vec<LineItem>>,
}

// SAFETY: All fields are Send. Mmap handle is Send + Sync per
// memmap2; every ring access goes through the LCRQ sequence-number
// protocol (per-slot Acquire / Release pair) so concurrent producers
// and consumers see a consistent view. The Mutex around the LIFO
// linearizes owner-side accesses across any thread the originator
// happens to schedule the push on.
unsafe impl Send for SharedDequeLoh {}
// SAFETY: Same justification as the `Send` impl directly above.
unsafe impl Sync for SharedDequeLoh {}

impl SharedDequeLoh {
    /// Create a fresh LOH file. `capacity` rounds up to the next
    /// power of two (min 2). `flush_threshold` is the LIFO length at
    /// which an automatic [`Self::flush`] fires on the next push.
    pub fn create<P: AsRef<Path>>(
        path: P,
        capacity: usize,
        flush_threshold: usize,
    ) -> io::Result<Self> {
        let capacity = capacity.max(2).next_power_of_two();
        let size = loh_file_size(capacity);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;
        file.set_len(size as u64)?;

        // SAFETY: `map_mut` is unsafe because the kernel cannot
        // prevent another process from truncating or mutating the
        // backing file in ways that violate Rust's aliasing rules.
        // This call site upholds the soundness contract by writing
        // only through the LCRQ per-slot sequence-number protocol;
        // the file size is fixed by `file.set_len` immediately above
        // and never shrunk for the lifetime of any mapping.
        let mut mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };

        let header_ptr = mmap.as_mut_ptr() as *mut LohHeader;
        // SAFETY: mmap is page-aligned (well above the 64-byte
        // alignment LohHeader requires); the map covers
        // `loh_file_size(capacity)` bytes by construction.
        unsafe {
            (*header_ptr).magic = LOH_MAGIC;
            (*header_ptr).capacity = capacity as u64;
            (*header_ptr).owner_pid = AtomicU64::new(std::process::id() as u64);
            (*header_ptr).epoch = AtomicU64::new(0);
            std::ptr::write_bytes((*header_ptr)._pad_meta.as_mut_ptr(), 0, 24);
            (*header_ptr).tail = AtomicI64::new(0);
            std::ptr::write_bytes((*header_ptr)._pad_tail.as_mut_ptr(), 0, 56);
            (*header_ptr).head = AtomicI64::new(0);
            std::ptr::write_bytes((*header_ptr)._pad_head.as_mut_ptr(), 0, 56);
        }

        // Initialise each slot's sequence to its index. On first
        // producer touch, `sequence == idx`, so the publisher knows
        // the slot is ready to publish (write payload, then
        // Release-store `idx + 1`).
        let slots_start = std::mem::size_of::<LohHeader>();
        for i in 0..capacity {
            let off = slots_start + i * LOH_SLOT_SIZE;
            // SAFETY: `off + LOH_SLOT_SIZE <= loh_file_size(capacity)`
            // by construction; the cast to `*mut LcrqJobSlot` is sound
            // because the slot is `repr(C, align(64))` and `off` is a
            // multiple of 64.
            let slot_ptr = unsafe { mmap.as_mut_ptr().add(off) as *mut LcrqJobSlot };
            // SAFETY: `slot_ptr` is in-bounds and aligned; payload
            // bytes are valid for any bit pattern.
            unsafe {
                (*slot_ptr).sequence = AtomicI64::new(i as i64);
                (*slot_ptr).item = LineItem::default();
                std::ptr::write_bytes((*slot_ptr)._pad.as_mut_ptr(), 0, 40);
            }
        }

        mmap.flush()?;

        let flush_threshold = flush_threshold.max(1);
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            capacity_mask: (capacity as i64) - 1,
            flush_threshold,
            lifo_cap: DEFAULT_LIFO_CAP,
            local_lifo: Mutex::new(Vec::with_capacity(DEFAULT_LIFO_CAP)),
        })
    }

    /// Open an existing LOH file. Validates magic and capacity.
    pub fn open<P: AsRef<Path>>(path: P, flush_threshold: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        let size = file.metadata()?.len() as usize;
        if size < std::mem::size_of::<LohHeader>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "loh file too small to contain header",
            ));
        }

        // SAFETY: Same justification as `create` - protocol-only
        // access through the per-slot sequence number.
        let mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };

        let header_ptr = mmap.as_ptr() as *const LohHeader;
        // SAFETY: map size verified to cover header; mmap alignment
        // exceeds header alignment.
        let (magic, capacity) =
            unsafe { ((*header_ptr).magic, (*header_ptr).capacity as usize) };
        if magic != LOH_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("loh magic mismatch: got {magic:#x}, want {LOH_MAGIC:#x}"),
            ));
        }
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("loh capacity {capacity} is not a power of two >= 2"),
            ));
        }
        if size < loh_file_size(capacity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "loh file size {size} below expected {}",
                    loh_file_size(capacity)
                ),
            ));
        }

        let flush_threshold = flush_threshold.max(1);
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            capacity_mask: (capacity as i64) - 1,
            flush_threshold,
            lifo_cap: DEFAULT_LIFO_CAP,
            local_lifo: Mutex::new(Vec::with_capacity(DEFAULT_LIFO_CAP)),
        })
    }

    /// Slot count of the ring (always a power of two).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Configured auto-flush threshold (LIFO length that triggers a
    /// flush on the next push).
    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold
    }

    /// Pid of the owner process at create time, or 0 if cleared.
    pub fn owner_pid(&self) -> u64 {
        self.header().owner_pid.load(Ordering::Acquire)
    }

    /// Owner shutdown: zero pid + advance epoch.
    pub fn close_owner(&self) {
        self.header().owner_pid.store(0, Ordering::Release);
        self.header().epoch.fetch_add(1, Ordering::Release);
    }

    fn header(&self) -> &LohHeader {
        // SAFETY: map covers the header; alignment is page-aligned.
        unsafe { &*(self.mmap.as_ptr() as *const LohHeader) }
    }

    fn slot_ptr(&self, idx: i64) -> *mut LcrqJobSlot {
        let slot_idx = (idx & self.capacity_mask) as usize;
        let off = std::mem::size_of::<LohHeader>() + slot_idx * LOH_SLOT_SIZE;
        // SAFETY: `slot_idx` is in [0, capacity); `off` is within the
        // mapped region and 64-byte aligned.
        unsafe { self.mmap.as_ptr().add(off) as *mut LcrqJobSlot }
    }

    /// Snapshot the current `(head, tail, ring_size, lifo_len)`.
    /// Loads are independent; the tuple is not a linearizable
    /// snapshot - useful for debug / introspection only.
    pub fn snapshot_size(&self) -> (i64, i64, i64, usize) {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        let tail = h.tail.load(Ordering::Acquire);
        let lifo_len = self.local_lifo.try_lock().map(|g| g.len()).unwrap_or(0);
        (head, tail, tail - head, lifo_len)
    }

    /// Owner-side push. Stages the item in the local LIFO; when the
    /// LIFO reaches `flush_threshold` an automatic [`Self::flush`]
    /// fires that drains the LIFO into the ring tail.
    ///
    /// **Only the owner process may call this.**
    pub fn push(&self, item: LineItem) -> Result<(), PushError> {
        let mut lifo = self.local_lifo.lock();
        if lifo.len() >= self.lifo_cap {
            return Err(PushError::LifoFull);
        }
        lifo.push(item);
        if lifo.len() >= self.flush_threshold {
            // Flush from inside the lock to keep the LIFO consistent
            // with the migration count. If the flush fails (ring at
            // capacity), undo the push so the caller can retry with
            // a clean LIFO state.
            if let Err(e) = self.flush_locked(&mut lifo) {
                lifo.pop();
                return Err(e);
            }
        }
        Ok(())
    }

    /// Owner-side explicit flush. Drains the local LIFO into the
    /// ring's tail in one batch (one `tail.fetch_add(N)` + N
    /// Release-stores). Returns the number of items migrated.
    pub fn flush(&self) -> Result<usize, PushError> {
        let mut lifo = self.local_lifo.lock();
        self.flush_locked(&mut lifo)
    }

    /// Owner-side single-call batch publish. **Holds zero locks.**
    /// The LIFO-bypass property is the SubEtha-native lever: the
    /// upstream LCRQ-on-LIFO design held a Mutex during batch
    /// publish to satisfy a separate dispatch-backend `&self`
    /// contract; SubEtha's owner-only protocol makes that Mutex
    /// gratuitous on the batch path. `tail.fetch_add(N)` atomically
    /// reserves a disjoint slot range; the per-slot sequence-number
    /// protocol gates the writes. A sibling `flush()` or `push()`
    /// touching the LIFO is independent: it competes only on
    /// `tail.fetch_add`, not on the LIFO Vec.
    ///
    /// Cost per call: one `tail.fetch_add(items.len())` plus
    /// `items.len()` per-slot Release-stores on the sequence number.
    ///
    /// Returns the number of items migrated.
    pub fn publish_batch(&self, items: &[LineItem]) -> Result<usize, PushError> {
        if items.is_empty() {
            return Ok(0);
        }
        let n = items.len();
        let h = self.header();
        let head_snapshot = h.head.load(Ordering::Acquire);
        let tail_snapshot = h.tail.load(Ordering::Relaxed);
        if (tail_snapshot - head_snapshot + n as i64) > self.capacity as i64 {
            return Err(PushError::Full);
        }
        let base = h.tail.fetch_add(n as i64, Ordering::AcqRel);

        // Prefetch the first slot before entering the publish loop so
        // the producer's sequence Acquire-load hits a warm line.
        prefetch_slot(self.slot_ptr(base));

        for (i, item) in items.iter().enumerate() {
            let idx = base + i as i64;
            // Warm the NEXT slot's cache line while we publish this
            // one. The `i + 1 < n` guard avoids prefetching past the
            // reserved range.
            if i + 1 < n {
                prefetch_slot(self.slot_ptr(idx + 1));
            }
            // SAFETY: slot_ptr returns an in-bounds aligned pointer.
            unsafe {
                self.publish_at(idx, *item);
            }
        }
        Ok(n)
    }

    fn flush_locked(&self, lifo: &mut Vec<LineItem>) -> Result<usize, PushError> {
        let n = lifo.len();
        if n == 0 {
            return Ok(0);
        }
        let h = self.header();
        let head_snapshot = h.head.load(Ordering::Acquire);
        let tail_snapshot = h.tail.load(Ordering::Relaxed);
        if (tail_snapshot - head_snapshot + n as i64) > self.capacity as i64 {
            // Ring would overflow; report Full so caller can back
            // off. Items remain in the LIFO for the next flush
            // attempt.
            return Err(PushError::Full);
        }
        let base = h.tail.fetch_add(n as i64, Ordering::AcqRel);

        // Drain LIFO in FIFO order (oldest first) so the ring sees
        // items in their original push order. `drain()` avoids the
        // O(N) shift cost of pop()-into-reverse.
        for (i, item) in lifo.drain(..).enumerate() {
            let idx = base + i as i64;
            // SAFETY: slot_ptr returns an in-bounds aligned pointer.
            unsafe {
                self.publish_at(idx, item);
            }
        }
        Ok(n)
    }

    /// Migrate one item into the slot at ring index `idx` under the
    /// Vyukov sequence-number protocol.
    ///
    /// # Safety
    ///
    /// Caller must have reserved the slot by holding the producer
    /// lock and having `idx` in `[base, base + N)` of a successful
    /// `tail.fetch_add(N)`.
    unsafe fn publish_at(&self, idx: i64, item: LineItem) {
        let slot = self.slot_ptr(idx);
        // Spin-wait until the slot is publishable (sequence == idx).
        // For the owner path this should usually already be true:
        // head <= tail always and slot.sequence advances past idx
        // only when a consumer has taken it.
        loop {
            // SAFETY: `slot` is the in-bounds aligned pointer returned
            // by `slot_ptr`; the LCRQ sequence-number protocol ensures
            // no other writer touches this slot between our reservation
            // (caller-held `tail.fetch_add`) and the Release-store at
            // the bottom of this function.
            let seq = unsafe { (*slot).sequence.load(Ordering::Acquire) };
            let diff = seq - idx;
            if diff == 0 {
                // Slot ready: consumer released the prior round (or
                // this is the first publish, where init set
                // sequence == idx).
                break;
            }
            if diff < 0 {
                // Prior round's consumer still owns the slot. Spin.
                std::hint::spin_loop();
                continue;
            }
            // diff > 0: the slot's sequence is for a future round.
            // With a single producer and the capacity-check guard
            // this is unreachable; loud panic so the cause can be
            // diagnosed instead of silently overwriting a slot.
            panic!(
                "LOH producer protocol violation: slot[{}] seq={} ahead of idx={}",
                idx & self.capacity_mask,
                seq,
                idx
            );
        }
        // SAFETY: same as the Acquire-load above; we own the slot for
        // this round per the caller's reservation in `tail.fetch_add`.
        unsafe {
            (*slot).item = item;
            (*slot).sequence.store(idx + 1, Ordering::Release);
        }
    }

    /// Owner-side pop from the local LIFO. Items still in the LIFO
    /// (unmigrated) may be retrieved locally without round-tripping
    /// through the ring.
    pub fn pop_local(&self) -> Option<LineItem> {
        let mut lifo = self.local_lifo.lock();
        lifo.pop()
    }

    /// Thief-side steal. Race-free CAS-on-head with sequence-number
    /// validation on the slot. Returns [`Steal::Retry`] when a
    /// competing thief beat us on the head CAS, or when the
    /// publisher's Release on the sequence number is missing from
    /// the slot snapshot; outer loop should retry.
    pub fn steal(&self) -> Steal {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        fence(Ordering::SeqCst);
        let tail = h.tail.load(Ordering::Acquire);
        if head >= tail {
            return Steal::Empty;
        }
        let slot = self.slot_ptr(head);
        // Check the sequence ahead of the CAS. The producer Release-
        // stores `head + 1` after writing the slot bytes; a value
        // less than that means the publisher's Release on the slot
        // is missing from our snapshot, and a value greater than
        // that means the ring has wrapped and the producer has
        // re-published this slot for a future round (the head we
        // loaded is stale).
        //
        // SAFETY: slot is in-bounds + aligned.
        let seq = unsafe { (*slot).sequence.load(Ordering::Acquire) };
        if seq != head + 1 {
            return Steal::Retry;
        }
        // Try to claim head. Once we win the CAS we own slot[head &
        // mask] for this round: the producer cannot re-publish the
        // slot until we release the sequence to `head + capacity`,
        // and the seq-check above already confirmed the publisher
        // released `head + 1`. The slot bytes we read below are the
        // bytes the producer wrote for this round.
        let won = h
            .head
            .compare_exchange(head, head + 1, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok();
        if !won {
            return Steal::Retry;
        }
        // SAFETY: same as above; head is now ours and the producer's
        // Release on slot.sequence happens-before our Acquire load
        // of slot.sequence above.
        let result = unsafe {
            StealResult {
                item: (*slot).item,
            }
        };
        // Release the slot for the next round at `head + capacity`.
        //
        // SAFETY: still our slot; the Release synchronises with the
        // next producer's Acquire-spin in `publish_at`.
        unsafe {
            (*slot)
                .sequence
                .store(head + self.capacity as i64, Ordering::Release);
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
        p.push(format!("subetha_loh_{pid}_{nonce}_{name}.bin"));
        p
    }

    fn u32_item(id: u32) -> LineItem {
        LineItem::new(&id.to_le_bytes()).expect("build item")
    }

    fn item_id(item: &LineItem) -> u32 {
        u32::from_le_bytes(item.payload[..4].try_into().unwrap())
    }

    #[test]
    fn create_then_open_round_trips_header() {
        let path = temp_path("create_open");
        let _d = SharedDequeLoh::create(&path, 8, 4).expect("create");
        let o = SharedDequeLoh::open(&path, 4).expect("open");
        assert_eq!(o.capacity(), 8);
        assert_eq!(o.owner_pid(), std::process::id() as u64);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = temp_path("bad_magic");
        std::fs::write(&path, vec![0xCDu8; 8192]).expect("seed");
        let r = SharedDequeLoh::open(&path, 4);
        assert!(r.is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn push_and_explicit_flush_migrates() {
        let path = temp_path("flush");
        // flush_threshold = usize::MAX so auto-flush never fires;
        // the explicit `flush()` is the only path to the ring.
        let d = SharedDequeLoh::create(&path, 8, usize::MAX).expect("create");
        for i in 0..3u32 {
            d.push(u32_item(i)).expect("push");
        }
        // Ring is still empty before flush.
        let (head, tail, sz, lifo_len) = d.snapshot_size();
        assert_eq!(head, 0);
        assert_eq!(tail, 0);
        assert_eq!(sz, 0);
        assert_eq!(lifo_len, 3);
        // Flush: 3 items migrate.
        let n = d.flush().expect("flush");
        assert_eq!(n, 3);
        let (_, tail, sz, lifo_len) = d.snapshot_size();
        assert_eq!(tail, 3);
        assert_eq!(sz, 3);
        assert_eq!(lifo_len, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn push_auto_flushes_at_threshold() {
        let path = temp_path("autoflush");
        let d = SharedDequeLoh::create(&path, 8, 4).expect("create");
        for i in 0..4u32 {
            d.push(u32_item(i)).expect("push");
        }
        // The 4th push triggers auto-flush.
        let (_, tail, sz, lifo_len) = d.snapshot_size();
        assert_eq!(tail, 4);
        assert_eq!(sz, 4);
        assert_eq!(lifo_len, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_migrates_in_fifo_order() {
        let path = temp_path("publish_batch");
        let d = SharedDequeLoh::create(&path, 64, usize::MAX).expect("create");
        let items: Vec<LineItem> = (1..=5u32).map(u32_item).collect();
        let n = d.publish_batch(&items).expect("publish_batch");
        assert_eq!(n, 5);
        let (_, tail, sz, lifo_len) = d.snapshot_size();
        assert_eq!(tail, 5);
        assert_eq!(sz, 5);
        // publish_batch bypasses the LIFO entirely.
        assert_eq!(lifo_len, 0);
        for expected in 1..=5u32 {
            loop {
                match d.steal() {
                    Steal::Success(r) => {
                        assert_eq!(item_id(&r.item), expected);
                        break;
                    }
                    Steal::Empty | Steal::Retry => std::thread::yield_now(),
                }
            }
        }
        assert!(matches!(d.steal(), Steal::Empty));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_empty_is_noop() {
        let path = temp_path("publish_batch_empty");
        let d = SharedDequeLoh::create(&path, 4, usize::MAX).expect("create");
        let n = d.publish_batch(&[]).expect("publish_batch empty");
        assert_eq!(n, 0);
        let (_, tail, sz, _) = d.snapshot_size();
        assert_eq!(tail, 0);
        assert_eq!(sz, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_full_returns_full() {
        let path = temp_path("publish_batch_full");
        let d = SharedDequeLoh::create(&path, 4, usize::MAX).expect("create");
        let items: Vec<LineItem> = (1..=4u32).map(u32_item).collect();
        d.publish_batch(&items).expect("publish first batch");
        // Ring at capacity; the follow-up publish_batch reports Full.
        let err = d
            .publish_batch(&[u32_item(99)])
            .expect_err("publish past capacity");
        assert_eq!(err, PushError::Full);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn steal_drains_in_fifo_order_after_flush() {
        let path = temp_path("fifo");
        let d = SharedDequeLoh::create(&path, 8, usize::MAX).expect("create");
        for i in 1..=3u32 {
            d.push(u32_item(i)).expect("push");
        }
        d.flush().expect("flush");
        for expected in 1..=3u32 {
            loop {
                match d.steal() {
                    Steal::Success(slot) => {
                        assert_eq!(item_id(&slot.item), expected);
                        break;
                    }
                    Steal::Empty | Steal::Retry => std::thread::yield_now(),
                }
            }
        }
        assert!(matches!(d.steal(), Steal::Empty));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn pop_local_drains_lifo_in_lifo_order() {
        let path = temp_path("pop_local_lifo");
        let d = SharedDequeLoh::create(&path, 4, usize::MAX).expect("create");
        for i in 1..=3u32 {
            d.push(u32_item(i)).expect("push");
        }
        // Owner pops in LIFO order (newest first).
        for expected in (1..=3u32).rev() {
            let e = d.pop_local().expect("pop_local");
            assert_eq!(item_id(&e), expected);
        }
        assert!(d.pop_local().is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn ring_full_at_capacity() {
        let path = temp_path("full");
        let d = SharedDequeLoh::create(&path, 2, usize::MAX).expect("create");
        d.push(u32_item(1)).expect("push");
        d.push(u32_item(2)).expect("push");
        let n = d.flush().expect("flush");
        assert_eq!(n, 2);
        // Ring is at capacity; pushing more + flushing reports Full.
        d.push(u32_item(3)).expect("push to lifo");
        let err = d.flush().expect_err("flush past capacity");
        assert_eq!(err, PushError::Full);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn close_owner_zeros_pid_and_advances_epoch() {
        let path = temp_path("close");
        let d = SharedDequeLoh::create(&path, 2, 1).expect("create");
        assert_eq!(d.owner_pid(), std::process::id() as u64);
        let h = d.header();
        let before = h.epoch.load(O::Acquire);
        d.close_owner();
        assert_eq!(d.owner_pid(), 0);
        assert_eq!(h.epoch.load(O::Acquire), before + 1);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn concurrent_thieves_no_double_take() {
        // Stress: owner pushes + auto-flushes; two thief threads
        // race to drain. Every slot must be consumed exactly once.
        let path = temp_path("stress");
        let d = Arc::new(SharedDequeLoh::create(&path, 128, 8).expect("create"));
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
                    match d.steal() {
                        Steal::Success(slot) => {
                            consumed.fetch_add(1, O::Relaxed);
                            sum.fetch_add(item_id(&slot.item) as usize, O::Relaxed);
                        }
                        Steal::Empty | Steal::Retry => std::thread::yield_now(),
                    }
                }
            }));
        }

        for i in 0..n {
            loop {
                match d.push(u32_item(i as u32)) {
                    Ok(()) => break,
                    Err(PushError::LifoFull) | Err(PushError::Full) => {
                        std::thread::yield_now();
                        // Opportunistic: a Full flush here just means
                        // the ring is congested; the outer loop keeps
                        // retrying the push.
                        d.flush().ok();
                    }
                }
            }
        }
        // The TERMINAL flush must succeed or the tail of the run
        // (up to flush_threshold - 1 items) stays stranded in the
        // process-local LIFO and the thieves spin on `consumed < n`
        // forever - flush() returning Full leaves items staged by
        // contract ("items remain in the LIFO for the next flush
        // attempt"). Retry until the thieves free ring space.
        loop {
            match d.flush() {
                Ok(_) => break,
                Err(PushError::Full) => std::thread::yield_now(),
                Err(e) => panic!("terminal flush: {e:?}"),
            }
        }
        for h in thieves {
            h.join().expect("thief");
        }
        let expected: usize = (0..n).sum();
        assert_eq!(
            sum.load(O::Relaxed),
            expected,
            "every slot consumed once"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_stress_two_thieves() {
        // Stress the canonical hot path: publish_batch fires N=64
        // items per call; two thieves race to drain.
        let path = temp_path("publish_batch_stress");
        let d = Arc::new(
            SharedDequeLoh::create(&path, 256, usize::MAX).expect("create"),
        );
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
                    match d.steal() {
                        Steal::Success(slot) => {
                            consumed.fetch_add(1, O::Relaxed);
                            sum.fetch_add(item_id(&slot.item) as usize, O::Relaxed);
                        }
                        Steal::Empty | Steal::Retry => std::thread::yield_now(),
                    }
                }
            }));
        }

        let mut pushed = 0usize;
        let burst = 64usize;
        while pushed < n {
            let want = burst.min(n - pushed);
            let batch: Vec<LineItem> = (0..want)
                .map(|j| u32_item((pushed + j) as u32))
                .collect();
            loop {
                match d.publish_batch(&batch) {
                    Ok(_) => break,
                    Err(PushError::Full) => std::thread::yield_now(),
                    Err(other) => panic!("publish_batch: {other:?}"),
                }
            }
            pushed += want;
        }

        for t in thieves {
            t.join().expect("thief");
        }
        let expected: usize = (0..n).sum();
        assert_eq!(
            sum.load(O::Relaxed),
            expected,
            "publish_batch stress: every item consumed once"
        );
        std::fs::remove_file(&path).ok();
    }
}
