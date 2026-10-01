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
//! bounce. Where LOH does not win: single-item request-reply,
//! because there's no batching to amortize against.
//!
//! ## Hot path API: [`publish_batch`](SharedDequeLoh::publish_batch)
//!
//! The canonical producer-fast API takes a slice of [`LineItem`] and
//! migrates the whole batch in one shot, paying one reservation on
//! `tail` plus `items.len()` Release-stores for the call. This is the
//! path that exercises the amortization lever and is the shape
//! benchmarks measure.
//!
//! Items staged one at a time go through a [`LohStager`], the owner's
//! LIFO: [`push`](LohStager::push) is a plain `Vec::push` that
//! auto-flushes at a configurable threshold, and
//! [`flush`](LohStager::flush) migrates the staged items in one batch.
//! The stager is its holder's own buffer, mutated through `&mut self`,
//! so staging takes no lock and no atomic. Every migration, staged or
//! batched, reserves its slots with one compare-and-swap on `tail`, so
//! publishers racing for the last free slots see `Full` rather than
//! overfill.
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
//! across all three primitives without re-marshaling.
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

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering, fence};

use memmap2::{MmapMut, MmapOptions};

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

/// Outcome of [`LohStager::push`] / [`LohStager::flush`] /
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
    /// LIFO length at which a [`LohStager::push`] flushes.
    flush_threshold: usize,
    /// Most items a [`LohStager`] holds before `push` refuses.
    lifo_cap: usize,
}

// SAFETY: All fields are Send. Mmap handle is Send + Sync per
// memmap2; every ring access goes through the LCRQ sequence-number
// protocol (per-slot Acquire / Release pair) so concurrent producers
// and consumers see a consistent view, and producers reserve disjoint
// slots through the compare-and-swap on `tail`.
unsafe impl Send for SharedDequeLoh {}
// SAFETY: Same justification as the `Send` impl directly above.
unsafe impl Sync for SharedDequeLoh {}

impl SharedDequeLoh {
    /// Create a fresh LOH file. `capacity` rounds up to the next
    /// power of two (min 2). `flush_threshold` is the LIFO length at
    /// which an automatic [`LohStager::flush`] fires on the next push.
    pub fn create<P: AsRef<Path>>(
        path: P,
        capacity: usize,
        flush_threshold: usize,
    ) -> io::Result<Self> {
        let capacity = capacity.max(2).next_power_of_two();
        let size = loh_file_size(capacity);

        let file = crate::region_file::create_truncated(path.as_ref())?;
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

        // Initialize each slot's sequence to its index. On first
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
        })
    }

    /// Open an existing LOH file. Validates magic and capacity.
    pub fn open<P: AsRef<Path>>(path: P, flush_threshold: usize) -> io::Result<Self> {
        let file = crate::region_file::open_existing(path.as_ref())?;
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

    /// Snapshot the current `(head, tail, ring_size)`. Loads are
    /// independent; the tuple is not a linearizable snapshot - useful
    /// for debug / introspection only. Staged items live in their
    /// [`LohStager`] and are not counted here.
    pub fn snapshot_size(&self) -> (i64, i64, i64) {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        let tail = h.tail.load(Ordering::Acquire);
        (head, tail, tail - head)
    }

    /// The owner-side LIFO: items [`push`](LohStager::push)ed into it
    /// reach the ring on [`flush`](LohStager::flush), or on the push
    /// that brings it to [`flush_threshold`](Self::flush_threshold).
    /// **Only the owner process may stage.**
    pub fn stager(&self) -> LohStager<'_> {
        LohStager { deque: self, lifo: Vec::with_capacity(self.lifo_cap) }
    }

    /// Reserve `n` slots at the tail and return the first one's index,
    /// or `Full` when they would pass the capacity left unclaimed. The
    /// reservation is a compare-and-swap on `tail`, so of two producers
    /// racing for the last free slots only one has them, and the other
    /// checks again against the winner's tail.
    fn reserve_slots(&self, n: usize) -> Result<i64, PushError> {
        let h = self.header();
        let n = n as i64;
        let mut tail = h.tail.load(Ordering::Relaxed);
        loop {
            let head = h.head.load(Ordering::Acquire);
            if tail - head + n > self.capacity as i64 {
                return Err(PushError::Full);
            }
            #[cfg(test)]
            crate::test_races::pause_point();
            match h.tail.compare_exchange_weak(tail, tail + n, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return Ok(tail),
                Err(now) => tail = now,
            }
        }
    }

    /// Owner-side single-call batch publish. It bypasses the LIFO:
    /// one compare-and-swap on `tail` reserves a disjoint slot range,
    /// and the per-slot sequence-number protocol gates the writes. A
    /// [`LohStager`] flushing at the same time competes only on that
    /// reservation.
    ///
    /// Cost per call: one reservation on `tail` plus `items.len()`
    /// per-slot Release-stores on the sequence number.
    ///
    /// Returns the number of items migrated.
    pub fn publish_batch(&self, items: &[LineItem]) -> Result<usize, PushError> {
        if items.is_empty() {
            return Ok(0);
        }
        let n = items.len();
        let base = self.reserve_slots(n)?;

        // Prefetch the first slot before entering the publish loop so
        // the producer's sequence Acquire-load hits a warm line.
        prefetch_slot(self.slot_ptr(base));

        for (i, item) in items.iter().enumerate() {
            let idx = base + i as i64;
            // Warm the next slot's cache line while we publish this
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

    /// Migrate one item into the slot at ring index `idx` under the
    /// Vyukov sequence-number protocol.
    ///
    /// # Safety
    ///
    /// Caller must have reserved the slot: `idx` lies in
    /// `[base, base + N)` of a successful `reserve_slots(N)` that
    /// returned `base`.
    unsafe fn publish_at(&self, idx: i64, item: LineItem) {
        let slot = self.slot_ptr(idx);
        // Spin-wait until the slot is publishable (sequence == idx).
        // For the owner path this should usually already be true:
        // head <= tail always and slot.sequence advances past idx
        // only when a consumer has taken it.
        loop {
            // SAFETY: `slot` is the in-bounds aligned pointer returned
            // by `slot_ptr`; the LCRQ sequence-number protocol ensures
            // no other writer touches this slot between the caller's
            // reservation and the Release-store at the bottom of this
            // function.
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
            // Reservations never pass the capacity, so this is
            // unreachable; loud panic so the cause can be diagnosed
            // instead of silently overwriting a slot.
            panic!(
                "LOH producer protocol violation: slot[{}] seq={} ahead of idx={}",
                idx & self.capacity_mask,
                seq,
                idx
            );
        }
        // SAFETY: same as the Acquire-load above; we own the slot for
        // this round per the caller's reservation.
        unsafe {
            (*slot).item = item;
            (*slot).sequence.store(idx + 1, Ordering::Release);
        }
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
        // SAFETY: still our slot; the Release synchronizes with the
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

/// The owner's LIFO for a [`SharedDequeLoh`], from
/// [`SharedDequeLoh::stager`]. Its holder mutates it through
/// `&mut self`, so a push is a plain `Vec::push`.
pub struct LohStager<'a> {
    deque: &'a SharedDequeLoh,
    lifo: Vec<LineItem>,
}

impl LohStager<'_> {
    /// Stage the item in the LIFO. When the LIFO reaches the deque's
    /// [`flush_threshold`](SharedDequeLoh::flush_threshold) an automatic
    /// [`flush`](Self::flush) drains it into the ring tail; if that
    /// flush finds the ring full, the push is undone and `Full`
    /// returned, so the caller retries with the LIFO as it was.
    pub fn push(&mut self, item: LineItem) -> Result<(), PushError> {
        if self.lifo.len() >= self.deque.lifo_cap {
            return Err(PushError::LifoFull);
        }
        self.lifo.push(item);
        if self.lifo.len() >= self.deque.flush_threshold
            && let Err(e) = self.flush()
        {
            self.lifo.pop();
            return Err(e);
        }
        Ok(())
    }

    /// Drain the LIFO into the ring's tail in one batch, oldest first,
    /// through [`SharedDequeLoh::publish_batch`]. Returns the number of
    /// items migrated; on `Full` the items stay staged.
    pub fn flush(&mut self) -> Result<usize, PushError> {
        let n = self.deque.publish_batch(&self.lifo)?;
        self.lifo.clear();
        Ok(n)
    }

    /// Pop the newest staged item. Items not yet migrated may be
    /// retrieved locally without round-tripping through the ring.
    pub fn pop_local(&mut self) -> Option<LineItem> {
        self.lifo.pop()
    }

    /// Items staged and not yet migrated.
    pub fn len(&self) -> usize {
        self.lifo.len()
    }

    /// Whether nothing is staged.
    pub fn is_empty(&self) -> bool {
        self.lifo.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as O};
    use std::thread;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn temp_path(name: &str) -> crate::test_paths::TmpFile {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the wall clock is after the epoch")
            .as_nanos();
        crate::test_paths::TmpFile::new(format!("subetha_loh_{}_{nonce}_{name}.bin", std::process::id()))
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
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = temp_path("bad_magic");
        std::fs::write(&path, vec![0xCDu8; 8192]).expect("seed");
        let r = SharedDequeLoh::open(&path, 4);
        assert!(r.is_err());
    }

    #[test]
    fn push_and_explicit_flush_migrates() {
        let path = temp_path("flush");
        // flush_threshold = usize::MAX so auto-flush never fires;
        // the explicit `flush()` is the only path to the ring.
        let d = SharedDequeLoh::create(&path, 8, usize::MAX).expect("create");
        let mut s = d.stager();
        for i in 0..3u32 {
            s.push(u32_item(i)).expect("push");
        }
        // Ring is still empty before flush.
        let (head, tail, sz) = d.snapshot_size();
        assert_eq!(head, 0);
        assert_eq!(tail, 0);
        assert_eq!(sz, 0);
        assert_eq!(s.len(), 3);
        // Flush: 3 items migrate.
        let n = s.flush().expect("flush");
        assert_eq!(n, 3);
        let (_, tail, sz) = d.snapshot_size();
        assert_eq!(tail, 3);
        assert_eq!(sz, 3);
        assert!(s.is_empty());
    }

    #[test]
    fn push_auto_flushes_at_threshold() {
        let path = temp_path("autoflush");
        let d = SharedDequeLoh::create(&path, 8, 4).expect("create");
        let mut s = d.stager();
        for i in 0..4u32 {
            s.push(u32_item(i)).expect("push");
        }
        // The 4th push triggers auto-flush.
        let (_, tail, sz) = d.snapshot_size();
        assert_eq!(tail, 4);
        assert_eq!(sz, 4);
        assert!(s.is_empty());
    }

    #[test]
    fn publish_batch_migrates_in_fifo_order() {
        let path = temp_path("publish_batch");
        let d = SharedDequeLoh::create(&path, 64, usize::MAX).expect("create");
        let items: Vec<LineItem> = (1..=5u32).map(u32_item).collect();
        let n = d.publish_batch(&items).expect("publish_batch");
        assert_eq!(n, 5);
        let (_, tail, sz) = d.snapshot_size();
        assert_eq!(tail, 5);
        assert_eq!(sz, 5);
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
    }

    #[test]
    fn publish_batch_empty_is_noop() {
        let path = temp_path("publish_batch_empty");
        let d = SharedDequeLoh::create(&path, 4, usize::MAX).expect("create");
        let n = d.publish_batch(&[]).expect("publish_batch empty");
        assert_eq!(n, 0);
        let (_, tail, sz) = d.snapshot_size();
        assert_eq!(tail, 0);
        assert_eq!(sz, 0);
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
    }

    #[test]
    fn publishers_racing_for_the_last_slot_do_not_both_take_it() {
        let path = temp_path("race_last_slot");
        let d = Arc::new(SharedDequeLoh::create(&path, 2, usize::MAX).expect("create"));
        d.publish_batch(&[u32_item(1)]).expect("the first slot");

        // One slot is left. A batch stops after its capacity check and
        // before it reserves, and another batch takes the last slot in
        // that window.
        let stopped = Arc::clone(&d);
        let (pause, first) =
            crate::test_races::stopped(move || stopped.publish_batch(&[u32_item(2)]));
        let second = d.publish_batch(&[u32_item(3)]).expect("the second batch takes the last slot");
        assert_eq!(second, 1);
        pause.release();
        assert!(
            crate::test_races::within_lost(|| first.is_finished()),
            "the stopped batch returns"
        );
        let refused = first
            .join()
            .expect("the stopped batch")
            .expect_err("the stopped batch finds the deque full");
        assert_eq!(refused, PushError::Full);
        let (head, tail, _) = d.snapshot_size();
        assert_eq!(tail - head, 2, "no more slots are claimed than the deque holds");
    }

    #[test]
    fn steal_drains_in_fifo_order_after_flush() {
        let path = temp_path("fifo");
        let d = SharedDequeLoh::create(&path, 8, usize::MAX).expect("create");
        let mut s = d.stager();
        for i in 1..=3u32 {
            s.push(u32_item(i)).expect("push");
        }
        s.flush().expect("flush");
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
    }

    #[test]
    fn pop_local_drains_lifo_in_lifo_order() {
        let path = temp_path("pop_local_lifo");
        let d = SharedDequeLoh::create(&path, 4, usize::MAX).expect("create");
        let mut s = d.stager();
        for i in 1..=3u32 {
            s.push(u32_item(i)).expect("push");
        }
        // Owner pops in LIFO order (newest first).
        for expected in (1..=3u32).rev() {
            let e = s.pop_local().expect("pop_local");
            assert_eq!(item_id(&e), expected);
        }
        assert!(s.pop_local().is_none());
    }

    #[test]
    fn ring_full_at_capacity() {
        let path = temp_path("full");
        let d = SharedDequeLoh::create(&path, 2, usize::MAX).expect("create");
        let mut s = d.stager();
        s.push(u32_item(1)).expect("push");
        s.push(u32_item(2)).expect("push");
        let n = s.flush().expect("flush");
        assert_eq!(n, 2);
        // Ring is at capacity; pushing more + flushing reports Full.
        s.push(u32_item(3)).expect("push to lifo");
        let err = s.flush().expect_err("flush past capacity");
        assert_eq!(err, PushError::Full);
        assert_eq!(s.len(), 1, "a flush refused as full leaves its items staged");
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

        let mut s = d.stager();
        for i in 0..n {
            loop {
                match s.push(u32_item(i as u32)) {
                    Ok(()) => break,
                    Err(PushError::LifoFull) | Err(PushError::Full) => {
                        std::thread::yield_now();
                        // Opportunistic: a Full flush here just means
                        // the ring is congested; the outer loop keeps
                        // retrying the push. Anything else is a defect.
                        match s.flush() {
                            Ok(_) | Err(PushError::Full) => {}
                            Err(e) => panic!("opportunistic flush: {e:?}"),
                        }
                    }
                }
            }
        }
        // The terminal flush must succeed or the tail of the run
        // (up to flush_threshold - 1 items) stays stranded in the
        // stager and the thieves spin on `consumed < n` forever: a
        // flush that returns Full leaves its items staged. Retry until
        // the thieves free ring space.
        loop {
            match s.flush() {
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
    }
}
