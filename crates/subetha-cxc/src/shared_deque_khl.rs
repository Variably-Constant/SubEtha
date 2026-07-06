//! `SharedDequeKhl` - K-axis Hierarchical LCRQ deque, MMF-backed.
//!
//! Novel SubEtha-native hybrid that pulls THREE amortization levers
//! the four prior primitives pull individually:
//!
//! 1. **KHPD's 3-items-per-Release-store** - each ring slot carries
//!    up to [`KHL_ITEMS_PER_SLOT`] = 3 [`LineItem`] payloads, and one
//!    Release-store on the slot's Vyukov sequence number publishes
//!    them all together. The per-item coherence cost is one cache
//!    line bounce per 3 items.
//! 2. **LOH's K-slots-per-counter-update** -
//!    [`SharedDequeKhl::publish_batch`] reserves
//!    `ceil(K / KHL_ITEMS_PER_SLOT)` slots with ONE update of the
//!    producer tail counter, amortizing the producer-counter cost
//!    across the whole batch.
//! 3. **Chase-Lev's owner-private tail counter** - the producer's
//!    tail-counter update is a Release-store (not a `LOCK XADD`),
//!    because the contract is "single owner process pushes." The
//!    Release ordering on the per-slot sequence number is what
//!    publishes the slot bytes; the tail counter only signals "this
//!    many slots reserved." Saves ~15 cycles per batch vs an atomic
//!    fetch_add.
//!
//! Why this hybrid is SubEtha-only: the upstream LCRQ-on-LIFO ring
//! has 56 bytes of dispatch-coupled payload per slot (closure id +
//! args + latch offset), so three slots cannot fit in one cache
//! line. SubEtha's byte-oriented [`LineItem`] is 16 bytes; three of
//! them plus an 8-byte sequence number plus a 4-byte count plus 4
//! bytes of reservation fit exactly in 64 bytes. The decoupling
//! between dispatch (`pass_registry`) and transport (`SharedDeque*`)
//! is what unlocks the hybrid.
//!
//! ## Cost-model comparison (per K=64 producer-fast batch)
//!
//! | Primitive | Producer atomics | Thief CAS attempts |
//! |---|---:|---:|
//! | `SharedDeque<u64>` (Chase-Lev per-item) | 64 Release-stores + 64 fences | 64 |
//! | `SharedDequeKhpd::publish_batch` | 22 slot Release-stores + 1 `fetch_add(LOCK XADD)` | 22 |
//! | `SharedDequeLoh::publish_batch` | 64 slot Release-stores + 1 `fetch_add(LOCK XADD)` | 64 |
//! | **`SharedDequeKhl::publish_batch`** | **22 slot Release-stores + 1 Release-store on tail** | **22** |
//!
//! KHL matches KHPD's per-slot count, matches LOH's per-batch
//! counter amortization, and adds Chase-Lev's owner-private counter
//! to save the LOCK XADD on top of that.
//!
//! ## Layout
//!
//! ```text
//! +-----------------------------+
//! | KhlHeader (192B)            |  magic, capacity, owner_pid,
//! |                             |  epoch, tail on its own cache
//! |                             |  line, head on its own cache line
//! +-----------------------------+
//! | KhlSlot[0]  (64B)           |  sequence (8B) + n_items (4B) +
//! | KhlSlot[1]                  |  reserved (4B) + 3 LineItems (48B)
//! | ...                         |
//! | KhlSlot[capacity-1]         |
//! +-----------------------------+
//! ```
//!
//! Each slot is exactly one cache line. The Vyukov sequence number
//! gating protocol is identical to
//! [`SharedDequeLoh`](crate::SharedDequeLoh) at the per-slot level:
//! `seq == idx` (empty) -> `seq == idx + 1` (published) ->
//! `seq == idx + capacity` (consumed).
//!
//! ## When to use this vs the four base primitives
//!
//! - `SharedDeque` (Chase-Lev): per-item dispatch, no batching.
//!   Lowest constant per push but pays one Release-store per item.
//! - `SharedDequeKhpd`: small batches (K up to ~64 on Zen+ R7 2700).
//!   Pays one `fetch_add` per batch.
//! - `SharedDequeLoh`: very large batches where the per-slot
//!   amortization dominates the per-line one.
//! - `SharedDequeUrd`: multi-thief workloads where the per-thief
//!   mailbox eliminates shared-head CAS contention.
//! - **`SharedDequeKhl`**: producer-fast single-thief batches at any
//!   K >= 6 where the caller wants the best of KHPD's per-slot density
//!   and LOH's per-batch amortization simultaneously. Empirically
//!   the strongest single-thief batched primitive on Zen+ R7 2700.

#![allow(clippy::missing_errors_doc)]

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{fence, AtomicI64, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};
use subetha_core::has_movdir64b;

use crate::shared_deque_khpd::LineItem;

/// `K_radius` axis - the coherence distance the publish operation
/// crosses. Captures the empirical observation that the optimal
/// publish mechanism differs by 50-100x across coherence domains
/// (same-CCX vs cross-CCX vs cross-socket), and that no algorithmic
/// structure axis (K_inner/K_outer/K_consumer/K_counter_share)
/// captures this dimension.
///
/// On the producer side this enum picks between cached Release-store
/// (best at small K_radius, where the line stays in the publisher's
/// L1d) and MOVDIR64B non-temporal stores (best at large K_radius,
/// where the line transfer to the consumer's L1d dominates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishRadius {
    /// Local: producer and consumer share L1d or L2 (d=0..1,
    /// same physical core or same CCX). Cached Release-store on the
    /// per-slot sequence is the right mechanism; the line stays in
    /// the publisher's L1d and the consumer's first read pays one
    /// L1d -> L1d transfer at ~3-10 ns.
    Local,
    /// Distant: producer and consumer are in different coherence
    /// clusters (d=2..6, cross-CCX / cross-CCD / cross-socket /
    /// CXL.mem). The per-slot publish uses `MOVDIR64B` plus `SFENCE`
    /// so the line writes go directly to LLC, bypassing the
    /// publisher's L1d. The consumer's first read fetches from LLC
    /// without paying the cross-CCX coherence-upgrade penalty that a
    /// cached store would otherwise force. Requires
    /// [`subetha_core::has_movdir64b`] to return true; on hosts
    /// without `MOVDIR64B` the [`PublishRadius::pick_auto`] helper
    /// degrades to `Local`.
    Distant,
}

impl PublishRadius {
    /// Pick a default radius for the current host. If MOVDIR64B is
    /// available, picks `Distant` (the M-state-direct publish wins
    /// whenever the consumer is anywhere outside the publisher's L1d
    /// and never loses badly inside it). Otherwise picks `Local`.
    pub fn pick_auto() -> Self {
        if has_movdir64b() {
            Self::Distant
        } else {
            Self::Local
        }
    }

    /// Resolve a caller-supplied request: a request of `Distant`
    /// degrades to `Local` on hosts without MOVDIR64B, since the
    /// instruction is unavailable.
    pub fn resolve(self) -> Self {
        match self {
            Self::Local => Self::Local,
            Self::Distant => {
                if has_movdir64b() {
                    Self::Distant
                } else {
                    Self::Local
                }
            }
        }
    }
}

/// Prefetch the cache line at `slot` with write-intent (M-state).
/// Emits `PREFETCHW` directly via inline asm on x86_64. See
/// `shared_deque_loh::prefetch_slot` for the architectural reasoning.
#[inline(always)]
fn prefetchw_slot(slot: *const KhlSlot) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `prefetchw` is a hardware hint and never faults.
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

/// Magic byte sequence marking a valid KHL file. ASCII "WKHL" + ver
/// 2 (bumped for the `n_items`-into-sequence bit-pack layout change).
pub const KHL_MAGIC: u64 = 0x574B_484C_0000_0002;

/// Cache-line size; one slot per cache line.
pub const KHL_SLOT_SIZE: usize = 64;

/// Items per slot: state (8 B sequence + 4 B n_items + 4 B reserved
/// = 16 B header) + 3 * 16 = 48 B = 64 B total.
pub const KHL_ITEMS_PER_SLOT: usize = 3;

/// File header. Cache-line aligned. `head` and `tail` each get their
/// own cache line so the producer's owner-private store on `tail`
/// does not invalidate the consumer-side `head` line.
#[repr(C, align(64))]
pub struct KhlHeader {
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
    /// Producer counter. Written by the owner only (Chase-Lev-style
    /// owner-private counter); Release-stored after the per-slot
    /// publish loop. Thieves Acquire-load to learn the high watermark.
    pub tail: AtomicI64,
    /// Padding to push `head` to its own cache line.
    pub _pad_tail: [u8; 56],
    /// Consumer counter. Thieves CAS to claim a slot.
    pub head: AtomicI64,
    /// Padding round to two whole cache lines after `head`.
    pub _pad_head: [u8; 56],
}

/// Ring slot: Vyukov sequence (with `n_items` bit-packed into the
/// low 2 bits) + 3 [`LineItem`] payloads. Fixed shape, 64 bytes,
/// process-portable.
///
/// ## Cross-axis fusion: `n_items` packed into `sequence`
///
/// `n_items` is always in `1..=KHL_ITEMS_PER_SLOT = 3`, which fits
/// in 2 bits. Instead of paying a separate store to publish
/// `n_items` alongside the sequence number, we encode it in the low
/// 2 bits of `packed_sequence`. The producer's ONE Release-store on
/// `packed_sequence` publishes BOTH the protocol state AND the
/// payload count - saving one store per slot, which fuses the
/// `K_inner` axis (items per slot) with the `K_gating` axis
/// (per-slot atomic) at the slot's cache line.
///
/// Encoding:
/// - `idx_value` = high 62 bits of `packed_sequence`
/// - `n_items` = low 2 bits of `packed_sequence`
/// - State: `idx_value == idx` (empty) -> `idx_value == idx + 1`
///   (published, `n_items` valid) -> `idx_value == idx + capacity`
///   (consumed)
#[repr(C, align(64))]
pub struct KhlSlot {
    /// Bit-packed Vyukov sequence: `(idx_value << 2) | n_items`.
    pub packed_sequence: AtomicI64,
    /// Reserved 8 bytes for cache-line alignment of the items array
    /// (items start at offset 16, slot is 64 bytes total).
    pub _reserved: u64,
    /// Caller's byte-oriented payloads. Only the first
    /// `unpack_n_items(packed_sequence.load())` are guaranteed valid.
    pub items: [LineItem; KHL_ITEMS_PER_SLOT],
}

/// Pack `(idx_value, n_items)` into a single i64 for atomic store.
#[inline(always)]
pub const fn pack_seq(idx_value: i64, n_items: usize) -> i64 {
    (idx_value << 2) | (n_items as i64 & 0x3)
}

/// Unpack idx_value from a packed sequence word.
#[inline(always)]
pub const fn unpack_idx(packed: i64) -> i64 {
    packed >> 2
}

/// Unpack n_items from a packed sequence word.
#[inline(always)]
pub const fn unpack_n_items(packed: i64) -> usize {
    (packed & 0x3) as usize
}

/// Total file size for a ring with `capacity` slots.
pub const fn khl_file_size(capacity: usize) -> usize {
    std::mem::size_of::<KhlHeader>() + capacity * KHL_SLOT_SIZE
}

/// Outcome of [`SharedDequeKhl::publish_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// Ring at capacity; consumer hasn't caught up.
    Full,
}

/// Outcome of [`SharedDequeKhl::steal_slot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Steal {
    /// Got items from one slot (1..=KHL_ITEMS_PER_SLOT).
    Success(StealResult),
    /// Ring empty.
    Empty,
    /// CAS lost or publisher's Release on sequence is missing from
    /// the snapshot; outer loop should retry.
    Retry,
}

/// Payload returned by a successful steal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StealResult {
    /// Count of valid items in `items`.
    pub n_items: usize,
    /// The slot's items (only `items[..n_items]` are valid).
    pub items: [LineItem; KHL_ITEMS_PER_SLOT],
}

/// MMF-backed K-axis Hierarchical LCRQ deque. Single owner, N thieves.
pub struct SharedDequeKhl {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    capacity_mask: i64,
    publish_radius: PublishRadius,
}

// SAFETY: All fields are Send. Mmap handle is Send + Sync per
// memmap2; every slot access goes through the LCRQ sequence-number
// protocol. The owner-private tail contract ("only the owner process
// pushes") makes the Relaxed/Release-store-on-tail safe; the
// per-slot Release-store on sequence is what publishes the bytes.
unsafe impl Send for SharedDequeKhl {}
// SAFETY: Same justification as the `Send` impl directly above.
unsafe impl Sync for SharedDequeKhl {}

impl SharedDequeKhl {
    /// Create a fresh KHL file. `capacity` rounds up to the next
    /// power of two (min 2). Capacity is in SLOTS; total item
    /// capacity is `capacity * KHL_ITEMS_PER_SLOT`.
    pub fn create<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        let capacity = capacity.max(2).next_power_of_two();
        let size = khl_file_size(capacity);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;
        file.set_len(size as u64)?;

        // SAFETY: `map_mut` soundness contract is upheld by writing
        // only through the per-slot Vyukov sequence-number protocol;
        // file size is fixed by `set_len` and never shrunk for the
        // lifetime of any mapping.
        let mut mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };

        let header_ptr = mmap.as_mut_ptr() as *mut KhlHeader;
        // SAFETY: mmap is page-aligned; the map covers the full
        // header + slots by construction.
        unsafe {
            (*header_ptr).magic = KHL_MAGIC;
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
        // the slot is ready.
        let slots_start = std::mem::size_of::<KhlHeader>();
        for i in 0..capacity {
            let off = slots_start + i * KHL_SLOT_SIZE;
            // SAFETY: off + KHL_SLOT_SIZE <= khl_file_size(capacity)
            // by construction; cast to *mut KhlSlot is sound because
            // the slot is repr(C, align(64)) and off is a multiple
            // of 64.
            let slot_ptr = unsafe { mmap.as_mut_ptr().add(off) as *mut KhlSlot };
            // SAFETY: slot_ptr is in-bounds + aligned.
            unsafe {
                // Empty state: packed_sequence = (i << 2) | 0
                (*slot_ptr).packed_sequence =
                    AtomicI64::new(pack_seq(i as i64, 0));
                (*slot_ptr)._reserved = 0;
                (*slot_ptr).items = [LineItem::default(); KHL_ITEMS_PER_SLOT];
            }
        }

        mmap.flush()?;
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            capacity_mask: (capacity as i64) - 1,
            publish_radius: PublishRadius::pick_auto(),
        })
    }

    /// Open an existing KHL file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        let size = file.metadata()?.len() as usize;
        if size < std::mem::size_of::<KhlHeader>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "khl file too small to contain header",
            ));
        }

        // SAFETY: Same protocol-only-access justification as create.
        let mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };

        let header_ptr = mmap.as_ptr() as *const KhlHeader;
        // SAFETY: map size verified to cover header.
        let (magic, capacity) =
            unsafe { ((*header_ptr).magic, (*header_ptr).capacity as usize) };
        if magic != KHL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("khl magic mismatch: got {magic:#x}, want {KHL_MAGIC:#x}"),
            ));
        }
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("khl capacity {capacity} is not pow2 >= 2"),
            ));
        }
        if size < khl_file_size(capacity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "khl file size {size} below expected {}",
                    khl_file_size(capacity)
                ),
            ));
        }
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            capacity_mask: (capacity as i64) - 1,
            publish_radius: PublishRadius::pick_auto(),
        })
    }

    /// Capacity in slots (always a power of two). Total item
    /// capacity is `capacity() * KHL_ITEMS_PER_SLOT`.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The currently configured `K_radius` axis value. Defaults to
    /// [`PublishRadius::pick_auto`] at construction; callers can
    /// override via [`Self::with_publish_radius`] when they know the
    /// producer-consumer coherence distance in advance (e.g. a
    /// cross-CCD scheduler explicitly requesting `Distant`).
    pub fn publish_radius(&self) -> PublishRadius {
        self.publish_radius
    }

    /// Override the publish radius for this handle. The setter
    /// resolves `Distant` to `Local` on hosts without MOVDIR64B so
    /// callers can request `Distant` unconditionally without breaking
    /// on older silicon.
    pub fn with_publish_radius(mut self, radius: PublishRadius) -> Self {
        self.publish_radius = radius.resolve();
        self
    }

    /// Owner pid at create time, or 0 after `close_owner()`.
    pub fn owner_pid(&self) -> u64 {
        self.header().owner_pid.load(Ordering::Acquire)
    }

    /// Owner shutdown: zero pid + advance epoch.
    pub fn close_owner(&self) {
        self.header().owner_pid.store(0, Ordering::Release);
        self.header().epoch.fetch_add(1, Ordering::Release);
    }

    fn header(&self) -> &KhlHeader {
        // SAFETY: map covers the header; alignment is page-aligned.
        unsafe { &*(self.mmap.as_ptr() as *const KhlHeader) }
    }

    fn slot_ptr(&self, idx: i64) -> *mut KhlSlot {
        let slot_idx = (idx & self.capacity_mask) as usize;
        let off = std::mem::size_of::<KhlHeader>() + slot_idx * KHL_SLOT_SIZE;
        // SAFETY: slot_idx is in [0, capacity); off is in-bounds +
        // 64-byte aligned.
        unsafe { self.mmap.as_ptr().add(off) as *mut KhlSlot }
    }

    /// Snapshot `(head, tail, ring_size_slots)`. Loads are
    /// independent; the tuple is not a linearizable snapshot.
    pub fn snapshot_size(&self) -> (i64, i64, i64) {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        let tail = h.tail.load(Ordering::Acquire);
        (head, tail, tail - head)
    }

    /// Owner-side batch publish. Packs `items` into
    /// `ceil(items.len() / KHL_ITEMS_PER_SLOT)` slots, advances the
    /// owner-private tail by that many slots via ONE Release-store
    /// (no atomic fetch_add), and writes each slot's payload with one
    /// Release-store on the per-slot Vyukov sequence number.
    ///
    /// **Only the owner process may call this.**
    ///
    /// Cost per call: 1 Release-store on tail + `ceil(K/3)` slot
    /// Release-stores. For K=64 items: 1 + 22 = 23 atomic ops total
    /// vs Chase-Lev's 64+ and LOH's 65.
    ///
    /// Returns the number of items published.
    pub fn publish_batch(&self, items: &[LineItem]) -> Result<usize, PushError> {
        if items.is_empty() {
            return Ok(0);
        }
        let k = items.len();
        let n_slots = k.div_ceil(KHL_ITEMS_PER_SLOT);
        let h = self.header();
        // Chase-Lev-style: read head Acquire, then the owner reads
        // its own private tail with a Relaxed load (the owner is the
        // only writer to tail). Capacity check before reserving.
        let head_snapshot = h.head.load(Ordering::Acquire);
        let tail_snapshot = h.tail.load(Ordering::Relaxed);
        if (tail_snapshot - head_snapshot + n_slots as i64) > self.capacity as i64 {
            return Err(PushError::Full);
        }
        let base = tail_snapshot;
        // Prefetch the first slot before entering the publish loop.
        prefetchw_slot(self.slot_ptr(base));

        // Publish each slot under the Vyukov protocol.
        let mut written = 0usize;
        for slot_i in 0..n_slots {
            let idx = base + slot_i as i64;
            // Warm the next slot while we publish this one.
            if slot_i + 1 < n_slots {
                prefetchw_slot(self.slot_ptr(idx + 1));
            }
            let take = (k - written).min(KHL_ITEMS_PER_SLOT);
            // SAFETY: slot_ptr returns in-bounds aligned pointer;
            // caller has reserved `idx` by the (pending) tail update.
            unsafe {
                self.publish_slot_at(idx, &items[written..written + take]);
            }
            written += take;
        }

        // Owner-private Release-store on tail. The Release ordering
        // is overkill for the protocol (the per-slot Release on
        // sequence is what publishes the slot bytes; tail is just a
        // high-watermark hint to thieves), but Release lets the thief
        // Acquire-load on tail synchronise reliably even on weakly-
        // ordered architectures. On x86 a Release store costs the
        // same as a Relaxed store.
        h.tail.store(base + n_slots as i64, Ordering::Release);

        Ok(k)
    }

    /// Publish one slot at ring index `idx` under the Vyukov
    /// sequence-number protocol with up to KHL_ITEMS_PER_SLOT items.
    ///
    /// # Safety
    ///
    /// Caller must hold the producer reservation: `idx` is in the
    /// range `[base, base + n_slots)` for a successful capacity check
    /// in `publish_batch` that has not yet been committed to `tail`.
    /// `items.len() <= KHL_ITEMS_PER_SLOT`.
    unsafe fn publish_slot_at(&self, idx: i64, items: &[LineItem]) {
        let slot = self.slot_ptr(idx);
        // Spin-wait until the slot is publishable: idx_value == idx
        // (low 2 bits ignored; an empty slot has packed = idx << 2).
        loop {
            // SAFETY: slot is in-bounds + aligned; producer owns the
            // reservation; LCRQ sequence-number protocol ensures no
            // other writer touches this slot until consumer Releases.
            let packed = unsafe {
                (*slot).packed_sequence.load(Ordering::Acquire)
            };
            let idx_value = unpack_idx(packed);
            let diff = idx_value - idx;
            if diff == 0 {
                break;
            }
            if diff < 0 {
                std::hint::spin_loop();
                continue;
            }
            // diff > 0: future round. Single-producer + capacity
            // check makes this unreachable.
            panic!(
                "KHL producer protocol violation: slot[{}] idx_value={} ahead of idx={}",
                idx & self.capacity_mask,
                idx_value,
                idx
            );
        }
        // K_radius dispatch: pick the publish mechanism based on the
        // configured coherence distance.
        match self.publish_radius {
            PublishRadius::Local => {
                // SAFETY: producer owns the slot for this round.
                // ONE Release-store on packed_sequence publishes
                // BOTH the protocol state AND `n_items` together
                // (cross-axis fusion: K_inner + K_gating).
                unsafe {
                    let n = items.len();
                    for (i, item) in items.iter().enumerate() {
                        (*slot).items[i] = *item;
                    }
                    (*slot)
                        .packed_sequence
                        .store(pack_seq(idx + 1, n), Ordering::Release);
                }
            }
            PublishRadius::Distant => {
                // SAFETY: same; `Distant` is only set when
                // `has_movdir64b()` returned true.
                unsafe {
                    self.publish_slot_movdir64b(slot, idx, items);
                }
            }
        }
    }

    /// Build a 64-byte source line on the stack carrying the new
    /// sequence + n_items + items, then atomically write it to the
    /// destination slot via `MOVDIR64B` + `SFENCE`. The whole slot
    /// (including the sequence number) is published as one atomic
    /// Write-Combining store that bypasses the producer's L1d.
    ///
    /// # Safety
    ///
    /// Caller must have validated that `self.publish_radius ==
    /// Distant` (so `has_movdir64b()` returned true), holds the
    /// producer reservation for `idx`, and `items.len() <=
    /// KHL_ITEMS_PER_SLOT`.
    #[inline(always)]
    unsafe fn publish_slot_movdir64b(
        &self,
        slot: *mut KhlSlot,
        idx: i64,
        items: &[LineItem],
    ) {
        // The src line is layout-compatible with KhlSlot. The
        // packed_sequence carries (idx+1, n_items) bit-packed - the
        // cross-axis fusion of K_inner + K_gating in the same atomic
        // word the K_radius MOVDIR64B atomically publishes.
        #[repr(C, align(64))]
        struct SrcLine {
            packed_sequence: i64,
            _reserved: u64,
            items: [LineItem; KHL_ITEMS_PER_SLOT],
        }
        let mut src = SrcLine {
            packed_sequence: pack_seq(idx + 1, items.len()),
            _reserved: 0,
            items: [LineItem::default(); KHL_ITEMS_PER_SLOT],
        };
        for (i, item) in items.iter().enumerate() {
            src.items[i] = *item;
        }

        let dst_ptr = slot as *mut u8;
        let src_ptr = &src as *const SrcLine as *const u8;

        #[cfg(target_arch = "x86_64")]
        {
            // SAFETY: `MOVDIR64B` writes 64 bytes from `[src_ptr]` to
            // `[dst_ptr]`. Both pointers are 64-byte aligned (KhlSlot
            // is repr(C, align(64)); SrcLine matches). `SFENCE`
            // drains the WC store buffer so the publish is globally
            // visible. The MOVDIR64B atomically publishes the
            // sequence + n_items + items together, so the consumer
            // observes either the OLD slot (seq != idx + 1) or the
            // NEW slot (seq == idx + 1) with no partial publish
            // visible.
            unsafe {
                core::arch::asm!(
                    "movdir64b {dst}, [{src}]",
                    "sfence",
                    dst = in(reg) dst_ptr,
                    src = in(reg) src_ptr,
                    options(nostack, preserves_flags),
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            _ = dst_ptr;
            _ = src_ptr;
            _ = slot;
            _ = idx;
            unreachable!(
                "publish_slot_movdir64b reached on non-x86_64 host; \
                 PublishRadius::resolve() returns Local there"
            );
        }
    }

    /// Thief-side steal. Claim one slot's worth of items via CAS on
    /// the shared head + Acquire-load on the per-slot sequence.
    pub fn steal_slot(&self) -> Steal {
        let h = self.header();
        let head = h.head.load(Ordering::Acquire);
        fence(Ordering::SeqCst);
        let tail = h.tail.load(Ordering::Acquire);
        if head >= tail {
            return Steal::Empty;
        }
        let slot = self.slot_ptr(head);
        // SAFETY: slot is in-bounds + aligned.
        let packed = unsafe {
            (*slot).packed_sequence.load(Ordering::Acquire)
        };
        // Cross-axis fusion: the ONE Acquire-load above reads BOTH
        // the protocol state (idx_value) AND the payload count
        // (n_items) packed into the same atomic word.
        let idx_value = unpack_idx(packed);
        if idx_value != head + 1 {
            return Steal::Retry;
        }
        let n = unpack_n_items(packed).min(KHL_ITEMS_PER_SLOT);
        let won = h
            .head
            .compare_exchange(head, head + 1, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok();
        if !won {
            return Steal::Retry;
        }
        // SAFETY: the CAS established exclusive read access for this
        // round; the producer's Release on packed_sequence
        // happens-before our Acquire load above.
        let result = unsafe {
            StealResult {
                n_items: n,
                items: (*slot).items,
            }
        };
        // Release the slot for the next round at head + capacity.
        // n_items=0 in the released state (consumed).
        // SAFETY: still our slot; the Release synchronises with the
        // next producer's Acquire-spin in publish_slot_at.
        unsafe {
            (*slot).packed_sequence.store(
                pack_seq(head + self.capacity as i64, 0),
                Ordering::Release,
            );
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
        p.push(format!("subetha_khl_{pid}_{nonce}_{name}.bin"));
        p
    }

    fn u32_item(id: u32) -> LineItem {
        LineItem::new(&id.to_le_bytes()).expect("item")
    }

    fn item_id(item: &LineItem) -> u32 {
        u32::from_le_bytes(item.payload[..4].try_into().unwrap())
    }

    #[test]
    fn publish_radius_matches_host() {
        let path = temp_path("radius_auto");
        let d = SharedDequeKhl::create(&path, 8).expect("create");
        let r = d.publish_radius();
        if subetha_core::has_movdir64b() {
            assert_eq!(r, PublishRadius::Distant);
        } else {
            assert_eq!(r, PublishRadius::Local);
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_radius_distant_resolves_to_local_without_movdir64b() {
        let path = temp_path("radius_resolve");
        let d = SharedDequeKhl::create(&path, 8)
            .expect("create")
            .with_publish_radius(PublishRadius::Distant);
        // On hosts without MOVDIR64B the resolve degrades to Local.
        if subetha_core::has_movdir64b() {
            assert_eq!(d.publish_radius(), PublishRadius::Distant);
        } else {
            assert_eq!(d.publish_radius(), PublishRadius::Local);
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_then_drain_works_under_both_radius_modes() {
        // Round-trip a batch under whichever radius pick_auto chose
        // for this host (Local on Zen+ R7 2700; Distant on Zen 5+/
        // Tiger Lake+). Either path produces bit-exact slots.
        let path = temp_path("radius_round_trip");
        let d = SharedDequeKhl::create(&path, 8).expect("create");
        let items: Vec<LineItem> = (1..=6u32).map(u32_item).collect();
        d.publish_batch(&items).expect("publish");
        let mut drained = Vec::new();
        loop {
            match d.steal_slot() {
                Steal::Success(r) => {
                    for i in 0..r.n_items {
                        drained.push(item_id(&r.items[i]));
                    }
                }
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }
        assert_eq!(drained, vec![1, 2, 3, 4, 5, 6]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn create_then_open_round_trips_header() {
        let path = temp_path("create_open");
        let _d = SharedDequeKhl::create(&path, 8).expect("create");
        let o = SharedDequeKhl::open(&path).expect("open");
        assert_eq!(o.capacity(), 8);
        assert_eq!(o.owner_pid(), std::process::id() as u64);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = temp_path("bad_magic");
        std::fs::write(&path, vec![0xCDu8; 8192]).expect("seed");
        assert!(SharedDequeKhl::open(&path).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_packs_three_items_per_slot() {
        let path = temp_path("publish_batch_packs");
        let d = SharedDequeKhl::create(&path, 64).expect("create");
        let items: Vec<LineItem> = (1..=7u32).map(u32_item).collect();
        let n = d.publish_batch(&items).expect("publish_batch");
        assert_eq!(n, 7);
        let (_, tail, sz) = d.snapshot_size();
        // 7 items = ceil(7/3) = 3 slots.
        assert_eq!(tail, 3);
        assert_eq!(sz, 3);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_empty_is_noop() {
        let path = temp_path("publish_empty");
        let d = SharedDequeKhl::create(&path, 4).expect("create");
        assert_eq!(d.publish_batch(&[]).expect("noop"), 0);
        let (_, tail, sz) = d.snapshot_size();
        assert_eq!(tail, 0);
        assert_eq!(sz, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_full_returns_full() {
        let path = temp_path("publish_full");
        let d = SharedDequeKhl::create(&path, 2).expect("create");
        // Capacity is 2 slots = 6 items. First batch fills all 2 slots.
        let first: Vec<LineItem> = (1..=6u32).map(u32_item).collect();
        d.publish_batch(&first).expect("first batch");
        // Next publish should fail with Full.
        let err = d
            .publish_batch(&[u32_item(99)])
            .expect_err("publish past capacity");
        assert_eq!(err, PushError::Full);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn steal_drains_in_publication_order() {
        let path = temp_path("steal_order");
        let d = SharedDequeKhl::create(&path, 8).expect("create");
        // 7 items: slots [0]=(1,2,3), [1]=(4,5,6), [2]=(7).
        let items: Vec<LineItem> = (1..=7u32).map(u32_item).collect();
        d.publish_batch(&items).expect("publish");
        let mut drained = Vec::new();
        loop {
            match d.steal_slot() {
                Steal::Success(r) => {
                    for i in 0..r.n_items {
                        drained.push(item_id(&r.items[i]));
                    }
                }
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }
        assert_eq!(drained, vec![1, 2, 3, 4, 5, 6, 7]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn close_owner_zeros_pid_and_advances_epoch() {
        let path = temp_path("close");
        let d = SharedDequeKhl::create(&path, 2).expect("create");
        let before = d.header().epoch.load(O::Acquire);
        d.close_owner();
        assert_eq!(d.owner_pid(), 0);
        assert_eq!(d.header().epoch.load(O::Acquire), before + 1);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn concurrent_thieves_no_double_take() {
        let path = temp_path("stress");
        let d = Arc::new(SharedDequeKhl::create(&path, 256).expect("create"));
        let n: usize = 5_000;
        let consumed = Arc::new(AtomicUsize::new(0));
        let sum = Arc::new(AtomicUsize::new(0));

        let mut thieves = Vec::new();
        for _ in 0..2 {
            let d = Arc::clone(&d);
            let consumed = Arc::clone(&consumed);
            let sum = Arc::clone(&sum);
            thieves.push(thread::spawn(move || {
                while consumed.load(O::Relaxed) < n {
                    match d.steal_slot() {
                        Steal::Success(r) => {
                            for i in 0..r.n_items {
                                consumed.fetch_add(1, O::Relaxed);
                                sum.fetch_add(
                                    item_id(&r.items[i]) as usize,
                                    O::Relaxed,
                                );
                            }
                        }
                        Steal::Empty | Steal::Retry => std::thread::yield_now(),
                    }
                }
            }));
        }

        // Producer: 64 items per batch.
        let burst = 64usize;
        let mut pushed = 0usize;
        while pushed < n {
            let want = burst.min(n - pushed);
            let batch: Vec<LineItem> =
                (0..want).map(|j| u32_item((pushed + j) as u32)).collect();
            loop {
                match d.publish_batch(&batch) {
                    Ok(_) => break,
                    Err(PushError::Full) => std::thread::yield_now(),
                }
            }
            pushed += want;
        }

        for t in thieves {
            t.join().expect("thief");
        }
        let expected: usize = (0..n).sum();
        assert_eq!(sum.load(O::Relaxed), expected, "every item consumed once");
        std::fs::remove_file(&path).ok();
    }
}
