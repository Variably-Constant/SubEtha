//! `SharedDeque<T>` - cross-thread / cross-process Chase-Lev work-
//! stealing deque backed by a memory-mapped file.
//!
//! Chase-Lev's signature asymmetry is what makes this primitive
//! interesting: the *owner* of the deque pushes and pops the bottom
//! end with no atomic CAS on the fast path (a Relaxed store on the
//! `bottom` index), while any number of *thieves* steal from the top
//! end with one CAS each. There is no MPMC ring contention; the
//! local-pop fast path costs roughly one cache-line write.
//!
//! Lifting this protocol into a memory-mapped file lets the *same*
//! deque serve in-process worker-thread stealing AND cross-process
//! work distribution. A second process opens the same file via
//! [`SharedDeque::open_as_thief`] and steals from a remote owner with
//! the identical CAS protocol, because the atomics touch physical
//! pages whose coherence is identical to the cross-thread case
//! (kernel uninvolved on the steal hot path).
//!
//! The trade is a discriminant on the stored type: values stored in
//! the deque must implement [`Marshal`], the type-system contract
//! that the value's bytes mean the same thing in every address
//! space. Closures with environment-capturing pointers cannot be
//! stored directly; they must travel through
//! [`pass_registry`](crate::pass_registry) as `(closure_id, args)`
//! pairs where `args: T: Marshal`.
//!
//! # Source
//!
//! - David Chase and Yossi Lev, *Dynamic Circular Work-Stealing
//!   Deque*, SPAA 2005.
//! - The capacity is fixed at create time so the slot layout matches
//!   the MMF's fixed file size; the paper's resizing variant is a
//!   different primitive shape with a different contract and is not
//!   what this file implements.
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | DequeHeader (64B aligned)   |
//! |   magic, capacity, slot_bytes
//! |   owner_pid (informational) |
//! |   top: AtomicI64            |
//! |   bottom: AtomicI64         |
//! +-----------------------------+
//! | Slot[0]  (slot_bytes)       |  marshalled T payload
//! | Slot[1]                     |
//! | ...                         |
//! | Slot[capacity - 1]          |
//! +-----------------------------+
//! ```
//!
//! `capacity` is required to be a power of two so the slot-index
//! computation is `b & (capacity - 1)`. Each slot stores exactly
//! `T::PAYLOAD_BYTES` rounded up to 8-byte alignment.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{fence, AtomicI64, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};
use subetha_core::Marshal;

/// Prefetch the cache line at `addr` with write-intent (M-state
/// hint). Emits `PREFETCHW` directly via inline asm on x86_64
/// because Rust's stable `_mm_prefetch` only exposes the
/// T0/T1/T2/NTA hints, which force a publisher write to pay an RFO
/// coherence upgrade. `PREFETCHW` brings the line to M-state
/// directly so the subsequent slot write costs one cycle instead of
/// a cross-core RFO. `PREFETCHW` is a NOP on x86_64 CPUs without
/// the PRFCHW feature flag (3DNow-era AMD has it natively; Intel
/// since Broadwell), so it is safe to unconditionally emit.
#[inline(always)]
fn prefetchw_line(addr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `prefetchw` is a hardware hint and never faults on
        // unmapped memory; the CPU ignores invalid addresses.
        unsafe {
            core::arch::asm!(
                "prefetchw [{ptr}]",
                ptr = in(reg) addr,
                options(nostack, preserves_flags),
            );
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        // Mark `addr` as used on non-x86_64 targets where the
        // PREFETCHW path is cfg'd out. `_ = addr` is a statement
        // assignment that satisfies the unused-variable lint
        // without inserting a real drop (the value is `*const u8`,
        // which is `Copy` and has no drop glue anyway).
        _ = addr;
    }
}

/// ASCII 'WDEQ' + version 1.
pub const DEQUE_MAGIC: u64 = 0x5744_4551_0000_0001;

/// Errors returned by `SharedDeque` operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DequeError {
    Io(String),
    InvalidCapacity,
    InvalidMagic,
    CapacityMismatch { file_capacity: u64, requested: u64 },
    SlotBytesMismatch { file_slot_bytes: u32, type_slot_bytes: u32 },
    Full,
    Marshal(subetha_core::MarshalError),
}

impl From<std::io::Error> for DequeError {
    fn from(e: std::io::Error) -> Self { Self::Io(e.to_string()) }
}

impl From<subetha_core::MarshalError> for DequeError {
    fn from(e: subetha_core::MarshalError) -> Self { Self::Marshal(e) }
}

/// File header. 64-byte aligned, fits in one cache line so the
/// owner's `bottom` updates and a thief's `top` CAS land on the
/// same cache-line coherence path.
#[repr(C, align(64))]
pub struct DequeHeader {
    pub magic: u64,
    pub capacity: u64,
    pub slot_bytes: u32,
    pub _reserved_a: u32,
    pub owner_pid: u64,
    pub top: AtomicI64,
    pub bottom: AtomicI64,
    pub epoch: AtomicU64,
    pub _reserved_b: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<DequeHeader>() == 64);

/// Per-T slot byte width, rounded up to 8-byte alignment for atomic-
/// friendly storage.
pub const fn slot_bytes_for<T: Marshal>() -> u32 {
    let raw = T::PAYLOAD_BYTES;
    let rounded = (raw + 7) & !7;
    let with_min = if rounded < 8 { 8 } else { rounded };
    with_min as u32
}

/// Compute the total MMF byte size for a deque of `capacity` slots
/// holding `T` values.
pub const fn deque_file_size<T: Marshal>(capacity: usize) -> usize {
    std::mem::size_of::<DequeHeader>() + capacity * slot_bytes_for::<T>() as usize
}

/// Chase-Lev work-stealing deque backed by a memory-mapped file.
///
/// See the [module docs](self) for the protocol description and
/// citation. Drop semantics: dropping the handle unmaps the file but
/// does NOT delete it (in keeping with the rest of `subetha-cxc`'s
/// MMF-backed primitives).
pub struct SharedDeque<T: Marshal> {
    mmap: MmapMut,
    capacity: usize,
    slot_bytes: usize,
    _file: File,
    _phantom: PhantomData<T>,
}

// SAFETY: the underlying mmap is `Send` and `Sync` (memmap2 guarantees
// this for MmapMut), and the Chase-Lev protocol is the synchronisation.
// PhantomData<T> carries no runtime data.
unsafe impl<T: Marshal + Send> Send for SharedDeque<T> {}
unsafe impl<T: Marshal + Send> Sync for SharedDeque<T> {}

impl<T: Marshal> SharedDeque<T> {
    /// Create a new MMF-backed deque at `path` with the given
    /// capacity. `capacity` must be a non-zero power of two.
    ///
    /// The calling process is recorded as the "owner" in the header
    /// for informational purposes; the protocol does not enforce
    /// single-owner discipline at runtime - that is a contract the
    /// caller's scheduler is responsible for.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, DequeError> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(DequeError::InvalidCapacity);
        }
        let slot_bytes = slot_bytes_for::<T>() as usize;
        let total = std::mem::size_of::<DequeHeader>() + capacity * slot_bytes;
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        // SAFETY: a fresh file of the right size is exclusive to this
        // process while we initialise the header.
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        // Initialise header in place.
        // SAFETY: the mapped region is exactly `total` bytes; the
        // first sizeof(DequeHeader) bytes are aligned because mmap
        // returns page-aligned memory.
        let header_ptr = mmap.as_mut_ptr() as *mut DequeHeader;
        unsafe {
            (*header_ptr).magic = DEQUE_MAGIC;
            (*header_ptr).capacity = capacity as u64;
            (*header_ptr).slot_bytes = slot_bytes as u32;
            (*header_ptr)._reserved_a = 0;
            (*header_ptr).owner_pid = std::process::id() as u64;
            (*header_ptr).top.store(0, Ordering::Relaxed);
            (*header_ptr).bottom.store(0, Ordering::Relaxed);
            (*header_ptr).epoch.store(0, Ordering::Relaxed);
            (*header_ptr)._reserved_b = [0; 8];
        }
        mmap.flush()?;
        Ok(Self { mmap, capacity, slot_bytes, _file: file, _phantom: PhantomData })
    }

    /// Open an existing MMF-backed deque created by another handle.
    ///
    /// The caller asserts the role of "thief" - the same protocol
    /// works for any number of thief handles open at once, in any
    /// number of processes. The header's `slot_bytes` field is
    /// verified against `T::PAYLOAD_BYTES`; opening a deque whose
    /// slot width does not match `T` returns
    /// [`DequeError::SlotBytesMismatch`].
    pub fn open_as_thief(path: impl AsRef<Path>) -> Result<Self, DequeError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let len = file.metadata()?.len() as usize;
        // SAFETY: the file's bytes back this process's view; the
        // owner process is the only writer of slot payloads, and we
        // are about to validate the header.
        let mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        let header_ptr = mmap.as_ptr() as *const DequeHeader;
        let (magic, capacity, slot_bytes) = unsafe {
            ((*header_ptr).magic, (*header_ptr).capacity, (*header_ptr).slot_bytes)
        };
        if magic != DEQUE_MAGIC { return Err(DequeError::InvalidMagic); }
        let expected_slot_bytes = slot_bytes_for::<T>();
        if slot_bytes != expected_slot_bytes {
            return Err(DequeError::SlotBytesMismatch {
                file_slot_bytes: slot_bytes,
                type_slot_bytes: expected_slot_bytes,
            });
        }
        Ok(Self {
            mmap,
            capacity: capacity as usize,
            slot_bytes: slot_bytes as usize,
            _file: file,
            _phantom: PhantomData,
        })
    }

    fn header(&self) -> &DequeHeader {
        // SAFETY: header was initialised at create time; layout is
        // stable across the lifetime of the mmap.
        unsafe { &*(self.mmap.as_ptr() as *const DequeHeader) }
    }

    fn slot_ptr(&self, idx: usize) -> *mut u8 {
        let base = self.mmap.as_ptr() as usize + std::mem::size_of::<DequeHeader>();
        (base + idx * self.slot_bytes) as *mut u8
    }

    /// Capacity (power of two).
    pub fn capacity(&self) -> usize { self.capacity }

    /// Approximate current length. Not authoritative under
    /// concurrent steal / push; useful for heuristics and observers.
    pub fn approx_len(&self) -> usize {
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        let t = h.top.load(Ordering::Relaxed);
        (b - t).max(0) as usize
    }

    /// Owner side: push a value onto the bottom of the deque.
    ///
    /// This is the only operation safe to call from the owner thread
    /// alone; calling it concurrently from multiple threads breaks
    /// the Chase-Lev protocol. The fast path is one Relaxed load on
    /// `bottom`, one Acquire load on `top`, the marshal, a Release
    /// fence, and one Relaxed store on `bottom`. No CAS, no mutex.
    pub fn push(&self, value: &T) -> Result<(), DequeError> {
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        // Issue `PREFETCHW` on the slot the marshal is about to write
        // BEFORE the `top.load(Acquire)`. The Acquire-load hides the
        // prefetch's latency: by the time we drop into `value.marshal`
        // the slot's cache line is already arriving in M-state, so the
        // write does not pay a cross-core RFO upgrade.
        let idx = (b as usize) & (self.capacity - 1);
        prefetchw_line(self.slot_ptr(idx));
        let t = h.top.load(Ordering::Acquire);
        if b - t >= self.capacity as i64 {
            return Err(DequeError::Full);
        }
        // SAFETY: idx is in [0, capacity); each slot is slot_bytes
        // long; the slice covers a valid mapped region.
        let slot = unsafe { std::slice::from_raw_parts_mut(self.slot_ptr(idx), self.slot_bytes) };
        value.marshal(slot);
        fence(Ordering::Release);
        h.bottom.store(b + 1, Ordering::Relaxed);
        Ok(())
    }

    /// Owner-side batched push via a per-slot fill closure.
    /// Reserves `n` contiguous slots under ONE `top.load(Acquire)`,
    /// then calls `fill(i, slot_bytes)` for each slot, then ONE
    /// Release fence and ONE `bottom.store(Relaxed)` publishes all
    /// `n` slots atomically from the thieves' perspective.
    ///
    /// The closure writes directly into the slot's raw bytes,
    /// avoiding any intermediate `T` buffer. This is the path
    /// caller-defined fat-slot types take when they want to bypass
    /// the [`Marshal`] indirection on the hot path. Returns
    /// `Err(Full)` if the batch would overflow capacity at the
    /// current `top` snapshot.
    pub fn push_batch_with<F>(&self, n: usize, mut fill: F) -> Result<(), DequeError>
    where
        F: FnMut(usize, &mut [u8]),
    {
        if n == 0 {
            return Ok(());
        }
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        let t = h.top.load(Ordering::Acquire);
        if (b - t) + n as i64 > self.capacity as i64 {
            return Err(DequeError::Full);
        }
        let mask = self.capacity - 1;
        prefetchw_line(self.slot_ptr((b as usize) & mask));
        for i in 0..n {
            let idx = ((b + i as i64) as usize) & mask;
            if i + 1 < n {
                prefetchw_line(self.slot_ptr(((b + (i + 1) as i64) as usize) & mask));
            }
            // SAFETY: idx is in [0, capacity); each slot is
            // slot_bytes long; the slice covers a valid mapped
            // region; the capacity check above guarantees the
            // producer-side reservation is free of consumer claims.
            let slot = unsafe {
                std::slice::from_raw_parts_mut(self.slot_ptr(idx), self.slot_bytes)
            };
            fill(i, slot);
        }
        fence(Ordering::Release);
        h.bottom.store(b + n as i64, Ordering::Relaxed);
        Ok(())
    }

    /// Owner-side batched push. Amortizes ONE `top.load(Acquire)`,
    /// ONE Release fence, and ONE `bottom.store(Relaxed)` across the
    /// whole batch instead of paying them per item. Critical for
    /// producer-fast workloads where the per-item `top` load goes
    /// cross-core to the thief and dominates per-push cost.
    ///
    /// Returns `Err(Full)` (and writes no slots) if the batch would
    /// overflow capacity at the current `top` snapshot.
    pub fn push_batch(&self, values: &[T]) -> Result<(), DequeError> {
        if values.is_empty() {
            return Ok(());
        }
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        // Single Acquire-load on top covers the whole batch.
        let t = h.top.load(Ordering::Acquire);
        if (b - t) + values.len() as i64 > self.capacity as i64 {
            return Err(DequeError::Full);
        }
        // Prefetch the first slot before the marshal loop.
        let mask = self.capacity - 1;
        prefetchw_line(self.slot_ptr((b as usize) & mask));
        for (i, v) in values.iter().enumerate() {
            let idx = ((b + i as i64) as usize) & mask;
            // Warm the next slot while we write this one.
            if i + 1 < values.len() {
                prefetchw_line(self.slot_ptr(((b + (i + 1) as i64) as usize) & mask));
            }
            // SAFETY: idx is in [0, capacity); each slot is
            // slot_bytes long; the slice covers a valid mapped
            // region; the capacity check above guarantees the
            // producer-side reservation is free of consumer claims.
            let slot = unsafe {
                std::slice::from_raw_parts_mut(self.slot_ptr(idx), self.slot_bytes)
            };
            v.marshal(slot);
        }
        // ONE Release fence + ONE bottom store publishes all N slots
        // atomically from the thief's perspective: after the store,
        // bottom advanced by N and every slot in [b, b+N) carries
        // the marshalled bytes (Release-fence ordered them all
        // before this store).
        fence(Ordering::Release);
        h.bottom.store(b + values.len() as i64, Ordering::Relaxed);
        Ok(())
    }

    /// Owner side: pop a value off the bottom of the deque.
    ///
    /// Fast path (no contention with thieves) is a single Relaxed
    /// load + Relaxed store on `bottom`, a SeqCst fence, and a
    /// Relaxed load on `top`. Only the contended case - one item
    /// left and a thief is trying to take it - falls back to a CAS
    /// on `top` to disambiguate.
    pub fn pop(&self) -> Option<T> {
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed) - 1;
        h.bottom.store(b, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let t = h.top.load(Ordering::Relaxed);
        if b < t {
            // Deque was empty; restore bottom and return.
            h.bottom.store(b + 1, Ordering::Relaxed);
            return None;
        }
        let idx = (b as usize) & (self.capacity - 1);
        // SAFETY: idx is in [0, capacity); slot is fully marshalled.
        let slot = unsafe { std::slice::from_raw_parts(self.slot_ptr(idx) as *const u8, self.slot_bytes) };
        let v = T::unmarshal(slot).ok()?;
        if b > t {
            // No race; the popped slot was strictly above any thief's
            // reach.
            return Some(v);
        }
        // b == t: one element left; race a thief.
        let race_result = h.top.compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed);
        h.bottom.store(b + 1, Ordering::Relaxed);
        if race_result.is_ok() {
            Some(v)
        } else {
            // Thief won; we lose the value we read.
            None
        }
    }

    /// Thief side: steal a value off the top of the deque.
    ///
    /// Any number of threads or processes can call this concurrently
    /// with each other and with the owner's `pop`. Each call costs
    /// one Acquire load on `top`, a SeqCst fence, one Acquire load
    /// on `bottom`, a slot read, and one CAS on `top`. The slot read
    /// happens before the CAS so a CAS-loss discards a possibly-stale
    /// value safely.
    pub fn steal(&self) -> Option<T> {
        let h = self.header();
        let t = h.top.load(Ordering::Acquire);
        fence(Ordering::SeqCst);
        let b = h.bottom.load(Ordering::Acquire);
        if t >= b { return None; }
        let idx = (t as usize) & (self.capacity - 1);
        // SAFETY: idx is in [0, capacity); slot bytes are stable
        // until the owner's push wraps around `capacity` operations
        // later, which cannot happen before this CAS resolves
        // because `t < b` here and the owner is bounded by capacity.
        let slot = unsafe { std::slice::from_raw_parts(self.slot_ptr(idx) as *const u8, self.slot_bytes) };
        let v = T::unmarshal(slot).ok()?;
        match h.top.compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => Some(v),
            Err(_) => None,
        }
    }

    /// Force the mapped region to be written back to disk. Useful
    /// for the disk-persistent deployment mode.
    pub fn flush(&self) -> std::io::Result<()> { self.mmap.flush() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-deque-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn single_thread_push_pop_lifo() {
        let path = tmp("st-lifo");
        let dq = SharedDeque::<u64>::create(&path, 64).unwrap();
        for i in 0..10u64 { dq.push(&i).unwrap(); }
        let mut popped = Vec::new();
        while let Some(v) = dq.pop() { popped.push(v); }
        assert_eq!(popped, (0..10).rev().collect::<Vec<_>>(),
                   "Chase-Lev owner pop is LIFO");
        drop(dq);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_pop_returns_none() {
        let path = tmp("empty-pop");
        let dq = SharedDeque::<u64>::create(&path, 8).unwrap();
        assert_eq!(dq.pop(), None);
        assert_eq!(dq.approx_len(), 0);
        drop(dq);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn full_push_returns_err() {
        let path = tmp("full-push");
        let dq = SharedDeque::<u64>::create(&path, 4).unwrap();
        for i in 0..4u64 { dq.push(&i).unwrap(); }
        assert!(matches!(dq.push(&999), Err(DequeError::Full)));
        drop(dq);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn capacity_must_be_power_of_two() {
        let path = tmp("badcap");
        assert!(matches!(
            SharedDeque::<u64>::create(&path, 7),
            Err(DequeError::InvalidCapacity)
        ));
    }

    #[test]
    fn second_handle_steals_fifo() {
        // Steals take from the TOP (oldest first), so a sequence of
        // pushes then steals reads FIFO order.
        let path = tmp("steal-fifo");
        let owner = SharedDeque::<u64>::create(&path, 16).unwrap();
        for i in 0..5u64 { owner.push(&i).unwrap(); }
        let thief = SharedDeque::<u64>::open_as_thief(&path).unwrap();
        let mut stolen = Vec::new();
        while let Some(v) = thief.steal() { stolen.push(v); }
        assert_eq!(stolen, (0..5).collect::<Vec<_>>());
        drop(thief); drop(owner);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn one_owner_one_thief_concurrent() {
        let path = tmp("1o1t");
        let owner = Arc::new(SharedDeque::<u64>::create(&path, 1024).unwrap());
        let thief = Arc::new(SharedDeque::<u64>::open_as_thief(&path).unwrap());
        let n = 10_000u64;
        let owner_h = owner.clone();
        let producer = thread::spawn(move || {
            for i in 0..n {
                while owner_h.push(&i).is_err() { std::hint::spin_loop(); }
            }
        });
        let consumer = thread::spawn(move || {
            let mut taken = 0u64;
            let mut sum = 0u64;
            while taken < n {
                if let Some(v) = thief.steal() { sum += v; taken += 1; }
                else { std::hint::spin_loop(); }
            }
            sum
        });
        producer.join().unwrap();
        let sum = consumer.join().unwrap();
        assert_eq!(sum, (0..n).sum::<u64>());
        drop(owner);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn one_owner_four_thieves_concurrent() {
        let path = tmp("1o4t");
        let owner = Arc::new(SharedDeque::<u64>::create(&path, 4096).unwrap());
        let n = 8_000u64;
        let total = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let owner_h = owner.clone();
        let producer = thread::spawn(move || {
            for i in 1..=n {
                while owner_h.push(&i).is_err() { std::hint::spin_loop(); }
            }
        });
        let mut thieves = Vec::new();
        for _ in 0..4 {
            let path_t = path.clone();
            let total_t = total.clone();
            thieves.push(thread::spawn(move || {
                let h = SharedDeque::<u64>::open_as_thief(&path_t).unwrap();
                let stop = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    if let Some(v) = h.steal() {
                        total_t.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
                    } else if std::time::Instant::now() > stop {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        producer.join().unwrap();
        // Drain remaining from owner side.
        while let Some(v) = owner.pop() {
            total.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        for t in thieves { t.join().unwrap(); }
        // Drain any final stragglers via a fresh thief handle.
        let drain = SharedDeque::<u64>::open_as_thief(&path).unwrap();
        while let Some(v) = drain.steal() {
            total.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
        }
        let expected = (1..=n).sum::<u64>();
        let actual = total.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(actual, expected, "all pushed values must be accounted for");
        drop(drain); drop(owner);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn slot_bytes_mismatch_rejected_on_open() {
        let path = tmp("mismatch");
        let _o = SharedDeque::<u64>::create(&path, 16).unwrap();
        let result = SharedDeque::<u128>::open_as_thief(&path);
        assert!(matches!(result, Err(DequeError::SlotBytesMismatch { .. })));
        drop(_o);
        std::fs::remove_file(&path).ok();
    }
}
