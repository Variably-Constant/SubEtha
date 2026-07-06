//! `SharedRegion<T>` - cross-process typed arena with position-
//! independent `OffsetPtr<T>` references.
//!
//! The foundational building block for cross-process pointer-bearing
//! data structures (BTree nodes, trie nodes, linked lists, any
//! pointer-graph). Pointers are `OffsetPtr<T> { index: u32 }` which
//! resolve via `mmap_base + index * size_of::<T>()` in any process.
//!
//! # Layout
//!
//! ```text
//! +----------------------------+
//! | RegionHeader (64B aligned) |
//! |   magic, capacity          |
//! |   bump_next: AtomicU64     |
//! |   free_head: AtomicU64     |  (counter << 32 | index)
//! +----------------------------+
//! | next[capacity]             |  (free-list links, when slot free)
//! +----------------------------+
//! | slots[capacity]            |  (T payloads, size_of<T> each)
//! +----------------------------+
//! ```
//!
//! Free-list links live in a separate `next[capacity]` array (not
//! union'd into T storage) because T need not be 4-byte aligned. The
//! cost is `4 * capacity` extra bytes; the gain is layout simplicity
//! and zero overhead on the T storage.
//!
//! # Concurrent allocation protocol
//!
//! Allocate:
//! 1. Try to pop from `free_head` (lock-free Treiber stack):
//!    - Load packed `(counter, index)`.
//!    - If `index == NIL_INDEX`, free list is empty; fall through.
//!    - Read `next[index]`; CAS `free_head` to `(counter+1, next)`.
//!    - On success, return `OffsetPtr { index }`.
//! 2. Bump alloc: `bump_next.fetch_add(1, AcqRel)`.
//!    - If the returned index >= capacity, rollback and return Full.
//!    - Write `value` into `slots[index]`; return `OffsetPtr { index }`.
//!
//! Free:
//! 1. Read current `(counter, head)` from `free_head`.
//! 2. Write `head` into `next[ptr.index]`.
//! 3. CAS `free_head` to `(counter+1, ptr.index)`.
//! 4. On CAS lose, retry from step 1.
//!
//! # ABA safety
//!
//! The 32-bit counter prevents ABA: every push bumps the counter, so
//! the packed word is different even when an index repeats. 32 bits
//! of counter span 4B operations, which is far beyond any realistic
//! concrete race window.
//!
//! # No drop semantics
//!
//! T: Copy + Sized. Allocated T values are NOT dropped on `free`
//! (Copy types don't need drop, and we can't run drop glue on bytes
//! living in shared memory anyway). `free` returns the value as a
//! by-copy.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const REGION_MAGIC: u32 = 0x4150_5247;
pub const NIL_INDEX: u32 = u32::MAX;

#[repr(C, align(64))]
pub struct RegionHeader {
    pub magic: u32,
    pub capacity: u32,
    pub slot_size: u32,
    _pad1: u32,
    pub bump_next: AtomicU64,
    pub free_head: AtomicU64,
    _pad2: [u8; 32],
}

const _: () = {
    assert!(size_of::<RegionHeader>() == 64);
};

pub const fn region_file_size(capacity: usize, slot_size: usize) -> usize {
    size_of::<RegionHeader>()
        + capacity * size_of::<AtomicU32>()    // next[] array
        + capacity * slot_size                 // slots[]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionError {
    Full,
    InvalidPtr,
    PayloadTooLarge,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for RegionError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

/// Position-independent pointer to a slot in a SharedRegion. Stable
/// across processes because the underlying MMF is byte-identical;
/// adding `mmap_base + ptr.index * size_of::<T>()` resolves in any
/// process.
#[derive(Debug)]
#[repr(C)]
pub struct OffsetPtr<T> {
    pub index: u32,
    _phantom: PhantomData<T>,
}

impl<T> Clone for OffsetPtr<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for OffsetPtr<T> {}
impl<T> PartialEq for OffsetPtr<T> {
    fn eq(&self, other: &Self) -> bool { self.index == other.index }
}
impl<T> Eq for OffsetPtr<T> {}
impl<T> std::hash::Hash for OffsetPtr<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

impl<T> OffsetPtr<T> {
    pub const NIL: Self = Self { index: NIL_INDEX, _phantom: PhantomData };

    #[inline]
    pub fn new(index: u32) -> Self {
        Self { index, _phantom: PhantomData }
    }

    #[inline]
    pub fn is_nil(self) -> bool { self.index == NIL_INDEX }
}

#[inline]
fn pack(counter: u32, index: u32) -> u64 {
    ((counter as u64) << 32) | (index as u64)
}
#[inline]
fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

pub struct SharedRegion<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    next_offset: usize,
    slots_offset: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedRegion<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedRegion<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedRegion<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedRegion<T> {
    pub fn create(
        path: impl AsRef<Path>, capacity: usize,
    ) -> Result<Self, RegionError> {
        assert!(capacity >= 1);
        assert!(capacity < NIL_INDEX as usize, "capacity must be < u32::MAX");
        let slot_size = size_of::<T>();
        let total = region_file_size(capacity, slot_size);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        crate::mmf_warm::warm_mmap(&mut mmap);
        let hdr = mmap.as_mut_ptr() as *mut RegionHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<RegionHeader>());
            (*hdr).magic = REGION_MAGIC;
            (*hdr).capacity = capacity as u32;
            (*hdr).slot_size = slot_size as u32;
            (*hdr).bump_next.store(0, Ordering::Release);
            (*hdr).free_head.store(pack(0, NIL_INDEX), Ordering::Release);
        }
        // next[] array zero-init (zeros mean "this slot is not in a free
        // chain"; only meaningful when slot is on the free list, which
        // it isn't at construction).
        let next_offset = size_of::<RegionHeader>();
        let slots_offset = next_offset + capacity * size_of::<AtomicU32>();
        Ok(Self {
            _file: file, mmap, capacity, next_offset, slots_offset,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_capacity: usize,
    ) -> Result<Self, RegionError> {
        let slot_size = size_of::<T>();
        let total = region_file_size(expected_capacity, slot_size);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(RegionError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        crate::mmf_warm::warm_mmap(&mut mmap);
        let hdr = unsafe { &*(mmap.as_ptr() as *const RegionHeader) };
        if hdr.magic != REGION_MAGIC
            || hdr.capacity != expected_capacity as u32
            || hdr.slot_size != slot_size as u32
        {
            return Err(RegionError::LayoutMismatch);
        }
        let next_offset = size_of::<RegionHeader>();
        let slots_offset = next_offset + expected_capacity * size_of::<AtomicU32>();
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity,
            next_offset, slots_offset,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    /// Raw pointer to the start of the MMF. Useful for primitives
    /// built on top of SharedRegion that need direct memory access
    /// (e.g., SharedLinkedList reads node next/prev/value fields directly
    /// at computed offsets instead of copying the whole node).
    #[inline]
    pub fn mmap_ptr(&self) -> *const u8 { self.mmap.as_ptr() }

    fn header(&self) -> &RegionHeader {
        unsafe { &*(self.mmap.as_ptr() as *const RegionHeader) }
    }

    fn next_link(&self, idx: usize) -> &AtomicU32 {
        let base = unsafe { self.mmap.as_ptr().add(self.next_offset) };
        unsafe { &*(base.add(idx * size_of::<AtomicU32>()) as *const AtomicU32) }
    }

    fn slot_ptr(&self, idx: usize) -> *mut T {
        let base = unsafe { self.mmap.as_ptr().add(self.slots_offset) };
        unsafe { base.add(idx * size_of::<T>()) as *mut T }
    }

    /// Number of slots currently allocated (bump high-water minus
    /// free-list length). This is a snapshot; concurrent
    /// alloc/free may race.
    pub fn len(&self) -> usize {
        let bump = self.header().bump_next.load(Ordering::Acquire) as usize;
        bump.saturating_sub(self.free_count())
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Walk the free list (best-effort under concurrent activity)
    /// and return its length. O(N_free).
    pub fn free_count(&self) -> usize {
        let mut count = 0usize;
        let (_, mut idx) = unpack(self.header().free_head.load(Ordering::Acquire));
        let mut visited = 0;
        while idx != NIL_INDEX && (visited as usize) < self.capacity {
            count += 1;
            visited += 1;
            idx = self.next_link(idx as usize).load(Ordering::Acquire);
        }
        count
    }

    /// Allocate a slot. Tries the free list first, then bump
    /// allocates. Returns `Err(Full)` when both are exhausted.
    pub fn allocate(&self, value: T) -> Result<OffsetPtr<T>, RegionError> {
        // 1. Try Treiber-stack pop from the free list.
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, idx) = unpack(head);
            if idx == NIL_INDEX { break; } // free list empty; bump path
            let next_idx = self.next_link(idx as usize).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next_idx);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                unsafe { std::ptr::write(self.slot_ptr(idx as usize), value); }
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::region::OP_ALLOCATE, 0);
                return Ok(OffsetPtr::new(idx));
            }
            // CAS lost; retry.
        }
        // 2. Bump allocation.
        let idx = self.header().bump_next.fetch_add(1, Ordering::AcqRel);
        if idx >= self.capacity as u64 {
            self.header().bump_next.fetch_sub(1, Ordering::AcqRel);
            self.ring_sidecar
                .push_op(crate::sidecar_ops::region::OP_ALLOCATE, 1); // full
            return Err(RegionError::Full);
        }
        let idx = idx as u32;
        unsafe { std::ptr::write(self.slot_ptr(idx as usize), value); }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::region::OP_ALLOCATE, 0);
        Ok(OffsetPtr::new(idx))
    }

    /// Free a slot, returning the T it held. Pushes onto the
    /// Treiber-stack free list.
    pub fn free(&self, ptr: OffsetPtr<T>) -> Result<T, RegionError> {
        if ptr.is_nil() || (ptr.index as usize) >= self.capacity {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::region::OP_FREE, 1); // invalid ptr
            return Err(RegionError::InvalidPtr);
        }
        let value = unsafe { std::ptr::read(self.slot_ptr(ptr.index as usize)) };
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(ptr.index as usize).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), ptr.index);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::region::OP_FREE, 0);
                return Ok(value);
            }
            // CAS lost; retry.
        }
    }

    /// Read the value at `ptr`. Returns `Err(InvalidPtr)` for nil or
    /// out-of-bounds. Caller is responsible for ensuring the ptr
    /// refers to a still-allocated slot (free'd slots may be
    /// reallocated and contain a different value).
    pub fn get(&self, ptr: OffsetPtr<T>) -> Result<T, RegionError> {
        if ptr.is_nil() || (ptr.index as usize) >= self.capacity {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::region::OP_GET, 1); // invalid ptr
            return Err(RegionError::InvalidPtr);
        }
        let v = unsafe { std::ptr::read(self.slot_ptr(ptr.index as usize)) };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::region::OP_GET, 0);
        Ok(v)
    }

    /// Overwrite the value at `ptr`. Same caveats as `get`: caller
    /// must hold a still-valid pointer.
    pub fn set(&self, ptr: OffsetPtr<T>, value: T) -> Result<(), RegionError> {
        if ptr.is_nil() || (ptr.index as usize) >= self.capacity {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::region::OP_SET, 1); // invalid ptr
            return Err(RegionError::InvalidPtr);
        }
        unsafe { std::ptr::write(self.slot_ptr(ptr.index as usize), value); }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::region::OP_SET, 0);
        Ok(())
    }

    /// Reset the region to empty: bump pointer back to 0 and free
    /// list back to NIL. Existing `OffsetPtr` values become stale;
    /// callers must drop them. Useful for steady-state benches that
    /// need to reset accumulated state between iterations.
    ///
    /// Not thread-safe: do not call concurrently with `allocate` /
    /// `free` from other threads. Intended for single-threaded reset
    /// (e.g. test setup, bench iter setup).
    pub fn clear(&self) {
        self.header().bump_next.store(0, Ordering::Release);
        self.header().free_head.store(pack(0, NIL_INDEX), Ordering::Release);
    }

    pub fn flush(&self) -> Result<(), RegionError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), RegionError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-region-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let r: SharedRegion<u64> = SharedRegion::create(&p, 16).unwrap();
        assert_eq!(r.capacity(), 16);
        assert_eq!(r.len(), 0);
        assert!(r.is_empty());
        assert_eq!(r.free_count(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn allocate_and_get_round_trip() {
        let p = tmp("rt");
        let r: SharedRegion<u64> = SharedRegion::create(&p, 8).unwrap();
        let p1 = r.allocate(100).unwrap();
        let p2 = r.allocate(200).unwrap();
        let p3 = r.allocate(300).unwrap();
        assert_eq!(p1.index, 0);
        assert_eq!(p2.index, 1);
        assert_eq!(p3.index, 2);
        assert_eq!(r.get(p1).unwrap(), 100);
        assert_eq!(r.get(p2).unwrap(), 200);
        assert_eq!(r.get(p3).unwrap(), 300);
        assert_eq!(r.len(), 3);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_region_returns_error_on_bump() {
        let p = tmp("full");
        let r: SharedRegion<u32> = SharedRegion::create(&p, 4).unwrap();
        for i in 0..4u32 { r.allocate(i).unwrap(); }
        assert_eq!(r.allocate(99).err(), Some(RegionError::Full));
        assert_eq!(r.len(), 4);  // rolled back
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn free_returns_value_and_decrements_len() {
        let p = tmp("free");
        let r: SharedRegion<u64> = SharedRegion::create(&p, 8).unwrap();
        let ptr1 = r.allocate(111).unwrap();
        let ptr2 = r.allocate(222).unwrap();
        assert_eq!(r.len(), 2);
        let v = r.free(ptr1).unwrap();
        assert_eq!(v, 111);
        assert_eq!(r.len(), 1);
        assert_eq!(r.free_count(), 1);
        // ptr2 still valid.
        assert_eq!(r.get(ptr2).unwrap(), 222);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn free_then_allocate_reuses_slot() {
        let p = tmp("reuse");
        let r: SharedRegion<u64> = SharedRegion::create(&p, 8).unwrap();
        let p1 = r.allocate(100).unwrap();
        let _p2 = r.allocate(200).unwrap();
        r.free(p1).unwrap();
        // Next allocate should reuse p1's slot (LIFO Treiber stack).
        let p3 = r.allocate(999).unwrap();
        assert_eq!(p3.index, p1.index, "free list should have returned slot 0");
        assert_eq!(r.get(p3).unwrap(), 999);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn free_invalid_ptr_returns_error() {
        let p = tmp("invalid");
        let r: SharedRegion<u64> = SharedRegion::create(&p, 4).unwrap();
        assert_eq!(r.free(OffsetPtr::NIL).err(), Some(RegionError::InvalidPtr));
        let oob = OffsetPtr::<u64>::new(100);
        assert_eq!(r.free(oob).err(), Some(RegionError::InvalidPtr));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_overwrites_value_in_place() {
        let p = tmp("set");
        let r: SharedRegion<u64> = SharedRegion::create(&p, 4).unwrap();
        let ptr = r.allocate(42).unwrap();
        r.set(ptr, 999).unwrap();
        assert_eq!(r.get(ptr).unwrap(), 999);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedRegion<u64> = SharedRegion::create(&p, 16).unwrap();
        let reader: SharedRegion<u64> = SharedRegion::open(&p, 16).unwrap();
        let ptr = writer.allocate(7777).unwrap();
        assert_eq!(reader.get(ptr).unwrap(), 7777);
        writer.set(ptr, 8888).unwrap();
        assert_eq!(reader.get(ptr).unwrap(), 8888);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_allocations_get_distinct_indices() {
        let p = tmp("concurrent");
        let r: Arc<SharedRegion<u64>> = Arc::new(SharedRegion::create(&p, 1024).unwrap());
        let n_threads = 4;
        let per_thread = 100;
        let mut handles = vec![];
        for t in 0..n_threads as u64 {
            let r = r.clone();
            handles.push(thread::spawn(move || {
                let mut ptrs = vec![];
                for i in 0..per_thread as u64 {
                    let v = t * 1000 + i;
                    let ptr = r.allocate(v).unwrap();
                    ptrs.push((v, ptr));
                }
                ptrs
            }));
        }
        let all: Vec<(u64, OffsetPtr<u64>)> = handles.into_iter()
            .flat_map(|h| h.join().unwrap()).collect();

        // All indices distinct.
        let mut indices: Vec<u32> = all.iter().map(|(_, p)| p.index).collect();
        indices.sort();
        for w in indices.windows(2) {
            assert_ne!(w[0], w[1], "two threads got the same slot index");
        }
        // Every allocation reads back its written value.
        for (expected, ptr) in &all {
            assert_eq!(r.get(*ptr).unwrap(), *expected);
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_free_and_realloc_no_corruption() {
        let p = tmp("free-realloc");
        let r: Arc<SharedRegion<u64>> = Arc::new(SharedRegion::create(&p, 64).unwrap());
        // Pre-populate.
        let initial: Vec<OffsetPtr<u64>> = (0..64u64)
            .map(|i| r.allocate(i * 10).unwrap()).collect();
        let r_a = r.clone();
        let _init = initial.clone();
        // Worker A: free even-indexed slots.
        let freer = thread::spawn(move || {
            for (i, ptr) in _init.iter().enumerate() {
                if i % 2 == 0 { r_a.free(*ptr).ok(); }
            }
        });
        // Worker B: allocate new slots.
        let r_b = r.clone();
        let alloc = thread::spawn(move || {
            let mut new_ptrs = vec![];
            for i in 100u64..132 {
                if let Ok(p) = r_b.allocate(i) { new_ptrs.push((i, p)); }
            }
            new_ptrs
        });
        freer.join().unwrap();
        let new_ptrs = alloc.join().unwrap();
        // Each new ptr resolves to its value.
        for (val, ptr) in new_ptrs {
            assert_eq!(r.get(ptr).unwrap(), val);
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn offset_ptr_is_position_independent() {
        // The whole point: the same index resolves to the same value
        // in two independently-mapped views of the same file.
        let p = tmp("position-indep");
        let writer: SharedRegion<u64> = SharedRegion::create(&p, 4).unwrap();
        let ptr = writer.allocate(0xCAFE_BABE).unwrap();
        let reader: SharedRegion<u64> = SharedRegion::open(&p, 4).unwrap();
        // Verify the ptr's raw index field (u32) is the same in both.
        let same_ptr = OffsetPtr::<u64>::new(ptr.index);
        assert_eq!(reader.get(same_ptr).unwrap(), 0xCAFE_BABE);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Node { left: u32, right: u32, key: u64 }
        let p = tmp("struct");
        let r: SharedRegion<Node> = SharedRegion::create(&p, 16).unwrap();
        let n = Node { left: 1, right: 2, key: 42 };
        let ptr = r.allocate(n).unwrap();
        assert_eq!(r.get(ptr).unwrap(), n);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn nil_ptr_round_trips() {
        let p: OffsetPtr<u64> = OffsetPtr::NIL;
        assert!(p.is_nil());
        assert_eq!(p.index, NIL_INDEX);
    }

    #[test]
    fn offset_ptr_equality_and_hash() {
        use std::collections::HashSet;
        let a: OffsetPtr<u64> = OffsetPtr::new(5);
        let b: OffsetPtr<u64> = OffsetPtr::new(5);
        let c: OffsetPtr<u64> = OffsetPtr::new(6);
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut s = HashSet::new();
        s.insert(a);
        assert!(s.contains(&b));
        assert!(!s.contains(&c));
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        let saved_ptr_index;
        {
            let r: SharedRegion<u64> = SharedRegion::create(&p, 16).unwrap();
            let p1 = r.allocate(1111).unwrap();
            let _p2 = r.allocate(2222).unwrap();
            r.flush().unwrap();
            saved_ptr_index = p1.index;
        }
        let r2: SharedRegion<u64> = SharedRegion::open(&p, 16).unwrap();
        let restored = OffsetPtr::<u64>::new(saved_ptr_index);
        assert_eq!(r2.get(restored).unwrap(), 1111);
        // Continue allocating.
        let p3 = r2.allocate(3333).unwrap();
        assert_eq!(p3.index, 2);
        assert_eq!(r2.get(p3).unwrap(), 3333);
        std::fs::remove_file(&p).ok();
    }
}
