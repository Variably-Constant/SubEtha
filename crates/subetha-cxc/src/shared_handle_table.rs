//! `SharedHandleTable<T>` - cross-process ECS-style slotmap.
//!
//! Same architectural shape as the in-process `AdaptiveHandle` /
//! `Slotmap`, but the slot table lives in a memory-mapped file so
//! handles are valid across processes. Handle = `(generation: u32,
//! slot: u32)` packed into a `u64`; generation bumps on every
//! insert and every remove so stale handles fail visibility check
//! across the process boundary.
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | HandleHeader (64B)          |
//! |   - magic                   |
//! |   - capacity                |
//! |   - slot_size               |
//! |   - free_list_head_packed   |  (counter:u32, slot:u32) packed u64
//! |   - live_count              |
//! +-----------------------------+
//! | Slot[0] (64B cache line)    |
//! |   - generation: AtomicU32   |
//! |   - occupied:   AtomicU32   |
//! |   - next_free:  AtomicU32   |
//! |   - _pad:       u32         |
//! |   - payload:    [u8; 48]    |
//! +-----------------------------+
//! | Slot[1] ...                 |
//! +-----------------------------+
//! ```
//!
//! # Generation parity
//!
//! Even generation = vacant, odd = occupied. Bumped on every
//! insert (vacant -> occupied) and every remove (occupied ->
//! vacant). A handle from generation N matches only when the slot
//! is currently at generation N.
//!
//! # Free list
//!
//! ABA-free Treiber stack. Head is packed `(counter, slot_idx)` so
//! every CAS bumps the counter, making the (head, next) sequence
//! distinguishable from the (head, _, next-after-reinsert) sequence
//! that would otherwise alias. Each vacant slot's `next_free` field
//! is the linkage.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const HANDLE_TABLE_MAGIC: u64 = 0x4150_4D46_5354_424C;

pub const SLOT_PAYLOAD_BYTES: usize = 48;

/// Sentinel for "no slot" in the free list.
pub const NIL_SLOT: u32 = u32::MAX;

/// Generation 0 is reserved for `Handle::NULL`.
const GEN_VACANT_BIT: u32 = 0;

#[repr(C, align(64))]
pub struct HandleHeader {
    pub magic: u64,
    pub capacity: u32,
    pub slot_size: u32,
    pub free_list_head: AtomicU64,
    pub live_count: AtomicU64,
    _pad: [u8; 32],
}

#[repr(C, align(64))]
pub struct SharedSlot {
    pub generation: AtomicU32,
    pub occupied: AtomicU32,
    pub next_free: AtomicU32,
    _pad: u32,
    pub payload: [u8; SLOT_PAYLOAD_BYTES],
}

/// Opaque cross-process handle. Packed `(generation: u32 high, slot: u32 low)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Handle(u64);

impl Handle {
    pub const NULL: Self = Self(0);

    #[inline]
    pub const fn from_parts(generation: u32, slot: u32) -> Self {
        Self(((generation as u64) << 32) | (slot as u64))
    }

    #[inline]
    pub const fn generation(self) -> u32 { (self.0 >> 32) as u32 }

    #[inline]
    pub const fn slot(self) -> u32 { self.0 as u32 }

    #[inline]
    pub const fn is_null(self) -> bool { self.0 == 0 }

    #[inline]
    pub const fn raw(self) -> u64 { self.0 }
}

pub const fn slot_offset() -> usize { size_of::<HandleHeader>() }

pub const fn handle_table_file_size(capacity: usize) -> usize {
    slot_offset() + capacity * size_of::<SharedSlot>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleTableError {
    LayoutMismatch,
    PayloadTooLarge,
    Full,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for HandleTableError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedHandleTable<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedHandleTable<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedHandleTable<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedHandleTable<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

#[inline]
fn pack_head(counter: u32, slot: u32) -> u64 {
    ((counter as u64) << 32) | (slot as u64)
}

#[inline]
fn unpack_head(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

impl<T: Copy + 'static> SharedHandleTable<T> {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, HandleTableError> {
        Self::check_layout()?;
        assert!(capacity >= 1 && capacity <= (u32::MAX - 1) as usize);
        let total = handle_table_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut HandleHeader;
        // Initial free list: link all slots in order [0 -> 1 -> 2 -> ... -> NIL].
        unsafe {
            std::ptr::write(hdr, HandleHeader {
                magic: HANDLE_TABLE_MAGIC,
                capacity: capacity as u32,
                slot_size: size_of::<T>() as u32,
                free_list_head: AtomicU64::new(pack_head(0, 0)),
                live_count: AtomicU64::new(0),
                _pad: [0; 32],
            });
        }
        let slots_base = unsafe { mmap.as_mut_ptr().add(slot_offset()) };
        for i in 0..capacity {
            let slot_ptr = unsafe { slots_base.add(i * size_of::<SharedSlot>()) as *mut SharedSlot };
            let next = if i + 1 < capacity { (i + 1) as u32 } else { NIL_SLOT };
            unsafe {
                std::ptr::write(slot_ptr, SharedSlot {
                    generation: AtomicU32::new(GEN_VACANT_BIT),
                    occupied: AtomicU32::new(0),
                    next_free: AtomicU32::new(next),
                    _pad: 0,
                    payload: [0; SLOT_PAYLOAD_BYTES],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, HandleTableError> {
        Self::check_layout()?;
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = handle_table_file_size(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(HandleTableError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const HandleHeader) };
        if hdr.magic != HANDLE_TABLE_MAGIC
            || hdr.capacity != expected_capacity as u32
            || hdr.slot_size as usize != size_of::<T>()
        {
            return Err(HandleTableError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn check_layout() -> Result<(), HandleTableError> {
        if size_of::<T>() > SLOT_PAYLOAD_BYTES {
            return Err(HandleTableError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(HandleTableError::PayloadTooLarge);
        }
        Ok(())
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    pub fn header(&self) -> &HandleHeader {
        unsafe { &*(self.mmap.as_ptr() as *const HandleHeader) }
    }

    #[inline]
    fn slot(&self, idx: u32) -> &SharedSlot {
        debug_assert!((idx as usize) < self.capacity);
        let base = unsafe { self.mmap.as_ptr().add(slot_offset()) };
        unsafe { &*(base.add((idx as usize) * size_of::<SharedSlot>()) as *const SharedSlot) }
    }

    /// Pop the head of the free list. ABA-free via the packed counter.
    /// Returns `None` when the table is full.
    fn pop_free(&self) -> Option<u32> {
        let header = self.header();
        loop {
            let head = header.free_list_head.load(Ordering::Acquire);
            let (cnt, idx) = unpack_head(head);
            if idx == NIL_SLOT { return None; }
            let next = self.slot(idx).next_free.load(Ordering::Acquire);
            let new_head = pack_head(cnt.wrapping_add(1), next);
            if header.free_list_head.compare_exchange_weak(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Some(idx);
            }
            std::hint::spin_loop();
        }
    }

    /// Push `slot_idx` onto the free list. Updates its `next_free` first.
    fn push_free(&self, slot_idx: u32) {
        let header = self.header();
        loop {
            let head = header.free_list_head.load(Ordering::Acquire);
            let (cnt, head_idx) = unpack_head(head);
            self.slot(slot_idx).next_free.store(head_idx, Ordering::Release);
            let new_head = pack_head(cnt.wrapping_add(1), slot_idx);
            if header.free_list_head.compare_exchange_weak(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Insert `value`, returning a Handle. Errors with `Full` when
    /// the slot table is exhausted.
    pub fn insert(&self, value: T) -> Result<Handle, HandleTableError> {
        let slot_idx = match self.pop_free() {
            Some(i) => i,
            None => {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_ACQUIRE, 1);
                return Err(HandleTableError::Full);
            }
        };
        let slot = self.slot(slot_idx);
        // Bump generation: even (vacant) -> odd (occupied).
        let new_gen = slot.generation.fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1)
            .max(1);  // Skip the reserved gen=0 sentinel.
        // SAFETY: this slot is exclusively ours (we just popped it
        // from the free list) until we set occupied=1 below.
        unsafe {
            let dst = slot.payload.as_ptr() as *mut T;
            std::ptr::write_unaligned(dst, value);
        }
        slot.occupied.store(1, Ordering::Release);
        self.header().live_count.fetch_add(1, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ownership::OP_ACQUIRE, 0);
        Ok(Handle::from_parts(new_gen, slot_idx))
    }

    /// Look up by handle. Returns `None` when the handle is stale
    /// (generation mismatch) or the slot is currently vacant.
    pub fn get(&self, h: Handle) -> Option<T> {
        let r = self.get_inner(h);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ownership::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn get_inner(&self, h: Handle) -> Option<T> {
        if h.is_null() { return None; }
        let slot_idx = h.slot();
        if (slot_idx as usize) >= self.capacity { return None; }
        let slot = self.slot(slot_idx);
        // Re-check generation AFTER reading payload to catch
        // mid-read modification by a remover.
        loop {
            let gen1 = slot.generation.load(Ordering::Acquire);
            if gen1 != h.generation() { return None; }
            if slot.occupied.load(Ordering::Acquire) == 0 { return None; }
            let value: T = unsafe {
                let src = slot.payload.as_ptr() as *const T;
                std::ptr::read_unaligned(src)
            };
            let gen2 = slot.generation.load(Ordering::Acquire);
            if gen1 == gen2 {
                return Some(value);
            }
            // Concurrent remove + reinsert; retry.
            std::hint::spin_loop();
        }
    }

    /// True when handle is currently live.
    pub fn contains(&self, h: Handle) -> bool {
        if h.is_null() { return false; }
        let slot_idx = h.slot();
        if (slot_idx as usize) >= self.capacity { return false; }
        let slot = self.slot(slot_idx);
        slot.generation.load(Ordering::Acquire) == h.generation()
            && slot.occupied.load(Ordering::Acquire) == 1
    }

    /// Remove the value at handle. Returns the value if live,
    /// `None` if stale or already removed.
    pub fn remove(&self, h: Handle) -> Option<T> {
        let r = self.remove_inner(h);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ownership::OP_RELEASE,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn remove_inner(&self, h: Handle) -> Option<T> {
        if h.is_null() { return None; }
        let slot_idx = h.slot();
        if (slot_idx as usize) >= self.capacity { return None; }
        let slot = self.slot(slot_idx);
        // Atomic CAS occupied 1 -> 0; loses to concurrent remove.
        if slot.occupied.compare_exchange(
            1, 0, Ordering::AcqRel, Ordering::Acquire,
        ).is_err() {
            return None;
        }
        // Generation check: when generation already differs, our
        // remove was on a slot that's already been reused. Restore
        // occupied (the other thread should have set it).
        let cur_gen = slot.generation.load(Ordering::Acquire);
        if cur_gen != h.generation() {
            // Roll back: somebody else owns the slot.
            slot.occupied.store(1, Ordering::Release);
            return None;
        }
        let value: T = unsafe {
            let src = slot.payload.as_ptr() as *const T;
            std::ptr::read_unaligned(src)
        };
        // Bump generation to mark slot vacant again (odd -> even).
        slot.generation.fetch_add(1, Ordering::AcqRel);
        self.header().live_count.fetch_sub(1, Ordering::AcqRel);
        // Return to free list.
        self.push_free(slot_idx);
        Some(value)
    }

    pub fn len(&self) -> usize {
        self.header().live_count.load(Ordering::Acquire) as usize
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), HandleTableError> {
        self.mmap.flush_async()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), HandleTableError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-handle-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn insert_get_remove_round_trip() {
        let p = tmp("rt");
        let t: SharedHandleTable<u64> = SharedHandleTable::create(&p, 16).unwrap();
        let h = t.insert(42).unwrap();
        assert!(t.contains(h));
        assert_eq!(t.get(h), Some(42));
        assert_eq!(t.len(), 1);
        let v = t.remove(h);
        assert_eq!(v, Some(42));
        assert!(!t.contains(h));
        assert_eq!(t.get(h), None);
        assert_eq!(t.len(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn stale_handle_after_remove_returns_none() {
        let p = tmp("stale");
        let t: SharedHandleTable<u64> = SharedHandleTable::create(&p, 4).unwrap();
        let h1 = t.insert(100).unwrap();
        assert_eq!(t.remove(h1), Some(100));
        // h1 now stale; reinsert into the same slot.
        let h2 = t.insert(200).unwrap();
        assert_eq!(h2.slot(), h1.slot(), "free-list LIFO reused the slot");
        assert_ne!(h2.generation(), h1.generation(), "generation differs");
        assert_eq!(t.get(h1), None, "stale h1 must not see h2's value");
        assert_eq!(t.get(h2), Some(200));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_table_insert_returns_error() {
        let p = tmp("full");
        let t: SharedHandleTable<u64> = SharedHandleTable::create(&p, 2).unwrap();
        let _val = t.insert(1).unwrap();
        let _val = t.insert(2).unwrap();
        assert_eq!(t.insert(3), Err(HandleTableError::Full));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedHandleTable<u64> = SharedHandleTable::create(&p, 16).unwrap();
        let reader: SharedHandleTable<u64> = SharedHandleTable::open(&p, 16).unwrap();
        let h = writer.insert(777).unwrap();
        assert_eq!(reader.get(h), Some(777));
        assert!(reader.contains(h));
        // Confirm remove returns the just-inserted value.
        assert_eq!(writer.remove(h), Some(777));
        assert_eq!(reader.get(h), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_inserts_and_removes_preserve_count() {
        let p = tmp("concurrent");
        let t: Arc<SharedHandleTable<u64>>
            = Arc::new(SharedHandleTable::create(&p, 256).unwrap());
        let n_threads = 4usize;
        let per_thread = 50usize;
        let mut handles = vec![];
        for tid in 0..n_threads {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                let mut owned = Vec::with_capacity(per_thread);
                for i in 0..per_thread {
                    let v = (tid * per_thread + i) as u64;
                    loop {
                        match t.insert(v) {
                            Ok(h) => { owned.push((h, v)); break; }
                            Err(HandleTableError::Full) => {
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected error: {e:?}"),
                        }
                    }
                }
                // Verify everything we own is still ours.
                for &(h, v) in &owned {
                    assert_eq!(t.get(h), Some(v));
                }
                // Remove everything.
                for (h, v) in owned {
                    assert_eq!(t.remove(h), Some(v));
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(t.len(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        let h1;
        let h2;
        {
            let t: SharedHandleTable<u64> = SharedHandleTable::create(&p, 8).unwrap();
            h1 = t.insert(11).unwrap();
            h2 = t.insert(22).unwrap();
            t.flush().unwrap();
        }
        let t2: SharedHandleTable<u64> = SharedHandleTable::open(&p, 8).unwrap();
        assert_eq!(t2.get(h1), Some(11));
        assert_eq!(t2.get(h2), Some(22));
        assert_eq!(t2.len(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn null_handle_returns_none() {
        let p = tmp("null-handle");
        let t: SharedHandleTable<u64> = SharedHandleTable::create(&p, 4).unwrap();
        assert_eq!(t.get(Handle::NULL), None);
        assert!(!t.contains(Handle::NULL));
        assert_eq!(t.remove(Handle::NULL), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn handle_packing_round_trips() {
        let h = Handle::from_parts(42, 1337);
        assert_eq!(h.generation(), 42);
        assert_eq!(h.slot(), 1337);
        assert!(!h.is_null());
        assert!(Handle::NULL.is_null());
        assert_eq!(Handle::NULL.generation(), 0);
        assert_eq!(Handle::NULL.slot(), 0);
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Item { id: u32, weight: f32, flags: u32 }
        let p = tmp("struct");
        let t: SharedHandleTable<Item> = SharedHandleTable::create(&p, 8).unwrap();
        let item = Item { id: 42, weight: 1.5, flags: 0xFF };
        let h = t.insert(item).unwrap();
        assert_eq!(t.get(h), Some(item));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // size_of<Big> is the test signal, not the field
        struct Big([u8; SLOT_PAYLOAD_BYTES + 1]);
        impl Copy for Big {}
        impl Clone for Big { fn clone(&self) -> Self { *self } }
        let p = tmp("too-large");
        match SharedHandleTable::<Big>::create(&p, 4) {
            Err(HandleTableError::PayloadTooLarge) => {}
            other => panic!("expected PayloadTooLarge, got {:?}", other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }
}
