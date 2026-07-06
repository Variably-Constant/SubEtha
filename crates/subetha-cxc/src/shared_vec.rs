//! `SharedVec<T>` - cross-process bounded indexable sequence.
//!
//! Distinct from [`SharedRing`](crate::SharedRing) (FIFO; drain
//! semantics): SharedVec is RANDOM-ACCESS, accumulates monotonically
//! up to capacity, and supports `get(i)` for any prior index.
//!
//! # Layout
//!
//! Single MMF file:
//!
//! ```text
//! +---------------------------+
//! | VecHeader  (64B aligned)  |  magic, capacity, len, slot_size
//! +---------------------------+
//! | Slot[0]    (64B = cache)  |  version + payload[VEC_PAYLOAD_BYTES]
//! | Slot[1]                   |
//! | ...                       |
//! | Slot[capacity - 1]        |
//! +---------------------------+
//! ```
//!
//! Each slot is its own SeqLock cell (same shape as SharedCell);
//! per-slot writes never false-share because each is its own cache
//! line.
//!
//! # Concurrency
//!
//! - `push_back`: atomic `len.fetch_add(1)` claims a slot index; if
//!   it exceeds capacity, rollback with `fetch_sub(1)` and return
//!   `Full`. On success, write the payload under the slot's SeqLock
//!   (version bump odd → write → bump even).
//! - `get(i)`: load `len` (Acquire). If `i >= len`, return None.
//!   Otherwise SeqLock-read `slot[i]`: spin if version is odd
//!   (writer in progress), reread on version change.
//! - `pop_back`: `compare_exchange` on `len` to decrement; if
//!   successful, read the now-popped slot's payload at the old
//!   index. The slot bytes remain in place but are no longer
//!   addressable via `len`-bounded access.
//! - `set(i, v)`: bounds-check against `len`, then SeqLock-write.
//! - `clear`: store `len = 0` (Release). Previously-pushed slot
//!   payloads remain on disk but become unreachable through the
//!   bounded indexing.
//!
//! # Capacity
//!
//! Fixed at create time. The MMF is pre-allocated to the full size;
//! no resize-on-grow protocol. The unbounded variant (with
//! coordinator-mediated MMF resize) is a separate primitive.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const VEC_MAGIC: u32 = 0x4150_5656;
pub const VEC_PAYLOAD_BYTES: usize = 52;

#[repr(C, align(64))]
pub struct VecHeader {
    pub magic: u32,
    pub slot_payload_size: u32,
    pub capacity: u64,
    pub len: AtomicU64,
    _pad: [u8; 40],
}

#[repr(C, align(64))]
pub struct VecSlot {
    pub version: AtomicU32,
    _pad: [u8; 4],
    pub payload: [u8; VEC_PAYLOAD_BYTES],
}

const _: () = {
    assert!(size_of::<VecHeader>() == 64);
    assert!(size_of::<VecSlot>() == 64);
};

pub const fn vec_file_size(capacity: usize) -> usize {
    size_of::<VecHeader>() + capacity * size_of::<VecSlot>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecError {
    Full,
    OutOfBounds,
    LayoutMismatch,
    PayloadTooLarge,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for VecError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedVec<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedVec<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedVec<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedVec<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedVec<T> {
    pub fn create(
        path: impl AsRef<Path>, capacity: usize,
    ) -> Result<Self, VecError> {
        if size_of::<T>() > VEC_PAYLOAD_BYTES {
            return Err(VecError::PayloadTooLarge);
        }
        assert!(capacity >= 1);
        let total = vec_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut VecHeader;
        unsafe {
            std::ptr::write(hdr, VecHeader {
                magic: VEC_MAGIC,
                slot_payload_size: VEC_PAYLOAD_BYTES as u32,
                capacity: capacity as u64,
                len: AtomicU64::new(0),
                _pad: [0; 40],
            });
        }
        for i in 0..capacity {
            let slot_ptr = unsafe {
                mmap.as_mut_ptr()
                    .add(size_of::<VecHeader>())
                    .add(i * size_of::<VecSlot>())
            } as *mut VecSlot;
            unsafe {
                std::ptr::write(slot_ptr, VecSlot {
                    version: AtomicU32::new(0),
                    _pad: [0; 4],
                    payload: [0u8; VEC_PAYLOAD_BYTES],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_capacity: usize,
    ) -> Result<Self, VecError> {
        if size_of::<T>() > VEC_PAYLOAD_BYTES {
            return Err(VecError::PayloadTooLarge);
        }
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = vec_file_size(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(VecError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const VecHeader) };
        if hdr.magic != VEC_MAGIC || hdr.capacity != expected_capacity as u64 {
            return Err(VecError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    pub fn len(&self) -> usize {
        self.header().len.load(Ordering::Acquire) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    fn header(&self) -> &VecHeader {
        unsafe { &*(self.mmap.as_ptr() as *const VecHeader) }
    }

    fn slot(&self, i: usize) -> &VecSlot {
        assert!(i < self.capacity, "slot index {i} out of bounds for cap {}", self.capacity);
        let base = unsafe { self.mmap.as_ptr().add(size_of::<VecHeader>()) };
        unsafe { &*(base.add(i * size_of::<VecSlot>()) as *const VecSlot) }
    }

    /// SeqLock write of a payload into a slot. Caller is responsible
    /// for ensuring `i < capacity`.
    fn write_slot(&self, i: usize, v: T) {
        let slot = self.slot(i);
        // Bump version odd; subsequent readers will spin.
        slot.version.fetch_add(1, Ordering::AcqRel);
        // Memcpy the value.
        let dst = unsafe {
            let base = self.mmap.as_ptr().add(size_of::<VecHeader>())
                .add(i * size_of::<VecSlot>())
                .add(std::mem::offset_of!(VecSlot, payload));
            base as *mut u8
        };
        unsafe {
            std::ptr::copy_nonoverlapping(
                &v as *const T as *const u8,
                dst,
                size_of::<T>(),
            );
        }
        // Bump version even; readers resume.
        slot.version.fetch_add(1, Ordering::AcqRel);
    }

    /// SeqLock read of a slot. Spins if version is odd; rereads on
    /// version change.
    fn read_slot(&self, i: usize) -> T {
        let slot = self.slot(i);
        loop {
            let v1 = slot.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let mut out = std::mem::MaybeUninit::<T>::uninit();
            let src = unsafe {
                self.mmap.as_ptr().add(size_of::<VecHeader>())
                    .add(i * size_of::<VecSlot>())
                    .add(std::mem::offset_of!(VecSlot, payload))
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src, out.as_mut_ptr() as *mut u8, size_of::<T>(),
                );
            }
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                return unsafe { out.assume_init() };
            }
        }
    }

    /// Append a value. Returns the index it landed at.
    /// Returns `Err(Full)` when the vec is at capacity.
    pub fn push_back(&self, v: T) -> Result<usize, VecError> {
        let idx = self.header().len.fetch_add(1, Ordering::AcqRel) as usize;
        if idx >= self.capacity {
            self.header().len.fetch_sub(1, Ordering::AcqRel);
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ordered::OP_INSERT, 1); // full
            return Err(VecError::Full);
        }
        self.write_slot(idx, v);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_INSERT, 0);
        Ok(idx)
    }

    /// Remove and return the last element. Returns None when empty.
    pub fn pop_back(&self) -> Option<T> {
        loop {
            let cur = self.header().len.load(Ordering::Acquire);
            if cur == 0 {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ordered::OP_POP, 2); // empty
                return None;
            }
            let new = cur - 1;
            if self.header().len.compare_exchange(
                cur, new, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                let v = self.read_slot(new as usize);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ordered::OP_POP, 0);
                return Some(v);
            }
        }
    }

    /// Read the value at index `i`. Returns None when `i >= len`.
    pub fn get(&self, i: usize) -> Option<T> {
        if i >= self.len() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ordered::OP_GET, 2); // out of bounds / absent
            return None;
        }
        let v = self.read_slot(i);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_GET, 0);
        Some(v)
    }

    /// Overwrite the value at index `i`. Returns `Err(OutOfBounds)`
    /// when `i >= len`.
    pub fn set(&self, i: usize, v: T) -> Result<(), VecError> {
        if i >= self.len() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ordered::OP_INSERT, 1); // out of bounds (positional write rejected)
            return Err(VecError::OutOfBounds);
        }
        self.write_slot(i, v);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_INSERT, 0);
        Ok(())
    }

    /// Clear the vec by resetting len to 0. Slot payloads are not
    /// zeroed; they become unreachable through bounded indexing.
    pub fn clear(&self) {
        self.header().len.store(0, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_REMOVE, 0);
    }

    /// Snapshot all current values into a Vec. Best-effort: under
    /// concurrent writers, the snapshot is a consistent prefix at
    /// the moment of the `len` load, with each slot read under its
    /// own SeqLock.
    pub fn snapshot(&self) -> Vec<T> {
        let n = self.len();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(self.read_slot(i));
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_ITER, 0);
        out
    }

    pub fn flush(&self) -> Result<(), VecError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), VecError> {
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
        p.push(format!("subetha-vec-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let v: SharedVec<u32> = SharedVec::create(&p, 16).unwrap();
        assert_eq!(v.capacity(), 16);
        assert_eq!(v.len(), 0);
        assert!(v.is_empty());
        assert_eq!(v.get(0), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn push_back_advances_len_and_get_round_trip() {
        let p = tmp("push");
        let v: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
        for i in 0..5u32 {
            let idx = v.push_back(i * 10).unwrap();
            assert_eq!(idx, i as usize);
        }
        assert_eq!(v.len(), 5);
        for i in 0..5 {
            assert_eq!(v.get(i), Some((i as u32) * 10));
        }
        assert_eq!(v.get(5), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_capacity_returns_error() {
        let p = tmp("full");
        let v: SharedVec<u32> = SharedVec::create(&p, 4).unwrap();
        for i in 0..4u32 { v.push_back(i).unwrap(); }
        assert_eq!(v.push_back(99).err(), Some(VecError::Full));
        assert_eq!(v.len(), 4);  // rolled back
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn pop_back_returns_last_then_none() {
        let p = tmp("pop");
        let v: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
        v.push_back(10).unwrap();
        v.push_back(20).unwrap();
        v.push_back(30).unwrap();
        assert_eq!(v.pop_back(), Some(30));
        assert_eq!(v.pop_back(), Some(20));
        assert_eq!(v.len(), 1);
        assert_eq!(v.pop_back(), Some(10));
        assert_eq!(v.pop_back(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_replaces_value_at_index() {
        let p = tmp("set");
        let v: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
        v.push_back(1).unwrap();
        v.push_back(2).unwrap();
        v.set(0, 100).unwrap();
        assert_eq!(v.get(0), Some(100));
        assert_eq!(v.get(1), Some(2));
        assert_eq!(v.set(2, 200).err(), Some(VecError::OutOfBounds));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn clear_resets_len_to_zero() {
        let p = tmp("clear");
        let v: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
        for i in 0..5u32 { v.push_back(i).unwrap(); }
        assert_eq!(v.len(), 5);
        v.clear();
        assert_eq!(v.len(), 0);
        assert_eq!(v.get(0), None);
        // After clear, push works fresh.
        v.push_back(42).unwrap();
        assert_eq!(v.get(0), Some(42));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn snapshot_returns_consistent_prefix() {
        let p = tmp("snapshot");
        let v: SharedVec<u32> = SharedVec::create(&p, 16).unwrap();
        for i in 0..7u32 { v.push_back(i + 100).unwrap(); }
        let snap = v.snapshot();
        assert_eq!(snap, vec![100, 101, 102, 103, 104, 105, 106]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
        let reader: SharedVec<u32> = SharedVec::open(&p, 8).unwrap();
        writer.push_back(777).unwrap();
        assert_eq!(reader.get(0), Some(777));
        reader.push_back(888).unwrap();
        assert_eq!(writer.get(1), Some(888));
        assert_eq!(writer.len(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_pushers_all_land_at_distinct_indices() {
        let p = tmp("concurrent");
        let v: Arc<SharedVec<u32>> = Arc::new(SharedVec::create(&p, 1024).unwrap());
        let n_threads = 4;
        let per_thread = 50u32;
        let mut handles = vec![];
        for t in 0..n_threads {
            let v = v.clone();
            handles.push(thread::spawn(move || {
                let mut indices = vec![];
                for i in 0..per_thread {
                    let val = (t as u32) * per_thread + i;
                    let idx = v.push_back(val).unwrap();
                    indices.push(idx);
                }
                indices
            }));
        }
        let mut all_indices: Vec<usize> = handles.into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all_indices.sort();
        for (expected, actual) in all_indices.iter().enumerate() {
            assert_eq!(*actual, expected,
                "indices must form a contiguous 0..N sequence");
        }
        assert_eq!(v.len(), (n_threads * per_thread as usize));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // size_of<Big> is the test signal, not the field
        struct Big([u8; VEC_PAYLOAD_BYTES + 1]);
        impl Copy for Big {}
        impl Clone for Big { fn clone(&self) -> Self { *self } }
        let p = tmp("too-large");
        let r = SharedVec::<Big>::create(&p, 4);
        assert_eq!(r.err(), Some(VecError::PayloadTooLarge));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Point { x: f64, y: f64, z: f64 }
        let p = tmp("struct");
        let v: SharedVec<Point> = SharedVec::create(&p, 8).unwrap();
        v.push_back(Point { x: 1.0, y: 2.0, z: 3.0 }).unwrap();
        v.push_back(Point { x: -1.5, y: 0.0, z: 7.25 }).unwrap();
        assert_eq!(v.get(0), Some(Point { x: 1.0, y: 2.0, z: 3.0 }));
        assert_eq!(v.get(1), Some(Point { x: -1.5, y: 0.0, z: 7.25 }));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_data_survives_reopen() {
        let p = tmp("disk");
        {
            let v: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
            for i in 0..4u32 { v.push_back(i * 100).unwrap(); }
            v.flush().unwrap();
        }
        let v2: SharedVec<u32> = SharedVec::open(&p, 8).unwrap();
        assert_eq!(v2.len(), 4);
        for i in 0..4 {
            assert_eq!(v2.get(i), Some((i as u32) * 100));
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_reader_during_writes_sees_consistent_data() {
        let p = tmp("read-during-write");
        let v: Arc<SharedVec<u32>> = Arc::new(SharedVec::create(&p, 256).unwrap());
        let v_w = v.clone();
        let writer = thread::spawn(move || {
            for i in 0..100u32 {
                v_w.push_back(i).unwrap();
            }
        });
        let v_r = v.clone();
        let reader = thread::spawn(move || {
            let mut last_len = 0;
            loop {
                let n = v_r.len();
                if n == 100 { break; }
                // Read every visible slot; values must equal index.
                for i in last_len..n {
                    let got = v_r.get(i);
                    assert_eq!(got, Some(i as u32),
                        "slot {i} should hold {i}, got {got:?}");
                }
                last_len = n;
                std::thread::yield_now();
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
        std::fs::remove_file(&p).ok();
    }
}
