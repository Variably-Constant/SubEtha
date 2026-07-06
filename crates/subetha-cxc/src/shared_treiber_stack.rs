//! `SharedTreiberStack<T>` - cross-process lock-free LIFO stack.
//!
//! Classic Treiber-stack pattern: a packed `(counter, head_index)`
//! atomic head, ABA-safe via the counter wrap. Push and pop are
//! CAS loops bounded only by contention rate (not by logical
//! waiting conditions).
//!
//! # Companion to other queue primitives
//!
//! - [`SharedRing`](crate::SharedRing): MPMC FIFO with fixed slot ordering
//! - [`SharedBroadcastRing`](crate::SharedBroadcastRing): 1P+NC pub/sub
//! - [`SharedTreiberStack`]: MPMC LIFO (this one)
//!
//! # Safety properties
//!
//! - **Bounded capacity** at create time; push returns `Err(Full)`
//!   when capacity is exhausted.
//! - **ABA-safe** via 32-bit counter packed with index in the head
//!   atomic; same proven design as [`SharedRegion`](crate::SharedRegion)'s
//!   free list.
//! - **CAS loops are contention-bounded**, not logical-condition
//!   bounded. Each retry happens because another writer won the
//!   race; eventually contention resolves.
//! - **No RAII guards** with Drop semantics that risk being
//!   aliased or double-released. Push and pop return owned values.
//! - **No underflow**: pop returns `None` on empty rather than
//!   wrapping a counter.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | StackHeader (64B)         |
//! |   magic, capacity         |
//! |   head: AtomicU64         |  // (counter << 32) | top_index, NIL when empty
//! |   free_head: AtomicU64    |  // free-list of returned slots
//! |   bump_next: AtomicU32    |
//! +---------------------------+
//! | next[capacity: AtomicU32] |  // chain pointers (overlap usage as free + occupied)
//! +---------------------------+
//! | slots[capacity * size_of<T>] |
//! +---------------------------+
//! ```

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const STACK_MAGIC: u32 = 0x4150_5354;
pub const STACK_NIL: u32 = u32::MAX;

#[repr(C, align(64))]
pub struct StackHeader {
    pub magic: u32,
    pub capacity: u32,
    pub slot_size: u32,
    _pad1: u32,
    pub head: AtomicU64,       // (counter << 32) | top_index (NIL when empty)
    pub free_head: AtomicU64,  // free-list of returned slots
    pub bump_next: AtomicU32,
    _pad2: [u8; 28],
}

const _: () = {
    assert!(size_of::<StackHeader>() == 64);
};

pub fn stack_file_size(capacity: usize, slot_size: usize) -> usize {
    size_of::<StackHeader>()
        + capacity * size_of::<AtomicU32>()
        + capacity * slot_size
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackError {
    Full,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for StackError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

#[inline]
fn pack(counter: u32, index: u32) -> u64 {
    ((counter as u64) << 32) | (index as u64)
}
#[inline]
fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

pub struct SharedTreiberStack<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    next_offset: usize,
    slots_offset: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedTreiberStack<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedTreiberStack<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedTreiberStack<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedTreiberStack<T> {
    pub fn create(
        path: impl AsRef<Path>, capacity: usize,
    ) -> Result<Self, StackError> {
        assert!(capacity >= 1);
        assert!(capacity < STACK_NIL as usize, "capacity must be < u32::MAX");
        let slot_size = size_of::<T>();
        let total = stack_file_size(capacity, slot_size);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut StackHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<StackHeader>());
            (*hdr).magic = STACK_MAGIC;
            (*hdr).capacity = capacity as u32;
            (*hdr).slot_size = slot_size as u32;
            (*hdr).head.store(pack(0, STACK_NIL), Ordering::Release);
            (*hdr).free_head.store(pack(0, STACK_NIL), Ordering::Release);
            (*hdr).bump_next.store(0, Ordering::Release);
        }
        let next_offset = size_of::<StackHeader>();
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
    ) -> Result<Self, StackError> {
        let slot_size = size_of::<T>();
        let total = stack_file_size(expected_capacity, slot_size);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(StackError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const StackHeader) };
        if hdr.magic != STACK_MAGIC
            || hdr.capacity != expected_capacity as u32
            || hdr.slot_size != slot_size as u32
        {
            return Err(StackError::LayoutMismatch);
        }
        let next_offset = size_of::<StackHeader>();
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

    fn header(&self) -> &StackHeader {
        unsafe { &*(self.mmap.as_ptr() as *const StackHeader) }
    }

    fn next_link(&self, idx: usize) -> &AtomicU32 {
        let base = unsafe { self.mmap.as_ptr().add(self.next_offset) };
        unsafe { &*(base.add(idx * size_of::<AtomicU32>()) as *const AtomicU32) }
    }

    fn slot_ptr(&self, idx: usize) -> *mut T {
        let base = unsafe { self.mmap.as_ptr().add(self.slots_offset) };
        unsafe { base.add(idx * size_of::<T>()) as *mut T }
    }

    /// Acquire a slot index via the free list, falling back to bump
    /// alloc. Returns `Err(Full)` when capacity is exhausted.
    fn acquire_slot(&self) -> Result<u32, StackError> {
        // Try free-list pop.
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, idx) = unpack(head);
            if idx == STACK_NIL { break; }
            let next_idx = self.next_link(idx as usize).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next_idx);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Ok(idx);
            }
        }
        // Bump allocation.
        let idx = self.header().bump_next.fetch_add(1, Ordering::AcqRel);
        if (idx as usize) >= self.capacity {
            self.header().bump_next.fetch_sub(1, Ordering::AcqRel);
            return Err(StackError::Full);
        }
        Ok(idx)
    }

    /// Return a slot to the free list (Treiber push onto free_head).
    fn release_slot(&self, idx: u32) {
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(idx as usize).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), idx);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return;
            }
        }
    }

    /// Push a value onto the stack.
    pub fn push(&self, value: T) -> Result<(), StackError> {
        let idx = match self.acquire_slot() {
            Ok(i) => i,
            Err(e) => {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ordered::OP_INSERT, 1); // full
                return Err(e);
            }
        };
        unsafe { std::ptr::write(self.slot_ptr(idx as usize), value); }
        // Treiber push: CAS head from (c, old_top) to (c+1, idx),
        // with next_link[idx] = old_top.
        loop {
            let head = self.header().head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(idx as usize).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), idx);
            if self.header().head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ordered::OP_INSERT, 0);
                return Ok(());
            }
        }
    }

    /// Pop a value off the stack. Returns `None` if empty.
    pub fn pop(&self) -> Option<T> {
        loop {
            let head = self.header().head.load(Ordering::Acquire);
            let (counter, top) = unpack(head);
            if top == STACK_NIL {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ordered::OP_POP, 2); // empty
                return None;
            }
            let next_top = self.next_link(top as usize).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next_top);
            if self.header().head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                let value = unsafe { std::ptr::read(self.slot_ptr(top as usize)) };
                self.release_slot(top);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ordered::OP_POP, 0);
                return Some(value);
            }
        }
    }

    /// Peek at the top without popping. Returns `None` if empty.
    pub fn peek(&self) -> Option<T> {
        let head = self.header().head.load(Ordering::Acquire);
        let (_, top) = unpack(head);
        if top == STACK_NIL {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ordered::OP_GET, 2); // empty
            return None;
        }
        let v = unsafe { std::ptr::read(self.slot_ptr(top as usize)) };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_GET, 0);
        Some(v)
    }

    /// True when the stack is empty.
    pub fn is_empty(&self) -> bool {
        let head = self.header().head.load(Ordering::Acquire);
        unpack(head).1 == STACK_NIL
    }

    /// Approximate len: walks the linked list from head. O(N).
    /// Subject to race with concurrent push/pop.
    pub fn approx_len(&self) -> usize {
        let head = self.header().head.load(Ordering::Acquire);
        let (_, mut idx) = unpack(head);
        let mut count = 0usize;
        let mut visited = 0;
        while idx != STACK_NIL && visited < self.capacity {
            count += 1;
            visited += 1;
            idx = self.next_link(idx as usize).load(Ordering::Acquire);
        }
        count
    }

    pub fn flush(&self) -> Result<(), StackError> {
        self.mmap.flush()?;
        Ok(())
    }
    pub fn flush_async(&self) -> Result<(), StackError> {
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
        p.push(format!("subetha-stack-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let s: SharedTreiberStack<u64> = SharedTreiberStack::create(&p, 16).unwrap();
        assert!(s.is_empty());
        assert_eq!(s.pop(), None);
        assert_eq!(s.peek(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn push_pop_lifo_order() {
        let p = tmp("lifo");
        let s: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 16).unwrap();
        s.push(10).unwrap();
        s.push(20).unwrap();
        s.push(30).unwrap();
        assert_eq!(s.pop(), Some(30));
        assert_eq!(s.pop(), Some(20));
        assert_eq!(s.pop(), Some(10));
        assert_eq!(s.pop(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn peek_does_not_remove() {
        let p = tmp("peek");
        let s: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 8).unwrap();
        s.push(42).unwrap();
        assert_eq!(s.peek(), Some(42));
        assert_eq!(s.peek(), Some(42));
        assert_eq!(s.pop(), Some(42));
        assert_eq!(s.peek(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_capacity_returns_error() {
        let p = tmp("full");
        let s: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 4).unwrap();
        for i in 0..4 { s.push(i).unwrap(); }
        assert_eq!(s.push(99).err(), Some(StackError::Full));
        // After popping, can push again.
        s.pop();
        s.push(99).unwrap();
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn free_list_reuse_after_pop() {
        let p = tmp("reuse");
        let s: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 4).unwrap();
        for i in 0..4 { s.push(i).unwrap(); }
        for _ in 0..4 { s.pop(); }
        // After full drain, push 4 more should succeed (slots reused).
        for i in 100..104 { s.push(i).unwrap(); }
        assert_eq!(s.pop(), Some(103));
        assert_eq!(s.pop(), Some(102));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn approx_len_tracks_size() {
        let p = tmp("len");
        let s: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 16).unwrap();
        assert_eq!(s.approx_len(), 0);
        s.push(1).unwrap();
        s.push(2).unwrap();
        s.push(3).unwrap();
        assert_eq!(s.approx_len(), 3);
        s.pop();
        assert_eq!(s.approx_len(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let w: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 8).unwrap();
        let r: SharedTreiberStack<u32> = SharedTreiberStack::open(&p, 8).unwrap();
        w.push(42).unwrap();
        w.push(7).unwrap();
        assert_eq!(r.peek(), Some(7));
        assert_eq!(r.pop(), Some(7));
        assert_eq!(w.pop(), Some(42));
        assert!(r.is_empty());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Frame { pc: u64, sp: u64 }
        let p = tmp("struct");
        let s: SharedTreiberStack<Frame> = SharedTreiberStack::create(&p, 8).unwrap();
        s.push(Frame { pc: 0x1000, sp: 0xFF00 }).unwrap();
        s.push(Frame { pc: 0x2000, sp: 0xFE00 }).unwrap();
        assert_eq!(s.pop(), Some(Frame { pc: 0x2000, sp: 0xFE00 }));
        assert_eq!(s.pop(), Some(Frame { pc: 0x1000, sp: 0xFF00 }));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_pushers_all_succeed() {
        let p = tmp("concurrent-push");
        let s: Arc<SharedTreiberStack<u32>>
            = Arc::new(SharedTreiberStack::create(&p, 1024).unwrap());
        let n_threads = 4;
        let per_thread = 100;
        let mut handles = vec![];
        for t in 0..n_threads as u32 {
            let s = s.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread as u32 {
                    s.push(t * 1000 + i).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(s.approx_len(), n_threads * per_thread);
        // Drain and collect.
        let mut all = Vec::new();
        while let Some(v) = s.pop() { all.push(v); }
        all.sort();
        // Expect 4 threads * 100 values: t=0..4, i=0..100.
        let mut expected: Vec<u32> = (0..n_threads as u32)
            .flat_map(|t| (0..per_thread as u32).map(move |i| t * 1000 + i))
            .collect();
        expected.sort();
        assert_eq!(all, expected);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_push_pop_no_corruption() {
        // Producers push, consumers pop. After joining, no items
        // lost or duplicated.
        let p = tmp("concurrent-pp");
        let s: Arc<SharedTreiberStack<u32>>
            = Arc::new(SharedTreiberStack::create(&p, 1024).unwrap());
        // Pre-fill with 500 known values.
        for i in 0..500u32 { s.push(i).unwrap(); }
        // 4 consumer threads pop everything they can, collecting locally.
        let mut handles = vec![];
        for _ in 0..4 {
            let s = s.clone();
            handles.push(thread::spawn(move || {
                let mut got = Vec::new();
                while let Some(v) = s.pop() { got.push(v); }
                got
            }));
        }
        let mut total: Vec<u32> = handles.into_iter()
            .flat_map(|h| h.join().unwrap()).collect();
        total.sort();
        let expected: Vec<u32> = (0..500u32).collect();
        assert_eq!(total, expected, "no items should be lost or duplicated");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let s: SharedTreiberStack<u32> = SharedTreiberStack::create(&p, 8).unwrap();
            s.push(1).unwrap();
            s.push(2).unwrap();
            s.push(3).unwrap();
            s.flush().unwrap();
        }
        let s2: SharedTreiberStack<u32> = SharedTreiberStack::open(&p, 8).unwrap();
        assert_eq!(s2.pop(), Some(3));
        assert_eq!(s2.pop(), Some(2));
        assert_eq!(s2.pop(), Some(1));
        std::fs::remove_file(&p).ok();
    }
}
