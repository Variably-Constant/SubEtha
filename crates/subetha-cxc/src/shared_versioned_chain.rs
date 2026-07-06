//! `SharedVersionedChain<T>` - cross-process MVCC linked list.
//!
//! Each node holds `(version: u64, value: T)`. Nodes are linked
//! newest-first via an `AtomicU32` head and per-node `next` offsets.
//! A reader walking from head sees nodes in descending version
//! order and can `read_at(snapshot)` to find the newest version
//! that's <= the snapshot.
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | ChainHeader (64B)           |
//! |   - magic                   |
//! |   - capacity                |
//! |   - payload_size            |
//! |   - head: AtomicU32 (idx)   |
//! |   - free_list_head: u64     |  (counter, idx) packed
//! |   - live_count: u64         |
//! +-----------------------------+
//! | VersionNode[0] (64B)        |
//! |   - version: AtomicU64      |
//! |   - next:   AtomicU32       |
//! |   - next_free: AtomicU32    |
//! |   - payload: [u8; 48]       |
//! +-----------------------------+
//! | VersionNode[1] ...          |
//! +-----------------------------+
//! ```
//!
//! Same slot-allocator pattern as `SharedHandleTable`: ABA-free
//! Treiber stack for the free list, atomic CAS for head updates.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const VERSIONED_CHAIN_MAGIC: u64 = 0x4150_4D46_5643_4E48;
pub const NODE_PAYLOAD_BYTES: usize = 48;
pub const NIL_NODE: u32 = u32::MAX;

#[repr(C, align(64))]
pub struct ChainHeader {
    pub magic: u64,
    pub capacity: u32,
    pub payload_size: u32,
    pub head: AtomicU32,
    pub free_list_head: AtomicU64,
    pub live_count: AtomicU64,
    _pad: [u8; 32],
}

#[repr(C, align(64))]
pub struct VersionNode {
    pub version: AtomicU64,
    pub next: AtomicU32,
    pub next_free: AtomicU32,
    pub payload: [u8; NODE_PAYLOAD_BYTES],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainError {
    LayoutMismatch,
    PayloadTooLarge,
    Full,
    NonMonotonicVersion,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for ChainError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub const fn versioned_chain_file_size(capacity: usize) -> usize {
    size_of::<ChainHeader>() + capacity * size_of::<VersionNode>()
}

pub struct SharedVersionedChain<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedVersionedChain<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedVersionedChain<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedVersionedChain<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

#[inline]
fn pack_head(counter: u32, idx: u32) -> u64 {
    ((counter as u64) << 32) | (idx as u64)
}

#[inline]
fn unpack_head(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

impl<T: Copy + 'static> SharedVersionedChain<T> {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, ChainError> {
        Self::check_layout()?;
        assert!(capacity >= 1 && capacity < (u32::MAX - 1) as usize);
        let total = versioned_chain_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut ChainHeader;
        unsafe {
            std::ptr::write(hdr, ChainHeader {
                magic: VERSIONED_CHAIN_MAGIC,
                capacity: capacity as u32,
                payload_size: size_of::<T>() as u32,
                head: AtomicU32::new(NIL_NODE),
                free_list_head: AtomicU64::new(pack_head(0, 0)),
                live_count: AtomicU64::new(0),
                _pad: [0; 32],
            });
        }
        let nodes_base = unsafe { mmap.as_mut_ptr().add(size_of::<ChainHeader>()) };
        for i in 0..capacity {
            let node_ptr = unsafe {
                nodes_base.add(i * size_of::<VersionNode>()) as *mut VersionNode
            };
            let next_free = if i + 1 < capacity { (i + 1) as u32 } else { NIL_NODE };
            unsafe {
                std::ptr::write(node_ptr, VersionNode {
                    version: AtomicU64::new(0),
                    next: AtomicU32::new(NIL_NODE),
                    next_free: AtomicU32::new(next_free),
                    payload: [0; NODE_PAYLOAD_BYTES],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, ChainError> {
        Self::check_layout()?;
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = versioned_chain_file_size(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(ChainError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const ChainHeader) };
        if hdr.magic != VERSIONED_CHAIN_MAGIC
            || hdr.capacity != expected_capacity as u32
            || hdr.payload_size as usize != size_of::<T>()
        {
            return Err(ChainError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn check_layout() -> Result<(), ChainError> {
        if size_of::<T>() > NODE_PAYLOAD_BYTES {
            return Err(ChainError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(ChainError::PayloadTooLarge);
        }
        Ok(())
    }

    pub fn capacity(&self) -> usize { self.capacity }

    /// Reset to empty: head -> NIL, live_count -> 0, free list rebuilt
    /// to contain all slots. Stale `version` values referring to old
    /// snapshots become invalid. Not thread-safe with concurrent
    /// push/read from other threads.
    pub fn clear(&self) {
        let header = self.header();
        header.head.store(NIL_NODE, Ordering::Release);
        header.live_count.store(0, Ordering::Release);
        // Rebuild free list: slot 0 is head, slot i links to i+1, last links to NIL.
        for i in 0..self.capacity {
            let next_free = if i + 1 < self.capacity { (i + 1) as u32 } else { NIL_NODE };
            self.node(i as u32).next_free.store(next_free, Ordering::Release);
            self.node(i as u32).next.store(NIL_NODE, Ordering::Release);
            self.node(i as u32).version.store(0, Ordering::Release);
        }
        header.free_list_head.store(pack_head(0, 0), Ordering::Release);
    }

    pub fn header(&self) -> &ChainHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ChainHeader) }
    }

    fn node(&self, idx: u32) -> &VersionNode {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<ChainHeader>()) };
        unsafe { &*(base.add((idx as usize) * size_of::<VersionNode>()) as *const VersionNode) }
    }

    fn pop_free(&self) -> Option<u32> {
        let header = self.header();
        loop {
            let head = header.free_list_head.load(Ordering::Acquire);
            let (cnt, idx) = unpack_head(head);
            if idx == NIL_NODE { return None; }
            let next = self.node(idx).next_free.load(Ordering::Acquire);
            let new_head = pack_head(cnt.wrapping_add(1), next);
            if header.free_list_head.compare_exchange_weak(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Some(idx);
            }
            std::hint::spin_loop();
        }
    }

    /// Push a new version at the head. `version` must be strictly
    /// greater than the current head's version (MVCC invariant).
    pub fn push(&self, version: u64, value: T) -> Result<(), ChainError> {
        let r = self.push_inner(version, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::versioned::OP_PUSH,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn push_inner(&self, version: u64, value: T) -> Result<(), ChainError> {
        let header = self.header();
        // Optimistic CAS loop on the head pointer; verify version
        // monotonicity under contention.
        loop {
            let cur_head = header.head.load(Ordering::Acquire);
            if cur_head != NIL_NODE {
                let cur_version = self.node(cur_head).version.load(Ordering::Acquire);
                if version <= cur_version {
                    return Err(ChainError::NonMonotonicVersion);
                }
            }
            let new_idx = self.pop_free().ok_or(ChainError::Full)?;
            let new_node = self.node(new_idx);
            new_node.version.store(version, Ordering::Release);
            new_node.next.store(cur_head, Ordering::Release);
            // SAFETY: we just allocated new_idx from the free list,
            // so we own its payload exclusively until the head CAS.
            unsafe {
                let dst = new_node.payload.as_ptr() as *mut T;
                std::ptr::write_unaligned(dst, value);
            }
            // CAS head from cur_head -> new_idx.
            if header.head.compare_exchange_weak(
                cur_head, new_idx, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                header.live_count.fetch_add(1, Ordering::AcqRel);
                return Ok(());
            }
            // CAS failed; return the node to the free list and retry.
            self.push_free(new_idx);
            std::hint::spin_loop();
        }
    }

    fn push_free(&self, idx: u32) {
        let header = self.header();
        loop {
            let head = header.free_list_head.load(Ordering::Acquire);
            let (cnt, head_idx) = unpack_head(head);
            self.node(idx).next_free.store(head_idx, Ordering::Release);
            let new_head = pack_head(cnt.wrapping_add(1), idx);
            if header.free_list_head.compare_exchange_weak(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Read the value visible at `snapshot_version`. Walks back from
    /// head through the chain until a node with version <= snapshot
    /// is found. Returns `None` if no such version exists.
    pub fn read_at(&self, snapshot_version: u64) -> Option<T> {
        let header = self.header();
        let mut cur = header.head.load(Ordering::Acquire);
        while cur != NIL_NODE {
            let node = self.node(cur);
            let v = node.version.load(Ordering::Acquire);
            if v <= snapshot_version {
                let value: T = unsafe {
                    let src = node.payload.as_ptr() as *const T;
                    std::ptr::read_unaligned(src)
                };
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::versioned::OP_READ_AT, 0);
                return Some(value);
            }
            cur = node.next.load(Ordering::Acquire);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::versioned::OP_READ_AT, 2);
        None
    }

    /// Latest (head) version + value, or `None` if empty.
    pub fn current(&self) -> Option<(u64, T)> {
        let head = self.header().head.load(Ordering::Acquire);
        if head == NIL_NODE {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::versioned::OP_CURRENT, 2);
            return None;
        }
        let node = self.node(head);
        let v = node.version.load(Ordering::Acquire);
        let value: T = unsafe {
            let src = node.payload.as_ptr() as *const T;
            std::ptr::read_unaligned(src)
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::versioned::OP_CURRENT, 0);
        Some((v, value))
    }

    pub fn len(&self) -> usize {
        self.header().live_count.load(Ordering::Acquire) as usize
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn flush(&self) -> Result<(), ChainError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), ChainError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-chain-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn push_then_read_at_returns_correct_version() {
        let p = tmp("push-read");
        let c: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 8).unwrap();
        c.push(1, 10).unwrap();
        c.push(2, 20).unwrap();
        c.push(3, 30).unwrap();
        // Time-travel reads.
        assert_eq!(c.read_at(0), None);
        assert_eq!(c.read_at(1), Some(10));
        assert_eq!(c.read_at(2), Some(20));
        assert_eq!(c.read_at(3), Some(30));
        assert_eq!(c.read_at(100), Some(30));
        assert_eq!(c.current(), Some((3, 30)));
        assert_eq!(c.len(), 3);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn push_rejects_non_monotonic_version() {
        let p = tmp("non-mono");
        let c: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 4).unwrap();
        c.push(10, 100).unwrap();
        assert_eq!(c.push(5, 50).unwrap_err(), ChainError::NonMonotonicVersion);
        assert_eq!(c.push(10, 100).unwrap_err(), ChainError::NonMonotonicVersion);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_chain_returns_error() {
        let p = tmp("full");
        let c: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 2).unwrap();
        c.push(1, 10).unwrap();
        c.push(2, 20).unwrap();
        assert_eq!(c.push(3, 30).unwrap_err(), ChainError::Full);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 8).unwrap();
        let reader: SharedVersionedChain<u64> = SharedVersionedChain::open(&p, 8).unwrap();
        writer.push(1, 100).unwrap();
        writer.push(2, 200).unwrap();
        assert_eq!(reader.read_at(2), Some(200));
        assert_eq!(reader.current(), Some((2, 200)));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let c: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 8).unwrap();
            c.push(10, 1000).unwrap();
            c.push(20, 2000).unwrap();
            c.flush().unwrap();
        }
        let c2: SharedVersionedChain<u64> = SharedVersionedChain::open(&p, 8).unwrap();
        assert_eq!(c2.read_at(20), Some(2000));
        assert_eq!(c2.read_at(10), Some(1000));
        assert_eq!(c2.len(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_chain_reads_none() {
        let p = tmp("empty");
        let c: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 4).unwrap();
        assert!(c.is_empty());
        assert_eq!(c.read_at(0), None);
        assert_eq!(c.read_at(u64::MAX), None);
        assert_eq!(c.current(), None);
        std::fs::remove_file(&p).ok();
    }
}
