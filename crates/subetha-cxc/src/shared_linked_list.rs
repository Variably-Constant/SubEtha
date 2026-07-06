//! `SharedLinkedList<T>` - cross-process doubly-linked list with
//! O(1) handle-based removal.
//!
//! Backed by [`SharedRegion`] of `Node<T>`.
//! Each node stores T plus raw `next` and `prev` slot indices.
//! Slot 0 is the sentinel head; real nodes live in slots 1..N.
//!
//! # Architectural value: stable handles
//!
//! Push operations return a [`NodeHandle`] - a stable u32 slot
//! index. Callers retain handles to call `remove(handle)` for O(1)
//! middle removal. This is the unique value over
//! [`SharedRing`](crate::SharedRing) (FIFO only, no random access)
//! and [`SharedVec`](crate::SharedVec) (no middle removal).
//!
//! # Handle validity contract
//!
//! A NodeHandle is valid from the moment push returns it until the
//! caller invokes `remove(handle)` or one of the pop operations on
//! that node. Using a stale handle after the node has been removed
//! is a logic error (the slot may have been reused by a subsequent
//! push). This is the same contract as `std::list::iterator` in
//! C++.
//!
//! # Concurrency model
//!
//! SINGLE-WRITER, MULTI-READER. push / pop / remove / set require
//! external serialisation (wrap in a [`SharedSemaphore`](
//! crate::SharedSemaphore) with 1 permit, or use the application's
//! own coordination). Iteration is lock-free.

use std::marker::PhantomData;
use std::path::Path;

use crate::shared_region::{OffsetPtr, RegionError, SharedRegion};

/// Sentinel NIL value for next/prev pointers.
pub const NIL_INDEX: u32 = u32::MAX;

/// Slot index of the sentinel head node within the underlying
/// SharedRegion. Always 0; allocated at create time.
pub const HEAD_INDEX: u32 = 0;

#[derive(Debug)]
#[repr(C)]
pub struct Node<T: Copy + Default + 'static> {
    pub value: T,
    pub next: u32,
    pub prev: u32,
}

// Manual impls so we don't need `T: Clone` (Copy implies it but
// derive(Clone) requires it written explicitly).
impl<T: Copy + Default + 'static> Clone for Node<T> {
    fn clone(&self) -> Self { *self }
}
impl<T: Copy + Default + 'static> Copy for Node<T> {}

/// Stable handle to a list node. Valid until removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct NodeHandle<T> {
    pub index: u32,
    _phantom: PhantomData<T>,
}

impl<T> NodeHandle<T> {
    pub const NIL: Self = Self { index: NIL_INDEX, _phantom: PhantomData };

    #[inline]
    pub fn new(index: u32) -> Self {
        Self { index, _phantom: PhantomData }
    }

    #[inline]
    pub fn is_nil(self) -> bool { self.index == NIL_INDEX }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkedListError {
    Region(RegionError),
    InvalidHandle,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<RegionError> for LinkedListError {
    fn from(e: RegionError) -> Self { Self::Region(e) }
}
impl From<std::io::Error> for LinkedListError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedLinkedList<T: Copy + Default + 'static> {
    region: SharedRegion<Node<T>>,
    /// Cached absolute base of the node-slot array (mmap_ptr + header +
    /// per-slot guard array), computed once at create/open. Lets the
    /// link-update paths read/write only the `next`/`prev`/`value` field
    /// they touch instead of copying the whole Node through
    /// region.get/region.set. The MMF mapping is fixed for the handle's
    /// lifetime, so the address is stable across moves.
    slots_base: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<T: Copy + Default + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for SharedLinkedList<T>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + Default + 'static> SharedLinkedList<T> {
    pub fn create(
        path: impl AsRef<Path>, capacity: usize,
    ) -> Result<Self, LinkedListError> {
        assert!(capacity >= 2, "capacity must include sentinel head + at least one node");
        let region = SharedRegion::<Node<T>>::create(path, capacity)?;
        // Allocate the sentinel head at slot 0. next and prev both
        // point at HEAD (self) so an empty list is a 1-element ring.
        let head = Node {
            value: T::default(),
            next: HEAD_INDEX,
            prev: HEAD_INDEX,
        };
        let head_ptr = region.allocate(head)?;
        assert_eq!(head_ptr.index, HEAD_INDEX,
            "first allocation must be slot 0");
        let slots_base = Self::slots_base_of(&region);
        Ok(Self {
            region, slots_base, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, capacity: usize,
    ) -> Result<Self, LinkedListError> {
        let region = SharedRegion::<Node<T>>::open(path, capacity)?;
        let slots_base = Self::slots_base_of(&region);
        Ok(Self {
            region, slots_base, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Absolute base address of the node-slot array within the region's
    /// MMF (after the RegionHeader and the per-slot atomic-guard array).
    #[inline]
    fn slots_base_of(region: &SharedRegion<Node<T>>) -> usize {
        region.mmap_ptr() as usize
            + std::mem::size_of::<crate::shared_region::RegionHeader>()
            + region.capacity() * std::mem::size_of::<u32>()
    }

    #[inline]
    fn node_ptr(&self, idx: u32) -> usize {
        self.slots_base + idx as usize * std::mem::size_of::<Node<T>>()
    }

    /// Field-direct reads/writes of a node's `value`/`next`/`prev`. The
    /// link-update paths only touch one field, so this avoids copying the
    /// whole Node through region.get/region.set. Non-atomic, matching the
    /// existing model (writers serialise externally; a concurrent reader
    /// sees old-or-new for a word-sized field, never a torn whole node).
    #[inline]
    fn read_value(&self, idx: u32) -> T {
        let addr = self.node_ptr(idx) + std::mem::offset_of!(Node<T>, value);
        unsafe { (addr as *const T).read() }
    }
    #[inline]
    fn read_next(&self, idx: u32) -> u32 {
        let addr = self.node_ptr(idx) + std::mem::offset_of!(Node<T>, next);
        unsafe { (addr as *const u32).read() }
    }
    #[inline]
    fn read_prev(&self, idx: u32) -> u32 {
        let addr = self.node_ptr(idx) + std::mem::offset_of!(Node<T>, prev);
        unsafe { (addr as *const u32).read() }
    }
    #[inline]
    fn set_next(&self, idx: u32, value: u32) {
        let addr = self.node_ptr(idx) + std::mem::offset_of!(Node<T>, next);
        unsafe { (addr as *mut u32).write(value); }
    }
    #[inline]
    fn set_prev(&self, idx: u32, value: u32) {
        let addr = self.node_ptr(idx) + std::mem::offset_of!(Node<T>, prev);
        unsafe { (addr as *mut u32).write(value); }
    }
    #[inline]
    fn set_value(&self, idx: u32, value: T) {
        let addr = self.node_ptr(idx) + std::mem::offset_of!(Node<T>, value);
        unsafe { (addr as *mut T).write(value); }
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.region.capacity() }

    /// Approximate node count (excludes the sentinel head).
    pub fn len(&self) -> usize {
        self.region.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Access the underlying region (advanced use; e.g., to wrap
    /// writes in a SharedSemaphore for cross-process serialisation).
    pub fn region(&self) -> &SharedRegion<Node<T>> { &self.region }

    fn read_node(&self, idx: u32) -> Node<T> {
        self.region.get(OffsetPtr::new(idx))
            .expect("valid node index")
    }

    /// Push to the front of the list. Returns a stable NodeHandle.
    pub fn push_front(&self, value: T) -> Result<NodeHandle<T>, LinkedListError> {
        let r = self.push_front_inner(value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_PUSH_FRONT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn push_front_inner(&self, value: T) -> Result<NodeHandle<T>, LinkedListError> {
        let old_first = self.read_next(HEAD_INDEX);
        let new = Node {
            value,
            next: old_first,
            prev: HEAD_INDEX,
        };
        let new_ptr = self.region.allocate(new)?;
        let new_idx = new_ptr.index;
        // Link the successor's `prev` to the new node: the old first node,
        // or the head itself when the list was empty.
        if old_first != HEAD_INDEX {
            self.set_prev(old_first, new_idx);
        } else {
            self.set_prev(HEAD_INDEX, new_idx);
        }
        self.set_next(HEAD_INDEX, new_idx);
        Ok(NodeHandle::new(new_idx))
    }

    /// Push to the back of the list. Returns a stable NodeHandle.
    pub fn push_back(&self, value: T) -> Result<NodeHandle<T>, LinkedListError> {
        let r = self.push_back_inner(value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_PUSH_BACK,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn push_back_inner(&self, value: T) -> Result<NodeHandle<T>, LinkedListError> {
        let old_last = self.read_prev(HEAD_INDEX);
        let new = Node {
            value,
            next: HEAD_INDEX,
            prev: old_last,
        };
        let new_ptr = self.region.allocate(new)?;
        let new_idx = new_ptr.index;
        // Link the predecessor's `next` to the new node: the old last
        // node, or the head itself when the list was empty.
        if old_last != HEAD_INDEX {
            self.set_next(old_last, new_idx);
        } else {
            self.set_next(HEAD_INDEX, new_idx);
        }
        self.set_prev(HEAD_INDEX, new_idx);
        Ok(NodeHandle::new(new_idx))
    }

    /// Remove and return the first element.
    pub fn pop_front(&self) -> Option<T> {
        let first_idx = self.read_next(HEAD_INDEX);
        if first_idx == HEAD_INDEX {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::linked_list::OP_POP_FRONT, 2); // empty
            return None;
        }
        let r = self.remove_by_index(first_idx);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_POP_FRONT,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Remove and return the last element.
    pub fn pop_back(&self) -> Option<T> {
        let head = self.read_node(HEAD_INDEX);
        if head.prev == HEAD_INDEX {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::linked_list::OP_POP_BACK, 2); // empty
            return None;
        }
        let last_idx = head.prev;
        let r = self.remove_by_index(last_idx);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_POP_BACK,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Remove the node referenced by `handle` in O(1) (splice +
    /// free the slot). Returns the removed value or None when the
    /// handle is the sentinel head OR was already freed.
    pub fn remove(&self, handle: NodeHandle<T>) -> Option<T> {
        if handle.is_nil() || handle.index == HEAD_INDEX {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::linked_list::OP_REMOVE, 2); // absent (nil/head)
            return None;
        }
        let r = self.remove_by_index(handle.index);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_REMOVE,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn remove_by_index(&self, idx: u32) -> Option<T> {
        // Read only the three fields of the node being spliced out, not
        // the whole node, and repoint neighbours field-direct.
        let node_prev = self.read_prev(idx);
        let node_next = self.read_next(idx);
        let node_value = self.read_value(idx);
        // Predecessor side: link prev.next -> node.next.
        if node_prev == HEAD_INDEX {
            self.set_next(HEAD_INDEX, node_next);
            if node_next == HEAD_INDEX {
                self.set_prev(HEAD_INDEX, HEAD_INDEX);
            }
        } else {
            self.set_next(node_prev, node_next);
        }
        // Successor side: link next.prev -> node.prev.
        if node_next == HEAD_INDEX {
            self.set_prev(HEAD_INDEX, node_prev);
            if node_prev == HEAD_INDEX {
                self.set_next(HEAD_INDEX, HEAD_INDEX);
            }
        } else {
            self.set_prev(node_next, node_prev);
        }
        self.region.free(OffsetPtr::new(idx)).ok();
        Some(node_value)
    }

    /// Read the value at `handle`. Returns None for nil or stale
    /// handle (after remove).
    pub fn get(&self, handle: NodeHandle<T>) -> Option<T> {
        let r = if handle.is_nil() || handle.index == HEAD_INDEX {
            None
        } else {
            Some(self.read_value(handle.index))
        };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_ITER,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Overwrite the value at `handle` (keeping the node's position
    /// in the list).
    pub fn set(&self, handle: NodeHandle<T>, value: T) -> Result<(), LinkedListError> {
        if handle.is_nil() || handle.index == HEAD_INDEX {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::linked_list::OP_PUSH_BACK, 1); // invalid handle (positional write rejected)
            return Err(LinkedListError::InvalidHandle);
        }
        self.set_value(handle.index, value);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::linked_list::OP_PUSH_BACK, 0);
        Ok(())
    }

    /// First node's value (None if empty).
    pub fn first(&self) -> Option<T> {
        let head = self.read_node(HEAD_INDEX);
        let r = if head.next == HEAD_INDEX {
            None
        } else {
            Some(self.read_node(head.next).value)
        };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_ITER,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Last node's value (None if empty).
    pub fn last(&self) -> Option<T> {
        let head = self.read_node(HEAD_INDEX);
        let r = if head.prev == HEAD_INDEX {
            None
        } else {
            Some(self.read_node(head.prev).value)
        };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::linked_list::OP_ITER,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Snapshot of all values in forward order.
    pub fn iter_forward(&self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.len());
        let mut cur = self.read_node(HEAD_INDEX).next;
        while cur != HEAD_INDEX {
            let node = self.read_node(cur);
            out.push(node.value);
            cur = node.next;
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::linked_list::OP_ITER, 0);
        out
    }

    /// Snapshot of all values in reverse order (back to front).
    pub fn iter_backward(&self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.len());
        let mut cur = self.read_node(HEAD_INDEX).prev;
        while cur != HEAD_INDEX {
            let node = self.read_node(cur);
            out.push(node.value);
            cur = node.prev;
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::linked_list::OP_ITER, 0);
        out
    }

    /// Snapshot of all (handle, value) pairs in forward order.
    /// Useful for finding a handle by predicate, then removing it.
    pub fn iter_forward_with_handles(&self) -> Vec<(NodeHandle<T>, T)> {
        let mut out = Vec::with_capacity(self.len());
        let mut cur = self.read_node(HEAD_INDEX).next;
        while cur != HEAD_INDEX {
            let node = self.read_node(cur);
            out.push((NodeHandle::new(cur), node.value));
            cur = node.next;
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::linked_list::OP_ITER, 0);
        out
    }

    pub fn flush(&self) -> Result<(), LinkedListError> {
        Ok(self.region.flush()?)
    }

    pub fn flush_async(&self) -> Result<(), LinkedListError> {
        Ok(self.region.flush_async()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-linkedlist-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 32).unwrap();
        assert!(l.is_empty());
        assert_eq!(l.len(), 0);
        assert_eq!(l.first(), None);
        assert_eq!(l.last(), None);
        assert_eq!(l.iter_forward(), Vec::<u32>::new());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn push_back_and_iterate_forward() {
        let p = tmp("push-back");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 32).unwrap();
        for i in [10u32, 20, 30, 40, 50] { l.push_back(i).unwrap(); }
        assert_eq!(l.len(), 5);
        assert_eq!(l.iter_forward(), vec![10, 20, 30, 40, 50]);
        assert_eq!(l.iter_backward(), vec![50, 40, 30, 20, 10]);
        assert_eq!(l.first(), Some(10));
        assert_eq!(l.last(), Some(50));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn push_front_inserts_at_head() {
        let p = tmp("push-front");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 32).unwrap();
        for i in [10u32, 20, 30] { l.push_front(i).unwrap(); }
        assert_eq!(l.iter_forward(), vec![30, 20, 10]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn pop_front_and_back_round_trip() {
        let p = tmp("pop");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 32).unwrap();
        for i in [10u32, 20, 30, 40] { l.push_back(i).unwrap(); }
        assert_eq!(l.pop_front(), Some(10));
        assert_eq!(l.pop_back(), Some(40));
        assert_eq!(l.iter_forward(), vec![20, 30]);
        assert_eq!(l.pop_front(), Some(20));
        assert_eq!(l.pop_back(), Some(30));
        assert!(l.is_empty());
        assert_eq!(l.pop_front(), None);
        assert_eq!(l.pop_back(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn remove_by_handle_in_middle_preserves_integrity() {
        let p = tmp("remove-middle");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 32).unwrap();
        let h1 = l.push_back(10).unwrap();
        let h2 = l.push_back(20).unwrap();
        let h3 = l.push_back(30).unwrap();
        let h4 = l.push_back(40).unwrap();
        let h5 = l.push_back(50).unwrap();
        // Remove the middle one.
        assert_eq!(l.remove(h3), Some(30));
        assert_eq!(l.len(), 4);
        assert_eq!(l.iter_forward(), vec![10, 20, 40, 50]);
        assert_eq!(l.iter_backward(), vec![50, 40, 20, 10]);
        // Remove the head.
        assert_eq!(l.remove(h1), Some(10));
        assert_eq!(l.iter_forward(), vec![20, 40, 50]);
        // Remove the tail.
        assert_eq!(l.remove(h5), Some(50));
        assert_eq!(l.iter_forward(), vec![20, 40]);
        // Remove remaining via handles.
        assert_eq!(l.remove(h2), Some(20));
        assert_eq!(l.remove(h4), Some(40));
        assert!(l.is_empty());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn remove_nil_or_head_returns_none() {
        let p = tmp("remove-nil");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 8).unwrap();
        l.push_back(1).unwrap();
        assert_eq!(l.remove(NodeHandle::NIL), None);
        assert_eq!(l.remove(NodeHandle::new(HEAD_INDEX)), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn get_and_set_via_handle() {
        let p = tmp("get-set");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 8).unwrap();
        let h = l.push_back(42).unwrap();
        assert_eq!(l.get(h), Some(42));
        l.set(h, 100).unwrap();
        assert_eq!(l.get(h), Some(100));
        assert_eq!(l.iter_forward(), vec![100]);
        // get on NIL.
        assert_eq!(l.get(NodeHandle::NIL), None);
        // set on NIL.
        assert!(l.set(NodeHandle::NIL, 0).is_err());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_capacity_returns_error() {
        let p = tmp("full");
        // capacity 4 = head + 3 nodes.
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 4).unwrap();
        l.push_back(1).unwrap();
        l.push_back(2).unwrap();
        l.push_back(3).unwrap();
        assert!(l.push_back(4).is_err());
        // After popping, can push again.
        l.pop_front().unwrap();
        l.push_back(4).unwrap();
        assert_eq!(l.iter_forward(), vec![2, 3, 4]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn iter_forward_with_handles_returns_pairs() {
        let p = tmp("iter-handles");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 16).unwrap();
        let h1 = l.push_back(10).unwrap();
        let h2 = l.push_back(20).unwrap();
        let h3 = l.push_back(30).unwrap();
        let pairs = l.iter_forward_with_handles();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (h1, 10));
        assert_eq!(pairs[1], (h2, 20));
        assert_eq!(pairs[2], (h3, 30));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedLinkedList<u32> = SharedLinkedList::create(&p, 16).unwrap();
        let reader: SharedLinkedList<u32> = SharedLinkedList::open(&p, 16).unwrap();
        writer.push_back(100).unwrap();
        writer.push_back(200).unwrap();
        assert_eq!(reader.iter_forward(), vec![100, 200]);
        let h = writer.push_back(300).unwrap();
        assert_eq!(reader.iter_forward(), vec![100, 200, 300]);
        writer.remove(h);
        assert_eq!(reader.iter_forward(), vec![100, 200]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq, Default)]
        #[repr(C)]
        struct Event { ts_us: u64, code: u32 }
        let p = tmp("struct");
        let l: SharedLinkedList<Event> = SharedLinkedList::create(&p, 16).unwrap();
        let h1 = l.push_back(Event { ts_us: 100, code: 1 }).unwrap();
        let _h2 = l.push_back(Event { ts_us: 200, code: 2 }).unwrap();
        assert_eq!(l.get(h1), Some(Event { ts_us: 100, code: 1 }));
        let items = l.iter_forward();
        assert_eq!(items.len(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 16).unwrap();
            for i in [10u32, 20, 30, 40] { l.push_back(i).unwrap(); }
            l.flush().unwrap();
        }
        let l2: SharedLinkedList<u32> = SharedLinkedList::open(&p, 16).unwrap();
        assert_eq!(l2.iter_forward(), vec![10, 20, 30, 40]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lru_pattern_move_to_front() {
        // Realistic LRU pattern: when a key is accessed, remove its
        // node and re-push at the front. Demonstrates O(1) handle-
        // based remove + push_front composition.
        let p = tmp("lru");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 32).unwrap();
        let h_a = l.push_back(1).unwrap();  // LRU=1
        let _h_b = l.push_back(2).unwrap();
        let _h_c = l.push_back(3).unwrap();  // MRU=3
        // "Access" key A: move to front.
        let val_a = l.remove(h_a).unwrap();
        l.push_front(val_a).unwrap();
        // Now order is A, C... wait, push_front so A is first; then
        // existing order continues.
        assert_eq!(l.iter_forward(), vec![1, 2, 3]);
        // After move-to-front, A is now MRU. Pop_back evicts
        // the LRU.
        assert_eq!(l.pop_back(), Some(3));  // wait, last was 3
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn free_list_pattern_uses_handles() {
        // Realistic free-list pattern: callers hold handles to their
        // own work items in a shared list; release returns the value.
        let p = tmp("free-list");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 16).unwrap();
        let mut handles = vec![];
        for i in 0..10u32 { handles.push(l.push_back(i).unwrap()); }
        // Release every other one.
        for (idx, h) in handles.iter().enumerate() {
            if idx % 2 == 0 {
                let v = l.remove(*h).unwrap();
                assert_eq!(v, idx as u32);
            }
        }
        assert_eq!(l.len(), 5);
        assert_eq!(l.iter_forward(), vec![1, 3, 5, 7, 9]);
        std::fs::remove_file(&p).ok();
    }
}
