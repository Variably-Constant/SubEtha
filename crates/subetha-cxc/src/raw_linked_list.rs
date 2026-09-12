//! `RawLinkedList`: the doubly-linked list of
//! [`SharedLinkedList`](crate::shared_linked_list::SharedLinkedList) at an
//! element size and alignment fixed at run time instead of by a type
//! parameter, for a caller reaching the list from another language. It
//! reads and writes the same file format over a [`RawRegion`]: a node is
//! the value followed by its `next` and `prev` slot indices, slot 0 is the
//! sentinel head, and an empty list is that head pointing at itself.
//!
//! A push returns the node's slot index, which stays valid until that
//! node is removed or popped and names the same node in every process, so
//! a removal from the middle costs one splice. An index used after its
//! node is gone is a caller error: the slot is handed out again.
//!
//! # Concurrency
//!
//! One writer, any number of readers, as the typed list has it. Two
//! concurrent pushes or removes corrupt the links; serializing them is the
//! caller's contract. Reads of a value and of the links are plain copies.

use std::path::Path;

use crate::raw_region::RawRegion;
use crate::raw_treiber_stack::ElementLayout;
use crate::shared_linked_list::{LinkedListError, HEAD_INDEX, NIL_INDEX};
use crate::shared_region::RegionError;

/// Where a node's `next` and `prev` lie and how wide a node is for a value
/// of `layout`: `(next_offset, node_size, node_alignment)`. `prev` sits
/// one `u32` past `next`.
pub fn raw_node_geometry(layout: &ElementLayout) -> (usize, usize, usize) {
    let next_offset = layout.slot_size.next_multiple_of(4);
    let node_alignment = layout.alignment.max(4);
    let node_size = (next_offset + 8).next_multiple_of(node_alignment);
    (next_offset, node_size, node_alignment)
}

/// The region layout a list of `layout` values is built on.
pub fn raw_node_layout(layout: &ElementLayout) -> ElementLayout {
    let (_, node_size, node_alignment) = raw_node_geometry(layout);
    ElementLayout { slot_size: node_size, alignment: node_alignment, tag: layout.tag }
}

pub struct RawLinkedList {
    region: RawRegion,
    layout: ElementLayout,
    next_offset: usize,
    node_size: usize,
}

unsafe impl Send for RawLinkedList {}
unsafe impl Sync for RawLinkedList {}

impl std::fmt::Debug for RawLinkedList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawLinkedList")
            .field("capacity", &self.region.capacity())
            .field("layout", &self.layout)
            .field("node_size", &self.node_size)
            .finish()
    }
}

impl RawLinkedList {
    fn assemble(region: RawRegion, layout: ElementLayout) -> Self {
        let (next_offset, node_size, _) = raw_node_geometry(&layout);
        Self { region, layout, next_offset, node_size }
    }

    /// Obtain the list at `path` holding up to `capacity` nodes, the
    /// sentinel head among them: an empty one is initialized when the file
    /// does not exist, an existing one is attached with its nodes in
    /// place. A file built with another capacity or layout is a
    /// `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, LinkedListError> {
        if capacity < 2 {
            return Err(LinkedListError::Region(RegionError::LayoutMismatch));
        }
        let region = RawRegion::create(path, capacity, raw_node_layout(&layout))?;
        let this = Self::assemble(region, layout);
        this.ensure_head()?;
        Ok(this)
    }

    /// Truncate the list at `path` and initialize an empty one,
    /// invalidating every index live peers hold.
    pub fn reset(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, LinkedListError> {
        if capacity < 2 {
            return Err(LinkedListError::Region(RegionError::LayoutMismatch));
        }
        let region = RawRegion::reset(path, capacity, raw_node_layout(&layout))?;
        let this = Self::assemble(region, layout);
        this.ensure_head()?;
        Ok(this)
    }

    /// Attach to the list at `path`; the file must exist and already hold
    /// the sentinel head its creator allocated.
    pub fn open(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, LinkedListError> {
        let region = RawRegion::open(path, capacity, raw_node_layout(&layout))?;
        let this = Self::assemble(region, layout);
        if this.region.is_empty() {
            return Err(LinkedListError::Region(RegionError::LayoutMismatch));
        }
        Ok(this)
    }

    /// Allocate the sentinel head at slot 0 when the region is fresh. Its
    /// `next` and `prev` point at itself, so an empty list is a one-node
    /// ring and every splice has a neighbor to touch.
    fn ensure_head(&self) -> Result<(), LinkedListError> {
        if !self.region.is_empty() {
            return Ok(());
        }
        // SAFETY: the fill writes the two link words inside the node.
        let index = unsafe {
            self.region.allocate_with(|slot| {
                write_link(slot, self.next_offset, HEAD_INDEX, HEAD_INDEX);
            })
        }?;
        if index != HEAD_INDEX {
            return Err(LinkedListError::Region(RegionError::LayoutMismatch));
        }
        Ok(())
    }

    /// Nodes the list can hold, the sentinel head among them.
    pub fn capacity(&self) -> usize {
        self.region.capacity()
    }

    /// The value layout this handle opened the list with.
    pub fn layout(&self) -> ElementLayout {
        self.layout
    }

    /// Bytes one node takes, its links included.
    pub fn node_size(&self) -> usize {
        self.node_size
    }

    /// Nodes in the list, the sentinel head excluded.
    pub fn len(&self) -> usize {
        self.region.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The links of the node at `index`.
    fn links(&self, index: u32) -> Result<(u32, u32), LinkedListError> {
        let node = self.region.element_ptr(index)?;
        // SAFETY: the region bounds-checked the index, and the geometry puts
        // the links inside the node.
        Ok(unsafe { read_link(node, self.next_offset) })
    }

    /// Point the node at `index` at `next`, leaving its `prev` and value.
    fn set_next(&self, index: u32, next: u32) -> Result<(), LinkedListError> {
        let node = self.region.element_ptr(index)?;
        // SAFETY: as above.
        unsafe { (node.add(self.next_offset) as *mut u32).write(next) };
        Ok(())
    }

    /// Point the node at `index` at `prev`, leaving its `next` and value.
    fn set_prev(&self, index: u32, prev: u32) -> Result<(), LinkedListError> {
        let node = self.region.element_ptr(index)?;
        // SAFETY: as above.
        unsafe { (node.add(self.next_offset + 4) as *mut u32).write(prev) };
        Ok(())
    }

    /// A value argument of exactly the element size.
    fn sized(&self, value: &[u8]) -> Result<(), LinkedListError> {
        if value.len() != self.layout.slot_size {
            return Err(LinkedListError::Region(RegionError::PayloadTooLarge));
        }
        Ok(())
    }

    /// An output buffer of at least the element size.
    fn room(&self, out: &[u8]) -> Result<(), LinkedListError> {
        if out.len() < self.layout.slot_size {
            return Err(LinkedListError::Region(RegionError::PayloadTooLarge));
        }
        Ok(())
    }

    /// Allocate a node holding `value` with the links `next` and `prev`,
    /// written straight into its slot.
    fn allocate_node(&self, value: &[u8], next: u32, prev: u32) -> Result<u32, LinkedListError> {
        let size = self.layout.slot_size;
        let next_offset = self.next_offset;
        // SAFETY: the fill writes the value and the two link words, which
        // the geometry places inside the node.
        let index = unsafe {
            self.region.allocate_with(|slot| {
                std::ptr::copy_nonoverlapping(value.as_ptr(), slot, size);
                write_link(slot, next_offset, next, prev);
            })
        }?;
        Ok(index)
    }

    /// Add `value` at the front and return its index.
    pub fn push_front(&self, value: &[u8]) -> Result<u32, LinkedListError> {
        self.sized(value)?;
        let (old_first, _) = self.links(HEAD_INDEX)?;
        let index = self.allocate_node(value, old_first, HEAD_INDEX)?;
        if old_first != HEAD_INDEX {
            self.set_prev(old_first, index)?;
        } else {
            self.set_prev(HEAD_INDEX, index)?;
        }
        self.set_next(HEAD_INDEX, index)?;
        Ok(index)
    }

    /// Add `value` at the back and return its index.
    pub fn push_back(&self, value: &[u8]) -> Result<u32, LinkedListError> {
        self.sized(value)?;
        let (_, old_last) = self.links(HEAD_INDEX)?;
        let index = self.allocate_node(value, HEAD_INDEX, old_last)?;
        if old_last != HEAD_INDEX {
            self.set_next(old_last, index)?;
        } else {
            self.set_next(HEAD_INDEX, index)?;
        }
        self.set_prev(HEAD_INDEX, index)?;
        Ok(index)
    }

    /// Remove the first node into `out`, at least one element long:
    /// `Ok(true)` with its value copied, `Ok(false)` when the list is
    /// empty.
    pub fn pop_front(&self, out: &mut [u8]) -> Result<bool, LinkedListError> {
        self.room(out)?;
        let (first, _) = self.links(HEAD_INDEX)?;
        if first == HEAD_INDEX {
            return Ok(false);
        }
        self.remove_at(first, out)?;
        Ok(true)
    }

    /// Remove the last node into `out`, at least one element long.
    pub fn pop_back(&self, out: &mut [u8]) -> Result<bool, LinkedListError> {
        self.room(out)?;
        let (_, last) = self.links(HEAD_INDEX)?;
        if last == HEAD_INDEX {
            return Ok(false);
        }
        self.remove_at(last, out)?;
        Ok(true)
    }

    /// Remove the node at `index` into `out`, at least one element long.
    /// The sentinel head and an index past the capacity are
    /// `InvalidHandle`; an index whose node was already removed names a
    /// slot the region has handed out again, which this cannot see.
    pub fn remove(&self, index: u32, out: &mut [u8]) -> Result<(), LinkedListError> {
        self.room(out)?;
        if index == NIL_INDEX || index == HEAD_INDEX || index as usize >= self.capacity() {
            return Err(LinkedListError::InvalidHandle);
        }
        self.remove_at(index, out)
    }

    /// Splice the node at `index` out of the ring, copy its value into
    /// `out` and free its slot.
    fn remove_at(&self, index: u32, out: &mut [u8]) -> Result<(), LinkedListError> {
        let node = self.region.element_ptr(index)?;
        // SAFETY: the region bounds-checked the index; the value lies at the
        // node's front and the links past it.
        let (next, prev) = unsafe {
            std::ptr::copy_nonoverlapping(node, out.as_mut_ptr(), self.layout.slot_size);
            read_link(node, self.next_offset)
        };
        if prev == HEAD_INDEX {
            self.set_next(HEAD_INDEX, next)?;
            if next == HEAD_INDEX {
                self.set_prev(HEAD_INDEX, HEAD_INDEX)?;
            }
        } else {
            self.set_next(prev, next)?;
        }
        if next == HEAD_INDEX {
            self.set_prev(HEAD_INDEX, prev)?;
            if prev == HEAD_INDEX {
                self.set_next(HEAD_INDEX, HEAD_INDEX)?;
            }
        } else {
            self.set_prev(next, prev)?;
        }
        self.region.free_slot(index)?;
        Ok(())
    }

    /// Copy the value at `index` into `out`, at least one element long.
    pub fn get(&self, index: u32, out: &mut [u8]) -> Result<(), LinkedListError> {
        self.room(out)?;
        if index == NIL_INDEX || index == HEAD_INDEX || index as usize >= self.capacity() {
            return Err(LinkedListError::InvalidHandle);
        }
        let node = self.region.element_ptr(index)?;
        // SAFETY: the region bounds-checked the index and the value lies at
        // the node's front.
        unsafe { std::ptr::copy_nonoverlapping(node, out.as_mut_ptr(), self.layout.slot_size) };
        Ok(())
    }

    /// Overwrite the value at `index`, leaving the node where it is.
    pub fn set(&self, index: u32, value: &[u8]) -> Result<(), LinkedListError> {
        self.sized(value)?;
        if index == NIL_INDEX || index == HEAD_INDEX || index as usize >= self.capacity() {
            return Err(LinkedListError::InvalidHandle);
        }
        let node = self.region.element_ptr(index)?;
        // SAFETY: as above; the links sit past the value and are untouched.
        unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), node, self.layout.slot_size) };
        Ok(())
    }

    /// The first node's index, or the head's own index when the list is
    /// empty.
    pub fn first(&self) -> Result<u32, LinkedListError> {
        Ok(self.links(HEAD_INDEX)?.0)
    }

    /// The last node's index, or the head's own index when the list is
    /// empty.
    pub fn last(&self) -> Result<u32, LinkedListError> {
        Ok(self.links(HEAD_INDEX)?.1)
    }

    /// The index after `index`, which is the head's when `index` is the
    /// last node: a walk ends when it comes back to the head.
    pub fn next_of(&self, index: u32) -> Result<u32, LinkedListError> {
        if index == NIL_INDEX || index as usize >= self.capacity() {
            return Err(LinkedListError::InvalidHandle);
        }
        Ok(self.links(index)?.0)
    }

    /// The index before `index`, which is the head's when `index` is the
    /// first node.
    pub fn prev_of(&self, index: u32) -> Result<u32, LinkedListError> {
        if index == NIL_INDEX || index as usize >= self.capacity() {
            return Err(LinkedListError::InvalidHandle);
        }
        Ok(self.links(index)?.1)
    }

    pub fn flush(&self) -> Result<(), LinkedListError> {
        self.region.flush()?;
        Ok(())
    }
}

/// The `next` and `prev` a node carries. The geometry puts both on a
/// four-byte boundary inside the node.
///
/// # Safety
/// `node` addresses a node of this list's geometry.
unsafe fn read_link(node: *const u8, next_offset: usize) -> (u32, u32) {
    // SAFETY: the caller guarantees the node spans its links.
    unsafe {
        (
            (node.add(next_offset) as *const u32).read(),
            (node.add(next_offset + 4) as *const u32).read(),
        )
    }
}

/// Write `next` and `prev` into a node.
///
/// # Safety
/// `node` addresses a node of this list's geometry.
unsafe fn write_link(node: *mut u8, next_offset: usize, next: u32, prev: u32) {
    // SAFETY: the caller guarantees the node spans its links.
    unsafe {
        (node.add(next_offset) as *mut u32).write(next);
        (node.add(next_offset + 4) as *mut u32).write(prev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_linked_list::{NodeHandle, SharedLinkedList};

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-raw-list-{name}-{}.bin", std::process::id()))
    }

    const BYTES_16: ElementLayout = ElementLayout { slot_size: 16, alignment: 4, tag: 21 };

    fn value(i: u8) -> [u8; 16] {
        let mut v = [i; 16];
        v[15] = i.wrapping_add(1);
        v
    }

    /// Every value in the list, walked from the front.
    fn forward(list: &RawLinkedList) -> Vec<[u8; 16]> {
        let mut out = Vec::new();
        let mut at = list.first().unwrap();
        let mut buf = [0u8; 16];
        while at != HEAD_INDEX {
            list.get(at, &mut buf).unwrap();
            out.push(buf);
            at = list.next_of(at).unwrap();
        }
        out
    }

    /// Every value in the list, walked from the back.
    fn backward(list: &RawLinkedList) -> Vec<[u8; 16]> {
        let mut out = Vec::new();
        let mut at = list.last().unwrap();
        let mut buf = [0u8; 16];
        while at != HEAD_INDEX {
            list.get(at, &mut buf).unwrap();
            out.push(buf);
            at = list.prev_of(at).unwrap();
        }
        out
    }

    #[test]
    fn pushes_pops_and_a_removal_from_the_middle_keep_the_ring_whole() {
        let path = tmp("ring");
        let list = RawLinkedList::create(&path, 8, BYTES_16).unwrap();
        assert!(list.is_empty());
        assert_eq!(list.first().unwrap(), HEAD_INDEX);
        assert_eq!(list.last().unwrap(), HEAD_INDEX);
        let mut out = [0u8; 16];
        assert!(!list.pop_front(&mut out).unwrap());
        assert!(!list.pop_back(&mut out).unwrap());

        let b = list.push_back(&value(2)).unwrap();
        let a = list.push_front(&value(1)).unwrap();
        let c = list.push_back(&value(3)).unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(forward(&list), vec![value(1), value(2), value(3)]);
        assert_eq!(backward(&list), vec![value(3), value(2), value(1)]);

        list.remove(b, &mut out).unwrap();
        assert_eq!(out, value(2));
        assert_eq!(list.len(), 2);
        assert_eq!(forward(&list), vec![value(1), value(3)]);
        assert_eq!(backward(&list), vec![value(3), value(1)]);

        list.set(a, &value(9)).unwrap();
        list.get(a, &mut out).unwrap();
        assert_eq!(out, value(9));
        assert_eq!(list.remove(HEAD_INDEX, &mut out).unwrap_err(), LinkedListError::InvalidHandle);
        assert_eq!(list.remove(NIL_INDEX, &mut out).unwrap_err(), LinkedListError::InvalidHandle);
        assert_eq!(list.get(8, &mut out).unwrap_err(), LinkedListError::InvalidHandle);
        assert_eq!(list.set(a, &[0u8; 15]).unwrap_err(), LinkedListError::Region(RegionError::PayloadTooLarge));

        assert!(list.pop_front(&mut out).unwrap());
        assert_eq!(out, value(9));
        assert!(list.pop_back(&mut out).unwrap());
        assert_eq!(out, value(3));
        assert!(list.is_empty());
        assert_eq!(list.first().unwrap(), HEAD_INDEX);
        assert_eq!(list.last().unwrap(), HEAD_INDEX);
        assert_eq!(c, 3, "the third push took the slot after the head and the first two");
    }

    #[test]
    fn a_full_list_refuses_the_next_push_and_a_removed_slot_returns() {
        let path = tmp("full");
        let list = RawLinkedList::create(&path, 3, BYTES_16).unwrap();
        let a = list.push_back(&value(1)).unwrap();
        list.push_back(&value(2)).unwrap();
        assert_eq!(
            list.push_back(&value(3)).unwrap_err(),
            LinkedListError::Region(RegionError::Full),
            "the head takes one of the three slots"
        );
        let mut out = [0u8; 16];
        list.remove(a, &mut out).unwrap();
        assert_eq!(list.push_back(&value(4)).unwrap(), a, "the freed slot is handed out again");
        assert_eq!(forward(&list), vec![value(2), value(4)]);
    }

    #[test]
    fn a_typed_list_and_a_raw_one_share_a_region() {
        let path = tmp("interop");
        let typed: SharedLinkedList<[u8; 16]> = SharedLinkedList::create(&path, 8).unwrap();
        let first = typed.push_back(*b"sixteen bytes!!!").unwrap();
        let raw = RawLinkedList::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 0 }).unwrap();
        assert_eq!(raw.len(), 1);
        let mut out = [0u8; 16];
        raw.get(first.index, &mut out).unwrap();
        assert_eq!(&out, b"sixteen bytes!!!");
        let added = raw.push_back(b"raw side wrote!!").unwrap();
        assert_eq!(typed.len(), 2);
        assert_eq!(typed.get(NodeHandle::new(added)), Some(*b"raw side wrote!!"));
        assert_eq!(typed.iter_forward(), vec![*b"sixteen bytes!!!", *b"raw side wrote!!"]);
        raw.remove(first.index, &mut out).unwrap();
        assert_eq!(typed.iter_forward(), vec![*b"raw side wrote!!"]);
    }

    #[test]
    fn the_node_geometry_matches_the_typed_one() {
        assert_eq!(raw_node_geometry(&BYTES_16), (16, 24, 4));
        assert_eq!(std::mem::size_of::<crate::shared_linked_list::Node<[u8; 16]>>(), 24);
        let wide = ElementLayout { slot_size: 20, alignment: 8, tag: 0 };
        assert_eq!(raw_node_geometry(&wide), (20, 32, 8));
        assert_eq!(std::mem::size_of::<crate::shared_linked_list::Node<[u64; 2]>>(), 24);
        let ragged = ElementLayout { slot_size: 3, alignment: 1, tag: 0 };
        assert_eq!(raw_node_geometry(&ragged), (4, 12, 4));
    }

    #[test]
    fn a_list_of_fewer_than_two_slots_has_no_room_for_a_node() {
        let path = tmp("tiny");
        assert_eq!(
            RawLinkedList::create(&path, 1, BYTES_16).unwrap_err(),
            LinkedListError::Region(RegionError::LayoutMismatch)
        );
        assert!(!path.exists());
    }
}
