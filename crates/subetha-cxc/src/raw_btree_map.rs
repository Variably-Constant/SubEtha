//! `RawBTreeMap`: the ordered map of
//! [`SharedBTreeMap`](crate::shared_btree_map::SharedBTreeMap) at key and
//! value sizes fixed at run time instead of by type parameters, for a
//! caller reaching the map from another language.
//!
//! # What orders the keys
//!
//! Keys compare as unsigned bytes, left to right, for the shorter of the
//! two lengths and then by length, which is `memcmp` order. Every process
//! and every language agrees on it without a comparator crossing the
//! boundary, and it is the same order on every platform. A caller that
//! wants numeric order stores its integers big-endian; little-endian bytes
//! sort by their least significant byte first, which is not what anyone
//! means by a range.
//!
//! # Why it is not the typed map's region
//!
//! The typed map orders by `K: Ord`, which for a multi-byte integer is
//! numeric rather than byte order, so the two would put the same keys in
//! different places in the same tree. They carry different magics and
//! neither will open the other's file.
//!
//! # Shape
//!
//! A B-tree of minimum degree [`T`], so a node holds up to [`B`] keys and
//! [`B`] + 1 children. Insert splits full children on the way down, so one
//! pass never overflows. A global seqlock makes a structural change
//! visible atomically to readers: a writer leaves the version odd while it
//! works, and a reader that sees an odd or changed version searches again.
//! One writer, any number of readers, as the typed map has it.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::Ordering;

use memmap2::{MmapMut, MmapOptions};

use crate::shared_btree_map::{BTreeError, BTreeHeader, B, MAX_TREE_DEPTH, NIL, T};

/// Which end of a subtree an entry is wanted from.
#[derive(Clone, Copy)]
enum Edge {
    Smallest,
    Largest,
}

/// Format tag: a raw map's file is never opened as a typed one, because
/// the two order their keys differently.
pub const RAW_BTREE_MAGIC: u64 = 0x5241_5742_5452_4545; // "RAWBTREE"

/// Where a node keeps its parts, for keys and values of the given sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeGeometry {
    /// Where the keys start; the count and the leaf flag sit before it.
    pub keys: usize,
    /// Where the child indices start.
    pub children: usize,
    /// Where the values start.
    pub values: usize,
    /// Bytes from one node to the next.
    pub stride: usize,
}

/// The geometry of a node holding `key_size`-byte keys and
/// `value_size`-byte values.
pub fn node_geometry(key_size: usize, value_size: usize) -> NodeGeometry {
    let keys = 8;
    let children = (keys + B * key_size).next_multiple_of(4);
    let values = children + (B + 1) * size_of::<u32>();
    let stride = (values + B * value_size).next_multiple_of(8);
    NodeGeometry { keys, children, values, stride }
}

/// Bytes the file holds for `capacity` nodes of this geometry.
pub fn raw_btree_file_size(capacity: usize, key_size: usize, value_size: usize) -> usize {
    size_of::<BTreeHeader>() + capacity * node_geometry(key_size, value_size).stride
}

pub struct RawBTreeMap {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    key_size: usize,
    value_size: usize,
    geometry: NodeGeometry,
    tag: u64,
}

unsafe impl Send for RawBTreeMap {}
unsafe impl Sync for RawBTreeMap {}

impl std::fmt::Debug for RawBTreeMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawBTreeMap")
            .field("capacity", &self.capacity)
            .field("key_size", &self.key_size)
            .field("value_size", &self.value_size)
            .field("stride", &self.geometry.stride)
            .finish()
    }
}

impl RawBTreeMap {
    /// A key and value the region can hold: a non-empty key, and sizes
    /// that fit the header's 32-bit fields.
    fn check_layout(key_size: usize, value_size: usize) -> Result<(), BTreeError> {
        if key_size == 0 || key_size > u32::MAX as usize || value_size > u32::MAX as usize {
            return Err(BTreeError::InvalidConfig);
        }
        Ok(())
    }

    /// Obtain the map at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. A file built
    /// with another capacity, key size, value size or tag is a
    /// `LayoutMismatch`.
    pub fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        key_size: usize,
        value_size: usize,
        tag: u64,
    ) -> Result<Self, BTreeError> {
        Self::check_layout(key_size, value_size)?;
        if capacity < 1 {
            return Err(BTreeError::InvalidConfig);
        }
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            raw_btree_file_size(capacity, key_size, value_size),
            |ptr| unsafe { Self::init_region(ptr, capacity, key_size, value_size, tag) },
            |ptr| unsafe { (*(ptr as *const BTreeHeader)).magic == RAW_BTREE_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, BTreeError::LayoutMismatch))?;
        Self::from_region(file, mmap, capacity, key_size, value_size, tag)
    }

    /// Truncate the map at `path` and initialize an empty one, discarding
    /// the tree live peers share.
    pub fn reset(
        path: impl AsRef<Path>,
        capacity: usize,
        key_size: usize,
        value_size: usize,
        tag: u64,
    ) -> Result<Self, BTreeError> {
        Self::check_layout(key_size, value_size)?;
        if capacity < 1 {
            return Err(BTreeError::InvalidConfig);
        }
        let (file, mmap) = crate::mmf_attach::reset(
            path.as_ref(),
            raw_btree_file_size(capacity, key_size, value_size),
            |ptr| unsafe { Self::init_region(ptr, capacity, key_size, value_size, tag) },
        )?;
        Self::from_region(file, mmap, capacity, key_size, value_size, tag)
    }

    /// Attach to the map at `path`; the file must exist.
    pub fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        key_size: usize,
        value_size: usize,
        tag: u64,
    ) -> Result<Self, BTreeError> {
        Self::check_layout(key_size, value_size)?;
        let total = raw_btree_file_size(capacity, key_size, value_size);
        let file = crate::region_file::open_existing(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(BTreeError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::from_region(file, mmap, capacity, key_size, value_size, tag)
    }

    /// Lay out an empty map: the sizes and an empty root first, magic
    /// last, because attachers spin on it.
    ///
    /// # Safety
    /// `ptr` addresses at least `raw_btree_file_size(..)` writable zeroed
    /// bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, key_size: usize, value_size: usize, tag: u64) {
        let hdr = ptr as *mut BTreeHeader;
        unsafe {
            (*hdr).capacity = capacity as u64;
            (*hdr).key_size = key_size as u32;
            (*hdr).value_size = value_size as u32;
            (*hdr).layout_tag = tag;
            std::ptr::write(&raw mut (*hdr).root, std::sync::atomic::AtomicU32::new(NIL));
            std::ptr::write(&raw mut (*hdr).free_head, std::sync::atomic::AtomicU32::new(NIL));
            std::ptr::write_volatile(&raw mut (*hdr).magic, RAW_BTREE_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one built to another shape.
    fn from_region(
        file: File,
        mmap: MmapMut,
        capacity: usize,
        key_size: usize,
        value_size: usize,
        tag: u64,
    ) -> Result<Self, BTreeError> {
        let hdr = unsafe { &*(mmap.as_ptr() as *const BTreeHeader) };
        if hdr.magic != RAW_BTREE_MAGIC
            || hdr.capacity != capacity as u64
            || hdr.key_size as usize != key_size
            || hdr.value_size as usize != value_size
            || hdr.layout_tag != tag
        {
            return Err(BTreeError::LayoutMismatch);
        }
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            key_size,
            value_size,
            geometry: node_geometry(key_size, value_size),
            tag,
        })
    }

    #[inline]
    fn header(&self) -> &BTreeHeader {
        unsafe { &*(self.mmap.as_ptr() as *const BTreeHeader) }
    }

    #[inline]
    fn node(&self, idx: u32) -> *mut u8 {
        let off = size_of::<BTreeHeader>() + idx as usize * self.geometry.stride;
        unsafe { self.mmap.as_ptr().add(off) as *mut u8 }
    }

    /// Keys a node holds, clamped to what its arrays can carry so a torn
    /// read under a concurrent writer never indexes past them.
    #[inline]
    fn count(&self, idx: u32) -> usize {
        (unsafe { (self.node(idx) as *const u16).read() } as usize).min(B)
    }

    #[inline]
    fn set_count(&self, idx: u32, count: usize) {
        unsafe { (self.node(idx) as *mut u16).write(count as u16) };
    }

    #[inline]
    fn is_leaf(&self, idx: u32) -> bool {
        unsafe { self.node(idx).add(2).read() != 0 }
    }

    #[inline]
    fn set_leaf(&self, idx: u32, leaf: bool) {
        unsafe { self.node(idx).add(2).write(leaf as u8) };
    }

    #[inline]
    fn key_ptr(&self, idx: u32, i: usize) -> *mut u8 {
        unsafe { self.node(idx).add(self.geometry.keys + i * self.key_size) }
    }

    #[inline]
    fn value_ptr(&self, idx: u32, i: usize) -> *mut u8 {
        unsafe { self.node(idx).add(self.geometry.values + i * self.value_size) }
    }

    #[inline]
    fn child_ptr(&self, idx: u32, i: usize) -> *mut u32 {
        unsafe { self.node(idx).add(self.geometry.children + i * size_of::<u32>()) as *mut u32 }
    }

    #[inline]
    fn key(&self, idx: u32, i: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.key_ptr(idx, i), self.key_size) }
    }

    #[inline]
    fn child(&self, idx: u32, i: usize) -> u32 {
        unsafe { self.child_ptr(idx, i).read() }
    }

    #[inline]
    fn set_child(&self, idx: u32, i: usize, to: u32) {
        unsafe { self.child_ptr(idx, i).write(to) };
    }

    /// Copy entry `from` of node `src` over entry `to` of node `dst`.
    fn copy_entry(&self, dst: u32, to: usize, src: u32, from: usize) {
        unsafe {
            std::ptr::copy(self.key_ptr(src, from), self.key_ptr(dst, to), self.key_size);
            std::ptr::copy(self.value_ptr(src, from), self.value_ptr(dst, to), self.value_size);
        }
    }

    fn write_entry(&self, idx: u32, i: usize, key: &[u8], value: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), self.key_ptr(idx, i), self.key_size);
            std::ptr::copy_nonoverlapping(value.as_ptr(), self.value_ptr(idx, i), self.value_size);
        }
    }

    /// Nodes the map can hold.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn key_size(&self) -> usize {
        self.key_size
    }

    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// The tag this handle opened the map with.
    pub fn layout_tag(&self) -> u64 {
        self.tag
    }

    /// Where a node keeps its parts.
    pub fn geometry(&self) -> NodeGeometry {
        self.geometry
    }

    /// Entries in the map.
    pub fn len(&self) -> usize {
        self.header().len.load(Ordering::Acquire) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Nodes handed out, the free list included.
    pub fn node_count(&self) -> usize {
        self.header().node_count.load(Ordering::Acquire) as usize
    }

    /// A key argument of exactly the key size.
    fn sized_key(&self, key: &[u8]) -> Result<(), BTreeError> {
        if key.len() != self.key_size {
            return Err(BTreeError::InvalidConfig);
        }
        Ok(())
    }

    /// A value argument of exactly the value size.
    fn sized_value(&self, value: &[u8]) -> Result<(), BTreeError> {
        if value.len() != self.value_size {
            return Err(BTreeError::InvalidConfig);
        }
        Ok(())
    }

    /// Allocate a node: the free list first, then the bump cursor. One
    /// writer, so the free list needs no counter against reuse races.
    fn alloc_node(&self, leaf: bool) -> Result<u32, BTreeError> {
        let h = self.header();
        let free = h.free_head.load(Ordering::Acquire);
        let idx = if free != NIL {
            let next = self.child(free, 0);
            h.free_head.store(next, Ordering::Release);
            free
        } else {
            let idx = h.node_count.fetch_add(1, Ordering::AcqRel);
            if idx as usize >= self.capacity {
                h.node_count.fetch_sub(1, Ordering::AcqRel);
                return Err(BTreeError::Full);
            }
            idx
        };
        self.set_count(idx, 0);
        self.set_leaf(idx, leaf);
        for i in 0..=B {
            self.set_child(idx, i, NIL);
        }
        Ok(idx)
    }

    /// Return a node to the free list, linked through its first child.
    fn free_node(&self, idx: u32) {
        let h = self.header();
        let head = h.free_head.load(Ordering::Acquire);
        self.set_child(idx, 0, head);
        h.free_head.store(idx, Ordering::Release);
    }

    /// The version is odd while a structural change is in flight.
    #[inline]
    fn begin_write(&self) {
        self.header().version.fetch_add(1, Ordering::AcqRel);
    }

    #[inline]
    fn end_write(&self) {
        self.header().version.fetch_add(1, Ordering::Release);
    }

    /// Where `key` sits in node `idx`, and whether it is there: on a miss
    /// the position is the child to descend into, or where a leaf would
    /// take it.
    fn search(&self, idx: u32, key: &[u8]) -> (usize, bool) {
        let count = self.count(idx);
        let (mut lo, mut hi) = (0usize, count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(idx, mid).cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return (mid, true),
            }
        }
        (lo, false)
    }

    /// Copy `key`'s value into `out`, at least the value size:
    /// `Ok(true)` with it copied, `Ok(false)` when the key is absent. A
    /// lock-free read that retries while a writer is changing the tree.
    pub fn get(&self, key: &[u8], out: &mut [u8]) -> Result<bool, BTreeError> {
        self.sized_key(key)?;
        if out.len() < self.value_size {
            return Err(BTreeError::InvalidConfig);
        }
        let h = self.header();
        let cap = self.capacity as u32;
        loop {
            let before = h.version.load(Ordering::Acquire);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let mut idx = h.root.load(Ordering::Acquire);
            let mut found = false;
            let mut torn = false;
            while idx != NIL {
                if idx >= cap {
                    torn = true;
                    break;
                }
                let (pos, hit) = self.search(idx, key);
                if hit {
                    unsafe {
                        std::ptr::copy_nonoverlapping(self.value_ptr(idx, pos), out.as_mut_ptr(), self.value_size)
                    };
                    found = true;
                    break;
                }
                if self.is_leaf(idx) {
                    break;
                }
                idx = self.child(idx, pos);
            }
            if h.version.load(Ordering::Acquire) == before && !torn {
                return Ok(found);
            }
            std::hint::spin_loop();
        }
    }

    /// Whether `key` has an entry.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, BTreeError> {
        let mut scratch = vec![0u8; self.value_size];
        self.get(key, &mut scratch)
    }

    /// Insert or update. One writer: serialize this against other writers
    /// yourself. `Ok(true)` when it replaced a value, which it copies into
    /// `previous` when the caller wants it; a caller that does not passes
    /// `None` and reads only whether it replaced.
    pub fn insert(&self, key: &[u8], value: &[u8], mut previous: Option<&mut [u8]>) -> Result<bool, BTreeError> {
        self.sized_key(key)?;
        self.sized_value(value)?;
        if previous.as_deref().is_some_and(|p| p.len() < self.value_size) {
            return Err(BTreeError::InvalidConfig);
        }
        self.begin_write();
        let outcome = self.insert_inner(key, value, &mut previous);
        self.end_write();
        outcome
    }

    /// Copy the value at `pos` of node `idx` out, when the caller asked
    /// for it.
    fn take_previous(&self, previous: &mut Option<&mut [u8]>, idx: u32, pos: usize) {
        if let Some(buf) = previous.as_deref_mut() {
            // SAFETY: the position came from a search of this node.
            unsafe { std::ptr::copy_nonoverlapping(self.value_ptr(idx, pos), buf.as_mut_ptr(), self.value_size) };
        }
    }

    fn insert_inner(&self, key: &[u8], value: &[u8], previous: &mut Option<&mut [u8]>) -> Result<bool, BTreeError> {
        let root = self.header().root.load(Ordering::Acquire);
        if root == NIL {
            let fresh = self.alloc_node(true)?;
            self.write_entry(fresh, 0, key, value);
            self.set_count(fresh, 1);
            self.header().root.store(fresh, Ordering::Release);
            self.header().len.fetch_add(1, Ordering::AcqRel);
            return Ok(false);
        }
        let root = if self.count(root) == B {
            let taller = self.alloc_node(false)?;
            self.set_child(taller, 0, root);
            self.set_count(taller, 0);
            self.split_child(taller, 0)?;
            self.header().root.store(taller, Ordering::Release);
            taller
        } else {
            root
        };
        self.insert_nonfull(root, key, value, previous)
    }

    /// Insert into a subtree whose root is known not to be full.
    fn insert_nonfull(
        &self,
        mut idx: u32,
        key: &[u8],
        value: &[u8],
        previous: &mut Option<&mut [u8]>,
    ) -> Result<bool, BTreeError> {
        loop {
            let (pos, found) = self.search(idx, key);
            if found {
                self.take_previous(previous, idx, pos);
                // SAFETY: the position came from a search of this node.
                unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), self.value_ptr(idx, pos), self.value_size) };
                return Ok(true);
            }
            if self.is_leaf(idx) {
                let count = self.count(idx);
                let mut j = count;
                while j > pos {
                    self.copy_entry(idx, j, idx, j - 1);
                    j -= 1;
                }
                self.write_entry(idx, pos, key, value);
                self.set_count(idx, count + 1);
                self.header().len.fetch_add(1, Ordering::AcqRel);
                return Ok(false);
            }
            let child = self.child(idx, pos);
            if self.count(child) == B {
                self.split_child(idx, pos)?;
                // The median the split promoted now sits at `pos`.
                match self.key(idx, pos).cmp(key) {
                    std::cmp::Ordering::Equal => {
                        self.take_previous(previous, idx, pos);
                        // SAFETY: the position came from a search of this node.
                        unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), self.value_ptr(idx, pos), self.value_size) };
                        return Ok(true);
                    }
                    std::cmp::Ordering::Less => idx = self.child(idx, pos + 1),
                    std::cmp::Ordering::Greater => idx = self.child(idx, pos),
                }
            } else {
                idx = child;
            }
        }
    }

    /// Split the full child at `parent`'s slot `i` in two, promoting its
    /// median into the parent at `i`.
    fn split_child(&self, parent: u32, i: usize) -> Result<(), BTreeError> {
        let full = self.child(parent, i);
        let leaf = self.is_leaf(full);
        let right = self.alloc_node(leaf)?;

        for j in 0..(T - 1) {
            self.copy_entry(right, j, full, T + j);
        }
        if !leaf {
            for j in 0..T {
                self.set_child(right, j, self.child(full, T + j));
            }
        }
        self.set_count(right, T - 1);
        self.set_leaf(right, leaf);

        // The median leaves the left node as the count falls; publish that
        // count last so a reader never sees it twice.
        let mut median_key = vec![0u8; self.key_size];
        let mut median_value = vec![0u8; self.value_size];
        unsafe {
            std::ptr::copy_nonoverlapping(self.key_ptr(full, T - 1), median_key.as_mut_ptr(), self.key_size);
            std::ptr::copy_nonoverlapping(self.value_ptr(full, T - 1), median_value.as_mut_ptr(), self.value_size);
        }
        self.set_count(full, T - 1);

        let count = self.count(parent);
        let mut j = count;
        while j > i {
            self.set_child(parent, j + 1, self.child(parent, j));
            j -= 1;
        }
        self.set_child(parent, i + 1, right);
        let mut j = count;
        while j > i {
            self.copy_entry(parent, j, parent, j - 1);
            j -= 1;
        }
        self.write_entry(parent, i, &median_key, &median_value);
        self.set_count(parent, count + 1);
        Ok(())
    }

    /// Remove `key`, copying the value it held into `out` when it is long
    /// enough. `Ok(true)` when it was there. One writer, as insert is.
    pub fn remove(&self, key: &[u8], out: &mut [u8]) -> Result<bool, BTreeError> {
        self.sized_key(key)?;
        if out.len() < self.value_size {
            return Err(BTreeError::InvalidConfig);
        }
        self.begin_write();
        let removed = self.remove_inner(key, out);
        self.end_write();
        Ok(removed)
    }

    fn remove_inner(&self, key: &[u8], out: &mut [u8]) -> bool {
        let root = self.header().root.load(Ordering::Acquire);
        if root == NIL {
            return false;
        }
        let removed = self.delete_from(root, key, out);
        // The root can empty; then the tree is one level shorter, or gone.
        if self.count(root) == 0 {
            if self.is_leaf(root) {
                self.header().root.store(NIL, Ordering::Release);
            } else {
                self.header().root.store(self.child(root, 0), Ordering::Release);
            }
            self.free_node(root);
        }
        if removed {
            self.header().len.fetch_sub(1, Ordering::AcqRel);
        }
        removed
    }

    /// Delete `key` from the subtree at `idx`, which holds at least `T`
    /// keys or is the root: from a leaf outright, and from an internal node
    /// by taking the neighboring entry that can spare itself, or by
    /// merging when neither can.
    fn delete_from(&self, idx: u32, key: &[u8], out: &mut [u8]) -> bool {
        let (pos, found) = self.search(idx, key);
        if found {
            unsafe { std::ptr::copy_nonoverlapping(self.value_ptr(idx, pos), out.as_mut_ptr(), self.value_size) };
            if self.is_leaf(idx) {
                let count = self.count(idx);
                for j in pos..count - 1 {
                    self.copy_entry(idx, j, idx, j + 1);
                }
                self.set_count(idx, count - 1);
                return true;
            }
            let left = self.child(idx, pos);
            let right = self.child(idx, pos + 1);
            let mut scratch = vec![0u8; self.value_size];
            if self.count(left) >= T {
                let borrowed = self.edge_entry(left, Edge::Largest);
                self.copy_entry(idx, pos, borrowed.0, borrowed.1);
                let taken = self.key(idx, pos).to_vec();
                self.delete_from(left, &taken, &mut scratch);
            } else if self.count(right) >= T {
                let borrowed = self.edge_entry(right, Edge::Smallest);
                self.copy_entry(idx, pos, borrowed.0, borrowed.1);
                let taken = self.key(idx, pos).to_vec();
                self.delete_from(right, &taken, &mut scratch);
            } else {
                self.merge_at(idx, pos);
                self.delete_from(left, key, &mut scratch);
            }
            return true;
        }
        if self.is_leaf(idx) {
            return false;
        }
        let child = self.ensure_min_degree(idx, pos);
        self.delete_from(child, key, out)
    }

    /// Where the largest or smallest entry of the subtree at `idx` lives,
    /// as a node and a position in it.
    fn edge_entry(&self, mut idx: u32, edge: Edge) -> (u32, usize) {
        loop {
            if self.is_leaf(idx) {
                return match edge {
                    Edge::Largest => (idx, self.count(idx) - 1),
                    Edge::Smallest => (idx, 0),
                };
            }
            idx = match edge {
                Edge::Largest => self.child(idx, self.count(idx)),
                Edge::Smallest => self.child(idx, 0),
            };
        }
    }

    /// Give `parent`'s child at `i` at least `T` keys before descending
    /// into it, by taking one from a sibling or merging with one, and
    /// return the child to descend into.
    fn ensure_min_degree(&self, parent: u32, i: usize) -> u32 {
        let child = self.child(parent, i);
        if self.count(child) >= T {
            return child;
        }
        let count = self.count(parent);
        if i > 0 && self.count(self.child(parent, i - 1)) >= T {
            self.borrow_from_left(parent, i);
            return child;
        }
        if i < count && self.count(self.child(parent, i + 1)) >= T {
            self.borrow_from_right(parent, i);
            return child;
        }
        if i < count {
            self.merge_at(parent, i);
            self.child(parent, i)
        } else {
            self.merge_at(parent, i - 1);
            self.child(parent, i - 1)
        }
    }

    /// Move the separator down into the child and the left sibling's
    /// largest entry up into its place.
    fn borrow_from_left(&self, parent: u32, i: usize) {
        let child = self.child(parent, i);
        let left = self.child(parent, i - 1);
        let count = self.count(child);
        let internal = !self.is_leaf(child);
        let mut j = count;
        while j > 0 {
            self.copy_entry(child, j, child, j - 1);
            j -= 1;
        }
        if internal {
            let mut j = count + 1;
            while j > 0 {
                self.set_child(child, j, self.child(child, j - 1));
                j -= 1;
            }
        }
        self.copy_entry(child, 0, parent, i - 1);
        let left_count = self.count(left);
        if internal {
            self.set_child(child, 0, self.child(left, left_count));
        }
        self.copy_entry(parent, i - 1, left, left_count - 1);
        self.set_count(left, left_count - 1);
        self.set_count(child, count + 1);
    }

    /// Move the separator down into the child and the right sibling's
    /// smallest entry up into its place.
    fn borrow_from_right(&self, parent: u32, i: usize) {
        let child = self.child(parent, i);
        let right = self.child(parent, i + 1);
        let count = self.count(child);
        let internal = !self.is_leaf(child);
        self.copy_entry(child, count, parent, i);
        if internal {
            self.set_child(child, count + 1, self.child(right, 0));
        }
        self.copy_entry(parent, i, right, 0);
        let right_count = self.count(right);
        for j in 0..right_count - 1 {
            self.copy_entry(right, j, right, j + 1);
        }
        if internal {
            for j in 0..right_count {
                self.set_child(right, j, self.child(right, j + 1));
            }
        }
        self.set_count(right, right_count - 1);
        self.set_count(child, count + 1);
    }

    /// Fold `children[i]`, the separator at `keys[i]` and `children[i+1]`
    /// into one node, freeing the right one and dropping the separator.
    fn merge_at(&self, parent: u32, i: usize) {
        let left = self.child(parent, i);
        let right = self.child(parent, i + 1);
        let left_count = self.count(left);
        let internal = !self.is_leaf(left);
        self.copy_entry(left, left_count, parent, i);
        let right_count = self.count(right);
        for j in 0..right_count {
            self.copy_entry(left, left_count + 1 + j, right, j);
        }
        if internal {
            for j in 0..=right_count {
                self.set_child(left, left_count + 1 + j, self.child(right, j));
            }
        }
        self.set_count(left, left_count + 1 + right_count);
        let count = self.count(parent);
        for j in i..count - 1 {
            self.copy_entry(parent, j, parent, j + 1);
        }
        for j in i + 1..count {
            self.set_child(parent, j, self.child(parent, j + 1));
        }
        self.set_count(parent, count - 1);
        self.free_node(right);
    }

    /// Copy the smallest key and its value into `key_out` and `value_out`,
    /// each at least its own size: `Ok(true)` when the map holds anything.
    pub fn first(&self, key_out: &mut [u8], value_out: &mut [u8]) -> Result<bool, BTreeError> {
        if key_out.len() < self.key_size || value_out.len() < self.value_size {
            return Err(BTreeError::InvalidConfig);
        }
        let root = self.header().root.load(Ordering::Acquire);
        if root == NIL {
            return Ok(false);
        }
        let (node, pos) = self.edge_entry(root, Edge::Smallest);
        unsafe {
            std::ptr::copy_nonoverlapping(self.key_ptr(node, pos), key_out.as_mut_ptr(), self.key_size);
            std::ptr::copy_nonoverlapping(self.value_ptr(node, pos), value_out.as_mut_ptr(), self.value_size);
        }
        Ok(true)
    }

    /// Copy the largest key and its value out, as [`first`](Self::first).
    pub fn last(&self, key_out: &mut [u8], value_out: &mut [u8]) -> Result<bool, BTreeError> {
        if key_out.len() < self.key_size || value_out.len() < self.value_size {
            return Err(BTreeError::InvalidConfig);
        }
        let root = self.header().root.load(Ordering::Acquire);
        if root == NIL {
            return Ok(false);
        }
        let (node, pos) = self.edge_entry(root, Edge::Largest);
        unsafe {
            std::ptr::copy_nonoverlapping(self.key_ptr(node, pos), key_out.as_mut_ptr(), self.key_size);
            std::ptr::copy_nonoverlapping(self.value_ptr(node, pos), value_out.as_mut_ptr(), self.value_size);
        }
        Ok(true)
    }

    /// Mark every node free and the tree empty.
    pub fn clear(&self) {
        self.begin_write();
        let h = self.header();
        h.root.store(NIL, Ordering::Release);
        h.free_head.store(NIL, Ordering::Release);
        h.node_count.store(0, Ordering::Release);
        h.len.store(0, Ordering::Release);
        self.end_write();
    }

    /// Whether a key at or past the low bound.
    fn at_or_above_low(k: &[u8], low: std::ops::Bound<&[u8]>) -> bool {
        match low {
            std::ops::Bound::Unbounded => true,
            std::ops::Bound::Included(b) => k >= b,
            std::ops::Bound::Excluded(b) => k > b,
        }
    }

    /// Whether a key past the high bound, which ends the walk.
    fn above_high(k: &[u8], high: std::ops::Bound<&[u8]>) -> bool {
        match high {
            std::ops::Bound::Unbounded => false,
            std::ops::Bound::Included(b) => k > b,
            std::ops::Bound::Excluded(b) => k >= b,
        }
    }

    /// Whether the subtree left of `k` can hold anything in range. It
    /// holds keys below `k`, so only a `k` above the low bound admits it.
    fn low_admits_below(k: &[u8], low: std::ops::Bound<&[u8]>) -> bool {
        match low {
            std::ops::Bound::Unbounded => true,
            std::ops::Bound::Included(b) | std::ops::Bound::Excluded(b) => k > b,
        }
    }

    /// The entries in `low..high` in key order, at most `limit` of them,
    /// each as its own key and value bytes.
    ///
    /// A snapshot of each entry, not of the range: an entry can change
    /// after the walk passes it. A point-in-time view of a whole range
    /// needs writers held off for the scan, which is the caller's to
    /// arrange.
    ///
    /// `limit` bounds the work one call does, and with it the cost of a
    /// seqlock retry. An unbounded walk retries from the start on every
    /// concurrent insert, so over a large range it need never finish.
    pub fn range(
        &self,
        low: std::ops::Bound<&[u8]>,
        high: std::ops::Bound<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let h = self.header();
        let cap = self.capacity as u32;
        loop {
            let v1 = h.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue; // a writer is mid-mutation
            }
            let mut out = Vec::new();
            let mut torn = false;
            let mut stop = false;
            let root = h.root.load(Ordering::Acquire);
            if root != NIL {
                self.walk_range(
                    root, low, high, limit, &mut out, cap, MAX_TREE_DEPTH, &mut torn, &mut stop,
                );
            }
            let v2 = h.version.load(Ordering::Acquire);
            if v1 == v2 && !torn {
                return out;
            }
            out.clear();
            std::hint::spin_loop();
        }
    }

    /// In-order walk restricted to `low..high`, stopping at `limit`.
    ///
    /// Every node index is bounds-guarded and the descent carries a depth
    /// budget, so a torn read under a concurrent writer cannot deref out
    /// of range or follow a cycle; either sets `torn` and the caller's
    /// version re-check discards the partial result.
    #[allow(clippy::too_many_arguments)]
    fn walk_range(
        &self,
        idx: u32,
        low: std::ops::Bound<&[u8]>,
        high: std::ops::Bound<&[u8]>,
        limit: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
        cap: u32,
        depth: u32,
        torn: &mut bool,
        stop: &mut bool,
    ) {
        if *torn || *stop {
            return;
        }
        if idx >= cap || depth == 0 {
            *torn = true;
            return;
        }
        let count = self.count(idx);
        let is_leaf = self.is_leaf(idx);
        for i in 0..count {
            let k = self.key(idx, i).to_vec();
            if !is_leaf && Self::low_admits_below(&k, low) {
                let c = self.child(idx, i);
                self.walk_range(c, low, high, limit, out, cap, depth - 1, torn, stop);
                if *torn || *stop {
                    return;
                }
            }
            if Self::above_high(&k, high) {
                *stop = true;
                return;
            }
            if Self::at_or_above_low(&k, low) {
                let mut value = vec![0u8; self.value_size];
                // The node index is bounds-checked above and `i` is below
                // the clamped count, so the slot is inside the region.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.value_ptr(idx, i),
                        value.as_mut_ptr(),
                        self.value_size,
                    );
                }
                out.push((k, value));
                if out.len() >= limit {
                    *stop = true;
                    return;
                }
            }
        }
        // The subtree right of the last key.
        if !is_leaf {
            let c = self.child(idx, count);
            self.walk_range(c, low, high, limit, out, cap, depth - 1, torn, stop);
        }
    }

    pub fn flush(&self) -> Result<(), BTreeError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-raw-btree-{name}-{}.bin", std::process::id()))
    }

    /// A four-byte key in the order the map sorts by: big-endian, so the
    /// bytes rise with the number.
    fn key_of(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }

    /// A map holding `n` big-endian keys, each valued with the same number
    /// so a walk's order can be read off the values too.
    fn filled(name: &str, n: u32) -> (RawBTreeMap, crate::test_paths::TmpFile) {
        let path = tmp(name);
        let map = RawBTreeMap::create(&path, 256, 4, 4, 0).expect("the map is created");
        for i in 0..n {
            map.insert(&key_of(i), &key_of(i), None).expect("a key lands");
        }
        (map, path)
    }

    fn numbers(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u32> {
        entries
            .iter()
            .map(|(k, _)| u32::from_be_bytes(k[..].try_into().expect("a four-byte key")))
            .collect()
    }

    #[test]
    fn a_range_walks_the_whole_map_in_key_order_across_several_nodes() {
        // Past one node's worth, so the walk has to descend rather than
        // read a single leaf.
        let (map, _p) = filled("range-all", 200);
        let all = map.range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, 1000);
        assert_eq!(all.len(), 200, "every key is walked");
        assert_eq!(numbers(&all), (0..200).collect::<Vec<_>>(), "in key order");
        assert_eq!(all[7].1, key_of(7), "each key keeps its own value");
    }

    #[test]
    fn a_range_honours_both_bounds_and_the_limit() {
        let (map, _p) = filled("range-bounds", 100);
        let lo = key_of(10);
        let hi = key_of(20);

        let included = map.range(
            std::ops::Bound::Included(&lo[..]),
            std::ops::Bound::Included(&hi[..]),
            1000,
        );
        assert_eq!(numbers(&included), (10..=20).collect::<Vec<_>>());

        let excluded = map.range(
            std::ops::Bound::Excluded(&lo[..]),
            std::ops::Bound::Excluded(&hi[..]),
            1000,
        );
        assert_eq!(numbers(&excluded), (11..20).collect::<Vec<_>>());

        let limited = map.range(
            std::ops::Bound::Included(&lo[..]),
            std::ops::Bound::Unbounded,
            5,
        );
        assert_eq!(numbers(&limited), (10..15).collect::<Vec<_>>(), "the limit stops the walk");

        assert!(
            map.range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, 0).is_empty(),
            "a limit of zero walks nothing"
        );
    }

    #[test]
    fn the_geometry_puts_the_parts_where_a_node_can_hold_them() {
        let g = node_geometry(4, 8);
        assert_eq!(g.keys, 8);
        assert_eq!(g.children, 8 + 15 * 4);
        assert_eq!(g.values, g.children + 16 * 4);
        assert_eq!(g.stride, (g.values + 15 * 8).next_multiple_of(8));
        // A key size that leaves the children unaligned is padded up.
        let odd = node_geometry(3, 1);
        assert_eq!(odd.children % 4, 0);
        assert!(odd.children >= 8 + 15 * 3);
    }

    #[test]
    fn entries_come_back_and_an_absent_key_says_so() {
        let path = tmp("round-trip");
        let map = RawBTreeMap::create(&path, 64, 4, 8, 1).unwrap();
        assert!(map.is_empty());
        let mut out = [0u8; 8];
        let mut previous = [0u8; 8];
        assert!(!map.get(&key_of(1), &mut out).unwrap());
        assert!(!map.insert(&key_of(1), &[1u8; 8], Some(&mut previous)).unwrap());
        assert_eq!(map.len(), 1);
        assert!(map.get(&key_of(1), &mut out).unwrap());
        assert_eq!(out, [1u8; 8]);
        assert!(map.insert(&key_of(1), &[2u8; 8], Some(&mut previous)).unwrap(), "the second insert replaces");
        assert_eq!(previous, [1u8; 8]);
        assert_eq!(map.len(), 1, "a replacement is not a new entry");
        assert!(map.get(&key_of(1), &mut out).unwrap());
        assert_eq!(out, [2u8; 8]);
        assert!(map.contains_key(&key_of(1)).unwrap());
        assert!(!map.contains_key(&key_of(2)).unwrap());
        assert_eq!(map.insert(&[0u8; 3], &[0u8; 8], Some(&mut previous)).unwrap_err(), BTreeError::InvalidConfig);
        assert_eq!(map.insert(&key_of(1), &[0u8; 7], Some(&mut previous)).unwrap_err(), BTreeError::InvalidConfig);
        assert_eq!(map.get(&key_of(1), &mut [0u8; 7]).unwrap_err(), BTreeError::InvalidConfig);
        // A caller that does not want the value it replaced says so.
        assert!(map.insert(&key_of(1), &[3u8; 8], None).unwrap(), "still a replacement");
        assert!(map.get(&key_of(1), &mut out).unwrap());
        assert_eq!(out, [3u8; 8]);
        assert!(!map.insert(&key_of(2), &[4u8; 8], None).unwrap(), "and a fresh key is still fresh");
    }

    #[test]
    fn a_tree_deeper_than_one_node_keeps_every_key() {
        let path = tmp("deep");
        let map = RawBTreeMap::create(&path, 256, 4, 8, 0).unwrap();
        let mut previous = [0u8; 8];
        // Enough to split the root more than once: B is 15.
        for n in 0..500u32 {
            assert!(!map.insert(&key_of(n), &u64::from(n).to_le_bytes(), Some(&mut previous)).unwrap(), "{n} is new");
        }
        assert_eq!(map.len(), 500);
        assert!(map.node_count() > 1, "the tree grew past one node");
        let mut out = [0u8; 8];
        for n in 0..500u32 {
            assert!(map.get(&key_of(n), &mut out).unwrap(), "{n} is present");
            assert_eq!(u64::from_le_bytes(out), u64::from(n));
        }
        assert!(!map.get(&key_of(500), &mut out).unwrap());
    }

    #[test]
    fn keys_are_ordered_as_bytes_so_big_endian_numbers_sort_numerically() {
        let path = tmp("order");
        let map = RawBTreeMap::create(&path, 64, 4, 4, 0).unwrap();
        let mut previous = [0u8; 4];
        // Inserted out of order, including a pair whose little-endian bytes
        // would sort the other way round.
        for n in [300u32, 1, 256, 2, 65536] {
            map.insert(&key_of(n), &n.to_be_bytes(), Some(&mut previous)).unwrap();
        }
        // A walk of the leftmost path reaches the smallest key.
        let root = map.header().root.load(Ordering::Acquire);
        let mut idx = root;
        while !map.is_leaf(idx) {
            idx = map.child(idx, 0);
        }
        assert_eq!(map.key(idx, 0), key_of(1), "the smallest key sits leftmost");
    }

    #[test]
    fn a_second_handle_reads_what_the_first_wrote_and_another_shape_is_refused() {
        let path = tmp("attach");
        let map = RawBTreeMap::create(&path, 64, 4, 8, 7).unwrap();
        let mut previous = [0u8; 8];
        map.insert(&key_of(42), &[9u8; 8], Some(&mut previous)).unwrap();
        let again = RawBTreeMap::open(&path, 64, 4, 8, 7).unwrap();
        let mut out = [0u8; 8];
        assert!(again.get(&key_of(42), &mut out).unwrap());
        assert_eq!(out, [9u8; 8]);
        assert_eq!(RawBTreeMap::open(&path, 64, 4, 8, 8).unwrap_err(), BTreeError::LayoutMismatch);
        assert_eq!(RawBTreeMap::open(&path, 64, 8, 8, 7).unwrap_err(), BTreeError::LayoutMismatch);
        assert_eq!(RawBTreeMap::open(&path, 32, 4, 8, 7).unwrap_err(), BTreeError::LayoutMismatch);
        assert_eq!(RawBTreeMap::create(&path, 64, 0, 8, 7).unwrap_err(), BTreeError::InvalidConfig);
    }

    #[test]
    fn a_full_map_refuses_the_insert_that_needs_a_node_it_has_not_got() {
        let path = tmp("full");
        let map = RawBTreeMap::create(&path, 1, 4, 4, 0).unwrap();
        let mut previous = [0u8; 4];
        for n in 0..B as u32 {
            map.insert(&key_of(n), &n.to_be_bytes(), Some(&mut previous)).unwrap();
        }
        assert_eq!(map.len(), B);
        assert_eq!(
            map.insert(&key_of(B as u32), &[0u8; 4], Some(&mut previous)).unwrap_err(),
            BTreeError::Full,
            "the root is full and there is no node for the split"
        );
    }

    #[test]
    fn a_removal_takes_the_entry_and_leaves_the_rest_findable() {
        let path = tmp("remove-small");
        let map = RawBTreeMap::create(&path, 64, 4, 8, 0).unwrap();
        let mut previous = [0u8; 8];
        let mut out = [0u8; 8];
        assert!(!map.remove(&key_of(1), &mut out).unwrap(), "an absent key removes nothing");
        for n in 0..10u32 {
            map.insert(&key_of(n), &u64::from(n).to_le_bytes(), Some(&mut previous)).unwrap();
        }
        assert!(map.remove(&key_of(4), &mut out).unwrap());
        assert_eq!(u64::from_le_bytes(out), 4);
        assert_eq!(map.len(), 9);
        assert!(!map.get(&key_of(4), &mut out).unwrap());
        assert!(!map.remove(&key_of(4), &mut out).unwrap(), "the second removal finds nothing");
        for n in (0..10u32).filter(|n| *n != 4) {
            assert!(map.get(&key_of(n), &mut out).unwrap(), "{n} survived");
            assert_eq!(u64::from_le_bytes(out), u64::from(n));
        }
    }

    #[test]
    fn every_key_can_be_removed_from_a_deep_tree_in_any_order() {
        let path = tmp("remove-deep");
        let map = RawBTreeMap::create(&path, 512, 4, 8, 0).unwrap();
        let mut previous = [0u8; 8];
        let mut out = [0u8; 8];
        const COUNT: u32 = 400;
        for n in 0..COUNT {
            map.insert(&key_of(n), &u64::from(n).to_le_bytes(), Some(&mut previous)).unwrap();
        }
        assert!(map.node_count() > 3, "the tree has interior nodes to merge and borrow from");

        // Take them out in an order that walks the tree rather than
        // draining one end: every third, then the rest.
        let mut removed = 0u64;
        for n in (0..COUNT).step_by(3) {
            assert!(map.remove(&key_of(n), &mut out).unwrap(), "{n} was there");
            assert_eq!(u64::from_le_bytes(out), u64::from(n));
            removed += 1;
            assert_eq!(map.len() as u64, u64::from(COUNT) - removed);
        }
        for n in 0..COUNT {
            let expected = !n.is_multiple_of(3);
            assert_eq!(map.get(&key_of(n), &mut out).unwrap(), expected, "{n} after the first pass");
        }
        for n in 0..COUNT {
            if n.is_multiple_of(3) {
                continue;
            }
            assert!(map.remove(&key_of(n), &mut out).unwrap(), "{n} was still there");
            assert_eq!(u64::from_le_bytes(out), u64::from(n));
        }
        assert!(map.is_empty(), "every key came out");
        for n in 0..COUNT {
            assert!(!map.get(&key_of(n), &mut out).unwrap(), "{n} is gone");
        }
        // An empty tree takes entries again.
        map.insert(&key_of(7), &7u64.to_le_bytes(), Some(&mut previous)).unwrap();
        assert!(map.get(&key_of(7), &mut out).unwrap());
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn the_ends_of_the_map_are_its_smallest_and_largest_keys() {
        let path = tmp("ends");
        let map = RawBTreeMap::create(&path, 128, 4, 8, 0).unwrap();
        let mut previous = [0u8; 8];
        let mut key_out = [0u8; 4];
        let mut value_out = [0u8; 8];
        assert!(!map.first(&mut key_out, &mut value_out).unwrap(), "an empty map has no ends");
        assert!(!map.last(&mut key_out, &mut value_out).unwrap());
        for n in [50u32, 3, 900, 1, 77, 65536] {
            map.insert(&key_of(n), &u64::from(n).to_le_bytes(), Some(&mut previous)).unwrap();
        }
        assert!(map.first(&mut key_out, &mut value_out).unwrap());
        assert_eq!(u32::from_be_bytes(key_out), 1);
        assert_eq!(u64::from_le_bytes(value_out), 1);
        assert!(map.last(&mut key_out, &mut value_out).unwrap());
        assert_eq!(u32::from_be_bytes(key_out), 65536);
        assert_eq!(u64::from_le_bytes(value_out), 65536);
        let mut out = [0u8; 8];
        map.remove(&key_of(1), &mut out).unwrap();
        assert!(map.first(&mut key_out, &mut value_out).unwrap());
        assert_eq!(u32::from_be_bytes(key_out), 3, "the next smallest takes its place");
    }

    #[test]
    fn a_removal_hands_its_nodes_back_for_reuse() {
        let path = tmp("reuse");
        // A node below the root carries at least T - 1 keys, so 300 of them
        // need about 43 nodes at worst; 128 leaves the fill room to spare.
        let map = RawBTreeMap::create(&path, 128, 4, 4, 0).unwrap();
        let mut previous = [0u8; 4];
        let mut out = [0u8; 4];
        for n in 0..300u32 {
            map.insert(&key_of(n), &n.to_be_bytes(), Some(&mut previous)).unwrap();
        }
        let high_water = map.node_count();
        for n in 0..300u32 {
            map.remove(&key_of(n), &mut out).unwrap();
        }
        assert!(map.is_empty());
        // Refilling reuses the freed nodes rather than asking for more.
        for n in 0..300u32 {
            map.insert(&key_of(n), &n.to_be_bytes(), Some(&mut previous)).unwrap();
        }
        assert_eq!(map.node_count(), high_water, "the second fill took no node the first had not");
        assert_eq!(map.len(), 300);
    }

    #[test]
    fn a_clear_empties_the_map_for_every_handle() {
        let path = tmp("clear");
        let map = RawBTreeMap::create(&path, 64, 4, 4, 0).unwrap();
        let again = RawBTreeMap::open(&path, 64, 4, 4, 0).unwrap();
        let mut previous = [0u8; 4];
        for n in 0..40u32 {
            map.insert(&key_of(n), &n.to_be_bytes(), Some(&mut previous)).unwrap();
        }
        assert_eq!(again.len(), 40);
        map.clear();
        assert!(again.is_empty());
        let mut out = [0u8; 4];
        assert!(!again.get(&key_of(1), &mut out).unwrap());
        map.insert(&key_of(7), &7u32.to_be_bytes(), Some(&mut previous)).unwrap();
        assert!(again.get(&key_of(7), &mut out).unwrap());
        assert_eq!(u32::from_be_bytes(out), 7);
    }
}
