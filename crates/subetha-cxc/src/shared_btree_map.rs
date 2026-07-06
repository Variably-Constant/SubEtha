//! `SharedBTreeMap` - cross-process MMF B-tree ordered map.
//!
//! A cross-process MMF ordered key/value map with a cache-friendly layout:
//! each node packs up to `B` sorted keys (fanout `B + 1`), so a lookup
//! touches ~`log_{B+1}(N)` nodes. The per-node binary search reads a
//! contiguous key array (prefetcher-friendly) rather than chasing scattered
//! single-cache-line nodes, which is what wins once the map far exceeds L3
//! and lookups go to RAM. It is the substrate's ordered-map primitive.
//!
//! Minimum degree `T = 8` => up to `B = 2T - 1 = 15` keys per node, `2T = 16`
//! children. Insert uses CLRS proactive top-down splitting (full children
//! are split before descent), so it is single-pass and never overflows.
//!
//! Storage: self-contained MMF `[BTreeHeader | BTreeNode array]` with bump
//! allocation. Concurrency model: single-writer for `insert` / `remove`
//! (serialise externally); reads are consistent
//! against a quiescent tree (build-then-query), which is what the cold
//! benchmark exercises.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

/// Minimum degree.
pub const T: usize = 8;
/// Max keys per node.
pub const B: usize = 2 * T - 1; // 15
/// Sentinel "no node".
pub const NIL: u32 = u32::MAX;

pub const BTREE_MAGIC: u64 = 0x4254_5245_454D_4150; // "BTREEMAP"

#[repr(C, align(64))]
pub struct BTreeHeader {
    pub magic: u64,
    pub root: AtomicU32,
    pub node_count: AtomicU32,
    pub capacity: u64,
    pub len: AtomicU64,
    /// Head of the single-writer free list (NIL = empty); merged/removed
    /// nodes are recycled here so deletes reclaim slots.
    pub free_head: AtomicU32,
    _pad0: u32,
    /// Global seqlock. A writer makes it odd for the duration of a
    /// structural mutation (insert/remove) and even after; readers retry
    /// the whole search if it changes or is odd, so concurrent reads never
    /// observe a torn tree. Single-writer, multi-reader.
    pub version: AtomicU64,
    _pad: [u8; 16],
}

const _: () = {
    assert!(size_of::<BTreeHeader>() == 64);
};

#[repr(C)]
pub struct BTreeNode<K: Copy + Ord + Default + 'static, V: Copy + Default + 'static> {
    pub count: u16,
    pub is_leaf: u8,
    _pad: [u8; 5],
    pub keys: [K; B],
    pub children: [u32; B + 1],
    pub values: [V; B],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BTreeError {
    Full,
    LayoutMismatch,
    InvalidConfig,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for BTreeError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.kind())
    }
}

pub fn btree_file_size<K, V>(capacity: usize) -> usize
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    size_of::<BTreeHeader>() + capacity * size_of::<BTreeNode<K, V>>()
}

pub struct SharedBTreeMap<K: Copy + Ord + Default + 'static, V: Copy + Default + 'static> {
    _file: File,
    mmap: MmapMut,
    raw_ptr: *mut u8,
    capacity: usize,
    _phantom: std::marker::PhantomData<(K, V)>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<K: Copy + Ord + Default + Send + 'static, V: Copy + Default + Send + 'static> Send
    for SharedBTreeMap<K, V>
{
}
unsafe impl<K: Copy + Ord + Default + Sync + 'static, V: Copy + Default + Sync + 'static> Sync
    for SharedBTreeMap<K, V>
{
}

impl<K: Copy + Ord + Default + Send + Sync + 'static, V: Copy + Default + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for SharedBTreeMap<K, V>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<K: Copy + Ord + Default + 'static, V: Copy + Default + 'static> SharedBTreeMap<K, V> {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, BTreeError> {
        if capacity < 1 {
            return Err(BTreeError::InvalidConfig);
        }
        let total = btree_file_size::<K, V>(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut BTreeHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<BTreeHeader>());
            (*hdr).magic = BTREE_MAGIC;
            (*hdr).root = AtomicU32::new(NIL);
            (*hdr).node_count = AtomicU32::new(0);
            (*hdr).capacity = capacity as u64;
            (*hdr).len = AtomicU64::new(0);
            (*hdr).free_head = AtomicU32::new(NIL);
            (*hdr).version = AtomicU64::new(0);
        }
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _file: file, mmap, raw_ptr, capacity,
            _phantom: std::marker::PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, BTreeError> {
        let total = btree_file_size::<K, V>(expected_capacity);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(BTreeError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const BTreeHeader) };
        if hdr.magic != BTREE_MAGIC || hdr.capacity != expected_capacity as u64 {
            return Err(BTreeError::LayoutMismatch);
        }
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _file: file, mmap, raw_ptr, capacity: expected_capacity,
            _phantom: std::marker::PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    fn header(&self) -> &BTreeHeader {
        unsafe { &*(self.raw_ptr as *const BTreeHeader) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.header().len.load(Ordering::Acquire) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of nodes bump-allocated so far (resident-memory witness:
    /// `node_count() * size_of::<BTreeNode>()`).
    #[inline]
    pub fn node_count(&self) -> usize {
        self.header().node_count.load(Ordering::Acquire) as usize
    }

    #[inline]
    fn node(&self, idx: u32) -> *mut BTreeNode<K, V> {
        let off = size_of::<BTreeHeader>() + idx as usize * size_of::<BTreeNode<K, V>>();
        unsafe { self.raw_ptr.add(off) as *mut BTreeNode<K, V> }
    }

    /// Allocate a node: recycle from the free list, else bump-allocate.
    /// Single-writer, so the free list needs no ABA protection.
    fn alloc_node(&self, is_leaf: bool) -> Result<u32, BTreeError> {
        let h = self.header();
        let free = h.free_head.load(Ordering::Acquire);
        let idx = if free != NIL {
            let next = unsafe { (*self.node(free)).children[0] };
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
        let n = self.node(idx);
        unsafe {
            (*n).count = 0;
            (*n).is_leaf = is_leaf as u8;
            (*n).children = [NIL; B + 1];
        }
        Ok(idx)
    }

    /// Return a node to the free list (linked via `children[0]`).
    fn free_node(&self, idx: u32) {
        let h = self.header();
        let head = h.free_head.load(Ordering::Acquire);
        unsafe { (*self.node(idx)).children[0] = head; }
        h.free_head.store(idx, Ordering::Release);
    }

    /// Enter / leave a structural mutation. The global version is odd while
    /// a write is in flight; readers retry the whole search if they observe
    /// an odd or changed version (seqlock). Single-writer.
    #[inline]
    fn begin_write(&self) {
        self.header().version.fetch_add(1, Ordering::AcqRel);
    }
    #[inline]
    fn end_write(&self) {
        self.header().version.fetch_add(1, Ordering::Release);
    }

    /// Binary search `key` within node `idx`. Returns `(pos, found)`: when
    /// found, `keys[pos] == key`; otherwise `pos` is the child slot / leaf
    /// insertion position. `count` is clamped to `B` so a torn read under a
    /// concurrent writer can never index past the fixed-size arrays.
    #[inline]
    fn search(&self, idx: u32, key: &K) -> (usize, bool) {
        let n = self.node(idx);
        let count = (unsafe { (*n).count } as usize).min(B);
        let (mut lo, mut hi) = (0usize, count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let mk = unsafe { (*n).keys[mid] };
            match mk.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return (mid, true),
            }
        }
        (lo, false)
    }

    /// Look up `key`. Lock-free read against a quiescent tree.
    pub fn get(&self, key: &K) -> Option<V> {
        let r = self.get_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ordered::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn get_inner(&self, key: &K) -> Option<V> {
        let h = self.header();
        let cap = self.capacity as u32;
        loop {
            let v1 = h.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue; // a writer is mid-mutation
            }
            // Descend. Every node index is bounds-guarded so a torn read
            // (a concurrent split moving keys) cannot deref out of range;
            // `search` clamps `count`. If anything looks inconsistent we
            // simply finish and let the version re-check force a retry.
            let mut idx = h.root.load(Ordering::Acquire);
            let mut result: Option<V> = None;
            let mut torn = false;
            while idx != NIL {
                if idx >= cap {
                    torn = true;
                    break;
                }
                let (pos, found) = self.search(idx, key);
                let n = self.node(idx);
                if found {
                    result = Some(unsafe { (*n).values[pos] });
                    break;
                }
                if unsafe { (*n).is_leaf } != 0 {
                    break;
                }
                idx = unsafe { (*n).children[pos] };
            }
            let v2 = h.version.load(Ordering::Acquire);
            if v1 == v2 && !torn {
                return result;
            }
            std::hint::spin_loop();
        }
    }

    /// True if `key` is present.
    pub fn contains_key(&self, key: &K) -> bool {
        self.get_inner(key).is_some()
    }

    /// Insert / update. Single-writer (serialise externally). Returns the
    /// previous value if `key` was present.
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>, BTreeError> {
        self.begin_write();
        let r = self.insert_inner(key, value);
        self.end_write();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ordered::OP_INSERT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn insert_inner(&self, key: K, value: V) -> Result<Option<V>, BTreeError> {
        let root = self.header().root.load(Ordering::Acquire);
        if root == NIL {
            let r = self.alloc_node(true)?;
            let n = self.node(r);
            unsafe {
                (*n).keys[0] = key;
                (*n).values[0] = value;
                (*n).count = 1;
            }
            self.header().root.store(r, Ordering::Release);
            self.header().len.fetch_add(1, Ordering::AcqRel);
            return Ok(None);
        }
        // Grow height if the root is full.
        let root = if unsafe { (*self.node(root)).count as usize } == B {
            let new_root = self.alloc_node(false)?;
            unsafe {
                (*self.node(new_root)).children[0] = root;
                (*self.node(new_root)).count = 0;
            }
            self.split_child(new_root, 0)?;
            self.header().root.store(new_root, Ordering::Release);
            new_root
        } else {
            root
        };
        self.insert_nonfull(root, key, value)
    }

    /// Insert into a guaranteed-non-full subtree rooted at `idx`.
    fn insert_nonfull(&self, mut idx: u32, key: K, value: V) -> Result<Option<V>, BTreeError> {
        loop {
            let (pos, found) = self.search(idx, &key);
            let n = self.node(idx);
            if found {
                let old = unsafe { (*n).values[pos] };
                unsafe { (*n).values[pos] = value; }
                return Ok(Some(old));
            }
            if unsafe { (*n).is_leaf } != 0 {
                let count = unsafe { (*n).count as usize };
                unsafe {
                    let mut j = count;
                    while j > pos {
                        (*n).keys[j] = (*n).keys[j - 1];
                        (*n).values[j] = (*n).values[j - 1];
                        j -= 1;
                    }
                    (*n).keys[pos] = key;
                    (*n).values[pos] = value;
                    (*n).count = (count + 1) as u16;
                }
                self.header().len.fetch_add(1, Ordering::AcqRel);
                return Ok(None);
            }
            let child = unsafe { (*n).children[pos] };
            if unsafe { (*self.node(child)).count as usize } == B {
                self.split_child(idx, pos)?;
                // After split, the promoted median sits at keys[pos].
                let n = self.node(idx);
                let med = unsafe { (*n).keys[pos] };
                match key.cmp(&med) {
                    std::cmp::Ordering::Equal => {
                        let old = unsafe { (*n).values[pos] };
                        unsafe { (*n).values[pos] = value; }
                        return Ok(Some(old));
                    }
                    std::cmp::Ordering::Greater => idx = unsafe { (*n).children[pos + 1] },
                    std::cmp::Ordering::Less => idx = unsafe { (*n).children[pos] },
                }
            } else {
                idx = child;
            }
        }
    }

    /// Split the full child at `parent.children[i]` into two, promoting the
    /// median (key+value) into `parent` at position `i`.
    fn split_child(&self, parent: u32, i: usize) -> Result<(), BTreeError> {
        let full = unsafe { (*self.node(parent)).children[i] };
        let is_leaf = unsafe { (*self.node(full)).is_leaf };
        let right = self.alloc_node(is_leaf != 0)?;

        let fp = self.node(full);
        let rp = self.node(right);
        unsafe {
            // right gets the upper T-1 keys/values.
            for j in 0..(T - 1) {
                (*rp).keys[j] = (*fp).keys[T + j];
                (*rp).values[j] = (*fp).values[T + j];
            }
            if is_leaf == 0 {
                for j in 0..T {
                    (*rp).children[j] = (*fp).children[T + j];
                }
            }
            (*rp).count = (T - 1) as u16;
            (*rp).is_leaf = is_leaf;
        }
        let median_key = unsafe { (*fp).keys[T - 1] };
        let median_value = unsafe { (*fp).values[T - 1] };
        // Left keeps the lower T-1 keys. Publish the new count last so a
        // reader never sees the promoted/duplicated entries on the left.
        unsafe { (*fp).count = (T - 1) as u16; }

        let pp = self.node(parent);
        unsafe {
            let pc = (*pp).count as usize;
            let mut j = pc;
            while j > i {
                (*pp).children[j + 1] = (*pp).children[j];
                j -= 1;
            }
            (*pp).children[i + 1] = right;
            let mut j = pc;
            while j > i {
                (*pp).keys[j] = (*pp).keys[j - 1];
                (*pp).values[j] = (*pp).values[j - 1];
                j -= 1;
            }
            (*pp).keys[i] = median_key;
            (*pp).values[i] = median_value;
            (*pp).count = (pc + 1) as u16;
        }
        Ok(())
    }

    /// Remove `key`, returning its previous value if present. Single-writer
    /// (serialise externally, as with insert).
    pub fn remove(&self, key: &K) -> Result<Option<V>, BTreeError> {
        self.begin_write();
        let r = self.remove_inner(key);
        self.end_write();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ordered::OP_REMOVE,
            if r.is_none() { 2 } else { 0 },
        );
        Ok(r)
    }

    fn remove_inner(&self, key: &K) -> Option<V> {
        let root = self.header().root.load(Ordering::Acquire);
        if root == NIL {
            return None;
        }
        let removed = self.delete_from(root, key);
        // Shrink height if the root emptied.
        let rn = self.node(root);
        if unsafe { (*rn).count } == 0 {
            if unsafe { (*rn).is_leaf } != 0 {
                self.header().root.store(NIL, Ordering::Release);
            } else {
                let new_root = unsafe { (*rn).children[0] };
                self.header().root.store(new_root, Ordering::Release);
            }
            self.free_node(root);
        }
        if removed.is_some() {
            self.header().len.fetch_sub(1, Ordering::AcqRel);
        }
        removed
    }

    /// Delete `key` from the subtree at `idx` (guaranteed >= T keys, or the
    /// root). CLRS deletion: from a leaf directly; from an internal node by
    /// replacing with the in-order predecessor/successor, or merging.
    fn delete_from(&self, idx: u32, key: &K) -> Option<V> {
        let (pos, found) = self.search(idx, key);
        let n = self.node(idx);
        let is_leaf = unsafe { (*n).is_leaf } != 0;
        if found {
            let old = unsafe { (*n).values[pos] };
            if is_leaf {
                let count = unsafe { (*n).count as usize };
                unsafe {
                    for j in pos..count - 1 {
                        (*n).keys[j] = (*n).keys[j + 1];
                        (*n).values[j] = (*n).values[j + 1];
                    }
                    (*n).count = (count - 1) as u16;
                }
            } else {
                let left = unsafe { (*n).children[pos] };
                let right = unsafe { (*n).children[pos + 1] };
                if unsafe { (*self.node(left)).count as usize } >= T {
                    let (pk, pv) = self.max_pair(left);
                    unsafe {
                        (*n).keys[pos] = pk;
                        (*n).values[pos] = pv;
                    }
                    self.delete_from(left, &pk);
                } else if unsafe { (*self.node(right)).count as usize } >= T {
                    let (sk, sv) = self.min_pair(right);
                    unsafe {
                        (*n).keys[pos] = sk;
                        (*n).values[pos] = sv;
                    }
                    self.delete_from(right, &sk);
                } else {
                    self.merge_at(idx, pos);
                    self.delete_from(left, key);
                }
            }
            return Some(old);
        }
        if is_leaf {
            return None;
        }
        let child = self.ensure_min_degree(idx, pos);
        self.delete_from(child, key)
    }

    /// Ensure `parent.children[i]` has >= T keys before descending, by
    /// borrowing from a sibling or merging. Returns the index to descend.
    fn ensure_min_degree(&self, parent: u32, i: usize) -> u32 {
        let p = self.node(parent);
        let child = unsafe { (*p).children[i] };
        if unsafe { (*self.node(child)).count as usize } >= T {
            return child;
        }
        let pcount = unsafe { (*p).count as usize };
        if i > 0 {
            let left = unsafe { (*p).children[i - 1] };
            if unsafe { (*self.node(left)).count as usize } >= T {
                self.borrow_from_left(parent, i);
                return child;
            }
        }
        if i < pcount {
            let right = unsafe { (*p).children[i + 1] };
            if unsafe { (*self.node(right)).count as usize } >= T {
                self.borrow_from_right(parent, i);
                return child;
            }
        }
        if i < pcount {
            self.merge_at(parent, i);
            unsafe { (*self.node(parent)).children[i] }
        } else {
            self.merge_at(parent, i - 1);
            unsafe { (*self.node(parent)).children[i - 1] }
        }
    }

    fn borrow_from_left(&self, parent: u32, i: usize) {
        let p = self.node(parent);
        let child = unsafe { (*p).children[i] };
        let left = unsafe { (*p).children[i - 1] };
        let c = self.node(child);
        let l = self.node(left);
        unsafe {
            let cc = (*c).count as usize;
            let internal = (*c).is_leaf == 0;
            let mut j = cc;
            while j > 0 {
                (*c).keys[j] = (*c).keys[j - 1];
                (*c).values[j] = (*c).values[j - 1];
                j -= 1;
            }
            if internal {
                let mut j = cc + 1;
                while j > 0 {
                    (*c).children[j] = (*c).children[j - 1];
                    j -= 1;
                }
            }
            (*c).keys[0] = (*p).keys[i - 1];
            (*c).values[0] = (*p).values[i - 1];
            let lc = (*l).count as usize;
            if internal {
                (*c).children[0] = (*l).children[lc];
            }
            (*p).keys[i - 1] = (*l).keys[lc - 1];
            (*p).values[i - 1] = (*l).values[lc - 1];
            (*l).count = (lc - 1) as u16;
            (*c).count = (cc + 1) as u16;
        }
    }

    fn borrow_from_right(&self, parent: u32, i: usize) {
        let p = self.node(parent);
        let child = unsafe { (*p).children[i] };
        let right = unsafe { (*p).children[i + 1] };
        let c = self.node(child);
        let r = self.node(right);
        unsafe {
            let cc = (*c).count as usize;
            let internal = (*c).is_leaf == 0;
            (*c).keys[cc] = (*p).keys[i];
            (*c).values[cc] = (*p).values[i];
            if internal {
                (*c).children[cc + 1] = (*r).children[0];
            }
            (*p).keys[i] = (*r).keys[0];
            (*p).values[i] = (*r).values[0];
            let rc = (*r).count as usize;
            for j in 0..rc - 1 {
                (*r).keys[j] = (*r).keys[j + 1];
                (*r).values[j] = (*r).values[j + 1];
            }
            if internal {
                for j in 0..rc {
                    (*r).children[j] = (*r).children[j + 1];
                }
            }
            (*r).count = (rc - 1) as u16;
            (*c).count = (cc + 1) as u16;
        }
    }

    /// Merge `children[i]` + separator `keys[i]` + `children[i+1]` into
    /// `children[i]`, freeing the right node and dropping the separator.
    fn merge_at(&self, parent: u32, i: usize) {
        let p = self.node(parent);
        let left = unsafe { (*p).children[i] };
        let right = unsafe { (*p).children[i + 1] };
        let l = self.node(left);
        let r = self.node(right);
        unsafe {
            let lc = (*l).count as usize;
            let internal = (*l).is_leaf == 0;
            (*l).keys[lc] = (*p).keys[i];
            (*l).values[lc] = (*p).values[i];
            let rc = (*r).count as usize;
            for j in 0..rc {
                (*l).keys[lc + 1 + j] = (*r).keys[j];
                (*l).values[lc + 1 + j] = (*r).values[j];
            }
            if internal {
                for j in 0..=rc {
                    (*l).children[lc + 1 + j] = (*r).children[j];
                }
            }
            (*l).count = (lc + 1 + rc) as u16;
            let pc = (*p).count as usize;
            for j in i..pc - 1 {
                (*p).keys[j] = (*p).keys[j + 1];
                (*p).values[j] = (*p).values[j + 1];
            }
            for j in i + 1..pc {
                (*p).children[j] = (*p).children[j + 1];
            }
            (*p).count = (pc - 1) as u16;
        }
        self.free_node(right);
    }

    fn max_pair(&self, mut idx: u32) -> (K, V) {
        loop {
            let n = self.node(idx);
            let count = unsafe { (*n).count as usize };
            if unsafe { (*n).is_leaf } != 0 {
                return unsafe { ((*n).keys[count - 1], (*n).values[count - 1]) };
            }
            idx = unsafe { (*n).children[count] };
        }
    }

    fn min_pair(&self, mut idx: u32) -> (K, V) {
        loop {
            let n = self.node(idx);
            if unsafe { (*n).is_leaf } != 0 {
                return unsafe { ((*n).keys[0], (*n).values[0]) };
            }
            idx = unsafe { (*n).children[0] };
        }
    }

    /// Smallest (key, value) in the map, or `None` if empty.
    pub fn first(&self) -> Option<(K, V)> {
        let root = self.header().root.load(Ordering::Acquire);
        let r = if root == NIL { None } else { Some(self.min_pair(root)) };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ordered::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Collect all (K, V) in ascending key order (validation / iteration).
    pub fn iter_ascending(&self) -> Vec<(K, V)> {
        let mut out = Vec::with_capacity(self.len());
        let root = self.header().root.load(Ordering::Acquire);
        if root != NIL {
            self.walk(root, &mut out);
        }
        out
    }

    fn walk(&self, idx: u32, out: &mut Vec<(K, V)>) {
        let n = self.node(idx);
        let count = unsafe { (*n).count as usize };
        let is_leaf = unsafe { (*n).is_leaf } != 0;
        for i in 0..count {
            if !is_leaf {
                let c = unsafe { (*n).children[i] };
                self.walk(c, out);
            }
            out.push(unsafe { ((*n).keys[i], (*n).values[i]) });
        }
        if !is_leaf {
            let c = unsafe { (*n).children[count] };
            self.walk(c, out);
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

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("btree_{name}_{}.bin", std::process::id()));
        p
    }

    #[test]
    fn insert_get_round_trip() {
        let p = tmp("rt");
        let m: SharedBTreeMap<u64, u64> = SharedBTreeMap::create(&p, 64).unwrap();
        for i in 0..100u64 {
            assert_eq!(m.insert(i, i * 10).unwrap(), None);
        }
        for i in 0..100u64 {
            assert_eq!(m.get(&i), Some(i * 10));
        }
        assert_eq!(m.get(&1000), None);
        assert_eq!(m.len(), 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn duplicate_insert_updates_and_returns_previous() {
        let p = tmp("dup");
        let m: SharedBTreeMap<u64, u64> = SharedBTreeMap::create(&p, 64).unwrap();
        assert_eq!(m.insert(5, 50).unwrap(), None);
        assert_eq!(m.insert(5, 55).unwrap(), Some(50));
        assert_eq!(m.get(&5), Some(55));
        assert_eq!(m.len(), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn many_inserts_stay_sorted_and_findable() {
        let p = tmp("many");
        let n = 5000u64;
        let m: SharedBTreeMap<u64, u64> = SharedBTreeMap::create(&p, 4096).unwrap();
        // Pseudo-random insertion order.
        let mut x = 0x1234_5678u64;
        let mut inserted = Vec::new();
        for _ in 0..n {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let k = x % 1_000_000;
            if m.insert(k, k.wrapping_mul(3)).unwrap().is_none() {
                inserted.push(k);
            }
        }
        for &k in &inserted {
            assert_eq!(m.get(&k), Some(k.wrapping_mul(3)), "missing {k}");
        }
        // Ascending order holds across all splits.
        let asc = m.iter_ascending();
        for w in asc.windows(2) {
            assert!(w[0].0 < w[1].0, "order broken: {} >= {}", w[0].0, w[1].0);
        }
        assert_eq!(asc.len(), inserted.len());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("xhandle");
        let a: SharedBTreeMap<u64, u64> = SharedBTreeMap::create(&p, 64).unwrap();
        for i in 0..50u64 { a.insert(i, i).unwrap(); }
        let b: SharedBTreeMap<u64, u64> = SharedBTreeMap::open(&p, 64).unwrap();
        for i in 0..50u64 { assert_eq!(b.get(&i), Some(i)); }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn random_ops_match_std_btreemap() {
        use std::collections::BTreeMap;
        let p = tmp("oracle");
        let m: SharedBTreeMap<u64, u64> = SharedBTreeMap::create(&p, 16384).unwrap();
        let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
        let mut x = 0xdead_beef_1234_5678u64;
        let mut rng = || {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17; x
        };
        for _ in 0..80_000 {
            let k = rng() % 3000;
            match rng() % 3 {
                0 | 1 => {
                    let v = rng();
                    assert_eq!(m.insert(k, v).unwrap(), oracle.insert(k, v), "insert {k}");
                }
                _ => {
                    assert_eq!(m.remove(&k).unwrap(), oracle.remove(&k), "remove {k}");
                }
            }
            assert_eq!(m.len(), oracle.len(), "len after op on {k}");
        }
        for (k, v) in &oracle {
            assert_eq!(m.get(k), Some(*v), "final get {k}");
        }
        let asc = m.iter_ascending();
        let oref: Vec<(u64, u64)> = oracle.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(asc, oref, "iteration order / contents mismatch");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn first_returns_smallest() {
        let p = tmp("first");
        let m: SharedBTreeMap<u64, u64> = SharedBTreeMap::create(&p, 1024).unwrap();
        assert_eq!(m.first(), None);
        for i in (0..500u64).rev() {
            m.insert(i, i * 7).unwrap();
        }
        assert_eq!(m.first(), Some((0, 0)));
        m.remove(&0).unwrap();
        assert_eq!(m.first(), Some((1, 7)));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_readers_during_inserts() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        let p = tmp("concurrent");
        let m = Arc::new(SharedBTreeMap::<u64, u64>::create(&p, 16384).unwrap());
        for i in 0..1000u64 {
            m.insert(i, i.wrapping_mul(2)).unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let m = m.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        for i in 0..1000u64 {
                            // Keys 0..1000 are never removed; a reader must
                            // see the exact value or (transiently) retry to
                            // it - never a torn / garbage value.
                            if let Some(v) = m.get(&i) {
                                assert_eq!(v, i.wrapping_mul(2), "torn read at {i}");
                            }
                        }
                    }
                })
            })
            .collect();
        for i in 1000..6000u64 {
            m.insert(i, i.wrapping_mul(2)).unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        std::fs::remove_file(&p).ok();
    }
}
