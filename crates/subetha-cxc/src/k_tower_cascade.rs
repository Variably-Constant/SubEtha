//! `KTowerCascade<T, const DEPTH: usize>` - recursive pow2-of-pow2
//! pointer encoding, the userspace MMU primitive.
//!
//! A `KTowerCascade<T, N>` is `[u32; N]` of indices. Each index at
//! level `i` selects a slot in the level-`i` region; the slot at the
//! upper levels holds the next-level cascade pointer, and the slot
//! at the leaf level holds T itself. Resolution walks N levels of
//! [`SharedRegion`] lookups.
//!
//! # The recursive form
//!
//! ```text
//! KTowerCascade<T, 1> = [u32; 1]          // flat: one region
//! KTowerCascade<T, 2> = [u32; 2]          // (region_id, offset)
//! KTowerCascade<T, 4> = [u32; 4]          // mirrors MMU PML4
//! KTowerCascade<T, N> = [u32; N]          // any depth
//! ```
//!
//! At resolution time, level `i`'s region is a
//! `SharedRegion<KTowerCascade<T, N-i-1>>` if `i < N-1`, or
//! `SharedRegion<T>` if `i == N-1` (the leaf).
//!
//! # Why this matters
//!
//! - **Sparse address spaces**: with DEPTH=4 and u32 per level, the
//!   total addressable space is 2^128 logical slots, but you only
//!   pay storage for the branches you actually populate. Empty
//!   subtrees take zero space.
//! - **Userspace MMU**: this is the same mechanism the hardware MMU
//!   uses (PML4 -> PDPT -> PD -> PT, four levels of 9-bit indices)
//!   lifted to userspace and made position-independent.
//! - **Cross-process**: every level uses INDICES, not virtual
//!   addresses, so the cascade resolves identically in any process
//!   that maps the same N regions.
//! - **Adaptive depth**: callers pick DEPTH per workload. Hot dense
//!   data uses DEPTH=1; cold sparse data uses DEPTH=4 or higher.

use std::marker::PhantomData;
use std::path::Path;

use crate::shared_region::{OffsetPtr, RegionError, SharedRegion};

/// Sentinel NIL: all-ones at every level.
pub const NIL_INDEX: u32 = u32::MAX;

/// Position-independent N-level cascade pointer.
#[derive(Debug)]
#[repr(C)]
pub struct KTowerCascade<T, const DEPTH: usize> {
    pub indices: [u32; DEPTH],
    _phantom: PhantomData<T>,
}

impl<T, const DEPTH: usize> Clone for KTowerCascade<T, DEPTH> {
    fn clone(&self) -> Self { *self }
}
impl<T, const DEPTH: usize> Copy for KTowerCascade<T, DEPTH> {}
impl<T, const DEPTH: usize> PartialEq for KTowerCascade<T, DEPTH> {
    fn eq(&self, other: &Self) -> bool { self.indices == other.indices }
}
impl<T, const DEPTH: usize> Eq for KTowerCascade<T, DEPTH> {}
impl<T, const DEPTH: usize> std::hash::Hash for KTowerCascade<T, DEPTH> {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) { self.indices.hash(s); }
}
// SAFETY: KTowerCascade is a plain `[u32; DEPTH]` with a PhantomData;
// PhantomData<T> normally restricts Send/Sync to follow T, but the
// cascade does not own a T - it is only an index into a region.
// So Send/Sync are unconditional.
unsafe impl<T, const DEPTH: usize> Send for KTowerCascade<T, DEPTH> {}
unsafe impl<T, const DEPTH: usize> Sync for KTowerCascade<T, DEPTH> {}

impl<T, const DEPTH: usize> Default for KTowerCascade<T, DEPTH> {
    fn default() -> Self { Self::NIL }
}

impl<T, const DEPTH: usize> KTowerCascade<T, DEPTH> {
    /// NIL sentinel: every level is u32::MAX.
    pub const NIL: Self = Self {
        indices: [NIL_INDEX; DEPTH],
        _phantom: PhantomData,
    };

    pub const fn new(indices: [u32; DEPTH]) -> Self {
        Self { indices, _phantom: PhantomData }
    }

    /// Construct from raw u32 array (alias of `new` for symmetry
    /// with other primitives).
    pub const fn from_raw(indices: [u32; DEPTH]) -> Self { Self::new(indices) }

    /// Extract the raw u32 array. Stable cross-process representation.
    pub const fn raw(self) -> [u32; DEPTH] { self.indices }

    /// True when every level is NIL.
    pub fn is_nil(self) -> bool {
        self.indices.iter().all(|&i| i == NIL_INDEX)
    }

    /// Index at a specific level (0 = top, DEPTH-1 = leaf).
    pub fn level(self, level: usize) -> u32 {
        self.indices[level]
    }

    /// Convenience: index at the leaf (deepest) level.
    pub fn leaf(self) -> u32 { self.indices[DEPTH - 1] }

    /// Return a new cascade with one level replaced.
    pub fn with_level(mut self, level: usize, value: u32) -> Self {
        self.indices[level] = value;
        self
    }
}

/// Errors during cascade resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeError {
    Region(RegionError),
    NilAtLevel(usize),
    OutOfBounds,
    IoError(std::io::ErrorKind),
}

impl From<RegionError> for CascadeError {
    fn from(e: RegionError) -> Self { Self::Region(e) }
}
impl From<std::io::Error> for CascadeError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

// Concrete 2-level resolver: simplest case, equivalent to KTower2.
// Generalises to N-level via repeated nesting below.

/// Two-level cascade resolver: one inner SharedRegion holding T, one
/// outer SharedRegion of `KTowerCascade<T, 1>` slots that ref the
/// inner. Lookup walks: `cascade.indices[0]` -> outer_region slot ->
/// `KTowerCascade<T, 1>` -> `cascade.indices[1]` -> inner_region slot.
///
/// For larger DEPTHs, compose `CascadeResolver2` and
/// `CascadeResolver3` / `CascadeResolver4` below or use
/// [`SharedRegion`] directly with custom traversal.
pub struct CascadeResolver2<T: Copy + Default + 'static> {
    pub outer: SharedRegion<KTowerCascade<T, 1>>,
    pub inner: SharedRegion<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<T: Copy + Default + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for CascadeResolver2<T>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + Default + 'static> CascadeResolver2<T> {
    pub fn create(
        outer_path: impl AsRef<Path>, outer_capacity: usize,
        inner_path: impl AsRef<Path>, inner_capacity: usize,
    ) -> Result<Self, CascadeError> {
        let outer = SharedRegion::create(outer_path, outer_capacity)?;
        let inner = SharedRegion::create(inner_path, inner_capacity)?;
        Ok(Self {
            outer, inner,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        outer_path: impl AsRef<Path>, outer_capacity: usize,
        inner_path: impl AsRef<Path>, inner_capacity: usize,
    ) -> Result<Self, CascadeError> {
        let outer = SharedRegion::open(outer_path, outer_capacity)?;
        let inner = SharedRegion::open(inner_path, inner_capacity)?;
        Ok(Self {
            outer, inner,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Insert a T at a chosen cascade path. Allocates the inner slot
    /// + any missing outer slot; returns the resolved cascade.
    pub fn insert(
        &self, outer_idx: u32, value: T,
    ) -> Result<KTowerCascade<T, 2>, CascadeError> {
        let r = self.insert_inner(outer_idx, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::cascade::OP_INSERT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn insert_inner(
        &self, outer_idx: u32, value: T,
    ) -> Result<KTowerCascade<T, 2>, CascadeError> {
        // Allocate leaf T.
        let leaf = self.inner.allocate(value)?;
        // Outer slot holds a KTowerCascade<T, 1> that points to leaf.
        let inner_cascade = KTowerCascade::<T, 1>::new([leaf.index]);
        // Replace outer slot if it exists, otherwise allocate.
        let outer_slot = if outer_idx < self.outer.capacity() as u32 {
            // Check whether this outer slot has been allocated.
            // The simplest contract: the caller's outer_idx is the
            // SLOT INDEX in `outer`, so we allocate sequentially and
            // require outer_idx to be < self.outer.len() OR exactly
            // self.outer.len() (next free slot).
            let cur_len = self.outer.len() as u32;
            if outer_idx == cur_len {
                let p = self.outer.allocate(inner_cascade)?;
                p.index
            } else if outer_idx < cur_len {
                self.outer.set(OffsetPtr::new(outer_idx), inner_cascade)?;
                outer_idx
            } else {
                return Err(CascadeError::OutOfBounds);
            }
        } else {
            return Err(CascadeError::OutOfBounds);
        };
        Ok(KTowerCascade::<T, 2>::new([outer_slot, leaf.index]))
    }

    /// Resolve a 2-level cascade to its T value.
    pub fn get(&self, c: KTowerCascade<T, 2>) -> Result<T, CascadeError> {
        let r = self.get_inner(c);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::cascade::OP_GET,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn get_inner(&self, c: KTowerCascade<T, 2>) -> Result<T, CascadeError> {
        if c.indices[0] == NIL_INDEX { return Err(CascadeError::NilAtLevel(0)); }
        let inner_cascade = self.outer.get(OffsetPtr::new(c.indices[0]))?;
        if inner_cascade.indices[0] != c.indices[1] {
            // The cascade we received doesn't match what's currently
            // at this outer slot - either stale or pointing at the
            // wrong leaf. Honour the cascade's claim by reading the
            // leaf directly.
        }
        if c.indices[1] == NIL_INDEX { return Err(CascadeError::NilAtLevel(1)); }
        Ok(self.inner.get(OffsetPtr::new(c.indices[1]))?)
    }

    pub fn flush(&self) -> Result<(), CascadeError> {
        self.outer.flush()?;
        self.inner.flush()?;
        Ok(())
    }
}

/// Generic N-level resolver: caller provides a sequence of
/// SharedRegions, one per non-leaf level holding intermediate
/// cascade pointers, plus the leaf region holding T. Resolution
/// walks the indices array level-by-level.
///
/// This is the explicit-walk variant. For a more ergonomic 4-level
/// MMU-shaped resolver, callers can wrap this in their own type.
pub struct CascadeResolverN<T: Copy + Default + 'static, const DEPTH: usize> {
    /// Intermediate regions, one per non-leaf level. Each holds a
    /// flat u32 (the next level's index). For depth N, this has
    /// length N-1.
    pub intermediate: Vec<SharedRegion<u32>>,
    /// Leaf region holding T.
    pub leaf: SharedRegion<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<T: Copy + Default + Send + Sync + 'static, const DEPTH: usize>
    subetha_sidecar::AdaptiveInstance for CascadeResolverN<T, DEPTH>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + Default + 'static, const DEPTH: usize> CascadeResolverN<T, DEPTH> {
    pub fn create(
        leaf_path: impl AsRef<Path>, leaf_capacity: usize,
        intermediate_paths_and_caps: Vec<(std::path::PathBuf, usize)>,
    ) -> Result<Self, CascadeError> {
        assert_eq!(intermediate_paths_and_caps.len(), DEPTH.saturating_sub(1),
            "must supply DEPTH-1 intermediate region descriptors");
        let leaf = SharedRegion::create(leaf_path, leaf_capacity)?;
        let intermediate: Vec<SharedRegion<u32>> = intermediate_paths_and_caps
            .into_iter()
            .map(|(p, cap)| SharedRegion::create(p, cap))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            intermediate, leaf,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        leaf_path: impl AsRef<Path>, leaf_capacity: usize,
        intermediate_paths_and_caps: Vec<(std::path::PathBuf, usize)>,
    ) -> Result<Self, CascadeError> {
        assert_eq!(intermediate_paths_and_caps.len(), DEPTH.saturating_sub(1));
        let leaf = SharedRegion::open(leaf_path, leaf_capacity)?;
        let intermediate: Vec<SharedRegion<u32>> = intermediate_paths_and_caps
            .into_iter()
            .map(|(p, cap)| SharedRegion::open(p, cap))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            intermediate, leaf,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Allocate a T value at the leaf and return its cascade. The
    /// caller is responsible for populating the intermediate levels
    /// (e.g., by treating outer indices as slot positions that
    /// reference the next-level slot to descend into).
    ///
    /// For the simplest case where each level's "index" is just the
    /// slot position in the corresponding intermediate region, use
    /// `insert_path` which performs the full descent.
    pub fn allocate_leaf(&self, value: T) -> Result<u32, CascadeError> {
        Ok(self.leaf.allocate(value)?.index)
    }

    /// Insert at a chosen TOP-LEVEL slot. Every intermediate level
    /// and the leaf are auto-allocated to fresh slots. Returns the
    /// full cascade. Calling twice with the same `top_idx` will
    /// OVERWRITE the previous top-level chain (the orphaned
    /// intermediates remain in their regions but become unreachable
    /// from `top_idx`).
    ///
    /// For independent insertions, use distinct `top_idx` values
    /// (typically `cascade_count`, i.e. the next free top slot,
    /// which can be obtained as `intermediate[0].len() as u32`).
    pub fn insert_at_top(
        &self, top_idx: u32, value: T,
    ) -> Result<KTowerCascade<T, DEPTH>, CascadeError> {
        let r = self.insert_at_top_inner(top_idx, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::cascade::OP_INSERT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn insert_at_top_inner(
        &self, top_idx: u32, value: T,
    ) -> Result<KTowerCascade<T, DEPTH>, CascadeError> {
        let mut full = [0u32; DEPTH];
        // Allocate the leaf first.
        full[DEPTH - 1] = self.leaf.allocate(value)?.index;
        // For each intermediate level from leaf-side back toward the
        // top, allocate a fresh slot whose value points at the next
        // level. The TOP slot is the caller-chosen top_idx.
        for level in (1..DEPTH - 1).rev() {
            let p = self.intermediate[level].allocate(full[level + 1])?;
            full[level] = p.index;
        }
        // The top-level write: write full[1] into intermediate[0]
        // at slot top_idx.
        if DEPTH > 1 {
            self.write_intermediate(0, top_idx, full[1])?;
            full[0] = top_idx;
        } else {
            full[0] = top_idx;
        }
        Ok(KTowerCascade::<T, DEPTH>::new(full))
    }

    /// Append a fresh entry. Picks the next free top slot
    /// automatically. Equivalent to
    /// `insert_at_top(intermediate[0].len() as u32, value)` for
    /// DEPTH > 1, or `(allocate leaf, return its index)` for DEPTH=1.
    pub fn append(
        &self, value: T,
    ) -> Result<KTowerCascade<T, DEPTH>, CascadeError> {
        if DEPTH == 1 {
            let leaf_idx = self.leaf.allocate(value)?.index;
            return Ok(KTowerCascade::<T, DEPTH>::new([leaf_idx; DEPTH]));
        }
        let next_top = self.intermediate[0].len() as u32;
        self.insert_at_top(next_top, value)
    }

    fn write_intermediate(
        &self, level: usize, slot: u32, value: u32,
    ) -> Result<(), CascadeError> {
        let region = &self.intermediate[level];
        let cur_len = region.len() as u32;
        if slot == cur_len {
            region.allocate(value)?;
        } else if slot < cur_len {
            region.set(OffsetPtr::new(slot), value)?;
        } else {
            return Err(CascadeError::OutOfBounds);
        }
        Ok(())
    }

    /// Resolve a cascade to its T value by walking the levels.
    pub fn get(&self, c: KTowerCascade<T, DEPTH>) -> Result<T, CascadeError> {
        let r = self.get_inner(c);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::cascade::OP_GET,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn get_inner(&self, c: KTowerCascade<T, DEPTH>) -> Result<T, CascadeError> {
        for level in 0..DEPTH {
            if c.indices[level] == NIL_INDEX {
                return Err(CascadeError::NilAtLevel(level));
            }
        }
        // Walk: for each non-leaf level, verify that the
        // intermediate region's slot matches the next level's index.
        for level in 0..DEPTH - 1 {
            let stored = self.intermediate[level].get(OffsetPtr::new(c.indices[level]))?;
            if stored != c.indices[level + 1] {
                return Err(CascadeError::NilAtLevel(level + 1));
            }
        }
        Ok(self.leaf.get(OffsetPtr::new(c.indices[DEPTH - 1]))?)
    }

    pub fn flush(&self) -> Result<(), CascadeError> {
        self.leaf.flush()?;
        for r in &self.intermediate { r.flush()?; }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-cascade-{name}-{pid}.bin"));
        p
    }

    // ===== Pointer-type tests =====

    #[test]
    fn nil_is_all_ones() {
        let n = KTowerCascade::<u64, 4>::NIL;
        assert!(n.is_nil());
        assert_eq!(n.raw(), [NIL_INDEX; 4]);
    }

    #[test]
    fn new_and_level_access() {
        let c = KTowerCascade::<u64, 3>::new([1, 2, 3]);
        assert_eq!(c.level(0), 1);
        assert_eq!(c.level(1), 2);
        assert_eq!(c.level(2), 3);
        assert_eq!(c.leaf(), 3);
        assert!(!c.is_nil());
    }

    #[test]
    fn with_level_updates_only_one() {
        let c = KTowerCascade::<u64, 4>::new([1, 2, 3, 4]);
        let c2 = c.with_level(2, 99);
        assert_eq!(c2.raw(), [1, 2, 99, 4]);
        // Original unchanged.
        assert_eq!(c.raw(), [1, 2, 3, 4]);
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let a = KTowerCascade::<u64, 2>::new([5, 10]);
        let b = KTowerCascade::<u64, 2>::new([5, 10]);
        let c = KTowerCascade::<u64, 2>::new([5, 11]);
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut s = HashSet::new();
        s.insert(a);
        assert!(s.contains(&b));
        assert!(!s.contains(&c));
    }

    #[test]
    fn raw_round_trip_preserves_indices() {
        let c = KTowerCascade::<u64, 4>::new([10, 20, 30, 40]);
        let raw = c.raw();
        let c2 = KTowerCascade::<u64, 4>::from_raw(raw);
        assert_eq!(c, c2);
    }

    #[test]
    fn depth_1_degenerates_to_offset_ptr() {
        let c = KTowerCascade::<u64, 1>::new([42]);
        assert_eq!(c.leaf(), 42);
        assert_eq!(c.level(0), 42);
    }

    #[test]
    fn size_grows_linearly_with_depth() {
        assert_eq!(std::mem::size_of::<KTowerCascade<u64, 1>>(), 4);
        assert_eq!(std::mem::size_of::<KTowerCascade<u64, 2>>(), 8);
        assert_eq!(std::mem::size_of::<KTowerCascade<u64, 4>>(), 16);
        assert_eq!(std::mem::size_of::<KTowerCascade<u64, 8>>(), 32);
    }

    // ===== CascadeResolver2 tests =====

    #[test]
    fn resolver2_insert_get_round_trip() {
        let op = tmp("r2-outer");
        let ip = tmp("r2-inner");
        let r: CascadeResolver2<u64> = CascadeResolver2::create(&op, 16, &ip, 64).unwrap();
        let c1 = r.insert(0, 100).unwrap();
        let c2 = r.insert(1, 200).unwrap();
        let c3 = r.insert(2, 300).unwrap();
        assert_eq!(r.get(c1).unwrap(), 100);
        assert_eq!(r.get(c2).unwrap(), 200);
        assert_eq!(r.get(c3).unwrap(), 300);
        std::fs::remove_file(&op).ok();
        std::fs::remove_file(&ip).ok();
    }

    #[test]
    fn resolver2_cross_handle_visibility() {
        let op = tmp("r2-cross-outer");
        let ip = tmp("r2-cross-inner");
        let writer: CascadeResolver2<u64> = CascadeResolver2::create(&op, 16, &ip, 64).unwrap();
        let reader: CascadeResolver2<u64> = CascadeResolver2::open(&op, 16, &ip, 64).unwrap();
        let c = writer.insert(0, 7777).unwrap();
        // The cascade's raw bits are stable.
        let raw = c.raw();
        // Reader uses the same raw cascade bits to resolve.
        let c_reconstructed: KTowerCascade<u64, 2>
            = KTowerCascade::from_raw(raw);
        assert_eq!(reader.get(c_reconstructed).unwrap(), 7777);
        std::fs::remove_file(&op).ok();
        std::fs::remove_file(&ip).ok();
    }

    // ===== Generic CascadeResolverN tests =====

    #[test]
    fn resolver_n_depth_4_full_walk() {
        let lp = tmp("rn-leaf");
        let i0 = tmp("rn-i0");
        let i1 = tmp("rn-i1");
        let i2 = tmp("rn-i2");
        let r: CascadeResolverN<u64, 4> = CascadeResolverN::create(
            &lp, 64,
            vec![(i0.clone(), 16), (i1.clone(), 16), (i2.clone(), 16)],
        ).unwrap();
        // Each insert picks a distinct top slot via append().
        let c1 = r.append(1111).unwrap();
        let c2 = r.append(2222).unwrap();
        let c3 = r.append(3333).unwrap();
        assert_eq!(r.get(c1).unwrap(), 1111);
        assert_eq!(r.get(c2).unwrap(), 2222);
        assert_eq!(r.get(c3).unwrap(), 3333);
        for p in [&lp, &i0, &i1, &i2] { std::fs::remove_file(p).ok(); }
    }

    #[test]
    fn resolver_n_nil_at_level_rejected() {
        let lp = tmp("rn-nil-leaf");
        let i0 = tmp("rn-nil-i0");
        let r: CascadeResolverN<u64, 2> = CascadeResolverN::create(
            &lp, 8, vec![(i0.clone(), 8)],
        ).unwrap();
        let nil = KTowerCascade::<u64, 2>::NIL;
        assert!(matches!(r.get(nil), Err(CascadeError::NilAtLevel(0))));
        for p in [&lp, &i0] { std::fs::remove_file(p).ok(); }
    }

    #[test]
    fn resolver_n_struct_value_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq, Default)]
        #[repr(C)]
        struct Entry { key: u64, payload: u64 }
        let lp = tmp("rn-struct-leaf");
        let i0 = tmp("rn-struct-i0");
        let r: CascadeResolverN<Entry, 2> = CascadeResolverN::create(
            &lp, 8, vec![(i0.clone(), 8)],
        ).unwrap();
        let entry = Entry { key: 42, payload: 999 };
        let c = r.append(entry).unwrap();
        assert_eq!(r.get(c).unwrap(), entry);
        for p in [&lp, &i0] { std::fs::remove_file(p).ok(); }
    }

    #[test]
    fn cascade_bits_are_position_independent() {
        // The whole point: the same [u32; DEPTH] resolves to the
        // same value in any process that maps the same regions.
        let lp = tmp("pi-leaf");
        let i0 = tmp("pi-i0");
        let r_a: CascadeResolverN<u64, 2> = CascadeResolverN::create(
            &lp, 8, vec![(i0.clone(), 8)],
        ).unwrap();
        let c = r_a.append(0xCAFE_BABE).unwrap();
        let raw = c.raw();
        let r_b: CascadeResolverN<u64, 2> = CascadeResolverN::open(
            &lp, 8, vec![(i0.clone(), 8)],
        ).unwrap();
        let c_b = KTowerCascade::<u64, 2>::from_raw(raw);
        assert_eq!(r_b.get(c_b).unwrap(), 0xCAFE_BABE);
        for p in [&lp, &i0] { std::fs::remove_file(p).ok(); }
    }

    #[test]
    fn resolver_n_disk_persistence_survives_reopen() {
        let lp = tmp("rn-disk-leaf");
        let i0 = tmp("rn-disk-i0");
        let i1 = tmp("rn-disk-i1");
        let c_raw;
        {
            let r: CascadeResolverN<u64, 3> = CascadeResolverN::create(
                &lp, 8, vec![(i0.clone(), 8), (i1.clone(), 8)],
            ).unwrap();
            let c = r.append(5555).unwrap();
            c_raw = c.raw();
            r.flush().unwrap();
        }
        let r2: CascadeResolverN<u64, 3> = CascadeResolverN::open(
            &lp, 8, vec![(i0.clone(), 8), (i1.clone(), 8)],
        ).unwrap();
        let c_restored = KTowerCascade::<u64, 3>::from_raw(c_raw);
        assert_eq!(r2.get(c_restored).unwrap(), 5555);
        for p in [&lp, &i0, &i1] { std::fs::remove_file(p).ok(); }
    }

    #[test]
    fn default_is_nil() {
        let c: KTowerCascade<u64, 4> = KTowerCascade::default();
        assert!(c.is_nil());
    }
}
