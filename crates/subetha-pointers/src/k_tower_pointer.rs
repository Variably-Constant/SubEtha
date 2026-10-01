//! `KTower2<T>` and `KTower3<T>` - segmented addresses in one `u64`.
//!
//! A pointer here is a set of indices rather than an address: a region
//! number and an offset within that region, resolved through a table
//! the caller keeps. The hardware MMU resolves addresses the same way
//! (x86_64 page tables are PML4 -> PDPT -> PD -> PT, four levels of
//! 9-bit indices into nested tables).
//!
//! - [`KTower2<T>`]: two-segment `(region_id: u32, offset: u32)`
//!   packed into a u64. The region table is supplied by the caller
//!   (typically a `Vec<*const u8>` of region base pointers). Resolves
//!   via `region_table[region_id] + offset`. Equivalent to one MMU
//!   page-table level.
//!
//! - [`KTower3<T>`]: three-segment `(zone: u16, region_id: u16,
//!   offset: u32)` for hierarchical naming (zone -> region -> slot),
//!   such as zones for racks or data centers and regions for the
//!   nodes in them. It packs and unpacks the three fields; resolving
//!   them is the caller's.
//!
//! Both are 8 bytes, the size of a native pointer, and hold no virtual
//! addresses, so a value resolves identically in any process that
//! holds the same region tables. `subetha_cxc::KTowerCascade<T, DEPTH>`
//! carries the same indices to any depth over shared regions.

use std::marker::PhantomData;

/// Two-segment pointer: `(region_id: u32 high, offset: u32 low)`.
/// Resolution requires a region-base table.
#[repr(transparent)]
pub struct KTower2<T> {
    raw: u64,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Send> Send for KTower2<T> {}
unsafe impl<T: Sync> Sync for KTower2<T> {}

impl<T> KTower2<T> {
    /// Direction signature of `KTower2<T>`. Engages the
    /// `K_segmented` axis (two-segment `(region_id, offset)` address
    /// space).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Segmented],
    );

    #[inline]
    pub const fn new(region_id: u32, offset: u32) -> Self {
        let raw = ((region_id as u64) << 32) | (offset as u64);
        Self { raw, _phantom: PhantomData }
    }

    #[inline]
    pub const fn region_id(&self) -> u32 { (self.raw >> 32) as u32 }
    #[inline]
    pub const fn offset(&self) -> u32 { (self.raw & 0xFFFF_FFFF) as u32 }
    #[inline]
    pub const fn raw(&self) -> u64 { self.raw }

    /// Resolve to a real address using a region-base table.
    /// `region_table[region_id]` is the base pointer of the region.
    ///
    /// # Safety
    ///
    /// `region_id` must be a valid index into `region_table`; the
    /// resulting address `base + offset` must be a valid `T`.
    pub unsafe fn resolve(&self, region_table: &[*const u8]) -> *const T {
        let base = region_table[self.region_id() as usize];
        unsafe { base.add(self.offset() as usize) as *const T }
    }
}

impl<T> Clone for KTower2<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for KTower2<T> {}

impl<T> std::fmt::Debug for KTower2<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KTower2 {{ region: {}, offset: {:#x} }}",
               self.region_id(), self.offset())
    }
}

impl<T> PartialEq for KTower2<T> {
    fn eq(&self, other: &Self) -> bool { self.raw == other.raw }
}
impl<T> Eq for KTower2<T> {}

/// Three-segment pointer: `(zone: u16, region: u16, offset: u32)`.
/// Hierarchical: zone -> region -> within-region offset.
#[repr(transparent)]
pub struct KTower3<T> {
    raw: u64,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Send> Send for KTower3<T> {}
unsafe impl<T: Sync> Sync for KTower3<T> {}

impl<T> KTower3<T> {
    /// Direction signature of `KTower3<T>`. Engages the
    /// `K_segmented` axis (three-segment `(zone, region, offset)`
    /// hierarchical address space).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Segmented],
    );

    #[inline]
    pub const fn new(zone: u16, region: u16, offset: u32) -> Self {
        let raw = ((zone as u64) << 48) | ((region as u64) << 32) | (offset as u64);
        Self { raw, _phantom: PhantomData }
    }

    #[inline]
    pub const fn zone(&self) -> u16 { (self.raw >> 48) as u16 }
    #[inline]
    pub const fn region(&self) -> u16 { ((self.raw >> 32) & 0xFFFF) as u16 }
    #[inline]
    pub const fn offset(&self) -> u32 { (self.raw & 0xFFFF_FFFF) as u32 }
    #[inline]
    pub const fn raw(&self) -> u64 { self.raw }
}

impl<T> Clone for KTower3<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for KTower3<T> {}

impl<T> std::fmt::Debug for KTower3<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KTower3 {{ zone: {}, region: {}, offset: {:#x} }}",
               self.zone(), self.region(), self.offset())
    }
}

impl<T> PartialEq for KTower3<T> {
    fn eq(&self, other: &Self) -> bool { self.raw == other.raw }
}
impl<T> Eq for KTower3<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ktower2_layout_is_8_bytes() {
        assert_eq!(std::mem::size_of::<KTower2<u64>>(), 8);
    }

    #[test]
    fn ktower2_segments_round_trip() {
        let p: KTower2<u64> = KTower2::new(7, 0xCAFE);
        assert_eq!(p.region_id(), 7);
        assert_eq!(p.offset(), 0xCAFE);
    }

    #[test]
    fn ktower2_resolves_via_region_table() {
        // Two regions of u64 values.
        let region_a: Vec<u64> = vec![10, 20, 30, 40];
        let region_b: Vec<u64> = vec![100, 200, 300, 400];
        let table: Vec<*const u8> = vec![
            region_a.as_ptr() as *const u8,
            region_b.as_ptr() as *const u8,
        ];
        // Pointer to region 1, offset 8 bytes (second u64 = 200).
        let p: KTower2<u64> = KTower2::new(1, 8);
        let resolved = unsafe { p.resolve(&table) };
        assert_eq!(unsafe { *resolved }, 200);
        // Offset 16 bytes = third u64 = 300.
        let p2: KTower2<u64> = KTower2::new(1, 16);
        assert_eq!(unsafe { *p2.resolve(&table) }, 300);
    }

    #[test]
    fn ktower3_layout_is_8_bytes() {
        assert_eq!(std::mem::size_of::<KTower3<u64>>(), 8);
    }

    #[test]
    fn ktower3_segments_round_trip() {
        let p: KTower3<u64> = KTower3::new(3, 17, 0xDEAD);
        assert_eq!(p.zone(), 3);
        assert_eq!(p.region(), 17);
        assert_eq!(p.offset(), 0xDEAD);
    }

    #[test]
    fn ktower_segments_distinguishable() {
        let a: KTower2<u64> = KTower2::new(1, 100);
        let b: KTower2<u64> = KTower2::new(2, 100);
        let c: KTower2<u64> = KTower2::new(1, 200);
        assert_ne!(a, b, "different regions");
        assert_ne!(a, c, "different offsets");
        assert_eq!(a, KTower2::<u64>::new(1, 100));
    }
}
