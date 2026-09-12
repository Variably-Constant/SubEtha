//! `RawRegion`: the slot arena of
//! [`SharedRegion`](crate::shared_region::SharedRegion) at an element size
//! and alignment fixed at run time instead of by a type parameter, for a
//! caller reaching the region from another language. It reads and writes
//! the same file format under the same allocator: a Treiber-stack free
//! list with an ABA counter in front of a bump high-water mark, the free
//! links in their own array ahead of the slots. A slot is named by its
//! index, which resolves to the same bytes in every process that maps
//! the file. The region records the element size, the alignment, where
//! the slots start and a tag the caller chooses, and an open that states
//! another layout is refused.
//!
//! # Slot geometry
//!
//! Slots are `element_size` bytes apart, so the size a caller declares is
//! the stride, as `size_of::<T>()` is for the typed region. The slot array
//! starts at the first multiple of the alignment past the free links,
//! which for an alignment of at most 4 is exactly where the typed region
//! puts it, so the two open each other's regions.
//!
//! Reads and writes of a slot are plain copies, as they are in the typed
//! region: a reader racing a writer on one slot sees a mix of old and new
//! bytes. A caller that needs a torn read detected uses the slab.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::raw_treiber_stack::ElementLayout;
use crate::shared_region::{RegionError, RegionHeader, NIL_INDEX, REGION_MAGIC};

#[inline]
fn pack(counter: u32, index: u32) -> u64 {
    ((counter as u64) << 32) | (index as u64)
}

#[inline]
fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// Where the free links start: right behind the header.
pub const fn raw_region_links_offset() -> usize {
    size_of::<RegionHeader>()
}

/// Where the slot array starts for `capacity` slots of `layout`: the
/// first multiple of the alignment at or past the free links.
pub fn raw_region_slots_offset(capacity: usize, layout: &ElementLayout) -> usize {
    (raw_region_links_offset() + capacity * size_of::<AtomicU32>()).next_multiple_of(layout.alignment)
}

/// Bytes the file holds for `capacity` slots of `layout`.
pub fn raw_region_file_size(capacity: usize, layout: &ElementLayout) -> usize {
    raw_region_slots_offset(capacity, layout) + capacity * layout.slot_size
}

pub struct RawRegion {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    layout: ElementLayout,
    slots_offset: usize,
}

unsafe impl Send for RawRegion {}
unsafe impl Sync for RawRegion {}

impl std::fmt::Debug for RawRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawRegion")
            .field("capacity", &self.capacity)
            .field("layout", &self.layout)
            .field("slots_offset", &self.slots_offset)
            .finish()
    }
}

impl RawRegion {
    /// A layout the region can hold: a non-empty element that fits the
    /// header's 32-bit size field and an alignment that is a power of two.
    /// Anything else is a `LayoutMismatch`: no region carries it.
    fn check_layout(layout: &ElementLayout) -> Result<(), RegionError> {
        if layout.slot_size == 0
            || layout.slot_size > u32::MAX as usize
            || layout.alignment == 0
            || !layout.alignment.is_power_of_two()
            || layout.alignment > u32::MAX as usize
        {
            return Err(RegionError::LayoutMismatch);
        }
        Ok(())
    }

    /// A capacity the index space can name: at least one slot, below the
    /// index that means none.
    fn check_capacity(capacity: usize) -> Result<(), RegionError> {
        if capacity == 0 || capacity >= NIL_INDEX as usize {
            return Err(RegionError::LayoutMismatch);
        }
        Ok(())
    }

    /// Obtain the region at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. Attaching leaves
    /// allocated slots and the free list in place, so indexes other
    /// processes hold stay resolvable; a region built with a different
    /// capacity or layout is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, RegionError> {
        Self::check_layout(&layout)?;
        Self::check_capacity(capacity)?;
        let (file, mut mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            raw_region_file_size(capacity, &layout),
            |ptr| unsafe { Self::init_region(ptr, capacity, &layout) },
            |ptr| unsafe { (*(ptr as *const RegionHeader)).magic == REGION_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, RegionError::LayoutMismatch))?;
        crate::mmf_warm::warm_mmap(&mut mmap);
        Self::from_region(file, mmap, capacity, layout)
    }

    /// Truncate the region at `path` and initialize an empty one,
    /// invalidating every index live peers hold.
    pub fn reset(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, RegionError> {
        Self::check_layout(&layout)?;
        Self::check_capacity(capacity)?;
        let (file, mut mmap) =
            crate::mmf_attach::reset(path.as_ref(), raw_region_file_size(capacity, &layout), |ptr| unsafe {
                Self::init_region(ptr, capacity, &layout)
            })?;
        crate::mmf_warm::warm_mmap(&mut mmap);
        Self::from_region(file, mmap, capacity, layout)
    }

    /// Attach to the region at `path`; the file must exist.
    pub fn open(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, RegionError> {
        Self::check_layout(&layout)?;
        Self::check_capacity(capacity)?;
        let total = raw_region_file_size(capacity, &layout);
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(RegionError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        crate::mmf_warm::warm_mmap(&mut mmap);
        Self::from_region(file, mmap, capacity, layout)
    }

    /// Lay out an empty region: the geometry, the zero bump cursor and the
    /// NIL free head first, magic last, because attachers spin on it. The
    /// zeroed link array is the not-on-a-free-chain state.
    ///
    /// # Safety
    /// `ptr` addresses at least `raw_region_file_size(capacity, layout)`
    /// writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, layout: &ElementLayout) {
        let hdr = ptr as *mut RegionHeader;
        unsafe {
            (*hdr).capacity = capacity as u32;
            (*hdr).slot_size = layout.slot_size as u32;
            (*hdr).slots_offset = raw_region_slots_offset(capacity, layout) as u32;
            (*hdr).alignment = layout.alignment as u32;
            (*hdr).layout_tag = layout.tag;
            std::ptr::write(&raw mut (*hdr).free_head, AtomicU64::new(pack(0, NIL_INDEX)));
            std::ptr::write_volatile(&raw mut (*hdr).magic, REGION_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one built with a different
    /// capacity, element size, slot start or tag. The alignment field is
    /// informational; the slot start it produced is what is checked.
    fn from_region(file: File, mmap: MmapMut, capacity: usize, layout: ElementLayout) -> Result<Self, RegionError> {
        let slots_offset = raw_region_slots_offset(capacity, &layout);
        let hdr = unsafe { &*(mmap.as_ptr() as *const RegionHeader) };
        if hdr.magic != REGION_MAGIC
            || hdr.capacity != capacity as u32
            || hdr.slot_size != layout.slot_size as u32
            || hdr.slots_offset != slots_offset as u32
            || hdr.layout_tag != layout.tag
        {
            return Err(RegionError::LayoutMismatch);
        }
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            layout,
            slots_offset,
        })
    }

    #[inline]
    fn header(&self) -> &RegionHeader {
        unsafe { &*(self.mmap.as_ptr() as *const RegionHeader) }
    }

    #[inline]
    fn next_link(&self, idx: usize) -> &AtomicU32 {
        let base = unsafe { self.mmap.as_ptr().add(raw_region_links_offset()) };
        unsafe { &*(base.add(idx * size_of::<AtomicU32>()) as *const AtomicU32) }
    }

    #[inline]
    fn slot_ptr(&self, idx: usize) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(self.slots_offset).add(idx * self.layout.slot_size) as *mut u8 }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The layout this handle opened the region with.
    #[inline]
    pub fn layout(&self) -> ElementLayout {
        self.layout
    }

    /// Where the slot array starts in the file.
    #[inline]
    pub fn slots_offset(&self) -> usize {
        self.slots_offset
    }

    /// Slots allocated right now: the bump high-water mark less the free
    /// list's length. A snapshot; allocations and frees race it.
    pub fn len(&self) -> usize {
        let bump = self.header().bump_next.load(Ordering::Acquire) as usize;
        bump.saturating_sub(self.free_count())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The free list's length, walked; best-effort under concurrent
    /// activity.
    pub fn free_count(&self) -> usize {
        let mut count = 0usize;
        let (_, mut idx) = unpack(self.header().free_head.load(Ordering::Acquire));
        while idx != NIL_INDEX && count < self.capacity {
            count += 1;
            idx = self.next_link(idx as usize).load(Ordering::Acquire);
        }
        count
    }

    /// A slot index that names a slot: not NIL and below the capacity.
    fn checked(&self, idx: u32) -> Result<usize, RegionError> {
        if idx == NIL_INDEX || idx as usize >= self.capacity {
            return Err(RegionError::InvalidPtr);
        }
        Ok(idx as usize)
    }

    /// A value argument of exactly the element size.
    fn sized(&self, value: &[u8]) -> Result<(), RegionError> {
        if value.len() != self.layout.slot_size {
            return Err(RegionError::PayloadTooLarge);
        }
        Ok(())
    }

    /// An output buffer of at least the element size.
    fn room(&self, out: &[u8]) -> Result<(), RegionError> {
        if out.len() < self.layout.slot_size {
            return Err(RegionError::PayloadTooLarge);
        }
        Ok(())
    }

    fn write_slot(&self, idx: usize, value: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), self.slot_ptr(idx), self.layout.slot_size);
        }
    }

    fn read_slot(&self, idx: usize, out: &mut [u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(self.slot_ptr(idx), out.as_mut_ptr(), self.layout.slot_size);
        }
    }

    /// The bytes of slot `idx`, for a caller that reads or writes part of
    /// an element in place rather than copying the whole of it.
    /// `InvalidPtr` for NIL or an index at or past the capacity.
    pub fn element_ptr(&self, idx: u32) -> Result<*mut u8, RegionError> {
        let slot = self.checked(idx)?;
        Ok(self.slot_ptr(slot))
    }

    /// Allocate a slot holding `value`, exactly one element, and return
    /// its index: the free list first, then the bump cursor. `Full` when
    /// both are exhausted.
    pub fn allocate(&self, value: &[u8]) -> Result<u32, RegionError> {
        self.sized(value)?;
        // SAFETY: the slot is this element's size and the value was checked
        // to match it.
        unsafe { self.allocate_with(|dst| std::ptr::copy_nonoverlapping(value.as_ptr(), dst, value.len())) }
    }

    /// Allocate a slot and let `fill` write the element into it, for a
    /// caller that would otherwise build the bytes somewhere else and copy
    /// them in. `fill` receives the slot's address and may write the
    /// element's whole size; the slot holds whatever it leaves.
    ///
    /// # Safety
    /// `fill` writes at most `layout.slot_size` bytes at the pointer.
    pub unsafe fn allocate_with(&self, fill: impl FnOnce(*mut u8)) -> Result<u32, RegionError> {
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, idx) = unpack(head);
            if idx == NIL_INDEX {
                break;
            }
            let next_idx = self.next_link(idx as usize).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next_idx);
            if self
                .header()
                .free_head
                .compare_exchange(head, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                fill(self.slot_ptr(idx as usize));
                return Ok(idx);
            }
        }
        let idx = self.header().bump_next.fetch_add(1, Ordering::AcqRel);
        if idx >= self.capacity as u64 {
            self.header().bump_next.fetch_sub(1, Ordering::AcqRel);
            return Err(RegionError::Full);
        }
        let idx = idx as u32;
        fill(self.slot_ptr(idx as usize));
        Ok(idx)
    }

    /// Free slot `idx`, copying the element it held into `out`, at least
    /// one element long, and pushing the slot onto the free list.
    pub fn free(&self, idx: u32, out: &mut [u8]) -> Result<(), RegionError> {
        let slot = self.checked(idx)?;
        self.room(out)?;
        self.read_slot(slot, out);
        self.free_slot(idx)
    }

    /// Push slot `idx` onto the free list without reading what it held,
    /// for a caller that has already taken what it needs.
    pub fn free_slot(&self, idx: u32) -> Result<(), RegionError> {
        let slot = self.checked(idx)?;
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(slot).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), idx);
            if self
                .header()
                .free_head
                .compare_exchange(head, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Copy the element at `idx` into `out`, at least one element long.
    /// `InvalidPtr` for NIL or an index at or past the capacity; whether
    /// the slot is still allocated is the caller's knowledge, since a
    /// freed slot is handed out again.
    pub fn get(&self, idx: u32, out: &mut [u8]) -> Result<(), RegionError> {
        let slot = self.checked(idx)?;
        self.room(out)?;
        self.read_slot(slot, out);
        Ok(())
    }

    /// Overwrite the element at `idx` with `value`, exactly one element.
    pub fn set(&self, idx: u32, value: &[u8]) -> Result<(), RegionError> {
        let slot = self.checked(idx)?;
        self.sized(value)?;
        self.write_slot(slot, value);
        Ok(())
    }

    /// Empty the region: the bump cursor back to zero and the free list to
    /// NIL, so every index handed out is stale. Not safe against an
    /// allocate or a free running in any process.
    pub fn clear(&self) {
        self.header().bump_next.store(0, Ordering::Release);
        self.header().free_head.store(pack(0, NIL_INDEX), Ordering::Release);
    }

    pub fn flush(&self) -> Result<(), RegionError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_region::{OffsetPtr, SharedRegion};

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-raw-region-{name}-{}.bin", std::process::id()))
    }

    const BYTES_24: ElementLayout = ElementLayout { slot_size: 24, alignment: 8, tag: 13 };

    fn element(i: u8) -> [u8; 24] {
        let mut e = [i; 24];
        e[23] = i.wrapping_add(1);
        e
    }

    #[test]
    fn allocate_get_set_free_and_reuse_round_trip() {
        let path = tmp("round-trip");
        let region = RawRegion::create(&path, 3, BYTES_24).unwrap();
        assert_eq!(region.layout(), BYTES_24);
        assert!(region.is_empty());
        let a = region.allocate(&element(1)).unwrap();
        let b = region.allocate(&element(2)).unwrap();
        let c = region.allocate(&element(3)).unwrap();
        assert_eq!((a, b, c), (0, 1, 2));
        assert_eq!(region.allocate(&element(4)).unwrap_err(), RegionError::Full);
        assert_eq!(region.len(), 3);
        let mut out = [0u8; 24];
        region.get(b, &mut out).unwrap();
        assert_eq!(out, element(2));
        region.set(b, &element(22)).unwrap();
        region.get(b, &mut out).unwrap();
        assert_eq!(out, element(22));
        assert_eq!(region.get(NIL_INDEX, &mut out).unwrap_err(), RegionError::InvalidPtr);
        assert_eq!(region.get(3, &mut out).unwrap_err(), RegionError::InvalidPtr);
        assert_eq!(region.set(3, &element(1)).unwrap_err(), RegionError::InvalidPtr);
        assert_eq!(region.allocate(&[1u8; 23]).unwrap_err(), RegionError::PayloadTooLarge);
        assert_eq!(region.get(a, &mut [0u8; 23]).unwrap_err(), RegionError::PayloadTooLarge);
        region.free(b, &mut out).unwrap();
        assert_eq!(out, element(22));
        assert_eq!(region.free_count(), 1);
        assert_eq!(region.len(), 2);
        assert_eq!(region.allocate(&element(5)).unwrap(), b, "a freed slot is handed out again");
        region.clear();
        assert!(region.is_empty());
        assert_eq!(region.allocate(&element(6)).unwrap(), 0);
    }

    #[test]
    fn a_typed_region_and_a_raw_one_share_a_region() {
        let path = tmp("interop");
        let typed: SharedRegion<[u8; 16]> = SharedRegion::create(&path, 8).unwrap();
        let p = typed.allocate(*b"sixteen bytes!!!").unwrap();
        let raw = RawRegion::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 0 }).unwrap();
        assert_eq!(raw.len(), 1);
        let mut out = [0u8; 16];
        raw.get(p.index, &mut out).unwrap();
        assert_eq!(&out, b"sixteen bytes!!!");
        let q = raw.allocate(b"raw side wrote!!").unwrap();
        assert_eq!(typed.get(OffsetPtr::new(q)).unwrap(), *b"raw side wrote!!");
        raw.free(q, &mut out).unwrap();
        assert_eq!(typed.free_count(), 1);
        let wrong_tag = RawRegion::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 1 });
        assert_eq!(wrong_tag.unwrap_err(), RegionError::LayoutMismatch);
        let wrong_size = RawRegion::open(&path, 8, ElementLayout { slot_size: 8, alignment: 1, tag: 0 });
        assert_eq!(wrong_size.unwrap_err(), RegionError::LayoutMismatch);
        let wrong_capacity = RawRegion::open(&path, 4, ElementLayout { slot_size: 16, alignment: 1, tag: 0 });
        assert_eq!(wrong_capacity.unwrap_err(), RegionError::LayoutMismatch);
    }

    #[test]
    fn a_wide_alignment_moves_the_slot_array() {
        assert_eq!(raw_region_slots_offset(3, &BYTES_24), 80);
        let aligned_64 = ElementLayout { slot_size: 24, alignment: 64, tag: 0 };
        assert_eq!(raw_region_slots_offset(3, &aligned_64), 128);
        assert_eq!(raw_region_file_size(3, &aligned_64), 128 + 72);
        let path = tmp("aligned");
        let region = RawRegion::create(&path, 3, aligned_64).unwrap();
        assert_eq!(region.slots_offset(), 128);
        let a = region.allocate(&element(9)).unwrap();
        let mut out = [0u8; 24];
        let again = RawRegion::open(&path, 3, aligned_64).unwrap();
        again.get(a, &mut out).unwrap();
        assert_eq!(out, element(9));
        let narrower = RawRegion::open(&path, 3, BYTES_24);
        assert_eq!(narrower.unwrap_err(), RegionError::LayoutMismatch);
    }

    #[test]
    fn an_invalid_layout_or_capacity_is_refused_before_any_file_is_touched() {
        let path = tmp("invalid");
        let empty = RawRegion::create(&path, 4, ElementLayout { slot_size: 0, alignment: 8, tag: 0 });
        assert_eq!(empty.unwrap_err(), RegionError::LayoutMismatch);
        let misaligned = RawRegion::create(&path, 4, ElementLayout { slot_size: 8, alignment: 12, tag: 0 });
        assert_eq!(misaligned.unwrap_err(), RegionError::LayoutMismatch);
        let no_room = RawRegion::create(&path, 0, BYTES_24);
        assert_eq!(no_room.unwrap_err(), RegionError::LayoutMismatch);
        assert!(!path.exists());
    }
}
