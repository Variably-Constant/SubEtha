//! `RawVec`: the bounded, indexable, append-only sequence of
//! [`SharedVec`](crate::shared_vec::SharedVec) at an element size and
//! alignment fixed at run time instead of by a type parameter, for a
//! caller reaching the region from another language. It reads and writes
//! the same file format under the same protocol: the header, the per-slot
//! SeqLock, the reservation-then-publish push and the allocator-agreeing
//! pop. The region records the element size, the slot geometry and a tag
//! the caller chooses, and an open that states another layout is refused.
//!
//! # Slot geometry
//!
//! A slot starts with its version word and pads to the first offset that
//! honors the element's alignment, 8 at the least, which is where the
//! typed vec keeps its payload; its stride is that offset plus the element
//! rounded up to whole cache lines, or to the alignment when that is
//! wider. The slot array starts at the first multiple of the alignment
//! past the header. An element of at most 56 bytes at an alignment of at
//! most 8 lands in the 64-byte slot the typed vec uses, so the two open
//! each other's regions.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::MmapOptions;

use crate::raw_treiber_stack::ElementLayout;
use crate::shared_vec::{Mapping, VecError, VecHeader, VEC_MAGIC};

/// Bytes a slot spends on its version word and the padding behind it
/// before an element of alignment at most 8.
pub const VEC_SLOT_PREFIX: usize = 8;

/// Where a slot's element lies and how wide the slot is for `layout`:
/// `(payload_offset, stride)`.
pub fn raw_vec_geometry(layout: &ElementLayout) -> (usize, usize) {
    let payload_offset = VEC_SLOT_PREFIX.next_multiple_of(layout.alignment);
    let line = 64usize.max(layout.alignment);
    let stride = (payload_offset + layout.slot_size).next_multiple_of(line);
    (payload_offset, stride)
}

/// Where the slot array starts for `layout`: the first multiple of the
/// alignment at or past the header.
pub fn raw_vec_data_offset(layout: &ElementLayout) -> usize {
    size_of::<VecHeader>().next_multiple_of(layout.alignment)
}

/// Bytes the file holds for `capacity` slots of `layout`.
pub fn raw_vec_file_size(capacity: usize, layout: &ElementLayout) -> usize {
    let (_, stride) = raw_vec_geometry(layout);
    raw_vec_data_offset(layout) + capacity * stride
}

pub struct RawVec {
    _file: File,
    mmap: Mapping,
    capacity: usize,
    layout: ElementLayout,
    payload_offset: usize,
    stride: usize,
    data_offset: usize,
}

unsafe impl Send for RawVec {}
unsafe impl Sync for RawVec {}

impl std::fmt::Debug for RawVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawVec")
            .field("capacity", &self.capacity)
            .field("layout", &self.layout)
            .field("stride", &self.stride)
            .field("writable", &self.mmap.is_writable())
            .finish()
    }
}

impl RawVec {
    /// A layout the region can hold: a non-empty element that fits the
    /// header's 32-bit size field and an alignment that is a power of two.
    /// Anything else is a `LayoutMismatch`: no region carries it.
    fn check_layout(layout: &ElementLayout) -> Result<(), VecError> {
        if layout.slot_size == 0
            || layout.slot_size > u32::MAX as usize
            || layout.alignment == 0
            || !layout.alignment.is_power_of_two()
            || layout.alignment > u32::MAX as usize
        {
            return Err(VecError::LayoutMismatch);
        }
        Ok(())
    }

    fn assemble(file: File, mmap: Mapping, capacity: usize, layout: ElementLayout) -> Self {
        let (payload_offset, stride) = raw_vec_geometry(&layout);
        Self {
            _file: file,
            mmap,
            capacity,
            layout,
            payload_offset,
            stride,
            data_offset: raw_vec_data_offset(&layout),
        }
    }

    /// Obtain the vec at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. Attaching leaves
    /// live elements and `len` in place; a region built with a different
    /// capacity or layout is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, VecError> {
        Self::check_layout(&layout)?;
        if capacity == 0 {
            return Err(VecError::LayoutMismatch);
        }
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            raw_vec_file_size(capacity, &layout),
            |ptr| unsafe { Self::init_region(ptr, capacity, &layout) },
            |ptr| unsafe { (*(ptr as *const VecHeader)).magic == VEC_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, VecError::LayoutMismatch))?;
        let this = Self::assemble(file, Mapping::Writable(mmap), capacity, layout);
        this.validate()?;
        Ok(this)
    }

    /// Truncate the vec at `path` and initialize an empty one, discarding
    /// every element live peers share.
    pub fn reset(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, VecError> {
        Self::check_layout(&layout)?;
        if capacity == 0 {
            return Err(VecError::LayoutMismatch);
        }
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), raw_vec_file_size(capacity, &layout), |ptr| unsafe {
            Self::init_region(ptr, capacity, &layout)
        })?;
        Ok(Self::assemble(file, Mapping::Writable(mmap), capacity, layout))
    }

    /// Attach to the vec at `path`; the file must exist.
    pub fn open(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, VecError> {
        Self::check_layout(&layout)?;
        let file = crate::region_file::open_existing(path.as_ref())?;
        let total = raw_vec_file_size(capacity, &layout);
        if file.metadata()?.len() < total as u64 {
            return Err(VecError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let this = Self::assemble(file, Mapping::Writable(mmap), capacity, layout);
        this.validate()?;
        Ok(this)
    }

    /// Attach to the vec at `path` with read access alone; writes return
    /// [`VecError::ReadOnly`].
    pub fn open_read_only(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, VecError> {
        Self::check_layout(&layout)?;
        let file = crate::region_file::open_read_only(path.as_ref())?;
        let total = raw_vec_file_size(capacity, &layout);
        if file.metadata()?.len() < total as u64 {
            return Err(VecError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map(&file)? };
        let this = Self::assemble(file, Mapping::ReadOnly(mmap), capacity, layout);
        this.validate()?;
        Ok(this)
    }

    /// Lay out an empty vec: sizes and geometry first, magic last, because
    /// attachers spin on it. The zeroed region is already the empty slot
    /// array (every version 0) with `len` and `reserved` 0.
    ///
    /// # Safety
    /// `ptr` addresses at least `raw_vec_file_size(capacity, layout)`
    /// writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, layout: &ElementLayout) {
        let (payload_offset, stride) = raw_vec_geometry(layout);
        let hdr = ptr as *mut VecHeader;
        unsafe {
            (*hdr).slot_payload_size = (stride - payload_offset) as u32;
            (*hdr).capacity = capacity as u64;
            (*hdr).slot_stride = stride as u32;
            (*hdr).payload_offset = payload_offset as u32;
            (*hdr).element_size = layout.slot_size as u32;
            (*hdr).alignment = layout.alignment as u32;
            (*hdr).layout_tag = layout.tag;
            std::ptr::write_volatile(&raw mut (*hdr).magic, VEC_MAGIC);
        }
    }

    /// Whether the header on disk is the one this mapping expects: the
    /// capacity, the slot geometry, the element size and the tag must all
    /// match. The alignment field is informational; the geometry it
    /// produced is what is checked, so a typed region whose element aligns
    /// wider than its payload offset opens under the alignment that
    /// offset honors. A writable mapping raises `reserved` to `len` when
    /// it finds it below.
    fn validate(&self) -> Result<(), VecError> {
        let hdr = self.header();
        if hdr.magic != VEC_MAGIC
            || hdr.capacity != self.capacity as u64
            || hdr.slot_stride as usize != self.stride
            || hdr.payload_offset as usize != self.payload_offset
            || hdr.element_size as usize != self.layout.slot_size
            || hdr.layout_tag != self.layout.tag
        {
            return Err(VecError::LayoutMismatch);
        }
        if self.mmap.is_writable() {
            let len = hdr.len.load(Ordering::Acquire);
            let mut cur = hdr.reserved.load(Ordering::Acquire);
            while cur < len {
                match hdr.reserved.compare_exchange_weak(cur, len, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(seen) => cur = seen,
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn header(&self) -> &VecHeader {
        unsafe { &*(self.mmap.as_ptr() as *const VecHeader) }
    }

    /// Whether this mapping may be written.
    #[inline]
    pub fn is_writable(&self) -> bool {
        self.mmap.is_writable()
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

    /// Bytes from one slot to the next.
    #[inline]
    pub fn slot_stride(&self) -> usize {
        self.stride
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
    fn slot_ptr(&self, i: usize) -> *const u8 {
        assert!(i < self.capacity, "slot index {i} out of bounds for capacity {}", self.capacity);
        unsafe { self.mmap.as_ptr().add(self.data_offset).add(i * self.stride) }
    }

    #[inline]
    fn version(&self, i: usize) -> &AtomicU32 {
        unsafe { &*(self.slot_ptr(i) as *const AtomicU32) }
    }

    /// A value argument of exactly the element size.
    fn sized(&self, value: &[u8]) -> Result<(), VecError> {
        if value.len() != self.layout.slot_size {
            return Err(VecError::PayloadTooLarge);
        }
        Ok(())
    }

    /// An output buffer of at least the element size.
    fn room(&self, out: &[u8]) -> Result<(), VecError> {
        if out.len() < self.layout.slot_size {
            return Err(VecError::PayloadTooLarge);
        }
        Ok(())
    }

    /// SeqLock write of an element into slot `i`, which is below the
    /// capacity.
    fn write_slot(&self, i: usize, value: &[u8]) {
        let version = self.version(i);
        let payload = unsafe { self.slot_ptr(i).add(self.payload_offset) as *mut u8 };
        version.fetch_add(1, Ordering::AcqRel);
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), payload, self.layout.slot_size);
        }
        version.fetch_add(1, Ordering::AcqRel);
    }

    /// SeqLock read of slot `i`, below the capacity, into `out`: spins
    /// while a writer holds the slot and rereads if the version moved.
    fn read_slot(&self, i: usize, out: &mut [u8]) {
        let version = self.version(i);
        let payload = unsafe { self.slot_ptr(i).add(self.payload_offset) };
        let size = self.layout.slot_size;
        loop {
            let v1 = version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            unsafe {
                std::ptr::copy_nonoverlapping(payload, out.as_mut_ptr(), size);
            }
            if version.load(Ordering::Acquire) == v1 {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Append `value`, exactly one element, and return the index it landed
    /// at. `Full` when the vec is at capacity.
    pub fn push_back(&self, value: &[u8]) -> Result<usize, VecError> {
        if !self.mmap.is_writable() {
            return Err(VecError::ReadOnly);
        }
        self.sized(value)?;
        let idx = self.header().reserved.fetch_add(1, Ordering::AcqRel) as usize;
        if idx >= self.capacity {
            self.header().reserved.fetch_sub(1, Ordering::AcqRel);
            return Err(VecError::Full);
        }
        self.write_slot(idx, value);
        // Publish only once this slot is written and every earlier
        // reservation has published, so `len` never covers a slot a reader
        // would find half-written.
        while self
            .header()
            .len
            .compare_exchange_weak(idx as u64, idx as u64 + 1, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Ok(idx)
    }

    /// Remove the last element into `out`, at least one element long:
    /// `Ok(true)` with it copied, `Ok(false)` when the vec is empty.
    pub fn pop_back(&self, out: &mut [u8]) -> Result<bool, VecError> {
        if !self.mmap.is_writable() {
            return Err(VecError::ReadOnly);
        }
        self.room(out)?;
        loop {
            let cur = self.header().len.load(Ordering::Acquire);
            if cur == 0 {
                return Ok(false);
            }
            // Claim from the allocator only while it agrees with the
            // published length: a reservation in flight owns the slot
            // above `len`.
            if self.header().reserved.load(Ordering::Acquire) != cur {
                std::hint::spin_loop();
                continue;
            }
            let new = cur - 1;
            if self
                .header()
                .reserved
                .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.read_slot(new as usize, out);
                // Unpublish after the read, so a reader never sees a length
                // covering a slot this pop is still reading.
                self.header().len.store(new, Ordering::Release);
                return Ok(true);
            }
        }
    }

    /// Copy the element at `i` into `out`, at least one element long:
    /// `Ok(true)` with it copied, `Ok(false)` when `i` is at or past the
    /// length.
    pub fn get(&self, i: usize, out: &mut [u8]) -> Result<bool, VecError> {
        self.room(out)?;
        if i >= self.len() {
            return Ok(false);
        }
        self.read_slot(i, out);
        Ok(true)
    }

    /// Overwrite the element at `i`, which is below the length, with
    /// `value`, exactly one element.
    pub fn set(&self, i: usize, value: &[u8]) -> Result<(), VecError> {
        if !self.mmap.is_writable() {
            return Err(VecError::ReadOnly);
        }
        self.sized(value)?;
        if i >= self.len() {
            return Err(VecError::OutOfBounds);
        }
        self.write_slot(i, value);
        Ok(())
    }

    /// Empty the vec: the length goes to zero, then the allocator, so no
    /// reader is left addressing a slot the allocator has handed back out.
    /// Slot bytes stay in place, unreachable through the bounded index.
    pub fn clear(&self) -> Result<(), VecError> {
        if !self.mmap.is_writable() {
            return Err(VecError::ReadOnly);
        }
        self.header().len.store(0, Ordering::Release);
        self.header().reserved.store(0, Ordering::Release);
        Ok(())
    }

    pub fn flush(&self) -> Result<(), VecError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_vec::SharedVec;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-raw-vec-{name}-{}.bin", std::process::id()))
    }

    const BYTES_24: ElementLayout = ElementLayout { slot_size: 24, alignment: 8, tag: 7 };

    fn element(i: u8) -> [u8; 24] {
        let mut e = [i; 24];
        e[0] = i.wrapping_mul(3);
        e
    }

    #[test]
    fn push_get_set_pop_and_clear_round_trip() {
        let path = tmp("round-trip");
        let vec = RawVec::create(&path, 4, BYTES_24).unwrap();
        assert_eq!(vec.layout(), BYTES_24);
        assert!(vec.is_empty());
        let mut out = [0u8; 24];
        assert!(!vec.get(0, &mut out).unwrap());
        for i in 0..4u8 {
            assert_eq!(vec.push_back(&element(i)).unwrap(), i as usize);
        }
        assert_eq!(vec.push_back(&element(9)).unwrap_err(), VecError::Full);
        assert_eq!(vec.len(), 4);
        assert!(vec.get(2, &mut out).unwrap());
        assert_eq!(out, element(2));
        vec.set(2, &element(42)).unwrap();
        assert!(vec.get(2, &mut out).unwrap());
        assert_eq!(out, element(42));
        assert_eq!(vec.set(4, &element(1)).unwrap_err(), VecError::OutOfBounds);
        assert_eq!(vec.push_back(&[1u8; 23]).unwrap_err(), VecError::PayloadTooLarge);
        assert_eq!(vec.get(0, &mut [0u8; 23]).unwrap_err(), VecError::PayloadTooLarge);
        assert!(vec.pop_back(&mut out).unwrap());
        assert_eq!(out, element(3));
        assert_eq!(vec.len(), 3);
        vec.clear().unwrap();
        assert!(vec.is_empty());
        assert!(!vec.pop_back(&mut out).unwrap());
        assert_eq!(vec.push_back(&element(5)).unwrap(), 0);
    }

    #[test]
    fn a_typed_vec_and_a_raw_one_share_a_region() {
        let path = tmp("interop");
        let typed: SharedVec<[u8; 16]> = SharedVec::create(&path, 8).unwrap();
        typed.push_back(*b"sixteen bytes!!!").unwrap();
        let raw = RawVec::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 0 }).unwrap();
        assert_eq!(raw.len(), 1);
        let mut out = [0u8; 16];
        assert!(raw.get(0, &mut out).unwrap());
        assert_eq!(&out, b"sixteen bytes!!!");
        assert_eq!(raw.push_back(b"raw side wrote!!").unwrap(), 1);
        assert_eq!(typed.get(1), Some(*b"raw side wrote!!"));
        assert_eq!(typed.len(), 2);
        let wrong_tag = RawVec::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 1 });
        assert_eq!(wrong_tag.unwrap_err(), VecError::LayoutMismatch);
        let wrong_size = RawVec::open(&path, 8, ElementLayout { slot_size: 8, alignment: 1, tag: 0 });
        assert_eq!(wrong_size.unwrap_err(), VecError::LayoutMismatch);
        let wrong_capacity = RawVec::open(&path, 4, ElementLayout { slot_size: 16, alignment: 1, tag: 0 });
        assert_eq!(wrong_capacity.unwrap_err(), VecError::LayoutMismatch);
    }

    #[test]
    fn a_wide_alignment_moves_the_element_and_widens_the_slot() {
        assert_eq!(raw_vec_geometry(&BYTES_24), (8, 64));
        assert_eq!(raw_vec_data_offset(&BYTES_24), 64);
        let aligned_32 = ElementLayout { slot_size: 24, alignment: 32, tag: 0 };
        assert_eq!(raw_vec_geometry(&aligned_32), (32, 64));
        assert_eq!(raw_vec_data_offset(&aligned_32), 64);
        let aligned_128 = ElementLayout { slot_size: 24, alignment: 128, tag: 0 };
        assert_eq!(raw_vec_geometry(&aligned_128), (128, 256));
        assert_eq!(raw_vec_data_offset(&aligned_128), 128);
        let big = ElementLayout { slot_size: 100, alignment: 8, tag: 0 };
        assert_eq!(raw_vec_geometry(&big), (8, 128));

        let path = tmp("aligned");
        let vec = RawVec::create(&path, 3, aligned_128).unwrap();
        for i in 0..3u8 {
            vec.push_back(&element(i)).unwrap();
        }
        let mut out = [0u8; 24];
        assert!(vec.get(2, &mut out).unwrap());
        assert_eq!(out, element(2));
        let again = RawVec::open(&path, 3, aligned_128).unwrap();
        assert_eq!(again.len(), 3);
        let narrower = RawVec::open(&path, 3, ElementLayout { slot_size: 24, alignment: 8, tag: 0 });
        assert_eq!(narrower.unwrap_err(), VecError::LayoutMismatch);
    }

    #[test]
    fn a_read_only_mapping_reads_and_refuses_writes() {
        let path = tmp("read-only");
        let vec = RawVec::create(&path, 4, BYTES_24).unwrap();
        vec.push_back(&element(1)).unwrap();
        let reader = RawVec::open_read_only(&path, 4, BYTES_24).unwrap();
        assert!(!reader.is_writable());
        let mut out = [0u8; 24];
        assert!(reader.get(0, &mut out).unwrap());
        assert_eq!(out, element(1));
        assert_eq!(reader.push_back(&element(2)).unwrap_err(), VecError::ReadOnly);
        assert_eq!(reader.pop_back(&mut out).unwrap_err(), VecError::ReadOnly);
        assert_eq!(reader.set(0, &element(2)).unwrap_err(), VecError::ReadOnly);
        assert_eq!(reader.clear().unwrap_err(), VecError::ReadOnly);
    }

    #[test]
    fn an_invalid_layout_is_refused_before_any_file_is_touched() {
        let path = tmp("invalid");
        let empty = RawVec::create(&path, 4, ElementLayout { slot_size: 0, alignment: 8, tag: 0 });
        assert_eq!(empty.unwrap_err(), VecError::LayoutMismatch);
        let misaligned = RawVec::create(&path, 4, ElementLayout { slot_size: 8, alignment: 3, tag: 0 });
        assert_eq!(misaligned.unwrap_err(), VecError::LayoutMismatch);
        let no_room = RawVec::create(&path, 0, BYTES_24);
        assert_eq!(no_room.unwrap_err(), VecError::LayoutMismatch);
        assert!(!path.exists());
    }
}
