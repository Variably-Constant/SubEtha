//! `RawSlab`: the fixed-capacity slab of records of
//! [`SharedSlab`](crate::shared_slab::SharedSlab) at a record size and
//! alignment fixed at run time instead of by a type parameter, for a
//! caller reaching the region from another language. It reads and writes
//! the same file format under the same per-slot SeqLock, addressed by the
//! caller's own index with no length, allocator or free list. The region
//! records the record size, the slot geometry and a tag the caller
//! chooses, and an open that states another layout is refused.
//!
//! # Slot geometry
//!
//! A slot starts with its version word and pads to the first offset that
//! honors the record's alignment, 8 at the least, which is where the
//! typed slab keeps its record; its stride is that offset plus the record
//! rounded up to whole cache lines, or to the alignment when that is
//! wider. The slot array starts at the first multiple of the alignment
//! past the header. A record at an alignment of at most 8 lands exactly
//! where the typed slab puts it, so the two open each other's regions.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::MmapOptions;

use crate::raw_treiber_stack::ElementLayout;
use crate::shared_slab::{Mapping, SlabError, SlabHeader, SLAB_MAGIC, SLAB_SLOT_PREFIX};

/// Where a slot's record lies and how wide the slot is for `layout`:
/// `(payload_offset, stride)`.
pub fn raw_slab_geometry(layout: &ElementLayout) -> (usize, usize) {
    let payload_offset = SLAB_SLOT_PREFIX.next_multiple_of(layout.alignment);
    let line = 64usize.max(layout.alignment);
    let stride = (payload_offset + layout.slot_size).next_multiple_of(line);
    (payload_offset, stride)
}

/// Where the slot array starts for `layout`: the first multiple of the
/// alignment at or past the header.
pub fn raw_slab_data_offset(layout: &ElementLayout) -> usize {
    size_of::<SlabHeader>().next_multiple_of(layout.alignment)
}

/// Bytes the file holds for `capacity` slots of `layout`.
pub fn raw_slab_file_size(capacity: usize, layout: &ElementLayout) -> usize {
    let (_, stride) = raw_slab_geometry(layout);
    raw_slab_data_offset(layout) + capacity * stride
}

pub struct RawSlab {
    _file: File,
    mmap: Mapping,
    capacity: usize,
    layout: ElementLayout,
    payload_offset: usize,
    stride: usize,
    data_offset: usize,
}

unsafe impl Send for RawSlab {}
unsafe impl Sync for RawSlab {}

impl std::fmt::Debug for RawSlab {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawSlab")
            .field("capacity", &self.capacity)
            .field("layout", &self.layout)
            .field("stride", &self.stride)
            .field("writable", &self.mmap.is_writable())
            .finish()
    }
}

impl RawSlab {
    /// A layout the region can hold: a non-empty record that fits the
    /// header's 32-bit size field and an alignment that is a power of two.
    /// Anything else is a `LayoutMismatch`: no region carries it.
    fn check_layout(layout: &ElementLayout) -> Result<(), SlabError> {
        if layout.slot_size == 0
            || layout.slot_size > u32::MAX as usize
            || layout.alignment == 0
            || !layout.alignment.is_power_of_two()
            || layout.alignment > u32::MAX as usize
        {
            return Err(SlabError::LayoutMismatch);
        }
        Ok(())
    }

    fn assemble(file: File, mmap: Mapping, capacity: usize, layout: ElementLayout) -> Self {
        let (payload_offset, stride) = raw_slab_geometry(&layout);
        Self {
            _file: file,
            mmap,
            capacity,
            layout,
            payload_offset,
            stride,
            data_offset: raw_slab_data_offset(&layout),
        }
    }

    /// Obtain the slab at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. Attaching leaves
    /// live records in place; a region built with a different capacity or
    /// layout is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, SlabError> {
        Self::check_layout(&layout)?;
        if capacity == 0 {
            return Err(SlabError::LayoutMismatch);
        }
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            raw_slab_file_size(capacity, &layout),
            |ptr| unsafe { Self::init_region(ptr, capacity, &layout) },
            |ptr| unsafe { (*(ptr as *const SlabHeader)).magic == SLAB_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, SlabError::LayoutMismatch))?;
        let this = Self::assemble(file, Mapping::Writable(mmap), capacity, layout);
        this.validate()?;
        Ok(this)
    }

    /// Truncate the slab at `path` and initialize an empty one, discarding
    /// every record live peers share.
    pub fn reset(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, SlabError> {
        Self::check_layout(&layout)?;
        if capacity == 0 {
            return Err(SlabError::LayoutMismatch);
        }
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), raw_slab_file_size(capacity, &layout), |ptr| unsafe {
            Self::init_region(ptr, capacity, &layout)
        })?;
        Ok(Self::assemble(file, Mapping::Writable(mmap), capacity, layout))
    }

    /// Attach to the slab at `path`; the file must exist.
    pub fn open(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, SlabError> {
        Self::check_layout(&layout)?;
        let file = crate::region_file::open_existing(path.as_ref())?;
        let total = raw_slab_file_size(capacity, &layout);
        if file.metadata()?.len() < total as u64 {
            return Err(SlabError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let this = Self::assemble(file, Mapping::Writable(mmap), capacity, layout);
        this.validate()?;
        Ok(this)
    }

    /// Attach to the slab at `path` with read access alone; writes return
    /// [`SlabError::ReadOnly`].
    pub fn open_read_only(path: impl AsRef<Path>, capacity: usize, layout: ElementLayout) -> Result<Self, SlabError> {
        Self::check_layout(&layout)?;
        let file = crate::region_file::open_read_only(path.as_ref())?;
        let total = raw_slab_file_size(capacity, &layout);
        if file.metadata()?.len() < total as u64 {
            return Err(SlabError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map(&file)? };
        let this = Self::assemble(file, Mapping::ReadOnly(mmap), capacity, layout);
        this.validate()?;
        Ok(this)
    }

    /// Lay out an empty slab: sizes and geometry first, magic last, because
    /// attachers spin on it. The zeroed region is already the empty slot
    /// array, every version 0 and even.
    ///
    /// # Safety
    /// `ptr` addresses at least `raw_slab_file_size(capacity, layout)`
    /// writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, layout: &ElementLayout) {
        let (payload_offset, stride) = raw_slab_geometry(layout);
        let hdr = ptr as *mut SlabHeader;
        unsafe {
            (*hdr).slot_size = stride as u32;
            (*hdr).capacity = capacity as u64;
            (*hdr).payload_offset = payload_offset as u32;
            (*hdr).element_size = layout.slot_size as u32;
            (*hdr).alignment = layout.alignment as u32;
            (*hdr).layout_tag = layout.tag;
            std::ptr::write_volatile(&raw mut (*hdr).magic, SLAB_MAGIC);
        }
    }

    /// Whether the header on disk is the one this mapping expects: the
    /// capacity, the slot geometry, the record size and the tag must all
    /// match. The alignment field is informational; the geometry it
    /// produced is what is checked.
    fn validate(&self) -> Result<(), SlabError> {
        let hdr = self.header();
        if hdr.magic != SLAB_MAGIC
            || hdr.capacity != self.capacity as u64
            || hdr.slot_size as usize != self.stride
            || hdr.payload_offset as usize != self.payload_offset
            || hdr.element_size as usize != self.layout.slot_size
            || hdr.layout_tag != self.layout.tag
        {
            return Err(SlabError::LayoutMismatch);
        }
        Ok(())
    }

    #[inline]
    fn header(&self) -> &SlabHeader {
        unsafe { &*(self.mmap.as_ptr() as *const SlabHeader) }
    }

    #[inline]
    fn slot_ptr(&self, i: usize) -> *const u8 {
        unsafe { self.mmap.as_ptr().add(self.data_offset).add(i * self.stride) }
    }

    #[inline]
    fn version(&self, i: usize) -> &AtomicU32 {
        unsafe { &*(self.slot_ptr(i) as *const AtomicU32) }
    }

    /// Slots this slab addresses.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether this mapping may write.
    pub fn is_writable(&self) -> bool {
        self.mmap.is_writable()
    }

    /// The layout this handle opened the region with.
    pub fn layout(&self) -> ElementLayout {
        self.layout
    }

    /// Bytes from one slot to the next.
    pub fn slot_stride(&self) -> usize {
        self.stride
    }

    /// Copy the record at `i` into `out`, at least one record long. Spins
    /// while a writer holds the slot and rereads if the version moved
    /// under it, so the bytes returned were never observed half-written.
    /// A slot nothing has written reads as zeros.
    pub fn get(&self, i: usize, out: &mut [u8]) -> Result<(), SlabError> {
        if i >= self.capacity {
            return Err(SlabError::OutOfBounds);
        }
        if out.len() < self.layout.slot_size {
            return Err(SlabError::PayloadTooLarge);
        }
        let version = self.version(i);
        let payload = unsafe { self.slot_ptr(i).add(self.payload_offset) };
        loop {
            let v1 = version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            unsafe {
                std::ptr::copy_nonoverlapping(payload, out.as_mut_ptr(), self.layout.slot_size);
            }
            if version.load(Ordering::Acquire) == v1 {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    /// Write `value`, exactly one record, at `i`. One writer per slot: two
    /// writers on the same slot race, since the SeqLock makes a torn read
    /// detectable and does not make a torn write safe.
    pub fn set(&self, i: usize, value: &[u8]) -> Result<(), SlabError> {
        if i >= self.capacity {
            return Err(SlabError::OutOfBounds);
        }
        if !self.mmap.is_writable() {
            return Err(SlabError::ReadOnly);
        }
        if value.len() != self.layout.slot_size {
            return Err(SlabError::PayloadTooLarge);
        }
        let version = self.version(i);
        let payload = unsafe { self.slot_ptr(i).add(self.payload_offset) as *mut u8 };
        version.fetch_add(1, Ordering::AcqRel);
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), payload, self.layout.slot_size);
        }
        version.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// How many times slot `i` has been written, doubled: even at rest,
    /// odd while a writer holds it.
    pub fn slot_version(&self, i: usize) -> Result<u32, SlabError> {
        if i >= self.capacity {
            return Err(SlabError::OutOfBounds);
        }
        Ok(self.version(i).load(Ordering::Acquire))
    }

    pub fn flush(&self) -> Result<(), SlabError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_slab::SharedSlab;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-raw-slab-{name}-{}.bin", std::process::id()))
    }

    const BYTES_100: ElementLayout = ElementLayout { slot_size: 100, alignment: 8, tag: 11 };

    fn record(i: u8) -> [u8; 100] {
        let mut r = [i; 100];
        r[99] = i.wrapping_add(1);
        r
    }

    #[test]
    fn set_get_and_version_round_trip_at_the_callers_index() {
        let path = tmp("round-trip");
        let slab = RawSlab::create(&path, 4, BYTES_100).unwrap();
        assert_eq!(slab.layout(), BYTES_100);
        assert_eq!(slab.capacity(), 4);
        let mut out = [0u8; 100];
        slab.get(3, &mut out).unwrap();
        assert_eq!(out, [0u8; 100]);
        assert_eq!(slab.slot_version(3).unwrap(), 0);
        slab.set(3, &record(7)).unwrap();
        slab.get(3, &mut out).unwrap();
        assert_eq!(out, record(7));
        assert_eq!(slab.slot_version(3).unwrap(), 2);
        assert_eq!(slab.set(4, &record(1)).unwrap_err(), SlabError::OutOfBounds);
        assert_eq!(slab.get(4, &mut out).unwrap_err(), SlabError::OutOfBounds);
        assert_eq!(slab.slot_version(4).unwrap_err(), SlabError::OutOfBounds);
        assert_eq!(slab.set(0, &[1u8; 99]).unwrap_err(), SlabError::PayloadTooLarge);
        assert_eq!(slab.get(0, &mut [0u8; 99]).unwrap_err(), SlabError::PayloadTooLarge);
    }

    #[test]
    fn a_typed_slab_and_a_raw_one_share_a_region() {
        let path = tmp("interop");
        let typed: SharedSlab<[u8; 16]> = SharedSlab::create(&path, 8).unwrap();
        typed.set(5, *b"sixteen bytes!!!").unwrap();
        let raw = RawSlab::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 0 }).unwrap();
        let mut out = [0u8; 16];
        raw.get(5, &mut out).unwrap();
        assert_eq!(&out, b"sixteen bytes!!!");
        raw.set(2, b"raw side wrote!!").unwrap();
        assert_eq!(typed.get(2).unwrap(), *b"raw side wrote!!");
        assert_eq!(typed.slot_version(2).unwrap(), 2);
        let wrong_tag = RawSlab::open(&path, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 1 });
        assert_eq!(wrong_tag.unwrap_err(), SlabError::LayoutMismatch);
        let wrong_size = RawSlab::open(&path, 8, ElementLayout { slot_size: 8, alignment: 1, tag: 0 });
        assert_eq!(wrong_size.unwrap_err(), SlabError::LayoutMismatch);
        let wrong_capacity = RawSlab::open(&path, 4, ElementLayout { slot_size: 16, alignment: 1, tag: 0 });
        assert_eq!(wrong_capacity.unwrap_err(), SlabError::LayoutMismatch);
    }

    #[test]
    fn a_wide_alignment_moves_the_record_and_widens_the_slot() {
        assert_eq!(raw_slab_geometry(&BYTES_100), (8, 128));
        assert_eq!(raw_slab_data_offset(&BYTES_100), 64);
        let aligned_64 = ElementLayout { slot_size: 100, alignment: 64, tag: 0 };
        assert_eq!(raw_slab_geometry(&aligned_64), (64, 192));
        assert_eq!(raw_slab_data_offset(&aligned_64), 64);
        let aligned_256 = ElementLayout { slot_size: 100, alignment: 256, tag: 0 };
        assert_eq!(raw_slab_geometry(&aligned_256), (256, 512));
        assert_eq!(raw_slab_data_offset(&aligned_256), 256);

        let path = tmp("aligned");
        let slab = RawSlab::create(&path, 3, aligned_256).unwrap();
        slab.set(2, &record(9)).unwrap();
        let mut out = [0u8; 100];
        slab.get(2, &mut out).unwrap();
        assert_eq!(out, record(9));
        let again = RawSlab::open(&path, 3, aligned_256).unwrap();
        again.get(2, &mut out).unwrap();
        assert_eq!(out, record(9));
        let narrower = RawSlab::open(&path, 3, BYTES_100);
        assert_eq!(narrower.unwrap_err(), SlabError::LayoutMismatch);
    }

    #[test]
    fn a_read_only_mapping_reads_and_refuses_writes() {
        let path = tmp("read-only");
        let slab = RawSlab::create(&path, 4, BYTES_100).unwrap();
        slab.set(1, &record(1)).unwrap();
        let reader = RawSlab::open_read_only(&path, 4, BYTES_100).unwrap();
        assert!(!reader.is_writable());
        let mut out = [0u8; 100];
        reader.get(1, &mut out).unwrap();
        assert_eq!(out, record(1));
        assert_eq!(reader.set(1, &record(2)).unwrap_err(), SlabError::ReadOnly);
    }

    #[test]
    fn an_invalid_layout_is_refused_before_any_file_is_touched() {
        let path = tmp("invalid");
        let empty = RawSlab::create(&path, 4, ElementLayout { slot_size: 0, alignment: 8, tag: 0 });
        assert_eq!(empty.unwrap_err(), SlabError::LayoutMismatch);
        let misaligned = RawSlab::create(&path, 4, ElementLayout { slot_size: 8, alignment: 6, tag: 0 });
        assert_eq!(misaligned.unwrap_err(), SlabError::LayoutMismatch);
        let no_room = RawSlab::create(&path, 0, BYTES_100);
        assert_eq!(no_room.unwrap_err(), SlabError::LayoutMismatch);
        assert!(!path.exists());
    }
}
