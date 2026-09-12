//! `RawCell`: the one-value cell of
//! [`SharedCell`](crate::shared_cell::SharedCell) at a value size fixed at
//! run time instead of by a type parameter, for a caller reaching the cell
//! from another language.
//!
//! It reads and writes the same file format under the same version
//! protocol, and records the caller's size in the header where the typed
//! cell records its type's, so the two open each other's regions when the
//! sizes agree and refuse each other when they do not. A `RawCell` of four
//! bytes and a `SharedCell<u32>` are the same region.
//!
//! One writer, any number of readers. A write bumps the version to odd,
//! copies the bytes and bumps it to even; a read takes the version,
//! copies, and takes it again, retrying while the two differ. Two writers
//! race: the protocol makes a torn read detectable, not a torn write safe.

use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_cell::{CellHeader, SharedCellError, CELL_FILE_SIZE, CELL_MAGIC, PAYLOAD_BYTES};

pub struct RawCell {
    _file: File,
    mmap: MmapMut,
    value_size: usize,
}

unsafe impl Send for RawCell {}
unsafe impl Sync for RawCell {}

impl std::fmt::Debug for RawCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawCell").field("value_size", &self.value_size).finish()
    }
}

impl RawCell {
    /// A value size the region can hold: at least one byte, and no more
    /// than one cell carries.
    fn check_size(value_size: usize) -> Result<(), SharedCellError> {
        if value_size == 0 || value_size > PAYLOAD_BYTES {
            return Err(SharedCellError::PayloadTooLarge);
        }
        Ok(())
    }

    /// Obtain the cell at `path` for a `value_size`-byte value,
    /// initializing an empty one if the path does not yet exist and
    /// attaching to it if it does. Attaching leaves the value and version
    /// in place; a region built for another size is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, value_size: usize) -> Result<Self, SharedCellError> {
        Self::check_size(value_size)?;
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            CELL_FILE_SIZE,
            |ptr| unsafe { Self::init_region(ptr, value_size) },
            |ptr| unsafe { (*(ptr as *const CellHeader)).magic == CELL_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, SharedCellError::LayoutMismatch))?;
        Self::from_region(file, mmap, value_size)
    }

    /// Truncate the cell at `path` and initialize an empty one,
    /// discarding whatever value a live peer holds.
    pub fn reset(path: impl AsRef<Path>, value_size: usize) -> Result<Self, SharedCellError> {
        Self::check_size(value_size)?;
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), CELL_FILE_SIZE, |ptr| unsafe {
            Self::init_region(ptr, value_size)
        })?;
        Self::from_region(file, mmap, value_size)
    }

    /// Attach to the cell at `path`; the file must exist.
    pub fn open(path: impl AsRef<Path>, value_size: usize) -> Result<Self, SharedCellError> {
        Self::check_size(value_size)?;
        let file = crate::region_file::open_existing(path.as_ref())?;
        if (file.metadata()?.len() as usize) < CELL_FILE_SIZE {
            return Err(SharedCellError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(CELL_FILE_SIZE).map_mut(&file)? };
        Self::from_region(file, mmap, value_size)
    }

    /// Lay out a fresh cell: the size first, magic last, because attachers
    /// spin on it.
    ///
    /// # Safety
    /// `ptr` addresses at least `CELL_FILE_SIZE` writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, value_size: usize) {
        let hdr = ptr as *mut CellHeader;
        unsafe {
            (*hdr).size = value_size as u32;
            std::ptr::write_volatile(&raw mut (*hdr).magic, CELL_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one built for another size.
    fn from_region(file: File, mmap: MmapMut, value_size: usize) -> Result<Self, SharedCellError> {
        let header = unsafe { &*(mmap.as_ptr() as *const CellHeader) };
        if header.magic != CELL_MAGIC || header.size as usize != value_size {
            return Err(SharedCellError::LayoutMismatch);
        }
        Ok(Self { _file: file, mmap, value_size })
    }

    #[inline]
    fn header(&self) -> &CellHeader {
        unsafe { &*(self.mmap.as_ptr() as *const CellHeader) }
    }

    /// Where the value sits. The header is cache-line aligned, so its size
    /// rounds up past its fields and the payload is nowhere near the end
    /// of it.
    #[inline]
    fn payload(&self) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(std::mem::offset_of!(CellHeader, payload)) as *mut u8 }
    }

    /// Bytes the value takes.
    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// Twice the writes the cell has taken, and odd while one is in
    /// flight.
    pub fn version(&self) -> u32 {
        self.header().version.load(Ordering::Acquire)
    }

    /// Replace the value with `value`, exactly the value size. The bytes
    /// past it stay zero, so a shorter value never leaves a longer one's
    /// tail behind.
    pub fn set(&self, value: &[u8]) -> Result<(), SharedCellError> {
        if value.len() != self.value_size {
            return Err(SharedCellError::PayloadTooLarge);
        }
        let version: &AtomicU32 = &self.header().version;
        version.fetch_add(1, Ordering::AcqRel);
        // SAFETY: the payload spans PAYLOAD_BYTES and the value fits it;
        // readers spin while the version is odd.
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), self.payload(), self.value_size);
            std::ptr::write_bytes(self.payload().add(self.value_size), 0, PAYLOAD_BYTES - self.value_size);
        }
        version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Copy the value into `out`, at least the value size, retrying while
    /// a write is in flight.
    pub fn get(&self, out: &mut [u8]) -> Result<(), SharedCellError> {
        if out.len() < self.value_size {
            return Err(SharedCellError::PayloadTooLarge);
        }
        let version = &self.header().version;
        loop {
            let before = version.load(Ordering::Acquire);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            // SAFETY: the payload spans the value, and the version check
            // below rejects a copy a writer was changing under.
            unsafe { std::ptr::copy_nonoverlapping(self.payload(), out.as_mut_ptr(), self.value_size) };
            if version.load(Ordering::Acquire) == before {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    pub fn flush(&self) -> Result<(), SharedCellError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_cell::SharedCell;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-raw-cell-{name}-{}.bin", std::process::id()))
    }

    #[test]
    fn the_payload_sits_where_the_typed_cell_puts_it() {
        // The header is cache-line aligned, so its size rounds up well
        // past its fields and is not where the payload ends.
        assert_eq!(std::mem::offset_of!(CellHeader, payload), 16);
        assert!(CELL_FILE_SIZE >= std::mem::offset_of!(CellHeader, payload) + PAYLOAD_BYTES);
    }

    #[test]
    fn a_value_round_trips_and_the_version_counts_the_writes() {
        let path = tmp("round-trip");
        let cell = RawCell::create(&path, 8).unwrap();
        assert_eq!(cell.value_size(), 8);
        assert_eq!(cell.version(), 0);
        let mut out = [0u8; 8];
        cell.get(&mut out).unwrap();
        assert_eq!(out, [0u8; 8], "a fresh cell reads as zeros");
        cell.set(&[7u8; 8]).unwrap();
        assert_eq!(cell.version(), 2, "one write, counted twice");
        cell.get(&mut out).unwrap();
        assert_eq!(out, [7u8; 8]);
        cell.set(&[9u8; 8]).unwrap();
        assert_eq!(cell.version(), 4);
        cell.get(&mut out).unwrap();
        assert_eq!(out, [9u8; 8]);
        assert_eq!(cell.set(&[0u8; 7]).unwrap_err(), SharedCellError::PayloadTooLarge);
        assert_eq!(cell.get(&mut [0u8; 7]).unwrap_err(), SharedCellError::PayloadTooLarge);
    }

    #[test]
    fn a_typed_cell_and_a_raw_one_of_the_same_size_share_a_region() {
        let path = tmp("interop");
        let typed: SharedCell<[u8; 4]> = SharedCell::create(&path).unwrap();
        typed.set(*b"abcd");
        let raw = RawCell::open(&path, 4).unwrap();
        let mut out = [0u8; 4];
        raw.get(&mut out).unwrap();
        assert_eq!(&out, b"abcd");
        raw.set(b"wxyz").unwrap();
        assert_eq!(typed.get(), *b"wxyz");
        assert_eq!(typed.version(), raw.version());
        // A size the region was not built for is refused, in both
        // directions.
        assert_eq!(RawCell::open(&path, 8).unwrap_err(), SharedCellError::LayoutMismatch);
        match SharedCell::<[u8; 8]>::open(&path) {
            Ok(_) => panic!("a typed cell of another size must be refused this region"),
            Err(e) => assert_eq!(e, SharedCellError::LayoutMismatch),
        }
    }

    #[test]
    fn a_shorter_value_leaves_no_tail_of_a_longer_one() {
        let path = tmp("tail");
        let cell = RawCell::create(&path, 52).unwrap();
        cell.set(&[0xAA; 52]).unwrap();
        let mut out = [0u8; 52];
        cell.get(&mut out).unwrap();
        assert_eq!(out, [0xAA; 52]);
        // The same region opened at its own size: the write zeroes what it
        // does not use, so nothing of the previous value survives past it.
        let mut short = [0u8; 52];
        cell.set(&{
            let mut v = [0u8; 52];
            v[..4].copy_from_slice(b"abcd");
            v
        })
        .unwrap();
        cell.get(&mut short).unwrap();
        assert_eq!(&short[..4], b"abcd");
        assert_eq!(&short[4..], &[0u8; 48], "the tail is zero, not the old value");
    }

    #[test]
    fn a_size_no_cell_can_hold_is_refused_before_any_file_is_touched() {
        let path = tmp("too-big");
        assert_eq!(RawCell::create(&path, 0).unwrap_err(), SharedCellError::PayloadTooLarge);
        assert_eq!(RawCell::create(&path, PAYLOAD_BYTES + 1).unwrap_err(), SharedCellError::PayloadTooLarge);
        assert!(!path.exists());
    }
}
