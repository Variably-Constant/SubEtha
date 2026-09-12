//! `SharedArray` - a flat, fixed-stride array written once and read by
//! many processes.
//!
//! Every other array-shaped primitive here gives each element its own
//! synchronization word and rounds its stride up to a cache line, so a
//! reader never sees a half-written element. That is the right trade for
//! something written while it is read, and pure cost for a table baked at
//! build time and never written again: a 12-byte row costs 64 bytes of
//! file, and a 3.5 MB table lands at 18 MB.
//!
//! This is the other trade. The stride is exactly the element size, there
//! is no per-element word, and the whole span is readable as one slice.
//! What it gives up is concurrent writing: a reader attached to an array
//! being written sees whatever the writer has reached.
//!
//! # The contract
//!
//! One writer fills it, calls [`seal`](SharedArray::seal), and from then
//! on it is read-only for every process including the one that made it.
//! Readers attach with [`open_read_only`](SharedArray::open_read_only),
//! which maps without write access, so a reader cannot modify the file
//! even by mistake.
//!
//! # Why the stride is in the header
//!
//! A caller could get this shape today by treating a byte region as an
//! array and doing its own index arithmetic. The reason not to is that
//! nothing then checks the reader and the writer agree about the element
//! size: a reader using a different stride reads misaligned bytes that
//! look exactly like data. Here the stride is written into the header and
//! every attach compares against it, so a disagreement is refused the way
//! every other shape mismatch in this crate is refused.

use std::fs::OpenOptions;
use std::path::Path;

use memmap2::{Mmap, MmapMut};

/// Marks a file as an array of this shape.
pub const ARRAY_MAGIC: u32 = 0x4152_5241;

/// Why an operation on the array could not be carried out.
#[derive(Debug)]
pub enum ArrayError {
    /// The file is not an array, or its header says another shape.
    LayoutMismatch,
    /// An index past the end.
    OutOfBounds { index: u64, len: u64 },
    /// A value slice that was not the element size.
    WrongSize { expected: usize, found: usize },
    /// A write to an array that has been sealed.
    Sealed,
    /// A write through a read-only attachment.
    ReadOnly,
    /// Zero elements, or a zero-byte element.
    EmptyShape,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for ArrayError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.kind())
    }
}

/// The 64-byte header. Its own alignment is a cache line so the data
/// following it starts on one; the elements themselves are not padded.
#[repr(C, align(64))]
pub struct ArrayHeader {
    pub magic: u32,
    /// Bytes per element, and the stride exactly.
    pub stride: u32,
    /// Elements the array holds.
    pub len: u64,
    /// Non-zero once the writer has finished. A sealed array refuses
    /// every write, including from the handle that created it.
    pub sealed: u64,
    _pad: [u8; 40],
}

const _: () = assert!(size_of::<ArrayHeader>() == 64);

/// Where the elements begin.
pub const ARRAY_DATA_OFFSET: usize = 64;

/// The file size an array of this shape needs.
pub fn array_file_size(len: u64, stride: u32) -> usize {
    ARRAY_DATA_OFFSET + (len as usize) * (stride as usize)
}

enum Mapping {
    Writable(MmapMut),
    ReadOnly(Mmap),
}

impl Mapping {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Writable(m) => m,
            Self::ReadOnly(m) => m,
        }
    }
}

pub struct SharedArray {
    _file: std::fs::File,
    map: Mapping,
    stride: u32,
    len: u64,
}

impl SharedArray {
    /// Create an array of `len` elements of `stride` bytes at `path`, or
    /// attach to one already there with the same shape.
    ///
    /// The elements start zeroed and the array starts unsealed.
    pub fn create(path: impl AsRef<Path>, len: u64, stride: u32) -> Result<Self, ArrayError> {
        if len == 0 || stride == 0 {
            return Err(ArrayError::EmptyShape);
        }
        // truncate(false) is the whole point rather than a formality:
        // this call attaches to an array already there with the same
        // shape, so truncating would destroy a baked table on every
        // reopen. Stated explicitly because the lint that asks for it
        // suggests the opposite default.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.set_len(array_file_size(len, stride) as u64)?;
        // SAFETY: the file is sized to the layout above and every access
        // below stays inside it.
        let mut map = unsafe { MmapMut::map_mut(&file)? };
        {
            // SAFETY: the mapping is at least 64 bytes and the header
            // sits at offset 0.
            let header = unsafe { &mut *(map.as_mut_ptr() as *mut ArrayHeader) };
            if header.magic == ARRAY_MAGIC {
                if header.stride != stride || header.len != len {
                    return Err(ArrayError::LayoutMismatch);
                }
            } else {
                header.magic = ARRAY_MAGIC;
                header.stride = stride;
                header.len = len;
                header.sealed = 0;
            }
        }
        Ok(Self { _file: file, map: Mapping::Writable(map), stride, len })
    }

    /// Attach without write access.
    ///
    /// The mapping carries no write permission, so a reader cannot modify
    /// the file even through a mistake in its own code. The header must
    /// agree about both the stride and the length.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        expected_len: u64,
        expected_stride: u32,
    ) -> Result<Self, ArrayError> {
        let file = OpenOptions::new().read(true).open(path)?;
        // SAFETY: read-only mapping of a file whose header is checked
        // immediately below before any element is addressed.
        let map = unsafe { Mmap::map(&file)? };
        if map.len() < ARRAY_DATA_OFFSET {
            return Err(ArrayError::LayoutMismatch);
        }
        // SAFETY: length checked above.
        let header = unsafe { &*(map.as_ptr() as *const ArrayHeader) };
        if header.magic != ARRAY_MAGIC
            || header.stride != expected_stride
            || header.len != expected_len
        {
            return Err(ArrayError::LayoutMismatch);
        }
        if map.len() < array_file_size(expected_len, expected_stride) {
            return Err(ArrayError::LayoutMismatch);
        }
        Ok(Self {
            _file: file,
            map: Mapping::ReadOnly(map),
            stride: expected_stride,
            len: expected_len,
        })
    }

    fn header(&self) -> &ArrayHeader {
        // SAFETY: every constructor checked the mapping covers a header.
        unsafe { &*(self.map.bytes().as_ptr() as *const ArrayHeader) }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn stride(&self) -> u32 {
        self.stride
    }

    /// Whether the writer has finished with it.
    pub fn is_sealed(&self) -> bool {
        self.header().sealed != 0
    }

    pub fn is_writable(&self) -> bool {
        matches!(self.map, Mapping::Writable(_)) && !self.is_sealed()
    }

    /// Every element as one slice, without copying.
    ///
    /// This is the accessor a baked table exists for: a consumer maps the
    /// file and reads the whole span, paying nothing per element.
    pub fn as_slice(&self) -> &[u8] {
        let end = ARRAY_DATA_OFFSET + (self.len as usize) * (self.stride as usize);
        &self.map.bytes()[ARRAY_DATA_OFFSET..end]
    }

    /// One element, without copying.
    pub fn get(&self, index: u64) -> Result<&[u8], ArrayError> {
        if index >= self.len {
            return Err(ArrayError::OutOfBounds { index, len: self.len });
        }
        let start = ARRAY_DATA_OFFSET + (index as usize) * (self.stride as usize);
        Ok(&self.map.bytes()[start..start + self.stride as usize])
    }

    /// Write one element. Refused once the array is sealed, and on a
    /// read-only attachment.
    pub fn set(&mut self, index: u64, value: &[u8]) -> Result<(), ArrayError> {
        if value.len() != self.stride as usize {
            return Err(ArrayError::WrongSize {
                expected: self.stride as usize,
                found: value.len(),
            });
        }
        if index >= self.len {
            return Err(ArrayError::OutOfBounds { index, len: self.len });
        }
        if self.is_sealed() {
            return Err(ArrayError::Sealed);
        }
        let start = ARRAY_DATA_OFFSET + (index as usize) * (self.stride as usize);
        match &mut self.map {
            Mapping::Writable(m) => {
                m[start..start + self.stride as usize].copy_from_slice(value);
                Ok(())
            }
            Mapping::ReadOnly(_) => Err(ArrayError::ReadOnly),
        }
    }

    /// Fill the array from a contiguous buffer of `len * stride` bytes.
    ///
    /// The whole point of a baked table: one copy rather than one call per
    /// element.
    pub fn fill_from(&mut self, values: &[u8]) -> Result<(), ArrayError> {
        let wanted = (self.len as usize) * (self.stride as usize);
        if values.len() != wanted {
            return Err(ArrayError::WrongSize { expected: wanted, found: values.len() });
        }
        if self.is_sealed() {
            return Err(ArrayError::Sealed);
        }
        match &mut self.map {
            Mapping::Writable(m) => {
                m[ARRAY_DATA_OFFSET..ARRAY_DATA_OFFSET + wanted].copy_from_slice(values);
                Ok(())
            }
            Mapping::ReadOnly(_) => Err(ArrayError::ReadOnly),
        }
    }

    /// Mark the array finished. Every later write is refused, through any
    /// handle, in any process.
    ///
    /// Flushed before the flag is set, so a reader that sees the seal sees
    /// the data behind it.
    pub fn seal(&mut self) -> Result<(), ArrayError> {
        match &mut self.map {
            Mapping::Writable(m) => {
                m.flush()?;
                // SAFETY: the mapping covers the header.
                let header = unsafe { &mut *(m.as_mut_ptr() as *mut ArrayHeader) };
                header.sealed = 1;
                m.flush()?;
                Ok(())
            }
            Mapping::ReadOnly(_) => Err(ArrayError::ReadOnly),
        }
    }

    /// Push to disk and wait.
    pub fn flush(&self) -> Result<(), ArrayError> {
        match &self.map {
            Mapping::Writable(m) => Ok(m.flush()?),
            // Nothing to write back.
            Mapping::ReadOnly(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha_shared_array_{name}_{}.bin", std::process::id()));
        p
    }

    fn cleanup(p: &Path) {
        match std::fs::remove_file(p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("could not clear {}: {e}", p.display()),
        }
    }

    /// Write a table and seal it, leaving the writer's mapping closed by
    /// scope rather than by a bare drop, so a writeback failure surfaces
    /// through `seal` instead of vanishing into a discarded unmap.
    fn bake(path: &Path, len: u64, stride: u32, fill: impl FnOnce(&mut SharedArray)) {
        let mut writer = SharedArray::create(path, len, stride).expect("created");
        fill(&mut writer);
        writer.seal().expect("seal");
    }

    #[test]
    fn the_stride_is_the_element_size_with_no_padding() {
        let path = scratch("stride");
        cleanup(&path);
        // Twelve bytes is the case this primitive exists for: a u64 key
        // and a u32 value, which every locked primitive rounds to 64.
        let a = SharedArray::create(&path, 1000, 12).expect("created");
        assert_eq!(a.stride(), 12);
        assert_eq!(array_file_size(1000, 12), 64 + 12_000);
        let on_disk = std::fs::metadata(&path).expect("stat").len();
        assert_eq!(on_disk, 64 + 12_000, "header plus exactly the data");
        cleanup(&path);
    }

    #[test]
    fn a_filled_array_reads_back_element_by_element_and_whole() {
        let path = scratch("fill");
        cleanup(&path);
        let mut a = SharedArray::create(&path, 4, 8).expect("created");

        let mut buffer = Vec::new();
        for i in 0u64..4 {
            buffer.extend_from_slice(&(i * 11).to_le_bytes());
        }
        a.fill_from(&buffer).expect("fill");

        for i in 0u64..4 {
            let e = a.get(i).expect("get");
            assert_eq!(u64::from_le_bytes(e.try_into().expect("eight bytes")), i * 11);
        }
        assert_eq!(a.as_slice(), &buffer[..], "the whole span, zero copy");

        cleanup(&path);
    }

    #[test]
    fn a_sealed_array_refuses_every_write_including_from_its_maker() {
        let path = scratch("seal");
        cleanup(&path);
        let mut a = SharedArray::create(&path, 4, 8).expect("created");
        a.set(0, &1u64.to_le_bytes()).expect("write before sealing");
        assert!(a.is_writable());

        a.seal().expect("seal");
        assert!(a.is_sealed());
        assert!(!a.is_writable());
        assert!(matches!(a.set(1, &2u64.to_le_bytes()), Err(ArrayError::Sealed)));
        assert!(matches!(a.fill_from(&[0u8; 32]), Err(ArrayError::Sealed)));
        // And what was written before the seal is still there.
        assert_eq!(
            u64::from_le_bytes(a.get(0).expect("get").try_into().expect("eight")),
            1
        );
        cleanup(&path);
    }

    #[test]
    fn a_read_only_attachment_cannot_write_and_says_so() {
        let path = scratch("ro");
        cleanup(&path);
        bake(&path, 4, 8, |w| {
            w.set(2, &77u64.to_le_bytes()).expect("write");
        });

        let mut reader = SharedArray::open_read_only(&path, 4, 8).expect("opened");
        assert!(!reader.is_writable());
        assert_eq!(
            u64::from_le_bytes(reader.get(2).expect("get").try_into().expect("eight")),
            77
        );
        // The seal is checked before the mapping kind, so a sealed
        // read-only array reports the seal; what matters is that neither
        // lets a write through.
        assert!(matches!(
            reader.set(0, &1u64.to_le_bytes()),
            Err(ArrayError::Sealed) | Err(ArrayError::ReadOnly)
        ));
        cleanup(&path);
    }

    #[test]
    fn a_reader_that_disagrees_about_the_shape_is_refused() {
        let path = scratch("shape");
        cleanup(&path);
        bake(&path, 100, 12, |_| {});

        // The whole reason the stride lives in the header: a reader using
        // 16 where the writer used 12 would otherwise read misaligned
        // bytes that look exactly like data.
        assert!(matches!(
            SharedArray::open_read_only(&path, 100, 16),
            Err(ArrayError::LayoutMismatch)
        ));
        assert!(matches!(
            SharedArray::open_read_only(&path, 50, 12),
            Err(ArrayError::LayoutMismatch)
        ));
        SharedArray::open_read_only(&path, 100, 12).expect("the true shape opens");
        cleanup(&path);
    }

    #[test]
    fn an_index_past_the_end_is_refused_rather_than_read() {
        let path = scratch("bounds");
        cleanup(&path);
        let mut a = SharedArray::create(&path, 4, 8).expect("created");
        assert!(matches!(a.get(4), Err(ArrayError::OutOfBounds { index: 4, len: 4 })));
        assert!(matches!(
            a.set(9, &0u64.to_le_bytes()),
            Err(ArrayError::OutOfBounds { index: 9, len: 4 })
        ));
        cleanup(&path);
    }

    #[test]
    fn a_wrongly_sized_write_is_refused_rather_than_padded() {
        let path = scratch("size");
        cleanup(&path);
        let mut a = SharedArray::create(&path, 4, 8).expect("created");
        assert!(matches!(
            a.set(0, &[0u8; 4]),
            Err(ArrayError::WrongSize { expected: 8, found: 4 })
        ));
        assert!(matches!(
            a.fill_from(&[0u8; 8]),
            Err(ArrayError::WrongSize { expected: 32, found: 8 })
        ));
        cleanup(&path);
    }

    #[test]
    fn an_empty_shape_is_refused() {
        let path = scratch("empty");
        cleanup(&path);
        assert!(matches!(SharedArray::create(&path, 0, 8), Err(ArrayError::EmptyShape)));
        assert!(matches!(SharedArray::create(&path, 8, 0), Err(ArrayError::EmptyShape)));
        cleanup(&path);
    }

    #[test]
    fn a_second_reader_sees_the_table_the_writer_baked() {
        let path = scratch("shared");
        cleanup(&path);
        bake(&path, 8, 8, |w| {
            for i in 0u64..8 {
                w.set(i, &(i * 3).to_le_bytes()).expect("write");
            }
        });

        let first = SharedArray::open_read_only(&path, 8, 8).expect("first reader");
        let second = SharedArray::open_read_only(&path, 8, 8).expect("second reader");
        assert!(first.is_sealed() && second.is_sealed());
        assert_eq!(first.as_slice(), second.as_slice());
        for i in 0u64..8 {
            let e = second.get(i).expect("get");
            assert_eq!(u64::from_le_bytes(e.try_into().expect("eight")), i * 3);
        }
        cleanup(&path);
    }

    #[test]
    fn the_file_is_the_data_plus_one_header_at_a_realistic_size() {
        // A table of 290_000 rows carrying a u64 key and a u32 value.
        // Recorded as a test so the saving is a fact rather than a claim
        // in a commit message.
        let rows = 290_000u64;
        let stride = 12u32;
        let flat = array_file_size(rows, stride);
        assert_eq!(flat, 64 + 3_480_000);

        // What a per-element synchronization word and a cache-line stride
        // would have cost for the same data.
        let padded = 64 + (rows as usize) * 64;
        assert_eq!(padded, 64 + 18_560_000);
        assert!(flat * 5 < padded, "the flat form is more than five times smaller");
    }
}
