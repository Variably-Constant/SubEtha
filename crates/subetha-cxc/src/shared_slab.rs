//! `SharedSlab<T>` - a fixed-capacity MMF slab of records, each slot its
//! own SeqLock cell, addressed by an index the caller chooses.
//!
//! # Where it sits
//!
//! [`SharedVec`](crate::shared_vec::SharedVec) is append-plus-index: a
//! pusher reserves a slot, writes it, and publishes an index only once
//! every earlier reservation has committed. Its slot is one cache line,
//! so `VEC_PAYLOAD_BYTES` caps a payload at 52 bytes.
//!
//! [`SharedRegion`](crate::shared_region::SharedRegion) carries records
//! of any size and hands out slots from a free list, but its accessors
//! are a plain read and write, so a reader racing a writer tears.
//!
//! This is the third point: records of any size, addressed by the
//! caller's own index, read under the same per-slot SeqLock. There is no
//! length, no allocator and no free list. A caller that persists ids -
//! a write-ahead log naming a slot, a snapshot restoring one - needs the
//! index to be its own, and needs a released id never to address another
//! record.
//!
//! # Slot layout
//!
//! ```text
//! | SlabHeader (64B) | Slot 0 | Slot 1 | ... |
//!
//! Slot = | version: AtomicU32 | pad 4 | payload: size_of::<T>() | pad |
//! ```
//!
//! A slot is rounded up to a multiple of 64 bytes and the array is
//! 64-byte aligned, so no slot's version word shares a cache line with
//! another slot's payload. A record spanning several lines does not
//! weaken the SeqLock: the version is a single atomic and the reader's
//! two loads bracket the whole copy, so a tear across three lines is
//! caught exactly as one within a line is. What a larger payload costs
//! is retry work, not correctness - the slot is held odd for as long as
//! the copy takes, so a colliding reader retries more often and copies
//! more each time.
//!
//! # Reading a slot nothing has written
//!
//! A fresh region is zeroed, so an untouched slot reads as the zero bit
//! pattern of `T` rather than reporting itself as empty. There is no
//! occupancy bit: a caller that has to tell absent from written encodes
//! that in `T`, which a record whose zero value already means absent
//! gets for free.
//!
//! # Concurrency
//!
//! One writer per slot, any number of readers, no coordination between
//! slots. Two writers on the SAME slot are a data race the SeqLock does
//! not resolve - it makes a torn read detectable, not a torn write
//! safe - so a caller writing the same index from two threads
//! serialises that itself.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::{Mmap, MmapMut, MmapOptions};

pub const SLAB_MAGIC: u32 = 0x4150_534C;

/// Bytes a slot spends on its version word and the padding that keeps
/// the payload 8-byte aligned behind it.
pub const SLAB_SLOT_PREFIX: usize = 8;

/// How this process mapped the file. `MmapMut` demands a read+write
/// file handle, which a consumer holding read access alone cannot get.
enum Mapping {
    Writable(MmapMut),
    ReadOnly(Mmap),
}

impl Mapping {
    #[inline]
    fn as_ptr(&self) -> *const u8 {
        match self {
            Mapping::Writable(m) => m.as_ptr(),
            Mapping::ReadOnly(m) => m.as_ptr(),
        }
    }

    #[inline]
    fn is_writable(&self) -> bool {
        matches!(self, Mapping::Writable(_))
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        match self {
            Mapping::Writable(m) => m.flush(),
            Mapping::ReadOnly(_) => Ok(()),
        }
    }
}

#[repr(C, align(64))]
pub struct SlabHeader {
    pub magic: u32,
    /// Full stride of one slot, version word included.
    pub slot_size: u32,
    pub capacity: u64,
    _pad: [u8; 48],
}

const _: () = {
    assert!(size_of::<SlabHeader>() == 64);
};

/// Stride of one slot holding a `T`: the version word plus the record,
/// rounded up to a whole number of cache lines.
pub const fn slab_slot_size<T>() -> usize {
    let raw = SLAB_SLOT_PREFIX + size_of::<T>();
    raw.div_ceil(64) * 64
}

pub const fn slab_file_size<T>(capacity: usize) -> usize {
    size_of::<SlabHeader>() + capacity * slab_slot_size::<T>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabError {
    OutOfBounds,
    LayoutMismatch,
    ReadOnly,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for SlabError {
    fn from(e: std::io::Error) -> Self {
        SlabError::IoError(e.kind())
    }
}

impl std::fmt::Display for SlabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlabError::OutOfBounds => write!(f, "slot index out of bounds"),
            SlabError::LayoutMismatch => write!(f, "slab layout mismatch"),
            SlabError::ReadOnly => write!(f, "slab opened read-only"),
            SlabError::IoError(k) => write!(f, "slab io error: {k:?}"),
        }
    }
}

impl std::error::Error for SlabError {}

pub struct SharedSlab<T: Copy + 'static> {
    _file: File,
    mmap: Mapping,
    capacity: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedSlab<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedSlab<T> {}

impl<T: Copy + 'static> SharedSlab<T> {
    /// Obtain the slab at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. Attaching
    /// leaves live records in place; a region built for a different
    /// capacity or a different record size is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, SlabError> {
        assert!(capacity >= 1);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            slab_file_size::<T>(capacity),
            |ptr| unsafe { Self::init_region(ptr, capacity, slab_slot_size::<T>()) },
            |ptr| unsafe { (*(ptr as *const SlabHeader)).magic == SLAB_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, SlabError::LayoutMismatch))?;
        let this = Self {
            _file: file,
            mmap: Mapping::Writable(mmap),
            capacity,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        this.validate(capacity)?;
        Ok(this)
    }

    /// Truncate the slab at `path` and initialize an empty one,
    /// discarding every record live peers share. For a caller that knows
    /// it owns the path.
    pub fn reset(path: impl AsRef<Path>, capacity: usize) -> Result<Self, SlabError> {
        assert!(capacity >= 1);
        let (file, mmap) = crate::mmf_attach::reset(
            path.as_ref(),
            slab_file_size::<T>(capacity),
            |ptr| unsafe { Self::init_region(ptr, capacity, slab_slot_size::<T>()) },
        )?;
        Ok(Self {
            _file: file,
            mmap: Mapping::Writable(mmap),
            capacity,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Attach to an existing slab.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, SlabError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = slab_file_size::<T>(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(SlabError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let this = Self {
            _file: file,
            mmap: Mapping::Writable(mmap),
            capacity: expected_capacity,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        this.validate(expected_capacity)?;
        Ok(this)
    }

    /// Open a slab this process may only read.
    ///
    /// [`open`](Self::open) needs a read+write file handle, which a
    /// consumer of a privileged producer's slab does not have. Reads
    /// behave identically, under the same per-slot SeqLock; writes
    /// return [`SlabError::ReadOnly`].
    pub fn open_read_only(
        path: impl AsRef<Path>,
        expected_capacity: usize,
    ) -> Result<Self, SlabError> {
        let file = OpenOptions::new().read(true).open(path.as_ref())?;
        let total = slab_file_size::<T>(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(SlabError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map(&file)? };
        let this = Self {
            _file: file,
            mmap: Mapping::ReadOnly(mmap),
            capacity: expected_capacity,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        this.validate(expected_capacity)?;
        Ok(this)
    }

    /// Lay out an empty slab: sizes first, magic last, because attachers
    /// spin on it. The zeroed region is already the empty slot array,
    /// every version 0 and even.
    ///
    /// # Safety
    /// `ptr` addresses at least `slab_file_size::<T>(capacity)` writable
    /// zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, slot_size: usize) {
        let hdr = ptr as *mut SlabHeader;
        unsafe {
            (*hdr).slot_size = slot_size as u32;
            (*hdr).capacity = capacity as u64;
            std::ptr::write_volatile(&raw mut (*hdr).magic, SLAB_MAGIC);
        }
    }

    #[inline]
    fn header(&self) -> &SlabHeader {
        unsafe { &*(self.mmap.as_ptr() as *const SlabHeader) }
    }

    /// Whether the header on disk is the one this mapping expects. The
    /// slot size is checked as well as the capacity: two callers whose
    /// `T` differs in size derive different strides from the same file
    /// and would read each other's records at the wrong offset.
    fn validate(&self, expected_capacity: usize) -> Result<(), SlabError> {
        let hdr = self.header();
        if hdr.magic != SLAB_MAGIC
            || hdr.capacity != expected_capacity as u64
            || hdr.slot_size as usize != slab_slot_size::<T>()
        {
            return Err(SlabError::LayoutMismatch);
        }
        Ok(())
    }

    #[inline]
    fn slot_ptr(&self, i: usize) -> *const u8 {
        unsafe {
            self.mmap
                .as_ptr()
                .add(size_of::<SlabHeader>())
                .add(i * slab_slot_size::<T>())
        }
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

    /// Read the record at `i`.
    ///
    /// Spins while a writer holds the slot and rereads if the version
    /// moved under it, so the value returned was never observed
    /// half-written. A slot nothing has written reads as the zero bit
    /// pattern of `T`.
    pub fn get(&self, i: usize) -> Result<T, SlabError> {
        if i >= self.capacity {
            return Err(SlabError::OutOfBounds);
        }
        let version = self.version(i);
        let payload = unsafe { self.slot_ptr(i).add(SLAB_SLOT_PREFIX) };
        let out = loop {
            let v1 = version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue; // a writer holds this slot
            }
            let mut val = std::mem::MaybeUninit::<T>::uninit();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    payload,
                    val.as_mut_ptr() as *mut u8,
                    size_of::<T>(),
                );
            }
            if version.load(Ordering::Acquire) == v1 {
                break unsafe { val.assume_init() };
            }
            std::hint::spin_loop();
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_GET, 0);
        Ok(out)
    }

    /// Write the record at `i`.
    ///
    /// One writer per slot. Two writers on the same slot race: the
    /// SeqLock makes a torn READ detectable, and does not make a torn
    /// write safe.
    pub fn set(&self, i: usize, value: T) -> Result<(), SlabError> {
        if i >= self.capacity {
            return Err(SlabError::OutOfBounds);
        }
        if !self.mmap.is_writable() {
            return Err(SlabError::ReadOnly);
        }
        let version = self.version(i);
        let payload = unsafe { self.slot_ptr(i).add(SLAB_SLOT_PREFIX) as *mut u8 };
        version.fetch_add(1, Ordering::AcqRel); // odd: readers spin
        unsafe {
            std::ptr::copy_nonoverlapping(
                &value as *const T as *const u8,
                payload,
                size_of::<T>(),
            );
        }
        version.fetch_add(1, Ordering::AcqRel); // even: readers resume
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ordered::OP_INSERT, 0);
        Ok(())
    }

    /// How many times slot `i` has been written. Even at rest; odd while
    /// a writer holds it.
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

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedSlab<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader {
        &self.header_sidecar
    }
    fn ring(&self) -> &subetha_core::ObservationRing {
        &self.ring_sidecar
    }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
    struct Big {
        a: [u64; 20],
        b: u32,
        c: u8,
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("subetha_slab_{tag}_{}_{nonce}.bin", std::process::id()))
    }

    /// The record SharedVec cannot hold is the case this exists for.
    #[test]
    fn a_record_past_the_vec_payload_cap_round_trips() {
        assert!(
            size_of::<Big>() > crate::shared_vec::VEC_PAYLOAD_BYTES,
            "the fixture must exceed the cap this primitive exists to clear"
        );
        let p = tmp("big");
        let s: SharedSlab<Big> = SharedSlab::create(&p, 64).unwrap();

        let mut v = Big::default();
        v.a[0] = 0xDEAD_BEEF;
        v.a[19] = 0xFEED_FACE;
        v.b = 7;
        v.c = 9;
        s.set(5, v).unwrap();
        assert_eq!(s.get(5).unwrap(), v);

        // Untouched slots read as the zero pattern.
        assert_eq!(s.get(6).unwrap(), Big::default());
        drop(s);
        std::fs::remove_file(&p).ok();
    }

    /// A slot spans three cache lines at this size, and the stride keeps
    /// every version word on a line of its own.
    #[test]
    fn a_slot_is_whole_cache_lines_and_holds_the_record() {
        let stride = slab_slot_size::<Big>();
        assert_eq!(stride % 64, 0, "stride must be whole cache lines");
        assert!(stride >= SLAB_SLOT_PREFIX + size_of::<Big>());
        assert_eq!(stride, 192, "168-byte record plus an 8-byte prefix rounds to 192");
        assert_eq!(slab_slot_size::<u32>(), 64, "a small record still takes a line");
    }

    /// Two mappings of one file address the same records.
    #[test]
    fn a_second_handle_sees_the_first_handles_writes() {
        let p = tmp("share");
        let a: SharedSlab<Big> = SharedSlab::create(&p, 32).unwrap();
        let b: SharedSlab<Big> = SharedSlab::open(&p, 32).unwrap();

        let v = Big { b: 0x1234, ..Big::default() };
        a.set(9, v).unwrap();
        assert_eq!(b.get(9).unwrap(), v);
        assert_eq!(b.slot_version(9).unwrap(), 2, "one write leaves an even version");
        drop(a);
        drop(b);
        std::fs::remove_file(&p).ok();
    }

    /// A reader racing a writer on one slot never observes a mixture of
    /// two records. Every field of the fixture is derived from the same
    /// seed, so a torn read is detectable by the record disagreeing with
    /// itself rather than by comparing against an expected value.
    #[test]
    fn a_concurrent_reader_never_sees_a_half_written_record() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomOrd};
        use std::sync::Arc;

        let p = tmp("tear");
        let s: Arc<SharedSlab<Big>> = Arc::new(SharedSlab::create(&p, 8).unwrap());
        let stop = Arc::new(AtomicBool::new(false));

        let w = {
            let s = Arc::clone(&s);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut seed = 1u64;
                while !stop.load(AtomOrd::Acquire) {
                    let mut v = Big::default();
                    for (k, slot) in v.a.iter_mut().enumerate() {
                        *slot = seed.wrapping_mul(k as u64 + 1);
                    }
                    v.b = seed as u32;
                    v.c = (seed & 0xFF) as u8;
                    s.set(3, v).unwrap();
                    seed = seed.wrapping_add(1);
                }
            })
        };

        for _ in 0..50_000 {
            let v = s.get(3).unwrap();
            let seed = v.a[0];
            if seed == 0 {
                continue; // never written yet
            }
            for (k, slot) in v.a.iter().enumerate() {
                assert_eq!(
                    *slot,
                    seed.wrapping_mul(k as u64 + 1),
                    "record disagrees with itself: torn read at field {k}"
                );
            }
            assert_eq!(v.b, seed as u32, "torn read across the tail");
        }
        stop.store(true, AtomOrd::Release);
        w.join().unwrap();
        drop(s);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn an_index_past_capacity_is_refused() {
        let p = tmp("oob");
        let s: SharedSlab<Big> = SharedSlab::create(&p, 4).unwrap();
        assert_eq!(s.get(4), Err(SlabError::OutOfBounds));
        assert_eq!(s.set(4, Big::default()), Err(SlabError::OutOfBounds));
        drop(s);
        std::fs::remove_file(&p).ok();
    }

    /// A record of a different size derives a different stride, so the
    /// same file read as the wrong type is refused rather than returning
    /// records sliced at the wrong offset.
    #[test]
    fn a_different_record_size_is_a_layout_mismatch() {
        let p = tmp("layout");
        let s: SharedSlab<Big> = SharedSlab::create(&p, 8).unwrap();
        drop(s);
        assert_eq!(
            SharedSlab::<u64>::open(&p, 8).err(),
            Some(SlabError::LayoutMismatch),
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn a_read_only_mapping_refuses_writes() {
        let p = tmp("ro");
        let w: SharedSlab<Big> = SharedSlab::create(&p, 8).unwrap();
        let v = Big { b: 5, ..Big::default() };
        w.set(1, v).unwrap();

        let r: SharedSlab<Big> = SharedSlab::open_read_only(&p, 8).unwrap();
        assert_eq!(r.get(1).unwrap(), v);
        assert!(!r.is_writable());
        assert_eq!(r.set(1, Big::default()), Err(SlabError::ReadOnly));
        drop(w);
        drop(r);
        std::fs::remove_file(&p).ok();
    }
}
