//! `SharedCell<T>` - cross-process single-value cell using the
//! SeqLock protocol over a memory-mapped file.
//!
//! `T: Copy` plus a stable `#[repr(C)]` layout is the contract;
//! readers in different processes will memcpy the same bytes and
//! interpret them identically. The SeqLock retry loop guarantees
//! that a reader never observes a torn write across the writer's
//! payload-update window.
//!
//! # SeqLock protocol
//!
//! Layout (one cache line):
//! ```text
//! +---------+---------+---------+--------------------------+
//! | magic   | size    | version | payload [u8; PAYLOAD]    |
//! +---------+---------+---------+--------------------------+
//!   u32       u32       u32       up to 52 bytes
//! ```
//!
//! Writer protocol:
//! 1. Bump `version` from V (even) to V+1 (odd). All concurrent
//!    readers now see an odd version and will retry.
//! 2. Memcpy the new payload bytes into place.
//! 3. Bump `version` from V+1 to V+2 (even). Readers resume.
//!
//! Reader protocol:
//! 1. Load `version` (Acquire). If odd, spin and retry.
//! 2. Memcpy the payload into a local buffer.
//! 3. Load `version` again (Acquire). If it changed, retry.
//! 4. Return the buffered payload.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const CELL_MAGIC: u32 = 0x4350_4D46;
pub const PAYLOAD_BYTES: usize = 52;

#[repr(C, align(64))]
pub struct CellHeader {
    pub magic: u32,
    pub size: u32,
    pub version: AtomicU32,
    pub _pad_to_payload: u32,
    pub payload: [u8; PAYLOAD_BYTES],
}

pub const CELL_FILE_SIZE: usize = size_of::<CellHeader>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedCellError {
    LayoutMismatch,
    PayloadTooLarge,
    NotInitialised,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for SharedCellError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedCell<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedCell<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedCell<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedCell<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedCell<T> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, SharedCellError> {
        Self::check_layout()?;
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(CELL_FILE_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(CELL_FILE_SIZE).map_mut(&file)? };
        let ptr = mmap.as_mut_ptr() as *mut CellHeader;
        unsafe {
            std::ptr::write(ptr, CellHeader {
                magic: CELL_MAGIC,
                size: size_of::<T>() as u32,
                version: AtomicU32::new(0),
                _pad_to_payload: 0,
                payload: [0; PAYLOAD_BYTES],
            });
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedCellError> {
        Self::check_layout()?;
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < CELL_FILE_SIZE as u64 {
            return Err(SharedCellError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(CELL_FILE_SIZE).map_mut(&file)? };
        let header = unsafe { &*(mmap.as_ptr() as *const CellHeader) };
        if header.magic != CELL_MAGIC || header.size as usize != size_of::<T>() {
            return Err(SharedCellError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn check_layout() -> Result<(), SharedCellError> {
        if size_of::<T>() > PAYLOAD_BYTES {
            return Err(SharedCellError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(SharedCellError::PayloadTooLarge);
        }
        Ok(())
    }

    fn header(&self) -> &CellHeader {
        unsafe { &*(self.mmap.as_ptr() as *const CellHeader) }
    }

    /// Atomically replace the cell value. SeqLock protocol: odd
    /// version during the memcpy, even after.
    pub fn set(&self, value: T) {
        let header = self.header();
        // Bump to odd (writer in progress).
        let v_old = header.version.fetch_add(1, Ordering::AcqRel);
        debug_assert!(v_old & 1 == 0, "concurrent writers not supported on SharedCell");
        // SAFETY: payload bytes are exclusive to this writer for
        // the odd-version window; readers spin until even.
        unsafe {
            let dst = header.payload.as_ptr() as *mut T;
            std::ptr::write_unaligned(dst, value);
        }
        // Release fence + bump to even (writer done).
        header.version.fetch_add(1, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::cell::OP_SET, 0);
    }

    /// Read the current value via the SeqLock retry loop. Always
    /// returns; the loop is bounded by writer frequency.
    pub fn get(&self) -> T {
        let header = self.header();
        let mut retries: u32 = 0;
        loop {
            let v1 = header.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                retries = retries.saturating_add(1);
                std::hint::spin_loop();
                continue;
            }
            // SAFETY: payload may change under us; the v1 == v2
            // check below verifies consistency.
            let value: T = unsafe {
                let src = header.payload.as_ptr() as *const T;
                std::ptr::read_unaligned(src)
            };
            let v2 = header.version.load(Ordering::Acquire);
            if v1 == v2 {
                self.ring_sidecar.push_op(
                    crate::sidecar_ops::cell::OP_GET,
                    if retries > 0 { 1 } else { 0 },
                );
                return value;
            }
            // Writer concurrent with our read; retry.
            retries = retries.saturating_add(1);
            std::hint::spin_loop();
        }
    }

    pub fn version(&self) -> u32 {
        self.header().version.load(Ordering::Acquire)
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), SharedCellError> {
        self.mmap.flush_async()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), SharedCellError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-cell-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn round_trip_simple_payload() {
        let p = tmp("round-trip");
        let c: SharedCell<u64> = SharedCell::create(&p).unwrap();
        c.set(42);
        assert_eq!(c.get(), 42);
        c.set(99);
        assert_eq!(c.get(), 99);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedCell<u64> = SharedCell::create(&p).unwrap();
        let reader: SharedCell<u64> = SharedCell::open(&p).unwrap();
        writer.set(0xDEAD_BEEF);
        assert_eq!(reader.get(), 0xDEAD_BEEF);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn version_advances_on_each_set() {
        let p = tmp("version");
        let c: SharedCell<u32> = SharedCell::create(&p).unwrap();
        let v0 = c.version();
        c.set(1);
        let v1 = c.version();
        c.set(2);
        let v2 = c.version();
        // Each set advances by 2 (odd-then-even).
        assert_eq!(v1, v0 + 2);
        assert_eq!(v2, v0 + 4);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let c: SharedCell<u64> = SharedCell::create(&p).unwrap();
            c.set(7777);
            c.flush().unwrap();
        }
        let c2: SharedCell<u64> = SharedCell::open(&p).unwrap();
        assert_eq!(c2.get(), 7777);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_rejects_wrong_payload_size() {
        let p = tmp("wrong-size");
        let _c: SharedCell<u64> = SharedCell::create(&p).unwrap();
        match SharedCell::<u32>::open(&p) {
            Err(SharedCellError::LayoutMismatch) => {}
            other => panic!("expected LayoutMismatch, got {:?}", other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Point { x: f64, y: f64, z: f64 }
        let p = tmp("struct");
        let c: SharedCell<Point> = SharedCell::create(&p).unwrap();
        let pt = Point { x: 1.0, y: 2.0, z: 3.0 };
        c.set(pt);
        assert_eq!(c.get(), pt);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_readers_during_writes() {
        use std::sync::Arc;
        use std::thread;
        let p = tmp("concurrent-rw");
        let c: Arc<SharedCell<u64>> = Arc::new(SharedCell::create(&p).unwrap());
        c.set(0);
        let writer_c = c.clone();
        let writer = thread::spawn(move || {
            for i in 1..1000u64 {
                writer_c.set(i);
            }
            999u64
        });
        let mut handles = vec![];
        for _ in 0..4 {
            let reader_c = c.clone();
            handles.push(thread::spawn(move || {
                let mut last = 0u64;
                for _ in 0..1000 {
                    let v = reader_c.get();
                    // Values must be monotonic (writer never goes backwards).
                    assert!(v >= last, "torn read detected: v={v} last={last}");
                    last = v;
                }
            }));
        }
        let final_w = writer.join().unwrap();
        for h in handles { h.join().unwrap(); }
        assert!(c.get() >= final_w);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // size_of<Big> is the test signal, not the field
        struct Big([u8; PAYLOAD_BYTES + 1]);
        impl Copy for Big {}
        impl Clone for Big { fn clone(&self) -> Self { *self } }
        let p = tmp("too-large");
        match SharedCell::<Big>::create(&p) {
            Err(SharedCellError::PayloadTooLarge) => {}
            other => panic!("expected PayloadTooLarge, got {:?}", other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }
}
