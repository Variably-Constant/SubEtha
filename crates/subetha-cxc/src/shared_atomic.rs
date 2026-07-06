//! `SharedAtomic<T>` - cross-process atomic counter / flag.
//!
//! Backed by an MMF cell whose payload is interpreted directly as
//! `AtomicU8 / AtomicU16 / AtomicU32 / AtomicU64`. The atomic ops
//! are cross-process safe on every modern CPU because hardware
//! cache coherence guarantees the atomic semantics across address
//! spaces; the only requirement is that both processes map the
//! same physical page (which the OS guarantees when they open the
//! same MMF file).
//!
//! Three concrete types:
//! - `SharedAtomicU32`
//! - `SharedAtomicU64`
//! - `SharedAtomicBool` (one byte, but enforced bool semantics)
//!
//! Type-erased layout: header + payload region the size of the
//! native atomic, aligned naturally.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const ATOMIC_MAGIC: u32 = 0x4150_5443;

#[repr(C, align(64))]
struct AtomicHeader {
    magic: u32,
    width: u32,  // 1, 4, or 8 bytes
    payload_u64: AtomicU64,  // also covers u32, u8 via punning
}

const ATOMIC_FILE_SIZE: usize = std::mem::size_of::<AtomicHeader>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedAtomicError {
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for SharedAtomicError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

macro_rules! shared_atomic_impl {
    ($name:ident, $atomic:ty, $native:ty, $width:expr) => {
        pub struct $name {
            _file: File,
            mmap: MmapMut,
            header_sidecar: subetha_core::HandshakeHeader,
            ring_sidecar: Box<subetha_core::ObservationRing>,
        }

        unsafe impl Send for $name {}
        unsafe impl Sync for $name {}

        impl subetha_sidecar::AdaptiveInstance for $name {
            fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
            fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
            fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
                Box::new(subetha_sidecar::NoMigrationPolicy)
            }
        }

        impl $name {
            pub fn create(path: impl AsRef<Path>, init: $native) -> Result<Self, SharedAtomicError> {
                let file = OpenOptions::new()
                    .read(true).write(true).create(true).truncate(true)
                    .open(path.as_ref())?;
                file.set_len(ATOMIC_FILE_SIZE as u64)?;
                let mut mmap = unsafe { MmapOptions::new().len(ATOMIC_FILE_SIZE).map_mut(&file)? };
                let hdr = mmap.as_mut_ptr() as *mut AtomicHeader;
                unsafe {
                    std::ptr::write(hdr, AtomicHeader {
                        magic: ATOMIC_MAGIC,
                        width: $width,
                        payload_u64: AtomicU64::new(0),
                    });
                }
                let s = Self {
                    _file: file, mmap,
                    header_sidecar: subetha_core::HandshakeHeader::new(),
                    ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
                };
                s.atomic().store(init, Ordering::Release);
                Ok(s)
            }

            pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedAtomicError> {
                let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
                if file.metadata()?.len() < ATOMIC_FILE_SIZE as u64 {
                    return Err(SharedAtomicError::LayoutMismatch);
                }
                let mmap = unsafe { MmapOptions::new().len(ATOMIC_FILE_SIZE).map_mut(&file)? };
                let hdr = unsafe { &*(mmap.as_ptr() as *const AtomicHeader) };
                if hdr.magic != ATOMIC_MAGIC || hdr.width != $width {
                    return Err(SharedAtomicError::LayoutMismatch);
                }
                Ok(Self {
                    _file: file, mmap,
                    header_sidecar: subetha_core::HandshakeHeader::new(),
                    ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
                })
            }

            #[inline]
            fn atomic(&self) -> &$atomic {
                let base = unsafe {
                    self.mmap.as_ptr()
                        .add(std::mem::offset_of!(AtomicHeader, payload_u64))
                };
                unsafe { &*(base as *const $atomic) }
            }

            #[inline]
            pub fn load(&self, ord: Ordering) -> $native {
                let v = self.atomic().load(ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_LOAD, 0);
                v
            }

            #[inline]
            pub fn store(&self, v: $native, ord: Ordering) {
                self.atomic().store(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_STORE, 0);
            }

            #[inline]
            pub fn fetch_add(&self, v: $native, ord: Ordering) -> $native {
                let prev = self.atomic().fetch_add(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_FETCH_ADD, 0);
                prev
            }

            #[inline]
            pub fn fetch_sub(&self, v: $native, ord: Ordering) -> $native {
                let prev = self.atomic().fetch_sub(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_FETCH_ADD, 0);
                prev
            }

            #[inline]
            pub fn fetch_or(&self, v: $native, ord: Ordering) -> $native {
                let prev = self.atomic().fetch_or(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_FETCH_ADD, 0);
                prev
            }

            #[inline]
            pub fn fetch_and(&self, v: $native, ord: Ordering) -> $native {
                let prev = self.atomic().fetch_and(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_FETCH_ADD, 0);
                prev
            }

            #[inline]
            pub fn fetch_xor(&self, v: $native, ord: Ordering) -> $native {
                let prev = self.atomic().fetch_xor(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_FETCH_ADD, 0);
                prev
            }

            #[inline]
            pub fn swap(&self, v: $native, ord: Ordering) -> $native {
                let prev = self.atomic().swap(v, ord);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_CAS, 0);
                prev
            }

            #[inline]
            pub fn compare_exchange(
                &self, current: $native, new: $native,
                success: Ordering, failure: Ordering,
            ) -> Result<$native, $native> {
                let r = self.atomic().compare_exchange(current, new, success, failure);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::atomic::OP_CAS, if r.is_err() { 1 } else { 0 });
                r
            }

            pub fn flush(&self) -> Result<(), SharedAtomicError> {
                self.mmap.flush()?;
                Ok(())
            }

            /// Non-blocking flush: schedules a writeback via the OS
            /// (msync(MS_ASYNC) on Linux; FlushViewOfFile without
            /// FlushFileBuffers on Windows). Note: Windows is only
            /// partially async (sync to page cache, not to disk).
            pub fn flush_async(&self) -> Result<(), SharedAtomicError> {
                self.mmap.flush_async()?;
                Ok(())
            }
        }
    };
}

shared_atomic_impl!(SharedAtomicU32, AtomicU32, u32, 4);
shared_atomic_impl!(SharedAtomicU64, AtomicU64, u64, 8);

pub struct SharedAtomicBool {
    _file: File,
    mmap: MmapMut,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedAtomicBool {}
unsafe impl Sync for SharedAtomicBool {}

impl subetha_sidecar::AdaptiveInstance for SharedAtomicBool {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedAtomicBool {
    pub fn create(path: impl AsRef<Path>, init: bool) -> Result<Self, SharedAtomicError> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(ATOMIC_FILE_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(ATOMIC_FILE_SIZE).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut AtomicHeader;
        unsafe {
            std::ptr::write(hdr, AtomicHeader {
                magic: ATOMIC_MAGIC,
                width: 1,
                payload_u64: AtomicU64::new(0),
            });
        }
        let s = Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        s.atomic().store(init, Ordering::Release);
        Ok(s)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedAtomicError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < ATOMIC_FILE_SIZE as u64 {
            return Err(SharedAtomicError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(ATOMIC_FILE_SIZE).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const AtomicHeader) };
        if hdr.magic != ATOMIC_MAGIC || hdr.width != 1 {
            return Err(SharedAtomicError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn atomic(&self) -> &AtomicBool {
        let base = unsafe {
            self.mmap.as_ptr().add(std::mem::offset_of!(AtomicHeader, payload_u64))
        };
        unsafe { &*(base as *const AtomicBool) }
    }

    pub fn load(&self, ord: Ordering) -> bool {
        let v = self.atomic().load(ord);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::atomic::OP_LOAD, 0);
        v
    }
    pub fn store(&self, v: bool, ord: Ordering) {
        self.atomic().store(v, ord);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::atomic::OP_STORE, 0);
    }
    pub fn swap(&self, v: bool, ord: Ordering) -> bool {
        let prev = self.atomic().swap(v, ord);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::atomic::OP_CAS, 0);
        prev
    }

    pub fn flush(&self) -> Result<(), SharedAtomicError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), SharedAtomicError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-atomic-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn u64_load_store_round_trip() {
        let p = tmp("u64-rt");
        let a = SharedAtomicU64::create(&p, 42).unwrap();
        assert_eq!(a.load(Ordering::Acquire), 42);
        a.store(99, Ordering::Release);
        assert_eq!(a.load(Ordering::Acquire), 99);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn u32_fetch_add_increments() {
        let p = tmp("u32-add");
        let a = SharedAtomicU32::create(&p, 0).unwrap();
        for _ in 0..100 { a.fetch_add(1, Ordering::AcqRel); }
        assert_eq!(a.load(Ordering::Acquire), 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer = SharedAtomicU64::create(&p, 0).unwrap();
        let reader = SharedAtomicU64::open(&p).unwrap();
        writer.store(7777, Ordering::Release);
        assert_eq!(reader.load(Ordering::Acquire), 7777);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_fetch_add_sums_correctly() {
        use std::sync::Arc;
        use std::thread;
        let p = tmp("concurrent");
        let a = Arc::new(SharedAtomicU64::create(&p, 0).unwrap());
        let mut handles = vec![];
        for _ in 0..8 {
            let a = a.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 { a.fetch_add(1, Ordering::AcqRel); }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(a.load(Ordering::Acquire), 8000);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compare_exchange_wins_once() {
        let p = tmp("cas");
        let a = SharedAtomicU64::create(&p, 5).unwrap();
        let r1 = a.compare_exchange(5, 10, Ordering::AcqRel, Ordering::Acquire);
        let r2 = a.compare_exchange(5, 20, Ordering::AcqRel, Ordering::Acquire);
        assert_eq!(r1, Ok(5));
        assert_eq!(r2, Err(10));
        assert_eq!(a.load(Ordering::Acquire), 10);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn bool_load_store_swap() {
        let p = tmp("bool");
        let b = SharedAtomicBool::create(&p, false).unwrap();
        assert!(!b.load(Ordering::Acquire));
        b.store(true, Ordering::Release);
        assert!(b.load(Ordering::Acquire));
        let prev = b.swap(false, Ordering::AcqRel);
        assert!(prev);
        assert!(!b.load(Ordering::Acquire));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let a = SharedAtomicU64::create(&p, 12345).unwrap();
            a.flush().unwrap();
        }
        let a2 = SharedAtomicU64::open(&p).unwrap();
        assert_eq!(a2.load(Ordering::Acquire), 12345);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_rejects_wrong_width() {
        let p = tmp("wrong-width");
        let _a = SharedAtomicU64::create(&p, 0).unwrap();
        match SharedAtomicU32::open(&p) {
            Err(SharedAtomicError::LayoutMismatch) => {}
            other => panic!("expected LayoutMismatch, got {:?}", other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }
}
