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
            /// Obtain the atomic at `path`, initializing it to `init` if
            /// the path does not yet exist and attaching to it if it
            /// does. Attaching leaves the live value in place; `init` is
            /// then unused. A region built for a different width is a
            /// `LayoutMismatch`. [`reset`](Self::reset) reinitializes.
            pub fn create(path: impl AsRef<Path>, init: $native) -> Result<Self, SharedAtomicError> {
                let (file, mmap) = crate::mmf_attach::create_or_attach(
                    path.as_ref(),
                    ATOMIC_FILE_SIZE,
                    |ptr| unsafe { Self::init_region(ptr, init) },
                    |ptr| unsafe { (*(ptr as *const AtomicHeader)).magic == ATOMIC_MAGIC },
                )?;
                Self::from_region(file, mmap)
            }

            /// Truncate the atomic at `path` and initialize it to
            /// `init`, discarding the value a live peer holds. For a
            /// caller that knows it owns the path.
            pub fn reset(path: impl AsRef<Path>, init: $native) -> Result<Self, SharedAtomicError> {
                let (file, mmap) = crate::mmf_attach::reset(
                    path.as_ref(),
                    ATOMIC_FILE_SIZE,
                    |ptr| unsafe { Self::init_region(ptr, init) },
                )?;
                Self::from_region(file, mmap)
            }

            /// Lay out a fresh atomic: width and value first, magic
            /// last, because attachers spin on it.
            ///
            /// # Safety
            /// `ptr` addresses at least `ATOMIC_FILE_SIZE` writable
            /// zeroed bytes.
            unsafe fn init_region(ptr: *mut u8, init: $native) {
                let hdr = ptr as *mut AtomicHeader;
                unsafe {
                    (*hdr).width = $width;
                    let payload = (&raw mut (*hdr).payload_u64) as *mut $atomic;
                    std::ptr::write(payload, <$atomic>::new(init));
                    std::ptr::write_volatile(&raw mut (*hdr).magic, ATOMIC_MAGIC);
                }
            }

            /// Wrap an initialized region, refusing one built for a
            /// different width.
            fn from_region(file: File, mmap: MmapMut) -> Result<Self, SharedAtomicError> {
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

            pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedAtomicError> {
                let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
                if file.metadata()?.len() < ATOMIC_FILE_SIZE as u64 {
                    return Err(SharedAtomicError::LayoutMismatch);
                }
                let mmap = unsafe { MmapOptions::new().len(ATOMIC_FILE_SIZE).map_mut(&file)? };
                Self::from_region(file, mmap)
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
    /// Obtain the flag at `path`, initializing it to `init` if the path
    /// does not yet exist and attaching to it if it does. Attaching
    /// leaves the live value in place, so a racing peer never resets a
    /// flag another process already set.
    /// [`reset`](Self::reset) reinitializes.
    pub fn create(path: impl AsRef<Path>, init: bool) -> Result<Self, SharedAtomicError> {
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            ATOMIC_FILE_SIZE,
            |ptr| unsafe { Self::init_region(ptr, init) },
            |ptr| unsafe { (*(ptr as *const AtomicHeader)).magic == ATOMIC_MAGIC },
        )?;
        Self::from_region(file, mmap)
    }

    /// Truncate the flag at `path` and initialize it to `init`,
    /// discarding the value live peers share. For a caller that knows
    /// it owns the path.
    pub fn reset(path: impl AsRef<Path>, init: bool) -> Result<Self, SharedAtomicError> {
        let (file, mmap) = crate::mmf_attach::reset(
            path.as_ref(),
            ATOMIC_FILE_SIZE,
            |ptr| unsafe { Self::init_region(ptr, init) },
        )?;
        Self::from_region(file, mmap)
    }

    /// Lay out the flag: width and payload first, magic last, because
    /// attachers spin on it.
    ///
    /// # Safety
    /// `ptr` addresses at least `ATOMIC_FILE_SIZE` writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, init: bool) {
        let hdr = ptr as *mut AtomicHeader;
        unsafe {
            (*hdr).width = 1;
            std::ptr::write(&raw mut (*hdr).payload_u64, AtomicU64::new(u64::from(init)));
            std::ptr::write_volatile(&raw mut (*hdr).magic, ATOMIC_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one laid out for a
    /// different width.
    fn from_region(file: File, mmap: MmapMut) -> Result<Self, SharedAtomicError> {
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

    /// A second create attaches with the live value in place, ignoring
    /// its init argument; reset is what re-seeds.
    #[test]
    fn second_create_attaches_and_keeps_the_value() {
        let p = tmp("attach");
        std::fs::remove_file(&p).ok();
        let a = SharedAtomicU64::create(&p, 42).unwrap();
        a.store(777, Ordering::Release);

        let b = SharedAtomicU64::create(&p, 0).unwrap();
        assert_eq!(b.load(Ordering::Acquire), 777, "attach clobbered the value");

        // Windows refuses to truncate a mapped file, so every handle goes
        // before the reset.
        drop(a);
        drop(b);
        let fresh = SharedAtomicU64::reset(&p, 5).unwrap();
        assert_eq!(fresh.load(Ordering::Acquire), 5, "reset did not re-seed");
        drop(fresh);
        std::fs::remove_file(&p).ok();
    }

    /// Attaching with a different width is refused.
    #[test]
    fn create_refuses_a_mismatched_width() {
        let p = tmp("mismatch");
        std::fs::remove_file(&p).ok();
        let a = SharedAtomicU64::create(&p, 1).unwrap();
        assert!(matches!(
            SharedAtomicU32::create(&p, 1),
            Err(SharedAtomicError::LayoutMismatch),
        ));
        drop(a);
        std::fs::remove_file(&p).ok();
    }

    /// A second create attaches to the live flag rather than resetting
    /// it to its init value; reset is what discards it. The other widths
    /// obtain the same way, so the bool must not be the odd one out.
    #[test]
    fn bool_second_create_attaches_and_keeps_the_value() {
        let p = tmp("bool-attach");
        std::fs::remove_file(&p).ok();

        let first = SharedAtomicBool::create(&p, false).unwrap();
        first.store(true, Ordering::Release);

        let second = SharedAtomicBool::create(&p, false).unwrap();
        assert!(
            second.load(Ordering::Acquire),
            "attach reset a flag another handle had already set",
        );

        // Windows refuses to truncate a mapped file, so every handle goes
        // before the reset.
        drop(first);
        drop(second);
        let fresh = SharedAtomicBool::reset(&p, false).unwrap();
        assert!(!fresh.load(Ordering::Acquire), "reset kept the old value");
        drop(fresh);
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
