//! `SharedOnceCell<T>` - cross-process init-once cell.
//!
//! State machine:
//! - EMPTY (0): no value; first writer to CAS to INITIALIZING wins.
//! - INITIALIZING (1): a writer is filling the payload; other
//!   writers spin until state advances.
//! - INITIALIZED (2): payload is stable; readers may consume.
//!
//! The winner of the EMPTY -> INITIALIZING CAS performs the write
//! and advances to INITIALIZED. Losers see INITIALIZED and read the
//! winner's bytes.
//!
//! This is the cross-process analog of `once_cell::sync::OnceCell`,
//! with cross-process safety guaranteed by the atomic CAS protocol
//! over shared memory.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const ONCE_MAGIC: u32 = 0x4F4E_4346;
pub const ONCE_PAYLOAD_BYTES: usize = 56;

pub const STATE_EMPTY: u8 = 0;
pub const STATE_INITIALIZING: u8 = 1;
pub const STATE_INITIALIZED: u8 = 2;

#[repr(C, align(64))]
pub struct OnceHeader {
    pub magic: u32,
    pub size: u32,
    pub state: AtomicU8,
    pub _pad_to_payload: [u8; 7],
    pub payload: [u8; ONCE_PAYLOAD_BYTES],
}

pub const ONCE_FILE_SIZE: usize = size_of::<OnceHeader>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedOnceError {
    LayoutMismatch,
    PayloadTooLarge,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for SharedOnceError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedOnceCell<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedOnceCell<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedOnceCell<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedOnceCell<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedOnceCell<T> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, SharedOnceError> {
        Self::check_layout()?;
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(ONCE_FILE_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(ONCE_FILE_SIZE).map_mut(&file)? };
        let ptr = mmap.as_mut_ptr() as *mut OnceHeader;
        unsafe {
            std::ptr::write(ptr, OnceHeader {
                magic: ONCE_MAGIC,
                size: size_of::<T>() as u32,
                state: AtomicU8::new(STATE_EMPTY),
                _pad_to_payload: [0; 7],
                payload: [0; ONCE_PAYLOAD_BYTES],
            });
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedOnceError> {
        Self::check_layout()?;
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < ONCE_FILE_SIZE as u64 {
            return Err(SharedOnceError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(ONCE_FILE_SIZE).map_mut(&file)? };
        let header = unsafe { &*(mmap.as_ptr() as *const OnceHeader) };
        if header.magic != ONCE_MAGIC || header.size as usize != size_of::<T>() {
            return Err(SharedOnceError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn check_layout() -> Result<(), SharedOnceError> {
        if size_of::<T>() > ONCE_PAYLOAD_BYTES {
            return Err(SharedOnceError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(SharedOnceError::PayloadTooLarge);
        }
        Ok(())
    }

    fn header(&self) -> &OnceHeader {
        unsafe { &*(self.mmap.as_ptr() as *const OnceHeader) }
    }

    /// True when the cell has been initialised.
    pub fn is_initialized(&self) -> bool {
        self.header().state.load(Ordering::Acquire) == STATE_INITIALIZED
    }

    /// Get the value if initialised; otherwise return None.
    /// Non-blocking; never invokes the initialiser.
    pub fn get(&self) -> Option<T> {
        let header = self.header();
        if header.state.load(Ordering::Acquire) != STATE_INITIALIZED {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::cell::OP_GET, 2); // empty / uninitialised
            return None;
        }
        let value: T = unsafe {
            let src = header.payload.as_ptr() as *const T;
            std::ptr::read_unaligned(src)
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::cell::OP_GET, 0);
        Some(value)
    }

    /// Try to write the value. Returns `true` if this caller won
    /// the init race, `false` if the cell was already initialised
    /// or another init is in progress.
    pub fn set(&self, value: T) -> bool {
        let header = self.header();
        if header.state.compare_exchange(
            STATE_EMPTY, STATE_INITIALIZING,
            Ordering::AcqRel, Ordering::Acquire,
        ).is_err() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::cell::OP_SET, 1); // lost the init race
            return false;
        }
        unsafe {
            let dst = header.payload.as_ptr() as *mut T;
            std::ptr::write_unaligned(dst, value);
        }
        header.state.store(STATE_INITIALIZED, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::cell::OP_SET, 0);
        true
    }

    /// Get the cached value, or run `init` to produce it. The first
    /// caller across all processes runs `init`; subsequent callers
    /// spin until the value is published and return that value.
    pub fn get_or_init<F: FnOnce() -> T>(&self, init: F) -> T {
        if let Some(v) = self.get() { return v; }
        let header = self.header();
        match header.state.compare_exchange(
            STATE_EMPTY, STATE_INITIALIZING,
            Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => {
                // We won; produce the value and publish.
                let v = init();
                unsafe {
                    let dst = header.payload.as_ptr() as *mut T;
                    std::ptr::write_unaligned(dst, v);
                }
                header.state.store(STATE_INITIALIZED, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::cell::OP_SET, 0);
                v
            }
            Err(_) => {
                // Spin until the winner publishes.
                while header.state.load(Ordering::Acquire) != STATE_INITIALIZED {
                    std::hint::spin_loop();
                }
                self.get().expect("INITIALIZED implies value present")
            }
        }
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), SharedOnceError> {
        self.mmap.flush_async()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), SharedOnceError> {
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
        p.push(format!("subetha-once-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn fresh_cell_is_empty() {
        let p = tmp("fresh");
        let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert!(!c.is_initialized());
        assert_eq!(c.get(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn first_set_wins_subsequent_sets_lose() {
        let p = tmp("first-wins");
        let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert!(c.set(42));
        assert!(!c.set(99));
        assert_eq!(c.get(), Some(42));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_init_visible_to_other_handle() {
        let p = tmp("cross-handle");
        let writer: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        let reader: SharedOnceCell<u64> = SharedOnceCell::open(&p).unwrap();
        assert!(!reader.is_initialized());
        writer.set(7777);
        assert!(reader.is_initialized());
        assert_eq!(reader.get(), Some(7777));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn get_or_init_runs_closure_at_most_once() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;
        use std::thread;
        let p = tmp("get-or-init");
        let c: Arc<SharedOnceCell<u64>> = Arc::new(SharedOnceCell::create(&p).unwrap());
        let runs = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];
        for _ in 0..8 {
            let c = c.clone();
            let runs = runs.clone();
            handles.push(thread::spawn(move || {
                c.get_or_init(|| {
                    runs.fetch_add(1, Ordering::AcqRel);
                    1234u64
                })
            }));
        }
        let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(results.iter().all(|v| *v == 1234));
        assert_eq!(runs.load(Ordering::Acquire), 1,
                   "init closure must run exactly once across 8 threads");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
            c.set(8888);
            c.flush().unwrap();
        }
        let c2: SharedOnceCell<u64> = SharedOnceCell::open(&p).unwrap();
        assert_eq!(c2.get(), Some(8888));
        assert!(c2.is_initialized());
        // Set must fail on reopen because the cell is already init.
        assert!(!c2.set(9999));
        assert_eq!(c2.get(), Some(8888));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // size_of<Big> is the test signal, not the field
        struct Big([u8; ONCE_PAYLOAD_BYTES + 1]);
        impl Copy for Big {}
        impl Clone for Big { fn clone(&self) -> Self { *self } }
        let p = tmp("too-large");
        match SharedOnceCell::<Big>::create(&p) {
            Err(SharedOnceError::PayloadTooLarge) => {}
            other => panic!("expected PayloadTooLarge, got {:?}", other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_rejects_wrong_payload_size() {
        let p = tmp("wrong-size");
        let _c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        match SharedOnceCell::<u32>::open(&p) {
            Err(SharedOnceError::LayoutMismatch) => {}
            other => panic!("expected LayoutMismatch, got {:?}", other.as_ref().err()),
        }
        std::fs::remove_file(&p).ok();
    }
}
