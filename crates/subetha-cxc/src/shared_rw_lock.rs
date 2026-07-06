//! `SharedRWLock` - cross-process reader-writer lock with writer
//! priority.
//!
//! Multiple concurrent readers OR exactly one writer. When a writer
//! is waiting, new readers block to prevent writer starvation.
//!
//! # State encoding
//!
//! ONE AtomicU64 packed:
//! - bit 63: writer active (1 if a writer holds the lock)
//! - bits 32-62: writers waiting count (31 bits)
//! - bits 0-31: reader count (32 bits)
//!
//! All transitions are single CAS so observers never see torn state.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const RWLOCK_MAGIC: u64 = 0x4150_5257_4C4F_434B;

const WRITER_BIT: u64 = 1u64 << 63;
const WAITING_SHIFT: u64 = 32;
const WAITING_MASK: u64 = 0x7FFF_FFFF << WAITING_SHIFT;
const READERS_MASK: u64 = 0xFFFF_FFFF;

#[repr(C, align(64))]
pub struct RWLockHeader {
    pub magic: u64,
    pub state: AtomicU64,
    _pad: [u8; 48],
}

const _: () = {
    assert!(size_of::<RWLockHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RWLockError {
    WouldBlock,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for RWLockError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedRWLock {
    _file: File,
    mmap: MmapMut,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedRWLock {}
unsafe impl Sync for SharedRWLock {}

impl subetha_sidecar::AdaptiveInstance for SharedRWLock {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedRWLock {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, RWLockError> {
        let total = size_of::<RWLockHeader>();
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut RWLockHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, total);
            (*hdr).magic = RWLOCK_MAGIC;
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, RWLockError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < size_of::<RWLockHeader>() as u64 {
            return Err(RWLockError::LayoutMismatch);
        }
        let mmap = unsafe {
            MmapOptions::new().len(size_of::<RWLockHeader>()).map_mut(&file)?
        };
        let hdr = unsafe { &*(mmap.as_ptr() as *const RWLockHeader) };
        if hdr.magic != RWLOCK_MAGIC {
            return Err(RWLockError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn state(&self) -> &AtomicU64 {
        unsafe { &(*(self.mmap.as_ptr() as *const RWLockHeader)).state }
    }

    /// Try to acquire a read lock without blocking.
    pub fn try_read_lock(&self) -> Result<ReadGuard<'_>, RWLockError> {
        let r = self.try_read_lock_inner();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::rw_lock::OP_TRY_READ,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn try_read_lock_inner(&self) -> Result<ReadGuard<'_>, RWLockError> {
        loop {
            let s = self.state().load(Ordering::Acquire);
            let writer_active = (s & WRITER_BIT) != 0;
            let writers_waiting = (s & WAITING_MASK) >> WAITING_SHIFT;
            if writer_active || writers_waiting > 0 {
                return Err(RWLockError::WouldBlock);
            }
            let readers = s & READERS_MASK;
            let new = (s & !READERS_MASK) | (readers + 1);
            if self.state().compare_exchange(
                s, new, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Ok(ReadGuard { lock: self });
            }
        }
    }

    /// Acquire a read lock, blocking with backoff until available.
    /// Writer-priority: blocks if any writer is active OR waiting.
    pub fn read_lock(&self) -> ReadGuard<'_> {
        let mut spins = 0u32;
        loop {
            if let Ok(g) = self.try_read_lock_inner() {
                self.ring_sidecar.push_op(
                    crate::sidecar_ops::rw_lock::OP_READ,
                    if spins > 0 { 1 } else { 0 }, // contention
                );
                return g;
            }
            spins += 1;
            if spins < 32 {
                std::hint::spin_loop();
            } else if spins < 256 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        }
    }

    /// Try to acquire a write lock without blocking.
    pub fn try_write_lock(&self) -> Result<WriteGuard<'_>, RWLockError> {
        let r = self.try_write_lock_inner();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::rw_lock::OP_TRY_WRITE,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn try_write_lock_inner(&self) -> Result<WriteGuard<'_>, RWLockError> {
        loop {
            let s = self.state().load(Ordering::Acquire);
            let writer_active = (s & WRITER_BIT) != 0;
            let readers = s & READERS_MASK;
            if writer_active || readers > 0 {
                return Err(RWLockError::WouldBlock);
            }
            let new = (s & !WRITER_BIT) | WRITER_BIT;
            if self.state().compare_exchange(
                s, new, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Ok(WriteGuard { lock: self });
            }
        }
    }

    /// Acquire a write lock, blocking until available. Registers
    /// as "waiting" so new readers will block.
    pub fn write_lock(&self) -> WriteGuard<'_> {
        // Register as waiting.
        self.state().fetch_add(1u64 << WAITING_SHIFT, Ordering::AcqRel);
        let mut spins = 0u32;
        loop {
            let s = self.state().load(Ordering::Acquire);
            let writer_active = (s & WRITER_BIT) != 0;
            let readers = s & READERS_MASK;
            if !writer_active && readers == 0 {
                // Try to claim: set writer bit + decrement waiting.
                let new = (s & READERS_MASK) | WRITER_BIT
                    | ((((s & WAITING_MASK) >> WAITING_SHIFT) - 1) << WAITING_SHIFT);
                if self.state().compare_exchange(
                    s, new, Ordering::AcqRel, Ordering::Acquire,
                ).is_ok() {
                    self.ring_sidecar.push_op(
                        crate::sidecar_ops::rw_lock::OP_WRITE,
                        if spins > 0 { 1 } else { 0 }, // contention
                    );
                    return WriteGuard { lock: self };
                }
            }
            spins += 1;
            if spins < 32 {
                std::hint::spin_loop();
            } else if spins < 256 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        }
    }

    /// Number of active readers (observational; may race).
    pub fn reader_count(&self) -> u32 {
        (self.state().load(Ordering::Acquire) & READERS_MASK) as u32
    }

    /// True if a writer currently holds the lock.
    pub fn has_writer(&self) -> bool {
        (self.state().load(Ordering::Acquire) & WRITER_BIT) != 0
    }

    /// Number of writers currently waiting for the lock.
    pub fn waiting_writers(&self) -> u32 {
        ((self.state().load(Ordering::Acquire) & WAITING_MASK) >> WAITING_SHIFT) as u32
    }

    /// Release one reader. Internal; called by ReadGuard::drop.
    /// Defensive: checks that the reader count is positive before
    /// decrementing. In debug builds this panics on protocol
    /// violation (release without acquire); in release builds it
    /// silently no-ops to avoid underflow corruption.
    fn release_read(&self) {
        loop {
            let s = self.state().load(Ordering::Acquire);
            let readers = s & READERS_MASK;
            debug_assert!(
                readers > 0,
                "SharedRWLock::release_read called when reader count is 0 - \
                 indicates a protocol violation (double-release or release \
                 without acquire). The lock counter will not be decremented.",
            );
            if readers == 0 { return; }
            let new = (s & !READERS_MASK) | (readers - 1);
            if self.state().compare_exchange(
                s, new, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return;
            }
        }
    }

    /// Release the writer. Internal; called by WriteGuard::drop.
    /// Defensive: checks that a writer is actually active before
    /// clearing. In debug builds this panics on protocol violation.
    fn release_write(&self) {
        let prev = self.state().fetch_and(!WRITER_BIT, Ordering::AcqRel);
        debug_assert!(
            (prev & WRITER_BIT) != 0,
            "SharedRWLock::release_write called when no writer holds the lock - \
             indicates a protocol violation (double-release or release without \
             acquire).",
        );
    }

    /// Public hook for the `BlockingRWLock` wrapper to mirror the
    /// inner `ReadGuard::drop` semantics after the wrapper's own
    /// guard runs (the wrapper `mem::forget`s the inner guard so it
    /// can interleave a wake call between the state release and the
    /// guard's destructor).
    pub fn release_read_for_blocking(&self) { self.release_read(); }

    /// Public hook for the `BlockingRWLock` wrapper; mirror of
    /// `WriteGuard::drop`.
    pub fn release_write_for_blocking(&self) { self.release_write(); }

    pub fn flush(&self) -> Result<(), RWLockError> {
        self.mmap.flush()?;
        Ok(())
    }
    pub fn flush_async(&self) -> Result<(), RWLockError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

pub struct ReadGuard<'a> { lock: &'a SharedRWLock }
impl Drop for ReadGuard<'_> {
    fn drop(&mut self) { self.lock.release_read(); }
}

pub struct WriteGuard<'a> { lock: &'a SharedRWLock }
impl Drop for WriteGuard<'_> {
    fn drop(&mut self) { self.lock.release_write(); }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as O};
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-rwlock-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_idle() {
        let p = tmp("init");
        let l = SharedRWLock::create(&p).unwrap();
        assert_eq!(l.reader_count(), 0);
        assert!(!l.has_writer());
        assert_eq!(l.waiting_writers(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn try_read_succeeds_when_idle() {
        let p = tmp("try-read");
        let l = SharedRWLock::create(&p).unwrap();
        let g = l.try_read_lock().unwrap();
        assert_eq!(l.reader_count(), 1);
        drop(g);
        assert_eq!(l.reader_count(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn try_write_succeeds_when_idle() {
        let p = tmp("try-write");
        let l = SharedRWLock::create(&p).unwrap();
        let g = l.try_write_lock().unwrap();
        assert!(l.has_writer());
        drop(g);
        assert!(!l.has_writer());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn multiple_readers_coexist() {
        let p = tmp("multi-read");
        let l = SharedRWLock::create(&p).unwrap();
        let g1 = l.try_read_lock().unwrap();
        let g2 = l.try_read_lock().unwrap();
        let g3 = l.try_read_lock().unwrap();
        assert_eq!(l.reader_count(), 3);
        drop(g1); drop(g2); drop(g3);
        assert_eq!(l.reader_count(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn writer_excludes_readers() {
        let p = tmp("w-excl-r");
        let l = SharedRWLock::create(&p).unwrap();
        let _w = l.try_write_lock().unwrap();
        assert_eq!(l.try_read_lock().err(), Some(RWLockError::WouldBlock));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn reader_excludes_writer() {
        let p = tmp("r-excl-w");
        let l = SharedRWLock::create(&p).unwrap();
        let _r = l.try_read_lock().unwrap();
        assert_eq!(l.try_write_lock().err(), Some(RWLockError::WouldBlock));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn writer_excludes_writer() {
        let p = tmp("w-excl-w");
        let l = SharedRWLock::create(&p).unwrap();
        let _w = l.try_write_lock().unwrap();
        assert_eq!(l.try_write_lock().err(), Some(RWLockError::WouldBlock));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn write_lock_blocks_until_readers_drop() {
        // Clean pattern: spawn a reader thread that holds its guard
        // for a known duration. Main thread spawns a writer that
        // must block until the reader's guard drops. No unsafe
        // ptr::read; the reader's guard lifetime is tied to its
        // thread's scope.
        let p = tmp("w-blocks");
        let l = Arc::new(SharedRWLock::create(&p).unwrap());
        let l_reader = l.clone();
        let reader_done = Arc::new(AtomicU32::new(0));
        let reader_done_clone = reader_done.clone();
        let reader = thread::spawn(move || {
            let _g = l_reader.read_lock();
            std::thread::sleep(std::time::Duration::from_millis(30));
            reader_done_clone.store(1, O::Release);
            // Guard drops here, releasing the lock.
        });
        // Wait (bounded) for the reader thread to acquire; a fixed
        // sleep races the scheduler under full-suite load.
        let acquire_deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(5);
        while l.reader_count() != 1
            && std::time::Instant::now() < acquire_deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(l.reader_count(), 1);

        let l_writer = l.clone();
        let writer_started = std::time::Instant::now();
        let writer = thread::spawn(move || {
            let _g = l_writer.write_lock();
            writer_started.elapsed()
        });

        let elapsed = writer.join().unwrap();
        reader.join().unwrap();
        // Writer should have blocked at least until reader finished
        // (which is ~30ms - 5ms from when writer was spawned = ~25ms).
        assert!(
            elapsed >= std::time::Duration::from_millis(15),
            "writer should have blocked for the reader's hold time, got {elapsed:?}",
        );
        assert_eq!(reader_done.load(O::Acquire), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn writer_priority_blocks_new_readers() {
        // When a writer is waiting, new try_read should fail
        // (writer priority).
        let p = tmp("w-priority");
        let l = SharedRWLock::create(&p).unwrap();
        // Simulate a waiting writer by bumping the waiting count
        // directly (real writers do this in write_lock).
        l.state().fetch_add(1u64 << WAITING_SHIFT, Ordering::AcqRel);
        assert_eq!(l.try_read_lock().err(), Some(RWLockError::WouldBlock));
        // Clean up the bumped count for the file teardown.
        l.state().fetch_sub(1u64 << WAITING_SHIFT, Ordering::AcqRel);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_readers_all_succeed() {
        let p = tmp("concurrent-r");
        let l = Arc::new(SharedRWLock::create(&p).unwrap());
        let n = 8;
        let count = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];
        for _ in 0..n {
            let l = l.clone();
            let count = count.clone();
            handles.push(thread::spawn(move || {
                let _g = l.read_lock();
                count.fetch_add(1, O::AcqRel);
                std::thread::sleep(std::time::Duration::from_millis(5));
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(count.load(O::Acquire), n);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let w = SharedRWLock::create(&p).unwrap();
        let r = SharedRWLock::open(&p).unwrap();
        let _g = w.try_read_lock().unwrap();
        // Reader handle sees the same state.
        assert_eq!(r.reader_count(), 1);
        assert_eq!(r.try_write_lock().err(), Some(RWLockError::WouldBlock));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn writer_then_reader_serialized() {
        let p = tmp("w-then-r");
        let l = SharedRWLock::create(&p).unwrap();
        {
            let _w = l.try_write_lock().unwrap();
        }
        // After writer drops, reader can acquire.
        let _r = l.try_read_lock().unwrap();
        std::fs::remove_file(&p).ok();
    }
}
