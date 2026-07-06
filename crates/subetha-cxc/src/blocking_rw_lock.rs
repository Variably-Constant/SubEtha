//! `BlockingRWLock`: cross-process reader-writer lock with a
//! kernel-park slow path via [`CrossProcessWaker`].
//!
//! Composes [`crate::shared_rw_lock::SharedRWLock`]
//! with one `CrossProcessWaker` plus a small mmap-backed
//! wakeup-generation counter. The hot path delegates to
//! `try_read_lock` / `try_write_lock` (single CAS each). The
//! contention slow path differs from the existing primitive:
//!
//! - **`SharedRWLock::read_lock` / `write_lock`** spin → yield →
//!   `sleep(50us)` indefinitely until the lock becomes available.
//!   The sleep tail burns CPU on each wake-up tick AND can miss
//!   an unlock by up to 50us.
//! - **`BlockingRWLock::read_park` / `write_park`** register in
//!   the waker at the current generation, then park in the kernel.
//!   The kernel returns within microseconds of the next unlock.
//!
//! Both readers and writers park on the SAME waker; an unlock
//! fires `wake_all` so the contending side picks up the lock
//! according to the underlying writer-priority policy.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

use crate::cross_process_waker::{
    CrossProcessWaker, MAX_WAITERS_DEFAULT, WakerError,
};
use crate::shared_rw_lock::{RWLockError, SharedRWLock};

const WAKEUP_MAGIC: u64 = 0x4257_524C_5742_4B30; // "BWRLWBK0"
const WAKEUP_REGION_SIZE: usize = 64;
const WAKEUP_OFFSET: usize = 8;

/// Errors returned by [`BlockingRWLock`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingRWLockError {
    Lock(RWLockError),
    Waker(WakerError),
    Timeout,
    LayoutMismatch,
    Io(std::io::ErrorKind),
}

impl From<RWLockError> for BlockingRWLockError {
    fn from(e: RWLockError) -> Self { Self::Lock(e) }
}
impl From<WakerError> for BlockingRWLockError {
    fn from(e: WakerError) -> Self {
        match e {
            WakerError::Timeout => Self::Timeout,
            other => Self::Waker(other),
        }
    }
}
impl From<std::io::Error> for BlockingRWLockError {
    fn from(e: std::io::Error) -> Self { Self::Io(e.kind()) }
}

#[allow(dead_code)]
enum WakeupBacking {
    Anon(MmapMut),
    File(std::fs::File, MmapMut),
}

struct WakeupAtom {
    #[allow(dead_code)]
    backing: WakeupBacking,
    ptr: *const AtomicU64,
}

// SAFETY: AtomicU64 ptr points into the mmap held by `backing`,
// valid for the lifetime of WakeupAtom.
unsafe impl Send for WakeupAtom {}
unsafe impl Sync for WakeupAtom {}

impl WakeupAtom {
    fn create_file(path: &Path) -> Result<Self, BlockingRWLockError> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(WAKEUP_REGION_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(WAKEUP_REGION_SIZE).map_mut(&file)? };
        let base = mmap.as_mut_ptr();
        unsafe {
            (base as *mut u64).write(WAKEUP_MAGIC);
            (base.add(WAKEUP_OFFSET) as *mut AtomicU64).write(AtomicU64::new(0));
        }
        let ptr = unsafe { base.add(WAKEUP_OFFSET) as *const AtomicU64 };
        Ok(Self { backing: WakeupBacking::File(file, mmap), ptr })
    }

    fn open_file(path: &Path) -> Result<Self, BlockingRWLockError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        if (file.metadata()?.len() as usize) < WAKEUP_REGION_SIZE {
            return Err(BlockingRWLockError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(WAKEUP_REGION_SIZE).map_mut(&file)? };
        let base = mmap.as_mut_ptr();
        let magic = unsafe { (base as *const u64).read() };
        if magic != WAKEUP_MAGIC {
            return Err(BlockingRWLockError::LayoutMismatch);
        }
        let ptr = unsafe { base.add(WAKEUP_OFFSET) as *const AtomicU64 };
        Ok(Self { backing: WakeupBacking::File(file, mmap), ptr })
    }

    #[inline]
    fn atom(&self) -> &AtomicU64 { unsafe { &*self.ptr } }
}

/// Cross-process reader-writer lock with kernel-park slow path.
pub struct BlockingRWLock {
    inner: Arc<SharedRWLock>,
    waker: Arc<CrossProcessWaker>,
    wakeup: Arc<WakeupAtom>,
}

const PRE_PARK_SPIN: u32 = 32;

impl BlockingRWLock {
    /// Create a blocking rwlock. Lays out three files under one
    /// base path: `<base>.rwlock.bin` for the state atom,
    /// `<base>.waker.bin` for the waker slots, `<base>.wakeup.bin`
    /// for the magic + generation atom.
    pub fn create(base_path: impl AsRef<Path>) -> Result<Self, BlockingRWLockError> {
        let base = base_path.as_ref();
        let inner = SharedRWLock::create(rwlock_path(base))?;
        let waker = CrossProcessWaker::create(waker_path(base), MAX_WAITERS_DEFAULT)?;
        let wakeup = WakeupAtom::create_file(&wakeup_path(base))?;
        Ok(Self {
            inner: Arc::new(inner),
            waker: Arc::new(waker),
            wakeup: Arc::new(wakeup),
        })
    }

    /// Open an existing blocking rwlock.
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self, BlockingRWLockError> {
        let base = base_path.as_ref();
        let inner = SharedRWLock::open(rwlock_path(base))?;
        let waker = CrossProcessWaker::open(waker_path(base), MAX_WAITERS_DEFAULT)?;
        let wakeup = WakeupAtom::open_file(&wakeup_path(base))?;
        Ok(Self {
            inner: Arc::new(inner),
            waker: Arc::new(waker),
            wakeup: Arc::new(wakeup),
        })
    }

    /// Non-blocking read-lock attempt.
    pub fn try_read_lock(&self) -> Result<BlockingReadGuard<'_>, BlockingRWLockError> {
        match self.inner.try_read_lock() {
            Ok(g) => {
                std::mem::forget(g);
                Ok(BlockingReadGuard { lock: self })
            }
            Err(e) => Err(BlockingRWLockError::Lock(e)),
        }
    }

    /// Non-blocking write-lock attempt.
    pub fn try_write_lock(&self) -> Result<BlockingWriteGuard<'_>, BlockingRWLockError> {
        match self.inner.try_write_lock() {
            Ok(g) => {
                std::mem::forget(g);
                Ok(BlockingWriteGuard { lock: self })
            }
            Err(e) => Err(BlockingRWLockError::Lock(e)),
        }
    }

    /// Read-lock with kernel-park slow path.
    pub fn read_park(&self) -> Result<BlockingReadGuard<'_>, BlockingRWLockError> {
        loop {
            if let Ok(g) = self.inner.try_read_lock() {
                std::mem::forget(g);
                return Ok(BlockingReadGuard { lock: self });
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(g) = self.inner.try_read_lock() {
                    std::mem::forget(g);
                    return Ok(BlockingReadGuard { lock: self });
                }
                std::hint::spin_loop();
            }
            let snapshot = self.wakeup.atom().load(Ordering::Acquire);
            let token = self.waker.try_park(snapshot + 1)?;
            if let Ok(g) = self.inner.try_read_lock() {
                self.waker.release(token);
                std::mem::forget(g);
                return Ok(BlockingReadGuard { lock: self });
            }
            self.waker.wait(token, None)?;
        }
    }

    /// Read-lock with bounded wait.
    pub fn read_park_timeout(
        &self,
        timeout: Duration,
    ) -> Result<BlockingReadGuard<'_>, BlockingRWLockError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(g) = self.inner.try_read_lock() {
                std::mem::forget(g);
                return Ok(BlockingReadGuard { lock: self });
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(g) = self.inner.try_read_lock() {
                    std::mem::forget(g);
                    return Ok(BlockingReadGuard { lock: self });
                }
                std::hint::spin_loop();
            }
            let snapshot = self.wakeup.atom().load(Ordering::Acquire);
            let token = self.waker.try_park(snapshot + 1)?;
            if let Ok(g) = self.inner.try_read_lock() {
                self.waker.release(token);
                std::mem::forget(g);
                return Ok(BlockingReadGuard { lock: self });
            }
            let now = Instant::now();
            if now >= deadline {
                self.waker.release(token);
                return Err(BlockingRWLockError::Timeout);
            }
            let remaining = deadline - now;
            match self.waker.wait(token, Some(remaining)) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingRWLockError::Timeout),
                Err(e) => return Err(BlockingRWLockError::Waker(e)),
            }
        }
    }

    /// Write-lock with kernel-park slow path.
    pub fn write_park(&self) -> Result<BlockingWriteGuard<'_>, BlockingRWLockError> {
        loop {
            if let Ok(g) = self.inner.try_write_lock() {
                std::mem::forget(g);
                return Ok(BlockingWriteGuard { lock: self });
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(g) = self.inner.try_write_lock() {
                    std::mem::forget(g);
                    return Ok(BlockingWriteGuard { lock: self });
                }
                std::hint::spin_loop();
            }
            let snapshot = self.wakeup.atom().load(Ordering::Acquire);
            let token = self.waker.try_park(snapshot + 1)?;
            if let Ok(g) = self.inner.try_write_lock() {
                self.waker.release(token);
                std::mem::forget(g);
                return Ok(BlockingWriteGuard { lock: self });
            }
            self.waker.wait(token, None)?;
        }
    }

    /// Write-lock with bounded wait.
    pub fn write_park_timeout(
        &self,
        timeout: Duration,
    ) -> Result<BlockingWriteGuard<'_>, BlockingRWLockError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(g) = self.inner.try_write_lock() {
                std::mem::forget(g);
                return Ok(BlockingWriteGuard { lock: self });
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(g) = self.inner.try_write_lock() {
                    std::mem::forget(g);
                    return Ok(BlockingWriteGuard { lock: self });
                }
                std::hint::spin_loop();
            }
            let snapshot = self.wakeup.atom().load(Ordering::Acquire);
            let token = self.waker.try_park(snapshot + 1)?;
            if let Ok(g) = self.inner.try_write_lock() {
                self.waker.release(token);
                std::mem::forget(g);
                return Ok(BlockingWriteGuard { lock: self });
            }
            let now = Instant::now();
            if now >= deadline {
                self.waker.release(token);
                return Err(BlockingRWLockError::Timeout);
            }
            let remaining = deadline - now;
            match self.waker.wait(token, Some(remaining)) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingRWLockError::Timeout),
                Err(e) => return Err(BlockingRWLockError::Waker(e)),
            }
        }
    }

    /// Bump wakeup-generation + wake every parker. Called from the
    /// guard Drop paths after the inner lock is released.
    fn signal_unlock(&self) {
        let new_gen = self.wakeup.atom().fetch_add(1, Ordering::Release) + 1;
        self.waker.wake_up_to(new_gen);
    }

    /// Inner primitive (sidecar / observability access).
    pub fn inner(&self) -> &Arc<SharedRWLock> { &self.inner }
}

/// RAII guard for a held read-lock. Drops to release.
pub struct BlockingReadGuard<'a> {
    lock: &'a BlockingRWLock,
}

impl Drop for BlockingReadGuard<'_> {
    fn drop(&mut self) {
        // Manually release the inner read-count. Inner SharedRWLock
        // does this via its own ReadGuard::drop; we mem::forget'd
        // that guard so we need to do the equivalent state update.
        release_read_state(&self.lock.inner);
        self.lock.signal_unlock();
    }
}

/// RAII guard for a held write-lock. Drops to release.
pub struct BlockingWriteGuard<'a> {
    lock: &'a BlockingRWLock,
}

impl Drop for BlockingWriteGuard<'_> {
    fn drop(&mut self) {
        release_write_state(&self.lock.inner);
        self.lock.signal_unlock();
    }
}

/// Mirrors `ReadGuard::drop`. Called by `BlockingReadGuard::drop`
/// after the wrapper's guard captures the unlock event so we can
/// fire a wake right after the state-release.
fn release_read_state(lock: &SharedRWLock) {
    lock.release_read_for_blocking();
}

fn release_write_state(lock: &SharedRWLock) {
    lock.release_write_for_blocking();
}

fn rwlock_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".rwlock.bin");
    PathBuf::from(p)
}
fn waker_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".waker.bin");
    PathBuf::from(p)
}
fn wakeup_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".wakeup.bin");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::thread;

    fn fresh_base() -> PathBuf {
        let dir = std::env::temp_dir();
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        dir.join(format!("subetha_brwlock_test_{}_{}", std::process::id(), n))
    }

    fn cleanup(base: &Path) {
        for suffix in [".rwlock.bin", ".waker.bin", ".wakeup.bin"] {
            let mut p = base.as_os_str().to_owned();
            p.push(suffix);
            drop(std::fs::remove_file(PathBuf::from(p)));
        }
    }

    #[test]
    fn try_read_then_drop_then_try_write_succeeds() {
        let base = fresh_base();
        cleanup(&base);
        let lock = BlockingRWLock::create(&base).expect("create");
        let r = lock.try_read_lock().expect("read");
        drop(r);
        let w = lock.try_write_lock().expect("write");
        drop(w);
        cleanup(&base);
    }

    #[test]
    fn write_park_completes_after_reader_releases() {
        let base = fresh_base();
        cleanup(&base);
        let lock = Arc::new(BlockingRWLock::create(&base).expect("create"));
        let r = lock.try_read_lock().expect("read");

        // Assert the ORDERING property directly: the parked writer
        // cannot complete before the reader's release. (A fixed
        // sleep + minimum-elapsed assertion is schedule-sensitive:
        // under full-suite load the spawned thread can start late
        // and measure a short block despite behaving correctly.)
        let lock2 = Arc::clone(&lock);
        let t = thread::spawn(move || {
            let _w = lock2.write_park().expect("park-write");
            Instant::now()
        });
        thread::sleep(Duration::from_millis(40));
        let released_at = Instant::now();
        drop(r);
        let completed_at = t.join().unwrap();
        assert!(completed_at >= released_at,
                "write_park must not complete before the reader released");
        cleanup(&base);
    }

    #[test]
    fn read_park_timeout_returns_timeout_when_writer_held() {
        let base = fresh_base();
        cleanup(&base);
        let lock = BlockingRWLock::create(&base).expect("create");
        let _w = lock.try_write_lock().expect("hold-write");
        let t0 = Instant::now();
        let err = lock.read_park_timeout(Duration::from_millis(60));
        assert!(matches!(err, Err(BlockingRWLockError::Timeout)));
        assert!(t0.elapsed() >= Duration::from_millis(50));
        cleanup(&base);
    }
}
