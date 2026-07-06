//! `BlockingSemaphore`: cross-process counting semaphore with a
//! kernel-park slow path via [`CrossProcessWaker`].
//!
//! Composes [`crate::shared_semaphore::SharedSemaphore`]
//! (the counter + generation-counter primitive) with one
//! `CrossProcessWaker`. The hot path is unchanged from
//! `SharedSemaphore::try_acquire`: a single CAS on the permit
//! count. The contention slow path differs:
//!
//! - **`SharedSemaphore::acquire`** loops `try_acquire` → `yield_now`
//!   → `sleep(50us)` indefinitely. The sleep tail burns CPU on
//!   the wake-up tick AND can miss a release by up to 50us.
//! - **`BlockingSemaphore::acquire_park`** loops `try_acquire`,
//!   then registers in the waker at the current generation, then
//!   parks via the platform wait syscall. The kernel returns
//!   within microseconds of the next `release`.
//!
//! Cross-process Linux uses SHARED `futex` so a `release` from
//! process A wakes a parker in process B. Windows runs intra-
//! process via `WaitOnAddress` (one process at a time, share via
//! `Arc::clone`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cross_process_waker::{
    CrossProcessWaker, MAX_WAITERS_DEFAULT, WakerError,
};
use crate::shared_semaphore::{SemaphoreError, SharedSemaphore};

/// Errors returned by [`BlockingSemaphore`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingSemaphoreError {
    Semaphore(SemaphoreError),
    Waker(WakerError),
    Timeout,
}

impl From<SemaphoreError> for BlockingSemaphoreError {
    fn from(e: SemaphoreError) -> Self { Self::Semaphore(e) }
}
impl From<WakerError> for BlockingSemaphoreError {
    fn from(e: WakerError) -> Self {
        match e {
            WakerError::Timeout => Self::Timeout,
            other => Self::Waker(other),
        }
    }
}

/// Cross-process semaphore with a kernel-park slow path.
pub struct BlockingSemaphore {
    inner: Arc<SharedSemaphore>,
    waker: Arc<CrossProcessWaker>,
}

const PRE_PARK_SPIN: u32 = 32;

impl BlockingSemaphore {
    /// Create a new blocking semaphore. Lays out the underlying
    /// `SharedSemaphore` files plus a `<base>.waker.bin` for the
    /// waker. Caller picks `max_permits` (capacity) and
    /// `init_permits` (starting value); see
    /// [`SharedSemaphore::create`] for semantics.
    pub fn create(
        base_path: impl AsRef<Path>,
        max_permits: u32,
        init_permits: u32,
    ) -> Result<Self, BlockingSemaphoreError> {
        let base = base_path.as_ref();
        let inner = SharedSemaphore::create(base, init_permits, max_permits)?;
        let waker = CrossProcessWaker::create(waker_path(base), MAX_WAITERS_DEFAULT)?;
        Ok(Self {
            inner: Arc::new(inner),
            waker: Arc::new(waker),
        })
    }

    /// Open an existing blocking semaphore.
    pub fn open(
        base_path: impl AsRef<Path>,
        expected_max_permits: u32,
    ) -> Result<Self, BlockingSemaphoreError> {
        let base = base_path.as_ref();
        let inner = SharedSemaphore::open(base, expected_max_permits)?;
        let waker = CrossProcessWaker::open(waker_path(base), MAX_WAITERS_DEFAULT)?;
        Ok(Self {
            inner: Arc::new(inner),
            waker: Arc::new(waker),
        })
    }

    /// Non-blocking acquire. Pure CAS; never sleeps.
    pub fn try_acquire(&self) -> Result<BlockingPermit<'_>, BlockingSemaphoreError> {
        match self.inner.try_acquire() {
            Ok(p) => {
                // The inner Permit's drop would call `release` on the
                // inner sema; we forget it and re-arm our own Permit
                // that calls the wrapper's release (which also fires
                // a wake).
                std::mem::forget(p);
                Ok(BlockingPermit { sem: self })
            }
            Err(SemaphoreError::WouldBlock) => Err(BlockingSemaphoreError::Semaphore(SemaphoreError::WouldBlock)),
            Err(e) => Err(BlockingSemaphoreError::Semaphore(e)),
        }
    }

    /// Blocking acquire with kernel-park slow path. Returns when a
    /// permit is available. No timeout variant returns
    /// `Err(Timeout)`; for a bounded wait use `acquire_park_timeout`.
    pub fn acquire_park(&self) -> Result<BlockingPermit<'_>, BlockingSemaphoreError> {
        loop {
            if let Ok(p) = self.inner.try_acquire() {
                std::mem::forget(p);
                return Ok(BlockingPermit { sem: self });
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(p) = self.inner.try_acquire() {
                    std::mem::forget(p);
                    return Ok(BlockingPermit { sem: self });
                }
                std::hint::spin_loop();
            }
            // Slow path: mark as waiter so the inner release path
            // bumps the wakeup generation; snapshot; double-check;
            // park.
            self.inner.mark_waiter_entered();
            let snapshot = self.inner.wakeup_generation();
            let token = match self.waker.try_park(snapshot + 1) {
                Ok(t) => t,
                Err(e) => {
                    self.inner.mark_waiter_left();
                    return Err(BlockingSemaphoreError::from(e));
                }
            };
            if let Ok(p) = self.inner.try_acquire() {
                self.waker.release(token);
                self.inner.mark_waiter_left();
                std::mem::forget(p);
                return Ok(BlockingPermit { sem: self });
            }
            let wait_res = self.waker.wait(token, None);
            self.inner.mark_waiter_left();
            wait_res?;
        }
    }

    /// Blocking acquire with bounded wait. `Err(Timeout)` when the
    /// timeout elapses before a permit is available.
    pub fn acquire_park_timeout(
        &self,
        timeout: Duration,
    ) -> Result<BlockingPermit<'_>, BlockingSemaphoreError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(p) = self.inner.try_acquire() {
                std::mem::forget(p);
                return Ok(BlockingPermit { sem: self });
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(p) = self.inner.try_acquire() {
                    std::mem::forget(p);
                    return Ok(BlockingPermit { sem: self });
                }
                std::hint::spin_loop();
            }
            self.inner.mark_waiter_entered();
            let snapshot = self.inner.wakeup_generation();
            let token = match self.waker.try_park(snapshot + 1) {
                Ok(t) => t,
                Err(e) => {
                    self.inner.mark_waiter_left();
                    return Err(BlockingSemaphoreError::from(e));
                }
            };
            if let Ok(p) = self.inner.try_acquire() {
                self.waker.release(token);
                self.inner.mark_waiter_left();
                std::mem::forget(p);
                return Ok(BlockingPermit { sem: self });
            }
            let now = Instant::now();
            if now >= deadline {
                self.waker.release(token);
                self.inner.mark_waiter_left();
                return Err(BlockingSemaphoreError::Timeout);
            }
            let remaining = deadline - now;
            let wait_res = self.waker.wait(token, Some(remaining));
            self.inner.mark_waiter_left();
            match wait_res {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingSemaphoreError::Timeout),
                Err(e) => return Err(BlockingSemaphoreError::Waker(e)),
            }
        }
    }

    /// Release one permit. Bumps the inner generation atom and
    /// fires `wake_up_to(new_gen)` on the waker.
    pub fn release(&self) -> Result<(), BlockingSemaphoreError> {
        self.inner.release()?;
        // SharedSemaphore::release internally fetched-add'd wakeup
        // when waiters > 0. The wake on our side is unconditional
        // (cheap if no slots parked).
        let new_gen = self.inner.wakeup_generation();
        self.waker.wake_up_to(new_gen);
        Ok(())
    }

    /// Available-permits observation (may race).
    pub fn available(&self) -> u32 { self.inner.available() }

    /// Cap fixed at construction.
    pub fn max_permits(&self) -> u32 { self.inner.max_permits() }

    /// Inner primitive (for sidecar / observability hooks).
    pub fn inner(&self) -> &Arc<SharedSemaphore> { &self.inner }
}

/// RAII guard for an acquired permit. Drops to `release`.
pub struct BlockingPermit<'a> {
    sem: &'a BlockingSemaphore,
}

impl Drop for BlockingPermit<'_> {
    fn drop(&mut self) {
        // Release-overflow + waker errors are unrecoverable from
        // inside Drop; surface them on stderr and continue so the
        // RAII chain still runs.
        if let Err(e) = self.sem.release() {
            eprintln!("BlockingSemaphore: release failed in Drop: {e:?}");
        }
    }
}

fn waker_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".waker.bin");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    fn fresh_base() -> PathBuf {
        let dir = std::env::temp_dir();
        // Use both pid and a per-test counter so parallel tests in
        // the same process don't clobber each other.
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        dir.join(format!("subetha_bsem_test_{}_{}", std::process::id(), n))
    }

    fn cleanup(base: &Path) {
        for suffix in [
            ".count.bin", ".wakeup.bin", ".waiters.bin",
            ".count.bin.hh.bin", ".count.bin.ring.bin",
            ".wakeup.bin.hh.bin", ".wakeup.bin.ring.bin",
            ".waiters.bin.hh.bin", ".waiters.bin.ring.bin",
            ".hh.bin", ".ring.bin",
            ".waker.bin",
        ] {
            let mut p = base.as_os_str().to_owned();
            p.push(suffix);
            drop(std::fs::remove_file(PathBuf::from(p)));
        }
    }

    #[test]
    fn try_acquire_succeeds_when_permits_available() {
        let base = fresh_base();
        cleanup(&base);
        let sem = BlockingSemaphore::create(&base, 4, 4).expect("create");
        let p = sem.try_acquire().expect("permit");
        drop(p);
        cleanup(&base);
    }

    #[test]
    fn acquire_park_blocks_then_completes_on_release() {
        let base = fresh_base();
        cleanup(&base);
        let sem = Arc::new(BlockingSemaphore::create(&base, 1, 1).expect("create"));
        let p0 = sem.try_acquire().expect("permit-0");

        // Assert the ORDERING property directly: the parked
        // acquirer cannot complete before the permit's release. (A
        // fixed sleep + minimum-elapsed assertion is schedule-
        // sensitive: under full-suite load the spawned thread can
        // start late and measure a short block despite behaving
        // correctly.)
        let s2 = Arc::clone(&sem);
        let t = thread::spawn(move || {
            let _g = s2.acquire_park().expect("park-acquire");
            Instant::now()
        });
        thread::sleep(Duration::from_millis(40));
        let released_at = Instant::now();
        drop(p0); // release fires wake_up_to.
        let completed_at = t.join().unwrap();
        assert!(completed_at >= released_at,
                "acquire_park must not complete before the permit released");

        cleanup(&base);
    }

    #[test]
    fn acquire_park_timeout_returns_timeout() {
        let base = fresh_base();
        cleanup(&base);
        let sem = BlockingSemaphore::create(&base, 1, 1).expect("create");
        let _hold = sem.try_acquire().expect("hold");
        let t0 = Instant::now();
        let err = sem.acquire_park_timeout(Duration::from_millis(60));
        assert!(matches!(err, Err(BlockingSemaphoreError::Timeout)));
        assert!(t0.elapsed() >= Duration::from_millis(50));
        cleanup(&base);
    }
}
