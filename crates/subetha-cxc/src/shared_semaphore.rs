//! `SharedSemaphore` - cross-process counting semaphore.
//!
//! Three MMF files compose the primitive:
//! - `<base>.count.bin`   - SharedAtomicU32: available permits.
//! - `<base>.wakeup.bin`  - SharedAtomicU64: monotonic generation
//!   bumped on every `release` to wake waiters.
//! - `<base>.waiters.bin` - SharedAtomicU32: count of currently-
//!   waiting acquirers. Releasers consult it to skip the wakeup
//!   bump when there are no waiters (saves an atomic store on the
//!   uncontended path).
//!
//! # Protocol
//!
//! `acquire`:
//! 1. Load `count`. If > 0, try CAS to decrement; on success, return.
//! 2. On 0 (or CAS lost), increment `waiters`, snapshot `wakeup`,
//!    re-check `count`, then yield/sleep until either `count > 0` OR
//!    `wakeup` advances. Loop back to 1.
//!
//! `release`:
//! 1. `count.fetch_add(1, AcqRel)`.
//! 2. If `waiters.load(Acquire) > 0`, `wakeup.fetch_add(1, Release)`
//!    to wake at least one waiter.
//!
//! `try_acquire`: a single CAS pass; never spins, never sleeps.
//!
//! # Why no real wait queue ring?
//!
//! Linux futex semantics are "wake N waiters"; an acquirer just
//! needs to know "something changed." A generation counter gives
//! that exactly. Adding a ring of waiter PIDs only helps if you need
//! strict FIFO fairness, which most cross-process resource limiters
//! do NOT. The generation-counter design is simpler, has zero
//! allocation, and matches the semantics of every modern OS
//! semaphore primitive (which all coalesce identical wakeups
//! internally).
//!
//! # Permit RAII
//!
//! `acquire` / `try_acquire` return a [`Permit`] guard tied to the
//! semaphore. Dropping the permit releases the count. For cross-
//! thread / cross-process ownership (e.g., handing a permit to a
//! background task), use the standalone `release` API:
//! `mem::forget(permit)` then `sem.release()` from the new owner.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::shared_atomic::{SharedAtomicError, SharedAtomicU32, SharedAtomicU64};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemaphoreError {
    Atomic(SharedAtomicError),
    WouldBlock,
    Timeout,
    ReleaseOverflow,
}

impl From<SharedAtomicError> for SemaphoreError {
    fn from(e: SharedAtomicError) -> Self { Self::Atomic(e) }
}

fn count_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.count.bin"));
    p
}
fn wakeup_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.wakeup.bin"));
    p
}
fn waiters_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.waiters.bin"));
    p
}

pub struct SharedSemaphore {
    count: Arc<SharedAtomicU32>,
    wakeup: Arc<SharedAtomicU64>,
    waiters: Arc<SharedAtomicU32>,
    max_permits: u32,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl subetha_sidecar::AdaptiveInstance for SharedSemaphore {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedSemaphore {
    /// Create a new semaphore with `initial` available permits and
    /// an upper bound `max_permits` (release fails if it pushes
    /// `count` above this).
    pub fn create(
        base_path: impl AsRef<Path>,
        initial: u32,
        max_permits: u32,
    ) -> Result<Self, SemaphoreError> {
        assert!(initial <= max_permits, "initial permits must be <= max_permits");
        let base = base_path.as_ref();
        let count = Arc::new(SharedAtomicU32::create(count_path(base), initial)?);
        let wakeup = Arc::new(SharedAtomicU64::create(wakeup_path(base), 0)?);
        let waiters = Arc::new(SharedAtomicU32::create(waiters_path(base), 0)?);
        Ok(Self {
            count, wakeup, waiters, max_permits,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing semaphore. Must pass the same `max_permits`
    /// the creator used; this is enforced only at release time, so
    /// open is cheap (no header magic check beyond what the
    /// underlying atomic provides).
    pub fn open(
        base_path: impl AsRef<Path>,
        max_permits: u32,
    ) -> Result<Self, SemaphoreError> {
        let base = base_path.as_ref();
        let count = Arc::new(SharedAtomicU32::open(count_path(base))?);
        let wakeup = Arc::new(SharedAtomicU64::open(wakeup_path(base))?);
        let waiters = Arc::new(SharedAtomicU32::open(waiters_path(base))?);
        Ok(Self {
            count, wakeup, waiters, max_permits,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Non-blocking acquire. Returns `Err(WouldBlock)` immediately
    /// when no permits are available.
    pub fn try_acquire(&self) -> Result<Permit<'_>, SemaphoreError> {
        loop {
            let cur = self.count.load(Ordering::Acquire);
            if cur == 0 {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::semaphore::OP_TRY_ACQUIRE, 1); // would-block (no permits)
                return Err(SemaphoreError::WouldBlock);
            }
            match self.count.compare_exchange(
                cur, cur - 1, Ordering::AcqRel, Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::semaphore::OP_TRY_ACQUIRE, 0);
                    return Ok(Permit { sem: self });
                }
                Err(_) => continue, // CAS lost; retry
            }
        }
    }

    /// Blocking acquire. Spins on a generation-counter wakeup signal;
    /// yields between spins and sleeps briefly after a yield budget.
    pub fn acquire(&self) -> Permit<'_> {
        // Hot try-CAS first.
        let mut had_contention = false;
        loop {
            let cur = self.count.load(Ordering::Acquire);
            if cur > 0 {
                if self.count.compare_exchange(
                    cur, cur - 1, Ordering::AcqRel, Ordering::Acquire,
                ).is_ok() {
                    self.ring_sidecar.push_op(
                        crate::sidecar_ops::semaphore::OP_ACQUIRE,
                        if had_contention { 1 } else { 0 },
                    );
                    return Permit { sem: self };
                }
                continue;
            }
            had_contention = true;
            // Slow path: park on wakeup generation.
            self.waiters.fetch_add(1, Ordering::AcqRel);
            let snapshot = self.wakeup.load(Ordering::Acquire);
            // Re-check after registering as waiter (avoid lost-wakeup race).
            if self.count.load(Ordering::Acquire) > 0 {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                continue;
            }
            // Wait until either count > 0 OR wakeup advances.
            let mut spins = 0u32;
            loop {
                let cur_count = self.count.load(Ordering::Acquire);
                let cur_gen = self.wakeup.load(Ordering::Acquire);
                if cur_count > 0 || cur_gen != snapshot {
                    self.waiters.fetch_sub(1, Ordering::AcqRel);
                    break;
                }
                spins += 1;
                if spins < 32 {
                    std::hint::spin_loop();
                } else if spins < 256 {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_micros(50));
                }
            }
        }
    }

    /// Blocking acquire with deadline. Returns `Err(Timeout)` when
    /// the deadline passes before a permit becomes available.
    pub fn acquire_timeout(&self, timeout: Duration) -> Result<Permit<'_>, SemaphoreError> {
        let deadline = Instant::now() + timeout;
        let mut had_contention = false;
        loop {
            let cur = self.count.load(Ordering::Acquire);
            if cur > 0 {
                if self.count.compare_exchange(
                    cur, cur - 1, Ordering::AcqRel, Ordering::Acquire,
                ).is_ok() {
                    self.ring_sidecar.push_op(
                        crate::sidecar_ops::semaphore::OP_ACQUIRE,
                        if had_contention { 1 } else { 0 },
                    );
                    return Ok(Permit { sem: self });
                }
                continue;
            }
            had_contention = true;
            if Instant::now() >= deadline {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::semaphore::OP_ACQUIRE, 1); // timed out
                return Err(SemaphoreError::Timeout);
            }
            self.waiters.fetch_add(1, Ordering::AcqRel);
            let snapshot = self.wakeup.load(Ordering::Acquire);
            if self.count.load(Ordering::Acquire) > 0 {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                continue;
            }
            let mut spins = 0u32;
            loop {
                let cur_count = self.count.load(Ordering::Acquire);
                let cur_gen = self.wakeup.load(Ordering::Acquire);
                if cur_count > 0 || cur_gen != snapshot {
                    self.waiters.fetch_sub(1, Ordering::AcqRel);
                    break;
                }
                if Instant::now() >= deadline {
                    self.waiters.fetch_sub(1, Ordering::AcqRel);
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::semaphore::OP_ACQUIRE, 1); // timed out
                    return Err(SemaphoreError::Timeout);
                }
                spins += 1;
                if spins < 32 {
                    std::hint::spin_loop();
                } else if spins < 256 {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_micros(50));
                }
            }
        }
    }

    /// Standalone release (one permit). Use this when a Permit guard
    /// has been mem::forgotten to transfer ownership across an API
    /// boundary that can't carry the lifetime. Returns
    /// `Err(ReleaseOverflow)` if releasing pushes the count past
    /// `max_permits`; rolls back the count in that case.
    pub fn release(&self) -> Result<(), SemaphoreError> {
        let prev = self.count.fetch_add(1, Ordering::AcqRel);
        if prev >= self.max_permits {
            // Rollback: someone misuses the API by releasing more
            // than they acquired.
            self.count.fetch_sub(1, Ordering::AcqRel);
            self.ring_sidecar
                .push_op(crate::sidecar_ops::semaphore::OP_RELEASE, 1); // overflow / rolled back
            return Err(SemaphoreError::ReleaseOverflow);
        }
        if self.waiters.load(Ordering::Acquire) > 0 {
            self.wakeup.fetch_add(1, Ordering::Release);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::semaphore::OP_RELEASE, 0);
        Ok(())
    }

    /// Currently available permit count (observational; may race).
    #[inline]
    pub fn available(&self) -> u32 {
        self.count.load(Ordering::Acquire)
    }

    /// Currently waiting acquirers (observational).
    #[inline]
    pub fn waiters(&self) -> u32 {
        self.waiters.load(Ordering::Acquire)
    }

    /// Maximum permit cap configured at construction.
    #[inline]
    pub fn max_permits(&self) -> u32 { self.max_permits }

    /// Current wakeup-generation counter snapshot. Used by the
    /// `BlockingSemaphore` wrapper to compute waker park targets:
    /// the wrapper snapshots this BEFORE checking `available()`,
    /// then parks at `snapshot + 1`. Any subsequent `release` bumps
    /// the generation, which the wake call observes as `seq >=
    /// target`.
    #[inline]
    pub fn wakeup_generation(&self) -> u64 {
        self.wakeup.load(Ordering::Acquire)
    }

    /// Mark the calling thread as entering the waiter set. Callers
    /// must pair every `mark_waiter_entered` with exactly one
    /// `mark_waiter_left`. The existing `acquire` / `acquire_timeout`
    /// slow paths call these around their sleep loop; the
    /// `BlockingSemaphore` wrapper calls them around its
    /// kernel-park slow path.
    ///
    /// The internal release path keys its wakeup-bump on
    /// `waiters > 0`, so a parker that does NOT register here will
    /// not be woken (the wakeup generation stays unchanged).
    #[inline]
    pub fn mark_waiter_entered(&self) {
        self.waiters.fetch_add(1, Ordering::AcqRel);
    }

    /// Counterpart to `mark_waiter_entered`. Must be called exactly
    /// once per entered marker (regardless of whether the parker
    /// woke from the release or timed out).
    #[inline]
    pub fn mark_waiter_left(&self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }

    /// Sync all three files to disk.
    pub fn flush(&self) -> Result<(), SemaphoreError> {
        self.count.flush()?;
        self.wakeup.flush()?;
        self.waiters.flush()?;
        Ok(())
    }

    /// Non-blocking flush of all three files. Delegates to each
    /// inner SharedAtomic's flush_async.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), SemaphoreError> {
        self.count.flush_async()?;
        self.wakeup.flush_async()?;
        self.waiters.flush_async()?;
        Ok(())
    }
}

/// RAII permit guard. Dropping releases one permit back to the
/// semaphore. Use `mem::forget(permit)` + `sem.release()` to
/// transfer ownership.
pub struct Permit<'a> {
    sem: &'a SharedSemaphore,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        // Ignore overflow on drop: that indicates the user has
        // released more permits than they acquired via a separate
        // path; the rollback inside `release` keeps the count
        // bounded by max_permits anyway.
        self.sem.release().ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as O};
    use std::sync::Barrier;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-semaphore-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        std::fs::remove_file(count_path(base)).ok();
        std::fs::remove_file(wakeup_path(base)).ok();
        std::fs::remove_file(waiters_path(base)).ok();
    }

    #[test]
    fn create_initial_state_is_correct() {
        let base = tmp_base("init");
        let sem = SharedSemaphore::create(&base, 4, 4).unwrap();
        assert_eq!(sem.available(), 4);
        assert_eq!(sem.waiters(), 0);
        assert_eq!(sem.max_permits(), 4);
        cleanup(&base);
    }

    #[test]
    fn try_acquire_succeeds_until_empty_then_returns_would_block() {
        let base = tmp_base("try");
        let sem = SharedSemaphore::create(&base, 3, 3).unwrap();
        let _p1 = sem.try_acquire().unwrap();
        let _p2 = sem.try_acquire().unwrap();
        let _p3 = sem.try_acquire().unwrap();
        assert_eq!(sem.try_acquire().err(), Some(SemaphoreError::WouldBlock));
        cleanup(&base);
    }

    #[test]
    fn permit_drop_releases() {
        let base = tmp_base("drop");
        let sem = SharedSemaphore::create(&base, 1, 1).unwrap();
        {
            let _p = sem.try_acquire().unwrap();
            assert_eq!(sem.available(), 0);
        }
        assert_eq!(sem.available(), 1);
        cleanup(&base);
    }

    #[test]
    fn acquire_blocks_until_release() {
        let base = tmp_base("block-release");
        let sem = Arc::new(SharedSemaphore::create(&base, 1, 1).unwrap());
        let p1 = sem.try_acquire().unwrap();
        // Hold p1; spawn a thread that tries to acquire (it blocks).
        let sem2 = sem.clone();
        let h = thread::spawn(move || {
            let _p = sem2.acquire();  // blocks until p1 dropped
            42u32
        });
        thread::sleep(Duration::from_millis(20));
        // Drop p1 to release; thread should now complete.
        drop(p1);
        let v = h.join().unwrap();
        assert_eq!(v, 42);
        cleanup(&base);
    }

    #[test]
    fn acquire_timeout_returns_timeout_when_no_permit() {
        let base = tmp_base("timeout");
        let sem = SharedSemaphore::create(&base, 0, 1).unwrap();
        let start = Instant::now();
        let r = sem.acquire_timeout(Duration::from_millis(20));
        let elapsed = start.elapsed();
        assert_eq!(r.err(), Some(SemaphoreError::Timeout));
        assert!(elapsed >= Duration::from_millis(20));
        assert!(elapsed < Duration::from_millis(200), "timeout took too long: {elapsed:?}");
        cleanup(&base);
    }

    #[test]
    fn acquire_timeout_succeeds_when_released_before_deadline() {
        let base = tmp_base("timeout-ok");
        let sem = Arc::new(SharedSemaphore::create(&base, 0, 1).unwrap());
        let sem2 = sem.clone();
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            sem2.release().unwrap();
        });
        let p = sem.acquire_timeout(Duration::from_millis(500)).unwrap();
        drop(p);
        releaser.join().unwrap();
        cleanup(&base);
    }

    #[test]
    fn release_overflow_is_rejected_and_rolls_back() {
        let base = tmp_base("overflow");
        let sem = SharedSemaphore::create(&base, 1, 1).unwrap();
        // count is already at max (1); release pushes to 2.
        assert_eq!(sem.release().err(), Some(SemaphoreError::ReleaseOverflow));
        assert_eq!(sem.available(), 1);  // rolled back
        cleanup(&base);
    }

    #[test]
    fn cross_handle_acquire_release() {
        let base = tmp_base("cross-handle");
        let owner = SharedSemaphore::create(&base, 2, 2).unwrap();
        let consumer = SharedSemaphore::open(&base, 2).unwrap();
        let p = owner.try_acquire().unwrap();
        // Consumer sees 1 left.
        assert_eq!(consumer.available(), 1);
        // Consumer acquires; both held.
        let q = consumer.try_acquire().unwrap();
        assert_eq!(owner.available(), 0);
        assert_eq!(consumer.try_acquire().err(), Some(SemaphoreError::WouldBlock));
        drop(p);
        drop(q);
        assert_eq!(owner.available(), 2);
        cleanup(&base);
    }

    #[test]
    fn contended_8_threads_bounded_to_2_permits() {
        let base = tmp_base("contended");
        let sem = Arc::new(SharedSemaphore::create(&base, 2, 2).unwrap());
        let n_threads = 8;
        let per_thread = 5;
        let in_flight = Arc::new(AtomicU32::new(0));
        let max_seen = Arc::new(AtomicU32::new(0));
        let barrier = Arc::new(Barrier::new(n_threads));
        let mut handles = vec![];
        for _ in 0..n_threads {
            let sem = sem.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..per_thread {
                    let _p = sem.acquire();
                    let cur = in_flight.fetch_add(1, O::AcqRel) + 1;
                    max_seen.fetch_max(cur, O::AcqRel);
                    thread::sleep(Duration::from_micros(100));
                    in_flight.fetch_sub(1, O::AcqRel);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        // At no point should more than 2 holders have been concurrent.
        assert!(max_seen.load(O::Acquire) <= 2,
            "saw {} concurrent holders, expected <= 2",
            max_seen.load(O::Acquire));
        assert_eq!(sem.available(), 2);
        cleanup(&base);
    }

    #[test]
    fn many_waiters_all_eventually_acquire() {
        let base = tmp_base("many-waiters");
        let sem = Arc::new(SharedSemaphore::create(&base, 0, 4).unwrap());
        let n = 4;
        let count = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];
        for _ in 0..n {
            let sem = sem.clone();
            let count = count.clone();
            handles.push(thread::spawn(move || {
                let _p = sem.acquire();
                count.fetch_add(1, O::AcqRel);
                thread::sleep(Duration::from_micros(100));
            }));
        }
        // Stagger releases.
        for _ in 0..n {
            thread::sleep(Duration::from_millis(5));
            sem.release().unwrap();
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(count.load(O::Acquire), n);
        cleanup(&base);
    }

    #[test]
    fn standalone_release_works_with_forgotten_permit() {
        let base = tmp_base("forget");
        let sem = SharedSemaphore::create(&base, 1, 1).unwrap();
        let p = sem.try_acquire().unwrap();
        std::mem::forget(p);  // ownership transferred via different API
        assert_eq!(sem.available(), 0);
        sem.release().unwrap();
        assert_eq!(sem.available(), 1);
        cleanup(&base);
    }

    #[test]
    fn waiters_counter_reflects_blocked_threads() {
        let base = tmp_base("waiters");
        let sem = Arc::new(SharedSemaphore::create(&base, 0, 4).unwrap());
        let n = 3;
        let mut handles = vec![];
        for _ in 0..n {
            let sem = sem.clone();
            handles.push(thread::spawn(move || {
                let _p = sem.acquire();
            }));
        }
        // Give time for all three to enter the wait loop.
        let mut tries = 0;
        while sem.waiters() < n as u32 && tries < 100 {
            thread::sleep(Duration::from_millis(5));
            tries += 1;
        }
        assert!(sem.waiters() >= n as u32 - 1,  // allow off-by-one race
            "expected ~{n} waiters, saw {}", sem.waiters());
        // Release all so the threads complete.
        for _ in 0..n { sem.release().unwrap(); }
        for h in handles { h.join().unwrap(); }
        assert_eq!(sem.waiters(), 0);
        cleanup(&base);
    }
}
