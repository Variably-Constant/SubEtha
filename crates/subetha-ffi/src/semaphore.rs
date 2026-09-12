//! The counting semaphore through the C ABI: a bounded number of permits
//! that any number of processes take and give back, for limiting how many
//! of them work on something at once.
//!
//! A permit is a handle of its own. The acquiring calls hand one back;
//! destroying it gives the permit back, and it keeps the semaphore alive,
//! so a caller may destroy the semaphore's handle first. A caller that
//! leaks a permit keeps it taken for the life of its process.
//!
//! `subetha_semaphore_acquire` waits up to `timeout_ms` and polls while
//! it waits, and destroying the semaphore's handle releases every caller
//! waiting on it with `SUBETHA_E_DESTROYED`, so a destroy and
//! `subetha_shutdown` both complete while an acquire is outstanding. A
//! waiting caller registers itself, so a release from any process bumps
//! the wakeup generation the Rust waiters watch.
//!
//! Three files carry the state, named from the `path` a caller gives:
//! `<path>.count.bin` holds the permits available, `<path>.wakeup.bin` the
//! generation a release advances, and `<path>.waiters.bin` the count of
//! callers waiting. `subetha_semaphore_unlink` removes all three.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_semaphore::{SemaphoreError, SharedSemaphore};

use crate::error::{
    fail, semaphore_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_TIMEOUT, SUBETHA_E_WOULD_BLOCK, SUBETHA_E_WRONG_KIND,
    SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_SEMAPHORE};
use crate::holds::{HoldTable, ReleaseError};
use crate::ring::{deadline_from, finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{poll_until, Waiting};

/// A snapshot of a semaphore.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_semaphore_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// The ceiling the semaphore was created with.
    pub max_permits: u32,
    /// Permits free to take right now, across every process.
    pub available: u32,
    /// Callers waiting for a permit, across every process.
    pub waiters: u32,
    /// The generation a release advances, which waiters watch.
    pub wakeup_generation: u64,
    /// Releases refused because the count was already at `max_permits`:
    /// permits given back by some path that never took them.
    pub release_overflows: u64,
    /// Acquires through this handle that gave up when their timeout
    /// elapsed.
    pub timeouts: u64,
}

pub(crate) struct SemaphoreObject {
    semaphore: Arc<SharedSemaphore>,
    mode: u32,
    waiting: Arc<Waiting>,
    timeouts: AtomicU64,
    /// Releases a permit could not perform, kept here so the count
    /// survives the permit it belonged to.
    overflows: Arc<AtomicU64>,
    /// The permits taken through this handle. Sized to the semaphore's
    /// own `max_permits`, so the table refuses only where the semaphore
    /// would have refused too.
    permits: HoldTable,
}

impl Drop for SemaphoreObject {
    /// Give back every permit still outstanding. The semaphore goes with
    /// this object, so a permit left taken would leave the count short
    /// with nothing able to return it, and every other process would see
    /// a semaphore that never refills.
    fn drop(&mut self) {
        for _ in self.permits.drain() {
            give_back(&self.semaphore, &self.overflows);
        }
    }
}

impl SemaphoreObject {
    /// Release every caller waiting on this semaphore so a destroy can
    /// proceed. The waiting callers are polling and check this flag
    /// between tries.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
    }

    fn stats(&self) -> subetha_semaphore_stats {
        subetha_semaphore_stats {
            mode: self.mode,
            max_permits: self.semaphore.max_permits(),
            available: self.semaphore.available(),
            waiters: self.semaphore.waiters(),
            wakeup_generation: self.semaphore.wakeup_generation(),
            release_overflows: self.semaphore.permit_release_overflows() + self.overflows.load(Ordering::Acquire),
            timeouts: self.timeouts.load(Ordering::Acquire),
        }
    }
}

/// Give a permit back to the semaphore. Paired with [`take`], and
/// reached only from a token the hold table has just accepted, so it runs
/// once per permit however many times a caller calls the release.
fn give_back(semaphore: &SharedSemaphore, overflows: &AtomicU64) {
    match semaphore.release() {
        Ok(()) => {}
        Err(e) => {
            // The count was already at `max_permits`: permits were given
            // back by some path that never took them. The semaphore's own
            // rollback keeps the count bounded, and this counts the
            // refusal so `release_overflows` reports it rather than the
            // permit vanishing quietly.
            overflows.fetch_add(1, Ordering::AcqRel);
            debug_assert!(
                matches!(e, SemaphoreError::ReleaseOverflow),
                "a permit release reported {e:?} rather than an overflow",
            );
        }
    }
}

/// Take a permit and keep it past the end of this call.
///
/// The Rust guard gives the permit back when it is dropped, and here the
/// permit outlives the call, so the guard is forgotten on purpose and
/// [`give_back`] releases instead.
fn take(semaphore: &SharedSemaphore) -> Result<(), SemaphoreError> {
    let permit = semaphore.try_acquire()?;
    std::mem::forget(permit);
    Ok(())
}

fn with_semaphore(handle: subetha_handle, f: impl FnOnce(&SemaphoreObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_SEMAPHORE, |object| match object {
        Object::Semaphore(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a semaphore"),
    })
}

/// The three files a semaphore's state lives in, named from the base
/// `path` a caller gives.
fn backing_files(base: &Path) -> Option<[PathBuf; 3]> {
    let stem = base.file_name()?.to_string_lossy().to_string();
    Some(["count", "wakeup", "waiters"].map(|suffix| {
        let mut file = base.to_path_buf();
        file.set_file_name(format!("{stem}.{suffix}.bin"));
        file
    }))
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    max_permits: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if max_permits == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_permits is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, mode))
}

fn place(semaphore: Result<SharedSemaphore, SemaphoreError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match semaphore {
        Ok(semaphore) => {
            let permits = HoldTable::bounded(semaphore.max_permits() as usize);
            let object = SemaphoreObject {
                semaphore: Arc::new(semaphore),
                mode,
                waiting: Arc::new(Waiting::new()),
                timeouts: AtomicU64::new(0),
                overflows: Arc::new(AtomicU64::new(0)),
                permits,
            };
            unsafe { issue(Object::Semaphore(object), out) }
        }
        Err(e) => semaphore_code(e),
    }
}

/// Create the semaphore at `path` with `initial` permits available and
/// `max_permits` as the ceiling a release may not push the count past.
/// `initial` above `max_permits` is refused. The three backing files are
/// initialized, so an existing semaphore at the same path is set back to
/// `initial`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_create(
    path: *const c_char,
    initial: u32,
    max_permits: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, max_permits, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        if initial > max_permits {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("initial {initial} is above max_permits {max_permits}"),
            );
        }
        place(SharedSemaphore::create(path, initial, max_permits), mode, out)
    })
}

/// Attach to the semaphore another process created at `path`, with the
/// same `max_permits` the creator used; the backing files must exist.
/// `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_open(
    path: *const c_char,
    max_permits: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, max_permits, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedSemaphore::open(path, max_permits), mode, out)
    })
}

/// Record a permit already taken, and write the token naming it into
/// `out`. The permit is given back if no slot can be had, so a refusal
/// leaves the count exactly as it was rather than short a permit nobody
/// holds.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn issue_token(s: &SemaphoreObject, out: *mut u64) -> i32 {
    match s.permits.claim(0) {
        Some(token) => {
            // SAFETY: the caller checked `out` is non-null and writable.
            unsafe { *out = token };
            SUBETHA_OK
        }
        None => {
            give_back(&s.semaphore, &s.overflows);
            fail(SUBETHA_E_WOULD_BLOCK, "the semaphore has no room to record another permit")
        }
    }
}

/// Take a permit without waiting, and write the token naming it into
/// `out`. `SUBETHA_E_WOULD_BLOCK` when no permit is available.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_try_acquire(handle: subetha_handle, out: *mut u64) -> i32 {
    with_semaphore(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match take(&s.semaphore) {
            Ok(()) => {
                unsafe { issue_token(s, out) }
            }
            Err(e) => semaphore_code(e),
        }
    })
}

/// `subetha_semaphore_try_acquire`, waiting up to `timeout_ms` for a
/// permit; `SUBETHA_WAIT_FOREVER` waits without a deadline.
/// `SUBETHA_E_TIMEOUT` when the wait elapses and `SUBETHA_E_DESTROYED`
/// when the semaphore's handle is destroyed under it. The caller counts
/// among the waiters while it waits, so a release from any process
/// advances the wakeup generation.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_acquire(handle: subetha_handle, timeout_ms: i64, out: *mut u64) -> i32 {
    with_semaphore(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        // A first try before registering keeps the uncontended path off
        // the waiter count entirely.
        let taken = match take(&s.semaphore) {
            Ok(()) => Ok(()),
            Err(SemaphoreError::WouldBlock) => {
                s.semaphore.mark_waiter_entered();
                let waited = poll_until(&s.waiting, deadline, SUBETHA_E_WOULD_BLOCK, || {
                    take(&s.semaphore).map_err(semaphore_code)
                });
                s.semaphore.mark_waiter_left();
                waited
            }
            Err(e) => Err(semaphore_code(e)),
        };
        match taken {
            Ok(()) => {
                unsafe { issue_token(s, out) }
            }
            Err(code) => {
                if code == SUBETHA_E_TIMEOUT {
                    s.timeouts.fetch_add(1, Ordering::AcqRel);
                }
                code
            }
        }
    })
}

/// Give the permit `token` names back. Another process waiting on the
/// semaphore is free to take it as soon as this returns, and the token
/// names nothing afterwards.
///
/// A token given back twice is refused rather than returning a permit the
/// semaphore never lent, and so is a token from a permit that ended whose
/// slot has since been taken by another.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_semaphore_release(handle: subetha_handle, token: u64) -> i32 {
    with_semaphore(handle, |s| match s.permits.release(token) {
        Ok(_) => {
            give_back(&s.semaphore, &s.overflows);
            SUBETHA_OK
        }
        Err(ReleaseError::NotHeld) => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the token names no permit on this semaphore: it was given back already, or never taken",
        ),
        Err(ReleaseError::Stale) => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the token is from a permit that ended, and its slot has been taken since",
        ),
        Err(ReleaseError::NoSuchSlot) => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the token names no slot on this semaphore")
        }
        Err(ReleaseError::NotAToken) => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "that is a handle, not a permit token")
        }
    })
}

/// Whether `token` still names a permit this semaphore has lent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_holds(handle: subetha_handle, token: u64, out: *mut bool) -> i32 {
    with_semaphore(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = s.permits.kind(token).is_ok() };
        SUBETHA_OK
    })
}

/// A snapshot of the semaphore into `out`. Every count races an acquire
/// or a release in some process, so it is a diagnostic rather than
/// something to branch on.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_read_stats(handle: subetha_handle, out: *mut subetha_semaphore_stats) -> i32 {
    with_semaphore(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = s.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Permits this handle has lent and not had back, which a caller reads
/// to find a leak: it should return to zero once every token taken has
/// been released.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_held(handle: subetha_handle, out: *mut u64) -> i32 {
    with_semaphore(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = s.permits.live() as u64 };
        SUBETHA_OK
    })
}

/// Remove the semaphore's three files at `path`: `<path>.count.bin`,
/// `<path>.wakeup.bin` and `<path>.waiters.bin`. The contract is
/// `subetha_ring_unlink`'s, and the report counts all three.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_semaphore_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let base = match unsafe { text(path, "path") } {
            Ok(p) => PathBuf::from(p),
            Err(code) => return code,
        };
        let files = match backing_files(&base) {
            Some(f) => f,
            None => return fail(SUBETHA_E_INVALID_ARGUMENT, "path names no file"),
        };
        let mut found = UnlinkReport::default();
        for file in files {
            found.remove(file);
        }
        unsafe { finish_unlink(found, report) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SUBETHA_E_DESTROYED;
    use crate::runtime::SUBETHA_MODE_STRICT;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!("subetha-ffi-semaphore-{name}-{}", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            for file in backing_files(&self.0).expect("the scratch path names a file") {
                found.remove(file);
            }
            assert_eq!(found.failed, 0, "the semaphore's files were removed: {:?}", found.first_failure);
        }
    }

    fn object(path: &Path, initial: u32, max: u32) -> SemaphoreObject {
        SemaphoreObject {
            semaphore: Arc::new(SharedSemaphore::create(path, initial, max).unwrap()),
            mode: SUBETHA_MODE_STRICT,
            waiting: Arc::new(Waiting::new()),
            timeouts: AtomicU64::new(0),
            overflows: Arc::new(AtomicU64::new(0)),
            permits: HoldTable::bounded(max as usize),
        }
    }

    /// A permit whose drop gives it back, so a test writes one as a
    /// block.
    struct Held<'a> {
        semaphore: &'a SemaphoreObject,
    }

    impl Drop for Held<'_> {
        fn drop(&mut self) {
            give_back(&self.semaphore.semaphore, &self.semaphore.overflows);
        }
    }

    /// A permit, or `None` when none is available. An empty count is the
    /// answer these tests ask for; any other refusal is a fault in the
    /// fixture and is raised rather than read as "none free".
    fn permit(s: &SemaphoreObject) -> Option<Held<'_>> {
        match take(&s.semaphore) {
            Ok(()) => Some(Held { semaphore: s }),
            Err(SemaphoreError::WouldBlock) => None,
            Err(e) => panic!("the test semaphore reported {e:?} rather than an empty count"),
        }
    }

    /// Each permit's lifetime is a block here, so what gives it back is
    /// the same `give_back` a release reaches once its token has been
    /// accepted.
    #[test]
    fn permits_run_out_and_a_released_permit_is_taken_again() {
        let scratch = Scratch::new("permits");
        let s = object(&scratch.0, 2, 2);
        assert_eq!(s.stats().available, 2);
        assert_eq!(s.stats().max_permits, 2);
        assert_eq!(s.stats().waiters, 0);

        {
            let _first = permit(&s).expect("a first permit");
            {
                let _second = permit(&s).expect("a second permit");
                assert_eq!(s.stats().available, 0);
                assert!(permit(&s).is_none(), "the count is out");
            }
            assert_eq!(s.stats().available, 1, "the second permit came back");
            assert!(permit(&s).is_some(), "and is taken again");
        }
        assert_eq!(s.stats().available, 2, "every permit came back");
        assert_eq!(s.stats().release_overflows, 0);
    }

    #[test]
    fn a_wait_ends_on_its_deadline_and_a_destroy_releases_an_unbounded_one() {
        let scratch = Scratch::new("wait");
        let s = object(&scratch.0, 1, 1);
        let _held = permit(&s).expect("the only permit");

        // A bounded wait against an empty count gives up rather than
        // hanging, and the waiter count comes back with it.
        s.semaphore.mark_waiter_entered();
        assert_eq!(s.stats().waiters, 1);
        let deadline = Some(std::time::Instant::now() + std::time::Duration::from_millis(20));
        let waited = poll_until(&s.waiting, deadline, SUBETHA_E_WOULD_BLOCK, || {
            take(&s.semaphore).map_err(semaphore_code)
        });
        s.semaphore.mark_waiter_left();
        assert_eq!(waited.unwrap_err(), SUBETHA_E_TIMEOUT);
        assert_eq!(s.stats().waiters, 0, "the waiter count came back");
        assert_eq!(s.stats().available, 0, "the wait left the permit alone");

        // An unbounded wait is released by the destroy's interrupt, which
        // is what keeps a destroy from being held up by it.
        let waiting = Arc::clone(&s.waiting);
        let semaphore = Arc::clone(&s.semaphore);
        let waiter = std::thread::spawn(move || {
            poll_until(&waiting, None, SUBETHA_E_WOULD_BLOCK, || {
                take(&semaphore).map_err(semaphore_code)
            })
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        s.interrupt();
        assert_eq!(waiter.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }

    /// Giving back a permit that was never taken is refused and counted,
    /// and the count stays at the ceiling either way.
    #[test]
    fn a_release_past_the_ceiling_is_counted_and_the_count_stays_bounded() {
        let scratch = Scratch::new("overflow");
        let s = object(&scratch.0, 1, 1);
        assert_eq!(s.stats().available, 1);
        give_back(&s.semaphore, &s.overflows);
        assert_eq!(s.stats().available, 1, "the count stayed at the ceiling");
        assert_eq!(s.stats().release_overflows, 1, "the refused release was counted");
    }
}
