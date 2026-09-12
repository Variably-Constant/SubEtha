//! The reader-writer lock through the C ABI: many readers or one writer,
//! across processes, with writer priority.
//!
//! A hold is a handle of its own. The acquiring calls hand one back;
//! destroying it releases the lock, and it keeps the lock alive, so a
//! caller may destroy the lock's handle first. A caller that leaks a hold
//! keeps the lock shut for the life of its process.
//!
//! `subetha_rwlock_read` and `subetha_rwlock_write` wait up to
//! `timeout_ms` and poll while they wait, since a release on this lock
//! signals nobody. Destroying the lock's handle releases every caller
//! waiting on it with `SUBETHA_E_DESTROYED`, so a destroy and
//! `subetha_shutdown` both complete while an acquire is outstanding.
//!
//! A waiting writer registers itself for as long as it waits, so new
//! readers are refused and it takes the lock ahead of them; giving up on
//! the timeout takes the registration back with it.
//!
//! A holder that exits without releasing leaves its state set for the
//! life of the file. `subetha_rwlock_reset` clears it, discarding
//! whatever a live holder owns.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_rw_lock::{RWLockError, SharedRWLock};

use crate::error::{
    fail, rwlock_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_TIMEOUT, SUBETHA_E_WOULD_BLOCK, SUBETHA_E_WRONG_KIND,
    SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_RWLOCK};
use crate::holds::{HoldTable, ReleaseError};
use crate::ring::{deadline_from, finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{poll_until, Waiting};

/// A hold taken for reading: others may read beside it, none may write.
pub const SUBETHA_LOCK_READ: u32 = 0;
/// A hold taken for writing: nobody else may read or write.
pub const SUBETHA_LOCK_WRITE: u32 = 1;

/// A snapshot of a lock.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_rwlock_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Holds taken for reading right now, across every process.
    pub readers: u32,
    /// Writers waiting, across every process. A new reader is refused
    /// while this is above zero.
    pub waiting_writers: u32,
    /// Whether a hold is taken for writing right now.
    pub has_writer: bool,
    /// Acquires through this handle that gave up when their timeout
    /// elapsed.
    pub timeouts: u64,
}

pub(crate) struct RwLockObject {
    lock: Arc<SharedRWLock>,
    mode: u32,
    waiting: Arc<Waiting>,
    timeouts: AtomicU64,
    /// The write hold, of which the lock allows one.
    writes: HoldTable,
    /// The read holds. `SharedRWLock` counts its readers and names no
    /// ceiling, so neither does this: a table that stopped growing would
    /// refuse a read the lock itself allows.
    reads: HoldTable,
}

impl Drop for RwLockObject {
    /// Give back every hold still outstanding. The lock goes with this
    /// object, so a hold left taken would leave the file saying it is
    /// held with nothing able to release it, and every other process
    /// would wait on it forever.
    fn drop(&mut self) {
        for (_, kind) in self.writes.drain() {
            give_back(&self.lock, kind);
        }
        for (_, kind) in self.reads.drain() {
            give_back(&self.lock, kind);
        }
    }
}

impl RwLockObject {
    /// Release every caller waiting on this lock so a destroy can
    /// proceed. There is no waker to signal: the waiting callers are
    /// polling, and they check this flag between tries.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
    }

    fn stats(&self) -> subetha_rwlock_stats {
        subetha_rwlock_stats {
            mode: self.mode,
            readers: self.lock.reader_count(),
            waiting_writers: self.lock.waiting_writers(),
            has_writer: self.lock.has_writer(),
            timeouts: self.timeouts.load(Ordering::Acquire),
        }
    }
}

/// Give a hold of `kind` back to the lock. Paired with [`take`], and
/// reached only from a token the hold table has just accepted, so it runs
/// once per hold however many times a caller calls the unlock.
fn give_back(lock: &SharedRWLock, kind: u32) {
    if kind == SUBETHA_LOCK_WRITE {
        lock.release_write_for_blocking();
    } else {
        lock.release_read_for_blocking();
    }
}

/// Take a hold of `kind` and keep it past the end of this call.
/// `registered` says this caller counts among the waiting writers, so its
/// claim must be the form that clears that registration as it takes the
/// lock.
///
/// The Rust guard releases the lock when it is dropped, and here the hold
/// outlives the call, so the guard is forgotten on purpose and
/// [`give_back`] releases instead.
fn take(lock: &SharedRWLock, kind: u32, registered: bool) -> Result<(), RWLockError> {
    if kind == SUBETHA_LOCK_WRITE {
        let guard = if registered { lock.try_write_lock_registered()? } else { lock.try_write_lock()? };
        std::mem::forget(guard);
    } else {
        let guard = lock.try_read_lock()?;
        std::mem::forget(guard);
    }
    Ok(())
}

fn with_rwlock(handle: subetha_handle, f: impl FnOnce(&RwLockObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_RWLOCK, |object| match object {
        Object::RwLock(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a lock"),
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(path: *const c_char, mode: u32, out: *mut subetha_handle) -> Result<(&'a Path, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let mode = resolve_mode(mode)?;
    Ok((path, mode))
}

fn place(lock: Result<SharedRWLock, RWLockError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match lock {
        Ok(lock) => {
            let object = RwLockObject {
                lock: Arc::new(lock),
                mode,
                waiting: Arc::new(Waiting::new()),
                timeouts: AtomicU64::new(0),
                writes: HoldTable::bounded(1),
                reads: HoldTable::unbounded(),
            };
            unsafe { issue(Object::RwLock(object), out) }
        }
        Err(e) => rwlock_code(e),
    }
}

/// Obtain the lock at `path`: an empty one is initialized when the file
/// does not exist, an existing one is attached with whatever holds are
/// live, so a caller arriving while another process holds the write lock
/// does not clear it.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_create(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedRWLock::create(path), mode, out)
    })
}

/// Attach to the lock another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_open(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedRWLock::open(path), mode, out)
    })
}

/// Truncate the file at `path` and initialize an unheld lock there,
/// discarding whatever a live holder owns.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_reset(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedRWLock::reset(path), mode, out)
    })
}

/// Set in a token taken for reading, so an unlock knows which table to
/// give it back to without asking both and having to choose between two
/// refusals. This is the family bit, below the tag every token carries;
/// the slot index has the thirty bits under it.
const READ_TOKEN: u64 = 1 << 62;

/// The table a hold of `kind` is taken from: one writer, and readers
/// without a ceiling because the lock names none.
fn table_for(l: &RwLockObject, kind: u32) -> &HoldTable {
    if kind == SUBETHA_LOCK_WRITE { &l.writes } else { &l.reads }
}

/// Record a hold already taken on the lock, and write the token naming it
/// into `out`.
///
/// The lock is given back if no slot can be had, so a refusal here leaves
/// the lock exactly as it was rather than held by a token nobody has.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn issue_token(l: &RwLockObject, kind: u32, out: *mut u64) -> i32 {
    match table_for(l, kind).claim(kind) {
        Some(token) => {
            let token = if kind == SUBETHA_LOCK_WRITE { token } else { token | READ_TOKEN };
            // SAFETY: the caller checked `out` is non-null and writable.
            unsafe { *out = token };
            SUBETHA_OK
        }
        None => {
            give_back(&l.lock, kind);
            fail(
                SUBETHA_E_WOULD_BLOCK,
                format!("the lock has no room to record another hold of kind {kind}"),
            )
        }
    }
}

/// Take a hold of `kind` without waiting, and write the token naming it
/// into `out`.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn try_acquire(handle: subetha_handle, kind: u32, out: *mut u64) -> i32 {
    with_rwlock(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match take(&l.lock, kind, false) {
            Ok(()) => unsafe { issue_token(l, kind, out) },
            Err(e) => rwlock_code(e),
        }
    })
}

/// Take a hold of `kind`, waiting up to `timeout_ms`, and write the token
/// naming it into `out`. A writer registers for the length of its wait,
/// so new readers are refused while it waits, and the registration goes
/// back whether the wait succeeds or ends.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn wait_acquire(handle: subetha_handle, kind: u32, timeout_ms: i64, out: *mut u64) -> i32 {
    with_rwlock(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        let registered = kind == SUBETHA_LOCK_WRITE;
        if registered {
            l.lock.register_waiting_writer();
        }
        let taken = poll_until(&l.waiting, deadline, SUBETHA_E_WOULD_BLOCK, || {
            take(&l.lock, kind, registered).map_err(rwlock_code)
        });
        match taken {
            // The claim cleared the registration as it took the lock, so
            // there is nothing to give back here.
            Ok(()) => unsafe { issue_token(l, kind, out) },
            Err(code) => {
                if registered {
                    l.lock.unregister_waiting_writer();
                }
                if code == SUBETHA_E_TIMEOUT {
                    l.timeouts.fetch_add(1, Ordering::AcqRel);
                }
                code
            }
        }
    })
}

/// Take a hold for reading without waiting, and write a handle naming it
/// into `out`. `SUBETHA_E_WOULD_BLOCK` while a writer holds the lock or
/// is waiting for it, which is what stops a stream of readers starving a
/// writer.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_try_read(handle: subetha_handle, out: *mut u64) -> i32 {
    unsafe { try_acquire(handle, SUBETHA_LOCK_READ, out) }
}

/// Take a hold for writing without waiting, and write a handle naming it
/// into `out`. `SUBETHA_E_WOULD_BLOCK` while anyone holds the lock at
/// all.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_try_write(handle: subetha_handle, out: *mut u64) -> i32 {
    unsafe { try_acquire(handle, SUBETHA_LOCK_WRITE, out) }
}

/// `subetha_rwlock_try_read`, waiting up to `timeout_ms` for the lock;
/// `SUBETHA_WAIT_FOREVER` waits without a deadline.
/// `SUBETHA_E_TIMEOUT` when the wait elapses and `SUBETHA_E_DESTROYED`
/// when the lock's handle is destroyed under it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_read(handle: subetha_handle, timeout_ms: i64, out: *mut u64) -> i32 {
    unsafe { wait_acquire(handle, SUBETHA_LOCK_READ, timeout_ms, out) }
}

/// `subetha_rwlock_try_write`, waiting up to `timeout_ms` for the lock.
/// The contract is `subetha_rwlock_read`'s, and for as long as this waits
/// it counts in `waiting_writers`, so readers arriving after it are
/// refused and it takes the lock ahead of them.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_write(handle: subetha_handle, timeout_ms: i64, out: *mut u64) -> i32 {
    unsafe { wait_acquire(handle, SUBETHA_LOCK_WRITE, timeout_ms, out) }
}

/// Whether `token` names a hold taken for reading or for writing:
/// `SUBETHA_LOCK_READ` or `SUBETHA_LOCK_WRITE`. A token that names no
/// live hold is `SUBETHA_E_INVALID_ARGUMENT`, so this also answers
/// whether a token is still good.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_hold_kind(handle: subetha_handle, token: u64, out: *mut u32) -> i32 {
    with_rwlock(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let reading = token & READ_TOKEN != 0;
        let table = if reading { &l.reads } else { &l.writes };
        match table.kind(token & !READ_TOKEN) {
            Ok(kind) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = kind };
                SUBETHA_OK
            }
            Err(ReleaseError::NotAToken) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "that is a handle, not a hold token")
            }
            Err(ReleaseError::NotHeld) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "the token names no live hold on this lock")
            }
            Err(ReleaseError::Stale) => fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "the token is from a hold that ended, and its slot has been taken since",
            ),
            Err(ReleaseError::NoSuchSlot) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "the token names no slot on this lock")
            }
        }
    })
}

/// A snapshot of the lock into `out`. Every count races an acquire or a
/// release in some process, so it is a diagnostic rather than something
/// to branch on.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_read_stats(handle: subetha_handle, out: *mut subetha_rwlock_stats) -> i32 {
    with_rwlock(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = l.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Give the lock back and close this hold's handle, which names nothing
/// afterwards. Another process waiting on the lock is free to take it as
/// soon as this returns.
///
/// This is the call to use to unlock. `subetha_handle_destroy` on a hold
/// does the same thing to the caller's eye, but pays a process-wide
/// barrier for this one handle where this call pays one for a batch of
/// them - about 331 ns against about 5 ns on an idle process, and
/// 5.90 us against the same 5 ns once other threads are running. A lock
/// taken and released in a loop should not be paying that.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rwlock_unlock(handle: subetha_handle, token: u64) -> i32 {
    with_rwlock(handle, |l| {
        let reading = token & READ_TOKEN != 0;
        let table = if reading { &l.reads } else { &l.writes };
        match table.release(token & !READ_TOKEN) {
            Ok(kind) => {
                give_back(&l.lock, kind);
                SUBETHA_OK
            }
            Err(ReleaseError::NotHeld) => fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "the token names no hold on this lock: it was given back already, or never taken",
            ),
            Err(ReleaseError::Stale) => fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "the token is from a hold that ended, and its slot has been taken since",
            ),
            Err(ReleaseError::NoSuchSlot) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "the token names no slot on this lock")
            }
            Err(ReleaseError::NotAToken) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "that is a handle, not a hold token")
            }
        }
    })
}

/// Write the lock's page to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rwlock_flush(handle: subetha_handle) -> i32 {
    with_rwlock(handle, |l| match l.lock.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => rwlock_code(e),
    })
}

/// Remove the lock's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rwlock_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => PathBuf::from(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        found.remove(path);
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-rwlock-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the lock's file was removed: {:?}", found.first_failure);
        }
    }

    fn object(path: &Path) -> RwLockObject {
        RwLockObject {
            lock: Arc::new(SharedRWLock::create(path).unwrap()),
            mode: SUBETHA_MODE_STRICT,
            waiting: Arc::new(Waiting::new()),
            timeouts: AtomicU64::new(0),
            writes: HoldTable::bounded(1),
            reads: HoldTable::unbounded(),
        }
    }

    /// A hold whose drop gives the lock back, so a test writes one as a
    /// block.
    struct Held<'a> {
        lock: &'a RwLockObject,
        kind: u32,
    }

    impl Drop for Held<'_> {
        fn drop(&mut self) {
            give_back(&self.lock.lock, self.kind);
        }
    }

    /// A hold, or `None` when the lock is held against this caller.
    /// Contention is the answer these tests ask for; any other refusal is
    /// a fault in the fixture and is raised rather than read as "busy".
    fn hold(l: &RwLockObject, kind: u32) -> Option<Held<'_>> {
        match take(&l.lock, kind, false) {
            Ok(()) => Some(Held { lock: l, kind }),
            Err(RWLockError::WouldBlock) => None,
            Err(e) => panic!("the test lock reported {e:?} rather than contention"),
        }
    }

    /// Each hold's lifetime is a block here, so what releases the lock is
    /// the same `give_back` an unlock reaches once its token has been
    /// accepted.
    #[test]
    fn holds_exclude_each_other_and_a_released_hold_clears_the_state() {
        let scratch = Scratch::new("holds");
        let l = object(&scratch.0);
        assert_eq!(l.stats().readers, 0);
        assert!(!l.stats().has_writer);

        {
            let _first = hold(&l, SUBETHA_LOCK_READ).expect("a first reader");
            {
                let _second = hold(&l, SUBETHA_LOCK_READ).expect("a second reader beside it");
                assert_eq!(l.stats().readers, 2);
                assert!(hold(&l, SUBETHA_LOCK_WRITE).is_none(), "a writer waits out the readers");
            }
            assert_eq!(l.stats().readers, 1, "the second reader's release was seen");
            assert!(hold(&l, SUBETHA_LOCK_WRITE).is_none(), "one reader is still enough to hold it");
        }
        assert_eq!(l.stats().readers, 0, "the first reader's release was seen");

        {
            let writer = hold(&l, SUBETHA_LOCK_WRITE).expect("a writer once the readers are gone");
            assert!(l.stats().has_writer);
            assert_eq!(writer.kind, SUBETHA_LOCK_WRITE);
            assert_eq!(l.writes.capacity(), 1, "the lock allows one writer, so the table offers one slot");
            assert!(hold(&l, SUBETHA_LOCK_READ).is_none(), "a reader waits out the writer");
            assert!(hold(&l, SUBETHA_LOCK_WRITE).is_none(), "and so does another writer");
        }
        assert!(!l.stats().has_writer, "the release cleared the bit");
        assert!(hold(&l, SUBETHA_LOCK_READ).is_some(), "the lock is open again");
    }

    /// A waiting writer holds new readers off for as long as it waits and
    /// takes its registration back with it when it stops, which is what
    /// stops a stream of readers starving it.
    #[test]
    fn a_waiting_writer_registers_and_readers_are_refused_until_it_stops() {
        let scratch = Scratch::new("priority");
        let l = object(&scratch.0);
        let reader = hold(&l, SUBETHA_LOCK_READ).expect("a reader holds the lock");
        assert!(hold(&l, SUBETHA_LOCK_READ).is_some(), "another reader joins it freely");

        l.lock.register_waiting_writer();
        assert_eq!(l.stats().waiting_writers, 1);
        assert!(hold(&l, SUBETHA_LOCK_READ).is_none(), "a new reader is refused while a writer waits");
        assert_eq!(
            take(&l.lock, SUBETHA_LOCK_WRITE, true).unwrap_err(),
            RWLockError::WouldBlock,
            "and the writer waits out the reader already holding it",
        );

        // The registered claim clears the registration as it takes the
        // lock, so the count is back to zero with the writer holding.
        {
            let _releases_here = reader;
        }
        take(&l.lock, SUBETHA_LOCK_WRITE, true).expect("the writer claims once the reader is gone");
        assert_eq!(l.stats().waiting_writers, 0, "the claim cleared the registration");
        assert!(l.stats().has_writer);
        l.lock.release_write_for_blocking();
        assert!(hold(&l, SUBETHA_LOCK_READ).is_some(), "readers are let in again");
    }

    #[test]
    fn a_wait_ends_on_its_deadline_and_a_destroy_releases_an_unbounded_one() {
        let scratch = Scratch::new("wait");
        let l = object(&scratch.0);
        let _held = hold(&l, SUBETHA_LOCK_WRITE).expect("a writer");

        // A bounded wait against a held lock gives up rather than hanging.
        let deadline = Some(std::time::Instant::now() + std::time::Duration::from_millis(20));
        let waited = poll_until(&l.waiting, deadline, SUBETHA_E_WOULD_BLOCK, || {
            take(&l.lock, SUBETHA_LOCK_READ, false).map_err(rwlock_code)
        });
        assert_eq!(waited.unwrap_err(), SUBETHA_E_TIMEOUT);
        assert!(l.stats().has_writer, "the wait left the hold alone");

        // An unbounded wait is released by the destroy's interrupt, which
        // is what keeps a destroy from being held up by it.
        let waiting = Arc::clone(&l.waiting);
        let lock = Arc::clone(&l.lock);
        let waiter = std::thread::spawn(move || {
            poll_until(&waiting, None, SUBETHA_E_WOULD_BLOCK, || {
                take(&lock, SUBETHA_LOCK_READ, false).map_err(rwlock_code)
            })
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        l.interrupt();
        assert_eq!(waiter.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }
}
