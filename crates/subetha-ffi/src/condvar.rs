//! The condition variable through the C ABI: a way for one process to
//! park until another says something changed.
//!
//! # The predicate stays with the caller
//!
//! The Rust API takes a closure and re-checks it after every wake. A C
//! caller keeps its own predicate instead - in a shared atomic, a cell,
//! or a field of its own - and writes the loop every condvar user
//! already writes:
//!
//! ```c
//! for (;;) {
//!     uint64_t seen = 0;
//!     int32_t rc = subetha_condvar_generation(cv, &seen);
//!     if (rc != SUBETHA_OK) { return rc; }
//!     if (my_predicate_is_true()) { break; }
//!     rc = subetha_condvar_wait(cv, seen, 1000);
//!     if (rc != SUBETHA_OK && rc != SUBETHA_E_TIMEOUT) { return rc; }
//! }
//! ```
//!
//! Reading the generation first, ahead of the predicate check, is what
//! closes the window: a notify that lands between the check and the wait
//! has already moved the generation past `seen`, so the wait returns at
//! once rather than parking on a signal that has been and gone. This is
//! the same shape as a futex's expected-value argument, and it is why
//! `subetha_condvar_wait` takes `from_generation` rather than parking
//! blind.
//!
//! Every notify advances the generation, so a caller with no predicate of
//! its own can use the generation as one: wait for it to move.
//!
//! A destroy of the condvar's handle releases every caller parked on it
//! with `SUBETHA_E_DESTROYED`.
//!
//! Two files carry the state, named from the `path` a caller gives:
//! `<path>.waker.bin` holds the waiter slots and `<path>.gen.bin` the
//! generation. `subetha_condvar_unlink` removes both.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_condvar::{CondvarError, SharedCondvar};

use crate::error::{
    condvar_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_TIMEOUT, SUBETHA_E_WOULD_BLOCK, SUBETHA_E_WRONG_KIND,
    SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_CONDVAR};
use crate::ring::{deadline_from, finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting};

/// A snapshot of a condition variable.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_condvar_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Waiter slots the backing was built with.
    pub max_waiters: u32,
    /// The generation, which every notify advances by one.
    pub generation: u64,
    /// Waits through this handle that gave up when their timeout
    /// elapsed.
    pub timeouts: u64,
    /// Waits through this handle refused because every waiter slot was
    /// parked.
    pub waker_full: u64,
    /// Waiters woken by notifies through this handle.
    pub woken: u64,
}

pub(crate) struct CondvarObject {
    condvar: Arc<SharedCondvar>,
    mode: u32,
    max_waiters: u32,
    waiting: Waiting,
    timeouts: AtomicU64,
    woken: AtomicU64,
}

impl CondvarObject {
    /// Release every caller parked on this condvar so a destroy can
    /// proceed: raise the flag they check and wake them out of the
    /// kernel.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.condvar.waker().wake_all();
    }

    fn stats(&self) -> subetha_condvar_stats {
        subetha_condvar_stats {
            mode: self.mode,
            max_waiters: self.max_waiters,
            generation: self.condvar.generation(),
            timeouts: self.timeouts.load(Ordering::Acquire),
            waker_full: self.waiting.waker_full(),
            woken: self.woken.load(Ordering::Acquire),
        }
    }
}

fn with_condvar(handle: subetha_handle, f: impl FnOnce(&CondvarObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CONDVAR, |object| match object {
        Object::Condvar(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a condition variable"),
    })
}

/// The two files a condvar's state lives in, named from the base `path` a
/// caller gives.
fn backing_files(base: &Path) -> Option<[PathBuf; 2]> {
    let stem = base.file_name()?.to_string_lossy().to_string();
    Some(["waker", "gen"].map(|suffix| {
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
    max_waiters: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if max_waiters == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_waiters is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, max_waiters as usize, mode))
}

fn place(
    condvar: Result<SharedCondvar, CondvarError>,
    mode: u32,
    max_waiters: u32,
    out: *mut subetha_handle,
) -> i32 {
    match condvar {
        Ok(condvar) => {
            let object = CondvarObject {
                condvar: Arc::new(condvar),
                mode,
                max_waiters,
                waiting: Waiting::new(),
                timeouts: AtomicU64::new(0),
                woken: AtomicU64::new(0),
            };
            unsafe { issue(Object::Condvar(object), out) }
        }
        Err(e) => condvar_code(e),
    }
}

/// Create the condition variable at `path` with room for `max_waiters`
/// parked callers at once. Both backing files are initialized, so the
/// generation starts at zero.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_create(
    path: *const c_char,
    max_waiters: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, waiters, mode) = match unsafe { read_arguments(path, max_waiters, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedCondvar::create_with_capacity(path, waiters), mode, max_waiters, out)
    })
}

/// Attach to the condition variable another process created at `path`,
/// with the same `max_waiters` the creator used; the backing files must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_open(
    path: *const c_char,
    max_waiters: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, waiters, mode) = match unsafe { read_arguments(path, max_waiters, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedCondvar::open_with_capacity(path, waiters), mode, max_waiters, out)
    })
}

/// The generation, which every notify from any process advances by one.
/// Read it first, ahead of checking your own predicate, and hand it to
/// `subetha_condvar_wait`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_generation(handle: subetha_handle, out: *mut u64) -> i32 {
    with_condvar(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let generation = c.condvar.generation();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = generation };
        SUBETHA_OK
    })
}

/// Park until the generation moves past `from_generation`, waiting up to
/// `timeout_ms`; `SUBETHA_WAIT_FOREVER` waits without a deadline.
///
/// Returns `SUBETHA_OK` once a notify has advanced the generation,
/// `SUBETHA_E_TIMEOUT` when the wait elapses first,
/// `SUBETHA_E_DESTROYED` when the condvar's handle is destroyed under it,
/// and `SUBETHA_E_RING_WAKER_FULL` when every waiter slot is parked. A
/// caller passing a `from_generation` the condvar has already moved past
/// returns at once with `SUBETHA_OK`, which is what makes reading the
/// generation ahead of the predicate close the lost-notify window.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_condvar_wait(handle: subetha_handle, from_generation: u64, timeout_ms: i64) -> i32 {
    with_condvar(handle, |c| {
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        let waited = wait_until(&c.waiting, c.condvar.waker(), deadline, SUBETHA_E_WOULD_BLOCK, || {
            if c.condvar.generation() > from_generation {
                Ok(())
            } else {
                Err(SUBETHA_E_WOULD_BLOCK)
            }
        });
        match waited {
            Ok(()) => SUBETHA_OK,
            Err(code) => {
                if code == SUBETHA_E_TIMEOUT {
                    c.timeouts.fetch_add(1, Ordering::AcqRel);
                }
                code
            }
        }
    })
}

/// Advance the generation and wake at most one parked caller, writing how
/// many were woken into `out_woken`. Change what the waiters are watching
/// before calling, since a woken caller re-checks its own predicate.
///
/// # Safety
/// `out_woken` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_notify_one(handle: subetha_handle, out_woken: *mut u32) -> i32 {
    with_condvar(handle, |c| {
        let woken = c.condvar.notify_one();
        c.woken.fetch_add(woken as u64, Ordering::AcqRel);
        if !out_woken.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_woken = woken as u32 };
        }
        SUBETHA_OK
    })
}

/// Advance the generation and wake every parked caller, writing how many
/// were woken into `out_woken`. The contract is
/// `subetha_condvar_notify_one`'s.
///
/// # Safety
/// `out_woken` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_notify_all(handle: subetha_handle, out_woken: *mut u32) -> i32 {
    with_condvar(handle, |c| {
        let woken = c.condvar.notify_all();
        c.woken.fetch_add(woken as u64, Ordering::AcqRel);
        if !out_woken.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_woken = woken as u32 };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the condition variable into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_read_stats(handle: subetha_handle, out: *mut subetha_condvar_stats) -> i32 {
    with_condvar(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = c.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove the condition variable's two files at `path`:
/// `<path>.waker.bin` and `<path>.gen.bin`. The contract is
/// `subetha_ring_unlink`'s, and the report counts both.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_condvar_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-condvar-{name}-{}", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            for file in backing_files(&self.0).expect("the scratch path names a file") {
                found.remove(file);
            }
            assert_eq!(found.failed, 0, "the condvar's files were removed: {:?}", found.first_failure);
        }
    }

    fn object(path: &Path) -> Arc<CondvarObject> {
        Arc::new(CondvarObject {
            condvar: Arc::new(SharedCondvar::create_with_capacity(path, 8).unwrap()),
            mode: SUBETHA_MODE_STRICT,
            max_waiters: 8,
            waiting: Waiting::new(),
            timeouts: AtomicU64::new(0),
            woken: AtomicU64::new(0),
        })
    }

    /// The wait a `subetha_condvar_wait` call runs, without the handle
    /// table in the way.
    fn wait_from(c: &CondvarObject, from: u64, deadline: Option<std::time::Instant>) -> Result<(), i32> {
        wait_until(&c.waiting, c.condvar.waker(), deadline, SUBETHA_E_WOULD_BLOCK, || {
            if c.condvar.generation() > from { Ok(()) } else { Err(SUBETHA_E_WOULD_BLOCK) }
        })
    }

    #[test]
    fn a_notify_advances_the_generation_and_releases_a_parked_caller() {
        let scratch = Scratch::new("notify");
        let c = object(&scratch.0);
        assert_eq!(c.stats().generation, 0);
        assert_eq!(c.stats().max_waiters, 8);

        // Nothing is parked, so a notify wakes nobody but still moves the
        // generation, which is what a later waiter compares against.
        assert_eq!(c.condvar.notify_all(), 0, "no caller is parked");
        assert_eq!(c.stats().generation, 1);

        // A wait from a generation already passed returns at once. This
        // is the lost-notify window closing: the caller read 0, the
        // notify above moved it to 1, and the wait does not park.
        wait_from(&c, 0, None).expect("a wait from a generation already passed does not park");

        // A caller parked from the current generation is released by a
        // notify from another thread.
        let parked = {
            let c = Arc::clone(&c);
            std::thread::spawn(move || wait_from(&c, 1, None))
        };
        std::thread::sleep(std::time::Duration::from_millis(20));
        let woken = c.condvar.notify_all();
        parked.join().unwrap().expect("the notify released the parked caller");
        assert!(woken <= 1, "at most the one parked caller was woken");
        assert_eq!(c.stats().generation, 2);
    }

    #[test]
    fn a_wait_ends_on_its_deadline_and_a_destroy_releases_an_unbounded_one() {
        let scratch = Scratch::new("wait");
        let c = object(&scratch.0);
        let now = c.condvar.generation();

        let deadline = Some(std::time::Instant::now() + std::time::Duration::from_millis(20));
        assert_eq!(wait_from(&c, now, deadline).unwrap_err(), SUBETHA_E_TIMEOUT);
        assert_eq!(c.stats().generation, now, "a timeout moves nothing");

        // An unbounded wait is released by the destroy's interrupt, which
        // raises the flag and wakes the caller out of the kernel.
        let parked = {
            let c = Arc::clone(&c);
            std::thread::spawn(move || wait_from(&c, now, None))
        };
        std::thread::sleep(std::time::Duration::from_millis(20));
        c.interrupt();
        assert_eq!(parked.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }
}
