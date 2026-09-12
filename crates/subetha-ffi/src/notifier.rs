//! The pollable notifier through the C ABI: a consumer attaches one to an
//! adaptive ring or a blocking SPSC ring and hands its native object to an
//! event loop, and every push through the ABI in any process on the same
//! ring signals it. On Unix the native object is a file descriptor that
//! `poll`, `epoll` and `kqueue` report readable while a signal is
//! pending; on Windows it is a manual-reset event `HANDLE` a wait function
//! returns from while one is pending. A signal stays until it is drained
//! and means "something was pushed since the last drain", not "one item
//! per signal": a consumer drains the notifier, then pops until the ring
//! is empty.

use subetha_cxc::cross_process_notifier::Notifier;

use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_TIMEOUT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_NOTIFIER};
use crate::runtime::with_kind;

pub(crate) struct NotifierObject {
    notifier: Notifier,
}

impl NotifierObject {
    pub(crate) fn new(notifier: Notifier) -> Self {
        Self { notifier }
    }

    /// A notifier parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}
}

fn with_notifier(handle: subetha_handle, f: impl FnOnce(&NotifierObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_NOTIFIER, |object| match object {
        Object::Notifier(n) => f(n),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a notifier"),
    })
}

/// The notifier's native object as an integer: a file descriptor on Unix,
/// an event `HANDLE` on Windows. It belongs to the notifier and is closed
/// when the notifier's handle is destroyed.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_notifier_native(handle: subetha_handle, out: *mut u64) -> i32 {
    with_notifier(handle, |n| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = n.notifier.native() };
        SUBETHA_OK
    })
}

/// Consume every pending signal, so the next poll waits for the next push.
/// Call it before popping the ring to empty; a push that lands between
/// the drain and the last pop signals again and is not lost.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_notifier_drain(handle: subetha_handle) -> i32 {
    with_notifier(handle, |n| {
        n.notifier.drain();
        SUBETHA_OK
    })
}

/// Whether a signal is pending right now, without waiting.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_notifier_is_signaled(handle: subetha_handle, out: *mut bool) -> i32 {
    with_notifier(handle, |n| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = n.notifier.is_signaled() };
        SUBETHA_OK
    })
}

/// Wait up to `timeout_ms` for a signal, `SUBETHA_WAIT_FOREVER` for no
/// deadline; `SUBETHA_E_TIMEOUT` when none arrived. The event loop's own
/// wait on the native object is what a consumer normally uses; this is
/// the same wait for a caller without one.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_notifier_wait(handle: subetha_handle, timeout_ms: i64) -> i32 {
    with_notifier(handle, |n| {
        if timeout_ms < -1 || timeout_ms > i64::from(i32::MAX) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, format!("timeout_ms {timeout_ms} is not between -1 and {}", i32::MAX));
        }
        if n.notifier.wait(timeout_ms as i32) {
            SUBETHA_OK
        } else {
            SUBETHA_E_TIMEOUT
        }
    })
}
