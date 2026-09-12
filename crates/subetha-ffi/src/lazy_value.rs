//! A value produced once across every process that asks for it, through
//! the C ABI.
//!
//! Several processes starting at once each want the same configuration,
//! and each fetching it independently is N requests against whatever
//! serves it. Here one of them produces the value and the rest read what
//! it produced.
//!
//! # The three steps
//!
//! `claim` hands the right to produce the value to one caller and refuses
//! every other. The winner produces it however it likes - a network call,
//! a file, a computation - and calls `publish`. The rest call `wait`,
//! which returns as soon as the value is there.
//!
//! `try_get` reads a published value without claiming and without
//! blocking, which is the path every caller after the first takes.
//!
//! # A claim spans the caller's own code
//!
//! A claim stands from `claim` until `publish`, across whatever the
//! caller does in between, so a claimant can die holding one. The claim
//! carries the process that took it: `wait` answers
//! `SUBETHA_E_LAZY_CLAIMANT_GONE` once that process is gone, and
//! `subetha_lazy_reclaim` returns the value to unclaimed for the next
//! caller. A claim held by a live process is left alone, however slow it
//! is being.
//!
//! `publish` takes the claiming process id, so a caller that never won
//! the claim cannot overwrite what the winner produced.
//!
//! The value runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_once_cell::{
    SharedOnceCellDyn, SharedOnceError, WaitError, ONCE_PAYLOAD_BYTES, STATE_EMPTY,
    STATE_INITIALIZED, STATE_INITIALIZING,
};

use crate::error::{
    fail, once_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_LAZY_CLAIMANT_GONE,
    SUBETHA_E_LAZY_NOT_CLAIMED, SUBETHA_E_TIMEOUT, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LAZY_VALUE};
use crate::ring::{deadline_from, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// Bytes a lazy value can hold.
pub const SUBETHA_LAZY_MAX_BYTES: usize = ONCE_PAYLOAD_BYTES;

/// Nobody has claimed the value.
pub const SUBETHA_LAZY_UNCLAIMED: u32 = STATE_EMPTY as u32;
/// A caller holds the claim and has not published yet.
pub const SUBETHA_LAZY_CLAIMED: u32 = STATE_INITIALIZING as u32;
/// The value is published.
pub const SUBETHA_LAZY_PUBLISHED: u32 = STATE_INITIALIZED as u32;

/// A snapshot of a lazy value.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_lazy_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of the `SUBETHA_LAZY_` state constants.
    pub state: u32,
    /// Bytes the value holds.
    pub value_bytes: u64,
}

pub(crate) struct LazyValueObject {
    cell: SharedOnceCellDyn,
    mode: u32,
}

impl LazyValueObject {
    /// A lazy value parks nothing inside a call except a caller's own
    /// bounded wait, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_lazy_stats {
        subetha_lazy_stats {
            mode: self.mode,
            state: self.cell.state() as u32,
            value_bytes: self.cell.value_len() as u64,
        }
    }
}

fn with_lazy(handle: subetha_handle, f: impl FnOnce(&LazyValueObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LAZY_VALUE, |object| match object {
        Object::LazyValue(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a lazy value"),
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    value_bytes: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if value_bytes == 0 || value_bytes > SUBETHA_LAZY_MAX_BYTES {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("value_bytes is {value_bytes}, and a lazy value holds 1 to {SUBETHA_LAZY_MAX_BYTES}"),
        ));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, value_bytes, mode))
}

fn place(
    cell: Result<SharedOnceCellDyn, SharedOnceError>,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    match cell {
        Ok(cell) => unsafe { issue(Object::LazyValue(LazyValueObject { cell, mode }), out) },
        Err(e) => once_code(e),
    }
}

/// Obtain the lazy value at `path`, holding `value_bytes`: an unclaimed
/// one is initialized when the file does not exist, and an existing one
/// is attached in whatever state it stands.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_create(
    path: *const c_char,
    value_bytes: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, value_bytes, mode) = match unsafe { read_arguments(path, value_bytes, mode, out) }
        {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedOnceCellDyn::create(path, value_bytes), mode, out)
    })
}

/// Attach to the lazy value another process created at `path`, whose
/// value must be `value_bytes` long; the file must exist.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_open(
    path: *const c_char,
    value_bytes: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, value_bytes, mode) = match unsafe { read_arguments(path, value_bytes, mode, out) }
        {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedOnceCellDyn::open(path, value_bytes), mode, out)
    })
}

/// Take the right to produce the value, stamping the claim with `pid`,
/// and write whether this caller won into `out_won`.
///
/// One caller wins. Every other is told it did not, whether because
/// another claim stands or because the value is already published, and
/// reads the value with `subetha_lazy_wait` or `subetha_lazy_try_get`.
///
/// # Safety
/// `out_won` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_claim(handle: subetha_handle, pid: u32, out_won: *mut bool) -> i32 {
    with_lazy(handle, |l| {
        if out_won.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_won is null");
        }
        if pid == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "pid is zero, which names no process");
        }
        let won = l.cell.claim(pid);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_won = won };
        SUBETHA_OK
    })
}

/// Publish the value produced under the claim `pid` holds, and release
/// the claim. `SUBETHA_E_LAZY_NOT_CLAIMED` when that claim is not the one
/// standing, so a caller that never won cannot overwrite the winner's
/// value.
///
/// # Safety
/// `value` addresses `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_publish(
    handle: subetha_handle,
    pid: u32,
    value: *const u8,
    len: usize,
) -> i32 {
    with_lazy(handle, |l| {
        if value.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "value is null");
        }
        if len != l.cell.value_len() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("len is {len} and the value holds {}", l.cell.value_len()),
            );
        }
        // SAFETY: checked non-null; the caller guarantees it addresses len
        // readable bytes.
        let bytes = unsafe { std::slice::from_raw_parts(value, len) };
        if l.cell.publish(pid, bytes) {
            SUBETHA_OK
        } else {
            fail(
                SUBETHA_E_LAZY_NOT_CLAIMED,
                format!("process {pid} does not hold the claim on this value"),
            )
        }
    })
}

/// The published value into `out`, or `SUBETHA_E_LAZY_NOT_CLAIMED` while
/// none is published. Never blocks and never claims.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_try_get(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_lazy(handle, |l| {
        let want = l.cell.value_len();
        let buf = match unsafe { out_buffer(out, cap, out_len, want) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        if l.cell.try_get(&mut buf[..want]) {
            // SAFETY: out_buffer checked it; the caller guarantees it is
            // writable.
            unsafe { *out_len = want };
            SUBETHA_OK
        } else {
            fail(SUBETHA_E_LAZY_NOT_CLAIMED, "no value is published yet")
        }
    })
}

/// Wait for the published value into `out`, giving up after
/// `timeout_ms`.
///
/// `SUBETHA_E_TIMEOUT` when the deadline passes with a live claim still
/// outstanding, and `SUBETHA_E_LAZY_CLAIMANT_GONE` as soon as the claim
/// belongs to a process that has died, since no deadline is long enough
/// for a publish that will never come.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_wait(
    handle: subetha_handle,
    timeout_ms: i64,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_lazy(handle, |l| {
        let want = l.cell.value_len();
        let buf = match unsafe { out_buffer(out, cap, out_len, want) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(Some(d)) => d,
            Ok(None) => Instant::now() + Duration::from_secs(86_400),
            Err(code) => return code,
        };
        match l.cell.wait(&mut buf[..want], deadline) {
            Ok(()) => {
                // SAFETY: out_buffer checked it; the caller guarantees it
                // is writable.
                unsafe { *out_len = want };
                SUBETHA_OK
            }
            Err(WaitError::TimedOut) => SUBETHA_E_TIMEOUT,
            Err(WaitError::ClaimantGone) => fail(
                SUBETHA_E_LAZY_CLAIMANT_GONE,
                "the process holding the claim is gone; reclaim it and claim again",
            ),
        }
    })
}

/// Free a claim whose process is gone, and write whether one was freed
/// into `out_freed`. A claim held by a live process is left alone.
///
/// # Safety
/// `out_freed` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_reclaim(handle: subetha_handle, out_freed: *mut bool) -> i32 {
    with_lazy(handle, |l| {
        let freed = l.cell.reclaim();
        if !out_freed.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is
            // writable.
            unsafe { *out_freed = freed };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the value into `out`. The state races every claim and
/// publish, so it describes a moment that has already passed.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_read_stats(handle: subetha_handle, out: *mut subetha_lazy_stats) -> i32 {
    with_lazy(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = l.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Flush the value to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_lazy_flush(handle: subetha_handle) -> i32 {
    with_lazy(handle, |l| match l.cell.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => once_code(e),
    })
}

/// Remove the value's file. On Windows the file must not be mapped by any
/// handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lazy_unlink(path: *const c_char, out: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => PathBuf::from(p),
            Err(code) => return code,
        };
        let mut report = UnlinkReport::default();
        report.remove(path);
        unsafe { finish_unlink(report, out) }
    })
}
