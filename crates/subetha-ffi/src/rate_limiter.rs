//! The shared rate limiter through the C ABI: a token bucket in a file
//! that any number of processes draw from.
//!
//! The bucket holds `capacity` tokens and refills at `refill_rate_per_sec`
//! toward that ceiling, so the capacity is the burst a caller may take at
//! once and the rate is the steady one. A request larger than the
//! capacity can never be met and is refused rather than waited on. The
//! limiter runs no background work, so strict and managed modes are the
//! same: a waiting acquire spins on the calling thread.

use std::ffi::c_char;
use std::path::Path;
use std::time::Duration;

use subetha_cxc::shared_rate_limiter::{RateLimiterError, SharedRateLimiter};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_TIMEOUT, SUBETHA_E_WOULD_BLOCK, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_RATE_LIMITER};
use crate::ring::text;
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared rate limiter.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_rate_limiter_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Tokens the bucket holds when full, which is the largest burst.
    pub capacity: u32,
    /// Tokens added per second, toward the capacity.
    pub refill_rate_per_sec: u32,
    /// Tokens available right now.
    pub available: u32,
}

pub(crate) struct RateLimiterObject {
    limiter: SharedRateLimiter,
    mode: u32,
}

impl RateLimiterObject {
    /// A waiting acquire spins on its own thread rather than parking, so
    /// a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: RateLimiterError) -> i32 {
    match e {
        RateLimiterError::InsufficientTokens { available, requested } => fail(
            SUBETHA_E_WOULD_BLOCK,
            format!("{requested} token(s) requested with {available} available"),
        ),
        RateLimiterError::Timeout => SUBETHA_E_TIMEOUT,
        RateLimiterError::InvalidConfig => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "capacity and refill_rate_per_sec must both be above zero",
        ),
        RateLimiterError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the limiter on disk was built with another capacity or rate",
        ),
        RateLimiterError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_limiter(handle: subetha_handle, f: impl FnOnce(&RateLimiterObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_RATE_LIMITER, |object| match object {
        Object::RateLimiter(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a rate limiter"),
    })
}

/// The arguments both constructors read, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u32,
    refill_rate_per_sec: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, u32, u32, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    if refill_rate_per_sec == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "refill_rate_per_sec is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, refill_rate_per_sec, mode))
}

/// Obtain the limiter at `path` holding `capacity` tokens and refilling
/// at `refill_rate_per_sec`: a full bucket is initialized when the file
/// does not exist, an existing one is attached with its live token count
/// in place. A limiter built with another capacity or rate is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rate_limiter_create(
    path: *const c_char,
    capacity: u32,
    refill_rate_per_sec: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, rate, mode) =
            match unsafe { read_arguments(path, capacity, refill_rate_per_sec, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedRateLimiter::create(path, capacity, rate) {
            Ok(limiter) => unsafe {
                issue(Object::RateLimiter(RateLimiterObject { limiter, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the limiter another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rate_limiter_open(
    path: *const c_char,
    capacity: u32,
    refill_rate_per_sec: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, rate, mode) =
            match unsafe { read_arguments(path, capacity, refill_rate_per_sec, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedRateLimiter::open(path, capacity, rate) {
            Ok(limiter) => unsafe {
                issue(Object::RateLimiter(RateLimiterObject { limiter, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Take `n` tokens if they are there, and refuse with
/// `SUBETHA_E_WOULD_BLOCK` if they are not. A request above the capacity
/// is refused the same way, since no amount of waiting would meet it.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rate_limiter_try_acquire(handle: subetha_handle, n: u32) -> i32 {
    with_limiter(handle, |l| match l.limiter.try_acquire(n) {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Take `n` tokens, waiting up to `timeout_ms` for the bucket to refill
/// enough. `SUBETHA_E_TIMEOUT` when the deadline passes first, and
/// `SUBETHA_E_WOULD_BLOCK` at once for a request above the capacity.
///
/// The wait spins on the calling thread in either mode; the limiter
/// starts no thread of its own.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rate_limiter_acquire_wait(
    handle: subetha_handle,
    n: u32,
    timeout_ms: u64,
) -> i32 {
    with_limiter(handle, |l| {
        match l.limiter.acquire_or_wait(n, Duration::from_millis(timeout_ms)) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// Tokens available right now, into `out`. A snapshot: another process
/// may take them before the caller acts on it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rate_limiter_available(
    handle: subetha_handle,
    out: *mut u32,
) -> i32 {
    with_limiter(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let available = l.limiter.available();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = available };
        SUBETHA_OK
    })
}

/// Refill the bucket to its capacity. Peers sharing the file see the same
/// full bucket.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rate_limiter_reset(handle: subetha_handle) -> i32 {
    with_limiter(handle, |l| {
        l.limiter.reset();
        SUBETHA_OK
    })
}

/// Push the limiter's dirty pages to disk, returning when they are
/// durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rate_limiter_flush(handle: subetha_handle) -> i32 {
    with_limiter(handle, |l| match l.limiter.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the limiter's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_rate_limiter_flush_async(handle: subetha_handle) -> i32 {
    with_limiter(handle, |l| match l.limiter.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the limiter into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_rate_limiter_read_stats(
    handle: subetha_handle,
    out: *mut subetha_rate_limiter_stats,
) -> i32 {
    with_limiter(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_rate_limiter_stats {
            mode: l.mode,
            capacity: l.limiter.capacity(),
            refill_rate_per_sec: l.limiter.refill_rate_per_sec(),
            available: l.limiter.available(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
