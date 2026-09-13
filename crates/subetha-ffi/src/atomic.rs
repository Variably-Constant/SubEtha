//! The shared atomics through the C ABI: a 32-bit, 64-bit or boolean
//! counter or flag in a file that every process mapping it operates on
//! atomically, because cache coherence gives the hardware atomics their
//! semantics across address spaces.
//!
//! Every operation comes in two forms, as `<stdatomic.h>` shapes them:
//! the plain call is sequentially consistent, and the `_explicit` call
//! takes a `SUBETHA_ORDER_` value. A compare-exchange takes two, one for
//! the exchange and one for the load that fails. The atomics run no
//! background work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_atomic::{SharedAtomicBool, SharedAtomicError, SharedAtomicU32, SharedAtomicU64};

use crate::error::{atomic_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_ATOMIC};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// No ordering constraint beyond the operation's own atomicity.
pub const SUBETHA_ORDER_RELAXED: u32 = 0;
/// Reads that follow this one see everything the releasing write saw.
pub const SUBETHA_ORDER_ACQUIRE: u32 = 1;
/// Writes made before this one are visible to an acquiring reader.
pub const SUBETHA_ORDER_RELEASE: u32 = 2;
/// Acquire and release together, for a read-modify-write.
pub const SUBETHA_ORDER_ACQ_REL: u32 = 3;
/// Acquire and release, and a single total order over every such
/// operation. What the plain calls use.
pub const SUBETHA_ORDER_SEQ_CST: u32 = 4;

/// The atomic holds a `uint32_t`.
pub const SUBETHA_ATOMIC_U32: u32 = 0;
/// The atomic holds a `uint64_t`.
pub const SUBETHA_ATOMIC_U64: u32 = 1;
/// The atomic holds a `bool`.
pub const SUBETHA_ATOMIC_BOOL: u32 = 2;

/// A snapshot of a shared atomic.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_atomic_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of the `SUBETHA_ATOMIC_` constants.
    pub width: u32,
    /// Bytes the value occupies: 4, 8, or 1 for the boolean.
    pub bytes: u32,
}

enum Atomic {
    U32(SharedAtomicU32),
    U64(SharedAtomicU64),
    Bool(SharedAtomicBool),
}

pub(crate) struct AtomicObject {
    atomic: Atomic,
    mode: u32,
}

impl AtomicObject {
    /// An atomic parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_atomic_stats {
        let (width, bytes) = match self.atomic {
            Atomic::U32(_) => (SUBETHA_ATOMIC_U32, 4),
            Atomic::U64(_) => (SUBETHA_ATOMIC_U64, 8),
            Atomic::Bool(_) => (SUBETHA_ATOMIC_BOOL, 1),
        };
        subetha_atomic_stats { mode: self.mode, width, bytes }
    }

    fn flush(&self) -> Result<(), SharedAtomicError> {
        match &self.atomic {
            Atomic::U32(a) => a.flush(),
            Atomic::U64(a) => a.flush(),
            Atomic::Bool(a) => a.flush(),
        }
    }
}

fn with_atomic(handle: subetha_handle, f: impl FnOnce(&AtomicObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_ATOMIC, |object| match object {
        Object::Atomic(a) => f(a),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an atomic"),
    })
}

/// The 32-bit atomic a handle names, for the `u32` entry points.
fn with_u32(handle: subetha_handle, f: impl FnOnce(&SharedAtomicU32) -> i32) -> i32 {
    with_atomic(handle, |object| match &object.atomic {
        Atomic::U32(a) => f(a),
        _ => fail(SUBETHA_E_WRONG_KIND, "the atomic is not 32 bits wide"),
    })
}

/// The 64-bit atomic a handle names, for the `u64` entry points.
fn with_u64(handle: subetha_handle, f: impl FnOnce(&SharedAtomicU64) -> i32) -> i32 {
    with_atomic(handle, |object| match &object.atomic {
        Atomic::U64(a) => f(a),
        _ => fail(SUBETHA_E_WRONG_KIND, "the atomic is not 64 bits wide"),
    })
}

/// Reach the 64-bit atomic a handle names and return at once, so a bench
/// can price this family's dispatch apart from the borrow beneath it and
/// the load above it. Present only with the `test-hooks` feature.
#[cfg(feature = "test-hooks")]
#[unsafe(no_mangle)]
pub extern "C" fn subetha_test_atomic_borrow_only(handle: subetha_handle) -> i32 {
    with_u64(handle, |_atomic| SUBETHA_OK)
}

/// The boolean atomic a handle names, for the `bool` entry points.
fn with_bool(handle: subetha_handle, f: impl FnOnce(&SharedAtomicBool) -> i32) -> i32 {
    with_atomic(handle, |object| match &object.atomic {
        Atomic::Bool(a) => f(a),
        _ => fail(SUBETHA_E_WRONG_KIND, "the atomic is not a boolean"),
    })
}

/// The ordering a `SUBETHA_ORDER_` value names. Every read-modify-write
/// takes any of them.
fn order(value: u32) -> Result<Ordering, i32> {
    match value {
        SUBETHA_ORDER_RELAXED => Ok(Ordering::Relaxed),
        SUBETHA_ORDER_ACQUIRE => Ok(Ordering::Acquire),
        SUBETHA_ORDER_RELEASE => Ok(Ordering::Release),
        SUBETHA_ORDER_ACQ_REL => Ok(Ordering::AcqRel),
        SUBETHA_ORDER_SEQ_CST => Ok(Ordering::SeqCst),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("order {other} names no memory ordering"))),
    }
}

/// The ordering a load takes: a store-only ordering has no meaning for a
/// read, and the standard library panics rather than reporting it.
fn load_order(value: u32) -> Result<Ordering, i32> {
    match order(value)? {
        Ordering::Release => Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a load cannot be release")),
        Ordering::AcqRel => Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a load cannot be acquire-release")),
        ord => Ok(ord),
    }
}

/// The ordering a store takes.
fn store_order(value: u32) -> Result<Ordering, i32> {
    match order(value)? {
        Ordering::Acquire => Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a store cannot be acquire")),
        Ordering::AcqRel => Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a store cannot be acquire-release")),
        ord => Ok(ord),
    }
}

/// Write `prev` through `out_prev` when the caller asked for it.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
unsafe fn put_u32(out_prev: *mut u32, prev: u32) -> i32 {
    if !out_prev.is_null() {
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_prev = prev };
    }
    SUBETHA_OK
}

/// Write `prev` through `out_prev` when the caller asked for it.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
unsafe fn put_u64(out_prev: *mut u64, prev: u64) -> i32 {
    if !out_prev.is_null() {
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_prev = prev };
    }
    SUBETHA_OK
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

fn place(atomic: Result<Atomic, SharedAtomicError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match atomic {
        Ok(atomic) => unsafe { issue(Object::Atomic(AtomicObject { atomic, mode }), out) },
        Err(e) => atomic_code(e),
    }
}

/// Obtain the 32-bit atomic at `path`, initialized to `init` when the file
/// does not exist and attached with its live value when it does, so a
/// racing peer never resets a counter another process is already using. A
/// file built for another width is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_create(path: *const c_char, init: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicU32::create(path, init).map(Atomic::U32), mode, out)
    })
}

/// Truncate the file at `path` and initialize a 32-bit atomic holding
/// `init`, discarding the value other handles share.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_reset(path: *const c_char, init: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicU32::reset(path, init).map(Atomic::U32), mode, out)
    })
}

/// Attach to the 32-bit atomic another process created at `path`; the file
/// must exist.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_open(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicU32::open(path).map(Atomic::U32), mode, out)
    })
}

/// Obtain the 64-bit atomic at `path`. The contract is
/// `subetha_atomic_u32_create`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_create(path: *const c_char, init: u64, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicU64::create(path, init).map(Atomic::U64), mode, out)
    })
}

/// Truncate the file at `path` and initialize a 64-bit atomic holding
/// `init`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_reset(path: *const c_char, init: u64, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicU64::reset(path, init).map(Atomic::U64), mode, out)
    })
}

/// Attach to the 64-bit atomic another process created at `path`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_open(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicU64::open(path).map(Atomic::U64), mode, out)
    })
}

/// Obtain the boolean atomic at `path`. The contract is
/// `subetha_atomic_u32_create`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_create(path: *const c_char, init: bool, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicBool::create(path, init).map(Atomic::Bool), mode, out)
    })
}

/// Truncate the file at `path` and initialize a boolean atomic holding
/// `init`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_reset(path: *const c_char, init: bool, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicBool::reset(path, init).map(Atomic::Bool), mode, out)
    })
}

/// Attach to the boolean atomic another process created at `path`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_open(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedAtomicBool::open(path).map(Atomic::Bool), mode, out)
    })
}

/// Read the 32-bit value into `out`, sequentially consistent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_load(handle: subetha_handle, out: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_load_explicit(handle, SUBETHA_ORDER_SEQ_CST, out) }
}

/// Read the 32-bit value into `out` under `order`, which is relaxed,
/// acquire or sequentially consistent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_load_explicit(handle: subetha_handle, order: u32, out: *mut u32) -> i32 {
    with_u32(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let ord = match load_order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = a.load(ord) };
        SUBETHA_OK
    })
}

/// Write the 32-bit value, sequentially consistent.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_u32_store(handle: subetha_handle, value: u32) -> i32 {
    subetha_atomic_u32_store_explicit(handle, value, SUBETHA_ORDER_SEQ_CST)
}

/// Write the 32-bit value under `order`, which is relaxed, release or
/// sequentially consistent.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_u32_store_explicit(handle: subetha_handle, value: u32, order: u32) -> i32 {
    with_u32(handle, |a| {
        let ord = match store_order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        a.store(value, ord);
        SUBETHA_OK
    })
}

/// Replace the 32-bit value, the previous one into `out_prev` when that is
/// not null, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_swap(handle: subetha_handle, value: u32, out_prev: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_swap_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Replace the 32-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_swap_explicit(handle: subetha_handle, value: u32, order: u32, out_prev: *mut u32) -> i32 {
    with_u32(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u32(out_prev, a.swap(value, ord)) }
    })
}

/// Add to the 32-bit value, wrapping, the previous one into `out_prev`
/// when that is not null, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_add(handle: subetha_handle, value: u32, out_prev: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_fetch_add_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Add to the 32-bit value, wrapping, under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_add_explicit(
    handle: subetha_handle,
    value: u32,
    order: u32,
    out_prev: *mut u32,
) -> i32 {
    with_u32(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u32(out_prev, a.fetch_add(value, ord)) }
    })
}

/// Subtract from the 32-bit value, wrapping, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_sub(handle: subetha_handle, value: u32, out_prev: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_fetch_sub_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Subtract from the 32-bit value, wrapping, under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_sub_explicit(
    handle: subetha_handle,
    value: u32,
    order: u32,
    out_prev: *mut u32,
) -> i32 {
    with_u32(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u32(out_prev, a.fetch_sub(value, ord)) }
    })
}

/// Bitwise-and the 32-bit value, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_and(handle: subetha_handle, value: u32, out_prev: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_fetch_and_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Bitwise-and the 32-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_and_explicit(
    handle: subetha_handle,
    value: u32,
    order: u32,
    out_prev: *mut u32,
) -> i32 {
    with_u32(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u32(out_prev, a.fetch_and(value, ord)) }
    })
}

/// Bitwise-or the 32-bit value, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_or(handle: subetha_handle, value: u32, out_prev: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_fetch_or_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Bitwise-or the 32-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_or_explicit(
    handle: subetha_handle,
    value: u32,
    order: u32,
    out_prev: *mut u32,
) -> i32 {
    with_u32(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u32(out_prev, a.fetch_or(value, ord)) }
    })
}

/// Bitwise-exclusive-or the 32-bit value, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_xor(handle: subetha_handle, value: u32, out_prev: *mut u32) -> i32 {
    unsafe { subetha_atomic_u32_fetch_xor_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Bitwise-exclusive-or the 32-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_fetch_xor_explicit(
    handle: subetha_handle,
    value: u32,
    order: u32,
    out_prev: *mut u32,
) -> i32 {
    with_u32(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u32(out_prev, a.fetch_xor(value, ord)) }
    })
}

/// Replace the 32-bit value with `new` only if it is `expected`.
/// `out_swapped` receives whether it was, and `out_current`, when not
/// null, the value found. Sequentially consistent.
///
/// # Safety
/// `out_swapped` is a valid pointer; `out_current` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_compare_exchange(
    handle: subetha_handle,
    expected: u32,
    new: u32,
    out_current: *mut u32,
    out_swapped: *mut bool,
) -> i32 {
    unsafe {
        subetha_atomic_u32_compare_exchange_explicit(
            handle,
            expected,
            new,
            SUBETHA_ORDER_SEQ_CST,
            SUBETHA_ORDER_SEQ_CST,
            out_current,
            out_swapped,
        )
    }
}

/// Replace the 32-bit value with `new` only if it is `expected`, under
/// `success` when it is replaced and `failure` for the load when it is
/// not; `failure` cannot be release or acquire-release.
///
/// # Safety
/// `out_swapped` is a valid pointer; `out_current` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u32_compare_exchange_explicit(
    handle: subetha_handle,
    expected: u32,
    new: u32,
    success: u32,
    failure: u32,
    out_current: *mut u32,
    out_swapped: *mut bool,
) -> i32 {
    with_u32(handle, |a| {
        if out_swapped.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_swapped is null");
        }
        let success = match order(success) {
            Ok(o) => o,
            Err(code) => return code,
        };
        let failure = match load_order(failure) {
            Ok(o) => o,
            Err(code) => return code,
        };
        let (swapped, current) = match a.compare_exchange(expected, new, success, failure) {
            Ok(prev) => (true, prev),
            Err(prev) => (false, prev),
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_swapped = swapped };
        unsafe { put_u32(out_current, current) }
    })
}

/// Read the 64-bit value into `out`, sequentially consistent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_load(handle: subetha_handle, out: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_load_explicit(handle, SUBETHA_ORDER_SEQ_CST, out) }
}

/// Read the 64-bit value into `out` under `order`, which is relaxed,
/// acquire or sequentially consistent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_load_explicit(handle: subetha_handle, order: u32, out: *mut u64) -> i32 {
    with_u64(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let ord = match load_order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = a.load(ord) };
        SUBETHA_OK
    })
}

/// Write the 64-bit value, sequentially consistent.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_u64_store(handle: subetha_handle, value: u64) -> i32 {
    subetha_atomic_u64_store_explicit(handle, value, SUBETHA_ORDER_SEQ_CST)
}

/// Write the 64-bit value under `order`, which is relaxed, release or
/// sequentially consistent.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_u64_store_explicit(handle: subetha_handle, value: u64, order: u32) -> i32 {
    with_u64(handle, |a| {
        let ord = match store_order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        a.store(value, ord);
        SUBETHA_OK
    })
}

/// Replace the 64-bit value, the previous one into `out_prev` when that is
/// not null, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_swap(handle: subetha_handle, value: u64, out_prev: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_swap_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Replace the 64-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_swap_explicit(handle: subetha_handle, value: u64, order: u32, out_prev: *mut u64) -> i32 {
    with_u64(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u64(out_prev, a.swap(value, ord)) }
    })
}

/// Add to the 64-bit value, wrapping, the previous one into `out_prev`
/// when that is not null, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_add(handle: subetha_handle, value: u64, out_prev: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_fetch_add_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Add to the 64-bit value, wrapping, under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_add_explicit(
    handle: subetha_handle,
    value: u64,
    order: u32,
    out_prev: *mut u64,
) -> i32 {
    with_u64(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u64(out_prev, a.fetch_add(value, ord)) }
    })
}

/// Subtract from the 64-bit value, wrapping, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_sub(handle: subetha_handle, value: u64, out_prev: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_fetch_sub_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Subtract from the 64-bit value, wrapping, under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_sub_explicit(
    handle: subetha_handle,
    value: u64,
    order: u32,
    out_prev: *mut u64,
) -> i32 {
    with_u64(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u64(out_prev, a.fetch_sub(value, ord)) }
    })
}

/// Bitwise-and the 64-bit value, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_and(handle: subetha_handle, value: u64, out_prev: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_fetch_and_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Bitwise-and the 64-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_and_explicit(
    handle: subetha_handle,
    value: u64,
    order: u32,
    out_prev: *mut u64,
) -> i32 {
    with_u64(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u64(out_prev, a.fetch_and(value, ord)) }
    })
}

/// Bitwise-or the 64-bit value, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_or(handle: subetha_handle, value: u64, out_prev: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_fetch_or_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Bitwise-or the 64-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_or_explicit(
    handle: subetha_handle,
    value: u64,
    order: u32,
    out_prev: *mut u64,
) -> i32 {
    with_u64(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u64(out_prev, a.fetch_or(value, ord)) }
    })
}

/// Bitwise-exclusive-or the 64-bit value, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_xor(handle: subetha_handle, value: u64, out_prev: *mut u64) -> i32 {
    unsafe { subetha_atomic_u64_fetch_xor_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Bitwise-exclusive-or the 64-bit value under `order`.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_fetch_xor_explicit(
    handle: subetha_handle,
    value: u64,
    order: u32,
    out_prev: *mut u64,
) -> i32 {
    with_u64(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        unsafe { put_u64(out_prev, a.fetch_xor(value, ord)) }
    })
}

/// Replace the 64-bit value with `new` only if it is `expected`.
/// `out_swapped` receives whether it was, and `out_current`, when not
/// null, the value found. Sequentially consistent.
///
/// # Safety
/// `out_swapped` is a valid pointer; `out_current` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_compare_exchange(
    handle: subetha_handle,
    expected: u64,
    new: u64,
    out_current: *mut u64,
    out_swapped: *mut bool,
) -> i32 {
    unsafe {
        subetha_atomic_u64_compare_exchange_explicit(
            handle,
            expected,
            new,
            SUBETHA_ORDER_SEQ_CST,
            SUBETHA_ORDER_SEQ_CST,
            out_current,
            out_swapped,
        )
    }
}

/// Replace the 64-bit value with `new` only if it is `expected`, under
/// `success` when it is replaced and `failure` for the load when it is
/// not; `failure` cannot be release or acquire-release.
///
/// # Safety
/// `out_swapped` is a valid pointer; `out_current` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_u64_compare_exchange_explicit(
    handle: subetha_handle,
    expected: u64,
    new: u64,
    success: u32,
    failure: u32,
    out_current: *mut u64,
    out_swapped: *mut bool,
) -> i32 {
    with_u64(handle, |a| {
        if out_swapped.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_swapped is null");
        }
        let success = match order(success) {
            Ok(o) => o,
            Err(code) => return code,
        };
        let failure = match load_order(failure) {
            Ok(o) => o,
            Err(code) => return code,
        };
        let (swapped, current) = match a.compare_exchange(expected, new, success, failure) {
            Ok(prev) => (true, prev),
            Err(prev) => (false, prev),
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_swapped = swapped };
        unsafe { put_u64(out_current, current) }
    })
}

/// Read the flag into `out`, sequentially consistent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_load(handle: subetha_handle, out: *mut bool) -> i32 {
    unsafe { subetha_atomic_bool_load_explicit(handle, SUBETHA_ORDER_SEQ_CST, out) }
}

/// Read the flag into `out` under `order`, which is relaxed, acquire or
/// sequentially consistent.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_load_explicit(handle: subetha_handle, order: u32, out: *mut bool) -> i32 {
    with_bool(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let ord = match load_order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = a.load(ord) };
        SUBETHA_OK
    })
}

/// Write the flag, sequentially consistent.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_bool_store(handle: subetha_handle, value: bool) -> i32 {
    subetha_atomic_bool_store_explicit(handle, value, SUBETHA_ORDER_SEQ_CST)
}

/// Write the flag under `order`, which is relaxed, release or
/// sequentially consistent.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_bool_store_explicit(handle: subetha_handle, value: bool, order: u32) -> i32 {
    with_bool(handle, |a| {
        let ord = match store_order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        a.store(value, ord);
        SUBETHA_OK
    })
}

/// Replace the flag, the previous value into `out_prev` when that is not
/// null, sequentially consistent.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_swap(handle: subetha_handle, value: bool, out_prev: *mut bool) -> i32 {
    unsafe { subetha_atomic_bool_swap_explicit(handle, value, SUBETHA_ORDER_SEQ_CST, out_prev) }
}

/// Replace the flag under `order`, the previous value into `out_prev`
/// when that is not null.
///
/// # Safety
/// `out_prev` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_bool_swap_explicit(
    handle: subetha_handle,
    value: bool,
    order: u32,
    out_prev: *mut bool,
) -> i32 {
    with_bool(handle, |a| {
        let ord = match crate::atomic::order(order) {
            Ok(o) => o,
            Err(code) => return code,
        };
        let prev = a.swap(value, ord);
        if !out_prev.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_prev = prev };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the atomic into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_read_stats(handle: subetha_handle, out: *mut subetha_atomic_stats) -> i32 {
    with_atomic(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = a.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the atomic's page to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_atomic_flush(handle: subetha_handle) -> i32 {
    with_atomic(handle, |a| match a.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => atomic_code(e),
    })
}

/// Remove the atomic's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_atomic_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
    use crate::runtime::SUBETHA_MODE_STRICT;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!("subetha-ffi-atomic-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the atomic's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_orderings_map_and_the_impossible_ones_are_refused() {
        assert_eq!(order(SUBETHA_ORDER_RELAXED).unwrap(), Ordering::Relaxed);
        assert_eq!(order(SUBETHA_ORDER_ACQUIRE).unwrap(), Ordering::Acquire);
        assert_eq!(order(SUBETHA_ORDER_RELEASE).unwrap(), Ordering::Release);
        assert_eq!(order(SUBETHA_ORDER_ACQ_REL).unwrap(), Ordering::AcqRel);
        assert_eq!(order(SUBETHA_ORDER_SEQ_CST).unwrap(), Ordering::SeqCst);
        assert_eq!(order(5).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(load_order(SUBETHA_ORDER_ACQUIRE).unwrap(), Ordering::Acquire);
        assert_eq!(load_order(SUBETHA_ORDER_RELEASE).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(load_order(SUBETHA_ORDER_ACQ_REL).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(store_order(SUBETHA_ORDER_RELEASE).unwrap(), Ordering::Release);
        assert_eq!(store_order(SUBETHA_ORDER_ACQUIRE).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(store_order(SUBETHA_ORDER_ACQ_REL).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
    }

    #[test]
    fn the_object_reports_the_width_it_was_built_at() {
        let scratch = Scratch::new("width");
        let object = AtomicObject {
            atomic: Atomic::U64(SharedAtomicU64::create(&scratch.0, 7).unwrap()),
            mode: SUBETHA_MODE_STRICT,
        };
        let stats = object.stats();
        assert_eq!(stats.width, SUBETHA_ATOMIC_U64);
        assert_eq!(stats.bytes, 8);
        assert_eq!(stats.mode, SUBETHA_MODE_STRICT);
        match &object.atomic {
            Atomic::U64(a) => {
                assert_eq!(a.load(Ordering::SeqCst), 7);
                assert_eq!(a.fetch_add(3, Ordering::AcqRel), 7);
                assert_eq!(a.load(Ordering::SeqCst), 10);
            }
            _ => panic!("the object holds the width it was built at"),
        }
    }
}
