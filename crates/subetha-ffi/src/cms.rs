//! The shared count-min sketch through the C ABI: a counter matrix in a
//! file that any number of processes count into and query.
//!
//! Frequencies are one-sided. Hash collisions add to a counter and never
//! take from one, so an estimate is at or above the true count and never
//! below it. Items are bytes, so the same bytes name the same item from
//! every process and every language. The sketch runs no background work,
//! so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_count_min_sketch::{CMSError, SharedCountMinSketch};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_CMS};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared count-min sketch.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_cms_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Rows: one hash function each, and the estimate is their minimum.
    pub d: u32,
    /// Counters per row.
    pub w: u32,
    /// Everything counted in, summed over every insert.
    pub total_inserts: u64,
}

pub(crate) struct CmsObject {
    sketch: SharedCountMinSketch,
    mode: u32,
}

impl CmsObject {
    /// A sketch parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: CMSError) -> i32 {
    match e {
        CMSError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the sketch on disk was built with other dimensions",
        ),
        CMSError::InvalidConfig => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "d and w must both be above zero")
        }
        CMSError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_cms(handle: subetha_handle, f: impl FnOnce(&CmsObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CMS, |object| match object {
        Object::Cms(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a count-min sketch"),
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    d: u32,
    w: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, u32, u32, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if d == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "d is zero"));
    }
    if w == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "w is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, d, w, mode))
}

/// The dimensions that hold an estimate within `epsilon` of the true
/// count with probability `1 - delta`, into `out_d` and `out_w`. A caller
/// sizes a sketch from the error it will tolerate rather than from a
/// counter matrix.
///
/// # Safety
/// `out_d` and `out_w` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_suggest(
    epsilon: f64,
    delta: f64,
    out_d: *mut u32,
    out_w: *mut u32,
) -> i32 {
    entry(|| {
        if out_d.is_null() || out_w.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_d or out_w is null");
        }
        if !(epsilon > 0.0 && epsilon < 1.0) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "epsilon is not between zero and one");
        }
        if !(delta > 0.0 && delta < 1.0) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "delta is not between zero and one");
        }
        let (d, w) = SharedCountMinSketch::suggest_config(epsilon, delta);
        // Checked non-null; the caller guarantees they are writable.
        unsafe {
            *out_d = d;
            *out_w = w;
        }
        SUBETHA_OK
    })
}

/// Obtain the sketch at `path` with `d` rows of `w` counters: an empty one
/// is initialized when the file does not exist, an existing one is
/// attached with its counts in place. A sketch built with other
/// dimensions is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_create(
    path: *const c_char,
    d: u32,
    w: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, d, w, mode) = match unsafe { read_arguments(path, d, w, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedCountMinSketch::create(path, d, w) {
            Ok(sketch) => unsafe { issue(Object::Cms(CmsObject { sketch, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the sketch another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_open(
    path: *const c_char,
    d: u32,
    w: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, d, w, mode) = match unsafe { read_arguments(path, d, w, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedCountMinSketch::open(path, d, w) {
            Ok(sketch) => unsafe { issue(Object::Cms(CmsObject { sketch, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Count one occurrence of the `len` bytes at `item`.
///
/// # Safety
/// `item` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_insert(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
) -> i32 {
    with_cms(handle, |c| {
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        c.sketch.insert(item);
        SUBETHA_OK
    })
}

/// Count `count` occurrences of the `len` bytes at `item`.
///
/// # Safety
/// `item` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_insert_n(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
    count: u64,
) -> i32 {
    with_cms(handle, |c| {
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        c.sketch.insert_n(item, count);
        SUBETHA_OK
    })
}

/// The estimated count of the `len` bytes at `item`, into `out`. At or
/// above the true count, never below it.
///
/// # Safety
/// `item` points to `len` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_estimate(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
    out: *mut u64,
) -> i32 {
    with_cms(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        let estimate = c.sketch.estimate_count(item);
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = estimate };
        SUBETHA_OK
    })
}

/// Zero every counter, so the sketch holds no counts. Peers sharing the
/// file see the same empty sketch.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_cms_reset(handle: subetha_handle) -> i32 {
    with_cms(handle, |c| {
        c.sketch.reset();
        SUBETHA_OK
    })
}

/// Push the sketch's dirty pages to disk, returning when they are
/// durable. `subetha_cms_flush_async` starts the same work and returns at
/// once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_cms_flush(handle: subetha_handle) -> i32 {
    with_cms(handle, |c| match c.sketch.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the sketch's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_cms_flush_async(handle: subetha_handle) -> i32 {
    with_cms(handle, |c| match c.sketch.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the sketch into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cms_read_stats(
    handle: subetha_handle,
    out: *mut subetha_cms_stats,
) -> i32 {
    with_cms(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_cms_stats {
            mode: c.mode,
            d: c.sketch.d(),
            w: c.sketch.w(),
            total_inserts: c.sketch.total_inserts(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
