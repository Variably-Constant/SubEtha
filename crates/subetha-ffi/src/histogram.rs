//! The shared histogram through the C ABI: bucket counters in a file that
//! any number of processes record into and read.
//!
//! The caller declares the bucket boundaries, ascending, and gets one
//! bucket per boundary plus a final one for everything above the last.
//! A percentile is read off the counts, so it names a bucket boundary
//! rather than an interpolated value. The histogram runs no background
//! work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_histogram::{HistogramError, SharedHistogram};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_HISTOGRAM};
use crate::ring::text;
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared histogram.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_histogram_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Buckets, which is one more than the boundaries the caller gave.
    pub n_buckets: u64,
    /// Values recorded.
    pub total_count: u64,
}

pub(crate) struct HistogramObject {
    histogram: SharedHistogram,
    mode: u32,
}

impl HistogramObject {
    /// A histogram parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: HistogramError) -> i32 {
    match e {
        HistogramError::EmptyBoundaries => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the boundaries are empty")
        }
        HistogramError::NonMonotonicBoundaries => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the boundaries do not ascend")
        }
        HistogramError::OutOfBounds => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "that bucket index is past the last bucket")
        }
        HistogramError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the histogram on disk was built with other boundaries",
        ),
        HistogramError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_histogram(handle: subetha_handle, f: impl FnOnce(&HistogramObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_HISTOGRAM, |object| match object {
        Object::Histogram(h) => f(h),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a histogram"),
    })
}

/// The arguments both constructors read, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `boundaries` points to
/// `n_boundaries` readable values; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    boundaries: *const u64,
    n_boundaries: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, &'a [u64], u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if boundaries.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "boundaries is null"));
    }
    if n_boundaries == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "n_boundaries is zero"));
    }
    // Checked non-null; the caller guarantees the count.
    let boundaries = unsafe { std::slice::from_raw_parts(boundaries, n_boundaries) };
    let mode = resolve_mode(mode)?;
    Ok((path, boundaries, mode))
}

/// Obtain the histogram at `path` with the `n_boundaries` ascending
/// upper bounds at `boundaries`: an empty one is initialized when the
/// file does not exist, an existing one is attached with its counts in
/// place. A histogram built with other boundaries is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `boundaries` points to
/// `n_boundaries` readable values; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_create(
    path: *const c_char,
    boundaries: *const u64,
    n_boundaries: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, boundaries, mode) =
            match unsafe { read_arguments(path, boundaries, n_boundaries, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedHistogram::create(path, boundaries) {
            Ok(histogram) => unsafe {
                issue(Object::Histogram(HistogramObject { histogram, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the histogram another process created at `path`; the file
/// must exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `boundaries` points to
/// `n_boundaries` readable values; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_open(
    path: *const c_char,
    boundaries: *const u64,
    n_boundaries: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, boundaries, mode) =
            match unsafe { read_arguments(path, boundaries, n_boundaries, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedHistogram::open(path, boundaries) {
            Ok(histogram) => unsafe {
                issue(Object::Histogram(HistogramObject { histogram, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Record `value`, and report the bucket it landed in through `out_bucket`
/// when that is not null.
///
/// # Safety
/// `out_bucket` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_record(
    handle: subetha_handle,
    value: u64,
    out_bucket: *mut u64,
) -> i32 {
    with_histogram(handle, |h| {
        let bucket = h.histogram.record(value);
        if !out_bucket.is_null() {
            // Checked non-null; the caller guarantees it is writable.
            unsafe { *out_bucket = bucket as u64 };
        }
        SUBETHA_OK
    })
}

/// The bucket `value` would land in, into `out`, without recording it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_bucket_for(
    handle: subetha_handle,
    value: u64,
    out: *mut u64,
) -> i32 {
    with_histogram(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let bucket = h.histogram.bucket_for(value);
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = bucket as u64 };
        SUBETHA_OK
    })
}

/// The count in bucket `bucket_idx`, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_count(
    handle: subetha_handle,
    bucket_idx: u64,
    out: *mut u64,
) -> i32 {
    with_histogram(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match h.histogram.count(bucket_idx as usize) {
            Ok(count) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = count };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Every bucket's count into `out`, which holds `cap` values, and how many
/// were written into `out_len`. `SUBETHA_E_BUFFER_TOO_SMALL` when `cap` is
/// below the bucket count, with the count needed in `out_len`.
///
/// # Safety
/// `out` points to `cap` writable values; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_counts(
    handle: subetha_handle,
    out: *mut u64,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_histogram(handle, |h| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        let counts = h.histogram.counts();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = counts.len() };
        if out.is_null() || cap < counts.len() {
            return crate::error::SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable values, and cap >= len here.
        unsafe { std::ptr::copy_nonoverlapping(counts.as_ptr(), out, counts.len()) };
        SUBETHA_OK
    })
}

/// The boundaries the histogram was built with into `out`, the same way
/// `subetha_histogram_counts` reports counts.
///
/// # Safety
/// `out` points to `cap` writable values; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_boundaries(
    handle: subetha_handle,
    out: *mut u64,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_histogram(handle, |h| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        let boundaries = h.histogram.boundaries_vec();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = boundaries.len() };
        if out.is_null() || cap < boundaries.len() {
            return crate::error::SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable values, and cap >= len here.
        unsafe { std::ptr::copy_nonoverlapping(boundaries.as_ptr(), out, boundaries.len()) };
        SUBETHA_OK
    })
}

/// The value at percentile `p`, from zero to one hundred, into `out`. The
/// answer is a bucket boundary, since that is what the counts record.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_percentile(
    handle: subetha_handle,
    p: f64,
    out: *mut u64,
) -> i32 {
    with_histogram(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if !(0.0..=100.0).contains(&p) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "p is not between zero and one hundred");
        }
        let value = h.histogram.percentile(p);
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = value };
        SUBETHA_OK
    })
}

/// Zero every bucket. Peers sharing the file see the same empty histogram.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_histogram_reset(handle: subetha_handle) -> i32 {
    with_histogram(handle, |h| {
        h.histogram.reset();
        SUBETHA_OK
    })
}

/// Push the histogram's dirty pages to disk, returning when they are
/// durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_histogram_flush(handle: subetha_handle) -> i32 {
    with_histogram(handle, |h| match h.histogram.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the histogram's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_histogram_flush_async(handle: subetha_handle) -> i32 {
    with_histogram(handle, |h| match h.histogram.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the histogram into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_histogram_read_stats(
    handle: subetha_handle,
    out: *mut subetha_histogram_stats,
) -> i32 {
    with_histogram(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_histogram_stats {
            mode: h.mode,
            n_buckets: h.histogram.n_buckets() as u64,
            total_count: h.histogram.total_count(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
