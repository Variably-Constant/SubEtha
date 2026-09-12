//! The shared Bloom filter through the C ABI: a bit array in a file that
//! any number of processes insert into and test, sized by the caller.
//!
//! Membership is one-sided. A negative answer is certain; a positive one
//! carries the filter's false-positive rate, which `read_stats` reports
//! from the bits actually set rather than from the sizing alone. Items
//! are bytes, so the same bytes name the same member from every process
//! and every language. The filter runs no background work, so strict and
//! managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_bloom_filter::{BloomError, SharedBloomFilter};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_BLOOM};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared Bloom filter.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_bloom_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Hash functions each item is put through.
    pub n_hashes: u32,
    /// Bits in the filter.
    pub n_bits: u64,
    /// Items the set bits imply, which is an estimate: the filter stores
    /// no count, and re-inserting a member adds nothing to it.
    pub estimated_items: u64,
    /// The false-positive rate the bits now set imply, from zero to one.
    pub false_positive_rate: f64,
}

pub(crate) struct BloomObject {
    filter: SharedBloomFilter,
    mode: u32,
}

impl BloomObject {
    /// A filter parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: BloomError) -> i32 {
    match e {
        BloomError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the filter on disk was built with other dimensions",
        ),
        BloomError::InvalidConfig => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "n_bits and n_hashes must both be above zero")
        }
        BloomError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
        BloomError::BitVec(e) => fail(SUBETHA_E_RING_IO, format!("bit vector: {e:?}")),
    }
}

fn with_bloom(handle: subetha_handle, f: impl FnOnce(&BloomObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_BLOOM, |object| match object {
        Object::Bloom(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a Bloom filter"),
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    n_bits: u64,
    n_hashes: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if n_bits == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "n_bits is zero"));
    }
    if n_hashes == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "n_hashes is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, n_bits as usize, n_hashes, mode))
}

/// The dimensions that hold `n_items` members at a false-positive rate of
/// `p`, into `out_bits` and `out_hashes`. A caller sizes a filter from
/// what it expects to hold rather than from bit counts.
///
/// # Safety
/// `out_bits` and `out_hashes` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_suggest(
    n_items: u64,
    p: f64,
    out_bits: *mut u64,
    out_hashes: *mut u32,
) -> i32 {
    entry(|| {
        if out_bits.is_null() || out_hashes.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_bits or out_hashes is null");
        }
        if n_items == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "n_items is zero");
        }
        if !(p > 0.0 && p < 1.0) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "p is not between zero and one");
        }
        let (bits, hashes) = SharedBloomFilter::suggest_config(n_items as usize, p);
        // Checked non-null; the caller guarantees they are writable.
        unsafe {
            *out_bits = bits as u64;
            *out_hashes = hashes;
        }
        SUBETHA_OK
    })
}

/// Obtain the filter at `path` with `n_bits` bits and `n_hashes` hash
/// functions: an empty one is initialized when its files do not exist, an
/// existing one is attached with its members in place. A filter built
/// with other dimensions is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_create(
    path: *const c_char,
    n_bits: u64,
    n_hashes: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, bits, hashes, mode) =
            match unsafe { read_arguments(path, n_bits, n_hashes, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedBloomFilter::create(path, bits, hashes) {
            Ok(filter) => unsafe { issue(Object::Bloom(BloomObject { filter, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Truncate the filter's files at `path` and initialize an empty one,
/// discarding every member live peers share. For a caller that knows it
/// owns the path.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_reset(
    path: *const c_char,
    n_bits: u64,
    n_hashes: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, bits, hashes, mode) =
            match unsafe { read_arguments(path, n_bits, n_hashes, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedBloomFilter::reset(path, bits, hashes) {
            Ok(filter) => unsafe { issue(Object::Bloom(BloomObject { filter, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the filter another process created at `path`; its files must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_open(
    path: *const c_char,
    n_bits: u64,
    n_hashes: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, bits, hashes, mode) =
            match unsafe { read_arguments(path, n_bits, n_hashes, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedBloomFilter::open(path, bits, hashes) {
            Ok(filter) => unsafe { issue(Object::Bloom(BloomObject { filter, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Add the `len` bytes at `item` to the filter.
///
/// # Safety
/// `item` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_insert(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
) -> i32 {
    with_bloom(handle, |b| {
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        match b.filter.insert(item) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// Whether the `len` bytes at `item` may be a member, into `out`. False is
/// certain; true carries the filter's false-positive rate.
///
/// # Safety
/// `item` points to `len` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_contains(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
    out: *mut bool,
) -> i32 {
    with_bloom(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        match b.filter.contains(item) {
            Ok(present) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = present };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Clear every bit, so the filter holds no members. Peers sharing the
/// files see the same empty filter.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bloom_clear(handle: subetha_handle) -> i32 {
    with_bloom(handle, |b| {
        b.filter.clear();
        SUBETHA_OK
    })
}

/// Push the filter's dirty pages to disk, returning when they are
/// durable. `subetha_bloom_flush_async` starts the same work and returns
/// at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bloom_flush(handle: subetha_handle) -> i32 {
    with_bloom(handle, |b| match b.filter.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the filter's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bloom_flush_async(handle: subetha_handle) -> i32 {
    with_bloom(handle, |b| match b.filter.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the filter into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bloom_read_stats(
    handle: subetha_handle,
    out: *mut subetha_bloom_stats,
) -> i32 {
    with_bloom(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_bloom_stats {
            mode: b.mode,
            n_hashes: b.filter.n_hashes(),
            n_bits: b.filter.n_bits(),
            estimated_items: b.filter.estimated_insert_count(),
            false_positive_rate: b.filter.estimated_false_positive_rate(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
