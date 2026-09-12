//! The shared blocked Bloom filter through the C ABI: the same membership
//! contract as `subetha_bloom_*`, laid out so a lookup touches one cache
//! line.
//!
//! An item hashes first to a single 512-bit block and then sets its bits
//! inside that block, so a query reads one line instead of scattering
//! across the array. The trade is that concentrating the bits raises the
//! false-positive rate a little for the same sizing. Membership stays
//! one-sided: a negative answer is certain. The filter runs no background
//! work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_blocked_bloom_filter::{BlockedBloomError, SharedBlockedBloomFilter};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_BLOCKED_BLOOM};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared blocked Bloom filter.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_blocked_bloom_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Hash functions each item is put through, all within one block.
    pub n_hashes: u32,
    /// Blocks of 512 bits, one cache line each.
    pub n_blocks: u64,
}

pub(crate) struct BlockedBloomObject {
    filter: SharedBlockedBloomFilter,
    mode: u32,
}

impl BlockedBloomObject {
    /// A filter parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: BlockedBloomError) -> i32 {
    match e {
        BlockedBloomError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the filter on disk was built with other dimensions",
        ),
        BlockedBloomError::InvalidConfig => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "n_bits and n_hashes must both be above zero")
        }
        BlockedBloomError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_filter(handle: subetha_handle, f: impl FnOnce(&BlockedBloomObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_BLOCKED_BLOOM, |object| match object {
        Object::BlockedBloom(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a blocked Bloom filter"),
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
/// `p`, into `out_bits` and `out_hashes`.
///
/// # Safety
/// `out_bits` and `out_hashes` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_suggest(
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
        let (bits, hashes) = SharedBlockedBloomFilter::suggest_config(n_items as usize, p);
        // Checked non-null; the caller guarantees they are writable.
        unsafe {
            *out_bits = bits as u64;
            *out_hashes = hashes;
        }
        SUBETHA_OK
    })
}

/// Obtain the filter at `path` with `n_bits` bits and `n_hashes` hash
/// functions: an empty one is initialized when the file does not exist, an
/// existing one is attached with its members in place. The bits are
/// rounded up to whole 512-bit blocks, which `read_stats` reports.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_create(
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
        match SharedBlockedBloomFilter::create(path, bits, hashes) {
            Ok(filter) => unsafe {
                issue(Object::BlockedBloom(BlockedBloomObject { filter, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Truncate the filter's file at `path` and initialize an empty one,
/// discarding every member live peers share.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_reset(
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
        match SharedBlockedBloomFilter::reset(path, bits, hashes) {
            Ok(filter) => unsafe {
                issue(Object::BlockedBloom(BlockedBloomObject { filter, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the filter another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_open(
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
        match SharedBlockedBloomFilter::open(path, bits, hashes) {
            Ok(filter) => unsafe {
                issue(Object::BlockedBloom(BlockedBloomObject { filter, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Add the `len` bytes at `item` to the filter.
///
/// # Safety
/// `item` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_insert(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
) -> i32 {
    with_filter(handle, |b| {
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        b.filter.insert(item);
        SUBETHA_OK
    })
}

/// Whether the `len` bytes at `item` may be a member, into `out`. False is
/// certain; true carries the filter's false-positive rate.
///
/// # Safety
/// `item` points to `len` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_contains(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
    out: *mut bool,
) -> i32 {
    with_filter(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        let present = b.filter.contains(item);
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = present };
        SUBETHA_OK
    })
}

/// Clear every block, so the filter holds no members.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_blocked_bloom_clear(handle: subetha_handle) -> i32 {
    with_filter(handle, |b| {
        b.filter.clear();
        SUBETHA_OK
    })
}

/// Push the filter's dirty pages to disk, returning when they are durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_blocked_bloom_flush(handle: subetha_handle) -> i32 {
    with_filter(handle, |b| match b.filter.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the filter into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocked_bloom_read_stats(
    handle: subetha_handle,
    out: *mut subetha_blocked_bloom_stats,
) -> i32 {
    with_filter(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_blocked_bloom_stats {
            mode: b.mode,
            n_hashes: b.filter.n_hashes(),
            n_blocks: b.filter.n_blocks(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
