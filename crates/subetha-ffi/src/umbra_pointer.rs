//! The content-prefix pointer through the C ABI.
//!
//! Unlike every other family here the pointer has no handle. It is
//! sixteen bytes the caller holds, copies, and stores wherever it likes;
//! these entry points pack it, unpack it, and resolve it against a region
//! the caller does own a handle to. So an operation borrows exactly one
//! handle, never two.
//!
//! # What the prefix promises
//!
//! Four bytes of the value travel with the pointer. Comparing them
//! answers "these definitely differ" without reading the region at all.
//! A prefix match is not equality - it is four bytes of agreement - so a
//! match means resolve and compare, while a mismatch means skip without a
//! dereference. That is the whole saving, which is why the comparison is
//! called `subetha_umbra_prefix_eq` rather than anything suggesting it
//! settles the question.
//!
//! # Two things a caller cannot infer
//!
//! A value shorter than four bytes has its prefix padded with zeros, so a
//! two-byte value and the same two bytes followed by two zeros share a
//! prefix. That costs a dereference on a collision and never a wrong
//! answer.
//!
//! The prefix is the value as stored. Two callers agree on it only if
//! they agree on the value's byte layout, so a value with padding or a
//! different endianness has a prefix that means nothing across that
//! boundary.

use subetha_cxc::raw_umbra_pointer::{
    raw_content_prefix, RawUmbraError, RawUmbraPointer, RAW_UMBRA_EXT_BYTES, RAW_UMBRA_NIL,
};
use subetha_cxc::shared_region::RegionError;

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_FULL, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_OK,
};
use crate::handle::subetha_handle;
use crate::region::with_region;
use crate::ring::bytes;
use crate::runtime::entry;

/// Bytes a packed pointer occupies.
pub const SUBETHA_UMBRA_BYTES: u64 = 16;
/// Payload bytes a caller may carry beside the tag.
pub const SUBETHA_UMBRA_EXT_BYTES: u64 = 7;
/// The tag meaning no caller extension is present.
pub const SUBETHA_UMBRA_EXT_NONE: u32 = 0;
/// The target index meaning the pointer aims at nothing.
pub const SUBETHA_UMBRA_NIL: u32 = u32::MAX;

// Literals pinned to the layer below, because the header generator copies
// a constant's expression rather than its value.
const _: () = assert!(SUBETHA_UMBRA_EXT_BYTES as usize == RAW_UMBRA_EXT_BYTES);
const _: () = assert!(SUBETHA_UMBRA_NIL == RAW_UMBRA_NIL);

fn code_for(e: RawUmbraError) -> i32 {
    match e {
        RawUmbraError::WrongSize { expected, found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("this region holds {expected}-byte values, and {found} was given"),
        ),
        RawUmbraError::ExtTooLong { found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("an extension payload is at most 7 bytes, and {found} was given"),
        ),
        RawUmbraError::Nil => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the pointer aims at nothing")
        }
        RawUmbraError::Region(RegionError::Full) => {
            fail(SUBETHA_E_MAP_FULL, "the region has no free slot")
        }
        RawUmbraError::Region(RegionError::InvalidPtr) => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the pointer names no live slot")
        }
        RawUmbraError::Region(RegionError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the region was built with a different element size",
        ),
        RawUmbraError::Region(RegionError::IoError(k)) => {
            fail(SUBETHA_E_RING_IO, format!("the region could not be reached: {k:?}"))
        }
        RawUmbraError::Region(other) => {
            fail(SUBETHA_E_RING_IO, format!("the region refused: {other:?}"))
        }
    }
}

/// # Safety
/// `out` points to at least `SUBETHA_UMBRA_BYTES` writable bytes.
unsafe fn write_pointer(p: RawUmbraPointer, out: *mut u8) {
    let packed = p.to_bytes();
    // The caller guarantees the buffer; the length is this library's.
    unsafe { std::ptr::copy_nonoverlapping(packed.as_ptr(), out, packed.len()) };
}

/// # Safety
/// `bytes_in` points to at least `SUBETHA_UMBRA_BYTES` readable bytes.
unsafe fn read_pointer(bytes_in: *const u8) -> Result<RawUmbraPointer, i32> {
    if bytes_in.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "the pointer buffer is null"));
    }
    let mut packed = [0u8; 16];
    // The caller guarantees sixteen readable bytes.
    unsafe { std::ptr::copy_nonoverlapping(bytes_in, packed.as_mut_ptr(), packed.len()) };
    Ok(RawUmbraPointer::from_bytes(packed))
}

/// The four-byte prefix `value` would carry, without storing anything.
///
/// Lets a caller compute a query prefix once and test many pointers
/// against it with `subetha_umbra_matches_prefix`.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `prefix_out` is valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_content_prefix(
    value: *const u8,
    value_len: u64,
    prefix_out: *mut u32,
) -> i32 {
    entry(|| {
        if prefix_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "prefix_out is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        // Checked non-null above.
        unsafe { *prefix_out = raw_content_prefix(value) };
        SUBETHA_OK
    })
}

/// A pointer at nothing, into `out`.
///
/// # Safety
/// `out` points to at least `SUBETHA_UMBRA_BYTES` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_nil(out: *mut u8) -> i32 {
    entry(|| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        unsafe { write_pointer(RawUmbraPointer::NIL, out) };
        SUBETHA_OK
    })
}

/// Store `value` in the region `region` names and write a pointer at it
/// into `out`, taking the prefix from the value's own first four bytes.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `out` points to at least
/// `SUBETHA_UMBRA_BYTES` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_allocate(
    region: subetha_handle,
    value: *const u8,
    value_len: u64,
    out: *mut u8,
) -> i32 {
    with_region(region, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match RawUmbraPointer::allocate(r.raw(), value) {
            Ok(p) => {
                unsafe { write_pointer(p, out) };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Store `value` and write a pointer at it carrying `prefix`, rather than
/// one taken from the bytes.
///
/// For a record opening with a header its first four bytes share with
/// every other record, a caller-chosen prefix is what makes the filter
/// worth having at all.
///
/// # Safety
/// As [`subetha_umbra_allocate`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_allocate_with_prefix(
    region: subetha_handle,
    value: *const u8,
    value_len: u64,
    prefix: u32,
    out: *mut u8,
) -> i32 {
    with_region(region, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match RawUmbraPointer::allocate_with_prefix(r.raw(), value, prefix) {
            Ok(p) => {
                unsafe { write_pointer(p, out) };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Read the value `pointer` names into `value_out`.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `value_out` points to at least `value_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_resolve(
    region: subetha_handle,
    pointer: *const u8,
    value_out: *mut u8,
    value_len: u64,
) -> i32 {
    with_region(region, |r| {
        if value_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "value_out is null");
        }
        let p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let mut scratch = vec![0u8; value_len as usize];
        match p.resolve(r.raw(), &mut scratch) {
            Ok(()) => {
                // Checked non-null above.
                unsafe {
                    std::ptr::copy_nonoverlapping(scratch.as_ptr(), value_out, scratch.len());
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// The slot `pointer` names, into `target_out`, or `SUBETHA_UMBRA_NIL`.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `target_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_target(
    pointer: *const u8,
    target_out: *mut u32,
) -> i32 {
    entry(|| {
        if target_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "target_out is null");
        }
        let p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        // Checked non-null above.
        unsafe { *target_out = p.target() };
        SUBETHA_OK
    })
}

/// The prefix `pointer` carries, into `prefix_out`.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `prefix_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_prefix(
    pointer: *const u8,
    prefix_out: *mut u32,
) -> i32 {
    entry(|| {
        if prefix_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "prefix_out is null");
        }
        let p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        // Checked non-null above.
        unsafe { *prefix_out = p.prefix() };
        SUBETHA_OK
    })
}

/// Whether `pointer` aims at nothing, into `nil_out`.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `nil_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_is_nil(
    pointer: *const u8,
    nil_out: *mut bool,
) -> i32 {
    entry(|| {
        if nil_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "nil_out is null");
        }
        let p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        // Checked non-null above.
        unsafe { *nil_out = p.is_nil() };
        SUBETHA_OK
    })
}

/// Whether two pointers could aim at equal values, into `equal_out`.
///
/// A false answer is certain. A true answer means resolve both and
/// compare: four bytes agreeing is not equality.
///
/// # Safety
/// Both pointers point to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `equal_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_prefix_eq(
    left: *const u8,
    right: *const u8,
    equal_out: *mut bool,
) -> i32 {
    entry(|| {
        if equal_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "equal_out is null");
        }
        let a = match unsafe { read_pointer(left) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let b = match unsafe { read_pointer(right) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        // Checked non-null above.
        unsafe { *equal_out = a.prefix_eq(b) };
        SUBETHA_OK
    })
}

/// Whether `pointer` could aim at a value starting with `query`, into
/// `matches_out`.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `matches_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_matches_prefix(
    pointer: *const u8,
    query: u32,
    matches_out: *mut bool,
) -> i32 {
    entry(|| {
        if matches_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "matches_out is null");
        }
        let p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        // Checked non-null above.
        unsafe { *matches_out = p.matches_prefix(query) };
        SUBETHA_OK
    })
}

/// Attach a caller tag and up to `SUBETHA_UMBRA_EXT_BYTES` of payload,
/// writing the changed pointer back into `pointer`.
///
/// A payload longer than that is refused and the pointer is left alone.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` bytes readable and
/// writable; `payload` points to at least `payload_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_set_ext(
    pointer: *mut u8,
    tag: u8,
    payload: *const u8,
    payload_len: u64,
) -> i32 {
    entry(|| {
        let mut p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let payload = match unsafe { bytes(payload, payload_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match p.set_ext(tag, payload) {
            Ok(()) => {
                unsafe { write_pointer(p, pointer) };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// The tag and payload `pointer` carries.
///
/// The payload bytes are handed over whatever the tag says; a caller that
/// has not checked the tag is reading bytes whose meaning nobody agreed.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` readable bytes;
/// `tag_out` is valid; `payload_out` points to at least
/// `SUBETHA_UMBRA_EXT_BYTES` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_get_ext(
    pointer: *const u8,
    tag_out: *mut u8,
    payload_out: *mut u8,
) -> i32 {
    entry(|| {
        if tag_out.is_null() || payload_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "tag_out or payload_out is null");
        }
        let p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let payload = *p.ext_payload();
        // Checked non-null above.
        unsafe {
            *tag_out = p.ext_tag();
            std::ptr::copy_nonoverlapping(payload.as_ptr(), payload_out, payload.len());
        }
        SUBETHA_OK
    })
}

/// Drop any caller tag and zero its payload, writing the changed pointer
/// back into `pointer`.
///
/// # Safety
/// `pointer` points to at least `SUBETHA_UMBRA_BYTES` bytes readable and
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_umbra_clear_ext(pointer: *mut u8) -> i32 {
    entry(|| {
        let mut p = match unsafe { read_pointer(pointer) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        p.clear_ext();
        unsafe { write_pointer(p, pointer) };
        SUBETHA_OK
    })
}
