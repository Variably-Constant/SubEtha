//! A shared bit vector through the C ABI: a file-backed array of bits
//! several processes set, clear and count without a lock.
//!
//! Every operation is on one bit or one range, and each is atomic on its
//! own. A count taken while another process is writing is a real count of
//! a moment that passed, not a snapshot held still.
//!
//! Strict and managed modes are the same here: a bit vector runs nothing.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_bit_vec::{BitVecError, SharedBitVec};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_BIT_VEC};
use crate::ring::text;
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared bit vector.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_bit_vec_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Bits the vector holds.
    pub capacity_bits: u64,
    /// Machine words those bits occupy.
    pub capacity_words: u64,
    /// Bits set when this was read. Another process writing during the
    /// count moves it; the number is a real count of a moment that has
    /// passed rather than a held snapshot.
    pub ones: u64,
    /// Bits clear on the same reading, by the same caution.
    pub zeros: u64,
}

pub(crate) struct BitVecObject {
    bits: SharedBitVec,
    mode: u32,
}

impl BitVecObject {
    /// A bit vector parks nothing inside a call, so a destroy has nobody
    /// to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: BitVecError) -> i32 {
    match e {
        BitVecError::OutOfBounds => {
            fail(SUBETHA_E_OUT_OF_BOUNDS, "the index is past the end of the vector")
        }
        BitVecError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the vector on disk was built with another capacity",
        ),
        BitVecError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_bits(handle: subetha_handle, f: impl FnOnce(&BitVecObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_BIT_VEC, |object| match object {
        Object::BitVec(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a bit vector"),
    })
}

/// The checks every constructor makes.
///
/// A capacity of zero is refused here rather than passed down: the layer
/// below asserts on it, which would reach the caller as a caught panic
/// and a poisoned handle instead of an argument they can correct.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity_bits: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    if capacity_bits == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity_bits is zero"));
    }
    let bits = usize::try_from(capacity_bits).map_err(|_| {
        fail(SUBETHA_E_INVALID_ARGUMENT, format!("capacity_bits {capacity_bits} exceeds this host"))
    })?;
    let path = Path::new(unsafe { text(path, "path") }?);
    let mode = resolve_mode(mode)?;
    Ok((path, bits, mode))
}

/// Create the bit vector at `path`, or attach to one already there with
/// the same capacity. Every bit starts clear.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_create(
    path: *const c_char,
    capacity_bits: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, bits, mode) = match unsafe { read_arguments(path, capacity_bits, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedBitVec::create(path, bits) {
            Ok(b) => unsafe { issue(Object::BitVec(BitVecObject { bits: b, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Truncate the file at `path` and initialize an all-clear vector,
/// discarding the bits live peers share. For a caller that knows it owns
/// the path.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_reset(
    path: *const c_char,
    capacity_bits: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, bits, mode) = match unsafe { read_arguments(path, capacity_bits, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedBitVec::reset(path, bits) {
            Ok(b) => unsafe { issue(Object::BitVec(BitVecObject { bits: b, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a vector another process created at `path`. The file must
/// exist and must be at least as large as `capacity_bits` needs, or the
/// answer is `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_open(
    path: *const c_char,
    capacity_bits: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, bits, mode) = match unsafe { read_arguments(path, capacity_bits, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedBitVec::open(path, bits) {
            Ok(b) => unsafe { issue(Object::BitVec(BitVecObject { bits: b, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Set bit `index`, writing whether it had already been set into `was`.
///
/// The previous value is what makes this usable as a claim: two processes
/// setting the same bit both succeed, and exactly one is told it was the
/// one that changed it. `was` may be null when that is not wanted.
///
/// This and `subetha_bit_vec_clear` report the value from before the
/// call. `subetha_bit_vec_toggle` reports the value after it.
///
/// # Safety
/// `was` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_set(
    handle: subetha_handle,
    index: u64,
    was: *mut bool,
) -> i32 {
    with_bits(handle, |b| match index_of(index) {
        Ok(i) => match b.bits.set(i) {
            Ok(prev) => {
                if !was.is_null() {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *was = prev };
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        },
        Err(code) => code,
    })
}

/// Clear bit `index`, writing whether it had been set into `was`.
///
/// # Safety
/// `was` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_clear(
    handle: subetha_handle,
    index: u64,
    was: *mut bool,
) -> i32 {
    with_bits(handle, |b| match index_of(index) {
        Ok(i) => match b.bits.clear(i) {
            Ok(prev) => {
                if !was.is_null() {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *was = prev };
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        },
        Err(code) => code,
    })
}

/// Flip bit `index`, writing the value it now holds into `now`.
///
/// The parameter is `now` and not `was` because this one differs from its
/// two siblings: `subetha_bit_vec_set` and `subetha_bit_vec_clear` report
/// the value from before the call, and this reports the value after it.
/// A caller using all three must not read them alike.
///
/// # Safety
/// `now` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_toggle(
    handle: subetha_handle,
    index: u64,
    now: *mut bool,
) -> i32 {
    with_bits(handle, |b| match index_of(index) {
        Ok(i) => match b.bits.toggle(i) {
            Ok(new) => {
                if !now.is_null() {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *now = new };
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        },
        Err(code) => code,
    })
}

/// Read bit `index` into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_get(
    handle: subetha_handle,
    index: u64,
    out: *mut bool,
) -> i32 {
    with_bits(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match index_of(index) {
            Ok(i) => match b.bits.get(i) {
                Ok(v) => {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *out = v };
                    SUBETHA_OK
                }
                Err(e) => code_for(e),
            },
            Err(code) => code,
        }
    })
}

/// Set every bit from `lo` up to but not including `hi`.
///
/// The range is not one atomic step: a reader during it sees some of the
/// bits changed and some not. Only the individual bits are atomic.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bit_vec_set_range(handle: subetha_handle, lo: u64, hi: u64) -> i32 {
    with_bits(handle, |b| match range_of(lo, hi) {
        Ok((lo, hi)) => match b.bits.set_range(lo, hi) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        },
        Err(code) => code,
    })
}

/// Clear every bit from `lo` up to but not including `hi`, with the same
/// caution about the range not being one step.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bit_vec_clear_range(handle: subetha_handle, lo: u64, hi: u64) -> i32 {
    with_bits(handle, |b| match range_of(lo, hi) {
        Ok((lo, hi)) => match b.bits.clear_range(lo, hi) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        },
        Err(code) => code,
    })
}

/// Set every bit in the vector.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bit_vec_set_all(handle: subetha_handle) -> i32 {
    with_bits(handle, |b| {
        b.bits.set_all();
        SUBETHA_OK
    })
}

/// Clear every bit in the vector.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bit_vec_clear_all(handle: subetha_handle) -> i32 {
    with_bits(handle, |b| {
        b.bits.clear_all();
        SUBETHA_OK
    })
}

/// Push the mapped bytes to disk and wait for them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bit_vec_flush(handle: subetha_handle) -> i32 {
    with_bits(handle, |b| match b.bits.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Ask for the mapped bytes to reach disk without waiting.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_bit_vec_flush_async(handle: subetha_handle) -> i32 {
    with_bits(handle, |b| match b.bits.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of this vector into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_bit_vec_read_stats(
    handle: subetha_handle,
    out: *mut subetha_bit_vec_stats,
) -> i32 {
    with_bits(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let ones = b.bits.count_ones() as u64;
        let stats = subetha_bit_vec_stats {
            mode: b.mode,
            capacity_bits: b.bits.capacity_bits() as u64,
            capacity_words: b.bits.capacity_words() as u64,
            ones,
            // Derived from the same count rather than counted again, so
            // the pair always sums to the capacity. Counting twice could
            // straddle a writer and return a pair that does not.
            zeros: b.bits.capacity_bits() as u64 - ones,
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// An index the host can hold, or the refusal for one it cannot.
fn index_of(index: u64) -> Result<usize, i32> {
    usize::try_from(index).map_err(|_| {
        fail(SUBETHA_E_OUT_OF_BOUNDS, format!("index {index} exceeds this host's range"))
    })
}

/// A range the host can hold, refusing an inverted one by name rather
/// than letting it read as empty.
fn range_of(lo: u64, hi: u64) -> Result<(usize, usize), i32> {
    if hi < lo {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("the range {lo}..{hi} ends before it starts"),
        ));
    }
    Ok((index_of(lo)?, index_of(hi)?))
}
