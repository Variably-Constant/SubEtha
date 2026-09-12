//! A shared HyperLogLog through the C ABI: how many distinct items have
//! been seen, in memory that does not grow with the answer.
//!
//! The count is an estimate and cannot be anything else. The sketch
//! stores registers rather than members, so it can say roughly how many
//! distinct items it saw and can never say whether a particular one was
//! among them. A caller who needs membership wants the Bloom filter
//! beside it.
//!
//! Several processes insert into one sketch without a lock; each register
//! takes the maximum of what it held and what an insert implies, so
//! inserts commute and none is lost to a race.
//!
//! Strict and managed modes are the same here: a sketch runs nothing.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_hyper_log_log::{
    HLLError, SharedHyperLogLog, MAX_PRECISION, MIN_PRECISION,
};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_HLL};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The smallest precision the sketch accepts.
pub const SUBETHA_HLL_MIN_PRECISION: u32 = 4;
/// The largest. Registers, and so the file, grow as two to this power.
pub const SUBETHA_HLL_MAX_PRECISION: u32 = 16;

// Literals rather than `MIN_PRECISION as u32`, because the header
// generator copies the expression rather than its value and would emit a
// name C has never heard of. These assertions are what keeps the literals
// honest: the crate stops compiling if the layer below moves either bound.
const _: () = assert!(SUBETHA_HLL_MIN_PRECISION == MIN_PRECISION as u32);
const _: () = assert!(SUBETHA_HLL_MAX_PRECISION == MAX_PRECISION as u32);

/// A snapshot of a shared HyperLogLog.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_hll_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// The precision it was built with.
    pub precision: u32,
    /// Registers that precision implies, which is two to its power.
    pub n_registers: u32,
    /// Distinct items the registers imply. An estimate, and one that
    /// another process inserting during the read can move.
    pub estimated_distinct: u64,
}

pub(crate) struct HllObject {
    sketch: SharedHyperLogLog,
    mode: u32,
}

impl HllObject {
    /// A sketch parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: HLLError) -> i32 {
    match e {
        HLLError::InvalidPrecision => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("precision must be {MIN_PRECISION} to {MAX_PRECISION}"),
        ),
        HLLError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the sketch on disk was built with another precision",
        ),
        HLLError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_hll(handle: subetha_handle, f: impl FnOnce(&HllObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_HLL, |object| match object {
        Object::Hll(h) => f(h),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a HyperLogLog"),
    })
}

/// The checks every constructor makes.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    precision: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, u8, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    // Narrowed here so a precision far outside the range is refused with
    // the range in the message rather than wrapping into it.
    if !(SUBETHA_HLL_MIN_PRECISION..=SUBETHA_HLL_MAX_PRECISION).contains(&precision) {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!(
                "precision {precision} is outside {SUBETHA_HLL_MIN_PRECISION} to \
                 {SUBETHA_HLL_MAX_PRECISION}"
            ),
        ));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let mode = resolve_mode(mode)?;
    Ok((path, precision as u8, mode))
}

/// Create the sketch at `path`, or attach to one already there with the
/// same precision.
///
/// Precision decides both the accuracy and the size: the file holds two
/// to the `precision` registers, so raising it costs memory and lowers
/// the error.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hll_create(
    path: *const c_char,
    precision: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, precision, mode) =
            match unsafe { read_arguments(path, precision, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedHyperLogLog::create(path, precision) {
            Ok(s) => unsafe { issue(Object::Hll(HllObject { sketch: s, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a sketch another process created at `path`. The file must
/// exist and match `precision`, or the answer is
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hll_open(
    path: *const c_char,
    precision: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, precision, mode) =
            match unsafe { read_arguments(path, precision, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match SharedHyperLogLog::open(path, precision) {
            Ok(s) => unsafe { issue(Object::Hll(HllObject { sketch: s, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Record that `item` was seen.
///
/// Inserting the same item again changes nothing, which is what makes the
/// estimate a distinct count rather than a total. There is no answer to
/// return: the sketch cannot tell a caller whether this item was new.
///
/// # Safety
/// `item` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hll_insert(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
) -> i32 {
    with_hll(handle, |h| {
        let item = match unsafe { bytes(item, len) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        h.sketch.insert(item);
        SUBETHA_OK
    })
}

/// The number of distinct items the registers imply, into `out`.
///
/// An estimate. A sketch that has seen nothing answers zero, and one that
/// has seen a great many answers a number near but rarely equal to the
/// truth. Another process inserting during this read moves it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hll_estimate(handle: subetha_handle, out: *mut u64) -> i32 {
    with_hll(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = h.sketch.estimate() };
        SUBETHA_OK
    })
}

/// Return every register to zero, so the sketch reads as having seen
/// nothing.
///
/// This is visible to every process sharing the file, not only to this
/// handle: a peer mid-insert keeps whatever it writes next, and the
/// counts before the reset are gone for all of them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_hll_reset(handle: subetha_handle) -> i32 {
    with_hll(handle, |h| {
        h.sketch.reset();
        SUBETHA_OK
    })
}

/// Push the mapped bytes to disk and wait for them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_hll_flush(handle: subetha_handle) -> i32 {
    with_hll(handle, |h| match h.sketch.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Ask for the mapped bytes to reach disk without waiting.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_hll_flush_async(handle: subetha_handle) -> i32 {
    with_hll(handle, |h| match h.sketch.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of this sketch into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hll_read_stats(
    handle: subetha_handle,
    out: *mut subetha_hll_stats,
) -> i32 {
    with_hll(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_hll_stats {
            mode: h.mode,
            precision: u32::from(h.sketch.precision()),
            n_registers: h.sketch.n_registers(),
            estimated_distinct: h.sketch.estimate(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
