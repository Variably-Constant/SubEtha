//! The shared cell through the C ABI: one value in a file that many
//! processes read while one writes, at a size the caller declares.
//!
//! A write bumps the cell's version to odd, copies the bytes, and bumps it
//! to even; a read takes the version, copies, and takes it again, retrying
//! while the two differ. So a reader never sees half of one value and half
//! of another, without either side taking a lock. The version is also the
//! count of writes doubled, so a reader that only wants to know whether
//! anything changed can watch it instead of comparing values.
//!
//! One writer, any number of readers: two writers race, since the version
//! protocol makes a torn read detectable rather than a torn write safe.
//! The cell runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_cell::RawCell;
use subetha_cxc::shared_cell::{SharedCellError, PAYLOAD_BYTES};

use crate::error::{cell_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_CELL};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The most bytes one cell holds.
pub const SUBETHA_CELL_PAYLOAD_BYTES: usize = 52;

const _: () = assert!(SUBETHA_CELL_PAYLOAD_BYTES == PAYLOAD_BYTES);

/// A snapshot of a shared cell.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_cell_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Bytes the value takes.
    pub value_size: u64,
    /// Twice the writes the cell has taken, and odd while one is in
    /// flight.
    pub version: u64,
}

pub(crate) struct CellObject {
    cell: RawCell,
    mode: u32,
}

impl CellObject {
    /// A cell parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_cell_stats {
        subetha_cell_stats {
            mode: self.mode,
            value_size: self.cell.value_size() as u64,
            version: u64::from(self.cell.version()),
        }
    }
}

fn with_cell(handle: subetha_handle, f: impl FnOnce(&CellObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CELL, |object| match object {
        Object::Cell(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a cell"),
    })
}

/// A value size the region can hold: at least one byte, and no more than
/// one cell carries.
fn checked_size(value_size: u32) -> Result<usize, i32> {
    if value_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "value_size is zero"));
    }
    if value_size as usize > SUBETHA_CELL_PAYLOAD_BYTES {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("value_size {value_size} exceeds the {SUBETHA_CELL_PAYLOAD_BYTES} bytes one cell holds"),
        ));
    }
    Ok(value_size as usize)
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    value_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let value_size = checked_size(value_size)?;
    let mode = resolve_mode(mode)?;
    Ok((path, value_size, mode))
}

fn place(cell: Result<RawCell, SharedCellError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match cell {
        Ok(cell) => unsafe { issue(Object::Cell(CellObject { cell, mode }), out) },
        Err(e) => cell_code(e),
    }
}

/// Obtain the cell at `path` for a `value_size`-byte value: an empty one
/// is initialized when the file does not exist, an existing one is
/// attached with its value and version in place, so a peer that is already
/// using it is never reset out from under. The size is recorded in the
/// region, so a handle that declares another is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`, and a cell of four bytes is the same
/// region as a Rust `SharedCell<u32>`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_create(path: *const c_char, value_size: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, value_size, mode) = match unsafe { read_arguments(path, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawCell::create(path, value_size), mode, out)
    })
}

/// Attach to the cell another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_open(path: *const c_char, value_size: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, value_size, mode) = match unsafe { read_arguments(path, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawCell::open(path, value_size), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty cell there,
/// discarding the value other handles share. On Windows the file must not
/// be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_reset(path: *const c_char, value_size: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, value_size, mode) = match unsafe { read_arguments(path, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawCell::reset(path, value_size), mode, out)
    })
}

/// Replace the value with `data`, exactly `value_size` bytes. One writer:
/// two processes writing at once race, and serializing them is the
/// caller's contract.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_set(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_cell(handle, |c| {
        let data = match unsafe { bytes(data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        if data.len() != c.cell.value_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a value is {} bytes, not {}", c.cell.value_size(), data.len()),
            );
        }
        match c.cell.set(data) {
            Ok(()) => SUBETHA_OK,
            Err(e) => cell_code(e),
        }
    })
}

/// Copy the value into `out`, at least `value_size` bytes, and its length
/// into `out_len`. Retries while a write is in flight, so it never yields
/// half of one value and half of another.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_get(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_cell(handle, |c| {
        let buf = match unsafe { out_buffer(out, cap, out_len, c.cell.value_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match c.cell.get(buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = c.cell.value_size() };
                SUBETHA_OK
            }
            Err(e) => cell_code(e),
        }
    })
}

/// The cell's version into `out`: twice the writes it has taken, and odd
/// while one is in flight. A reader that only wants to know whether the
/// value changed watches this rather than comparing bytes.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_version(handle: subetha_handle, out: *mut u32) -> i32 {
    with_cell(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = c.cell.version() };
        SUBETHA_OK
    })
}

/// A snapshot of the cell into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_read_stats(handle: subetha_handle, out: *mut subetha_cell_stats) -> i32 {
    with_cell(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = c.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the cell's page to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_cell_flush(handle: subetha_handle) -> i32 {
    with_cell(handle, |c| match c.cell.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => cell_code(e),
    })
}

/// Remove the cell's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_cell_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-cell-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the cell's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_carries_the_size_the_caller_declared_and_counts_its_writes() {
        let scratch = Scratch::new("shape");
        let object = CellObject { cell: RawCell::create(&scratch.0, 8).unwrap(), mode: SUBETHA_MODE_STRICT };
        let stats = object.stats();
        assert_eq!(stats.value_size, 8);
        assert_eq!(stats.version, 0, "nothing written yet");
        object.cell.set(&[7u8; 8]).unwrap();
        assert_eq!(object.stats().version, 2, "one write, counted twice");
        let mut out = [0u8; 8];
        object.cell.get(&mut out).unwrap();
        assert_eq!(out, [7u8; 8]);
        drop(object);
        assert_eq!(checked_size(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_size(53).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_size(52).unwrap(), 52);
        assert_eq!(checked_size(1).unwrap(), 1);
    }
}
