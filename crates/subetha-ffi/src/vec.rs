//! The shared vec through the C ABI: a bounded, indexable, append-only
//! sequence in a file any number of processes push to, read by index and
//! pop from, at an element layout the caller declares in
//! `subetha_element_layout`, stored in the region and checked at every
//! attach. A push reserves a slot, writes it under the slot's SeqLock and
//! publishes its index once every earlier reservation has; a get reads
//! under the same lock, so it never sees a torn element. The vec runs no
//! background work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_treiber_stack::ElementLayout;
use subetha_cxc::raw_vec::RawVec;
use subetha_cxc::shared_vec::VecError;

use crate::error::{
    fail, vec_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_READ_ONLY, SUBETHA_E_RING_EMPTY,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::batch::{run_pop_many_indexed, run_push_many_reporting};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_VEC};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::stack::{read_layout, subetha_element_layout};

/// A snapshot of a shared vec.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_vec_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Whether this handle may push, pop, set and clear; false for one
    /// opened read-only.
    pub writable: bool,
    /// Elements the vec can hold.
    pub capacity: u64,
    /// Elements pushed and published.
    pub len: u64,
    /// Bytes per element.
    pub element_size: u64,
    /// The element alignment the layout declares.
    pub alignment: u64,
    /// The layout tag the vec was created with.
    pub tag: u64,
    /// Bytes from one slot to the next in the region.
    pub slot_stride: u64,
}

pub(crate) struct VecObject {
    vec: RawVec,
    mode: u32,
}

impl VecObject {
    /// A vec parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}

    fn element_size(&self) -> usize {
        self.vec.layout().slot_size
    }

    fn stats(&self) -> subetha_vec_stats {
        let layout = self.vec.layout();
        subetha_vec_stats {
            mode: self.mode,
            writable: self.vec.is_writable(),
            capacity: self.vec.capacity() as u64,
            len: self.vec.len() as u64,
            element_size: layout.slot_size as u64,
            alignment: layout.alignment as u64,
            tag: layout.tag,
            slot_stride: self.vec.slot_stride() as u64,
        }
    }
}

fn with_vec(handle: subetha_handle, f: impl FnOnce(&VecObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_VEC, |object| match object {
        Object::Vec(v) => f(v),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a vec"),
    })
}

/// A vec capacity: at least one element.
fn vec_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    Ok(capacity as usize)
}

/// An index argument as the vec addresses it.
fn index_of(index: u64) -> Result<usize, i32> {
    usize::try_from(index).map_err(|e| fail(SUBETHA_E_OUT_OF_BOUNDS, format!("index {index} does not fit this platform: {e}")))
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, ElementLayout, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let capacity = vec_capacity(capacity)?;
    let layout = unsafe { read_layout(layout) }?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, layout, mode))
}

fn place(vec: Result<RawVec, VecError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match vec {
        Ok(vec) => unsafe { issue(Object::Vec(VecObject { vec, mode }), out) },
        Err(e) => vec_code(e),
    }
}

/// Obtain the vec at `path` for `capacity` elements of `layout`: an empty
/// one is initialized when the file does not exist, an existing one is
/// attached with its elements in place. A file built with another
/// capacity or layout is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_create(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, mode) = match unsafe { read_arguments(path, capacity, layout, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawVec::create(path, capacity, layout), mode, out)
    })
}

/// Attach to the vec another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_open(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, mode) = match unsafe { read_arguments(path, capacity, layout, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawVec::open(path, capacity, layout), mode, out)
    })
}

/// Attach to the vec at `path` with read access alone, for a process that
/// may not write the file. `get` and the stats behave as on any handle;
/// `push_back`, `pop_back`, `set` and `clear` return `SUBETHA_E_READ_ONLY`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_open_read_only(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, mode) = match unsafe { read_arguments(path, capacity, layout, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawVec::open_read_only(path, capacity, layout), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty vec there,
/// discarding every element other handles share. On Windows the file must
/// not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_reset(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, mode) = match unsafe { read_arguments(path, capacity, layout, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawVec::reset(path, capacity, layout), mode, out)
    })
}

/// An element argument of exactly the vec's element size.
///
/// # Safety
/// `data` points to `len` readable bytes.
unsafe fn element_of<'a>(v: &VecObject, data: *const u8, len: usize) -> Result<&'a [u8], i32> {
    let data = unsafe { bytes(data, len) }?;
    if data.len() != v.element_size() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("an element is {} bytes, not {}", v.element_size(), data.len()),
        ));
    }
    Ok(data)
}

/// Append one element, exactly `element_size` bytes, and write the index
/// it landed at into `out_index` when that is not null.
/// `SUBETHA_E_RING_FULL` when the vec is at capacity.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_index` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_push_back(handle: subetha_handle, data: *const u8, len: usize, out_index: *mut u64) -> i32 {
    with_vec(handle, |v| {
        let data = match unsafe { element_of(v, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match v.vec.push_back(data) {
            Ok(index) => {
                if !out_index.is_null() {
                    // SAFETY: checked non-null; the caller guarantees it is writable.
                    unsafe { *out_index = index as u64 };
                }
                SUBETHA_OK
            }
            Err(e) => vec_code(e),
        }
    })
}

/// Append `count` elements from the caller's own array, each exactly
/// `element_size` bytes at `items + i * stride`, and write the index each
/// landed at into `out_indices`. One handle lookup and one panic guard
/// for the run.
///
/// The indices are reported rather than left to be worked out: another
/// process may append between two of these calls, so the caller cannot
/// assume its elements are consecutive from the length it read before.
/// The batch stops at the first refusal and reports how many landed, so
/// `out_indices` holds `out_done` of them.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_indices`
/// addresses `count` writable `uint64_t`s; `out_done` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_push_back_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_indices: *mut u64,
    out_done: *mut usize,
) -> i32 {
    with_vec(handle, |v| {
        if len != v.element_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("an element is {} bytes, not {len}", v.element_size()),
            );
        }
        unsafe {
            run_push_many_reporting(items, stride, len, count, out_indices, out_done, |element| {
                match v.vec.push_back(element) {
                    Ok(index) => Ok(index as u64),
                    Err(e) => Err(vec_code(e)),
                }
            })
        }
    })
}

/// Read `count` elements into the caller's own array, the element at
/// `indices[i]` into `out + i * stride`. One handle lookup and one panic
/// guard for the run; the batch stops at the first index the vec refuses
/// and reports how many it read.
///
/// # Safety
/// `indices` addresses `count` readable `uint64_t`s; `out` addresses
/// `count * stride` writable bytes; `out_done` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_get_many(
    handle: subetha_handle,
    indices: *const u64,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_vec(handle, |v| {
        if count != 0 && indices.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "indices is null with a non-zero count");
        }
        let size = v.element_size();
        // Defined outside the `unsafe` below so its own block is the one
        // carrying the pointer read, rather than being swallowed by an
        // enclosing block that covers the call as well.
        let read = |i: usize, slot: &mut [u8]| {
            // SAFETY: `i` is below `count`, which the caller guarantees
            // is readable at `indices`.
            let index = unsafe { *indices.add(i) };
            match v.vec.get(index as usize, &mut slot[..size]) {
                Ok(true) => SUBETHA_OK,
                // An index past the length stops the run rather than
                // leaving a slot the caller cannot tell from an element:
                // `out_done` is how many it filled, and every one of
                // those is a real element.
                Ok(false) => fail(
                    SUBETHA_E_OUT_OF_BOUNDS,
                    format!("index {index} is at or past the length {}", v.vec.len()),
                ),
                Err(e) => vec_code(e),
            }
        };
        // SAFETY: the caller guarantees `out` spans `count * stride`.
        unsafe { run_pop_many_indexed(out, stride, size, count, out_done, read) }
    })
}

/// Remove the last element into `out`, at least `element_size` bytes, its
/// length into `out_len`. `SUBETHA_E_RING_EMPTY` when the vec is empty.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_pop_back(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_vec(handle, |v| {
        let buf = match unsafe { out_buffer(out, cap, out_len, v.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match v.vec.pop_back(buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = v.element_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_RING_EMPTY,
            Err(e) => vec_code(e),
        }
    })
}

/// Copy the element at `index` into `out`, at least `element_size` bytes,
/// its length into `out_len`. `SUBETHA_E_OUT_OF_BOUNDS` when `index` is at
/// or past the length.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_get(handle: subetha_handle, index: u64, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_vec(handle, |v| {
        let index = match index_of(index) {
            Ok(i) => i,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, v.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match v.vec.get(index, buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = v.element_size() };
                SUBETHA_OK
            }
            Ok(false) => fail(SUBETHA_E_OUT_OF_BOUNDS, format!("index {index} is at or past the length {}", v.vec.len())),
            Err(e) => vec_code(e),
        }
    })
}

/// Overwrite the element at `index`, below the length, with `data`,
/// exactly `element_size` bytes. `SUBETHA_E_OUT_OF_BOUNDS` when `index` is
/// at or past the length.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_set(handle: subetha_handle, index: u64, data: *const u8, len: usize) -> i32 {
    with_vec(handle, |v| {
        let index = match index_of(index) {
            Ok(i) => i,
            Err(code) => return code,
        };
        let data = match unsafe { element_of(v, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match v.vec.set(index, data) {
            Ok(()) => SUBETHA_OK,
            Err(VecError::OutOfBounds) => {
                fail(SUBETHA_E_OUT_OF_BOUNDS, format!("index {index} is at or past the length {}", v.vec.len()))
            }
            Err(e) => vec_code(e),
        }
    })
}

/// Empty the vec for every handle: the length goes to zero and the next
/// push lands at index zero. Slot bytes stay in place, unreachable by
/// index. Not safe against a push or a pop running in any process.
/// `SUBETHA_E_READ_ONLY` on a handle opened read-only.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_vec_clear(handle: subetha_handle) -> i32 {
    with_vec(handle, |v| match v.vec.clear() {
        Ok(()) => SUBETHA_OK,
        Err(VecError::ReadOnly) => fail(SUBETHA_E_READ_ONLY, "the vec was opened read-only"),
        Err(e) => vec_code(e),
    })
}

/// A snapshot of the vec into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_read_stats(handle: subetha_handle, out: *mut subetha_vec_stats) -> i32 {
    with_vec(handle, |v| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = v.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the vec's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_vec_flush(handle: subetha_handle) -> i32 {
    with_vec(handle, |v| match v.vec.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => vec_code(e),
    })
}

/// Remove the vec file at `path`. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vec_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-vec-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the vec file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_shape_and_the_layout_it_opened_with() {
        let scratch = Scratch::new("shape");
        let layout = ElementLayout { slot_size: 24, alignment: 8, tag: 5 };
        let vec = RawVec::create(&scratch.0, 16, layout).unwrap();
        let object = VecObject { vec, mode: SUBETHA_MODE_STRICT };
        let stats = object.stats();
        assert_eq!(stats.capacity, 16);
        assert_eq!(stats.len, 0);
        assert_eq!(stats.element_size, 24);
        assert_eq!(stats.alignment, 8);
        assert_eq!(stats.tag, 5);
        assert_eq!(stats.slot_stride, 64);
        assert!(stats.writable);
        assert_eq!(object.vec.push_back(&[1u8; 24]).unwrap(), 0);
        assert_eq!(object.stats().len, 1);
        drop(object);
        assert_eq!(vec_capacity(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(vec_capacity(3).unwrap(), 3);
        assert_eq!(index_of(7).unwrap(), 7);
    }
}
