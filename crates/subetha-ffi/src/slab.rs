//! The shared slab through the C ABI: a fixed-capacity array of records
//! in a file, each slot its own SeqLock cell, addressed by an index the
//! caller chooses, at a record layout the caller declares in
//! `subetha_element_layout`, stored in the region and checked at every
//! attach. There is no length, allocator or free list: a slot nothing has
//! written reads as zeros, and a released index never addresses another
//! record. The slab runs no background work, so strict and managed modes
//! are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_slab::RawSlab;
use subetha_cxc::raw_treiber_stack::ElementLayout;
use subetha_cxc::shared_slab::SlabError;

use crate::error::{fail, slab_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_SLAB};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::stack::{read_layout, subetha_element_layout};

/// A snapshot of a shared slab.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_slab_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Whether this handle may set; false for one opened read-only.
    pub writable: bool,
    /// Slots the slab addresses.
    pub capacity: u64,
    /// Bytes per record.
    pub element_size: u64,
    /// The record alignment the layout declares.
    pub alignment: u64,
    /// The layout tag the slab was created with.
    pub tag: u64,
    /// Bytes from one slot to the next in the region.
    pub slot_stride: u64,
}

pub(crate) struct SlabObject {
    slab: RawSlab,
    mode: u32,
}

impl SlabObject {
    /// A slab parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}

    fn element_size(&self) -> usize {
        self.slab.layout().slot_size
    }

    fn stats(&self) -> subetha_slab_stats {
        let layout = self.slab.layout();
        subetha_slab_stats {
            mode: self.mode,
            writable: self.slab.is_writable(),
            capacity: self.slab.capacity() as u64,
            element_size: layout.slot_size as u64,
            alignment: layout.alignment as u64,
            tag: layout.tag,
            slot_stride: self.slab.slot_stride() as u64,
        }
    }
}

fn with_slab(handle: subetha_handle, f: impl FnOnce(&SlabObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_SLAB, |object| match object {
        Object::Slab(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a slab"),
    })
}

/// A slab capacity: at least one slot.
fn slab_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    Ok(capacity as usize)
}

/// An index argument as the slab addresses it.
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
    let capacity = slab_capacity(capacity)?;
    let layout = unsafe { read_layout(layout) }?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, layout, mode))
}

fn place(slab: Result<RawSlab, SlabError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match slab {
        Ok(slab) => unsafe { issue(Object::Slab(SlabObject { slab, mode }), out) },
        Err(e) => slab_code(e),
    }
}

/// Obtain the slab at `path` for `capacity` records of `layout`: an empty
/// one is initialized when the file does not exist, an existing one is
/// attached with its records in place. A file built with another capacity
/// or layout is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_create(
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
        place(RawSlab::create(path, capacity, layout), mode, out)
    })
}

/// Attach to the slab another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_open(
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
        place(RawSlab::open(path, capacity, layout), mode, out)
    })
}

/// Attach to the slab at `path` with read access alone, for a process that
/// may not write the file. `get`, `slot_version` and the stats behave as
/// on any handle; `set` returns `SUBETHA_E_READ_ONLY`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_open_read_only(
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
        place(RawSlab::open_read_only(path, capacity, layout), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty slab there,
/// discarding every record other handles share. On Windows the file must
/// not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_reset(
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
        place(RawSlab::reset(path, capacity, layout), mode, out)
    })
}

/// Copy the record at `index` into `out`, at least `element_size` bytes,
/// its length into `out_len`; a slot nothing has written reads as zeros.
/// The copy is taken under the slot's SeqLock, so it is never torn.
/// `SUBETHA_E_OUT_OF_BOUNDS` when `index` is at or past the capacity.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_get(handle: subetha_handle, index: u64, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_slab(handle, |s| {
        let index = match index_of(index) {
            Ok(i) => i,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, s.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match s.slab.get(index, buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = s.element_size() };
                SUBETHA_OK
            }
            Err(e) => slab_code(e),
        }
    })
}

/// Write `data`, exactly `element_size` bytes, at `index` under the slot's
/// SeqLock. One writer per slot: two writers on the same slot race, and
/// serializing them is the caller's contract. `SUBETHA_E_OUT_OF_BOUNDS`
/// when `index` is at or past the capacity.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_set(handle: subetha_handle, index: u64, data: *const u8, len: usize) -> i32 {
    with_slab(handle, |s| {
        let index = match index_of(index) {
            Ok(i) => i,
            Err(code) => return code,
        };
        let data = match unsafe { bytes(data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        if data.len() != s.element_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a record is {} bytes, not {}", s.element_size(), data.len()),
            );
        }
        match s.slab.set(index, data) {
            Ok(()) => SUBETHA_OK,
            Err(e) => slab_code(e),
        }
    })
}

/// The version word of slot `index` into `out`: twice the writes the slot
/// has taken, odd while a writer holds it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_slot_version(handle: subetha_handle, index: u64, out: *mut u32) -> i32 {
    with_slab(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let index = match index_of(index) {
            Ok(i) => i,
            Err(code) => return code,
        };
        match s.slab.slot_version(index) {
            Ok(version) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = version };
                SUBETHA_OK
            }
            Err(e) => slab_code(e),
        }
    })
}

/// A snapshot of the slab into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_read_stats(handle: subetha_handle, out: *mut subetha_slab_stats) -> i32 {
    with_slab(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = s.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the slab's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_slab_flush(handle: subetha_handle) -> i32 {
    with_slab(handle, |s| match s.slab.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => slab_code(e),
    })
}

/// Remove the slab file at `path`. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_slab_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-slab-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the slab file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_shape_and_the_layout_it_opened_with() {
        let scratch = Scratch::new("shape");
        let layout = ElementLayout { slot_size: 100, alignment: 8, tag: 3 };
        let slab = RawSlab::create(&scratch.0, 8, layout).unwrap();
        let object = SlabObject { slab, mode: SUBETHA_MODE_STRICT };
        let stats = object.stats();
        assert_eq!(stats.capacity, 8);
        assert_eq!(stats.element_size, 100);
        assert_eq!(stats.alignment, 8);
        assert_eq!(stats.tag, 3);
        assert_eq!(stats.slot_stride, 128);
        assert!(stats.writable);
        object.slab.set(7, &[9u8; 100]).unwrap();
        assert_eq!(object.slab.slot_version(7).unwrap(), 2);
        drop(object);
        assert_eq!(slab_capacity(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(slab_capacity(3).unwrap(), 3);
        assert_eq!(index_of(7).unwrap(), 7);
    }
}
