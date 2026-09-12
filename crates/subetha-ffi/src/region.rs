//! The shared region through the C ABI: a slot arena in a file that any
//! number of processes allocate from and free to, at an element layout the
//! caller declares in `subetha_element_layout`, stored in the region and
//! checked at every attach. A slot is named by a 32-bit index that
//! resolves to the same bytes in every process, so a pointer-bearing
//! structure built on the region travels between them. The allocator is a
//! free list with an ABA counter in front of a bump cursor; a freed slot
//! is handed out again, so whether an index is still allocated is the
//! caller's knowledge. Reads and writes of a slot are plain copies: a
//! reader racing a writer on one slot sees a mix of old and new bytes,
//! and a caller that needs a torn read detected uses the slab. The region
//! runs no background work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_region::RawRegion;
use subetha_cxc::raw_treiber_stack::ElementLayout;
use subetha_cxc::shared_region::{RegionError, NIL_INDEX};

use crate::error::{fail, region_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_REGION};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::stack::{read_layout, subetha_element_layout};

/// The index that names no slot; every other index below the capacity
/// names one.
pub const SUBETHA_REGION_NIL_INDEX: u32 = 0xFFFF_FFFF;

const _: () = assert!(SUBETHA_REGION_NIL_INDEX == NIL_INDEX);

/// A snapshot of a shared region.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_region_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the region holds.
    pub capacity: u64,
    /// Slots allocated right now; allocations and frees race this.
    pub len: u64,
    /// Slots on the free list, walked.
    pub free_count: u64,
    /// Bytes per element.
    pub element_size: u64,
    /// The element alignment the layout declares.
    pub alignment: u64,
    /// The layout tag the region was created with.
    pub tag: u64,
    /// Where the slot array starts in the file.
    pub slots_offset: u64,
}

pub(crate) struct RegionObject {
    region: RawRegion,
    mode: u32,
}

impl RegionObject {
    /// A region parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn element_size(&self) -> usize {
        self.region.layout().slot_size
    }

    fn stats(&self) -> subetha_region_stats {
        let layout = self.region.layout();
        subetha_region_stats {
            mode: self.mode,
            capacity: self.region.capacity() as u64,
            len: self.region.len() as u64,
            free_count: self.region.free_count() as u64,
            element_size: layout.slot_size as u64,
            alignment: layout.alignment as u64,
            tag: layout.tag,
            slots_offset: self.region.slots_offset() as u64,
        }
    }
}

impl RegionObject {
    /// The region itself, for a family that addresses one without owning
    /// a handle of its own. The umbra pointer is such a family: it is
    /// sixteen bytes the caller holds, so it borrows the region and never
    /// a second handle.
    pub(crate) fn raw(&self) -> &RawRegion {
        &self.region
    }
}

pub(crate) fn with_region(handle: subetha_handle, f: impl FnOnce(&RegionObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_REGION, |object| match object {
        Object::Region(r) => f(r),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a region"),
    })
}

/// A region capacity: at least one slot, below the index that names none.
fn region_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    if capacity == SUBETHA_REGION_NIL_INDEX {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("capacity {capacity} is the index that names no slot"),
        ));
    }
    Ok(capacity as usize)
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
    let capacity = region_capacity(capacity)?;
    let layout = unsafe { read_layout(layout) }?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, layout, mode))
}

fn place(region: Result<RawRegion, RegionError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match region {
        Ok(region) => unsafe { issue(Object::Region(RegionObject { region, mode }), out) },
        Err(e) => region_code(e),
    }
}

/// Obtain the region at `path` for `capacity` elements of `layout`: an
/// empty one is initialized when the file does not exist, an existing one
/// is attached with its allocated slots and free list in place, so indexes
/// other processes hold keep resolving. A file built with another capacity
/// or layout is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_create(
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
        place(RawRegion::create(path, capacity, layout), mode, out)
    })
}

/// Attach to the region another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_open(
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
        place(RawRegion::open(path, capacity, layout), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty region there,
/// invalidating every index other handles hold. On Windows the file must
/// not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_reset(
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
        place(RawRegion::reset(path, capacity, layout), mode, out)
    })
}

/// An element argument of exactly the region's element size.
///
/// # Safety
/// `data` points to `len` readable bytes.
unsafe fn element_of<'a>(r: &RegionObject, data: *const u8, len: usize) -> Result<&'a [u8], i32> {
    let data = unsafe { bytes(data, len) }?;
    if data.len() != r.element_size() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("an element is {} bytes, not {}", r.element_size(), data.len()),
        ));
    }
    Ok(data)
}

/// Allocate a slot holding `data`, exactly `element_size` bytes, and write
/// its index into `out_index`. `SUBETHA_E_RING_FULL` when the free list
/// and the bump cursor are both exhausted.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_index` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_allocate(handle: subetha_handle, data: *const u8, len: usize, out_index: *mut u32) -> i32 {
    with_region(handle, |r| {
        if out_index.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_index is null");
        }
        let data = match unsafe { element_of(r, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match r.region.allocate(data) {
            Ok(index) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_index = index };
                SUBETHA_OK
            }
            Err(e) => region_code(e),
        }
    })
}

/// Free the slot at `index`, copying the element it held into `out`, at
/// least `element_size` bytes, and its length into `out_len`. The slot
/// returns to the free list and is handed out again.
/// `SUBETHA_E_OUT_OF_BOUNDS` for `SUBETHA_REGION_NIL_INDEX` or an index at
/// or past the capacity.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_free(handle: subetha_handle, index: u32, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_region(handle, |r| {
        let buf = match unsafe { out_buffer(out, cap, out_len, r.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match r.region.free(index, buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = r.element_size() };
                SUBETHA_OK
            }
            Err(e) => region_code(e),
        }
    })
}

/// Copy the element at `index` into `out`, at least `element_size` bytes,
/// and its length into `out_len`. Whether the slot is still allocated is
/// the caller's knowledge. `SUBETHA_E_OUT_OF_BOUNDS` for
/// `SUBETHA_REGION_NIL_INDEX` or an index at or past the capacity.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_get(handle: subetha_handle, index: u32, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_region(handle, |r| {
        let buf = match unsafe { out_buffer(out, cap, out_len, r.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match r.region.get(index, buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = r.element_size() };
                SUBETHA_OK
            }
            Err(e) => region_code(e),
        }
    })
}

/// Overwrite the element at `index` with `data`, exactly `element_size`
/// bytes. `SUBETHA_E_OUT_OF_BOUNDS` for `SUBETHA_REGION_NIL_INDEX` or an
/// index at or past the capacity.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_set(handle: subetha_handle, index: u32, data: *const u8, len: usize) -> i32 {
    with_region(handle, |r| {
        let data = match unsafe { element_of(r, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match r.region.set(index, data) {
            Ok(()) => SUBETHA_OK,
            Err(e) => region_code(e),
        }
    })
}

/// Empty the region for every handle: the bump cursor goes back to zero
/// and the free list to none, so every index handed out is stale. Not safe
/// against an allocate or a free running in any process.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_region_clear(handle: subetha_handle) -> i32 {
    with_region(handle, |r| {
        r.region.clear();
        SUBETHA_OK
    })
}

/// A snapshot of the region into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_read_stats(handle: subetha_handle, out: *mut subetha_region_stats) -> i32 {
    with_region(handle, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = r.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the region's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_region_flush(handle: subetha_handle) -> i32 {
    with_region(handle, |r| match r.region.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => region_code(e),
    })
}

/// Remove the region file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_region_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-region-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the region file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_shape_and_its_free_list() {
        let scratch = Scratch::new("shape");
        let layout = ElementLayout { slot_size: 24, alignment: 8, tag: 9 };
        let region = RawRegion::create(&scratch.0, 4, layout).unwrap();
        let object = RegionObject { region, mode: SUBETHA_MODE_STRICT };
        let stats = object.stats();
        assert_eq!(stats.capacity, 4);
        assert_eq!(stats.len, 0);
        assert_eq!(stats.element_size, 24);
        assert_eq!(stats.alignment, 8);
        assert_eq!(stats.tag, 9);
        assert_eq!(stats.slots_offset, 80);
        let index = object.region.allocate(&[3u8; 24]).unwrap();
        assert_eq!(object.stats().len, 1);
        let mut out = [0u8; 24];
        object.region.free(index, &mut out).unwrap();
        assert_eq!(object.stats().free_count, 1);
        assert_eq!(object.stats().len, 0);
        drop(object);
        assert_eq!(region_capacity(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(region_capacity(SUBETHA_REGION_NIL_INDEX).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(region_capacity(4).unwrap(), 4);
    }
}
