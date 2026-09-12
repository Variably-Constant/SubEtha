//! The shared linked list through the C ABI: a doubly-linked list in a
//! file at an element layout the caller declares in
//! `subetha_element_layout`, over the same slot arena the region uses. A
//! push returns the node's index, which names the same node in every
//! process and stays valid until that node is removed, so a removal from
//! the middle costs one splice rather than a walk. An index used after
//! its node is gone names a slot the arena has handed out again, which
//! the list cannot detect: the caller drops an index when it removes the
//! node.
//!
//! A walk runs from `subetha_list_first` or `subetha_list_last` and ends
//! when `subetha_list_next`, or `_prev`, comes back to
//! `SUBETHA_LIST_HEAD_INDEX`.
//!
//! One writer, any number of readers, as the Rust type has it: two
//! concurrent pushes or removals corrupt the links, and serializing them
//! is the caller's contract. The list runs no background work, so strict
//! and managed modes are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_linked_list::RawLinkedList;
use subetha_cxc::raw_treiber_stack::ElementLayout;
use subetha_cxc::shared_linked_list::{LinkedListError, HEAD_INDEX, NIL_INDEX};

use crate::error::{fail, list_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LIST};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::stack::{read_layout, subetha_element_layout};

/// The sentinel head's index. A walk that reaches it has run out of
/// nodes, and no node of the caller's ever carries it.
pub const SUBETHA_LIST_HEAD_INDEX: u32 = 0;
/// The index that names no node.
pub const SUBETHA_LIST_NIL_INDEX: u32 = 0xFFFF_FFFF;

const _: () = assert!(SUBETHA_LIST_HEAD_INDEX == HEAD_INDEX);
const _: () = assert!(SUBETHA_LIST_NIL_INDEX == NIL_INDEX);

/// A snapshot of a shared linked list.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_list_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Nodes the list can hold, the sentinel head among them.
    pub capacity: u64,
    /// Nodes in the list, the sentinel head excluded.
    pub len: u64,
    /// Bytes per value.
    pub element_size: u64,
    /// The value alignment the layout declares.
    pub alignment: u64,
    /// The layout tag the list was created with.
    pub tag: u64,
    /// Bytes one node takes, its two link words included.
    pub node_size: u64,
}

pub(crate) struct ListObject {
    list: RawLinkedList,
    mode: u32,
}

impl ListObject {
    /// A list parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn element_size(&self) -> usize {
        self.list.layout().slot_size
    }

    fn stats(&self) -> subetha_list_stats {
        let layout = self.list.layout();
        subetha_list_stats {
            mode: self.mode,
            capacity: self.list.capacity() as u64,
            len: self.list.len() as u64,
            element_size: layout.slot_size as u64,
            alignment: layout.alignment as u64,
            tag: layout.tag,
            node_size: self.list.node_size() as u64,
        }
    }
}

fn with_list(handle: subetha_handle, f: impl FnOnce(&ListObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LIST, |object| match object {
        Object::List(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a list"),
    })
}

/// A list capacity: room for the sentinel head and at least one node.
fn list_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity < 2 {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("capacity {capacity} leaves no room past the sentinel head"),
        ));
    }
    if capacity == SUBETHA_LIST_NIL_INDEX {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is the index that names no node"));
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
    let capacity = list_capacity(capacity)?;
    let layout = unsafe { read_layout(layout) }?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, layout, mode))
}

fn place(list: Result<RawLinkedList, LinkedListError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match list {
        Ok(list) => unsafe { issue(Object::List(ListObject { list, mode }), out) },
        Err(e) => list_code(e),
    }
}

/// Obtain the list at `path` holding up to `capacity` nodes, the sentinel
/// head among them, at `layout`: an empty one is initialized when the file
/// does not exist, an existing one is attached with its nodes in place. A
/// file built with another capacity or layout is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_create(
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
        place(RawLinkedList::create(path, capacity, layout), mode, out)
    })
}

/// Attach to the list another process created at `path`; the file must
/// exist and already hold the sentinel head. `SUBETHA_E_RING_IO` names an
/// absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_open(
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
        place(RawLinkedList::open(path, capacity, layout), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty list there,
/// invalidating every index other handles hold. On Windows the file must
/// not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_reset(
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
        place(RawLinkedList::reset(path, capacity, layout), mode, out)
    })
}

/// A value argument of exactly the list's element size.
///
/// # Safety
/// `data` points to `len` readable bytes.
unsafe fn value_of<'a>(l: &ListObject, data: *const u8, len: usize) -> Result<&'a [u8], i32> {
    let data = unsafe { bytes(data, len) }?;
    if data.len() != l.element_size() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("a value is {} bytes, not {}", l.element_size(), data.len()),
        ));
    }
    Ok(data)
}

/// Add `data`, exactly `element_size` bytes, at the front and write the
/// new node's index into `out_index`. `SUBETHA_E_RING_FULL` when every
/// slot is taken.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_index` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_push_front(handle: subetha_handle, data: *const u8, len: usize, out_index: *mut u32) -> i32 {
    with_list(handle, |l| {
        if out_index.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_index is null");
        }
        let data = match unsafe { value_of(l, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match l.list.push_front(data) {
            Ok(index) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_index = index };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// Add `data`, exactly `element_size` bytes, at the back and write the new
/// node's index into `out_index`.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_index` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_push_back(handle: subetha_handle, data: *const u8, len: usize, out_index: *mut u32) -> i32 {
    with_list(handle, |l| {
        if out_index.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_index is null");
        }
        let data = match unsafe { value_of(l, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match l.list.push_back(data) {
            Ok(index) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_index = index };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// Remove the first node, its value into `out`, at least `element_size`
/// bytes, and its length into `out_len`. `SUBETHA_E_RING_EMPTY` when the
/// list holds no node.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_pop_front(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_list(handle, |l| {
        let buf = match unsafe { out_buffer(out, cap, out_len, l.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match l.list.pop_front(buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = l.element_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_RING_EMPTY,
            Err(e) => list_code(e),
        }
    })
}

/// Remove the last node, its value into `out`, at least `element_size`
/// bytes.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_pop_back(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_list(handle, |l| {
        let buf = match unsafe { out_buffer(out, cap, out_len, l.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match l.list.pop_back(buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = l.element_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_RING_EMPTY,
            Err(e) => list_code(e),
        }
    })
}

/// Remove the node at `index`, wherever it sits, its value into `out`, at
/// least `element_size` bytes. `SUBETHA_E_OUT_OF_BOUNDS` for the sentinel
/// head, the nil index, or an index past the capacity.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_remove(handle: subetha_handle, index: u32, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_list(handle, |l| {
        let buf = match unsafe { out_buffer(out, cap, out_len, l.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match l.list.remove(index, buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = l.element_size() };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// Copy the value at `index` into `out`, at least `element_size` bytes.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_get(handle: subetha_handle, index: u32, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_list(handle, |l| {
        let buf = match unsafe { out_buffer(out, cap, out_len, l.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match l.list.get(index, buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = l.element_size() };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// Overwrite the value at `index` with `data`, exactly `element_size`
/// bytes, leaving the node where it is.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_set(handle: subetha_handle, index: u32, data: *const u8, len: usize) -> i32 {
    with_list(handle, |l| {
        let data = match unsafe { value_of(l, data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        match l.list.set(index, data) {
            Ok(()) => SUBETHA_OK,
            Err(e) => list_code(e),
        }
    })
}

/// The first node's index into `out`, or `SUBETHA_LIST_HEAD_INDEX` when
/// the list holds no node.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_first(handle: subetha_handle, out: *mut u32) -> i32 {
    with_list(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match l.list.first() {
            Ok(index) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = index };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// The last node's index into `out`, or `SUBETHA_LIST_HEAD_INDEX` when the
/// list holds no node.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_last(handle: subetha_handle, out: *mut u32) -> i32 {
    with_list(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match l.list.last() {
            Ok(index) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = index };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// The index after `index` into `out`, which is
/// `SUBETHA_LIST_HEAD_INDEX` once the walk has passed the last node.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_next(handle: subetha_handle, index: u32, out: *mut u32) -> i32 {
    with_list(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match l.list.next_of(index) {
            Ok(next) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = next };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// The index before `index` into `out`, which is
/// `SUBETHA_LIST_HEAD_INDEX` once the walk has passed the first node.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_prev(handle: subetha_handle, index: u32, out: *mut u32) -> i32 {
    with_list(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match l.list.prev_of(index) {
            Ok(prev) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = prev };
                SUBETHA_OK
            }
            Err(e) => list_code(e),
        }
    })
}

/// A snapshot of the list into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_read_stats(handle: subetha_handle, out: *mut subetha_list_stats) -> i32 {
    with_list(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = l.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the list's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_list_flush(handle: subetha_handle) -> i32 {
    with_list(handle, |l| match l.list.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => list_code(e),
    })
}

/// Remove the list file at `path`. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_list_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-list-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the list file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_shape_and_walks_its_nodes() {
        let scratch = Scratch::new("shape");
        let layout = ElementLayout { slot_size: 16, alignment: 4, tag: 4 };
        let list = RawLinkedList::create(&scratch.0, 8, layout).unwrap();
        let object = ListObject { list, mode: SUBETHA_MODE_STRICT };
        let stats = object.stats();
        assert_eq!(stats.capacity, 8);
        assert_eq!(stats.len, 0);
        assert_eq!(stats.element_size, 16);
        assert_eq!(stats.alignment, 4);
        assert_eq!(stats.tag, 4);
        assert_eq!(stats.node_size, 24);
        let first = object.list.push_back(&[1u8; 16]).unwrap();
        let second = object.list.push_back(&[2u8; 16]).unwrap();
        assert_eq!(object.stats().len, 2);
        assert_eq!(object.list.first().unwrap(), first);
        assert_eq!(object.list.next_of(first).unwrap(), second);
        assert_eq!(object.list.next_of(second).unwrap(), SUBETHA_LIST_HEAD_INDEX);
        drop(object);
        assert_eq!(list_capacity(1).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(list_capacity(SUBETHA_LIST_NIL_INDEX).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(list_capacity(2).unwrap(), 2);
    }
}
