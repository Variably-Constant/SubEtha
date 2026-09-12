//! The shared B-tree map through the C ABI: an ordered map in a file at
//! key and value sizes the caller declares, which any number of processes
//! read while one writes.
//!
//! Keys compare as unsigned bytes, left to right, which is `memcmp` order.
//! Every language agrees on it without a comparator crossing the boundary,
//! and it is the same order on every platform. A caller that wants numeric
//! order stores its integers big-endian: little-endian bytes sort by their
//! least significant byte first, which is not what anyone means by a
//! range.
//!
//! One writer, any number of readers. `insert`, `remove` and `clear`
//! change the tree's shape and must be serialized against each other by
//! the caller; `get`, `contains`, `first` and `last` are lock-free and
//! retry while a writer is mid-change. The map runs no background work, so
//! strict and managed modes are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_btree_map::RawBTreeMap;
use subetha_cxc::shared_btree_map::{BTreeError, B, T};

use crate::error::{
    btree_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_KEY_ABSENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_BTREE};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// Keys one node holds. A tree of `capacity` nodes holds at most
/// `capacity * SUBETHA_BTREE_KEYS_PER_NODE` entries, and fewer in
/// practice, since a node below the root is only guaranteed half full.
pub const SUBETHA_BTREE_KEYS_PER_NODE: u32 = 15;
/// The least keys a node below the root carries.
pub const SUBETHA_BTREE_MIN_KEYS_PER_NODE: u32 = 7;

const _: () = assert!(SUBETHA_BTREE_KEYS_PER_NODE as usize == B);
const _: () = assert!(SUBETHA_BTREE_MIN_KEYS_PER_NODE as usize == T - 1);

/// A snapshot of a shared B-tree map.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_btree_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Nodes the map can hold.
    pub capacity: u64,
    /// Entries in the map.
    pub len: u64,
    /// Nodes handed out, the free list included.
    pub node_count: u64,
    /// Bytes per key.
    pub key_size: u64,
    /// Bytes per value.
    pub value_size: u64,
    /// The layout tag the map was created with.
    pub tag: u64,
    /// Bytes from one node to the next in the region.
    pub node_stride: u64,
}

pub(crate) struct BTreeObject {
    map: RawBTreeMap,
    mode: u32,
}

impl BTreeObject {
    /// A map parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_btree_stats {
        subetha_btree_stats {
            mode: self.mode,
            capacity: self.map.capacity() as u64,
            len: self.map.len() as u64,
            node_count: self.map.node_count() as u64,
            key_size: self.map.key_size() as u64,
            value_size: self.map.value_size() as u64,
            tag: self.map.layout_tag(),
            node_stride: self.map.geometry().stride as u64,
        }
    }
}

fn with_btree(handle: subetha_handle, f: impl FnOnce(&BTreeObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_BTREE, |object| match object {
        Object::BTree(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a B-tree map"),
    })
}

/// A node capacity: at least one.
fn btree_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    Ok(capacity as usize)
}

/// The sizes a constructor takes: a non-empty key, and both within what
/// the region records.
fn checked_sizes(key_size: u32, value_size: u32) -> Result<(usize, usize), i32> {
    if key_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "key_size is zero"));
    }
    Ok((key_size as usize, value_size as usize))
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, usize, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let capacity = btree_capacity(capacity)?;
    let (k, v) = checked_sizes(key_size, value_size)?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, k, v, mode))
}

fn place(map: Result<RawBTreeMap, BTreeError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match map {
        Ok(map) => unsafe { issue(Object::BTree(BTreeObject { map, mode }), out) },
        Err(e) => btree_code(e),
    }
}

/// Obtain the map at `path` for `capacity` nodes of `key_size`-byte keys
/// and `value_size`-byte values: an empty one is initialized when the file
/// does not exist, an existing one is attached with its tree in place. A
/// file built with another capacity, sizes or `tag` is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`, and so is one written by the Rust
/// `SharedBTreeMap`, which orders its keys by their type rather than as
/// bytes.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_create(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    tag: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, k, v, mode) = match unsafe { read_arguments(path, capacity, key_size, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawBTreeMap::create(path, capacity, k, v, tag), mode, out)
    })
}

/// Attach to the map another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_open(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    tag: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, k, v, mode) = match unsafe { read_arguments(path, capacity, key_size, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawBTreeMap::open(path, capacity, k, v, tag), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty map there,
/// discarding the tree other handles share. On Windows the file must not
/// be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_reset(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    tag: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, k, v, mode) = match unsafe { read_arguments(path, capacity, key_size, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawBTreeMap::reset(path, capacity, k, v, tag), mode, out)
    })
}

/// A key argument of exactly the map's key size.
///
/// # Safety
/// `key` points to `key_len` readable bytes.
unsafe fn key_of<'a>(b: &BTreeObject, key: *const u8, key_len: usize) -> Result<&'a [u8], i32> {
    let key = unsafe { bytes(key, key_len) }?;
    if key.len() != b.map.key_size() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("a key is {} bytes, not {}", b.map.key_size(), key.len()),
        ));
    }
    Ok(key)
}

/// Insert or update `key` with `value`. `out_replaced`, when not null,
/// receives whether a value was already there. A caller that also wants
/// the value it replaced passes `previous` and `previous_len`; one that
/// does not passes null for both. `SUBETHA_E_RING_FULL` when the map has
/// no node for the split the insert needs. One writer: serialize this
/// against other writers yourself.
///
/// # Safety
/// `key` and `value` point to readable bytes; `previous` is null or
/// points to `cap` writable bytes; `previous_len` and `out_replaced` are
/// null or valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_insert(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    previous: *mut u8,
    cap: usize,
    previous_len: *mut usize,
    out_replaced: *mut bool,
) -> i32 {
    with_btree(handle, |b| {
        let key = match unsafe { key_of(b, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let value = match unsafe { bytes(value, value_len) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        if value.len() != b.map.value_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a value is {} bytes, not {}", b.map.value_size(), value.len()),
            );
        }
        // A caller that does not want the replaced value passes neither a
        // buffer nor a length for it.
        let wants_previous = !previous.is_null() || !previous_len.is_null();
        let buf = if wants_previous {
            match unsafe { out_buffer(previous, cap, previous_len, b.map.value_size()) } {
                Ok(p) => Some(p),
                Err(code) => return code,
            }
        } else {
            None
        };
        match b.map.insert(key, value, buf) {
            Ok(replaced) => {
                // SAFETY: each pointer was checked non-null; the caller
                // guarantees the ones it passed are writable.
                unsafe {
                    if !previous_len.is_null() {
                        *previous_len = if replaced { b.map.value_size() } else { 0 };
                    }
                    if !out_replaced.is_null() {
                        *out_replaced = replaced;
                    }
                }
                SUBETHA_OK
            }
            Err(e) => btree_code(e),
        }
    })
}

/// Copy `key`'s value into `out`, at least `value_size` bytes;
/// `SUBETHA_E_MAP_KEY_ABSENT` when the key has no entry.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_get(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_btree(handle, |b| {
        let key = match unsafe { key_of(b, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, b.map.value_size()) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        match b.map.get(key, buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = b.map.value_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => btree_code(e),
        }
    })
}

/// Whether `key` has an entry.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_contains(handle: subetha_handle, key: *const u8, key_len: usize, out: *mut bool) -> i32 {
    with_btree(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let key = match unsafe { key_of(b, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        match b.map.contains_key(key) {
            Ok(present) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = present };
                SUBETHA_OK
            }
            Err(e) => btree_code(e),
        }
    })
}

/// Remove `key`, copying the value it held into `out`, at least
/// `value_size` bytes; `SUBETHA_E_MAP_KEY_ABSENT` when it had none. One
/// writer, as insert is.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_remove(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_btree(handle, |b| {
        let key = match unsafe { key_of(b, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, b.map.value_size()) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        match b.map.remove(key, buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = b.map.value_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => btree_code(e),
        }
    })
}

/// Copy the smallest key and its value into `key_out` and `value_out`,
/// each at least its own size. `SUBETHA_E_MAP_KEY_ABSENT` when the map is
/// empty.
///
/// # Safety
/// `key_out` and `value_out` point to `key_cap` and `value_cap` writable
/// bytes; `key_len` and `value_len` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_first(
    handle: subetha_handle,
    key_out: *mut u8,
    key_cap: usize,
    key_len: *mut usize,
    value_out: *mut u8,
    value_cap: usize,
    value_len: *mut usize,
) -> i32 {
    with_btree(handle, |b| unsafe { edge(b, key_out, key_cap, key_len, value_out, value_cap, value_len, End::First) })
}

/// Copy the largest key and its value out, as `subetha_btree_first`.
///
/// # Safety
/// `key_out` and `value_out` point to `key_cap` and `value_cap` writable
/// bytes; `key_len` and `value_len` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_last(
    handle: subetha_handle,
    key_out: *mut u8,
    key_cap: usize,
    key_len: *mut usize,
    value_out: *mut u8,
    value_cap: usize,
    value_len: *mut usize,
) -> i32 {
    with_btree(handle, |b| unsafe { edge(b, key_out, key_cap, key_len, value_out, value_cap, value_len, End::Last) })
}

/// Which end of the map an entry is wanted from.
#[derive(Clone, Copy)]
enum End {
    First,
    Last,
}

/// The shared body of `first` and `last`.
///
/// # Safety
/// The buffers and lengths are the entry points' own.
#[allow(clippy::too_many_arguments)]
unsafe fn edge(
    b: &BTreeObject,
    key_out: *mut u8,
    key_cap: usize,
    key_len: *mut usize,
    value_out: *mut u8,
    value_cap: usize,
    value_len: *mut usize,
    end: End,
) -> i32 {
    let keys = match unsafe { out_buffer(key_out, key_cap, key_len, b.map.key_size()) } {
        Ok(k) => k,
        Err(code) => return code,
    };
    let values = match unsafe { out_buffer(value_out, value_cap, value_len, b.map.value_size()) } {
        Ok(v) => v,
        Err(code) => return code,
    };
    let found = match end {
        End::First => b.map.first(keys, values),
        End::Last => b.map.last(keys, values),
    };
    match found {
        Ok(true) => {
            // SAFETY: checked non-null; the caller guarantees they are writable.
            unsafe {
                *key_len = b.map.key_size();
                *value_len = b.map.value_size();
            }
            SUBETHA_OK
        }
        Ok(false) => {
            // SAFETY: as above.
            unsafe {
                *key_len = 0;
                *value_len = 0;
            }
            SUBETHA_E_MAP_KEY_ABSENT
        }
        Err(e) => btree_code(e),
    }
}

/// Empty the map for every handle. One writer: not safe against an insert
/// or a remove running in any process.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_btree_clear(handle: subetha_handle) -> i32 {
    with_btree(handle, |b| {
        b.map.clear();
        SUBETHA_OK
    })
}

/// A snapshot of the map into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_read_stats(handle: subetha_handle, out: *mut subetha_btree_stats) -> i32 {
    with_btree(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = b.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the map's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_btree_flush(handle: subetha_handle) -> i32 {
    with_btree(handle, |b| match b.map.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => btree_code(e),
    })
}

/// Remove the map file at `path`. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_btree_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-btree-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the map file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_shape_and_refuses_a_key_of_no_size() {
        let scratch = Scratch::new("shape");
        let map = RawBTreeMap::create(&scratch.0, 32, 4, 8, 3).unwrap();
        let object = BTreeObject { map, mode: SUBETHA_MODE_STRICT };
        let stats = object.stats();
        assert_eq!(stats.capacity, 32);
        assert_eq!(stats.len, 0);
        assert_eq!(stats.key_size, 4);
        assert_eq!(stats.value_size, 8);
        assert_eq!(stats.tag, 3);
        assert!(stats.node_stride > 0);
        let mut previous = [0u8; 8];
        object.map.insert(&1u32.to_be_bytes(), &[7u8; 8], Some(&mut previous)).unwrap();
        assert_eq!(object.stats().len, 1);
        assert_eq!(object.stats().node_count, 1);
        drop(object);
        assert_eq!(checked_sizes(0, 8).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_sizes(4, 0).unwrap(), (4, 0));
        assert_eq!(btree_capacity(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(btree_capacity(9).unwrap(), 9);
    }
}
