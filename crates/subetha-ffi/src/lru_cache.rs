//! The shared LRU cache through the C ABI.
//!
//! Keys and values cross as byte buffers of exactly the sizes the cache
//! was created with. A buffer of any other length is refused rather than
//! padded or truncated, because a key silently changed names a different
//! entry.
//!
//! Reads come in two forms and the difference is the point of the cache.
//! `subetha_lru_get` leaves recency alone, so a caller inspecting entries
//! does not reorder them; `subetha_lru_get_and_touch` promotes what it
//! reads, which is what a cache in use wants. Choosing the wrong one does
//! not corrupt anything, it just evicts the wrong entry later.

use std::ffi::c_char;

use subetha_cxc::raw_lru_cache::{RawLruCache, RawLruError};
use subetha_cxc::shared_hash_map::MapError;
use subetha_cxc::shared_linked_list::LinkedListError;

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_FULL, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LRU_CACHE};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

pub(crate) struct LruObject {
    cache: RawLruCache,
    mode: u32,
}

impl LruObject {
    /// Nothing parks on a cache, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

/// A snapshot of a shared LRU cache.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_lru_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Entries the cache can hold before it evicts.
    pub capacity: u32,
    /// Entries it holds now.
    pub len: u64,
    /// Bytes in a key.
    pub key_size: u64,
    /// Bytes in a value.
    pub value_size: u64,
}

fn code_for(e: RawLruError) -> i32 {
    match e {
        RawLruError::WrongSize { expected, found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("this cache uses {expected}-byte buffers here, and {found} was given"),
        ),
        RawLruError::ZeroCapacity => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "a cache of zero entries holds nothing")
        }
        RawLruError::Map(MapError::Full) => {
            fail(SUBETHA_E_MAP_FULL, "the cache's index is full")
        }
        RawLruError::Map(MapError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the cache on disk was built with different key or value sizes",
        ),
        RawLruError::Map(MapError::IoError(k)) => {
            fail(SUBETHA_E_RING_IO, format!("the cache's index could not be reached: {k:?}"))
        }
        RawLruError::Map(other) => {
            fail(SUBETHA_E_RING_IO, format!("the cache's index refused: {other:?}"))
        }
        RawLruError::List(LinkedListError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the cache on disk was built with different key or value sizes",
        ),
        RawLruError::List(LinkedListError::IoError(k)) => {
            fail(SUBETHA_E_RING_IO, format!("the cache's order could not be reached: {k:?}"))
        }
        RawLruError::List(other) => {
            fail(SUBETHA_E_RING_IO, format!("the cache's order refused: {other:?}"))
        }
    }
}

fn with_lru(handle: subetha_handle, f: impl FnOnce(&LruObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LRU_CACHE, |object| match object {
        Object::Lru(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an LRU cache"),
    })
}

/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments(
    base_path: *const c_char,
    capacity: u32,
    key_size: u64,
    value_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'static str, u32, usize, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = unsafe { text(base_path, "base_path") }?;
    let mode = resolve_mode(mode)?;
    if key_size == 0 || value_size == 0 {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "a key or value of zero bytes carries nothing",
        ));
    }
    Ok((path, capacity, key_size as usize, value_size as usize, mode))
}

/// Create an LRU cache under `base_path`, holding `capacity` entries of
/// `key_size` and `value_size` bytes.
///
/// Two files are made beside that prefix: the index and the recency order.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_create(
    base_path: *const c_char,
    capacity: u32,
    key_size: u64,
    value_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, key_size, value_size, mode) = match unsafe {
            read_arguments(base_path, capacity, key_size, value_size, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawLruCache::create(path, capacity, key_size, value_size) {
            Ok(c) => unsafe { issue(Object::Lru(LruObject { cache: c, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a cache another process created under `base_path`. The files
/// must exist and have been built with the same shape.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_open(
    base_path: *const c_char,
    capacity: u32,
    key_size: u64,
    value_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, key_size, value_size, mode) = match unsafe {
            read_arguments(base_path, capacity, key_size, value_size, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawLruCache::open(path, capacity, key_size, value_size) {
            Ok(c) => unsafe { issue(Object::Lru(LruObject { cache: c, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Store `value` under `key` at the most recent end, evicting the least
/// recent entry first where the cache is full.
///
/// `replaced` is set to true where the key was already present and its
/// value overwritten. It may be null when the caller does not care.
///
/// # Safety
/// `key` and `value` point to at least their declared sizes; `replaced` is
/// null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_put(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    value: *const u8,
    value_len: u64,
    replaced: *mut bool,
) -> i32 {
    with_lru(handle, |c| {
        let key = match unsafe { bytes(key, key_len as usize) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match c.cache.put(key, value) {
            Ok(was_there) => {
                if !replaced.is_null() {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *replaced = was_there };
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Read the value for `key` into `value_out` without changing its recency.
///
/// `found` is set to whether the cache held it. When it did not, nothing
/// is written to `value_out`.
///
/// # Safety
/// `key` and `value_out` point to at least their declared sizes; `found`
/// is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_get(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    value_out: *mut u8,
    value_len: u64,
    found: *mut bool,
) -> i32 {
    unsafe { lru_read(handle, key, key_len, value_out, value_len, found, false) }
}

/// Read the value for `key` into `value_out` and move it to the most
/// recent end.
///
/// # Safety
/// As [`subetha_lru_get`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_get_and_touch(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    value_out: *mut u8,
    value_len: u64,
    found: *mut bool,
) -> i32 {
    unsafe { lru_read(handle, key, key_len, value_out, value_len, found, true) }
}

/// # Safety
/// As the two entry points above.
unsafe fn lru_read(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    value_out: *mut u8,
    value_len: u64,
    found: *mut bool,
    promote: bool,
) -> i32 {
    with_lru(handle, |c| {
        if found.is_null() || value_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "found or value_out is null");
        }
        let key = match unsafe { bytes(key, key_len as usize) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        // The value buffer is the caller's and is written only on a hit,
        // so it is built here and copied out rather than borrowed.
        let mut scratch = vec![0u8; value_len as usize];
        let result = if promote {
            c.cache.get_and_touch(key, &mut scratch)
        } else {
            c.cache.get(key, &mut scratch)
        };
        match result {
            Ok(hit) => {
                // Checked non-null above.
                unsafe { *found = hit };
                if hit {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            scratch.as_ptr(),
                            value_out,
                            scratch.len(),
                        );
                    }
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Whether the cache holds `key`, without changing its recency.
///
/// # Safety
/// `key` points to at least `key_len` bytes; `present` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_contains(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    present: *mut bool,
) -> i32 {
    with_lru(handle, |c| {
        if present.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "present is null");
        }
        let key = match unsafe { bytes(key, key_len as usize) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        match c.cache.contains_key(key) {
            Ok(hit) => {
                // Checked non-null above.
                unsafe { *present = hit };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Move `key` to the most recent end without reading its value.
///
/// `promoted` is set to whether the cache held it, and may be null.
///
/// # Safety
/// `key` points to at least `key_len` bytes; `promoted` is null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_touch(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    promoted: *mut bool,
) -> i32 {
    with_lru(handle, |c| {
        let key = match unsafe { bytes(key, key_len as usize) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        match c.cache.touch(key) {
            Ok(hit) => {
                if !promoted.is_null() {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *promoted = hit };
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Drop `key`, writing its value into `value_out`.
///
/// `removed` is set to whether the cache held it. When it did not,
/// nothing is written to `value_out`.
///
/// # Safety
/// `key` and `value_out` point to at least their declared sizes;
/// `removed` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_remove(
    handle: subetha_handle,
    key: *const u8,
    key_len: u64,
    value_out: *mut u8,
    value_len: u64,
    removed: *mut bool,
) -> i32 {
    with_lru(handle, |c| {
        if removed.is_null() || value_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "removed or value_out is null");
        }
        let key = match unsafe { bytes(key, key_len as usize) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let mut scratch = vec![0u8; value_len as usize];
        match c.cache.remove(key, &mut scratch) {
            Ok(hit) => {
                // Checked non-null above.
                unsafe { *removed = hit };
                if hit {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            scratch.as_ptr(),
                            value_out,
                            scratch.len(),
                        );
                    }
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Drop the least recently used entry, writing its key and value out.
///
/// `evicted` is set to whether there was anything to evict.
///
/// # Safety
/// `key_out` and `value_out` point to at least their declared sizes;
/// `evicted` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_evict_oldest(
    handle: subetha_handle,
    key_out: *mut u8,
    key_len: u64,
    value_out: *mut u8,
    value_len: u64,
    evicted: *mut bool,
) -> i32 {
    with_lru(handle, |c| {
        if evicted.is_null() || key_out.is_null() || value_out.is_null() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "evicted, key_out or value_out is null",
            );
        }
        let mut key_scratch = vec![0u8; key_len as usize];
        let mut value_scratch = vec![0u8; value_len as usize];
        match c.cache.evict_oldest(&mut key_scratch, &mut value_scratch) {
            Ok(hit) => {
                // Checked non-null above.
                unsafe { *evicted = hit };
                if hit {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            key_scratch.as_ptr(),
                            key_out,
                            key_scratch.len(),
                        );
                        std::ptr::copy_nonoverlapping(
                            value_scratch.as_ptr(),
                            value_out,
                            value_scratch.len(),
                        );
                    }
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Read the cache's shape and occupancy into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lru_read_stats(
    handle: subetha_handle,
    out: *mut subetha_lru_stats,
) -> i32 {
    with_lru(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_lru_stats {
            mode: c.mode,
            capacity: c.cache.capacity(),
            len: c.cache.len() as u64,
            key_size: c.cache.key_size() as u64,
            value_size: c.cache.value_size() as u64,
        };
        // Checked non-null above.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Push both of the cache's files to disk and wait for them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_lru_flush(handle: subetha_handle) -> i32 {
    with_lru(handle, |c| match c.cache.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}
