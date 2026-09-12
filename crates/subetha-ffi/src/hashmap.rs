//! The shared hash map through the C ABI: an open-addressed table in a
//! file that any number of processes insert into, look up and remove
//! from, at key and value sizes the caller declares. Keys compare as
//! bytes and hash with FNV-1a, so the same bytes name the same entry from
//! every process and every language. Every operation copies bytes; a key
//! or value that is not exactly its declared size is refused. The map
//! runs no background work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::raw_hash_map::RawHashMap;
use subetha_cxc::shared_hash_map::{InsertOutcome, MapError, MAP_PAYLOAD_BYTES};

use crate::error::{
    fail, map_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_KEY_ABSENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::batch::{run_pair_many, run_pop_many_indexed};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_HASHMAP};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The insert placed a new entry.
pub const SUBETHA_MAP_INSERTED: u32 = 0;
/// The insert overwrote an existing entry's value.
pub const SUBETHA_MAP_UPDATED: u32 = 1;

/// The most bytes a key and a value take together in one entry.
pub const SUBETHA_HASHMAP_PAYLOAD_BYTES: usize = 48;

const _: () = assert!(SUBETHA_HASHMAP_PAYLOAD_BYTES == MAP_PAYLOAD_BYTES);

/// A snapshot of a shared hash map.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_hashmap_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in the table.
    pub capacity: u64,
    /// Entries present.
    pub len: u64,
    /// Bytes per key.
    pub key_size: u64,
    /// Bytes per value.
    pub value_size: u64,
    /// Slots holding a removed entry, reclaimed by an insert or a compact.
    pub tombstones: u64,
    /// `len / capacity`.
    pub load_factor: f64,
}

pub(crate) struct HashMapObject {
    map: RawHashMap,
    mode: u32,
}

impl HashMapObject {
    /// A map parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_hashmap_stats {
        subetha_hashmap_stats {
            mode: self.mode,
            capacity: self.map.capacity() as u64,
            len: self.map.len() as u64,
            key_size: self.map.key_size() as u64,
            value_size: self.map.value_size() as u64,
            tombstones: self.map.tombstone_count() as u64,
            load_factor: self.map.load_factor(),
        }
    }
}

fn with_map(handle: subetha_handle, f: impl FnOnce(&HashMapObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_HASHMAP, |object| match object {
        Object::HashMap(m) => f(m),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a hash map"),
    })
}

/// The sizes a constructor takes: a non-empty key and a key plus value
/// that fit one entry.
fn checked_sizes(key_size: u32, value_size: u32) -> Result<(usize, usize), i32> {
    let (k, v) = (key_size as usize, value_size as usize);
    if k == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "key_size is zero"));
    }
    if k + v > MAP_PAYLOAD_BYTES {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("key_size {k} plus value_size {v} exceed the {MAP_PAYLOAD_BYTES}-byte entry"),
        ));
    }
    Ok((k, v))
}

fn map_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity < 2 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("capacity {capacity} is below 2")));
    }
    Ok(capacity as usize)
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
    let capacity = map_capacity(capacity)?;
    let (k, v) = checked_sizes(key_size, value_size)?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, k, v, mode))
}

fn place(map: Result<RawHashMap, MapError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match map {
        Ok(map) => unsafe { issue(Object::HashMap(HashMapObject { map, mode }), out) },
        Err(e) => map_code(e),
    }
}

/// Obtain the map at `path` for `key_size`-byte keys and `value_size`-byte
/// values, `capacity` slots: an empty one is initialized when the file
/// does not exist, an existing one is attached with its entries in place.
/// A file built with another capacity or other sizes is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`. Size the table at twice the entries
/// expected; the probe chain saturates past a load factor of 0.7.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_create(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, k, v, mode) = match unsafe { read_arguments(path, capacity, key_size, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawHashMap::create(path, capacity, k, v), mode, out)
    })
}

/// Attach to the map another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_open(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, k, v, mode) = match unsafe { read_arguments(path, capacity, key_size, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawHashMap::open(path, capacity, k, v), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty map there,
/// discarding every entry other handles share. On Windows the file must
/// not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_reset(
    path: *const c_char,
    capacity: u32,
    key_size: u32,
    value_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, k, v, mode) = match unsafe { read_arguments(path, capacity, key_size, value_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawHashMap::reset(path, capacity, k, v), mode, out)
    })
}

/// A key argument of exactly the map's key size.
///
/// # Safety
/// `key` points to `key_len` readable bytes.
unsafe fn key_of<'a>(m: &HashMapObject, key: *const u8, key_len: usize) -> Result<&'a [u8], i32> {
    let key = unsafe { bytes(key, key_len) }?;
    if key.len() != m.map.key_size() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("a key is {} bytes, not {}", m.map.key_size(), key.len()),
        ));
    }
    Ok(key)
}

/// A value argument of exactly the map's value size.
///
/// # Safety
/// `value` points to `value_len` readable bytes.
unsafe fn value_of<'a>(m: &HashMapObject, value: *const u8, value_len: usize, what: &str) -> Result<&'a [u8], i32> {
    let value = unsafe { bytes(value, value_len) }?;
    if value.len() != m.map.value_size() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("{what} is {} bytes, not {}", value.len(), m.map.value_size()),
        ));
    }
    Ok(value)
}

/// Insert or update `key` with `value`; `out_outcome`, when not null,
/// receives `SUBETHA_MAP_INSERTED` or `SUBETHA_MAP_UPDATED`.
/// `SUBETHA_E_MAP_FULL` when no slot is free.
///
/// # Safety
/// `key` and `value` point to `key_len` and `value_len` readable bytes;
/// `out_outcome` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_insert(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    out_outcome: *mut u32,
) -> i32 {
    with_map(handle, |m| {
        let key = match unsafe { key_of(m, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let value = match unsafe { value_of(m, value, value_len, "the value") } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match m.map.insert(key, value) {
            Ok(outcome) => {
                if !out_outcome.is_null() {
                    // SAFETY: checked non-null; the caller guarantees it is writable.
                    unsafe {
                        *out_outcome = match outcome {
                            InsertOutcome::Inserted => SUBETHA_MAP_INSERTED,
                            InsertOutcome::Updated => SUBETHA_MAP_UPDATED,
                        }
                    };
                }
                SUBETHA_OK
            }
            Err(e) => map_code(e),
        }
    })
}

/// Insert `count` entries from the caller's own two arrays, the key at
/// `keys + i * key_stride` and its value at `values + i * value_stride`.
/// One handle lookup and one panic guard for the run.
///
/// The keys and the values stay in separate arrays because that is how a
/// caller already has them - a column of each - and packing them into
/// pairs to make one array would cost the copy this call exists to avoid.
///
/// Whether each entry was new or replaced an existing one is not
/// reported: a run of inserts is a caller saying what the map should
/// hold, and the per-entry outcome is what `subetha_hashmap_insert`
/// answers for a caller that needs it. The batch stops at the first
/// refusal, `SUBETHA_E_MAP_FULL` being the one to expect, and reports how
/// many landed.
///
/// # Safety
/// `keys` addresses `count * key_stride` readable bytes and `values`
/// `count * value_stride`; `out_done` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_insert_many(
    handle: subetha_handle,
    keys: *const u8,
    key_stride: usize,
    values: *const u8,
    value_stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        let (key_size, value_size) = (m.map.key_size(), m.map.value_size());
        unsafe {
            run_pair_many(
                keys,
                key_stride,
                key_size,
                values,
                value_stride,
                value_size,
                count,
                out_done,
                |key, value| match m.map.insert(key, value) {
                    Ok(_) => SUBETHA_OK,
                    Err(e) => map_code(e),
                },
            )
        }
    })
}

/// Read `count` entries into the caller's own array, the value for the
/// key at `keys + i * key_stride` into `out + i * value_stride`. One
/// handle lookup and one panic guard for the run.
///
/// The batch stops at the first key the map does not hold and reports how
/// many it read, so a caller that wants every hit rather than a run of
/// them looks each up on its own. `SUBETHA_E_MAP_KEY_ABSENT` is what a
/// first miss reports.
///
/// # Safety
/// `keys` addresses `count * key_stride` readable bytes; `out` addresses
/// `count * value_stride` writable bytes; `out_done` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_get_many(
    handle: subetha_handle,
    keys: *const u8,
    key_stride: usize,
    out: *mut u8,
    value_stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        let (key_size, value_size) = (m.map.key_size(), m.map.value_size());
        if count != 0 && keys.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "keys is null with a non-zero count");
        }
        if count != 0 && key_stride < key_size {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key stride of {key_stride} does not span a key of {key_size} bytes"),
            );
        }
        // Defined outside the `unsafe` below so its own block is the one
        // carrying the pointer read, rather than being swallowed by an
        // enclosing block that covers the call as well.
        let read = |i: usize, slot: &mut [u8]| {
            // SAFETY: `i` is below `count`, which the caller guarantees
            // is readable at `keys`.
            let key = unsafe { std::slice::from_raw_parts(keys.add(i * key_stride), key_size) };
            match m.map.get(key, &mut slot[..value_size]) {
                Ok(true) => SUBETHA_OK,
                // A key the map does not hold stops the run rather than
                // leaving a slot the caller cannot tell from a value:
                // `out_done` is how many it filled, and every one of
                // those is a real entry.
                Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
                Err(e) => map_code(e),
            }
        };
        // SAFETY: the caller guarantees `out` spans `count * value_stride`.
        unsafe { run_pop_many_indexed(out, value_stride, value_size, count, out_done, read) }
    })
}

/// Insert `key` only if it is absent. `out_present` receives whether a
/// value was already there, in which case nothing was written and that
/// value was copied into `existing`, at least `value_size` bytes.
///
/// # Safety
/// `key` and `value` point to readable bytes; `existing` points to `cap`
/// writable bytes; `existing_len` and `out_present` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_insert_if_absent(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    existing: *mut u8,
    cap: usize,
    existing_len: *mut usize,
    out_present: *mut bool,
) -> i32 {
    with_map(handle, |m| {
        if out_present.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_present is null");
        }
        let key = match unsafe { key_of(m, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let value = match unsafe { value_of(m, value, value_len, "the value") } {
            Ok(v) => v,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(existing, cap, existing_len, m.map.value_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match m.map.insert_if_absent(key, value, buf) {
            Ok(present) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_present = present;
                    *existing_len = if present { m.map.value_size() } else { 0 };
                }
                SUBETHA_OK
            }
            Err(e) => map_code(e),
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
pub unsafe extern "C" fn subetha_hashmap_get(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        let key = match unsafe { key_of(m, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, m.map.value_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match m.map.get(key, buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = m.map.value_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => map_code(e),
        }
    })
}

/// Whether `key` has an entry.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_contains(handle: subetha_handle, key: *const u8, key_len: usize, out: *mut bool) -> i32 {
    with_map(handle, |m| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let key = match unsafe { key_of(m, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        match m.map.contains_key(key) {
            Ok(present) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = present };
                SUBETHA_OK
            }
            Err(e) => map_code(e),
        }
    })
}

/// Remove `key`, copying the value it had into `out`, at least
/// `value_size` bytes; `SUBETHA_E_MAP_KEY_ABSENT` when it had none.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_remove(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        let key = match unsafe { key_of(m, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, m.map.value_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match m.map.remove(key, buf) {
            Ok(true) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = m.map.value_size() };
                SUBETHA_OK
            }
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => map_code(e),
        }
    })
}

/// Replace `key`'s value with `new` only if its current value is
/// byte-for-byte `expected`. `out_swapped` receives whether it was; when
/// it was not, the current value is copied into `current`, at least
/// `value_size` bytes. `SUBETHA_E_MAP_KEY_ABSENT` when the key has no
/// entry. The comparison and the write happen under the entry's lock.
///
/// # Safety
/// `key`, `expected` and `new` point to readable bytes; `current` points
/// to `cap` writable bytes; `current_len` and `out_swapped` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_compare_exchange(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    expected: *const u8,
    expected_len: usize,
    new: *const u8,
    new_len: usize,
    current: *mut u8,
    cap: usize,
    current_len: *mut usize,
    out_swapped: *mut bool,
) -> i32 {
    with_map(handle, |m| {
        if out_swapped.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_swapped is null");
        }
        let key = match unsafe { key_of(m, key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let expected = match unsafe { value_of(m, expected, expected_len, "expected") } {
            Ok(v) => v,
            Err(code) => return code,
        };
        let new = match unsafe { value_of(m, new, new_len, "new") } {
            Ok(v) => v,
            Err(code) => return code,
        };
        let buf = match unsafe { out_buffer(current, cap, current_len, m.map.value_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match m.map.compare_exchange(key, expected, new, buf) {
            Ok(swapped) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_swapped = swapped;
                    *current_len = if swapped { 0 } else { m.map.value_size() };
                }
                SUBETHA_OK
            }
            Err(e) => map_code(e),
        }
    })
}

/// Mark every slot empty. Not safe against an insert or remove running in
/// any process.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_hashmap_clear(handle: subetha_handle) -> i32 {
    with_map(handle, |m| {
        m.map.clear();
        SUBETHA_OK
    })
}

/// Reclaim the tombstones removes left by rebuilding the table in place;
/// `out_reclaimed`, when not null, receives how many. Not safe against a
/// writer in any process; a reader may miss an entry mid-rebuild.
///
/// # Safety
/// `out_reclaimed` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_compact(handle: subetha_handle, out_reclaimed: *mut u64) -> i32 {
    with_map(handle, |m| match m.map.compact() {
        Ok(reclaimed) => {
            if !out_reclaimed.is_null() {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_reclaimed = reclaimed as u64 };
            }
            SUBETHA_OK
        }
        Err(e) => map_code(e),
    })
}

/// Walk the entries: with `cursor` at zero, the first entry; each call
/// copies one entry's key and value into `key_out` and `value_out` and
/// advances `cursor`. `out_found` receives false, with nothing copied,
/// once the table is walked. An entry is seen once its insert has
/// published it; a slot an insert has claimed and not yet written is
/// passed over, so a walk under concurrent writers sees the entries
/// published before it reached their slots and never a payload still
/// being written. A walker that must see every entry walks again once
/// the writers have returned.
///
/// # Safety
/// `cursor`, `key_len`, `value_len` and `out_found` are valid pointers;
/// `key_out` and `value_out` point to `key_cap` and `value_cap` writable
/// bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_next(
    handle: subetha_handle,
    cursor: *mut u64,
    key_out: *mut u8,
    key_cap: usize,
    key_len: *mut usize,
    value_out: *mut u8,
    value_cap: usize,
    value_len: *mut usize,
    out_found: *mut bool,
) -> i32 {
    with_map(handle, |m| {
        if cursor.is_null() || out_found.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "cursor or out_found is null");
        }
        let key_buf = match unsafe { out_buffer(key_out, key_cap, key_len, m.map.key_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let value_buf = match unsafe { out_buffer(value_out, value_cap, value_len, m.map.value_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        // SAFETY: checked non-null; the caller guarantees it is readable.
        let at = unsafe { *cursor } as usize;
        match m.map.next_entry(at, key_buf, value_buf) {
            Ok(Some(next)) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *cursor = next as u64;
                    *key_len = m.map.key_size();
                    *value_len = m.map.value_size();
                    *out_found = true;
                }
                SUBETHA_OK
            }
            Ok(None) => {
                // SAFETY: as above.
                unsafe {
                    *key_len = 0;
                    *value_len = 0;
                    *out_found = false;
                }
                SUBETHA_OK
            }
            Err(e) => map_code(e),
        }
    })
}

/// A snapshot of the map into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_read_stats(handle: subetha_handle, out: *mut subetha_hashmap_stats) -> i32 {
    with_map(handle, |m| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = m.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the map's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_hashmap_flush(handle: subetha_handle) -> i32 {
    with_map(handle, |m| match m.map.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => map_code(e),
    })
}

/// Remove the map file at `path`. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_hashmap_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-map-{name}-{}.bin", std::process::id())))
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
    fn the_object_reports_its_shape_and_refuses_bad_sizes() {
        let scratch = Scratch::new("shape");
        let map = RawHashMap::create(&scratch.0, 16, 4, 8).unwrap();
        let object = HashMapObject { map, mode: SUBETHA_MODE_STRICT };
        assert_eq!(object.stats().capacity, 16);
        assert_eq!(object.stats().key_size, 4);
        assert_eq!(object.stats().value_size, 8);
        assert_eq!(object.stats().len, 0);
        object.map.insert(&[1, 2, 3, 4], &[9; 8]).unwrap();
        assert_eq!(object.stats().len, 1);
        assert_eq!(object.stats().load_factor, 1.0 / 16.0);
        drop(object);
        assert_eq!(checked_sizes(0, 8).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_sizes(40, 9).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_sizes(40, 8).unwrap(), (40, 8));
        assert_eq!(map_capacity(1).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
    }
}
