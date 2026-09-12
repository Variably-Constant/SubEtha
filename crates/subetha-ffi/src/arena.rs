//! The shared string arena through the C ABI: an append-only byte pool in
//! a file every process maps, where a value is named by a 64-bit reference
//! packing its byte offset and length, so the reference that names a value
//! in one process names the same bytes in every other. Bytes are never
//! moved once written; a reference resolves until the arena is cleared or
//! reset. The arena runs no background work, so strict and managed modes
//! are the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_string_arena::{ArenaError, SharedStringArena, StringRef, MAX_LEN, MAX_OFFSET};

use crate::error::{arena_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_READ_ONLY, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::batch::run_push_many_reporting;
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_ARENA};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The most bytes one interned value holds: a reference carries the
/// length in 24 bits.
pub const SUBETHA_ARENA_VALUE_MAX: u64 = 16_777_215;
/// The largest capacity an arena is created with: a reference carries the
/// offset in 40 bits.
pub const SUBETHA_ARENA_CAPACITY_MAX: u64 = 1_099_511_627_775;

const _: () = assert!(SUBETHA_ARENA_VALUE_MAX == MAX_LEN);
const _: () = assert!(SUBETHA_ARENA_CAPACITY_MAX == MAX_OFFSET);

/// A snapshot of a shared string arena.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_arena_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Whether this handle may intern and clear; false for one opened
    /// read-only.
    pub writable: bool,
    /// Bytes of room past the header.
    pub capacity_bytes: u64,
    /// Bytes interned so far.
    pub used_bytes: u64,
    /// `capacity_bytes - used_bytes`.
    pub remaining_bytes: u64,
}

pub(crate) struct ArenaObject {
    arena: SharedStringArena,
    mode: u32,
}

impl ArenaObject {
    /// An arena parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_arena_stats {
        subetha_arena_stats {
            mode: self.mode,
            writable: self.arena.is_writable(),
            capacity_bytes: self.arena.capacity_bytes() as u64,
            used_bytes: self.arena.used_bytes() as u64,
            remaining_bytes: self.arena.remaining_bytes() as u64,
        }
    }
}

fn with_arena(handle: subetha_handle, f: impl FnOnce(&ArenaObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_ARENA, |object| match object {
        Object::Arena(a) => f(a),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a string arena"),
    })
}

/// A capacity every constructor takes: at least one byte and no more than
/// a reference can address.
fn checked_capacity(capacity_bytes: u64) -> Result<usize, i32> {
    if capacity_bytes == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity_bytes is zero"));
    }
    if capacity_bytes > SUBETHA_ARENA_CAPACITY_MAX {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("capacity_bytes {capacity_bytes} exceeds the {SUBETHA_ARENA_CAPACITY_MAX} bytes a reference can address"),
        ));
    }
    usize::try_from(capacity_bytes).map_err(|e| {
        fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("capacity_bytes {capacity_bytes} does not fit this platform's address space: {e}"),
        )
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity_bytes: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let capacity = checked_capacity(capacity_bytes)?;
    let mode = resolve_mode(mode)?;
    Ok((path, capacity, mode))
}

fn place(arena: Result<SharedStringArena, ArenaError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match arena {
        Ok(arena) => unsafe { issue(Object::Arena(ArenaObject { arena, mode }), out) },
        Err(e) => arena_code(e),
    }
}

/// Obtain the arena at `path` with `capacity_bytes` of room: an empty one
/// is initialized when the file does not exist, an existing one is
/// attached with its bytes in place, so references other processes hold
/// keep resolving. A file built with another capacity is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`. `capacity_bytes` is at most
/// `SUBETHA_ARENA_CAPACITY_MAX`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_create(
    path: *const c_char,
    capacity_bytes: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity_bytes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedStringArena::create(path, capacity), mode, out)
    })
}

/// Attach to the arena another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_open(
    path: *const c_char,
    capacity_bytes: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity_bytes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedStringArena::open(path, capacity), mode, out)
    })
}

/// Attach to the arena at `path` with read access alone, for a process
/// that may not write the file. `get` and `view` behave as on any handle;
/// `intern` and `clear` return `SUBETHA_E_READ_ONLY`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_open_read_only(
    path: *const c_char,
    capacity_bytes: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity_bytes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedStringArena::open_read_only(path, capacity), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty arena there,
/// invalidating every reference other handles hold. On Windows the file
/// must not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_reset(
    path: *const c_char,
    capacity_bytes: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity_bytes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedStringArena::reset(path, capacity), mode, out)
    })
}

/// Append `len` bytes from `data` and write the reference naming them into
/// `out_ref`; an empty value is a reference of length zero at the current
/// end. `SUBETHA_E_ARENA_FULL` when the arena has no room for the whole
/// value, in which case nothing was written; `len` is at most
/// `SUBETHA_ARENA_VALUE_MAX`.
///
/// # Safety
/// `data` points to `len` readable bytes, or is null with `len` zero;
/// `out_ref` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_intern(handle: subetha_handle, data: *const u8, len: usize, out_ref: *mut u64) -> i32 {
    with_arena(handle, |a| {
        if out_ref.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_ref is null");
        }
        let data = match unsafe { bytes(data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        if data.len() as u64 > SUBETHA_ARENA_VALUE_MAX {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("{} bytes exceed the {SUBETHA_ARENA_VALUE_MAX} bytes one value holds", data.len()),
            );
        }
        match a.arena.intern_bytes(data) {
            Ok(r) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_ref = r.to_u64() };
                SUBETHA_OK
            }
            Err(e) => arena_code(e),
        }
    })
}

/// Intern `count` values of `len` bytes each from the caller's own array,
/// the one at `items + i * stride`, and write the reference naming each
/// into `out_refs`. One handle lookup and one panic guard for the run.
///
/// Every value in the run is the same length, which is what lets one
/// stride address them all. A caller with values of differing lengths
/// interns them one at a time; the arena stores each at its own length
/// either way, so nothing is padded.
///
/// The batch stops at the first refusal, `SUBETHA_E_ARENA_FULL` being the
/// one to expect, and reports how many landed, so `out_refs` holds
/// `out_done` of them.
///
/// Despite the name, this appends rather than deduplicating: the same
/// bytes interned twice occupy the arena twice and answer with two
/// different references, both of which read back the same value. A caller
/// wanting one copy per distinct value keeps its own map from value to
/// reference; the arena does not keep one, which is what makes an intern
/// a bump of the cursor rather than a lookup.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_refs`
/// addresses `count` writable `uint64_t`s; `out_done` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_intern_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_refs: *mut u64,
    out_done: *mut usize,
) -> i32 {
    with_arena(handle, |a| {
        if len as u64 > SUBETHA_ARENA_VALUE_MAX {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("{len} bytes exceed the {SUBETHA_ARENA_VALUE_MAX} bytes one value holds"),
            );
        }
        unsafe {
            run_push_many_reporting(items, stride, len, count, out_refs, out_done, |value| {
                match a.arena.intern_bytes(value) {
                    Ok(r) => Ok(r.to_u64()),
                    Err(e) => Err(arena_code(e)),
                }
            })
        }
    })
}

/// Copy the bytes `reference` names into `out`, at least their length,
/// and their length into `out_len`. `SUBETHA_E_ARENA_INVALID_REF` when the
/// reference reaches past what the arena holds.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_get(
    handle: subetha_handle,
    reference: u64,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_arena(handle, |a| {
        let found = match a.arena.get_bytes(StringRef::from_u64(reference)) {
            Ok(f) => f,
            Err(e) => return arena_code(e),
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, found.len()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        buf[..found.len()].copy_from_slice(found);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = found.len() };
        SUBETHA_OK
    })
}

/// Hand out the bytes `reference` names in place: `out_data` receives a
/// pointer into the arena's mapping and `out_len` their length. The
/// pointer stays valid while this handle is live and until the arena is
/// cleared or reset; destroying the handle unmaps it.
/// `SUBETHA_E_ARENA_INVALID_REF` when the reference reaches past what the
/// arena holds.
///
/// # Safety
/// `out_data` and `out_len` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_view(
    handle: subetha_handle,
    reference: u64,
    out_data: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    with_arena(handle, |a| {
        if out_data.is_null() || out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_data or out_len is null");
        }
        match a.arena.get_bytes(StringRef::from_u64(reference)) {
            Ok(found) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_data = found.as_ptr();
                    *out_len = found.len();
                }
                SUBETHA_OK
            }
            Err(e) => arena_code(e),
        }
    })
}

/// The byte offset a reference names.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_arena_ref_offset(reference: u64) -> u64 {
    StringRef::from_u64(reference).offset
}

/// The length a reference names.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_arena_ref_len(reference: u64) -> u32 {
    StringRef::from_u64(reference).len
}

/// Pack `offset` and `len` into the reference an intern of `len` bytes at
/// `offset` returns, for a caller that knows where a value lies. Refused
/// past `SUBETHA_ARENA_CAPACITY_MAX` or `SUBETHA_ARENA_VALUE_MAX`.
///
/// # Safety
/// `out_ref` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_ref_pack(offset: u64, len: u32, out_ref: *mut u64) -> i32 {
    entry(|| {
        if out_ref.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_ref is null");
        }
        if offset > SUBETHA_ARENA_CAPACITY_MAX {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("offset {offset} exceeds the {SUBETHA_ARENA_CAPACITY_MAX} bytes a reference can address"),
            );
        }
        if u64::from(len) > SUBETHA_ARENA_VALUE_MAX {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("len {len} exceeds the {SUBETHA_ARENA_VALUE_MAX} bytes one value holds"),
            );
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_ref = StringRef { offset, len }.to_u64() };
        SUBETHA_OK
    })
}

/// Empty the arena for every handle: the next intern lands at offset zero
/// and every reference handed out so far stops resolving. Not safe against
/// an intern or a get running in any process. `SUBETHA_E_READ_ONLY` on a
/// handle opened read-only.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_arena_clear(handle: subetha_handle) -> i32 {
    with_arena(handle, |a| {
        if !a.arena.is_writable() {
            return fail(SUBETHA_E_READ_ONLY, "the arena was opened read-only");
        }
        a.arena.clear();
        SUBETHA_OK
    })
}

/// A snapshot of the arena into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_read_stats(handle: subetha_handle, out: *mut subetha_arena_stats) -> i32 {
    with_arena(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = a.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the arena's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_arena_flush(handle: subetha_handle) -> i32 {
    with_arena(handle, |a| match a.arena.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => arena_code(e),
    })
}

/// Remove the arena file at `path`. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_arena_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-arena-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the arena file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_room_and_a_reference_round_trips() {
        let scratch = Scratch::new("shape");
        let arena = SharedStringArena::create(&scratch.0, 64).unwrap();
        let object = ArenaObject { arena, mode: SUBETHA_MODE_STRICT };
        assert_eq!(object.stats().capacity_bytes, 64);
        assert!(object.stats().writable);
        let r = object.arena.intern_bytes(b"hello").unwrap();
        assert_eq!(object.stats().used_bytes, 5);
        assert_eq!(object.stats().remaining_bytes, 59);
        let packed = r.to_u64();
        assert_eq!(subetha_arena_ref_offset(packed), 0);
        assert_eq!(subetha_arena_ref_len(packed), 5);
        assert_eq!(StringRef::from_u64(packed), r);
        let reader = ArenaObject {
            arena: SharedStringArena::open_read_only(&scratch.0, 64).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        assert!(!reader.stats().writable);
        assert_eq!(reader.arena.get_bytes(r).unwrap(), b"hello");
        assert_eq!(reader.arena.intern_bytes(b"no").unwrap_err(), ArenaError::ReadOnly);
        drop(reader);
        drop(object);
        assert_eq!(checked_capacity(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_capacity(SUBETHA_ARENA_CAPACITY_MAX + 1).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_capacity(64).unwrap(), 64);
    }
}
