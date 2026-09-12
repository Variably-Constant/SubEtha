//! A value in shared memory kept alive by the processes holding it,
//! through the C ABI.
//!
//! Ownership is a holder table, one slot per holder stamped with the
//! process that took it, and `strong_count` is how many slots are held. A
//! holder whose process dies never releases, so its slot is reclaimed by
//! probing whether that process is still there: `reap_dead_holders` does
//! it on demand, and an open that would otherwise report the table full
//! does it first.
//!
//! `SUBETHA_ARC_UNLINK` removes the backing when the last holder releases,
//! which is the `Arc` shape. `SUBETHA_ARC_KEEP` leaves it for a process
//! that attaches later.
//!
//! # The bytes are the caller's to interpret and to synchronize
//!
//! The region is a run of bytes whose length is fixed at create. Nothing
//! here says what they mean, and nothing orders one holder's write against
//! another's read: a write is a copy and a concurrent read can see it half
//! done. A caller that mutates puts an atomic in the region, or shares it
//! under a `subetha_rwlock_` or a `subetha_cell_`, exactly as it would in
//! Rust.
//!
//! The backing is the same layout the Rust `SharedArc<T>` uses and records
//! the region's length, so a Rust holder of a `T` and a C holder of
//! `sizeof(T)` bytes share one value, and a length that disagrees is
//! refused. What the two cannot check for each other is the layout inside
//! those bytes; that stays the caller's obligation.
//!
//! The value runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_arc::{ArcError, LastHolder, SharedArcDyn};

use crate::error::{
    arc_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_WRONG_KIND,
    SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_SHARED_ARC};
use crate::ring::{finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// Remove the backing when the last holder releases.
pub const SUBETHA_ARC_UNLINK: u32 = 0;
/// Leave the backing for a process that attaches later.
pub const SUBETHA_ARC_KEEP: u32 = 1;

/// A snapshot of a shared value.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_shared_arc_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// What becomes of the backing when the last holder releases.
    pub on_last: u32,
    /// Bytes the shared region holds.
    pub value_bytes: u64,
    /// Holder slots the backing carries.
    pub capacity: u64,
    /// Slots held right now, this process included. A holder whose
    /// process has died still counts until something reaps it.
    pub strong_count: u64,
}

pub(crate) struct SharedArcObject {
    arc: SharedArcDyn,
    mode: u32,
    on_last: u32,
}

impl SharedArcObject {
    /// A shared value parks nothing inside a call, so a destroy has nobody
    /// to wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_shared_arc_stats {
        subetha_shared_arc_stats {
            mode: self.mode,
            on_last: self.on_last,
            value_bytes: self.arc.value_len() as u64,
            capacity: self.arc.capacity() as u64,
            strong_count: self.arc.strong_count() as u64,
        }
    }
}

fn with_arc(handle: subetha_handle, f: impl FnOnce(&SharedArcObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_SHARED_ARC, |object| match object {
        Object::SharedArc(a) => f(a),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a shared value"),
    })
}

fn resolve_on_last(on_last: u32) -> Result<LastHolder, i32> {
    match on_last {
        SUBETHA_ARC_UNLINK => Ok(LastHolder::Unlink),
        SUBETHA_ARC_KEEP => Ok(LastHolder::Keep),
        other => Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("on_last {other} is neither SUBETHA_ARC_UNLINK nor SUBETHA_ARC_KEEP"),
        )),
    }
}

/// `SUBETHA_E_OUT_OF_BOUNDS` when `offset` and `len` name bytes the region
/// does not hold, naming the range and the length it has.
fn range_within(a: &SharedArcObject, offset: usize, len: usize) -> Result<(), i32> {
    let end = offset.checked_add(len).ok_or_else(|| {
        fail(SUBETHA_E_OUT_OF_BOUNDS, format!("offset {offset} plus {len} does not fit a size_t"))
    })?;
    if end > a.arc.value_len() {
        return Err(fail(
            SUBETHA_E_OUT_OF_BOUNDS,
            format!("{offset}..{end} runs past the {} bytes the value holds", a.arc.value_len()),
        ));
    }
    Ok(())
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    max_holders: u32,
    on_last: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, LastHolder, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if max_holders == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_holders is zero"));
    }
    let on_last = resolve_on_last(on_last)?;
    let mode = resolve_mode(mode)?;
    Ok((path, max_holders as usize, on_last, mode))
}

fn place(
    arc: Result<SharedArcDyn, ArcError>,
    mode: u32,
    on_last: u32,
    out: *mut subetha_handle,
) -> i32 {
    match arc {
        Ok(arc) => unsafe { issue(Object::SharedArc(SharedArcObject { arc, mode, on_last }), out) },
        Err(e) => arc_code(e),
    }
}

/// Obtain the value at `path` and take a holder slot: a region of
/// `value_bytes` holding `value` is initialized when the file does not
/// exist, and an existing one is attached with its contents in place.
///
/// `value` is written only by the call that creates the backing; an
/// attach leaves what is there alone, as a second `Arc::clone` leaves
/// what the first one points at. A null `value` creates the region
/// zeroed.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `value` is null or addresses
/// `value_bytes` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_create(
    path: *const c_char,
    value: *const u8,
    value_bytes: usize,
    max_holders: u32,
    on_last: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, max_holders, last, mode) =
            match unsafe { read_arguments(path, max_holders, on_last, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        if value_bytes == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "value_bytes is zero");
        }
        let zeroed;
        let bytes = if value.is_null() {
            zeroed = vec![0u8; value_bytes];
            &zeroed[..]
        } else {
            // SAFETY: checked non-null; the caller guarantees it addresses
            // value_bytes readable bytes.
            unsafe { std::slice::from_raw_parts(value, value_bytes) }
        };
        place(SharedArcDyn::create(path, bytes, max_holders, last), mode, on_last, out)
    })
}

/// Attach to the value another process created at `path`, whose region
/// must be `value_bytes` long, and take a holder slot. A backing built
/// for another length or another holder count is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_open(
    path: *const c_char,
    value_bytes: usize,
    max_holders: u32,
    on_last: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, max_holders, last, mode) =
            match unsafe { read_arguments(path, max_holders, on_last, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        place(SharedArcDyn::open(path, value_bytes, max_holders, last), mode, on_last, out)
    })
}

/// Copy `len` bytes from `offset` in the shared region into `out`, and
/// the count into `out_len`.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_read(
    handle: subetha_handle,
    offset: usize,
    len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_arc(handle, |a| {
        let buf = match unsafe { out_buffer(out, cap, out_len, len) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        if let Err(code) = range_within(a, offset, len) {
            return code;
        }
        match a.arc.read_at(offset, &mut buf[..len]) {
            Ok(()) => {
                // SAFETY: out_buffer checked it; the caller guarantees it
                // is writable.
                unsafe { *out_len = len };
                SUBETHA_OK
            }
            Err(e) => arc_code(e),
        }
    })
}

/// Copy `len` bytes from `src` into the shared region at `offset`.
///
/// Not atomic and not ordered against another holder's read; see this
/// module's note on synchronizing writes.
///
/// # Safety
/// `src` addresses `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_write(
    handle: subetha_handle,
    offset: usize,
    src: *const u8,
    len: usize,
) -> i32 {
    with_arc(handle, |a| {
        if src.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "src is null");
        }
        if let Err(code) = range_within(a, offset, len) {
            return code;
        }
        // SAFETY: checked non-null; the caller guarantees it addresses len
        // readable bytes.
        let bytes = unsafe { std::slice::from_raw_parts(src, len) };
        match a.arc.write_at(offset, bytes) {
            Ok(()) => SUBETHA_OK,
            Err(e) => arc_code(e),
        }
    })
}

/// Processes holding this value, into `out`, this one included.
///
/// A holder whose process has died still counts until something reaps it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_strong_count(handle: subetha_handle, out: *mut u64) -> i32 {
    with_arc(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let count = a.arc.strong_count() as u64;
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = count };
        SUBETHA_OK
    })
}

/// Free every holder slot whose process is gone and report how many went
/// into `out`, which may be null.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_reap_dead_holders(handle: subetha_handle, out: *mut u64) -> i32 {
    with_arc(handle, |a| {
        let freed = a.arc.reap_dead_holders() as u64;
        if !out.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is
            // writable.
            unsafe { *out = freed };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the value into `out`. The holder count races every
/// attach and release, so it describes a moment that has already passed.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_read_stats(
    handle: subetha_handle,
    out: *mut subetha_shared_arc_stats,
) -> i32 {
    with_arc(handle, |a| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = a.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Flush the shared region to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_shared_arc_flush(handle: subetha_handle) -> i32 {
    with_arc(handle, |a| match a.arc.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => arc_code(e),
    })
}

/// Remove the value's file. On Windows the file must not be mapped by any
/// handle.
///
/// A value created with `SUBETHA_ARC_UNLINK` removes its own backing when
/// the last holder releases, so this reports nothing removed once that
/// has happened.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_shared_arc_unlink(path: *const c_char, out: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => PathBuf::from(p),
            Err(code) => return code,
        };
        let mut report = UnlinkReport::default();
        report.remove(path);
        unsafe { finish_unlink(report, out) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "subetha-ffi-arc-{name}-{}.bin",
                std::process::id()
            )))
        }

        fn object(&self, value: &[u8], holders: usize, on_last: u32) -> SharedArcObject {
            let last = resolve_on_last(on_last).expect("a known policy");
            SharedArcObject {
                arc: SharedArcDyn::create(&self.0, value, holders, last).expect("a shared value"),
                mode: SUBETHA_MODE_STRICT,
                on_last,
            }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the value's file was removed: {:?}", found.first_failure);
        }
    }

    /// A snapshot answers with the region's length, the holder count and
    /// the policy the value was built with.
    #[test]
    fn a_snapshot_reports_the_region_and_its_holders() {
        let scratch = Scratch::new("stats");
        let arc = scratch.object(&[1, 2, 3, 4, 5, 6, 7, 8], 4, SUBETHA_ARC_KEEP);
        let stats = arc.stats();
        assert_eq!(stats.mode, SUBETHA_MODE_STRICT);
        assert_eq!(stats.on_last, SUBETHA_ARC_KEEP);
        assert_eq!(stats.value_bytes, 8);
        assert_eq!(stats.capacity, 4);
        assert_eq!(stats.strong_count, 1, "this process holds it");
    }

    /// A range past the region is out of bounds, which is a different
    /// answer from the backing being the wrong shape.
    #[test]
    fn a_range_past_the_region_is_out_of_bounds() {
        let scratch = Scratch::new("bounds");
        let arc = scratch.object(&[0; 8], 2, SUBETHA_ARC_KEEP);
        assert_eq!(
            range_within(&arc, 4, 8).unwrap_err(),
            crate::error::SUBETHA_E_OUT_OF_BOUNDS,
        );
        assert_eq!(
            range_within(&arc, usize::MAX, 1).unwrap_err(),
            crate::error::SUBETHA_E_OUT_OF_BOUNDS,
            "an offset that would overflow the addition is caught",
        );
        range_within(&arc, 4, 4).expect("the last four bytes are reachable");
        range_within(&arc, 8, 0).expect("an empty range at the end is inside");
    }

    /// The policy is one of two named values; anything else is refused
    /// rather than taken as a default.
    #[test]
    fn an_unknown_last_holder_policy_is_refused() {
        assert_eq!(resolve_on_last(SUBETHA_ARC_UNLINK).expect("unlink"), LastHolder::Unlink);
        assert_eq!(resolve_on_last(SUBETHA_ARC_KEEP).expect("keep"), LastHolder::Keep);
        assert_eq!(
            resolve_on_last(7).unwrap_err(),
            crate::error::SUBETHA_E_INVALID_ARGUMENT,
        );
    }
}
