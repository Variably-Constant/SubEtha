//! The shared laned versioned map through the C ABI: one versioned tree
//! per lane over a shared epoch table, so several writers make progress at
//! once while readers scan every lane at one pinned moment.
//!
//! A statement claims a lane, writes through it and releases it. A key
//! belongs to one lane for its whole life, so a removal aimed at the wrong
//! lane is refused with `SUBETHA_E_WRONG_KIND` naming the lane that holds
//! it, rather than reported as absent: a lane is a separate tree, and
//! removing it here would report a row gone that no reader has stopped
//! seeing.
//!
//! Lanes are claimed by index rather than by a guard, because a C caller
//! has nowhere to keep one. A holder that dies without releasing is
//! reclaimed by `subetha_laned_map_reap_dead_claims`.
//!
//! A range yields one page ending at the smallest last key any lane
//! reported, and reports that frontier so the caller can resume past it.
//! Rows beyond the frontier are withheld even where a lane already walked
//! them, because a lane that stopped earlier may still hold a smaller key.

use std::ffi::c_char;
use std::ops::Bound;
use std::path::Path;

use subetha_cxc::laned_versioned_map::LanedError;
use subetha_cxc::raw_laned_versioned_map::RawLanedVersionedMap;

use crate::error::{
    fail, SUBETHA_E_BUFFER_TOO_SMALL, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_KEY_ABSENT,
    SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WOULD_BLOCK,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LANED_MAP};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a shared laned versioned map.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_laned_map_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Lanes the map holds.
    pub lanes: u32,
    /// Lanes a statement is holding right now.
    pub held_lanes: u32,
    /// Entries across every lane, tombstones not yet reclaimed included.
    pub len: u64,
    /// Bytes a key takes.
    pub key_size: u64,
    /// Bytes a value takes.
    pub value_size: u64,
    /// The epoch the shared table stands at now.
    pub epoch: u64,
}

pub(crate) struct LanedMapObject {
    map: RawLanedVersionedMap,
    mode: u32,
}

impl LanedMapObject {
    /// A map parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: LanedError) -> i32 {
    match e {
        LanedError::NoFreeLane => {
            fail(SUBETHA_E_WOULD_BLOCK, "every lane is held by another statement")
        }
        LanedError::LaneBusy(i) => fail(
            SUBETHA_E_WOULD_BLOCK,
            format!("lane {i} holds this key and another statement holds the lane"),
        ),
        LanedError::KeyAbsent => SUBETHA_E_MAP_KEY_ABSENT,
        LanedError::KeyInAnotherLane(i) => fail(
            SUBETHA_E_WRONG_KIND,
            format!("the key is in lane {i}; it must be removed there"),
        ),
        LanedError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the map on disk was built with another shape",
        ),
        LanedError::Versioned(e) => fail(SUBETHA_E_RING_IO, format!("lane: {e}")),
        LanedError::Epochs(e) => fail(SUBETHA_E_RING_IO, format!("epoch table: {e}")),
        LanedError::Io(e) => fail(SUBETHA_E_RING_IO, format!("claims file: {}", e.kind())),
    }
}

fn with_laned(handle: subetha_handle, f: impl FnOnce(&LanedMapObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LANED_MAP, |object| match object {
        Object::LanedMap(m) => f(m),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a laned map"),
    })
}

/// The arguments both constructors read, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `dir` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[allow(clippy::too_many_arguments)]
unsafe fn read_arguments<'a>(
    dir: *const c_char,
    lanes: u32,
    nodes_per_lane: u64,
    key_size: usize,
    value_size: usize,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, usize, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let dir = Path::new(unsafe { text(dir, "dir") }?);
    if lanes == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "lanes is zero"));
    }
    if nodes_per_lane == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "nodes_per_lane is zero"));
    }
    if key_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "key_size is zero"));
    }
    if value_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "value_size is zero"));
    }
    if max_pins == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_pins is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((dir, lanes as usize, nodes_per_lane as usize, max_pins as usize, mode))
}

/// Obtain the laned map under `dir`: `lanes` trees of `nodes_per_lane`
/// nodes each for `key_size`-byte keys and `value_size`-byte values, one
/// epoch table they share holding `max_pins` pins, and the claims table
/// beside them.
///
/// # Safety
/// `dir` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_laned_map_create(
    dir: *const c_char,
    lanes: u32,
    nodes_per_lane: u64,
    key_size: usize,
    value_size: usize,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (dir, lanes, nodes, pins, mode) = match unsafe {
            read_arguments(dir, lanes, nodes_per_lane, key_size, value_size, max_pins, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawLanedVersionedMap::create(dir, lanes, nodes, key_size, value_size, pins) {
            Ok(map) => unsafe { issue(Object::LanedMap(LanedMapObject { map, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the laned map another process created under `dir`; its files
/// must exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `dir` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_laned_map_open(
    dir: *const c_char,
    lanes: u32,
    nodes_per_lane: u64,
    key_size: usize,
    value_size: usize,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (dir, lanes, nodes, pins, mode) = match unsafe {
            read_arguments(dir, lanes, nodes_per_lane, key_size, value_size, max_pins, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawLanedVersionedMap::open(dir, lanes, nodes, key_size, value_size, pins) {
            Ok(map) => unsafe { issue(Object::LanedMap(LanedMapObject { map, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Claim any free lane, for a statement inserting keys that do not yet
/// exist, and report which into `out_lane`. `SUBETHA_E_WOULD_BLOCK` when
/// every lane is held.
///
/// # Safety
/// `out_lane` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_claim_lane(
    handle: subetha_handle,
    out_lane: *mut u32,
) -> i32 {
    with_laned(handle, |m| {
        if out_lane.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_lane is null");
        }
        match m.map.claim_any_lane() {
            Some(lane) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_lane = lane as u32 };
                SUBETHA_OK
            }
            None => code_for(LanedError::NoFreeLane),
        }
    })
}

/// Claim the lane that owns `key`, for a statement that must remove or
/// rewrite a key that already exists, and report which into `out_lane`.
/// `SUBETHA_E_MAP_KEY_ABSENT` when no lane holds it and
/// `SUBETHA_E_WOULD_BLOCK` when its lane is held by another statement,
/// which the caller retries rather than writing elsewhere, because
/// elsewhere is a different tree.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out_lane` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_claim_lane_for(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out_lane: *mut u32,
) -> i32 {
    with_laned(handle, |m| {
        if out_lane.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_lane is null");
        }
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let lane = match m.map.lane_of(key) {
            Ok(Some(i)) => i,
            Ok(None) => return SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => return code_for(e),
        };
        if m.map.claim_lane_index(lane) {
            // Checked non-null; the caller guarantees it is writable.
            unsafe { *out_lane = lane as u32 };
            SUBETHA_OK
        } else {
            code_for(LanedError::LaneBusy(lane))
        }
    })
}

/// Release a lane this process claimed, so another statement may take it.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_laned_map_release_lane(handle: subetha_handle, lane: u32) -> i32 {
    with_laned(handle, |m| {
        if lane as usize >= m.map.lanes() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "that lane index is past the last lane");
        }
        m.map.release_lane(lane as usize);
        SUBETHA_OK
    })
}

/// Release the lanes of holders whose process is gone, reporting how many
/// came back through `out_reaped` when that is not null.
///
/// # Safety
/// `out_reaped` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_reap_dead_claims(
    handle: subetha_handle,
    out_reaped: *mut u64,
) -> i32 {
    with_laned(handle, |m| {
        let reaped = m.map.reap_dead_claims();
        if !out_reaped.is_null() {
            // Checked non-null; the caller guarantees it is writable.
            unsafe { *out_reaped = reaped as u64 };
        }
        SUBETHA_OK
    })
}

/// Make `value` current at `key` in `lane`, at a fresh epoch. The caller
/// holds `lane`.
///
/// # Safety
/// `key` points to `key_len` readable bytes and `value` to `value_len`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_insert(
    handle: subetha_handle,
    lane: u32,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    with_laned(handle, |m| {
        if key_len != m.map.key_size() || value_len != m.map.value_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!(
                    "a key is {} bytes and a value {}, not {key_len} and {value_len}",
                    m.map.key_size(),
                    m.map.value_size()
                ),
            );
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let value = match unsafe { bytes(value, value_len) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match m.map.insert_in(lane as usize, key, value) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// Stamp `key` superseded in `lane` at a fresh epoch, writing what was
/// current into `out`. `SUBETHA_E_WRONG_KIND` names the lane that holds
/// the key when this one does not.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_laned_map_remove(
    handle: subetha_handle,
    lane: u32,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_laned(handle, |m| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        let value_size = m.map.value_size();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = value_size };
        if out.is_null() || cap < value_size {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, value_size) };
        match m.map.remove_in(lane as usize, key, buf) {
            Ok(true) => SUBETHA_OK,
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// The value current at `key` into `out`, from whichever lane holds it.
/// `SUBETHA_E_MAP_KEY_ABSENT` when no lane does.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_get(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_laned(handle, |m| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        let value_size = m.map.value_size();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = value_size };
        if out.is_null() || cap < value_size {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, value_size) };
        match m.map.get(key, buf) {
            Ok(true) => SUBETHA_OK,
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// The lane holding `key` into `out_lane`.
/// `SUBETHA_E_MAP_KEY_ABSENT` when no lane does.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out_lane` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_lane_of(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out_lane: *mut u32,
) -> i32 {
    with_laned(handle, |m| {
        if out_lane.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_lane is null");
        }
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        match m.map.lane_of(key) {
            Ok(Some(lane)) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_lane = lane as u32 };
                SUBETHA_OK
            }
            Ok(None) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// One page of the entries current at one moment, merged across every
/// lane in key order, packed into `out` as key bytes then value bytes per
/// entry.
///
/// `after` is null to start, or the frontier of the previous page to
/// resume strictly past it. `out_frontier` receives the frontier this page
/// reached and `out_has_frontier` whether there is one: no frontier means
/// every lane reached the end and the walk is done. Rows beyond the
/// frontier are withheld even where a lane already walked them, because a
/// lane that stopped earlier may still hold a smaller key.
///
/// # Safety
/// `after` and `out_frontier` are null or point to `key_size` bytes;
/// `out` points to `cap` writable bytes; the out pointers are valid.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_laned_map_range(
    handle: subetha_handle,
    after: *const u8,
    limit: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_count: *mut usize,
    out_frontier: *mut u8,
    out_has_frontier: *mut bool,
) -> i32 {
    with_laned(handle, |m| {
        if out_len.is_null() || out_count.is_null() || out_has_frontier.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "an out pointer is null");
        }
        let key_size = m.map.key_size();
        let stride = key_size + m.map.value_size();
        let after = if after.is_null() {
            None
        } else {
            match unsafe { bytes(after, key_size) } {
                Ok(b) => Some(b),
                Err(code) => return code,
            }
        };
        let pin = match m.map.pin() {
            Ok(p) => p,
            Err(e) => return code_for(e),
        };
        let low = after.map_or(Bound::Unbounded, Bound::Excluded);
        let (rows, frontier) = m.map.range_at_with_cursor(low, Bound::Unbounded, limit, &pin);
        let needed = rows.len() * stride;
        // Checked non-null; the caller guarantees they are writable.
        unsafe {
            *out_len = needed;
            *out_count = rows.len();
            *out_has_frontier = frontier.is_some();
        }
        if out.is_null() || cap < needed {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap >= needed.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, needed) };
        for (i, (k, v)) in rows.iter().enumerate() {
            let at = i * stride;
            buf[at..at + key_size].copy_from_slice(k);
            buf[at + key_size..at + stride].copy_from_slice(v);
        }
        if let Some(f) = frontier
            && !out_frontier.is_null()
        {
            // The caller guarantees `key_size` writable bytes there.
            let slot = unsafe { std::slice::from_raw_parts_mut(out_frontier, key_size) };
            slot.copy_from_slice(&f);
        }
        SUBETHA_OK
    })
}

/// Drop every tombstone no pin can reach, in every lane, reporting how
/// many went through `out_freed` when that is not null.
///
/// # Safety
/// `out_freed` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_sweep(
    handle: subetha_handle,
    out_freed: *mut u64,
) -> i32 {
    with_laned(handle, |m| match m.map.sweep() {
        Ok(freed) => {
            if !out_freed.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_freed = freed as u64 };
            }
            SUBETHA_OK
        }
        Err(e) => code_for(e),
    })
}

/// Undo every stamp at `epoch` in every lane, for an epoch whose holder
/// died part way through a compound write. Reports the entries touched
/// through `out_touched` when that is not null.
///
/// # Safety
/// `out_touched` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_void_epoch(
    handle: subetha_handle,
    epoch: u64,
    out_touched: *mut u64,
) -> i32 {
    with_laned(handle, |m| match m.map.void_epoch(epoch) {
        Ok(touched) => {
            if !out_touched.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_touched = touched as u64 };
            }
            SUBETHA_OK
        }
        Err(e) => code_for(e),
    })
}

/// Push every lane's dirty pages to disk, returning when they are durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_laned_map_flush(handle: subetha_handle) -> i32 {
    with_laned(handle, |m| match m.map.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the map into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_laned_map_read_stats(
    handle: subetha_handle,
    out: *mut subetha_laned_map_stats,
) -> i32 {
    with_laned(handle, |m| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_laned_map_stats {
            mode: m.mode,
            lanes: m.map.lanes() as u32,
            held_lanes: m.map.held_lanes() as u32,
            len: m.map.len() as u64,
            key_size: m.map.key_size() as u64,
            value_size: m.map.value_size() as u64,
            epoch: m.map.epochs().now(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
