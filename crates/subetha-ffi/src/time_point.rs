//! The versioned time-point tile through the C ABI.
//!
//! A tile holds a fixed number of lanes, each carrying a version and a
//! payload. A reader supplies a snapshot version and sees the lanes whose
//! own version is no greater than it, so a writer publishing above every
//! live snapshot is invisible until those readers move on. There is no
//! lock and no reader registration.
//!
//! # Two rules a caller cannot infer
//!
//! Version 0 means "no value here" and is refused on insert. A lane
//! written at version 0 would be occupied and invisible to every snapshot
//! at once, which no reader could tell from an empty lane and no writer
//! could clear.
//!
//! The visibility boundary is inclusive: a snapshot at exactly a lane's
//! version sees that lane.
//!
//! # Reading a whole tile
//!
//! `subetha_tile_visible_mask` answers a bit per lane in one call, so a
//! caller tests sixteen lanes without sixteen boundary crossings, then
//! reads only the payloads it wants.

use std::ffi::c_char;

use subetha_cxc::raw_time_point::{RawTileError, RawTimePointTile};
use subetha_cxc::shared_time_point::{TileError, SLOT_PAYLOAD, TILE_CAP};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_FULL, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_TIME_POINT};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// Lanes in a tile.
pub const SUBETHA_TILE_LANES: u32 = 16;
/// The most a lane's payload can carry.
pub const SUBETHA_TILE_MAX_PAYLOAD: u64 = 56;

// Literals, because cbindgen copies a constant's expression rather than
// its value. These assertions are what keeps them honest.
const _: () = assert!(SUBETHA_TILE_LANES as usize == TILE_CAP);
const _: () = assert!(SUBETHA_TILE_MAX_PAYLOAD as usize == SLOT_PAYLOAD);

pub(crate) struct TileObject {
    tile: RawTimePointTile,
    mode: u32,
}

impl TileObject {
    /// Nothing parks on a tile, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

/// A snapshot of a shared tile.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_tile_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Lanes the tile has, which is always `SUBETHA_TILE_LANES`.
    pub capacity: u32,
    /// Lanes occupied now, whether or not any snapshot can see them.
    pub len: u32,
    /// Bytes in a lane's payload.
    pub payload_size: u64,
}

fn code_for(e: RawTileError) -> i32 {
    match e {
        RawTileError::WrongSize { expected, found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("this tile uses {expected}-byte payloads, and {found} was given"),
        ),
        RawTileError::BadPayloadSize { asked, max } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("a payload of {asked} bytes does not fit a lane, which holds {max}"),
        ),
        RawTileError::ZeroVersion => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "version 0 means no value, so it cannot be published",
        ),
        RawTileError::Full => fail(SUBETHA_E_MAP_FULL, "every lane is taken"),
        RawTileError::Tile(TileError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "that file is not a tile, or was built with a different payload size",
        ),
        RawTileError::Tile(TileError::IoError(k)) => {
            fail(SUBETHA_E_RING_IO, format!("the tile could not be reached: {k:?}"))
        }
        RawTileError::Tile(other) => {
            fail(SUBETHA_E_RING_IO, format!("the tile refused: {other:?}"))
        }
    }
}

fn with_tile(handle: subetha_handle, f: impl FnOnce(&TileObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_TIME_POINT, |object| match object {
        Object::Tile(t) => f(t),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a time-point tile"),
    })
}

/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments(
    path: *const c_char,
    payload_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'static str, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = unsafe { text(path, "path") }?;
    let mode = resolve_mode(mode)?;
    Ok((path, payload_size as usize, mode))
}

/// Create a tile at `path` whose lanes carry `payload_size` bytes, at most
/// `SUBETHA_TILE_MAX_PAYLOAD`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_create(
    path: *const c_char,
    payload_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, payload_size, mode) =
            match unsafe { read_arguments(path, payload_size, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match RawTimePointTile::create(path, payload_size) {
            Ok(t) => unsafe { issue(Object::Tile(TileObject { tile: t, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a tile another process created. Its header must agree about
/// the payload size, since otherwise the two callers would disagree about
/// how many of a lane's bytes mean anything.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_open(
    path: *const c_char,
    payload_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, payload_size, mode) =
            match unsafe { read_arguments(path, payload_size, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match RawTimePointTile::open(path, payload_size) {
            Ok(t) => unsafe { issue(Object::Tile(TileObject { tile: t, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Publish `value` at `version`, writing the lane it took into `lane_out`.
///
/// `version` must not be zero. The payload reaches the lane before the
/// version does, so a reader that sees the version sees the whole payload.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `lane_out` is valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_insert(
    handle: subetha_handle,
    version: u64,
    value: *const u8,
    value_len: u64,
    lane_out: *mut u32,
) -> i32 {
    with_tile(handle, |t| {
        if lane_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "lane_out is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match t.tile.insert(version, value) {
            Ok(lane) => {
                // Checked non-null above.
                unsafe { *lane_out = lane as u32 };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Free `lane`.
///
/// The version is cleared before the lane is released, so a lane reclaimed
/// afterwards cannot be seen through its predecessor's version.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_tile_remove(handle: subetha_handle, lane: u32) -> i32 {
    with_tile(handle, |t| {
        t.tile.remove(lane as usize);
        SUBETHA_OK
    })
}

/// Read the payload at `lane` into `value_out` and its version into
/// `version_out`.
///
/// `present` is set to whether the lane holds a published value. A lane
/// that is claimed but not yet published reads as absent, which is what
/// keeps a half-written payload from being seen.
///
/// # Safety
/// `value_out` points to at least `value_len` bytes; `version_out` and
/// `present` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_at(
    handle: subetha_handle,
    lane: u32,
    value_out: *mut u8,
    value_len: u64,
    version_out: *mut u64,
    present: *mut bool,
) -> i32 {
    with_tile(handle, |t| {
        if value_out.is_null() || version_out.is_null() || present.is_null() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "value_out, version_out or present is null",
            );
        }
        let mut scratch = vec![0u8; value_len as usize];
        match t.tile.at(lane as usize, &mut scratch) {
            Ok(Some(version)) => {
                // Checked non-null above.
                unsafe {
                    std::ptr::copy_nonoverlapping(scratch.as_ptr(), value_out, scratch.len());
                    *version_out = version;
                    *present = true;
                }
                SUBETHA_OK
            }
            Ok(None) => {
                // Nothing written to value_out: an absent lane leaves the
                // caller's buffer as it was rather than half-filled.
                unsafe { *present = false };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// A bit per lane visible at `snapshot`, into `mask_out`.
///
/// Bit `n` is set when lane `n` is occupied, published, and carries a
/// version no greater than `snapshot`. The boundary is inclusive.
///
/// # Safety
/// `mask_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_visible_mask(
    handle: subetha_handle,
    snapshot: u64,
    mask_out: *mut u32,
) -> i32 {
    with_tile(handle, |t| {
        if mask_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "mask_out is null");
        }
        let mask = t.tile.visible_mask(snapshot);
        // Widened to u32 for the boundary; the tile has 16 lanes, so the
        // high half is always zero.
        unsafe { *mask_out = u32::from(mask) };
        SUBETHA_OK
    })
}

/// How many lanes `snapshot` sees, into `count_out`.
///
/// # Safety
/// `count_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_visible_count(
    handle: subetha_handle,
    snapshot: u64,
    count_out: *mut u32,
) -> i32 {
    with_tile(handle, |t| {
        if count_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "count_out is null");
        }
        // Checked non-null above.
        unsafe { *count_out = t.tile.visible_count(snapshot) };
        SUBETHA_OK
    })
}

/// Read the tile's shape and occupancy into `out`.
///
/// `len` counts occupied lanes whether or not any snapshot can see them,
/// so it can exceed what `subetha_tile_visible_count` answers.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tile_read_stats(
    handle: subetha_handle,
    out: *mut subetha_tile_stats,
) -> i32 {
    with_tile(handle, |t| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_tile_stats {
            mode: t.mode,
            capacity: t.tile.capacity() as u32,
            len: t.tile.len() as u32,
            payload_size: t.tile.payload_size() as u64,
        };
        // Checked non-null above.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Push the tile to disk and wait for it.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_tile_flush(handle: subetha_handle) -> i32 {
    with_tile(handle, |t| match t.tile.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}
