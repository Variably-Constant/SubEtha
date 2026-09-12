//! The cascade tower through the C ABI.
//!
//! A tower of `depth` levels. The last holds values; each level above
//! holds one index per slot naming a slot on the level below. A cascade is
//! a path of `depth` indices, one per level.
//!
//! No maximum depth is imposed. Paths cross as caller-provided arrays of
//! exactly `depth` entries, so nothing here guesses how deep a caller
//! goes and no variable-length result has to cross the boundary.
//!
//! # A path validates itself
//!
//! `subetha_tower_get` does not follow the indices it is given. At every
//! level it checks the slot named there actually points at the next index
//! in the path, and refuses at the first level that disagrees. A path kept
//! across a change that rewrote an intermediate slot names the level that
//! stopped agreeing rather than resolving to whatever value now sits at
//! the end of it. That check is the reason to use a tower rather than a
//! bare leaf index.

use std::ffi::c_char;
use std::path::PathBuf;

use subetha_cxc::raw_k_tower::{RawKTower, RawTowerError, RAW_TOWER_NIL};
use subetha_cxc::shared_region::RegionError;

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_FULL, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_K_TOWER};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The index that means "nothing here" at any level of a path.
pub const SUBETHA_TOWER_NIL: u32 = u32::MAX;

// A literal pinned to the core, because cbindgen copies a constant's
// expression rather than its value.
const _: () = assert!(SUBETHA_TOWER_NIL == RAW_TOWER_NIL);

pub(crate) struct TowerObject {
    tower: RawKTower,
    mode: u32,
}

impl TowerObject {
    /// Nothing parks on a tower, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

/// A snapshot of a shared tower.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_tower_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Levels including the leaf. A path has this many entries.
    pub depth: u32,
    /// Values stored at the leaf.
    pub len: u64,
    /// Bytes in a leaf value.
    pub value_size: u64,
}

fn code_for(e: RawTowerError) -> i32 {
    match e {
        RawTowerError::WrongSize { expected, found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("this tower expects {expected} here, and {found} was given"),
        ),
        RawTowerError::ZeroDepth => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "a tower needs at least one level")
        }
        RawTowerError::NilAtLevel(level) => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("the path holds nil at level {level}"),
        ),
        RawTowerError::BrokenAtLevel(level) => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!(
                "the path no longer describes the tower from level {level} down: \
                 the slot named there does not point at the next index"
            ),
        ),
        RawTowerError::Region(RegionError::Full) => {
            fail(SUBETHA_E_MAP_FULL, "a level of the tower has no free slot")
        }
        RawTowerError::Region(RegionError::InvalidPtr) => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the path names a slot that is not live")
        }
        RawTowerError::Region(RegionError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "a level on disk was built with a different shape",
        ),
        RawTowerError::Region(RegionError::IoError(k)) => {
            fail(SUBETHA_E_RING_IO, format!("the tower could not be reached: {k:?}"))
        }
        RawTowerError::Region(other) => {
            fail(SUBETHA_E_RING_IO, format!("the tower refused: {other:?}"))
        }
    }
}

fn with_tower(handle: subetha_handle, f: impl FnOnce(&TowerObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_K_TOWER, |object| match object {
        Object::Tower(t) => f(t),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a tower"),
    })
}

/// The intermediate levels, top first, as paths and capacities.
///
/// # Safety
/// `level_paths` points to `n_levels` NUL-terminated UTF-8 strings and
/// `level_capacities` to `n_levels` values; either may be null when
/// `n_levels` is zero.
unsafe fn read_levels(
    level_paths: *const *const c_char,
    level_capacities: *const u64,
    n_levels: u64,
) -> Result<Vec<(PathBuf, usize)>, i32> {
    let n = n_levels as usize;
    if n == 0 {
        return Ok(Vec::new());
    }
    if level_paths.is_null() || level_capacities.is_null() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "level_paths or level_capacities is null with n_levels above zero",
        ));
    }
    let mut levels = Vec::with_capacity(n);
    for i in 0..n {
        // SAFETY: the caller guarantees n_levels entries in each array.
        let path_ptr = unsafe { *level_paths.add(i) };
        let capacity = unsafe { *level_capacities.add(i) };
        let path = unsafe { text(path_ptr, "level path") }?;
        levels.push((PathBuf::from(path), capacity as usize));
    }
    Ok(levels)
}

/// Everything a constructor needs once the C arguments have been read:
/// the leaf path, the value size, the intermediate levels top first, and
/// the resolved mode.
type TowerArguments = (&'static str, usize, Vec<(PathBuf, usize)>, u32);

/// # Safety
/// As [`subetha_tower_create`].
#[allow(clippy::too_many_arguments)]
unsafe fn read_arguments(
    leaf_path: *const c_char,
    value_size: u64,
    level_paths: *const *const c_char,
    level_capacities: *const u64,
    n_levels: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<TowerArguments, i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let leaf = unsafe { text(leaf_path, "leaf_path") }?;
    let levels = unsafe { read_levels(level_paths, level_capacities, n_levels) }?;
    let mode = resolve_mode(mode)?;
    Ok((leaf, value_size as usize, levels, mode))
}

/// Create a tower whose leaf at `leaf_path` holds `leaf_capacity` values
/// of `value_size` bytes, with one intermediate level per entry in
/// `level_paths` / `level_capacities`, top first.
///
/// Depth is `n_levels + 1`. Zero levels makes a depth-1 tower, which is a
/// bare region with a one-entry path.
///
/// # Safety
/// `leaf_path` is a NUL-terminated UTF-8 string; `level_paths` points to
/// `n_levels` such strings and `level_capacities` to `n_levels` values;
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_tower_create(
    leaf_path: *const c_char,
    leaf_capacity: u64,
    value_size: u64,
    level_paths: *const *const c_char,
    level_capacities: *const u64,
    n_levels: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (leaf, value_size, levels, mode) = match unsafe {
            read_arguments(leaf_path, value_size, level_paths, level_capacities, n_levels, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawKTower::create(leaf, leaf_capacity as usize, value_size, &levels) {
            Ok(t) => unsafe { issue(Object::Tower(TowerObject { tower: t, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a tower another process created, with the same shape.
///
/// # Safety
/// As [`subetha_tower_create`].
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_tower_open(
    leaf_path: *const c_char,
    leaf_capacity: u64,
    value_size: u64,
    level_paths: *const *const c_char,
    level_capacities: *const u64,
    n_levels: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (leaf, value_size, levels, mode) = match unsafe {
            read_arguments(leaf_path, value_size, level_paths, level_capacities, n_levels, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawKTower::open(leaf, leaf_capacity as usize, value_size, &levels) {
            Ok(t) => unsafe { issue(Object::Tower(TowerObject { tower: t, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// The path buffer a caller hands in, of exactly `depth` entries.
///
/// # Safety
/// `path_out` points to at least `path_len` writable `u32`s.
unsafe fn path_slice<'a>(path_out: *mut u32, path_len: u64) -> Result<&'a mut [u32], i32> {
    if path_out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "path is null"));
    }
    // SAFETY: the caller guarantees path_len writable entries.
    Ok(unsafe { std::slice::from_raw_parts_mut(path_out, path_len as usize) })
}

/// Store `value` and hang it off the next free slot on the top level,
/// writing the path into `path_out`, which must hold exactly `depth`
/// entries.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `path_out` points to at
/// least `path_len` writable `u32`s.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tower_append(
    handle: subetha_handle,
    value: *const u8,
    value_len: u64,
    path_out: *mut u32,
    path_len: u64,
) -> i32 {
    with_tower(handle, |t| {
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        let path = match unsafe { path_slice(path_out, path_len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match t.tower.append(value, path) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// Store `value` and build the chain of links down to it from `top_slot`
/// on the top level, writing the path into `path_out`.
///
/// The leaf is written first and the top link last, so a reader walking
/// down never reaches a level that does not yet name a live slot below.
///
/// # Safety
/// As [`subetha_tower_append`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tower_insert_at_top(
    handle: subetha_handle,
    top_slot: u32,
    value: *const u8,
    value_len: u64,
    path_out: *mut u32,
    path_len: u64,
) -> i32 {
    with_tower(handle, |t| {
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        let path = match unsafe { path_slice(path_out, path_len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match t.tower.insert_at_top(top_slot, value, path) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// Read the value `path` names into `value_out`, checking every level.
///
/// Refused with the level that failed rather than resolving a path the
/// tower no longer agrees with, and nothing is written on a refusal.
///
/// # Safety
/// `path` points to at least `path_len` readable `u32`s; `value_out`
/// points to at least `value_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tower_get(
    handle: subetha_handle,
    path: *const u32,
    path_len: u64,
    value_out: *mut u8,
    value_len: u64,
) -> i32 {
    with_tower(handle, |t| {
        if path.is_null() || value_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "path or value_out is null");
        }
        // SAFETY: the caller guarantees path_len readable entries.
        let path = unsafe { std::slice::from_raw_parts(path, path_len as usize) };
        let mut scratch = vec![0u8; value_len as usize];
        match t.tower.get(path, &mut scratch) {
            Ok(()) => {
                // Checked non-null above.
                unsafe {
                    std::ptr::copy_nonoverlapping(scratch.as_ptr(), value_out, scratch.len());
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Read the tower's shape and occupancy into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tower_read_stats(
    handle: subetha_handle,
    out: *mut subetha_tower_stats,
) -> i32 {
    with_tower(handle, |t| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_tower_stats {
            mode: t.mode,
            depth: t.tower.depth() as u32,
            len: t.tower.len() as u64,
            value_size: t.tower.value_size() as u64,
        };
        // Checked non-null above.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Push every level to disk and wait for them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_tower_flush(handle: subetha_handle) -> i32 {
    with_tower(handle, |t| match t.tower.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}
