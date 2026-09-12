//! The strategy-switching set through the C ABI.
//!
//! A set of fixed-size elements backed by either a vector scanned
//! linearly or a hash map, and movable between them while it holds data.
//! Nothing here migrates on its own: `subetha_universal_migrate` is
//! called or it is not, and the operation counts are published so the
//! caller can decide. A policy that reads those counts belongs above this
//! layer, where knowledge of the access pattern lives.
//!
//! # How a reader knows the ground moved
//!
//! One word carries the strategy, a generation and a version, rewritten
//! as a unit by a migration. A reader that takes the stamp before and
//! after an operation and sees the same value knows no migration
//! interleaved. That is what makes a lockless read safe: not that
//! migrations are prevented, but that they are detectable.

use std::ffi::c_char;

use subetha_cxc::raw_universal::{RawUniversal, RawUniversalError};
use subetha_cxc::shared_hash_map::MapError;
use subetha_cxc::shared_universal::Strategy;
use subetha_cxc::shared_vec::VecError;

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_FULL, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WOULD_BLOCK, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_UNIVERSAL};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// Elements in a vector, scanned linearly. Faster for a small set.
pub const SUBETHA_UNIVERSAL_VEC: u32 = 0;
/// Elements in a hash map. Faster for a large one.
pub const SUBETHA_UNIVERSAL_MAP: u32 = 1;

pub(crate) struct UniversalObject {
    set: RawUniversal,
    mode: u32,
}

impl UniversalObject {
    /// Nothing parks on a set, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

/// A snapshot of a shared strategy-switching set.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_universal_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of `SUBETHA_UNIVERSAL_VEC`, `SUBETHA_UNIVERSAL_MAP`.
    pub strategy: u32,
    /// Migrations since creation, rolling into `generation` at overflow.
    pub version: u32,
    /// Carries the version's overflow, so the pair is unique.
    pub generation: u32,
    /// The word a reader compares across an operation. Equal before and
    /// after means no migration interleaved.
    pub stamp: u64,
    /// Elements held now.
    pub len: u64,
    /// Inserts attempted since this handle attached.
    pub inserts: u64,
    /// Membership tests since this handle attached.
    pub contains: u64,
    /// Bytes in an element.
    pub element_size: u64,
}

fn strategy_of(tag: u32) -> Result<Strategy, i32> {
    match tag {
        SUBETHA_UNIVERSAL_VEC => Ok(Strategy::Vec),
        SUBETHA_UNIVERSAL_MAP => Ok(Strategy::Map),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("strategy {other} names none"))),
    }
}

fn tag_of(s: Strategy) -> u32 {
    match s {
        Strategy::Vec => SUBETHA_UNIVERSAL_VEC,
        Strategy::Map => SUBETHA_UNIVERSAL_MAP,
    }
}

fn code_for(e: RawUniversalError) -> i32 {
    match e {
        RawUniversalError::WrongSize { expected, found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("this set holds {expected}-byte elements, and {found} was given"),
        ),
        RawUniversalError::ZeroElement => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "an element of zero bytes carries nothing")
        }
        RawUniversalError::Full => fail(SUBETHA_E_MAP_FULL, "the set is full"),
        RawUniversalError::VersionExhausted => fail(
            SUBETHA_E_MAP_FULL,
            "every migration stamp has been used; no further migration can be told apart",
        ),
        RawUniversalError::StateMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the set's state file was built for another capacity or element size",
        ),
        RawUniversalError::MigrationRaced { published } => fail(
            SUBETHA_E_WOULD_BLOCK,
            format!("another process migrated the set under this call; the word that stands is {published:#x}"),
        ),
        RawUniversalError::Vec(VecError::LayoutMismatch)
        | RawUniversalError::Map(MapError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the set on disk was built with a different element size",
        ),
        RawUniversalError::Vec(other) => {
            fail(SUBETHA_E_RING_IO, format!("the set's vector backing refused: {other:?}"))
        }
        RawUniversalError::Map(other) => {
            fail(SUBETHA_E_RING_IO, format!("the set's map backing refused: {other:?}"))
        }
        RawUniversalError::Io(k) => {
            fail(SUBETHA_E_RING_IO, format!("the set could not be reached: {k:?}"))
        }
    }
}

fn with_set(handle: subetha_handle, f: impl FnOnce(&UniversalObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_UNIVERSAL, |object| match object {
        Object::Universal(u) => f(u),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a strategy-switching set"),
    })
}

/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments(
    base_path: *const c_char,
    element_size: u64,
    strategy: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'static str, usize, Strategy, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = unsafe { text(base_path, "base_path") }?;
    let mode = resolve_mode(mode)?;
    let strategy = strategy_of(strategy)?;
    Ok((path, element_size as usize, strategy, mode))
}

/// Obtain the set under `base_path` holding up to `capacity` elements of
/// `element_size` bytes: a new one starts on `strategy`, and one that
/// already exists keeps the strategy in force, which lives in a state
/// file beside the two backings and is what every handle reads.
///
/// Both backings are created up front, so a later migration creates no
/// file while callers are attached.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_universal_create(
    base_path: *const c_char,
    capacity: u64,
    element_size: u64,
    strategy: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, element_size, strategy, mode) =
            match unsafe { read_arguments(base_path, element_size, strategy, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match RawUniversal::create(path, capacity as usize, element_size, strategy) {
            Ok(s) => unsafe {
                issue(Object::Universal(UniversalObject { set: s, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a set another process created under `base_path`. The
/// strategy in force is read from the set's state file, so this handle
/// holds no belief of its own; a migration through any handle is what
/// this one reads on its next call, and the stamp in the stats says when
/// that happened.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_universal_open(
    base_path: *const c_char,
    capacity: u64,
    element_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        // The vector strategy stands in for the argument the open does
        // not take; the state file decides.
        let (path, element_size, _, mode) =
            match unsafe { read_arguments(base_path, element_size, SUBETHA_UNIVERSAL_VEC, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        match RawUniversal::open(path, capacity as usize, element_size) {
            Ok(s) => unsafe {
                issue(Object::Universal(UniversalObject { set: s, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Add `value` if it is not already present.
///
/// `added` is set to true when it was added and false when the set
/// already held it. It may be null when the caller does not care.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `added` is null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_universal_insert(
    handle: subetha_handle,
    value: *const u8,
    value_len: u64,
    added: *mut bool,
) -> i32 {
    with_set(handle, |u| {
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match u.set.insert(value) {
            Ok(was_new) => {
                if !added.is_null() {
                    // Checked non-null; the caller guarantees it is writable.
                    unsafe { *added = was_new };
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Whether the set holds `value`, into `present`.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `present` is valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_universal_contains(
    handle: subetha_handle,
    value: *const u8,
    value_len: u64,
    present: *mut bool,
) -> i32 {
    with_set(handle, |u| {
        if present.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "present is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match u.set.contains(value) {
            Ok(hit) => {
                // Checked non-null above.
                unsafe { *present = hit };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Move the set to `strategy`, carrying every element across.
///
/// Elements reach the target backing before the stamp changes, so a
/// reader whose stamp is unchanged was reading a complete backing
/// throughout. A failure part-way leaves the stamp untouched and the set
/// on its old backing. Migrating to the strategy already in force changes
/// nothing and does not advance the stamp.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_universal_migrate(handle: subetha_handle, strategy: u32) -> i32 {
    with_set(handle, |u| {
        let target = match strategy_of(strategy) {
            Ok(s) => s,
            Err(code) => return code,
        };
        match u.set.migrate_to(target) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// Empty the live backing.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_universal_clear(handle: subetha_handle) -> i32 {
    with_set(handle, |u| match u.set.clear() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Read the strategy word, the counts and the occupancy into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_universal_read_stats(
    handle: subetha_handle,
    out: *mut subetha_universal_stats,
) -> i32 {
    with_set(handle, |u| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let len = match u.set.len() {
            Ok(n) => n as u64,
            Err(e) => return code_for(e),
        };
        let s = u.set.state();
        let stats = subetha_universal_stats {
            mode: u.mode,
            strategy: tag_of(s.strategy),
            version: s.version,
            generation: u32::from(s.generation),
            stamp: s.stamp(),
            len,
            inserts: s.inserts,
            contains: s.contains,
            element_size: u.set.element_size() as u64,
        };
        // Checked non-null above.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Push both backings to disk and wait for them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_universal_flush(handle: subetha_handle) -> i32 {
    with_set(handle, |u| match u.set.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}
