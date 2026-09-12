//! The shared versioned chain through the C ABI: one value's history in a
//! file, newest first, that any number of processes read at a version they
//! name.
//!
//! A push carries the version it becomes current at, and that version must
//! be strictly above the head's, so the chain is ordered by construction
//! rather than by arrival. A reader hands in the version it is reading as
//! of and gets the value that was current then, which is what lets readers
//! and a writer share the chain without locking each other out.
//!
//! Values are records of exactly `SUBETHA_VERSIONED_CHAIN_ITEM_BYTES`
//! bytes, the node the primitive lays out. The chain runs no background
//! work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_versioned_chain::{ChainError, SharedVersionedChain, NODE_PAYLOAD_BYTES};

use crate::error::{
    fail, SUBETHA_E_BUFFER_TOO_SMALL, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_FULL,
    SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_VERSIONED_CHAIN};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The bytes one version's value takes.
pub const SUBETHA_VERSIONED_CHAIN_ITEM_BYTES: usize = 48;

const _: () = assert!(SUBETHA_VERSIONED_CHAIN_ITEM_BYTES == NODE_PAYLOAD_BYTES);

/// The record the chain stores, which is the node's payload.
type Item = [u8; SUBETHA_VERSIONED_CHAIN_ITEM_BYTES];

/// A snapshot of a shared versioned chain.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_versioned_chain_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Nodes the chain can hold at once.
    pub capacity: u64,
    /// Versions in the chain now.
    pub len: u64,
    /// The head's version, or zero when the chain is empty.
    pub current_version: u64,
}

pub(crate) struct VersionedChainObject {
    chain: SharedVersionedChain<Item>,
    mode: u32,
}

impl VersionedChainObject {
    /// A chain parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: ChainError) -> i32 {
    match e {
        ChainError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the chain on disk was built with another capacity",
        ),
        ChainError::PayloadTooLarge => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the value does not fit the chain's node",
        ),
        ChainError::Full => SUBETHA_E_RING_FULL,
        ChainError::NonMonotonicVersion => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "a pushed version must be strictly above the head's",
        ),
        ChainError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_chain(handle: subetha_handle, f: impl FnOnce(&VersionedChainObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_VERSIONED_CHAIN, |object| match object {
        Object::VersionedChain(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a versioned chain"),
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    // The primitive addresses nodes with a u32 and reserves the top two
    // values, so a capacity at or above that has no slot to name.
    if capacity >= (u32::MAX - 1) as u64 {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("capacity {capacity} is past what a node index addresses"),
        ));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, capacity as usize, mode))
}

/// Obtain the chain at `path` holding `capacity` versions: an empty one is
/// initialized when the file does not exist, an existing one is attached
/// with its history in place. A chain built with another capacity is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_create(
    path: *const c_char,
    capacity: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedVersionedChain::<Item>::create(path, capacity) {
            Ok(chain) => unsafe {
                issue(Object::VersionedChain(VersionedChainObject { chain, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Truncate the chain's file at `path` and initialize an empty one,
/// discarding every version live peers share.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_reset(
    path: *const c_char,
    capacity: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedVersionedChain::<Item>::reset(path, capacity) {
            Ok(chain) => unsafe {
                issue(Object::VersionedChain(VersionedChainObject { chain, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the chain another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_open(
    path: *const c_char,
    capacity: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedVersionedChain::<Item>::open(path, capacity) {
            Ok(chain) => unsafe {
                issue(Object::VersionedChain(VersionedChainObject { chain, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Make the `len` bytes at `value` the version current from `version`.
/// `version` must be strictly above the head's, and `SUBETHA_E_RING_FULL`
/// says every node is taken.
///
/// # Safety
/// `value` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_push(
    handle: subetha_handle,
    version: u64,
    value: *const u8,
    len: usize,
) -> i32 {
    with_chain(handle, |c| {
        if len != SUBETHA_VERSIONED_CHAIN_ITEM_BYTES {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a value is {SUBETHA_VERSIONED_CHAIN_ITEM_BYTES} bytes, not {len}"),
            );
        }
        let value = match unsafe { bytes(value, len) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        let mut record: Item = [0u8; SUBETHA_VERSIONED_CHAIN_ITEM_BYTES];
        record.copy_from_slice(value);
        match c.chain.push(version, record) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// The value current as of `snapshot_version` into `out`, and its length
/// into `out_len`. `SUBETHA_E_MAP_KEY_ABSENT` when no version at or below
/// that one is in the chain.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_read_at(
    handle: subetha_handle,
    snapshot_version: u64,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_chain(handle, |c| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = SUBETHA_VERSIONED_CHAIN_ITEM_BYTES };
        let Some(value) = c.chain.read_at(snapshot_version) else {
            return crate::error::SUBETHA_E_MAP_KEY_ABSENT;
        };
        if out.is_null() || cap < SUBETHA_VERSIONED_CHAIN_ITEM_BYTES {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        unsafe {
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                out,
                SUBETHA_VERSIONED_CHAIN_ITEM_BYTES,
            );
        }
        SUBETHA_OK
    })
}

/// The head version into `out_version` and its value into `out`, the way
/// `subetha_versioned_chain_read_at` reports one.
/// `SUBETHA_E_MAP_KEY_ABSENT` on an empty chain.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` and `out_version` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_current(
    handle: subetha_handle,
    out_version: *mut u64,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_chain(handle, |c| {
        if out_len.is_null() || out_version.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len or out_version is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = SUBETHA_VERSIONED_CHAIN_ITEM_BYTES };
        let Some((version, value)) = c.chain.current() else {
            return crate::error::SUBETHA_E_MAP_KEY_ABSENT;
        };
        if out.is_null() || cap < SUBETHA_VERSIONED_CHAIN_ITEM_BYTES {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        unsafe {
            *out_version = version;
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                out,
                SUBETHA_VERSIONED_CHAIN_ITEM_BYTES,
            );
        }
        SUBETHA_OK
    })
}

/// Empty the chain and rebuild its free list. Every version goes, and a
/// snapshot version a reader still holds stops naming anything.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_versioned_chain_clear(handle: subetha_handle) -> i32 {
    with_chain(handle, |c| {
        c.chain.clear();
        SUBETHA_OK
    })
}

/// Push the chain's dirty pages to disk, returning when they are durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_versioned_chain_flush(handle: subetha_handle) -> i32 {
    with_chain(handle, |c| match c.chain.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the chain's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_versioned_chain_flush_async(handle: subetha_handle) -> i32 {
    with_chain(handle, |c| match c.chain.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the chain into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_chain_read_stats(
    handle: subetha_handle,
    out: *mut subetha_versioned_chain_stats,
) -> i32 {
    with_chain(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_versioned_chain_stats {
            mode: c.mode,
            capacity: c.chain.capacity() as u64,
            len: c.chain.len() as u64,
            current_version: c.chain.current().map_or(0, |(version, _)| version),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
