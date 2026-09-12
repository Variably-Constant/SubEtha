//! The shared reservoir sampler through the C ABI: a fixed number of
//! slots in a file holding a uniform sample of an unbounded stream.
//!
//! Every item recorded has the same chance of being in the reservoir,
//! whatever the stream's length and without holding it. Items are records
//! of exactly `SUBETHA_RESERVOIR_ITEM_BYTES` bytes, which is the slot the
//! primitive lays out; a caller with shorter items encodes its own length
//! inside the record. The sampler runs no background work, so strict and
//! managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_reservoir_sampler::{
    ReservoirError, SharedReservoirSampler, RESERVOIR_SLOT_PAYLOAD,
};

use crate::error::{
    fail, SUBETHA_E_BUFFER_TOO_SMALL, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_RESERVOIR};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The bytes one sampled record takes.
pub const SUBETHA_RESERVOIR_ITEM_BYTES: usize = 56;

const _: () = assert!(SUBETHA_RESERVOIR_ITEM_BYTES == RESERVOIR_SLOT_PAYLOAD);

/// The record the sampler stores, which is the slot's payload.
type Item = [u8; SUBETHA_RESERVOIR_ITEM_BYTES];

/// A snapshot of a shared reservoir sampler.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_reservoir_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the reservoir holds.
    pub capacity: u64,
    /// Items offered to the sampler, which is the stream's length and not
    /// the sample's size.
    pub total_seen: u64,
    /// Slots filled, which is the capacity once the stream passes it.
    pub filled: u64,
}

pub(crate) struct ReservoirObject {
    sampler: SharedReservoirSampler<Item>,
    mode: u32,
}

impl ReservoirObject {
    /// A sampler parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: ReservoirError) -> i32 {
    match e {
        ReservoirError::PayloadTooLarge => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the record does not fit the sampler's slot",
        ),
        ReservoirError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the sampler on disk was built with another capacity",
        ),
        ReservoirError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_reservoir(handle: subetha_handle, f: impl FnOnce(&ReservoirObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_RESERVOIR, |object| match object {
        Object::Reservoir(r) => f(r),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a reservoir sampler"),
    })
}

/// The arguments both constructors read, in order, so the first refusal
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
    let mode = resolve_mode(mode)?;
    Ok((path, capacity as usize, mode))
}

/// Obtain the sampler at `path` holding `capacity` records: an empty one
/// is initialized when the file does not exist, an existing one is
/// attached with its sample in place. A sampler built with another
/// capacity is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_reservoir_create(
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
        match SharedReservoirSampler::<Item>::create(path, capacity) {
            Ok(sampler) => unsafe {
                issue(Object::Reservoir(ReservoirObject { sampler, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the sampler another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_reservoir_open(
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
        match SharedReservoirSampler::<Item>::open(path, capacity) {
            Ok(sampler) => unsafe {
                issue(Object::Reservoir(ReservoirObject { sampler, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Offer the record at `item`, exactly `SUBETHA_RESERVOIR_ITEM_BYTES`
/// bytes, to the sampler. Whether it was kept, and in which slot, goes
/// into `out_slot` when that is not null: a slot index when the record
/// entered the reservoir, and `UINT64_MAX` when the sampler declined it.
///
/// # Safety
/// `item` points to `len` readable bytes; `out_slot` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_reservoir_record(
    handle: subetha_handle,
    item: *const u8,
    len: usize,
    out_slot: *mut u64,
) -> i32 {
    with_reservoir(handle, |r| {
        if len != SUBETHA_RESERVOIR_ITEM_BYTES {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a record is {SUBETHA_RESERVOIR_ITEM_BYTES} bytes, not {len}"),
            );
        }
        let item = match unsafe { bytes(item, len) } {
            Ok(i) => i,
            Err(code) => return code,
        };
        let mut record: Item = [0u8; SUBETHA_RESERVOIR_ITEM_BYTES];
        record.copy_from_slice(item);
        let kept = r.sampler.record(record);
        if !out_slot.is_null() {
            // Checked non-null; the caller guarantees it is writable.
            unsafe { *out_slot = kept.map_or(u64::MAX, |slot| slot as u64) };
        }
        SUBETHA_OK
    })
}

/// The sample as it stands into `out`, which holds `cap` bytes, and the
/// records written into `out_len`. `SUBETHA_E_BUFFER_TOO_SMALL` when `cap`
/// is below what the sample needs, with the byte count in `out_len`.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_reservoir_snapshot(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_reservoir(handle, |r| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        let sample = r.sampler.snapshot();
        let needed = sample.len() * SUBETHA_RESERVOIR_ITEM_BYTES;
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = needed };
        if out.is_null() || cap < needed {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap >= needed
        // here. The records are contiguous and each is its own array.
        for (i, record) in sample.iter().enumerate() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    record.as_ptr(),
                    out.add(i * SUBETHA_RESERVOIR_ITEM_BYTES),
                    SUBETHA_RESERVOIR_ITEM_BYTES,
                );
            }
        }
        SUBETHA_OK
    })
}

/// Forget the stream: the sampler counts from zero again and its next
/// records fill the reservoir. Peers sharing the file see the same reset.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_reservoir_reset(handle: subetha_handle) -> i32 {
    with_reservoir(handle, |r| {
        r.sampler.reset();
        SUBETHA_OK
    })
}

/// Push the sampler's dirty pages to disk, returning when they are
/// durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_reservoir_flush(handle: subetha_handle) -> i32 {
    with_reservoir(handle, |r| match r.sampler.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the sampler's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_reservoir_flush_async(handle: subetha_handle) -> i32 {
    with_reservoir(handle, |r| match r.sampler.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the sampler into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_reservoir_read_stats(
    handle: subetha_handle,
    out: *mut subetha_reservoir_stats,
) -> i32 {
    with_reservoir(handle, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let capacity = r.sampler.capacity();
        let total_seen = r.sampler.total_seen();
        let stats = subetha_reservoir_stats {
            mode: r.mode,
            capacity: capacity as u64,
            total_seen,
            filled: total_seen.min(capacity as u64),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
