//! The frame region through the C ABI: fixed-size blocks in a file that
//! many processes allocate from and free to, for payloads too large to
//! travel inside a ring slot.
//!
//! A producer takes a block, writes its bytes and sends the block's index
//! through whatever ring it is using; a consumer reads the block and frees
//! it. Reclaim order does not matter, so any consumer may free any block
//! and the next allocation takes it. This is the region an adaptive ring
//! builds for itself when a frame outgrows a slot; a caller reaching it
//! directly is building the same shape by hand.
//!
//! The region runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::frame_region::{frame_region_file_size, FrameRegion, MIN_BLOCK_SIZE};
use subetha_cxc::shared_ring::RingError;

use crate::error::{
    fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_RING_FULL,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_FRAME_REGION};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The smallest block a region carries: one must hold the free-list link
/// that threads it onto the free list.
pub const SUBETHA_FRAME_BLOCK_MIN: u64 = 8;
/// The index that names no block.
pub const SUBETHA_FRAME_NO_BLOCK: u32 = 0xFFFF_FFFF;

const _: () = assert!(SUBETHA_FRAME_BLOCK_MIN as usize == MIN_BLOCK_SIZE);

/// A snapshot of a frame region.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_frame_region_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Bytes one block holds.
    pub block_size: u64,
    /// Blocks the region carries.
    pub block_count: u64,
    /// Bytes the whole region takes on disk.
    pub file_size: u64,
}

pub(crate) struct FrameRegionObject {
    region: FrameRegion,
    mode: u32,
}

impl FrameRegionObject {
    /// A region parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_frame_region_stats {
        subetha_frame_region_stats {
            mode: self.mode,
            block_size: self.region.block_size() as u64,
            block_count: self.region.block_count() as u64,
            file_size: frame_region_file_size(self.region.block_size(), self.region.block_count()) as u64,
        }
    }
}

fn with_frame_region(handle: subetha_handle, f: impl FnOnce(&FrameRegionObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_FRAME_REGION, |object| match object {
        Object::FrameRegion(r) => f(r),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a frame region"),
    })
}

/// A geometry the region can carry: a block that holds the free-list link
/// and is a whole number of eight-byte words, and at least one of them.
fn checked_geometry(block_size: u32, block_count: u32) -> Result<(usize, usize), i32> {
    if (block_size as u64) < SUBETHA_FRAME_BLOCK_MIN || !block_size.is_multiple_of(8) {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("block_size {block_size} is not a multiple of eight of at least {SUBETHA_FRAME_BLOCK_MIN}"),
        ));
    }
    if block_count == 0 || block_count == SUBETHA_FRAME_NO_BLOCK {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("block_count {block_count} is zero or the index that names no block"),
        ));
    }
    Ok((block_size as usize, block_count as usize))
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    block_size: u32,
    block_count: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let (block_size, block_count) = checked_geometry(block_size, block_count)?;
    let mode = resolve_mode(mode)?;
    Ok((path, block_size, block_count, mode))
}

fn place(region: Result<FrameRegion, RingError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match region {
        Ok(region) => unsafe { issue(Object::FrameRegion(FrameRegionObject { region, mode }), out) },
        Err(e) => ring_code(e),
    }
}

/// Obtain the region at `path` with `block_count` blocks of `block_size`
/// bytes: an empty one is initialized when the file does not exist, an
/// existing one is attached with its allocated blocks and free list in
/// place, so a late joiner never wipes what a producer already filled. A
/// file built with another geometry is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_create(
    path: *const c_char,
    block_size: u32,
    block_count: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, block_size, block_count, mode) = match unsafe { read_arguments(path, block_size, block_count, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(FrameRegion::create(path, block_size, block_count), mode, out)
    })
}

/// Attach to the region another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_open(
    path: *const c_char,
    block_size: u32,
    block_count: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, block_size, block_count, mode) = match unsafe { read_arguments(path, block_size, block_count, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(FrameRegion::open(path, block_size, block_count), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty region there,
/// invalidating every block index other handles hold. On Windows the file
/// must not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_reset(
    path: *const c_char,
    block_size: u32,
    block_count: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, block_size, block_count, mode) = match unsafe { read_arguments(path, block_size, block_count, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(FrameRegion::reset(path, block_size, block_count), mode, out)
    })
}

/// Take a block and write its index into `out_block`: the free list first,
/// then the never-yet-allocated ones. `SUBETHA_E_RING_FULL` when every
/// block is out.
///
/// # Safety
/// `out_block` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_alloc(handle: subetha_handle, out_block: *mut u32) -> i32 {
    with_frame_region(handle, |r| {
        if out_block.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_block is null");
        }
        match r.region.alloc() {
            Some(block) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_block = block };
                SUBETHA_OK
            }
            None => SUBETHA_E_RING_FULL,
        }
    })
}

/// Return block `block` to the free list, for the next allocation to take.
/// Any process may free any block. The free list is threaded through the
/// blocks themselves, so this overwrites the first four bytes of the one it
/// takes: a block's payload does not survive being freed, and a caller that
/// wants the bytes reads them first. `SUBETHA_E_OUT_OF_BOUNDS` for an index
/// past the region.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_frame_region_free(handle: subetha_handle, block: u32) -> i32 {
    with_frame_region(handle, |r| {
        if block as usize >= r.region.block_count() {
            return fail(
                SUBETHA_E_OUT_OF_BOUNDS,
                format!("block {block} is past the {} the region carries", r.region.block_count()),
            );
        }
        r.region.free(block);
        SUBETHA_OK
    })
}

/// Copy `len` bytes from `data` into block `block`, at most one block's
/// worth. The bytes past what is written keep whatever the block held, so
/// a reader takes the length from wherever it took the block's index.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_write(handle: subetha_handle, block: u32, data: *const u8, len: usize) -> i32 {
    with_frame_region(handle, |r| {
        if block as usize >= r.region.block_count() {
            return fail(
                SUBETHA_E_OUT_OF_BOUNDS,
                format!("block {block} is past the {} the region carries", r.region.block_count()),
            );
        }
        if len > r.region.block_size() {
            return fail(
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
                format!("{len} bytes exceed the {}-byte block", r.region.block_size()),
            );
        }
        let data = match unsafe { bytes(data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        r.region.write_block(block, data);
        SUBETHA_OK
    })
}

/// Copy `len` bytes out of block `block` into `out`, at least `len` bytes,
/// and what was copied into `out_len`.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_read(
    handle: subetha_handle,
    block: u32,
    len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_frame_region(handle, |r| {
        if block as usize >= r.region.block_count() {
            return fail(
                SUBETHA_E_OUT_OF_BOUNDS,
                format!("block {block} is past the {} the region carries", r.region.block_count()),
            );
        }
        if len > r.region.block_size() {
            return fail(
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
                format!("{len} bytes exceed the {}-byte block", r.region.block_size()),
            );
        }
        let buf = match unsafe { out_buffer(out, cap, out_len, len) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let copied = r.region.read_block(block, len, buf);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = copied };
        SUBETHA_OK
    })
}

/// A snapshot of the region into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_read_stats(handle: subetha_handle, out: *mut subetha_frame_region_stats) -> i32 {
    with_frame_region(handle, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = r.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove the region file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_frame_region_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-frames-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the region file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_its_geometry_and_hands_blocks_round() {
        let scratch = Scratch::new("shape");
        let object = FrameRegionObject {
            region: FrameRegion::create(&scratch.0, 64, 4).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        let stats = object.stats();
        assert_eq!(stats.block_size, 64);
        assert_eq!(stats.block_count, 4);
        assert_eq!(stats.file_size, frame_region_file_size(64, 4) as u64);
        let first = object.region.alloc().expect("a block");
        object.region.write_block(first, b"a frame");
        let mut out = [0u8; 7];
        assert_eq!(object.region.read_block(first, 7, &mut out), 7);
        assert_eq!(&out, b"a frame");
        object.region.free(first);
        assert_eq!(object.region.alloc(), Some(first), "the freed block comes back");
        drop(object);
        assert_eq!(checked_geometry(4, 4).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_geometry(12, 4).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_geometry(64, 0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_geometry(64, SUBETHA_FRAME_NO_BLOCK).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_geometry(64, 4).unwrap(), (64, 4));
        assert_eq!(checked_geometry(8, 1).unwrap(), (8, 1));
    }
}
