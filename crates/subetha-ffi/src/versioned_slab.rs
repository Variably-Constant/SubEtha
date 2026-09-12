//! The shared versioned slab through the C ABI: indexed slots each
//! keeping a short history, so a reader pinned at an epoch sees what was
//! current then while a writer moves on.
//!
//! The value size and the chain depth are given at create time rather than
//! fixed by the ABI, so a caller sizes the slab to what it stores. Three
//! classes are named for callers with no reason to choose:
//! `SUBETHA_VERSIONED_VALUE_BYTES_SMALL`, `_DEFAULT` and `_LARGE`, with
//! matching depths, and `subetha_versioned_slab_create_default` takes the
//! middle one.
//!
//! A push onto a full chain first drops every version no pin can reach.
//! When every version is still reachable it answers
//! `SUBETHA_E_WOULD_BLOCK` rather than dropping one a reader is inside.
//! The slab runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::raw_versioned_slab::{RawVersionedSlab, VersionedSlabLayout};
use subetha_cxc::shared_versioned_slab::{SharedVersionedSlab, VersionedSlabError};

use crate::error::{
    fail, SUBETHA_E_BUFFER_TOO_SMALL, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_KEY_ABSENT,
    SUBETHA_E_RING_IO, SUBETHA_E_WOULD_BLOCK, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_VERSIONED_SLAB};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A value size for keys, counters and small records.
pub const SUBETHA_VERSIONED_VALUE_BYTES_SMALL: usize = 16;
/// The value size `subetha_versioned_slab_create_default` uses.
pub const SUBETHA_VERSIONED_VALUE_BYTES_DEFAULT: usize = 64;
/// A value size for records that carry a payload of their own.
pub const SUBETHA_VERSIONED_VALUE_BYTES_LARGE: usize = 256;

/// The depth that pairs with the small value size.
pub const SUBETHA_VERSIONED_DEPTH_SMALL: usize = 2;
/// The depth `subetha_versioned_slab_create_default` uses.
pub const SUBETHA_VERSIONED_DEPTH_DEFAULT: usize = 4;
/// The depth that pairs with the large value size.
pub const SUBETHA_VERSIONED_DEPTH_LARGE: usize = 8;

/// A snapshot of a shared versioned slab.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_versioned_slab_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the slab addresses.
    pub capacity: u64,
    /// Bytes one version's value takes.
    pub value_size: u64,
    /// Versions a slot keeps before a push has to drop one.
    pub depth: u64,
    /// The epoch the table stands at now.
    pub epoch: u64,
}

/// What the entry points call, whichever form is carrying the slab.
///
/// A value size and depth that name one of the classes take a slab whose
/// sizes are compile-time constants, so its copies are fixed-width; any
/// other pair takes the runtime-sized form. The choice is a function of
/// the two arguments alone, so every process opening one file with the
/// same arguments lands on the same form, and the two never share a file.
trait Backing: Send + Sync {
    fn capacity(&self) -> usize;
    fn value_size(&self) -> usize;
    fn depth(&self) -> usize;
    fn epoch_now(&self) -> u64;
    fn set(&self, i: usize, value: &[u8]) -> Result<(), VersionedSlabError>;
    fn get(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError>;
    fn retire(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError>;
    fn sweep_slot(&self, i: usize) -> Result<usize, VersionedSlabError>;
    fn void_epoch(&self, epoch: u64) -> Result<usize, VersionedSlabError>;
    fn flush(&self) -> Result<(), VersionedSlabError>;
}

impl Backing for RawVersionedSlab {
    fn capacity(&self) -> usize {
        RawVersionedSlab::capacity(self)
    }
    fn value_size(&self) -> usize {
        self.layout().value_size
    }
    fn depth(&self) -> usize {
        self.layout().depth
    }
    fn epoch_now(&self) -> u64 {
        self.epochs().now()
    }
    fn set(&self, i: usize, value: &[u8]) -> Result<(), VersionedSlabError> {
        RawVersionedSlab::set(self, i, value)
    }
    fn get(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError> {
        RawVersionedSlab::get(self, i, out)
    }
    fn retire(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError> {
        RawVersionedSlab::retire(self, i, out)
    }
    fn sweep_slot(&self, i: usize) -> Result<usize, VersionedSlabError> {
        RawVersionedSlab::sweep_slot(self, i)
    }
    fn void_epoch(&self, epoch: u64) -> Result<usize, VersionedSlabError> {
        RawVersionedSlab::void_epoch(self, epoch)
    }
    fn flush(&self) -> Result<(), VersionedSlabError> {
        RawVersionedSlab::flush(self)
    }
}

impl<const N: usize, const D: usize> Backing for SharedVersionedSlab<[u8; N], D> {
    fn capacity(&self) -> usize {
        SharedVersionedSlab::capacity(self)
    }
    fn value_size(&self) -> usize {
        N
    }
    fn depth(&self) -> usize {
        D
    }
    fn epoch_now(&self) -> u64 {
        self.epochs().now()
    }
    fn set(&self, i: usize, value: &[u8]) -> Result<(), VersionedSlabError> {
        let mut record = [0u8; N];
        record.copy_from_slice(value);
        SharedVersionedSlab::set(self, i, record)
    }
    fn get(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError> {
        match SharedVersionedSlab::get(self, i)? {
            Some(value) => {
                out[..N].copy_from_slice(&value);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn retire(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError> {
        match SharedVersionedSlab::retire(self, i)? {
            Some(value) => {
                out[..N].copy_from_slice(&value);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn sweep_slot(&self, i: usize) -> Result<usize, VersionedSlabError> {
        SharedVersionedSlab::sweep_slot(self, i)
    }
    fn void_epoch(&self, epoch: u64) -> Result<usize, VersionedSlabError> {
        SharedVersionedSlab::void_epoch(self, epoch)
    }
    fn flush(&self) -> Result<(), VersionedSlabError> {
        SharedVersionedSlab::flush(self)
    }
}

pub(crate) struct VersionedSlabObject {
    slab: Box<dyn Backing>,
    mode: u32,
}

impl VersionedSlabObject {
    /// A slab parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: VersionedSlabError) -> i32 {
    match e {
        VersionedSlabError::Pinned => fail(
            SUBETHA_E_WOULD_BLOCK,
            "every version in the slot is still reachable by a pin, so none can be dropped",
        ),
        VersionedSlabError::Slab(e) => fail(SUBETHA_E_RING_IO, format!("slab: {e}")),
        VersionedSlabError::Epochs(e) => fail(SUBETHA_E_RING_IO, format!("epoch table: {e}")),
    }
}

fn with_slab(handle: subetha_handle, f: impl FnOnce(&VersionedSlabObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_VERSIONED_SLAB, |object| match object {
        Object::VersionedSlab(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a versioned slab"),
    })
}

/// The arguments both constructors read, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `slab_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[allow(clippy::too_many_arguments)]
unsafe fn read_arguments<'a>(
    slab_path: *const c_char,
    capacity: u64,
    value_size: usize,
    depth: usize,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, VersionedSlabLayout, &'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let slab_path = Path::new(unsafe { text(slab_path, "slab_path") }?);
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    if value_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "value_size is zero"));
    }
    if depth == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "depth is zero"));
    }
    let epochs_path = Path::new(unsafe { text(epochs_path, "epochs_path") }?);
    if max_pins == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_pins is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((
        slab_path,
        capacity as usize,
        VersionedSlabLayout { value_size, depth },
        epochs_path,
        max_pins as usize,
        mode,
    ))
}

/// Obtain the slab at `slab_path` with `capacity` slots of `value_size`
/// bytes keeping `depth` versions each, and its epoch table at
/// `epochs_path` holding `max_pins` pins. `epochs_path` may be the table
/// the store's other structures share.
///
/// # Safety
/// `slab_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_versioned_slab_create(
    slab_path: *const c_char,
    capacity: u64,
    value_size: usize,
    depth: usize,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (slab_path, capacity, layout, epochs_path, max_pins, mode) = match unsafe {
            read_arguments(slab_path, capacity, value_size, depth, epochs_path, max_pins, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let made = build(false, slab_path, capacity, layout, epochs_path, max_pins);
        match made {
            Ok(slab) => unsafe {
                issue(Object::VersionedSlab(VersionedSlabObject { slab, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// The slab one set of arguments names, compiled at fixed sizes when they
/// match a class and runtime-sized otherwise. `attach` opens rather than
/// creates.
fn build(
    attach: bool,
    slab_path: &Path,
    capacity: usize,
    layout: VersionedSlabLayout,
    epochs_path: &Path,
    max_pins: usize,
) -> Result<Box<dyn Backing>, VersionedSlabError> {
    macro_rules! fixed {
        ($n:expr, $d:expr) => {{
            let slab = if attach {
                SharedVersionedSlab::<[u8; $n], $d>::open(
                    slab_path, capacity, epochs_path, max_pins,
                )?
            } else {
                SharedVersionedSlab::<[u8; $n], $d>::create(
                    slab_path, capacity, epochs_path, max_pins,
                )?
            };
            Ok(Box::new(slab) as Box<dyn Backing>)
        }};
    }
    match (layout.value_size, layout.depth) {
        (SUBETHA_VERSIONED_VALUE_BYTES_SMALL, SUBETHA_VERSIONED_DEPTH_SMALL) => {
            fixed!(16, 2)
        }
        (SUBETHA_VERSIONED_VALUE_BYTES_DEFAULT, SUBETHA_VERSIONED_DEPTH_DEFAULT) => {
            fixed!(64, 4)
        }
        (SUBETHA_VERSIONED_VALUE_BYTES_LARGE, SUBETHA_VERSIONED_DEPTH_LARGE) => {
            fixed!(256, 8)
        }
        _ => {
            let slab = if attach {
                RawVersionedSlab::open(slab_path, capacity, layout, epochs_path, max_pins)?
            } else {
                RawVersionedSlab::create(slab_path, capacity, layout, epochs_path, max_pins)?
            };
            Ok(Box::new(slab) as Box<dyn Backing>)
        }
    }
}

/// `subetha_versioned_slab_create` at the default class:
/// `SUBETHA_VERSIONED_VALUE_BYTES_DEFAULT` bytes and
/// `SUBETHA_VERSIONED_DEPTH_DEFAULT` versions.
///
/// # Safety
/// `slab_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_create_default(
    slab_path: *const c_char,
    capacity: u64,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    unsafe {
        subetha_versioned_slab_create(
            slab_path,
            capacity,
            SUBETHA_VERSIONED_VALUE_BYTES_DEFAULT,
            SUBETHA_VERSIONED_DEPTH_DEFAULT,
            epochs_path,
            max_pins,
            mode,
            out,
        )
    }
}

/// Attach to the slab and epoch table another process created; both files
/// must exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `slab_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_versioned_slab_open(
    slab_path: *const c_char,
    capacity: u64,
    value_size: usize,
    depth: usize,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (slab_path, capacity, layout, epochs_path, max_pins, mode) = match unsafe {
            read_arguments(slab_path, capacity, value_size, depth, epochs_path, max_pins, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match build(true, slab_path, capacity, layout, epochs_path, max_pins) {
            Ok(slab) => unsafe {
                issue(Object::VersionedSlab(VersionedSlabObject { slab, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Make the `len` bytes at `value` the version current at slot `i`, from a
/// fresh epoch. `len` must be the slab's value size.
/// `SUBETHA_E_WOULD_BLOCK` says the chain is full and every version in it
/// is still reachable by a pin.
///
/// # Safety
/// `value` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_set(
    handle: subetha_handle,
    i: u64,
    value: *const u8,
    len: usize,
) -> i32 {
    with_slab(handle, |s| {
        if len != s.slab.value_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a value is {} bytes, not {len}", s.slab.value_size()),
            );
        }
        let value = match unsafe { bytes(value, len) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match s.slab.set(i as usize, value) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// The current value at slot `i` into `out`, and its length into
/// `out_len`. `SUBETHA_E_MAP_KEY_ABSENT` when the slot has no live version.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_get(
    handle: subetha_handle,
    i: u64,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_slab(handle, |s| {
        let value_size = s.slab.value_size();
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = value_size };
        if out.is_null() || cap < value_size {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, value_size) };
        match s.slab.get(i as usize, buf) {
            Ok(true) => SUBETHA_OK,
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// Supersede the current version at slot `i` at a fresh epoch, writing
/// what was current into `out` the way `subetha_versioned_slab_get` does.
/// `SUBETHA_E_MAP_KEY_ABSENT` when the slot had no live version.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_retire(
    handle: subetha_handle,
    i: u64,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_slab(handle, |s| {
        let value_size = s.slab.value_size();
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = value_size };
        if out.is_null() || cap < value_size {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, value_size) };
        match s.slab.retire(i as usize, buf) {
            Ok(true) => SUBETHA_OK,
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// Drop every version at slot `i` that no pin can reach, reporting how
/// many went through `out_dropped` when that is not null. A push does this
/// itself on a full chain; this is for a caller reclaiming on its own
/// schedule.
///
/// # Safety
/// `out_dropped` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_sweep_slot(
    handle: subetha_handle,
    i: u64,
    out_dropped: *mut u64,
) -> i32 {
    with_slab(handle, |s| match s.slab.sweep_slot(i as usize) {
        Ok(dropped) => {
            if !out_dropped.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_dropped = dropped as u64 };
            }
            SUBETHA_OK
        }
        Err(e) => code_for(e),
    })
}

/// Undo every stamp this slab holds at `epoch`: a version born there goes,
/// and a version superseded there is current again. For an epoch whose
/// holder died part way through a compound write. Reports the versions
/// touched through `out_touched` when that is not null.
///
/// # Safety
/// `out_touched` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_void_epoch(
    handle: subetha_handle,
    epoch: u64,
    out_touched: *mut u64,
) -> i32 {
    with_slab(handle, |s| match s.slab.void_epoch(epoch) {
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

/// Push the slab's dirty pages to disk, returning when they are durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_versioned_slab_flush(handle: subetha_handle) -> i32 {
    with_slab(handle, |s| match s.slab.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the slab into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_slab_read_stats(
    handle: subetha_handle,
    out: *mut subetha_versioned_slab_stats,
) -> i32 {
    with_slab(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_versioned_slab_stats {
            mode: s.mode,
            capacity: s.slab.capacity() as u64,
            value_size: s.slab.value_size() as u64,
            depth: s.slab.depth() as u64,
            epoch: s.slab.epoch_now(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
