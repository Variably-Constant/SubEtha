//! The shared fence clock through the C ABI: a hybrid logical clock per
//! participant in one file, and the global fence beneath which every
//! participant has been heard from.
//!
//! Each participant registers for a slot and ticks its own clock as it
//! does work. A value carried between processes is merged into the
//! receiver's clock, which is what makes the order total across them: a
//! merge takes the later of the two and steps the logical part, so an
//! event that caused another always compares below it. The global fence
//! is the latest clock across live slots: every event any participant has
//! recorded stands at or below it, so it is the point by which all of
//! their work is accounted for.
//!
//! A slot is named by a plain index rather than a handle, so registering
//! and unregistering cost nothing beyond the call: nothing is allocated
//! per participant and nothing has to be destroyed. A process that dies
//! holding a slot leaves it registered, and the caller reclaims it by
//! reading `pid` out of `subetha_fence_clock_slot` and unregistering the
//! slots whose process is gone.
//!
//! The clock runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_fence_clock::{FenceClockError, Hlc, SharedFenceClock};

use crate::error::{fail, fence_clock_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_FENCE_CLOCK};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A hybrid logical clock: a microsecond reading and a counter that
/// breaks ties within it. Two of them compare lexicographically, the
/// physical part first, which is the order every participant agrees on.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_hlc {
    /// Microseconds, from the clock the participants share.
    pub physical_us: u64,
    /// Ticks within one microsecond, so two events in the same
    /// microsecond still order.
    pub logical: u64,
}

impl From<Hlc> for subetha_hlc {
    fn from(h: Hlc) -> Self {
        Self { physical_us: h.physical_us, logical: h.logical }
    }
}

impl From<subetha_hlc> for Hlc {
    fn from(h: subetha_hlc) -> Self {
        Self { physical_us: h.physical_us, logical: h.logical }
    }
}

/// What one participant's slot holds.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_fence_clock_slot {
    /// The process holding the slot; a caller reclaims a slot whose
    /// process is gone by unregistering it.
    pub pid: u32,
    /// That participant's clock as it last published it.
    pub hlc: subetha_hlc,
    /// When it last did, on the shared microsecond clock.
    pub last_updated_us: u64,
}

/// A snapshot of a fence clock.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_fence_clock_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the clock holds.
    pub capacity: u64,
    /// The shared microsecond reading right now.
    pub shared_clock_us: u64,
    /// The global fence as last published.
    pub fence: subetha_hlc,
    /// How many times a fence has been published.
    pub fence_epoch: u64,
}

pub(crate) struct FenceClockObject {
    clock: SharedFenceClock,
    mode: u32,
}

impl FenceClockObject {
    /// A clock parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_fence_clock_stats {
        subetha_fence_clock_stats {
            mode: self.mode,
            capacity: self.clock.capacity() as u64,
            shared_clock_us: self.clock.shared_clock_us(),
            fence: self.clock.read_global_fence().into(),
            fence_epoch: self.clock.fence_epoch(),
        }
    }
}

fn with_clock(handle: subetha_handle, f: impl FnOnce(&FenceClockObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_FENCE_CLOCK, |object| match object {
        Object::FenceClock(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a fence clock"),
    })
}

/// The one shape most of these share: an `Hlc` written into a caller's
/// out-parameter.
///
/// # Safety
/// `out` is null or points to a writable `subetha_hlc`.
unsafe fn write_hlc(out: *mut subetha_hlc, value: Hlc) -> i32 {
    if out.is_null() {
        return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
    }
    // SAFETY: checked non-null; the caller guarantees it is writable.
    unsafe { *out = value.into() };
    SUBETHA_OK
}

/// A slot index the clock can carry.
fn slot_of(c: &FenceClockObject, slot: u32) -> Result<usize, i32> {
    if slot as usize >= c.clock.capacity() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("slot {slot} is past the {} the clock holds", c.clock.capacity()),
        ));
    }
    Ok(slot as usize)
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u32,
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

fn place(clock: Result<SharedFenceClock, FenceClockError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match clock {
        Ok(clock) => unsafe { issue(Object::FenceClock(FenceClockObject { clock, mode }), out) },
        Err(e) => fence_clock_code(e),
    }
}

/// Obtain the fence clock at `path` with room for `capacity`
/// participants: an empty one is initialized when the file does not
/// exist, an existing one is attached with its registered slots and its
/// published fence in place. A file built with another capacity is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_create(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedFenceClock::create(path, capacity), mode, out)
    })
}

/// Attach to the fence clock another process created at `path`; the file
/// must exist.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_open(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedFenceClock::open(path, capacity), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty clock there,
/// dropping every registration. On Windows the file must not be mapped by
/// any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_reset(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedFenceClock::reset(path, capacity), mode, out)
    })
}

/// Take a slot for `pid` and write its index into `out_slot`. The index
/// is a plain number, not a handle: nothing is allocated for it and
/// nothing has to be destroyed, so a participant that registers and
/// unregisters repeatedly costs only the calls.
/// `SUBETHA_E_FENCE_CLOCK_FULL` when every slot is taken.
///
/// # Safety
/// `out_slot` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_register(handle: subetha_handle, pid: u32, out_slot: *mut u32) -> i32 {
    with_clock(handle, |c| {
        if out_slot.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_slot is null");
        }
        match c.clock.register(pid) {
            Ok(slot) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_slot = slot as u32 };
                SUBETHA_OK
            }
            Err(e) => fence_clock_code(e),
        }
    })
}

/// Give slot `slot` back, so it stops holding the global fence down and
/// the next participant can take it. A caller reclaiming after a crash
/// reads `pid` from `subetha_fence_clock_slot` and unregisters the ones
/// whose process is gone.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_fence_clock_unregister(handle: subetha_handle, slot: u32) -> i32 {
    with_clock(handle, |c| match slot_of(c, slot) {
        Ok(slot) => {
            c.clock.unregister(slot);
            SUBETHA_OK
        }
        Err(code) => code,
    })
}

/// Step slot `slot`'s clock forward and write the new value into `out`.
/// Called as the participant does work, so what it publishes is at least
/// as late as anything it has done.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_tick(handle: subetha_handle, slot: u32, out: *mut subetha_hlc) -> i32 {
    with_clock(handle, |c| match slot_of(c, slot) {
        Ok(slot) => unsafe { write_hlc(out, c.clock.tick(slot)) },
        Err(code) => code,
    })
}

/// Take `remote` into slot `slot`'s clock and write the result into
/// `out`: the later of the two, stepped, so an event that caused the
/// remote one compares below it. This is what a receiver calls with the
/// value that traveled beside a message.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_merge(handle: subetha_handle, slot: u32, remote: subetha_hlc, out: *mut subetha_hlc) -> i32 {
    with_clock(handle, |c| match slot_of(c, slot) {
        Ok(slot) => unsafe { write_hlc(out, c.clock.merge(slot, remote.into())) },
        Err(code) => code,
    })
}

/// Slot `slot`'s clock as it stands, without stepping it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_get_local(handle: subetha_handle, slot: u32, out: *mut subetha_hlc) -> i32 {
    with_clock(handle, |c| match slot_of(c, slot) {
        Ok(slot) => unsafe { write_hlc(out, c.clock.get_local(slot)) },
        Err(code) => code,
    })
}

/// The latest clock across registered slots, computed now and written
/// into `out`: every event any participant has recorded stands at or
/// below it, so it is the point by which all of their work is accounted
/// for. This walks the slots without publishing what it finds.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_compute_global_fence(handle: subetha_handle, out: *mut subetha_hlc) -> i32 {
    with_clock(handle, |c| unsafe { write_hlc(out, c.clock.compute_global_fence()) })
}

/// Compute the fence and publish it, so every other process reads it
/// without walking the slots, and write it into `out`. One participant
/// doing this on a schedule is the usual arrangement.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_publish_global_fence(handle: subetha_handle, out: *mut subetha_hlc) -> i32 {
    with_clock(handle, |c| unsafe { write_hlc(out, c.clock.publish_global_fence()) })
}

/// The fence as last published, into `out`. Cheap: it reads one value
/// rather than walking the slots, which is what publishing is for.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_read_global_fence(handle: subetha_handle, out: *mut subetha_hlc) -> i32 {
    with_clock(handle, |c| unsafe { write_hlc(out, c.clock.read_global_fence()) })
}

/// What slot `slot` holds, into `out`, and whether it is registered at
/// all into `out_registered`. An unregistered slot leaves `out`
/// untouched.
///
/// # Safety
/// `out` and `out_registered` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_read_slot(
    handle: subetha_handle,
    slot: u32,
    out: *mut subetha_fence_clock_slot,
    out_registered: *mut bool,
) -> i32 {
    with_clock(handle, |c| {
        let slot = match slot_of(c, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        if out.is_null() || out_registered.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out or out_registered is null");
        }
        match c.clock.slot_snapshot(slot) {
            Some(s) => {
                let found = subetha_fence_clock_slot {
                    pid: s.pid,
                    hlc: s.hlc.into(),
                    last_updated_us: s.last_updated_us,
                };
                // SAFETY: checked non-null; the caller guarantees both are writable.
                unsafe {
                    *out = found;
                    *out_registered = true;
                }
                SUBETHA_OK
            }
            None => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_registered = false };
                SUBETHA_OK
            }
        }
    })
}

/// A snapshot of the clock into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_read_stats(handle: subetha_handle, out: *mut subetha_fence_clock_stats) -> i32 {
    with_clock(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = c.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the clock's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_fence_clock_flush(handle: subetha_handle) -> i32 {
    with_clock(handle, |c| match c.clock.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => fence_clock_code(e),
    })
}

/// Remove the clock's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_fence_clock_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-fence-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the clock's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn a_merge_orders_after_what_it_took_in_and_the_fence_follows_the_slowest() {
        let scratch = Scratch::new("shape");
        let clock = FenceClockObject {
            clock: SharedFenceClock::create(&scratch.0, 4).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        assert_eq!(clock.stats().capacity, 4);

        let a = clock.clock.register(11).unwrap();
        let b = clock.clock.register(22).unwrap();
        let sent = clock.clock.tick(a);
        let got = clock.clock.merge(b, sent);
        assert!(got > sent, "a merge orders after the value it took in");

        // The fence is the latest of the slots, so every participant's
        // work stands at or below it and none is left unaccounted for.
        let fence = clock.clock.compute_global_fence();
        assert!(fence >= clock.clock.get_local(a), "the fence is at or above every slot");
        assert!(fence >= clock.clock.get_local(b));
        assert_eq!(fence, clock.clock.get_local(b), "b merged a's tick, so b is the latest");

        // Publishing makes the same value readable without the walk.
        let published = clock.clock.publish_global_fence();
        assert_eq!(clock.clock.read_global_fence(), published);
        assert!(clock.stats().fence_epoch >= 1);

        // A slot answers with the pid that took it; giving it back frees
        // it for the next participant.
        let snapshot = clock.clock.slot_snapshot(a).expect("slot a is registered");
        assert_eq!(snapshot.pid, 11);
        clock.clock.unregister(a);
        assert!(clock.clock.slot_snapshot(a).is_none(), "an unregistered slot holds nothing");
        drop(clock);
    }
}
