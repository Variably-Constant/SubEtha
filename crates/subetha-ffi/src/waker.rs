//! Parking and waking across processes, through the C ABI.
//!
//! A consumer that has run out of work parks at the sequence number it is
//! waiting for; a producer that reaches that sequence wakes it. The park
//! goes down to the platform's own wait - `futex` on Linux, `_umtx_op` on
//! FreeBSD, and on Windows the hardware monitor followed by a named event
//! across processes or `WaitOnAddress` within one - so a parked thread
//! costs nothing until it is woken.
//!
//! # Parking is three calls
//!
//! `park` reserves a slot and returns a token. `wait` blocks on it, and
//! gives the slot back before it returns, so a caller that waits does not
//! release. `release` is for a caller that parks and then decides not to
//! wait after all.
//!
//! The sequence number is the caller's own: a ring's write position, a
//! job counter, anything a producer can compare against. A parker names
//! the value it wants to see, and `wake_up_to` wakes every parker whose
//! value that number has reached.
//!
//! # A full table is not an error to report upward
//!
//! `park` answers `SUBETHA_E_RING_WAKER_FULL` when every slot is taken. The
//! caller spins on whatever it was waiting for instead, which is what it
//! would do with no waker at all.
//!
//! The waker runs no background work, so strict and managed modes are the
//! same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::cross_process_waker::{CrossProcessWaker, WakerError, WakerToken};

use crate::error::{fail, waker_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_WAKER};
use crate::holds::{slot_of, HoldTable, ReleaseError};
use crate::ring::{deadline_from, finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The kind a park is held as. One kind, so a token names a park and
/// nothing else.
const PARK: u32 = 1;

/// A snapshot of a waker.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_waker_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the waker holds.
    pub capacity: u64,
    /// Parks this handle has taken and not given back.
    pub parked: u64,
}

pub(crate) struct WakerObject {
    waker: CrossProcessWaker,
    mode: u32,
    /// A park is taken and given back in a loop, so it is a token rather
    /// than a handle. The table is bounded by the waker's own capacity.
    parks: HoldTable,
    /// The waker slot each park names, beside the table because a token
    /// carries a slot and a generation and nothing else.
    park_slot: Box<[AtomicU32]>,
}

impl WakerObject {
    /// Every park this handle holds goes back, so a destroy leaves no
    /// slot reserved by a caller that can no longer release it.
    pub(crate) fn interrupt(&self) {
        for (index, _) in self.parks.drain() {
            let slot = self.park_slot[index].load(Ordering::Acquire);
            self.waker.release(WakerToken::from_slot(slot));
        }
    }

    fn stats(&self) -> subetha_waker_stats {
        subetha_waker_stats {
            mode: self.mode,
            capacity: self.waker.capacity() as u64,
            parked: self.parks.live() as u64,
        }
    }
}

impl Drop for WakerObject {
    fn drop(&mut self) {
        self.interrupt();
    }
}

fn with_waker(handle: subetha_handle, f: impl FnOnce(&WakerObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_WAKER, |object| match object {
        Object::Waker(w) => f(w),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a waker"),
    })
}

/// The refusal a token that names no live park earns.
fn refuse(e: ReleaseError) -> i32 {
    match e {
        ReleaseError::NotAToken => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "that is a handle, not a park token")
        }
        ReleaseError::NotHeld => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the token names no park on this waker: it ended already, or never began",
        ),
        ReleaseError::Stale => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the token is from a park that ended, and its slot has been taken since",
        ),
        ReleaseError::NoSuchSlot => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "the token names no slot on this waker")
        }
    }
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

fn place(waker: Result<CrossProcessWaker, WakerError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match waker {
        Ok(waker) => {
            let capacity = waker.capacity();
            let object = WakerObject {
                waker,
                mode,
                parks: HoldTable::bounded(capacity),
                park_slot: (0..capacity.max(1)).map(|_| AtomicU32::new(0)).collect(),
            };
            unsafe { issue(Object::Waker(object), out) }
        }
        Err(e) => waker_code(e),
    }
}

/// Obtain the waker at `path` holding `capacity` parks: an empty one is
/// initialized when the file does not exist, an existing one is attached
/// with its parked slots in place. A file built with another capacity is
/// a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_create(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(CrossProcessWaker::create(path, capacity), mode, out)
    })
}

/// Attach to the waker another process created at `path`; the file must
/// exist.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_open(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(CrossProcessWaker::open(path, capacity), mode, out)
    })
}

/// Truncate the file at `path` and initialize an empty waker there,
/// discarding whatever parks a live peer holds.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_reset(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(CrossProcessWaker::reset(path, capacity), mode, out)
    })
}

/// Reserve a slot and park at `target_seq`, writing the token into `out`.
///
/// `SUBETHA_E_RING_WAKER_FULL` when every slot is taken; the caller spins
/// on whatever it was waiting for instead.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_park(handle: subetha_handle, target_seq: u64, out: *mut u64) -> i32 {
    with_waker(handle, |w| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let token = match w.parks.claim(PARK) {
            Some(t) => t,
            None => return waker_code(WakerError::Full),
        };
        match w.waker.try_park(target_seq) {
            Ok(park) => {
                w.park_slot[slot_of(token)].store(park.slot_index(), Ordering::Release);
                // SAFETY: checked non-null; the caller guarantees it is
                // writable.
                unsafe { *out = token };
                SUBETHA_OK
            }
            Err(e) => {
                w.parks
                    .release(token)
                    .expect("a token claimed two lines above releases");
                waker_code(e)
            }
        }
    })
}

/// Block until a producer wakes `token` or `timeout_ms` elapses, and give
/// the park back either way, so a caller that waits does not release.
///
/// `SUBETHA_E_TIMEOUT` when the deadline passes first. A negative
/// `timeout_ms` waits without one.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_waker_wait(handle: subetha_handle, token: u64, timeout_ms: i64) -> i32 {
    with_waker(handle, |w| {
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        let slot = match w.parks.kind(token) {
            Ok(_) => w.park_slot[slot_of(token)].load(Ordering::Acquire),
            Err(e) => return refuse(e),
        };
        let timeout = deadline.map(|d| d.saturating_duration_since(std::time::Instant::now()));
        let outcome = w.waker.wait(WakerToken::from_slot(slot), timeout);
        // The waker released its own slot, so the table entry goes back
        // whatever the wait answered.
        if let Err(e) = w.parks.release(token) {
            return refuse(e);
        }
        match outcome {
            Ok(()) => SUBETHA_OK,
            Err(e) => waker_code(e),
        }
    })
}

/// Give a park back without waiting on it.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_waker_release(handle: subetha_handle, token: u64) -> i32 {
    with_waker(handle, |w| {
        let slot = match w.parks.kind(token) {
            Ok(_) => w.park_slot[slot_of(token)].load(Ordering::Acquire),
            Err(e) => return refuse(e),
        };
        w.waker.release(WakerToken::from_slot(slot));
        match w.parks.release(token) {
            Ok(_) => SUBETHA_OK,
            Err(e) => refuse(e),
        }
    })
}

/// Wake every parker whose target sequence `seq` has reached, and write
/// how many were woken into `out`.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_wake_up_to(handle: subetha_handle, seq: u64, out: *mut u64) -> i32 {
    with_waker(handle, |w| {
        let woken = w.waker.wake_up_to(seq) as u64;
        if !out.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is
            // writable.
            unsafe { *out = woken };
        }
        SUBETHA_OK
    })
}

/// Wake at most one parker whose target sequence `seq` has reached, for a
/// producer handing one item to one consumer.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_wake_one_up_to(handle: subetha_handle, seq: u64, out: *mut u64) -> i32 {
    with_waker(handle, |w| {
        let woken = w.waker.wake_one_up_to(seq) as u64;
        if !out.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is
            // writable.
            unsafe { *out = woken };
        }
        SUBETHA_OK
    })
}

/// Wake every parker whatever it is waiting for, for a shutdown that must
/// not leave anyone parked.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_wake_all(handle: subetha_handle, out: *mut u64) -> i32 {
    with_waker(handle, |w| {
        let woken = w.waker.wake_all() as u64;
        if !out.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is
            // writable.
            unsafe { *out = woken };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the waker into `out`. The parked count races every park
/// and wake, so it describes a moment that has already passed.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_read_stats(handle: subetha_handle, out: *mut subetha_waker_stats) -> i32 {
    with_waker(handle, |w| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = w.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove the waker's file. On Windows the file must not be mapped by any
/// handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_waker_unlink(path: *const c_char, out: *mut subetha_unlink_report) -> i32 {
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
                "subetha-ffi-waker-{name}-{}.bin",
                std::process::id()
            )))
        }

        fn object(&self, capacity: usize) -> WakerObject {
            let waker = CrossProcessWaker::create(&self.0, capacity).expect("a waker");
            WakerObject {
                waker,
                mode: SUBETHA_MODE_STRICT,
                parks: HoldTable::bounded(capacity),
                park_slot: (0..capacity).map(|_| AtomicU32::new(0)).collect(),
            }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the waker's file was removed: {:?}", found.first_failure);
        }
    }

    /// A park is a token from the shared table, so a second release of
    /// one is refused and cannot free a slot its next holder relies on.
    #[test]
    fn a_park_token_is_good_once() {
        let scratch = Scratch::new("once");
        let w = scratch.object(4);
        let token = w.parks.claim(PARK).expect("a free slot");
        assert_eq!(w.parks.live(), 1);
        assert_eq!(w.parks.release(token).expect("released"), PARK);
        assert_eq!(w.parks.release(token).unwrap_err(), ReleaseError::NotHeld);
        assert_eq!(w.parks.live(), 0);
    }

    /// The table takes the waker's own capacity, so it refuses a park
    /// exactly where the waker would.
    #[test]
    fn the_park_table_holds_what_the_waker_holds() {
        let scratch = Scratch::new("capacity");
        let w = scratch.object(2);
        let a = w.parks.claim(PARK).expect("first");
        let b = w.parks.claim(PARK).expect("second");
        assert!(w.parks.claim(PARK).is_none(), "no third park");
        assert_eq!(w.stats().capacity, 2);
        assert_eq!(w.stats().parked, 2);
        w.parks.release(a).expect("first back");
        w.parks.release(b).expect("second back");
        assert_eq!(w.stats().parked, 0);
    }

    /// A wake with nobody parked wakes nobody.
    #[test]
    fn waking_an_empty_waker_wakes_nobody() {
        let scratch = Scratch::new("empty");
        let w = scratch.object(4);
        assert_eq!(w.waker.wake_up_to(u64::MAX), 0);
        assert_eq!(w.waker.wake_all(), 0);
    }

    /// A park taken and then woken is released by the wait, and the
    /// handle's own table agrees the slot is free again.
    #[test]
    fn a_woken_park_is_released_by_its_wait() {
        let scratch = Scratch::new("woken");
        let w = scratch.object(4);
        let token = w.parks.claim(PARK).expect("a free slot");
        let park = w.waker.try_park(7).expect("a waker slot");
        w.park_slot[slot_of(token)].store(park.slot_index(), Ordering::Release);

        assert_eq!(w.waker.wake_up_to(7), 1, "the target was reached");
        w.waker
            .wait(WakerToken::from_slot(park.slot_index()), Some(std::time::Duration::from_secs(5)))
            .expect("an already-woken park returns at once");
        w.parks.release(token).expect("the table entry goes back");
        assert_eq!(w.stats().parked, 0);
    }
}
