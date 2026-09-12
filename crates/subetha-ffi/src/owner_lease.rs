//! The owner lease through the C ABI: one process at a time holds a
//! resource, and a holder that dies loses it rather than keeping it for
//! good.
//!
//! This is what `subetha_rwlock_*` is not. A lock's write bit is released
//! by its holder and by nothing else, so a process that exits holding it
//! shuts the lock until someone resets the file. A lease is held by a
//! process id, and a holder is displaced in two ways: a lower process id
//! preempts it outright, and a heartbeat that has fallen more than
//! `grace_epochs` behind the global epoch marks it gone.
//!
//! The grace window is counted in epochs rather than seconds, and nothing
//! advances the epoch on its own: something in the system calls
//! `subetha_owner_lease_tick_epoch` at whatever rate it wants a
//! stale holder detected at. A holder keeps its claim by calling
//! `subetha_owner_lease_beat` faster than that.
//!
//! The payload travels with the lease. It is at most
//! `SUBETHA_LEASE_PAYLOAD_MAX` bytes at a size the caller declares, which
//! is recorded in the region and checked at every attach, so a lease
//! created here at eight bytes and a Rust `OwnerLease<u64>` are the same
//! region and a handle declaring another size is refused. Only the owner
//! reads or writes it, under a version so a torn value is never
//! published.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::owner_lease::{LeaseError, NO_OWNER, PAYLOAD_BYTES};
use subetha_cxc::raw_owner_lease::RawOwnerLease;

use crate::error::{fail, lease_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_OWNER_LEASE};
use crate::ring::{bytes, finish_unlink, out_buffer, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The largest payload a lease carries.
pub const SUBETHA_LEASE_PAYLOAD_MAX: u64 = 48;
/// The process id that holds no lease; every real process id differs.
pub const SUBETHA_LEASE_NO_OWNER: u32 = 0;

const _: () = assert!(SUBETHA_LEASE_PAYLOAD_MAX as usize == PAYLOAD_BYTES);
const _: () = assert!(SUBETHA_LEASE_NO_OWNER == NO_OWNER);

/// A snapshot of an owner lease.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_owner_lease_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Bytes the payload holds, as the region records it.
    pub payload_size: u32,
    /// The process id holding the lease, or `SUBETHA_LEASE_NO_OWNER`.
    pub owner_pid: u32,
    /// How many times the lease has changed hands.
    pub lease_term: u32,
    /// The payload's version, odd while a write is in flight.
    pub seq_version: u32,
    /// The epoch the owner last beat at.
    pub heartbeat_epoch: u64,
    /// The epoch the grace window is measured against.
    pub global_epoch: u64,
}

pub(crate) struct OwnerLeaseObject {
    lease: RawOwnerLease,
    mode: u32,
}

impl OwnerLeaseObject {
    /// A lease parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_owner_lease_stats {
        subetha_owner_lease_stats {
            mode: self.mode,
            payload_size: self.lease.payload_size() as u32,
            owner_pid: self.lease.current_owner().unwrap_or(NO_OWNER),
            lease_term: self.lease.lease_term(),
            seq_version: self.lease.seq_version(),
            heartbeat_epoch: self.lease.heartbeat_epoch(),
            global_epoch: self.lease.global_epoch(),
        }
    }
}

fn with_lease(handle: subetha_handle, f: impl FnOnce(&OwnerLeaseObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_OWNER_LEASE, |object| match object {
        Object::OwnerLease(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an owner lease"),
    })
}

/// A process id a lease can be held by. Zero is the value that means
/// nobody, so a caller passing it is refused rather than taken as a
/// release.
fn checked_pid(pid: u32) -> Result<u32, i32> {
    if pid == NO_OWNER {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "pid 0 is the value that means no owner",
        ));
    }
    Ok(pid)
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    payload_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if payload_size == 0 || u64::from(payload_size) > SUBETHA_LEASE_PAYLOAD_MAX {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("payload_size {payload_size} is zero or above {SUBETHA_LEASE_PAYLOAD_MAX}"),
        ));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, payload_size as usize, mode))
}

fn place(lease: Result<RawOwnerLease, LeaseError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match lease {
        Ok(lease) => unsafe { issue(Object::OwnerLease(OwnerLeaseObject { lease, mode }), out) },
        Err(e) => lease_code(e),
    }
}

/// Obtain the lease at `path` over a payload of `payload_size` bytes,
/// initialized with the `len` bytes at `initial` when the file does not
/// yet exist. An existing lease is attached with its owner and term in
/// place and `initial` goes unused. A region built for another payload
/// size is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `initial` points to `len`
/// readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_create(
    path: *const c_char,
    initial: *const u8,
    len: usize,
    payload_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, size, mode) = match unsafe { read_arguments(path, payload_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let initial = match unsafe { bytes(initial, len) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        place(RawOwnerLease::create(path, initial, size), mode, out)
    })
}

/// Attach to the lease another process created at `path`; the file must
/// exist and must declare `payload_size`. `SUBETHA_E_RING_IO` names an
/// absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_open(
    path: *const c_char,
    payload_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, size, mode) = match unsafe { read_arguments(path, payload_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawOwnerLease::open(path, size), mode, out)
    })
}

/// Truncate the file at `path` and lay out an unheld lease there,
/// stripping whatever owner and term a live holder has.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `initial` points to `len`
/// readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_reset(
    path: *const c_char,
    initial: *const u8,
    len: usize,
    payload_size: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, size, mode) = match unsafe { read_arguments(path, payload_size, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let initial = match unsafe { bytes(initial, len) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        place(RawOwnerLease::reset(path, initial, size), mode, out)
    })
}

/// Try to take the lease for `pid`, and write whether it was taken into
/// `out_acquired`. It is taken when nobody holds it, when `pid` is below
/// the current holder's and so preempts it, or when the holder's
/// heartbeat has fallen more than `grace_epochs` behind the global epoch.
/// A caller that already holds it is told so rather than refused.
///
/// # Safety
/// `out_acquired` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_try_acquire(
    handle: subetha_handle,
    pid: u32,
    grace_epochs: u64,
    out_acquired: *mut bool,
) -> i32 {
    with_lease(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        if out_acquired.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_acquired is null");
        }
        let acquired = l.lease.try_acquire(pid, grace_epochs);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_acquired = acquired };
        SUBETHA_OK
    })
}

/// Give the lease up, and write whether `pid` was holding it into
/// `out_released`.
///
/// # Safety
/// `out_released` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_release(handle: subetha_handle, pid: u32, out_released: *mut bool) -> i32 {
    with_lease(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        let released = l.lease.release(pid);
        if !out_released.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_released = released };
        }
        SUBETHA_OK
    })
}

/// Copy the payload into `out`, at least `payload_size` bytes, and its
/// length into `out_len`. `SUBETHA_E_NOT_OWNER` when `pid` does not hold
/// the lease, in which case `out` is untouched.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_read(
    handle: subetha_handle,
    pid: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_lease(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        let size = l.lease.payload_size();
        let buf = match unsafe { out_buffer(out, cap, out_len, size) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        if !l.lease.read_as_owner(pid, buf) {
            return fail(
                crate::error::SUBETHA_E_NOT_OWNER,
                format!("pid {pid} does not hold the lease"),
            );
        }
        // SAFETY: checked non-null by `out_buffer`; the caller
        // guarantees it is writable.
        unsafe { *out_len = size };
        SUBETHA_OK
    })
}

/// Overwrite the payload with the `len` bytes at `data`, at most
/// `payload_size` of them, under the version so a reader never sees half
/// of each value. `SUBETHA_E_NOT_OWNER` when `pid` does not hold the
/// lease, in which case nothing is written.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_write(handle: subetha_handle, pid: u32, data: *const u8, len: usize) -> i32 {
    with_lease(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        if len > l.lease.payload_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("{len} bytes exceed the {}-byte payload", l.lease.payload_size()),
            );
        }
        let data = match unsafe { bytes(data, len) } {
            Ok(d) => d,
            Err(code) => return code,
        };
        if !l.lease.write_as_owner(pid, data) {
            return fail(
                crate::error::SUBETHA_E_NOT_OWNER,
                format!("pid {pid} does not hold the lease"),
            );
        }
        SUBETHA_OK
    })
}

/// Refresh the holder's heartbeat, and write whether `pid` still holds
/// the lease into `out_still_owner`. A holder calls this faster than the
/// epoch is ticked, or the grace window expires under it.
///
/// # Safety
/// `out_still_owner` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_beat(handle: subetha_handle, pid: u32, out_still_owner: *mut bool) -> i32 {
    with_lease(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        let still = l.lease.beat(pid);
        if !out_still_owner.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_still_owner = still };
        }
        SUBETHA_OK
    })
}

/// Advance the global epoch and write it into `out_epoch`. The grace
/// window is measured against this, so a stale holder is only detected
/// once something ticks it; whatever does so sets how quickly that
/// happens.
///
/// # Safety
/// `out_epoch` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_tick_epoch(handle: subetha_handle, out_epoch: *mut u64) -> i32 {
    with_lease(handle, |l| {
        let epoch = l.lease.tick_epoch();
        if !out_epoch.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_epoch = epoch };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the lease into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_read_stats(handle: subetha_handle, out: *mut subetha_owner_lease_stats) -> i32 {
    with_lease(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = l.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the lease's page to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_owner_lease_flush(handle: subetha_handle) -> i32 {
    with_lease(handle, |l| match l.lease.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => lease_code(e),
    })
}

/// Remove the lease's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_owner_lease_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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

/// This process's id, which is what a lease is held by. A C caller can
/// reach its own platform's call for this; it is here so a program using
/// the ABI needs nothing else to work the lease.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_current_pid(out: *mut u32) -> i32 {
    entry(|| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = std::process::id() };
        SUBETHA_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!("subetha-ffi-lease-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the lease's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_object_reports_the_holder_and_the_payload_moves_with_the_lease() {
        let scratch = Scratch::new("shape");
        let object = OwnerLeaseObject {
            lease: RawOwnerLease::create(&scratch.0, &[1u8; 8], 8).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        let stats = object.stats();
        assert_eq!(stats.payload_size, 8);
        assert_eq!(stats.owner_pid, SUBETHA_LEASE_NO_OWNER);
        assert_eq!(stats.lease_term, 0);

        assert!(object.lease.try_acquire(100, 3));
        assert_eq!(object.stats().owner_pid, 100);
        assert_eq!(object.stats().lease_term, 1);
        let mut out = [0u8; 8];
        assert!(object.lease.read_as_owner(100, &mut out));
        assert_eq!(out, [1u8; 8]);
        assert!(object.lease.write_as_owner(100, &[2u8; 8]));
        assert!(object.stats().seq_version >= 2, "the write moved the version twice");

        // A lower pid preempts and finds the payload the last holder
        // left.
        assert!(object.lease.try_acquire(50, 3));
        assert!(object.lease.read_as_owner(50, &mut out));
        assert_eq!(out, [2u8; 8]);
        assert_eq!(object.stats().owner_pid, 50);
        assert!(object.lease.release(50));
        assert_eq!(object.stats().owner_pid, SUBETHA_LEASE_NO_OWNER);

        // pid 0 is the value that means nobody, so it is refused rather
        // than taken as a release.
        assert_eq!(checked_pid(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_pid(1).unwrap(), 1);
    }

    #[test]
    fn the_epoch_has_to_be_ticked_for_a_stale_holder_to_be_displaced() {
        let scratch = Scratch::new("stale");
        let object = OwnerLeaseObject {
            lease: RawOwnerLease::create(&scratch.0, &[0u8; 4], 4).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        assert!(object.lease.try_acquire(100, 2));
        assert!(!object.lease.try_acquire(200, 2), "a higher pid waits");

        // Nothing advances the epoch on its own, so a holder that never
        // beats is still safe until something ticks past the window.
        assert_eq!(object.stats().global_epoch, 0);
        assert!(!object.lease.try_acquire(200, 2), "no epoch has passed");
        for _ in 0..3 {
            object.lease.tick_epoch();
        }
        assert_eq!(object.stats().global_epoch, 3);
        assert!(object.lease.try_acquire(200, 2), "three epochs past a grace of two");
        assert_eq!(object.stats().owner_pid, 200);
        assert_eq!(object.stats().heartbeat_epoch, 3, "taking it beats at once");
    }
}
