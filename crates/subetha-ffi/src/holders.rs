//! The holder table through the C ABI: a fixed array of claimable slots,
//! each stamped with the process holding it, so a holder that died can be
//! told from one that is working.
//!
//! This is the substrate under the epoch table's pins and tickets,
//! reached directly. A bare count cannot answer whether a holder is still
//! running, because a number cannot be asked; a slot carrying its
//! process id can, and `subetha_holders_reap_dead` is that question asked
//! of every slot at once.
//!
//! What a payload means is the caller's. It is one `uint64_t` per slot at
//! any encoding the processes sharing the file agree on, with two values
//! reserved because the table itself reads them:
//! `SUBETHA_HOLDER_FREE` for a slot nobody holds and
//! `SUBETHA_HOLDER_RESERVED` for one claimed whose payload is not decided
//! yet.
//!
//! # Reserve, then publish
//!
//! A caller whose payload depends on state it reads at claim time has to
//! make the slot visible before reading that state, or another party sees
//! the slot as free, acts on it, and is wrong an instant later.
//! `subetha_holders_reserve` publishes the slot with nothing decided and
//! `subetha_holders_publish` fills it in. `subetha_holders_claim` does
//! both at once, for a caller whose payload does not depend on anything
//! read between them.
//!
//! A reader walking the slots that must not act on a half-formed claim
//! restarts its walk when it sees `SUBETHA_HOLDER_RESERVED`, rather than
//! reading past it: the slot is a holder an instant later, and a decision
//! made without it is a decision made against a table that no longer
//! exists.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::holder_table::{HOLDER_FREE, HOLDER_RESERVED};
use subetha_cxc::shared_holder_table::{shared_holder_file_size, SharedHolderError, SharedHolderTable};

use crate::error::{fail, holders_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_RING_FULL, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_HOLDERS};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The state of a slot nobody holds.
pub const SUBETHA_HOLDER_FREE: u64 = 0;
/// The state of a slot claimed whose payload is not decided yet. A
/// caller's payload may not be this value.
pub const SUBETHA_HOLDER_RESERVED: u64 = u64::MAX;

const _: () = assert!(SUBETHA_HOLDER_FREE == HOLDER_FREE);
const _: () = assert!(SUBETHA_HOLDER_RESERVED == HOLDER_RESERVED);

/// A snapshot of a holder table.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_holders_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the table holds.
    pub capacity: u32,
    /// Slots held right now, across every process. A slot that is
    /// reserved but not published does not count.
    pub live: u32,
    /// Bytes the table takes on disk.
    pub file_size: u64,
}

pub(crate) struct HoldersObject {
    holders: SharedHolderTable,
    mode: u32,
}

impl HoldersObject {
    /// A table parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_holders_stats {
        subetha_holders_stats {
            mode: self.mode,
            capacity: self.holders.capacity() as u32,
            live: self.holders.live() as u32,
            file_size: shared_holder_file_size(self.holders.capacity()) as u64,
        }
    }
}

fn with_holders(handle: subetha_handle, f: impl FnOnce(&HoldersObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_HOLDERS, |object| match object {
        Object::Holders(h) => f(h),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a holder table"),
    })
}

/// A slot index the table holds.
fn checked_slot(h: &HoldersObject, slot: u32) -> Result<usize, i32> {
    if slot as usize >= h.holders.capacity() {
        return Err(fail(
            SUBETHA_E_OUT_OF_BOUNDS,
            format!("slot {slot} is past the {} the table holds", h.holders.capacity()),
        ));
    }
    Ok(slot as usize)
}

/// A payload a caller may set. The two values the table reads for itself
/// are refused rather than stored, since storing one makes a held slot
/// read as free or as still being decided.
fn checked_payload(payload: u64) -> Result<u64, i32> {
    if payload == HOLDER_FREE {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "a payload of SUBETHA_HOLDER_FREE makes the slot read as unheld",
        ));
    }
    if payload == HOLDER_RESERVED {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "a payload of SUBETHA_HOLDER_RESERVED makes the slot read as still being decided",
        ));
    }
    Ok(payload)
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

fn place(holders: Result<SharedHolderTable, SharedHolderError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match holders {
        Ok(holders) => unsafe { issue(Object::Holders(HoldersObject { holders, mode }), out) },
        Err(e) => holders_code(e),
    }
}

/// Obtain the table at `path` with `capacity` slots: an empty one is laid
/// out when the file does not exist, an existing one is attached with its
/// claims in place, so a late joiner does not release anyone. A file
/// built with another capacity is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_create(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedHolderTable::create(path, capacity), mode, out)
    })
}

/// Attach to the table another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_open(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedHolderTable::open(path, capacity), mode, out)
    })
}

/// Truncate the file at `path` and lay out an empty table, releasing
/// every slot a live holder owns.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_reset(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedHolderTable::reset(path, capacity), mode, out)
    })
}

/// Claim a free slot with `payload` and write its index into `out_slot`.
/// `SUBETHA_E_RING_FULL` when every slot is held.
///
/// # Safety
/// `out_slot` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_claim(handle: subetha_handle, payload: u64, out_slot: *mut u32) -> i32 {
    with_holders(handle, |h| {
        let payload = match checked_payload(payload) {
            Ok(p) => p,
            Err(code) => return code,
        };
        if out_slot.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_slot is null");
        }
        match h.holders.claim(payload) {
            Some(slot) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_slot = slot as u32 };
                SUBETHA_OK
            }
            None => SUBETHA_E_RING_FULL,
        }
    })
}

/// Claim a free slot with nothing decided yet and write its index into
/// `out_slot`. The slot is visible to every process at once, so a caller
/// may then read whatever its payload depends on and call
/// `subetha_holders_publish`. `SUBETHA_E_RING_FULL` when every slot is
/// held.
///
/// # Safety
/// `out_slot` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_reserve(handle: subetha_handle, out_slot: *mut u32) -> i32 {
    with_holders(handle, |h| {
        if out_slot.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_slot is null");
        }
        match h.holders.reserve() {
            Some(slot) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_slot = slot as u32 };
                SUBETHA_OK
            }
            None => SUBETHA_E_RING_FULL,
        }
    })
}

/// Fill in the payload of a slot `subetha_holders_reserve` took.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_holders_publish(handle: subetha_handle, slot: u32, payload: u64) -> i32 {
    with_holders(handle, |h| {
        let index = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        let payload = match checked_payload(payload) {
            Ok(p) => p,
            Err(code) => return code,
        };
        h.holders.publish(index, payload);
        SUBETHA_OK
    })
}

/// Claim the slot at `slot` in particular, and write whether it was free
/// into `out_claimed`. For a caller that has already decided which slot
/// it wants rather than taking whichever is free.
///
/// # Safety
/// `out_claimed` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_try_claim_slot(
    handle: subetha_handle,
    slot: u32,
    payload: u64,
    out_claimed: *mut bool,
) -> i32 {
    with_holders(handle, |h| {
        let index = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        let payload = match checked_payload(payload) {
            Ok(p) => p,
            Err(code) => return code,
        };
        if out_claimed.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_claimed is null");
        }
        let claimed = h.holders.try_claim_slot(index, payload);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_claimed = claimed };
        SUBETHA_OK
    })
}

/// Give the slot at `slot` back.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_holders_release(handle: subetha_handle, slot: u32) -> i32 {
    with_holders(handle, |h| {
        let index = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        h.holders.release(index);
        SUBETHA_OK
    })
}

/// Read the slot at `slot`: its state into `out_state` and the process
/// holding it into `out_pid`. The state is `SUBETHA_HOLDER_FREE`,
/// `SUBETHA_HOLDER_RESERVED`, or the payload, and a caller walking the
/// table restarts its walk on a reservation rather than reading past one.
///
/// # Safety
/// `out_state` and `out_pid` are null or valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_read_slot(
    handle: subetha_handle,
    slot: u32,
    out_state: *mut u64,
    out_pid: *mut u32,
) -> i32 {
    with_holders(handle, |h| {
        let index = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        // The state is read before the pid, so a caller that sees a
        // payload sees the process that set it rather than one that has
        // released since.
        let state = h.holders.state(index);
        let pid = h.holders.owner_pid(index);
        if !out_state.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_state = state };
        }
        if !out_pid.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_pid = pid };
        }
        SUBETHA_OK
    })
}

/// Release every slot whose owning process is gone, and write how many
/// went into `out_freed`. This is the question a bare count cannot
/// answer: a number cannot be asked whether it is still running, and a
/// slot carrying its process id can.
///
/// # Safety
/// `out_freed` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_reap_dead(handle: subetha_handle, out_freed: *mut u32) -> i32 {
    with_holders(handle, |h| {
        let freed = h.holders.reap_dead();
        if !out_freed.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_freed = freed as u32 };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the table into `out`. The live count races a claim or a
/// release in some process, so it is a diagnostic rather than something
/// to branch on.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_read_stats(handle: subetha_handle, out: *mut subetha_holders_stats) -> i32 {
    with_holders(handle, |h| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = h.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove the table's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_holders_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-holders-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the table's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn slots_carry_a_payload_and_the_process_that_set_it() {
        let scratch = Scratch::new("claims");
        let h = HoldersObject {
            holders: SharedHolderTable::create(&scratch.0, 4).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        assert_eq!(h.stats().capacity, 4);
        assert_eq!(h.stats().live, 0);
        assert_eq!(h.stats().file_size, shared_holder_file_size(4) as u64);

        let first = h.holders.claim(7).expect("a free slot");
        assert_eq!(h.stats().live, 1);
        assert_eq!(h.holders.state(first), 7);
        assert_eq!(h.holders.owner_pid(first), std::process::id());
        assert_eq!(h.holders.state(3), SUBETHA_HOLDER_FREE, "an unclaimed slot reads as free");

        // A reservation is visible as held but reports no payload, which
        // is what stops a reader acting on a claim that is not finished.
        let held = h.holders.reserve().expect("a slot to reserve");
        assert_eq!(h.holders.state(held), SUBETHA_HOLDER_RESERVED);
        assert_eq!(h.holders.payload(held), None);
        h.holders.publish(held, 13);
        assert_eq!(h.holders.payload(held), Some(13));

        h.holders.release(first);
        assert_eq!(h.stats().live, 1);
        assert_eq!(h.holders.state(first), SUBETHA_HOLDER_FREE);

        // The two values the table reads for itself are refused rather
        // than stored: either makes a held slot read as something else.
        assert_eq!(checked_payload(SUBETHA_HOLDER_FREE).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_payload(SUBETHA_HOLDER_RESERVED).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_payload(1).unwrap(), 1);
        assert_eq!(checked_slot(&h, 4).unwrap_err(), SUBETHA_E_OUT_OF_BOUNDS);
        assert_eq!(checked_slot(&h, 3).unwrap(), 3);
    }

    #[test]
    fn a_reap_leaves_a_live_process_alone() {
        let scratch = Scratch::new("reap");
        let h = HoldersObject {
            holders: SharedHolderTable::create(&scratch.0, 2).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        h.holders.claim(5).expect("a free slot");
        assert_eq!(h.stats().live, 1);
        assert_eq!(h.holders.reap_dead(), 0, "this process is alive, so its slot stands");
        assert_eq!(h.stats().live, 1);
    }
}
