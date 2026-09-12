//! The heartbeat table through the C ABI: a fixed set of slots in a file
//! where each process registers itself and says, by beating, that it is
//! still there.
//!
//! `subetha_owner_lease_*` and `subetha_leader_*` each track one holder.
//! This tracks all of them at once, so a supervisor can ask which
//! processes are alive, which have gone quiet, and what work each was
//! holding when it went. A slot carries the process id, the epoch it last
//! beat at, a bitmap of up to `SUBETHA_HEARTBEAT_IN_FLIGHT` work units it
//! has taken, and a role a caller assigns meaning to.
//!
//! As everywhere in this tier, the grace window is counted in epochs and
//! nothing advances them on its own: something calls
//! `subetha_heartbeat_tick_epoch` at whatever rate it wants a departed
//! process noticed at, and a live process calls `subetha_heartbeat_beat`
//! faster than that. A slot whose epoch has fallen behind names a process
//! to check on, and the in-flight bits it left are the work that needs
//! reassigning.
//!
//! Each slot is its own cache line, so processes beating at once never
//! contend, and a slot is written under a version a reader retries on so
//! a snapshot is never half of one process and half of another.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::heartbeat::{heartbeat_file_size, HeartbeatError, HeartbeatTable, EMPTY_PID, IN_FLIGHT_SLOTS};

use crate::error::{fail, heartbeat_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_OUT_OF_BOUNDS, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_HEARTBEAT};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The process id in a slot nobody holds.
pub const SUBETHA_HEARTBEAT_EMPTY_PID: u32 = 0;
/// Work units one slot's bitmap tracks, so a bit index runs from zero to
/// one below this.
pub const SUBETHA_HEARTBEAT_IN_FLIGHT: u32 = 64;

const _: () = assert!(SUBETHA_HEARTBEAT_EMPTY_PID == EMPTY_PID);
const _: () = assert!(SUBETHA_HEARTBEAT_IN_FLIGHT as usize == IN_FLIGHT_SLOTS);

/// One slot's state, read under its version.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_heartbeat_slot {
    /// The process holding the slot, or `SUBETHA_HEARTBEAT_EMPTY_PID`.
    pub pid: u32,
    /// The role the holder set; the meaning is the caller's.
    pub role: u32,
    /// The epoch the holder last beat at.
    pub last_seen_epoch: u64,
    /// The work units the holder has taken, one per bit.
    pub in_flight_bitmap: u64,
}

/// A snapshot of a heartbeat table.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_heartbeat_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots the table holds.
    pub capacity: u32,
    /// Slots a process holds right now.
    pub registered: u32,
    /// The epoch a grace window is measured against.
    pub global_epoch: u64,
    /// Bytes the table takes on disk.
    pub file_size: u64,
}

pub(crate) struct HeartbeatObject {
    /// Shared so an epoch barrier can count live peers in the same table
    /// this handle beats into, rather than opening a second view of it.
    table: Arc<HeartbeatTable>,
    mode: u32,
}

impl HeartbeatObject {
    /// A table parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    /// The table itself, for a barrier counting live peers in it. The
    /// barrier holds its own reference, so it keeps working after the
    /// heartbeat handle that built it is destroyed.
    pub(crate) fn table(&self) -> &Arc<HeartbeatTable> {
        &self.table
    }

    fn stats(&self) -> subetha_heartbeat_stats {
        let registered = (0..self.table.capacity())
            .filter(|i| self.table.snapshot(*i).is_some())
            .count();
        subetha_heartbeat_stats {
            mode: self.mode,
            capacity: self.table.capacity() as u32,
            registered: registered as u32,
            global_epoch: self.table.global_epoch(),
            file_size: heartbeat_file_size(self.table.capacity()) as u64,
        }
    }
}

fn with_heartbeat(handle: subetha_handle, f: impl FnOnce(&HeartbeatObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_HEARTBEAT, |object| match object {
        Object::Heartbeat(h) => f(h),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a heartbeat table"),
    })
}

/// A slot index the table holds.
fn checked_slot(h: &HeartbeatObject, slot: u32) -> Result<usize, i32> {
    if slot as usize >= h.table.capacity() {
        return Err(fail(
            SUBETHA_E_OUT_OF_BOUNDS,
            format!("slot {slot} is past the {} the table holds", h.table.capacity()),
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

fn place(table: Result<HeartbeatTable, HeartbeatError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match table {
        Ok(table) => unsafe {
            issue(Object::Heartbeat(HeartbeatObject { table: Arc::new(table), mode }), out)
        },
        Err(e) => heartbeat_code(e),
    }
}

/// Obtain the table at `path` with `capacity` slots: an empty one is
/// initialized when the file does not exist, an existing one is attached
/// with its registered processes in place, so a late joiner does not
/// unregister anyone. A file built with another capacity is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_create(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(HeartbeatTable::create(path, capacity), mode, out)
    })
}

/// Attach to the table another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_open(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(HeartbeatTable::open(path, capacity), mode, out)
    })
}

/// Truncate the file at `path` and lay out an empty table, unregistering
/// every process that held a slot.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_reset(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(HeartbeatTable::reset(path, capacity), mode, out)
    })
}

/// Take a free slot for `pid` and write its index into `out_slot`. The
/// index is what every later call about this process uses.
/// `SUBETHA_E_RING_FULL` when every slot is held.
///
/// # Safety
/// `out_slot` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_register(handle: subetha_handle, pid: u32, out_slot: *mut u32) -> i32 {
    with_heartbeat(handle, |h| {
        if pid == EMPTY_PID {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "pid 0 is the value that means a free slot");
        }
        if out_slot.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_slot is null");
        }
        match h.table.register(pid) {
            Ok(slot) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_slot = slot as u32 };
                SUBETHA_OK
            }
            Err(e) => heartbeat_code(e),
        }
    })
}

/// Free the slot at `slot`, so another process can take it. A process
/// shutting down cleanly does this rather than leaving a slot for a
/// watcher to reclaim.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_heartbeat_unregister(handle: subetha_handle, slot: u32) -> i32 {
    with_heartbeat(handle, |h| {
        let slot = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        h.table.unregister(slot);
        SUBETHA_OK
    })
}

/// Stamp the slot at `slot` with the current epoch, which is how its
/// holder says it is still there.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_heartbeat_beat(handle: subetha_handle, slot: u32) -> i32 {
    with_heartbeat(handle, |h| {
        let slot = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        h.table.beat(slot);
        SUBETHA_OK
    })
}

/// Advance the global epoch and write it into `out_epoch`. Every grace
/// window is measured against this, so a departed process is only
/// noticed once something ticks it.
///
/// # Safety
/// `out_epoch` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_tick_epoch(handle: subetha_handle, out_epoch: *mut u64) -> i32 {
    with_heartbeat(handle, |h| {
        let epoch = h.table.tick_global_epoch();
        if !out_epoch.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_epoch = epoch };
        }
        SUBETHA_OK
    })
}

/// Set bit `bit` in the slot's in-flight bitmap, saying its holder has
/// taken that work unit. `bit` runs from zero to one below
/// `SUBETHA_HEARTBEAT_IN_FLIGHT`.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_heartbeat_mark_in_flight(handle: subetha_handle, slot: u32, bit: u32) -> i32 {
    with_heartbeat(handle, |h| {
        let slot = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        if bit >= SUBETHA_HEARTBEAT_IN_FLIGHT {
            return fail(
                SUBETHA_E_OUT_OF_BOUNDS,
                format!("bit {bit} is past the {SUBETHA_HEARTBEAT_IN_FLIGHT} a slot tracks"),
            );
        }
        h.table.mark_in_flight(slot, bit as u8);
        SUBETHA_OK
    })
}

/// Clear bit `bit` in the slot's in-flight bitmap, saying its holder has
/// finished that work unit.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_heartbeat_clear_in_flight(handle: subetha_handle, slot: u32, bit: u32) -> i32 {
    with_heartbeat(handle, |h| {
        let slot = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        if bit >= SUBETHA_HEARTBEAT_IN_FLIGHT {
            return fail(
                SUBETHA_E_OUT_OF_BOUNDS,
                format!("bit {bit} is past the {SUBETHA_HEARTBEAT_IN_FLIGHT} a slot tracks"),
            );
        }
        h.table.clear_in_flight(slot, bit as u8);
        SUBETHA_OK
    })
}

/// Read the slot at `slot` into `out`, under its version so the answer is
/// never half of one write and half of another. Writes
/// `SUBETHA_HEARTBEAT_EMPTY_PID` into `out->pid` for a slot nobody holds,
/// which is not an error: a watcher walks every slot and reads the empty
/// ones as empty.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_read_slot(handle: subetha_handle, slot: u32, out: *mut subetha_heartbeat_slot) -> i32 {
    with_heartbeat(handle, |h| {
        let index = match checked_slot(h, slot) {
            Ok(s) => s,
            Err(code) => return code,
        };
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let read = match h.table.snapshot(index) {
            Some(s) => subetha_heartbeat_slot {
                pid: s.pid,
                role: s.role,
                last_seen_epoch: s.last_seen_epoch,
                in_flight_bitmap: s.in_flight_bitmap,
            },
            None => subetha_heartbeat_slot::default(),
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = read };
        SUBETHA_OK
    })
}

/// A snapshot of the table into `out`. The registered count is a walk of
/// every slot, so it races a register or an unregister in some process.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_heartbeat_read_stats(handle: subetha_handle, out: *mut subetha_heartbeat_stats) -> i32 {
    with_heartbeat(handle, |h| {
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
pub unsafe extern "C" fn subetha_heartbeat_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-heartbeat-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the table's file was removed: {:?}", found.first_failure);
        }
    }

    fn object(path: &Path, capacity: usize) -> HeartbeatObject {
        HeartbeatObject {
            table: Arc::new(HeartbeatTable::create(path, capacity).unwrap()),
            mode: SUBETHA_MODE_STRICT,
        }
    }

    #[test]
    fn slots_are_taken_and_given_back_and_a_quiet_one_shows_its_last_epoch() {
        let scratch = Scratch::new("slots");
        let h = object(&scratch.0, 4);
        assert_eq!(h.stats().capacity, 4);
        assert_eq!(h.stats().registered, 0);
        assert_eq!(h.stats().file_size, heartbeat_file_size(4) as u64);

        let first = h.table.register(100).expect("a free slot");
        let second = h.table.register(200).expect("another");
        assert_eq!(h.stats().registered, 2);
        let snapshot = h.table.snapshot(first).expect("the slot is held");
        assert_eq!(snapshot.pid, 100);
        assert_eq!(snapshot.last_seen_epoch, 0);
        assert!(h.table.snapshot(3).is_none(), "a slot nobody holds reads as empty");

        // One process keeps beating while the other goes quiet, so the
        // gap between their epochs is what names the one to check on.
        for _ in 0..3 {
            h.table.tick_global_epoch();
            h.table.beat(first);
        }
        assert_eq!(h.stats().global_epoch, 3);
        assert_eq!(h.table.snapshot(first).expect("held").last_seen_epoch, 3, "the live one is current");
        assert_eq!(h.table.snapshot(second).expect("held").last_seen_epoch, 0, "the quiet one is behind");

        // The work a process took is on its slot, so a watcher knows what
        // to reassign when it goes.
        h.table.mark_in_flight(second, 0);
        h.table.mark_in_flight(second, 63);
        let quiet = h.table.snapshot(second).expect("held");
        assert_eq!(quiet.in_flight_bitmap, (1u64 << 63) | 1);
        h.table.clear_in_flight(second, 0);
        assert_eq!(h.table.snapshot(second).expect("held").in_flight_bitmap, 1u64 << 63);

        h.table.unregister(second);
        assert_eq!(h.stats().registered, 1);
        assert!(h.table.snapshot(second).is_none(), "the freed slot reads as empty");
        assert_eq!(h.table.register(300).expect("the freed slot comes back"), second);

        assert_eq!(checked_slot(&h, 4).unwrap_err(), SUBETHA_E_OUT_OF_BOUNDS);
        assert_eq!(checked_slot(&h, 3).unwrap(), 3);
    }

    #[test]
    fn a_full_table_refuses_the_next_registration() {
        let scratch = Scratch::new("full");
        let h = object(&scratch.0, 2);
        h.table.register(100).expect("the first slot");
        h.table.register(200).expect("the second");
        assert_eq!(h.stats().registered, 2);
        match h.table.register(300) {
            Ok(slot) => panic!("a full table handed out slot {slot}"),
            Err(e) => assert_eq!(e, HeartbeatError::TableFull),
        }
    }
}
