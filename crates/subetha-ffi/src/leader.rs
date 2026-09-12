//! Leader election through the C ABI: among the processes attached to
//! one file, exactly one is the leader, and which one converges without
//! a vote.
//!
//! The rule is the lowest live process id. A caller claims leadership
//! when nobody holds it, when its own id is below the current leader's,
//! or when the leader's heartbeat has fallen more than `grace_epochs`
//! behind the global epoch. Process ids are unique on a host, so a set of
//! live processes always agrees on which of them deserves the role, and
//! it takes one compare-exchange rather than a quorum.
//!
//! As with `subetha_owner_lease_*`, nothing advances the epoch on its
//! own: something calls `subetha_leader_tick_epoch` at whatever rate it
//! wants a departed leader noticed at, and a leader keeps the role by
//! calling `subetha_leader_beat` faster than that. The election term goes
//! up on every handover, so a follower watching for a change in
//! leadership polls the term rather than the process id, which can come
//! back to the same value.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_leader_election::{LeaderError, SharedLeaderElection, DEFAULT_GRACE_EPOCHS, NO_LEADER};

use crate::error::{fail, leader_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LEADER};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The process id that leads nothing; every real process id differs.
pub const SUBETHA_LEADER_NONE: u32 = 0;
/// The grace window to pass when a caller has no reason to pick another.
pub const SUBETHA_LEADER_DEFAULT_GRACE: u64 = 3;

const _: () = assert!(SUBETHA_LEADER_NONE == NO_LEADER);
const _: () = assert!(SUBETHA_LEADER_DEFAULT_GRACE == DEFAULT_GRACE_EPOCHS);

/// A snapshot of an election.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_leader_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// The process id leading right now, or `SUBETHA_LEADER_NONE`.
    pub leader_pid: u32,
    /// How many times leadership has changed hands. A follower watching
    /// for a change polls this rather than the process id, which can
    /// come back to a value it held before.
    pub election_term: u32,
    /// The epoch the leader last beat at.
    pub leader_heartbeat: u64,
    /// The epoch the grace window is measured against.
    pub global_epoch: u64,
}

pub(crate) struct LeaderObject {
    election: SharedLeaderElection,
    mode: u32,
}

impl LeaderObject {
    /// An election parks nothing inside a call, so a destroy has nobody
    /// to wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_leader_stats {
        subetha_leader_stats {
            mode: self.mode,
            leader_pid: self.election.current_leader().unwrap_or(NO_LEADER),
            election_term: self.election.election_term(),
            leader_heartbeat: self.election.header().leader_heartbeat.load(std::sync::atomic::Ordering::Acquire),
            global_epoch: self.election.global_epoch(),
        }
    }
}

fn with_leader(handle: subetha_handle, f: impl FnOnce(&LeaderObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LEADER, |object| match object {
        Object::Leader(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a leader election"),
    })
}

/// A process id that can lead. Zero is the value that means nobody, so a
/// caller passing it is refused rather than taken as a step-down.
fn checked_pid(pid: u32) -> Result<u32, i32> {
    if pid == NO_LEADER {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "pid 0 is the value that means no leader",
        ));
    }
    Ok(pid)
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(path: *const c_char, mode: u32, out: *mut subetha_handle) -> Result<(&'a Path, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let mode = resolve_mode(mode)?;
    Ok((path, mode))
}

fn place(election: Result<SharedLeaderElection, LeaderError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match election {
        Ok(election) => unsafe { issue(Object::Leader(LeaderObject { election, mode }), out) },
        Err(e) => leader_code(e),
    }
}

/// Obtain the election at `path`: an empty one is initialized when the
/// file does not exist, an existing one is attached with its leader and
/// term in place.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_create(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedLeaderElection::create(path), mode, out)
    })
}

/// Attach to the election another process created at `path`; the file
/// must exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_open(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedLeaderElection::open(path), mode, out)
    })
}

/// Truncate the file at `path` and lay out an election with no leader,
/// discarding whatever a live one holds.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_reset(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, mode) = match unsafe { read_arguments(path, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedLeaderElection::reset(path), mode, out)
    })
}

/// Try to take leadership for `pid`, and write whether it was taken into
/// `out_claimed`. It is taken when nobody leads, when `pid` is below the
/// current leader's and so preempts it, or when the leader's heartbeat
/// has fallen more than `grace_epochs` behind the global epoch.
///
/// # Safety
/// `out_claimed` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_try_claim(
    handle: subetha_handle,
    pid: u32,
    grace_epochs: u64,
    out_claimed: *mut bool,
) -> i32 {
    with_leader(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        if out_claimed.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_claimed is null");
        }
        let claimed = l.election.try_claim_leadership(pid, grace_epochs);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_claimed = claimed };
        SUBETHA_OK
    })
}

/// Refresh the leader's heartbeat, and write whether `pid` still leads
/// into `out_still_leader`. A leader calls this faster than the epoch is
/// ticked, or the grace window expires under it.
///
/// # Safety
/// `out_still_leader` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_beat(handle: subetha_handle, pid: u32, out_still_leader: *mut bool) -> i32 {
    with_leader(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        let still = l.election.beat_as_leader(pid);
        if !out_still_leader.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_still_leader = still };
        }
        SUBETHA_OK
    })
}

/// Give up leadership, and write whether `pid` was leading into
/// `out_stepped`. A process shutting down cleanly does this so the next
/// leader takes over at once rather than waiting out the grace window.
///
/// # Safety
/// `out_stepped` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_step_down(handle: subetha_handle, pid: u32, out_stepped: *mut bool) -> i32 {
    with_leader(handle, |l| {
        let pid = match checked_pid(pid) {
            Ok(p) => p,
            Err(code) => return code,
        };
        let stepped = l.election.step_down(pid);
        if !out_stepped.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_stepped = stepped };
        }
        SUBETHA_OK
    })
}

/// Advance the global epoch and write it into `out_epoch`. The grace
/// window is measured against this, so a departed leader is only noticed
/// once something ticks it.
///
/// # Safety
/// `out_epoch` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_tick_epoch(handle: subetha_handle, out_epoch: *mut u64) -> i32 {
    with_leader(handle, |l| {
        let epoch = l.election.tick_epoch();
        if !out_epoch.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_epoch = epoch };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the election into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_read_stats(handle: subetha_handle, out: *mut subetha_leader_stats) -> i32 {
    with_leader(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = l.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the election's page to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_leader_flush(handle: subetha_handle) -> i32 {
    with_leader(handle, |l| match l.election.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => leader_code(e),
    })
}

/// Remove the election's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_leader_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
            Self(std::env::temp_dir().join(format!("subetha-ffi-leader-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the election's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn the_lowest_process_id_leads_and_the_term_records_every_handover() {
        let scratch = Scratch::new("lowest");
        let object = LeaderObject {
            election: SharedLeaderElection::create(&scratch.0).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        assert_eq!(object.stats().leader_pid, SUBETHA_LEADER_NONE);
        assert_eq!(object.stats().election_term, 0);

        assert!(object.election.try_claim_leadership(100, 3), "nobody leads, so the first caller does");
        assert_eq!(object.stats().leader_pid, 100);
        assert_eq!(object.stats().election_term, 1);
        assert!(!object.election.try_claim_leadership(200, 3), "a higher id does not preempt");
        assert!(object.election.try_claim_leadership(50, 3), "a lower one does");
        assert_eq!(object.stats().leader_pid, 50);
        assert_eq!(object.stats().election_term, 2, "the term records the handover");
        assert!(object.election.try_claim_leadership(50, 3), "the leader asking again is told it leads");
        assert_eq!(object.stats().election_term, 2, "and that is not a handover");

        assert!(object.election.step_down(50));
        assert_eq!(object.stats().leader_pid, SUBETHA_LEADER_NONE);
        assert!(!object.election.step_down(50), "stepping down twice answers false");

        assert_eq!(checked_pid(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(checked_pid(1).unwrap(), 1);
    }

    #[test]
    fn a_leader_that_stops_beating_is_replaced_once_the_epoch_moves() {
        let scratch = Scratch::new("stale");
        let object = LeaderObject {
            election: SharedLeaderElection::create(&scratch.0).unwrap(),
            mode: SUBETHA_MODE_STRICT,
        };
        assert!(object.election.try_claim_leadership(100, 2));

        // Nothing advances the epoch on its own, so a leader that never
        // beats still leads until something ticks past the window.
        assert!(!object.election.try_claim_leadership(200, 2), "no epoch has passed");
        for _ in 0..3 {
            object.election.tick_epoch();
        }
        assert_eq!(object.stats().global_epoch, 3);
        assert!(object.election.try_claim_leadership(200, 2), "three epochs past a grace of two");
        assert_eq!(object.stats().leader_pid, 200);
        assert_eq!(object.stats().leader_heartbeat, 3, "taking the role beats at once");

        // A beat puts the leader back out of reach of a higher id.
        object.election.tick_epoch();
        object.election.tick_epoch();
        object.election.tick_epoch();
        assert!(object.election.beat_as_leader(200));
        assert!(!object.election.beat_as_leader(300), "a follower's beat does nothing");
        assert!(!object.election.try_claim_leadership(300, 2), "the fresh leader keeps it");
    }
}
