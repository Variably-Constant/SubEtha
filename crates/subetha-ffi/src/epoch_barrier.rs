//! The epoch barrier through the C ABI: a rendezvous across processes,
//! where each one waits at an epoch until the others reach it.
//!
//! The barrier counts arrivals in one shared word and takes its notion of
//! how many peers there are from a heartbeat table, so a process that dies
//! between rounds stops being waited for once its slot goes stale. A
//! caller passes the heartbeat handle it is already beating into, and the
//! barrier holds its own reference to that table: destroying the heartbeat
//! handle afterwards leaves the barrier working.
//!
//! The claim a barrier makes is ORDER, not duration. A process that
//! arrives early does not pass until the late one gets there; nothing is
//! promised about how long either waits.
//!
//! A wait blocks the calling thread. `subetha_epoch_barrier_wait_timeout`
//! bounds it, and a quorum variant releases once enough peers have
//! arrived rather than all of them, which is what a caller uses when a
//! straggler must not hold up the round.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::epoch_barrier::{BarrierError, EpochBarrier};
use subetha_cxc::heartbeat::HeartbeatTable;

use crate::error::{barrier_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_EPOCH_BARRIER, SUBETHA_KIND_HEARTBEAT};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The grace window a barrier uses when the caller passes zero: how many
/// epochs a heartbeat slot may be stale before it stops counting as a
/// peer worth waiting for.
///
/// Spelled as a literal because cbindgen copies the right-hand side into
/// the header verbatim, and a name from another crate does not exist
/// there. `the_default_grace_matches_the_primitive` holds it to the
/// value it mirrors.
pub const SUBETHA_BARRIER_GRACE_DEFAULT: u64 = 3;

/// A snapshot of a barrier.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_epoch_barrier_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// The epoch the barrier stands at.
    pub epoch: u32,
    /// Peers that have arrived at that epoch and not yet been released.
    pub arrived: u32,
    /// Peers the heartbeat table shows as live, which is how many an
    /// unqualified wait waits for.
    pub live_peers: u32,
    /// Epochs a heartbeat slot may be stale before it stops counting.
    pub grace_epochs: u64,
}

pub(crate) struct EpochBarrierObject {
    barrier: EpochBarrier,
    mode: u32,
    /// Kept here because the barrier takes it at construction and does not
    /// report it back.
    grace_epochs: u64,
}

impl EpochBarrierObject {
    /// A waiting thread is inside a backoff loop on shared memory rather
    /// than parked on an object this can signal, so a destroy has nothing
    /// to wake. A wait outlives its handle by at most one backoff step.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_epoch_barrier_stats {
        let (epoch, arrived) = self.barrier.snapshot();
        subetha_epoch_barrier_stats {
            mode: self.mode,
            epoch,
            arrived,
            live_peers: self.barrier.live_peer_count(),
            grace_epochs: self.grace_epochs,
        }
    }
}

fn with_barrier(handle: subetha_handle, f: impl FnOnce(&EpochBarrierObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_EPOCH_BARRIER, |object| match object {
        Object::EpochBarrier(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an epoch barrier"),
    })
}

/// The heartbeat table a barrier is to count peers in, taken from the
/// caller's handle. The barrier keeps its own reference, so the caller
/// may destroy that handle afterwards.
fn heartbeat_table(handle: subetha_handle) -> Result<Arc<HeartbeatTable>, i32> {
    let mut taken = None;
    let code = with_kind(handle, SUBETHA_KIND_HEARTBEAT, |object| match object {
        Object::Heartbeat(h) => {
            taken = Some(Arc::clone(h.table()));
            SUBETHA_OK
        }
        _ => fail(SUBETHA_E_WRONG_KIND, "heartbeat does not name a heartbeat table"),
    });
    match taken {
        Some(table) => Ok(table),
        None => Err(code),
    }
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    heartbeat: subetha_handle,
    grace_epochs: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, Arc<HeartbeatTable>, u64, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    let table = heartbeat_table(heartbeat)?;
    let grace = if grace_epochs == 0 { SUBETHA_BARRIER_GRACE_DEFAULT } else { grace_epochs };
    let mode = resolve_mode(mode)?;
    Ok((path, table, grace, mode))
}

fn place(
    barrier: Result<EpochBarrier, BarrierError>,
    mode: u32,
    grace_epochs: u64,
    out: *mut subetha_handle,
) -> i32 {
    match barrier {
        Ok(barrier) => unsafe {
            issue(Object::EpochBarrier(EpochBarrierObject { barrier, mode, grace_epochs }), out)
        },
        Err(e) => barrier_code(e),
    }
}

/// Obtain the barrier at `path`, counting peers in the heartbeat table
/// `heartbeat` names: an empty one is initialized when the file does not
/// exist, an existing one is attached at the epoch it stands at. A
/// `grace_epochs` of zero takes `SUBETHA_BARRIER_GRACE_DEFAULT`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epoch_barrier_create(
    path: *const c_char,
    heartbeat: subetha_handle,
    grace_epochs: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, table, grace, mode) =
            match unsafe { read_arguments(path, heartbeat, grace_epochs, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        place(EpochBarrier::create(path, table, grace), mode, grace, out)
    })
}

/// Attach to the barrier another process created at `path`; the file must
/// exist.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epoch_barrier_open(
    path: *const c_char,
    heartbeat: subetha_handle,
    grace_epochs: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, table, grace, mode) =
            match unsafe { read_arguments(path, heartbeat, grace_epochs, mode, out) } {
                Ok(a) => a,
                Err(code) => return code,
            };
        place(EpochBarrier::open(path, table, grace), mode, grace, out)
    })
}

/// Wait at `epoch` until every live peer has reached it. Blocks the
/// calling thread. `SUBETHA_E_BARRIER_EPOCH_PASSED` when the barrier is
/// already past `epoch`, and `SUBETHA_E_BARRIER_NO_LIVE_PEERS` when the
/// heartbeat table shows nobody to wait for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_epoch_barrier_wait(handle: subetha_handle, epoch: u32) -> i32 {
    with_barrier(handle, |b| match b.barrier.wait(epoch) {
        Ok(()) => SUBETHA_OK,
        Err(e) => barrier_code(e),
    })
}

/// `subetha_epoch_barrier_wait`, released once `quorum` peers have
/// arrived rather than all of them, so one straggler cannot hold a round.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_epoch_barrier_wait_quorum(handle: subetha_handle, epoch: u32, quorum: u32) -> i32 {
    with_barrier(handle, |b| {
        if quorum == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "quorum is zero");
        }
        match b.barrier.wait_quorum(epoch, quorum) {
            Ok(()) => SUBETHA_OK,
            Err(e) => barrier_code(e),
        }
    })
}

/// `subetha_epoch_barrier_wait` with a deadline; `SUBETHA_E_TIMEOUT` when
/// `timeout_ms` elapses first. The arrival stands, so a later wait at the
/// same epoch does not count this caller twice.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_epoch_barrier_wait_timeout(handle: subetha_handle, epoch: u32, timeout_ms: u64) -> i32 {
    with_barrier(handle, |b| match b.barrier.wait_timeout(epoch, Duration::from_millis(timeout_ms)) {
        Ok(()) => SUBETHA_OK,
        Err(e) => barrier_code(e),
    })
}

/// `subetha_epoch_barrier_wait_quorum` with a deadline.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_epoch_barrier_wait_quorum_timeout(
    handle: subetha_handle,
    epoch: u32,
    quorum: u32,
    timeout_ms: u64,
) -> i32 {
    with_barrier(handle, |b| {
        if quorum == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "quorum is zero");
        }
        match b.barrier.wait_quorum_timeout(epoch, quorum, Duration::from_millis(timeout_ms)) {
            Ok(()) => SUBETHA_OK,
            Err(e) => barrier_code(e),
        }
    })
}

/// The epoch the barrier stands at, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epoch_barrier_epoch(handle: subetha_handle, out: *mut u32) -> i32 {
    with_barrier(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let epoch = b.barrier.current_epoch();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = epoch };
        SUBETHA_OK
    })
}

/// Peers the heartbeat table shows as live, into `out`: how many an
/// unqualified wait waits for.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epoch_barrier_live_peers(handle: subetha_handle, out: *mut u32) -> i32 {
    with_barrier(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let live = b.barrier.live_peer_count();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = live };
        SUBETHA_OK
    })
}

/// A snapshot of the barrier into `out`. Every count races an arrival, so
/// it describes a moment that has already passed.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epoch_barrier_read_stats(
    handle: subetha_handle,
    out: *mut subetha_epoch_barrier_stats,
) -> i32 {
    with_barrier(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = b.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Flush the barrier's state to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_epoch_barrier_flush(handle: subetha_handle) -> i32 {
    with_barrier(handle, |b| match b.barrier.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => barrier_code(e),
    })
}

/// Remove the barrier's file. On Windows the file must not be mapped by
/// any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epoch_barrier_unlink(path: *const c_char, out: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => PathBuf::from(p),
            Err(code) => return code,
        };
        let mut report = UnlinkReport::default();
        let mut state = path.clone();
        let stem = match state.file_name() {
            Some(name) => name.to_string_lossy().to_string(),
            None => return fail(SUBETHA_E_INVALID_ARGUMENT, "path names no file"),
        };
        state.set_file_name(format!("{stem}.state.bin"));
        report.remove(state);
        unsafe { finish_unlink(report, out) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    struct Scratch(PathBuf, PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let pid = std::process::id();
            let dir = std::env::temp_dir();
            Self(
                dir.join(format!("subetha-ffi-barrier-{name}-{pid}.bin")),
                dir.join(format!("subetha-ffi-barrier-{name}-{pid}-beats.bin")),
            )
        }

        /// A barrier and the heartbeat table it counts peers in, with one
        /// slot already beating so there is a live peer to wait for.
        fn build(&self, grace: u64) -> (EpochBarrierObject, Arc<HeartbeatTable>) {
            let beats = Arc::new(HeartbeatTable::create(&self.1, 8).expect("a heartbeat table"));
            beats.register(std::process::id()).expect("a slot to beat in");
            let barrier = EpochBarrier::create(&self.0, Arc::clone(&beats), grace).expect("a barrier");
            (EpochBarrierObject { barrier, mode: SUBETHA_MODE_STRICT, grace_epochs: grace }, beats)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            let mut state = self.0.clone();
            let stem = state.file_name().expect("a file name").to_string_lossy().to_string();
            state.set_file_name(format!("{stem}.state.bin"));
            found.remove(state);
            found.remove(self.1.clone());
            assert_eq!(found.failed, 0, "the barrier's files were removed: {:?}", found.first_failure);
        }
    }

    /// The claim a barrier makes: the early arriver does not pass until
    /// the late one reaches it. The late thread sets a flag before it
    /// arrives, so a waiter that returned early would read it unset.
    #[test]
    fn an_early_arriver_does_not_pass_until_the_late_one_arrives() {
        let scratch = Scratch::new("order");
        let (barrier, beats) = scratch.build(SUBETHA_BARRIER_GRACE_DEFAULT);
        beats.register(std::process::id() + 1).expect("a second slot");
        assert_eq!(barrier.stats().live_peers, 2, "two slots are beating");

        let late_has_arrived = AtomicU32::new(0);
        std::thread::scope(|s| {
            s.spawn(|| {
                std::thread::sleep(Duration::from_millis(50));
                late_has_arrived.store(1, Ordering::Release);
                barrier.barrier.wait(0).expect("the late peer arrives");
            });
            barrier.barrier.wait(0).expect("the early peer is released");
            assert_eq!(
                late_has_arrived.load(Ordering::Acquire),
                1,
                "the early waiter was released before the late one arrived",
            );
        });
    }

    /// A quorum releases on enough arrivals rather than all of them, so
    /// one peer that never comes does not hold the round.
    #[test]
    fn a_quorum_releases_without_the_straggler() {
        let scratch = Scratch::new("quorum");
        let (barrier, beats) = scratch.build(SUBETHA_BARRIER_GRACE_DEFAULT);
        beats.register(std::process::id() + 1).expect("a second slot");
        beats.register(std::process::id() + 2).expect("a third slot");
        assert_eq!(barrier.stats().live_peers, 3);

        // One arrival short of everyone, and it still comes back.
        std::thread::scope(|s| {
            s.spawn(|| barrier.barrier.wait_quorum(0, 2).expect("the second of two"));
            barrier.barrier.wait_quorum(0, 2).expect("the first of two");
        });
    }

    /// A wait that nobody else joins gives the deadline back rather than
    /// blocking forever, and the elapsed time shows it waited.
    #[test]
    fn a_wait_alone_times_out_and_reports_it() {
        let scratch = Scratch::new("timeout");
        let (barrier, beats) = scratch.build(SUBETHA_BARRIER_GRACE_DEFAULT);
        beats.register(std::process::id() + 1).expect("a second slot nobody beats for");

        let began = Instant::now();
        let code = subetha_epoch_barrier_wait_timeout_inner(&barrier, 0, 60);
        assert_eq!(code, crate::error::SUBETHA_E_TIMEOUT);
        assert!(began.elapsed() >= Duration::from_millis(50), "it waited for the deadline");
    }

    /// The entry point's body, so the timeout test exercises the mapping
    /// from `BarrierError` to a code without going through a handle.
    fn subetha_epoch_barrier_wait_timeout_inner(b: &EpochBarrierObject, epoch: u32, ms: u64) -> i32 {
        match b.barrier.wait_timeout(epoch, Duration::from_millis(ms)) {
            Ok(()) => SUBETHA_OK,
            Err(e) => barrier_code(e),
        }
    }

    /// The header carries the default as a literal, so this is what keeps
    /// it equal to the primitive's own constant.
    #[test]
    fn the_default_grace_matches_the_primitive() {
        assert_eq!(
            SUBETHA_BARRIER_GRACE_DEFAULT,
            subetha_cxc::epoch_barrier::DEFAULT_BARRIER_GRACE_EPOCHS,
        );
    }

    /// A snapshot reports the epoch, the arrivals and the live peers it
    /// was built to count.
    #[test]
    fn a_snapshot_reports_the_epoch_and_its_peers() {
        let scratch = Scratch::new("stats");
        let (barrier, _beats) = scratch.build(7);
        let stats = barrier.stats();
        assert_eq!(stats.mode, SUBETHA_MODE_STRICT);
        assert_eq!(stats.epoch, 0, "a fresh barrier stands at epoch zero");
        assert_eq!(stats.arrived, 0);
        assert_eq!(stats.live_peers, 1, "the one slot that is beating");
        assert_eq!(stats.grace_epochs, 7, "the window it was built with");
    }
}
