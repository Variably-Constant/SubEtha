//! `SharedLeaderElection` - cross-process leader election with
//! lowest-live-PID semantics and heartbeat-driven failover.
//!
//! Each process can call [`SharedLeaderElection::try_claim_leadership`]
//! which atomically claims the leader role if:
//! 1. There is no current leader (PID == 0), OR
//! 2. The caller's PID is strictly lower than the current leader's
//!    (lower PIDs preempt higher; the lowest live PID always wins), OR
//! 3. The current leader's heartbeat has gone stale (last beat is
//!    more than `grace_epochs` behind the global epoch).
//!
//! Election term increments on each handover so processes can
//! observe leadership changes by polling the term.
//!
//! # Why lowest-live-PID
//!
//! It's the simplest deterministic election that always converges:
//! given a set of live processes, exactly one (the lowest PID)
//! deserves leadership. PIDs are unique within a host. No quorum
//! needed; no two-round Paxos; no Raft term advance vote, just a
//! CAS protocol that anyone reading the same MMF agrees on.
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | LeaderHeader (64B)          |
//! |   - magic                   |
//! |   - current_leader_pid: u32 |
//! |   - election_term: u32      |
//! |   - leader_heartbeat: u64   |  (global epoch at last beat)
//! |   - global_epoch: u64       |  (monotonic; bumped by leader's scan)
//! +-----------------------------+
//! ```

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const LEADER_MAGIC: u64 = 0x4150_4D46_4C44_5253;

pub const DEFAULT_GRACE_EPOCHS: u64 = 3;

/// Reserved PID value meaning "no leader claimed".
pub const NO_LEADER: u32 = 0;

#[repr(C, align(64))]
pub struct LeaderHeader {
    pub magic: u64,
    pub current_leader_pid: AtomicU32,
    pub election_term: AtomicU32,
    pub leader_heartbeat: AtomicU64,
    pub global_epoch: AtomicU64,
    _pad: [u8; 32],
}

pub const LEADER_FILE_SIZE: usize = std::mem::size_of::<LeaderHeader>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderError {
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for LeaderError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedLeaderElection {
    _file: File,
    mmap: MmapMut,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedLeaderElection {}
unsafe impl Sync for SharedLeaderElection {}

impl subetha_sidecar::AdaptiveInstance for SharedLeaderElection {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedLeaderElection {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, LeaderError> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(LEADER_FILE_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(LEADER_FILE_SIZE).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut LeaderHeader;
        unsafe {
            std::ptr::write(hdr, LeaderHeader {
                magic: LEADER_MAGIC,
                current_leader_pid: AtomicU32::new(NO_LEADER),
                election_term: AtomicU32::new(0),
                leader_heartbeat: AtomicU64::new(0),
                global_epoch: AtomicU64::new(0),
                _pad: [0; 32],
            });
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, LeaderError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < LEADER_FILE_SIZE as u64 {
            return Err(LeaderError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(LEADER_FILE_SIZE).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const LeaderHeader) };
        if hdr.magic != LEADER_MAGIC {
            return Err(LeaderError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn header(&self) -> &LeaderHeader {
        unsafe { &*(self.mmap.as_ptr() as *const LeaderHeader) }
    }

    /// Attempt to claim leadership for `my_pid`. Returns `true` if
    /// successful (now leader) or already leader; `false` if the
    /// current leader is alive AND has a lower or equal PID.
    ///
    /// `grace_epochs` is the staleness window: when the current
    /// leader's heartbeat is more than this many epochs behind the
    /// global epoch, the leader is presumed dead and any process
    /// can claim.
    pub fn try_claim_leadership(&self, my_pid: u32, grace_epochs: u64) -> bool {
        assert!(my_pid != NO_LEADER, "PID 0 is reserved for NO_LEADER sentinel");
        let header = self.header();
        loop {
            let cur_pid = header.current_leader_pid.load(Ordering::Acquire);
            let can_claim = if cur_pid == NO_LEADER {
                true
            } else if cur_pid == my_pid {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_CLAIM, 0);
                return true;  // already leader
            } else if my_pid < cur_pid {
                true  // lower PID preempts
            } else {
                // Higher PID: only claim if leader is stale.
                let beat = header.leader_heartbeat.load(Ordering::Acquire);
                let global = header.global_epoch.load(Ordering::Acquire);
                global.saturating_sub(beat) > grace_epochs
            };
            if !can_claim {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_CLAIM, 1);
                return false;
            }
            if header.current_leader_pid.compare_exchange(
                cur_pid, my_pid, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                header.election_term.fetch_add(1, Ordering::AcqRel);
                let global = header.global_epoch.load(Ordering::Acquire);
                header.leader_heartbeat.store(global, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_CLAIM, 0);
                return true;
            }
            std::hint::spin_loop();
        }
    }

    /// Heartbeat as the current leader. Updates the heartbeat to
    /// the current global epoch. Returns `true` if the caller is
    /// still leader (so the heartbeat counts), `false` if another
    /// process has taken over (caller is no longer leader).
    pub fn beat_as_leader(&self, my_pid: u32) -> bool {
        let header = self.header();
        if header.current_leader_pid.load(Ordering::Acquire) != my_pid {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ownership::OP_BEAT, 1);
            return false;
        }
        let global = header.global_epoch.load(Ordering::Acquire);
        header.leader_heartbeat.store(global, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ownership::OP_BEAT, 0);
        true
    }

    /// Advance the global epoch by 1 and return the new value.
    /// Typically called by the leader once per scan tick.
    pub fn tick_epoch(&self) -> u64 {
        self.header().global_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Current global epoch value.
    pub fn global_epoch(&self) -> u64 {
        self.header().global_epoch.load(Ordering::Acquire)
    }

    /// Current leader PID, or `None` when there is no leader.
    pub fn current_leader(&self) -> Option<u32> {
        let pid = self.header().current_leader_pid.load(Ordering::Acquire);
        if pid == NO_LEADER { None } else { Some(pid) }
    }

    /// Convenience: is `my_pid` the current leader?
    pub fn am_i_leader(&self, my_pid: u32) -> bool {
        self.header().current_leader_pid.load(Ordering::Acquire) == my_pid
    }

    /// Current election term. Increments on each leadership change.
    /// Subscribers can poll this to detect handovers.
    pub fn election_term(&self) -> u32 {
        self.header().election_term.load(Ordering::Acquire)
    }

    /// Voluntarily release leadership. Returns `true` if the caller
    /// was the leader at the moment of release.
    pub fn step_down(&self, my_pid: u32) -> bool {
        let ok = self.header().current_leader_pid
            .compare_exchange(my_pid, NO_LEADER, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ownership::OP_RELEASE,
            if ok { 0 } else { 1 },
        );
        ok
    }

    pub fn flush(&self) -> Result<(), LeaderError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), LeaderError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-leader-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn empty_election_first_claimer_wins() {
        let p = tmp("empty");
        let e = SharedLeaderElection::create(&p).unwrap();
        assert_eq!(e.current_leader(), None);
        assert!(e.try_claim_leadership(42, 3));
        assert_eq!(e.current_leader(), Some(42));
        assert!(e.am_i_leader(42));
        assert!(!e.am_i_leader(99));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lower_pid_preempts_higher() {
        let p = tmp("preempt");
        let e = SharedLeaderElection::create(&p).unwrap();
        assert!(e.try_claim_leadership(500, 3));
        assert!(e.am_i_leader(500));
        // Lower PID claims; preempts.
        assert!(e.try_claim_leadership(100, 3));
        assert_eq!(e.current_leader(), Some(100));
        // Higher PID cannot reclaim while 100 is alive (in the same epoch).
        assert!(!e.try_claim_leadership(500, 3));
        assert_eq!(e.current_leader(), Some(100));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn equal_pid_returns_true_idempotent() {
        let p = tmp("equal-pid");
        let e = SharedLeaderElection::create(&p).unwrap();
        assert!(e.try_claim_leadership(42, 3));
        // Same PID claiming again is a no-op success.
        assert!(e.try_claim_leadership(42, 3));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn stale_leader_replaced_after_grace_window() {
        let p = tmp("stale");
        let e = SharedLeaderElection::create(&p).unwrap();
        assert!(e.try_claim_leadership(100, 1));
        // Tick global epoch beyond grace without beating.
        e.tick_epoch();
        e.tick_epoch();
        // Higher PID can now claim because heartbeat is stale.
        assert!(e.try_claim_leadership(500, 1));
        assert_eq!(e.current_leader(), Some(500));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn beat_keeps_leader_alive() {
        let p = tmp("beat");
        let e = SharedLeaderElection::create(&p).unwrap();
        assert!(e.try_claim_leadership(100, 1));
        e.tick_epoch();
        assert!(e.beat_as_leader(100));
        e.tick_epoch();
        assert!(e.beat_as_leader(100));
        // Higher PID still can't preempt (heartbeat fresh).
        assert!(!e.try_claim_leadership(500, 1));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn election_term_increments_on_each_handover() {
        let p = tmp("term");
        let e = SharedLeaderElection::create(&p).unwrap();
        let t0 = e.election_term();
        assert!(e.try_claim_leadership(500, 3));
        let t1 = e.election_term();
        assert_eq!(t1, t0 + 1);
        // Preemption by lower PID is a handover; term advances.
        assert!(e.try_claim_leadership(100, 3));
        let t2 = e.election_term();
        assert_eq!(t2, t1 + 1);
        // Same-PID re-claim is a no-op; term stays.
        assert!(e.try_claim_leadership(100, 3));
        assert_eq!(e.election_term(), t2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn step_down_clears_leadership() {
        let p = tmp("step-down");
        let e = SharedLeaderElection::create(&p).unwrap();
        e.try_claim_leadership(42, 3);
        assert!(e.step_down(42));
        assert_eq!(e.current_leader(), None);
        // Non-leader step_down is a no-op.
        assert!(!e.step_down(99));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let e_a = SharedLeaderElection::create(&p).unwrap();
        let e_b = SharedLeaderElection::open(&p).unwrap();
        assert!(e_a.try_claim_leadership(100, 3));
        assert_eq!(e_b.current_leader(), Some(100));
        assert!(e_b.am_i_leader(100));
        assert!(!e_b.am_i_leader(200));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn beat_returns_false_after_preemption() {
        let p = tmp("beat-after-preempt");
        let e = SharedLeaderElection::create(&p).unwrap();
        e.try_claim_leadership(500, 3);
        // Lower PID preempts.
        e.try_claim_leadership(100, 3);
        // 500's beat now returns false.
        assert!(!e.beat_as_leader(500));
        assert!(e.beat_as_leader(100));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let e = SharedLeaderElection::create(&p).unwrap();
            e.try_claim_leadership(42, 3);
            e.flush().unwrap();
        }
        let e2 = SharedLeaderElection::open(&p).unwrap();
        assert_eq!(e2.current_leader(), Some(42));
        std::fs::remove_file(&p).ok();
    }
}
