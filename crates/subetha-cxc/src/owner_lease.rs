//! `OwnerLease<T>` - cross-process Mutex with auto-failover.
//!
//! Composite primitive: combines a SeqLock-protected payload cell
//! with the lowest-live-PID + heartbeat-based ownership tracking
//! pattern. Provides Mutex-like exclusive access to a shared value
//! T, with the additional guarantee that if the current owner dies,
//! another process can claim ownership within `grace_epochs` and
//! continue.
//!
//! # API shape
//!
//! - `try_acquire(my_pid, grace_epochs)` -> bool: CAS-claim ownership
//! - `release(my_pid)` -> bool: voluntarily release
//! - `with_lease(my_pid, grace_epochs, |&mut T| ...)` -> closure-based RAII pattern
//! - `read_as_owner(my_pid)` -> `Option<T>`: read the value, owner only
//! - `write_as_owner(my_pid, T)` -> bool: write the value, owner only
//! - `beat(my_pid)` -> bool: refresh the heartbeat, returns false if no longer owner
//! - `tick_epoch()`: advance the global epoch (caller responsible for periodic ticks)
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | LeaseHeader (64B)           |
//! |   - magic                   |
//! |   - payload_size            |
//! |   - seq_version: AtomicU32  | (for payload SeqLock)
//! |   - owner_pid: AtomicU32    |
//! |   - lease_term: AtomicU32   |
//! |   - heartbeat: AtomicU64    |
//! |   - global_epoch: AtomicU64 |
//! +-----------------------------+
//! | payload [u8; PAYLOAD_BYTES] | (one cache line, 48 bytes usable)
//! +-----------------------------+
//! ```

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const LEASE_MAGIC: u64 = 0x4150_4D46_4F57_4C53;

pub const PAYLOAD_BYTES: usize = 48;

pub const NO_OWNER: u32 = 0;

#[repr(C, align(64))]
pub struct LeaseHeader {
    pub magic: u64,
    pub payload_size: u32,
    pub seq_version: AtomicU32,
    pub owner_pid: AtomicU32,
    pub lease_term: AtomicU32,
    pub heartbeat_epoch: AtomicU64,
    pub global_epoch: AtomicU64,
    _pad: [u8; 24],
}

#[repr(C, align(64))]
pub struct LeasePayload {
    pub bytes: [u8; PAYLOAD_BYTES],
    _pad: [u8; 16],
}

pub const LEASE_FILE_SIZE: usize = size_of::<LeaseHeader>() + size_of::<LeasePayload>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError {
    LayoutMismatch,
    PayloadTooLarge,
    NotOwner,
    Contention,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for LeaseError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct OwnerLease<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for OwnerLease<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for OwnerLease<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for OwnerLease<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> OwnerLease<T> {
    pub fn create(path: impl AsRef<Path>, initial: T) -> Result<Self, LeaseError> {
        Self::check_layout()?;
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(LEASE_FILE_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(LEASE_FILE_SIZE).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut LeaseHeader;
        unsafe {
            std::ptr::write(hdr, LeaseHeader {
                magic: LEASE_MAGIC,
                payload_size: size_of::<T>() as u32,
                seq_version: AtomicU32::new(0),
                owner_pid: AtomicU32::new(NO_OWNER),
                lease_term: AtomicU32::new(0),
                heartbeat_epoch: AtomicU64::new(0),
                global_epoch: AtomicU64::new(0),
                _pad: [0; 24],
            });
        }
        let payload_ptr = unsafe { mmap.as_mut_ptr().add(size_of::<LeaseHeader>()) as *mut LeasePayload };
        unsafe {
            std::ptr::write(payload_ptr, LeasePayload { bytes: [0; PAYLOAD_BYTES], _pad: [0; 16] });
            let dst = (*payload_ptr).bytes.as_mut_ptr() as *mut T;
            std::ptr::write_unaligned(dst, initial);
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, LeaseError> {
        Self::check_layout()?;
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < LEASE_FILE_SIZE as u64 {
            return Err(LeaseError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(LEASE_FILE_SIZE).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const LeaseHeader) };
        if hdr.magic != LEASE_MAGIC || hdr.payload_size as usize != size_of::<T>() {
            return Err(LeaseError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn check_layout() -> Result<(), LeaseError> {
        if size_of::<T>() > PAYLOAD_BYTES {
            return Err(LeaseError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(LeaseError::PayloadTooLarge);
        }
        Ok(())
    }

    fn header(&self) -> &LeaseHeader {
        unsafe { &*(self.mmap.as_ptr() as *const LeaseHeader) }
    }

    fn payload_ptr(&self) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(size_of::<LeaseHeader>()) as *mut u8 }
    }

    /// Try to claim ownership. Succeeds when (a) no current owner,
    /// (b) my_pid < current owner's PID (preemption), or
    /// (c) current owner's heartbeat is more than grace_epochs stale.
    pub fn try_acquire(&self, my_pid: u32, grace_epochs: u64) -> bool {
        assert!(my_pid != NO_OWNER, "PID 0 reserved for NO_OWNER");
        let header = self.header();
        loop {
            let cur = header.owner_pid.load(Ordering::Acquire);
            let can_claim = if cur == NO_OWNER {
                true
            } else if cur == my_pid {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_ACQUIRE, 0);
                return true;
            } else if my_pid < cur {
                true
            } else {
                let beat = header.heartbeat_epoch.load(Ordering::Acquire);
                let global = header.global_epoch.load(Ordering::Acquire);
                global.saturating_sub(beat) > grace_epochs
            };
            if !can_claim {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_ACQUIRE, 1);
                return false;
            }
            if header.owner_pid.compare_exchange(
                cur, my_pid, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                header.lease_term.fetch_add(1, Ordering::AcqRel);
                let global = header.global_epoch.load(Ordering::Acquire);
                header.heartbeat_epoch.store(global, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::ownership::OP_ACQUIRE, 0);
                return true;
            }
            std::hint::spin_loop();
        }
    }

    pub fn release(&self, my_pid: u32) -> bool {
        let ok = self.header().owner_pid
            .compare_exchange(my_pid, NO_OWNER, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::ownership::OP_RELEASE,
            if ok { 0 } else { 1 },
        );
        ok
    }

    /// Closure-based lease scope. Tries to acquire; if successful,
    /// runs `f(&mut T)` with the payload, then releases. Returns
    /// `None` if acquisition failed.
    pub fn with_lease<R, F: FnOnce(&mut T) -> R>(
        &self,
        my_pid: u32,
        grace_epochs: u64,
        f: F,
    ) -> Option<R> {
        if !self.try_acquire(my_pid, grace_epochs) { return None; }
        let result = {
            // Read current payload, hand to closure, then write back.
            let header = self.header();
            let mut value: T = unsafe {
                let src = self.payload_ptr() as *const T;
                std::ptr::read_unaligned(src)
            };
            let r = f(&mut value);
            // Publish updated payload via SeqLock so external readers
            // (when added) see consistent state.
            header.seq_version.fetch_add(1, Ordering::AcqRel);
            unsafe {
                let dst = self.payload_ptr() as *mut T;
                std::ptr::write_unaligned(dst, value);
            }
            header.seq_version.fetch_add(1, Ordering::Release);
            r
        };
        self.release(my_pid);
        Some(result)
    }

    /// Read the payload only when the caller holds the lease.
    pub fn read_as_owner(&self, my_pid: u32) -> Option<T> {
        if !self.am_i_owner(my_pid) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ownership::OP_GET, 1);
            return None;
        }
        let value: T = unsafe {
            let src = self.payload_ptr() as *const T;
            std::ptr::read_unaligned(src)
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ownership::OP_GET, 0);
        Some(value)
    }

    /// Write the payload only when the caller holds the lease.
    pub fn write_as_owner(&self, my_pid: u32, value: T) -> bool {
        if !self.am_i_owner(my_pid) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ownership::OP_GET, 1);
            return false;
        }
        let header = self.header();
        header.seq_version.fetch_add(1, Ordering::AcqRel);
        unsafe {
            let dst = self.payload_ptr() as *mut T;
            std::ptr::write_unaligned(dst, value);
        }
        header.seq_version.fetch_add(1, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ownership::OP_GET, 0);
        true
    }

    /// Refresh the heartbeat. Returns `false` if the caller has
    /// been preempted (no longer owner).
    pub fn beat(&self, my_pid: u32) -> bool {
        let header = self.header();
        if header.owner_pid.load(Ordering::Acquire) != my_pid {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::ownership::OP_BEAT, 1);
            return false;
        }
        let global = header.global_epoch.load(Ordering::Acquire);
        header.heartbeat_epoch.store(global, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::ownership::OP_BEAT, 0);
        true
    }

    pub fn tick_epoch(&self) -> u64 {
        self.header().global_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn current_owner(&self) -> Option<u32> {
        let pid = self.header().owner_pid.load(Ordering::Acquire);
        if pid == NO_OWNER { None } else { Some(pid) }
    }

    pub fn am_i_owner(&self, my_pid: u32) -> bool {
        self.header().owner_pid.load(Ordering::Acquire) == my_pid
    }

    pub fn lease_term(&self) -> u32 {
        self.header().lease_term.load(Ordering::Acquire)
    }

    pub fn flush(&self) -> Result<(), LeaseError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), LeaseError> {
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
        p.push(format!("subetha-lease-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn acquire_release_round_trip() {
        let p = tmp("rt");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 42).unwrap();
        assert_eq!(l.current_owner(), None);
        assert!(l.try_acquire(100, 3));
        assert_eq!(l.current_owner(), Some(100));
        assert!(l.am_i_owner(100));
        assert!(l.release(100));
        assert_eq!(l.current_owner(), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lower_pid_preempts_higher() {
        let p = tmp("preempt");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 0).unwrap();
        assert!(l.try_acquire(500, 3));
        assert!(l.try_acquire(100, 3));
        assert_eq!(l.current_owner(), Some(100));
        assert!(!l.try_acquire(500, 3));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn read_and_write_require_ownership() {
        let p = tmp("read-write");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 100).unwrap();
        // No owner; reads/writes fail.
        assert_eq!(l.read_as_owner(50), None);
        assert!(!l.write_as_owner(50, 999));
        // Acquire.
        assert!(l.try_acquire(50, 3));
        assert_eq!(l.read_as_owner(50), Some(100));
        assert!(l.write_as_owner(50, 999));
        assert_eq!(l.read_as_owner(50), Some(999));
        // Non-owner reads fail even after another's acquire.
        assert_eq!(l.read_as_owner(999), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn with_lease_acquires_runs_releases() {
        let p = tmp("with-lease");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 10).unwrap();
        let r = l.with_lease(100, 3, |v| {
            *v += 5;
            *v
        });
        assert_eq!(r, Some(15));
        assert_eq!(l.current_owner(), None, "with_lease releases on return");
        // Verify persistence.
        assert!(l.try_acquire(100, 3));
        assert_eq!(l.read_as_owner(100), Some(15));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn stale_owner_failover_within_grace() {
        let p = tmp("failover");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 0).unwrap();
        assert!(l.try_acquire(100, 1));
        // Tick beyond grace without 100 beating.
        l.tick_epoch();
        l.tick_epoch();
        // Higher PID 500 can now preempt because 100's heartbeat is stale.
        assert!(l.try_acquire(500, 1));
        assert_eq!(l.current_owner(), Some(500));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn beat_keeps_owner_alive() {
        let p = tmp("beat");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 0).unwrap();
        assert!(l.try_acquire(100, 1));
        l.tick_epoch();
        assert!(l.beat(100));
        // 500 cannot preempt while 100 beats.
        assert!(!l.try_acquire(500, 1));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lease_term_advances_on_handover() {
        let p = tmp("term");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 0).unwrap();
        let t0 = l.lease_term();
        l.try_acquire(500, 3);
        let t1 = l.lease_term();
        assert_eq!(t1, t0 + 1);
        l.try_acquire(100, 3);  // preempt
        let t2 = l.lease_term();
        assert_eq!(t2, t1 + 1);
        l.try_acquire(100, 3);  // idempotent
        assert_eq!(l.lease_term(), t2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_lease_visibility() {
        let p = tmp("cross-handle");
        let a: OwnerLease<u64> = OwnerLease::create(&p, 7).unwrap();
        let b: OwnerLease<u64> = OwnerLease::open(&p).unwrap();
        assert!(a.try_acquire(100, 3));
        assert_eq!(b.current_owner(), Some(100));
        assert!(b.am_i_owner(100));
        assert!(!b.am_i_owner(200));
        // Process B (who isn't owner) can't write.
        assert!(!b.write_as_owner(200, 99));
        // Process A writes; B sees via own acquire later.
        a.write_as_owner(100, 88);
        a.release(100);
        assert!(b.try_acquire(200, 3));
        assert_eq!(b.read_as_owner(200), Some(88));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let l: OwnerLease<u64> = OwnerLease::create(&p, 1234).unwrap();
            l.try_acquire(42, 3);
            l.write_as_owner(42, 5678);
            l.flush().unwrap();
        }
        let l2: OwnerLease<u64> = OwnerLease::open(&p).unwrap();
        assert_eq!(l2.current_owner(), Some(42));
        assert_eq!(l2.read_as_owner(42), Some(5678));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn beat_after_preemption_returns_false() {
        let p = tmp("beat-preempt");
        let l: OwnerLease<u64> = OwnerLease::create(&p, 0).unwrap();
        l.try_acquire(500, 3);
        l.try_acquire(100, 3);  // 100 preempts
        assert!(!l.beat(500));
        assert!(l.beat(100));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct State { active: u32, count: u32, score: f64 }
        let p = tmp("struct");
        let l: OwnerLease<State> = OwnerLease::create(&p,
            State { active: 0, count: 0, score: 0.0 }).unwrap();
        l.with_lease(7, 3, |s| {
            s.active = 1;
            s.count = 42;
            s.score = 2.5;
        });
        // Re-acquire to read.
        l.try_acquire(7, 3);
        let s = l.read_as_owner(7).unwrap();
        assert_eq!(s, State { active: 1, count: 42, score: 2.5 });
        std::fs::remove_file(&p).ok();
    }
}
