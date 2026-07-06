//! Per-process heartbeat slots stored in an MMF.
//!
//! Each participating process owns one [`HeartbeatSlot`] in the
//! shared table. On each scan tick, the process advances its slot's
//! `last_seen_epoch`. A watchdog (separate module) compares the
//! slot's epoch against a global `epoch` counter; if the process
//! hasn't advanced its heartbeat within the configured grace, its
//! work is presumed dead and reclaimed.
//!
//! Layout:
//! ```text
//! +-----------------------------+
//! | HeartbeatHeader (64B)       |
//! |   - magic, capacity, epoch  |
//! +-----------------------------+
//! | HeartbeatSlot[0]  (64B)     |
//! | HeartbeatSlot[1]  (64B)     |
//! | ...                         |
//! | HeartbeatSlot[N - 1]        |
//! +-----------------------------+
//! ```
//!
//! Each slot is one cache line so cross-process writes to different
//! slots never false-share.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const HEARTBEAT_MAGIC: u64 = 0x4150_4D46_4842_4154;

/// Unused slot ID (no pid).
pub const EMPTY_PID: u32 = 0;

/// Maximum number of in-flight work items a slot tracks. Each bit in
/// `in_flight_bitmap` represents one work unit; on failover those
/// bits are reclaimable.
pub const IN_FLIGHT_SLOTS: usize = 64;

#[repr(C, align(64))]
pub struct HeartbeatHeader {
    pub magic: u64,
    pub capacity: u64,
    /// Global epoch counter; the watchdog advances this each scan.
    pub epoch: AtomicU64,
    _reserved: [u8; 40],
}

#[repr(C, align(64))]
pub struct HeartbeatSlot {
    /// Owning process id. 0 = vacant.
    pub pid: AtomicU32,
    /// Sequence-lock generation; bumped on each meaningful write so
    /// readers can detect torn writes (read with seqlock retry).
    pub seq_version: AtomicU32,
    /// Last global epoch at which this process incremented its
    /// heartbeat. Watchdog reclaims when `global.epoch -
    /// last_seen_epoch > grace_epochs`.
    pub last_seen_epoch: AtomicU64,
    /// Bitmap of work units currently assigned to this process.
    /// Watchdog reclaims set bits on failover.
    pub in_flight_bitmap: AtomicU64,
    /// Process role: 0 = worker, 1 = coordinator.
    pub role: AtomicU32,
    _pad: [u8; 36],
}

/// Total file size for a heartbeat table with `capacity` slots.
pub const fn heartbeat_file_size(capacity: usize) -> usize {
    std::mem::size_of::<HeartbeatHeader>() + capacity * std::mem::size_of::<HeartbeatSlot>()
}

/// Cross-process heartbeat registry. Each process opens this and
/// reserves one slot via [`HeartbeatTable::register`].
pub struct HeartbeatTable {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for HeartbeatTable {}
unsafe impl Sync for HeartbeatTable {}

impl subetha_sidecar::AdaptiveInstance for HeartbeatTable {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatError {
    LayoutMismatch,
    TableFull,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for HeartbeatError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

impl HeartbeatTable {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, HeartbeatError> {
        assert!(capacity >= 1);
        let total = heartbeat_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr_ptr = mmap.as_mut_ptr() as *mut HeartbeatHeader;
        unsafe {
            std::ptr::write(hdr_ptr, HeartbeatHeader {
                magic: HEARTBEAT_MAGIC,
                capacity: capacity as u64,
                epoch: AtomicU64::new(0),
                _reserved: [0; 40],
            });
        }
        let slots_base = unsafe {
            mmap.as_mut_ptr().add(std::mem::size_of::<HeartbeatHeader>())
        };
        for i in 0..capacity {
            let slot_ptr = unsafe {
                slots_base.add(i * std::mem::size_of::<HeartbeatSlot>()) as *mut HeartbeatSlot
            };
            unsafe {
                std::ptr::write(slot_ptr, HeartbeatSlot {
                    pid: AtomicU32::new(EMPTY_PID),
                    seq_version: AtomicU32::new(0),
                    last_seen_epoch: AtomicU64::new(0),
                    in_flight_bitmap: AtomicU64::new(0),
                    role: AtomicU32::new(0),
                    _pad: [0; 36],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, HeartbeatError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = heartbeat_file_size(expected_capacity);
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let header = unsafe { &*(mmap.as_ptr() as *const HeartbeatHeader) };
        if header.magic != HEARTBEAT_MAGIC || header.capacity != expected_capacity as u64 {
            return Err(HeartbeatError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn capacity(&self) -> usize { self.capacity }

    pub fn header(&self) -> &HeartbeatHeader {
        unsafe { &*(self.mmap.as_ptr() as *const HeartbeatHeader) }
    }

    fn slot(&self, idx: usize) -> &HeartbeatSlot {
        let base = unsafe {
            self.mmap.as_ptr().add(std::mem::size_of::<HeartbeatHeader>())
        };
        unsafe {
            &*(base.add(idx * std::mem::size_of::<HeartbeatSlot>()) as *const HeartbeatSlot)
        }
    }

    /// Register the current process. Returns the slot index. CAS-claim
    /// of the first empty slot.
    pub fn register(&self, pid: u32) -> Result<usize, HeartbeatError> {
        for i in 0..self.capacity {
            let slot = self.slot(i);
            if slot.pid.compare_exchange(
                EMPTY_PID, pid, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                slot.seq_version.fetch_add(1, Ordering::Release);
                slot.last_seen_epoch.store(
                    self.header().epoch.load(Ordering::Acquire),
                    Ordering::Release,
                );
                slot.in_flight_bitmap.store(0, Ordering::Release);
                slot.role.store(0, Ordering::Release);
                slot.seq_version.fetch_add(1, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::liveness::OP_REGISTER, 0);
                return Ok(i);
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::liveness::OP_REGISTER, 1);
        Err(HeartbeatError::TableFull)
    }

    /// Release the slot at `idx`. Call before process exit.
    pub fn unregister(&self, idx: usize) {
        let slot = self.slot(idx);
        slot.seq_version.fetch_add(1, Ordering::Release);
        slot.in_flight_bitmap.store(0, Ordering::Release);
        slot.pid.store(EMPTY_PID, Ordering::Release);
        slot.seq_version.fetch_add(1, Ordering::Release);
    }

    /// Heartbeat: advance this slot's `last_seen_epoch` to match the
    /// global epoch. Call once per scan tick.
    pub fn beat(&self, idx: usize) {
        let global = self.header().epoch.load(Ordering::Acquire);
        let slot = self.slot(idx);
        slot.last_seen_epoch.store(global, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::liveness::OP_BEAT, 0);
    }

    /// Advance the global epoch. Watchdog calls this once per scan
    /// interval. Returns the new epoch value.
    pub fn tick_global_epoch(&self) -> u64 {
        let v = self.header().epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.ring_sidecar
            .push_op(crate::sidecar_ops::liveness::OP_TICK_EPOCH, 0);
        v
    }

    pub fn global_epoch(&self) -> u64 {
        self.header().epoch.load(Ordering::Acquire)
    }

    /// Mark a work unit as in-flight for `slot_idx`.
    pub fn mark_in_flight(&self, slot_idx: usize, bit: u8) {
        debug_assert!((bit as usize) < IN_FLIGHT_SLOTS);
        let slot = self.slot(slot_idx);
        slot.in_flight_bitmap.fetch_or(1u64 << bit, Ordering::AcqRel);
    }

    pub fn clear_in_flight(&self, slot_idx: usize, bit: u8) {
        debug_assert!((bit as usize) < IN_FLIGHT_SLOTS);
        let slot = self.slot(slot_idx);
        slot.in_flight_bitmap.fetch_and(!(1u64 << bit), Ordering::AcqRel);
    }

    /// Snapshot a slot via SeqLock retry. Returns `None` if the slot
    /// is vacant.
    pub fn snapshot(&self, idx: usize) -> Option<HeartbeatSnapshot> {
        let slot = self.slot(idx);
        loop {
            let v1 = slot.seq_version.load(Ordering::Acquire);
            if v1 & 1 != 0 { continue; }  // writer in progress
            let pid = slot.pid.load(Ordering::Acquire);
            let last = slot.last_seen_epoch.load(Ordering::Acquire);
            let inflight = slot.in_flight_bitmap.load(Ordering::Acquire);
            let role = slot.role.load(Ordering::Acquire);
            let v2 = slot.seq_version.load(Ordering::Acquire);
            if v1 == v2 {
                if pid == EMPTY_PID { return None; }
                return Some(HeartbeatSnapshot {
                    pid, last_seen_epoch: last,
                    in_flight_bitmap: inflight, role,
                });
            }
        }
    }
}

/// Snapshot of one slot's state. Cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatSnapshot {
    pub pid: u32,
    pub last_seen_epoch: u64,
    pub in_flight_bitmap: u64,
    pub role: u32,
}

/// Crate-internal accessor for the watchdog module. NOT pub-exported
/// from the crate (only re-exported intra-crate).
#[doc(hidden)]
pub fn __slot_for_watchdog(table: &HeartbeatTable, idx: usize) -> &HeartbeatSlot {
    table.slot(idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-hb-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn register_returns_slot_indices() {
        let p = tmp_path("register");
        let t = HeartbeatTable::create(&p, 4).unwrap();
        let s0 = t.register(1001).unwrap();
        let s1 = t.register(1002).unwrap();
        assert_ne!(s0, s1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn table_full_returns_error() {
        let p = tmp_path("table-full");
        let t = HeartbeatTable::create(&p, 2).unwrap();
        let _val = t.register(1).unwrap();
        let _val = t.register(2).unwrap();
        assert_eq!(t.register(3).unwrap_err(), HeartbeatError::TableFull);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn beat_advances_last_seen_epoch() {
        let p = tmp_path("beat");
        let t = HeartbeatTable::create(&p, 1).unwrap();
        let s = t.register(99).unwrap();
        for _ in 0..5 { t.tick_global_epoch(); }
        let snap_before = t.snapshot(s).unwrap();
        let global_after_tick = t.global_epoch();
        t.beat(s);
        let snap_after = t.snapshot(s).unwrap();
        assert!(snap_after.last_seen_epoch > snap_before.last_seen_epoch);
        assert_eq!(snap_after.last_seen_epoch, global_after_tick);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn unregister_frees_slot_for_reuse() {
        let p = tmp_path("unreg");
        let t = HeartbeatTable::create(&p, 2).unwrap();
        let s0 = t.register(11).unwrap();
        let _s1 = t.register(22).unwrap();
        t.unregister(s0);
        // Now there should be a free slot.
        let new = t.register(33).unwrap();
        assert_eq!(new, s0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn in_flight_bitmap_mark_and_clear() {
        let p = tmp_path("inflight");
        let t = HeartbeatTable::create(&p, 1).unwrap();
        let s = t.register(7).unwrap();
        t.mark_in_flight(s, 3);
        t.mark_in_flight(s, 5);
        let snap = t.snapshot(s).unwrap();
        assert_eq!(snap.in_flight_bitmap, (1u64 << 3) | (1u64 << 5));
        t.clear_in_flight(s, 3);
        let snap = t.snapshot(s).unwrap();
        assert_eq!(snap.in_flight_bitmap, 1u64 << 5);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn snapshot_via_seqlock_returns_consistent_data() {
        let p = tmp_path("snap");
        let t = HeartbeatTable::create(&p, 1).unwrap();
        let s = t.register(42).unwrap();
        t.tick_global_epoch();
        t.beat(s);
        let snap = t.snapshot(s).unwrap();
        assert_eq!(snap.pid, 42);
        assert!(snap.last_seen_epoch >= 1);
        std::fs::remove_file(&p).ok();
    }
}
