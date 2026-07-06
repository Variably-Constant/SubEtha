//! `FailoverWatchdog` - scans the heartbeat table and reclaims
//! in-flight work whose owning process has stopped beating.
//!
//! Architectural contract: failover happens within ONE epoch of the
//! peer's last beat. The watchdog advances the global epoch on each
//! scan; any process whose `last_seen_epoch < global - grace_epochs`
//! is presumed dead and its `in_flight_bitmap` is returned to the
//! caller as a `ReclaimReport` so the caller (typically the
//! scheduler) can reassign the work.

use crate::heartbeat::{HeartbeatSnapshot, HeartbeatTable, IN_FLIGHT_SLOTS};

/// Default grace window. A slot must miss more than this many epochs
/// before it is reclaimed.
pub const DEFAULT_GRACE_EPOCHS: u64 = 1;

/// Report from one watchdog scan.
#[derive(Debug, Clone)]
pub struct ReclaimReport {
    /// Slot index -> last snapshot of a dead process whose
    /// in-flight bits should be reclaimed.
    pub dead_slots: Vec<(usize, HeartbeatSnapshot)>,
    /// New global epoch after the scan.
    pub new_global_epoch: u64,
}

impl ReclaimReport {
    pub fn is_empty(&self) -> bool { self.dead_slots.is_empty() }
}

/// Watchdog scanner; one per cooperating cluster.
pub struct FailoverWatchdog<'a> {
    pub table: &'a HeartbeatTable,
    pub grace_epochs: u64,
}

impl<'a> FailoverWatchdog<'a> {
    pub fn new(table: &'a HeartbeatTable) -> Self {
        Self { table, grace_epochs: DEFAULT_GRACE_EPOCHS }
    }

    pub fn with_grace(table: &'a HeartbeatTable, grace_epochs: u64) -> Self {
        Self { table, grace_epochs }
    }

    /// Advance global epoch and scan every slot. Returns a report
    /// of slots whose last beat is more than `grace_epochs` behind
    /// the new global epoch.
    ///
    /// Per-scan observation is pushed to the underlying
    /// HeartbeatTable's sidecar ring rather than a separate
    /// watchdog-owned ring: the watchdog borrows the table with
    /// lifetime `'a`, which is incompatible with the
    /// `AdaptiveInstance: 'static` trait bound. Routing the scan
    /// observation through the table preserves visibility to any
    /// policy attached at that level.
    pub fn scan(&self) -> ReclaimReport {
        let new_epoch = self.table.tick_global_epoch();
        let mut dead = Vec::new();
        for i in 0..self.table.capacity() {
            if let Some(snap) = self.table.snapshot(i) {
                let lag = new_epoch.saturating_sub(snap.last_seen_epoch);
                if lag > self.grace_epochs && snap.in_flight_bitmap != 0 {
                    dead.push((i, snap));
                }
            }
        }
        let dead_count = dead.len();
        <HeartbeatTable as subetha_sidecar::AdaptiveInstance>::ring(self.table).push(
            subetha_core::Observation {
                op_kind: crate::sidecar_ops::liveness::OP_SCAN,
                flags: if dead_count > 0 { 1 } else { 0 },  // 1 = reclaim required
                ..subetha_core::Observation::ZERO
            },
        );
        ReclaimReport { dead_slots: dead, new_global_epoch: new_epoch }
    }

    /// Iterate the set bits in `bitmap`, returning each bit's
    /// position. Used by callers reclaiming an in_flight_bitmap.
    pub fn iter_in_flight_bits(bitmap: u64) -> impl Iterator<Item = u8> {
        (0u8..IN_FLIGHT_SLOTS as u8).filter(move |b| (bitmap >> b) & 1 == 1)
    }

    /// Clear the dead process's bitmap so subsequent scans don't
    /// re-report it. Typically called by the caller after they have
    /// reassigned the work.
    pub fn clear_dead_bitmap(&self, slot_idx: usize) {
        let slot = self.table_slot(slot_idx);
        slot.in_flight_bitmap.store(0, std::sync::atomic::Ordering::Release);
    }

    fn table_slot(&self, idx: usize) -> &crate::heartbeat::HeartbeatSlot {
        // Re-derive via the public snapshot path is awkward; reach
        // into the table directly. (HeartbeatTable's private slot
        // accessor is accessed via this crate-private helper.)
        crate::heartbeat::__slot_for_watchdog(self.table, idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::HeartbeatTable;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-failover-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn watchdog_reports_no_dead_when_all_beat() {
        let p = tmp_path("all-alive");
        let t = HeartbeatTable::create(&p, 4).unwrap();
        let s0 = t.register(1).unwrap();
        let s1 = t.register(2).unwrap();
        t.mark_in_flight(s0, 0);
        t.mark_in_flight(s1, 1);

        let w = FailoverWatchdog::new(&t);
        // Beat both and then scan - should not be dead.
        t.beat(s0); t.beat(s1);
        let r = w.scan();
        assert!(r.is_empty(),
                "no dead processes expected; got {} dead", r.dead_slots.len());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn watchdog_reports_dead_when_grace_exceeded() {
        let p = tmp_path("dead-one");
        let t = HeartbeatTable::create(&p, 4).unwrap();
        let s_alive = t.register(1).unwrap();
        let s_dead = t.register(2).unwrap();
        t.mark_in_flight(s_alive, 0);
        t.mark_in_flight(s_dead, 1);
        t.beat(s_alive); t.beat(s_dead);

        let w = FailoverWatchdog::with_grace(&t, 2);
        // Scan: global=1, both slots lag=1, grace=2 -> not dead.
        let r1 = w.scan();
        assert!(r1.is_empty(), "first scan within grace; got {:?}", r1.dead_slots);
        // Only alive beats.
        t.beat(s_alive);
        // Scan: global=2, alive lag=1, dead lag=2 == grace -> not dead.
        let r2 = w.scan();
        assert!(r2.is_empty(), "second scan equal to grace; got {:?}", r2.dead_slots);
        // Only alive beats again.
        t.beat(s_alive);
        // Scan: global=3, alive lag=1, dead lag=3 > grace=2 -> dead.
        let r3 = w.scan();
        assert_eq!(r3.dead_slots.len(), 1);
        let (idx, snap) = &r3.dead_slots[0];
        assert_eq!(*idx, s_dead);
        assert_eq!(snap.pid, 2);
        assert_eq!(snap.in_flight_bitmap, 1u64 << 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn iter_in_flight_bits_walks_set_positions() {
        let bm = (1u64 << 0) | (1u64 << 3) | (1u64 << 5) | (1u64 << 63);
        let bits: Vec<u8> = FailoverWatchdog::iter_in_flight_bits(bm).collect();
        assert_eq!(bits, vec![0, 3, 5, 63]);
    }

    #[test]
    fn clear_dead_bitmap_silences_reports() {
        let p = tmp_path("clear-dead");
        let t = HeartbeatTable::create(&p, 1).unwrap();
        let s = t.register(7).unwrap();
        t.mark_in_flight(s, 4);
        t.beat(s);
        let w = FailoverWatchdog::with_grace(&t, 0);
        // grace=0 + tick=1 -> lag 1 > 0 -> reported as dead.
        let r1 = w.scan();
        assert_eq!(r1.dead_slots.len(), 1);
        // Clear and rescan.
        w.clear_dead_bitmap(s);
        let r2 = w.scan();
        assert!(r2.is_empty(), "after clear, no dead slots reported");
        std::fs::remove_file(&p).ok();
    }
}
