//! `FailoverWatchdog` - scans the heartbeat table and reclaims
//! in-flight work whose owning process has stopped beating.
//!
//! Architectural contract: failover happens within a single epoch of the
//! peer's last beat. The watchdog advances the global epoch on each
//! scan; any process whose `last_seen_epoch < global - grace_epochs`
//! is presumed dead and its `in_flight_bitmap` is returned to the
//! caller as a `ReclaimReport` so the caller (typically the
//! scheduler) can reassign the work.
//!
//! A watchdog also heals the rings handed to
//! [`watch_ring`](FailoverWatchdog::watch_ring). A slot a producer
//! claimed and never published, found at the same position on two
//! consecutive scans, is healed with [`SharedRing::heal_stuck_slot`]
//! once any registered process's last beat is more than
//! `grace_epochs` behind; a publish takes nanoseconds, so a live
//! producer is not mid-publish across a whole scan interval.

use std::sync::Arc;

use subetha_core::SwapCell;

use crate::heartbeat::{HeartbeatSnapshot, HeartbeatTable, IN_FLIGHT_SLOTS};
use crate::shared_ring::{RingError, SharedRing};

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
    /// Slots this scan healed on the watched rings: the ring's index
    /// from [`FailoverWatchdog::watch_ring`], then the slot's position.
    pub healed_slots: Vec<(usize, u64)>,
    /// Heals a watched ring refused: ring index, position, error. The
    /// position stays a suspect for the next scan.
    pub heal_errors: Vec<(usize, u64, RingError)>,
}

impl ReclaimReport {
    pub fn is_empty(&self) -> bool { self.dead_slots.is_empty() }
}

/// A ring the watchdog heals, with the stuck positions its last scan
/// found there.
struct WatchedRing<'a> {
    ring: &'a SharedRing,
    suspects: SwapCell<Vec<u64>>,
}

/// Watchdog scanner; one per cooperating cluster.
pub struct FailoverWatchdog<'a> {
    pub table: &'a HeartbeatTable,
    pub grace_epochs: u64,
    watched: Vec<WatchedRing<'a>>,
}

impl<'a> FailoverWatchdog<'a> {
    pub fn new(table: &'a HeartbeatTable) -> Self {
        Self::with_grace(table, DEFAULT_GRACE_EPOCHS)
    }

    pub fn with_grace(table: &'a HeartbeatTable, grace_epochs: u64) -> Self {
        Self { table, grace_epochs, watched: Vec::new() }
    }

    /// Heal `ring`'s stuck slots on later scans. Returns the index the
    /// report's `healed_slots` and `heal_errors` name the ring by.
    pub fn watch_ring(&mut self, ring: &'a SharedRing) -> usize {
        self.watched.push(WatchedRing { ring, suspects: SwapCell::new(Vec::new()) });
        self.watched.len() - 1
    }

    /// Advance global epoch and scan every slot. Returns a report
    /// of slots whose last beat is more than `grace_epochs` behind
    /// the new global epoch and still hold in-flight work, and heals
    /// the watched rings' slots stuck since the previous scan when any
    /// registered process's beat is that far behind.
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
        let mut lapsed = false;
        for i in 0..self.table.capacity() {
            if let Some(snap) = self.table.snapshot(i) {
                let lag = new_epoch.saturating_sub(snap.last_seen_epoch);
                if lag > self.grace_epochs {
                    lapsed = true;
                    if snap.in_flight_bitmap != 0 {
                        dead.push((i, snap));
                    }
                }
            }
        }
        let mut healed_slots = Vec::new();
        let mut heal_errors = Vec::new();
        self.heal_watched(lapsed, &mut healed_slots, &mut heal_errors);
        let dead_count = dead.len();
        <HeartbeatTable as subetha_sidecar::AdaptiveInstance>::ring(self.table).push(
            subetha_core::Observation {
                op_kind: crate::sidecar_ops::liveness::OP_SCAN,
                flags: if dead_count > 0 { 1 } else { 0 },  // 1 = reclaim required
                ..subetha_core::Observation::ZERO
            },
        );
        ReclaimReport { dead_slots: dead, new_global_epoch: new_epoch, healed_slots, heal_errors }
    }

    /// On each watched ring, heal every slot stuck at a position the
    /// previous scan also found stuck, when `lapsed`, appending to
    /// `healed` and `errors`; every other stuck position becomes a
    /// suspect for the next scan.
    fn heal_watched(
        &self,
        lapsed: bool,
        healed: &mut Vec<(usize, u64)>,
        errors: &mut Vec<(usize, u64, RingError)>,
    ) {
        for (index, watched) in self.watched.iter().enumerate() {
            let suspects = watched.suspects.load_full();
            let mut still_stuck = Vec::new();
            let mut from = 0;
            while let Some(pos) = watched.ring.next_stuck_slot(from) {
                from = pos + 1;
                if !lapsed || !suspects.contains(&pos) {
                    still_stuck.push(pos);
                    continue;
                }
                match watched.ring.heal_stuck_slot(pos) {
                    Ok(true) => healed.push((index, pos)),
                    // Published since the scan found it: not stuck after all.
                    Ok(false) => {}
                    Err(error) => {
                        errors.push((index, pos, error));
                        still_stuck.push(pos);
                    }
                }
            }
            watched.suspects.store(Arc::new(still_stuck));
        }
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

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp_path(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-failover-{name}-{}.bin", std::process::id()))
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
    }

    /// Claim the ring's next position the way a producer does and never
    /// publish it, as a producer that died between the two leaves it.
    fn claim_without_publishing(ring: &SharedRing) -> u64 {
        ring.header().producer_seq.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    #[test]
    fn a_slot_stuck_across_two_scans_is_healed_once_a_beat_lapses() {
        let p = tmp_path("heal-two-scans");
        let t = HeartbeatTable::create(&p, 2).unwrap();
        t.register(9).unwrap();
        let ring = SharedRing::create_anon(8).unwrap();
        let pos = claim_without_publishing(&ring);

        // grace 0: the process never beats, so every scan finds it lapsed.
        let mut w = FailoverWatchdog::with_grace(&t, 0);
        let watched = w.watch_ring(&ring);
        let first = w.scan();
        assert!(first.healed_slots.is_empty(),
                "a slot one scan has seen stuck is not healed; got {:?}", first.healed_slots);
        let second = w.scan();
        assert_eq!(second.healed_slots, vec![(watched, pos)]);
        assert!(second.heal_errors.is_empty(), "{:?}", second.heal_errors);

        let mut out = [0u8; crate::shared_ring::PAYLOAD_BYTES];
        assert!(ring.try_pop(&mut out).is_ok(), "the consumer drains the healed slot");
    }

    #[test]
    fn no_slot_is_healed_while_every_process_beats() {
        let p = tmp_path("no-heal-alive");
        let t = HeartbeatTable::create(&p, 2).unwrap();
        let alive = t.register(9).unwrap();
        let ring = SharedRing::create_anon(8).unwrap();
        claim_without_publishing(&ring);

        let mut w = FailoverWatchdog::new(&t);
        w.watch_ring(&ring);
        for _ in 0..3 {
            t.beat(alive);
            let report = w.scan();
            assert!(report.healed_slots.is_empty(),
                    "no process has lapsed, so nothing is healed; got {:?}", report.healed_slots);
        }
        assert!(ring.next_stuck_slot(0).is_some(), "the slot is still stuck");
    }
}
