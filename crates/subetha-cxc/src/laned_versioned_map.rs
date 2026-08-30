//! `LanedVersionedMap<K, V>` - one versioned index split across `n`
//! single-writer lanes, so `n` statements write it without queueing.
//!
//! # Why lanes
//!
//! [`SharedBTreeMap`](crate::SharedBTreeMap) is single-writer: two
//! simultaneous writers mutate node structure under one seqlock and
//! corrupt it, so concurrent writes need external coordination, and
//! [`VersionedBTreeMap`] inherits that. The coordination available
//! today is one mutex over the whole index, which makes every writing
//! statement queue behind every other.
//!
//! A lane is a whole `VersionedBTreeMap` with its own arena and its own
//! single writer. A statement claims a lane for its duration and writes
//! only there, so `n` statements run at once with no mutex between
//! them. Every lane shares ONE epoch table, so a pin is one view across
//! all of them and one horizon reclaims all of them.
//!
//! What this costs is ordered reads. A `range_at` must merge `n` lanes,
//! and on a whole-store scan that measured 2.37x a single tree at four
//! lanes and 2.97x at eight. Point reads probe lanes until the key is
//! found. The trade is deliberate: scans cost more, writers stop
//! queueing.
//!
//! # A key belongs to the lane that created it, for its whole life
//!
//! This is the constraint the whole design rests on, and it is the
//! caller's to keep. A key inserted in lane 2 is removed in lane 2. It
//! is not a convention - a lane is a separate tree, so a key written to
//! two lanes exists twice, and neither copy knows about the other.
//!
//! The shape that fits is the one [`VersionedBTreeMap`] already asks
//! for: keys that are born and die but do not change, such as a
//! composite of key and record id. A statement that inserts fresh keys
//! claims any free lane with [`claim_lane`](LanedVersionedMap::claim_lane).
//! A statement that must touch keys that already exist claims THEIR
//! lane with
//! [`claim_lane_for`](LanedVersionedMap::claim_lane_for).
//!
//! Where enforcing it is free, it is enforced. Removing a key that is
//! absent from the claimed lane but present in another returns
//! [`LanedError::KeyInAnotherLane`] naming that lane, rather than the
//! `Ok(None)` that would report a row silently not removed. That check
//! costs a probe of the other lanes and runs only on the path that was
//! already failing.
//!
//! # The merge frontier
//!
//! A lane asked for a chunk reports the last key its walk examined. It
//! may hold keys past that, so the merged output can only be trusted up
//! to the SMALLEST cursor any lane reported: emitting beyond it would
//! publish a key ahead of a smaller one that some lane has not been
//! asked for yet. A lane whose cursor is `None` reached the end of the
//! range and bounds nothing.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::size_of;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::{holder_table_size, HolderTable};
use crate::shared_epochs::{Epoch, EpochError, PinGuard, SharedEpochs};
use crate::versioned_btree_map::{VersionedBTreeMap, VersionedError};

/// "LANECLM1": a header followed by one holder table of lane claims.
pub const LANES_MAGIC: u64 = 0x4C41_4E45_434C_4D31;

#[repr(C)]
pub struct LanesHeader {
    pub magic: u64,
    pub lanes: u64,
    _pad: [u64; 6],
}

/// Bytes the claims file needs for `lanes` lanes.
pub const fn lanes_file_size(lanes: usize) -> usize {
    size_of::<LanesHeader>() + holder_table_size(lanes)
}

#[derive(Debug)]
pub enum LanedError {
    /// Every lane is held by another statement.
    NoFreeLane,
    /// The lane this key belongs to is held by another statement.
    LaneBusy(usize),
    /// No lane holds this key.
    KeyAbsent,
    /// The key is not in the claimed lane; it belongs to the lane named
    /// here and must be removed there.
    KeyInAnotherLane(usize),
    LayoutMismatch,
    Versioned(VersionedError),
    Epochs(EpochError),
    Io(std::io::Error),
}

impl From<VersionedError> for LanedError {
    fn from(e: VersionedError) -> Self {
        LanedError::Versioned(e)
    }
}

impl From<EpochError> for LanedError {
    fn from(e: EpochError) -> Self {
        LanedError::Epochs(e)
    }
}

impl From<std::io::Error> for LanedError {
    fn from(e: std::io::Error) -> Self {
        LanedError::Io(e)
    }
}

impl std::fmt::Display for LanedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LanedError::NoFreeLane => write!(f, "every lane is held by another statement"),
            LanedError::LaneBusy(i) => {
                write!(f, "lane {i}, which owns this key, is held by another statement")
            }
            LanedError::KeyAbsent => write!(f, "no lane holds this key"),
            LanedError::KeyInAnotherLane(i) => {
                write!(f, "this key belongs to lane {i} and must be removed there")
            }
            LanedError::LayoutMismatch => write!(f, "the claims file was laid out for a different lane count"),
            LanedError::Versioned(e) => write!(f, "lane: {e}"),
            LanedError::Epochs(e) => write!(f, "epoch table: {e}"),
            LanedError::Io(e) => write!(f, "claims file: {e}"),
        }
    }
}

impl std::error::Error for LanedError {}

pub struct LanedVersionedMap<K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    lanes: Vec<VersionedBTreeMap<K, V>>,
    _claims_file: Option<std::fs::File>,
    _claims_mmap: MmapMut,
    claims: HolderTable,
}

impl<K, V> LanedVersionedMap<K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    /// Obtain a laned map under `dir`: one tree per lane, one shared
    /// epoch table, and the claims table beside them.
    ///
    /// `nodes_per_lane` is a NODE count, as `VersionedBTreeMap::create`
    /// takes, and each lane gets its own arena of that size. `max_pins`
    /// is how many scans may hold a pin at once, across all lanes.
    pub fn create(
        dir: impl AsRef<Path>,
        lanes: usize,
        nodes_per_lane: usize,
        max_pins: usize,
    ) -> Result<Self, LanedError> {
        assert!(lanes >= 1, "a laned map has at least one lane");
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let epochs = dir.join("shared.epochs");
        let trees = (0..lanes)
            .map(|i| {
                VersionedBTreeMap::create(Self::lane_path(dir, i), nodes_per_lane, &epochs, max_pins)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            &dir.join("lanes.claims"),
            lanes_file_size(lanes),
            |ptr| unsafe { Self::init_region(ptr, lanes) },
            |ptr| unsafe { (*(ptr as *const LanesHeader)).magic == LANES_MAGIC },
        )?;
        Self::attach(trees, Some(file), mmap, lanes)
    }

    /// Attach to a laned map another process created.
    pub fn open(
        dir: impl AsRef<Path>,
        lanes: usize,
        nodes_per_lane: usize,
        max_pins: usize,
    ) -> Result<Self, LanedError> {
        let dir = dir.as_ref();
        let epochs = dir.join("shared.epochs");
        let trees = (0..lanes)
            .map(|i| {
                VersionedBTreeMap::open(Self::lane_path(dir, i), nodes_per_lane, &epochs, max_pins)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let path = dir.join("lanes.claims");
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&path)?;
        let total = lanes_file_size(lanes);
        if file.metadata()?.len() < total as u64 {
            return Err(LanedError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::attach(trees, Some(file), mmap, lanes)
    }

    fn lane_path(dir: &Path, i: usize) -> PathBuf {
        dir.join(format!("lane{i}.bin"))
    }

    /// # Safety
    /// `ptr` addresses at least `lanes_file_size(lanes)` zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, lanes: usize) {
        let h = ptr as *mut LanesHeader;
        unsafe {
            (*h).lanes = lanes as u64;
            // Magic last: a reader that sees it sees a formed header.
            (*h).magic = LANES_MAGIC;
        }
    }

    fn attach(
        lanes_vec: Vec<VersionedBTreeMap<K, V>>,
        file: Option<std::fs::File>,
        mmap: MmapMut,
        lanes: usize,
    ) -> Result<Self, LanedError> {
        let header = unsafe { &*(mmap.as_ptr() as *const LanesHeader) };
        if header.magic != LANES_MAGIC || header.lanes as usize != lanes {
            return Err(LanedError::LayoutMismatch);
        }
        let base = unsafe { mmap.as_ptr().add(size_of::<LanesHeader>()) };
        let claims = unsafe { HolderTable::from_ptr(base, lanes) };
        Ok(Self { lanes: lanes_vec, _claims_file: file, _claims_mmap: mmap, claims })
    }

    /// Lanes this map holds.
    #[inline]
    pub fn lanes(&self) -> usize {
        self.lanes.len()
    }

    /// The epoch table every lane shares.
    #[inline]
    pub fn epochs(&self) -> &SharedEpochs {
        self.lanes[0].epochs()
    }

    /// Pin the published epoch. One pin is one view across every lane.
    pub fn pin(&self) -> Result<PinGuard<'_>, LanedError> {
        Ok(self.lanes[0].pin()?)
    }

    /// Lanes currently held by a statement.
    pub fn held_lanes(&self) -> usize {
        self.claims.live()
    }

    /// Release the lanes of holders whose process is gone, and report
    /// how many came back.
    pub fn reap_dead_claims(&self) -> usize {
        self.claims.reap_dead()
    }

    /// Claim any free lane, for a statement inserting keys that do not
    /// yet exist.
    pub fn claim_lane(&self) -> Result<LaneGuard<'_, K, V>, LanedError> {
        match self.claims.claim(CLAIM_PAYLOAD) {
            Some(i) => Ok(LaneGuard { map: self, lane: i }),
            None => Err(LanedError::NoFreeLane),
        }
    }

    /// Claim the lane that owns `key`, for a statement that must remove
    /// or rewrite a key that already exists.
    ///
    /// [`LanedError::KeyAbsent`] when no lane holds it,
    /// [`LanedError::LaneBusy`] when its lane is held by another
    /// statement - the caller retries rather than writing elsewhere,
    /// because elsewhere is a different tree.
    pub fn claim_lane_for(&self, key: &K) -> Result<LaneGuard<'_, K, V>, LanedError> {
        let i = self.lane_of(key).ok_or(LanedError::KeyAbsent)?;
        if self.claims.try_claim_slot(i, CLAIM_PAYLOAD) {
            Ok(LaneGuard { map: self, lane: i })
        } else {
            Err(LanedError::LaneBusy(i))
        }
    }

    /// The lane holding `key` right now, if any.
    pub fn lane_of(&self, key: &K) -> Option<usize> {
        self.lanes.iter().position(|l| l.get(key).is_some())
    }

    /// The value current right now, from whichever lane holds the key.
    pub fn get(&self, key: &K) -> Option<V> {
        self.lanes.iter().find_map(|l| l.get(key))
    }

    /// The value current at `pin`, from whichever lane holds the key.
    pub fn get_at(&self, key: &K, pin: &PinGuard<'_>) -> Option<V> {
        self.lanes.iter().find_map(|l| l.get_at(key, pin))
    }

    /// Entries current at `pin` across every lane, in key order, at most
    /// `limit` EXAMINED per lane.
    pub fn range_at(
        &self,
        low: Bound<&K>,
        high: Bound<&K>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> Vec<(K, V)> {
        self.range_at_with_cursor(low, high, limit, pin).0
    }

    /// As [`range_at`](Self::range_at), and the frontier the merge is
    /// good to: the smallest cursor any lane reported, or `None` when
    /// every lane reached the end of the range.
    ///
    /// Resume from `Bound::Excluded(frontier)`. Rows past the frontier
    /// are NOT returned even when a lane already walked them, because a
    /// lane that stopped earlier may still hold a smaller key.
    pub fn range_at_with_cursor(
        &self,
        low: Bound<&K>,
        high: Bound<&K>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> (Vec<(K, V)>, Option<K>) {
        let mut runs: Vec<Vec<(K, V)>> = Vec::with_capacity(self.lanes.len());
        let mut frontier: Option<K> = None;
        for l in &self.lanes {
            let (rows, cursor) = l.range_at_with_cursor(low, high, limit, pin);
            if let Some(c) = cursor {
                frontier = Some(match frontier {
                    Some(f) if f <= c => f,
                    _ => c,
                });
            }
            runs.push(rows);
        }
        // Cut every ordered run at the frontier. A run is ascending, so
        // the cut is a truncate rather than a filter.
        if let Some(f) = frontier {
            for rows in &mut runs {
                let end = rows.partition_point(|(k, _)| *k <= f);
                rows.truncate(end);
            }
        }
        let total: usize = runs.iter().map(|r| r.len()).sum();
        let mut out = Vec::with_capacity(total);
        let mut idx = vec![0usize; runs.len()];
        let mut heap: BinaryHeap<Reverse<(K, usize)>> = BinaryHeap::new();
        for (i, rows) in runs.iter().enumerate() {
            if let Some((k, _)) = rows.first() {
                heap.push(Reverse((*k, i)));
            }
        }
        while let Some(Reverse((_, i))) = heap.pop() {
            out.push(runs[i][idx[i]]);
            idx[i] += 1;
            if let Some((k, _)) = runs[i].get(idx[i]) {
                heap.push(Reverse((*k, i)));
            }
        }
        (out, frontier)
    }

    /// Undo every stamp at `epoch` in every lane, for an epoch whose
    /// ticket holder died mid-compound. Returns the entries touched.
    pub fn void_epoch(&self, epoch: Epoch) -> Result<usize, LanedError> {
        let mut touched = 0usize;
        for l in &self.lanes {
            touched += l.void_epoch(epoch)?;
        }
        Ok(touched)
    }

    /// Drop every entry superseded below the horizon, in every lane.
    pub fn sweep(&self) -> Result<usize, LanedError> {
        let mut dropped = 0usize;
        for l in &self.lanes {
            dropped += l.sweep()?;
        }
        Ok(dropped)
    }

    /// Entries every lane holds, tombstones included.
    pub fn len(&self) -> usize {
        self.lanes.iter().map(|l| l.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn flush(&self) -> Result<(), LanedError> {
        for l in &self.lanes {
            l.flush()?;
        }
        Ok(())
    }
}

/// A held lane. Writes go through it, and dropping it releases the
/// claim so another statement can take that lane.
pub struct LaneGuard<'a, K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    map: &'a LanedVersionedMap<K, V>,
    lane: usize,
}

/// A claim carries no information beyond being held; the payload only
/// has to avoid the table's two reserved states.
const CLAIM_PAYLOAD: u64 = 1;

impl<K, V> LaneGuard<'_, K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    /// Which lane this guard holds.
    #[inline]
    pub fn index(&self) -> usize {
        self.lane
    }

    fn lane(&self) -> &VersionedBTreeMap<K, V> {
        &self.map.lanes[self.lane]
    }

    /// Make `key` current in this lane at a fresh epoch.
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>, LanedError> {
        Ok(self.lane().insert(key, value)?)
    }

    /// Make `key` current in this lane at a ticket's epoch, so every
    /// entry of one compound write is seen all or none.
    pub fn insert_at(&self, key: K, value: V, born: Epoch) -> Result<Option<V>, LanedError> {
        Ok(self.lane().insert_at(key, value, born)?)
    }

    /// Stamp `key` superseded in this lane at a fresh epoch.
    ///
    /// [`LanedError::KeyInAnotherLane`] when the key is absent here but
    /// present elsewhere: a lane is a separate tree, so removing it
    /// here would report a row gone that no reader has stopped seeing.
    pub fn remove(&self, key: &K) -> Result<Option<V>, LanedError> {
        match self.lane().remove(key)? {
            Some(v) => Ok(Some(v)),
            None => self.absent_here(key),
        }
    }

    /// As [`remove`](Self::remove), at a ticket's epoch.
    pub fn remove_at(&self, key: &K, died: Epoch) -> Result<Option<V>, LanedError> {
        match self.lane().remove_at(key, died)? {
            Some(v) => Ok(Some(v)),
            None => self.absent_here(key),
        }
    }

    /// Name the lane that holds a key this one does not, so a misrouted
    /// removal is reported rather than read as "there was nothing here".
    fn absent_here(&self, key: &K) -> Result<Option<V>, LanedError> {
        match self.map.lane_of(key) {
            Some(other) if other != self.lane => Err(LanedError::KeyInAnotherLane(other)),
            _ => Ok(None),
        }
    }
}

impl<K, V> Drop for LaneGuard<'_, K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    fn drop(&mut self) {
        self.map.claims.release(self.lane);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        dir: PathBuf,
        map: LanedVersionedMap<u64, u64>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn fixture(name: &str, lanes: usize) -> Fixture {
        let dir = std::env::temp_dir().join(format!("subetha_lanes_{name}_{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        let map = LanedVersionedMap::create(&dir, lanes, 1 << 10, 16).unwrap();
        Fixture { dir, map }
    }

    fn all(map: &LanedVersionedMap<u64, u64>, pin: &PinGuard<'_>) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        let mut low: Bound<u64> = Bound::Unbounded;
        loop {
            let lb = match &low {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Excluded(k) => Bound::Excluded(k),
                Bound::Included(k) => Bound::Included(k),
            };
            let (rows, frontier) = map.range_at_with_cursor(lb, Bound::Unbounded, 64, pin);
            out.extend(rows);
            match frontier {
                None => return out,
                Some(f) => low = Bound::Excluded(f),
            }
        }
    }

    #[test]
    fn a_merged_scan_returns_every_lane_in_key_order() {
        let f = fixture("order", 4);
        for i in 0..40u64 {
            let g = f.map.claim_lane().unwrap();
            g.insert(i, i * 10).unwrap();
        }
        let pin = f.map.pin().unwrap();
        let rows = all(&f.map, &pin);
        let expect: Vec<(u64, u64)> = (0..40).map(|i| (i, i * 10)).collect();
        assert_eq!(rows, expect, "the merge must be ordered and lose nothing");
    }

    /// Round-robin so every lane spans the whole key range, which is
    /// what makes the frontier rule load-bearing.
    #[test]
    fn a_chunked_merged_scan_covers_the_same_rows_as_one_call() {
        let f = fixture("chunked", 4);
        // Four statements each holding a lane for the duration, which is
        // what the primitive is for, and what puts every lane across the
        // whole key range.
        let guards: Vec<_> = (0..4).map(|_| f.map.claim_lane().unwrap()).collect();
        for i in 0..64u64 {
            guards[(i % 4) as usize].insert(i, i).unwrap();
        }
        drop(guards);
        let pin = f.map.pin().unwrap();
        let chunked = all(&f.map, &pin);
        let expect: Vec<(u64, u64)> = (0..64).map(|i| (i, i)).collect();
        assert_eq!(chunked, expect);
    }

    #[test]
    fn a_claimed_lane_is_refused_to_a_second_statement_and_freed_on_drop() {
        let f = fixture("claim", 2);
        let a = f.map.claim_lane().unwrap();
        let b = f.map.claim_lane().unwrap();
        assert_ne!(a.index(), b.index(), "two statements must not share a lane");
        let Err(LanedError::NoFreeLane) = f.map.claim_lane() else {
            panic!("a third statement must be refused when both lanes are held");
        };
        assert_eq!(f.map.held_lanes(), 2);
        drop(a);
        let c = f.map.claim_lane().unwrap();
        assert_eq!(c.index(), 0, "the released lane comes back");
    }

    #[test]
    fn removing_a_key_from_the_wrong_lane_names_the_right_one() {
        let f = fixture("wronglane", 3);
        let owner = {
            let g = f.map.claim_lane().unwrap();
            g.insert(7, 70).unwrap();
            g.index()
        };
        // A statement holding a different lane must not read the miss as
        // "there was nothing here". The owner's guard was released, so
        // claim two and keep whichever is not the owner's.
        let a = f.map.claim_lane().unwrap();
        let b = f.map.claim_lane().unwrap();
        let (other, spare) = if a.index() == owner { (b, a) } else { (a, b) };
        drop(spare);
        assert_ne!(other.index(), owner);
        let Err(LanedError::KeyInAnotherLane(named)) = other.remove(&7) else {
            panic!("removing another lane's key must name that lane");
        };
        assert_eq!(named, owner);
        // A key absent everywhere is still an ordinary None.
        assert_eq!(other.remove(&999).unwrap(), None);
        drop(other);

        let g = f.map.claim_lane_for(&7).unwrap();
        assert_eq!(g.index(), owner);
        assert_eq!(g.remove(&7).unwrap(), Some(70));
    }

    #[test]
    fn claiming_the_lane_of_a_key_reports_absent_and_busy_apart() {
        let f = fixture("claimfor", 2);
        let Err(LanedError::KeyAbsent) = f.map.claim_lane_for(&1) else {
            panic!("a key no lane holds is KeyAbsent");
        };
        let g = f.map.claim_lane().unwrap();
        g.insert(1, 10).unwrap();
        let held = g.index();
        let Err(LanedError::LaneBusy(i)) = f.map.claim_lane_for(&1) else {
            panic!("the owning lane being held is LaneBusy, not KeyAbsent");
        };
        assert_eq!(i, held);
    }

    #[test]
    fn a_pin_taken_before_a_write_in_any_lane_does_not_see_it() {
        let f = fixture("pin", 4);
        {
            let g = f.map.claim_lane().unwrap();
            g.insert(1, 10).unwrap();
        }
        let pin = f.map.pin().unwrap();
        {
            let g = f.map.claim_lane().unwrap();
            g.insert(2, 20).unwrap();
        }
        assert_eq!(all(&f.map, &pin), vec![(1, 10)], "the later write is not in the pinned view");
        assert_eq!(f.map.get(&2), Some(20), "but it is current");
        let after = f.map.pin().unwrap();
        assert_eq!(all(&f.map, &after), vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn one_ticket_across_lanes_is_seen_all_or_none() {
        let f = fixture("ticket", 4);
        let t = f.map.epochs().begin().unwrap();
        let e = t.epoch();
        {
            let g = f.map.claim_lane().unwrap();
            g.insert_at(1, 10, e).unwrap();
        }
        {
            let g = f.map.claim_lane().unwrap();
            g.insert_at(2, 20, e).unwrap();
        }
        let mid = f.map.pin().unwrap();
        assert!(all(&f.map, &mid).is_empty(), "an open ticket is invisible in every lane");
        t.publish();
        let after = f.map.pin().unwrap();
        assert_eq!(all(&f.map, &after), vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn voiding_a_dead_tickets_epoch_undoes_every_lane() {
        let f = fixture("void", 4);
        let t = f.map.epochs().begin().unwrap();
        let e = t.epoch();
        {
            let g = f.map.claim_lane().unwrap();
            g.insert_at(1, 10, e).unwrap();
        }
        {
            let g = f.map.claim_lane().unwrap();
            g.insert_at(2, 20, e).unwrap();
        }
        std::mem::forget(t);
        assert_eq!(f.map.void_epoch(e).unwrap(), 2);
        assert_eq!(f.map.get(&1), None);
        assert_eq!(f.map.get(&2), None);
    }

    #[test]
    fn a_second_handle_shares_the_lanes_and_the_claims() {
        let f = fixture("second", 2);
        {
            let g = f.map.claim_lane().unwrap();
            g.insert(5, 50).unwrap();
        }
        let other: LanedVersionedMap<u64, u64> =
            LanedVersionedMap::open(&f.dir, 2, 1 << 10, 16).unwrap();
        assert_eq!(other.get(&5), Some(50));
        let held = f.map.claim_lane().unwrap();
        // Bind the guard: a temporary would release the lane at the end
        // of the statement and the next claim would find it free.
        let taken = other.claim_lane().expect("the other handle takes the remaining lane");
        assert_ne!(taken.index(), held.index(), "two handles must not share a lane");
        let Err(LanedError::NoFreeLane) = other.claim_lane() else {
            panic!("a claim held through one handle must be visible to the other");
        };
        drop(taken);
        drop(held);
    }
}
