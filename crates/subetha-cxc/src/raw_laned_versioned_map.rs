//! The laned versioned map with its key and value sizes given at run
//! time, for callers that cannot name them in the type system.
//!
//! [`LanedVersionedMap`](crate::laned_versioned_map::LanedVersionedMap)
//! fixes both as generic parameters, which suits Rust callers and shuts
//! out every binding that reaches this crate through C. This form is the
//! same shape over
//! [`RawVersionedBTreeMap`](crate::raw_versioned_btree_map::RawVersionedBTreeMap):
//! one tree per lane, one epoch
//! table shared by all of them, and a claims table so one statement holds
//! one lane while it writes.
//!
//! A key belongs to one lane for its whole life. A removal aimed at the
//! wrong lane is refused by name rather than reported as absent, because
//! a lane is a separate tree and removing it here would report a row gone
//! that no reader has stopped seeing.

use std::path::{Path, PathBuf};

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::HolderTable;
use crate::laned_versioned_map::{lanes_file_size, LanedError, LanesHeader, LANES_MAGIC};
use crate::raw_versioned_btree_map::{RawEntry, RawVersionedBTreeMap};
use crate::versioned_btree_map::VersionedError;
use crate::shared_epochs::{Epoch, PinGuard, SharedEpochs};

/// A claim carries no information beyond being held; the payload only has
/// to avoid the table's two reserved states.
const CLAIM_PAYLOAD: u64 = 1;

/// A range's merged entries and the frontier the merge is good to.
pub type RawLanedPage = (Vec<RawEntry>, Option<Vec<u8>>);

pub struct RawLanedVersionedMap {
    lanes: Vec<RawVersionedBTreeMap>,
    _claims_file: Option<std::fs::File>,
    _claims_mmap: MmapMut,
    claims: HolderTable,
    key_size: usize,
    value_size: usize,
}

impl RawLanedVersionedMap {
    /// Obtain a laned map under `dir`: one tree per lane, one shared epoch
    /// table, and the claims table beside them.
    ///
    /// `nodes_per_lane` is a node count and each lane gets its own arena
    /// of that size. `max_pins` is how many scans may hold a pin at once,
    /// across all lanes.
    pub fn create(
        dir: impl AsRef<Path>,
        lanes: usize,
        nodes_per_lane: usize,
        key_size: usize,
        value_size: usize,
        max_pins: usize,
    ) -> Result<Self, LanedError> {
        assert!(lanes >= 1, "a laned map has at least one lane");
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let epochs = dir.join("shared.epochs");
        let trees = (0..lanes)
            .map(|i| {
                RawVersionedBTreeMap::create(
                    Self::lane_path(dir, i),
                    nodes_per_lane,
                    key_size,
                    value_size,
                    &epochs,
                    max_pins,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            &dir.join("lanes.claims"),
            lanes_file_size(lanes),
            |ptr| unsafe { Self::init_region(ptr, lanes) },
            |ptr| unsafe { (*(ptr as *const LanesHeader)).magic == LANES_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, LanedError::LayoutMismatch))?;
        Self::attach(trees, Some(file), mmap, lanes, key_size, value_size)
    }

    /// Attach to a laned map another process created.
    pub fn open(
        dir: impl AsRef<Path>,
        lanes: usize,
        nodes_per_lane: usize,
        key_size: usize,
        value_size: usize,
        max_pins: usize,
    ) -> Result<Self, LanedError> {
        let dir = dir.as_ref();
        let epochs = dir.join("shared.epochs");
        let trees = (0..lanes)
            .map(|i| {
                RawVersionedBTreeMap::open(
                    Self::lane_path(dir, i),
                    nodes_per_lane,
                    key_size,
                    value_size,
                    &epochs,
                    max_pins,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let path = dir.join("lanes.claims");
        let file = crate::region_file::open_existing(&path)?;
        let total = lanes_file_size(lanes);
        if file.metadata()?.len() < total as u64 {
            return Err(LanedError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::attach(trees, Some(file), mmap, lanes, key_size, value_size)
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
        lanes_vec: Vec<RawVersionedBTreeMap>,
        file: Option<std::fs::File>,
        mmap: MmapMut,
        lanes: usize,
        key_size: usize,
        value_size: usize,
    ) -> Result<Self, LanedError> {
        let header = unsafe { &*(mmap.as_ptr() as *const LanesHeader) };
        if header.magic != LANES_MAGIC || header.lanes as usize != lanes {
            return Err(LanedError::LayoutMismatch);
        }
        let base = unsafe { mmap.as_ptr().add(size_of::<LanesHeader>()) };
        let claims = unsafe { HolderTable::from_ptr(base, lanes) };
        Ok(Self {
            lanes: lanes_vec,
            _claims_file: file,
            _claims_mmap: mmap,
            claims,
            key_size,
            value_size,
        })
    }

    /// Lanes this map holds.
    pub fn lanes(&self) -> usize {
        self.lanes.len()
    }

    /// Bytes a key takes.
    pub fn key_size(&self) -> usize {
        self.key_size
    }

    /// Bytes a value takes.
    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// The epoch table every lane shares.
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

    /// Release the lanes of holders whose process is gone, and report how
    /// many came back.
    pub fn reap_dead_claims(&self) -> usize {
        self.claims.reap_dead()
    }

    /// Claim any free lane, for a statement inserting keys that do not yet
    /// exist.
    pub fn claim_lane(&self) -> Result<RawLaneGuard<'_>, LanedError> {
        match self.claims.claim(CLAIM_PAYLOAD) {
            Some(i) => Ok(RawLaneGuard { map: self, lane: i }),
            None => Err(LanedError::NoFreeLane),
        }
    }

    /// Claim the lane that owns `key`, for a statement that must remove or
    /// rewrite a key that already exists.
    pub fn claim_lane_for(&self, key: &[u8]) -> Result<RawLaneGuard<'_>, LanedError> {
        let i = self.lane_of(key)?.ok_or(LanedError::KeyAbsent)?;
        if self.claims.try_claim_slot(i, CLAIM_PAYLOAD) {
            Ok(RawLaneGuard { map: self, lane: i })
        } else {
            Err(LanedError::LaneBusy(i))
        }
    }

    /// Claim any free lane and report which, for a caller that cannot
    /// hold a guard because its lifetime is not expressible where it
    /// lives. It releases with [`release_lane`](Self::release_lane), and
    /// a holder that dies without doing so is reaped by
    /// [`reap_dead_claims`](Self::reap_dead_claims).
    pub fn claim_any_lane(&self) -> Option<usize> {
        self.claims.claim(CLAIM_PAYLOAD)
    }

    /// Claim lane `i` if it is free. As [`claim_any_lane`](Self::claim_any_lane)
    /// for who should use it.
    pub fn claim_lane_index(&self, i: usize) -> bool {
        i < self.lanes.len() && self.claims.try_claim_slot(i, CLAIM_PAYLOAD)
    }

    /// Release a lane claimed without a guard.
    pub fn release_lane(&self, i: usize) {
        self.claims.release(i);
    }

    /// Make `key` current in lane `i` at a fresh epoch, for a caller
    /// holding that lane without a guard.
    pub fn insert_in(&self, i: usize, key: &[u8], value: &[u8]) -> Result<(), LanedError> {
        let lane = self.lanes.get(i).ok_or(LanedError::LayoutMismatch)?;
        Ok(lane.insert(key, value)?)
    }

    /// Stamp `key` superseded in lane `i` at a fresh epoch, for a caller
    /// holding that lane without a guard. [`LanedError::KeyInAnotherLane`]
    /// when the key is absent here but present elsewhere.
    pub fn remove_in(
        &self,
        i: usize,
        key: &[u8],
        out: &mut [u8],
    ) -> Result<bool, LanedError> {
        let lane = self.lanes.get(i).ok_or(LanedError::LayoutMismatch)?;
        if lane.remove(key, out)? {
            return Ok(true);
        }
        match self.lane_of(key)? {
            Some(other) if other != i => Err(LanedError::KeyInAnotherLane(other)),
            _ => Ok(false),
        }
    }

    /// The lane holding `key` right now, if any.
    pub fn lane_of(&self, key: &[u8]) -> Result<Option<usize>, LanedError> {
        let mut discard = vec![0u8; self.value_size];
        for (i, lane) in self.lanes.iter().enumerate() {
            if lane.get(key, &mut discard)? {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// The value current right now, from whichever lane holds the key.
    pub fn get(&self, key: &[u8], out: &mut [u8]) -> Result<bool, LanedError> {
        for lane in &self.lanes {
            if lane.get(key, out)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The value current at `pin`, from whichever lane holds the key.
    pub fn get_at(
        &self,
        key: &[u8],
        pin: &PinGuard<'_>,
        out: &mut [u8],
    ) -> Result<bool, LanedError> {
        for lane in &self.lanes {
            if lane.get_at(key, pin, out)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Entries current at `pin` across every lane, in key order, at most
    /// `limit` examined per lane.
    pub fn range_at(
        &self,
        low: std::ops::Bound<&[u8]>,
        high: std::ops::Bound<&[u8]>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> Vec<RawEntry> {
        self.range_at_with_cursor(low, high, limit, pin).0
    }

    /// As [`range_at`](Self::range_at), and the frontier the merge is good
    /// to: the smallest cursor any lane reported, or `None` when every
    /// lane reached the end of the range.
    ///
    /// Resume from just past the frontier. Rows beyond it are not returned
    /// even when a lane already walked them, because a lane that stopped
    /// earlier may still hold a smaller key.
    pub fn range_at_with_cursor(
        &self,
        low: std::ops::Bound<&[u8]>,
        high: std::ops::Bound<&[u8]>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> RawLanedPage {
        let mut runs: Vec<Vec<RawEntry>> = Vec::with_capacity(self.lanes.len());
        let mut frontier: Option<Vec<u8>> = None;
        for lane in &self.lanes {
            let (rows, cursor) = lane.range_at_with_cursor(low, high, limit, pin);
            if let Some(c) = cursor {
                frontier = Some(match frontier {
                    Some(f) if f <= c => f,
                    _ => c,
                });
            }
            runs.push(rows);
        }
        // Cut every ordered run at the frontier. A run ascends, so the cut
        // is a truncate rather than a filter.
        if let Some(f) = &frontier {
            for rows in &mut runs {
                let end = rows.partition_point(|(k, _)| k <= f);
                rows.truncate(end);
            }
        }
        // Merge the ordered runs. Taking the smallest head each time keeps
        // the whole result in key order across lanes.
        let total: usize = runs.iter().map(|r| r.len()).sum();
        let mut out: Vec<RawEntry> = Vec::with_capacity(total);
        let mut idx = vec![0usize; runs.len()];
        loop {
            let mut pick: Option<usize> = None;
            for (i, rows) in runs.iter().enumerate() {
                let Some((k, _)) = rows.get(idx[i]) else {
                    continue;
                };
                match pick {
                    Some(best) if runs[best][idx[best]].0 <= *k => {}
                    _ => pick = Some(i),
                }
            }
            let Some(i) = pick else {
                break;
            };
            out.push(runs[i][idx[i]].clone());
            idx[i] += 1;
        }
        (out, frontier)
    }

    /// Undo every stamp at `epoch` in every lane, for an epoch whose
    /// ticket holder died mid-compound. Returns the entries touched.
    pub fn void_epoch(&self, epoch: Epoch) -> Result<usize, LanedError> {
        let mut touched = 0usize;
        for lane in &self.lanes {
            touched += lane.void_epoch(epoch)?;
        }
        Ok(touched)
    }

    /// Drop every entry superseded below the horizon, in every lane.
    ///
    /// A lane with nothing to drop reports [`VersionedError::Full`],
    /// which here means only that this lane freed none; the sweep goes
    /// on to the rest and sums what they freed. The whole sweep reports
    /// `Full` when no lane freed anything, which is the signal an
    /// insert out of room needs.
    pub fn sweep(&self) -> Result<usize, LanedError> {
        let mut dropped = 0usize;
        for lane in &self.lanes {
            match lane.sweep() {
                Ok(freed) => dropped += freed,
                Err(VersionedError::Full) => {}
                Err(e) => return Err(e.into()),
            }
        }
        if dropped == 0 {
            return Err(LanedError::Versioned(VersionedError::Full));
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
        for lane in &self.lanes {
            lane.flush()?;
        }
        Ok(())
    }
}

/// A held lane. Writes go through it, and dropping it releases the claim
/// so another statement can take that lane.
pub struct RawLaneGuard<'a> {
    map: &'a RawLanedVersionedMap,
    lane: usize,
}

impl RawLaneGuard<'_> {
    /// Which lane this guard holds.
    pub fn index(&self) -> usize {
        self.lane
    }

    fn lane(&self) -> &RawVersionedBTreeMap {
        &self.map.lanes[self.lane]
    }

    /// Make `key` current in this lane at a fresh epoch.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), LanedError> {
        Ok(self.lane().insert(key, value)?)
    }

    /// Make `key` current in this lane at a ticket's epoch, so every entry
    /// of one compound write is seen all or none.
    pub fn insert_at(
        &self,
        key: &[u8],
        value: &[u8],
        born: Epoch,
    ) -> Result<(), LanedError> {
        Ok(self.lane().insert_at(key, value, born)?)
    }

    /// Stamp `key` superseded in this lane at a fresh epoch.
    ///
    /// [`LanedError::KeyInAnotherLane`] when the key is absent here but
    /// present elsewhere: a lane is a separate tree, so removing it here
    /// would report a row gone that no reader has stopped seeing.
    pub fn remove(&self, key: &[u8], out: &mut [u8]) -> Result<bool, LanedError> {
        if self.lane().remove(key, out)? {
            return Ok(true);
        }
        self.absent_here(key)
    }

    /// As [`remove`](Self::remove), at a ticket's epoch.
    pub fn remove_at(
        &self,
        key: &[u8],
        died: Epoch,
        out: &mut [u8],
    ) -> Result<bool, LanedError> {
        if self.lane().remove_at(key, died, out)? {
            return Ok(true);
        }
        self.absent_here(key)
    }

    /// Name the lane that holds a key this one does not, so a misrouted
    /// removal is reported rather than read as "there was nothing here".
    fn absent_here(&self, key: &[u8]) -> Result<bool, LanedError> {
        match self.map.lane_of(key)? {
            Some(other) if other != self.lane => Err(LanedError::KeyInAnotherLane(other)),
            _ => Ok(false),
        }
    }
}

impl Drop for RawLaneGuard<'_> {
    fn drop(&mut self) {
        self.map.claims.release(self.lane);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh directory for one test: whatever an earlier run left there
    /// is removed, and a removal refused for any reason but absence fails
    /// the test rather than gating it on stale files.
    fn fresh_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("subetha_rawlaned_{name}_{}", std::process::id()));
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("stale directory {} not removed: {e}", dir.display()),
        }
        dir
    }

    fn map(name: &str, lanes: usize) -> RawLanedVersionedMap {
        RawLanedVersionedMap::create(fresh_dir(name), lanes, 64, 4, 8, 8)
            .expect("the laned map is created")
    }

    fn key(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }

    #[test]
    fn a_claimed_lane_is_not_handed_to_a_second_statement() {
        let m = map("claims", 2);
        let first = m.claim_lane().expect("a free lane");
        let second = m.claim_lane().expect("the other free lane");
        assert_ne!(first.index(), second.index(), "two statements hold two lanes");
        assert!(
            matches!(m.claim_lane(), Err(LanedError::NoFreeLane)),
            "a two-lane map has none left"
        );
        drop(first);
        m.claim_lane().expect("the released lane is free again");
    }

    #[test]
    fn a_removal_aimed_at_the_wrong_lane_names_the_right_one() {
        let m = map("misrouted", 2);
        let held = m.claim_lane().expect("a lane");
        let mine = held.index();
        held.insert(&key(1), &7u64.to_le_bytes()).expect("the key lands");
        drop(held);

        let other = (mine + 1) % 2;
        assert!(m.claims.try_claim_slot(other, CLAIM_PAYLOAD), "the other lane is free");
        let wrong = RawLaneGuard { map: &m, lane: other };
        let mut out = [0u8; 8];
        assert!(
            matches!(wrong.remove(&key(1), &mut out), Err(LanedError::KeyInAnotherLane(l)) if l == mine),
            "the refusal names the lane that holds the key"
        );
    }

    /// One page stops at the frontier, which is the smallest last key any
    /// lane reported: a lane that stopped earlier could still hold a key
    /// below another lane's last, so rows past it are withheld even when a
    /// lane already walked them. Resuming past the frontier is what walks
    /// the whole map, and that is the contract worth holding.
    #[test]
    fn resuming_past_the_frontier_merges_every_lane_in_key_order() {
        use std::ops::Bound;
        let m = map("merge", 4);
        // Round-robin the keys across lanes, so key order and lane order
        // disagree and the merge has to do real work.
        for i in 0..40u32 {
            let lane = (i % 4) as usize;
            assert!(m.claims.try_claim_slot(lane, CLAIM_PAYLOAD), "the lane is free");
            let guard = RawLaneGuard { map: &m, lane };
            guard.insert(&key(i), &(i as u64).to_le_bytes()).expect("the key lands");
        }
        let pin = m.pin().expect("a pin");

        let mut seen: Vec<u32> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let low = match &cursor {
                Some(c) => Bound::Excluded(c.as_slice()),
                None => Bound::Unbounded,
            };
            let (rows, frontier) = m.range_at_with_cursor(low, Bound::Unbounded, 100, &pin);
            if rows.is_empty() {
                break;
            }
            seen.extend(rows.iter().map(|(k, _)| {
                u32::from_be_bytes(k[..].try_into().expect("a four-byte key"))
            }));
            cursor = frontier;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, (0..40).collect::<Vec<_>>(), "one ascending run across four lanes");
    }

    #[test]
    fn one_page_stops_at_the_smallest_last_key_any_lane_reported() {
        use std::ops::Bound;
        let m = map("frontier", 4);
        for i in 0..40u32 {
            let lane = (i % 4) as usize;
            assert!(m.claims.try_claim_slot(lane, CLAIM_PAYLOAD), "the lane is free");
            let guard = RawLaneGuard { map: &m, lane };
            guard.insert(&key(i), &(i as u64).to_le_bytes()).expect("the key lands");
        }
        let pin = m.pin().expect("a pin");
        let (rows, frontier) = m.range_at_with_cursor(Bound::Unbounded, Bound::Unbounded, 100, &pin);
        // Lane 0 holds 0, 4, .. 36 and stops there; the other lanes walked
        // past it, and what they walked past is withheld until a resume.
        assert_eq!(frontier.as_deref(), Some(&key(36)[..]), "the smallest last key");
        let last = rows.last().expect("the page is not empty");
        assert_eq!(
            u32::from_be_bytes(last.0[..].try_into().expect("a four-byte key")),
            36,
            "the page ends at the frontier"
        );
    }

    #[test]
    fn a_key_is_found_whichever_lane_holds_it() {
        let m = map("find", 3);
        let held = m.claim_lane().expect("a lane");
        held.insert(&key(9), &42u64.to_le_bytes()).expect("the key lands");
        let mine = held.index();
        drop(held);

        assert_eq!(m.lane_of(&key(9)).expect("the lookup runs"), Some(mine));
        let mut out = [0u8; 8];
        assert!(m.get(&key(9), &mut out).expect("the read runs"));
        assert_eq!(u64::from_le_bytes(out), 42);
        assert_eq!(m.lane_of(&key(8)).expect("the lookup runs"), None, "an absent key has no lane");
    }
}
