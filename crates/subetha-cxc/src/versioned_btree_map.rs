//! `VersionedBTreeMap<K, V>` - an ordered map whose entries carry the
//! epochs they were current between, so a scan reads a fixed view while
//! writers keep running.
//!
//! # What it is
//!
//! A [`SharedBTreeMap`] holding [`Versioned<V>`] beside a
//! [`SharedEpochs`]. An entry records the epoch it became current and
//! the epoch it stopped being current; a delete stamps the second
//! rather than removing the entry; a scan pins an epoch and sees the
//! entries that were current then. Superseded entries are dropped once
//! no pin can still reach them.
//!
//! # Retention
//!
//! Nothing is copied and no root is retained, so the arena holds the
//! live entries plus the tombstones created since the last sweep.
//! Retention is bounded by write volume between horizon advances, not
//! by how long a scan runs.
//!
//! # The one refusal
//!
//! A map holds one entry per key. When a key that is currently a
//! tombstone is inserted again while a live pin can still need that
//! tombstone, there is nowhere to put the new entry that does not
//! destroy the old one - and destroying it makes the pinned scan lose a
//! row it should have seen, silently. [`insert`](VersionedBTreeMap::insert)
//! refuses with [`VersionedError::RebornUnderPin`] rather than take
//! that trade. What lifts the restriction is the birth epoch joining
//! the key ordering, which makes versions of one key distinct entries;
//! that changes every range bound a caller writes, so it is a decision
//! for the caller and not a default.
//!
//! # Reclamation
//!
//! An insert that exhausts the node arena sweeps entries whose death
//! epoch is below the reclaim horizon and retries, so the caller sizes
//! for live entries plus what dies between sweeps rather than for every
//! entry that has ever existed. Under a live pin the horizon does not
//! move and the sweep frees nothing - a bulk rewrite performed while a
//! scan is running needs the arena to hold the old entries and the new
//! ones at once.

use std::ops::Bound;
use std::path::Path;

use crate::shared_btree_map::{BTreeError, SharedBTreeMap};
use crate::shared_epochs::{Epoch, EpochError, PinGuard, SharedEpochs};

/// The death epoch of an entry that is still current.
pub const DIED_LIVE: Epoch = Epoch::MAX;

/// A value and the epochs it was current between.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct Versioned<V: Copy + Default + 'static> {
    /// The epoch this became current.
    pub born: Epoch,
    /// The epoch it stopped being current, or [`DIED_LIVE`].
    pub died: Epoch,
    pub value: V,
}

impl<V: Copy + Default + 'static> Versioned<V> {
    #[inline]
    pub fn is_live(&self) -> bool {
        self.died == DIED_LIVE
    }

    /// Whether a reader pinned at `pin` sees this entry: it was born at
    /// or before the pin, and had not yet been superseded when the pin
    /// was taken.
    #[inline]
    pub fn visible_at(&self, pin: Epoch) -> bool {
        self.born <= pin && self.died > pin
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionedError {
    /// A key that is a tombstone a live pin can still reach was
    /// inserted again. One entry per key leaves nowhere to put the new
    /// version without destroying a row that scan must still see.
    RebornUnderPin,
    /// The node arena is full and no entry is dead enough to reclaim.
    Full,
    LayoutMismatch,
    Epochs(EpochError),
    IoError(std::io::ErrorKind),
}

impl From<BTreeError> for VersionedError {
    fn from(e: BTreeError) -> Self {
        match e {
            BTreeError::Full => VersionedError::Full,
            BTreeError::LayoutMismatch | BTreeError::InvalidConfig => {
                VersionedError::LayoutMismatch
            }
            BTreeError::IoError(k) => VersionedError::IoError(k),
        }
    }
}

impl From<EpochError> for VersionedError {
    fn from(e: EpochError) -> Self {
        VersionedError::Epochs(e)
    }
}

impl std::fmt::Display for VersionedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionedError::RebornUnderPin => write!(
                f,
                "the key is a tombstone a live scan can still reach; \
                 put the birth epoch in the key to hold both versions"
            ),
            VersionedError::Full => write!(f, "node arena full, nothing reclaimable"),
            VersionedError::LayoutMismatch => write!(f, "versioned map layout mismatch"),
            VersionedError::Epochs(e) => write!(f, "epoch table: {e}"),
            VersionedError::IoError(k) => write!(f, "versioned map io error: {k:?}"),
        }
    }
}

impl std::error::Error for VersionedError {}

/// Entries a sweep examines per pass. A sweep walks the whole key range
/// in chunks so one pass never builds an unbounded vector.
const SWEEP_CHUNK: usize = 4096;

pub struct VersionedBTreeMap<K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    tree: SharedBTreeMap<K, Versioned<V>>,
    epochs: SharedEpochs,
}

impl<K, V> VersionedBTreeMap<K, V>
where
    K: Copy + Ord + Default + 'static,
    V: Copy + Default + 'static,
{
    /// Obtain the map at `tree_path` with its epoch table at
    /// `epochs_path`, initializing either that does not yet exist.
    ///
    /// `capacity` is a NODE count, as [`SharedBTreeMap::create`] takes;
    /// `max_pins` is how many scans may hold a pin at once.
    pub fn create(
        tree_path: impl AsRef<Path>,
        capacity: usize,
        epochs_path: impl AsRef<Path>,
        max_pins: usize,
    ) -> Result<Self, VersionedError> {
        Ok(Self {
            tree: SharedBTreeMap::create(tree_path, capacity)?,
            epochs: SharedEpochs::create(epochs_path, max_pins)?,
        })
    }

    /// Attach to an existing map and its epoch table.
    pub fn open(
        tree_path: impl AsRef<Path>,
        expected_capacity: usize,
        epochs_path: impl AsRef<Path>,
        expected_pins: usize,
    ) -> Result<Self, VersionedError> {
        Ok(Self {
            tree: SharedBTreeMap::open(tree_path, expected_capacity)?,
            epochs: SharedEpochs::open(epochs_path, expected_pins)?,
        })
    }

    /// The epoch table, for a caller that reclaims or that wants the
    /// horizon.
    #[inline]
    pub fn epochs(&self) -> &SharedEpochs {
        &self.epochs
    }

    /// Pin the current epoch. Every read taken through the returned
    /// guard sees one fixed view.
    pub fn pin(&self) -> Result<PinGuard<'_>, VersionedError> {
        Ok(self.epochs.pin()?)
    }

    /// Entries the tree holds, live and tombstone alike.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Nodes the arena holds.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.tree.capacity()
    }

    /// The value current at `pin`, or `None` if the key was not yet
    /// born then or had already been superseded.
    pub fn get_at(&self, key: &K, pin: &PinGuard<'_>) -> Option<V> {
        self.tree
            .get(key)
            .filter(|e| e.visible_at(pin.epoch()))
            .map(|e| e.value)
    }

    /// The value current right now.
    pub fn get(&self, key: &K) -> Option<V> {
        self.tree.get(key).filter(Versioned::is_live).map(|e| e.value)
    }

    /// Make `key` current with `value`.
    ///
    /// Returns the value it replaced, if the key was live. A key that
    /// is a tombstone a live pin can still reach is refused with
    /// [`VersionedError::RebornUnderPin`]: one entry per key leaves
    /// nowhere to put the new version without destroying a row that
    /// scan must still see.
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>, VersionedError> {
        let born = self.epochs.advance();
        self.insert_at(key, value, born)
    }

    /// Make `key` current with `value`, stamped as born at `born` - the
    /// epoch of an [`EpochTicket`](crate::shared_epochs::EpochTicket)
    /// shared by every entry of one compound write, so a scan sees them
    /// all or none. The refusal is [`insert`](Self::insert)'s.
    pub fn insert_at(&self, key: K, value: V, born: Epoch) -> Result<Option<V>, VersionedError> {
        let existing = self.tree.get(&key);
        // Reclaimable is `died <= horizon`, because the horizon is the
        // newest epoch no reader holds. A tombstone above it is one a
        // live pin can still reach.
        if let Some(prev) = existing
            && !prev.is_live()
            && prev.died > self.epochs.reclaim_horizon()
        {
            return Err(VersionedError::RebornUnderPin);
        }
        let entry = Versioned { born, died: DIED_LIVE, value };
        let displaced = match self.tree.insert(key, entry) {
            Ok(d) => d,
            Err(BTreeError::Full) => {
                self.sweep()?;
                self.tree.insert(key, entry)?
            }
            Err(e) => return Err(e.into()),
        };
        Ok(displaced.filter(Versioned::is_live).map(|e| e.value))
    }

    /// Stamp `key` as superseded. The entry stays until no pin can
    /// reach it.
    ///
    /// Returns the value that was current, or `None` if the key was
    /// absent or already a tombstone.
    pub fn remove(&self, key: &K) -> Result<Option<V>, VersionedError> {
        let Some(prev) = self.tree.get(key) else {
            return Ok(None);
        };
        if !prev.is_live() {
            return Ok(None);
        }
        let died = self.epochs.advance();
        self.stamp_dead(key, prev, died)
    }

    /// Stamp `key` as superseded at `died`, an
    /// [`EpochTicket`](crate::shared_epochs::EpochTicket)'s epoch shared
    /// by every entry of one compound write. Otherwise as
    /// [`remove`](Self::remove).
    pub fn remove_at(&self, key: &K, died: Epoch) -> Result<Option<V>, VersionedError> {
        let Some(prev) = self.tree.get(key) else {
            return Ok(None);
        };
        if !prev.is_live() {
            return Ok(None);
        }
        self.stamp_dead(key, prev, died)
    }

    fn stamp_dead(&self, key: &K, prev: Versioned<V>, died: Epoch) -> Result<Option<V>, VersionedError> {
        let stamped = Versioned { died, ..prev };
        match self.tree.insert(*key, stamped) {
            Ok(_) => Ok(Some(prev.value)),
            // Stamping replaces a key already present, so the arena
            // cannot grow here; a Full is a torn read of a key that
            // vanished under us, and the delete has nothing to do.
            Err(BTreeError::Full) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The entries current at `pin`, in key order, at most `limit` of
    /// them.
    ///
    /// Resume from a partial result with `Bound::Excluded(last_key)`.
    /// The limit counts entries EXAMINED, not entries returned, so a
    /// range dense in tombstones can return fewer than `limit` while
    /// more remain; resume from the last key of the underlying walk,
    /// which [`range_at_with_cursor`](Self::range_at_with_cursor)
    /// reports.
    pub fn range_at(
        &self,
        low: Bound<&K>,
        high: Bound<&K>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> Vec<(K, V)> {
        self.range_at_with_cursor(low, high, limit, pin).0
    }

    /// As [`range_at`](Self::range_at), and also the last key the walk
    /// examined, so a caller can resume past the tombstones that were
    /// filtered out.
    pub fn range_at_with_cursor(
        &self,
        low: Bound<&K>,
        high: Bound<&K>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> (Vec<(K, V)>, Option<K>) {
        let raw = self.tree.range(low, high, limit);
        let cursor = raw.last().map(|(k, _)| *k);
        let at = pin.epoch();
        let out = raw
            .into_iter()
            .filter(|(_, e)| e.visible_at(at))
            .map(|(k, e)| (k, e.value))
            .collect();
        (out, cursor)
    }

    /// Drop every entry superseded below the reclaim horizon, and
    /// report how many went.
    ///
    /// Called on a full arena before an insert reports failure, and
    /// available to a caller that would rather reclaim on its own
    /// schedule. Under a live pin the horizon does not move, so a sweep
    /// during a scan frees only what died before that scan started.
    /// Undo every stamp this map holds at `epoch`: an entry born there is
    /// removed, and an entry superseded there is made current again.
    /// For an epoch whose ticket holder died mid-compound, as
    /// [`SharedEpochs::dead_tickets`] reports; once every structure
    /// sharing the table has voided the epoch, the caller frees the
    /// ticket with [`SharedEpochs::free_dead_ticket`]. Returns the
    /// entries touched.
    ///
    /// Safe to run while the ticket is still held, which is the point:
    /// nothing stamped with an unpublished epoch is visible to any pin,
    /// so undoing it races no reader.
    pub fn void_epoch(&self, epoch: Epoch) -> Result<usize, VersionedError> {
        let mut touched = 0usize;
        let mut cursor: Option<K> = None;
        loop {
            let low = match &cursor {
                Some(k) => Bound::Excluded(k),
                None => Bound::Unbounded,
            };
            let chunk = self.tree.range(low, Bound::Unbounded, SWEEP_CHUNK);
            if chunk.is_empty() {
                break;
            }
            cursor = chunk.last().map(|(k, _)| *k);
            for (k, e) in chunk {
                if e.born == epoch {
                    self.tree.remove(&k)?;
                    touched += 1;
                } else if e.died == epoch {
                    self.tree.insert(k, Versioned { died: DIED_LIVE, ..e })?;
                    touched += 1;
                }
            }
        }
        Ok(touched)
    }

    pub fn sweep(&self) -> Result<usize, VersionedError> {
        let horizon = self.epochs.reclaim_horizon();
        let mut freed = 0usize;
        let mut cursor: Option<K> = None;
        loop {
            let low = match &cursor {
                Some(k) => Bound::Excluded(k),
                None => Bound::Unbounded,
            };
            let chunk = self.tree.range(low, Bound::Unbounded, SWEEP_CHUNK);
            if chunk.is_empty() {
                break;
            }
            cursor = chunk.last().map(|(k, _)| *k);
            for (k, e) in chunk {
                if !e.is_live() && e.died <= horizon {
                    self.tree.remove(&k)?;
                    freed += 1;
                }
            }
        }
        if freed == 0 {
            return Err(VersionedError::Full);
        }
        Ok(freed)
    }

    pub fn flush(&self) -> Result<(), VersionedError> {
        self.tree.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        dir: std::path::PathBuf,
        map: VersionedBTreeMap<u64, u64>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn fixture(name: &str, capacity: usize) -> Fixture {
        let dir = std::env::temp_dir()
            .join(format!("subetha_vbt_{name}_{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let map = VersionedBTreeMap::create(
            dir.join("tree.bin"),
            capacity,
            dir.join("epochs.bin"),
            16,
        )
        .unwrap();
        Fixture { dir, map }
    }

    #[test]
    fn a_scan_pinned_before_a_delete_still_sees_the_row() {
        let f = fixture("pinned_delete", 256);
        f.map.insert(1, 10).unwrap();
        f.map.insert(2, 20).unwrap();

        let pin = f.map.pin().unwrap();
        f.map.remove(&2).unwrap();

        assert_eq!(f.map.get(&2), None, "gone for a reader with no pin");
        assert_eq!(
            f.map.get_at(&2, &pin),
            Some(20),
            "the scan pinned before the delete must still see it"
        );
        let seen = f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &pin);
        assert_eq!(seen, vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn a_scan_does_not_see_a_row_written_after_it_pinned() {
        let f = fixture("pinned_insert", 256);
        f.map.insert(1, 10).unwrap();
        let pin = f.map.pin().unwrap();
        f.map.insert(2, 20).unwrap();

        assert_eq!(f.map.get(&2), Some(20), "current for an unpinned reader");
        assert_eq!(f.map.get_at(&2, &pin), None, "not born when the scan pinned");
        assert_eq!(
            f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &pin),
            vec![(1, 10)]
        );
    }

    /// One entry per key: an update replaces the entry in place, so a
    /// pin taken before it sees neither the old value nor the new.
    #[test]
    fn an_update_replaces_the_entry_so_an_earlier_pin_sees_neither_value() {
        let f = fixture("update", 256);
        f.map.insert(1, 10).unwrap();
        let pin = f.map.pin().unwrap();
        assert_eq!(f.map.insert(1, 11).unwrap(), Some(10));

        assert_eq!(f.map.get(&1), Some(11));
        assert_eq!(
            f.map.get_at(&1, &pin),
            None,
            "the entry the pin saw was replaced in place, so its birth is above the pin"
        );
    }

    /// The refusal, and the reason for it: one entry per key means a
    /// rebirth would overwrite a tombstone a live scan still needs, and
    /// that scan would lose the row with nothing reporting it.
    #[test]
    fn reinserting_a_key_a_live_pin_still_needs_is_refused() {
        let f = fixture("reborn", 256);
        f.map.insert(1, 10).unwrap();
        let pin = f.map.pin().unwrap();
        f.map.remove(&1).unwrap();
        assert_eq!(f.map.get_at(&1, &pin), Some(10));

        assert_eq!(
            f.map.insert(1, 99).unwrap_err(),
            VersionedError::RebornUnderPin,
            "the tombstone is still reachable, so it must not be overwritten"
        );
        assert_eq!(
            f.map.get_at(&1, &pin),
            Some(10),
            "the refusal left the pinned view intact"
        );
    }

    #[test]
    fn reinserting_a_key_no_pin_can_reach_is_allowed() {
        let f = fixture("reborn_free", 256);
        f.map.insert(1, 10).unwrap();
        f.map.remove(&1).unwrap();
        // No pin is held, so the horizon has caught up and the
        // tombstone is nobody's.
        assert_eq!(f.map.insert(1, 99).unwrap(), None);
        assert_eq!(f.map.get(&1), Some(99));
    }

    #[test]
    fn a_sweep_drops_only_what_no_pin_can_reach() {
        let f = fixture("sweep", 256);
        for k in 0..8u64 {
            f.map.insert(k, k * 10).unwrap();
        }
        f.map.remove(&3).unwrap();
        let before = f.map.len();

        let pin = f.map.pin().unwrap();
        f.map.remove(&5).unwrap();
        assert_eq!(
            f.map.sweep().unwrap(),
            1,
            "key 3 died before the pin; key 5 died after it and stays"
        );
        assert_eq!(f.map.len(), before - 1);
        assert_eq!(f.map.get_at(&5, &pin), Some(50), "the pinned view is intact");
        drop(pin);

        assert_eq!(f.map.sweep().unwrap(), 1, "with the pin gone key 5 goes too");
    }

    #[test]
    fn a_sweep_with_nothing_reclaimable_reports_full() {
        let f = fixture("sweep_empty", 256);
        f.map.insert(1, 10).unwrap();
        assert_eq!(f.map.sweep().unwrap_err(), VersionedError::Full);
    }

    /// An insert that exhausts the arena reclaims and retries rather
    /// than failing while dead entries are still held.
    #[test]
    fn an_insert_that_exhausts_the_arena_sweeps_and_retries() {
        let f = fixture("exhaust", 4);
        let mut wrote = 0u64;
        // Fill until the arena is genuinely out of nodes.
        loop {
            match f.map.insert(wrote, wrote) {
                Ok(_) => wrote += 1,
                Err(VersionedError::Full) => break,
                Err(e) => panic!("unexpected {e:?}"),
            }
            assert!(wrote < 10_000, "arena never filled");
        }
        assert!(wrote > 0, "nothing was ever written");

        // Retire half of them with no pin held, so the horizon covers
        // every tombstone.
        for k in 0..wrote / 2 {
            f.map.remove(&k).unwrap();
        }
        f.map
            .insert(wrote, wrote)
            .expect("the insert must reclaim the tombstones and succeed");
        assert_eq!(f.map.get(&wrote), Some(wrote));
    }

    #[test]
    fn a_range_matches_a_std_btreemap_filtered_at_the_pin() {
        let f = fixture("oracle", 4096);
        let mut oracle = std::collections::BTreeMap::new();
        for i in 0..200u64 {
            let k = i * 3 + 7;
            f.map.insert(k, i).unwrap();
            oracle.insert(k, i);
        }
        let pin = f.map.pin().unwrap();
        // Everything after the pin is invisible to it.
        for i in 200..260u64 {
            f.map.insert(i * 3 + 7, i).unwrap();
        }
        for k in [10u64, 100, 250] {
            f.map.remove(&k).ok();
        }

        let got = f.map.range_at(Bound::Unbounded, Bound::Unbounded, 4096, &pin);
        let want: Vec<(u64, u64)> = oracle.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(got, want, "the pinned view is the store as it was at the pin");
    }

    #[test]
    fn a_chunked_resume_covers_the_same_rows_as_one_call() {
        let f = fixture("chunked", 4096);
        for i in 0..300u64 {
            f.map.insert(i, i).unwrap();
        }
        for k in (0..300u64).step_by(3) {
            f.map.remove(&k).unwrap();
        }
        let pin = f.map.pin().unwrap();

        let whole = f.map.range_at(Bound::Unbounded, Bound::Unbounded, 4096, &pin);
        let mut chunked = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let low = match &cursor {
                Some(k) => Bound::Excluded(k),
                None => Bound::Unbounded,
            };
            let (rows, next) =
                f.map.range_at_with_cursor(low, Bound::Unbounded, 7, &pin);
            chunked.extend(rows);
            match next {
                Some(k) => cursor = Some(k),
                None => break,
            }
        }
        assert_eq!(chunked, whole, "resuming past filtered tombstones loses nothing");
    }

    /// Entries stamped from one ticket are seen all or none: a pin taken
    /// while the ticket is open sees none of them (and still sees what
    /// they superseded); a pin taken after publish sees them all.
    #[test]
    fn a_compound_write_is_all_or_none_to_a_scan() {
        let f = fixture("compound", 256);
        f.map.insert(1, 10).unwrap();
        f.map.insert(2, 20).unwrap();

        let t = f.map.epochs().begin().unwrap();
        f.map.insert_at(3, 30, t.epoch()).unwrap();
        f.map.insert_at(4, 40, t.epoch()).unwrap();
        f.map.remove_at(&1, t.epoch()).unwrap();

        let mid = f.map.pin().unwrap();
        assert_eq!(
            f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &mid),
            vec![(1, 10), (2, 20)],
            "mid-write: nothing the compound did is visible, and what it removed still is"
        );
        t.publish();
        let after = f.map.pin().unwrap();
        assert_eq!(
            f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &after),
            vec![(2, 20), (3, 30), (4, 40)],
            "after publish: all of it, at once"
        );
        assert_eq!(
            f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &mid),
            vec![(1, 10), (2, 20)],
            "the earlier pin keeps its view"
        );
    }

    /// A compound write whose ticket is never published - its writer
    /// died - is undone by void_epoch: what it inserted goes, what it
    /// removed is current again, and the map reads as before it began.
    #[test]
    fn voiding_an_epoch_undoes_a_dead_compound_write() {
        let f = fixture("void", 256);
        f.map.insert(1, 10).unwrap();
        f.map.insert(2, 20).unwrap();
        let before = f.map.pin().unwrap();
        let base = f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &before);
        drop(before);

        let t = f.map.epochs().begin().unwrap();
        let e = t.epoch();
        f.map.insert_at(3, 30, e).unwrap();
        f.map.insert_at(2, 21, e).unwrap();
        f.map.remove_at(&1, e).unwrap();
        // The writer dies here: the ticket is neither published nor
        // dropped through its guard.
        std::mem::forget(t);

        assert_eq!(f.map.void_epoch(e).unwrap(), 3, "two inserts and one removal undone");
        assert_eq!(f.map.get(&3), None);
        assert_eq!(f.map.get(&1), Some(10), "the removal is undone");
        // An update stamped at e replaced key 2's live entry; voiding
        // removes the version born at e, and the key reads as absent
        // until the caller restores it from its own record - which is
        // why every structure sharing the table voids, not just this one.
        assert_eq!(f.map.get(&2), None);
        let now = f.map.pin().unwrap();
        let after = f.map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &now);
        assert_eq!(after, vec![(1, 10)]);
        assert_ne!(after, base, "the overwritten value is the caller's to restore");
    }
}
