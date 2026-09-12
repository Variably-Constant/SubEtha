//! The versioned B-tree map with its key and value sizes given at run
//! time, for callers that cannot name them in the type system.
//!
//! [`VersionedBTreeMap`](crate::versioned_btree_map::VersionedBTreeMap)
//! fixes both as generic parameters, which suits Rust callers and shuts
//! out every binding that reaches this crate through C. This form carries
//! the same MVCC contract over [`RawBTreeMap`]: an entry stamped with the
//! epochs it was current between, a reader pinned at an epoch seeing what
//! was current then, and a tombstone kept until no pin can reach it.
//!
//! Keys order as unsigned bytes, which is [`RawBTreeMap`]'s order, so a
//! caller that wants numeric order stores its integers big-endian.
//!
//! One entry per key is what makes a tombstone under a live pin refuse a
//! re-insert: there is nowhere to put the new version without destroying
//! a row a scan must still see. The same shape means an update replaces
//! rather than versioning, so a reader pinned across one finds the key
//! invisible: the old value is gone and the new one is born after the
//! pin. A remove is what leaves something a pin can still reach.

use std::ops::Bound;
use std::path::Path;

use crate::raw_btree_map::RawBTreeMap;
use crate::shared_epochs::{Epoch, PinGuard, SharedEpochs};
use crate::versioned_btree_map::{VersionedError, DIED_LIVE};

/// Bytes an entry's two epochs take, ahead of its value.
const EPOCHS_BYTES: usize = 16;
/// Entries one sweep pass walks at a time, so a sweep of a large map never
/// builds an unbounded vector.
const SWEEP_CHUNK: usize = 256;
/// Distinguishes a runtime-sized versioned region from a plain raw one at
/// the same sizes, so the two never attach to each other's file.
const RAW_VERSIONED_BTREE_TAG: u64 = 0x5241_565F_4254_5245;

/// One entry a range yields: its key bytes and its value bytes.
pub type RawEntry = (Vec<u8>, Vec<u8>);

/// A range's entries and the last key its walk examined, which is where a
/// caller resumes from past the tombstones that were filtered out.
pub type RawRangePage = (Vec<RawEntry>, Option<Vec<u8>>);

/// An entry's stamps, and where its value sits in the entry buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawVersioned {
    /// The epoch this became current.
    pub born: Epoch,
    /// The epoch it stopped being current, or [`DIED_LIVE`].
    pub died: Epoch,
}

impl RawVersioned {
    /// Whether this is what a reader with no pin sees.
    pub fn is_live(&self) -> bool {
        self.died == DIED_LIVE
    }

    /// Whether a reader pinned at `pin` sees it.
    pub fn visible_at(&self, pin: Epoch) -> bool {
        self.born <= pin && self.died > pin
    }
}

pub struct RawVersionedBTreeMap {
    tree: RawBTreeMap,
    epochs: SharedEpochs,
    key_size: usize,
    value_size: usize,
}

impl RawVersionedBTreeMap {
    /// Obtain the map at `tree_path` with its epoch table at
    /// `epochs_path`, initializing either that does not yet exist.
    /// `epochs_path` may be the table the store's other structures share.
    pub fn create(
        tree_path: impl AsRef<Path>,
        capacity: usize,
        key_size: usize,
        value_size: usize,
        epochs_path: impl AsRef<Path>,
        max_pins: usize,
    ) -> Result<Self, VersionedError> {
        let tree = RawBTreeMap::create(
            tree_path,
            capacity,
            key_size,
            EPOCHS_BYTES + value_size,
            RAW_VERSIONED_BTREE_TAG,
        )?;
        let epochs = SharedEpochs::create(epochs_path, max_pins)?;
        Ok(Self { tree, epochs, key_size, value_size })
    }

    /// Attach to the map and epoch table another process created.
    pub fn open(
        tree_path: impl AsRef<Path>,
        capacity: usize,
        key_size: usize,
        value_size: usize,
        epochs_path: impl AsRef<Path>,
        max_pins: usize,
    ) -> Result<Self, VersionedError> {
        let tree = RawBTreeMap::open(
            tree_path,
            capacity,
            key_size,
            EPOCHS_BYTES + value_size,
            RAW_VERSIONED_BTREE_TAG,
        )?;
        let epochs = SharedEpochs::open(epochs_path, max_pins)?;
        Ok(Self { tree, epochs, key_size, value_size })
    }

    /// The epoch table this map stamps from.
    pub fn epochs(&self) -> &SharedEpochs {
        &self.epochs
    }

    /// Pin the published epoch. Every read through the guard sees one
    /// consistent moment.
    pub fn pin(&self) -> Result<PinGuard<'_>, VersionedError> {
        Ok(self.epochs.pin()?)
    }

    /// Bytes a key takes.
    pub fn key_size(&self) -> usize {
        self.key_size
    }

    /// Bytes a value takes.
    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// Entries the map holds, tombstones not yet reclaimed included.
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Nodes the tree addresses.
    pub fn capacity(&self) -> usize {
        self.tree.capacity()
    }

    fn stamps(entry: &[u8]) -> RawVersioned {
        let mut born = [0u8; 8];
        let mut died = [0u8; 8];
        born.copy_from_slice(&entry[..8]);
        died.copy_from_slice(&entry[8..16]);
        RawVersioned {
            born: Epoch::from_le_bytes(born),
            died: Epoch::from_le_bytes(died),
        }
    }

    fn write_entry(entry: &mut [u8], stamps: RawVersioned, value: &[u8]) {
        entry[..8].copy_from_slice(&stamps.born.to_le_bytes());
        entry[8..16].copy_from_slice(&stamps.died.to_le_bytes());
        entry[EPOCHS_BYTES..].copy_from_slice(value);
    }

    fn entry_buffer(&self) -> Vec<u8> {
        vec![0u8; EPOCHS_BYTES + self.value_size]
    }

    /// The entry stored at `key`, if the tree has one.
    fn entry(&self, key: &[u8]) -> Result<Option<Vec<u8>>, VersionedError> {
        let mut buf = self.entry_buffer();
        if self.tree.get(key, &mut buf)? {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    /// The value current at `key` into `out`, or false when the key is
    /// absent or a tombstone.
    pub fn get(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError> {
        let Some(entry) = self.entry(key)? else {
            return Ok(false);
        };
        if !Self::stamps(&entry).is_live() {
            return Ok(false);
        }
        out[..self.value_size].copy_from_slice(&entry[EPOCHS_BYTES..]);
        Ok(true)
    }

    /// The value a reader pinned at `pin` sees at `key`, into `out`.
    pub fn get_at(
        &self,
        key: &[u8],
        pin: &PinGuard<'_>,
        out: &mut [u8],
    ) -> Result<bool, VersionedError> {
        let Some(entry) = self.entry(key)? else {
            return Ok(false);
        };
        if !Self::stamps(&entry).visible_at(pin.epoch()) {
            return Ok(false);
        }
        out[..self.value_size].copy_from_slice(&entry[EPOCHS_BYTES..]);
        Ok(true)
    }

    /// Make `value` current at `key`, born at `born`. A key that is a
    /// tombstone a live pin can still reach is refused with
    /// [`VersionedError::RebornUnderPin`].
    pub fn insert_at(
        &self,
        key: &[u8],
        value: &[u8],
        born: Epoch,
    ) -> Result<(), VersionedError> {
        if value.len() != self.value_size {
            return Err(VersionedError::LayoutMismatch);
        }
        // Reclaimable is `died <= horizon`, because the horizon is the
        // newest epoch no reader holds. A tombstone above it is one a live
        // pin can still reach.
        if let Some(previous) = self.entry(key)? {
            let stamps = Self::stamps(&previous);
            if !stamps.is_live() && stamps.died > self.epochs.reclaim_horizon() {
                return Err(VersionedError::RebornUnderPin);
            }
        }
        let mut entry = self.entry_buffer();
        Self::write_entry(&mut entry, RawVersioned { born, died: DIED_LIVE }, value);
        match self.tree.insert(key, &entry, None) {
            Ok(_) => Ok(()),
            Err(crate::shared_btree_map::BTreeError::Full) => {
                self.sweep()?;
                self.tree.insert(key, &entry, None)?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Make `value` current at `key` at a fresh epoch.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), VersionedError> {
        let born = self.epochs.advance();
        self.insert_at(key, value, born)
    }

    /// Stamp `key` as superseded at `died`, writing what was current into
    /// `out`. The entry stays until no pin can reach it. False when the
    /// key was absent or already a tombstone.
    pub fn remove_at(
        &self,
        key: &[u8],
        died: Epoch,
        out: &mut [u8],
    ) -> Result<bool, VersionedError> {
        let Some(entry) = self.entry(key)? else {
            return Ok(false);
        };
        let stamps = Self::stamps(&entry);
        if !stamps.is_live() {
            return Ok(false);
        }
        out[..self.value_size].copy_from_slice(&entry[EPOCHS_BYTES..]);
        let mut stamped = entry.clone();
        stamped[8..16].copy_from_slice(&died.to_le_bytes());
        match self.tree.insert(key, &stamped, None) {
            Ok(_) => Ok(true),
            // Stamping replaces a key already present, so the arena cannot
            // grow here; a Full is a torn read of a key that vanished
            // under us, and the delete has nothing to do.
            Err(crate::shared_btree_map::BTreeError::Full) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Stamp `key` as superseded at a fresh epoch.
    pub fn remove(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError> {
        let died = self.epochs.advance();
        self.remove_at(key, died, out)
    }

    /// The entries current at `pin`, in key order, at most `limit` of them.
    ///
    /// The limit counts entries examined, not entries returned, so a range
    /// dense in tombstones can return fewer than `limit` while more
    /// remain; resume from the last key the walk examined, which
    /// [`range_at_with_cursor`](Self::range_at_with_cursor) reports.
    pub fn range_at(
        &self,
        low: Bound<&[u8]>,
        high: Bound<&[u8]>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> Vec<RawEntry> {
        self.range_at_with_cursor(low, high, limit, pin).0
    }

    /// As [`range_at`](Self::range_at), and also the last key the walk
    /// examined, so a caller can resume past the tombstones filtered out.
    pub fn range_at_with_cursor(
        &self,
        low: Bound<&[u8]>,
        high: Bound<&[u8]>,
        limit: usize,
        pin: &PinGuard<'_>,
    ) -> RawRangePage {
        let raw = self.tree.range(low, high, limit);
        let cursor = raw.last().map(|(k, _)| k.clone());
        let visible = raw
            .into_iter()
            .filter(|(_, entry)| Self::stamps(entry).visible_at(pin.epoch()))
            .map(|(k, entry)| (k, entry[EPOCHS_BYTES..].to_vec()))
            .collect();
        (visible, cursor)
    }

    /// Drop every tombstone no pin can reach; returns how many went.
    /// `VersionedError::Full` when none could go, which is what an insert
    /// that ran out of nodes reports after trying this.
    pub fn sweep(&self) -> Result<usize, VersionedError> {
        let horizon = self.epochs.reclaim_horizon();
        let mut freed = 0usize;
        let mut cursor: Option<Vec<u8>> = None;
        let mut discard = self.entry_buffer();
        loop {
            let low = match &cursor {
                Some(k) => Bound::Excluded(k.as_slice()),
                None => Bound::Unbounded,
            };
            let chunk = self.tree.range(low, Bound::Unbounded, SWEEP_CHUNK);
            if chunk.is_empty() {
                break;
            }
            cursor = chunk.last().map(|(k, _)| k.clone());
            for (k, entry) in chunk {
                let stamps = Self::stamps(&entry);
                if !stamps.is_live() && stamps.died <= horizon {
                    self.tree.remove(&k, &mut discard)?;
                    freed += 1;
                }
            }
        }
        if freed == 0 {
            return Err(VersionedError::Full);
        }
        Ok(freed)
    }

    /// Undo every stamp this map holds at `epoch`: an entry born there
    /// goes, and one superseded there is current again. Returns the
    /// entries touched.
    pub fn void_epoch(&self, epoch: Epoch) -> Result<usize, VersionedError> {
        let mut touched = 0usize;
        let mut cursor: Option<Vec<u8>> = None;
        let mut discard = self.entry_buffer();
        loop {
            let low = match &cursor {
                Some(k) => Bound::Excluded(k.as_slice()),
                None => Bound::Unbounded,
            };
            let chunk = self.tree.range(low, Bound::Unbounded, SWEEP_CHUNK);
            if chunk.is_empty() {
                break;
            }
            cursor = chunk.last().map(|(k, _)| k.clone());
            for (k, entry) in chunk {
                let stamps = Self::stamps(&entry);
                if stamps.born == epoch {
                    self.tree.remove(&k, &mut discard)?;
                    touched += 1;
                } else if stamps.died == epoch {
                    let mut revived = entry.clone();
                    revived[8..16].copy_from_slice(&DIED_LIVE.to_le_bytes());
                    self.tree.insert(&k, &revived, None)?;
                    touched += 1;
                }
            }
        }
        Ok(touched)
    }

    /// Push the map's dirty pages to disk, returning when they are
    /// durable.
    pub fn flush(&self) -> Result<(), VersionedError> {
        self.tree.flush()?;
        Ok(())
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
            .join(format!("subetha_rawvbtree_{name}_{}", std::process::id()));
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("stale directory {} not removed: {e}", dir.display()),
        }
        std::fs::create_dir_all(&dir).expect("the test directory is created");
        dir
    }

    fn map(name: &str) -> RawVersionedBTreeMap {
        let dir = fresh_dir(name);
        RawVersionedBTreeMap::create(dir.join("tree.bin"), 256, 4, 8, dir.join("table.epochs"), 8)
            .expect("the map is created")
    }

    fn key(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }

    #[test]
    fn a_reader_pinned_before_a_remove_still_sees_what_the_tombstone_covers() {
        let m = map("pinned");
        m.insert(&key(1), &1u64.to_le_bytes()).expect("the value lands");

        let pin = m.pin().expect("a pin taken while the value is current");
        let mut out = [0u8; 8];
        assert!(m.remove(&key(1), &mut out).expect("the remove runs"));

        assert!(m.get_at(&key(1), &pin, &mut out).expect("the pinned read runs"));
        assert_eq!(u64::from_le_bytes(out), 1, "the tombstone still covers the pin");
        assert!(!m.get(&key(1), &mut out).expect("the live read runs"), "nothing is current");
    }

    /// One entry per key, so an update replaces rather than versioning:
    /// the old value is gone and the new one is born after the pin, which
    /// leaves the key invisible to a reader pinned across the write. Only
    /// a remove leaves something a pin can still reach.
    #[test]
    fn an_update_across_a_pin_leaves_the_key_invisible_to_it() {
        let m = map("updated");
        m.insert(&key(1), &1u64.to_le_bytes()).expect("the first value lands");

        let pin = m.pin().expect("a pin");
        m.insert(&key(1), &2u64.to_le_bytes()).expect("the second value lands");

        let mut out = [0u8; 8];
        assert!(
            !m.get_at(&key(1), &pin, &mut out).expect("the pinned read runs"),
            "the replaced value is gone and the replacement is not born at this pin"
        );
        assert!(m.get(&key(1), &mut out).expect("the live read runs"));
        assert_eq!(u64::from_le_bytes(out), 2, "an unpinned read sees the newer");
    }

    #[test]
    fn a_tombstone_a_pin_can_reach_refuses_the_reinsert() {
        let m = map("reborn");
        m.insert(&key(2), &1u64.to_le_bytes()).expect("the value lands");
        let pin = m.pin().expect("a pin");
        let mut out = [0u8; 8];
        assert!(m.remove(&key(2), &mut out).expect("the remove runs"));

        assert_eq!(
            m.insert(&key(2), &3u64.to_le_bytes()).unwrap_err(),
            VersionedError::RebornUnderPin,
            "the pin can still reach the tombstone"
        );
        drop(pin);
        m.insert(&key(2), &3u64.to_le_bytes()).expect("the pin is gone, so the key is free again");
    }

    #[test]
    fn a_range_at_a_pin_yields_the_entries_current_then_in_key_order() {
        let m = map("range");
        for i in 0..10u32 {
            m.insert(&key(i), &(i as u64).to_le_bytes()).expect("a value lands");
        }
        let pin = m.pin().expect("a pin");
        let mut out = [0u8; 8];
        m.remove(&key(4), &mut out).expect("the remove runs");

        let seen = m.range_at(Bound::Unbounded, Bound::Unbounded, 100, &pin);
        let numbers: Vec<u32> = seen
            .iter()
            .map(|(k, _)| u32::from_be_bytes(k[..].try_into().expect("a four-byte key")))
            .collect();
        assert_eq!(
            numbers,
            (0..10).collect::<Vec<_>>(),
            "the pin predates the remove, so every key is still current at it"
        );

        let after = m.pin().expect("a second pin");
        let later = m.range_at(Bound::Unbounded, Bound::Unbounded, 100, &after);
        assert_eq!(later.len(), 9, "a pin taken after the remove does not see the tombstone");
    }

    #[test]
    fn a_sweep_reclaims_only_what_no_pin_can_reach() {
        let m = map("sweep");
        m.insert(&key(5), &1u64.to_le_bytes()).expect("the value lands");
        // Pinned before the remove, so this reader is still inside the
        // window the tombstone covers. A pin taken after it could not
        // reach the value and would not hold the entry.
        let held = m.pin().expect("a pin taken while the value is current");
        let mut out = [0u8; 8];
        m.remove(&key(5), &mut out).expect("the remove runs");

        assert_eq!(
            m.sweep().unwrap_err(),
            VersionedError::Full,
            "the pin predates the death, so the tombstone still covers it"
        );
        drop(held);
        assert_eq!(m.sweep().expect("the sweep runs"), 1, "the tombstone goes once nothing holds it");
    }

    #[test]
    fn voiding_an_epoch_drops_what_was_born_there_and_revives_what_it_superseded() {
        let m = map("void");
        m.insert(&key(6), &10u64.to_le_bytes()).expect("the value lands");
        let died = m.epochs().advance();
        let mut out = [0u8; 8];
        m.remove_at(&key(6), died, &mut out).expect("the remove runs");

        assert!(m.void_epoch(died).expect("the void runs") >= 1);
        assert!(m.get(&key(6), &mut out).expect("the read runs"), "the entry is current again");
        assert_eq!(u64::from_le_bytes(out), 10);
    }
}
