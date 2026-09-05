//! `SharedVersionedSlab<T, D>` - a slab whose slots each hold a chain of
//! versions, so a pinned scan reads the record version it pinned.
//!
//! # What it is
//!
//! A [`SharedSlab`] of [`VersionChain<T, D>`] beside a [`SharedEpochs`].
//! A slot holds up to `D` versions of its record, newest first, each
//! stamped with the epoch it became current and the epoch it stopped
//! being current. [`set_at`](SharedVersionedSlab::set_at) pushes a
//! version and supersedes the previous head at the same epoch;
//! [`retire_at`](SharedVersionedSlab::retire_at) supersedes the head
//! without a successor; [`get_at`](SharedVersionedSlab::get_at) returns
//! the version a pin sees. The epoch table is the one the store's other
//! structures share, so records and indexes reclaim under one horizon.
//!
//! # The chain is the cap, and a full chain sweeps
//!
//! `D` is the caller's, chosen at the type. A push into a full chain
//! first drops every version no pin can reach - those superseded at or
//! below the reclaim horizon - and pushes into the room that makes. Only
//! when every version in the slot is still reachable by a live pin does
//! the push refuse, by name, with
//! [`VersionedSlabError::Pinned`]: there is nothing to drop that some
//! scan does not still need, and dropping it anyway would be a row lost
//! with nothing reporting it.
//!
//! # One writer per slot
//!
//! A push is a read-modify-write of the slot under the slab's SeqLock,
//! so two writers on one slot lose an update to each other exactly as
//! they would on [`SharedSlab`]. Readers on any number of slots run
//! beside the writer and never see a half-written chain.

use std::path::Path;

use crate::shared_epochs::{Epoch, EpochError, PinGuard, SharedEpochs};
use crate::shared_slab::{SharedSlab, SlabError};
use crate::versioned_btree_map::DIED_LIVE;

/// One version of a record: the epoch it became current, the epoch it
/// stopped being current (`DIED_LIVE` while it is), and the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SlotVersion<T: Copy + 'static> {
    pub born: Epoch,
    pub died: Epoch,
    pub value: T,
}

impl<T: Copy + 'static> SlotVersion<T> {
    #[inline]
    pub fn is_live(&self) -> bool {
        self.died == DIED_LIVE
    }

    /// Whether a reader pinned at `pin` sees this version:
    /// `born <= pin < died`.
    #[inline]
    pub fn visible_at(&self, pin: Epoch) -> bool {
        self.born <= pin && pin < self.died
    }
}

/// One slot's chain: `len` versions, newest at index 0. Entries past
/// `len` are whatever bytes the slot last held there and are never
/// read; a slot nothing has written has `len` 0.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct VersionChain<T: Copy + 'static, const D: usize> {
    pub len: u32,
    _pad: u32,
    pub versions: [SlotVersion<T>; D],
}

impl<T: Copy + 'static, const D: usize> VersionChain<T, D> {
    /// The versions the slot holds, newest first.
    #[inline]
    pub fn chain(&self) -> &[SlotVersion<T>] {
        &self.versions[..(self.len as usize).min(D)]
    }

    /// The version a reader pinned at `pin` sees, if any.
    #[inline]
    pub fn visible_at(&self, pin: Epoch) -> Option<T> {
        self.chain().iter().find(|v| v.visible_at(pin)).map(|v| v.value)
    }

    /// The current version, if the slot has a live one.
    #[inline]
    pub fn live(&self) -> Option<T> {
        self.chain().first().filter(|v| v.is_live()).map(|v| v.value)
    }

    /// Drop every version superseded at or below `horizon`; returns how
    /// many went.
    fn sweep(&mut self, horizon: Epoch) -> usize {
        let len = (self.len as usize).min(D);
        let mut kept = 0usize;
        for i in 0..len {
            let v = self.versions[i];
            if v.is_live() || v.died > horizon {
                self.versions[kept] = v;
                kept += 1;
            }
        }
        self.len = kept as u32;
        len - kept
    }

    /// Push `v` as the new head, shifting the rest down. The caller has
    /// made room.
    fn push(&mut self, v: SlotVersion<T>) {
        let len = (self.len as usize).min(D);
        debug_assert!(len < D, "push into a full chain");
        for i in (1..=len).rev() {
            self.versions[i] = self.versions[i - 1];
        }
        self.versions[0] = v;
        self.len = (len + 1) as u32;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionedSlabError {
    /// The slot's chain is full and every version in it is still
    /// reachable by a live pin, so none can be dropped to make room.
    Pinned,
    Slab(SlabError),
    Epochs(EpochError),
}

impl From<SlabError> for VersionedSlabError {
    fn from(e: SlabError) -> Self {
        VersionedSlabError::Slab(e)
    }
}

impl From<EpochError> for VersionedSlabError {
    fn from(e: EpochError) -> Self {
        VersionedSlabError::Epochs(e)
    }
}

impl std::fmt::Display for VersionedSlabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionedSlabError::Pinned => write!(
                f,
                "every version in the slot is still pinned; nothing can be dropped to make room"
            ),
            VersionedSlabError::Slab(e) => write!(f, "slab: {e}"),
            VersionedSlabError::Epochs(e) => write!(f, "epoch table: {e}"),
        }
    }
}

impl std::error::Error for VersionedSlabError {}

pub struct SharedVersionedSlab<T: Copy + 'static, const D: usize> {
    slab: SharedSlab<VersionChain<T, D>>,
    epochs: SharedEpochs,
}

impl<T: Copy + 'static, const D: usize> SharedVersionedSlab<T, D> {
    /// Obtain the slab at `slab_path` with its epoch table at
    /// `epochs_path`, initializing either that does not yet exist.
    /// `epochs_path` may be the table the store's other structures
    /// share. `max_pins` is how many scans may hold a pin at once.
    pub fn create(
        slab_path: impl AsRef<Path>,
        capacity: usize,
        epochs_path: impl AsRef<Path>,
        max_pins: usize,
    ) -> Result<Self, VersionedSlabError> {
        const { assert!(D >= 1, "a chain holds at least one version") };
        Ok(Self {
            slab: SharedSlab::create(slab_path, capacity)?,
            epochs: SharedEpochs::create(epochs_path, max_pins)?,
        })
    }

    /// Attach to an existing slab and its epoch table.
    pub fn open(
        slab_path: impl AsRef<Path>,
        expected_capacity: usize,
        epochs_path: impl AsRef<Path>,
        expected_pins: usize,
    ) -> Result<Self, VersionedSlabError> {
        Ok(Self {
            slab: SharedSlab::open(slab_path, expected_capacity)?,
            epochs: SharedEpochs::open(epochs_path, expected_pins)?,
        })
    }

    /// The epoch table, for tickets, pins and the horizon.
    #[inline]
    pub fn epochs(&self) -> &SharedEpochs {
        &self.epochs
    }

    /// Pin the published epoch. Every read through the guard sees one
    /// fixed view.
    pub fn pin(&self) -> Result<PinGuard<'_>, VersionedSlabError> {
        Ok(self.epochs.pin()?)
    }

    /// Slots the slab addresses.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slab.capacity()
    }

    /// The current version at `i`, if the slot has a live one.
    pub fn get(&self, i: usize) -> Result<Option<T>, VersionedSlabError> {
        Ok(self.slab.get(i)?.live())
    }

    /// The version at `i` a reader pinned at `pin` sees, if any.
    pub fn get_at(&self, i: usize, pin: &PinGuard<'_>) -> Result<Option<T>, VersionedSlabError> {
        Ok(self.slab.get(i)?.visible_at(pin.epoch()))
    }

    /// The whole chain at `i`, newest first.
    pub fn chain(&self, i: usize) -> Result<Vec<SlotVersion<T>>, VersionedSlabError> {
        Ok(self.slab.get(i)?.chain().to_vec())
    }

    /// Make `value` the current version at `i`, born at `born`; the
    /// previous head, if live, is superseded at the same epoch. A full
    /// chain first drops every version no pin can reach; if every
    /// version is still pinned, [`VersionedSlabError::Pinned`].
    pub fn set_at(&self, i: usize, value: T, born: Epoch) -> Result<(), VersionedSlabError> {
        let mut slot = self.slab.get(i)?;
        if let Some(head) = slot.chain().first().copied()
            && head.is_live()
        {
            slot.versions[0] = SlotVersion { died: born, ..head };
        }
        if (slot.len as usize) >= D {
            slot.sweep(self.epochs.reclaim_horizon());
            if (slot.len as usize) >= D {
                return Err(VersionedSlabError::Pinned);
            }
        }
        slot.push(SlotVersion { born, died: DIED_LIVE, value });
        self.slab.set(i, slot)?;
        Ok(())
    }

    /// Make `value` the current version at `i` at a fresh epoch.
    pub fn set(&self, i: usize, value: T) -> Result<(), VersionedSlabError> {
        let born = self.epochs.advance();
        self.set_at(i, value, born)
    }

    /// Supersede the current version at `i` at `died`, leaving the slot
    /// with no live version. Returns the value that was current, or
    /// `None` if there was none.
    pub fn retire_at(&self, i: usize, died: Epoch) -> Result<Option<T>, VersionedSlabError> {
        let mut slot = self.slab.get(i)?;
        let Some(head) = slot.chain().first().copied() else {
            return Ok(None);
        };
        if !head.is_live() {
            return Ok(None);
        }
        slot.versions[0] = SlotVersion { died, ..head };
        self.slab.set(i, slot)?;
        Ok(Some(head.value))
    }

    /// Supersede the current version at `i` at a fresh epoch.
    pub fn retire(&self, i: usize) -> Result<Option<T>, VersionedSlabError> {
        let died = self.epochs.advance();
        self.retire_at(i, died)
    }

    /// Drop every version at `i` that no pin can reach; returns how many
    /// went. A push does this itself on a full chain; this is for a
    /// caller reclaiming on its own schedule.
    pub fn sweep_slot(&self, i: usize) -> Result<usize, VersionedSlabError> {
        let mut slot = self.slab.get(i)?;
        let dropped = slot.sweep(self.epochs.reclaim_horizon());
        if dropped > 0 {
            self.slab.set(i, slot)?;
        }
        Ok(dropped)
    }

    /// Undo every stamp this slab holds at `epoch`: a version born there
    /// goes, and a version superseded there is current again. For an
    /// epoch whose ticket holder died mid-compound, as
    /// [`SharedEpochs::dead_tickets`] reports; the caller frees the
    /// ticket once every structure has voided it. Returns the versions
    /// touched.
    pub fn void_epoch(&self, epoch: Epoch) -> Result<usize, VersionedSlabError> {
        let mut touched = 0usize;
        for i in 0..self.slab.capacity() {
            let mut slot = self.slab.get(i)?;
            let len = (slot.len as usize).min(D);
            if len == 0 {
                continue;
            }
            let mut changed = false;
            // Drop versions born at the epoch.
            let mut kept = 0usize;
            for j in 0..len {
                let v = slot.versions[j];
                if v.born == epoch {
                    changed = true;
                    touched += 1;
                } else {
                    slot.versions[kept] = v;
                    kept += 1;
                }
            }
            slot.len = kept as u32;
            // Restore versions superseded at the epoch.
            for v in &mut slot.versions[..kept] {
                if v.died == epoch {
                    v.died = DIED_LIVE;
                    changed = true;
                    touched += 1;
                }
            }
            if changed {
                self.slab.set(i, slot)?;
            }
        }
        Ok(touched)
    }

    pub fn flush(&self) -> Result<(), VersionedSlabError> {
        self.slab.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture<const D: usize> {
        dir: std::path::PathBuf,
        slab: SharedVersionedSlab<u64, D>,
    }

    impl<const D: usize> Drop for Fixture<D> {
        fn drop(&mut self) {
            // The slab is a field and drops after this body runs, so its
            // mapping is still live here; a removal that the OS refuses for
            // that reason is reported, and a panic during an unwinding test
            // would abort the run in place of its own failure message.
            if let Err(e) = std::fs::remove_dir_all(&self.dir) {
                eprintln!("fixture directory {} not removed: {e}", self.dir.display());
            }
        }
    }

    /// A fresh directory for one test: whatever an earlier run left there is
    /// removed, and a removal refused for any reason but absence fails the
    /// test rather than gating it on stale files.
    fn fresh_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("subetha_vslab_{name}_{}", std::process::id()));
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("stale directory {} not removed: {e}", dir.display()),
        }
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn fixture<const D: usize>(name: &str, capacity: usize) -> Fixture<D> {
        let dir = fresh_dir(name);
        let slab = SharedVersionedSlab::create(dir.join("slab.bin"), capacity, dir.join("epochs.bin"), 16)
            .unwrap();
        Fixture { dir, slab }
    }

    #[test]
    fn a_168_byte_record_at_depth_4_takes_a_768_byte_slot() {
        #[derive(Clone, Copy)]
        #[repr(C)]
        struct Record {
            _id: u64,
            _payload: [u8; 160],
        }
        assert_eq!(std::mem::size_of::<Record>(), 168);
        assert_eq!(crate::shared_slab::slab_slot_size::<VersionChain<Record, 4>>(), 768);
    }

    #[test]
    fn a_pinned_scan_reads_the_version_it_pinned() {
        let f = fixture::<4>("pinned", 8);
        f.slab.set(3, 100).unwrap();
        let pin = f.slab.pin().unwrap();
        f.slab.set(3, 101).unwrap();
        assert_eq!(f.slab.get(3).unwrap(), Some(101), "the current version");
        assert_eq!(f.slab.get_at(3, &pin).unwrap(), Some(100), "the version the pin saw");
        let later = f.slab.pin().unwrap();
        assert_eq!(f.slab.get_at(3, &later).unwrap(), Some(101));
    }

    #[test]
    fn a_retired_slot_is_gone_now_and_present_at_an_earlier_pin() {
        let f = fixture::<2>("retire", 8);
        f.slab.set(0, 7).unwrap();
        let pin = f.slab.pin().unwrap();
        assert_eq!(f.slab.retire(0).unwrap(), Some(7));
        assert_eq!(f.slab.get(0).unwrap(), None);
        assert_eq!(f.slab.get_at(0, &pin).unwrap(), Some(7));
        assert_eq!(f.slab.retire(0).unwrap(), None, "nothing live to retire");
    }

    /// The owner's rule: a full chain sweeps what no pin can reach and
    /// pushes into the room; it refuses only when everything is pinned.
    #[test]
    fn a_full_chain_sweeps_before_it_refuses_and_refuses_only_when_all_pinned() {
        let f = fixture::<2>("full", 8);
        f.slab.set(1, 1).unwrap();
        f.slab.set(1, 2).unwrap();
        assert_eq!(f.slab.chain(1).unwrap().len(), 2, "the chain is full");
        // No pin can reach a superseded version, so a third push sweeps
        // both: the one superseded earlier and the head it supersedes,
        // whose death epoch is the horizon itself.
        f.slab.set(1, 3).unwrap();
        let chain = f.slab.chain(1).unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].value, 3);

        // A pin holds version 3; the push of 4 supersedes it at an epoch
        // above the pin, so the chain is full with every version reachable.
        let pin = f.slab.pin().unwrap();
        f.slab.set(1, 4).unwrap();
        assert_eq!(f.slab.get_at(1, &pin).unwrap(), Some(3));
        assert_eq!(f.slab.chain(1).unwrap().len(), 2);
        assert_eq!(
            f.slab.set(1, 5).unwrap_err(),
            VersionedSlabError::Pinned,
            "nothing can be dropped while the pin can reach every version"
        );
        assert_eq!(f.slab.get(1).unwrap(), Some(4), "the refused push changed nothing");
        assert_eq!(f.slab.get_at(1, &pin).unwrap(), Some(3));
        drop(pin);
        f.slab.set(1, 5).unwrap();
        assert_eq!(f.slab.get(1).unwrap(), Some(5));
        assert_eq!(f.slab.chain(1).unwrap().len(), 1, "the release let the sweep empty the chain");
    }

    #[test]
    fn a_compound_write_across_slots_is_all_or_none_to_a_scan() {
        let f = fixture::<3>("compound", 8);
        f.slab.set(0, 10).unwrap();
        let t = f.slab.epochs().begin().unwrap();
        f.slab.set_at(0, 11, t.epoch()).unwrap();
        f.slab.set_at(1, 20, t.epoch()).unwrap();
        let mid = f.slab.pin().unwrap();
        assert_eq!(f.slab.get_at(0, &mid).unwrap(), Some(10));
        assert_eq!(f.slab.get_at(1, &mid).unwrap(), None);
        t.publish();
        let after = f.slab.pin().unwrap();
        assert_eq!(f.slab.get_at(0, &after).unwrap(), Some(11));
        assert_eq!(f.slab.get_at(1, &after).unwrap(), Some(20));
    }

    #[test]
    fn voiding_an_epoch_undoes_a_dead_compound_write() {
        let f = fixture::<3>("void", 8);
        f.slab.set(0, 10).unwrap();
        f.slab.set(2, 30).unwrap();
        let t = f.slab.epochs().begin().unwrap();
        let e = t.epoch();
        f.slab.set_at(0, 11, e).unwrap();
        f.slab.set_at(1, 20, e).unwrap();
        f.slab.retire_at(2, e).unwrap();
        std::mem::forget(t);
        assert_eq!(f.slab.void_epoch(e).unwrap(), 4, "two births, one supersession by them, one retirement");
        assert_eq!(f.slab.get(0).unwrap(), Some(10), "the overwritten version is current again");
        assert_eq!(f.slab.get(1).unwrap(), None);
        assert_eq!(f.slab.get(2).unwrap(), Some(30), "the retirement is undone");
    }

    #[test]
    fn a_second_handle_shares_the_chains_and_the_pins() {
        let f = fixture::<3>("second", 8);
        f.slab.set(5, 50).unwrap();
        let other: SharedVersionedSlab<u64, 3> =
            SharedVersionedSlab::open(f.dir.join("slab.bin"), 8, f.dir.join("epochs.bin"), 16).unwrap();
        let pin = other.pin().unwrap();
        f.slab.set(5, 51).unwrap();
        assert_eq!(other.get(5).unwrap(), Some(51));
        assert_eq!(other.get_at(5, &pin).unwrap(), Some(50));
        // The other handle's pin holds the horizon for this handle's sweep.
        assert_eq!(f.slab.sweep_slot(5).unwrap(), 0);
        drop(pin);
        assert_eq!(f.slab.sweep_slot(5).unwrap(), 1);
    }

    #[test]
    fn a_reader_never_sees_a_half_written_chain() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let dir = fresh_dir("torn");
        let slab: Arc<SharedVersionedSlab<u64, 4>> = Arc::new(
            SharedVersionedSlab::create(dir.join("slab.bin"), 4, dir.join("epochs.bin"), 16).unwrap(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let slab = Arc::clone(&slab);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    slab.set(0, n).unwrap();
                    n += 1;
                }
                n
            })
        };
        let start = std::time::Instant::now();
        let mut reads = 0u64;
        while start.elapsed() < std::time::Duration::from_millis(200) {
            let chain = slab.chain(0).unwrap();
            // Newest first, values strictly descending, each superseded
            // exactly at its successor's birth.
            for w in chain.windows(2) {
                assert!(w[0].value > w[1].value, "chain out of order: {chain:?}");
                assert_eq!(w[1].died, w[0].born, "a version is superseded at its successor's birth");
            }
            reads += 1;
        }
        stop.store(true, Ordering::Relaxed);
        let writes = writer.join().unwrap();
        assert!(reads > 0 && writes > 0);
        drop(slab);
        std::fs::remove_dir_all(&dir).expect("the slab is unmapped and its directory removable");
    }
}
