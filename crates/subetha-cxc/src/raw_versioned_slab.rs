//! The versioned slab with its value size and chain depth given at run
//! time, for callers that cannot name them in the type system.
//!
//! [`SharedVersionedSlab`](crate::shared_versioned_slab::SharedVersionedSlab)
//! fixes both as generic parameters, which suits Rust callers and shuts
//! out every binding that reaches this crate through C. This form carries
//! the same MVCC contract over [`RawSlab`]: newest version first, each
//! superseded exactly at its successor's birth, a reader pinned at an
//! epoch seeing what was current then, and a push on a full chain first
//! dropping what no pin can reach.
//!
//! A chain is laid out as a length and then `depth` versions, each a birth
//! epoch, a death epoch and the value's bytes. Nothing in it is read
//! through a typed pointer, so a value size that leaves the epochs
//! unaligned costs nothing but is still addressed correctly.

use std::path::Path;

use crate::raw_slab::{raw_slab_file_size, RawSlab};
use crate::raw_treiber_stack::ElementLayout;
use crate::shared_epochs::{Epoch, PinGuard, SharedEpochs};
use crate::shared_versioned_slab::VersionedSlabError;
use crate::versioned_btree_map::DIED_LIVE;

/// Bytes a version's two epochs take, ahead of its value.
const EPOCHS_BYTES: usize = 16;
/// Bytes the chain's length takes, ahead of the versions.
const LEN_BYTES: usize = 8;
/// Distinguishes a runtime-sized chain region from a typed one built at
/// the same slot size, so the two can never attach to each other's file.
const RAW_VERSIONED_SLAB_TAG: u64 = 0x5241_565F_534C_4142;

/// A value size and a chain depth, which together fix the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedSlabLayout {
    /// Bytes one version's value takes.
    pub value_size: usize,
    /// Versions one slot keeps before a push has to drop one.
    pub depth: usize,
}

impl VersionedSlabLayout {
    /// Bytes one version takes: its two epochs and its value.
    pub const fn version_stride(&self) -> usize {
        EPOCHS_BYTES + self.value_size
    }

    /// The slot the slab stores, which is the whole chain.
    pub const fn slot_size(&self) -> usize {
        LEN_BYTES + self.depth * self.version_stride()
    }

    /// The element layout this chain attaches with. The tag carries the
    /// depth as well as the marker, so two slabs that agree on slot size
    /// through different value-size and depth pairs do not attach to one
    /// another.
    pub const fn element(&self) -> ElementLayout {
        ElementLayout {
            slot_size: self.slot_size(),
            alignment: 8,
            tag: RAW_VERSIONED_SLAB_TAG ^ (self.depth as u64) << 32 ^ self.value_size as u64,
        }
    }
}

/// Bytes the slab's file holds for `capacity` chains of `layout`.
pub fn raw_versioned_slab_file_size(capacity: usize, layout: &VersionedSlabLayout) -> usize {
    raw_slab_file_size(capacity, &layout.element())
}

/// One version read out of a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawSlotVersion {
    /// The epoch this became current.
    pub born: Epoch,
    /// The epoch it stopped being current, or [`DIED_LIVE`].
    pub died: Epoch,
    /// Where its value sits in the chain buffer.
    pub value_at: usize,
}

impl RawSlotVersion {
    /// Whether this is the version a reader with no pin sees.
    pub fn is_live(&self) -> bool {
        self.died == DIED_LIVE
    }

    /// Whether a reader pinned at `pin` sees this version.
    pub fn visible_at(&self, pin: Epoch) -> bool {
        self.born <= pin && pin < self.died
    }
}

pub struct RawVersionedSlab {
    slab: RawSlab,
    epochs: SharedEpochs,
    layout: VersionedSlabLayout,
}

impl RawVersionedSlab {
    /// Obtain the slab at `slab_path` with its epoch table at
    /// `epochs_path`, initializing either that does not yet exist.
    /// `epochs_path` may be the table the store's other structures share.
    /// `max_pins` is how many scans may hold a pin at once.
    pub fn create(
        slab_path: impl AsRef<Path>,
        capacity: usize,
        layout: VersionedSlabLayout,
        epochs_path: impl AsRef<Path>,
        max_pins: usize,
    ) -> Result<Self, VersionedSlabError> {
        let slab = RawSlab::create(slab_path, capacity, layout.element())?;
        let epochs = SharedEpochs::create(epochs_path, max_pins)?;
        Ok(Self { slab, epochs, layout })
    }

    /// Attach to the slab and epoch table another process created.
    pub fn open(
        slab_path: impl AsRef<Path>,
        capacity: usize,
        layout: VersionedSlabLayout,
        epochs_path: impl AsRef<Path>,
        max_pins: usize,
    ) -> Result<Self, VersionedSlabError> {
        let slab = RawSlab::open(slab_path, capacity, layout.element())?;
        let epochs = SharedEpochs::open(epochs_path, max_pins)?;
        Ok(Self { slab, epochs, layout })
    }

    /// The epoch table this slab stamps from.
    pub fn epochs(&self) -> &SharedEpochs {
        &self.epochs
    }

    /// Pin the published epoch. Every read through the guard sees one
    /// consistent moment.
    pub fn pin(&self) -> Result<PinGuard<'_>, VersionedSlabError> {
        Ok(self.epochs.pin()?)
    }

    /// Slots the slab addresses.
    pub fn capacity(&self) -> usize {
        self.slab.capacity()
    }

    /// The value size and depth this handle opened the region with.
    pub fn layout(&self) -> VersionedSlabLayout {
        self.layout
    }

    /// A buffer one chain fits in, for the read and write forms below.
    pub fn chain_buffer(&self) -> Vec<u8> {
        vec![0u8; self.layout.slot_size()]
    }

    fn chain_len(&self, buf: &[u8]) -> usize {
        let mut n = [0u8; 4];
        n.copy_from_slice(&buf[..4]);
        (u32::from_le_bytes(n) as usize).min(self.layout.depth)
    }

    fn set_chain_len(&self, buf: &mut [u8], len: usize) {
        buf[..4].copy_from_slice(&(len as u32).to_le_bytes());
    }

    fn version_at(&self, buf: &[u8], j: usize) -> RawSlotVersion {
        let at = LEN_BYTES + j * self.layout.version_stride();
        let mut born = [0u8; 8];
        let mut died = [0u8; 8];
        born.copy_from_slice(&buf[at..at + 8]);
        died.copy_from_slice(&buf[at + 8..at + 16]);
        RawSlotVersion {
            born: Epoch::from_le_bytes(born),
            died: Epoch::from_le_bytes(died),
            value_at: at + EPOCHS_BYTES,
        }
    }

    fn write_epochs(&self, buf: &mut [u8], j: usize, born: Epoch, died: Epoch) {
        let at = LEN_BYTES + j * self.layout.version_stride();
        buf[at..at + 8].copy_from_slice(&born.to_le_bytes());
        buf[at + 8..at + 16].copy_from_slice(&died.to_le_bytes());
    }

    /// Move version `from` to `to` inside one chain buffer.
    fn move_version(&self, buf: &mut [u8], from: usize, to: usize) {
        if from == to {
            return;
        }
        let stride = self.layout.version_stride();
        let src = LEN_BYTES + from * stride;
        let dst = LEN_BYTES + to * stride;
        buf.copy_within(src..src + stride, dst);
    }

    /// Read the whole chain at `i` into `buf`, newest first, and report
    /// how many versions it holds.
    pub fn read_chain(&self, i: usize, buf: &mut [u8]) -> Result<usize, VersionedSlabError> {
        self.slab.get(i, buf)?;
        Ok(self.chain_len(buf))
    }

    /// The version at index `j` of a chain already read into `buf`.
    pub fn version(&self, buf: &[u8], j: usize) -> RawSlotVersion {
        self.version_at(buf, j)
    }

    /// The current value at `i` into `out`, or false when the slot has no
    /// live version.
    pub fn get(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError> {
        let mut buf = self.chain_buffer();
        let len = self.read_chain(i, &mut buf)?;
        if len == 0 {
            return Ok(false);
        }
        let head = self.version_at(&buf, 0);
        if !head.is_live() {
            return Ok(false);
        }
        out[..self.layout.value_size]
            .copy_from_slice(&buf[head.value_at..head.value_at + self.layout.value_size]);
        Ok(true)
    }

    /// The value at `i` a reader pinned at `pin` sees, into `out`, or
    /// false when no version was current then.
    pub fn get_at(
        &self,
        i: usize,
        pin: &PinGuard<'_>,
        out: &mut [u8],
    ) -> Result<bool, VersionedSlabError> {
        let mut buf = self.chain_buffer();
        let len = self.read_chain(i, &mut buf)?;
        for j in 0..len {
            let v = self.version_at(&buf, j);
            if v.visible_at(pin.epoch()) {
                out[..self.layout.value_size]
                    .copy_from_slice(&buf[v.value_at..v.value_at + self.layout.value_size]);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Drop every version in `buf` that no reader at or past `horizon` can
    /// reach, keeping the newest live one; returns how many went.
    fn sweep_buffer(&self, buf: &mut [u8], horizon: Epoch) -> usize {
        let len = self.chain_len(buf);
        let mut kept = 0usize;
        let mut dropped = 0usize;
        for j in 0..len {
            let v = self.version_at(buf, j);
            // A version still current, or one a reader at the horizon can
            // still be inside, has to stay.
            if v.is_live() || v.died > horizon {
                self.move_version(buf, j, kept);
                kept += 1;
            } else {
                dropped += 1;
            }
        }
        if dropped > 0 {
            self.set_chain_len(buf, kept);
        }
        dropped
    }

    /// Make `value` the current version at `i`, born at `born`; the
    /// previous head, if live, is superseded at the same epoch. A full
    /// chain first drops every version no pin can reach; if every version
    /// is still pinned, [`VersionedSlabError::Pinned`].
    pub fn set_at(
        &self,
        i: usize,
        value: &[u8],
        born: Epoch,
    ) -> Result<(), VersionedSlabError> {
        if value.len() != self.layout.value_size {
            return Err(VersionedSlabError::Slab(crate::shared_slab::SlabError::PayloadTooLarge));
        }
        let mut buf = self.chain_buffer();
        let mut len = self.read_chain(i, &mut buf)?;
        if len > 0 {
            let head = self.version_at(&buf, 0);
            if head.is_live() {
                self.write_epochs(&mut buf, 0, head.born, born);
            }
        }
        if len >= self.layout.depth {
            self.sweep_buffer(&mut buf, self.epochs.reclaim_horizon());
            len = self.chain_len(&buf);
            if len >= self.layout.depth {
                return Err(VersionedSlabError::Pinned);
            }
        }
        // Newest first: everything shifts back one and the new version
        // takes the head.
        let stride = self.layout.version_stride();
        if len > 0 {
            buf.copy_within(LEN_BYTES..LEN_BYTES + len * stride, LEN_BYTES + stride);
        }
        self.write_epochs(&mut buf, 0, born, DIED_LIVE);
        let at = LEN_BYTES + EPOCHS_BYTES;
        buf[at..at + self.layout.value_size].copy_from_slice(value);
        self.set_chain_len(&mut buf, len + 1);
        self.slab.set(i, &buf)?;
        Ok(())
    }

    /// Make `value` the current version at `i` at a fresh epoch.
    pub fn set(&self, i: usize, value: &[u8]) -> Result<(), VersionedSlabError> {
        let born = self.epochs.advance();
        self.set_at(i, value, born)
    }

    /// Supersede the current version at `i` at `died`, leaving the slot
    /// with no live version. Writes what was current into `out` and
    /// reports whether there was one.
    pub fn retire_at(
        &self,
        i: usize,
        died: Epoch,
        out: &mut [u8],
    ) -> Result<bool, VersionedSlabError> {
        let mut buf = self.chain_buffer();
        let len = self.read_chain(i, &mut buf)?;
        if len == 0 {
            return Ok(false);
        }
        let head = self.version_at(&buf, 0);
        if !head.is_live() {
            return Ok(false);
        }
        out[..self.layout.value_size]
            .copy_from_slice(&buf[head.value_at..head.value_at + self.layout.value_size]);
        self.write_epochs(&mut buf, 0, head.born, died);
        self.slab.set(i, &buf)?;
        Ok(true)
    }

    /// Supersede the current version at `i` at a fresh epoch.
    pub fn retire(&self, i: usize, out: &mut [u8]) -> Result<bool, VersionedSlabError> {
        let died = self.epochs.advance();
        self.retire_at(i, died, out)
    }

    /// Drop every version at `i` that no pin can reach; returns how many
    /// went. A push does this itself on a full chain; this is for a caller
    /// reclaiming on its own schedule.
    pub fn sweep_slot(&self, i: usize) -> Result<usize, VersionedSlabError> {
        let mut buf = self.chain_buffer();
        self.read_chain(i, &mut buf)?;
        let dropped = self.sweep_buffer(&mut buf, self.epochs.reclaim_horizon());
        if dropped > 0 {
            self.slab.set(i, &buf)?;
        }
        Ok(dropped)
    }

    /// Undo every stamp this slab holds at `epoch`: a version born there
    /// goes, and a version superseded there is current again. For an epoch
    /// whose ticket holder died mid-compound. Returns the versions touched.
    pub fn void_epoch(&self, epoch: Epoch) -> Result<usize, VersionedSlabError> {
        let mut touched = 0usize;
        let mut buf = self.chain_buffer();
        for i in 0..self.slab.capacity() {
            let len = self.read_chain(i, &mut buf)?;
            if len == 0 {
                continue;
            }
            let mut changed = false;
            let mut kept = 0usize;
            for j in 0..len {
                if self.version_at(&buf, j).born == epoch {
                    changed = true;
                    touched += 1;
                } else {
                    self.move_version(&mut buf, j, kept);
                    kept += 1;
                }
            }
            self.set_chain_len(&mut buf, kept);
            for j in 0..kept {
                let v = self.version_at(&buf, j);
                if v.died == epoch {
                    self.write_epochs(&mut buf, j, v.born, DIED_LIVE);
                    changed = true;
                    touched += 1;
                }
            }
            if changed {
                self.slab.set(i, &buf)?;
            }
        }
        Ok(touched)
    }

    /// Push the slab's dirty pages to disk, returning when they are
    /// durable.
    pub fn flush(&self) -> Result<(), VersionedSlabError> {
        self.slab.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> VersionedSlabLayout {
        VersionedSlabLayout { value_size: 8, depth: 4 }
    }

    /// A fresh directory for one test: whatever an earlier run left there
    /// is removed, and a removal refused for any reason but absence fails
    /// the test rather than gating it on stale files.
    fn fresh_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("subetha_rawvslab_{name}_{}", std::process::id()));
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("stale directory {} not removed: {e}", dir.display()),
        }
        std::fs::create_dir_all(&dir).expect("the test directory is created");
        dir
    }

    fn slab(name: &str) -> RawVersionedSlab {
        let dir = fresh_dir(name);
        RawVersionedSlab::create(
            dir.join("chains.slab"),
            8,
            layout(),
            dir.join("table.epochs"),
            8,
        )
        .expect("the slab is created")
    }

    #[test]
    fn a_reader_pinned_before_a_write_still_sees_what_was_current() {
        let s = slab("pinned");
        s.set(0, &1u64.to_le_bytes()).expect("the first version lands");

        let pin = s.pin().expect("a pin");
        s.set(0, &2u64.to_le_bytes()).expect("the second version lands");

        let mut out = [0u8; 8];
        assert!(s.get_at(0, &pin, &mut out).expect("the pinned read runs"));
        assert_eq!(u64::from_le_bytes(out), 1, "the pin holds the older version");

        assert!(s.get(0, &mut out).expect("the live read runs"));
        assert_eq!(u64::from_le_bytes(out), 2, "an unpinned read sees the newer");
    }

    #[test]
    fn a_full_chain_with_nothing_reclaimable_refuses_the_push() {
        let s = slab("full");
        let pin = s.pin().expect("a pin");
        for v in 0..layout().depth as u64 {
            s.set(0, &v.to_le_bytes()).expect("a version lands");
        }
        assert_eq!(
            s.set(0, &99u64.to_le_bytes()).unwrap_err(),
            VersionedSlabError::Pinned,
            "every version is still reachable by the pin"
        );
        drop(pin);
        s.set(0, &99u64.to_le_bytes()).expect("the pin is gone, so the sweep makes room");
    }

    #[test]
    fn a_retired_slot_has_no_live_version_and_reports_what_went() {
        let s = slab("retire");
        s.set(3, &7u64.to_le_bytes()).expect("the version lands");
        let mut out = [0u8; 8];
        assert!(s.retire(3, &mut out).expect("the retire runs"));
        assert_eq!(u64::from_le_bytes(out), 7, "the retire reports what was current");
        assert!(!s.get(3, &mut out).expect("the read runs"), "nothing is live now");
    }

    #[test]
    fn voiding_an_epoch_drops_what_was_born_there_and_revives_what_it_superseded() {
        let s = slab("void");
        s.set(1, &10u64.to_le_bytes()).expect("the first version lands");
        let born = s.epochs().advance();
        s.set_at(1, &20u64.to_le_bytes(), born).expect("the second version lands");

        assert!(s.void_epoch(born).expect("the void runs") >= 1);
        let mut out = [0u8; 8];
        assert!(s.get(1, &mut out).expect("the read runs"));
        assert_eq!(u64::from_le_bytes(out), 10, "the superseded version is current again");
    }
}
