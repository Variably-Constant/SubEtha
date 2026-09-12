//! The processes holding a ring, and whether the last one out removes
//! its backings.
//!
//! A ring's [`PeerDirectory`](crate::peer_directory::PeerDirectory)
//! records producer and consumer slots. It cannot answer "is anyone
//! still using this ring", because a process can hold a ring handle
//! without registering as either - an attacher that has not begun, a
//! supervisor that only reads stats, a caller between registrations.
//! So holding is tracked separately from participating.
//!
//! # Its own region
//!
//! The table lives in a region of its own beside the directory rather
//! than inside it. The directory's size is a `const fn` with
//! compile-time offsets, while the holder count is the caller's to
//! choose: the substrate has no limit on processes holding a ring, and
//! a limit it does not have is not one to invent. A variable-size table
//! belongs where variable sizing is free.
//!
//! # Layout
//!
//! ```text
//! | HoldersHeader (64B) | HolderSlot 0..N (64B each) |
//! ```
//!
//! # The last holder is the last live process
//!
//! Not the last handle within a process, and not the last registered
//! peer. A holder whose process died without releasing is reaped by the
//! same pid-liveness probe the peer slots use, so a crash cannot keep a
//! ring's files alive for ever, and a slow holder cannot have the files
//! removed under it.

use std::fs::File;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::{holder_table_size, HolderTable};

pub const HOLDERS_MAGIC: u64 = 0x5355_4245_484c_4452; // "SUBEHLDR"

/// Payload every holder slot carries. The table needs a value that is
/// neither free nor reserved, and a ring holder has nothing else to say.
const HOLDER_PRESENT: u64 = 1;

/// What becomes of a ring's backings when the last holding process
/// releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastHolder {
    /// Remove them.
    Unlink,
    /// Leave them for a later process to attach to. The default: a ring
    /// outliving its creator is the normal cross-process shape.
    Keep,
}

#[repr(C, align(64))]
pub struct HoldersHeader {
    pub magic: u64,
    /// Holder slots the table carries, checked on attach so a process
    /// opening at a different count is refused rather than reading past
    /// the region.
    pub capacity: u64,
    _pad: [u8; 48],
}

const _: () = {
    assert!(size_of::<HoldersHeader>() == 64);
};

/// Bytes the region needs for `capacity` holders.
pub const fn holders_region_size(capacity: usize) -> usize {
    size_of::<HoldersHeader>() + holder_table_size(capacity)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldersError {
    /// Every slot is held by a live process.
    Exhausted,
    /// The region was built for a different holder count.
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for HoldersError {
    fn from(e: std::io::Error) -> Self {
        HoldersError::IoError(e.kind())
    }
}

impl std::fmt::Display for HoldersError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HoldersError::Exhausted => write!(f, "every holder slot is held by a live process"),
            HoldersError::LayoutMismatch => {
                write!(f, "the holders region was built for a different holder count")
            }
            HoldersError::IoError(k) => write!(f, "holders region io: {k:?}"),
        }
    }
}

impl std::error::Error for HoldersError {}

/// One process's hold on a ring.
pub struct RingHolders {
    _file: File,
    mmap: MmapMut,
    holders: HolderTable,
    slot: usize,
    capacity: usize,
    path: PathBuf,
    on_last: LastHolder,
}

// The slots are atomics in a shared mapping; every access goes through
// an atomic, and the mapping outlives the view.
unsafe impl Send for RingHolders {}
unsafe impl Sync for RingHolders {}

impl RingHolders {
    /// Create the region if it is not there, attach if it is, and take a
    /// slot either way.
    pub fn create_or_attach(
        path: impl AsRef<Path>,
        max_holders: usize,
        on_last: LastHolder,
    ) -> Result<Self, HoldersError> {
        assert!(max_holders >= 1, "a ring needs room for at least one holder");
        let path = path.as_ref().to_path_buf();
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            &path,
            holders_region_size(max_holders),
            |ptr| unsafe { Self::init_region(ptr, max_holders) },
            |ptr| unsafe { (*(ptr as *const HoldersHeader)).magic == HOLDERS_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, HoldersError::LayoutMismatch))?;
        Self::attach(file, mmap, path, max_holders, on_last)
    }

    /// Attach to a region that must already exist.
    pub fn open(
        path: impl AsRef<Path>,
        max_holders: usize,
        on_last: LastHolder,
    ) -> Result<Self, HoldersError> {
        let path = path.as_ref().to_path_buf();
        let file = crate::region_file::open_existing(&path)?;
        let total = holders_region_size(max_holders);
        if file.metadata()?.len() < total as u64 {
            return Err(HoldersError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::attach(file, mmap, path, max_holders, on_last)
    }

    /// # Safety
    /// `ptr` addresses a zeroed region of at least
    /// `holders_region_size(capacity)` bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize) {
        let header = ptr as *mut HoldersHeader;
        unsafe {
            (*header).capacity = capacity as u64;
            // The magic is written last: a process attaching while this
            // one initializes spins on it, so it must not appear before
            // the fields behind it are there.
            std::ptr::write_volatile(std::ptr::addr_of_mut!((*header).magic), HOLDERS_MAGIC);
        }
    }

    fn attach(
        file: File,
        mmap: MmapMut,
        path: PathBuf,
        capacity: usize,
        on_last: LastHolder,
    ) -> Result<Self, HoldersError> {
        let header = unsafe { &*(mmap.as_ptr() as *const HoldersHeader) };
        if header.magic != HOLDERS_MAGIC || header.capacity != capacity as u64 {
            return Err(HoldersError::LayoutMismatch);
        }
        let holders = unsafe {
            HolderTable::from_ptr(mmap.as_ptr().add(size_of::<HoldersHeader>()), capacity)
        };
        let slot = match holders.claim(HOLDER_PRESENT) {
            Some(s) => s,
            None => {
                // A table full of processes that died is not a table
                // that is full. Reap once, then try again.
                holders.reap_dead();
                holders.claim(HOLDER_PRESENT).ok_or(HoldersError::Exhausted)?
            }
        };
        Ok(Self { _file: file, mmap, holders, slot, capacity, path, on_last })
    }

    /// Processes holding this ring, this one included.
    #[inline]
    pub fn live(&self) -> usize {
        self.holders.live()
    }

    /// Holder slots the region carries.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// What this hold does when it is the last one out.
    #[inline]
    pub fn on_last(&self) -> LastHolder {
        self.on_last
    }

    /// Free every slot whose holding process is gone, and report how
    /// many went.
    pub fn reap_dead(&self) -> usize {
        self.holders.reap_dead()
    }

    /// Give up this process's hold, and say whether the caller should
    /// now remove the ring's backings.
    ///
    /// The ring's paths are the ring's own business, so the decision is
    /// returned rather than acted on: this type knows how many hold the
    /// ring, not what removing it would mean.
    ///
    /// Idempotent, so a caller that releases early and is then dropped
    /// does not free a slot another process has since claimed.
    pub fn release(&mut self) -> bool {
        if self.slot == usize::MAX {
            return false;
        }
        self.holders.release(self.slot);
        self.slot = usize::MAX;
        if self.on_last == LastHolder::Keep {
            return false;
        }
        // Reap first, or a table of corpses keeps the backings alive
        // after the last live holder has gone.
        self.holders.reap_dead();
        self.holders.live() == 0
    }

    /// Drop the mapping and remove the holders region itself.
    ///
    /// Consumed, because the region is gone afterwards and the view must
    /// not outlive it.
    pub fn unlink_self(mut self) -> Result<(), HoldersError> {
        let path = std::mem::take(&mut self.path);
        self.release_mapping()?;
        match crate::region_file::remove(&path) {
            Ok(()) => Ok(()),
            // Another holder removed it first, which is the same outcome.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Replace the live mapping with a one-page anonymous one, so the
    /// file handle is the only thing still holding the path.
    ///
    /// Windows refuses to remove a file while a mapping is live, so a
    /// failure here means the removal that follows cannot succeed. It is
    /// returned rather than absorbed: an unlink that quietly left the
    /// region on disk would read as a ring that had been cleaned up.
    fn release_mapping(&mut self) -> Result<(), HoldersError> {
        self.mmap = MmapOptions::new().len(1).map_anon()?;
        Ok(())
    }
}

impl std::fmt::Debug for RingHolders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingHolders")
            .field("live", &self.live())
            .field("capacity", &self.capacity)
            .field("on_last", &self.on_last)
            .finish()
    }
}

impl Drop for RingHolders {
    fn drop(&mut self) {
        // A hold being dropped rather than released has no owner left to
        // act on "you are the last", so the answer is checked here and
        // not returned. A ring that wants the unlink calls `release`
        // itself while it can still reach its paths.
        let last = self.release();
        debug_assert!(
            !last || self.on_last == LastHolder::Unlink,
            "a Keep hold reported itself as the last one out",
        );
    }
}

/// The header's magic, for a reader that wants to check a region without
/// mapping it as a header.
#[inline]
pub fn magic_of(region: &[u8]) -> Option<u64> {
    if region.len() < size_of::<HoldersHeader>() {
        return None;
    }
    let p = region.as_ptr() as *const AtomicU64;
    Some(unsafe { (*p).load(Ordering::Acquire) })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the wall clock is after the epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("subetha_holders_{tag}_{}_{nanos}.bin", std::process::id()))
    }

    /// Remove a scratch region, saying so if it cannot be removed. A
    /// leftover mapping on Windows shows up here rather than as the next
    /// run mysteriously attaching to a stale region.
    fn cleanup(path: &Path) {
        match crate::region_file::remove(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("the scratch region {} could not be removed: {e}", path.display()),
        }
    }

    /// Two holds on one region count each other, and the region survives
    /// the first release.
    #[test]
    fn a_second_hold_keeps_the_region_alive() {
        let path = scratch("two");
        let a = RingHolders::create_or_attach(&path, 4, LastHolder::Unlink).unwrap();
        let mut b = RingHolders::open(&path, 4, LastHolder::Unlink).unwrap();
        assert_eq!(a.live(), 2);
        assert_eq!(b.live(), 2);

        assert!(!b.release(), "one of two holders out is not the last");
        assert_eq!(a.live(), 1);
        drop(b);

        assert!(path.exists(), "a live holder keeps the region");
        drop(a);
        cleanup(&path);
    }

    /// The last hold out reports that it is, and only under Unlink.
    #[test]
    fn the_last_hold_reports_itself_and_only_when_asked_to() {
        let path = scratch("last");
        let mut only = RingHolders::create_or_attach(&path, 2, LastHolder::Unlink).unwrap();
        assert_eq!(only.live(), 1);
        assert!(only.release(), "the only holder is the last one out");
        only.unlink_self().unwrap();
        assert!(!path.exists(), "unlink_self removes the region");

        let keep_path = scratch("keep");
        let mut keeper = RingHolders::create_or_attach(&keep_path, 2, LastHolder::Keep).unwrap();
        assert!(
            !keeper.release(),
            "a Keep hold never asks for the backings to be removed, even alone",
        );
        assert!(keep_path.exists());
        drop(keeper);
        cleanup(&keep_path);
    }

    /// Releasing twice frees one slot, not two. A second release would
    /// otherwise free a slot another process had since claimed.
    #[test]
    fn releasing_twice_frees_one_slot() {
        let path = scratch("twice");
        let mut a = RingHolders::create_or_attach(&path, 4, LastHolder::Keep).unwrap();
        let b = RingHolders::open(&path, 4, LastHolder::Keep).unwrap();
        assert_eq!(b.live(), 2);
        a.release();
        assert_eq!(b.live(), 1);
        a.release();
        assert_eq!(b.live(), 1, "the second release is a no-op, not another slot freed");
        drop(a);
        drop(b);
        cleanup(&path);
    }

    /// A region built for one holder count refuses an attach at another,
    /// rather than reading past what is mapped.
    #[test]
    fn a_different_holder_count_is_refused() {
        let path = scratch("count");
        let a = RingHolders::create_or_attach(&path, 4, LastHolder::Keep).unwrap();
        assert_eq!(
            RingHolders::open(&path, 8, LastHolder::Keep).unwrap_err(),
            HoldersError::LayoutMismatch,
        );
        drop(a);
        cleanup(&path);
    }

    /// Every slot held by a live process is exhaustion; the caller is
    /// told rather than given a slot that is not theirs.
    #[test]
    fn a_full_table_is_reported_rather_than_overrun() {
        let path = scratch("full");
        let a = RingHolders::create_or_attach(&path, 1, LastHolder::Keep).unwrap();
        assert_eq!(
            RingHolders::open(&path, 1, LastHolder::Keep).unwrap_err(),
            HoldersError::Exhausted,
        );
        drop(a);
        cleanup(&path);
    }

    #[test]
    fn the_region_is_a_header_and_its_slots() {
        assert_eq!(holders_region_size(0), 64);
        assert_eq!(holders_region_size(1), 64 + 64);
        assert_eq!(holders_region_size(8), 64 + 8 * 64);
    }
}
