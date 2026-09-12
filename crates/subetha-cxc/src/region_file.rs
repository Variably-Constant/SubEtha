//! The one place a file-backed region is opened and removed.
//!
//! Every region in this crate is a memory-mapped file, and every one of
//! them is eventually removed while some process may still have it
//! mapped. That is ordinary: a removal takes the name and not the file,
//! so the file itself lives until the last mapping and descriptor close,
//! a peer mid-read keeps reading, and the storage is reclaimed when it
//! stops. The whole design of a last-holder removal assumes those
//! semantics, and the tests below hold the platform to them.
//!
//! Nothing here works around a platform. Windows removes a mapped file
//! while a peer still maps it, because the standard library's default
//! open already carries what the removal needs.
//! This module is the one place
//! that decides what each kind of open means, so a caller picks a
//! meaning rather than assembling one.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Open a region that must already exist.
pub fn open_existing(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

/// Open a region for reading only, which is what a view that never
/// writes takes so the file system can refuse a write rather than
/// trusting the caller not to make one.
pub fn open_read_only(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

/// Create a region, discarding the contents of one already there.
pub fn create_truncated(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)
}

/// Create a region only if nothing is there, failing with
/// `AlreadyExists` when something is.
///
/// This is the race that decides an initializer: several peers reach a
/// path together, exactly one of them gets the file, and the losers
/// attach and wait for it to be filled in. It has to be the file
/// system's own exclusive create, since a check followed by a create
/// lets two callers both believe they won.
pub fn create_new(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).create_new(true).open(path)
}

/// Create a region, keeping the contents of one already there. The
/// create-or-attach open: whoever gets there first builds it and the
/// rest join it.
pub fn create_or_open(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)
}

/// Remove a region's name. A process that still has it mapped keeps
/// working; the file goes when the last one lets go.
///
/// `NotFound` is passed through rather than swallowed, because a caller
/// removing a set of regions reports what was already gone separately
/// from what it removed.
pub fn remove(path: &Path) -> io::Result<()> {
    std::fs::remove_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(stem: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock reads at or past the unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "subetha_region_{}_{}_{}",
            stem,
            std::process::id(),
            unique
        ))
    }

    /// A region can be removed while this process still has it mapped,
    /// and the mapping keeps working afterwards. This is the property
    /// the whole last-holder design rests on, and the one Windows does
    /// not give a file opened the default way.
    #[test]
    fn a_mapped_region_can_be_removed_and_still_read() {
        let path = scratch("mapped");
        let file = create_truncated(&path).expect("create the region");
        file.set_len(64).expect("size it");
        let mut map = unsafe {
            memmap2::MmapOptions::new().len(64).map_mut(&file).expect("map it")
        };
        map[..5].copy_from_slice(b"alive");

        remove(&path).expect("remove a region that is still mapped");
        assert!(!path.exists(), "the name is gone at once");
        assert_eq!(&map[..5], b"alive", "and the mapping still reads");

        map[..5].copy_from_slice(b"still");
        assert_eq!(&map[..5], b"still", "and still writes");
    }

    /// What the default open actually does, measured rather than
    /// assumed: a plainly opened region is removable while this process
    /// has it mapped, on every platform this builds for, Windows
    /// included.
    ///
    /// This was written to assert the opposite, because two comments in
    /// this tree said a mapped file cannot be removed on Windows. It
    /// failed. The standard library's own defaults already carry what
    /// makes the removal work, so the behavior this module documents was
    /// never missing - the helper's value is that one place decides how
    /// a region is opened and removed, not that it repairs a platform.
    ///
    /// Kept pointing the way the platform actually behaves, so a change
    /// underneath - a standard library that stops defaulting this way, a
    /// file system that will not carry it - is caught here rather than
    /// in a teardown that silently leaves files behind.
    #[test]
    fn the_default_open_can_also_remove_a_mapped_region() {
        let path = scratch("default_open");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create the region the default way");
        file.set_len(64).expect("size it");
        let map = unsafe {
            memmap2::MmapOptions::new().len(64).map_mut(&file).expect("map it")
        };

        std::fs::remove_file(&path)
            .expect("a plainly opened region is removable while mapped");
        assert!(!path.exists(), "and its name is gone at once");
        assert_eq!(map[0], 0, "while the mapping still reads");
    }

    /// Removing something already gone says so rather than succeeding,
    /// because a caller removing a set reports what it found separately
    /// from what it removed.
    #[test]
    fn removing_an_absent_region_reports_it_absent() {
        let path = scratch("absent");
        let e = remove(&path).expect_err("nothing is there to remove");
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
    }

    /// The create-or-attach open keeps what is already there, and the
    /// truncating one does not.
    #[test]
    fn create_or_open_keeps_the_contents_and_create_truncated_does_not() {
        let path = scratch("keep");
        {
            let file = create_truncated(&path).expect("create");
            file.set_len(64).expect("size it");
            let mut map = unsafe {
                memmap2::MmapOptions::new().len(64).map_mut(&file).expect("map")
            };
            map[0] = 7;
        }
        {
            let file = create_or_open(&path).expect("attach");
            let map = unsafe {
                memmap2::MmapOptions::new().len(64).map(&file).expect("map")
            };
            assert_eq!(map[0], 7, "attaching keeps what was written");
        }
        {
            let file = create_truncated(&path).expect("recreate");
            file.set_len(64).expect("size it");
            let map = unsafe {
                memmap2::MmapOptions::new().len(64).map(&file).expect("map")
            };
            assert_eq!(map[0], 0, "recreating does not");
        }
        remove(&path).expect("remove the scratch region");
    }

    /// An open of something absent is an error rather than a fresh
    /// region, which is what tells an attach from a create.
    #[test]
    fn open_existing_refuses_a_region_that_is_not_there() {
        let path = scratch("missing");
        let e = open_existing(&path).expect_err("nothing to open");
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
    }
}
