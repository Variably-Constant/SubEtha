//! Race-free construction for the file-backed MMF primitives.
//!
//! [`create_or_attach`] elects a builder on a marker beside the region,
//! builds under a staging name, and publishes by linking that name to the
//! real one, so the region's name only ever appears over complete bytes.
//! Everyone else attaches. [`reset`] truncates and reinitializes.
//!
//! An attacher past the deadline drops the marker and elects again. That
//! is safe because the link, not the marker, decides who publishes: a
//! second builder finds the name taken and discards its staging region.
//! The region's own name is never reclaimed, since a removal takes the
//! name and not the file, leaving a slow builder filling in an orphan.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

/// How long an attacher waits for the elected creator to finish initializing
/// before giving up. Bounded so a creator that dies mid-initialization surfaces
/// as an error rather than an unbounded spin.
pub(crate) const INIT_WAIT: Duration = Duration::from_secs(5);

/// Map the region at `path`, initializing it only if this caller wins the
/// creation election.
///
/// `total` is the region size. `init` runs on the elected builder, over a
/// zeroed mapping of its own staging region, at most once per call; it
/// must write whatever `ready` tests last, because attachers spin on it.
/// `ready` reports whether a mapping is initialized, normally a
/// magic-number check.
///
/// Two builders overlap only when one takes the election over from
/// another past its deadline; the link then picks one and the other's
/// region is discarded.
///
/// Errors if the region is smaller than `total`, or if `path` is held by
/// something this module did not publish.
pub(crate) fn create_or_attach<I, R>(
    path: &Path,
    total: usize,
    init: I,
    ready: R,
) -> io::Result<(File, MmapMut)>
where
    I: FnOnce(*mut u8),
    R: Fn(*const u8) -> bool,
{
    let marker = sibling(path, ".building")?;
    match elect(path, &marker, total, &ready)? {
        Some(pair) => Ok(pair),
        None => {
            let built = publish(path, total, init);
            // Released after the link and whatever the outcome, so the
            // next holder finds the region there and a failed build leaves
            // nobody waiting out a deadline.
            let released = drop_marker(&marker);
            match built {
                Ok(pair) => released.map(|()| pair),
                // A peer published first, or the name holds a region built
                // in place under its own name.
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    released?;
                    match try_attach(path, total, &ready)? {
                        Some(pair) => Ok(pair),
                        None => Err(unreadable(path)),
                    }
                }
                Err(e) => Err(e),
            }
        }
    }
}

/// The error for a region name held by something this module did not
/// publish. Names the file and the fix: removing it by hand is the whole
/// of the recovery, since those bytes may belong to a region another
/// build is filling in right now.
fn unreadable(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "the region at {} never became readable: it is empty or unfinished, which a creator \
             that died while building it in place leaves behind. Remove the file to let it be \
             built again.",
            path.display()
        ),
    )
}

/// `Some` when the region was published and this caller attached to it,
/// `None` when this caller holds the marker and must build it.
///
/// A caller that wins the marker looks once more before building, since a
/// builder may have published and released between its last look and its
/// win.
fn elect<R>(
    path: &Path,
    marker: &Path,
    total: usize,
    ready: &R,
) -> io::Result<Option<(File, MmapMut)>>
where
    R: Fn(*const u8) -> bool,
{
    let mut deadline = Instant::now() + INIT_WAIT;
    loop {
        match crate::region_file::create_new(marker) {
            Ok(_taken) => {
                if let Some(pair) = try_attach(path, total, ready)? {
                    drop_marker(marker)?;
                    return Ok(Some(pair));
                }
                return Ok(None);
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if let Some(pair) = try_attach(path, total, ready)? {
                    return Ok(Some(pair));
                }
                if Instant::now() >= deadline {
                    // The holder never published. A builder still alive
                    // loses nothing: the link decides who wins.
                    drop_marker(marker)?;
                    deadline = Instant::now() + INIT_WAIT;
                }
                std::thread::yield_now();
            }
            Err(e) => return Err(e),
        }
    }
}

/// Build the region under a staging name and publish it as `path`.
///
/// The link fails with `AlreadyExists` rather than replacing, so whoever
/// links first wins and no one overwrites a region a peer is using. Both
/// names address one file afterwards, so dropping the staging name leaves
/// the region and this mapping in place.
fn publish<I>(path: &Path, total: usize, init: I) -> io::Result<(File, MmapMut)>
where
    I: FnOnce(*mut u8),
{
    let (staging, file) = create_staging(path)?;
    let mapped = match build(&file, total, init) {
        Ok(m) => m,
        Err(e) => return Err(discard_staging(&staging, e)),
    };
    if let Err(e) = std::fs::hard_link(&staging, path) {
        return Err(discard_staging(&staging, e));
    }
    crate::region_file::remove(&staging)?;
    Ok((file, mapped))
}

/// Release the election marker. Already gone means another caller took it
/// over past a deadline, which is the recovery working.
fn drop_marker(marker: &Path) -> io::Result<()> {
    match crate::region_file::remove(marker) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// How many staging names to try before giving up on finding a free one.
const STAGING_ATTEMPTS: u32 = 64;

/// Take a staging file under a name nothing else holds. A taken name is
/// one an earlier run left behind, so it is retried rather than reported.
fn create_staging(path: &Path) -> io::Result<(PathBuf, File)> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    for _ in 0..STAGING_ATTEMPTS {
        let staging = sibling(
            path,
            &format!(".staging.{}.{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)),
        )?;
        match crate::region_file::create_new(&staging) {
            Ok(file) => return Ok((staging, file)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other(format!(
        "no free staging name for {} in {STAGING_ATTEMPTS} tries",
        path.display()
    )))
}

/// A name beside `path` carrying `suffix`. The same directory, because
/// publishing links the finished region into place and a link cannot
/// cross a file system.
fn sibling(path: &Path, suffix: &str) -> io::Result<PathBuf> {
    let Some(stem) = path.file_name() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} does not name a region file", path.display()),
        ));
    };
    let mut name = stem.to_os_string();
    name.push(suffix);
    Ok(path.with_file_name(name))
}

/// Drop a staging region after a failure, reporting `cause`. One that
/// cannot be removed is named in the error too, since it sits in the
/// region's own directory.
fn discard_staging(staging: &Path, cause: io::Error) -> io::Error {
    match crate::region_file::remove(staging) {
        Ok(()) => cause,
        Err(removal) => io::Error::new(
            cause.kind(),
            format!(
                "{cause}; the staging region at {} was also left behind: {removal}",
                staging.display()
            ),
        ),
    }
}

/// Size, map and initialize a region: zeroed first, because `init`
/// writes only the fields it names and a reader reads all of them.
fn build<I>(file: &File, total: usize, init: I) -> io::Result<MmapMut>
where
    I: FnOnce(*mut u8),
{
    file.set_len(total as u64)?;
    let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(file)? };
    unsafe {
        std::ptr::write_bytes(mmap.as_mut_ptr(), 0, total);
        init(mmap.as_mut_ptr());
    }
    Ok(mmap)
}

/// Convert an attach error for a caller with a layout-mismatch error of
/// its own: a size mismatch becomes `mismatch`, anything else converts
/// as the I/O error it is.
pub(crate) fn attach_error<E: From<io::Error>>(e: io::Error, mismatch: E) -> E {
    if is_size_mismatch(&e) { mismatch } else { E::from(e) }
}

/// Whether `e` is the error [`create_or_attach`] returns for a region that
/// exists at a different size than the one requested. A caller with a
/// layout-mismatch error of its own reports that instead of an I/O error.
pub(crate) fn is_size_mismatch(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::InvalidData && e.get_ref().is_some_and(|inner| inner.is::<SizeMismatch>())
}

/// The region on disk is a different size than the caller requested.
#[derive(Debug)]
struct SizeMismatch {
    path: std::path::PathBuf,
    on_disk: u64,
    requested: usize,
}

impl std::fmt::Display for SizeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the region at {} is {} bytes on disk, a different size than the {} requested; it was created at another capacity",
            self.path.display(),
            self.on_disk,
            self.requested
        )
    }
}

impl std::error::Error for SizeMismatch {}

/// One attach attempt. `Ok(None)` means the file exists but the creator has not
/// published yet, which is a state to wait through rather than an error.
///
/// The creator's `set_len` is what takes the file from zero bytes to
/// `total`, so a zero-length file is a creator between election and
/// `set_len`, and a file shorter than `total` but not empty is a region
/// that exists at another size: its creator finished long ago, at a
/// different capacity, and waiting on it would never end in an attach.
fn try_attach<R>(path: &Path, total: usize, ready: &R) -> io::Result<Option<(File, MmapMut)>>
where
    R: Fn(*const u8) -> bool,
{
    let file = match crate::region_file::open_existing(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let on_disk = file.metadata()?.len();
    if on_disk == 0 {
        return Ok(None);
    }
    if (on_disk as usize) < total {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            SizeMismatch { path: path.to_path_buf(), on_disk, requested: total },
        ));
    }
    let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
    if !ready(mmap.as_ptr()) {
        return Ok(None);
    }
    Ok(Some((file, mmap)))
}

/// Truncate and reinitialize the region at `path`, discarding any state a live
/// peer holds. For a caller that knows it owns the path.
///
/// On Windows this errors while any process still maps the region
/// (ERROR_USER_MAPPED_FILE): the OS refuses to truncate a mapped file, so a
/// reset succeeds only once every handle is gone.
pub(crate) fn reset<I>(path: &Path, total: usize, init: I) -> io::Result<(File, MmapMut)>
where
    I: FnOnce(*mut u8),
{
    let file = crate::region_file::create_truncated(path)?;
    file.set_len(total as u64)?;
    let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
    unsafe {
        std::ptr::write_bytes(mmap.as_mut_ptr(), 0, total);
        init(mmap.as_mut_ptr());
    }
    Ok((file, mmap))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Barrier};

    const MAGIC: u64 = 0x4D4D_4641_5454_4143;

    /// A fresh path for one test: whatever an earlier run left there is
    /// removed, and a removal refused for any reason but absence fails the
    /// test rather than gating it on a stale region. The election marker
    /// goes too, so a test starts with no builder holding the region.
    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha-mmf-attach-{name}-{}.bin", std::process::id()));
        let marker = sibling(&p, ".building").expect("the region path names a file");
        for stale in [&p, &marker] {
            match std::fs::remove_file(stale) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => panic!("stale file {} not removed: {e}", stale.display()),
            }
        }
        p
    }

    /// The election marker beside a region. Reads only: tests assert on
    /// whether it is there, so removing it here would void them.
    fn marker_of(path: &std::path::Path) -> std::path::PathBuf {
        sibling(path, ".building").expect("the region path names a file")
    }

    unsafe fn write_magic(ptr: *mut u8) {
        unsafe { std::ptr::write_unaligned(ptr as *mut u64, MAGIC) };
    }

    fn has_magic(ptr: *const u8) -> bool {
        unsafe { std::ptr::read_unaligned(ptr as *const u64) == MAGIC }
    }

    /// Racing callers all attach to one region, and exactly one initializes it.
    #[test]
    fn exactly_one_caller_initialises() {
        let p = tmp("elect");
        let inits = Arc::new(AtomicU32::new(0));
        let gate = Arc::new(Barrier::new(8));
        let mut hs = Vec::new();
        for _ in 0..8 {
            let (p, inits, gate) = (p.clone(), Arc::clone(&inits), Arc::clone(&gate));
            hs.push(std::thread::spawn(move || {
                gate.wait();
                let (_f, m) = create_or_attach(
                    &p,
                    64,
                    |ptr| {
                        inits.fetch_add(1, Ordering::Relaxed);
                        unsafe { write_magic(ptr) };
                    },
                    has_magic,
                )
                .expect("attach");
                assert!(has_magic(m.as_ptr()), "attached to an uninitialized region");
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(inits.load(Ordering::Relaxed), 1, "more than one caller initialized");
        std::fs::remove_file(&p).expect("every thread's mapping is dropped and the file removable");
    }

    /// A second call attaches to the live region rather than zeroing it.
    #[test]
    fn attach_does_not_clear_existing_state() {
        let p = tmp("attach");

        let (_f, mut first) =
            create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic).unwrap();
        // State a live peer owns, past the magic.
        unsafe { std::ptr::write_unaligned(first.as_mut_ptr().add(8) as *mut u64, 0xDEAD_BEEF) };

        let (_f2, second) =
            create_or_attach(&p, 64, |_| panic!("must not re-initialize"), has_magic).unwrap();
        let seen = unsafe { std::ptr::read_unaligned(second.as_ptr().add(8) as *const u64) };
        assert_eq!(seen, 0xDEAD_BEEF, "attaching cleared state the first caller owned");
        drop(second);
        drop(first);
        drop(_f2);
        drop(_f);
        std::fs::remove_file(&p).expect("the region file is unmapped and removable");
    }

    /// A region that exists at a smaller size is refused at once, as a size
    /// mismatch, rather than waited on as a creator still initializing.
    #[test]
    fn a_smaller_published_region_is_a_size_mismatch_not_a_wait() {
        let p = tmp("smaller");
        let (file, mapping) =
            create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic).unwrap();
        let started = Instant::now();
        let err = create_or_attach(&p, 128, |_| panic!("must not re-initialize"), has_magic)
            .expect_err("a smaller region must not attach at a larger size");
        assert!(is_size_mismatch(&err), "not a size mismatch: {err}");
        assert!(
            started.elapsed() < INIT_WAIT / 2,
            "refused only after waiting {:?} for a creator that finished long ago",
            started.elapsed()
        );
        let text = err.to_string();
        assert!(text.contains("64 bytes") && text.contains("128 requested"), "{text}");
        drop(mapping);
        drop(file);
        std::fs::remove_file(&p).expect("the region file is unmapped and removable");
    }

    /// A region name held by something this module did not publish is
    /// reported, naming the file and the fix. Nothing recovers it: those
    /// bytes may belong to a region another build is filling in.
    #[test]
    fn an_empty_file_under_the_region_name_is_named_in_the_error() {
        let p = tmp("empty");
        File::create(&p).expect("an empty file at the region path");
        let err = create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic)
            .expect_err("an empty file has no region to attach to");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
        assert!(!is_size_mismatch(&err));
        let text = err.to_string();
        assert!(text.contains(&p.display().to_string()), "the error names the file: {text}");
        assert!(text.contains("Remove the file"), "the error carries the fix: {text}");
        assert!(!marker_of(&p).exists(), "the election marker was released");
        std::fs::remove_file(&p).expect("the empty file is removable");
    }

    /// A builder that dies holding the marker does not keep the region
    /// from being built: the next caller gives it until the deadline,
    /// drops the marker, and builds the region itself.
    #[test]
    fn a_builder_that_dies_holding_the_marker_does_not_keep_the_region_from_being_built() {
        let p = tmp("dead_builder");
        let marker = marker_of(&p);
        File::create(&marker).expect("the dead builder's marker");
        assert!(!p.exists(), "it died before publishing anything");

        let started = Instant::now();
        let (f, m) = create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic)
            .expect("the region is built despite the abandoned marker");
        assert!(has_magic(m.as_ptr()), "and it is initialized");
        assert!(
            started.elapsed() >= INIT_WAIT,
            "took the marker over after only {:?}, without giving a live builder its deadline",
            started.elapsed()
        );
        assert!(!marker.exists(), "the marker is released once the region is published");

        drop(m);
        drop(f);
        std::fs::remove_file(&p).expect("the region file is unmapped and removable");
    }

    /// Publishing leaves nothing beside the region: no marker, and no
    /// staging file, which would be a full-size region nobody reclaims.
    #[test]
    fn publishing_leaves_no_marker_and_no_staging_file() {
        let p = tmp("no_litter");
        let (f, m) =
            create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic).unwrap();
        assert!(p.exists(), "the region is published under its own name");
        assert!(!marker_of(&p).exists(), "no election marker is left");

        let dir = p.parent().expect("the region has a directory");
        let stem = p.file_name().expect("the region has a name").to_string_lossy().to_string();
        let mut strays = Vec::new();
        for entry in std::fs::read_dir(dir).expect("the directory lists") {
            let name = entry.expect("the entry reads").file_name().to_string_lossy().to_string();
            if name.starts_with(&stem) && name != stem {
                strays.push(name);
            }
        }
        assert!(strays.is_empty(), "left beside the region: {strays:?}");

        drop(m);
        drop(f);
        std::fs::remove_file(&p).expect("the region file is unmapped and removable");
    }


    /// reset deliberately discards it, which is the case truncation was for.
    #[test]
    fn reset_clears_the_region() {
        let p = tmp("reset");

        let (_f, mut m) =
            create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic).unwrap();
        unsafe { std::ptr::write_unaligned(m.as_mut_ptr().add(8) as *mut u64, 0xDEAD_BEEF) };
        drop(m);

        let (_f2, fresh) = reset(&p, 64, |ptr| unsafe { write_magic(ptr) }).unwrap();
        let seen = unsafe { std::ptr::read_unaligned(fresh.as_ptr().add(8) as *const u64) };
        assert_eq!(seen, 0, "reset left prior state behind");
        drop(fresh);
        drop(_f2);
        drop(_f);
        std::fs::remove_file(&p).expect("the region file is unmapped and removable");
    }
}
