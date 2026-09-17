//! Race-free construction for the file-backed MMF primitives.
//!
//! [`create_or_attach`] elects one creator through an exclusive `create_new`;
//! the winner initializes over a zeroed mapping and everyone else attaches to
//! what the winner built. [`reset`] truncates and reinitializes.
//!
//! The election hands the winner the region's own name before the region
//! exists, so a creator that dies while building leaves a file no one may
//! replace: every later attacher waits out the deadline and is told to
//! remove it by hand. Telling a dead creator from a slow one needs a
//! liveness test this module does not have, and without one no recovery
//! is safe.

use std::fs::File;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

/// How long an attacher waits for the elected creator to finish initializing
/// before giving up. Bounded so a creator that dies mid-initialization surfaces
/// as an error rather than an unbounded spin.
pub(crate) const INIT_WAIT: Duration = Duration::from_secs(5);

/// Map the region at `path`, initializing it only if this caller wins the
/// creation election.
///
/// `total` is the region size. `init` runs exactly once, on the winner, over a
/// zeroed mapping; it must publish whatever `ready` tests last, because
/// attachers spin on it. `ready` reports whether a mapping is fully
/// initialized - normally a magic-number check.
///
/// Returns the file and mapping. Errors if the region is smaller than `total`,
/// or if an elected creator never finishes.
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
    match crate::region_file::create_new(path) {
        Ok(file) => {
            let mapped = build(&file, total, init)?;
            Ok((file, mapped))
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => wait_for_region(path, total, &ready),
        Err(e) => Err(e),
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

/// Attach to a region an elected creator is still building, giving it
/// until the deadline to finish.
///
/// A name that stays unready for the whole window belongs to a creator
/// that died while building it. Nothing recovers that automatically -
/// the file is indistinguishable from one a live creator is working on -
/// so the error names the file and says what to do with it.
fn wait_for_region<R>(path: &Path, total: usize, ready: &R) -> io::Result<(File, MmapMut)>
where
    R: Fn(*const u8) -> bool,
{
    let deadline = Instant::now() + INIT_WAIT;
    loop {
        if let Some(pair) = try_attach(path, total, ready)? {
            return Ok(pair);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "the region at {} never became readable: it is empty or unfinished, which a \
                     creator that died while building it in place leaves behind. Remove the file \
                     to let it be built again.",
                    path.display()
                ),
            ));
        }
        std::thread::yield_now();
    }
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
    /// test rather than gating it on a stale region.
    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha-mmf-attach-{name}-{}.bin", std::process::id()));
        match std::fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => panic!("stale region {} not removed: {e}", p.display()),
        }
        p
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

    /// An empty file under the region's own name is waited on and then
    /// reported, and the error says to remove it.
    ///
    /// Publishing by link never produces this state: a name appears only
    /// over a finished region. What does produce it is an older build,
    /// which took the region's own name first and sized it afterwards, so
    /// a death in between left exactly this. Nothing can recover it
    /// automatically - the file may equally be a region being built in
    /// place by such a build right now - so the wait stands and the
    /// message carries the fix.
    #[test]
    fn an_empty_file_is_waited_on_and_then_named_in_the_error() {
        let p = tmp("empty");
        File::create(&p).expect("an empty file at the region path");
        let started = Instant::now();
        let err = create_or_attach(&p, 64, |_| panic!("must not re-initialize"), has_magic)
            .expect_err("an empty file has no region to attach to");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
        assert!(!is_size_mismatch(&err));
        assert!(started.elapsed() >= INIT_WAIT, "gave up early after {:?}", started.elapsed());
        let text = err.to_string();
        assert!(text.contains(&p.display().to_string()), "the error names the file: {text}");
        assert!(text.contains("Remove the file"), "the error carries the fix: {text}");
        std::fs::remove_file(&p).expect("the empty file is removable");
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
