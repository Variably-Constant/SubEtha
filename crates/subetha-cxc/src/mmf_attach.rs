//! Race-free construction for the file-backed MMF primitives.
//!
//! [`create_or_attach`] elects one creator through an exclusive `create_new`;
//! the winner initializes over a zeroed mapping and everyone else attaches to
//! what the winner built. [`reset`] truncates and reinitializes.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

/// How long an attacher waits for the elected creator to finish initializing
/// before giving up. Bounded so a creator that dies mid-initialisation surfaces
/// as an error rather than an unbounded spin.
pub(crate) const INIT_WAIT: Duration = Duration::from_secs(5);

/// Map the region at `path`, initializing it only if this caller wins the
/// creation election.
///
/// `total` is the region size. `init` runs exactly once, on the winner, over a
/// zeroed mapping; it must publish whatever `ready` tests LAST, because
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
    match OpenOptions::new().read(true).write(true).create_new(true).open(path) {
        Ok(file) => {
            file.set_len(total as u64)?;
            let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
            unsafe {
                std::ptr::write_bytes(mmap.as_mut_ptr(), 0, total);
                init(mmap.as_mut_ptr());
            }
            Ok((file, mmap))
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            let deadline = Instant::now() + INIT_WAIT;
            loop {
                if let Some(pair) = try_attach(path, total, &ready)? {
                    return Ok(pair);
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "the region's creator did not finish initializing it",
                    ));
                }
                std::thread::yield_now();
            }
        }
        Err(e) => Err(e),
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
    let file = match OpenOptions::new().read(true).write(true).open(path) {
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

/// Truncate and reinitialise the region at `path`, discarding any state a live
/// peer holds. For a caller that knows it owns the path.
///
/// On Windows this errors while any process still maps the region
/// (ERROR_USER_MAPPED_FILE): the OS refuses to truncate a mapped file, so a
/// reset succeeds only once every handle is gone.
pub(crate) fn reset<I>(path: &Path, total: usize, init: I) -> io::Result<(File, MmapMut)>
where
    I: FnOnce(*mut u8),
{
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
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

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha-mmf-attach-{name}-{}.bin", std::process::id()));
        p
    }

    unsafe fn write_magic(ptr: *mut u8) {
        unsafe { std::ptr::write_unaligned(ptr as *mut u64, MAGIC) };
    }

    fn has_magic(ptr: *const u8) -> bool {
        unsafe { std::ptr::read_unaligned(ptr as *const u64) == MAGIC }
    }

    /// Racing callers all attach to one region, and exactly one initialises it.
    #[test]
    fn exactly_one_caller_initialises() {
        let p = tmp("elect");
        std::fs::remove_file(&p).ok();

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
                assert!(has_magic(m.as_ptr()), "attached to an uninitialised region");
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(inits.load(Ordering::Relaxed), 1, "more than one caller initialized");
        std::fs::remove_file(&p).ok();
    }

    /// A second call attaches to the live region rather than zeroing it.
    #[test]
    fn attach_does_not_clear_existing_state() {
        let p = tmp("attach");
        std::fs::remove_file(&p).ok();

        let (_f, mut first) =
            create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic).unwrap();
        // State a live peer owns, past the magic.
        unsafe { std::ptr::write_unaligned(first.as_mut_ptr().add(8) as *mut u64, 0xDEAD_BEEF) };

        let (_f2, second) =
            create_or_attach(&p, 64, |_| panic!("must not re-initialize"), has_magic).unwrap();
        let seen = unsafe { std::ptr::read_unaligned(second.as_ptr().add(8) as *const u64) };
        assert_eq!(seen, 0xDEAD_BEEF, "attaching cleared state the first caller owned");
        std::fs::remove_file(&p).ok();
    }

    /// A region that exists at a smaller size is refused at once, as a size
    /// mismatch, rather than waited on as a creator still initialising.
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

    /// An empty file is a creator between its election and its set_len, and
    /// is still waited on; one that never grows surfaces as the timeout.
    #[test]
    fn an_empty_file_is_waited_on_until_the_creator_deadline() {
        let p = tmp("empty");
        File::create(&p).expect("an empty file at the region path");
        let started = Instant::now();
        let err = create_or_attach(&p, 64, |_| panic!("must not re-initialize"), has_magic)
            .expect_err("an empty file has no region to attach to");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
        assert!(!is_size_mismatch(&err));
        assert!(started.elapsed() >= INIT_WAIT, "gave up early after {:?}", started.elapsed());
        std::fs::remove_file(&p).expect("the empty file is removable");
    }

    /// reset deliberately discards it, which is the case truncation was for.
    #[test]
    fn reset_clears_the_region() {
        let p = tmp("reset");
        std::fs::remove_file(&p).ok();

        let (_f, mut m) =
            create_or_attach(&p, 64, |ptr| unsafe { write_magic(ptr) }, has_magic).unwrap();
        unsafe { std::ptr::write_unaligned(m.as_mut_ptr().add(8) as *mut u64, 0xDEAD_BEEF) };
        drop(m);

        let (_f2, fresh) = reset(&p, 64, |ptr| unsafe { write_magic(ptr) }).unwrap();
        let seen = unsafe { std::ptr::read_unaligned(fresh.as_ptr().add(8) as *const u64) };
        assert_eq!(seen, 0, "reset left prior state behind");
        std::fs::remove_file(&p).ok();
    }
}
