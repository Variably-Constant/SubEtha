//! Race-free construction for the file-backed MMF primitives.
//!
//! A `create` that opens with `truncate(true)` and zeroes its header resets
//! whatever a live peer is using: a held lock flag, a ring's cursors, a
//! directory's claims. An exists-then-create check does not prevent it, since
//! the check and the create are separate steps.
//!
//! [`create_or_attach`] elects one creator through an exclusive `create_new`.
//! The winner initialises; everyone else attaches to what the winner built.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

/// How long an attacher waits for the elected creator to finish initializing
/// before giving up. Bounded so a creator that dies mid-initialisation surfaces
/// as an error rather than an unbounded spin.
const INIT_WAIT: Duration = Duration::from_secs(5);

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

/// One attach attempt. `Ok(None)` means the file exists but the creator has not
/// published yet, which is a state to wait through rather than an error.
fn try_attach<R>(path: &Path, total: usize, ready: &R) -> io::Result<Option<(File, MmapMut)>>
where
    R: Fn(*const u8) -> bool,
{
    let file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if (file.metadata()?.len() as usize) < total {
        return Ok(None);
    }
    let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
    if !ready(mmap.as_ptr()) {
        return Ok(None);
    }
    Ok(Some((file, mmap)))
}

/// Truncate and reinitialise the region at `path`, discarding any state a live
/// peer holds. For a caller that knows it owns the path.
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
