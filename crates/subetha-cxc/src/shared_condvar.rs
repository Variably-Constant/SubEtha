//! `SharedCondvar`: cross-process condition variable built on top
//! of [`CrossProcessWaker`](crate::cross_process_waker).
//!
//! Classic Mesa-style condvar interface: waiters check a user-owned
//! predicate, park if not satisfied, and resume when a notifier
//! advances the predicate AND calls `notify_*`. The substrate uses
//! a monotonic generation counter so each `wait` parks at
//! `target = current_gen + 1`; every `notify_*` bumps the generation
//! and fires `wake_(one_)up_to(new_gen)`, which wakes parked waiters
//! whose `target <= new_gen`.
//!
//! # Cross-process semantics
//!
//! Two processes mmap the same condvar base; both call `wait` /
//! `notify_*` directly. On Linux the wake call crosses the process
//! boundary via SHARED `futex` (keyed by inode + offset, so two
//! different mmaps of the same file page DO match). On Windows /
//! macOS the primitive runs intra-process via `WaitOnAddress` /
//! spin fallback.
//!
//! # Intra-process sharing: use Arc::clone, NOT create+open
//!
//! Within ONE process, share a single `SharedCondvar` through
//! `Arc<SharedCondvar>` + `Arc::clone`. Calling `create` and then
//! `open` on the same path in the same process produces two
//! independent mmaps with different virtual-address ranges aliased
//! to the same file pages. Windows `WaitOnAddress` is keyed by
//! virtual address, so a `notify_*` on the second handle does NOT
//! reach a `wait` on the first handle - the wake hashtable lookup
//! misses on the differing virtual address. Linux SHARED `futex`
//! keys by the underlying file page, which works across separate
//! mmaps, but the rule "use one `Arc<SharedCondvar>` per process"
//! is cross-platform safe.
//!
//! The `open` constructor is exclusively for joiners in SEPARATE
//! processes that need to find the file the creator already
//! initialised.
//!
//! # Predicate ownership
//!
//! The condvar does NOT own the predicate atom; the caller passes
//! a closure that returns the current predicate value. This matches
//! `parking_lot::Condvar::wait_while` semantics and lets the same
//! condvar guard predicates held in any cross-process atom
//! (`SharedAtomicU32`, a field in a `SharedCell`, an offset into
//! an MMF struct, etc.).

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

use crate::cross_process_waker::{
    CrossProcessWaker, MAX_WAITERS_DEFAULT, WakerError,
};

/// Magic header byte so `open` validates that the file at the gen
/// path was actually written by this primitive.
const CONDVAR_GEN_MAGIC: u64 = 0x434F_4E44_5641_5230; // "CONDVAR0"
const GEN_REGION_SIZE: usize = 64; // one cache line: [magic u64][gen AtomicU64]
const GEN_OFFSET: usize = 8;

/// Errors returned by [`SharedCondvar`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CondvarError {
    /// All waker slots in use; caller's fallback is to spin on the
    /// predicate via the underlying atom.
    WakerFull,
    /// `wait_timeout` returned because the caller-supplied timeout
    /// elapsed before the predicate became true.
    Timeout,
    /// Backing file (waker or gen) layout did not match expectations
    /// on `open`.
    LayoutMismatch,
    /// I/O error from the underlying mmap.
    Io(io::ErrorKind),
}

impl From<WakerError> for CondvarError {
    fn from(e: WakerError) -> Self {
        match e {
            WakerError::Full => Self::WakerFull,
            WakerError::Timeout => Self::Timeout,
            WakerError::LayoutMismatch => Self::LayoutMismatch,
            WakerError::IoError(k) => Self::Io(k),
        }
    }
}

impl From<io::Error> for CondvarError {
    fn from(e: io::Error) -> Self { Self::Io(e.kind()) }
}

/// Generation-counter backing. Owns either an anon mmap (in-process)
/// or a file-backed mmap (cross-process); exposes a stable
/// `&AtomicU64` view into the first 8 bytes after a magic header.
///
/// Variant payloads are held purely for their `Drop` side effects:
/// dropping the `MmapMut` unmaps, dropping the `File` releases the
/// fd. The `GenAtom::ptr` field reads through them, so they ARE
/// load-bearing despite never being named.
#[allow(dead_code)]
enum GenBacking {
    Anon(MmapMut),
    File(std::fs::File, MmapMut),
}

struct GenAtom {
    /// Owns the underlying mmap so `ptr` stays valid until Drop.
    #[allow(dead_code)]
    backing: GenBacking,
    ptr: *const AtomicU64,
}

// SAFETY: the AtomicU64 ptr lives inside the mmap we own; mmap
// pages are valid for the lifetime of GenAtom. AtomicU64 is Sync.
unsafe impl Send for GenAtom {}
unsafe impl Sync for GenAtom {}

impl GenAtom {
    fn create_anon() -> Result<Self, CondvarError> {
        let mut mmap = MmapOptions::new().len(GEN_REGION_SIZE).map_anon()?;
        let base = mmap.as_mut_ptr();
        unsafe {
            (base as *mut u64).write(CONDVAR_GEN_MAGIC);
            (base.add(GEN_OFFSET) as *mut AtomicU64).write(AtomicU64::new(0));
        }
        let ptr = unsafe { base.add(GEN_OFFSET) as *const AtomicU64 };
        Ok(Self { backing: GenBacking::Anon(mmap), ptr })
    }

    fn create_file(path: &Path) -> Result<Self, CondvarError> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(GEN_REGION_SIZE as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(GEN_REGION_SIZE).map_mut(&file)? };
        let base = mmap.as_mut_ptr();
        unsafe {
            (base as *mut u64).write(CONDVAR_GEN_MAGIC);
            (base.add(GEN_OFFSET) as *mut AtomicU64).write(AtomicU64::new(0));
        }
        let ptr = unsafe { base.add(GEN_OFFSET) as *const AtomicU64 };
        Ok(Self { backing: GenBacking::File(file, mmap), ptr })
    }

    fn open_file(path: &Path) -> Result<Self, CondvarError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let meta = file.metadata()?;
        if (meta.len() as usize) < GEN_REGION_SIZE {
            return Err(CondvarError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(GEN_REGION_SIZE).map_mut(&file)? };
        let base = mmap.as_mut_ptr();
        let magic = unsafe { (base as *const u64).read() };
        if magic != CONDVAR_GEN_MAGIC {
            return Err(CondvarError::LayoutMismatch);
        }
        let ptr = unsafe { base.add(GEN_OFFSET) as *const AtomicU64 };
        Ok(Self { backing: GenBacking::File(file, mmap), ptr })
    }

    #[inline]
    fn atom(&self) -> &AtomicU64 {
        // SAFETY: ptr points GEN_OFFSET bytes into an mmap owned by
        // this struct; AtomicU64 was initialised in create_*
        // (or read from a peer's create_* on open_file).
        unsafe { &*self.ptr }
    }
}

/// Cross-process condition variable. Mesa-style: callers re-check
/// the predicate after each wake. Internally backed by one
/// [`CrossProcessWaker`] plus a generation counter in mmap.
pub struct SharedCondvar {
    waker: Arc<CrossProcessWaker>,
    gen_atom: Arc<GenAtom>,
}

impl SharedCondvar {
    /// In-process condvar (anonymous waker + anonymous mmap for the
    /// generation atom).
    pub fn create_anon() -> Result<Self, CondvarError> {
        Self::create_anon_with_capacity(MAX_WAITERS_DEFAULT)
    }

    /// In-process condvar with a custom max-waiters capacity.
    pub fn create_anon_with_capacity(max_waiters: usize) -> Result<Self, CondvarError> {
        let waker = Arc::new(CrossProcessWaker::create_anon(max_waiters)?);
        let gen_atom = Arc::new(GenAtom::create_anon()?);
        Ok(Self { waker, gen_atom })
    }

    /// File-backed condvar. Path layout:
    ///   `<base>.waker.bin`  - waker slot array
    ///   `<base>.gen.bin`    - magic + generation counter
    pub fn create(base_path: impl AsRef<Path>) -> Result<Self, CondvarError> {
        Self::create_with_capacity(base_path, MAX_WAITERS_DEFAULT)
    }

    pub fn create_with_capacity(
        base_path: impl AsRef<Path>,
        max_waiters: usize,
    ) -> Result<Self, CondvarError> {
        let (waker_path, gen_path) = side_paths(base_path.as_ref());
        let waker = Arc::new(CrossProcessWaker::create(waker_path, max_waiters)?);
        let gen_atom = Arc::new(GenAtom::create_file(&gen_path)?);
        Ok(Self { waker, gen_atom })
    }

    /// Open an existing file-backed condvar. Both processes that
    /// share the condvar pass the same `base_path`; one calls
    /// `create`, the other (and any later joiners) call `open`.
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self, CondvarError> {
        Self::open_with_capacity(base_path, MAX_WAITERS_DEFAULT)
    }

    pub fn open_with_capacity(
        base_path: impl AsRef<Path>,
        expected_max_waiters: usize,
    ) -> Result<Self, CondvarError> {
        let (waker_path, gen_path) = side_paths(base_path.as_ref());
        let waker = Arc::new(CrossProcessWaker::open(waker_path, expected_max_waiters)?);
        let gen_atom = Arc::new(GenAtom::open_file(&gen_path)?);
        Ok(Self { waker, gen_atom })
    }

    /// Park until `predicate()` returns true. Re-evaluates the
    /// predicate after every wake (Mesa-style). Spurious wakes
    /// re-loop without surfacing to the caller.
    pub fn wait<F: FnMut() -> bool>(&self, mut predicate: F) -> Result<(), CondvarError> {
        loop {
            if predicate() {
                return Ok(());
            }
            // Snapshot generation BEFORE re-checking. If a notify
            // slips in between predicate() and try_park, the
            // snapshot is older than the bumped generation, so the
            // wake call's wake_*_up_to(new_gen) matches our slot's
            // target_seq = snapshot + 1 <= new_gen.
            let snapshot = self.gen_atom.atom().load(Ordering::Acquire);
            let token = self.waker.try_park(snapshot + 1)?;
            // Wake-before-park recovery.
            if predicate() {
                self.waker.release(token);
                return Ok(());
            }
            self.waker.wait(token, None)?;
        }
    }

    /// Park until `predicate()` returns true OR `timeout` elapses.
    /// On `Err(Timeout)` the predicate is guaranteed to have been
    /// false at the point of return.
    pub fn wait_timeout<F: FnMut() -> bool>(
        &self,
        mut predicate: F,
        timeout: Duration,
    ) -> Result<(), CondvarError> {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return Ok(());
            }
            let snapshot = self.gen_atom.atom().load(Ordering::Acquire);
            let token = self.waker.try_park(snapshot + 1)?;
            if predicate() {
                self.waker.release(token);
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                self.waker.release(token);
                return Err(CondvarError::Timeout);
            }
            let remaining = deadline - now;
            match self.waker.wait(token, Some(remaining)) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => {
                    if predicate() {
                        return Ok(());
                    }
                    return Err(CondvarError::Timeout);
                }
                Err(e) => return Err(CondvarError::from(e)),
            }
        }
    }

    /// Wake at most one parked waiter. Caller is responsible for
    /// having advanced the predicate before calling. Returns 1 if a
    /// waiter was woken, 0 if none were parked.
    pub fn notify_one(&self) -> usize {
        let new_gen = self.gen_atom.atom().fetch_add(1, Ordering::Release) + 1;
        self.waker.wake_one_up_to(new_gen)
    }

    /// Wake every parked waiter. Returns the count actually woken.
    pub fn notify_all(&self) -> usize {
        let new_gen = self.gen_atom.atom().fetch_add(1, Ordering::Release) + 1;
        self.waker.wake_up_to(new_gen)
    }

    /// Current generation snapshot (observational; advances on
    /// every notify).
    pub fn generation(&self) -> u64 {
        self.gen_atom.atom().load(Ordering::Acquire)
    }

    /// Underlying waker handle, for callers who want to peek wake
    /// state directly.
    pub fn waker(&self) -> &Arc<CrossProcessWaker> { &self.waker }
}

fn side_paths(base: &Path) -> (PathBuf, PathBuf) {
    let mut w = base.as_os_str().to_owned();
    w.push(".waker.bin");
    let mut g = base.as_os_str().to_owned();
    g.push(".gen.bin");
    (PathBuf::from(w), PathBuf::from(g))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    #[test]
    fn notify_one_wakes_exactly_one_waiter() {
        let cv = Arc::new(SharedCondvar::create_anon().expect("create"));
        let pred = Arc::new(AtomicBool::new(false));
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let cv2 = Arc::clone(&cv);
                let pred2 = Arc::clone(&pred);
                thread::spawn(move || {
                    cv2.wait(|| pred2.load(Ordering::Acquire)).unwrap();
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(50));

        // First notify: pred still false, the woken waiter
        // re-checks, re-parks. We're checking that notify_one
        // returns 1 (saw a parked slot).
        assert_eq!(cv.notify_one(), 1);
        // Now flip the predicate and wake all so the test exits.
        thread::sleep(Duration::from_millis(20));
        pred.store(true, Ordering::Release);
        cv.notify_all();
        for h in waiters {
            h.join().unwrap();
        }
    }

    #[test]
    fn notify_all_wakes_every_waiter() {
        let cv = Arc::new(SharedCondvar::create_anon().expect("create"));
        let pred = Arc::new(AtomicBool::new(false));
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let cv2 = Arc::clone(&cv);
                let pred2 = Arc::clone(&pred);
                thread::spawn(move || {
                    cv2.wait(|| pred2.load(Ordering::Acquire)).unwrap();
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(30));
        pred.store(true, Ordering::Release);
        let woken = cv.notify_all();
        assert!(woken >= 1, "at least one waiter woken (got {woken})");
        for h in waiters {
            h.join().unwrap();
        }
    }

    #[test]
    fn wait_timeout_returns_timeout() {
        let cv = SharedCondvar::create_anon().expect("create");
        let t0 = Instant::now();
        let err = cv.wait_timeout(|| false, Duration::from_millis(60));
        assert_eq!(err, Err(CondvarError::Timeout));
        assert!(t0.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn wait_returns_immediately_if_predicate_already_true() {
        let cv = SharedCondvar::create_anon().expect("create");
        let pred = AtomicBool::new(true);
        let t0 = Instant::now();
        cv.wait(|| pred.load(Ordering::Acquire)).unwrap();
        assert!(t0.elapsed() < Duration::from_millis(10));
    }

    /// Intra-process file-backed sharing uses Arc::clone (NOT
    /// create+open). The `open` constructor is for callers in
    /// SEPARATE processes joining a file the creator already
    /// initialised; calling `open` in the SAME process as `create`
    /// produces a second mmap with a different virtual-address
    /// range aliased to the same file pages. On Windows that
    /// breaks the wake path because `WaitOnAddress` /
    /// `WakeByAddressSingle` are keyed by virtual address, not by
    /// the underlying file page. Cross-process Linux works via
    /// SHARED `futex` (keyed by inode-offset); see
    /// `examples/condvar_xproc_*.rs` + the matching sweep script
    /// for that path.
    #[test]
    fn file_backed_create_then_arc_clone_round_trip() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("subetha_condvar_test_{}", std::process::id()));
        // Cleanup leftover files from a prior aborted run.
        for suffix in [".waker.bin", ".gen.bin"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            drop(std::fs::remove_file(PathBuf::from(p)));
        }
        let cv = Arc::new(SharedCondvar::create(&path).expect("create"));
        let pred = Arc::new(AtomicBool::new(false));
        let cv2 = Arc::clone(&cv);
        let pred2 = Arc::clone(&pred);
        let waiter = thread::spawn(move || {
            cv2.wait(|| pred2.load(Ordering::Acquire)).unwrap();
        });
        thread::sleep(Duration::from_millis(30));
        pred.store(true, Ordering::Release);
        cv.notify_all();
        waiter.join().unwrap();

        // Cleanup.
        for suffix in [".waker.bin", ".gen.bin"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            drop(std::fs::remove_file(PathBuf::from(p)));
        }
    }
}
