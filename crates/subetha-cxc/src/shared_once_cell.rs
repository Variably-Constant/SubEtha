//! `SharedOnceCell<T>` - cross-process init-once cell.
//!
//! State machine:
//! - EMPTY (0): no value; first writer to CAS to INITIALIZING wins.
//! - INITIALIZING (1): a writer is filling the payload; other
//!   writers spin until state advances.
//! - INITIALIZED (2): payload is stable; readers may consume.
//!
//! The winner of the EMPTY -> INITIALIZING CAS performs the write
//! and advances to INITIALIZED. Losers see INITIALIZED and read the
//! winner's bytes.
//!
//! This is the cross-process analog of `once_cell::sync::OnceCell`,
//! with cross-process safety guaranteed by the atomic CAS protocol
//! over shared memory.

use std::fs::File;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const ONCE_MAGIC: u32 = 0x4F4E_4346;
pub const ONCE_PAYLOAD_BYTES: usize = 56;

pub const STATE_EMPTY: u8 = 0;
pub const STATE_INITIALIZING: u8 = 1;
pub const STATE_INITIALIZED: u8 = 2;

#[repr(C, align(64))]
pub struct OnceHeader {
    pub magic: u32,
    pub size: u32,
    pub state: AtomicU8,
    pub _pad_to_pid: [u8; 3],
    /// The process that moved the cell to `STATE_INITIALIZING`, or zero.
    /// A claim whose process is gone is one nobody will ever publish, so
    /// the pid is what lets a waiter tell that from a fetch still running.
    pub claimant_pid: AtomicU32,
    pub payload: [u8; ONCE_PAYLOAD_BYTES],
}

pub const ONCE_FILE_SIZE: usize = size_of::<OnceHeader>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedOnceError {
    LayoutMismatch,
    PayloadTooLarge,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for SharedOnceError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedOnceCell<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedOnceCell<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedOnceCell<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedOnceCell<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedOnceCell<T> {
    /// Obtain the cell at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. Attaching
    /// leaves an initialized value in place; a region built for a
    /// different payload type is a `LayoutMismatch`.
    /// [`reset`](Self::reset) reinitializes.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, SharedOnceError> {
        Self::check_layout()?;
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            ONCE_FILE_SIZE,
            |ptr| unsafe { Self::init_region(ptr) },
            |ptr| unsafe { (*(ptr as *const OnceHeader)).magic == ONCE_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, SharedOnceError::LayoutMismatch))?;
        Self::from_region(file, mmap)
    }

    /// Truncate the cell at `path` and initialize an empty one,
    /// discarding whatever value a live peer holds. For a caller that
    /// knows it owns the path.
    pub fn reset(path: impl AsRef<Path>) -> Result<Self, SharedOnceError> {
        Self::check_layout()?;
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), ONCE_FILE_SIZE, |ptr| unsafe {
            Self::init_region(ptr)
        })?;
        Self::from_region(file, mmap)
    }

    /// Lay out an empty cell: the zeroed region is already
    /// `STATE_EMPTY` with a zero payload, so only the size and then the
    /// magic are written, magic last, because attachers spin on it.
    ///
    /// # Safety
    /// `ptr` addresses at least [`ONCE_FILE_SIZE`] writable zeroed
    /// bytes.
    unsafe fn init_region(ptr: *mut u8) {
        let hdr = ptr as *mut OnceHeader;
        unsafe {
            (*hdr).size = size_of::<T>() as u32;
            std::ptr::write_volatile(&raw mut (*hdr).magic, ONCE_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one built for a different
    /// payload type.
    fn from_region(file: File, mmap: MmapMut) -> Result<Self, SharedOnceError> {
        let header = unsafe { &*(mmap.as_ptr() as *const OnceHeader) };
        if header.magic != ONCE_MAGIC || header.size as usize != size_of::<T>() {
            return Err(SharedOnceError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedOnceError> {
        Self::check_layout()?;
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < ONCE_FILE_SIZE as u64 {
            return Err(SharedOnceError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(ONCE_FILE_SIZE).map_mut(&file)? };
        Self::from_region(file, mmap)
    }

    fn check_layout() -> Result<(), SharedOnceError> {
        if size_of::<T>() > ONCE_PAYLOAD_BYTES {
            return Err(SharedOnceError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(SharedOnceError::PayloadTooLarge);
        }
        Ok(())
    }

    fn header(&self) -> &OnceHeader {
        unsafe { &*(self.mmap.as_ptr() as *const OnceHeader) }
    }

    /// True when the cell has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.header().state.load(Ordering::Acquire) == STATE_INITIALIZED
    }

    /// Get the value if initialized; otherwise return None.
    /// Non-blocking; never invokes the initializer.
    pub fn get(&self) -> Option<T> {
        let header = self.header();
        if header.state.load(Ordering::Acquire) != STATE_INITIALIZED {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::cell::OP_GET, 2); // empty / uninitialized
            return None;
        }
        let value: T = unsafe {
            let src = header.payload.as_ptr() as *const T;
            std::ptr::read_unaligned(src)
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::cell::OP_GET, 0);
        Some(value)
    }

    /// Try to write the value. Returns `true` if this caller won
    /// the init race, `false` if the cell was already initialized
    /// or another init is in progress.
    pub fn set(&self, value: T) -> bool {
        let header = self.header();
        if header.state.compare_exchange(
            STATE_EMPTY, STATE_INITIALIZING,
            Ordering::AcqRel, Ordering::Acquire,
        ).is_err() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::cell::OP_SET, 1); // lost the init race
            return false;
        }
        unsafe {
            let dst = header.payload.as_ptr() as *mut T;
            std::ptr::write_unaligned(dst, value);
        }
        header.state.store(STATE_INITIALIZED, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::cell::OP_SET, 0);
        true
    }

    /// Get the cached value, or run `init` to produce it. The first
    /// caller across all processes runs `init`; subsequent callers
    /// spin until the value is published and return that value.
    pub fn get_or_init<F: FnOnce() -> T>(&self, init: F) -> T {
        if let Some(v) = self.get() { return v; }
        let header = self.header();
        match header.state.compare_exchange(
            STATE_EMPTY, STATE_INITIALIZING,
            Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => {
                // We won; produce the value and publish.
                let v = init();
                unsafe {
                    let dst = header.payload.as_ptr() as *mut T;
                    std::ptr::write_unaligned(dst, v);
                }
                header.state.store(STATE_INITIALIZED, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::cell::OP_SET, 0);
                v
            }
            Err(_) => {
                // Spin until the winner publishes.
                while header.state.load(Ordering::Acquire) != STATE_INITIALIZED {
                    std::hint::spin_loop();
                }
                self.get().expect("INITIALIZED implies value present")
            }
        }
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), SharedOnceError> {
        self.mmap.flush_async()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), SharedOnceError> {
        self.mmap.flush()?;
        Ok(())
    }
}

/// Why a wait for a published value gave up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    /// The deadline passed with a claim still outstanding.
    TimedOut,
    /// The claim belongs to a process that is gone.
    ClaimantGone,
}

/// A once-cell whose payload size is given at run time and whose
/// initialization is three steps: claim the right to produce the value,
/// produce it, publish it. Callers that lose the claim wait for the
/// winner's value, so the value is produced once however many processes
/// ask at once. This is the form a caller with no closures uses.
///
/// # A claim spans the caller's own code
///
/// A claim stands from `claim` until `publish`, across whatever the
/// caller does to produce the value, so a claimant can die holding one.
/// The claim carries the process that took it: [`wait`] answers
/// `ClaimantGone` when that process is gone, and [`reclaim`] returns the
/// cell to empty for the next caller.
///
/// [`wait`]: Self::wait
/// [`reclaim`]: Self::reclaim
pub struct SharedOnceCellDyn {
    _file: File,
    mmap: MmapMut,
    value_bytes: usize,
}

unsafe impl Send for SharedOnceCellDyn {}
unsafe impl Sync for SharedOnceCellDyn {}

impl std::fmt::Debug for SharedOnceCellDyn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedOnceCellDyn")
            .field("value_len", &self.value_bytes)
            .field("state", &self.state())
            .finish()
    }
}

impl SharedOnceCellDyn {
    /// Obtain the cell at `path` for a payload of `value_bytes`,
    /// initializing an empty one when the path does not exist. A payload
    /// past [`ONCE_PAYLOAD_BYTES`] is a `PayloadTooLarge`.
    pub fn create(path: impl AsRef<Path>, value_bytes: usize) -> Result<Self, SharedOnceError> {
        if value_bytes == 0 || value_bytes > ONCE_PAYLOAD_BYTES {
            return Err(SharedOnceError::PayloadTooLarge);
        }
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            ONCE_FILE_SIZE,
            |ptr| unsafe {
                let hdr = ptr as *mut OnceHeader;
                (*hdr).size = value_bytes as u32;
                std::ptr::write_volatile(&raw mut (*hdr).magic, ONCE_MAGIC);
            },
            |ptr| unsafe { (*(ptr as *const OnceHeader)).magic == ONCE_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, SharedOnceError::LayoutMismatch))?;
        Self::from_region(file, mmap, value_bytes)
    }

    /// Attach to the cell another process created at `path`, whose
    /// payload must be `value_bytes` long.
    pub fn open(path: impl AsRef<Path>, value_bytes: usize) -> Result<Self, SharedOnceError> {
        if value_bytes == 0 || value_bytes > ONCE_PAYLOAD_BYTES {
            return Err(SharedOnceError::PayloadTooLarge);
        }
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < ONCE_FILE_SIZE as u64 {
            return Err(SharedOnceError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(ONCE_FILE_SIZE).map_mut(&file)? };
        Self::from_region(file, mmap, value_bytes)
    }

    fn from_region(file: File, mmap: MmapMut, value_bytes: usize) -> Result<Self, SharedOnceError> {
        let header = unsafe { &*(mmap.as_ptr() as *const OnceHeader) };
        if header.magic != ONCE_MAGIC || header.size as usize != value_bytes {
            return Err(SharedOnceError::LayoutMismatch);
        }
        Ok(Self { _file: file, mmap, value_bytes })
    }

    fn header(&self) -> &OnceHeader {
        unsafe { &*(self.mmap.as_ptr() as *const OnceHeader) }
    }

    /// Bytes the payload holds.
    #[inline]
    pub fn value_len(&self) -> usize {
        self.value_bytes
    }

    /// One of `STATE_EMPTY`, `STATE_INITIALIZING` or `STATE_INITIALIZED`.
    pub fn state(&self) -> u8 {
        self.header().state.load(Ordering::Acquire)
    }

    /// The published value into `out`, or `false` while none is
    /// published. Non-blocking, and takes no claim.
    pub fn try_get(&self, out: &mut [u8]) -> bool {
        let header = self.header();
        if header.state.load(Ordering::Acquire) != STATE_INITIALIZED {
            return false;
        }
        let len = self.value_bytes.min(out.len());
        out[..len].copy_from_slice(&header.payload[..len]);
        true
    }

    /// Take the right to produce the value, stamping the claim with
    /// `pid`. `true` to the one caller that wins.
    pub fn claim(&self, pid: u32) -> bool {
        let header = self.header();
        if header
            .state
            .compare_exchange(STATE_EMPTY, STATE_INITIALIZING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        header.claimant_pid.store(pid, Ordering::Release);
        true
    }

    /// Publish the value and release the claim held by `pid`. `false`
    /// unless that claim is the one standing.
    pub fn publish(&self, pid: u32, value: &[u8]) -> bool {
        let header = self.header();
        if header.state.load(Ordering::Acquire) != STATE_INITIALIZING {
            return false;
        }
        if header.claimant_pid.load(Ordering::Acquire) != pid {
            return false;
        }
        let len = self.value_bytes.min(value.len());
        unsafe {
            let dst = header.payload.as_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(value.as_ptr(), dst, len);
        }
        header.claimant_pid.store(0, Ordering::Release);
        header.state.store(STATE_INITIALIZED, Ordering::Release);
        true
    }

    /// Wait for a published value into `out` until `deadline`.
    pub fn wait(&self, out: &mut [u8], deadline: std::time::Instant) -> Result<(), WaitError> {
        let header = self.header();
        loop {
            if self.try_get(out) {
                return Ok(());
            }
            if header.state.load(Ordering::Acquire) == STATE_INITIALIZING {
                let pid = header.claimant_pid.load(Ordering::Acquire);
                if pid != 0 && !crate::peer_directory::process_alive(pid) {
                    return Err(WaitError::ClaimantGone);
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(WaitError::TimedOut);
            }
            std::thread::yield_now();
        }
    }

    /// Break a claim whose process is gone, returning the cell to empty.
    pub fn reclaim(&self) -> bool {
        let header = self.header();
        if header.state.load(Ordering::Acquire) != STATE_INITIALIZING {
            return false;
        }
        let pid = header.claimant_pid.load(Ordering::Acquire);
        if pid == 0 || crate::peer_directory::process_alive(pid) {
            return false;
        }
        if header
            .state
            .compare_exchange(STATE_INITIALIZING, STATE_EMPTY, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            header.claimant_pid.store(0, Ordering::Release);
            return true;
        }
        false
    }

    pub fn flush(&self) -> Result<(), SharedOnceError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-once-{name}-{}.bin", std::process::id()))
    }

    /// One claim is granted and the rest are refused, so the value is
    /// produced once however many callers arrive.
    #[test]
    fn one_caller_claims_and_the_others_wait_for_its_value() {
        let p = tmp("dyn-claim");
        let a = SharedOnceCellDyn::create(&p, 4).unwrap();
        let b = SharedOnceCellDyn::open(&p, 4).unwrap();
        assert_eq!(a.state(), STATE_EMPTY);

        let mine = std::process::id();
        assert!(a.claim(mine), "the first caller takes the claim");
        assert!(!b.claim(mine), "the second is refused while one stands");
        assert_eq!(b.state(), STATE_INITIALIZING);

        let mut out = [0u8; 4];
        assert!(!b.try_get(&mut out), "nothing is published yet");
        assert!(
            !b.publish(mine + 1, &[9, 9, 9, 9]),
            "a publish under a pid that does not hold the claim is refused",
        );

        assert!(a.publish(mine, &[1, 2, 3, 4]));
        assert!(b.try_get(&mut out));
        assert_eq!(out, [1, 2, 3, 4], "the waiter reads the claimant's value");
        assert_eq!(b.state(), STATE_INITIALIZED);

        // Once published, nobody claims again.
        assert!(!a.claim(mine));
    }

    /// A wait against a published value returns at once.
    #[test]
    fn a_wait_on_a_published_value_returns_it() {
        let p = tmp("dyn-wait");
        let c = SharedOnceCellDyn::create(&p, 2).unwrap();
        let mine = std::process::id();
        assert!(c.claim(mine));
        assert!(c.publish(mine, &[7, 8]));
        let mut out = [0u8; 2];
        c.wait(&mut out, std::time::Instant::now() + std::time::Duration::from_millis(50))
            .expect("the value is there");
        assert_eq!(out, [7, 8]);
    }

    /// A claim held by a live process outlasts a deadline, and the waiter
    /// is told the deadline passed.
    #[test]
    fn a_wait_under_a_live_claim_times_out() {
        let p = tmp("dyn-timeout");
        let c = SharedOnceCellDyn::create(&p, 2).unwrap();
        assert!(c.claim(std::process::id()), "this process holds it and is alive");
        let mut out = [0u8; 2];
        let began = std::time::Instant::now();
        assert_eq!(
            c.wait(&mut out, began + std::time::Duration::from_millis(40)).unwrap_err(),
            WaitError::TimedOut,
        );
        assert!(began.elapsed() >= std::time::Duration::from_millis(30));
        assert!(!c.reclaim(), "a live claimant keeps its claim");
    }

    /// A claim stamped with a pid that names no process is one nobody
    /// will publish: the waiter is told so rather than waiting out its
    /// deadline, and the claim can be broken for the next caller.
    #[test]
    fn a_claim_whose_process_is_gone_is_reported_and_reclaimed() {
        let p = tmp("dyn-gone");
        let c = SharedOnceCellDyn::create(&p, 2).unwrap();
        // A pid this high is not a live process on any of the gate hosts.
        let absent = 0x7FFF_FFF0u32;
        assert!(c.claim(absent));

        let mut out = [0u8; 2];
        let began = std::time::Instant::now();
        assert_eq!(
            c.wait(&mut out, began + std::time::Duration::from_secs(30)).unwrap_err(),
            WaitError::ClaimantGone,
        );
        assert!(began.elapsed() < std::time::Duration::from_secs(5), "it did not wait out the deadline");

        assert!(c.reclaim(), "the abandoned claim is broken");
        assert_eq!(c.state(), STATE_EMPTY);
        let mine = std::process::id();
        assert!(c.claim(mine), "the next caller may claim it");
        assert!(c.publish(mine, &[4, 5]));
        assert!(c.try_get(&mut out));
        assert_eq!(out, [4, 5]);
    }

    /// The payload's size is part of the layout, so a size that
    /// disagrees is refused, and one past the header's room is too.
    #[test]
    fn a_dyn_cell_at_the_wrong_size_is_refused() {
        let p = tmp("dyn-size");
        let _c = SharedOnceCellDyn::create(&p, 8).unwrap();
        assert_eq!(SharedOnceCellDyn::open(&p, 4).unwrap_err(), SharedOnceError::LayoutMismatch);
        assert_eq!(
            SharedOnceCellDyn::create(tmp("dyn-big"), ONCE_PAYLOAD_BYTES + 1).unwrap_err(),
            SharedOnceError::PayloadTooLarge,
        );
        assert_eq!(
            SharedOnceCellDyn::create(tmp("dyn-zero"), 0).unwrap_err(),
            SharedOnceError::PayloadTooLarge,
        );
    }

    #[test]
    fn fresh_cell_is_empty() {
        let p = tmp("fresh");
        let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert!(!c.is_initialized());
        assert_eq!(c.get(), None);
    }

    #[test]
    fn first_set_wins_subsequent_sets_lose() {
        let p = tmp("first-wins");
        let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert!(c.set(42));
        assert!(!c.set(99));
        assert_eq!(c.get(), Some(42));
    }

    /// A second create attaches with the initialized value in place -
    /// truncation here would break the once guarantee; reset is what
    /// strips it.
    #[test]
    fn second_create_attaches_and_keeps_the_value() {
        let p = tmp("attach");
        let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert!(c.set(42));

        let c2: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert_eq!(c2.get(), Some(42), "attach lost the initialized value");
        assert!(!c2.set(99), "attach reopened a spent cell");

        // Windows refuses to truncate a mapped file, so every handle goes
        // before the reset.
        drop(c);
        drop(c2);
        let fresh: SharedOnceCell<u64> = SharedOnceCell::reset(&p).unwrap();
        assert_eq!(fresh.get(), None, "reset left a value behind");
        assert!(fresh.set(7));
        drop(fresh);
    }

    /// Attaching with a different payload type is refused.
    #[test]
    fn create_refuses_a_mismatched_region() {
        let p = tmp("mismatch");
        let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        assert!(matches!(
            SharedOnceCell::<u32>::create(&p),
            Err(SharedOnceError::LayoutMismatch),
        ));
        drop(c);
    }

    #[test]
    fn cross_handle_init_visible_to_other_handle() {
        let p = tmp("cross-handle");
        let writer: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        let reader: SharedOnceCell<u64> = SharedOnceCell::open(&p).unwrap();
        assert!(!reader.is_initialized());
        writer.set(7777);
        assert!(reader.is_initialized());
        assert_eq!(reader.get(), Some(7777));
    }

    #[test]
    fn get_or_init_runs_closure_at_most_once() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;
        use std::thread;
        let p = tmp("get-or-init");
        let c: Arc<SharedOnceCell<u64>> = Arc::new(SharedOnceCell::create(&p).unwrap());
        let runs = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];
        for _ in 0..8 {
            let c = c.clone();
            let runs = runs.clone();
            handles.push(thread::spawn(move || {
                c.get_or_init(|| {
                    runs.fetch_add(1, Ordering::AcqRel);
                    1234u64
                })
            }));
        }
        let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(results.iter().all(|v| *v == 1234));
        assert_eq!(runs.load(Ordering::Acquire), 1,
                   "init closure must run exactly once across 8 threads");
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
            c.set(8888);
            c.flush().unwrap();
        }
        let c2: SharedOnceCell<u64> = SharedOnceCell::open(&p).unwrap();
        assert_eq!(c2.get(), Some(8888));
        assert!(c2.is_initialized());
        // Set must fail on reopen because the cell is already init.
        assert!(!c2.set(9999));
        assert_eq!(c2.get(), Some(8888));
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // size_of<Big> is the test signal, not the field
        struct Big([u8; ONCE_PAYLOAD_BYTES + 1]);
        impl Copy for Big {}
        impl Clone for Big { fn clone(&self) -> Self { *self } }
        let p = tmp("too-large");
        match SharedOnceCell::<Big>::create(&p) {
            Err(SharedOnceError::PayloadTooLarge) => {}
            other => panic!("expected PayloadTooLarge, got {:?}", other.as_ref().err()),
        }
    }

    #[test]
    fn open_rejects_wrong_payload_size() {
        let p = tmp("wrong-size");
        let _c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
        match SharedOnceCell::<u32>::open(&p) {
            Err(SharedOnceError::LayoutMismatch) => {}
            other => panic!("expected LayoutMismatch, got {:?}", other.as_ref().err()),
        }
    }
}
