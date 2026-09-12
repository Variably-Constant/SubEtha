//! `RawOwnerLease` - [`OwnerLease`](crate::owner_lease::OwnerLease) at a
//! payload size given at run time rather than by a type parameter.
//!
//! The typed lease is generic over `T: Copy` and records `size_of::<T>()`
//! in the region's header, refusing an attach that declares anything
//! else. A caller that only learns its payload size at run time - the C
//! ABI, where the size arrives as an argument - cannot name a `T`, so
//! this carries the size as a field and validates it against the same
//! header field.
//!
//! The layout, the magic and every rule about them are the typed lease's:
//! a lease created here at eight bytes and an `OwnerLease<u64>` are the
//! same region, and either refuses a handle declaring another size. What
//! differs is only how the size reaches the code.
//!
//! Ownership, preemption by lower pid, the grace window on a stale
//! heartbeat, and the SeqLock around the payload all behave exactly as
//! they do in the typed lease, because this reads and writes the same
//! header fields in the same order.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::Ordering;

use memmap2::{MmapMut, MmapOptions};

use crate::owner_lease::{
    begin_lease_region, publish_lease_magic, LeaseError, LeaseHeader, LEASE_FILE_SIZE, LEASE_MAGIC, NO_OWNER,
    PAYLOAD_BYTES,
};

/// A lease over a payload whose size is known at run time.
pub struct RawOwnerLease {
    _file: File,
    mmap: MmapMut,
    payload_size: usize,
}

impl std::fmt::Debug for RawOwnerLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawOwnerLease")
            .field("payload_size", &self.payload_size)
            .field("owner", &self.current_owner())
            .field("term", &self.lease_term())
            .finish()
    }
}

unsafe impl Send for RawOwnerLease {}
unsafe impl Sync for RawOwnerLease {}

fn check_size(payload_size: usize) -> Result<(), LeaseError> {
    if payload_size == 0 || payload_size > PAYLOAD_BYTES {
        return Err(LeaseError::PayloadTooLarge);
    }
    Ok(())
}

/// Lay out a fresh lease region and copy `initial` into its payload. The
/// layout itself is the typed lease's, through the same two functions, so
/// the two cannot drift into disagreeing about the same file.
///
/// # Safety
/// `ptr` addresses at least [`LEASE_FILE_SIZE`] writable bytes, and
/// `payload_size` is no larger than [`PAYLOAD_BYTES`].
unsafe fn init_region(ptr: *mut u8, initial: &[u8], payload_size: usize) {
    unsafe {
        let payload = begin_lease_region(ptr, payload_size);
        std::ptr::copy_nonoverlapping(initial.as_ptr(), payload, initial.len().min(payload_size));
        publish_lease_magic(ptr);
    }
}

impl RawOwnerLease {
    /// Obtain the lease at `path`, initializing it with `initial` only
    /// when the path does not yet exist. Attaching leaves the current
    /// owner and term in place and `initial` goes unused. A region built
    /// for another payload size is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, initial: &[u8], payload_size: usize) -> Result<Self, LeaseError> {
        check_size(payload_size)?;
        if initial.len() > payload_size {
            return Err(LeaseError::PayloadTooLarge);
        }
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            LEASE_FILE_SIZE,
            |ptr| unsafe { init_region(ptr, initial, payload_size) },
            |ptr| unsafe { (*(ptr as *const LeaseHeader)).magic == LEASE_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, LeaseError::LayoutMismatch))?;
        let hdr = unsafe { &*(mmap.as_ptr() as *const LeaseHeader) };
        if hdr.payload_size as usize != payload_size {
            return Err(LeaseError::LayoutMismatch);
        }
        Ok(Self { _file: file, mmap, payload_size })
    }

    /// Reinitialize the lease at `path`, stripping whatever owner and
    /// term a live holder has. For a caller that knows it owns the path.
    pub fn reset(path: impl AsRef<Path>, initial: &[u8], payload_size: usize) -> Result<Self, LeaseError> {
        check_size(payload_size)?;
        if initial.len() > payload_size {
            return Err(LeaseError::PayloadTooLarge);
        }
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), LEASE_FILE_SIZE, |ptr| unsafe {
            init_region(ptr, initial, payload_size)
        })?;
        Ok(Self { _file: file, mmap, payload_size })
    }

    /// Attach to an existing lease; the file must be there and must
    /// declare `payload_size`.
    pub fn open(path: impl AsRef<Path>, payload_size: usize) -> Result<Self, LeaseError> {
        check_size(payload_size)?;
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < LEASE_FILE_SIZE as u64 {
            return Err(LeaseError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(LEASE_FILE_SIZE).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const LeaseHeader) };
        if hdr.magic != LEASE_MAGIC || hdr.payload_size as usize != payload_size {
            return Err(LeaseError::LayoutMismatch);
        }
        Ok(Self { _file: file, mmap, payload_size })
    }

    /// Bytes the payload holds, which every handle on this region agrees
    /// on.
    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    fn header(&self) -> &LeaseHeader {
        unsafe { &*(self.mmap.as_ptr() as *const LeaseHeader) }
    }

    fn payload_ptr(&self) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(size_of::<LeaseHeader>()) as *mut u8 }
    }

    /// Try to claim ownership. Succeeds when there is no current owner,
    /// when `my_pid` is below the current owner's and so preempts it, or
    /// when the current owner's heartbeat is more than `grace_epochs`
    /// behind the global epoch.
    pub fn try_acquire(&self, my_pid: u32, grace_epochs: u64) -> bool {
        assert!(my_pid != NO_OWNER, "pid 0 is reserved for NO_OWNER");
        let header = self.header();
        loop {
            let cur = header.owner_pid.load(Ordering::Acquire);
            let can_claim = if cur == NO_OWNER {
                true
            } else if cur == my_pid {
                return true;
            } else if my_pid < cur {
                true
            } else {
                let beat = header.heartbeat_epoch.load(Ordering::Acquire);
                let global = header.global_epoch.load(Ordering::Acquire);
                global.saturating_sub(beat) > grace_epochs
            };
            if !can_claim {
                return false;
            }
            if header
                .owner_pid
                .compare_exchange(cur, my_pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                header.lease_term.fetch_add(1, Ordering::AcqRel);
                let global = header.global_epoch.load(Ordering::Acquire);
                header.heartbeat_epoch.store(global, Ordering::Release);
                return true;
            }
            std::hint::spin_loop();
        }
    }

    /// Give up ownership. Answers whether this caller held it.
    pub fn release(&self, my_pid: u32) -> bool {
        self.header()
            .owner_pid
            .compare_exchange(my_pid, NO_OWNER, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Copy the payload into `out` when the caller holds the lease.
    /// Answers whether it did; `out` is untouched when it did not.
    pub fn read_as_owner(&self, my_pid: u32, out: &mut [u8]) -> bool {
        if !self.am_i_owner(my_pid) {
            return false;
        }
        let n = self.payload_size.min(out.len());
        // SAFETY: the payload is `PAYLOAD_BYTES` long and `n` is inside
        // the declared size, which is no larger.
        unsafe {
            std::ptr::copy_nonoverlapping(self.payload_ptr(), out.as_mut_ptr(), n);
        }
        true
    }

    /// Overwrite the payload when the caller holds the lease, under the
    /// SeqLock so a reader never sees half of each value. Answers whether
    /// it did.
    pub fn write_as_owner(&self, my_pid: u32, value: &[u8]) -> bool {
        if !self.am_i_owner(my_pid) {
            return false;
        }
        let header = self.header();
        let n = self.payload_size.min(value.len());
        header.seq_version.fetch_add(1, Ordering::AcqRel);
        // SAFETY: as `read_as_owner`.
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), self.payload_ptr(), n);
        }
        header.seq_version.fetch_add(1, Ordering::Release);
        true
    }

    /// Refresh the heartbeat, so a grace window does not expire under a
    /// holder that is still alive. Answers `false` once the caller has
    /// been preempted.
    pub fn beat(&self, my_pid: u32) -> bool {
        let header = self.header();
        if header.owner_pid.load(Ordering::Acquire) != my_pid {
            return false;
        }
        let global = header.global_epoch.load(Ordering::Acquire);
        header.heartbeat_epoch.store(global, Ordering::Release);
        true
    }

    /// Advance the global epoch and return it. The grace window is
    /// measured against this, so something has to tick it for a stale
    /// heartbeat to become stale.
    pub fn tick_epoch(&self) -> u64 {
        self.header().global_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// The global epoch, which the grace window is measured against.
    pub fn global_epoch(&self) -> u64 {
        self.header().global_epoch.load(Ordering::Acquire)
    }

    /// The epoch the owner last beat at.
    pub fn heartbeat_epoch(&self) -> u64 {
        self.header().heartbeat_epoch.load(Ordering::Acquire)
    }

    /// The pid holding the lease, or `None` when nobody does.
    pub fn current_owner(&self) -> Option<u32> {
        let pid = self.header().owner_pid.load(Ordering::Acquire);
        if pid == NO_OWNER { None } else { Some(pid) }
    }

    /// Whether `my_pid` holds the lease right now.
    pub fn am_i_owner(&self, my_pid: u32) -> bool {
        self.header().owner_pid.load(Ordering::Acquire) == my_pid
    }

    /// How many times the lease has changed hands.
    pub fn lease_term(&self) -> u32 {
        self.header().lease_term.load(Ordering::Acquire)
    }

    /// The SeqLock version, odd while a write is in flight.
    pub fn seq_version(&self) -> u32 {
        self.header().seq_version.load(Ordering::Acquire)
    }

    pub fn flush(&self) -> Result<(), LeaseError> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), LeaseError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner_lease::OwnerLease;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("subetha-raw-lease-{name}-{}.bin", std::process::id()))
    }

    /// Remove a scratch file if it is there. An absent file is the state
    /// before the first run and after the last; any other refusal means
    /// the scratch directory is not usable and the test result after it
    /// means nothing.
    fn clear(path: &Path) {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("the scratch lease at {} could not be removed: {e}", path.display()),
        }
    }

    #[test]
    fn a_lease_changes_hands_and_the_payload_travels_with_it() {
        let path = tmp("hands");
        clear(&path);
        let lease = RawOwnerLease::create(&path, &[7u8; 8], 8).unwrap();
        assert_eq!(lease.payload_size(), 8);
        assert_eq!(lease.current_owner(), None);
        assert_eq!(lease.lease_term(), 0);

        assert!(lease.try_acquire(100, 3), "an unheld lease is taken");
        assert_eq!(lease.current_owner(), Some(100));
        assert_eq!(lease.lease_term(), 1);
        let mut out = [0u8; 8];
        assert!(lease.read_as_owner(100, &mut out), "the owner reads it");
        assert_eq!(out, [7u8; 8], "the initial payload is there");
        assert!(!lease.read_as_owner(200, &mut out), "a non-owner does not");
        assert!(lease.write_as_owner(100, &[9u8; 8]));
        assert!(!lease.write_as_owner(200, &[1u8; 8]), "nor does it write");

        // A higher pid cannot take it from a live holder, but a lower one
        // preempts, and the payload the previous holder left is there.
        assert!(!lease.try_acquire(200, 3), "a higher pid waits its turn");
        assert!(lease.try_acquire(50, 3), "a lower pid preempts");
        assert_eq!(lease.current_owner(), Some(50));
        assert_eq!(lease.lease_term(), 2);
        assert!(lease.read_as_owner(50, &mut out));
        assert_eq!(out, [9u8; 8], "the payload survived the handover");

        assert!(lease.release(50));
        assert_eq!(lease.current_owner(), None);
        assert!(!lease.release(50), "releasing twice answers false");
        drop(lease);
        clear(&path);
    }

    #[test]
    fn a_stale_heartbeat_lets_a_higher_pid_take_over() {
        let path = tmp("stale");
        clear(&path);
        let lease = RawOwnerLease::create(&path, &[0u8; 4], 4).unwrap();
        assert!(lease.try_acquire(100, 3));

        // Inside the grace window the holder keeps it.
        for _ in 0..3 {
            lease.tick_epoch();
        }
        assert!(!lease.try_acquire(200, 3), "three epochs is still inside a grace of three");

        // Past it, a higher pid takes over; a beat from the holder puts
        // it back out of reach.
        lease.tick_epoch();
        assert!(lease.global_epoch() - lease.heartbeat_epoch() > 3);
        assert!(lease.try_acquire(200, 3), "the stale holder is displaced");
        assert_eq!(lease.current_owner(), Some(200));
        lease.tick_epoch();
        assert!(lease.beat(200), "the new holder beats");
        assert!(!lease.try_acquire(300, 3), "and is no longer stale");
        drop(lease);
        clear(&path);
    }

    /// The raw lease and the typed one are the same region: a payload
    /// written through one is read through the other, and a handle
    /// declaring another size is refused by both.
    #[test]
    fn the_raw_lease_and_the_typed_one_share_a_region() {
        let path = tmp("interop");
        clear(&path);
        let raw = RawOwnerLease::create(&path, &0u64.to_le_bytes(), 8).unwrap();
        let typed: OwnerLease<u64> = OwnerLease::open(&path).unwrap();

        assert!(raw.try_acquire(100, 3));
        assert!(raw.write_as_owner(100, &0x0102_0304_0506_0708u64.to_le_bytes()));
        assert_eq!(typed.read_as_owner(100), Some(0x0102_0304_0506_0708u64), "the typed handle reads it");
        assert!(typed.write_as_owner(100, 42));
        let mut out = [0u8; 8];
        assert!(raw.read_as_owner(100, &mut out));
        assert_eq!(u64::from_le_bytes(out), 42, "and the raw handle reads what it wrote");
        assert!(raw.release(100));

        match RawOwnerLease::open(&path, 4) {
            Ok(_) => panic!("a four-byte handle on an eight-byte lease was accepted"),
            Err(e) => assert_eq!(e, LeaseError::LayoutMismatch),
        }
        match OwnerLease::<u32>::open(&path) {
            Ok(_) => panic!("a u32 handle on an eight-byte lease was accepted"),
            Err(e) => assert_eq!(e, LeaseError::LayoutMismatch),
        }
        drop(typed);
        drop(raw);
        clear(&path);
    }

    #[test]
    fn a_payload_past_the_ceiling_is_refused() {
        let path = tmp("ceiling");
        clear(&path);
        match RawOwnerLease::create(&path, &[0u8; 8], PAYLOAD_BYTES + 1) {
            Ok(_) => panic!("a payload past the ceiling was accepted"),
            Err(e) => assert_eq!(e, LeaseError::PayloadTooLarge),
        }
        match RawOwnerLease::create(&path, &[0u8; 8], 0) {
            Ok(_) => panic!("a zero payload was accepted"),
            Err(e) => assert_eq!(e, LeaseError::PayloadTooLarge),
        }
        match RawOwnerLease::create(&path, &[0u8; 16], 8) {
            Ok(_) => panic!("an initial value past the declared size was accepted"),
            Err(e) => assert_eq!(e, LeaseError::PayloadTooLarge),
        }
        clear(&path);
    }
}
