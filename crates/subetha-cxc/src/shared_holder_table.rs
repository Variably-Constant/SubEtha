//! `SharedHolderTable` - a [`HolderTable`] with a file of its own.
//!
//! The bare [`HolderTable`] is a view over slots the caller has already
//! mapped, so it sits inside whatever header layout that caller needs: an
//! epoch counter beside its pins, a payload beside its holders. That is
//! what makes it reusable, and it is also why it cannot be handed to a
//! caller who has nothing to embed it in.
//!
//! This owns a file: a small header carrying the magic and the capacity,
//! then the slots. Everything else is the view's, delegated straight
//! through, so a table reached this way and one embedded in another
//! structure behave identically.
//!
//! What the slots mean is the caller's. The payload is one `u64` per
//! slot, at any encoding that agrees across the processes sharing the
//! file; [`HOLDER_FREE`](crate::holder_table::HOLDER_FREE) and
//! [`HOLDER_RESERVED`](crate::holder_table::HOLDER_RESERVED) are the two values a
//! caller may not use, since the table itself reads them.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::{holder_table_size, HolderSlot, HolderTable};

/// "HOLDERS1": a header followed by the slots.
pub const SHARED_HOLDER_MAGIC: u64 = 0x484F_4C44_4552_5331;

/// The header ahead of the slots. One cache line, so the slots behind it
/// keep the alignment [`HolderTable`] needs.
#[repr(C, align(64))]
struct SharedHolderHeader {
    magic: u64,
    capacity: u64,
    _pad: [u8; 48],
}

const _: () = {
    assert!(size_of::<SharedHolderHeader>() == 64);
};

/// Bytes a table of `capacity` slots needs.
pub const fn shared_holder_file_size(capacity: usize) -> usize {
    size_of::<SharedHolderHeader>() + holder_table_size(capacity)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedHolderError {
    /// The file exists but its magic or capacity is not the one asked
    /// for.
    LayoutMismatch,
    /// A capacity of zero, which has no slot to claim.
    EmptyCapacity,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for SharedHolderError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.kind())
    }
}

impl std::fmt::Display for SharedHolderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SharedHolderError::LayoutMismatch => write!(f, "the file was built for another capacity"),
            SharedHolderError::EmptyCapacity => write!(f, "a capacity of zero has no slot to claim"),
            SharedHolderError::IoError(kind) => write!(f, "io error: {kind:?}"),
        }
    }
}

impl std::error::Error for SharedHolderError {}

/// A holder table backed by a file of its own.
pub struct SharedHolderTable {
    _file: File,
    _mmap: MmapMut,
    table: HolderTable,
    capacity: usize,
}

impl std::fmt::Debug for SharedHolderTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedHolderTable")
            .field("capacity", &self.capacity)
            .field("live", &self.live())
            .finish()
    }
}

fn check_capacity(capacity: usize) -> Result<(), SharedHolderError> {
    if capacity == 0 {
        return Err(SharedHolderError::EmptyCapacity);
    }
    Ok(())
}

/// Build the view over the slots behind the header.
///
/// # Safety
/// `mmap` covers at least `shared_holder_file_size(capacity)` bytes and
/// outlives the returned table.
unsafe fn view(mmap: &MmapMut, capacity: usize) -> HolderTable {
    unsafe { HolderTable::from_ptr(mmap.as_ptr().add(size_of::<SharedHolderHeader>()), capacity) }
}

impl SharedHolderTable {
    /// Obtain the table at `path` with `capacity` slots: an empty one is
    /// laid out when the file does not exist, an existing one is attached
    /// with its claims in place, so a late joiner does not release
    /// anyone.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, SharedHolderError> {
        check_capacity(capacity)?;
        let total = shared_holder_file_size(capacity);
        let file = crate::region_file::create_or_open(path.as_ref())?;
        let fresh = (file.metadata()?.len() as usize) < total;
        if fresh {
            file.set_len(total as u64)?;
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        if fresh {
            // A zeroed region is already a table of free slots, so the
            // magic goes last: an attacher spinning on it must not see
            // it over slots that are not yet zeroed.
            let hdr = mmap.as_mut_ptr() as *mut SharedHolderHeader;
            unsafe {
                std::ptr::write(hdr, SharedHolderHeader { magic: 0, capacity: capacity as u64, _pad: [0; 48] });
                std::ptr::write_bytes(
                    mmap.as_mut_ptr().add(size_of::<SharedHolderHeader>()) as *mut HolderSlot,
                    0,
                    capacity,
                );
                std::ptr::write_volatile(std::ptr::addr_of_mut!((*hdr).magic), SHARED_HOLDER_MAGIC);
            }
        }
        Self::finish(file, mmap, capacity)
    }

    /// Truncate the file at `path` and lay out an empty table, releasing
    /// every slot a live holder owns. For a caller that knows it owns the
    /// path.
    pub fn reset(path: impl AsRef<Path>, capacity: usize) -> Result<Self, SharedHolderError> {
        check_capacity(capacity)?;
        let total = shared_holder_file_size(capacity);
        let file = crate::region_file::create_truncated(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut SharedHolderHeader;
        unsafe {
            std::ptr::write(hdr, SharedHolderHeader { magic: 0, capacity: capacity as u64, _pad: [0; 48] });
            std::ptr::write_bytes(
                mmap.as_mut_ptr().add(size_of::<SharedHolderHeader>()) as *mut HolderSlot,
                0,
                capacity,
            );
            std::ptr::write_volatile(std::ptr::addr_of_mut!((*hdr).magic), SHARED_HOLDER_MAGIC);
        }
        Self::finish(file, mmap, capacity)
    }

    /// Attach to an existing table; the file must be there and must
    /// declare `capacity`.
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self, SharedHolderError> {
        check_capacity(capacity)?;
        let total = shared_holder_file_size(capacity);
        let file = crate::region_file::open_existing(path.as_ref())?;
        // The length is checked before the map: a caller declaring more
        // slots than the file holds is a layout disagreement, and
        // mapping past the end of the file names the symptom instead.
        if (file.metadata()?.len() as usize) < total {
            return Err(SharedHolderError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::finish(file, mmap, capacity)
    }

    fn finish(file: File, mmap: MmapMut, capacity: usize) -> Result<Self, SharedHolderError> {
        let header = unsafe { &*(mmap.as_ptr() as *const SharedHolderHeader) };
        if header.magic != SHARED_HOLDER_MAGIC || header.capacity as usize != capacity {
            return Err(SharedHolderError::LayoutMismatch);
        }
        let table = unsafe { view(&mmap, capacity) };
        Ok(Self { _file: file, _mmap: mmap, table, capacity })
    }

    /// The view the rest of this crate takes, for a caller that already
    /// speaks [`HolderTable`].
    pub fn table(&self) -> &HolderTable {
        &self.table
    }

    /// Slots the table holds.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Claim a slot with no payload decided yet, for a caller whose
    /// payload depends on state it must read after the slot is visible.
    /// [`publish`](Self::publish) fills it in.
    pub fn reserve(&self) -> Option<usize> {
        self.table.reserve()
    }

    /// Fill in the payload of a slot [`reserve`](Self::reserve) took.
    pub fn publish(&self, slot: usize, payload: u64) {
        self.table.publish(slot, payload);
    }

    /// Claim a slot and set its payload in one step, for a caller whose
    /// payload does not depend on anything read between the two.
    pub fn claim(&self, payload: u64) -> Option<usize> {
        self.table.claim(payload)
    }

    /// Claim the slot at `i` in particular, for a caller that has already
    /// decided which one it wants. Answers whether it was free.
    pub fn try_claim_slot(&self, i: usize, payload: u64) -> bool {
        self.table.try_claim_slot(i, payload)
    }

    /// Give a slot back.
    pub fn release(&self, slot: usize) {
        self.table.release(slot);
    }

    /// The payload of a slot, or `None` for one that is free or still
    /// reserved.
    pub fn payload(&self, slot: usize) -> Option<u64> {
        self.table.payload(slot)
    }

    /// Slots held right now, across every process.
    pub fn live(&self) -> usize {
        self.table.live()
    }

    /// Release every slot whose owning process is gone, and report how
    /// many went.
    pub fn reap_dead(&self) -> usize {
        self.table.reap_dead()
    }

    /// The process holding the slot at `i`, or zero for one that is free.
    pub fn owner_pid(&self, i: usize) -> u32 {
        self.table.slot(i).owner_pid.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The raw state word of the slot at `i`: [`HOLDER_FREE`],
    /// [`HOLDER_RESERVED`], or the caller's payload.
    ///
    /// [`HOLDER_FREE`]: crate::holder_table::HOLDER_FREE
    /// [`HOLDER_RESERVED`]: crate::holder_table::HOLDER_RESERVED
    pub fn state(&self, i: usize) -> u64 {
        self.table.slot(i).state.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holder_table::{HOLDER_FREE, HOLDER_RESERVED};

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("subetha-shared-holders-{name}-{}.bin", std::process::id()))
    }

    /// Remove a scratch file if it is there. An absent file is the state
    /// before the first run and after the last; any other refusal means
    /// the scratch directory is not usable and the test result after it
    /// means nothing.
    fn clear(path: &Path) {
        match crate::region_file::remove(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("the scratch table at {} could not be removed: {e}", path.display()),
        }
    }

    #[test]
    fn slots_are_claimed_released_and_handed_out_again() {
        let path = tmp("claims");
        clear(&path);
        let table = SharedHolderTable::create(&path, 4).unwrap();
        assert_eq!(table.capacity(), 4);
        assert_eq!(table.live(), 0);
        assert_eq!(table.state(0), HOLDER_FREE);

        let first = table.claim(7).expect("a free slot");
        let second = table.claim(9).expect("another");
        assert_eq!(table.live(), 2);
        assert_eq!(table.payload(first), Some(7));
        assert_eq!(table.payload(second), Some(9));
        assert_eq!(table.owner_pid(first), std::process::id());
        assert_eq!(table.payload(3), None, "a free slot carries no payload");

        table.release(first);
        assert_eq!(table.live(), 1);
        assert_eq!(table.payload(first), None);
        assert_eq!(table.claim(11), Some(first), "the freed slot comes back");

        // A reservation is held but reports no payload, which is what
        // stops a reader acting on a claim that is not finished.
        let held = table.reserve().expect("a slot to reserve");
        assert_eq!(table.state(held), HOLDER_RESERVED);
        assert_eq!(table.payload(held), None, "a reservation is not a holder yet");
        table.publish(held, 13);
        assert_eq!(table.payload(held), Some(13));

        // Slots zero, one and two are taken; the fourth fills it, and
        // only then does a claim have nowhere to go.
        table.claim(15).expect("the last free slot");
        assert!(table.claim(19).is_none(), "the table is full");
        assert!(!table.try_claim_slot(held, 17), "a held slot refuses a direct claim");
        table.release(held);
        assert!(table.try_claim_slot(held, 17), "and takes one once it is free");
        assert_eq!(table.payload(held), Some(17));
        drop(table);
        clear(&path);
    }

    #[test]
    fn a_second_handle_shares_the_claims_and_another_capacity_is_refused() {
        let path = tmp("share");
        clear(&path);
        let first = SharedHolderTable::create(&path, 4).unwrap();
        let slot = first.claim(42).expect("a free slot");

        let second = SharedHolderTable::open(&path, 4).unwrap();
        assert_eq!(second.payload(slot), Some(42), "the claim is in the file");
        assert_eq!(second.live(), 1);
        second.release(slot);
        assert_eq!(first.live(), 0, "and so is the release");

        match SharedHolderTable::open(&path, 8) {
            Ok(_) => panic!("an eight-slot handle on a four-slot table was accepted"),
            Err(e) => assert_eq!(e, SharedHolderError::LayoutMismatch),
        }
        match SharedHolderTable::create(&path, 0) {
            Ok(_) => panic!("a capacity of zero was accepted"),
            Err(e) => assert_eq!(e, SharedHolderError::EmptyCapacity),
        }

        // A create over an existing table attaches rather than clearing
        // it; a reset is what strips the claims.
        let held = first.claim(99).expect("a free slot");
        drop(second);
        let attached = SharedHolderTable::create(&path, 4).unwrap();
        assert_eq!(attached.payload(held), Some(99), "the create attached");
        drop(attached);
        drop(first);
        let fresh = SharedHolderTable::reset(&path, 4).unwrap();
        assert_eq!(fresh.live(), 0, "the reset released every slot");
        drop(fresh);
        clear(&path);
    }
}
