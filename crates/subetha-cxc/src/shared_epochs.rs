//! `SharedEpochs` - a cross-process epoch counter and the pins held
//! against it, so a scan reads a fixed view while writers keep running.
//!
//! A writer that supersedes a record leaves the old version in place and
//! stamps it with the epoch at which it stopped being current. A scan
//! PINS the epoch it started at and reads, for each record, the version
//! that was current then. A superseded version is reclaimable only once
//! no pin sits at or below the epoch it was retired in, which
//! [`SharedEpochs::reclaim_horizon`] reports.
//!
//! Writers never wait for a scan. The cost is retention: a long scan
//! holds a low pin, and every version superseded since stays until it
//! finishes.
//!
//! # The pins are in the mapping
//!
//! Both the counter and the pin table live in shared memory, so every
//! attached process sees every pin. A pin is a claim against
//! reclamation and the party that reclaims may be a different process
//! from the one scanning; a pin table private to one of them is
//! invisible to the other, which reclaims a version that scan is still
//! reading and loses rows with nothing reporting it.
//!
//! # Layout
//!
//! ```text
//! | EpochHeader (64B) | HolderSlot 0 (64B) | HolderSlot 1 (64B) | ... |
//! ```
//!
//! The pins are a [`HolderTable`] over the slots behind the header. One
//! slot holds one pin, and its payload is `epoch + 1` - the bias is
//! what lets epoch 0 be pinned while
//! [`HOLDER_FREE`](crate::holder_table::HOLDER_FREE) still means
//! unclaimed. `capacity` is therefore the number of pins that may be
//! held at once, and the caller chooses it at create.
//!
//! # A pin whose process died
//!
//! A slot is released when its [`PinGuard`] drops. A process that
//! crashes mid-scan leaves its slot claimed, and that pin holds the
//! horizon down for as long as it stands. The holder table's pid stamp
//! resolves it: [`reap_dead_pins`](SharedEpochs::reap_dead_pins) frees
//! every slot whose owning process is gone, and
//! [`pin`](SharedEpochs::pin) calls it before reporting the table full,
//! so the ordinary path recovers without the caller arranging anything.
//! The horizon itself reads only atomics, so it costs no liveness
//! probe.
//!
//! # Tickets: one epoch for a compound write
//!
//! A write that touches several entries - or several structures sharing
//! one table - stamps them all with ONE epoch, taken from a
//! [`EpochTicket`] returned by [`begin`](SharedEpochs::begin). While the
//! ticket is open its epoch is reserved and not yet published:
//! [`now`](SharedEpochs::now), which is what a pin takes, stays at the
//! highest epoch every lower ticket has published, so a scan started
//! mid-compound pins below it and sees all-old; a scan started after
//! [`publish`](EpochTicket::publish) sees all-new. The reclaim horizon
//! is bounded by the same value, so a reclaimer never frees a version an
//! open compound is about to supersede.
//!
//! Tickets are a second [`HolderTable`] beside the pins, stamped with
//! the holder's process. A ticket whose process died mid-compound is
//! not simply freed - freeing it would publish a half-written epoch.
//! [`dead_tickets`](SharedEpochs::dead_tickets) names such epochs so
//! each structure sharing the table can void what it holds at them
//! (the versioned map's `void_epoch`), and
//! [`free_dead_ticket`](SharedEpochs::free_dead_ticket) releases the
//! slot once every structure has. Dropping a ticket publishes it: a
//! caller abandoning a compound removes what it wrote first.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::{holder_table_size, HolderTable};

/// "EPOCHS_2": a header followed by the pin table and the ticket table.
/// A table laid out before tickets existed carries a different magic
/// and is refused as a [`EpochError::LayoutMismatch`].
pub const EPOCH_MAGIC: u64 = 0x4550_4F43_4853_5F32;

/// A monotonic point in the store's history.
pub type Epoch = u64;

/// A pin slot holding nothing. The pins are a
/// [`HolderTable`], so this is that table's
/// [`HOLDER_FREE`](crate::holder_table::HOLDER_FREE) under the name
/// 0.2.4 published it as.
pub const PIN_FREE: u64 = crate::holder_table::HOLDER_FREE;

/// A pin slot claimed whose epoch is not decided yet, the same value as
/// [`HOLDER_RESERVED`](crate::holder_table::HOLDER_RESERVED).
pub const PIN_RESERVED: u64 = crate::holder_table::HOLDER_RESERVED;

/// Scans a reclaimer spins on a reservation before probing whether the
/// process holding it is still there.
const RESERVATION_SPINS: u32 = 64;

#[repr(C, align(64))]
pub struct EpochHeader {
    pub magic: u64,
    pub capacity: u64,
    /// The highest epoch handed out, to a ticket or an advance.
    pub now: AtomicU64,
    _pad: [u8; 40],
}

const _: () = {
    assert!(size_of::<EpochHeader>() == 64);
};

/// Bytes a table of `capacity` pins and `capacity` tickets needs.
pub const fn epoch_file_size(capacity: usize) -> usize {
    size_of::<EpochHeader>() + 2 * holder_table_size(capacity)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochError {
    /// Every pin slot is held by a live process.
    PinsExhausted,
    /// Every ticket slot is held by a live process.
    TicketsExhausted,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for EpochError {
    fn from(e: std::io::Error) -> Self {
        EpochError::IoError(e.kind())
    }
}

impl std::fmt::Display for EpochError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochError::PinsExhausted => write!(f, "every pin slot is held"),
            EpochError::TicketsExhausted => write!(f, "every ticket slot is held"),
            EpochError::LayoutMismatch => write!(f, "epoch table layout mismatch"),
            EpochError::IoError(k) => write!(f, "epoch table io error: {k:?}"),
        }
    }
}

impl std::error::Error for EpochError {}

/// The current epoch and every pin held against it, shared by every
/// process mapped onto the same table.
pub struct SharedEpochs {
    _file: Option<File>,
    mmap: MmapMut,
    capacity: usize,
    pins: HolderTable,
    /// Open compound writes, each holding `epoch + 1` as its payload.
    tickets: HolderTable,
}

unsafe impl Send for SharedEpochs {}
unsafe impl Sync for SharedEpochs {}

impl SharedEpochs {
    /// Obtain the table at `path`, initializing one if the path does not
    /// yet exist and attaching to it if it does. Attaching leaves the
    /// counter and every live pin in place.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, EpochError> {
        assert!(capacity >= 1);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            epoch_file_size(capacity),
            |ptr| unsafe { Self::init_region(ptr, capacity) },
            |ptr| unsafe { (*(ptr as *const EpochHeader)).magic == EPOCH_MAGIC },
        )?;
        let this = Self::attach(Some(file), mmap, capacity);
        this.validate(capacity)?;
        Ok(this)
    }

    /// Attach to an existing table.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, EpochError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = epoch_file_size(expected_capacity);
        if file.metadata()?.len() < total as u64 {
            return Err(EpochError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let this = Self::attach(Some(file), mmap, expected_capacity);
        this.validate(expected_capacity)?;
        Ok(this)
    }

    /// A table private to this process, for a store that is not shared.
    pub fn create_anon(capacity: usize) -> Result<Self, EpochError> {
        assert!(capacity >= 1);
        let mut mmap = MmapOptions::new().len(epoch_file_size(capacity)).map_anon()?;
        unsafe { Self::init_region(mmap.as_mut_ptr(), capacity) };
        Ok(Self::attach(None, mmap, capacity))
    }

    /// Build the views over the two tables behind the header: pins first,
    /// tickets after them.
    fn attach(_file: Option<File>, mmap: MmapMut, capacity: usize) -> Self {
        let pins_base = unsafe { mmap.as_ptr().add(size_of::<EpochHeader>()) };
        let pins = unsafe { HolderTable::from_ptr(pins_base, capacity) };
        let tickets = unsafe {
            HolderTable::from_ptr(pins_base.add(holder_table_size(capacity)), capacity)
        };
        Self { _file, mmap, capacity, pins, tickets }
    }

    /// Lay out an empty table: sizes first, magic last, because
    /// attachers spin on it. The zeroed region is already the free slot
    /// array, every slot unclaimed.
    ///
    /// # Safety
    /// `ptr` addresses at least `epoch_file_size(capacity)` writable
    /// zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize) {
        let hdr = ptr as *mut EpochHeader;
        unsafe {
            (*hdr).capacity = capacity as u64;
            std::ptr::write_volatile(&raw mut (*hdr).magic, EPOCH_MAGIC);
        }
    }

    fn validate(&self, expected_capacity: usize) -> Result<(), EpochError> {
        let h = self.header();
        if h.magic != EPOCH_MAGIC || h.capacity != expected_capacity as u64 {
            return Err(EpochError::LayoutMismatch);
        }
        Ok(())
    }

    #[inline]
    fn header(&self) -> &EpochHeader {
        unsafe { &*(self.mmap.as_ptr() as *const EpochHeader) }
    }

    /// The pin slots, for a caller that wants the table directly.
    #[inline]
    pub fn pins(&self) -> &HolderTable {
        &self.pins
    }

    /// The ticket slots, for a caller that wants the table directly.
    #[inline]
    pub fn tickets(&self) -> &HolderTable {
        &self.tickets
    }

    /// Pins that may be held at once, and tickets likewise.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The highest epoch handed out so far, published or not.
    #[inline]
    fn counter(&self) -> Epoch {
        self.header().now.load(Ordering::Acquire)
    }

    /// The published epoch: what a pin takes, and the newest epoch a
    /// reader is shown. With no ticket open it is the counter; with
    /// tickets open it is one below the oldest of them, so nothing a
    /// compound write has stamped is visible before that write publishes.
    ///
    /// A ticket mid-reservation restarts the read, as
    /// [`reclaim_horizon`](Self::reclaim_horizon) restarts on a pin: its
    /// epoch is being decided right now and reading past it would report
    /// an epoch that ticket is about to hold.
    pub fn now(&self) -> Epoch {
        let mut restarts = 0u32;
        loop {
            let counter = self.counter();
            if let Some(p) = self.tickets.try_fold(counter, |acc, payload| acc.min(payload - 2)) {
                return p;
            }
            restarts += 1;
            if restarts.is_multiple_of(RESERVATION_SPINS) {
                self.tickets.reap_dead();
            }
            std::hint::spin_loop();
        }
    }

    /// Advance to the next epoch and return it, for a single-entry write
    /// that stamps as it goes.
    ///
    /// Called by a writer that supersedes a version, so the old one
    /// carries the epoch at which it stopped being current. The epoch is
    /// published the instant it is stamped, unless an older ticket is
    /// still open, in which case [`now`](Self::now) holds below both
    /// until that ticket publishes.
    #[inline]
    pub fn advance(&self) -> Epoch {
        self.header().now.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Reserve the next epoch for a compound write. Every entry the write
    /// stamps carries [`EpochTicket::epoch`]; nothing stamped with it is
    /// visible to a pin until [`publish`](EpochTicket::publish).
    ///
    /// Returns [`EpochError::TicketsExhausted`] when every ticket slot is
    /// held by a live process; a slot left by a dead process is reported
    /// by [`dead_tickets`](Self::dead_tickets) rather than reused here,
    /// since its epoch may be half-written.
    pub fn begin(&self) -> Result<EpochTicket<'_>, EpochError> {
        // Reserve first, then take the epoch: the reservation is what
        // holds `now` and the horizon below the epoch about to be taken.
        let slot = self.tickets.reserve().ok_or(EpochError::TicketsExhausted)?;
        let epoch = self.advance();
        // Biased by one so epoch 0 is a payload and not the free state;
        // `now` and the horizon read it back as `payload - 2`, one below
        // the ticket's own epoch.
        self.tickets.publish(slot, epoch + 1);
        Ok(EpochTicket { epochs: self, epoch, slot })
    }

    /// Epochs whose ticket is held by a process that is gone: compound
    /// writes that will never publish. Each structure sharing this table
    /// voids what it holds at those epochs, then
    /// [`free_dead_ticket`](Self::free_dead_ticket) releases the slot.
    pub fn dead_tickets(&self) -> Vec<Epoch> {
        let mut out = Vec::new();
        for i in 0..self.capacity {
            let slot = self.tickets.slot(i);
            let state = slot.state.load(Ordering::Acquire);
            if state == crate::holder_table::HOLDER_FREE
                || state == crate::holder_table::HOLDER_RESERVED
            {
                continue;
            }
            let pid = slot.owner_pid.load(Ordering::Acquire);
            if pid != 0 && !crate::peer_directory::process_alive(pid) {
                out.push(state - 1);
            }
        }
        out
    }

    /// Release the ticket for `epoch`, once every structure has voided
    /// what it holds there. Releases only a ticket whose process is gone;
    /// a live writer's ticket is left alone and `false` is returned, as it
    /// is for an epoch no ticket holds.
    pub fn free_dead_ticket(&self, epoch: Epoch) -> bool {
        for i in 0..self.capacity {
            let slot = self.tickets.slot(i);
            if slot.state.load(Ordering::Acquire) != epoch + 1 {
                continue;
            }
            let pid = slot.owner_pid.load(Ordering::Acquire);
            if pid == 0 || crate::peer_directory::process_alive(pid) {
                return false;
            }
            return slot
                .state
                .compare_exchange(
                    epoch + 1,
                    crate::holder_table::HOLDER_FREE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
        }
        false
    }

    /// Tickets open right now.
    pub fn open_tickets(&self) -> usize {
        self.tickets.live()
    }

    /// Pin the current epoch. The returned guard holds it until dropped;
    /// nothing superseded at or after it is reclaimable while it lives.
    ///
    /// Returns [`EpochError::PinsExhausted`] when every slot is held by a
    /// live process. A slot left claimed by a process that died is
    /// reclaimed here rather than counting against the table.
    pub fn pin(&self) -> Result<PinGuard<'_>, EpochError> {
        if let Some(g) = self.try_claim() {
            return Ok(g);
        }
        self.reap_dead_pins();
        self.try_claim().ok_or(EpochError::PinsExhausted)
    }

    /// Reserve a slot, then decide the epoch.
    ///
    /// The order is the safety property: the epoch is read once the
    /// slot is visible, so it lands at or above any horizon a reclaimer
    /// could be computing, and
    /// [`reclaim_horizon`](Self::reclaim_horizon) waits out a
    /// reservation rather than reading past one.
    fn try_claim(&self) -> Option<PinGuard<'_>> {
        let slot = self.pins.reserve()?;
        let at = self.now();
        // Biased by one so epoch 0 is a payload and not the free state.
        self.pins.publish(slot, at + 1);
        Some(PinGuard { epochs: self, at, slot })
    }

    /// Epochs at or below this have no reader, so anything superseded in
    /// them can be reclaimed.
    ///
    /// With no pins outstanding this is the current epoch: nothing is
    /// being read, so everything superseded is reclaimable.
    /// A slot mid-reservation restarts the scan: its epoch is being read
    /// right now, and reading past it would return a horizon above the
    /// pin it is about to publish. The window is one load and one store
    /// wide.
    pub fn reclaim_horizon(&self) -> Epoch {
        let mut restarts = 0u32;
        loop {
            // Sample the published epoch BEFORE the pins. A pin that
            // reserves after this load reads its epoch after reserving,
            // so it lands at or above the value returned; and an open
            // ticket holds the published epoch below its own, so nothing
            // a compound is about to supersede is reclaimable.
            let now = self.now();
            if let Some(h) = self.pins.try_fold(now, |acc, payload| acc.min(payload - 1)) {
                return h;
            }
            restarts += 1;
            // A reservation that outlives its window belongs to a
            // process that died inside it, and spinning on it would
            // never end.
            if restarts.is_multiple_of(RESERVATION_SPINS) {
                self.reap_dead_pins();
            }
            std::hint::spin_loop();
        }
    }

    /// Pins outstanding.
    pub fn live_pins(&self) -> usize {
        self.pins.live()
    }

    /// Free every slot whose owning process is gone, and report how many
    /// were freed.
    ///
    /// A pin outlives its process only when that process died holding
    /// it, so the slot names a scan that will never finish and the
    /// horizon it holds down is stale.
    pub fn reap_dead_pins(&self) -> usize {
        self.pins.reap_dead()
    }

    fn release(&self, slot: usize) {
        self.pins.release(slot);
    }
}

/// Holds an epoch against reclamation for as long as it lives.
pub struct PinGuard<'a> {
    epochs: &'a SharedEpochs,
    at: Epoch,
    slot: usize,
}

impl PinGuard<'_> {
    /// The pinned epoch. A reader shows this to each record to pick the
    /// version it should see.
    #[inline]
    pub fn epoch(&self) -> Epoch {
        self.at
    }

    /// Whether a version superseded at `superseded_at` is visible to this
    /// reader.
    ///
    /// A version is visible when it was still current at the pinned
    /// epoch: it was superseded after the pin, or never superseded.
    #[inline]
    pub fn sees(&self, superseded_at: Option<Epoch>) -> bool {
        match superseded_at {
            None => true,
            Some(e) => e > self.at,
        }
    }
}

impl std::fmt::Debug for SharedEpochs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedEpochs")
            .field("now", &self.now())
            .field("capacity", &self.capacity)
            .field("live_pins", &self.live_pins())
            .finish()
    }
}

impl std::fmt::Debug for PinGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinGuard").field("at", &self.at).field("slot", &self.slot).finish()
    }
}

/// A reserved epoch for one compound write: everything the write stamps
/// carries [`epoch`](Self::epoch), and none of it is visible to a pin
/// until [`publish`](Self::publish). Dropping the ticket publishes it.
pub struct EpochTicket<'a> {
    epochs: &'a SharedEpochs,
    epoch: Epoch,
    slot: usize,
}

impl EpochTicket<'_> {
    /// The epoch every entry of this write is stamped with.
    #[inline]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Make the write visible: every entry stamped with this epoch is
    /// seen by every pin taken from now on, all at once.
    pub fn publish(self) {
        drop(self);
    }
}

impl std::fmt::Debug for EpochTicket<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochTicket").field("epoch", &self.epoch).field("slot", &self.slot).finish()
    }
}

impl Drop for EpochTicket<'_> {
    fn drop(&mut self) {
        self.epochs.tickets.release(self.slot);
    }
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        self.epochs.release(self.slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holder_table::HOLDER_RESERVED;

    fn table(capacity: usize) -> SharedEpochs {
        SharedEpochs::create_anon(capacity).unwrap()
    }

    #[test]
    fn the_horizon_with_no_readers_is_the_current_epoch() {
        let e = table(8);
        assert_eq!(e.reclaim_horizon(), 0);
        e.advance();
        e.advance();
        assert_eq!(e.now(), 2);
        assert_eq!(
            e.reclaim_horizon(),
            2,
            "nothing is being read, so everything superseded is reclaimable"
        );
    }

    /// The property the whole scheme rests on: a live reader holds the
    /// horizon down, so nothing it can still see is reclaimed.
    #[test]
    fn a_pin_holds_the_horizon_at_its_epoch_while_writers_advance() {
        let e = table(8);
        e.advance();
        let p = e.pin().unwrap();
        assert_eq!(p.epoch(), 1);
        for _ in 0..10 {
            e.advance();
        }
        assert_eq!(e.now(), 11, "writers are not blocked");
        assert_eq!(e.reclaim_horizon(), 1, "the horizon stays at the oldest live reader");
        drop(p);
        assert_eq!(e.reclaim_horizon(), 11, "released, so the horizon catches up");
    }

    /// The horizon follows the OLDEST reader, not the newest.
    #[test]
    fn the_oldest_pin_sets_the_horizon() {
        let e = table(8);
        e.advance();
        let old = e.pin().unwrap();
        e.advance();
        e.advance();
        let recent = e.pin().unwrap();
        assert_eq!(recent.epoch(), 3);
        assert_eq!(e.reclaim_horizon(), 1, "the long scan holds it back");
        drop(recent);
        assert_eq!(e.reclaim_horizon(), 1, "still held by the older one");
        drop(old);
        assert_eq!(e.reclaim_horizon(), 3);
    }

    #[test]
    fn two_readers_at_the_same_epoch_both_have_to_release() {
        let e = table(8);
        e.advance();
        let a = e.pin().unwrap();
        let b = e.pin().unwrap();
        assert_eq!(e.live_pins(), 2);
        assert_eq!(e.reclaim_horizon(), 1);
        drop(a);
        assert_eq!(e.reclaim_horizon(), 1, "one reader remains at that epoch");
        drop(b);
        assert_eq!(e.live_pins(), 0);
    }

    /// Visibility: a reader sees a version that was still current when it
    /// pinned, and not one superseded before.
    #[test]
    fn a_reader_sees_the_version_current_at_its_pin() {
        let e = table(8);
        e.advance();
        e.advance();
        let p = e.pin().unwrap();
        assert_eq!(p.epoch(), 2);
        assert!(p.sees(None), "a live version is always visible");
        assert!(
            p.sees(Some(3)),
            "superseded after the pin, so it was current when the scan started"
        );
        assert!(
            !p.sees(Some(2)),
            "superseded at the pin, so a newer version was already current"
        );
        assert!(!p.sees(Some(1)), "superseded before the pin");
    }

    /// A scan must not be affected by writes that land after it pins,
    /// which is what makes the view fixed.
    #[test]
    fn writes_after_a_pin_are_invisible_to_it() {
        let e = table(8);
        let p = e.pin().unwrap();
        let after = e.advance();
        assert!(after > p.epoch());
        assert!(p.sees(Some(after)), "superseded at {after}, pinned at {}", p.epoch());
    }

    #[test]
    fn concurrent_pins_and_advances_leave_no_stuck_horizon() {
        let e = std::sync::Arc::new(table(64));
        std::thread::scope(|s| {
            for _ in 0..4 {
                let e = std::sync::Arc::clone(&e);
                s.spawn(move || {
                    for _ in 0..200 {
                        if let Ok(p) = e.pin() {
                            std::hint::black_box(p.epoch());
                        }
                    }
                });
            }
            let e2 = std::sync::Arc::clone(&e);
            s.spawn(move || {
                for _ in 0..800 {
                    e2.advance();
                }
            });
        });
        assert_eq!(e.live_pins(), 0, "every pin released");
        assert_eq!(
            e.reclaim_horizon(),
            e.now(),
            "with no readers the horizon must reach the current epoch"
        );
    }

    /// The horizon must never report above a pin taken concurrently with
    /// it, or a reclaimer drops a version a live scan is about to read.
    #[test]
    fn the_horizon_never_passes_a_pin_taken_beside_it() {
        let e = std::sync::Arc::new(table(64));
        std::thread::scope(|s| {
            let w = std::sync::Arc::clone(&e);
            s.spawn(move || {
                for _ in 0..5_000 {
                    w.advance();
                }
            });
            for _ in 0..3 {
                let r = std::sync::Arc::clone(&e);
                s.spawn(move || {
                    for _ in 0..2_000 {
                        if let Ok(p) = r.pin() {
                            assert!(
                                r.reclaim_horizon() <= p.epoch(),
                                "horizon {} passed a live pin at {}",
                                r.reclaim_horizon(),
                                p.epoch()
                            );
                        }
                    }
                });
            }
        });
    }

    /// A slot is reserved before its epoch is read, and a reclaimer
    /// waits out the reservation. Reading past one returns a horizon
    /// above the pin about to publish there, and reclaims a version
    /// that pin needs.
    #[test]
    fn a_reservation_holds_the_horizon_until_its_epoch_is_published() {
        let e = std::sync::Arc::new(table(2));
        e.advance();
        let _live = e.pin().unwrap();
        for _ in 0..10 {
            e.advance();
        }
        assert_eq!(e.reclaim_horizon(), 1);

        // Stand in for a pinner between its reserve and its publish.
        e.pins().slot(1).state.store(HOLDER_RESERVED, Ordering::Release);
        e.pins().slot(1).owner_pid.store(std::process::id(), Ordering::Release);

        let reader = std::sync::Arc::clone(&e);
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&done);
        let h = std::thread::spawn(move || {
            let got = reader.reclaim_horizon();
            flag.store(true, Ordering::Release);
            got
        });

        // It must still be scanning: the reservation is unresolved.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !done.load(Ordering::Acquire),
            "the horizon read past a slot whose epoch was not yet decided"
        );

        // Publish an epoch below the live pin and it must be respected.
        e.pins().slot(1).state.store(1, Ordering::Release);
        assert_eq!(h.join().unwrap(), 0, "the published reservation sets the horizon");
    }

    /// A process that died between reserving and publishing must not
    /// spin every reclaimer forever.
    #[test]
    fn a_reservation_whose_process_died_is_reaped_by_the_reclaimer() {
        let e = table(2);
        e.advance();
        e.advance();
        e.pins().slot(0).state.store(HOLDER_RESERVED, Ordering::Release);
        e.pins().slot(0).owner_pid.store(u32::MAX - 1, Ordering::Release);
        assert_eq!(
            e.reclaim_horizon(),
            2,
            "a dead process's reservation must not hold the horizon"
        );
        assert_eq!(e.live_pins(), 0, "and its slot is returned to the table");
    }

    #[test]
    fn a_full_table_refuses_rather_than_overwriting_a_live_pin() {
        let e = table(2);
        let _a = e.pin().unwrap();
        let _b = e.pin().unwrap();
        assert_eq!(e.pin().unwrap_err(), EpochError::PinsExhausted);
        assert_eq!(e.live_pins(), 2, "the refusal left both live pins alone");
    }

    /// A slot left claimed by a process that is gone must not hold the
    /// horizon down forever.
    #[test]
    fn a_pin_whose_process_died_is_reaped() {
        let e = table(2);
        let _live = e.pin().unwrap();
        e.advance();
        // Stand in for a crashed peer: claim the second slot and stamp it
        // with a pid that cannot be running.
        let dead = e.pins().slot(1);
        dead.state.store(1, Ordering::Release);
        dead.owner_pid.store(u32::MAX - 1, Ordering::Release);
        assert_eq!(e.live_pins(), 2);
        assert_eq!(e.reap_dead_pins(), 1, "the dead owner's slot is freed");
        assert_eq!(e.live_pins(), 1, "the live pin is untouched");
    }

    #[test]
    fn a_full_table_of_dead_pins_recovers_on_the_next_pin() {
        let e = table(1);
        let slot = e.pins().slot(0);
        slot.state.store(1, Ordering::Release);
        slot.owner_pid.store(u32::MAX - 1, Ordering::Release);
        let p = e.pin().expect("pin reaps the dead owner rather than reporting full");
        assert_eq!(p.epoch(), e.now());
    }

    #[test]
    fn two_handles_on_one_file_share_the_counter_and_the_pins() {
        let dir = std::env::temp_dir().join(format!("subetha_epoch_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("epochs.bin");
        let a = SharedEpochs::create(&path, 8).unwrap();
        let b = SharedEpochs::open(&path, 8).unwrap();

        a.advance();
        assert_eq!(b.now(), 1, "the counter is shared");

        let p = a.pin().unwrap();
        assert_eq!(b.live_pins(), 1, "a pin taken through one handle is visible to the other");
        assert_eq!(
            b.reclaim_horizon(),
            1,
            "the other handle must not reclaim past a pin it did not take"
        );
        a.advance();
        assert_eq!(b.reclaim_horizon(), 1);
        drop(p);
        assert_eq!(b.reclaim_horizon(), 2);

        drop(a);
        drop(b);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    /// The property tickets exist for: a pin taken while a compound write
    /// is open lands below its epoch and sees none of it; a pin taken
    /// after it publishes sees all of it.
    fn a_pin_during_an_open_ticket_sees_all_old_and_after_publish_all_new() {
        let e = table(8);
        e.advance();
        assert_eq!(e.now(), 1);
        let ticket = e.begin().unwrap();
        assert_eq!(ticket.epoch(), 2);
        assert_eq!(e.now(), 1, "the published epoch stays below the open ticket");
        let mid = e.pin().unwrap();
        assert_eq!(mid.epoch(), 1);
        assert!(
            mid.sees(Some(2)),
            "the version the compound supersedes at 2 was current at 1, so the pin sees it"
        );
        // A single-entry write after the ticket opens is also held back:
        // published stays below the OLDEST open ticket.
        let single = e.advance();
        assert_eq!(single, 3);
        assert_eq!(e.now(), 1);
        ticket.publish();
        assert_eq!(e.now(), 3, "publishing releases everything up to the counter");
        let after = e.pin().unwrap();
        assert_eq!(after.epoch(), 3);
    }

    /// The horizon cannot pass an open ticket either, or a reclaimer
    /// frees what the compound is about to supersede.
    #[test]
    fn the_horizon_holds_below_an_open_ticket() {
        let e = table(8);
        for _ in 0..5 {
            e.advance();
        }
        let t = e.begin().unwrap();
        assert_eq!(t.epoch(), 6);
        for _ in 0..4 {
            e.advance();
        }
        assert_eq!(e.reclaim_horizon(), 5, "one below the open ticket, with no pins");
        drop(t);
        assert_eq!(e.reclaim_horizon(), 10);
    }

    #[test]
    fn a_full_ticket_table_refuses() {
        let e = table(2);
        let _a = e.begin().unwrap();
        let _b = e.begin().unwrap();
        assert_eq!(e.begin().err(), Some(EpochError::TicketsExhausted));
        assert_eq!(e.open_tickets(), 2);
    }

    /// A ticket whose process died is reported, not silently freed: its
    /// epoch is held back until the structures void it and the caller
    /// frees it.
    #[test]
    fn a_dead_ticket_is_reported_and_freed_only_on_request() {
        let e = table(2);
        e.advance();
        // Stand in for a writer that died mid-compound at epoch 2.
        let dead = e.tickets.slot(0);
        dead.state.store(2 + 1, Ordering::Release);
        dead.owner_pid.store(u32::MAX - 1, Ordering::Release);
        e.header().now.store(2, Ordering::Release);
        assert_eq!(e.now(), 1, "the dead ticket still holds the published epoch down");
        assert_eq!(e.dead_tickets(), vec![2]);
        assert!(!e.free_dead_ticket(7), "no ticket holds epoch 7");
        assert!(e.free_dead_ticket(2));
        assert_eq!(e.now(), 2);
        assert!(e.dead_tickets().is_empty());
        // A live writer's ticket is never freed by request.
        let live = e.begin().unwrap();
        assert!(!e.free_dead_ticket(live.epoch()));
        assert_eq!(e.open_tickets(), 1);
    }

    #[test]
    fn a_mismatched_capacity_is_refused() {
        let dir = std::env::temp_dir().join(format!("subetha_epoch_cap_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("epochs.bin");
        let a = SharedEpochs::create(&path, 8).unwrap();
        assert_eq!(SharedEpochs::open(&path, 16).unwrap_err(), EpochError::LayoutMismatch);
        drop(a);
        std::fs::remove_dir_all(&dir).ok();
    }
}
