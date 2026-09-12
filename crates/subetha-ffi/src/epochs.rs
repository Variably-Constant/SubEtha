//! The epoch table through the C ABI: a counter in a file and the pins
//! held against it, so a scan reads one fixed view of a store while
//! writers keep running.
//!
//! A writer that supersedes a record leaves the old version in place and
//! stamps it with the epoch it stopped being current in. A scan takes a
//! pin and, for each record, reads the version that was current at the
//! pinned epoch. Nothing superseded at or after a live pin may be
//! reclaimed, which is what `subetha_epochs_reclaim_horizon` reports.
//! Writers never wait for a scan; the cost is retention, since a long
//! scan holds a low pin and every version superseded since it started
//! stays.
//!
//! A pin and a ticket are handles of their own. Destroying the pin
//! handle releases it, and destroying the ticket handle publishes it, so
//! a caller that leaks either holds the horizon down until its process
//! ends. Both keep the table alive, so the order in which a caller
//! destroys them does not matter.
//!
//! A write that touches several records, or several structures sharing
//! one table, stamps them all with a single epoch taken from a ticket.
//! While the ticket is open its epoch is reserved and not published, so a
//! scan that starts mid-write pins below it and sees all-old, and one
//! that starts after the publish sees all-new. A ticket whose process
//! died mid-write is not simply freed: `subetha_epochs_dead_tickets`
//! names the epochs so each structure can undo what it stamped there, and
//! `subetha_epochs_free_dead_ticket` releases the slot once they have.
//!
//! The table runs no background work, so strict and managed modes are
//! the same.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_epochs::{epoch_file_size, EpochError, SharedEpochs};
use subetha_cxc::versioned_btree_map::DIED_LIVE;

use crate::error::{
    epochs_code, fail, SUBETHA_E_EPOCHS_PINS_EXHAUSTED, SUBETHA_E_EPOCHS_TICKETS_EXHAUSTED,
    SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_EPOCHS};
use crate::holds::{slot_of, HoldTable, ReleaseError};
use crate::ring::{finish_unlink, subetha_unlink_report, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The epoch a version carries while it is still current: no pin can be
/// at or above it, so `subetha_pin_sees` answers true for it.
pub const SUBETHA_EPOCH_LIVE: u64 = u64::MAX;

const _: () = assert!(SUBETHA_EPOCH_LIVE == DIED_LIVE);

/// A snapshot of an epoch table.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_epochs_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Pins the table holds, which is also the ticket count.
    pub capacity: u64,
    /// The published epoch: what a pin takes right now.
    pub now: u64,
    /// Epochs at or below this have no reader.
    pub reclaim_horizon: u64,
    /// Pins outstanding across every process.
    pub live_pins: u64,
    /// Tickets open across every process.
    pub open_tickets: u64,
    /// Bytes the table takes on disk.
    pub file_size: u64,
}

pub(crate) struct EpochsObject {
    epochs: Arc<SharedEpochs>,
    mode: u32,
    /// The pins taken through this handle, and the tickets. Both are
    /// sized to the table's own capacity, which is what it allows of
    /// each, so neither refuses where the table would not have.
    pins: HoldTable,
    tickets: HoldTable,
    /// What each pin and ticket slot is holding: the epoch it stands at,
    /// and the slot it took in the shared table beneath.
    ///
    /// A lock hold and a semaphore permit need nothing but the token,
    /// because releasing them is the same act whichever one it is. A pin
    /// carries an epoch the caller reads and a slot the release has to
    /// give back to `SharedEpochs`, so those travel here rather than in
    /// the token, which has room for a slot and a generation and no more.
    pin_state: Box<[HeldEpoch]>,
    ticket_state: Box<[HeldEpoch]>,
}

/// One pin's or ticket's epoch and its slot in the shared table.
/// Written before the token naming it is handed out and read only
/// through a token the hold table has accepted, so a reader has already
/// been told the slot is live at this generation.
#[derive(Default)]
pub(crate) struct HeldEpoch {
    epoch: AtomicU64,
    slot: AtomicUsize,
}

impl Drop for EpochsObject {
    /// Give back every pin and publish every ticket still outstanding.
    /// The shared table outlives this handle - other handles hold it, and
    /// its file does - so a pin left held would hold the reclaim horizon
    /// down for every process and a ticket left open would hold the
    /// published epoch below itself, with nothing able to clear either.
    fn drop(&mut self) {
        for (index, _) in self.pins.drain() {
            self.epochs.release_pin(self.pin_state[index].slot.load(Ordering::Acquire));
        }
        for (index, _) in self.tickets.drain() {
            self.epochs.publish_ticket(self.ticket_state[index].slot.load(Ordering::Acquire));
        }
    }
}

impl EpochsObject {
    /// A table parks nothing inside a call, so a destroy has nobody to
    /// wake.
    pub(crate) fn interrupt(&self) {}

    fn stats(&self) -> subetha_epochs_stats {
        subetha_epochs_stats {
            mode: self.mode,
            capacity: self.epochs.capacity() as u64,
            now: self.epochs.now(),
            reclaim_horizon: self.epochs.reclaim_horizon(),
            live_pins: self.epochs.live_pins() as u64,
            open_tickets: self.epochs.open_tickets() as u64,
            file_size: epoch_file_size(self.epochs.capacity()) as u64,
        }
    }
}

fn with_epochs(handle: subetha_handle, f: impl FnOnce(&EpochsObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_EPOCHS, |object| match object {
        Object::Epochs(e) => f(e),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an epoch table"),
    })
}

/// The refusal a token that names no live hold earns, worded for what it
/// was a hold of.
fn refuse(what: &str, e: ReleaseError) -> i32 {
    match e {
        ReleaseError::NotHeld => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("the token names no {what} on this table: it ended already, or never began"),
        ),
        ReleaseError::Stale => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("the token is from a {what} that ended, and its slot has been taken since"),
        ),
        ReleaseError::NoSuchSlot => {
            fail(SUBETHA_E_INVALID_ARGUMENT, format!("the token names no {what} slot on this table"))
        }
        ReleaseError::NotAToken => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("that is a handle, not a {what} token"),
        ),
    }
}

/// The one shape every epoch reader shares: a `uint64_t` out-parameter.
///
/// # Safety
/// `out` is null or points to a writable `uint64_t`.
unsafe fn write_u64(out: *mut u64, value: u64) -> i32 {
    if out.is_null() {
        return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
    }
    // SAFETY: checked non-null; the caller guarantees it is writable.
    unsafe { *out = value };
    SUBETHA_OK
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, capacity as usize, mode))
}

fn place(epochs: Result<SharedEpochs, EpochError>, mode: u32, out: *mut subetha_handle) -> i32 {
    match epochs {
        Ok(epochs) => {
            let capacity = epochs.capacity();
            let object = EpochsObject {
                epochs: Arc::new(epochs),
                mode,
                pins: HoldTable::bounded(capacity),
                tickets: HoldTable::bounded(capacity),
                pin_state: (0..capacity.max(1)).map(|_| HeldEpoch::default()).collect(),
                ticket_state: (0..capacity.max(1)).map(|_| HeldEpoch::default()).collect(),
            };
            unsafe { issue(Object::Epochs(object), out) }
        }
        Err(e) => epochs_code(e),
    }
}

/// Obtain the epoch table at `path` holding `capacity` pins and as many
/// tickets: an empty one is initialized when the file does not exist, an
/// existing one is attached with its counter and its live pins in place.
/// `capacity` is how many scans may hold a pin at once. A file built with
/// another capacity is a `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_create(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedEpochs::create(path, capacity), mode, out)
    })
}

/// Attach to the epoch table another process created at `path`; the file
/// must exist. `SUBETHA_E_RING_IO` names an absent file.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_open(path: *const c_char, capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        let (path, capacity, mode) = match unsafe { read_arguments(path, capacity, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(SharedEpochs::open(path, capacity), mode, out)
    })
}

/// The published epoch, which is what a pin taken now would hold. With
/// no ticket open this is the counter; with tickets open it is one below
/// the oldest of them, so nothing a compound write has stamped is
/// visible before that write publishes.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_now(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| unsafe { write_u64(out, e.epochs.now()) })
}

/// Take the next epoch, for a single-record write that stamps as it goes.
/// A write touching several records wants one epoch across all of them
/// and takes a ticket instead.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_advance(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| unsafe { write_u64(out, e.epochs.advance()) })
}

/// Epochs at or below this have no reader, so anything superseded in one
/// of them may be reclaimed. With no pin outstanding it is the published
/// epoch: nothing is being read, so everything superseded is
/// reclaimable.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_reclaim_horizon(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| unsafe { write_u64(out, e.epochs.reclaim_horizon()) })
}

/// Pins outstanding across every process attached to the table.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_live_pins(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| unsafe { write_u64(out, e.epochs.live_pins() as u64) })
}

/// Tickets open across every process attached to the table.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_open_tickets(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| unsafe { write_u64(out, e.epochs.open_tickets() as u64) })
}

/// Free every pin slot whose owning process is gone and report how many
/// went. A pin outlives its process only when that process died holding
/// it, so the slot names a scan that will never finish and the horizon it
/// holds down is stale. `subetha_epochs_pin` does this itself before
/// reporting the table full; this is for a caller reclaiming on its own
/// schedule.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_reap_dead_pins(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| unsafe { write_u64(out, e.epochs.reap_dead_pins() as u64) })
}

/// Pin the published epoch and write a handle naming the pin into `out`.
/// Nothing superseded at or after that epoch is reclaimable until the pin
/// handle is destroyed. `SUBETHA_E_EPOCHS_PINS_EXHAUSTED` when every slot
/// is held by a live process; a slot left by a process that died is
/// reclaimed here rather than counting against the table.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_pin(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match e.epochs.claim_pin() {
            Ok((slot, epoch)) => match e.pins.claim(0) {
                Some(token) => {
                    let held = &e.pin_state[slot_of(token)];
                    held.epoch.store(epoch, Ordering::Release);
                    held.slot.store(slot, Ordering::Release);
                    // SAFETY: the caller checked `out` is non-null and writable.
                    unsafe { *out = token };
                    SUBETHA_OK
                }
                None => {
                    // The pin is let go rather than left held by a token
                    // nobody has, so a refusal leaves the horizon where
                    // it was.
                    e.epochs.release_pin(slot);
                    SUBETHA_E_EPOCHS_PINS_EXHAUSTED
                }
            },
            Err(err) => epochs_code(err),
        }
    })
}

/// The epoch the pin `token` names. A reader shows it to each record to
/// pick the version it should see.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pin_epoch(handle: subetha_handle, token: u64, out: *mut u64) -> i32 {
    with_epochs(handle, |e| match e.pins.kind(token) {
        Ok(_) => {
            let epoch = e.pin_state[slot_of(token)].epoch.load(Ordering::Acquire);
            unsafe { write_u64(out, epoch) }
        }
        Err(e) => refuse("pin", e),
    })
}

/// Whether a version superseded at `superseded_at` is visible to this
/// pin: true when it was superseded after the pin was taken, and for
/// `SUBETHA_EPOCH_LIVE`, which is the version still current.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pin_sees(handle: subetha_handle, token: u64, superseded_at: u64, out: *mut bool) -> i32 {
    with_epochs(handle, |e| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match e.pins.kind(token) {
            Ok(_) => {
                let epoch = e.pin_state[slot_of(token)].epoch.load(Ordering::Acquire);
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = superseded_at > epoch };
                SUBETHA_OK
            }
            Err(e) => refuse("pin", e),
        }
    })
}

/// Let the pin `token` names go. The reclaim horizon is free to pass its
/// epoch as soon as this returns, and the token names nothing afterwards.
///
/// A token released twice is refused rather than freeing a pin slot
/// another scan has since taken.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_pin_release(handle: subetha_handle, token: u64) -> i32 {
    with_epochs(handle, |e| {
        // Read the slot before releasing the token: afterwards the entry
        // belongs to whoever claims it next.
        let index = (slot_of(token)).min(e.pin_state.len().saturating_sub(1));
        let slot = e.pin_state[index].slot.load(Ordering::Acquire);
        match e.pins.release(token) {
            Ok(_) => {
                e.epochs.release_pin(slot);
                SUBETHA_OK
            }
            Err(err) => refuse("pin", err),
        }
    })
}

/// Reserve an epoch for one compound write and write a handle naming the
/// ticket into `out`. Every record the write stamps carries
/// `subetha_ticket_epoch`, and none of it is visible to a pin until the
/// ticket is published. `SUBETHA_E_EPOCHS_TICKETS_EXHAUSTED` when every
/// ticket slot is held by a live process; a slot left by a process that
/// died is reported by `subetha_epochs_dead_tickets` rather than reused
/// here, since its epoch may be half-written.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_begin(handle: subetha_handle, out: *mut u64) -> i32 {
    with_epochs(handle, |e| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match e.epochs.claim_ticket() {
            Ok((slot, epoch)) => match e.tickets.claim(0) {
                Some(token) => {
                    let held = &e.ticket_state[slot_of(token)];
                    held.epoch.store(epoch, Ordering::Release);
                    held.slot.store(slot, Ordering::Release);
                    // SAFETY: the caller checked `out` is non-null and writable.
                    unsafe { *out = token };
                    SUBETHA_OK
                }
                None => {
                    // Publishing an epoch nobody can stamp against would
                    // let readers past a write that never happened, so
                    // the ticket goes back instead.
                    e.epochs.publish_ticket(slot);
                    SUBETHA_E_EPOCHS_TICKETS_EXHAUSTED
                }
            },
            Err(err) => epochs_code(err),
        }
    })
}

/// The epoch every record of the compound write `token` names is stamped
/// with.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ticket_epoch(handle: subetha_handle, token: u64, out: *mut u64) -> i32 {
    with_epochs(handle, |e| match e.tickets.kind(token) {
        Ok(_) => {
            let epoch = e.ticket_state[slot_of(token)].epoch.load(Ordering::Acquire);
            unsafe { write_u64(out, epoch) }
        }
        Err(e) => refuse("ticket", e),
    })
}

/// Pins and tickets this handle has handed out and not had back, which a
/// caller reads to find a leak: both return to zero once every token
/// taken has been released or published.
///
/// # Safety
/// `out_pins` and `out_tickets` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_held(handle: subetha_handle, out_pins: *mut u64, out_tickets: *mut u64) -> i32 {
    with_epochs(handle, |e| {
        if out_pins.is_null() || out_tickets.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_pins or out_tickets is null");
        }
        // SAFETY: checked non-null; the caller guarantees both are writable.
        unsafe {
            *out_pins = e.pins.live() as u64;
            *out_tickets = e.tickets.live() as u64;
        }
        SUBETHA_OK
    })
}

/// Make the write visible: every record stamped with this ticket's epoch
/// is seen by every pin taken from now on, all at once. The token names
/// nothing afterwards.
///
/// A token published twice is refused rather than freeing a ticket slot
/// another writer has since taken. A caller abandoning a compound write
/// publishes too, having removed what it stamped first.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_ticket_publish(handle: subetha_handle, token: u64) -> i32 {
    with_epochs(handle, |e| {
        // Read the slot before releasing the token: afterwards the entry
        // belongs to whoever claims it next.
        let index = (slot_of(token)).min(e.ticket_state.len().saturating_sub(1));
        let slot = e.ticket_state[index].slot.load(Ordering::Acquire);
        match e.tickets.release(token) {
            Ok(_) => {
                e.epochs.publish_ticket(slot);
                SUBETHA_OK
            }
            Err(err) => refuse("ticket", err),
        }
    })
}

/// The epochs whose ticket is held by a process that is gone: compound
/// writes that will never publish. Each structure sharing the table undoes
/// what it stamped at those epochs, and then
/// `subetha_epochs_free_dead_ticket` releases the slot. Writes at most
/// `cap` epochs into `out` and the number found into `out_count`, which
/// may exceed `cap`; a caller that sees it exceed `cap` calls again with a
/// larger buffer.
///
/// # Safety
/// `out` points to `cap` writable `uint64_t`s; `out_count` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_dead_tickets(handle: subetha_handle, out: *mut u64, cap: usize, out_count: *mut usize) -> i32 {
    with_epochs(handle, |e| {
        if out_count.is_null() || (out.is_null() && cap != 0) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out or out_count is null");
        }
        let dead = e.epochs.dead_tickets();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_count = dead.len() };
        for (i, epoch) in dead.iter().take(cap).enumerate() {
            // SAFETY: `i` is below `cap`, which the caller guarantees is
            // writable at `out`.
            unsafe { *out.add(i) = *epoch };
        }
        SUBETHA_OK
    })
}

/// Release the ticket for `epoch`, once every structure has undone what it
/// stamped there, and write whether it went into `out_freed`. Only a
/// ticket whose process is gone is released: a live writer's ticket is
/// left alone and `out_freed` is false, as it is for an epoch no ticket
/// holds.
///
/// # Safety
/// `out_freed` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_free_dead_ticket(handle: subetha_handle, epoch: u64, out_freed: *mut bool) -> i32 {
    with_epochs(handle, |e| {
        if out_freed.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_freed is null");
        }
        let freed = e.epochs.free_dead_ticket(epoch);
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out_freed = freed };
        SUBETHA_OK
    })
}

/// A snapshot of the table into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_read_stats(handle: subetha_handle, out: *mut subetha_epochs_stats) -> i32 {
    with_epochs(handle, |e| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = e.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove the epoch table's file at `path`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_epochs_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => PathBuf::from(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        found.remove(path);
        unsafe { finish_unlink(found, report) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!("subetha-ffi-epochs-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            assert_eq!(found.failed, 0, "the epoch table's file was removed: {:?}", found.first_failure);
        }
    }

    #[test]
    fn a_pin_holds_the_horizon_and_a_ticket_holds_the_published_epoch() {
        let scratch = Scratch::new("shape");
        let epochs = Arc::new(SharedEpochs::create(&scratch.0, 4).unwrap());
        let table = EpochsObject {
            epochs: Arc::clone(&epochs),
            mode: SUBETHA_MODE_STRICT,
            pins: HoldTable::bounded(4),
            tickets: HoldTable::bounded(4),
            pin_state: (0..4).map(|_| HeldEpoch::default()).collect(),
            ticket_state: (0..4).map(|_| HeldEpoch::default()).collect(),
        };
        assert_eq!(table.stats().capacity, 4);
        assert_eq!(table.stats().file_size, epoch_file_size(4) as u64);

        let first = table.epochs.advance();
        assert_eq!(table.epochs.now(), first, "nothing is open, so the counter is published");

        // A pin holds the horizon where it was taken, however far the
        // counter runs on.
        let (slot, at) = table.epochs.claim_pin().unwrap();
        assert_eq!(at, first);
        let second = table.epochs.advance();
        assert_eq!(table.epochs.now(), second);
        assert_eq!(table.epochs.reclaim_horizon(), first, "the pin holds it");
        assert_eq!(table.stats().live_pins, 1);
        assert!(at < second);
        table.epochs.release_pin(slot);
        assert_eq!(table.epochs.reclaim_horizon(), second, "the released pin lets it run on");
        assert_eq!(table.stats().live_pins, 0);

        // A ticket holds the published epoch one below its own until it
        // is published.
        let (tslot, tepoch) = table.epochs.claim_ticket().unwrap();
        assert_eq!(tepoch, second + 1);
        assert_eq!(table.epochs.now(), second, "the open ticket is not published");
        assert_eq!(table.stats().open_tickets, 1);
        table.epochs.publish_ticket(tslot);
        assert_eq!(table.epochs.now(), tepoch, "publishing let it through");
        assert_eq!(table.stats().open_tickets, 0);
        assert!(table.epochs.dead_tickets().is_empty(), "this process is alive");
        assert!(!table.epochs.free_dead_ticket(tepoch), "no ticket holds it");

        // A token is good once: the second release is refused rather
        // than freeing a pin slot another scan may have taken.
        let token = table.pins.claim(0).expect("a pin slot");
        assert!(table.pins.release(token).is_ok());
        assert!(table.pins.release(token).is_err(), "a token spends once");
    }
}
