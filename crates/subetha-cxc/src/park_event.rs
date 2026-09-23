//! The kernel park of a Windows waiter on a file- or shared-memory-backed
//! [`CrossProcessWaker`](crate::cross_process_waker::CrossProcessWaker).
//!
//! `WaitOnAddress` is never woken from another process, so past its
//! monitor budget such a waiter sleeps on a named auto-reset event, one per
//! waker instance and slot, with the event's id in the slot's `park_event`
//! word. A waker that wins the slot's `PARKED` to `WOKEN` exchange loads
//! the word and sets that event. The waiter's store and re-check and the
//! waker's exchange and load are all SeqCst, so either the waiter sees the
//! slot woken before it sleeps or the waker sees the id. Names follow
//! `cross_process_notifier`: `Local\subetha_park_<id>` beside a file, the
//! region's namespace and descriptor beside a shared-memory region, where
//! the id is the process id above a process-wide counter. A
//! sleep re-checks the slot at least every [`PARK_HEAL`], which bounds a
//! wake that sets no event.

use std::io;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{
    OpenEventW, SetEvent, WaitForSingleObject, EVENT_MODIFY_STATE, INFINITE,
};

use crate::cross_process_waker::WakerError;
use crate::shm_file::ShmNamespace;

/// The longest a parked waiter sleeps before it checks its slot again.
pub(crate) const PARK_HEAL: Duration = Duration::from_millis(20);

/// The low half of the next event id this process makes.
static NEXT_SEQ: AtomicU32 = AtomicU32::new(1);

/// An open event handle and the id its name carries, closed on drop.
struct Event {
    handle: HANDLE,
    id: u64,
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: the handle is open and owned by this value.
        unsafe { CloseHandle(self.handle) };
    }
}

/// One waker instance's park events, per slot: the event its own waiters
/// sleep on, and the first event of a peer its wakers opened.
pub(crate) struct ParkEvents {
    prefix: &'static str,
    sddl: Option<String>,
    /// Set once per slot and freed only on drop, so a reference taken from
    /// one stays valid for the instance's life.
    own: Box<[AtomicPtr<Event>]>,
    /// Set once per slot, like `own`. A wake for any other id opens, sets
    /// and closes, since replacing a handle another thread may be setting
    /// would take a lock.
    peer: Box<[AtomicPtr<Event>]>,
    /// Per slot, whether a failure to set that slot's event was reported.
    set_reported: Box<[AtomicBool]>,
    /// Whether a failure to create an own event was reported.
    create_reported: AtomicBool,
    #[cfg(test)]
    heal_off: AtomicBool,
}

impl ParkEvents {
    /// The events beside a file-backed waker: `Local` names and the
    /// creator's default descriptor.
    pub(crate) fn file(capacity: usize) -> Self {
        Self::new("Local", None, capacity)
    }

    /// The events beside a waker on a shared-memory region, in the
    /// region's namespace and with its descriptor.
    pub(crate) fn shm(namespace: ShmNamespace, sddl: Option<&str>, capacity: usize) -> Self {
        let prefix = match namespace {
            ShmNamespace::Session => "Local",
            ShmNamespace::Machine => "Global",
        };
        Self::new(prefix, sddl.map(str::to_owned), capacity)
    }

    fn new(prefix: &'static str, sddl: Option<String>, capacity: usize) -> Self {
        Self {
            prefix,
            sddl,
            own: (0..capacity).map(|_| AtomicPtr::new(null_mut())).collect(),
            peer: (0..capacity).map(|_| AtomicPtr::new(null_mut())).collect(),
            set_reported: (0..capacity).map(|_| AtomicBool::new(false)).collect(),
            create_reported: AtomicBool::new(false),
            #[cfg(test)]
            heal_off: AtomicBool::new(false),
        }
    }

    fn name(&self, id: u64) -> String {
        format!("{}\\subetha_park_{id:016x}", self.prefix)
    }

    /// Sleep on slot `idx`'s event until `state` leaves `parked` or
    /// `deadline` passes, with the event's id in `word` meanwhile.
    ///
    /// `None` when the event cannot be created, which is reported once per
    /// instance and leaves the wait to the caller. A failed sleep is an
    /// `IoError`.
    pub(crate) fn park(
        &self,
        idx: usize,
        state: &AtomicU32,
        parked: u32,
        word: &AtomicU64,
        deadline: Option<Instant>,
    ) -> Option<Result<(), WakerError>> {
        let event = match self.own_event(idx) {
            Ok(event) => event,
            Err((name, e)) => {
                if !self.create_reported.swap(true, Ordering::Relaxed) {
                    eprintln!(
                        "subetha: park event {name} not created, so cross-process waits on this waker fall back to the monitor and their timeouts: {e}"
                    );
                }
                return None;
            }
        };
        let heal = self.heal();
        word.store(event.id, Ordering::SeqCst);
        let result = loop {
            if state.load(Ordering::SeqCst) != parked {
                break Ok(());
            }
            let sleep = match deadline {
                None => heal,
                Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                    Some(rest) if !rest.is_zero() => Some(heal.map_or(rest, |h| rest.min(h))),
                    // Deadline reached; one last check catches a wake that
                    // landed at the wire.
                    _ => {
                        break if state.load(Ordering::SeqCst) != parked {
                            Ok(())
                        } else {
                            Err(WakerError::Timeout)
                        };
                    }
                },
            };
            // SAFETY: the handle is this instance's and stays open until it
            // drops, which cannot happen while `self` is borrowed here.
            match unsafe { WaitForSingleObject(event.handle, sleep.map_or(INFINITE, whole_ms)) } {
                WAIT_OBJECT_0 | WAIT_TIMEOUT => {}
                _ => break Err(WakerError::IoError(io::Error::last_os_error().kind())),
            }
        };
        word.store(0, Ordering::SeqCst);
        Some(result)
    }

    /// Set the event whose id slot `idx` held when this thread woke it.
    pub(crate) fn signal(&self, idx: usize, id: u64) {
        let cell = &self.peer[idx];
        let cached = cell.load(Ordering::Acquire);
        // SAFETY: a published event is freed only when `self` drops.
        if let Some(event) = unsafe { cached.as_ref() }
            && event.id == id
        {
            self.set(idx, event);
            return;
        }
        let name = self.name(id);
        let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        // SAFETY: wide is a NUL-terminated name.
        let handle = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
        if handle.is_null() {
            let e = io::Error::last_os_error();
            // Not found is a waiter that has gone, and nothing is owed to it.
            if e.kind() != io::ErrorKind::NotFound {
                self.report_set(idx, &name, &e);
            }
            return;
        }
        let opened = Event { handle, id };
        self.set(idx, &opened);
        if cached.is_null() {
            let fresh = Box::into_raw(Box::new(opened));
            if cell
                .compare_exchange(null_mut(), fresh, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                // SAFETY: fresh came from Box::into_raw and was never published.
                drop(unsafe { Box::from_raw(fresh) });
            }
        }
    }

    /// Slot `idx`'s own event, created on first use. The error carries the
    /// name the create was refused for.
    fn own_event(&self, idx: usize) -> Result<&Event, (String, io::Error)> {
        let cell = &self.own[idx];
        let current = cell.load(Ordering::Acquire);
        // SAFETY: a published event is freed only when `self` drops.
        if let Some(event) = unsafe { current.as_ref() } {
            return Ok(event);
        }
        let id = (u64::from(std::process::id()) << 32)
            | u64::from(NEXT_SEQ.fetch_add(1, Ordering::Relaxed));
        let name = self.name(id);
        let handle = match create(&name, self.sddl.as_deref()) {
            Ok(handle) => handle,
            Err(e) => return Err((name, e)),
        };
        let fresh = Box::into_raw(Box::new(Event { handle, id }));
        match cell.compare_exchange(null_mut(), fresh, Ordering::AcqRel, Ordering::Acquire) {
            // SAFETY: fresh is now the published event, freed only on drop.
            Ok(_) => Ok(unsafe { &*fresh }),
            Err(existing) => {
                // SAFETY: fresh came from Box::into_raw and was never
                // published; existing is published and outlives the borrow.
                drop(unsafe { Box::from_raw(fresh) });
                Ok(unsafe { &*existing })
            }
        }
    }

    fn set(&self, idx: usize, event: &Event) {
        // SAFETY: the handle is open and carries EVENT_MODIFY_STATE.
        if unsafe { SetEvent(event.handle) } == 0 {
            let e = io::Error::last_os_error();
            self.report_set(idx, &self.name(event.id), &e);
        }
    }

    fn report_set(&self, idx: usize, name: &str, e: &io::Error) {
        if !self.set_reported[idx].swap(true, Ordering::Relaxed) {
            eprintln!(
                "subetha: park event {name} not set, so its waiter wakes at its next check of the slot: {e}"
            );
        }
    }

    /// The longest one sleep lasts: [`PARK_HEAL`], or no bound in a test
    /// that has turned the heal off so a lost wake shows as a timeout.
    fn heal(&self) -> Option<Duration> {
        #[cfg(test)]
        if self.heal_off.load(Ordering::Relaxed) {
            return None;
        }
        Some(PARK_HEAL)
    }

    #[cfg(test)]
    pub(crate) fn turn_heal_off(&self) {
        self.heal_off.store(true, Ordering::Relaxed);
    }

    /// The id of slot `idx`'s own event, once a park has made it.
    #[cfg(test)]
    pub(crate) fn own_id(&self, idx: usize) -> Option<u64> {
        // SAFETY: a published event is freed only when `self` drops.
        unsafe { self.own[idx].load(Ordering::Acquire).as_ref() }.map(|event| event.id)
    }

    /// Whether slot `idx`'s own event holds a set, consuming it.
    #[cfg(test)]
    pub(crate) fn take_set(&self, idx: usize) -> bool {
        // SAFETY: a published event is freed only when `self` drops.
        let event = unsafe { self.own[idx].load(Ordering::Acquire).as_ref() }
            .expect("a park has made this slot's event");
        // SAFETY: the handle is open for the instance's life.
        unsafe { WaitForSingleObject(event.handle, 0) == WAIT_OBJECT_0 }
    }
}

impl Drop for ParkEvents {
    fn drop(&mut self) {
        for cell in self.own.iter_mut().chain(self.peer.iter_mut()) {
            let ptr = *cell.get_mut();
            if !ptr.is_null() {
                // SAFETY: every published pointer came from Box::into_raw and
                // is freed here, once.
                drop(unsafe { Box::from_raw(ptr) });
            }
        }
    }
}

/// A named auto-reset event: one set releases one wait and then clears, so
/// a set is one wake, and a late one costs a single extra check.
fn create(name: &str, sddl: Option<&str>) -> io::Result<HANDLE> {
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
    use windows_sys::Win32::System::Threading::CreateEventW;

    let mut sd = null_mut();
    if let Some(s) = sddl {
        let wide_sddl: Vec<u16> = s.encode_utf16().chain(Some(0)).collect();
        // SAFETY: wide_sddl is NUL-terminated; sd receives a descriptor
        // LocalFree releases below.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(wide_sddl.as_ptr(), SDDL_REVISION_1, &mut sd, null_mut())
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    let sa = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: sd,
        bInheritHandle: 0,
    };
    let sa_ptr: *const SECURITY_ATTRIBUTES = if sddl.is_some() { &sa } else { std::ptr::null() };
    let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    // SAFETY: the attributes and the name are valid for the call.
    let handle = unsafe { CreateEventW(sa_ptr, 0, 0, wide.as_ptr()) };
    let err = io::Error::last_os_error();
    if !sd.is_null() {
        // SAFETY: sd was allocated by the conversion above.
        unsafe { LocalFree(sd as _) };
    }
    if handle.is_null() {
        return Err(err);
    }
    Ok(handle)
}

/// `d` in whole milliseconds, rounded up so a remainder under a millisecond
/// sleeps rather than spins, and kept below `INFINITE`.
fn whole_ms(d: Duration) -> u32 {
    d.as_nanos().div_ceil(1_000_000).min(u128::from(INFINITE - 1)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARKED: u32 = 2;
    const WOKEN: u32 = 3;

    fn events(prefix: &'static str, sddl: Option<&str>) -> ParkEvents {
        ParkEvents::new(prefix, sddl.map(str::to_owned), 2)
    }

    #[test]
    fn whole_ms_rounds_up_and_stays_below_infinite() {
        assert_eq!(whole_ms(Duration::ZERO), 0);
        assert_eq!(whole_ms(Duration::from_nanos(1)), 1);
        assert_eq!(whole_ms(Duration::from_millis(20)), 20);
        assert_eq!(whole_ms(Duration::from_micros(20_001)), 21);
        assert_eq!(whole_ms(Duration::MAX), INFINITE - 1);
    }

    #[test]
    fn a_slot_no_longer_parked_returns_at_once_and_clears_its_word() {
        let park = events("Local", None);
        let state = AtomicU32::new(WOKEN);
        let word = AtomicU64::new(0);
        assert_eq!(park.park(0, &state, PARKED, &word, None), Some(Ok(())));
        assert_eq!(word.load(Ordering::SeqCst), 0);
        assert!(park.own_id(0).is_some(), "the event is made on the first park and kept");
        assert!(park.own_id(1).is_none());
    }

    /// Only the `Local` and `Global` prefixes name a namespace, so any other
    /// is refused at the create. A deadline already passed turns a create
    /// that unexpectedly succeeds into a timeout rather than a hang.
    #[test]
    fn an_event_that_cannot_be_created_leaves_the_wait_to_the_caller() {
        let park = events("NoSuchNamespace", None);
        let state = AtomicU32::new(PARKED);
        let word = AtomicU64::new(0);
        assert_eq!(park.park(0, &state, PARKED, &word, Some(Instant::now())), None);
        assert_eq!(word.load(Ordering::SeqCst), 0, "a refused park publishes no id");
        assert!(park.create_reported.load(Ordering::Relaxed));
    }

    #[test]
    fn a_wake_for_a_waiter_that_has_gone_is_not_reported() {
        let park = events("Local", None);
        // The counter starts at 1, so no event of this process has a low
        // half of zero, and no other process has this process's id.
        park.signal(0, u64::from(std::process::id()) << 32);
        assert!(!park.set_reported[0].load(Ordering::Relaxed));
        assert!(park.peer[0].load(Ordering::Acquire).is_null());
    }

    /// An empty DACL admits no account, so a second instance is refused the
    /// right to set the event while the creator's own handle keeps it.
    #[test]
    fn a_waker_refused_the_right_to_set_reports_it_for_that_slot() {
        let waiter = events("Local", Some("D:"));
        let state = AtomicU32::new(WOKEN);
        let word = AtomicU64::new(0);
        assert_eq!(waiter.park(0, &state, PARKED, &word, None), Some(Ok(())));
        let id = waiter.own_id(0).expect("the park made the event");
        let waker = events("Local", None);
        waker.signal(0, id);
        assert!(waker.set_reported[0].load(Ordering::Relaxed), "a refused open is reported");
        assert!(!waker.set_reported[1].load(Ordering::Relaxed), "and only for its own slot");
        assert!(waker.peer[0].load(Ordering::Acquire).is_null(), "a refused event is not kept");
    }

    #[test]
    fn a_set_from_a_second_instance_is_seen_by_the_next_sleep() {
        let waiter = events("Local", None);
        let state = AtomicU32::new(WOKEN);
        let word = AtomicU64::new(0);
        assert_eq!(waiter.park(0, &state, PARKED, &word, None), Some(Ok(())));
        // SAFETY: as above.
        let event = unsafe { &*waiter.own[0].load(Ordering::Acquire) };
        let waker = events("Local", None);
        waker.signal(0, event.id);
        assert!(!waker.set_reported[0].load(Ordering::Relaxed));
        assert!(!waker.peer[0].load(Ordering::Acquire).is_null(), "the first opened event is kept");
        // SAFETY: the waiter's handle is open.
        assert_eq!(unsafe { WaitForSingleObject(event.handle, 0) }, WAIT_OBJECT_0, "the set is on the event");
        assert_eq!(unsafe { WaitForSingleObject(event.handle, 0) }, WAIT_TIMEOUT, "and one wait consumed it");
        waker.signal(0, event.id);
        assert_eq!(unsafe { WaitForSingleObject(event.handle, 0) }, WAIT_OBJECT_0, "the kept handle sets it again");
    }
}
