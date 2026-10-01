//! `WakerSlot`: one task's `Waker`, registered on one thread and woken
//! from another, with no lock.
//!
//! A task that finds nothing ready registers its `Waker` here and then
//! checks again; the side that makes something ready wakes whatever is
//! registered. One word of state orders the two. A registration holds the
//! slot while it stores its `Waker`; a wake that arrives meanwhile leaves
//! a mark instead of taking the `Waker`, and the registration, finding the
//! mark as it lets go, wakes the `Waker` it just stored. Neither side waits
//! for the other, and no wake is lost between them.
//!
//! One task registers at a time. A registration that races another is a
//! caller error, which debug builds report.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

/// Nothing is registering or waking.
const WAITING: usize = 0;
/// A registration holds the slot while it stores its `Waker`.
const REGISTERING: usize = 0b01;
/// A wake is taking the `Waker`, or arrived while a registration held the
/// slot and left this mark for it.
const WAKING: usize = 0b10;

/// The `Waker` of the one task waiting on something, behind one state word.
pub(crate) struct WakerSlot {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: the cell is reached only by the side the state word admits: a
// registration while it holds REGISTERING, a wake while it holds WAKING
// with no registration under way. `Waker` is Send + Sync.
unsafe impl Send for WakerSlot {}
unsafe impl Sync for WakerSlot {}

impl WakerSlot {
    /// An empty slot.
    pub(crate) const fn new() -> Self {
        Self { state: AtomicUsize::new(WAITING), waker: UnsafeCell::new(None) }
    }

    /// Register `waker` as the one to wake, in place of any registered
    /// before it. A wake that lands while this runs is not lost: this call
    /// wakes `waker` itself before it returns.
    pub(crate) fn register(&self, waker: &Waker) {
        match self.state.compare_exchange(WAITING, REGISTERING, Ordering::Acquire, Ordering::Acquire) {
            Ok(_) => {
                // SAFETY: REGISTERING admits this call alone to the cell.
                unsafe {
                    let held = &mut *self.waker.get();
                    match held {
                        Some(current) if current.will_wake(waker) => {}
                        _ => *held = Some(waker.clone()),
                    }
                }
                #[cfg(test)]
                crate::test_races::pause_point();
                if let Err(state) =
                    self.state.compare_exchange(REGISTERING, WAITING, Ordering::AcqRel, Ordering::Acquire)
                {
                    // A wake arrived while the slot was held and left its mark
                    // rather than take the cell; the wake is this call's to make.
                    debug_assert_eq!(state, REGISTERING | WAKING);
                    // SAFETY: the wake left the cell to this call.
                    let woken = unsafe { (*self.waker.get()).take() };
                    self.state.swap(WAITING, Ordering::AcqRel);
                    if let Some(waker) = woken {
                        waker.wake();
                    }
                }
            }
            // A wake is taking the registered `Waker` right now; this one is
            // woken directly, so its task polls again and finds what woke.
            Err(WAKING) => waker.wake_by_ref(),
            Err(state) => {
                debug_assert!(
                    state == REGISTERING || state == REGISTERING | WAKING,
                    "two registrations raced on one waker slot"
                );
            }
        }
    }

    /// Take the registered `Waker` and leave the slot empty. `None` when
    /// nothing is registered, and when a registration holds the slot: that
    /// registration then wakes its own `Waker` as it lets go.
    pub(crate) fn take(&self) -> Option<Waker> {
        match self.state.fetch_or(WAKING, Ordering::AcqRel) {
            WAITING => {
                // SAFETY: WAKING with no registration under way admits this
                // call alone to the cell.
                let waker = unsafe { (*self.waker.get()).take() };
                self.state.fetch_and(!WAKING, Ordering::Release);
                waker
            }
            _ => None,
        }
    }

    /// Wake the registered `Waker`, if one is registered.
    pub(crate) fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }
}

impl Default for WakerSlot {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::Wake;

    /// A `Waker` that counts its wakes.
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        (count, waker)
    }

    /// A wake reaches the registered `Waker` once and leaves the slot
    /// empty, so a second wake has nothing to wake.
    #[test]
    fn a_wake_reaches_the_registered_waker_once() {
        let slot = WakerSlot::new();
        let (count, waker) = counting_waker();
        slot.wake();
        assert_eq!(count.0.load(Ordering::SeqCst), 0, "a wake with nothing registered woke something");
        slot.register(&waker);
        slot.wake();
        assert_eq!(count.0.load(Ordering::SeqCst), 1, "the registered waker was not woken");
        slot.wake();
        assert_eq!(count.0.load(Ordering::SeqCst), 1, "a second wake woke the waker the first one took");
    }

    /// A later registration replaces the earlier `Waker`: the wake goes to
    /// the task that registered last.
    #[test]
    fn a_later_registration_replaces_the_earlier_waker() {
        let slot = WakerSlot::new();
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        slot.register(&first_waker);
        slot.register(&second_waker);
        slot.wake();
        assert_eq!(first.0.load(Ordering::SeqCst), 0, "the replaced waker was woken");
        assert_eq!(second.0.load(Ordering::SeqCst), 1, "the waker registered last was not woken");
    }

    /// A wake that lands while a registration holds the slot, after it has
    /// stored its `Waker` and before it lets go, reaches that `Waker`.
    #[test]
    fn a_wake_during_a_registration_reaches_the_registered_waker() {
        let slot = Arc::new(WakerSlot::new());
        let (count, waker) = counting_waker();
        let registering = Arc::clone(&slot);
        let (pause, registrant) = crate::test_races::stopped(move || registering.register(&waker));
        slot.wake();
        pause.release();
        registrant.join().expect("the registrant");
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            1,
            "the wake that landed during the registration was lost"
        );
    }
}
