//! Pause points and armed refusals for race tests.
//!
//! A write path in the shared maps, typed and raw, calls [`pause_point`]
//! where a test needs to stop it: an update or a remove once it has
//! matched its key and before its write lands, a compare_exchange once its
//! hash has matched and before it takes the slot's lock. The region
//! election calls it between its look at the region and its first marker
//! create. A thread a test has stopped there waits until the test releases
//! it, so the test can run a competing operation inside that window and
//! read what it did.
//!
//! The election's marker create can also be armed, with
//! [`refuse_next_create`], to be refused the way a create racing the
//! marker's removal is refused by the file system, which a test cannot
//! make the file system do on cue.
//!
//! The next wake window a thread runs can be armed, with
//! [`hold_next_waiter`], to keep its waiter from reporting ready for a
//! while, the way a loaded host keeps that thread off the processor.
//!
//! What a background thread does on its own cadence, such as a sidecar's
//! scan, is waited for with [`within_lost`].

use std::cell::{Cell, RefCell};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

/// How long a competing operation is given to reach the point it waits
/// at, a claimed slot it waits on or a slot lock a stopped writer holds,
/// before a test reads it as waiting there.
pub(crate) const PARK: Duration = Duration::from_millis(100);

/// How long a test waits for a background thread's periodic work, such as
/// a sidecar's scan, before calling it lost: the bound the waker's tests
/// give a wake.
pub(crate) const LOST: Duration = Duration::from_secs(30);

/// Waits up to [`LOST`] for `holds`; whether it came to hold.
pub(crate) fn within_lost(mut holds: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + LOST;
    while !holds() {
        if Instant::now() >= deadline {
            return false;
        }
        thread::yield_now();
    }
    true
}

thread_local! {
    static ARMED: RefCell<Option<Arc<Pause>>> = const { RefCell::new(None) };
    static REFUSE_CREATE: Cell<bool> = const { Cell::new(false) };
    static WAITER_HOLD: Cell<Option<Duration>> = const { Cell::new(None) };
}

/// Arms the next wake window this thread runs to hold its waiter for
/// `hold` before the waiter reports ready for its first round trip.
pub(crate) fn hold_next_waiter(hold: Duration) {
    WAITER_HOLD.with(|armed| armed.set(Some(hold)));
}

/// The hold armed on this thread, if one is still waiting to be met;
/// taking it disarms it.
pub(crate) fn take_waiter_hold() -> Option<Duration> {
    WAITER_HOLD.with(|armed| armed.take())
}

/// Arms this thread's next marker create in the region election to be
/// refused with `PermissionDenied`, the answer a create that races a
/// removal of the same name gets.
pub(crate) fn refuse_next_create() {
    REFUSE_CREATE.with(|armed| armed.set(true));
}

/// The refusal armed on this thread, if one is still waiting to be met;
/// taking it disarms it.
pub(crate) fn take_create_refusal() -> Option<io::Error> {
    REFUSE_CREATE
        .with(|armed| armed.replace(false))
        .then(|| io::Error::from(io::ErrorKind::PermissionDenied))
}

/// One stop a test arms on one thread: that thread's next pause point
/// stops it until the test releases it.
#[derive(Default)]
pub(crate) struct Pause {
    reached: AtomicBool,
    released: AtomicBool,
    stopped: OnceLock<Thread>,
}

impl Pause {
    /// Waits up to [`LOST`] for the armed thread to stop at its pause
    /// point: false when it finished, or the time passed, without reaching
    /// one. A loaded host can leave a thread it has just started unrun for
    /// longer than [`PARK`], so reaching the point is waited for as a lost
    /// wake is; one that finishes without reaching it answers at once.
    fn reached<T>(&self, armed: &JoinHandle<T>) -> bool {
        let deadline = Instant::now() + LOST;
        while !self.reached.load(Ordering::SeqCst) {
            if armed.is_finished() || Instant::now() >= deadline {
                return self.reached.load(Ordering::SeqCst);
            }
            thread::yield_now();
        }
        true
    }

    /// Lets the stopped thread go on.
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(stopped) = self.stopped.get() {
            stopped.unpark();
        }
    }
}

/// Runs `op` on a thread of its own, stopped at the first pause point it
/// reaches, and hands back the stop and the thread.
pub(crate) fn stopped<T: Send + 'static>(op: impl FnOnce() -> T + Send + 'static) -> (Arc<Pause>, JoinHandle<T>) {
    let pause = Arc::new(Pause::default());
    let armed = Arc::clone(&pause);
    let handle = thread::spawn(move || {
        ARMED.with(|slot| *slot.borrow_mut() = Some(armed));
        op()
    });
    assert!(pause.reached(&handle), "the stopped operation reaches its pause point");
    (pause, handle)
}

/// Stops the calling thread here, once, if a test armed it, until the test
/// releases it; on any other thread it does nothing.
pub(crate) fn pause_point() {
    let Some(pause) = ARMED.with(|slot| slot.borrow_mut().take()) else {
        return;
    };
    pause.stopped.get_or_init(thread::current);
    pause.reached.store(true, Ordering::SeqCst);
    while !pause.released.load(Ordering::SeqCst) {
        thread::park();
    }
}

/// Waits up to [`PARK`] for `competitor` to finish: a competing operation
/// either lands inside the window a stopped writer holds open, or waits on
/// the slot lock until the writer is released.
pub(crate) fn settle<T>(competitor: &JoinHandle<T>) {
    let deadline = Instant::now() + PARK;
    while !competitor.is_finished() && Instant::now() < deadline {
        thread::yield_now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stopped operation whose thread the host runs late is still found
    /// at its pause point: the stop is waited for as a lost wake is, not for
    /// the time a competing operation is given to block.
    #[test]
    fn a_stopped_operation_that_runs_late_still_stops_at_its_pause_point() {
        let (pause, stopped_op) = stopped(|| {
            thread::sleep(PARK * 2);
            pause_point();
            7
        });
        pause.release();
        assert_eq!(stopped_op.join().expect("the stopped operation completes"), 7);
    }
}
