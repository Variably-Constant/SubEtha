//! The wait loop every blocking call in the ABI runs: try, park on the
//! object's waker, try again to catch a wake that landed before the park,
//! then sleep until a wake, the deadline, or a destroy.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use subetha_cxc::cross_process_waker::{CrossProcessWaker, WakerError};

use crate::error::{
    fail, waker_code, SUBETHA_E_DESTROYED, SUBETHA_E_RING_WAKER_FULL, SUBETHA_E_TIMEOUT,
};

/// The target every parked waiter registers and every wake reaches: any
/// progress counts, so the sequence has no meaning here beyond zero.
pub(crate) const WAKE_ANY: u64 = 0;

/// What a blocking object carries beside its primitive: the flag a destroy
/// raises so parked callers leave, and the count of waits refused for want
/// of a waiter slot.
pub(crate) struct Waiting {
    closing: AtomicBool,
    waker_full: AtomicU64,
}

impl Waiting {
    pub(crate) fn new() -> Self {
        Self {
            closing: AtomicBool::new(false),
            waker_full: AtomicU64::new(0),
        }
    }

    /// Raise the flag every later wait checks; the caller wakes the wakers.
    pub(crate) fn close(&self) {
        self.closing.store(true, Ordering::Release);
    }

    fn closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Waits refused because every waiter slot was in use.
    pub(crate) fn waker_full(&self) -> u64 {
        self.waker_full.load(Ordering::Acquire)
    }
}

/// Run `op` until it succeeds, parking on `waker` while it reports
/// `would_block`; any other code returns at once. Past the deadline the
/// result is `SUBETHA_E_TIMEOUT`, after a destroy `SUBETHA_E_DESTROYED`,
/// and with no free waiter slot `SUBETHA_E_RING_WAKER_FULL`. The waker's
/// slot is never held across `op`, so `op` may take the object's own lock.
pub(crate) fn wait_until<T>(
    waiting: &Waiting,
    waker: &CrossProcessWaker,
    deadline: Option<Instant>,
    would_block: i32,
    mut op: impl FnMut() -> Result<T, i32>,
) -> Result<T, i32> {
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(code) if code == would_block => {}
            Err(code) => return Err(code),
        }
        if waiting.closing() {
            return Err(fail(SUBETHA_E_DESTROYED, "the object is being destroyed"));
        }
        let token = match waker.try_park(WAKE_ANY) {
            Ok(t) => t,
            Err(WakerError::Full) => {
                waiting.waker_full.fetch_add(1, Ordering::AcqRel);
                return Err(fail(SUBETHA_E_RING_WAKER_FULL, "every waiter slot is parked"));
            }
            Err(e) => return Err(waker_code(e)),
        };
        // A wake that landed between the failed try and the park is caught
        // here; the slot goes back without entering the kernel.
        match op() {
            Ok(v) => {
                waker.release(token);
                return Ok(v);
            }
            Err(code) if code == would_block => {}
            Err(code) => {
                waker.release(token);
                return Err(code);
            }
        }
        if waiting.closing() {
            waker.release(token);
            return Err(fail(SUBETHA_E_DESTROYED, "the object is being destroyed"));
        }
        let remaining = match deadline {
            None => None,
            Some(d) => match d.checked_duration_since(Instant::now()) {
                Some(r) if !r.is_zero() => Some(r),
                _ => {
                    waker.release(token);
                    return Err(fail(SUBETHA_E_TIMEOUT, "the timeout elapsed"));
                }
            },
        };
        match waker.wait(token, remaining) {
            Ok(()) => {}
            Err(WakerError::Timeout) => return Err(fail(SUBETHA_E_TIMEOUT, "the timeout elapsed")),
            Err(e) => return Err(waker_code(e)),
        }
    }
}

/// Run `op` until it succeeds, backing off between tries, for a
/// primitive that has no waker to park on: the shared lock releases by
/// clearing a bit and signals nobody. Past the deadline the result is
/// `SUBETHA_E_TIMEOUT`, and after a destroy `SUBETHA_E_DESTROYED`, so a
/// caller waiting here is still released when its handle goes.
///
/// The backoff spins, then yields, then sleeps in 50-microsecond steps,
/// which is what the lock's own blocking acquire does. A caller passing
/// no deadline waits for good, and a holder that died leaves it there:
/// that is why the entry points over this take a timeout and the
/// underlying blocking forms are not exposed.
pub(crate) fn poll_until<T>(
    waiting: &Waiting,
    deadline: Option<Instant>,
    would_block: i32,
    mut op: impl FnMut() -> Result<T, i32>,
) -> Result<T, i32> {
    let mut spins = 0u32;
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(code) if code == would_block => {}
            Err(code) => return Err(code),
        }
        if waiting.closing() {
            return Err(fail(SUBETHA_E_DESTROYED, "the object is being destroyed"));
        }
        if let Some(d) = deadline
            && Instant::now() >= d
        {
            return Err(fail(SUBETHA_E_TIMEOUT, "the timeout elapsed"));
        }
        spins += 1;
        if spins < 32 {
            std::hint::spin_loop();
        } else if spins < 256 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn a_poll_ends_on_its_deadline_and_on_a_destroy() {
        let waiting = Waiting::new();
        let deadline = Some(Instant::now() + Duration::from_millis(20));
        let attempts = std::cell::Cell::new(0);
        let result: Result<(), i32> = poll_until(&waiting, deadline, 7, || {
            attempts.set(attempts.get() + 1);
            Err(7)
        });
        assert_eq!(result.unwrap_err(), SUBETHA_E_TIMEOUT);
        assert!(attempts.get() >= 2, "the op is retried before the deadline");

        // Another code passes straight through rather than being retried.
        let result: Result<(), i32> = poll_until(&waiting, None, 7, || Err(9));
        assert_eq!(result.unwrap_err(), 9);

        // A destroy releases a caller that would otherwise wait for good.
        let waiting = Arc::new(Waiting::new());
        let waiter = {
            let waiting = Arc::clone(&waiting);
            std::thread::spawn(move || poll_until::<()>(&waiting, None, 7, || Err(7)))
        };
        std::thread::sleep(Duration::from_millis(20));
        waiting.close();
        assert_eq!(waiter.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }

    #[test]
    fn a_wake_releases_the_wait_and_a_deadline_ends_it() {
        let waiting = Waiting::new();
        let waker = CrossProcessWaker::create_anon(4).expect("an anonymous waker");
        let deadline = Some(Instant::now() + Duration::from_millis(20));
        let attempts = std::cell::Cell::new(0);
        let result: Result<(), i32> = wait_until(&waiting, &waker, deadline, 7, || {
            attempts.set(attempts.get() + 1);
            Err(7)
        });
        assert_eq!(result.unwrap_err(), SUBETHA_E_TIMEOUT);
        assert!(attempts.get() >= 2, "the op is retried after the park");

        let waker = Arc::new(waker);
        let ready = Arc::new(AtomicBool::new(false));
        let waiter = {
            let waker = Arc::clone(&waker);
            let ready = Arc::clone(&ready);
            std::thread::spawn(move || {
                let waiting = Waiting::new();
                wait_until(&waiting, &waker, None, 7, || {
                    if ready.load(Ordering::Acquire) { Ok(42) } else { Err(7) }
                })
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        ready.store(true, Ordering::Release);
        waker.wake_all();
        assert_eq!(waiter.join().unwrap().unwrap(), 42);
    }

    #[test]
    fn a_close_ends_the_wait_with_its_own_code() {
        let waiting = Arc::new(Waiting::new());
        let waker = Arc::new(CrossProcessWaker::create_anon(4).expect("an anonymous waker"));
        let waiter = {
            let waiting = Arc::clone(&waiting);
            let waker = Arc::clone(&waker);
            std::thread::spawn(move || wait_until(&waiting, &waker, None, 7, || Err::<(), i32>(7)))
        };
        std::thread::sleep(Duration::from_millis(20));
        waiting.close();
        waker.wake_all();
        assert_eq!(waiter.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }
}
