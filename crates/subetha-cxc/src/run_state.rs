//! A task's run state for the crate's executors, in one atomic word.
//!
//! A wake that finds the task idle queues it. A wake that lands while a
//! worker polls the task leaves a mark, and that worker queues the task
//! again when its poll returns. A wake that finds the task queued,
//! marked or finished has nothing to add. So a task sits in a ready
//! queue at most once, and the worker that took it from the queue is
//! the only one polling it until its poll returns, which is what lets
//! the future live in an `UnsafeCell` rather than behind a lock.
//!
//! Every transition is a read-modify-write on the one word. A wake is
//! therefore ordered before the poll that answers it, and one poll's
//! writes to the future before the next poll's reads.

use std::sync::atomic::{AtomicU8, Ordering};

/// Queued, or woken while a worker polls it.
const WOKEN: u8 = 1;
/// A worker is polling the task.
const RUNNING: u8 = 2;
/// The task's future returned `Ready`.
const DONE: u8 = 4;

/// Whether a task is idle, queued, being polled, or finished.
pub(crate) struct RunState(AtomicU8);

impl RunState {
    /// An idle task; its first wake queues it.
    pub(crate) const fn new() -> Self {
        Self(AtomicU8::new(0))
    }

    /// Records a wake. True when the caller is the one to queue the task.
    pub(crate) fn wake(&self) -> bool {
        self.0.fetch_or(WOKEN, Ordering::AcqRel) == 0
    }

    /// Takes a task popped from a ready queue to running. The poll that
    /// follows answers every wake that arrived while the task was queued.
    pub(crate) fn start(&self) {
        let was = self.0.swap(RUNNING, Ordering::AcqRel);
        debug_assert_eq!(was, WOKEN, "only a queued task is started");
    }

    /// The poll returned `Pending`. True when a wake landed during it and
    /// the caller is the one to queue the task again.
    pub(crate) fn pause(&self) -> bool {
        self.0.fetch_and(!RUNNING, Ordering::AcqRel) & WOKEN != 0
    }

    /// The poll returned `Ready`; every later wake finds the task finished.
    pub(crate) fn finish(&self) {
        self.0.store(DONE, Ordering::Release);
    }
}

/// What a [`WokenMidPoll`] saw: whether one of its polls is running, and
/// how many polls started while another was running.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct PollProbe {
    polling: std::sync::atomic::AtomicBool,
    pub(crate) overlaps: std::sync::atomic::AtomicU64,
}

/// A future for executor tests. Each poll wakes its own task, then stays
/// in the poll for [`PARK`](crate::test_races::PARK) or until a second
/// poll of the task starts, so a worker that took the task again while
/// the first poll ran would be counted in the probe.
#[cfg(test)]
pub(crate) struct WokenMidPoll {
    pub(crate) probe: std::sync::Arc<PollProbe>,
    pub(crate) polls_left: u32,
}

#[cfg(test)]
impl std::future::Future for WokenMidPoll {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.probe.polling.swap(true, Ordering::AcqRel) {
            self.probe.overlaps.fetch_add(1, Ordering::AcqRel);
            return std::task::Poll::Pending;
        }
        let out = if self.polls_left == 0 {
            std::task::Poll::Ready(())
        } else {
            self.polls_left -= 1;
            cx.waker().wake_by_ref();
            let until = std::time::Instant::now() + crate::test_races::PARK;
            while std::time::Instant::now() < until
                && self.probe.overlaps.load(Ordering::Acquire) == 0
            {
                std::thread::yield_now();
            }
            std::task::Poll::Pending
        };
        self.probe.polling.store(false, Ordering::Release);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wake_during_a_poll_queues_the_task_once_the_poll_returns() {
        let s = RunState::new();
        assert!(s.wake(), "the first wake queues an idle task");
        assert!(!s.wake(), "a queued task is not queued again");
        s.start();
        assert!(!s.wake(), "a task being polled is not queued while the poll runs");
        assert!(!s.wake(), "nor by a second wake during the same poll");
        assert!(s.pause(), "the worker queues it again when the poll returns");
        s.start();
        assert!(!s.pause(), "a poll no wake landed in leaves the task idle");
        assert!(s.wake(), "and the next wake queues it");
    }

    #[test]
    fn a_finished_task_is_never_queued_again() {
        let s = RunState::new();
        assert!(s.wake());
        s.start();
        assert!(!s.wake());
        s.finish();
        assert!(!s.wake(), "a wake after the future completed queues nothing");
        assert!(!s.wake());
    }
}
