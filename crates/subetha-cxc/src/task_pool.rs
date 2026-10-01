//! `TaskPool`: a minimal bounded async executor, no external runtime.
//!
//! The substrate's rings are executor-agnostic (any `std::future`
//! executor drives them). `TaskPool` is the proof that "any" includes
//! "a tiny one we ship ourselves": a fixed pool of worker threads
//! running an arbitrary number of suspended tasks. There is no tokio,
//! no reactor, no per-task thread. A task that awaits a ring parks its
//! `Waker` in the ring (see [`crate::waker_ring`]); the producer's push
//! fires that `Waker`, which re-enqueues the task here, and a worker
//! polls it. M threads, N tasks, with M fixed and N unbounded.
//!
//! The ready queue is an unbounded lock-free queue every worker pops
//! from. A task's run state is one atomic word: a wake queues an idle
//! task, and a wake that lands while a worker polls the task is queued
//! by that worker when the poll returns, so a task is in the queue at
//! most once and one worker polls it at a time. A worker that finds the
//! queue empty parks, and a push unparks one parked worker.

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Waker};
use std::thread::{JoinHandle, Thread};

use crate::run_state::RunState;
use crate::unbounded_queue::UnboundedQueue;

/// One scheduled unit of work.
struct Task {
    /// Polled only by the worker that took the task from the queue,
    /// until that poll returns; `None` once the future has completed.
    future: UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    state: RunState,
    ready: Arc<ReadyQueue>,
}

// SAFETY: `future` is reached only by the worker between `RunState::start`
// and the `pause` or `finish` that ends its poll, and the run state lets one
// worker at a time hold the task that way. The other fields are `Sync`.
unsafe impl Sync for Task {}

impl std::task::Wake for Task {
    fn wake(self: Arc<Self>) {
        if self.state.wake() {
            let ready = Arc::clone(&self.ready);
            ready.push(self);
        }
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.state.wake() {
            self.ready.push(Arc::clone(self));
        }
    }
}

/// A worker's parking spot: its thread, recorded when the worker starts,
/// and whether it has announced a park no push has yet answered.
#[derive(Default)]
struct Spot {
    thread: OnceLock<Thread>,
    parked: AtomicBool,
}

struct ReadyQueue {
    queue: UnboundedQueue<Arc<Task>>,
    /// One spot per worker.
    spots: Box<[Spot]>,
    /// How many spots hold an announced park.
    parked: AtomicUsize,
    shutdown: AtomicBool,
}

impl ReadyQueue {
    fn push(&self, task: Arc<Task>) {
        self.queue.push(task);
        // Pairs with the fence a parking worker puts between announcing
        // its park and looking at the queue: either that look finds this
        // push, or the read below finds the announcement.
        fence(Ordering::SeqCst);
        if self.parked.load(Ordering::Relaxed) > 0 {
            self.unpark_one();
        }
    }

    /// Unparks one worker whose announced park no push has answered yet.
    fn unpark_one(&self) {
        for spot in self.spots.iter() {
            if spot.parked.load(Ordering::Relaxed) && spot.parked.swap(false, Ordering::Acquire) {
                self.parked.fetch_sub(1, Ordering::Relaxed);
                if let Some(thread) = spot.thread.get() {
                    thread.unpark();
                }
                return;
            }
        }
    }

    /// The next task for the worker at `spot`, parking while there is
    /// none. `None` once shutdown is requested and the queue is empty.
    fn pop(&self, spot: &Spot) -> Option<Arc<Task>> {
        loop {
            if let Some(task) = self.queue.pop() {
                return Some(task);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            // The count rises before the spot is marked, and whoever clears
            // the mark reads it with Acquire before lowering the count, so
            // each decrement follows the increment it answers and the count
            // never wraps below zero.
            self.parked.fetch_add(1, Ordering::Relaxed);
            spot.parked.store(true, Ordering::Release);
            fence(Ordering::SeqCst);
            if self.queue.is_empty() && !self.shutdown.load(Ordering::Relaxed) {
                std::thread::park();
            }
            // Withdraw the announcement, unless a push has answered it.
            if spot.parked.swap(false, Ordering::Acquire) {
                self.parked.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// A fixed-size pool of worker threads driving an unbounded set of
/// suspended tasks.
pub struct TaskPool {
    ready: Arc<ReadyQueue>,
    workers: Vec<JoinHandle<()>>,
}

impl TaskPool {
    /// Build a pool with `n_workers` threads (clamped to at least 1).
    pub fn new(n_workers: usize) -> Self {
        let n = n_workers.max(1);
        let ready = Arc::new(ReadyQueue {
            queue: UnboundedQueue::new(),
            spots: (0..n).map(|_| Spot::default()).collect(),
            parked: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });
        let workers = (0..n)
            .map(|me| {
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || worker_loop(&ready, me))
            })
            .collect();
        Self { ready, workers }
    }

    /// Number of worker threads in the pool.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Spawn a future. It runs to completion on the pool, suspending
    /// (off-thread) whenever it awaits, with no thread dedicated to it.
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let task = Arc::new(Task {
            future: UnsafeCell::new(Some(Box::pin(future))),
            state: RunState::new(),
            ready: Arc::clone(&self.ready),
        });
        std::task::Wake::wake(task);
    }

    /// Stop the workers once the current ready queue drains. Joins all
    /// threads. Call after the work you spawned has completed.
    ///
    /// Every worker is unparked after the flag is set. A worker already
    /// parked wakes to it, and one between its last look at the flag and
    /// its park finds the unpark delivered and does not sleep.
    pub fn shutdown(self) {
        self.ready.shutdown.store(true, Ordering::Release);
        for w in &self.workers {
            w.thread().unpark();
        }
        for w in self.workers {
            // A worker that panicked panicked in a task; the caller who
            // asked for the shutdown is the one to hear it.
            if let Err(payload) = w.join() {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

fn worker_loop(ready: &ReadyQueue, me: usize) {
    let spot = &ready.spots[me];
    spot.thread.get_or_init(std::thread::current);
    while let Some(task) = ready.pop(spot) {
        run(task);
    }
}

/// Poll a task taken from the queue once, then finish it or, when a wake
/// landed during the poll, queue it again.
fn run(task: Arc<Task>) {
    task.state.start();
    let finished = {
        // SAFETY: `start` made this worker the only one polling the task
        // until the `pause` or `finish` below, and this borrow ends first.
        let slot = unsafe { &mut *task.future.get() };
        let future = slot.as_mut().expect("a queued task has not finished");
        let waker = Waker::from(Arc::clone(&task));
        let mut cx = Context::from_waker(&waker);
        let finished = future.as_mut().poll(&mut cx).is_ready();
        if finished {
            *slot = None;
        }
        finished
    };
    if finished {
        task.state.finish();
    } else if task.state.pause() {
        let ready = Arc::clone(&task.ready);
        ready.push(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_state::{PollProbe, WokenMidPoll};
    use std::sync::atomic::AtomicU64;

    #[test]
    fn runs_many_tasks_on_few_threads_with_yields() {
        // Each task yields once (returns Pending then re-wakes itself),
        // so the pool must round-trip them through the ready queue.
        let pool = TaskPool::new(2);
        let done = Arc::new(AtomicU64::new(0));
        let n = 5_000u64;
        for _ in 0..n {
            let done = Arc::clone(&done);
            pool.spawn(async move {
                YieldOnce::default().await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }
        // Spin until all complete (a real executor would join handles;
        // this test just watches the shared counter).
        let start = std::time::Instant::now();
        while done.load(Ordering::Acquire) < n {
            if start.elapsed() > std::time::Duration::from_secs(10) {
                panic!("only {} of {n} tasks finished", done.load(Ordering::Acquire));
            }
            std::hint::spin_loop();
        }
        assert_eq!(pool.worker_count(), 2);
        pool.shutdown();
    }

    #[test]
    fn a_task_woken_during_its_poll_runs_again_and_never_in_two_polls_at_once() {
        // Two workers, one task: every poll of the task wakes it and then
        // stays in the poll, so the idle worker is free to take the task
        // if a wake during a poll queued it.
        let pool = TaskPool::new(2);
        let probe = Arc::new(PollProbe::default());
        let done = Arc::new(AtomicBool::new(false));
        {
            let probe = Arc::clone(&probe);
            let done = Arc::clone(&done);
            pool.spawn(async move {
                WokenMidPoll { probe, polls_left: 8 }.await;
                done.store(true, Ordering::Release);
            });
        }
        crate::test_races::within_lost(|| {
            done.load(Ordering::Acquire) || probe.overlaps.load(Ordering::Acquire) > 0
        });
        assert_eq!(
            probe.overlaps.load(Ordering::Acquire),
            0,
            "no poll of the task started while another was running"
        );
        assert!(done.load(Ordering::Acquire), "the task completes");
        pool.shutdown();
    }

    #[derive(Default)]
    struct YieldOnce {
        yielded: bool,
    }
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<()> {
            if self.yielded {
                std::task::Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}
