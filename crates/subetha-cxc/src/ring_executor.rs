//! `RingExecutor`: an async executor whose READY QUEUE is built from
//! SubEtha rings. The future's handle rides through a ring; the ring IS
//! the scheduler, not a data channel beside one.
//!
//! This is the deeper async shape than [`crate::waker_ring`]. There the
//! ring carries message bytes and the future lives in a heap queue
//! (`Mutex<VecDeque>` in [`crate::task_pool`]). Here the future's
//! `Arc<Task>` handle is the ring payload: a worker pops a handle,
//! reconstructs the `Arc`, polls it; a `wake()` pushes the handle back
//! into a ring. Scheduling = a ring push; running = a ring pop.
//!
//! # Shape-adaptive ready queue
//!
//! The ready queue is NOT one ring. A single Vyukov ring funnels every
//! core's CAS through one counter and walls throughput as cores climb
//! (Vyukov contention rises sharply past a handful of producers). So
//! the executor shards the ready queue to the hardware: ONE ready-ring
//! shard per worker, worker count taken from
//! [`std::thread::available_parallelism`] (or supplied explicitly).
//! Each worker owns a home shard, drains it first, and STEALS from the
//! other shards round-robin when its own is empty. A task is homed to
//! one shard round-robin at spawn and always reschedules there, so a
//! self-waking task's handle stays on one ring (locality) and the home
//! worker is almost always the only thread touching it. The ring count
//! equals the core count: 1 core -> 1 ring, a 44-thread host -> 44
//! rings, no single-counter wall.
//!
//! Workers pin to distinct cores best-effort
//! ([`crate::cpu_affinity`]); on a host without an affinity API they
//! run unpinned.
//!
//! # Why this answers "uncapped consumers"
//!
//! WORKERS are the hardware parallelism - a small, fixed cap matched to
//! the machine. TASKS are unbounded: they are `Arc<Task>` handles
//! multiplexed onto the worker pool through the rings, not threads. A
//! 44-thread host drives an arbitrary task population on 44 workers.
//!
//! # Handle / refcount discipline
//!
//! A task is in its home ring at most once, gated by a `scheduled`
//! flag:
//!  - `spawn` / `wake` flip `scheduled` false->true and push one
//!    `Arc::into_raw` handle. A redundant wake (flag already true) drops
//!    its clone instead of double-pushing.
//!  - a worker pops a handle, `Arc::from_raw` reclaims that ref, clears
//!    `scheduled`, and polls. `Ready` drops the future and the run-ref;
//!    `Pending` drops the run-ref, leaving the future's stashed waker
//!    clone as the liveness anchor until the next wake re-pushes.
//!
//! Because each live task occupies at most one slot of its home shard,
//! a shard sized to `>= peak tasks homed there` never returns `Full`;
//! the round-robin home spread keeps that at about `peak_tasks /
//! shards`. A `Full` push (only possible under an adversarial
//! liveness/home correlation) spins until a stealer drains the shard,
//! which is deadlock-free whenever more than one worker runs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
use std::thread::JoinHandle;

use parking_lot::Mutex;

use crate::shared_ring::{SharedRing, PAYLOAD_BYTES};

/// One scheduled unit of work. The future is `Option`-wrapped so a
/// completed task drops its future and any later (spurious) wake that
/// re-pushes the handle finds `None` and skips it.
struct Task {
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    /// This task's home ready-ring shard; it always schedules here.
    home: Arc<SharedRing>,
    /// Live count of not-yet-complete tasks, shared with the executor;
    /// decremented exactly once when this task first returns `Ready`.
    pending: Arc<AtomicUsize>,
    /// True while a handle for this task sits in its home ring. Gates
    /// the at-most-one-handle-per-task invariant.
    scheduled: AtomicBool,
}

impl Task {
    /// Enqueue this task into its home ready shard, at most once. A
    /// second caller while a handle is already queued drops its clone
    /// instead of pushing a duplicate.
    fn schedule(self: &Arc<Self>) {
        if self.scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        let raw = Arc::into_raw(Arc::clone(self)) as usize as u64;
        let mut buf = [0u8; PAYLOAD_BYTES];
        buf[..8].copy_from_slice(&raw.to_le_bytes());
        // Home shard is sized to its expected peak load, so this push
        // almost never fails; if an adversarial home/liveness skew fills
        // it, spin until a stealing worker drains a slot.
        while self.home.try_push(&buf).is_err() {
            std::hint::spin_loop();
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.schedule();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
    }
}

/// A hardware-shaped pool of worker threads draining an unbounded set
/// of tasks through per-worker SubEtha ready-ring shards.
pub struct RingExecutor {
    shards: Arc<Vec<Arc<SharedRing>>>,
    pending: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    next_home: AtomicUsize,
    pinned: Arc<AtomicUsize>,
    workers: Vec<JoinHandle<()>>,
    shard_capacity: usize,
}

impl RingExecutor {
    /// Build an executor whose worker count matches the host's logical
    /// core count ([`std::thread::available_parallelism`]). The ready
    /// queue gets one shard per worker; `max_tasks` is the peak number
    /// of simultaneously-live tasks the queue must hold.
    pub fn with_available_parallelism(max_tasks: usize) -> Self {
        let n = std::thread::available_parallelism()
            .map(|x| x.get())
            .unwrap_or(1);
        Self::new(n, max_tasks)
    }

    /// Build an executor with `n_workers` worker threads (one ready
    /// shard each) and a ready queue sized to hold at least `max_tasks`
    /// simultaneously-live task handles, spread across the shards.
    pub fn new(n_workers: usize, max_tasks: usize) -> Self {
        let n = n_workers.max(1);
        // Each shard holds about an even slice of the peak live set.
        let per_shard = max_tasks.div_ceil(n).max(1);
        let shard_capacity = per_shard.next_power_of_two().max(2);

        let shards: Vec<Arc<SharedRing>> = (0..n)
            .map(|_| {
                Arc::new(
                    SharedRing::create_anon(shard_capacity)
                        .expect("ready shard alloc"),
                )
            })
            .collect();
        let shards = Arc::new(shards);
        let pending = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let pinned = Arc::new(AtomicUsize::new(0));
        // Startup barrier: every worker bumps `ready` after its pin
        // attempt, so by the time `new` returns the pinned count is
        // final and the pool is fully spun up.
        let ready = Arc::new(AtomicUsize::new(0));

        let workers = (0..n)
            .map(|w| {
                let shards = Arc::clone(&shards);
                let shutdown = Arc::clone(&shutdown);
                let pinned = Arc::clone(&pinned);
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || {
                    if crate::cpu_affinity::pin_current_thread_to_core(w) {
                        pinned.fetch_add(1, Ordering::AcqRel);
                    }
                    ready.fetch_add(1, Ordering::AcqRel);
                    worker_loop(shards, w, shutdown);
                })
            })
            .collect();

        while ready.load(Ordering::Acquire) < n {
            std::hint::spin_loop();
        }

        Self {
            shards,
            pending,
            shutdown,
            next_home: AtomicUsize::new(0),
            pinned,
            workers,
            shard_capacity,
        }
    }

    /// Spawn a future. It runs to completion on the pool, suspending
    /// (off-thread) whenever it awaits, with no thread dedicated to it.
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.pending.fetch_add(1, Ordering::AcqRel);
        let home_idx =
            self.next_home.fetch_add(1, Ordering::Relaxed) % self.shards.len();
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            home: Arc::clone(&self.shards[home_idx]),
            pending: Arc::clone(&self.pending),
            scheduled: AtomicBool::new(false),
        });
        task.schedule();
    }

    /// Number of tasks not yet complete.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    /// Number of worker threads (one ready shard each).
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Number of ready-ring shards (equals worker count).
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// How many workers were pinned to a distinct core (best-effort;
    /// 0 on hosts without an affinity API). Read after the workers have
    /// started.
    pub fn pinned_workers(&self) -> usize {
        self.pinned.load(Ordering::Acquire)
    }

    /// Per-shard slot capacity (power of two).
    pub fn shard_capacity(&self) -> usize {
        self.shard_capacity
    }

    /// Spin until every spawned task has completed.
    pub fn wait_idle(&self) {
        while self.pending() > 0 {
            std::hint::spin_loop();
        }
    }

    /// Stop the workers (after the rings drain) and join them. Call
    /// once the spawned work has completed.
    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Release);
        for w in self.workers {
            w.join().ok();
        }
    }
}

/// Poll one task handle popped from a ready shard. Reclaims the ref the
/// producer transferred into the ring, polls once, and on completion
/// drops the future and decrements the live count.
fn run_handle(raw: usize) {
    let task = unsafe { Arc::from_raw(raw as *const Task) };
    // Allow a wake during this poll to re-schedule the task.
    task.scheduled.store(false, Ordering::Release);

    let mut guard = task.future.lock();
    if let Some(fut) = guard.as_mut() {
        let waker = Waker::from(Arc::clone(&task));
        let mut cx = Context::from_waker(&waker);
        if fut.as_mut().poll(&mut cx).is_ready() {
            *guard = None;
            drop(guard);
            task.pending.fetch_sub(1, Ordering::AcqRel);
        }
    }
    // Dropping `task` releases the run-ref. If the poll returned
    // Pending, the future's stashed waker clone keeps the task alive
    // until the next wake re-pushes a handle.
}

fn worker_loop(
    shards: Arc<Vec<Arc<SharedRing>>>,
    home: usize,
    shutdown: Arc<AtomicBool>,
) {
    let s = shards.len();
    let mut buf = [0u8; PAYLOAD_BYTES];
    loop {
        // Home shard first, then steal round-robin from the others.
        let mut ran = false;
        for k in 0..s {
            let idx = (home + k) % s;
            if shards[idx].try_pop(&mut buf).is_ok() {
                let raw =
                    u64::from_le_bytes(buf[..8].try_into().unwrap()) as usize;
                run_handle(raw);
                ran = true;
                break;
            }
        }
        if !ran {
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    /// A future that re-schedules itself `left` times before completing,
    /// so each yield is one round-trip of the task handle through a
    /// ready shard.
    struct YieldN {
        left: u32,
    }
    impl Future for YieldN {
        type Output = ();
        fn poll(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<()> {
            if self.left == 0 {
                std::task::Poll::Ready(())
            } else {
                self.left -= 1;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }

    #[test]
    fn many_tasks_few_workers_through_the_rings() {
        // Tasks >> workers: the shards multiplex an unbounded task set
        // onto a fixed worker pool. Each task yields several times, so
        // handles cycle through their home shard repeatedly; an idle
        // worker steals from other shards.
        let n_tasks = 20_000u64;
        let exec = RingExecutor::new(4, n_tasks as usize);
        assert_eq!(exec.worker_count(), 4);
        assert_eq!(exec.shard_count(), 4);

        let done = Arc::new(AtomicU64::new(0));
        for _ in 0..n_tasks {
            let done = Arc::clone(&done);
            exec.spawn(async move {
                YieldN { left: 5 }.await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }

        let start = std::time::Instant::now();
        while done.load(Ordering::Acquire) < n_tasks {
            if start.elapsed() > std::time::Duration::from_secs(30) {
                panic!("only {} of {n_tasks} tasks completed",
                       done.load(Ordering::Acquire));
            }
            std::hint::spin_loop();
        }
        assert_eq!(done.load(Ordering::Acquire), n_tasks);
        exec.wait_idle();
        exec.shutdown();
    }

    #[test]
    fn completes_with_single_worker() {
        // One worker, one shard: a self-waking task re-enters its home
        // ring and the same worker picks it up. No stealing needed.
        let exec = RingExecutor::new(1, 4_000);
        assert_eq!(exec.shard_count(), 1);
        let done = Arc::new(AtomicU64::new(0));
        for _ in 0..4_000u64 {
            let done = Arc::clone(&done);
            exec.spawn(async move {
                YieldN { left: 3 }.await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }
        exec.wait_idle();
        assert_eq!(done.load(Ordering::Acquire), 4_000);
        exec.shutdown();
    }

    #[test]
    fn adapts_worker_count_to_hardware() {
        // The auto-detected constructor sizes workers to the host; the
        // shard count tracks it, and the task set still drains.
        let exec = RingExecutor::with_available_parallelism(2_000);
        assert!(exec.worker_count() >= 1);
        assert_eq!(exec.shard_count(), exec.worker_count());
        let done = Arc::new(AtomicU64::new(0));
        for _ in 0..2_000u64 {
            let done = Arc::clone(&done);
            exec.spawn(async move {
                YieldN { left: 2 }.await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }
        exec.wait_idle();
        assert_eq!(done.load(Ordering::Acquire), 2_000);
        exec.shutdown();
    }
}
