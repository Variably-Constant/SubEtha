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
//! The ready queue is a `Mutex<VecDeque>` + `Condvar` on purpose: the
//! executor's own scheduling is not the thing under test, and a
//! std-only queue keeps the dependency surface at zero.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Waker};
use std::thread::JoinHandle;

/// One scheduled unit of work. The future is `Option`-wrapped so a
/// completed task drops its future and any later (spurious) wake that
/// re-enqueues it is a no-op rather than a poll-after-ready contract
/// violation.
struct Task {
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    ready: Arc<ReadyQueue>,
}

impl std::task::Wake for Task {
    fn wake(self: Arc<Self>) {
        self.ready.clone().push(self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.clone().push(self.clone());
    }
}

struct ReadyQueue {
    queue: Mutex<VecDeque<Arc<Task>>>,
    signal: Condvar,
    shutdown: AtomicBool,
}

impl ReadyQueue {
    fn push(&self, task: Arc<Task>) {
        self.queue.lock().unwrap().push_back(task);
        self.signal.notify_one();
    }

    /// Block until a task is available or shutdown is requested.
    fn pop(&self) -> Option<Arc<Task>> {
        let mut q = self.queue.lock().unwrap();
        loop {
            if let Some(task) = q.pop_front() {
                return Some(task);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            q = self.signal.wait(q).unwrap();
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
            queue: Mutex::new(VecDeque::new()),
            signal: Condvar::new(),
            shutdown: AtomicBool::new(false),
        });
        let workers = (0..n)
            .map(|_| {
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || worker_loop(ready))
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
            future: Mutex::new(Some(Box::pin(future))),
            ready: Arc::clone(&self.ready),
        });
        self.ready.push(task);
    }

    /// Stop the workers once the current ready queue drains. Joins all
    /// threads. Call after the work you spawned has completed.
    pub fn shutdown(self) {
        self.ready.shutdown.store(true, Ordering::Release);
        self.ready.signal.notify_all();
        for w in self.workers {
            w.join().ok();
        }
    }
}

fn worker_loop(ready: Arc<ReadyQueue>) {
    while let Some(task) = ready.pop() {
        let mut guard = task.future.lock().unwrap();
        if let Some(fut) = guard.as_mut() {
            let waker = Waker::from(Arc::clone(&task));
            let mut cx = Context::from_waker(&waker);
            if fut.as_mut().poll(&mut cx).is_ready() {
                // Done: drop the future so a later spurious wake that
                // re-enqueues this task finds `None` and skips it.
                *guard = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
