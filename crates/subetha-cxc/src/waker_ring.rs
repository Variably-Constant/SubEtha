//! `WakerRing`: a thread-free async ring. The producer fires the
//! consumer task's `Waker` directly on push - no worker thread per
//! awaiting task, no reactor, no syscall on the wake path.
//!
//! This is the piece that lets a bounded pool serve an unbounded set of
//! awaiting subscribers. Contrast [`crate::async_ring::AsyncSpscRing`],
//! which spawns one OS thread per in-flight future to do the blocking
//! recv: fine for a handful of long-running tasks, wrong for ten
//! thousand. Here a consumer that finds the ring empty parks its
//! `Waker` in a process-local cell beside the ring; the producer's push
//! takes that `Waker` and wakes it, which re-enqueues the task on
//! whatever executor is driving it (including [`crate::task_pool`]).
//!
//! Why no OS primitive is involved: the producer and consumer share an
//! address space, so the producer can call the consumer's `Waker`
//! directly. There is no kernel in the loop and nothing platform-
//! specific - it runs the same on Windows and Linux. (The cross-process
//! case is the one that needs a per-process reactor bridging the MMF
//! `CrossProcessWaker` to local `Waker`s; this module is the intra-
//! process foundation that reactor reuses.)
//!
//! The lost-wake race is closed by the standard register-then-recheck:
//! a poll that finds the ring empty registers its `Waker`, then checks
//! the ring ONCE more before returning `Pending`, so an item that
//! landed between the first check and the registration is never missed.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

use crate::shared_ring::RingError;
use crate::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

/// A single-slot home for the consumer task's `Waker`. The producer
/// `wake()`s it; the consumer `register()`s on each empty poll.
struct WakerCell {
    waker: Mutex<Option<Waker>>,
}

impl WakerCell {
    fn new() -> Self {
        Self { waker: Mutex::new(None) }
    }

    fn register(&self, w: &Waker) {
        let mut g = self.waker.lock();
        match g.as_ref() {
            // Same task re-polling: skip the clone.
            Some(existing) if existing.will_wake(w) => {}
            _ => *g = Some(w.clone()),
        }
    }

    fn wake(&self) {
        if let Some(w) = self.waker.lock().take() {
            w.wake();
        }
    }
}

/// Factory for a thread-free async SPSC pair.
pub struct WakerRing;

impl WakerRing {
    /// Anonymous in-process pair: a producer and a consumer sharing one
    /// SPSC core and one waker cell. `capacity` must be a power of two.
    pub fn create_anon_pair(
        capacity: usize,
    ) -> Result<(WakerProducer, WakerConsumer), RingError> {
        let ring = Arc::new(SpscRingCore::create_anon(capacity)?);
        let cell = Arc::new(WakerCell::new());
        Ok((
            WakerProducer { ring: Arc::clone(&ring), cell: Arc::clone(&cell) },
            WakerConsumer { ring, cell },
        ))
    }
}

/// Producer half. `try_push` publishes the item and fires the awaiting
/// consumer's `Waker` in the same call - no thread, no syscall.
pub struct WakerProducer {
    ring: Arc<SpscRingCore>,
    cell: Arc<WakerCell>,
}

impl WakerProducer {
    /// Push a payload; on success, wake the consumer task (if any is
    /// parked). Returns `Err(Full)` when the ring is full.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        let r = self.ring.try_push(payload);
        if r.is_ok() {
            self.cell.wake();
        }
        r
    }
}

/// Consumer half. `recv()` is an `.await`-able future that resolves
/// when an item arrives, suspending the task (off-thread) until then.
pub struct WakerConsumer {
    ring: Arc<SpscRingCore>,
    cell: Arc<WakerCell>,
}

impl WakerConsumer {
    /// A future that resolves to the next slot's bytes. Spawnable: it
    /// owns clones of the ring + waker cell, so it is `Send + 'static`.
    pub fn recv(&self) -> WakerRecv {
        WakerRecv {
            ring: Arc::clone(&self.ring),
            cell: Arc::clone(&self.cell),
        }
    }

    /// Non-blocking pop, for draining without awaiting.
    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.ring.try_pop(out)
    }
}

/// Future returned by [`WakerConsumer::recv`].
pub struct WakerRecv {
    ring: Arc<SpscRingCore>,
    cell: Arc<WakerCell>,
}

impl Future for WakerRecv {
    type Output = [u8; SPSC_PAYLOAD_BYTES];

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        if self.ring.try_pop(&mut out).is_ok() {
            return Poll::Ready(out);
        }
        // Register, then re-check: an item that landed between the
        // first pop and this registration is caught here, not lost.
        self.cell.register(cx.waker());
        if self.ring.try_pop(&mut out).is_ok() {
            return Poll::Ready(out);
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_pool::TaskPool;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn many_subscribers_few_threads_no_thread_per_sub() {
        // N awaiting tasks, a fixed pool. Each task awaits its own ring;
        // the producer side fires the wakers. If this needed a thread
        // per subscriber it could not run N >> pool size.
        let pool = TaskPool::new(2);
        let n = 4_000u64;
        let sum = Arc::new(AtomicU64::new(0));
        let mut producers = Vec::with_capacity(n as usize);
        for i in 0..n {
            let (p, c) = WakerRing::create_anon_pair(4).expect("pair");
            producers.push((i, p));
            let sum = Arc::clone(&sum);
            pool.spawn(async move {
                let got = c.recv().await;
                let v = u64::from_le_bytes(got[..8].try_into().unwrap());
                sum.fetch_add(v, Ordering::AcqRel);
            });
        }
        assert_eq!(pool.worker_count(), 2, "N tasks on a 2-thread pool");
        // Now feed every subscriber exactly once. Each push wakes one
        // suspended task.
        let mut payload = [0u8; SPSC_PAYLOAD_BYTES];
        for (i, p) in &producers {
            payload[..8].copy_from_slice(&i.to_le_bytes());
            while p.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
        let expected: u64 = (0..n).sum();
        let start = std::time::Instant::now();
        while sum.load(Ordering::Acquire) < expected {
            if start.elapsed() > std::time::Duration::from_secs(10) {
                panic!("sum {} != expected {expected}", sum.load(Ordering::Acquire));
            }
            std::hint::spin_loop();
        }
        assert_eq!(sum.load(Ordering::Acquire), expected);
        pool.shutdown();
    }
}
