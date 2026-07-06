//! `AsyncSpscRing`: `Future`-shaped async adapter on top of
//! [`crate::blocking_spsc_ring::BlockingSpscRing`].
//!
//! Turns the synchronous `send_blocking` / `recv_blocking` API into
//! `send(...).await` / `recv(...).await` so SubEtha rings compose
//! with any async executor (tokio, smol, async-std, custom). The
//! adapter is executor-agnostic: it uses only `std::future::Future`
//! plus `std::thread` to bridge the cross-process kernel-park to
//! the Rust `Waker` ecosystem.
//!
//! # How the bridge works
//!
//! Rust's async model is "Future returns `Pending` and registers a
//! Waker; something fires the Waker; executor re-polls." The
//! underlying `CrossProcessWaker` is a kernel-park primitive that
//! does the wait OFF the async runtime's thread. To bridge:
//!
//! 1. First poll calls `try_*` on the inner ring. If immediately
//!    ready, return `Poll::Ready`.
//! 2. Otherwise, spawn a std::thread that calls the blocking
//!    counterpart (`recv_blocking` / `send_blocking`) with the
//!    caller-supplied timeout. Park the rust Waker.
//! 3. When the blocking call returns, store the result + fire the
//!    Waker.
//! 4. Next poll observes the stored result and returns `Poll::Ready`.
//!
//! # Why a bounded timeout is required
//!
//! Dropping a pending `AsyncRecv` / `AsyncSend` future does NOT
//! cancel the spawned worker thread (`std::thread` lacks safe
//! cancellation). The thread's worst-case lifetime equals the
//! caller-supplied timeout. Unbounded waits are rejected at the
//! type level by requiring `Duration` (not `Option<Duration>`).
//!
//! # Worker-thread cost model
//!
//! Each in-flight future spawns one OS thread. This pattern fits
//! the substrate's intended use of async (a small number of
//! long-running consumer tasks per process, not thousands of
//! short-lived futures). Callers driving high concurrency batch
//! through one `BlockingSpscRing` per consumer task and call
//! `recv_blocking` directly inside `tokio::task::spawn_blocking`
//! (or the executor's equivalent).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::blocking_spsc_ring::{BlockingError, BlockingSpscRing};
use crate::shared_ring::RingError;

/// Wrapper providing `.recv(timeout).await` and `.send(timeout).await`.
pub struct AsyncSpscRing {
    inner: Arc<BlockingSpscRing>,
}

impl AsyncSpscRing {
    /// Wrap an existing `BlockingSpscRing`. Both halves share the
    /// same underlying ring + wakers; this is just an async-shaped
    /// view of the same primitive.
    pub fn new(inner: Arc<BlockingSpscRing>) -> Self {
        Self { inner }
    }

    /// Async pop with bounded wait. Returns a future that resolves
    /// when an item arrives, or `BlockingError::Timeout` after the
    /// caller-supplied duration.
    pub fn recv(&self, timeout: Duration) -> AsyncRecv {
        AsyncRecv {
            ring: Arc::clone(&self.inner),
            state: Arc::new(Mutex::new(SlotState::Pending)),
            timeout,
            spawned: false,
        }
    }

    /// Async push with bounded wait.
    pub fn send(&self, payload: Vec<u8>, timeout: Duration) -> AsyncSend {
        AsyncSend {
            ring: Arc::clone(&self.inner),
            state: Arc::new(Mutex::new(SlotState::Pending)),
            timeout,
            payload: Some(payload),
            spawned: false,
        }
    }

    /// Direct access to the underlying blocking ring.
    pub fn inner(&self) -> &Arc<BlockingSpscRing> { &self.inner }
}

/// Shared state between the async future and its worker thread.
enum SlotState<T> {
    Pending,
    Ready(T),
    Parked(Waker),
}

type RecvSlot = Arc<Mutex<SlotState<Result<Vec<u8>, BlockingError>>>>;
type SendSlot = Arc<Mutex<SlotState<Result<(), BlockingError>>>>;

/// Future returned by [`AsyncSpscRing::recv`].
pub struct AsyncRecv {
    ring: Arc<BlockingSpscRing>,
    state: RecvSlot,
    timeout: Duration,
    spawned: bool,
}

impl Future for AsyncRecv {
    type Output = Result<Vec<u8>, BlockingError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Fast path: data already present, skip the worker thread.
        if !this.spawned {
            let mut buf = vec![0u8; 64];
            match this.ring.try_pop(&mut buf) {
                Ok(n) => {
                    buf.truncate(n);
                    return Poll::Ready(Ok(buf));
                }
                Err(RingError::Empty) => {}
                Err(e) => return Poll::Ready(Err(BlockingError::Ring(e))),
            }
        }

        let mut guard = this.state.lock().unwrap();
        match &mut *guard {
            SlotState::Ready(_) => {
                let taken = std::mem::replace(&mut *guard, SlotState::Pending);
                match taken {
                    SlotState::Ready(r) => return Poll::Ready(r),
                    _ => unreachable!(),
                }
            }
            SlotState::Pending | SlotState::Parked(_) => {
                *guard = SlotState::Parked(cx.waker().clone());
            }
        }
        drop(guard);

        if !this.spawned {
            this.spawned = true;
            let ring = Arc::clone(&this.ring);
            let state = Arc::clone(&this.state);
            let timeout = this.timeout;
            std::thread::spawn(move || {
                let mut buf = vec![0u8; 64];
                let r = ring.recv_blocking(&mut buf, Some(timeout)).map(|n| {
                    buf.truncate(n);
                    buf
                });
                finish_slot(&state, r);
            });
        }

        Poll::Pending
    }
}

/// Future returned by [`AsyncSpscRing::send`].
pub struct AsyncSend {
    ring: Arc<BlockingSpscRing>,
    state: SendSlot,
    timeout: Duration,
    payload: Option<Vec<u8>>,
    spawned: bool,
}

impl Future for AsyncSend {
    type Output = Result<(), BlockingError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if !this.spawned {
            let payload_ref = this.payload.as_ref().expect("payload taken twice");
            match this.ring.try_push(payload_ref) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(RingError::Full) => {}
                Err(e) => return Poll::Ready(Err(BlockingError::Ring(e))),
            }
        }

        let mut guard = this.state.lock().unwrap();
        match &mut *guard {
            SlotState::Ready(_) => {
                let taken = std::mem::replace(&mut *guard, SlotState::Pending);
                match taken {
                    SlotState::Ready(r) => return Poll::Ready(r),
                    _ => unreachable!(),
                }
            }
            SlotState::Pending | SlotState::Parked(_) => {
                *guard = SlotState::Parked(cx.waker().clone());
            }
        }
        drop(guard);

        if !this.spawned {
            this.spawned = true;
            let ring = Arc::clone(&this.ring);
            let state = Arc::clone(&this.state);
            let timeout = this.timeout;
            let payload = this.payload.take().expect("payload taken twice");
            std::thread::spawn(move || {
                let r = ring.send_blocking(&payload, Some(timeout));
                finish_slot(&state, r);
            });
        }

        Poll::Pending
    }
}

fn finish_slot<T>(state: &Arc<Mutex<SlotState<T>>>, value: T) {
    let waker_to_fire = {
        let mut guard = state.lock().unwrap();
        let prev = std::mem::replace(&mut *guard, SlotState::Ready(value));
        match prev {
            SlotState::Parked(w) => Some(w),
            _ => None,
        }
    };
    if let Some(w) = waker_to_fire {
        w.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    /// Minimal hand-rolled block-on for tests: poll the future,
    /// park on a condvar when it returns Pending, re-poll when
    /// the waker fires.
    struct TestWaker {
        woken: std::sync::Mutex<bool>,
        cv: std::sync::Condvar,
    }
    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            let mut g = self.woken.lock().unwrap();
            *g = true;
            self.cv.notify_one();
        }
    }

    fn block_on<F: Future>(mut fut: F) -> F::Output {
        let waker_inner = Arc::new(TestWaker {
            woken: std::sync::Mutex::new(true),
            cv: std::sync::Condvar::new(),
        });
        let waker: Waker = Arc::clone(&waker_inner).into();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: future stays on the stack; we never move it again.
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        loop {
            {
                let mut g = waker_inner.woken.lock().unwrap();
                while !*g {
                    g = waker_inner.cv.wait(g).unwrap();
                }
                *g = false;
            }
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => continue,
            }
        }
    }

    #[test]
    fn recv_returns_immediately_when_ring_has_item() {
        let ring = Arc::new(BlockingSpscRing::create_anon(4).expect("ring"));
        let mut payload = [0u8; 56];
        payload[..8].copy_from_slice(&42u64.to_le_bytes());
        ring.try_push(&payload).expect("push");

        let adapter = AsyncSpscRing::new(Arc::clone(&ring));
        let got = block_on(adapter.recv(Duration::from_secs(1))).unwrap();
        let val = u64::from_le_bytes(got[..8].try_into().unwrap());
        assert_eq!(val, 42);
    }

    #[test]
    fn recv_parks_then_completes_when_producer_pushes() {
        let ring = Arc::new(BlockingSpscRing::create_anon(4).expect("ring"));
        let r2 = Arc::clone(&ring);
        let pushed = Arc::new(AtomicBool::new(false));
        let pushed2 = Arc::clone(&pushed);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&7u64.to_le_bytes());
            // Publish the flag BEFORE the push. `recv` returns the
            // instant the item is visible, and the ring's Release on
            // `head` (paired with the consumer's Acquire) carries this
            // store with it - so the assert below never races a flag
            // that was published after the data.
            pushed2.store(true, Ordering::Release);
            r2.try_push(&payload).expect("push");
        });
        let adapter = AsyncSpscRing::new(Arc::clone(&ring));
        let got = block_on(adapter.recv(Duration::from_secs(2))).unwrap();
        assert!(pushed.load(Ordering::Acquire), "producer ran");
        let val = u64::from_le_bytes(got[..8].try_into().unwrap());
        assert_eq!(val, 7);
    }

    #[test]
    fn recv_times_out_when_no_producer() {
        let ring = Arc::new(BlockingSpscRing::create_anon(4).expect("ring"));
        let adapter = AsyncSpscRing::new(Arc::clone(&ring));
        let t0 = std::time::Instant::now();
        let r = block_on(adapter.recv(Duration::from_millis(80)));
        assert!(matches!(r, Err(BlockingError::Timeout)));
        assert!(t0.elapsed() >= Duration::from_millis(60));
    }

    #[test]
    fn send_completes_immediately_when_ring_not_full() {
        let ring = Arc::new(BlockingSpscRing::create_anon(4).expect("ring"));
        let adapter = AsyncSpscRing::new(Arc::clone(&ring));
        let payload = (12345u64).to_le_bytes().to_vec();
        block_on(adapter.send(payload, Duration::from_secs(1))).unwrap();
        let mut buf = [0u8; 64];
        let n = ring.try_pop(&mut buf).expect("pop");
        let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
        assert_eq!(v, 12345);
        assert!(n >= 8);
    }
}
