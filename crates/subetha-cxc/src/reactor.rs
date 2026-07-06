//! `reactor`: the bridge that makes a SubEtha ring a first-class async
//! source ACROSS processes, not just across threads.
//!
//! Intra-process async is direct: the producer holds the consumer's
//! `Waker` and fires it on push (see [`crate::waker_ring`]). Across
//! processes the producer is in another address space and cannot touch
//! a local `Waker`, so a parked future needs something in THIS process
//! to notice the cross-process publish and fire its `Waker`. That is
//! the reactor: one background thread per process that blocks on the
//! MMF [`CrossProcessWaker`], and when another process publishes, fires
//! the local `Waker` of the
//! future parked on the ring. It is the epoll/IOCP reactor pattern with
//! the readiness source being a shared-memory ring head instead of a
//! socket.
//!
//! # One surface, two locales
//!
//! [`ReactiveReceiver::recv`] returns the same future whether the
//! producer is a thread or a process:
//!  - [`anon_pair`] builds an intra-process channel; the sender fires
//!    the receiver's `Waker` directly, no reactor thread.
//!  - [`receiver_cross`] / [`sender_cross`] build the cross-process
//!    halves over a shared MMF ring + named waker; a reactor thread in
//!    the consumer bridges the publish to the local `Waker`.
//!
//! Unlike [`crate::async_ring`], which spawns one OS thread per
//! in-flight future, the reactor uses ONE thread per process regardless
//! of how many futures park on the ring.
//!
//! # `block_on`
//!
//! [`block_on`] is a minimal thread-parking driver: it sleeps the
//! calling thread between polls and is unparked by the future's `Waker`.
//! Paired with the reactor, a consumer process genuinely sleeps (both
//! the driver thread and the reactor thread park in the kernel) until
//! another process publishes - no busy-spin.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;

use crate::cross_process_waker::CrossProcessWaker;
use crate::shared_ring::RingError;
use crate::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

/// Maximum the reactor sleeps per wait before re-checking the ring head,
/// independent of the cross-process wake. The wake (the common path)
/// returns far sooner; this tick only matters when a wake is lost to the
/// `CrossProcessWaker` register/wake visibility race, which the head
/// re-check at the loop top then heals. Bounded, so a lost wake cannot
/// hang the consumer; large enough that an idle reactor barely ticks.
const REACTOR_HEAL_INTERVAL: Duration = Duration::from_millis(50);

/// A `Waker` that unparks a specific thread. The driver behind
/// [`block_on`].
struct ThreadWaker {
    thread: std::thread::Thread,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

/// Drive a future to completion on the current thread, parking the
/// thread between polls. The future's `Waker` unparks it; a reactor
/// (or a local sender) fires that `Waker` on readiness.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWaker {
        thread: std::thread::current(),
    }));
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            // A spurious unpark just re-polls (which re-checks the ring
            // and re-parks), so this is correct without a flag.
            Poll::Pending => std::thread::park(),
        }
    }
}

/// Where a sender's push delivers its readiness signal.
enum SenderSignal {
    /// Intra-process: fire the receiver's `Waker` directly.
    Local(Arc<Mutex<Option<Waker>>>),
    /// Cross-process: wake the consumer's reactor through the MMF.
    Cross(Arc<CrossProcessWaker>),
}

/// Producer half. `try_send` publishes the payload and signals the
/// consumer - a direct `Waker` fire intra-process, an MMF wake
/// cross-process.
pub struct ReactiveSender {
    ring: Arc<SpscRingCore>,
    signal: SenderSignal,
}

impl ReactiveSender {
    /// Push a payload and signal the consumer. Returns `Err(Full)` when
    /// the ring is full (the signal is sent only on a successful push).
    pub fn try_send(&self, payload: &[u8]) -> Result<(), RingError> {
        self.ring.try_push(payload)?;
        match &self.signal {
            SenderSignal::Local(slot) => {
                if let Some(w) = slot.lock().take() {
                    w.wake();
                }
            }
            SenderSignal::Cross(xwaker) => {
                xwaker.wake_up_to(self.ring.head());
            }
        }
        Ok(())
    }

    /// The producer's published item count (ring head).
    pub fn published(&self) -> u64 {
        self.ring.head()
    }
}

/// Consumer half. `recv()` is an `.await`-able future that resolves
/// when an item arrives, suspending the task until then - off-thread
/// across threads OR across processes, behind the same call.
pub struct ReactiveReceiver {
    ring: Arc<SpscRingCore>,
    slot: Arc<Mutex<Option<Waker>>>,
    /// Present only in cross-process mode; owns the reactor thread and
    /// stops it on drop.
    _reactor: Option<ReactorHandle>,
}

impl ReactiveReceiver {
    /// A future resolving to the next slot's bytes. Owns clones of the
    /// ring + waker slot, so it is `Send + 'static`.
    pub fn recv(&self) -> ReactiveRecv {
        ReactiveRecv {
            ring: Arc::clone(&self.ring),
            slot: Arc::clone(&self.slot),
        }
    }

    /// Non-blocking pop, for draining without awaiting.
    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.ring.try_pop(out)
    }
}

/// Future returned by [`ReactiveReceiver::recv`].
pub struct ReactiveRecv {
    ring: Arc<SpscRingCore>,
    slot: Arc<Mutex<Option<Waker>>>,
}

impl Future for ReactiveRecv {
    type Output = [u8; SPSC_PAYLOAD_BYTES];

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        if self.ring.try_pop(&mut out).is_ok() {
            return Poll::Ready(out);
        }
        // Register, then re-check: an item that landed between the first
        // pop and this registration is caught here, not lost.
        *self.slot.lock() = Some(cx.waker().clone());
        if self.ring.try_pop(&mut out).is_ok() {
            return Poll::Ready(out);
        }
        Poll::Pending
    }
}

/// Owns the reactor thread and stops it when the receiver drops.
struct ReactorHandle {
    shutdown: Arc<AtomicBool>,
    xwaker: Arc<CrossProcessWaker>,
    join: Option<JoinHandle<()>>,
}

impl Drop for ReactorHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // Unblock the reactor's wait() so it sees the shutdown flag.
        self.xwaker.wake_all();
        if let Some(j) = self.join.take() {
            j.join().ok();
        }
    }
}

/// Intra-process reactive channel: the sender fires the receiver's
/// `Waker` directly on push. No reactor thread. `capacity` must be a
/// power of two.
pub fn anon_pair(
    capacity: usize,
) -> Result<(ReactiveSender, ReactiveReceiver), RingError> {
    let ring = Arc::new(SpscRingCore::create_anon(capacity)?);
    let slot = Arc::new(Mutex::new(None));
    Ok((
        ReactiveSender {
            ring: Arc::clone(&ring),
            signal: SenderSignal::Local(Arc::clone(&slot)),
        },
        ReactiveReceiver { ring, slot, _reactor: None },
    ))
}

/// Cross-process producer half over a shared MMF ring + named waker.
/// The two processes share the same ring file and the same waker file.
pub fn sender_cross(
    ring: Arc<SpscRingCore>,
    xwaker: Arc<CrossProcessWaker>,
) -> ReactiveSender {
    ReactiveSender { ring, signal: SenderSignal::Cross(xwaker) }
}

/// Cross-process consumer half. Spawns a reactor thread that blocks on
/// the shared waker and fires the local `Waker` of the future parked on
/// the ring whenever the producer process publishes.
pub fn receiver_cross(
    ring: Arc<SpscRingCore>,
    xwaker: Arc<CrossProcessWaker>,
) -> ReactiveReceiver {
    let slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
    let shutdown = Arc::new(AtomicBool::new(false));

    let join = {
        let ring = Arc::clone(&ring);
        let xwaker = Arc::clone(&xwaker);
        let slot = Arc::clone(&slot);
        let shutdown = Arc::clone(&shutdown);
        std::thread::spawn(move || reactor_loop(ring, xwaker, slot, shutdown))
    };

    ReactiveReceiver {
        ring,
        slot,
        _reactor: Some(ReactorHandle {
            shutdown,
            xwaker,
            join: Some(join),
        }),
    }
}

/// The reactor: bridge cross-process publishes to the local `Waker`.
/// Blocks on the MMF waker; on every observed head advance fires the
/// parked future's `Waker` so the driver re-polls and pops.
fn reactor_loop(
    ring: Arc<SpscRingCore>,
    xwaker: Arc<CrossProcessWaker>,
    slot: Arc<Mutex<Option<Waker>>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut last = ring.head();
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let head = ring.head();
        if head != last {
            last = head;
            if let Some(w) = slot.lock().take() {
                w.wake();
            }
            continue;
        }
        // Park until the producer publishes past `head`.
        match xwaker.try_park(head + 1) {
            Ok(token) => {
                // Lost-wake guard: an item that landed (or a shutdown
                // that fired) between the head read and the park is
                // caught here.
                if shutdown.load(Ordering::Acquire) || ring.head() != head {
                    xwaker.release(token);
                    continue;
                }
                // Heal-bounded: a real cross-process wake (producer
                // publish) or the shutdown `wake_all` ends the wait fast;
                // the bounded tick is the backstop so a wake lost to the
                // register/visibility race self-heals at the loop top
                // (head re-check) instead of hanging the consumer.
                xwaker.wait(token, Some(REACTOR_HEAL_INTERVAL)).ok();
            }
            Err(crate::cross_process_waker::WakerError::Full) => {
                // No free waker slot; re-check shortly.
                std::hint::spin_loop();
            }
            Err(_) => break,
        }
    }
}

/// A bridge from an arbitrary monotonic published-seq source to a local
/// `Waker` slot, for channels whose backing is not an `SpscRingCore`
/// (e.g. a [`SharedRing`](crate::SharedRing)'s `producer_seq` /
/// `consumer_seq`). Same heal-bounded loop as [`reactor_loop`]; the
/// closure supplies the count. Stops its thread on drop.
pub(crate) struct SeqReactor {
    shutdown: Arc<AtomicBool>,
    xwaker: Arc<CrossProcessWaker>,
    join: Option<JoinHandle<()>>,
}

impl Drop for SeqReactor {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.xwaker.wake_all();
        if let Some(j) = self.join.take() {
            j.join().ok();
        }
    }
}

/// Spawn a reactor firing `slot` whenever `published()` advances,
/// parking on `xwaker` between observations.
pub(crate) fn spawn_seq_reactor(
    published: Arc<dyn Fn() -> u64 + Send + Sync>,
    xwaker: Arc<CrossProcessWaker>,
    slot: Arc<Mutex<Option<Waker>>>,
) -> SeqReactor {
    let shutdown = Arc::new(AtomicBool::new(false));
    let join = {
        let xwaker = Arc::clone(&xwaker);
        let shutdown = Arc::clone(&shutdown);
        std::thread::spawn(move || seq_reactor_loop(published, &xwaker, &slot, &shutdown))
    };
    SeqReactor { shutdown, xwaker, join: Some(join) }
}

fn seq_reactor_loop(
    published: Arc<dyn Fn() -> u64 + Send + Sync>,
    xwaker: &CrossProcessWaker,
    slot: &Mutex<Option<Waker>>,
    shutdown: &AtomicBool,
) {
    let mut last = published();
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let cur = published();
        if cur != last {
            last = cur;
            if let Some(w) = slot.lock().take() {
                w.wake();
            }
            continue;
        }
        match xwaker.try_park(cur + 1) {
            Ok(token) => {
                if shutdown.load(Ordering::Acquire) || published() != cur {
                    xwaker.release(token);
                    continue;
                }
                xwaker.wait(token, Some(REACTOR_HEAL_INTERVAL)).ok();
            }
            Err(crate::cross_process_waker::WakerError::Full) => {
                std::hint::spin_loop();
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    #[test]
    fn intra_process_block_on_parks_and_wakes_on_send() {
        // The driver thread parks between items; the sender's direct
        // Waker fire unparks it. Proves the recv future suspends rather
        // than spins, with no reactor thread in this locale.
        let (tx, rx) = anon_pair(8).unwrap();
        const N: u64 = 1000;

        let producer = thread::spawn(move || {
            for i in 0..N {
                let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                buf[..8].copy_from_slice(&i.to_le_bytes());
                while tx.try_send(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let sum = block_on(async move {
            let mut s = 0u64;
            for _ in 0..N {
                let item = rx.recv().await;
                s += u64::from_le_bytes(item[..8].try_into().unwrap());
            }
            s
        });

        producer.join().unwrap();
        assert_eq!(sum, (0..N).sum());
    }

    #[test]
    fn many_futures_one_local_signal() {
        // A second consumer task (driven on a worker) also wakes on the
        // same channel's sender fire; verifies the slot-register /
        // re-check path under interleaving.
        let (tx, rx) = anon_pair(4).unwrap();
        let got = Arc::new(AtomicU64::new(0));
        let got2 = Arc::clone(&got);

        let consumer = thread::spawn(move || {
            block_on(async move {
                for _ in 0..500u64 {
                    let item = rx.recv().await;
                    got2.fetch_add(
                        u64::from_le_bytes(item[..8].try_into().unwrap()),
                        Ordering::AcqRel,
                    );
                }
            });
        });

        for i in 0..500u64 {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..8].copy_from_slice(&i.to_le_bytes());
            while tx.try_send(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
        consumer.join().unwrap();
        assert_eq!(got.load(Ordering::Acquire), (0..500u64).sum());
    }
}
