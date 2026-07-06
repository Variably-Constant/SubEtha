//! Top-level user-facing IPC API.
//!
//! Wraps the [`MmfDispatcher`] family pick behind one type per
//! access pattern, so callers express WHAT they want (streaming,
//! work-stealing, key-value) and the dispatcher decides which
//! MMF-backed primitive to use under the hood. The shape of the
//! API mirrors `std::sync::mpsc::channel` but adds:
//!
//! - cross-process visibility via memory-mapped file backing
//! - kernel-bypass data path (atomic protocol layer in user space)
//! - per-workload routing to the empirically-best primitive
//!
//! Three intent types are exposed:
//!
//! - [`Channel<T>`]: streaming MPMC. Backed by [`SharedRing`].
//! - [`WorkStealQueue<T>`]: single-owner, multi-thief work-stealing.
//!   Backed by [`MmfDispatcher`]'s within-family pick (Chase-Lev /
//!   KHPD / LOH / URD / KHL / Fcl) for the deque family. Currently
//!   exposes the Chase-Lev `T: Marshal` surface; batched-fast deque
//!   variants are routed through internally for byte-slice payloads.
//! - [`KvMap<K, V>`]: key-value lookup. Backed by [`SharedHashMap`].
//!
//! ## Example
//!
//! ```no_run
//! use subetha_cxc::api::Channel;
//! use subetha_cxc::MmfWorkloadShape;
//!
//! let chan: Channel<u64> = Channel::create(
//!     "/tmp/my-channel.bin",
//!     MmfWorkloadShape::StreamingMpmc { n_producers: 4, n_consumers: 4 },
//!     1024,
//! ).expect("create channel");
//! chan.send(&42).expect("send");
//! let v = chan.recv().expect("recv");
//! assert_eq!(v, 42);
//! ```

#![allow(clippy::missing_errors_doc)]

use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use subetha_core::Marshal;

use crate::cross_process_waker::{CrossProcessWaker, WakerError, MAX_WAITERS_DEFAULT};
use crate::dispatch_deque::DequeVariant;
use crate::message_transport::TransportError;
use crate::mmf_dispatcher::{MmfDispatcher, MmfFamily, MmfWorkloadShape};
use crate::reactor::{spawn_seq_reactor, SeqReactor};
use crate::shared_deque::SharedDeque;
use crate::shared_hash_map::{InsertOutcome, MapError, SharedHashMap};
use crate::shared_ring::{RingError, SharedRing, PAYLOAD_BYTES};

/// Heal tick for a blocking wait with no caller deadline: a wake (the
/// common path) returns far sooner, the tick only backstops a wake lost
/// to the register/visibility race.
pub(crate) const BLOCKING_HEAL: Duration = Duration::from_millis(1);

/// Errors returned by the user-facing IPC types.
#[derive(Debug)]
pub enum ApiError {
    /// Underlying transport error (full / empty / etc.).
    Transport(TransportError),
    /// Marshal codec error.
    Marshal(subetha_core::MarshalError),
    /// I/O error during MMF setup.
    Io(std::io::Error),
    /// Key-value map error.
    Map(MapError),
    /// The workload shape resolved to a family this type cannot wrap.
    WrongFamily { wanted: &'static str, got: MmfFamily },
    /// Payload too large for the chosen transport's wire format.
    PayloadTooLarge,
    /// A blocking send / recv hit its deadline before the ring made
    /// progress.
    Timeout,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e:?}"),
            Self::Marshal(e) => write!(f, "marshal: {e:?}"),
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Map(e) => write!(f, "map: {e:?}"),
            Self::WrongFamily { wanted, got } => {
                write!(f, "wrong family: wanted {wanted}, got {got:?}")
            }
            Self::PayloadTooLarge => write!(f, "payload too large for transport"),
            Self::Timeout => write!(f, "blocking op timed out"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<subetha_core::MarshalError> for ApiError {
    fn from(e: subetha_core::MarshalError) -> Self {
        Self::Marshal(e)
    }
}
impl From<TransportError> for ApiError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}
impl From<MapError> for ApiError {
    fn from(e: MapError) -> Self {
        Self::Map(e)
    }
}
impl From<crate::shared_ring::RingError> for ApiError {
    fn from(e: crate::shared_ring::RingError) -> Self {
        match e {
            crate::shared_ring::RingError::Full => {
                ApiError::Transport(TransportError::Full)
            }
            crate::shared_ring::RingError::Empty => {
                ApiError::Transport(TransportError::Empty)
            }
            crate::shared_ring::RingError::PayloadTooLarge => {
                ApiError::Transport(TransportError::PayloadTooLarge)
            }
            _ => ApiError::Transport(TransportError::Other),
        }
    }
}

/// Streaming MPMC channel, backed by [`SharedRing`]. Use this for
/// arrival-order queues with multiple producers and multiple
/// consumers (the canonical request-fanout / result-fanin shape).
///
/// The dispatcher confirms `SharedRing` is the right family for the
/// caller's workload shape; if a different family is picked, the
/// constructor returns [`ApiError::WrongFamily`].
pub struct Channel<T: Marshal> {
    ring: Arc<SharedRing>,
    /// Producer fires on push; a blocking / awaiting recv waits on it.
    consumer_waker: Arc<CrossProcessWaker>,
    /// Consumer fires on pop; a blocking / awaiting send waits on it.
    producer_waker: Arc<CrossProcessWaker>,
    /// The awaiting consumer's `Waker` (fired directly in-process, or by
    /// the recv reactor cross-process).
    recv_slot: Arc<Mutex<Option<Waker>>>,
    /// The awaiting producer's `Waker`.
    send_slot: Arc<Mutex<Option<Waker>>>,
    /// Reactors spawned on first async use; bridge the MMF waker to the
    /// local slot when the peer is in another process.
    recv_reactor: OnceLock<SeqReactor>,
    send_reactor: OnceLock<SeqReactor>,
    /// Set once a recv / send blocks or awaits. Gates the wake signal so
    /// a pure-sync channel pays nothing for the async machinery.
    has_recv_waiter: AtomicBool,
    has_send_waiter: AtomicBool,
    family: MmfFamily,
    _phantom: PhantomData<T>,
}

fn waker_paths(base: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let mut cw = base.as_os_str().to_owned();
    cw.push(".cw");
    let mut pw = base.as_os_str().to_owned();
    pw.push(".pw");
    (std::path::PathBuf::from(cw), std::path::PathBuf::from(pw))
}

fn ring_err(e: RingError) -> ApiError {
    match e {
        RingError::Full => ApiError::Transport(TransportError::Full),
        RingError::Empty => ApiError::Transport(TransportError::Empty),
        RingError::PayloadTooLarge => ApiError::PayloadTooLarge,
        RingError::IoError(k) => ApiError::Io(std::io::Error::from(k)),
        _ => ApiError::Transport(TransportError::Other),
    }
}

impl<T: Marshal> Channel<T> {
    fn assemble(
        ring: SharedRing,
        consumer_waker: CrossProcessWaker,
        producer_waker: CrossProcessWaker,
        family: MmfFamily,
    ) -> Self {
        Self {
            ring: Arc::new(ring),
            consumer_waker: Arc::new(consumer_waker),
            producer_waker: Arc::new(producer_waker),
            recv_slot: Arc::new(Mutex::new(None)),
            send_slot: Arc::new(Mutex::new(None)),
            recv_reactor: OnceLock::new(),
            send_reactor: OnceLock::new(),
            has_recv_waiter: AtomicBool::new(false),
            has_send_waiter: AtomicBool::new(false),
            family,
            _phantom: PhantomData,
        }
    }

    // UFCS onto the concrete ring: `Arc<SharedRing>` also implements
    // `MessageTransport`, so `self.ring.try_push` would resolve to the
    // trait method (TransportError); these force the inherent
    // `SharedRing` methods (RingError) the blocking paths match on.
    #[inline]
    fn ring_push(&self, payload: &[u8]) -> Result<(), RingError> {
        SharedRing::try_push(&self.ring, payload)
    }

    #[inline]
    fn ring_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        SharedRing::try_pop(&self.ring, out)
    }

    /// Create a channel at `path` with the given workload shape.
    /// `capacity` is the ring slot count (rounded up to next pow2).
    /// Two small adjacent waker files (`.cw` / `.pw`) carry the
    /// blocking + async wakeups across processes.
    pub fn create(
        path: impl AsRef<Path>,
        shape: MmfWorkloadShape,
        capacity: usize,
    ) -> Result<Self, ApiError> {
        let family = MmfDispatcher::pick(shape);
        if family != MmfFamily::SharedRing {
            return Err(ApiError::WrongFamily {
                wanted: "SharedRing",
                got: family,
            });
        }
        if T::PAYLOAD_BYTES > PAYLOAD_BYTES {
            return Err(ApiError::PayloadTooLarge);
        }
        let (cw, pw) = waker_paths(path.as_ref());
        let ring = SharedRing::create(path.as_ref(), capacity)?;
        let consumer_waker = CrossProcessWaker::create(cw, MAX_WAITERS_DEFAULT)
            .map_err(map_waker)?;
        let producer_waker = CrossProcessWaker::create(pw, MAX_WAITERS_DEFAULT)
            .map_err(map_waker)?;
        Ok(Self::assemble(ring, consumer_waker, producer_waker, family))
    }

    /// Open an existing channel at `path`.
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self, ApiError> {
        if T::PAYLOAD_BYTES > PAYLOAD_BYTES {
            return Err(ApiError::PayloadTooLarge);
        }
        let (cw, pw) = waker_paths(path.as_ref());
        let ring = SharedRing::open(path.as_ref(), capacity)?;
        let consumer_waker = CrossProcessWaker::open(cw, MAX_WAITERS_DEFAULT)
            .map_err(map_waker)?;
        let producer_waker = CrossProcessWaker::open(pw, MAX_WAITERS_DEFAULT)
            .map_err(map_waker)?;
        Ok(Self::assemble(
            ring, consumer_waker, producer_waker, MmfFamily::SharedRing,
        ))
    }

    /// Wake whoever waits to RECEIVE: the awaiting task's `Waker` and
    /// any thread parked in `recv_blocking`. A pure-sync channel never
    /// trips `has_recv_waiter`, so this returns on one relaxed load.
    fn signal_consumer(&self) {
        if !self.has_recv_waiter.load(Ordering::Relaxed) {
            return;
        }
        if let Some(w) = self.recv_slot.lock().take() {
            w.wake();
        }
        self.consumer_waker.wake_up_to(self.ring.producer_seq());
    }

    /// Wake whoever waits to SEND.
    fn signal_producer(&self) {
        if !self.has_send_waiter.load(Ordering::Relaxed) {
            return;
        }
        if let Some(w) = self.send_slot.lock().take() {
            w.wake();
        }
        self.producer_waker.wake_up_to(self.ring.consumer_seq());
    }

    fn ensure_recv_reactor(&self) {
        self.recv_reactor.get_or_init(|| {
            let ring = Arc::clone(&self.ring);
            spawn_seq_reactor(
                Arc::new(move || ring.producer_seq()),
                Arc::clone(&self.consumer_waker),
                Arc::clone(&self.recv_slot),
            )
        });
    }

    fn ensure_send_reactor(&self) {
        self.send_reactor.get_or_init(|| {
            let ring = Arc::clone(&self.ring);
            spawn_seq_reactor(
                Arc::new(move || ring.consumer_seq()),
                Arc::clone(&self.producer_waker),
                Arc::clone(&self.send_slot),
            )
        });
    }

    fn marshal_buf(item: &T) -> ([u8; PAYLOAD_BYTES], usize) {
        let mut buf = [0u8; PAYLOAD_BYTES];
        item.marshal(&mut buf[..T::PAYLOAD_BYTES]);
        (buf, T::PAYLOAD_BYTES)
    }

    fn unmarshal_buf(buf: &[u8], n: usize) -> Result<T, ApiError> {
        Ok(T::unmarshal(&buf[..n.min(T::PAYLOAD_BYTES.max(1))])?)
    }

    /// Non-blocking send. `Err(Transport(Full))` when the ring is full.
    pub fn send(&self, item: &T) -> Result<(), ApiError> {
        let (buf, len) = Self::marshal_buf(item);
        self.ring_push(&buf[..len]).map_err(ring_err)?;
        self.signal_consumer();
        Ok(())
    }

    /// Non-blocking recv. `Err(Transport(Empty))` when the ring is empty.
    pub fn recv(&self) -> Result<T, ApiError> {
        let mut buf = [0u8; PAYLOAD_BYTES];
        let n = self.ring_pop(&mut buf).map_err(ring_err)?;
        self.signal_producer();
        Self::unmarshal_buf(&buf, n)
    }

    /// Blocking send: parks the calling thread until space frees up (or
    /// `timeout` elapses). `None` waits indefinitely.
    pub fn send_blocking(
        &self,
        item: &T,
        timeout: Option<Duration>,
    ) -> Result<(), ApiError> {
        self.has_send_waiter.store(true, Ordering::Relaxed);
        let (buf, len) = Self::marshal_buf(item);
        let deadline = timeout.map(|d| Instant::now() + d);
        loop {
            match self.ring_push(&buf[..len]) {
                Ok(()) => {
                    self.signal_consumer();
                    return Ok(());
                }
                Err(RingError::Full) => {}
                Err(e) => return Err(ring_err(e)),
            }
            let seen = self.ring.consumer_seq();
            let token = self.producer_waker.try_park(seen + 1).map_err(map_waker)?;
            // Re-attempt after registering: take space freed during the
            // park instead of sleeping over it.
            match self.ring_push(&buf[..len]) {
                Ok(()) => {
                    self.producer_waker.release(token);
                    self.signal_consumer();
                    return Ok(());
                }
                Err(RingError::Full) => {}
                Err(e) => {
                    self.producer_waker.release(token);
                    return Err(ring_err(e));
                }
            }
            match wait_heal(&self.producer_waker, token, deadline) {
                Ok(()) => continue,
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    /// Blocking recv: parks the calling thread until an item arrives (or
    /// `timeout` elapses). `None` waits indefinitely.
    pub fn recv_blocking(&self, timeout: Option<Duration>) -> Result<T, ApiError> {
        self.has_recv_waiter.store(true, Ordering::Relaxed);
        let deadline = timeout.map(|d| Instant::now() + d);
        let mut buf = [0u8; PAYLOAD_BYTES];
        loop {
            match self.ring_pop(&mut buf) {
                Ok(n) => {
                    self.signal_producer();
                    return Self::unmarshal_buf(&buf, n);
                }
                Err(RingError::Empty) => {}
                Err(e) => return Err(ring_err(e)),
            }
            let seen = self.ring.producer_seq();
            let token = self.consumer_waker.try_park(seen + 1).map_err(map_waker)?;
            match self.ring_pop(&mut buf) {
                Ok(n) => {
                    self.consumer_waker.release(token);
                    self.signal_producer();
                    return Self::unmarshal_buf(&buf, n);
                }
                Err(RingError::Empty) => {}
                Err(e) => {
                    self.consumer_waker.release(token);
                    return Err(ring_err(e));
                }
            }
            match wait_heal(&self.consumer_waker, token, deadline) {
                Ok(()) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Async recv. Resolves to the next item, suspending the task while
    /// the ring is empty. Spawns a recv reactor on first call so the
    /// wake bridges across processes.
    pub fn recv_async(&self) -> RecvFut<'_, T> {
        self.has_recv_waiter.store(true, Ordering::Relaxed);
        self.ensure_recv_reactor();
        RecvFut { chan: self }
    }

    /// Async send. Resolves once the item is in the ring, suspending the
    /// task while it is full.
    pub fn send_async(&self, item: &T) -> SendFut<'_, T> {
        self.has_send_waiter.store(true, Ordering::Relaxed);
        self.ensure_send_reactor();
        let (buf, len) = Self::marshal_buf(item);
        SendFut { chan: self, buf, len }
    }

    /// The family the dispatcher picked at construction.
    pub fn family(&self) -> MmfFamily {
        self.family
    }
}

pub(crate) fn map_waker(e: WakerError) -> ApiError {
    match e {
        WakerError::Timeout => ApiError::Timeout,
        WakerError::IoError(k) => ApiError::Io(std::io::Error::from(k)),
        _ => ApiError::Transport(TransportError::Other),
    }
}

/// Heal-bounded wait: a real wake (the common path) ends it fast; an
/// unbounded caller still re-checks the ring on each tick, so a lost
/// wake self-heals instead of hanging.
pub(crate) fn wait_heal(
    waker: &CrossProcessWaker,
    token: crate::cross_process_waker::WakerToken,
    deadline: Option<Instant>,
) -> Result<(), ApiError> {
    let wait_for = match deadline {
        None => BLOCKING_HEAL,
        Some(d) => {
            let now = Instant::now();
            if now >= d {
                waker.release(token);
                return Err(ApiError::Timeout);
            }
            (d - now).min(BLOCKING_HEAL)
        }
    };
    match waker.wait(token, Some(wait_for)) {
        Ok(()) | Err(WakerError::Timeout) => Ok(()),
        Err(e) => Err(map_waker(e)),
    }
}

/// Future from [`Channel::recv_async`].
pub struct RecvFut<'a, T: Marshal> {
    chan: &'a Channel<T>,
}

impl<'a, T: Marshal> Future for RecvFut<'a, T> {
    type Output = Result<T, ApiError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let c = self.chan;
        let mut buf = [0u8; PAYLOAD_BYTES];
        if let Ok(n) = c.ring_pop(&mut buf) {
            c.signal_producer();
            return Poll::Ready(Channel::<T>::unmarshal_buf(&buf, n));
        }
        *c.recv_slot.lock() = Some(cx.waker().clone());
        if let Ok(n) = c.ring_pop(&mut buf) {
            c.signal_producer();
            return Poll::Ready(Channel::<T>::unmarshal_buf(&buf, n));
        }
        Poll::Pending
    }
}

/// Future from [`Channel::send_async`].
pub struct SendFut<'a, T: Marshal> {
    chan: &'a Channel<T>,
    buf: [u8; PAYLOAD_BYTES],
    len: usize,
}

impl<'a, T: Marshal> Future for SendFut<'a, T> {
    type Output = Result<(), ApiError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.chan.ring_push(&this.buf[..this.len]).is_ok() {
            this.chan.signal_consumer();
            return Poll::Ready(Ok(()));
        }
        *this.chan.send_slot.lock() = Some(cx.waker().clone());
        if this.chan.ring_push(&this.buf[..this.len]).is_ok() {
            this.chan.signal_consumer();
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }
}

/// Single-owner, multi-thief work-stealing queue, backed by the
/// [`SharedDeque<T>`](crate::SharedDeque) family. The owner pushes
/// via [`push`](Self::push); thieves drain via [`steal`](Self::steal).
///
/// The dispatcher's variant pick for the caller's workload shape is
/// reported via [`variant`](Self::variant); the generic-`T: Marshal`
/// surface uses Chase-Lev as the backing because it is the only
/// variant generic over arbitrary `T: Marshal`. Byte-slice
/// workloads (e.g. scheduler `PassSlot`) can ride the higher-
/// throughput KHL/KHPD/LOH/URD variants directly via the
/// [`DequeDispatcher`](crate::DequeDispatcher) API.
pub struct WorkStealQueue<T: Marshal + Copy + 'static> {
    owner: Arc<SharedDeque<T>>,
    variant: DequeVariant,
}

impl<T: Marshal + Copy + 'static> WorkStealQueue<T> {
    /// Create the queue at `path` with the given workload shape.
    /// `capacity` is rounded up to next pow2.
    pub fn create(
        path: impl AsRef<Path>,
        shape: MmfWorkloadShape,
        capacity: usize,
    ) -> Result<Self, ApiError> {
        let family = MmfDispatcher::pick(shape);
        let variant = match family {
            MmfFamily::SharedDeque(v) => v,
            other => {
                return Err(ApiError::WrongFamily {
                    wanted: "SharedDeque",
                    got: other,
                });
            }
        };
        let owner = SharedDeque::<T>::create(path.as_ref(), capacity)?;
        Ok(Self {
            owner: Arc::new(owner),
            variant,
        })
    }

    /// Open an existing queue at `path` as a thief.
    pub fn open_as_thief(path: impl AsRef<Path>) -> Result<Self, ApiError> {
        let thief = SharedDeque::<T>::open_as_thief(path.as_ref())?;
        Ok(Self {
            owner: Arc::new(thief),
            variant: DequeVariant::ChaseLev,
        })
    }

    /// Owner-side push.
    pub fn push(&self, item: &T) -> Result<(), ApiError> {
        self.owner.push(item)?;
        Ok(())
    }

    /// Owner-side pop (LIFO end).
    pub fn pop(&self) -> Option<T> {
        self.owner.pop()
    }

    /// Thief-side steal (FIFO end).
    pub fn steal(&self) -> Option<T> {
        self.owner.steal()
    }

    /// The variant the dispatcher picked for this workload.
    pub fn variant(&self) -> DequeVariant {
        self.variant
    }
}

impl From<crate::shared_deque::DequeError> for ApiError {
    fn from(e: crate::shared_deque::DequeError) -> Self {
        match e {
            crate::shared_deque::DequeError::Full => {
                ApiError::Transport(TransportError::Full)
            }
            _ => ApiError::Transport(TransportError::Other),
        }
    }
}

/// Key-value lookup map, backed by [`SharedHashMap`]. Multiple
/// processes can insert + look up concurrently via the shared MMF.
///
/// The dispatcher confirms `SharedHashMap` is the right family;
/// if a different family is picked, the constructor returns
/// [`ApiError::WrongFamily`].
pub struct KvMap<K: Copy + Eq + Send + Sync + 'static, V: Copy + Send + Sync + 'static> {
    map: Arc<SharedHashMap<K, V>>,
}

impl<K, V> KvMap<K, V>
where
    K: Copy + Eq + std::hash::Hash + Send + Sync + 'static,
    V: Copy + Send + Sync + 'static,
{
    /// Create the map at `path` with the given workload shape.
    /// `capacity` is rounded up to next pow2.
    pub fn create(
        path: impl AsRef<Path>,
        shape: MmfWorkloadShape,
        capacity: usize,
    ) -> Result<Self, ApiError> {
        let family = MmfDispatcher::pick(shape);
        if family != MmfFamily::SharedHashMap {
            return Err(ApiError::WrongFamily {
                wanted: "SharedHashMap",
                got: family,
            });
        }
        let map = SharedHashMap::<K, V>::create(path.as_ref(), capacity)?;
        Ok(Self { map: Arc::new(map) })
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: K, value: V) -> Result<InsertOutcome, ApiError> {
        let outcome = self.map.insert(key, value)?;
        Ok(outcome)
    }

    /// Look up a value by key.
    pub fn get(&self, key: &K) -> Option<V> {
        self.map.get(key)
    }

    /// Current number of occupied slots.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// `true` if the map has no occupied slots.
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

/// Builder for an auto-inferred IPC endpoint. The caller describes
/// the workload with declarative hints; the builder infers the
/// `MmfWorkloadShape`, asks [`MmfDispatcher`] for the family, and
/// constructs the right typed-intent wrapper.
///
/// No workload shape, no family enum, no primitive choice ever
/// touches the user. They write:
///
/// ```no_run
/// use subetha_cxc::AutoIpc;
///
/// let auto = AutoIpc::new("/tmp/auto-ipc.bin")
///     .producers(4)
///     .consumers(4)
///     .batch_size(64)
///     .capacity(1024)
///     .build_channel::<u64>()
///     .expect("create");
/// auto.send(&42).expect("send");
/// ```
///
/// The builder picks streaming MPMC when there are multiple
/// producers / consumers without a single-owner constraint;
/// work-stealing when there is one producer and multiple consumers
/// with a batch hint; key-value when the caller selects
/// `build_kv_map`. The inference is zero-cost: it runs once at
/// `build_*`, never per-op.
pub struct AutoIpc {
    path: std::path::PathBuf,
    n_producers: usize,
    n_consumers: usize,
    batch_size: Option<usize>,
    wait_idle: bool,
    capacity: usize,
    ordering: crate::qos_policy::Ordering,
    auto_order: Option<f64>,
}

impl AutoIpc {
    /// Start a new auto-inferred IPC endpoint at `path`.
    /// Defaults: 1 producer, 1 consumer, no batch, capacity 64,
    /// per-producer ordering, no auto-order threshold.
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            n_producers: 1,
            n_consumers: 1,
            batch_size: None,
            wait_idle: false,
            capacity: 64,
            ordering: crate::qos_policy::Ordering::PerProducer,
            auto_order: None,
        }
    }

    /// Number of producers expected to push concurrently.
    pub fn producers(mut self, n: usize) -> Self {
        self.n_producers = n.max(1);
        self
    }

    /// Number of consumers expected to drain concurrently.
    pub fn consumers(mut self, n: usize) -> Self {
        self.n_consumers = n.max(1);
        self
    }

    /// Hint that the producer will publish batches of `k` items.
    /// Setting this is what flips a single-producer streaming
    /// workload into work-stealing routing.
    pub fn batch_size(mut self, k: usize) -> Self {
        self.batch_size = Some(k);
        self
    }

    /// Hint that consumers should idle-wait between batches
    /// (WAITPKG on capable silicon; PAUSE-spin otherwise).
    pub fn idle_wait(mut self, on: bool) -> Self {
        self.wait_idle = on;
        self
    }

    /// Ring slot capacity (rounded to next pow2).
    pub fn capacity(mut self, n: usize) -> Self {
        self.capacity = n.max(2);
        self
    }

    /// Declare the ordering requirement. `GlobalFifo` constrains
    /// the inference to the streaming family (a work-stealing
    /// deque's LIFO owner end cannot honor FIFO at all), and
    /// [`build_adaptive`](Self::build_adaptive) applies the
    /// declaration to the stamped ring's merge flag.
    pub fn ordering(mut self, ordering: crate::qos_policy::Ordering) -> Self {
        self.ordering = ordering;
        self
    }

    /// Pre-authorize an automatic ordering response: when the
    /// built endpoint observes more than `threshold` cross-producer
    /// inversions per second, its sidecar arms global-FIFO delivery
    /// (the stamped merge) without a further declaration. Effective
    /// through [`build_adaptive`](Self::build_adaptive), which
    /// constructs the stamped ring the response needs.
    pub fn auto_order(mut self, threshold: f64) -> Self {
        self.auto_order = Some(threshold);
        self
    }

    /// Infer the workload shape from the declared hints.
    pub fn inferred_shape(&self) -> MmfWorkloadShape {
        // GlobalFifo pins the inference to the streaming family:
        // deques cannot honor cross-producer FIFO.
        if self.ordering == crate::qos_policy::Ordering::GlobalFifo {
            return MmfWorkloadShape::StreamingMpmc {
                n_producers: self.n_producers,
                n_consumers: self.n_consumers,
            };
        }
        // n_producers >= 2 OR n_consumers >= 2 with no batch +
        // streaming intent -> streaming MPMC.
        // single-producer + batch_size hint -> work-stealing.
        // wait_idle -> work-stealing (URD).
        if self.batch_size.is_some() || self.wait_idle {
            MmfWorkloadShape::WorkStealing(
                crate::dispatch_deque::WorkloadShape {
                    n_thieves: self.n_consumers,
                    batch_size: self.batch_size,
                    wait_idle: self.wait_idle,
                },
            )
        } else if self.n_producers >= 2 || self.n_consumers >= 2 {
            MmfWorkloadShape::StreamingMpmc {
                n_producers: self.n_producers,
                n_consumers: self.n_consumers,
            }
        } else {
            // Single producer, single consumer, no batch -> degenerate
            // streaming case (one-to-one queue). SharedRing handles it.
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 1,
                n_consumers: 1,
            }
        }
    }

    /// Inferred family pick (informational; no construction).
    pub fn inferred_family(&self) -> MmfFamily {
        MmfDispatcher::pick(self.inferred_shape())
    }

    /// Build a streaming MPMC channel for `T: Marshal`. Returns
    /// `WrongFamily` if the inferred shape resolves to something
    /// other than `SharedRing` (e.g. you set `batch_size` and the
    /// inference picked work-stealing).
    pub fn build_channel<T: Marshal>(self) -> Result<Channel<T>, ApiError> {
        let shape = self.inferred_shape();
        Channel::<T>::create(&self.path, shape, self.capacity)
    }

    /// Build a work-stealing queue. Returns `WrongFamily` if the
    /// inferred shape is not work-stealing (call `batch_size` to
    /// force work-stealing inference) or if `GlobalFifo` ordering
    /// was declared (a deque's LIFO owner end cannot honor FIFO).
    pub fn build_work_steal_queue<T: Marshal + Copy + 'static>(
        self,
    ) -> Result<WorkStealQueue<T>, ApiError> {
        if self.ordering == crate::qos_policy::Ordering::GlobalFifo {
            return Err(ApiError::WrongFamily {
                wanted: "SharedRing (GlobalFifo ordering declared)",
                got: MmfFamily::SharedDeque(
                    crate::dispatch_deque::DequeVariant::ChaseLev,
                ),
            });
        }
        // Force work-stealing inference when this method is called.
        let shape = MmfWorkloadShape::WorkStealing(
            crate::dispatch_deque::WorkloadShape {
                n_thieves: self.n_consumers,
                batch_size: self.batch_size,
                wait_idle: self.wait_idle,
            },
        );
        WorkStealQueue::<T>::create(&self.path, shape, self.capacity)
    }

    /// Build an [`AdaptiveIpc`](crate::AdaptiveIpc) endpoint with
    /// the ordering axis wired through: the inner ring carries push
    /// stamps, the [`ordering`](Self::ordering) declaration is
    /// applied at construction (GlobalFifo = stamped merge ON), and
    /// an [`auto_order`](Self::auto_order) threshold pre-authorizes
    /// the sidecar's automatic arm on observed inversion rate.
    pub fn build_adaptive<T: Marshal + Copy + 'static>(
        self,
    ) -> Result<crate::AdaptiveIpc<T>, ApiError> {
        let shape = self.inferred_shape();
        crate::AdaptiveIpc::<T>::create_with_ordering(
            &self.path,
            shape,
            self.capacity,
            self.n_consumers,
            self.ordering,
            self.auto_order,
        )
    }

    /// Build a key-value map. The caller declares key-value intent
    /// by calling this method (key-value access doesn't share
    /// signature axes with streaming / work-stealing).
    pub fn build_kv_map<K, V>(self) -> Result<KvMap<K, V>, ApiError>
    where
        K: Copy + Eq + std::hash::Hash + Send + Sync + 'static,
        V: Copy + Send + Sync + 'static,
    {
        let shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: self.n_consumers,
            n_writers: self.n_producers,
        };
        KvMap::<K, V>::create(&self.path, shape, self.capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch_deque::WorkloadShape;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subetha_api_{pid}_{nonce}_{name}.bin"));
        p
    }

    // A tiny Marshal type for the channel tests.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct U32Item(u32);

    unsafe impl Marshal for U32Item {
        const PAYLOAD_BYTES: usize = 4;
        fn marshal(&self, dst: &mut [u8]) {
            dst[..4].copy_from_slice(&self.0.to_le_bytes());
        }
        fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
            if src.len() < 4 {
                return Err(subetha_core::MarshalError::ShortBuffer {
                    expected: 4,
                    got: src.len(),
                });
            }
            Ok(U32Item(u32::from_le_bytes(src[..4].try_into().unwrap())))
        }
    }

    #[test]
    fn channel_round_trips_via_streaming_shape() {
        let path = tmp("channel");
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let chan: Channel<U32Item> = Channel::create(&path, shape, 64).expect("create");
        assert_eq!(chan.family(), MmfFamily::SharedRing);
        chan.send(&U32Item(42)).expect("send");
        let v = chan.recv().expect("recv");
        assert_eq!(v, U32Item(42));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn channel_rejects_wrong_family() {
        let path = tmp("channel_wrong_family");
        let bad_shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: 1,
            n_writers: 1,
        };
        let result = Channel::<U32Item>::create(&path, bad_shape, 64);
        match result {
            Err(ApiError::WrongFamily {
                wanted: "SharedRing",
                got: MmfFamily::SharedHashMap,
            }) => {}
            Err(other) => panic!("expected WrongFamily, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn work_steal_queue_round_trips_via_request_reply_shape() {
        let path = tmp("wsq");
        let shape = MmfWorkloadShape::WorkStealing(WorkloadShape::request_reply());
        let q: WorkStealQueue<u64> = WorkStealQueue::create(&path, shape, 64).expect("create");
        // request_reply -> ChaseLev (per-item).
        assert_eq!(q.variant(), DequeVariant::ChaseLev);
        q.push(&100).expect("push");
        q.push(&200).expect("push");
        // pop is LIFO end (owner) -> 200 first.
        assert_eq!(q.pop(), Some(200));
        // steal is FIFO end (thief) -> 100 next.
        assert_eq!(q.steal(), Some(100));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn kv_map_round_trips_via_key_value_shape() {
        let path = tmp("kv");
        let shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: 1,
            n_writers: 1,
        };
        let map: KvMap<u32, u32> = KvMap::create(&path, shape, 64).expect("create");
        for k in 0..10u32 {
            map.insert(k, k * k).expect("insert");
        }
        for k in 0..10u32 {
            assert_eq!(map.get(&k), Some(k * k));
        }
        assert_eq!(map.len(), 10);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn auto_ipc_default_infers_streaming_one_to_one() {
        let auto = AutoIpc::new("/tmp/test-default.bin");
        let shape = auto.inferred_shape();
        assert!(matches!(shape, MmfWorkloadShape::StreamingMpmc { .. }));
        assert_eq!(auto.inferred_family(), MmfFamily::SharedRing);
    }

    #[test]
    fn auto_ipc_multi_producer_infers_streaming_mpmc() {
        let auto = AutoIpc::new("/tmp/test-mp.bin").producers(4).consumers(4);
        assert_eq!(auto.inferred_family(), MmfFamily::SharedRing);
    }

    #[test]
    fn auto_ipc_batch_hint_flips_to_work_stealing() {
        let auto = AutoIpc::new("/tmp/test-batch.bin").batch_size(64);
        let shape = auto.inferred_shape();
        assert!(matches!(shape, MmfWorkloadShape::WorkStealing(_)));
        // Single-thief batched -> KHL.
        assert_eq!(
            auto.inferred_family(),
            MmfFamily::SharedDeque(DequeVariant::Khl)
        );
    }

    #[test]
    fn auto_ipc_multi_consumer_plus_batch_infers_urd() {
        let auto = AutoIpc::new("/tmp/test-mt.bin")
            .consumers(4)
            .batch_size(64);
        assert_eq!(
            auto.inferred_family(),
            MmfFamily::SharedDeque(DequeVariant::Urd)
        );
    }

    #[test]
    fn auto_ipc_idle_wait_routes_to_urd() {
        let auto = AutoIpc::new("/tmp/test-idle.bin").idle_wait(true);
        assert_eq!(
            auto.inferred_family(),
            MmfFamily::SharedDeque(DequeVariant::Urd)
        );
    }

    #[test]
    fn auto_ipc_build_channel_end_to_end_round_trip() {
        let path = tmp("auto_ch");
        let auto = AutoIpc::new(&path).capacity(64);
        let chan: Channel<U32Item> = auto.build_channel().expect("build");
        chan.send(&U32Item(123)).expect("send");
        let v = chan.recv().expect("recv");
        assert_eq!(v, U32Item(123));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn auto_ipc_build_work_steal_queue_with_batch_hint() {
        let path = tmp("auto_wsq");
        let q: WorkStealQueue<u64> = AutoIpc::new(&path)
            .batch_size(8)
            .capacity(64)
            .build_work_steal_queue()
            .expect("build");
        q.push(&10).expect("push");
        q.push(&20).expect("push");
        assert_eq!(q.pop(), Some(20));
        assert_eq!(q.steal(), Some(10));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn auto_ipc_build_kv_map() {
        let path = tmp("auto_kv");
        let map: KvMap<u32, u32> = AutoIpc::new(&path)
            .capacity(64)
            .build_kv_map()
            .expect("build");
        map.insert(7, 49).expect("insert");
        assert_eq!(map.get(&7), Some(49));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn auto_ipc_global_fifo_forces_streaming_inference() {
        // A batch hint normally flips the inference to work-stealing;
        // the GlobalFifo declaration overrides it (deques cannot
        // honor cross-producer FIFO).
        let auto = AutoIpc::new("/tmp/test-fifo.bin")
            .producers(4)
            .batch_size(64)
            .ordering(crate::qos_policy::Ordering::GlobalFifo);
        assert!(matches!(
            auto.inferred_shape(),
            MmfWorkloadShape::StreamingMpmc { .. }
        ));
        assert_eq!(auto.inferred_family(), MmfFamily::SharedRing);
    }

    #[test]
    fn auto_ipc_global_fifo_rejects_work_steal_queue() {
        let path = tmp("fifo_wsq");
        let result = AutoIpc::new(&path)
            .batch_size(8)
            .ordering(crate::qos_policy::Ordering::GlobalFifo)
            .build_work_steal_queue::<u64>();
        assert!(matches!(result, Err(ApiError::WrongFamily { .. })),
                "GlobalFifo + work-stealing must be rejected, got Ok or wrong error");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn auto_ipc_build_adaptive_with_ordering_round_trips() {
        let path = tmp("auto_adaptive");
        let ipc = AutoIpc::new(&path)
            .capacity(64)
            .ordering(crate::qos_policy::Ordering::GlobalFifo)
            .build_adaptive::<u64>()
            .expect("build");
        assert!(ipc.ring_handle().is_stamped(),
                "build_adaptive must construct the stamped ring");
        assert_eq!(ipc.ordering(), crate::qos_policy::Ordering::GlobalFifo);
        ipc.send(&31337).expect("send");
        assert_eq!(ipc.recv().expect("recv"), 31337);
    }

    #[test]
    fn auto_ipc_auto_order_threshold_reaches_adaptive_endpoint() {
        let path = tmp("auto_threshold");
        let ipc = AutoIpc::new(&path)
            .capacity(64)
            .auto_order(5.0)
            .build_adaptive::<u64>()
            .expect("build");
        assert!(ipc.ring_handle().is_stamped(),
                "auto_order requires the stamped ring and build_adaptive must provide it");
        assert_eq!(ipc.ordering(), crate::qos_policy::Ordering::PerProducer,
                   "auto_order alone must not pre-arm the merge");
    }

    #[test]
    fn kv_map_rejects_streaming_shape() {
        let path = tmp("kv_wrong");
        let bad_shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let result = KvMap::<u32, u32>::create(&path, bad_shape, 64);
        match result {
            Err(ApiError::WrongFamily {
                wanted: "SharedHashMap",
                got: MmfFamily::SharedRing,
            }) => {}
            Err(other) => panic!("expected WrongFamily, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        std::fs::remove_file(&path).ok();
    }
}
