//! `BlockingMpscRing`: composed-SPSC MPSC fan-in with cross-process
//! futex-shaped `send_blocking` / `recv_blocking`.
//!
//! Wraps [`crate::mpsc_ring::SharedRingMpsc`] (N independent Lamport
//! SPSC rings, one per producer) with one [`CrossProcessWaker`] per
//! ring on the producer side plus one shared consumer waker.
//!
//! Wake routing:
//! - Each producer parks on its own ring's `producer_waker[i]` when
//!   the ring is full. The consumer wakes that specific waker after
//!   popping from ring `i` so only the producer who was actually
//!   blocked on ring `i` runs.
//! - The consumer parks on a single shared `consumer_waker` when
//!   every ring is empty. Any producer that pushes wakes that
//!   single waker by advancing a shared `total_published` counter.
//!
//! See [`crate::cross_process_waker`] for the wake protocol +
//! storage layout. See [`crate::blocking_spsc_ring::BlockingSpscRing`]
//! for the simpler 1P/1C shape.

use std::cell::Cell;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::blocking_spsc_ring::BlockingError;
use crate::cross_process_waker::{
    CrossProcessWaker, MAX_WAITERS_DEFAULT, WakerError,
};
use crate::shared_ring::RingError;
use crate::spsc_ring::SpscRingCore;

const PRE_PARK_SPIN: u32 = 32;

/// Factory for an MPSC pool of N SPSC rings wired into the
/// cross-process waker primitive.
pub struct BlockingMpscRing;

/// A single producer handle. Sole writer to one underlying SPSC
/// ring. `!Sync + !Clone + Send`: one producer per thread.
pub struct BlockingMpscProducer {
    ring: Arc<SpscRingCore>,
    own_waker: Arc<CrossProcessWaker>,
    consumer_waker: Arc<CrossProcessWaker>,
    total_published: Arc<AtomicU64>,
    _not_sync: PhantomData<Cell<()>>,
}

/// The single consumer handle. Drains all N producer rings
/// round-robin. `!Sync + !Clone + Send`.
pub struct BlockingMpscConsumer {
    rings: Vec<Arc<SpscRingCore>>,
    producer_wakers: Vec<Arc<CrossProcessWaker>>,
    consumer_waker: Arc<CrossProcessWaker>,
    total_published: Arc<AtomicU64>,
    next_drain: AtomicUsize,
    _not_sync: PhantomData<Cell<()>>,
}

impl BlockingMpscRing {
    /// In-process pool: N producer rings + per-ring producer wakers
    /// + one consumer waker, all anon-mapped.
    pub fn create_anon_pool(
        n_producers: usize,
        capacity: usize,
    ) -> Result<(Vec<BlockingMpscProducer>, BlockingMpscConsumer), BlockingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        let mut producer_wakers: Vec<Arc<CrossProcessWaker>> =
            Vec::with_capacity(n_producers);
        for _ in 0..n_producers {
            rings.push(Arc::new(
                SpscRingCore::create_anon(capacity).map_err(BlockingError::from)?,
            ));
            producer_wakers.push(Arc::new(
                CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
        }
        let consumer_waker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT)
                .map_err(BlockingError::from)?,
        );
        let total_published = Arc::new(AtomicU64::new(0));
        Ok(build_pool(
            rings,
            producer_wakers,
            consumer_waker,
            total_published,
        ))
    }

    /// File-backed pool. Path layout:
    ///   `<prefix>.ring.{i}.bin`  - SPSC ring for producer `i`
    ///   `<prefix>.pw.{i}.bin`    - producer-side waker for ring `i`
    ///   `<prefix>.cw.bin`        - shared consumer waker
    pub fn create_pool(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        capacity: usize,
    ) -> Result<(Vec<BlockingMpscProducer>, BlockingMpscConsumer), BlockingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let base = path_prefix.as_ref().to_path_buf();
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        let mut producer_wakers: Vec<Arc<CrossProcessWaker>> =
            Vec::with_capacity(n_producers);
        for i in 0..n_producers {
            rings.push(Arc::new(
                SpscRingCore::create(ring_path(&base, i), capacity)
                    .map_err(BlockingError::from)?,
            ));
            producer_wakers.push(Arc::new(
                CrossProcessWaker::create(pw_path(&base, i), MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
        }
        let consumer_waker = Arc::new(
            CrossProcessWaker::create(cw_path(&base), MAX_WAITERS_DEFAULT)
                .map_err(BlockingError::from)?,
        );
        let total_published = Arc::new(AtomicU64::new(0));
        Ok(build_pool(
            rings,
            producer_wakers,
            consumer_waker,
            total_published,
        ))
    }

    /// Open an existing file-backed pool. Caller passes the same
    /// `path_prefix`, `n_producers`, and `capacity` the pool was
    /// created with.
    pub fn open_pool(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        expected_capacity: usize,
    ) -> Result<(Vec<BlockingMpscProducer>, BlockingMpscConsumer), BlockingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let base = path_prefix.as_ref().to_path_buf();
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        let mut producer_wakers: Vec<Arc<CrossProcessWaker>> =
            Vec::with_capacity(n_producers);
        for i in 0..n_producers {
            rings.push(Arc::new(
                SpscRingCore::open(ring_path(&base, i), expected_capacity)
                    .map_err(BlockingError::from)?,
            ));
            producer_wakers.push(Arc::new(
                CrossProcessWaker::open(pw_path(&base, i), MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
        }
        let consumer_waker = Arc::new(
            CrossProcessWaker::open(cw_path(&base), MAX_WAITERS_DEFAULT)
                .map_err(BlockingError::from)?,
        );
        let total_published = Arc::new(AtomicU64::new(0));
        Ok(build_pool(
            rings,
            producer_wakers,
            consumer_waker,
            total_published,
        ))
    }
}

fn ring_path(base: &Path, i: usize) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".ring.{i}.bin"));
    PathBuf::from(s)
}

fn pw_path(base: &Path, i: usize) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".pw.{i}.bin"));
    PathBuf::from(s)
}

fn cw_path(base: &Path) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(".cw.bin");
    PathBuf::from(s)
}

fn build_pool(
    rings: Vec<Arc<SpscRingCore>>,
    producer_wakers: Vec<Arc<CrossProcessWaker>>,
    consumer_waker: Arc<CrossProcessWaker>,
    total_published: Arc<AtomicU64>,
) -> (Vec<BlockingMpscProducer>, BlockingMpscConsumer) {
    let producers: Vec<BlockingMpscProducer> = rings
        .iter()
        .zip(producer_wakers.iter())
        .map(|(r, pw)| BlockingMpscProducer {
            ring: Arc::clone(r),
            own_waker: Arc::clone(pw),
            consumer_waker: Arc::clone(&consumer_waker),
            total_published: Arc::clone(&total_published),
            _not_sync: PhantomData,
        })
        .collect();
    let consumer = BlockingMpscConsumer {
        rings,
        producer_wakers,
        consumer_waker,
        total_published,
        next_drain: AtomicUsize::new(0),
        _not_sync: PhantomData,
    };
    (producers, consumer)
}

impl BlockingMpscProducer {
    /// Non-blocking push. On success, increments the shared
    /// `total_published` counter and fires a wake at the consumer
    /// waker (cheap if no parked consumer).
    #[inline]
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        let r = self.ring.try_push(payload);
        if r.is_ok() {
            let new_seq = self.total_published.fetch_add(1, Ordering::Release) + 1;
            self.consumer_waker.wake_up_to(new_seq);
        }
        r
    }

    /// Block until either push succeeds or `timeout` elapses.
    /// Producer parks on its OWN ring's waker; the consumer's
    /// pop-side wakes the right ring's waker by `try_pop_ring_blocking`.
    pub fn send_blocking(
        &self,
        payload: &[u8],
        timeout: Option<Duration>,
    ) -> Result<(), BlockingError> {
        let deadline = timeout.map(|d| Instant::now() + d);
        loop {
            match self.try_push(payload) {
                Ok(()) => return Ok(()),
                Err(RingError::Full) => {}
                Err(e) => return Err(BlockingError::Ring(e)),
            }
            for _ in 0..PRE_PARK_SPIN {
                if self.ring.try_push(payload).is_ok() {
                    let new_seq = self.total_published.fetch_add(1, Ordering::Release) + 1;
                    self.consumer_waker.wake_up_to(new_seq);
                    return Ok(());
                }
                std::hint::spin_loop();
            }
            let target = self.ring.tail() + 1;
            let token = self.own_waker.try_park(target)?;
            // Wake-before-park recovery.
            if self.ring.try_push(payload).is_ok() {
                self.own_waker.release(token);
                let new_seq = self.total_published.fetch_add(1, Ordering::Release) + 1;
                self.consumer_waker.wake_up_to(new_seq);
                return Ok(());
            }
            let remaining = match deadline {
                None => None,
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        self.own_waker.release(token);
                        return Err(BlockingError::Timeout);
                    }
                    Some(d - now)
                }
            };
            match self.own_waker.wait(token, remaining) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingError::Timeout),
                Err(e) => return Err(BlockingError::from(e)),
            }
        }
    }

    /// This producer's ring capacity (constant).
    pub fn capacity(&self) -> usize { self.ring.capacity() }
    /// This producer's own publish head.
    pub fn head(&self) -> u64 { self.ring.head() }
}

impl BlockingMpscConsumer {
    /// Non-blocking pop, round-robin across all N rings.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        let n = self.rings.len();
        let start = self.next_drain.load(Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(bytes) = self.rings[idx].try_pop(out) {
                self.next_drain.store((idx + 1) % n, Ordering::Relaxed);
                let tail = self.rings[idx].tail();
                self.producer_wakers[idx].wake_up_to(tail);
                return Ok(bytes);
            }
        }
        Err(RingError::Empty)
    }

    /// Block until either a pop succeeds or `timeout` elapses.
    /// Consumer parks on the SHARED consumer waker; any producer's
    /// push fires it.
    pub fn recv_blocking(
        &self,
        out: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, BlockingError> {
        let deadline = timeout.map(|d| Instant::now() + d);
        loop {
            match self.try_pop(out) {
                Ok(n) => return Ok(n),
                Err(RingError::Empty) => {}
                Err(e) => return Err(BlockingError::Ring(e)),
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(n) = self.try_pop_inner(out) {
                    return Ok(n);
                }
                std::hint::spin_loop();
            }
            let target = self.total_published.load(Ordering::Acquire) + 1;
            let token = self.consumer_waker.try_park(target)?;
            if let Ok(n) = self.try_pop_inner(out) {
                self.consumer_waker.release(token);
                return Ok(n);
            }
            let remaining = match deadline {
                None => None,
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        self.consumer_waker.release(token);
                        return Err(BlockingError::Timeout);
                    }
                    Some(d - now)
                }
            };
            match self.consumer_waker.wait(token, remaining) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingError::Timeout),
                Err(e) => return Err(BlockingError::from(e)),
            }
        }
    }

    #[inline]
    fn try_pop_inner(&self, out: &mut [u8]) -> Result<usize, RingError> {
        let n = self.rings.len();
        let start = self.next_drain.load(Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(bytes) = self.rings[idx].try_pop(out) {
                self.next_drain.store((idx + 1) % n, Ordering::Relaxed);
                let tail = self.rings[idx].tail();
                self.producer_wakers[idx].wake_up_to(tail);
                return Ok(bytes);
            }
        }
        Err(RingError::Empty)
    }

    /// Number of producer rings this consumer drains.
    pub fn n_producers(&self) -> usize { self.rings.len() }

    /// Approximate total pending items across every ring.
    pub fn approx_total_len(&self) -> usize {
        self.rings.iter().map(|r| r.approx_len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn round_trip_4p_1c_anon() {
        let (producers, consumer) =
            BlockingMpscRing::create_anon_pool(4, 8).expect("create");
        const PER_PROD: u64 = 25;
        let total: u64 = PER_PROD * 4;
        let handles: Vec<_> = producers
            .into_iter()
            .enumerate()
            .map(|(pid, p)| {
                thread::spawn(move || {
                    for i in 0..PER_PROD {
                        let val = (pid as u64) * 1_000_000 + i;
                        let mut payload = [0u8; 56];
                        payload[..8].copy_from_slice(&val.to_le_bytes());
                        p.send_blocking(&payload, Some(Duration::from_secs(5)))
                            .expect("send");
                    }
                })
            })
            .collect();
        let mut buf = [0u8; 64];
        let mut seen: Vec<u64> = Vec::with_capacity(total as usize);
        for _ in 0..total {
            consumer
                .recv_blocking(&mut buf, Some(Duration::from_secs(5)))
                .expect("recv");
            seen.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        for h in handles {
            h.join().unwrap();
        }
        seen.sort_unstable();
        let mut expected: Vec<u64> = Vec::with_capacity(total as usize);
        for pid in 0..4u64 {
            for i in 0..PER_PROD {
                expected.push(pid * 1_000_000 + i);
            }
        }
        expected.sort_unstable();
        assert_eq!(seen, expected, "every item delivered exactly once");
    }

    #[test]
    fn recv_blocking_returns_timeout() {
        let (_producers, consumer) =
            BlockingMpscRing::create_anon_pool(2, 4).expect("create");
        let mut buf = [0u8; 64];
        let t0 = Instant::now();
        let err = consumer.recv_blocking(&mut buf, Some(Duration::from_millis(60)));
        assert_eq!(err, Err(BlockingError::Timeout));
        assert!(t0.elapsed() >= Duration::from_millis(50));
    }
}
