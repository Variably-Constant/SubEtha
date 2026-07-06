//! `BlockingMpmcRing`: composed-SPSC MPMC grid with cross-process
//! futex-shaped `send_blocking` / `recv_blocking`.
//!
//! Wraps [`crate::mpmc_ring::SharedRingMpmc`] (N independent Lamport
//! SPSC rings, statically partitioned across M consumers
//! round-robin: consumer `m` owns rings `m, m + M, m + 2M, ...`).
//!
//! Wake routing:
//! - Each producer parks on its own ring's `producer_waker[i]` when
//!   the ring is full. The owning consumer wakes that specific
//!   waker after popping from ring `i`.
//! - Each consumer parks on its own `consumer_waker[m]` when every
//!   ring in its subset is empty. A producer publishing to ring `i`
//!   wakes `consumer_waker[i % M]` only.
//!
//! Per-subset shared `total_published[m]` counter drives the wake
//! seq so consumer parks at `total_published[m] + 1`.
//!
//! See [`crate::cross_process_waker`] for the wake protocol and
//! [`crate::blocking_spsc_ring::BlockingSpscRing`] /
//! [`crate::blocking_mpsc_ring::BlockingMpscRing`] for the simpler
//! shapes.

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

/// Factory for an MPMC grid of N SPSC rings partitioned across M
/// consumers, every blocking call backed by a cross-process waker.
pub struct BlockingMpmcRing;

/// One producer handle. Sole writer to one underlying SPSC ring.
/// `!Sync + !Clone + Send`: one producer per thread.
pub struct BlockingMpmcProducer {
    ring: Arc<SpscRingCore>,
    own_waker: Arc<CrossProcessWaker>,
    /// Consumer waker for the subset that owns this ring
    /// (`subset = ring_index % n_consumers`).
    consumer_waker: Arc<CrossProcessWaker>,
    /// Shared total-published counter for the subset that owns this
    /// ring (drives the consumer-side wake seq).
    subset_total_published: Arc<AtomicU64>,
    _not_sync: PhantomData<Cell<()>>,
}

/// One consumer handle. Sole drainer of an assigned subset of
/// producer rings (round-robin from the factory).
/// `!Sync + !Clone + Send`.
pub struct BlockingMpmcConsumer {
    /// Producer rings in this consumer's subset.
    rings: Vec<Arc<SpscRingCore>>,
    /// Per-ring producer-side wakers (parallel to `rings`).
    producer_wakers: Vec<Arc<CrossProcessWaker>>,
    /// This consumer's own waker.
    own_waker: Arc<CrossProcessWaker>,
    /// Counter shared with all producers in this consumer's subset.
    subset_total_published: Arc<AtomicU64>,
    next_drain: AtomicUsize,
    _not_sync: PhantomData<Cell<()>>,
}

impl BlockingMpmcRing {
    /// In-process grid: N rings, M consumer subsets, anon-mapped.
    pub fn create_anon_grid(
        n_producers: usize,
        n_consumers: usize,
        capacity: usize,
    ) -> Result<(Vec<BlockingMpmcProducer>, Vec<BlockingMpmcConsumer>), BlockingError>
    {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(
            n_producers >= n_consumers,
            "n_producers ({n_producers}) must be >= n_consumers ({n_consumers})",
        );
        let mut rings = Vec::with_capacity(n_producers);
        let mut producer_wakers = Vec::with_capacity(n_producers);
        for _ in 0..n_producers {
            rings.push(Arc::new(
                SpscRingCore::create_anon(capacity).map_err(BlockingError::from)?,
            ));
            producer_wakers.push(Arc::new(
                CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
        }
        let mut consumer_wakers = Vec::with_capacity(n_consumers);
        let mut subset_total_published = Vec::with_capacity(n_consumers);
        for _ in 0..n_consumers {
            consumer_wakers.push(Arc::new(
                CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
            subset_total_published.push(Arc::new(AtomicU64::new(0)));
        }
        Ok(build_grid(
            rings,
            producer_wakers,
            consumer_wakers,
            subset_total_published,
            n_consumers,
        ))
    }

    /// File-backed grid. Path layout:
    ///   `<prefix>.ring.{i}.bin`  - SPSC ring for producer `i`
    ///   `<prefix>.pw.{i}.bin`    - producer-side waker for ring `i`
    ///   `<prefix>.cw.{m}.bin`    - consumer-side waker for subset `m`
    pub fn create_grid(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        n_consumers: usize,
        capacity: usize,
    ) -> Result<(Vec<BlockingMpmcProducer>, Vec<BlockingMpmcConsumer>), BlockingError>
    {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(n_producers >= n_consumers, "n_producers must be >= n_consumers");
        let base = path_prefix.as_ref().to_path_buf();
        let mut rings = Vec::with_capacity(n_producers);
        let mut producer_wakers = Vec::with_capacity(n_producers);
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
        let mut consumer_wakers = Vec::with_capacity(n_consumers);
        let mut subset_total_published = Vec::with_capacity(n_consumers);
        for m in 0..n_consumers {
            consumer_wakers.push(Arc::new(
                CrossProcessWaker::create(cw_path(&base, m), MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
            subset_total_published.push(Arc::new(AtomicU64::new(0)));
        }
        Ok(build_grid(
            rings,
            producer_wakers,
            consumer_wakers,
            subset_total_published,
            n_consumers,
        ))
    }

    /// Open an existing file-backed grid.
    pub fn open_grid(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        n_consumers: usize,
        expected_capacity: usize,
    ) -> Result<(Vec<BlockingMpmcProducer>, Vec<BlockingMpmcConsumer>), BlockingError>
    {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(n_producers >= n_consumers, "n_producers must be >= n_consumers");
        let base = path_prefix.as_ref().to_path_buf();
        let mut rings = Vec::with_capacity(n_producers);
        let mut producer_wakers = Vec::with_capacity(n_producers);
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
        let mut consumer_wakers = Vec::with_capacity(n_consumers);
        let mut subset_total_published = Vec::with_capacity(n_consumers);
        for m in 0..n_consumers {
            consumer_wakers.push(Arc::new(
                CrossProcessWaker::open(cw_path(&base, m), MAX_WAITERS_DEFAULT)
                    .map_err(BlockingError::from)?,
            ));
            subset_total_published.push(Arc::new(AtomicU64::new(0)));
        }
        Ok(build_grid(
            rings,
            producer_wakers,
            consumer_wakers,
            subset_total_published,
            n_consumers,
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

fn cw_path(base: &Path, m: usize) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".cw.{m}.bin"));
    PathBuf::from(s)
}

fn build_grid(
    rings: Vec<Arc<SpscRingCore>>,
    producer_wakers: Vec<Arc<CrossProcessWaker>>,
    consumer_wakers: Vec<Arc<CrossProcessWaker>>,
    subset_total_published: Vec<Arc<AtomicU64>>,
    n_consumers: usize,
) -> (Vec<BlockingMpmcProducer>, Vec<BlockingMpmcConsumer>) {
    let producers: Vec<BlockingMpmcProducer> = rings
        .iter()
        .zip(producer_wakers.iter())
        .enumerate()
        .map(|(i, (r, pw))| {
            let subset = i % n_consumers;
            BlockingMpmcProducer {
                ring: Arc::clone(r),
                own_waker: Arc::clone(pw),
                consumer_waker: Arc::clone(&consumer_wakers[subset]),
                subset_total_published: Arc::clone(&subset_total_published[subset]),
                _not_sync: PhantomData,
            }
        })
        .collect();

    let mut consumer_rings: Vec<Vec<Arc<SpscRingCore>>> =
        (0..n_consumers).map(|_| Vec::new()).collect();
    let mut consumer_pwakers: Vec<Vec<Arc<CrossProcessWaker>>> =
        (0..n_consumers).map(|_| Vec::new()).collect();
    for (i, (r, pw)) in rings.iter().zip(producer_wakers.iter()).enumerate() {
        let subset = i % n_consumers;
        consumer_rings[subset].push(Arc::clone(r));
        consumer_pwakers[subset].push(Arc::clone(pw));
    }
    let consumers: Vec<BlockingMpmcConsumer> = consumer_rings
        .into_iter()
        .zip(consumer_pwakers)
        .zip(consumer_wakers)
        .zip(subset_total_published)
        .map(|(((subset_rings, subset_pwakers), own_waker), total)| {
            BlockingMpmcConsumer {
                rings: subset_rings,
                producer_wakers: subset_pwakers,
                own_waker,
                subset_total_published: total,
                next_drain: AtomicUsize::new(0),
                _not_sync: PhantomData,
            }
        })
        .collect();

    (producers, consumers)
}

impl BlockingMpmcProducer {
    /// Non-blocking push. On success, advances subset-shared
    /// counter + fires a wake at the consumer waker.
    #[inline]
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        let r = self.ring.try_push(payload);
        if r.is_ok() {
            let new_seq =
                self.subset_total_published.fetch_add(1, Ordering::Release) + 1;
            self.consumer_waker.wake_up_to(new_seq);
        }
        r
    }

    /// Block until either push succeeds or `timeout` elapses.
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
                    let new_seq = self
                        .subset_total_published
                        .fetch_add(1, Ordering::Release)
                        + 1;
                    self.consumer_waker.wake_up_to(new_seq);
                    return Ok(());
                }
                std::hint::spin_loop();
            }
            let target = self.ring.tail() + 1;
            let token = self.own_waker.try_park(target)?;
            if self.ring.try_push(payload).is_ok() {
                self.own_waker.release(token);
                let new_seq = self
                    .subset_total_published
                    .fetch_add(1, Ordering::Release)
                    + 1;
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

    pub fn capacity(&self) -> usize { self.ring.capacity() }
    pub fn head(&self) -> u64 { self.ring.head() }
}

impl BlockingMpmcConsumer {
    /// Non-blocking pop, round-robin across this consumer's subset.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.try_pop_inner(out)
    }

    /// Block until either a pop succeeds or `timeout` elapses.
    pub fn recv_blocking(
        &self,
        out: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, BlockingError> {
        let deadline = timeout.map(|d| Instant::now() + d);
        loop {
            match self.try_pop_inner(out) {
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
            let target = self.subset_total_published.load(Ordering::Acquire) + 1;
            let token = self.own_waker.try_park(target)?;
            if let Ok(n) = self.try_pop_inner(out) {
                self.own_waker.release(token);
                return Ok(n);
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

    /// Number of rings in this consumer's subset.
    pub fn n_rings(&self) -> usize { self.rings.len() }

    /// Approximate pending items across this consumer's subset.
    pub fn approx_subset_len(&self) -> usize {
        self.rings.iter().map(|r| r.approx_len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn round_trip_4p_2c_anon() {
        let (producers, consumers) =
            BlockingMpmcRing::create_anon_grid(4, 2, 8).expect("create");
        const PER_PROD: u64 = 25;
        let total: u64 = PER_PROD * 4;

        let prod_handles: Vec<_> = producers
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

        let cons_handles: Vec<_> = consumers
            .into_iter()
            .map(|c| {
                let per_consumer = (total / 2) as usize;
                thread::spawn(move || {
                    let mut buf = [0u8; 64];
                    let mut got: Vec<u64> = Vec::with_capacity(per_consumer);
                    for _ in 0..per_consumer {
                        c.recv_blocking(&mut buf, Some(Duration::from_secs(5)))
                            .expect("recv");
                        got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
                    }
                    got
                })
            })
            .collect();

        for h in prod_handles {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = cons_handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();

        let mut expected: Vec<u64> = Vec::with_capacity(total as usize);
        for pid in 0..4u64 {
            for i in 0..PER_PROD {
                expected.push(pid * 1_000_000 + i);
            }
        }
        expected.sort_unstable();
        assert_eq!(all, expected, "every item delivered exactly once");
    }

    #[test]
    fn recv_blocking_returns_timeout() {
        let (_producers, consumers) =
            BlockingMpmcRing::create_anon_grid(2, 2, 4).expect("create");
        let mut buf = [0u8; 64];
        let t0 = Instant::now();
        let err = consumers[0].recv_blocking(&mut buf, Some(Duration::from_millis(60)));
        assert_eq!(err, Err(BlockingError::Timeout));
        assert!(t0.elapsed() >= Duration::from_millis(50));
    }
}
