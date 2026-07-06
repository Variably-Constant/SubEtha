//! `SharedRingMpsc` - composed multi-producer / single-consumer
//! ring built from N independent Lamport SPSC rings.
//!
//! Each producer is a sole-writer to its own [`SpscRingCore`]. The
//! single consumer drains all N rings round-robin. Per-push cost
//! is one Acquire load + one Release store (pure Lamport SPSC).
//! Per-pop cost is the same on the successful ring, plus an
//! Acquire-load on each empty ring the consumer skips before
//! finding a non-empty one.
//!
//! # When this beats Vyukov MPMC
//!
//! `SharedRing` (the existing Vyukov MPMC primitive) is the right
//! choice when callers need **global FIFO order across all
//! producers**. `SharedRingMpsc` only preserves **per-producer
//! FIFO**: items from producer A and producer B can interleave at
//! the consumer based on which ring the consumer drained first.
//!
//! For most fan-in workloads (task queues, log shipping, metric
//! aggregation, result collection), per-producer FIFO is what
//! callers actually need. Saving the consumer-side CAS that
//! Vyukov pays buys ~2x consumer-side throughput on this class
//! of workload.
//!
//! # Compile-time enforcement
//!
//! The consumer handle is `!Sync + !Clone + Send` so the compiler
//! guarantees a single consumer at runtime. Producer handles are
//! `!Sync + !Clone + Send` per-handle; callers receive a `Vec` of
//! them at construction and distribute them to producer threads
//! (each handle moves to its dedicated thread).

use std::cell::Cell;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::shared_ring::{RingError, SharedRing};
use crate::spsc_ring::SpscRingCore;

/// Factory for an MPSC pool composed from N Lamport SPSC rings.
pub struct SharedRingMpsc;

/// A single producer handle. Sole writer to one underlying SPSC
/// ring; `!Sync + !Clone + Send` so the compiler enforces that one
/// thread owns one producer handle.
pub struct MpscProducer {
    inner: Arc<SpscRingCore>,
    _not_sync: PhantomData<Cell<()>>,
}

/// The single consumer handle. Drains all N producer rings
/// round-robin; `!Sync + !Clone + Send` so the compiler enforces
/// a single consumer.
pub struct MpscConsumer {
    rings: Vec<Arc<SpscRingCore>>,
    /// Round-robin cursor: index of the ring to try first on next
    /// `try_pop`. Avoids always hammering ring 0 first under steady
    /// load; gives each producer a fair share of the consumer's
    /// attention.
    next_drain: AtomicUsize,
    _not_sync: PhantomData<Cell<()>>,
}

impl SharedRingMpsc {
    /// Anonymous in-memory pool of N producer rings + one consumer.
    /// Skips file create + ftruncate; in-process only.
    pub fn create_anon_pool(
        n_producers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpscProducer>, MpscConsumer), RingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        for _ in 0..n_producers {
            rings.push(Arc::new(SpscRingCore::create_anon(capacity)?));
        }
        let producers: Vec<MpscProducer> = rings
            .iter()
            .map(|r| MpscProducer {
                inner: Arc::clone(r),
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscConsumer {
            rings,
            next_drain: AtomicUsize::new(0),
            _not_sync: PhantomData,
        };
        Ok((producers, consumer))
    }

    /// File-backed pool: one file per producer ring. Each ring's
    /// path is derived from `path_prefix` by appending `.{i}.bin`.
    pub fn create_pool(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpscProducer>, MpscConsumer), RingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        let base = path_prefix.as_ref().to_path_buf();
        for i in 0..n_producers {
            let path = ring_path(&base, i);
            rings.push(Arc::new(SpscRingCore::create(&path, capacity)?));
        }
        let producers: Vec<MpscProducer> = rings
            .iter()
            .map(|r| MpscProducer {
                inner: Arc::clone(r),
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscConsumer {
            rings,
            next_drain: AtomicUsize::new(0),
            _not_sync: PhantomData,
        };
        Ok((producers, consumer))
    }

    /// Open an existing file-backed pool. Caller passes the same
    /// path_prefix + n_producers + capacity the pool was created
    /// with.
    pub fn open_pool(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        expected_capacity: usize,
    ) -> Result<(Vec<MpscProducer>, MpscConsumer), RingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        let base = path_prefix.as_ref().to_path_buf();
        for i in 0..n_producers {
            let path = ring_path(&base, i);
            rings.push(Arc::new(SpscRingCore::open(&path, expected_capacity)?));
        }
        let producers: Vec<MpscProducer> = rings
            .iter()
            .map(|r| MpscProducer {
                inner: Arc::clone(r),
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscConsumer {
            rings,
            next_drain: AtomicUsize::new(0),
            _not_sync: PhantomData,
        };
        Ok((producers, consumer))
    }
}

fn ring_path(prefix: &std::path::Path, i: usize) -> std::path::PathBuf {
    let mut s = prefix.as_os_str().to_owned();
    s.push(format!(".{i}.bin"));
    std::path::PathBuf::from(s)
}

impl MpscProducer {
    /// Push one payload to this producer's ring. Pure Lamport SPSC.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        self.inner.try_push(payload)
    }

    /// Capacity of this producer's ring (always a power of 2).
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Current head of this producer's ring (own published position).
    pub fn head(&self) -> u64 {
        self.inner.head()
    }
}

impl MpscConsumer {
    /// Drain one item, round-robin across all N producer rings.
    /// Returns `Ok` on the first ring that has an item;
    /// `Err(Empty)` only if every ring is empty.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        let n = self.rings.len();
        let start = self.next_drain.load(Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(bytes) = self.rings[idx].try_pop(out) {
                // Advance the cursor past the ring we just drained so
                // the NEXT call starts at idx+1; producer fairness.
                self.next_drain.store((idx + 1) % n, Ordering::Relaxed);
                return Ok(bytes);
            }
        }
        Err(RingError::Empty)
    }

    /// Number of producer rings this consumer drains.
    pub fn n_producers(&self) -> usize {
        self.rings.len()
    }

    /// Total approximate items waiting across all producer rings.
    pub fn approx_total_len(&self) -> usize {
        self.rings.iter().map(|r| r.approx_len()).sum()
    }
}

// ---------------------------------------------------------------------------
// Single-ring MPSC variant that preserves global FIFO.
// ---------------------------------------------------------------------------

/// Factory for a single-ring MPSC pool that preserves **global FIFO
/// ordering across all producers**. Producers contend on one
/// Vyukov-MPMC `producer_seq` (CAS retries on push) while the
/// single consumer skips the consumer-side CAS by using the
/// `try_pop_spsc` fast path on the underlying [`SharedRing`].
///
/// # When this beats [`SharedRingMpsc`] (the composed variant)
///
/// `SharedRingMpsc` is faster on the producer side for any N
/// because each producer owns its own ring (zero CAS contention).
/// `SharedRingMpscFifo` wins on the consumer side because the
/// consumer drains one ring instead of round-robining N. The
/// crossover depends on:
///
/// - **N (producer count)**: low N favours `Fifo` (light producer
///   CAS contention + no consumer round-robin); high N favours
///   `Composed` (independent producer rings, no shared CAS).
/// - **ordering requirement**: `Fifo` is the only choice when the
///   caller needs global FIFO across all producers (e.g. a totally-
///   ordered event log). `Composed` only preserves per-producer FIFO.
///
/// `examples/mpmc_shootout.rs` benches both side by side at common
/// (N producers, 1 consumer) shapes; pick by measurement.
pub struct SharedRingMpscFifo;

/// One producer handle on a [`SharedRingMpscFifo`] pool. Sole
/// owner of one slot of the producer pool; `!Sync + !Clone + Send`.
pub struct MpscFifoProducer {
    inner: Arc<SharedRing>,
    _not_sync: PhantomData<Cell<()>>,
}

/// The single consumer handle. Uses the consumer-side SPSC fast
/// path on the shared Vyukov ring; `!Sync + !Clone + Send`.
pub struct MpscFifoConsumer {
    inner: Arc<SharedRing>,
    _not_sync: PhantomData<Cell<()>>,
}

impl SharedRingMpscFifo {
    /// Anonymous in-memory single-ring MPSC pool.
    pub fn create_anon_pool(
        n_producers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpscFifoProducer>, MpscFifoConsumer), RingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let ring = Arc::new(SharedRing::create_anon(capacity)?);
        let producers: Vec<MpscFifoProducer> = (0..n_producers)
            .map(|_| MpscFifoProducer {
                inner: Arc::clone(&ring),
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscFifoConsumer {
            inner: ring,
            _not_sync: PhantomData,
        };
        Ok((producers, consumer))
    }

    /// File-backed single-ring MPSC pool. One backing file (not
    /// one per producer), so cross-process layout is identical to
    /// a single [`SharedRing`].
    pub fn create_pool(
        path: impl AsRef<Path>,
        n_producers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpscFifoProducer>, MpscFifoConsumer), RingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let ring = Arc::new(SharedRing::create(path, capacity)?);
        let producers: Vec<MpscFifoProducer> = (0..n_producers)
            .map(|_| MpscFifoProducer {
                inner: Arc::clone(&ring),
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscFifoConsumer {
            inner: ring,
            _not_sync: PhantomData,
        };
        Ok((producers, consumer))
    }

    /// Open an existing file-backed single-ring MPSC pool.
    pub fn open_pool(
        path: impl AsRef<Path>,
        n_producers: usize,
        expected_capacity: usize,
    ) -> Result<(Vec<MpscFifoProducer>, MpscFifoConsumer), RingError> {
        assert!(n_producers >= 1, "n_producers must be >= 1");
        let ring = Arc::new(SharedRing::open(path, expected_capacity)?);
        let producers: Vec<MpscFifoProducer> = (0..n_producers)
            .map(|_| MpscFifoProducer {
                inner: Arc::clone(&ring),
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscFifoConsumer {
            inner: ring,
            _not_sync: PhantomData,
        };
        Ok((producers, consumer))
    }
}

impl MpscFifoProducer {
    /// Push one payload via the Vyukov MPMC producer-side protocol
    /// (CAS on `producer_seq` to claim a slot, write payload, then
    /// Release-store the slot's sequence number to publish).
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        self.inner.try_push(payload)
    }
}

impl MpscFifoConsumer {
    /// Pop one payload using the single-consumer fast path on the
    /// shared Vyukov ring. Skips the consumer-side CAS that the
    /// MPMC `try_pop` needs to defend against racing consumers.
    /// Sound here because the `!Sync + !Clone` type contract
    /// guarantees this is the only consumer.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        self.inner.try_pop_spsc(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spsc_ring::SPSC_PAYLOAD_BYTES;
    use std::thread;

    #[test]
    fn create_anon_pool_round_trip() {
        let (producers, consumer) = SharedRingMpsc::create_anon_pool(4, 8).unwrap();
        assert_eq!(producers.len(), 4);
        assert_eq!(consumer.n_producers(), 4);

        // Each producer pushes one item with a value matching its index.
        for (i, p) in producers.iter().enumerate() {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&(i as u32).to_le_bytes());
            p.try_push(&buf).unwrap();
        }

        // Drain 4 items; each producer's value appears exactly once.
        let mut seen = [false; 4];
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..4 {
            consumer.try_pop(&mut out).unwrap();
            let v = u32::from_le_bytes(out[..4].try_into().unwrap()) as usize;
            assert!(v < 4, "got value {v} outside producer index range");
            assert!(!seen[v], "value {v} appeared twice");
            seen[v] = true;
        }
        assert!(seen.iter().all(|&s| s), "not every producer delivered");
        assert_eq!(consumer.try_pop(&mut out).unwrap_err(), RingError::Empty);
    }

    #[test]
    fn concurrent_producers_lose_no_items() {
        let (producers, consumer) = SharedRingMpsc::create_anon_pool(4, 64).unwrap();
        const PER_PRODUCER: u32 = 10_000;

        // Spawn one thread per producer.
        let producer_handles: Vec<_> = producers
            .into_iter()
            .enumerate()
            .map(|(pid, p)| {
                thread::spawn(move || {
                    for i in 0..PER_PRODUCER {
                        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                        // Encode (producer_id, sequence) so we can verify
                        // per-producer FIFO and total count.
                        buf[..4].copy_from_slice(&(pid as u32).to_le_bytes());
                        buf[4..8].copy_from_slice(&i.to_le_bytes());
                        while p.try_push(&buf).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        // Consumer drains; tracks (per-producer next-expected sequence).
        let n_producers = consumer.n_producers();
        let consumer_handle = thread::spawn(move || -> (u32, Vec<u32>) {
            let mut next: Vec<u32> = vec![0; n_producers];
            let mut total: u32 = 0;
            let target = PER_PRODUCER * n_producers as u32;
            let mut out = [0u8; SPSC_PAYLOAD_BYTES];
            while total < target {
                if consumer.try_pop(&mut out).is_ok() {
                    let pid = u32::from_le_bytes(out[..4].try_into().unwrap()) as usize;
                    let seq = u32::from_le_bytes(out[4..8].try_into().unwrap());
                    assert_eq!(
                        seq, next[pid],
                        "per-producer FIFO violated for producer {pid}: expected {} got {}",
                        next[pid], seq,
                    );
                    next[pid] += 1;
                    total += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            (total, next)
        });

        for h in producer_handles {
            h.join().unwrap();
        }
        let (total, next) = consumer_handle.join().unwrap();
        assert_eq!(total, PER_PRODUCER * 4);
        assert_eq!(next, vec![PER_PRODUCER; 4]);
    }

    #[test]
    fn round_robin_is_fair() {
        // With 3 producers each pushing 1 item, the consumer should
        // see items in the round-robin drain order rather than always
        // ring 0 first.
        let (producers, consumer) = SharedRingMpsc::create_anon_pool(3, 4).unwrap();
        for (i, p) in producers.iter().enumerate() {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&(i as u32).to_le_bytes());
            p.try_push(&buf).unwrap();
        }
        let mut order = Vec::new();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        while consumer.try_pop(&mut out).is_ok() {
            order.push(u32::from_le_bytes(out[..4].try_into().unwrap()));
        }
        // First pop should be ring 0 (cursor starts there); next pops
        // visit 1, 2 in order due to the cursor advance.
        assert_eq!(order, vec![0, 1, 2]);
    }
}
