//! `SharedRingMpmc` - composed multi-producer / multi-consumer
//! ring built from N independent Lamport SPSC rings, with M
//! consumers partitioning the rings round-robin.
//!
//! Architecture:
//!
//!  * N producers, each sole-writer to its own
//!    [`SpscRingCore`]. Per-push cost is one Acquire load + one
//!    Release store (pure Lamport).
//!  * M consumers, each statically assigned a subset of the N
//!    producer rings (round-robin: consumer `i` gets producer rings
//!    `i, i+M, i+2M, ...`). Each consumer is the sole drainer of
//!    its subset, so the consumer-side CAS Vyukov MPMC needs is
//!    not present here either.
//!
//! Per-pop cost (when the chosen ring has an item):
//!  - 1 Acquire load + 1 Release store on the consumer's subset
//!    ring. Pure Lamport.
//!
//! Per-pop cost (worst case, every ring in the subset is empty):
//!  - 1 Acquire load per ring in the subset (`ceil(N/M)` checks),
//!    then `Err(Empty)`.
//!
//! # When this is the right MPMC primitive
//!
//! `SharedRingMpmc` is the **default-recommended MPMC primitive**
//! when callers do NOT need global FIFO order across all
//! producers. It preserves **per-producer FIFO** (items from one
//! producer arrive at one consumer in push order), but items from
//! different producers can interleave at different consumers
//! arbitrarily.
//!
//! Use [`SharedRing`](crate::SharedRing) (Vyukov MPMC) if global
//! FIFO across all producers is required. The Vyukov primitive is
//! somewhat slower under contention - this grid runs ~1.3-1.6x
//! faster at the same total buffer - but gives total ordering.
//!
//! # Compile-time contracts
//!
//! - Producer handles ([`MpmcProducer`]): `!Sync + !Clone + Send`.
//!   Each handle is the sole writer for its ring; the compiler
//!   guarantees one producer per ring at one time.
//! - Consumer handles ([`MpmcConsumer`]): `!Sync + !Clone + Send`.
//!   Each handle is the sole drainer of its assigned subset; the
//!   compiler guarantees one consumer per subset.
//!
//! Callers receive `Vec<MpmcProducer>` and `Vec<MpmcConsumer>` at
//! construction and move each handle to its dedicated thread.

use std::cell::Cell;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::shared_ring::RingError;
use crate::spsc_ring::SpscRingCore;

/// Factory for an MPMC grid composed from N Lamport SPSC rings
/// partitioned across M consumers.
pub struct SharedRingMpmc;

/// One producer handle. Sole writer to one underlying SPSC ring.
pub struct MpmcProducer {
    inner: Arc<SpscRingCore>,
    _not_sync: PhantomData<Cell<()>>,
}

/// One consumer handle. Sole drainer of a subset of producer rings
/// (round-robin assignment from the factory).
pub struct MpmcConsumer {
    rings: Vec<Arc<SpscRingCore>>,
    next_drain: AtomicUsize,
    _not_sync: PhantomData<Cell<()>>,
}

impl SharedRingMpmc {
    /// Anonymous in-memory MPMC grid: `n_producers` rings,
    /// `n_consumers` consumer handles. Consumer `i` drains
    /// producer rings `i`, `i + n_consumers`, `i + 2*n_consumers`,
    /// and so on.
    ///
    /// Constraints: `n_consumers >= 1`, `n_producers >= n_consumers`
    /// (so every consumer has at least one ring; idle consumers
    /// are a configuration smell, not a feature).
    pub fn create_anon_grid(
        n_producers: usize,
        n_consumers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpmcProducer>, Vec<MpmcConsumer>), RingError> {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(
            n_producers >= n_consumers,
            "n_producers ({n_producers}) must be >= n_consumers ({n_consumers}); \
             every consumer needs at least one ring to drain",
        );

        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        for _ in 0..n_producers {
            rings.push(Arc::new(SpscRingCore::create_anon(capacity)?));
        }
        build_grid(rings, n_consumers)
    }

    /// MPMC grid laid out in ONE caller-owned region (huge / large
    /// pages). All `n_producers` SPSC lanes are carved back-to-back
    /// from the single region, so the whole grid sits on a handful of
    /// 2 MB / 1 GB pages instead of `n_producers` separate small
    /// mappings - the case where large pages actually shed TLB
    /// pressure. The region must hold at least
    /// `spsc_ring_file_size(capacity) * n_producers` bytes.
    ///
    /// This is the per-producer-FIFO MPMC primitive on large pages;
    /// [`SharedRing::create_in_region`](crate::SharedRing::create_in_region)
    /// is the global-FIFO (Vyukov) counterpart.
    pub fn create_grid_in_region<R: crate::spsc_ring::RegionOwner>(
        mut region: R,
        n_producers: usize,
        n_consumers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpmcProducer>, Vec<MpmcConsumer>), RingError> {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(n_producers >= n_consumers,
            "n_producers must be >= n_consumers");
        let lane_bytes = crate::spsc_ring::spsc_ring_file_size(capacity);
        let need = lane_bytes
            .checked_mul(n_producers)
            .ok_or(RingError::LayoutMismatch)?;
        if region.region_len() < need {
            return Err(RingError::LayoutMismatch);
        }
        // Capture the base pointer while we still hold the region
        // exclusively, then move it behind an Arc every lane keeps
        // alive. The mapping address is stable across the move - it is
        // an OS mapping, not the struct's own address.
        let base = region.region_ptr();
        let whole: Arc<dyn std::any::Any + Send + Sync> = Arc::new(region);

        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        for i in 0..n_producers {
            let lane = SubRegion {
                _whole: Arc::clone(&whole),
                ptr: unsafe { base.add(i * lane_bytes) },
                len: lane_bytes,
            };
            rings.push(Arc::new(SpscRingCore::create_in_region(lane, capacity)?));
        }
        build_grid(rings, n_consumers)
    }

    /// File-backed MPMC grid: one file per producer ring; the
    /// file path for ring `i` is `path_prefix.{i}.bin`.
    pub fn create_grid(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        n_consumers: usize,
        capacity: usize,
    ) -> Result<(Vec<MpmcProducer>, Vec<MpmcConsumer>), RingError> {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(n_producers >= n_consumers,
            "n_producers must be >= n_consumers");
        let base = path_prefix.as_ref().to_path_buf();
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        for i in 0..n_producers {
            let path = ring_path(&base, i);
            rings.push(Arc::new(SpscRingCore::create(&path, capacity)?));
        }
        build_grid(rings, n_consumers)
    }

    /// Open an existing file-backed grid.
    pub fn open_grid(
        path_prefix: impl AsRef<Path>,
        n_producers: usize,
        n_consumers: usize,
        expected_capacity: usize,
    ) -> Result<(Vec<MpmcProducer>, Vec<MpmcConsumer>), RingError> {
        assert!(n_consumers >= 1, "n_consumers must be >= 1");
        assert!(n_producers >= n_consumers,
            "n_producers must be >= n_consumers");
        let base = path_prefix.as_ref().to_path_buf();
        let mut rings: Vec<Arc<SpscRingCore>> = Vec::with_capacity(n_producers);
        for i in 0..n_producers {
            let path = ring_path(&base, i);
            rings.push(Arc::new(SpscRingCore::open(&path, expected_capacity)?));
        }
        build_grid(rings, n_consumers)
    }
}

fn build_grid(
    rings: Vec<Arc<SpscRingCore>>,
    n_consumers: usize,
) -> Result<(Vec<MpmcProducer>, Vec<MpmcConsumer>), RingError> {
    let producers: Vec<MpmcProducer> = rings
        .iter()
        .map(|r| MpmcProducer {
            inner: Arc::clone(r),
            _not_sync: PhantomData,
        })
        .collect();

    // Round-robin assign producer rings to consumer subsets.
    let mut consumer_rings: Vec<Vec<Arc<SpscRingCore>>> =
        (0..n_consumers).map(|_| Vec::new()).collect();
    for (producer_idx, ring) in rings.iter().enumerate() {
        consumer_rings[producer_idx % n_consumers].push(Arc::clone(ring));
    }

    let consumers: Vec<MpmcConsumer> = consumer_rings
        .into_iter()
        .map(|subset| MpmcConsumer {
            rings: subset,
            next_drain: AtomicUsize::new(0),
            _not_sync: PhantomData,
        })
        .collect();

    Ok((producers, consumers))
}

fn ring_path(prefix: &std::path::Path, i: usize) -> std::path::PathBuf {
    let mut s = prefix.as_os_str().to_owned();
    s.push(format!(".{i}.bin"));
    std::path::PathBuf::from(s)
}

/// One back-to-back slice of a shared backing region, handed to a
/// single grid lane. Holds an `Arc` to the whole region so the mapping
/// outlives every lane carved from it; `ptr` is this lane's start
/// (`base + lane_index * lane_bytes`).
struct SubRegion {
    _whole: Arc<dyn std::any::Any + Send + Sync>,
    ptr: *mut u8,
    len: usize,
}

// SAFETY: each lane owns a disjoint, non-overlapping byte range of the
// shared region, and the Arc keeps the mapping alive. The raw pointer
// points into OS-mapped memory whose address is stable for the
// region's whole lifetime.
unsafe impl Send for SubRegion {}
unsafe impl Sync for SubRegion {}

impl crate::spsc_ring::RegionOwner for SubRegion {
    fn region_ptr(&mut self) -> *mut u8 { self.ptr }
    fn region_len(&self) -> usize { self.len }
}

impl MpmcProducer {
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

impl MpmcConsumer {
    /// Drain one item from this consumer's assigned subset of
    /// producer rings, round-robin. Returns `Ok` on the first
    /// non-empty ring; `Err(Empty)` only if every ring in the
    /// subset is empty.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        let n = self.rings.len();
        let start = self.next_drain.load(Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(bytes) = self.rings[idx].try_pop(out) {
                self.next_drain.store((idx + 1) % n, Ordering::Relaxed);
                return Ok(bytes);
            }
        }
        Err(RingError::Empty)
    }

    /// Number of producer rings assigned to this consumer.
    pub fn n_rings(&self) -> usize {
        self.rings.len()
    }

    /// Approximate total items waiting across this consumer's subset.
    pub fn approx_subset_len(&self) -> usize {
        self.rings.iter().map(|r| r.approx_len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spsc_ring::SPSC_PAYLOAD_BYTES;
    use std::thread;

    #[test]
    fn create_anon_grid_round_trip() {
        // 4 producers, 2 consumers; each consumer drains 2 rings.
        let (producers, consumers) =
            SharedRingMpmc::create_anon_grid(4, 2, 8).unwrap();
        assert_eq!(producers.len(), 4);
        assert_eq!(consumers.len(), 2);
        // Round-robin assignment: consumer 0 gets rings 0, 2;
        // consumer 1 gets rings 1, 3.
        assert_eq!(consumers[0].n_rings(), 2);
        assert_eq!(consumers[1].n_rings(), 2);

        for (i, p) in producers.iter().enumerate() {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&(i as u32).to_le_bytes());
            p.try_push(&buf).unwrap();
        }

        // Consumer 0 should see items 0 + 2; consumer 1 should see 1 + 3.
        let mut c0_seen = Vec::new();
        let mut c1_seen = Vec::new();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        while consumers[0].try_pop(&mut out).is_ok() {
            c0_seen.push(u32::from_le_bytes(out[..4].try_into().unwrap()));
        }
        while consumers[1].try_pop(&mut out).is_ok() {
            c1_seen.push(u32::from_le_bytes(out[..4].try_into().unwrap()));
        }
        c0_seen.sort();
        c1_seen.sort();
        assert_eq!(c0_seen, vec![0, 2]);
        assert_eq!(c1_seen, vec![1, 3]);
    }

    #[test]
    fn concurrent_mpmc_loses_no_items() {
        const N_PRODUCERS: usize = 4;
        const N_CONSUMERS: usize = 2;
        const PER_PRODUCER: u32 = 10_000;

        let (producers, consumers) =
            SharedRingMpmc::create_anon_grid(N_PRODUCERS, N_CONSUMERS, 64).unwrap();

        let producer_handles: Vec<_> = producers
            .into_iter()
            .enumerate()
            .map(|(pid, p)| {
                thread::spawn(move || {
                    for i in 0..PER_PRODUCER {
                        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                        buf[..4].copy_from_slice(&(pid as u32).to_le_bytes());
                        buf[4..8].copy_from_slice(&i.to_le_bytes());
                        while p.try_push(&buf).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        let target_per_consumer = (PER_PRODUCER as usize * N_PRODUCERS / N_CONSUMERS) as u32;
        let consumer_handles: Vec<_> = consumers
            .into_iter()
            .map(|c| {
                thread::spawn(move || -> (u32, std::collections::HashMap<u32, u32>) {
                    let mut next: std::collections::HashMap<u32, u32> = Default::default();
                    let mut total: u32 = 0;
                    let mut out = [0u8; SPSC_PAYLOAD_BYTES];
                    while total < target_per_consumer {
                        if c.try_pop(&mut out).is_ok() {
                            let pid = u32::from_le_bytes(out[..4].try_into().unwrap());
                            let seq = u32::from_le_bytes(out[4..8].try_into().unwrap());
                            let expected = next.entry(pid).or_insert(0);
                            assert_eq!(*expected, seq,
                                "per-producer FIFO violated for producer {pid}: expected {} got {}",
                                expected, seq);
                            *expected += 1;
                            total += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                    (total, next)
                })
            })
            .collect();

        for h in producer_handles {
            h.join().unwrap();
        }
        let mut grand_total: u32 = 0;
        for h in consumer_handles {
            let (t, _next) = h.join().unwrap();
            grand_total += t;
        }
        assert_eq!(grand_total, PER_PRODUCER * N_PRODUCERS as u32);
    }

    #[test]
    fn create_grid_in_region_round_trip() {
        // Carve all four SPSC lanes from ONE heap-backed region (the
        // large-page path in miniature; a heap region needs no
        // privilege). Round-trip integrity proves the lanes occupy
        // disjoint, non-overlapping byte ranges.
        use crate::spsc_ring::{spsc_ring_file_size, RegionOwner};
        // 64-byte-aligned heap backing (a page-backed region gives this
        // for free; a Box<[u8]> would not).
        #[repr(C, align(64))]
        #[derive(Clone, Copy)]
        struct Block64([u8; 64]);
        struct HeapRegion(Vec<Block64>);
        impl RegionOwner for HeapRegion {
            fn region_ptr(&mut self) -> *mut u8 {
                self.0.as_mut_ptr() as *mut u8
            }
            fn region_len(&self) -> usize { self.0.len() * 64 }
        }

        let (n_prod, n_cons, cap) = (4usize, 2usize, 8usize);
        let bytes = spsc_ring_file_size(cap) * n_prod;
        let region = HeapRegion(vec![Block64([0u8; 64]); bytes.div_ceil(64)]);
        let (producers, consumers) =
            SharedRingMpmc::create_grid_in_region(region, n_prod, n_cons, cap)
                .unwrap();
        assert_eq!(producers.len(), 4);
        assert_eq!(consumers.len(), 2);

        // Stamp each lane with its producer index, drain everything,
        // and confirm all four arrived exactly once.
        for (i, p) in producers.iter().enumerate() {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&(i as u32).to_le_bytes());
            p.try_push(&buf).unwrap();
        }
        let mut seen = Vec::new();
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        for c in &consumers {
            while c.try_pop(&mut out).is_ok() {
                seen.push(u32::from_le_bytes(out[..4].try_into().unwrap()));
            }
        }
        seen.sort();
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }
}
