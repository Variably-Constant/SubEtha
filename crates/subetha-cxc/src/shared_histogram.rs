//! `SharedHistogram` - cross-process bucketed counter for
//! distribution tracking.
//!
//! Fixed-bucket histogram: caller supplies N bucket boundaries at
//! create time. `record(value)` finds the right bucket via binary
//! search and atomically increments its counter. Useful for
//! latency distributions, request-size distributions, queue-depth
//! sampling - anything where N distributed processes need to
//! aggregate "how many in each bucket" into one shared view.
//!
//! # Bucket semantics
//!
//! For boundaries `[b0, b1, b2, ..., bN-1]`:
//! - Bucket 0: values `value < b0`
//! - Bucket i (1..N-1): values `b{i-1} <= value < bi`
//! - Bucket N: values `value >= b{N-1}` (the overflow bucket)
//!
//! So a histogram with K boundaries has K+1 buckets.
//!
//! # Layout
//!
//! Single MMF file:
//!
//! ```text
//! +---------------------------+
//! | HistogramHeader (64B)     |
//! |   magic, n_boundaries     |
//! |   total_count: AtomicU64  |
//! +---------------------------+
//! | boundaries [u64; N]       |  ascending; verified at open
//! +---------------------------+
//! | counters [AtomicU64; N+1] |  one per bucket
//! +---------------------------+
//! ```
//!
//! # Concurrency
//!
//! Each bucket's counter is its own AtomicU64. `record` uses
//! `fetch_add(1, AcqRel)` to atomically increment; multiple
//! recorders contend only on the SAME bucket's cache line
//! (different buckets are fully concurrent).
//!
//! # Percentile estimation
//!
//! `percentile(p)` walks buckets accumulating counts until p of the
//! total is covered, then linearly interpolates within the target
//! bucket. For coarse boundaries the estimate has bucket-width
//! granularity; for log-spaced boundaries that's typically <1
//! decade error which suffices for latency dashboards.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const HISTOGRAM_MAGIC: u64 = 0x4150_4849_5354_3031;

#[repr(C, align(64))]
pub struct HistogramHeader {
    pub magic: u64,
    pub n_boundaries: u32,
    _pad1: u32,
    pub total_count: AtomicU64,
    _pad2: [u8; 40],
}

const _: () = {
    assert!(size_of::<HistogramHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramError {
    EmptyBoundaries,
    NonMonotonicBoundaries,
    LayoutMismatch,
    OutOfBounds,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for HistogramError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub const fn histogram_file_size(n_boundaries: usize) -> usize {
    size_of::<HistogramHeader>()
        + n_boundaries * size_of::<u64>()
        + (n_boundaries + 1) * size_of::<AtomicU64>()
}

pub struct SharedHistogram {
    _file: File,
    mmap: MmapMut,
    n_boundaries: usize,
    boundaries_offset: usize,
    counters_offset: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedHistogram {}
unsafe impl Sync for SharedHistogram {}

impl subetha_sidecar::AdaptiveInstance for SharedHistogram {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedHistogram {
    pub fn create(
        path: impl AsRef<Path>, boundaries: &[u64],
    ) -> Result<Self, HistogramError> {
        if boundaries.is_empty() {
            return Err(HistogramError::EmptyBoundaries);
        }
        for w in boundaries.windows(2) {
            if w[0] >= w[1] {
                return Err(HistogramError::NonMonotonicBoundaries);
            }
        }
        let n_boundaries = boundaries.len();
        let total = histogram_file_size(n_boundaries);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut HistogramHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<HistogramHeader>());
            (*hdr).magic = HISTOGRAM_MAGIC;
            (*hdr).n_boundaries = n_boundaries as u32;
        }
        let boundaries_offset = size_of::<HistogramHeader>();
        let counters_offset = boundaries_offset + std::mem::size_of_val(boundaries);
        // Write boundaries.
        unsafe {
            let dst = mmap.as_mut_ptr().add(boundaries_offset) as *mut u64;
            std::ptr::copy_nonoverlapping(boundaries.as_ptr(), dst, n_boundaries);
        }
        // Counters are already zero from set_len + map_mut.
        Ok(Self {
            _file: file, mmap, n_boundaries,
            boundaries_offset, counters_offset,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_boundaries: &[u64],
    ) -> Result<Self, HistogramError> {
        let n_boundaries = expected_boundaries.len();
        if n_boundaries == 0 { return Err(HistogramError::EmptyBoundaries); }
        let total = histogram_file_size(n_boundaries);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(HistogramError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const HistogramHeader) };
        if hdr.magic != HISTOGRAM_MAGIC || hdr.n_boundaries != n_boundaries as u32 {
            return Err(HistogramError::LayoutMismatch);
        }
        let boundaries_offset = size_of::<HistogramHeader>();
        let counters_offset = boundaries_offset + std::mem::size_of_val(expected_boundaries);
        // Verify stored boundaries match expected.
        let stored = unsafe {
            std::slice::from_raw_parts(
                mmap.as_ptr().add(boundaries_offset) as *const u64,
                n_boundaries,
            )
        };
        if stored != expected_boundaries {
            return Err(HistogramError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, n_boundaries,
            boundaries_offset, counters_offset,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn header(&self) -> &HistogramHeader {
        unsafe { &*(self.mmap.as_ptr() as *const HistogramHeader) }
    }

    fn boundaries(&self) -> &[u64] {
        unsafe {
            std::slice::from_raw_parts(
                self.mmap.as_ptr().add(self.boundaries_offset) as *const u64,
                self.n_boundaries,
            )
        }
    }

    fn counter(&self, bucket_idx: usize) -> &AtomicU64 {
        let base = unsafe { self.mmap.as_ptr().add(self.counters_offset) };
        unsafe { &*(base.add(bucket_idx * size_of::<AtomicU64>()) as *const AtomicU64) }
    }

    /// Number of buckets (n_boundaries + 1).
    pub fn n_buckets(&self) -> usize { self.n_boundaries + 1 }

    /// Total count across all buckets.
    pub fn total_count(&self) -> u64 {
        self.header().total_count.load(Ordering::Acquire)
    }

    /// Find the bucket index for `value`. Binary search on boundaries.
    pub fn bucket_for(&self, value: u64) -> usize {
        let bounds = self.boundaries();
        // partition_point returns the first index where the predicate
        // is false. With `|&b| b <= value`, it returns the first
        // boundary GREATER than value, i.e., the bucket index.
        bounds.partition_point(|&b| b <= value)
    }

    /// Record one observation of `value`. Atomically increments the
    /// matching bucket counter and the total. Returns the bucket
    /// index it landed in.
    pub fn record(&self, value: u64) -> usize {
        let idx = self.bucket_for(value);
        self.counter(idx).fetch_add(1, Ordering::AcqRel);
        self.header().total_count.fetch_add(1, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::histogram::OP_RECORD, 0);
        idx
    }

    /// Read a specific bucket's count.
    pub fn count(&self, bucket_idx: usize) -> Result<u64, HistogramError> {
        if bucket_idx >= self.n_buckets() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::histogram::OP_COUNT, 1);
            return Err(HistogramError::OutOfBounds);
        }
        let v = self.counter(bucket_idx).load(Ordering::Acquire);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::histogram::OP_COUNT, 0);
        Ok(v)
    }

    /// Snapshot all bucket counts as a Vec.
    pub fn counts(&self) -> Vec<u64> {
        (0..self.n_buckets())
            .map(|i| self.counter(i).load(Ordering::Acquire))
            .collect()
    }

    /// Get the boundaries vector (copy).
    pub fn boundaries_vec(&self) -> Vec<u64> {
        self.boundaries().to_vec()
    }

    /// Estimate the p-th percentile (p in 0.0..=1.0). Walks buckets
    /// accumulating counts until p of total is covered, then linearly
    /// interpolates within the target bucket. Returns 0 if total is 0.
    pub fn percentile(&self, p: f64) -> u64 {
        let p = p.clamp(0.0, 1.0);
        let total = self.total_count();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::histogram::OP_PERCENTILE,
            if total == 0 { 2 } else { 0 },
        );
        if total == 0 { return 0; }
        let target = (total as f64 * p).round() as u64;
        let mut acc = 0u64;
        let counts = self.counts();
        let bounds = self.boundaries();
        for (i, &c) in counts.iter().enumerate() {
            let new_acc = acc.saturating_add(c);
            if new_acc >= target {
                // Target falls in bucket i. Interpolate within.
                let lo = if i == 0 { 0 } else { bounds[i - 1] };
                let hi = if i < bounds.len() { bounds[i] } else { lo.saturating_mul(2) };
                if c == 0 { return lo; }
                let frac = (target - acc) as f64 / c as f64;
                return lo + ((hi - lo) as f64 * frac) as u64;
            }
            acc = new_acc;
        }
        // Shouldn't reach; return last boundary as fallback.
        bounds.last().copied().unwrap_or(0)
    }

    /// Reset all counters to 0. Not concurrency-coordinated; expect
    /// transient race with concurrent recorders.
    pub fn reset(&self) {
        for i in 0..self.n_buckets() {
            self.counter(i).store(0, Ordering::Release);
        }
        self.header().total_count.store(0, Ordering::Release);
    }

    pub fn flush(&self) -> Result<(), HistogramError> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), HistogramError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-histogram-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let h = SharedHistogram::create(&p, &[10, 100, 1000]).unwrap();
        assert_eq!(h.n_buckets(), 4);  // 3 boundaries -> 4 buckets
        assert_eq!(h.total_count(), 0);
        for i in 0..h.n_buckets() {
            assert_eq!(h.count(i).unwrap(), 0);
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_boundaries_rejected() {
        let p = tmp("empty");
        assert_eq!(
            SharedHistogram::create(&p, &[]).err(),
            Some(HistogramError::EmptyBoundaries)
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn non_monotonic_boundaries_rejected() {
        let p = tmp("non-mono");
        assert_eq!(
            SharedHistogram::create(&p, &[10, 5, 20]).err(),
            Some(HistogramError::NonMonotonicBoundaries)
        );
        assert_eq!(
            SharedHistogram::create(&p, &[10, 10]).err(),
            Some(HistogramError::NonMonotonicBoundaries)
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn bucket_assignment_correct() {
        let p = tmp("bucket-assign");
        // Boundaries: [10, 100, 1000]
        // Bucket 0: value < 10
        // Bucket 1: 10 <= value < 100
        // Bucket 2: 100 <= value < 1000
        // Bucket 3: value >= 1000
        let h = SharedHistogram::create(&p, &[10, 100, 1000]).unwrap();
        assert_eq!(h.bucket_for(0), 0);
        assert_eq!(h.bucket_for(9), 0);
        assert_eq!(h.bucket_for(10), 1);
        assert_eq!(h.bucket_for(99), 1);
        assert_eq!(h.bucket_for(100), 2);
        assert_eq!(h.bucket_for(999), 2);
        assert_eq!(h.bucket_for(1000), 3);
        assert_eq!(h.bucket_for(u64::MAX), 3);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn record_increments_correct_bucket() {
        let p = tmp("record");
        let h = SharedHistogram::create(&p, &[10, 100, 1000]).unwrap();
        let inputs = [5, 5, 50, 500, 5000, 50_000];
        for &v in &inputs {
            h.record(v);
        }
        // Bucket 0 (< 10): 2 (the two 5s)
        // Bucket 1 (10..100): 1 (the 50)
        // Bucket 2 (100..1000): 1 (the 500)
        // Bucket 3 (>= 1000): 2 (5000 and 50_000)
        assert_eq!(h.count(0).unwrap(), 2);
        assert_eq!(h.count(1).unwrap(), 1);
        assert_eq!(h.count(2).unwrap(), 1);
        assert_eq!(h.count(3).unwrap(), 2);
        assert_eq!(h.total_count(), 6);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn record_returns_bucket_index() {
        let p = tmp("record-idx");
        let h = SharedHistogram::create(&p, &[10, 100]).unwrap();
        assert_eq!(h.record(5), 0);
        assert_eq!(h.record(50), 1);
        assert_eq!(h.record(500), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn counts_snapshot_returns_all_buckets() {
        let p = tmp("counts");
        let h = SharedHistogram::create(&p, &[10, 100]).unwrap();
        h.record(5);
        h.record(50);
        h.record(50);
        h.record(500);
        h.record(500);
        h.record(500);
        let counts = h.counts();
        assert_eq!(counts, vec![1, 2, 3]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn percentile_basic() {
        let p = tmp("p-basic");
        let h = SharedHistogram::create(&p, &[10, 100, 1000]).unwrap();
        // 100 values uniformly in 0..10. All land in bucket 0.
        for v in 0..100u64 { h.record(v % 10); }
        // p50 should be ~5 (within bucket 0 interpolation).
        let p50 = h.percentile(0.5);
        assert!(p50 <= 10, "p50 {p50} should be in bucket 0 (<10)");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn percentile_zero_total_returns_zero() {
        let p = tmp("p-zero");
        let h = SharedHistogram::create(&p, &[10]).unwrap();
        assert_eq!(h.percentile(0.5), 0);
        assert_eq!(h.percentile(0.99), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn reset_clears_all_buckets() {
        let p = tmp("reset");
        let h = SharedHistogram::create(&p, &[10, 100]).unwrap();
        for _ in 0..5 { h.record(50); }
        assert_eq!(h.total_count(), 5);
        h.reset();
        assert_eq!(h.total_count(), 0);
        for i in 0..h.n_buckets() {
            assert_eq!(h.count(i).unwrap(), 0);
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer = SharedHistogram::create(&p, &[10, 100, 1000]).unwrap();
        let reader = SharedHistogram::open(&p, &[10, 100, 1000]).unwrap();
        writer.record(50);
        writer.record(500);
        assert_eq!(reader.count(1).unwrap(), 1);
        assert_eq!(reader.count(2).unwrap(), 1);
        assert_eq!(reader.total_count(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_with_wrong_boundaries_rejected() {
        let p = tmp("wrong-bounds");
        let _w = SharedHistogram::create(&p, &[10, 100]).unwrap();
        assert!(matches!(
            SharedHistogram::open(&p, &[10, 200]),
            Err(HistogramError::LayoutMismatch)
        ));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_recorders_no_lost_updates() {
        let p = tmp("concurrent");
        let h: Arc<SharedHistogram>
            = Arc::new(SharedHistogram::create(&p, &[10, 100, 1000]).unwrap());
        let n_threads = 4;
        let per_thread = 250;
        let mut handles = vec![];
        for t in 0..n_threads {
            let h = h.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    // Distribute across buckets via modulo.
                    let value = match (t * per_thread + i) % 4 {
                        0 => 5,    // bucket 0
                        1 => 50,   // bucket 1
                        2 => 500,  // bucket 2
                        _ => 5000, // bucket 3
                    };
                    h.record(value);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let total = n_threads * per_thread;
        assert_eq!(h.total_count() as usize, total);
        let counts = h.counts();
        // Each bucket should have ~total/4 records.
        let expected_per = (total / 4) as u64;
        for (i, &c) in counts.iter().enumerate() {
            assert_eq!(c, expected_per,
                "bucket {i} count {c} should be {expected_per}");
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        let bounds = vec![10u64, 100, 1000];
        {
            let h = SharedHistogram::create(&p, &bounds).unwrap();
            h.record(5);
            h.record(50);
            h.record(50);
            h.record(5000);
            h.flush().unwrap();
        }
        let h2 = SharedHistogram::open(&p, &bounds).unwrap();
        assert_eq!(h2.count(0).unwrap(), 1);
        assert_eq!(h2.count(1).unwrap(), 2);
        assert_eq!(h2.count(2).unwrap(), 0);
        assert_eq!(h2.count(3).unwrap(), 1);
        assert_eq!(h2.total_count(), 4);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn latency_distribution_pattern() {
        // Realistic latency histogram: log-spaced boundaries in us.
        let p = tmp("latency");
        let bounds = vec![10u64, 100, 1_000, 10_000, 100_000, 1_000_000];
        let h = SharedHistogram::create(&p, &bounds).unwrap();
        // Simulate 1000 measurements; mostly fast, some tail.
        for i in 0..1000u64 {
            let latency_us = match i % 100 {
                0..=80 => 5 + (i % 5),    // 81% under 10us
                81..=95 => 50 + (i % 50), // 15% in 10..100us
                _ => 500 + (i * 10),      // tail
            };
            h.record(latency_us);
        }
        // p50 should be in bucket 0 (< 10us).
        let p50 = h.percentile(0.5);
        assert!(p50 < 10, "p50 {p50} should be under 10us");
        // p99 should be much higher (in the tail).
        let p99 = h.percentile(0.99);
        assert!(p99 > 100, "p99 {p99} should be over 100us");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn count_out_of_bounds_rejected() {
        let p = tmp("oob");
        let h = SharedHistogram::create(&p, &[10]).unwrap();
        // 2 buckets total (indices 0 and 1).
        assert_eq!(h.count(2).err(), Some(HistogramError::OutOfBounds));
        std::fs::remove_file(&p).ok();
    }
}
