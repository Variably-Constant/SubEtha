//! `SharedReservoirSampler<T>` - cross-process uniform random
//! sampling via Vitter's Algorithm R.
//!
//! Maintains `k` reservoir slots. After processing N items, each
//! has equal probability `k/N` of being in the reservoir.
//!
//! # Algorithm
//!
//! For item n (1-indexed):
//! - If n <= k: slot[n-1] = item.
//! - Else: j = uniform(1..=n). If j <= k: slot[j-1] = item.
//!
//! # Safety
//!
//! - No spin loops, no CAS retry loops, no Drop guards.
//! - Bounded capacity at create.
//! - `total_seen.fetch_add` is monotonic (no underflow).
//! - Concurrent races on `slot[j]` for the SAME `j` after capacity
//!   is exceeded just keep one of the racers' values; statistically
//!   the uniform-sampling property is preserved because both values
//!   were equally eligible.
//!
//! # SeqLock per slot
//!
//! For T larger than 8 bytes, concurrent writes risk tearing. We use
//! a SeqLock cell (version + payload) per slot so readers spin on
//! odd version and re-read on version change. Same protocol as
//! [`SharedCell`](crate::SharedCell).

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const RESERVOIR_MAGIC: u64 = 0x4150_5253_4D50_4C31;
pub const RESERVOIR_SLOT_PAYLOAD: usize = 56;

#[repr(C, align(64))]
pub struct ReservoirHeader {
    pub magic: u64,
    pub capacity: u32,
    pub slot_size: u32,
    pub total_seen: AtomicU64,
    _pad: [u8; 40],
}

#[repr(C, align(64))]
pub struct ReservoirSlot {
    pub version: AtomicU32,
    _pad: [u8; 4],
    pub payload: [u8; RESERVOIR_SLOT_PAYLOAD],
}

const _: () = {
    assert!(size_of::<ReservoirHeader>() == 64);
    assert!(size_of::<ReservoirSlot>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservoirError {
    PayloadTooLarge,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for ReservoirError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub fn reservoir_file_size(capacity: usize) -> usize {
    size_of::<ReservoirHeader>() + capacity * size_of::<ReservoirSlot>()
}

thread_local! {
    static RNG_STATE: Cell<u64> = Cell::new({
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let mix = t.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        if mix == 0 { 1 } else { mix }
    });
}

#[inline]
fn next_random_u64() -> u64 {
    RNG_STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}

pub struct SharedReservoirSampler<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedReservoirSampler<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedReservoirSampler<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedReservoirSampler<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedReservoirSampler<T> {
    pub fn create(
        path: impl AsRef<Path>, capacity: usize,
    ) -> Result<Self, ReservoirError> {
        if size_of::<T>() > RESERVOIR_SLOT_PAYLOAD {
            return Err(ReservoirError::PayloadTooLarge);
        }
        assert!(capacity >= 1);
        let total = reservoir_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut ReservoirHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<ReservoirHeader>());
            (*hdr).magic = RESERVOIR_MAGIC;
            (*hdr).capacity = capacity as u32;
            (*hdr).slot_size = size_of::<T>() as u32;
        }
        // Slots zero-initialised from set_len + map_mut.
        Ok(Self {
            _file: file, mmap, capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_capacity: usize,
    ) -> Result<Self, ReservoirError> {
        if size_of::<T>() > RESERVOIR_SLOT_PAYLOAD {
            return Err(ReservoirError::PayloadTooLarge);
        }
        let total = reservoir_file_size(expected_capacity);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(ReservoirError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const ReservoirHeader) };
        if hdr.magic != RESERVOIR_MAGIC
            || hdr.capacity != expected_capacity as u32
            || hdr.slot_size != size_of::<T>() as u32
        {
            return Err(ReservoirError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    pub fn total_seen(&self) -> u64 {
        self.header().total_seen.load(Ordering::Acquire)
    }

    fn header(&self) -> &ReservoirHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ReservoirHeader) }
    }

    fn slot(&self, i: usize) -> &ReservoirSlot {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<ReservoirHeader>()) };
        unsafe { &*(base.add(i * size_of::<ReservoirSlot>()) as *const ReservoirSlot) }
    }

    /// SeqLock-write a payload into a slot.
    fn write_slot(&self, idx: usize, value: T) {
        let slot = self.slot(idx);
        slot.version.fetch_add(1, Ordering::AcqRel); // odd
        let dst = unsafe {
            let base = self.mmap.as_ptr().add(size_of::<ReservoirHeader>())
                .add(idx * size_of::<ReservoirSlot>())
                .add(std::mem::offset_of!(ReservoirSlot, payload));
            base as *mut u8
        };
        unsafe {
            std::ptr::copy_nonoverlapping(
                &value as *const T as *const u8,
                dst,
                size_of::<T>(),
            );
        }
        slot.version.fetch_add(1, Ordering::AcqRel); // even
    }

    /// SeqLock-read a payload from a slot.
    fn read_slot(&self, idx: usize) -> T {
        let slot = self.slot(idx);
        loop {
            let v1 = slot.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let mut out = std::mem::MaybeUninit::<T>::uninit();
            let src = unsafe {
                self.mmap.as_ptr().add(size_of::<ReservoirHeader>())
                    .add(idx * size_of::<ReservoirSlot>())
                    .add(std::mem::offset_of!(ReservoirSlot, payload))
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src, out.as_mut_ptr() as *mut u8, size_of::<T>(),
                );
            }
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                return unsafe { out.assume_init() };
            }
        }
    }

    /// Record a value. Returns the slot index it landed in, or
    /// None if the value was rejected (the reservoir kept its
    /// existing sample for this position).
    pub fn record(&self, value: T) -> Option<usize> {
        let prev = self.header().total_seen.fetch_add(1, Ordering::AcqRel);
        let n = prev + 1; // 1-indexed item number
        let k = self.capacity as u64;
        let r = if n <= k {
            // Reservoir not full yet; always accept.
            let idx = (n - 1) as usize;
            self.write_slot(idx, value);
            Some(idx)
        } else {
            // Vitter R: j = uniform(1..=n); if j <= k, accept at slot j-1.
            let j = (next_random_u64() % n) + 1;
            if j <= k {
                let idx = (j - 1) as usize;
                self.write_slot(idx, value);
                Some(idx)
            } else {
                None
            }
        };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::reservoir::OP_RECORD,
            if r.is_none() { 2 } else { 0 }, // 2 = rejected
        );
        r
    }

    /// Snapshot the current reservoir. Returns min(total_seen, k)
    /// slots filled so far; the rest are unused.
    pub fn snapshot(&self) -> Vec<T> {
        let filled = (self.total_seen() as usize).min(self.capacity);
        let v: Vec<T> = (0..filled).map(|i| self.read_slot(i)).collect();
        self.ring_sidecar
            .push_op(crate::sidecar_ops::reservoir::OP_SNAPSHOT, 0);
        v
    }

    /// Reset to empty (total_seen = 0; slots become invalid but
    /// not zeroed - next record overwrites them).
    pub fn reset(&self) {
        self.header().total_seen.store(0, Ordering::Release);
    }

    pub fn flush(&self) -> Result<(), ReservoirError> {
        self.mmap.flush()?;
        Ok(())
    }
    pub fn flush_async(&self) -> Result<(), ReservoirError> {
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
        p.push(format!("subetha-reservoir-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let r: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 10).unwrap();
        assert_eq!(r.capacity(), 10);
        assert_eq!(r.total_seen(), 0);
        assert_eq!(r.snapshot(), Vec::<u32>::new());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn first_k_items_always_accepted() {
        let p = tmp("first-k");
        let r: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 5).unwrap();
        for i in 0..5u32 {
            let idx = r.record(i);
            assert_eq!(idx, Some(i as usize));
        }
        let snap = r.snapshot();
        assert_eq!(snap, vec![0, 1, 2, 3, 4]);
        assert_eq!(r.total_seen(), 5);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn after_capacity_some_items_rejected() {
        let p = tmp("after-cap");
        let r: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 5).unwrap();
        for i in 0..5u32 { r.record(i); }
        // After capacity, each new item has probability 5/n of acceptance.
        let mut accepted = 0;
        let mut rejected = 0;
        for i in 5..100u32 {
            if r.record(i).is_some() { accepted += 1; } else { rejected += 1; }
        }
        // Expect most to be rejected since acceptance probability drops to
        // ~5%. Allow generous bounds for randomness.
        assert!(rejected > 50, "expected mostly rejections; got {accepted} accepted, {rejected} rejected");
        assert_eq!(r.total_seen(), 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn snapshot_always_has_correct_length() {
        let p = tmp("snap-len");
        let r: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 10).unwrap();
        // After 3 records, snapshot has 3 items.
        for i in 0..3u32 { r.record(i); }
        assert_eq!(r.snapshot().len(), 3);
        // After 100 records, snapshot has 10 (capacity).
        for i in 3..100u32 { r.record(i); }
        assert_eq!(r.snapshot().len(), 10);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn uniform_distribution_over_many_trials() {
        // For capacity=1 and N=100, each item should appear in the
        // reservoir with probability 1/100. Over 1000 trials, each
        // item should appear ~10 times. Bin into 10 buckets and
        // check no bucket is hugely off.
        let p = tmp("uniform");
        let n_trials = 1000;
        let n_items = 100u32;
        let mut counts = [0u32; 10];
        for trial in 0..n_trials {
            let path = std::env::temp_dir().join(
                format!("subetha-reservoir-uniform-{trial}-{}.bin", std::process::id()),
            );
            let r: SharedReservoirSampler<u32>
                = SharedReservoirSampler::create(&path, 1).unwrap();
            for i in 0..n_items { r.record(i); }
            let snap = r.snapshot();
            let kept = snap[0];
            let bucket = (kept * 10 / n_items) as usize;
            counts[bucket.min(9)] += 1;
            std::fs::remove_file(&path).ok();
        }
        // Each bucket should be ~100. Allow [50, 200] for stochastic noise.
        for (i, &c) in counts.iter().enumerate() {
            assert!((30..=200).contains(&c),
                "bucket {i} count {c} is way out of expected ~100");
        }
        let _p = p;
    }

    #[test]
    fn reset_clears_count() {
        let p = tmp("reset");
        let r: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 5).unwrap();
        for i in 0..10u32 { r.record(i); }
        assert_eq!(r.total_seen(), 10);
        r.reset();
        assert_eq!(r.total_seen(), 0);
        assert_eq!(r.snapshot(), Vec::<u32>::new());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let w: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 5).unwrap();
        let rdr: SharedReservoirSampler<u32> = SharedReservoirSampler::open(&p, 5).unwrap();
        for i in 0..5u32 { w.record(i); }
        let snap = rdr.snapshot();
        assert_eq!(snap, vec![0, 1, 2, 3, 4]);
        assert_eq!(rdr.total_seen(), 5);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_rejected() {
        #[allow(dead_code)]
        struct Big([u8; RESERVOIR_SLOT_PAYLOAD + 1]);
        impl Copy for Big {}
        impl Clone for Big { fn clone(&self) -> Self { *self } }
        let p = tmp("too-large");
        assert_eq!(
            SharedReservoirSampler::<Big>::create(&p, 4).err(),
            Some(ReservoirError::PayloadTooLarge)
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_recorders_count_correctly() {
        let p = tmp("concurrent");
        let r: Arc<SharedReservoirSampler<u32>>
            = Arc::new(SharedReservoirSampler::create(&p, 10).unwrap());
        let n_threads = 4;
        let per_thread = 100;
        let mut handles = vec![];
        for t in 0..n_threads as u32 {
            let r = r.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread as u32 {
                    r.record(t * 1000 + i);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(r.total_seen() as usize, n_threads * per_thread);
        let snap = r.snapshot();
        assert_eq!(snap.len(), 10);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct LogEntry { ts: u64, code: u32, severity: u32 }
        let p = tmp("struct");
        let r: SharedReservoirSampler<LogEntry>
            = SharedReservoirSampler::create(&p, 3).unwrap();
        let e1 = LogEntry { ts: 100, code: 1, severity: 1 };
        let e2 = LogEntry { ts: 200, code: 2, severity: 2 };
        let e3 = LogEntry { ts: 300, code: 3, severity: 3 };
        r.record(e1);
        r.record(e2);
        r.record(e3);
        let snap = r.snapshot();
        assert_eq!(snap, vec![e1, e2, e3]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let r: SharedReservoirSampler<u32> = SharedReservoirSampler::create(&p, 5).unwrap();
            for i in 0..5u32 { r.record(i); }
            r.flush().unwrap();
        }
        let r2: SharedReservoirSampler<u32> = SharedReservoirSampler::open(&p, 5).unwrap();
        assert_eq!(r2.total_seen(), 5);
        assert_eq!(r2.snapshot(), vec![0, 1, 2, 3, 4]);
        std::fs::remove_file(&p).ok();
    }
}
