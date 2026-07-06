//! `SharedHyperLogLog` - cross-process probabilistic distinct-count
//! estimator.
//!
//! 2^p AtomicU8 registers, each storing the maximum-observed rank
//! (leading-zero count + 1) of items hashed to that register.
//! Estimate via harmonic mean with bias correction.
//!
//! # Why this is safe
//!
//! Each `insert` is exactly one `fetch_max` on one AtomicU8. No
//! CAS loops, no Drop guards, no spin waits. The cache-line
//! contention is bounded to one register per insert.
//!
//! # Accuracy
//!
//! Standard error ~= 1.04 / sqrt(m) where m = 2^p.
//!
//! | p  | m      | std err | size  |
//! |----|--------|---------|-------|
//! | 8  | 256    | 6.5%    | 256B  |
//! | 10 | 1024   | 3.3%    | 1 KB  |
//! | 12 | 4096   | 1.6%    | 4 KB  |
//! | 14 | 16384  | 0.8%    | 16 KB |
//! | 16 | 65536  | 0.4%    | 64 KB |
//!
//! # Encoding
//!
//! Hash item to u64 h. Register index = top p bits of h. Rank =
//! (leading_zeros of (h << p) | (1 << (63-p))) + 1, clamped to
//! 64. (The OR ensures rank is bounded even when low bits are 0.)

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const HLL_MAGIC: u64 = 0x4150_484C_4C56_3031;
/// Minimum precision (smaller = less memory but worse accuracy).
pub const MIN_PRECISION: u8 = 4;
/// Maximum precision (larger = more memory).
pub const MAX_PRECISION: u8 = 16;

#[repr(C, align(64))]
pub struct HLLHeader {
    pub magic: u64,
    pub precision: u32,
    pub m: u32,
    _pad: [u8; 48],
}

const _: () = {
    assert!(size_of::<HLLHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HLLError {
    InvalidPrecision,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for HLLError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x100_0000_01b3;

#[inline]
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET_BASIS;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// MurmurHash3 fmix64 finalizer. Bit-mixes a u64 to give excellent
/// distribution properties (avalanche: 1-bit input flip => ~50%
/// of output bits flip). Critical for HLL because the register
/// index is extracted from the TOP bits of the hash, and raw FNV-1a
/// on short inputs has poor top-bit distribution.
#[inline]
fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[inline]
fn hash_for_hll(item: &[u8]) -> u64 {
    fmix64(fnv1a_64(item))
}

pub fn hll_file_size(precision: u8) -> usize {
    size_of::<HLLHeader>() + (1usize << precision)
}

pub struct SharedHyperLogLog {
    _file: File,
    mmap: MmapMut,
    precision: u8,
    m: u32,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedHyperLogLog {}
unsafe impl Sync for SharedHyperLogLog {}

impl subetha_sidecar::AdaptiveInstance for SharedHyperLogLog {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedHyperLogLog {
    pub fn create(
        path: impl AsRef<Path>, precision: u8,
    ) -> Result<Self, HLLError> {
        if !(MIN_PRECISION..=MAX_PRECISION).contains(&precision) {
            return Err(HLLError::InvalidPrecision);
        }
        let m = 1u32 << precision;
        let total = hll_file_size(precision);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut HLLHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<HLLHeader>());
            (*hdr).magic = HLL_MAGIC;
            (*hdr).precision = precision as u32;
            (*hdr).m = m;
        }
        // Registers are zero from set_len + map_mut.
        Ok(Self {
            _file: file, mmap, precision, m,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_precision: u8,
    ) -> Result<Self, HLLError> {
        let total = hll_file_size(expected_precision);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(HLLError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const HLLHeader) };
        if hdr.magic != HLL_MAGIC || hdr.precision != expected_precision as u32 {
            return Err(HLLError::LayoutMismatch);
        }
        let m = hdr.m;
        Ok(Self {
            _file: file, mmap, precision: expected_precision, m,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn precision(&self) -> u8 { self.precision }
    #[inline]
    pub fn n_registers(&self) -> u32 { self.m }

    fn register(&self, idx: usize) -> &AtomicU8 {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<HLLHeader>()) };
        unsafe { &*(base.add(idx) as *const AtomicU8) }
    }

    /// Insert an item. One fetch_max on one register; no spinning.
    pub fn insert(&self, item: &[u8]) {
        let h = hash_for_hll(item);
        let p = self.precision as u32;
        let reg_idx = (h >> (64 - p)) as usize;
        // Compute rank: position of leftmost 1-bit in the low
        // (64 - p) bits. We OR in a guard bit so rank is always
        // bounded by 64 - p + 1.
        let w = (h << p) | (1u64 << (p.saturating_sub(1)));
        let rank = (w.leading_zeros() as u8) + 1;
        self.register(reg_idx).fetch_max(rank, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_INSERT, 0);
    }

    /// Estimate cardinality (distinct count) via harmonic mean with
    /// bias correction.
    pub fn estimate(&self) -> u64 {
        let m = self.m as f64;
        let alpha = match self.m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for i in 0..self.m as usize {
            let r = self.register(i).load(Ordering::Acquire);
            if r == 0 { zeros += 1; }
            sum += 2f64.powi(-(r as i32));
        }
        let raw = alpha * m * m / sum;
        // Small-range correction: if estimate < 2.5 * m and there
        // are empty registers, use linear counting.
        let v = if raw <= 2.5 * m && zeros > 0 {
            let z = zeros as f64;
            (m * (m / z).ln()).round() as u64
        } else {
            raw.round() as u64
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_QUERY, 0);
        v
    }

    /// Reset all registers to zero (the empty state).
    pub fn reset(&self) {
        for i in 0..self.m as usize {
            self.register(i).store(0, Ordering::Release);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_CLEAR, 0);
    }

    pub fn flush(&self) -> Result<(), HLLError> {
        self.mmap.flush()?;
        Ok(())
    }
    pub fn flush_async(&self) -> Result<(), HLLError> {
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
        p.push(format!("subetha-hll-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_empty_estimate_is_zero() {
        let p = tmp("init");
        let h = SharedHyperLogLog::create(&p, 12).unwrap();
        assert_eq!(h.precision(), 12);
        assert_eq!(h.n_registers(), 4096);
        assert_eq!(h.estimate(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn invalid_precision_rejected() {
        let p = tmp("invalid");
        assert_eq!(
            SharedHyperLogLog::create(&p, 3).err(),
            Some(HLLError::InvalidPrecision)
        );
        assert_eq!(
            SharedHyperLogLog::create(&p, 17).err(),
            Some(HLLError::InvalidPrecision)
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn single_insert_estimate_is_one() {
        let p = tmp("single");
        let h = SharedHyperLogLog::create(&p, 12).unwrap();
        h.insert(b"hello");
        let est = h.estimate();
        // Linear-counting small-range correction handles single
        // insert; estimate should be exactly 1.
        assert_eq!(est, 1, "single insert should estimate 1, got {est}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn idempotent_inserts_same_item() {
        let p = tmp("idempotent");
        let h = SharedHyperLogLog::create(&p, 12).unwrap();
        for _ in 0..100 { h.insert(b"same-item"); }
        let est = h.estimate();
        assert_eq!(est, 1, "100 inserts of same item should estimate 1, got {est}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn distinct_count_within_error_bound_p12() {
        // p=12 -> m=4096, std err 1.6%. For 1000 distinct items,
        // expect estimate in roughly [950, 1050] (2 sigma).
        let p = tmp("distinct-1k");
        let h = SharedHyperLogLog::create(&p, 12).unwrap();
        for i in 0..1000u32 {
            h.insert(format!("item-{i:05}").as_bytes());
        }
        let est = h.estimate();
        assert!(
            (900..=1100).contains(&est),
            "estimate {est} should be within 10% of 1000 (got {est})",
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn distinct_count_within_error_bound_p14() {
        // p=14 -> m=16384, std err 0.8%. For 10000 distinct items,
        // expect estimate within ~4% (2.5 sigma).
        let p = tmp("distinct-10k");
        let h = SharedHyperLogLog::create(&p, 14).unwrap();
        for i in 0..10_000u32 {
            h.insert(format!("k{i:06}").as_bytes());
        }
        let est = h.estimate();
        assert!(
            (9600..=10400).contains(&est),
            "estimate {est} should be within 4% of 10000",
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn reset_clears_registers() {
        let p = tmp("reset");
        let h = SharedHyperLogLog::create(&p, 10).unwrap();
        for i in 0..100u32 { h.insert(format!("k{i}").as_bytes()); }
        assert!(h.estimate() > 0);
        h.reset();
        assert_eq!(h.estimate(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let w = SharedHyperLogLog::create(&p, 10).unwrap();
        let r = SharedHyperLogLog::open(&p, 10).unwrap();
        for i in 0..50u32 { w.insert(format!("k{i}").as_bytes()); }
        let est_w = w.estimate();
        let est_r = r.estimate();
        assert_eq!(est_w, est_r);
        // Roughly 50.
        assert!((40..=60).contains(&est_r), "estimate {est_r} should be near 50");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn config_mismatch_at_open_rejected() {
        let p = tmp("mismatch");
        let _w = SharedHyperLogLog::create(&p, 10).unwrap();
        assert!(matches!(
            SharedHyperLogLog::open(&p, 12),
            Err(HLLError::LayoutMismatch)
        ));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_inserters_correct_estimate() {
        // 4 threads each insert 1000 distinct items (disjoint key
        // spaces). Total = 4000. Estimate should be near 4000.
        let p = tmp("concurrent");
        let h: Arc<SharedHyperLogLog>
            = Arc::new(SharedHyperLogLog::create(&p, 12).unwrap());
        let mut handles = vec![];
        for t in 0..4u32 {
            let h = h.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000u32 {
                    h.insert(format!("t{t}-i{i:05}").as_bytes());
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let est = h.estimate();
        assert!(
            (3700..=4300).contains(&est),
            "concurrent inserts: estimate {est} should be within ~7% of 4000",
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let h = SharedHyperLogLog::create(&p, 10).unwrap();
            for i in 0..100u32 { h.insert(format!("k{i}").as_bytes()); }
            h.flush().unwrap();
        }
        let h2 = SharedHyperLogLog::open(&p, 10).unwrap();
        let est = h2.estimate();
        assert!((80..=120).contains(&est),
            "reopened estimate {est} should be near 100");
        std::fs::remove_file(&p).ok();
    }
}
