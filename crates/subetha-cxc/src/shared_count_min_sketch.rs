//! `SharedCountMinSketch` - cross-process probabilistic frequency
//! estimator.
//!
//! `d` hash functions, `w` cells per row. `insert(item)` does `d`
//! atomic `fetch_add(1)` ops; `estimate_count(item)` reads `d`
//! cells and returns the minimum (the tightest unbiased upper
//! bound on true count).
//!
//! # Safety properties
//!
//! - Pure `fetch_add` writes - no underflow possible.
//! - No spin loops, no CAS retries, no RAII guards.
//! - Bounded memory at create time (`d * w` cells of u64).
//! - Hash collisions OVERCOUNT, never undercount. Estimate is a
//!   guaranteed upper bound on true count.
//!
//! # Error bound
//!
//! With probability `1 - delta`, estimate <= true + `epsilon * N`
//! where N is total inserts. Sizing: `w >= e/epsilon`,
//! `d >= ln(1/delta)`. Standard config (epsilon=0.001, delta=0.001):
//! w=2718, d=7, mem ~152 KB.
//!
//! # Hash family
//!
//! `d` independent hash positions derived from one FNV-1a + fmix64
//! hash, then `pos[i] = (h + i * h2) % w` using two-hash
//! double-hashing (Kirsch-Mitzenmacher). Same technique as
//! [`SharedBloomFilter`](crate::SharedBloomFilter).

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const CMS_MAGIC: u64 = 0x4150_434D_5330_3031;

#[repr(C, align(64))]
pub struct CMSHeader {
    pub magic: u64,
    pub d: u32,
    pub w: u32,
    pub total_inserts: AtomicU64,
    _pad: [u8; 40],
}

const _: () = {
    assert!(size_of::<CMSHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CMSError {
    InvalidConfig,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for CMSError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub fn cms_file_size(d: u32, w: u32) -> usize {
    size_of::<CMSHeader>() + (d as usize) * (w as usize) * size_of::<AtomicU64>()
}

const FNV_OFFSET_BASIS_1: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_BASIS_2: u64 = 0x8422_2325_cbf2_9ce4;
const FNV_PRIME: u64 = 0x100_0000_01b3;

#[inline]
fn fnv1a(bytes: &[u8], basis: u64) -> u64 {
    let mut h = basis;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

#[inline]
fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

pub struct SharedCountMinSketch {
    _file: File,
    mmap: MmapMut,
    d: u32,
    w: u32,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedCountMinSketch {}
unsafe impl Sync for SharedCountMinSketch {}

impl subetha_sidecar::AdaptiveInstance for SharedCountMinSketch {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedCountMinSketch {
    /// Suggest (d, w) for target (epsilon, delta) error bounds.
    /// Estimate <= true + epsilon * N with probability 1 - delta.
    pub fn suggest_config(epsilon: f64, delta: f64) -> (u32, u32) {
        assert!(epsilon > 0.0 && epsilon < 1.0);
        assert!(delta > 0.0 && delta < 1.0);
        let w = (std::f64::consts::E / epsilon).ceil() as u32;
        let d = (1.0 / delta).ln().ceil() as u32;
        (d.max(1), w.max(1))
    }

    pub fn create(
        path: impl AsRef<Path>, d: u32, w: u32,
    ) -> Result<Self, CMSError> {
        if d == 0 || w == 0 {
            return Err(CMSError::InvalidConfig);
        }
        let total = cms_file_size(d, w);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut CMSHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<CMSHeader>());
            (*hdr).magic = CMS_MAGIC;
            (*hdr).d = d;
            (*hdr).w = w;
        }
        Ok(Self {
            _file: file, mmap, d, w,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_d: u32, expected_w: u32,
    ) -> Result<Self, CMSError> {
        let total = cms_file_size(expected_d, expected_w);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(CMSError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const CMSHeader) };
        if hdr.magic != CMS_MAGIC || hdr.d != expected_d || hdr.w != expected_w {
            return Err(CMSError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, d: expected_d, w: expected_w,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn d(&self) -> u32 { self.d }
    #[inline]
    pub fn w(&self) -> u32 { self.w }
    #[inline]
    pub fn total_inserts(&self) -> u64 {
        unsafe { (*(self.mmap.as_ptr() as *const CMSHeader)).total_inserts.load(Ordering::Acquire) }
    }

    fn cell(&self, row: u32, col: u32) -> &AtomicU64 {
        let idx = (row as usize) * (self.w as usize) + (col as usize);
        let base = unsafe { self.mmap.as_ptr().add(size_of::<CMSHeader>()) };
        unsafe { &*(base.add(idx * size_of::<AtomicU64>()) as *const AtomicU64) }
    }

    /// Invoke `f(row, col)` for each of the `d` hash positions of `item`,
    /// without allocating. Two FNV-1a + fmix64 hashes feed
    /// Kirsch-Mitzenmacher double-hashing `h1 + row * h2`; the column is
    /// reduced mod `w`, using a bit-mask when `w` is a power of two (the
    /// runtime divisor otherwise compiles to a hardware DIV per row).
    #[inline]
    fn for_each_position(&self, item: &[u8], mut f: impl FnMut(u32, u32)) {
        let h1 = fmix64(fnv1a(item, FNV_OFFSET_BASIS_1));
        let h2 = fmix64(fnv1a(item, FNV_OFFSET_BASIS_2));
        let w = self.w as u64;
        let pow2 = self.w.is_power_of_two();
        let mask = w.wrapping_sub(1);
        for row in 0..self.d {
            let raw = h1.wrapping_add((row as u64).wrapping_mul(h2));
            let col = if pow2 {
                (raw & mask) as u32
            } else {
                // Lemire fastrange: multiply-shift maps into [0, w) with no
                // hardware DIV. h1/h2 are fmix64-avalanched, so the high
                // bits the shift keys on are well-distributed.
                ((raw as u128 * w as u128) >> 64) as u32
            };
            f(row, col);
        }
    }

    /// Insert one observation of `item`. `d` atomic increments.
    pub fn insert(&self, item: &[u8]) {
        self.for_each_position(item, |row, col| {
            self.cell(row, col).fetch_add(1, Ordering::AcqRel);
        });
        unsafe {
            (*(self.mmap.as_ptr() as *const CMSHeader))
                .total_inserts.fetch_add(1, Ordering::AcqRel);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_INSERT, 0);
    }

    /// Insert `count` observations at once (bulk increment).
    pub fn insert_n(&self, item: &[u8], count: u64) {
        if count == 0 { return; }
        self.for_each_position(item, |row, col| {
            self.cell(row, col).fetch_add(count, Ordering::AcqRel);
        });
        unsafe {
            (*(self.mmap.as_ptr() as *const CMSHeader))
                .total_inserts.fetch_add(count, Ordering::AcqRel);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_INSERT, 0);
    }

    /// Estimate the frequency of `item`. Guaranteed upper bound on
    /// true count.
    pub fn estimate_count(&self, item: &[u8]) -> u64 {
        let mut v = u64::MAX;
        self.for_each_position(item, |row, col| {
            let c = self.cell(row, col).load(Ordering::Acquire);
            if c < v { v = c; }
        });
        let v = if self.d == 0 { 0 } else { v };
        self.ring_sidecar.push_op(
            crate::sidecar_ops::sketch::OP_QUERY,
            if v == 0 { 2 } else { 0 },
        );
        v
    }

    /// Reset all cells to zero.
    pub fn reset(&self) {
        for row in 0..self.d {
            for col in 0..self.w {
                self.cell(row, col).store(0, Ordering::Release);
            }
        }
        unsafe {
            (*(self.mmap.as_ptr() as *const CMSHeader))
                .total_inserts.store(0, Ordering::Release);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_CLEAR, 0);
    }

    pub fn flush(&self) -> Result<(), CMSError> {
        self.mmap.flush()?;
        Ok(())
    }
    pub fn flush_async(&self) -> Result<(), CMSError> {
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
        p.push(format!("subetha-cms-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_zero() {
        let p = tmp("init");
        let cms = SharedCountMinSketch::create(&p, 4, 256).unwrap();
        assert_eq!(cms.d(), 4);
        assert_eq!(cms.w(), 256);
        assert_eq!(cms.total_inserts(), 0);
        assert_eq!(cms.estimate_count(b"anything"), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn invalid_config_rejected() {
        let p = tmp("invalid");
        assert_eq!(
            SharedCountMinSketch::create(&p, 0, 256).err(),
            Some(CMSError::InvalidConfig)
        );
        assert_eq!(
            SharedCountMinSketch::create(&p, 4, 0).err(),
            Some(CMSError::InvalidConfig)
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn insert_then_estimate_returns_at_least_true_count() {
        let p = tmp("insert");
        let cms = SharedCountMinSketch::create(&p, 4, 1024).unwrap();
        for _ in 0..5 { cms.insert(b"foo"); }
        for _ in 0..3 { cms.insert(b"bar"); }
        // Guaranteed: estimate >= true.
        assert!(cms.estimate_count(b"foo") >= 5);
        assert!(cms.estimate_count(b"bar") >= 3);
        assert_eq!(cms.total_inserts(), 8);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn insert_n_is_equivalent_to_n_inserts() {
        let p = tmp("insert-n");
        let a = SharedCountMinSketch::create(tmp("insert-n-a"), 4, 1024).unwrap();
        let b = SharedCountMinSketch::create(tmp("insert-n-b"), 4, 1024).unwrap();
        for _ in 0..100 { a.insert(b"item"); }
        b.insert_n(b"item", 100);
        assert_eq!(a.estimate_count(b"item"), b.estimate_count(b"item"));
        assert_eq!(a.total_inserts(), b.total_inserts());
        let _p = p;
        std::fs::remove_file(tmp("insert-n-a")).ok();
        std::fs::remove_file(tmp("insert-n-b")).ok();
    }

    #[test]
    fn estimate_for_absent_item_is_low() {
        // Probability of false-positive (estimate > 0) on absent
        // item is bounded by (n / w)^d. For n=100 inserts, w=1024,
        // d=4: (100/1024)^4 = 9e-5 - very low.
        let p = tmp("absent");
        let cms = SharedCountMinSketch::create(&p, 4, 1024).unwrap();
        for i in 0..100u32 {
            cms.insert(format!("inserted-{i}").as_bytes());
        }
        // Try 100 absent items; expect very few false positives.
        let mut fp = 0u32;
        for i in 0..100u32 {
            if cms.estimate_count(format!("absent-{i}").as_bytes()) > 0 {
                fp += 1;
            }
        }
        // Allow up to 10% (well above theoretical bound).
        assert!(fp < 10, "expected < 10 false positives, got {fp}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn heavy_hitter_detection() {
        // Insert 1 heavy item (10000 times) + 1000 single-insert
        // items. Heavy item should clearly stand out.
        let p = tmp("heavy");
        let cms = SharedCountMinSketch::create(&p, 5, 2048).unwrap();
        for _ in 0..10_000 { cms.insert(b"HEAVY"); }
        for i in 0..1000u32 {
            cms.insert(format!("light-{i}").as_bytes());
        }
        let heavy = cms.estimate_count(b"HEAVY");
        // Heavy is at least 10000; error <= epsilon * N where
        // N = 11000 and epsilon ~ e/2048 ~ 0.00133. So error <= ~15.
        assert!(heavy >= 10_000);
        assert!(heavy <= 10_100, "heavy estimate {heavy} should be very close to 10000");
        // Light items should estimate near 1.
        for i in 0..10u32 {
            let light = cms.estimate_count(format!("light-{i}").as_bytes());
            assert!(light < 20, "light item {i} estimate {light} should be small");
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn reset_zeroes_everything() {
        let p = tmp("reset");
        let cms = SharedCountMinSketch::create(&p, 4, 256).unwrap();
        for _ in 0..50 { cms.insert(b"x"); }
        assert!(cms.estimate_count(b"x") >= 50);
        cms.reset();
        assert_eq!(cms.estimate_count(b"x"), 0);
        assert_eq!(cms.total_inserts(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let w = SharedCountMinSketch::create(&p, 4, 256).unwrap();
        let r = SharedCountMinSketch::open(&p, 4, 256).unwrap();
        w.insert(b"shared");
        w.insert(b"shared");
        assert!(r.estimate_count(b"shared") >= 2);
        assert_eq!(r.total_inserts(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn config_mismatch_at_open_rejected() {
        let p = tmp("mismatch");
        let _w = SharedCountMinSketch::create(&p, 4, 256).unwrap();
        assert!(matches!(
            SharedCountMinSketch::open(&p, 5, 256),
            Err(CMSError::LayoutMismatch)
        ));
        assert!(matches!(
            SharedCountMinSketch::open(&p, 4, 512),
            Err(CMSError::LayoutMismatch)
        ));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn suggest_config_returns_sensible_values() {
        // epsilon=0.01, delta=0.01 -> w ~ e/0.01 ~ 272, d ~ ln(100) ~ 5.
        let (d, w) = SharedCountMinSketch::suggest_config(0.01, 0.01);
        assert!((250..=300).contains(&w));
        assert!((4..=6).contains(&d));
    }

    #[test]
    fn concurrent_inserters_accurate() {
        // 4 threads each insert "shared-item" 1000 times. After
        // join, estimate should be at least 4000.
        let p = tmp("concurrent");
        let cms = Arc::new(SharedCountMinSketch::create(&p, 4, 1024).unwrap());
        let mut handles = vec![];
        for _ in 0..4 {
            let cms = cms.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 { cms.insert(b"shared-item"); }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let est = cms.estimate_count(b"shared-item");
        assert!(est >= 4000, "concurrent estimate {est} should be >= 4000");
        assert_eq!(cms.total_inserts(), 4000);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let cms = SharedCountMinSketch::create(&p, 4, 256).unwrap();
            for _ in 0..50 { cms.insert(b"persisted"); }
            cms.flush().unwrap();
        }
        let cms2 = SharedCountMinSketch::open(&p, 4, 256).unwrap();
        assert!(cms2.estimate_count(b"persisted") >= 50);
        assert_eq!(cms2.total_inserts(), 50);
        std::fs::remove_file(&p).ok();
    }
}
