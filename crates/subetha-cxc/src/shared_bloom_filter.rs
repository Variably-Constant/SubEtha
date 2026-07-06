//! `SharedBloomFilter` - cross-process probabilistic set membership.
//!
//! Composite primitive: [`SharedBitVec`] +
//! `k` hash functions. Insert hashes the input `k` times and sets
//! those `k` bits; `contains` returns true if and only if all `k`
//! bits are set. No false negatives; false positives are possible
//! with a tunable rate.
//!
//! # Sizing rules of thumb
//!
//! For `n` distinct items and target false-positive rate `p`:
//! - Optimal `n_bits` = `-(n * ln(p)) / (ln(2)^2)`
//! - Optimal `n_hashes` = `(n_bits / n) * ln(2)`
//!
//! For example, n=10_000 items with p=0.01 (1% FPR):
//! `n_bits ~= 95_851`, `n_hashes ~= 7`.
//!
//! Use [`suggest_config`](SharedBloomFilter::suggest_config) to
//! compute these.
//!
//! # Cross-process angle
//!
//! Just a SharedBitVec wrapper. The underlying bit array is the
//! shared state; n_bits and n_hashes are header-stored config so
//! cross-handle opens verify they match.
//!
//! # Hash function: double-hashing FNV-1a
//!
//! We compute two FNV-1a hashes with different seeds, then derive
//! the k hash positions via `(h1 + i * h2) mod n_bits` for
//! i in 0..k. This is the standard Kirsch-Mitzenmacher
//! double-hashing technique that gives k effectively-independent
//! hash positions from only two underlying hash computations.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_bit_vec::{BitVecError, SharedBitVec};

pub const BLOOM_MAGIC: u64 = 0x4150_424C_4F4F_4D31;

#[repr(C, align(64))]
pub struct BloomHeader {
    pub magic: u64,
    pub n_bits: u64,
    pub n_hashes: u32,
    _pad: [u8; 44],
}

const _: () = {
    assert!(std::mem::size_of::<BloomHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomError {
    BitVec(BitVecError),
    LayoutMismatch,
    InvalidConfig,
    IoError(std::io::ErrorKind),
}

impl From<BitVecError> for BloomError {
    fn from(e: BitVecError) -> Self { Self::BitVec(e) }
}
impl From<std::io::Error> for BloomError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

fn header_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.bloom.bin"));
    p
}
fn bits_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.bits.bin"));
    p
}

pub struct SharedBloomFilter {
    _file: File,
    _mmap: MmapMut,
    bits: SharedBitVec,
    n_bits: u64,
    n_hashes: u32,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedBloomFilter {}
unsafe impl Sync for SharedBloomFilter {}

impl subetha_sidecar::AdaptiveInstance for SharedBloomFilter {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

const FNV_OFFSET_BASIS_1: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_BASIS_2: u64 = 0x84222325_cbf29ce4;
const FNV_PRIME: u64 = 0x100_0000_01b3;

#[inline]
fn fnv1a_seeded(bytes: &[u8], offset_basis: u64) -> u64 {
    let mut h = offset_basis;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// MurmurHash3 64-bit finalizer (avalanche). FNV-1a alone has weak
/// high-bit diffusion; the position mapping uses Lemire fastrange, which
/// keys on the high bits, so the seed hashes are avalanched here first.
/// This also tightens the achieved false-positive rate toward the
/// configured target.
#[inline]
fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

impl SharedBloomFilter {
    /// Suggest n_bits and n_hashes for a target false-positive rate
    /// `p` and expected item count `n`. Both rounded up.
    pub fn suggest_config(n_items: usize, p: f64) -> (usize, u32) {
        let n = n_items as f64;
        let n_bits = (-n * p.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2)).ceil() as usize;
        let n_hashes = ((n_bits as f64 / n) * std::f64::consts::LN_2).round() as u32;
        (n_bits.max(64), n_hashes.max(1))
    }

    pub fn create(
        base_path: impl AsRef<Path>, n_bits: usize, n_hashes: u32,
    ) -> Result<Self, BloomError> {
        if n_bits == 0 || n_hashes == 0 {
            return Err(BloomError::InvalidConfig);
        }
        let base = base_path.as_ref();
        let hpath = header_path(base);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&hpath)?;
        file.set_len(std::mem::size_of::<BloomHeader>() as u64)?;
        let mut mmap = unsafe {
            MmapOptions::new().len(std::mem::size_of::<BloomHeader>()).map_mut(&file)?
        };
        let hdr = mmap.as_mut_ptr() as *mut BloomHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, std::mem::size_of::<BloomHeader>());
            (*hdr).magic = BLOOM_MAGIC;
            (*hdr).n_bits = n_bits as u64;
            (*hdr).n_hashes = n_hashes;
        }
        let bits = SharedBitVec::create(bits_path(base), n_bits)?;
        Ok(Self {
            _file: file, _mmap: mmap, bits,
            n_bits: n_bits as u64, n_hashes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        base_path: impl AsRef<Path>, n_bits: usize, n_hashes: u32,
    ) -> Result<Self, BloomError> {
        let base = base_path.as_ref();
        let hpath = header_path(base);
        let file = OpenOptions::new().read(true).write(true).open(&hpath)?;
        if file.metadata()?.len() < std::mem::size_of::<BloomHeader>() as u64 {
            return Err(BloomError::LayoutMismatch);
        }
        let mmap = unsafe {
            MmapOptions::new().len(std::mem::size_of::<BloomHeader>()).map_mut(&file)?
        };
        let hdr = unsafe { &*(mmap.as_ptr() as *const BloomHeader) };
        if hdr.magic != BLOOM_MAGIC
            || hdr.n_bits != n_bits as u64
            || hdr.n_hashes != n_hashes
        {
            return Err(BloomError::LayoutMismatch);
        }
        let bits = SharedBitVec::open(bits_path(base), n_bits)?;
        Ok(Self {
            _file: file, _mmap: mmap, bits,
            n_bits: n_bits as u64, n_hashes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn n_bits(&self) -> u64 { self.n_bits }
    #[inline]
    pub fn n_hashes(&self) -> u32 { self.n_hashes }

    /// Insert an item. Sets `n_hashes` bits in the underlying
    /// vector. Idempotent: re-inserting the same item is a no-op
    /// for membership purposes (the bits stay set).
    /// Map the i-th Kirsch-Mitzenmacher double-hash to a bit index in
    /// `[0, n_bits)` via Lemire's fastrange (multiply-shift) instead of
    /// `% n_bits`: replaces a hardware DIV per hash with a multiply +
    /// shift and needs no power-of-two `n_bits`. insert and contains
    /// share this mapping, so membership stays consistent.
    #[inline]
    fn position(&self, h1: u64, h2: u64, i: u64) -> usize {
        let h = h1.wrapping_add(i.wrapping_mul(h2));
        ((h as u128 * self.n_bits as u128) >> 64) as usize
    }

    pub fn insert(&self, item: &[u8]) -> Result<(), BloomError> {
        let h1 = fmix64(fnv1a_seeded(item, FNV_OFFSET_BASIS_1));
        let h2 = fmix64(fnv1a_seeded(item, FNV_OFFSET_BASIS_2));
        for i in 0..self.n_hashes as u64 {
            self.bits.set(self.position(h1, h2, i))?;
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_INSERT, 0);
        Ok(())
    }

    /// True if `item` MIGHT be in the set; false if definitely not.
    /// False positives are possible; false negatives are NOT.
    pub fn contains(&self, item: &[u8]) -> Result<bool, BloomError> {
        let h1 = fmix64(fnv1a_seeded(item, FNV_OFFSET_BASIS_1));
        let h2 = fmix64(fnv1a_seeded(item, FNV_OFFSET_BASIS_2));
        for i in 0..self.n_hashes as u64 {
            if !self.bits.get(self.position(h1, h2, i))? {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::sketch::OP_QUERY, 2); // absent
                return Ok(false);
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_QUERY, 0);
        Ok(true)
    }

    /// Clear all bits (resets the filter to empty).
    pub fn clear(&self) {
        self.bits.clear_all();
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_CLEAR, 0);
    }

    /// Estimate the current false-positive rate from the bit-set
    /// density. Returns 0.0 for an empty filter; approaches 1.0
    /// as the filter saturates.
    pub fn estimated_false_positive_rate(&self) -> f64 {
        let fill = self.bits.count_ones() as f64;
        let n = self.n_bits as f64;
        let k = self.n_hashes as f64;
        if fill == 0.0 { return 0.0; }
        (fill / n).powf(k)
    }

    /// Estimate the number of distinct inserted items based on bit
    /// density. Formula: `-n_bits / n_hashes * ln(1 - fill / n_bits)`.
    pub fn estimated_insert_count(&self) -> u64 {
        let fill = self.bits.count_ones() as f64;
        let n = self.n_bits as f64;
        let k = self.n_hashes as f64;
        if fill == 0.0 { return 0; }
        if fill >= n { return u64::MAX; }
        ((-n / k) * (1.0 - fill / n).ln()).round() as u64
    }

    pub fn flush(&self) -> Result<(), BloomError> {
        Ok(self.bits.flush()?)
    }

    pub fn flush_async(&self) -> Result<(), BloomError> {
        Ok(self.bits.flush_async()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-bloom-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        std::fs::remove_file(header_path(base)).ok();
        std::fs::remove_file(bits_path(base)).ok();
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let base = tmp_base("empty");
        let b = SharedBloomFilter::create(&base, 1024, 3).unwrap();
        assert!(!b.contains(b"anything").unwrap());
        assert!(!b.contains(b"").unwrap());
        cleanup(&base);
    }

    #[test]
    fn insert_then_contains_returns_true_no_false_negatives() {
        let base = tmp_base("no-fn");
        let b = SharedBloomFilter::create(&base, 1024, 3).unwrap();
        let items: &[&[u8]] = &[
            b"hello", b"world", b"adaptive-prims", b"42",
            b"the quick brown fox", b"", b"a",
        ];
        for &item in items {
            b.insert(item).unwrap();
        }
        for &item in items {
            assert!(b.contains(item).unwrap(),
                "false negative for item: {item:?}");
        }
        cleanup(&base);
    }

    #[test]
    fn invalid_config_rejected() {
        let base = tmp_base("invalid");
        assert_eq!(
            SharedBloomFilter::create(&base, 0, 3).err(),
            Some(BloomError::InvalidConfig)
        );
        assert_eq!(
            SharedBloomFilter::create(&base, 1024, 0).err(),
            Some(BloomError::InvalidConfig)
        );
        cleanup(&base);
    }

    #[test]
    fn false_positive_rate_within_bounds_at_target_load() {
        let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(1000, 0.01);
        let base = tmp_base("fpr");
        let b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
        for i in 0..1000u32 {
            b.insert(format!("item-{i:04}").as_bytes()).unwrap();
        }
        let mut fp = 0u32;
        for i in 10_000u32..20_000 {
            if b.contains(format!("query-{i:05}").as_bytes()).unwrap() {
                fp += 1;
            }
        }
        let observed_fpr = fp as f64 / 10_000.0;
        // Target was 0.01; allow stochastic slack up to 0.03.
        assert!(observed_fpr < 0.03,
            "observed FPR {observed_fpr} should be below 0.03 (target was 0.01)");
        cleanup(&base);
    }

    #[test]
    fn clear_resets_filter() {
        let base = tmp_base("clear");
        let b = SharedBloomFilter::create(&base, 1024, 3).unwrap();
        b.insert(b"foo").unwrap();
        b.insert(b"bar").unwrap();
        assert!(b.contains(b"foo").unwrap());
        b.clear();
        assert!(!b.contains(b"foo").unwrap());
        assert!(!b.contains(b"bar").unwrap());
        cleanup(&base);
    }

    #[test]
    fn cross_handle_visibility() {
        let base = tmp_base("cross-handle");
        let writer = SharedBloomFilter::create(&base, 1024, 3).unwrap();
        let reader = SharedBloomFilter::open(&base, 1024, 3).unwrap();
        writer.insert(b"cross-process").unwrap();
        assert!(reader.contains(b"cross-process").unwrap());
        assert!(!reader.contains(b"not-inserted").unwrap());
        cleanup(&base);
    }

    #[test]
    fn config_mismatch_at_open_rejected() {
        let base = tmp_base("mismatch");
        let _w = SharedBloomFilter::create(&base, 1024, 3).unwrap();
        assert!(matches!(
            SharedBloomFilter::open(&base, 2048, 3),
            Err(BloomError::LayoutMismatch)
        ));
        assert!(matches!(
            SharedBloomFilter::open(&base, 1024, 5),
            Err(BloomError::LayoutMismatch)
        ));
        cleanup(&base);
    }

    #[test]
    fn estimated_count_tracks_real_inserts() {
        let base = tmp_base("count-est");
        let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(1000, 0.01);
        let b = SharedBloomFilter::create(&base, n_bits, n_hashes).unwrap();
        for i in 0..500u32 {
            b.insert(format!("k{i}").as_bytes()).unwrap();
        }
        let est = b.estimated_insert_count();
        // Roughly 500, within 25% slack for stochastic distribution.
        assert!(est > 350 && est < 650,
            "estimated insert count {est} should be near 500");
        cleanup(&base);
    }

    #[test]
    fn suggest_config_returns_sensible_values() {
        let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(10_000, 0.01);
        assert!(n_bits > 90_000 && n_bits < 100_000,
            "n_bits {n_bits} should be ~95k");
        assert!((6..=8).contains(&n_hashes),
            "n_hashes {n_hashes} should be ~7");
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let base = tmp_base("disk");
        {
            let b = SharedBloomFilter::create(&base, 1024, 3).unwrap();
            b.insert(b"persisted").unwrap();
            b.insert(b"also-persisted").unwrap();
            b.flush().unwrap();
        }
        let b2 = SharedBloomFilter::open(&base, 1024, 3).unwrap();
        assert!(b2.contains(b"persisted").unwrap());
        assert!(b2.contains(b"also-persisted").unwrap());
        assert!(!b2.contains(b"not-there").unwrap());
        cleanup(&base);
    }

    #[test]
    fn concurrent_inserters_no_lost_updates() {
        use std::sync::Arc;
        use std::thread;
        let base = tmp_base("concurrent");
        let b: Arc<SharedBloomFilter>
            = Arc::new(SharedBloomFilter::create(&base, 8192, 5).unwrap());
        let n_threads = 4;
        let per_thread = 100;
        let mut handles = vec![];
        for t in 0..n_threads {
            let b = b.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    b.insert(format!("t{t}-i{i}").as_bytes()).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        for t in 0..n_threads {
            for i in 0..per_thread {
                let item = format!("t{t}-i{i}");
                assert!(b.contains(item.as_bytes()).unwrap(),
                    "missing item {item}");
            }
        }
        cleanup(&base);
    }
}
