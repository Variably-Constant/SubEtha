//! `SharedBlockedBloomFilter` - cache-blocked cross-process Bloom filter.
//!
//! A standard Bloom filter sets `k` bits at positions scattered across the
//! whole bit array, so each insert/contains touches up to `k` distinct
//! cache lines - the dominant cost once the filter exceeds L2. A *blocked*
//! Bloom filter first hashes the item to one 512-bit **block** (exactly one
//! 64-byte cache line) and then sets all `k` bits *within that block*. Every
//! insert/contains touches a single cache line.
//!
//! The trade is a slightly higher false-positive rate at the same memory
//! (the bits concentrate in one block instead of spreading), but for
//! 512-bit blocks the inflation is modest and the cache win is large. This
//! is the same structure production systems use for join filters.
//!
//! Layout (one MMF): `[BlockedBloomHeader | AtomicU64 words...]`, with
//! `BLOCK_WORDS` (8) words per block. All bit ops are lock-free atomics, so
//! concurrent inserters across threads / processes compose correctly.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

/// Bits per block = one 64-byte cache line.
pub const BLOCK_BITS: usize = 512;
/// `u64` words per block.
pub const BLOCK_WORDS: usize = BLOCK_BITS / 64; // 8

pub const BLOCKED_BLOOM_MAGIC: u64 = 0x4250_4C4F_4F4D_4231; // "BPLOOMB1"

#[repr(C, align(64))]
pub struct BlockedBloomHeader {
    pub magic: u64,
    pub n_blocks: u64,
    pub n_hashes: u32,
    _pad: [u8; 44],
}

const _: () = {
    assert!(size_of::<BlockedBloomHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedBloomError {
    LayoutMismatch,
    InvalidConfig,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for BlockedBloomError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.kind())
    }
}

const FNV_OFFSET_BASIS_1: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_BASIS_2: u64 = 0x8422_2325_cbf2_9ce4;
const FNV_PRIME: u64 = 0x100_0000_01b3;
const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

#[inline]
fn fnv1a_seeded(bytes: &[u8], basis: u64) -> u64 {
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

pub fn blocked_bloom_file_size(n_blocks: usize) -> usize {
    size_of::<BlockedBloomHeader>() + n_blocks * BLOCK_WORDS * size_of::<u64>()
}

pub struct SharedBlockedBloomFilter {
    _file: File,
    mmap: MmapMut,
    raw_ptr: *mut u8,
    n_blocks: u64,
    n_hashes: u32,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedBlockedBloomFilter {}
unsafe impl Sync for SharedBlockedBloomFilter {}

impl subetha_sidecar::AdaptiveInstance for SharedBlockedBloomFilter {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedBlockedBloomFilter {
    /// Suggest `(n_bits, n_hashes)` for `n` items at false-positive rate
    /// `p`, identical to the standard Bloom formula; the caller passes the
    /// resulting `n_bits` to [`create`](Self::create), which rounds up to
    /// whole 512-bit blocks.
    pub fn suggest_config(n: usize, p: f64) -> (usize, u32) {
        let n = n.max(1) as f64;
        let std_bits = -n * p.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2);
        // k is the optimal hash count for the standard per-item density.
        let n_hashes = ((std_bits / n) * std::f64::consts::LN_2).round() as u32;
        // With decorrelated within-block bits the achieved FPR tracks the
        // standard formula closely; a small margin (1.15x) absorbs the
        // per-block Poisson load variance so the blocked filter matches the
        // standard target FPR at essentially the same memory.
        let n_bits = (std_bits * 1.15).ceil() as usize;
        (n_bits.max(BLOCK_BITS), n_hashes.max(1))
    }

    /// Obtain the filter at `path`, initializing an empty one if the
    /// path does not yet exist and attaching to it if it does.
    /// Attaching leaves inserted members in place; a region built with
    /// a different block count or hash count is a `LayoutMismatch`.
    /// [`reset`](Self::reset) reinitializes.
    pub fn create(
        path: impl AsRef<Path>, n_bits: usize, n_hashes: u32,
    ) -> Result<Self, BlockedBloomError> {
        if n_bits == 0 || n_hashes == 0 {
            return Err(BlockedBloomError::InvalidConfig);
        }
        let n_blocks = n_bits.div_ceil(BLOCK_BITS).max(1);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            blocked_bloom_file_size(n_blocks),
            |ptr| unsafe { Self::init_region(ptr, n_blocks, n_hashes) },
            |ptr| unsafe { (*(ptr as *const BlockedBloomHeader)).magic == BLOCKED_BLOOM_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, BlockedBloomError::LayoutMismatch))?;
        Self::from_region(file, mmap, n_blocks, n_hashes)
    }

    /// Truncate the filter at `path` and initialize an empty one,
    /// discarding every member live peers share. For a caller that
    /// knows it owns the path.
    pub fn reset(
        path: impl AsRef<Path>, n_bits: usize, n_hashes: u32,
    ) -> Result<Self, BlockedBloomError> {
        if n_bits == 0 || n_hashes == 0 {
            return Err(BlockedBloomError::InvalidConfig);
        }
        let n_blocks = n_bits.div_ceil(BLOCK_BITS).max(1);
        let (file, mmap) = crate::mmf_attach::reset(
            path.as_ref(),
            blocked_bloom_file_size(n_blocks),
            |ptr| unsafe { Self::init_region(ptr, n_blocks, n_hashes) },
        )?;
        Self::from_region(file, mmap, n_blocks, n_hashes)
    }

    /// Lay out an empty filter: config first, magic last, because
    /// attachers spin on it. The zeroed region is already the all-clear
    /// block array.
    ///
    /// # Safety
    /// `ptr` addresses at least `blocked_bloom_file_size(n_blocks)`
    /// writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, n_blocks: usize, n_hashes: u32) {
        let hdr = ptr as *mut BlockedBloomHeader;
        unsafe {
            (*hdr).n_blocks = n_blocks as u64;
            (*hdr).n_hashes = n_hashes;
            std::ptr::write_volatile(&raw mut (*hdr).magic, BLOCKED_BLOOM_MAGIC);
        }
    }

    /// Wrap an initialized region, refusing one built with a different
    /// config.
    fn from_region(
        file: File,
        mut mmap: MmapMut,
        n_blocks: usize,
        n_hashes: u32,
    ) -> Result<Self, BlockedBloomError> {
        let hdr = unsafe { &*(mmap.as_ptr() as *const BlockedBloomHeader) };
        if hdr.magic != BLOCKED_BLOOM_MAGIC
            || hdr.n_blocks != n_blocks as u64
            || hdr.n_hashes != n_hashes
        {
            return Err(BlockedBloomError::LayoutMismatch);
        }
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self {
            _file: file, mmap, raw_ptr,
            n_blocks: n_blocks as u64, n_hashes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, n_bits: usize, n_hashes: u32,
    ) -> Result<Self, BlockedBloomError> {
        let n_blocks = n_bits.div_ceil(BLOCK_BITS).max(1);
        let total = blocked_bloom_file_size(n_blocks);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(BlockedBloomError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::from_region(file, mmap, n_blocks, n_hashes)
    }

    #[inline]
    pub fn n_hashes(&self) -> u32 { self.n_hashes }
    #[inline]
    pub fn n_blocks(&self) -> u64 { self.n_blocks }

    /// `&AtomicU64` for word `w` (0..BLOCK_WORDS) of `block`.
    #[inline]
    fn word(&self, block: u64, w: usize) -> &AtomicU64 {
        let word_idx = block as usize * BLOCK_WORDS + w;
        let addr = unsafe {
            self.raw_ptr
                .add(size_of::<BlockedBloomHeader>())
                .add(word_idx * size_of::<u64>())
        };
        unsafe { &*(addr as *const AtomicU64) }
    }

    /// Hash an item into (block index, k within-block bit positions packed
    /// as the second hash). The block is chosen by Lemire fastrange on the
    /// avalanched first hash; the within-block bits come from the second.
    #[inline]
    fn locate(&self, item: &[u8]) -> (u64, u64) {
        let h1 = fmix64(fnv1a_seeded(item, FNV_OFFSET_BASIS_1));
        let h2 = fmix64(fnv1a_seeded(item, FNV_OFFSET_BASIS_2));
        let block = ((h1 as u128 * self.n_blocks as u128) >> 64) as u64;
        (block, h2)
    }

    /// `i`-th within-block bit position in `[0, BLOCK_BITS)`. `h2` is
    /// already fmix64-avalanched, so a distinct 9-bit slice per `i` gives
    /// decorrelated positions for free - seven 9-bit slices fit in 63
    /// bits. Each `i` must take its OWN slice: a shared slice correlates
    /// every position of an item and inflates the achieved FPR far above
    /// the configured target. Past seven, `h2` is re-avalanched per `i`
    /// rather than sliced again.
    #[inline]
    fn bit_in_block(h2: u64, i: u32) -> usize {
        let src = if i < 7 {
            h2 >> (i * 9)
        } else {
            fmix64(h2.wrapping_add((i as u64).wrapping_mul(GOLDEN)))
        };
        (src & (BLOCK_BITS as u64 - 1)) as usize
    }

    /// Insert an item. Touches exactly one cache-line block.
    pub fn insert(&self, item: &[u8]) {
        let (block, h2) = self.locate(item);
        for i in 0..self.n_hashes {
            let bit = Self::bit_in_block(h2, i);
            self.word(block, bit / 64).fetch_or(1u64 << (bit % 64), Ordering::AcqRel);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_INSERT, 0);
    }

    /// True if `item` MIGHT be present; false if definitely absent. One
    /// cache line read.
    pub fn contains(&self, item: &[u8]) -> bool {
        let (block, h2) = self.locate(item);
        for i in 0..self.n_hashes {
            let bit = Self::bit_in_block(h2, i);
            if self.word(block, bit / 64).load(Ordering::Acquire) & (1u64 << (bit % 64)) == 0 {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::sketch::OP_QUERY, 2);
                return false;
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::sketch::OP_QUERY, 0);
        true
    }

    /// Reset all bits.
    pub fn clear(&self) {
        let words = self.n_blocks as usize * BLOCK_WORDS;
        for i in 0..words {
            self.word(0, i).store(0, Ordering::Release);
        }
    }

    pub fn flush(&self) -> Result<(), BlockedBloomError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file for one test, removed when the test ends; declared before the
    /// primitive that maps it so it drops after it.
    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("blocked_bloom_{name}_{}.bin", std::process::id()))
    }

    /// A second create attaches with inserted members in place; reset
    /// is what strips them.
    #[test]
    fn second_create_attaches_and_keeps_members() {
        let p = tmp("attach");
        let bf = SharedBlockedBloomFilter::create(&p, 4096, 4).unwrap();
        bf.insert(b"member");

        let bf2 = SharedBlockedBloomFilter::create(&p, 4096, 4).unwrap();
        assert!(bf2.contains(b"member"), "attach lost a member");
        assert!(matches!(
            SharedBlockedBloomFilter::create(&p, 4096, 7),
            Err(BlockedBloomError::LayoutMismatch),
        ));

        // Windows refuses to truncate a mapped file, so every handle goes
        // before the reset.
        drop(bf);
        drop(bf2);
        let fresh = SharedBlockedBloomFilter::reset(&p, 4096, 4).unwrap();
        assert!(!fresh.contains(b"member"), "reset kept a member");
        drop(fresh);
    }

    #[test]
    fn insert_then_contains_no_false_negatives() {
        let (nb, nh) = SharedBlockedBloomFilter::suggest_config(10_000, 0.01);
        let p = tmp("nofalseneg");
        let bf = SharedBlockedBloomFilter::create(&p, nb, nh).unwrap();
        for i in 0..10_000u64 {
            bf.insert(&i.to_le_bytes());
        }
        // No false negatives: every inserted item must be reported present.
        for i in 0..10_000u64 {
            assert!(bf.contains(&i.to_le_bytes()), "false negative at {i}");
        }
    }

    #[test]
    fn false_positive_rate_within_bounds() {
        let n = 10_000usize;
        let target = 0.01;
        let (nb, nh) = SharedBlockedBloomFilter::suggest_config(n, target);
        let p = tmp("fpr");
        let bf = SharedBlockedBloomFilter::create(&p, nb, nh).unwrap();
        for i in 0..n as u64 {
            bf.insert(&i.to_le_bytes());
        }
        let mut fp = 0u32;
        let trials = 20_000u64;
        for i in 0..trials {
            let key = (1_000_000u64 + i).to_le_bytes();
            if bf.contains(&key) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        // Blocked filters inflate FPR modestly; allow up to 4x target.
        assert!(rate < target * 4.0, "blocked FPR {rate} exceeded {} (4x target)", target * 4.0);
    }

    #[test]
    fn cross_handle_visibility() {
        let (nb, nh) = SharedBlockedBloomFilter::suggest_config(1000, 0.01);
        let p = tmp("xhandle");
        let a = SharedBlockedBloomFilter::create(&p, nb, nh).unwrap();
        a.insert(b"shared-key");
        let b = SharedBlockedBloomFilter::open(&p, nb, nh).unwrap();
        assert!(b.contains(b"shared-key"));
    }
}
