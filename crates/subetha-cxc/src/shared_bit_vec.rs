//! `SharedBitVec` - cross-process bit-packed boolean array.
//!
//! Fixed-size bit array stored in an MMF. Each underlying u64 word
//! is operated on atomically (fetch_or for set, fetch_and for clear)
//! so concurrent writers don't lose updates.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | BitVecHeader (64B)        |
//! |   magic, capacity_bits    |
//! +---------------------------+
//! | words[ceil(cap/64)]       |  AtomicU64 each
//! +---------------------------+
//! ```
//!
//! # Concurrency
//!
//! - `set(i)` -> `word.fetch_or(1 << bit, AcqRel)`. Multiple
//!   writers setting distinct bits in the same word compose
//!   correctly (RMW; no lost updates).
//! - `clear(i)` -> `word.fetch_and(!(1 << bit), AcqRel)`.
//! - `toggle(i)` -> `word.fetch_xor(1 << bit, AcqRel)`.
//! - `get(i)` -> `word.load(Acquire) & (1 << bit) != 0`.
//! - `set_range(lo, hi)` / `clear_range(lo, hi)` use RMW on
//!   boundary words and `store` on fully-covered interior words.
//!   Interior stores are safe because they overwrite all 64 bits;
//!   no concurrent writer can be modifying interior bits THIS
//!   call expects to keep (we're setting/clearing them all).
//!
//! # Use cases
//!
//! - Cross-process set membership (presence/absence flags).
//! - Bloom filter backing array.
//! - Allocation bitmaps (slot in use / free).
//! - Feature flag arrays.
//! - Multi-process work-stealing claim bits.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const BITVEC_MAGIC: u64 = 0x4150_4256_4543_2031;
pub const BITS_PER_WORD: usize = 64;

#[repr(C, align(64))]
pub struct BitVecHeader {
    pub magic: u64,
    pub capacity_bits: u64,
    pub word_count: u64,
    _pad: [u8; 40],
}

const _: () = {
    assert!(size_of::<BitVecHeader>() == 64);
};

pub const fn bit_vec_file_size(capacity_bits: usize) -> usize {
    let word_count = capacity_bits.div_ceil(BITS_PER_WORD);
    size_of::<BitVecHeader>() + word_count * size_of::<AtomicU64>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitVecError {
    OutOfBounds,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for BitVecError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedBitVec {
    _file: File,
    mmap: MmapMut,
    capacity_bits: usize,
    word_count: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedBitVec {}
unsafe impl Sync for SharedBitVec {}

impl subetha_sidecar::AdaptiveInstance for SharedBitVec {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedBitVec {
    pub fn create(
        path: impl AsRef<Path>, capacity_bits: usize,
    ) -> Result<Self, BitVecError> {
        assert!(capacity_bits >= 1);
        let word_count = capacity_bits.div_ceil(BITS_PER_WORD);
        let total = bit_vec_file_size(capacity_bits);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut BitVecHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<BitVecHeader>());
            (*hdr).magic = BITVEC_MAGIC;
            (*hdr).capacity_bits = capacity_bits as u64;
            (*hdr).word_count = word_count as u64;
        }
        Ok(Self {
            _file: file, mmap, capacity_bits, word_count,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_capacity_bits: usize,
    ) -> Result<Self, BitVecError> {
        let total = bit_vec_file_size(expected_capacity_bits);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(BitVecError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const BitVecHeader) };
        if hdr.magic != BITVEC_MAGIC || hdr.capacity_bits != expected_capacity_bits as u64 {
            return Err(BitVecError::LayoutMismatch);
        }
        let word_count = hdr.word_count as usize;
        Ok(Self {
            _file: file, mmap, capacity_bits: expected_capacity_bits, word_count,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity_bits(&self) -> usize { self.capacity_bits }

    #[inline]
    pub fn capacity_words(&self) -> usize { self.word_count }

    fn word(&self, word_idx: usize) -> &AtomicU64 {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<BitVecHeader>()) };
        unsafe { &*(base.add(word_idx * size_of::<AtomicU64>()) as *const AtomicU64) }
    }

    #[inline]
    fn check_bounds(&self, bit: usize) -> Result<(), BitVecError> {
        if bit >= self.capacity_bits { Err(BitVecError::OutOfBounds) } else { Ok(()) }
    }

    /// Set bit `i` to 1. Returns the previous value at that bit.
    pub fn set(&self, i: usize) -> Result<bool, BitVecError> {
        if let Err(e) = self.check_bounds(i) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::bit_vec::OP_SET, 1);
            return Err(e);
        }
        let (w, b) = (i / BITS_PER_WORD, i % BITS_PER_WORD);
        let mask = 1u64 << b;
        let prev = self.word(w).fetch_or(mask, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_SET, 0);
        Ok((prev & mask) != 0)
    }

    /// Clear bit `i` to 0. Returns the previous value at that bit.
    pub fn clear(&self, i: usize) -> Result<bool, BitVecError> {
        if let Err(e) = self.check_bounds(i) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::bit_vec::OP_CLEAR, 1);
            return Err(e);
        }
        let (w, b) = (i / BITS_PER_WORD, i % BITS_PER_WORD);
        let mask = 1u64 << b;
        let prev = self.word(w).fetch_and(!mask, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_CLEAR, 0);
        Ok((prev & mask) != 0)
    }

    /// Flip bit `i`. Returns the new value at that bit.
    pub fn toggle(&self, i: usize) -> Result<bool, BitVecError> {
        if let Err(e) = self.check_bounds(i) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::bit_vec::OP_TOGGLE, 1);
            return Err(e);
        }
        let (w, b) = (i / BITS_PER_WORD, i % BITS_PER_WORD);
        let mask = 1u64 << b;
        let prev = self.word(w).fetch_xor(mask, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_TOGGLE, 0);
        // New value is the flip of the previous.
        Ok((prev & mask) == 0)
    }

    /// Read bit `i`.
    pub fn get(&self, i: usize) -> Result<bool, BitVecError> {
        if let Err(e) = self.check_bounds(i) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::bit_vec::OP_GET, 1);
            return Err(e);
        }
        let (w, b) = (i / BITS_PER_WORD, i % BITS_PER_WORD);
        let mask = 1u64 << b;
        let v = (self.word(w).load(Ordering::Acquire) & mask) != 0;
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_GET, 0);
        Ok(v)
    }

    /// Set all bits in `lo..hi` (exclusive end). Uses RMW on boundary
    /// words and unconditional store on interior words.
    pub fn set_range(&self, lo: usize, hi: usize) -> Result<(), BitVecError> {
        if lo > hi || hi > self.capacity_bits {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::bit_vec::OP_RANGE, 1);
            return Err(BitVecError::OutOfBounds);
        }
        if lo != hi { self.range_op(lo, hi, RangeOp::Set); }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_RANGE, 0);
        Ok(())
    }

    /// Clear all bits in `lo..hi` (exclusive end). Same RMW-on-
    /// boundary, store-on-interior pattern.
    pub fn clear_range(&self, lo: usize, hi: usize) -> Result<(), BitVecError> {
        if lo > hi || hi > self.capacity_bits {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::bit_vec::OP_RANGE, 1);
            return Err(BitVecError::OutOfBounds);
        }
        if lo != hi { self.range_op(lo, hi, RangeOp::Clear); }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_RANGE, 0);
        Ok(())
    }

    fn range_op(&self, lo: usize, hi: usize, op: RangeOp) {
        let lo_word = lo / BITS_PER_WORD;
        let hi_word = (hi - 1) / BITS_PER_WORD;  // inclusive end word
        let lo_bit = lo % BITS_PER_WORD;
        let hi_bit_excl = hi - hi_word * BITS_PER_WORD;
        if lo_word == hi_word {
            // Single-word case: mask covers bits [lo_bit, hi_bit_excl).
            let count = hi - lo;
            let mask = if count == BITS_PER_WORD { u64::MAX }
                       else { ((1u64 << count) - 1) << lo_bit };
            match op {
                RangeOp::Set => { self.word(lo_word).fetch_or(mask, Ordering::AcqRel); }
                RangeOp::Clear => { self.word(lo_word).fetch_and(!mask, Ordering::AcqRel); }
            }
            return;
        }
        // First word: partial cover at the high end.
        let lo_mask = !((1u64 << lo_bit) - 1);
        match op {
            RangeOp::Set => { self.word(lo_word).fetch_or(lo_mask, Ordering::AcqRel); }
            RangeOp::Clear => { self.word(lo_word).fetch_and(!lo_mask, Ordering::AcqRel); }
        }
        // Interior words: fully covered, plain Release-store.
        for w in (lo_word + 1)..hi_word {
            let v = match op {
                RangeOp::Set => u64::MAX,
                RangeOp::Clear => 0,
            };
            self.word(w).store(v, Ordering::Release);
        }
        // Last word: partial cover at the low end.
        let hi_mask = if hi_bit_excl == BITS_PER_WORD { u64::MAX }
                      else { (1u64 << hi_bit_excl) - 1 };
        match op {
            RangeOp::Set => { self.word(hi_word).fetch_or(hi_mask, Ordering::AcqRel); }
            RangeOp::Clear => { self.word(hi_word).fetch_and(!hi_mask, Ordering::AcqRel); }
        }
    }

    /// Count total number of 1-bits across all words. O(words).
    pub fn count_ones(&self) -> usize {
        let mut sum = 0usize;
        for w in 0..self.word_count {
            sum += self.word(w).load(Ordering::Acquire).count_ones() as usize;
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::bit_vec::OP_COUNT_ONES, 0);
        // The last word may have padding bits beyond capacity_bits;
        // those start as 0 and should never be set by our public API,
        // so they don't affect the count.
        sum
    }

    /// Count total number of 0-bits in the valid range.
    pub fn count_zeros(&self) -> usize {
        self.capacity_bits.saturating_sub(self.count_ones())
    }

    pub fn is_all_set(&self) -> bool { self.count_ones() == self.capacity_bits }
    pub fn is_all_clear(&self) -> bool { self.count_ones() == 0 }

    /// Set every bit. Unconditional Release-store on every word,
    /// with the last word masked to only valid bits so count_ones
    /// stays accurate.
    pub fn set_all(&self) {
        if self.word_count == 0 { return; }
        let full_words = self.word_count - 1;
        for w in 0..full_words {
            self.word(w).store(u64::MAX, Ordering::Release);
        }
        // Last word: only set bits that fall within capacity_bits.
        let last_word_bits = self.capacity_bits - full_words * BITS_PER_WORD;
        let last_mask = if last_word_bits == BITS_PER_WORD { u64::MAX }
                        else { (1u64 << last_word_bits) - 1 };
        self.word(full_words).store(last_mask, Ordering::Release);
    }

    /// Clear every bit.
    pub fn clear_all(&self) {
        for w in 0..self.word_count {
            self.word(w).store(0, Ordering::Release);
        }
    }

    pub fn flush(&self) -> Result<(), BitVecError> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), BitVecError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum RangeOp { Set, Clear }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-bitvec-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_all_zero() {
        let p = tmp("init");
        let b = SharedBitVec::create(&p, 100).unwrap();
        assert_eq!(b.capacity_bits(), 100);
        assert_eq!(b.count_ones(), 0);
        assert!(b.is_all_clear());
        for i in 0..100 {
            assert!(!b.get(i).unwrap(), "bit {i} should start clear");
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_get_round_trip() {
        let p = tmp("rt");
        let b = SharedBitVec::create(&p, 200).unwrap();
        assert!(!b.set(5).unwrap());
        assert!(!b.set(63).unwrap());
        assert!(!b.set(64).unwrap());
        assert!(!b.set(199).unwrap());
        assert!(b.get(5).unwrap());
        assert!(b.get(63).unwrap());
        assert!(b.get(64).unwrap());
        assert!(b.get(199).unwrap());
        assert!(!b.get(6).unwrap());
        assert!(!b.get(0).unwrap());
        // Setting again returns the previous value (now true).
        assert!(b.set(5).unwrap());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn clear_works() {
        let p = tmp("clear");
        let b = SharedBitVec::create(&p, 64).unwrap();
        b.set(10).unwrap();
        b.set(20).unwrap();
        assert!(b.clear(10).unwrap());
        assert!(!b.get(10).unwrap());
        assert!(b.get(20).unwrap());
        // Clearing already-clear bit returns false.
        assert!(!b.clear(0).unwrap());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn toggle_alternates() {
        let p = tmp("toggle");
        let b = SharedBitVec::create(&p, 8).unwrap();
        assert!(b.toggle(3).unwrap());   // was 0, now 1
        assert!(b.get(3).unwrap());
        assert!(!b.toggle(3).unwrap());  // was 1, now 0
        assert!(!b.get(3).unwrap());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn count_ones_accurate() {
        let p = tmp("count");
        let b = SharedBitVec::create(&p, 200).unwrap();
        for i in [0, 7, 31, 63, 64, 100, 199] {
            b.set(i).unwrap();
        }
        assert_eq!(b.count_ones(), 7);
        assert_eq!(b.count_zeros(), 200 - 7);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_range_single_word() {
        let p = tmp("range-single");
        let b = SharedBitVec::create(&p, 64).unwrap();
        b.set_range(10, 20).unwrap();
        for i in 0..64 {
            let expected = (10..20).contains(&i);
            assert_eq!(b.get(i).unwrap(), expected, "bit {i}");
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_range_spans_multiple_words() {
        let p = tmp("range-multi");
        let b = SharedBitVec::create(&p, 200).unwrap();
        b.set_range(50, 150).unwrap();
        for i in 0..200 {
            let expected = (50..150).contains(&i);
            assert_eq!(b.get(i).unwrap(), expected, "bit {i}");
        }
        assert_eq!(b.count_ones(), 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn clear_range_after_set_all() {
        let p = tmp("clear-range");
        let b = SharedBitVec::create(&p, 200).unwrap();
        b.set_all();
        assert!(b.is_all_set());
        b.clear_range(70, 130).unwrap();
        for i in 0..200 {
            let expected = !(70..130).contains(&i);
            assert_eq!(b.get(i).unwrap(), expected, "bit {i}");
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn out_of_bounds_rejected() {
        let p = tmp("oob");
        let b = SharedBitVec::create(&p, 8).unwrap();
        assert_eq!(b.set(8).err(), Some(BitVecError::OutOfBounds));
        assert_eq!(b.set(999).err(), Some(BitVecError::OutOfBounds));
        assert_eq!(b.get(8).err(), Some(BitVecError::OutOfBounds));
        assert_eq!(b.set_range(0, 9).err(), Some(BitVecError::OutOfBounds));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_all_and_clear_all() {
        let p = tmp("all");
        let b = SharedBitVec::create(&p, 130).unwrap();
        b.set_all();
        assert!(b.is_all_set());
        for i in 0..130 { assert!(b.get(i).unwrap()); }
        b.clear_all();
        assert!(b.is_all_clear());
        for i in 0..130 { assert!(!b.get(i).unwrap()); }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer = SharedBitVec::create(&p, 100).unwrap();
        let reader = SharedBitVec::open(&p, 100).unwrap();
        writer.set(42).unwrap();
        writer.set(77).unwrap();
        assert!(reader.get(42).unwrap());
        assert!(reader.get(77).unwrap());
        assert!(!reader.get(0).unwrap());
        reader.clear(42).unwrap();
        assert!(!writer.get(42).unwrap());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_setters_of_disjoint_bits_all_visible() {
        // 4 threads each set 100 distinct bits; all 400 must be visible.
        let p = tmp("concurrent-disjoint");
        let b = Arc::new(SharedBitVec::create(&p, 1000).unwrap());
        let n_threads = 4;
        let per_thread = 100;
        let mut handles = vec![];
        for t in 0..n_threads {
            let b = b.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    let bit = t * per_thread + i;
                    b.set(bit).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(b.count_ones(), n_threads * per_thread);
        for t in 0..n_threads {
            for i in 0..per_thread {
                assert!(b.get(t * per_thread + i).unwrap());
            }
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_setters_of_same_word_distinct_bits_all_visible() {
        // 64 threads each set ONE distinct bit in the same word.
        // Without atomic RMW this would race and lose updates.
        let p = tmp("concurrent-same-word");
        let b = Arc::new(SharedBitVec::create(&p, 64).unwrap());
        let mut handles = vec![];
        for bit in 0..64 {
            let b = b.clone();
            handles.push(thread::spawn(move || {
                b.set(bit).unwrap();
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert!(b.is_all_set(), "every bit in the word should be set");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_setters_of_same_bit_idempotent() {
        // 16 threads all set bit 42. Result: bit 42 set exactly once.
        let p = tmp("concurrent-same-bit");
        let b = Arc::new(SharedBitVec::create(&p, 100).unwrap());
        let mut handles = vec![];
        for _ in 0..16 {
            let b = b.clone();
            handles.push(thread::spawn(move || b.set(42).unwrap()));
        }
        let prev_values: Vec<bool> = handles.into_iter()
            .map(|h| h.join().unwrap()).collect();
        // Exactly one set saw prev=false; the others saw prev=true.
        let false_count = prev_values.iter().filter(|&&v| !v).count();
        let true_count = prev_values.iter().filter(|&&v| v).count();
        assert_eq!(false_count, 1, "exactly one setter should see prev=false");
        assert_eq!(true_count, 15);
        assert!(b.get(42).unwrap());
        assert_eq!(b.count_ones(), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let b = SharedBitVec::create(&p, 200).unwrap();
            for i in [5, 50, 100, 150, 199] { b.set(i).unwrap(); }
            b.flush().unwrap();
        }
        let b2 = SharedBitVec::open(&p, 200).unwrap();
        assert_eq!(b2.count_ones(), 5);
        for i in [5, 50, 100, 150, 199] { assert!(b2.get(i).unwrap()); }
        for i in [0, 4, 6, 49, 51] { assert!(!b2.get(i).unwrap()); }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn capacity_words_matches_packed_count() {
        let p = tmp("words");
        let b1 = SharedBitVec::create(&p, 64).unwrap();
        assert_eq!(b1.capacity_words(), 1);
        drop(b1);
        std::fs::remove_file(&p).ok();

        let p2 = tmp("words2");
        let b2 = SharedBitVec::create(&p2, 65).unwrap();
        assert_eq!(b2.capacity_words(), 2);
        drop(b2);
        std::fs::remove_file(&p2).ok();

        let p3 = tmp("words3");
        let b3 = SharedBitVec::create(&p3, 1000).unwrap();
        assert_eq!(b3.capacity_words(), 16);  // ceil(1000/64) = 16
        drop(b3);
        std::fs::remove_file(&p3).ok();
    }

    #[test]
    fn allocation_bitmap_pattern() {
        // Realistic use: cross-process allocation bitmap. set() to
        // claim a slot, clear() to release. count_ones reports
        // current usage.
        let p = tmp("alloc-pattern");
        let b = SharedBitVec::create(&p, 1024).unwrap();
        // Claim slots 0..10.
        for i in 0..10 { assert!(!b.set(i).unwrap()); }
        assert_eq!(b.count_ones(), 10);
        // Release slot 5.
        assert!(b.clear(5).unwrap());
        assert_eq!(b.count_ones(), 9);
        // Re-claim slot 5.
        assert!(!b.set(5).unwrap());
        assert_eq!(b.count_ones(), 10);
        std::fs::remove_file(&p).ok();
    }
}
