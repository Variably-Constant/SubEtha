//! `SharedTimePointTile<T>` - cross-process BSPA + Versioned tile
//! with AVX2 SIMD snapshot-isolation scan.
//!
//! Direct lift of the in-process TimePointTile to MMF. The SIMD
//! code is unchanged because AVX2 instructions operate on memory
//! addresses identically whether the address is stack-local or
//! memory-mapped. Cross-process safety comes from atomic insert
//! (CAS on the occupied bitmap) and atomic version writes; the
//! SIMD scan is a pure read (no synchronization needed since
//! version writes are AtomicU64 with Release semantics).
//!
//! # Layout
//!
//! ```text
//! +-----------------------------+
//! | TileHeader (64B)            |
//! |   - magic                   |
//! |   - capacity (always 16)    |
//! |   - payload_size            |
//! |   - occupied: AtomicU32     |
//! +-----------------------------+
//! | VersionedSlot[0] (64B)      |
//! |   - version: AtomicU64      |
//! |   - payload: [u8; 56]       |
//! +-----------------------------+
//! | ... 15 more slots           |
//! +-----------------------------+
//! ```

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const TIME_POINT_MAGIC: u64 = 0x4150_4D46_5450_5054;
pub const TILE_CAP: usize = 16;
pub const SLOT_PAYLOAD: usize = 56;

#[repr(C, align(64))]
pub struct TileHeader {
    pub magic: u64,
    pub capacity: u32,
    pub payload_size: u32,
    pub occupied: AtomicU32,
    _pad: [u8; 44],
}

#[repr(C, align(64))]
pub struct VersionedSlot {
    pub version: AtomicU64,
    pub payload: [u8; SLOT_PAYLOAD],
}

pub const fn tile_file_size() -> usize {
    size_of::<TileHeader>() + TILE_CAP * size_of::<VersionedSlot>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TileError {
    LayoutMismatch,
    PayloadTooLarge,
    Full,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for TileError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedTimePointTile<T: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Send + 'static> Send for SharedTimePointTile<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for SharedTimePointTile<T> {}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedTimePointTile<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + 'static> SharedTimePointTile<T> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, TileError> {
        Self::check_layout()?;
        let total = tile_file_size();
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut TileHeader;
        unsafe {
            std::ptr::write(hdr, TileHeader {
                magic: TIME_POINT_MAGIC,
                capacity: TILE_CAP as u32,
                payload_size: size_of::<T>() as u32,
                occupied: AtomicU32::new(0),
                _pad: [0; 44],
            });
        }
        let slots_base = unsafe { mmap.as_mut_ptr().add(size_of::<TileHeader>()) };
        for i in 0..TILE_CAP {
            let slot_ptr = unsafe {
                slots_base.add(i * size_of::<VersionedSlot>()) as *mut VersionedSlot
            };
            unsafe {
                std::ptr::write(slot_ptr, VersionedSlot {
                    version: AtomicU64::new(0),
                    payload: [0; SLOT_PAYLOAD],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, TileError> {
        Self::check_layout()?;
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < tile_file_size() as u64 {
            return Err(TileError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(tile_file_size()).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const TileHeader) };
        if hdr.magic != TIME_POINT_MAGIC
            || hdr.capacity != TILE_CAP as u32
            || hdr.payload_size as usize != size_of::<T>()
        {
            return Err(TileError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn check_layout() -> Result<(), TileError> {
        if size_of::<T>() > SLOT_PAYLOAD {
            return Err(TileError::PayloadTooLarge);
        }
        if align_of::<T>() > 8 {
            return Err(TileError::PayloadTooLarge);
        }
        Ok(())
    }

    pub fn header(&self) -> &TileHeader {
        unsafe { &*(self.mmap.as_ptr() as *const TileHeader) }
    }

    fn slot(&self, idx: usize) -> &VersionedSlot {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<TileHeader>()) };
        unsafe {
            &*(base.add(idx * size_of::<VersionedSlot>()) as *const VersionedSlot)
        }
    }

    /// Atomic insert via CAS on the occupied bitmap. Returns the
    /// claimed lane index, or `Err(Full)`.
    pub fn insert(&self, version: u64, value: T) -> Result<usize, TileError> {
        let header = self.header();
        loop {
            let cur = header.occupied.load(Ordering::Acquire);
            let free = !cur & ((1u32 << TILE_CAP) - 1);
            if free == 0 {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::versioned::OP_PUSH, 1);
                return Err(TileError::Full);
            }
            let lane = free.trailing_zeros() as usize;
            let new_occupied = cur | (1u32 << lane);
            if header.occupied.compare_exchange_weak(
                cur, new_occupied, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                let slot = self.slot(lane);
                // SAFETY: lane is now exclusively ours (CAS won).
                unsafe {
                    let dst = slot.payload.as_ptr() as *mut T;
                    std::ptr::write_unaligned(dst, value);
                }
                slot.version.store(version, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::versioned::OP_PUSH, 0);
                return Ok(lane);
            }
            std::hint::spin_loop();
        }
    }

    pub fn remove(&self, lane: usize) {
        if lane < TILE_CAP {
            self.header().occupied.fetch_and(!(1u32 << lane), Ordering::AcqRel);
        }
    }

    pub fn len(&self) -> usize {
        self.header().occupied.load(Ordering::Acquire).count_ones() as usize
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn is_full(&self) -> bool {
        self.header().occupied.load(Ordering::Acquire) == ((1u32 << TILE_CAP) - 1)
    }

    /// SIMD scan: return a 16-bit lane mask of entries with version
    /// <= snapshot AND currently occupied. AVX2 uses the unsigned-
    /// compare-via-sign-XOR trick because cmpgt_epi64 is signed.
    #[inline]
    pub fn visible_mask(&self, snapshot: u64) -> u16 {
        let header = self.header();
        let occupied = header.occupied.load(Ordering::Acquire) as u16;
        self.ring_sidecar.push_op(
            crate::sidecar_ops::versioned::OP_VISIBLE_MASK,
            if occupied == 0 { 2 } else { 0 },
        );
        if occupied == 0 { return 0; }
        let versions_base = unsafe {
            self.mmap.as_ptr().add(size_of::<TileHeader>())
        };
        // The versions are at offset 0 of each VersionedSlot. We
        // need a contiguous u64 array of versions for the SIMD load;
        // since slots are 64-byte aligned and versions are at slot
        // offset 0, a naive gather is needed. For simplicity we
        // copy into a stack buffer; the bench shows this is still
        // very fast for 16 entries.
        let mut versions = [0u64; TILE_CAP];
        for (i, v) in versions.iter_mut().enumerate() {
            let slot = unsafe {
                &*(versions_base.add(i * size_of::<VersionedSlot>()) as *const VersionedSlot)
            };
            *v = slot.version.load(Ordering::Acquire);
        }
        Self::simd_visible_mask(&versions, snapshot) & occupied
    }

    /// SIMD visibility scan dispatcher. Picks AVX-512F (one ZMM
    /// per 8-lane half + mask-producing `_mm512_cmple_epu64_mask`)
    /// when present, AVX2 (4 YMM compares with sign-bit XOR trick)
    /// otherwise, scalar on non-x86 or feature-stripped builds.
    #[inline]
    pub fn simd_visible_mask(versions: &[u64; TILE_CAP], snapshot: u64) -> u16 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                // SAFETY: AVX-512F runtime-detected.
                return unsafe { Self::simd_visible_mask_avx512(versions, snapshot) };
            }
            if std::is_x86_feature_detected!("avx2") {
                // SAFETY: AVX2 runtime-detected.
                return unsafe { Self::simd_visible_mask_avx2(versions, snapshot) };
            }
        }
        Self::simd_visible_mask_scalar(versions, snapshot)
    }

    /// AVX-512F path: TILE_CAP=16 covered by two 8-u64 chunks. Each
    /// chunk uses one `_mm512_loadu_si512` and one
    /// `_mm512_cmple_epu64_mask` (returns `__mmask8` directly - no
    /// sign-bit XOR trick needed because the instruction is unsigned
    /// natively). Two 8-bit masks pack into the 16-bit result via
    /// `low | (high << 8)`.
    ///
    /// # Safety
    /// Caller must ensure AVX-512F is available.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    pub unsafe fn simd_visible_mask_avx512(
        versions: &[u64; TILE_CAP],
        snapshot: u64,
    ) -> u16 {
        use std::arch::x86_64::*;
        let snap = _mm512_set1_epi64(snapshot as i64);
        // SAFETY: versions is 16 contiguous u64s; two 8-u64 loads at
        // offsets 0 and 8 cover the full tile.
        let v_lo = unsafe {
            _mm512_loadu_si512(versions.as_ptr() as *const __m512i)
        };
        let v_hi = unsafe {
            _mm512_loadu_si512(versions.as_ptr().add(8) as *const __m512i)
        };
        let mask_lo: u8 = _mm512_cmple_epu64_mask(v_lo, snap);
        let mask_hi: u8 = _mm512_cmple_epu64_mask(v_hi, snap);
        (mask_lo as u16) | ((mask_hi as u16) << 8)
    }

    /// AVX2 path: 4 chunks of 4 u64. Signed `cmpgt_epi64` plus
    /// sign-bit XOR delivers the unsigned `<=` predicate; `cmpeq`
    /// handles the equality boundary.
    ///
    /// # Safety
    /// Caller must ensure AVX2 is available.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    pub unsafe fn simd_visible_mask_avx2(
        versions: &[u64; TILE_CAP],
        snapshot: u64,
    ) -> u16 {
        use std::arch::x86_64::*;
        let sign_bit = _mm256_set1_epi64x(i64::MIN);
        let snap_raw = _mm256_set1_epi64x(snapshot as i64);
        let snap_s = _mm256_xor_si256(snap_raw, sign_bit);
        // SAFETY: versions is 16 contiguous u64s; four 4-u64 loads at
        // offsets 0, 4, 8, 12 cover the full tile.
        let load_xord = |off: usize| -> __m256i {
            let raw = unsafe {
                _mm256_loadu_si256(versions.as_ptr().add(off) as *const __m256i)
            };
            _mm256_xor_si256(raw, sign_bit)
        };
        let load_raw = |off: usize| -> __m256i {
            unsafe {
                _mm256_loadu_si256(versions.as_ptr().add(off) as *const __m256i)
            }
        };
        let v0 = load_xord(0);
        let v1 = load_xord(4);
        let v2 = load_xord(8);
        let v3 = load_xord(12);
        let raw0 = load_raw(0);
        let raw1 = load_raw(4);
        let raw2 = load_raw(8);
        let raw3 = load_raw(12);
        let gt0 = _mm256_cmpgt_epi64(snap_s, v0);
        let gt1 = _mm256_cmpgt_epi64(snap_s, v1);
        let gt2 = _mm256_cmpgt_epi64(snap_s, v2);
        let gt3 = _mm256_cmpgt_epi64(snap_s, v3);
        let eq0 = _mm256_cmpeq_epi64(snap_raw, raw0);
        let eq1 = _mm256_cmpeq_epi64(snap_raw, raw1);
        let eq2 = _mm256_cmpeq_epi64(snap_raw, raw2);
        let eq3 = _mm256_cmpeq_epi64(snap_raw, raw3);
        let m0 = _mm256_or_si256(gt0, eq0);
        let m1 = _mm256_or_si256(gt1, eq1);
        let m2 = _mm256_or_si256(gt2, eq2);
        let m3 = _mm256_or_si256(gt3, eq3);
        let bits0 = _mm256_movemask_pd(_mm256_castsi256_pd(m0)) as u16;
        let bits1 = _mm256_movemask_pd(_mm256_castsi256_pd(m1)) as u16;
        let bits2 = _mm256_movemask_pd(_mm256_castsi256_pd(m2)) as u16;
        let bits3 = _mm256_movemask_pd(_mm256_castsi256_pd(m3)) as u16;
        bits0 | (bits1 << 4) | (bits2 << 8) | (bits3 << 12)
    }

    /// Scalar reference: always available, used as the fallback for
    /// non-x86 builds and as the ground-truth oracle in tests.
    #[inline]
    pub fn simd_visible_mask_scalar(versions: &[u64; TILE_CAP], snapshot: u64) -> u16 {
        let mut mask = 0u16;
        for (i, v) in versions.iter().enumerate() {
            if *v <= snapshot {
                mask |= 1u16 << i;
            }
        }
        mask
    }

    /// Read the payload at `lane` if occupied.
    pub fn at(&self, lane: usize) -> Option<(u64, T)> {
        if lane >= TILE_CAP {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::versioned::OP_READ_AT, 2);
            return None;
        }
        let occ = self.header().occupied.load(Ordering::Acquire);
        if (occ >> lane) & 1 == 0 {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::versioned::OP_READ_AT, 2);
            return None;
        }
        let slot = self.slot(lane);
        let v = slot.version.load(Ordering::Acquire);
        let value: T = unsafe {
            let src = slot.payload.as_ptr() as *const T;
            std::ptr::read_unaligned(src)
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::versioned::OP_READ_AT, 0);
        Some((v, value))
    }

    /// Count visible at `snapshot`.
    pub fn visible_count(&self, snapshot: u64) -> u32 {
        self.visible_mask(snapshot).count_ones()
    }

    pub fn flush(&self) -> Result<(), TileError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), TileError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-tile-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn empty_tile_visible_mask_is_zero() {
        let p = tmp("empty");
        let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        assert_eq!(t.visible_mask(u64::MAX), 0);
        assert!(t.is_empty());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn insert_then_visible_at_snapshot() {
        let p = tmp("visible");
        let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        t.insert(10, 100).unwrap();
        t.insert(20, 200).unwrap();
        t.insert(30, 300).unwrap();
        // Snapshot 25 sees lanes 0+1 (versions 10, 20).
        let m = t.visible_mask(25);
        assert_eq!(m, 0b011);
        assert_eq!(t.visible_count(25), 2);
        // Snapshot u64::MAX sees all three.
        let m_all = t.visible_mask(u64::MAX);
        assert_eq!(m_all.count_ones(), 3);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn fill_to_capacity_then_overflow() {
        let p = tmp("fill");
        let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        for i in 0..TILE_CAP as u64 {
            t.insert(i, i * 10).unwrap();
        }
        assert!(t.is_full());
        assert_eq!(t.insert(99, 999).unwrap_err(), TileError::Full);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn remove_frees_lane_for_reinsert() {
        let p = tmp("remove");
        let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        let l0 = t.insert(1, 100).unwrap();
        let l1 = t.insert(2, 200).unwrap();
        t.remove(l0);
        assert_eq!(t.len(), 1);
        let l2 = t.insert(3, 300).unwrap();
        assert_eq!(l2, l0, "freed lane reused");
        assert_eq!(t.at(l1), Some((2, 200)));
        assert_eq!(t.at(l2), Some((3, 300)));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_lanes_dont_match_zero_snapshot() {
        let p = tmp("zero-snap");
        let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        t.insert(0, 100).unwrap();
        // Snapshot 0: only the occupied lane with version 0 matches.
        let m = t.visible_mask(0);
        assert_eq!(m, 0b1);
        assert_eq!(m.count_ones(), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn simd_matches_scalar_for_boundary_snapshots() {
        let p = tmp("simd-vs-scalar");
        let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        let versions = [5u64, 10, 15, 20, 25, 30, 35, 40,
                        45, 50, 55, 60, 65, 70, 75, 80];
        for &v in versions.iter() {
            t.insert(v, v * 100).unwrap();
        }
        for snap in [0u64, 10, 35, 80, 100, u64::MAX] {
            let simd = t.visible_mask(snap);
            let mut scalar = 0u16;
            for (i, &v) in versions.iter().enumerate() {
                if v <= snap { scalar |= 1 << i; }
            }
            assert_eq!(simd, scalar, "snap={snap}: simd={simd:#b} scalar={scalar:#b}");
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_inserts_visible() {
        let p = tmp("cross-handle");
        let writer: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
        let reader: SharedTimePointTile<u64> = SharedTimePointTile::open(&p).unwrap();
        writer.insert(10, 100).unwrap();
        writer.insert(20, 200).unwrap();
        assert_eq!(reader.visible_count(u64::MAX), 2);
        let m = reader.visible_mask(15);
        assert_eq!(m.count_ones(), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
            t.insert(7, 70).unwrap();
            t.insert(8, 80).unwrap();
            t.flush().unwrap();
        }
        let t2: SharedTimePointTile<u64> = SharedTimePointTile::open(&p).unwrap();
        assert_eq!(t2.len(), 2);
        assert_eq!(t2.visible_count(u64::MAX), 2);
        assert_eq!(t2.at(0), Some((7, 70)));
        assert_eq!(t2.at(1), Some((8, 80)));
        std::fs::remove_file(&p).ok();
    }
}
