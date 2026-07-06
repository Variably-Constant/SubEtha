//! `FrameRegion` - concurrent fixed-block payload region for the
//! self-describing offset path shared by every `AdaptiveRing` shape.
//!
//! Records too large to inline in a ring slot spill here: the producer
//! allocates a block, copies the payload in, and writes the block index
//! into the ring descriptor; the consumer reads the block and frees it.
//! Because the offset payloads of every shape (SPSC / MPSC / MPMC /
//! Vyukov) land in one region, the allocator must be safe for many
//! producers allocating and many consumers freeing at once, in any
//! order. That is a Treiber-stack free list with an ABA counter plus a
//! bump high-water mark - the same allocator
//! [`SharedRegion`](crate::shared_region::SharedRegion) ships, here with
//! a runtime block size instead of a const-generic `T` so the
//! `AdaptiveRing` can size its blocks to the workload.
//!
//! Reclaim order does not matter: a freed block returns to the stack
//! and is handed to the next allocation regardless of which consumer
//! freed it, so no FIFO bookkeeping is needed across consumers.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_ring::RingError;

/// Magic identifying a `FrameRegion` layout. ASCII "FRGN" + version.
pub const FRAME_REGION_MAGIC: u64 = 0x4652_474e_0000_0001;

/// Free-list sentinel: "no next block".
const NIL: u32 = u32::MAX;

/// Smallest block: must hold the 4-byte free-list link.
pub const MIN_BLOCK_SIZE: usize = 8;

#[inline]
fn pack(counter: u32, index: u32) -> u64 {
    ((counter as u64) << 32) | (index as u64)
}
#[inline]
fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// Header: metadata line, then the bump cursor and the free-list head
/// each on their own cache line so allocators and freers do not
/// false-share.
#[repr(C, align(64))]
struct FrameRegionHeader {
    magic: u64,
    block_size: u64,
    block_count: u64,
    _pad_meta: [u8; 64 - 24],
    /// Bump high-water mark (next never-yet-allocated block).
    bump_next: AtomicU32,
    _pad_bump: [u8; 64 - 4],
    /// Treiber-stack free-list head, ABA-tagged (`counter << 32 | idx`).
    free_head: AtomicU64,
    _pad_free: [u8; 64 - 8],
}

/// Total mapped bytes for `block_count` blocks of `block_size`.
pub const fn frame_region_file_size(block_size: usize, block_count: usize) -> usize {
    std::mem::size_of::<FrameRegionHeader>() + block_size * block_count
}

#[allow(dead_code)]
enum RegionBacking {
    Anon(MmapMut),
    File(File, MmapMut),
    Shm(crate::shm_file::ShmFile),
}

/// Concurrent fixed-block region. Multi-producer `alloc`,
/// multi-consumer `free`, any-order reclaim.
pub struct FrameRegion {
    _backing: RegionBacking,
    raw_ptr: *mut u8,
    block_size: usize,
    block_count: usize,
    blocks_base: usize,
}

unsafe impl Send for FrameRegion {}
unsafe impl Sync for FrameRegion {}

fn validate(block_size: usize, block_count: usize) -> Result<(), RingError> {
    if block_size < MIN_BLOCK_SIZE || !block_size.is_multiple_of(8) {
        return Err(RingError::LayoutMismatch);
    }
    if block_count < 1 || block_count >= NIL as usize {
        return Err(RingError::LayoutMismatch);
    }
    Ok(())
}

unsafe fn init_region(ptr: *mut u8, block_size: usize, block_count: usize) {
    unsafe {
        std::ptr::write(ptr as *mut FrameRegionHeader, FrameRegionHeader {
            magic: FRAME_REGION_MAGIC,
            block_size: block_size as u64,
            block_count: block_count as u64,
            _pad_meta: [0; 64 - 24],
            bump_next: AtomicU32::new(0),
            _pad_bump: [0; 64 - 4],
            free_head: AtomicU64::new(pack(0, NIL)),
            _pad_free: [0; 64 - 8],
        });
    }
}

impl FrameRegion {
    /// Anonymous in-process region.
    pub fn create_anon(block_size: usize, block_count: usize) -> Result<Self, RingError> {
        validate(block_size, block_count)?;
        let total = frame_region_file_size(block_size, block_count);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        unsafe { init_region(mmap.as_mut_ptr(), block_size, block_count) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self::from_parts(RegionBacking::Anon(mmap), raw_ptr, block_size, block_count))
    }

    /// File-backed region; cross-process via the page cache.
    pub fn create(
        path: impl AsRef<Path>, block_size: usize, block_count: usize,
    ) -> Result<Self, RingError> {
        validate(block_size, block_count)?;
        let total = frame_region_file_size(block_size, block_count);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        unsafe { init_region(mmap.as_mut_ptr(), block_size, block_count) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self::from_parts(RegionBacking::File(file, mmap), raw_ptr, block_size, block_count))
    }

    /// Open an existing file-backed region. Validates the header.
    pub fn open(
        path: impl AsRef<Path>, block_size: usize, block_count: usize,
    ) -> Result<Self, RingError> {
        validate(block_size, block_count)?;
        let total = frame_region_file_size(block_size, block_count);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(RingError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::check_header(mmap.as_ptr(), block_size, block_count)?;
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self::from_parts(RegionBacking::File(file, mmap), raw_ptr, block_size, block_count))
    }

    /// Build a region on a named RAM-resident shared-memory backing.
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile, block_size: usize, block_count: usize,
    ) -> Result<Self, RingError> {
        validate(block_size, block_count)?;
        if shm.len() < frame_region_file_size(block_size, block_count) {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        unsafe { init_region(raw_ptr, block_size, block_count) };
        Ok(Self::from_parts(RegionBacking::Shm(shm), raw_ptr, block_size, block_count))
    }

    /// Open an existing named ShmFs-backed region (no re-init).
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile, block_size: usize, block_count: usize,
    ) -> Result<Self, RingError> {
        validate(block_size, block_count)?;
        if shm.len() < frame_region_file_size(block_size, block_count) {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        Self::check_header(raw_ptr, block_size, block_count)?;
        Ok(Self::from_parts(RegionBacking::Shm(shm), raw_ptr, block_size, block_count))
    }

    fn from_parts(
        backing: RegionBacking, raw_ptr: *mut u8, block_size: usize, block_count: usize,
    ) -> Self {
        Self {
            _backing: backing, raw_ptr, block_size, block_count,
            blocks_base: std::mem::size_of::<FrameRegionHeader>(),
        }
    }

    fn check_header(ptr: *const u8, block_size: usize, block_count: usize) -> Result<(), RingError> {
        let h = unsafe { &*(ptr as *const FrameRegionHeader) };
        if h.magic != FRAME_REGION_MAGIC
            || h.block_size != block_size as u64
            || h.block_count != block_count as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        Ok(())
    }

    /// Largest payload a block holds.
    pub fn block_size(&self) -> usize { self.block_size }
    /// Number of blocks.
    pub fn block_count(&self) -> usize { self.block_count }

    fn header(&self) -> &FrameRegionHeader {
        unsafe { &*(self.raw_ptr as *const FrameRegionHeader) }
    }

    fn block_ptr(&self, idx: u32) -> *mut u8 {
        unsafe { self.raw_ptr.add(self.blocks_base + idx as usize * self.block_size) }
    }

    /// The block's first 4 bytes reinterpreted as the free-list link
    /// (only meaningful while the block is free).
    fn next_link(&self, idx: u32) -> &AtomicU32 {
        unsafe { &*(self.block_ptr(idx) as *const AtomicU32) }
    }

    /// Allocate a block. Free list first, then bump. `None` when full.
    pub fn alloc(&self) -> Option<u32> {
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, idx) = unpack(head);
            if idx == NIL {
                break;
            }
            let next = self.next_link(idx).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Some(idx);
            }
        }
        let idx = self.header().bump_next.fetch_add(1, Ordering::AcqRel);
        if idx >= self.block_count as u32 {
            self.header().bump_next.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(idx)
    }

    /// Return a block to the free list. Any consumer may free any block.
    pub fn free(&self, idx: u32) {
        if idx as usize >= self.block_count {
            return;
        }
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(idx).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), idx);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return;
            }
        }
    }

    /// Copy `payload` into block `idx`. Caller guarantees
    /// `payload.len() <= block_size`.
    pub fn write_block(&self, idx: u32, payload: &[u8]) {
        debug_assert!(payload.len() <= self.block_size);
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(), self.block_ptr(idx), payload.len(),
            );
        }
    }

    /// Copy `len` bytes out of block `idx` into `out` (appended).
    pub fn read_block_into(&self, idx: u32, len: usize, out: &mut Vec<u8>) {
        debug_assert!(len <= self.block_size);
        out.reserve(len);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.block_ptr(idx),
                out.spare_capacity_mut().as_mut_ptr() as *mut u8,
                len,
            );
            let new_len = out.len() + len;
            out.set_len(new_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn alloc_write_read_free_cycle() {
        let r = FrameRegion::create_anon(256, 8).unwrap();
        let idx = r.alloc().unwrap();
        let payload = vec![0xABu8; 200];
        r.write_block(idx, &payload);
        let mut out = Vec::new();
        r.read_block_into(idx, 200, &mut out);
        assert_eq!(out, payload);
        r.free(idx);
        // Freed block is reused by the next alloc.
        let idx2 = r.alloc().unwrap();
        assert_eq!(idx2, idx, "freed block returns to the stack");
    }

    #[test]
    fn exhausts_then_full() {
        let r = FrameRegion::create_anon(64, 4).unwrap();
        let a: Vec<u32> = (0..4).map(|_| r.alloc().unwrap()).collect();
        assert_eq!(a.len(), 4);
        assert!(r.alloc().is_none(), "region full");
        r.free(a[1]);
        assert!(r.alloc().is_some(), "freeing reopens a block");
    }

    #[test]
    fn concurrent_alloc_free_no_double_issue() {
        // Many threads alloc + free in a loop; assert no index is ever
        // held by two threads at once (a double-issue would corrupt).
        let r = Arc::new(FrameRegion::create_anon(64, 64).unwrap());
        let held: Arc<Vec<AtomicUsize>> =
            Arc::new((0..64).map(|_| AtomicUsize::new(0)).collect());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let r = r.clone();
            let held = held.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..20_000 {
                    if let Some(idx) = r.alloc() {
                        let prev = held[idx as usize].fetch_add(1, Ordering::AcqRel);
                        assert_eq!(prev, 0, "block {idx} double-issued");
                        held[idx as usize].fetch_sub(1, Ordering::AcqRel);
                        r.free(idx);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn shm_cross_handle() {
        use crate::shm_file::ShmFile;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let name = format!("frame_region_{}_{}", std::process::id(), nonce);
        let (bs, bc) = (256usize, 8usize);
        let size = frame_region_file_size(bs, bc);
        let a = FrameRegion::create_from_shm(
            ShmFile::create_or_open_named(&name, size).unwrap(), bs, bc).unwrap();
        let b = FrameRegion::open_from_shm(
            ShmFile::create_or_open_named(&name, size).unwrap(), bs, bc).unwrap();
        let idx = a.alloc().unwrap();
        a.write_block(idx, b"shared across handles");
        let mut out = Vec::new();
        b.read_block_into(idx, 21, &mut out);
        assert_eq!(out, b"shared across handles");
    }

    #[test]
    fn rejects_bad_params() {
        assert!(matches!(FrameRegion::create_anon(7, 8), Err(RingError::LayoutMismatch)));
        assert!(matches!(FrameRegion::create_anon(64, 0), Err(RingError::LayoutMismatch)));
    }
}
