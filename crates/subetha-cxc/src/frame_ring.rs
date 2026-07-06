//! `FrameRing` - self-describing variable-payload SPSC ring.
//!
//! Where [`SpscRingCore`](crate::spsc_ring::SpscRingCore) carries a
//! fixed 64-byte payload and rejects anything larger
//! ([`RingError::PayloadTooLarge`]), `FrameRing` makes the payload
//! layout part of the record itself. Every record is a self-describing
//! frame - a one-byte class tag plus a length - so the ring carries a
//! payload of *any* size, inlining the small ones and spilling the
//! large ones to a co-located byte region, with the consumer reading
//! the class to know which path to take. This is the QUIC frame model
//! (a type tag plus length-delimited fields) applied to the ring slot.
//!
//! # The two layers
//!
//! 1. **Descriptor ring** - a fixed-stride Lamport SPSC ring (one
//!    producer-owned `desc_head`, one consumer-owned `desc_tail`).
//!    Fixed stride keeps the O(1) `index -> address` arithmetic, the
//!    one-Acquire-one-Release atomic budget, and cache-line isolation
//!    that the raw SPSC ring earns. Each slot is
//!    `[class:u8][_pad:3][len:u32][ inline-bytes | region_off:u64 ]`.
//! 2. **Payload region** - a bip-buffer byte ring (absolute-monotonic
//!    `region_head` / `region_tail` cursors). Records spill here only
//!    when they exceed the inline budget; the descriptor then carries
//!    the region offset instead of the bytes.
//!
//! # Per-op layout selection
//!
//! `send` picks inline when `payload.len() <= inline_budget`, else the
//! region. `send_as` lets the producer override
//! ([`LayoutHint::ForceInline`] / [`LayoutHint::ForceOffset`]). The
//! consumer never overrides: it reads the class tag the producer wrote,
//! because the consumer cannot know the layout without reading it.
//!
//! # Wrap correctness
//!
//! The region cursors are absolute monotonic counters addressed
//! `% region_bytes`. When a record would straddle the region end the
//! producer skip-pads to the next wrap boundary and records the
//! post-skip offset in the descriptor. Region payloads are capped at
//! `region_bytes / 2` so a skip-pad on an empty region can never report
//! a false `Full` (the skipped tail plus the record always fit).
//!
//! # Crash recovery
//!
//! Identical in shape to the raw SPSC ring: a producer that dies
//! between writing a slot and the Release-store on `desc_head` leaves
//! the slot unpublished, so the consumer never reads it. Region bytes
//! are published before the descriptor, so a consumer that observes a
//! descriptor always observes its region bytes.

use std::cell::UnsafeCell;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_ring::RingError;

/// Magic identifying a `FrameRing` layout. ASCII "FRMR" + version byte.
pub const FRAME_MAGIC: u64 = 0x4652_4d52_0000_0001;

/// Descriptor header bytes: `class:u8` + `_pad:3` + `len:u32`. The
/// inline payload (or the 8-byte region offset) follows at byte 8.
pub const DESC_HEADER_BYTES: usize = 8;

/// Smallest descriptor slot: 8-byte header + 8-byte region offset.
pub const MIN_SLOT_SIZE: usize = DESC_HEADER_BYTES + 8;

/// How a record's payload is stored. The producer writes the tag; the
/// consumer reads it to know how to recover the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameClass {
    /// Payload bytes live inline in the descriptor slot.
    Inline = 0,
    /// Payload bytes live in the byte region; the descriptor carries
    /// the region offset.
    Offset = 1,
}

/// Producer-side override for the per-record layout decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LayoutHint {
    /// Inline when it fits the budget, else spill to the region.
    #[default]
    Auto,
    /// Force inline; returns [`RingError::PayloadTooLarge`] if the
    /// payload exceeds the inline budget.
    ForceInline,
    /// Force the region path even when the payload would fit inline.
    ForceOffset,
}

/// Header for a `FrameRing`. Five cache lines: metadata, then each
/// cursor on its own line so producer and consumer never false-share.
#[repr(C, align(64))]
struct FrameHeader {
    magic: u64,
    capacity: u64,
    slot_size: u64,
    region_bytes: u64,
    inline_budget: u64,
    _pad_meta: [u8; 64 - 40],
    /// Producer-owned descriptor head.
    desc_head: AtomicU64,
    _pad_dh: [u8; 64 - 8],
    /// Consumer-owned descriptor tail.
    desc_tail: AtomicU64,
    _pad_dt: [u8; 64 - 8],
    /// Producer-owned region byte head (absolute monotonic).
    region_head: AtomicU64,
    _pad_rh: [u8; 64 - 8],
    /// Consumer-owned region byte tail (absolute monotonic).
    region_tail: AtomicU64,
    _pad_rt: [u8; 64 - 8],
}

/// Total mapped bytes for a frame ring of `capacity` descriptor slots
/// (`slot_size` each) plus a `region_bytes` payload region.
pub const fn frame_ring_file_size(
    capacity: usize, slot_size: usize, region_bytes: usize,
) -> usize {
    std::mem::size_of::<FrameHeader>() + capacity * slot_size + region_bytes
}

/// Marker so the header pointer is treated as shared mutable state.
#[allow(dead_code)]
struct FrameCell(UnsafeCell<u8>);

#[allow(dead_code)]
enum FrameBacking {
    Anon(MmapMut),
    File(File, MmapMut),
    Shm(crate::shm_file::ShmFile),
}

/// Self-describing variable-payload SPSC ring. One producer, one
/// consumer. Carries any payload size: small inline, large via the
/// co-located byte region, the layout chosen per record and recorded
/// in the descriptor.
pub struct FrameRing {
    _backing: FrameBacking,
    raw_ptr: *mut u8,
    capacity: usize,
    slot_size: usize,
    region_bytes: usize,
    inline_budget: usize,
    desc_base: usize,
    region_base: usize,
}

unsafe impl Send for FrameRing {}
unsafe impl Sync for FrameRing {}

fn validate_params(capacity: usize, slot_size: usize, region_bytes: usize)
    -> Result<(), RingError>
{
    if !capacity.is_power_of_two() || capacity < 2 {
        return Err(RingError::LayoutMismatch);
    }
    if slot_size < MIN_SLOT_SIZE {
        return Err(RingError::LayoutMismatch);
    }
    if !region_bytes.is_power_of_two() || region_bytes < 2 {
        return Err(RingError::LayoutMismatch);
    }
    Ok(())
}

unsafe fn init_frame_layout_raw(
    ptr: *mut u8, capacity: usize, slot_size: usize, region_bytes: usize,
) {
    let inline_budget = slot_size - DESC_HEADER_BYTES;
    unsafe {
        std::ptr::write(ptr as *mut FrameHeader, FrameHeader {
            magic: FRAME_MAGIC,
            capacity: capacity as u64,
            slot_size: slot_size as u64,
            region_bytes: region_bytes as u64,
            inline_budget: inline_budget as u64,
            _pad_meta: [0; 64 - 40],
            desc_head: AtomicU64::new(0),
            _pad_dh: [0; 64 - 8],
            desc_tail: AtomicU64::new(0),
            _pad_dt: [0; 64 - 8],
            region_head: AtomicU64::new(0),
            _pad_rh: [0; 64 - 8],
            region_tail: AtomicU64::new(0),
            _pad_rt: [0; 64 - 8],
        });
        // Zero the descriptor slots so a stale class byte from a prior
        // mapping cannot be misread before its slot is published.
        let desc_base = std::mem::size_of::<FrameHeader>();
        std::ptr::write_bytes(ptr.add(desc_base), 0, capacity * slot_size);
    }
}

impl FrameRing {
    /// Anonymous in-process frame ring. `slot_size` is the descriptor
    /// stride (inline budget is `slot_size - 8`); `region_bytes` sizes
    /// the spill region (payloads cap at `region_bytes / 2`).
    pub fn create_anon(
        capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Result<Self, RingError> {
        validate_params(capacity, slot_size, region_bytes)?;
        let total = frame_ring_file_size(capacity, slot_size, region_bytes);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        unsafe { init_frame_layout_raw(mmap.as_mut_ptr(), capacity, slot_size, region_bytes) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self::from_parts(
            FrameBacking::Anon(mmap), raw_ptr, capacity, slot_size, region_bytes,
        ))
    }

    /// File-backed frame ring; cross-process via the OS page cache.
    pub fn create(
        path: impl AsRef<Path>, capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Result<Self, RingError> {
        validate_params(capacity, slot_size, region_bytes)?;
        let total = frame_ring_file_size(capacity, slot_size, region_bytes);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        unsafe { init_frame_layout_raw(mmap.as_mut_ptr(), capacity, slot_size, region_bytes) };
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self::from_parts(
            FrameBacking::File(file, mmap), raw_ptr, capacity, slot_size, region_bytes,
        ))
    }

    /// Open an existing file-backed frame ring. Validates the header.
    pub fn open(
        path: impl AsRef<Path>, capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Result<Self, RingError> {
        validate_params(capacity, slot_size, region_bytes)?;
        let total = frame_ring_file_size(capacity, slot_size, region_bytes);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(RingError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::check_header(mmap.as_ptr(), capacity, slot_size, region_bytes)?;
        let raw_ptr = mmap.as_mut_ptr();
        Ok(Self::from_parts(
            FrameBacking::File(file, mmap), raw_ptr, capacity, slot_size, region_bytes,
        ))
    }

    /// Build a fresh frame ring on a named RAM-resident shared-memory
    /// backing (cross-process, never touches the page cache).
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Result<Self, RingError> {
        validate_params(capacity, slot_size, region_bytes)?;
        let total = frame_ring_file_size(capacity, slot_size, region_bytes);
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        unsafe { init_frame_layout_raw(raw_ptr, capacity, slot_size, region_bytes) };
        Ok(Self::from_parts(
            FrameBacking::Shm(shm), raw_ptr, capacity, slot_size, region_bytes,
        ))
    }

    /// Open an existing named ShmFs-backed frame ring (no re-init).
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Result<Self, RingError> {
        validate_params(capacity, slot_size, region_bytes)?;
        let total = frame_ring_file_size(capacity, slot_size, region_bytes);
        if shm.len() < total {
            return Err(RingError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        Self::check_header(raw_ptr, capacity, slot_size, region_bytes)?;
        Ok(Self::from_parts(
            FrameBacking::Shm(shm), raw_ptr, capacity, slot_size, region_bytes,
        ))
    }

    fn from_parts(
        backing: FrameBacking, raw_ptr: *mut u8,
        capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Self {
        let desc_base = std::mem::size_of::<FrameHeader>();
        let region_base = desc_base + capacity * slot_size;
        Self {
            _backing: backing, raw_ptr, capacity, slot_size, region_bytes,
            inline_budget: slot_size - DESC_HEADER_BYTES,
            desc_base, region_base,
        }
    }

    fn check_header(
        ptr: *const u8, capacity: usize, slot_size: usize, region_bytes: usize,
    ) -> Result<(), RingError> {
        let header = unsafe { &*(ptr as *const FrameHeader) };
        if header.magic != FRAME_MAGIC
            || header.capacity != capacity as u64
            || header.slot_size != slot_size as u64
            || header.region_bytes != region_bytes as u64
        {
            return Err(RingError::LayoutMismatch);
        }
        Ok(())
    }

    /// Descriptor slot count (power of 2).
    pub fn capacity(&self) -> usize { self.capacity }
    /// Descriptor stride in bytes.
    pub fn slot_size(&self) -> usize { self.slot_size }
    /// Largest payload stored inline (`slot_size - 8`).
    pub fn inline_budget(&self) -> usize { self.inline_budget }
    /// Byte-region size. Region payloads cap at half this.
    pub fn region_bytes(&self) -> usize { self.region_bytes }
    /// Largest payload the region accepts (`region_bytes / 2`).
    pub fn max_payload(&self) -> usize { self.region_bytes / 2 }

    fn header(&self) -> &FrameHeader {
        unsafe { &*(self.raw_ptr as *const FrameHeader) }
    }

    fn desc_slot_ptr(&self, idx: u64) -> *mut u8 {
        let masked = (idx as usize) & (self.capacity - 1);
        unsafe { self.raw_ptr.add(self.desc_base + masked * self.slot_size) }
    }

    fn region_ptr(&self) -> *mut u8 {
        unsafe { self.raw_ptr.add(self.region_base) }
    }

    /// Items waiting in the descriptor ring (`desc_head - desc_tail`).
    pub fn approx_len(&self) -> usize {
        let h = self.header();
        h.desc_head.load(Ordering::Acquire)
            .saturating_sub(h.desc_tail.load(Ordering::Acquire)) as usize
    }

    /// Send a payload, letting the ring pick inline vs region.
    pub fn send(&self, payload: &[u8]) -> Result<FrameClass, RingError> {
        self.send_as(payload, LayoutHint::Auto)
    }

    /// Send a payload with an explicit layout override. **Caller is the
    /// sole producer.**
    pub fn send_as(&self, payload: &[u8], hint: LayoutHint)
        -> Result<FrameClass, RingError>
    {
        let h = self.header();
        let head = h.desc_head.load(Ordering::Relaxed);
        let tail = h.desc_tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= self.capacity as u64 {
            return Err(RingError::Full);
        }

        let inline = match hint {
            LayoutHint::ForceInline => {
                if payload.len() > self.inline_budget {
                    return Err(RingError::PayloadTooLarge);
                }
                true
            }
            LayoutHint::ForceOffset => false,
            LayoutHint::Auto => payload.len() <= self.inline_budget,
        };

        let slot = self.desc_slot_ptr(head);
        let len = payload.len() as u32;

        let class = if inline {
            unsafe {
                slot.write(FrameClass::Inline as u8);
                std::ptr::copy_nonoverlapping(
                    len.to_le_bytes().as_ptr(), slot.add(4), 4,
                );
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(), slot.add(DESC_HEADER_BYTES), payload.len(),
                );
            }
            FrameClass::Inline
        } else {
            if payload.len() > self.max_payload() {
                return Err(RingError::PayloadTooLarge);
            }
            let rh = h.region_head.load(Ordering::Relaxed);
            let rt = h.region_tail.load(Ordering::Acquire);
            let rb = self.region_bytes as u64;
            let phys = rh % rb;
            // Skip-pad to the next wrap boundary if the record would
            // straddle the region end.
            let start = if phys + payload.len() as u64 > rb {
                rh + (rb - phys)
            } else {
                rh
            };
            if start.wrapping_add(payload.len() as u64).wrapping_sub(rt) > rb {
                return Err(RingError::Full);
            }
            let pstart = (start % rb) as usize;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(), self.region_ptr().add(pstart), payload.len(),
                );
            }
            // Publish region bytes before the descriptor that points at
            // them.
            h.region_head.store(start + payload.len() as u64, Ordering::Release);
            unsafe {
                slot.write(FrameClass::Offset as u8);
                std::ptr::copy_nonoverlapping(
                    len.to_le_bytes().as_ptr(), slot.add(4), 4,
                );
                std::ptr::copy_nonoverlapping(
                    start.to_le_bytes().as_ptr(), slot.add(DESC_HEADER_BYTES), 8,
                );
            }
            FrameClass::Offset
        };

        h.desc_head.store(head + 1, Ordering::Release);
        crate::cache_ops::cldemote(slot as *const u8);
        Ok(class)
    }

    /// Receive the next payload into `out` (cleared then filled).
    /// Returns the [`FrameClass`] the producer used. **Caller is the
    /// sole consumer.**
    pub fn recv_into(&self, out: &mut Vec<u8>) -> Result<FrameClass, RingError> {
        let h = self.header();
        let tail = h.desc_tail.load(Ordering::Relaxed);
        let head = h.desc_head.load(Ordering::Acquire);
        if tail == head {
            return Err(RingError::Empty);
        }
        let slot = self.desc_slot_ptr(tail);
        let class_byte = unsafe { slot.read() };
        let len = unsafe {
            let mut b = [0u8; 4];
            std::ptr::copy_nonoverlapping(slot.add(4), b.as_mut_ptr(), 4);
            u32::from_le_bytes(b) as usize
        };

        out.clear();
        out.reserve(len);
        let class = if class_byte == FrameClass::Inline as u8 {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    slot.add(DESC_HEADER_BYTES),
                    out.spare_capacity_mut().as_mut_ptr() as *mut u8,
                    len,
                );
                out.set_len(len);
            }
            FrameClass::Inline
        } else {
            let off = unsafe {
                let mut b = [0u8; 8];
                std::ptr::copy_nonoverlapping(slot.add(DESC_HEADER_BYTES), b.as_mut_ptr(), 8);
                u64::from_le_bytes(b)
            };
            let pstart = (off % self.region_bytes as u64) as usize;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.region_ptr().add(pstart),
                    out.spare_capacity_mut().as_mut_ptr() as *mut u8,
                    len,
                );
                out.set_len(len);
            }
            // Reclaim region space up to the end of this record.
            h.region_tail.store(off + len as u64, Ordering::Release);
            FrameClass::Offset
        };

        h.desc_tail.store(tail + 1, Ordering::Release);
        crate::cache_ops::cldemote(slot as *const u8);
        Ok(class)
    }

    /// Receive the next payload as a fresh `Vec`.
    pub fn recv(&self) -> Result<Vec<u8>, RingError> {
        let mut out = Vec::new();
        self.recv_into(&mut out)?;
        Ok(out)
    }

    /// Force any dirty MMF pages to disk (file backing only).
    pub fn flush(&self) -> Result<(), RingError> {
        if let FrameBacking::File(_, mmap) = &self._backing {
            mmap.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn ring() -> FrameRing {
        // 64-byte slots (56-byte inline budget), 64 KiB region.
        FrameRing::create_anon(16, 64, 1 << 16).unwrap()
    }

    #[test]
    fn inline_round_trip() {
        let r = ring();
        let payload = b"small payload under the inline budget";
        assert_eq!(r.send(payload).unwrap(), FrameClass::Inline);
        let got = r.recv().unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn offset_round_trip_large() {
        let r = ring();
        let payload = vec![0xABu8; 4096]; // far over the 56-byte budget
        assert_eq!(r.send(&payload).unwrap(), FrameClass::Offset);
        let got = r.recv().unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn boundary_inline_vs_offset() {
        let r = ring();
        // Exactly the inline budget stays inline.
        let at = vec![1u8; r.inline_budget()];
        assert_eq!(r.send(&at).unwrap(), FrameClass::Inline);
        assert_eq!(r.recv().unwrap(), at);
        // One byte over spills to the region.
        let over = vec![2u8; r.inline_budget() + 1];
        assert_eq!(r.send(&over).unwrap(), FrameClass::Offset);
        assert_eq!(r.recv().unwrap(), over);
    }

    #[test]
    fn empty_payload_round_trip() {
        let r = ring();
        assert_eq!(r.send(&[]).unwrap(), FrameClass::Inline);
        assert_eq!(r.recv().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn force_offset_overrides_small() {
        let r = ring();
        assert_eq!(r.send_as(b"tiny", LayoutHint::ForceOffset).unwrap(),
                   FrameClass::Offset);
        assert_eq!(r.recv().unwrap(), b"tiny");
    }

    #[test]
    fn force_inline_rejects_oversize() {
        let r = ring();
        let big = vec![0u8; r.inline_budget() + 1];
        assert_eq!(r.send_as(&big, LayoutHint::ForceInline).unwrap_err(),
                   RingError::PayloadTooLarge);
    }

    #[test]
    fn payload_over_region_cap_rejected() {
        let r = ring();
        let too_big = vec![0u8; r.max_payload() + 1];
        assert_eq!(r.send(&too_big).unwrap_err(), RingError::PayloadTooLarge);
    }

    #[test]
    fn descriptor_full_then_drains() {
        let r = FrameRing::create_anon(4, 64, 1 << 16).unwrap();
        for i in 0..4u8 {
            r.send(&[i; 8]).unwrap();
        }
        assert_eq!(r.send(&[9u8; 8]).unwrap_err(), RingError::Full);
        assert_eq!(r.recv().unwrap(), &[0u8; 8]);
        r.send(&[9u8; 8]).unwrap();
    }

    #[test]
    fn region_wraps_with_skip_pad() {
        // Small region forces many wraps; alternate large records so the
        // region head laps the buffer end repeatedly. Each record is
        // pushed then immediately drained so the region tail follows.
        let region = 1usize << 12; // 4 KiB region, max payload 2 KiB
        let r = FrameRing::create_anon(8, 64, region).unwrap();
        for i in 0..200u32 {
            let len = 600 + (i as usize % 700); // 600..1299 bytes, all > budget
            let payload: Vec<u8> = (0..len).map(|k| (k as u32 ^ i) as u8).collect();
            assert_eq!(r.send(&payload).unwrap(), FrameClass::Offset);
            let got = r.recv().unwrap();
            assert_eq!(got, payload, "record {i} survived the region wrap");
        }
    }

    #[test]
    fn mixed_inline_and_offset_fifo_order() {
        let r = FrameRing::create_anon(64, 64, 1 << 16).unwrap();
        let mut expected = Vec::new();
        for i in 0..40u32 {
            // Alternate small (inline) and large (offset) records.
            let len = if i % 2 == 0 { 16 } else { 500 };
            let p: Vec<u8> = (0..len).map(|k| (k as u32 + i) as u8).collect();
            r.send(&p).unwrap();
            expected.push(p);
        }
        for want in expected {
            assert_eq!(r.recv().unwrap(), want);
        }
        assert_eq!(r.recv().unwrap_err(), RingError::Empty);
    }

    #[test]
    fn two_thread_mixed_size_stream() {
        let r = Arc::new(FrameRing::create_anon(256, 64, 1 << 20).unwrap());
        let rp = r.clone();
        let rc = r.clone();
        const N: u32 = 50_000;

        let producer = thread::spawn(move || {
            for i in 0..N {
                // Sizes sweep the inline/offset boundary. Content is a
                // per-byte ramp keyed on the item id so a torn or
                // mis-ordered record is caught at any length (the id
                // alone would not distinguish two items that alias the
                // same slot modulo capacity).
                let len = (i as usize % 300) + 1;
                let p: Vec<u8> =
                    (0..len).map(|k| i.wrapping_add(k as u32) as u8).collect();
                while rp.send(&p).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut buf = Vec::new();
            let mut got = 0u32;
            while got < N {
                if rc.recv_into(&mut buf).is_ok() {
                    let len = (got as usize % 300) + 1;
                    assert_eq!(buf.len(), len, "item {got} length");
                    for (k, &b) in buf.iter().enumerate() {
                        assert_eq!(b, got.wrapping_add(k as u32) as u8,
                                   "item {got} byte {k}");
                    }
                    got += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn shm_cross_handle_visibility() {
        use crate::shm_file::ShmFile;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let name = format!("frame_shm_{}_{}", std::process::id(), nonce);
        let (cap, slot, region) = (16usize, 64usize, 1usize << 16);
        let size = frame_ring_file_size(cap, slot, region);

        let shm_a = ShmFile::create_or_open_named(&name, size).unwrap();
        let producer = FrameRing::create_from_shm(shm_a, cap, slot, region).unwrap();
        let shm_b = ShmFile::create_or_open_named(&name, size).unwrap();
        let consumer = FrameRing::open_from_shm(shm_b, cap, slot, region).unwrap();

        let small = b"inline across handles";
        let large = vec![0x5Au8; 2000];
        producer.send(small).unwrap();
        producer.send(&large).unwrap();
        assert_eq!(consumer.recv().unwrap(), small);
        assert_eq!(consumer.recv().unwrap(), large);
    }

    #[test]
    fn file_round_trips() {
        let p = std::env::temp_dir().join(format!(
            "subetha-frame-{}.bin", std::process::id(),
        ));
        std::fs::remove_file(&p).ok();
        let (cap, slot, region) = (16usize, 128usize, 1usize << 16);
        {
            let r = FrameRing::create(&p, cap, slot, region).unwrap();
            r.send(b"persisted inline").unwrap();
            r.send(&vec![7u8; 3000]).unwrap();
            r.flush().unwrap();
        }
        let r2 = FrameRing::open(&p, cap, slot, region).unwrap();
        assert_eq!(r2.recv().unwrap(), b"persisted inline");
        assert_eq!(r2.recv().unwrap(), vec![7u8; 3000]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn rejects_bad_params() {
        assert!(matches!(FrameRing::create_anon(3, 64, 1 << 16),
                         Err(RingError::LayoutMismatch))); // capacity not pow2
        assert!(matches!(FrameRing::create_anon(16, 8, 1 << 16),
                         Err(RingError::LayoutMismatch))); // slot < MIN_SLOT_SIZE
        assert!(matches!(FrameRing::create_anon(16, 64, 1000),
                         Err(RingError::LayoutMismatch))); // region not pow2
    }
}
