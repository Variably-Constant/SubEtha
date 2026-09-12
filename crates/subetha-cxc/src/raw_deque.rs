//! `RawDeque` - the Chase-Lev work-stealing deque at a slot size chosen
//! at run time, for attachers that know their element as a byte count
//! rather than a [`Marshal`](subetha_core::Marshal) type.
//!
//! It shares the region layout, the header and the protocol of
//! [`SharedDeque`](crate::SharedDeque): the owner pushes and pops the
//! bottom end without a CAS, thieves steal from the top end with one
//! CAS each. A raw handle and a typed handle on the same file
//! interoperate when the slot width matches, which is
//! `T::PAYLOAD_BYTES` rounded up to eight. The header carries the
//! element layout the raw attacher declares (a caller-chosen tag and
//! the element's alignment) so two attachers that disagree about what
//! the bytes mean are refused at attach time.
//!
//! Every push copies the caller's bytes into the slot; a payload
//! shorter than the slot is zero-filled past its end.

use std::fs::File;
use std::path::Path;
use std::sync::atomic::{fence, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::raw_treiber_stack::ElementLayout;
use crate::shared_deque::{prefetchw_line, DequeError, DequeHeader, DEQUE_MAGIC};

/// The slot width for an element of `element_size` bytes: rounded up
/// to eight, at least eight, the width the typed deque gives a
/// `Marshal` payload of the same size.
pub const fn raw_slot_bytes(element_size: usize) -> usize {
    let rounded = (element_size + 7) & !7;
    if rounded < 8 { 8 } else { rounded }
}

/// The MMF byte size for a raw deque of `capacity` slots holding
/// elements of `element_size` bytes.
pub const fn raw_deque_file_size(capacity: usize, element_size: usize) -> usize {
    std::mem::size_of::<DequeHeader>() + capacity * raw_slot_bytes(element_size)
}

pub struct RawDeque {
    mmap: MmapMut,
    capacity: usize,
    slot_bytes: usize,
    layout: ElementLayout,
    _file: File,
}

// SAFETY: the mmap is Send and Sync, and the Chase-Lev protocol is the
// synchronization, exactly as for the typed deque.
unsafe impl Send for RawDeque {}
unsafe impl Sync for RawDeque {}

impl RawDeque {
    fn check_layout(layout: &ElementLayout) -> Result<(), DequeError> {
        if layout.slot_size == 0
            || layout.slot_size > u32::MAX as usize
            || layout.alignment == 0
            || !layout.alignment.is_power_of_two()
        {
            return Err(DequeError::InvalidCapacity);
        }
        Ok(())
    }

    /// Create the deque at `path` as its owner with `capacity` slots
    /// (a non-zero power of two), truncating whatever the path held.
    /// The calling process is recorded as the owner for information;
    /// single-owner discipline is the caller's contract.
    pub fn create(
        path: impl AsRef<Path>, capacity: usize, layout: ElementLayout,
    ) -> Result<Self, DequeError> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(DequeError::InvalidCapacity);
        }
        Self::check_layout(&layout)?;
        let slot_bytes = raw_slot_bytes(layout.slot_size);
        let total = std::mem::size_of::<DequeHeader>() + capacity * slot_bytes;
        let file = crate::region_file::create_truncated(path.as_ref())?;
        file.set_len(total as u64)?;
        // SAFETY: a fresh file of the right size is exclusive to this
        // process while the header is initialized.
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let header_ptr = mmap.as_mut_ptr() as *mut DequeHeader;
        // SAFETY: the mapping is `total` bytes and page aligned, so the
        // header fits at its start.
        unsafe {
            (*header_ptr).magic = DEQUE_MAGIC;
            (*header_ptr).capacity = capacity as u64;
            (*header_ptr).slot_bytes = slot_bytes as u32;
            (*header_ptr).alignment = layout.alignment as u32;
            (*header_ptr).owner_pid = std::process::id() as u64;
            (*header_ptr).top.store(0, Ordering::Relaxed);
            (*header_ptr).bottom.store(0, Ordering::Relaxed);
            (*header_ptr).epoch.store(0, Ordering::Relaxed);
            (*header_ptr).layout_tag = layout.tag;
        }
        mmap.flush()?;
        Ok(Self { mmap, capacity, slot_bytes, layout, _file: file })
    }

    /// Open an existing deque as a thief. The header's slot width, tag
    /// and alignment are checked against `layout`; a different slot
    /// width is a `SlotBytesMismatch` and a different tag or alignment
    /// an `InvalidMagic`, because the region is not the one the caller
    /// described.
    pub fn open_as_thief(
        path: impl AsRef<Path>, layout: ElementLayout,
    ) -> Result<Self, DequeError> {
        Self::check_layout(&layout)?;
        let file = crate::region_file::open_existing(path.as_ref())?;
        let len = file.metadata()?.len() as usize;
        if len < std::mem::size_of::<DequeHeader>() {
            return Err(DequeError::InvalidMagic);
        }
        // SAFETY: the file's bytes back this process's view; the header
        // is validated before any slot is read.
        let mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        let header_ptr = mmap.as_ptr() as *const DequeHeader;
        let (magic, capacity, slot_bytes, alignment, tag) = unsafe {
            (
                (*header_ptr).magic,
                (*header_ptr).capacity,
                (*header_ptr).slot_bytes,
                (*header_ptr).alignment,
                (*header_ptr).layout_tag,
            )
        };
        if magic != DEQUE_MAGIC { return Err(DequeError::InvalidMagic); }
        let expected = raw_slot_bytes(layout.slot_size) as u32;
        if slot_bytes != expected {
            return Err(DequeError::SlotBytesMismatch {
                file_slot_bytes: slot_bytes,
                type_slot_bytes: expected,
            });
        }
        if alignment != layout.alignment as u32 || tag != layout.tag {
            return Err(DequeError::InvalidMagic);
        }
        let capacity = capacity as usize;
        if len < std::mem::size_of::<DequeHeader>() + capacity * slot_bytes as usize {
            return Err(DequeError::InvalidMagic);
        }
        Ok(Self { mmap, capacity, slot_bytes: slot_bytes as usize, layout, _file: file })
    }

    fn header(&self) -> &DequeHeader {
        // SAFETY: the header was initialized at create time and the
        // layout is stable for the lifetime of the mapping.
        unsafe { &*(self.mmap.as_ptr() as *const DequeHeader) }
    }

    fn slot_ptr(&self, idx: usize) -> *mut u8 {
        let base = self.mmap.as_ptr() as usize + std::mem::size_of::<DequeHeader>();
        (base + idx * self.slot_bytes) as *mut u8
    }

    pub fn capacity(&self) -> usize { self.capacity }

    /// The slot width in bytes: the element size rounded up to eight.
    pub fn slot_bytes(&self) -> usize { self.slot_bytes }

    pub fn layout(&self) -> ElementLayout { self.layout }

    /// The `top` index: elements stolen or popped from the top so far.
    pub fn top(&self) -> i64 { self.header().top.load(Ordering::Relaxed) }

    /// The `bottom` index: elements the owner has pushed, less the ones
    /// it popped.
    pub fn bottom(&self) -> i64 { self.header().bottom.load(Ordering::Relaxed) }

    /// Approximate current length, racing concurrent pushes and steals.
    pub fn approx_len(&self) -> usize {
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        let t = h.top.load(Ordering::Relaxed);
        (b - t).max(0) as usize
    }

    fn write_slot(&self, idx: usize, bytes: &[u8]) {
        let dst = self.slot_ptr(idx);
        // SAFETY: idx is in [0, capacity), the slot is slot_bytes long
        // and bytes.len() <= slot_size <= slot_bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            std::ptr::write_bytes(dst.add(bytes.len()), 0, self.slot_bytes - bytes.len());
        }
    }

    fn read_slot(&self, idx: usize, out: &mut [u8]) -> usize {
        let n = self.slot_bytes.min(out.len());
        // SAFETY: idx is in [0, capacity) and n <= slot_bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(self.slot_ptr(idx) as *const u8, out.as_mut_ptr(), n);
        }
        n
    }

    /// Owner side: push `bytes` onto the bottom. `bytes` is at most
    /// the element size; the rest of the slot is zeroed. Returns
    /// `Err(Full)` at capacity and `Err(Marshal(ShortBuffer))` for a
    /// payload longer than an element.
    pub fn push(&self, bytes: &[u8]) -> Result<(), DequeError> {
        if bytes.len() > self.layout.slot_size {
            return Err(DequeError::Marshal(subetha_core::MarshalError::ShortBuffer {
                expected: self.layout.slot_size,
                got: bytes.len(),
            }));
        }
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        let idx = (b as usize) & (self.capacity - 1);
        prefetchw_line(self.slot_ptr(idx));
        let t = h.top.load(Ordering::Acquire);
        if b - t >= self.capacity as i64 {
            return Err(DequeError::Full);
        }
        self.write_slot(idx, bytes);
        fence(Ordering::Release);
        h.bottom.store(b + 1, Ordering::Relaxed);
        Ok(())
    }

    /// Owner side: reserve `n` slots under one `top` load, let `fill`
    /// write each slot's bytes, then publish them all with one Release
    /// fence and one `bottom` store. Returns `Err(Full)` and writes
    /// nothing when the batch does not fit.
    pub fn push_batch_with<F>(&self, n: usize, mut fill: F) -> Result<(), DequeError>
    where
        F: FnMut(usize, &mut [u8]),
    {
        if n == 0 {
            return Ok(());
        }
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed);
        let t = h.top.load(Ordering::Acquire);
        if (b - t) + n as i64 > self.capacity as i64 {
            return Err(DequeError::Full);
        }
        let mask = self.capacity - 1;
        prefetchw_line(self.slot_ptr((b as usize) & mask));
        for i in 0..n {
            let idx = ((b + i as i64) as usize) & mask;
            if i + 1 < n {
                prefetchw_line(self.slot_ptr(((b + (i + 1) as i64) as usize) & mask));
            }
            // SAFETY: idx is in [0, capacity); the slot is slot_bytes
            // long; the capacity check above keeps the reservation
            // clear of thieves.
            let slot = unsafe {
                std::slice::from_raw_parts_mut(self.slot_ptr(idx), self.slot_bytes)
            };
            fill(i, slot);
        }
        fence(Ordering::Release);
        h.bottom.store(b + n as i64, Ordering::Relaxed);
        Ok(())
    }

    /// Owner side: pop the bottom element into `out`. Returns the
    /// number of bytes written (the slot width, or `out.len()` when
    /// that is shorter), or `None` when the deque is empty or a thief
    /// took the last element first.
    pub fn pop(&self, out: &mut [u8]) -> Option<usize> {
        let h = self.header();
        let b = h.bottom.load(Ordering::Relaxed) - 1;
        h.bottom.store(b, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let t = h.top.load(Ordering::Relaxed);
        if b < t {
            h.bottom.store(b + 1, Ordering::Relaxed);
            return None;
        }
        let idx = (b as usize) & (self.capacity - 1);
        let n = self.read_slot(idx, out);
        if b > t {
            return Some(n);
        }
        // One element left: race the thieves for it. A lost CAS means a
        // thief took it, which is a `None`, not a failure.
        let won = h.top.compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok();
        h.bottom.store(b + 1, Ordering::Relaxed);
        if won { Some(n) } else { None }
    }

    /// Thief side: steal the top element into `out`. Any number of
    /// thieves run concurrently with each other and with the owner's
    /// pop. The slot is read before the CAS, so a lost CAS discards a
    /// possibly stale copy and reports `None`.
    pub fn steal(&self, out: &mut [u8]) -> Option<usize> {
        let h = self.header();
        let t = h.top.load(Ordering::Acquire);
        fence(Ordering::SeqCst);
        let b = h.bottom.load(Ordering::Acquire);
        if t >= b { return None; }
        let idx = (t as usize) & (self.capacity - 1);
        let n = self.read_slot(idx, out);
        // A lost CAS means another thief or the owner took the element.
        let won = h.top.compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok();
        if won { Some(n) } else { None }
    }

    pub fn flush(&self) -> std::io::Result<()> { self.mmap.flush() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SharedDeque;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-rawdeque-{name}-{}.bin", std::process::id()))
    }

    fn layout(slot_size: usize) -> ElementLayout {
        ElementLayout { slot_size, alignment: 8, tag: 0x5241_5744_4551_5545 }
    }

    #[test]
    fn owner_pop_is_lifo_and_a_short_payload_is_zero_filled() {
        let p = tmp("lifo");
        let dq = RawDeque::create(&p, 64, layout(12)).unwrap();
        assert_eq!(dq.slot_bytes(), 16);
        for i in 0..10u8 {
            dq.push(&[i; 12]).unwrap();
        }
        assert_eq!(dq.approx_len(), 10);
        let mut out = [0xFFu8; 16];
        for expect in (0..10u8).rev() {
            assert_eq!(dq.pop(&mut out), Some(16));
            assert_eq!(&out[..12], &[expect; 12]);
            assert_eq!(&out[12..], &[0u8; 4]);
        }
        assert_eq!(dq.pop(&mut out), None);
        assert!(matches!(dq.push(&[0; 13]), Err(DequeError::Marshal(_))));
    }

    #[test]
    fn full_and_bad_shapes_are_reported() {
        let p = tmp("full");
        let dq = RawDeque::create(&p, 4, layout(8)).unwrap();
        for i in 0..4u8 { dq.push(&[i; 8]).unwrap(); }
        assert!(matches!(dq.push(&[9; 8]), Err(DequeError::Full)));
        assert!(matches!(RawDeque::create(tmp("cap"), 6, layout(8)), Err(DequeError::InvalidCapacity)));
        assert!(matches!(
            RawDeque::create(tmp("align"), 8, ElementLayout { alignment: 3, ..layout(8) }),
            Err(DequeError::InvalidCapacity)
        ));
    }

    #[test]
    fn a_thief_steals_fifo_and_checks_the_layout() {
        let p = tmp("steal");
        let owner = RawDeque::create(&p, 16, layout(8)).unwrap();
        for i in 0..5u64 { owner.push(&i.to_le_bytes()).unwrap(); }
        let thief = RawDeque::open_as_thief(&p, layout(8)).unwrap();
        let mut out = [0u8; 8];
        let mut stolen = Vec::new();
        while thief.steal(&mut out).is_some() {
            stolen.push(u64::from_le_bytes(out));
        }
        assert_eq!(stolen, (0..5).collect::<Vec<_>>());
        assert!(matches!(
            RawDeque::open_as_thief(&p, layout(24)),
            Err(DequeError::SlotBytesMismatch { file_slot_bytes: 8, type_slot_bytes: 24 })
        ));
        assert!(matches!(
            RawDeque::open_as_thief(&p, ElementLayout { tag: 5, ..layout(8) }),
            Err(DequeError::InvalidMagic)
        ));
        drop(thief);
        drop(owner);
    }

    /// A typed deque declares `T`'s alignment and a zero tag, so a raw
    /// thief stating the same steals from it and one stating another
    /// alignment is refused; a raw owner with a zero tag serves a typed
    /// thief whose payload width matches.
    #[test]
    fn a_typed_region_and_a_raw_region_agree_on_the_slot_width() {
        let p = tmp("typed");
        let owner = SharedDeque::<u64>::create(&p, 16).unwrap();
        for i in 0..3u64 { owner.push(&(i + 100)).unwrap(); }
        assert!(matches!(
            RawDeque::open_as_thief(&p, ElementLayout { slot_size: 8, alignment: 0, tag: 0 }),
            Err(DequeError::InvalidCapacity)
        ), "alignment zero is not a layout");
        assert!(matches!(
            RawDeque::open_as_thief(&p, ElementLayout { slot_size: 8, alignment: 1, tag: 0 }),
            Err(DequeError::InvalidMagic)
        ), "a typed u64 region declares alignment 8");
        let raw_thief = RawDeque::open_as_thief(&p, ElementLayout { slot_size: 8, alignment: 8, tag: 0 }).unwrap();
        let mut out = [0u8; 8];
        assert_eq!(raw_thief.steal(&mut out), Some(8));
        assert_eq!(u64::from_le_bytes(out), 100);
        drop(raw_thief);
        drop(owner);
        let raw_owner = RawDeque::create(&p, 16, ElementLayout { slot_size: 8, alignment: 8, tag: 0 }).unwrap();
        raw_owner.push(&7u64.to_le_bytes()).unwrap();
        let typed_thief = SharedDeque::<u64>::open_as_thief(&p).unwrap();
        assert_eq!(typed_thief.steal(), Some(7));
        drop(typed_thief);
        drop(raw_owner);
    }

    #[test]
    fn one_owner_two_thieves_lose_nothing() {
        let p = tmp("1o2t");
        let owner = Arc::new(RawDeque::create(&p, 1024, layout(8)).unwrap());
        let n = 20_000u64;
        let thieves: Vec<_> = (0..2)
            .map(|_| {
                let t = RawDeque::open_as_thief(&p, layout(8)).unwrap();
                let owner = Arc::clone(&owner);
                thread::spawn(move || {
                    let mut got = Vec::new();
                    let mut out = [0u8; 8];
                    let mut idle = 0u32;
                    loop {
                        match t.steal(&mut out) {
                            Some(_) => { got.push(u64::from_le_bytes(out)); idle = 0; }
                            None => {
                                idle += 1;
                                if idle > 200_000 && owner.approx_len() == 0 { break; }
                                thread::yield_now();
                            }
                        }
                    }
                    got
                })
            })
            .collect();
        let mut kept = Vec::new();
        let mut out = [0u8; 8];
        for i in 0..n {
            loop {
                match owner.push(&i.to_le_bytes()) {
                    Ok(()) => break,
                    Err(DequeError::Full) => match owner.pop(&mut out) {
                        Some(_) => kept.push(u64::from_le_bytes(out)),
                        None => thread::yield_now(),
                    },
                    Err(e) => panic!("push failed: {e:?}"),
                }
            }
        }
        while owner.pop(&mut out).is_some() {
            kept.push(u64::from_le_bytes(out));
        }
        let mut all: Vec<u64> = thieves.into_iter().flat_map(|h| h.join().unwrap()).collect();
        all.extend(kept);
        all.sort_unstable();
        assert_eq!(all, (0..n).collect::<Vec<_>>());
    }
}
