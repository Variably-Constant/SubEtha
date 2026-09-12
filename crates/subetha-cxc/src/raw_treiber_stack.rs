//! `RawTreiberStack` - the Treiber stack at a slot size chosen at run
//! time, for attachers that know their element as a byte count rather
//! than a Rust type.
//!
//! It shares the region layout, the header and the CAS protocol of
//! [`SharedTreiberStack`](crate::SharedTreiberStack): a raw handle and a
//! typed handle on the same file interoperate as long as the slot size
//! matches `size_of::<T>()` and the typed side's `T` is plain bytes. The
//! header carries the element layout the raw attacher declares (a
//! caller-chosen tag and the element's alignment) so two attachers that
//! disagree about what the bytes mean are refused at attach time.
//!
//! Every push copies the caller's bytes into the slot; a slot shorter
//! than `slot_size` is zero-filled past the payload so the bytes a
//! popper reads are what the pusher wrote and nothing older.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::shared_treiber_stack::{
    pack, stack_file_size, unpack, StackError, StackHeader, STACK_MAGIC, STACK_NIL,
};

/// The element layout a raw attacher declares. `tag` is caller-chosen;
/// `alignment` is the element's alignment in bytes. Both are stored in
/// the region header and compared on every attach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElementLayout {
    pub slot_size: usize,
    pub alignment: usize,
    pub tag: u64,
}

pub struct RawTreiberStack {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    layout: ElementLayout,
    next_offset: usize,
    slots_offset: usize,
}

unsafe impl Send for RawTreiberStack {}
unsafe impl Sync for RawTreiberStack {}

impl RawTreiberStack {
    /// Obtain the stack at `path`, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. Attaching keeps
    /// pushed entries and the free list; a region built with a different
    /// capacity, slot size, alignment or tag is a `LayoutMismatch`.
    pub fn create(
        path: impl AsRef<Path>, capacity: usize, layout: ElementLayout,
    ) -> Result<Self, StackError> {
        Self::check_shape(capacity, &layout);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            stack_file_size(capacity, layout.slot_size),
            |ptr| unsafe { Self::init_region(ptr, capacity, &layout) },
            |ptr| unsafe { (*(ptr as *const StackHeader)).magic == STACK_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, StackError::LayoutMismatch))?;
        Self::from_region(file, mmap, capacity, layout)
    }

    /// Truncate the stack at `path` and initialize an empty one,
    /// discarding every entry live peers share.
    pub fn reset(
        path: impl AsRef<Path>, capacity: usize, layout: ElementLayout,
    ) -> Result<Self, StackError> {
        Self::check_shape(capacity, &layout);
        let (file, mmap) = crate::mmf_attach::reset(
            path.as_ref(),
            stack_file_size(capacity, layout.slot_size),
            |ptr| unsafe { Self::init_region(ptr, capacity, &layout) },
        )?;
        Self::from_region(file, mmap, capacity, layout)
    }

    /// Attach to an existing stack whose capacity and layout the caller
    /// states; an absent file is an I/O error and a region of another
    /// shape is a `LayoutMismatch`.
    pub fn open(
        path: impl AsRef<Path>, capacity: usize, layout: ElementLayout,
    ) -> Result<Self, StackError> {
        Self::check_shape(capacity, &layout);
        let total = stack_file_size(capacity, layout.slot_size);
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(StackError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::from_region(file, mmap, capacity, layout)
    }

    fn check_shape(capacity: usize, layout: &ElementLayout) {
        assert!(capacity >= 1);
        assert!(capacity < STACK_NIL as usize, "capacity must be < u32::MAX");
        assert!(layout.slot_size >= 1, "slot_size must be at least one byte");
        assert!(layout.slot_size <= u32::MAX as usize, "slot_size must fit u32");
        assert!(
            layout.alignment >= 1 && layout.alignment.is_power_of_two(),
            "alignment must be a power of two"
        );
    }

    /// Lay out an empty stack: config, layout, the NIL head, free head
    /// and bump cursor first, magic last, because attachers spin on it.
    ///
    /// # Safety
    /// `ptr` addresses at least `stack_file_size(capacity,
    /// layout.slot_size)` writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, layout: &ElementLayout) {
        let hdr = ptr as *mut StackHeader;
        unsafe {
            (*hdr).capacity = capacity as u32;
            (*hdr).slot_size = layout.slot_size as u32;
            (*hdr).layout_tag = layout.tag;
            (*hdr).alignment = layout.alignment as u32;
            std::ptr::write(&raw mut (*hdr).head, AtomicU64::new(pack(0, STACK_NIL)));
            std::ptr::write(&raw mut (*hdr).free_head, AtomicU64::new(pack(0, STACK_NIL)));
            std::ptr::write_volatile(&raw mut (*hdr).magic, STACK_MAGIC);
        }
    }

    fn from_region(
        file: File, mmap: MmapMut, capacity: usize, layout: ElementLayout,
    ) -> Result<Self, StackError> {
        let hdr = unsafe { &*(mmap.as_ptr() as *const StackHeader) };
        if hdr.magic != STACK_MAGIC
            || hdr.capacity != capacity as u32
            || hdr.slot_size != layout.slot_size as u32
            || hdr.layout_tag != layout.tag
            || hdr.alignment != layout.alignment as u32
        {
            return Err(StackError::LayoutMismatch);
        }
        let next_offset = size_of::<StackHeader>();
        let slots_offset = next_offset + capacity * size_of::<AtomicU32>();
        Ok(Self { _file: file, mmap, capacity, layout, next_offset, slots_offset })
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    pub fn layout(&self) -> ElementLayout { self.layout }

    #[inline]
    pub fn slot_size(&self) -> usize { self.layout.slot_size }

    fn header(&self) -> &StackHeader {
        unsafe { &*(self.mmap.as_ptr() as *const StackHeader) }
    }

    fn next_link(&self, idx: usize) -> &AtomicU32 {
        let base = unsafe { self.mmap.as_ptr().add(self.next_offset) };
        unsafe { &*(base.add(idx * size_of::<AtomicU32>()) as *const AtomicU32) }
    }

    fn slot(&self, idx: usize) -> *mut u8 {
        let base = unsafe { self.mmap.as_ptr().add(self.slots_offset) };
        unsafe { base.add(idx * self.layout.slot_size) as *mut u8 }
    }

    fn acquire_slot(&self) -> Result<u32, StackError> {
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, idx) = unpack(head);
            if idx == STACK_NIL { break; }
            let next_idx = self.next_link(idx as usize).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next_idx);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Ok(idx);
            }
        }
        let idx = self.header().bump_next.fetch_add(1, Ordering::AcqRel);
        if (idx as usize) >= self.capacity {
            self.header().bump_next.fetch_sub(1, Ordering::AcqRel);
            return Err(StackError::Full);
        }
        Ok(idx)
    }

    fn release_slot(&self, idx: u32) {
        loop {
            let head = self.header().free_head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(idx as usize).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), idx);
            if self.header().free_head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return;
            }
        }
    }

    /// Push `bytes` as one element. `bytes` is at most `slot_size`
    /// long; the rest of the slot is zeroed. Returns `Err(Full)` at
    /// capacity and `Err(LayoutMismatch)` for a payload longer than a
    /// slot.
    pub fn push(&self, bytes: &[u8]) -> Result<(), StackError> {
        if bytes.len() > self.layout.slot_size {
            return Err(StackError::LayoutMismatch);
        }
        let idx = self.acquire_slot()?;
        let dst = self.slot(idx as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            std::ptr::write_bytes(dst.add(bytes.len()), 0, self.layout.slot_size - bytes.len());
        }
        loop {
            let head = self.header().head.load(Ordering::Acquire);
            let (counter, old_top) = unpack(head);
            self.next_link(idx as usize).store(old_top, Ordering::Release);
            let new_head = pack(counter.wrapping_add(1), idx);
            if self.header().head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                return Ok(());
            }
        }
    }

    /// Pop the top element into `out`, which holds at least `slot_size`
    /// bytes; the element is copied out before its slot returns to the
    /// free list. Returns the number of bytes written, or `None` when
    /// the stack is empty.
    pub fn pop(&self, out: &mut [u8]) -> Option<usize> {
        assert!(out.len() >= self.layout.slot_size, "pop buffer shorter than a slot");
        loop {
            let head = self.header().head.load(Ordering::Acquire);
            let (counter, top) = unpack(head);
            if top == STACK_NIL {
                return None;
            }
            let next_top = self.next_link(top as usize).load(Ordering::Acquire);
            let new_head = pack(counter.wrapping_add(1), next_top);
            if self.header().head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.slot(top as usize), out.as_mut_ptr(), self.layout.slot_size,
                    );
                }
                self.release_slot(top);
                return Some(self.layout.slot_size);
            }
        }
    }

    /// Copy the top element into `out` without popping it. The bytes
    /// are a snapshot: a concurrent pop can retire the slot while it
    /// is read, exactly as the typed peek.
    pub fn peek(&self, out: &mut [u8]) -> Option<usize> {
        assert!(out.len() >= self.layout.slot_size, "peek buffer shorter than a slot");
        let head = self.header().head.load(Ordering::Acquire);
        let (_, top) = unpack(head);
        if top == STACK_NIL {
            return None;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.slot(top as usize), out.as_mut_ptr(), self.layout.slot_size,
            );
        }
        Some(self.layout.slot_size)
    }

    pub fn is_empty(&self) -> bool {
        let head = self.header().head.load(Ordering::Acquire);
        unpack(head).1 == STACK_NIL
    }

    /// Approximate len: walks the chain from the head, O(N), racing
    /// concurrent pushes and pops.
    pub fn approx_len(&self) -> usize {
        let head = self.header().head.load(Ordering::Acquire);
        let (_, mut idx) = unpack(head);
        let mut count = 0usize;
        let mut visited = 0;
        while idx != STACK_NIL && visited < self.capacity {
            count += 1;
            visited += 1;
            idx = self.next_link(idx as usize).load(Ordering::Acquire);
        }
        count
    }

    pub fn flush(&self) -> Result<(), StackError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SharedTreiberStack;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-rawstack-{name}-{}.bin", std::process::id()))
    }

    fn layout(slot_size: usize) -> ElementLayout {
        ElementLayout { slot_size, alignment: 8, tag: 0x5241_5753_5441_434b }
    }

    #[test]
    fn push_pop_is_lifo_at_a_runtime_slot_size() {
        let p = tmp("lifo");
        let s = RawTreiberStack::create(&p, 8, layout(24)).unwrap();
        assert!(s.is_empty());
        for i in 0..5u8 {
            let mut bytes = vec![i; 24];
            bytes[23] = 0xEE;
            s.push(&bytes).unwrap();
        }
        assert_eq!(s.approx_len(), 5);
        let mut out = [0u8; 24];
        assert_eq!(s.peek(&mut out), Some(24));
        assert_eq!(out[0], 4);
        for expect in (0..5u8).rev() {
            assert_eq!(s.pop(&mut out), Some(24));
            assert_eq!(out[0], expect);
            assert_eq!(out[23], 0xEE);
        }
        assert_eq!(s.pop(&mut out), None);
    }

    #[test]
    fn a_short_payload_is_zero_filled_and_a_long_one_refused() {
        let p = tmp("fill");
        let s = RawTreiberStack::create(&p, 4, layout(16)).unwrap();
        s.push(&[0xAB; 3]).unwrap();
        let mut out = [0xFFu8; 16];
        assert_eq!(s.pop(&mut out), Some(16));
        assert_eq!(&out[..3], &[0xAB; 3]);
        assert_eq!(&out[3..], &[0u8; 13]);
        assert!(matches!(s.push(&[0; 17]), Err(StackError::LayoutMismatch)));
    }

    #[test]
    fn full_is_reported_and_a_pop_frees_a_slot() {
        let p = tmp("full");
        let s = RawTreiberStack::create(&p, 2, layout(8)).unwrap();
        s.push(&[1; 8]).unwrap();
        s.push(&[2; 8]).unwrap();
        assert!(matches!(s.push(&[3; 8]), Err(StackError::Full)));
        let mut out = [0u8; 8];
        assert_eq!(s.pop(&mut out), Some(8));
        s.push(&[3; 8]).unwrap();
    }

    #[test]
    fn attach_checks_capacity_slot_size_alignment_and_tag() {
        let p = tmp("attach");
        let s = RawTreiberStack::create(&p, 8, layout(16)).unwrap();
        s.push(&[7; 16]).unwrap();
        let again = RawTreiberStack::create(&p, 8, layout(16)).unwrap();
        assert_eq!(again.approx_len(), 1);
        let opened = RawTreiberStack::open(&p, 8, layout(16)).unwrap();
        assert_eq!(opened.approx_len(), 1);
        assert!(matches!(
            RawTreiberStack::create(&p, 4, layout(16)),
            Err(StackError::LayoutMismatch)
        ));
        assert!(matches!(
            RawTreiberStack::open(&p, 8, ElementLayout { slot_size: 16, alignment: 4, tag: layout(16).tag }),
            Err(StackError::LayoutMismatch)
        ));
        assert!(matches!(
            RawTreiberStack::open(&p, 8, ElementLayout { tag: 1, ..layout(16) }),
            Err(StackError::LayoutMismatch)
        ));
        let missing = tmp("absent");
        assert!(matches!(
            RawTreiberStack::open(&missing, 8, layout(16)),
            Err(StackError::IoError(std::io::ErrorKind::NotFound))
        ));
    }

    /// A typed stack over `[u8; 16]` declares the array's alignment, 1,
    /// and a zero tag; a raw stack at slot size 16 stating the same
    /// addresses the same region and the two see each other's entries.
    #[test]
    fn a_typed_stack_and_a_raw_stack_share_a_region() {
        let p = tmp("typed");
        let typed: SharedTreiberStack<[u8; 16]> = SharedTreiberStack::create(&p, 8).unwrap();
        typed.push([9; 16]).unwrap();
        let raw = RawTreiberStack::open(
            &p, 8, ElementLayout { slot_size: 16, alignment: 1, tag: 0 },
        ).unwrap();
        let mut out = [0u8; 16];
        assert_eq!(raw.pop(&mut out), Some(16));
        assert_eq!(out, [9; 16]);
        raw.push(&[3; 16]).unwrap();
        assert_eq!(typed.pop(), Some([3; 16]));
    }

    #[test]
    fn concurrent_pushers_and_poppers_lose_nothing() {
        let p = tmp("threads");
        let s = Arc::new(RawTreiberStack::create(&p, 1024, layout(8)).unwrap());
        let pushers: Vec<_> = (0..4u64)
            .map(|t| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for i in 0..500u64 {
                        let v = (t << 32) | i;
                        loop {
                            match s.push(&v.to_le_bytes()) {
                                Ok(()) => break,
                                Err(StackError::Full) => thread::yield_now(),
                                Err(e) => panic!("push failed: {e:?}"),
                            }
                        }
                    }
                })
            })
            .collect();
        let poppers: Vec<_> = (0..4)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    let mut seen = Vec::new();
                    let mut out = [0u8; 8];
                    while seen.len() < 500 {
                        match s.pop(&mut out) {
                            Some(_) => seen.push(u64::from_le_bytes(out)),
                            None => thread::yield_now(),
                        }
                    }
                    seen
                })
            })
            .collect();
        for h in pushers { h.join().unwrap(); }
        let mut all: Vec<u64> = poppers.into_iter().flat_map(|h| h.join().unwrap()).collect();
        all.sort_unstable();
        let mut expect: Vec<u64> =
            (0..4u64).flat_map(|t| (0..500u64).map(move |i| (t << 32) | i)).collect();
        expect.sort_unstable();
        assert_eq!(all, expect);
        assert!(s.is_empty());
    }
}
