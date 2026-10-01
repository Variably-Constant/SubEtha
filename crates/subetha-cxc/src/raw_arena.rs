//! `RawArena` - power-of-two blocks carved from one shared file, handed to
//! a writer, published, retired under an epoch and reused, with every step
//! recoverable from the block headers after a crash.
//!
//! # Layout
//!
//! ```text
//! +------------------------------------------+
//! | ArenaHeader: magic, capacity, classes,   |
//! |   bump, the collector's slot, scan hints |
//! +------------------------------------------+
//! | free bitmap, one per class               |
//! | retired bitmap, one per class            |
//! +------------------------------------------+
//! | block space: capacity bytes              |
//! +------------------------------------------+
//! ```
//!
//! A block of class `c` is `2^c` bytes: a [`BLOCK_HEADER_BYTES`] header and
//! the payload. The block space is carved from the front in chunks of the
//! largest class, and a chunk is halved down to the class a writer needs,
//! so every block of class `c` starts at a multiple of `2^c` and the pair
//! `(class, offset >> class)` names it. Each class has one bit per possible
//! block in its free bitmap and one in its retired bitmap.
//!
//! # A block's life
//!
//! `Unbuilt -> Free -> Writing(pid) -> Published -> Retired(epoch) -> Free`,
//! each step one compare-and-swap on the block's state word, which carries
//! the writer's pid or the retire epoch beside the state. A writer takes a
//! free block, fills its payload, makes the handle reachable from whatever
//! structure names its values, and only then publishes it; the block it
//! replaced is retired at the epoch [`SharedEpochs::advance`] hands back,
//! and comes free again once [`SharedEpochs::reclaim_horizon`] has passed
//! that epoch, so a reader pinned before the replacement still reads it.
//!
//! # Bitmaps, not lists
//!
//! A free or retired bit is set with a `fetch_or`, so setting it twice is
//! setting it once, and a crash between a state change and its bit leaves
//! a block a walk can place from its header alone. A bit says the block
//! may be taken; taking it is the state compare-and-swap, so a bit left
//! over from an earlier life costs one failed swap and is cleared.
//!
//! # Recovery
//!
//! Nothing repairs the file. When a writer finds its class empty after
//! reclaiming, no chunk left to carve and no larger block to halve, the
//! caller [`collect`](RawArena::collect)s: one process at a time, holding
//! a pid-stamped slot that is reaped when its holder has died, walks the
//! block headers from the front to the bump. An unbuilt chunk becomes
//! free; a free or retired block gets its bit back; a published block the
//! caller's roots do not reach is retired at the current epoch; a block a
//! dead process was writing is published when the roots reach it, since
//! its payload was complete before its handle became reachable, and
//! retired when they do not. The ordinary reclaim then returns whatever
//! no pin can see.

use std::fs::File;
use std::io;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::HolderTable;
use crate::shared_epochs::SharedEpochs;

/// Format tag written last when a region is initialized, and required to
/// match on open.
pub const RAW_ARENA_MAGIC: u64 = 0x5241_5741_5245_4E41;

/// Bytes a block gives to its header; the payload follows.
pub const BLOCK_HEADER_BYTES: usize = 32;

/// The smallest class an arena may be built with: a 64-byte block, half
/// of it payload.
pub const SMALLEST_CLASS: u32 = 6;

/// The largest class an arena may be built with: a 1 TiB block.
pub const LARGEST_CLASS: u32 = 40;

const MAX_CLASSES: usize = (LARGEST_CLASS - SMALLEST_CLASS + 1) as usize;

/// Bytes the arena header takes, the collector's slot and the scan hints
/// included.
const HEADER_BYTES: usize = 512;
/// Where the collector's [`HolderSlot`](crate::holder_table::HolderSlot)
/// sits in the header: on its own cache line.
const COLLECTOR_OFFSET: usize = 64;
/// Where the per-class scan hints sit in the header.
const HINTS_OFFSET: usize = 128;
/// The payload the collector's slot publishes while it walks.
const COLLECTING: u64 = 1;

const _: () = assert!(HINTS_OFFSET + MAX_CLASSES * size_of::<AtomicU32>() <= HEADER_BYTES);

/// The state a block's word carries in its top byte.
const TAG_SHIFT: u32 = 56;
const PAYLOAD_MASK: u64 = (1 << TAG_SHIFT) - 1;
const UNBUILT: u64 = 0;
const FREE: u64 = 1;
const WRITING: u64 = 2;
const PUBLISHED: u64 = 3;
const RETIRED: u64 = 4;

#[inline]
fn state_word(tag: u64, payload: u64) -> u64 {
    (tag << TAG_SHIFT) | (payload & PAYLOAD_MASK)
}

#[inline]
fn tag_of(word: u64) -> u64 {
    word >> TAG_SHIFT
}

#[inline]
fn payload_of(word: u64) -> u64 {
    word & PAYLOAD_MASK
}

#[repr(C)]
struct ArenaHeader {
    magic: AtomicU64,
    capacity: AtomicU64,
    min_class: AtomicU32,
    max_class: AtomicU32,
    /// The first byte of the block space not yet carved.
    bump: AtomicU64,
    _pad: [u8; 32],
}

const _: () = assert!(size_of::<ArenaHeader>() == COLLECTOR_OFFSET);

/// The first bytes of every block.
#[repr(C)]
struct BlockHeader {
    /// The tag in the top byte, the writer's pid or the retire epoch in
    /// the rest.
    state: AtomicU64,
    class: AtomicU32,
    /// Payload bytes, written by the writer before the block is
    /// published.
    len: AtomicU32,
    _pad: [u8; 16],
}

const _: () = assert!(size_of::<BlockHeader>() == BLOCK_HEADER_BYTES);

/// Names a block: its byte offset in the block space and its class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlockHandle {
    pub offset: u64,
    pub class: u32,
}

impl BlockHandle {
    /// The payload bytes a block of this class holds.
    #[inline]
    pub fn payload_capacity(&self) -> usize {
        (1usize << self.class) - BLOCK_HEADER_BYTES
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawArenaError {
    IoError(io::ErrorKind),
    /// The region on disk was built with another capacity or class range.
    LayoutMismatch,
    /// A payload larger than the largest class holds.
    TooLarge { len: usize, max: usize },
    /// No free block of the class, nothing left to carve and nothing
    /// larger to halve; the caller collects and tries again.
    Exhausted,
    /// A handle outside the block space, misaligned for its class, or of
    /// a class the arena does not have.
    BadHandle,
    /// The block is not being written by this process.
    NotWriting,
    /// A block header the walk cannot read: its class is outside the
    /// arena's range.
    Corrupt { offset: u64 },
}

impl From<io::Error> for RawArenaError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e.kind())
    }
}

impl std::fmt::Display for RawArenaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(k) => write!(f, "I/O error: {k:?}"),
            Self::LayoutMismatch => write!(f, "the arena on disk has another capacity or class range"),
            Self::TooLarge { len, max } => write!(f, "a payload of {len} bytes; the largest block holds {max}"),
            Self::Exhausted => write!(f, "no free block, nothing to carve and nothing to halve"),
            Self::BadHandle => write!(f, "the handle names no block of this arena"),
            Self::NotWriting => write!(f, "the block is not being written by this process"),
            Self::Corrupt { offset } => write!(f, "the block header at offset {offset} names no class"),
        }
    }
}

impl std::error::Error for RawArenaError {}

/// What one [`collect`](RawArena::collect) walk found and did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CollectReport {
    /// Chunks whose carver died before building their header, made free.
    pub unbuilt_freed: usize,
    /// Free blocks, each given its bit.
    pub free: usize,
    /// Blocks a live process is writing.
    pub writing: usize,
    /// Published blocks the roots reach.
    pub published: usize,
    /// Blocks a dead process was writing that the roots reach, published.
    pub published_for_dead: usize,
    /// Blocks a dead process was writing that the roots do not reach,
    /// retired.
    pub retired_dead: usize,
    /// Published blocks the roots do not reach, retired.
    pub retired_unreached: usize,
    /// Retired blocks, each given its bit.
    pub retired: usize,
}

pub struct RawArena {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    min_class: u32,
    max_class: u32,
    /// Where the bitmaps start, in bytes.
    bitmaps_offset: usize,
    /// Where the block space starts, in bytes.
    blocks_offset: usize,
    /// Words each class's bitmap takes, by class index.
    words: Vec<usize>,
    /// The first word of each class's free bitmap, by class index.
    free_start: Vec<usize>,
    /// The first word of each class's retired bitmap, by class index.
    retired_start: Vec<usize>,
    collector: HolderTable,
}

// The mapping holds atomics and payload bytes; every access goes through
// an atomic or copies bytes out under the state that guards them.
unsafe impl Send for RawArena {}
unsafe impl Sync for RawArena {}

/// The words each class's bitmap takes for `capacity` bytes of block space.
fn bitmap_words(capacity: usize, min_class: u32, max_class: u32) -> Vec<usize> {
    (min_class..=max_class).map(|c| (capacity >> c).div_ceil(64)).collect()
}

/// Bytes the region takes: the header, both bitmap sets and the block
/// space, which starts on a 64-byte boundary.
pub fn raw_arena_file_size(capacity: usize, min_class: u32, max_class: u32) -> usize {
    let words: usize = bitmap_words(capacity, min_class, max_class).iter().sum();
    let bitmaps = 2 * words * size_of::<AtomicU64>();
    (HEADER_BYTES + bitmaps).next_multiple_of(64) + capacity
}

impl RawArena {
    /// Obtain the arena at `path` with `capacity` bytes of block space and
    /// classes `min_class..=max_class`, initializing an empty one if the
    /// path does not yet exist and attaching to it if it does. A region
    /// built with another capacity or class range is a `LayoutMismatch`.
    ///
    /// # Panics
    /// If the classes are outside [`SMALLEST_CLASS`]`..=`[`LARGEST_CLASS`]
    /// or out of order, or `capacity` is not a positive multiple of the
    /// largest class's block.
    pub fn create(path: impl AsRef<Path>, capacity: usize, min_class: u32, max_class: u32) -> Result<Self, RawArenaError> {
        Self::check_shape(capacity, min_class, max_class);
        let total = raw_arena_file_size(capacity, min_class, max_class);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            total,
            |ptr| unsafe { Self::init_region(ptr, capacity, min_class, max_class) },
            |ptr| unsafe { (*(ptr as *const ArenaHeader)).magic.load(Ordering::Acquire) == RAW_ARENA_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, RawArenaError::LayoutMismatch))?;
        Self::from_region(file, mmap, capacity, min_class, max_class)
    }

    /// Attach to an existing arena; an absent file is an I/O error and a
    /// region of another shape a `LayoutMismatch`.
    pub fn open(path: impl AsRef<Path>, capacity: usize, min_class: u32, max_class: u32) -> Result<Self, RawArenaError> {
        Self::check_shape(capacity, min_class, max_class);
        let total = raw_arena_file_size(capacity, min_class, max_class);
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(RawArenaError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::from_region(file, mmap, capacity, min_class, max_class)
    }

    fn check_shape(capacity: usize, min_class: u32, max_class: u32) {
        assert!(
            (SMALLEST_CLASS..=LARGEST_CLASS).contains(&min_class) && min_class <= max_class && max_class <= LARGEST_CLASS,
            "classes {min_class}..={max_class} are outside {SMALLEST_CLASS}..={LARGEST_CLASS} or out of order"
        );
        let top = 1usize << max_class;
        assert!(
            capacity >= top && capacity.is_multiple_of(top),
            "capacity {capacity} is not a positive multiple of the largest block, {top} bytes"
        );
    }

    /// Lay out an empty arena: capacity, classes and a bump at the start
    /// of the block space first, the magic last, because attachers spin
    /// on it.
    ///
    /// # Safety
    /// `ptr` addresses at least `raw_arena_file_size(capacity, min_class,
    /// max_class)` writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, min_class: u32, max_class: u32) {
        let hdr = ptr as *mut ArenaHeader;
        unsafe {
            (*hdr).capacity.store(capacity as u64, Ordering::Relaxed);
            (*hdr).min_class.store(min_class, Ordering::Relaxed);
            (*hdr).max_class.store(max_class, Ordering::Relaxed);
            (*hdr).bump.store(0, Ordering::Relaxed);
            (*hdr).magic.store(RAW_ARENA_MAGIC, Ordering::Release);
        }
    }

    fn from_region(file: File, mmap: MmapMut, capacity: usize, min_class: u32, max_class: u32) -> Result<Self, RawArenaError> {
        let hdr = unsafe { &*(mmap.as_ptr() as *const ArenaHeader) };
        if hdr.magic.load(Ordering::Acquire) != RAW_ARENA_MAGIC
            || hdr.capacity.load(Ordering::Acquire) != capacity as u64
            || hdr.min_class.load(Ordering::Acquire) != min_class
            || hdr.max_class.load(Ordering::Acquire) != max_class
        {
            return Err(RawArenaError::LayoutMismatch);
        }
        let words = bitmap_words(capacity, min_class, max_class);
        let mut free_start = Vec::with_capacity(words.len());
        let mut next = 0usize;
        for w in &words {
            free_start.push(next);
            next += w;
        }
        let mut retired_start = Vec::with_capacity(words.len());
        for w in &words {
            retired_start.push(next);
            next += w;
        }
        let bitmaps_offset = HEADER_BYTES;
        let blocks_offset = (bitmaps_offset + next * size_of::<AtomicU64>()).next_multiple_of(64);
        let collector = unsafe { HolderTable::from_ptr(mmap.as_ptr().add(COLLECTOR_OFFSET), 1) };
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            min_class,
            max_class,
            bitmaps_offset,
            blocks_offset,
            words,
            free_start,
            retired_start,
            collector,
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn min_class(&self) -> u32 {
        self.min_class
    }

    #[inline]
    pub fn max_class(&self) -> u32 {
        self.max_class
    }

    /// The largest payload one block holds.
    #[inline]
    pub fn max_payload(&self) -> usize {
        (1usize << self.max_class) - BLOCK_HEADER_BYTES
    }

    /// Bytes of block space carved so far.
    pub fn carved(&self) -> usize {
        self.header().bump.load(Ordering::Acquire) as usize
    }

    /// The smallest class whose block holds `len` payload bytes, or
    /// `None` when no class does.
    pub fn class_for(&self, len: usize) -> Option<u32> {
        let needed = len.checked_add(BLOCK_HEADER_BYTES)?;
        (self.min_class..=self.max_class).find(|&c| (1usize << c) >= needed)
    }

    #[inline]
    fn header(&self) -> &ArenaHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ArenaHeader) }
    }

    #[inline]
    fn class_index(&self, class: u32) -> usize {
        (class - self.min_class) as usize
    }

    #[inline]
    fn hint(&self, class: u32) -> &AtomicU32 {
        let at = HINTS_OFFSET + self.class_index(class) * size_of::<AtomicU32>();
        unsafe { &*(self.mmap.as_ptr().add(at) as *const AtomicU32) }
    }

    #[inline]
    fn word(&self, index: usize) -> &AtomicU64 {
        let at = self.bitmaps_offset + index * size_of::<AtomicU64>();
        unsafe { &*(self.mmap.as_ptr().add(at) as *const AtomicU64) }
    }

    /// The bits a class's bitmap has: one per block of that class the
    /// block space could hold.
    #[inline]
    fn bits(&self, class: u32) -> usize {
        self.capacity >> class
    }

    #[inline]
    fn set_bit(&self, start: usize, index: usize) {
        self.word(start + index / 64).fetch_or(1u64 << (index % 64), Ordering::AcqRel);
    }

    #[inline]
    fn clear_bit(&self, start: usize, index: usize) {
        self.word(start + index / 64).fetch_and(!(1u64 << (index % 64)), Ordering::AcqRel);
    }

    #[inline]
    fn set_free(&self, class: u32, index: usize) {
        self.set_bit(self.free_start[self.class_index(class)], index);
    }

    #[inline]
    fn clear_free(&self, class: u32, index: usize) {
        self.clear_bit(self.free_start[self.class_index(class)], index);
    }

    #[inline]
    fn set_retired(&self, class: u32, index: usize) {
        self.set_bit(self.retired_start[self.class_index(class)], index);
    }

    #[inline]
    fn clear_retired(&self, class: u32, index: usize) {
        self.clear_bit(self.retired_start[self.class_index(class)], index);
    }

    #[inline]
    fn block(&self, offset: u64) -> &BlockHeader {
        unsafe { &*(self.mmap.as_ptr().add(self.blocks_offset + offset as usize) as *const BlockHeader) }
    }

    #[inline]
    fn payload_ptr(&self, offset: u64) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(self.blocks_offset + offset as usize + BLOCK_HEADER_BYTES) as *mut u8 }
    }

    fn check_handle(&self, h: BlockHandle) -> Result<(), RawArenaError> {
        if h.class < self.min_class
            || h.class > self.max_class
            || h.offset >= self.capacity as u64
            || !h.offset.is_multiple_of(1u64 << h.class)
        {
            return Err(RawArenaError::BadHandle);
        }
        Ok(())
    }

    /// Free blocks of `class` by the bitmap, as it stands.
    pub fn free_blocks(&self, class: u32) -> usize {
        let ci = self.class_index(class);
        (0..self.words[ci]).map(|w| self.word(self.free_start[ci] + w).load(Ordering::Acquire).count_ones() as usize).sum()
    }

    /// Retired blocks of `class` by the bitmap, as it stands.
    pub fn retired_blocks(&self, class: u32) -> usize {
        let ci = self.class_index(class);
        (0..self.words[ci]).map(|w| self.word(self.retired_start[ci] + w).load(Ordering::Acquire).count_ones() as usize).sum()
    }

    /// Take a free block of `class` for this process, or `None` when the
    /// class's bitmap has none. A bit whose block is no longer free, or no
    /// longer of the class, is cleared and passed. The block handed back
    /// may be of a larger class, when the bit named a block that was
    /// halved and came free again as its head; the caller halves it.
    fn take_free(&self, class: u32) -> Option<BlockHandle> {
        let ci = self.class_index(class);
        let words = self.words[ci];
        if words == 0 {
            return None;
        }
        let bits = self.bits(class);
        let hint = self.hint(class).load(Ordering::Relaxed) as usize % words;
        let pid = u64::from(std::process::id());
        for step in 0..words {
            let w = (hint + step) % words;
            let mut word = self.word(self.free_start[ci] + w).load(Ordering::Acquire);
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= !(1u64 << bit);
                let index = w * 64 + bit;
                if index >= bits {
                    break;
                }
                self.clear_free(class, index);
                let offset = (index as u64) << class;
                let b = self.block(offset);
                if b.class.load(Ordering::Acquire) != class {
                    continue;
                }
                if b
                    .state
                    .compare_exchange(state_word(FREE, 0), state_word(WRITING, pid), Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                let got = b.class.load(Ordering::Acquire);
                if got < class {
                    // The block was halved and came free again as a smaller
                    // head between the class check and the swap.
                    self.give_back(BlockHandle { offset, class: got });
                    continue;
                }
                self.hint(class).store(w as u32, Ordering::Relaxed);
                return Some(BlockHandle { offset, class: got });
            }
        }
        None
    }

    /// Return a block this process is writing to the free set.
    fn give_back(&self, h: BlockHandle) {
        let b = self.block(h.offset);
        b.len.store(0, Ordering::Relaxed);
        b.state.store(state_word(FREE, 0), Ordering::Release);
        self.set_free(h.class, (h.offset >> h.class) as usize);
    }

    /// Move every retired block of `class` that no pin can see to the free
    /// set, and say how many moved.
    pub fn reclaim(&self, class: u32, epochs: &SharedEpochs) -> usize {
        let ci = self.class_index(class);
        let horizon = epochs.reclaim_horizon();
        let bits = self.bits(class);
        let mut moved = 0;
        for w in 0..self.words[ci] {
            let mut word = self.word(self.retired_start[ci] + w).load(Ordering::Acquire);
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= !(1u64 << bit);
                let index = w * 64 + bit;
                if index >= bits {
                    break;
                }
                let offset = (index as u64) << class;
                let b = self.block(offset);
                let state = b.state.load(Ordering::Acquire);
                if tag_of(state) != RETIRED || b.class.load(Ordering::Acquire) != class {
                    self.clear_retired(class, index);
                    continue;
                }
                if payload_of(state) > horizon {
                    continue;
                }
                if b
                    .state
                    .compare_exchange(state, state_word(FREE, 0), Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    b.len.store(0, Ordering::Relaxed);
                    self.set_free(class, index);
                    self.clear_retired(class, index);
                    moved += 1;
                }
            }
        }
        moved
    }

    /// Carve the next chunk of the largest class for this process, or
    /// `None` when the block space is used up.
    fn carve(&self) -> Option<BlockHandle> {
        let top = 1u64 << self.max_class;
        let pid = u64::from(std::process::id());
        loop {
            let at = self.header().bump.load(Ordering::Acquire);
            if at + top > self.capacity as u64 {
                return None;
            }
            if self
                .header()
                .bump
                .compare_exchange(at, at + top, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let b = self.block(at);
            b.class.store(self.max_class, Ordering::Release);
            b.len.store(0, Ordering::Relaxed);
            // A collector that found the chunk unbuilt has made it free,
            // and another writer may have taken it from there.
            let writing = state_word(WRITING, pid);
            let mut from = state_word(UNBUILT, 0);
            loop {
                match b.state.compare_exchange(from, writing, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_was) => return Some(BlockHandle { offset: at, class: self.max_class }),
                    Err(now) if tag_of(now) == FREE => from = now,
                    Err(_taken) => break,
                }
            }
        }
    }

    /// Halve a block this process is writing down to `class`, giving each
    /// spare half to its class's free set.
    fn split_down(&self, h: BlockHandle, class: u32) -> BlockHandle {
        let mut current = h.class;
        while current > class {
            current -= 1;
            let spare = h.offset + (1u64 << current);
            let s = self.block(spare);
            s.class.store(current, Ordering::Release);
            s.len.store(0, Ordering::Relaxed);
            s.state.store(state_word(FREE, 0), Ordering::Release);
            self.block(h.offset).class.store(current, Ordering::Release);
            self.set_free(current, (spare >> current) as usize);
        }
        BlockHandle { offset: h.offset, class }
    }

    /// Take a free block of any larger class and halve it down to `class`.
    fn take_larger(&self, class: u32) -> Option<BlockHandle> {
        (class + 1..=self.max_class).find_map(|larger| self.take_free(larger).map(|h| self.split_down(h, class)))
    }

    /// Take a block for `len` payload bytes for this process to write: a
    /// free block of the smallest class that holds it, else one reclaimed
    /// from the class's retired blocks no pin can see, else the next
    /// chunk carved from the block space and halved, else a larger free
    /// block halved. `Exhausted` when none of those exist; the caller
    /// [`collect`](Self::collect)s and tries once more.
    pub fn allocate(&self, len: usize, epochs: &SharedEpochs) -> Result<BlockHandle, RawArenaError> {
        let class = self.class_for(len).ok_or(RawArenaError::TooLarge { len, max: self.max_payload() })?;
        if let Some(h) = self.take_free(class) {
            return Ok(self.split_down(h, class));
        }
        self.reclaim(class, epochs);
        if let Some(h) = self.take_free(class) {
            return Ok(self.split_down(h, class));
        }
        if let Some(h) = self.carve() {
            return Ok(self.split_down(h, class));
        }
        if let Some(h) = self.take_larger(class) {
            return Ok(h);
        }
        Err(RawArenaError::Exhausted)
    }

    /// Whether `h` is a block this process is writing.
    fn writing_here(&self, h: BlockHandle) -> bool {
        let b = self.block(h.offset);
        b.state.load(Ordering::Acquire) == state_word(WRITING, u64::from(std::process::id()))
            && b.class.load(Ordering::Acquire) == h.class
    }

    /// Copy `bytes` into the payload of a block this process is writing,
    /// and record their length.
    pub fn write_payload(&self, h: BlockHandle, bytes: &[u8]) -> Result<(), RawArenaError> {
        self.check_handle(h)?;
        if bytes.len() > h.payload_capacity() {
            return Err(RawArenaError::TooLarge { len: bytes.len(), max: h.payload_capacity() });
        }
        if !self.writing_here(h) {
            return Err(RawArenaError::NotWriting);
        }
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.payload_ptr(h.offset), bytes.len()) };
        self.block(h.offset).len.store(bytes.len() as u32, Ordering::Release);
        Ok(())
    }

    /// Publish a block this process is writing, once its handle is
    /// reachable from the structure that names it.
    pub fn publish(&self, h: BlockHandle) -> Result<(), RawArenaError> {
        self.check_handle(h)?;
        let b = self.block(h.offset);
        if b.class.load(Ordering::Acquire) != h.class {
            return Err(RawArenaError::NotWriting);
        }
        match b.state.compare_exchange(
            state_word(WRITING, u64::from(std::process::id())),
            state_word(PUBLISHED, 0),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_was) => Ok(()),
            Err(_found) => Err(RawArenaError::NotWriting),
        }
    }

    /// Give back a block this process is writing and will not publish.
    pub fn abandon(&self, h: BlockHandle) -> Result<(), RawArenaError> {
        self.check_handle(h)?;
        if !self.writing_here(h) {
            return Err(RawArenaError::NotWriting);
        }
        self.give_back(h);
        Ok(())
    }

    /// Retire a published block at the epoch [`SharedEpochs::advance`]
    /// hands back, so it comes free once no pin can see it. `Ok(false)`
    /// when the block is not published under this handle: already
    /// retired, or reused since.
    pub fn retire(&self, h: BlockHandle, epochs: &SharedEpochs) -> Result<bool, RawArenaError> {
        self.check_handle(h)?;
        let b = self.block(h.offset);
        if b.class.load(Ordering::Acquire) != h.class {
            return Ok(false);
        }
        let at = epochs.advance();
        if b
            .state
            .compare_exchange(state_word(PUBLISHED, 0), state_word(RETIRED, at), Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(false);
        }
        self.set_retired(h.class, (h.offset >> h.class) as usize);
        Ok(true)
    }

    /// Copy the payload of a published or retired block into `out`,
    /// replacing its contents. `Ok(false)` when the block holds no value
    /// under this handle: free, being written, or of another class. A
    /// reader that took a pin before it learned the handle reads the
    /// bytes the writer published, since the block cannot come free while
    /// the pin lives.
    pub fn read_into(&self, h: BlockHandle, out: &mut Vec<u8>) -> Result<bool, RawArenaError> {
        self.check_handle(h)?;
        let b = self.block(h.offset);
        if b.class.load(Ordering::Acquire) != h.class {
            return Ok(false);
        }
        let holds = |state: u64| matches!(tag_of(state), PUBLISHED | RETIRED);
        if !holds(b.state.load(Ordering::Acquire)) {
            return Ok(false);
        }
        let len = b.len.load(Ordering::Acquire) as usize;
        if len > h.payload_capacity() {
            return Ok(false);
        }
        out.clear();
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(self.payload_ptr(h.offset), len) });
        Ok(holds(b.state.load(Ordering::Acquire)) && b.class.load(Ordering::Acquire) == h.class)
    }

    /// Copy the payload of any built block into `out`, whatever its state,
    /// replacing `out`'s contents: what a collector reads to learn which
    /// name a block was written for. `Ok(false)` when the block is free,
    /// unbuilt, or of another class.
    pub fn peek_payload(&self, h: BlockHandle, out: &mut Vec<u8>) -> Result<bool, RawArenaError> {
        self.check_handle(h)?;
        let b = self.block(h.offset);
        if b.class.load(Ordering::Acquire) != h.class {
            return Ok(false);
        }
        if matches!(tag_of(b.state.load(Ordering::Acquire)), UNBUILT | FREE) {
            return Ok(false);
        }
        let len = b.len.load(Ordering::Acquire) as usize;
        if len > h.payload_capacity() {
            return Ok(false);
        }
        out.clear();
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(self.payload_ptr(h.offset), len) });
        Ok(true)
    }

    /// The pid of the process writing a block, or `None` when no process
    /// is.
    pub fn writer_of(&self, h: BlockHandle) -> Result<Option<u32>, RawArenaError> {
        self.check_handle(h)?;
        let b = self.block(h.offset);
        if b.class.load(Ordering::Acquire) != h.class {
            return Ok(None);
        }
        let state = b.state.load(Ordering::Acquire);
        Ok((tag_of(state) == WRITING).then_some(payload_of(state) as u32))
    }

    /// Whether the process `pid` is alive on this host.
    pub fn process_alive(pid: u32) -> bool {
        pid != 0 && crate::peer_directory::process_alive(pid)
    }

    /// Walk every block from the front of the block space to the bump and
    /// put each where its header says it belongs, with `reached` saying
    /// whether the caller's roots name a block. One process collects at a
    /// time; a caller finding the collector's slot held waits for it, and
    /// reaps it when its holder has died. Blocks retired here come free
    /// through the ordinary reclaim once no pin can see them.
    pub fn collect(&self, epochs: &SharedEpochs, reached: impl Fn(BlockHandle) -> bool) -> Result<CollectReport, RawArenaError> {
        while !self.collector.try_claim_slot(0, COLLECTING) {
            self.collector.reap_dead();
            std::thread::yield_now();
        }
        let walked = self.walk(epochs, reached);
        self.collector.release(0);
        walked
    }

    fn walk(&self, epochs: &SharedEpochs, reached: impl Fn(BlockHandle) -> bool) -> Result<CollectReport, RawArenaError> {
        let now = epochs.advance();
        let bump = self.header().bump.load(Ordering::Acquire);
        let top = 1u64 << self.max_class;
        let mut report = CollectReport::default();
        let mut at = 0u64;
        while at < bump {
            let b = self.block(at);
            let state = b.state.load(Ordering::Acquire);
            if tag_of(state) == UNBUILT {
                b.class.store(self.max_class, Ordering::Release);
                b.len.store(0, Ordering::Relaxed);
                if b
                    .state
                    .compare_exchange(state, state_word(FREE, 0), Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.set_free(self.max_class, (at >> self.max_class) as usize);
                    report.unbuilt_freed += 1;
                }
                at += top;
                continue;
            }
            let class = b.class.load(Ordering::Acquire);
            if class < self.min_class || class > self.max_class {
                return Err(RawArenaError::Corrupt { offset: at });
            }
            let index = (at >> class) as usize;
            let h = BlockHandle { offset: at, class };
            match tag_of(state) {
                FREE => {
                    self.set_free(class, index);
                    report.free += 1;
                }
                WRITING => {
                    let pid = payload_of(state) as u32;
                    if pid != 0 && crate::peer_directory::process_alive(pid) {
                        report.writing += 1;
                    } else if reached(h) {
                        if b
                            .state
                            .compare_exchange(state, state_word(PUBLISHED, 0), Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            report.published_for_dead += 1;
                        }
                    } else if b
                        .state
                        .compare_exchange(state, state_word(RETIRED, now), Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.set_retired(class, index);
                        report.retired_dead += 1;
                    }
                }
                PUBLISHED => {
                    if reached(h) {
                        report.published += 1;
                    } else if b
                        .state
                        .compare_exchange(state, state_word(RETIRED, now), Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.set_retired(class, index);
                        report.retired_unreached += 1;
                    }
                }
                RETIRED => {
                    self.set_retired(class, index);
                    report.retired += 1;
                }
                _ => return Err(RawArenaError::Corrupt { offset: at }),
            }
            at += 1u64 << class;
        }
        Ok(report)
    }
}

impl std::fmt::Debug for RawArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawArena")
            .field("capacity", &self.capacity)
            .field("min_class", &self.min_class)
            .field("max_class", &self.max_class)
            .field("carved", &self.carved())
            .finish()
    }
}

/// Ways a test puts the arena into a state a crash would leave.
#[cfg(test)]
impl RawArena {
    /// Overwrite a block's state word.
    fn set_state(&self, h: BlockHandle, tag: u64, payload: u64) {
        self.block(h.offset).state.store(state_word(tag, payload), Ordering::Release);
    }

    /// Move the bump past one chunk without building it, as a carver
    /// that died between the two leaves it.
    fn carve_without_building(&self) {
        self.header().bump.fetch_add(1u64 << self.max_class, Ordering::AcqRel);
    }

    /// Clear every free and retired bit, as none of the states they
    /// stand for change.
    fn clear_all_bits(&self) {
        let total: usize = self.words.iter().sum::<usize>() * 2;
        for w in 0..total {
            self.word(w).store(0, Ordering::Release);
        }
    }

    /// Set the free bit of a block whatever its state.
    fn set_free_bit_of(&self, h: BlockHandle) {
        self.set_free(h.class, (h.offset >> h.class) as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const CAPACITY: usize = 64 * 1024;
    const MIN: u32 = 6;
    const MAX: u32 = 12;
    const PINS: usize = 4;
    /// A pid the alive check answers false for on every host: past a
    /// pid_t on Unix, and naming no process on Windows.
    const NO_SUCH_PID: u64 = u32::MAX as u64 - 1;

    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-arena-{name}-{}.bin", std::process::id()))
    }

    fn arena(name: &str) -> (crate::test_paths::TmpFile, RawArena, SharedEpochs) {
        let p = tmp(name);
        let a = RawArena::create(&p, CAPACITY, MIN, MAX).unwrap();
        let e = SharedEpochs::create_anon(PINS).unwrap();
        (p, a, e)
    }

    /// Allocate, write and publish one payload.
    fn put(a: &RawArena, e: &SharedEpochs, bytes: &[u8]) -> BlockHandle {
        let h = a.allocate(bytes.len(), e).unwrap();
        a.write_payload(h, bytes).unwrap();
        a.publish(h).unwrap();
        h
    }

    fn read(a: &RawArena, h: BlockHandle) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        a.read_into(h, &mut out).unwrap().then_some(out)
    }

    #[test]
    fn a_block_holds_its_payload_once_published() {
        let (_p, a, e) = arena("publish");
        let bytes: Vec<u8> = (0..100u8).collect();
        let h = a.allocate(bytes.len(), &e).unwrap();
        assert_eq!(h.class, 8, "100 bytes and a 32-byte header take a 256-byte block");
        a.write_payload(h, &bytes).unwrap();
        assert_eq!(read(&a, h), None, "a block being written holds no value yet");
        a.publish(h).unwrap();
        assert_eq!(read(&a, h).as_deref(), Some(&bytes[..]));
        assert_eq!(a.carved(), 1 << MAX, "one chunk was carved for it");
        assert_eq!(a.publish(h), Err(RawArenaError::NotWriting), "a published block is not written again");
    }

    #[test]
    fn allocation_halves_a_chunk_and_frees_each_spare_half() {
        let (_p, a, e) = arena("halve");
        let h = a.allocate(1, &e).unwrap();
        assert_eq!((h.offset, h.class), (0, MIN));
        for class in MIN..MAX {
            assert_eq!(a.free_blocks(class), 1, "one spare half of class {class}");
        }
        assert_eq!(a.free_blocks(MAX), 0);
        let second = a.allocate(1, &e).unwrap();
        assert_eq!((second.offset, second.class), (64, MIN), "the next block is the spare half beside it");
        assert_eq!(a.carved(), 1 << MAX, "no second chunk was carved");
    }

    #[test]
    fn a_retired_block_comes_back_once_no_pin_sees_it() {
        let (_p, a, e) = arena("retire");
        let h = put(&a, &e, b"first");
        let pin = e.pin().unwrap();
        assert!(a.retire(h, &e).unwrap());
        assert!(!a.retire(h, &e).unwrap(), "a block retires once");
        assert_eq!(read(&a, h).as_deref(), Some(&b"first"[..]), "a pinned reader still reads it");
        assert_eq!(a.reclaim(MIN, &e), 0, "held by the pin");
        assert_eq!(a.retired_blocks(MIN), 1);
        drop(pin);
        assert_eq!(a.reclaim(MIN, &e), 1);
        assert_eq!((a.retired_blocks(MIN), a.free_blocks(MIN)), (0, 2));
        assert_eq!(read(&a, h), None, "a free block holds no value");
    }

    #[test]
    fn a_full_arena_reports_and_collecting_unreached_blocks_refills_it() {
        let (_p, a, e) = arena("exhaust");
        let len = (1usize << MAX) - BLOCK_HEADER_BYTES;
        let mut handles = Vec::new();
        loop {
            match a.allocate(len, &e) {
                Ok(h) => {
                    a.write_payload(h, &vec![7u8; len]).unwrap();
                    a.publish(h).unwrap();
                    handles.push(h);
                }
                Err(RawArenaError::Exhausted) => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(handles.len(), CAPACITY >> MAX, "one whole-chunk block per chunk");
        let report = a.collect(&e, |_| false).unwrap();
        assert_eq!(report.retired_unreached, handles.len());
        let h = a.allocate(len, &e).unwrap();
        assert!(handles.contains(&h), "the block came from a collected one");
    }

    #[test]
    fn collecting_publishes_a_dead_writers_reached_block_and_retires_its_other() {
        let (_p, a, e) = arena("dead-writer");
        let reached = a.allocate(10, &e).unwrap();
        a.write_payload(reached, b"kept").unwrap();
        let lost = a.allocate(10, &e).unwrap();
        a.write_payload(lost, b"gone").unwrap();
        a.set_state(reached, WRITING, NO_SUCH_PID);
        a.set_state(lost, WRITING, NO_SUCH_PID);
        let report = a.collect(&e, |h| h == reached).unwrap();
        assert_eq!((report.published_for_dead, report.retired_dead), (1, 1));
        assert_eq!(read(&a, reached).as_deref(), Some(&b"kept"[..]));
        assert_eq!(a.retired_blocks(MIN), 1);
        assert_eq!(a.reclaim(MIN, &e), 1);
        assert_eq!(read(&a, lost), None);
    }

    #[test]
    fn collecting_frees_a_chunk_whose_carver_died_before_its_header() {
        let (_p, a, e) = arena("unbuilt");
        a.carve_without_building();
        assert_eq!(a.free_blocks(MAX), 0);
        let report = a.collect(&e, |_| false).unwrap();
        assert_eq!(report.unbuilt_freed, 1);
        assert_eq!(a.free_blocks(MAX), 1);
        let h = a.allocate((1 << MAX) - BLOCK_HEADER_BYTES, &e).unwrap();
        assert_eq!((h.offset, h.class), (0, MAX), "the block is the recovered chunk");
        assert_eq!(a.carved(), 1 << MAX, "nothing more was carved");
    }

    #[test]
    fn a_stale_free_bit_costs_one_failed_swap() {
        let (_p, a, e) = arena("stale-bit");
        let taken = put(&a, &e, b"held");
        a.set_free_bit_of(taken);
        assert_eq!(a.free_blocks(MIN), 2, "the stale bit and the spare half");
        let h = a.allocate(1, &e).unwrap();
        assert_ne!(h, taken, "a published block is passed over");
        assert_eq!(a.free_blocks(MIN), 0, "the stale bit is cleared");
        assert_eq!(read(&a, taken).as_deref(), Some(&b"held"[..]));
    }

    #[test]
    fn collecting_gives_free_and_retired_blocks_their_bits_back() {
        let (_p, a, e) = arena("rebuild-bits");
        let kept = put(&a, &e, b"kept");
        let retired = put(&a, &e, b"old");
        assert!(a.retire(retired, &e).unwrap());
        let free_before: Vec<usize> = (MIN..=MAX).map(|c| a.free_blocks(c)).collect();
        a.clear_all_bits();
        assert!((MIN..=MAX).all(|c| a.free_blocks(c) == 0 && a.retired_blocks(c) == 0));
        let report = a.collect(&e, |h| h == kept).unwrap();
        assert_eq!((report.published, report.retired), (1, 1));
        let free_after: Vec<usize> = (MIN..=MAX).map(|c| a.free_blocks(c)).collect();
        assert_eq!(free_after, free_before);
        assert_eq!(a.retired_blocks(MIN), 1);
    }

    #[test]
    fn racing_writers_take_distinct_blocks_until_the_arena_is_full() {
        let (_p, a, e) = arena("race");
        let a = Arc::new(a);
        let e = Arc::new(e);
        let threads = std::thread::available_parallelism().expect("the host reports its logical processors").get();
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let (a, e) = (Arc::clone(&a), Arc::clone(&e));
                std::thread::spawn(move || {
                    let mut mine = Vec::new();
                    loop {
                        match a.allocate(1, &e) {
                            Ok(h) => mine.push(h),
                            Err(RawArenaError::Exhausted) => return mine,
                            Err(e) => panic!("{e}"),
                        }
                    }
                })
            })
            .collect();
        let mut all: Vec<BlockHandle> = workers.into_iter().flat_map(|w| w.join().unwrap()).collect();
        let taken = all.len();
        all.sort_by_key(|h| h.offset);
        all.dedup();
        assert_eq!(all.len(), taken, "a block was handed to two writers");
        assert_eq!(taken, CAPACITY >> MIN, "every smallest block was handed out once");
    }

    #[test]
    fn handles_outside_the_arena_and_payloads_past_the_largest_block_are_refused() {
        let (_p, a, e) = arena("refused");
        let mut out = Vec::new();
        for bad in [
            BlockHandle { offset: CAPACITY as u64, class: MIN },
            BlockHandle { offset: 32, class: MIN },
            BlockHandle { offset: 0, class: MAX + 1 },
            BlockHandle { offset: 0, class: MIN - 1 },
        ] {
            assert_eq!(a.read_into(bad, &mut out), Err(RawArenaError::BadHandle), "{bad:?}");
        }
        let max = a.max_payload();
        assert_eq!(a.allocate(max + 1, &e), Err(RawArenaError::TooLarge { len: max + 1, max }));
        let h = a.allocate(max, &e).unwrap();
        assert_eq!(h.class, MAX);
        assert_eq!(a.write_payload(h, &vec![1u8; max + 1]), Err(RawArenaError::TooLarge { len: max + 1, max }));
    }

    #[test]
    fn an_abandoned_block_is_free_again_and_open_refuses_another_shape() {
        let (p, a, e) = arena("abandon");
        let h = a.allocate(1, &e).unwrap();
        a.abandon(h).unwrap();
        assert_eq!(a.abandon(h), Err(RawArenaError::NotWriting));
        assert_eq!(a.allocate(1, &e).unwrap(), h, "the abandoned block is handed out again");
        assert_eq!(RawArena::open(&p, CAPACITY * 2, MIN, MAX).err(), Some(RawArenaError::LayoutMismatch));
        assert_eq!(RawArena::open(&p, CAPACITY, MIN, MAX - 1).err(), Some(RawArenaError::LayoutMismatch));
        let again = RawArena::open(&p, CAPACITY, MIN, MAX).unwrap();
        assert_eq!(again.carved(), 1 << MAX);
    }
}
