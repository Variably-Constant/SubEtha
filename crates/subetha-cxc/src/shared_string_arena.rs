//! `SharedStringArena` - append-only position-independent string
//! pool backed by an MMF.
//!
//! # Why this exists
//!
//! Variable-length strings can't be stored inline in fixed-size
//! slots (SharedHashMap, SharedVec, etc.) without padding waste or
//! truncation. The natural cross-process solution is a shared byte
//! arena: every process maps the same file at (potentially)
//! different base addresses, and string references are
//! position-independent `(offset, len)` pairs. Adding
//! `mmap_base + offset` in any process resolves to the same bytes.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | ArenaHeader (64B)         |
//! |   magic, capacity_bytes   |
//! |   used_bytes: AtomicU64   |
//! +---------------------------+
//! | bytes[0 .. capacity]      |
//! +---------------------------+
//! ```
//!
//! # Protocol
//!
//! `intern(s)`:
//! 1. `offset = used_bytes.fetch_add(len)`.
//! 2. If `offset + len > capacity`, rollback with
//!    `fetch_sub(len)` and return `Full`. (Note: the
//!    rollback is best-effort; if two threads race-overflow
//!    simultaneously, both fetch_subs leave the counter
//!    deterministic without "losing" bytes.)
//! 3. Memcpy `s.bytes()` into `arena[offset..offset+len]`.
//! 4. Return `StringRef { offset, len }`.
//!
//! `get(r)`:
//! - Bounds-check `r.offset + r.len <= used_bytes` (sanity), then
//!   return `&arena[r.offset..r.offset+r.len]` as a `&str`.
//!
//! # Concurrency
//!
//! Concurrent interners get distinct slices via fetch_add. Once
//! the bytes are written, they are never moved (append-only). A
//! reader holding a StringRef can always resolve it correctly,
//! provided their `get` happens AFTER the interner returned the
//! ref (which is the natural happens-before edge: the interner
//! does the write, then makes the ref visible to the reader).
//!
//! # Deduplication
//!
//! Not provided here. For dedup, layer a `SharedHashMap<u64 hash,
//! StringRef>` over the arena and consult it before each intern.
//!
//! # No deletion
//!
//! Append-only. The whole arena is reclaimed via `clear` (callers
//! must ensure no concurrent readers); fine-grained deletion
//! requires a free-list / compaction protocol that defeats the
//! point of an arena.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const ARENA_MAGIC: u64 = 0x4150_5341_524E_4131;

#[repr(C, align(64))]
pub struct ArenaHeader {
    pub magic: u64,
    pub capacity_bytes: u64,
    pub used_bytes: AtomicU64,
    _pad: [u8; 40],
}

const _: () = {
    assert!(size_of::<ArenaHeader>() == 64);
};

pub const fn arena_file_size(capacity_bytes: usize) -> usize {
    size_of::<ArenaHeader>() + capacity_bytes
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaError {
    Full,
    InvalidRef,
    InvalidUtf8,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for ArenaError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

/// Position-independent reference to a string in a SharedStringArena.
/// Encoded as a `u64` (offset:u32, len:u32) for stable cross-process
/// passing (the same u64 resolves to the same bytes in every process
/// that maps the arena).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StringRef {
    pub offset: u32,
    pub len: u32,
}

impl StringRef {
    #[inline]
    pub fn to_u64(self) -> u64 {
        ((self.offset as u64) << 32) | (self.len as u64)
    }
    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Self {
            offset: (v >> 32) as u32,
            len: v as u32,
        }
    }
}

pub struct SharedStringArena {
    _file: File,
    mmap: MmapMut,
    capacity_bytes: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedStringArena {}
unsafe impl Sync for SharedStringArena {}

impl subetha_sidecar::AdaptiveInstance for SharedStringArena {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedStringArena {
    pub fn create(
        path: impl AsRef<Path>, capacity_bytes: usize,
    ) -> Result<Self, ArenaError> {
        assert!(capacity_bytes >= 1);
        assert!(capacity_bytes <= u32::MAX as usize,
            "capacity_bytes must fit in u32 for StringRef offset");
        let total = arena_file_size(capacity_bytes);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut ArenaHeader;
        unsafe {
            std::ptr::write(hdr, ArenaHeader {
                magic: ARENA_MAGIC,
                capacity_bytes: capacity_bytes as u64,
                used_bytes: AtomicU64::new(0),
                _pad: [0; 40],
            });
        }
        Ok(Self {
            _file: file, mmap, capacity_bytes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, expected_capacity_bytes: usize,
    ) -> Result<Self, ArenaError> {
        let total = arena_file_size(expected_capacity_bytes);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(ArenaError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const ArenaHeader) };
        if hdr.magic != ARENA_MAGIC || hdr.capacity_bytes != expected_capacity_bytes as u64 {
            return Err(ArenaError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity_bytes: expected_capacity_bytes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity_bytes(&self) -> usize { self.capacity_bytes }

    #[inline]
    pub fn used_bytes(&self) -> usize {
        self.header().used_bytes.load(Ordering::Acquire) as usize
    }

    #[inline]
    pub fn remaining_bytes(&self) -> usize {
        self.capacity_bytes.saturating_sub(self.used_bytes())
    }

    fn header(&self) -> &ArenaHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ArenaHeader) }
    }

    /// Append a string to the arena. Returns a StringRef that
    /// resolves to the bytes in any mapping of the same file.
    ///
    /// Returns `Err(Full)` when the arena has no room. The empty
    /// string `""` interns at the current offset with `len = 0`.
    pub fn intern(&self, s: &str) -> Result<StringRef, ArenaError> {
        self.intern_bytes(s.as_bytes())
    }

    /// Append arbitrary bytes (not necessarily UTF-8) to the arena.
    /// Useful for storing binary blobs alongside strings. Retrieve
    /// with `get_bytes`; `get` will reject non-UTF-8 with
    /// `InvalidUtf8`.
    pub fn intern_bytes(&self, bytes: &[u8]) -> Result<StringRef, ArenaError> {
        let len = bytes.len() as u64;
        if len > self.capacity_bytes as u64 {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::string_arena::OP_INTERN, 1);
            return Err(ArenaError::Full);
        }
        let offset = self.header().used_bytes.fetch_add(len, Ordering::AcqRel);
        if offset.saturating_add(len) > self.capacity_bytes as u64 {
            self.header().used_bytes.fetch_sub(len, Ordering::AcqRel);
            self.ring_sidecar
                .push_op(crate::sidecar_ops::string_arena::OP_INTERN, 1);
            return Err(ArenaError::Full);
        }
        let dst = unsafe {
            self.mmap.as_ptr()
                .add(size_of::<ArenaHeader>())
                .add(offset as usize)
                as *mut u8
        };
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::string_arena::OP_INTERN, 0);
        Ok(StringRef { offset: offset as u32, len: len as u32 })
    }

    /// Resolve a StringRef to its `&[u8]`. Returns `Err(InvalidRef)`
    /// when the ref doesn't fall inside the arena's used region.
    pub fn get_bytes(&self, r: StringRef) -> Result<&[u8], ArenaError> {
        let end = (r.offset as u64).saturating_add(r.len as u64);
        if end > self.header().used_bytes.load(Ordering::Acquire) {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::string_arena::OP_GET_BYTES, 1);
            return Err(ArenaError::InvalidRef);
        }
        if end > self.capacity_bytes as u64 {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::string_arena::OP_GET_BYTES, 1);
            return Err(ArenaError::InvalidRef);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::string_arena::OP_GET_BYTES, 0);
        let base = unsafe {
            self.mmap.as_ptr()
                .add(size_of::<ArenaHeader>())
                .add(r.offset as usize)
        };
        Ok(unsafe { std::slice::from_raw_parts(base, r.len as usize) })
    }

    /// Resolve a StringRef to a `&str`. Returns `Err(InvalidUtf8)`
    /// when the bytes aren't valid UTF-8 (the arena doesn't enforce
    /// validity per-segment; it's checked on read).
    pub fn get(&self, r: StringRef) -> Result<&str, ArenaError> {
        let bytes = self.get_bytes(r)?;
        std::str::from_utf8(bytes).map_err(|_| ArenaError::InvalidUtf8)
    }

    /// Convenience: intern AND return a `&str` view into the
    /// just-written bytes plus the ref.
    pub fn intern_and_get(&self, s: &str) -> Result<(StringRef, &str), ArenaError> {
        let r = self.intern(s)?;
        let got = self.get(r)?;
        Ok((r, got))
    }

    /// Reset the arena to empty. NOT concurrency-safe; callers must
    /// ensure no other threads/processes are interning or reading.
    /// Existing StringRefs become invalid (their bytes may be
    /// overwritten by subsequent interns).
    pub fn clear(&self) {
        self.header().used_bytes.store(0, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::string_arena::OP_CLEAR, 0);
    }

    pub fn flush(&self) -> Result<(), ArenaError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), ArenaError> {
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
        p.push(format!("subetha-arena-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state_is_empty() {
        let p = tmp("init");
        let a = SharedStringArena::create(&p, 1024).unwrap();
        assert_eq!(a.capacity_bytes(), 1024);
        assert_eq!(a.used_bytes(), 0);
        assert_eq!(a.remaining_bytes(), 1024);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn intern_and_get_round_trip() {
        let p = tmp("rt");
        let a = SharedStringArena::create(&p, 1024).unwrap();
        let r1 = a.intern("hello").unwrap();
        let r2 = a.intern("world").unwrap();
        assert_eq!(a.get(r1).unwrap(), "hello");
        assert_eq!(a.get(r2).unwrap(), "world");
        assert_eq!(a.used_bytes(), 10);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_string_interns_with_zero_len() {
        let p = tmp("empty");
        let a = SharedStringArena::create(&p, 16).unwrap();
        let r = a.intern("").unwrap();
        assert_eq!(r.len, 0);
        assert_eq!(a.get(r).unwrap(), "");
        assert_eq!(a.used_bytes(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_arena_returns_error() {
        let p = tmp("full");
        let a = SharedStringArena::create(&p, 10).unwrap();
        a.intern("hello").unwrap();
        a.intern("world").unwrap();
        assert_eq!(a.intern("more").err(), Some(ArenaError::Full));
        // Used bytes rolled back, not 14.
        assert_eq!(a.used_bytes(), 10);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn string_too_large_returns_full() {
        let p = tmp("too-large");
        let a = SharedStringArena::create(&p, 8).unwrap();
        let big = "x".repeat(100);
        assert_eq!(a.intern(&big).err(), Some(ArenaError::Full));
        assert_eq!(a.used_bytes(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn string_ref_packs_and_unpacks() {
        let r = StringRef { offset: 0x1234_5678, len: 42 };
        let packed = r.to_u64();
        let unpacked = StringRef::from_u64(packed);
        assert_eq!(unpacked, r);
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer = SharedStringArena::create(&p, 1024).unwrap();
        let reader = SharedStringArena::open(&p, 1024).unwrap();
        let r = writer.intern("cross-process").unwrap();
        assert_eq!(reader.get(r).unwrap(), "cross-process");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn invalid_ref_beyond_used_rejected() {
        let p = tmp("invalid");
        let a = SharedStringArena::create(&p, 1024).unwrap();
        a.intern("hi").unwrap();  // used = 2
        let bad = StringRef { offset: 100, len: 5 };
        assert_eq!(a.get(bad).err(), Some(ArenaError::InvalidRef));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_interners_get_distinct_refs() {
        let p = tmp("concurrent");
        let a: Arc<SharedStringArena> = Arc::new(SharedStringArena::create(&p, 4096).unwrap());
        let n_threads = 4;
        let per_thread = 20;
        let mut handles = vec![];
        for t in 0..n_threads {
            let a = a.clone();
            handles.push(thread::spawn(move || {
                let mut refs = vec![];
                for i in 0..per_thread {
                    let s = format!("thread-{t}-msg-{i:03}");
                    let r = a.intern(&s).unwrap();
                    refs.push((s, r));
                }
                refs
            }));
        }
        let all: Vec<(String, StringRef)> = handles.into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        // Every interned string must read back to its original value.
        for (expected, r) in &all {
            let got = a.get(*r).unwrap();
            assert_eq!(got, expected,
                "ref offset={} len={} should resolve to {expected}",
                r.offset, r.len);
        }
        // No two refs overlap.
        let mut refs: Vec<StringRef> = all.iter().map(|(_, r)| *r).collect();
        refs.sort_by_key(|r| r.offset);
        for w in refs.windows(2) {
            let r1_end = w[0].offset + w[0].len;
            assert!(r1_end <= w[1].offset,
                "ref {:?} overlaps with ref {:?}", w[0], w[1]);
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn intern_and_get_helper_returns_both() {
        let p = tmp("intern-and-get");
        let a = SharedStringArena::create(&p, 1024).unwrap();
        let (r, s) = a.intern_and_get("composite").unwrap();
        assert_eq!(s, "composite");
        assert_eq!(a.get(r).unwrap(), "composite");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn utf8_validation_on_get() {
        let p = tmp("utf8");
        let a = SharedStringArena::create(&p, 128).unwrap();
        // Intern valid UTF-8.
        let r = a.intern("hello").unwrap();
        assert!(a.get(r).is_ok());
        // intern_bytes accepts arbitrary bytes; get() then rejects
        // non-UTF-8 with InvalidUtf8 while get_bytes returns the raw
        // bytes without validation.
        let r2 = a.intern_bytes(&[0xFF, 0xFE, 0xFD]).unwrap();
        assert_eq!(a.get(r2).err(), Some(ArenaError::InvalidUtf8));
        assert_eq!(a.get_bytes(r2).unwrap(), &[0xFF, 0xFE, 0xFD]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn clear_resets_used_bytes() {
        let p = tmp("clear");
        let a = SharedStringArena::create(&p, 128).unwrap();
        a.intern("first").unwrap();
        a.intern("second").unwrap();
        assert!(a.used_bytes() > 0);
        a.clear();
        assert_eq!(a.used_bytes(), 0);
        // Fresh interns work.
        let r = a.intern("after-clear").unwrap();
        assert_eq!(a.get(r).unwrap(), "after-clear");
        assert_eq!(r.offset, 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        let r_persist;
        {
            let a = SharedStringArena::create(&p, 1024).unwrap();
            r_persist = a.intern("persisted-string").unwrap();
            a.flush().unwrap();
        }
        let a2 = SharedStringArena::open(&p, 1024).unwrap();
        assert_eq!(a2.get(r_persist).unwrap(), "persisted-string");
        // And it can keep interning.
        let r2 = a2.intern("more-after-reopen").unwrap();
        assert_eq!(a2.get(r2).unwrap(), "more-after-reopen");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn deduplication_via_hashmap_composition() {
        // Demonstrate the dedup pattern: layer SharedHashMap<u64, u64>
        // (hash -> StringRef.to_u64) over the arena.
        use crate::SharedHashMap;
        use crate::shared_hash_map::fnv1a_64;

        let p_arena = tmp("dedup-arena");
        let p_index = tmp("dedup-index");
        let arena = SharedStringArena::create(&p_arena, 256).unwrap();
        let index: SharedHashMap<u64, u64> = SharedHashMap::create(&p_index, 32).unwrap();

        let s = "deduplicate-me";
        let h = fnv1a_64(s.as_bytes());

        // First intern: check index, miss, intern + insert into index.
        let r = if let Some(packed) = index.get(&h) {
            StringRef::from_u64(packed)
        } else {
            let r = arena.intern(s).unwrap();
            index.insert(h, r.to_u64()).unwrap();
            r
        };
        let used_after_first = arena.used_bytes();

        // Second intern of same string: hit in index, no arena append.
        let r2 = if let Some(packed) = index.get(&h) {
            StringRef::from_u64(packed)
        } else {
            let r = arena.intern(s).unwrap();
            index.insert(h, r.to_u64()).unwrap();
            r
        };
        assert_eq!(r, r2, "dedup should return the same ref");
        assert_eq!(arena.used_bytes(), used_after_first,
            "second intern should not consume more bytes");

        std::fs::remove_file(&p_arena).ok();
        std::fs::remove_file(&p_index).ok();
    }
}
