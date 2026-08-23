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

use memmap2::{Mmap, MmapMut, MmapOptions};

/// Format tag written last when a region is initialized, and required to
/// match on open. The trailing byte is the layout generation: `A1` carried
/// a `StringRef` packed as offset:u32|len:u32, `A2` packs it 40:24. A
/// region written by one generation resolves every ref wrongly under the
/// other, so the tag differs and the older region is refused rather than
/// misread.
pub const ARENA_MAGIC: u64 = 0x4150_5341_524E_4132;

/// The `A1` tag, retained so an old-format region is recognised and named
/// in the refusal instead of reported as unrecognised bytes.
pub const ARENA_MAGIC_V1: u64 = 0x4150_5341_524E_4131;

/// Bits of a packed [`StringRef`] given to the byte offset, and the
/// resulting ceiling on arena capacity.
pub const OFFSET_BITS: u32 = 40;
/// Bits given to the string length, and the resulting ceiling on one
/// interned string.
pub const LEN_BITS: u32 = 24;

const _: () = assert!(OFFSET_BITS + LEN_BITS == 64);

/// Largest addressable byte offset: 1 TiB - 1.
pub const MAX_OFFSET: u64 = (1u64 << OFFSET_BITS) - 1;
/// Largest interned string: 16 MiB - 1.
pub const MAX_LEN: u64 = (1u64 << LEN_BITS) - 1;

/// How this process mapped the file. `MmapMut` demands a read+write
/// file handle, which a consumer holding read access alone cannot get.
enum Mapping {
    Writable(MmapMut),
    ReadOnly(Mmap),
}

impl Mapping {
    #[inline]
    fn as_ptr(&self) -> *const u8 {
        match self {
            Mapping::Writable(m) => m.as_ptr(),
            Mapping::ReadOnly(m) => m.as_ptr(),
        }
    }

    #[inline]
    fn is_writable(&self) -> bool {
        matches!(self, Mapping::Writable(_))
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        match self {
            Mapping::Writable(m) => m.flush(),
            Mapping::ReadOnly(_) => Ok(()),
        }
    }

    fn flush_async(&self) -> Result<(), std::io::Error> {
        match self {
            Mapping::Writable(m) => m.flush_async(),
            Mapping::ReadOnly(_) => Ok(()),
        }
    }
}

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
    /// The arena was opened read-only and something tried to write it.
    ReadOnly,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for ArenaError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

/// Position-independent reference to a string in a SharedStringArena.
/// Encoded as a `u64` of [`OFFSET_BITS`] offset and [`LEN_BITS`] length,
/// for stable cross-process passing: the same u64 resolves to the same
/// bytes in every process that maps the arena.
///
/// The split gives a 1 TiB arena holding strings of up to 16 MiB each.
/// Both are enforced where a ref is minted, so a value that does not fit
/// is refused rather than truncated into a ref that reads someone else's
/// bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StringRef {
    pub offset: u64,
    pub len: u32,
}

impl StringRef {
    #[inline]
    pub fn to_u64(self) -> u64 {
        ((self.offset & MAX_OFFSET) << LEN_BITS) | (self.len as u64 & MAX_LEN)
    }
    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Self {
            offset: v >> LEN_BITS,
            len: (v & MAX_LEN) as u32,
        }
    }
}

pub struct SharedStringArena {
    _file: File,
    mmap: Mapping,
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
    /// Obtain the arena at `path`, initializing an empty one if the
    /// path does not yet exist and attaching to it if it does.
    /// Attaching leaves interned strings and `used_bytes` in place, so
    /// outstanding [`StringRef`]s stay resolvable; a region built with
    /// a different capacity is a `LayoutMismatch`.
    /// [`reset`](Self::reset) reinitializes.
    pub fn create(
        path: impl AsRef<Path>, capacity_bytes: usize,
    ) -> Result<Self, ArenaError> {
        Self::check_capacity(capacity_bytes)?;
        // An existing region carrying an older layout tag never satisfies
        // the readiness test, so attaching would spin to its deadline and
        // report that the creator never finished - which is not what
        // happened. Name the layout instead, before any of that.
        if Self::region_format_tag(path.as_ref()) == Some(ARENA_MAGIC_V1) {
            return Err(ArenaError::LayoutMismatch);
        }
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            arena_file_size(capacity_bytes),
            |ptr| unsafe { Self::init_region(ptr, capacity_bytes) },
            |ptr| unsafe { (*(ptr as *const ArenaHeader)).magic == ARENA_MAGIC },
        )?;
        let this = Self {
            _file: file, mmap: Mapping::Writable(mmap), capacity_bytes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        this.validate(capacity_bytes)?;
        Ok(this)
    }

    /// Truncate the arena at `path` and initialize an empty one,
    /// invalidating every StringRef live peers hold. For a caller that
    /// knows it owns the path.
    pub fn reset(
        path: impl AsRef<Path>, capacity_bytes: usize,
    ) -> Result<Self, ArenaError> {
        Self::check_capacity(capacity_bytes)?;
        let (file, mmap) = crate::mmf_attach::reset(
            path.as_ref(),
            arena_file_size(capacity_bytes),
            |ptr| unsafe { Self::init_region(ptr, capacity_bytes) },
        )?;
        Ok(Self {
            _file: file, mmap: Mapping::Writable(mmap), capacity_bytes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// The format tag an existing region carries, or `None` when the path
    /// does not exist or is too short to hold one. Reads the file rather
    /// than mapping it, so it says nothing about whether the region is
    /// otherwise usable.
    fn region_format_tag(path: &Path) -> Option<u64> {
        use std::io::Read;
        let mut f = File::open(path).ok()?;
        let mut tag = [0u8; 8];
        f.read_exact(&mut tag).ok()?;
        Some(u64::from_le_bytes(tag))
    }

    /// A capacity every constructor must agree on: at least one byte,
    /// and no larger than a [`StringRef`] offset can address. Reported
    /// rather than asserted, because a service that panics building its
    /// arena dies with it.
    fn check_capacity(capacity_bytes: usize) -> Result<(), ArenaError> {
        if capacity_bytes < 1 || capacity_bytes as u64 > MAX_OFFSET {
            return Err(ArenaError::LayoutMismatch);
        }
        Ok(())
    }

    /// Lay out an empty arena: capacity first, magic last, because
    /// attachers spin on it. The zeroed region is already `used_bytes`
    /// 0 and empty byte space.
    ///
    /// # Safety
    /// `ptr` addresses at least `arena_file_size(capacity_bytes)`
    /// writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, capacity_bytes: usize) {
        let hdr = ptr as *mut ArenaHeader;
        unsafe {
            (*hdr).capacity_bytes = capacity_bytes as u64;
            std::ptr::write_volatile(&raw mut (*hdr).magic, ARENA_MAGIC);
        }
    }

    pub fn open(
        path: impl AsRef<Path>, expected_capacity_bytes: usize,
    ) -> Result<Self, ArenaError> {
        // A capacity a StringRef offset cannot address is not a layout
        // this crate ever creates, and interning into it would truncate.
        if expected_capacity_bytes as u64 > MAX_OFFSET {
            return Err(ArenaError::LayoutMismatch);
        }
        let total = arena_file_size(expected_capacity_bytes);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(ArenaError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let this = Self {
            _file: file, mmap: Mapping::Writable(mmap),
            capacity_bytes: expected_capacity_bytes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        this.validate(expected_capacity_bytes)?;
        Ok(this)
    }

    /// Open an arena this process may only read.
    ///
    /// [`open`](Self::open) needs a read+write file handle, which a
    /// consumer of a privileged producer's arena does not have. Reads
    /// behave identically; [`intern`](Self::intern) and friends return
    /// [`ArenaError::ReadOnly`].
    pub fn open_read_only(
        path: impl AsRef<Path>, expected_capacity_bytes: usize,
    ) -> Result<Self, ArenaError> {
        let total = arena_file_size(expected_capacity_bytes);
        let file = OpenOptions::new().read(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(ArenaError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map(&file)? };
        let this = Self {
            _file: file, mmap: Mapping::ReadOnly(mmap),
            capacity_bytes: expected_capacity_bytes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        };
        this.validate(expected_capacity_bytes)?;
        Ok(this)
    }

    /// Whether the header on disk is the one this mapping expects.
    fn validate(&self, expected_capacity_bytes: usize) -> Result<(), ArenaError> {
        let hdr = self.header();
        if hdr.magic != ARENA_MAGIC || hdr.capacity_bytes != expected_capacity_bytes as u64 {
            return Err(ArenaError::LayoutMismatch);
        }
        Ok(())
    }

    /// Whether this mapping may be written.
    #[inline]
    pub fn is_writable(&self) -> bool {
        self.mmap.is_writable()
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
        if !self.mmap.is_writable() {
            return Err(ArenaError::ReadOnly);
        }
        let len = bytes.len() as u64;
        // A StringRef carries the length in LEN_BITS, so a longer string is
        // refused here rather than wrapped into a ref that resolves to a
        // prefix of itself and silently loses the tail.
        if len > MAX_LEN || len > self.capacity_bytes as u64 {
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
        // A StringRef carries the offset in OFFSET_BITS, so an offset past
        // that is refused here rather than truncated: a truncated offset
        // lands inside the used region and resolves to another string's
        // bytes, which every downstream bounds check would accept.
        // `create` and `reset` refuse such a capacity, so this covers an
        // arena opened at a capacity they never sanctioned.
        if offset.saturating_add(len) > MAX_OFFSET {
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
        Ok(StringRef { offset, len: len as u32 })
    }

    /// Resolve a StringRef to its `&[u8]`. Returns `Err(InvalidRef)`
    /// when the ref doesn't fall inside the arena's used region.
    pub fn get_bytes(&self, r: StringRef) -> Result<&[u8], ArenaError> {
        let end = r.offset.saturating_add(r.len as u64);
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
        if !self.mmap.is_writable() {
            return;
        }
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

    /// A second create attaches with interned strings in place; reset
    /// is what strips them.
    #[test]
    fn second_create_attaches_and_keeps_strings() {
        let p = tmp("attach");
        std::fs::remove_file(&p).ok();
        let a = SharedStringArena::create(&p, 4096).unwrap();
        let r = a.intern("held").unwrap();

        let a2 = SharedStringArena::create(&p, 4096).unwrap();
        assert_eq!(a2.get(r).unwrap(), "held", "attach lost an interned string");
        assert!(matches!(
            SharedStringArena::create(&p, 2048),
            Err(ArenaError::LayoutMismatch),
        ));

        // Windows refuses to truncate a mapped file, so every handle goes
        // before the reset.
        drop(a);
        drop(a2);
        let fresh = SharedStringArena::reset(&p, 4096).unwrap();
        assert_eq!(fresh.used_bytes(), 0, "reset kept interned bytes");
        drop(fresh);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn a_read_only_arena_resolves_refs_and_refuses_interning() {
        let p = tmp("readonly");
        let r = {
            let w = SharedStringArena::create(&p, 4096).unwrap();
            let r = w.intern("notepad.exe").unwrap();
            w.flush().unwrap();
            r
        };
        let ro = SharedStringArena::open_read_only(&p, 4096).unwrap();
        assert!(!ro.is_writable());
        assert_eq!(ro.get(r), Ok("notepad.exe"));
        assert_eq!(ro.get_bytes(r), Ok(&b"notepad.exe"[..]));
        assert_eq!(ro.used_bytes(), 11);
        assert_eq!(ro.intern("more"), Err(ArenaError::ReadOnly));
        assert_eq!(ro.intern_bytes(b"more"), Err(ArenaError::ReadOnly));
        ro.clear();
        assert_eq!(ro.used_bytes(), 11, "clear on a read-only arena is inert");
        ro.flush().unwrap();
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn a_read_only_open_still_validates_the_header() {
        let p = tmp("readonly-mismatch");
        {
            let w = SharedStringArena::create(&p, 4096).unwrap();
            w.intern("x").unwrap();
            w.flush().unwrap();
        }
        assert_eq!(
            SharedStringArena::open_read_only(&p, 2048).err(),
            Some(ArenaError::LayoutMismatch)
        );
        std::fs::remove_file(&p).ok();
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
            let r1_end = w[0].offset + w[0].len as u64;
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

    /// A StringRef addresses its bytes with an OFFSET_BITS offset, so a
    /// capacity past that is refused at construction rather than silently
    /// truncating every ref past the ceiling into another string's bytes.
    /// No file is touched: the check precedes the mapping.
    #[test]
    fn create_refuses_a_capacity_a_ref_cannot_address() {
        let past = MAX_OFFSET as usize + 1;
        assert!(matches!(
            SharedStringArena::create(tmp("too-big"), past),
            Err(ArenaError::LayoutMismatch),
        ));
        assert!(matches!(
            SharedStringArena::reset(tmp("too-big-reset"), past),
            Err(ArenaError::LayoutMismatch),
        ));
        assert!(matches!(
            SharedStringArena::create(tmp("zero-cap"), 0),
            Err(ArenaError::LayoutMismatch),
        ));
    }

    /// The same layout refused on the open path, where no assertion
    /// guards construction.
    #[test]
    fn open_refuses_a_capacity_a_ref_cannot_address() {
        assert!(matches!(
            SharedStringArena::open(tmp("open-too-big"), MAX_OFFSET as usize + 1),
            Err(ArenaError::LayoutMismatch),
        ));
    }

    /// The packing is what every process agrees on, so it has to round-trip
    /// exactly at the extremes of both fields - the places a shift or mask
    /// off by one bit shows up and nowhere else.
    #[test]
    fn a_string_ref_round_trips_at_both_field_ceilings() {
        for (offset, len) in [
            (0u64, 0u32),
            (MAX_OFFSET, MAX_LEN as u32),
            (MAX_OFFSET, 0),
            (0, MAX_LEN as u32),
            (1, 1),
            (MAX_OFFSET - 1, MAX_LEN as u32 - 1),
        ] {
            let r = StringRef { offset, len };
            let back = StringRef::from_u64(r.to_u64());
            assert_eq!(back, r, "offset {offset} len {len} did not round-trip");
        }
        // The two fields must not bleed into each other: a maximal length
        // leaves the offset untouched and the reverse.
        assert_eq!(StringRef { offset: 0, len: MAX_LEN as u32 }.to_u64(), MAX_LEN);
        assert_eq!(
            StringRef { offset: MAX_OFFSET, len: 0 }.to_u64(),
            MAX_OFFSET << LEN_BITS
        );
    }

    /// A region written under the previous layout resolves every ref
    /// wrongly under this one, so opening it must be refused rather than
    /// read as though the bits meant the same thing. The fixture is a real
    /// arena file with only its format tag rewritten, so what is refused is
    /// the layout and not some other corruption.
    #[test]
    fn an_old_format_region_is_refused() {
        let p = tmp("old-format");
        std::fs::remove_file(&p).ok();
        {
            let a = SharedStringArena::create(&p, 1024).unwrap();
            a.intern("written-under-the-new-layout").unwrap();
            a.flush().unwrap();
        }
        // Stamp the previous generation's tag over the header's magic.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&ARENA_MAGIC_V1.to_le_bytes()).unwrap();
            f.flush().unwrap();
        }
        assert!(
            matches!(
                SharedStringArena::open(&p, 1024),
                Err(ArenaError::LayoutMismatch)
            ),
            "an arena tagged with the previous layout must be refused"
        );
        // The obtain-on-create path must name the layout too, rather than
        // spinning to its deadline and blaming an absent creator.
        let started = std::time::Instant::now();
        assert!(
            matches!(
                SharedStringArena::create(&p, 1024),
                Err(ArenaError::LayoutMismatch)
            ),
            "create must refuse the previous layout, not attach to it"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "create should refuse immediately, not wait out the attach deadline"
        );
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
