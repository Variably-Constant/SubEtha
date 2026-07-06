//! `DirectFileRing`: non-mmap positioned-I/O ring that bypasses the OS
//! page cache (cross-platform).
//!
//! Where the substrate's other ring primitives use mmap to share memory
//! with peers, `DirectFileRing` opens a file in unbuffered mode and
//! reads/writes via positioned I/O. The page cache is bypassed: every
//! write goes directly to the underlying block device, every read comes
//! directly from the device. Useful when the substrate IS the buffer
//! (the caller does its own caching and does not want the kernel
//! double-buffering) - common in database storage engines.
//!
//! # Cross-platform mechanism
//!
//! The unbuffered-I/O surface differs per OS; only that surface is
//! gated, the ring layout and coordination are shared:
//!
//! - Unix: `O_DIRECT` open flag, `pwrite(2)` / `pread(2)`,
//!   `posix_memalign(3)` aligned buffers.
//! - Windows: `FILE_FLAG_NO_BUFFERING` + `FILE_FLAG_WRITE_THROUGH` open
//!   flags, `WriteFile` / `ReadFile` with an `OVERLAPPED` offset for
//!   positioned I/O, `VirtualAlloc` page-aligned buffers.
//!
//! # Alignment
//!
//! Both `O_DIRECT` and `FILE_FLAG_NO_BUFFERING` require that buffer
//! addresses, file offsets, and transfer lengths be aligned to the
//! device's logical block / sector size (typically 512 or 4096 bytes).
//! This primitive fixes the slot size at 4096 bytes and uses
//! page-aligned buffers, which satisfies both.
//!
//! # Coordination
//!
//! Head/tail counters live in a SEPARATE small MMF
//! ([`SharedAtomicU64`]) because writing them through the unbuffered
//! data path would defeat their purpose (atomic visibility across
//! processes). The data file holds payload slots only; the control
//! files hold head + tail. The data is device-resident (write-through /
//! O_DIRECT), so an independent reader process sees the producer's
//! writes once it observes the head counter advance.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::shared_atomic::SharedAtomicU64;

/// Fixed slot size matching the most common modern 4K-sector alignment.
/// All positioned reads/writes are exactly this size.
pub const DIRECT_FILE_SLOT_SIZE: usize = 4096;

/// Errors `DirectFileRing` operations can return.
#[derive(Debug)]
pub enum DirectFileError {
    Io(std::io::Error),
    LayoutMismatch,
    Empty,
    Full,
    PayloadTooLarge,
}

impl std::fmt::Display for DirectFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::LayoutMismatch => write!(f, "layout mismatch"),
            Self::Empty => write!(f, "ring is empty"),
            Self::Full => write!(f, "ring is full"),
            Self::PayloadTooLarge => write!(f, "payload too large"),
        }
    }
}

impl std::error::Error for DirectFileError {}

impl From<std::io::Error> for DirectFileError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}

/// A page-aligned heap buffer suitable for unbuffered (O_DIRECT /
/// FILE_FLAG_NO_BUFFERING) I/O. Allocated and freed via the platform's
/// aligned allocator.
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    #[cfg(unix)]
    fn new(len: usize) -> std::io::Result<Self> {
        assert_eq!(len % DIRECT_FILE_SLOT_SIZE, 0);
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut ptr, DIRECT_FILE_SLOT_SIZE, len) };
        if rc != 0 {
            return Err(std::io::Error::from_raw_os_error(rc));
        }
        unsafe { std::ptr::write_bytes(ptr as *mut u8, 0, len) };
        Ok(Self { ptr: ptr as *mut u8, len })
    }

    #[cfg(windows)]
    fn new(len: usize) -> std::io::Result<Self> {
        use windows_sys::Win32::System::Memory::{
            VirtualAlloc, MEM_COMMIT, MEM_RESERVE, PAGE_READWRITE,
        };
        assert_eq!(len % DIRECT_FILE_SLOT_SIZE, 0);
        // VirtualAlloc returns page-aligned (>= 4096) memory, zero-filled.
        let ptr = unsafe {
            VirtualAlloc(std::ptr::null(), len, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE)
        };
        if ptr.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { ptr: ptr as *mut u8, len })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuf {
    #[cfg(unix)]
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr as *mut libc::c_void) };
    }

    #[cfg(windows)]
    fn drop(&mut self) {
        use windows_sys::Win32::System::Memory::{VirtualFree, MEM_RELEASE};
        unsafe { VirtualFree(self.ptr as *mut core::ffi::c_void, 0, MEM_RELEASE) };
    }
}

/// Open `path` for unbuffered, page-cache-bypassing positioned I/O.
fn open_unbuffered(path: &Path, create: bool) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true);
    if create {
        opts.create(true).truncate(true);
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        // Linux / FreeBSD: O_DIRECT bypasses the page cache at open(2).
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_DIRECT);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH,
        };
        // NO_BUFFERING bypasses the cache (the O_DIRECT analogue);
        // WRITE_THROUGH forces each write to the device so an
        // independent reader process sees it.
        opts.custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH);
    }
    let file = opts.open(path)?;
    #[cfg(target_os = "macos")]
    {
        // macOS has no O_DIRECT open flag; `fcntl(fd, F_NOCACHE, 1)` is the
        // descriptor-level page-cache bypass, applied after open(2). It still
        // requires the page-aligned buffers the O_DIRECT path uses. Best-effort:
        // if it fails the descriptor stays cached, which is still correct.
        use std::os::unix::io::AsRawFd;
        // SAFETY: `file` owns a valid open descriptor for the duration of the call.
        unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
        }
    }
    Ok(file)
}

/// Non-mmap positioned-I/O ring with page-cache bypass.
pub struct DirectFileRing {
    data_file: std::fs::File,
    head: Arc<SharedAtomicU64>,
    tail: Arc<SharedAtomicU64>,
    capacity: usize,
    base_path: PathBuf,
}

unsafe impl Send for DirectFileRing {}
unsafe impl Sync for DirectFileRing {}

impl DirectFileRing {
    /// Construct a fresh ring at `base_path`. Creates three files:
    /// `{base}.directfile.data.bin` (the slot array, unbuffered),
    /// `{base}.directfile.head.bin` (head counter, MMF),
    /// `{base}.directfile.tail.bin` (tail counter, MMF).
    pub fn create(
        base_path: impl AsRef<Path>,
        capacity: usize,
    ) -> Result<Self, DirectFileError> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let base = base_path.as_ref().to_path_buf();
        let data_path = with_suffix(&base, ".directfile.data.bin");
        let head_path = with_suffix(&base, ".directfile.head.bin");
        let tail_path = with_suffix(&base, ".directfile.tail.bin");

        let data_file = open_unbuffered(&data_path, true)?;
        data_file.set_len((capacity * DIRECT_FILE_SLOT_SIZE) as u64)?;

        let head = Arc::new(SharedAtomicU64::create(&head_path, 0)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?);
        let tail = Arc::new(SharedAtomicU64::create(&tail_path, 0)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?);

        Ok(Self { data_file, head, tail, capacity, base_path: base })
    }

    /// Open an existing ring at `base_path` (a second process).
    pub fn open(
        base_path: impl AsRef<Path>,
        expected_capacity: usize,
    ) -> Result<Self, DirectFileError> {
        let base = base_path.as_ref().to_path_buf();
        let data_path = with_suffix(&base, ".directfile.data.bin");
        let head_path = with_suffix(&base, ".directfile.head.bin");
        let tail_path = with_suffix(&base, ".directfile.tail.bin");

        let data_file = open_unbuffered(&data_path, false)?;
        let actual_size = data_file.metadata()?.len();
        let expected_size = (expected_capacity * DIRECT_FILE_SLOT_SIZE) as u64;
        if actual_size < expected_size {
            return Err(DirectFileError::LayoutMismatch);
        }

        let head = Arc::new(SharedAtomicU64::open(&head_path)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?);
        let tail = Arc::new(SharedAtomicU64::open(&tail_path)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?);

        Ok(Self {
            data_file, head, tail,
            capacity: expected_capacity,
            base_path: base,
        })
    }

    /// Capacity in slots.
    pub fn capacity(&self) -> usize { self.capacity }

    /// Current head index.
    pub fn head(&self) -> u64 { self.head.load(Ordering::Acquire) }

    /// Current tail index.
    pub fn tail(&self) -> u64 { self.tail.load(Ordering::Acquire) }

    /// Push a payload: copy it into a page-aligned buffer and write it at
    /// the head's slot offset with the page cache bypassed. Returns
    /// Err(Full) when head - tail == capacity.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), DirectFileError> {
        if payload.len() > DIRECT_FILE_SLOT_SIZE {
            return Err(DirectFileError::PayloadTooLarge);
        }
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= self.capacity as u64 {
            return Err(DirectFileError::Full);
        }
        let slot_offset = ((head as usize) & (self.capacity - 1))
            * DIRECT_FILE_SLOT_SIZE;
        let mut buf = AlignedBuf::new(DIRECT_FILE_SLOT_SIZE)?;
        buf.as_mut_slice()[..payload.len()].copy_from_slice(payload);
        let n = pwrite_aligned(&self.data_file, buf.as_slice(), slot_offset)?;
        if n != DIRECT_FILE_SLOT_SIZE {
            return Err(DirectFileError::Io(std::io::Error::other(
                format!("partial write: {n} != {DIRECT_FILE_SLOT_SIZE}")
            )));
        }
        self.head.store(head + 1, Ordering::Release);
        Ok(())
    }

    /// Pop the oldest payload: read the tail's slot into a page-aligned
    /// buffer (page cache bypassed), copy the relevant bytes to `out`,
    /// and advance the tail.
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, DirectFileError> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail == head {
            return Err(DirectFileError::Empty);
        }
        let slot_offset = ((tail as usize) & (self.capacity - 1))
            * DIRECT_FILE_SLOT_SIZE;
        let mut buf = AlignedBuf::new(DIRECT_FILE_SLOT_SIZE)?;
        let n = pread_aligned(&self.data_file, buf.as_mut_slice(), slot_offset)?;
        if n != DIRECT_FILE_SLOT_SIZE {
            return Err(DirectFileError::Io(std::io::Error::other(
                format!("partial read: {n} != {DIRECT_FILE_SLOT_SIZE}")
            )));
        }
        let copy_len = out.len().min(DIRECT_FILE_SLOT_SIZE);
        out[..copy_len].copy_from_slice(&buf.as_slice()[..copy_len]);
        self.tail.store(tail + 1, Ordering::Release);
        Ok(copy_len)
    }
}

impl Drop for DirectFileRing {
    fn drop(&mut self) {
        let data_path = with_suffix(&self.base_path, ".directfile.data.bin");
        let head_path = with_suffix(&self.base_path, ".directfile.head.bin");
        let tail_path = with_suffix(&self.base_path, ".directfile.tail.bin");
        std::fs::remove_file(&data_path).ok();
        std::fs::remove_file(&head_path).ok();
        std::fs::remove_file(&tail_path).ok();
    }
}

fn with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

#[cfg(unix)]
fn pwrite_aligned(
    file: &std::fs::File,
    buf: &[u8],
    offset: usize,
) -> std::io::Result<usize> {
    use std::os::unix::io::AsRawFd;
    let n = unsafe {
        libc::pwrite(
            file.as_raw_fd(),
            buf.as_ptr() as *const libc::c_void,
            buf.len(),
            offset as libc::off_t,
        )
    };
    if n < 0 { Err(std::io::Error::last_os_error()) } else { Ok(n as usize) }
}

#[cfg(unix)]
fn pread_aligned(
    file: &std::fs::File,
    buf: &mut [u8],
    offset: usize,
) -> std::io::Result<usize> {
    use std::os::unix::io::AsRawFd;
    let n = unsafe {
        libc::pread(
            file.as_raw_fd(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
            offset as libc::off_t,
        )
    };
    if n < 0 { Err(std::io::Error::last_os_error()) } else { Ok(n as usize) }
}

#[cfg(windows)]
fn pwrite_aligned(
    file: &std::fs::File,
    buf: &[u8],
    offset: usize,
) -> std::io::Result<usize> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::WriteFile;
    use windows_sys::Win32::System::IO::OVERLAPPED;
    let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
    ov.Anonymous.Anonymous.Offset = (offset as u64 & 0xFFFF_FFFF) as u32;
    ov.Anonymous.Anonymous.OffsetHigh = ((offset as u64) >> 32) as u32;
    let mut written: u32 = 0;
    let ok = unsafe {
        WriteFile(
            file.as_raw_handle() as _,
            buf.as_ptr(),
            buf.len() as u32,
            &mut written,
            &mut ov,
        )
    };
    if ok == 0 { Err(std::io::Error::last_os_error()) } else { Ok(written as usize) }
}

#[cfg(windows)]
fn pread_aligned(
    file: &std::fs::File,
    buf: &mut [u8],
    offset: usize,
) -> std::io::Result<usize> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::ReadFile;
    use windows_sys::Win32::System::IO::OVERLAPPED;
    let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
    ov.Anonymous.Anonymous.Offset = (offset as u64 & 0xFFFF_FFFF) as u32;
    ov.Anonymous.Anonymous.OffsetHigh = ((offset as u64) >> 32) as u32;
    let mut read: u32 = 0;
    let ok = unsafe {
        ReadFile(
            file.as_raw_handle() as _,
            buf.as_mut_ptr(),
            buf.len() as u32,
            &mut read,
            &mut ov,
        )
    };
    if ok == 0 { Err(std::io::Error::last_os_error()) } else { Ok(read as usize) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("dfring_{pid}_{nonce}_{name}"));
        p
    }

    #[test]
    fn create_then_push_pop_round_trip() {
        let path = tmp("rt");
        let ring = DirectFileRing::create(&path, 4).expect("create");
        let payload = b"hello unbuffered world";
        ring.try_push(payload).expect("push");
        let mut out = [0u8; DIRECT_FILE_SLOT_SIZE];
        let n = ring.try_pop(&mut out).expect("pop");
        assert_eq!(n, DIRECT_FILE_SLOT_SIZE);
        assert_eq!(&out[..payload.len()], payload);
    }

    #[test]
    fn fills_to_capacity_then_full() {
        let path = tmp("fills");
        let ring = DirectFileRing::create(&path, 4).expect("create");
        for i in 0u8..4 {
            ring.try_push(&[i; 16]).expect("push within cap");
        }
        assert!(matches!(
            ring.try_push(&[0u8; 16]),
            Err(DirectFileError::Full)
        ));
    }

    #[test]
    fn payload_too_large_rejected() {
        let path = tmp("oversize");
        let ring = DirectFileRing::create(&path, 4).expect("create");
        let big = vec![0u8; DIRECT_FILE_SLOT_SIZE + 1];
        assert!(matches!(
            ring.try_push(&big),
            Err(DirectFileError::PayloadTooLarge)
        ));
    }

    /// Interleaved push/pop of many items, each round-trip verified in
    /// order - exercises the wrap and the positioned-I/O path repeatedly.
    #[test]
    fn many_items_round_trip_in_order() {
        let path = tmp("many");
        let ring = DirectFileRing::create(&path, 8).expect("create");
        let mut buf = [0u8; DIRECT_FILE_SLOT_SIZE];
        for i in 0u64..500 {
            ring.try_push(&i.to_le_bytes()).expect("push");
            ring.try_pop(&mut buf).expect("pop");
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            assert_eq!(v, i, "in-order round trip at {i}");
        }
    }
}
