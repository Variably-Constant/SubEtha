//! `hugepages`: Linux-only hugepage-backed mmap helper.
//!
//! Allocates an anonymous mmap region backed by 2MB hugepages
//! (MAP_HUGETLB | MAP_HUGE_2MB). Useful for large rings where
//! TLB pressure measurably hurts: a 16MB region fits in 8
//! hugepages vs 4096 4KB pages.
//!
//! Falls back gracefully (returns Err) if the kernel does not have
//! hugepages reserved; callers handle the fallback by switching to
//! standard 4KB anon mmap.

#![cfg(target_os = "linux")]

use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr;

/// 2MB hugepage size in bytes.
pub const HUGEPAGE_2MB: usize = 2 * 1024 * 1024;

/// 1GB hugepage size in bytes.
pub const HUGEPAGE_1GB: usize = 1024 * 1024 * 1024;

/// Which hugepage size to request from the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HugepageSize {
    /// 2 MB hugepages (most widely available).
    Mb2,
    /// 1 GB hugepages (require CONFIG_HUGETLBFS + reserved
    /// gigabyte pages at boot).
    Gb1,
}

impl HugepageSize {
    fn bytes(self) -> usize {
        match self {
            Self::Mb2 => HUGEPAGE_2MB,
            Self::Gb1 => HUGEPAGE_1GB,
        }
    }

    fn mmap_flag(self) -> libc::c_int {
        match self {
            // MAP_HUGE_2MB = 21 << MAP_HUGE_SHIFT
            // MAP_HUGE_1GB = 30 << MAP_HUGE_SHIFT
            // MAP_HUGE_SHIFT = 26
            Self::Mb2 => libc::MAP_HUGETLB | (21 << 26),
            Self::Gb1 => libc::MAP_HUGETLB | (30 << 26),
        }
    }
}

/// A hugepage-backed anonymous mmap region.
pub struct HugepageRegion {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for HugepageRegion {}
unsafe impl Sync for HugepageRegion {}

impl HugepageRegion {
    /// Allocate `pages` hugepages of the requested size.
    pub fn allocate(pages: usize, size: HugepageSize) -> io::Result<Self> {
        assert!(pages > 0);
        let len = pages * size.bytes();
        let prot = libc::PROT_READ | libc::PROT_WRITE;
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | size.mmap_flag();
        let raw = unsafe { libc::mmap(ptr::null_mut(), len, prot, flags, -1, 0) };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { ptr: raw as *mut u8, len })
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn len(&self) -> usize { self.len }
    pub fn is_empty(&self) -> bool { self.len == 0 }
}

impl Drop for HugepageRegion {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
    }
}

impl crate::spsc_ring::RegionOwner for HugepageRegion {
    fn region_ptr(&mut self) -> *mut u8 {
        self.as_mut_slice().as_mut_ptr()
    }
    fn region_len(&self) -> usize {
        self.len()
    }
}

/// A CROSS-PROCESS hugepage-backed region: a file on a `hugetlbfs` mount,
/// mmap'd `MAP_SHARED`. Unrelated processes open the same path and mmap it,
/// so a ring laid out in the region is shared through hugepage physical
/// memory. This is the Linux analogue of the Windows large-page
/// `LargePageSection` (named, openable by a second process); the
/// anonymous [`HugepageRegion`] above is in-process / fork-shared only.
///
/// The file's mmap is automatically hugepage-backed because it lives on a
/// `hugetlbfs` filesystem (mount one with `mount -t hugetlbfs nodev
/// <dir>`); the length must be a multiple of the mount's hugepage size.
pub struct SharedHugepageRegion {
    _file: std::fs::File,
    ptr: *mut u8,
    len: usize,
    path: PathBuf,
    owner: bool,
}

unsafe impl Send for SharedHugepageRegion {}
unsafe impl Sync for SharedHugepageRegion {}

impl SharedHugepageRegion {
    fn map(file: &std::fs::File, len: usize) -> io::Result<*mut u8> {
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            Err(io::Error::last_os_error())
        } else {
            Ok(raw as *mut u8)
        }
    }

    /// Create (or truncate) a hugetlbfs file of `pages` hugepages and map it.
    /// `path` must be on a hugetlbfs mount.
    pub fn create(
        path: impl AsRef<Path>,
        pages: usize,
        size: HugepageSize,
    ) -> io::Result<Self> {
        assert!(pages > 0);
        let path = path.as_ref().to_path_buf();
        let len = pages * size.bytes();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        // hugetlbfs requires the length be a multiple of its hugepage size.
        file.set_len(len as u64)?;
        let ptr = Self::map(&file, len)?;
        Ok(Self { _file: file, ptr, len, path, owner: true })
    }

    /// Open an existing hugetlbfs region (a second process) and map it.
    pub fn open(
        path: impl AsRef<Path>,
        pages: usize,
        size: HugepageSize,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let len = pages * size.bytes();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let ptr = Self::map(&file, len)?;
        Ok(Self { _file: file, ptr, len, path, owner: false })
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn len(&self) -> usize { self.len }
    pub fn is_empty(&self) -> bool { self.len == 0 }
}

impl Drop for SharedHugepageRegion {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
        // The creator unlinks the backing file; an opener leaves it.
        if self.owner {
            std::fs::remove_file(&self.path).ok();
        }
    }
}

impl crate::spsc_ring::RegionOwner for SharedHugepageRegion {
    fn region_ptr(&mut self) -> *mut u8 {
        self.as_mut_slice().as_mut_ptr()
    }
    fn region_len(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default hugetlbfs mount on most distros. The test skips
    /// cleanly when it is absent / unwritable / has no reserved pages,
    /// so it is safe to run on any host; on a host with hugepages it
    /// proves two independent maps of the same hugetlbfs file see each
    /// other's writes (the cross-process sharing mechanism, exercised
    /// in-process).
    fn hugetlbfs_dir() -> Option<PathBuf> {
        let candidate = Path::new("/dev/hugepages");
        // A cheap writability probe: try to create + remove a temp file.
        let probe = candidate.join(format!("subetha_probe_{}", std::process::id()));
        match std::fs::File::create(&probe) {
            Ok(_) => {
                std::fs::remove_file(&probe).ok();
                Some(candidate.to_path_buf())
            }
            Err(_) => None,
        }
    }

    #[test]
    fn shared_hugepage_region_two_maps_share_memory() {
        let Some(dir) = hugetlbfs_dir() else {
            eprintln!("skipping: no writable hugetlbfs mount at /dev/hugepages");
            return;
        };
        let path = dir.join(format!("subetha_shr_{}", std::process::id()));

        // Creating may still fail if no pages are reserved; skip if so.
        let mut a = match SharedHugepageRegion::create(&path, 1, HugepageSize::Mb2) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skipping: hugetlbfs create failed ({e}); reserve pages");
                return;
            }
        };
        assert_eq!(a.len(), HUGEPAGE_2MB);

        // A second independent map of the SAME file.
        let mut b = SharedHugepageRegion::open(&path, 1, HugepageSize::Mb2)
            .expect("open second map of the same hugetlbfs file");

        // Write through A, read through B: same physical hugepage.
        a.as_mut_slice()[0] = 0xAB;
        a.as_mut_slice()[HUGEPAGE_2MB - 1] = 0xCD;
        assert_eq!(b.as_slice()[0], 0xAB, "B must see A's write at offset 0");
        assert_eq!(
            b.as_slice()[HUGEPAGE_2MB - 1], 0xCD,
            "B must see A's write at the last byte",
        );

        // And the reverse direction.
        b.as_mut_slice()[42] = 0x7E;
        assert_eq!(a.as_slice()[42], 0x7E, "A must see B's write");
    }
}
