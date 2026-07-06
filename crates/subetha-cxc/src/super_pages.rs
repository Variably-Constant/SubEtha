//! `super_pages`: superpage-backed mmap helper for FreeBSD and macOS.
//!
//! Allocates an anonymous mmap region backed by 2 MB superpages, aligned
//! to the superpage boundary, behind one [`SuperPageRegion`] type. Only
//! the allocation syscall is platform-specific; the
//! [`RegionOwner`](crate::spsc_ring::RegionOwner) it exposes is shared:
//!
//! - **FreeBSD** (`MAP_ALIGNED_SUPER`): a best-effort hint over
//!   transparent superpages (`vm.pmap.pg_ps_enabled`) - no pre-reserved
//!   pool - so the kernel degrades to base pages rather than failing when
//!   superpages are momentarily scarce, and promotes to a superpage once
//!   the mapping is fully populated and aligned.
//! - **macOS x86_64** (`VM_FLAGS_SUPERPAGE_SIZE_2MB`): the superpage size
//!   rides in `mmap`'s `fd` slot (the Darwin anonymous-superpage
//!   overload), giving a wired, 2 MB-aligned, 2 MB-paged region. Apple
//!   Silicon has no userspace superpage API, so the request returns
//!   `Unsupported` there and the caller falls back to base pages.
//!
//! Returns `Err` when the kernel cannot satisfy the mapping, so callers
//! fall back to a standard anon mmap. Mirrors the Linux
//! [`HugepageRegion`](crate::hugepages::HugepageRegion) and Windows
//! [`LargePageRegion`](crate::large_pages::LargePageRegion).

#![cfg(any(target_os = "freebsd", target_os = "macos"))]

use std::io;

/// Superpage size (2 MB) on amd64 / arm64 FreeBSD and x86_64 macOS.
pub const SUPERPAGE_2MB: usize = 2 * 1024 * 1024;

/// macOS `VM_FLAGS_SUPERPAGE_SIZE_2MB` (mach/vm_statistics.h):
/// `SUPERPAGE_SIZE_2MB (2) << VM_FLAGS_SUPERPAGE_SHIFT (16)`. Passed in
/// `mmap`'s `fd` argument - the Darwin anonymous-superpage overload.
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
const VM_FLAGS_SUPERPAGE_SIZE_2MB: libc::c_int = 2 << 16;

/// A superpage-backed anonymous mmap region.
pub struct SuperPageRegion {
    ptr: *mut u8,
    len: usize,
}

// The region is a plain mmap span owned solely by this handle; sending it
// across threads only moves the pointer + length, exactly as for the
// Linux/Windows region owners.
unsafe impl Send for SuperPageRegion {}
unsafe impl Sync for SuperPageRegion {}

impl SuperPageRegion {
    /// Allocate at least `bytes`, rounded up to a whole number of 2 MB
    /// superpages, requesting superpage backing (FreeBSD
    /// `MAP_ALIGNED_SUPER`, macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`).
    pub fn allocate(bytes: usize) -> io::Result<Self> {
        let len = bytes.div_ceil(SUPERPAGE_2MB).max(1) * SUPERPAGE_2MB;
        let raw = Self::map_superpages(len)?;
        Ok(Self { ptr: raw as *mut u8, len })
    }

    /// FreeBSD: best-effort superpage hint over transparent superpages.
    #[cfg(target_os = "freebsd")]
    fn map_superpages(len: usize) -> io::Result<*mut libc::c_void> {
        let prot = libc::PROT_READ | libc::PROT_WRITE;
        let flags = libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_ALIGNED_SUPER;
        let raw = unsafe { libc::mmap(std::ptr::null_mut(), len, prot, flags, -1, 0) };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(raw)
    }

    /// macOS x86_64: the 2 MB superpage size rides in mmap's `fd` slot.
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    fn map_superpages(len: usize) -> io::Result<*mut libc::c_void> {
        let prot = libc::PROT_READ | libc::PROT_WRITE;
        let flags = libc::MAP_PRIVATE | libc::MAP_ANON;
        let raw = unsafe {
            libc::mmap(std::ptr::null_mut(), len, prot, flags, VM_FLAGS_SUPERPAGE_SIZE_2MB, 0)
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(raw)
    }

    /// Apple Silicon has no userspace superpage API.
    #[cfg(all(target_os = "macos", not(target_arch = "x86_64")))]
    fn map_superpages(_len: usize) -> io::Result<*mut libc::c_void> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "macOS superpages require x86_64; Apple Silicon has no userspace superpage API",
        ))
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

impl Drop for SuperPageRegion {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
    }
}

impl crate::spsc_ring::RegionOwner for SuperPageRegion {
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
    use crate::spsc_ring::RegionOwner;

    /// A superpage region is 2 MB-aligned (the superpage-backing contract:
    /// `MAP_ALIGNED_SUPER` on FreeBSD, `VM_FLAGS_SUPERPAGE_SIZE_2MB` on
    /// macOS) and read/write-back coherent across its full extent.
    #[test]
    fn superpage_region_is_aligned_and_writable() {
        let mut r = match SuperPageRegion::allocate(SUPERPAGE_2MB) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skipping: superpage mmap unavailable ({e})");
                return;
            }
        };
        assert_eq!(
            r.region_ptr() as usize % SUPERPAGE_2MB,
            0,
            "superpage backing must align the base to the superpage size"
        );
        assert!(r.region_len() >= SUPERPAGE_2MB);
        let s = r.as_mut_slice();
        s[0] = 0xAB;
        s[SUPERPAGE_2MB - 1] = 0xCD;
        assert_eq!(r.as_slice()[0], 0xAB);
        assert_eq!(r.as_slice()[SUPERPAGE_2MB - 1], 0xCD);
    }
}
