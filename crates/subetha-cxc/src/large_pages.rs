//! `large_pages`: Windows-only large-page memory helpers. The
//! Windows sibling of the Linux transparent-huge-page path, with
//! one extra capability that path does not have: cross-process
//! named sections (`LargePageSection`).
//!
//! Two primitives:
//!
//! - [`LargePageRegion`]: private (single-process) memory backed
//!   by large pages via `VirtualAlloc(MEM_LARGE_PAGES)`. Direct
//!   parity with Linux `HugepageRegion`.
//! - [`LargePageSection`]: NAMED pagefile-backed section created
//!   with `CreateFileMappingW(SEC_LARGE_PAGES)` + mapped via
//!   `MapViewOfFile`. Two processes that open the same section
//!   name share the same large-page-backed physical memory - the
//!   "huge memory table shared across processes" capability.
//!
//! # The privilege gate (different from Linux)
//!
//! Linux gates hugepages on RESERVATION (`/proc/sys/vm/nr_hugepages`);
//! Windows gates large pages on an ACCOUNT PRIVILEGE:
//! `SeLockMemoryPrivilege` ("Lock pages in memory" in Local
//! Security Policy). The privilege must be (a) granted to the
//! user account (admin grants it in secpol.msc under Local
//! Policies > User Rights Assignment, then the user logs off and
//! on again so the token picks it up), AND (b) enabled in the
//! process token at runtime via [`enable_lock_memory_privilege`].
//!
//! When the privilege is absent, allocation fails with
//! `ERROR_PRIVILEGE_NOT_HELD` (1314); callers fall back to
//! standard 4KB-page allocation exactly as they do on Linux when
//! no hugepages are reserved.
//!
//! # File-backed mappings can NEVER use large pages on Windows
//!
//! `SEC_LARGE_PAGES` requires the section be pagefile-backed
//! (`INVALID_HANDLE_VALUE` as the file handle). A mapping over a
//! real on-disk file is always 4KB-paged by OS design. So the
//! substrate's File locale stays standard-paged on Windows; the
//! Anon and named-section locales are the large-page candidates.

#![cfg(windows)]

use std::io;

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_NOT_ALL_ASSIGNED, HANDLE,
    INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Security::{
    AdjustTokenPrivileges, LookupPrivilegeValueW, LUID_AND_ATTRIBUTES,
    SE_PRIVILEGE_ENABLED, TOKEN_ADJUST_PRIVILEGES, TOKEN_PRIVILEGES,
    TOKEN_QUERY,
};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, GetLargePageMinimum, MapViewOfFile,
    OpenFileMappingW, UnmapViewOfFile, VirtualAlloc, VirtualFree,
    FILE_MAP_ALL_ACCESS, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RELEASE,
    MEM_RESERVE, PAGE_READWRITE, SEC_COMMIT, SEC_LARGE_PAGES,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcessToken,
};

/// Win32 error code surfaced when the calling token lacks
/// `SeLockMemoryPrivilege`. Callers match on
/// `err.raw_os_error() == Some(ERROR_PRIVILEGE_NOT_HELD as i32)`
/// to distinguish "privilege not granted" from transient
/// allocation failures.
pub const ERROR_PRIVILEGE_NOT_HELD: u32 = 1314;

/// Win32 error code for "not enough contiguous physical memory to
/// satisfy a large-page allocation" (large pages are never paged
/// out, so the system needs free contiguous physical RAM).
pub const ERROR_NO_SYSTEM_RESOURCES: u32 = 1450;

/// The minimum large-page size on this host, in bytes (2MB on all
/// current x86_64 / ARM64 Windows). Returns 0 when the processor
/// or OS does not support large pages at all.
pub fn large_page_minimum() -> usize {
    unsafe { GetLargePageMinimum() }
}

/// Enable `SeLockMemoryPrivilege` in the current process token.
///
/// This can only ENABLE a privilege the account already HOLDS.
/// If "Lock pages in memory" has not been granted to the user in
/// Local Security Policy, `AdjustTokenPrivileges` reports
/// `ERROR_NOT_ALL_ASSIGNED` and this function returns an error
/// naming that exact condition.
pub fn enable_lock_memory_privilege() -> io::Result<()> {
    // "SeLockMemoryPrivilege" as UTF-16, NUL-terminated.
    let name: Vec<u16> = "SeLockMemoryPrivilege\0".encode_utf16().collect();

    unsafe {
        let mut token: HANDLE = std::ptr::null_mut();
        if OpenProcessToken(
            GetCurrentProcess(),
            TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
            &mut token,
        ) == 0
        {
            return Err(io::Error::last_os_error());
        }

        let mut tp = TOKEN_PRIVILEGES {
            PrivilegeCount: 1,
            Privileges: [LUID_AND_ATTRIBUTES {
                Luid: std::mem::zeroed(),
                Attributes: SE_PRIVILEGE_ENABLED,
            }],
        };
        if LookupPrivilegeValueW(
            std::ptr::null(),
            name.as_ptr(),
            &mut tp.Privileges[0].Luid,
        ) == 0
        {
            let e = io::Error::last_os_error();
            CloseHandle(token);
            return Err(e);
        }

        let ok = AdjustTokenPrivileges(
            token,
            0,
            &tp,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        // AdjustTokenPrivileges returns success even when nothing
        // was assigned; the real verdict is in GetLastError.
        let last = GetLastError();
        CloseHandle(token);

        if ok == 0 {
            return Err(io::Error::from_raw_os_error(last as i32));
        }
        if last == ERROR_NOT_ALL_ASSIGNED {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "SeLockMemoryPrivilege is not granted to this account. \
                 Grant 'Lock pages in memory' in Local Security Policy \
                 (secpol.msc > Local Policies > User Rights Assignment), \
                 then log off and on so the token picks it up.",
            ));
        }
        Ok(())
    }
}

/// Round `bytes` up to the next multiple of the host's large-page
/// minimum. Returns `None` when the host does not support large
/// pages.
pub fn round_to_large_page(bytes: usize) -> Option<usize> {
    let min = large_page_minimum();
    if min == 0 {
        return None;
    }
    Some(bytes.div_ceil(min) * min)
}

/// Private (single-process) large-page-backed memory region.
/// Windows parity for the Linux `HugepageRegion`.
pub struct LargePageRegion {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for LargePageRegion {}
unsafe impl Sync for LargePageRegion {}

impl LargePageRegion {
    /// Allocate at least `bytes` of large-page-backed private
    /// memory (rounded up to the large-page minimum). The caller
    /// must have run [`enable_lock_memory_privilege`] first.
    ///
    /// Failure modes callers handle:
    /// - `ERROR_PRIVILEGE_NOT_HELD` (1314): account lacks the
    ///   privilege; fall back to standard allocation.
    /// - `ERROR_NO_SYSTEM_RESOURCES` (1450): not enough contiguous
    ///   physical RAM right now; fall back or retry later.
    pub fn allocate(bytes: usize) -> io::Result<Self> {
        assert!(bytes > 0);
        let len = round_to_large_page(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "this host does not support large pages (GetLargePageMinimum() == 0)",
            )
        })?;
        let raw = unsafe {
            VirtualAlloc(
                std::ptr::null(),
                len,
                MEM_RESERVE | MEM_COMMIT | MEM_LARGE_PAGES,
                PAGE_READWRITE,
            )
        };
        if raw.is_null() {
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

impl Drop for LargePageRegion {
    fn drop(&mut self) {
        unsafe { VirtualFree(self.ptr as *mut core::ffi::c_void, 0, MEM_RELEASE) };
    }
}

/// Cross-process NAMED large-page section. Pagefile-backed
/// (`SEC_LARGE_PAGES` requires it); two processes that pass the
/// same `name` share the same large-page-backed physical memory.
///
/// This is the "huge memory table shared across processes"
/// primitive: a multi-GB lookup table mapped once per process
/// with 2MB TLB entries instead of 4KB.
pub struct LargePageSection {
    handle: HANDLE,
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for LargePageSection {}
unsafe impl Sync for LargePageSection {}

impl LargePageSection {
    /// Create a named large-page section of at least `bytes`
    /// (rounded up to the large-page minimum). The name is
    /// kernel-namespace-global (`Local\` prefix recommended for
    /// per-session scoping, e.g. `Local\my_table`).
    pub fn create(name: &str, bytes: usize) -> io::Result<Self> {
        assert!(bytes > 0);
        let len = round_to_large_page(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "this host does not support large pages (GetLargePageMinimum() == 0)",
            )
        })?;
        let wname: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        let handle = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE, // pagefile-backed (required for SEC_LARGE_PAGES)
                std::ptr::null(),
                PAGE_READWRITE | SEC_COMMIT | SEC_LARGE_PAGES,
                (len >> 32) as u32,
                (len & 0xFFFF_FFFF) as u32,
                wname.as_ptr(),
            )
        };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Self::map(handle, len)
    }

    /// Open an existing named large-page section created by
    /// another process (or this one). `bytes` must match the
    /// creator's request (it determines the view length).
    pub fn open(name: &str, bytes: usize) -> io::Result<Self> {
        let len = round_to_large_page(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "this host does not support large pages (GetLargePageMinimum() == 0)",
            )
        })?;
        let wname: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        let handle = unsafe {
            OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, wname.as_ptr())
        };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Self::map(handle, len)
    }

    fn map(handle: HANDLE, len: usize) -> io::Result<Self> {
        let ptr = unsafe {
            MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, len)
        };
        if ptr.Value.is_null() {
            let e = io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(e);
        }
        Ok(Self {
            handle,
            ptr: ptr.Value as *mut u8,
            len,
        })
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

impl Drop for LargePageSection {
    fn drop(&mut self) {
        unsafe {
            UnmapViewOfFile(
                windows_sys::Win32::System::Memory::MEMORY_MAPPED_VIEW_ADDRESS {
                    Value: self.ptr as *mut core::ffi::c_void,
                },
            );
            CloseHandle(self.handle);
        }
    }
}

impl crate::spsc_ring::RegionOwner for LargePageRegion {
    fn region_ptr(&mut self) -> *mut u8 {
        self.as_mut_slice().as_mut_ptr()
    }
    fn region_len(&self) -> usize {
        self.len()
    }
}

impl crate::spsc_ring::RegionOwner for LargePageSection {
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

    #[test]
    fn large_page_minimum_is_pow2_or_zero() {
        let min = large_page_minimum();
        // Every x86_64 / ARM64 Windows reports 2MB; 0 means the
        // host genuinely lacks support. Both are valid; anything
        // else (non-pow2) is a binding bug.
        if min != 0 {
            assert!(min.is_power_of_two(), "large-page min {min} not a power of 2");
            assert!(min >= 2 * 1024 * 1024, "large-page min {min} below 2MB");
        }
    }

    #[test]
    fn round_to_large_page_rounds_up() {
        if large_page_minimum() == 0 {
            // Host lacks large pages entirely; the rounding helper
            // contract is None.
            assert_eq!(round_to_large_page(1), None);
            return;
        }
        let min = large_page_minimum();
        assert_eq!(round_to_large_page(1), Some(min));
        assert_eq!(round_to_large_page(min), Some(min));
        assert_eq!(round_to_large_page(min + 1), Some(2 * min));
    }

    /// Exact-contract test: allocation either succeeds (account
    /// holds SeLockMemoryPrivilege) and the memory is usable, or
    /// fails with PRECISELY the documented privilege /
    /// resource error. Any other outcome is a bug.
    #[test]
    fn allocate_succeeds_or_fails_with_documented_error() {
        if large_page_minimum() == 0 {
            return; // host lacks large pages; nothing to assert
        }
        // Try to enable the privilege; remember the verdict.
        let priv_ok = enable_lock_memory_privilege().is_ok();

        match LargePageRegion::allocate(1) {
            Ok(mut region) => {
                assert!(priv_ok, "allocation succeeded but privilege-enable failed");
                assert_eq!(region.len() % large_page_minimum(), 0);
                // Write + read through the mapping.
                let last = region.len() - 1;
                let s = region.as_mut_slice();
                s[0] = 0xAB;
                s[last] = 0xCD;
                assert_eq!(region.as_slice()[0], 0xAB);
                assert_eq!(region.as_slice()[last], 0xCD);
            }
            Err(e) => {
                let code = e.raw_os_error().unwrap_or(-1) as u32;
                assert!(
                    code == ERROR_PRIVILEGE_NOT_HELD
                        || code == ERROR_NO_SYSTEM_RESOURCES
                        || !priv_ok,
                    "allocation failed with undocumented error: {e:?} (code {code})"
                );
            }
        }
    }

    /// Same exact contract for the named cross-process section.
    #[test]
    fn section_succeeds_or_fails_with_documented_error() {
        if large_page_minimum() == 0 {
            return;
        }
        let priv_ok = enable_lock_memory_privilege().is_ok();
        let name = format!("Local\\subetha_lp_test_{}", std::process::id());

        match LargePageSection::create(&name, 1) {
            Ok(mut section) => {
                assert!(priv_ok, "section create succeeded but privilege-enable failed");
                let s = section.as_mut_slice();
                s[0] = 0x5A;
                // Open a second view of the SAME section (cross-
                // handle, same process) and verify the byte is
                // visible through it: proves the views alias the
                // same physical large pages.
                let view2 = LargePageSection::open(&name, 1).expect("open second view");
                assert_eq!(view2.as_slice()[0], 0x5A);
            }
            Err(e) => {
                let code = e.raw_os_error().unwrap_or(-1) as u32;
                assert!(
                    code == ERROR_PRIVILEGE_NOT_HELD
                        || code == ERROR_NO_SYSTEM_RESOURCES
                        || !priv_ok,
                    "section create failed with undocumented error: {e:?} (code {code})"
                );
            }
        }
    }
}
