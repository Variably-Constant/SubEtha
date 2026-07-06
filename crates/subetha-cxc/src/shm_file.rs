//! `ShmFile`: cross-platform RAM-resident named shared-memory backing.
//!
//! Wraps the platform's named-shared-memory primitive so the rest of
//! the substrate can treat ShmFs the same way it treats anon and
//! file backings: hand it to a ring constructor, get a `&mut [u8]`
//! into the shared region, build a ring on top.
//!
//! - **Unix** (Linux + macOS): `shm_open(2)` + `ftruncate(2)` +
//!   memmap2 via `File::from_raw_fd`. On Drop: the inner `File`
//!   closes the fd; `shm_unlink(2)` removes the name so a later
//!   create with the same name starts fresh.
//! - **Windows**: `CreateFileMappingW(INVALID_HANDLE_VALUE, ...)`
//!   for page-file-backed shared memory + `MapViewOfFile` to get
//!   the mapped pointer. On Drop: `UnmapViewOfFile` + `CloseHandle`.
//!   Windows refcounts handles; the named object goes away on last
//!   handle close.
//!
//! Naming convention: a caller-supplied logical name is prefixed
//! with `/subetha_` on Unix (shm_open requires names starting with
//! `/`) and `Local\\subetha_` on Windows (per-session visibility).
//! Embedded slashes in the caller's name become underscores so the
//! whole logical name is one path component.

use std::io;

#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::os::unix::io::FromRawFd;

#[cfg(unix)]
use memmap2::{MmapMut, MmapOptions};

/// Cross-platform RAM-resident named shared-memory backing.
///
/// Two handles created with the same logical name map onto the same
/// underlying memory region. This is the cross-process visibility
/// property that makes this distinct from `MmapOptions::map_anon`.
pub struct ShmFile {
    /// Logical name (used for cleanup bookkeeping).
    name: String,
    /// Size of the mapped region in bytes.
    len: usize,
    #[cfg(unix)]
    mmap: MmapMut,
    #[cfg(unix)]
    _file: File,
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(windows)]
    view: *mut core::ffi::c_void,
}

unsafe impl Send for ShmFile {}
unsafe impl Sync for ShmFile {}

impl ShmFile {
    /// Create or open a named RAM-resident shared-memory region of
    /// `size` bytes. Two handles created with the same logical name
    /// map onto the same underlying memory.
    pub fn create_or_open_named(
        logical_name: &str,
        size: usize,
    ) -> io::Result<Self> {
        assert!(size > 0, "ShmFile size must be > 0");
        let safe_name = sanitize(logical_name);
        unsafe { Self::platform_create_or_open(&safe_name, size) }
    }

    /// Mutable byte slice into the mapped region. Length equals the
    /// `size` passed at creation time. Cross-platform.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        #[cfg(unix)]
        {
            &mut self.mmap[..]
        }
        #[cfg(windows)]
        {
            unsafe {
                std::slice::from_raw_parts_mut(self.view as *mut u8, self.len)
            }
        }
    }

    /// Length of the mapped region in bytes.
    pub fn len(&self) -> usize { self.len }

    /// True if the mapped region is zero bytes (never possible since
    /// `create_or_open_named` asserts size > 0; method exists for
    /// clippy's `len_without_is_empty`).
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Logical name (without the platform prefix).
    pub fn logical_name(&self) -> &str { &self.name }

    // ---------------------------------------------------------------
    // Unix implementation: shm_open + ftruncate + File::from_raw_fd.
    // ---------------------------------------------------------------
    #[cfg(unix)]
    unsafe fn platform_create_or_open(
        safe_name: &str,
        size: usize,
    ) -> io::Result<Self> {
        let c_name = std::ffi::CString::new(safe_name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let fd = unsafe {
            libc::shm_open(
                c_name.as_ptr(),
                libc::O_CREAT | libc::O_RDWR,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // macOS permits ftruncate on a POSIX shm object only once,
        // right at creation; a second opener (the child process, or a
        // re-open of an existing region) gets EINVAL. Size it only when
        // it is not already at least `size`, so the creator grows it and
        // every later opener maps the existing region as-is. Linux
        // tolerates the repeat ftruncate, so the guard is a harmless
        // no-op there.
        let cur_len = {
            let mut st: libc::stat = unsafe { std::mem::zeroed() };
            if unsafe { libc::fstat(fd, &mut st) } == 0 {
                st.st_size as usize
            } else {
                0
            }
        };
        if cur_len < size && unsafe { libc::ftruncate(fd, size as libc::off_t) } != 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(err);
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let mut mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };
        // Every adaptive-ring / bridge / locale backing flows
        // through here: prefault in one call instead of one soft
        // fault per 4 KiB on the first traffic pass.
        crate::mmf_warm::warm_mmap(&mut mmap);
        Ok(Self {
            name: safe_name.to_string(),
            len: size,
            mmap,
            _file: file,
        })
    }

    // ---------------------------------------------------------------
    // Windows implementation: CreateFileMappingW + MapViewOfFile.
    // ---------------------------------------------------------------
    #[cfg(windows)]
    unsafe fn platform_create_or_open(
        safe_name: &str,
        size: usize,
    ) -> io::Result<Self> {
        use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::System::Memory::{
            CreateFileMappingW, MapViewOfFile,
            FILE_MAP_ALL_ACCESS, PAGE_READWRITE,
        };

        let wide: Vec<u16> = safe_name.encode_utf16().chain(Some(0)).collect();
        let hi = (size >> 32) as u32;
        let lo = (size & 0xFFFF_FFFF) as u32;
        let handle = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                core::ptr::null(),
                PAGE_READWRITE,
                hi,
                lo,
                wide.as_ptr(),
            )
        };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let view = unsafe {
            MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size)
        };
        if view.Value.is_null() {
            let err = io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(err);
        }
        // Prefault the view in one call (see the unix arm).
        unsafe {
            crate::mmf_warm::warm_region(view.Value as *mut u8, size);
        }
        Ok(Self {
            name: safe_name.to_string(),
            len: size,
            handle,
            view: view.Value,
        })
    }
}

impl Drop for ShmFile {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            // _file closes the fd on drop. shm_unlink removes the
            // named object so a subsequent open with the same name
            // starts fresh.
            let safe_name = self.name.clone();
            if let Ok(c_name) = std::ffi::CString::new(safe_name) {
                unsafe { libc::shm_unlink(c_name.as_ptr()) };
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::CloseHandle;
            use windows_sys::Win32::System::Memory::{
                MEMORY_MAPPED_VIEW_ADDRESS, UnmapViewOfFile,
            };
            unsafe {
                if !self.view.is_null() {
                    UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
                        Value: self.view,
                    });
                }
                if !self.handle.is_null() {
                    CloseHandle(self.handle);
                }
            }
        }
    }
}

/// Sanitize the caller's name into a platform-safe identifier.
/// Replaces path separators with underscores and prefixes with the
/// platform-appropriate namespace.
fn sanitize(logical_name: &str) -> String {
    let cleaned: String = logical_name
        .chars()
        .map(|c| if c == '/' || c == '\\' { '_' } else { c })
        .collect();
    #[cfg(unix)]
    {
        let full = format!("/subetha_{cleaned}");
        // macOS (and every Apple target) caps POSIX shm names at
        // PSHMNAMLEN (31 chars including the leading '/'); a
        // $TMPDIR-derived logical name overruns it and shm_open
        // returns ENAMETOOLONG. Collapse an over-long name to a fixed
        // short hash so a create here and an open in another process
        // still resolve to the same region. Linux (NAME_MAX 255) keeps
        // the readable name.
        #[cfg(target_vendor = "apple")]
        {
            if full.len() > 31 {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                cleaned.hash(&mut h);
                return format!("/se_{:016x}", h.finish());
            }
        }
        full
    }
    #[cfg(windows)]
    {
        format!("Local\\subetha_{cleaned}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_name(prefix: &str) -> String {
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{prefix}_{pid}_{nonce}")
    }

    #[test]
    fn create_named_and_read_write() {
        let name = unique_name("shm_basic");
        let mut shm = ShmFile::create_or_open_named(&name, 4096)
            .expect("create shm");
        let slice = shm.as_mut_slice();
        slice[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(&slice[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(shm.len(), 4096);
    }

    #[test]
    fn two_handles_same_name_see_same_memory() {
        let name = unique_name("shm_share");
        let mut a = ShmFile::create_or_open_named(&name, 4096)
            .expect("create A");
        let mut b = ShmFile::create_or_open_named(&name, 4096)
            .expect("create B (same name)");
        a.as_mut_slice()[100..104]
            .copy_from_slice(&[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(&b.as_mut_slice()[100..104], &[0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn sanitize_is_deterministic() {
        // A create and a later open in another process derive the
        // backing name from the same logical name; the derivation
        // (including the Apple hash fallback) must be stable.
        let n = unique_name("shm_det");
        assert_eq!(sanitize(&n), sanitize(&n));
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn apple_shm_name_within_pshmnamlen() {
        // A $TMPDIR-derived ring name far exceeds macOS's 31-char
        // shm_open limit (PSHMNAMLEN); sanitize must shorten it while
        // staying deterministic so create and open still agree.
        let long = "subetha_cmp_spsc_p2c_99999_1234567890123456789012_spsc";
        let name = sanitize(long);
        assert!(name.len() <= 31, "shm name too long for macOS: {name} ({})", name.len());
        assert!(name.starts_with('/'));
        assert_eq!(sanitize(long), name, "must be deterministic");
    }

    #[test]
    fn drop_then_recreate_fresh() {
        let name = unique_name("shm_drop");
        {
            let mut a = ShmFile::create_or_open_named(&name, 4096)
                .expect("create A");
            a.as_mut_slice()[0..4].copy_from_slice(&[1, 2, 3, 4]);
        }
        // After A drops, the named object is gone; the new open
        // creates fresh, zeroed memory.
        let mut b = ShmFile::create_or_open_named(&name, 4096)
            .expect("recreate after drop");
        assert_eq!(&b.as_mut_slice()[0..4], &[0, 0, 0, 0]);
    }
}
