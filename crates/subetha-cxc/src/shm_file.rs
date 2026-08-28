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
//! `/`) and, on Windows, with `Local\\subetha_` or `Global\\subetha_`
//! according to the [`ShmNamespace`] the caller asks for. Embedded
//! slashes in the caller's name become underscores so the whole
//! logical name is one path component.

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

/// Which object namespace a region's name is created in.
///
/// Windows resolves a shared-memory name inside a namespace. Two
/// processes in different terminal sessions that pass the same logical
/// name under [`Session`](ShmNamespace::Session) reach two different
/// regions, and both creates succeed, so a service in session 0 and an
/// interactive client in session 1 each get memory the other cannot
/// see. [`Machine`](ShmNamespace::Machine) resolves one name to one
/// region for every session on the host.
///
/// On Unix a POSIX shared-memory name is already machine-wide, so both
/// variants produce the same name and the choice changes nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShmNamespace {
    /// Per-session naming: `Local\` on Windows.
    #[default]
    Session,
    /// Machine-wide naming: `Global\` on Windows. Creating a region
    /// here requires `SeCreateGlobalPrivilege`, which a service running
    /// as LocalSystem holds and an ordinary interactive process does
    /// not; opening one that already exists requires no privilege.
    /// Naming is all this widens - which processes may map the region
    /// is still decided by its security descriptor.
    Machine,
}

impl ShmFile {
    /// Create or open a named RAM-resident shared-memory region of
    /// `size` bytes. Two handles created with the same logical name
    /// map onto the same underlying memory.
    pub fn create_or_open_named(
        logical_name: &str,
        size: usize,
    ) -> io::Result<Self> {
        Self::create_or_open_named_in(logical_name, size, ShmNamespace::Session)
    }

    /// Create or open a named region in `namespace`.
    ///
    /// [`ShmNamespace::Machine`] is what lets a process in one Windows
    /// session reach a region created by a process in another, which a
    /// service and its interactive clients need. A create that the
    /// caller lacks `SeCreateGlobalPrivilege` for fails with the OS
    /// error; it does not quietly fall back to a per-session region,
    /// because that succeeds while leaving each side mapping memory the
    /// other cannot see.
    pub fn create_or_open_named_in(
        logical_name: &str,
        size: usize,
        namespace: ShmNamespace,
    ) -> io::Result<Self> {
        Self::create_or_open_named_secured(logical_name, size, namespace, None)
    }

    /// Create or open a named region in `namespace`, with `sddl` as the
    /// security descriptor a create applies to it.
    ///
    /// On Windows a section created with no descriptor carries the
    /// creator's default, which admits the creating account and
    /// administrators. A service running as LocalSystem therefore
    /// creates a region in [`ShmNamespace::Machine`] whose name an
    /// interactive client resolves and whose contents it is refused, so
    /// reaching across sessions takes a descriptor that names who may
    /// map it. `sddl` is that descriptor in SDDL form, applied only when
    /// this call creates the region; opening one that exists uses the
    /// descriptor already on it.
    ///
    /// The mapping asks for `FILE_MAP_ALL_ACCESS`, so a descriptor that
    /// grants only read is refused at the map. A caller admitting
    /// authenticated users to map and query writes
    /// `"D:(A;;0x000F001F;;;AU)"`.
    ///
    /// Who may map a shared region is the creating application's
    /// decision: the crate applies what it is given and supplies no
    /// default of its own.
    ///
    /// Unix ignores `sddl`. A POSIX shared-memory object carries mode
    /// bits rather than an ACL, and the caller sets those on the object
    /// itself.
    pub fn create_or_open_named_secured(
        logical_name: &str,
        size: usize,
        namespace: ShmNamespace,
        sddl: Option<&str>,
    ) -> io::Result<Self> {
        assert!(size > 0, "ShmFile size must be > 0");
        let safe_name = sanitize(logical_name, namespace);
        unsafe { Self::platform_create_or_open(&safe_name, size, sddl) }
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
        #[cfg_attr(unix, allow(unused_variables))] sddl: Option<&str>,
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
        sddl: Option<&str>,
    ) -> io::Result<Self> {
        use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::Security::Authorization::{
            ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
        };
        use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
        use windows_sys::Win32::System::Memory::{
            CreateFileMappingW, MapViewOfFile,
            FILE_MAP_ALL_ACCESS, PAGE_READWRITE,
        };

        // The descriptor is built before the mapping and released after
        // it, because CreateFileMappingW copies what it is given.
        let mut sd = core::ptr::null_mut();
        if let Some(s) = sddl {
            let wide_sddl: Vec<u16> = s.encode_utf16().chain(Some(0)).collect();
            let ok = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    wide_sddl.as_ptr(),
                    SDDL_REVISION_1,
                    &mut sd,
                    core::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        let sa = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: sd,
            bInheritHandle: 0,
        };
        let sa_ptr: *const SECURITY_ATTRIBUTES = if sddl.is_some() {
            &sa
        } else {
            core::ptr::null()
        };

        let wide: Vec<u16> = safe_name.encode_utf16().chain(Some(0)).collect();
        let hi = (size >> 32) as u32;
        let lo = (size & 0xFFFF_FFFF) as u32;
        let handle = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                sa_ptr,
                PAGE_READWRITE,
                hi,
                lo,
                wide.as_ptr(),
            )
        };
        let create_err = io::Error::last_os_error();
        if !sd.is_null() {
            unsafe { LocalFree(sd as _) };
        }
        if handle.is_null() {
            return Err(create_err);
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
/// namespace `ns` names on this platform. A POSIX shared-memory name is
/// machine-wide whichever namespace is asked for, so `ns` selects a
/// prefix only on Windows.
#[cfg_attr(unix, allow(unused_variables))]
fn sanitize(logical_name: &str, ns: ShmNamespace) -> String {
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
        let prefix = match ns {
            ShmNamespace::Session => "Local",
            ShmNamespace::Machine => "Global",
        };
        format!("{prefix}\\subetha_{cleaned}")
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
        assert_eq!(
            sanitize(&n, ShmNamespace::Session),
            sanitize(&n, ShmNamespace::Session)
        );
    }

    /// The namespace decides which processes can resolve the name, so a
    /// caller that asks for one must not be handed the other.
    #[test]
    fn namespace_selects_the_windows_prefix() {
        let n = unique_name("shm_ns");
        let session = sanitize(&n, ShmNamespace::Session);
        let machine = sanitize(&n, ShmNamespace::Machine);

        assert_eq!(ShmNamespace::default(), ShmNamespace::Session);

        #[cfg(windows)]
        {
            assert!(session.starts_with("Local\\subetha_"), "{session}");
            assert!(machine.starts_with("Global\\subetha_"), "{machine}");
            assert_ne!(session, machine);
        }
        #[cfg(unix)]
        {
            // A POSIX name is machine-wide either way, so the two agree
            // and a caller writes one code path across platforms.
            assert_eq!(session, machine);
            assert!(session.starts_with('/'), "{session}");
        }
    }

    /// The machine namespace reaches the real OS call rather than
    /// stopping at name construction. Creating there needs
    /// `SeCreateGlobalPrivilege`, which an ordinary test process does
    /// not hold, so a refusal is a valid outcome - what must not happen
    /// is a per-session region handed back as though the request had
    /// been honoured.
    #[test]
    fn machine_namespace_reaches_the_os_and_never_downgrades() {
        let n = unique_name("shm_machine");
        match ShmFile::create_or_open_named_in(&n, 4096, ShmNamespace::Machine) {
            Ok(shm) => {
                let want = sanitize(&n, ShmNamespace::Machine);
                assert_eq!(
                    shm.logical_name(),
                    want,
                    "a region opened in the machine namespace must carry its name"
                );
                #[cfg(windows)]
                assert!(shm.logical_name().starts_with("Global\\"));
            }
            Err(e) => {
                // Refused for want of privilege, which is the honest
                // answer. The failure must not have been converted into
                // a session-scoped region behind the caller's back.
                let session = sanitize(&n, ShmNamespace::Session);
                let reopened =
                    ShmFile::create_or_open_named_in(&n, 4096, ShmNamespace::Machine);
                assert!(
                    reopened.is_err(),
                    "a refused machine create must stay refused, not settle into {session}"
                );
                println!("machine namespace refused for this process: {e}");
            }
        }
    }

    /// A descriptor reaches the OS rather than being carried and
    /// dropped. A create that names one and succeeds has applied it; one
    /// that names an unparseable descriptor is refused, so a caller
    /// cannot end up with a region open to whoever the default admits
    /// while believing its own descriptor took effect.
    #[test]
    fn a_security_descriptor_is_applied_or_the_create_fails() {
        let n = unique_name("shm_sddl");

        // Grants authenticated users the access the mapping asks for.
        let granted = ShmFile::create_or_open_named_secured(
            &n,
            4096,
            ShmNamespace::Session,
            Some("D:(A;;0x000F001F;;;AU)"),
        );
        match granted {
            Ok(shm) => assert_eq!(shm.len(), 4096),
            Err(e) => println!("descriptor refused for this process: {e}"),
        }

        let bad = ShmFile::create_or_open_named_secured(
            &format!("{n}_bad"),
            4096,
            ShmNamespace::Session,
            Some("this is not a security descriptor"),
        );
        #[cfg(windows)]
        assert!(
            bad.is_err(),
            "an unparseable descriptor must fail the create, not be ignored"
        );
        #[cfg(unix)]
        assert!(bad.is_ok(), "unix carries mode bits and ignores the descriptor");

        std::fs::remove_file(format!("/tmp/{n}")).ok();
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn apple_shm_name_within_pshmnamlen() {
        // A $TMPDIR-derived ring name far exceeds macOS's 31-char
        // shm_open limit (PSHMNAMLEN); sanitize must shorten it while
        // staying deterministic so create and open still agree.
        let long = "subetha_cmp_spsc_p2c_99999_1234567890123456789012_spsc";
        let name = sanitize(long, ShmNamespace::Session);
        assert!(name.len() <= 31, "shm name too long for macOS: {name} ({})", name.len());
        assert!(name.starts_with('/'));
        assert_eq!(
            sanitize(long, ShmNamespace::Session),
            name,
            "must be deterministic"
        );
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
