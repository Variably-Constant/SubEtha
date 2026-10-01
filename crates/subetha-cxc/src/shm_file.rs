//! `ShmFile`: cross-platform RAM-resident named shared-memory backing.
//!
//! Wraps the platform's named-shared-memory primitive so the rest of
//! the substrate can treat ShmFs the same way it treats anon and
//! file backings: hand it to a ring constructor, get a `&mut [u8]`
//! into the shared region, build a ring on top.
//!
//! - **Unix** (Linux + macOS): `shm_open(2)` + `ftruncate(2)` +
//!   memmap2 via `File::from_raw_fd`. On Drop the inner `File` closes
//!   the fd, and a handle that owns the region's name `shm_unlink(2)`s
//!   it, so a later create with the same name starts fresh. A handle
//!   owns the name when it made the region or was asked to create it;
//!   a handle that opened a region another made leaves the name alone,
//!   so peers attaching and leaving never stop a later process from
//!   attaching. A structure whose regions different handles make gives
//!   each name up with [`ShmFile::keep_name`] and removes them together,
//!   as an `AdaptiveRing` does.
//! - **Windows**: `CreateFileMappingW(INVALID_HANDLE_VALUE, ...)`
//!   for page-file-backed shared memory, `OpenFileMappingW` for a region
//!   that must already exist, and `MapViewOfFile` to get the mapped
//!   pointer. On Drop: `UnmapViewOfFile` + `CloseHandle`. Windows
//!   refcounts handles; the named object goes away on last handle close.
//!
//! Naming convention: a caller-supplied logical name is prefixed
//! with `/subetha_` on Unix (shm_open requires names starting with
//! `/`) and, on Windows, with the object directory of the
//! [`ShmNamespace`] the caller asks for: `Local\subetha_`,
//! `Global\subetha_`, or an AppContainer's named-object path followed by
//! `\subetha_`. Embedded slashes in the caller's name become underscores
//! so the whole logical name is one path component.

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
    /// Whether this handle owns the region's name: it made the region,
    /// or it was asked to create one and found a region of that name
    /// there, which its caller then lays out afresh, and it has not given
    /// the name up with `keep_name`. On Unix only an owning handle
    /// removes the name when it drops.
    owns_name: bool,
    /// The name as `shm_unlink` takes it, kept so the drop that removes
    /// the name has nothing left to convert.
    #[cfg(unix)]
    c_name: std::ffi::CString,
    #[cfg(unix)]
    mmap: MmapMut,
    #[cfg(unix)]
    _file: File,
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(windows)]
    view: *mut core::ffi::c_void,
    /// The object directory the name was resolved in, which a waker on
    /// this region names its park events in.
    #[cfg(windows)]
    directory: String,
    /// The descriptor this handle was asked to create the region with,
    /// which a waker on this region gives its park events.
    #[cfg(windows)]
    sddl: Option<String>,
}

unsafe impl Send for ShmFile {}
unsafe impl Sync for ShmFile {}

/// The most bytes a SID occupies: Windows' `SECURITY_MAX_SID_SIZE`, which
/// is a revision byte, a sub-authority count, six authority bytes and at
/// most fifteen four-byte sub-authorities.
const SID_MAX_BYTES: usize = 68;

/// The most sub-authorities a SID carries (`SID_MAX_SUB_AUTHORITIES`).
const SID_MAX_SUB_AUTHORITIES: usize = 15;

/// The identifier authority of app package and AppContainer SIDs
/// (`SECURITY_APP_PACKAGE_AUTHORITY`).
const APP_PACKAGE_AUTHORITY: u64 = 15;

/// The first sub-authority of an AppContainer SID
/// (`SECURITY_APP_PACKAGE_BASE_RID`).
const APP_PACKAGE_BASE_RID: u32 = 2;

/// An AppContainer's security identifier, held inline so a
/// [`ShmNamespace`] that names it stays `Copy`.
///
/// Parsed from its string form, `S-1-15-2-` followed by the container's
/// sub-authorities, or on Windows derived from the name the container's
/// profile was created under with
/// [`from_container_name`](Self::from_container_name). Anything that is
/// not an AppContainer's SID is refused. It prints as the string form.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContainerSid {
    len: u8,
    bytes: [u8; SID_MAX_BYTES],
}

impl ContainerSid {
    /// From a SID's binary form: a revision of 1, a sub-authority count,
    /// the six-byte big-endian authority and the little-endian
    /// sub-authorities. Refused unless the authority is the app package
    /// authority and the first sub-authority the AppContainer base.
    fn from_bytes(raw: &[u8]) -> io::Result<Self> {
        let refuse = |why: &str| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("not an AppContainer SID: {why}"))
        };
        if raw.len() < 8 || raw.len() > SID_MAX_BYTES {
            return Err(refuse("a SID is 8 to 68 bytes"));
        }
        let count = usize::from(raw[1]);
        if raw[0] != 1 || raw.len() != 8 + 4 * count {
            return Err(refuse("the revision or sub-authority count does not match its length"));
        }
        let authority = u64::from_be_bytes([0, 0, raw[2], raw[3], raw[4], raw[5], raw[6], raw[7]]);
        if authority != APP_PACKAGE_AUTHORITY
            || count < 2
            || u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) != APP_PACKAGE_BASE_RID
        {
            return Err(refuse("it is not in the AppContainer range S-1-15-2-..."));
        }
        let mut bytes = [0u8; SID_MAX_BYTES];
        bytes[..raw.len()].copy_from_slice(raw);
        Ok(Self { len: raw.len() as u8, bytes })
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    /// The SID Windows gives the AppContainer whose profile was created
    /// under `name`, as `DeriveAppContainerSidFromAppContainerName`
    /// computes it.
    #[cfg(windows)]
    pub fn from_container_name(name: &str) -> io::Result<Self> {
        use windows_sys::Win32::Security::Isolation::DeriveAppContainerSidFromAppContainerName;
        use windows_sys::Win32::Security::{FreeSid, GetLengthSid};

        let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        let mut sid = core::ptr::null_mut();
        // SAFETY: wide is a NUL-terminated name; sid receives an
        // allocation freed below.
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut sid) };
        if hr < 0 {
            return Err(io::Error::other(format!(
                "DeriveAppContainerSidFromAppContainerName({name}) returned {hr:#x}"
            )));
        }
        // SAFETY: sid is the valid SID the call allocated, GetLengthSid
        // bytes long.
        let result = unsafe {
            let len = GetLengthSid(sid) as usize;
            Self::from_bytes(std::slice::from_raw_parts(sid as *const u8, len))
        };
        // SAFETY: sid came from the derive call and is freed once.
        unsafe { FreeSid(sid) };
        result
    }

    /// The container's named-object directory, as
    /// `GetAppContainerNamedObjectPath` names it: the path a name is
    /// created under from outside the container.
    #[cfg(windows)]
    fn named_object_path(&self) -> io::Result<String> {
        use windows_sys::Win32::Security::Isolation::GetAppContainerNamedObjectPath;

        // The call takes the SID through a mutable pointer, so it is
        // handed a copy.
        let mut sid = self.bytes;
        let psid = sid.as_mut_ptr() as *mut core::ffi::c_void;
        let mut needed = 0u32;
        // SAFETY: psid addresses a valid SID; a null buffer of length
        // zero asks only for the length, so the call's own answer is the
        // length it writes, read below.
        unsafe {
            GetAppContainerNamedObjectPath(
                core::ptr::null_mut(),
                psid,
                0,
                core::ptr::null_mut(),
                &mut needed,
            )
        };
        if needed == 0 {
            return Err(io::Error::last_os_error());
        }
        // One more for a terminator the reported length may leave out.
        let mut path = vec![0u16; needed as usize + 1];
        // SAFETY: path holds path.len() writable wide characters.
        let ok = unsafe {
            GetAppContainerNamedObjectPath(
                core::ptr::null_mut(),
                psid,
                path.len() as u32,
                path.as_mut_ptr(),
                &mut needed,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let end = path.iter().position(|&c| c == 0).ok_or_else(|| {
            io::Error::other("GetAppContainerNamedObjectPath returned an unterminated path")
        })?;
        Ok(String::from_utf16_lossy(&path[..end]))
    }
}

impl std::str::FromStr for ContainerSid {
    type Err = io::Error;

    /// Parse the string form, `S-1-15-2-` and the container's
    /// sub-authorities in decimal.
    fn from_str(text: &str) -> io::Result<Self> {
        let refuse = |why: String| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{text} is not a SID string: {why}"))
        };
        let mut parts = text.split('-');
        if parts.next() != Some("S") {
            return Err(refuse("it does not start with S-".to_owned()));
        }
        let revision: u8 = match parts.next() {
            Some(p) => p.parse().map_err(|e| refuse(format!("revision {p}: {e}")))?,
            None => return Err(refuse("no revision".to_owned())),
        };
        let authority: u64 = match parts.next() {
            Some(p) => p.parse().map_err(|e| refuse(format!("authority {p}: {e}")))?,
            None => return Err(refuse("no authority".to_owned())),
        };
        if authority >= 1 << 48 {
            return Err(refuse(format!("authority {authority} is wider than six bytes")));
        }
        let mut raw = [0u8; SID_MAX_BYTES];
        raw[0] = revision;
        raw[2..8].copy_from_slice(&authority.to_be_bytes()[2..]);
        let mut count = 0usize;
        for part in parts {
            if count == SID_MAX_SUB_AUTHORITIES {
                return Err(refuse(format!("more than {SID_MAX_SUB_AUTHORITIES} sub-authorities")));
            }
            let sub: u32 = part.parse().map_err(|e| refuse(format!("sub-authority {part}: {e}")))?;
            raw[8 + 4 * count..12 + 4 * count].copy_from_slice(&sub.to_le_bytes());
            count += 1;
        }
        raw[1] = count as u8;
        Self::from_bytes(&raw[..8 + 4 * count])
    }
}

impl std::fmt::Display for ContainerSid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.as_bytes();
        let authority = u64::from_be_bytes([0, 0, b[2], b[3], b[4], b[5], b[6], b[7]]);
        write!(f, "S-{}-{authority}", b[0])?;
        for sub in b[8..].as_chunks::<4>().0 {
            write!(f, "-{}", u32::from_le_bytes(*sub))?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for ContainerSid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ContainerSid({self})")
    }
}

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
/// [`AppContainer`](ShmNamespace::AppContainer) reaches an AppContainer's
/// named-object directory from outside it. A process inside the container
/// reaches the same objects under [`Session`](ShmNamespace::Session),
/// because inside a container `Local\` resolves to that directory, so the
/// outside process names the container and the process inside names
/// nothing.
///
/// On Unix a POSIX shared-memory name is already machine-wide, so every
/// variant produces the same name and the choice changes nothing.
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
    /// The named-object directory of the AppContainer this SID names,
    /// `AppContainerNamedObjects\<SID>` on Windows. The directory exists
    /// only while a process of the container is running, and a create
    /// there fails with the path not found until one is, so a creator
    /// starts its container process suspended, creates what the
    /// container will open, and then lets it run. Which processes may
    /// map a region is still decided by its security descriptor, which
    /// must admit the container's SID.
    AppContainer(ContainerSid),
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
    /// This is for a region either side may be first to reach. The
    /// handle owns the name only if this call made the region.
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
        let resolved = resolve(logical_name, namespace)?;
        unsafe { Self::platform_create_or_open(resolved, size, sddl, false) }
    }

    /// Create a named region as its creator, the handle whose drop
    /// removes the name on Unix.
    ///
    /// As [`create_or_open_named_secured`](Self::create_or_open_named_secured),
    /// except that the handle owns the name even when a region of that
    /// name was already there, such as one a process that died left
    /// behind: the caller is about to lay it out afresh, so the region is
    /// its own from here.
    pub fn create_named_secured(
        logical_name: &str,
        size: usize,
        namespace: ShmNamespace,
        sddl: Option<&str>,
    ) -> io::Result<Self> {
        assert!(size > 0, "ShmFile size must be > 0");
        let resolved = resolve(logical_name, namespace)?;
        unsafe { Self::platform_create_or_open(resolved, size, sddl, true) }
    }

    /// Open a named region in `namespace` that must already exist.
    pub fn open_named_in(
        logical_name: &str,
        size: usize,
        namespace: ShmNamespace,
    ) -> io::Result<Self> {
        Self::open_named_secured(logical_name, size, namespace, None)
    }

    /// Open a named region in `namespace` that must already exist.
    ///
    /// A region nobody made is [`io::ErrorKind::NotFound`] rather than a
    /// fresh empty one, and a region smaller than `size` is refused. The
    /// handle never owns the name, so on Unix its drop leaves the region
    /// for its owner and the processes still attached.
    ///
    /// `sddl` is not applied to the region, which carries its own
    /// descriptor already. It is kept for the objects made beside the
    /// region, such as a waker's park events, which need the same
    /// descriptor to be reached from the other side.
    pub fn open_named_secured(
        logical_name: &str,
        size: usize,
        namespace: ShmNamespace,
        sddl: Option<&str>,
    ) -> io::Result<Self> {
        assert!(size > 0, "ShmFile size must be > 0");
        let resolved = resolve(logical_name, namespace)?;
        unsafe { Self::platform_open(resolved, size, sddl) }
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

    /// The region's name as the platform resolves it, prefix included:
    /// `Local\subetha_...`, `Global\subetha_...` or an AppContainer's
    /// named-object path on Windows, and a name rooted at `/` on Unix.
    pub fn logical_name(&self) -> &str { &self.name }

    /// Whether this handle owns the region's name: it made the region,
    /// or it was created with
    /// [`create_named_secured`](Self::create_named_secured), and it has
    /// not given the name up with [`keep_name`](Self::keep_name). On Unix
    /// only the owner removes the name when it drops; on Windows the name
    /// lasts until the last handle to the region closes, whoever owns it.
    pub fn owns_name(&self) -> bool { self.owns_name }

    /// Give up this handle's ownership of the region's name, so on Unix
    /// its drop leaves the name for whatever removes it by name.
    ///
    /// This is for a structure whose regions different handles make and
    /// which removes them together rather than as each maker drops: an
    /// [`AdaptiveRing`](crate::adaptive_ring::AdaptiveRing) keeps every
    /// name it makes, and
    /// [`unlink_shmfs`](crate::adaptive_ring::AdaptiveRing::unlink_shmfs)
    /// or its last holder removes them, as a file-backed ring's files are
    /// removed. On Windows the name lasts until the last handle to the
    /// region closes either way.
    pub fn keep_name(&mut self) { self.owns_name = false; }

    /// The object directory this region's name was resolved in.
    #[cfg(windows)]
    pub(crate) fn directory(&self) -> &str { &self.directory }

    /// The SDDL descriptor this handle was created or opened with, if any.
    #[cfg(windows)]
    pub(crate) fn sddl(&self) -> Option<&str> { self.sddl.as_deref() }

    // ---------------------------------------------------------------
    // Unix implementation: shm_open + ftruncate + File::from_raw_fd.
    // ---------------------------------------------------------------

    /// Make the object, or open the one of that name. `creator` makes the
    /// handle own the name even when it opened an existing object.
    #[cfg(unix)]
    unsafe fn platform_create_or_open(
        resolved: Resolved,
        size: usize,
        _sddl: Option<&str>,
        creator: bool,
    ) -> io::Result<Self> {
        let safe_name = resolved.name.as_str();
        let c_name = std::ffi::CString::new(safe_name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        loop {
            // An exclusive create is how the handle learns whether it
            // made the object, which decides whether it owns the name.
            let fd = unsafe {
                libc::shm_open(
                    c_name.as_ptr(),
                    libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                    0o600,
                )
            };
            if fd >= 0 {
                let made = unsafe { Self::map_fd(fd, c_name.clone(), safe_name, size, true, true) };
                if made.is_err() {
                    // Nobody else holds a region this call made and could
                    // not map, so its name goes with it.
                    unsafe { libc::shm_unlink(c_name.as_ptr()) };
                }
                return made;
            }
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EEXIST) {
                return Err(err);
            }
            let fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_RDWR, 0o600) };
            if fd >= 0 {
                return unsafe { Self::map_fd(fd, c_name, safe_name, size, true, creator) };
            }
            let err = io::Error::last_os_error();
            // Its owner removed it between the two calls: make it afresh.
            if err.raw_os_error() != Some(libc::ENOENT) {
                return Err(err);
            }
        }
    }

    /// Open the object, which must exist.
    #[cfg(unix)]
    unsafe fn platform_open(
        resolved: Resolved,
        size: usize,
        _sddl: Option<&str>,
    ) -> io::Result<Self> {
        let safe_name = resolved.name.as_str();
        let c_name = std::ffi::CString::new(safe_name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_RDWR, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe { Self::map_fd(fd, c_name, safe_name, size, false, false) }
    }

    /// Map `size` bytes of the object `fd` refers to, closing `fd` on
    /// failure. With `grow` an object shorter than `size` is sized up to
    /// it; without, it is refused.
    #[cfg(unix)]
    unsafe fn map_fd(
        fd: i32,
        c_name: std::ffi::CString,
        safe_name: &str,
        size: usize,
        grow: bool,
        owns_name: bool,
    ) -> io::Result<Self> {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } != 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(err);
        }
        let cur_len = st.st_size as usize;
        // macOS permits ftruncate on a POSIX shm object only once,
        // right at creation; a second opener (the child process, or a
        // re-open of an existing region) gets EINVAL. Size it only when
        // it is not already at least `size`, so the creator grows it and
        // every later opener maps the existing region as-is. Linux
        // tolerates the repeat ftruncate, so the guard is a harmless
        // no-op there.
        if cur_len < size {
            if !grow {
                unsafe { libc::close(fd) };
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{safe_name} holds {cur_len} bytes where {size} were expected"),
                ));
            }
            if unsafe { libc::ftruncate(fd, size as libc::off_t) } != 0 {
                let err = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(err);
            }
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
            owns_name,
            c_name,
            mmap,
            _file: file,
        })
    }

    // ---------------------------------------------------------------
    // Windows implementation: CreateFileMappingW / OpenFileMappingW +
    // MapViewOfFile.
    // ---------------------------------------------------------------

    /// Create the section, or open the one of that name. `creator` makes
    /// the handle own the name even when it opened an existing section.
    #[cfg(windows)]
    unsafe fn platform_create_or_open(
        resolved: Resolved,
        size: usize,
        sddl: Option<&str>,
        creator: bool,
    ) -> io::Result<Self> {
        use windows_sys::Win32::Foundation::{
            CloseHandle, LocalFree, SetLastError, ERROR_ALREADY_EXISTS, INVALID_HANDLE_VALUE,
        };
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

        let wide: Vec<u16> = resolved.name.encode_utf16().chain(Some(0)).collect();
        let hi = (size >> 32) as u32;
        let lo = (size & 0xFFFF_FFFF) as u32;
        // Cleared first, so the ERROR_ALREADY_EXISTS read below is this
        // call's answer and never one an earlier call left behind.
        unsafe { SetLastError(0) };
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
        let existed = create_err.raw_os_error() == Some(ERROR_ALREADY_EXISTS as i32);
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
            name: resolved.name,
            len: size,
            owns_name: creator || !existed,
            handle,
            view: view.Value,
            directory: resolved.directory,
            sddl: sddl.map(str::to_owned),
        })
    }

    /// Open the section, which must exist.
    #[cfg(windows)]
    unsafe fn platform_open(
        resolved: Resolved,
        size: usize,
        sddl: Option<&str>,
    ) -> io::Result<Self> {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Memory::{
            MapViewOfFile, OpenFileMappingW, FILE_MAP_ALL_ACCESS,
        };

        let wide: Vec<u16> = resolved.name.encode_utf16().chain(Some(0)).collect();
        let handle = unsafe { OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, wide.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // Every byte of a view must lie within the size the section was
        // created with, so a section smaller than `size` is refused here.
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
            name: resolved.name,
            len: size,
            owns_name: false,
            handle,
            view: view.Value,
            directory: resolved.directory,
            sddl: sddl.map(str::to_owned),
        })
    }
}

impl Drop for ShmFile {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            // _file closes the fd on drop. The name's owner removes it so
            // a later create starts fresh; a handle that opened a region
            // another made leaves it for the processes still attached.
            if self.owns_name {
                unsafe { libc::shm_unlink(self.c_name.as_ptr()) };
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

/// The object directory a Windows name in `ns` is created under:
/// `Local`, `Global`, or the AppContainer's named-object path. The
/// shared-memory regions, the notifiers and the park events beside them
/// all take their prefix from here, so every object of one structure
/// lands in the same directory.
#[cfg(windows)]
pub(crate) fn object_directory(ns: ShmNamespace) -> io::Result<String> {
    match ns {
        ShmNamespace::Session => Ok("Local".to_owned()),
        ShmNamespace::Machine => Ok("Global".to_owned()),
        ShmNamespace::AppContainer(sid) => sid.named_object_path(),
    }
}

/// Remove the name `logical_name` resolves to in `ns`, so no later open
/// finds the region. Handles that map it keep their mappings. A name
/// that is not there is [`io::ErrorKind::NotFound`].
#[cfg(unix)]
pub(crate) fn unlink_named(logical_name: &str, ns: ShmNamespace) -> io::Result<()> {
    let resolved = resolve(logical_name, ns)?;
    let c_name = std::ffi::CString::new(resolved.name)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    if unsafe { libc::shm_unlink(c_name.as_ptr()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// A logical name resolved for this platform.
struct Resolved {
    /// The name the platform call takes.
    name: String,
    /// The object directory the name lives in.
    #[cfg(windows)]
    directory: String,
}

/// Resolve `logical_name` in `ns`. Path separators in the caller's name
/// become underscores. A POSIX shared-memory name is machine-wide
/// whichever namespace is asked for, so `ns` selects a directory only on
/// Windows.
#[cfg(windows)]
fn resolve(logical_name: &str, ns: ShmNamespace) -> io::Result<Resolved> {
    let directory = object_directory(ns)?;
    let name = format!("{directory}\\subetha_{}", clean(logical_name));
    Ok(Resolved { name, directory })
}

/// Resolve `logical_name`; see the Windows arm.
#[cfg(unix)]
fn resolve(logical_name: &str, _ns: ShmNamespace) -> io::Result<Resolved> {
    let cleaned = clean(logical_name);
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
            return Ok(Resolved { name: format!("/se_{:016x}", h.finish()) });
        }
    }
    Ok(Resolved { name: full })
}

/// The caller's name as one path component: path separators become
/// underscores.
fn clean(logical_name: &str) -> String {
    logical_name
        .chars()
        .map(|c| if c == '/' || c == '\\' { '_' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_name(prefix: &str) -> String {
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the wall clock is after the epoch")
            .as_nanos();
        format!("{prefix}_{pid}_{nonce}")
    }

    /// The platform name `n` resolves to in `ns`.
    fn platform_name(n: &str, ns: ShmNamespace) -> String {
        resolve(n, ns).expect("the name resolves").name
    }

    /// The SID of an AppContainer profile created on the Windows host.
    const A_CONTAINER_SID: &str =
        "S-1-15-2-2984720079-756820249-1175153539-3767409642-3989518943-1696783984-3418178104";

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
            platform_name(&n, ShmNamespace::Session),
            platform_name(&n, ShmNamespace::Session)
        );
    }

    /// The namespace decides which processes can resolve the name, so a
    /// caller that asks for one must not be handed the other.
    #[test]
    fn namespace_selects_the_windows_prefix() {
        let n = unique_name("shm_ns");
        let session = platform_name(&n, ShmNamespace::Session);
        let machine = platform_name(&n, ShmNamespace::Machine);

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

    /// A name in an AppContainer's namespace lands in that container's
    /// named-object directory, which is where a process inside the
    /// container finds it under `Local\`.
    #[test]
    fn an_appcontainer_namespace_names_the_containers_directory() {
        let sid: ContainerSid = A_CONTAINER_SID.parse().expect("an AppContainer SID");
        let n = unique_name("shm_ac");
        let name = platform_name(&n, ShmNamespace::AppContainer(sid));
        #[cfg(windows)]
        assert_eq!(name, format!("AppContainerNamedObjects\\{A_CONTAINER_SID}\\subetha_{n}"));
        #[cfg(unix)]
        assert_eq!(name, platform_name(&n, ShmNamespace::Session));
    }

    /// The string form parses to the binary SID and prints back
    /// unchanged, so a SID handed over as text names the same container.
    #[test]
    fn a_container_sid_round_trips_its_string_form() {
        let sid: ContainerSid = A_CONTAINER_SID.parse().expect("an AppContainer SID");
        assert_eq!(sid.to_string(), A_CONTAINER_SID);
        assert_eq!(format!("{sid:?}"), format!("ContainerSid({A_CONTAINER_SID})"));
        assert_eq!(sid.as_bytes().len(), 8 + 4 * 8, "authority 15 with eight sub-authorities");
    }

    /// Only an AppContainer's SID names a container, so a user or
    /// capability SID and anything malformed are refused at the parse
    /// rather than at the first create.
    #[test]
    fn anything_but_an_appcontainer_sid_is_refused() {
        for text in [
            "S-1-5-21-1004336348-1177238915-682003330-512",
            "S-1-15-3-1024-1065365936-1281604716-3511738428-1654721687-432734479-3232135806-4053264122-3456934681",
            "S-1-15",
            "S-1-15-2",
            "X-1-15-2-1-2",
            "S-1-15-2-one-2",
            "S-1-15-2-1-2-3-4-5-6-7-8-9-10-11-12-13-14-15",
            "S-1-281474976710656-2-1",
            "",
        ] {
            let parsed = text.parse::<ContainerSid>();
            assert!(
                matches!(&parsed, Err(e) if e.kind() == io::ErrorKind::InvalidInput),
                "{text:?} parsed as {parsed:?}"
            );
        }
    }

    /// On Windows a container's name derives the SID the container was
    /// given, so a creator can name its container either way.
    #[cfg(windows)]
    #[test]
    fn a_container_name_derives_its_sid() {
        let sid = ContainerSid::from_container_name("subetha.acns.probe")
            .expect("any name derives a SID");
        assert_eq!(sid.to_string(), A_CONTAINER_SID);
    }

    /// The machine namespace reaches the real OS call rather than
    /// stopping at name construction. Creating there needs
    /// `SeCreateGlobalPrivilege`, which an ordinary test process does
    /// not hold, so a refusal is a valid outcome - what must not happen
    /// is a per-session region handed back as though the request had
    /// been honored.
    #[test]
    fn machine_namespace_reaches_the_os_and_never_downgrades() {
        let n = unique_name("shm_machine");
        match ShmFile::create_or_open_named_in(&n, 4096, ShmNamespace::Machine) {
            Ok(shm) => {
                let want = platform_name(&n, ShmNamespace::Machine);
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
                let session = platform_name(&n, ShmNamespace::Session);
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
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn apple_shm_name_within_pshmnamlen() {
        // A $TMPDIR-derived ring name far exceeds macOS's 31-char
        // shm_open limit (PSHMNAMLEN); resolve must shorten it while
        // staying deterministic so create and open still agree.
        let long = "subetha_cmp_spsc_p2c_99999_1234567890123456789012_spsc";
        let name = platform_name(long, ShmNamespace::Session);
        assert!(name.len() <= 31, "shm name too long for macOS: {name} ({})", name.len());
        assert!(name.starts_with('/'));
        assert_eq!(
            platform_name(long, ShmNamespace::Session),
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

    /// An open finds a region or says it is not there: it never makes an
    /// empty one that a peer would then attach to as though it were the
    /// creator's.
    #[test]
    fn opening_a_region_nobody_made_is_not_found() {
        let name = unique_name("shm_absent");
        let opened = ShmFile::open_named_in(&name, 4096, ShmNamespace::Session);
        assert!(
            matches!(&opened, Err(e) if e.kind() == io::ErrorKind::NotFound),
            "an open of a missing region reported {:?}",
            opened.as_ref().map(ShmFile::len)
        );
        let made = ShmFile::create_or_open_named(&name, 4096).expect("the create after it");
        assert!(made.owns_name(), "the failed open made nothing, so this call made the region");
    }

    /// An open that asks for more than the region holds is refused
    /// rather than mapping past its end.
    #[test]
    fn opening_a_region_smaller_than_asked_is_refused() {
        let name = unique_name("shm_short");
        let _made = ShmFile::create_or_open_named(&name, 4096).expect("create");
        assert!(ShmFile::open_named_in(&name, 1 << 20, ShmNamespace::Session).is_err());
    }

    /// The handle that made a region owns its name, and a handle asked to
    /// create one owns it even when the region was there already; one
    /// that found the region or opened it does not.
    #[test]
    fn a_name_is_owned_by_the_handle_that_made_or_was_asked_to_create_it() {
        let name = unique_name("shm_owner");
        let made = ShmFile::create_or_open_named(&name, 4096).expect("create");
        let found = ShmFile::create_or_open_named(&name, 4096).expect("create-or-open again");
        let opened =
            ShmFile::open_named_in(&name, 4096, ShmNamespace::Session).expect("open");
        let creator = ShmFile::create_named_secured(&name, 4096, ShmNamespace::Session, None)
            .expect("create as the creator");
        assert!(made.owns_name());
        assert!(!found.owns_name());
        assert!(!opened.owns_name());
        assert!(creator.owns_name());
    }

    /// A handle that opened a region leaves its name when it drops, so a
    /// process attaching after another has left still finds what the
    /// creator made. On Unix the name would otherwise go with the first
    /// handle to drop, whoever made it.
    #[test]
    fn a_handle_that_opened_a_region_leaves_the_name_for_later_openers() {
        let name = unique_name("shm_leave");
        let mut creator = ShmFile::create_named_secured(&name, 4096, ShmNamespace::Session, None)
            .expect("create");
        creator.as_mut_slice()[0..4].copy_from_slice(b"kept");
        let first =
            ShmFile::open_named_in(&name, 4096, ShmNamespace::Session).expect("the first open");
        drop(first);
        let mut later = ShmFile::open_named_in(&name, 4096, ShmNamespace::Session)
            .expect("an open after another handle has dropped");
        assert_eq!(&later.as_mut_slice()[0..4], b"kept");
    }

    /// A handle that gives its name up leaves the region for a later open
    /// when it drops, where an owning handle would remove it, and the
    /// name is then removed by name. On Windows a section goes with its
    /// last handle whatever its name's owner, so this is Unix's to show.
    #[cfg(unix)]
    #[test]
    fn a_kept_name_outlives_the_handle_that_made_it_until_removed_by_name() {
        let name = unique_name("shm_kept");
        let mut made = ShmFile::create_named_secured(&name, 4096, ShmNamespace::Session, None)
            .expect("create");
        made.keep_name();
        assert!(!made.owns_name(), "a kept name is no longer the handle's");
        made.as_mut_slice()[0..4].copy_from_slice(b"kept");
        drop(made);

        let mut later = ShmFile::open_named_in(&name, 4096, ShmNamespace::Session)
            .expect("an open after the maker has dropped");
        assert_eq!(&later.as_mut_slice()[0..4], b"kept");
        drop(later);
        unlink_named(&name, ShmNamespace::Session).expect("the kept name is removed by name");
        let gone = ShmFile::open_named_in(&name, 4096, ShmNamespace::Session);
        assert!(
            matches!(&gone, Err(e) if e.kind() == io::ErrorKind::NotFound),
            "an open after the name was removed reported {:?}",
            gone.as_ref().map(ShmFile::len)
        );
    }
}
