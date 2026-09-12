//! A pollable notifier beside a ring: a consumer creates one and hands its
//! native handle to an event loop, and every push in every process that
//! shares the ring's notifier record signals it. On Unix the notifier is
//! a named FIFO the consumer holds open for reading without blocking, so
//! `poll`, `epoll` and `kqueue` see it readable once a push has written a
//! byte; on Windows it is a named manual-reset event a wait function
//! returns from once a push has set it. Either way a signal stays until
//! the consumer drains it.
//!
//! The record shared by the ring's processes is 64 bytes: a magic, the
//! next notifier index, a generation and the count attached. A consumer
//! takes an index from the record, creates the named object for it and
//! bumps the generation; a producer keeps the write ends it has opened
//! and rescans the indexes when the generation moved, so a push costs one
//! atomic load while nothing is attached and one write or event set per
//! attached notifier while something is. Producers open a FIFO for both
//! reading and writing, so a write never finds the reader gone and never
//! raises `SIGPIPE`; a full pipe is a notifier already signaled.
//!
//! The record lives in a file beside a file-backed ring, in a named
//! shared-memory region beside a shared-memory ring, and in this
//! process's memory beside an anonymous ring, where the notifier is an
//! unnamed pipe or event and the set holds its signal side directly.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::shm_file::{ShmFile, ShmNamespace};

/// Magic in the record: ASCII "NTFY".
pub const NOTIFY_MAGIC: u32 = 0x4e54_4659;

/// The shared record. Sixty-four bytes, one cache line.
#[repr(C, align(64))]
pub struct NotifyRecord {
    pub magic: u32,
    /// The next index a consumer takes; never reused.
    pub next_index: AtomicU32,
    /// Bumped on every attach and detach; a producer rescans when it
    /// differs from the generation it last scanned at.
    pub generation: AtomicU64,
    /// Notifiers attached right now.
    pub attached: AtomicU32,
    _pad: [u8; 44],
}

const _: () = assert!(std::mem::size_of::<NotifyRecord>() == 64);

#[derive(Debug)]
pub enum NotifyError {
    Io(io::ErrorKind, String),
    /// The record exists with another magic.
    LayoutMismatch,
}

impl From<io::Error> for NotifyError {
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind(), e.to_string())
    }
}

/// A lock whose poisoning is irrelevant: the data under it is a list of
/// handles a panicking thread cannot have left half-written.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Where a ring's notifiers are named and its record kept.
#[derive(Clone)]
pub enum NotifyPlace {
    /// Beside a file-backed ring under `base`: the record at
    /// `<base>.notify.bin`, the notifiers at `<base>.notify.<index>` on
    /// Unix and named events on Windows.
    File(PathBuf),
    /// Beside a shared-memory ring named `name`: the record in the region
    /// `{name}_notify`, the notifiers named from `name` in the same
    /// namespace and, on Windows, with the same descriptor.
    Shm { name: String, namespace: ShmNamespace, sddl: Option<String> },
}

impl NotifyPlace {
    fn native_name(&self, index: u32) -> String {
        match self {
            NotifyPlace::File(base) => native_name_for_path(base, index),
            NotifyPlace::Shm { name, namespace, .. } => native_name_for_shm(name, *namespace, index),
        }
    }
}

/// Windows names live in the object namespace: a file path is hashed
/// into one, a shared-memory name is prefixed like the ring's regions.
#[cfg(windows)]
fn native_name_for_path(base: &Path, index: u32) -> String {
    format!("Local\\subetha_notify_{:016x}_{index}", fnv(base.to_string_lossy().as_bytes()))
}

#[cfg(windows)]
fn native_name_for_shm(name: &str, namespace: ShmNamespace, index: u32) -> String {
    let prefix = match namespace {
        ShmNamespace::Session => "Local",
        ShmNamespace::Machine => "Global",
    };
    let cleaned: String = name.chars().map(|c| if c == '/' || c == '\\' { '_' } else { c }).collect();
    format!("{prefix}\\subetha_{cleaned}_notify_{index}")
}

/// Unix notifiers are FIFOs on the filesystem: beside a file-backed ring,
/// and under the temp directory for a shared-memory ring.
#[cfg(unix)]
fn native_name_for_path(base: &Path, index: u32) -> String {
    let mut p = base.as_os_str().to_owned();
    p.push(format!(".notify.{index}"));
    p.to_string_lossy().into_owned()
}

#[cfg(unix)]
fn native_name_for_shm(name: &str, _namespace: ShmNamespace, index: u32) -> String {
    let cleaned: String = name.chars().map(|c| if c == '/' || c == '\\' { '_' } else { c }).collect();
    std::env::temp_dir()
        .join(format!("subetha_{cleaned}_notify_{index}"))
        .to_string_lossy()
        .into_owned()
}

#[cfg(windows)]
fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// The record's backing: a file, a shared-memory region, or this
/// process's memory.
enum RecordBacking {
    File { _file: std::fs::File, mmap: memmap2::MmapMut },
    /// The region and the start of its mapping, which is stable for the
    /// region's life.
    Shm { _shm: ShmFile, ptr: *const u8 },
    Anon(Box<NotifyRecord>),
}

/// The record a ring's producers and consumers share.
pub struct NotifyRecordHandle {
    backing: RecordBacking,
}

// SAFETY: the record is atomics behind a mapping the ring's processes
// share; the backing types are Send and Sync themselves.
unsafe impl Send for NotifyRecordHandle {}
unsafe impl Sync for NotifyRecordHandle {}

impl NotifyRecordHandle {
    fn record(&self) -> &NotifyRecord {
        match &self.backing {
            // SAFETY: the mapping is at least 64 bytes and page aligned;
            // the record was initialized before the magic was written.
            RecordBacking::File { mmap, .. } => unsafe { &*(mmap.as_ptr() as *const NotifyRecord) },
            RecordBacking::Shm { ptr, .. } => unsafe { &*(*ptr as *const NotifyRecord) },
            RecordBacking::Anon(record) => record,
        }
    }

    fn anon() -> Self {
        Self {
            backing: RecordBacking::Anon(Box::new(NotifyRecord {
                magic: NOTIFY_MAGIC,
                next_index: AtomicU32::new(0),
                generation: AtomicU64::new(0),
                attached: AtomicU32::new(0),
                _pad: [0; 44],
            })),
        }
    }

    /// Lay out an empty record: counters first, magic last, because
    /// attachers spin on it.
    ///
    /// # Safety
    /// `ptr` addresses at least 64 writable zeroed bytes.
    unsafe fn init(ptr: *mut u8) {
        let record = ptr as *mut NotifyRecord;
        unsafe {
            std::ptr::write(&raw mut (*record).next_index, AtomicU32::new(0));
            std::ptr::write(&raw mut (*record).generation, AtomicU64::new(0));
            std::ptr::write(&raw mut (*record).attached, AtomicU32::new(0));
            std::ptr::write_volatile(&raw mut (*record).magic, NOTIFY_MAGIC);
        }
    }

    fn file(base: &Path) -> Result<Self, NotifyError> {
        let mut path = base.as_os_str().to_owned();
        path.push(".notify.bin");
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            Path::new(&path),
            std::mem::size_of::<NotifyRecord>(),
            |ptr| unsafe { Self::init(ptr) },
            |ptr| unsafe { (*(ptr as *const NotifyRecord)).magic == NOTIFY_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, NotifyError::LayoutMismatch))?;
        Ok(Self { backing: RecordBacking::File { _file: file, mmap } })
    }

    fn shm(name: &str, namespace: ShmNamespace, sddl: Option<&str>) -> Result<Self, NotifyError> {
        let mut shm = ShmFile::create_or_open_named_secured(
            &format!("{name}_notify"),
            std::mem::size_of::<NotifyRecord>(),
            namespace,
            sddl,
        )?;
        let ptr = shm.as_mut_slice().as_mut_ptr();
        // SAFETY: the region is 64 zeroed bytes on creation; an existing
        // region carries the magic and is left as it is.
        unsafe {
            let magic = std::ptr::read_volatile(ptr as *const u32);
            if magic == 0 {
                Self::init(ptr);
            } else if magic != NOTIFY_MAGIC {
                return Err(NotifyError::LayoutMismatch);
            }
        }
        Ok(Self { backing: RecordBacking::Shm { _shm: shm, ptr } })
    }

    /// Every path a file-backed record under `base` and its notifiers
    /// occupy on this platform, for an unlink: the FIFO of every index the
    /// record ever handed out, then the record. Nothing when there is no
    /// record.
    pub fn file_paths_under(base: &Path) -> Vec<PathBuf> {
        let mut record_path = base.as_os_str().to_owned();
        record_path.push(".notify.bin");
        let record_path = PathBuf::from(record_path);
        let mut paths = Vec::new();
        // The record is read as bytes rather than attached, so an unlink
        // of a ring that never had a notifier creates nothing.
        let bytes = match std::fs::read(&record_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return paths,
            Err(e) => {
                eprintln!("subetha: notifier record {} not read: {e}", record_path.display());
                paths.push(record_path);
                return paths;
            }
        };
        if bytes.len() >= 8 && u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) == NOTIFY_MAGIC {
            let next = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
            paths.extend(fifo_paths(base, next));
        }
        paths.push(record_path);
        paths
    }
}

/// The FIFOs of the indexes `0..next` under `base`; none on Windows,
/// where a notifier is an event that goes with its last handle.
#[cfg(unix)]
fn fifo_paths(base: &Path, next: u32) -> Vec<PathBuf> {
    (0..next).map(|index| PathBuf::from(native_name_for_path(base, index))).collect()
}

#[cfg(windows)]
fn fifo_paths(_base: &Path, _next: u32) -> Vec<PathBuf> {
    Vec::new()
}

/// One write end a producer holds.
enum WriteEnd {
    #[cfg(unix)]
    Fifo(std::fs::File),
    #[cfg(windows)]
    Event(windows_sys::Win32::Foundation::HANDLE),
    /// The signal side of an anonymous notifier in this process.
    Anon(Arc<AnonSignal>),
}

// SAFETY: a FIFO file and an event handle are usable from any thread.
unsafe impl Send for WriteEnd {}

impl WriteEnd {
    /// Signal the notifier; false when it is gone and should be dropped.
    fn signal(&self) -> bool {
        match self {
            #[cfg(unix)]
            WriteEnd::Fifo(file) => write_one(file),
            #[cfg(windows)]
            WriteEnd::Event(handle) => unsafe { windows_sys::Win32::System::Threading::SetEvent(*handle) != 0 },
            WriteEnd::Anon(signal) => signal.signal(),
        }
    }
}

#[cfg(windows)]
impl Drop for WriteEnd {
    fn drop(&mut self) {
        if let WriteEnd::Event(handle) = self {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(*handle) };
        }
    }
}

/// One byte into a pipe; a full pipe is a notifier already signaled.
#[cfg(unix)]
fn write_one(mut file: &std::fs::File) -> bool {
    use std::io::Write;
    match file.write_all(&[1]) {
        Ok(()) => true,
        Err(e) => e.kind() == io::ErrorKind::WouldBlock,
    }
}

/// The signal side of an anonymous notifier: the write end of an unnamed
/// pipe on Unix, an unnamed event on Windows.
pub struct AnonSignal {
    #[cfg(unix)]
    write: std::fs::File,
    #[cfg(windows)]
    event: windows_sys::Win32::Foundation::HANDLE,
}

// SAFETY: usable from any thread, as above.
unsafe impl Send for AnonSignal {}
unsafe impl Sync for AnonSignal {}

impl AnonSignal {
    fn signal(&self) -> bool {
        #[cfg(unix)]
        {
            write_one(&self.write)
        }
        #[cfg(windows)]
        unsafe {
            windows_sys::Win32::System::Threading::SetEvent(self.event) != 0
        }
    }
}

#[cfg(windows)]
impl Drop for AnonSignal {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.event) };
    }
}

/// The anonymous notifiers of one set, shared with each notifier so a
/// detach removes its own signal side whichever is dropped first.
type AnonRegistry = Arc<Mutex<Vec<(u32, Arc<AnonSignal>)>>>;

/// The producers' side: everything attached to one ring, signaled on
/// every push.
pub struct NotifierSet {
    place: Option<NotifyPlace>,
    record: Arc<NotifyRecordHandle>,
    /// Write ends by notifier index, as of `scanned`.
    cache: Mutex<Vec<(u32, WriteEnd)>>,
    scanned: AtomicU64,
    anon: AnonRegistry,
}

impl NotifierSet {
    /// The set for an anonymous ring: every notifier is in this process.
    pub fn anon() -> Self {
        Self {
            place: None,
            record: Arc::new(NotifyRecordHandle::anon()),
            cache: Mutex::new(Vec::new()),
            scanned: AtomicU64::new(0),
            anon: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The set for a file-backed ring under `base`.
    pub fn file(base: impl AsRef<Path>) -> Result<Self, NotifyError> {
        Ok(Self {
            place: Some(NotifyPlace::File(base.as_ref().to_path_buf())),
            record: Arc::new(NotifyRecordHandle::file(base.as_ref())?),
            cache: Mutex::new(Vec::new()),
            scanned: AtomicU64::new(0),
            anon: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// The set for a shared-memory ring named `name`.
    pub fn shm(name: &str, namespace: ShmNamespace, sddl: Option<&str>) -> Result<Self, NotifyError> {
        Ok(Self {
            place: Some(NotifyPlace::Shm { name: name.to_owned(), namespace, sddl: sddl.map(str::to_owned) }),
            record: Arc::new(NotifyRecordHandle::shm(name, namespace, sddl)?),
            cache: Mutex::new(Vec::new()),
            scanned: AtomicU64::new(0),
            anon: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Notifiers attached right now, by anyone.
    pub fn attached(&self) -> u32 {
        self.record.record().attached.load(Ordering::Acquire)
    }

    /// Signal every attached notifier. Returns how many were signaled.
    /// One atomic load when the set is unchanged since the last scan.
    pub fn signal(&self) -> usize {
        let record = self.record.record();
        let generation = record.generation.load(Ordering::Acquire);
        if generation != self.scanned.load(Ordering::Acquire) {
            self.rescan(generation);
        }
        let mut signaled = 0;
        let mut cache = lock(&self.cache);
        cache.retain(|(_, end)| {
            let ok = end.signal();
            if ok {
                signaled += 1;
            }
            ok
        });
        signaled
    }

    /// Open the write end of every notifier index the record has handed
    /// out and that still exists. The old write ends go first: on Windows
    /// a handle of ours would keep a detached notifier's event alive and
    /// the rescan would find it again.
    fn rescan(&self, generation: u64) {
        lock(&self.cache).clear();
        let record = self.record.record();
        let next = record.next_index.load(Ordering::Acquire);
        let mut fresh = Vec::new();
        match &self.place {
            None => {
                for (index, signal) in lock(&self.anon).iter() {
                    fresh.push((*index, WriteEnd::Anon(Arc::clone(signal))));
                }
            }
            Some(place) => {
                for index in 0..next {
                    if let Some(end) = open_write_end(&place.native_name(index)) {
                        fresh.push((index, end));
                    }
                }
            }
        }
        *lock(&self.cache) = fresh;
        self.scanned.store(generation, Ordering::Release);
    }

    /// Attach a new notifier to this ring for the calling process to poll.
    pub fn attach(&self) -> Result<Notifier, NotifyError> {
        let record = self.record.record();
        let index = record.next_index.fetch_add(1, Ordering::AcqRel);
        let inner = match &self.place {
            None => {
                let (read, signal) = create_anon()?;
                let signal = Arc::new(signal);
                lock(&self.anon).push((index, Arc::clone(&signal)));
                NotifierInner::Anon { read, registry: Arc::clone(&self.anon) }
            }
            Some(place) => NotifierInner::Named(create_named(place, index)?),
        };
        record.attached.fetch_add(1, Ordering::AcqRel);
        record.generation.fetch_add(1, Ordering::AcqRel);
        Ok(Notifier { index, inner, record: Arc::clone(&self.record) })
    }
}

/// The consumer's side: one pollable object.
pub struct Notifier {
    index: u32,
    inner: NotifierInner,
    record: Arc<NotifyRecordHandle>,
}

// SAFETY: the native objects are usable from any thread.
unsafe impl Send for Notifier {}
unsafe impl Sync for Notifier {}

enum NotifierInner {
    Anon { read: NativeRead, registry: AnonRegistry },
    Named(NamedNotifier),
}

/// What the consumer polls.
enum NativeRead {
    #[cfg(unix)]
    Fd(std::fs::File),
    #[cfg(windows)]
    Event(windows_sys::Win32::Foundation::HANDLE),
}

impl NativeRead {
    fn native(&self) -> u64 {
        match self {
            #[cfg(unix)]
            NativeRead::Fd(file) => {
                use std::os::unix::io::AsRawFd;
                file.as_raw_fd() as u64
            }
            #[cfg(windows)]
            NativeRead::Event(handle) => *handle as usize as u64,
        }
    }

    /// Consume every pending signal.
    fn drain(&self) {
        match self {
            #[cfg(unix)]
            NativeRead::Fd(file) => {
                use std::io::Read;
                let mut file = file;
                let mut buf = [0u8; 256];
                loop {
                    match file.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => {
                            if e.kind() != io::ErrorKind::WouldBlock {
                                eprintln!("subetha: notifier drain stopped: {e}");
                            }
                            break;
                        }
                    }
                }
            }
            #[cfg(windows)]
            NativeRead::Event(handle) => unsafe {
                windows_sys::Win32::System::Threading::ResetEvent(*handle);
            },
        }
    }
}

#[cfg(windows)]
impl Drop for NativeRead {
    fn drop(&mut self) {
        let NativeRead::Event(handle) = self;
        unsafe { windows_sys::Win32::Foundation::CloseHandle(*handle) };
    }
}

struct NamedNotifier {
    read: NativeRead,
    /// The FIFO path to unlink on detach; nothing on Windows, where the
    /// event goes with its last handle.
    #[cfg(unix)]
    path: PathBuf,
}

impl Notifier {
    /// The index this notifier holds in the ring's record.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// The native object: a file descriptor on Unix, an event `HANDLE` on
    /// Windows, as an integer.
    pub fn native(&self) -> u64 {
        match &self.inner {
            NotifierInner::Anon { read, .. } => read.native(),
            NotifierInner::Named(named) => named.read.native(),
        }
    }

    /// Consume every pending signal, so the next poll waits for the next
    /// push.
    pub fn drain(&self) {
        match &self.inner {
            NotifierInner::Anon { read, .. } => read.drain(),
            NotifierInner::Named(named) => named.read.drain(),
        }
    }

    /// Whether a signal is pending right now, without waiting.
    pub fn is_signaled(&self) -> bool {
        wait_native(self.native(), 0)
    }

    /// Wait up to `timeout_ms` for a signal; -1 waits without a deadline.
    /// Returns whether one arrived. An event loop's own wait is what a
    /// consumer normally uses; this serves tests and simple callers.
    pub fn wait(&self, timeout_ms: i32) -> bool {
        wait_native(self.native(), timeout_ms)
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        match &self.inner {
            NotifierInner::Anon { registry, .. } => {
                lock(registry).retain(|(index, _)| *index != self.index);
            }
            NotifierInner::Named(named) => {
                #[cfg(unix)]
                match std::fs::remove_file(&named.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => eprintln!("subetha: notifier {} not removed: {e}", named.path.display()),
                }
                #[cfg(windows)]
                let _named = named;
            }
        }
        let record = self.record.record();
        record.attached.fetch_sub(1, Ordering::AcqRel);
        record.generation.fetch_add(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------- unix --

#[cfg(unix)]
fn create_anon() -> Result<(NativeRead, AnonSignal), NotifyError> {
    use std::os::unix::io::FromRawFd;
    let mut fds = [0i32; 2];
    // SAFETY: fds is a valid two-element array.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: both fds are pipe ends this process just created and owns.
    let read = unsafe { std::fs::File::from_raw_fd(fds[0]) };
    let write = unsafe { std::fs::File::from_raw_fd(fds[1]) };
    for fd in fds {
        // SAFETY: fd is one of the two ends above.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    Ok((NativeRead::Fd(read), AnonSignal { write }))
}

#[cfg(unix)]
fn create_named(place: &NotifyPlace, index: u32) -> Result<NamedNotifier, NotifyError> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = PathBuf::from(place.native_name(index));
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|e| NotifyError::Io(io::ErrorKind::InvalidInput, e.to_string()))?;
    // SAFETY: c_path is a NUL-terminated path.
    if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::AlreadyExists {
            return Err(e.into());
        }
    }
    let read = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(&path)?;
    Ok(NamedNotifier { read: NativeRead::Fd(read), path })
}

/// The write end of the FIFO at `name`, opened for both reading and
/// writing so a write never finds the reader gone; `None` when the FIFO
/// is not there, which is a detached notifier.
#[cfg(unix)]
fn open_write_end(name: &str) -> Option<WriteEnd> {
    use std::os::unix::fs::OpenOptionsExt;
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(name)
    {
        Ok(file) => Some(WriteEnd::Fifo(file)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => {
            eprintln!("subetha: notifier {name} not opened for signaling: {e}");
            None
        }
    }
}

#[cfg(unix)]
fn wait_native(native: u64, timeout_ms: i32) -> bool {
    let mut fds = libc::pollfd { fd: native as i32, events: libc::POLLIN, revents: 0 };
    // SAFETY: fds is one valid pollfd.
    let rc = unsafe { libc::poll(&mut fds, 1, timeout_ms) };
    rc > 0 && (fds.revents & libc::POLLIN) != 0
}

// ------------------------------------------------------------- windows --

#[cfg(windows)]
fn create_event(name: Option<&str>, sddl: Option<&str>) -> Result<windows_sys::Win32::Foundation::HANDLE, NotifyError> {
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
    use windows_sys::Win32::System::Threading::CreateEventW;

    let mut sd = core::ptr::null_mut();
    if let Some(s) = sddl {
        let wide_sddl: Vec<u16> = s.encode_utf16().chain(Some(0)).collect();
        // SAFETY: wide_sddl is NUL-terminated; sd receives a descriptor
        // LocalFree releases below.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(wide_sddl.as_ptr(), SDDL_REVISION_1, &mut sd, core::ptr::null_mut())
        };
        if ok == 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    let sa = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: sd,
        bInheritHandle: 0,
    };
    let sa_ptr: *const SECURITY_ATTRIBUTES = if sddl.is_some() { &sa } else { core::ptr::null() };
    let wide: Vec<u16>;
    let name_ptr = match name {
        Some(n) => {
            wide = n.encode_utf16().chain(Some(0)).collect();
            wide.as_ptr()
        }
        None => core::ptr::null(),
    };
    // A manual-reset event: a signal stays until a drain resets it, the
    // way a byte stays in a FIFO until it is read, so a wait does not
    // consume what a later poll should still see.
    // SAFETY: the attributes and the name are valid for the call.
    let handle = unsafe { CreateEventW(sa_ptr, 1, 0, name_ptr) };
    let err = io::Error::last_os_error();
    if !sd.is_null() {
        unsafe { LocalFree(sd as _) };
    }
    if handle.is_null() {
        return Err(err.into());
    }
    Ok(handle)
}

#[cfg(windows)]
fn create_anon() -> Result<(NativeRead, AnonSignal), NotifyError> {
    use windows_sys::Win32::Foundation::{DuplicateHandle, DUPLICATE_SAME_ACCESS};
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let event = create_event(None, None)?;
    let mut signal = core::ptr::null_mut();
    // SAFETY: event is a live handle of this process.
    let ok = unsafe {
        DuplicateHandle(GetCurrentProcess(), event, GetCurrentProcess(), &mut signal, 0, 0, DUPLICATE_SAME_ACCESS)
    };
    if ok == 0 {
        let e = io::Error::last_os_error();
        unsafe { windows_sys::Win32::Foundation::CloseHandle(event) };
        return Err(e.into());
    }
    Ok((NativeRead::Event(event), AnonSignal { event: signal }))
}

#[cfg(windows)]
fn create_named(place: &NotifyPlace, index: u32) -> Result<NamedNotifier, NotifyError> {
    let sddl = match place {
        NotifyPlace::Shm { sddl, .. } => sddl.as_deref(),
        NotifyPlace::File(_) => None,
    };
    let event = create_event(Some(&place.native_name(index)), sddl)?;
    Ok(NamedNotifier { read: NativeRead::Event(event) })
}

/// A handle on the event named `name` with the right to set it; `None`
/// when no such event exists, which is a detached notifier.
#[cfg(windows)]
fn open_write_end(name: &str) -> Option<WriteEnd> {
    use windows_sys::Win32::System::Threading::{OpenEventW, EVENT_MODIFY_STATE};
    let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    // SAFETY: wide is a NUL-terminated name.
    let handle = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
    if handle.is_null() {
        None
    } else {
        Some(WriteEnd::Event(handle))
    }
}

#[cfg(windows)]
fn wait_native(native: u64, timeout_ms: i32) -> bool {
    use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
    use windows_sys::Win32::System::Threading::{WaitForSingleObject, INFINITE};
    let timeout = if timeout_ms < 0 { INFINITE } else { timeout_ms as u32 };
    // SAFETY: native is an event handle this process holds.
    let rc = unsafe { WaitForSingleObject(native as usize as _, timeout) };
    rc == WAIT_OBJECT_0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-notify-{name}-{}", std::process::id()))
    }

    #[test]
    fn an_anonymous_notifier_is_signaled_by_a_push_in_another_thread() {
        let set = Arc::new(NotifierSet::anon());
        let notifier = set.attach().unwrap();
        assert_eq!(set.attached(), 1);
        assert!(!notifier.is_signaled());
        assert!(!notifier.wait(20));
        let producer = {
            let set = Arc::clone(&set);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(30));
                set.signal()
            })
        };
        assert!(notifier.wait(5000), "a signal arrives from the producer thread");
        assert_eq!(producer.join().unwrap(), 1);
        notifier.drain();
        assert!(!notifier.is_signaled());
        drop(notifier);
        assert_eq!(set.attached(), 0);
        assert_eq!(set.signal(), 0);
    }

    #[test]
    fn a_file_notifier_is_signaled_through_a_second_set_on_the_same_base() {
        let base = tmp("file");
        let consumer_set = NotifierSet::file(&base).unwrap();
        let producer_set = NotifierSet::file(&base).unwrap();
        assert_eq!(producer_set.signal(), 0, "nothing attached yet");
        let notifier = consumer_set.attach().unwrap();
        assert_eq!(producer_set.attached(), 1);
        assert_eq!(producer_set.signal(), 1, "the producer set finds the new notifier");
        assert!(notifier.wait(1000));
        notifier.drain();
        assert!(!notifier.is_signaled());
        let second = consumer_set.attach().unwrap();
        assert_eq!(producer_set.signal(), 2);
        assert!(notifier.wait(1000) && second.wait(1000));
        drop(notifier);
        assert_eq!(producer_set.signal(), 1, "a detached notifier is dropped at the next scan");
        drop(second);
        assert_eq!(producer_set.signal(), 0);
        let mut record = base.as_os_str().to_owned();
        record.push(".notify.bin");
        drop(producer_set);
        drop(consumer_set);
        std::fs::remove_file(record).unwrap();
    }

    #[test]
    fn a_shared_memory_notifier_is_signaled_across_sets() {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let name = format!("subetha_notify_test_{}_{:x}", std::process::id(), nanos & 0xffff_ffff);
        let consumer_set = NotifierSet::shm(&name, ShmNamespace::Session, None).unwrap();
        let producer_set = NotifierSet::shm(&name, ShmNamespace::Session, None).unwrap();
        let notifier = consumer_set.attach().unwrap();
        assert_eq!(producer_set.signal(), 1);
        assert!(notifier.wait(1000));
        drop(notifier);
        assert_eq!(producer_set.signal(), 0);
    }
}
