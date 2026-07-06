//! `kernel_async_ring`: the kernel async-I/O ring (io_uring on Linux,
//! IoRing on Windows, POSIX aio on FreeBSD / macOS) exposed as a
//! substrate ring primitive.
//!
//! Both OSes ship the same architecture - a user->kernel SUBMISSION ring
//! and a kernel->user COMPLETION ring - which is exactly the substrate's
//! SharedRing shape applied to the user/kernel boundary. This wraps that
//! kernel object behind one cross-platform surface; only the ring
//! syscalls are gated, the verb shape (prepare / submit / reap) and the
//! normalized [`Completion`] are shared:
//!
//! - Linux (`#[cfg(target_os = "linux")]`): `io_uring` via the
//!   mainline `io-uring` crate (`IoUring::new` / `opcode::Read` /
//!   `submit_and_wait` / completion iterator).
//! - Windows (`#[cfg(windows)]`): `IoRing` via `windows-sys`
//!   (`CreateIoRing` / `BuildIoRingReadFile` / `SubmitIoRing` /
//!   `PopIoRingCompletion`), gated on `QueryIoRingCapabilities` +
//!   `IsIoRingOpSupported` so an unsupported build degrades to `Err`
//!   rather than UB.
//! - FreeBSD (`#[cfg(target_os = "freebsd")]`): POSIX `aio` with kqueue
//!   completion. Each `aio_read` carries an `aio_sigevent` set to
//!   `SIGEV_KEVENT` against the ring's kqueue, so completion posts an
//!   `EVFILT_AIO` kevent (`ident` = the aiocb pointer, `udata` = the
//!   caller's tag); `submit_and_wait` is a `kevent` wait, `reap` calls
//!   `aio_return`. There is no separate batched submit - `aio_read`
//!   issues each op immediately - but the prepare / wait / reap verb
//!   shape is identical.
//! - macOS (`#[cfg(target_os = "macos")]`): POSIX `aio` with
//!   `aio_suspend` completion. Darwin has no `SIGEV_KEVENT` and its
//!   `EVFILT_AIO` kqueue filter rejects registration, so completion is
//!   driven by `aio_suspend` over the in-flight set rather than a
//!   `kevent` wait; `aio_read` still does the real kernel async I/O and
//!   `reap` calls `aio_return`. Same prepare / wait / reap verb shape.
//!
//! The completion encodings differ - io_uring packs bytes-or-`-errno`
//! into one `i32`; IoRing splits `ResultCode` (HRESULT) and
//! `Information` (bytes); aio reports via `aio_error` + `aio_return` - so
//! all three are normalized to a single [`Completion`] with an
//! `io::Result<usize>` byte count.

#![cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))]

use std::io;

/// One reaped completion: which submission it answers (`user_data`, the
/// tag the caller passed to `prepare_read`) and its result - the number
/// of bytes transferred, or the error the kernel reported.
pub struct Completion {
    pub user_data: u64,
    pub bytes: io::Result<usize>,
}

/// Open a file for reading through the kernel async ring. On Windows the
/// handle must carry `FILE_FLAG_OVERLAPPED` for `IoRing`; on Linux and
/// FreeBSD a plain read handle is fine. Keep the returned `File` alive
/// while its reads are in flight.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub fn open_for_async_read(path: impl AsRef<std::path::Path>) -> io::Result<std::fs::File> {
    std::fs::File::open(path)
}

/// Open a file for reading through the kernel async ring. The handle is
/// opened with `FILE_FLAG_OVERLAPPED`, required for `IoRing` operations.
/// Keep the returned `File` alive while its reads are in flight.
#[cfg(windows)]
pub fn open_for_async_read(path: impl AsRef<std::path::Path>) -> io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(path)
}

// ---------------------------------------------------------------------
// Linux: io_uring via the mainline `io-uring` crate.
// ---------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod linux_impl {
    use super::Completion;
    use io_uring::{opcode, types, IoUring};
    use std::io;

    /// A kernel async-I/O ring backed by an `io_uring` instance.
    pub struct KernelAsyncRing {
        ring: IoUring,
    }

    impl KernelAsyncRing {
        /// Create a ring with `entries` submission slots. Fails on kernels
        /// without io_uring (e.g. some containers) so callers can fall
        /// back to synchronous I/O.
        pub fn new(entries: u32) -> io::Result<Self> {
            Ok(Self { ring: IoUring::new(entries)? })
        }

        /// Queue a read of `len` bytes from `file` at `offset` into `buf`,
        /// tagged with `user_data`. Open `file` with
        /// [`open_for_async_read`](super::open_for_async_read).
        ///
        /// # Safety
        /// `buf` must remain valid and not move until the matching
        /// completion is reaped - the kernel writes into it asynchronously.
        pub unsafe fn prepare_read(
            &mut self,
            file: &std::fs::File,
            buf: *mut u8,
            len: u32,
            offset: u64,
            user_data: u64,
        ) -> io::Result<()> {
            use std::os::unix::io::AsRawFd;
            let entry = opcode::Read::new(types::Fd(file.as_raw_fd()), buf, len)
                .offset(offset)
                .build()
                .user_data(user_data);
            // SAFETY: `entry` references `buf`, which the caller pledges to
            // keep valid until the completion is reaped (this fn's contract).
            unsafe {
                self.ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("io_uring submission queue full"))?;
            }
            Ok(())
        }

        /// Submit all queued ops and block until at least `want` complete.
        pub fn submit_and_wait(&mut self, want: u32) -> io::Result<u32> {
            self.ring.submit_and_wait(want as usize).map(|n| n as u32)
        }

        /// Reap one completion if available.
        pub fn reap(&mut self) -> Option<Completion> {
            self.ring.completion().next().map(|cqe| {
                let r = cqe.result();
                let bytes = if r >= 0 {
                    Ok(r as usize)
                } else {
                    Err(io::Error::from_raw_os_error(-r))
                };
                Completion { user_data: cqe.user_data(), bytes }
            })
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::KernelAsyncRing;

// ---------------------------------------------------------------------
// FreeBSD: POSIX aio with kqueue (EVFILT_AIO) completion.
// ---------------------------------------------------------------------

#[cfg(target_os = "freebsd")]
mod freebsd_impl {
    use super::Completion;
    use std::collections::{HashMap, VecDeque};
    use std::io;
    use std::os::unix::io::AsRawFd;

    /// A kernel async-I/O ring backed by POSIX `aio` with kqueue
    /// completion. `aio_read` issues each op immediately with its
    /// `aio_sigevent` set to `SIGEV_KEVENT` against this ring's kqueue;
    /// `submit_and_wait` is a `kevent` wait and `reap` calls `aio_return`.
    pub struct KernelAsyncRing {
        kq: i32,
        // aiocbs in flight, keyed by their stable (boxed) address - which
        // is exactly the kevent `ident` the kernel reports on completion,
        // so the completion demux is a HashMap lookup.
        inflight: HashMap<usize, Box<libc::aiocb>>,
        // completions gathered by submit_and_wait, not yet reaped:
        // (aiocb address = kevent ident, user_data = kevent udata).
        ready: VecDeque<(usize, u64)>,
    }

    impl KernelAsyncRing {
        /// Create a ring (a kqueue). `entries` is a capacity hint for the
        /// in-flight + completion tracking maps. Fails if `kqueue(2)` does.
        pub fn new(entries: u32) -> io::Result<Self> {
            let kq = unsafe { libc::kqueue() };
            if kq < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                kq,
                inflight: HashMap::with_capacity(entries as usize),
                ready: VecDeque::with_capacity(entries as usize),
            })
        }

        /// Queue a read of `len` bytes from `file` at `offset` into `buf`,
        /// tagged with `user_data`. Open `file` with
        /// [`open_for_async_read`](super::open_for_async_read).
        ///
        /// # Safety
        /// `buf` must remain valid and not move until the matching
        /// completion is reaped - the kernel writes into it asynchronously.
        pub unsafe fn prepare_read(
            &mut self,
            file: &std::fs::File,
            buf: *mut u8,
            len: u32,
            offset: u64,
            user_data: u64,
        ) -> io::Result<()> {
            let mut cb: Box<libc::aiocb> = Box::new(unsafe { std::mem::zeroed() });
            cb.aio_fildes = file.as_raw_fd();
            cb.aio_buf = buf as *mut libc::c_void;
            cb.aio_nbytes = len as libc::size_t;
            cb.aio_offset = offset as libc::off_t;
            cb.aio_sigevent.sigev_notify = libc::SIGEV_KEVENT;
            // By FreeBSD convention sigev_notify_kqueue *is* sigev_signo
            // (see <sys/signal.h>); the completion's udata carries our tag.
            cb.aio_sigevent.sigev_signo = self.kq;
            cb.aio_sigevent.sigev_value.sival_ptr = user_data as usize as *mut libc::c_void;
            // Stable boxed address == the kevent `ident` reported back.
            let key = &*cb as *const libc::aiocb as usize;
            // SAFETY: `cb` is boxed (fixed address `key`) and held in
            // `inflight` until reaped; `buf` validity is the caller's
            // contract above.
            let rc = unsafe { libc::aio_read(&mut *cb) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
            self.inflight.insert(key, cb);
            Ok(())
        }

        /// Block until at least `want` of the in-flight reads complete,
        /// buffering their completions for [`reap`](Self::reap). `aio_read`
        /// already submitted them, so this only waits on the kqueue.
        pub fn submit_and_wait(&mut self, want: u32) -> io::Result<u32> {
            let target = (want as usize).min(self.inflight.len());
            let mut gathered = 0u32;
            while self.ready.len() < target {
                let cap = self.inflight.len().max(1);
                let mut evs: Vec<libc::kevent> =
                    (0..cap).map(|_| unsafe { std::mem::zeroed() }).collect();
                // NULL changelist: aio_read self-registered the knotes, we
                // only retrieve. NULL timeout: block until >= 1 posts.
                let n = unsafe {
                    libc::kevent(
                        self.kq,
                        std::ptr::null(),
                        0,
                        evs.as_mut_ptr(),
                        cap as libc::c_int,
                        std::ptr::null(),
                    )
                };
                if n < 0 {
                    let e = io::Error::last_os_error();
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(e);
                }
                for ev in &evs[..n as usize] {
                    self.ready.push_back((ev.ident, ev.udata as usize as u64));
                    gathered += 1;
                }
            }
            Ok(gathered)
        }

        /// Reap one gathered completion if available, retrieving its byte
        /// count via `aio_return` (or the error via `aio_error`).
        pub fn reap(&mut self) -> Option<Completion> {
            let (ident, user_data) = self.ready.pop_front()?;
            let mut cb = self.inflight.remove(&ident)?;
            let err = unsafe { libc::aio_error(&*cb) };
            let ret = unsafe { libc::aio_return(&mut *cb) };
            let bytes = if err == 0 {
                Ok(ret.max(0) as usize)
            } else {
                Err(io::Error::from_raw_os_error(err))
            };
            Some(Completion { user_data, bytes })
        }
    }

    impl Drop for KernelAsyncRing {
        fn drop(&mut self) {
            // Cancel + reclaim any still-in-flight aios so the kernel stops
            // referencing their about-to-be-freed aiocbs, then close kq.
            for cb in self.inflight.values_mut() {
                unsafe {
                    libc::aio_cancel(cb.aio_fildes, &mut **cb);
                    libc::aio_return(&mut **cb);
                }
            }
            unsafe { libc::close(self.kq) };
        }
    }
}

#[cfg(target_os = "freebsd")]
pub use freebsd_impl::KernelAsyncRing;

// ---------------------------------------------------------------------
// macOS: POSIX aio with aio_suspend completion.
//
// Darwin has no `SIGEV_KEVENT`, and its `EVFILT_AIO` kqueue filter rejects
// registration (`kevent` returns ENOTSUP), so the FreeBSD aio+kqueue path
// does not port. `aio_read` still performs the real kernel async I/O; the
// event-driven wait is `aio_suspend` over the in-flight set instead of a
// `kevent` wait. The prepare / submit / reap verb shape is identical.
// ---------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod macos_impl {
    use super::Completion;
    use std::collections::{HashMap, VecDeque};
    use std::io;
    use std::os::unix::io::AsRawFd;

    /// A kernel async-I/O ring backed by POSIX `aio` with `aio_suspend`
    /// completion. `aio_read` issues each op immediately; `submit_and_wait`
    /// blocks in `aio_suspend` until in-flight ops finish, and `reap` calls
    /// `aio_return`. The caller's `user_data` tag rides in each aiocb's
    /// `aio_sigevent.sigev_value` (with `SIGEV_NONE`), so completion demux
    /// needs no side table.
    pub struct KernelAsyncRing {
        // aiocbs in flight, keyed by their stable (boxed) address.
        inflight: HashMap<usize, Box<libc::aiocb>>,
        // addresses of completed-but-not-yet-reaped aiocbs.
        ready: VecDeque<usize>,
    }

    impl KernelAsyncRing {
        /// Create a ring. `entries` is a capacity hint for the in-flight +
        /// completion tracking maps. Infallible on macOS (no kernel object
        /// is created until the first `aio_read`).
        pub fn new(entries: u32) -> io::Result<Self> {
            Ok(Self {
                inflight: HashMap::with_capacity(entries as usize),
                ready: VecDeque::with_capacity(entries as usize),
            })
        }

        /// Queue a read of `len` bytes from `file` at `offset` into `buf`,
        /// tagged with `user_data`. Open `file` with
        /// [`open_for_async_read`](super::open_for_async_read).
        ///
        /// # Safety
        /// `buf` must remain valid and not move until the matching
        /// completion is reaped - the kernel writes into it asynchronously.
        pub unsafe fn prepare_read(
            &mut self,
            file: &std::fs::File,
            buf: *mut u8,
            len: u32,
            offset: u64,
            user_data: u64,
        ) -> io::Result<()> {
            let mut cb: Box<libc::aiocb> = Box::new(unsafe { std::mem::zeroed() });
            cb.aio_fildes = file.as_raw_fd();
            cb.aio_buf = buf as *mut libc::c_void;
            cb.aio_nbytes = len as libc::size_t;
            cb.aio_offset = offset as libc::off_t;
            // No completion event: aio_suspend polls the in-flight set. The
            // tag rides in sigev_value so reap recovers it without a side map.
            cb.aio_sigevent.sigev_notify = libc::SIGEV_NONE;
            cb.aio_sigevent.sigev_value.sival_ptr = user_data as usize as *mut libc::c_void;
            let key = &*cb as *const libc::aiocb as usize;
            // SAFETY: `cb` is boxed (fixed address `key`) and held in
            // `inflight` until reaped; `buf` validity is the caller's contract.
            let rc = unsafe { libc::aio_read(&mut *cb) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
            self.inflight.insert(key, cb);
            Ok(())
        }

        /// Block until at least `want` of the in-flight reads complete,
        /// buffering their completions for [`reap`](Self::reap).
        pub fn submit_and_wait(&mut self, want: u32) -> io::Result<u32> {
            let target = (want as usize).min(self.inflight.len());
            // Sweep any already-finished ops first.
            let mut gathered = self.harvest();
            while self.ready.len() < target {
                // Suspend on the in-flight ops not already harvested.
                let pending: Vec<*const libc::aiocb> = self
                    .inflight
                    .iter()
                    .filter_map(|(&k, cb)| {
                        if self.ready.contains(&k) {
                            None
                        } else {
                            Some(&**cb as *const libc::aiocb)
                        }
                    })
                    .collect();
                if pending.is_empty() {
                    break;
                }
                // SAFETY: every pointer references a live boxed aiocb still
                // owned by `inflight`. NULL timeout: block until >= 1 posts.
                let rc = unsafe {
                    libc::aio_suspend(pending.as_ptr(), pending.len() as libc::c_int, std::ptr::null())
                };
                if rc != 0 {
                    let e = io::Error::last_os_error();
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(e);
                }
                gathered += self.harvest();
            }
            Ok(gathered)
        }

        /// Move every in-flight aiocb whose `aio_error` is no longer
        /// `EINPROGRESS` into `ready`; returns how many were newly added.
        fn harvest(&mut self) -> u32 {
            let mut newly: Vec<usize> = Vec::new();
            for (&k, cb) in self.inflight.iter() {
                if self.ready.contains(&k) {
                    continue;
                }
                // SAFETY: `cb` is a live boxed aiocb owned by `inflight`.
                if unsafe { libc::aio_error(&**cb) } != libc::EINPROGRESS {
                    newly.push(k);
                }
            }
            let added = newly.len() as u32;
            for k in newly {
                self.ready.push_back(k);
            }
            added
        }

        /// Reap one gathered completion if available, retrieving its byte
        /// count via `aio_return` (or the error via `aio_error`).
        pub fn reap(&mut self) -> Option<Completion> {
            let key = self.ready.pop_front()?;
            let mut cb = self.inflight.remove(&key)?;
            let user_data = cb.aio_sigevent.sigev_value.sival_ptr as usize as u64;
            // SAFETY: `cb` is still a valid aiocb whose op has completed.
            let err = unsafe { libc::aio_error(&*cb) };
            let ret = unsafe { libc::aio_return(&mut *cb) };
            let bytes = if err == 0 {
                Ok(ret.max(0) as usize)
            } else {
                Err(io::Error::from_raw_os_error(err))
            };
            Some(Completion { user_data, bytes })
        }
    }

    impl Drop for KernelAsyncRing {
        fn drop(&mut self) {
            // Cancel + reclaim any still-in-flight aios so the kernel stops
            // referencing their about-to-be-freed aiocbs.
            for cb in self.inflight.values_mut() {
                unsafe {
                    libc::aio_cancel(cb.aio_fildes, &mut **cb);
                    libc::aio_return(&mut **cb);
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub use macos_impl::KernelAsyncRing;

// ---------------------------------------------------------------------
// Windows: IoRing via windows-sys.
// ---------------------------------------------------------------------

#[cfg(windows)]
mod windows_impl {
    use super::Completion;
    use std::io;
    use windows_sys::Win32::Storage::FileSystem::{
        BuildIoRingReadFile, CloseIoRing, CreateIoRing, IsIoRingOpSupported,
        PopIoRingCompletion, QueryIoRingCapabilities, SubmitIoRing, HIORING,
        IORING_BUFFER_REF, IORING_BUFFER_REF_0, IORING_CAPABILITIES, IORING_CQE,
        IORING_CREATE_ADVISORY_FLAGS_NONE, IORING_CREATE_FLAGS,
        IORING_CREATE_REQUIRED_FLAGS_NONE, IORING_HANDLE_REF, IORING_HANDLE_REF_0,
        IORING_OP_READ, IORING_REF_RAW,
    };

    const INFINITE: u32 = 0xFFFF_FFFF;

    /// A kernel async-I/O ring backed by a Windows `IoRing`.
    pub struct KernelAsyncRing {
        ring: HIORING,
    }

    impl KernelAsyncRing {
        /// Create a ring with `entries` submission slots. Queries the
        /// runtime IoRing capabilities to pick a supported version + clamp
        /// the queue sizes, and verifies READ is supported. Fails (so the
        /// caller can fall back) on Windows builds without IoRing.
        pub fn new(entries: u32) -> io::Result<Self> {
            let mut caps: IORING_CAPABILITIES = unsafe { std::mem::zeroed() };
            let hr = unsafe { QueryIoRingCapabilities(&mut caps) };
            if hr < 0 {
                return Err(io::Error::other(
                    "IoRing not supported on this Windows build",
                ));
            }
            let sq = entries.min(caps.MaxSubmissionQueueSize.max(1));
            let cq = entries
                .saturating_mul(2)
                .min(caps.MaxCompletionQueueSize.max(1));
            let flags = IORING_CREATE_FLAGS {
                Required: IORING_CREATE_REQUIRED_FLAGS_NONE,
                Advisory: IORING_CREATE_ADVISORY_FLAGS_NONE,
            };
            let mut ring: HIORING = std::ptr::null_mut();
            let hr = unsafe { CreateIoRing(caps.MaxVersion, flags, sq, cq, &mut ring) };
            if hr < 0 {
                return Err(io::Error::other(format!("CreateIoRing failed: {hr:#x}")));
            }
            if unsafe { IsIoRingOpSupported(ring, IORING_OP_READ) } == 0 {
                unsafe { CloseIoRing(ring) };
                return Err(io::Error::other("IoRing READ op not supported"));
            }
            Ok(Self { ring })
        }

        /// Queue a read of `len` bytes from `file` at `offset` into `buf`,
        /// tagged with `user_data`. Open `file` with
        /// [`open_for_async_read`](super::open_for_async_read) so its
        /// handle carries `FILE_FLAG_OVERLAPPED`.
        ///
        /// # Safety
        /// `buf` must remain valid and not move until the matching
        /// completion is reaped - the kernel writes into it asynchronously.
        pub unsafe fn prepare_read(
            &mut self,
            file: &std::fs::File,
            buf: *mut u8,
            len: u32,
            offset: u64,
            user_data: u64,
        ) -> io::Result<()> {
            use std::os::windows::io::AsRawHandle;
            let fileref = IORING_HANDLE_REF {
                Kind: IORING_REF_RAW,
                Handle: IORING_HANDLE_REF_0 {
                    Handle: file.as_raw_handle(),
                },
            };
            let dataref = IORING_BUFFER_REF {
                Kind: IORING_REF_RAW,
                Buffer: IORING_BUFFER_REF_0 {
                    Address: buf as *mut core::ffi::c_void,
                },
            };
            // SAFETY: `dataref` points at `buf`, which the caller pledges to
            // keep valid until the completion is reaped (this fn's contract);
            // `fileref` wraps a live file handle.
            let hr = unsafe {
                BuildIoRingReadFile(
                    self.ring,
                    fileref,
                    dataref,
                    len,
                    offset,
                    user_data as usize,
                    0, // IORING_SQE_FLAGS_NONE
                )
            };
            if hr < 0 {
                Err(io::Error::other(format!("BuildIoRingReadFile failed: {hr:#x}")))
            } else {
                Ok(())
            }
        }

        /// Submit all queued ops and block until at least `want` complete.
        pub fn submit_and_wait(&mut self, want: u32) -> io::Result<u32> {
            let mut submitted: u32 = 0;
            let hr = unsafe { SubmitIoRing(self.ring, want, INFINITE, &mut submitted) };
            if hr < 0 {
                Err(io::Error::other(format!("SubmitIoRing failed: {hr:#x}")))
            } else {
                Ok(submitted)
            }
        }

        /// Reap one completion if available. `PopIoRingCompletion` returns
        /// `S_OK` (0) when it popped one and `S_FALSE` (1) when the queue
        /// is empty.
        pub fn reap(&mut self) -> Option<Completion> {
            let mut cqe: IORING_CQE = unsafe { std::mem::zeroed() };
            let hr = unsafe { PopIoRingCompletion(self.ring, &mut cqe) };
            if hr != 0 {
                return None; // S_FALSE (empty) or an error
            }
            let bytes = if cqe.ResultCode >= 0 {
                Ok(cqe.Information)
            } else {
                Err(io::Error::other(format!(
                    "IoRing op failed: {:#x}",
                    cqe.ResultCode
                )))
            };
            Some(Completion { user_data: cqe.UserData as u64, bytes })
        }
    }

    impl Drop for KernelAsyncRing {
        fn drop(&mut self) {
            unsafe { CloseIoRing(self.ring) };
        }
    }
}

#[cfg(windows)]
pub use windows_impl::KernelAsyncRing;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Submit a single async read of a known file through the kernel ring
    /// and verify the reaped completion returns the exact bytes. Skips
    /// cleanly when the kernel ring is unavailable (old kernel / Windows
    /// build, or a sandbox without io_uring).
    #[test]
    fn single_async_read_round_trips() {
        let path = std::env::temp_dir().join(format!(
            "karing_ut_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let content = b"kernel-async-ring round trip payload";
        {
            let mut f = std::fs::File::create(&path).expect("create");
            f.write_all(content).expect("write");
            f.flush().expect("flush");
        }

        let mut ring = match KernelAsyncRing::new(8) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skipping: kernel async ring unavailable ({e})");
                std::fs::remove_file(&path).ok();
                return;
            }
        };
        let file = open_for_async_read(&path).expect("open_for_async_read");

        let mut buf = vec![0u8; content.len()];
        unsafe {
            ring.prepare_read(&file, buf.as_mut_ptr(), buf.len() as u32, 0, 0xAB)
                .expect("prepare_read");
        }
        let submitted = ring.submit_and_wait(1).expect("submit_and_wait");
        assert!(submitted >= 1, "at least one entry submitted");

        let c = ring.reap().expect("a completion");
        assert_eq!(c.user_data, 0xAB, "completion carries the submission tag");
        let n = c.bytes.expect("read succeeded");
        assert_eq!(n, content.len(), "read the whole payload");
        assert_eq!(&buf[..n], content, "bytes match the file content");

        drop(file);
        std::fs::remove_file(&path).ok();
    }
}
