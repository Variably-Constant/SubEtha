//! `fd_handoff`: live cross-process handle handoff (cross-platform).
//!
//! Hands a live OS handle to a shared kernel object from one process to
//! another, so the receiver attaches to the SAME object WITHOUT
//! re-opening it by name / path. Only the irreducible per-OS syscall is
//! gated; the verb-pair shape is shared:
//!
//! - Unix (`#[cfg(unix)]`): SCM_RIGHTS over a UNIX domain socket. The
//!   sender's `sendmsg` carries the fd in ancillary data; the receiver's
//!   `recvmsg` gets a duplicated fd referring to the same kernel file
//!   table entry. Pair: `send_fd` / `recv_fd` (+ `accept_one` /
//!   `connect` UDS helpers).
//! - Windows (`#[cfg(windows)]`): `DuplicateHandle` into the target
//!   process. The sender injects the handle into the receiver's handle
//!   table (via `OpenProcess(PROCESS_DUP_HANDLE)`) and sends the
//!   resulting target-valid handle VALUE over any byte stream; the
//!   receiver uses it directly. Pair: [`send_handle`] / [`recv_handle`]
//!   (+ [`create_anon_mapping`] / [`map_handle`] for an anonymous,
//!   nameless shared region - the analogue of a Unix memfd).
//!
//! For the substrate this enables a sender process to hand the
//! underlying handle of a shared region (ShmFile / file-backed
//! `SpscRingCore` on unix, a file mapping on Windows) to a receiver,
//! which attaches and observes the same region without opening it by
//! path. Per the substrate's design choice this is a VERB pair (send /
//! recv on existing rings), NOT a new Locale variant; the locale axis
//! stays at three members (Anon / ShmFs / File).

#![cfg(any(unix, windows))]

// ---------------------------------------------------------------------
// Unix: SCM_RIGHTS fd passing over a UNIX domain socket.
// ---------------------------------------------------------------------

#[cfg(unix)]
mod scm_rights {
    use std::io;
    use std::os::unix::io::{AsRawFd, RawFd};
    use std::os::unix::net::UnixStream;
    use std::path::Path;

    /// Send a single file descriptor over an established UnixStream.
    /// Uses libc::sendmsg with an SCM_RIGHTS ancillary message.
    pub fn send_fd(stream: &UnixStream, fd: RawFd) -> io::Result<()> {
        let stream_fd = stream.as_raw_fd();
        let dummy = [0u8; 1];
        let mut iov = libc::iovec {
            iov_base: dummy.as_ptr() as *mut libc::c_void,
            iov_len: 1,
        };

        let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
        let mut cmsg_buf = vec![0u8; cmsg_space];

        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;

        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            if cmsg.is_null() {
                return Err(io::Error::other("CMSG_FIRSTHDR returned null"));
            }
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
            std::ptr::copy_nonoverlapping(
                &fd as *const RawFd,
                libc::CMSG_DATA(cmsg) as *mut RawFd,
                1,
            );
        }

        let n = unsafe { libc::sendmsg(stream_fd, &msg, 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Receive a single file descriptor from an established UnixStream.
    /// Blocks until the peer's sendmsg arrives. Returns the duplicated
    /// fd that the receiver now owns (caller is responsible for
    /// closing it / wrapping in File).
    pub fn recv_fd(stream: &UnixStream) -> io::Result<RawFd> {
        let stream_fd = stream.as_raw_fd();
        let mut dummy = [0u8; 1];
        let mut iov = libc::iovec {
            iov_base: dummy.as_mut_ptr() as *mut libc::c_void,
            iov_len: 1,
        };

        let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
        let mut cmsg_buf = vec![0u8; cmsg_space];

        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;

        let n = unsafe { libc::recvmsg(stream_fd, &mut msg, 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        if cmsg.is_null() {
            return Err(io::Error::other("no SCM_RIGHTS ancillary data"));
        }
        let level = unsafe { (*cmsg).cmsg_level };
        let ctype = unsafe { (*cmsg).cmsg_type };
        if level != libc::SOL_SOCKET || ctype != libc::SCM_RIGHTS {
            return Err(io::Error::other("unexpected cmsg type"));
        }
        let fd = unsafe {
            let data = libc::CMSG_DATA(cmsg) as *const RawFd;
            std::ptr::read(data)
        };
        if fd < 0 {
            return Err(io::Error::other("received negative fd"));
        }
        Ok(fd)
    }

    /// Bind a UDS, accept one connection, return the connected stream.
    pub fn accept_one(uds_path: impl AsRef<Path>) -> io::Result<UnixStream> {
        let path = uds_path.as_ref();
        std::fs::remove_file(path).ok();
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        let (stream, _) = listener.accept()?;
        Ok(stream)
    }

    /// Connect to a UDS, return the stream. Caller passes the same path
    /// the server `accept_one`'d on.
    pub fn connect(uds_path: impl AsRef<Path>) -> io::Result<UnixStream> {
        UnixStream::connect(uds_path.as_ref())
    }
}

#[cfg(unix)]
pub use scm_rights::*;

// ---------------------------------------------------------------------
// Windows: DuplicateHandle-based handle handoff. The handle is injected
// into the target process's table and its target-valid value is sent
// over any byte stream - the parallel to pushing an fd through
// SCM_RIGHTS ancillary data.
// ---------------------------------------------------------------------

#[cfg(windows)]
mod dup_handle {
    use std::io::{self, Read, Write};
    use windows_sys::Win32::Foundation::{
        CloseHandle, DuplicateHandle, DUPLICATE_SAME_ACCESS, HANDLE,
        INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Memory::{
        CreateFileMappingW, MapViewOfFile, UnmapViewOfFile, FILE_MAP_ALL_ACCESS,
        MEMORY_MAPPED_VIEW_ADDRESS, PAGE_READWRITE, SEC_COMMIT,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, PROCESS_DUP_HANDLE,
    };

    /// Create an anonymous (pagefile-backed, NO name) shared file
    /// mapping of `size` bytes - the Windows analogue of a Unix memfd.
    /// Because it has no name, the only way a peer reaches the region is
    /// the handle handed over by [`send_handle`]. Returns the handle as
    /// an integer value (mirroring the unix `RawFd`), so the public API
    /// carries no raw pointer.
    pub fn create_anon_mapping(size: usize) -> io::Result<u64> {
        assert!(size > 0);
        let len = size as u64;
        let handle = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE, // pagefile-backed
                std::ptr::null(),
                PAGE_READWRITE | SEC_COMMIT,
                (len >> 32) as u32,
                (len & 0xFFFF_FFFF) as u32,
                std::ptr::null(), // NO name -> anonymous
            )
        };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(handle as usize as u64)
        }
    }

    /// Map an entire file-mapping handle into this process; returns the
    /// base pointer. Used by the sender after [`create_anon_mapping`]
    /// and by the receiver after [`recv_handle`].
    pub fn map_handle(handle: u64, size: usize) -> io::Result<*mut u8> {
        let h = handle as usize as HANDLE;
        let view = unsafe { MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, size) };
        if view.Value.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(view.Value as *mut u8)
        }
    }

    /// Unmap a base pointer previously returned by [`map_handle`].
    pub fn unmap(base: *mut u8) {
        unsafe {
            UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
                Value: base as *mut core::ffi::c_void,
            });
        }
    }

    /// Close a handle from [`create_anon_mapping`] / [`recv_handle`].
    pub fn close_handle(handle: u64) {
        unsafe {
            CloseHandle(handle as usize as HANDLE);
        }
    }

    /// Duplicate `handle` INTO the process `target_pid` and write the
    /// resulting target-valid handle value over `stream` (8 LE bytes).
    /// The Windows parallel to SCM_RIGHTS: rather than pushing the
    /// handle through ancillary socket data, inject it into the target's
    /// handle table and transmit the integer value.
    pub fn send_handle<W: Write>(
        stream: &mut W,
        handle: u64,
        target_pid: u32,
    ) -> io::Result<()> {
        let target = unsafe { OpenProcess(PROCESS_DUP_HANDLE, 0, target_pid) };
        if target.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut dup: HANDLE = std::ptr::null_mut();
        let ok = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                handle as usize as HANDLE,
                target,
                &mut dup,
                0,
                0, // not inheritable
                DUPLICATE_SAME_ACCESS,
            )
        };
        unsafe {
            CloseHandle(target);
        }
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let value = dup as usize as u64;
        stream.write_all(&value.to_le_bytes())?;
        Ok(())
    }

    /// Receive a handle value (8 LE bytes) from `stream`. Because the
    /// sender duplicated the handle INTO this process, the value is a
    /// valid handle here; map it with [`map_handle`].
    pub fn recv_handle<R: Read>(stream: &mut R) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

#[cfg(windows)]
pub use dup_handle::*;

#[cfg(all(test, unix))]
mod tests_unix {
    use super::*;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::UnixStream;

    /// Over an in-process socketpair, a fd sent via SCM_RIGHTS arrives as
    /// a duplicate that refers to the SAME kernel file entry: data written
    /// through one fd is visible through the other.
    #[test]
    fn scm_rights_fd_shares_the_kernel_file() {
        let (a, b) = UnixStream::pair().expect("socketpair");
        let path = std::env::temp_dir().join(format!(
            "fdhandoff_ut_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("open");
        f.write_all(b"shared-via-scm-rights").expect("write");
        f.flush().expect("flush");

        send_fd(&a, f.as_raw_fd()).expect("send_fd");
        let dup = recv_fd(&b).expect("recv_fd");
        assert!(dup >= 0 && dup != f.as_raw_fd(), "got a distinct dup'd fd");

        // Read the original content through the duplicated fd (offset 0).
        let mut buf = [0u8; 21];
        let n = unsafe {
            libc::pread(dup, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
        };
        assert_eq!(n, 21, "pread via dup'd fd");
        assert_eq!(&buf, b"shared-via-scm-rights");

        // Append through the dup, observe it through the ORIGINAL fd: same
        // kernel file entry, not a copy.
        let m = unsafe {
            libc::pwrite(dup, b"!".as_ptr() as *const libc::c_void, 1, 21)
        };
        assert_eq!(m, 1);
        let mut buf2 = [0u8; 22];
        let r = unsafe {
            libc::pread(f.as_raw_fd(), buf2.as_mut_ptr() as *mut libc::c_void, 22, 0)
        };
        assert_eq!(r, 22);
        assert_eq!(&buf2, b"shared-via-scm-rights!");

        unsafe { libc::close(dup) };
        std::fs::remove_file(&path).ok();
    }

    /// `recv_fd` on a stream that carried no ancillary data is an error,
    /// not a silent bogus fd.
    #[test]
    fn recv_without_scm_rights_errs() {
        let (a, b) = UnixStream::pair().expect("socketpair");
        // Plain byte, no SCM_RIGHTS.
        (&a).write_all(b"x").expect("write");
        assert!(recv_fd(&b).is_err(), "no ancillary data must error");
    }
}

#[cfg(all(test, windows))]
mod tests_windows {
    use super::*;
    use std::io::Cursor;

    /// In-process proof of the duplicate-into-target mechanism: duplicate
    /// an anonymous mapping handle into THIS process (target_pid = our
    /// own pid), then map the received handle. A write through one view
    /// is visible through the other - the same kernel section object, not
    /// a copy. (Cross-process coverage is `fd_handoff_xproc`.)
    #[test]
    fn dup_handle_shares_the_kernel_section() {
        const SIZE: usize = 4096;
        let handle = create_anon_mapping(SIZE).expect("create_anon_mapping");
        let base_a = map_handle(handle, SIZE).expect("map original");

        // Duplicate into our own process and ferry the value through a
        // byte buffer, exactly as the cross-process path would.
        let mut chan = Cursor::new(Vec::new());
        send_handle(&mut chan, handle, std::process::id()).expect("send_handle");
        chan.set_position(0);
        let dup = recv_handle(&mut chan).expect("recv_handle");
        assert_ne!(dup, handle, "got a distinct dup'd handle");
        let base_b = map_handle(dup, SIZE).expect("map dup");

        // Write through A, read through B.
        let marker = b"dup-handle-shared";
        unsafe {
            std::ptr::copy_nonoverlapping(marker.as_ptr(), base_a, marker.len());
        }
        let got = unsafe { std::slice::from_raw_parts(base_b, marker.len()) };
        assert_eq!(got, marker, "B must see A's write (same section object)");

        // And the reverse direction at a different offset.
        let marker2 = b"reverse";
        unsafe {
            std::ptr::copy_nonoverlapping(marker2.as_ptr(), base_b.add(64), marker2.len());
        }
        let got2 = unsafe { std::slice::from_raw_parts(base_a.add(64), marker2.len()) };
        assert_eq!(got2, marker2, "A must see B's write");

        unmap(base_a);
        unmap(base_b);
        close_handle(dup);
        close_handle(handle);
    }
}
