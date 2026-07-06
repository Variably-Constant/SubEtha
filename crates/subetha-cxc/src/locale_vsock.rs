//! `locale_vsock`: host-VM byte streaming that bypasses the network
//! stack (cross-platform).
//!
//! A "remote-but-same-machine" locale member, between QUIC (cross-host)
//! and ShmFs (same-host shared memory): the hypervisor forwards bytes
//! between guest and host with no TCP/IP, DNS, or NIC involved. Only the
//! socket family is gated; the [`HostVmSocket`] stream API
//! (`listen_loopback` / `accept` / `connect_loopback` / `send` / `recv`)
//! is shared:
//!
//! - Linux (`#[cfg(target_os = "linux")]`): vsock(7) - `AF_VSOCK`
//!   `SOCK_STREAM`, addressed by `(cid, port)`. The raw `VsockSocket`
//!   exposes the explicit-CID primitive for real guest<->host links;
//!   `HostVmSocket` wraps it for the loopback path (`VMADDR_CID_LOCAL`).
//! - Windows (`#[cfg(windows)]`): Hyper-V sockets - `AF_HYPERV`
//!   `SOCK_STREAM` / `HV_PROTOCOL_RAW`, addressed by `(VmId, ServiceId)`
//!   GUIDs. The port maps to a ServiceId via the documented VSOCK
//!   template GUID; loopback uses `HV_GUID_LOOPBACK`.
//!
//! Both expose loopback (same-partition) streaming for same-host IPC and
//! self-test; the same code reaches a real guest/host by addressing a
//! peer CID / VmId instead of loopback. Loopback needs the kernel
//! `vsock_loopback` module (Linux) or the Hyper-V / Virtual Machine
//! Platform feature registering the `AF_HYPERV` provider (Windows); when
//! absent the constructors return `Err` so callers degrade gracefully.

#![cfg(any(target_os = "linux", windows))]

// ---------------------------------------------------------------------
// Linux: vsock(7) over AF_VSOCK.
// ---------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod vsock_impl {
    use std::io;
    use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

    /// CID values defined by vsock(7).
    pub const VMADDR_CID_ANY: u32 = 0xFFFFFFFF;
    pub const VMADDR_CID_HOST: u32 = 2;
    /// Loopback CID (same-host, no hypervisor transport): requires the
    /// `vsock_loopback` kernel module (Linux 5.6+).
    pub const VMADDR_CID_LOCAL: u32 = 1;

    /// A vsock SOCK_STREAM socket. Owns its fd; closes on Drop. This is
    /// the explicit-CID primitive; for the loopback path use the
    /// cross-platform [`HostVmSocket`].
    pub struct VsockSocket {
        fd: RawFd,
    }

    unsafe impl Send for VsockSocket {}
    unsafe impl Sync for VsockSocket {}

    impl VsockSocket {
        /// Create a fresh vsock SOCK_STREAM socket (not yet bound /
        /// connected).
        pub fn new() -> io::Result<Self> {
            let fd = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM, 0) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self { fd })
        }

        /// Bind to (cid, port). Use VMADDR_CID_ANY to bind to any CID.
        pub fn bind(&self, cid: u32, port: u32) -> io::Result<()> {
            let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
            addr.svm_family = libc::AF_VSOCK as _;
            addr.svm_cid = cid;
            addr.svm_port = port;
            let rc = unsafe {
                libc::bind(
                    self.fd,
                    &addr as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_vm>() as _,
                )
            };
            if rc < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        /// Listen on a bound socket.
        pub fn listen(&self, backlog: i32) -> io::Result<()> {
            let rc = unsafe { libc::listen(self.fd, backlog) };
            if rc < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        /// Accept one incoming vsock connection. Returns the connected
        /// socket; the listener stays alive.
        pub fn accept(&self) -> io::Result<VsockSocket> {
            let fd =
                unsafe { libc::accept(self.fd, std::ptr::null_mut(), std::ptr::null_mut()) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(VsockSocket { fd })
        }

        /// Connect this socket to (cid, port).
        pub fn connect(&self, cid: u32, port: u32) -> io::Result<()> {
            let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
            addr.svm_family = libc::AF_VSOCK as _;
            addr.svm_cid = cid;
            addr.svm_port = port;
            let rc = unsafe {
                libc::connect(
                    self.fd,
                    &addr as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_vm>() as _,
                )
            };
            if rc < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        /// Blocking send. Returns bytes written.
        pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
            let n = unsafe { libc::send(self.fd, buf.as_ptr() as *const _, buf.len(), 0) };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }

        /// Blocking recv. Returns bytes read.
        pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            let n = unsafe { libc::recv(self.fd, buf.as_mut_ptr() as *mut _, buf.len(), 0) };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }
    }

    impl AsRawFd for VsockSocket {
        fn as_raw_fd(&self) -> RawFd {
            self.fd
        }
    }

    impl FromRawFd for VsockSocket {
        unsafe fn from_raw_fd(fd: RawFd) -> Self {
            Self { fd }
        }
    }

    impl Drop for VsockSocket {
        fn drop(&mut self) {
            if self.fd >= 0 {
                unsafe { libc::close(self.fd) };
            }
        }
    }

    /// Cross-platform host-VM stream socket (Linux side). Wraps
    /// [`VsockSocket`] with the loopback-oriented port API shared with
    /// the Windows `AF_HYPERV` implementation.
    pub struct HostVmSocket {
        inner: VsockSocket,
    }

    impl HostVmSocket {
        /// Bind + listen on `port` for loopback connections.
        pub fn listen_loopback(port: u32) -> io::Result<Self> {
            let s = VsockSocket::new()?;
            // Bind to ANY so loopback (CID_LOCAL) connectors reach us.
            s.bind(VMADDR_CID_ANY, port)?;
            s.listen(16)?;
            Ok(Self { inner: s })
        }

        /// Accept one loopback connection.
        pub fn accept(&self) -> io::Result<Self> {
            Ok(Self { inner: self.inner.accept()? })
        }

        /// Connect to a loopback listener on `port` (same host).
        pub fn connect_loopback(port: u32) -> io::Result<Self> {
            let s = VsockSocket::new()?;
            s.connect(VMADDR_CID_LOCAL, port)?;
            Ok(Self { inner: s })
        }

        pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
            self.inner.send(buf)
        }
        pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.recv(buf)
        }
    }
}

#[cfg(target_os = "linux")]
pub use vsock_impl::{
    HostVmSocket, VsockSocket, VMADDR_CID_ANY, VMADDR_CID_HOST, VMADDR_CID_LOCAL,
};

// ---------------------------------------------------------------------
// Windows: Hyper-V sockets over AF_HYPERV.
// ---------------------------------------------------------------------

#[cfg(windows)]
mod hyperv_impl {
    use std::io;
    use std::sync::Once;
    use windows_sys::core::GUID;
    use windows_sys::Win32::Networking::WinSock::{
        accept, bind, closesocket, connect, listen, recv, send, socket, WSAStartup,
        INVALID_SOCKET, SOCKADDR, SOCKET, SOCKET_ERROR, SOCK_STREAM, WSADATA,
    };

    const AF_HYPERV: i32 = 34;
    const HV_PROTOCOL_RAW: i32 = 1;

    /// Loopback VmId - connecting here reaches the same partition.
    const HV_GUID_LOOPBACK: GUID = GUID {
        data1: 0xe0e1_6197,
        data2: 0xdd56,
        data3: 0x4a10,
        data4: [0x91, 0x95, 0x5e, 0xe7, 0xa1, 0x55, 0xa8, 0x38],
    };
    /// Wildcard VmId - listeners bind here to accept from all partitions.
    const HV_GUID_WILDCARD: GUID = GUID {
        data1: 0,
        data2: 0,
        data3: 0,
        data4: [0; 8],
    };

    /// The documented Linux-guest VSOCK service-ID template; `Data1` is
    /// the port. Mapping a port through this keeps the address model the
    /// same as the Linux `(cid, port)` side.
    fn service_id_for_port(port: u32) -> GUID {
        GUID {
            data1: port,
            data2: 0xfacb,
            data3: 0x11e6,
            data4: [0xbd, 0x58, 0x64, 0x00, 0x6a, 0x79, 0x86, 0xd3],
        }
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SOCKADDR_HV {
        family: u16,
        reserved: u16,
        vm_id: GUID,
        service_id: GUID,
    }

    fn ensure_winsock() {
        static START: Once = Once::new();
        START.call_once(|| {
            let mut data: WSADATA = unsafe { std::mem::zeroed() };
            // MAKEWORD(2, 2) = 0x0202.
            unsafe { WSAStartup(0x0202, &mut data) };
        });
    }

    fn last_err() -> io::Error {
        io::Error::last_os_error()
    }

    /// Cross-platform host-VM stream socket (Windows side): a Hyper-V
    /// socket addressed by VmId + ServiceId GUIDs. Same surface as the
    /// Linux `AF_VSOCK` implementation.
    pub struct HostVmSocket {
        sock: SOCKET,
    }

    unsafe impl Send for HostVmSocket {}
    unsafe impl Sync for HostVmSocket {}

    impl HostVmSocket {
        fn raw_socket() -> io::Result<SOCKET> {
            ensure_winsock();
            let s = unsafe { socket(AF_HYPERV, SOCK_STREAM, HV_PROTOCOL_RAW) };
            if s == INVALID_SOCKET {
                Err(io::Error::other(format!(
                    "AF_HYPERV socket unavailable ({}); enable the Hyper-V / \
                     Virtual Machine Platform feature",
                    last_err()
                )))
            } else {
                Ok(s)
            }
        }

        fn addr(vm_id: GUID, port: u32) -> SOCKADDR_HV {
            SOCKADDR_HV {
                family: AF_HYPERV as u16,
                reserved: 0,
                vm_id,
                service_id: service_id_for_port(port),
            }
        }

        /// Bind + listen on `port` for loopback connections.
        pub fn listen_loopback(port: u32) -> io::Result<Self> {
            let sock = Self::raw_socket()?;
            let addr = Self::addr(HV_GUID_WILDCARD, port);
            let rc = unsafe {
                bind(
                    sock,
                    &addr as *const _ as *const SOCKADDR,
                    std::mem::size_of::<SOCKADDR_HV>() as i32,
                )
            };
            if rc == SOCKET_ERROR {
                let e = last_err();
                unsafe { closesocket(sock) };
                return Err(e);
            }
            if unsafe { listen(sock, 16) } == SOCKET_ERROR {
                let e = last_err();
                unsafe { closesocket(sock) };
                return Err(e);
            }
            Ok(Self { sock })
        }

        /// Accept one loopback connection.
        pub fn accept(&self) -> io::Result<Self> {
            let s = unsafe { accept(self.sock, std::ptr::null_mut(), std::ptr::null_mut()) };
            if s == INVALID_SOCKET {
                Err(last_err())
            } else {
                Ok(Self { sock: s })
            }
        }

        /// Connect to a loopback listener on `port` (same partition).
        pub fn connect_loopback(port: u32) -> io::Result<Self> {
            let sock = Self::raw_socket()?;
            let addr = Self::addr(HV_GUID_LOOPBACK, port);
            let rc = unsafe {
                connect(
                    sock,
                    &addr as *const _ as *const SOCKADDR,
                    std::mem::size_of::<SOCKADDR_HV>() as i32,
                )
            };
            if rc == SOCKET_ERROR {
                let e = last_err();
                unsafe { closesocket(sock) };
                return Err(e);
            }
            Ok(Self { sock })
        }

        pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
            let n = unsafe { send(self.sock, buf.as_ptr(), buf.len() as i32, 0) };
            if n == SOCKET_ERROR {
                Err(last_err())
            } else {
                Ok(n as usize)
            }
        }

        pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            let n = unsafe { recv(self.sock, buf.as_mut_ptr(), buf.len() as i32, 0) };
            if n == SOCKET_ERROR {
                Err(last_err())
            } else {
                Ok(n as usize)
            }
        }
    }

    impl Drop for HostVmSocket {
        fn drop(&mut self) {
            unsafe { closesocket(self.sock) };
        }
    }
}

#[cfg(windows)]
pub use hyperv_impl::HostVmSocket;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    /// In-process loopback round trip through the host-VM socket: a
    /// listener accepts a loopback connection and echoes. Skips cleanly
    /// when the loopback transport is unavailable (no `vsock_loopback`
    /// module / no Hyper-V provider).
    #[test]
    fn host_vm_loopback_round_trip() {
        // Per-process port so parallel test runs do not collide.
        let port: u32 = 0x4000 + (std::process::id() & 0x3fff);

        let listener = match HostVmSocket::listen_loopback(port) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("skipping: host-vm loopback unavailable ({e})");
                return;
            }
        };

        let client = thread::spawn(move || {
            let c = match HostVmSocket::connect_loopback(port) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("connect failed: {e}");
                    return false;
                }
            };
            if c.send(b"ping").is_err() {
                return false;
            }
            let mut buf = [0u8; 4];
            match c.recv(&mut buf) {
                Ok(n) => &buf[..n] == b"pong",
                Err(_) => false,
            }
        });

        let conn = listener.accept().expect("accept");
        let mut buf = [0u8; 4];
        let n = conn.recv(&mut buf).expect("recv ping");
        assert_eq!(&buf[..n], b"ping", "server received ping");
        conn.send(b"pong").expect("send pong");

        assert!(client.join().unwrap(), "client completed the round trip");
    }
}
