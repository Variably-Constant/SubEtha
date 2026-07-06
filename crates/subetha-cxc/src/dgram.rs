//! `dgram`: a pluggable datagram backend for the RLC transport.
//!
//! The transport's wire I/O normally runs on a plain `std::net::UdpSocket`.
//! `DgramSock` wraps that socket and, on Linux, transparently upgrades it to
//! an **io_uring**-backed datagram socket - the kernel async submission /
//! completion ring drives batched `recvmsg` / `sendmsg`, cutting the
//! syscall-per-packet overhead of the hot loop. The backend is:
//!
//! - **auto-detected** at runtime: `DgramSock::wrap` tries the io_uring
//!   backend and silently falls back to the plain `UdpSocket` when io_uring
//!   is unavailable (old kernel, container, or any non-Linux target), so the
//!   transport works everywhere and uses the ring only where it exists; and
//! - **overridable** via the `SUBETHA_DGRAM` env var (`iouring` forces the
//!   ring - and warns if it is unavailable rather than silently degrading -
//!   `udp` forces the plain socket), so a test can prove the ring path is
//!   actually exercised; and
//! - **link-speed-gated** for the NIC-bypass Wire backend (AF_XDP on Linux,
//!   netmap on FreeBSD, BPF on macOS): kernel-bypass only beats plain UDP once the link is
//!   fast enough that the per-packet syscall path - not the link - is the
//!   bottleneck, so the auto path engages Wire only when the configured
//!   `SUBETHA_WIRE_IFNAME`'s detected link speed is at or above the gate
//!   (`SUBETHA_WIRE_MIN_GBPS`, default 10 Gbit/s) and falls back to io_uring
//!   / UDP below it. `SUBETHA_DGRAM=wire` forces Wire regardless (warned
//!   when the link is below the gate, since it is net-negative there).
//!
//! The io_uring backend preserves every semantic the transport relies on:
//! `(n, src_addr)` from `recv_from`, the `SO_TIMESTAMPNS` kernel arrival
//! timestamp from `recv_with_kts`, non-blocking `WouldBlock`, and per-peer
//! `send_to`.

use std::io;
use std::net::{SocketAddr, UdpSocket};

/// Which datagram backend a [`DgramSock`] resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DgramBackend {
    /// Plain `std::net::UdpSocket` (the universal fallback).
    Udp,
    /// io_uring-backed batched `recvmsg`/`sendmsg` (Linux, when available).
    IoUring,
    /// NIC-bypass via a `WireSocket` (AF_XDP on Linux, netmap on FreeBSD, BPF on macOS):
    /// the transport's datagrams ride raw Ethernet+IPv4+UDP frames, bypassing
    /// the kernel networking stack. Engaged only above the link-speed gate.
    Wire,
    /// Inbound stream is an in-process [`DemuxQueue`] fed by a demux reader;
    /// sends forward to a shared real socket. Used by the unified Sens-O-Matic
    /// endpoint to fan one socket out to its per-code receivers.
    Demux,
}

// The backends are inherently different sizes (a plain UdpSocket vs an io_uring
// ring). There is exactly ONE Inner per DgramSock, alive for the socket's whole
// life - never a collection - so the per-variant size gap the lint warns about
// (wasted slots in a Vec) does not apply; boxing would only add a hot-path deref.
#[allow(clippy::large_enum_variant)]
enum Inner {
    Udp(UdpSocket),
    #[cfg(target_os = "linux")]
    IoUring(linux_iou::IoUringDgram),
    #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
    Wire(wire_backend::WireDgram),
    /// Inbound side is an in-process queue fed by a demux reader; outbound side
    /// forwards to a shared real socket. See [`DemuxDgram`].
    Demux(DemuxDgram),
}

/// Inbound datagram queue for a [`DgramSock::demux`] socket. The unified
/// endpoint's demux reader classifies each datagram by its first wire byte and
/// pushes `(bytes, from, kernel_ts)` onto the matching code's queue; that
/// code's receiver pops it through the normal `recv_*` surface, unmodified.
pub type DemuxQueue =
    std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<(Vec<u8>, SocketAddr, Option<i128>)>>>;

/// A fresh, empty [`DemuxQueue`].
pub fn new_demux_queue() -> DemuxQueue {
    std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()))
}

/// Datagram backend whose inbound stream is an in-process [`DemuxQueue`] and
/// whose sends forward to a shared real socket. Lets one real socket fan a
/// demultiplexed inbound stream out to several independent per-code receivers
/// that each believe they own a socket, while they all transmit through the one
/// real socket.
struct DemuxDgram {
    real: std::sync::Arc<UdpSocket>,
    queue: DemuxQueue,
    /// Peer set by `connect`; the connected `send` carries it, since the shared
    /// real socket is not itself connected to one peer.
    peer: std::sync::Mutex<Option<SocketAddr>>,
    /// Optional shared counter of datagrams sent through this socket, the
    /// unified endpoint's raw-channel-loss numerator (sent vs received).
    sent: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

impl DemuxDgram {
    fn recv_with_kts(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr, Option<i128>)> {
        match self.queue.lock().unwrap().pop_front() {
            Some((data, from, kts)) => {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok((n, from, kts))
            }
            // An empty queue reads as a non-blocking socket would with no
            // datagram ready, so the receivers' idle-backoff loop is unchanged.
            None => Err(io::Error::new(io::ErrorKind::WouldBlock, "demux queue empty")),
        }
    }

    fn connect(&self, addr: SocketAddr) {
        *self.peer.lock().unwrap() = Some(addr);
    }

    fn count_fwd(&self, buf: &[u8]) {
        // Count only forward data/repair (RS data 1, RLC data 10 / repair 11),
        // exactly what the receiver tallies, so the raw-loss ratio is unbiased
        // by control frames (heartbeats, probes) the receiver does not count.
        if let Some(c) = &self.sent
            && matches!(buf.first(), Some(&(1 | 10 | 11)))
        {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.count_fwd(buf);
        self.real.send_to(buf, addr)
    }

    fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.count_fwd(buf);
        match *self.peer.lock().unwrap() {
            Some(p) => self.real.send_to(buf, p),
            None => Err(io::Error::new(io::ErrorKind::NotConnected, "demux send before connect")),
        }
    }
}

/// A datagram socket whose backend is chosen at runtime: io_uring where
/// available, plain UDP otherwise. Same surface either way.
pub struct DgramSock {
    inner: Inner,
}

impl DgramSock {
    /// Wrap a bound `UdpSocket`, auto-detecting the io_uring backend. Honors
    /// `SUBETHA_DGRAM` (`iouring` / `udp`); otherwise prefers io_uring on
    /// Linux and falls back to plain UDP when the ring cannot be created.
    pub fn wrap(sock: UdpSocket) -> Self {
        enable_rx_timestamp(&sock);
        let forced = std::env::var("SUBETHA_DGRAM").ok();
        if forced.as_deref() == Some("udp") {
            return Self { inner: Inner::Udp(sock) };
        }

        // NIC-bypass backend (AF_XDP on Linux, netmap on FreeBSD, BPF on macOS): the
        // transport's datagrams ride raw Ethernet+IPv4+UDP frames with the
        // kernel stack bypassed. It only wins when the link is fast enough
        // that the per-packet syscall path - not the link - is the
        // bottleneck, so the AUTO path engages it ONLY above the link-speed
        // gate (`SUBETHA_WIRE_MIN_GBPS`, default 10 Gbit/s) and otherwise
        // falls through to io_uring / UDP. `SUBETHA_DGRAM=wire` forces it
        // regardless (warned when below the gate). Either way it needs the
        // `SUBETHA_WIRE_*` interface / address / peer-MAC config.
        #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
        {
            let wire_if = std::env::var("SUBETHA_WIRE_IFNAME").ok();
            let force_wire = forced.as_deref() == Some("wire");
            let auto_wire = forced.is_none()
                && wire_if.as_deref().map(wire_link_fast_enough).unwrap_or(false);
            if force_wire || auto_wire {
                let port = sock.local_addr().map(|a| a.port()).unwrap_or(0);
                match wire_backend::WireDgram::from_env(port) {
                    Ok(w) => {
                        if force_wire
                            && !wire_if.as_deref().map(wire_link_fast_enough).unwrap_or(false)
                        {
                            eprintln!(
                                "SUBETHA_DGRAM=wire forced on a link below the {:.0} Gbit/s \
                                 gate; the kernel-bypass path is net-negative below line rate",
                                wire_min_link_bps() as f64 / 1e9
                            );
                        }
                        return Self { inner: Inner::Wire(w) };
                    }
                    Err(e) => eprintln!(
                        "Wire backend requested but unavailable ({e}); falling back"
                    ),
                }
            }
        }

        #[cfg(target_os = "linux")]
        {
            let force_ring = forced.as_deref() == Some("iouring");
            match linux_iou::IoUringDgram::new(sock) {
                Ok(d) => Self { inner: Inner::IoUring(d) },
                Err((sock, e)) => {
                    if force_ring {
                        eprintln!(
                            "SUBETHA_DGRAM=iouring requested but the ring is unavailable \
                             ({e}); using plain UDP"
                        );
                    }
                    Self { inner: Inner::Udp(sock) }
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            drop(forced);
            Self { inner: Inner::Udp(sock) }
        }
    }

    /// Build a demux-backed socket: inbound datagrams are popped from `queue`
    /// (fed by a demux reader that classifies one real socket's datagrams by
    /// first wire byte), outbound sends forward to `real`. The unified
    /// Sens-O-Matic endpoint uses this to fan one socket out to its per-code
    /// RLC and RS receivers without modifying either.
    pub fn demux(real: std::sync::Arc<UdpSocket>, queue: DemuxQueue) -> Self {
        Self::demux_inner(real, queue, None)
    }

    /// Like [`demux`](Self::demux) but tallies every datagram sent through it
    /// into `sent` (the unified endpoint's raw-loss numerator).
    pub fn demux_counted(
        real: std::sync::Arc<UdpSocket>,
        queue: DemuxQueue,
        sent: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self::demux_inner(real, queue, Some(sent))
    }

    fn demux_inner(
        real: std::sync::Arc<UdpSocket>,
        queue: DemuxQueue,
        sent: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) -> Self {
        Self {
            inner: Inner::Demux(DemuxDgram {
                real,
                queue,
                peer: std::sync::Mutex::new(None),
                sent,
            }),
        }
    }

    /// Wrap a bound `UdpSocket` as a plain-UDP `DgramSock` WITHOUT the io_uring
    /// auto-upgrade. The Reed-Solomon transport drives the raw fd directly for
    /// GRO / TTL / ECN / connected-send / Windows USO, so it needs the `Udp`
    /// backend (reachable via [`as_udp`](Self::as_udp)); `wrap`'s io_uring
    /// upgrade would hide the fd. Sets no sockopts of its own - that transport
    /// manages its own recvmsg cmsgs and control-buffer sizing, so adding the
    /// RX-timestamp cmsg here could overflow its control buffer.
    pub fn from_udp(sock: UdpSocket) -> Self {
        Self { inner: Inner::Udp(sock) }
    }

    /// The underlying `UdpSocket` when this is a plain-UDP backend (the only
    /// backend with a directly-usable fd), else `None`. Lets a transport that
    /// needs raw-fd socket features keep them on the standalone path and fall
    /// back cleanly on the demux / io_uring / wire paths.
    pub fn as_udp(&self) -> Option<&UdpSocket> {
        match &self.inner {
            Inner::Udp(s) => Some(s),
            _ => None,
        }
    }

    /// Connect the socket to `addr` so [`send`](Self::send) can omit it. Udp
    /// connects the kernel socket; Demux records the peer for its forwarded
    /// send. io_uring / wire are not used by the connected-send transport.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        match &self.inner {
            Inner::Udp(s) => s.connect(addr),
            Inner::Demux(d) => {
                d.connect(addr);
                Ok(())
            }
            #[allow(unreachable_patterns)]
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "connect on a non-Udp/Demux backend",
            )),
        }
    }

    /// Send on the connected peer (see [`connect`](Self::connect)).
    pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
        match &self.inner {
            Inner::Udp(s) => s.send(buf),
            Inner::Demux(d) => d.send(buf),
            #[allow(unreachable_patterns)]
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "send on a non-Udp/Demux backend",
            )),
        }
    }

    /// Receive on the connected socket (see [`connect`](Self::connect)). Udp
    /// uses the kernel connected recv; Demux pops its demux queue.
    pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match &self.inner {
            Inner::Udp(s) => s.recv(buf),
            Inner::Demux(d) => d.recv_with_kts(buf).map(|(n, _, _)| n),
            #[allow(unreachable_patterns)]
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "recv on a non-Udp/Demux backend",
            )),
        }
    }

    /// Which backend was selected.
    pub fn backend(&self) -> DgramBackend {
        match &self.inner {
            Inner::Udp(_) => DgramBackend::Udp,
            #[cfg(target_os = "linux")]
            Inner::IoUring(_) => DgramBackend::IoUring,
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(_) => DgramBackend::Wire,
            Inner::Demux(_) => DgramBackend::Demux,
        }
    }

    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        match &self.inner {
            Inner::Udp(s) => s.send_to(buf, addr),
            #[cfg(target_os = "linux")]
            Inner::IoUring(d) => d.send_to(buf, addr),
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(d) => d.send_to(buf, addr),
            Inner::Demux(d) => d.send_to(buf, addr),
        }
    }

    /// The underlying UDP socket fd, when the backend has one (Udp / io_uring).
    /// The Wire (AF_XDP / netmap / BPF) backend has no kernel UDP fd and returns
    /// `None`. Used by the GSO batch path, which does a direct `sendmsg`.
    #[cfg(target_os = "linux")]
    fn raw_fd(&self) -> Option<i32> {
        use std::os::unix::io::AsRawFd;
        match &self.inner {
            Inner::Udp(s) => Some(s.as_raw_fd()),
            Inner::IoUring(d) => Some(d.raw_fd()),
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(_) => None,
            Inner::Demux(_) => None,
        }
    }

    /// Ship `batch` (an integer number of `seg_size`-byte datagrams concatenated)
    /// to `addr` in ONE `sendmsg` via UDP GSO (`UDP_SEGMENT`) - the kernel slices
    /// it into `batch.len() / seg_size` wire datagrams, replicating IP+UDP
    /// headers. Collapses the per-datagram syscall + stack-traversal cost (~62x
    /// fewer syscalls at MTU). Falls back to one `send_to` per segment on
    /// backends without a UDP fd (Wire) or non-Linux. `batch.len()` MUST be a
    /// multiple of `seg_size`, and `seg_size * n_segs` must fit a single IP
    /// datagram (<= 65535) - the caller caps the batch.
    pub fn send_gso(&self, batch: &[u8], seg_size: u16, addr: SocketAddr) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(fd) = self.raw_fd() {
            return linux_gso_send(fd, batch, seg_size, addr);
        }
        let n = (seg_size as usize).max(1);
        let mut off = 0;
        while off < batch.len() {
            let end = (off + n).min(batch.len());
            self.send_to(&batch[off..end], addr)?;
            off = end;
        }
        Ok(())
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match &self.inner {
            Inner::Udp(s) => s.recv_from(buf),
            #[cfg(target_os = "linux")]
            Inner::IoUring(d) => d.recv_with_kts(buf).map(|(n, a, _)| (n, a)),
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(d) => d.recv_with_kts(buf).map(|(n, a, _)| (n, a)),
            Inner::Demux(d) => d.recv_with_kts(buf).map(|(n, a, _)| (n, a)),
        }
    }

    /// Receive one datagram with the kernel arrival timestamp when available.
    pub fn recv_with_kts(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr, Option<i128>)> {
        match &self.inner {
            Inner::Udp(s) => udp_recv_with_kts(s, buf),
            #[cfg(target_os = "linux")]
            Inner::IoUring(d) => d.recv_with_kts(buf),
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(d) => d.recv_with_kts(buf),
            Inner::Demux(d) => d.recv_with_kts(buf),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.inner {
            Inner::Udp(s) => s.local_addr(),
            #[cfg(target_os = "linux")]
            Inner::IoUring(d) => d.local_addr(),
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(d) => d.local_addr(),
            Inner::Demux(d) => d.real.local_addr(),
        }
    }

    pub fn set_nonblocking(&self, nb: bool) -> io::Result<()> {
        match &self.inner {
            Inner::Udp(s) => s.set_nonblocking(nb),
            #[cfg(target_os = "linux")]
            Inner::IoUring(d) => d.set_nonblocking(nb),
            #[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
            Inner::Wire(d) => d.set_nonblocking(nb),
            // The demux queue's pop is inherently non-blocking (empty -> WouldBlock),
            // so blocking mode is a no-op; the per-code receivers run non-blocking.
            Inner::Demux(_) => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------
// Shared UDP datagram helpers (also the universal fallback path).
// ---------------------------------------------------------------------

/// Best-effort kernel RX timestamps (`SO_TIMESTAMPNS`).
#[cfg(target_os = "linux")]
fn enable_rx_timestamp(sock: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let on: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            &on as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn enable_rx_timestamp(_sock: &UdpSocket) {}

/// `recvmsg`-based receive that extracts the kernel arrival timestamp.
#[cfg(target_os = "linux")]
pub(crate) fn udp_recv_with_kts(sock: &UdpSocket, buf: &mut [u8]) -> io::Result<(usize, SocketAddr, Option<i128>)> {
    use std::os::fd::AsRawFd;
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };
    let mut name: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut control = [0u8; 64];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_name = &mut name as *mut _ as *mut libc::c_void;
    msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = control.len();
    let n = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let kts = parse_timestamp(&msg);
    let from = unsafe { sockaddr_to_socketaddr(&name) }
        .ok_or_else(|| io::Error::other("non-IP source"))?;
    Ok((n as usize, from, kts))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn udp_recv_with_kts(sock: &UdpSocket, buf: &mut [u8]) -> io::Result<(usize, SocketAddr, Option<i128>)> {
    let (n, from) = sock.recv_from(buf)?;
    Ok((n, from, None))
}

#[cfg(target_os = "linux")]
fn parse_timestamp(msg: &libc::msghdr) -> Option<i128> {
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS {
                let ts = (libc::CMSG_DATA(cmsg) as *const libc::timespec).read_unaligned();
                return Some(ts.tv_sec as i128 * 1_000_000_000 + ts.tv_nsec as i128);
            }
            cmsg = libc::CMSG_NXTHDR(msg, cmsg);
        }
    }
    None
}

#[cfg(target_os = "linux")]
unsafe fn sockaddr_to_socketaddr(name: &libc::sockaddr_storage) -> Option<SocketAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr};
    match name.ss_family as libc::c_int {
        libc::AF_INET => {
            let sin = unsafe { &*(name as *const libc::sockaddr_storage as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Some(SocketAddr::new(ip.into(), u16::from_be(sin.sin_port)))
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { &*(name as *const libc::sockaddr_storage as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Some(SocketAddr::new(ip.into(), u16::from_be(sin6.sin6_port)))
        }
        _ => None,
    }
}

/// Build a kernel `sockaddr_storage` (+ length) from a [`SocketAddr`] for
/// `sendmsg`.
#[cfg(target_os = "linux")]
fn socketaddr_to_sockaddr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(a) => {
            let sin = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = a.port().to_be();
            sin.sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
            (storage, std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        SocketAddr::V6(a) => {
            let sin6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr.s6_addr = a.ip().octets();
            (storage, std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

/// One `sendmsg` shipping `batch` (k * `seg_size` bytes) to `addr` as k wire
/// datagrams of `seg_size` via a `UDP_SEGMENT` control message; the kernel
/// (or the NIC, with `tx-udp-segmentation`) slices and replicates the headers.
#[cfg(target_os = "linux")]
fn linux_gso_send(fd: i32, batch: &[u8], seg_size: u16, addr: SocketAddr) -> io::Result<()> {
    /// `UDP_SEGMENT` cmsg type; `SOL_UDP` = 17.
    const UDP_SEGMENT: libc::c_int = 103;
    const SOL_UDP: libc::c_int = 17;
    let (storage, addrlen) = socketaddr_to_sockaddr(addr);
    let mut iov = libc::iovec {
        iov_base: batch.as_ptr() as *mut libc::c_void,
        iov_len: batch.len(),
    };
    let cmsg_space = unsafe { libc::CMSG_SPACE(2) } as usize;
    let mut cbuf = vec![0u8; cmsg_space];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_name = &storage as *const _ as *mut libc::c_void;
    msg.msg_namelen = addrlen;
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;
    // SAFETY: msg + storage + iov + cbuf all outlive the sendmsg call; the
    // cmsg is sized by CMSG_SPACE(2) for a single u16 UDP_SEGMENT value.
    unsafe {
        let c = libc::CMSG_FIRSTHDR(&msg);
        (*c).cmsg_level = SOL_UDP;
        (*c).cmsg_type = UDP_SEGMENT;
        (*c).cmsg_len = libc::CMSG_LEN(2) as _;
        std::ptr::copy_nonoverlapping(&seg_size as *const u16 as *const u8, libc::CMSG_DATA(c), 2);
        loop {
            let r = libc::sendmsg(fd, &msg, 0);
            if r >= 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
    }
}

// ---------------------------------------------------------------------
// Linux io_uring datagram backend.
// ---------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod linux_iou {
    use super::{parse_timestamp, sockaddr_to_socketaddr, socketaddr_to_sockaddr};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io;
    use std::net::{SocketAddr, UdpSocket};
    use std::os::fd::AsRawFd;

    use io_uring::{opcode, types, IoUring};

    const RECV_DEPTH: usize = 32;
    const SEND_DEPTH: usize = 32;
    const FRAME_CAP: usize = 2048;
    const CONTROL_CAP: usize = 64;
    const SEND_TAG: u64 = 1 << 32; // user_data >= SEND_TAG marks send completions

    /// A self-referential `recvmsg` context: the `msghdr` points at the addr
    /// / iov / control / buf fields of the same heap box, so the box must
    /// never move while an SQE referencing it is in flight (it lives in the
    /// `recv` Vec for the socket's lifetime).
    struct RecvCtx {
        addr: libc::sockaddr_storage,
        iov: libc::iovec,
        control: [u8; CONTROL_CAP],
        msghdr: libc::msghdr,
        buf: [u8; FRAME_CAP],
    }

    impl RecvCtx {
        fn boxed() -> Box<Self> {
            let mut b: Box<Self> = Box::new(unsafe { std::mem::zeroed() });
            b.refresh();
            b
        }
        fn refresh(&mut self) {
            self.iov.iov_base = self.buf.as_mut_ptr() as *mut libc::c_void;
            self.iov.iov_len = FRAME_CAP;
            self.msghdr.msg_name = std::ptr::addr_of_mut!(self.addr) as *mut libc::c_void;
            self.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
            self.msghdr.msg_iov = std::ptr::addr_of_mut!(self.iov);
            self.msghdr.msg_iovlen = 1;
            self.msghdr.msg_control = self.control.as_mut_ptr() as *mut libc::c_void;
            self.msghdr.msg_controllen = CONTROL_CAP;
        }
    }

    struct SendCtx {
        addr: libc::sockaddr_storage,
        iov: libc::iovec,
        msghdr: libc::msghdr,
        buf: [u8; FRAME_CAP],
    }

    struct State {
        ring: IoUring,
        // Box per element is load-bearing, not redundant: each ctx's msghdr
        // points at its own addr/iov/control fields, so every element needs a
        // stable heap address independent of the Vec's buffer (clippy's
        // vec_box lint assumes the boxing is unnecessary - it is not here).
        #[allow(clippy::vec_box)]
        recv: Vec<Box<RecvCtx>>,
        #[allow(clippy::vec_box)]
        send: Vec<Box<SendCtx>>,
        ready: VecDeque<usize>,
        ready_len: Vec<usize>,
        free_send: Vec<usize>,
    }

    pub struct IoUringDgram {
        sock: UdpSocket,
        fd: i32,
        st: RefCell<State>,
    }

    unsafe impl Send for IoUringDgram {}

    impl IoUringDgram {
        pub fn new(sock: UdpSocket) -> Result<Self, (UdpSocket, io::Error)> {
            let entries = ((RECV_DEPTH + SEND_DEPTH) * 2).next_power_of_two() as u32;
            let ring = match IoUring::new(entries) {
                Ok(r) => r,
                Err(e) => return Err((sock, e)),
            };
            let fd = sock.as_raw_fd();
            let recv: Vec<Box<RecvCtx>> = (0..RECV_DEPTH).map(|_| RecvCtx::boxed()).collect();
            let send: Vec<Box<SendCtx>> =
                (0..SEND_DEPTH).map(|_| Box::new(unsafe { std::mem::zeroed() })).collect();
            let st = State {
                ring,
                recv,
                send,
                ready: VecDeque::new(),
                ready_len: vec![0usize; RECV_DEPTH],
                free_send: (0..SEND_DEPTH).collect(),
            };
            let me = Self { sock, fd, st: RefCell::new(st) };
            if let Err(e) = me.submit_all_recv() {
                return Err((me.sock, e));
            }
            Ok(me)
        }

        pub fn local_addr(&self) -> io::Result<SocketAddr> {
            self.sock.local_addr()
        }

        /// The underlying UDP socket fd - a normal socket the io_uring ring
        /// submits ops against. A direct `sendmsg` on it (e.g. the GSO batch
        /// path) is independent of the ring's recv SQEs.
        pub fn raw_fd(&self) -> i32 {
            self.fd
        }

        pub fn set_nonblocking(&self, nb: bool) -> io::Result<()> {
            self.sock.set_nonblocking(nb)
        }

        fn submit_all_recv(&self) -> io::Result<()> {
            let mut st = self.st.borrow_mut();
            for i in 0..st.recv.len() {
                self.push_recv(&mut st, i)?;
            }
            st.ring.submit()?;
            Ok(())
        }

        fn push_recv(&self, st: &mut State, idx: usize) -> io::Result<()> {
            st.recv[idx].refresh();
            let msg: *mut libc::msghdr = std::ptr::addr_of_mut!(st.recv[idx].msghdr);
            let e = opcode::RecvMsg::new(types::Fd(self.fd), msg)
                .build()
                .user_data(idx as u64);
            // SAFETY: the msghdr + buffers live in the boxed ctx for the
            // socket's lifetime; the ctx is not reused until this op completes.
            unsafe {
                st.ring
                    .submission()
                    .push(&e)
                    .map_err(|_| io::Error::other("io_uring SQ full (recv)"))?;
            }
            Ok(())
        }

        fn reap(&self, st: &mut State) {
            let mut completed: Vec<(u64, i32)> = Vec::new();
            for cqe in st.ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }
            for (ud, res) in completed {
                if ud >= SEND_TAG {
                    st.free_send.push((ud - SEND_TAG) as usize);
                } else {
                    let idx = ud as usize;
                    if res >= 0 {
                        st.ready_len[idx] = res as usize;
                        st.ready.push_back(idx);
                    } else {
                        self.push_recv(st, idx).ok();
                    }
                }
            }
        }

        pub fn recv_with_kts(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr, Option<i128>)> {
            let mut st = self.st.borrow_mut();
            self.reap(&mut st);
            if st.ready.is_empty() {
                st.ring.submit()?;
                self.reap(&mut st);
                if st.ready.is_empty() {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock));
                }
            }
            let idx = st.ready.pop_front().unwrap();
            let n = st.ready_len[idx];
            let copy = n.min(buf.len());
            let from;
            let kts;
            {
                let ctx = &st.recv[idx];
                buf[..copy].copy_from_slice(&ctx.buf[..copy]);
                from = unsafe { sockaddr_to_socketaddr(&ctx.addr) }
                    .ok_or_else(|| io::Error::other("non-IP source"))?;
                kts = parse_timestamp(&ctx.msghdr);
            }
            self.push_recv(&mut st, idx)?;
            st.ring.submit()?;
            Ok((copy, from, kts))
        }

        pub fn send_to(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> {
            if data.len() > FRAME_CAP {
                return Err(io::Error::other("datagram exceeds frame cap"));
            }
            let mut st = self.st.borrow_mut();
            self.reap(&mut st);
            if st.free_send.is_empty() {
                st.ring.submit()?;
                self.reap(&mut st);
                if st.free_send.is_empty() {
                    return self.sock.send_to(data, addr);
                }
            }
            let idx = st.free_send.pop().unwrap();
            let (sa, sa_len) = socketaddr_to_sockaddr(addr);
            {
                let ctx = &mut st.send[idx];
                ctx.buf[..data.len()].copy_from_slice(data);
                ctx.addr = sa;
                ctx.iov.iov_base = ctx.buf.as_mut_ptr() as *mut libc::c_void;
                ctx.iov.iov_len = data.len();
                ctx.msghdr.msg_name = std::ptr::addr_of_mut!(ctx.addr) as *mut libc::c_void;
                ctx.msghdr.msg_namelen = sa_len;
                ctx.msghdr.msg_iov = std::ptr::addr_of_mut!(ctx.iov);
                ctx.msghdr.msg_iovlen = 1;
            }
            let msg: *const libc::msghdr = std::ptr::addr_of!(st.send[idx].msghdr);
            let e = opcode::SendMsg::new(types::Fd(self.fd), msg)
                .build()
                .user_data(SEND_TAG | idx as u64);
            // SAFETY: the msghdr + buffers live in the boxed send ctx; the ctx
            // is not reused until this op completes (it left free_send).
            unsafe {
                st.ring
                    .submission()
                    .push(&e)
                    .map_err(|_| io::Error::other("io_uring SQ full (send)"))?;
            }
            st.ring.submit()?;
            Ok(data.len())
        }
    }
}

// ---------------------------------------------------------------------
// Wire (AF_XDP NIC-bypass) datagram backend.
//
// The RLC datagram rides a hand-built Ethernet+IPv4+UDP frame through a
// `WireSocket`, bypassing the kernel networking stack. Point-to-point: the
// single peer's MAC is configured (no general ARP needed for a sender <->
// receiver link). Linux-only for now (AF_XDP); behind the wire-locale
// feature. Config via SUBETHA_WIRE_{IFNAME,LOCAL_IP,LOCAL_MAC,PEER_MAC}.
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Link-speed gate: the kernel-bypass Wire backend only beats plain UDP
// once the link is fast enough that the per-packet syscall path - not the
// link - is the bottleneck. The auto path consults the detected link
// speed of the configured Wire interface against SUBETHA_WIRE_MIN_GBPS.
// ---------------------------------------------------------------------

/// Detected link speed (bits/sec) of `ifname`, or `None` when it cannot be
/// determined (unknown speed, or a pure-software netmap port - `vale*` /
/// pipe - with no physical link). Linux reads `/sys/class/net/<if>/speed`;
/// FreeBSD reads the AF_LINK `if_data.ifi_baudrate`. Exposed so callers can
/// see why the gate chose a backend.
#[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub fn link_speed_bps(ifname: &str) -> Option<u64> {
    // A netmap spec like "netmap:em0" gauges the underlying NIC; a VALE
    // switch / pipe ("vale0:a") has no physical link to measure.
    let phys = ifname.strip_prefix("netmap:").unwrap_or(ifname);
    if phys.starts_with("vale") || phys.contains(':') || phys.contains('{') {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        // /sys/class/net/<if>/speed: negotiated link speed in Mbit/s, or -1.
        let mbps: i64 = std::fs::read_to_string(format!("/sys/class/net/{phys}/speed"))
            .ok()?
            .trim()
            .parse()
            .ok()?;
        (mbps > 0).then_some(mbps as u64 * 1_000_000)
    }
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    {
        link_speed_baudrate(phys)
    }
}

/// FreeBSD / macOS link speed via the AF_LINK `if_data.ifi_baudrate` (the
/// link's negotiated line rate in bits/sec), read from `getifaddrs`.
#[cfg(all(feature = "wire-locale", any(target_os = "freebsd", target_os = "macos")))]
fn link_speed_baudrate(ifname: &str) -> Option<u64> {
    use std::ffi::CStr;
    let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut ifap) } != 0 {
        return None;
    }
    let mut speed = None;
    let mut cur = ifap;
    while !cur.is_null() {
        // SAFETY: getifaddrs returned a valid NUL-terminated linked list; we
        // only read each node's name / addr / data through live pointers and
        // stop at the null terminator.
        let ifa = unsafe { &*cur };
        if !ifa.ifa_name.is_null() && !ifa.ifa_addr.is_null() && !ifa.ifa_data.is_null() {
            let name = unsafe { CStr::from_ptr(ifa.ifa_name) }.to_string_lossy();
            let family = unsafe { (*ifa.ifa_addr).sa_family } as i32;
            if name == ifname && family == libc::AF_LINK {
                let baud =
                    unsafe { (*(ifa.ifa_data as *const libc::if_data)).ifi_baudrate };
                if baud > 0 {
                    // `ifi_baudrate` is u64 on FreeBSD but u32 on Darwin.
                    #[cfg(target_os = "freebsd")]
                    {
                        speed = Some(baud);
                    }
                    #[cfg(target_os = "macos")]
                    {
                        speed = Some(u64::from(baud));
                    }
                }
            }
        }
        cur = ifa.ifa_next;
    }
    unsafe { libc::freeifaddrs(ifap) };
    speed
}

/// The link-speed gate threshold (bits/sec). Default 10 Gbit/s - the rough
/// crossover where the per-packet syscall / copy path, not the link, is the
/// bottleneck, so kernel-bypass starts to pay. `SUBETHA_WIRE_MIN_GBPS`
/// overrides it; `0` disables the gate (always engage Wire when configured -
/// e.g. for a software VALE switch, which has no physical link to gauge).
#[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
fn wire_min_link_bps() -> u64 {
    std::env::var("SUBETHA_WIRE_MIN_GBPS")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|g| (g * 1e9) as u64)
        .unwrap_or(10_000_000_000)
}

/// Whether `ifname`'s detected link is fast enough for the Wire backend to
/// win. Unknown speed -> `false` (conservative: don't pay the bypass
/// overhead on a link we cannot confirm is line-rate). Threshold `0` ->
/// always `true`. Exposed so callers can introspect the gate decision.
#[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub fn wire_gate_admits(ifname: &str) -> bool {
    wire_link_fast_enough(ifname)
}

#[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
fn wire_link_fast_enough(ifname: &str) -> bool {
    let threshold = wire_min_link_bps();
    if threshold == 0 {
        return true;
    }
    link_speed_bps(ifname).map(|bps| bps >= threshold).unwrap_or(false)
}

#[cfg(all(feature = "wire-locale", any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
mod wire_backend {
    use super::*;
    use std::cell::RefCell;
    use std::net::{IpAddr, Ipv4Addr};

    use crate::locale_wire::WireSocket;

    const ETH_HDR: usize = 14;
    const IP_HDR: usize = 20;
    const UDP_HDR: usize = 8;
    const HDRS: usize = ETH_HDR + IP_HDR + UDP_HDR; // 42

    fn ipv4_csum(h: &[u8]) -> u16 {
        let mut sum = 0u32;
        let mut i = 0;
        while i + 1 < h.len() {
            sum += u16::from_be_bytes([h[i], h[i + 1]]) as u32;
            i += 2;
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        !(sum as u16)
    }

    /// Build Eth+IPv4+UDP around `payload` into `out`. The args are the frame
    /// fields themselves, so grouping them into a struct would not aid clarity.
    #[allow(clippy::too_many_arguments)]
    fn build_frame(
        dst_mac: [u8; 6],
        src_mac: [u8; 6],
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) {
        out.clear();
        out.resize(HDRS + payload.len(), 0);
        out[0..6].copy_from_slice(&dst_mac);
        out[6..12].copy_from_slice(&src_mac);
        out[12..14].copy_from_slice(&0x0800u16.to_be_bytes());
        out[14] = 0x45;
        out[16..18].copy_from_slice(&((IP_HDR + UDP_HDR + payload.len()) as u16).to_be_bytes());
        out[22] = 64;
        out[23] = 17;
        out[26..30].copy_from_slice(&src_ip);
        out[30..34].copy_from_slice(&dst_ip);
        let c = ipv4_csum(&out[14..34]);
        out[24..26].copy_from_slice(&c.to_be_bytes());
        out[34..36].copy_from_slice(&src_port.to_be_bytes());
        out[36..38].copy_from_slice(&dst_port.to_be_bytes());
        out[38..40].copy_from_slice(&((UDP_HDR + payload.len()) as u16).to_be_bytes());
        out[42..].copy_from_slice(payload);
    }

    /// Parse an Eth+IPv4+UDP frame -> (src_ip, src_port, dst_port, payload
    /// offset, payload len). None if it is not IPv4/UDP.
    fn parse_frame(f: &[u8]) -> Option<([u8; 4], u16, u16, usize, usize)> {
        if f.len() < HDRS || f[12..14] != 0x0800u16.to_be_bytes() {
            return None;
        }
        let ihl = (f[14] & 0x0f) as usize * 4;
        if ihl < IP_HDR || f[23] != 17 {
            return None;
        }
        let l4 = ETH_HDR + ihl;
        if f.len() < l4 + UDP_HDR {
            return None;
        }
        let src_ip = [f[26], f[27], f[28], f[29]];
        let src_port = u16::from_be_bytes([f[l4], f[l4 + 1]]);
        let dst_port = u16::from_be_bytes([f[l4 + 2], f[l4 + 3]]);
        let udp_len = u16::from_be_bytes([f[l4 + 4], f[l4 + 5]]) as usize;
        let pstart = l4 + UDP_HDR;
        let plen = udp_len.saturating_sub(UDP_HDR).min(f.len() - pstart);
        Some((src_ip, src_port, dst_port, pstart, plen))
    }

    fn parse_mac(s: &str) -> Option<[u8; 6]> {
        let mut mac = [0u8; 6];
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 6 {
            return None;
        }
        for (i, p) in parts.iter().enumerate() {
            mac[i] = u8::from_str_radix(p, 16).ok()?;
        }
        Some(mac)
    }

    fn parse_ipv4(s: &str) -> Option<[u8; 4]> {
        s.parse::<Ipv4Addr>().ok().map(|a| a.octets())
    }

    /// A datagram socket whose wire is a `WireSocket` (AF_XDP on Linux,
    /// netmap on FreeBSD, BPF on macOS). Same surface as the UDP / io_uring backends; the
    /// kernel stack is bypassed. The interface in `SUBETHA_WIRE_IFNAME` is a
    /// NIC name on Linux and a netmap port spec (e.g. `vale0:a`,
    /// `netmap:em0`) on FreeBSD.
    pub struct WireDgram {
        wire: RefCell<WireSocket>,
        scratch: RefCell<Vec<u8>>,
        local_ip: [u8; 4],
        local_mac: [u8; 6],
        peer_mac: [u8; 6],
        local_port: u16,
    }

    impl WireDgram {
        pub fn from_env(local_port: u16) -> io::Result<Self> {
            let getv = |k: &str| -> io::Result<String> {
                std::env::var(k).map_err(|_| io::Error::other(format!("{k} unset")))
            };
            let ifname = getv("SUBETHA_WIRE_IFNAME")?;
            let local_ip = parse_ipv4(&getv("SUBETHA_WIRE_LOCAL_IP")?)
                .ok_or_else(|| io::Error::other("bad SUBETHA_WIRE_LOCAL_IP"))?;
            let local_mac = parse_mac(&getv("SUBETHA_WIRE_LOCAL_MAC")?)
                .ok_or_else(|| io::Error::other("bad SUBETHA_WIRE_LOCAL_MAC"))?;
            let peer_mac = parse_mac(&getv("SUBETHA_WIRE_PEER_MAC")?)
                .ok_or_else(|| io::Error::other("bad SUBETHA_WIRE_PEER_MAC"))?;
            let wire = WireSocket::bind(&ifname, 0)?;
            Ok(Self {
                wire: RefCell::new(wire),
                scratch: RefCell::new(Vec::with_capacity(HDRS + 2048)),
                local_ip,
                local_mac,
                peer_mac,
                local_port,
            })
        }

        pub fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(self.local_ip)), self.local_port))
        }

        pub fn set_nonblocking(&self, _nb: bool) -> io::Result<()> {
            // The wire is polled with a timeout; it is always non-blocking.
            Ok(())
        }

        pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
            let dst_ip = match addr.ip() {
                IpAddr::V4(v) => v.octets(),
                IpAddr::V6(_) => return Err(io::Error::other("wire backend is IPv4-only")),
            };
            let mut scratch = self.scratch.borrow_mut();
            build_frame(
                self.peer_mac,
                self.local_mac,
                self.local_ip,
                dst_ip,
                self.local_port,
                addr.port(),
                buf,
                &mut scratch,
            );
            self.wire.borrow_mut().send_frame(&scratch)?;
            Ok(buf.len())
        }

        pub fn recv_with_kts(
            &self,
            buf: &mut [u8],
        ) -> io::Result<(usize, SocketAddr, Option<i128>)> {
            let mut fb = [0u8; 2048];
            let mut wire = self.wire.borrow_mut();
            loop {
                // Non-blocking poll of the RX ring (0 ms timeout).
                let n = wire.recv_frame(&mut fb, 0)?;
                if n == 0 {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock));
                }
                // The XSK sees all queue traffic; keep only IPv4/UDP frames
                // addressed to our port.
                if let Some((src_ip, src_port, dst_port, pstart, plen)) = parse_frame(&fb[..n])
                    && dst_port == self.local_port
                {
                    let copy = plen.min(buf.len());
                    buf[..copy].copy_from_slice(&fb[pstart..pstart + copy]);
                    let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(src_ip)), src_port);
                    return Ok((copy, from, None));
                }
                // Not ours; drain the next frame.
            }
        }
    }
}
