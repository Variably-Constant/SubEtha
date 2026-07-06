//! Item 14: Trace mini-traceroute on the control stream + path asymmetry.
//!
//! Two signals about the *shape* of the path, both read without a separate probe
//! flow:
//!
//!  - **Mini-traceroute.** The sender emits a few `Trace` control datagrams at
//!    ascending IP TTL (1, 2, ...). A datagram whose TTL expires at an
//!    intermediate router draws an ICMP TimeExceeded back; on Linux that error
//!    is delivered on the socket's error queue (`IP_RECVERR` +
//!    `recvmsg(MSG_ERRQUEUE)`), carrying the offending router's address and the
//!    timestamp machinery for a per-hop RTT - a traceroute riding the transport's
//!    own socket, no second flow. The TTL that drew each reply is the hop index.
//!  - **Path asymmetry.** The forward hop count (how many hops the peer says our
//!    packets crossed, from its `Path` frame) versus the reverse hop count (how
//!    many hops the peer's feedback crossed, from our own received-TTL cmsg). A
//!    difference means the two directions are routed differently - which biases
//!    the per-hop RTT model, since a one-way delay no longer splits evenly.
//!
//! The error-queue read is a Linux / BSD capability (the per-platform matrix
//! lists no Windows path), so the traceroute half is `#[cfg(target_os =
//! "linux")]`; the asymmetry half is portable (it is pure hop-count arithmetic
//! over signals the control plane already carries).

use std::net::IpAddr;

/// One discovered hop on the path to the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceHop {
    /// The TTL at which this hop replied (1 = first router, 2 = second, ...).
    pub ttl: u8,
    /// The router that sent the ICMP TimeExceeded.
    pub addr: IpAddr,
    /// Round-trip time to this hop, microseconds.
    pub rtt_us: u64,
}

/// Forward-vs-reverse path asymmetry. The forward hop count is what the peer
/// reports about our packets; the reverse is what we observe about the peer's.
#[derive(Debug, Clone, Copy, Default)]
pub struct PathAsymmetry {
    forward_hops: u8,
    reverse_hops: u8,
    have_forward: bool,
    have_reverse: bool,
}

impl PathAsymmetry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the forward hop count (from the peer's `Path` frame about us).
    pub fn observe_forward(&mut self, hops: u8) {
        self.forward_hops = hops;
        self.have_forward = true;
    }

    /// Record the reverse hop count (from our received-TTL cmsg about the peer).
    pub fn observe_reverse(&mut self, hops: u8) {
        self.reverse_hops = hops;
        self.have_reverse = true;
    }

    pub fn forward(&self) -> Option<u8> {
        self.have_forward.then_some(self.forward_hops)
    }

    pub fn reverse(&self) -> Option<u8> {
        self.have_reverse.then_some(self.reverse_hops)
    }

    /// `|forward - reverse|`, or `None` until both directions are known. A
    /// nonzero value means the path is routed asymmetrically.
    pub fn asymmetry(&self) -> Option<u8> {
        if self.have_forward && self.have_reverse {
            Some(self.forward_hops.abs_diff(self.reverse_hops))
        } else {
            None
        }
    }
}

/// Enable the ICMP error queue on a socket so an expired-TTL probe's
/// TimeExceeded is delivered (Linux). A no-op elsewhere.
#[cfg(target_os = "linux")]
pub fn enable_icmp_errors(fd: std::os::fd::RawFd) {
    let on: libc::c_int = 1;
    // SAFETY: `fd` is a valid socket; `on` is a valid c_int that outlives the
    // call. IP_RECVERR turns on the per-socket error queue.
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_IP,
            libc::IP_RECVERR,
            &on as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
pub fn enable_icmp_errors(_fd: i32) {}

/// Send `payload` on the **connected** socket `fd` with the IP TTL set to `ttl`
/// for this one datagram (via an `IP_TTL` cmsg, so the socket's default TTL is
/// untouched). The socket must already be connected to the peer - `msg_name` is
/// left null, since a non-null name on a connected socket returns `EISCONN`.
/// `peer` is used only to skip an IPv6 peer (the hop-limit cmsg is a separate
/// spelling not needed for the netns / LAN proof). Linux only; a no-op elsewhere.
#[cfg(target_os = "linux")]
pub fn send_at_ttl(
    fd: std::os::fd::RawFd,
    peer: std::net::SocketAddr,
    payload: &[u8],
    ttl: u8,
) -> std::io::Result<()> {
    use std::mem::{size_of, zeroed};
    if !peer.is_ipv4() {
        return Ok(());
    }
    // SAFETY: every pointer below refers to a stack local that outlives the
    // sendmsg call; the cmsg buffer is sized by CMSG_SPACE and written through
    // CMSG_FIRSTHDR / CMSG_DATA exactly as the kernel ABI requires.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: payload.as_ptr() as *mut libc::c_void,
            iov_len: payload.len(),
        };
        let mut cbuf = [0u8; 64];
        let mut msg: libc::msghdr = zeroed();
        msg.msg_name = std::ptr::null_mut();
        msg.msg_namelen = 0;
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = libc::CMSG_SPACE(size_of::<libc::c_int>() as u32) as usize;

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(std::io::Error::other("CMSG_FIRSTHDR null"));
        }
        (*cmsg).cmsg_level = libc::IPPROTO_IP;
        (*cmsg).cmsg_type = libc::IP_TTL;
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<libc::c_int>() as u32) as usize;
        let ttl_i = ttl as libc::c_int;
        std::ptr::copy_nonoverlapping(
            &ttl_i as *const libc::c_int as *const u8,
            libc::CMSG_DATA(cmsg),
            size_of::<libc::c_int>(),
        );

        let n = libc::sendmsg(fd, &msg, 0);
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn send_at_ttl(
    _fd: i32,
    _peer: std::net::SocketAddr,
    _payload: &[u8],
    _ttl: u8,
) -> std::io::Result<()> {
    Ok(())
}

/// Drain the socket's error queue, returning, for each ICMP TimeExceeded found,
/// the offending router address and the bytes of the original probe it expired
/// (so the caller can read back the TTL it stamped and match the per-hop RTT).
/// Linux only.
#[cfg(target_os = "linux")]
pub fn drain_icmp_errors(fd: std::os::fd::RawFd) -> Vec<(IpAddr, Vec<u8>)> {
    use std::mem::{size_of, zeroed};
    let mut hops = Vec::new();
    // SAFETY: the msghdr and its buffers are stack locals living across each
    // recvmsg; the cmsg walk uses CMSG_FIRSTHDR / CMSG_NXTHDR / CMSG_DATA on a
    // buffer the kernel filled, and the offender sockaddr is read from the bytes
    // immediately after the sock_extended_err the kernel placed.
    unsafe {
        loop {
            let mut from: libc::sockaddr_in = zeroed();
            let mut buf = [0u8; 512];
            let mut cbuf = [0u8; 512];
            let mut iov = libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            };
            let mut msg: libc::msghdr = zeroed();
            msg.msg_name = &mut from as *mut _ as *mut libc::c_void;
            msg.msg_namelen = size_of::<libc::sockaddr_in>() as libc::socklen_t;
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
            msg.msg_controllen = cbuf.len();

            let n = libc::recvmsg(fd, &mut msg, libc::MSG_ERRQUEUE | libc::MSG_DONTWAIT);
            if n < 0 {
                break;
            }
            // The returned iov holds the original UDP payload of the expired
            // probe, so the caller can read back the TTL it stamped.
            let payload = buf[..(n as usize).min(buf.len())].to_vec();
            let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::IPPROTO_IP && (*cmsg).cmsg_type == libc::IP_RECVERR {
                    let ee = libc::CMSG_DATA(cmsg) as *const libc::sock_extended_err;
                    if (*ee).ee_origin == libc::SO_EE_ORIGIN_ICMP {
                        // The offender sockaddr_in follows the sock_extended_err
                        // (the SO_EE_OFFENDER macro is exactly this offset).
                        let off = (ee as *const u8).add(size_of::<libc::sock_extended_err>())
                            as *const libc::sockaddr_in;
                        // s_addr holds the address in network byte order, so its
                        // in-memory bytes ARE the octets a.b.c.d in order.
                        let octets = (*off).sin_addr.s_addr.to_ne_bytes();
                        hops.push((IpAddr::V4(std::net::Ipv4Addr::from(octets)), payload.clone()));
                    }
                }
                cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
            }
        }
    }
    hops
}

#[cfg(not(target_os = "linux"))]
pub fn drain_icmp_errors(_fd: i32) -> Vec<(IpAddr, Vec<u8>)> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asymmetry_is_none_until_both_directions_known() {
        let mut a = PathAsymmetry::new();
        assert_eq!(a.asymmetry(), None);
        a.observe_forward(3);
        assert_eq!(a.asymmetry(), None, "one direction is not enough");
        a.observe_reverse(3);
        assert_eq!(a.asymmetry(), Some(0), "a symmetric path reads 0");
    }

    #[test]
    fn asymmetry_counts_the_hop_difference() {
        let mut a = PathAsymmetry::new();
        a.observe_forward(5);
        a.observe_reverse(2);
        assert_eq!(a.asymmetry(), Some(3), "forward 5 vs reverse 2 -> 3");
        assert_eq!(a.forward(), Some(5));
        assert_eq!(a.reverse(), Some(2));
    }
}
