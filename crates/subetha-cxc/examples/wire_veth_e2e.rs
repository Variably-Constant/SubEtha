//! E2E: the `Locale::Wire` AF_XDP datapath over a `veth` pair.
//!
//! Stands up a veth pair and exercises the AF_XDP [`WireSocket`] against
//! an `AF_PACKET` raw socket that plays the role of the wire / NIC peer
//! (in production `WireSocket` talks to a real NIC; AF_PACKET injects and
//! captures frames on the veth exactly as a NIC would). Both directions
//! of the bypass datapath are proven:
//!
//! - **RX bypass** (the substrate's primary path, NIC -> ring): the peer
//!   injects frames on one veth end; `WireSocket::recv_frame` reads them
//!   off the AF_XDP RX ring on the other end (libxdp's program redirects
//!   them in), socket stack bypassed.
//! - **TX bypass** (ring -> NIC): `WireSocket::send_frame` writes frames
//!   to the AF_XDP TX ring; the peer captures them on the wire.
//!
//! Each frame carries a sequence number, verified in order. A veth pair
//! stands in for a real NIC link so the datapath runs without touching a
//! physical interface.
//!
//! Needs root: veth creation + XDP program attach require privilege.
//!
//! Run (root):
//!     cargo build --release --features wire-locale --example wire_veth_e2e -p subetha-cxc
//!     sudo target/release/examples/wire_veth_e2e

#[cfg(all(target_os = "linux", feature = "wire-locale"))]
mod pktsock {
    //! Minimal AF_PACKET raw-socket peer: injects + captures full
    //! Ethernet frames on a specific interface (the "wire" / NIC stand-in).
    use std::io;
    use std::os::unix::io::RawFd;

    pub struct PktSock {
        fd: RawFd,
        ifindex: i32,
    }

    impl PktSock {
        pub fn open(if_name: &str) -> io::Result<Self> {
            let cname = std::ffi::CString::new(if_name).unwrap();
            let ifindex = unsafe { libc::if_nametoindex(cname.as_ptr()) } as i32;
            if ifindex == 0 {
                return Err(io::Error::last_os_error());
            }
            let proto = (libc::ETH_P_ALL as u16).to_be() as i32;
            let fd = unsafe { libc::socket(libc::AF_PACKET, libc::SOCK_RAW, proto) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut sll: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
            sll.sll_family = libc::AF_PACKET as u16;
            sll.sll_protocol = (libc::ETH_P_ALL as u16).to_be();
            sll.sll_ifindex = ifindex;
            let rc = unsafe {
                libc::bind(
                    fd,
                    &sll as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_ll>() as u32,
                )
            };
            if rc < 0 {
                let e = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(e);
            }
            Ok(Self { fd, ifindex })
        }

        /// Transmit a raw Ethernet frame out this interface.
        pub fn send(&self, frame: &[u8]) -> io::Result<usize> {
            let mut sll: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
            sll.sll_family = libc::AF_PACKET as u16;
            sll.sll_ifindex = self.ifindex;
            let n = unsafe {
                libc::sendto(
                    self.fd,
                    frame.as_ptr() as *const libc::c_void,
                    frame.len(),
                    0,
                    &sll as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_ll>() as u32,
                )
            };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }

        /// Receive a frame, waiting up to `timeout_ms`. `Ok(0)` on timeout.
        pub fn recv(&self, buf: &mut [u8], timeout_ms: i32) -> io::Result<usize> {
            let mut pfd = libc::pollfd { fd: self.fd, events: libc::POLLIN, revents: 0 };
            let p = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            if p < 0 {
                return Err(io::Error::last_os_error());
            }
            if p == 0 {
                return Ok(0);
            }
            let n = unsafe {
                libc::recv(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
            };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }
    }

    impl Drop for PktSock {
        fn drop(&mut self) {
            unsafe { libc::close(self.fd) };
        }
    }
}

#[cfg(all(target_os = "linux", feature = "wire-locale"))]
fn main() {
    use std::process::Command;

    const VETH_PEER: &str = "sxe_xdp_a"; // AF_PACKET wire peer
    const VETH_XDP: &str = "sxe_xdp_b"; // AF_XDP WireSocket under test
    const N: u32 = 200;
    const FRAME_LEN: usize = 64;

    fn ip(args: &[&str]) -> bool {
        Command::new("ip")
            .args(args)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    if unsafe { libc::geteuid() } != 0 {
        eprintln!("wire_veth_e2e must run as root (veth create + XDP attach).");
        eprintln!("  sudo target/release/examples/wire_veth_e2e");
        std::process::exit(0);
    }

    println!("=== Locale::Wire AF_XDP E2E (veth pair, socket stack bypassed) ===");

    ip(&["link", "del", VETH_PEER]);
    if !ip(&["link", "add", VETH_PEER, "type", "veth", "peer", "name", VETH_XDP]) {
        eprintln!("failed to create veth pair");
        std::process::exit(1);
    }
    ip(&["link", "set", VETH_PEER, "up"]);
    ip(&["link", "set", VETH_XDP, "up"]);
    println!("[setup] veth pair {VETH_PEER} (AF_PACKET peer) <-> {VETH_XDP} (AF_XDP) up");

    let result = run(VETH_PEER, VETH_XDP, N, FRAME_LEN);

    ip(&["link", "del", VETH_PEER]);

    match result {
        Ok((rx, tx, elapsed)) => {
            println!();
            println!("=== Result ===");
            println!("  RX bypass: {rx} frames NIC-peer -> AF_XDP RX ring, verified in order");
            println!("  TX bypass: {tx} frames AF_XDP TX ring -> NIC-peer, verified in order");
            println!("  elapsed:   {elapsed:?}");
            println!("  integrity: PASS (userspace RX/TX rings, socket stack bypassed)");
        }
        Err(e) => {
            eprintln!("E2E failed: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(all(target_os = "linux", feature = "wire-locale"))]
fn run(
    peer_if: &str,
    xdp_if: &str,
    n: u32,
    frame_len: usize,
) -> std::io::Result<(u32, u32, std::time::Duration)> {
    use pktsock::PktSock;
    use std::time::Instant;
    use subetha_cxc::locale_wire::WireSocket;

    let mut wire = WireSocket::bind(xdp_if, 0)?;
    let peer = PktSock::open(peer_if)?;
    println!("[bind] AF_XDP WireSocket on {xdp_if}, AF_PACKET peer on {peer_if}");

    let mut frame = vec![0xA5u8; frame_len];
    frame[0..6].copy_from_slice(&[0xff; 6]); // dst broadcast
    frame[6..12].copy_from_slice(&[0x02, 0, 0, 0, 0, 1]); // src
    frame[12..14].copy_from_slice(&0x88b5u16.to_be_bytes()); // experimental ethertype

    let mut out = vec![0u8; 2048];

    let seq_of = |buf: &[u8]| -> Option<u32> {
        if buf.len() >= 18 && buf[12..14] == 0x88b5u16.to_be_bytes() {
            Some(u32::from_le_bytes(buf[14..18].try_into().unwrap()))
        } else {
            None
        }
    };

    // Warm up so the first counted frame is not lost while the XDP
    // program settles; drain any stragglers.
    for w in 0..16u32 {
        frame[14..18].copy_from_slice(&(0xF000_0000 | w).to_le_bytes());
        peer.send(&frame)?;
        wire.recv_frame(&mut out, 200)?;
    }
    while wire.recv_frame(&mut out, 50)? != 0 {}

    let t0 = Instant::now();

    // --- RX bypass: peer injects, AF_XDP receives ---
    let mut rx_ok = 0u32;
    for seq in 0..n {
        frame[14..18].copy_from_slice(&seq.to_le_bytes());
        peer.send(&frame)?;
        let mut tries = 0;
        loop {
            let got = wire.recv_frame(&mut out, 500)?;
            if got > 0 && let Some(rseq) = seq_of(&out[..got]) {
                if rseq == seq {
                    rx_ok += 1;
                    break;
                } else if rseq & 0xF000_0000 != 0 {
                    // warmup straggler; ignore and keep polling
                } else {
                    return Err(std::io::Error::other(format!(
                        "RX out of order: got {rseq} want {seq}"
                    )));
                }
            }
            tries += 1;
            if tries > 40 {
                return Err(std::io::Error::other(format!("RX: no frame for seq {seq}")));
            }
        }
    }

    // Drain anything the peer may still hold from warmup.
    while peer.recv(&mut out, 50)? != 0 {}

    // --- TX bypass: AF_XDP transmits, peer captures ---
    let mut tx_ok = 0u32;
    for seq in 0..n {
        frame[14..18].copy_from_slice(&seq.to_le_bytes());
        wire.send_frame(&frame)?;
        let mut tries = 0;
        loop {
            let got = peer.recv(&mut out, 500)?;
            // A non-matching frame (other link traffic) just keeps polling.
            if got > 0 && let Some(rseq) = seq_of(&out[..got]) && rseq == seq {
                tx_ok += 1;
                break;
            }
            tries += 1;
            if tries > 40 {
                return Err(std::io::Error::other(format!("TX: peer missed seq {seq}")));
            }
        }
    }

    Ok((rx_ok, tx_ok, t0.elapsed()))
}

#[cfg(not(all(target_os = "linux", feature = "wire-locale")))]
fn main() {
    eprintln!("wire_veth_e2e needs Linux + --features wire-locale.");
}
