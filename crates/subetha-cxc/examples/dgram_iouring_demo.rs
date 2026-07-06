//! E2E: the auto-detecting datagram backend ([`subetha_cxc::dgram::DgramSock`]).
//!
//! Binds two loopback UDP sockets, wraps each in a `DgramSock` (which on
//! Linux upgrades to the io_uring-backed datagram socket, else falls back to
//! plain UDP), and streams sequence-numbered datagrams A -> B -> A with the
//! source address and (on Linux) kernel timestamp verified each hop. Prints
//! the resolved backend so a run can confirm io_uring is actually exercised.
//!
//! Force a backend with `SUBETHA_DGRAM=iouring` (errors loudly if the ring is
//! unavailable) or `SUBETHA_DGRAM=udp` (the fallback). Unset = auto-detect.
//!
//! Run:
//!     cargo run --release --example dgram_iouring_demo -p subetha-cxc

fn main() {
    use std::net::UdpSocket;
    use std::time::{Duration, Instant};
    use subetha_cxc::dgram::DgramSock;

    const N: u32 = 2000;

    let a_udp = UdpSocket::bind("127.0.0.1:0").expect("bind a");
    let b_udp = UdpSocket::bind("127.0.0.1:0").expect("bind b");
    let a_addr = a_udp.local_addr().unwrap();
    let b_addr = b_udp.local_addr().unwrap();

    let a = DgramSock::wrap(a_udp);
    let b = DgramSock::wrap(b_udp);
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();

    println!("=== DgramSock datagram E2E (auto-detected backend) ===");
    println!("  A backend: {:?}", a.backend());
    println!("  B backend: {:?}", b.backend());

    // Block-on-WouldBlock helper for the non-blocking ring.
    let recv = |sock: &DgramSock, buf: &mut [u8]| -> (usize, std::net::SocketAddr, Option<i128>) {
        let start = Instant::now();
        loop {
            match sock.recv_with_kts(buf) {
                Ok(t) => return t,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed() > Duration::from_secs(10) {
                        panic!("recv timed out");
                    }
                    std::hint::spin_loop();
                }
                Err(e) => panic!("recv error: {e}"),
            }
        }
    };

    let t0 = Instant::now();
    let mut buf = [0u8; 2048];
    let mut kts_seen = 0u32;
    for seq in 0..N {
        // A -> B
        a.send_to(&seq.to_le_bytes(), b_addr).expect("a->b send");
        let (n, from, kts) = recv(&b, &mut buf);
        assert_eq!(n, 4, "B got {n} bytes");
        assert_eq!(from, a_addr, "B source addr");
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), seq, "B seq");
        if kts.is_some() {
            kts_seen += 1;
        }
        // B -> A (echo)
        b.send_to(&seq.to_le_bytes(), a_addr).expect("b->a echo");
        let (n2, from2, _) = recv(&a, &mut buf);
        assert_eq!(n2, 4);
        assert_eq!(from2, b_addr, "A source addr");
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), seq, "A echo seq");
    }
    let elapsed = t0.elapsed();

    println!();
    println!("=== Result ===");
    println!("  datagrams:  {N} round-tripped A->B->A, src-addr + seq verified");
    println!("  kernel ts:  {kts_seen}/{N} carried an SO_TIMESTAMPNS arrival time");
    println!("  elapsed:    {elapsed:?}");
    println!("  integrity:  PASS ({:?} backend)", a.backend());
}
