//! E2E: raw Ethernet frames through the netmap `WireSocket` (FreeBSD),
//! with the kernel networking stack bypassed.
//!
//! Opens two ports on a netmap VALE software switch - `vale0:a` and
//! `vale0:b` - and streams `N` sequence-numbered raw Ethernet frames from
//! a -> b through the in-kernel switch, with no socket / IP stack
//! involved. Each frame is broadcast (dst `ff:ff:ff:ff:ff:ff`) so the
//! VALE switch floods it to port b; the receiver verifies the frame's
//! sequence in order. VALE needs no NIC, so this proves the bypass
//! datapath end to end on any FreeBSD host with netmap.
//!
//! Only the netmap ring access is platform-gated (in `locale_wire`); the
//! send/recv verb shape matches the Linux AF_XDP and Windows XDP wires.
//!
//! Needs root (`/dev/netmap` + VALE) and the wire-locale feature.
//!
//! Run (root):
//!     cargo build --release --features wire-locale --example wire_netmap_e2e -p subetha-cxc
//!     sudo target/release/examples/wire_netmap_e2e

#[cfg(all(target_os = "freebsd", feature = "wire-locale"))]
fn main() {
    use std::time::{Duration, Instant};
    use subetha_cxc::locale_wire::WireSocket;

    if unsafe { libc::geteuid() } != 0 {
        eprintln!("wire_netmap_e2e must run as root (/dev/netmap + VALE).");
        std::process::exit(0);
    }

    const N: u32 = 2000;
    const FRAME: usize = 64;

    println!("=== WireSocket netmap (VALE) raw-frame E2E (kernel bypass) ===");

    // Port a creates the vale0 switch and attaches; port b attaches to it.
    let mut a = match WireSocket::bind("vale0:a", 0) {
        Ok(s) => s,
        Err(e) => {
            println!("netmap VALE unavailable: {e}");
            println!("  (needs the netmap device + VALE; /dev/netmap is in GENERIC.)");
            std::process::exit(0);
        }
    };
    let mut b = WireSocket::bind("vale0:b", 0).expect("bind vale0:b");
    // Let the switch attach both ports before forwarding begins.
    std::thread::sleep(Duration::from_millis(300));
    println!("[init] vale0:a + vale0:b attached to the VALE switch");

    let mut frame = [0u8; FRAME];
    frame[0..6].copy_from_slice(&[0xff; 6]); // dst: broadcast -> flood to b
    frame[6..12].copy_from_slice(&[0x02, 0, 0, 0x77, 0, 0x0a]); // src
    frame[12..14].copy_from_slice(&0x88b5u16.to_be_bytes()); // experimental EtherType

    // Blocking single-frame receive with a deadline.
    let recv_one = |b: &mut WireSocket, buf: &mut [u8]| -> u32 {
        let start = Instant::now();
        loop {
            match b.recv_frame(buf, 200).expect("recv_frame") {
                0 => {
                    if start.elapsed() > Duration::from_secs(3) {
                        panic!("recv timed out waiting for a frame");
                    }
                }
                n => {
                    assert!(n >= 18, "frame too short: {n}");
                    return u32::from_le_bytes(buf[14..18].try_into().unwrap());
                }
            }
        }
    };

    // Lock-step send -> receive so the VALE rings never overflow: every
    // frame is verified delivered, in order, before the next is sent.
    let mut recvbuf = [0u8; 2048];
    let t0 = Instant::now();
    for seq in 0..N {
        frame[14..18].copy_from_slice(&seq.to_le_bytes());
        a.send_frame(&frame).expect("send_frame");
        let got = recv_one(&mut b, &mut recvbuf);
        assert_eq!(got, seq, "out-of-order: expected {seq}, got {got}");
    }
    let elapsed = t0.elapsed();

    println!();
    println!("=== Result ===");
    println!("  frames:    {N} x {FRAME}B raw Ethernet, a -> VALE -> b, in order");
    println!("  elapsed:   {elapsed:?}");
    println!("  integrity: PASS (netmap kernel-bypass raw-frame round trip)");
}

#[cfg(not(all(target_os = "freebsd", feature = "wire-locale")))]
fn main() {
    eprintln!("wire_netmap_e2e needs FreeBSD + --features wire-locale (netmap).");
}
