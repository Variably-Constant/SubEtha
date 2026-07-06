//! E2E: raw Ethernet frames through the BPF `WireSocket` (macOS), with the
//! kernel networking stack bypassed.
//!
//! Binds two BPF endpoints to a `feth` virtual-Ethernet pair - `feth0` and
//! `feth1`, peered like a veth cable - and streams `N` sequence-numbered raw
//! Ethernet frames from `feth0` -> `feth1`, with no socket / IP stack
//! involved. Each frame is broadcast (dst `ff:ff:ff:ff:ff:ff`) so the
//! unpromiscuous receiver still gets it; the receiver verifies the frame's
//! sequence in order. This proves the BPF bypass datapath end to end.
//!
//! Only the BPF device access is platform-gated (in `locale_wire`); the
//! send/recv verb shape matches the Linux AF_XDP, FreeBSD netmap, and
//! Windows XDP wires.
//!
//! Needs root (`/dev/bpf*`) and a `feth` pair, plus the wire-locale feature:
//!     sudo ifconfig feth0 create; sudo ifconfig feth1 create
//!     sudo ifconfig feth0 peer feth1; sudo ifconfig feth0 up; sudo ifconfig feth1 up
//!
//! Run (root):
//!     cargo build --release --features wire-locale --example wire_feth_e2e -p subetha-cxc
//!     sudo target/release/examples/wire_feth_e2e

#[cfg(all(target_os = "macos", feature = "wire-locale"))]
fn main() {
    use std::time::{Duration, Instant};
    use subetha_cxc::locale_wire::WireSocket;

    if unsafe { libc::geteuid() } != 0 {
        eprintln!("wire_feth_e2e must run as root (/dev/bpf access).");
        std::process::exit(0);
    }

    const N: u32 = 2000;
    const FRAME: usize = 64;

    println!("=== WireSocket BPF (feth pair) raw-frame E2E (kernel bypass) ===");

    // feth0 transmits, feth1 receives (the peer link delivers feth0's output
    // to feth1's input, where the BPF endpoint captures it).
    let mut a = match WireSocket::bind("feth0", 0) {
        Ok(s) => s,
        Err(e) => {
            println!("BPF bind to feth0 unavailable: {e}");
            println!("  (create the pair: sudo ifconfig feth0 create; \
                      feth1 create; feth0 peer feth1; both up.)");
            std::process::exit(0);
        }
    };
    let mut b = WireSocket::bind("feth1", 0).expect("bind feth1");
    println!("[init] BPF endpoints bound to feth0 (tx) + feth1 (rx)");

    let mut frame = [0u8; FRAME];
    frame[0..6].copy_from_slice(&[0xff; 6]); // dst: broadcast
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

    // Lock-step send -> receive so the kernel ring never overflows: every
    // frame is verified delivered, in order, before the next is sent.
    let mut recvbuf = [0u8; 4096];
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
    println!("  frames:    {N} x {FRAME}B raw Ethernet, feth0 -> feth1, in order");
    println!("  elapsed:   {elapsed:?}");
    println!("  integrity: PASS (BPF kernel-bypass raw-frame round trip)");
}

#[cfg(not(all(target_os = "macos", feature = "wire-locale")))]
fn main() {
    eprintln!("wire_feth_e2e needs macOS + --features wire-locale (BPF).");
}
