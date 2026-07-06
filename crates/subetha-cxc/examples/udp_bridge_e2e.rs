//! E2E for Sens-O-Matic, the reliable-UDP FEC bridge: the TLS-free,
//! adaptive cross-host transport. A `ReliableUdpReceiver` and `ReliableUdpSender`
//! ship a sequence over a real loopback `UdpSocket` with diagnostic loss
//! injected, so the FEC-primary / ARQ-fallback path engages, and the
//! receiver verifies exact in-order delivery.
//!
//! No tokio, no quinn, no rustls - the whole transport is `std`-only.
//!
//! Run: cargo run --release --example udp_bridge_e2e -p subetha-cxc

use std::time::{Duration, Instant};

use subetha_cxc::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};

fn main() {
    const N: u64 = 3000;
    let (k, r) = (8usize, 2usize);

    // Receiver on an ephemeral loopback port, with 15% injected loss so
    // FEC / ARQ actually engage.
    let mut recv = ReliableUdpReceiver::bind("127.0.0.1:0")
        .expect("bind receiver")
        .with_debug_loss(15, 7);
    let recv_addr = recv.local_addr().expect("recv addr");

    let rx = std::thread::spawn(move || {
        let mut got: Vec<u64> = Vec::with_capacity(N as usize);
        let start = Instant::now();
        while (got.len() as u64) < N {
            if start.elapsed() > Duration::from_secs(30) {
                panic!("receiver timed out at {} / {N}", got.len());
            }
            for item in recv.poll().expect("poll") {
                got.push(u64::from_le_bytes(item.try_into().expect("u64")));
            }
        }
        // Grace so the sender learns the final ack.
        for _ in 0..50 {
            recv.nudge_feedback().ok();
            std::thread::sleep(Duration::from_millis(2));
        }
        got
    });

    // Sender: interleave depth 8 (burst tolerance), tower on (whole-block
    // recovery). The adaptive controller drives parity from the loss the
    // receiver reports.
    let mut send = ReliableUdpSender::bind("127.0.0.1:0", recv_addr, k, r, 8).expect("bind sender");
    send.control().set_interleave_depth(8);
    send.enable_tower(8, 2);
    for i in 0..N {
        while send.flow_blocked() {
            send.drain_until_acked(Duration::from_millis(50)).ok();
        }
        send.send_item(&i.to_le_bytes()).expect("send_item");
    }
    send.flush().expect("flush");
    let acked = send
        .drain_until_acked(Duration::from_secs(15))
        .expect("drain");

    let got = rx.join().expect("receiver thread");
    let expected: Vec<u64> = (0..N).collect();
    assert_eq!(got, expected, "every item delivered exactly once, in order");
    println!(
        "E2E OK: {N} items round-tripped over Sens-O-Matic (FEC k={k}/r={r}, interleave=8, \
         tower on) at 15% injected loss; fully_acked={acked}. No TLS, std-only."
    );
}
