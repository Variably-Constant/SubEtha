//! E2E: the RLC transport over the Wire (AF_XDP NIC-bypass) datagram backend.
//!
//! The RLC datagrams ride raw Ethernet+IPv4+UDP frames through a `WireSocket`,
//! bypassing the kernel networking stack entirely. To give the two AF_XDP
//! sockets a real L2 boundary to cross (two XSKs on the same-namespace veth
//! pair do not deliver to each other), the orchestrator puts each end of a
//! veth pair in its own network namespace and runs the RLC sender in one, the
//! receiver in the other. A real `SensOMaticRlcSender` -> `SensOMaticRlcReceiver` transfer of
//! `N` items is verified delivered in order, over the Wire backend.
//!
//! Needs root (netns + veth + XDP attach) and the wire-locale feature.
//!
//! Run (root):
//!     cargo build --release --features wire-locale --example rlc_wire_e2e -p subetha-cxc
//!     sudo target/release/examples/rlc_wire_e2e

#[cfg(all(target_os = "linux", feature = "wire-locale"))]
mod e2e {
    use std::process::Command;
    use std::time::{Duration, Instant};
    use subetha_cxc::sens_rlc::{SensOMaticRlcReceiver, SensOMaticRlcSender};

    const NS1: &str = "sxe_rw1";
    const NS2: &str = "sxe_rw2";
    const VETH1: &str = "sxe_rw_a";
    const VETH2: &str = "sxe_rw_b";
    const IP1: &str = "10.77.0.1";
    const IP2: &str = "10.77.0.2";
    const MAC1: &str = "02:00:00:77:00:01";
    const MAC2: &str = "02:00:00:77:00:02";
    const RECV_PORT: u16 = 5000;
    const SEND_PORT: u16 = 5001;
    const N: u64 = 5000;
    const ITEM_LEN: usize = 1024;
    const SYMBOL_LEN: usize = 1100;

    fn sh(cmd: &str) -> bool {
        Command::new("sh").arg("-c").arg(cmd).status().map(|s| s.success()).unwrap_or(false)
    }

    fn setup() {
        teardown();
        sh(&format!("ip netns add {NS1}"));
        sh(&format!("ip netns add {NS2}"));
        sh(&format!("ip link add {VETH1} type veth peer name {VETH2}"));
        sh(&format!("ip link set {VETH1} address {MAC1}"));
        sh(&format!("ip link set {VETH2} address {MAC2}"));
        sh(&format!("ip link set {VETH1} netns {NS1}"));
        sh(&format!("ip link set {VETH2} netns {NS2}"));
        sh(&format!("ip netns exec {NS1} ip addr add {IP1}/24 dev {VETH1}"));
        sh(&format!("ip netns exec {NS2} ip addr add {IP2}/24 dev {VETH2}"));
        sh(&format!("ip netns exec {NS1} ip link set {VETH1} up"));
        sh(&format!("ip netns exec {NS2} ip link set {VETH2} up"));
        sh(&format!("ip netns exec {NS1} ip link set lo up"));
        sh(&format!("ip netns exec {NS2} ip link set lo up"));
    }

    fn teardown() {
        sh(&format!("ip netns del {NS1} 2>/dev/null"));
        sh(&format!("ip netns del {NS2} 2>/dev/null"));
        sh(&format!("ip link del {VETH1} 2>/dev/null"));
    }

    /// Receiver child (runs inside NS2 via `ip netns exec`).
    pub fn run_recv() -> ! {
        let mut recv = SensOMaticRlcReceiver::bind(format!("0.0.0.0:{RECV_PORT}"), SYMBOL_LEN)
            .expect("recv bind");
        eprintln!("[recv] backend={:?}", recv.dgram_backend());
        let mut got: Vec<u64> = Vec::new();
        let start = Instant::now();
        while (got.len() as u64) < N {
            if start.elapsed() > Duration::from_secs(60) {
                eprintln!("[recv] timeout at {}/{N}", got.len());
                std::process::exit(2);
            }
            for item in recv.poll().unwrap() {
                got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
            }
        }
        for _ in 0..100 {
            recv.poll().ok();
            std::thread::sleep(Duration::from_millis(2));
        }
        let expected: Vec<u64> = (0..N).collect();
        if got == expected {
            eprintln!("[recv] OK: {N} items in order (recovered {})", recv.rlc_recovered());
            std::process::exit(0);
        }
        eprintln!("[recv] MISMATCH");
        std::process::exit(1);
    }

    /// Sender child (runs inside NS1 via `ip netns exec`).
    pub fn run_send() -> ! {
        let peer = format!("{IP2}:{RECV_PORT}").parse().unwrap();
        let mut send = SensOMaticRlcSender::bind(format!("0.0.0.0:{SEND_PORT}"), peer, 16, 2, 15, SYMBOL_LEN)
            .expect("send bind");
        eprintln!("[send] backend={:?}", send.dgram_backend());
        for i in 0..N {
            let mut item = vec![0u8; ITEM_LEN];
            item[..8].copy_from_slice(&i.to_le_bytes());
            send.send_item(&item).expect("send_item");
        }
        send.drain_until_acked(N as u32, Duration::from_secs(60)).expect("drain");
        std::process::exit(0);
    }

    pub fn orchestrate() {
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("rlc_wire_e2e must run as root (netns + veth + XDP attach).");
            std::process::exit(0);
        }
        println!("=== RLC transport over the Wire (AF_XDP NIC-bypass) backend ===");
        setup();
        println!("[setup] {NS1}({VETH1},{IP1}) <-> {NS2}({VETH2},{IP2}) via veth");

        let exe = std::env::current_exe().unwrap();
        let exe = exe.to_string_lossy().to_string();

        // Receiver in NS2.
        let recv = Command::new("ip")
            .args(["netns", "exec", NS2, "env",
                   "SUBETHA_DGRAM=wire",
                   &format!("SUBETHA_WIRE_IFNAME={VETH2}"),
                   &format!("SUBETHA_WIRE_LOCAL_IP={IP2}"),
                   &format!("SUBETHA_WIRE_LOCAL_MAC={MAC2}"),
                   &format!("SUBETHA_WIRE_PEER_MAC={MAC1}"),
                   &exe, "recv"])
            .spawn()
            .expect("spawn recv");
        std::thread::sleep(Duration::from_millis(400));

        // Sender in NS1.
        let t0 = Instant::now();
        let send_status = Command::new("ip")
            .args(["netns", "exec", NS1, "env",
                   "SUBETHA_DGRAM=wire",
                   &format!("SUBETHA_WIRE_IFNAME={VETH1}"),
                   &format!("SUBETHA_WIRE_LOCAL_IP={IP1}"),
                   &format!("SUBETHA_WIRE_LOCAL_MAC={MAC1}"),
                   &format!("SUBETHA_WIRE_PEER_MAC={MAC2}"),
                   &exe, "send"])
            .status()
            .expect("run send");
        let recv_status = { let mut r = recv; r.wait().expect("reap recv") };
        let elapsed = t0.elapsed();

        teardown();

        let mb = N as f64 * ITEM_LEN as f64 / 1e6;
        println!();
        println!("=== Result ===");
        println!("  send exit: {send_status}, recv exit: {recv_status}");
        if send_status.success() && recv_status.success() {
            println!("  items:      {N} x {ITEM_LEN}B delivered in order over the Wire backend");
            println!("  elapsed:    {elapsed:?}");
            println!("  throughput: {:.1} MB/s", mb / elapsed.as_secs_f64());
            println!("  integrity:  PASS (RLC transport over AF_XDP, kernel stack bypassed)");
        } else {
            eprintln!("  FAILED");
            std::process::exit(1);
        }
    }
}

#[cfg(all(target_os = "linux", feature = "wire-locale"))]
fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("recv") => e2e::run_recv(),
        Some("send") => e2e::run_send(),
        _ => e2e::orchestrate(),
    }
}

#[cfg(not(all(target_os = "linux", feature = "wire-locale")))]
fn main() {
    eprintln!("rlc_wire_e2e needs Linux + --features wire-locale.");
}
