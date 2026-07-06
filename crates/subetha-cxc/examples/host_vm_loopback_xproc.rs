//! Cross-process E2E for the host-VM socket loopback path
//! ([`subetha_cxc::locale_vsock::HostVmSocket`]).
//!
//! Two independent processes stream over a host-VM socket that bypasses
//! the network stack entirely - Linux vsock (`AF_VSOCK`, loopback CID) or
//! a Windows Hyper-V socket (`AF_HYPERV`, `HV_GUID_LOOPBACK`). The parent
//! listens, the child connects loopback, and they run an echo protocol:
//! the parent sends `N` sequence numbers, the child echoes each, and the
//! parent verifies every one round-tripped in order. No TCP/IP, no NIC.
//!
//! The same code reaches a real guest<->host link by addressing a peer
//! CID / VmId instead of loopback; loopback is the same-host path that is
//! self-testable here. Only the socket family is gated; the listen /
//! accept / connect / echo logic is shared.
//!
//! Run:
//!     cargo run --release --example host_vm_loopback_xproc -p subetha-cxc

#[cfg(any(target_os = "linux", windows))]
fn main() {
    use std::time::{Duration, Instant};
    use subetha_cxc::locale_vsock::HostVmSocket;

    const N: u32 = 1000;

    // Receive exactly `buf.len()` bytes (stream sockets may chunk).
    fn recv_exact(sock: &HostVmSocket, buf: &mut [u8]) -> std::io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = sock.recv(&mut buf[filled..])?;
            if n == 0 {
                return Err(std::io::Error::other("peer closed"));
            }
            filled += n;
        }
        Ok(())
    }

    let args: Vec<String> = std::env::args().collect();

    // --- child connector role: echo N sequence numbers back ---
    if args.get(1).map(String::as_str) == Some("connect") {
        let port: u32 = args[2].parse().expect("port");
        let start = Instant::now();
        let conn = loop {
            match HostVmSocket::connect_loopback(port) {
                Ok(c) => break c,
                Err(_) if start.elapsed() < Duration::from_secs(5) => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => panic!("child connect: {e}"),
            }
        };
        let mut buf = [0u8; 4];
        for _ in 0..N {
            if recv_exact(&conn, &mut buf).is_err() {
                break;
            }
            if conn.send(&buf).is_err() {
                break;
            }
        }
        return;
    }

    // --- parent listener role: drive + verify the echo ---
    println!("=== HostVmSocket loopback CROSS-PROCESS E2E (vsock / Hyper-V socket) ===");
    let port: u32 = 0x6000 + (std::process::id() & 0x1fff);
    let listener = match HostVmSocket::listen_loopback(port) {
        Ok(l) => l,
        Err(e) => {
            println!("host-vm loopback unavailable: {e}");
            #[cfg(target_os = "linux")]
            println!("  load the loopback transport: sudo modprobe vsock_loopback");
            #[cfg(windows)]
            println!("  enable the Hyper-V / Virtual Machine Platform feature so the \
                      AF_HYPERV provider is registered.");
            return;
        }
    };
    println!("[parent] listening on loopback service port {port}");

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = std::process::Command::new(exe)
        .args(["connect", &port.to_string()])
        .spawn()
        .expect("spawn child");
    println!("[parent] spawned child pid={}", child.id());

    let conn = listener.accept().expect("accept");
    println!("[parent] child connected over the host-VM socket");

    let t0 = Instant::now();
    let mut buf = [0u8; 4];
    for i in 0..N {
        conn.send(&i.to_le_bytes()).expect("send seq");
        recv_exact(&conn, &mut buf).expect("recv echo");
        let got = u32::from_le_bytes(buf);
        assert_eq!(got, i, "echo out of order: got {got}, expected {i}");
    }
    let elapsed = t0.elapsed();
    let status = child.wait().expect("reap child");

    println!();
    println!("=== Result ===");
    println!("  messages:  {N} round-tripped in order");
    println!("  elapsed:   {elapsed:?}");
    println!("  child exit: {status}");
    assert!(status.success(), "child failed");
    println!("  integrity: PASS (two processes streamed over a host-VM socket,");
    println!("    no TCP/IP and no NIC - hypervisor/loopback transport only)");
}

#[cfg(not(any(target_os = "linux", windows)))]
fn main() {
    eprintln!("host_vm_loopback_xproc needs vsock (Linux) or Hyper-V sockets (Windows).");
}
