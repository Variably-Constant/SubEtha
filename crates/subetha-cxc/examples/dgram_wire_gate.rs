//! E2E: the link-speed gate that decides whether the RLC transport's
//! datagram backend engages the NIC-bypass Wire path (AF_XDP on Linux,
//! netmap on FreeBSD) or falls back to io_uring / UDP.
//!
//! Kernel-bypass only beats plain UDP once the link is fast enough that the
//! per-packet syscall path - not the link - is the bottleneck. So the auto
//! selector reads the configured interface's detected link speed and engages
//! Wire only at or above the gate (`SUBETHA_WIRE_MIN_GBPS`, default 10
//! Gbit/s). This reports three observable facts for one run:
//!
//!   1. the detected link speed of `SUBETHA_WIRE_DETECT_IF` (or
//!      `SUBETHA_WIRE_IFNAME`),
//!   2. whether the gate admits Wire at the current threshold, and
//!   3. (when `WIRE_GATE_BIND=1`) which backend `DgramSock::wrap` actually
//!      resolves to in auto mode - flipping the threshold flips the backend.
//!
//! The detection + gate steps need no root; the bind step needs the
//! `SUBETHA_WIRE_*` config + root (AF_XDP / netmap). Run it under the
//! orchestrating harness, which varies `SUBETHA_WIRE_MIN_GBPS` to show the
//! selection flip.
//!
//! Run:
//!     SUBETHA_WIRE_DETECT_IF=eth0 SUBETHA_WIRE_MIN_GBPS=10 \
//!       cargo run --release --features wire-locale --example dgram_wire_gate -p subetha-cxc

#[cfg(all(any(target_os = "linux", target_os = "freebsd"), feature = "wire-locale"))]
fn main() {
    use subetha_cxc::dgram::{link_speed_bps, wire_gate_admits};

    let detect_if = std::env::var("SUBETHA_WIRE_DETECT_IF")
        .or_else(|_| std::env::var("SUBETHA_WIRE_IFNAME"))
        .unwrap_or_default();
    let threshold_gbps = std::env::var("SUBETHA_WIRE_MIN_GBPS").unwrap_or_else(|_| "10".into());

    println!("=== DgramSock link-speed gate ===");
    println!("  interface:        {detect_if}");
    match link_speed_bps(&detect_if) {
        Some(bps) => println!("  detected link:    {:.1} Gbit/s", bps as f64 / 1e9),
        None => println!("  detected link:    unknown (no physical link / unreadable)"),
    }
    println!("  gate threshold:   {threshold_gbps} Gbit/s (SUBETHA_WIRE_MIN_GBPS)");
    let admits = wire_gate_admits(&detect_if);
    println!("  gate admits Wire: {admits}");

    // Optionally exercise the real selection path: wrap a fresh socket in
    // AUTO mode and report the backend the gate resolved to. Requires the
    // SUBETHA_WIRE_* config + root for the actual Wire bind.
    if std::env::var("WIRE_GATE_BIND").as_deref() == Ok("1") {
        use std::net::UdpSocket;
        use subetha_cxc::dgram::DgramSock;
        // Auto mode: ensure SUBETHA_DGRAM is not forcing a backend.
        if std::env::var_os("SUBETHA_DGRAM").is_some() {
            eprintln!("  (note: SUBETHA_DGRAM is set; unset it to exercise the auto gate)");
        }
        let sock = UdpSocket::bind("0.0.0.0:0").expect("bind udp");
        let d = DgramSock::wrap(sock);
        println!("  resolved backend: {:?}  <- DgramSock::wrap auto-selection", d.backend());
    }
}

#[cfg(not(all(any(target_os = "linux", target_os = "freebsd"), feature = "wire-locale")))]
fn main() {
    eprintln!("dgram_wire_gate needs Linux/FreeBSD + --features wire-locale.");
}
