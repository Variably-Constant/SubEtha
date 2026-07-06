//! E2E: the `Locale::Wire` XDP-for-Windows datapath across two real NICs.
//!
//! Auto-detects the Up Ethernet NICs, binds an XDP [`WireSocket`] to each
//! (the second gets the redirect program), and streams UDP frames from
//! one NIC out onto the wire and back in on the other - TX ring -> NIC ->
//! cable -> NIC -> RX ring, the socket stack bypassed end to end. Each
//! frame carries a sequence number in its payload, verified in order.
//!
//! The two NICs must share an L2 segment (a direct cable between them, or
//! both plugged into the same switch) so a unicast frame addressed to the
//! RX NIC's MAC is delivered there. A queue-0 XSK only sees queue-0
//! traffic, so the example temporarily forces the RX NIC to a single RSS
//! queue for the run and restores RSS afterwards (the self-contained
//! setup/teardown, mirroring the Linux side's ephemeral veth pair).
//!
//! Needs the XDP runtime driver installed and admin (binding + attaching
//! the XDP program and reconfiguring RSS are privileged).
//!
//! Run (admin):
//!     cargo build --release --features wire-locale --example wire_xdp_win_e2e -p subetha-cxc
//!     target\release\examples\wire_xdp_win_e2e.exe          (auto-detect)
//!     target\release\examples\wire_xdp_win_e2e.exe <tx_ifindex> <rx_ifindex>

#[cfg(all(windows, feature = "wire-locale"))]
fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    let mut i = 0;
    while i + 1 < header.len() {
        sum += u16::from_be_bytes([header[i], header[i + 1]]) as u32;
        i += 2;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Build an Ethernet + IPv4 + UDP frame. The 4-byte sequence number lives
/// at offset 42 (right after the UDP header); the IP/UDP checksums do not
/// cover the payload, so the seq can be rewritten without recomputing them.
#[cfg(all(windows, feature = "wire-locale"))]
fn build_udp_frame(dst_mac: [u8; 6], src_mac: [u8; 6], dst_port: u16, payload_len: usize) -> Vec<u8> {
    let total = 42 + payload_len;
    let mut f = vec![0u8; total];
    f[0..6].copy_from_slice(&dst_mac);
    f[6..12].copy_from_slice(&src_mac);
    f[12..14].copy_from_slice(&0x0800u16.to_be_bytes()); // IPv4

    f[14] = 0x45; // version 4, IHL 5
    f[16..18].copy_from_slice(&((20 + 8 + payload_len) as u16).to_be_bytes());
    f[22] = 64; // TTL
    f[23] = 17; // UDP
    f[26..30].copy_from_slice(&[10, 0, 0, 1]); // src IP
    f[30..34].copy_from_slice(&[10, 0, 0, 2]); // dst IP
    let csum = ipv4_checksum(&f[14..34]);
    f[24..26].copy_from_slice(&csum.to_be_bytes());

    f[34..36].copy_from_slice(&4321u16.to_be_bytes()); // UDP src port
    f[36..38].copy_from_slice(&dst_port.to_be_bytes()); // UDP dst port
    f[38..40].copy_from_slice(&((8 + payload_len) as u16).to_be_bytes());
    // UDP checksum left 0 (optional for IPv4).
    f
}

#[cfg(all(windows, feature = "wire-locale"))]
fn run_ps(cmd: &str) -> bool {
    std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", cmd])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(all(windows, feature = "wire-locale"))]
fn rss_enabled(if_index: u32) -> bool {
    run_ps(&format!(
        "$n=(Get-NetAdapter -InterfaceIndex {if_index}).Name; \
         if ((Get-NetAdapterRss -Name $n -EA SilentlyContinue).Enabled) {{ exit 0 }} else {{ exit 1 }}"
    ))
}

#[cfg(all(windows, feature = "wire-locale"))]
fn set_rss(if_index: u32, enable: bool) {
    let verb = if enable { "Enable-NetAdapterRss" } else { "Disable-NetAdapterRss" };
    if !run_ps(&format!("Get-NetAdapter -InterfaceIndex {if_index} | {verb} -EA SilentlyContinue")) {
        eprintln!("warning: {verb} on ifIndex {if_index} did not report success");
    }
}

#[cfg(all(windows, feature = "wire-locale"))]
fn wait_link_up(if_index: u32, secs: u32) {
    for _ in 0..secs {
        if run_ps(&format!(
            "if ((Get-NetAdapter -InterfaceIndex {if_index}).Status -eq 'Up') {{ exit 0 }} else {{ exit 1 }}"
        )) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

#[cfg(all(windows, feature = "wire-locale"))]
fn run_e2e(
    tx_nic: &subetha_cxc::locale_wire::NicInfo,
    rx_nic: &subetha_cxc::locale_wire::NicInfo,
) -> Result<std::time::Duration, String> {
    use subetha_cxc::locale_wire::WireSocket;

    const UDP_PORT: u16 = 1234;
    const N: u32 = 200;
    const PAYLOAD: usize = 22; // total frame 64 bytes
    const SEQ_OFF: usize = 42;

    // Bind the RX socket first (it attaches the redirect program), then TX.
    let mut rx = WireSocket::bind(rx_nic.if_index, 0, UDP_PORT)
        .map_err(|e| format!("RX WireSocket::bind failed: {e}"))?;
    let mut tx = WireSocket::bind(tx_nic.if_index, 0, UDP_PORT)
        .map_err(|e| format!("TX WireSocket::bind failed: {e}"))?;
    println!("[bind] XDP sockets bound on both NICs (queue 0, generic mode)");

    let mut frame = build_udp_frame(rx_nic.mac, tx_nic.mac, UDP_PORT, PAYLOAD);
    let mut out = vec![0u8; 2048];

    let seq_of = |buf: &[u8]| -> Option<u32> {
        if buf.len() >= SEQ_OFF + 4
            && buf[12..14] == 0x0800u16.to_be_bytes()
            && buf[23] == 17
            && buf[36..38] == UDP_PORT.to_be_bytes()
        {
            Some(u32::from_le_bytes(buf[SEQ_OFF..SEQ_OFF + 4].try_into().unwrap()))
        } else {
            None
        }
    };

    // Warm up so the first counted frame is not lost while the program
    // settles; drain stragglers.
    for w in 0..16u32 {
        frame[SEQ_OFF..SEQ_OFF + 4].copy_from_slice(&(0xF000_0000 | w).to_le_bytes());
        tx.send_frame(&frame).map_err(|e| format!("warmup send: {e}"))?;
        rx.recv_frame(&mut out, 200).map_err(|e| format!("warmup recv: {e}"))?;
    }
    while rx.recv_frame(&mut out, 50).map_err(|e| format!("drain: {e}"))? != 0 {}

    let t0 = std::time::Instant::now();
    for seq in 0..N {
        frame[SEQ_OFF..SEQ_OFF + 4].copy_from_slice(&seq.to_le_bytes());
        tx.send_frame(&frame).map_err(|e| format!("send: {e}"))?;
        let mut tries = 0;
        loop {
            let got = rx.recv_frame(&mut out, 500).map_err(|e| format!("recv: {e}"))?;
            if got > 0
                && let Some(rseq) = seq_of(&out[..got])
                && rseq == seq
            {
                break;
            }
            tries += 1;
            if tries > 40 {
                let s = rx.stats().unwrap_or_default();
                return Err(format!(
                    "RX missed seq {seq} (xsk stats: dropped={} truncated={} \
                     rx_invalid={} tx_invalid={})",
                    s.rx_dropped, s.rx_truncated, s.rx_invalid_descriptors,
                    s.tx_invalid_descriptors
                ));
            }
        }
    }
    Ok(t0.elapsed())
}

#[cfg(all(windows, feature = "wire-locale"))]
fn main() {
    use subetha_cxc::locale_wire::list_ethernet_nics;

    println!("=== Locale::Wire XDP-for-Windows E2E (two NICs, socket stack bypassed) ===");

    let all = match list_ethernet_nics() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("NIC enumeration failed: {e}");
            std::process::exit(1);
        }
    };
    let dump = |nics: &[subetha_cxc::locale_wire::NicInfo]| {
        for n in nics {
            println!("  ifIndex {} mac {:02x?}  {}", n.if_index, n.mac, n.description);
        }
    };

    // Explicit `<tx_ifindex> <rx_ifindex>` override, else auto-detect the
    // physical Ethernet NICs (excluding virtual / wireless adapters).
    let args: Vec<String> = std::env::args().collect();
    let (tx_nic, rx_nic) = if args.len() >= 3 {
        let want = |s: &str| -> u32 { s.parse().expect("ifindex must be a number") };
        let find = |idx: u32| all.iter().find(|n| n.if_index == idx).cloned();
        match (find(want(&args[1])), find(want(&args[2]))) {
            (Some(t), Some(r)) => (t, r),
            _ => {
                eprintln!("ifIndex not found among Up Ethernet NICs:");
                dump(&all);
                std::process::exit(1);
            }
        }
    } else {
        let is_virtual = |d: &str| {
            let d = d.to_lowercase();
            ["virtual", "hyper-v", "loopback", "wsl", "vpn", "bluetooth", "wi-fi", "wireless"]
                .iter()
                .any(|k| d.contains(k))
        };
        let phys: Vec<_> = all.iter().filter(|n| !is_virtual(&n.description)).cloned().collect();
        if phys.len() < 2 {
            println!("need 2 physical Up Ethernet NICs on a shared segment; found {}:", phys.len());
            dump(&all);
            println!("  override: wire_xdp_win_e2e <tx_ifindex> <rx_ifindex>");
            std::process::exit(0);
        }
        (phys[0].clone(), phys[1].clone())
    };

    let mac = |m: [u8; 6]| m.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":");
    println!(
        "[nics] TX ifIndex={} ({}) -> RX ifIndex={} ({})",
        tx_nic.if_index, mac(tx_nic.mac), rx_nic.if_index, mac(rx_nic.mac)
    );

    // A queue-0 XSK only sees queue-0 traffic; force the RX NIC to a single
    // RSS queue for the run, then restore.
    let restore_rss = rss_enabled(rx_nic.if_index);
    if restore_rss {
        println!("[rss] disabling RSS on RX NIC for single-queue capture (will restore)");
        set_rss(rx_nic.if_index, false);
        wait_link_up(rx_nic.if_index, 15);
    }

    let result = run_e2e(&tx_nic, &rx_nic);

    if restore_rss {
        set_rss(rx_nic.if_index, true);
        println!("[rss] restored RSS on RX NIC");
    }

    match result {
        Ok(elapsed) => {
            println!();
            println!("=== Result ===");
            println!("  frames:    200 TX-ring -> NIC -> wire -> NIC -> RX-ring, verified in order");
            println!("  elapsed:   {elapsed:?}");
            println!("  integrity: PASS (userspace TX/RX rings across two real NICs, stack bypassed)");
        }
        Err(e) => {
            eprintln!("E2E failed: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(not(all(windows, feature = "wire-locale")))]
fn main() {
    eprintln!("wire_xdp_win_e2e needs Windows + --features wire-locale.");
}
