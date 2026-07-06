//! Cross-host test harness for the reliable-UDP FEC transport.
//!
//! Items are the `u64` sequence `0..count`. The receiver verifies exact
//! in-order, exactly-once delivery and prints a one-line PASS/FAIL plus
//! throughput, so a real run across two machines is observable end to
//! end.
//!
//! Run via `cargo run --release -p subetha-cxc --example udp_xhost --`:
//!
//! Receiver: `--role receiver --bind 0.0.0.0:9000 --count N \
//!            [--loss PCT --seed S]`
//! Sender:   `--role sender --bind 0.0.0.0:0 --peer IP:9000 \
//!            --count N [--k 8 --r 2 --interleave D --item-bytes 8]`
//!
//! `--loss` injects deterministic receiver-side drops to force FEC / ARQ
//! / interleave to engage on a clean link; omit it for real-link loss.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use subetha_cxc::control_table::ControlTable;
use subetha_cxc::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};

fn arg(args: &[String], key: &str) -> Option<String> {
    args.iter().position(|a| a == key).and_then(|i| args.get(i + 1)).cloned()
}

fn argp<T: std::str::FromStr>(args: &[String], key: &str, default: T) -> T {
    arg(args, key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = arg(&args, "--role").unwrap_or_default();
    let bind = arg(&args, "--bind").unwrap_or_else(|| "0.0.0.0:0".into());
    let count: u64 = argp(&args, "--count", 1000);
    let item_bytes: usize = argp(&args, "--item-bytes", 8).max(8);
    match role.as_str() {
        "sender" => run_sender(&args, &bind, count, item_bytes),
        "receiver" => run_receiver(&args, &bind, count),
        "probe" => run_probe(),
        "salvage" => run_salvage(&args),
        other => {
            eprintln!("usage: --role <sender|receiver|probe|salvage> ... (got {other:?})");
            std::process::exit(2);
        }
    }
}

/// Demonstrate intra-packet salvage on the running binary: encode
/// payloads, corrupt them as a captured bad-FCS frame would be (within
/// the parity budget), and recover them - packets the kernel would have
/// dropped entirely. Real bad-FCS capture is driver-gated; the injected
/// corruption exercises the identical salvage path.
fn run_salvage(args: &[String]) {
    use subetha_cxc::salvage::PacketSalvage;
    let n: u64 = argp(args, "--count", 2000);
    let (b, r, bl) = (8usize, 2usize, 16usize);
    let s = PacketSalvage::new(b, r, bl).expect("salvage code");
    let mut rng = 0xDEAD_BEEF_1234_5678u64;
    let mut next = || {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        rng >> 33
    };
    let (mut salvaged, mut beyond) = (0u64, 0u64);
    let stride = bl + 4;
    for i in 0..n {
        let payload: Vec<u8> = (0..b * bl).map(|j| ((i as usize * 31 + j * 7) & 0xFF) as u8).collect();
        let mut packet = s.encode(&payload).expect("encode");
        // Corrupt 1..=r blocks (within budget) most of the time; ~1 in 8
        // gets r+1 corrupt blocks (beyond budget) to show the honest
        // boundary.
        let n_corrupt = if next() % 8 == 0 { r + 1 } else { 1 + (next() % r as u64) as usize };
        for _ in 0..n_corrupt {
            let blk = (next() % (b + r) as u64) as usize;
            let pos = blk * stride + (next() % bl as u64) as usize;
            packet[pos] ^= 0xFF;
        }
        match s.decode(&packet) {
            Some(p) if p == payload => salvaged += 1,
            _ => beyond += 1,
        }
    }
    println!(
        "[salvage] recovered {salvaged}/{n} corrupt packets the kernel would have DROPPED \
         ({}%); {beyond} were corrupted beyond the {r}-block budget (fall back to inter-packet FEC/ARQ)",
        salvaged * 100 / n
    );
}

/// Print the platform link sensor's live reading - proves the per-OS
/// backend reads real hardware (Wi-Fi signal on Windows, NIC drop
/// counters on Linux). Samples a few times since the drop rate is a
/// delta between samples.
fn run_probe() {
    use subetha_cxc::link_sensor::platform_sensor;
    let mut s = platform_sensor(None);
    for i in 0..3 {
        let snap = s.sample();
        println!(
            "[probe {i}] backend={} signal_quality={:?} drop_rate={:?} link_stress={:.3}",
            s.backend(),
            snap.signal_quality,
            snap.drop_rate,
            snap.link_stress()
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn run_sender(args: &[String], bind: &str, count: u64, item_bytes: usize) {
    let peer: SocketAddr = arg(args, "--peer")
        .expect("--peer IP:port required for sender")
        .parse()
        .expect("--peer must be IP:port");
    let k: usize = argp(args, "--k", 8);
    let r: usize = argp(args, "--r", 2);
    let depth: u8 = argp(args, "--interleave", 1);

    let control = Arc::new(ControlTable::new());
    control.set_interleave_depth(depth);
    let mut s = ReliableUdpSender::bind_with_control(bind, peer, k, r, item_bytes, control)
        .expect("bind sender");
    let tower_r: usize = argp(args, "--tower-r", 0);
    if tower_r > 0 {
        s.enable_tower(8, tower_r);
    }
    println!(
        "[sender] {} -> {peer} k={k} r={r} interleave={depth} count={count} item_bytes={item_bytes}",
        s.local_addr().expect("local addr")
    );

    let t0 = Instant::now();
    let mut last_print = Instant::now();
    let mut buf = vec![0u8; item_bytes];
    for i in 0..count {
        buf[..8].copy_from_slice(&i.to_le_bytes());
        // Respect flow control: pause sending while the in-flight window
        // is full, pumping acks (non-blocking) so we resume the instant
        // window space frees - never block the full drain timeout here.
        while s.flow_blocked() {
            s.pump_feedback().ok();
            if s.flow_blocked() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        s.send_item(&buf).expect("send_item");
        if last_print.elapsed() > Duration::from_millis(400) {
            let c = s.control();
            eprintln!(
                "[sender] adapt: level={:?} parity_r={} interleave={} link={}({:.2})",
                c.level(),
                c.parity_r(),
                c.interleave_depth(),
                s.link_backend(),
                s.link_stress()
            );
            last_print = Instant::now();
        }
    }
    s.flush().expect("flush");
    let acked = s
        .drain_until_acked(Duration::from_secs(60))
        .expect("drain_until_acked");
    let secs = t0.elapsed().as_secs_f64();
    let goodput = (count as f64 * item_bytes as f64 * 8.0) / secs.max(1e-9) / 1e6;
    println!(
        "[sender] DONE sent={count} fully_acked={acked} pending={} secs={secs:.2} \
         rate={:.0} items/s goodput={goodput:.1} Mbit/s",
        s.pending_len(),
        count as f64 / secs.max(1e-9)
    );
}

fn run_receiver(args: &[String], bind: &str, count: u64) {
    let loss: u32 = argp(args, "--loss", 0);
    let seed: u64 = argp(args, "--seed", 1);
    let mut recv = ReliableUdpReceiver::bind(bind).expect("bind receiver");
    if loss > 0 {
        recv = recv.with_debug_loss(loss, seed);
    }
    let drop_mod: u32 = argp(args, "--drop-block-mod", 0);
    if drop_mod > 0 {
        recv = recv.with_block_drop_mod(drop_mod);
    }
    let max_hold_ms: u64 = argp(args, "--max-hold-ms", 0);
    if max_hold_ms > 0 {
        recv = recv.with_max_hold(Duration::from_millis(max_hold_ms));
    }
    // Simulate a real link's recovery round-trip on loopback by delaying
    // feedback, so selective vs serial NAK is measurable without the LAN.
    let rtt_ms: u64 = argp(args, "--rtt-ms", 0);
    if rtt_ms > 0 {
        recv = recv.with_feedback_delay(Duration::from_millis(rtt_ms));
    }
    // A/B: --serial-nak caps NAKs to the head block (one gap per
    // round-trip), reproducing the head-of-line recovery for comparison.
    if args.iter().any(|a| a == "--serial-nak") {
        recv = recv.with_nak_batch(1);
    }
    // Concentrated loss BURST [at, at+len) by datagram index, to show a
    // throughput blip and full recovery in the interval trace.
    let burst_at: u64 = argp(args, "--burst-at", 0);
    let burst_len: u64 = argp(args, "--burst-len", 0);
    if burst_len > 0 {
        recv = recv.with_burst_loss(burst_at, burst_len);
    }
    println!(
        "[receiver] {} count={count} injected_loss={loss}% seed={seed} drop_block_mod={drop_mod}",
        recv.local_addr().expect("local addr")
    );

    let t0 = Instant::now();
    let mut got: Vec<u64> = Vec::with_capacity(count as usize);
    let mut bytes: u128 = 0;
    let deadline = Duration::from_secs(180);
    let mut last_print = Instant::now();
    let mut last_bytes: u128 = 0;
    while (got.len() as u64) < count {
        if t0.elapsed() > deadline {
            eprintln!("[receiver] TIMEOUT got {} / {count}", got.len());
            break;
        }
        for item in recv.poll().expect("poll") {
            bytes += item.len() as u128;
            let idx = u64::from_le_bytes(item[..8].try_into().expect("8-byte index"));
            got.push(idx);
        }
        if last_print.elapsed() > Duration::from_millis(500) {
            // Interval goodput: delivered bytes since the last print over
            // the elapsed window, so a loss blip and its recovery are
            // visible over time rather than only in the final average.
            let dt = last_print.elapsed().as_secs_f64().max(1e-9);
            let interval_mbit = ((bytes - last_bytes) as f64 * 8.0) / dt / 1e6;
            eprintln!(
                "[receiver] t={:.1}s got={} interval={:.1} Mbit/s head={:?}",
                t0.elapsed().as_secs_f64(),
                got.len(),
                interval_mbit,
                recv.head_status()
            );
            last_bytes = bytes;
            last_print = Instant::now();
        }
    }
    // Grace: keep feeding feedback so the sender learns the final ack.
    for _ in 0..100 {
        recv.nudge_feedback().ok();
        std::thread::sleep(Duration::from_millis(2));
    }

    let secs = t0.elapsed().as_secs_f64();
    let ordered = got.iter().copied().eq(0..count);
    let sum: u128 = got.iter().map(|&x| x as u128).sum();
    let expected: u128 = (0..count as u128).sum();
    let pass = (got.len() as u64 == count) && ordered && sum == expected;
    let goodput = (bytes as f64 * 8.0) / secs.max(1e-9) / 1e6;
    println!(
        "[receiver] {} got={} expected={count} ordered={ordered} sum_ok={} secs={secs:.2} \
         rate={:.0} items/s goodput={goodput:.1} Mbit/s",
        if pass { "PASS" } else { "FAIL" },
        got.len(),
        sum == expected,
        count as f64 / secs.max(1e-9)
    );
    std::process::exit(if pass { 0 } else { 1 });
}
