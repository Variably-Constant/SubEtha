//! Windows USO loopback E2E for the Sens-O-Matic bridge. Runs the real
//! `SensOMaticSender` / `SensOMaticReceiver` over `127.0.0.1`, ships a
//! stream of MTU-sized items through the FEC block path (so each block's
//! `k + r` same-size shards hit `send_batch`), verifies every item arrives
//! byte-exact, reports goodput, and prints whether `WSASendMsg` with
//! `UDP_SEND_MSG_SIZE` actually segmented in-stack (offload) or the kernel
//! rejected it and the sender fell back to per-datagram sends.
//!
//! On Linux the same path is GSO; this example is the Windows counterpart
//! and the `uso_stats()` line is `(0, 0)` off Windows.

use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::udp_bridge::{gro_stats, uso_stats, SensOMaticReceiver, SensOMaticSender};

/// Deterministic, index-stamped payloads so the receiver can check each
/// item against the exact bytes the sender staged.
fn gen_items(n: usize, item_len: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut v = vec![0u8; item_len];
        let seed = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for (j, b) in v.iter_mut().enumerate() {
            *b = (seed.wrapping_add(j as u64).wrapping_mul(2_654_435_761) >> 24) as u8;
        }
        v[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        out.push(v);
    }
    out
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n = 200_000usize;
    let item_len = 1408usize;
    let items = gen_items(n, item_len);

    let raddr: SocketAddr = "127.0.0.1:24770".parse().unwrap();
    let expected = items.clone();
    let total = items.len();
    let rx = thread::spawn(move || -> (bool, u64) {
        let mut recv = SensOMaticReceiver::bind(raddr).expect("bind recv");
        let (mut got, mut ok) = (0usize, true);
        let dbg = std::env::var("SUBETHA_DBG").is_ok();
        let start = Instant::now();
        let mut last = Instant::now();
        while got < total {
            if start.elapsed() > Duration::from_secs(180) {
                return (false, recv.recv_count());
            }
            for item in recv.poll().unwrap_or_default() {
                if item != expected[got] {
                    ok = false;
                }
                got += 1;
            }
            if dbg && last.elapsed() > Duration::from_millis(500) {
                eprintln!("[recv] got={got}/{total} recv_datagrams={}", recv.recv_count());
                last = Instant::now();
            }
        }
        if dbg {
            eprintln!("[recv] DONE got={got}/{total} recv_datagrams={}", recv.recv_count());
        }
        // Keep answering ARQ/feedback briefly so the sender's drain settles.
        for _ in 0..100 {
            recv.nudge_feedback().ok();
            thread::sleep(Duration::from_millis(2));
        }
        (ok && got == total, recv.recv_count())
    });

    let dbg = std::env::var("SUBETHA_DBG").is_ok();
    thread::sleep(Duration::from_millis(250));
    let mut send = SensOMaticSender::bind("127.0.0.1:0", raddr, 8, 2, item_len)?;
    let t0 = Instant::now();
    for (idx, it) in items.iter().enumerate() {
        let mut waited = 0u64;
        while send.flow_blocked() {
            send.pump_feedback().ok();
            if send.flow_blocked() {
                thread::sleep(Duration::from_micros(50));
                waited += 1;
                if dbg && waited.is_multiple_of(20_000) {
                    eprintln!("[send] flow_blocked at item {idx}, pending={}", send.pending_len());
                }
            }
        }
        send.send_item(it)?;
        if dbg && idx.is_multiple_of(50_000) {
            eprintln!("[send] sent item {idx}");
        }
    }
    if dbg {
        eprintln!("[send] all {n} items sent, flushing");
    }
    send.flush()?;
    if dbg {
        eprintln!("[send] flushed, draining (pending={})", send.pending_len());
    }
    let acked = send.drain_until_acked(Duration::from_secs(180))?;
    if dbg {
        eprintln!("[send] drain returned acked={acked} pending={}", send.pending_len());
    }
    let secs = t0.elapsed().as_secs_f64();
    let (ok, rc) = rx.join().expect("rx join");

    let mbit = (n * item_len) as f64 * 8.0 / secs / 1e6;
    let (offload, fallback) = uso_stats();
    let (gro_calls, gro_segs) = gro_stats();
    println!("Segmentation-offload loopback E2E: {n} items x {item_len} B over the real FEC bridge");
    println!("  goodput      : {mbit:7.0} Mbit/s   secs={secs:.2}");
    println!("  byte-exact   : {ok}   fully_acked={acked}   recv_datagrams={rc}");
    // Send side: USO is Windows-only (WSASendMsg + UDP_SEND_MSG_SIZE); GSO is
    // Linux-only (UDP_SEGMENT, no separate counter); FreeBSD batches with
    // sendmmsg, which is syscall batching, not kernel segmentation - so its
    // counters stay zero and it prints the sendmmsg line below instead.
    if offload + fallback > 0 {
        let pct = 100.0 * offload as f64 / (offload + fallback) as f64;
        println!("  USO offload  : {offload} batches   fallback: {fallback}   ({pct:.1}% offloaded)");
    } else if cfg!(target_os = "freebsd") {
        println!("  send path    : sendmmsg batching (FreeBSD has no UDP segmentation offload)");
    }
    // Receive side: GRO coalescing (Linux). segs >> calls means it engaged.
    if gro_calls > 0 {
        let fan = gro_segs as f64 / gro_calls as f64;
        println!("  GRO recv     : {gro_calls} recvmsg calls   {gro_segs} segments   {fan:.1} datagrams/call");
    }
    if !ok || !acked {
        return Err("loopback E2E failed (not byte-exact or not acked)".into());
    }
    println!("E2E PASS: every item byte-exact over the segmentation-offload path");
    Ok(())
}
