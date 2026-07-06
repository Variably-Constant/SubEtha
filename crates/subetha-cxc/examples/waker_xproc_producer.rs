//! Producer side of the cross-process `BlockingSpscRing` end-to-end demo.
//!
//! Run this AFTER the consumer side has opened the file-backed
//! ring (the consumer's "ready" marker file appears, see below).
//! The two binaries communicate via a file-backed BlockingSpscRing
//! whose backing files sit in `%TEMP%` (Windows) / `/tmp` (Unix)
//! with a caller-supplied unique base name.
//!
//! Usage:
//!     waker_xproc_producer <base_path> <n_items>
//!
//! Example:
//!     waker_xproc_producer /tmp/subetha_xproc_demo 50000
//!
//! Coordination:
//!   1. Run waker_xproc_consumer FIRST. It creates the
//!      BlockingSpscRing's three files (ring + 2 wakers) and a
//!      ".consumer_ready" marker.
//!   2. Run THIS binary. It waits for the marker, opens the ring,
//!      pushes <n_items>, then drops a ".producer_done" marker.
//!   3. The consumer recv_blocking-loops to <n_items> and exits.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use subetha_cxc::BlockingSpscRing;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: {} <base_path> <n_items>", args[0]);
        std::process::exit(2);
    }
    let base = PathBuf::from(&args[1]);
    let n_items: u64 = args[2].parse().expect("n_items u64");

    let consumer_ready = base.with_extension("consumer_ready");
    let producer_done = base.with_extension("producer_done");

    // Wait for the consumer to signal ready by creating its
    // marker file. Bounded wait so the producer does not hang
    // forever if the consumer never starts.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !consumer_ready.exists() {
        if Instant::now() > deadline {
            eprintln!("waker_xproc_producer: consumer_ready marker did not appear within 10s; bailing");
            std::process::exit(3);
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    println!("[producer] consumer ready; opening ring at {}", base.display());
    let ring = BlockingSpscRing::open(&base, 64).expect("producer open ring");

    let t0 = Instant::now();
    for i in 0..n_items {
        let mut payload = [0u8; 56];
        payload[..8].copy_from_slice(&i.to_le_bytes());
        ring.send_blocking(&payload, Some(Duration::from_secs(10)))
            .expect("producer send");
    }
    let elapsed = t0.elapsed();

    // Drop the done marker so the consumer (or a sweep script)
    // knows the producer finished cleanly.
    std::fs::write(&producer_done, format!("{n_items}").as_bytes())
        .expect("write producer_done marker");

    println!(
        "[producer] shipped {n_items} items in {elapsed:?} ({:.2} M items/s)",
        n_items as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
}
