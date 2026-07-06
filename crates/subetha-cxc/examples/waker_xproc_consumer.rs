//! Consumer side of the cross-process `BlockingSpscRing` end-to-end demo.
//!
//! Creates the file-backed BlockingSpscRing (ring + 2 wakers) at
//! the caller-supplied base path, drops a "consumer_ready" marker
//! so the producer can start, then recv_blocking-loops to
//! <n_items>. Reports observed consumer-park count - the test
//! is meaningful only if at least one park fired (otherwise the
//! producer was so fast that try_pop always returned items without
//! going through the wake path, and the cross-process wake
//! mechanism is not actually exercised).
//!
//! Usage:
//!     waker_xproc_consumer <base_path> <n_items>
//!
//! See `waker_xproc_producer.rs` for the producer side and the
//! intended coordination order.

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

    // Drop any leftover marker files so prior aborted runs do
    // not interfere with this one.
    let consumer_ready = base.with_extension("consumer_ready");
    let producer_done = base.with_extension("producer_done");
    drop(std::fs::remove_file(&consumer_ready));
    drop(std::fs::remove_file(&producer_done));

    println!("[consumer] creating ring at {}", base.display());
    let ring = BlockingSpscRing::create(&base, 64).expect("consumer create ring");

    // Mark consumer ready -> producer starts pushing.
    std::fs::write(&consumer_ready, b"go").expect("write consumer_ready marker");
    println!("[consumer] ready marker dropped; awaiting items");

    let t0 = Instant::now();
    let mut buf = [0u8; 64];
    let mut got: Vec<u64> = Vec::with_capacity(n_items as usize);
    let mut park_count: u64 = 0;

    for _ in 0..n_items {
        let t_enter = Instant::now();
        ring.recv_blocking(&mut buf, Some(Duration::from_secs(30)))
            .expect("consumer recv");
        // Heuristic: if recv_blocking took >= 50us it most likely
        // hit the kernel-block path (pre-park spin and try_pop
        // would have returned in <10us if items were already
        // present).
        if t_enter.elapsed() >= Duration::from_micros(50) {
            park_count += 1;
        }
        let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
        got.push(v);
    }
    let elapsed = t0.elapsed();

    // Integrity: every item id 0..n in send-order.
    let expected: Vec<u64> = (0..n_items).collect();
    if got != expected {
        eprintln!("[consumer] FIFO integrity FAILED");
        std::process::exit(4);
    }

    println!(
        "[consumer] drained {n_items} items in {elapsed:?} ({:.2} M items/s)",
        n_items as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
    println!(
        "[consumer] parks observed:    {park_count} ({:.1}% of recvs)",
        park_count as f64 / n_items as f64 * 100.0,
    );

    if park_count == 0 {
        eprintln!(
            "[consumer] WARNING: zero parks observed - the cross-process \
             wake path was not exercised. The producer was probably so \
             fast that try_pop always returned items immediately."
        );
        std::process::exit(5);
    }

    // Cleanup markers; leave the ring files for the next sweep
    // run to overwrite via create() (truncate mode).
    drop(std::fs::remove_file(&consumer_ready));
    drop(std::fs::remove_file(&producer_done));

    println!("[consumer] PASS - cross-process wake path exercised");
}
