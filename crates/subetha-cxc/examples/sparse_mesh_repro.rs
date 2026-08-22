//! Sparse-cadence mesh repro for the n1-goes-dark stall: one process holds
//! three independent `UnifiedSensSender`s (ForceRlc, separate sockets, one
//! peer each) and ships one small item per link per interval, the shape of a
//! replication hello loop. Three receivers count what actually arrives.
//!
//! The stall signature hunted here: every send returns Ok, the receivers
//! deliver the first item, then nothing more arrives on any of the process's
//! links while other processes' identical links flow.
//!
//! Run: cargo run --profile test-fast -p subetha-cxc --example sparse_mesh_repro -- [trials] [rounds] [interval_ms]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use subetha_cxc::sens_unified::{CodePolicy, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender};

const ITEM_BYTES: usize = 72;

fn cfg() -> UnifiedConfig {
    UnifiedConfig {
        policy: CodePolicy::ForceRlc,
        symbol_len: ITEM_BYTES + 8,
        k: 16,
        r: 16,
        rlc_flow_window: 4096,
        debug_loss: 0,
        seed: 42,
        rlc_step: 4,
        rlc_static: false,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let trials: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(5);
    let rounds: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);
    let interval = Duration::from_millis(args.next().and_then(|s| s.parse().ok()).unwrap_or(500));

    let mut failures = 0u32;
    for trial in 0..trials {
        let stop = Arc::new(AtomicBool::new(false));
        let mut receivers = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..3 {
            let r = UnifiedSensReceiver::bind("127.0.0.1:0", cfg()).expect("bind receiver");
            addrs.push(r.local_addr().expect("receiver addr"));
            receivers.push(r);
        }
        let mut counters = Vec::new();
        let mut rx_threads = Vec::new();
        for mut r in receivers {
            let stop = Arc::clone(&stop);
            let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
            counters.push(Arc::clone(&count));
            rx_threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let items = r.poll().unwrap_or_default();
                    count.fetch_add(items.len() as u64, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(5));
                }
            }));
        }

        let mut senders: Vec<UnifiedSensSender> = addrs
            .iter()
            .map(|a| UnifiedSensSender::connect("0.0.0.0:0", *a, cfg()).expect("connect sender"))
            .collect();

        let mut buf = vec![0u8; ITEM_BYTES];
        let started = Instant::now();
        for round in 0..rounds {
            for (li, s) in senders.iter_mut().enumerate() {
                buf[..8].copy_from_slice(&round.to_le_bytes());
                buf[8] = li as u8;
                s.send_item(&buf).expect("send_item errored");
            }
            std::thread::sleep(interval);
        }
        // Delivery tail: give in-flight repairs/ARQ a moment to land.
        std::thread::sleep(Duration::from_millis(1500));
        stop.store(true, Ordering::Relaxed);
        for t in rx_threads {
            t.join().expect("receiver thread");
        }

        let got: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
        let sent_recv: Vec<(u64, u64)> = senders.iter().map(|s| s.raw_sent_recv()).collect();
        let ok = got.iter().all(|&g| g == rounds);
        if !ok {
            failures += 1;
        }
        println!(
            "trial {trial}: {} delivered={got:?} of {rounds} per link, raw_sent_recv={sent_recv:?}, wall={:.1}s",
            if ok { "OK  " } else { "FAIL" },
            started.elapsed().as_secs_f64(),
        );
    }
    println!("{failures} of {trials} trials failed");
    std::process::exit(if failures > 0 { 1 } else { 0 });
}
