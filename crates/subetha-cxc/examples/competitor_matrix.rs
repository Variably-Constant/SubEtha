//! In-process throughput matrix: every SubEtha ring shape against the Rust
//! channel field, across three producer/consumer scenarios.
//!
//! The `spsc_shootout` covers only the 1P/1C slice. This example runs all
//! four AdaptiveRing shapes at their natural concurrency and pits each against
//! the channel competitors that fit that shape:
//!
//!   - 1P/1C  : Spsc (Lamport pair) + Vyukov  vs  rtrb, crossbeam, flume, std::mpsc
//!   - NP/1C  : Mpsc (composed)               vs  crossbeam, flume, std::mpsc
//!   - NP/NC  : Mpmc (composed grid) + Vyukov vs  crossbeam, flume
//!
//! SubEtha is driven through the real `AdaptiveRing` pinned to each shape
//! (register_producer / register_consumer + try_send / try_recv), so the
//! number is what a user actually pays, not a hand-rolled inner loop. Every
//! contender ships the same total item count, the same 16-byte payload, the
//! same busy-spin on full/empty, the same P producer + C consumer threads.
//!
//! Median-of-RUNS per contender (robust to scheduler noise; not best-of), and
//! the result is written to `docs/competitor_matrix_results.json` for the chart.
//!
//! Run on a quiet box (release):
//!     cargo run --release --example competitor_matrix -p subetha-cxc

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use serde::Serialize;
use subetha_cxc::adaptive_ring::{AdaptiveRing, RingShape};
use subetha_cxc::SharedRingSpsc;

const N: u64 = 1_000_000; // total items transferred per run
const CAP: usize = 4096;
const RUNS: usize = 11; // median-of-11
const NP: usize = 4; // "many" producers / consumers

#[derive(Serialize, Clone)]
struct Row {
    name: String,
    is_subetha: bool,
    ns_per_item: f64,
    m_items_per_s: f64,
}

#[derive(Serialize)]
struct Scenario {
    name: String,
    producers: usize,
    consumers: usize,
    rows: Vec<Row>,
}

#[derive(Serialize)]
struct Output {
    machine: String,
    payload_bytes: usize,
    total_items: u64,
    runs_per_contender: usize,
    scenarios: Vec<Scenario>,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Run `f` (returns ns/item) `RUNS` times and take the median.
fn run_median(f: impl Fn() -> f64) -> f64 {
    median((0..RUNS).map(|_| f()).collect())
}

fn row(name: &str, is_subetha: bool, ns_per_item: f64) -> Row {
    Row {
        name: name.to_string(),
        is_subetha,
        ns_per_item,
        m_items_per_s: 1e9 / ns_per_item / 1e6,
    }
}

// --------------------------------------------------------------------------
// SubEtha: the real AdaptiveRing pinned to one shape.
// --------------------------------------------------------------------------

fn bench_subetha(shape: RingShape, producers: usize, consumers: usize) -> f64 {
    let ring = Arc::new(
        AdaptiveRing::create_anon(producers.max(consumers), consumers, CAP).unwrap(),
    );
    ring.morph_to(shape).unwrap();
    let consumed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for _ in 0..consumers {
        let ring = ring.clone();
        let consumed = consumed.clone();
        handles.push(thread::spawn(move || {
            let cid = ring.register_consumer().unwrap();
            let mut out = [0u8; 64];
            loop {
                if ring.try_recv(cid, &mut out).is_ok() {
                    consumed.fetch_add(1, Ordering::Relaxed);
                } else if consumed.load(Ordering::Relaxed) >= N {
                    break;
                } else {
                    std::hint::spin_loop();
                }
            }
        }));
    }

    let t0 = Instant::now();
    let per = N / producers as u64;
    for p in 0..producers {
        let ring = ring.clone();
        let count = if p == producers - 1 {
            N - per * (producers as u64 - 1)
        } else {
            per
        };
        handles.push(thread::spawn(move || {
            let pid = ring.register_producer().unwrap();
            let payload = [0xABu8; 16];
            for _ in 0..count {
                while ring.try_send(pid, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        }));
    }

    while consumed.load(Ordering::Relaxed) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    for h in handles {
        h.join().ok();
    }
    elapsed.as_nanos() as f64 / N as f64
}

/// The typed Lamport SPSC pair (1P/1C only).
fn bench_lamport() -> f64 {
    let (producer, consumer) = SharedRingSpsc::create_anon_pair(CAP).unwrap();
    let consumed = Arc::new(AtomicU64::new(0));
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || {
        let mut out = [0u8; 64];
        loop {
            if consumer.try_pop(&mut out).is_ok() {
                consumed_c.fetch_add(1, Ordering::Relaxed);
            } else if consumed_c.load(Ordering::Relaxed) >= N {
                break;
            } else {
                std::hint::spin_loop();
            }
        }
    });
    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while producer.try_push(&payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Relaxed) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    consumer_thread.join().ok();
    elapsed.as_nanos() as f64 / N as f64
}

// --------------------------------------------------------------------------
// Competitors. Same total N, 16-byte payload, P producers + C consumers,
// busy-spin on full/empty.
// --------------------------------------------------------------------------

fn bench_crossbeam(producers: usize, consumers: usize) -> f64 {
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 16]>(CAP);
    drive_channel(producers, consumers, move || rx.clone(), move || tx.clone(),
        |rx| rx.try_recv().is_ok(), |tx, p| tx.try_send(p).is_err())
}

fn bench_flume(producers: usize, consumers: usize) -> f64 {
    let (tx, rx) = flume::bounded::<[u8; 16]>(CAP);
    drive_channel(producers, consumers, move || rx.clone(), move || tx.clone(),
        |rx| rx.try_recv().is_ok(), |tx, p| tx.try_send(p).is_err())
}

/// Shared driver for clone-able-sender/receiver channels (crossbeam, flume).
fn drive_channel<S: Send + 'static, R: Send + 'static>(
    producers: usize,
    consumers: usize,
    make_rx: impl Fn() -> R + Send + Sync + 'static + Clone,
    make_tx: impl Fn() -> S + Send + Sync + 'static + Clone,
    try_recv: impl Fn(&R) -> bool + Send + Sync + Copy + 'static,
    try_send_full: impl Fn(&S, [u8; 16]) -> bool + Send + Sync + Copy + 'static,
) -> f64 {
    let consumed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..consumers {
        let rx = make_rx();
        let consumed = consumed.clone();
        handles.push(thread::spawn(move || loop {
            if try_recv(&rx) {
                consumed.fetch_add(1, Ordering::Relaxed);
            } else if consumed.load(Ordering::Relaxed) >= N {
                break;
            } else {
                std::hint::spin_loop();
            }
        }));
    }
    let t0 = Instant::now();
    let per = N / producers as u64;
    for p in 0..producers {
        let tx = make_tx();
        let count = if p == producers - 1 {
            N - per * (producers as u64 - 1)
        } else {
            per
        };
        handles.push(thread::spawn(move || {
            let payload = [0xABu8; 16];
            for _ in 0..count {
                while try_send_full(&tx, payload) {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    while consumed.load(Ordering::Relaxed) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    for h in handles {
        h.join().ok();
    }
    elapsed.as_nanos() as f64 / N as f64
}

/// std::sync::mpsc::sync_channel - MPSC only (single consumer).
fn bench_std_mpsc(producers: usize) -> f64 {
    let (tx, rx) = std::sync::mpsc::sync_channel::<[u8; 16]>(CAP);
    let consumed = Arc::new(AtomicU64::new(0));
    let consumed_c = consumed.clone();
    let consumer = thread::spawn(move || loop {
        if rx.try_recv().is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        } else if consumed_c.load(Ordering::Relaxed) >= N {
            break;
        } else {
            std::hint::spin_loop();
        }
    });
    let t0 = Instant::now();
    let per = N / producers as u64;
    let mut producer_handles = Vec::new();
    for p in 0..producers {
        let tx = tx.clone();
        let count = if p == producers - 1 {
            N - per * (producers as u64 - 1)
        } else {
            per
        };
        producer_handles.push(thread::spawn(move || {
            let payload = [0xABu8; 16];
            for _ in 0..count {
                while tx.try_send(payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    while consumed.load(Ordering::Relaxed) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    drop(tx);
    for h in producer_handles {
        h.join().ok();
    }
    consumer.join().ok();
    elapsed.as_nanos() as f64 / N as f64
}

/// rtrb - SPSC only.
fn bench_rtrb() -> f64 {
    let (mut producer, mut consumer) = rtrb::RingBuffer::<[u8; 16]>::new(CAP);
    let consumed = Arc::new(AtomicU64::new(0));
    let consumed_c = consumed.clone();
    let consumer_thread = thread::spawn(move || loop {
        if consumer.pop().is_ok() {
            consumed_c.fetch_add(1, Ordering::Relaxed);
        } else if consumed_c.load(Ordering::Relaxed) >= N {
            break;
        } else {
            std::hint::spin_loop();
        }
    });
    let t0 = Instant::now();
    let payload = [0xABu8; 16];
    for _ in 0..N {
        while producer.push(payload).is_err() {
            std::hint::spin_loop();
        }
    }
    while consumed.load(Ordering::Relaxed) < N {
        std::hint::spin_loop();
    }
    let elapsed = t0.elapsed();
    consumer_thread.join().ok();
    elapsed.as_nanos() as f64 / N as f64
}

fn main() {
    let machine = std::env::var("SUBETHA_BENCH_MACHINE")
        .unwrap_or_else(|_| "unknown host".to_string());
    eprintln!(
        "competitor_matrix: {N} items, 16-byte payload, capacity {CAP}, median-of-{RUNS}"
    );

    let mut scenarios = Vec::new();

    // -- 1P/1C: SPSC rings + the general channels --
    eprintln!("[1/3] 1P/1C ...");
    let mut rows = vec![
        row("SubEtha Spsc (Lamport pair)", true, run_median(bench_lamport)),
        row("SubEtha Spsc (AdaptiveRing)", true, run_median(|| bench_subetha(RingShape::Spsc, 1, 1))),
        row("SubEtha Vyukov", true, run_median(|| bench_subetha(RingShape::Vyukov, 1, 1))),
        row("rtrb (SPSC)", false, run_median(bench_rtrb)),
        row("crossbeam_channel", false, run_median(|| bench_crossbeam(1, 1))),
        row("flume", false, run_median(|| bench_flume(1, 1))),
        row("std::sync::mpsc", false, run_median(|| bench_std_mpsc(1))),
    ];
    rows.sort_by(|a, b| a.ns_per_item.partial_cmp(&b.ns_per_item).unwrap());
    scenarios.push(Scenario { name: "1 producer / 1 consumer".into(), producers: 1, consumers: 1, rows });

    // -- NP/1C: composed MPSC + the MPSC channels --
    eprintln!("[2/3] {NP}P/1C ...");
    let mut rows = vec![
        row("SubEtha Mpsc (composed)", true, run_median(|| bench_subetha(RingShape::Mpsc, NP, 1))),
        row("crossbeam_channel", false, run_median(|| bench_crossbeam(NP, 1))),
        row("flume", false, run_median(|| bench_flume(NP, 1))),
        row("std::sync::mpsc", false, run_median(|| bench_std_mpsc(NP))),
    ];
    rows.sort_by(|a, b| a.ns_per_item.partial_cmp(&b.ns_per_item).unwrap());
    scenarios.push(Scenario { name: format!("{NP} producers / 1 consumer"), producers: NP, consumers: 1, rows });

    // -- NP/NC: composed MPMC grid + Vyukov + the MPMC channels --
    eprintln!("[3/3] {NP}P/{NP}C ...");
    let mut rows = vec![
        row("SubEtha Mpmc (composed grid)", true, run_median(|| bench_subetha(RingShape::Mpmc, NP, NP))),
        row("SubEtha Vyukov", true, run_median(|| bench_subetha(RingShape::Vyukov, NP, NP))),
        row("crossbeam_channel", false, run_median(|| bench_crossbeam(NP, NP))),
        row("flume", false, run_median(|| bench_flume(NP, NP))),
    ];
    rows.sort_by(|a, b| a.ns_per_item.partial_cmp(&b.ns_per_item).unwrap());
    scenarios.push(Scenario { name: format!("{NP} producers / {NP} consumers"), producers: NP, consumers: NP, rows });

    // Print + emit JSON.
    for s in &scenarios {
        println!("\n=== {} (lower ns/item is better) ===", s.name);
        for r in &s.rows {
            let tag = if r.is_subetha { "*" } else { " " };
            println!("  {tag} {:<32} {:>8.1} ns/item  ({:>6.2} M items/s)",
                r.name, r.ns_per_item, r.m_items_per_s);
        }
    }

    let out = Output {
        machine,
        payload_bytes: 16,
        total_items: N,
        runs_per_contender: RUNS,
        scenarios,
    };
    let json = serde_json::to_string_pretty(&out).unwrap();
    // Write to repo docs/ (same convention as cross_process_compare) so the
    // render script finds it regardless of the cwd the bench was launched from.
    let docs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap().parent().unwrap()
        .join("docs");
    std::fs::create_dir_all(&docs_dir).ok();
    let json_path = docs_dir.join("competitor_matrix_results.json");
    std::fs::write(&json_path, &json).unwrap();
    eprintln!("\nwrote {}", json_path.display());
}
