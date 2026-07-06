//! E2E memory proof for the lazy observation buffer.
//!
//! Creates N `ObservationRing`s unarmed, samples RSS, then arms half of
//! them and samples again. A raw (unarmed) ring must cost only its small
//! header; the ~96 KiB observation buffer must appear only on `arm()`.
//!
//!     cargo run --release --example obs_ring_memory -- <n> <arm_count>
//!
//! Linux-only (reads /proc/self/statm). Run on the bench VM.

use std::hint::black_box;
use subetha_core::ObservationRing;

/// Resident set size in KiB (Linux: statm field 2 = resident pages).
fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0);
    pages * 4 // 4 KiB pages
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);
    let arm_count: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(n / 2);

    let base = rss_kb();
    let rings: Vec<Box<ObservationRing>> =
        (0..n).map(|_| Box::new(ObservationRing::new())).collect();
    black_box(&rings);
    let after_create = rss_kb();

    for r in rings.iter().take(arm_count) {
        r.arm();
    }
    black_box(&rings);
    let after_arm = rss_kb();

    let create_delta = after_create.saturating_sub(base);
    let arm_delta = after_arm.saturating_sub(after_create);
    eprintln!("rings={n} armed={arm_count}");
    eprintln!(
        "RSS: base={} MiB  after_create=+{} MiB ({} KiB/ring)  after_arm=+{} MiB ({} KiB/armed-ring)",
        base / 1024,
        create_delta / 1024,
        create_delta / (n as u64).max(1),
        arm_delta / 1024,
        arm_delta / (arm_count as u64).max(1),
    );
    eprintln!(
        "interpretation: create cost ~0/ring (lazy), arm cost ~96 KiB/ring (buffer allocated on arm)"
    );
    black_box(&rings);
}
