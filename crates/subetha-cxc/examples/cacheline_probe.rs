//! Operation-level A/B for the wired cache-line primitives:
//!
//! 1. Contended CAS ping-pong on one shared line: with vs without
//!    PREFETCHW ahead of the RMW.
//! 2. CLDEMOTE liveness + capability report on this host.
//! 3. MMF create/open/first-pass walk with warm-up on vs off
//!    (driven by re-running with SUBETHA_NO_MMF_WARM=1).
//!
//! Fairness notes (audited): the CAS contenders run identical
//! protocols differing only in the prefetch hint; warm on/off runs
//! build identical rings from identical files.
//!
//! Run: cargo run --release --example cacheline_probe

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use subetha_cxc::{cldemote, has_cldemote, prefetchw};

const CAS_ROUNDS: u64 = 200_000;

fn main() {
    println!("=== cache-line primitive probe ===");
    println!("  cldemote:      {}", has_cldemote());
    println!(
        "  mmf warm:      {}",
        if std::env::var_os("SUBETHA_NO_MMF_WARM").is_some_and(|v| v == "1") {
            "OFF (baseline run)"
        } else {
            "on"
        }
    );
    println!();

    bench_cas_prefetchw();
    bench_mmf_warm();
}

fn bench_cas_prefetchw() {
    for use_pw in [false, true] {
        let shared = Arc::new(AtomicU64::new(0));
        let s2 = Arc::clone(&shared);
        let h = std::thread::spawn(move || {
            // Odd turns belong to this thread.
            let mut turn = 1u64;
            while turn < CAS_ROUNDS * 2 {
                if use_pw {
                    prefetchw(s2.as_ptr() as *const u8);
                }
                if s2
                    .compare_exchange_weak(
                        turn, turn + 1, Ordering::AcqRel, Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    turn += 2;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        let t0 = Instant::now();
        let mut turn = 0u64;
        while turn < CAS_ROUNDS * 2 {
            if use_pw {
                prefetchw(shared.as_ptr() as *const u8);
            }
            if shared
                .compare_exchange_weak(
                    turn, turn + 1, Ordering::AcqRel, Ordering::Relaxed,
                )
                .is_ok()
            {
                turn += 2;
            } else {
                std::hint::spin_loop();
            }
        }
        let elapsed = t0.elapsed();
        h.join().expect("cas peer");
        println!(
            "  CAS ping-pong {}: {:.0} ns/round-trip",
            if use_pw { "with prefetchw   " } else { "without prefetchw" },
            elapsed.as_nanos() as f64 / CAS_ROUNDS as f64,
        );
    }
    // cldemote has no same-host A/B on silicon where it is a NOP;
    // exercise it for liveness and report capability honestly.
    let line = [0u8; 64];
    cldemote(line.as_ptr());
    println!(
        "  cldemote: executed (hardware support: {})",
        has_cldemote()
    );
}

fn bench_mmf_warm() {
    // The warm-up wiring targets the OPEN path (create's init pass
    // touches every page itself). Shape: creator fills a 32 MiB ring
    // and drops its mapping; the OPENER - a fresh mapping with empty
    // page tables - drains it, which is the attach-side first-traffic
    // pass the bridges pay. Warm on/off comes from re-running with
    // SUBETHA_NO_MMF_WARM=1.
    let dir = std::env::temp_dir().join("subetha_cacheline_probe");
    drop(std::fs::create_dir_all(&dir)); // @hook-allow:no-let-underscore
    let path = dir.join("warm_probe.ring");
    drop(std::fs::remove_file(&path));

    let capacity = 512 * 1024; // 512k slots x 64B = 32 MiB
    let payload = [0x42u8; 56];
    {
        let ring = subetha_cxc::SharedRing::create(&path, capacity).expect("create");
        for _ in 0..capacity {
            ring.try_push(&payload).expect("push");
        }
    }

    let mut out = [0u8; 56];
    let t0 = Instant::now();
    let ring = subetha_cxc::SharedRing::open(&path, capacity).expect("open");
    let opened = t0.elapsed();
    let t0 = Instant::now();
    for _ in 0..capacity {
        ring.try_pop(&mut out).expect("pop");
    }
    let drain = t0.elapsed();

    println!(
        "  mmf 32MiB ring open-side: open {:.1} ms, first full drain {:.1} ms",
        opened.as_secs_f64() * 1e3,
        drain.as_secs_f64() * 1e3,
    );
    drop(ring);
    drop(std::fs::remove_file(&path));
}
