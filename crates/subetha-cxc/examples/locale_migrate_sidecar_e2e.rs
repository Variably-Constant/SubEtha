//! End-to-end demo of `LocaleAdaptiveRingSidecar`.
//!
//! Runs a producer/consumer workload while the application
//! periodically calls `sidecar.request_locale(target)` to flip
//! the desired locale. The sidecar honours the request only when
//! the hysteresis cooldown has elapsed - so rapid back-and-forth
//! requests collapse into a single migration per cooldown
//! window. The migration itself transfers in-flight items
//! between backings as part of the wrapper's `migrate_to` flow.
//!
//! The demo intentionally requests anon -> file -> shmfs -> file
//! -> anon with varying delays so a human can SEE both the
//! "request honoured" and "request suppressed by hysteresis"
//! cases in the trace.
//!
//! Run:
//!     cargo run --release --example locale_migrate_sidecar_e2e

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::locale_adaptive_ring::{
    DefaultLocalePolicy, Locale, LocaleAdaptiveRing, LocaleAdaptiveRingSidecar,
};

const CAPACITY: usize = 256;
const ITEMS_PER_PHASE: u64 = 2_000;

fn main() {
    println!("=== LocaleAdaptiveRingSidecar E2E ===");
    println!("Hysteresis: 250 ms (default DefaultLocalePolicy)");
    println!();

    let base = std::env::temp_dir()
        .join(format!("locale_migrate_sidecar_{}", std::process::id()));
    let ring = Arc::new(
        LocaleAdaptiveRing::create(&base, 1, 1, CAPACITY).expect("create"),
    );
    ring.register_producer().expect("register producer");
    ring.register_consumer().expect("register consumer");

    let sidecar = LocaleAdaptiveRingSidecar::spawn(
        Arc::clone(&ring),
        DefaultLocalePolicy::default(),
        Duration::from_millis(15),
    );

    // Locale-change observer: print every transition the sidecar
    // applies. The application's requests go through
    // `sidecar.request_locale`; the OBSERVED `current_locale`
    // changes only when the policy actually migrates.
    let stop_obs = Arc::new(AtomicBool::new(false));
    let stop_obs_c = Arc::clone(&stop_obs);
    let ring_obs = Arc::clone(&ring);
    let t0 = Instant::now();
    let obs_h = thread::spawn(move || {
        let mut last = ring_obs.current_locale();
        println!("[{:6.3}s] start locale = {last:?}", t0.elapsed().as_secs_f64());
        while !stop_obs_c.load(Ordering::Acquire) {
            let now = ring_obs.current_locale();
            if now != last {
                println!(
                    "[{:6.3}s] MIGRATE {last:?} -> {now:?}",
                    t0.elapsed().as_secs_f64()
                );
                last = now;
            }
            thread::sleep(Duration::from_millis(5));
        }
    });

    // Producer: pushes items across the entire demo. Per push
    // dispatches through the wrapper's current locale.
    let r_prod = Arc::clone(&ring);
    let phases = 5u64;
    let total = phases * ITEMS_PER_PHASE;
    let producer = thread::spawn(move || {
        for i in 0..total {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            while r_prod.try_send(0, &payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    // Consumer: drains items.
    let r_cons = Arc::clone(&ring);
    let drained = Arc::new(AtomicU64::new(0));
    let drained_c = Arc::clone(&drained);
    let consumer = thread::spawn(move || {
        let mut buf = [0u8; 64];
        let mut count: u64 = 0;
        while count < total {
            while r_cons.try_recv(0, &mut buf).is_err() {
                std::hint::spin_loop();
            }
            count += 1;
            drained_c.store(count, Ordering::Release);
        }
    });

    // Driver: request locale changes with timing that exercises
    // both "honoured" and "suppressed by hysteresis" paths.
    let driver_t0 = t0;
    println!("[{:6.3}s] request: File", driver_t0.elapsed().as_secs_f64());
    sidecar.request_locale(Locale::File);
    thread::sleep(Duration::from_millis(300)); // > 250 ms hysteresis - honoured

    println!("[{:6.3}s] request: ShmFs", driver_t0.elapsed().as_secs_f64());
    sidecar.request_locale(Locale::ShmFs);
    thread::sleep(Duration::from_millis(100)); // < 250 ms hysteresis - SUPPRESSED at first

    println!(
        "[{:6.3}s] request: File (within hysteresis - rapid flip)",
        driver_t0.elapsed().as_secs_f64()
    );
    sidecar.request_locale(Locale::File);
    thread::sleep(Duration::from_millis(50));

    println!(
        "[{:6.3}s] request: ShmFs (the suppressed-then-current request finally fires)",
        driver_t0.elapsed().as_secs_f64()
    );
    sidecar.request_locale(Locale::ShmFs);
    thread::sleep(Duration::from_millis(400));

    println!("[{:6.3}s] request: Anon", driver_t0.elapsed().as_secs_f64());
    sidecar.request_locale(Locale::Anon);
    thread::sleep(Duration::from_millis(400));

    producer.join().expect("producer thread");
    consumer.join().expect("consumer thread");

    // Give the sidecar one final scan tick.
    thread::sleep(Duration::from_millis(50));
    stop_obs.store(true, Ordering::Release);
    obs_h.join().expect("observer thread");

    let migrations = sidecar.migrations_triggered();
    sidecar.shutdown();

    println!();
    println!("=== Result ===");
    println!("  total items produced/consumed:    {total}");
    println!("  drained:                          {}", drained.load(Ordering::Acquire));
    println!("  final locale:                     {:?}", ring.current_locale());
    println!("  sidecar migrations triggered:     {migrations}");
    println!();

    assert_eq!(
        drained.load(Ordering::Acquire),
        total,
        "all items must be drained"
    );
    assert!(
        migrations >= 2,
        "sidecar must have triggered at least two migrations under the requested sequence (observed {migrations})"
    );
    println!("PASS - sidecar applied {migrations} migrations under hysteresis-gated policy");

    // Cleanup the file-backed artifacts (best-effort).
    for suffix in [
        ".tag.bin", ".gen.bin", ".file.spsc.bin", ".file.mpsc.0.bin",
        ".file.mpmc.0.bin", ".file.vyukov.bin",
    ] {
        let mut p = base.clone();
        let mut s = p.as_os_str().to_owned();
        s.push(suffix);
        p = std::path::PathBuf::from(s);
        drop(std::fs::remove_file(&p));
    }
}
