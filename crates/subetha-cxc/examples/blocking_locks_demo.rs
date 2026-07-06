//! Intra-process worked-example demo of [`BlockingSemaphore`] +
//! [`BlockingRWLock`].
//!
//! For each primitive: spawns contending threads that hold +
//! release the lock under load, times the actual park->wake
//! latency observed by parkers, and asserts that at least one
//! park happened (so the kernel-park slow path was actually
//! exercised and the demo is not entirely on the fast CAS path).
//!
//! Run:
//!     cargo run --release --example blocking_locks_demo

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::{BlockingRWLock, BlockingSemaphore};

const SEM_HOLD: Duration = Duration::from_micros(800);
const RWLOCK_HOLD: Duration = Duration::from_micros(1200);

fn fresh_base(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.join(format!("subetha_blocking_{tag}_{}_{}", std::process::id(), stamp))
}

fn cleanup(base: &std::path::Path, suffixes: &[&str]) {
    for s in suffixes {
        let mut p = base.as_os_str().to_owned();
        p.push(*s);
        drop(std::fs::remove_file(PathBuf::from(p)));
    }
}

fn main() {
    println!("=== Blocking semaphore + rw_lock E2E demo ===");
    println!();

    demo_semaphore();
    println!();
    demo_rw_lock();
    println!();
    println!("=== Both blocking primitives verified ===");
}

fn demo_semaphore() {
    println!("--- BlockingSemaphore (cap=2, contenders=8) ---");
    let base = fresh_base("sem");
    let sem_suffixes = [
        ".count.bin", ".wakeup.bin", ".waiters.bin",
        ".count.bin.hh.bin", ".count.bin.ring.bin",
        ".wakeup.bin.hh.bin", ".wakeup.bin.ring.bin",
        ".waiters.bin.hh.bin", ".waiters.bin.ring.bin",
        ".hh.bin", ".ring.bin",
        ".waker.bin",
    ];
    cleanup(&base, &sem_suffixes);

    let sem = Arc::new(
        BlockingSemaphore::create(&base, 2, 2).expect("create sem"),
    );
    let park_count = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let s = Arc::clone(&sem);
            let pc = Arc::clone(&park_count);
            thread::spawn(move || {
                for _ in 0..20 {
                    let t_enter = Instant::now();
                    let _p = s.acquire_park().expect("park acquire");
                    if t_enter.elapsed() >= Duration::from_micros(50) {
                        pc.fetch_add(1, Ordering::Relaxed);
                    }
                    thread::sleep(SEM_HOLD);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed();
    let parks = park_count.load(Ordering::Relaxed);

    println!("  elapsed:        {elapsed:?}");
    println!("  parks observed: {parks} (slow-path acquires)");
    assert!(parks > 0, "no semaphore parks observed; primitive ran entirely on the fast CAS path");
    println!("  PASS");

    cleanup(&base, &sem_suffixes);
}

fn demo_rw_lock() {
    println!("--- BlockingRWLock (1 writer, 6 readers) ---");
    let base = fresh_base("rwl");
    let rwl_suffixes = [".rwlock.bin", ".waker.bin", ".wakeup.bin"];
    cleanup(&base, &rwl_suffixes);

    let lock = Arc::new(BlockingRWLock::create(&base).expect("create lock"));
    let park_count = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    // Writer holds the lock periodically so readers have to park.
    let writer_lock = Arc::clone(&lock);
    let writer_parks = Arc::clone(&park_count);
    let writer = thread::spawn(move || {
        for _ in 0..10 {
            let t_enter = Instant::now();
            let _w = writer_lock.write_park().expect("write park");
            if t_enter.elapsed() >= Duration::from_micros(50) {
                writer_parks.fetch_add(1, Ordering::Relaxed);
            }
            thread::sleep(RWLOCK_HOLD);
        }
    });

    let reader_handles: Vec<_> = (0..6)
        .map(|_| {
            let l = Arc::clone(&lock);
            let pc = Arc::clone(&park_count);
            thread::spawn(move || {
                for _ in 0..30 {
                    let t_enter = Instant::now();
                    let _r = l.read_park().expect("read park");
                    if t_enter.elapsed() >= Duration::from_micros(50) {
                        pc.fetch_add(1, Ordering::Relaxed);
                    }
                    thread::sleep(Duration::from_micros(50));
                }
            })
        })
        .collect();

    writer.join().unwrap();
    for h in reader_handles {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed();
    let parks = park_count.load(Ordering::Relaxed);

    println!("  elapsed:        {elapsed:?}");
    println!("  parks observed: {parks} (slow-path acquires)");
    assert!(parks > 0, "no rw_lock parks observed; primitive ran entirely on the fast CAS path");
    println!("  PASS");

    cleanup(&base, &rwl_suffixes);
}
