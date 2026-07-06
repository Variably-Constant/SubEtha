//! Cross-process async: a `Future` in one process, woken by a push from
//! ANOTHER process. The consumer `.await`s items over a shared-memory
//! ring and genuinely sleeps (its driver thread and its reactor thread
//! both park in the kernel) until the producer process publishes and
//! signals - no busy-spin, no thread-per-future.
//!
//! This is the piece that makes the ring a first-class async source
//! across processes, not just across threads. Intra-process the
//! producer fires the consumer's `Waker` directly; here the producer is
//! a separate process, so a reactor thread in the consumer bridges the
//! MMF wake to the local `Waker` (see `crate::reactor`).
//!
//! The parent (producer) deliberately pauses a few times (20 ms each)
//! mid-stream. Because the consumer is parked, each resume is driven by
//! the parent's cross-process wake firing the reactor; the reactor's
//! wait is heal-bounded (a 50 ms backstop so a lost wake cannot hang
//! it), so a resume inside the 20 ms pause shows the wake itself fired,
//! not the backstop. Every item arrives in order across the boundary.
//!
//! Run:
//!     cargo run --release --example xproc_async_ring -p subetha-cxc

use std::sync::Arc;
use std::time::{Duration, Instant};

use subetha_cxc::cross_process_waker::{CrossProcessWaker, MAX_WAITERS_DEFAULT};
use subetha_cxc::reactor::{block_on, receiver_cross, sender_cross};
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

const CAPACITY: usize = 64;
const COUNT: u32 = 100_000;
const PAUSES: u32 = 5;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 5 && args[1] == "--consume" {
        consumer(&args[2], &args[3], args[4].parse().expect("count"));
        return;
    }
    producer();
}

fn paths() -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir();
    let pid = std::process::id();
    (
        dir.join(format!("subetha-xproc-async-ring-{pid}.bin")),
        dir.join(format!("subetha-xproc-async-waker-{pid}.bin")),
    )
}

fn producer() {
    use std::process::Command;

    let (ring_path, waker_path) = paths();
    std::fs::remove_file(&ring_path).ok();
    std::fs::remove_file(&waker_path).ok();

    // Lay out the shared ring + named waker before the child opens them.
    let ring = Arc::new(
        SpscRingCore::create(&ring_path, CAPACITY).expect("create ring"),
    );
    let xwaker = Arc::new(
        CrossProcessWaker::create(&waker_path, MAX_WAITERS_DEFAULT)
            .expect("create waker"),
    );
    let tx = sender_cross(Arc::clone(&ring), Arc::clone(&xwaker));

    println!("== cross-process async ring ==");
    println!("parent (producer) pid {}", std::process::id());
    println!("ring: {}", ring_path.display());
    println!("waker: {}", waker_path.display());
    println!("streaming {COUNT} items to a child that AWAITs them (parked, not spinning)\n");

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .arg("--consume")
        .arg(&ring_path)
        .arg(&waker_path)
        .arg(COUNT.to_string())
        .spawn()
        .expect("spawn consumer child");

    let t0 = Instant::now();
    let pause_every = COUNT / (PAUSES + 1);
    let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
    for seq in 0..COUNT {
        // Pause a few times so the drained consumer actually parks; the
        // resume relies on the cross-process wake firing its reactor.
        if seq > 0 && seq % pause_every == 0 {
            std::thread::sleep(Duration::from_millis(20));
        }
        buf[..4].copy_from_slice(&seq.to_le_bytes());
        while tx.try_send(&buf).is_err() {
            std::hint::spin_loop();
        }
    }

    let status = child.wait().expect("child wait");
    let elapsed = t0.elapsed();

    std::fs::remove_file(&ring_path).ok();
    std::fs::remove_file(&waker_path).ok();

    if status.success() {
        println!("child drained + verified all {COUNT} items in order, woken \
                  cross-process across {PAUSES} parent pauses, in {elapsed:?}");
        println!("{:.2} M items/s end to end, integrity OK",
                 COUNT as f64 / elapsed.as_secs_f64() / 1e6);
    } else {
        println!("consumer child failed: {status:?}");
        std::process::exit(1);
    }
}

fn consumer(ring_path: &str, waker_path: &str, count: u32) {
    // The parent created the files before spawning us, but retry the
    // open to be robust against any startup ordering.
    let ring = Arc::new(open_ring_retry(ring_path));
    let xwaker = Arc::new(open_waker_retry(waker_path));
    let rx = receiver_cross(Arc::clone(&ring), xwaker);

    // Drive with block_on: the thread parks between items and is
    // unparked by the reactor when the parent process publishes.
    let sum = block_on(async move {
        let mut s: u64 = 0;
        for expected in 0..count {
            let item = rx.recv().await;
            let seq = u32::from_le_bytes(item[..4].try_into().unwrap());
            assert_eq!(seq, expected, "cross-process FIFO order violated");
            s = s.wrapping_add(seq as u64);
        }
        s
    });

    let want: u64 = (0..count as u64).sum();
    assert_eq!(sum, want, "consumer checksum mismatch");
    // Exit 0 signals the parent the integrity check passed.
}

fn open_ring_retry(path: &str) -> SpscRingCore {
    loop {
        match SpscRingCore::open(path, CAPACITY) {
            Ok(r) => return r,
            Err(_) => std::hint::spin_loop(),
        }
    }
}

fn open_waker_retry(path: &str) -> CrossProcessWaker {
    loop {
        match CrossProcessWaker::open(path, MAX_WAITERS_DEFAULT) {
            Ok(w) => return w,
            Err(_) => std::hint::spin_loop(),
        }
    }
}
