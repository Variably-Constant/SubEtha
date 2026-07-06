//! Waiter side of the cross-process [`SharedCondvar`] E2E.
//!
//! Creates the file-backed condvar (waker + gen counter) and a
//! shared predicate atom, drops the `.waiter_ready` marker so the
//! notifier can start, then calls `cv.wait(|| predicate.load())`.
//!
//! Times its own park so the harness can verify the wait actually
//! crossed the kernel (elapsed >= 50ms is the proof; spin-only
//! fast paths return in <10us).
//!
//! Usage:
//!     condvar_xproc_waiter <base_path>
//!
//! See `condvar_xproc_notifier.rs` for the notifier side and the
//! coordination order.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use subetha_cxc::shared_atomic::SharedAtomicBool;
use subetha_cxc::SharedCondvar;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: {} <base_path>", args[0]);
        std::process::exit(2);
    }
    let base = PathBuf::from(&args[1]);
    let waiter_ready = base.with_extension("waiter_ready");
    let notifier_done = base.with_extension("notifier_done");
    let pred_path = {
        let mut p = base.as_os_str().to_owned();
        p.push(".pred.bin");
        PathBuf::from(p)
    };

    // Scrub any leftover artifacts from a prior aborted run.
    drop(std::fs::remove_file(&waiter_ready));
    drop(std::fs::remove_file(&notifier_done));
    for suffix in [".waker.bin", ".gen.bin", ".pred.bin"] {
        let mut p = base.as_os_str().to_owned();
        p.push(suffix);
        drop(std::fs::remove_file(PathBuf::from(p)));
    }

    println!("[waiter] creating condvar + predicate at {}", base.display());
    let cv = SharedCondvar::create(&base).expect("waiter create condvar");
    let pred = SharedAtomicBool::create(&pred_path, false).expect("waiter create pred");

    // Signal the notifier we are ready.
    std::fs::write(&waiter_ready, b"go").expect("write waiter_ready marker");
    println!("[waiter] ready marker dropped; entering wait");

    let t_enter = Instant::now();
    cv.wait(|| pred.load(Ordering::Acquire)).expect("wait");
    let elapsed = t_enter.elapsed();

    println!("[waiter] wait returned after {elapsed:?}");
    if elapsed < Duration::from_millis(20) {
        eprintln!(
            "[waiter] WARNING: wait returned in <20ms, meaning the \
             notifier set the predicate BEFORE this waiter actually \
             parked. The cross-process wake path was not exercised \
             - try a longer notifier delay."
        );
        std::process::exit(4);
    }

    if !pred.load(Ordering::Acquire) {
        eprintln!("[waiter] predicate FALSE after wake; condvar protocol broken");
        std::process::exit(5);
    }

    // Cleanup markers; leave the backing files for the next run to
    // overwrite via create().
    drop(std::fs::remove_file(&waiter_ready));
    drop(std::fs::remove_file(&notifier_done));

    println!("[waiter] PASS - cross-process condvar wake exercised");
}
