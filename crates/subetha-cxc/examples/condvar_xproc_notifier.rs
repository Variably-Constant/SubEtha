//! Notifier side of the cross-process [`SharedCondvar`] E2E.
//!
//! Coordination:
//!   1. Run `condvar_xproc_waiter` FIRST. It creates the condvar
//!      files + a shared predicate atom + drops a `.waiter_ready`
//!      marker.
//!   2. Run THIS binary. It waits for the marker, opens the
//!      condvar + predicate, sleeps briefly, sets the predicate
//!      to true, and calls `notify_all`. It then drops a
//!      `.notifier_done` marker.
//!   3. The waiter's `wait(predicate)` call returns and the waiter
//!      exits rc=0.
//!
//! On Linux/WSL the wake call crosses the process boundary via
//! SHARED `futex` and the test proves cross-process condvar
//! wake-without-per-message-syscalls. On Windows the primitive
//! falls back to spin (`WaitOnAddress` is intra-process only) so
//! THIS binary should be run from WSL Linux.
//!
//! Usage:
//!     condvar_xproc_notifier <base_path>
//!
//! Example:
//!     condvar_xproc_notifier /tmp/subetha_cv_xproc_demo

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

    // Wait for waiter's ready marker (bounded so the notifier does
    // not hang forever if the waiter never starts).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !waiter_ready.exists() {
        if Instant::now() > deadline {
            eprintln!("[notifier] waiter_ready never appeared; bailing");
            std::process::exit(3);
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    println!("[notifier] waiter ready; opening condvar at {}", base.display());
    let cv = SharedCondvar::open(&base).expect("notifier open condvar");
    let pred = SharedAtomicBool::open(&pred_path).expect("notifier open pred");

    // Brief pause so the waiter has time to enter `wait` (otherwise
    // notify_all races with the parker and may find 0 parked slots).
    std::thread::sleep(Duration::from_millis(50));

    println!("[notifier] setting predicate + notifying all");
    pred.store(true, Ordering::Release);
    let woken = cv.notify_all();

    // Drop the done marker so the harness knows the notifier
    // finished its half cleanly.
    std::fs::write(&notifier_done, format!("woken={woken}").as_bytes())
        .expect("write notifier_done marker");

    println!("[notifier] notify_all woke {woken} parked waiter(s)");
}
