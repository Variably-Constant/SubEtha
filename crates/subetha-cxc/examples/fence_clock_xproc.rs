//! Cross-process E2E for the shared cached wall clock.
//!
//! The shared `cached_us` field lives in the FenceClock MMF, so a value a
//! writer process publishes (by ticking) must be readable by a separate
//! reader process that maps the same file.
//!
//!     fence_clock_xproc writer <path>   # creates, ticks, prints WRITER_CACHED
//!     fence_clock_xproc reader <path>   # opens same file, prints READER_CACHED
//!
//! Run writer then reader against the same path: READER_CACHED equals the
//! writer's last published value (the reader does not tick, so the MMF
//! field is exactly what the writer left), proving cross-process sharing.

use std::hint::black_box;
use subetha_cxc::SharedFenceClock;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/tmp/fence_clock_xproc.bin".to_string());

    match mode.as_str() {
        "writer" => {
            let fc = SharedFenceClock::create(&path, 4).expect("create");
            let idx = fc.register(std::process::id()).expect("register");
            for _ in 0..200_000 {
                black_box(fc.tick(idx));
            }
            // Leave the file in place for the reader process.
            println!("WRITER_CACHED={}", fc.shared_clock_us());
        }
        "reader" => {
            let fc = SharedFenceClock::open(&path, 4).expect("open");
            // No tick: the value read is exactly what the writer published.
            println!("READER_CACHED={}", fc.shared_clock_us());
            std::fs::remove_file(&path).ok();
        }
        other => {
            eprintln!("usage: fence_clock_xproc <writer|reader> <path>  (got {other:?})");
            std::process::exit(2);
        }
    }
}
