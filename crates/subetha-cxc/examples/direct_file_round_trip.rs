//! E2E demonstration of `DirectFileRing` (unbuffered file I/O).
//!
//! Round-trips items through a non-mmap positioned-I/O ring with the
//! page cache bypassed (O_DIRECT on unix, FILE_FLAG_NO_BUFFERING on
//! windows), proving the path works end-to-end against a real on-disk
//! file. Verifies that the substrate's ring semantics hold even when the
//! underlying I/O is not mmap-mediated.
//!
//! Run:
//!     cargo run --release --example direct_file_round_trip

#[cfg(any(unix, windows))]
use std::time::Instant;

#[cfg(any(unix, windows))]
use subetha_cxc::protocol_direct_file::{
    DirectFileRing, DIRECT_FILE_SLOT_SIZE,
};

#[cfg(any(unix, windows))]
const N_ITEMS: u64 = 1_000;
#[cfg(any(unix, windows))]
const RING_CAPACITY: usize = 128;

#[cfg(any(unix, windows))]
fn main() {
    println!("=== DirectFileRing E2E (unbuffered positioned I/O, page-cache bypass) ===");
    println!();

    let base_path = std::env::temp_dir()
        .join(format!("directfile_e2e_{}", std::process::id()));
    let ring = DirectFileRing::create(&base_path, RING_CAPACITY)
        .expect("create");
    println!("[init] capacity = {RING_CAPACITY} slots, slot size = {DIRECT_FILE_SLOT_SIZE} bytes");

    let start = Instant::now();
    let mut produced_sum = 0u64;
    let mut consumed_sum = 0u64;
    let mut consumed_count = 0u64;

    // Interleaved push + pop to avoid filling the ring.
    let mut buf = [0u8; DIRECT_FILE_SLOT_SIZE];
    for i in 0..N_ITEMS {
        let payload = i.to_le_bytes();
        while ring.try_push(&payload).is_err() {
            std::hint::spin_loop();
        }
        produced_sum += i;
        while ring.try_pop(&mut buf).is_err() {
            std::hint::spin_loop();
        }
        let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
        consumed_sum += v;
        consumed_count += 1;
    }

    let elapsed = start.elapsed();
    let expected_sum: u64 = (0..N_ITEMS).sum();

    println!();
    println!("=== Result ===");
    println!("  elapsed:        {elapsed:?}");
    println!("  items:          {N_ITEMS}");
    println!("  consumed:       {consumed_count}");
    println!("  produced sum:   {produced_sum}");
    println!("  consumed sum:   {consumed_sum}");
    println!("  expected sum:   {expected_sum}");
    println!("  head:           {}", ring.head());
    println!("  tail:           {}", ring.tail());

    assert_eq!(consumed_count, N_ITEMS, "INTEGRITY FAIL: count mismatch");
    assert_eq!(consumed_sum, produced_sum,
               "INTEGRITY FAIL: sum mismatch");
    assert_eq!(consumed_sum, expected_sum);
    println!("  integrity:      PASS");
    println!("    every item round-tripped via unbuffered positioned I/O");
    println!("    no page cache touched on the data path");
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("direct_file_round_trip needs O_DIRECT (unix) or FILE_FLAG_NO_BUFFERING (windows).");
}
