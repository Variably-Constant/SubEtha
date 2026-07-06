//! Cross-process E2E for `DirectFileRing` (unbuffered file I/O).
//!
//! Proves the page-cache-bypass path works ACROSS PROCESSES, which is
//! the primitive's reason to exist: a producer process writes payload
//! slots with the page cache bypassed (straight to the block device),
//! a separate consumer process reads them back the same way, and the
//! two coordinate purely through the head/tail counters in the shared
//! MMF. The consumer verifies it received every item, in order, with
//! the exact sum - i.e. the producer's device-resident writes are
//! visible to an independent reader with no shared mmap of the data.
//!
//! The parent acts as consumer (creates the ring), spawns itself with
//! `produce <base> <n>` as the producer, drains `n` items, and checks
//! the integrity before reaping the child.
//!
//! Run:
//!     cargo run --release --example direct_file_xproc

#[cfg(any(unix, windows))]
fn main() {
    use std::time::{Duration, Instant};
    use subetha_cxc::protocol_direct_file::{DirectFileRing, DIRECT_FILE_SLOT_SIZE};

    const N: u64 = 5_000;
    const CAP: usize = 256;

    let args: Vec<String> = std::env::args().collect();

    // --- producer role: open by path, push n sequenced payloads, exit ---
    if args.get(1).map(String::as_str) == Some("produce") {
        let base = &args[2];
        let n: u64 = args[3].parse().expect("n");
        // The parent creates the ring just before spawning us; retry the
        // open briefly in case we win the race.
        let start = Instant::now();
        let ring = loop {
            match DirectFileRing::open(base, CAP) {
                Ok(r) => break r,
                Err(_) if start.elapsed() < Duration::from_secs(5) => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => panic!("producer open: {e}"),
            }
        };
        for i in 0..n {
            let payload = i.to_le_bytes();
            while ring.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
        // Hold the open fd until the parent has drained everything, so the
        // device-resident slots stay readable; the parent reaps us.
        std::thread::sleep(Duration::from_millis(200));
        return;
    }

    // --- consumer role (parent): create, spawn producer, drain, verify ---
    println!("=== DirectFileRing CROSS-PROCESS E2E (unbuffered I/O, no shared data mmap) ===");
    let base = std::env::temp_dir().join(format!("directfile_xproc_{}", std::process::id()));
    let base_str = base.to_string_lossy().to_string();
    let ring = DirectFileRing::create(&base, CAP).expect("create");
    println!("[parent] created ring: cap={CAP} slot={DIRECT_FILE_SLOT_SIZE}B at {base_str}");

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = std::process::Command::new(exe)
        .args(["produce", &base_str, &N.to_string()])
        .spawn()
        .expect("spawn producer");
    println!("[parent] spawned producer pid={}", child.id());

    let start = Instant::now();
    let mut buf = [0u8; DIRECT_FILE_SLOT_SIZE];
    let mut got = 0u64;
    let mut sum = 0u64;
    while got < N {
        match ring.try_pop(&mut buf) {
            Ok(_) => {
                let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                assert_eq!(v, got, "ORDER FAIL: got {v}, expected {got}");
                sum += v;
                got += 1;
            }
            Err(_) => {
                if start.elapsed() > Duration::from_secs(30) {
                    panic!("timeout draining: got {got} / {N}");
                }
                std::hint::spin_loop();
            }
        }
    }
    let elapsed = start.elapsed();
    let status = child.wait().expect("reap producer");

    let expected: u64 = (0..N).sum();
    println!();
    println!("=== Result ===");
    println!("  elapsed:      {elapsed:?}");
    println!("  items:        {N}");
    println!("  consumed:     {got}");
    println!("  consumed sum: {sum}");
    println!("  expected sum: {expected}");
    println!("  producer exit: {status}");
    assert_eq!(got, N, "INTEGRITY FAIL: count");
    assert_eq!(sum, expected, "INTEGRITY FAIL: sum");
    assert!(status.success(), "producer process failed");
    println!("  integrity:    PASS");
    println!("    producer's unbuffered writes (device-resident) read back by an");
    println!("    independent consumer process, page cache bypassed - coordinated");
    println!("    only through the shared head/tail MMF, no shared data mapping.");
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("direct_file_xproc needs unbuffered file I/O (unix/windows).");
}
