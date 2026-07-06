//! E2E: batched async file reads through the kernel async-I/O ring
//! (io_uring on Linux, IoRing on Windows, aio + kqueue on FreeBSD) via
//! [`subetha_cxc::kernel_async_ring`].
//!
//! Writes a file of `N` known blocks, then submits `N` async reads - one
//! per block, each tagged with its block index - through the kernel
//! submission ring in a single batch, blocks for all completions, and
//! reaps them from the completion ring. Each completion is verified:
//! the right number of bytes, the tag (`user_data`) that identifies its
//! block, and the block's content. Completions may arrive in any order;
//! the tag routes each back to its buffer.
//!
//! Only the file-open + the ring syscalls are platform-gated; the
//! submit/reap loop and the verification are shared.
//!
//! When the kernel ring is unavailable (old kernel / Windows build, or a
//! sandbox without io_uring) it prints why and exits 0.
//!
//! Run:
//!     cargo run --release --example kernel_async_ring_demo -p subetha-cxc

#[cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))]
fn main() {
    use std::io::Write;
    use std::time::Instant;
    use subetha_cxc::kernel_async_ring::{open_for_async_read, KernelAsyncRing};

    const N: usize = 64;
    const BLOCK: usize = 512;

    println!("=== KernelAsyncRing batched async-read E2E (io_uring / IoRing / aio) ===");

    // Build a file of N blocks; block i starts with its index as u64 LE,
    // the rest filled with (i as u8) so a wrong-block read is detectable.
    let path = std::env::temp_dir().join(format!("karing_demo_{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&path).expect("create");
        for i in 0..N {
            let mut blk = [i as u8; BLOCK];
            blk[..8].copy_from_slice(&(i as u64).to_le_bytes());
            f.write_all(&blk).expect("write block");
        }
        f.flush().expect("flush");
    }
    println!("[init] wrote {N} blocks x {BLOCK} bytes = {} KiB", N * BLOCK / 1024);

    let mut ring = match KernelAsyncRing::new((N as u32).next_power_of_two()) {
        Ok(r) => r,
        Err(e) => {
            println!("kernel async ring unavailable: {e}");
            #[cfg(target_os = "linux")]
            println!("  needs a kernel with io_uring (5.1+); some containers disable it.");
            #[cfg(windows)]
            println!("  needs a Windows build with IoRing (Win11 21H2+).");
            #[cfg(target_os = "freebsd")]
            println!("  needs kqueue (always present); this is unexpected.");
            std::fs::remove_file(&path).ok();
            return;
        }
    };
    let file = open_for_async_read(&path).expect("open_for_async_read");
    println!("[init] kernel ring created, file opened for async I/O");

    // One owned buffer per block; the raw pointers handed to the kernel
    // must stay valid + pinned until reaped, so `bufs` outlives the wait.
    let mut bufs: Vec<Vec<u8>> = (0..N).map(|_| vec![0u8; BLOCK]).collect();

    // Submit + reap with kernel-queue backpressure: fill the in-flight
    // window until the backend's submission limit pushes back, drain
    // completions to free slots, repeat. macOS aio caps outstanding ops
    // per process and signals the cap via `WouldBlock`; io_uring / IoRing /
    // FreeBSD-aio hold all N, so they fill in one wave and drain in the next.
    let mut got = [false; N];
    let mut reaped = 0usize;
    let mut next = 0usize;
    let mut inflight = 0usize;
    let mut waves = 0u32;
    let deadline = Instant::now();
    let t0 = Instant::now();
    while reaped < N {
        while next < N {
            let ptr = bufs[next].as_mut_ptr();
            // SAFETY: `bufs[next]` lives in `bufs` for the whole function;
            // not moved or reallocated until its completion is reaped below.
            let r = unsafe {
                ring.prepare_read(&file, ptr, BLOCK as u32, (next * BLOCK) as u64, next as u64)
            };
            match r {
                Ok(()) => {
                    inflight += 1;
                    next += 1;
                }
                // Submission queue full (the macOS aio per-process limit):
                // stop filling and drain to free slots.
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("prepare_read: {e}"),
            }
        }
        assert!(inflight > 0, "no in-flight ops but {reaped}/{N} reaped");
        ring.submit_and_wait(1).expect("submit_and_wait");
        waves += 1;
        while let Some(c) = ring.reap() {
            let idx = c.user_data as usize;
            let n = c.bytes.expect("read op succeeded");
            assert_eq!(n, BLOCK, "block {idx}: short read {n}");
            let blk = &bufs[idx];
            let stamp = u64::from_le_bytes(blk[..8].try_into().unwrap());
            assert_eq!(stamp, idx as u64, "block {idx}: wrong stamp {stamp}");
            assert_eq!(blk[BLOCK - 1], idx as u8, "block {idx}: wrong fill");
            assert!(!got[idx], "block {idx}: duplicate completion");
            got[idx] = true;
            reaped += 1;
            inflight -= 1;
        }
        assert!(deadline.elapsed().as_secs() < 30, "timed out reaping: {reaped}/{N}");
    }
    let elapsed = t0.elapsed();
    println!("[submit] {reaped} ops completed across {waves} submit/drain wave(s)");

    drop(file);
    std::fs::remove_file(&path).ok();

    assert!(got.iter().all(|&b| b), "every block completed exactly once");
    println!();
    println!("=== Result ===");
    println!("  blocks:    {N} (all completed exactly once, in-tag verified)");
    println!("  bytes:     {} KiB via {N} kernel async reads", N * BLOCK / 1024);
    println!("  elapsed:   {elapsed:?}");
    println!("  integrity: PASS (batched submit -> kernel -> completion ring)");
}

#[cfg(not(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos")))]
fn main() {
    eprintln!("kernel_async_ring_demo needs io_uring (Linux), IoRing (Windows), \
               or POSIX aio (FreeBSD / macOS).");
}
