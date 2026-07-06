//! Both adaptive front doors do sync AND async on one handle. A channel
//! built the normal way - `AutoIpc::new(path).build_channel()` or
//! `.build_adaptive()` - answers `recv()` (sync), `recv_blocking()`, and
//! `recv_async().await` (and the send equivalents), not a second type.
//!
//! This program drives each endpoint three ways and checks the total.
//!
//! Run:
//!     cargo run --release --example channel_async -p subetha-cxc

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use subetha_cxc::reactor::block_on;
use subetha_cxc::AutoIpc;

const N: u64 = 300_000;
const PER: u64 = N / 3;

fn main() {
    demo_channel();
    demo_adaptive();
}

/// `build_channel` -> `Channel<T>` (streaming SharedRing).
fn demo_channel() {
    let path = tmp("channel");
    cleanup(&path);
    let chan = Arc::new(
        AutoIpc::new(&path)
            .producers(4)
            .consumers(4)
            .capacity(1024)
            .build_channel::<u64>()
            .expect("build_channel"),
    );

    println!("== build_channel -> Channel<u64>: sync + blocking + async ==");
    let expected: u64 = (0..N).sum();
    let t0 = Instant::now();

    let producer = {
        let chan = Arc::clone(&chan);
        thread::spawn(move || {
            block_on(async move {
                for i in 0..N {
                    if i < PER {
                        while chan.send(&i).is_err() {
                            std::hint::spin_loop();
                        }
                    } else if i < 2 * PER {
                        chan.send_blocking(&i, None).expect("send_blocking");
                    } else {
                        chan.send_async(&i).await.expect("send_async");
                    }
                }
            });
        })
    };

    let sum = block_on(async move {
        let mut s = 0u64;
        for i in 0..N {
            let v = if i < PER {
                loop {
                    match chan.recv() {
                        Ok(v) => break v,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            } else if i < 2 * PER {
                chan.recv_blocking(None).expect("recv_blocking")
            } else {
                chan.recv_async().await.expect("recv_async")
            };
            s = s.wrapping_add(v);
        }
        s
    });
    producer.join().expect("producer");
    let elapsed = t0.elapsed();

    assert_eq!(sum, expected, "channel: every item once");
    cleanup(&path);
    println!("  {N} items, integrity OK, {:.2} M items/s\n",
             N as f64 / elapsed.as_secs_f64() / 1e6);
}

/// `build_adaptive` -> `AdaptiveIpc<T>` (the ring<->deque migrating
/// endpoint). Same three calling conventions on one handle.
fn demo_adaptive() {
    let path = tmp("adaptive");
    cleanup_adaptive(&path);
    let ipc = Arc::new(
        AutoIpc::new(&path)
            .consumers(1)
            .capacity(1024)
            .build_adaptive::<u64>()
            .expect("build_adaptive"),
    );

    println!("== build_adaptive -> AdaptiveIpc<u64>: sync + blocking + async ==");
    let expected: u64 = (0..N).sum();
    let t0 = Instant::now();

    let producer = {
        let ipc = Arc::clone(&ipc);
        thread::spawn(move || {
            block_on(async move {
                for i in 0..N {
                    if i < PER {
                        while ipc.send(&i).is_err() {
                            std::hint::spin_loop();
                        }
                    } else if i < 2 * PER {
                        ipc.send_blocking(&i, None).expect("send_blocking");
                    } else {
                        ipc.send_async(&i).await.expect("send_async");
                    }
                }
            });
        })
    };

    let sum = block_on(async move {
        let mut s = 0u64;
        for i in 0..N {
            let v = if i < PER {
                loop {
                    match ipc.recv() {
                        Ok(v) => break v,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            } else if i < 2 * PER {
                ipc.recv_blocking(None).expect("recv_blocking")
            } else {
                ipc.recv_async().await.expect("recv_async")
            };
            s = s.wrapping_add(v);
        }
        s
    });
    producer.join().expect("producer");
    let elapsed = t0.elapsed();

    assert_eq!(sum, expected, "adaptive: every item once");
    cleanup_adaptive(&path);
    println!("  {N} items, integrity OK, {:.2} M items/s",
             N as f64 / elapsed.as_secs_f64() / 1e6);
    println!("  one channel - async is a calling convention, not a second surface.");
}

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "subetha-channel-async-{tag}-{}.bin", std::process::id(),
    ))
}

fn rm_suffix(path: &std::path::Path, suffix: &str) {
    let mut p = path.as_os_str().to_owned();
    p.push(suffix);
    std::fs::remove_file(std::path::PathBuf::from(p)).ok();
}

fn cleanup(path: &std::path::Path) {
    std::fs::remove_file(path).ok();
    rm_suffix(path, ".cw");
    rm_suffix(path, ".pw");
}

fn cleanup_adaptive(path: &std::path::Path) {
    // AdaptiveIpc lays out several adjacent MMF files by stem.
    for suffix in [".ctl.bin", ".deque.bin", ".pingen.bin"] {
        let stem = path.file_stem().map(|s| s.to_owned()).unwrap_or_default();
        let mut p = path.to_path_buf();
        p.set_file_name(format!("{}{suffix}", stem.to_string_lossy()));
        std::fs::remove_file(p).ok();
    }
    std::fs::remove_file(path).ok();
}
