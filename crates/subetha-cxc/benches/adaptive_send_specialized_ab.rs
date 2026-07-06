//! A/B bench: does the IR-pass-marker-style specialized `send_u64`
//! fast path actually deliver a speedup over the generic
//! `send::<u64>` Marshal path?
//!
//! Contenders:
//! - **A (baseline / current path)**: `AdaptiveIpc<u64>::send(&val)`.
//!   This is what every consumer who has a u64 stream writes today;
//!   it goes through the `Marshal` trait: `T::marshal(&mut buf[..8])`
//!   on a 56-byte buffer, slice-bounds-check on `T::PAYLOAD_BYTES`,
//!   then the SharedRing / SharedDeque push.
//! - **B (specialized fast path)**: `AdaptiveIpc<u64>::send_u64(val)`.
//!   Hand-rolled specialization equivalent to what an IR-pass marker
//!   rewrite produces: 8-byte payload buffer, direct `to_le_bytes`,
//!   no Marshal indirection, no slice bounds check on a runtime
//!   `T::PAYLOAD_BYTES`.
//!
//! Both end up pushing the same 8-byte payload prefix into the same
//! transport; the difference is the call-graph shape that LLVM sees.
//!
//! Workload: SPSC sustained burst of N=10000 sends, consumer drains
//! through the same `recv()` in both cases.
//!
//! Bench audit (HARD RULE 3):
//! - Both contenders push the same payload bytes through the same
//!   `SharedRing` transport (TAG_RING path).
//! - Same N, same consumer drain pattern, same `iter_custom` shape
//!   so per-iter wall-clock is comparable.
//! - The only differential is which `send` method the producer calls.
//! - Neither path allocates on the hot loop; the only allocations
//!   happen at the AdaptiveIpc construction outside the timed window.

#![allow(clippy::missing_docs_in_private_items)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_cxc::{AdaptiveIpc, MmfWorkloadShape};

const N: u64 = 10_000;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha-bench-special-{name}-{pid}-{nonce}"));
    p
}

fn bench_a_generic_send(c: &mut Criterion) {
    let path = tmp("a");
    let shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(&path, shape, 16384, 1).expect("create"),
    );

    c.bench_function("adaptive_send_ab/A_generic_marshal_path", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let ipc_c = Arc::clone(&ipc);
                let stop = Arc::new(AtomicBool::new(false));
                let counted = Arc::new(AtomicU64::new(0));
                let stop_c = Arc::clone(&stop);
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while !stop_c.load(Ordering::Acquire) {
                        match ipc_c.recv() {
                            Ok(v) => {
                                black_box(v);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                });
                let start = Instant::now();
                for i in 0..N {
                    while ipc.send(&i).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while counted.load(Ordering::Acquire) < N {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                drain.join().ok();
            }
            total
        });
    });

    drop(ipc);
    for suffix in &[".ring.bin", ".deque.bin", ".ctl.bin"] {
        let mut p = path.clone();
        let s = p.file_name().map(|s| s.to_owned()).unwrap_or_default();
        p.set_file_name(format!("{}{suffix}", s.to_string_lossy()));
        std::fs::remove_file(&p).ok();
    }
}

fn bench_b_specialized_send(c: &mut Criterion) {
    let path = tmp("b");
    let shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(&path, shape, 16384, 1).expect("create"),
    );

    c.bench_function("adaptive_send_ab/B_specialized_u64_path", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let ipc_c = Arc::clone(&ipc);
                let stop = Arc::new(AtomicBool::new(false));
                let counted = Arc::new(AtomicU64::new(0));
                let stop_c = Arc::clone(&stop);
                let counted_c = Arc::clone(&counted);
                let drain = thread::spawn(move || {
                    while !stop_c.load(Ordering::Acquire) {
                        match ipc_c.recv() {
                            Ok(v) => {
                                black_box(v);
                                counted_c.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                });
                let start = Instant::now();
                for i in 0..N {
                    while ipc.send_u64(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
                while counted.load(Ordering::Acquire) < N {
                    std::hint::spin_loop();
                }
                total += start.elapsed();
                stop.store(true, Ordering::Release);
                drain.join().ok();
            }
            total
        });
    });

    drop(ipc);
    for suffix in &[".ring.bin", ".deque.bin", ".ctl.bin"] {
        let mut p = path.clone();
        let s = p.file_name().map(|s| s.to_owned()).unwrap_or_default();
        p.set_file_name(format!("{}{suffix}", s.to_string_lossy()));
        std::fs::remove_file(&p).ok();
    }
}

criterion_group!(benches, bench_a_generic_send, bench_b_specialized_send);
criterion_main!(benches);
