//! Bench: what the async calling convention costs on the hot path.
//!
//! `Channel<T>` answers `recv()` (sync), `recv_blocking()`, and
//! `recv_async().await` on one handle. Async is NOT a lower-latency
//! path: the sync recv skips all Waker machinery (one relaxed load on
//! the `has_recv_waiter` gate), while the async recv constructs a
//! future and polls it. This bench quantifies that tax on the
//! item-available fast path, where every contender returns without
//! parking.
//!
//! Three contenders, one `Channel<u64>`, 8-byte payload, single thread
//! pushing then popping so the item is always present:
//!
//! - **sync**: `send` / `recv`. The floor - no blocking support
//!   touched, no future.
//! - **blocking**: `send_blocking` / `recv_blocking` with `None`
//!   timeout. Same fast path plus the blocking-path re-check.
//! - **async**: `send_async().await` / `recv_async().await` driven by
//!   the crate's runtime-free `block_on`. The future construction +
//!   single ready-poll is the measured delta.
//!
//! Bench audit: all three push and pop the SAME `Channel<u64>` with the
//! SAME 8-byte payload on the item-available fast path. The only
//! difference is the calling convention - no contender pays a surplus
//! alloc, lock, or syscall the others avoid. `block_on` setup is
//! amortized: each iteration drives a `BATCH`-item inner loop inside one
//! `block_on`, and the reported throughput is per item.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use subetha_cxc::reactor::block_on;
use subetha_cxc::AutoIpc;

const BATCH: u64 = 1000;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha-async-overhead-{name}-{pid}-{nonce}"));
    p
}

fn cleanup(path: &std::path::Path) {
    std::fs::remove_file(path).ok();
    for suffix in [".cw", ".pw"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        std::fs::remove_file(std::path::PathBuf::from(p)).ok();
    }
}

fn round_trip(c: &mut Criterion) {
    let path = tmp("rt");
    cleanup(&path);
    let chan = Arc::new(
        AutoIpc::new(&path)
            .producers(1)
            .consumers(1)
            .capacity(1024)
            .build_channel::<u64>()
            .expect("build_channel"),
    );

    let mut group = c.benchmark_group("async_overhead.round_trip");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function("sync", |b| {
        b.iter(|| {
            for i in 0..BATCH {
                chan.send(black_box(&i)).unwrap();
                let v: u64 = chan.recv().unwrap();
                black_box(v);
            }
        });
    });

    group.bench_function("blocking", |b| {
        b.iter(|| {
            for i in 0..BATCH {
                chan.send_blocking(black_box(&i), None).unwrap();
                let v: u64 = chan.recv_blocking(None).unwrap();
                black_box(v);
            }
        });
    });

    group.bench_function("async", |b| {
        b.iter(|| {
            block_on(async {
                for i in 0..BATCH {
                    chan.send_async(black_box(&i)).await.unwrap();
                    let v: u64 = chan.recv_async().await.unwrap();
                    black_box(v);
                }
            });
        });
    });

    group.finish();
    drop(chan);
    cleanup(&path);
}

criterion_group!(benches, round_trip);
criterion_main!(benches);
