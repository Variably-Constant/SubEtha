//! Bench: cost of the `Arc<dyn MessageTransport>` indirection
//! introduced by the BackgroundScheduler migration.
//!
//! Three contenders, all pushing / popping the same 56-byte payload
//! through the same `SharedRing`:
//!
//! - `direct`: `SharedRing::try_push` called on a concrete handle.
//! - `arc_concrete`: `(&*arc_ring).try_push` where `arc_ring` is
//!   `Arc<SharedRing>`. Adds one Arc deref but no vtable.
//! - `arc_dyn`: `arc_dyn.try_push` where `arc_dyn` is
//!   `Arc<dyn MessageTransport>`. This is the topology
//!   `BackgroundScheduler` uses post-migration. Adds Arc deref +
//!   vtable lookup.

#![allow(clippy::missing_docs_in_private_items)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{MessageTransport, SharedRing};
use subetha_cxc::shared_ring::PAYLOAD_BYTES;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha-bench-indirection-{name}-{pid}-{nonce}.bin"));
    p
}

fn bench_direct(c: &mut Criterion) {
    let path = tmp("direct");
    let ring = SharedRing::create(&path, 1024).expect("create");
    let payload = [0xABu8; PAYLOAD_BYTES];
    let mut out = [0u8; PAYLOAD_BYTES];

    c.bench_function("transport_indirection/direct_try_push", |b| {
        b.iter(|| {
            if ring.try_push(black_box(&payload)).is_ok() {
                ring.try_pop(black_box(&mut out)).ok();
            }
            black_box(&out);
        });
    });

    std::fs::remove_file(&path).ok();
}

fn bench_arc_concrete(c: &mut Criterion) {
    let path = tmp("arc_concrete");
    let ring: Arc<SharedRing> = Arc::new(SharedRing::create(&path, 1024).expect("create"));
    let payload = [0xABu8; PAYLOAD_BYTES];
    let mut out = [0u8; PAYLOAD_BYTES];

    c.bench_function("transport_indirection/arc_concrete_try_push", |b| {
        b.iter(|| {
            if ring.try_push(black_box(&payload)).is_ok() {
                ring.try_pop(black_box(&mut out)).ok();
            }
            black_box(&out);
        });
    });

    std::fs::remove_file(&path).ok();
}

fn bench_arc_dyn(c: &mut Criterion) {
    let path = tmp("arc_dyn");
    let ring: Arc<dyn MessageTransport> =
        Arc::new(SharedRing::create(&path, 1024).expect("create"));
    let payload = [0xABu8; PAYLOAD_BYTES];
    let mut out = [0u8; PAYLOAD_BYTES];

    c.bench_function("transport_indirection/arc_dyn_try_push", |b| {
        b.iter(|| {
            if ring.try_push(black_box(&payload)).is_ok() {
                ring.try_pop(black_box(&mut out)).ok();
            }
            black_box(&out);
        });
    });

    std::fs::remove_file(&path).ok();
}

criterion_group!(
    benches,
    bench_direct,
    bench_arc_concrete,
    bench_arc_dyn,
);
criterion_main!(benches);
