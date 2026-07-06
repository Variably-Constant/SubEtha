//! Bench: AdaptiveIpc<T> pinned-path vs adaptive-path vs native primitive.
//!
//! AdaptiveIpc's ring backing was wired through to AdaptiveRing so
//! the protocol-axis pin (`PinnedIpc::as_ring()`) returns the
//! AdaptiveRing; chaining into `pin_current_shape()` gives a
//! shape-pinned native primitive (`PinnedRing`) and from there
//! `spsc_try_push` / `spsc_try_pop` route to the bare
//! `SpscRingCore::try_push` / `try_pop`.
//!
//! What this bench measures (1P/1C SPSC path, the default initial
//! shape AdaptiveIpc registers):
//!
//! - **native**: raw `SpscRingCore::try_push` / `try_pop`. The
//!   absolute floor for the SPSC primitive that AdaptiveRing
//!   morphs to under 1P/1C.
//! - **adaptive**: `AdaptiveIpc::send_u64` / `recv`. Pays the
//!   MMF Acquire-load on the control flag, the match dispatch,
//!   the AdaptiveRing's own shape-tag Acquire-load + dispatch,
//!   and the profile counter `fetch_add`.
//! - **pinned**: two-axis composition. Pin PinnedIpc, drop to
//!   AdaptiveRing, pin PinnedRing, call `spsc_try_push` /
//!   `spsc_try_pop`. Native primitive speed with two cheap
//!   Acquire loads per validity check (one per axis, at the
//!   caller's chosen cadence).
//!
//! Bench audit: the native column compares against the same
//! primitive (`SpscRingCore`) the pinned-IPC path drops to. Both
//! columns push 8-byte payloads into a 64-byte SPSC slot. Apples
//! to apples on the underlying primitive; the pinned column only
//! adds the two Acquire loads needed for pin validity (not done
//! per-op in the bench - the pin captures once at setup, the loop
//! body is the native call).

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::spsc_ring::SpscRingCore;
use subetha_cxc::{AdaptiveIpc, MmfWorkloadShape};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha-ipc-bench-{name}-{pid}-{nonce}"));
    p
}

// ===================================================================
// SPSC round-trip: single-threaded push + pop per iter.
// ===================================================================

fn spsc_round_trip(c: &mut Criterion) {
    let payload = 0xDEADBEEFu64.to_le_bytes();
    let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];

    // ---- native: bare SpscRingCore ----
    let native = SpscRingCore::create_anon(1024)
        .expect("native SpscRingCore create");
    c.bench_function("adaptive_ipc_overhead.spsc_round_trip/native", |b| {
        b.iter(|| {
            native.try_push(black_box(&payload)).unwrap();
            native.try_pop(black_box(&mut buf)).unwrap();
        });
    });
    drop(native);

    // ---- adaptive + pinned share the same AdaptiveIpc instance ----
    let ipc_path = tmp("ipc-rt");
    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(
            &ipc_path,
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 1,
                n_consumers: 1,
            },
            1024,
            1,
        )
        .expect("ipc create"),
    );

    c.bench_function("adaptive_ipc_overhead.spsc_round_trip/adaptive", |b| {
        b.iter(|| {
            ipc.send_u64(black_box(0xDEADBEEFu64)).unwrap();
            let v: u64 = ipc.recv().unwrap();
            black_box(v);
        });
    });

    // Two-axis pin: PinnedIpc -> AdaptiveRing -> PinnedRing.
    let pin_ipc = ipc.pin_current_family();
    let ring = pin_ipc.as_ring().expect("pinned at ring family");
    let pin_ring = ring.pin_current_shape();
    assert_eq!(pin_ring.shape(), subetha_cxc::RingShape::Spsc,
               "1P/1C registration should give initial SPSC shape");
    c.bench_function("adaptive_ipc_overhead.spsc_round_trip/pinned", |b| {
        b.iter(|| {
            pin_ring.spsc_try_push(black_box(&payload)).unwrap();
            pin_ring.spsc_try_pop(black_box(&mut buf)).unwrap();
        });
    });
}

// ===================================================================
// Push-only loop: 1000 pushes per iter (no pops in between). Captures
// the per-push cost when the ring has headroom.
// ===================================================================

fn push_only_1000(c: &mut Criterion) {
    const N: usize = 1000;
    let payload = 0xDEADBEEFu64.to_le_bytes();

    // ---- native: bare SpscRingCore ----
    let native = SpscRingCore::create_anon(8192)
        .expect("native SpscRingCore create");
    c.bench_function("adaptive_ipc_overhead.push_only_1000/native", |b| {
        b.iter(|| {
            for _ in 0..N {
                native.try_push(black_box(&payload)).unwrap();
            }
            let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            for _ in 0..N {
                native.try_pop(&mut buf).unwrap();
            }
        });
    });
    drop(native);

    // ---- adaptive ----
    let ipc_path = tmp("ipc-push");
    let ipc = Arc::new(
        AdaptiveIpc::<u64>::create(
            &ipc_path,
            MmfWorkloadShape::StreamingMpmc {
                n_producers: 1,
                n_consumers: 1,
            },
            8192,
            1,
        )
        .expect("ipc create"),
    );

    c.bench_function("adaptive_ipc_overhead.push_only_1000/adaptive", |b| {
        b.iter(|| {
            for _ in 0..N {
                ipc.send_u64(black_box(0xDEADBEEFu64)).unwrap();
            }
            for _ in 0..N {
                let v: u64 = ipc.recv().unwrap();
                black_box(v);
            }
        });
    });

    // ---- pinned: two-axis composition ----
    let pin_ipc = ipc.pin_current_family();
    let ring = pin_ipc.as_ring().expect("pinned at ring family");
    let pin_ring = ring.pin_current_shape();
    c.bench_function("adaptive_ipc_overhead.push_only_1000/pinned", |b| {
        b.iter(|| {
            for _ in 0..N {
                pin_ring.spsc_try_push(black_box(&payload)).unwrap();
            }
            let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            for _ in 0..N {
                pin_ring.spsc_try_pop(&mut buf).unwrap();
            }
        });
    });
}

criterion_group!(benches, spsc_round_trip, push_only_1000);
criterion_main!(benches);
