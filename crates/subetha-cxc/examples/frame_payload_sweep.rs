//! A/B payload-size sweep for [`FrameRing`].
//!
//! Three contenders per payload size:
//! - `frame.auto`: the ring picks inline (small) or region (large)
//! - `frame.offset`: the ring is forced to the region every time
//!   (the manual-arena indirection, now automated)
//! - `raw.spsc`: the fixed 64-byte `SpscRingCore` (size <= 64)
//!
//! Each contender round-trips one record (send then recv) `N` times on
//! one producer + one consumer, reported as min-of-`REPEATS` ns/op. A
//! correctness pass runs first: every size is sent and recovered
//! byte-for-byte in both frame modes, so timing only runs on a
//! verified-correct ring (this is the example's E2E proof, not a test).

use std::time::Instant;

use subetha_cxc::frame_ring::{FrameRing, LayoutHint};
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};
use subetha_cxc::FrameClass;

const SIZES: &[usize] = &[8, 16, 32, 48, 56, 64, 128, 256, 512, 1024, 4096];
const N: usize = 200_000;
const REPEATS: usize = 5;

fn best_of(mut f: impl FnMut() -> f64) -> f64 {
    let mut best = f64::INFINITY;
    for _ in 0..REPEATS {
        best = best.min(f());
    }
    best
}

/// E2E correctness: every size round-trips byte-for-byte in both modes.
fn verify() {
    let r = FrameRing::create_anon(64, 64, 1 << 20).unwrap();
    let mut buf = Vec::new();
    for &size in SIZES {
        for hint in [LayoutHint::Auto, LayoutHint::ForceOffset] {
            let payload: Vec<u8> =
                (0..size).map(|i| (i as u32).wrapping_mul(2654435761) as u8).collect();
            r.send_as(&payload, hint).unwrap();
            r.recv_into(&mut buf).unwrap();
            assert_eq!(buf, payload, "size {size} hint {hint:?} round-trip mismatch");
        }
    }
    println!("correctness: every size round-trips byte-for-byte in Auto and ForceOffset");
}

fn bench_frame(size: usize, hint: LayoutHint) -> (f64, FrameClass) {
    let r = FrameRing::create_anon(64, 64, 1 << 20).unwrap();
    let payload = vec![0xA5u8; size];
    let mut buf = Vec::with_capacity(size);
    let class = r.send_as(&payload, hint).unwrap();
    r.recv_into(&mut buf).unwrap();
    let ns = best_of(|| {
        let t = Instant::now();
        for _ in 0..N {
            r.send_as(&payload, hint).unwrap();
            r.recv_into(&mut buf).unwrap();
        }
        t.elapsed().as_nanos() as f64 / N as f64
    });
    (ns, class)
}

fn bench_raw(size: usize) -> Option<f64> {
    if size > SPSC_PAYLOAD_BYTES {
        return None;
    }
    let r = SpscRingCore::create_anon(64).unwrap();
    let payload = vec![0xA5u8; size];
    let mut out = [0u8; SPSC_PAYLOAD_BYTES];
    Some(best_of(|| {
        let t = Instant::now();
        for _ in 0..N {
            r.try_push(&payload).unwrap();
            r.try_pop(&mut out).unwrap();
        }
        t.elapsed().as_nanos() as f64 / N as f64
    }))
}

fn main() {
    println!("FrameRing payload sweep: round-trip ns/op, min-of-{REPEATS}, N={N}\n");
    verify();
    println!();
    println!("| payload | frame.auto | class  | frame.offset | raw.spsc | auto vs offset |");
    println!("|--------:|-----------:|:-------|-------------:|---------:|---------------:|");
    for &size in SIZES {
        let (auto, class) = bench_frame(size, LayoutHint::Auto);
        let (offset, _) = bench_frame(size, LayoutHint::ForceOffset);
        let raw = bench_raw(size);
        let raw_s = raw.map(|v| format!("{v:.1}")).unwrap_or_else(|| "-".to_string());
        let speedup = offset / auto;
        let class_s = match class {
            FrameClass::Inline => "inline",
            FrameClass::Offset => "offset",
        };
        println!(
            "| {size:>5} B | {auto:>10.1} | {class_s:<6} | {offset:>12.1} | {raw_s:>8} | {speedup:>13.2}x |",
        );
    }
}
