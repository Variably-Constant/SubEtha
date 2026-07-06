//! A/B bench for the GF(256) multiply-add ladder (scalar / SSSE3 / AVX2 /
//! AVX-512-PSHUFB / GFNI) that drives Reed-Solomon FEC encode and decode.
//!
//! Two views, both forcing one backend at a time so the comparison is a clean
//! A/B (same buffer, same coefficients, only the rung differs):
//!
//!  - **primitive**: `gf_mul_add` over a fixed buffer - the raw per-rung
//!    throughput of the inner step.
//!  - **end-to-end encode**: `RsCode::encode` of a realistic `(k, r, shard)`
//!    block - the speedup production actually sees, since encode IS the hot
//!    path.
//!
//! Only backends `available()` on this host run. An AVX2 box reports
//! scalar / SSSE3 / AVX2 plus the (deliberately slow) affine emulation that
//! validates the GFNI logic; a GFNI / AVX-512 box (Genoa, Sapphire Rapids,
//! Zen 4+) adds the AVX-512-PSHUFB and GFNI rungs and is where the top of the
//! ladder is measured.

use std::hint::black_box;
use std::time::Instant;
use subetha_cxc::fec::{gf_mul_add_backend, GfBackend, RsCode};

const ALL: [GfBackend; 8] = [
    GfBackend::Scalar,
    GfBackend::AffineScalar,
    GfBackend::Ssse3,
    GfBackend::Avx2,
    GfBackend::Avx512Pshufb,
    GfBackend::Gfni256,
    GfBackend::Gfni512,
    GfBackend::Neon,
];

/// GB/s of the `gf_mul_add` primitive on a `buf_len`-byte buffer.
fn bench_primitive(b: GfBackend, buf_len: usize, iters: usize) -> f64 {
    let src: Vec<u8> = (0..buf_len).map(|i| ((i * 73 + 11) & 0xff) as u8).collect();
    let mut out = vec![0u8; buf_len];
    gf_mul_add_backend(b, &mut out, &src, 7); // warm
    let t = Instant::now();
    for k in 0..iters {
        // A coefficient that is never 0 or 1 (which would shortcut the multiply).
        let coef = (2 + (k % 253)) as u8;
        gf_mul_add_backend(b, black_box(&mut out), black_box(&src), coef);
    }
    let secs = t.elapsed().as_secs_f64();
    black_box(&out);
    (buf_len * iters) as f64 / secs / 1e9
}

/// GB/s of data through `RsCode::encode` for a `(k, r)` block of `shard`-byte
/// shards, forcing backend `b`. Throughput counts the `k` data shards (the
/// payload), since parity is overhead.
fn bench_encode(b: GfBackend, k: usize, r: usize, shard: usize, iters: usize) -> f64 {
    let code = RsCode::new(k, r).expect("code").with_backend(Some(b));
    let data: Vec<Vec<u8>> = (0..k)
        .map(|i| (0..shard).map(|x| ((i * 131 + x * 17 + 7) & 0xff) as u8).collect())
        .collect();
    let mut parity: Vec<Vec<u8>> = vec![vec![0u8; shard]; r];
    {
        let dr: Vec<&[u8]> = data.iter().map(|s| s.as_slice()).collect();
        let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(|s| s.as_mut_slice()).collect();
        code.encode(&dr, &mut pr).expect("encode"); // warm
    }
    let t = Instant::now();
    for _ in 0..iters {
        let dr: Vec<&[u8]> = data.iter().map(|s| s.as_slice()).collect();
        let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(|s| s.as_mut_slice()).collect();
        code.encode(black_box(&dr), black_box(&mut pr)).expect("encode");
    }
    let secs = t.elapsed().as_secs_f64();
    black_box(&parity);
    (k * shard * iters) as f64 / secs / 1e9
}

fn main() {
    let avail: Vec<GfBackend> = ALL.into_iter().filter(|b| b.available()).collect();
    println!("GF(256) multiply-add ladder A/B (available rungs on this host)\n");
    println!("primitive gf_mul_add, 64 KiB buffer:");
    let scalar_p = bench_primitive(GfBackend::Scalar, 64 * 1024, 20_000);
    for &b in &avail {
        let gbps = bench_primitive(b, 64 * 1024, 20_000);
        println!(
            "  {:<16} {:>7.2} GB/s   {:>5.2}x vs scalar",
            b.name(),
            gbps,
            gbps / scalar_p
        );
    }
    println!("\nprimitive gf_mul_add, 1408 B shard (production datagram size):");
    let scalar_s = bench_primitive(GfBackend::Scalar, 1408, 400_000);
    for &b in &avail {
        let gbps = bench_primitive(b, 1408, 400_000);
        println!(
            "  {:<16} {:>7.2} GB/s   {:>5.2}x vs scalar",
            b.name(),
            gbps,
            gbps / scalar_s
        );
    }
    println!("\nend-to-end RsCode::encode, k=8 r=4 shard=1408:");
    let scalar_e = bench_encode(GfBackend::Scalar, 8, 4, 1408, 20_000);
    for &b in &avail {
        let gbps = bench_encode(b, 8, 4, 1408, 20_000);
        println!(
            "  {:<16} {:>7.2} GB/s   {:>5.2}x vs scalar",
            b.name(),
            gbps,
            gbps / scalar_e
        );
    }
    println!("\n(affine-emulated is the bit-exact GFNI software path; it is slow");
    println!(" by design - it proves the GFNI logic where the silicon is absent.)");
}
