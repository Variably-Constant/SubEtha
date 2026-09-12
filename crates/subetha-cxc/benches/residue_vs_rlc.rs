//! The residue erasure code against the sliding-window RLC, at the same
//! symbol size and the same redundancy, so the numbers are comparable.
//!
//! Both recover lost packets and both spend one repair per `k` sources,
//! so a run of each over the same payload is a like-for-like reading of
//! what the construction costs. What it does not read is recovery
//! power: the two spread their redundancy differently, and that
//! comparison belongs on a loss trace rather than in a timing harness.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::hint::black_box;

use subetha_cxc::residue_fec::PacketResidueCode;
use subetha_cxc::rlc_fec::{RlcDecoder, RlcEncoder};

/// Sources protected by one repair, in both codes.
const K: usize = 8;
/// Bytes in a symbol. The RLC's own slot size, so neither code is being
/// measured at a width the other would not see.
const SYMBOL: usize = 1024;

/// Deterministic payload, so a run measures the code rather than the
/// allocator's mood or a pattern the moduli happen to like.
fn payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

fn sources() -> Vec<Vec<u8>> {
    (0..K).map(|i| payload(0x9E37_79B9_7F4A_7C15 ^ i as u64, SYMBOL)).collect()
}

/// One repair produced over `K` sources.
fn encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("one repair over 8 sources");

    let residue = PacketResidueCode::new(K, 1).expect("a residue code");
    let src = sources();
    let refs: Vec<&[u8]> = src.iter().map(|s| s.as_slice()).collect();
    group.bench_function("residue, CRT", |b| {
        b.iter_batched(
            Vec::new,
            |mut repairs| {
                residue
                    .encode_packets(black_box(&refs), &mut repairs)
                    .expect("encode");
                repairs
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sliding-window RLC, GF(2^8)", |b| {
        b.iter_batched(
            || RlcEncoder::new(K, K, 15, SYMBOL),
            |mut enc| {
                let mut last = None;
                for s in &src {
                    let (_, repair) = enc.push_source(black_box(s));
                    if repair.is_some() {
                        last = repair;
                    }
                }
                last
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// One lost source recovered from the repair, which is the operation a
/// live stream actually pays for.
fn recover_one_loss(c: &mut Criterion) {
    let mut group = c.benchmark_group("recover one lost source of 8");

    let residue = PacketResidueCode::new(K, 1).expect("a residue code");
    let src = sources();
    let refs: Vec<&[u8]> = src.iter().map(|s| s.as_slice()).collect();
    let mut repairs = Vec::new();
    residue.encode_packets(&refs, &mut repairs).expect("encode");

    // Everything but source 3, plus the repair.
    let mut have: Vec<(usize, &[u8])> = (0..K)
        .filter(|i| *i != 3)
        .map(|i| (i, src[i].as_slice()))
        .collect();
    have.push((K, repairs[0].as_slice()));

    group.bench_function("residue, CRT", |b| {
        b.iter_batched(
            || vec![Vec::new(); K],
            |mut out| {
                residue
                    .decode_packets(black_box(&have), SYMBOL, &mut out)
                    .expect("decode");
                out
            },
            BatchSize::SmallInput,
        )
    });

    // The same loss through the RLC: feed every source but one, then the
    // repair that spans them.
    let mut enc = RlcEncoder::new(K, K, 15, SYMBOL);
    let mut repair = None;
    let mut ids = Vec::new();
    for s in &src {
        let (sid, r) = enc.push_source(s);
        ids.push(sid);
        if r.is_some() {
            repair = r;
        }
    }
    let repair = repair.expect("a repair after k sources");

    group.bench_function("sliding-window RLC, GF(2^8)", |b| {
        b.iter_batched(
            || RlcDecoder::new(SYMBOL),
            |mut dec| {
                for (i, s) in src.iter().enumerate() {
                    if i != 3 {
                        dec.on_source(ids[i], s);
                    }
                }
                dec.on_repair(black_box(repair.clone()))
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, encode, recover_one_loss);
criterion_main!(benches);
