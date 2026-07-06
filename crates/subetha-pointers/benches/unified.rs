//! Bench: capability validation hot path (RaspBatch SoA SIMD + cheri
//! ReadableCapability / WritableCapability) and the owned-capability
//! RAII construct/drop cost, against native baselines.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use subetha_pointers::adaptive_cheri_pointer::{
    CapabilityPermission, OwnedReadableCapability, OwnedWritableCapability,
    ReadableCapability, WritableCapability,
};
use subetha_pointers::adaptive_rasp_batch::{RaspBatch, RaspPermission};

// ============================================================================
// Group 1: Capability validation hot path (RASP + cheri ReadableCapability)
// ============================================================================

fn group_capability_validation(c: &mut Criterion) {
    let mut grp = c.benchmark_group("capability_validation_10k");
    grp.throughput(Throughput::Elements(10_000));

    // RaspBatch SoA: 10k pointers stored as parallel u64/u32 vecs so
    // single `vmovdqu` loads pack 4 ptrs / bases / lengths / perms
    // into SIMD registers with zero GPR->SIMD domain crossings. This
    // is the sole RASP API; the previous AoS RaspPointer /
    // RaspWidePointer types have been replaced by this design after
    // bench data showed AoS+SIMD was 2-3x slower than scalar.
    let storages: Vec<Vec<u64>> = (0..10_000).map(|i| vec![i as u64]).collect();
    let mut soa: RaspBatch<u64> = RaspBatch::with_capacity(10_000);
    for s in &storages {
        soa.push_from_slice(s, RaspPermission::Read as u32).unwrap();
    }
    grp.bench_function("rasp_soa_count_valid_scalar", |b| {
        b.iter(|| {
            let ok = soa.count_valid_scalar();
            black_box(ok)
        });
    });
    if std::is_x86_feature_detected!("avx2") {
        // Wrapper marked #[target_feature(enable = "avx2")] so the
        // inlined SoA SIMD body folds into the call site without
        // paying the Windows x64 callee-saved-XMM prologue per call.
        #[target_feature(enable = "avx2")]
        unsafe fn run_soa_count_avx2(soa: &RaspBatch<u64>) -> u32 {
            // SAFETY: caller is #[target_feature(enable = "avx2")].
            unsafe { soa.count_valid_avx2() }
        }
        grp.bench_function("rasp_soa_count_valid_avx2", |b| {
            b.iter(|| {
                // SAFETY: avx2 feature-detected above.
                let ok = unsafe { run_soa_count_avx2(&soa) };
                black_box(ok)
            });
        });
    }
    if std::is_x86_feature_detected!("avx512f") {
        // Wrapper marked #[target_feature(enable = "avx512f")] so the
        // AVX-512 body folds into the call site. Only runs on hosts
        // with AVX-512F (e.g. EPYC Genoa); skipped on Zen+/Zen2/Zen3.
        #[target_feature(enable = "avx512f")]
        unsafe fn run_soa_count_avx512(soa: &RaspBatch<u64>) -> u32 {
            // SAFETY: caller is #[target_feature(enable = "avx512f")].
            unsafe { soa.count_valid_avx512() }
        }
        grp.bench_function("rasp_soa_count_valid_avx512", |b| {
            b.iter(|| {
                // SAFETY: avx512f feature-detected above.
                let ok = unsafe { run_soa_count_avx512(&soa) };
                black_box(ok)
            });
        });
    }
    drop(soa);

    // ReadableCapability: same workload.
    let read_caps: Vec<ReadableCapability<u64>> = storages.iter().map(|s| {
        let (c, _a) = ReadableCapability::from_slice(
            s.as_slice(), CapabilityPermission::Read as u32,
        );
        c
    }).collect();
    grp.bench_function("readable_capability_read", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for c in &read_caps {
                if let Ok(v) = c.read() { sum = sum.wrapping_add(v); }
            }
            black_box(sum)
        });
    });
    drop(read_caps);

    // Baseline: native slice bounds-check (what you'd write without capabilities).
    grp.bench_function("baseline_native_slice_check", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for s in &storages {
                if !s.is_empty() { sum = sum.wrapping_add(s[0]); }
            }
            black_box(sum)
        });
    });

    // WritableCapability: write each, read back, sum.
    let mut storages = storages;
    grp.bench_function("writable_capability_write_then_read", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for s in storages.iter_mut() {
                let (mut c, _a) = WritableCapability::from_slice_mut(s.as_mut_slice());
                c.write(42).expect("capability write");
                if let Ok(v) = c.read() { sum = sum.wrapping_add(v); }
            }
            black_box(sum)
        });
    });

    // Fair baseline for the writable path: same workload via direct
    // mutable slice access. Per rule 3b, the writable_capability
    // contender needs a native counterpart to isolate the capability
    // overhead from the underlying write+read cost.
    grp.bench_function("baseline_native_slice_write_then_read", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for s in storages.iter_mut() {
                if !s.is_empty() {
                    s[0] = 42;
                    sum = sum.wrapping_add(s[0]);
                }
            }
            black_box(sum)
        });
    });

    grp.finish();
}

// ============================================================================
// Group 2: Owned capability RAII construct + drop (1k cycles)
// ============================================================================

fn group_owned_capability_raii(c: &mut Criterion) {
    let mut grp = c.benchmark_group("owned_capability_construct_drop_1k");
    grp.throughput(Throughput::Elements(1_000));

    grp.bench_function("owned_readable_construct_then_drop", |b| {
        b.iter(|| {
            for i in 0..1_000u64 {
                let owned = OwnedReadableCapability::new(i);
                black_box(owned.read().expect("capability read"));
                // Dropped at end of iteration; RAII reclaims Box.
            }
        });
    });

    grp.bench_function("owned_writable_construct_write_drop", |b| {
        b.iter(|| {
            for i in 0..1_000u64 {
                let mut owned = OwnedWritableCapability::new(i);
                owned.cap_mut().write(i.wrapping_add(1)).expect("capability write");
                black_box(owned.read().expect("capability read"));
                // Dropped at end of iteration; RAII reclaims Box.
            }
        });
    });

    grp.bench_function("baseline_box_new_drop", |b| {
        b.iter(|| {
            for i in 0..1_000u64 {
                let b = Box::new(i);
                black_box(*b);
                // Standard Box drop.
            }
        });
    });

    grp.finish();
}

criterion_group!(benches, group_capability_validation, group_owned_capability_raii);
criterion_main!(benches);
