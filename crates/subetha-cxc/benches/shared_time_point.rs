//! Bench: SharedTimePointTile<T> vs `Mutex<Vec<(u64, T)>>` linear-scan
//! baseline.
//!
//! Architectural claim: SIMD-shaped visibility scan over a 16-slot
//! tile at cache-line speed vs lock+linear scan baseline.
//!
//! Workloads:
//! - insert (iter_batched: fresh tile per iter)
//! - visible_mask (SIMD scan over 16 slots)
//! - visible_count
//! - at (resolve known lane)

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedTimePointTile, TILE_CAP};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-timepoint-{name}-{pid}.bin"));
    p
}

fn insert_hot(c: &mut Criterion) {
    let p = tmp("insert");
    c.bench_function("timepoint.insert/mmf", |b| {
        b.iter_batched(
            || {
                std::fs::remove_file(&p).ok();
                SharedTimePointTile::<u64>::create(&p).unwrap()
            },
            |t| {
                t.insert(black_box(42), black_box(4242)).expect("tile insert");
            },
            criterion::BatchSize::PerIteration,
        );
    });
    std::fs::remove_file(&p).ok();

    c.bench_function("timepoint.insert/mutex_vec", |b| {
        b.iter_batched(
            || Mutex::new(Vec::<(u64, u64)>::with_capacity(16)),
            |m| {
                m.lock().unwrap().push((black_box(42), black_box(4242)));
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

fn visible_mask(c: &mut Criterion) {
    let p = tmp("visible");
    let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
    for i in 0..16u64 { t.insert(i * 10, i).unwrap(); }
    c.bench_function("timepoint.visible_mask/mmf", |b| {
        b.iter(|| black_box(t.visible_mask(black_box(50))));
    });
    drop(t);
    std::fs::remove_file(&p).ok();

    let m: Mutex<Vec<(u64, u64)>> = Mutex::new((0..16u64).map(|i| (i * 10, i)).collect());
    c.bench_function("timepoint.visible_mask/mutex_vec_scan", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            let mut mask = 0u16;
            for (i, &(v, _)) in g.iter().enumerate() {
                if v <= 50 { mask |= 1 << i; }
            }
            black_box(mask)
        });
    });
}

fn visible_count(c: &mut Criterion) {
    let p = tmp("count");
    let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
    for i in 0..16u64 { t.insert(i * 10, i).unwrap(); }
    c.bench_function("timepoint.visible_count/mmf", |b| {
        b.iter(|| black_box(t.visible_count(black_box(80))));
    });
    drop(t);
    std::fs::remove_file(&p).ok();
}

fn at_lane(c: &mut Criterion) {
    let p = tmp("at");
    let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
    let lane = t.insert(7, 7000).unwrap();
    c.bench_function("timepoint.at/mmf", |b| {
        b.iter(|| black_box(t.at(black_box(lane))));
    });
    drop(t);
    std::fs::remove_file(&p).ok();
}

/// Isolate the three SIMD tiers of the visibility scan
/// (`simd_visible_mask_{scalar,avx2,avx512}`) so each is measured on
/// its own. On Zen+/Zen2/Zen3 only the scalar + AVX2 rows run; the
/// AVX-512 row runs on AVX-512F silicon (e.g. EPYC Genoa). The
/// `dispatched` row routes through the runtime CPUID picker, so it
/// reports whichever tier the host actually selects.
fn visible_mask_simd_tiers(c: &mut Criterion) {
    // 16 staggered versions, ~half visible at snapshot 50.
    let versions: [u64; TILE_CAP] = std::array::from_fn(|i| (i as u64) * 10);
    let snap = 50u64;

    c.bench_function("timepoint.simd_tiers/scalar", |b| {
        b.iter(|| {
            black_box(SharedTimePointTile::<u64>::simd_visible_mask_scalar(
                black_box(&versions),
                black_box(snap),
            ))
        });
    });
    c.bench_function("timepoint.simd_tiers/dispatched", |b| {
        b.iter(|| {
            black_box(SharedTimePointTile::<u64>::simd_visible_mask(
                black_box(&versions),
                black_box(snap),
            ))
        });
    });
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            #[target_feature(enable = "avx2")]
            unsafe fn run_avx2(v: &[u64; TILE_CAP], s: u64) -> u16 {
                // SAFETY: caller is #[target_feature(enable = "avx2")].
                unsafe { SharedTimePointTile::<u64>::simd_visible_mask_avx2(v, s) }
            }
            c.bench_function("timepoint.simd_tiers/avx2", |b| {
                // SAFETY: avx2 feature-detected above.
                b.iter(|| black_box(unsafe { run_avx2(black_box(&versions), black_box(snap)) }));
            });
        }
        if std::is_x86_feature_detected!("avx512f") {
            #[target_feature(enable = "avx512f")]
            unsafe fn run_avx512(v: &[u64; TILE_CAP], s: u64) -> u16 {
                // SAFETY: caller is #[target_feature(enable = "avx512f")].
                unsafe { SharedTimePointTile::<u64>::simd_visible_mask_avx512(v, s) }
            }
            c.bench_function("timepoint.simd_tiers/avx512", |b| {
                // SAFETY: avx512f feature-detected above.
                b.iter(|| black_box(unsafe { run_avx512(black_box(&versions), black_box(snap)) }));
            });
        }
    }
}

criterion_group!(
    benches,
    insert_hot,
    visible_mask,
    visible_count,
    at_lane,
    visible_mask_simd_tiers,
);
criterion_main!(benches);
