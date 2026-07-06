//! Bench: SharedUniversal<T> auto-migration win - contains() on a
//! Vec backing vs the same contains() after migrating to Map.
//!
//! Architectural claim: when contains-heavy workload triggers
//! migration Vec -> Map, the post-migration latency drops by a
//! factor proportional to N (O(N) scan → O(1) hash lookup). The
//! migration step itself costs O(N) per element copied, so it pays
//! off after ~N/3 contains calls.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedUniversal, UniversalStrategy as Strategy};

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-universal-{name}-{pid}"));
    p
}

fn cleanup(base: &std::path::Path) {
    let stem = base.file_name().unwrap().to_string_lossy().to_string();
    let parent = base.parent().unwrap_or_else(|| std::path::Path::new(""));
    if let Ok(entries) = std::fs::read_dir(parent) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(&stem) {
                // Best-effort cleanup; the file may already be gone.
                std::fs::remove_file(e.path()).ok();
            }
        }
    }
}

fn contains_vec_vs_map_n1k(c: &mut Criterion) {
    const N: u64 = 1024;
    let base = tmp_base("contains-n1k");
    let u: SharedUniversal<u64> = SharedUniversal::create(&base, N as usize).unwrap();
    for k in 0..N { u.insert(k).unwrap(); }
    // Vec mode: every contains scans N entries.
    c.bench_function("universal.contains/vec_n1k", |b| {
        b.iter(|| black_box(u.contains(black_box(&999u64)).unwrap()));
    });
    // Migrate to Map.
    u.migrate_to(Strategy::Map).unwrap();
    c.bench_function("universal.contains/map_n1k", |b| {
        b.iter(|| black_box(u.contains(black_box(&999u64)).unwrap()));
    });
    drop(u);
    cleanup(&base);
}

fn migration_cost_n1k(c: &mut Criterion) {
    // Cost of one Vec->Map migration of 1024 entries.
    const N: u64 = 1024;
    let base = tmp_base("migrate-n1k");
    let u: SharedUniversal<u64> = SharedUniversal::create(&base, N as usize).unwrap();
    for k in 0..N { u.insert(k).unwrap(); }
    let mut toggle = false;
    c.bench_function("universal.migrate_vec_to_map_n1k", |b| {
        b.iter(|| {
            let target = if toggle { Strategy::Vec } else { Strategy::Map };
            u.migrate_to(target).unwrap();
            toggle = !toggle;
        });
    });
    drop(u);
    cleanup(&base);
}

fn insert_vec_vs_map_n1k(c: &mut Criterion) {
    // Insert cost is similar on both backings; this confirms the
    // migration isn't a Pareto loss on insert-heavy workloads.
    const N: u64 = 1024;
    let base_v = tmp_base("ins-vec");
    let base_m = tmp_base("ins-map");
    let uv: SharedUniversal<u64> = SharedUniversal::create(&base_v, (N * 2) as usize).unwrap();
    let um: SharedUniversal<u64> = SharedUniversal::create(&base_m, (N * 2) as usize).unwrap();
    um.migrate_to(Strategy::Map).unwrap();
    // Each iter: clear() then insert N/4 distinct values. State
    // is reset per iter so the bench measures the same workload
    // every time, with .expect() catching any overflow.
    const PER_ITER: u64 = 256;  // N=1024, room for 4 iter's worth before re-clear
    c.bench_function("universal.insert/vec_n1k", |b| {
        b.iter_batched(
            || uv.clear().expect("vec clear"),
            |_| for k in 0..PER_ITER {
                uv.insert(black_box(k)).expect("vec insert");
            },
            criterion::BatchSize::PerIteration,
        );
    });
    c.bench_function("universal.insert/map_n1k", |b| {
        b.iter_batched(
            || um.clear().expect("map clear"),
            |_| for k in 0..PER_ITER {
                um.insert(black_box(k)).expect("map insert");
            },
            criterion::BatchSize::PerIteration,
        );
    });
    drop(uv);
    drop(um);
    cleanup(&base_v);
    cleanup(&base_m);
}

criterion_group!(benches,
    contains_vec_vs_map_n1k,
    migration_cost_n1k,
    insert_vec_vs_map_n1k,
);
criterion_main!(benches);
