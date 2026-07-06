//! Bench: KTowerCascade resolution vs Mutex<HashMap<u64, T>>
//! lookup on sparse address spaces.
//!
//! Architectural claim: for SPARSE address spaces, the
//! KTowerCascade walks at most DEPTH cache-line reads (one per
//! region per level), with cache-friendly access patterns because
//! the regions are MMF-aligned flat arrays. HashMap pays one hash
//! computation + one bucket lookup + Mutex lock/unlock per access,
//! with cache misses scaling with bucket size.
//!
//! The cascade also gives O(1) memory per empty branch (just the
//! empty intermediate region slot), where HashMap can't represent
//! "an entire address subspace is empty" - it has no notion of
//! sparse address structure.
//!
//! Workloads:
//! - cascade construction (insert)
//! - cascade resolution (get)
//! - storage density: bytes per logical key in a populated cascade
//! - position-independence witness (size of cascade vs raw pointer)

use std::cell::RefCell;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{CascadeResolverN, KTowerCascade};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-cascade-{name}-{pid}.bin"));
    p
}

// =========================================================
// insert
// =========================================================

// Large-scale amortized insert.
//
// Cascade pre-sized at 1<<28 = 268M slots so criterion's full
// warmup + measurement (~33M iters at ~52 ns per op) never
// overflows. Hashmap sized at 1<<20 = 1M with_capacity so its
// memory-access pattern is realistic: it grows from 1M and
// rehashes ~7-8 times as inserts accumulate, paying the actual
// cost of using a hashmap at criterion's iter scale. The 1<<20
// sizing captures the rehash cost as part of the workload, which
// IS the honest cost of "insert many into a hashmap." Cascade
// disk footprint during the bench: ~2.1 GB (268M slots * 4 bytes
// * 2 regions). Cleaned up after.
fn insert_hot_presized(c: &mut Criterion) {
    let lp = tmp("ins-leaf-presized");
    let i0 = tmp("ins-i0-presized");
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&i0).ok();
    const CAP: usize = 1 << 28;
    // RefCell so the closure can swap the resolver when capacity is
    // exhausted. Criterion picks per-bench iter counts from the host's
    // measured throughput, and on fast silicon (Zen 4, Sapphire-Rapids
    // class) the warm-up + measurement window crosses `CAP` appends
    // mid-bench. The recreate cost amortises across `CAP` appends, so
    // per-iter timing approaches the pure append cost (recreate adds
    // a few ms per ~270M appends = under 1% overhead at CAP=1<<28).
    let r = RefCell::new(
        CascadeResolverN::<u64, 2>::create(
            &lp, CAP, vec![(i0.clone(), CAP)],
        ).unwrap(),
    );
    c.bench_function("cascade.insert_presized/mmf", |b| {
        b.iter(|| {
            if r.borrow().append(black_box(42)).is_err() {
                // Cascade full. Delete files, recreate in place, re-do
                // the append against the fresh resolver. The Ref from
                // `r.borrow()` was already dropped at the end of the
                // `if` condition, so the upcoming `borrow_mut()` does
                // not collide.
                std::fs::remove_file(&lp).ok();
                std::fs::remove_file(&i0).ok();
                *r.borrow_mut() = CascadeResolverN::<u64, 2>::create(
                    &lp, CAP, vec![(i0.clone(), CAP)],
                ).unwrap();
                r.borrow().append(black_box(42)).expect("fresh cascade rejected append");
            }
            black_box(())
        });
    });
    drop(r);
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&i0).ok();

    let h: Mutex<HashMap<u64, u64>> = Mutex::new(HashMap::with_capacity(1 << 20));
    let mut k = 0u64;
    c.bench_function("cascade.insert_presized/mutex_hashmap", |b| {
        b.iter(|| {
            k = k.wrapping_add(1);
            h.lock().unwrap().insert(black_box(k), black_box(42))
        });
    });
}

// Per-batch insert with setup.
//
// iter_batched(PerIteration) on both contenders so neither fills
// across criterion's iters. Symmetric setup cost (resolver
// create + region init / hashmap allocate). Per-batch inserts:
// 256 cascades / 256 hashmap entries. Absolute numbers include
// the per-batch setup but the ratio comparison is fair and the
// disk footprint stays under 1 MB.
fn insert_hot_batched(c: &mut Criterion) {
    const N: u64 = 256;
    let lp = tmp("ins-leaf-batched");
    let i0 = tmp("ins-i0-batched");
    c.bench_function("cascade.insert_batched_256/mmf", |b| {
        b.iter_batched(
            || {
                std::fs::remove_file(&lp).ok();
                std::fs::remove_file(&i0).ok();
                CascadeResolverN::<u64, 2>::create(
                    &lp, N as usize * 2, vec![(i0.clone(), N as usize * 2)],
                ).unwrap()
            },
            |r| {
                for i in 0..N {
                    r.append(black_box(i)).expect("cascade overflow");
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&i0).ok();

    c.bench_function("cascade.insert_batched_256/mutex_hashmap", |b| {
        b.iter_batched(
            || Mutex::new(HashMap::<u64, u64>::with_capacity(N as usize * 2)),
            |h| {
                for i in 0..N {
                    h.lock().unwrap().insert(black_box(i), 42);
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

// =========================================================
// get
// =========================================================

fn get_hot(c: &mut Criterion) {
    let lp = tmp("get-leaf");
    let i0 = tmp("get-i0");
    let r: CascadeResolverN<u64, 2> = CascadeResolverN::create(
        &lp, 1024, vec![(i0.clone(), 1024)],
    ).unwrap();
    let cascade = r.append(0xDEAD_BEEF).unwrap();
    c.bench_function("cascade.get/mmf_depth2", |b| {
        b.iter(|| black_box(r.get(black_box(cascade)).unwrap()));
    });
    drop(r);
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&i0).ok();

    // Depth-4 cascade.
    let lp4 = tmp("get-leaf-4");
    let i0_4 = tmp("get-i0-4");
    let i1_4 = tmp("get-i1-4");
    let i2_4 = tmp("get-i2-4");
    let r4: CascadeResolverN<u64, 4> = CascadeResolverN::create(
        &lp4, 1024,
        vec![(i0_4.clone(), 1024), (i1_4.clone(), 1024), (i2_4.clone(), 1024)],
    ).unwrap();
    let cascade4 = r4.append(0xDEAD_BEEF).unwrap();
    c.bench_function("cascade.get/mmf_depth4", |b| {
        b.iter(|| black_box(r4.get(black_box(cascade4)).unwrap()));
    });
    drop(r4);
    for p in [&lp4, &i0_4, &i1_4, &i2_4] { std::fs::remove_file(p).ok(); }

    // Pre-populate the hashmap to 1024 entries so the lookup hits
    // a representative working-set size matching the cascade's
    // 1024-slot regions.
    let h: Mutex<HashMap<u64, u64>> = Mutex::new({
        let mut m = HashMap::with_capacity(1024);
        for i in 0..1024u64 { m.insert(i, 0xDEAD_BEEF); }
        m
    });
    c.bench_function("cascade.get/mutex_hashmap", |b| {
        b.iter(|| {
            let g = h.lock().unwrap();
            black_box(g.get(&black_box(42u64)).copied())
        });
    });
}

// =========================================================
// storage density: bytes per logical key
// =========================================================

fn storage_density(c: &mut Criterion) {
    let cascade2 = std::mem::size_of::<KTowerCascade<u64, 2>>();
    let cascade4 = std::mem::size_of::<KTowerCascade<u64, 4>>();
    let raw_ptr = std::mem::size_of::<*const u64>();
    eprintln!("[storage] *const u64                 = {raw_ptr} bytes");
    eprintln!("[storage] KTowerCascade<u64, 2>      = {cascade2} bytes ({}x raw ptr)",
        cascade2 as f64 / raw_ptr as f64);
    eprintln!("[storage] KTowerCascade<u64, 4>      = {cascade4} bytes ({}x raw ptr)",
        cascade4 as f64 / raw_ptr as f64);
    eprintln!("[storage] (cascades trade native-ptr size for cross-process position independence)");
    c.bench_function("cascade.size_witness", |b| {
        b.iter(|| black_box(cascade2 + cascade4 + raw_ptr));
    });
}

// =========================================================
// sparse-address-space benchmark: insert 1000 entries with widely
// spaced top indices vs HashMap with the same keys.
// =========================================================

fn sparse_insertion(c: &mut Criterion) {
    const N: u64 = 100;

    // iter_batched(PerIteration) on BOTH contenders so neither
    // fills across criterion's iters. Symmetric setup cost
    // (resolver create / hashmap allocate). The .expect surfaces
    // any sizing mistake loudly.
    let lp = tmp("sparse-leaf");
    let i0 = tmp("sparse-i0");
    c.bench_function("cascade.sparse_insert_100/mmf", |b| {
        b.iter_batched(
            || {
                std::fs::remove_file(&lp).ok();
                std::fs::remove_file(&i0).ok();
                CascadeResolverN::<u64, 2>::create(
                    &lp, 256, vec![(i0.clone(), 256)],
                ).unwrap()
            },
            |r| {
                for _ in 0..N {
                    r.append(0u64).expect("cascade overflow");
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&i0).ok();

    c.bench_function("cascade.sparse_insert_100/mutex_hashmap", |b| {
        b.iter_batched(
            || Mutex::new(HashMap::<u64, u64>::with_capacity(N as usize * 2)),
            |h| {
                for k in 0..N {
                    // Spread keys across u64 address space.
                    let key = k.wrapping_mul(0x100_0000_0001);
                    h.lock().unwrap().insert(black_box(key), 0);
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

criterion_group!(benches,
    insert_hot_presized,
    insert_hot_batched,
    get_hot,
    storage_density,
    sparse_insertion,
);
criterion_main!(benches);
