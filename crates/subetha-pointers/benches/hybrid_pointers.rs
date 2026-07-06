//! Bench: KTower2 region-table pointer resolve cost vs a native
//! (region_id, offset) struct and a pre-resolved direct pointer.
//!
//! Only the KTower2 group of the upstream hybrid-pointers bench is
//! ported here; the other upstream groups (CompactQueryPtr, GraphPtr,
//! TieredAddr, TimePointTile) cover types that are not part of this
//! crate.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_pointers::k_tower_pointer::KTower2;

// =========================================================
// KTower2 resolve cost vs native (region_id, offset) struct
// vs pre-resolved direct pointer.
//
// AUDIT (rule 3b): comparing KTower's 1 indirection (table lookup +
// offset add + deref) against a pre-resolved *const u64 (zero
// indirection) is a surplus-indirection asymmetry. The fair
// contender is a native (region_id: u32, offset: u32) tuple doing
// the same table lookup + offset add + deref. KTower's storage is
// 8 B (packed u64); the native struct is also 8 B with the same
// layout - so the bench isolates the KTower API cost vs the
// raw-encoding cost. The 'direct_ptr' contender is the absolute
// floor (pre-resolved access, the alternative when cross-process
// portability is NOT needed).
// =========================================================

fn ktower_resolve(c: &mut Criterion) {
    let regions: Vec<Vec<u64>> = (0..8).map(|r| {
        (0..1024u64).map(|i| r * 1000 + i).collect()
    }).collect();
    let table: Vec<*const u8> = regions.iter()
        .map(|r| r.as_ptr() as *const u8).collect();
    // 1024 KTower2 pointers across the 8 regions.
    let ptrs: Vec<KTower2<u64>> = (0..1024u32).map(|i| {
        let region = i % 8;
        let offset = (i / 8) * 8;
        KTower2::new(region, offset)
    }).collect();
    // Native (region_id, offset) struct: same data layout but
    // without the KTower API. Does the same table lookup +
    // offset add + deref.
    let native: Vec<(u32, u32)> = ptrs.iter()
        .map(|p| (p.region_id(), p.offset()))
        .collect();

    c.bench_function("hybrid.ktower/resolve_1024", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for p in ptrs.iter() {
                let r = unsafe { *p.resolve(black_box(&table)) };
                sum = sum.wrapping_add(r);
            }
            black_box(sum)
        });
    });
    c.bench_function("hybrid.ktower/native_struct_resolve_1024", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for &(rid, off) in native.iter() {
                let base = black_box(&table)[rid as usize];
                let p = unsafe { base.add(off as usize) as *const u64 };
                sum = sum.wrapping_add(unsafe { *p });
            }
            black_box(sum)
        });
    });
    // Pre-resolved direct pointer: zero indirection. The cost
    // floor for "you already paid the table lookup once".
    let direct: Vec<*const u64> = ptrs.iter().map(|p| {
        unsafe { p.resolve(&table) }
    }).collect();
    c.bench_function("hybrid.ktower/direct_ptr_1024", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for p in direct.iter() {
                sum = sum.wrapping_add(unsafe { **p });
            }
            black_box(sum)
        });
    });
}

criterion_group!(benches, ktower_resolve);
criterion_main!(benches);
