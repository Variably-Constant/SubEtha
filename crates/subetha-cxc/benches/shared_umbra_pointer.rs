//! Bench: SharedUmbraPointer<T> prefix-shortcircuit scan vs naive
//! resolve-every-target scan.
//!
//! Architectural claim: scanning a 100k-entry Vec<SharedUmbraPointer<T>>
//! looking for a rare matching prefix should be dominated by the
//! in-register prefix check (no region MMF reads except on the few
//! actual matches). The naive baseline that resolves every target's
//! value first then compares pays one MMF read per entry - that's
//! the cache miss the prefix is designed to skip.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedRegion, SharedUmbraPointer};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-umbra-{name}-{pid}.bin"));
    p
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Wide {
    key: u32,
    pad: [u32; 7],
}

fn build_n(n: usize) -> (std::path::PathBuf, SharedRegion<Wide>, Vec<SharedUmbraPointer<Wide>>) {
    let p = tmp(&format!("n-{n}"));
    let region: SharedRegion<Wide> = SharedRegion::create(&p, n).unwrap();
    let pointers: Vec<SharedUmbraPointer<Wide>> = (0..n as u32)
        .map(|i| SharedUmbraPointer::from_region_alloc(
            &region, Wide { key: i, pad: [0; 7] }, i,
        ).unwrap())
        .collect();
    (p, region, pointers)
}

fn prefix_scan_no_match(c: &mut Criterion) {
    // 10k entries, query for a prefix that does NOT match any.
    const N: usize = 10_000;
    let (path, region, pointers) = build_n(N);

    c.bench_function("umbra.prefix_scan_no_match/in_register", |b| {
        let q = u32::MAX;  // no prefix in 0..N equals u32::MAX
        b.iter(|| {
            let count = pointers.iter()
                .filter(|p| p.matches_prefix(black_box(q)))
                .count();
            black_box(count)
        });
    });

    c.bench_function("umbra.prefix_scan_no_match/resolve_then_compare", |b| {
        // Naive baseline: resolve every target's value via the region
        // MMF, then compare the value's key field.
        let q = u32::MAX;
        b.iter(|| {
            let count = pointers.iter()
                .filter(|p| {
                    let w = p.resolve(&region).unwrap();
                    w.key == black_box(q)
                })
                .count();
            black_box(count)
        });
    });

    drop(region);
    std::fs::remove_file(&path).ok();
}

fn prefix_scan_rare_match(c: &mut Criterion) {
    // 10k entries, query for a prefix that matches exactly one entry.
    const N: usize = 10_000;
    let (path, region, pointers) = build_n(N);

    c.bench_function("umbra.prefix_scan_rare_match/in_register", |b| {
        let q = 4242u32;  // matches exactly i=4242
        b.iter(|| {
            let mut hits = 0;
            for p in &pointers {
                if p.matches_prefix(black_box(q)) {
                    // Only on prefix match do we resolve.
                    black_box(p.resolve(&region).unwrap());
                    hits += 1;
                }
            }
            black_box(hits)
        });
    });

    c.bench_function("umbra.prefix_scan_rare_match/resolve_every", |b| {
        let q = 4242u32;
        b.iter(|| {
            let mut hits = 0;
            for p in &pointers {
                let w = p.resolve(&region).unwrap();
                if w.key == black_box(q) {
                    hits += 1;
                }
            }
            black_box(hits)
        });
    });

    drop(region);
    std::fs::remove_file(&path).ok();
}

fn construction_overhead(c: &mut Criterion) {
    let p = tmp("ctor");
    let region: SharedRegion<u64> = SharedRegion::create(&p, 1 << 16).unwrap();
    let mut next: u64 = 0;

    c.bench_function("umbra.from_region_alloc_content_prefix/mmf", |b| {
        b.iter(|| {
            let u = SharedUmbraPointer::from_region_alloc_content_prefix(
                &region, black_box(next),
            );
            next = next.wrapping_add(1);
            match u {
                Ok(ptr) => { black_box(ptr); }
                // Region exhausted: bench cycle done, leave the
                // result so criterion measures the populated case.
                Err(e) => { black_box(e); }
            }
        });
    });

    drop(region);
    std::fs::remove_file(&p).ok();
}

criterion_group!(benches,
    prefix_scan_no_match,
    prefix_scan_rare_match,
    construction_overhead,
);
criterion_main!(benches);
