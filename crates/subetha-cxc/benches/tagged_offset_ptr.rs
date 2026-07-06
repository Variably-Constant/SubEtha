//! Bench: TaggedOffsetPtr<T, 4> vs (OffsetPtr<T>, u8) tuple - the
//! textbook "just store the tag in a separate field" baseline.
//!
//! Architectural claim: storing the tag inline via high-bit stealing
//! is comparable to or faster than storing it as a separate field,
//! AND uses half the memory (4 bytes vs 8 bytes after Rust padding).
//! The win matters when arrays of pointer+tag pairs are stored
//! densely (graph nodes, tree children).
//!
//! Workloads:
//! - construct (pack tag + index into one word vs build a tuple)
//! - extract index (read low bits vs read tuple field)
//! - extract tag (read high bits vs read tuple field)
//! - storage density: array of N tagged ptrs measured by bytes
//! - SIMD-friendly batch scan: count entries with a specific tag

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{OffsetPtr, TaggedOffsetPtr};

// =========================================================
// Construct
// =========================================================

fn construct(c: &mut Criterion) {
    c.bench_function("tagged_ptr.construct/mmf", |b| {
        b.iter(|| {
            let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(
                black_box(42), black_box(7),
            );
            black_box(p)
        });
    });

    c.bench_function("tagged_ptr.construct/tuple", |b| {
        b.iter(|| {
            let p: (OffsetPtr<u64>, u8) = (
                OffsetPtr::new(black_box(42)), black_box(7),
            );
            black_box(p)
        });
    });
}

// =========================================================
// Extract index
// =========================================================

fn extract_index(c: &mut Criterion) {
    let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(42, 7);
    c.bench_function("tagged_ptr.index/mmf", |b| {
        b.iter(|| black_box(black_box(p).index()));
    });

    let t: (OffsetPtr<u64>, u8) = (OffsetPtr::new(42), 7);
    c.bench_function("tagged_ptr.index/tuple", |b| {
        b.iter(|| black_box(black_box(t).0.index));
    });
}

// =========================================================
// Extract tag
// =========================================================

fn extract_tag(c: &mut Criterion) {
    let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(42, 7);
    c.bench_function("tagged_ptr.tag/mmf", |b| {
        b.iter(|| black_box(black_box(p).tag()));
    });

    let t: (OffsetPtr<u64>, u8) = (OffsetPtr::new(42), 7);
    c.bench_function("tagged_ptr.tag/tuple", |b| {
        b.iter(|| black_box(black_box(t).1));
    });
}

// =========================================================
// SIMD-friendly batch: count entries with a specific tag.
// This is where high-bit stealing wins: the entire array is u32,
// SIMD can mask+compare in parallel. The tuple version needs to
// iterate struct fields.
// =========================================================

fn count_with_tag(c: &mut Criterion) {
    const N: usize = 1024;
    let target_tag = 3u32;
    let arr: Vec<TaggedOffsetPtr<u64, 4>> = (0..N)
        .map(|i| TaggedOffsetPtr::new(i as u32, (i as u32) & 0xF))
        .collect();
    c.bench_function("tagged_ptr.count_tag_1024/mmf", |b| {
        b.iter(|| {
            let count = arr.iter()
                .filter(|p| p.tag() == target_tag)
                .count();
            black_box(count)
        });
    });

    let target_u8 = target_tag as u8;
    let tuple_arr: Vec<(OffsetPtr<u64>, u8)> = (0..N)
        .map(|i| (OffsetPtr::new(i as u32), (i as u32 & 0xF) as u8))
        .collect();
    c.bench_function("tagged_ptr.count_tag_1024/tuple", |b| {
        b.iter(|| {
            let count = tuple_arr.iter()
                .filter(|t| t.1 == target_u8)
                .count();
            black_box(count)
        });
    });
}

// =========================================================
// Storage density: just report sizes (not really a bench but a
// witness of the architectural claim).
// =========================================================

fn storage_density(c: &mut Criterion) {
    let mmf_size = std::mem::size_of::<TaggedOffsetPtr<u64, 4>>();
    let tuple_size = std::mem::size_of::<(OffsetPtr<u64>, u8)>();
    eprintln!("[storage] TaggedOffsetPtr<T, 4> = {mmf_size} bytes");
    eprintln!("[storage] (OffsetPtr<T>, u8)    = {tuple_size} bytes ({}x larger)",
        tuple_size as f64 / mmf_size as f64);
    // Bench a no-op to keep Criterion happy.
    c.bench_function("tagged_ptr.storage_witness", |b| {
        b.iter(|| black_box(mmf_size + tuple_size));
    });
}

criterion_group!(benches,
    construct,
    extract_index,
    extract_tag,
    count_with_tag,
    storage_density,
);
criterion_main!(benches);
