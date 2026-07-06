//! Bench: SharedNaNTaggedValue vs `Box<dyn Trait>` polymorphism -
//! the textbook in-process pattern for heterogeneous values with
//! typed-pointer variants.
//!
//! Architectural claim: two-level discrimination (outer NaN tag +
//! inner pointer tag) fits in 8 bytes with zero heap alloc. The
//! Box<dyn Trait> baseline costs ~16 bytes (fat pointer = data
//! pointer + vtable pointer) PLUS one heap allocation per value.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{SharedNaNTaggedValue, TaggedOffsetPtr};

// Box<dyn Trait> baseline: every value is heap-allocated, accessed
// through a vtable dispatch.
trait Value: std::fmt::Debug {
    fn kind(&self) -> u8;
}

#[derive(Debug)]
struct LeafNode { _key: u64, _value: u64 }
impl Value for LeafNode { fn kind(&self) -> u8 { 0 } }

#[derive(Debug)]
struct InternalNode { _key: u64, _value: u64 }
impl Value for InternalNode { fn kind(&self) -> u8 { 1 } }

#[derive(Debug)]
struct Tombstone;
impl Value for Tombstone { fn kind(&self) -> u8 { 2 } }

// =========================================================
// construct
// =========================================================

fn construct(c: &mut Criterion) {
    c.bench_function("nan_tagged.construct_tagged_ptr/mmf", |b| {
        b.iter(|| {
            let p: TaggedOffsetPtr<u64, 2> = TaggedOffsetPtr::new(
                black_box(42), black_box(1),
            );
            black_box(SharedNaNTaggedValue::from_tagged_offset_ptr(p))
        });
    });

    c.bench_function("nan_tagged.construct_box_dyn/box_dyn", |b| {
        b.iter(|| {
            let v: Box<dyn Value> = Box::new(InternalNode {
                _key: black_box(42), _value: black_box(100),
            });
            black_box(v)
        });
    });
}

// =========================================================
// extract + dispatch on kind
// =========================================================

fn extract_kind(c: &mut Criterion) {
    let p: TaggedOffsetPtr<u64, 2> = TaggedOffsetPtr::new(42, 1);
    let v_mmf = SharedNaNTaggedValue::from_tagged_offset_ptr(p);
    c.bench_function("nan_tagged.extract_kind/mmf", |b| {
        b.iter(|| {
            let extracted: TaggedOffsetPtr<u64, 2> =
                black_box(v_mmf).as_tagged_offset_ptr().unwrap();
            black_box(extracted.tag())
        });
    });

    let v_box: Box<dyn Value> = Box::new(InternalNode { _key: 42, _value: 100 });
    c.bench_function("nan_tagged.extract_kind/box_dyn", |b| {
        b.iter(|| black_box(black_box(&v_box).kind()));
    });
}

// =========================================================
// size comparison
// =========================================================

fn size_comparison(c: &mut Criterion) {
    let nv_size = std::mem::size_of::<SharedNaNTaggedValue>();
    let box_dyn_size = std::mem::size_of::<Box<dyn Value>>();
    let heap_alloc_size = std::mem::size_of::<InternalNode>();
    let total_box_size = box_dyn_size + heap_alloc_size;
    eprintln!("[storage] SharedNaNTaggedValue = {nv_size} bytes (inline)");
    eprintln!("[storage] Box<dyn Value>       = {box_dyn_size} bytes fat ptr + {heap_alloc_size} bytes heap = {total_box_size} bytes total ({}x larger)",
        total_box_size as f64 / nv_size as f64);
    c.bench_function("nan_tagged.size_witness", |b| {
        b.iter(|| black_box(nv_size + total_box_size));
    });
}

// =========================================================
// Batch: build N heterogeneous values, count internal nodes.
// =========================================================

fn batch_build_and_count(c: &mut Criterion) {
    const N: u32 = 1024;

    c.bench_function("nan_tagged.batch_build_count_1024/mmf", |b| {
        b.iter(|| {
            let arr: Vec<SharedNaNTaggedValue> = (0..N).map(|i| {
                let kind = i % 3;
                let p: TaggedOffsetPtr<u64, 2> = TaggedOffsetPtr::new(i, kind);
                SharedNaNTaggedValue::from_tagged_offset_ptr(p)
            }).collect();
            // Count tag=1 (Internal).
            let count = arr.iter()
                .filter_map(|v| v.as_tagged_offset_ptr::<u64, 2>())
                .filter(|p| p.tag() == 1)
                .count();
            black_box(count)
        });
    });

    c.bench_function("nan_tagged.batch_build_count_1024/box_dyn", |b| {
        b.iter(|| {
            let arr: Vec<Box<dyn Value>> = (0..N).map(|i| {
                let kind = i % 3;
                if kind == 0 {
                    Box::new(LeafNode { _key: i as u64, _value: 0 }) as Box<dyn Value>
                } else if kind == 1 {
                    Box::new(InternalNode { _key: i as u64, _value: 0 }) as Box<dyn Value>
                } else {
                    Box::new(Tombstone) as Box<dyn Value>
                }
            }).collect();
            let count = arr.iter().filter(|v| v.kind() == 1).count();
            black_box(count)
        });
    });
}

criterion_group!(benches,
    construct,
    extract_kind,
    size_comparison,
    batch_build_and_count,
);
criterion_main!(benches);
