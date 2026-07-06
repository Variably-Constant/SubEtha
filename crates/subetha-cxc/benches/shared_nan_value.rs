//! Bench: SharedNaNValue vs Rust enum-as-tagged-union.
//!
//! Architectural claim: NaN boxing packs heterogeneous values into
//! 8 bytes vs Rust's natural enum layout which pads to 16 bytes
//! (1-byte discriminant + padding to align the f64 variant). The
//! storage density doubles, and per-op extraction is comparable.
//! The win compounds when stored in containers (Vec, HashMap, MMF
//! slots) where the per-element bytes matter.
//!
//! Workloads:
//! - construct each variant
//! - extract correct variant
//! - size_of comparison
//! - batch sum over Vec<value> (the storage-density payoff)

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedNaNValue;

// Rust enum baseline: natural-layout tagged union. Variants U32 and
// OffsetPtr are kept for layout-size parity with SharedNaNValue's
// full variant set, even when the bench doesn't construct them.
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)]
enum EnumValue {
    F64(f64),
    Nil,
    I32(i32),
    U32(u32),
    Bool(bool),
    OffsetPtr(u32),
}

impl EnumValue {
    fn as_i32(self) -> Option<i32> {
        if let EnumValue::I32(v) = self { Some(v) } else { None }
    }
    fn as_f64(self) -> Option<f64> {
        if let EnumValue::F64(v) = self { Some(v) } else { None }
    }
}

// =========================================================
// construct
// =========================================================

fn construct(c: &mut Criterion) {
    c.bench_function("nan_value.construct_i32/mmf", |b| {
        b.iter(|| black_box(SharedNaNValue::from_i32(black_box(42))));
    });
    c.bench_function("nan_value.construct_i32/enum", |b| {
        b.iter(|| black_box(EnumValue::I32(black_box(42))));
    });

    c.bench_function("nan_value.construct_f64/mmf", |b| {
        b.iter(|| black_box(SharedNaNValue::from_f64(black_box(2.5))));
    });
    c.bench_function("nan_value.construct_f64/enum", |b| {
        b.iter(|| black_box(EnumValue::F64(black_box(2.5))));
    });
}

// =========================================================
// extract
// =========================================================

fn extract(c: &mut Criterion) {
    let nv_i32 = SharedNaNValue::from_i32(42);
    c.bench_function("nan_value.extract_i32/mmf", |b| {
        b.iter(|| black_box(black_box(nv_i32).as_i32()));
    });
    let ev_i32 = EnumValue::I32(42);
    c.bench_function("nan_value.extract_i32/enum", |b| {
        b.iter(|| black_box(black_box(ev_i32).as_i32()));
    });

    let nv_f64 = SharedNaNValue::from_f64(2.5);
    c.bench_function("nan_value.extract_f64/mmf", |b| {
        b.iter(|| black_box(black_box(nv_f64).as_f64()));
    });
    let ev_f64 = EnumValue::F64(2.5);
    c.bench_function("nan_value.extract_f64/enum", |b| {
        b.iter(|| black_box(black_box(ev_f64).as_f64()));
    });
}

// =========================================================
// size comparison
// =========================================================

fn size_comparison(c: &mut Criterion) {
    let mmf_size = std::mem::size_of::<SharedNaNValue>();
    let enum_size = std::mem::size_of::<EnumValue>();
    eprintln!("[storage] SharedNaNValue = {mmf_size} bytes");
    eprintln!("[storage] EnumValue (Rust enum) = {enum_size} bytes ({}x larger)",
        enum_size as f64 / mmf_size as f64);
    c.bench_function("nan_value.size_witness", |b| {
        b.iter(|| black_box(mmf_size + enum_size));
    });
}

// =========================================================
// Batch sum over heterogeneous values: pick i32 entries and
// accumulate their values. This is where storage density wins.
// =========================================================

fn batch_sum_i32_filter(c: &mut Criterion) {
    const N: usize = 4096;

    let mmf_arr: Vec<SharedNaNValue> = (0..N)
        .map(|i| if i % 4 == 0 { SharedNaNValue::from_i32(i as i32) }
                 else if i % 4 == 1 { SharedNaNValue::from_f64(i as f64) }
                 else if i % 4 == 2 { SharedNaNValue::from_bool(i & 1 == 0) }
                 else { SharedNaNValue::NIL })
        .collect();
    c.bench_function("nan_value.batch_sum_i32_4096/mmf", |b| {
        b.iter(|| {
            let sum: i64 = mmf_arr.iter()
                .filter_map(|v| v.as_i32())
                .map(|v| v as i64)
                .sum();
            black_box(sum)
        });
    });

    let enum_arr: Vec<EnumValue> = (0..N)
        .map(|i| if i % 4 == 0 { EnumValue::I32(i as i32) }
                 else if i % 4 == 1 { EnumValue::F64(i as f64) }
                 else if i % 4 == 2 { EnumValue::Bool(i & 1 == 0) }
                 else { EnumValue::Nil })
        .collect();
    c.bench_function("nan_value.batch_sum_i32_4096/enum", |b| {
        b.iter(|| {
            let sum: i64 = enum_arr.iter()
                .filter_map(|v| v.as_i32())
                .map(|v| v as i64)
                .sum();
            black_box(sum)
        });
    });
}

criterion_group!(benches,
    construct,
    extract,
    size_comparison,
    batch_sum_i32_filter,
);
criterion_main!(benches);
