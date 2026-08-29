//! Bench: `SharedArc<T>` vs `std::sync::Arc<T>`.
//!
//! Architectural claim: the same shape as `Arc` across a process
//! boundary, where `Arc` cannot go at all. The cost is that taking a
//! hold is a slot claim in a mapping rather than an increment of a
//! word the allocator handed out.
//!
//! Fairness, audited against the three questions the house rule asks:
//!
//! 1. Both arms take a hold and drop it. `Arc::clone` plus drop is the
//!    whole of what `Arc` does; `SharedArc::open` plus drop maps the
//!    file, validates the header and claims a slot.
//! 2. The open arm's cost is dominated by the mmap, not by the slot
//!    claim, and saying so is the point rather than a caveat - so the
//!    slot claim is ALSO measured on its own through `holders()`, and
//!    both numbers are reported.
//! 3. Reading the value is measured separately from taking a hold,
//!    because a caller opens once and reads many times.
//!
//! What the numbers do NOT show: `Arc` cannot address a value in
//! another process at any price, and its count cannot survive the
//! death of a holder. Those are the reasons to pay the difference.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{LastHolder, SharedArc};

const HOLDERS: usize = 64;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("subetha-bench-arc-{name}-{}.bin", std::process::id()));
    p
}

/// Taking a hold and dropping it, the whole operation each side offers.
fn hold_release(c: &mut Criterion) {
    let p = tmp("hold");
    let root = SharedArc::create(&p, 42u64, HOLDERS, LastHolder::Keep).unwrap();
    c.bench_function("arc.hold_release/shared_open", |b| {
        b.iter(|| {
            let h = SharedArc::<u64>::open(&p, HOLDERS, LastHolder::Keep).unwrap();
            black_box(*h);
        });
    });

    // The slot claim alone, separated from the mmap that dominates an
    // open, because they are different costs and only one is the
    // primitive's own.
    let holders = root.holders();
    c.bench_function("arc.hold_release/shared_slot_only", |b| {
        b.iter(|| {
            let s = holders.claim(black_box(1)).unwrap();
            holders.release(s);
        });
    });
    drop(root);
    std::fs::remove_file(&p).ok();

    let a = Arc::new(42u64);
    c.bench_function("arc.hold_release/std_arc_clone", |b| {
        b.iter(|| {
            let h = Arc::clone(&a);
            black_box(*h);
        });
    });
}

/// Reading the value through an already-held handle, which is what a
/// caller does repeatedly after opening once.
fn read_value(c: &mut Criterion) {
    let p = tmp("read");
    let s = SharedArc::create(&p, 42u64, HOLDERS, LastHolder::Keep).unwrap();
    c.bench_function("arc.read/shared_deref", |b| {
        b.iter(|| black_box(*s));
    });
    drop(s);
    std::fs::remove_file(&p).ok();

    let a = Arc::new(42u64);
    c.bench_function("arc.read/std_arc_deref", |b| {
        b.iter(|| black_box(*a));
    });
}

/// How many holders there are: a slot scan against `Arc::strong_count`,
/// which is one load.
fn strong_count(c: &mut Criterion) {
    let p = tmp("count");
    let s = SharedArc::create(&p, 42u64, HOLDERS, LastHolder::Keep).unwrap();
    c.bench_function("arc.strong_count/shared", |b| {
        b.iter(|| black_box(s.strong_count()));
    });
    drop(s);
    std::fs::remove_file(&p).ok();

    let a = Arc::new(42u64);
    let _b = Arc::clone(&a);
    c.bench_function("arc.strong_count/std_arc", |b| {
        b.iter(|| black_box(Arc::strong_count(&a)));
    });
}

criterion_group!(benches, hold_release, read_value, strong_count);
criterion_main!(benches);
