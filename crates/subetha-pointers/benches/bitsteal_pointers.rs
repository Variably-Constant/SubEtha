//! Bench: bit-steal pointers (CardinalityPointer, KStepPointer,
//! SelfDescPointer) against the naive alternatives they replace.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_pointers::cardinality_pointer::{CardinalityPointer, SizeTier};
use subetha_pointers::kstep_pointer::KStepPointer;
use subetha_pointers::self_desc_pointer::{LayoutShape, SelfDescPointer};

// =========================================================
// Cardinality pointer: branch on a size tier stored with the
// pointer, against the same tiers precomputed in plain tables.
// Three contenders:
//
// 1. Padded tuple: Vec<(*const u64, u8)>, padded to 16 bytes per
//    entry for the pointer's alignment; four entries per cache line.
//
// 2. Parallel vecs: Vec<*const u64> + Vec<u8>, two separate
//    streams; a lookup that needs both fields reads both.
//
// 3. CardinalityPointer: 8 bytes per entry, the top byte holding
//    log2 of the cardinality; eight entries per cache line.
//
// 10,000 entries: 80 KB as CardinalityPointers, 160 KB as padded
// tuples.
// =========================================================

fn cardinality_branch_vs_table(c: &mut Criterion) {
    const N: usize = 10_000;

    // Precompute tier byte for the native cases so the bench
    // measures lookup cost, not log2 cost.
    let tiers_u8: Vec<u8> = (0..N as u64).map(|i| match i % 3 {
        0 => 1,   // SizeTier::Tiny = 0..=3
        1 => 5,   // SizeTier::Medium = 4..=10
        _ => 15,  // SizeTier::Large = 11..=
    }).collect();
    let ptrs: Vec<*const u64> = (1..=N).map(|i| i as *const u64).collect();
    let padded_tuples: Vec<(*const u64, u8)> = ptrs.iter()
        .zip(tiers_u8.iter())
        .map(|(p, &t)| (*p, t))
        .collect();
    let cps: Vec<CardinalityPointer<u64>> = (0..N as u64).map(|i| {
        let card = match i % 3 {
            0 => 5u64,         // -> tier Tiny
            1 => 500u64,       // -> tier Medium
            _ => 1_000_000u64, // -> tier Large
        };
        unsafe { CardinalityPointer::from_raw((i + 1) as *const u64, card) }
    }).collect();

    // Padded tuple: 16 bytes per entry (8 ptr + 1 tier + 7 padding).
    c.bench_function("bitsteal.cardinality/padded_tuple_baseline", |b| {
        b.iter(|| {
            let (mut tiny, mut medium, mut large) = (0u32, 0u32, 0u32);
            for (_, tier) in padded_tuples.iter() {
                match *tier {
                    0..=3 => tiny += 1,
                    4..=10 => medium += 1,
                    _ => large += 1,
                }
            }
            black_box((tiny, medium, large))
        });
    });

    // Parallel vecs: 8-byte ptr + 1-byte tier in separate arrays.
    // Iterates the tier vec alone: a planner that branches on the
    // tier before it touches the pointer.
    c.bench_function("bitsteal.cardinality/parallel_vecs_baseline", |b| {
        b.iter(|| {
            let (mut tiny, mut medium, mut large) = (0u32, 0u32, 0u32);
            for tier in tiers_u8.iter() {
                match *tier {
                    0..=3 => tiny += 1,
                    4..=10 => medium += 1,
                    _ => large += 1,
                }
            }
            black_box((tiny, medium, large))
        });
    });

    // CardinalityPointer: 8 bytes per entry, top byte holds the
    // precomputed log2 of cardinality. size_tier reads the byte
    // and matches.
    c.bench_function("bitsteal.cardinality/inline_size_tier", |b| {
        b.iter(|| {
            let (mut tiny, mut medium, mut large) = (0u32, 0u32, 0u32);
            for p in cps.iter() {
                match p.size_tier() {
                    SizeTier::Tiny => tiny += 1,
                    SizeTier::Medium => medium += 1,
                    SizeTier::Large => large += 1,
                }
            }
            black_box((tiny, medium, large))
        });
    });

    // Dispatch: each entry's pointer and tier are both read. A
    // CardinalityPointer gives both fields in one 8-byte load; the
    // parallel vecs take two loads from separate arrays.
    //
    // The pointers are summed as integers and the sum goes into
    // black_box, so the pointer loads are not eliminated.
    c.bench_function("bitsteal.cardinality/dispatch_padded_tuple", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            let (mut tiny, mut medium, mut large) = (0u32, 0u32, 0u32);
            for (ptr, tier) in padded_tuples.iter() {
                acc = acc.wrapping_add(*ptr as u64);
                match *tier {
                    0..=3 => tiny += 1,
                    4..=10 => medium += 1,
                    _ => large += 1,
                }
            }
            black_box((acc, tiny, medium, large))
        });
    });
    c.bench_function("bitsteal.cardinality/dispatch_parallel_vecs", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            let (mut tiny, mut medium, mut large) = (0u32, 0u32, 0u32);
            for i in 0..N {
                let ptr = ptrs[i];
                let tier = tiers_u8[i];
                acc = acc.wrapping_add(ptr as u64);
                match tier {
                    0..=3 => tiny += 1,
                    4..=10 => medium += 1,
                    _ => large += 1,
                }
            }
            black_box((acc, tiny, medium, large))
        });
    });
    c.bench_function("bitsteal.cardinality/dispatch_inline", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            let (mut tiny, mut medium, mut large) = (0u32, 0u32, 0u32);
            for p in cps.iter() {
                acc = acc.wrapping_add(p.as_raw() as u64);
                match p.size_tier() {
                    SizeTier::Tiny => tiny += 1,
                    SizeTier::Medium => medium += 1,
                    SizeTier::Large => large += 1,
                }
            }
            black_box((acc, tiny, medium, large))
        });
    });
}

// =========================================================
// KStep strided: KStepPointer::get with a k_step the compiler can
// see, against a stride read at run time and a compile-time
// constant stride.
//
// The runtime arm reads the stride from a Vec<usize> at a
// black_box'd index, so the compiler cannot fold it into a
// constant shift.
// =========================================================

fn kstep_vs_runtime_stride(c: &mut Criterion) {
    const N: usize = 1024;
    let matrix: Vec<u64> = (0..N as u64).collect();
    // Runtime-supplied stride: read from a Vec at a runtime index
    // so the compiler cannot constant-fold the value into SHL.
    let strides_table: Vec<usize> = vec![16, 32, 48, 64];

    c.bench_function("bitsteal.kstep/runtime_stride_usize", |b| {
        b.iter(|| {
            let stride_idx = black_box(1usize);
            let stride = strides_table[stride_idx];
            let mut sum = 0u64;
            let base = matrix.as_ptr();
            for i in 0..(N / 4) {
                let p = unsafe { (base as *const u8).add(i * stride) as *const u64 };
                sum = sum.wrapping_add(unsafe { *p });
            }
            black_box(sum)
        });
    });

    let kp = unsafe { KStepPointer::new(matrix.as_ptr(), 2) };
    c.bench_function("bitsteal.kstep/typed_k_step", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for i in 0..(N / 4) {
                sum = sum.wrapping_add(unsafe { *kp.get(i) });
            }
            black_box(sum)
        });
    });

    // Third contender: a compile-time-constant stride, which the
    // compiler folds into SHL like the typed path, so all three
    // regimes run side by side.
    c.bench_function("bitsteal.kstep/compile_const_stride_baseline", |b| {
        b.iter(|| {
            const STRIDE: usize = 32;
            let mut sum = 0u64;
            let base = matrix.as_ptr();
            for i in 0..(N / 4) {
                let p = unsafe { (base as *const u8).add(i * STRIDE) as *const u64 };
                sum = sum.wrapping_add(unsafe { *p });
            }
            black_box(sum)
        });
    });
}

// =========================================================
// SelfDescPointer: type dispatch via byte switch vs vtable.
// The contenders:
//
// 1. Arc<dyn Handle>:  the shared-ownership shape. The loop only
//                      derefs, which touches no refcount.
// 2. Box<dyn Handle>:  the single-owner vtable shape.
// 3. enum Handle:      the Rust-idiomatic alternative.
// 4. SelfDescPointer:  the inline-byte dispatch.
// =========================================================

trait Handle: Send + Sync {
    fn kind(&self) -> u8;
}

// The payloads give each handle the size of an inline, array or boxed
// handle; kind() does not read them.
#[allow(dead_code)]
struct ScalarHandle(u64);
#[allow(dead_code)]
struct ArrayHandle(Vec<u64>);
#[allow(dead_code)]
struct TreeHandle(Box<u64>);

impl Handle for ScalarHandle { fn kind(&self) -> u8 { 1 } }
impl Handle for ArrayHandle { fn kind(&self) -> u8 { 2 } }
impl Handle for TreeHandle { fn kind(&self) -> u8 { 3 } }

#[allow(dead_code)]
enum EnumHandle {
    Scalar(u64),
    Array(Vec<u64>),
    Tree(Box<u64>),
}

impl EnumHandle {
    #[inline]
    fn kind(&self) -> u8 {
        match self {
            EnumHandle::Scalar(_) => 1,
            EnumHandle::Array(_) => 2,
            EnumHandle::Tree(_) => 3,
        }
    }
}

fn dispatch_via_vtable_vs_byte(c: &mut Criterion) {
    const N: usize = 1024;

    // Arc-shared dyn dispatch: real-world heterogeneous container.
    let arcs: Vec<Arc<dyn Handle>> = (0..N).map(|i| {
        let h: Arc<dyn Handle> = match i % 3 {
            0 => Arc::new(ScalarHandle(i as u64)),
            1 => Arc::new(ArrayHandle(vec![i as u64])),
            _ => Arc::new(TreeHandle(Box::new(i as u64))),
        };
        h
    }).collect();

    c.bench_function("bitsteal.self_desc/arc_dyn_vtable", |b| {
        b.iter(|| {
            let (mut t1, mut t2, mut t3) = (0u32, 0u32, 0u32);
            for h in arcs.iter() {
                match h.kind() {
                    1 => t1 += 1,
                    2 => t2 += 1,
                    3 => t3 += 1,
                    _ => {}
                }
            }
            black_box((t1, t2, t3))
        });
    });

    // Box<dyn>: vtable dispatch without atomic refcount.
    let boxes: Vec<Box<dyn Handle>> = (0..N).map(|i| {
        let h: Box<dyn Handle> = match i % 3 {
            0 => Box::new(ScalarHandle(i as u64)),
            1 => Box::new(ArrayHandle(vec![i as u64])),
            _ => Box::new(TreeHandle(Box::new(i as u64))),
        };
        h
    }).collect();

    c.bench_function("bitsteal.self_desc/box_dyn_vtable", |b| {
        b.iter(|| {
            let (mut t1, mut t2, mut t3) = (0u32, 0u32, 0u32);
            for h in boxes.iter() {
                match h.kind() {
                    1 => t1 += 1,
                    2 => t2 += 1,
                    3 => t3 += 1,
                    _ => {}
                }
            }
            black_box((t1, t2, t3))
        });
    });

    // Enum dispatch: idiomatic Rust closed-universe alternative.
    let enums: Vec<EnumHandle> = (0..N).map(|i| match i % 3 {
        0 => EnumHandle::Scalar(i as u64),
        1 => EnumHandle::Array(vec![i as u64]),
        _ => EnumHandle::Tree(Box::new(i as u64)),
    }).collect();

    c.bench_function("bitsteal.self_desc/enum_tag_match", |b| {
        b.iter(|| {
            let (mut t1, mut t2, mut t3) = (0u32, 0u32, 0u32);
            for h in enums.iter() {
                match h.kind() {
                    1 => t1 += 1,
                    2 => t2 += 1,
                    3 => t3 += 1,
                    _ => {}
                }
            }
            black_box((t1, t2, t3))
        });
    });

    // SelfDescPointer-based: type ID is the byte; no vtable, no
    // indirect call.
    let descs: Vec<SelfDescPointer<u64>> = (0..N as u64).map(|i| {
        let (id, shape) = match i % 3 {
            0 => (1u8, LayoutShape::Scalar),
            1 => (2u8, LayoutShape::FixedArray),
            _ => (3u8, LayoutShape::Tree),
        };
        unsafe { SelfDescPointer::from_raw((i + 1) as *const u64, id, shape) }
    }).collect();

    c.bench_function("bitsteal.self_desc/byte_switch_no_vtable", |b| {
        b.iter(|| {
            let (mut t1, mut t2, mut t3) = (0u32, 0u32, 0u32);
            for p in descs.iter() {
                match p.type_id() {
                    1 => t1 += 1,
                    2 => t2 += 1,
                    3 => t3 += 1,
                    _ => {}
                }
            }
            black_box((t1, t2, t3))
        });
    });
}

criterion_group!(
    benches,
    cardinality_branch_vs_table,
    kstep_vs_runtime_stride,
    dispatch_via_vtable_vs_byte,
);
criterion_main!(benches);
