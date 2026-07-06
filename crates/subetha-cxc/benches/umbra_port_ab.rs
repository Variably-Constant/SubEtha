//! A/B/C bench: does the Umbra content-prefix optimization help vs
//! the baseline, and how do the two implementations compare?
//!
//! Contenders:
//! - **A (baseline)**: No Umbra. Scan `Vec<*const Item>`, deref each
//!   pointer and compare the 32-byte struct against the query.
//!   Represents the IPC crate's lookup pattern WITHOUT the Umbra
//!   optimization.
//! - **B (original `UmbraPointer<Item>`)**: From `subetha-pointers`.
//!   The existing nightly-required implementation. Prefix-first
//!   compare, full equality only on prefix match.
//! - **C (ported `Umbra<RawPtr<Item>>`)**: New module in
//!   `subetha-cxc`. Same algorithmic protocol as B, generic over
//!   `PointerTarget` for cross-process reuse. Stable Rust portable.
//!
//! Workload designed to make the Umbra short-circuit matter:
//! - `Item` is 32 bytes (key: u32 + 28-byte payload). Full equality
//!   reads 32 bytes from memory.
//! - 1024 items stored.
//! - 10,000 queries, 50% hit / 50% miss. Misses are where Umbra
//!   wins (prefix-reject without deref).
//!
//! Bench audit (HARD RULE 3):
//! - All three contenders exercise their named feature (A does
//!   full compare always; B and C do prefix-first then full compare).
//! - Same payload size (32 B), same N (1024), same M (10000),
//!   same 50/50 hit-miss split.
//! - All three use the same `*const Item` underneath; the equality
//!   protocol is the only differential.

#![allow(clippy::missing_docs_in_private_items)]

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use subetha_pointers::umbra_pointer::UmbraPointer;

/// Derive a 4-byte prefix from the leading bytes of `value`. Inlined
/// here because the bench needs it for both the baseline (A) and the
/// UmbraPointer (B) setups; the helper was previously in a deleted
/// `umbra_ptr` module that lived in subetha-cxc before consolidation.
fn prefix_from_leading_bytes<T>(value: &T) -> u32 {
    let n = core::mem::size_of::<T>().min(4);
    let mut buf = [0u8; 4];
    // SAFETY: reading min(size, 4) bytes from the byte representation
    // of `value`. The borrow is local to this function.
    unsafe {
        let src = value as *const T as *const u8;
        core::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), n);
    }
    u32::from_le_bytes(buf)
}

const N: usize = 1024;
const M: usize = 10_000;

#[derive(Clone, Copy)]
#[repr(C)]
struct Item {
    key: u32,
    payload: [u8; 28],
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.payload == other.payload
    }
}

fn build_items() -> Vec<Item> {
    (0..N as u32)
        .map(|i| Item {
            key: i,
            payload: {
                let mut p = [0u8; 28];
                p[0] = (i & 0xFF) as u8;
                p[27] = ((i >> 8) & 0xFF) as u8;
                p
            },
        })
        .collect()
}

fn build_queries(items: &[Item]) -> Vec<Item> {
    (0..M)
        .map(|i| {
            if i % 2 == 0 {
                items[i % items.len()]
            } else {
                Item {
                    key: 0xFFFF_0000 + i as u32,
                    payload: [0xAA; 28],
                }
            }
        })
        .collect()
}

// ============================================================
// A: No Umbra (baseline)
// ============================================================
fn bench_a_no_umbra(c: &mut Criterion) {
    let items = build_items();
    let queries = build_queries(&items);
    let ptrs: Vec<*const Item> = items.iter().map(|i| i as *const Item).collect();

    c.bench_function("umbra_abc/A_baseline_no_umbra", |b| {
        b.iter(|| {
            let mut hits = 0u64;
            for q in &queries {
                for &p in &ptrs {
                    // SAFETY: ptrs[..] alias items[..] which outlives this loop.
                    let item = unsafe { &*p };
                    if item == q {
                        hits += 1;
                        break;
                    }
                }
            }
            black_box(hits)
        });
    });
}

// ============================================================
// B: Original UmbraPointer<Item> from subetha-pointers
// ============================================================
fn bench_b_original_umbra(c: &mut Criterion) {
    let items = build_items();
    let queries = build_queries(&items);
    let uptrs: Vec<UmbraPointer<Item>> = items
        .iter()
        .map(|i| unsafe {
            UmbraPointer::from_raw(prefix_from_leading_bytes(i), i as *const Item)
        })
        .collect();

    c.bench_function("umbra_abc/B_original_umbra_primitives", |b| {
        b.iter(|| {
            let mut hits = 0u64;
            for q in &queries {
                let q_prefix = prefix_from_leading_bytes(q);
                for up in &uptrs {
                    if up.matches_prefix(q_prefix) {
                        let item = unsafe { &*up.as_raw() };
                        if item == q {
                            hits += 1;
                            break;
                        }
                    }
                }
            }
            black_box(hits)
        });
    });
}

criterion_group!(
    benches,
    bench_a_no_umbra,
    bench_b_original_umbra,
);
criterion_main!(benches);
