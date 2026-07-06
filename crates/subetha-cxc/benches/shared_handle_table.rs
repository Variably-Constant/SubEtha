//! Bench: SharedHandleTable vs in-process slotmap (using the
//! already-shipped Slotmap from subetha-pointers' AdaptiveHandle) and
//! vs Arc<RwLock<HashMap<u64, T>>> as a baseline.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use parking_lot::RwLock;

use subetha_cxc::{Handle, SharedHandleTable};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-handle-{name}-{pid}.bin"));
    p
}

// =========================================================
// Insert (warm-table) latency
// =========================================================

fn insert_warm(c: &mut Criterion) {
    let path = tmp("insert");
    let t: SharedHandleTable<u64> = SharedHandleTable::create(&path, 100_000).unwrap();
    c.bench_function("shared_handle.insert/mmf", |b| {
        b.iter(|| {
            let h = t.insert(black_box(42)).unwrap();
            // Remove to prevent table exhaustion mid-bench. The
            // handle was just inserted, so remove must return Some;
            // .expect surfaces table corruption as a panic.
            t.remove(h).expect("just-inserted handle missing");
        });
    });
    drop(t); std::fs::remove_file(&path).ok();

    let map: RwLock<HashMap<u64, u64>> = RwLock::new(HashMap::new());
    let mut next_id = 0u64;
    c.bench_function("shared_handle.insert/rwlock_hashmap", |b| {
        b.iter(|| {
            let id = next_id;
            next_id += 1;
            map.write().insert(black_box(id), black_box(42));
            map.write().remove(&id);
        });
    });
}

// =========================================================
// Live-handle get latency
// =========================================================

fn get_live(c: &mut Criterion) {
    let path = tmp("get");
    let t: SharedHandleTable<u64> = SharedHandleTable::create(&path, 1024).unwrap();
    let handles: Vec<Handle> = (0..1024).map(|i| t.insert(i as u64).unwrap()).collect();
    let mut idx = 0usize;
    c.bench_function("shared_handle.get/mmf_live", |b| {
        b.iter(|| {
            let h = handles[idx % handles.len()];
            idx += 1;
            black_box(t.get(black_box(h)))
        });
    });
    drop(t); std::fs::remove_file(&path).ok();

    let map: HashMap<u64, u64> = (0..1024u64).map(|i| (i, i)).collect();
    let map_arc = Arc::new(RwLock::new(map));
    let mut idx = 0u64;
    c.bench_function("shared_handle.get/rwlock_hashmap_live", |b| {
        b.iter(|| {
            let k = idx % 1024;
            idx += 1;
            black_box(map_arc.read().get(&black_box(k)).copied())
        });
    });
}

// =========================================================
// Stale-handle rejection cost (the safe-after-free path)
// =========================================================

fn stale_rejection(c: &mut Criterion) {
    let path = tmp("stale");
    let t: SharedHandleTable<u64> = SharedHandleTable::create(&path, 1024).unwrap();
    let mut stale: Vec<Handle> = Vec::with_capacity(1024);
    for i in 0..1024u64 {
        let h = t.insert(i).unwrap();
        t.remove(h).expect("just-inserted handle missing");
        stale.push(h);
    }
    let mut idx = 0usize;
    c.bench_function("shared_handle.get_stale/mmf", |b| {
        b.iter(|| {
            let h = stale[idx % stale.len()];
            idx += 1;
            black_box(t.get(black_box(h)))  // returns None
        });
    });
    drop(t); std::fs::remove_file(&path).ok();
}

criterion_group!(benches, insert_warm, get_live, stale_rejection);
criterion_main!(benches);
