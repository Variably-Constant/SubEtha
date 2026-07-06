//! Bench: SharedHashMap<K, V> vs Mutex<std::HashMap> and
//! RwLock<std::HashMap> (the standard in-process patterns).
//!
//! Architectural claim: SharedHashMap matches in-process maps on
//! hot reads (one atomic load + SeqLock cell read vs Mutex
//! lock+lookup+unlock) and provides cross-process map semantics
//! that the in-process baselines cannot offer at any cost. Open-
//! addressing with linear probing keeps probe chains short under
//! sub-0.5 load factor.
//!
//! Workloads:
//! - insert hot (uncontended)
//! - get hot (uncontended)
//! - len observer (dashboard)
//! - 4-thread concurrent insert + get mix

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedHashMap;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-hashmap-{name}-{pid}.bin"));
    p
}

// =========================================================
// insert hot
// =========================================================

fn insert_hot(c: &mut Criterion) {
    let p = tmp("insert");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 4096).unwrap();
    let mut k = 0u32;
    c.bench_function("hashmap.insert/mmf", |b| {
        b.iter(|| {
            // Rotate keys to avoid filling the map; this measures
            // insert-or-update steady state.
            k = k.wrapping_add(1) & 0xFFF;
            m.insert(black_box(k), black_box(k * 2)).unwrap();
        });
    });
    drop(m);
    std::fs::remove_file(&p).ok();

    let mh: Mutex<HashMap<u32, u32>> = Mutex::new(HashMap::with_capacity(4096));
    let mut k = 0u32;
    c.bench_function("hashmap.insert/mutex_stdhashmap", |b| {
        b.iter(|| {
            k = k.wrapping_add(1) & 0xFFF;
            mh.lock().unwrap().insert(black_box(k), black_box(k * 2));
        });
    });

    let rw: RwLock<HashMap<u32, u32>> = RwLock::new(HashMap::with_capacity(4096));
    let mut k = 0u32;
    c.bench_function("hashmap.insert/rwlock_stdhashmap", |b| {
        b.iter(|| {
            k = k.wrapping_add(1) & 0xFFF;
            rw.write().unwrap().insert(black_box(k), black_box(k * 2));
        });
    });
}

// =========================================================
// get hot
// =========================================================

fn get_hot(c: &mut Criterion) {
    let p = tmp("get");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 4096).unwrap();
    for i in 0..1000u32 { m.insert(i, i * 2).unwrap(); }
    c.bench_function("hashmap.get/mmf", |b| {
        b.iter(|| black_box(m.get(&black_box(500))));
    });
    drop(m);
    std::fs::remove_file(&p).ok();

    let mut h: HashMap<u32, u32> = HashMap::with_capacity(4096);
    for i in 0..1000u32 { h.insert(i, i * 2); }
    let mh = Mutex::new(h.clone());
    c.bench_function("hashmap.get/mutex_stdhashmap", |b| {
        b.iter(|| {
            let g = mh.lock().unwrap();
            black_box(g.get(&black_box(500)).copied())
        });
    });
    let rw = RwLock::new(h);
    c.bench_function("hashmap.get/rwlock_stdhashmap", |b| {
        b.iter(|| {
            let g = rw.read().unwrap();
            black_box(g.get(&black_box(500)).copied())
        });
    });
}

// =========================================================
// len observer (dashboard query)
// =========================================================

fn len_observer(c: &mut Criterion) {
    let p = tmp("len");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 256).unwrap();
    for i in 0..42u32 { m.insert(i, i).unwrap(); }
    c.bench_function("hashmap.len/mmf", |b| {
        b.iter(|| black_box(m.len()));
    });
    drop(m);
    std::fs::remove_file(&p).ok();

    let mh: Mutex<HashMap<u32, u32>> = Mutex::new((0..42u32).map(|i| (i, i)).collect());
    c.bench_function("hashmap.len/mutex_stdhashmap", |b| {
        b.iter(|| black_box(mh.lock().unwrap().len()));
    });
}

// =========================================================
// 4-thread concurrent insert+get mix
// =========================================================

fn concurrent_mix(c: &mut Criterion) {
    const PER_THREAD: u32 = 50;

    let p = tmp("concurrent");
    let m: Arc<SharedHashMap<u32, u32>> = Arc::new(SharedHashMap::create(&p, 8192).unwrap());
    c.bench_function("hashmap.concurrent_mix_4t/mmf", |b| {
        b.iter(|| {
            m.clear();
            let mut handles = vec![];
            for t in 0..4u32 {
                let m = m.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let key = t * PER_THREAD + i;
                        m.insert(key, key * 2).unwrap();
                        black_box(m.get(&key));
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
    drop(m);
    std::fs::remove_file(&p).ok();

    let mh: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(HashMap::with_capacity(8192)));
    c.bench_function("hashmap.concurrent_mix_4t/mutex_stdhashmap", |b| {
        b.iter(|| {
            mh.lock().unwrap().clear();
            let mut handles = vec![];
            for t in 0..4u32 {
                let mh = mh.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let key = t * PER_THREAD + i;
                        mh.lock().unwrap().insert(key, key * 2);
                        black_box(mh.lock().unwrap().get(&key).copied());
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
}

criterion_group!(benches,
    insert_hot,
    get_hot,
    len_observer,
    concurrent_mix,
);
criterion_main!(benches);
