//! Bench: SharedCountMinSketch vs Mutex<HashMap<Vec<u8>, u64>>.
//!
//! Architectural claim: CMS uses bounded memory (d*w*8 bytes
//! regardless of distinct item count) and d atomic increments per
//! insert. HashMap pays unbounded memory growth + lock per access.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedCountMinSketch;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-cms-{name}-{pid}.bin"));
    p
}

fn insert_single(c: &mut Criterion) {
    let p = tmp("ins");
    let cms = SharedCountMinSketch::create(&p, 4, 1024).unwrap();
    c.bench_function("cms.insert/mmf", |b| {
        b.iter(|| cms.insert(black_box(b"adaptive-prims")));
    });
    drop(cms);
    std::fs::remove_file(&p).ok();

    let m: Mutex<HashMap<Vec<u8>, u64>> = Mutex::new(HashMap::with_capacity(1024));
    c.bench_function("cms.insert/mutex_hashmap", |b| {
        b.iter(|| {
            let mut g = m.lock().unwrap();
            *g.entry(b"adaptive-prims".to_vec()).or_insert(0) += 1;
        });
    });
}

fn estimate_single(c: &mut Criterion) {
    let p = tmp("est");
    let cms = SharedCountMinSketch::create(&p, 4, 1024).unwrap();
    for _ in 0..1000 { cms.insert(b"foo"); }
    c.bench_function("cms.estimate/mmf", |b| {
        b.iter(|| black_box(cms.estimate_count(black_box(b"foo"))));
    });
    drop(cms);
    std::fs::remove_file(&p).ok();

    let m: Mutex<HashMap<Vec<u8>, u64>> = Mutex::new({
        let mut h = HashMap::new();
        h.insert(b"foo".to_vec(), 1000u64);
        h
    });
    c.bench_function("cms.estimate/mutex_hashmap", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(g.get(b"foo".as_slice()).copied().unwrap_or(0))
        });
    });
}

fn storage_witness(c: &mut Criterion) {
    let cms_bytes = 64 + 4 * 1024 * 8;
    let hashmap_avg = std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<u64>() + 16;
    eprintln!("[storage] CMS d=4 w=1024 (any distinct-count) = {cms_bytes} bytes");
    eprintln!("[storage] HashMap per entry                   ~= {hashmap_avg} bytes/key");
    eprintln!("[storage] HashMap at 10K distinct             ~= {} bytes ({}x)",
        hashmap_avg * 10_000,
        (hashmap_avg * 10_000) as f64 / cms_bytes as f64);
    c.bench_function("cms.storage_witness", |b| {
        b.iter(|| black_box(cms_bytes + hashmap_avg));
    });
}

criterion_group!(benches, insert_single, estimate_single, storage_witness);
criterion_main!(benches);
