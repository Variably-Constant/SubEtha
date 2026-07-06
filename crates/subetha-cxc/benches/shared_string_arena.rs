//! Bench: SharedStringArena vs Mutex<Vec<String>> and
//! RwLock<Vec<String>> (the textbook in-process interning patterns).
//!
//! Architectural claim: SharedStringArena provides position-
//! independent strings across processes at lock-free fetch_add
//! cost per intern. Mutex<Vec<String>> pays Mutex lock, Vec push,
//! and heap allocation for each new string. The MMF version trades
//! one allocation per intern for one memcpy into a pre-allocated
//! arena region.
//!
//! Workloads:
//! - intern hot (short string)
//! - get_bytes hot (resolve a ref)
//! - intern longer string (16 bytes)
//! - 4-thread concurrent intern

use std::hint::black_box;
use std::sync::{Mutex, RwLock};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedStringArena;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-arena-{name}-{pid}.bin"));
    p
}

// =========================================================
// intern hot (short string)
// =========================================================

fn intern_short(c: &mut Criterion) {
    let p = tmp("intern-short");
    let a = SharedStringArena::create(&p, 16 * 1024 * 1024).unwrap();  // 16 MB
    c.bench_function("arena.intern_short/mmf", |b| {
        b.iter(|| {
            match a.intern("hi") {
                Ok(r) => black_box(r),
                Err(_) => { a.clear(); a.intern("hi").unwrap() }
            }
        });
    });
    drop(a);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<String>> = Mutex::new(Vec::with_capacity(1 << 20));
    c.bench_function("arena.intern_short/mutex_vec_string", |b| {
        b.iter(|| {
            let mut g = mv.lock().unwrap();
            g.push(black_box("hi".to_string()));
            black_box(g.len() - 1)
        });
    });

    let rw: RwLock<Vec<String>> = RwLock::new(Vec::with_capacity(1 << 20));
    c.bench_function("arena.intern_short/rwlock_vec_string", |b| {
        b.iter(|| {
            let mut g = rw.write().unwrap();
            g.push(black_box("hi".to_string()));
            black_box(g.len() - 1)
        });
    });
}

// =========================================================
// intern longer (16 bytes)
// =========================================================

fn intern_16(c: &mut Criterion) {
    let p = tmp("intern-16");
    let a = SharedStringArena::create(&p, 16 * 1024 * 1024).unwrap();
    c.bench_function("arena.intern_16b/mmf", |b| {
        b.iter(|| {
            match a.intern("0123456789abcdef") {
                Ok(r) => black_box(r),
                Err(_) => { a.clear(); a.intern("0123456789abcdef").unwrap() }
            }
        });
    });
    drop(a);
    std::fs::remove_file(&p).ok();

    let mv: Mutex<Vec<String>> = Mutex::new(Vec::with_capacity(1 << 20));
    c.bench_function("arena.intern_16b/mutex_vec_string", |b| {
        b.iter(|| {
            let mut g = mv.lock().unwrap();
            g.push(black_box("0123456789abcdef".to_string()));
            black_box(g.len() - 1)
        });
    });
}

// =========================================================
// get_bytes hot (resolve a known ref)
// =========================================================

fn get_bytes_hot(c: &mut Criterion) {
    let p = tmp("get");
    let a = SharedStringArena::create(&p, 1024).unwrap();
    let r = a.intern("hello-world").unwrap();
    c.bench_function("arena.get_bytes/mmf", |b| {
        b.iter(|| black_box(a.get_bytes(black_box(r)).unwrap()));
    });
    drop(a);
    std::fs::remove_file(&p).ok();

    let v = Mutex::new(vec!["hello-world".to_string()]);
    c.bench_function("arena.get_bytes/mutex_vec_string", |b| {
        b.iter(|| {
            let g = v.lock().unwrap();
            black_box(g[0].as_bytes().to_owned())
        });
    });
}

// Multi-thread intern correctness is covered by source-level
// unit tests. A per-iter thread::spawn microbench is dominated
// by Windows thread-creation cost.

criterion_group!(benches,
    intern_short,
    intern_16,
    get_bytes_hot,
);
criterion_main!(benches);
