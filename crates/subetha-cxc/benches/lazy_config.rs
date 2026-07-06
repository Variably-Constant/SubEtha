//! Bench: LazyConfig<T> hot paths vs `std::sync::OnceLock<T>`
//! (in-process equivalent) and `Mutex<Option<T>>` (naive lock).
//!
//! Architectural claim: LazyConfig prevents the thundering-herd
//! fetch across N processes via the SharedOnceCell CAS. Per-op
//! cost for the cached-read path is competitive with the
//! in-process OnceLock baseline; the cross-process visibility is
//! what neither baseline can do at any cost.
//!
//! Workloads (all single-threaded; concurrent fetch-once
//! validation lives in the unit-test
//! `fetch_runs_at_most_once_under_concurrency`):
//! - try_get on loaded config (hot read)
//! - get_or_fetch on loaded config (post-fetch hot path)
//! - is_loaded status check

use std::hint::black_box;
use std::sync::{Mutex, OnceLock};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::LazyConfig;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-lazyconf-{name}-{pid}.bin"));
    p
}

// =========================================================
// try_get on loaded config
// =========================================================

fn try_get_loaded(c: &mut Criterion) {
    let path = tmp("tryget");
    let lc: LazyConfig<u64> = LazyConfig::create(&path).unwrap();
    let _val = lc.get_or_fetch(|| 0xCAFE_BABE);
    c.bench_function("lazyconf.try_get_loaded/mmf", |b| {
        b.iter(|| black_box(lc.try_get()));
    });
    drop(lc);
    std::fs::remove_file(&path).ok();

    let once: OnceLock<u64> = OnceLock::new();
    let _val = once.get_or_init(|| 0xCAFE_BABE);
    c.bench_function("lazyconf.try_get_loaded/std_oncelock", |b| {
        b.iter(|| black_box(once.get().copied()));
    });

    let m: Mutex<Option<u64>> = Mutex::new(Some(0xCAFE_BABE));
    c.bench_function("lazyconf.try_get_loaded/mutex_option", |b| {
        b.iter(|| black_box(*m.lock().unwrap()));
    });
}

// =========================================================
// get_or_fetch on loaded config (post-fetch hot path)
// =========================================================

fn get_or_fetch_loaded(c: &mut Criterion) {
    let path = tmp("gof");
    let lc: LazyConfig<u64> = LazyConfig::create(&path).unwrap();
    let _val = lc.get_or_fetch(|| 0xCAFE_BABE);
    c.bench_function("lazyconf.get_or_fetch_loaded/mmf", |b| {
        b.iter(|| black_box(lc.get_or_fetch(|| panic!("must not run"))));
    });
    drop(lc);
    std::fs::remove_file(&path).ok();

    let once: OnceLock<u64> = OnceLock::new();
    let _val = once.get_or_init(|| 0xCAFE_BABE);
    c.bench_function("lazyconf.get_or_fetch_loaded/std_oncelock", |b| {
        b.iter(|| black_box(*once.get_or_init(|| panic!("must not run"))));
    });
}

// =========================================================
// is_loaded status check
// =========================================================

fn is_loaded_check(c: &mut Criterion) {
    let path = tmp("isload");
    let lc: LazyConfig<u64> = LazyConfig::create(&path).unwrap();
    let _val = lc.get_or_fetch(|| 1u64);
    c.bench_function("lazyconf.is_loaded/mmf", |b| {
        b.iter(|| black_box(lc.is_loaded()));
    });
    drop(lc);
    std::fs::remove_file(&path).ok();
}

criterion_group!(benches,
    try_get_loaded,
    get_or_fetch_loaded,
    is_loaded_check,
);
criterion_main!(benches);
