//! Bench: OwnerLease<T> hot paths vs `std::sync::Mutex<T>` and
//! `parking_lot::Mutex<T>`.
//!
//! Architectural claim: cross-process exclusive access with
//! lowest-PID preemption and heartbeat-stale auto-failover, at
//! per-op cost competitive with in-process mutex baselines. The
//! cross-process visibility and the failover property are what
//! neither baseline can do at any cost.
//!
//! Workloads:
//! - acquire_release cycle (full lock cycle)
//! - with_lease closure-based RAII (acquire + mutate + release)
//! - read_as_owner post-acquire (lease-held fast path)
//! - write_as_owner post-acquire (lease-held fast path)
//! - beat (heartbeat refresh)
//! - am_i_owner (cheap status check)

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};
use parking_lot::Mutex as PlMutex;

use subetha_cxc::OwnerLease;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-lease-{name}-{pid}.bin"));
    p
}

// =========================================================
// acquire_release cycle (full lock cycle)
// =========================================================
//
// Pre-audit: try_acquire from NO_OWNER is one CAS; release is
// one CAS. std::Mutex::lock + drop is the textbook equivalent
// lock cycle. parking_lot::Mutex is the production-grade
// in-process baseline.

fn acquire_release_cycle(c: &mut Criterion) {
    let path = tmp("ar");
    let l: OwnerLease<u64> = OwnerLease::create(&path, 0).unwrap();
    c.bench_function("lease.acquire_release/mmf", |b| {
        b.iter(|| {
            assert!(l.try_acquire(black_box(100), 1_000_000));
            assert!(l.release(100));
        });
    });
    drop(l);
    std::fs::remove_file(&path).ok();

    let m: Mutex<u64> = Mutex::new(0);
    c.bench_function("lease.acquire_release/std_mutex", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(*g);
        });
    });

    let p: PlMutex<u64> = PlMutex::new(0);
    c.bench_function("lease.acquire_release/parking_lot_mutex", |b| {
        b.iter(|| {
            let g = p.lock();
            black_box(*g);
        });
    });
}

// =========================================================
// with_lease closure-based RAII cycle
// =========================================================
//
// Pre-audit: with_lease does try_acquire + read payload + run
// closure + write payload via SeqLock + release. Equivalent to
// std::Mutex::lock + read + mutate + drop. Both cycle the
// lock once per iter and update the payload once.

fn with_lease_cycle(c: &mut Criterion) {
    let path = tmp("wl");
    let l: OwnerLease<u64> = OwnerLease::create(&path, 0).unwrap();
    c.bench_function("lease.with_lease/mmf", |b| {
        b.iter(|| {
            l.with_lease(black_box(100), 1_000_000, |v| {
                *v = v.wrapping_add(1);
            });
        });
    });
    drop(l);
    std::fs::remove_file(&path).ok();

    let m: Mutex<u64> = Mutex::new(0);
    c.bench_function("lease.with_lease/std_mutex", |b| {
        b.iter(|| {
            let mut g = m.lock().unwrap();
            *g = g.wrapping_add(1);
        });
    });

    let pl: PlMutex<u64> = PlMutex::new(0);
    c.bench_function("lease.with_lease/parking_lot_mutex", |b| {
        b.iter(|| {
            let mut g = pl.lock();
            *g = g.wrapping_add(1);
        });
    });
}

// =========================================================
// read_as_owner post-acquire (lease-held fast path)
// =========================================================
//
// Pre-audit: the lease is pre-acquired ONCE outside b.iter.
// Per-iter cost is one am_i_owner check + one unaligned-read of
// payload. No mutex equivalent exists (mutexes don't model
// "hold lock across many reads"), so we compare against
// std::Mutex::lock+read+unlock as the closest in-process shape.

fn read_as_owner_fast_path(c: &mut Criterion) {
    let path = tmp("ro");
    let l: OwnerLease<u64> = OwnerLease::create(&path, 0xCAFE_BABE).unwrap();
    assert!(l.try_acquire(100, 1_000_000));
    c.bench_function("lease.read_as_owner_held/mmf", |b| {
        b.iter(|| black_box(l.read_as_owner(black_box(100))));
    });
    l.release(100);
    drop(l);
    std::fs::remove_file(&path).ok();

    let m: Mutex<u64> = Mutex::new(0xCAFE_BABE);
    c.bench_function("lease.read_under_lock/std_mutex", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(*g)
        });
    });
}

// =========================================================
// write_as_owner post-acquire (lease-held fast path)
// =========================================================

fn write_as_owner_fast_path(c: &mut Criterion) {
    let path = tmp("wo");
    let l: OwnerLease<u64> = OwnerLease::create(&path, 0).unwrap();
    assert!(l.try_acquire(100, 1_000_000));
    let mut v = 0u64;
    c.bench_function("lease.write_as_owner_held/mmf", |b| {
        b.iter(|| {
            v = v.wrapping_add(1);
            l.write_as_owner(black_box(100), black_box(v));
        });
    });
    l.release(100);
    drop(l);
    std::fs::remove_file(&path).ok();

    let m: Mutex<u64> = Mutex::new(0);
    let mut v = 0u64;
    c.bench_function("lease.write_under_lock/std_mutex", |b| {
        b.iter(|| {
            v = v.wrapping_add(1);
            *m.lock().unwrap() = black_box(v);
        });
    });
}

// =========================================================
// beat (heartbeat refresh; unique to lease, no fair contender)
// =========================================================

fn beat_hot(c: &mut Criterion) {
    let path = tmp("beat");
    let l: OwnerLease<u64> = OwnerLease::create(&path, 0).unwrap();
    assert!(l.try_acquire(100, 1_000_000));
    c.bench_function("lease.beat/mmf", |b| {
        b.iter(|| black_box(l.beat(black_box(100))));
    });
    l.release(100);
    drop(l);
    std::fs::remove_file(&path).ok();
}

// =========================================================
// am_i_owner status check (one atomic load)
// =========================================================

fn am_i_owner_check(c: &mut Criterion) {
    let path = tmp("aio");
    let l: OwnerLease<u64> = OwnerLease::create(&path, 0).unwrap();
    assert!(l.try_acquire(100, 1_000_000));
    c.bench_function("lease.am_i_owner/mmf", |b| {
        b.iter(|| black_box(l.am_i_owner(black_box(100))));
    });
    l.release(100);
    drop(l);
    std::fs::remove_file(&path).ok();
}

criterion_group!(benches,
    acquire_release_cycle,
    with_lease_cycle,
    read_as_owner_fast_path,
    write_as_owner_fast_path,
    beat_hot,
    am_i_owner_check,
);
criterion_main!(benches);
