//! Bench: SharedRateLimiter vs Mutex<TokenBucket> (the textbook
//! in-process baseline).
//!
//! Architectural claim: lock-free CAS-based token bucket beats the
//! Mutex baseline AND provides cross-process visibility no in-
//! process baseline can match.
//!
//! Workloads:
//! - try_acquire(1) on a full bucket (hot path)
//! - try_acquire(1) on an empty bucket (rejection path)
//! - available() observer read
//! - 4-thread concurrent acquire

use std::hint::black_box;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedRateLimiter;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-ratelim-{name}-{pid}.bin"));
    p
}

// Baseline: Mutex-protected token bucket.
struct MutexBucket {
    inner: Mutex<MutexBucketState>,
    capacity: u32,
    rate: u32,
}
struct MutexBucketState { tokens: u32, last_refill_us: u32 }

impl MutexBucket {
    fn new(capacity: u32, rate: u32) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u32;
        Self {
            inner: Mutex::new(MutexBucketState {
                tokens: capacity, last_refill_us: now,
            }),
            capacity, rate,
        }
    }
    fn try_acquire(&self, n: u32) -> bool {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u32;
        let mut s = self.inner.lock().unwrap();
        let elapsed = now.wrapping_sub(s.last_refill_us) as u64;
        let refilled = ((elapsed * self.rate as u64) / 1_000_000).min(u32::MAX as u64) as u32;
        let after = (s.tokens.saturating_add(refilled)).min(self.capacity);
        if after < n { return false; }
        s.tokens = after - n;
        s.last_refill_us = now;
        true
    }
    fn available(&self) -> u32 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u32;
        let s = self.inner.lock().unwrap();
        let elapsed = now.wrapping_sub(s.last_refill_us) as u64;
        let refilled = ((elapsed * self.rate as u64) / 1_000_000).min(u32::MAX as u64) as u32;
        (s.tokens.saturating_add(refilled)).min(self.capacity)
    }
}

// =========================================================
// try_acquire on a full bucket (hot path)
// =========================================================

fn try_acquire_hot(c: &mut Criterion) {
    let p = tmp("acquire-hot");
    // Very high capacity + high refill so we never run out.
    let r = SharedRateLimiter::create(&p, 1_000_000, 1_000_000_000).unwrap();
    c.bench_function("ratelim.try_acquire/mmf", |b| {
        b.iter(|| r.try_acquire(black_box(1)).is_ok());
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let m = MutexBucket::new(1_000_000, 1_000_000_000);
    c.bench_function("ratelim.try_acquire/mutex_bucket", |b| {
        b.iter(|| m.try_acquire(black_box(1)));
    });
}

// =========================================================
// try_acquire on an empty bucket (rejection path)
// =========================================================

fn try_acquire_empty(c: &mut Criterion) {
    let p = tmp("acquire-empty");
    let r = SharedRateLimiter::create(&p, 10, 1).unwrap();  // slow refill
    // Drain it.
    for _ in 0..10 { r.try_acquire(1).ok(); }
    c.bench_function("ratelim.try_acquire_empty/mmf", |b| {
        b.iter(|| r.try_acquire(black_box(1)).is_err());
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let m = MutexBucket::new(10, 1);
    for _ in 0..10 { m.try_acquire(1); }
    c.bench_function("ratelim.try_acquire_empty/mutex_bucket", |b| {
        b.iter(|| !m.try_acquire(black_box(1)));
    });
}

// =========================================================
// available() observer
// =========================================================

fn available_observer(c: &mut Criterion) {
    let p = tmp("avail");
    let r = SharedRateLimiter::create(&p, 1000, 100).unwrap();
    c.bench_function("ratelim.available/mmf", |b| {
        b.iter(|| black_box(r.available()));
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let m = MutexBucket::new(1000, 100);
    c.bench_function("ratelim.available/mutex_bucket", |b| {
        b.iter(|| black_box(m.available()));
    });
}

// Multi-thread acquire correctness is covered by source-level
// tests. A per-iter thread::spawn microbench is dominated by
// Windows thread-creation cost.

criterion_group!(benches,
    try_acquire_hot,
    try_acquire_empty,
    available_observer,
);
criterion_main!(benches);
