//! Bench: SharedSemaphore vs the standard in-process semaphore
//! pattern (`Arc<(Mutex<u32>, Condvar)>` - the textbook
//! permit-counting-condvar shape).
//!
//! Architectural claim: SharedSemaphore is comparable to the
//! in-process Mutex+Condvar pattern on the uncontended hot path
//! (a single atomic CAS vs a Mutex lock+unlock), and the
//! cross-process / disk-persistence capability comes for free.
//!
//! Workloads:
//! - try_acquire (single CAS, never blocks)
//! - acquire+drop (uncontended; CAS then drop's release)
//! - available() observer read
//! - 4-thread contended acquire+drop (cache-line contention)

use std::hint::black_box;
use std::sync::{Condvar, Mutex};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedSemaphore;

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-sem-{name}-{pid}"));
    p
}

fn cleanup_base(base: &std::path::Path) {
    for ext in ["count", "wakeup", "waiters"] {
        let mut p = base.to_path_buf();
        let stem = p.file_name().unwrap().to_string_lossy().to_string();
        p.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&p).ok();
    }
}

// =========================================================
// Textbook in-process Mutex+Condvar semaphore (baseline)
// =========================================================

struct MutexSem {
    inner: Mutex<u32>,
    cond: Condvar,
    max: u32,
}

impl MutexSem {
    fn new(initial: u32, max: u32) -> Self {
        Self { inner: Mutex::new(initial), cond: Condvar::new(), max }
    }
    fn try_acquire(&self) -> bool {
        let mut g = self.inner.lock().unwrap();
        if *g > 0 { *g -= 1; true } else { false }
    }
    #[allow(dead_code)] // available for the contended bench; kept for parity
    fn acquire(&self) {
        let mut g = self.inner.lock().unwrap();
        while *g == 0 { g = self.cond.wait(g).unwrap(); }
        *g -= 1;
    }
    #[allow(dead_code)]
    fn release(&self) {
        let mut g = self.inner.lock().unwrap();
        if *g < self.max { *g += 1; }
        self.cond.notify_one();
    }
    fn available(&self) -> u32 {
        *self.inner.lock().unwrap()
    }
}

// =========================================================
// try_acquire on a permit-rich semaphore (uncontended hot path)
// =========================================================

fn try_acquire_uncontended(c: &mut Criterion) {
    let base = tmp_base("try");
    let sem = SharedSemaphore::create(&base, 1_000_000, 1_000_000).unwrap();
    c.bench_function("sem.try_acquire/mmf", |b| {
        b.iter(|| {
            let p = sem.try_acquire();
            black_box(&p);
            // Forget the permit to avoid release's atomic_add cost
            // dominating the bench; we want to measure the CAS only.
            if let Ok(permit) = p { std::mem::forget(permit); }
        });
    });
    // Reset count for clean teardown.
    drop(sem);
    cleanup_base(&base);

    let m = MutexSem::new(1_000_000, 1_000_000);
    c.bench_function("sem.try_acquire/mutex_condvar", |b| {
        b.iter(|| black_box(m.try_acquire()));
    });
}

// =========================================================
// acquire+drop (full RAII cycle, uncontended)
// =========================================================

fn acquire_release_uncontended(c: &mut Criterion) {
    let base = tmp_base("ar");
    let sem = SharedSemaphore::create(&base, 1, 1).unwrap();
    c.bench_function("sem.acquire_release/mmf", |b| {
        b.iter(|| {
            let _p = sem.try_acquire().unwrap();
            // _p drops here, releasing
        });
    });
    drop(sem);
    cleanup_base(&base);

    let m = MutexSem::new(1, 1);
    c.bench_function("sem.acquire_release/mutex_condvar", |b| {
        b.iter(|| {
            m.try_acquire();
            m.release();
        });
    });
}

// =========================================================
// Observer: available()
// =========================================================

fn observe_available(c: &mut Criterion) {
    let base = tmp_base("obs");
    let sem = SharedSemaphore::create(&base, 7, 16).unwrap();
    c.bench_function("sem.available/mmf", |b| {
        b.iter(|| black_box(sem.available()));
    });
    drop(sem);
    cleanup_base(&base);

    let m = MutexSem::new(7, 16);
    c.bench_function("sem.available/mutex_condvar", |b| {
        b.iter(|| black_box(m.available()));
    });
}

// Multi-thread contended acquire correctness is covered by
// source-level unit tests. A per-iter thread::spawn microbench
// is dominated by Windows thread-creation cost.

criterion_group!(benches,
    try_acquire_uncontended,
    acquire_release_uncontended,
    observe_available,
);
criterion_main!(benches);
