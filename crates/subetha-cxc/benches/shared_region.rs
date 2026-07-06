//! Bench: SharedRegion<T> vs `Mutex<Vec<T>> + Mutex<Vec<usize>>`
//! (the textbook in-process typed-arena + free-list pattern).
//!
//! Architectural claim: SharedRegion provides typed cross-process
//! arena allocation at lock-free fetch_add + Treiber-CAS cost.
//! The Mutex-based baseline pays Mutex lock+unlock per allocate
//! AND per free. Plus only the MMF version provides position-
//! independent OffsetPtr<T> resolvable across processes.
//!
//! Workloads:
//! - allocate hot (bump path, uncontended)
//! - allocate + free cycle (free-list reuse)
//! - get hot (resolve a known OffsetPtr)
//! - 4-thread concurrent allocate
//! - 4-thread concurrent allocate + free mix

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedRegion;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-region-{name}-{pid}.bin"));
    p
}

// =========================================================
// In-process arena baseline: Mutex<Vec<T>> + Mutex<Vec<usize>>
// =========================================================

struct MutexArena<T: Copy + Default> {
    slots: Mutex<Vec<T>>,
    free_list: Mutex<Vec<usize>>,
}

impl<T: Copy + Default> MutexArena<T> {
    fn new(capacity: usize) -> Self {
        Self {
            slots: Mutex::new(Vec::with_capacity(capacity)),
            free_list: Mutex::new(Vec::with_capacity(capacity)),
        }
    }
    fn allocate(&self, v: T) -> usize {
        // Try free list first.
        if let Some(idx) = self.free_list.lock().unwrap().pop() {
            self.slots.lock().unwrap()[idx] = v;
            return idx;
        }
        // Bump alloc.
        let mut s = self.slots.lock().unwrap();
        s.push(v);
        s.len() - 1
    }
    fn free(&self, idx: usize) -> T {
        let v = self.slots.lock().unwrap()[idx];
        self.free_list.lock().unwrap().push(idx);
        v
    }
    fn get(&self, idx: usize) -> T {
        self.slots.lock().unwrap()[idx]
    }
    fn clear(&self) {
        self.slots.lock().unwrap().clear();
        self.free_list.lock().unwrap().clear();
    }
}

// =========================================================
// allocate hot (bump path)
// =========================================================

fn allocate_hot(c: &mut Criterion) {
    let p = tmp("alloc");
    // Cap of 4096 is enough for one b.iter call (1 allocate per iter)
    // with clear() reset between batches via iter_batched. .expect()
    // panics on overflow rather than silently returning Err.
    let r: SharedRegion<u64> = SharedRegion::create(&p, 4096).unwrap();
    c.bench_function("region.allocate/mmf", |b| {
        b.iter_batched(
            || r.clear(),
            |_| {
                black_box(r.allocate(black_box(42)).expect("region overflow"))
            },
            criterion::BatchSize::PerIteration,
        );
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let a: MutexArena<u64> = MutexArena::new(4096);
    c.bench_function("region.allocate/mutex_arena", |b| {
        b.iter_batched(
            || a.clear(),
            |_| black_box(a.allocate(black_box(42))),
            criterion::BatchSize::PerIteration,
        );
    });
}

// =========================================================
// alloc + free cycle (free-list path)
// =========================================================

fn alloc_free_cycle(c: &mut Criterion) {
    let p = tmp("cycle");
    let r: SharedRegion<u64> = SharedRegion::create(&p, 1024).unwrap();
    c.bench_function("region.alloc_free_cycle/mmf", |b| {
        b.iter(|| {
            let ptr = r.allocate(black_box(7)).unwrap();
            black_box(r.free(ptr).unwrap())
        });
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let a: MutexArena<u64> = MutexArena::new(1024);
    c.bench_function("region.alloc_free_cycle/mutex_arena", |b| {
        b.iter(|| {
            let idx = a.allocate(black_box(7));
            black_box(a.free(idx))
        });
    });
}

// =========================================================
// get hot (resolve known ptr)
// =========================================================

fn get_hot(c: &mut Criterion) {
    let p = tmp("get");
    let r: SharedRegion<u64> = SharedRegion::create(&p, 16).unwrap();
    let ptr = r.allocate(0xDEAD_BEEF).unwrap();
    c.bench_function("region.get/mmf", |b| {
        b.iter(|| black_box(r.get(black_box(ptr)).unwrap()));
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let a: MutexArena<u64> = MutexArena::new(16);
    let idx = a.allocate(0xDEAD_BEEF);
    c.bench_function("region.get/mutex_arena", |b| {
        b.iter(|| black_box(a.get(black_box(idx))));
    });
}

// Multi-thread allocate correctness is covered by source-level
// unit tests. A per-iter thread::spawn microbench (even inside
// iter_batched) is dominated by Windows thread-creation cost.

criterion_group!(benches,
    allocate_hot,
    alloc_free_cycle,
    get_hot,
);
criterion_main!(benches);
