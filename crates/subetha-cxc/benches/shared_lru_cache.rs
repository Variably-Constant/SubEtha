//! Bench: SharedLRUCache vs Mutex<(HashMap, VecDeque)> textbook
//! in-process LRU baseline.
//!
//! Architectural claim: the composite primitive matches in-process
//! LRU on the hot read path AND provides cross-process visibility.
//! Writer-side ops (put / touch) cost more because we update two
//! underlying primitives; reads are competitive.
//!
//! Workloads:
//! - get hot (lock-free read)
//! - put new (insert that doesn't evict)
//! - get_and_touch (read + promote, the strict-LRU path)
//! - put with eviction (full cache + new key)

use std::collections::{HashMap, VecDeque};
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedLRUCache;

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-lru-{name}-{pid}"));
    p
}

fn cleanup(base: &std::path::Path) {
    let stem = base.file_name().unwrap().to_string_lossy().to_string();
    for ext in ["map", "list"] {
        let mut p = base.to_path_buf();
        p.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&p).ok();
    }
}

// Textbook in-process LRU: HashMap + VecDeque (recency order) inside a Mutex.
struct MutexLRU {
    inner: Mutex<MutexLRUInner>,
    capacity: usize,
}
struct MutexLRUInner {
    map: HashMap<u32, u32>,
    order: VecDeque<u32>,  // front = MRU, back = LRU
}

impl MutexLRU {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(MutexLRUInner {
                map: HashMap::with_capacity(capacity),
                order: VecDeque::with_capacity(capacity),
            }),
            capacity,
        }
    }
    fn get(&self, k: u32) -> Option<u32> {
        self.inner.lock().unwrap().map.get(&k).copied()
    }
    fn get_and_touch(&self, k: u32) -> Option<u32> {
        let mut g = self.inner.lock().unwrap();
        let v = g.map.get(&k).copied()?;
        // Move k to front of order.
        if let Some(pos) = g.order.iter().position(|&x| x == k) {
            g.order.remove(pos);
            g.order.push_front(k);
        }
        Some(v)
    }
    fn put(&self, k: u32, v: u32) {
        let mut g = self.inner.lock().unwrap();
        if let std::collections::hash_map::Entry::Occupied(mut e) = g.map.entry(k) {
            e.insert(v);
            if let Some(pos) = g.order.iter().position(|&x| x == k) {
                g.order.remove(pos);
            }
            g.order.push_front(k);
            return;
        }
        if g.map.len() >= self.capacity
            && let Some(evict) = g.order.pop_back() {
                g.map.remove(&evict);
            }
        g.map.insert(k, v);
        g.order.push_front(k);
    }
}

// =========================================================
// get hot (no promote)
// =========================================================

fn get_hot(c: &mut Criterion) {
    let base = tmp_base("get");
    let cache: SharedLRUCache<u32, u32>
        = SharedLRUCache::create(&base, 1000).unwrap();
    for k in 0..1000u32 { cache.put(k, k * 10).unwrap(); }
    c.bench_function("lru.get/mmf", |b| {
        b.iter(|| black_box(cache.get(&black_box(500u32))));
    });
    drop(cache);
    cleanup(&base);

    let m = MutexLRU::new(1000);
    for k in 0..1000u32 { m.put(k, k * 10); }
    c.bench_function("lru.get/mutex_hashmap_vecdeque", |b| {
        b.iter(|| black_box(m.get(black_box(500u32))));
    });
}

// =========================================================
// put new (no eviction)
// =========================================================

fn put_new(c: &mut Criterion) {
    // SharedHashMap is sized 8x the LRU capacity to absorb eviction
    // tombstones, so the safe insert budget per cache instance is
    // roughly 7x capacity (per the `Long-running workload limit`
    // docstring on SharedLRUCache). At capacity 100_000 that is
    // ~700_000 inserts before tombstones fill the map and per-insert
    // cost explodes from ~14 ns to ~ms while insert_inner walks every
    // slot. Criterion at publication-grade defaults on fast silicon
    // measures hundreds of millions of iterations, so we must
    // recreate the cache periodically within the iter_custom loop.
    c.bench_function("lru.put/mmf", |b| {
        let base = tmp_base("put-new");
        // Per-window iter cap well under the tombstone budget so the
        // map stays in the fast linear-probe regime.
        const CAP_PER_WINDOW: u32 = 100_000;
        b.iter_custom(|iters| {
            use std::time::{Duration, Instant};
            let mut total = Duration::ZERO;
            let mut remaining = iters as u32;
            let mut k: u32 = 0;
            while remaining > 0 {
                let cache: SharedLRUCache<u32, u32>
                    = SharedLRUCache::create(&base, 100_000).unwrap();
                let batch = remaining.min(CAP_PER_WINDOW);
                let start = Instant::now();
                for _ in 0..batch {
                    k = k.wrapping_add(1);
                    cache.put(black_box(k), black_box(42)).ok();
                }
                total += start.elapsed();
                drop(cache);
                cleanup(&base);
                remaining = remaining.saturating_sub(batch);
            }
            total
        });
    });

    let m = MutexLRU::new(100_000);
    let mut k = 0u32;
    c.bench_function("lru.put/mutex_hashmap_vecdeque", |b| {
        b.iter(|| {
            k = k.wrapping_add(1);
            m.put(black_box(k), black_box(42));
        });
    });
}

// =========================================================
// get_and_touch (read + promote)
// =========================================================

fn get_and_touch(c: &mut Criterion) {
    let base = tmp_base("touch");
    let cache: SharedLRUCache<u32, u32>
        = SharedLRUCache::create(&base, 100).unwrap();
    for k in 0..100u32 { cache.put(k, k * 10).unwrap(); }
    c.bench_function("lru.get_and_touch/mmf", |b| {
        b.iter(|| black_box(cache.get_and_touch(&black_box(50u32))));
    });
    drop(cache);
    cleanup(&base);

    let m = MutexLRU::new(100);
    for k in 0..100u32 { m.put(k, k * 10); }
    c.bench_function("lru.get_and_touch/mutex_hashmap_vecdeque", |b| {
        b.iter(|| black_box(m.get_and_touch(black_box(50u32))));
    });
}

// =========================================================
// put with eviction (full cache, new key)
//
// NOTE: bounded total puts to stay within SharedHashMap tombstone
// budget. Each iteration is a put-then-evict cycle.
// =========================================================

fn put_with_eviction(c: &mut Criterion) {
    c.bench_function("lru.put_evict/mmf", |b| {
        let base = tmp_base("put-evict");
        // Capacity 10 -> 8x = 80 map slots -> ~70 evictions per cache
        // before the underlying SharedHashMap fills with tombstones
        // and per-insert cost explodes. Recreate the cache each
        // window so the bench stays in the fast linear-probe regime.
        const CAP_PER_WINDOW: u64 = 50;
        b.iter_custom(|iters| {
            use std::time::{Duration, Instant};
            let mut remaining = iters.max(1);
            let mut total = Duration::ZERO;
            let mut k = 100u32;
            while remaining > 0 {
                let cache: SharedLRUCache<u32, u32>
                    = SharedLRUCache::create(&base, 10).unwrap();
                for k0 in 0..10u32 { cache.put(k0, k0).unwrap(); }
                let batch = remaining.min(CAP_PER_WINDOW);
                let start = Instant::now();
                for _ in 0..batch {
                    k = k.wrapping_add(1);
                    cache.put(black_box(k), black_box(k)).ok();
                }
                total += start.elapsed();
                drop(cache);
                cleanup(&base);
                remaining = remaining.saturating_sub(batch);
            }
            total
        });
    });

    c.bench_function("lru.put_evict/mutex_hashmap_vecdeque", |b| {
        let m = MutexLRU::new(10);
        for k in 0..10u32 { m.put(k, k); }
        let mut k = 100u32;
        b.iter(|| {
            k = k.wrapping_add(1);
            m.put(black_box(k), black_box(k));
        });
    });
}

criterion_group!(benches,
    get_hot,
    put_new,
    get_and_touch,
    put_with_eviction,
);
criterion_main!(benches);
