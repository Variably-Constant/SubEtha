//! Bench: SharedTopologyMap vs `Mutex<HashMap<(u32,u32), u64>>` -
//! the textbook in-process flow-counter pattern. Both maintain
//! per-edge counts; SharedTopologyMap uses a flat N*N atomic array,
//! the baseline uses a Mutex-protected HashMap.
//!
//! Architectural claim: a flat N*N AtomicU64 array dominates the
//! Mutex<HashMap> pattern on every hot path because:
//! - record_send is one atomic fetch_add vs Mutex lock + hash +
//!   lookup + update + unlock.
//! - fan_out / fan_in are linear scans over a row / column with
//!   O(N) atomic loads; the HashMap baseline needs a Mutex lock for
//!   any coherent view.
//! - The flat layout is contention-free across distinct edges
//!   because each AtomicU64 sits on its own slot (no Mutex
//!   serialisation).
//!
//! Plus the MMF version is cross-process; the in-process baseline
//! is not.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedTopologyMap;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-topo-{name}-{pid}.bin"));
    p
}

// =========================================================
// Baseline: Mutex<HashMap<(u32, u32), u64>>
// =========================================================

struct MutexEdgeCounters {
    inner: Mutex<HashMap<(u32, u32), u64>>,
}

impl MutexEdgeCounters {
    fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }
    fn record_send(&self, src: u32, dst: u32) {
        let mut g = self.inner.lock().unwrap();
        *g.entry((src, dst)).or_insert(0) += 1;
    }
    fn fan_out(&self, src: u32) -> u32 {
        let g = self.inner.lock().unwrap();
        g.keys().filter(|(s, _)| *s == src).count() as u32
    }
    fn recommend(&self, fo_threshold: u32, fi_threshold: u32) -> &'static str {
        let g = self.inner.lock().unwrap();
        let mut max_fo = 0u32;
        let mut max_fi = 0u32;
        let mut by_src: HashMap<u32, u32> = HashMap::new();
        let mut by_dst: HashMap<u32, u32> = HashMap::new();
        for &(s, d) in g.keys() {
            *by_src.entry(s).or_insert(0) += 1;
            *by_dst.entry(d).or_insert(0) += 1;
        }
        for v in by_src.values() { max_fo = max_fo.max(*v); }
        for v in by_dst.values() { max_fi = max_fi.max(*v); }
        if max_fo >= fo_threshold && max_fi >= fi_threshold { "AllToAllMesh" }
        else if max_fo >= fo_threshold { "BroadcastTree" }
        else { "PointToPoint" }
    }
}

// =========================================================
// record_send hot path
// =========================================================

fn record_send_hot(c: &mut Criterion) {
    let p = tmp("record");
    let t = SharedTopologyMap::create(&p, 8).unwrap();
    c.bench_function("topology.record_send/mmf", |b| {
        b.iter(|| t.record_send(black_box(0), black_box(1)).unwrap());
    });
    drop(t);
    std::fs::remove_file(&p).ok();

    let m = MutexEdgeCounters::new();
    c.bench_function("topology.record_send/mutex_hashmap", |b| {
        b.iter(|| m.record_send(black_box(0), black_box(1)));
    });
}

// =========================================================
// fan_out observer
// =========================================================

fn fan_out_observer(c: &mut Criterion) {
    let p = tmp("fan-out");
    let t = SharedTopologyMap::create(&p, 16).unwrap();
    for d in 0..8u32 { t.record_send(0, d).unwrap(); }
    c.bench_function("topology.fan_out/mmf", |b| {
        b.iter(|| black_box(t.fan_out(black_box(0))));
    });
    drop(t);
    std::fs::remove_file(&p).ok();

    let m = MutexEdgeCounters::new();
    for d in 0..8u32 { m.record_send(0, d); }
    c.bench_function("topology.fan_out/mutex_hashmap", |b| {
        b.iter(|| black_box(m.fan_out(black_box(0))));
    });
}

// =========================================================
// recommend (the policy evaluation)
// =========================================================

fn recommend_bench(c: &mut Criterion) {
    let p = tmp("recommend");
    let t = SharedTopologyMap::create(&p, 16).unwrap();
    // Establish a broadcast pattern: node 0 sends to 1..8.
    for d in 1..8u32 { t.record_send(0, d).unwrap(); }
    c.bench_function("topology.recommend/mmf", |b| {
        b.iter(|| black_box(t.recommend()));
    });
    drop(t);
    std::fs::remove_file(&p).ok();

    let m = MutexEdgeCounters::new();
    for d in 1..8u32 { m.record_send(0, d); }
    c.bench_function("topology.recommend/mutex_hashmap", |b| {
        b.iter(|| black_box(m.recommend(3, 3)));
    });
}

// =========================================================
// read_recommendation O(1) header read (after publish)
// =========================================================

fn read_recommendation_bench(c: &mut Criterion) {
    let p = tmp("read");
    let t = SharedTopologyMap::create(&p, 16).unwrap();
    for d in 1..8u32 { t.record_send(0, d).unwrap(); }
    t.publish_recommendation();
    c.bench_function("topology.read_recommendation/mmf", |b| {
        b.iter(|| black_box(t.read_recommendation()));
    });
    drop(t);
    std::fs::remove_file(&p).ok();
}

// Multi-thread record_send correctness is covered by source-level
// unit tests. A per-iter thread::spawn microbench is dominated by
// Windows thread-creation cost.

criterion_group!(benches,
    record_send_hot,
    fan_out_observer,
    recommend_bench,
    read_recommendation_bench,
);
criterion_main!(benches);
