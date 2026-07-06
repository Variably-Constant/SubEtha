//! Bench: SharedGraph vs Mutex<HashMap<u32, Vec<u32>>> (textbook
//! adjacency-list graph baseline).

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{NodeIndex, SharedGraph};

fn tmp_base(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-graph-{name}-{pid}"));
    p
}

fn cleanup(base: &std::path::Path) {
    let stem = base.file_name().unwrap().to_string_lossy().to_string();
    for ext in ["nodes", "edges"] {
        let mut p = base.to_path_buf();
        p.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&p).ok();
    }
}

fn add_node(c: &mut Criterion) {
    // SharedGraph has no clear(), so use file-recreate-per-iter in
    // setup to bound the node region's growth across criterion's
    // millions of closure invocations. The .expect() panics on
    // overflow rather than silently returning Err.
    let base = tmp_base("add-node");
    c.bench_function("graph.add_node/mmf", |b| {
        b.iter_batched(
            || {
                cleanup(&base);
                SharedGraph::<u32, u32>::create(&base, 4096, 8).unwrap()
            },
            |g| {
                g.add_node(black_box(42)).expect("graph node overflow");
            },
            criterion::BatchSize::PerIteration,
        );
    });
    cleanup(&base);

    // Match the mmf side's growth pattern: start small, grow
    // naturally via rehash. Pre-allocating a large capacity would
    // page-fault on random-hash inserts and inflate the baseline.
    c.bench_function("graph.add_node/mutex_hashmap", |b| {
        b.iter_batched(
            || Mutex::new(HashMap::<u32, Vec<u32>>::with_capacity(16)),
            |m| {
                m.lock().unwrap().insert(black_box(42), Vec::new());
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

fn add_edge(c: &mut Criterion) {
    // Same pattern: recreate per iter to bound state. The
    // .expect() panics on edge-region overflow rather than
    // silently returning Err.
    let base = tmp_base("add-edge");
    c.bench_function("graph.add_edge/mmf", |b| {
        b.iter_batched(
            || {
                cleanup(&base);
                let g: SharedGraph<u32, u32> =
                    SharedGraph::create(&base, 4, 4096).unwrap();
                let src = g.add_node(0).unwrap();
                let dst = g.add_node(1).unwrap();
                (g, src, dst)
            },
            |(g, src, dst)| {
                g.add_edge(src, dst, black_box(42)).expect("graph edge overflow");
            },
            criterion::BatchSize::PerIteration,
        );
    });
    cleanup(&base);

    let m: Mutex<HashMap<u32, Vec<(u32, u32)>>> = Mutex::new({
        let mut h = HashMap::new();
        h.insert(0u32, Vec::new());
        h
    });
    c.bench_function("graph.add_edge/mutex_hashmap", |b| {
        b.iter(|| {
            let mut g = m.lock().unwrap();
            g.get_mut(&0).unwrap().push((1, black_box(42)));
        });
    });
}

fn neighbors_walk(c: &mut Criterion) {
    let base = tmp_base("neighbors");
    let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 100, 1024).unwrap();
    let src = g.add_node(0).unwrap();
    let dsts: Vec<NodeIndex<u32>> = (1..=50).map(|i| g.add_node(i).unwrap()).collect();
    for (i, &d) in dsts.iter().enumerate() {
        g.add_edge(src, d, i as u32).unwrap();
    }
    c.bench_function("graph.neighbors_50/mmf", |b| {
        b.iter(|| black_box(g.neighbors(src)));
    });
    drop(g);
    cleanup(&base);

    let m: Mutex<HashMap<u32, Vec<(u32, u32)>>> = Mutex::new({
        let mut h = HashMap::new();
        h.insert(0u32, (1..=50).map(|i| (i, i)).collect());
        h
    });
    c.bench_function("graph.neighbors_50/mutex_hashmap", |b| {
        b.iter(|| {
            let g = m.lock().unwrap();
            black_box(g.get(&0).cloned().unwrap())
        });
    });
}

criterion_group!(benches, add_node, add_edge, neighbors_walk);
criterion_main!(benches);
