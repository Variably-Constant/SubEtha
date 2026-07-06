//! Bench: SharedReservoirSampler vs Mutex<Vec<T>> with manual
//! Vitter's algorithm.

use std::cell::Cell;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedReservoirSampler;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-reservoir-{name}-{pid}.bin"));
    p
}

thread_local! {
    static RNG: Cell<u64> = Cell::new({
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        if t == 0 { 1 } else { t }
    });
}

fn next_rand() -> u64 {
    RNG.with(|c| {
        let mut x = c.get();
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        c.set(x);
        x
    })
}

struct MutexReservoir<T> {
    inner: Mutex<MutexReservoirInner<T>>,
    capacity: usize,
}
struct MutexReservoirInner<T> {
    slots: Vec<T>,
    n_seen: u64,
}

impl<T: Copy + Default> MutexReservoir<T> {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(MutexReservoirInner {
                slots: vec![T::default(); capacity],
                n_seen: 0,
            }),
            capacity,
        }
    }
    fn record(&self, v: T) {
        let mut g = self.inner.lock().unwrap();
        g.n_seen += 1;
        let n_seen = g.n_seen;
        if n_seen <= self.capacity as u64 {
            let idx = (n_seen - 1) as usize;
            g.slots[idx] = v;
        } else {
            let j = (next_rand() % n_seen) + 1;
            if j <= self.capacity as u64 {
                let idx = (j - 1) as usize;
                g.slots[idx] = v;
            }
        }
    }
}

fn record_under_capacity(c: &mut Criterion) {
    let p = tmp("under-cap");
    let r: SharedReservoirSampler<u64> = SharedReservoirSampler::create(&p, 1_000_000).unwrap();
    c.bench_function("reservoir.record_under_cap/mmf", |b| {
        b.iter(|| black_box(r.record(black_box(42))));
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let m: MutexReservoir<u64> = MutexReservoir::new(1_000_000);
    c.bench_function("reservoir.record_under_cap/mutex_vec", |b| {
        b.iter(|| m.record(black_box(42)));
    });
}

fn record_over_capacity(c: &mut Criterion) {
    let p = tmp("over-cap");
    let r: SharedReservoirSampler<u64> = SharedReservoirSampler::create(&p, 100).unwrap();
    // Pre-fill so subsequent records hit the probabilistic path.
    for i in 0..100u64 { r.record(i); }
    c.bench_function("reservoir.record_over_cap/mmf", |b| {
        b.iter(|| black_box(r.record(black_box(42))));
    });
    drop(r);
    std::fs::remove_file(&p).ok();

    let m: MutexReservoir<u64> = MutexReservoir::new(100);
    for i in 0..100u64 { m.record(i); }
    c.bench_function("reservoir.record_over_cap/mutex_vec", |b| {
        b.iter(|| m.record(black_box(42)));
    });
}

fn snapshot_100(c: &mut Criterion) {
    let p = tmp("snap");
    let r: SharedReservoirSampler<u64> = SharedReservoirSampler::create(&p, 100).unwrap();
    for i in 0..100u64 { r.record(i); }
    c.bench_function("reservoir.snapshot_100/mmf", |b| {
        b.iter(|| black_box(r.snapshot()));
    });
    drop(r);
    std::fs::remove_file(&p).ok();
}

criterion_group!(benches, record_under_capacity, record_over_capacity, snapshot_100);
criterion_main!(benches);
