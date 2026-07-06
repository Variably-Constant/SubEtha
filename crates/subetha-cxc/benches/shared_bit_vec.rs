//! Bench: SharedBitVec vs Mutex<Vec<bool>> (the naive baseline) and
//! Mutex<Vec<u64>> (the manual bit-packed baseline).
//!
//! Architectural claim: SharedBitVec is faster than both AND
//! provides cross-process visibility neither baseline can match.
//! The win comes from:
//! - Atomic per-word RMW (fetch_or / fetch_and) instead of Mutex
//!   lock/unlock per bit.
//! - 1 bit per slot vs 1 byte for Vec<bool> (8x storage density).
//! - Cache-friendly contiguous u64 layout.
//!
//! Workloads:
//! - set hot (single-bit write)
//! - get hot (single-bit read)
//! - count_ones over 1024-bit vector
//! - storage density witness
//!
//! Concurrent-set throughput is covered by the
//! `concurrent_setters_of_same_word_distinct_bits_all_visible`
//! unit test in `crates/subetha-cxc/src/shared_bit_vec.rs`; a
//! microbench would be dominated by Windows thread-spawn cost
//! (~50-100 us per iter) rather than the bit-set work, so this
//! bench keeps the workload single-threaded for a clean signal.

use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::SharedBitVec;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-bitvec-{name}-{pid}.bin"));
    p
}

// Baseline: naive Vec<bool> behind a Mutex.
struct MutexBoolVec {
    inner: Mutex<Vec<bool>>,
}
impl MutexBoolVec {
    fn new(n: usize) -> Self { Self { inner: Mutex::new(vec![false; n]) } }
    fn set(&self, i: usize) { self.inner.lock().unwrap()[i] = true; }
    fn get(&self, i: usize) -> bool { self.inner.lock().unwrap()[i] }
    fn count_ones(&self) -> usize {
        self.inner.lock().unwrap().iter().filter(|&&b| b).count()
    }
}

// Baseline: manual bit-packed Vec<u64> behind a Mutex.
struct MutexBitPacked {
    words: Mutex<Vec<u64>>,
}
impl MutexBitPacked {
    fn new(n_bits: usize) -> Self {
        Self { words: Mutex::new(vec![0u64; n_bits.div_ceil(64)]) }
    }
    fn set(&self, i: usize) {
        let mut g = self.words.lock().unwrap();
        g[i / 64] |= 1u64 << (i % 64);
    }
    fn get(&self, i: usize) -> bool {
        let g = self.words.lock().unwrap();
        (g[i / 64] & (1u64 << (i % 64))) != 0
    }
    fn count_ones(&self) -> usize {
        self.words.lock().unwrap().iter().map(|w| w.count_ones() as usize).sum()
    }
}

// =========================================================
// set hot
// =========================================================

fn set_hot(c: &mut Criterion) {
    let p = tmp("set");
    let b = SharedBitVec::create(&p, 1 << 16).unwrap();
    let mut i = 0usize;
    c.bench_function("bitvec.set/mmf", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) & 0xFFFF;
            b.set(black_box(i)).unwrap()
        });
    });
    drop(b);
    std::fs::remove_file(&p).ok();

    let m = MutexBoolVec::new(1 << 16);
    let mut i = 0usize;
    c.bench_function("bitvec.set/mutex_vec_bool", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) & 0xFFFF;
            m.set(black_box(i))
        });
    });

    let p = MutexBitPacked::new(1 << 16);
    let mut i = 0usize;
    c.bench_function("bitvec.set/mutex_bit_packed", |b_iter| {
        b_iter.iter(|| {
            i = (i + 1) & 0xFFFF;
            p.set(black_box(i))
        });
    });
}

// =========================================================
// get hot
// =========================================================

fn get_hot(c: &mut Criterion) {
    let p = tmp("get");
    let b = SharedBitVec::create(&p, 1024).unwrap();
    b.set(500).unwrap();
    c.bench_function("bitvec.get/mmf", |b_iter| {
        b_iter.iter(|| black_box(b.get(black_box(500)).unwrap()));
    });
    drop(b);
    std::fs::remove_file(&p).ok();

    let m = MutexBoolVec::new(1024);
    c.bench_function("bitvec.get/mutex_vec_bool", |b_iter| {
        b_iter.iter(|| black_box(m.get(black_box(500))));
    });

    let p = MutexBitPacked::new(1024);
    c.bench_function("bitvec.get/mutex_bit_packed", |b_iter| {
        b_iter.iter(|| black_box(p.get(black_box(500))));
    });
}

// =========================================================
// count_ones over 1024-bit vector
// =========================================================

fn count_ones_1024(c: &mut Criterion) {
    let p = tmp("count");
    let b = SharedBitVec::create(&p, 1024).unwrap();
    for i in (0..1024).step_by(3) { b.set(i).unwrap(); }
    c.bench_function("bitvec.count_ones_1024/mmf", |b_iter| {
        b_iter.iter(|| black_box(b.count_ones()));
    });
    drop(b);
    std::fs::remove_file(&p).ok();

    let m = MutexBoolVec::new(1024);
    for i in (0..1024).step_by(3) { m.set(i); }
    c.bench_function("bitvec.count_ones_1024/mutex_vec_bool", |b_iter| {
        b_iter.iter(|| black_box(m.count_ones()));
    });

    let pk = MutexBitPacked::new(1024);
    for i in (0..1024).step_by(3) { pk.set(i); }
    c.bench_function("bitvec.count_ones_1024/mutex_bit_packed", |b_iter| {
        b_iter.iter(|| black_box(pk.count_ones()));
    });
}

// =========================================================
// Storage witness
// =========================================================

fn storage_witness(c: &mut Criterion) {
    let n = 1024usize;
    let bool_vec_bytes = n * std::mem::size_of::<bool>();
    let packed_bytes = n.div_ceil(64) * 8;
    eprintln!("[storage] Vec<bool>[{n}] = {bool_vec_bytes} bytes (1 byte per bit)");
    eprintln!("[storage] SharedBitVec(n={n}) words = {packed_bytes} bytes ({}x smaller)",
        bool_vec_bytes as f64 / packed_bytes as f64);
    c.bench_function("bitvec.storage_witness", |b_iter| {
        b_iter.iter(|| black_box(bool_vec_bytes + packed_bytes));
    });
}

criterion_group!(benches,
    set_hot,
    get_hot,
    count_ones_1024,
    storage_witness,
);
criterion_main!(benches);
