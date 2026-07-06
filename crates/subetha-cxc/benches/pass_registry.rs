//! Bench: pass_registry execute / register / is_registered vs
//! a direct closure call and a `Mutex<HashMap>` registry variant.
//!
//! The pass_registry is purely in-process; its cross-process
//! value is the dispatch CONVENTION (Pass { id, args } shipped
//! between processes; each process has its own registry). This
//! bench measures the lookup + dispatch overhead vs the
//! underlying closure-call cost, and vs a Mutex-based registry
//! to validate the RwLock choice.
//!
//! Workloads:
//! - execute (RwLock-read + HashMap lookup + dispatch)
//! - direct closure call (baseline: pure dispatch with no lookup)
//! - Mutex<HashMap> registry (slower read-side baseline)
//! - register (RwLock-write + HashMap insert)
//! - is_registered (RwLock-read + HashMap contains)

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Mutex, RwLock};

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::pass_registry::{execute, register, unregister, is_registered, Pass, PassResult};

// Naive Mutex<HashMap> registry - same logical shape, mutex
// instead of RwLock. The closures stored cannot be `dyn Fn` with
// the same Send + Sync bounds inside an arbitrary contender, so
// we use the same handler signature.
type Handler = Box<dyn Fn(&[u8]) -> PassResult + Send + Sync + 'static>;

struct MutexRegistry { handlers: Mutex<HashMap<u32, Handler>> }
struct RwLockRegistry { handlers: RwLock<HashMap<u32, Handler>> }

impl MutexRegistry {
    fn new() -> Self { Self { handlers: Mutex::new(HashMap::new()) } }
    fn register(&self, id: u32, f: Handler) {
        self.handlers.lock().unwrap().insert(id, f);
    }
    fn execute(&self, pass: &Pass) -> PassResult {
        let g = self.handlers.lock().unwrap();
        match g.get(&pass.closure_id) {
            Some(h) => h(&pass.args),
            None => Err(subetha_cxc::pass_registry::PassError::UnknownClosureId(pass.closure_id)),
        }
    }
}

impl RwLockRegistry {
    fn new() -> Self { Self { handlers: RwLock::new(HashMap::new()) } }
    fn register(&self, id: u32, f: Handler) {
        self.handlers.write().unwrap().insert(id, f);
    }
    fn execute(&self, pass: &Pass) -> PassResult {
        let g = self.handlers.read().unwrap();
        match g.get(&pass.closure_id) {
            Some(h) => h(&pass.args),
            None => Err(subetha_cxc::pass_registry::PassError::UnknownClosureId(pass.closure_id)),
        }
    }
}

// =========================================================
// Per-bench-unique IDs to avoid cross-contamination with other
// benches sharing the static REGISTRY.
// =========================================================

const BENCH_ID_EXEC_HOT_BASE: u32 = 0x9000_0000;
const BENCH_ID_REGISTER: u32 = 0x9000_1000;
const BENCH_ID_IS_REG: u32 = 0x9000_2000;

// =========================================================
// execute hot path (RwLock-read + HashMap lookup + dispatch)
// =========================================================

fn execute_hot(c: &mut Criterion) {
    // Pre-populate 10 closures in the global registry.
    for i in 0..10u32 {
        register(BENCH_ID_EXEC_HOT_BASE + i, move |args| {
            Ok(args.iter().map(|b| b.wrapping_add(i as u8)).collect())
        });
    }
    let pass = Pass {
        closure_id: BENCH_ID_EXEC_HOT_BASE + 5,
        args: b"hello".to_vec(),
    };
    c.bench_function("pass_registry.execute/global", |b| {
        b.iter(|| black_box(execute(black_box(&pass)).unwrap()));
    });
    for i in 0..10u32 { unregister(BENCH_ID_EXEC_HOT_BASE + i); }

    // Baseline: direct closure call (no registry lookup).
    let direct: Handler = Box::new(|args| {
        Ok(args.iter().map(|b| b.wrapping_add(5)).collect())
    });
    c.bench_function("pass_registry.direct_call/baseline", |b| {
        b.iter(|| black_box(direct(black_box(b"hello")).unwrap()));
    });

    // Local RwLock registry (same protocol as the global static).
    let rwl = RwLockRegistry::new();
    for i in 0..10u32 {
        rwl.register(BENCH_ID_EXEC_HOT_BASE + i, Box::new(move |args| {
            Ok(args.iter().map(|b| b.wrapping_add(i as u8)).collect())
        }));
    }
    c.bench_function("pass_registry.execute/local_rwlock", |b| {
        b.iter(|| black_box(rwl.execute(black_box(&pass)).unwrap()));
    });

    // Mutex<HashMap> baseline.
    let m = MutexRegistry::new();
    for i in 0..10u32 {
        m.register(BENCH_ID_EXEC_HOT_BASE + i, Box::new(move |args| {
            Ok(args.iter().map(|b| b.wrapping_add(i as u8)).collect())
        }));
    }
    c.bench_function("pass_registry.execute/local_mutex", |b| {
        b.iter(|| black_box(m.execute(black_box(&pass)).unwrap()));
    });
}

// =========================================================
// register (RwLock-write + HashMap insert)
// =========================================================

fn register_hot(c: &mut Criterion) {
    let mut next_id = BENCH_ID_REGISTER;
    c.bench_function("pass_registry.register/global", |b| {
        b.iter(|| {
            let id = next_id;
            next_id = next_id.wrapping_add(1);
            register(id, |args| Ok(args.to_vec()));
            unregister(id);
        });
    });
}

// =========================================================
// is_registered (RwLock-read + HashMap contains)
// =========================================================

fn is_registered_hot(c: &mut Criterion) {
    let id = BENCH_ID_IS_REG;
    register(id, |args| Ok(args.to_vec()));
    c.bench_function("pass_registry.is_registered/global", |b| {
        b.iter(|| black_box(is_registered(black_box(id))));
    });
    unregister(id);
}

criterion_group!(benches,
    execute_hot,
    register_hot,
    is_registered_hot,
);
criterion_main!(benches);
