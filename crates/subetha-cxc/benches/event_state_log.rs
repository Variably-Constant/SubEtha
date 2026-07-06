//! Bench: EventStateLog<Event, State> vs the naive in-process
//! event-sourcing shape (`Mutex<VecDeque<Event>>` for the log +
//! `Mutex<State>` for the materialized view).
//!
//! The architectural claim: EventStateLog gives you cross-process
//! event-sourcing AND disk persistence (the ring file IS the durable
//! log) at lock-free MMF cost; the naive in-process baseline gives
//! you neither. This bench shows the *single-process* cost difference
//! so we can quantify what the user pays / gains for the additional
//! capabilities.
//!
//! Three workloads:
//! - emit single event (hot producer path)
//! - read_current snapshot (hot reader path)
//! - emit N then drain_and_fold N (full cycle)

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::EventStateLog;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-eventlog-{name}-{pid}"));
    p
}

// =========================================================
// Naive in-process event-sourcing baseline
// =========================================================

struct NaiveEventStateLog<E, S> {
    log: Mutex<VecDeque<E>>,
    state: Mutex<S>,
}

impl<E: Copy, S: Copy> NaiveEventStateLog<E, S> {
    fn new(initial: S) -> Self {
        Self {
            log: Mutex::new(VecDeque::with_capacity(256)),
            state: Mutex::new(initial),
        }
    }
    fn emit(&self, ev: E) {
        self.log.lock().unwrap().push_back(ev);
    }
    fn read_current(&self) -> S {
        *self.state.lock().unwrap()
    }
    fn drain_and_fold<F: FnMut(&mut S, &E)>(&self, mut f: F) -> usize {
        let mut log = self.log.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        let mut n = 0;
        while let Some(ev) = log.pop_front() {
            f(&mut *state, &ev);
            n += 1;
        }
        n
    }
}

// =========================================================
// emit (hot producer path)
// =========================================================

fn emit_single(c: &mut Criterion) {
    let path = tmp("emit");
    let log: EventStateLog<u32, u64> = EventStateLog::create(&path, 4096, 0).unwrap();
    c.bench_function("eventlog.emit/mmf", |b| {
        b.iter(|| {
            if log.emit(black_box(7)).is_err() {
                // drain when full to keep producer hot
                log.drain_and_fold(|s, e| *s += *e as u64);
            }
        });
    });
    drop(log);
    std::fs::remove_file(format!("{}.events.bin", path.display())).ok();
    std::fs::remove_file(format!("{}.state.bin", path.display())).ok();

    let naive: NaiveEventStateLog<u32, u64> = NaiveEventStateLog::new(0);
    c.bench_function("eventlog.emit/mutex_vecdeque", |b| {
        b.iter(|| naive.emit(black_box(7)));
    });
}

// =========================================================
// read_current (hot reader path)
// =========================================================

fn read_current(c: &mut Criterion) {
    let path = tmp("read");
    let log: EventStateLog<u32, u64> = EventStateLog::create(&path, 16, 4242).unwrap();
    c.bench_function("eventlog.read_current/mmf_seqlock", |b| {
        b.iter(|| black_box(log.read_current()));
    });
    drop(log);
    std::fs::remove_file(format!("{}.events.bin", path.display())).ok();
    std::fs::remove_file(format!("{}.state.bin", path.display())).ok();

    let naive: NaiveEventStateLog<u32, u64> = NaiveEventStateLog::new(4242);
    c.bench_function("eventlog.read_current/mutex_state", |b| {
        b.iter(|| black_box(naive.read_current()));
    });
}

// =========================================================
// Full cycle: emit N + drain_and_fold N
// =========================================================

fn emit_then_drain(c: &mut Criterion) {
    const N: u32 = 64;

    let path = tmp("cycle");
    let log: EventStateLog<u32, u64> = EventStateLog::create(&path, 256, 0).unwrap();
    c.bench_function("eventlog.cycle_64/mmf", |b| {
        b.iter(|| {
            for i in 0..N {
                log.emit(black_box(i)).unwrap();
            }
            let n = log.drain_and_fold(|s, e| *s += *e as u64);
            black_box(n);
        });
    });
    drop(log);
    std::fs::remove_file(format!("{}.events.bin", path.display())).ok();
    std::fs::remove_file(format!("{}.state.bin", path.display())).ok();

    let naive: NaiveEventStateLog<u32, u64> = NaiveEventStateLog::new(0);
    c.bench_function("eventlog.cycle_64/mutex_vecdeque", |b| {
        b.iter(|| {
            for i in 0..N {
                naive.emit(black_box(i));
            }
            let n = naive.drain_and_fold(|s, e| *s += *e as u64);
            black_box(n);
        });
    });
}

// Multi-threaded contention is covered by the
// `concurrent_producers_drain_correctly` unit test in
// `crates/subetha-cxc/src/event_state_log.rs`; a microbench of
// concurrent emit+read would be dominated by per-iter Windows
// thread-spawn cost (~50-100 us) rather than the protocol cost.

criterion_group!(benches,
    emit_single,
    read_current,
    emit_then_drain,
);
criterion_main!(benches);
