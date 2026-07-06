//! Unified flamegraph driver: exercises every ring + system primitive
//! in one process so a single `perf` / `cargo flamegraph` run shows the
//! whole substrate's hot paths in one tree.
//!
//! Usage:
//!     cargo flamegraph --example unified_flame --release -- <iters_per_primitive>
//!
//! Default iters = 1_500_000 per primitive. Each section creates a
//! primitive once (MMF in $TMPDIR) and loops its core op so the CPU
//! profile is dominated by the primitive's hot path, not setup.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

use subetha_cxc::{
    HeartbeatTable, OwnerLease, SharedAtomicU64, SharedBitVec, SharedBloomFilter,
    SharedBroadcastRing, SharedCell, SharedCountMinSketch, SharedFenceClock, SharedHandleTable,
    SharedHashMap, SharedHistogram, SharedHyperLogLog, SharedLeaderElection, SharedLinkedList,
    SharedLRUCache, SharedOnceCell, SharedRWLock, SharedRateLimiter, SharedRegion,
    SharedReservoirSampler, SharedRing, SharedSemaphore, SharedBTreeMap, SharedStringArena,
    SharedTreiberStack, SharedVec, TaggedOffsetPtr,
};
use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::event_state_log::EventStateLog;

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Consume a `#[must_use]` value without `drop()` (which the compiler
/// flags as a no-op for `Copy` error types) and without `let _`. The
/// side-effecting call still runs; this only discards its result.
#[inline(never)]
fn sink<T>(_v: T) {}

fn tmp(name: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push(format!("uflame_{name}_{}_{n}.bin", std::process::id()));
    p
}

fn section(name: &str, f: impl FnOnce()) {
    let t = Instant::now();
    f();
    eprintln!("  [{name}] {:.0} ms", t.elapsed().as_secs_f64() * 1000.0);
}

fn run_suite(n: u64) {
    bench_rings(n);
    bench_maps(n);
    bench_sketches(n);
    bench_scalars(n);
    bench_locks(n);
    bench_coordination(n);
    bench_storage(n);
    bench_pointers(n);
}

fn main() {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_500_000);
    eprintln!("unified_flame: {n} iters/primitive, pid {}", std::process::id());
    let t0 = Instant::now();
    run_suite(n);
    eprintln!("unified_flame DONE in {:.2}s", t0.elapsed().as_secs_f64());
}

// ---------------------------------------------------------------- rings

fn bench_rings(n: u64) {
    eprintln!("== rings ==");
    let payload = [0xABu8; 8];
    let mut out = [0u8; 64];

    section("SharedRing fill/drain", || {
        let r = SharedRing::create_anon(1024).unwrap();
        for _ in 0..n {
            sink(black_box(r.try_push(black_box(&payload))));
            sink(black_box(r.try_pop(black_box(&mut out))));
        }
    });

    section("AdaptiveRing send/recv", || {
        let r = AdaptiveRing::create_anon(1, 1, 1024).unwrap();
        let pid = r.register_producer().unwrap();
        let cid = r.register_consumer().unwrap();
        for _ in 0..n {
            sink(black_box(r.try_send(pid, black_box(&payload))));
            sink(black_box(r.try_recv(cid, black_box(&mut out))));
        }
    });

    section("SharedTreiberStack push/pop", || {
        let s: SharedTreiberStack<u64> = SharedTreiberStack::create(tmp("treiber"), 4096).unwrap();
        for i in 0..n {
            sink(black_box(s.push(black_box(i))));
            black_box(s.pop());
        }
    });

    section("SharedBroadcastRing push/recv", || {
        let r = SharedBroadcastRing::create_anon(1024).unwrap();
        let cidx = r.register_consumer().unwrap();
        for _ in 0..n {
            sink(black_box(r.try_push(black_box(&payload))));
            sink(black_box(r.try_recv(cidx, black_box(&mut out))));
        }
    });
}

// ----------------------------------------------------------------- maps

fn bench_maps(n: u64) {
    eprintln!("== maps ==");

    section("SharedHashMap insert/get/remove", || {
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(tmp("hmap"), 4096).unwrap();
        for i in 0..n {
            let k = (i % 2048) as u32;
            sink(black_box(m.insert(black_box(k), black_box(i))));
            black_box(m.get(&k));
            if i % 4 == 0 {
                black_box(m.remove(&k));
            }
        }
    });

    section("SharedBTreeMap insert/get", || {
        let bt: SharedBTreeMap<u32, u64> = SharedBTreeMap::create(tmp("btree"), 4096).unwrap();
        for i in 0..n {
            let k = (i % 2048) as u32;
            sink(black_box(bt.insert(black_box(k), black_box(i))));
            black_box(bt.get(&k));
        }
    });

    section("SharedLinkedList push/pop", || {
        let l: SharedLinkedList<u64> = SharedLinkedList::create(tmp("llist"), 4096).unwrap();
        for i in 0..n {
            sink(black_box(l.push_back(black_box(i))));
            black_box(l.pop_front());
        }
    });

    section("SharedLRUCache put/get", || {
        let c: SharedLRUCache<u32, u64> = SharedLRUCache::create(tmp("lru"), 2048).unwrap();
        for i in 0..n {
            let k = (i % 1024) as u32;
            sink(black_box(c.put(black_box(k), black_box(i))));
            black_box(c.get(&k));
        }
    });
}

// ------------------------------------------------------------- sketches

fn bench_sketches(n: u64) {
    eprintln!("== sketches ==");

    section("SharedBitVec set/get", || {
        let b = SharedBitVec::create(tmp("bitvec"), 1 << 20).unwrap();
        for i in 0..n {
            let idx = (i % (1 << 20)) as usize;
            sink(black_box(b.set(black_box(idx))));
            sink(black_box(b.get(idx)));
        }
    });

    section("SharedBloomFilter insert/contains", || {
        let (nb, nh) = SharedBloomFilter::suggest_config(100_000, 0.01);
        let bf = SharedBloomFilter::create(tmp("bloom"), nb, nh).unwrap();
        for i in 0..n {
            let key = (i % 50_000).to_le_bytes();
            sink(black_box(bf.insert(black_box(&key))));
            sink(black_box(bf.contains(&key)));
        }
    });

    section("SharedCountMinSketch insert/estimate", || {
        let cms = SharedCountMinSketch::create(tmp("cms"), 4, 2048).unwrap();
        for i in 0..n {
            let key = (i % 10_000).to_le_bytes();
            cms.insert(black_box(&key));
            black_box(cms.estimate_count(&key));
        }
    });

    section("SharedHyperLogLog insert", || {
        let hll = SharedHyperLogLog::create(tmp("hll"), 12).unwrap();
        for i in 0..n {
            let key = i.to_le_bytes();
            hll.insert(black_box(&key));
        }
        black_box(hll.estimate());
    });

    section("SharedHistogram record", || {
        let h = SharedHistogram::create(
            tmp("hist"),
            &[10, 100, 1_000, 10_000, 100_000, 1_000_000],
        )
        .unwrap();
        for i in 0..n {
            black_box(h.record(black_box(i % 2_000_000)));
        }
    });

    section("SharedReservoirSampler record", || {
        let rs: SharedReservoirSampler<u64> =
            SharedReservoirSampler::create(tmp("reservoir"), 256).unwrap();
        for i in 0..n {
            black_box(rs.record(black_box(i)));
        }
    });
}

// -------------------------------------------------------------- scalars

fn bench_scalars(n: u64) {
    eprintln!("== scalars ==");

    section("SharedAtomicU64 fetch_add/load", || {
        let a = SharedAtomicU64::create(tmp("atomic"), 0).unwrap();
        for _ in 0..n {
            black_box(a.fetch_add(black_box(1), Ordering::AcqRel));
            black_box(a.load(Ordering::Acquire));
        }
    });

    section("SharedCell set/get", || {
        let c: SharedCell<u64> = SharedCell::create(tmp("cell")).unwrap();
        for i in 0..n {
            c.set(black_box(i));
            black_box(c.get());
        }
    });

    section("SharedOnceCell get_or_init/get", || {
        let oc: SharedOnceCell<u64> = SharedOnceCell::create(tmp("once")).unwrap();
        black_box(oc.get_or_init(|| 42));
        for _ in 0..n {
            black_box(oc.get());
        }
    });
}

// ---------------------------------------------------------------- locks

fn bench_locks(n: u64) {
    eprintln!("== locks ==");

    section("SharedRWLock read/write", || {
        // Pure reader-writer lock: the guard is RAII-only (no protected
        // payload), so the hot path is the lock/unlock cycle itself.
        let l = SharedRWLock::create(tmp("rwlock")).unwrap();
        for i in 0..n {
            {
                let g = l.read_lock();
                black_box(&g);
            }
            if i % 8 == 0 {
                let g = l.write_lock();
                black_box(&g);
            }
        }
    });

    section("SharedSemaphore acquire/release", || {
        let s = SharedSemaphore::create(tmp("sem"), 1_000_000, 1_000_000).unwrap();
        for _ in 0..n {
            if let Ok(p) = s.try_acquire() {
                black_box(&p);
                drop(p);
            }
        }
    });

    section("SharedRateLimiter try_acquire", || {
        let r = SharedRateLimiter::create(tmp("rl"), u32::MAX, u32::MAX).unwrap();
        for _ in 0..n {
            sink(black_box(r.try_acquire(1)));
        }
    });

    section("SharedFenceClock tick", || {
        let clk = SharedFenceClock::create(tmp("hlc"), 8).unwrap();
        let idx = clk.register(std::process::id()).unwrap();
        for _ in 0..n {
            black_box(clk.tick(idx));
        }
    });
}

// --------------------------------------------------------- coordination

fn bench_coordination(n: u64) {
    eprintln!("== coordination ==");
    let pid = std::process::id();

    section("HeartbeatTable beat", || {
        let h = HeartbeatTable::create(tmp("hb"), 16).unwrap();
        let idx = h.register(pid).unwrap();
        for _ in 0..n {
            h.beat(idx);
            black_box(h.snapshot(idx));
        }
    });

    section("OwnerLease read/write/beat", || {
        let lease: OwnerLease<u64> = OwnerLease::create(tmp("lease"), 0).unwrap();
        black_box(lease.try_acquire(pid, 3));
        for i in 0..n {
            black_box(lease.read_as_owner(pid));
            black_box(lease.write_as_owner(pid, black_box(i)));
            if i % 16 == 0 {
                black_box(lease.beat(pid));
            }
        }
    });

    section("SharedLeaderElection claim/beat", || {
        let e = SharedLeaderElection::create(tmp("leader")).unwrap();
        for i in 0..n {
            black_box(e.try_claim_leadership(pid, 3));
            black_box(e.beat_as_leader(pid));
            if i % 64 == 0 {
                black_box(e.current_leader());
            }
        }
    });

    section("EventStateLog emit/drain_fold", || {
        let log: EventStateLog<u64, u64> =
            EventStateLog::create(tmp("evlog"), 4096, 0u64).unwrap();
        for i in 0..n {
            sink(black_box(log.emit(black_box(i))));
            if i % 32 == 0 {
                black_box(log.drain_and_fold(|s: &mut u64, e: &u64| *s = s.wrapping_add(*e)));
            }
        }
        black_box(log.read_current());
    });
}

// -------------------------------------------------------------- storage

fn bench_storage(n: u64) {
    eprintln!("== storage ==");

    section("SharedRegion allocate/get/free", || {
        let r: SharedRegion<u64> = SharedRegion::create(tmp("region"), 4096).unwrap();
        for i in 0..n {
            if let Ok(ptr) = r.allocate(black_box(i)) {
                black_box(r.get(ptr).ok());
                sink(black_box(r.free(ptr)));
            }
        }
    });

    section("SharedStringArena intern", || {
        let a = SharedStringArena::create(tmp("arena"), 1 << 20).unwrap();
        let strs = ["alpha", "beta", "gamma", "delta", "epsilon"];
        for i in 0..n {
            let s = strs[(i as usize) % strs.len()];
            if a.intern(black_box(s)).is_err() {
                a.clear();
            }
        }
    });

    section("SharedHandleTable insert/get/remove", || {
        let t: SharedHandleTable<u64> = SharedHandleTable::create(tmp("htable"), 4096).unwrap();
        for i in 0..n {
            if let Ok(h) = t.insert(black_box(i)) {
                black_box(t.get(h));
                black_box(t.remove(h));
            }
        }
    });

    section("SharedVec push/pop/get", || {
        let v: SharedVec<u64> = SharedVec::create(tmp("vec"), 4096).unwrap();
        for i in 0..n {
            sink(black_box(v.push_back(black_box(i))));
            black_box(v.get(0));
            black_box(v.pop_back());
        }
    });
}

// -------------------------------------------------------------- pointers

fn bench_pointers(n: u64) {
    eprintln!("== pointers ==");

    section("TaggedOffsetPtr pack/unpack", || {
        let mut acc = 0u64;
        for i in 0..n {
            let p: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::new(
                black_box((i as u32) & 0x0FFF_FFFF),
                black_box((i as u32) & 0xF),
            );
            let raw = p.raw();
            let q: TaggedOffsetPtr<u64, 4> = TaggedOffsetPtr::from_raw(black_box(raw));
            acc = acc
                .wrapping_add(q.index() as u64)
                .wrapping_add(q.tag() as u64);
        }
        black_box(acc);
    });
}
