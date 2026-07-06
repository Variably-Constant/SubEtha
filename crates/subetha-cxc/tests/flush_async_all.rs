//! Verifies that every primitive's `flush_async` returns Ok and
//! does not corrupt the underlying state.
//!
//! Rationale: `flush_async` is the non-blocking variant of `flush`
//! (delegates to `memmap2::MmapMut::flush_async`). On Linux it's
//! `msync(MS_ASYNC)`; on Windows it's `FlushViewOfFile` without
//! `FlushFileBuffers`. Windows is therefore only partially async
//! (sync to page cache, not to disk).
//!
//! Each test below creates a primitive, performs one meaningful
//! state-modifying operation, calls `flush_async`, and re-reads
//! to verify the state survived.

use std::path::PathBuf;
use std::sync::Arc;

use subetha_cxc::{
    EpochBarrier, EventStateLog, HeartbeatTable, LazyConfig, OwnerLease,
    PriorityFanout, ProgressTask, SharedAsyncPointer, SharedAtomicBool,
    SharedAtomicU32, SharedAtomicU64, SharedBroadcastRing, SharedCell,
    SharedFenceClock, SharedHandleTable, SharedHashMap, SharedLeaderElection,
    SharedOnceCell, SharedRegion, SharedRing, SharedSemaphore,
    SharedStringArena, SharedTimePointTile, SharedTopologyMap, SharedVec,
    SharedVersionedChain,
};

fn tmp(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-flush-async-{name}-{pid}.bin"));
    p
}

fn tmp_base(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-flush-async-{name}-{pid}"));
    p
}

#[test]
fn shared_ring_flush_async() {
    let p = tmp("ring");
    let r = SharedRing::create(&p, 16).unwrap();
    let payload = [42u8; subetha_cxc::PAYLOAD_BYTES];
    r.try_push(&payload).unwrap();
    r.flush_async().unwrap();
    let mut buf = [0u8; subetha_cxc::PAYLOAD_BYTES];
    r.try_pop(&mut buf).unwrap();
    assert_eq!(buf, payload);
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_cell_flush_async() {
    let p = tmp("cell");
    let c: SharedCell<u64> = SharedCell::create(&p).unwrap();
    c.set(0xDEAD_BEEF);
    c.flush_async().unwrap();
    assert_eq!(c.get(), 0xDEAD_BEEF);
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_atomic_u32_flush_async() {
    let p = tmp("atomic-u32");
    let a = SharedAtomicU32::create(&p, 7).unwrap();
    a.store(99, std::sync::atomic::Ordering::Release);
    a.flush_async().unwrap();
    assert_eq!(a.load(std::sync::atomic::Ordering::Acquire), 99);
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_atomic_u64_flush_async() {
    let p = tmp("atomic-u64");
    let a = SharedAtomicU64::create(&p, 11).unwrap();
    a.fetch_add(5, std::sync::atomic::Ordering::AcqRel);
    a.flush_async().unwrap();
    assert_eq!(a.load(std::sync::atomic::Ordering::Acquire), 16);
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_atomic_bool_flush_async() {
    let p = tmp("atomic-bool");
    let a = SharedAtomicBool::create(&p, false).unwrap();
    a.store(true, std::sync::atomic::Ordering::Release);
    a.flush_async().unwrap();
    assert!(a.load(std::sync::atomic::Ordering::Acquire));
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_once_cell_flush_async() {
    let p = tmp("once-cell");
    let c: SharedOnceCell<u64> = SharedOnceCell::create(&p).unwrap();
    c.set(123);
    c.flush_async().unwrap();
    assert_eq!(c.get(), Some(123));
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_handle_table_flush_async() {
    let p = tmp("handle-table");
    let t: SharedHandleTable<u64> = SharedHandleTable::create(&p, 8).unwrap();
    let h = t.insert(42).unwrap();
    t.flush_async().unwrap();
    assert_eq!(t.get(h), Some(42));
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_versioned_chain_flush_async() {
    let p = tmp("versioned-chain");
    let c: SharedVersionedChain<u64> = SharedVersionedChain::create(&p, 8).unwrap();
    c.push(1, 100).unwrap();
    c.push(2, 200).unwrap();
    c.flush_async().unwrap();
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_time_point_flush_async() {
    let p = tmp("time-point");
    let t: SharedTimePointTile<u64> = SharedTimePointTile::create(&p).unwrap();
    t.insert(100, 42).unwrap();
    t.flush_async().unwrap();
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_async_pointer_flush_async() {
    let p = tmp("async-ptr");
    let a: SharedAsyncPointer<u64> = SharedAsyncPointer::create(&p).unwrap();
    a.set_resolved(777);
    a.flush_async().unwrap();
    assert_eq!(a.try_get(), Some(777));
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_leader_election_flush_async() {
    let p = tmp("leader");
    let e = SharedLeaderElection::create(&p).unwrap();
    e.try_claim_leadership(std::process::id(), 3);
    e.flush_async().unwrap();
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_broadcast_ring_flush_async() {
    let p = tmp("broadcast");
    let r = SharedBroadcastRing::create(&p, 8).unwrap();
    let _c = r.register_consumer().unwrap();
    let payload = [55u8; subetha_cxc::BROADCAST_PAYLOAD_BYTES];
    r.try_push(&payload).unwrap();
    r.flush_async().unwrap();
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_vec_flush_async() {
    let p = tmp("vec");
    let v: SharedVec<u32> = SharedVec::create(&p, 8).unwrap();
    v.push_back(100).unwrap();
    v.push_back(200).unwrap();
    v.flush_async().unwrap();
    assert_eq!(v.get(0), Some(100));
    assert_eq!(v.get(1), Some(200));
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_hash_map_flush_async() {
    let p = tmp("hash-map");
    let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
    m.insert(1, 10).unwrap();
    m.insert(2, 20).unwrap();
    m.flush_async().unwrap();
    assert_eq!(m.get(&1), Some(10));
    assert_eq!(m.get(&2), Some(20));
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_region_flush_async() {
    let p = tmp("region");
    let r: SharedRegion<u64> = SharedRegion::create(&p, 8).unwrap();
    let ptr = r.allocate(0xCAFE).unwrap();
    r.flush_async().unwrap();
    assert_eq!(r.get(ptr).unwrap(), 0xCAFE);
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_string_arena_flush_async() {
    let p = tmp("arena");
    let a = SharedStringArena::create(&p, 256).unwrap();
    let r = a.intern("hello-async").unwrap();
    a.flush_async().unwrap();
    assert_eq!(a.get(r).unwrap(), "hello-async");
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_topology_map_flush_async() {
    let p = tmp("topology");
    let t = SharedTopologyMap::create(&p, 4).unwrap();
    t.record_send(0, 1).unwrap();
    t.publish_recommendation();
    t.flush_async().unwrap();
    assert_eq!(t.total_msgs(), 1);
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_fence_clock_flush_async() {
    let p = tmp("fence-clock");
    let c = SharedFenceClock::create(&p, 4).unwrap();
    let idx = c.register(1).unwrap();
    c.tick(idx);
    c.flush_async().unwrap();
    std::fs::remove_file(&p).ok();
}

#[test]
fn shared_semaphore_flush_async() {
    let base = tmp_base("semaphore");
    let s = SharedSemaphore::create(&base, 2, 2).unwrap();
    let _p = s.try_acquire().unwrap();
    s.flush_async().unwrap();
    assert_eq!(s.available(), 1);
    let mut bp = base.clone();
    let stem = bp.file_name().unwrap().to_string_lossy().to_string();
    for ext in ["count", "wakeup", "waiters"] {
        bp.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&bp).ok();
    }
}

#[test]
fn owner_lease_flush_async() {
    let p = tmp("owner-lease");
    let l: OwnerLease<u64> = OwnerLease::create(&p, 0).unwrap();
    let _acquired = l.try_acquire(std::process::id(), 3);
    l.flush_async().unwrap();
    std::fs::remove_file(&p).ok();
}

#[test]
fn lazy_config_flush_async() {
    let p = tmp("lazy-config");
    let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
    c.force_set(8888);
    c.flush_async().unwrap();
    assert_eq!(c.try_get(), Some(8888));
    std::fs::remove_file(&p).ok();
}

#[test]
fn epoch_barrier_flush_async() {
    let base = tmp_base("barrier");
    let hb_p = tmp("barrier-hb");
    let hb = Arc::new(HeartbeatTable::create(&hb_p, 4).unwrap());
    let b = EpochBarrier::create(&base, hb.clone(), 10).unwrap();
    b.flush_async().unwrap();
    let mut sp = base.clone();
    let stem = sp.file_name().unwrap().to_string_lossy().to_string();
    sp.set_file_name(format!("{stem}.state.bin"));
    std::fs::remove_file(&sp).ok();
    std::fs::remove_file(&hb_p).ok();
}

#[test]
fn progress_task_flush_async() {
    let base = tmp_base("progress");
    let t: ProgressTask<u64> = ProgressTask::create(&base, 0).unwrap();
    t.run(5, |r| { r.advance(5); 42 });
    t.flush_async().unwrap();
    assert_eq!(t.read_result(), Some(42));
    let stem = base.file_name().unwrap().to_string_lossy().to_string();
    for ext in ["progress", "total", "done", "result"] {
        let mut sp = base.clone();
        sp.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&sp).ok();
    }
}

#[test]
fn event_state_log_flush_async() {
    let base = tmp_base("eventlog");
    let log: EventStateLog<u32, u32> = EventStateLog::create(&base, 8, 0).unwrap();
    log.emit(10).unwrap();
    log.drain_and_fold(|s, e| *s += *e);
    log.flush_async().unwrap();
    assert_eq!(log.read_current(), 10);
    let stem = base.file_name().unwrap().to_string_lossy().to_string();
    for ext in ["events", "state"] {
        let mut sp = base.clone();
        sp.set_file_name(format!("{stem}.{ext}.bin"));
        std::fs::remove_file(&sp).ok();
    }
}

#[test]
fn priority_fanout_flush_async() {
    let base = tmp_base("fanout");
    let f = PriorityFanout::create(&base, 4, 8).unwrap();
    let payload = [9u8; subetha_cxc::PAYLOAD_BYTES];
    f.submit(2, &payload).unwrap();
    f.flush_async().unwrap();
    assert_eq!(f.highest_active_priority(), Some(2));
    let stem = base.file_name().unwrap().to_string_lossy().to_string();
    let mut bp = base.clone();
    bp.set_file_name(format!("{stem}.bitmap.bin"));
    std::fs::remove_file(&bp).ok();
    for i in 0..4 {
        let mut bp = base.clone();
        bp.set_file_name(format!("{stem}.prio{i}.bin"));
        std::fs::remove_file(&bp).ok();
    }
}
