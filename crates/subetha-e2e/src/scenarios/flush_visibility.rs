//! Every MMF-backed primitive, read back from a second process.
//!
//! `flush_async` is the non-blocking flush: `msync(MS_ASYNC)` on unix,
//! `FlushViewOfFile` without `FlushFileBuffers` on Windows. The claim
//! it has to satisfy is that the mutation it flushes is intact and
//! visible to another participant.
//!
//! So the parent creates each primitive, performs one meaningful
//! state-modifying operation, calls `flush_async`, and holds every
//! mapping open. A child process then opens each file by path and
//! reads the value back. Reading from a second address space is what
//! makes the answer mean something: a re-read inside the writing
//! process can be served by that process's own mapping whether or not
//! the state ever became shareable.
//!
//! Handles cross as integers, which is what makes them handles rather
//! than pointers - `Handle::raw`, `OffsetPtr::index` and
//! `StringRef::to_u64` are all position-independent, so the child
//! rebuilds them without knowing where the parent mapped anything.

use std::sync::Arc;

use subetha_cxc::shared_region::OffsetPtr;
use subetha_cxc::shared_string_arena::StringRef;
use subetha_cxc::{
    EpochBarrier, EventStateLog, Handle, HeartbeatTable, LazyConfig, OwnerLease, PriorityFanout,
    ProgressTask, SharedAsyncPointer, SharedAtomicBool, SharedAtomicU32, SharedAtomicU64,
    SharedBroadcastRing, SharedCell, SharedFenceClock, SharedHandleTable, SharedHashMap,
    SharedLeaderElection, SharedOnceCell, SharedRegion, SharedRing, SharedSemaphore,
    SharedStringArena, SharedTimePointTile, SharedTopologyMap, SharedVec, SharedVersionedChain,
    BROADCAST_PAYLOAD_BYTES, PAYLOAD_BYTES,
};

use crate::harness::{arg_path, arg_u64, require, BoxErr, Harness};

const RING_CAP: usize = 16;
const SMALL_CAP: usize = 8;
const MAP_CAP: usize = 16;
const NODES: usize = 4;
const ARENA_BYTES: usize = 256;
const PERMITS: u32 = 2;
const PRIORITIES: usize = 4;
const GRACE: u64 = 3;
const BARRIER_GRACE: u64 = 10;

const RING_FILL: u8 = 42;
const BROADCAST_FILL: u8 = 55;
const FANOUT_FILL: u8 = 9;
const FANOUT_PRIORITY: usize = 2;

const CELL_VALUE: u64 = 0xDEAD_BEEF;
const ATOMIC_U32_VALUE: u32 = 99;
const ATOMIC_U64_BASE: u64 = 11;
const ATOMIC_U64_ADD: u64 = 5;
const ONCE_VALUE: u64 = 123;
const HANDLE_VALUE: u64 = 42;
const CHAIN: [(u64, u64); 2] = [(1, 100), (2, 200)];
const TILE_VERSION: u64 = 100;
const TILE_VALUE: u64 = 42;
const ASYNC_VALUE: u64 = 777;
const VEC_VALUES: [u32; 2] = [100, 200];
const MAP_ENTRIES: [(u32, u32); 2] = [(1, 10), (2, 20)];
const REGION_VALUE: u64 = 0xCAFE;
const ARENA_TEXT: &str = "hello-async";
const CONFIG_VALUE: u64 = 8888;
const PROGRESS_TOTAL: u64 = 5;
const PROGRESS_RESULT: u64 = 42;
const EVENT_VALUE: u32 = 10;

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    let parent_pid = std::process::id();

    // Every mapping stays alive for the whole function: the child
    // reads while the parent still holds its side open, which is the
    // configuration a live participant actually runs in.
    let ring = SharedRing::create(h.path("ring.bin"), RING_CAP)
        .map_err(|e| format!("ring: {e:?}"))?;
    ring.try_push(&[RING_FILL; PAYLOAD_BYTES])
        .map_err(|e| format!("ring push: {e:?}"))?;
    ring.flush_async().map_err(|e| format!("ring flush: {e:?}"))?;

    let cell: SharedCell<u64> =
        SharedCell::create(h.path("cell.bin")).map_err(|e| format!("cell: {e:?}"))?;
    cell.set(CELL_VALUE);
    cell.flush_async().map_err(|e| format!("cell flush: {e:?}"))?;

    let atomic_u32 = SharedAtomicU32::create(h.path("atomic-u32.bin"), 7)
        .map_err(|e| format!("atomic u32: {e:?}"))?;
    atomic_u32.store(ATOMIC_U32_VALUE, std::sync::atomic::Ordering::Release);
    atomic_u32.flush_async().map_err(|e| format!("atomic u32 flush: {e:?}"))?;

    let atomic_u64 = SharedAtomicU64::create(h.path("atomic-u64.bin"), ATOMIC_U64_BASE)
        .map_err(|e| format!("atomic u64: {e:?}"))?;
    atomic_u64.fetch_add(ATOMIC_U64_ADD, std::sync::atomic::Ordering::AcqRel);
    atomic_u64.flush_async().map_err(|e| format!("atomic u64 flush: {e:?}"))?;

    let atomic_bool = SharedAtomicBool::create(h.path("atomic-bool.bin"), false)
        .map_err(|e| format!("atomic bool: {e:?}"))?;
    atomic_bool.store(true, std::sync::atomic::Ordering::Release);
    atomic_bool.flush_async().map_err(|e| format!("atomic bool flush: {e:?}"))?;

    let once: SharedOnceCell<u64> =
        SharedOnceCell::create(h.path("once.bin")).map_err(|e| format!("once cell: {e:?}"))?;
    once.set(ONCE_VALUE);
    once.flush_async().map_err(|e| format!("once cell flush: {e:?}"))?;

    let table: SharedHandleTable<u64> =
        SharedHandleTable::create(h.path("handles.bin"), SMALL_CAP)
            .map_err(|e| format!("handle table: {e:?}"))?;
    let handle = table.insert(HANDLE_VALUE).map_err(|e| format!("handle insert: {e:?}"))?;
    table.flush_async().map_err(|e| format!("handle table flush: {e:?}"))?;

    let chain: SharedVersionedChain<u64> =
        SharedVersionedChain::create(h.path("chain.bin"), SMALL_CAP)
            .map_err(|e| format!("chain: {e:?}"))?;
    for (version, value) in CHAIN {
        chain.push(version, value).map_err(|e| format!("chain push: {e:?}"))?;
    }
    chain.flush_async().map_err(|e| format!("chain flush: {e:?}"))?;

    let tile: SharedTimePointTile<u64> =
        SharedTimePointTile::create(h.path("tile.bin")).map_err(|e| format!("tile: {e:?}"))?;
    let lane = tile
        .insert(TILE_VERSION, TILE_VALUE)
        .map_err(|e| format!("tile insert: {e:?}"))?;
    tile.flush_async().map_err(|e| format!("tile flush: {e:?}"))?;

    let async_ptr: SharedAsyncPointer<u64> = SharedAsyncPointer::create(h.path("async-ptr.bin"))
        .map_err(|e| format!("async pointer: {e:?}"))?;
    async_ptr.set_resolved(ASYNC_VALUE);
    async_ptr.flush_async().map_err(|e| format!("async pointer flush: {e:?}"))?;

    let election = SharedLeaderElection::create(h.path("leader.bin"))
        .map_err(|e| format!("leader election: {e:?}"))?;
    require(
        election.try_claim_leadership(parent_pid, GRACE),
        "parent failed to claim leadership of a fresh election",
    )?;
    election.flush_async().map_err(|e| format!("leader flush: {e:?}"))?;

    let broadcast = SharedBroadcastRing::create(h.path("broadcast.bin"), SMALL_CAP)
        .map_err(|e| format!("broadcast ring: {e:?}"))?;
    // Registered before the push: a broadcast consumer reads from the
    // position it joined at, so a later joiner would not see this item.
    let consumer = broadcast
        .register_consumer()
        .map_err(|e| format!("broadcast register: {e:?}"))?;
    broadcast
        .try_push(&[BROADCAST_FILL; BROADCAST_PAYLOAD_BYTES])
        .map_err(|e| format!("broadcast push: {e:?}"))?;
    broadcast.flush_async().map_err(|e| format!("broadcast flush: {e:?}"))?;

    let vec: SharedVec<u32> = SharedVec::create(h.path("vec.bin"), SMALL_CAP)
        .map_err(|e| format!("vec: {e:?}"))?;
    for v in VEC_VALUES {
        vec.push_back(v).map_err(|e| format!("vec push: {e:?}"))?;
    }
    vec.flush_async().map_err(|e| format!("vec flush: {e:?}"))?;

    let map: SharedHashMap<u32, u32> = SharedHashMap::create(h.path("map.bin"), MAP_CAP)
        .map_err(|e| format!("hash map: {e:?}"))?;
    for (k, v) in MAP_ENTRIES {
        map.insert(k, v).map_err(|e| format!("map insert: {e:?}"))?;
    }
    map.flush_async().map_err(|e| format!("map flush: {e:?}"))?;

    let region: SharedRegion<u64> = SharedRegion::create(h.path("region.bin"), SMALL_CAP)
        .map_err(|e| format!("region: {e:?}"))?;
    let region_ptr = region
        .allocate(REGION_VALUE)
        .map_err(|e| format!("region allocate: {e:?}"))?;
    region.flush_async().map_err(|e| format!("region flush: {e:?}"))?;

    let arena = SharedStringArena::create(h.path("arena.bin"), ARENA_BYTES)
        .map_err(|e| format!("arena: {e:?}"))?;
    let interned = arena.intern(ARENA_TEXT).map_err(|e| format!("arena intern: {e:?}"))?;
    arena.flush_async().map_err(|e| format!("arena flush: {e:?}"))?;

    let topology = SharedTopologyMap::create(h.path("topology.bin"), NODES)
        .map_err(|e| format!("topology: {e:?}"))?;
    topology.record_send(0, 1).map_err(|e| format!("topology record: {e:?}"))?;
    topology.publish_recommendation();
    topology.flush_async().map_err(|e| format!("topology flush: {e:?}"))?;

    let clock = SharedFenceClock::create(h.path("fence-clock.bin"), NODES)
        .map_err(|e| format!("fence clock: {e:?}"))?;
    let clock_slot = clock.register(parent_pid).map_err(|e| format!("clock register: {e:?}"))?;
    clock.tick(clock_slot);
    clock.flush_async().map_err(|e| format!("clock flush: {e:?}"))?;

    let semaphore = SharedSemaphore::create(h.path("semaphore"), PERMITS, PERMITS)
        .map_err(|e| format!("semaphore: {e:?}"))?;
    let _permit = semaphore
        .try_acquire()
        .map_err(|e| format!("semaphore acquire: {e:?}"))?;
    semaphore.flush_async().map_err(|e| format!("semaphore flush: {e:?}"))?;

    let lease: OwnerLease<u64> =
        OwnerLease::create(h.path("lease.bin"), 0).map_err(|e| format!("lease: {e:?}"))?;
    require(
        lease.try_acquire(parent_pid, GRACE),
        "parent failed to acquire a fresh owner lease",
    )?;
    lease.flush_async().map_err(|e| format!("lease flush: {e:?}"))?;

    let config: LazyConfig<u64> =
        LazyConfig::create(h.path("config.bin")).map_err(|e| format!("lazy config: {e:?}"))?;
    config.force_set(CONFIG_VALUE);
    config.flush_async().map_err(|e| format!("config flush: {e:?}"))?;

    let barrier_hb = Arc::new(
        HeartbeatTable::create(h.path("barrier-hb.bin"), SMALL_CAP)
            .map_err(|e| format!("barrier heartbeat: {e:?}"))?,
    );
    let barrier = EpochBarrier::create(h.path("barrier"), barrier_hb, BARRIER_GRACE)
        .map_err(|e| format!("epoch barrier: {e:?}"))?;
    barrier.flush_async().map_err(|e| format!("barrier flush: {e:?}"))?;

    let progress: ProgressTask<u64> =
        ProgressTask::create(h.path("progress"), 0).map_err(|e| format!("progress task: {e:?}"))?;
    progress.run(PROGRESS_TOTAL, |r| {
        r.advance(PROGRESS_TOTAL);
        PROGRESS_RESULT
    });
    progress.flush_async().map_err(|e| format!("progress flush: {e:?}"))?;

    let log: EventStateLog<u32, u32> = EventStateLog::create(h.path("eventlog"), SMALL_CAP, 0)
        .map_err(|e| format!("event log: {e:?}"))?;
    log.emit(EVENT_VALUE).map_err(|e| format!("event emit: {e:?}"))?;
    log.drain_and_fold(|s, e| *s += *e);
    log.flush_async().map_err(|e| format!("event log flush: {e:?}"))?;

    let fanout = PriorityFanout::create(h.path("fanout"), PRIORITIES, SMALL_CAP)
        .map_err(|e| format!("priority fanout: {e:?}"))?;
    fanout
        .submit(FANOUT_PRIORITY, &[FANOUT_FILL; PAYLOAD_BYTES])
        .map_err(|e| format!("fanout submit: {e:?}"))?;
    fanout.flush_async().map_err(|e| format!("fanout flush: {e:?}"))?;

    println!("   parent: 25 primitives written and flushed, handing off to the reader process");

    h.run(
        "reader",
        &[
            h.dir().to_string_lossy().into_owned(),
            handle.raw().to_string(),
            u64::from(region_ptr.index).to_string(),
            interned.to_u64().to_string(),
            (lane as u64).to_string(),
            (consumer as u64).to_string(),
            (clock_slot as u64).to_string(),
            u64::from(parent_pid).to_string(),
        ],
    )?;

    println!("   parent: reader process recovered every flushed value");
    Ok(())
}

pub fn child(role: &str, args: &[String]) -> Result<(), BoxErr> {
    match role {
        "reader" => reader(args),
        other => Err(format!("flush-visibility: unknown child role {other:?}").into()),
    }
}

#[allow(clippy::too_many_lines)]
fn reader(args: &[String]) -> Result<(), BoxErr> {
    let dir = arg_path(args, 0, "scratch directory")?.to_path_buf();
    let handle = Handle::from_parts(
        (arg_u64(args, 1, "handle")? >> 32) as u32,
        arg_u64(args, 1, "handle")? as u32,
    );
    let region_index = arg_u64(args, 2, "region index")? as u32;
    let interned = StringRef::from_u64(arg_u64(args, 3, "string ref")?);
    let lane = arg_u64(args, 4, "tile lane")? as usize;
    let consumer = arg_u64(args, 5, "broadcast consumer")? as usize;
    let clock_slot = arg_u64(args, 6, "clock slot")? as usize;
    let parent_pid = arg_u64(args, 7, "parent pid")? as u32;

    let at = |stem: &str| dir.join(stem);

    let ring = SharedRing::open(at("ring.bin"), RING_CAP)
        .map_err(|e| format!("open ring: {e:?}"))?;
    let mut buf = [0u8; PAYLOAD_BYTES];
    let n = ring.try_pop(&mut buf).map_err(|e| format!("ring pop: {e:?}"))?;
    require(
        buf[..n].iter().all(|b| *b == RING_FILL) && n == PAYLOAD_BYTES,
        format!("ring payload came back as {n} bytes of mixed content"),
    )?;

    let cell: SharedCell<u64> =
        SharedCell::open(at("cell.bin")).map_err(|e| format!("open cell: {e:?}"))?;
    require(cell.get() == CELL_VALUE, format!("cell = {:#x}", cell.get()))?;

    let atomic_u32 = SharedAtomicU32::open(at("atomic-u32.bin"))
        .map_err(|e| format!("open atomic u32: {e:?}"))?;
    let got = atomic_u32.load(std::sync::atomic::Ordering::Acquire);
    require(got == ATOMIC_U32_VALUE, format!("atomic u32 = {got}"))?;

    let atomic_u64 = SharedAtomicU64::open(at("atomic-u64.bin"))
        .map_err(|e| format!("open atomic u64: {e:?}"))?;
    let got = atomic_u64.load(std::sync::atomic::Ordering::Acquire);
    require(
        got == ATOMIC_U64_BASE + ATOMIC_U64_ADD,
        format!("atomic u64 = {got}"),
    )?;

    let atomic_bool = SharedAtomicBool::open(at("atomic-bool.bin"))
        .map_err(|e| format!("open atomic bool: {e:?}"))?;
    require(
        atomic_bool.load(std::sync::atomic::Ordering::Acquire),
        "atomic bool came back false",
    )?;

    let once: SharedOnceCell<u64> =
        SharedOnceCell::open(at("once.bin")).map_err(|e| format!("open once cell: {e:?}"))?;
    require(once.get() == Some(ONCE_VALUE), format!("once cell = {:?}", once.get()))?;

    let table: SharedHandleTable<u64> = SharedHandleTable::open(at("handles.bin"), SMALL_CAP)
        .map_err(|e| format!("open handle table: {e:?}"))?;
    require(
        table.get(handle) == Some(HANDLE_VALUE),
        format!("handle {handle:?} = {:?}", table.get(handle)),
    )?;

    let chain: SharedVersionedChain<u64> = SharedVersionedChain::open(at("chain.bin"), SMALL_CAP)
        .map_err(|e| format!("open chain: {e:?}"))?;
    let newest = CHAIN[CHAIN.len() - 1];
    require(
        chain.current() == Some(newest),
        format!("chain head = {:?}, expected {newest:?}", chain.current()),
    )?;

    let tile: SharedTimePointTile<u64> =
        SharedTimePointTile::open(at("tile.bin")).map_err(|e| format!("open tile: {e:?}"))?;
    require(
        tile.at(lane) == Some((TILE_VERSION, TILE_VALUE)),
        format!("tile lane {lane} = {:?}", tile.at(lane)),
    )?;

    let async_ptr: SharedAsyncPointer<u64> = SharedAsyncPointer::open(at("async-ptr.bin"))
        .map_err(|e| format!("open async pointer: {e:?}"))?;
    require(
        async_ptr.try_get() == Some(ASYNC_VALUE),
        format!("async pointer = {:?}", async_ptr.try_get()),
    )?;

    let election = SharedLeaderElection::open(at("leader.bin"))
        .map_err(|e| format!("open leader election: {e:?}"))?;
    require(
        election.current_leader() == Some(parent_pid),
        format!("leader = {:?}, expected the parent {parent_pid}", election.current_leader()),
    )?;

    let broadcast = SharedBroadcastRing::open(at("broadcast.bin"), SMALL_CAP)
        .map_err(|e| format!("open broadcast: {e:?}"))?;
    let mut bbuf = [0u8; BROADCAST_PAYLOAD_BYTES];
    let n = broadcast
        .try_recv(consumer, &mut bbuf)
        .map_err(|e| format!("broadcast recv: {e:?}"))?;
    require(
        bbuf[..n].iter().all(|b| *b == BROADCAST_FILL),
        "broadcast payload content differs",
    )?;

    let vec: SharedVec<u32> =
        SharedVec::open(at("vec.bin"), SMALL_CAP).map_err(|e| format!("open vec: {e:?}"))?;
    for (i, want) in VEC_VALUES.iter().enumerate() {
        require(
            vec.get(i) == Some(*want),
            format!("vec[{i}] = {:?}, expected {want}", vec.get(i)),
        )?;
    }

    let map: SharedHashMap<u32, u32> =
        SharedHashMap::open(at("map.bin"), MAP_CAP).map_err(|e| format!("open map: {e:?}"))?;
    for (k, want) in MAP_ENTRIES {
        require(
            map.get(&k) == Some(want),
            format!("map[{k}] = {:?}, expected {want}", map.get(&k)),
        )?;
    }

    let region: SharedRegion<u64> =
        SharedRegion::open(at("region.bin"), SMALL_CAP).map_err(|e| format!("open region: {e:?}"))?;
    let ptr = OffsetPtr::<u64>::new(region_index);
    let got = region.get(ptr).map_err(|e| format!("region get: {e:?}"))?;
    require(got == REGION_VALUE, format!("region value = {got:#x}"))?;

    let arena = SharedStringArena::open(at("arena.bin"), ARENA_BYTES)
        .map_err(|e| format!("open arena: {e:?}"))?;
    let got = arena.get(interned).map_err(|e| format!("arena get: {e:?}"))?;
    require(got == ARENA_TEXT, format!("arena string = {got:?}"))?;

    let topology = SharedTopologyMap::open(at("topology.bin"), NODES)
        .map_err(|e| format!("open topology: {e:?}"))?;
    require(
        topology.total_msgs() == 1,
        format!("topology total_msgs = {}", topology.total_msgs()),
    )?;

    let clock = SharedFenceClock::open(at("fence-clock.bin"), NODES)
        .map_err(|e| format!("open fence clock: {e:?}"))?;
    require(
        clock.slot_snapshot(clock_slot).is_some(),
        format!("fence clock slot {clock_slot} is not live"),
    )?;

    let semaphore = SharedSemaphore::open(at("semaphore"), PERMITS)
        .map_err(|e| format!("open semaphore: {e:?}"))?;
    require(
        semaphore.available() == PERMITS - 1,
        format!(
            "semaphore has {} permits, expected {} while the parent holds one",
            semaphore.available(),
            PERMITS - 1
        ),
    )?;

    let lease: OwnerLease<u64> =
        OwnerLease::open(at("lease.bin")).map_err(|e| format!("open lease: {e:?}"))?;
    require(
        lease.current_owner() == Some(parent_pid),
        format!("lease owner = {:?}, expected the parent {parent_pid}", lease.current_owner()),
    )?;

    let config: LazyConfig<u64> =
        LazyConfig::open(at("config.bin")).map_err(|e| format!("open lazy config: {e:?}"))?;
    require(
        config.try_get() == Some(CONFIG_VALUE),
        format!("lazy config = {:?}", config.try_get()),
    )?;

    let barrier_hb = Arc::new(
        HeartbeatTable::open(at("barrier-hb.bin"), SMALL_CAP)
            .map_err(|e| format!("open barrier heartbeat: {e:?}"))?,
    );
    EpochBarrier::open(at("barrier"), barrier_hb, BARRIER_GRACE)
        .map_err(|e| format!("open epoch barrier: {e:?}"))?;

    let progress: ProgressTask<u64> =
        ProgressTask::open(at("progress")).map_err(|e| format!("open progress task: {e:?}"))?;
    require(
        progress.read_result() == Some(PROGRESS_RESULT),
        format!("progress result = {:?}", progress.read_result()),
    )?;

    let log: EventStateLog<u32, u32> = EventStateLog::open(at("eventlog"), SMALL_CAP)
        .map_err(|e| format!("open event log: {e:?}"))?;
    require(
        log.read_current() == EVENT_VALUE,
        format!("event log state = {}", log.read_current()),
    )?;

    let fanout = PriorityFanout::open(at("fanout"), PRIORITIES, SMALL_CAP)
        .map_err(|e| format!("open priority fanout: {e:?}"))?;
    require(
        fanout.highest_active_priority() == Some(FANOUT_PRIORITY),
        format!("fanout highest priority = {:?}", fanout.highest_active_priority()),
    )?;

    println!("   reader: every primitive's flushed state read back from a second process");
    Ok(())
}
