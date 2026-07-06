//! Per-primitive op_kind constants for sidecar observations.
//!
//! Every subetha primitive carries a `HandshakeHeader` + `ObservationRing`
//! and pushes a per-op `Observation` with the op_kind drawn from the
//! constants below. The sidecar's drain folds these into
//! `InstanceStats.op_kind_counts[op_kind]`, letting policies distinguish
//! insert-heavy from get-heavy workloads, contention from idle, etc.
//!
//! `op_kind = 0` is reserved for "unspecified" by the sidecar; primitives
//! start their op_kinds at 1. The sidecar's `N_OP_KINDS = 8` caps how
//! many distinct op_kinds any one primitive can address; primitives with
//! more than 7 distinguishable ops must collapse some into a shared
//! bucket.

/// Op kinds for [`SharedRing`](crate::SharedRing).
pub mod ring {
    pub const OP_PUSH: u16 = 1;
    pub const OP_POP: u16 = 2;
}

/// Op kinds for [`SharedCell`](crate::SharedCell) and
/// [`SharedOnceCell`](crate::SharedOnceCell).
pub mod cell {
    pub const OP_GET: u16 = 1;
    pub const OP_SET: u16 = 2;
}

/// Op kinds for [`SharedHashMap`](crate::SharedHashMap).
pub mod hash_map {
    pub const OP_INSERT: u16 = 1;
    pub const OP_GET: u16 = 2;
    pub const OP_REMOVE: u16 = 3;
    pub const OP_CONTAINS: u16 = 4;
    pub const OP_CLEAR: u16 = 5;
    pub const OP_COMPACT: u16 = 6;
}

/// Op kinds for [`SharedRegion`](crate::SharedRegion).
pub mod region {
    pub const OP_ALLOCATE: u16 = 1;
    pub const OP_FREE: u16 = 2;
    pub const OP_GET: u16 = 3;
    pub const OP_SET: u16 = 4;
}

/// Op kinds for [`SharedBroadcastRing`](crate::SharedBroadcastRing).
pub mod broadcast_ring {
    pub const OP_PUSH: u16 = 1;
    pub const OP_RECV: u16 = 2;
    pub const OP_REGISTER: u16 = 3;
    pub const OP_UNREGISTER: u16 = 4;
}

/// Op kinds for [`SharedAtomicU32`](crate::SharedAtomicU32),
/// [`SharedAtomicU64`](crate::SharedAtomicU64), and
/// [`SharedAtomicBool`](crate::SharedAtomicBool).
pub mod atomic {
    pub const OP_LOAD: u16 = 1;
    pub const OP_STORE: u16 = 2;
    pub const OP_FETCH_ADD: u16 = 3;
    pub const OP_CAS: u16 = 4;
}

/// Op kinds for [`SharedBitVec`](crate::SharedBitVec).
pub mod bit_vec {
    pub const OP_SET: u16 = 1;
    pub const OP_CLEAR: u16 = 2;
    pub const OP_GET: u16 = 3;
    pub const OP_TOGGLE: u16 = 4;
    pub const OP_RANGE: u16 = 5;
    pub const OP_COUNT_ONES: u16 = 6;
}

/// Op kinds for [`SharedBloomFilter`](crate::SharedBloomFilter),
/// [`SharedCountMinSketch`](crate::SharedCountMinSketch),
/// [`SharedHyperLogLog`](crate::SharedHyperLogLog).
pub mod sketch {
    pub const OP_INSERT: u16 = 1;
    pub const OP_QUERY: u16 = 2;
    pub const OP_CLEAR: u16 = 3;
}

/// Op kinds for [`SharedHistogram`](crate::SharedHistogram).
pub mod histogram {
    pub const OP_RECORD: u16 = 1;
    pub const OP_COUNT: u16 = 2;
    pub const OP_PERCENTILE: u16 = 3;
}

/// Op kinds for [`SharedLinkedList`](crate::SharedLinkedList).
pub mod linked_list {
    pub const OP_PUSH_BACK: u16 = 1;
    pub const OP_PUSH_FRONT: u16 = 2;
    pub const OP_POP_BACK: u16 = 3;
    pub const OP_POP_FRONT: u16 = 4;
    pub const OP_REMOVE: u16 = 5;
    pub const OP_ITER: u16 = 6;
}

/// Op kinds for [`SharedLRUCache`](crate::SharedLRUCache).
pub mod lru_cache {
    pub const OP_GET: u16 = 1;
    pub const OP_PUT: u16 = 2;
    pub const OP_TOUCH: u16 = 3;
    pub const OP_REMOVE: u16 = 4;
    pub const OP_EVICT: u16 = 5;
}

/// Op kinds for [`SharedRWLock`](crate::SharedRWLock).
pub mod rw_lock {
    pub const OP_READ: u16 = 1;
    pub const OP_WRITE: u16 = 2;
    pub const OP_TRY_READ: u16 = 3;
    pub const OP_TRY_WRITE: u16 = 4;
}

/// Op kinds for [`SharedSemaphore`](crate::SharedSemaphore).
pub mod semaphore {
    pub const OP_ACQUIRE: u16 = 1;
    pub const OP_RELEASE: u16 = 2;
    pub const OP_TRY_ACQUIRE: u16 = 3;
}

/// Op kinds for [`SharedRateLimiter`](crate::SharedRateLimiter).
pub mod rate_limiter {
    pub const OP_TRY_ACQUIRE: u16 = 1;
    pub const OP_AVAILABLE: u16 = 2;
}

/// Op kinds for [`SharedBTreeMap`](crate::SharedBTreeMap) and
/// [`SharedTreiberStack`](crate::SharedTreiberStack) and
/// [`SharedVec`](crate::SharedVec).
pub mod ordered {
    pub const OP_INSERT: u16 = 1;
    pub const OP_GET: u16 = 2;
    pub const OP_REMOVE: u16 = 3;
    pub const OP_ITER: u16 = 4;
    pub const OP_POP: u16 = 5;
}

/// Op kinds for [`SharedStringArena`](crate::SharedStringArena).
pub mod string_arena {
    pub const OP_INTERN: u16 = 1;
    pub const OP_GET_BYTES: u16 = 2;
    pub const OP_CLEAR: u16 = 3;
}

/// Op kinds for [`SharedFenceClock`](crate::SharedFenceClock).
pub mod fence_clock {
    pub const OP_TICK: u16 = 1;
    pub const OP_MERGE: u16 = 2;
    pub const OP_GET_LOCAL: u16 = 3;
    pub const OP_COMPUTE_FENCE: u16 = 4;
}

/// Op kinds for [`HeartbeatTable`](crate::HeartbeatTable) and
/// [`EpochBarrier`](crate::EpochBarrier).
pub mod liveness {
    pub const OP_BEAT: u16 = 1;
    pub const OP_REGISTER: u16 = 2;
    pub const OP_WAIT: u16 = 3;
    pub const OP_TICK_EPOCH: u16 = 4;
    pub const OP_SCAN: u16 = 5;
}

/// Op kinds for [`SharedHandleTable`](crate::SharedHandleTable) and
/// [`OwnerLease`](crate::OwnerLease) and
/// [`SharedLeaderElection`](crate::SharedLeaderElection).
pub mod ownership {
    pub const OP_ACQUIRE: u16 = 1;
    pub const OP_RELEASE: u16 = 2;
    pub const OP_GET: u16 = 3;
    pub const OP_BEAT: u16 = 4;
    pub const OP_CLAIM: u16 = 5;
}

/// Op kinds for [`SharedReservoirSampler`](crate::SharedReservoirSampler).
pub mod reservoir {
    pub const OP_RECORD: u16 = 1;
    pub const OP_SNAPSHOT: u16 = 2;
}

/// Op kinds for [`SharedVersionedChain`](crate::SharedVersionedChain) and
/// [`SharedTimePointTile`](crate::SharedTimePointTile).
pub mod versioned {
    pub const OP_PUSH: u16 = 1;
    pub const OP_READ_AT: u16 = 2;
    pub const OP_CURRENT: u16 = 3;
    pub const OP_VISIBLE_MASK: u16 = 4;
}

/// Op kinds for [`SharedUniversal`](crate::SharedUniversal).
pub mod universal {
    pub const OP_INSERT: u16 = 1;
    pub const OP_CONTAINS: u16 = 2;
    pub const OP_REMOVE: u16 = 3;
    pub const OP_MIGRATE: u16 = 4;
}

/// Op kinds for [`SharedTopologyMap`](crate::SharedTopologyMap).
pub mod topology {
    pub const OP_RECORD: u16 = 1;
    pub const OP_FAN_OUT: u16 = 2;
    pub const OP_FAN_IN: u16 = 3;
    pub const OP_RECOMMEND: u16 = 4;
}

/// Op kinds for [`SharedGraph`](crate::SharedGraph).
pub mod graph {
    pub const OP_ADD_NODE: u16 = 1;
    pub const OP_ADD_EDGE: u16 = 2;
    pub const OP_NEIGHBORS: u16 = 3;
    pub const OP_REMOVE_EDGE: u16 = 4;
}

/// Op kinds for [`PriorityFanout`](crate::PriorityFanout).
pub mod priority_fanout {
    pub const OP_SUBMIT: u16 = 1;
    pub const OP_DRAIN_HIGHEST: u16 = 2;
    pub const OP_DRAIN_PRIORITY: u16 = 3;
}

/// Op kinds for [`EventStateLog`](crate::EventStateLog).
pub mod event_log {
    pub const OP_EMIT: u16 = 1;
    pub const OP_DRAIN_FOLD: u16 = 2;
    pub const OP_READ_CURRENT: u16 = 3;
}

/// Op kinds for [`ProgressTask`](crate::ProgressTask).
pub mod progress {
    pub const OP_ADVANCE: u16 = 1;
    pub const OP_READ: u16 = 2;
    pub const OP_COMPLETE: u16 = 3;
}

/// Op kinds for [`LazyConfig`](crate::LazyConfig).
pub mod lazy_config {
    pub const OP_GET: u16 = 1;
    pub const OP_FETCH: u16 = 2;
}

/// Op kinds for [`BackgroundScheduler`](crate::BackgroundScheduler) and
/// [`FailoverWatchdog`](crate::failover::FailoverWatchdog).
pub mod scheduler {
    pub const OP_SUBMIT: u16 = 1;
    pub const OP_RECV: u16 = 2;
    pub const OP_WATCHDOG_SCAN: u16 = 3;
}

/// Op kinds for [`SharedAsyncPointer`](crate::SharedAsyncPointer).
pub mod async_pointer {
    pub const OP_GET_OR_FETCH: u16 = 1;
    pub const OP_TRY_GET: u16 = 2;
}

/// Op kinds for [`SharedUmbraPointer`](crate::SharedUmbraPointer).
pub mod umbra_pointer {
    pub const OP_PREFIX_EQ: u16 = 1;
    pub const OP_RESOLVE: u16 = 2;
}

/// Op kinds for [`KTowerCascade`](crate::KTowerCascade) cascade resolvers.
pub mod cascade {
    pub const OP_INSERT: u16 = 1;
    pub const OP_GET: u16 = 2;
}

/// Op kinds for the ordering layer of
/// [`AdaptiveRing`](crate::AdaptiveRing): one observation per
/// cross-producer inversion detected at pop. The sidecar's drain
/// folds these into the per-instance op counts, giving policies the
/// inversion rate that justifies (or kills) the stamped-merge mode
/// with data.
pub mod ordering {
    pub const OP_ORDER_INVERSION: u16 = 1;
}
