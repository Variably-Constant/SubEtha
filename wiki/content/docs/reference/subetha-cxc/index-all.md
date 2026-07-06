---
weight: 100
---

# Alphabetical index

Every primitive that ships a canonical `docs/pointers/*.md` design
doc, alphabetised, with a link to that doc. The polymorphic-substrate
rings, the blocking/async wrappers, the cross-host bridges, and the
OS-specific primitives are documented in source rather than a
`pointers/*.md`; the [master catalog](catalog.md) lists those too.

For category-grouped pages with prose, see
[the subetha-cxc index](../) and the per-category pages it
links to.

## B

- `BackgroundScheduler` -
  [SCHEDULER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SCHEDULER.md)
  (see [coordination.md](coordination.md#backgroundscheduler))

## E

- `EpochBarrier` -
  [EPOCH_BARRIER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/EPOCH_BARRIER.md)
  (see [coordination.md](coordination.md#epochbarrier))
- `EventStateLog` -
  [EVENT_STATE_LOG.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/EVENT_STATE_LOG.md)
  (see [coordination.md](coordination.md#eventstatelog))

## F

- `FailoverWatchdog` -
  [FAILOVER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/FAILOVER.md)
  (see [coordination.md](coordination.md#failoverwatchdog))

## H

- `HeartbeatTable` -
  [HEARTBEAT.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/HEARTBEAT.md)
  (see [coordination.md](coordination.md#heartbeattable))

## K

- `KTowerCascade` -
  [K_TOWER_CASCADE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/K_TOWER_CASCADE.md)
  (see [coordination.md](coordination.md#ktowercascade))

## L

- `LazyConfig` -
  [LAZY_CONFIG.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/LAZY_CONFIG.md)
  (see [ownership.md](ownership.md#lazyconfig))

## O

- `OffsetPtr` -
  [OFFSET_PTR.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/OFFSET_PTR.md)
- `OwnerLease` -
  [OWNER_LEASE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/OWNER_LEASE.md)
  (see [ownership.md](ownership.md#ownerlease))

## P

- `pass_registry` (top-level fns
  `register` / `unregister` / `execute` /
  `is_registered` / `registered_count`, plus the
  `register_pass!` macro) -
  [PASS_REGISTRY.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/PASS_REGISTRY.md)
  (see [coordination.md](coordination.md#passregistry))
- `PriorityFanout` -
  [PRIORITY_FANOUT.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/PRIORITY_FANOUT.md)
  (see [coordination.md](coordination.md#priorityfanout))
- `ProgressTask` -
  [PROGRESS_TASK.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/PROGRESS_TASK.md)
  (see [coordination.md](coordination.md#progresstask))

## S

- `SharedAsyncPointer` -
  [SHARED_ASYNC_POINTER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ASYNC_POINTER.md)
  (see [coordination.md](coordination.md#sharedasyncpointer))
- `SharedAtomicBool` / `SharedAtomicU32` / `SharedAtomicU64` -
  [SHARED_ATOMIC.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ATOMIC.md)
  (see [shared-atomic.md](shared-atomic.md))
- `SharedBitVec` -
  [SHARED_BIT_VEC.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BIT_VEC.md)
  (see [shared-sketches.md](shared-sketches.md#sharedbitvec))
- `SharedBloomFilter` -
  [SHARED_BLOOM_FILTER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BLOOM_FILTER.md)
  (see [shared-sketches.md](shared-sketches.md#sharedbloomfilter))
- `SharedBlockedBloomFilter` -
  [SHARED_BLOCKED_BLOOM_FILTER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BLOCKED_BLOOM_FILTER.md)
  (see [sketches/shared-blocked-bloom-filter.md](sketches/shared-blocked-bloom-filter.md))
- `SharedBroadcastRing` -
  [SHARED_BROADCAST_RING.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BROADCAST_RING.md)
  (see [shared-ring.md](shared-ring.md#sharedbroadcastring))
- `SharedCell` -
  [SHARED_CELL.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_CELL.md)
  (see [shared-cell.md](shared-cell.md#sharedcell))
- `SharedCountMinSketch` -
  [SHARED_COUNT_MIN_SKETCH.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_COUNT_MIN_SKETCH.md)
  (see [shared-sketches.md](shared-sketches.md#sharedcountminsketch))
- `SharedFenceClock` -
  [SHARED_FENCE_CLOCK.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_FENCE_CLOCK.md)
  (see [shared-locks.md](shared-locks.md#sharedfenceclock))
- `SharedGraph` -
  [SHARED_GRAPH.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_GRAPH.md)
  (see [coordination.md](coordination.md#sharedgraph))
- `SharedHandleTable` -
  [SHARED_HANDLE_TABLE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HANDLE_TABLE.md)
  (see [shared-sketches.md](shared-sketches.md#sharedhandletable))
- `SharedHashMap` -
  [SHARED_HASH_MAP.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HASH_MAP.md)
  (see [shared-hash-map.md](shared-hash-map.md#sharedhashmapk-v))
- `SharedHistogram` -
  [SHARED_HISTOGRAM.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HISTOGRAM.md)
  (see [shared-sketches.md](shared-sketches.md#sharedhistogram))
- `SharedHyperLogLog` -
  [SHARED_HYPER_LOG_LOG.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HYPER_LOG_LOG.md)
  (see [shared-sketches.md](shared-sketches.md#sharedhyperloglog))
- `SharedLeaderElection` -
  [SHARED_LEADER_ELECTION.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LEADER_ELECTION.md)
  (see [ownership.md](ownership.md#sharedleaderelection))
- `SharedLinkedList` -
  [SHARED_LINKED_LIST.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LINKED_LIST.md)
  (see [coordination.md](coordination.md#vec-and-linked-list))
- `SharedLRUCache` -
  [SHARED_LRU_CACHE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LRU_CACHE.md)
  (see [shared-lru-cache.md](shared-lru-cache.md))
- `SharedNaNTaggedValue` / `SharedNaNValue` -
  [SHARED_NAN_TAGGED_VALUE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_NAN_TAGGED_VALUE.md),
  [SHARED_NAN_VALUE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_NAN_VALUE.md)
  (see [coordination.md](coordination.md#sharednanvalue-and-sharednantaggedvalue))
- `SharedOnceCell` -
  [SHARED_ONCE_CELL.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ONCE_CELL.md)
  (see [shared-cell.md](shared-cell.md#sharedoncecell))
- `SharedRateLimiter` -
  [SHARED_RATE_LIMITER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RATE_LIMITER.md)
  (see [shared-locks.md](shared-locks.md#sharedratelimiter))
- `SharedRegion` -
  [SHARED_REGION.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_REGION.md)
  (see [coordination.md](coordination.md#sharedregion))
- `SharedReservoirSampler` -
  [SHARED_RESERVOIR_SAMPLER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RESERVOIR_SAMPLER.md)
  (see [shared-sketches.md](shared-sketches.md#sharedreservoirsampler))
- `SharedRing` -
  [SHARED_RING.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RING.md)
  (see [shared-ring.md](shared-ring.md#sharedring))
- `SharedRWLock` -
  [SHARED_RW_LOCK.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RW_LOCK.md)
  (see [shared-locks.md](shared-locks.md#sharedrwlock))
- `SharedSemaphore` -
  [SHARED_SEMAPHORE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_SEMAPHORE.md)
  (see [shared-locks.md](shared-locks.md#sharedsemaphore))
- `SharedBTreeMap` -
  [SHARED_BTREE_MAP.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BTREE_MAP.md)
  (see [shared-hash-map.md](shared-hash-map.md#sharedbtreemap))
- `SharedStringArena` -
  [SHARED_STRING_ARENA.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_STRING_ARENA.md)
  (see [shared-sketches.md](shared-sketches.md#sharedstringarena))
- `SharedTimePointTile` -
  [SHARED_TIME_POINT.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TIME_POINT.md)
  (see [coordination.md](coordination.md#sharedtimepointtile))
- `SharedTopologyMap` -
  [SHARED_TOPOLOGY_MAP.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TOPOLOGY_MAP.md)
  (see [coordination.md](coordination.md#sharedtopologymap))
- `SharedTreiberStack` -
  [SHARED_TREIBER_STACK.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TREIBER_STACK.md)
  (see [shared-ring.md](shared-ring.md#sharedtreiberstack))
- `SharedUmbraPointer` -
  [SHARED_UMBRA_POINTER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_UMBRA_POINTER.md)
  (see [coordination.md](coordination.md#sharedumbrapointer))
- `SharedUniversal` -
  [SHARED_UNIVERSAL.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_UNIVERSAL.md)
  (see [coordination.md](coordination.md#shareduniversal))
- `SharedVec` -
  [SHARED_VEC.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_VEC.md)
  (see [coordination.md](coordination.md#vec-and-linked-list))
- `SharedVersionedChain` -
  [SHARED_VERSIONED_CHAIN.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_VERSIONED_CHAIN.md)
  (see [coordination.md](coordination.md#sharedversionedchain))

## T

- `TaggedOffsetPtr` -
  [TAGGED_OFFSET_PTR.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/TAGGED_OFFSET_PTR.md)
