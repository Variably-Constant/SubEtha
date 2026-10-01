---
weight: 100
---

# Alphabetical index

The primitives below, alphabetized, each with a link to its reference
page. The polymorphic-substrate rings, the blocking/async wrappers, the
cross-host bridges, and the OS-specific primitives are not listed here;
the [master catalog](catalog.md) lists them.

For category-grouped pages with prose, see
[the subetha-cxc index](../) and the per-category pages it
links to.

## B

- `BackgroundScheduler` -
  [Background Scheduler](coordination-types/scheduler/)
  (see [coordination.md](coordination.md#backgroundscheduler))

## E

- `EpochBarrier` -
  [Epoch Barrier](coordination-types/epoch-barrier/)
  (see [coordination.md](coordination.md#epochbarrier))
- `EventStateLog` -
  [Event State Log](coordination-types/event-state-log/)
  (see [coordination.md](coordination.md#eventstatelog))

## F

- `FailoverWatchdog` -
  [Failover Watchdog](coordination-types/failover/)
  (see [coordination.md](coordination.md#failoverwatchdog))

## H

- `HeartbeatTable` -
  [Heartbeat Table](coordination-types/heartbeat/)
  (see [coordination.md](coordination.md#heartbeattable))
- `HolderTable` -
  [Holder Table](coordination-types/holder-table/)
  (see [coordination-types/holder-table.md](coordination-types/holder-table.md))

## K

- `KTowerCascade` -
  [K-Tower Cascade](coordination-types/k-tower-cascade/)
  (see [coordination.md](coordination.md#ktowercascade))

## L

- `LanedVersionedMap` -
  [Laned Versioned Map](maps/laned-versioned-map/)
  (see [maps/laned-versioned-map.md](maps/laned-versioned-map.md))
- `LazyConfig` -
  [Lazy Config](ownership-types/lazy-config/)
  (see [ownership.md](ownership.md#lazyconfig))

## O

- `OffsetPtr` -
  [Offset Pointer](pointers/offset-ptr/)
- `OwnerLease` -
  [Owner Lease](ownership-types/owner-lease/)
  (see [ownership.md](ownership.md#ownerlease))

## P

- `pass_registry` (top-level fns
  `register` / `unregister` / `execute` /
  `is_registered` / `registered_count`, plus the
  `register_pass!` macro) -
  [Pass Registry](coordination-types/pass-registry/)
  (see [coordination.md](coordination.md#pass_registry))
- `PriorityFanout` -
  [Priority Fanout](coordination-types/priority-fanout/)
  (see [coordination.md](coordination.md#priorityfanout))
- `ProgressTask` -
  [Progress Task](coordination-types/progress-task/)
  (see [coordination.md](coordination.md#progresstask))

## S

- `SharedAsyncPointer` -
  [Shared Async Pointer](coordination-types/shared-async-pointer/)
  (see [coordination.md](coordination.md#sharedasyncpointer))
- `SharedAtomicBool` / `SharedAtomicU32` / `SharedAtomicU64` -
  [Shared Atomic](atomics/shared-atomic/)
  (see [shared-atomic.md](shared-atomic.md))
- `SharedBitVec` -
  [Shared Bit Vec](sketches/shared-bit-vec/)
  (see [shared-sketches.md](shared-sketches.md#sharedbitvec))
- `SharedBloomFilter` -
  [Shared Bloom Filter](sketches/shared-bloom-filter/)
  (see [shared-sketches.md](shared-sketches.md#sharedbloomfilter))
- `SharedBlockedBloomFilter` -
  [Shared Blocked Bloom Filter](sketches/shared-blocked-bloom-filter/)
  (see [sketches/shared-blocked-bloom-filter.md](sketches/shared-blocked-bloom-filter.md))
- `SharedBroadcastRing` -
  [Shared Broadcast Ring](rings/shared-broadcast-ring/)
  (see [shared-ring.md](shared-ring.md#sharedbroadcastring))
- `SharedCell` -
  [Shared Cell](cells/shared-cell/)
  (see [shared-cell.md](shared-cell.md#sharedcell))
- `SharedCountMinSketch` -
  [Shared Count-Min Sketch](sketches/shared-count-min-sketch/)
  (see [shared-sketches.md](shared-sketches.md#sharedcountminsketch))
- `SharedArc` -
  [Shared Arc](ownership-types/shared-arc/)
  (see [ownership-types/shared-arc.md](ownership-types/shared-arc.md))
- `SharedEpochs` -
  [Shared Epochs](coordination-types/shared-epochs/)
  (see [coordination-types/shared-epochs.md](coordination-types/shared-epochs.md))
- `SharedFenceClock` -
  [Shared Fence Clock](locks/shared-fence-clock/)
  (see [shared-locks.md](shared-locks.md#sharedfenceclock))
- `SharedGraph` -
  [Shared Graph](specialized/shared-graph/)
  (see [coordination.md](coordination.md#sharedgraph))
- `SharedHandleTable` -
  [Shared Handle Table](arenas/shared-handle-table/)
  (see [shared-sketches.md](shared-sketches.md#sharedhandletable))
- `SharedHashMap` -
  [Shared Hash Map](maps/shared-hash-map/)
  (see [shared-hash-map.md](shared-hash-map.md#sharedhashmapk-v))
- `SharedHistogram` -
  [Shared Histogram](sketches/shared-histogram/)
  (see [shared-sketches.md](shared-sketches.md#sharedhistogram))
- `SharedHyperLogLog` -
  [Shared HyperLogLog](sketches/shared-hyper-log-log/)
  (see [shared-sketches.md](shared-sketches.md#sharedhyperloglog))
- `SharedLeaderElection` -
  [Shared Leader Election](ownership-types/shared-leader-election/)
  (see [ownership.md](ownership.md#sharedleaderelection))
- `SharedLinkedList` -
  [Shared Linked List](maps/shared-linked-list/)
  (see [coordination.md](coordination.md#vec-and-linked-list))
- `SharedLRUCache` -
  [Shared LRU Cache](caches/shared-lru-cache/)
  (see [shared-lru-cache.md](shared-lru-cache.md))
- `SharedNamedValues` -
  [Shared Named Values](maps/shared-named-values/)
  (see [maps/shared-named-values.md](maps/shared-named-values.md))
- `SharedNaNTaggedValue` / `SharedNaNValue` -
  [Shared NaN-Tagged Value](specialized/shared-nan-tagged-value/),
  [Shared NaN Value](specialized/shared-nan-value/)
  (see [coordination.md](coordination.md#sharednanvalue-and-sharednantaggedvalue))
- `SharedOnceCell` -
  [Shared Once Cell](cells/shared-once-cell/)
  (see [shared-cell.md](shared-cell.md#sharedoncecell))
- `SharedRateLimiter` -
  [Shared Rate Limiter](locks/shared-rate-limiter/)
  (see [shared-locks.md](shared-locks.md#sharedratelimiter))
- `SharedRegion` -
  [Shared Region](arenas/shared-region/)
  (see [coordination.md](coordination.md#sharedregion))
- `SharedReservoirSampler` -
  [Shared Reservoir Sampler](sketches/shared-reservoir-sampler/)
  (see [shared-sketches.md](shared-sketches.md#sharedreservoirsampler))
- `SharedRing` -
  [Shared Ring](rings/shared-ring/)
  (see [shared-ring.md](shared-ring.md#sharedring))
- `SharedRWLock` -
  [Shared RW Lock](locks/shared-rw-lock/)
  (see [shared-locks.md](shared-locks.md#sharedrwlock))
- `SharedSemaphore` -
  [Shared Semaphore](locks/shared-semaphore/)
  (see [shared-locks.md](shared-locks.md#sharedsemaphore))
- `SharedBTreeMap` -
  [Shared B-Tree Map](maps/shared-btree-map/)
  (see [shared-hash-map.md](shared-hash-map.md#sharedbtreemap))
- `SharedSlab` -
  [Shared Slab](specialized/shared-slab/)
  (see [specialized/shared-slab.md](specialized/shared-slab.md))
- `SharedArray` -
  [Shared Array](specialized/shared-array/)
  (see [specialized/shared-array.md](specialized/shared-array.md))
- `SharedStringArena` -
  [Shared String Arena](arenas/shared-string-arena/)
  (see [shared-sketches.md](shared-sketches.md#sharedstringarena))
- `SharedTimePointTile` -
  [Shared Time Point](specialized/shared-time-point/)
  (see [coordination.md](coordination.md#sharedtimepointtile))
- `SharedTopologyMap` -
  [Shared Topology Map](specialized/shared-topology-map/)
  (see [coordination.md](coordination.md#sharedtopologymap))
- `SharedTreiberStack` -
  [Shared Treiber Stack](rings/shared-treiber-stack/)
  (see [shared-ring.md](shared-ring.md#sharedtreiberstack))
- `SharedUmbraPointer` -
  [Shared Umbra Pointer](specialized/shared-umbra-pointer/)
  (see [coordination.md](coordination.md#sharedumbrapointer))
- `SharedUniversal` -
  [Shared Universal](specialized/shared-universal/)
  (see [coordination.md](coordination.md#shareduniversal))
- `SharedVec` -
  [Shared Vec](specialized/shared-vec/)
  (see [coordination.md](coordination.md#vec-and-linked-list))
- `SharedVersionedChain` -
  [Shared Versioned Chain](specialized/shared-versioned-chain/)
  (see [coordination.md](coordination.md#sharedversionedchain))
- `SharedVersionedSlab` -
  [Shared Versioned Slab](specialized/shared-versioned-slab/)
  (see [specialized/shared-versioned-slab.md](specialized/shared-versioned-slab.md))

## T

- `TaggedOffsetPtr` -
  [Tagged Offset Pointer](pointers/tagged-offset-ptr/)

## V

- `VersionedBTreeMap` -
  [Versioned BTree Map](maps/versioned-btree-map/)
  (see [maps/versioned-btree-map.md](maps/versioned-btree-map.md))
