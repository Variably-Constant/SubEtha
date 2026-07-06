---
title: Catalog
weight: 5
---

# `subetha-cxc` master catalog

Every MMF-backed primitive in `subetha-cxc`, grouped by category,
with a one-line description and a "use when..." hint per type. The
**Type** column links to the per-category page where the primitive's
prose doc lives; the **Source** column links to its canonical
in-source-tree `.md` (the per-type design doc).

For the alphabetised lookup (every name A-Z), see
[index-all](index-all/). For the role-pair-driven selection
guide, see [Pick the right primitive](../../how-to/role-pair-selection/).

## Rings, stacks, and queues

Bounded, lock-free FIFO / LIFO / pub-sub structures.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`SharedRing<P>`](shared-ring/) | Cross-thread / cross-process lock-free MPMC ring | Multiple producers AND multiple consumers compete on one bounded queue | [SHARED_RING.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RING.md) |
| [`SharedBroadcastRing`](shared-ring/#sharedbroadcastring) | Single-producer, multi-consumer pub/sub ring | One process broadcasts events; many subscribers each consume the full stream independently | [SHARED_BROADCAST_RING.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BROADCAST_RING.md) |
| [`SharedTreiberStack<T>`](shared-ring/#sharedtreiberstack) | Cross-process lock-free LIFO stack | LIFO ordering matters and contention is moderate; one CAS per push/pop | [SHARED_TREIBER_STACK.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TREIBER_STACK.md) |
| [`BlockingSpscRing`](rings/blocking-spsc-ring/) | SPSC ring + 2 `CrossProcessWaker` for cross-process blocking send / recv | Single producer + single consumer want to park kernel-side instead of spinning when the ring is empty / full; cross-process safe on Linux via SHARED `futex` | [blocking_spsc_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/blocking_spsc_ring.rs) |
| [`BlockingMpscRing`](rings/blocking-mpsc-ring/) | Composed-SPSC MPSC fan-in + per-ring producer wakers + shared consumer waker | N producers + 1 consumer want cross-process blocking semantics; per-producer FIFO; consumer parks on a shared waker any producer can fire | [blocking_mpsc_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/blocking_mpsc_ring.rs) |
| [`BlockingMpmcRing`](rings/blocking-mpmc-ring/) | Composed-SPSC MPMC grid + per-ring producer wakers + per-subset consumer wakers | N producers + M consumers want cross-process blocking semantics; each consumer owns a subset of rings and parks on its own waker | [blocking_mpmc_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/blocking_mpmc_ring.rs) |

## Maps, lists, and sequences

Keyed lookup and ordered storage.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`SharedHashMap<K, V>`](shared-hash-map/) | Cross-process open-addressed hash map | Key-value with O(1) lookup; FNV-1a hashing for cross-process determinism | [SHARED_HASH_MAP.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HASH_MAP.md) |
| [`SharedBTreeMap<K, V>`](shared-hash-map/#sharedbtreemap) | Cross-process ordered map via B-tree | Key-value with **ordered** iteration; range queries needed | [SHARED_BTREE_MAP.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BTREE_MAP.md) |
| [`SharedLinkedList<T>`](shared-hash-map/#sharedlinkedlist) | Cross-process doubly-linked list | Need stable iterator positions across mutations; not random access | [SHARED_LINKED_LIST.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LINKED_LIST.md) |
| `SharedVec<T>` | Cross-process bounded indexable sequence | Push/index/pop with a known capacity ceiling | [SHARED_VEC.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_VEC.md) |

## Atomics and cells

Scalar shared state.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`SharedAtomicU32` / `SharedAtomicU64` / `SharedAtomicBool`](shared-atomic/) | Cross-process atomic counter / flag | Single integer or bool flag shared across processes; cheaper than any map | [SHARED_ATOMIC.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ATOMIC.md) |
| [`SharedCell<T>`](shared-cell/) | Cross-process single-value cell | One typed value updated atomically; reads and writes from any process | [SHARED_CELL.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_CELL.md) |
| [`SharedOnceCell<T>`](shared-cell/#sharedoncecell) | Cross-process init-once cell | Initialise a value exactly once; subsequent processes read the cached result | [SHARED_ONCE_CELL.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ONCE_CELL.md) |
| `SharedAsyncPointer<T>` | Cross-process lazy / speculative pointer | Speculative reads; the first process to materialise wins, others race-free observe | [SHARED_ASYNC_POINTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ASYNC_POINTER.md) |

## Caches

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`SharedLRUCache<K, V>`](shared-lru-cache/) | Cross-process LRU cache | Bounded keyed cache with eviction; shared by many processes | [SHARED_LRU_CACHE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LRU_CACHE.md) |

## Locks and synchronisation

Mutual-exclusion and rate-limiting primitives.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`SharedRWLock`](shared-locks/) | Cross-process reader-writer lock with writer preference | Many readers, occasional writer; readers must not block each other | [SHARED_RW_LOCK.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RW_LOCK.md) |
| [`SharedSemaphore`](shared-locks/#sharedsemaphore) | Cross-process counting semaphore | Bounded resource pool (N concurrent users); acquire / release pattern | [SHARED_SEMAPHORE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_SEMAPHORE.md) |
| [`SharedRateLimiter`](shared-locks/#sharedratelimiter) | Cross-process token-bucket rate limiter | Throttle requests across many processes against one shared budget | [SHARED_RATE_LIMITER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RATE_LIMITER.md) |
| [`SharedFenceClock`](shared-locks/#sharedfenceclock) | Hybrid Logical Clock (HLC) lifted to a process boundary | Need a monotonic timestamp that orders events across processes | [SHARED_FENCE_CLOCK.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_FENCE_CLOCK.md) |

## Probabilistic sketches

Approximate aggregations - sub-linear memory for the cardinality of values they see.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`SharedBitVec`](shared-sketches/#sharedbitvec) | Cross-process bit-packed boolean array | Dense set membership over a known small key space | [SHARED_BIT_VEC.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BIT_VEC.md) |
| [`SharedBloomFilter`](shared-sketches/#sharedbloomfilter) | Cross-process probabilistic set membership | Approximate "has key X been seen?" with controlled false-positive rate | [SHARED_BLOOM_FILTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BLOOM_FILTER.md) |
| [`SharedBlockedBloomFilter`](sketches/shared-blocked-bloom-filter/) | Cache-blocked probabilistic set membership | Large-scale membership where one cache line per query matters (past L3) | [SHARED_BLOCKED_BLOOM_FILTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BLOCKED_BLOOM_FILTER.md) |
| [`SharedCountMinSketch`](shared-sketches/#sharedcountminsketch) | Cross-process probabilistic frequency counter | Approximate counts per key without keeping the keys themselves | [SHARED_COUNT_MIN_SKETCH.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_COUNT_MIN_SKETCH.md) |
| [`SharedHyperLogLog`](shared-sketches/#sharedhyperloglog) | Cross-process probabilistic distinct-count | Count unique elements with very low memory; merges across processes | [SHARED_HYPER_LOG_LOG.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HYPER_LOG_LOG.md) |
| [`SharedHistogram`](shared-sketches/#sharedhistogram) | Cross-process bucketed counter | Latency / value distributions binned at fixed buckets | [SHARED_HISTOGRAM.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HISTOGRAM.md) |
| [`SharedReservoirSampler<T>`](shared-sketches/#sharedreservoirsampler) | Cross-process uniform random sample | Sample N items from an unknown-size stream | [SHARED_RESERVOIR_SAMPLER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RESERVOIR_SAMPLER.md) |

## Arenas and region storage

Pool allocators backed by an MMF.

| Type | What it is | Use when | Source |
|---|---|---|---|
| `SharedStringArena` | Append-only position-independent string arena | Many small strings pooled in one MMF; refer to them by offset | [SHARED_STRING_ARENA.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_STRING_ARENA.md) |
| `SharedHandleTable<T>` | Cross-process ECS-style slotmap | Generational handles to slot-allocated entities; like an ECS world shared across processes | [SHARED_HANDLE_TABLE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HANDLE_TABLE.md) |
| `SharedRegion<T>` | Cross-process typed arena with position-independent pointers | Bulk allocation of T inside an MMF; offset pointers between regions | [SHARED_REGION.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_REGION.md) |

## Ownership and election

Who-holds-the-token primitives.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`OwnerLease<T>`](ownership/) | Cross-process Mutex with auto-failover | Exclusive resource access where the holder might die; lease auto-reassigns | [OWNER_LEASE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/OWNER_LEASE.md) |
| [`SharedLeaderElection`](ownership/#sharedleaderelection) | Cross-process leader election | Exactly one process plays the leader role; auto-elect a replacement on death | [SHARED_LEADER_ELECTION.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LEADER_ELECTION.md) |
| [`LazyConfig<T>`](ownership/#lazyconfig) | Thundering-herd-proof distributed config fetch | Many processes need the same config; only ONE actually fetches it; rest read | [LAZY_CONFIG.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/LAZY_CONFIG.md) |

## Liveness, failover, and barriers

Coordination across process boundaries.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`HeartbeatTable`](coordination/#heartbeattable) | Per-process heartbeat slots in an MMF | Discover which peer processes are alive; the table backs failover | [HEARTBEAT.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/HEARTBEAT.md) |
| [`FailoverWatchdog`](coordination/#failoverwatchdog) | Scans the heartbeat table and reclaims work from dead peers | Reassign owner-leases / leader-roles when a process dies | [FAILOVER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/FAILOVER.md) |
| [`EpochBarrier`](coordination/#epochbarrier) | Multi-process phase synchronisation | All N processes must finish phase K before any starts phase K+1 | [EPOCH_BARRIER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/EPOCH_BARRIER.md) |

## Work distribution

Higher-level coordination layered on the substrate.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`EventStateLog<E, S>`](coordination/#eventstatelog) | Event-sourced state with cross-process replay | Append-only event log + materialised state; readers reconstruct from log | [EVENT_STATE_LOG.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/EVENT_STATE_LOG.md) |
| [`PriorityFanout`](coordination/#priorityfanout) | Tiered work queue with O(1) priority selection | N priority classes; consumers grab work from the highest non-empty class | [PRIORITY_FANOUT.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/PRIORITY_FANOUT.md) |
| [`ProgressTask<R>`](coordination/#progresstask) | Distributed work with live cross-process progress reporting | Long-running task split across processes; UI watches aggregated progress | [PROGRESS_TASK.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/PROGRESS_TASK.md) |
| [`BackgroundScheduler`](coordination/#backgroundscheduler) | Autonomous `Pass` executor backed by the MMF | Schedule periodic / triggered work; survives process restart | [SCHEDULER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SCHEDULER.md) |
| [`pass_registry`](coordination/#passregistry) | Closure registry for cross-process `Pass` dispatch | Register handlers in process A; process B fires them via `execute` | [PASS_REGISTRY.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/PASS_REGISTRY.md) |
| [`CrossProcessWaker`](coordination-types/cross-process-waker/) | Userspace-`futex` slot list in MMF. Every wait runs the hardware monitor tier first (MONITORX/UMONITOR on x86-64, WFE on aarch64); kernel parks are SHARED `futex` (Linux), non-PRIVATE `_umtx_op` (FreeBSD), `os_sync_wait_on_address` SHARED (macOS 14.4+), `WaitOnAddress` (Windows anon backings; cross-process Windows wakes ride the monitor tier) | Backs the `Blocking{Spsc,Mpsc,Mpmc}Ring` wrappers; usable directly by callers who need cross-process park / wake with a per-slot target sequence | [cross_process_waker.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/cross_process_waker.rs) |
| [`SharedCondvar`](coordination-types/shared-condvar/) | Cross-process Mesa-style condition variable; one generation counter + `CrossProcessWaker` | Callers want condvar semantics across processes; predicate atom is caller-owned (any MMF-resident bool / counter); cross-process wake on Linux/WSL via SHARED `futex` | [shared_condvar.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/shared_condvar.rs) |
| [`BlockingSemaphore`](coordination-types/blocking-semaphore/) | Cross-process counting semaphore with kernel-park slow path | Callers want `SharedSemaphore` semantics but with zero CPU at idle and microsecond wake latency on `release` | [blocking_semaphore.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/blocking_semaphore.rs) |
| [`BlockingRWLock`](coordination-types/blocking-rw-lock/) | Cross-process reader-writer lock with kernel-park slow path | Callers want `SharedRWLock` semantics with zero CPU at idle; readers and writers both park on the same waker | [blocking_rw_lock.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/blocking_rw_lock.rs) |
| [`AsyncSpscRing`](rings/async-spsc-ring/) | `Future`-shaped adapter on `BlockingSpscRing` | Callers want `.recv().await` / `.send().await` semantics with any async executor (tokio, smol, async-std, custom); short-lived `std::thread` per in-flight future bridges kernel-park to Rust `Waker` | [async_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/async_ring.rs) |
| [`BlockingTcpBridge`](bridges/blocking-tcp-bridge/) | TCP bridge whose forwarder uses `recv_blocking` / `send_blocking` via `spawn_blocking` | Callers want the existing `TcpBridge`'s wire format but with zero CPU at idle on both sides; replaces `tokio::task::yield_now` polling with cross-process futex park | [blocking_tcp_bridge.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/blocking_tcp_bridge.rs) |

## Specialised data structures

Less common shapes for specific workloads.

| Type | What it is | Use when | Source |
|---|---|---|---|
| `SharedVersionedChain<T>` | Cross-process MVCC linked list | Time-travel reads at a versioned snapshot; writers append new versions | [SHARED_VERSIONED_CHAIN.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_VERSIONED_CHAIN.md) |
| `SharedTimePointTile<T>` | BSPA + Versioned tile (16-slot snapshot-isolation scan) | Time-point queries over a small set of slots; SIMD lane mask scan | [SHARED_TIME_POINT.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TIME_POINT.md) |
| `SharedNaNValue` | 64-bit NaN-boxed heterogeneous value cell | Pack a small typed value (int / float / short string) into one f64 slot | [SHARED_NAN_VALUE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_NAN_VALUE.md) |
| `SharedNaNTaggedValue` | NaN-boxed value where the pointer bits identify the payload type | Polymorphic value cell with no out-of-line type tag | [SHARED_NAN_TAGGED_VALUE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_NAN_TAGGED_VALUE.md) |
| `SharedGraph<N, E>` | Cross-process directed graph | Cross-process graph adjacency; nodes and edges in one MMF | [SHARED_GRAPH.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_GRAPH.md) |
| `SharedUniversal<T>` | Layer-2 cross-process container that adapts strategy | Single container that auto-picks among the IPC families based on observed load | [SHARED_UNIVERSAL.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_UNIVERSAL.md) |
| `SharedTopologyMap` | K_process axis observer + recommendation surface | Watch peer-process distribution; surface placement hints for cross-process work | [SHARED_TOPOLOGY_MAP.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TOPOLOGY_MAP.md) |
| `KTowerCascade<T, DEPTH>` | Recursive pow2-of-pow2 cascading container | Multi-resolution indexed access; each tower level halves resolution | [K_TOWER_CASCADE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/K_TOWER_CASCADE.md) |
| `SharedUmbraPointer<T>` | Cross-process content-prefixed pointer | Pointer comparisons that short-circuit on content prefix before deref | [SHARED_UMBRA_POINTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_UMBRA_POINTER.md) |

## IPC pointers (addressing primitives)

Low-level pointer types that other primitives compose into. Use these directly only when building a new MMF-backed type.

| Type | What it is | Use when | Source |
|---|---|---|---|
| `OffsetPtr<T>` | File-relative offset pointer (no tag bits) | Pointing into the same MMF from another process; offset from base | [OFFSET_PTR.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/OFFSET_PTR.md) |
| `TaggedOffsetPtr<T, TAG_BITS>` | High-bit-stealing tagged offset pointer | Same as `OffsetPtr` but you need to pack a small tag (state, type, generation) alongside the offset | [TAGGED_OFFSET_PTR.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/TAGGED_OFFSET_PTR.md) |

## Polymorphic substrate (Locale x Protocol x Shape x Capacity x Ordering)

Cross-axis primitives that compose under one pin-protocol contract.
Each entry's "Use when" is the situation that the substrate's
default-composed stack does NOT cover automatically.

| Type | What it is | Use when | Source |
|---|---|---|---|
| [`AdaptiveRing`](rings/shared-ring-adaptive/) | Shape-morphing ring with all 4 shapes pre-allocated; peers register / unregister at runtime and the per-producer backings grow on demand (shared peer directory) | Default ring type; shape auto-morphs SPSC -> MPSC -> MPMC to the live peer counts, Vyukov on declaration; registration errors only under an explicit `with_contract` ceiling | [adaptive_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/adaptive_ring.rs) |
| [Adaptive ordering](rings/adaptive-ordering/) | Ordering axis on stamped AdaptiveRings: push stamps (TSC / counter / monotonic), cross-producer inversion metric, MMF-resident merge flag, strict watermark gate, single-drainer lease | Global FIFO as a runtime decision on the composed rings: flip the flag, the backlog orders retroactively | [ordering.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/ordering.rs) |
| [Reorder consumer](rings/adaptive-ordering/#exact-delivery-the-reorder-consumer) | Consumer-side EXACT delivery for the best-effort by-stamp merge: `ReorderBuffer` (bounded min-by-stamp, adaptive window that also widens with producer growth), `ReorderingReceiver`, `AdaptiveOrderedReceiver` (auto reorder-vs-`MergeStrict`) | You need exact global FIFO on a SharedCounter stamped ring without the strict merge's slowest-producer tax | [reorder.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/reorder.rs) |
| [`PeerDirectory`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/peer_directory.rs) | The AdaptiveRing's shared topology substrate: producer / consumer slot bitmaps (claim / release / recycle), published backing count, MPMC ring-ownership table (claim / handoff / crash takeover via pid liveness), topology epoch | Consumed by `AdaptiveRing` automatically; reach for it directly when composing a new multi-peer primitive that needs cross-process peer accounting | [peer_directory.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/peer_directory.rs) |
| [`LocaleAdaptiveRing`](rings/locale-adaptive-ring/) | Three-locale wrapper (Anon / File / ShmFs) around AdaptiveRing; ships with `LocaleAdaptiveRingSidecar` + `DefaultLocalePolicy` for hysteresis-gated migrations | You want runtime morphability across storage locales | [locale_adaptive_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/locale_adaptive_ring.rs) |
| [`CapacityAdaptiveRing`](rings/capacity-adaptive-ring/) | Runtime-resizable AdaptiveRing wrapper; ArcSwap state-swap + stale-list; ships with `CapacityAdaptiveRingSidecar` + `DefaultCapacityPolicy` (fill-ratio thresholds + hysteresis) | Workload's queueing depth has wide dynamic range; sidecar-driven elastic capacity | [capacity_adaptive_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/capacity_adaptive_ring.rs) |
| [`CapacityBroadcastRing`](rings/capacity-broadcast-ring/) | Capacity-morph wrapper around `SharedBroadcastRing`; same ArcSwap state-swap with `lag(idx) == 0` spin discipline; subscribers stay in lockstep across morphs | Elastic-capacity 1P/NC fan-out broadcast | [capacity_broadcast_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/capacity_broadcast_ring.rs) |
| [`CapacityPubSubRing` + `CapacityPubSubSubscriber`](rings/capacity-pubsub-ring/) | Capacity-morph wrapper around `PubSubRing`; chain-of-backings; subscribers carry `(backing_idx, position)` and advance through the chain | Elastic-capacity 1P/NC pub/sub with per-subscriber absolute positions | [capacity_pubsub_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/capacity_pubsub_ring.rs) |
| [`PubSubRing` + `PubSubSubscriber`](rings/pubsub-ring/) | One-publisher many-subscriber broadcast with per-subscriber positions | Independent subscribers walking the same producer stream at independent rates | [protocol_pubsub.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/protocol_pubsub.rs) |
| [`VirtualEndpoint` + `VirtualEndpointRegistry`](coordination-types/virtual-endpoint/) | Substrate-level identity that resolves to local or remote at runtime | Application code wants one addressing surface covering both same-host and cross-host peers | [virtual_endpoint.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/virtual_endpoint.rs) |
| [`QosPolicy` + `QosSnapshot`](coordination-types/qos-policy/) | DDS-inspired runtime-mutable QoS knobs | Sidecar-driven morphs that depend on durability / reliability / history / latency wishes | [qos_policy.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/qos_policy.rs) |
| [`RingContract`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/ring_contract.rs) | Declared ring contract: producer/consumer count ceilings, an ordering contract, and a capacity ceiling as one validated artifact; UNBOUNDED unless declared - the declared contract is the only source of registration errors on an `AdaptiveRing` | Pin a peer ceiling (the user override on the otherwise grow-on-demand ring), or pin an ordering contract the auto-morph cannot violate (a `Fifo` contract forbids the partitioned per-producer-lane shapes) | [ring_contract.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/ring_contract.rs) |
| [`SubscriberPosition`](coordination-types/subscriber-position/) | MMF-resident position counter for resumable subscribers | Subscriber must survive a process restart + resume from its last position | [replay_positions.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/replay_positions.rs) |
| [`ShmFile`](specialized/shm-file/) | Cross-platform named shared-memory backing | Building a custom cross-process primitive on top of named shm | [shm_file.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/shm_file.rs) |

## Cross-host bridges (Cargo features)

Substrate primitives that ferry bytes between two AdaptiveRing
instances on different hosts. Gated behind Cargo features.

| Type | Cargo feature | Transport | Source |
|---|---|---|---|
| [`QuicBridgeClient` / `QuicBridgeServer`](bridges/quic-bridge/) | `quic-bridge` | QUIC over UDP (TLS via rustls) | [quic_bridge.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/quic_bridge.rs) |
| [`TcpBridgeClient` / `TcpBridgeServer`](bridges/tcp-bridge/) | `tcp-bridge` | Plain TCP | [tcp_bridge.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/tcp_bridge.rs) |

## OS-specific substrate primitives

Primitives whose implementation is platform-gated but whose surface is
shared across the targets each supports. Compiled away where unsupported;
the workspace stays buildable everywhere.

| Type | Cargo gate | What it is | Source |
|---|---|---|---|
| [`DirectFileRing`](linux/direct-file-ring/) | `cfg(any(unix, windows))` | Non-mmap pread/pwrite ring with page-cache bypass: `O_DIRECT` (Linux/FreeBSD), `F_NOCACHE` (macOS), `FILE_FLAG_NO_BUFFERING` (Windows) | [protocol_direct_file.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/protocol_direct_file.rs) |
| [`fd_handoff::send_fd` / `recv_fd`](linux/fd-handoff/) | `cfg(any(unix, windows))` | SCM_RIGHTS fd passing over a Unix socket (unix, incl. macOS); `DuplicateHandle` (Windows) | [fd_handoff.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/fd_handoff.rs) |
| [`HugepageRegion`](linux/hugepages/) | `cfg(target_os = "linux")` | MAP_HUGETLB anon mmap (2 MB or 1 GB pages) | [hugepages.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/hugepages.rs) |
| [`VsockSocket`](linux/locale-vsock/) | `cfg(any(target_os = "linux", windows))` | AF_VSOCK SOCK_STREAM for host-VM byte streaming | [locale_vsock.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/locale_vsock.rs) |
| [`WireSocket`](linux/locale-wire/) | `wire-locale` feature (Linux / Windows / FreeBSD / macOS) | Raw-L2 NIC-bypass socket: AF_XDP (Linux), XDP (Windows), netmap (FreeBSD), BPF (macOS) | [locale_wire.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/locale_wire.rs) |

Two further OS-specific primitives, referenced here by source: `SuperPageRegion`
([super_pages.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/super_pages.rs),
`cfg(any(target_os = "freebsd", target_os = "macos"))`) - the superpage anon
mmap (FreeBSD `MAP_ALIGNED_SUPER`, macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`)
that backs `AdaptiveRing::create_hugepage` on those OSes; and `KernelAsyncRing`
([kernel_async_ring.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/kernel_async_ring.rs),
`cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))`)
- the kernel async-I/O ring (io_uring on Linux, IoRing on Windows, POSIX `aio`
on FreeBSD / macOS).

## Windows-only substrate primitives

OS-specific primitives gated on `cfg(windows)`.

| Type | Cargo gate | What it is | Source |
|---|---|---|---|
| [`LargePageRegion`](windows/large-pages/) | `cfg(windows)` | `VirtualAlloc(MEM_LARGE_PAGES)` private memory (2 MB pages); Windows parity for `HugepageRegion` | [large_pages.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/large_pages.rs) |
| [`LargePageSection`](windows/large-pages/) | `cfg(windows)` | `SEC_LARGE_PAGES` named pagefile-backed section: cross-process large-page sharing by section name (huge memory tables shared between processes) | [large_pages.rs](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/src/large_pages.rs) |

## See also

- [Alphabetical index](index-all/) - every name A-Z without category grouping.
- [Pick the right primitive](../../how-to/role-pair-selection/) - role-pair-driven selection.
- [`subetha-pointers` reference](../subetha-pointers/_index.md) - the exotic-pointer sibling family.
- [Architecture](../../explanation/architecture/) - where this family sits in the four-crate stack.
- [The MMF substrate](../../explanation/mmf-substrate/) - why one byte layout serves three deployment modes.
