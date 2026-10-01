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

For the alphabetized lookup (every name A-Z), see
[index-all](index-all/). For the role-pair-driven selection
guide, see [Pick the right primitive](../../how-to/role-pair-selection/).

## Rings, stacks, and queues

Bounded, lock-free FIFO / LIFO / pub-sub structures.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`SharedRing<P>`](shared-ring/) | Cross-thread / cross-process lock-free MPMC ring | Multiple producers and multiple consumers compete on one bounded queue | [Shared Ring](rings/shared-ring/) |
| [`SharedBroadcastRing`](shared-ring/#sharedbroadcastring) | Single-producer, multi-consumer pub/sub ring | One process broadcasts events; many subscribers each consume the full stream independently | [Shared Broadcast Ring](rings/shared-broadcast-ring/) |
| [`SharedTreiberStack<T>`](shared-ring/#sharedtreiberstack) | Cross-process lock-free LIFO stack | LIFO ordering matters and contention is moderate; one CAS per push/pop | [Shared Treiber Stack](rings/shared-treiber-stack/) |
| [`BlockingSpscRing`](rings/blocking-spsc-ring/) | SPSC ring + 2 `CrossProcessWaker` for cross-process blocking send / recv | Single producer + single consumer want to park kernel-side instead of spinning when the ring is empty / full; cross-process safe on Linux via shared `futex` | [blocking_spsc_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/blocking_spsc_ring.rs) |
| [`BlockingMpscRing`](rings/blocking-mpsc-ring/) | Composed-SPSC MPSC fan-in + per-ring producer wakers + shared consumer waker | N producers + 1 consumer want cross-process blocking semantics; per-producer FIFO; consumer parks on a shared waker any producer can fire | [blocking_mpsc_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/blocking_mpsc_ring.rs) |
| [`BlockingMpmcRing`](rings/blocking-mpmc-ring/) | Composed-SPSC MPMC grid + per-ring producer wakers + per-subset consumer wakers | N producers + M consumers want cross-process blocking semantics; each consumer owns a subset of rings and parks on its own waker | [blocking_mpmc_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/blocking_mpmc_ring.rs) |

## Maps, lists, and sequences

Keyed lookup and ordered storage.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`SharedHashMap<K, V>`](shared-hash-map/) | Cross-process open-addressed hash map | Key-value with O(1) lookup; FNV-1a hashing for cross-process determinism | [Shared Hash Map](maps/shared-hash-map/) |
| [`SharedBTreeMap<K, V>`](shared-hash-map/#sharedbtreemap) | Cross-process ordered map via B-tree | Key-value with **ordered** iteration; range queries needed | [Shared B-Tree Map](maps/shared-btree-map/) |
| [`SharedNamedValues`](maps/shared-named-values/) | Byte values of any size under case-insensitive names, over a raw hash map, a raw arena and an epoch table | Values too large for a map slot, named by strings, shared by every process of a user and outliving the session | [Shared Named Values](maps/shared-named-values/) |
| [`SharedLinkedList<T>`](maps/shared-linked-list/) | Cross-process doubly-linked list | Need stable iterator positions across mutations; not random access | [Shared Linked List](maps/shared-linked-list/) |
| `SharedVec<T>` | Cross-process bounded indexable sequence | Push/index/pop with a known capacity ceiling | [Shared Vec](specialized/shared-vec/) |
| [`SharedSlab<T>`](specialized/shared-slab/) | Cross-process slab of caller-indexed records, one SeqLock per slot | Records past 52 bytes read concurrently with the writer; the caller owns the index | [Shared Slab](specialized/shared-slab/) |
| [`SharedVersionedSlab<T, D>`](specialized/shared-versioned-slab/) | Caller-indexed records, each slot a chain of `D` epoch-stamped versions | A scan needs the record version it pinned while the writer keeps overwriting the slot | [Shared Versioned Slab](specialized/shared-versioned-slab/) |
| [`SharedArray`](specialized/shared-array/) | Flat fixed-stride array, written once and sealed | A table baked once and read by many processes, where a per-slot version word and a cache-line stride are pure cost | [Shared Array](specialized/shared-array/) |

## Atomics and cells

Scalar shared state.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`SharedAtomicU32` / `SharedAtomicU64` / `SharedAtomicBool`](shared-atomic/) | Cross-process atomic counter / flag | Single integer or bool flag shared across processes; cheaper than any map | [Shared Atomic](atomics/shared-atomic/) |
| [`SharedCell<T>`](shared-cell/) | Cross-process single-value cell | One typed value updated atomically; reads and writes from any process | [Shared Cell](cells/shared-cell/) |
| [`SharedOnceCell<T>`](shared-cell/#sharedoncecell) | Cross-process init-once cell | Initialize a value exactly once; subsequent processes read the cached result | [Shared Once Cell](cells/shared-once-cell/) |
| `SharedAsyncPointer<T>` | Cross-process lazy / speculative pointer | Speculative reads; the first process to materialize wins, others race-free observe | [Shared Async Pointer](coordination-types/shared-async-pointer/) |

## Caches

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`SharedLRUCache<K, V>`](shared-lru-cache/) | Cross-process LRU cache | Bounded keyed cache with eviction; shared by many processes | [Shared LRU Cache](caches/shared-lru-cache/) |

## Locks and synchronization

Mutual-exclusion and rate-limiting primitives.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`SharedRWLock`](shared-locks/) | Cross-process reader-writer lock with writer preference | Many readers, occasional writer; readers must not block each other | [Shared RW Lock](locks/shared-rw-lock/) |
| [`SharedSemaphore`](shared-locks/#sharedsemaphore) | Cross-process counting semaphore | Bounded resource pool (N concurrent users); acquire / release pattern | [Shared Semaphore](locks/shared-semaphore/) |
| [`SharedRateLimiter`](shared-locks/#sharedratelimiter) | Cross-process token-bucket rate limiter | Throttle requests across many processes against one shared budget | [Shared Rate Limiter](locks/shared-rate-limiter/) |
| [`SharedFenceClock`](shared-locks/#sharedfenceclock) | Hybrid Logical Clock (HLC) lifted to a process boundary | Need a monotonic timestamp that orders events across processes | [Shared Fence Clock](locks/shared-fence-clock/) |

## Probabilistic sketches

Approximate aggregations - sub-linear memory for the cardinality of values they see.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`SharedBitVec`](shared-sketches/#sharedbitvec) | Cross-process bit-packed boolean array | Dense set membership over a known small key space | [Shared Bit Vec](sketches/shared-bit-vec/) |
| [`SharedBloomFilter`](shared-sketches/#sharedbloomfilter) | Cross-process probabilistic set membership | Approximate "has key X been seen?" with controlled false-positive rate | [Shared Bloom Filter](sketches/shared-bloom-filter/) |
| [`SharedBlockedBloomFilter`](sketches/shared-blocked-bloom-filter/) | Cache-blocked probabilistic set membership | Large-scale membership where one cache line per query matters (past L3) | [Shared Blocked Bloom Filter](sketches/shared-blocked-bloom-filter/) |
| [`SharedCountMinSketch`](shared-sketches/#sharedcountminsketch) | Cross-process probabilistic frequency counter | Approximate counts per key without keeping the keys themselves | [Shared Count-Min Sketch](sketches/shared-count-min-sketch/) |
| [`SharedHyperLogLog`](shared-sketches/#sharedhyperloglog) | Cross-process probabilistic distinct-count | Count unique elements with very low memory; merges across processes | [Shared HyperLogLog](sketches/shared-hyper-log-log/) |
| [`SharedHistogram`](shared-sketches/#sharedhistogram) | Cross-process bucketed counter | Latency / value distributions binned at fixed buckets | [Shared Histogram](sketches/shared-histogram/) |
| [`SharedReservoirSampler<T>`](shared-sketches/#sharedreservoirsampler) | Cross-process uniform random sample | Sample N items from an unknown-size stream | [Shared Reservoir Sampler](sketches/shared-reservoir-sampler/) |

## Arenas and region storage

Pool allocators backed by an MMF.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| `SharedStringArena` | Append-only position-independent string arena | Many small strings pooled in one MMF; refer to them by offset | [Shared String Arena](arenas/shared-string-arena/) |
| `SharedHandleTable<T>` | Cross-process ECS-style slotmap | Generational handles to slot-allocated entities; like an ECS world shared across processes | [Shared Handle Table](arenas/shared-handle-table/) |
| `SharedRegion<T>` | Cross-process typed arena with position-independent pointers | Bulk allocation of T inside an MMF; offset pointers between regions | [Shared Region](arenas/shared-region/) |
| [`RawArena`](arenas/raw-arena/) | Power-of-two blocks published, retired under an epoch and reused, with per-class bitmaps | Values that are replaced and must be reclaimed while readers may still hold the old one; recovery from a crashed writer without a repair step | [Raw Arena](arenas/raw-arena/) |

## Ownership and election

Who-holds-the-token primitives.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`OwnerLease<T>`](ownership/) | Cross-process Mutex with auto-failover | Exclusive resource access where the holder might die; lease auto-reassigns | [Owner Lease](ownership-types/owner-lease/) |
| [`SharedLeaderElection`](ownership/#sharedleaderelection) | Cross-process leader election | Exactly one process plays the leader role; auto-elect a replacement on death | [Shared Leader Election](ownership-types/shared-leader-election/) |
| [`LazyConfig<T>`](ownership/#lazyconfig) | Thundering-herd-proof distributed config fetch | Many processes need the same config; only one actually fetches it; rest read | [Lazy Config](ownership-types/lazy-config/) |

## Liveness, failover, and barriers

Coordination across process boundaries.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`HeartbeatTable`](coordination/#heartbeattable) | Per-process heartbeat slots in an MMF | Discover which peer processes are alive; the table backs failover | [Heartbeat Table](coordination-types/heartbeat/) |
| [`FailoverWatchdog`](coordination/#failoverwatchdog) | Scans the heartbeat table and reclaims work from dead peers | Reassign owner-leases / leader-roles when a process dies | [Failover Watchdog](coordination-types/failover/) |
| [`EpochBarrier`](coordination/#epochbarrier) | Multi-process phase synchronization | All N processes must finish phase K before any starts phase K+1 | [Epoch Barrier](coordination-types/epoch-barrier/) |
| [`SharedEpochs`](coordination-types/shared-epochs/) | Cross-process epoch counter with a shared pin table | A scan needs a fixed view of a store the writers keep changing, and a reclaimer in another process must not drop what that scan can still see | [Shared Epochs](coordination-types/shared-epochs/) |
| [`HolderTable`](coordination-types/holder-table/) | Claimable slots stamped with the holding process | Something must be held across processes and a holder that dies has to be told from one that is busy; a bare refcount cannot be asked | [Holder Table](coordination-types/holder-table/) |
| [`SharedArc<T>`](ownership-types/shared-arc/) | Shared ownership of a value in shared memory | Several processes read one value and the last to let go releases it | [Shared Arc](ownership-types/shared-arc/) |
| [`VersionedBTreeMap<K, V>`](maps/versioned-btree-map/) | Ordered map with epoch-stamped entries | A scan needs a fixed view of an ordered index while writers keep changing it | [Versioned BTree Map](maps/versioned-btree-map/) |
| [`SharedVersionedSlab<T, D>`](specialized/shared-versioned-slab/) | Slab with a chain of epoch-stamped versions per slot | The records an epoch-stamped index names must stay readable at the version a scan pinned | [Shared Versioned Slab](specialized/shared-versioned-slab/) |
| [`LanedVersionedMap<K, V>`](maps/laned-versioned-map/) | One versioned index across n single-writer lanes, claimed per statement | Several statements must write one ordered index at once, where a single tree takes one writer at a time | [Laned Versioned Map](maps/laned-versioned-map/) |

## Work distribution

Higher-level coordination layered on the substrate.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`EventStateLog<E, S>`](coordination/#eventstatelog) | Event-sourced state with cross-process replay | Append-only event log + materialized state; readers reconstruct from log | [Event State Log](coordination-types/event-state-log/) |
| [`PriorityFanout`](coordination/#priorityfanout) | Tiered work queue with O(1) priority selection | N priority classes; consumers grab work from the highest non-empty class | [Priority Fanout](coordination-types/priority-fanout/) |
| [`ProgressTask<R>`](coordination/#progresstask) | Distributed work with live cross-process progress reporting | Long-running task split across processes; UI watches aggregated progress | [Progress Task](coordination-types/progress-task/) |
| [`BackgroundScheduler`](coordination/#backgroundscheduler) | Autonomous `Pass` executor backed by the MMF | Schedule periodic / triggered work; survives process restart | [Background Scheduler](coordination-types/scheduler/) |
| [`pass_registry`](coordination/#pass_registry) | Closure registry for cross-process `Pass` dispatch | Register handlers in process A; process B fires them via `execute` | [Pass Registry](coordination-types/pass-registry/) |
| [`CrossProcessWaker`](coordination-types/cross-process-waker/) | Userspace-`futex` slot list in MMF. Every wait runs the hardware monitor tier first (MONITORX/UMONITOR on x86-64, WFE on aarch64); kernel parks are shared `futex` (Linux), non-private `_umtx_op` (FreeBSD), `os_sync_wait_on_address` shared (macOS 14.4+), `WaitOnAddress` (Windows anon backings), and on Windows file / shm backings a named auto-reset event whose id the waiter publishes in its slot | Backs the `Blocking{Spsc,Mpsc,Mpmc}Ring` wrappers; usable directly by callers who need cross-process park / wake with a per-slot target sequence | [cross_process_waker.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/cross_process_waker.rs) |
| [`SharedCondvar`](coordination-types/shared-condvar/) | Cross-process Mesa-style condition variable; one generation counter + `CrossProcessWaker` | Callers want condvar semantics across processes; predicate atom is caller-owned (any MMF-resident bool / counter); cross-process wake on Linux/WSL via shared `futex` | [shared_condvar.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_condvar.rs) |
| [`BlockingSemaphore`](coordination-types/blocking-semaphore/) | Cross-process counting semaphore with kernel-park slow path | Callers want `SharedSemaphore` semantics but with zero CPU at idle and microsecond wake latency on `release` | [blocking_semaphore.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/blocking_semaphore.rs) |
| [`BlockingRWLock`](coordination-types/blocking-rw-lock/) | Cross-process reader-writer lock with kernel-park slow path | Callers want `SharedRWLock` semantics with zero CPU at idle; readers and writers both park on the same waker | [blocking_rw_lock.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/blocking_rw_lock.rs) |
| [`AsyncSpscRing`](rings/async-spsc-ring/) | `Future`-shaped adapter on `BlockingSpscRing` | Callers want `.recv().await` / `.send().await` semantics with any async executor (tokio, smol, async-std, custom); short-lived `std::thread` per in-flight future bridges kernel-park to Rust `Waker` | [async_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/async_ring.rs) |
| [`BlockingTcpBridge`](bridges/blocking-tcp-bridge/) | TCP bridge whose forwarder uses `recv_blocking` / `send_blocking` via `spawn_blocking` | Callers want the existing `TcpBridge`'s wire format but with zero CPU at idle on both sides; replaces `tokio::task::yield_now` polling with cross-process futex park | [blocking_tcp_bridge.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/blocking_tcp_bridge.rs) |

## Specialized data structures

Less common shapes for specific workloads.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| `SharedVersionedChain<T>` | Cross-process MVCC linked list | Time-travel reads at a versioned snapshot; writers append new versions | [Shared Versioned Chain](specialized/shared-versioned-chain/) |
| `SharedTimePointTile<T>` | BSPA + Versioned tile (16-slot snapshot-isolation scan) | Time-point queries over a small set of slots; SIMD lane mask scan | [Shared Time Point](specialized/shared-time-point/) |
| `SharedNaNValue` | 64-bit NaN-boxed heterogeneous value cell | Pack a small typed value (int / float / short string) into one f64 slot | [Shared NaN Value](specialized/shared-nan-value/) |
| `SharedNaNTaggedValue` | NaN-boxed value where the pointer bits identify the payload type | Polymorphic value cell with no out-of-line type tag | [Shared NaN-Tagged Value](specialized/shared-nan-tagged-value/) |
| `SharedGraph<N, E>` | Cross-process directed graph | Cross-process graph adjacency; nodes and edges in one MMF | [Shared Graph](specialized/shared-graph/) |
| `SharedUniversal<T>` | Layer-2 cross-process container that adapts strategy | Single container that auto-picks among the IPC families based on observed load | [Shared Universal](specialized/shared-universal/) |
| `SharedTopologyMap` | K_process axis observer + recommendation surface | Watch peer-process distribution; surface placement hints for cross-process work | [Shared Topology Map](specialized/shared-topology-map/) |
| `KTowerCascade<T, DEPTH>` | Recursive pow2-of-pow2 cascading container | Multi-resolution indexed access; each tower level halves resolution | [K-Tower Cascade](coordination-types/k-tower-cascade/) |
| `SharedUmbraPointer<T>` | Cross-process content-prefixed pointer | Pointer comparisons that short-circuit on content prefix before deref | [Shared Umbra Pointer](specialized/shared-umbra-pointer/) |

## IPC pointers (addressing primitives)

Low-level pointer types that other primitives compose into. Use these directly only when building a new MMF-backed type.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| `OffsetPtr<T>` | File-relative offset pointer (no tag bits) | Pointing into the same MMF from another process; offset from base | [Offset Pointer](pointers/offset-ptr/) |
| `TaggedOffsetPtr<T, TAG_BITS>` | High-bit-stealing tagged offset pointer | Same as `OffsetPtr` but you need to pack a small tag (state, type, generation) alongside the offset | [Tagged Offset Pointer](pointers/tagged-offset-ptr/) |

## Polymorphic substrate (Locale x Protocol x Shape x Capacity x Ordering)

Cross-axis primitives that compose under one pin-protocol contract.
Each entry's "Use when" is the situation that the substrate's
default-composed stack does not cover automatically.

| Type | What it is | Use when | Reference |
|---|---|---|---|
| [`AdaptiveRing`](rings/shared-ring-adaptive/) | Shape-morphing ring with all 4 shapes pre-allocated; peers register / unregister at runtime and the per-producer backings grow on demand (shared peer directory) | Default ring type; shape auto-morphs SPSC -> MPSC -> MPMC to the live peer counts, Vyukov on declaration; registration errors only under an explicit `with_contract` ceiling | [adaptive_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/adaptive_ring.rs) |
| [Adaptive ordering](rings/adaptive-ordering/) | Ordering axis on stamped AdaptiveRings: push stamps (TSC / counter / monotonic), cross-producer inversion metric, MMF-resident merge flag, strict watermark gate, single-drainer lease | Global FIFO as a runtime decision on the composed rings: flip the flag, the backlog orders retroactively | [ordering.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/ordering.rs) |
| [Reorder consumer](rings/adaptive-ordering/#exact-delivery-the-reorder-consumer) | Consumer-side exact delivery for the best-effort by-stamp merge: `ReorderBuffer` (bounded min-by-stamp, adaptive window that also widens with producer growth), `ReorderingReceiver`, `AdaptiveOrderedReceiver` (auto reorder-vs-`MergeStrict`) | You need exact global FIFO on a SharedCounter stamped ring without the strict merge's slowest-producer tax | [reorder.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/reorder.rs) |
| [`PeerDirectory`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/peer_directory.rs) | The AdaptiveRing's shared topology substrate: producer / consumer slot bitmaps (claim / release / recycle), published backing count, MPMC ring-ownership table (claim / handoff / crash takeover via pid liveness), topology epoch | Consumed by `AdaptiveRing` automatically; reach for it directly when composing a new multi-peer primitive that needs cross-process peer accounting | [peer_directory.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/peer_directory.rs) |
| [`LocaleAdaptiveRing`](rings/locale-adaptive-ring/) | Three-locale wrapper (Anon / File / ShmFs) around AdaptiveRing; ships with `LocaleAdaptiveRingSidecar` + `DefaultLocalePolicy` for hysteresis-gated migrations | You want runtime morphability across storage locales | [locale_adaptive_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/locale_adaptive_ring.rs) |
| [`CapacityAdaptiveRing`](rings/capacity-adaptive-ring/) | Runtime-resizable AdaptiveRing wrapper; SwapCell state-swap + stale-list; ships with `CapacityAdaptiveRingSidecar` + `DefaultCapacityPolicy` (fill-ratio thresholds + hysteresis) | Workload's queueing depth has wide dynamic range; sidecar-driven elastic capacity | [capacity_adaptive_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/capacity_adaptive_ring.rs) |
| [`CapacityBroadcastRing`](rings/capacity-broadcast-ring/) | Capacity-morph wrapper around `SharedBroadcastRing`; same SwapCell state-swap with `lag(idx) == 0` spin discipline; subscribers stay in lockstep across morphs | Elastic-capacity 1P/NC fan-out broadcast | [capacity_broadcast_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/capacity_broadcast_ring.rs) |
| [`CapacityPubSubRing` + `CapacityPubSubSubscriber`](rings/capacity-pubsub-ring/) | Capacity-morph wrapper around `PubSubRing`; chain-of-backings; subscribers hold their backing and position and advance through the chain | Elastic-capacity pub/sub, any number of producers, with per-subscriber absolute positions | [capacity_pubsub_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/capacity_pubsub_ring.rs) |
| [`PubSubRing` + `PubSubSubscriber`](rings/pubsub-ring/) | One-publisher many-subscriber broadcast with per-subscriber positions | Independent subscribers walking the same producer stream at independent rates | [protocol_pubsub.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/protocol_pubsub.rs) |
| [`VirtualEndpoint` + `VirtualEndpointRegistry`](coordination-types/virtual-endpoint/) | Substrate-level identity that resolves to local or remote at runtime | Application code wants one addressing surface covering both same-host and cross-host peers | [virtual_endpoint.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/virtual_endpoint.rs) |
| [`QosPolicy` + `QosSnapshot`](coordination-types/qos-policy/) | DDS-inspired runtime-mutable QoS knobs | Sidecar-driven morphs that depend on durability / reliability / history / latency wishes | [qos_policy.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/qos_policy.rs) |
| [`RingContract`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/ring_contract.rs) | Declared ring contract: producer/consumer count ceilings, an ordering contract, and a capacity ceiling as one validated artifact; unbounded unless declared - the declared contract is the only source of registration errors on an `AdaptiveRing` | Pin a peer ceiling (the user override on the otherwise grow-on-demand ring), or pin an ordering contract the auto-morph cannot violate (a `Fifo` contract forbids the partitioned per-producer-lane shapes) | [ring_contract.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/ring_contract.rs) |
| [`SubscriberPosition`](coordination-types/subscriber-position/) | MMF-resident position counter for resumable subscribers | Subscriber must survive a process restart + resume from its last position | [replay_positions.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/replay_positions.rs) |
| [`ShmFile`](specialized/shm-file/) | Cross-platform named shared-memory backing | Building a custom cross-process primitive on top of named shm | [shm_file.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shm_file.rs) |

## Cross-host bridges (Cargo features)

Substrate primitives that ferry bytes between two AdaptiveRing
instances on different hosts. Gated behind Cargo features.

| Type | Cargo feature | Transport | Reference |
|---|---|---|---|
| [`QuicBridgeClient` / `QuicBridgeServer`](bridges/quic-bridge/) | `quic-bridge` | QUIC over UDP (TLS via rustls) | [quic_bridge.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/quic_bridge.rs) |
| [`TcpBridgeClient` / `TcpBridgeServer`](bridges/tcp-bridge/) | `tcp-bridge` | Plain TCP | [tcp_bridge.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/tcp_bridge.rs) |

## OS-specific substrate primitives

Primitives whose implementation is platform-gated but whose surface is
shared across the targets each supports. Compiled away where unsupported;
the workspace stays buildable everywhere.

| Type | Cargo gate | What it is | Reference |
|---|---|---|---|
| [`DirectFileRing`](linux/direct-file-ring/) | `cfg(any(unix, windows))` | Non-mmap pread/pwrite ring with page-cache bypass: `O_DIRECT` (Linux/FreeBSD), `F_NOCACHE` (macOS), `FILE_FLAG_NO_BUFFERING` (Windows) | [protocol_direct_file.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/protocol_direct_file.rs) |
| [`fd_handoff::send_fd` / `recv_fd`](linux/fd-handoff/) | `cfg(any(unix, windows))` | SCM_RIGHTS fd passing over a Unix socket (unix, incl. macOS); `DuplicateHandle` (Windows) | [fd_handoff.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/fd_handoff.rs) |
| [`HugepageRegion`](linux/hugepages/) | `cfg(target_os = "linux")` | MAP_HUGETLB anon mmap (2 MB or 1 GB pages) | [hugepages.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/hugepages.rs) |
| [`VsockSocket`](linux/locale-vsock/) | `cfg(any(target_os = "linux", windows))` | AF_VSOCK SOCK_STREAM for host-VM byte streaming | [locale_vsock.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/locale_vsock.rs) |
| [`WireSocket`](linux/locale-wire/) | `wire-locale` feature (Linux / Windows / FreeBSD / macOS) | Raw-L2 NIC-bypass socket: AF_XDP (Linux), XDP (Windows), netmap (FreeBSD), BPF (macOS) | [locale_wire.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/locale_wire.rs) |

Two further OS-specific primitives, referenced here by source: `SuperPageRegion`
([super_pages.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/super_pages.rs),
`cfg(any(target_os = "freebsd", target_os = "macos"))`) - the superpage anon
mmap (FreeBSD `MAP_ALIGNED_SUPER`, macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`)
that backs `AdaptiveRing::create_hugepage` on those OSes; and `KernelAsyncRing`
([kernel_async_ring.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/kernel_async_ring.rs),
`cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))`)
- the kernel async-I/O ring (io_uring on Linux, IoRing on Windows, POSIX `aio`
on FreeBSD / macOS).

## Windows-only substrate primitives

OS-specific primitives gated on `cfg(windows)`.

| Type | Cargo gate | What it is | Reference |
|---|---|---|---|
| [`LargePageRegion`](windows/large-pages/) | `cfg(windows)` | `VirtualAlloc(MEM_LARGE_PAGES)` private memory (2 MB pages); Windows parity for `HugepageRegion` | [large_pages.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/large_pages.rs) |
| [`LargePageSection`](windows/large-pages/) | `cfg(windows)` | `SEC_LARGE_PAGES` named pagefile-backed section: cross-process large-page sharing by section name (huge memory tables shared between processes) | [large_pages.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/large_pages.rs) |

## See also

- [Alphabetical index](index-all/) - every name A-Z without category grouping.
- [Pick the right primitive](../../how-to/role-pair-selection/) - role-pair-driven selection.
- [`subetha-pointers` reference](../subetha-pointers/_index.md) - the exotic-pointer sibling family.
- [Architecture](../../explanation/architecture/) - where this family sits in the four-crate stack.
- [The MMF substrate](../../explanation/mmf-substrate/) - why one byte layout serves three deployment modes.
