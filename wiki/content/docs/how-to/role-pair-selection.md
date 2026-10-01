---
weight: 10
---

# Role-pair selection

The fastest way to pick a SubEtha primitive: find the row whose
role pair matches your shape, take the type, move on. The
shape - who-talks-to-who - is what determines the primitive.
Strategy adaptation is a secondary axis the sidecar handles via
the `AdaptiveIpc<T>` umbrella, which auto-picks among the
specialized primitives below based on declarative workload hints.

## Cross-process MMF primitives

Use these when the two ends are in different address spaces, or
when one end is "the same process tomorrow after a restart". The
data lives in a memory-mapped file; the kernel page-aliases the
mapping between participants and there is no kernel on the data
path after construction.

The Python package and the PowerShell module name each structure the
same way: `subetha.HashMap` in Python is `SubEtha.HashMap` in
PowerShell, made by `New-SubEthaHashMap`.

| Role pair | Primitive | Python / PowerShell |
|---|---|---|
| producer + consumer (lock-free MPMC) | `SharedRing` | `Ring`, the adaptive ring, which takes the SPSC, MPSC or MPMC shape its peers need |
| single-producer + N consumers (fan-out) | `SharedBroadcastRing` | `BroadcastRing` |
| LIFO stack | `SharedTreiberStack` | `Stack` |
| shared mutable cell | `SharedCell` | `Cell` |
| one-shot init | `SharedOnceCell` | `LazyValue` |
| atomic word | `SharedAtomicU32`, `SharedAtomicU64`, `SharedAtomicBool` | `Atomic`, 64 bits |
| key/value lookup | `SharedHashMap` | `HashMap` |
| LRU cache (bounded, eviction) | `SharedLRUCache` | `LruCache` |
| mutual exclusion (reader/writer) | `SharedRWLock` | `RWLock` |
| counting semaphore | `SharedSemaphore` | `Semaphore` |
| rate limit (token bucket) | `SharedRateLimiter` | `RateLimiter` |
| logical clock (Lamport / hybrid) | `SharedFenceClock` | `FenceClock` |

Several primitives sit on the same role-pair shape but tune for a
specific data layout:

| Type | Same role-pair as | Specialization | Python / PowerShell |
|---|---|---|---|
| `SharedBTreeMap` | `SharedHashMap` | ordered keys, range queries | `BTreeMap` |
| `SharedLinkedList` | `SharedTreiberStack` | doubly linked, both-end ops | `LinkedList` |
| `SharedVec` | `SharedRing` | indexed array, random access | `Vec` |
| `SharedRegion` | (allocator role pair) | sub-allocator inside the MMF | `Region` |

Sketches and probabilistic structures - `SharedBloomFilter`,
`SharedCountMinSketch`, `SharedHyperLogLog`, `SharedReservoirSampler`,
`SharedHistogram` - share the "insert + query" role pair of
`SharedHashMap` but trade exactness for fixed-size footprint. From
Python and PowerShell they are `BloomFilter`, `CountMinSketch`,
`HyperLogLog`, `Reservoir` and `Histogram`.

## Work-stealing deques (producer + consumer family)

Several deque variants exist because the workload shape inside
"producer + consumer" splits further: batched producers want one
shape, work-stealing thieves want another, broadcast fan-out wants
a third.

| Variant | Shape that fits |
|---|---|
| `SharedDeque` | Chase-Lev baseline (owner-pop, thief-steal) |
| `SharedDequeKhl` | KHL - work stealing with per-slot publication radius |
| `SharedDequeKhpd` | KHPD - publication-line batched fan-out |
| `SharedDequeLoh` | LOH - LIFO cache + LCRQ steal slow path |
| `SharedDequeUrd` | URD - per-thief mailbox; explicit consumer set |
| `SharedDequeFcl` | FCL - flat combining for high contention |

`AutoIpc::build_work_steal_queue()` with declarative hints
(`.batch_size`, `.consumers`, `.idle_wait`) picks among these without
the caller naming a variant.

From Python and PowerShell, `Deque` (an owner and its thieves) and the
front door's `WorkQueue` carry this role pair; the named variants are
Rust's.

## Coordination primitives

The shapes here are not the canonical reader/writer or
producer/consumer; they coordinate liveness, ownership, or fan-out
across the participants.

| Role pair | Primitive | Python / PowerShell |
|---|---|---|
| liveness signal across processes | `HeartbeatTable`, `SharedLeaderElection` | `Heartbeat`, `LeaderElection` |
| owner of a resource + lease holders | `OwnerLease` | `OwnerLease` |
| epoch barrier (all participants synchronize) | `EpochBarrier` | `EpochBarrier` |
| failover (work reassignment on dead peer) | `FailoverWatchdog` | not bound |
| priority fan-out | `PriorityFanout` | not bound |
| event log (emit + drain + fold) | `EventStateLog` | not bound |
| version chain (append-only history) | `SharedVersionedChain` | `VersionChain` |
| time-keyed slot tile | `SharedTimePointTile` | `TimePointTile` |
| topology mapping (fan-in / fan-out routing) | `SharedTopologyMap` | `TopologyMap` |
| named handle table (transient identifiers) | `SharedHandleTable` | `HandleTable` |
| dependency graph (nodes + edges) | `SharedGraph` | `Graph` |
| async pointer (deferred resolution) | `SharedAsyncPointer` | not bound |

## What to do once you have picked one

- For a direct cross-process primitive: open or create the MMF via
  `create(path, capacity)` or `open(path, capacity)`, then call the
  primitive's regular methods; from Python, the class's constructor
  and its `open`, and from PowerShell its `New-` and `Open-` cmdlets.
  See the
  [cross-process tutorial](../tutorial/cross-process-roundtrip.md).
- For automatic primitive selection: use `AutoIpc::new(path)` and
  declare workload hints; the builder picks the best primitive
  among the table above. See
  [`AdaptiveIpc<T>`](../reference/subetha-cxc/_index.md). From Python
  and PowerShell the front door's `AdaptiveQueue` picks a shape from
  the traffic it sees.
- For a custom policy: implement the `Policy` trait, build an
  instance whose `make_policy` returns your impl, and let the
  sidecar consult it on each scan; from Python and PowerShell, pass a
  callable or a ScriptBlock to an object's `observe`. See
  [Write a custom Policy](custom-policy.md).

## See also

- [Architecture overview](../explanation/architecture.md) - the
  four-crate decomposition.
- [Frozen handshakes](../explanation/frozen-handshake.md) - why the
  byte layout is the contract.
- [`subetha-cxc` reference](../reference/subetha-cxc/_index.md) - per-primitive
  details for the MMF family.
- [`subetha-pointers` reference](../reference/subetha-pointers/_index.md) -
  the exotic pointer types that ride CXC payloads.
