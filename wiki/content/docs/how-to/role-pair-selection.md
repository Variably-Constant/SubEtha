---
weight: 10
---

# Role-pair selection

The fastest way to pick a SubEtha primitive: find the row whose
role pair matches your shape, take the type, move on. The
shape - who-talks-to-who - is what determines the primitive.
Strategy adaptation is a secondary axis the sidecar handles via
the `AdaptiveIpc<T>` umbrella, which auto-picks among the
specialised primitives below based on declarative workload hints.

## Cross-process MMF primitives

Use these when the two ends are in different address spaces, or
when one end is "the same process tomorrow after a restart". The
data lives in a memory-mapped file; the kernel page-aliases the
mapping between participants and there is no kernel on the data
path after construction.

| Role pair | Primitive |
|---|---|
| producer + consumer (lock-free MPMC) | `SharedRing` |
| single-producer + N consumers (fan-out) | `SharedBroadcastRing` |
| LIFO stack | `SharedTreiberStack` |
| shared mutable cell | `SharedCell` |
| one-shot init | `SharedOnceCell` |
| atomic word | `SharedAtomicU32`, `SharedAtomicU64`, `SharedAtomicBool` |
| key/value lookup | `SharedHashMap` |
| LRU cache (bounded, eviction) | `SharedLRUCache` |
| mutual exclusion (reader/writer) | `SharedRWLock` |
| counting semaphore | `SharedSemaphore` |
| rate limit (token bucket) | `SharedRateLimiter` |
| logical clock (Lamport / hybrid) | `SharedFenceClock` |

Several primitives sit on the same role-pair shape but tune for a
specific data layout:

| Type | Same role-pair as | Specialisation |
|---|---|---|
| `SharedBTreeMap` | `SharedHashMap` | ordered keys, range queries |
| `SharedLinkedList` | `SharedTreiberStack` | doubly linked, both-end ops |
| `SharedVec` | `SharedRing` | indexed array, random access |
| `SharedRegion` | (allocator role pair) | sub-allocator inside the MMF |

Sketches and probabilistic structures - `SharedBloomFilter`,
`SharedCountMinSketch`, `SharedHyperLogLog`, `SharedReservoirSampler`,
`SharedHistogram` - share the "insert + query" role pair of
`SharedHashMap` but trade exactness for fixed-size footprint.

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

## Coordination primitives

The shapes here are not the canonical reader/writer or
producer/consumer; they coordinate liveness, ownership, or fan-out
across the participants.

| Role pair | Primitive |
|---|---|
| liveness signal across processes | `HeartbeatTable`, `SharedLeaderElection` |
| owner of a resource + lease holders | `OwnerLease` |
| epoch barrier (all participants synchronise) | `EpochBarrier` |
| failover (work reassignment on dead peer) | `FailoverWatchdog` |
| priority fan-out | `PriorityFanout` |
| event log (emit + drain + fold) | `EventStateLog` |
| version chain (append-only history) | `SharedVersionedChain` |
| time-keyed slot tile | `SharedTimePointTile` |
| topology mapping (fan-in / fan-out routing) | `SharedTopologyMap` |
| named handle table (transient identifiers) | `SharedHandleTable` |
| dependency graph (nodes + edges) | `SharedGraph` |
| async pointer (deferred resolution) | `SharedAsyncPointer` |

## What to do once you have picked one

- For a direct cross-process primitive: open or create the MMF via
  `create(path, capacity)` or `open(path, capacity)`, then call the
  primitive's regular methods. See the
  [cross-process tutorial](../tutorial/cross-process-roundtrip.md).
- For automatic primitive selection: use `AutoIpc::new(path)` and
  declare workload hints; the builder picks the best primitive
  among the table above. See
  [`AdaptiveIpc<T>`](../reference/subetha-cxc/_index.md).
- For a custom policy: implement the `Policy` trait, build an
  instance whose `make_policy` returns your impl, and let the
  sidecar consult it on each scan. See
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
