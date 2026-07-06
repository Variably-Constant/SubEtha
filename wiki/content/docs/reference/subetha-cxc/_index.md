---
title: SubEtha CXC
weight: 50
---

# Cross-process MMF primitives (`subetha-cxc`)

`subetha-cxc` is the topology-axis crate. Every primitive here is
backed by a memory-mapped file: the same byte layout serves
cross-thread (two threads in one process map the same file),
cross-process (two processes open the same file and the kernel
page-aliases them), and disk-persistent (the file survives a
process restart). The crate is one of two primitive families on
the shared substrate; the other is
[`subetha-pointers`](../subetha-pointers/) for the workload axis.

Roughly forty primitives, grouped into the categories below. Each
primitive ships with its own prose doc in
`crates/subetha-cxc/docs/pointers/*.md`; the wiki pages below
summarise each group and link to the canonical source-tree docs.

## How the family is organised

| Category | Page | Primitives |
|---|---|---|
| Front door (sync / blocking / async) | [high-level-api.md](high-level-api.md) | `AutoIpc`, `Channel`, `AdaptiveIpc`, `WorkStealQueue`, `KvMap` |
| Async engine | [async-engine.md](async-engine.md) | `block_on`, `reactor`, `RingExecutor`, `TaskPool`, `WakerRing`, `net_bridge` |
| Rings and stacks | [shared-ring.md](shared-ring.md) | `SharedRing`, `SharedBroadcastRing`, `SharedTreiberStack` |
| Hash maps | [shared-hash-map.md](shared-hash-map.md) | `SharedHashMap`, `SharedBTreeMap` |
| Atomics | [shared-atomic.md](shared-atomic.md) | `SharedAtomicU32`, `SharedAtomicU64`, `SharedAtomicBool` |
| Cells | [shared-cell.md](shared-cell.md) | `SharedCell`, `SharedOnceCell` |
| Caches | [shared-lru-cache.md](shared-lru-cache.md) | `SharedLRUCache` |
| Locks | [shared-locks.md](shared-locks.md) | `SharedRWLock`, `SharedSemaphore`, `SharedRateLimiter`, `SharedFenceClock` |
| Sketches and arenas | [shared-sketches.md](shared-sketches.md) | `SharedBitVec`, `SharedBloomFilter`, `SharedBlockedBloomFilter`, `SharedCountMinSketch`, `SharedHyperLogLog`, `SharedHistogram`, `SharedReservoirSampler`, `SharedStringArena`, `SharedHandleTable` |
| Ownership | [ownership.md](ownership.md) | `OwnerLease`, `SharedLeaderElection`, `LazyConfig` |
| Coordination | [coordination.md](coordination.md) | `HeartbeatTable`, `EpochBarrier`, `FailoverWatchdog`, and the rest of the coordination layer |
| Alphabetical index | [index-all.md](index-all.md) | every primitive linked to its source-tree doc |
| Cross-platform benchmarks | [cross-platform-benchmarks.md](cross-platform-benchmarks.md) | per-primitive medians across Zen+/Zen2/Zen3 and Windows/Linux/FreeBSD |
| Polymorphic substrate (locale axis) | [rings/locale-adaptive-ring.md](rings/locale-adaptive-ring.md), [specialized/shm-file.md](specialized/shm-file.md) | `LocaleAdaptiveRing`, `ShmFile` |
| Polymorphic substrate (protocol axis) | [rings/pubsub-ring.md](rings/pubsub-ring.md) | `PubSubRing`, `PubSubSubscriber` |
| Polymorphic substrate (identity + policy) | [coordination-types/virtual-endpoint.md](coordination-types/virtual-endpoint.md), [coordination-types/qos-policy.md](coordination-types/qos-policy.md), [coordination-types/subscriber-position.md](coordination-types/subscriber-position.md) | `VirtualEndpoint`, `QosPolicy`, `SubscriberPosition` |
| Cross-host bridges (Cargo features) | [bridges/_index.md](bridges/_index.md) | `QuicBridgeClient` / `QuicBridgeServer`, `TcpBridgeClient` / `TcpBridgeServer` |
| Linux-only | [linux/_index.md](linux/_index.md) | `DirectFileRing`, `fd_handoff`, `HugepageRegion`, `VsockSocket`, `WireSocket` |

## What every primitive shares

Three invariants hold across the family.

**MMF-backed storage.** Every primitive's state lives in a
memory-mapped file. The `create(path, capacity)` and
`open(path, capacity)` constructors are universal. Capacity is
required to be a power of two for nearly all primitives so the
slot-index calculation reduces to a single mask
(`index & (capacity - 1)`) instead of a modulo. `SharedHashMap` is
the exception: it probes with `hash % capacity` and accepts any
`capacity >= 2`.

**No absolute pointers.** Pointers between slots inside the
mapped region are file-relative offsets via `OffsetPtr` or
`TaggedOffsetPtr`, never raw pointers. This is what makes the
two-process case work: the kernel maps the file at whatever
virtual base it chooses, and offset arithmetic remains valid in
both mappings.

**Deterministic hashing.** Primitives that hash keys (the maps,
the sketches) use FNV-1a via `fnv1a_64`, not `std::hash::RandomState`.
The `std` hasher's per-process random seed makes keys
irreproducible across processes; FNV-1a's fixed seed gives the
same hash for the same key in every process linking the crate.

See [the MMF substrate explanation](../../explanation/mmf-substrate.md)
for why these three invariants together are sufficient.

## Sidecar registration

Every Shared* primitive in this crate implements
`subetha_sidecar::AdaptiveInstance` and carries a
`HandshakeHeader` plus `ObservationRing`. The default `Policy`
is `NoMigrationPolicy` because the strategy here is the byte
layout, which is not migrable in place.

To get observation telemetry (without migration), wrap in
`SidecarBox`:

```rust,no_run
use subetha_cxc::SharedHashMap;
use subetha_sidecar::SidecarBox;

let m = SidecarBox::new(
    SharedHashMap::<u32, u64>::create("/tmp/sessions.bin", 1024).unwrap()
);
m.insert(42, 4242).unwrap();

let stats = m.stats().unwrap();
println!("ops_observed = {}", stats.ops_observed);
```

The bare `create` / `open` constructors return the unregistered
type; the `SidecarBox::new` wrap is the registration step. This
is opposite to the `subetha-pointers` adaptive primitives,
which return `SidecarBox<Self>` directly.

## Op_kind constants

Each primitive defines its op_kind constants in
[`subetha_cxc::sidecar_ops`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/sidecar_ops.rs).
Twenty-plus modules, one per primitive family. The constants
follow a consistent naming pattern: `OP_INSERT`, `OP_GET`,
`OP_REMOVE` for maps; `OP_PUSH`, `OP_POP` for rings; `OP_LOAD`,
`OP_STORE` for cells; and so on.

Custom policies referencing these constants import the
appropriate module:

```rust,no_run
use subetha_cxc::sidecar_ops::hash_map::{OP_INSERT, OP_GET, OP_REMOVE};
```

## See also

- [Architecture](../../explanation/architecture.md) - where this
  family sits in the four-crate stack.
- [The MMF substrate](../../explanation/mmf-substrate.md) - why
  one byte layout serves three deployment modes.
- [Cross-process round-trip tutorial](../../tutorial/cross-process-roundtrip.md) -
  end-to-end demo.
- [`subetha-pointers` reference](../subetha-pointers/) -
  the workload-axis sibling crate.
