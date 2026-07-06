---
weight: 10
---

# Rings and stacks

Three lock-free MPMC primitives backed by a memory-mapped file.
Each is sized at create time; capacity is fixed and pow2.

## `SharedRing`

The headline MPMC bounded ring. Single byte layout serves
cross-thread, cross-process, and disk-persistent. Lock-free
publish via atomic CAS on the producer cursor; the consumer's
Acquire load on the slot's Vyukov sequence number is the gate
that makes the payload bytes visible - no torn reads by
construction.

The trade against `crossbeam_queue::ArrayQueue<T>`: cross-process
visibility plus disk persistence, at the cost of fixed
slot-payload size (`PAYLOAD_BYTES`) and a slightly heavier
per-op cost from the cross-process-safe protocol. For
single-process work where neither extra is needed, the in-memory
queue wins.

Constructor signature:

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, RingError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, RingError>;
```

Op kinds: `OP_PUSH = 1`, `OP_POP = 2`. The canonical source-tree
doc with bench numbers and protocol detail is
[crates/subetha-cxc/docs/pointers/SHARED_RING.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RING.md).

## `SharedBroadcastRing`

One producer, N consumers. Each consumer registers via
`register_consumer()` and gets a private cursor; the producer
writes once and every registered consumer sees the message at
its own pace. The producer's `try_push` does not block on slow
consumers - it returns `Err(BroadcastError::Full)` when the
slowest active consumer's cursor is a full ring of capacity
behind.

Constructor:

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, BroadcastError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, BroadcastError>;
```

`MAX_CONSUMERS` caps the number of consumer cursors. Op kinds:
`OP_PUSH = 1`, `OP_RECV = 2`, `OP_REGISTER = 3`,
`OP_UNREGISTER = 4`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_BROADCAST_RING.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BROADCAST_RING.md).

## `SharedTreiberStack`

Lock-free LIFO stack backed by a Treiber-style head pointer.
Cross-process variant of the classic Treiber stack with
`OffsetPtr` links between slots (so the two-process case works
with both mappings pointing into the same physical pages).

Constructor:

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, StackError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, StackError>;
```

The classic ABA problem on the head pointer is avoided by
versioning the head with a counter in the same atomic word
(`(version: u32, slot_idx: u32)` packed into a `u64`). Op kinds
follow the `ordered` module: `OP_INSERT = 1`, `OP_GET = 2`,
`OP_REMOVE = 3`, `OP_ITER = 4`, `OP_POP = 5`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_TREIBER_STACK.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TREIBER_STACK.md).

## Picking between them

| Need | Primitive |
|---|---|
| FIFO, one producer, one consumer (or few of each) | `SharedRing` |
| Pub/sub fan-out, slow consumers must not block producer | `SharedBroadcastRing` |
| LIFO stack semantics with cross-process visibility | `SharedTreiberStack` |
| Work-stealing producer + consumers | `SharedDequeKhl`, `SharedDequeKhpd` (see [role-pair selection](../../how-to/role-pair-selection.md)) |

## See also

- [Role-pair selection](../../how-to/role-pair-selection.md) -
  the producer/consumer shape these primitives fit.
- [MMF substrate](../../explanation/mmf-substrate.md) - why
  power-of-2 capacity matters for the slot-index calculation.
