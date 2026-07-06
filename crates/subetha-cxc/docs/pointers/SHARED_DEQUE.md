# SharedDeque

`SharedDeque<T>` - a cross-thread / cross-process **Chase-Lev
work-stealing deque** backed by a memory-mapped file.

## The asymmetry that makes it useful

Chase-Lev's signature asymmetry is the point: the **owner** of the
deque pushes and pops the bottom end with no atomic CAS on the fast
path (a `Relaxed` store on the `bottom` index), while any number of
**thieves** steal from the top end with one CAS each. There is no MPMC
ring contention - the local-pop fast path costs roughly one cache-line
write.

Lifting that protocol into a memory-mapped file lets the *same* deque
serve in-process worker-thread stealing **and** cross-process work
distribution. A second process opens the same file via
[`SharedDeque::open_as_thief`] and steals from a remote owner with the
identical CAS protocol, because the atomics touch physical pages whose
coherence is identical to the cross-thread case - the kernel is
uninvolved on the steal hot path.

## The Marshal contract

Values stored in the deque must implement `Marshal`, the type-system
contract that the value's bytes mean the same thing in every address
space. Closures with environment-capturing pointers cannot be stored
directly; they travel through
[`pass_registry`](PASS_REGISTRY.md) as `(closure_id, args)` pairs where
`args: T: Marshal`.

## API

| Method | Role | Notes |
|---|---|---|
| `SharedDeque::create` | owner | create the MMF-backed deque at a fixed capacity |
| `SharedDeque::open_as_thief` | thief | attach a second thread/process to steal from the owner |
| `push` / `pop` | owner | bottom-end push / pop, no CAS on the fast path |
| `push_batch` / `push_batch_with` | owner | bulk bottom-end push |
| `steal` | thief | top-end steal, one CAS |
| `approx_len` | any | lock-free size estimate (top/bottom may move under it) |
| `capacity` | any | the fixed slot count set at create time |
| `flush` | owner | msync the backing for durability |

Errors surface as `DequeError`. Capacity is fixed at create time and
**must be a power of two** (`create` returns `DequeError::InvalidCapacity`
otherwise), so the slot index is a single mask `b & (capacity - 1)` and
the layout matches the MMF's fixed file size; the paper's resizing variant
is a different primitive shape and is not what this implements.

## Layout

```text
+-----------------------------+
| DequeHeader (64B aligned)   |
|   magic, capacity, slot_bytes
|   owner_pid (informational) |
|   top: AtomicI64            |
|   bottom: AtomicI64         |
+-----------------------------+
| Slot[0]  (slot_bytes)       |  marshalled T payload
| Slot[1]  ...                |
+-----------------------------+
```

## When to reach for it

Use a `SharedDeque` when one producer (the owner) generates work that
many consumers drain, and you want the owner's enqueue to stay almost
free while consumers self-balance by stealing - in-process worker pools
or cross-process worker fleets that attach to the owner's file. For
symmetric many-to-many handoff where every participant both produces and
consumes, reach for a ring instead ([SHARED_RING_MPMC.md](SHARED_RING_MPMC.md)).

## Source

David Chase and Yossi Lev, *Dynamic Circular Work-Stealing Deque*,
SPAA 2005.

## Related

- [SHARED_RING.md](SHARED_RING.md) - the role-mix selection table
- [SHARED_RING_MPMC.md](SHARED_RING_MPMC.md) - symmetric many-to-many ring
