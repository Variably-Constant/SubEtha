---
weight: 20
---

# Hash maps and ordered maps

Two key-value lookup primitives, distinguished by what they
support beyond `insert` / `get` / `remove`.

## `SharedHashMap<K, V>`

The headline MMF-backed open-addressing hash map. Single byte
layout serves cross-thread, cross-process, and disk-persistent
deployments. Linear probing from `hash % capacity` (so any
`capacity >= 2` works - the map does not require a power of two).
Hash is FNV-1a (deterministic across processes); the same key
produces the same slot in every process linking the crate.

Per-slot SeqLock for lock-free reads. Get hits at roughly 16.78
ns (1.75x faster than `Mutex<HashMap>`'s 29.41 ns). Insert is
slower than the mutex equivalent because of the linear probe
plus the SeqLock-update protocol; the architectural lever is
cross-process visibility, not insert throughput.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, MapError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, MapError>;

pub fn insert(&self, key: K, value: V) -> Result<InsertOutcome, MapError>;
pub fn get(&self, key: &K) -> Option<V>;
pub fn remove(&self, key: &K) -> Option<V>;
pub fn contains_key(&self, key: &K) -> bool;
pub fn flush(&self) -> Result<(), MapError>;
pub fn flush_async(&self) -> Result<(), MapError>;
```

Constraints:

- `K + V: Copy + 'static`. Variable-length values do not fit;
  use `SharedStringArena` to intern strings and store handles.
- Payload size capped at `MAP_PAYLOAD_BYTES = 48` per slot.
- Capacity fixed at create time (no auto-grow).
- Slot states: `SLOT_EMPTY`, `SLOT_OCCUPIED`, `SLOT_TOMBSTONE`.

Op kinds: `OP_INSERT = 1`, `OP_GET = 2`, `OP_REMOVE = 3`,
`OP_CONTAINS = 4`, `OP_CLEAR = 5`, `OP_COMPACT = 6`. Flag bit 0
on `OP_INSERT` is set when `Err(MapError::Full)`; flag bit 1 on
`OP_GET` / `OP_REMOVE` is set when the lookup returns `None`.

Canonical doc with bench numbers, the worked insert/get protocol,
and the SeqLock retry logic:
[crates/subetha-cxc/docs/pointers/SHARED_HASH_MAP.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HASH_MAP.md).

## `SharedBTreeMap`

Ordered keys with range queries. A B-tree with min degree
`T` (= 8), so each node packs up to `B` (= 15) sorted keys with
fanout `2T` (= 16); a lookup touches `~log_16(N)` nodes and
binary-searches a contiguous key array per node. `NIL` is the
sentinel "no node" index. Reads are lock-free against a quiescent
tree via a global seqlock; a single writer serialises `insert` /
`remove`.

Use when range queries matter and a `BTreeMap`-shaped API is
needed across processes. For lookup-only workloads,
`SharedHashMap` is faster because its slot computation is one
hash and a modulo.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, BTreeError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, BTreeError>;
```

Op kinds use the `ordered` module: `OP_INSERT = 1`, `OP_GET = 2`,
`OP_REMOVE = 3`, `OP_ITER = 4`, `OP_POP = 5`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_BTREE_MAP.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BTREE_MAP.md).

## Picking between them

| Need | Primitive |
|---|---|
| Point lookup, fixed-size keys and values | `SharedHashMap` |
| Range queries, ordered iteration | `SharedBTreeMap` |
| Bounded map with eviction (LRU) | `SharedLRUCache` (see [shared-lru-cache.md](shared-lru-cache.md)) |

## See also

- [Role-pair selection](../../how-to/role-pair-selection.md) -
  the caller/callee shape these primitives fit.
- [MMF substrate](../../explanation/mmf-substrate.md) - why
  FNV-1a is the right hash here.
