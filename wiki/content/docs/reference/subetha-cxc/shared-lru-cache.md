---
title: Shared LRU Cache
weight: 50
---

# Caches

## `SharedLRUCache`

A bounded key-value cache with LRU eviction, backed by an MMF.
Capacity fixed at create time; eviction kicks in when an insert
hits a full cache. Each slot holds a recency counter; the
eviction policy walks the slots to find the smallest counter.

The architectural shape is the same as `lru::LruCache` from the
in-memory ecosystem, with two adaptations for the MMF substrate:

1. The "doubly linked list of recency order" pattern is replaced
   by per-slot counters. A linked-list update on every access
   requires pointer mutation on the hot path; the counter
   approach is one atomic increment per access plus periodic
   eviction sweeps that amortise the linear scan.
2. All storage is inline (no allocator). Each slot is a single
   `u64` recency counter plus a fixed-size payload area.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, LRUError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, LRUError>;

pub fn get(&self, key: &K) -> Option<V>;
pub fn put(&self, key: K, value: V) -> Result<Option<V>, LRUError>;
pub fn remove(&self, key: &K) -> Option<V>;
```

Op kinds use the `lru_cache` module: `OP_GET = 1`, `OP_PUT = 2`,
`OP_TOUCH = 3`, `OP_REMOVE = 4`, `OP_EVICT = 5`. The `OP_EVICT`
op is what a custom policy reads to detect a cache that is
running hot - high eviction rate means the cache is too small
for the workload.

The shipped sidecar policy is `NoMigrationPolicy` because the
byte layout is the strategy and the byte layout does not
migrate. Custom policies can read the eviction-vs-hit ratio to
trigger application-level decisions (resize the cache by
creating a new file, log a warning, etc.).

## Picking against the in-memory alternatives

| Need | Primitive |
|---|---|
| Cross-process LRU cache | `SharedLRUCache` |
| In-memory LRU cache | `lru::LruCache` (external crate) |

## See also

- [Role-pair selection](../../how-to/role-pair-selection.md) -
  the bounded-cache / eviction shape.
