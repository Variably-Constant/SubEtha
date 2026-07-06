# SharedLRUCache&lt;K, V&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/composite-HashMap_+_LinkedList-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process LRU cache. Composite primitive demonstrating the
layered-composition thesis at full strength: combines
[`SharedHashMap<K, u32>`](./SHARED_HASH_MAP.md) for O(1) lookup
with [`SharedLinkedList<(K, V)>`](./SHARED_LINKED_LIST.md) for
O(1) move-to-front and O(1) eviction.

> **The "cross-process LRU at composite-primitive cost"
> primitive.** `get` at **23.28 ns** vs `Mutex<HashMap +
> VecDeque>` 44.31 ns (**1.90x faster** - lock-free composite
> read). `put` at **20.42 µs** vs 259 ns (~78x slower; high
> variance from SharedRegion allocation churn during composite
> write). `get_and_touch` at 104.90 ns vs 54 ns (1.95x slower -
> touch mutates the underlying linked list). Architectural
> lever: read-heavy workloads win on get + cross-process
> visibility; write-heavy workloads pay the composite cost.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`K + V: Copy + 'static`**: fixed-size payload.
- **Lock-free reads**: `get`, `contains_key`, `len`, snapshot
  ops. Multi-reader safe.
- **Single-writer writes**: `touch`, `get_and_touch`, `put`,
  `remove`, `evict_oldest` require external serialisation
  (e.g., a SharedSemaphore(1)).
- **Map sized to 8x cache capacity**: absorbs tombstone
  accumulation from eviction. After ~7x capacity insert-then-
  evict cycles, tombstones fill the map.
- **`get` vs `get_and_touch`**: get is cheap, no MRU promotion;
  get_and_touch promotes to MRU. Pattern matched to tokio /
  Moka / Caffeine.
- **3 MMF files**: `<base>.map.bin` + `<base>.list.bin`
  (+ list's region).
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [Bench evidence](#bench-evidence)
- [Worked examples](#worked-examples)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

```text
+---------------------------+   +---------------------------+
| SharedHashMap<K, u32>     |   | SharedLinkedList<(K, V)>  |
|   K -> handle.as_u32      |   |   handle -> Node<(K, V)>  |
|                           |   |   head_idx, tail_idx      |
+--------|------------------+   +---------------------------+
         |                              ^   ^
         |                              |   |
         +----> handle lookup ----------+   |
                                            |
              MRU                          LRU
              (head)                       (tail)
```

`get(k)`: hashmap lookup -> handle -> list node -> V. `put`:
hashmap insert + list push_front; evict back if at capacity.
`touch`: hashmap lookup -> remove from list -> push_front.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_lru_cache.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=10 --warm-up-time=1 --measurement-time=1`.

| Op | `SharedLRUCache` (mmf) | `Mutex<HashMap + VecDeque>` | Relative |
|---|---:|---:|---|
| get (no promote) | **23.28 ns** | 44.31 ns | **1.90x faster** |
| put (no eviction) | 20.42 µs | 259.44 ns | ~78x slower |
| get_and_touch (read + promote) | 104.90 ns | 53.72 ns | 1.95x slower |

### Reading the trade-offs

1. **get 1.90x faster.** Lock-free SharedHashMap lookup +
   SharedLinkedList value read vs mutex-guarded HashMap get.
2. **put 78x slower (~20 µs).** The composite write path
   touches SharedHashMap (insert + tombstone bookkeeping) +
   SharedLinkedList (push_front + region allocate + handle
   bookkeeping). High variance (6.8-32.7 µs) reflects
   allocation churn in the SharedRegion.
3. **get_and_touch 1.95x slower.** Touch requires removing the
   node from its current list position and re-pushing at the
   front; two list mutations on top of the hashmap lookup.
4. **The architectural lever**: cross-process visibility +
   read-heavy patterns win. Write-heavy patterns pay the
   composite cost.

### Rule 3b bench audit

- **Fair contender**: `Mutex<HashMap + VecDeque>` is the
  textbook in-process LRU shape. Both contenders measured at
  the same capacity (1000 for get; 100k for put; 100 for
  get_and_touch).
- **No `thread::spawn` inside `b.iter`**: single-threaded
  reads (multi-reader is the design point); writer-side ops
  use single-writer pattern.
- **Eviction correctness** is in the source unit tests
  (`put` past capacity drops the LRU; tombstone budget within
  expected 8x cap).
- **MMF lifecycle managed**: 3-file create + ops + drop + cleanup.

### What the numbers do NOT show

- **Cross-process LRU**: any process can open the cache and do
  lock-free gets. The mutex baseline is in-process only.
- **Disk persistence**: the cache state survives process
  restart; re-opening the files restores the LRU order.
- **Multi-reader scaling**: get is lock-free; N concurrent
  readers all hit the cache at ~23 ns without serializing.
  The mutex baseline serializes every reader.

---

## Worked examples

### Basic LRU usage

```rust
use subetha_cxc::SharedLRUCache;

let cache: SharedLRUCache<u64, u64> = SharedLRUCache::create("/tmp/lru", 1000).unwrap();
cache.put(42, 4242).unwrap();
cache.put(7, 77).unwrap();
assert_eq!(cache.get(&42), Some(4242));
let v = cache.get_and_touch(&7);   // 7 now MRU
```

### Cross-process read-heavy cache

```rust
// Writer process (single):
let cache: SharedLRUCache<u64, u64> =
    SharedLRUCache::create("/tmp/cache", 10_000).unwrap();
// ... populate ...

// Reader process (many):
let cache: SharedLRUCache<u64, u64> =
    SharedLRUCache::open("/tmp/cache", 10_000).unwrap();
if let Some(v) = cache.get(&key) { use_value(v); }
```

---

## Use case patterns

### Pattern: cross-process read-heavy cache

Single writer populates; N readers do lock-free gets.

### Pattern: durable LRU surviving restarts

Cache state lives in the MMF; a process restart re-opens the
files and resumes with the existing LRU order.

### Pattern: composite primitive demonstration

SharedLRUCache shows the architectural thesis: combining two
substrates (HashMap + LinkedList) backed by the same MMF
substrate yields a complete primitive without re-implementing
its components.

---

## Known limitations

- **Single-writer**: writes must be serialised externally.
- **Tombstone budget**: ~7x capacity insert-then-evict cycles
  before the map fills.
- **`put` is expensive**: composite write path.
- **3 MMF files**: capacity-fixed at create.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Concurrent writers.** Multiple writers calling `put` race
  on the underlying SharedHashMap + SharedLinkedList; wrap in
  a SharedSemaphore(1) or app-level mutex.

- **Treating `get` as cheap for hit-rate analysis.** It's
  lock-free but DOES NOT promote MRU; for strict LRU use
  `get_and_touch`.

- **Sizing the cache too small.** Tombstone budget exhausts
  after ~7x insertions; size for the workload churn.

- **Wrapping in a Mutex.** Reads stay lock-free; only writes
  need serialisation.

---

## References

- Source: `crates/subetha-cxc/src/shared_lru_cache.rs` (631
  lines, 18 unit tests covering put/get/touch/evict, eviction
  order, cross-handle visibility, and tombstone-budget edge
  cases).
- Bench: `crates/subetha-cxc/benches/shared_lru_cache.rs` (get,
  put, get_and_touch vs `Mutex<HashMap + VecDeque>`).
- Underlying primitive: [SHARED_HASH_MAP.md](./SHARED_HASH_MAP.md).
- Underlying primitive: [SHARED_LINKED_LIST.md](./SHARED_LINKED_LIST.md).
- Architectural reference: tokio MokaCache, Java Caffeine -
  the same get / get_and_touch split.
