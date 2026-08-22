---
title: "Shared B-Tree Map"
weight: 20
---

# SharedBTreeMap&lt;K, V&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/SWMR-single--writer-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process ordered key/value map, stored as a B-tree in a single
self-contained MMF. Each node packs up to `B = 15` sorted keys (min
degree `T = 8`, fanout `2T = 16`), so a lookup touches only
`~log_16(N)` nodes and each node's binary search reads a contiguous,
prefetcher-friendly key array rather than chasing scattered
single-cache-line nodes. Reads are lock-free against a quiescent
tree via a global seqlock; a single writer serialises `insert` /
`remove`.

> **The "cross-process ordered map" primitive.** At 100 keys get_hit
> is 25.15 ns vs `Mutex<BTreeMap>` 26.01 ns (tied) and get_miss is
> 22.60 ns vs 30.69 ns (**1.36x faster**); iter_ascending (100) is
> 254.48 ns vs 287.52 ns (1.13x faster). At 100k keys get_hit is
> 173.40 ns vs 108.67 ns (1.60x slower: the in-process map wins once
> the working set is RAM-resident and the seqlock + position-
> independent addressing cost shows). The architectural lever is
> **cross-process visibility + lock-free reads** that an in-process
> `BTreeMap` cannot offer; the contiguous-key node layout keeps the
> mmf competitive on raw lookup where a per-level pointer-chasing
> structure would not.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`K: Copy + Ord + Default`, `V: Copy + Default`**.
- **SINGLE-WRITER, MULTI-READER**: `insert` / `remove` require
  external serialisation. `get`, `contains_key`, `len`, `first`,
  `iter_ascending` are lock-free against a quiescent (build-then-
  query) tree.
- **Seqlock reads**: a writer makes the global version odd for the
  duration of a structural mutation and even after; a reader retries
  the whole search if the version changes or is odd, so concurrent
  reads never observe a torn tree.
- **Proactive top-down split (CLRS)**: full children are split
  before descent, so `insert` is single-pass and never overflows a
  node.
- **Free-list slot reuse**: merged / removed nodes are recycled, so
  deletes reclaim capacity.
- **Bounded node capacity at create**: no auto-grow.
- **Cross-process backed by MMF.**

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_btree_map.rs`.
Captured on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=12 --warm-up-time=1 --measurement-time=2`.

Workload: `K=u32`, `V=u32`.

| Op | `SharedBTreeMap<u32, u32>` (mmf) | `Mutex<BTreeMap<u32, u32>>` | Relative |
|---|---:|---:|---|
| get_hit (100 keys) | 25.15 ns | 26.01 ns | tied (1.03x faster) |
| get_miss (100 keys) | 22.60 ns | 30.69 ns | **1.36x faster** |
| iter_ascending (100 keys) | 254.48 ns | 287.52 ns | 1.13x faster |
| get_hit (100k keys, spread) | 173.40 ns | 108.67 ns | 1.60x slower |

### Reading the trade-offs

1. **Small-N reads are level or better.** At 100 keys the tree is one
   or two nodes; the lock-free seqlock read is a version load plus a
   binary search in a contiguous key array, with no lock/unlock, while
   `Mutex<BTreeMap>` pays the mutex round-trip on every read. That
   comes out as a tie on hits (1.03x) and a clear 1.36x on misses.
   Do not read the hit column as a win - the honest claim at this size
   is that a cross-process map costs nothing against an in-process one,
   which is the surprising part.
2. **Large-N reads cross over.** At 100k keys the working set is
   RAM-resident on both sides. `BTreeMap`'s in-process nodes are
   pointer-direct and pay no seqlock retry; the mmf walks
   `~log_16(100k) ~= 5` position-independent nodes and re-validates
   the seqlock version, landing 1.60x slower. The mmf trades that
   raw-speed gap for cross-process operability.
3. **Iteration is level.** A full in-order walk is a sequential node
   traversal on both sides; the contiguous key arrays keep the mmf
   level with the in-process iterator. Note that `iter_ascending`
   materialises a `Vec<(K, V)>` rather than returning a lazy iterator,
   so the cost includes building it.
4. **The architectural lever is what `BTreeMap` cannot do**:
   cross-process visibility, lock-free multi-reader access, and a
   durable ordered map that survives process restart.

### Rule 3b bench audit

- **Fair contender**: `Mutex<BTreeMap>` is the textbook in-process
  ordered map; the same operations are measured on both sides.
- **No `thread::spawn` inside `b.iter`**: single-threaded, matching
  the single-writer design.
- **Sizing**: both a small (100-key, warm) case and a large (100k-
  key, RAM-resident, pseudo-random spread) case are measured, so the
  small-N read win and the large-N raw-speed cost are both visible
  rather than one being hidden.
- **MMF lifecycle managed**: per-bench create + ops + drop +
  remove_file.

### What the numbers do NOT show

- **Cross-process ordered map**: any process opens the tree and
  reads it ordered; an in-process `BTreeMap` cannot be shared.
- **Lock-free multi-reader scaling**: N concurrent readers each walk
  independently with no lock; the mutex baseline serialises every
  reader.
- **Disk persistence**: the tree survives process restart.

---

## Worked examples

### Basic ordered map

```rust
use subetha_cxc::SharedBTreeMap;

let bt: SharedBTreeMap<u32, u32> = SharedBTreeMap::create("/tmp/bt.bin", 1024).unwrap();
bt.insert(10, 100).unwrap();
bt.insert(5, 50).unwrap();
bt.insert(15, 150).unwrap();
assert_eq!(bt.first(), Some((5, 50)));   // smallest first
let asc: Vec<_> = bt.iter_ascending();
assert_eq!(asc, vec![(5, 50), (10, 100), (15, 150)]);
```

### Cross-process ordered config

```rust
// Writer process (single):
let bt: SharedBTreeMap<u64, u64> = SharedBTreeMap::create("/tmp/cfg", 1024).unwrap();
bt.insert(1, 100).unwrap();
bt.insert(2, 200).unwrap();
// Reader processes (many):
let bt: SharedBTreeMap<u64, u64> = SharedBTreeMap::open("/tmp/cfg", 1024).unwrap();
let first_n: Vec<_> = bt.iter_ascending().into_iter().take(10).collect();
```

---

## Use case patterns

### Pattern: cross-process ordered config / index

A single writer publishes a sorted key->value index; many readers
walk it without locks. The ordering supports range and first/min
queries that a hash map cannot.

### Pattern: deduplicated ordered event log

Inserts are keyed by sequence number; `iter_ascending` walks events
in submission order, and duplicate keys replace in place.

### Pattern: persistent priority queue

Insert with key = priority; `first` reads the minimum and
`iter_ascending` drains in priority order. The MMF persistence
survives restart.

---

## Known limitations

- **Single-writer**: concurrent `insert` / `remove` need external
  serialisation; reads stay lock-free.
- **Large-N raw lookup** trails an in-process `BTreeMap`: the mmf
  pays seqlock re-validation and position-independent addressing.
  Choose this primitive for cross-process + lock-free-read shapes,
  not single-process raw speed at scale.
- **Bounded capacity at create**: the node array is fixed; size it
  for the maximum live entry count.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Concurrent writers without coordination.** `insert` / `remove`
  mutate node structure under the seqlock; two simultaneous writers
  corrupt the tree. Wrap writes in a `SharedSemaphore(1)` or a
  per-process mutex.

- **Sizing capacity for max live entries.** The node array caps at
  create; a `Full` error means the capacity was too small for the
  live set.

- **Expecting in-process `BTreeMap` speed at scale.** At large,
  RAM-resident sizes the in-process map wins on raw lookup; the mmf
  buys cross-process + lock-free reads, not single-process speed.

- **Wrapping in a Mutex.** Fine for single-writer protection; reads
  remain lock-free.

---

## References

- Source: `crates/subetha-cxc/src/shared_btree_map.rs` (CLRS B-tree,
  min degree `T = 8`, with unit tests covering insert+get round-trip,
  ascending iteration order, first, remove with borrow/merge,
  cross-handle visibility, duplicate-key replacement, and an
  80k-operation oracle against `std::collections::BTreeMap`).
- Bench: `crates/subetha-cxc/benches/shared_btree_map.rs` (get_hit,
  get_miss, iter_ascending, get_hit_100k vs `Mutex<BTreeMap>`).
- Underlying storage: the tree is a self-contained MMF
  (`BTreeHeader` + node array with bump + free-list allocation).
- Sibling primitive: [SharedHashMap](shared-hash-map/) -
  unordered O(1) lookup; the B-tree adds ordering and range/first
  queries.
