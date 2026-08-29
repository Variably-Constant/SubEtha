# VersionedBTreeMap&lt;K, V&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/snapshot-epoch_pinned-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

An ordered map whose entries carry the epochs they were current
between. A delete stamps a death epoch rather than removing the entry; a
scan pins an epoch and sees what was current then; an entry goes once no
pin can reach it.

> **A snapshot scan with no retained roots.** Writers never wait for a
> scan and no path is copied. What a long scan costs is the tombstones
> created during it, bounded by write volume rather than by how long the
> scan runs.

**Composed, not folded in.** A [SHARED_BTREE_MAP.md](SHARED_BTREE_MAP.md)
of `Versioned<V>` beside a [SHARED_EPOCHS.md](SHARED_EPOCHS.md), so the
plain tree is untouched and no existing user pays for versioning.

**Constraints (read first):**

- **`capacity` is a NODE count**, as `SharedBTreeMap::create` takes, not
  an entry count. `max_pins` is how many scans may hold a pin at once.
- **`len()` counts tombstones too.** It is entries the tree holds, live
  and superseded alike.
- **Reinserting a key a live pin can still reach is refused** with
  `VersionedError::RebornUnderPin`. See below.
- **Reclamation runs on the insert path.** An insert that exhausts the
  node arena sweeps and retries before reporting `Full`, so the caller
  sizes for live entries plus what dies between sweeps.
- **Under a live pin the horizon does not move**, so a sweep during a
  scan frees only what died before that scan started. A bulk rewrite
  performed while a scan runs needs the arena to hold the old entries
  and the new ones at once.
- **Reclaimable is `died <= horizon`**, the horizon being the newest
  epoch no reader holds.

---

## Bench evidence

Against the same `SharedBTreeMap` without versioning, at 4,000 entries
in a 16,384-node arena, so the numbers say what the snapshot costs and
nothing else.

| Op | Versioned | Plain | relative |
|---|---:|---:|---|
| insert | 157.79 ns | **65.49 ns** | **2.41x slower** |
| get (current) | 48.70 ns | **38.75 ns** | 1.26x slower |
| get_at (pinned) | 55.15 ns | n/a | the plain tree has no equivalent |
| range, 1024 rows, no tombstones | 10.85 us | **7.17 us** | 1.51x slower |
| range, a quarter tombstoned | 10.95 us | **7.17 us** | 1.53x slower |

### Reading the trade-offs

1. **Insert pays most.** An epoch advance, a wider node, and a lookup
   before the write to check the reborn-key case.
2. **Reads pay the node width, not the filter.** A range over a
   quarter-tombstoned tree costs 1% more than a clean one, so the
   visibility predicate is free next to walking the larger nodes.
3. **`get_at` is what the plain tree cannot do at all.** It is not a
   slower `get`; it answers a different question.
4. **The cost is the snapshot, and it is paid on writes.** A
   write-heavy store with rare scans is the wrong shape for this; a
   store scanned while it is written is what it is for.

### Bench audit

- **Fair contender**: the same B-tree at the same node capacity, so
  the only difference is `Versioned<u64>` against `u64`.
- **No surplus work in either arm**: both created and populated
  outside the measured loop.
- **The range is measured tombstoned as well as clean**, because a
  clean tree hides the filter entirely.

### What the numbers do NOT show

- **The plain tree has no answer to a scan that must not see a
  concurrent write.** It is not a faster way of doing the same thing.

---

## The one refusal

A map holds one entry per key. When a key that is currently a tombstone
is inserted again while a live pin can still need that tombstone, there
is nowhere to put the new entry that does not destroy the old one - and
destroying it makes the pinned scan lose a row it should have seen, with
nothing reporting it. `insert` returns
`VersionedError::RebornUnderPin` rather than take that trade.

What lifts the restriction is the birth epoch joining the key ordering,
which makes versions of one key distinct entries. That changes every
range bound a caller writes, so it is a decision for the caller and not
a default.

---

## API

| Call | Behavior |
|---|---|
| `VersionedBTreeMap::<K, V>::create(tree_path, capacity, epochs_path, max_pins)` | Obtain the map and its epoch table. |
| `VersionedBTreeMap::<K, V>::open(tree_path, expected_capacity, epochs_path, expected_pins)` | Attach to both. |
| `map.pin() -> Result<PinGuard, VersionedError>` | Pin the current epoch; every read through it sees one view. |
| `map.get(&k) -> Option<V>` | The value current right now. |
| `map.get_at(&k, &pin) -> Option<V>` | The value current at the pin. |
| `map.insert(k, v) -> Result<Option<V>, VersionedError>` | Make `k` current; returns what it replaced if the key was live. |
| `map.remove(&k) -> Result<Option<V>, VersionedError>` | Stamp `k` superseded; the entry stays until no pin can reach it. |
| `map.range_at(low, high, limit, &pin) -> Vec<(K, V)>` | Entries current at the pin, in key order. |
| `map.range_at_with_cursor(..)` | The same, and the last key the walk examined, to resume past filtered tombstones. |
| `map.sweep() -> Result<usize, VersionedError>` | Drop every entry superseded below the horizon. |
| `map.epochs()` / `map.len()` / `map.capacity()` / `map.flush()` | The epoch table; entries held; nodes; msync. |

`VersionedError` is `RebornUnderPin` / `Full` / `LayoutMismatch` /
`Epochs(EpochError)` / `IoError`.

`Versioned<V>` carries `born`, `died` (`DIED_LIVE` when current) and
`value`, with `is_live()` and `visible_at(epoch)`.

---

## Worked example

```rust
use std::ops::Bound;
use subetha_cxc::VersionedBTreeMap;

let map: VersionedBTreeMap<u64, u64> =
    VersionedBTreeMap::create("/tmp/idx.bin", 1 << 20, "/tmp/idx.epochs", 64)?;
map.insert(1, 10)?;
map.insert(2, 20)?;

let pin = map.pin()?;
map.remove(&2)?;                       // stamps, does not remove

assert_eq!(map.get(&2), None);         // gone for an unpinned reader
assert_eq!(map.get_at(&2, &pin), Some(20));   // the scan still sees it
assert_eq!(
    map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &pin),
    vec![(1, 10), (2, 20)]
);
```

### Resuming a chunked scan

The limit counts entries EXAMINED, not returned, so a range dense in
tombstones returns fewer than `limit` while more remain. Resume from the
cursor, not from the last row:

```rust
let (rows, next) = map.range_at_with_cursor(low, Bound::Unbounded, 4096, &pin);
let low = match next { Some(k) => Bound::Excluded(&k), None => break };
```

---

## Common pitfalls

- **Resuming from the last returned key.** Tombstones filtered out of
  the result sit between it and the next live row; resume from the
  cursor `range_at_with_cursor` reports.

- **Reading `len()` as live entries.** It includes tombstones.

- **Sizing the arena for live entries alone.** A sweep frees nothing
  while a pin is held, so a bulk rewrite under a scan needs room for
  both generations.

- **Treating `RebornUnderPin` as a transient error.** It clears when
  the pin holding that tombstone releases; retrying under the same pin
  fails identically.

---

## References

- Source: `crates/subetha-cxc/src/versioned_btree_map.rs` (10 unit tests
  covering a scan pinned before a delete still seeing the row, a scan
  not seeing a row written after it pinned, an update reading as the new
  value now, the reborn-key refusal and its release once no pin can
  reach the tombstone, a sweep dropping only what no pin can reach, a
  sweep with nothing reclaimable, an insert that exhausts the arena
  sweeping and retrying, a range matching a `std::collections::BTreeMap`
  filtered at the pin, and a chunked resume covering the same rows as
  one call).
- Bench: `crates/subetha-cxc/benches/versioned_btree_map.rs` (insert,
  get, pinned get and range with and without tombstones, against the
  same tree unversioned).
- Substrate: [SHARED_BTREE_MAP.md](SHARED_BTREE_MAP.md) - the ordered
  map it holds, unchanged.
- Substrate: [SHARED_EPOCHS.md](SHARED_EPOCHS.md) - the counter and the
  pins.
- Sibling primitive: [SHARED_VERSIONED_CHAIN.md](SHARED_VERSIONED_CHAIN.md) -
  MVCC per chain rather than per ordered map.
