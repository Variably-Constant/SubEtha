# LanedVersionedMap&lt;K, V&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/snapshot-epoch_pinned-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

One versioned index split across `n` single-writer lanes, so `n`
statements write it at once without queueing. A statement claims a
lane for its duration and writes only there; a scan pins once and
reads a merge of every lane in key order.

> **The alternative to one mutex over the whole index.**
> [SHARED_BTREE_MAP.md](SHARED_BTREE_MAP.md) is single-writer - two
> simultaneous writers mutate node structure under one seqlock and
> corrupt it - and [VERSIONED_BTREE_MAP.md](VERSIONED_BTREE_MAP.md)
> inherits that. The coordination available otherwise is a mutex every
> writing statement queues behind. A lane is a whole tree with its own
> arena and its own single writer, so the queue goes away.

**Composed, not folded in.** `n` `VersionedBTreeMap`s over ONE
[SHARED_EPOCHS.md](SHARED_EPOCHS.md), plus a
[HOLDER_TABLE.md](HOLDER_TABLE.md) of lane claims, so the plain
versioned map is untouched and no existing user pays for lanes.

**Constraints (read first):**

- **A key belongs to the lane that created it, for its whole life.**
  This is the constraint the design rests on and it is the caller's to
  keep. A lane is a separate tree, so a key written to two lanes
  exists twice and neither copy knows about the other. The shape that
  fits is the one `VersionedBTreeMap` already asks for: keys that are
  born and die but do not change, such as a composite of key and
  record id.
- **Claim by purpose.** A statement inserting keys that do not yet
  exist takes any free lane with `claim_lane`. A statement that must
  remove or rewrite existing keys takes THEIR lane with
  `claim_lane_for`, which reports `LaneBusy` rather than handing over a
  different tree.
- **A misrouted removal is refused, not silently empty.** Removing a
  key absent from the claimed lane but present in another returns
  `LanedError::KeyInAnotherLane` naming that lane. The probe costs a
  lookup per lane and runs only on the path that was already failing.
- **`nodes_per_lane` is a NODE count per lane**, not a total. `n` lanes
  at `nodes_per_lane` each hold `n` arenas of that size.
- **One epoch table across every lane**, so a pin is one view of the
  whole index and one horizon reclaims all of it. That is also the
  contended point: `advance` and `pin` are shared where the trees are
  not.
- **A scan is bounded by the slowest lane.** `range_at_with_cursor`
  returns a frontier, and rows past it are not returned even when a
  lane already walked them. See below.
- **A lane held by a process that died** comes back through
  `reap_dead_claims`, which the holder table stamps for.
- **Deadlock is the caller's to avoid.** A statement needing two lanes
  claims them in ascending index order.

---

## Bench evidence

A whole-store ordered drain, 4,000 entries, total node capacity held
constant and split across lanes, keys round-robin so every lane spans
the whole key range - the worst case for merging, and the realistic
one, since a statement claims whichever lane is free rather than one
chosen by key.

| Arm | Time | relative |
|---|---:|---|
| single tree, plain chunked resume | **34.97 us** | 1.00x |
| 1 lane, through the merge | 61.95 us | 1.77x |
| 4 lanes | 83.03 us | **2.37x** |
| 8 lanes | 103.77 us | 2.97x |
| 16 lanes | 149.45 us | 4.27x |

### Reading the trade-offs

1. **The scan pays; the writers stop queueing.** 2.37x on a whole-store
   ordered scan at four lanes is the price of four statements writing
   an index that otherwise takes one writer at a time.
2. **Most of the cost is the protocol, not the merge.** One lane
   through the merge machinery already costs 1.77x, before any second
   lane exists, because the frontier rule makes a scan re-walk what it
   could not yet emit.
3. **Past eight lanes it stops paying.** 16 lanes cost 4.27x for
   write concurrency most stores cannot use. Four to eight is the
   band this is built for.
4. **A point read probes lanes.** `get` walks lanes until the key is
   found, so it is `O(n)` in lanes where the single tree is one lookup.

### Bench audit

- **Fair contender**: the same total data and the same total node
  capacity, so the only difference is how many trees it is split into.
- **No surplus work in either arm**: every tree is created and
  populated outside the measured loop, and both arms drain with the
  same chunk limit through the same resume rule.
- **The merge is a heap over the lanes' already-ordered runs**, not a
  re-sort of their concatenation, which would charge the lane arms
  `O(n log n)` no real implementation would pay. It is also not a
  linear scan for the smallest head, which is `O(n * k)` and would
  charge the widest arm for the merge strategy rather than for having
  lanes.
- **The baseline is a single tree drained plainly**, not one lane
  driven through the merge machinery, which would flatter every lane
  arm by charging the baseline for scaffolding it does not use.

### What the numbers do NOT show

- **The write side.** These are single-threaded on purpose so the read
  regression is measured on its own. What justifies it - `n` writers
  where there was one - is not in this table.
- **Where the serialization goes instead.** Every lane shares one epoch
  table, so `advance` and `pin` become the contended point. A bench of
  `n` concurrent writers against a mutex-serialized single tree is what
  would settle that, and it has not been run.

---

## The merge frontier

A lane asked for a chunk reports the last key its walk examined. It may
hold keys past that, so the merged output can only be trusted up to the
SMALLEST cursor any lane reported: emitting beyond it would publish a
key ahead of a smaller one that some lane has not been asked for yet. A
lane whose cursor is `None` reached the end of the range and bounds
nothing.

This is why rows a lane already walked are held back, and why a whole
scan is a loop rather than a call.

---

## API

| Call | Behavior |
|---|---|
| `LanedVersionedMap::<K, V>::create(dir, lanes, nodes_per_lane, max_pins)` | Obtain the map: one tree per lane, one shared epoch table and the claims table, all under `dir`. |
| `LanedVersionedMap::<K, V>::open(dir, lanes, nodes_per_lane, max_pins)` | Attach to one another process created. |
| `map.claim_lane() -> Result<LaneGuard, LanedError>` | Claim any free lane; `NoFreeLane` when every lane is held. |
| `map.claim_lane_for(&k) -> Result<LaneGuard, LanedError>` | Claim the lane owning `k`; `KeyAbsent` when no lane holds it, `LaneBusy(i)` when its lane is taken. |
| `map.lane_of(&k) -> Option<usize>` | The lane holding `k` right now. |
| `map.pin() -> Result<PinGuard, LanedError>` | Pin the published epoch; one view across every lane. |
| `map.get(&k)` / `map.get_at(&k, &pin)` | The value current now / at the pin, from whichever lane holds it. |
| `map.range_at(low, high, limit, &pin) -> Vec<(K, V)>` | Entries current at the pin across every lane, in key order. |
| `map.range_at_with_cursor(..) -> (Vec<(K, V)>, Option<K>)` | The same, and the frontier the merge is good to. |
| `map.void_epoch(epoch)` / `map.sweep()` | Undo every stamp at an epoch / drop what no pin can reach, in every lane. |
| `map.lanes()` / `map.held_lanes()` / `map.reap_dead_claims()` | Lanes; lanes held now; lanes recovered from dead holders. |
| `map.epochs()` / `map.len()` / `map.flush()` | The shared table; entries including tombstones; msync. |
| `guard.index()` | Which lane this guard holds. |
| `guard.insert(k, v)` / `guard.insert_at(k, v, born)` | Write into the held lane, at a fresh epoch or a ticket's. |
| `guard.remove(&k)` / `guard.remove_at(&k, died)` | Stamp superseded in the held lane; `KeyInAnotherLane(i)` if it lives elsewhere. |

`LanedError` is `NoFreeLane` / `LaneBusy(usize)` / `KeyAbsent` /
`KeyInAnotherLane(usize)` / `LayoutMismatch` /
`Versioned(VersionedError)` / `Epochs(EpochError)` / `Io`.

Dropping a `LaneGuard` releases its lane.

---

## Worked example

```rust
use std::ops::Bound;
use subetha_cxc::LanedVersionedMap;

let map: LanedVersionedMap<u64, u64> =
    LanedVersionedMap::create("/tmp/idx", 4, 1 << 16, 64)?;

// A statement inserting fresh keys takes any free lane.
{
    let lane = map.claim_lane()?;
    lane.insert(1, 10)?;
    lane.insert(2, 20)?;
}   // the lane is released here

let pin = map.pin()?;
assert_eq!(
    map.range_at(Bound::Unbounded, Bound::Unbounded, 64, &pin),
    vec![(1, 10), (2, 20)]
);

// A statement removing an existing key takes THAT key's lane.
{
    let lane = map.claim_lane_for(&2)?;
    assert_eq!(lane.remove(&2)?, Some(20));
}
```

### Draining the whole store

The frontier, not the last returned row, is where the next chunk
resumes:

```rust
let mut low = Bound::Unbounded;
loop {
    let (rows, frontier) = map.range_at_with_cursor(low, Bound::Unbounded, 4096, &pin);
    handle(rows);
    match frontier {
        Some(f) => low = Bound::Excluded(&f),
        None => break,
    }
}
```

### A compound write across lanes

One ticket stamps every lane it touches, so a scan sees all of it or
none:

```rust
let t = map.epochs().begin()?;
{ let lane = map.claim_lane()?; lane.insert_at(key, id, t.epoch())?; }
{ let lane = map.claim_lane()?; lane.insert_at(pair, id, t.epoch())?; }
t.publish();
```

If the writer dies first, `void_epoch` undoes every lane's stamps
before the ticket is freed.

---

## Known limitations

- **Lane count and per-lane arena are fixed at create**: no auto-grow
  of either.
- **A key cannot move between lanes.** There is no rebalancing.
- **`get` is `O(lanes)`.** A store dominated by point reads is the
  wrong shape for this.
- **`len()` includes tombstones**, as each lane's does.

---

## Common pitfalls

- **Writing a key from whichever lane is free.** A key belongs to the
  lane that created it; written to two lanes it exists twice. Use
  `claim_lane_for` when the key already exists.

- **Resuming a scan from the last returned row.** Rows past the
  frontier are held back deliberately. Resume from the frontier.

- **Reading `LaneBusy` as failure.** It means another statement holds
  the lane owning that key. Retry; do not write elsewhere, because
  elsewhere is a different tree.

- **Claiming two lanes in inconsistent order.** Two statements each
  holding one lane and waiting for the other's deadlock. Claim in
  ascending index order.

- **Sizing lanes by core count.** Past eight the scan cost climbs
  faster than the write concurrency is usable; four to eight is the
  band the numbers support.

---

## References

- Source: `crates/subetha-cxc/src/laned_versioned_map.rs` (9 unit tests
  covering a merged scan returning every lane in key order, a chunked
  merged scan covering the same rows as one call, a claimed lane
  refused to a second statement and freed on drop, a removal from the
  wrong lane naming the right one, `claim_lane_for` telling absent from
  busy, a pin not seeing a later write in any lane, one ticket across
  lanes seen all or none, voiding a dead ticket's epoch across every
  lane, and a second handle sharing the lanes and the claims).
- Bench: `crates/subetha-cxc/benches/laned_versioned_map.rs` (whole-store
  ordered drain at 1, 4, 8 and 16 lanes against a single tree drained
  plainly).
- Substrate: [VERSIONED_BTREE_MAP.md](VERSIONED_BTREE_MAP.md) - the map
  a lane is, unchanged.
- Substrate: [SHARED_EPOCHS.md](SHARED_EPOCHS.md) - the counter, the
  pins and the tickets every lane shares.
- Substrate: [HOLDER_TABLE.md](HOLDER_TABLE.md) - the lane claims, and
  the dead-holder recovery behind `reap_dead_claims`.
- Sibling primitive: [SHARED_VERSIONED_SLAB.md](SHARED_VERSIONED_SLAB.md) -
  the record side of the same snapshot.
