---
title: "Shared Versioned Slab"
weight: 16
---

# SharedVersionedSlab&lt;T, D&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/snapshot-epoch_pinned-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

A slab whose slots each hold a chain of versions of their record, each
stamped with the epochs it was current between. A write pushes a
version and supersedes the previous one; a scan pins an epoch and reads
the version that was current then; a version goes once no pin can reach
it.

> **The record side of a snapshot scan.** An epoch-stamped index says
> which keys a scan sees; this says which version of each record it
> sees. A slab overwritten in place hands a pinned scan whatever the
> writer put there last, which is a row from the wrong snapshot with
> nothing reporting it.

**Composed, not folded in.** A [Shared Slab](../shared-slab/) of
`VersionChain<T, D>` beside a
[Shared Epochs](../../coordination-types/shared-epochs/), so the plain
slab is untouched and no existing user pays for versioning.

**Constraints (read first):**

- **`T: Copy + 'static`**, as the slab asks. No `Default`: a record
  with a 160-byte array is what this is for.
- **`D` is the chain depth and the caller's**, fixed at the type. A
  slot holds `D` stamped copies of `T` plus a count, rounded up to
  whole cache lines as every slab slot is; a 168-byte record at `D = 4`
  takes a 768-byte slot.
- **A full chain sweeps before it refuses.** A push into a full chain
  first drops every version no pin can reach, then pushes into the
  room that makes. Only when every version is still reachable by a
  live pin does it refuse with `VersionedSlabError::Pinned`. See below.
- **One writer per slot, any number of readers.** A push is a
  read-modify-write of the whole slot under the slab's SeqLock, so two
  writers on one slot lose an update to each other exactly as on
  `SharedSlab`. Readers never see a half-written chain.
- **Reclaimable is `died <= horizon`**, the horizon being the newest
  epoch no reader holds. A version superseded at the horizon itself is
  reachable by no pin, because a pin sees `born <= pin < died`.
- **`epochs_path` may be the table other structures share**, and
  should be when an index names these records: one horizon reclaims
  both, so an index entry never outlives its record and a record
  version is never dropped while a scan that can still reach it
  through the index runs.
- **Cross-process backed by MMF.**

---

## Bench evidence

Against the same `SharedSlab` holding the bare 168-byte record, at
65,536 slots and `D = 4`, so the numbers say what the chain costs and
nothing else.

| Op | Versioned | Plain | relative |
|---|---:|---:|---|
| set, chain has room | 180.68 ns | **25.58 ns** | **7.06x slower** |
| set, full chain sweeps first | 189.35 ns | **25.58 ns** | 7.40x slower |
| get (current) | 10.02 ns | **2.58 ns** | 3.88x slower |
| get_at (pinned, oldest of a full chain) | 90.86 ns | n/a | the plain slab has no equivalent |
| get_at (pinned, head of a full chain) | 106.49 ns | n/a | the same read at the shallowest walk |

### Reading the trade-offs

1. **A push pays the chain.** The slot is read whole, the head is
   restamped, the rest shifts down one, and the slot is written whole,
   so a push moves `D + 1` records where the plain slab writes one and
   reads nothing.
2. **Sweeping a full chain is nearly free.** 189.35 ns against 180.68
   ns with room is 5% for dropping every unreachable version, so the
   depth bound costs far less than the slot copy it rides on.
3. **Chain position is NOT what a pinned read costs.** The oldest
   version of a full chain reads no slower than the head - 90.86 ns
   against 106.49 ns, the deeper walk on the faster side. `D` sets the
   read cost through the width of the slot; how deep the version sits
   does not.
4. **A current read and a pinned read are not the same cost.** `get`
   stops at the head and lands at 10.02 ns where `get_at` costs 90.86
   ns on the same slot. Both read one slot, so the gap is in what each
   has to examine to answer; a caller that only needs the current
   version should not reach for `get_at`.
5. **`get_at` is what the plain slab cannot do at all.** It answers a
   different question, not the same one more slowly.

### Bench audit

- **Fair contender**: the same slab at the same capacity, so the only
  difference is `VersionChain<Record, 4>` against `Record`.
- **No surplus work in either arm**: both created outside the measured
  loop; the with-room arm sweeps in an unmeasured setup so the push it
  measures pays no sweep.
- **The push is measured both ways**, with room and on a full chain,
  because a chain that never fills would hide what the bound costs.
- **The pinned read is measured at both ends of the chain**, oldest
  and head, because measuring one end alone would read as a walk cost
  that the pair shows it is not.

### What the numbers do NOT show

- **The plain slab has no answer to a scan that must not see a
  concurrent overwrite.** It is not a faster way of doing the same
  thing.
- **Every arm is single-threaded**, which is the plain slab's best
  case: it is the arm with nothing to reconcile.

---

## The one refusal

A chain holds `D` versions. When it is full and a push arrives, the
versions no pin can reach are dropped first. If every version is still
reachable by a live pin, there is nothing to drop that some scan does
not still need, and dropping one anyway would make that scan read a
version from the wrong snapshot with nothing reporting it. `set_at`
returns `VersionedSlabError::Pinned` rather than take that trade, and
changes nothing.

The refusal clears when the pin holding the oldest reachable version
releases; retrying under the same pins fails identically. A deeper `D`
moves the point at which a slot under a long scan fills, at the cost of
a wider slot for every record.

---

## API

| Call | Behavior |
|---|---|
| `SharedVersionedSlab::<T, D>::create(slab_path, capacity, epochs_path, max_pins)` | Obtain the slab and its epoch table, initializing either that does not yet exist. |
| `SharedVersionedSlab::<T, D>::open(slab_path, expected_capacity, epochs_path, expected_pins)` | Attach to both. |
| `slab.pin() -> Result<PinGuard, VersionedSlabError>` | Pin the published epoch; every read through it sees one view. |
| `slab.get(i) -> Result<Option<T>, _>` | The current version at `i`, if the slot has a live one. |
| `slab.get_at(i, &pin) -> Result<Option<T>, _>` | The version at `i` the pin sees. |
| `slab.chain(i) -> Result<Vec<SlotVersion<T>>, _>` | Every version the slot holds, newest first. |
| `slab.set(i, v)` / `slab.set_at(i, v, born)` | Push `v` as current at a fresh epoch, or at a ticket's; the previous head is superseded at the same epoch. A full chain sweeps first; `Pinned` if nothing can go. |
| `slab.retire(i)` / `slab.retire_at(i, died)` | Supersede the current version without a successor; returns it, or `None` if there was none. |
| `slab.sweep_slot(i) -> Result<usize, _>` | Drop every version at `i` no pin can reach; how many went. |
| `slab.void_epoch(epoch) -> Result<usize, _>` | Undo every stamp at `epoch` across the slab: versions born there go, versions superseded there are current again. For an epoch whose ticket holder died. |
| `slab.epochs()` / `slab.capacity()` / `slab.flush()` | The epoch table; slots; msync. |

`VersionedSlabError` is `Pinned` / `Slab(SlabError)` /
`Epochs(EpochError)`.

`SlotVersion<T>` carries `born`, `died` (`DIED_LIVE` when current) and
`value`, with `is_live()` and `visible_at(epoch)`. `VersionChain<T, D>`
is the slot: `len`, `versions`, with `chain()`, `live()` and
`visible_at(epoch)`.

---

## Worked example

```rust
use subetha_cxc::SharedVersionedSlab;

#[derive(Clone, Copy)]
#[repr(C)]
struct Record { id: u64, payload: [u8; 160] }

let slab: SharedVersionedSlab<Record, 4> =
    SharedVersionedSlab::create("/tmp/cells.bin", 1 << 20, "/tmp/store.epochs", 64)?;
slab.set(4211, Record { id: 4211, payload: [7; 160] })?;

let pin = slab.pin()?;
slab.set(4211, Record { id: 4211, payload: [8; 160] })?;   // pushes, does not overwrite

assert_eq!(slab.get(4211)?.map(|r| r.payload[0]), Some(8));        // current
assert_eq!(slab.get_at(4211, &pin)?.map(|r| r.payload[0]), Some(7)); // what the scan pinned
```

---

## Use case patterns

### Pattern: records behind an epoch-stamped index

A store whose ordered index is a
[Versioned BTree Map](../../maps/versioned-btree-map/) keeps its
records here on the same epoch table. The index says which ids a scan
sees; the slab says which version of each record; one horizon reclaims
both.

### Pattern: a compound write published once

A record update touches the record and the index entries that name it.
A ticket from the shared epoch table stamps them all with one epoch
through `set_at` / `retire_at` on the slab and `insert_at` / `remove_at`
on the map; a scan pinned mid-write sees none of it and a scan pinned
after `publish` sees all of it. If the writer dies first, `void_epoch`
on each structure undoes its stamps before the ticket is freed.

```rust
let t = slab.epochs().begin()?;
slab.set_at(id, record, t.epoch())?;
index.insert_at(key, id, t.epoch())?;
t.publish();
```

---

## Known limitations

- **Bounded capacity and depth at create**: no auto-grow of either.
- **A push into a fully pinned chain is refused**, not queued.
- **`void_epoch` walks every slot**, so it costs the capacity, once
  per dead ticket.

---

## Common pitfalls

- **Treating `Pinned` as a transient error.** It clears when the pin
  holding the oldest reachable version releases; retrying under the
  same pins fails identically.

- **Two threads writing one slot.** A push reads and writes the whole
  slot; the SeqLock detects a torn read, not a torn write. Serialize
  the writer per slot.

- **A separate epoch table per structure.** An index on one table and
  the records on another reclaim under two horizons, and a record
  version can go while a scan can still reach its index entry. Share
  the table.

- **Sizing `D` for the writer alone.** The chain fills at the rate the
  slot is overwritten while the oldest pin holds, so `D` is chosen
  against the longest scan the store runs beside its hottest slot. It
  is also what a read costs: `D` sets the slot width, and every read
  pays it whether or not the chain is full.

---

## References

- Source: `crates/subetha-cxc/src/shared_versioned_slab.rs` (8 unit
  tests covering a pinned scan reading the version it pinned, a
  retired slot gone now and present at an earlier pin, a full chain
  sweeping before it refuses and refusing only while every version is
  pinned, a compound write across slots seen all or none, voiding an
  epoch undoing a dead compound write, a second handle sharing the
  chains and the pins, a reader never seeing a half-written chain
  under a concurrent writer, and the documented slot size of a
  168-byte record at `D = 4`).
- Bench: `crates/subetha-cxc/benches/shared_versioned_slab.rs` (push
  with room and on a full chain, head read and pinned oldest read,
  against the same slab unversioned).
- Substrate: [Shared Slab](../shared-slab/) - the slab it holds,
  unchanged.
- Substrate: [Shared Epochs](../../coordination-types/shared-epochs/) -
  the counter, the pins and the tickets.
- Sibling primitive:
  [Versioned BTree Map](../../maps/versioned-btree-map/) - the index
  side of the same snapshot.
