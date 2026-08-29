# HolderTable

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-view_over_caller_memory-green)
![Protocol](https://img.shields.io/badge/claim-1xCAS_per_slot-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Liveness](https://img.shields.io/badge/dead--holder_reap-pid_probe-informational)

A fixed array of claimable slots, each carrying a caller payload and the
process holding it. A caller takes a numbered slot, holds it, and
releases it; a slot whose process is gone is reclaimed by probing
whether that process is still there.

> **The "count that can be asked whether it is still running" primitive.**
> A bare `AtomicU32` refcount is wrong across processes in one specific
> way: a holder that dies never decrements, and no accounting anywhere
> reports it. A slot stamped with a pid can be checked.

**Consumers:** the peer directory's consumer slots, the pin table behind
[SHARED_EPOCHS.md](SHARED_EPOCHS.md), and the holders of
[SHARED_ARC.md](SHARED_ARC.md).

**Constraints (read first):**

- **A view, not a mapping.** The table does not own memory. It is
  constructed over slots the caller has already mapped, so it sits
  inside whatever header layout that caller needs.
- **One slot per cache line.** `HolderSlot` is `#[repr(C, align(64))]`,
  so two holders never share a line.
- **The payload is one `u64`, and two values are reserved.**
  `HOLDER_FREE` (0) means unclaimed and `HOLDER_RESERVED` (`u64::MAX`)
  means claimed with the payload not yet decided. `publish` panics on
  either, so a held slot can never read as free.
- **Reserve, then publish, when the payload depends on shared state.**
  A caller that reads state to build its payload must make the slot
  visible first, or another party observes the slot as free, acts on
  that, and is wrong an instant later. `claim` does both steps for a
  caller whose payload does not depend on anything read between them.
- **`try_fold` reports a reservation instead of reading past it.**
  `None` means a slot was mid-claim and the fold is not answerable yet;
  the caller decides whether to retry, to reap, or to do nothing.
- **A slot claimed but not yet stamped with a pid is mid-claim, not
  dead**, and `reap_dead` leaves it alone.
- **Bounded capacity**: no auto-grow. `reserve` returns `None` when
  every slot is held.

---

## Bench evidence

Against the `AtomicU32` refcount it replaces.

| Op | `HolderTable` | `AtomicU32` refcount | relative |
|---|---:|---:|---|
| claim + release, table empty | **7.69 ns** | 15.26 ns | **1.98x faster** |
| claim + release, 3/4 held | 51.85 ns | 15.26 ns | 3.4x slower |
| live count | 68.45 ns | **1.79 ns** | **38x slower** |

### Reading the trade-offs

1. **A sparse table beats the refcount.** One CAS plus two stores to
   distinct cache lines costs less than `fetch_add` and `fetch_sub` on
   the same word, which serialize against each other.
2. **The scan is the cost, and it shows when the table is full.** A
   claim searches for a free slot, so a three-quarters-held table pays
   6.7x the empty case.
3. **Counting is a scan.** `live()` walks every slot where a refcount
   is one load. A caller polling the count in a loop has the ratio the
   wrong way round.
4. **Size the capacity to expected holders, not to the worst case.**
   Both slow numbers are functions of capacity, not of holders.

### Bench audit

- **Fair contender**: the refcount is what a caller would otherwise
  write, and `fetch_add` / `fetch_sub` is the whole of what it does.
- **No surplus work in either arm**: table and counter both allocated
  outside the measured loop.
- **Claim measured loaded as well as empty**, because an empty table
  hides the free-slot scan that is the design's cost.

### What the numbers do NOT show

- **The reap.** A refcount has no equivalent - there is nothing in it
  to probe - which is the reason the table exists and the reason a
  bench cannot express its value.

---

## API

| Call | Behavior |
|---|---|
| `unsafe HolderTable::from_ptr(base, capacity)` | Build a view over `capacity` slots at `base`. |
| `holder_table_size(capacity)` | Bytes those slots occupy. |
| `table.reserve() -> Option<usize>` | Claim a slot without deciding its payload. |
| `table.publish(slot, payload)` | Fill in a reserved slot. Panics on a reserved sentinel. |
| `table.claim(payload) -> Option<usize>` | Reserve and publish in one step. |
| `table.release(slot)` | Return a slot to the table. |
| `table.payload(slot) -> Option<u64>` | The payload, or `None` if free or forming. |
| `table.live() -> usize` | Slots held, reservations included. |
| `table.try_fold(init, f) -> Option<T>` | Fold over published payloads; `None` if a slot was mid-claim. |
| `table.reap_dead() -> usize` | Free every slot whose process is gone. |
| `table.capacity()` / `table.slot(i)` | Slots the view covers; one slot. |

---

## Layout

```text
| HolderSlot 0 (64B) | HolderSlot 1 (64B) | ... |

HolderSlot = | state: AtomicU64 | owner_pid: AtomicU32 | pad |
```

---

## Worked example

A header of the caller's own with a table behind it:

```rust
use subetha_cxc::{holder_table_size, HolderTable};

const CAPACITY: usize = 64;
let bytes = size_of::<MyHeader>() + holder_table_size(CAPACITY);
// ... map `bytes` ...
let table = unsafe {
    HolderTable::from_ptr(mmap.as_ptr().add(size_of::<MyHeader>()), CAPACITY)
};

let slot = table.reserve().expect("a free slot");
let payload = read_some_shared_state();   // read AFTER the slot is visible
table.publish(slot, payload);
// ...
table.release(slot);
```

---

## Common pitfalls

- **Reading the state you will publish before reserving.** That is the
  ordering the two-step claim exists for; a reader can observe the slot
  as free and act on a table that is about to gain a holder.

- **Treating `try_fold`'s `None` as an empty table.** It means a slot
  was mid-claim. An empty table folds to `Some(init)`.

- **Publishing `0` or `u64::MAX`.** Both are reserved states and
  `publish` panics; bias a payload that can legitimately be either.

- **Expecting a crashed holder to release itself.** Its slot stands
  until something calls `reap_dead`.

---

## References

- Source: `crates/subetha-cxc/src/holder_table.rs` (8 unit tests
  covering claim and release, slot reuse, a full table, a reservation
  that is visible but not a payload, folding published payloads, a dead
  holder and a dead reservation being reaped, a refused sentinel
  payload, and concurrent claims never handing two callers one slot).
- Bench: `crates/subetha-cxc/benches/holder_table.rs` (claim and
  release on an empty and a three-quarters-held table, and the live
  count, against an `AtomicU32` refcount).
- Consumer: [SHARED_EPOCHS.md](SHARED_EPOCHS.md) - the pin table, with
  the pinned epoch plus one as its payload.
- Consumer: [SHARED_ARC.md](SHARED_ARC.md) - the holders of a shared
  value.
