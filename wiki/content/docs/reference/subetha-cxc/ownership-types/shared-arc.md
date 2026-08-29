---
title: "Shared Arc"
weight: 5
---

# SharedArc&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/holders-1xCAS_per_slot-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Liveness](https://img.shields.io/badge/dead--holder_reap-pid_probe-informational)

A value in shared memory kept alive by the processes holding it, and
released when the last one lets go. `open` takes a holder slot, `Drop`
returns it, and `strong_count` is how many are held.

> **The `Arc` whose count survives a crash.** Ownership is a
> [Holder Table](../../coordination-types/holder-table/), one slot per
> holder stamped with its process, not a number. A holder that dies
> never releases, and its slot is reclaimed by probing whether that
> process is still there.

**Constraints (read first):**

- **`T: ShmValue`, not `T: Copy`.** `AtomicU64` is deliberately not
  `Copy`, so a `Copy` bound would admit no shared counter, no shared
  flag and no lock word. `ShmValue` is `unsafe` and asserts a stable
  cross-process layout, no pointers, no `Drop`, and soundness under
  concurrent access. Implemented for the primitives, the atomics and
  arrays of them; a caller writes `unsafe impl ShmValue for MyStruct {}`
  for a `#[repr(C)]` struct of its own.
- **The value is immutable.** Written once by the call that creates the
  backing and read-only afterwards, so a reference into the mapping is
  sound without a lock. Mutable shared state goes inside the value: an
  atomic, a [Shared Cell](../../shared-cell/), or a lock.
- **`create` attaches to a live backing rather than overwriting**, so
  racing creators reach one value and the second one's `value` argument
  is discarded.
- **Bounded holders at create**: `open` returns
  `ArcError::HoldersExhausted` when every slot is held by a live
  process. A slot left by a dead process is reaped first.
- **A different value type or holder capacity is a `LayoutMismatch`**:
  the header carries `size_of::<T>()` and the capacity.
- **`LastHolder` is the caller's**: `Unlink` removes the backing when
  the last holder releases; `Keep` leaves it for a process that attaches
  later.
- **Cross-process backed by MMF.**

---

## API

| Call | Behavior |
|---|---|
| `SharedArc::<T>::create(path, value, max_holders, on_last)` | Obtain the value, writing `value` only if the path does not yet exist, and take a holder slot. |
| `SharedArc::<T>::open(path, max_holders, on_last)` | Attach to an existing value and take a holder slot. |
| `arc.get() -> &T`, or `Deref` | The shared value. |
| `arc.strong_count() -> usize` | Processes holding it, this one included. |
| `arc.reap_dead_holders() -> usize` | Free every slot whose process is gone. |
| `arc.capacity()` / `arc.holders()` / `arc.flush()` | Holder slots; the table itself; msync. |
| `arc_file_size::<T>(capacity)` | Bytes the backing needs. |

`ArcError` is `HoldersExhausted` / `LayoutMismatch` / `IoError`.

---

## Layout

```text
| ArcHeader (64B) | HolderSlot 0..N (64B each) | value: T |
```

---

## Worked examples

### A configuration every process reads

```rust
use subetha_cxc::{LastHolder, SharedArc, ShmValue};

#[derive(Clone, Copy)]
#[repr(C)]
struct Config { workers: u32, budget: u64 }
unsafe impl ShmValue for Config {}

let cfg = SharedArc::create(
    "/tmp/cfg.bin",
    Config { workers: 12, budget: 1 << 40 },
    16,
    LastHolder::Unlink,
)?;
assert_eq!(cfg.workers, 12);   // through Deref
```

### A counter every holder bumps

The arc is immutable; the atomic inside it is not.

```rust
use std::sync::atomic::AtomicU64;
use subetha_cxc::{LastHolder, SharedArc, SharedCounter};

let a = SharedArc::create(
    "/tmp/count.bin", SharedCounter(AtomicU64::new(0)), 8, LastHolder::Keep,
)?;
let b = SharedArc::<SharedCounter>::open("/tmp/count.bin", 8, LastHolder::Keep)?;
a.add(10);
b.add(5);
assert_eq!(a.get().get(), 15);
```

---

## Use case patterns

### Pattern: configuration published once, read by many

A supervisor writes the value at create; every worker opens it and
reads through `Deref`. The count says how many workers are attached,
and a worker that crashes stops counting once something reaps it.

### Pattern: a value whose lifetime is the last reader's

`LastHolder::Unlink` removes the backing when the final holder
releases, so a temporary shared value cleans itself up rather than
needing a supervisor to remember it.

---

## Known limitations

- **Bounded holders at create**: no auto-grow.
- **The value is immutable**: put an atomic or a lock inside it.
- **No `reset`**: `create` attaches to a live backing, so a fresh value
  needs the path removed first.
- **`Unlink` races a concurrent open**: a process opening the path as
  the last holder releases either attaches before the unlink and gets a
  live mapping, or finds no file and gets `NotFound`. It never sees a
  half-torn region, but it can be refused where a moment earlier it
  would have succeeded.

---

## Common pitfalls

- **Expecting `create` to overwrite.** It attaches to a live backing,
  so the second creator's value is discarded.

- **Sizing `max_holders` to processes rather than handles.** One
  process holding two handles holds two slots.

- **Reading `strong_count` as live processes.** A crashed holder counts
  until something reaps it, which `open` does before reporting the
  table full.

- **Reaching for `T: Copy` thinking an atomic qualifies.** It does not,
  which is why the bound is `ShmValue`.

---

## References

- Source: `crates/subetha-cxc/src/shared_arc.rs` (9 unit tests covering
  a second handle sharing the value and raising the count, create
  attaching rather than overwriting, a struct value round-tripping and
  outliving the handle that wrote it, refusal on a mismatched type or
  capacity, a full holder table, a dead holder ceasing to count, the
  last holder unlinking only when asked, an atomic value every holder
  bumps, and concurrent handles never sharing a slot).
- Substrate: [Holder Table](../../coordination-types/holder-table/) -
  the slots and the liveness probe.
- Sibling primitive: [Shared Cell](../../shared-cell/) - a single
  mutable cell, for the value inside.
