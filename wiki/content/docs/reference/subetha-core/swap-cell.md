---
weight: 40
---

# `SwapCell`

`SwapCell<T>` holds one `Arc<T>` that any thread reads, replaces, or
replaces only while the cell still holds the value it expects. A read
changes no reference count, and a replaced value is dropped when its
last reader lets go, exactly as a plain `Arc` would be.
`SwapCellOption<T>` is the same cell over an `Option<Arc<T>>`.

Every SubEtha crate keeps its shared snapshots in one: the capacity
rings' state, `AdaptiveRing`'s per-producer ring arrays, the pass
registry, the virtual endpoint table, the sidecar's per-instance
stats, `VersionedChain`'s head.

## API

| Call | Behavior |
|---|---|
| `SwapCell::new(value)` / `from_arc(arc)` | A cell holding the value. |
| `cell.load() -> Guard<'_, T>` | Read the value. The guard dereferences to `&T` and keeps the value alive while it is held; it is released on the thread that took it. |
| `cell.load_full() -> Arc<T>` | The value as an `Arc` of its own, kept past any guard. |
| `cell.store(arc)` | Put a new value in. |
| `cell.swap(arc) -> Arc<T>` | Put a new value in and return the one replaced, carrying the cell's own reference: once every reader of it has let go, the returned `Arc` is its last. |
| `cell.compare_and_set(&current, new) -> Result<(), Arc<T>>` | Put `new` in only while the cell holds `current`, the very value (pointer equality), and hand `new` back otherwise. |
| `cell.rcu(\|value\| next) -> Arc<T>` | Replace the value with a function of it, applying the function again to the newer value whenever another write lands in between. Returns the value replaced. |
| `guard.to_arc()` / `guard.ptr_eq(&arc)` | An `Arc` of the guarded value; whether it is that very value. |

`SwapCellOption` adds `empty()`, `take()`, and takes `Option`s where
`SwapCell` takes values: `load()` returns `Option<Guard>`, and
`compare_and_set(None, new)` fills an empty cell.

A cell is `Send` and `Sync` when its value type is both. A cell of any
other value type stays on the thread that has it, so the references a
write pays or drops are paid and dropped on that thread.

## How a read stays safe without a reference count

A read records the pointer it read in a slot of its own thread's, then
checks that the cell still holds that pointer. The recorded read is a
reference the reader owes. A write that replaces a value walks every
thread's slots and pays each debt on the value it took out: it adds a
reference for the reader, then frees the slot, in that order, so a
reader never finds its debt paid before the count that pays it. Only
then does the write drop or return the cell's own reference.

A guard's drop gives its slot back with one compare-and-swap. When the
swap fails, a writer paid the debt, and the guard drops the reference
it was paid. A read that finds the cell moved on before its check
takes its debt back and reads again, unless a writer already paid it,
in which case it keeps the value it read.

Each thread's slots come in blocks one cache line in size: the link to
the next block and the slots filling the rest of the line. A thread
holding more guards at once than its blocks have slots appends a
block. Blocks are never moved or freed, and a finished thread's slots
go to the next thread that needs them. A read touches only its own
thread's slots; a write walks every thread's, which is where the cost
of a write sits, since cells are written rarely and read on hot paths.

## Re-exports

```rust
pub use swap_cell::{SwapCell, SwapCellOption};
```

`Guard` is reached as `subetha_core::swap_cell::Guard`.
