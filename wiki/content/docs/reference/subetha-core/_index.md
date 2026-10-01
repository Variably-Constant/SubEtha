---
weight: 20
---

# The CXC substrate (`subetha-core`)

`subetha-core` is the substrate every CXC primitive sits on. It
ships five modules every cross-process and adaptive instance needs,
plus a CPUID helper and an axis-signature catalog the deque
dispatcher consults to pick a variant.

| Module | Public type | Role |
|---|---|---|
| `handshake` | `HandshakeHeader` | per-instance generation counter + in-flight tracker |
| `observation` | `Observation`, `ObservationRing`, `thread_id`, `any_observer_armed` | per-instance multi-producer ring of op observations |
| `migration` | `Generation<'a>`, `MigrationGuard<'a>` | RAII guards for the dual-stack migration protocol |
| `marshal` | `Marshal`, `MarshalError` | byte-identical-cross-boundary trait, stricter than `Send` |
| `swap_cell` | `SwapCell`, `SwapCellOption`, `Guard` | an `Arc` swapped atomically and read without touching its reference count |

The catalog and helpers:

| Module | Public type | Role |
|---|---|---|
| `axis_signature` | `Axis`, `AxisMask`, `Fusion` | direction-signature catalog the deque dispatcher routes workloads by |
| `cpuid` | `cpuid()`, `Cpuid`, `has_movdir64b`, `has_waitpkg`, `core_kind_here` | what the processor reports through CPUID, read once per process |

`subetha-cxc`'s deque dispatcher gives each deque variant an
`AxisMask` signature and routes a workload to a variant whose
signature satisfies the workload's. The cross-process primitives that
move typed payloads through a byte layout (`SharedDeque<T>`,
`Channel<T>`, `AdaptiveIpc<T>`) require `T: Marshal`.

## The five core abstractions

1. **[`HandshakeHeader`](handshake.md)** - a 128-byte two-cache-line
   header with the generation counter (read-mostly, line 0) and the
   in-flight counters (write-hot, line 1). Op entry captures the
   current generation and increments the in-flight slot indexed by
   `generation & 1`; op exit decrements.

2. **[`ObservationRing`](observation.md)** - a 64-byte-aligned ring
   of 4096 `Observation` records, one per primitive instance. Every
   thread that operates on the instance pushes into it; the sidecar's
   scan thread pops.

3. **[`Migration`](migration.md)** - the dual-stack swap protocol:
   allocate new alongside old, bump generation, drain the old
   generation's in-flight counter to zero, free old. `MigrationGuard`
   carries the bump and the drain; allocating the new representation
   and freeing the old one are the caller's.

4. **`Marshal`** - the type-system contract for "this value can cross
   an address-space boundary byte-identically." Stricter than `Send`
   because shared memory does not relocate references. The typed
   channel and deque primitives in `subetha-cxc` bound their payload
   type on `Marshal`, so a `SharedDeque<Vec<u8>>` is a compile error
   and a `SharedDeque<u64>` is fine.

5. **[`SwapCell`](swap-cell.md)** - an `Arc` swapped atomically and
   read without touching its reference count. A read records its
   pointer in a slot of its own thread's; a write that replaces the
   value pays each recorded read a reference, so a replaced value is
   dropped when its last reader lets go. Every SubEtha crate keeps its
   shared snapshots in one.

## Measuring it

The `async_overhead` bench (`cargo bench -p subetha-cxc --bench
async_overhead`) times a single-threaded `Channel<u64>` round trip
three ways: `send`/`recv`, the blocking pair, and the async pair. Its
sync round trip measured ~4.5 ns/op on an AMD Ryzen 9 7900X under
Windows 11.

## Re-exports

`subetha-core`'s `lib.rs` re-exports the most-used types at crate
root:

```rust
pub use axis_signature::{Axis, AxisMask, Fusion};
pub use cpuid::{has_movdir64b, has_waitpkg};
pub use handshake::HandshakeHeader;
pub use marshal::{Marshal, MarshalError};
pub use migration::{Generation, MigrationGuard};
pub use observation::{Observation, ObservationRing, any_observer_armed, thread_id};
pub use swap_cell::{SwapCell, SwapCellOption};
```

## See also

- [Architecture overview](../../explanation/architecture.md) - the
  three-layer (substrate / sidecar / primitives) decomposition.
- [The frozen-handshake explanation](../../explanation/frozen-handshake.md) -
  why the header is laid out the way it is, and why the layout is
  frozen across crate versions.
- [The sidecar control plane](../subetha-sidecar/_index.md) - what
  consumes the observation rings and decides migrations.
