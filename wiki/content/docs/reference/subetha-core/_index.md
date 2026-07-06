---
weight: 20
---

# The CXC substrate (`subetha-core`)

`subetha-core` is the substrate every CXC primitive sits on. It
ships four modules every cross-process and adaptive instance needs,
plus a CPUID helper and an axis-signature catalog the dispatcher
consults to pick a primitive.

| Module | Public type | Role |
|---|---|---|
| `handshake` | `HandshakeHeader` | per-instance generation counter + in-flight tracker |
| `observation` | `Observation`, `ObservationRing`, `thread_id` | TLS-local SPSC ring for op observations |
| `migration` | `Generation<'a>`, `MigrationGuard<'a>` | RAII guards for the dual-stack migration protocol |
| `marshal` | `Marshal`, `MarshalError` | byte-identical-cross-boundary trait, stricter than `Send` |

The catalog and helpers:

| Module | Public type | Role |
|---|---|---|
| `axis_signature` | `Axis`, `AxisMask`, `Fusion` | direction-signature catalog the MMF dispatcher uses to route workloads |
| `cpuid` | `has_movdir64b`, `has_waitpkg` | x86 CPU-feature probes used by the optional fast paths |

Every CXC primitive in `subetha-cxc` and every pointer type in
`subetha-pointers` declares its direction signature via `AxisMask`,
and the cross-process primitives that store typed payloads
(`SharedDeque<T>`, `SharedHashMap<K, V>`, `Channel<T>`) require
`T: Marshal`.

## The four core abstractions

1. **[`HandshakeHeader`](handshake.md)** - a 128-byte two-cache-line
   header with the generation counter (read-mostly, line 0) and the
   in-flight counters (write-hot, line 1). Op entry captures the
   current generation and increments the in-flight slot indexed by
   `generation & 1`; op exit decrements.

2. **[`ObservationRing`](observation.md)** - 64-byte-aligned SPSC ring
   of 4096 `Observation` records. One ring per producer thread. The
   sidecar drains all rings asynchronously. Push cost is ~3 cycles
   steady state, ~2.8 ns measured.

3. **[`Migration`](migration.md)** - the dual-stack swap protocol:
   allocate new alongside old, bump generation, drain the old
   generation's in-flight counter to zero, free old. The
   `MigrationGuard` RAII type wraps the protocol so callers cannot
   leak a half-completed migration.

4. **`Marshal`** - the type-system contract for "this value can cross
   an address-space boundary byte-identically." Stricter than `Send`
   because shared memory does not relocate references. Every typed
   cross-process primitive in `subetha-cxc` bounds its payload type
   on `Marshal`, so a `SharedDeque<Vec<u8>>` is a compile error and
   a `SharedDeque<u64>` is fine.

## Performance floor

The substrate's per-op cost is dominated by the `enter_op` / `exit_op`
bracket - two atomic-RMW on the in-flight counter (~6 ns each on a Zen+
R7 2700, so ~14 ns for the pair) - plus, on flagged ops, a single
SPSC observation push (~3 cycles, ~2.8 ns). A full uncontended
`migrate` dual-stack swap with no in-flight readers is ~80 ns. The
bracket fires only on ops the sidecar might care about; the fast-path
tag read is a single relaxed load + branch (~300 ps), not "every
memory access".

For a runnable end-to-end measurement of the floor, the `async_overhead`
bench (`cargo bench -p subetha-cxc --bench async_overhead`) reports a
single-threaded `Channel<u64>` sync round-trip at ~27 ns/op on a Zen+
R7 2700.

## Re-exports

`subetha-core::lib.rs` re-exports the most-used types at crate
root:

```rust
pub use axis_signature::{Axis, AxisMask, Fusion};
pub use cpuid::{has_movdir64b, has_waitpkg};
pub use handshake::HandshakeHeader;
pub use marshal::{Marshal, MarshalError};
pub use migration::{Generation, MigrationGuard};
pub use observation::{Observation, ObservationRing, thread_id};
```

## See also

- [Architecture overview](../../explanation/architecture.md) - the
  three-layer (substrate / sidecar / primitives) decomposition.
- [The frozen-handshake explanation](../../explanation/frozen-handshake.md) -
  why the header is laid out the way it is, and why the layout is
  frozen across crate versions.
- [The sidecar control plane](../subetha-sidecar/_index.md) - what
  consumes the observation rings and decides migrations.
