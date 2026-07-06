# subetha-sidecar

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE-MIT)
[![Wiki](https://img.shields.io/badge/wiki-variably--constant.github.io-blue)](https://variably-constant.github.io/SubEtha/docs/reference/subetha-sidecar/)

> **You probably want the [`subetha`](https://crates.io/crates/subetha)
> umbrella crate instead.** It pulls in `subetha-sidecar` along
> with both primitive families and re-exports the sidecar under
> `subetha::sidecar`. Reach for `subetha-sidecar` directly only
> when you are writing a third-party primitive that needs the
> control plane without the rest of the workspace.

The control plane for the [SubEtha](https://github.com/Variably-Constant/SubEtha)
adaptive primitives library. One background thread per detected
NUMA node drains every registered instance's observation ring,
folds the observations into per-instance `InstanceStats`, and
asks each instance's `Policy` whether to migrate the strategy.

## What it ships

- **`Sidecar`**. The process-wide singleton, accessed via
  `global() -> Arc<Sidecar>`. Pool of one `NodeSidecar` per
  detected NUMA node, each with its own slot table and scan
  thread.

- **`SidecarBox<T: AdaptiveInstance>`**. RAII wrapper that
  registers the primitive on construction and unregisters on
  drop. Drop order is load-bearing: `handle` drops first (blocks
  on any in-flight scan), then `inner: Box<T>` drops, so the
  scan thread never sees freed memory.

- **`AdaptiveInstance` trait**. The contract a primitive
  satisfies to be registered: `header()`, `ring()`,
  `make_policy()`, optional `apply_migration(new_tag)`.

- **`Policy` trait**. The decision function the sidecar calls
  per scan iteration: `decide(&InstanceStats, current_tag) ->
  Option<u32>`. Ships with `FixedPolicy(u32)` and
  `NoMigrationPolicy` built in.

- **`InstanceStats`**. The drain-and-fold accumulator the
  sidecar maintains per registered instance. Fields:
  `ops_observed`, `total_latency_ticks`, `contention_ops`,
  `op_kind_counts: [u64; 8]`, `last_seen_us_ago`,
  `migrations_triggered`, plus a per-op-kind distinct-thread
  cache for multi-producer detection.

## Capacity constants

| Constant | Value | Meaning |
|---|---|---|
| `N_OP_KINDS` | 8 | per-primitive op-kind enum size; index 0 reserved for "unspecified" |
| `MAX_TRACKED_THREADS_PER_KIND` | 4 | cardinality cache size; counts saturate at `MAX + 1` |
| `DEFAULT_MAX_INSTANCES` | 10,000 | hard cap on simultaneously-registered instances |
| `POLL_INTERVAL` | 200 us | scan thread sleep between iterations |
| `NODE_ID_BITS` | 8 | upper bits of `InstanceId` reserved for NUMA node index |

The instance cap is configurable via `Sidecar::set_max_instances`.
The rest are fixed by design. See the
[`tune-sidecar` how-to](https://variably-constant.github.io/SubEtha/docs/how-to/tune-sidecar/)
for the rationale behind each constant.

## Quick start

```rust,no_run
use subetha_sidecar::{SidecarBox, Policy, InstanceStats};
use subetha_core::{HandshakeHeader, ObservationRing};

struct MyPrim {
    header: HandshakeHeader,
    ring: ObservationRing,
}

impl subetha_sidecar::AdaptiveInstance for MyPrim {
    fn header(&self) -> &HandshakeHeader { &self.header }
    fn ring(&self) -> &ObservationRing { &self.ring }
    fn make_policy(&self) -> Box<dyn Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

// Construct + register in one call. Drop unregisters.
let prim = SidecarBox::new(MyPrim {
    header: HandshakeHeader::new(),
    ring: ObservationRing::new(),
});
let stats: Option<InstanceStats> = prim.stats();
```

For composing primitives via `SidecarBox`, including the
`Arc<SidecarBox<T>>` cross-thread pattern and the
`register_raw` escape hatch, see
[Compose primitives via SidecarBox](https://variably-constant.github.io/SubEtha/docs/how-to/sidecar-box/).

## Requirements

SubEtha builds on **stable Rust** (edition 2024, MSRV 1.96). The
`rust-toolchain.toml` at the workspace root pins the stable channel;
downstream projects need only a recent stable toolchain.

## Where it sits

```text
your code
    -> subetha / subetha-pointers  (the two primitive families)
       -> subetha-sidecar            (this crate; control plane)
          -> subetha-core            (substrate)
```

## Documentation

Full reference at the published wiki:
<https://variably-constant.github.io/SubEtha/docs/reference/subetha-sidecar/>.

## License

MIT. See [LICENSE-MIT](LICENSE-MIT).
