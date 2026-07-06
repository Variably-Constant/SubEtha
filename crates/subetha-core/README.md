# subetha-core

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE-MIT)
[![Wiki](https://img.shields.io/badge/wiki-variably--constant.github.io-blue)](https://variably-constant.github.io/SubEtha/docs/reference/subetha-core/)

> **You probably want the [`subetha`](https://crates.io/crates/subetha)
> umbrella crate instead.** It pulls in `subetha-core` plus both
> primitive families (`subetha-pointers` adaptive in-process and
> `subetha-cxc` MMF cross-process) in one install, and re-exports
> `subetha-core` under `subetha::core`. Reach for `subetha-core`
> directly only when you are writing a third-party primitive that
> sits on the substrate.

The substrate for the [SubEtha](https://github.com/Variably-Constant/SubEtha)
adaptive primitives library. Six modules covering the
per-instance state every primitive carries at a known offset:

| Module | Public types | Role |
|---|---|---|
| `handshake` | `HandshakeHeader` | per-instance generation + in-flight tracker |
| `dualstack` | `Dualstack<T>`, `DualstackGuard<'a, T>` | dual-slot heap storage for data-layout migration |
| `observation` | `Observation`, `ObservationRing`, `thread_id` | TLS-local SPSC ring for op observations |
| `strategy_tag` | `StrategyTag` trait | per-primitive strategy enum contract |
| `migration` | `Generation<'a>`, `MigrationGuard<'a>` | RAII guards for the dual-stack protocol |
| `pmu` | `PmuKind`, `PmuSample`, `pmu_available()` | hardware PMU sampling integration |

## What it ships

- **`HandshakeHeader`**. 128 bytes total, 64-byte aligned, two
  cache lines. Generation counter and strategy tag on line 0
  (read-mostly); in-flight counters indexed by generation parity
  on line 1 (write-hot). `enter_op` uses the canonical RCU/epoch
  double-check pattern; `migrate` and `drain` give the migration
  coordinator the bump-and-drain protocol.

- **`Dualstack<T>`**. Two `AtomicPtr<T>` slots indexed by
  generation parity. Pair with a `HandshakeHeader` to coordinate
  zero-blocking data-layout migration on heap-allocated payloads.

- **`ObservationRing`**. 64-byte aligned SPSC ring of 4096
  24-byte `Observation` records. Push cost ~3 cycles
  steady-state; full-ring observations are dropped silently
  (sampling, not coordination).

- **`StrategyTag` trait**. The shared contract every per-primitive
  strategy enum implements: `Copy + Eq`, `to_u32`/`from_u32`,
  `default_tag`.

- **`Generation` and `MigrationGuard`**. RAII helpers for
  op-side entry/exit and coordinator-side migrate-then-drain.

- **`pmu`**. PMU sampling scaffolding: `PmuKind` enum
  (`None`, `IntelPebs`, `AmdIbs`, `WindowsEtw`), 24-byte
  `PmuSample` matching `Observation`'s layout, `pmu_available()`
  runtime detection.

## Performance floor

| Operation | Cost | What it measures |
|---|---|---|
| `baseline_empty` | 303 ps | one `black_box`, control |
| `enter_exit` | 13.6 ns | two atomic RMW on same cache line |
| `tag_load` | 299 ps | PIC hot-path read (cached) |
| `guard_lifecycle` | 13.2 ns | `Generation::enter` RAII drop |
| `dualstack_read` | 13.5 ns | `Dualstack::read` + deref + drop |
| `observation_push` | 2.8 ns | TLS ring SPSC push |
| `migrate_uncontended` | ~80 ns | full dual-stack swap with no readers |

The bracket only goes on slow paths. Fast paths (state load +
branch) cost ~300 ps; the bracket is "I am doing an op the
sidecar might want to know about", not "every memory access".

Measured on AMD Zen+ R7 2700 (8-core / 16-thread), Windows 11,
rustc nightly, criterion `--quick`. Reproduce via
`cargo bench -p subetha-core --bench substrate_overhead`.

## Requirements

SubEtha builds on **stable Rust** (edition 2024, MSRV 1.96). The
`rust-toolchain.toml` at the workspace root pins the stable channel;
downstream projects need only a recent stable toolchain.

## Where it sits

`subetha-core` is the bottom layer of the SubEtha four-crate
stack:

```text
your code
    -> subetha / subetha-pointers  (the two primitive families)
       -> subetha-sidecar            (control plane)
          -> subetha-core            (this crate; substrate)
```

The crate has minimal dependencies (just `crossbeam-utils`) and
is the dependency floor every other SubEtha crate sits on.

## Documentation

Full reference at the published wiki:
<https://variably-constant.github.io/SubEtha/docs/reference/subetha-core/>.

## License

MIT. See [LICENSE-MIT](LICENSE-MIT).
