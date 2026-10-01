# subetha-core

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-core/LICENSE-MIT)
[![Wiki](https://img.shields.io/badge/wiki-variably--constant.github.io-blue)](https://variably-constant.github.io/SubEtha/docs/reference/subetha-core/)

> **You probably want the [`subetha`](https://crates.io/crates/subetha)
> umbrella crate instead.** It pulls in `subetha-core` plus both
> primitive families (`subetha-pointers` adaptive in-process and
> `subetha-cxc` MMF cross-process) in one install, and re-exports
> `subetha-core` under `subetha::core`. Reach for `subetha-core`
> directly only when you are writing a third-party primitive that
> sits on the substrate.

The substrate for the [SubEtha](https://github.com/Variably-Constant/SubEtha)
adaptive primitives library. Six modules:

| Module | Public types | Role |
|---|---|---|
| `handshake` | `HandshakeHeader` | per-instance generation + in-flight tracker |
| `observation` | `Observation`, `ObservationRing`, `thread_id`, `any_observer_armed` | per-instance multi-producer ring of op observations |
| `migration` | `Generation<'a>`, `MigrationGuard<'a>` | RAII guards for the dual-stack protocol |
| `marshal` | `Marshal`, `MarshalError` | the contract for a value that crosses an address-space boundary byte-identically |
| `axis_signature` | `Axis`, `AxisMask`, `Fusion` | the direction-signature catalog the deque dispatcher routes by |
| `cpuid` | `cpuid()`, `Cpuid`, `has_waitpkg`, `has_movdir64b`, `core_kind_here` | what the processor reports through CPUID, read once per process |

## What it ships

- **`HandshakeHeader`**. 128 bytes total, 64-byte aligned, two
  cache lines. Generation counter and strategy tag on line 0
  (read-mostly); in-flight counters indexed by generation parity
  on line 1 (write-hot). `enter_op` uses the canonical RCU/epoch
  double-check pattern; `migrate` and `drain` give the migration
  coordinator the bump-and-drain protocol.

- **`ObservationRing`**. A ring of 4096 24-byte `Observation`
  records that any number of threads push into and one consumer
  (the sidecar) pops. While no ring in the process is armed, a push
  is one relaxed load and a branch, and a ring allocates its 96 KiB
  buffer only when a sidecar arms it. A push into a full ring is
  dropped (sampling, not coordination).

- **`Generation` and `MigrationGuard`**. RAII helpers for
  op-side entry/exit and coordinator-side migrate-then-drain.

- **`Marshal`**. An `unsafe` trait for a value that is flattened into
  a fixed-size byte payload and rebuilt byte-identically in another
  address space. Implemented for the fixed-width integers, `f32`,
  `f64`, `bool`, `()`, arrays and pairs. `subetha-cxc`'s
  `SharedDeque<T>`, `Channel<T>` and `AdaptiveIpc<T>` bound their
  payload on it.

- **`AxisMask`**. A bitmask over the thirteen design-cube axes. A
  deque variant's signature `satisfies` a workload's required one
  when it engages every axis the workload needs.

- **`cpuid`**. One snapshot of the vendor and signature, the
  user-mode monitor-wait families (`MONITORX`, WAITPKG), MOVDIR64B,
  RDTSCP and the invariant TSC, the cache and monitor line sizes,
  the hypervisor, and whether the processor has cores of more than
  one kind.

## Requirements

SubEtha builds on **stable Rust** (edition 2024, MSRV 1.96). The
`rust-toolchain.toml` at the workspace root pins the stable channel;
downstream projects need only a recent stable toolchain.

## Where it sits

`subetha-core` has no dependencies, and the rest of SubEtha builds on
it:

| Crate | Builds on |
|---|---|
| `subetha-core` | nothing |
| `subetha-pointers` | `subetha-core` |
| `subetha-sidecar` | `subetha-core` |
| `subetha-cxc` | `subetha-core`, `subetha-pointers`, `subetha-sidecar` |
| `subetha-ffi` | `subetha-cxc` |
| `subetha` | re-exports `subetha-core`, `subetha-pointers`, `subetha-sidecar` and `subetha-cxc` |

## Documentation

Full reference at the published wiki:
<https://variably-constant.github.io/SubEtha/docs/reference/subetha-core/>.

## License

MIT. See [LICENSE-MIT](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-core/LICENSE-MIT).
