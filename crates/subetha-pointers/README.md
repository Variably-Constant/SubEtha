# subetha-pointers

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-pointers/LICENSE-MIT)
[![Wiki](https://img.shields.io/badge/wiki-variably--constant.github.io-blue)](https://variably-constant.github.io/SubEtha/)

Exotic pointer types for the
[SubEtha](https://github.com/Variably-Constant/SubEtha) (CXC) adaptive
primitives library. Each pointer carries an auxiliary payload beside
the address, and declares the `K_*` axes it engages as a
`subetha_core::AxisMask` in its `SIGNATURE` constant.

| Module | Public types | Carries beside the address |
|---|---|---|
| `umbra_pointer` | `UmbraPointer<T>`, `ArcUmbra<T>`, `UmbraOwner<T>` | inline content prefix (Umbra-style short-string / short-slice fast path) |
| `bloom_pointer` | `BloomPointer<T>`, `Bloom64`, `BloomFine`, `BloomCascade` | probabilistic set summary for membership pre-checks |
| `kstep_pointer` | `KStepPointer<T>`, `StridedIter<T>` | a log2 stride for branch-free strided iteration |
| `k_tower_pointer` | `KTower2<T>`, `KTower3<T>` | 2- or 3-segment address space for multi-region payloads |
| `self_desc_pointer` | `SelfDescPointer<T>`, `LayoutShape` | a type / layout discriminant read at deref |
| `versioned_pointer` | `VersionedPointer<T>`, `HlcVersionedPointer<T>`, `VectorClockPointer<T, N>`, `VectorClock`, `HybridLogicalClock`, `VersionedChain` | a version: a counter, a hybrid logical clock, or a vector clock |
| `cardinality_pointer` | `CardinalityPointer<T>`, `SizeTier` | log2 of a cardinality estimate, in the address's high byte |
| `adaptive_cheri_pointer` | `ReadableCapability`, `WritableCapability`, `OwnedReadableCapability`, `OwnedWritableCapability` | CHERI-style bounds + permissions enforced at deref |
| `adaptive_rasp_batch` | `RaspBatch`, `RaspBatchIndex` | a batched bounds-checked access table |

## What it ships

- **Content-prefix (`UmbraPointer`)**. Stores a short inline prefix of
  the pointee so a comparison or hash can short-circuit before chasing
  the pointer; `ArcUmbra` is the reference-counted owner.
- **Bloom-filtered (`BloomPointer`)**. A pointer paired with a Bloom
  summary (`Bloom64` / `BloomFine` / `BloomCascade`) so a negative
  membership test never dereferences.
- **Log2-strided (`KStepPointer`)**. A base plus a power-of-two stride;
  `StridedIter` walks it with shifts instead of multiplies.
- **Multi-segment (`KTower2` / `KTower3`)**. Two or three address
  segments behind one handle for payloads split across regions.
- **Self-describing (`SelfDescPointer`)**. A type ID and a layout shape
  (`LayoutShape`) travel with the address.
- **Versioned (`VersionedPointer` / `HlcVersionedPointer` /
  `VectorClockPointer`)**. Carry a counter, a `HybridLogicalClock` or a
  `VectorClock` for snapshot and causal-order checks; `VersionedChain`
  links successive versions.
- **Cardinality-tagged (`CardinalityPointer`)**. A cardinality estimate
  rides alongside the address for sizing decisions (`SizeTier`).
- **CHERI-style capabilities (`adaptive_cheri_pointer`)**. Bounds and
  read / write permissions checked at every deref, over borrowed or
  owned memory; a capability cannot outlive what it was made from.

## Where it sits

`subetha-pointers` is one of the two primitive families in the SubEtha
stack; it sits on the shared substrate:

```text
your code
    -> subetha-pointers   (this crate; in-process exotic pointers)
       -> subetha-core     (substrate: AxisMask, handshake, observation)
```

It depends only on `subetha-core`.

## Requirements

SubEtha builds on **stable Rust** (edition 2024, MSRV 1.96). The
`rust-toolchain.toml` at the workspace root pins the stable channel;
downstream projects need only a recent stable toolchain.

## Documentation

Full reference at the published wiki:
<https://variably-constant.github.io/SubEtha/>.

## License

MIT. See [LICENSE-MIT](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-pointers/LICENSE-MIT).
