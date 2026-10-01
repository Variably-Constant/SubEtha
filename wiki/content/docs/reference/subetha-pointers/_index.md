---
weight: 40
---

# Exotic pointer types (`subetha-pointers`)

`subetha-pointers` is a kit of pointer encodings built for the
cross-context payloads CXC carries. Each one is a thin pure-Rust
struct over `*const T` / `*mut T` with extra bytes packed alongside
the address: a content prefix, a Bloom filter, a stride, a version
tag, a CHERI capability bound. The point is that the consumer can
take a useful action - skip a deref, prune a hash bucket, branch
on type, validate a bound - without going through the data.

Every pointer here is an in-process encoding: the address it
carries is a raw `*const T` / `*mut T` (or an `Arc<T>`), valid
only within the constructing process. They are not
cross-process-portable and do not travel through an MMF unchanged.
For the cross-process siblings - position-independent encodings
that resolve in any process holding the same region - use the
`subetha-cxc` pointer family (`OffsetPtr`, `TaggedOffsetPtr`,
`SharedUmbraPointer`).

## The nine types

| Module | Type | Payload beside the pointer |
|---|---|---|
| `umbra_pointer` | `UmbraPointer<T>` | 4-byte content prefix; short-circuit equality |
| `bloom_pointer` | `BloomPointer<T>` | 64-bit Bloom filter; probabilistic set membership |
| `cardinality_pointer` | `CardinalityPointer<T>` | log2 cardinality estimate; size-class branching |
| `kstep_pointer` | `KStepPointer<T>` | log2 stride; SIMD-friendly indexing |
| `k_tower_pointer` | `KTower2<T>` / `KTower3<T>` | multi-segment zone/region/offset address |
| `self_desc_pointer` | `SelfDescPointer<T>` | type discriminant; heterogeneous channels |
| `versioned_pointer` | `VersionedPointer<T>` / `HlcVersionedPointer<T>` | version metadata; MVCC + HLC ordering |
| `adaptive_cheri_pointer` | `ReadableCapability<'a, T>` / `WritableCapability<'a, T>` | runtime bounds + permissions (CHERI-style, checked in software) |
| `adaptive_rasp_batch` | `RaspBatch<T>` / `RaspBatchIndex<T>` | SoA-stored (base, length, perms); AVX2 / AVX-512F batch validation on x86 |

The structure of the crate mirrors that:

```text
subetha-pointers/
└── src/
    ├── lib.rs
    ├── umbra_pointer.rs
    ├── bloom_pointer.rs
    ├── cardinality_pointer.rs
    ├── kstep_pointer.rs
    ├── k_tower_pointer.rs
    ├── self_desc_pointer.rs
    ├── versioned_pointer.rs
    ├── adaptive_cheri_pointer.rs
    └── adaptive_rasp_batch.rs
```

## Direction signatures

Most of these pointer types expose a `SIGNATURE: AxisMask`
associated const, built via [`subetha_core::AxisMask::from_axes`],
declaring which `Axis` it engages. The actual axes are
`ContentPrefix` (Umbra,
Bloom, Cardinality), `Stride` (KStep), `Segmented` (KTower2 /
KTower3), `TypeTag` (SelfDesc), `Version` (Versioned / HLC), and
`Bounds` (the CHERI capabilities). `AxisMask::satisfies` tests one
signature against another by containment (not equality), so a
signature asking for a subset of a pointer's axes matches it;
nothing in the crates routes on these constants.

## Subsections

- [Exotic pointers](exotic-pointers/) - umbra, bloom, cardinality,
  kstep, ktower, self-desc, versioned. The seven content-prefix /
  metadata-carrying types.
- [Bounds-check pointers](bounds-check/) - the CHERI-style
  `ReadableCapability` / `WritableCapability` pair, checked in
  software, plus the `RaspBatch<T>` SIMD-batched validator that
  runs the same (base, length, perms) checks on x86 silicon via
  AVX2 / AVX-512F.

## Stable-Rust portable

No nightly features. The pointer types are pure-Rust structs over
`*const T` / `*mut T` with content metadata. They build with the
same stable toolchain (1.96+) as the rest of the workspace, on
x86_64 and aarch64, on Linux / macOS / Windows.

## See also

- [The CXC primitives catalog](../subetha-cxc/) - which MMF
  primitive consumes which pointer kind.
- [The substrate's `AxisMask`](../subetha-core/_index.md) - how the
  direction signatures are encoded.
