---
title: Exotic Pointers
weight: 90
sidebar:
  open: true
---

# Exotic pointers

A family of pointer types that embed extra information alongside
the address. Each one steals bits, inlines a summary, decomposes
the address, or carries a content prefix - all to answer a question
about the target **without dereferencing**.

| Pointer | What it carries inline | Answers without deref |
|---|---|---|
| [Umbra Pointer](umbra-pointer/) | Short content prefix (4 bytes) | "Is the prefix equal to X?" |
| [Bloom Pointer](bloom-pointer/) | 64-bit Bloom filter of the target | "Does the target contain X?" |
| [Cardinality Pointer](cardinality-pointer/) | log2 element count of the target | "Is the target small, medium, or large?" |
| [K-Step Pointer](kstep-pointer/) | log2 stride packed into the pointer | "Where is element + N steps?" without multiply |
| [K-Tower Pointer](ktower-pointer/) | Multi-segment zone/region/offset address | "Which level of the multi-resolution structure?" |
| [Self-Describing Pointer](self-desc-pointer/) | Type discriminant | "What type does this point at?" |
| [Versioned Pointer](versioned-pointer/) | Version tag (or HLC timestamp) | "Read the value as of version V" |

## Why these exist

Every one of these pointer types is a **frozen handshake**
(see [the frozen-handshake explanation](../../../explanation/frozen-handshake/))
between the structure that constructs the pointer (writer side)
and the consumer that reads it (reader side). The conventional
choice - raw `*const T` - forces a memory load to answer any
question about the target. These pointers move the answer to the
most common question **into the pointer bits themselves**, so the
question can be answered with a single AND / compare / mask, no
dereference, no cache miss.

The win is shape-dependent. Umbra-style prefix carrying wins on
string comparison since most strings differ in the first four
bytes. Bloom-bearing pointers win on negative-membership queries
because they skip loads that would miss anyway. K-Tower
decomposition wins on multi-resolution indexed data. Versioned
pointers win on MVCC reads where a single deref must select the
right snapshot.

## Direction signatures

Every pointer here declares a `K_*` direction signature via
[`subetha_core::AxisMask`]. The MMF dispatcher consults the
signature when routing a workload: a workload asking for strided
access lands on `KStepPointer`, a workload asking for
set-membership lands on `BloomPointer`, and a workload asking for
both can land on a fused encoding.

## Composition

These are not exclusive choices. A `KTower2<UmbraPointer<T>>`
composes multi-segment addressing with a content prefix at every
leaf. A `BloomPointer<KStepPointer<T>>` composes set-membership
with strided indexing. The composition shape is left to the
consumer; the crate offers each axis as a separable type.

## See also

- [The bounds-checking siblings](../bounds-check/) - the CHERI
  capability pair for capability-secured cross-process channels.
- [`subetha-cxc` IPC pointers](../../subetha-cxc/pointers/_index.md) -
  the cross-process sibling pointer family (OffsetPtr,
  TaggedOffsetPtr).
