---
title: "Bounds Checking"
weight: 110
sidebar:
  open: true
---

# Bounds-checking primitives

Hardware-flavored pointer-safety primitives. The capability pair
here carries a region descriptor (base, length, permissions)
alongside the pointer itself, so a dereference can be validated
against the descriptor before the load actually issues:
out-of-bounds and wrong-permission accesses become observable
failures instead of silent corruption, and a capability cannot
outlive the memory it was made from.

| Primitive | What it carries | Use when |
|---|---|---|
| [Capabilities (CHERI)](cheri-capability/) | A software capability (base + length + permissions), checked on every access | Bounds and permission checks on every access, with read and write split at the type level |
| [RASP (Register-Aligned SIMD Pointer)](rasp-pointer/) | SoA-stored (base, length, perms) per pointer with AVX2 / AVX-512F batch validation | x86 silicon where CHERI hardware does not exist; need millions of bounds checks per second across a large pointer fan-out |

## The two shapes

The **CHERI** model enforces bounds in **silicon**: on CHERI
hardware such as Arm's Morello board, the processor refuses to
dereference outside a capability's range, and capability arithmetic
that escapes the bounds invalidates the capability tag. This crate's
capabilities carry the same (base, length, perms) descriptor and
check it in software on every target, in two wrappers:
`ReadableCapability<'a, T>` for read-only access and
`WritableCapability<'a, T>` for read-write access. Both bound the
permission set so a `ReadableCapability` cannot be coerced into a
write path.

The **RASP** primitive (`RaspBatch<T>`) is the x86 sibling: there
is no x86 / x86_64 capability ISA, so the same (base, length,
perms) descriptor that CHERI carries in silicon is stored in a
structure-of-arrays layout that x86 vector instructions can
validate in batches. AVX2 checks 4 pointers per loop iteration;
AVX-512F checks 8 per iteration; both paths verify the same
bounds + permission + sealed predicates the scalar reference does,
with runtime CPUID dispatch falling back to scalar on hosts
without AVX2.

## See also

- [Exotic Pointers](../exotic-pointers/) - pointer formats with
  inline metadata that compose with bounds-check enforcement.
