---
title: "Bounds Checking"
weight: 110
sidebar:
  open: true
---

# Bounds-checking primitives

Hardware-flavoured pointer-safety primitives. The capability pair
here carries a region descriptor (base, length, permissions)
alongside the pointer itself, so a dereference can be validated
against the descriptor before the load actually issues. The result
is fail-fast unsafe access: out-of-bounds, wrong-permission, and
freed-region accesses become observable failures instead of silent
corruption.

| Primitive | What it carries | Use when |
|---|---|---|
| [Capabilities (CHERI)](cheri-capability/) | A hardware capability (base + length + permissions) on CHERI-aware platforms (Morello / ARM CHERI) | Platform-level capability enforcement; pointer arithmetic is bounded by silicon |
| [RASP (Register-Aligned SIMD Pointer)](rasp-pointer/) | SoA-stored (base, length, perms) per pointer with AVX2 / AVX-512F batch validation | x86 silicon where CHERI hardware does not exist; need millions of bounds checks per second across a large pointer fan-out |

## The two shapes

The **CHERI** capability uses the **silicon** to enforce bounds.
The processor refuses to dereference outside the capability's
range, and capability arithmetic that escapes the bounds
invalidates the capability tag. The crate ships two specialised
wrappers: `ReadableCapability<T>` for read-only access and
`WritableCapability<T>` for read-write access. Both bound the
permission set so a `ReadableCapability` cannot be coerced into a
write path. CHERI ships on ARM Morello.

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
- [`subetha-cxc` `Channel<T>`](../../subetha-cxc/_index.md) - the
  primary consumer of CHERI capabilities for capability-secured
  cross-process channels.
