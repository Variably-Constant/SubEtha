---
title: "IPC Pointers"
weight: 310
sidebar:
  open: true
---

# IPC addressing pointers

Low-level pointer types that other MMF-backed primitives compose into. Use these directly only when building a new shared type.

| Primitive | What it carries |
|---|---|
| [Offset Pointer](offset-ptr/) | File-relative offset (no tag bits); the foundational position-independent pointer |
| [Tagged Offset Pointer](tagged-offset-ptr/) | High-bit-stealing tagged offset pointer; pack a small tag (state, type, generation) alongside the offset |

## See also

- [`subetha-pointers` Exotic Pointers](../../subetha-pointers/exotic-pointers/) - the in-process sibling pointer family (Umbra, K-Tower, Merkle, etc.).
