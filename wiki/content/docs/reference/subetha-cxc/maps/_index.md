---
title: "Maps & Lists"
weight: 210
sidebar:
  open: true
---

# Maps, ordered maps, and linked sequences

Keyed lookup and ordered storage primitives.

| Primitive | Lookup shape |
|---|---|
| [Shared Hash Map](shared-hash-map/) | O(1) average; open-addressed; FNV-1a hashing for cross-process determinism |
| [Shared B-Tree Map](shared-btree-map/) | Ordered; supports range queries |
| [Shared Linked List](shared-linked-list/) | Doubly-linked; stable iterator positions across mutations |

For prose overview of the category, see [shared-hash-map](../shared-hash-map/).
