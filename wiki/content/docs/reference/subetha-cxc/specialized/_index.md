---
title: "Specialised Primitives"
weight: 300
sidebar:
  open: true
---

# Specialised cross-process primitives

Specific shapes for workloads that don't fit a single broader category.

| Primitive | Shape |
|---|---|
| [Shared Vec](shared-vec/) | Bounded indexable sequence |
| [Shared Graph](shared-graph/) | Directed graph; nodes and edges in one MMF |
| [Shared NaN Value](shared-nan-value/) | 64-bit NaN-boxed heterogeneous value cell |
| [Shared NaN-Tagged Value](shared-nan-tagged-value/) | NaN-boxed with the payload type encoded in the pointer bits |
| [Shared Time Point](shared-time-point/) | BSPA + Versioned 16-slot tile with snapshot-isolation scan |
| [Shared Topology Map](shared-topology-map/) | K_process axis observer recommending a transport topology (P2P / broadcast / mesh) |
| [Shared Umbra Pointer](shared-umbra-pointer/) | Cross-process content-prefixed pointer |
| [Shared Universal](shared-universal/) | Layer-2 cross-process container that auto-picks strategy |
| [Shared Versioned Chain](shared-versioned-chain/) | MVCC linked list; time-travel reads at a versioned snapshot |
| [Shm File](shm-file/) | Cross-platform RAM-resident named shared-memory backing |
