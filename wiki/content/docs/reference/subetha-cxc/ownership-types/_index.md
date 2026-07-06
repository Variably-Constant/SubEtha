---
title: "Ownership & Election"
weight: 280
sidebar:
  open: true
---

# Ownership and election primitives

Who-holds-the-token primitives with auto-failover when a holder dies.

| Primitive | Role pair |
|---|---|
| [Owner Lease](owner-lease/) | Cross-process Mutex with auto-failover; exclusive access to a resource the holder might die holding |
| [Shared Leader Election](shared-leader-election/) | Exactly-one-leader-at-a-time; auto-elect a replacement on death |
| [Lazy Config](lazy-config/) | Thundering-herd-proof distributed config fetch; one process fetches, rest read |

For the prose overview, see [ownership](../ownership/).
