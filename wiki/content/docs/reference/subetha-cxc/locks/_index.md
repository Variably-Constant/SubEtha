---
title: "Locks & Synchronisation"
weight: 250
sidebar:
  open: true
---

# Cross-process locks and synchronisation primitives

Mutual exclusion, semaphores, rate-limiting, and logical clocks.

| Primitive | Shape |
|---|---|
| [Shared RW Lock](shared-rw-lock/) | Reader-writer lock with writer preference |
| [Shared Semaphore](shared-semaphore/) | Counting semaphore; bounded resource pool |
| [Shared Rate Limiter](shared-rate-limiter/) | Token-bucket rate limiter; shared budget across processes |
| [Shared Fence Clock](shared-fence-clock/) | Hybrid Logical Clock (HLC) for cross-process event ordering |

For the prose overview, see [shared-locks](../shared-locks/).
