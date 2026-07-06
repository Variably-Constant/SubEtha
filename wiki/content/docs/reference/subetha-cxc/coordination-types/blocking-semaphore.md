---
title: "Blocking Semaphore"
weight: 56
---

# BlockingSemaphore

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Slowpath](https://img.shields.io/badge/slow--path-kernel_park-brightgreen)

Cross-process counting semaphore with a kernel-park slow path
via [`CrossProcessWaker`]({{< ref "cross-process-waker" >}}).
Composes
[`SharedSemaphore`]({{< ref "../shared-locks" >}}#sharedsemaphore)
(counter + generation primitive) with one waker.

> **The "permit count + futex park" primitive.** Hot path is a
> single CAS on the inner permit count. Contention slow path
> snapshots the inner generation atom, parks on a waker slot at
> `target = snapshot + 1`, then waits in the kernel. The next
> `release` bumps the generation and fires `wake_up_to(new_gen)`,
> which wakes the parker.

## Constraints vs the existing `SharedSemaphore`

- **`SharedSemaphore::acquire`** loops `try_acquire` →
  `yield_now` → `sleep(50µs)` indefinitely. The sleep tail burns
  CPU on each wake-up tick AND can miss a release by up to 50µs.
- **`BlockingSemaphore::acquire_park`** loops `try_acquire`, then
  registers in the waker at the current generation, then parks
  via the platform wait syscall. The kernel returns within
  microseconds of the next `release`.

## Operations

```rust
use std::path::Path;
use std::time::Duration;
use subetha_cxc::{BlockingSemaphore, BlockingSemaphoreError};

impl BlockingSemaphore {
    pub fn create(
        base: impl AsRef<Path>, max_permits: u32, init_permits: u32,
    ) -> Result<Self, BlockingSemaphoreError>;
    pub fn open(
        base: impl AsRef<Path>, expected_max_permits: u32,
    ) -> Result<Self, BlockingSemaphoreError>;

    pub fn try_acquire(&self) -> Result<BlockingPermit<'_>, BlockingSemaphoreError>;
    pub fn acquire_park(&self) -> Result<BlockingPermit<'_>, BlockingSemaphoreError>;
    pub fn acquire_park_timeout(
        &self, timeout: Duration,
    ) -> Result<BlockingPermit<'_>, BlockingSemaphoreError>;

    pub fn release(&self) -> Result<(), BlockingSemaphoreError>;

    pub fn available(&self) -> u32;     // current permit count (may race)
    pub fn max_permits(&self) -> u32;   // cap fixed at construction
    pub fn inner(&self) -> &Arc<SharedSemaphore>;  // sidecar / observability hook
}
```

`try_acquire` / `acquire_park` / `acquire_park_timeout` return a
`BlockingPermit<'_>` RAII guard whose `Drop` calls `release` (so a held permit
is returned automatically when it leaves scope; a release-overflow or waker
error inside `Drop` is surfaced on stderr, not panicked). `BlockingSemaphoreError`
has three variants: `Semaphore(SemaphoreError)`, `Waker(WakerError)`, and
`Timeout` (a `WakerError::Timeout` is folded into the top-level `Timeout`).

The composed files include the original `SharedSemaphore` triplet
(`<base>.count.bin`, `<base>.wakeup.bin`, `<base>.waiters.bin`)
plus `<base>.waker.bin` for the waker slots.

## E2E proof

`examples/blocking_locks_demo.rs` spawns 8 contending threads
against a 2-permit semaphore; runs 160 total `acquire_park` calls
with 800µs hold + release; observes 39 slow-path parks (~24% of
acquires hit the kernel-park slow path); FIFO not strictly
enforced (semaphore doesn't promise fairness) but every permit
released and re-acquired without loss.

## See also

- [`SharedSemaphore`]({{< ref "../shared-locks" >}}#sharedsemaphore):
  the underlying counter primitive (existing surface, unchanged).
- [`CrossProcessWaker`]({{< ref "cross-process-waker" >}}): the
  waker substrate.
- [`BlockingRWLock`]({{< ref "blocking-rw-lock" >}}): same
  pattern for reader-writer locks.
