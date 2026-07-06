---
title: "Blocking RW Lock"
weight: 57
---

# BlockingRWLock

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Slowpath](https://img.shields.io/badge/slow--path-kernel_park-brightgreen)

Cross-process reader-writer lock with a kernel-park slow path
via [`CrossProcessWaker`]({{< ref "cross-process-waker" >}}).
Composes
[`SharedRWLock`]({{< ref "../shared-locks" >}}#sharedrwlock) with
one waker plus a small mmap-backed wakeup-generation counter.

> **The "rwlock state atom + futex park" primitive.** Hot path is
> a single CAS on the inner state atom. Contention slow path
> snapshots the wakeup-generation atom, parks on a waker slot at
> `target = snapshot + 1`, then waits in the kernel. Every unlock
> bumps the generation and calls `wake_up_to(new_gen)` so all
> parked readers + writers re-check the state and the
> writer-priority policy picks the next holder.

## Constraints vs the existing `SharedRWLock`

- **`SharedRWLock::read_lock` / `write_lock`** spin → `yield` →
  `sleep(50µs)` indefinitely until the lock is available.
- **`BlockingRWLock::read_park` / `write_park`** loop
  `try_*_lock`, then park on the waker. Wakeup within
  microseconds of the next unlock.

## Operations

```rust
use std::path::Path;
use std::time::Duration;
use subetha_cxc::{BlockingRWLock, BlockingRWLockError};

impl BlockingRWLock {
    pub fn create(base: impl AsRef<Path>) -> Result<Self, BlockingRWLockError>;
    pub fn open(base: impl AsRef<Path>) -> Result<Self, BlockingRWLockError>;

    pub fn try_read_lock(&self) -> Result<BlockingReadGuard<'_>, BlockingRWLockError>;
    pub fn try_write_lock(&self) -> Result<BlockingWriteGuard<'_>, BlockingRWLockError>;

    pub fn read_park(&self) -> Result<BlockingReadGuard<'_>, BlockingRWLockError>;
    pub fn read_park_timeout(
        &self, timeout: Duration,
    ) -> Result<BlockingReadGuard<'_>, BlockingRWLockError>;

    pub fn write_park(&self) -> Result<BlockingWriteGuard<'_>, BlockingRWLockError>;
    pub fn write_park_timeout(
        &self, timeout: Duration,
    ) -> Result<BlockingWriteGuard<'_>, BlockingRWLockError>;

    pub fn inner(&self) -> &Arc<SharedRWLock>;  // sidecar / observability hook
}
```

The read/write acquire calls return a `BlockingReadGuard<'_>` /
`BlockingWriteGuard<'_>` RAII guard whose `Drop` releases the inner read/write
state and then calls the private `signal_unlock` (bump the wakeup atom +
`wake_up_to(new_gen)`). Both readers and writers park on the SAME waker, so an
unlock's `wake_up_to` re-checks every parker and the underlying
writer-priority policy picks the next holder. `BlockingRWLockError` has five
variants: `Lock(RWLockError)`, `Waker(WakerError)`, `Timeout` (a
`WakerError::Timeout` folds into this), `LayoutMismatch` (wakeup-region magic /
size mismatch on `open`), and `Io(std::io::ErrorKind)`.

Layout files: `<base>.rwlock.bin` for the inner state,
`<base>.waker.bin` for the waker slots, `<base>.wakeup.bin` for
the magic + generation atom.

## E2E proof

`examples/blocking_locks_demo.rs` spawns 1 writer thread + 6
reader threads against a `BlockingRWLock`; writer holds for 1.2ms
periodically, readers contend for 30 acquires each. Observes 7
slow-path parks across the readers + writer; all locks released
cleanly without protocol violation.

## See also

- [`SharedRWLock`]({{< ref "../shared-locks" >}}#sharedrwlock):
  the underlying state atom (existing surface, unchanged).
- [`CrossProcessWaker`]({{< ref "cross-process-waker" >}}): the
  waker substrate.
- [`BlockingSemaphore`]({{< ref "blocking-semaphore" >}}): same
  pattern for counting semaphores.
