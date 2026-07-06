---
weight: 60
---

# Locks and synchronisation

Four cross-process synchronisation primitives. Each lifts a
classic in-memory shape into an MMF byte layout so two
processes can coordinate without an IPC channel.

## `SharedRWLock`

A reader-writer lock with shared (read) and exclusive (write)
acquisition. Multiple processes share the lock state via the
MMF; the lock's word lives at a known offset in the file.

```rust,no_run
pub fn create(path: impl AsRef<Path>) -> Result<Self, RWLockError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, RWLockError>;

pub fn read_lock(&self) -> ReadGuard<'_>;       // blocking
pub fn write_lock(&self) -> WriteGuard<'_>;     // blocking
pub fn try_read_lock(&self) -> Result<ReadGuard<'_>, RWLockError>;
pub fn try_write_lock(&self) -> Result<WriteGuard<'_>, RWLockError>;
pub fn reader_count(&self) -> u32;
pub fn has_writer(&self) -> bool;
```

The guards are RAII; drop releases the lock. The blocking
`read_lock` / `write_lock` spin with periodic yields and return the
guard directly; the `try_*` variants return `Err` when the lock
cannot be acquired immediately. The cross-process unfair-fast path is
the same fast path as in-process locks.

Op kinds: `OP_READ = 1`, `OP_WRITE = 2`, `OP_TRY_READ = 3`,
`OP_TRY_WRITE = 4`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_RW_LOCK.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RW_LOCK.md).

## `SharedSemaphore`

Counting semaphore. `try_acquire` takes one permit if the count is
positive; `acquire` blocks until one is free. The permit count lives in
a `SharedAtomicU32` (`<base>.count.bin`), with a second
`SharedAtomicU32` tracking waiters; CAS-loop on acquire, `fetch_add` on
release.

```rust,no_run
pub fn create(base_path: impl AsRef<Path>, initial: u32, max_permits: u32) -> Result<Self, SemaphoreError>;
pub fn open(base_path: impl AsRef<Path>, max_permits: u32) -> Result<Self, SemaphoreError>;

pub fn try_acquire(&self) -> Result<Permit<'_>, SemaphoreError>;  // Err if none free
pub fn acquire(&self) -> Permit<'_>;                             // blocks
pub fn release(&self) -> Result<(), SemaphoreError>;
```

`Permit<'_>` is the RAII permit type; drop releases one permit.
Op kinds: `OP_ACQUIRE = 1`, `OP_RELEASE = 2`, `OP_TRY_ACQUIRE = 3`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_SEMAPHORE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_SEMAPHORE.md).

## `SharedRateLimiter`

Token-bucket rate limiter. The bucket state (current tokens,
last refill timestamp, refill rate, capacity) lives in the MMF;
the `try_acquire` call refills based on elapsed wall time and
then attempts a token consumption.

```rust,no_run
pub fn create(
    path: impl AsRef<Path>,
    capacity: u32,
    refill_rate_per_sec: u32,
) -> Result<Self, RateLimiterError>;

pub fn open(path: impl AsRef<Path>, capacity: u32, refill_rate_per_sec: u32) -> Result<Self, RateLimiterError>;

pub fn try_acquire(&self, n: u32) -> Result<(), RateLimiterError>;  // Err when short on tokens
pub fn available(&self) -> u32;
```

The refill calculation is deterministic across processes
because both processes see the same `last_refill_ns` field; the
CAS on the token count is what serialises concurrent
acquisitions. Op kinds: `OP_TRY_ACQUIRE = 1`, `OP_AVAILABLE = 2`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_RATE_LIMITER.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RATE_LIMITER.md).

## `SharedFenceClock`

Hybrid Logical Clock (HLC). Each participating process has a
slot in the clock's slot table; the slot carries the process's
local HLC value. `tick` advances the local clock; `merge` takes
an incoming HLC and updates the local clock to
`max(local, incoming) + 1`. `compute_global_fence` reads every slot's
current value to compute the global fence point.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, FenceClockError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, FenceClockError>;

pub fn register(&self, pid: u32) -> Result<usize, FenceClockError>;  // claim a slot
pub fn tick(&self, idx: usize) -> Hlc;
pub fn merge(&self, idx: usize, remote: Hlc) -> Hlc;
pub fn get_local(&self, idx: usize) -> Hlc;
pub fn compute_global_fence(&self) -> Hlc;
pub fn slot_snapshot(&self, idx: usize) -> Option<HlcSlotSnapshot>;
```

The use case is distributed event ordering across processes
without a central coordinator. Each event carries an HLC; the
receiver's `merge` integrates the incoming clock with its local
one. Op kinds: `OP_TICK = 1`, `OP_MERGE = 2`, `OP_GET_LOCAL = 3`,
`OP_COMPUTE_FENCE = 4`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_FENCE_CLOCK.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_FENCE_CLOCK.md).

## See also

- [Role-pair selection](../../how-to/role-pair-selection.md) -
  the mutex role pair.
- [Coordination primitives](coordination.md) - higher-level
  liveness and election primitives built on these.
