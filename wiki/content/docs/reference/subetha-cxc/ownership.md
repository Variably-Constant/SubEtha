---
weight: 80
---

# Ownership and configuration

Three primitives that coordinate WHO owns or has the right to
access a shared resource across processes. The shape sits
between mutex (mutual exclusion of access) and leader election
(exclusive ownership with a designated holder).

## `OwnerLease`

Time-bounded exclusive ownership. One process holds the lease;
other processes can read the current owner's identity and the
lease's payload. The owner must renew the lease before the grace
period expires, otherwise the lease becomes available again.

```rust,no_run
// OwnerLease<T: Copy + 'static> - T is the lease payload.
pub fn create(path: impl AsRef<Path>, initial: T) -> Result<Self, LeaseError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, LeaseError>;

pub fn try_acquire(&self, my_pid: u32, grace_epochs: u64) -> bool;
pub fn beat(&self, my_pid: u32) -> bool;           // renew the lease (heartbeat)
pub fn release(&self, my_pid: u32) -> bool;
pub fn read_as_owner(&self, my_pid: u32) -> Option<T>;
pub fn write_as_owner(&self, my_pid: u32, value: T) -> bool;
pub fn current_owner(&self) -> Option<u32>;        // owner pid, or None
pub fn am_i_owner(&self, my_pid: u32) -> bool;
pub fn tick_epoch(&self) -> u64;
```

`PAYLOAD_BYTES = 48` caps the payload type `T`. `NO_OWNER` (pid 0) is
the sentinel for "unowned". The grace period is measured in epochs,
not wall time, which makes the lease behaviour deterministic across
processes that disagree on wall clock; the owner renews by calling
`beat` before `grace_epochs` elapse.

Used to coordinate exclusive access to an external resource
(a database write lock, a file handle, a network port) when the
owner process might crash without releasing - the grace period
gives a bounded window before the lease becomes available again.

Op kinds use the `ownership` module: `OP_ACQUIRE = 1`,
`OP_RELEASE = 2`, `OP_GET = 3`, `OP_BEAT = 4`, `OP_CLAIM = 5`.

Canonical doc:
[OWNER_LEASE.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/OWNER_LEASE.md).

## `SharedLeaderElection`

Single-leader election. One process at a time is the elected
leader; the election protocol uses CAS on a leader-id field plus
a heartbeat-driven grace period (same pattern as `OwnerLease`,
specialised for the leader-vs-followers shape).

```rust,no_run
pub fn create(path: impl AsRef<Path>) -> Result<Self, LeaderError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, LeaderError>;

pub fn try_claim_leadership(&self, my_pid: u32, grace_epochs: u64) -> bool;
pub fn beat_as_leader(&self, my_pid: u32) -> bool;   // renew leadership
pub fn step_down(&self, my_pid: u32) -> bool;
pub fn current_leader(&self) -> Option<u32>;         // leader pid, or None
pub fn am_i_leader(&self, my_pid: u32) -> bool;
```

The difference from `OwnerLease`: the leader has implicit
authority over the application's collective state, not just over
the lease's payload. Use cases include leader-driven background
work (only one process runs the periodic compaction), exclusive
write paths in multi-process services, and primary-secondary
failover.

`DEFAULT_GRACE_EPOCHS` (= 3) is the default expiration window
in heartbeat epochs.

Canonical doc:
[SHARED_LEADER_ELECTION.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LEADER_ELECTION.md).

## `LazyConfig`

Cross-process lazy configuration loader. One process fetches the
config (from a config server, a file, an environment variable);
other processes see the fetched value once it lands. The
load happens at most once per epoch, even if many processes call
`get` simultaneously.

```rust,no_run
// LazyConfig<T: Copy + Send + Sync + 'static>
pub fn create(path: impl AsRef<Path>) -> Result<Self, LazyConfigError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, LazyConfigError>;

pub fn get_or_fetch<F: FnOnce() -> T>(&self, fetcher: F) -> T;
pub fn try_get(&self) -> Option<T>;
pub fn is_loaded(&self) -> bool;
pub fn force_set(&self, value: T) -> bool;
```

The state machine resembles `SharedOnceCell`'s: `get_or_fetch` runs
`fetcher` once and caches the result so concurrent callers across
processes share a single load, `try_get` returns the cached value
without loading, and `force_set` overwrites the cached value directly
(for a controlling process pushing a config bump).

Op kinds use the `lazy_config` module: `OP_GET = 1`,
`OP_FETCH = 2`.

Canonical doc:
[LAZY_CONFIG.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/LAZY_CONFIG.md).

## Picking between them

| Need | Primitive |
|---|---|
| Time-bounded exclusive resource access | `OwnerLease` |
| Single-leader-many-followers role assignment | `SharedLeaderElection` |
| At-most-once cross-process lazy initialisation | `LazyConfig` |
| Multi-reader exclusive write lock (no leader semantics) | `SharedRWLock` (see [shared-locks.md](shared-locks.md)) |

## See also

- [Coordination primitives](coordination.md) - heartbeats and
  failover that complement these ownership shapes.
- [Role-pair selection](../../how-to/role-pair-selection.md) -
  the owner/lease-holder shape these primitives fit.
