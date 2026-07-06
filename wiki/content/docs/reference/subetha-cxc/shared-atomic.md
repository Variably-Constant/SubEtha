---
weight: 30
---

# Atomics

Three cross-process atomic word primitives. Each is a single
`AtomicU32` / `AtomicU64` / `AtomicBool` inside an MMF region;
two processes mapping the same file see the same atomic.

The architectural lever is that hardware atomics work across
process boundaries when the cache line is shared via the OS
page cache. Two threads in different processes can CAS the same
word and the cache-coherence protocol resolves the race
identically to two threads in one process.

## `SharedAtomicU32`

```rust,no_run
pub fn create(path: impl AsRef<Path>, init: u32) -> Result<Self, SharedAtomicError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedAtomicError>;

pub fn load(&self, ord: Ordering) -> u32;
pub fn store(&self, v: u32, ord: Ordering);
pub fn fetch_add(&self, v: u32, ord: Ordering) -> u32;
pub fn fetch_sub(&self, v: u32, ord: Ordering) -> u32;
pub fn swap(&self, v: u32, ord: Ordering) -> u32;
pub fn compare_exchange(
    &self, current: u32, new: u32,
    success: Ordering, failure: Ordering,
) -> Result<u32, u32>;
```

The API mirrors `std::sync::atomic::AtomicU32` directly, including the
explicit `Ordering` argument on every operation. The cross-process
guarantee comes from the shared cache line - the atomic lives in the
MMF, so the same memory-model semantics that hold between two threads
hold between two processes - not from forcing a fixed ordering.

## `SharedAtomicU64`

Same surface as `SharedAtomicU32` with a 64-bit value type. Used
for counters, generation numbers, and packed `(slot_idx, version)`
tuples for ABA prevention.

## `SharedAtomicBool`

Same surface restricted to boolean values. Useful for shared
flags that two processes both poll - "is shutdown requested",
"is the leader election complete", "is migration in flight".

## Shared op kinds

All three primitives use the same `atomic` module:

```rust,no_run
pub mod atomic {
    pub const OP_LOAD: u16 = 1;
    pub const OP_STORE: u16 = 2;
    pub const OP_FETCH_ADD: u16 = 3;
    pub const OP_CAS: u16 = 4;
}
```

## When to use these vs `SharedRing` or `SharedHashMap`

The atomics carry a single word. If your shared state is more
than one value, the hash map and ring primitives are the right
shape. A common pattern uses these as a cheap shared counter
inside a larger primitive (e.g., as the version field of a
`SharedHashMap` slot's SeqLock protocol).

## See also

- [`SharedCell`](shared-cell.md) - the byte-wider analogue with
  a fixed-size payload.
- [`SharedFenceClock`](shared-locks.md#sharedfenceclock) - hybrid
  logical clock built on shared atomics.
- Canonical doc:
  [crates/subetha-cxc/docs/pointers/SHARED_ATOMIC.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ATOMIC.md).
