---
title: "Shared Condvar"
weight: 55
---

# SharedCondvar

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Pattern](https://img.shields.io/badge/pattern-Mesa_condvar-brightgreen)

Cross-process Mesa-style condition variable built on top of
[`CrossProcessWaker`]({{< ref "cross-process-waker" >}}). Waiters
check a user-owned predicate, park if not satisfied, and resume
when a notifier advances the predicate AND calls `notify_*`.

> **The "Mesa condvar over a futex slot" primitive.** A monotonic
> generation counter lives in shared memory; every `wait` parks at
> `target = current_gen + 1`; every `notify_one` / `notify_all`
> bumps the generation and fires `wake_(one_)up_to(new_gen)`,
> which wakes parked waiters whose `target <= new_gen`. The
> condvar does NOT own the predicate atom; callers pass a closure
> that returns the current predicate value.

## Constraints

- **`Arc::clone` for intra-process sharing**, NOT `create` +
  `open`. The `open` constructor mmaps the same file a second
  time, producing a different virtual-address range aliased to
  the same file pages; Windows `WaitOnAddress` is keyed by
  virtual address, so a `notify` from the second handle does NOT
  reach a `wait` on the first handle. Cross-process Linux works
  via SHARED `futex`, but the rule "one `Arc<SharedCondvar>` per
  process" is cross-platform safe.
- **`open` is for SEPARATE processes** joining a condvar the
  creator already initialised.
- **Cross-process wake** rides the
  [`CrossProcessWaker`]({{< ref "cross-process-waker" >}}) parks:
  SHARED `futex` on Linux/WSL, non-PRIVATE `_umtx_op` on FreeBSD,
  `os_sync_wait_on_address` on macOS 14.4+, and the hardware
  monitor tier (`MONITORX` / `UMONITOR`, physical-address keyed)
  on Windows for file/shm-backed condvars; anon-backed Windows
  condvars stay intra-process via `WaitOnAddress`.

## Operations

```rust
use std::path::Path;
use std::time::Duration;
use subetha_cxc::SharedCondvar;

impl SharedCondvar {
    pub fn create_anon() -> Result<Self, CondvarError>;
    pub fn create_anon_with_capacity(max_waiters: usize) -> Result<Self, CondvarError>;
    pub fn create(base: impl AsRef<Path>) -> Result<Self, CondvarError>;
    pub fn create_with_capacity(base: impl AsRef<Path>, max_waiters: usize) -> Result<Self, CondvarError>;
    pub fn open(base: impl AsRef<Path>) -> Result<Self, CondvarError>;
    pub fn open_with_capacity(base: impl AsRef<Path>, expected_max_waiters: usize) -> Result<Self, CondvarError>;

    pub fn wait<F: FnMut() -> bool>(&self, predicate: F) -> Result<(), CondvarError>;
    pub fn wait_timeout<F: FnMut() -> bool>(
        &self, predicate: F, timeout: Duration,
    ) -> Result<(), CondvarError>;

    pub fn notify_one(&self) -> usize;              // wake_one_up_to(new_gen)
    pub fn notify_all(&self) -> usize;              // wake_up_to(new_gen)
    pub fn generation(&self) -> u64;
    pub fn waker(&self) -> &Arc<CrossProcessWaker>; // peek wake state directly
}
```

The default constructors use `MAX_WAITERS_DEFAULT` (32) waker slots; the
`*_with_capacity` variants override it. `CondvarError` has four variants:
`WakerFull` (all waker slots in use - fall back to spinning the predicate),
`Timeout` (a `wait_timeout` deadline elapsed with the predicate still false),
`LayoutMismatch` (a backing file's magic / size disagrees on `open`), and
`Io(std::io::ErrorKind)`.

File-backed mode lays out two files under one base path:
- `<base>.waker.bin` for the underlying waker slots.
- `<base>.gen.bin` for the magic + generation counter.

## Worked example

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use subetha_cxc::SharedCondvar;

let cv = Arc::new(SharedCondvar::create_anon()?);
let pred = Arc::new(AtomicBool::new(false));

let cv2 = Arc::clone(&cv);
let pred2 = Arc::clone(&pred);
let waiter = thread::spawn(move || {
    cv2.wait(|| pred2.load(Ordering::Acquire)).unwrap();
});

thread::sleep(Duration::from_millis(20));
pred.store(true, Ordering::Release);
cv.notify_all();
waiter.join().unwrap();
```

## E2E proof

- Cross-process WSL Linux pair
  (`examples/condvar_xproc_notifier.rs` +
  `examples/condvar_xproc_waiter.rs`) drives a 255ms cross-process
  wait that crosses the kernel boundary via SHARED `futex`;
  notifier wakes 1 parked waiter; both processes exit `rc=0`.
- 5 lib tests cover notify_one, notify_all, wait_timeout,
  immediate-true predicate, and Arc::clone file-backed
  round-trip.

## See also

- [`CrossProcessWaker`]({{< ref "cross-process-waker" >}}): the
  underlying wake / park primitive.
- [`BlockingSemaphore`]({{< ref "blocking-semaphore" >}}) /
  [`BlockingRWLock`]({{< ref "blocking-rw-lock" >}}): siblings
  that use the same waker substrate.
