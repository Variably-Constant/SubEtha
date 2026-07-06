---
title: "Blocking SPSC Ring"
weight: 18
---

# BlockingSpscRing

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Wakers](https://img.shields.io/badge/wakers-cross--process_futex-brightgreen)
![Contract](https://img.shields.io/badge/contract-compile--time_SPSC-success)
![Slot](https://img.shields.io/badge/slot-64B_payload--only-informational)

Single-producer / single-consumer ring with cross-process
futex-shaped `send_blocking` / `recv_blocking`. Wraps the
[`SpscRingCore`]({{< ref "shared-ring-spsc" >}}) layout plus two
[`CrossProcessWaker`]({{< ref
"../coordination-types/cross-process-waker" >}}) instances (one
for each side). The hot path (`try_push` / `try_pop`) is
identical to the bare SPSC ring; the blocking calls add a
pre-park spin and a kernel park backed by SHARED `futex` on
Linux, `WaitOnAddress` on Windows.

> **The "SPSC + futex slot" primitive.** Producer's `try_push`
> wakes the consumer-side waker after every successful publish.
> Consumer's `try_pop` wakes the producer-side waker after every
> successful drain. On ring empty / ring full, the blocking
> call parks the caller until the counterparty's wake fires
> or the timeout elapses.

## Constraints

- **Single producer, single consumer**, enforced at compile time
  by the inner `SpscRingCore` not being `Sync` and the wrapper
  holding it through `Arc<SpscRingCore>`.
- **Payload up to `SPSC_PAYLOAD_BYTES = 64` bytes per slot**;
  shorter pushes zero-fill the slot tail. Pops require an
  out-buffer of at least 64 bytes and always return the full 64
  (the slot stores no length prefix - framing is the payload
  format's job).
- **Capacity must be a power of 2**.
- **In-process anonymous** (`create_anon`) or **cross-process
  file-backed** (`create` / `open`).

## Operations

```rust
use std::path::Path;
use std::time::Duration;
use subetha_cxc::{BlockingSpscRing, BlockingError};

impl BlockingSpscRing {
    pub fn create_anon(capacity: usize) -> Result<Self, BlockingError>;
    pub fn create(base_path: impl AsRef<Path>, capacity: usize) -> Result<Self, BlockingError>;
    pub fn open(base_path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, BlockingError>;

    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError>;
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError>;

    pub fn send_blocking(&self, payload: &[u8], timeout: Option<Duration>) -> Result<(), BlockingError>;
    pub fn recv_blocking(&self, out: &mut [u8], timeout: Option<Duration>) -> Result<usize, BlockingError>;
}
```

File-backed mode lays out three files under one base path:
- `<base>.ring.bin` for the SPSC ring
- `<base>.cw.bin` for the consumer-side waker
- `<base>.pw.bin` for the producer-side waker

`open` reopens all three with the same expected capacity.

## Blocking protocol

`recv_blocking(timeout)` loop:

1. Try the non-blocking pop.
2. On `Empty`, spin through `try_pop` 32 times before parking
   (cheap for imminent items, skips the kernel round-trip).
3. Still empty: reserve a consumer-waker slot with
   `target = head + 1`.
4. Re-check `try_pop` after parking (closes the wake-before-park
   race; the producer's wake call to `wake_up_to(head)` reaches
   the slot only if it was already PARKED).
5. Call `wait(token, remaining_timeout)` on the waker. On wake,
   loop back to step 1. On timeout, return `BlockingError::Timeout`.

`send_blocking` is the mirror image, parking on the producer-side
waker with `target = tail + 1`.

## Phase-locked predictive waiting (opt-in)

Beyond the bare doorbell park, the consumer can *predict* the next arrival
and spin a short guard band instead of paying the park/wake round-trip. This
is **off by default**: the cross-process doorbell is already ~400-500 ns, and
predictive waiting is a LOSS there (worse p99 from prediction jitter). It
wins only for an **in-process** consumer whose producer contends for cores
(where thread-scheduling inflates the doorbell to ~10 us). Correctness
(exactly-once, FIFO) is identical in every mode.

Two ways to use it:

- **Automatic**, toggled on the ring: `set_phase_locking(true)` makes
  `recv_blocking` run the predictor. A consumer-local estimator engages only
  after `PHASE_MIN_SUSTAINED_WAITS` (8) consecutive empty-ring waits on a
  regular cadence, and leaves "wait mode" after `PHASE_EXIT_FAST_RUN` (64)
  fast catches (it has caught up). A wait-mode gate keeps the fast path at one
  relaxed atomic load when the predictor is idle. Observability accessors:
  `phase_locking_enabled()`, `phase_in_wait_mode()`, `phase_engaged()`, and
  the sticky `phase_predictive_catches()` (count of items caught by the
  guard-band spin - the syscall-free path).
- **Explicit**, caller-owned estimator: `recv_phase_locked(out, &mut
  PhaseEstimator, guard_band, timeout, &mut PhaseRecvStats)`. Pass the same
  estimator across calls so it accumulates cadence; `PhaseRecvStats` counts
  how each item was caught (`fast_catches`, `predictive_parks`,
  `spin_catches`, `doorbell_parks`, `doorbell_catches`). Engaged: park to
  `guard_band` before the predicted arrival, then spin the band; disengaged
  or a missed prediction falls through to the same doorbell park as
  `recv_blocking`.

## Accessors and errors

`inner()` exposes the underlying `&Arc<SpscRingCore>` for the non-blocking
surface; `consumer_waker()` / `producer_waker()` expose the two
`CrossProcessWaker`s for wake-count instrumentation. `BlockingError` is
`Ring(RingError)` / `WakerFull` (all waker slots in use - fall back to a
`try_*` spin) / `Timeout` / `WakerLayout` / `Io`.

## Worked example

```rust
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use subetha_cxc::BlockingSpscRing;

let ring = Arc::new(BlockingSpscRing::create_anon(64)?);
let r2 = Arc::clone(&ring);
let producer = thread::spawn(move || {
    for i in 0..10_000u64 {
        let mut payload = [0u8; 56];
        payload[..8].copy_from_slice(&i.to_le_bytes());
        r2.send_blocking(&payload, Some(Duration::from_secs(5))).unwrap();
    }
});
let r3 = Arc::clone(&ring);
let consumer = thread::spawn(move || {
    let mut buf = [0u8; 64];
    for expected in 0..10_000u64 {
        r3.recv_blocking(&mut buf, Some(Duration::from_secs(5))).unwrap();
        let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
        assert_eq!(got, expected);
    }
});
producer.join().unwrap();
consumer.join().unwrap();
```

For the cross-process shape, see `examples/waker_xproc_producer.rs`
+ `examples/waker_xproc_consumer.rs` (two separate binaries over a
file-backed MMF).

## E2E proof

- **Windows intra-process:** 50000 items in ~1.74s, 3124 consumer
  parks observed (~6.2% of recvs).
- **Linux/WSL cross-process** (two binaries via file-backed MMF):
  50000 items in ~0.55s, 288 to 323 cross-process parks per run
  (~0.6% of recvs). Both processes exit `rc=0` across the
  back-to-back sweep.

## See also

- [`CrossProcessWaker`]({{< ref "../coordination-types/cross-process-waker" >}}):
  the underlying wake / park primitive.
- [`SharedRingSpsc`]({{< ref "shared-ring-spsc" >}}): the
  non-blocking SPSC ring this wraps.
- [`BlockingMpscRing`]({{< ref "blocking-mpsc-ring" >}}) and
  [`BlockingMpmcRing`]({{< ref "blocking-mpmc-ring" >}}): the
  N-producer variants.
