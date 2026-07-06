---
title: "Async SPSC Ring"
weight: 21
---

# AsyncSpscRing

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Shape](https://img.shields.io/badge/shape-Future_adapter-brightgreen)
![Runtime](https://img.shields.io/badge/runtime-executor--agnostic-informational)

`Future`-shaped async adapter on top of
[`BlockingSpscRing`]({{< ref "blocking-spsc-ring" >}}). Turns the
synchronous `send_blocking` / `recv_blocking` API into
`send(...).await` / `recv(...).await` so SubEtha rings compose
with any async executor (tokio, smol, async-std, custom).

> [!NOTE]
> **The high-level channel does async without a thread per future.**
> `Channel<T>` and `AdaptiveIpc<T>` answer `recv_async().await` /
> `send_async().await` directly: the producer fires the awaiting task's
> `Waker` in-process, and a single per-process reactor bridges the
> shared-memory wake to a local `Waker` across the boundary. Reach for
> the adapter on this page when you specifically want a `Future` wrap
> around an existing `BlockingSpscRing`.

> **The "kernel-park to Rust Waker bridge" primitive.** First
> poll calls `try_*` on the inner ring; if immediately ready,
> return `Poll::Ready`. Otherwise spawn a `std::thread` that
> calls the blocking counterpart (`recv_blocking` /
> `send_blocking`) with the caller's timeout; park the Rust
> `Waker`. When the blocking call returns, store the result and
> fire the `Waker`. Next poll observes the stored result and
> returns `Poll::Ready`.

## Constraints

- **Bounded timeout is required** (`Duration`, not
  `Option<Duration>`). Dropping a pending future does NOT cancel
  the spawned worker thread (`std::thread` lacks safe
  cancellation); the worker's worst-case lifetime equals the
  caller-supplied timeout.
- **One OS thread per in-flight future**. Fits the substrate's
  intended use of async (a small number of long-running consumer
  tasks per process, not thousands of short-lived futures).
  High-concurrency callers batch through one `BlockingSpscRing`
  per consumer task and call `recv_blocking` directly inside
  `tokio::task::spawn_blocking`.
- **Executor-agnostic**: depends only on
  `std::future::Future` + `std::thread`; works on tokio, smol,
  async-std, or any custom executor.

## Operations

```rust
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::{AsyncSpscRing, BlockingSpscRing};

impl AsyncSpscRing {
    pub fn new(inner: Arc<BlockingSpscRing>) -> Self;
    pub fn recv(&self, timeout: Duration) -> AsyncRecv;
    pub fn send(&self, payload: Vec<u8>, timeout: Duration) -> AsyncSend;
    pub fn inner(&self) -> &Arc<BlockingSpscRing>;
}
```

`AsyncRecv` resolves to `Result<Vec<u8>, BlockingError>` (bytes
popped + length-truncated, or a `BlockingError::Timeout` after
the duration elapses).

`AsyncSend` resolves to `Result<(), BlockingError>` (success or
timeout).

## Worked example

```rust
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::{AsyncSpscRing, BlockingSpscRing};

let ring = Arc::new(BlockingSpscRing::create_anon(64).unwrap());
let adapter = AsyncSpscRing::new(Arc::clone(&ring));

async fn drain(adapter: AsyncSpscRing) -> u64 {
    let mut total = 0u64;
    while let Ok(bytes) = adapter.recv(Duration::from_secs(1)).await {
        let v = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        total += v;
    }
    total
}
```

## E2E proof

`examples/async_ring_demo.rs` ships 5000 items end-to-end with a
producer task pushing via `send().await` and a consumer task
draining via `recv().await`; uses a hand-rolled executor-agnostic
`block_on` so no tokio dependency is required to demonstrate the
unlock. The captured run observes 624 consumer-side
`Poll::Pending` returns (12.5% of recvs) - proving the wake
bridge actually fires the Rust waker instead of the future
returning `Ready` on first poll.
FIFO integrity asserted; producer never parked because the ring
capacity (16) was always ahead of the consumer.

## See also

- Source: `crates/subetha-cxc/src/async_ring.rs` (340 lines, 4
  unit tests: recv-ready-immediately, recv-parks-then-completes,
  recv-times-out, send-completes-when-not-full; driven by a
  hand-rolled executor-agnostic `block_on`).
- [`BlockingSpscRing`]({{< ref "blocking-spsc-ring" >}}): the
  synchronous primitive this wraps.
- [`CrossProcessWaker`]({{< ref "../coordination-types/cross-process-waker" >}}):
  the wake substrate the worker thread parks on.
