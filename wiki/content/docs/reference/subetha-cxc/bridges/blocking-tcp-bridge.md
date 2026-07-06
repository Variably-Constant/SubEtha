---
title: "Blocking TCP Bridge"
weight: 65
---

# BlockingTcpBridge

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Feature](https://img.shields.io/badge/cargo--feature-tcp--bridge-success)
![Wakers](https://img.shields.io/badge/wakers-cross--process_futex-brightgreen)
![Idle](https://img.shields.io/badge/idle-zero_CPU-informational)

TCP forwarder pair whose producer-side and consumer-side use
`recv_blocking` / `send_blocking` on a
[`BlockingSpscRing`]({{< ref "../rings/blocking-spsc-ring" >}}) via
`tokio::task::spawn_blocking`, so neither half burns scheduler
slices polling an empty / full ring.

> **The "QUIC-shape latency floor" primitive.** Where the existing
> [`TcpBridge`]({{< ref "tcp-bridge" >}}) calls
> `tokio::task::yield_now` when the local ring is empty (client
> side) or full (server side), this primitive's worker thread
> parks on a SHARED `futex` (Linux) or `WaitOnAddress` (Windows
> intra-process) and returns within microseconds of the next ring
> event. End-to-end latency floor drops from "polling interval +
> RTT" to "wake syscall + RTT".

## Constraints

- **Cargo feature `tcp-bridge`** (same gate as the original
  `TcpBridge`).
- **`Arc<BlockingSpscRing>`** on both halves (single-producer,
  single-consumer at each ring). Multi-producer / multi-consumer
  shapes use the
  [`BlockingMpscRing`]({{< ref "../rings/blocking-mpsc-ring" >}})
  /
  [`BlockingMpmcRing`]({{< ref "../rings/blocking-mpmc-ring" >}})
  primitives directly; bridge support for those shapes follows
  the same `spawn_blocking` pattern.
- **Wire-side slot width matches `SPSC_PAYLOAD_BYTES`** (64
  bytes). The wire carries whole slots back-to-back.
- **Burst-batched data path.** The client parks for the FIRST item
  (the zero-CPU-idle property), then drains every slot already in
  the ring via `try_pop` (up to `EGRESS_BATCH_SLOTS = 256`) and
  ships the batch in one socket write. The server's chunked reads
  push complete slots via the non-blocking `try_push` fast path,
  parking on `send_blocking` only when the ring is full.
  `TCP_NODELAY` is set on both ends.

## Operations

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use subetha_cxc::BlockingSpscRing;
use subetha_cxc::blocking_tcp_bridge::{
    BlockingTcpBridgeClient, BlockingTcpBridgeServer, BlockingTcpBridgeError,
};

impl BlockingTcpBridgeClient {
    pub fn new(producer_ring: Arc<BlockingSpscRing>, server_addr: SocketAddr) -> Self;
    pub async fn run(&self, n_items: u64) -> Result<(), BlockingTcpBridgeError>;
}

impl BlockingTcpBridgeServer {
    pub async fn bind(
        consumer_ring: Arc<BlockingSpscRing>,
        addr: SocketAddr,
    ) -> Result<Self, BlockingTcpBridgeError>;
    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error>;
    pub async fn accept_one(&self) -> Result<u64, BlockingTcpBridgeError>;
}
```

The internal `TICK_TIMEOUT` (5s) bounds the worker-thread
lifetime so a hung peer cannot strand the bridge indefinitely;
the bridge loops on Timeout internally and re-tries until the
caller's `n_items` budget is satisfied.

## Worked example

```rust
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use subetha_cxc::BlockingSpscRing;
use subetha_cxc::blocking_tcp_bridge::{
    BlockingTcpBridgeClient, BlockingTcpBridgeServer,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let client_ring = Arc::new(BlockingSpscRing::create_anon(64).unwrap());
    let server_ring = Arc::new(BlockingSpscRing::create_anon(64).unwrap());

    let server = BlockingTcpBridgeServer::bind(
        Arc::clone(&server_ring),
        SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
    ).await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.accept_one().await.unwrap() });

    // Producer thread pushes into client_ring.
    let prod_ring = Arc::clone(&client_ring);
    let prod = std::thread::spawn(move || {
        for i in 0..5_000u64 {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            prod_ring.send_blocking(&payload, Some(std::time::Duration::from_secs(5))).unwrap();
        }
    });

    // Bridge client task drains client_ring + ships over TCP.
    let bc_ring = Arc::clone(&client_ring);
    let bridge_client = tokio::spawn(async move {
        BlockingTcpBridgeClient::new(bc_ring, server_addr).run(5_000).await.unwrap();
    });

    // Consumer thread reads from server_ring.
    let cons_ring = Arc::clone(&server_ring);
    let cons = std::thread::spawn(move || {
        let mut buf = [0u8; 64];
        for _ in 0..5_000 {
            cons_ring.recv_blocking(&mut buf, Some(std::time::Duration::from_secs(5))).unwrap();
        }
    });

    bridge_client.await.unwrap();
    server_task.await.unwrap();
    prod.join().unwrap();
    cons.join().unwrap();
}
```

## E2E proof

`examples/blocking_tcp_bridge_e2e.rs` ships 5000 items end-to-end
across producer thread -> `BlockingSpscRing` -> bridge client
task (parked recv + burst drain) -> TCP -> bridge server task
(chunked reads + try_push/parked send) -> `BlockingSpscRing` ->
consumer thread on localhost loopback, FIFO integrity asserted at
the consumer end; build + run with
`cargo run --release --example blocking_tcp_bridge_e2e --features tcp-bridge`.

[`examples/bridge_lan.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
runs the same chain between two PHYSICAL hosts: 1,000,000 items
each direction with strict sequence assertions, plus a ping/pong
round-trip mode. Measured numbers live in
[`docs/LAN_BRIDGE_PERFORMANCE.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/LAN_BRIDGE_PERFORMANCE.md).

## See also

- [`TcpBridge`]({{< ref "tcp-bridge" >}}): original polling-based
  bridge over `AdaptiveRing`. Both bridges share wire-format with
  `n_items` header + payload-per-slot frames.
- [`BlockingSpscRing`]({{< ref "../rings/blocking-spsc-ring" >}}):
  the underlying ring primitive both halves consume.
- [`CrossProcessWaker`]({{< ref "../coordination-types/cross-process-waker" >}}):
  the substrate the kernel-park slow path uses.
