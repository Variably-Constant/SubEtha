---
title: "TCP Bridge"
weight: 62
---

# TcpBridgeClient + TcpBridgeServer

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Feature](https://img.shields.io/badge/feature-tcp--bridge-blueviolet)
![Transport](https://img.shields.io/badge/transport-plain%20TCP-green)

Cross-host substrate primitive that ferries bytes between two
[`AdaptiveRing`](../../rings/shared-ring-adaptive/) instances over
plain TCP. Sibling to [`QuicBridge`](../quic-bridge/) for
trusted-network deployments where TLS overhead is wasted.

Gated behind the `tcp-bridge` Cargo feature. Enabling pulls
tokio (net + io-util + rt-multi-thread + macros).

## Data path: burst-batched egress, chunked ingress

Same discipline as `QuicBridge`: the client burst-drains every
already-available ring slot (up to `EGRESS_BATCH_SLOTS = 256`,
16 KiB) into one buffer per socket write, and the server's chunked
reads push complete slots as they assemble with a partial-slot
carry. A lone item ships immediately. `TCP_NODELAY` is set on both
ends so a single latency-sensitive slot is never parked on Nagle's
timer; under a saturating stream the batched writes keep segments
MSS-filled regardless.

## Frame format

8-byte big-endian item count `N` followed by
`N * ADAPTIVE_SPSC_PAYLOAD_BYTES` (64-byte) slot payloads.

## API

### TcpBridgeClient

| Call | Behavior |
|---|---|
| `TcpBridgeClient::new(producer_ring, server_addr)` | Construct. `producer_ring: Arc<AdaptiveRing>`. |
| `client.run(n_items) -> Result<(), TcpBridgeError>` | Connect, ship `n_items` slots, shutdown the stream. |

### TcpBridgeServer

| Call | Behavior |
|---|---|
| `TcpBridgeServer::bind(consumer_ring, addr) -> Result<Self, TcpBridgeError>` | Async constructor; binds the TCP listener. |
| `server.local_addr() -> Result<SocketAddr, std::io::Error>` | Bound address. |
| `server.accept_one() -> Result<u64, TcpBridgeError>` | Accept one connection, drain, return item count. |

## Error type

`TcpBridgeError`:
- `Io(std::io::Error)` - stdlib I/O error binding or reading.
- `Closed` - connection closed unexpectedly.

## E2E proof

[`examples/tcp_bridge_e2e.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/tcp_bridge_e2e.rs)
runs producer AdaptiveRing -> TcpBridgeClient -> TCP ->
TcpBridgeServer -> consumer AdaptiveRing on 127.0.0.1
(100,000 items, integrity verified).

[`examples/bridge_lan.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
runs the same chain between two PHYSICAL hosts: 1,000,000 items
each direction with strict sequence assertions, plus a ping/pong
round-trip mode. Measured numbers live in
[`docs/LAN_BRIDGE_PERFORMANCE.md`](https://github.com/Variably-Constant/SubEtha/blob/main/docs/LAN_BRIDGE_PERFORMANCE.md).

## TLS variant

`TcpTlsBridgeClient` / `TcpTlsBridgeServer` (`tcp-tls-bridge` feature) are
the encrypted-TCP option: identical framing, batching, and `TCP_NODELAY` to
the plain bridge, with the bytes carried inside a rustls **TLS 1.3** record
layer (the same `ServerConfig` / `ClientConfig` the QUIC bridge uses). The
only wire delta is the AEAD record; on a clean link the throughput ties the
plain `TcpBridge` (the TLS cost is negligible). Like any TCP transport it
collapses under loss - a lost segment head-of-line-blocks the stream until
its retransmit lands - so reach for [Sens-O-Matic](../sens-rlc/) on a lossy
path.

## References

- [`QuicBridge`](../quic-bridge/) - encrypted + multiplexed
  sibling for untrusted networks.
- [`AdaptiveRing`](../../rings/shared-ring-adaptive/) - the
  producer/consumer ring type.
