---
title: "QUIC Bridge"
weight: 61
---

# QuicBridgeClient + QuicBridgeServer

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Feature](https://img.shields.io/badge/feature-quic--bridge-blueviolet)
![Transport](https://img.shields.io/badge/transport-QUIC%20%2F%20TLS-green)

Cross-host substrate primitive that ferries bytes between two
[`AdaptiveRing`](../../rings/shared-ring-adaptive/) instances via
QUIC streams. Encrypts the wire (TLS via rustls), multiplexes
streams within one connection, manages congestion control.

Gated behind the `quic-bridge` Cargo feature. Enabling pulls
quinn + rcgen + rustls + tokio as regular dependencies.

## Data path: burst-batched egress, chunked ingress

A per-slot stream write (one `write_all` await per 64-byte item)
serializes the bridge on reactor latency - microseconds per item
regardless of wire speed - so the client BURST-DRAINS the ring:
every already-available slot (up to `EGRESS_BATCH_SLOTS = 256`,
16 KiB) is copied into one contiguous buffer and handed to quinn
in a single write. The 64-byte memcpy per slot is noise next to
the TLS record processing the bytes pay anyway (quinn copies into
its send queue and encrypts in user space; no zero-copy egress
exists through an encrypting transport). A lone item still ships
immediately - batching never waits for items that have not
arrived, so request/response traffic is not penalized.

The server mirrors this with chunked stream reads: each read takes
whatever the stream has buffered, complete 64-byte slots push into
the consumer ring as they assemble, and a partial slot carries to
the next read.

## Frame format

Each connection carries one uni-directional stream. The stream
starts with an 8-byte big-endian item count `N`, followed by
`N * ADAPTIVE_SPSC_PAYLOAD_BYTES` (64-byte) slot payloads.

## API

### QuicBridgeClient

| Call | Behavior |
|---|---|
| `QuicBridgeClient::new(producer_ring, server_addr, client_config, bind_addr)` | Construct. `producer_ring: Arc<AdaptiveRing>`. |
| `client.run(n_items, server_name) -> Result<(), QuicBridgeError>` | Connect, ship `n_items` slots, finish the stream. |

### QuicBridgeServer

| Call | Behavior |
|---|---|
| `QuicBridgeServer::bind(consumer_ring, addr, server_config)` | Bind. `consumer_ring: Arc<AdaptiveRing>`. |
| `server.local_addr() -> Result<SocketAddr, std::io::Error>` | Bound address (useful when `0.0.0.0:0` was passed). |
| `server.accept_one() -> Result<u64, QuicBridgeError>` | Accept one connection, drain its uni stream, return item count. |

### Helpers

| Helper | Behavior |
|---|---|
| `make_self_signed_pair(sni_name) -> Result<(ServerConfig, ClientConfig), QuicBridgeError>` | Build a single-host / demo TLS config pair in one process. |
| `generate_self_signed_cert(sni_name) -> Result<(Vec<u8>, Vec<u8>), QuicBridgeError>` | Cross-host building block: `(cert_der, pkcs8_key_der)` as raw bytes to ship between hosts. |
| `make_server_config_from_der(cert_der, key_der)` | Rebuild the server config from shipped DER bytes. |
| `make_client_config_from_der(cert_der)` | Client config trusting exactly that cert; pass the SNI string the cert names to `connect`. |
| `install_default_crypto_provider()` | Idempotent rustls ring backend install for binary entrypoints. |

## Error type

`QuicBridgeError`:
- `Tls(String)` - rcgen / rustls setup failed.
- `Quic(String)` - QUIC connection or stream error.
- `Io(std::io::Error)` - stdlib I/O error binding the endpoint.

## E2E proof

[`examples/quic_bridge_e2e.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/quic_bridge_e2e.rs)
runs producer-side AdaptiveRing -> QuicBridgeClient -> QUIC ->
QuicBridgeServer -> consumer-side AdaptiveRing on 127.0.0.1
(100,000 items, integrity verified).

[`examples/bridge_lan.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
runs the same chain between two PHYSICAL hosts with the cert
shipped as DER bytes: 1,000,000 items each direction with strict
sequence assertions, plus a ping/pong round-trip mode. Measured
numbers live in
[`docs/LAN_BRIDGE_PERFORMANCE.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/LAN_BRIDGE_PERFORMANCE.md).

## References

- [`AdaptiveRing`](../../rings/shared-ring-adaptive/) - the
  producer/consumer ring type.
- [`TcpBridge`](../tcp-bridge/) - plain-TCP sibling for trusted
  networks where TLS overhead is wasted.
