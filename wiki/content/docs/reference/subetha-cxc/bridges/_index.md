---
title: "Bridges"
weight: 60
---

# Cross-host bridge primitives

Substrate primitives that ferry bytes between two
[`AdaptiveRing`](../rings/shared-ring-adaptive/) instances on
different hosts. Most bridges are a typed client + server pair
gated behind a Cargo feature so callers compile only the bridges
they use; the reliable-UDP transport is `std`-only with a sender /
receiver pair.

Sens-O-Matic is the reliable FEC-UDP **protocol**, and its erasure code is a
swappable detail (like a cipher suite); a unified endpoint also switches between
the codes mid-stream on measured loss. It appears below once per code (block
Reed-Solomon and sliding-window RLC) plus the unified auto-switch.

**For an untrusted or lossy real-world WAN, the [unified Sens-O-Matic
endpoint](unified-code-switch/) is the default choice** - it is encrypted
(TLS 1.3 on both codes) and its adaptive FEC holds throughput and a bounded
latency tail across the whole loss range, where the stream bridges below
degrade under loss. Reach for [QuicBridge](quic-bridge/) when you want QUIC's
stream multiplexing / migration / 0-RTT, and the TCP bridges on a trusted link.
(The standalone `RS` row is `std`-only and unencrypted; TLS on the RS stream
comes via the unified endpoint, which AEAD-seals both codes.)

| Bridge | Transport | Cargo feature | Encryption | Ring type | Idle behavior |
|---|---|---|---|---|---|
| [QuicBridge](quic-bridge/) | QUIC over UDP | `quic-bridge` | TLS 1.3 via rustls | `AdaptiveRing` | `tokio::task::yield_now` on empty/full |
| [TcpTlsBridge](tcp-bridge/#tls-variant) | TCP + rustls record | `tcp-tls-bridge` | TLS 1.3 via rustls | `AdaptiveRing` | `tokio::task::yield_now` on empty/full |
| [TcpBridge](tcp-bridge/) | Plain TCP | `tcp-bridge` | None | `AdaptiveRing` | `tokio::task::yield_now` on empty/full |
| [BlockingTcpBridge](blocking-tcp-bridge/) | Plain TCP | `tcp-bridge` | None | `BlockingSpscRing` | Kernel-park via cross-process waker (zero CPU at idle) |
| [Sens-O-Matic / RS](reliable-udp-bridge/) | Reliable UDP, block Reed-Solomon | none (`std`) | None | item-level sender / receiver | Receiver parks on read timeout; sender non-blocking |
| [Sens-O-Matic / RLC](sens-rlc/) | Reliable UDP, sliding-window RLC | none / `tls` | Optional TLS 1.3 | item-level sender / receiver | Poll-driven sender + receiver |
| [Sens-O-Matic / unified switch](unified-code-switch/) | Reliable UDP, RLC<->RS auto-switch | none / `tls` | Optional TLS 1.3 | item-level sender / receiver | Poll-driven sender + receiver |

The original bridges accept `Arc<AdaptiveRing>` as their producer
(client side) / consumer (server side) ring; the substrate's
default-facing ring type composes through unchanged, and the
bridges ride the shape-axis morph through `AdaptiveRing`
automatically. `BlockingTcpBridge` is the sibling whose forwarder
calls `recv_blocking` / `send_blocking` on a `BlockingSpscRing`
via `tokio::task::spawn_blocking`; the worker thread parks
kernel-side instead of yielding the runtime slice, so an idle
bridge consumes zero CPU and a freshly-published item ships
across the wire one wake + socket-write later.
