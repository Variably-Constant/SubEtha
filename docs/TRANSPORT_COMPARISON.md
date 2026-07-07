# Transport Head-to-Head: stream bridges vs Sens-O-Matic

The cross-host transports measured in one harness
([`examples/bridge_lan.rs`](../crates/subetha-cxc/examples/bridge_lan.rs))
so they sit in one table. Two kinds:

- **Stream bridges** ferry batched 64-byte ring slots over a reliable
  stream: `TcpBridge` (plain TCP), `TcpTlsBridge` (TCP inside a rustls 1.3
  record layer), `QuicBridge` (QUIC, TLS 1.3), and `BlockingTcpBridge`
  (plain TCP, parked rings, zero idle CPU).
- **Sens-O-Matic** is the reliable FEC-UDP *protocol* - the QUIC peer of
  this set. Its erasure code is a swappable detail (like a cipher suite):
  the **block Reed-Solomon** code (MDS, fixed parity, `std`-only) or the
  **sliding-window RLC** code (adaptive, optionally wrapped in TLS 1.3).
  It ships MTU-sized items as forward-error-corrected datagrams.

The two codes are measured below forced on individually, the head-to-head
that isolates each one's characteristics. In production the `UnifiedSens*`
endpoint (`sens_unified`) carries both on one UDP port and auto-switches
between them on the loss the receiver measures - RLC below the ~15%
crossover for its lower latency tail, RS above it for throughput - and its
TLS 1.3 handshake seals every item across the switch, so RS is encrypted
too when it rides the unified endpoint. The standalone `Sens-O-Matic / RS`
row below is the code on its own, which is `std`-only.

A raw `udp` blast (no reliability, no congestion control) is the
unprotected-datagram reference. Application goodput and round-trip latency
are the common metrics, wire loss the dividing line. Every run asserts
strict sequence order, exact count, and payload sum (the `udp` blast
reports delivery ratio instead, since it drops).

| Transport | Kind | Loss recovery | Encryption |
|---|---|---|---|
| `TcpBridge` | batched TCP stream | kernel TCP retransmit (ARQ) | none |
| `TcpTlsBridge` | batched TCP stream | kernel TCP retransmit (ARQ) | TLS 1.3 (rustls) |
| `QuicBridge` | QUIC stream | quinn retransmit (ARQ) | TLS 1.3 (rustls) |
| `BlockingTcpBridge` | batched TCP, parked rings | kernel TCP retransmit (ARQ) | none |
| **Sens-O-Matic / RS** | reliable-UDP FEC datagrams | forward error correction (block Reed-Solomon), ARQ fallback | none (`std`-only) |
| **Sens-O-Matic / RLC** | reliable-UDP FEC datagrams | forward error correction (sliding-window RLC), ARQ fallback | optional TLS 1.3 |
| `udp` | raw datagram blast | none | none |

## Method

Real wire, two regimes, measured between separate OS processes - not
loopback.

- **LAN**: an Ubuntu 24.04 host (Linux 6.8) and a FreeBSD 15.0 host, each a
  VM on one Zen3 machine (Ryzen 7 5700G), communicating cross-OS over their
  virtio NICs. The transport builds and runs natively on both. The Ubuntu
  side is the sender; loss is `tc qdisc ... netem loss X%` on its egress to
  the FreeBSD peer.
- **WAN**: the same Ubuntu host to a remote datacenter VPS over the public
  internet (~22 ms RTT, no UDP rate policer on the VPS link). Secure
  transports only (`quic`, `TcpTlsBridge`, Sens-O-Matic / RLC + TLS), since
  these are what one runs over an untrusted path.

Sampling is **interleaved**: within each round every transport runs
back-to-back, so a burst of cross-traffic hits them all alike; ten rounds
per cell, reported as the **median with a bootstrap 95% confidence
interval**. Volumes are **byte-matched** to ~70 MB: the stream bridges ship
1,100,000 64-byte slots, Sens-O-Matic and `udp` ship 50,000 1408-byte
(one-MTU) items. Latency is the round-trip `ping`/`pong` mode at a 64-byte
request; it is measured on the LAN, where the under-loss *tail* - the
transport contrast - is identical in kind to the WAN, while the WAN's ~22 ms
base RTT is a common-mode offset that shifts every transport equally.

## Clean-link throughput (LAN, Mbit/s goodput, median [95% CI])

| Transport | goodput |
|---|---:|
| `udp` (blast) | 1821 [1638, 1862] - but only 36% delivered |
| `TcpTlsBridge` | 1382 [1270, 1512] |
| `TcpBridge` | 1375 [1267, 1461] |
| `BlockingTcpBridge` | 1228 [1197, 1271] |
| Sens-O-Matic / RLC (plain) | 898 [737, 934] |
| Sens-O-Matic / RLC (+TLS) | 885 [706, 958] |
| `QuicBridge` (TLS) | 831 [769, 898] |
| Sens-O-Matic / RS | 801 [406, 842] |

On a clean link the stream bridges lead on raw throughput: they ferry bytes
the kernel/quinn move without parity, while Sens-O-Matic spends part of the
link on FEC datagrams the streams do not carry. The `udp` blast tops the
table on wire rate but **delivers only 36% of what it sends** - with no flow
control it overruns the receiver's socket buffer; its number is a raw-ceiling
reference, not goodput. TLS costs little: `TcpTlsBridge` ties `TcpBridge`,
and Sens-O-Matic / RLC with TLS ties it without (the AEAD is per-packet, no
extra round trips).

## Throughput under loss (LAN, Mbit/s goodput) - the dividing line

| Transport | clean | 3% loss | 8% loss |
|---|---:|---:|---:|
| `TcpBridge` | 1375 | 111 | 11 |
| `TcpTlsBridge` | 1382 | 112 | 9 |
| `BlockingTcpBridge` | 1228 | 120 | 11 |
| `QuicBridge` (TLS) | 831 | 648 | 651 |
| Sens-O-Matic / RLC (+TLS) | 885 | 837 | 547 |
| Sens-O-Matic / RLC (plain) | 898 | 907 | 553 |
| **Sens-O-Matic / RS** | 801 | 838 | **758** |

The instant loss appears the order **inverts**. The three TCP-based bridges
collapse - their congestion control reads loss as congestion and drives the
window toward zero, so a lossy non-congested link strands them at 9-120
Mbit/s. QUIC's BBR holds throughput (it does not treat loss as congestion).
Both Sens-O-Matic codes hold by recovering the loss from parity already on
the wire - no retransmit round-trip, no window collapse. The block-RS code
is the under-loss throughput champion (758 Mbit/s at 8% loss, ~95% of its
clean rate); the sliding-window RLC code holds 547-907.

## Latency under loss (LAN round-trip, microseconds)

| Transport | clean p50 | clean p99 | 3%-loss p99 | 8%-loss p99 |
|---|---:|---:|---:|---:|
| `TcpBridge` | 244 | 4175 | 206145 | 247818 |
| `TcpTlsBridge` | 245 | 3833 | 206247 | 253999 |
| `BlockingTcpBridge` | 348 | 1219 | 204733 | 254053 |
| `QuicBridge` (TLS) | 287 | 4203 | 29853 | 37619 |
| Sens-O-Matic / RLC | 401 | 697 | 30555 | 30608 |
| **Sens-O-Matic / RS** | 287 | 5889 | **1592** | **2041** |

This is the sharpest result. Under loss the TCP bridges' tail latency blows
out to **204-254 ms p99**: a single lost segment head-of-line-blocks the
whole stream until a retransmit arrives, and every byte queued behind it
waits. Sens-O-Matic recovers the loss in-band from parity, so the stream
never stalls: **block-RS holds a 1.6-2.0 ms p99 - a ~130x lower tail than
TCP at the same 3% loss**, with interleaving spreading a burst across blocks.
The RLC code's p99 sits at ~30 ms (its sliding window recovers a round
later than the block code's interleave). QUIC's quinn retransmit puts its
tail between the two (~30-38 ms). The clean-link p99 ordering is a different
story - the FEC receivers are poll-paced when idle - so the clean p50 is the
fair clean-latency number and the under-loss p99 is the fair loss-latency
number.

## Real internet (WAN, secure transports, Mbit/s goodput, median [95% CI])

Ubuntu host to a remote datacenter VPS, ~22 ms RTT.

| Transport | clean | 3% loss | 5% loss | 8% loss |
|---|---:|---:|---:|---:|
| `QuicBridge` (TLS) | 333 [301, 342] | 299 [288, 311] | 300 [271, 309] | 300 |
| Sens-O-Matic / RLC (+TLS) | 335 [261, 356] | 258 [196, 294] | 260 [181, 283] | 261 |
| `TcpTlsBridge` | 302 [211, 488] | 4 | 2 | **0** |

Over the real internet the same inversion holds. Clean, all three are
within noise (~300-335). Under loss `TcpTlsBridge` collapses to single-digit
Mbit/s and finally to zero (it cannot finish the transfer before the cap as
TCP backs off). QUIC and Sens-O-Matic / RLC both hold ~260-300: QUIC via BBR
and ARQ, Sens-O-Matic via forward error correction. A rigorous
interleaved-paired analysis of QUIC vs Sens-O-Matic / RLC on this path put
them at a statistical tie at 0-5% loss with QUIC ~15% ahead at 8% (the FEC
parity overhead) - the trade Sens-O-Matic makes for its decisively lower
latency tail under loss.

## Cross-platform: a macOS receiver

The same transports run with a **macOS** server, proving the cross-platform
contract on real Apple hardware. Measured with the Mac (macOS 10.15 Catalina,
Intel 2012 i5-3210M) as the secure-transport receiver and a Linux client
injecting loss on its egress, over two WAN paths: a home Ubuntu host (~32 ms)
and the datacenter Linux VPS (~23 ms). The transfer is a smaller ~20 MB - the
decade-old chip bounds the receive-side FEC-decode / TLS work, which is the
honest property of this endpoint, not an artifact.

| Path / loss | `QuicBridge` | `TcpTlsBridge` | Sens-O-Matic / RLC |
|---|---:|---:|---:|
| home -> Mac, clean | 404 | 189 | 213 |
| home -> Mac, 3% | 358 | **3** | 206 |
| home -> Mac, 8% | 369 | **0** | 142 |
| datacenter -> Mac, clean | 440 | 425 | 204 |
| datacenter -> Mac, 3% | 421 | **4** | 189 |
| datacenter -> Mac, 8% | 374 | **2** | 164 |

The same inversion holds: under loss `TcpTlsBridge` collapses to single digits
and then zero (TCP backs off) while QUIC and Sens-O-Matic / RLC hold - the FEC
decode runs on the macOS receiver and sustains 142-206 Mbit/s through 3-8% loss
exactly as it does on Linux. This is the conservative result: on macOS < 14.4
the cross-process waker falls back to polling (the `os_sync` public futex is a
14.4+ API), so a newer Mac takes the fast path automatically. The first build
on Apple hardware also surfaced three portability fixes (`O_DIRECT` ->
`fcntl(F_NOCACHE)`, `iceoryx2`/`MSG_NOSIGNAL` gated off macOS, and the `os_sync`
14.4 symbols resolved via `dlsym`).

## Choosing a transport

- **Clean trusted link, raw bulk throughput**: a stream bridge. `TcpBridge`
  for plain speed, `BlockingTcpBridge` for zero CPU at idle.
- **Clean untrusted link**: `QuicBridge` or `TcpTlsBridge` (TLS 1.3); the
  TLS cost is negligible.
- **Lossy or wireless link, or any link where latency under loss matters**:
  **Sens-O-Matic**. It is the only transport here that holds both throughput
  *and* a low latency tail through 3-8% loss, where the TCP bridges collapse
  on both. Pick the **block-RS** code for the best under-loss throughput and
  the lowest tail on a trusted link (`std`-only, no crypto deps); pick the
  **RLC** code for the adaptive, optionally-encrypted path tuned to a
  variable internet route. Not sure which, or a route whose loss shifts?
  The `UnifiedSens*` endpoint carries both and auto-switches on the
  measured loss (RLC below ~15%, RS above), TLS 1.3 over both codes; force
  a single code only when you want to pin one.
- **A datagram you are willing to lose**: raw `udp` - fastest on the wire,
  but no reliability (36% delivered on a clean link here).

Reproduce any cell with
[`examples/bridge_lan.rs`](../crates/subetha-cxc/examples/bridge_lan.rs):
`--transport tcp|tcptls|quic|btcp|sens|udp`, `--fec rs|rlc` for the
Sens-O-Matic code, `--role server|client|ping|pong`, `--tls` with a shared
`--cert`/`--key`, `--loss` for Sens-O-Matic's own injection or `netem` for
the wire. The Sens-O-Matic loss-resilience detail is in
[`SENS_O_MATIC_PERFORMANCE.md`](SENS_O_MATIC_PERFORMANCE.md); the bridges'
cross-host figures are in
[`LAN_BRIDGE_PERFORMANCE.md`](LAN_BRIDGE_PERFORMANCE.md).
