# Cross-Host LAN Bridge Performance

This document measures three byte-stream bridges - `TcpBridge`,
`QuicBridge`, and `BlockingTcpBridge` - between two PHYSICAL hosts over a
real LAN hop, not loopback, not two processes on one machine. They are
three of the six transports `bridge_lan` carries: the others are the
TCP+TLS bridge (`tcptls`, the encrypted-TCP contender - identical framing
to `TcpBridge` inside a rustls 1.3 record layer), **Sens-O-Matic**
(`sens`, the reliable-UDP FEC transport), and a raw-UDP reference (`udp`,
unreliable, reports its delivery ratio). Sens-O-Matic is the transport
built for this doc's lossy Wi-Fi hop; it is summarised under
[Sens-O-Matic on the lossy hop](#sens-o-matic-on-the-lossy-hop) below and
measured in full in
[SENS_O_MATIC_PERFORMANCE.md](SENS_O_MATIC_PERFORMANCE.md).

Every run is integrity-asserted: the receiving application checks
strict per-item sequence order, exact count, and the payload sum,
so a row that posts a number has proven exactly-once in-order
delivery on that run. Raw numbers in
[`lan_bridge_results.json`](lan_bridge_results.json); reproduce
with [`examples/bridge_lan.rs`](../crates/subetha-cxc/examples/bridge_lan.rs).

## Topology

| Host | Role mix | Hardware | Link |
|---|---|---|---|
| `192.168.1.210` | client / server / ping | Windows 11, Ryzen 7 2700 (Zen+) | **Wi-Fi** - the bandwidth-limiting hop |
| `192.168.1.213` | client / server / pong | Ubuntu 24.04 KVM guest, Ryzen 7 5700G (Zen3) | virtio, wired to the AP |
| `192.168.1.74` | client / server / pong | FreeBSD 15.0-RELEASE VM guest, Ryzen 7 5700G (Zen3) | wired to the AP |

The Windows box is one end of every pair; the Ubuntu and FreeBSD
peers were measured in separate sessions on the same hypervisor
host. Baseline ICMP RTT between the hosts: ~1 ms. The Wi-Fi hop
caps sustained throughput in the low-hundreds of Mbit/s; the
numbers below measure the BRIDGES at that wire's ceiling, not the
bridges' own ceiling (on loopback the same TCP bridge moves
3.2 Gbit/s).

## One-way streams (1,000,000 x 64-byte slots per run)

| Bridge | Win -> Linux | Linux -> Win | Win -> FreeBSD | FreeBSD -> Win | Integrity |
|---|---:|---:|---:|---:|---|
| `TcpBridge` | 237 Mbit/s | 227 Mbit/s | 256 Mbit/s | 238 Mbit/s | order + count + sum PASS, all four |
| `QuicBridge` (TLS, cross-host certs) | 197 Mbit/s | 215 Mbit/s | 193 Mbit/s | 247 Mbit/s | PASS, all four |
| `BlockingTcpBridge` (parked rings) | 213 Mbit/s | 270 Mbit/s | 255 Mbit/s | 238 Mbit/s | PASS, all four |

On FreeBSD the blocking bridge's ring endpoints park on the native
`_umtx_op` waker arm (the non-PRIVATE, physical-address-keyed umtx
ops) rather than a polling fallback - the same zero-CPU-idle
property the Linux futex arm provides.

Rates are the receiving application's first-pop-to-last-pop drain
rate. All three stacks saturate the same Wi-Fi-class wire within
~20% of each other: QUIC pays its TLS + UDP framing tax, the
blocking bridge's park-then-burst-drain egress holds TCP-class
throughput while keeping its zero-CPU-idle property.

The QUIC rows exercise the real cross-host certificate flow: a
self-signed cert generated once (`bridge_lan --gen-cert`), shipped
to both hosts as DER bytes, rebuilt into server/client configs via
`make_server_config_from_der` / `make_client_config_from_der`,
with the SNI naming the cert rather than the wire address.

## Loopback round trips (2,000 rounds, both halves on the Windows host)

The same rtt mode with both bridge halves on one machine isolates
the stack's own cost from the wire. The calibration point: a raw
8-byte TCP socket ping-pong on this host round-trips at ~73 µs
(one-way 36.7 µs in the IPC leaderboard), so the TCP bridge's full
ring -> wire -> ring -> echo chain matches a bare socket.

| Bridge | min | p50 | p99 |
|---|---:|---:|---:|
| `TcpBridge` | 45.3 µs | 71.3 µs | 211.5 µs |
| `QuicBridge` (TLS) | 85.4 µs | 144.4 µs | 346.8 µs |
| `BlockingTcpBridge` | 119.0 µs | 164.9 µs | 288.0 µs |

QUIC's premium over plain TCP is the TLS record path; the blocking
bridge's is its parked-waker tick machinery, the price of zero CPU
at idle. Reproduce with two local `bridge_lan` processes in `ping`
/ `pong` roles against `127.0.0.1`.

## Round trips (2,000 rounds, ping on Windows)

One round = app pushes a 64-byte item into its local ring, the
bridge ships it across the LAN, the remote app pops + echoes into
its outbound ring, the bridge ships it back, the app pops the echo:
ring -> wire -> ring -> echo -> wire -> ring.

| Bridge | peer | min | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| `TcpBridge` | Linux | 0.68 ms | 0.92 ms | 6.2 ms | 1.02 s |
| `QuicBridge` | Linux | 0.70 ms | 1.00 ms | 5.3 ms | 0.10 s |
| `BlockingTcpBridge` | Linux | 0.77 ms | 1.00 ms | 4.1 ms | 1.06 s |
| `TcpBridge` | FreeBSD | 0.67 ms | 0.88 ms | 6.2 ms | 1.06 s |
| `QuicBridge` | FreeBSD | 0.67 ms | 1.06 ms | 15.3 ms | 0.12 s |
| `BlockingTcpBridge` | FreeBSD | 0.87 ms | 1.20 ms | 15.3 ms | 1.06 s |

p50 sits on top of the raw ~1 ms ICMP RTT: the full
ring-to-ring-and-back chain adds well under 100 us at the median
on the plain-TCP rows. The blocking bridge's p50 premium over
plain TCP is the kernel park/wake pair on each ring touch - the
price of zero CPU at idle - measured at ~80 us against the Linux
futex and ~320 us against FreeBSD's `_umtx_op`. The max-column
outliers (one ~1 s stall per 2,000 rounds on the TCP runs) are
Wi-Fi power-save / retransmit artifacts on the wireless hop,
present in raw ICMP on the same link.

## What the LAN run exposed (and fixed)

Loopback smoke-testing the harness caught a real data-path flaw in
all three bridges: one `write_all` await and one 64-byte
`read_exact` per item - a syscall-per-slot wire path that capped
TCP loopback at 26 Mbit/s. The fix that shipped:

- **Burst-batched egress** (all bridges): every already-available
  ring slot (up to `EGRESS_BATCH_SLOTS = 256`, 16 KiB) goes out in
  one socket/stream write. A lone item ships immediately -
  batching never waits for items that have not arrived.
- **Chunked ingress** (all bridges): socket reads take whatever
  the wire has, complete slots push as they assemble, a partial
  slot carries to the next read - so a trickle flows item-by-item
  while a flood moves in 64 KiB reads.
- **`TCP_NODELAY` on both ends** (TCP bridges): a lone
  latency-sensitive slot is never parked on Nagle's timer; the
  batched writes keep segments MSS-filled under load regardless.
- The blocking bridge parks for the FIRST item only, then
  burst-drains via `try_pop`, preserving zero-CPU-idle while
  fixing its per-item `spawn_blocking` round trip.

After the fix, the same loopback run moved 3.2 Gbit/s (157
ns/item) - a 123x improvement - and the LAN runs above saturate
the physical wire.

## Sens-O-Matic on the lossy hop

The three byte-stream bridges above saturate the clean Wi-Fi wire, but
their max-latency column shows what the wireless link costs: ~1 s stalls
from 802.11 power-save / retransmit, one per ~2,000 rounds. Those stalls
are head-of-line blocking - TCP (and QUIC's per-stream order) cannot
deliver past a lost segment until the kernel retransmits it, so a single
drop parks the whole stream for a recovery round-trip.

`bridge_lan --transport sens` is the transport built for exactly that
hop. Sens-O-Matic ships MTU-sized items as forward-error-corrected
datagrams and recovers loss inside its parity budget with no retransmit
round-trip, so the wire never stalls behind a gap - the receiver buffers
out of order across a deep flow window and drains in order once the
parity (or a single selective-NAK round-trip) fills it. On the SAME
Win -> Ubuntu Wi-Fi hop, measured with MTU datagrams, it holds flat
through loss:

| Loss | Sens-O-Matic goodput | vs clean |
|---|---:|---|
| 0% | 122 Mbit/s | baseline |
| 15% | 127 Mbit/s | meets/beats clean - FEC recovers loss as work the receiver skips |
| 30% | 105 Mbit/s | 86% of clean - the retransmitted bytes' cost, not a stall |

Every Wi-Fi run delivered every item exactly once, in order. The full
loss-axis measurements - selective-NAK head-of-line behavior (30% loss
at clean-link speed, against 10x slower serial recovery), sharding to
2.75 Gbit/s across four streams, adaptive parity, and three-OS
exactly-once delivery - are in
[SENS_O_MATIC_PERFORMANCE.md](SENS_O_MATIC_PERFORMANCE.md).

Sens-O-Matic sits in its own table, not the byte-stream rows above,
because it ships different framing: the bridges ferry batched 64-byte
ring slots (a max-throughput byte-stream comparison), Sens-O-Matic ships
MTU datagrams (a loss-resilience comparison). The axes are orthogonal -
the bridges win raw throughput on a clean wire, Sens-O-Matic wins the
moment the wire drops.

## Reproducing

```bash
# Once, anywhere - ship cert.der to both hosts, key.der to servers:
bridge_lan --gen-cert cert.der key.der

# Receiving host:
bridge_lan --transport quic --role server --bind 0.0.0.0:7401 \
    --items 1000000 --cert cert.der --key key.der

# Sending host:
bridge_lan --transport quic --role client --connect <server-ip>:7401 \
    --items 1000000 --cert cert.der

# Round trips (both sides print BOUND, then wait for a stdin line
# so neither client dials before both listeners are up):
bridge_lan --transport tcp --role pong --bind 0.0.0.0:7402 \
    --connect <peer-ip>:7401 --rounds 2000
bridge_lan --transport tcp --role ping --bind 0.0.0.0:7401 \
    --connect <peer-ip>:7402 --rounds 2000
```

Build with `cargo build --release --example bridge_lan --features
quic-bridge,tcp-bridge`. Firewalls: the server side needs inbound
TCP (tcp/btcp) or UDP (quic) on the bound port; on Windows an
app-scoped exception for `bridge_lan.exe` covers both.

## Related results

- [Cross-process IPC comparison](CROSS_PROCESS_IPC_PERFORMANCE.md) -
  the same-host kernel-bypass numbers these bridges extend across
  hosts
- [Ordering mode ladder](ORDERING_MODES_PERFORMANCE.md) - the
  cross-producer ordering axis measured cross-process
- [Sens-O-Matic performance](SENS_O_MATIC_PERFORMANCE.md) - the
  reliable-UDP FEC transport (`bridge_lan --transport sens`), measured
  on the same LAN hosts on the loss-resilience axis
