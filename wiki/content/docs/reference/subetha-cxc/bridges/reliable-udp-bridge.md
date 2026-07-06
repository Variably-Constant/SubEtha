---
title: "Sens-O-Matic (Reliable-UDP Bridge)"
weight: 64
---

# Sens-O-Matic: ReliableUdpSender + ReliableUdpReceiver

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Feature](https://img.shields.io/badge/deps-std%20only-brightgreen)
![Transport](https://img.shields.io/badge/transport-reliable%20UDP-green)
![Encryption](https://img.shields.io/badge/encryption-none-lightgrey)

**Sens-O-Matic is the reliable FEC-UDP *protocol*** - a *sighted,
forward-correcting* alternative to a blind, reactive ARQ stack, named for
the Sub-Etha Sens-O-Matic that detects Sub-Etha signals. Like a cipher
suite inside TLS, its erasure code is a swappable detail; the protocol
carries either of two:

- **Block Reed-Solomon** (this page) - MDS, systematic Cauchy, fixed
  parity per block, `std`-only. Types `ReliableUdpSender` /
  `ReliableUdpReceiver`, also aliased `SensOMaticRsSender` /
  `SensOMaticRsReceiver`.
- **Sliding-window RLC** - adaptive, packet-pair rate control, optionally
  wrapped in TLS 1.3. Types `SensOMaticRlcSender` / `SensOMaticRlcReceiver`
  in [`sens_rlc`](../sens-rlc/). It recovers a lost symbol
  from the *next* repair rather than waiting for the rest of a block.

Both deliver ordered, lossless items over a `UdpSocket`, recovering most
loss from parity already on the wire (no retransmit round-trip) with ARQ
as the floor. The block-RS code below depends only on `std` - no tokio,
no quinn, no rustls - so a trusted-network deployment pays nothing for a
crypto stack it does not use; the RLC code adds optional TLS for an
untrusted path. The rest of this page documents the block Reed-Solomon
code.

Sibling to [`TcpBridge`](../tcp-bridge/) and [`QuicBridge`](../quic-bridge/):
where TCP is reliable but head-of-line-blocking and QUIC is reliable but
TLS-bound, this transport keeps UDP's properties and adds reliability
through coding rather than a stream abstraction.

## Reliability: FEC-primary, ARQ-fallback

Items are grouped into blocks of `k` source shards shipped with `r`
parity shards, computed by a systematic Cauchy Reed-Solomon code
([`fec`](../../../)). Up to `r` lost shards per block are reconstructed by
the receiver with **no retransmit**. When a block loses more than `r`
shards - the rare burst beyond the parity budget - the receiver NAKs the
missing shard indices and the sender retransmits exactly those (ARQ).
ARQ is the correctness floor; a head or tail block that loses every shard
is re-requested whole, and repeated NAKs for one block are rate-limited
per block so a single loss never triggers a retransmit storm.

The parity rate `r` is **automatic**: the receiver reports its measured
loss fraction on every feedback packet and the sender raises or lowers
`r` for subsequent blocks, so FEC carries the common case and ARQ stays a
fallback.

## Recovery without head-of-line stall

In-order delivery has to recover a gap before delivering past it, but it
must not stall the WIRE while doing so. The receiver re-requests **every
gap it is holding in one round-trip** (selective NAK), so all retransmits
ride the next interleave together and the delivery frontier advances in
bulk - rather than chasing one gap per round-trip while the sender's flow
window fills and the wire stalls behind the gap. At high loss, where almost
every block needs a retransmit, this is the difference between throughput
holding at the clean-link rate and collapsing: in a controlled 30%-loss run
with a 10ms recovery round-trip, selective recovery held 116.8 Mbit/s while
serial one-gap-per-round-trip recovery managed 11.1. The sender pipelines
new blocks the whole time (a deep flow window), and the receiver buffers
them out of order and drains them in order once the gap fills, so a loss is
a blip the stream recovers from, not a stall.

## Burst tolerance and whole-block recovery

Two structural layers compose on top of the per-block code:

- **Interleaving** permutes the transmit order so a burst of consecutive
  losses spreads to at most one shard per block - back inside the parity
  budget. The interleave depth is a control-table knob.
- **The cross-block tower** ships fire-and-forget outer-parity blocks per
  segment, reconstructing a whole lost block from its neighbours when an
  entire block (every shard) is erased - a loss ARQ alone cannot recover
  if the retransmits are lost too. Enable it with
  `ReliableUdpSender::enable_tower(d, r_outer)`.

## Adaptive control

A controller polls its sensors on a slow cadence and publishes coding
decisions into a lock-free atomic control table that the per-packet path
reads with a single relaxed load. Sensors: the in-band loss measurement,
a clock-offset-invariant one-way-delay trend estimator fed by sender
heartbeats, and a platform link sensor (Linux `/sys/class/net` drop
counters, Windows `WlanQueryInterface` signal quality). A degrading link
raises protection before the in-band loss estimate sees it.

## Sockets and pacing

The receiver socket parks on a read timeout (zero idle CPU; the timeout
also drives tail-ARQ feedback). The sender socket is non-blocking, so item
throughput never waits on feedback - but a non-blocking `send` returns
`WouldBlock` when the kernel send buffer fills, which happens whenever the
sender outruns the link. The sender **paces** there: it briefly spins and
retries rather than dropping the datagram, so it emits at link capacity
instead of manufacturing loss on top of the link's own.
`ConnectionReset` on a UDP receive (the Windows `WSAECONNRESET` artifact)
is treated as ignorable, not fatal.

## Hold-time and partial reliability

Delivery is in order, and a gap (a block that lost more than parity can
recover) is held while FEC and ARQ recover it in the background - the wire
keeps flowing past it with later blocks rather than stalling. By default a
gap is held a long time (`ReliableUdpReceiver::with_max_hold`, 60s) so
recovery lands first and delivery stays exactly-once. A caller that prefers
bounded latency over strict reliability sets a shorter hold: a gap held
past its deadline without recovering is skipped so the stream advances, and
a genuinely unrecoverable block costs only its own bytes instead of
blocking forever.

## Sharding across cores

A single stream runs the whole data path - encode and send on the way out,
receive and decode and deliver on the way in - on one thread, so on a link
faster than that one core can drive (loopback, multi-gigabit) it is
core-bound, not link-bound. `ShardedSender` / `ShardedReceiver` run N
independent streams, one thread each, distributing the entire path over
cores. Application item `i` rides shard `i % N` (shard `s` on port
`base + s`); the receiver reassembles the global order round-robin, since
each shard delivers its own items in order. Each shard is the
single-threaded `ReliableUdpSender` / `ReliableUdpReceiver` unchanged, so
FEC, selective NAK, and the hold-time hold per shard. Four shards reach
2.2x a single stream on loopback. On a bandwidth-limited link one stream
already saturates the wire, so sharding is the fast-link lever.

## Performance

Measured on real wire between separate OS processes, integrity-asserted
(order + count + sum) on every run; the full matrix, confidence intervals,
and methodology are in
[`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/TRANSPORT_COMPARISON.md).
The LAN is an Ubuntu 24.04 and a FreeBSD 15.0 host (each a VM on one Zen3 /
Ryzen 7 5700G machine, cross-OS over virtio NICs); the transport builds and
runs natively on both. Sampling is interleaved over ten rounds, reported as
the median with a bootstrap 95% confidence interval.

The block-RS code's defining result is holding throughput **and** a low
latency tail under loss, where TCP collapses on both. Clean it moves **801
Mbit/s** (the FEC parity is the gap to the raw stream bridges). Under `netem`
loss it holds **838 Mbit/s at 3% and 758 at 8%** - ~95% of its clean rate -
while the TCP bridges (`TcpBridge`, `TcpTlsBridge`, `BlockingTcpBridge`)
collapse to ~115 and ~10 Mbit/s as their congestion control reads loss as
congestion and backs the window toward zero. The latency gap is sharper: a
lost TCP segment head-of-line-blocks the whole stream until its retransmit
lands, so the TCP bridges' p99 round-trip is **204-254 ms** at 3-8% loss; the
block-RS code recovers in-band from parity already on the wire, holding a
**1.6-2.0 ms p99 - a ~130x lower tail at the same 3% loss**. Transmit
interleaving spreads a burst across blocks, and `ShardedSender` /
`ShardedReceiver` (N independent streams reassembled in order) is the
fast-link lever for when one core cannot drive the wire. The A/B, burst trace, the cross-OS matrix, the
per-shard scaling curve, pacing root-cause, and hold-time demonstration
are in
[`SENS_O_MATIC_PERFORMANCE.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/SENS_O_MATIC_PERFORMANCE.md)
and
[`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/TRANSPORT_COMPARISON.md).

## Verify

```rust
use std::time::Duration;
use subetha_cxc::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};

// Receiver on a loopback port, 15% injected loss so FEC / ARQ engage.
let mut recv = ReliableUdpReceiver::bind("127.0.0.1:0")?
    .with_debug_loss(15, 7);
let addr = recv.local_addr()?;

let rx = std::thread::spawn(move || {
    let mut got = Vec::new();
    while got.len() < 1000 {
        for item in recv.poll().unwrap() {
            got.push(u64::from_le_bytes(item.try_into().unwrap()));
        }
    }
    got
});

let mut send = ReliableUdpSender::bind("127.0.0.1:0", addr, 8, 2, 8)?;
send.enable_tower(8, 2);
for i in 0..1000u64 {
    send.send_item(&i.to_le_bytes())?;
}
send.flush()?;
send.drain_until_acked(Duration::from_secs(15))?;

assert_eq!(rx.join().unwrap(), (0..1000).collect::<Vec<_>>());
# Ok::<(), std::io::Error>(())
```

The full example, including the flow-control and grace-feedback details,
is [`udp_bridge_e2e`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/udp_bridge_e2e.rs):

```bash
cargo run --release --example udp_bridge_e2e -p subetha-cxc
```
