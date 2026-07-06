# Sens-O-Matic Performance

Sens-O-Matic, the reliable-UDP FEC transport, measured on real wire and
under controlled link conditions with MTU-sized datagrams. Every run is
integrity-asserted: the receiver checks strict per-item sequence order,
exact count, and the payload sum, so a row that posts a number has proven
exactly-once in-order delivery on that run. Reproduce with the
[`udp_xhost` example](../crates/subetha-cxc/examples/udp_xhost.rs)
(`cargo run --release -p subetha-cxc --example udp_xhost -- --role ...`).

## What this measures

Three things, all of which the transport is built for:

1. **No head-of-line stall under loss** - the wire keeps flowing while lost
   data recovers in the background, so throughput drops only by the
   retransmitted bytes, not by a stall behind the gap.
2. **Raw throughput** at one MTU-sized datagram per item (the genuine UDP
   datagram rate).
3. **Loss resilience** - throughput stays flat through the forward-error-
   correction budget instead of collapsing the way plain UDP (silent drops)
   or a reactive ARQ stack (retransmit round-trips) would.

## Topology

Three LAN hosts. The Windows box on Wi-Fi is the bandwidth-limiting hop;
the two VMs are wired to the AP. Loopback rows isolate the protocol from
the link; an injected feedback delay reproduces a real link's recovery
round-trip on loopback so head-of-line behavior is measurable without the
LAN.

| Host | Hardware | Link |
|---|---|---|
| `192.168.1.210` | Windows 11, Ryzen 7 2700 (Zen+) | Wi-Fi 802.11ac |
| `192.168.1.213` | Ubuntu 24.04 KVM guest, Ryzen 7 5700G (Zen3) | virtio, wired to the AP |
| `192.168.1.74` | FreeBSD 15.0 KVM guest, Ryzen 7 5700G (Zen3) | virtio, wired to the AP |

The transport builds and runs natively on all three (Windows MSVC, Linux,
FreeBSD 15.0 with clang), and every cross-host run below asserted
exactly-once in-order delivery on every platform pair.

## Head-of-line blocking: selective NAK

A reliable transport that delivers in order has to recover a gap before it
can deliver past it. The question is whether recovering the gap stalls the
WIRE. Sens-O-Matic recovers every gap the receiver is holding in a single
round-trip: the receiver re-requests them all at once (selective NAK), the
retransmits ride the next interleave together, and the delivery frontier
advances in bulk. The wire never stops - the sender pipelines new blocks
across a deep flow window while the receiver buffers them out of order and
drains in order once the gap fills.

The naive alternative, chasing one gap per round-trip (serial NAK), stalls
a real link: at high loss almost every block needs a retransmit, and
recovering them one round-trip at a time crawls. The harness exposes both
(`--serial-nak` selects serial recovery) so the difference is directly
measurable.

Controlled comparison, 30% loss with a 10ms recovery round-trip injected on
loopback, MTU datagrams, 20,000 items, every run PASS (order + count + sum):

| Recovery strategy | goodput | wall time |
|---|---:|---:|
| **Selective NAK (production)** | **116.8 Mbit/s** | 1.92s |
| Serial head-only NAK | 11.1 Mbit/s | 20.21s |
| Clean-link reference (selective) | 117.9 Mbit/s | 1.90s |

Selective NAK at 30% loss runs at clean-link speed (116.8 vs 117.9): the
throughput does not drop. Serial recovery crawls 10x slower on the same
workload. A burst makes the difference visible - a clean stream with a
2,000-datagram loss burst dropped mid-transfer, then clean again:

```
SERIAL head-only NAK, interval goodput across the burst:
  212.0 Mbit/s   normal
   16.3 Mbit/s   burst hits
    5.1 Mbit/s   |
    5.1 Mbit/s   |  ~2.5s head-of-line crawl: one gap per round-trip
    5.1 Mbit/s   |
    5.1 Mbit/s   |
   54.3 Mbit/s   draining
  240.7 Mbit/s   caught up
  => 6.08s total, 73.6 Mbit/s average

SELECTIVE NAK, same burst:
  => 2.61s total, 171.5 Mbit/s average - burst absorbed in one round-trip,
     no sustained drop (the recovery is shorter than one 500ms sample)
```

The 5.1 Mbit/s crawl is the head-of-line block: under serial recovery the
wire stalls behind the gap while it recovers one round-trip at a time.
Selective recovery keeps the wire flowing - the burst is a blip, recovered
fully, and throughput holds at the clean rate.

## Raw throughput (MTU datagrams, k=8 r=2, interleave 8)

Goodput is delivered application bits per second; every cell asserted
order + count + sum and PASSED.

| Path | 0% loss | 15% loss | 30% loss |
|---|---:|---:|---:|
| Win -> Ubuntu Wi-Fi (100k items, 3-sample median) | 122 Mbit/s | 127 Mbit/s | 105 Mbit/s |
| Ubuntu <-> FreeBSD wired (150k items) | PASS | PASS | PASS |

The Win -> Ubuntu Wi-Fi row is the clean wireless measurement - three
interleaved samples per cell (loss levels rotated 0,15,30,0,15,30,... so a
slow channel moment hits every cell equally), run-to-run spread under ±3%.
**15% loss meets or beats clean (127 vs 122)**: forward error correction
recovers loss inside the parity budget with no retransmit round-trip, and
the dropped datagrams are work the receiver no longer does. **30% loss
holds 86% of clean (105 vs 122)** - the cost of the retransmitted bytes
alone, not a stall. Every one of the nine Wi-Fi runs and all twelve wired
runs delivered every item exactly once, in order.

The Ubuntu <-> FreeBSD wired path runs in the high hundreds of Mbit/s but
swings several-fold run-to-run because both VMs share one KVM host's CPU,
so its absolute number is not load-bearing - the controlled comparison
above is the precise head-of-line measurement. Its value here is the
cross-platform, cross-OS exactly-once proof: a Linux sender and a FreeBSD
receiver, every loss level, every item delivered.

## Fast-link throughput and sharding

On a link faster than the wireless wire above - loopback, multi-gigabit -
a single stream is bound by one core's data path, not the link. One
Sens-O-Matic stream moves 1.24 Gbit/s on Linux loopback (batched
`recvmmsg` receive). Sharding distributes the whole data path
(encode + send out, recv + decode + deliver in) across N independent
streams on N threads, item `i` riding shard `i % N`, reassembled
round-robin into the global order:

| shards | 1 | 2 | 3 | 4 |
|---|---:|---:|---:|---:|
| Gbit/s | 1.26 | 1.97 | 2.45 | 2.75 |

Four shards reach 2.2x a single stream. The curve plateaus there at the
loopback packet-rate ceiling (more shards do not lift it, and at r=0 the
plateau moves only ~9%), so the residual to a 4-worker QUIC stream is the
per-block protocol cost plus the FEC parity carried for loss resilience,
not the threading. Each shard is the single-threaded transport unchanged,
so FEC, selective NAK, and the hold-time hold per shard. On a
bandwidth-limited link one stream already saturates the wire, so sharding
is the fast-link lever, not the lossy-link one.

## Pacing

Two mechanisms keep the sender at link capacity without manufacturing
loss.

**Send-buffer backpressure (the floor).** The sender socket is
non-blocking, so a `send` returns `WouldBlock` when the kernel send buffer
fills - the sender momentarily outrunning the link. It spins briefly and
retries rather than dropping the datagram, so it emits at link capacity
instead of adding loss on top of the link's own (which FEC and ARQ would
then have to recover).

**LEDBAT bufferbloat pacer.** Above that floor, a LEDBAT-style pacer
watches the round-trip delay and clamps the encoder's flow window DOWN
from its configured maximum toward the bandwidth-delay product whenever a
self-induced queue forms, holding the queue near a small target delay and
restoring the window as the queue drains. It only ever clamps below the
un-paced window, so it costs nothing on a queue-free link and prevents
bufferbloat on a buffered one. It is on by default; the harness's
`--no-pace` disables it for the un-paced A/B baseline.

Pacing keeps the only loss the link's own, and forward error correction
recovers that inside the parity budget with no round-trip. This is why the
15% column can meet or exceed clean.

## Resilience: FEC-primary, flat through the budget

- **Flat through the parity budget.** Inside the FEC budget (loss <= the
  parity rate) there is effectively no throughput cost to loss; the parity
  is already on the wire and recovery needs no round-trip.
- **Exact delivery at every loss level**, both directions, every path.
  That is the guarantee plain UDP cannot make and a reactive ARQ stack pays
  latency for.
- **Adaptive parity.** The receiver reports its measured loss on every
  feedback packet; the sender raises `r` for subsequent blocks, so FEC
  carries more of the loss as it rises and ARQ stays the fallback.
- **Whole-tail recovery.** A tail block whose every shard is lost - never
  "seen" by the receiver, so above its received frontier - is re-requested
  by the recv-timeout drain NAK, so delivery completes to the last item.

## Link-liveness and proactive recovery

A link can go silent - a Wi-Fi roam, a route flap, a peer pause - and the
difference between a transport that rides it out and one that stalls is how
fast it resumes when feedback returns.

- **Dead-link detection.** When feedback silence exceeds a PTO derived
  from the smoothed RTT, the sender declares the link dead. Nothing is
  lost - the producer is already held by flow-control backpressure (the
  window cannot advance with no ACKs) - and the sender adds a periodic
  probe, a retransmit of the oldest unacked block, both to detect recovery
  and to pre-position the stalled frontier.
- **Proactive burst on recovery.** On the first feedback after a dead
  spell, the sender proactively bursts the whole still-unacked window
  oldest-first instead of waiting one round-trip per NAK. The burst is
  metered through the same BtlBw rate the bufferbloat pacer uses, so the
  recovery resend fills the pipe without tripping the pacer it cooperates
  with, and a short grace window holds the pacer from clamping the window
  it just refilled. The harness's `--no-proactive` disables this for the
  reactive-NAK A/B baseline.

## Hold-time: bounded recovery, then skip the gap

Delivery is in order, and a gap is held while FEC and ARQ recover it in the
background - the wire keeps flowing past it. By default a gap is held a long
time (`with_max_hold`, 60s) so recovery lands first and delivery is
exactly-once. A caller that prefers bounded latency over strict reliability
sets a shorter hold: once a gap has been held past the deadline without
recovering, the transport skips it and advances delivery, so a genuinely
unrecoverable block (a link that dropped every copy and every retransmit)
costs only its own bytes instead of blocking the stream forever. This is the
partial-reliability escape hatch; the default keeps the exactly-once
contract.

Demonstrated on the running binary: with every k-th block forced
unrecoverable and a 500ms hold, the stream skips the dead gaps and delivers
the rest (1744 of 2000 with 256 genuinely-gone items dropped) instead of
blocking on the first gap.

## Reading the numbers

- **No head-of-line stall.** Selective NAK recovers every held gap in one
  round-trip, so 30% loss runs at clean-link speed; serial recovery crawls
  10x slower, and a loss burst is a blip, recovered fully.
- **Raw throughput is the genuine UDP datagram rate** at MTU - 122 Mbit/s
  clean across a Wi-Fi hop, high hundreds on the wired link.
- **15% loss is recovered without a round-trip.** The 15% column meets or
  beats clean (127 vs 122 Wi-Fi); FEC absorbs loss inside the budget.
- **30% loss costs only the retransmitted bytes** (105 vs 122 Wi-Fi, 86% of
  clean), not a stall: the wire keeps flowing while ARQ fills gaps in
  parallel.
- **Exactly-once across three OSes.** Every Wi-Fi and wired run, including a
  Linux-sender / FreeBSD-receiver pair, delivered every item in order;
  whole-tail loss is recovered by the drain NAK.
- **Unrecoverable loss is bounded by the hold-time**, not unbounded
  blocking; the default holds long enough to stay exactly-once.
