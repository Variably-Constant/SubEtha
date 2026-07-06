---
title: "Sens-O-Matic / unified RLC<->RS auto-switch"
weight: 66
---

# Sens-O-Matic, unified RLC<->RS auto-switch: UnifiedSensSender + UnifiedSensReceiver

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Feature](https://img.shields.io/badge/deps-std%20%2B%20optional%20tls-brightgreen)
![Transport](https://img.shields.io/badge/transport-reliable%20UDP-green)
![Encryption](https://img.shields.io/badge/encryption-optional%20TLS%201.3-blue)

The [Sens-O-Matic](../reliable-udp-bridge/) protocol carries two erasure
codes with opposite strengths: the **block Reed-Solomon** code recovers a
whole block at once (highest throughput and a bounded worst-case latency
under heavy loss), and the **sliding-window RLC** code
([sens-rlc](../sens-rlc/)) recovers a loss from the next repair (lowest
latency at light loss). The unified endpoint runs **both** on one port and
switches between them mid-stream on the loss the receiver feeds back, so a
connection rides the code that wins at its current loss. Types
`UnifiedSensSender` / `UnifiedSensReceiver` and `CodeSwitchController` live
in [`sens_unified`](../../../).

## The crossover

The controller runs RLC below the threshold and RS above it. The pinned
switch threshold is **~15% loss** (`CROSSOVER_LOSS_Q8 = 38`, loss encoded q8
as `loss * 256`): below it RLC keeps the stream for its low-loss latency edge
(lower TTFD and median, incremental delivery), and above it the cover-parity
RS carries the throughput and a bounded tail. The relax-back threshold is
**~10%** (`down_q8 = 26`); the gap between them is a hysteresis band so loss
hovering near the boundary does not flap the code. An up-switch confirms over
3 feedback windows and a down-switch over 8, so a transient spike or dip does
not cross either way on its own. The loss at which RS's raw throughput
overtakes RLC's is workload-dependent (on an MTU-item link RS leads from low
loss); the measured per-code matrix is in
[`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/TRANSPORT_COMPARISON.md).

## The code-agnostic loss estimate

The switch is driven by the raw channel loss read from sent-vs-received
datagram counts, **not** from either code's own feedback - a code-specific
signal collapses the moment that code recovers the loss, which is what made
a naive estimate flap. The estimate is size-weighted and decaying: the lost
and sent datagram counts each decay at `0.95` per feedback window and the
estimate is their ratio, so large windows dominate and a small window with
one drop cannot read a spuriously high rate.

It is sampled every `SWITCH_SAMPLE_PERIOD` (50 ms) and gated three ways so
it acts only on a real, settled channel rate: a `SWITCH_WARMUP` (1 s) while
the in-flight window ramps from zero (that ramp reads as loss); a
`MIN_ACCUM_WINDOWS` (6) maturity hold after the warmup, so a start-of-stream
retransmit burst cannot spike the cold accumulator across the threshold;
and a `MIN_LOSS_SAMPLE` (30 datagrams) floor per window. A 64-byte-item
loopback run measured the estimate at 0.060 / 0.102 / 0.125 / 0.150 against
injected 6 / 9 / 12 / 15% loss.

## Handover: in order and exactly once, both ways

A switch never drops, duplicates, or reorders an item. The sender keeps a
replay ring of recently-sent payloads so a handover resends the un-acked
tail over the new code rather than draining the old one slowly:

- **RLC -> RS** announces the boundary RLC has delivered to, switches, and
  resends the tail `[boundary, sent)` over RS (reliable and fast at any
  loss, so it never waits on RLC's slow frontier recovery). The receiver
  re-bases the RS stream onto the global item index at the boundary and
  drops the overlap with what RLC already delivered.
- **RS -> RLC** drains RS, then **re-syncs the RLC stream**. RLC's per-code
  source id advances only for RLC-phase items, so after an RS stint it has
  diverged from the global item index; both ends re-base their source-id
  frontier to the global boundary (`skip_to`), so the resumed stream is
  clean and in order instead of stalling on holes RLC never carried or
  replaying its pre-switch buffer that RS already delivered.

A **flow-block escape** backstops a genuine RLC deadlock the loss estimate
cannot see: if RLC's delivery frontier stays stuck for 750 ms (extreme loss
past its redundancy ceiling, where a stalled sender emits no fresh loss
sample), the transport migrates to RS and **latches** it, since a code that
just stalled at this loss must not relax straight back. A latency-priority
floor keeps RLC's repair step and window from relaxing below the configured
baseline so the light-loss latency edge is preserved.

## Cover-parity on Reed-Solomon

When the switch lands on RS, the parity rate is provisioned to **cover** the
measured loss rather than merely track it: with the loss inflated by a 20%
margin (`p = 1.2 * loss`), `r = ceil(p * k / (1 - p))`, capped so
`k + r <= MAX_SHARDS` (32, the per-block index space; `k + r = MAX_SHARDS`
is decode-sound over GF(256)). Covering the loss
is what makes RS the high-loss throughput code; the [block-RS
page](../reliable-udp-bridge/) covers the code itself.

## One port, optional TLS 1.3

The endpoint demultiplexes QUIC and Sens-O-Matic on a single UDP port by
the first wire byte, and a CODE_SWITCH control frame carries the boundary
to the receiver. `connect_tls` / `bind_tls` wrap the whole endpoint in one
rustls TLS 1.3 handshake whose 1-RTT key seals every datagram of **both**
codes, so the switch is crypto-transparent and adds no extra round trip.

## Verify

```rust
use subetha_cxc::sens_unified::{CodePolicy, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender};

const ITEM_BYTES: usize = 64;
const ITEMS: u64 = 5000;

let cfg = UnifiedConfig {
    policy: CodePolicy::default_auto(), // RLC<->RS, switched on measured loss
    symbol_len: ITEM_BYTES + 8,
    k: 16,
    r: 16,
    rlc_flow_window: 4096,
    debug_loss: 8, // 8% injected into both decoders so recovery engages
    seed: 42,
    rlc_step: 4,
    rlc_static: false,
};

// Receiver injects the loss into both decoders so delivery exercises recovery.
let mut recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg)?;
let addr = recv.local_addr()?;

let rx = std::thread::spawn(move || {
    let mut got = Vec::new();
    while (got.len() as u64) < ITEMS {
        for item in recv.poll().unwrap() {
            got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
        }
    }
    got
});

let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg)?;
let mut buf = vec![0u8; ITEM_BYTES];
for i in 0..ITEMS {
    buf[..8].copy_from_slice(&i.to_le_bytes());
    send.send_item(&buf)?;
}
send.finish()?;

// Every item is delivered exactly once, in order, regardless of code.
assert_eq!(rx.join().unwrap(), (0..ITEMS).collect::<Vec<_>>());
# Ok::<(), std::io::Error>(())
```

A short run stays on RLC (it finishes inside the warmup). The loss-driven
switch shows at sustained scale: in
[`bridge_lan`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
(`--transport sens --fec auto`), a 500k-item run holds RLC at 6% loss
(0 switches) and switches once to RS at 9-15% loss, every item in order
(integrity-asserted) with no flapping. The bidirectional handover is driven
deterministically with `--switch-seq 8000:rs,18000:rlc`, which forces
RLC -> RS -> RLC: every item is delivered in order at both 0% and 5% loss,
exercising the RS -> RLC re-sync.

## References

- [Sens-O-Matic / block Reed-Solomon code](../reliable-udp-bridge/) - the
  high-loss code the switch lands on.
- [Sens-O-Matic / RLC code](../sens-rlc/) - the light-loss code it starts on.
- [`QuicBridge`](../quic-bridge/) - shares the one-port endpoint via the
  first-wire-byte demux.
