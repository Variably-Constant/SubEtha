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
[`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/SubEtha/blob/main/docs/TRANSPORT_COMPARISON.md).

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

The sender's identity check is configurable: `connect_tls_named` asserts
a server name of the caller's choosing where `connect_tls` asserts the
fixed `rlc_crypto::SNI`, `rlc_crypto::client_config_trusting` accepts a
CA root (or several leaves) where `client_config` trusts exactly one,
and `rlc_crypto::self_signed_cert_for` issues a cert for chosen names.
The server side needs no name parameter - its identity is its
certificate.

## A TLS listener serving many senders

`listen_tls(local, cfg, tls, peers)` is the listening counterpart to
`bind_tls`: up before any peer dials, serving any number of TLS senders
at once. Each dialing peer runs its own handshake, driven on the demux
thread as its flights arrive, and every delivered item is opened with
that peer's keys and its own packet numbers, tagged with its session id
- `poll_from()` attribution as the [per-peer
contract](#one-window-per-peer) describes, carried down through the
crypto.

`peers` is the concurrent sender count the listener is provisioned for.
Handshakes pending at once are capped at twice it
(`set_pending_handshake_cap` adjusts), a ClientHello past the cap is
refused and counted in `handshake_refusals()`, a handshake that fails
or times out counts in `handshake_failures()`, and an incomplete one
ages out at the 10 s handshake deadline. Data from a peer that has not
completed its handshake is dropped before the decoders see it and
counted in `tls_preauth_dropped()` - to the transport that is link
loss, which its own FEC and ARQ recover once the peer completes - and
an item that cannot be opened is counted in `tls_unopened()` rather
than failing the poll, so one peer's bad frame cannot stall another's
delivery.

The policy must pin a code (`ForceRlc` / `ForceRs`): the switch
boundary is per endpoint, so an automatic switch under several peers
would misdeliver, and `CodePolicy::Auto` is refused at construction
instead.

## What the endpoint's own threads could not send

The receiver's reader thread answers path challenges, sends raw-loss
feedback to every peer it has heard from and retransmits handshake
flights; the sender's reader sends feedback the same way. A datagram
the socket refuses on any of those paths is counted - `send_failures()`
on the receiver, `demux_send_failures()` on the sender - because each
one is a peer left without an answer it was owed, and a count rising
on a quiet link is this socket, not the network. `handshake_failures()`
also covers the single-peer server of `from_shared_tls`, whose
handshake runs on its own thread: a server state that could not be
built, a flight exchange that failed or timed out, or keys that could
not be published each count once, since `poll` withholds delivery
until the keys land and an uncounted failure would read as a peer that
never sends.

Every socket the transports bind asks the kernel for 4 MiB send and
receive buffers. A kernel that refuses - FreeBSD's default
`kern.ipc.maxsockbuf` does - leaves the socket at its default size,
where a send burst can overflow the queue and read as link loss;
`sens_rlc::socket_buffer_refusals()` counts those refusals across the
process.

## A peer that leaves, on Windows

The receiver sends feedback to every peer it has heard from, so a sender
that finishes and drops its socket leaves the receiver addressing a port
nothing holds. That draws an ICMP port-unreachable, and Winsock reports
it to the application as `WSAECONNRESET` on a later `recv_from`: the
recv completes as an error rather than delivering a datagram, and the
datagram it displaced belonged to whichever peer happened to be next.

The endpoint disables that mapping through `SIO_UDP_CONNRESET` on every
socket it binds, so a departure costs the peers still sending nothing.
The cost it removes, measured on the demux socket under load: 712
errors against 773 successful receives, every received datagram routed
and none unroutable, and one of two peers delivering 112 of its 150
items.

The other platforms report an ICMP port-unreachable through `SO_ERROR`
on a connected socket, so an unconnected receiver never has a datagram
displaced by one and the call is a no-op there.

## Peer restart across the shared socket

The endpoint's demux routes by wire byte, and the two path-validation
frames sit outside the contiguous data range, so they are matched by name
alongside it. That is what carries both address migration and the
[replacement-session adoption](../sens-rlc/#surviving-a-peer-restart)
over an endpoint whose codes share one socket.

`take_session_changed()` and `session_adoption_counts()` are surfaced
here with the same meaning they have on the standalone receiver: one
report per admission, and `(admitted, unanswered)` so a refused forgery
is visible as the second rising without the first. Both cover either
code, so an endpoint pinned with `CodePolicy::ForceRs` reports its own
sessions rather than an idle RLC decoder's.

Per-peer reads are forwarded too: `live_rlc_sessions()` and
`live_rs_sessions()` list the ids holding a decode window on each code,
`session_refusals()` counts peers turned away,
`rlc_session_frontier(cid)` reports one RLC window's
`(delivered_through, highest_seen)`, `rlc_session_control(cid)` its
`(naks_sent, acks_sent, sends_skipped, peer_validated)`,
`rlc_session_peer(cid)` the address its control sends target, and
`rlc_path_validations()` / `rlc_path_validation_failures()` sum the
validation outcomes.

The sender carries a telemetry ladder over its own pipeline:
`raw_sent_recv()` (forward datagrams sent vs the receiver's fed-back
count), `rlc_tx_probe()` (`last_sid`, wire datagrams, acked frontier,
outstanding), `route_probe()` (active code, RS pending blocks, items
accepted, send_item entries), `rlc_ctrl_probe()` (NAK / ACK /
FEEDBACK frames the pump processed), `rlc_pump_types()` (challenge
echoes and default-arm drops), `demux_alive()` / `demux_probe()` /
`queue_seam_probe()` (reader-thread liveness, loop counters and the
push/pop seam), and `local_addr()`. Two env-gated stderr traces
exist: `SUBETHA_SEND_TRACE=1` prints each `send_item` entry for the
first 32 calls per sender, and `SUBETHA_WAKE_TRACE=1` prints the
reactor and net-bridge wake ledgers once per second.

Raw-loss feedback goes to **every peer heard from in the last
`FB_PEER_RETENTION` (30 s)**, not only the most recent speaker, so a
sparse sender's loss estimate matures instead of freezing the moment
another peer talks. A peer silent past the window ages out of the set,
which is what bounds both the memory and the outbound feedback to
addresses that recently sent something.

`finish()` drains for `DEFAULT_FINISH_DEADLINE` (120 s); a peer that
died mid-stream holds the caller for that whole window, so
`finish_within(deadline)` takes the caller's own budget and returns
`false` when the deadline passes with items unacked.

A wedged reader is a process that looks healthy and has stopped
hearing the world, so it is never left to be inferred:
`demux_stale_for()` reports how long since the reader last completed a
loop (an idle reader still loops every 100 us, so anything past a few
milliseconds is wedged), `demux_errors()` counts socket errors that
were neither `WouldBlock` nor a timeout, and the reader itself prints
to stderr when it enters or leaves an erroring state, when it panics,
and when it exits without a stop request. On the receiver these
accessors return `None` when an external demux owns the reader, which
is a different answer from "not stale".

A datagram whose first byte no code owns is claimed by no routing arm.
`demux_unroutable()` counts those, and the reader names the source,
the first byte and the length once on stderr, so traffic arriving and
being discarded before any decoder sees it is distinguishable from
traffic that never arrived.

Both codes survive a peer restart, by different means. RLC routes by the
connection id it already carries; block-RS carries a
[session epoch](../reliable-udp-bridge/#surviving-a-peer-restart) in
every data datagram and announces it on the heartbeat. Either way a
window opens only on a challenge answer.

## One window per peer

`poll_from()` returns `(peer, item)` - the RLC connection id, or the
block-RS session epoch widened to `u64` - so a node receiving from
several peers can tell whose item it is holding. `poll()` is the same
drain with the tag dropped, and is unchanged.

Both codes decode a window per peer: RLC by
[connection id](../sens-rlc/#one-window-per-peer), block-RS by
[session epoch](../reliable-udp-bridge/#one-window-per-peer). The
code-switch layer above them does not. The delivery frontier, the switch
boundary and the TLS packet number are per endpoint, since a switch is
negotiated for one connection.

A mesh node therefore pins a code with `CodePolicy::ForceRlc` or
`ForceRs`, leaves TLS off, or drives
[`SensOMaticRlcReceiver`](../sens-rlc/) /
[`ReliableUdpReceiver`](../reliable-udp-bridge/) directly. The block-RS
receiver also needs `with_multi_peer()` to serve more than one peer.

A code switch is not a session change. The connection id belongs to the
process, so RLC <-> RS handover leaves it untouched and no window reset
is triggered by the switch.

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
[`bridge_lan`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
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
