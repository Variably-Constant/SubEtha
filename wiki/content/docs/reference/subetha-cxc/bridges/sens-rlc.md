---
title: "Sens-O-Matic / RLC code"
weight: 65
---

# Sens-O-Matic, sliding-window RLC code: SensOMaticRlcSender + SensOMaticRlcReceiver

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Feature](https://img.shields.io/badge/deps-std%20%2B%20optional%20tls-brightgreen)
![Transport](https://img.shields.io/badge/transport-reliable%20UDP-green)
![Encryption](https://img.shields.io/badge/encryption-optional%20TLS%201.3-blue)

The second erasure code of the [Sens-O-Matic](../reliable-udp-bridge/)
protocol. Where the block Reed-Solomon code groups items into blocks of `k`
source + `r` parity shards, the **sliding-window Random Linear Code**
([`rlc_fec`](../../../)) ships items as source symbols interleaved with
RLC repair symbols and recovers a lost symbol from the *next* repair -
rather than waiting for the rest of a block, which is the lower-latency
recovery shape on a streaming workload. The two codes share the GF(2^8)
field and the committed SIMD multiply ladder; this one lives in
[`sens_rlc`](../../../), types `SensOMaticRlcSender` /
`SensOMaticRlcReceiver`.

It adds two things the block-RS code does not have: an **adaptive
controller** and **optional TLS 1.3**.

## Adaptive coding from the control plane

A sensing controller ([`RlcController`](../../../)) retunes the live coding
on every feedback frame from what the receiver measures: the window size
(how far back a repair reaches - sized to the burst length), the repair
cadence (code rate - sized to the loss rate), the coefficient density, and
whether to code at all (disable-on-clean reclaims the parity overhead on a
provably-clean link). Protection escalates immediately and relaxes only
after a sustained quiet run, so a loss spike is covered the instant it
appears.

## Packet-pair rate control for a variable path

On a bufferless internet path the usable rate sits below the raw link rate
at a sharp cliff, and every loss-derived rate signal (goodput, NAKs, a
congestion classifier) is confounded - random loss looks identical to a
cliff overshoot. The sender's `with_adaptive_push` mode measures the path
capacity from **packet-pair dispersion**: the bottleneck spaces two
back-to-back packets by its per-packet transmission time regardless of how
many *other* packets it drops, so the gap measures capacity independently
of loss. The sender cruises just under the measured capacity and lets the
FEC cover the residual loss, rather than probing into the cliff.

## One window per peer

The connection id identifies the SESSION, and the receiver keeps a
decode window per id: its own delivery frontier, loss estimate, gap
tracking and path validation. A node receiving from several peers at
once - a replication mesh, where every member ships to every other -
decodes each stream independently, so nothing one peer sends can stall
or displace another's.

That is also what makes a restart ordinary rather than special. A
restarted process draws a new id, so it opens a new window; the dead
session's window simply goes quiet. There is no replacement step,
because a session is never rebound to a different id.

`poll_from()` returns `(connection_id, item)` and is the call a
multi-peer node wants. `poll()` is the same drain with the tag
dropped. Ordering is guaranteed WITHIN a connection id; nothing orders
one peer against another, since they are independent streams.
`live_sessions()` lists the ids with a window.

## Admitting a peer

The first id a receiver sees is admitted outright: there is no
established session for a forgery to disturb, and that is the ordinary
point-to-point case. Every id after that is challenged. A datagram
carrying an unfamiliar id arms a `PATH_CHALLENGE` with that id and a
fresh nonce, and the window opens only when the nonce returns from the
address it was sent to. The provoking datagram is not delivered
meanwhile, and an unanswered challenge is retired after
`CHALLENGE_TIMEOUT`. A pending challenge is resent at most once per
`ADMISSION_RESEND_INTERVAL` (50 ms), and the unified sender's reader
thread echoes challenges at wire latency, so admission completes in
one round trip regardless of how often the sender services its
socket.

Admission is gated on the answer rather than on the id alone because
the two are indistinguishable from a datagram: without the challenge,
anyone able to guess the 4-tuple could make a receiver allocate a
decode window with a single packet. What the answer proves is
return-routability - the responder echoes the challenge without
inspecting the id - which is what an off-path attacker cannot supply.

There is no ceiling on windows unless one is declared.
`with_session_ceiling(max)` bounds the live windows and the candidates
under challenge, and `session_refusals()` counts every peer turned away
by it, so a refused peer is never indistinguishable from one that never
sent.

A newly admitted window anchors its delivery frontier at the bottom
rather than at the first id it happens to see. The datagrams the peer
sent during the challenge round trip were not delivered, so anchoring
where the stream *is* would skip them silently; anchoring at the bottom
leaves them as a gap ARQ recovers.

| Call | Answers |
|---|---|
| `take_session_changed()` | whether a window was admitted since the last call; edge-triggered |
| `session_adoption_counts()` | `(admitted, challenges_that_went_unanswered)`; a refused forgery raises the second without the first |
| `session_admissions_for(cid)` | the address that id answered from, or `None` if it holds no window |
| `peer_of(cid)` / `live_sessions()` | where one peer is bound, and which ids are live |
| `session_refusals()` | peers turned away by a declared ceiling, or by TLS already holding its one handshake |
| `session_frontier(cid)` | one window's `(delivered_through, highest_seen)`; `highest_seen` ahead of `delivered_through` is a window holding frames behind a gap |
| `session_control(cid)` | one window's `(naks_sent, acks_sent, sends_skipped, peer_validated)`; `sends_skipped` counts control frames dropped for a missing peer address or an exhausted anti-amplification budget |
| `path_validations()` / `path_validation_failures()` | address validations completed and challenges that timed out, summed over every session |

A session's control-plane send failure stays with that session: every
window is serviced each poll, and a tick's delivered items reach the
caller whether or not some peer's feedback send failed.

An ACK leaves immediately whenever the delivery frontier advanced; an
unmoved frontier re-acks at most once per 10 ms. The cumulative
frontier plus the SACK bitmap carries the full receive state, so each
ACK supersedes every prior one.

`take_session_changed()`, `session_adoption_counts()` and
`poll_from()` are also on `UnifiedSensReceiver`, whose
[multi-peer contract](../unified-code-switch/#one-window-per-peer)
stops at delivery.

## Optional TLS 1.3

`with_tls_client` / `with_tls_server` wrap the transport in a rustls TLS
1.3 handshake (driven over the transport's own reliable Crypto-frame
exchange) and seal every datagram with the 1-RTT key. FEC stays over the
cleartext; the AEAD is per-packet with no extra round trips, so the
encrypted path's latency matches the plaintext path's to within
microseconds. This is what the head-to-head calls `rlctls`.

## Performance

Measured on real wire between separate OS processes (an Ubuntu 24.04 and a
FreeBSD 15.0 host) and over a real ~22 ms internet path - full matrix,
confidence intervals, and methodology in
[`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/SubEtha/blob/main/docs/TRANSPORT_COMPARISON.md).
On the clean LAN the RLC code moves ~890 Mbit/s (FEC parity is the gap to
the raw stream bridges). Under loss it holds where the TCP bridges collapse:
**~870 Mbit/s at 3% LAN loss and ~550 at 8%, versus ~115 and ~10 for the TCP
bridges**, with a ~30 ms p99 round-trip against their 204-254 ms (a lost TCP
segment head-of-line-blocks the whole stream; the RLC code recovers in-band).
Over the real internet it holds ~260 Mbit/s through 3-8% loss where
`TcpTlsBridge` collapses to single digits, statistically tied with QUIC at
0-5% loss.

## Verify

```rust
use std::time::Duration;
use subetha_cxc::sens_rlc::{SensOMaticRlcReceiver, SensOMaticRlcSender};

const SYMBOL_LEN: usize = 64;

// Receiver on a loopback port, 15% injected loss so the RLC repairs engage.
let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", SYMBOL_LEN)?
    .with_debug_loss(15, 7);
let addr = recv.local_addr()?;

let rx = std::thread::spawn(move || {
    let mut got = Vec::new();
    while got.len() < 1000 {
        for item in recv.poll().unwrap() {
            got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
        }
    }
    got
});

// Sender: window 16, one repair per 2 source symbols, dense coefficients.
let mut send = SensOMaticRlcSender::bind("127.0.0.1:0", addr, 16, 2, 15, SYMBOL_LEN)?;
for i in 0..1000u64 {
    send.send_item(&i.to_le_bytes())?;
}
send.drain_until_acked(1000, Duration::from_secs(15))?;

assert_eq!(rx.join().unwrap(), (0..1000).collect::<Vec<_>>());
# Ok::<(), std::io::Error>(())
```

The full transport (TLS, packet-pair rate control, migration) runs in
[`rlc_transport_e2e`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/rlc_transport_e2e.rs)
and the cross-host head-to-head in
[`bridge_lan`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
(`--transport sens --fec rlc --tls`).

## References

- [Sens-O-Matic / block Reed-Solomon code](../reliable-udp-bridge/) - the
  MDS, `std`-only sibling code of the same protocol.
- [`QuicBridge`](../quic-bridge/) - the ARQ-based encrypted transport it is
  measured against.
