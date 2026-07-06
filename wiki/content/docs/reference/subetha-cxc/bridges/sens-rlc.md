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
[`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/TRANSPORT_COMPARISON.md).
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
[`rlc_transport_e2e`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/rlc_transport_e2e.rs)
and the cross-host head-to-head in
[`bridge_lan`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/bridge_lan.rs)
(`--transport sens --fec rlc --tls`).

## References

- [Sens-O-Matic / block Reed-Solomon code](../reliable-udp-bridge/) - the
  MDS, `std`-only sibling code of the same protocol.
- [`QuicBridge`](../quic-bridge/) - the ARQ-based encrypted transport it is
  measured against.
