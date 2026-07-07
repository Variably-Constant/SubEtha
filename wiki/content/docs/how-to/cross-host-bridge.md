---
title: "Bridge two hosts (QUIC / TCP / Sens-O-Matic)"
weight: 65
---

# Bridge two hosts over QUIC, TCP, or Sens-O-Matic

The full recipe for connecting rings on two machines: certificate
generation and shipping for the encrypted transports, the firewall
step per OS, the run commands, and the verification numbers you
should expect. The driver throughout is `examples/bridge_lan.rs`,
which is also how the project's own cross-host numbers were measured.

## Pick a bridge

For an untrusted or lossy real-world WAN, reach for **Sens-O-Matic**: it
is encrypted (TLS 1.3 on both erasure codes) and auto-switches RLC <-> RS
on measured loss, holding throughput and a bounded latency tail across
the whole loss range. `QuicBridge` is the alternative when you want
QUIC's stream multiplexing / migration / 0-RTT; the TCP bridges are for
trusted links.

| Bridge | Wire | Encrypted | Idle CPU | Reach for it when |
|---|---|---|---|---|
| [Sens-O-Matic (unified)](../reference/subetha-cxc/bridges/unified-code-switch.md) | reliable-UDP adaptive FEC (RLC <-> RS) | none, or TLS 1.3 on **both codes** | polling forwarder | **the default for untrusted or lossy WAN**: forward error correction holds throughput and a bounded latency tail across the whole loss range, auto-switching codes at the ~15% crossover, where the TCP bridges collapse and QUIC's single recovery path degrades |
| `QuicBridge` | QUIC over UDP | TLS 1.3 (rustls) | polling forwarder | you want QUIC specifically: multiplexed streams, connection migration, 0-RTT, or interop with a QUIC peer |
| `TcpTlsBridge` | TCP + rustls record layer | TLS 1.3 (rustls) | polling forwarder | encrypted TCP on a clean untrusted link where QUIC's dependency footprint is unwanted |
| `TcpBridge` | TCP | no | polling forwarder | trusted LAN, lowest latency (loopback RTT matches a bare socket) |
| `BlockingTcpBridge` | TCP | no | zero (parked waker) | trusted LAN, idle links that must not burn CPU |

The stream bridges (`QuicBridge` / `TcpTlsBridge` / `TcpBridge` /
`BlockingTcpBridge`) ship 64-byte ring slots with burst-batched egress
(256 slots per socket write) and chunked ingress; Sens-O-Matic ships
MTU-sized forward-error-corrected datagrams instead. Integrity (order,
count, payload sum) is asserted by the receiving app in the example for
all of them. Full per-transport throughput / latency / under-loss
numbers are in
[the transport comparison](../reference/subetha-cxc/bridges/_index.md).

## Step 0: build the driver on both hosts

```bash
cargo build --release -p subetha-cxc --example bridge_lan \
    --features quic-bridge,tcp-bridge
```

## Step 1 (QUIC only): generate and ship the certificate

On either host:

```bash
bridge_lan --gen-cert /tmp/cert.der /tmp/key.der
```

Ship BOTH files to the server host and `cert.der` (only) to every
client host - any channel you trust for key material. The
certificate names the SNI string `subetha-lan`, not an IP address,
so the same pair works for any addresses; the client passes that
SNI implicitly through `make_client_config_from_der`.

TCP bridges skip this step entirely.

## Step 2: open the firewall on the server host

The server side binds a listening port (`7401` in the examples
below; pick your own).

{{< tabs >}}

{{< tab name="Windows" >}}
Windows prompts on first bind, or add the rule directly
(administrator PowerShell):

```powershell
New-NetFirewallRule -DisplayName "subetha bridge" -Direction Inbound `
    -Program "C:\path\to\bridge_lan.exe" -Action Allow
```
{{< /tab >}}

{{< tab name="Linux" >}}
```bash
sudo ufw allow 7401/tcp   # TCP bridges
sudo ufw allow 7401/udp   # QUIC
```
(or the equivalent in firewalld / nftables; stock cloud images
often have no local firewall at all.)
{{< /tab >}}

{{< tab name="FreeBSD" >}}
If `pf` is enabled, add to `/etc/pf.conf` and reload:

```text
pass in proto tcp to port 7401
pass in proto udp to port 7401
```
A default FreeBSD install ships with pf not loaded, in which case
there is nothing to do.
{{< /tab >}}

{{< /tabs >}}

## Step 3: run a one-way integrity stream

Server host (receives, asserts order + count + sum):

```bash
bridge_lan --transport tcp --role server --bind 0.0.0.0:7401 --items 1000000
```

Client host (ships 1,000,000 sequenced 64-byte slots):

```bash
bridge_lan --transport tcp --role client \
    --connect <server-ip>:7401 --items 1000000
```

For QUIC, add `--cert /tmp/cert.der --key /tmp/key.der` on the
server and `--cert /tmp/cert.der` on the client. Both sides print
a `RESULT` line; the server's carries `order_ok=true sum_ok=true`
when every slot arrived intact and in order.

## Step 4: round-trip latency

Both roles bind a server AND connect a client (each direction has
its own socket), print `BOUND` once their listener is up, and wait
for a newline on stdin before connecting - start both, then press
Enter in each terminal (or drive them from a script):

```bash
# Host A
bridge_lan --transport tcp --role pong --bind 0.0.0.0:7402 \
    --connect <hostB>:7401 --rounds 2000
# Host B
bridge_lan --transport tcp --role ping --bind 0.0.0.0:7401 \
    --connect <hostA>:7402 --rounds 2000
```

The ping side prints min / avg / p50 / p99 / max for the full
chain: app ring, bridge, wire, remote ring, echo, and back.

## What to expect

Numbers from the project's measured runs
([`docs/TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/SubEtha/blob/main/docs/TRANSPORT_COMPARISON.md), the Ubuntu <-> FreeBSD virtio matrix):

- **LAN RTT p50** (real wire, Ubuntu <-> FreeBSD over virtio NICs):
  the TCP bridges ~244 us, QUIC ~287 us, blocking TCP ~348 us, the
  Sens-O-Matic codes ~290-400 us - the bridges sit on the link's own
  round-trip.
- **Clean throughput** (LAN goodput): the TCP/TLS stream bridges
  ~1230-1382 Mbit/s, the Sens-O-Matic codes ~800-900, raw `udp` 1821
  (but only ~36% delivered - no flow control).
- **Under loss the order inverts**: the TCP bridges collapse to
  ~10-115 Mbit/s and their p99 round-trip blows out to 200+ ms
  (head-of-line blocking), while QUIC and both Sens-O-Matic codes hold
  throughput and Sens-O-Matic holds a 1.6-30 ms tail. Full matrix and
  confidence intervals in
  [`TRANSPORT_COMPARISON.md`](https://github.com/Variably-Constant/SubEtha/blob/main/docs/TRANSPORT_COMPARISON.md).

If your one-way run reports `order_ok=false` or a count mismatch,
the bridge contract is broken and that is a bug worth filing, not
a tuning problem: every shipped configuration of the three bridges
passes these assertions on Windows, Linux, and FreeBSD.

## Wiring it into your application

The example's plumbing is exactly the production API surface:

- attach an `AdaptiveRing` (or `BlockingSpscRing` for btcp) on
  each host,
- hand the outbound ring to `TcpBridgeClient` /
  `QuicBridgeClient::new` and the inbound ring to the matching
  server half,
- your processes keep using plain ring sends and recvs; the bridge
  is just another MMF participant that happens to own a socket.

Per-bridge construction details and the full API tables:
[QUIC](../reference/subetha-cxc/bridges/quic-bridge.md),
[TCP](../reference/subetha-cxc/bridges/tcp-bridge.md),
[Blocking TCP](../reference/subetha-cxc/bridges/blocking-tcp-bridge.md).
Linux socket knobs (`SO_BUSY_POLL` and friends):
[tuning and overrides](tuning-overrides.md).
