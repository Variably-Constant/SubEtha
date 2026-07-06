# RLC/Sens secure-UDP optimization roadmap

Goal: push the RLC/Sens secure UDP transport (`rlctls`) to the hardware limit -
match or exceed QUIC (quinn) on throughput where the path allows, and decisively
beat it under loss, which is the forward-error-correction transport's reason to
exist. Every lever is wired-or-kept on empirical evidence (syscalls, goodput,
loss, CPU), never theory.

---

## Empirical baseline (measured, real WAN: home Zen3 VM `.213` -> Zen2 VPS, ~22 ms RTT)

| measurement | value | note |
|---|---|---|
| default `rlctls` | 46 Mbit/s | `flow_window=128` caps in-flight at 128 symbols |
| tuned static `flow_window=512` | **172 Mbit/s, 0% loss** | ack-clocked clean ceiling (3.7x) |
| BBR-cwnd auto-tuner | finds btlbw ~400 | overdrives the bufferless path (10-19% loss) |
| quinn (fair: BBR + BDP windows) | ~400 Mbit/s | userspace-paced; the bar to beat |
| GSO TX (loopback probe) | 62x fewer syscalls, 3.6x tput | CPU/syscall win, proven |

Key structural finding: the path is **bufferless** (~512-symbol buffer cliff), so
the 172 -> 400 gap is **loss / pacing-bound, not CPU-bound**. CPU levers (GSO,
SIMD) make the transport more efficient; they do not move the WAN throughput
number. The throughput-vs-quinn gap closes only with precise pacing; the FEC's
real win is on **lossy** paths, where quinn's loss-based control backs off and
forward recovery sustains.

---

## Already in place (no work needed - the architecture was right)

- **Per-packet AEAD framing (the keystone).** `rlc_crypto.rs` seals every DATA
  datagram independently via `rustls::quic` (own packet number, nonce, tag).
  This is the code-then-encrypt, per-coded-packet framing the research
  recommends; it makes GSO/GRO legal and NIC crypto offload possible.
- **FEC GF(2^8) SIMD ladder.** `fec.rs` `GfBackend`: scalar / ssse3 / avx2 /
  avx512-pshufb / gfni256 / gfni512, runtime-dispatched via `available()`. The
  bit-exact `affine-emulated` path verifies GFNI logic where the silicon is
  absent.
- **DgramSock pluggable backend** (Udp / io_uring / Wire) with auto-detect.
- **BBR module** (Startup / Drain / ProbeBw / ProbeRtt, `cwnd_bytes`,
  `pacing_rate_bps`), wired into the in-flight bound under `with_bbr_cwnd`.

Framing fork (encrypt-then-code vs code-then-encrypt): **RESOLVED** - the
existing per-packet AEAD is code-then-encrypt, the recommended choice.

---

## Phase 1 - CPU / syscall efficiency

| lever | status | what | E2E test | deps / hw | expected |
|---|---|---|---|---|---|
| GSO TX (`UDP_SEGMENT`) | proven (probe), integrate next | batch <= floor(65535/payload) fixed-size sealed DATA symbols per `sendmsg` | `rlctls` loopback throughput + CPU%, `strace -c` syscall count, with/without | Linux; any CPU | 62x syscalls, 3.6x CPU-bound tput |
| GRO RX (`UDP_GRO`) | todo | coalesce inbound same-size symbols, segment size returned in cmsg | recv CPU% + `nstat` with/without | Linux; any | matches a GSO sender, lower RX CPU |
| crypto `ring` -> `aws-lc-rs` | todo | VAES multi-buffer AES-GCM | AEAD throughput bench + TLS handshake/data correctness | `aws-lc-rs` (cc/nasm); VAES needs Genoa/Ice Lake+ | 2.6-3.8x AEAD on Genoa |
| `UDP_NO_CHECK6_TX/RX` (IPv6) | todo | skip redundant UDP checksum (AEAD already covers integrity) | per-byte CPU | IPv6 path only | small per-byte win |

GSO integration note: seal each symbol (already per-packet), accumulate up to the
segment cap of equal-size sealed DATA datagrams, flush one `sendmsg` with the
`UDP_SEGMENT` cmsg. Repairs are a different size, so they batch separately (their
own GSO run) or send singly. The flow-control gate (cwnd / ack-clock) decides how
many to batch.

---

## Phase 2 - Socket-config lever module (`udp_levers.rs`)

A probe-and-enable module: detect kernel + NIC features, enable each with a
graceful `Result` fallback so a missing feature degrades, never fails.

| lever | mechanism | E2E test |
|---|---|---|
| RX buffer | `SO_RCVBUF` <= `rmem_max` (raise `rmem_max` first) | `nstat` UdpRcvbufErrors before/after |
| GRO RX | `setsockopt(SOL_UDP, UDP_GRO)` | recv CPU + coalesce cmsg present |
| GSO TX | `setsockopt(SOL_UDP, UDP_SEGMENT)` or per-`sendmsg` cmsg | syscall count |
| Don't-fragment + PMTU | `IP_MTU_DISCOVER=IP_PMTUDISC_PROBE` + userspace DPLPMTUD (RFC 8899) | probe up with padded symbols, fall back on loss |
| HW timestamping | `SO_TIMESTAMPING` (+ `SOF_TIMESTAMPING_OPT_ID`) | ns RTT feeding the FEC margin + pacing |
| 4-tuple symmetric RSS | `ethtool -N <dev> rx-flow-hash udp4 sdfn` + `symmetric-xor` | one connection spreads past one core; both dirs same queue |
| threaded NAPI | `echo 1 > /sys/class/net/<dev>/threaded` | pin NAPI + crypto + decode on a core; remove softirq jitter |
| drop counters | `SO_RXQ_OVFL` | per-datagram drop visibility |

The module probes kernel version + NIC feature flags and reports which levers it
engaged, so a run is self-describing.

---

## Phase 3 - Loss-decorrelation pacing (the WAN goodput lever)

Pace repair (and data) packets so a burst into a tail-drop queue becomes
time-spread independent loss - which is the channel model the RLC code rate is
provisioned for. Without it, correlated burst loss fails the decode even when the
average loss rate is in budget.

- **Kernel path:** `etf` / `taprio` qdisc + `SO_TXTIME` EDT. WARNING: `etf` as
  ROOT qdisc drops all traffic without a valid departure time, including SSH (it
  cut off `.213` once). Apply it on a NON-management interface, or under an
  `mqprio`/`taprio` parent with a pass-through class. Validate with
  `txtime_pace_probe` on `etf` + CLOCK_TAI (the `fq` EDT path dropped packets in
  the VM).
- **Userspace path:** a tight token-bucket pacer + a 1xBDP cwnd (not BBR's 2x).
  quinn proves ~400 Mbit/s is achievable with pure userspace pacing in this VM
  (`gso 1.0x`), so this is the portable fallback when the kernel qdisc is not
  available.

E2E: `rlctls` WAN goodput + `est_loss` + `rlc_recovered` with pacing on/off on a
lossy path; the win is decorrelated loss holding the code rate, not raw tput.

Confirmed dead-ends (do not re-try): `SO_MAX_PACING_RATE` does not pace UDP (no
`sk_pacing_rate` for UDP); `SO_TXTIME` on `fq` drops packets; MWAITX hidden in the
VM.

---

## Phase 4 - SIMD (verify on AVX2 here, measure on Genoa)

- **FEC GFNI:** already dispatches; confirm gfni256 / gfni512 rungs on Genoa via
  `cargo bench --bench fec_gf_simd`. Bit-exact on AVX2 via affine-emulated.
- **Crypto VAES:** via `aws-lc-rs` (Phase 1). Correctness verifiable on AVX2;
  2.6-3.8x speed on Genoa.
- **Multi-buffer AEAD:** seal the coded batch in parallel (fills the vector width,
  the batched-lane pattern) once on `aws-lc-rs`.

---

## Phase 5 - Kernel bypass / steering (advanced)

| lever | what | when |
|---|---|---|
| XDP_DROP pre-crypto | connection-ID / port allowlist + rate-limit at the driver, before an skb is allocated | don't pay AEAD for spoofed / DDoS / non-RLCTLS packets |
| `BPF_PROG_TYPE_SK_LOOKUP` | steer a datagram to the right socket by connection ID (only fires for new / migrated flows) | connection migration survives client IP/port change |
| io_uring multishot recv + registered buffers + `sendmmsg` | one SQE delivers many datagrams; registered buffers skip per-op lookup | syscall-bound ingest (the DgramSock already has io_uring) |
| `BPF_MAP_TYPE_CPUMAP` | XDP hashes connection ID -> redirect to a CPU | software RSS when the NIC can't 4-tuple-hash UDP |
| AF_XDP zero-copy | raw frames in a UMEM ring, RLCTLS framing + GFNI + AEAD in userspace; pin the RLCTLS port via an ethtool flow rule so SSH survives | the pps ceiling; the Wire backend is the start |
| NIC QUIC/crypto offload (QEO) | program per-connection keys; NIC AEADs + header-protects off-CPU | the framing already supports it; mlx5-class NIC |

Not zerocopy for the hot path: `MSG_ZEROCOPY` loses below ~10 KB and our symbols
are ~MTU; in-place AEAD also fights the zerocopy completion race. Use GSO +
`sendmmsg` + io_uring instead.

---

## Phase 6 - Fragmentation avoidance (correctness, not perf)

A coded packet split into 2 IP fragments is lost if either fragment drops -
roughly doubling the effective symbol-loss rate and correlating it, silently
blowing the RLC code-rate margin. So: set DF (`IP_PMTUDISC_PROBE`), run DPLPMTUD
in userspace, and keep `symbol_size <= PMTU - (IP + UDP + RLC header + 16-byte
AEAD tag)`. Never fragment a coded packet.

---

## Phase 7 - The validation (the real "beat QUIC")

Lossy-path matrix: `rlctls` vs `quic` vs `tcptls` under injected and real loss /
jitter / reordering / bufferbloat. This is the FEC's home turf: quinn's
loss-based congestion control collapses under loss, `rlctls`'s forward recovery +
adaptive parity sustain. The honest faster-than-QUIC result lives here, not on a
clean bufferless datacenter uplink. Needs the WAN (reboot `.213` after the `etf`
incident).

---

## Measurement / observability (you cannot push a limit you cannot see)

- `nstat -az`: UdpInErrors, UdpRcvbufErrors, UdpInCsumErrors, UdpNoPorts -
  distinguishes wire loss (a FEC problem) from socket-buffer loss (an `rmem`
  problem). Completely different fixes.
- `SO_RXQ_OVFL`: per-datagram drop counter.
- `strace -c` or an in-process counter: GSO / `sendmmsg` batching factor.
- `SO_TIMESTAMPING` (HW): ns-accurate send/receive for the RTT/jitter estimator
  and EDT pacing.
- CPU% per lever (`perf`, `/proc`): every lever wired-or-kept on CPU evidence.

---

## Test infrastructure

- Probes: `examples/gso_probe.rs` (GSO), `examples/txtime_pace_probe.rs` (EDT).
- `examples/bridge_lan.rs`: the transport matrix, with `--bbr-cwnd`,
  `--flow-window`, `--rlc-step` tuning flags.
- Hosts: VPS `162.35.163.202` (Zen2, no UDP policer, the WAN server); `.213`
  Zen3 (the client - reboot after the `etf` SSH incident); a Genoa host (the
  AVX-512 / VAES / GFNI measurement target).

---

## Cross-platform lever map (the Linux / FreeBSD / Windows constellation)

| concept | Linux | FreeBSD | Windows |
|---|---|---|---|
| RX buffer ceiling | `net.core.rmem_max` | `kern.ipc.maxsockbuf` (+15% padding) | `SO_RCVBUF` (no global ceiling) |
| GSO / GRO | `UDP_SEGMENT` / `UDP_GRO` | partial (driver-dependent) | URO / USO via RIO |
| batched syscalls | `recvmmsg` / `sendmmsg`, io_uring | `recvmmsg` / `sendmmsg` | RIO (Registered I/O) |
| steering | RSS / RPS / RFS | RSS (`net.inet.rss.*`) | RSS + RSC |
| kernel bypass | AF_XDP | netmap | RIO / DPDK |
| busy poll | `SO_BUSY_POLL` | kqueue + polling | RIO completion polling |

---

## The keystone synergy

One framing decision - fixed-size, per-wire-packet-sealed coded symbols at or
below PMTU, already in place - simultaneously makes GSO/GRO batching legal, makes
pacing decorrelate loss cleanly, makes NIC crypto offload possible, and avoids
fragmentation. The framing is the keystone; every kernel lever above is what it
unlocks.
