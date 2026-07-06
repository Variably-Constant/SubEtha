---
title: "Tuning and overrides"
weight: 70
---

# Tuning and overrides

Every runtime knob, environment override, Cargo feature, and build
recipe the substrate exposes, in one place. The defaults are the
measured-best configuration on every platform tested; each override
exists for a documented reason, listed with it.

## Environment variables

All are read once per process at first use and cached.

| Variable | Effect | Default | When to set it |
|---|---|---|---|
| `SUBETHA_NO_MONITOR_WAIT=1` | Disables the hardware monitor-wait tier (`MONITORX`/`MWAITX`, `UMONITOR`/`UMWAIT`, `LDAXR`+`WFE`). Waits go straight from spin to the kernel park. | tier enabled where the CPU reports it | A/B measurement, or a host whose hypervisor advertises the CPUID bit but mishandles the instruction. |
| `SUBETHA_MONITOR_WAIT_CYCLES=<n>` | Per-wait monitor budget in counter ticks before escalating to the kernel park. | ~90,000 TSC cycles on x86-64 (about 28 us); derived from `CNTFRQ_EL0` for the same window on aarch64 | Lengthen on hosts where kernel parks are unusually expensive; shorten when waits should yield the core sooner. |
| `SUBETHA_NO_MMF_WARM=1` | Disables prefaulting at MMF attach everywhere. | warm enabled on Linux (`MADV_POPULATE_WRITE`) and FreeBSD (`MADV_WILLNEED`) | A/B measurement of attach-time vs first-traffic fault cost. |
| `SUBETHA_MMF_WARM=1` | Forces `PrefetchVirtualMemory` warm-up on Windows, where the automatic path is off (measured pure overhead on page-cache-hot backings). | off on Windows | Re-opening large persistent rings whose pages are cold on disk: one large batched I/O beats per-page demand faults. |
| `SUBETHA_BUSY_POLL_US=<n>` | Sets `SO_BUSY_POLL` on bridge TCP sockets (Linux only): the kernel busy-polls the NIC queue for `n` microseconds before sleeping. | unset (no busy poll) | Latency-critical bridge links on NICs with NAPI support, where trading CPU for tail latency is the right call. |

One bench-side variable, not read by the library:
`SUBETHA_COMPARE_FILE=1` switches `examples/cross_process_compare.rs`
to real-file ring backings instead of shared-memory sections, to
measure the NTFS file-backed mapping penalty on Windows
(~1.5-2.7 us one-way against ~100-400 ns section-backed).

### Datagram / Wire transport overrides

These tune the UDP-bridge / reliable-UDP / Wire datagram layer; same
once-per-process read-and-cache discipline as the substrate variables
above.

| Variable | Effect | Default |
|---|---|---|
| `SUBETHA_DGRAM=iouring\|udp\|wire` | Forces the datagram backend instead of auto-detecting. `iouring` forces the io_uring ring (warns if unavailable rather than silently degrading); `wire` forces the AF_XDP / netmap Wire backend regardless of the link-speed gate. | auto: io_uring on Linux, plain UDP fallback |
| `SUBETHA_USO=0` | Disables the UDP segmentation-offload (USO) send path, forcing the per-datagram baseline. | on (Linux) |
| `SUBETHA_GRO=0` | Disables the GRO receive-coalescing path, keeping the per-datagram `recvmmsg` path. | on (Linux) |
| `SUBETHA_REORDER_GUARD=0` | Disables the D-SACK reorder-guard subtraction in the reliable-UDP loss estimate. | on |
| `SUBETHA_PAIR_FRACTION=<f>` | Packet-pair pacing fraction for the Sens-O-Matic auto-tuner; clamped to `[0.30, 0.78]` so it can never target the loss cliff. | controller-derived |
| `SUBETHA_WIRE_IFNAME=<name>` | NIC name (Linux) or netmap port spec for the Wire backend. | unset |
| `SUBETHA_WIRE_MIN_GBPS=<n>` | Link-speed gate (Gbit/s) at or above which the Wire backend engages; `0` disables the gate. | 10 |
| `SUBETHA_WIRE_LOCAL_IP` / `SUBETHA_WIRE_LOCAL_MAC` / `SUBETHA_WIRE_PEER_MAC` | Wire-backend L2/L3 addressing for the AF_XDP / netmap path. | unset |

Two diagnostic-only switches log decisions to stderr and change no
behaviour: `SUBETHA_FEC_DEBUG` (each Sens-O-Matic coding-parameter
decision) and `SUBETHA_PAIR_DEBUG` (each packet-pair id-gap sample).

## Cargo features

| Feature | Pulls in | Gives you |
|---|---|---|
| `quic-bridge` | quinn, rcgen, rustls, tokio | `QuicBridgeClient` / `QuicBridgeServer` |
| `tcp-bridge` | tokio (net, io-util, rt) | `TcpBridgeClient` / `TcpBridgeServer`, `BlockingTcpBridge*` |
| `tls` | rustls, rcgen | TLS 1.3 record-layer primitives consumed by `tcp-tls-bridge` and the Sens-O-Matic RLC TLS path |
| `tcp-tls-bridge` | `tcp-bridge` + `tls` + tokio-rustls | `TcpTlsBridgeClient` / `TcpTlsBridgeServer` (TLS 1.3 over TCP) |
| `linux-futex-raw` | nothing extra (Linux only) | direct futex syscall surface on `CrossProcessWaker` |
| `wire-locale` | xsk-rs (Linux only) | `WireSocket` AF_XDP wire locale |
| `zmq-bench` | zmq (links C libzmq) | the ZeroMQ comparison arm in `examples/cross_process_compare.rs` (dev / bench only) |

The default feature set is empty: the core substrate compiles with
no optional dependencies on every supported target.

## Build recipes

| Recipe | What it does | Measured effect |
|---|---|---|
| `RUSTFLAGS=-Ctarget-cpu=x86-64-v3 cargo build --release` | Release build against `x86-64-v3` (AVX2 + BMI2 + FMA assumed) | +22% TCP bridge loopback throughput on a v3-capable Zen+ host |
| PGO (rustc `-Cprofile-generate` -> train on the loopback bridge workload -> `llvm-profdata merge` -> `-Cprofile-use`) | Full profile-guided instrument / train / optimize cycle | +38% TCP bridge loopback throughput (2,387 to 3,286 Mbit/s) |

The default build stays baseline `x86-64`: wide-register kernels
dispatch at runtime behind CPUID probes, so a baseline binary still
uses AVX2/AVX-512 where the silicon has it.

## Runtime probes (no override needed)

These select themselves per host and are listed so you know what
the substrate decided and where to check:

| Probe | Selects | Inspect via |
|---|---|---|
| Monitor-wait family | WAITPKG, then MWAITX, then WFE on aarch64; `None` when hidden (hypervisors often hide the CPUID bits) | `subetha_cxc::monitor_wait_kind()` |
| Invariant TSC (CPUID `0x8000_0007` EDX bit 8) | `StampKind::Tsc` ordering stamps; falls back to `SharedCounter` / `Monotonic` | `subetha_cxc::has_invariant_tsc()` |
| CLDEMOTE (CPUID `7.0` ECX bit 25) | diagnostic only - the instruction is emitted unconditionally and is an architectural NOP where unsupported | `subetha_cxc::has_cldemote()` |

## QoS-level knobs

Ordering, shape, capacity, and locale are declared per ring through
[`QosPolicy`](../../reference/subetha-cxc/coordination-types/qos-policy/)
and the adaptive constructors rather than environment variables; the
[adaptive-ordering page](../../reference/subetha-cxc/rings/adaptive-ordering/)
covers the ordering axis (including `auto_order(threshold)`
pre-authorization) and the
[polymorphic-substrate notes](../../reference/subetha-cxc/) map the
rest.
