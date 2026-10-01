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

All are read once per process at first use and cached, so a variable
set from inside the process has to be set before SubEtha first reads
it: before the first structure is made, and in Python and PowerShell
before the package or module is loaded.

{{< tabs >}}

{{< tab name="Rust" >}}
```bash
SUBETHA_WAIT_CALIBRATION=off cargo run --release
```
{{< /tab >}}

{{< tab name="Python" >}}
```python
import os

os.environ["SUBETHA_WAIT_CALIBRATION"] = "off"

import subetha
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$env:SUBETHA_WAIT_CALIBRATION = 'off'
Import-Module SubEtha
```
{{< /tab >}}

{{< /tabs >}}

| Variable | Effect | Default | When to set it |
|---|---|---|---|
| `SUBETHA_WAIT_CALIBRATION=off` | No background calibration, no measurement and no cache: every blocking wait keeps 0.5.1's fixed ladder. `on` calibrates. | `on` | A process that must not spend the calibration's background processor time, or an A/B against the fixed ladder. |
| `SUBETHA_NO_MONITOR_WAIT=1` | Removes the hardware monitor-wait phase (`MONITORX`/`MWAITX`, `UMONITOR`/`UMWAIT`, `LDAXR`+`WFE`) from every wait, and no monitor instruction runs, calibration included. A calibrated wait spins to its break-even and parks; the fixed ladder spins its rounds and parks. | phase present where the CPU reports a family | A/B measurement, or a host whose hypervisor advertises the CPUID bit but mishandles the instruction. |
| `SUBETHA_MONITOR_WAIT_CYCLES=<n>` | The monitor phase's length in counter ticks, in the calibrated plan and the fixed ladder alike. | the calibrated plan's; in the fixed ladder ~90,000 TSC cycles on x86-64 (about 28 us), the same window from `CNTFRQ_EL0` on aarch64 | Lengthen on hosts where kernel parks are unusually expensive; shorten when waits should yield the core sooner. |
| `SUBETHA_PRE_PARK_SPIN=<rounds>` | The spin phase's rounds before the monitor phase or the park, in every blocking wait; `0` skips the spin. | the calibrated plan's; 32 in the fixed ladder | A/B measurement; with `SUBETHA_NO_MONITOR_WAIT=1`, `0` gives a pure park. |
| `SUBETHA_NO_MMF_WARM=1` | Disables prefaulting at MMF attach everywhere. | warm enabled on Linux (`MADV_POPULATE_WRITE`) and FreeBSD (`MADV_WILLNEED`) | A/B measurement of attach-time vs first-traffic fault cost. |
| `SUBETHA_MMF_WARM=1` | Forces `PrefetchVirtualMemory` warm-up on Windows, where the automatic path is off (measured pure overhead on page-cache-hot backings). | off on Windows | Re-opening large persistent rings whose pages are cold on disk: one large batched I/O beats per-page demand faults. |
| `SUBETHA_BUSY_POLL_US=<n>` | Sets `SO_BUSY_POLL` on bridge TCP sockets (Linux only): the kernel busy-polls the NIC queue for `n` microseconds before sleeping. | unset (no busy poll) | Latency-critical bridge links on NICs with NAPI support, where trading CPU for tail latency is the right call. |

The four wait settings steer the
[wait calibration](../../reference/subetha-cxc/coordination-types/wait-calibration/);
a value they cannot use is ignored and named, with the reason, by
`subetha_cxc::wait_active::ignored_settings()`.

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
| `SUBETHA_DGRAM=iouring\|udp\|wire` | Forces the datagram backend instead of auto-detecting. `iouring` forces the io_uring ring (warns if unavailable rather than silently degrading); `wire` forces the AF_XDP / netmap Wire backend regardless of the link-speed gate. On the io_uring backend a receive that failed and could not be re-armed because the submission queue was full leaves a slot the ring no longer fills; `DgramSock::rearm_failures()` counts those, zero on every other backend. | auto: io_uring on Linux, plain UDP fallback |
| `SUBETHA_USO=0` | Disables the UDP segmentation-offload (USO) send path, forcing the per-datagram baseline. | on (Linux) |
| `SUBETHA_GRO=0` | Disables the GRO receive-coalescing path, keeping the per-datagram `recvmmsg` path. | on (Linux) |
| `SUBETHA_REORDER_GUARD=0` | Disables the D-SACK reorder-guard subtraction in the reliable-UDP loss estimate. | on |
| `SUBETHA_PAIR_FRACTION=<f>` | Packet-pair pacing fraction for the Sens-O-Matic auto-tuner; clamped to `[0.30, 0.78]` so it can never target the loss cliff. | controller-derived |
| `SUBETHA_WIRE_IFNAME=<name>` | NIC name (Linux) or netmap port spec for the Wire backend. | unset |
| `SUBETHA_WIRE_MIN_GBPS=<n>` | Link-speed gate (Gbit/s) at or above which the Wire backend engages; `0` disables the gate. | 10 |
| `SUBETHA_WIRE_LOCAL_IP` / `SUBETHA_WIRE_LOCAL_MAC` / `SUBETHA_WIRE_PEER_MAC` | Wire-backend L2/L3 addressing for the AF_XDP / netmap path. | unset |

Three diagnostic-only switches log decisions to stderr and change no
behavior: `SUBETHA_FEC_DEBUG` (each Sens-O-Matic coding-parameter
decision), `SUBETHA_PAIR_DEBUG` (each packet-pair id-gap sample) and
`SUBETHA_RING_DEBUG` (each `AdaptiveRing` shape transition, naming the
shape left, the shape taken and the walked set the morph leaves behind).

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

The bindings are built with their features already chosen. The
default Python wheel leaves the bridges out, and `subetha.transports`
lists the ones a wheel was built with. The PowerShell module always
carries the TCP and QUIC bridges, since a module ships as one
artifact with no way to ask for an extra.

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
| Wait plan | the spin, monitor and park lengths a blocking wait takes: calibrated for this host, core class and load band, or 0.5.1's fixed ladder until measured | `subetha_cxc::wait_active::active_plan(kind)`, `subetha_cxc::wait_calibration::report()` |
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
