---
title: "Locale Wire"
weight: 76
---

# WireSocket (Locale::Wire)

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-Linux%204.18%2B-blue)
![Feature](https://img.shields.io/badge/feature-wire--locale-blueviolet)

Userspace NIC access via AF_XDP. The kernel's mainline
userspace-bypass socket family binds a NIC queue to a userspace
ring (UMEM frame buffers + RX/TX rings shared with the kernel),
letting userspace receive and transmit packets without the
standard socket stack traversal.

For the substrate, this is the byte locale where the "other side"
is the NIC hardware via the kernel's AF_XDP plumbing. Bytes flow
producer ring -> AF_XDP TX ring -> NIC, or NIC -> AF_XDP RX ring
-> consumer ring.

Gated behind the `wire-locale` Cargo feature. The AF_XDP layer on this
page is `cfg(target_os = "linux")`; uses libc directly (the libxdp
datapath rides the optional, Linux-target-gated `xsk-rs` crate).

**Cross-platform backends.** The `WireSocket` endpoint
(`bind` / `send_frame` / `recv_frame`) has a per-OS implementation behind
one shared surface: **AF_XDP** on Linux (this page), **XDP** on Windows
(`xdpapi.dll`), **netmap** on FreeBSD (`libnetmap`), and **BPF**
(`/dev/bpf*`) on macOS. `send_frame(&mut self, &[u8]) -> io::Result<()>`
is identical on all four. `recv_frame` is the same shape too, except the
timeout is `i32` on Linux / FreeBSD / macOS and `u32` on Windows. `bind`
differs by platform: Linux / FreeBSD / macOS take `(if_name: &str,
queue_id: u32)`, while the Windows side takes `(if_index: u32, queue_id:
u32, udp_dst_port: u16)` (its redirect rule steers ingress UDP for that
port into the socket). The Windows build also exposes `stats()`,
`list_ethernet_nics()`, and `NicInfo`.

## API

| Constant | Value |
|---|---|
| `AF_XDP` | `44` (from linux/if_xdp.h). |

| Call | Behavior |
|---|---|
| `WireSocket::bind(if_name: &str, queue_id: u32) -> io::Result<Self>` | Open the AF_XDP socket and stand up the full datapath: UMEM frame buffers, the RX / TX / FILL / COMPLETION rings, and the NIC-queue attach for `if_name` queue `queue_id`. |
| `send_frame(&mut self, data: &[u8]) -> io::Result<()>` | Transmit one raw Ethernet frame through the TX ring. |
| `recv_frame(&mut self, out: &mut [u8], timeout_ms: i32) -> io::Result<usize>` | Receive one raw Ethernet frame from the RX ring into `out`; returns the byte count (`0` on timeout). |

`bind` performs the complete setup - there is no separate caller-side
ring-registration step - and `Drop` tears the socket and rings down.

## Required system setup

- Linux 4.18+ for AF_XDP support.
- An interface name + queue id. `bind` uses SKB (generic) mode
  with `XDP_COPY` (`XDP_FLAGS_SKB_MODE` + `BindFlags::XDP_COPY`),
  so it runs on any NIC without native-XDP support AND on a
  `veth` pair - no dedicated/native-XDP NIC and no boot-time
  AF_XDP binding are required. libxdp attaches its default
  redirect program at `bind` time.
- Root, OR `CAP_NET_RAW` + `CAP_BPF`, for the binding step (the
  `bind` rustdoc notes `CAP_BPF`; the module docs note
  `CAP_NET_RAW + CAP_BPF`).

## When to reach for this primitive

- Sub-microsecond NIC ingress / egress where the kernel stack
  is the bottleneck.
- Workloads that pin a NIC queue to a substrate process for
  exclusive access.

## When NOT to reach for this

- Standard cross-host RPC (use
  [`QuicBridge`](../../bridges/quic-bridge/) or
  [`TcpBridge`](../../bridges/tcp-bridge/)).
- Hosts where the process lacks root / `CAP_NET_RAW` + `CAP_BPF`,
  or where the `wire-locale` feature is not enabled.

## References

- AF_XDP kernel docs: <https://www.kernel.org/doc/html/latest/networking/af_xdp.html>
