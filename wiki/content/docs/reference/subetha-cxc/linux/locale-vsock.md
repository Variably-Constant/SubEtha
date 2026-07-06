---
title: "Locale Vsock"
weight: 74
---

# VsockSocket

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-Linux-blue)
![Family](https://img.shields.io/badge/family-AF__VSOCK-green)

Linux `vsock(7)` helper for host-VM byte streaming. vsock is a
Linux socket family for guest-host communication within a VM
environment; it bypasses the network stack entirely. The
hypervisor (KVM, Hyper-V) forwards bytes between guest and host
via a shared transport layer.

For the substrate, this is the "remote-but-in-same-machine"
locale that sits between QUIC (cross-host, full network stack)
and ShmFs (same-host, shared memory).

## API

| Constant | Value | Meaning |
|---|---|---|
| `VMADDR_CID_ANY` | `0xFFFFFFFF` | Bind to any CID. |
| `VMADDR_CID_HOST` | `2` | Special CID for the host. |
| `AF_VSOCK` (from libc) | (kernel-defined) | Socket family. |

| Call | Behavior |
|---|---|
| `VsockSocket::new() -> io::Result<Self>` | `socket(AF_VSOCK, SOCK_STREAM, 0)`. |
| `sock.bind(cid, port) -> io::Result<()>` | Bind to (cid, port). |
| `sock.listen(backlog: i32)` | Mark as listening. |
| `sock.accept() -> io::Result<VsockSocket>` | Accept one incoming connection. |
| `sock.connect(cid, port)` | Connect to (cid, port). |
| `sock.send(buf: &[u8]) -> io::Result<usize>` | Blocking send. |
| `sock.recv(buf: &mut [u8]) -> io::Result<usize>` | Blocking recv. |

Implements `AsRawFd` + `FromRawFd`. Drop closes the fd.

## Worked sketch

```rust,no_run
use subetha_cxc::locale_vsock::{VsockSocket, VMADDR_CID_HOST, VMADDR_CID_ANY};

// Guest -> host:
let sock = VsockSocket::new()?;
sock.connect(VMADDR_CID_HOST, 9000)?;
sock.send(b"hello from guest")?;

// Host server:
let server = VsockSocket::new()?;
server.bind(VMADDR_CID_ANY, 9000)?;
server.listen(8)?;
let conn = server.accept()?;
let mut buf = [0u8; 64];
let n = conn.recv(&mut buf)?;
# Ok::<(), std::io::Error>(())
```

## When to reach for this primitive

- Host-VM IPC where the network stack adds unnecessary latency
  + complexity.
- Per-VM substrate endpoints (one substrate process on the host,
  one in each guest, talking via vsock).

## When NOT to reach for this

- Cross-host (use [`QuicBridge`](../../bridges/quic-bridge/) or
  [`TcpBridge`](../../bridges/tcp-bridge/)).
- Same-host different-process (use
  [`LocaleAdaptiveRing`](../../rings/locale-adaptive-ring/) with
  the File or ShmFs locale).
