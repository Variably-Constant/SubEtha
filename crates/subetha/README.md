# subetha

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE-MIT)
[![Wiki](https://img.shields.io/badge/wiki-variably--constant.github.io-blue)](https://variably-constant.github.io/SubEtha/)

**Cross-Context Channel (CXC) for Rust.** Kernel-bypass IPC - lock-free,
thread- and process-safe - that spans threads, processes, disk, and
network through memory-mapped files, with no syscalls on the data path
and no locks in your code.

Local IPC normally means picking the least-bad option from a menu that
all goes through the kernel. SubEtha skips the menu: after construction,
every send and recv is a user-space atomic op on a memory-mapped file
the kernel page-aliases between participants. The same typed
`Channel<T>` API works cross-thread, cross-process, and persisted to
disk; the default ring picks and changes its own shape (SPSC to MPMC and
back) under live producers and consumers without losing an item.

Measured 80-528x faster than the fastest kernel IPC mechanism on every
platform tested (Windows, WSL2, Linux, FreeBSD, macOS) and 4.8-8.8x
faster than iceoryx2's zero-copy shared memory. The full benchmark set,
dot plots, and methodology live in the
[GitHub README](https://github.com/Variably-Constant/SubEtha#readme).

## This crate

`subetha` is the umbrella. It pulls in the whole stack and re-exports
each member as a module:

| Module | Crate | Role |
|---|---|---|
| `subetha::core` | `subetha-core` | shared substrate: handshake header, observation ring, `AxisMask` |
| `subetha::pointers` | `subetha-pointers` | adaptive in-process exotic pointers |
| `subetha::sidecar` | `subetha-sidecar` | adaptive control plane |
| `subetha::cxc` | `subetha-cxc` | MMF-backed cross-process primitives + bridges |

```toml
[dependencies]
subetha = "0.1"
```

```rust
use subetha::cxc::AutoIpc;

let chan = AutoIpc::new("/tmp/events.bin")
    .capacity(64)
    .build_channel::<u64>()?;

chan.send(&42)?;                  // non-blocking
let v = chan.recv()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Feature flags

The default build is dependency-light and cross-platform. The capability
features forward to `subetha-cxc`; the platform- or C-library-specific
dependencies behind each one stay gated, so the default build pulls none
of them.

| Feature | Adds |
|---|---|
| `quic-bridge` | `QuicBridgeClient` / `QuicBridgeServer` over QUIC (quinn + rustls) |
| `tcp-bridge` | `TcpBridgeClient` / `TcpBridgeServer` over TCP |
| `tcp-tls-bridge` | the TCP bridge inside a rustls 1.3 record layer |
| `tls` | the optional TLS record layer for the reliable-UDP transport |
| `wire-locale` | the NIC-bypass datapath (AF_XDP / netmap / BPF / XDP) |
| `linux-futex-raw` | the raw Linux futex surface on `CrossProcessWaker` |

## Requirements

SubEtha builds on **stable Rust** (edition 2024, MSRV 1.96).

## Documentation

Full reference at the wiki:
<https://variably-constant.github.io/SubEtha/>.

## License

MIT. See [LICENSE-MIT](LICENSE-MIT).
