---
title: SubEtha
toc: false
---

<p align="center">
  <img src="images/Logo.png" alt="SubEtha" width="400">
</p>

<p align="center">
  <em>Cross-Context Channel for Rust. Kernel-bypass IPC that spans threads, processes, and disk through a single memory-mapped file.</em>
</p>

> *Don't Panic.*

---

**SubEtha** implements **CXC**, the Cross-Context Channel: a typed channel
that spans every execution context users actually have. Cross-thread within
a process. Cross-process on the same machine. Persisted to disk through
the OS page cache. Cross-machine with a QUIC tunnel at the edge. One byte
layout, one API, **no kernel on the data path** after construction.

The name comes from Douglas Adams's *Hitchhiker's Guide to the Galaxy*.
The Sub-Etha Sens-O-Matic used sub-etheric waves to communicate **around**
conventional channels. CXC does the same thing: it skips the kernel's
"conventional channel" (named pipes, sockets, IPC handles) and writes
directly to user-space memory the kernel page-aliases between participants.

## The Guide

{{< cards >}}
  {{< card link="docs/tutorial/" title="Tutorial" subtitle="Zero to a working cross-process channel in twenty minutes." icon="academic-cap" >}}
  {{< card link="docs/how-to/" title="How-To" subtitle="Targeted recipes: pick a primitive, tune the sidecar, bridge two hosts." icon="book-open" >}}
  {{< card link="docs/reference/" title="Reference" subtitle="Every type, every trait, every guarantee - the dry spec." icon="document-text" >}}
  {{< card link="docs/explanation/" title="Explanation" subtitle="The design rationale: why the byte layout, the dispatcher, the morphs." icon="light-bulb" >}}
{{< /cards >}}

Jumping-off points inside the reference:
[the full primitive catalog](docs/reference/subetha-cxc/catalog/) ·
[rings, stacks, and queues](docs/reference/subetha-cxc/rings/) ·
[the adaptive shape-morphing ring](docs/reference/subetha-cxc/rings/shared-ring-adaptive/) ·
[cross-platform benchmarks](docs/reference/subetha-cxc/cross-platform-benchmarks/) ·
[the crate family](docs/reference/).

## How fast?

Measured on five hosts (native Windows + WSL2 on a Ryzen 7 2700; a Linux
VM and a FreeBSD VM on a Ryzen 7 5700G; a 2012 Intel Mac), 10,000
round-trips, 8-byte payloads, cross-process between parent and child. A
marker appears only on the hosts a contender could run on:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/platform_ipc_dotplot_dark.png">
  <img alt="Cross-process IPC one-way latency, every contender on every host" src="images/platform_ipc_dotplot.png">
</picture>

Across all five hosts the four pinned SubEtha shapes land at 54-408 ns
one-way - 80-528x faster than the fastest kernel IPC on each host, and
4.8-8.8x faster than iceoryx2's zero-copy shared memory where it builds.
The exact numbers on the Linux host:

| Method | One-way latency | vs SubEtha |
|---|---:|---:|
| **SubEtha pinned rings (MMF), 4 shapes** | **54-87 ns** | **1.00x** |
| iceoryx2 (Eclipse zero-copy) | 257 ns | **4.8x slower** |
| Anonymous stdio pipe | 16,618 ns | **308x slower** |
| Named pipe (`interprocess`) | 18,040 ns | **335x slower** |
| `ipc-channel` (Mozilla) | 20,629 ns | **383x slower** |
| UDP loopback | 21,354 ns | **396x slower** |
| TCP loopback | 24,995 ns | **463x slower** |
| ZeroMQ (`ipc://` REQ/REP) | 70,891 ns | **1315x slower** |

The methodology audit and reproducibility instructions are in
[`docs/CROSS_PROCESS_IPC_PERFORMANCE.md`](https://github.com/Variably-Constant/SubEtha/blob/main/docs/CROSS_PROCESS_IPC_PERFORMANCE.md).

### In-process, too

The same rings run thread-to-thread with no memory-mapped file, against
`crossbeam_channel`, `flume`, `rtrb`, and `std::sync::mpsc` (1,000,000
items, 16-byte payload, median-of-11), benched on every host:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/platform_inprocess_dotplot_dark.png">
  <img alt="In-process throughput by host: SubEtha ring shapes vs crossbeam, flume, rtrb, std::sync::mpsc" src="images/platform_inprocess_dotplot.png">
</picture>

The composed MPMC grid leads 4-producer / 4-consumer on every modern
host (37.6 ns/item on Linux, roughly 2x faster than `crossbeam_channel`),
though the 13-year-old Intel Mac flips that one cell to crossbeam.
rtrb's purpose-built SPSC leads raw 1P/1C everywhere. Each contender
appears only in the scenarios its API supports - rtrb (SPSC) in 1P/1C
alone, `std::sync::mpsc` (single-consumer) through 4P/1C, crossbeam and
flume (MPMC) in all three - and none of them crosses a process
boundary: this whole field is in-process only. SubEtha is the same
structure morphing across all four shapes, running cross-thread,
cross-process, and cross-host.

## What it looks like

```rust
use subetha_cxc::AutoIpc;

let chan = AutoIpc::new("/tmp/events.bin")
    .capacity(64)
    .build_channel::<u64>()?;

chan.send(&42)?;                  // non-blocking
let v = chan.recv()?;
assert_eq!(v, 42);

chan.send_blocking(&42, None)?;   // parks the thread
chan.send_async(&42).await?;      // suspends the task, any executor
```

One handle answers all three call styles. Sync, blocking, and async are
not separate types; you pick per call. The async future is a plain
`std::future::Future`, so it runs on tokio or on the crate's own
runtime-free `block_on`.

Same file path, opened from another thread or another process or after
a reboot: same behaviour. The `AutoIpc` builder picks the
empirically-best primitive based on declarative workload hints.
`.batch_size(64)` flips to KHL work-stealing. `.consumers(4)` flips to
URD per-thief mailboxes. No hints flips to SharedRing streaming.

## The crate family

The principal user-facing crate is
[`subetha-cxc`](docs/reference/subetha-cxc/_index.md), the CXC
implementation. `Channel<T>`, `AdaptiveIpc<T>`, `AutoIpc`, the MMF
dispatcher, and roughly forty MMF-backed primitives: `SharedRing`,
the `SharedDeque` family (Chase-Lev, plus the novel KHL / KHPD / LOH /
URD variants), `SharedHashMap`, `SharedRWLock`, `SharedSemaphore`,
`SharedLRUCache`, `SharedBTreeMap`, `OwnerLease`, `HeartbeatTable`,
`EpochBarrier`. Same MMF, three deployment modes. Map the file from a
second thread and it works cross-thread. Map it from a second process
and it works cross-process. Let the kernel flush dirty pages to disk
and it persists. **There is no separate "shared memory" abstraction
versus a "disk" abstraction. The MMF is both at once.**

The companion crate is
[`subetha-pointers`](docs/reference/subetha-pointers/_index.md): eleven
exotic pointer types built for the cross-context payloads CXC carries.

- `UmbraPointer<T>` carries a 4-byte content prefix beside the pointer
  for short-circuit equality before deref.
- `BloomPointer<T>` carries a 64-bit Bloom filter for probabilistic
  set membership without hashing.
- `CardinalityPointer<T>` carries a log2 cardinality estimate for
  size-class branching.
- `KStepPointer<T>` encodes a log2 stride for SIMD-friendly indexing.
- `KTower2<T>` and `KTower3<T>` encode multi-segment
  zone-region-offset addressing.
- `SelfDescPointer<T>` carries a type discriminant for heterogeneous
  channels.
- `VersionedPointer<T>` and `HlcVersionedPointer<T>` carry version
  metadata for MVCC and hybrid-logical-clock distributed consistency.
- `ReadableCapability<T>` and `WritableCapability<T>` carry runtime
  bounds for capability-secured cross-process channels (CHERI-style).

The substrate is [`subetha-core`](docs/reference/subetha-core/_index.md)
(handshake header, observation ring, marshal trait, axis-signature
catalog, CPUID helpers). The control plane is
[`subetha-sidecar`](docs/reference/subetha-sidecar/_index.md)
(per-NUMA scan thread, policy, `SidecarBox`, `AdaptiveInstance`).
The 1.62x `send::<u64>` speedup is in source: a `TypeId`-monomorphised
branch in `AdaptiveIpc::send` that LLVM resolves to a constant at
codegen time. No opt-in. **All four crates build on stable Rust 1.96+.**

## Three doors into the Guide

If you came to write code, start with the
[getting-started tutorial](docs/tutorial/getting-started/). Twenty
minutes from zero to your first working cross-process channel.

If you came to understand the architecture before touching code,
start with the [architecture overview](docs/explanation/architecture/)
and read it alongside the
[frozen-channel explanation](docs/explanation/frozen-handshake/) for
the *why*.

If you already know your concurrency shape, the
[role-pair selection guide](docs/how-to/role-pair-selection/) maps
shapes to primitives without ceremony.

## Reading order

The four sections of this guide follow
[Diataxis](https://diataxis.fr/): Tutorial, How-To, Reference,
Explanation. They are independent. Read in any order.

- **[Tutorial](docs/tutorial/)** is a teacher. It assumes nothing
  and walks you from zero to a working channel.
- **[How-To](docs/how-to/)** is a recipe book. Each page solves one
  specific problem in five minutes.
- **[Reference](docs/reference/)** is the operating manual. Every
  type, every parameter, every guarantee. Look things up.
- **[Explanation](docs/explanation/)** is the design rationale. Why
  the byte layout looks the way it does. Why the dispatcher exists.
  Read when you want to understand, not when you want to ship.

## License

SubEtha is licensed under the MIT License. See
[LICENSE-MIT](https://github.com/Variably-Constant/SubEtha/blob/main/LICENSE-MIT)
for the full text.

> *"So long, and thanks for all the pipes."*
