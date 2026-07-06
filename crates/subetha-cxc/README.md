# subetha-cxc

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE-MIT)
[![Wiki](https://img.shields.io/badge/wiki-variably--constant.github.io-blue)](https://variably-constant.github.io/subetha/docs/reference/subetha-cxc/)

The MMF-backed cross-process primitive family for
[SubEtha](https://github.com/Variably-Constant/subetha). Roughly
forty primitives across nine categories, all sitting on a single
mechanism: a memory-mapped file. The same byte layout serves
cross-thread, cross-process, and disk-persistent.

> **You probably want the [`subetha`](https://crates.io/crates/subetha)
> umbrella crate instead.** It pulls in this crate plus
> `subetha-pointers` (adaptive in-process) on one shared substrate
> and re-exports `subetha_cxc` under `subetha::ipc`. Reach for
> `subetha-cxc` directly only when you want to opt out of the
> adaptive in-process primitives via `default-features = false`.

## Why one mechanism

A memory-mapped file gives the caller three things at once:

1. **Cross-thread**. Two threads in one process both map the
   same file. They get two virtual mappings pointing at the
   same physical pages.

2. **Cross-process**. Two *processes* map the same file. The OS
   page cache aliases them onto identical physical pages. Atomic
   CAS that worked between threads now works between processes.

3. **Disk persistence**. The file is real. Modify the mapped
   region; eventually dirty pages flush back to disk. Restart
   the process and reopen the file - the bytes are right where
   the writer left them.

There is no separate "shared memory" abstraction versus a
"disk" abstraction. The MMF is both at once.

## The primitives

| Category | Primitives |
|---|---|
| Rings, stacks | `SharedRing`, `SharedBroadcastRing`, `SharedTreiberStack` |
| Hash maps, ordered maps | `SharedHashMap`, `SharedSkipList`, `SharedLRUCache` |
| Atomics | `SharedAtomicU32`, `SharedAtomicU64`, `SharedAtomicBool` |
| Cells | `SharedCell`, `SharedOnceCell` |
| Locks | `SharedRWLock`, `SharedSemaphore`, `SharedRateLimiter`, `SharedFenceClock` |
| Sketches | `SharedBloomFilter`, `SharedCountMinSketch`, `SharedHyperLogLog`, `SharedHistogram`, `SharedReservoirSampler` |
| Arenas, handles | `SharedStringArena`, `SharedHandleTable`, `SharedRegion` |
| Ownership | `OwnerLease`, `SharedLeaderElection`, `LazyConfig` |
| Coordination | `HeartbeatTable`, `EpochBarrier`, `FailoverWatchdog`, `EventStateLog`, `SharedVersionedChain`, `SharedTimePointTile`, `PriorityFanout`, `ProgressTask`, `BackgroundScheduler`, `SharedGraph`, `SharedTopologyMap`, `KTowerCascade`, `SharedAsyncPointer`, `PassRegistry` |
| Pointer variants | `SharedUmbraPointer`, `SharedUniversal`, `SharedNaNValue`, `SharedNaNTaggedValue`, `OffsetPtr`, `TaggedOffsetPtr` |
| Polymorphic substrate (shape axis) | `SpscRingCore` (native primitive), `SharedRingMpsc`, `SharedRingMpmc`, `AdaptiveRing` (default ring; SPSC/MPSC/MPMC/Vyukov morph) |
| Polymorphic substrate (locale axis) | `LocaleAdaptiveRing` (Anon/File/ShmFs morph), `ShmFile` (cross-platform named shared memory) |
| Polymorphic substrate (protocol axis) | `AdaptiveIpc<T>` (ring + deque), `PubSubRing` + `PubSubSubscriber` (one-to-many fanout) |
| Polymorphic substrate (identity + policy) | `VirtualEndpoint` + `VirtualEndpointRegistry`, `QosPolicy`, `SubscriberPosition` |
| Bridges (Cargo feature flags) | `QuicBridgeClient` / `QuicBridgeServer` (`quic-bridge`), `TcpBridgeClient` / `TcpBridgeServer` (`tcp-bridge`) |
| Linux-only | `DirectFileRing` (O_DIRECT), `HugepageRegion` (MAP_HUGETLB), `VsockSocket`, `fd_handoff::send_fd` / `recv_fd`, `AfXdpSocket` (`wire-locale` feature) |

Full primitive catalog at the wiki:
<https://variably-constant.github.io/subetha/docs/reference/subetha-cxc/catalog/>.

## Cargo feature flags

Optional substrate primitives that pull in additional dependencies
or target a specific OS. Default features compile clean without any
of these.

| Feature | Pulls | Enables |
|---|---|---|
| `quic-bridge` | quinn, rcgen, rustls, tokio | `quic_bridge::{QuicBridgeClient, QuicBridgeServer, make_self_signed_pair, install_default_crypto_provider}` |
| `tcp-bridge` | tokio (net + io-util + rt-multi-thread + macros) | `tcp_bridge::{TcpBridgeClient, TcpBridgeServer}` |
| `wire-locale` | (no extra dep; uses libc) | `locale_wire::AfXdpSocket` (Linux 4.18+) |

Modules gated on `cfg(target_os = "linux")` are compiled away on
other platforms; modules gated on `cfg(unix)` are excluded on
Windows.

## Quick start

A cross-process hash map:

```rust,no_run
use subetha_cxc::SharedHashMap;

// Producer process:
let m = SharedHashMap::<u32, u64>::create("/tmp/sessions.bin", 4096)
    .unwrap();
m.insert(42, 4242).unwrap();
m.flush().unwrap();  // msync dirty pages to disk

// Consumer process (different binary, same file):
let m = SharedHashMap::<u32, u64>::open("/tmp/sessions.bin", 4096)
    .unwrap();
assert_eq!(m.get(&42), Some(4242));
```

A lock-free MPMC ring:

```rust,no_run
use subetha_cxc::SharedRing;

let r = SharedRing::create("/tmp/queue.bin", 1024).unwrap();
r.try_push(b"hello").unwrap();
let mut buf = [0u8; 52];
let n = r.try_pop(&mut buf).unwrap();
```

## Invariants

Three invariants hold across the family:

- **No absolute pointers.** Pointers between slots inside the
  mapped region are file-relative offsets via `OffsetPtr` or
  `TaggedOffsetPtr`, never raw pointers. The kernel can map the
  file at any virtual base; offset arithmetic stays valid in
  both mappings.

- **Deterministic hashing.** Maps and sketches use FNV-1a via
  `fnv1a_64`, not `std::hash::RandomState`. The same key
  produces the same slot in every process linking the crate.

- **Power-of-2 capacity.** Slot index calculations reduce to
  `hash & (capacity - 1)`. One AND instruction vs a divide on
  every probe. Non-pow2 capacities return
  `MapError::InvalidCapacity` (and equivalents for the other
  primitives).

## Sidecar observation

Every `Shared*` primitive implements
`subetha_sidecar::AdaptiveInstance` and carries a
`HandshakeHeader` plus `ObservationRing`. The bare
`create` / `open` constructors return the unregistered type.
Wrap in `SidecarBox` for sidecar observation:

```rust,no_run
use subetha_cxc::SharedHashMap;
use subetha_sidecar::SidecarBox;

let m = SidecarBox::new(
    SharedHashMap::<u32, u64>::create("/tmp/sessions.bin", 4096).unwrap()
);
m.insert(42, 4242).unwrap();

let stats = m.stats().unwrap();
println!("ops_observed = {}", stats.ops_observed);
```

The default `Policy` for MMF primitives is `NoMigrationPolicy`
because the strategy IS the byte layout, which is not migrable
in place.

## Requirements

SubEtha builds on **stable Rust** (edition 2024, MSRV 1.96). The
`rust-toolchain.toml` at the workspace root pins the stable channel;
downstream projects need only a recent stable toolchain.

## Where it sits

```text
your code
    -> subetha                      (this crate)
       -> subetha-sidecar            (control plane)
          -> subetha-core            (substrate)
```

## Documentation

Per-primitive reference + bench numbers in
`crates/subetha-cxc/docs/pointers/*.md` (54 hand-written prose
documents, one per primitive).

Category overview at the published wiki:
<https://variably-constant.github.io/subetha/docs/reference/subetha-cxc/>.

Architectural background:
- [Frozen handshakes](https://variably-constant.github.io/subetha/docs/explanation/frozen-handshake/) -
  the thesis behind topology-axis un-freezing.
- [MMF substrate](https://variably-constant.github.io/subetha/docs/explanation/mmf-substrate/) -
  why one byte layout serves three deployment modes.

## License

MIT. See [LICENSE-MIT](LICENSE-MIT).
