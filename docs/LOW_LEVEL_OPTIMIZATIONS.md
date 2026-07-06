# Low-Level Optimizations

The hardware / ISA / OS / codegen levers the substrate runs on the
hot paths, plus the ones exported as caller overrides. Every SIMD
path the substrate runs ships scalar / SSE2 fallbacks (SSE2 is the
x86-64 baseline; non-x86 gets scalar).

Hosts the numbers below were measured on: Windows 11 / Ryzen 7 2700
(Zen+, bare metal), WSL2 Linux 6.6 (same machine), FreeBSD 15 VM /
Ryzen 7 5700G (Zen3). Probes: `examples/cacheline_probe.rs`,
`examples/monitor_wait_probe.rs`, `examples/bridge_lan.rs`, and a
PGO instrument -> train -> optimize build cycle over the bridge +
ring workloads.

## On the hot paths

| Lever | Where | Effect |
|---|---|---|
| MONITORX/MWAITX + WAITPKG monitor-wait tier | `monitor_wait.rs`, ahead of every kernel park in `cross_process_waker.rs` | waker wake p50 120 ns against 9,321 ns for the kernel park, Windows / Zen+; carries the Windows cross-process wake (`WaitOnAddress` is intra-process only), where a two-process pair completes in 336 ms |
| AArch64 `LDAXR`+`WFE` monitor arm | `monitor_wait.rs` (`ArmWfe`) | base-ISA, always selected on aarch64; a remote store wakes via the global exclusive monitor's Exclusive->Open event, no SEV needed. Type-checked on aarch64-linux + aarch64-darwin |
| macOS `os_sync_wait_on_address` park | `cross_process_waker.rs` platform arms | the public futex (macOS 14.4+), `OS_SYNC_WAIT_ON_ADDRESS_SHARED` for file / shm backings |
| `PREFETCHW` before producer / consumer seq CAS | `shared_ring.rs` MPMC push / pop | contended CAS ping-pong 59 ns/round-trip with the prefetch against 186 ns without it (3.2x) on bare-metal Zen+ |
| `CLDEMOTE` after slot publish / release | `shared_ring.rs`, `spsc_ring.rs` | purpose-built for the producer->consumer line handoff on silicon that has it; a NOP where `has_cldemote()` is false, so it is free everywhere else |
| MMF warm-up at attach | `mmf_warm.rs`, called from `shm_file.rs` + `shared_ring::open` + `shared_region` | Linux `MADV_POPULATE_WRITE`: first full 32 MiB drain 7-13 ms with warm-up against 54-63 ms without (5-8x; the fault storm leaves the traffic path). FreeBSD `MADV_WILLNEED`: neutral (shm pages already resident) |
| Egress staging buffer reuse | `blocking_tcp_bridge.rs` | one stream-lifetime staging buffer threaded through `spawn_blocking`, instead of an allocation per 256-slot batch |
| Linux TCP knobs | `net_tune.rs`, all bridge sockets | `TCP_QUICKACK` and `TCP_NOTSENT_LOWAT` (16 KiB = one egress batch); advisory, no-op off Linux |
| Waker `parked_mask` | `cross_process_waker.rs` header word | a producer wake scan reads ONE header line instead of `capacity` slot lines when nobody is parked (the common case); stale bits are filtered by per-slot state, missing bits covered by the parker's pre-wait double-check |

## Available as caller overrides

These ship off the default hot path; reach for them when your call
shape or build target matches.

| Override | What it is | When to use |
|---|---|---|
| `SUBETHA_MMF_WARM=1` | forces the Windows attach-time prefetch | cold-file attaches on Windows, where the automatic path stays off because `PrefetchVirtualMemory` costs ~6 ms on page-cache-hot backings |
| `SUBETHA_BUSY_POLL_US=<us>` | enables `SO_BUSY_POLL` on bridge sockets | Linux hosts with NIC / NAPI support, trading CPU for tail latency |
| `RUSTFLAGS=-Ctarget-cpu=x86-64-v3 cargo build --release` | release codegen against `x86-64-v3` (AVX2 + BMI2 + FMA assumed) | v3-capable hosts: TCP loopback bridge 2,919 Mbit/s against 2,387 at baseline (+22%). The default build stays baseline + runtime dispatch |
| PGO (rustc `-Cprofile-generate` -> run the bridge + ring workloads -> `llvm-profdata merge` -> `-Cprofile-use`) | instrument -> train -> optimize build | publication builds: TCP loopback bridge 3,286 Mbit/s (+38%) |
| `quic_bridge::install_default_crypto_provider` | installs the `ring` rustls crypto provider | callers wanting a different provider install their own before bridge setup; it is one public call |
