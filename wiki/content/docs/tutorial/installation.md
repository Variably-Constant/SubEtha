---
weight: 20
---

# Installation

SubEtha builds on **stable Rust 1.96+** with the 2024 edition
(`rust-version = "1.96"` in the workspace manifest). No
nightly features. The CXC stack, the substrate, the sidecar, and
the pointer kit all compile with the stock stable toolchain.

The repo pins this in `rust-toolchain.toml` so `rustup` picks the
right channel on first build. You do not need to switch toolchains
by hand.

```toml
# rust-toolchain.toml (already in the repo)
[toolchain]
channel = "stable"
profile = "default"
```

Windows, Linux, macOS, and FreeBSD all work; pick a C linker
through `rustup` (MSVC on Windows, clang or gcc elsewhere; FreeBSD
ships clang in base). The full test suite runs on Windows, Linux,
and FreeBSD as part of the project's own verification.

## Prerequisites

A stable Rust toolchain via [rustup](https://rustup.rs/). A C
linker. That is the whole list.

## Add SubEtha to your project

SubEtha ships as four separate crates. Pull in the one you need:

```toml
[dependencies]
subetha-cxc = "0.1"          # The primary user-facing crate.
subetha-pointers = "0.1"     # Exotic pointer types for CXC payloads.
subetha-core = "0.1"         # The substrate, if you only need that.
subetha-sidecar = "0.1"      # Control plane, if you embed it directly.
```

The crate inventory:

| Crate | What's in it |
|---|---|
| `subetha-cxc` | **The principal user-facing crate.** `Channel<T>`, `AdaptiveIpc<T>`, `AutoIpc`, the MMF dispatcher, and ~40 MMF-backed primitives. The big one. |
| `subetha-pointers` | Eight exotic pointer types for CXC payloads: `UmbraPointer`, `BloomPointer`, `CardinalityPointer`, `KStepPointer`, `KTower2/3`, `SelfDescPointer`, `VersionedPointer` / `HlcVersionedPointer`, `ReadableCapability` / `WritableCapability`. |
| `subetha-core` | Handshake header, observation ring, migration protocol, `Marshal` trait, axis-signature catalog, CPUID helpers. The substrate. |
| `subetha-sidecar` | Registry, `AdaptiveInstance`, `Policy`, `SidecarBox`. The control plane. |

Most callers want **`subetha-cxc`** plus `subetha-pointers` for
typed payloads. The substrate and sidecar come along as
transitive deps; you reach for them directly only when you
embed the control plane or implement custom primitives on top of
the substrate.

## Your project also needs the same toolchain

The stable + edition-2024 requirement propagates. Add a
`rust-toolchain.toml` at the root of your downstream project so
`rustup` picks the same channel:

```toml
[toolchain]
channel = "stable"
```

You can also leave it off and rely on the workspace default, but
pinning makes builds reproducible across machines.

## Smoke test

```rust,no_run
use std::sync::atomic::Ordering;
use subetha_cxc::SharedAtomicU64;

fn main() {
    let a = SharedAtomicU64::create("/tmp/subetha-smoke.bin", 0).unwrap();
    a.fetch_add(1, Ordering::AcqRel);
    println!("value = {}", a.load(Ordering::Acquire));
    std::fs::remove_file("/tmp/subetha-smoke.bin").ok();
}
```

```bash
cargo run --release
# value = 1
```

That confirms the MMF substrate maps and aliases correctly on your
host.

## Run the substrate microbench

The substrate has a fixed per-op cost floor. The `async_overhead`
bench checks your host matches it with a single-threaded `Channel<u64>`
round-trip (send + recv):

```bash
git clone https://github.com/Variably-Constant/SubEtha.git
cd subetha
cargo bench -p subetha-cxc --bench async_overhead
```

Each `b.iter` drives a 1000-round-trip inner loop, so every Criterion
`time:` line is the cost of 1000 round-trips - divide by 1000 for the
per-op figure. Reference per-round-trip numbers from an AMD Zen+ R7 2700
(8-core, 16-thread):

```text
sync     send/recv      ~  27 ns/round-trip   (27 us per 1000-op batch)
blocking send/recv      ~  55 ns/round-trip
async    send/recv      ~ 361 ns/round-trip
```

If the sync figure is dramatically slower than ~27 ns, the most likely
cause is a debug build, not release - `cargo bench` always builds the
bench profile in release.

Now go to [Cross-process round-trip in 30 lines](cross-process-roundtrip.md).
