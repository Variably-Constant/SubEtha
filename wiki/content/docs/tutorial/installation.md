---
weight: 20
---

# Installation

SubEtha has three surfaces: the Rust crates, the Python package
`subetha-ipc` (imported as `subetha`), and the PowerShell module
`SubEtha`. The Python and PowerShell surfaces ship prebuilt; only the
Rust crates need a toolchain.

The Rust crates build on **stable Rust 1.96+** with the 2024 edition
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

{{< tabs >}}

{{< tab name="Rust" >}}
A stable Rust toolchain via [rustup](https://rustup.rs/). A C
linker. That is the whole list.
{{< /tab >}}

{{< tab name="Python" >}}
CPython 3.11 or later. One wheel serves 3.11 and every later version
through the stable ABI; a free-threaded interpreter takes a wheel of
its own (see [SubEtha from Python](python.md)).
{{< /tab >}}

{{< tab name="PowerShell" >}}
PowerShell 7 or Windows PowerShell 5.1: one module folder serves
both.
{{< /tab >}}

{{< /tabs >}}

## Add SubEtha to your project

{{< tabs >}}

{{< tab name="Rust" >}}
SubEtha ships as six crates that share one version. Pull in the ones
you need:

```toml
[dependencies]
subetha-cxc = "0.6"          # The primary user-facing crate.
subetha-pointers = "0.6"     # Exotic pointer types for CXC payloads.
subetha-core = "0.6"         # The substrate, if you only need that.
subetha-sidecar = "0.6"      # Control plane, if you embed it directly.
```
{{< /tab >}}

{{< tab name="Python" >}}
```bash
pip install subetha-ipc
```

The package imports as `subetha`.
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
Install-Module SubEtha
Import-Module SubEtha
```
{{< /tab >}}

{{< /tabs >}}

The Rust crate inventory:

| Crate | What's in it |
|---|---|
| `subetha` | The umbrella crate over the others. |
| `subetha-cxc` | **The principal user-facing crate.** `Channel<T>`, `AdaptiveIpc<T>`, `AutoIpc`, the MMF dispatcher, and ~40 MMF-backed primitives. The big one. |
| `subetha-pointers` | Eight exotic pointer types for CXC payloads: `UmbraPointer`, `BloomPointer`, `CardinalityPointer`, `KStepPointer`, `KTower2/3`, `SelfDescPointer`, `VersionedPointer` / `HlcVersionedPointer`, `ReadableCapability` / `WritableCapability`. |
| `subetha-core` | Handshake header, observation ring, migration protocol, `Marshal` trait, axis-signature catalog, CPUID helpers. The substrate. |
| `subetha-sidecar` | Registry, `AdaptiveInstance`, `Policy`, `SidecarBox`. The control plane. |
| `subetha-ffi` | The C ABI: generation-checked handles over the memory-mapped primitives, for C, C++ and every language that binds through C. See [SubEtha from C and C++](c-and-cpp.md). |

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

{{< tabs >}}

{{< tab name="Rust" >}}
```rust,no_run
use std::sync::atomic::Ordering;
use subetha_cxc::SharedAtomicU64;

fn main() {
    let path = "/tmp/subetha-smoke.bin";
    let a = SharedAtomicU64::create(path, 0).unwrap();
    a.fetch_add(1, Ordering::AcqRel);
    println!("value = {}", a.load(Ordering::Acquire));
    drop(a);
    std::fs::remove_file(path).expect("remove the smoke-test file");
}
```

```bash
cargo run --release
# value = 1
```
{{< /tab >}}

{{< tab name="Python" >}}
```python
import os
import tempfile

import subetha

path = os.path.join(tempfile.gettempdir(), "subetha-smoke.bin")
a = subetha.Atomic(path, 0)
a.fetch_add(1)
print(f"value = {a.load()}")
del a  # the mapping goes with the last reference to it
os.remove(path)
```

```bash
python smoke.py
# value = 1
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
Import-Module SubEtha
$path = Join-Path ([IO.Path]::GetTempPath()) 'subetha-smoke.bin'
$a = New-SubEthaAtomic -Path $path -Init 0
$null = $a.FetchAdd(1)
"value = $($a.Load())"
$a.Dispose()
Remove-Item $path
# value = 1
```
{{< /tab >}}

{{< /tabs >}}

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
per-op figure. Reference per-round-trip numbers from an AMD Ryzen 9 7900X
(12-core, 24-thread) under Windows 11:

```text
sync     send/recv      ~ 4.5 ns/round-trip   (4.5 us per 1000-op batch)
blocking send/recv      ~  15 ns/round-trip
async    send/recv      ~ 125 ns/round-trip
```

If the sync figure is dramatically slower than ~4.5 ns, the most likely
cause is a debug build, not release - `cargo bench` always builds the
bench profile in release.

The bench runs from the Rust workspace. What a call costs from Python
and from PowerShell is in [SubEtha from Python](python.md) and
[SubEtha from PowerShell](powershell.md).

Now go to [Cross-process round-trip in 30 lines](cross-process-roundtrip.md).
