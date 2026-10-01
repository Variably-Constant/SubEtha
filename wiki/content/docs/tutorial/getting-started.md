---
weight: 10
---

# Getting Started

This tutorial walks from zero to a SubEtha program two processes
share and the sidecar is actively observing. No prior
knowledge of the substrate is assumed. Each step shows Rust, Python
and PowerShell side by side; Python and PowerShell also have a
tutorial of their own, [SubEtha from Python](python.md) and
[SubEtha from PowerShell](powershell.md), and C and C++ have
[theirs](c-and-cpp.md).

If you already know what `Channel<T>` and `SidecarBox` are and just
want a quick reminder of which primitive matches your shape, skip
to [role-pair selection](../how-to/role-pair-selection.md).

## The path through

Three short chapters. Reading them in order takes about twenty minutes.

1. [Installation](installation.md). Installs the crates, the Python
   package or the PowerShell module and runs a smoke test; in Rust, the
   substrate microbench too.
2. [Cross-process round-trip in 30 lines](cross-process-roundtrip.md).
   Two processes, one MMF file, a shared hash map written by one
   process and read by the other. The end-to-end demo of the CXC family.
3. [Reading sidecar observations](reading-observations.md). What
   the sidecar saw, via `InstanceStats`. Layout of the
   `Observation` record. The op-kind histogram. Rust registers a
   primitive by wrapping it in a `SidecarBox`; Python and PowerShell
   call the object's `observe()`.

Chapter 2 uses a shared hash map. In Rust the CXC channels (`Channel<T>` /
`AdaptiveIpc<T>`) answer three calling conventions on one handle: the
sync `send` / `recv`, `send_blocking` / `recv_blocking` (parks a
thread), and `send_async` / `recv_async` (suspends a task) - the choice
is per call site, not a second type. The
[high-level API reference](../reference/subetha-cxc/high-level-api.md)
lays out all three conventions, and
[async: cost and scaling](../how-to/async-paths.md) covers when to
reach for each and what the async path measures.

After the tutorial you know enough SubEtha to read the **How-To
Guides** for tasks and the **Reference** pages for the spec. Two
pages worth reading early:
[concurrency and safety](../explanation/concurrency-and-safety.md) -
whether you need locks (you do not), why the rings are thread- *and*
process-safe, and the one thing you are responsible for; and
[tuning and overrides](../how-to/tuning-overrides.md) for every
environment variable, Cargo feature, and build recipe. The
[bridges reference](../reference/subetha-cxc/bridges/_index.md)
takes the same channels cross-host over QUIC, TCP, or the Sens-O-Matic
reliable-FEC protocol.
