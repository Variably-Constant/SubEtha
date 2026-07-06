---
title: "Windows-only Primitives"
weight: 71
---

# Windows-only substrate primitives

Substrate modules that depend on Windows-specific Win32 calls.
Each module is gated behind `cfg(windows)` so the workspace stays
buildable on Linux and other platforms.

| Primitive | Module | OS gate | Cargo feature |
|---|---|---|---|
| [LargePageRegion + LargePageSection](large-pages/) | `large_pages` | Windows | none (uses windows-sys) |

E2E verification runs natively on the Windows host via
`cargo run --release --example large_pages_demo` (single-process
probe + allocation) and the demo's `create-wait` / `open-verify`
two-process mode (cross-process named-section proof).
