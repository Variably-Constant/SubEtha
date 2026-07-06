---
title: "Shm File"
weight: 50
---

# ShmFile

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-cross--platform-blue)
![Layout](https://img.shields.io/badge/Layout-shared--memory-green)

Cross-platform RAM-resident named shared-memory backing. Wraps the
platform's named shared-memory primitive so the rest of the
substrate can treat ShmFs the same way it treats anon and file
backings.

| Platform | Backend |
|---|---|
| Linux + macOS | `libc::shm_open` + `ftruncate` + memmap2 via `File::from_raw_fd` |
| Windows | `windows_sys::CreateFileMappingW(INVALID_HANDLE_VALUE, ...)` + `MapViewOfFile` |

Naming: the caller's logical name is prefixed with `/subetha_` on
Unix (POSIX shm names must start with `/`) and `Local\\subetha_`
on Windows (per-session visibility). Embedded slashes in the
caller's name become underscores. On Apple targets, where POSIX
shm names are capped at 31 chars (`PSHMNAMLEN`), a prefixed name
that would overrun the cap collapses to a deterministic short
`/se_{hash}` form so a create here and an open in a peer process
still resolve to the same region. macOS also only honours
`ftruncate` once at creation, so the constructor sizes the region
only when it is not already at least `size` (later openers map it
as-is); the mapping is prefaulted on construction.

## API

| Call | Behavior |
|---|---|
| `ShmFile::create_or_open_named(name: &str, size: usize) -> io::Result<Self>` | Create or open a named shared-memory region of `size` bytes. Asserts `size > 0`. |
| `shm.as_mut_slice() -> &mut [u8]` | Cross-platform mutable byte slice into the mapped region. |
| `shm.len() -> usize` | Region size in bytes. |
| `shm.is_empty() -> bool` | Always false for a valid region. |
| `shm.logical_name() -> &str` | The substrate-prefixed safe name. |

## Cross-process visibility

Two handles opened with the same logical name map onto the SAME
underlying memory region. This is the property that distinguishes
ShmFile from `MmapOptions::map_anon` (which is in-process only).

## Cleanup

Drop:
- Unix: drops the inner `File` (closes the fd) + calls `shm_unlink`
  on the safe name so a subsequent open with the same name starts
  fresh.
- Windows: `UnmapViewOfFile` + `CloseHandle`. Windows refcounts
  handles; the named object goes away when the last handle closes.

## Worked example

```rust,no_run
use subetha_cxc::shm_file::ShmFile;

let mut a = ShmFile::create_or_open_named("ipc_demo", 4096)?;
a.as_mut_slice()[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

// Process B (or same process, different handle):
let mut b = ShmFile::create_or_open_named("ipc_demo", 4096)?;
assert_eq!(&b.as_mut_slice()[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
# Ok::<(), std::io::Error>(())
```

## When to reach for this primitive

- Building a custom cross-process primitive that needs raw shared
  memory (the substrate's standard rings use this internally via
  `SpscRingCore::create_from_shm` / `SharedRing::create_from_shm`).
- Interop with non-SubEtha processes that speak POSIX shm.

## When NOT to reach for this

- You want a ring or a hash map: use the substrate's typed
  primitives ([`AdaptiveRing`](../../rings/shared-ring-adaptive/),
  [`SharedHashMap`](../../shared-hash-map/), etc.) which carry their
  own slot layouts.

## References

- Source: `crates/subetha-cxc/src/shm_file.rs` (340 lines, 5 unit
  tests: create+read/write, two-handles-same-memory, deterministic
  sanitize, drop-then-recreate-fresh, and an Apple-gated
  name-length test). `ShmFile` lives in the `pub mod shm_file`
  module path.
- [`LocaleAdaptiveRing`](../../rings/locale-adaptive-ring/) -
  uses `ShmFile` for the `Locale::ShmFs` backing.
- [`SpscRingCore::create_from_shm`](../../rings/shared-ring-spsc/),
  [`SharedRing::create_from_shm`](../../rings/shared-ring/) -
  direct construction on a ShmFile.
