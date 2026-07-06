---
title: "OS-specific Primitives"
weight: 70
---

# OS-specific substrate primitives

Substrate modules whose implementation is platform-gated. A few depend on
Linux-specific facilities (`MAP_HUGETLB`); most share one surface across
several OSes, with only the syscall layer gated. Each is compiled away where
unsupported, so the workspace stays buildable everywhere.

| Primitive | Module | OS support | Cargo feature |
|---|---|---|---|
| [DirectFileRing](direct-file-ring/) | `protocol_direct_file` | Unix + Windows (`O_DIRECT` / `F_NOCACHE` / `FILE_FLAG_NO_BUFFERING`) | none |
| [fd_handoff verb pair](fd-handoff/) | `fd_handoff` | Unix `SCM_RIGHTS` (incl. macOS) + Windows `DuplicateHandle` | none |
| `KernelAsyncRing` ([src](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/kernel_async_ring.rs)) | `kernel_async_ring` | Linux (io_uring) / Windows (IoRing) / FreeBSD + macOS (POSIX `aio`) | none |
| [HugepageRegion](hugepages/) | `hugepages` | Linux only (`MAP_HUGETLB`) | none |
| `SuperPageRegion` ([src](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/super_pages.rs)) | `super_pages` | FreeBSD (`MAP_ALIGNED_SUPER`) + macOS x86_64 (`VM_FLAGS_SUPERPAGE_SIZE_2MB`) | none |
| [VsockSocket](locale-vsock/) | `locale_vsock` | Linux (`AF_VSOCK`) + Windows (Hyper-V `AF_HYPERV`) | none |
| [WireSocket](locale-wire/) | `locale_wire` | Linux (AF_XDP) / Windows (XDP) / FreeBSD (netmap) / macOS (BPF) | `wire-locale` |

E2E verification of these primitives runs their real example
binaries on a hosted Linux VM (the Linux-only locales never execute
on Windows, so that VM is where their close gate runs).
