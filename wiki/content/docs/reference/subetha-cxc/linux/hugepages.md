---
title: "Hugepages"
weight: 73
---

# HugepageRegion

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-Linux-blue)

Linux-only hugepage-backed anonymous mmap helper. Allocates a
region backed by 2MB or 1GB hugepages (`MAP_HUGETLB` |
`MAP_HUGE_2MB` / `MAP_HUGE_1GB`). Useful for large rings where
TLB pressure measurably hurts: a 16MB region fits in 8 hugepages
vs 4096 4KB pages.

## API

| Constant / item | Value |
|---|---|
| `HUGEPAGE_2MB` | `2 * 1024 * 1024` |
| `HUGEPAGE_1GB` | `1024 * 1024 * 1024` |
| `HugepageSize::Mb2` | 2 MB pages (most widely available). |
| `HugepageSize::Gb1` | 1 GB pages (require kernel hugetlbfs + reserved gigabyte pages at boot). |

| Call | Behavior |
|---|---|
| `HugepageRegion::allocate(pages: usize, size: HugepageSize)` | Allocate `pages * size.bytes()` bytes backed by the requested hugepage size. Returns `io::Result<Self>`. |
| `region.as_mut_slice() -> &mut [u8]` | Mutable byte slice. |
| `region.as_slice() -> &[u8]` | Read-only byte slice. |
| `region.len() -> usize` | Region size in bytes. |
| `region.is_empty() -> bool` | Always false for a valid region. |

Drop runs `munmap` on the region.

## Falling back gracefully

`allocate` returns `Err(io::Error)` when the kernel does not have
the requested hugepage size reserved. Callers handle the fallback
by allocating via standard `MmapOptions::map_anon` for a 4KB-page
region.

To reserve hugepages on a Linux host:

```sh
echo 64 > /proc/sys/vm/nr_hugepages   # 64 * 2MB = 128MB
```

## When to reach for this primitive

- Very large rings (multi-MB) where TLB miss latency measurably
  hurts the hot path.
- Workloads that benefit from kernel-level hugepage-aware
  memory management (THP off, manual hugepages on).

## When NOT to reach for this

- Small rings (< 1MB). 4KB pages are fine; hugepages add no
  measurable benefit and pin scarce reserved pages.
- Hosts without reserved hugepages. `allocate` fails immediately
  on those.

## References

- [Large Pages](../../windows/large-pages/) - the Windows
  sibling (`VirtualAlloc(MEM_LARGE_PAGES)` for private regions
  plus `SEC_LARGE_PAGES` named sections for cross-process
  sharing, which this Linux module does not cover - hugepage
  cross-process sharing on Linux goes through hugetlbfs or
  `memfd_create(MFD_HUGETLB)`).
- [`SpscRingCore`](../../rings/shared-ring-spsc/),
  [`SharedRing`](../../rings/shared-ring/) - the ring primitives
  most sensitive to TLB pressure at large capacities.
  `HugepageRegion` implements `RegionOwner`, so
  `SpscRingCore::create_in_region` / `SharedRing::create_in_region`
  and `SharedRingMpmc::create_grid_in_region` lay a ring (or a whole
  many-lane grid) straight into the hugepage bytes. The Windows
  sibling with named cross-process sections is
  [`large_pages`](../../windows/large-pages/).
