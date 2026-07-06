---
title: "Large Pages"
weight: 72
---

# LargePageRegion + LargePageSection

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-Windows-blue)
![Pages](https://img.shields.io/badge/pages-2MB_large-brightgreen)
![Sharing](https://img.shields.io/badge/sections-cross--process-success)

Windows large-page memory helpers - the Windows sibling of the
Linux [hugepages]({{< ref "../linux/hugepages" >}}) module, with
one extra capability: cross-process NAMED sections.

| Primitive | Backing | Sharing | Win32 path |
|---|---|---|---|
| `LargePageRegion` | private committed memory | single process | `VirtualAlloc(MEM_LARGE_PAGES)` |
| `LargePageSection` | pagefile-backed named section | cross-process by name | `CreateFileMappingW(SEC_LARGE_PAGES)` + `MapViewOfFile` |

> **The "huge memory table" primitive.** A multi-GB lookup table
> mapped with 2MB TLB entries instead of 4KB: a 16MB region is 8
> TLB entries instead of 4096. `LargePageSection` lets two
> processes map the SAME large-page-backed physical memory by
> section name, so the table is built once and read everywhere.

## The privilege gate (different from Linux)

Linux gates hugepages on RESERVATION
(`/proc/sys/vm/nr_hugepages`); Windows gates large pages on an
ACCOUNT PRIVILEGE: `SeLockMemoryPrivilege` ("Lock pages in
memory" in Local Security Policy). Two steps:

1. **Grant** (one-time, admin): `secpol.msc` > Local Policies >
   User Rights Assignment > "Lock pages in memory" > add the
   account, then log off and on so the token picks it up.
2. **Enable** (per-process, runtime): call
   `enable_lock_memory_privilege()` before allocating. The
   privilege sits Disabled in the token until enabled.

Check what the account holds with `whoami /priv` - the
`SeLockMemoryPrivilege` row must exist (state Disabled is fine;
the enable call flips it).

When the privilege is absent, allocation fails with
`ERROR_PRIVILEGE_NOT_HELD` (1314); callers fall back to standard
4KB allocation exactly as on Linux when no hugepages are
reserved.

## File-backed mappings can NEVER use large pages on Windows

`SEC_LARGE_PAGES` requires the section be pagefile-backed
(`INVALID_HANDLE_VALUE` as the file handle). A mapping over a
real on-disk file is always 4KB-paged by OS design. The
substrate's File locale therefore stays standard-paged on
Windows; the Anon and named-section locales are the large-page
candidates.

## API

| Call | Behavior |
|---|---|
| `large_page_minimum() -> usize` | Host's large-page size in bytes (2MB on current x86_64/ARM64); 0 = unsupported. |
| `enable_lock_memory_privilege() -> io::Result<()>` | Enable `SeLockMemoryPrivilege` in the process token; precise error naming the secpol grant when the account lacks it. |
| `round_to_large_page(bytes) -> Option<usize>` | Round up to the large-page multiple; `None` when unsupported. |
| `LargePageRegion::allocate(bytes)` | Private large-page memory, rounded up. Drop = `VirtualFree`. |
| `LargePageSection::create(name, bytes)` | Named pagefile-backed large-page section + mapped view. |
| `LargePageSection::open(name, bytes)` | Open an existing named section from any process. Drop = `UnmapViewOfFile` + `CloseHandle`. |
| `ERROR_PRIVILEGE_NOT_HELD` (1314) / `ERROR_NO_SYSTEM_RESOURCES` (1450) | The two documented failure codes callers match for fallback. |

`ERROR_NO_SYSTEM_RESOURCES` deserves a note: large pages are
never paged out, so the kernel needs free CONTIGUOUS physical
RAM at allocation time. On a fragmented host a large allocation
fails with 1450 even with plenty of total free RAM; retry after
memory pressure drops or fall back.

## Worked example

```rust
use subetha_cxc::large_pages::{
    enable_lock_memory_privilege, LargePageSection,
};

// One-time per process.
enable_lock_memory_privilege()?;

// Process A: build the table.
let mut table = LargePageSection::create("Local\\my_table", 64 * 1024 * 1024)?;
populate(table.as_mut_slice());

// Process B: map the same physical pages.
let table = LargePageSection::open("Local\\my_table", 64 * 1024 * 1024)?;
lookup(table.as_slice());
```

## E2E proof

- `cargo run --release --example large_pages_demo` on a host
  whose account holds the privilege: enables the privilege,
  allocates an 8MB `LargePageRegion` (4 x 2MB pages, write/read
  verified on every page) and an 8MB `LargePageSection` with a
  second view confirming writes.
- Two-process mode (`create-wait` / `open-verify` subcommands):
  creator process writes 64 pattern points at 64KB strides into a
  4MB section; a SEPARATE verifier process opens the section by
  name and asserts all 64 points - proving the cross-process
  named-section sharing on running binaries.
- 4 lib tests pin the exact-contract behavior (success with
  usable memory OR precisely the documented error codes).

## When to reach for this primitive

- Very large shared lookup tables (multi-MB to multi-GB) read by
  several processes; TLB-miss latency measurably hurts.
- Long-lived regions: large pages are committed and locked up
  front, so churn-heavy short-lived allocations waste the
  privilege's value.

## When NOT to reach for this

- Small regions (< 1MB): 4KB pages are fine.
- Hosts where the account cannot be granted "Lock pages in
  memory" (locked-down enterprise policy): `allocate` fails with
  1314 immediately; keep the standard-page fallback.
- Anything that must be file-backed / disk-persistent: large
  pages are pagefile-backed only on Windows.

## Laying a ring into a large-page region

`LargePageRegion` and `LargePageSection` implement `RegionOwner`, so a
ring writes its header and slots straight into the large-page bytes and
holds the region for its lifetime. A many-lane MPMC grid then sits on a
handful of 2MB TLB entries instead of thousands of 4KB ones.

```rust
use subetha_cxc::mpmc_ring::SharedRingMpmc;
use subetha_cxc::large_pages::LargePageRegion;
use subetha_cxc::spsc_ring::spsc_ring_file_size;

let need = spsc_ring_file_size(4096) * 8;       // 8 lanes
let region = LargePageRegion::allocate(need)?;
let (producers, consumers) =
    SharedRingMpmc::create_grid_in_region(region, 8, 4, 4096)?;
```

`SpscRingCore::create_in_region` / `open_in_region` and
`SharedRing::create_in_region` / `open_in_region` do the same for a
single ring. `open_in_region` attaches to a ring another process laid
out in a named `LargePageSection`. A 64-byte alignment guard rejects a
mis-aligned hand-rolled region rather than dereferencing it; page-backed
regions clear it by construction.

The `large_page_ring` example runs an 8-lane grid on a real large-page
region plus a cross-process SPSC ring on a named section.

## References

- [hugepages]({{< ref "../linux/hugepages" >}}) - the Linux
  sibling (`MAP_HUGETLB`, private-anonymous only).
- [`SpscRingCore`]({{< ref "../rings/shared-ring-spsc" >}}),
  [`SharedRing`]({{< ref "../rings/shared-ring" >}}) - ring
  primitives whose backing rides a large-page region via
  `create_in_region`.
