---
title: "Direct File Ring"
weight: 71
---

# DirectFileRing

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-Linux-blue)
![Storage](https://img.shields.io/badge/storage-O__DIRECT%20pread%2Fpwrite-green)

Non-mmap pread/pwrite ring with `O_DIRECT` page-cache bypass.
Where the substrate's other ring primitives use mmap to share
memory with peers, `DirectFileRing` opens a file with `O_DIRECT`
and reads/writes via `pread(2)` / `pwrite(2)`. Every write goes
directly to the underlying block device; every read comes
directly from the device.

Useful when the substrate IS the buffer (caller does its own
caching, doesn't want the kernel double-buffering). Common in
database storage engines.

## Alignment

`O_DIRECT` requires that read/write buffer addresses, offsets,
and lengths all be aligned to the device's logical block size.
This primitive fixes the slot size at 4096 bytes
(`DIRECT_FILE_SLOT_SIZE`) and uses `posix_memalign(3)` for
4096-byte-aligned buffers.

## Coordination

Head/tail counters live in two SEPARATE small MMF
`SharedAtomicU64` files. Writing them via `O_DIRECT` `pwrite`
would defeat their purpose (atomic cross-process visibility).
File layout:

```
{base}.directfile.data.bin   - slot array, O_DIRECT
{base}.directfile.head.bin   - head counter, MMF
{base}.directfile.tail.bin   - tail counter, MMF
```

## API

| Call | Behavior |
|---|---|
| `DirectFileRing::create(base_path, capacity) -> Result<Self, DirectFileError>` | Create the three files. Capacity must be pow2 >= 2. |
| `DirectFileRing::open(base_path, expected_capacity)` | Reopen existing ring. |
| `ring.capacity() -> usize` | Slot count. |
| `ring.head() / ring.tail() -> u64` | Acquire loads of the counter files. |
| `ring.try_push(payload: &[u8]) -> Result<(), DirectFileError>` | Aligned-buf copy + pwrite at slot offset + Release head. |
| `ring.try_pop(out: &mut [u8]) -> Result<usize, DirectFileError>` | pread at tail + copy to out + Release tail. |

Drop removes all three files.

## Error type

`DirectFileError`:
- `Io(std::io::Error)` - syscall failure.
- `LayoutMismatch` - reopen finds a smaller file than expected.
- `Empty` - try_pop on an empty ring.
- `Full` - try_push when head - tail == capacity.
- `PayloadTooLarge` - try_push with payload > slot size.

## When to reach for this primitive

- Database / log-style workloads where the substrate is the
  buffer; the kernel page cache is wasted RAM.
- Workloads that need durability guarantees stronger than the
  page cache's writeback semantics.

## When NOT to reach for this

- Cross-process IPC: the substrate's mmap'd rings are faster
  AND give you the page cache for free (which you usually want).
- Workloads where the page cache IS the buffer (read-heavy with
  reuse) - O_DIRECT just throws away that cache.

## References

- [`AdaptiveRing`](../../rings/shared-ring-adaptive/) - the
  default mmap'd ring family.
- [`LocaleAdaptiveRing`](../../rings/locale-adaptive-ring/) - the
  three-locale (Anon / File / ShmFs) substrate ring.
