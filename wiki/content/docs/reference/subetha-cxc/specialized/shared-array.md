---
title: "Shared Array"
weight: 17
---

# SharedArray

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/write_once-seal-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-read--only-success)
![Density](https://img.shields.io/badge/stride-element_size-informational)

A flat, fixed-stride array written once and read by many processes. One
writer fills it and seals it; from then on it is read-only for every
process, including the one that made it. The stride is exactly the
element size, there is no per-element synchronization word, and the whole
span is readable as one slice.

> **The "baked table many processes read" primitive.** Every other
> array-shaped primitive here gives each element its own synchronization
> word and rounds its stride up to a cache line, so a reader never sees a
> half-written element. That is the right trade for something written
> while it is read, and pure cost for a table baked once and never
> written again: a 12-byte row costs 64 bytes of file, and a 3.5 MB table
> lands at 18 MB. Here 1000 elements of 12 bytes occupy 64 + 12,000 bytes
> on disk, header included.

**Constraints (read first):**

- **One writer, then seal.** `seal()` flushes, sets the flag, and flushes
  again, so a reader that sees the seal sees the data behind it. Every
  later write is refused through any handle, in any process.
- **A reader attached before the seal sees whatever the writer has
  reached.** There is no per-element version, so there is no torn-read
  detection; this primitive is for a table read after it is finished.
- **The stride is in the header and every attach checks it.** A reader
  using a different element size would read misaligned bytes that look
  exactly like data, so a disagreement is refused instead.
- **Read-only attachments carry no write permission.** `open_read_only`
  maps without it, so a reader cannot modify the file even through a
  mistake in its own code.
- **Fixed shape at create.** `len` and `stride` are locked into the
  header; both must be non-zero.
- **`create` does not truncate.** It attaches to an array already at the
  path when the shape matches, so reopening a baked table does not
  destroy it.

---

## Table of contents

- [What it is](#what-it-is)
- [Operation table](#operation-table)
- [Errors](#errors)
- [Worked example](#worked-example)
- [When not to reach for this](#when-not-to-reach-for-this)
- [References](#references)

---

## What it is

```text
+---------------------------+
| ArrayHeader (64B)         |  magic + stride + len + sealed
+---------------------------+
| element 0                 |  stride bytes, no padding
| element 1                 |
| ...                       |
| element len-1             |
+---------------------------+
```

Total file size: `64 + len * stride` bytes, which is what
`array_file_size(len, stride)` returns. The header is cache-line aligned
so the data behind it starts on a line; the elements themselves are not
padded.

---

## Operation table

| Call | What it does |
|---|---|
| `SharedArray::create(path, len, stride)` | Create at the path, or attach to an array already there with the same shape. Elements start zeroed and the array starts unsealed. |
| `SharedArray::open_read_only(path, expected_len, expected_stride)` | Attach without write access. The header must agree about both the stride and the length. |
| `fill_from(&[u8])` | Fill the whole array from a contiguous buffer of `len * stride` bytes, in one copy. |
| `set(index, &[u8])` | Write one element. |
| `seal()` | Mark the array finished. Flushes, sets the flag, flushes again. |
| `get(index) -> &[u8]` | One element, without copying. |
| `as_slice() -> &[u8]` | Every element as one slice, without copying. |
| `len()` / `is_empty()` / `stride()` | The shape, as the header records it. |
| `is_sealed()` / `is_writable()` | Whether the writer has finished, and whether this handle may still write. |
| `flush()` | Push to disk and wait. A no-op on a read-only attachment. |

---

## Errors

| `ArrayError` | When |
|---|---|
| `LayoutMismatch` | The file is not an array, or its header states another shape. |
| `OutOfBounds { index, len }` | An index past the end. |
| `WrongSize { expected, found }` | A value slice that was not the element size, or a `fill_from` buffer that was not `len * stride`. |
| `Sealed` | A write to an array that has been sealed. |
| `ReadOnly` | A write through a read-only attachment. |
| `EmptyShape` | Zero elements, or a zero-byte element. |
| `IoError(kind)` | The file or the mapping refused. |

---

## Worked example

```rust
use subetha_cxc::shared_array::SharedArray;

// Bake a table of 12-byte rows: a u64 key and a u32 value.
let mut rows = Vec::new();
for i in 0u64..1000 {
    rows.extend_from_slice(&i.to_le_bytes());
    rows.extend_from_slice(&(i as u32 * 7).to_le_bytes());
}

let mut table = SharedArray::create("/tmp/lookup.bin", 1000, 12)?;
table.fill_from(&rows)?;
table.seal()?;

// Any process reads it without write access, and pays nothing per element.
let reader = SharedArray::open_read_only("/tmp/lookup.bin", 1000, 12)?;
let all = reader.as_slice();
let row = reader.get(37)?;
```

---

## When not to reach for this

| Use case | Reach for instead |
|---|---|
| A table written while it is read | [Shared Slab](../shared-slab/) or [Shared Vec](../shared-vec/): one SeqLock per slot, so a reader racing the writer retries rather than seeing a mixture. |
| Records appended at run time | [Shared Vec](../shared-vec/): bump-pointer `push_back` with a length the readers see. |
| A caller-chosen index over records that change | [Shared Slab](../shared-slab/). |
| Recovering the per-slot cost without giving up the SeqLock | Pack the payload to a slot: a slot-sized array such as `[u8; 56]` keeps the seqlock at 1.14 times the payload rather than 64. |

---

## References

- Source: [shared_array.rs](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_array.rs)
- Page: [SHARED_ARRAY.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ARRAY.md)
