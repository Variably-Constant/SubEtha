---
title: "Shared Slab"
weight: 15
---

# SharedSlab&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/per--slot-SeqLock-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Fixed-capacity slab of records, each slot its own SeqLock cell,
addressed by an index the caller chooses. `set(i, v)` writes a slot,
`get(i)` reads one, and a reader racing the writer of that slot
retries rather than seeing a mixture.

> **The "record too large for a cache line, still read without a
> lock" primitive.** `SharedVec` gives per-slot SeqLock reads but
> caps a payload at `VEC_PAYLOAD_BYTES` = 52, because its slot is
> one cache line. `SharedRegion` carries records of any size but
> reads and writes a slot plainly, so a concurrent read tears.
> `SharedSlab` is the third point: any record size, the caller's own
> index, the same SeqLock.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + 'static`**. A slot is a byte copy; no `Drop` runs.
- **One writer per slot, any number of readers.** Two writers on the
  same slot race: the SeqLock makes a torn read detectable, and does
  not make a torn write safe. A caller writing one index from two
  threads serializes that itself.
- **The index is the caller's.** There is no allocator, no free list
  and no length. A caller that persists ids - a write-ahead log
  naming a slot, a snapshot restoring one - keeps addressing them
  itself, and a released id never comes back pointing at another
  record.
- **A slot spans whole cache lines.** The stride is the version word
  plus the record rounded up to a multiple of 64, and the array is
  64-byte aligned, so no slot's version shares a line with another
  slot's payload. A 168-byte record takes a 192-byte slot.
- **A multi-line slot keeps the guarantee.** The version is a single
  atomic and the reader's two loads bracket the whole copy, so a tear
  across three lines is caught exactly as one within a line is. Size
  changes the retry cost - the slot is held odd for as long as the
  copy takes - not the correctness.
- **A slot nothing has written reads as the zero bit pattern of `T`.**
  There is no occupancy bit. A caller that must tell absent from
  written encodes that in `T`, which a record whose zero value already
  means absent gets for free.
- **Bounded capacity at create**: no auto-grow. Segment and seal.
- **A different record size is a `LayoutMismatch`**: the header carries
  the slot stride, so the same file opened as the wrong type is
  refused rather than returning records sliced at the wrong offset.
- **`open_read_only` maps without write access**: `open` needs a
  read+write file handle, which a consumer of a privileged producer's
  slab does not hold. Reads are identical; `set` returns
  `SlabError::ReadOnly`.
- **Cross-process backed by MMF.**

---

## API

| Call | Behavior |
|---|---|
| `SharedSlab::<T>::create(path, capacity)` | Obtain the slab: initialize when the path does not yet exist, else attach with live records intact. |
| `SharedSlab::<T>::reset(path, capacity)` | Truncate and reinitialize, discarding every record live peers share. |
| `SharedSlab::<T>::open(path, expected_capacity)` | Attach to an existing slab. |
| `SharedSlab::<T>::open_read_only(path, expected_capacity)` | Attach without write access. |
| `slab.get(i) -> Result<T, SlabError>` | SeqLock read of slot `i`. |
| `slab.set(i, v) -> Result<(), SlabError>` | SeqLock write of slot `i`. |
| `slab.slot_version(i)` | Writes to slot `i`; even at rest, odd while held. |
| `slab.capacity()` / `slab.is_writable()` / `slab.flush()` | Slots addressed; whether this mapping may write; msync. |
| `slab_slot_size::<T>()` / `slab_file_size::<T>(capacity)` | Stride of one slot; bytes the file needs. |

`SlabError` is `OutOfBounds` / `LayoutMismatch` / `ReadOnly` /
`IoError`.

---

## Worked examples

### Records past the cache line

```rust
use subetha_cxc::SharedSlab;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Record { id: u64, payload: [u8; 160] }

let slab: SharedSlab<Record> = SharedSlab::create("/tmp/cells.bin", 1 << 20)?;
slab.set(4211, Record { id: 4211, payload: [7; 160] })?;
let back = slab.get(4211)?;
assert_eq!(back.id, 4211);
```

### Segmenting past one file

Capacity is fixed at create, so a store larger than one segment picks
a slot count per file and addresses by division:

```rust
let seg = index / SLOTS_PER_SEGMENT;
let slot = index % SLOTS_PER_SEGMENT;
segments[seg].get(slot)?
```

---

## Use case patterns

### Pattern: a store whose ids outlive the process

The index is the caller's, so an id written to a log or a snapshot
still names the same record after a restart. A slab is the backing
for a store that hands ids out and takes them back.

### Pattern: a privileged writer with unprivileged readers

The writer holds the read+write handle and calls `set`; readers
attach with `open_read_only` and see every completed write without a
lock and without write access to the file.

---

## Known limitations

- **Bounded capacity at create**: no auto-grow. Segment and seal.
- **No allocator and no length.** Addressing is entirely the
  caller's; a slab does not know which slots are in use.
- **`T: Copy`**: pointer-bearing T needs indirection.

---

## Common pitfalls

- **Two threads writing one slot.** The SeqLock detects a torn read,
  not a torn write. Serialize the writer per slot.

- **Reading a slot as an occupancy test.** An unwritten slot reads as
  zeroes, not as an error. Encode absence in `T`.

- **Opening with a different `T`.** The stride comes from
  `size_of::<T>()`, so a mismatch is refused as `LayoutMismatch`
  rather than returning misaligned records.

- **Reaching for a slab where a `SharedVec` fits.** A record of 52
  bytes or fewer with append-plus-index semantics is what `SharedVec`
  is; the slab costs a wider stride to carry the general case.

---

## References

- Source: `crates/subetha-cxc/src/shared_slab.rs`.
- Sibling primitive: [Shared Vec](../shared-vec/) - the same SeqLock
  with a one-cache-line slot and append-plus-index semantics.
- Sibling primitive: [Shared Region](../arenas/shared-region/) -
  records of any size with an allocator and a free list, read and
  written plainly.
