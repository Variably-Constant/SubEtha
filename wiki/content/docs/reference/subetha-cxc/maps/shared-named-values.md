---
title: "Shared Named Values"
weight: 15
---

# SharedNamedValues

Byte values under case-insensitive names, shared by every process that
opens the same three files: a [`RawHashMap`](../shared-hash-map/) from
the hash of each name to the block that holds its value, a
[`RawArena`](../../arenas/raw-arena/) of blocks, and a `SharedEpochs`
table that keeps a block readable while a reader who found it is still
reading. It is what the `shm:` PowerShell drive stores its values in.

## Files

`create(dir, stem, layout)` obtains `<stem>.map`, `<stem>.arena` and
`<stem>.epochs` in `dir`, creating each that is absent and attaching to
each that is present. The layout names the map's capacity (the most
names the store holds), the arena's block space and class range, and
the pin slots (the most readers at once, across every process).

## A value's life

- `set(name, value)` takes a block for the name and the value, writes
  both, swaps the block's handle into the map under the name's hash,
  publishes the block, and retires the block the swap replaced. A name
  the store holds keeps the spelling it was first written with.
- `get(name)` pins the current epoch, looks the hash up, reads the
  block, and checks the name stored in it against the one asked for.
  An entry that names a block still being written belongs to a writer
  between its swap and its publish: the reader yields until the writer
  publishes, and collects when the writer has died, which publishes it.
- `remove(name)` takes the entry out of the map and retires its block.
- `entries()` lists every name as first written with its value.

A block retired at an epoch stays readable until every pin taken before
that epoch is released, then comes free through the arena's reclaim.

## Names

A name is compared without regard to case: its key is the 128-bit
FNV-1a hash of its lowercase form (`name_key`). The name as first
written is stored ahead of the value, so a listing shows it and so two
names whose hashes collide are told apart: the second is refused with
`NameCollision`, naming both, never merged with the first. A name is at
most 65,535 bytes of UTF-8.

## Errors

| Error | When |
|---|---|
| `TooManyNames { capacity }` | the map has no slot left for a new name |
| `TooLarge { bytes, max }` | the name and value together need more than the largest block holds |
| `Full { bytes }` | no block could be found for the value, after collecting |
| `NameCollision { stored, asked }` | another name with the same hash holds the entry |
| `NameTooLong { bytes, max }` | the name is longer than 65,535 bytes |
| `Corrupt` | a block's stored name cannot be read |
| `Map`, `Arena`, `Epochs` | the underlying structure's error |

## Recovery

A writer that dies between taking a block and publishing it, or between
the swap and the retire, leaves a block the map does not reach, or
reaches while the block is still marked as being written. When the
arena reports itself exhausted, `collect()` walks it with the live map
as the root set: a block is reached when the map's entry for the name
stored in it is that very block, which is read from the map as the walk
goes rather than from a snapshot, so a value swapped in during the walk
is never taken for garbage.

## Verified

The module's unit tests run a store of 16 names over a 64 KiB arena with
classes 6 to 12 and 4 pins: a value comes back under its name in any
case; replacing a value retires its block and a pinned reader still
reads it; removing a name retires its block; entries list every name as
first written; a name whose hash another name holds is refused; a store
holds its names and no more; a full arena is collected with the map as
its root set; names and values past their bounds are refused; a second
handle on the same files sees the first one's values.
