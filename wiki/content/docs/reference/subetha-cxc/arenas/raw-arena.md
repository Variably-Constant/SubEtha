---
title: "Raw Arena"
weight: 25
---

# RawArena

Power-of-two blocks carved from one shared file, handed to a writer,
published, retired under an epoch and reused, with every step
recoverable from the block headers after a crash. It is the value
store behind [`SharedNamedValues`](../../maps/shared-named-values/) and
the `shm:` PowerShell drive.

## Layout

```text
+------------------------------------------+
| header: magic, capacity, classes, bump,  |
|   the collector's slot, scan hints       |
+------------------------------------------+
| free bitmap, one per class               |
| retired bitmap, one per class            |
+------------------------------------------+
| block space: capacity bytes              |
+------------------------------------------+
```

A block of class `c` is `2^c` bytes: a 32-byte header and the payload.
The block space is carved from the front in chunks of the largest
class, and a chunk is halved down to the class a writer needs, so every
block of class `c` starts at a multiple of `2^c` and the pair `(class,
offset >> class)` names it. Each class has one bit per possible block in
its free bitmap and one in its retired bitmap; for 256 MiB of block
space and classes 6 to 25 the two sets take about 2 MiB.

`create(path, capacity, min_class, max_class)` obtains the arena,
building an empty one when the path is absent and attaching when it is
present; a region built with another capacity or class range is a
`LayoutMismatch`. `capacity` is a positive multiple of the largest
class's block, and the classes lie in 6 to 40.

## A block's life

```text
Unbuilt -> Free -> Writing(pid) -> Published -> Retired(epoch) -> Free
```

Each step is one compare-and-swap on the block's state word, whose top
byte is the state and whose other 56 bits carry the writer's pid or the
retire epoch.

- `allocate(len, &epochs)` takes a block for `len` payload bytes and
  marks it `Writing` by this process: a free block of the smallest class
  that holds it; else one reclaimed from that class's retired blocks no
  pin can see; else the next chunk, carved and halved; else a larger
  free block, halved. `Exhausted` when none of those exist.
- `write_payload(h, bytes)` copies the bytes in and records their
  length.
- `publish(h)` marks the block `Published`. A writer publishes only once
  the handle is reachable from the structure that names its values, so
  a published block the roots do not reach was replaced, never
  half-made.
- `retire(h, &epochs)` marks a published block `Retired` at the epoch
  `SharedEpochs::advance` hands back, and it comes free through the
  reclaim once `SharedEpochs::reclaim_horizon` has passed that epoch. A
  reader that took a pin before it learned the handle still reads it.
- `abandon(h)` gives a block back without publishing it.
- `read_into(h, &mut out)` copies a published or retired block's
  payload out; `Ok(false)` when the block holds no value under the
  handle.
- `peek_payload(h, &mut out)` copies any built block's payload whatever
  its state, for a collector that reads which name a block was written
  for; `writer_of(h)` names the process writing a block.

## Bitmaps, not lists

A free or retired bit is set with a `fetch_or`, so setting it twice is
setting it once, and a crash between a state change and its bit leaves
a block a walk can place from its header alone. A bit says the block
may be taken; taking it is the state compare-and-swap, so a bit left
over from an earlier life costs one failed swap and is cleared.

## Recovery

Nothing repairs the file. When a writer finds its class empty after
reclaiming, no chunk left to carve and no larger block to halve, the
caller runs `collect(&epochs, reached)`, with `reached` saying whether
its roots name a block. One process collects at a time, holding a
pid-stamped slot that is reaped when its holder has died; the collector
walks the block headers from the front to the bump and puts each block
where its header says it belongs:

| The walk finds | It does |
|---|---|
| an unbuilt chunk (its carver died before writing the header) | makes it free |
| a free or retired block | gives it its bit back |
| a block a live process is writing | leaves it |
| a block a dead process was writing, which the roots reach | publishes it: its payload was complete before its handle became reachable |
| a block a dead process was writing, which the roots do not reach | retires it at the current epoch |
| a published block the roots do not reach | retires it at the current epoch |

The ordinary reclaim then returns whatever no pin can see. The
`CollectReport` says how many blocks took each path.

## Verified

The module's unit tests run a 64 KiB arena with classes 6 to 12 and a
4-pin epoch table: a block holds its payload once published; halving a
chunk frees each spare half; a retired block comes back once no pin
sees it; a full arena reports itself and collecting its unreached
blocks refills it; collecting publishes a dead writer's reached block
and retires its other; collecting frees a chunk whose carver died
before its header; a stale free bit costs one failed swap; collecting
gives free and retired blocks their bits back; writers on every logical
processor take distinct blocks until the arena is full; handles outside
the arena and payloads past the largest block are refused; an
abandoned block is free again and `open` refuses another shape.
