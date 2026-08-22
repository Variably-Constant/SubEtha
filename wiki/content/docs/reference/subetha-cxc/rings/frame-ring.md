---
title: "Frame Ring"
weight: 16
---

# FrameRing

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/protocol-self--describing_frame-brightgreen)
![Slot](https://img.shields.io/badge/payload-any_size-informational)

Self-describing variable-payload single-producer / single-consumer
ring. Where [`SharedRingSpsc`](../shared-ring-spsc/) carries a fixed
64-byte payload and rejects anything larger, `FrameRing` makes the
payload layout part of the record: every record is a frame - a class
tag plus a length - so one ring carries a payload of any size,
inlining the small ones and spilling the large ones to a co-located
byte region. This is the QUIC frame model (a type tag plus
length-delimited fields) applied to the ring slot.

> **The "self-describing frame" primitive.** The producer writes a
> class tag; the consumer reads it to know how to recover the bytes.
> Small records live inline in the descriptor slot; large records live
> in the byte region and the descriptor carries the offset. The
> indirection is paid per record, only when a record is too big to
> inline.

For the same capability woven into the morphing main ring across every
shape (SPSC / MPSC / MPMC / Vyukov), with a producer override, see the
[AdaptiveRing frame path](../shared-ring-adaptive/#the-payload-size-axis).
`FrameRing` is the dedicated single-producer form.

## The two layers

1. **Descriptor ring** - a fixed-stride Lamport SPSC ring (a
   producer-owned `desc_head`, a consumer-owned `desc_tail`). Fixed
   stride keeps the O(1) `index -> address` arithmetic, the
   one-Acquire-one-Release atomic budget, and cache-line isolation
   that the raw SPSC ring earns. Each slot is
   `[class:u8][_pad:3][len:u32][ inline-bytes | region_off:u64 ]`.
2. **Payload region** - a bip-buffer byte ring with absolute-monotonic
   `region_head` / `region_tail` cursors. Records spill here only when
   they exceed the inline budget; the descriptor then carries the
   region offset instead of the bytes.

## Constraints

- **Single producer, single consumer** - the caller upholds the SPSC
  discipline (`send` is the sole producer, `recv` the sole consumer);
  `FrameRing` is `Send + Sync` and does not enforce it at the type
  level (unlike the typed `SharedRingSpsc` pair).
- **`slot_size >= 16`** (the descriptor header is 8 bytes; the offset
  form needs 8 more). Inline budget is `slot_size - 8`.
- **`capacity` and `region_bytes` are powers of two**, each at least 2.
  A region payload is capped at `region_bytes / 2` so a skip-pad on an
  empty region can never report a false `Full`.
- **The geometry is validated, not asserted.** Every constructor runs
  the checks above and returns `Err(RingError::LayoutMismatch)` on a
  bad one. This is the exception in the ring family - `SharedRing` and
  `SpscRingCore` assert on a non-pow2 capacity - so `FrameRing` is the
  one you can hand a number straight from configuration.
- **In-process anonymous** (`create_anon`), **file-backed**
  (`create` / `open`), or **named shared memory**
  (`create_from_shm` / `open_from_shm`) - same byte layout, same
  protocol.
- **File-backed `create` obtains the ring**: it initializes the file
  only when the path does not yet exist, and otherwise attaches with
  queued frames and both region cursors intact; a ring built with
  different parameters is a `LayoutMismatch`. Racing creators on one
  path elect one initializer. `reset` is the call that truncates and
  reinitializes, and on Windows it succeeds only once every mapping
  handle is gone.

## Per-record layout selection

| Call | Behavior |
|---|---|
| `send(payload)` | Inline when `payload.len() <= inline_budget`, else region. Returns the `FrameClass` chosen. |
| `send_as(payload, LayoutHint::ForceInline)` | Inline; `Err(PayloadTooLarge)` if over budget. |
| `send_as(payload, LayoutHint::ForceOffset)` | Always the region. |
| `recv_into(&mut Vec<u8>)` | Clears and fills the buffer; reads the region and frees nothing (the region tail follows FIFO). Returns the `FrameClass`. |
| `recv()` | Same as `recv_into` into a fresh `Vec`, and returns that `Vec` - the class is read to recover the bytes but not handed back. Use `recv_into` when you want it. |

The consumer never overrides the layout: it reads the class the
producer wrote, because it cannot know the layout otherwise.

`capacity()`, `slot_size()`, `region_bytes()` and `inline_budget()`
report the constructed geometry. `inline_budget()` is the one worth
reading at runtime: it is what `send` compares against to choose a
layout, so it is the threshold a caller sizes its records around.

## Worked example

```rust
use subetha_cxc::frame_ring::{FrameRing, LayoutHint};
use subetha_cxc::FrameClass;

// 64-byte slots (56-byte inline budget), 1 MiB spill region.
let ring = FrameRing::create_anon(1024, 64, 1 << 20)?;

// Small record inlines; large record spills to the region.
assert_eq!(ring.send(b"small")?, FrameClass::Inline);
assert_eq!(ring.send(&vec![0u8; 4096])?, FrameClass::Offset);
// Force a small record through the region if you want to.
ring.send_as(b"forced", LayoutHint::ForceOffset)?;

let mut buf = Vec::new();
assert_eq!(ring.recv_into(&mut buf)?, FrameClass::Inline);
assert_eq!(buf, b"small");
```

## Bench evidence

`crates/subetha-cxc/examples/frame_payload_sweep.rs`, single
producer + consumer round-trip, min-of-5, 200,000 iterations per cell,
release build, Zen+ R7 2700 / Windows 11. `frame.auto` is the ring
picking inline/offset; `frame.offset` forces every record through the
region (the always-arena baseline); `raw.spsc` is the fixed 64-byte
`SpscRingCore` ceiling.

| Payload | frame.auto | class | frame.offset | raw.spsc | auto vs offset |
|--------:|-----------:|:------|-------------:|---------:|---------------:|
| 8 B | 9.3 ns | inline | 14.1 ns | 9.2 ns | 1.52x |
| 16 B | 9.0 ns | inline | 14.6 ns | 9.7 ns | 1.62x |
| 32 B | 9.0 ns | inline | 16.2 ns | 6.4 ns | 1.79x |
| 48 B | 15.9 ns | inline | 19.8 ns | 8.6 ns | 1.25x |
| 56 B | 15.7 ns | inline | 20.4 ns | 8.6 ns | 1.30x |
| 64 B | 20.8 ns | offset | 21.2 ns | 6.8 ns | 1.02x |
| 128 B | 23.0 ns | offset | 23.0 ns | - | 1.00x |
| 256 B | 32.9 ns | offset | 32.4 ns | - | 0.98x |
| 512 B | 52.8 ns | offset | 48.1 ns | - | 0.91x |
| 1024 B | 71.8 ns | offset | 70.7 ns | - | 0.98x |
| 4096 B | 224.4 ns | offset | 206.4 ns | - | 0.92x |

Read the shape, not the absolute numbers: consecutive runs on the same
machine move these by a factor approaching two, and the table is one
captured sweep.

The inline fast path beats the always-region path 1.25-1.79x for records
up to the 56-byte inline budget - that band is the whole point of the
class tag. At 64 bytes and above both paths are the region path, and
they tie within a few percent; `frame.auto` trails very slightly at 512
B and 4096 B, which is a measurement artifact of running first rather
than an extra cost, since past the budget `Auto` and `ForceOffset`
execute the same code after one length comparison.

`raw.spsc` stops at 64 B because that is the fixed slot's whole payload,
and it is the fastest column everywhere it appears: the frame header
costs a few ns over the rawest fixed slot, which is what buys carrying
any size at all.

## Known limitations

- **One producer + one consumer** - the caller upholds it; for
  many-producer / many-consumer variable payloads use the
  [AdaptiveRing frame path](../shared-ring-adaptive/#the-payload-size-axis),
  whose region is concurrency-safe.
- **Region payload capped at `region_bytes / 2`** - a larger record
  returns `RingError::PayloadTooLarge`; size `region_bytes` for the
  largest record you send.
- **Slot smaller than a cache line risks false sharing** - keep
  `slot_size >= 64` for cache-line isolation between adjacent
  descriptors.

## References

- Source: `crates/subetha-cxc/src/frame_ring.rs`.
- Bench: `crates/subetha-cxc/examples/frame_payload_sweep.rs`.
- All-shapes form: the
  [AdaptiveRing frame path](../shared-ring-adaptive/#the-payload-size-axis).
- Ring family siblings:
  [shared-ring-spsc](../shared-ring-spsc/) (fixed 64-byte SPSC),
  [shared-ring](../shared-ring/) (Vyukov MPMC).
