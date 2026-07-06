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
- **`capacity` and `region_bytes` are powers of two.** A region
  payload is capped at `region_bytes / 2` so a skip-pad on an empty
  region can never report a false `Full`.
- **In-process anonymous** (`create_anon`), **file-backed**
  (`create` / `open`), or **named shared memory**
  (`create_from_shm` / `open_from_shm`) - same byte layout, same
  protocol.

## Per-record layout selection

| Call | Behavior |
|---|---|
| `send(payload)` | Inline when `payload.len() <= inline_budget`, else region. Returns the `FrameClass` chosen. |
| `send_as(payload, LayoutHint::ForceInline)` | Inline; `Err(PayloadTooLarge)` if over budget. |
| `send_as(payload, LayoutHint::ForceOffset)` | Always the region. |
| `recv_into(&mut Vec<u8>)` | Clears and fills the buffer; reads the region and frees nothing (the region tail follows FIFO). Returns the `FrameClass`. |
| `recv()` | Same as `recv_into` into a fresh `Vec`. |

The consumer never overrides the layout: it reads the class the
producer wrote, because it cannot know the layout otherwise.

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
| 16 B | 16.9 ns | inline | 27.0 ns | 14.4 ns | 1.60x |
| 32 B | 18.8 ns | inline | 31.8 ns | 8.9 ns | 1.69x |
| 56 B | 29.7 ns | inline | 33.7 ns | 13.5 ns | 1.13x |
| 64 B | 35.8 ns | offset | 37.0 ns | - | 1.03x |
| 1024 B | 152.9 ns | offset | 121.1 ns | - | ~1.0x |

The inline fast path beats the always-region path 1.13-1.69x for
records up to the 56-byte inline budget; at 64 bytes and above both
take the region path and tie (the spread there is run-to-run noise on
the identical path). The frame header costs a few ns over the rawest
fixed slot in exchange for carrying any size.

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
