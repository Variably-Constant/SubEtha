# FrameRing + FrameRegion

Self-describing variable-payload single-producer / single-consumer
ring. Where `SpscRingCore` / `SharedRingSpsc` carry a fixed 64-byte
payload and reject anything larger, `FrameRing` makes the payload
layout part of the record: every record is a frame - a class tag plus
a length - so one ring carries a payload of any size, inlining small
records and spilling large ones to a co-located byte region. This is
the QUIC frame model (a type tag plus length-delimited fields) applied
to the ring slot.

> **The "self-describing frame" primitive.** The producer writes the
> class tag; the consumer reads it to recover the bytes. Small records
> live inline in the descriptor slot; large records live in the byte
> region and the descriptor carries the offset. The indirection is
> paid per record, only when a record is too big to inline.

For the same capability across every shape of the morphing main ring
(SPSC / MPSC / MPMC / Vyukov) with a producer override, see
`AdaptiveRing::send_frame` in `SHARED_RING_ADAPTIVE.md`. `FrameRing`
is the dedicated single-producer form.

## Two layers

1. **Descriptor ring** - a fixed-stride Lamport SPSC ring (one
   producer-owned `desc_head`, one consumer-owned `desc_tail`). Each
   slot: `[class:u8][_pad:3][len:u32][ inline-bytes | region_off:u64 ]`.
   Fixed stride keeps O(1) addressing, the one-Acquire-one-Release
   atomic budget, and cache-line isolation.
2. **Payload region** - a bip-buffer byte ring with absolute-monotonic
   `region_head` / `region_tail` cursors and skip-pad on wrap. Records
   spill here only when they exceed the inline budget.

## Constraints

- **Single producer, single consumer** - the caller upholds it;
  `FrameRing` is `Send + Sync` but does not enforce it at the type
  level.
- **`slot_size >= MIN_SLOT_SIZE` (16)**; inline budget is
  `slot_size - DESC_HEADER_BYTES` (8).
- **`capacity` and `region_bytes` are powers of two.** A region
  payload is capped at `region_bytes / 2` so a skip-pad on an empty
  region never reports a false `Full`.

## API surface

```rust
impl FrameRing {
    pub fn create_anon(capacity: usize, slot_size: usize, region_bytes: usize) -> Result<Self, RingError>;
    pub fn create(path, capacity, slot_size, region_bytes) -> Result<Self, RingError>;
    pub fn open(path, capacity, slot_size, region_bytes) -> Result<Self, RingError>;
    pub fn create_from_shm(shm, capacity, slot_size, region_bytes) -> Result<Self, RingError>;
    pub fn open_from_shm(shm, capacity, slot_size, region_bytes) -> Result<Self, RingError>;

    pub fn send(&self, payload: &[u8]) -> Result<FrameClass, RingError>;          // Auto
    pub fn send_as(&self, payload: &[u8], hint: LayoutHint) -> Result<FrameClass, RingError>;
    pub fn recv(&self) -> Result<Vec<u8>, RingError>;
    pub fn recv_into(&self, out: &mut Vec<u8>) -> Result<FrameClass, RingError>;

    pub fn inline_budget(&self) -> usize;   // slot_size - 8
    pub fn max_payload(&self) -> usize;     // region_bytes / 2
    pub fn capacity(&self) -> usize;
    pub fn approx_len(&self) -> usize;
}

pub enum FrameClass { Inline, Offset }
pub enum LayoutHint { Auto, ForceInline, ForceOffset }
```

`send` inlines when `payload.len() <= inline_budget`, else spills to
the region. `send_as` forces the choice (`ForceInline` rejects an
over-budget payload, `ForceOffset` always spills). The consumer never
overrides: `recv` / `recv_into` read the class the producer wrote.

`FrameRegion` is the standalone concurrent fixed-block region the
`AdaptiveRing` frame path uses (multi-producer alloc, multi-consumer
free via an ABA-countered Treiber free list plus a bump high-water
mark; `create_anon` / `create` / `create_from_shm`). `FrameRing`'s own
region is the single-producer bip-buffer above.

## Worked example

```rust
use subetha_cxc::frame_ring::{FrameRing, LayoutHint};
use subetha_cxc::FrameClass;

let ring = FrameRing::create_anon(1024, 64, 1 << 20)?; // 56-byte inline budget
assert_eq!(ring.send(b"small")?, FrameClass::Inline);
assert_eq!(ring.send(&vec![0u8; 4096])?, FrameClass::Offset);
let mut buf = Vec::new();
assert_eq!(ring.recv_into(&mut buf)?, FrameClass::Inline);
assert_eq!(buf, b"small");
```

## Bench evidence

`examples/frame_payload_sweep.rs` (single producer + consumer
round-trip, min-of-5, 200k iters, release, Zen+ R7 2700 / Windows 11):
the inline fast path beats the always-region path 1.13-1.69x for
records up to the 56-byte inline budget (16 B: 16.9 vs 27.0 ns; 32 B:
18.8 vs 31.8 ns; 56 B: 29.7 vs 33.7 ns). At 64 bytes and up both take
the region path and tie.

## Known limitations

- **One producer + one consumer** - for many-producer / many-consumer
  variable payloads use `AdaptiveRing::send_frame` (concurrency-safe
  region).
- **Region payload capped at `region_bytes / 2`** - larger records
  return `RingError::PayloadTooLarge`.
- **Slot below a cache line risks false sharing** - keep
  `slot_size >= 64`.

## References

- Source: `crates/subetha-cxc/src/frame_ring.rs`,
  `crates/subetha-cxc/src/frame_region.rs`.
- Bench: `crates/subetha-cxc/examples/frame_payload_sweep.rs`.
- All-shapes form: `SHARED_RING_ADAPTIVE.md` (`send_frame`).
