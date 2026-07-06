---
title: "PubSub Ring"
weight: 23
---

# PubSubRing + PubSubSubscriber

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Axis](https://img.shields.io/badge/axis-protocol--family-brightgreen)

One-producer many-subscriber broadcast primitive with
per-subscriber positions. Where a regular ring (`SpscRingCore`)
has one consumer position, PubSubRing exposes the producer's
monotonic head as the absolute position and lets each subscriber
walk positions independently via its own
[`SubscriberPosition`](../../coordination-types/subscriber-position/).

## Slot layout

| Bytes | Field |
|---|---|
| 0..8 | `AtomicU64` sequence (wraparound detector) |
| 8..64 | 56-byte payload (`PUBSUB_PAYLOAD_BYTES`) |

64-byte slot, cache-line aligned. Capacity is `pow2 >= 2`.

## API

### Constructors and locales

| Call | Locale | Visibility |
|---|---|---|
| `PubSubRing::create_anon(capacity)` | Anonymous mmap | In-process only; fastest construction. |
| `PubSubRing::create(path, capacity)` | File-backed | Cross-process via OS page cache. |
| `PubSubRing::open(path, expected_capacity)` | File-backed | Open an existing file-backed ring. Validates magic + capacity + slot_size. |
| `PubSubRing::create_from_shm(shm, capacity)` | Named shared memory | Cross-process RAM-resident (`/dev/shm` on Linux, named section on Windows). |
| `PubSubRing::open_from_shm(shm, expected_capacity)` | Named shared memory | Open an existing named-shm region without re-initialising. |

`capacity` must be a power of two >= 2 for every constructor.

### Operations

| Call | Behavior |
|---|---|
| `ring.publish(payload: &[u8]) -> u64` | Single producer writes one item; returns the assigned position. |
| `ring.read_at(position, out: &mut [u8]) -> Result<(), PubSubReadError>` | Read at absolute position; copies payload into `out`. |
| `ring.head() -> u64` | Acquire load of the producer's head. |
| `ring.capacity() -> usize` | Slot count. |
| `pubsub_ring_file_size(capacity) -> usize` | Bytes required for a capacity-`N` ring. |

`PubSubReadError`:
- `Pending` - position hasn't been published yet.
- `Lost` - position has been overwritten by the producer (subscriber lagged > capacity).

## PubSubSubscriber wrapper

| Call | Behavior |
|---|---|
| `PubSubSubscriber::new(ring: Arc<PubSubRing>, position: SubscriberPosition)` | Pair a ring + per-subscriber position. |
| `sub.position() -> u64` | Current absolute position. |
| `sub.ring() -> &Arc<PubSubRing>` | Borrow the underlying ring. |
| `sub.skip(n: u64) -> u64` | Advance position by `n` without reading. |
| `sub.try_next(out: &mut [u8]) -> Result<(), PubSubReadError>` | Read at current position + advance by 1. On `Lost`, position jumps to current head (skip-past-gap). |

## Worked example

```rust,no_run
use std::sync::Arc;
use subetha_cxc::protocol_pubsub::{
    PubSubRing, PubSubSubscriber, PUBSUB_PAYLOAD_BYTES,
};
use subetha_cxc::replay_positions::SubscriberPosition;

let ring = Arc::new(PubSubRing::create_anon(1024)?);
let pos_a = SubscriberPosition::create("/tmp/pos_a.bin", 0)?;
let pos_b = SubscriberPosition::create("/tmp/pos_b.bin", 0)?;
let sub_a = PubSubSubscriber::new(ring.clone(), pos_a);
let sub_b = PubSubSubscriber::new(ring.clone(), pos_b);

for i in 0u64..5 {
    let mut buf = [0u8; PUBSUB_PAYLOAD_BYTES];
    buf[..8].copy_from_slice(&i.to_le_bytes());
    ring.publish(&buf);
}

let mut out = [0u8; PUBSUB_PAYLOAD_BYTES];
sub_a.try_next(&mut out)?;  // sub_a now at position 1
sub_a.try_next(&mut out)?;  // sub_a now at position 2
sub_b.try_next(&mut out)?;  // sub_b independently at position 1
# Ok::<(), Box<dyn std::error::Error>>(())
```

## E2E proof

[`examples/pubsub_fanout.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/pubsub_fanout.rs)
runs 1 producer + 3 subscribers: subs A and B drain every item
(full sum), sub C uses `skip()` to consume only even positions
(half sum). 10000 items integrity, deterministic.

## When to reach for this primitive

- One producer fans out to N independent subscribers with
  separate read positions.
- Subscribers that want restart-resume via
  [`SubscriberPosition`](../../coordination-types/subscriber-position/).
- Workloads where some subscribers want every item and others
  want sampling (use `skip()`).

## When NOT to reach for this

- Point-to-point single-producer / single-consumer
  (use [SPSC](../shared-ring-spsc/) or
  [AdaptiveRing](../shared-ring-adaptive/) in SPSC shape).
- Multiple producers fanning into one consumer
  (use [MPSC](../shared-ring-mpsc/)).

## References

- Source: `crates/subetha-cxc/src/protocol_pubsub.rs` (475 lines, 5
  unit tests: publish-then-read, pending-for-unpublished,
  lost-for-overwritten, two-subscribers-independent-positions,
  subscriber-skips-past-lost). `PubSubRing` / `PubSubSubscriber` /
  `PUBSUB_PAYLOAD_BYTES` live in the `pub mod protocol_pubsub`
  module path (not re-exported at the crate root).
- [`SubscriberPosition`](../../coordination-types/subscriber-position/) -
  the per-subscriber MMF-resident position.
- [`QosPolicy`](../../coordination-types/qos-policy/) - the policy
  framework whose `reliable_pubsub_default` preset matches this
  protocol family.
