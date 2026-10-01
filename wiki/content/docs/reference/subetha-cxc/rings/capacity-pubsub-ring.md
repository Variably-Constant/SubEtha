---
title: "Capacity PubSub Ring"
weight: 25
---

# CapacityPubSubRing + CapacityPubSubSubscriber

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Layout](https://img.shields.io/badge/Layout-chain_of_backings-green)
![Axis](https://img.shields.io/badge/axis-capacity--morph-brightgreen)

Runtime-resizable wrapper around
[`PubSubRing`](../pubsub-ring/) that adds the capacity-axis morph
to the pub/sub (many-producer, many-subscriber, absolute-position)
primitive.

Sibling of [`CapacityAdaptiveRing`](../capacity-adaptive-ring/) and
[`CapacityBroadcastRing`](../capacity-broadcast-ring/). Where the
broadcast wrapper relies on the underlying ring's per-consumer
header state to track subscribers across morphs,
`CapacityPubSubRing` uses a chain-of-backings model because
`PubSubRing`'s per-subscriber position state lives outside the ring
(in `SubscriberPosition` / on the `PubSubSubscriber` wrapper).

## Chain-of-backings invariant

Each backing links the one a morph put after it. The wrapper holds
the active backing, where publishes go, and the oldest backing a new
subscriber from the start begins at; that oldest backing links every
backing after it, so it holds the chain. A
`CapacityPubSubSubscriber` holds:

- the backing it reads, which keeps that backing and every one after
  it alive until the subscriber moves on
- its position within that backing

On `try_next`, the subscriber reads at its position. On `Pending`,
when its backing has a successor linked, it crosses into the
successor at position 0; a successor is linked only once every
publish into the backing has returned, so the backing's content has
all drained before the subscriber crosses.

## Publish path

Any number of threads publish at once, and nothing takes a lock. A
publish announces itself on the active backing, then checks whether a
morph has sealed that backing. A sealed backing sends the publish to
the new active one; otherwise it publishes and withdraws its
announcement. A morph makes the new backing active, seals the old
one, waits for the publishes announced on it to return, and only then
links the new backing behind it. So no publish lands in a backing
after a subscriber has crossed out of it.

The subscriber path is lock-free: `try_next` reads the successor link
and the slot through the backing it holds.

## Constructors

| Call | Behavior |
|---|---|
| `CapacityPubSubRing::create_anon(initial_capacity) -> Arc<Self>` | In-process anon mmap. Returns Arc directly because subscribers need an Arc to subscribe through. |
| `CapacityPubSubRing::create(base_path, initial_capacity) -> Arc<Self>` | File-backed; per-morph file at `{base_path}.cap_{N}_g{morph_seq}.bin`. |
| `CapacityPubSubRing::create_shmfs(name_prefix, initial_capacity) -> Arc<Self>` | Named-shm; per-morph name at `{prefix}_cap_{N}_g{morph_seq}`. |

`initial_capacity` must be a power of two >= 2.

## API

### Wrapper

| Call | Behavior |
|---|---|
| `ring.current_capacity() -> usize` | The active backing's capacity. |
| `ring.pin_generation() -> u64` | Acquire-load the current pin generation. |
| `ring.publish(payload) -> u64` | Publish into the active backing, from any thread. Returns the position within that backing. |
| `ring.subscribe_from_now() -> CapacityPubSubSubscriber` | Subscribe starting from the active backing's current head. Late joiners see no history. |
| `ring.subscribe_from_oldest() -> CapacityPubSubSubscriber` | Subscribe starting from position 0 of the oldest chain entry. Subscriber drains every still-resident item. |
| `ring.morph_capacity_to(new_capacity) -> Result<(), PubSubCapacityMorphError>` | Make a fresh (or warm-cached) backing at the new capacity active, bump pin_generation, and link it behind the old one once the old one's publishes have returned. |
| `ring.prewarm(capacity) -> Result<(), PubSubCapacityMorphError>` | Speculatively build a backing into the one-slot warm cache, off the morph's critical path. |
| `ring.warm_capacity() -> Option<usize>` | Capacity currently held in the warm cache, if any. |
| `ring.warm_hits() -> u64` | Count of morphs that consumed a warm-cache prediction. |
| `ring.clear_warm()` | Drop the cached prediction (releasing its memory / file / shm region). |
| `ring.gc() -> usize` | Move the oldest kept backing forward past every backing nothing but the chain holds, so no subscriber reads it. Returns reclaimed-count. |
| `ring.ring_handle() -> Arc<PubSubRing>` | Direct access to the active inner backing. |
| `ring.chain_len() -> usize` | Number of backings currently in the chain. |
| `ring.chain_total_capacity() -> usize` | Sum of capacities across all chain backings. Used by KeepAll producers for back-pressure. |

`PubSubCapacityMorphError` has just two variants: `InvalidCapacity` (target not
a power of two, or `< 2`) and `Io(std::io::Error)` (backing allocation failed).
Unlike `BroadcastCapacityMorphError` there is no typed-broadcast variant,
because `PubSubRing` construction surfaces a plain `std::io::Result`. There is
no pin-handle type for this wrapper: callers that need pin-style invalidation
read `pin_generation()` directly and compare it against a previously captured
value (it is bumped on every successful morph).

A publish through `ring_handle()` bypasses the wrapper's announcement,
so a morph does not wait for it: publish through the wrapper while a
morph can run.

### Warm cache (predictive prebuild)

`prewarm(capacity)` builds the next backing in a one-slot cache off the
morph's critical path; the next `morph_capacity_to(capacity)` consumes it
(bumping `warm_hits`) and skips allocation, exactly as in
`CapacityBroadcastRing`. Re-prewarming the cached capacity is a no-op;
prewarming a different capacity replaces the slot; `clear_warm()` drops
it. A morph whose target does not match the cached capacity leaves the
cache intact and allocates cold.

### Subscriber

| Call | Behavior |
|---|---|
| `sub.try_next(out) -> Result<(), PubSubReadError>` | Read next item. On `Ok`, advances position by 1. On `Pending` with a successor linked, transparently advances to the next backing. On `Lost`, propagates. |
| `sub.backing_idx() -> u64` | The generation of the backing being read: 0 for the ring's first backing, one more for each morph after it. |
| `sub.position() -> u64` | Current position within the current backing. |

Through the C ABI, `subetha_capacity_subscriber_position` reports the
same generation and position.

## KeepAll back-pressure

`PubSubRing` is intrinsically KeepLastN: producers never block
on subscribers, and wrap-around at capacity silently overwrites
slots subscribers have not yet read. For workloads that need
KeepAll (zero loss), the wrapper exposes
`chain_total_capacity()` so the producer can implement its own
back-pressure: only publish when `(published_count -
min_subscriber_consumed) < chain_total_capacity`. As the morph
thread adds backings, the total grows; as `gc()` reclaims drained
backings, it shrinks. The producer gets exactly as much in-flight
room as the chain currently provides.

`examples/capacity_pubsub_morph_matrix.rs` uses this pattern at the
test level to verify no-loss correctness across the full
{subs x locale x size} matrix.

## Morph protocol (chain append, no in-place swap)

```text
1. Validate: new_capacity is pow2 >= 2.
2. If the active backing already has new_capacity: no-op return.
3. Warm-cache probe: if a prewarmed backing matches new_capacity,
   consume it and skip allocation; otherwise bump morph_seq and
   allocate a new PubSubRing at new_capacity at the same locale.
4. Compare-and-swap the active backing from the old one to the new
   one. If another morph swapped first, go back to 2 against the
   backing it put in place, keeping the backing built here.
5. pin_generation.fetch_add(1, AcqRel) -> a caller polling
   pin_generation() observes the bump and re-pins.
6. Seal the old backing and wait for the publishes announced on it
   to return.
7. Link the new backing behind the old one.
```

Concurrent morphs each land, one after another. Subscribers see the
new backing once they drain the old one past the link.

## Garbage collection

`gc()` moves the oldest kept backing forward while nothing but the
chain holds it: no subscriber holds that backing, and none reaches it
through an older one. It never passes the active backing. A
subscriber holds the backing it reads, so gc never takes a backing
from under a subscriber, and a subscriber's place does not move when
gc reclaims behind it.

## E2E proof

[`examples/capacity_pubsub_morph_matrix.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/capacity_pubsub_morph_matrix.rs)
ships the full {2,4,8} subs x {anon,file,shmfs} x {100k,1M items}
matrix with morphs every 200us. The producer implements KeepAll
back-pressure (waits when `head < cap`) so no subscriber loses
data. Verified: every subscriber observes the full 0..n_items
stream in strict send-order across hundreds of morph events.

## Constraints

- **Power-of-two capacity preserved.**
- **Any number of producers.** `PubSubRing` publishes claim their
  positions with an atomic add.
- **No lock on any path.** Publish announces itself on a counter,
  subscribers read through the backing they hold, and a morph swaps
  the active backing with a compare-and-swap.
- **Chain grows monotonically.** Without `gc()`, memory grows
  with morph count. Call `gc()` periodically from a background
  thread or as part of the morph thread's loop.

## When to reach for this primitive

- Fan-out where each subscriber must replay history from some
  starting point (vs broadcast which is KeepLastN).
- Workloads with runtime-elastic queueing depth on the fan-out
  side.

## When not to reach for this

- Fan-out where loss-on-overflow is acceptable - plain
  `SharedBroadcastRing` is simpler (no chain, no gc).
- Workloads with stable capacity - plain `PubSubRing` skips the
  chain and the publish announcement.

## References

- Source: `crates/subetha-cxc/src/capacity_pubsub_ring.rs` (5 unit
  tests: prewarm-hit-consumes-cache-and-subscribers-cross-chain,
  prewarm-mismatch-stays-cached, prewarm-rejects-non-pow2-and-clear-drops,
  gc-keeps-a-backing-a-subscriber-has-not-drained,
  a-subscriber-keeps-its-place-when-gc-reclaims-behind-it). Constructors
  return `Arc<Self>` (subscribers need the Arc to attach).
- [`PubSubRing`](../pubsub-ring/) - the underlying absolute-position
  primitive.
- [`CapacityAdaptiveRing`](../capacity-adaptive-ring/) - sibling
  capacity-morph wrapper for the fan-in family.
- [`CapacityBroadcastRing`](../capacity-broadcast-ring/) - sibling
  capacity-morph wrapper for the KeepLastN broadcast primitive.
- [Throughput results](../throughput-results/) - benchmark numbers
  for `capacity-pubsub`.
