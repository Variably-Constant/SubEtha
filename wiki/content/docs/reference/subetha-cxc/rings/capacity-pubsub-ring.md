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
to the pub/sub (1P / NC absolute-position) primitive.

Sibling of [`CapacityAdaptiveRing`](../capacity-adaptive-ring/) and
[`CapacityBroadcastRing`](../capacity-broadcast-ring/). Where the
broadcast wrapper relies on the underlying ring's per-consumer
header state to track subscribers across morphs,
`CapacityPubSubRing` uses a chain-of-backings model because
`PubSubRing`'s per-subscriber position state lives OUTSIDE the ring
(in `SubscriberPosition` / on the `PubSubSubscriber` wrapper).

## Chain-of-backings invariant

The wrapper maintains a `Vec<Arc<PubSubRing>>` ordered oldest-first
behind a `Mutex` (the chain mutex). The active backing is always at
the end. A `CapacityPubSubSubscriber` holds:

- `cap_ring: Arc<CapacityPubSubRing>` (so it can see the chain)
- `backing_idx: u64` (which entry in the chain we are reading from)
- `position: u64` (position within `backings[backing_idx]`)

On `try_next`, the subscriber reads at its current
`(backing_idx, position)`. On `Pending` AND when not on the
most-recent backing, it transparently advances `backing_idx` and
resets `position` to 0 (every stale backing's prior content drains
before the subscriber crosses into the next).

## Publisher path holds the chain lock

`publish` holds `chain.lock()` through the inner publish call. The
lock is uncontended in steady state (subscribers do not take the
chain lock per call - they snapshot the Arc once per try_next), but
holding it through publish is what gives the correctness
invariant: a concurrent morph cannot slip in and make the publish
land in a backing that just became stale. Subscribers walk the
chain oldest-to-newest and only advance forward; if a publish
landed in a now-stale backing past where any subscriber had already
advanced, those items would be silently lost. Holding the lock
through publish prevents that.

Subscriber path is lock-free: `try_next` does one chain-lock to
clone the current backing Arc + read the is_latest flag, drops the
lock, then reads via the cloned Arc.

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
| `ring.current_capacity() -> usize` | Acquire-load the active capacity. |
| `ring.pin_generation() -> u64` | Acquire-load the current pin generation. |
| `ring.publish(payload) -> u64` | Holds chain lock through inner publish. Returns position within the active backing. |
| `ring.subscribe_from_now() -> CapacityPubSubSubscriber` | Subscribe starting from the active backing's CURRENT head. Late joiners see no history. |
| `ring.subscribe_from_oldest() -> CapacityPubSubSubscriber` | Subscribe starting from position 0 of the oldest chain entry. Subscriber drains every still-resident item. |
| `ring.morph_capacity_to(new_capacity) -> Result<(), PubSubCapacityMorphError>` | Append a fresh (or warm-cached) backing to the chain at the new capacity, atomic-bump pin_generation. |
| `ring.prewarm(capacity) -> Result<(), PubSubCapacityMorphError>` | Speculatively build a backing into the one-slot warm cache, off the morph lock. |
| `ring.warm_capacity() -> Option<usize>` | Capacity currently held in the warm cache, if any. |
| `ring.warm_hits() -> u64` | Count of morphs that consumed a warm-cache prediction. |
| `ring.clear_warm()` | Drop the cached prediction (releasing its memory / file / shm region). |
| `ring.gc() -> usize` | Drop the oldest contiguous run of chain entries whose `strong_count == 1` (no subscriber holds an Arc). Returns reclaimed-count. |
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

### Warm cache (predictive prebuild)

`prewarm(capacity)` builds the next backing in a one-slot cache OFF the morph
lock; the next `morph_capacity_to(capacity)` consumes it (bumping `warm_hits`)
and skips allocation, exactly as in `CapacityBroadcastRing`. Re-prewarming the
cached capacity is a no-op; prewarming a different capacity replaces the slot;
`clear_warm()` drops it. A morph whose target does not match the cached
capacity leaves the cache intact and allocates cold.

### Subscriber

| Call | Behavior |
|---|---|
| `sub.try_next(out) -> Result<(), PubSubReadError>` | Read next item. On `Ok`, advances position by 1. On `Pending` AND not on the latest chain entry, transparently advances to the next backing. On `Lost`, propagates. |
| `sub.backing_idx() -> u64` | Current chain index being read. |
| `sub.position() -> u64` | Current position within the current backing. |

## KeepAll back-pressure

`PubSubRing` is intrinsically KeepLastN: the producer never blocks
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
1. Take morph_lock.
2. Validate: new_capacity is pow2 >= 2.
3. If old_capacity == new_capacity: no-op return.
4. Warm-cache probe: if a prewarmed backing matches new_capacity,
   consume it (bump warm_hits) and skip allocation; otherwise
   bump morph_seq and allocate a new PubSubRing at new_capacity at
   the same locale.
5. chain.lock(); chain.push(new_ring); drop(chain);
6. pin_generation.fetch_add(1, AcqRel) -> a caller polling
   pin_generation() observes the bump and re-pins.
7. capacity_atom.store(new_capacity, Release).
8. Release morph_lock.
```

Subscribers reading via `try_next` see the new entry on their next
call (chain lock is taken to snapshot the active backing). Already
in-flight try_next calls hold an Arc to their CURRENT backing and
complete on it; the chain grew under them but their cloned Arc is
still valid.

## Garbage collection

`gc()` reclaims fully-drained chain entries from the FRONT of the
chain. It serialises with `morph_lock` (the same lock the morph
takes) so chain mutations are atomic with respect to each other.

The check is `Arc::strong_count(&chain[0]) == 1` - meaning only the
chain itself holds the Arc; no subscriber is currently reading from
it. Active backing is never dropped even when its strong_count is
1 (that would lose the producer's target).

For correctness: subscribers walking the chain hold a transient
Arc on their current entry (cloned inside the chain-lock window).
That clone keeps the entry alive while the subscriber is in
try_next. Between try_next calls the Arc is dropped, so gc can
reclaim entries the subscriber has fully advanced past.

## E2E proof

[`examples/capacity_pubsub_morph_matrix.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/capacity_pubsub_morph_matrix.rs)
ships the full {2,4,8} subs x {anon,file,shmfs} x {100k,1M items}
matrix with morphs every 200us. The producer implements KeepAll
back-pressure (waits when `head < cap`) so no subscriber loses
data. Verified: every subscriber observes the full 0..n_items
stream in strict send-order across hundreds of morph events.

## Constraints

- **Power-of-two capacity preserved.**
- **Single producer.** PubSub itself is 1P/NC.
- **`publish` holds chain.lock through the inner publish.** This
  serialises against concurrent morphs; the lock is uncontended in
  steady state. The cost is one parking_lot mutex acquire per
  publish (~5-10 ns).
- **Subscriber path is lock-free.** `try_next` takes the chain
  lock only briefly to snapshot the active backing Arc; reads
  happen on the cloned Arc outside the lock.
- **Chain grows monotonically.** Without `gc()`, memory grows
  with morph count. Call `gc()` periodically from a background
  thread or as part of the morph thread's loop.

## When to reach for this primitive

- 1P/NC fan-out where each subscriber must replay history from
  some starting point (vs broadcast which is KeepLastN).
- Workloads with runtime-elastic queueing depth on the fan-out
  side.

## When NOT to reach for this

- 1P/NC fan-out where loss-on-overflow is acceptable - plain
  `SharedBroadcastRing` is simpler (no chain, no gc).
- Workloads with stable capacity - plain `PubSubRing` skips the
  chain mutex on publish.
- Multi-producer fan-out - feed producers into a `SharedRingMpsc`
  first and relay into a `CapacityPubSubRing` from the single
  consumer.

## References

- Source: `crates/subetha-cxc/src/capacity_pubsub_ring.rs` (499
  lines, 3 unit tests: prewarm-hit-consumes-cache-and-subscribers-
  cross-chain, prewarm-mismatch-stays-cached, plus the morph/gc
  chain coverage). Constructors return `Arc<Self>` (subscribers
  need the Arc to attach).
- [`PubSubRing`](../pubsub-ring/) - the underlying 1P/NC
  absolute-position primitive.
- [`CapacityAdaptiveRing`](../capacity-adaptive-ring/) - sibling
  capacity-morph wrapper for the fan-in family.
- [`CapacityBroadcastRing`](../capacity-broadcast-ring/) - sibling
  capacity-morph wrapper for the KeepLastN broadcast primitive.
- [Throughput results](../throughput-results/) - benchmark numbers
  for `capacity-pubsub`.
