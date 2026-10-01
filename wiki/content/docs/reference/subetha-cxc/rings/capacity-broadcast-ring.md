---
title: "Capacity Broadcast Ring"
weight: 24
---

# CapacityBroadcastRing + PinnedBroadcastCapacity

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Layout](https://img.shields.io/badge/Layout-SwapCell_RingState-green)
![Axis](https://img.shields.io/badge/axis-capacity--morph-brightgreen)

Runtime-resizable wrapper around
[`SharedBroadcastRing`](../shared-broadcast-ring/) that adds the
capacity-axis morph to the broadcast (1P / NC fan-out) primitive.

Sibling of [`CapacityAdaptiveRing`](../capacity-adaptive-ring/)
(which morphs the SPSC / MPSC / MPMC / Vyukov family) and
[`CapacityPubSubRing`](../capacity-pubsub-ring/) (which morphs the
absolute-position pub/sub primitive). `CapacityBroadcastRing`
morphs `SharedBroadcastRing`'s slot count at runtime under the same
stale-list invariant: producers only ever write to the active
backing; every subscriber walks the stale list oldest-first before
falling through to active.

## Per-subscriber position tracking, for free

Broadcast's distinguishing property is that every registered
consumer reads every slot independently. Each
`SharedBroadcastRing` already tracks per-consumer positions in its
mmap-resident header (`consumer_seqs[MAX_CONSUMERS]`), so the
per-stale-ring position tracker the capacity-morph wrapper needs is
provided by the underlying primitive at zero extra cost. Consumers
walk every stale backing at their own pace; a stale entry is
reclaimed by the next morph when
[`SharedBroadcastRing::is_fully_drained`](../shared-broadcast-ring/)
returns true (every active consumer's seq has caught up to the
frozen producer seq).

## Hot path: zero locks

`try_push` and `try_recv` both perform exactly one `SwapCell` load
on the wrapper's `state` field and delegate to the active backing.
The combined `(active, stale)` snapshot is atomic for free because
both fields live inside the single `BroadcastRingState` that the
`SwapCell` holds.

```text
try_push  -> state.load().active.try_push(payload)
try_recv  -> state = state.load()
             for stale_ring in &state.stale:
                 ...spin-on-mid-claim discipline...
             state.active.try_recv(consumer_idx, out)
```

## Constructors

| Call | Behavior |
|---|---|
| `CapacityBroadcastRing::create_anon(initial_capacity)` | In-process anon mmap. |
| `CapacityBroadcastRing::create(base_path, initial_capacity)` | File-backed; per-morph file at `{base_path}.cap_{N}_g{morph_seq}.bin`. |
| `CapacityBroadcastRing::create_shmfs(name_prefix, initial_capacity)` | Named-shm (RAM-resident, cross-process); per-morph name at `{prefix}_cap_{N}_g{morph_seq}`. |

`initial_capacity` must be a power of two >= 2.

## API

| Call | Behavior |
|---|---|
| `ring.current_capacity() -> usize` | Acquire-load the active capacity. |
| `ring.pin_generation() -> u64` | Acquire-load the current pin generation. |
| `ring.register_consumer() -> Result<usize, BroadcastError>` | Register a subscriber. Returns the next `consumer_idx` the wrapper hands out, claimed on the active backing. |
| `ring.try_push(payload) -> Result<(), BroadcastError>` | Hot-path producer push. Settles a backing a morph made active before its first push. |
| `ring.try_recv(consumer_idx, out) -> Result<usize, BroadcastError>` | Hot-path subscriber pop. Walks stale-first. |
| `ring.morph_capacity_to(new_capacity) -> Result<(), BroadcastCapacityMorphError>` | Allocate (or consume a warm-cached) backing and publish it with a compare-and-swap on the state. |
| `ring.prewarm(capacity) -> Result<(), BroadcastCapacityMorphError>` | Speculatively build a backing into the one-slot warm cache, off the morph's critical path. |
| `ring.warm_capacity() -> Option<usize>` | Capacity currently held in the warm cache, if any. |
| `ring.warm_hits() -> u64` | Count of morphs that consumed a warm-cache prediction. |
| `ring.clear_warm()` | Drop the cached prediction (releasing its memory / file / shm region). |
| `ring.pin_current_capacity() -> PinnedBroadcastCapacity<'_>` | Pin the current backing, settled, for a hot loop. |
| `ring.ring_handle() -> Arc<SharedBroadcastRing>` | Direct access to the active inner backing, settled. |

`PinnedBroadcastCapacity` exposes `ring() -> &Arc<SharedBroadcastRing>`,
`capacity() -> usize`, `generation() -> u64`, and `is_still_valid() -> bool`
(false once a morph has bumped the pin generation).
`BroadcastCapacityMorphError` has three variants: `InvalidCapacity` (target
not a power of two, or `< 2`), `Broadcast(BroadcastError)` (the new backing's
allocation failed; the active backing is left unchanged), and
`Io(std::io::Error)` (file / shm backing creation failed). It implements
`Display` + `std::error::Error`.

## Consumer registration model

The wrapper hands out consumer indices itself, in order, from a
count (`n_consumers`), and consumers never unregister - this
matches the capacity-morph use case (subscribers join, capacity
grows / shrinks under load, no subscriber churn). A registrant
claims its index on the backing active when it registers, reading
from that backing's producer position on.

A backing a morph makes active is **settled** before anything is
pushed into it, read from it, or handed out through
`pin_current_capacity` or `ring_handle`: every index handed out so
far is claimed there, at its start. A registrant the settle's count
missed registered after that backing became active, so that backing
is where it starts. Each subscriber therefore reads one unbroken run
of what the producer pushed, and an index is never held by two
subscribers. A subscriber has no slot on a backing that was already
stale when it registered, and the stale walk passes over it.

## Morph protocol (stale-list, atomic state swap)

```text
1. Validate: new_capacity is pow2 >= 2.
2. Load old_state; if its capacity is new_capacity: no-op return.
3. Warm-cache probe: if a prewarmed backing matches new_capacity,
   consume it and skip allocation entirely; a mismatch stays cached
   and the cold path runs unchanged.
4. Otherwise bump morph_seq atomically (unique file/shm name per
   morph) and allocate a fresh SharedBroadcastRing at new_capacity
   at the same locale the wrapper was constructed for.
5. Build new_state = BroadcastRingState {
       active: new,
       stale: prune(old_state.stale) ++ [old.active],
       capacity: new_capacity,
       settled: false,
   }
   The first push, read or pin on it claims every consumer index
   handed out so far on `new` before it goes on.
   Prune drops a prior-stale entry where every subscriber has caught
   up (is_fully_drained() == true) and that the stale list alone
   holds (Arc strong count 1): the producer pushes into the active
   backing of the state it loaded, so an entry a loaded snapshot still
   names stays on the list for the push that snapshot may yet deliver.
6. Compare-and-swap state from old_state to new_state -> single
   atomic publish. If another morph swapped first, the backing built
   here drops and the morph goes back to 2 against that state.
7. Bump pin_generation.
```

Nothing takes a lock: concurrent morphs each land, one after another.

The atomic publish gives subscribers a consistent
`(active, stale)` snapshot across the morph: a subscriber that
loads state mid-morph sees either the pre-morph snapshot or the
post-morph snapshot, never a half-state where active and stale
disagree.

## FIFO correctness: spin-on-stale-mid-claim

`try_recv` distinguishes "stale ring is truly drained for me" from
"stale ring has uncommitted slot claims" via the broadcast-specific
`lag(consumer_idx)` accessor. When a stale ring returns
`Err(Empty)`:

- If `ring.lag(consumer_idx) == 0`: this subscriber has caught up;
  safe to advance to the next stale entry or fall through to
  active.
- Otherwise: a producer is mid-write under the SeqLock (odd version
  observable as Empty until the version commits). Spin and retry
  on the same stale ring.

Same architectural pattern as
[`CapacityAdaptiveRing::try_recv`](../capacity-adaptive-ring/#fifo-correctness-spin-on-stale-mid-claim) -
the broadcast-ring-specific predicate is `lag(idx) == 0` instead of
`is_empty()`.

## Warm cache (predictive prebuild)

Allocating a fresh backing (file create + ftruncate + first-page-fault, or a
named-shm region) is the expensive part of a morph. When the next target
capacity is predictable, `prewarm(capacity)` builds that backing in a one-slot
cache off the morph lock's critical path; the next `morph_capacity_to(capacity)`
consumes it and skips allocation entirely. This is the same warm-cache design
`CapacityAdaptiveRing` uses.

```rust,no_run
ring.prewarm(4096)?;                 // build off the hot path
assert_eq!(ring.warm_capacity(), Some(4096));
ring.morph_capacity_to(4096)?;       // consumes the prediction
assert_eq!(ring.warm_hits(), 1);     // hit recorded
assert_eq!(ring.warm_capacity(), None);  // one-shot slot now empty
```

Re-prewarming the cached capacity is a no-op; prewarming a different capacity
replaces the slot. `warm_capacity()` reports what is cached, `warm_hits()`
counts consumed predictions, and `clear_warm()` drops the cached backing
(releasing its memory, and its file / shm region for non-anon locales). A morph
whose target does not match the cached capacity leaves the cache intact and
allocates cold.

## Pinned handoff

```rust,no_run
let pin_cap = ring.pin_current_capacity();
let inner: &Arc<SharedBroadcastRing> = pin_cap.ring();

let payload = [0u8; 52];
for _ in 0..1_000_000 {
    inner.try_push(&payload).unwrap();
    if !pin_cap.is_still_valid() {
        break;  // morph happened; release and re-acquire outside
    }
}
```

See [throughput results](../throughput-results/)
`capacity-pinned-broadcast` rows for the pinned-path numbers.

## Worked example

```rust,no_run
use std::sync::Arc;
use std::thread;
use subetha_cxc::CapacityBroadcastRing;

let ring = Arc::new(
    CapacityBroadcastRing::create_anon(256).unwrap()
);
let sub_ids: Vec<usize> = (0..4)
    .map(|_| ring.register_consumer().unwrap())
    .collect();

let r_prod = Arc::clone(&ring);
let producer = thread::spawn(move || {
    for i in 0..100_000u64 {
        let mut payload = [0u8; 52];
        payload[..8].copy_from_slice(&i.to_le_bytes());
        while r_prod.try_push(&payload).is_err() {
            std::hint::spin_loop();
        }
    }
});

let mut sub_handles = Vec::new();
for sid in sub_ids {
    let r = Arc::clone(&ring);
    sub_handles.push(thread::spawn(move || {
        let mut buf = [0u8; 64];
        for _ in 0..100_000u64 {
            while r.try_recv(sid, &mut buf).is_err() {
                std::hint::spin_loop();
            }
        }
    }));
}

ring.morph_capacity_to(4096).unwrap();   // safe under live traffic

producer.join().unwrap();
for h in sub_handles { h.join().unwrap(); }
```

## E2E proof

[`examples/capacity_broadcast_morph_matrix.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/capacity_broadcast_morph_matrix.rs)
ships the full {2,4,8} subs x {anon,file,shmfs} x {100k,1M items}
matrix with morphs every 200us. Verified: every subscriber receives
every item exactly once in strict send-order across hundreds of
morph events. Both Windows and Linux pass clean.

## Constraints

- **Power-of-two capacity preserved.**
- **Grow and shrink both succeed unconditionally.** In-flight items
  physically stay in the old backing as part of the stale list;
  subscribers drain them at their own pace.
- **Subscribers register at the wrapper, not the backing.** The
  wrapper hands out the index and each backing claims it, at
  registration or at the backing's settle. Each subscriber's
  `consumer_idx` is stable across morphs.
- **`MAX_CONSUMERS` (currently 16) is the limit.** Registering more
  subscribers than this returns `NoConsumerSlot`.

## References

- [`SharedBroadcastRing`](../shared-broadcast-ring/) - the
  underlying 1P/NC broadcast primitive.
- [`CapacityAdaptiveRing`](../capacity-adaptive-ring/) - sibling
  capacity-morph wrapper for the fan-in family.
- [`CapacityPubSubRing`](../capacity-pubsub-ring/) - sibling
  capacity-morph wrapper for the absolute-position pub/sub
  primitive.
- [Throughput results](../throughput-results/) - benchmark numbers
  for `capacity-broadcast` (unpinned) and `capacity-pinned-broadcast`
  (production hot path).
