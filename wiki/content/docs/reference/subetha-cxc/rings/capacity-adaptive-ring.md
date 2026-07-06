---
title: "Capacity Adaptive Ring"
weight: 23
---

# CapacityAdaptiveRing + PinnedCapacity

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Layout](https://img.shields.io/badge/Layout-ArcSwap_RingState-green)
![Axis](https://img.shields.io/badge/axis-capacity--morph-brightgreen)

Runtime-resizable wrapper around
[`AdaptiveRing`](../shared-ring-adaptive/) that adds capacity-axis
morphing to the polymorphic substrate. Where AdaptiveRing morphs
*shape* (SPSC / MPSC / MPMC / Vyukov) at a fixed capacity and
[`LocaleAdaptiveRing`](../locale-adaptive-ring/) morphs *locale*
(Anon / File / ShmFs) at a fixed capacity, `CapacityAdaptiveRing`
morphs the *capacity* itself: callers (or the bundled
[`CapacityAdaptiveRingSidecar`](#sidecar--hysteresis-gated-policy))
call `morph_capacity_to(new_pow2)`, the substrate allocates a fresh
underlying `AdaptiveRing` at the new size, atomically swaps a single
`ArcSwap<RingState>` that holds both the new active backing AND the
stale-list of post-morph backings still draining, and bumps a pin
generation so outstanding handles invalidate.

## The fourth axis

| Axis | Primitive | What morphs | Method |
|---|---|---|---|
| Shape | `AdaptiveRing` | SPSC / MPSC / MPMC / Vyukov backings | `morph_to(RingShape)` |
| Locale | `LocaleAdaptiveRing` | Anon / File / ShmFs storage | `migrate_to(Locale)` |
| **Capacity** | `CapacityAdaptiveRing` | **Slot count (any pow2 >= 2)** | `morph_capacity_to(usize)` |

## Hot path: zero locks

Both `try_send` and `try_recv` do exactly one Acquire load on an
`ArcSwap<Arc<RingState>>` and then delegate to the underlying
`AdaptiveRing`. There is no mutex on the steady-state path. The
combined `(active, stale)` snapshot is atomic for free because both
fields live inside the single `RingState` that the ArcSwap protects.

```text
try_send  -> state.load().active.try_send(producer_id, payload)
try_recv  -> state = state.load()
             for stale_ring in &state.stale:        # always-empty in steady state
                 ...spin-on-mid-claim discipline...
             state.active.try_recv(consumer_id, out)
```

For absolute production speed when the shape is stable AND the
capacity is stable, call
[`pin_current_capacity()`](#pinned-handoff--native-primitive-speed)
once and hot-loop on the inner native primitive via PinnedRing's
shape-specific `*_try_push` / `*_try_pop` (see the
[throughput results](../throughput-results/) for the actual numbers
the pinned path hits).

## Constructors

| Call | Behavior |
|---|---|
| `CapacityAdaptiveRing::create_anon(max_p, max_c, initial_capacity)` | In-process anon mmap. Morphs allocate fresh anon mmaps; the prior backing stays alive in the stale list until consumers drain it. |
| `CapacityAdaptiveRing::create(base_path, max_p, max_c, initial_capacity)` | File-backed. Per-morph file at `{base_path}.cap_{N}_g{morph_seq}.bin` so cycling capacities (e.g. 256 -> 1024 -> 256) never collide on names. |
| `CapacityAdaptiveRing::create_shmfs(name_prefix, max_p, max_c, initial_capacity)` | Named-shm (RAM-resident, cross-process). Per-morph name at `{prefix}_cap_{N}_g{morph_seq}`. |

`initial_capacity` must be a power of two >= 2.

## API

| Call | Behavior |
|---|---|
| `ring.current_capacity() -> usize` | Acquire-load the active capacity (lockstep with the active backing). |
| `ring.pin_generation() -> u64` | Acquire-load the current pin generation. |
| `ring.register_producer() / register_consumer() -> Result<usize, AdaptiveError>` | Mirror the active backing's registration. |
| `ring.try_send(producer_id, payload) -> Result<(), RingError>` | Hot-path push. One ArcSwap load + native dispatch. |
| `ring.try_recv(consumer_id, out) -> Result<usize, RingError>` | Hot-path pop. One ArcSwap load + stale-walk + native dispatch. |
| `ring.morph_capacity_to(new_capacity) -> Result<(), CapacityMorphError>` | Capacity-only morph. Delegates to `morph_to_config` with `capacity: Some(..)`. |
| `ring.morph_to_config(&RingConfig) -> Result<(), CapacityMorphError>` | Compound morph: change any subset of {shape, capacity, locale} in ONE transition (see below). |
| `ring.prewarm(capacity) -> Result<(), CapacityMorphError>` | Speculatively build the next backing off the morph lock (current locale). |
| `ring.prewarm_config(&RingConfig) -> Result<(), CapacityMorphError>` | Prewarm at a target (capacity, locale); shape is ignored in the warm key. |
| `ring.warm_capacity() -> Option<usize>` / `ring.warm_hits() -> u64` / `ring.clear_warm()` | Warm-cache introspection + drop (see below). |
| `ring.stale_pops() -> u64` | Items popped from stale (post-morph) backings rather than the active one - transition-cost observability. |
| `ring.is_stamped() -> bool` / `ring.ordering_mode() -> Option<OrderingMode>` / `ring.set_ordering_mode(mode) -> Result<(), RingError>` / `ring.inversions() -> u64` | Ordering-stamps surface (see below). |
| `ring.pin_current_capacity() -> PinnedCapacity<'_>` | Capture the current backing for a hot loop (pin generation captured for validity polling). |
| `ring.ring_handle() -> Arc<AdaptiveRing>` | Direct access to the active inner backing (snapshot at call time). |

`PinnedCapacity` exposes `ring() -> &Arc<AdaptiveRing>`, `capacity() -> usize`,
`generation() -> u64`, and `is_still_valid() -> bool`. `CapacityMorphError` has
four variants: `InvalidCapacity` (not pow2, or `< 2`), `Ring(RingError)` and
`Adaptive(AdaptiveError)` (allocation / registration failures during the morph;
the active backing is left unchanged), and `CannotShrinkInFlight { in_flight,
new_capacity }` - which is **reserved for API stability and never returned** by
the current implementation, because shrinks always succeed (in-flight items
stay in the old backing on the stale list).

## Morph protocol (stale-list, no drain)

The morph never reads from any backing. Producers only ever write to
`active`. Consumers are the sole reader of every backing (stale and
active alike). This is what rules out the two-consumer-on-one-SPSC
race that any drain-based morph has to defend against.

```text
1. Take morph_lock (serialises concurrent morphs).
2. Validate: new_capacity is pow2 >= 2.
3. If old_capacity == new_capacity (and shape + locale unchanged): no-op return.
4. Warm-cache probe: if a prewarmed backing matches the target
   (capacity, locale), consume it (bump warm_hits) and skip
   allocation; otherwise bump morph_seq and allocate a fresh
   AdaptiveRing at new_capacity in the target locale (anon / file / shmfs).
5. When the wrapper is stamped, seed the new region's ordering
   counters from the old one (keeps stamps monotone across the swap).
6. Mirror producer/consumer counts onto the new backing.
7. Mirror the SHAPE explicitly via new.morph_to(old.current_shape()).
8. Bump pin_generation -> all outstanding PinnedCapacity handles
   invalidate on their next is_still_valid() poll.
9. Build new_state = RingState {
       active: new,
       stale: prune(old_state.stale) ++ [old.active],
   }
   The prune drops any prior-stale entries that are fully empty
   (no in-flight items left); this is the shape-aware
   AdaptiveRing::is_empty() check.
10. state.store(new_state) -> single atomic publish.
11. capacity_atom.store(new_capacity, Release) -> observable
    capacity tracks active.
12. Release morph_lock.
```

Subscribers reading the wrapper via `state.load()` between step 1
and step 10 see the OLD state (full snapshot of pre-morph
`(active=old, stale=prior_stale)`). Subscribers reading AFTER step
10 see the new state. The single atomic publish is what gives FIFO
correctness across the morph for free.

## FIFO correctness: spin-on-stale-mid-claim

The stale-walk in `try_recv` distinguishes "ring truly empty for me"
from "ring has uncommitted slot claims". When a stale ring returns
`Err(Empty)`:

- If `ring.is_empty()` (producer_seq == consumer_seq for Vyukov, all
  sub-rings empty for composed shapes): safe to advance to the next
  stale entry or fall through to active.
- Otherwise: another consumer is mid-CAS on this ring's slot, OR a
  producer is mid-publish on a slot that was claimed before the
  state swap. Spin and retry on the same stale ring until either we
  read an item or the ring is truly drained.

Without this discipline, consumer X reading active first while
another consumer is mid-claim on stale would consume a
higher-producer-index item from active before the lower-index item
from stale becomes available - per-consumer per-producer FIFO would
break under Vyukov / MPMC with multiple consumers. This was caught
empirically at 1M-item / 8P-8C Vyukov runs (0.0044% reordering
rate); fixed and re-verified clean across the full capacity-morph
matrix.

## Compound morph (shape + capacity + locale in one transition)

`morph_capacity_to` is the capacity-only convenience; the underlying primitive
is `morph_to_config(&RingConfig)`, which changes any subset of
`{shape, capacity, locale}` in ONE transition. Each field is optional (`None`
keeps the current value):

```rust,no_run
use subetha_cxc::{RingConfig, BackingTarget, RingShape};

ring.morph_to_config(&RingConfig {
    shape: Some(RingShape::Mpmc),       // change shape ...
    capacity: Some(4096),               // ... and capacity ...
    locale: Some(BackingTarget::Anon),  // ... and locale, all at once
})?;
```

One compound morph builds ONE fresh backing at the combined target, mirrors
registrations once, applies the target shape to the empty backing once, bumps
the pin generation once, and appends the displaced active to the stale list
once - however many axes changed. A sequential walk of the same axes pays each
of those costs per axis. Two special cases short-circuit: every axis already at
target is a no-op (no generation bump), and a shape-ONLY change (capacity +
locale unchanged) delegates to the active backing's in-place `morph_to` (no
fresh backing, the wrapper pin stays valid, in-flight items stay put).
`BackingTarget` is `Anon` / `File(PathBuf)` / `Shm(String)`; setting the locale
axis retargets the wrapper for this morph AND every subsequent morph / prewarm.

## Warm cache (predictive prebuild)

Allocating a fresh backing (file create + ftruncate + first-page-fault, or a
named-shm region + zeroing) is the expensive part of a morph. `prewarm(capacity)`
(or `prewarm_config(&RingConfig)` for a capacity+locale target) builds that
backing in a one-slot cache OFF the morph lock; the next morph at the matching
(capacity, locale) consumes it (bumping `warm_hits`) and skips allocation.
Shape is deliberately NOT part of the warm key - a fresh backing starts SPSC
and the swap path shapes the empty backing in microseconds. `warm_capacity()`
reports what is cached, `warm_hits()` counts consumed predictions, and
`clear_warm()` drops the cached backing. The bundled sidecar wires this
automatically via the policy's `predict` (see below).

## Ordering stamps (global-FIFO axis across morphs)

Each constructor has a `*_stamped` twin (`create_anon_stamped`,
`create_stamped`, `create_shmfs_stamped`) that turns on the
[ordering-stamp](../adaptive-ordering/) axis on the backing AND on every backing
subsequent morphs allocate. A capacity morph SEEDS the fresh region's stamp
counters from the old region at swap time, so stamps stay monotone across the
resize. The surface: `is_stamped()` reports whether stamps are on,
`ordering_mode() -> Option<OrderingMode>` reads the live mode,
`set_ordering_mode(mode)` flips it across the active backing AND every stale
backing still draining (so the consumer's stale-walk-then-active pop applies one
consistent discipline), and `inversions()` reports cross-producer inversions
observed on the active backing (continuous across morphs via the counter seed).

## Constraints

- **Power-of-two capacity preserved.** New capacity must be a power
  of two and at least 2. Slot-index calculation stays
  `hash & (capacity - 1)` = one AND instruction.
- **Grow AND shrink both succeed unconditionally.** In-flight items
  physically stay in the old (larger or smaller) backing as part of
  the stale list; the new capacity governs only items the producer
  pushes after the morph. A shrink therefore never fails on in-flight
  items, so the `CannotShrinkInFlight` error variant is never returned.
- **Pin invalidation is caller-polled.** Outstanding
  `PinnedCapacity` handles observe the generation bump on the next
  `is_still_valid()` call. Hot loops sample at whatever cadence
  fits their latency budget; the substrate does not push.
- **Morph is serialised.** A single in-flight morph at a time;
  concurrent callers of `morph_capacity_to` are mutex-serialised
  so the state build + swap is atomic with respect to other morphs.
  Producer / consumer hot-path ops are NOT serialised against the
  morph - they keep dispatching via the ArcSwap.
- **Consumer is sole reader of every backing.** Producers only
  write to active; morphs never read from any backing. The
  per-backing SPSC/MPSC/MPMC contract stays intact across morphs
  because exactly one reader-role touches each `SpscRingCore`.

## Pinned handoff: native primitive speed

`PinnedCapacity::ring()` returns `&Arc<AdaptiveRing>` - the inner
backing - so callers chain into AdaptiveRing's shape-axis pin:

```rust,no_run
let pin_cap = ring.pin_current_capacity();
let inner: &Arc<AdaptiveRing> = pin_cap.ring();
let pin_shape = inner.pin_current_shape();

let payload = [0u8; 56];
for _ in 0..1_000_000 {
    pin_shape.spsc_try_push(&payload).unwrap();   // native primitive speed
    if !pin_shape.is_still_valid() || !pin_cap.is_still_valid() {
        break;  // morph happened; release and re-acquire outside
    }
}
```

The pinned hot path approaches native primitive speed (see
[throughput results](../throughput-results/) - `capacity-pinned-*`
rows). The unpinned path pays one ArcSwap load + empty-stale-iter +
AdaptiveRing dispatch per call (still lock-free; just a few extra
nanoseconds).

## Sidecar + hysteresis-gated policy

The wrapper ships with a background scanner thread that observes
the ring's fill ratio and grows / shrinks the capacity automatically.

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::{
    CapacityAdaptiveRing, CapacityAdaptiveRingSidecar, DefaultCapacityPolicy,
};

let ring = Arc::new(
    CapacityAdaptiveRing::create_anon(1, 1, 256).unwrap()
);
ring.register_producer().unwrap();
ring.register_consumer().unwrap();

let policy = DefaultCapacityPolicy {
    grow_at: 0.85,                              // fill >= 85% -> double cap
    shrink_at: 0.10,                            // fill <= 10% -> halve cap
    min_capacity: 64,
    max_capacity: 65536,
    hysteresis: Duration::from_millis(100),     // cooldown between morphs
};
let sidecar = CapacityAdaptiveRingSidecar::spawn(
    Arc::clone(&ring),
    policy,
    Duration::from_millis(10),                  // scan every 10 ms
);

// ... application runs, producer/consumer drive load ...

println!("sidecar morphs: {}", sidecar.morphs_triggered());
sidecar.shutdown();
```

`DefaultCapacityPolicy` reads
[`AdaptiveRing::approx_len()`](../shared-ring-adaptive/) +
[`AdaptiveRing::total_slot_capacity()`](../shared-ring-adaptive/),
computes `fill_ratio`, and returns `Some(new_cap)` when the ratio
crosses the threshold AND `since_last_morph >= hysteresis`. The
hysteresis cooldown prevents thrashing under bursty load: rapid
fill / drain oscillations collapse into one morph per cooldown
window, not one morph per burst.

The trait API for custom policies:

```rust,no_run
use subetha_cxc::{CapacityPolicy, CapacityPolicyObservation};

struct MyPolicy;

impl CapacityPolicy for MyPolicy {
    fn decide(&self, obs: &CapacityPolicyObservation) -> Option<usize> {
        // your decision logic here
        // obs.fill_ratio(), obs.current_capacity,
        // obs.active_approx_len, obs.total_slot_capacity,
        // obs.since_last_morph
        None
    }

    // Optional: name the capacity decide() will likely request soon.
    // When predict names the same target on two consecutive scans,
    // the sidecar prewarms it off the morph lock. Default: None.
    fn predict(&self, _obs: &CapacityPolicyObservation) -> Option<usize> { None }
}
```

`DefaultCapacityPolicy` overrides `predict` too: it names the doubled capacity
once the fill ratio crosses 75% of `grow_at`, and the halved capacity once it
falls under 150% of `shrink_at` - trend bands sitting in front of the decide
thresholds, deliberately NOT gated on hysteresis (the post-morph cooldown is
exactly when to build the next predicted backing). `prewarms_issued()` on the
sidecar counts how many speculative backings those trends pre-built.

For oscillating load there is `CapacityAdaptiveRingSidecar::spawn_gated(ring,
policy, scan_interval, GateConfig)`: a confidence gate sits between the policy's
recommendation and the morph, so a recommendation must hold across consecutive
scans until conviction crosses the gate threshold. Recommendation reversals,
peer-count changes, and fill-ratio jumps (> 0.5) collapse conviction, so a
flapping workload starves the gate instead of thrashing the ring. The default
`spawn` is `spawn_gated` with the gate disabled (identical behavior).

## Worked example (manual morph)

```rust,no_run
use std::sync::Arc;
use std::thread;
use subetha_cxc::CapacityAdaptiveRing;

let ring = Arc::new(
    CapacityAdaptiveRing::create_anon(1, 1, 256).unwrap()
);
ring.register_producer().unwrap();
ring.register_consumer().unwrap();

let r_prod = Arc::clone(&ring);
let producer = thread::spawn(move || {
    for i in 0..100_000u64 {
        let mut payload = [0u8; 56];
        payload[..8].copy_from_slice(&i.to_le_bytes());
        while r_prod.try_send(0, &payload).is_err() {
            std::hint::spin_loop();
        }
    }
});

// Consumer reads in another thread; morpher in a third grows /
// shrinks the ring under load (see examples/capacity_morph_e2e.rs
// for the full pattern).

ring.morph_capacity_to(4096).unwrap();
assert_eq!(ring.current_capacity(), 4096);

producer.join().unwrap();
```

## E2E proof

[`examples/capacity_morph_e2e.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/capacity_morph_e2e.rs)
ships 100,000 items through the ring at 200us morph cadence,
cycling capacities through `[1024, 4096, 1024, 256, 64, 512]`.
Verified: every ID 0..N appears in the consumed set exactly once;
strict 1P/1C global FIFO holds across morphs; per-consumer
per-producer FIFO holds across the full
{shape x locale x capacity} matrix verified by
`examples/capacity_morph_matrix.rs`.

[`examples/cap_morph_sidecar_e2e.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/cap_morph_sidecar_e2e.rs)
runs a 6-cycle burst-then-drain workload with the sidecar attached
and prints every grow / shrink the policy fires. Typical run on
Windows: ~36 sidecar-triggered morphs over ~2 seconds, 48000 items
delivered cleanly.

## When to reach for this primitive

- Workloads where queueing depth has a wide dynamic range (idle
  workload at 64 slots, burst workload at 16K slots) and you do
  not want to pre-allocate for the peak.
- Sidecar-driven elastic capacity policies (the default policy
  covers fill-ratio thresholds + hysteresis; custom policies plug
  into the same trait).
- Applications that change their declared workload shape at
  runtime (services that gain or shed consumers, batch jobs that
  switch phases).

## When NOT to reach for this

- Workloads where the queueing depth is known up-front and never
  changes. Plain `AdaptiveRing` at the right capacity is cheaper
  (one less ArcSwap load on the hot path).
- Workloads that need the lowest possible per-op latency AND
  cannot grab a pin. The unpinned dispatch adds a few ns vs a
  direct `AdaptiveRing` call. The pinned path matches native
  primitive speed (see [throughput results](../throughput-results/)).

## References

- Source: `crates/subetha-cxc/src/capacity_adaptive_ring.rs` (2019
  lines, 28 unit tests across morph/grow/shrink, compound morph,
  warm cache, ordering-stamp continuity, the sidecar + gate, and
  the FIFO stale-walk). `RingConfig` / `BackingTarget` /
  `CapacityMorphError` / `PinnedCapacity` / `CapacityPolicy` /
  `DefaultCapacityPolicy` / `CapacityAdaptiveRingSidecar` are all
  re-exported at the crate root.
- [`AdaptiveRing`](../shared-ring-adaptive/) - the shape-axis
  layer underneath each capacity backing.
- [`LocaleAdaptiveRing`](../locale-adaptive-ring/) - the
  locale-axis sibling with its own sidecar
  (`LocaleAdaptiveRingSidecar` + `DefaultLocalePolicy`).
- [`CapacityBroadcastRing`](../capacity-broadcast-ring/) /
  [`CapacityPubSubRing`](../capacity-pubsub-ring/) - capacity-axis
  morphing for the fan-out family.
- [Throughput results](../throughput-results/) -
  full benchmark matrix across all capacity-morph variants.
