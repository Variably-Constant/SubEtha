---
title: "Shared Ring Adaptive"
weight: 21
---

# AdaptiveRing + PinnedRing

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/protocol-shape--morphing-brightgreen)
![Pinned](https://img.shields.io/badge/pin-native_primitive_speed-success)

Shape-morphing ring with all four ring family shapes
([Lamport SPSC](../shared-ring-spsc/),
[composed MPSC](../shared-ring-mpsc/),
[composed MPMC](../shared-ring-mpmc/),
[Vyukov global-FIFO](../shared-ring/)) pre-allocated as backings.
The active shape is selected by an `AtomicU8` shape tag; a morph
swaps the tag with a Release-store and marks the old shape's
backing STALE - no data moves; the consumer's pop path drains the
stale backlog before reading the new shape. A `pin_generation`
counter, bumped on every morph, invalidates outstanding pinned
handles.

Two execution paths through the same primitive:

- **Adaptive path** (`try_send` / `try_recv`): full atomic
  dispatch. One Acquire load on the shape tag + one branch +
  the selected backend's native op. ~3-5 ns above the
  underlying primitive.
- **Pinned path** (`pin_current_shape()` returns
  `PinnedRing<'_>`): typed handle for the current shape at
  native speed. Hot loop matches the underlying primitive.
  Caller periodically calls `is_still_valid()` (one Acquire
  load) and re-pins after a morph.
- **Peek-direct path** (`peek_spsc_slot()` returns
  `Option<PeekedSpscSlot<'_>>`): zero-copy view into the SPSC
  backing's next slot. Returns `Some` when the active shape is
  SPSC AND the ring is non-empty; `None` otherwise. The slot
  derefs to `&[u8]` pointing INTO the mmap region; pass that
  slice straight to a downstream consumer (e.g. quinn's
  `write_all`), then call `PeekedSpscSlot::confirm()` to
  release. Used by the bridge primitives' egress loops for
  zero-userspace-copy on the wire side.
- **Frame path** (`send_frame` / `recv_frame`): carries any payload
  size at the active shape. Each record is a self-describing frame - a
  class byte plus a length - inlined in the ring slot when it fits
  `FRAME_INLINE_BUDGET` (51 bytes) or spilled to a shared concurrent
  block region when larger; the producer overrides the choice with
  `send_frame_as`, and the consumer reads the class to recover the
  bytes. Works at every shape because the descriptor rides the slot
  and the region is many-producer / many-consumer safe.

## Constraints

- **Raw-path payload is the smaller of the two slot families** (56
  bytes safe for any morph; 64 bytes if pinned to a SPSC/MPSC/MPMC
  shape). The **frame path** (`send_frame`) lifts this: records up to
  `FRAME_INLINE_BUDGET` (51 bytes) inline, and larger records up to
  the region block size (8 KB by default, set with `with_frames`)
  spill to the shared payload region.
- **Initial shape is `RingShape::Spsc`** (the cheapest backing).
- **`max_producers` + `max_consumers` are sizing HINTS**, not
  ceilings: they set how many per-producer backings are
  pre-allocated up front. `register_producer` past the hint GROWS
  the ring (a new backing pair, published cross-process through
  the shared peer directory); `register_consumer` claims a
  consumer slot and rebalances MPMC ring ownership. Registration
  fails only under a caller-declared `RingContract` ceiling
  (`with_contract` sets `max_concurrent_push` /
  `max_concurrent_pop`, an ordering contract, or a capacity
  ceiling) - the declared contract is the ONLY source of
  `AdaptiveError::TooManyProducers` / `TooManyConsumers`. Without
  one the contract is unbounded and growth is capped only by the
  substrate slot ceilings (4096 concurrent producer slots, 256
  concurrent consumer slots; slots recycle on unregister).

## Constructors (four locales)

All four backings are pre-allocated by every constructor; they differ only
in where the backing memory lives.

| Constructor | Locale | Notes |
|---|---|---|
| `create_anon(max_producers, max_consumers, capacity)` | process-private anon mmap | in-process only; cheapest |
| `create(path_prefix, max_producers, max_consumers, capacity)` | file-backed | cross-process + disk-persistent; one file per backing (`<prefix>.spsc.bin`, `.mpsc.{i}.bin`, `.mpmc.{i}.bin`, `.vyukov.bin`) |
| `open(path_prefix, max_producers, max_consumers, expected_capacity)` | file-backed (attach) | opens an existing `create` set, validating each backing's magic + capacity without re-initializing; the CURRENT backing count comes from the shared peer directory (a ring that grew past the creator's hint opens fully); the shape tag + pin generation are process-local and re-track the shared peer counts on the next op |
| `create_shmfs(name_prefix, max_producers, max_consumers, capacity)` | named RAM-resident shared memory (ShmFs) | cross-process, never touches the page cache; names `{prefix}_spsc` / `_mpsc_{i}` / `_mpmc_{i}` / `_vyukov` |
| `open_shmfs(name_prefix, max_producers, max_consumers, expected_capacity)` | named shared memory (ShmFs, attach) | the ShmFs peer of `open`: attaches to a region a *different* process already `create_shmfs`'d, validating each backing's magic + capacity WITHOUT re-initializing, so a snapshot the creator already enqueued survives. `create_shmfs` re-lays-out every backing (correct for the region's owner, data-loss for a late attacher), so any process that JOINS an existing region must use `open_shmfs`, not `create_shmfs`. Backing count comes from the shared peer directory |
| `create_hugepage(max_producers, max_consumers, capacity)` | huge / large / super pages | each backing on its own 2 MB-paged region (Linux `MAP_HUGETLB`, Windows `MEM_LARGE_PAGES`, FreeBSD `MAP_ALIGNED_SUPER`, macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`); needs a reservation/privilege on Linux/Windows, returns `Err` so the caller can fall back to `create_anon` |

The builders `with_contract`, `with_ordering_stamps` /
`with_ordering_stamps_kind`, and `with_frames` chain off any constructor
(each consumes and returns `self`). All constructors assert
`max_producers >= 1` and `max_consumers >= 1` (hints, not ceilings).

Every constructor also creates / attaches the ring's **peer
directory** (`<prefix>.peers.bin` / `{prefix}_peers` / in-process
for anon): shared producer + consumer slot bitmaps, the published
backing count, MPMC ring ownership, and a topology epoch. Hot
paths poll the epoch with one relaxed load; a change (a peer
registered / unregistered / grew the ring in ANY process) runs the
sync slow path - open the new backings, re-morph the shape.

## Morph protocol

Morphs are AUTOMATIC by default: every register / unregister
re-morphs the shape to the live cross-process peer counts
((1,1) -> SPSC, (N,1) -> MPSC, (N,M) -> MPMC), and other attached
processes follow through the topology epoch on their next op. An
explicit `morph_to` (or `pin_shape`) PINS the shape - the user
override - until `resume_auto_shape`. The mechanism either way:

```text
1. Register/unregister (automatic), a policy sidecar, or an
   explicit morph_to call requests new_shape.
2. If old_shape == new_shape: no-op return.
3. If the previous morph's stale backing still holds a backlog:
   Err(RingError::StaleBacklog) - retry after the consumer drains.
4. pin_generation.fetch_add(1, AcqRel)
     -> all outstanding PinnedRing handles see is_still_valid() == false.
5. stale_shape_tag.store(old as u8, Release)
6. shape_tag.store(new as u8, Release)
     -> subsequent try_send calls route via the new backing;
        try_recv walks the stale backing first until it drains.
7. Caller observes the morph by re-pinning via pin_current_shape().
```

A declared contract also filters this morph. The policy's proposed
shape passes through `contract_filtered_shape` before the confidence
gate, so a `Fifo` ordering contract steers a multi-producer morph to
`Vyukov` rather than the partitioned `Mpmc`: the auto-morph cannot
leave the declared ordering envelope. Under the default (unbounded) contract the
filter is the identity.

No data moves during a morph: the old backing keeps its single
reader (the consumer's stale walk) and the new backing starts
empty, so morphs are safe under saturating traffic with live
producers and a live consumer, and there is no transfer to
overflow the target shape's capacity. The stale marker stays set
until the next morph so a producer push that straddled the tag
flip still lands somewhere the consumer looks. Pinned NATIVE pops
(`spsc_try_pop` etc.) are shape-direct and skip the stale walk;
drain through `try_recv` (or `ordered_try_pop` on stamped rings)
across morphs.

For the developer-facing version of all this - whether you need a lock
(you do not), why the ring is thread- *and* process-safe, what you are
responsible for, and the proof that the morph is safe under live traffic -
see [Concurrency and safety](../../../explanation/concurrency-and-safety.md).

## Worked example

```rust
use std::sync::Arc;
use std::thread;
use subetha_cxc::{AdaptiveRing, RingShape};
use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;

let ring = Arc::new(AdaptiveRing::create_anon(8, 8, 1024)?);

let r = ring.clone();
let prod = thread::spawn(move || {
    for i in 0..100_000u64 {
        let mut buf = [0u8; 56];
        buf[..8].copy_from_slice(&i.to_le_bytes());
        while r.try_send(0, &buf).is_err() { std::hint::spin_loop(); }
    }
});

let r = ring.clone();
let cons = thread::spawn(move || {
    let mut got = 0u64;
    let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
    while got < 100_000 {
        if r.try_recv(0, &mut out).is_ok() { got += 1; }
    }
});

prod.join().unwrap();
cons.join().unwrap();

// Sidecar (or you) detect a peer-count change and morph:
ring.morph_to(RingShape::Mpsc)?;

// Pinned path drops to native speed once the shape is stable.
let pin = ring.pin_current_shape();
assert_eq!(pin.shape(), RingShape::Mpsc);
assert!(pin.is_still_valid());
```

## The ordering axis

`AdaptiveRing::create(...)?.with_ordering_stamps()?` attaches the
[ordering substrate](../adaptive-ordering/): every push carries an
8-byte stamp (payload cap drops to `STAMPED_PAYLOAD_BYTES = 56`),
pops strip it transparently and run a cross-producer inversion
counter, and an MMF-resident flag switches the consumer between the
partition pop and a k-way min-stamp merge that delivers global FIFO
on the composed shapes - no morph, no data movement, backlog
retroactively ordered. Stamped rings never morph to the Vyukov
shape (the stamped slot layout does not fit its 56-byte slots; the
`GlobalFifo` declaration is served by the merge flag instead).
Ordering-mode flips do NOT bump the pin generation; the pinned
`ordered_try_pop` consults the mode atom per call.
`with_ordering_stamps_kind(StampKind)` selects the stamp source
explicitly - `StampKind::SharedCounter` is the exactness opt-in (a
total order at one contended `fetch_add` per push) versus the default
invariant-TSC / monotonic-clock stamp.

The MMF-resident flag is `OrderingMode`, flipped by `set_ordering_mode`
and read by `ordering_mode()`:

| Mode | Pop behavior |
|---|---|
| `Unordered` | partition pop (per-producer FIFO) + the inversion counter |
| `MergeByStamp` | k-way min-stamp merge under a single-drainer lease; releases a candidate once no producer holds an in-flight stamp below it (plus a ~2us freshness guard for time-based stamps when a ring is empty) |
| `MergeStrict` | as `MergeByStamp`, plus every EMPTY in-use ring's watermark must have reached the candidate before release - zero time-semantics assumption |

**MergeStrict liveness is a caller contract.** Because a strict consumer
waits on every in-use producer's watermark, an idle producer must
heartbeat with `refresh_watermark(producer_id)` and an exiting one must
call `retire_producer(producer_id)`, or the strict consumer stalls on
their silence by design. The merge runs under a drainer lease
(`release_drainer` / `tick_drainer_epoch` hand a dead drainer's slot to
another consumer after `DRAINER_GRACE_EPOCHS` missed beats). `inversions()`
exposes the shared cross-producer inversion count; `try_recv_with_stamp`
(and `PinnedRing::ordered_try_pop_with_stamp`) return the popped stamp so a
consumer can ASSERT the monotonicity it paid for rather than trust it.
`is_stamped()` / `stamp_kind()` report the configuration.

## The payload-size axis

The raw `try_send` / `try_recv` path moves fixed-size slots (56 bytes
across any morph, 64 when pinned to a slot-family shape) and rejects
anything larger. The frame path makes the payload size part of the
record, so one ring carries a stream of mixed sizes at every shape.

- `send_frame(producer_id, payload)` writes a self-describing frame: a
  one-byte class tag plus a `u32` length, then either the payload
  inline (when `payload.len() <= FRAME_INLINE_BUDGET`, 51 bytes) or a
  4-byte block index into the shared payload region (larger records).
- `send_frame_as(producer_id, payload, hint)` overrides the choice
  with `LayoutHint::ForceInline` (rejects an over-budget payload) or
  `LayoutHint::ForceOffset` (always region).
- `recv_frame(consumer_id, &mut Vec<u8>)` reads the class, fills the
  buffer (inline slice or region block), and frees the block.
- Both send calls and `recv_frame` return the `FrameClass` the record
  took (`Inline` / `Offset`).

The region is a concurrent fixed-block allocator (multi-producer
alloc, multi-consumer free via an ABA-countered Treiber free list plus
a bump high-water mark), so one region serves every shape including
the true-MPMC Vyukov backing. It is created lazily on the first
oversized record - a ring that only sends small records never
allocates it - and `with_frames(block_size, block_count)` pre-creates
and sizes it. Frames survive shape morphs (descriptors and region
blocks are shape-independent) and are not offered on stamped rings
(stamps and frames both claim the slot head, so `send_frame` returns
`RingError::LayoutMismatch` there). Verified by the
`frame_round_trip_all_shapes`, `frame_survives_morph`,
`frame_override_and_limits`, `frame_rejected_on_stamped_ring`, and
`frame_vyukov_two_thread_mixed_size` tests (the last streams 10,000
mixed-size records through 2 producers and 2 consumers on the Vyukov
shape with every id delivered exactly once), and demonstrated end to
end by `examples/adaptive_ring_frames.rs` (24 mixed-size records
through all four shapes across 3 live morphs, every record recovered
byte-for-byte; run with
`cargo run --release --example adaptive_ring_frames`).

## Pinned-path native API

`pin_current_shape()` returns a `PinnedRing<'_>` (`!Sync`, captures the pin
generation). Besides `shape()` and `is_still_valid()` it exposes the
shape-direct native ops so a stable-shape hot loop skips the tag dispatch:

| Method | For |
|---|---|
| `spsc_try_push` / `spsc_try_pop` | the SPSC backing |
| `mpsc_try_push(producer_id, ..)` / `mpsc_try_pop` | the composed MPSC backing |
| `mpmc_try_push(producer_id, ..)` / `mpmc_try_pop(consumer_id, ..)` | the composed MPMC grid |
| `vyukov_try_push` / `vyukov_try_pop` | the Vyukov global-FIFO backing |
| `stamped_try_push(producer_id, ..)` | a stamped ring (else `NotStamped`) |
| `ordered_try_pop(consumer_id, ..)` / `ordered_try_pop_with_stamp(..)` | stamped pop that reads the live mode atom per call (valid across mode flips) |
| `recv_signal(shape) -> &AtomicU64` | a monitor-wait HINT: arm `monitor_wait::monitor_wait_u64` on it instead of a raw spin loop (Windows deschedules pure spinners). Covers producer line 0 on the composed shapes and the current consumer slot on Vyukov - a hint, not a wake guarantee, so keep waits budget-bounded |

The native pops are shape-direct and do NOT walk the stale backing across a
morph; a consumer that pops through pins across morphs uses
`AdaptiveRing::try_recv` / `PinnedRing::ordered_try_pop`, which do.

Payload-size constants: `ADAPTIVE_SPSC_PAYLOAD_BYTES = 64` (SPSC/MPSC/MPMC
Lamport slots, payload-only) and `ADAPTIVE_VYUKOV_PAYLOAD_BYTES = 56` (the
Vyukov backing spends 8 bytes on its per-slot sequence atom). The
shape-agnostic raw path is bounded by the smaller (56).

Peer / contract accessors round out the surface: `max_producers()` /
`max_consumers()` (construction HINTS), `published_producers()` (backings
that exist right now - pre-allocated + grown, shared across processes),
`active_producers()` / `active_consumers()` (live cross-process counts,
claimed / released by `register_*` / `unregister_*`), `contract()` (the
effective `RingContract`, UNBOUNDED unless declared with `with_contract`),
`shape_is_auto()` / `pin_shape()` / `resume_auto_shape()` (the automatic
shape dial), and `contract_filtered_shape(target)` (maps a proposed shape to
the nearest contract-legal one - applied before every automatic morph).

## Shape-aware observability

Beyond `try_send` / `try_recv`, AdaptiveRing exposes shape-aware
inspection methods that the capacity-morph wrappers and sidecar
policies build on:

| Call | Behavior |
|---|---|
| `ring.approx_len() -> usize` | Sum of in-flight items across every sub-ring of the active shape (single ring for SPSC / Vyukov; sum across per-producer rings for composed MPSC / MPMC). |
| `ring.is_empty() -> bool` | Convenience: `approx_len() == 0` for the active shape; conservative (a `true` value is guaranteed empty at the moment of observation). |
| `ring.sub_ring_capacity() -> usize` | Slot count of a single underlying sub-ring (= ring capacity for SPSC / Vyukov; per-producer slot count for composed MPSC / MPMC). |
| `ring.total_slot_capacity() -> usize` | Total slot inventory across every sub-ring (= `sub_ring_capacity()` for SPSC / Vyukov; `sub_ring_capacity() * n_sub_rings` for composed). |
| `ring.current_shape() -> RingShape` | Acquire-load the current shape tag. |
| `ring.pin_generation() -> u64` | Acquire-load the current pin generation. |

These are used by
[`DefaultCapacityPolicy`](../capacity-adaptive-ring/#sidecar--hysteresis-gated-policy)
to compute fill-ratio (`approx_len() / total_slot_capacity()`) and
decide whether to grow / shrink. They are constant-time per call.

## Automatic morphing

The shape tracks the live peer counts BY DEFAULT, with no thread and
no opt-in: every `register_producer` / `register_consumer` /
`unregister_*` (in any attached process) re-morphs the shape to the
counts, and other processes follow through the shared topology epoch
on their next op. `morph_to` / `pin_shape` are the user overrides
that pin the shape; `resume_auto_shape` resumes tracking;
`set_ordering_mode` is the ordering axis's manual lever.

### Policy sidecar (custom policies / the QoS ordering axis)

`AdaptiveRingSidecar` layers POLICY-driven morphing on top - custom
shape policies, QoS-declaration handling, and the stamped ring's
ordering-mode flips from a background scan thread. With the default
shape policy it has nothing to correct (the register path already
tracks counts); spawn it for custom policies or the QoS axes.

- `AdaptiveRingSidecar::spawn(ring, policy, scan_interval)` scans every
  interval, builds a `PolicyObservation` (active producer/consumer counts,
  current shape, time since last morph, stamped flag), asks the
  `RingShapePolicy`, and applies a `Some(shape)` answer (passed
  through `contract_filtered_shape` first, so an auto-morph never leaves a
  declared ordering envelope; policy morphs do NOT pin the shape).
- `spawn_gated(.., GateConfig)` inserts a confidence gate between the policy
  and the morph (the default `GateConfig` is disabled, reproducing `spawn`).
- `spawn_with_qos(ring, shape_policy, ordering_policy, qos, scan_interval)`
  drives BOTH axes per tick: the shape morph AND, on a stamped ring, the
  ordering-mode flip (computing inversions/sec from the shared counter delta
  and ticking the drainer-lease epoch). The shape axis is ungated by default
  (capacity-class morphs are cheap to reverse); the ordering AUTO-arm
  (`Unordered -> MergeByStamp` from a rising inversion rate under a
  `PerProducer` declaration) is gated by default, because that flip is
  one-way (merged pops read zero inversions, so nothing walks it back).
  Explicit declarations (`GlobalFifo` arm, declaration withdrawal) bypass
  the gate and fire immediately. `spawn_with_qos_gated(.., GateConfig)`
  overrides both gates.
- `morphs_triggered()` / `ordering_flips()` are telemetry counters;
  `shutdown()` stops and joins the thread (also done on `Drop`).

Policies are pluggable via two traits:

| Trait / impl | Decides |
|---|---|
| `RingShapePolicy::decide(&PolicyObservation) -> Option<RingShape>` | the morph target |
| `DefaultRingShapePolicy { hysteresis }` | cheapest shape for the peer counts: `1/1 -> Spsc`, `>=2/1 -> Mpsc`, `*/>=2 -> Mpmc`; returns `None` inside the hysteresis window or when either count is 0 |
| `QosRingShapePolicy { qos, hysteresis }` | as default, but a `GlobalFifo` QoS declaration on an UNSTAMPED ring forces `Vyukov` (stamped rings stay composed and use the merge flag) |
| `OrderingPolicy::decide(&OrderingPolicyObservation) -> Option<OrderingMode>` | the stamped ring's ordering-mode flip |
| `DefaultOrderingPolicy { hysteresis, auto_order_threshold }` | `GlobalFifo` declaration arms `MergeByStamp`; with `auto_order_threshold` set, a `PerProducer` ring whose inversions/sec exceed it auto-arms `MergeByStamp` (one-way) |

## E2E proof

`crates/subetha-cxc/examples/adaptive_ring_morph.rs` runs the
full morph cycle as a real binary:

```
[stage SPSC, 1P/1C]    100,000 items in 21.0ms = 4.76 M items/s
[morph SPSC -> MPSC]   pin_generation 0 -> 1
[stage MPSC, 2P/1C]    200,000 items in 52.3ms = 3.82 M items/s
[morph MPSC -> MPMC]   pin_generation 1 -> 2
[stage MPMC, 4P/4C]    400,000 items in 56.5ms = 7.08 M items/s
[morph MPMC -> Vyukov] pin_generation 2 -> 3
[stage Vyukov, 1P/1C]  100,000 items in 7.1ms = 14.04 M items/s
[pinned hot loop]      1,000 pushed / 1,000 popped via PinnedRing
[morph Vyukov -> SPSC] pin_generation 3 -> 4 (pin invalidated)

stages produced: 800,000   consumed: 800,000
integrity:       PASS (sum_produced == sum_consumed exactly)
```

800,000 items shipped through four live morphs with zero lost,
zero duplicated. Run with
`cargo run --release --example adaptive_ring_morph`.

`crates/subetha-cxc/examples/adaptive_growth_xproc.rs` proves the
AUTOMATIC axis across real OS processes: a ring created with a
1-producer / 1-consumer hint takes 3 producer processes and 2
consumer processes with zero registration errors - slots 1 and 2
GROW the ring (new backings published through the peer directory),
the driver's shape morphs SPSC -> MPSC -> MPMC on its own as each
process joins, MPMC ownership rebalances live between the two
consumers, and 240,000 items arrive exactly once with per-producer
FIFO monotone on both. Run with
`cargo run --release --example adaptive_growth_xproc`.

## When to reach for this primitive

| Use case | Why adaptive wins |
|---|---|
| Service that scales workers up/down at runtime | Starts in SPSC, morphs to MPMC as workers attach, morphs back as they detach. |
| Burst-vs-steady-state workloads | SPSC during quiet periods, MPSC / MPMC during bursts. |
| Operator-deployment-time-unknown shape | Library shipped to consumers who configure peer count externally; no need to re-instantiate. |
| Failover scenarios | Consumer dies and a backup attaches; morph from "1 consumer" to "2 consumers" mid-stream. |

## When NOT to reach for this

| Use case | Reach for instead |
|---|---|
| Known-shape tight loops (4P / 4C forever) | The dedicated typed primitive ([shared-ring-mpmc](../shared-ring-mpmc/), etc.). Adaptive dispatch overhead is pure waste. |
| Latency-critical single-digit-microsecond paths | Same: pick the typed primitive, skip the adaptive layer. |
| Memory-constrained environments | Pre-allocated backings cost ~4x the storage of a single-shape primitive. |

## Known limitations

- **Any peer counts**: consumers can outnumber producers; the MPMC
  ownership table simply leaves the surplus consumers with no rings
  until producers grow (they pop `Empty` in the meantime).
- **One stale backing at a time**: a second morph before the
  previous shape's backlog drains returns
  `RingError::StaleBacklog`; the sidecar retries once the
  consumer catches up.
- **Stale single-reader backings drain through consumer 0**:
  after a morph to MPMC, only consumer 0 walks a stale SPSC /
  MPSC backing (it tolerates exactly one reader).
- **Pin invalidation is caller-polled**: `is_still_valid()` is
  one Acquire load, so callers sample at any cadence (every op,
  every N ops, on backpressure events). No push notification.

## References

- Source: `crates/subetha-cxc/src/adaptive_ring.rs`.
- E2E: `crates/subetha-cxc/examples/adaptive_ring_morph.rs` (morph
  cycle under live traffic) and
  `crates/subetha-cxc/examples/adaptive_growth_xproc.rs` (automatic
  growth + shape morphs across 5 OS processes).
- Peer substrate: `crates/subetha-cxc/src/peer_directory.rs` (shared
  slot claims, backing publication, MPMC ring ownership, topology
  epoch; three locales matching the ring backings).
- Ring family siblings (shapes this primitive composes):
  [shared-ring-spsc](../shared-ring-spsc/),
  [shared-ring-mpsc](../shared-ring-mpsc/),
  [shared-ring-mpmc](../shared-ring-mpmc/),
  [shared-ring](../shared-ring/).
