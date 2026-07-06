---
title: "Adaptive Ordering"
weight: 22
---

# Adaptive global-FIFO ordering

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Axis](https://img.shields.io/badge/axis-ordering-brightgreen)
![Flag](https://img.shields.io/badge/switch-MMF--resident_flag-success)
![Stamps](https://img.shields.io/badge/stamps-rdtsc%2Fcounter%2Fmonotonic-blue)

The ordering axis of the polymorphic substrate. The composed MPSC /
MPMC shapes beat the Vyukov queue on the cross-process leaderboard
because they have zero CAS - but their guarantee is per-producer
FIFO only. This layer makes global FIFO a runtime property of the
SAME composed rings: every push carries an 8-byte stamp, and a
consumer that k-way-merges ring heads by stamp delivers items in
global order. The ordering toggle is one `Release` store on an
MMF-resident flag - no morph, no data movement, and the in-flight
backlog is retroactively ordered the instant the flag flips because
the stamps were already in the slots.

Three layers, separately useful:

1. **Declaration** - ordering need is semantic, so the caller
   declares it: the [`Ordering`](../../coordination-types/qos-policy/)
   QoS knob (`PerProducer` / `GlobalFifo`). The sidecar acts on the
   declaration: an unstamped
   [`AdaptiveRing`](../shared-ring-adaptive/) morphs to the Vyukov
   shape; a stamped ring flips the merge flag.
2. **Detection** - what IS observable is how often the composed
   interleave is visible: stamped pops feed a cross-producer
   INVERSION counter in the shared header (one count per pop whose
   stamp undercuts the previous pop's). The substrate reports
   inversions/sec; it acts on the rate only when the caller
   pre-authorized an automatic response (`auto_order(threshold)`).
3. **Ordered switch** - `set_ordering_mode(MergeByStamp)` flips the
   shared flag; every process attached to the ring sees it (the
   flag deliberately lives in the mapped region, not process-local
   state). Off -> On and On -> Off are both immediate.

## Stamp sources

Stamping from a shared counter to DETECT ordering need would pay
the exact contended-cache-line cost that makes Vyukov slower than
composed. The escape hatch is `rdtsc`:

| `StampKind` | Cost per push | Order quality | Selected when |
|---|---|---|---|
| `Tsc` | ~20 cycles, zero coherence traffic | global within cross-core TSC skew (ns) | invariant-TSC probe passes (CPUID `0x8000_0007` EDX bit 8, max-leaf-checked; identical on Intel and AMD) |
| `SharedCounter` | one contended `fetch_add` | exact total order | explicit opt-in (`with_ordering_stamps_kind`), or x86 without invariant TSC |
| `Monotonic` | one `CLOCK_MONOTONIC` / `QueryPerformanceCounter` read | global within clock resolution | non-x86 hosts |

Rings opened cross-process adopt the creator's stamp kind from the
region header. Per-producer stamps are strictly increasing
regardless of source skew (the issue site enforces a
`max(now, last + 1)` floor).

Slot-space symmetry: the stamp costs 8 of the 64 Lamport slot
bytes, leaving `STAMPED_PAYLOAD_BYTES = 56` - exactly the Vyukov
payload size, since Vyukov spends the same 8 bytes on its per-slot
sequence atom. A stamped ring never morphs to the Vyukov shape
(`RingError::LayoutMismatch`); the `GlobalFifo` declaration on a
stamped ring is served by the merge flag instead.

## The ordering region

One small shared region per stamped ring (`<prefix>.ordering.bin`
at the File locale, `{prefix}_ordering` named region at ShmFs, an
in-process page at Anon): a header cache line (magic, mode flag,
stamp kind, inversion counter, shared stamp counter, drainer lease)
plus one cache line per producer slot (`issued` + `watermark`
stamps).

## Ordering modes

| `OrderingMode` | Pop behavior | Guarantee |
|---|---|---|
| `Unordered` | existing partition pop + inversion counter | per-producer FIFO, inversion metric |
| `MergeByStamp` | k-way min-stamp merge over ring heads | global FIFO **within stamp skew** - best-effort; with `SharedCounter` stamps it can deliver a lower stamp late under producer lag (a scan/pop TOCTOU, see below). For exact delivery on that path use the [reorder consumer](#exact-delivery-the-reorder-consumer). |
| `MergeStrict` | merge + per-producer watermark gate | **exact** global FIFO with zero time-semantics assumptions; couples release latency to the slowest in-use producer |

The merge peeks every non-empty ring head, picks the minimum stamp,
confirms exactly that slot, and leaves every other head unconsumed
(`SpscRingCore::peek_slot` drop-without-confirm semantics). Three
release gates protect the order:

- **In-flight gate** (both merge modes): producers RESERVE their
  stamp slot before reading the clock and finalize the watermark
  after the push (success or `Full`), so `issued != watermark`
  brackets the entire reserve-stamp-push window. A candidate above
  an in-flight stamp waits for the publish. This is the only bound
  that survives producer descheduling - under preemption or
  virtualization the stamp-to-publish window stretches to scheduler
  quanta, far past any fixed time window (observed live: a WSL2
  vCPU preemption stretched it to ~63,000 cycles).
- **Freshness guard** (time-based stamps, when any ring is empty):
  a candidate younger than the guard (8,192 cycles for TSC, 2,000ns
  for the monotonic clock) could be raced by a stamp a producer has
  not even reserved yet (cross-core clock skew); the merge re-peeks
  until it ages out.
- **Watermark gate** (`MergeStrict` only): every EMPTY in-use
  ring's watermark must have reached the candidate. Closes the
  not-yet-reserved case with no clock assumptions at the price of
  the slowest-participant tax - Vyukov pays that tax at push
  (serialization), the strict merge pays it at pop. Idle producers
  heartbeat via `refresh_watermark`; exiting producers call
  `retire_producer` (terminal: saturates the slot's watermark so
  strict consumers never wait on it again).

## Exact delivery: the reorder consumer

`MergeByStamp` is the cheap merge, and its guarantee is "global FIFO
WITHIN stamp skew" - not exact. The scan that picks the minimum ring
head is a non-atomic snapshot, and the in-flight gate only holds
candidates above a *reserved-but-unpublished* stamp. A producer that
PUBLISHES a lower stamp in the window between the consumer's scan (which
saw that ring empty) and the pop is caught by neither: the scan missed
it, and the gate reports "not in flight" because it is already
published. With time-based stamps the freshness guard covers this window;
with `SharedCounter` stamps (no freshness guard) it does not, so under
producer lag on a many-core host the merge can deliver, e.g., stamp 512
just before 511. The displacement is bounded by the concurrent producer
count (off-by-one in practice); `MergeStrict`'s watermark gate is immune
because it waits until every empty in-use ring's watermark proves no
lower stamp exists. A WRC litmus test confirmed this is a scan/pop TOCTOU,
not a memory-ordering effect (the hosts tested are multi-copy-atomic).

For exact delivery on the SharedCounter path without paying the
`MergeStrict` slowest-producer tax, [`crate::reorder`] corrects on the
consumer side:

| Type | Role |
|---|---|
| `ReorderBuffer` | bounded min-by-stamp buffer; releases the minimum once more than `window` items are held. Exact while `window >= max displacement`; the window grows adaptively and `corrections()` reports if it ever had to. |
| `ReorderingReceiver` | pairs a `PinnedRing` with a `ReorderBuffer` for ergonomic exact pops (`try_recv` / `flush`). |
| `AdaptiveOrderedReceiver` | auto-selects the cheapest exact strategy for the ring: SharedCounter with producers <= `REORDER_PRODUCER_CAP` (256) keeps `MergeByStamp` and reorders with a window sized to the producer count (provably exact); more producers morph the ring to `MergeStrict`; time-based / unstamped rings deliver directly. |

```rust,no_run
use subetha_cxc::reorder::AdaptiveOrderedReceiver;

// Exact GlobalFifo delivery, strategy auto-selected per host/config.
let mut rx = AdaptiveOrderedReceiver::new(&ring, 0);
let mut out = [0u8; 56];
while let Some((_len, _stamp)) = rx.try_recv(&mut out) {
    // deliver &out[.._len] in global stamp order
}
while let Some((_len, _stamp)) = rx.flush(&mut out) {
    // drain the buffered tail in order at end of stream
}
```

Measured (16-vCPU KVM guest, 4 producers -> 1 consumer, consumer drain):
raw `MergeByStamp` ~106 ns/item but delivers off-by-one; the reorder
buffer ~115 ns/item exact (+9%); `MergeStrict` ~135 ns/item exact (+27%).
The reorder consumer keeps most of the cheap-merge throughput while
delivering exactly. On invariant-TSC hosts the freshness guard already
holds, so `AdaptiveOrderedReceiver` delivers directly with no overhead.

## Single drainer

With M concurrent consumers, "global FIFO delivery" is meaningless
downstream - two concurrent pops race regardless of pop order - so
merge mode implies ONE active drainer. On rings with
`max_consumers > 1` the merge pop auto-acquires a drainer lease in
the ordering header (the [`OwnerLease`](../../ownership-types/owner-lease/)
claim protocol embedded in the region so every locale gets it):
losers get `RingError::NotDrainer` and back off; a dead drainer
becomes preemptible after `DRAINER_GRACE_EPOCHS` missed heartbeats
(the QoS sidecar ticks the epoch each scan). Multi-consumer callers
that want exact pop-sequence semantics keep the Vyukov morph path.

## API map

```rust,no_run
use std::sync::Arc;
use subetha_cxc::{AdaptiveRing, OrderingMode, RingShape, StampKind};

// Stamping is FIXED at construction (a runtime stamping toggle
// would change slot interpretation under in-flight unstamped
// items). The merge flag stays runtime-dynamic.
let ring = AdaptiveRing::create("/tmp/ordered_ring", 4, 1, 16384)?
    .with_ordering_stamps()?;            // probe-selected kind
ring.morph_to(RingShape::Mpsc)?;

// Producers: stamped pushes (payload cap 56 bytes).
ring.try_send(0, &42u64.to_le_bytes())?; // stamp prepended transparently

// Another process attaches; the opener validates the region and
// adopts the creator's stamp kind. The flag is shared.
let attached = AdaptiveRing::open("/tmp/ordered_ring", 4, 1, 16384)?
    .with_ordering_stamps()?;

// Consumer: stamp-stripped pops; the inversion counter runs.
let mut out = [0u8; 56];
let n = attached.try_recv(0, &mut out)?;
println!("inversions so far: {}", attached.inversions());

// The ordered switch: one Release store, retroactive over the
// backlog, pins stay valid (pinned pops consult the mode atom).
attached.set_ordering_mode(OrderingMode::MergeByStamp)?;
let (n, stamp) = attached.try_recv_with_stamp(0, &mut out)?;
# let _ = (n, stamp);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Hot loops pin: `pin.stamped_try_push(producer_id, payload)` /
`pin.ordered_try_pop(consumer_id, out)` /
`pin.ordered_try_pop_with_stamp(...)` - the pinned pop reads the
mode atom per call (one Acquire load, a plain MOV on x86 TSO), so
ordering-mode flips do NOT invalidate pins.

Exact-order opt-in: `with_ordering_stamps_kind(StampKind::SharedCounter)`
buys a total stamp order for one contended `fetch_add` per push;
combine with `MergeStrict` for exactness with zero clock
assumptions.

## Automatic promotion

The pieces above compose into a closed loop that promotes a ring
from per-producer FIFO to global FIFO on its own, while traffic
runs:

```mermaid
sequenceDiagram
    participant P as Producers (N processes)
    participant R as Stamped ring (MMF)
    participant C as Consumer pop path
    participant S as QoS sidecar scan
    P->>R: push (8-byte stamp prepended, ~20-cycle rdtsc)
    C->>R: pop strips stamp; undercut vs previous pop?
    C->>R: inversion counter += 1 (shared header)
    S->>R: scan tick: delta(inversions) / interval
    S->>S: DefaultOrderingPolicy.decide(observation)
    Note over S: fires only if auto_order_threshold is set,<br/>rate exceeds it, and hysteresis elapsed
    S->>R: set_ordering_mode(MergeByStamp) - ONE Release store
    Note over R: every attached process sees the flag;<br/>backlog already carries stamps,<br/>so it merges in global order retroactively
```

What makes the loop cheap is WHERE each cost lands. Producers pay
~20 cycles per push for the stamp, with zero coherence traffic
under the TSC kind - they never coordinate. Detection rides the
consumer's existing pop (one comparison against the previous
stamp). The decision runs on the sidecar's scan thread, off every
hot path. And the response is one `Release` store: no morph, no
drain barrier, no data movement, because the in-flight backlog was
stamped at birth and the merge orders it after the fact. The
expensive ingredient - the stamps - was already purchased; the
flip just starts spending it.

Two design decisions worth knowing before relying on it:

- **The substrate never changes semantics uninvited.** Ordering is
  a meaning, not a performance knob, so the inversion rate alone
  triggers nothing. `DefaultOrderingPolicy` acts on the rate only
  when the caller pre-authorized it by setting
  `auto_order_threshold` (inversions/sec); without it the rate is
  reported, not acted on. Declarations (`GlobalFifo`) always act.
- **The auto arm is one-way by construction.** Merged pops read
  zero inversions - the merge eliminates the very signal that armed
  it - so there is no symmetric "rate dropped, disarm" trigger.
  Disarming is the caller's call: a `PerProducer` declaration or an
  explicit `set_ordering_mode(Unordered)`. A `hysteresis` window
  (default 100 ms) keeps declaration flips and auto-arms from
  thrashing.

The whole loop from the top-level builder:

```rust,no_run
use subetha_cxc::AutoIpc;

// Pre-authorize: if the observed cross-producer inversion rate
// exceeds 1,000/sec, the sidecar arms the stamp-merge - global
// FIFO from that pop onward, retroactive over the backlog.
let endpoint = AutoIpc::new("/tmp/orders.bin")
    .producers(4)
    .auto_order(1_000.0)
    .build_adaptive::<u64>()?;
# drop(endpoint);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`OrderingPolicyObservation` carries the full snapshot
(`inversions_per_sec`, `current_mode`, `declared`, participant
counts, `since_last_change`) and `OrderingPolicy` is a public
trait, so a custom policy can promote on whatever it likes - time
of day, participant count, an external flag - with the same
one-store response. The unit test
`auto_order_arms_merge_on_observed_inversion_rate` and the
mid-traffic flip in the [E2E proof](#e2e-proof) are the executable
versions of this section.

## Production surfaces

| Surface | Wiring |
|---|---|
| [`QosPolicy`](../../coordination-types/qos-policy/) | `Ordering { PerProducer, GlobalFifo }` knob + `recommends_ordering_change`. |
| `AdaptiveRingSidecar::spawn_with_qos` | Consults both a shape policy (`QosRingShapePolicy`: GlobalFifo + unstamped -> Vyukov morph) and an `OrderingPolicy` (`DefaultOrderingPolicy`: declaration always; inversion rate when `auto_order_threshold` pre-authorizes) per scan tick, and ticks the drainer epoch. |
| `AdaptiveIpc` | `create_with_ordering(..., ordering, auto_order)` builds the stamped ring; `set_ordering(...)` applies declarations at runtime (stamped: merge flag; unstamped: Vyukov morph); `maybe_promote()` runs the pre-authorized auto-order check. |
| `AutoIpc` | `.ordering(GlobalFifo)` pins inference to the streaming family (deques cannot honor FIFO) and `.auto_order(threshold)` pre-authorizes the automatic response; `.build_adaptive::<T>()` constructs the wired endpoint. |
| [`LocaleAdaptiveRing`](../locale-adaptive-ring/) | `create_with_ordering_stamps` stamps all three locale backings with one kind; `set_ordering_mode` applies to all three so the discipline follows migrations. |
| [`CapacityAdaptiveRing`](../capacity-adaptive-ring/) | `create_*_stamped` constructors; capacity morphs stamp the fresh backing and seed its region from the old one so counter stamps stay monotone across the swap. |

## Bench: the mode ladder

![Ordering modes comparison](/images/ordering_modes_comparison-light.png)

Numbers, methodology, and the bench-audit notes live in
[`docs/ORDERING_MODES_PERFORMANCE.md`](https://github.com/Variably-Constant/subetha/blob/main/docs/ORDERING_MODES_PERFORMANCE.md);
the raw JSON is `docs/ordering_modes_results.json`. Each contender's
consumer CHECKS the guarantee it charges for: per-producer sequence
monotonicity everywhere, and zero inversions for the STRICT merge rows
(they fail the run with a non-zero exit otherwise). The best-effort
`MergeByStamp` row REPORTS its within-skew inversion count rather than
asserting zero - matching its documented contract.

## E2E proof

Two real-multi-process binaries, verified on Windows + WSL Linux
across repeated back-to-back runs:

- `ordering_xproc_consumer` creates the stamped ring and spawns 4
  `ordering_xproc_producer` PROCESSES. Draining 400,000 items
  unordered observes ~30,000-60,000 inversions (the detection
  layer); flipping the flag mid-traffic yields ZERO new inversions
  and monotone stamps from the flip point, with every producer
  sequence complete (zero loss). The `--no-flip` variant is the
  report-only measurement.
- `qos_ordering_morph` proves the declaration path on an unstamped
  ring: the QoS flip drives a sidecar Vyukov morph under live
  traffic with zero loss.

## References

- Source: `crates/subetha-cxc/src/ordering.rs` (984 lines, 12
  unit tests - stamp sources, region, gates, drainer lease) +
  `crates/subetha-cxc/src/adaptive_ring.rs` (stamped paths -
  `with_ordering_stamps[_kind]` / `try_recv_with_stamp` / the pin
  `stamped_try_push` / `ordered_try_pop[_with_stamp]` - the merge
  pop, `OrderingPolicy` / `DefaultOrderingPolicy`, and the
  `spawn_with_qos[_gated]` sidecar; `DRAINER_GRACE_EPOCHS = 3`).
- Exact delivery: `crates/subetha-cxc/src/reorder.rs`
  (`ReorderBuffer` / `ReorderingReceiver` / `AdaptiveOrderedReceiver`,
  unit-tested) + `crates/subetha-cxc/examples/ordering_race_proof.rs`
  (demonstrates the scan/pop TOCTOU via the library's inversion counter
  and proves the auto-selected exact-delivery fix).
- Bench: `crates/subetha-cxc/examples/ordering_modes_compare.rs`
  (writes `docs/ordering_modes_results.json`, from which the
  committed comparison charts are rendered).
