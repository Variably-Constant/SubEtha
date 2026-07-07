# Polymorphic substrate: Locale x Protocol x Shape x Capacity x Ordering

A design sketch for the five-axis polymorphic category that unifies
`AdaptiveRing`, `AdaptiveIpc`, `CapacityAdaptiveRing`,
`LocaleAdaptiveRing`, and the stamped-ordering path into one coherent
substrate. Four of the five axes morph through the pinned-handoff layer
below; the fifth, ordering, flips through an MMF-resident flag the
consumer reads as it pops. `VirtualEndpoint` sits above the five as the
identity entry, routing local-or-remote at construction rather than
morphing at runtime.

## The five axes

Each axis names one live-morph primitive in the source. "Can it change
under a held pin?" is the test for what counts: every axis below has a
morph method (or, for ordering, a switch), and none of the five needs
its peers to move with it.

| Axis | Members (shipped) | Live-morph primitive | What it controls |
|---|---|---|---|
| **Locale** | anon / file / shmfs (remote through a bridge) | `LocaleAdaptiveRing::migrate_to` | Where the bytes live and how processes reach them |
| **Protocol** | ring / deque | `AdaptiveIpc` family `migrate_to` | Which operation family the substrate exposes |
| **Shape** | SPSC / MPSC / MPMC / Vyukov | `AdaptiveRing::morph_to` | Concurrency pattern within the ring family |
| **Capacity** | pow2 slot count | `CapacityAdaptiveRing::morph_capacity_to` | How many slots the buffer holds |
| **Ordering** | per-producer FIFO / global FIFO | ordered-switch flag (`auto_order` / `QosPolicy::GlobalFifo`) | Delivery-order guarantee on stamped rings |

Four of the five (locale, protocol, shape, capacity) are pinned-handoff
morphs: each owns a typed pin handle and a `pin_generation` that bumps on
morph. Ordering is the exception. Its switch flips an MMF-resident flag,
not the ring backing, so it never invalidates a shape pin; the consumer
reads the flag at `ordered_try_pop` and starts a k-way merge. That is why
the substrate is named for five axes but exposes only four `Pinned*`
morph handles.

Each primitive occupies one cell in the core locale/protocol/shape
grid; the capacity and ordering axes layer on top of the ring shapes,
so the carrier for each rides in the last column. The shipped surface
covers a strict subset:

| Primitive | Locale | Protocol | Shape / axis carried |
|---|---|---|---|
| `SpscRingCore` | anon, file | ring | SPSC |
| `SharedRingMpsc` | anon, file | ring | MPSC |
| `SharedRingMpmc` | anon, file | ring | MPMC |
| `SharedRing` | anon, file | ring | Vyukov |
| `SharedDeque<T>` | anon, file | deque | WorkStealing |
| `SharedHashMap` | anon, file | hashmap | KeyValue |
| `AdaptiveRing` | anon, file | ring | morphs across all 4 ring shapes |
| `AdaptiveIpc<T>` | file | morphs across ring + deque | ring backing is itself `AdaptiveRing` so the shape axis is COMPOSED in by default; deque path is fixed at WorkStealing |
| `LocaleAdaptiveRing` | morphs anon / file / shmfs | ring | carries the **locale** axis; holds one `AdaptiveRing` per locale |
| `CapacityAdaptiveRing` | anon, file, shmfs | ring | carries the **capacity** axis; ArcSwaps the active backing for a new pow2 slot count, old backing on a stale list |
| `AdaptiveRing` + `with_ordering_stamps` | anon, file, shmfs | ring | carries the **ordering** axis; MMF-resident switch flips per-producer FIFO to global FIFO |
| QUIC bridge | bridges two anon-locale rings | ring (transparent) | preserves endpoint shape |

`AdaptiveRing` carries the **shape** axis at fixed (locale=anon,
protocol=ring). `AdaptiveIpc` carries the **protocol** axis at fixed
(locale=file-MMF), with the shape axis composed in through its
`AdaptiveRing` backing. `LocaleAdaptiveRing` carries the **locale**
axis, holding one `AdaptiveRing` per locale and migrating the bytes
between them. `CapacityAdaptiveRing` carries the **capacity** axis,
swapping the active backing for a larger or smaller pow2 slot count.
Stamped rings carry the **ordering** axis through the switch above. And
QUIC is a **locale** bridge at the far end: it takes two anon-locale
endpoints and shifts the byte path from same-host to wire.

## The bridging mechanism: pinned handoff with MMF-resident generation

The pinned-handoff pattern is what lets the substrate morph any
single axis without forcing per-op dispatch tax on the steady-state
hot path. The substrate-level shape is:

```
                +---------------------------+
                |  Adaptive layer (slow)    |
                |  - dispatch by axis tag   |
                |  - bumps pin_generation   |
                |    on every morph         |
                +-----+---------------+-----+
                      |               |
                      | pin_current() |
                      v               |
                +-----------+         |
                | Pin holder|         |
                | - typed   |         | is_still_valid()
                |   handle  |         | (one Acquire load)
                | - native  |         |
                |   ops     |---------+
                +-----------+
                      |
                      | as_ring() / as_deque() / as_<protocol>()
                      v
                +---------------------------+
                |  Native primitive (fast)  |
                |  - no dispatch overhead   |
                |  - full API surface       |
                +---------------------------+
```

The bridging contract across axes:

1. Each adaptive layer owns a `pin_generation: AtomicU64`. Bumped
   on every successful morph along that layer's axis.
2. Pin handles capture both the active axis-tag (e.g. `RingShape`,
   `MmfFamily`) AND the pin_generation at pin time.
3. `is_still_valid()` is one Acquire load comparing the captured
   generation to the current one. The caller chooses the sampling
   cadence (every op, every N ops, on backpressure, on explicit
   refresh request).
4. On invalidation: caller drops the stale pin, re-acquires through
   the adaptive layer, gets a new typed handle.

Cross-process locale is the critical addition for `AdaptiveIpc`:
the pin_generation lives in an MMF-resident `SharedAtomicU64`, so
a remote process holding a pin sees invalidation through the same
kernel-bypass channel as the family-tag flip. No syscall in the
hot path of the validity check.

## Composition across axes

The architectural payoff: pins compose. A pin at one axis level
drops down to a pin at a finer axis level.

```
   PinnedIpc<T>      (locale axis, fixed at this MMF endpoint)
       |
       | .as_ring() -> &SharedRing
       v
   &SharedRing       (protocol axis pinned to "ring")
       |
       | (when SharedRing is wrapped in an AdaptiveRing)
       v
   PinnedRing<'_>    (shape axis, fixed at e.g. MPMC)
       |
       | .mpmc_try_push(producer_id, payload)
       v
   SpscRingCore      (native primitive, no further dispatch)
```

The substrate composes its pin levels by default (locale, protocol,
capacity, and shape each own a typed `Pinned*` handle; ordering rides the
shape pin as a pop-mode). `AdaptiveIpc<T>` wires its ring backing through
`AdaptiveRing`, so
`pin_current_family().as_ring().pin_current_shape()` reaches
`SpscRingCore::try_push` (or `mpsc_try_push`, `mpmc_try_push`,
`vyukov_try_push` depending on the morphed shape) through two
Acquire loads at the caller's chosen cadence. The override path
for callers wanting Vyukov global-FIFO semantics:
`ipc.ring_handle().morph_to(RingShape::Vyukov)` locks the ring
shape at Vyukov; the IPC-level family migration continues to work
on top.

Measured cost of the `AdaptiveIpc` -> `AdaptiveRing` composition (the
locale + protocol + shape chain) on the SPSC 1P/1C default path
(single-threaded round-trip, `benches/adaptive_ipc_overhead.rs` on a
Ryzen 7 5700G / Ubuntu 24.04):

- Native bare `SpscRingCore`: 7.0 ns per round-trip.
- Adaptive path (`AdaptiveIpc::send_u64` + `recv`): 21.0 ns.
  Adds the MMF-resident control Acquire-load + IPC family match +
  AdaptiveRing shape-tag Acquire-load + shape match + profile
  counter `fetch_add`.
- Pinned path (this chain): 7.1 ns. Within noise of native; the pinned
  chain is one method call deep at each level (LLVM inlines through to
  `SpscRingCore::try_push`). The capacity axis adds a fourth `Pinned*`
  level at the same one-Acquire-load cost, and ordering rides the shape
  pin, so the wider chain stays the same shape.

The absolute ns are hardware-bound - the same bench on a Windows Zen+
host reads 14 / 77 / 15 ns - but both load-bearing results hold on
either: the pinned chain stays within noise of native, and the adaptive
layer's per-op overhead is real (and larger, in cycles, on the slower
part).

## QUIC's place on the locale axis

The QUIC bridge is a **locale adapter**: it takes a pair of
endpoints at locale=anon-mem (SharedRing on each host) and ferries
bytes between them using QUIC streams over UDP.

```
   host A                                    host B
   ------                                    ------
   app code                                  app code
      |                                         ^
      v                                         |
   AdaptiveIpc<T> (file-MMF)                 AdaptiveIpc<T> (file-MMF)
      |                                         ^
      | PinnedIpc::as_ring() -> &SharedRing     |
      v                                         |
   QUIC bridge client                        QUIC bridge server
   (pulls from local ring,                   (writes to local ring,
    writes QUIC stream)                       reads QUIC stream)
      |                                         ^
      |               UDP / QUIC                |
      +-----------------------------------------+
                 wire format
```

The bridge consumes the pinned-handoff protocol from both
endpoints. It does NOT add a third pin level; it operates at the
locale-axis level and is transparent to the protocol + shape axes
underneath. Application code on either host sees a `SharedRing`
that happens to be paired across the wire.

The example at `examples/quic_bridge_e2e.rs` runs both endpoints
in the same process for E2E coverage of the wire format and the
local-ring -> wire -> local-ring round-trip. The same architecture
splits across two hosts when each endpoint binds a real UDP socket
on its own host.

## Morph cost characteristics per axis

Each axis has a different cost-of-morph. The pinned-handoff layer
amortises this cost by ensuring it is paid only at morph
boundaries, not per op.

| Axis | Morph cost | Per-op cost when stable | Notes |
|---|---|---|---|
| Shape (SPSC <-> MPSC <-> MPMC) | items in flight x per-op transfer cost (microseconds) | 1.06-1.09x native (measured on `AdaptiveRing`) | All shapes share the SpscRingCore primitive; transfer is byte copy through one buffer. |
| Protocol (ring <-> deque) | items in flight x per-op transfer cost; protocols pre-allocated so no syscall | within noise of native (measured on `AdaptiveIpc`) | Different primitives need a marshal/unmarshal step at the morph boundary because slot layout differs. |
| Locale (anon <-> file <-> ssd <-> remote) | mmap remap + page eviction; potential network round-trip for remote | depends on locale (anon = native, file = native, ssd = first-touch latency, remote = QUIC RTT) | Asymmetric for remote: the receiving side must run the bridge server before the locale-shift completes on the sending side. |
| Capacity (grow / shrink pow2) | one ArcSwap store on the active pointer + push the old backing to a stale list; no data copy | pinned: native; unpinned: one ArcSwap load + stale-list walk + inner dispatch per call | The old backing stays readable on the stale list until the consumer drains it, so in-flight items survive the capacity morph. |
| Ordering (per-producer <-> global FIFO) | one store on the MMF-resident switch flag; the ring backing does not change | per-producer: native pop; global FIFO: a k-way min-stamp merge over the ring heads at each pop | Retroactive: stamps written at push time order the in-flight backlog the moment the merge turns on. No drain, no re-send. |

## Shipped coverage of the grid

Today's substrate covers:

- **Shape axis morph**: complete. `AdaptiveRing` exposes
  SPSC/MPSC/MPMC/Vyukov + sidecar-driven morphs + the
  `adaptive_ring_morph` + `adaptive_ring_sidecar` examples prove
  it E2E.
- **Protocol axis morph**: complete for ring <-> deque.
  `AdaptiveIpc` exposes both backings + sidecar-driven promotion +
  the `adaptive_ipc_pinned` example proves it E2E with the
  pinned-handoff layer driving both endpoints. The ring backing
  composes the shape axis: `AdaptiveIpc.ring: AdaptiveRing`, so
  PinnedIpc -> AdaptiveRing -> PinnedRing -> SpscRingCore is the
  default locale + protocol + shape composition. Override via
  `ring_handle().morph_to(RingShape::Vyukov)`.
- **Locale axis bridge**: covered for `same-host <-> remote-via-QUIC`
  by the `quic_bridge_e2e` example. Same-host locale morphs
  (`anon <-> file <-> shmfs`) are wired through `LocaleAdaptiveRing`
  and the `locale_morph` + `locale_migrate_sidecar_e2e` examples.
- **Capacity axis morph**: complete. `CapacityAdaptiveRing` grows and
  shrinks the pow2 slot count through an ArcSwap of the active backing
  plus a stale list; sidecar-driven, with the `capacity_morph_e2e`,
  `cap_morph_sidecar_e2e`, and `capacity_morph_xproc` examples proving
  it E2E, cross-process included.
- **Ordering axis switch**: complete. A stamped `AdaptiveRing` flips
  per-producer FIFO to global FIFO through the MMF-resident switch;
  `auto_order` drives it from the observed inversion rate, and the
  `qos_ordering_morph`, `ordering_race_proof`, and `auto_order_xproc`
  examples prove it E2E.

## Why this matters

The substrate today is a collection of primitives + adaptive
dispatchers, with each adaptive layer reinventing how it observes
peer changes, when it triggers morphs, and how it preserves caller
speed across morphs. The five-axis category names the pattern:

- One mechanism (pinned handoff with MMF-resident generation
  counters) covers the four pinned axes; ordering flips through an
  MMF-resident switch the consumer reads at pop.
- One control-flow shape (sidecar polling a policy on a timer)
  drives morphs on every axis.
- One observation protocol (peer counts + workload shape signals)
  feeds into every policy.

A new primitive joining the substrate declares its axis-position
in the five-axis grid. The pinned-handoff layer comes free if the
primitive exposes a typed-handle accessor; the sidecar pattern
comes free if the primitive emits the standard observation
signals.
