# Polymorphic substrate: Locale x Protocol x Shape

A design sketch for the three-axis polymorphic category that
unifies `AdaptiveRing`, `AdaptiveIpc`, and the QUIC bridge into one
coherent substrate. The pinned-handoff layer is the bridging
mechanism along all three axes.

## The three axes

| Axis | Members | What it controls |
|---|---|---|
| **Locale** | anon-mem / file-MMF / ssd-spill / remote-via-QUIC | Where the bytes live and how processes reach them |
| **Protocol** | ring / deque / channel / lru / hashmap | What operations the substrate exposes |
| **Shape** | SPSC / MPSC / MPMC / Vyukov / WorkStealing / KeyValue | Concurrency pattern within a protocol |

Each substrate primitive occupies one cell in the 3D grid. The
shipped surface covers a strict subset:

| Primitive | Locale | Protocol | Shape |
|---|---|---|---|
| `SpscRingCore` | anon, file | ring | SPSC |
| `SharedRingMpsc` | anon, file | ring | MPSC |
| `SharedRingMpmc` | anon, file | ring | MPMC |
| `SharedRing` | anon, file | ring | Vyukov |
| `SharedDeque<T>` | anon, file | deque | WorkStealing |
| `SharedHashMap` | anon, file | hashmap | KeyValue |
| `AdaptiveRing` | anon, file | ring | morphs across all 4 ring shapes |
| `AdaptiveIpc<T>` | file | morphs across ring + deque | ring backing is itself `AdaptiveRing` so the shape axis is COMPOSED in by default; deque path is fixed at WorkStealing |
| QUIC bridge | bridges two anon-locale rings | ring (transparent) | preserves endpoint shape |

`AdaptiveRing` is mobility along the **shape** axis at fixed
(locale=anon, protocol=ring). `AdaptiveIpc` is mobility along the
**protocol** axis at fixed (locale=file-MMF) AND a coupled shape
axis (each protocol implies one workload shape). QUIC is a
**locale** bridge: it takes two anon-locale endpoints and shifts
the byte path from same-host to wire.

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

The substrate composes all three pin levels by default.
`AdaptiveIpc<T>` wires its ring backing through `AdaptiveRing`, so
`pin_current_family().as_ring().pin_current_shape()` reaches
`SpscRingCore::try_push` (or `mpsc_try_push`, `mpmc_try_push`,
`vyukov_try_push` depending on the morphed shape) through two
Acquire loads at the caller's chosen cadence. The override path
for callers wanting Vyukov global-FIFO semantics:
`ipc.ring_handle().morph_to(RingShape::Vyukov)` locks the ring
shape at Vyukov; the IPC-level family migration continues to work
on top.

Measured cost of the three-axis composition on the SPSC 1P/1C
default path (single-threaded round-trip,
`benches/adaptive_ipc_overhead.rs` on a Ryzen 7 5700G / Ubuntu 24.04):

- Native bare `SpscRingCore`: 7.0 ns per round-trip.
- Adaptive path (`AdaptiveIpc::send_u64` + `recv`): 21.0 ns.
  Adds the MMF-resident control Acquire-load + IPC family match +
  AdaptiveRing shape-tag Acquire-load + shape match + profile
  counter `fetch_add`.
- Pinned path (3-axis chain): 7.1 ns. Within noise of native; the
  pinned chain is one method call deep at each level (LLVM inlines
  through to `SpscRingCore::try_push`).

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
  default three-axis composition. Override via
  `ring_handle().morph_to(RingShape::Vyukov)`.
- **Locale axis bridge**: covered for `same-host <-> remote-via-QUIC`
  by the `quic_bridge_e2e` example. Same-host locale morphs
  (`anon <-> file`) are wired through `LocaleAdaptiveRing` and the
  `locale_morph` E2E example.

## Why this matters

The substrate today is a collection of primitives + adaptive
dispatchers, with each adaptive layer reinventing how it observes
peer changes, when it triggers morphs, and how it preserves caller
speed across morphs. The three-axis category names the pattern:

- One mechanism (pinned handoff with MMF-resident generation
  counters) covers all three axes.
- One control-flow shape (sidecar polling a policy on a timer)
  drives morphs on every axis.
- One observation protocol (peer counts + workload shape signals)
  feeds into every policy.

A new primitive joining the substrate declares its axis-position
in the 3D grid. The pinned-handoff layer comes free if the
primitive exposes a typed-handle accessor; the sidecar pattern
comes free if the primitive emits the standard observation
signals.
