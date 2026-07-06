---
title: "QoS Policy"
weight: 32
---

# QosPolicy + QosSnapshot

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Axis](https://img.shields.io/badge/axis-policy-brightgreen)

DDS-inspired runtime-mutable QoS knobs. Sidecar policies read them
on every scan; applications set them at runtime to drive substrate
morphs (durability -> locale flip, history -> ring capacity sizing,
ordering -> Vyukov morph or merge-flag flip). All five knobs are
MMF-friendly atomic fields; setters publish with Release, readers
consume with Acquire.

## Knobs

| Knob | Type | Values | Substrate effect |
|---|---|---|---|
| Durability | `Durability` enum | `Volatile` / `Transient` / `Persistent` | Maps to `Locale::Anon` / `Locale::ShmFs` / `Locale::File` via `recommended_locale()`. |
| Reliability | `Reliability` enum | `BestEffort` / `Reliable` | Drives whether senders drop on backpressure or block. |
| History | `History` enum | `KeepLastN(u32)` / `KeepAll` | Drives recommended ring capacity via `recommended_capacity()`. |
| Max latency | `Duration` | Any | Wish for delivery latency; sidecars factor into batch-vs-single dispatch. |
| Ordering | `Ordering` enum | `PerProducer` / `GlobalFifo` | The cross-producer delivery-order declaration. A `QosRingShapePolicy` sidecar morphs an unstamped [`AdaptiveRing`](../../rings/shared-ring-adaptive/) to the Vyukov shape on `GlobalFifo`; a stamped ring flips its [merge flag](../../rings/adaptive-ordering/) instead. The stamped by-stamp merge is best-effort (global FIFO within stamp skew); for exact delivery on the SharedCounter path use [`AdaptiveOrderedReceiver`](../../rings/adaptive-ordering/#exact-delivery-the-reorder-consumer). |

## API

| Constructor | Behavior |
|---|---|
| `QosPolicy::new(durability, reliability, history, max_latency)` | Explicit construction. |
| `QosPolicy::streaming_default()` | Volatile + BestEffort + KeepLastN(1024) + 100ms. |
| `QosPolicy::reliable_pubsub_default()` | Transient + Reliable + KeepAll + 1s. |
| `QosPolicy::persistent_log_default()` | Persistent + Reliable + KeepAll + 5s. |
| `QosPolicy::default()` | Equivalent to `streaming_default()`. |

Getters: `durability()`, `reliability()`, `history()`, `max_latency()`, `ordering()`.
Setters: `set_durability(...)`, `set_reliability(...)`, `set_history(...)`, `set_max_latency(...)`, `set_ordering(...)`.
Snapshot: `snapshot() -> QosSnapshot` reads all five in one call.
`QosPolicy::new(...)` takes the four original knobs; ordering
starts at `PerProducer` and is declared via `set_ordering`.

`QosSnapshot::recommends_locale_change(current: Locale) -> Option<Locale>`
returns `Some(target)` when the durability knob disagrees with the
active locale; `None` when they match.
`QosSnapshot::recommends_ordering_change(current: Ordering) -> Option<Ordering>`
is the same shape for the ordering declaration.

Ordering need is semantic - it lives in the application, not the
traffic - so the substrate never auto-changes the guarantee on a
heuristic. What it observes on its own is the cross-producer
INVERSION RATE on stamped rings, which it reports (and acts on only
when the caller pre-authorized an `auto_order` threshold). See the
[adaptive ordering page](../../rings/adaptive-ordering/) for the
full declaration / detection / ordered-switch architecture.

## Worked example

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::qos_policy::{QosPolicy, Durability, Reliability, History};
use subetha_cxc::{Locale, LocaleAdaptiveRing};

let qos = Arc::new(QosPolicy::streaming_default());
let ring = Arc::new(LocaleAdaptiveRing::create("/tmp/q", 1, 1, 256)?);
ring.register_producer()?; ring.register_consumer()?;

// App promotes to persistent log: set the knob, snapshot, migrate.
qos.set_durability(Durability::Persistent);
qos.set_reliability(Reliability::Reliable);
qos.set_history(History::KeepAll);
qos.set_max_latency(Duration::from_secs(5));

let snap = qos.snapshot();
if let Some(target) = snap.recommends_locale_change(ring.current_locale()) {
    ring.migrate_to(target)?;
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

## E2E proof

[`examples/qos_driven_morph.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/qos_driven_morph.rs)
runs three QoS stages (streaming -> reliable_pubsub -> persistent_log)
against a single LocaleAdaptiveRing; each stage's snapshot drives
the matching locale morph. Deterministic across multiple runs.

[`examples/qos_ordering_morph.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/examples/qos_ordering_morph.rs)
exercises the ordering knob end to end: 400,000 items stream from
two producers through a live consumer while the driver declares
`Ordering::GlobalFifo` mid-traffic; the `spawn_with_qos` sidecar
morphs the ring to Vyukov under load, with zero items lost and
per-producer FIFO asserted on every pop.

## References

- Source: `crates/subetha-cxc/src/qos_policy.rs` (444 lines, 8
  unit tests covering defaults, setters, the durability->locale
  map, `recommended_capacity` pow2 rounding, and both
  `recommends_*_change` helpers). `QosPolicy` / `QosSnapshot` /
  `Durability` / `Reliability` / `History` are re-exported at the
  crate root; `Ordering` is re-exported as `QosOrdering`
  (`recommended_capacity`: `KeepLastN(n)` -> `n.next_power_of_two().max(16)`,
  `KeepAll` -> 1024).
- [`LocaleAdaptiveRing`](../../rings/locale-adaptive-ring/) - the
  primary morph target for `recommends_locale_change`.
- [Polymorphic substrate design doc](https://github.com/Variably-Constant/SubEtha/blob/main/docs/POLYMORPHIC_SUBSTRATE_AXES.md).
