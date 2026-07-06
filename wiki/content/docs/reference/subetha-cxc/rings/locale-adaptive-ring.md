---
title: "Locale Adaptive Ring"
weight: 22
---

# LocaleAdaptiveRing + PinnedLocale

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Axis](https://img.shields.io/badge/axis-locale--morph-brightgreen)
![Locales](https://img.shields.io/badge/locales-Anon%20%2F%20File%20%2F%20ShmFs-success)

Three-locale ring with cross-process pin invalidation. Pre-allocates
[`AdaptiveRing`](../shared-ring-adaptive/) backings at all three
locales (Anon, File, ShmFs). The active locale is selected by an
MMF-resident `locale_tag`. `migrate_to(target)` bumps the
MMF-resident `locale_generation` first (invalidating outstanding
pinned-locale handles), then drains in-flight items from the old
backing into the new one via `try_send(0, ..)`, then Release-stores
the new `locale_tag`. A `migrate_to` to the currently-active locale
returns immediately without bumping the generation.

## Locale members

| `Locale` variant | Storage | Cross-process? | Disk? |
|---|---|---|---|
| `Locale::Anon` (0) | In-process anonymous mmap | No | No |
| `Locale::File` (1) | File-backed mmap, page cache | Yes (by path) | Yes |
| `Locale::ShmFs` (2) | Named RAM-resident shm (cross-platform shm) | Yes (by name) | No |

`Anon` is cheapest (no syscalls beyond construction). `File` adds
cross-process visibility + disk persistence via the page cache.
`ShmFs` is cross-process without polluting the page cache (use
when the workload does not benefit from caching). All three host
the full AdaptiveRing shape axis on top.

## Two execution paths

- **Adaptive path** (`try_send` / `try_recv`): one Acquire load on
  the locale tag + dispatch to the matching `AdaptiveRing` (which
  itself dispatches by shape). The substrate's default user-facing
  call shape.
- **Pinned path** (`pin_current_locale()` returns `PinnedLocale<'_>`):
  typed handle pinned to the current locale + generation. Methods
  `as_anon()` / `as_file()` / `as_shmfs()` return
  `Option<&AdaptiveRing>` so the caller chains directly into the
  shape-axis pin via `AdaptiveRing::pin_current_shape()`.

## Constraints

- **`max_producers` / `max_consumers` are sizing hints** (each `>= 1`)
  propagated to the inner `AdaptiveRing` instances, which grow their
  per-producer backings on demand past them.
- **Initial locale is `Locale::Anon`**; the cheapest backing.
- **Registrations apply to ALL THREE backings in lockstep**:
  `register_producer()` / `register_consumer()` register the same
  id on Anon + File + ShmFs so a subsequent morph finds the right
  active peer count.

## Pin composition

```text
PinnedLocale (locale axis)
    --> as_anon() / as_file() / as_shmfs() -> &AdaptiveRing
        --> pin_current_shape() (shape axis)
            --> spsc_try_push / mpsc_try_push / mpmc_try_push / vyukov_try_push
                --> SpscRingCore / SharedRing (native primitives)
```

Each pin layer holds one `pin_generation: AtomicU64` (MMF-resident
for the locale layer). One Acquire load per `is_still_valid()`
check per axis; caller chooses cadence.

`PinnedLocale` also exposes `locale()` (the captured locale) and
`pinned_generation()`. Outside the pin protocol, `current_locale() -> Locale`
and `locale_generation() -> u64` read the live tag + generation directly, and
`anon_ring()` / `file_ring()` / `shmfs_ring()` return `&AdaptiveRing` for each
backing unconditionally (the un-pinned counterpart to `as_anon` / `as_file` /
`as_shmfs`). Dropping the ring removes the locale tag/generation MMFs and the
file-backing ring files.

## Ordering stamps (global-FIFO axis across locales)

`create_with_ordering_stamps(...)` is the stamped twin of `create`: it turns on
the [ordering-stamp](../adaptive-ordering/) axis on ALL THREE locale backings
(one stamp kind picked once so the backings agree). The surface mirrors the
other adaptive wrappers: `is_stamped()` reports whether stamps are on,
`ordering_mode() -> Option<OrderingMode>` reads the ACTIVE backing's live mode,
`set_ordering_mode(mode)` flips it on all three backings (so the discipline
follows the ring across migrations), and `inversions()` sums cross-producer
inversions across the three backings.

A locale migration of a merge-mode ring re-stamps items as the transfer drains
them: the drain order IS stamp order under the merge, so the destination
preserves global order. (Under `Unordered` the transfer's round-robin drain can
reorder across producers, the same caveat as shape morphs.)

## Worked example

```rust,no_run
use subetha_cxc::{Locale, LocaleAdaptiveRing, RingShape};
use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;

let ring = LocaleAdaptiveRing::create("/tmp/lar", 1, 1, 1024)?;
ring.register_producer()?;
ring.register_consumer()?;

// Default Anon locale: round-trip via the two-axis pin chain.
let pin_locale = ring.pin_current_locale();
let adaptive = pin_locale.as_anon().expect("anon");
let pin_shape = adaptive.pin_current_shape();
assert_eq!(pin_shape.shape(), RingShape::Spsc);

let payload = 0xDEADBEEFu64.to_le_bytes();
pin_shape.spsc_try_push(&payload)?;
let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
pin_shape.spsc_try_pop(&mut buf)?;

// Morph to ShmFs; in-flight items transfer. Pin invalidates.
ring.migrate_to(Locale::ShmFs)?;
assert!(!pin_locale.is_still_valid());

// Re-pin to reach the ShmFs backing.
let pin_locale = ring.pin_current_locale();
let _shmfs_ring = pin_locale.as_shmfs().expect("shmfs");
# Ok::<(), subetha_cxc::shared_ring::RingError>(())
```

## Sidecar + hysteresis-gated policy

The wrapper ships with `LocaleAdaptiveRingSidecar` - a background
scanner thread that honours application-driven locale requests
under a hysteresis cooldown so rapid-flip requests collapse into a
single migration. The migration cost (every in-flight item copies
between backings) is paid once per cooldown window, not once per
user request.

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::{
    DefaultLocalePolicy, Locale, LocaleAdaptiveRing,
    LocaleAdaptiveRingSidecar,
};

let ring = Arc::new(
    LocaleAdaptiveRing::create("/tmp/topic", 1, 1, 1024).unwrap()
);
ring.register_producer().unwrap();
ring.register_consumer().unwrap();

let sidecar = LocaleAdaptiveRingSidecar::spawn(
    Arc::clone(&ring),
    DefaultLocalePolicy::default(),  // 250 ms hysteresis
    Duration::from_millis(15),       // scan every 15 ms
);

// Application signals intent. The sidecar applies migrations
// under hysteresis - rapid back-and-forth collapses to one.
sidecar.request_locale(Locale::File);
// ... 100 ms later ...
sidecar.request_locale(Locale::ShmFs);  // within cooldown - suppressed
// ... 300 ms later ...
// (the most recent request - ShmFs - fires at the next scan tick
// after the cooldown elapses)

println!("migrations: {}", sidecar.migrations_triggered());
sidecar.shutdown();
```

The trait API for custom policies:

```rust,no_run
use subetha_cxc::{Locale, LocalePolicy, LocalePolicyObservation};

struct MyPolicy;

impl LocalePolicy for MyPolicy {
    fn decide(&self, obs: &LocalePolicyObservation) -> Option<Locale> {
        // obs.current_locale, obs.requested_locale,
        // obs.since_last_migrate
        None
    }
}
```

For request flapping there is `LocaleAdaptiveRingSidecar::spawn_gated(ring,
policy, scan_interval, GateConfig)`: a confidence gate sits between the policy's
recommendation and the migration, so a requested locale must persist across
consecutive scans before the (expensive, item-copying) migration fires. The
plain `spawn` is `spawn_gated` with the gate disabled (identical behavior). The
`DefaultLocalePolicy` cooldown defaults to 250 ms - longer than the shape-morph
default because every migration copies all in-flight items between backings.

## E2E proof

[`examples/locale_morph.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/locale_morph.rs)
exercises anon -- file with in-flight item transfer.

[`examples/locale_shmfs.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/locale_shmfs.rs)
exercises the full three-locale walk (anon -> shmfs -> file)
through the four-axis pin chain.

[`examples/locale_migrate_sidecar_e2e.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/locale_migrate_sidecar_e2e.rs)
exercises the sidecar's hysteresis behaviour: 4 rapid user
requests collapse into 3 migrations because one falls inside the
cooldown window. Prints every observed migration with a timestamp
so the trace is human-auditable.

## When to reach for this primitive

- Workload migrates between storage tiers at runtime (e.g.
  ephemeral → durable based on observed traffic).
- Substrate apps that want page-cache-bypass cross-process IPC
  without the disk write-through of the file locale (use ShmFs).
- Any caller that wants the substrate's locale composition under
  one pin protocol instead of constructing per-locale rings
  manually.

## When NOT to reach for this

- Pure in-process workloads (use `AdaptiveRing` alone; `Anon` is
  the only relevant locale).
- Pure file-backed workloads with no need for morphing (use
  `AdaptiveRing::create(path, ...)` directly).

## References

- Source: `crates/subetha-cxc/src/locale_adaptive_ring.rs` (895
  lines, 11 unit tests across migrate + item transfer, the
  three-locale walk, ordering-stamp continuity, and the sidecar +
  gate). `Locale` / `LocaleAdaptiveRing` / `PinnedLocale` /
  `LocalePolicy` / `DefaultLocalePolicy` / `LocaleAdaptiveRingSidecar`
  are all re-exported at the crate root.
- [`AdaptiveRing`](../shared-ring-adaptive/) - the shape-axis layer
  underneath each locale backing.
- [`ShmFile`](../../specialized/shm-file/) - the cross-platform
  named-shm helper that powers the ShmFs locale.
- [Polymorphic substrate design doc](https://github.com/Variably-Constant/subetha/blob/main/docs/POLYMORPHIC_SUBSTRATE_AXES.md).
