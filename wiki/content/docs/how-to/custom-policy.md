---
weight: 20
---

# Write a custom `Policy`

The sidecar calls `Policy::decide(stats, current_tag)` on every
scan iteration that drained at least one new observation. This
guide walks through writing one end-to-end. The worked example is
a contention-rate-driven migration with hysteresis - the simplest
shape that covers most real production policies.

## The trait

```rust,no_run
pub trait Policy: Send + Sync + 'static {
    fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32>;
}
```

`Some(new_tag)` triggers `apply_migration(new_tag)` on the
instance (which the primitive overrides for data-layout swaps).
`None` leaves the strategy alone.

The sidecar checks `new_tag != current_tag` before calling
`apply_migration`, so a policy that returns the current tag is a
no-op. That makes the same-tag-shortcut safe: a contention-driven
policy returning `Some(MUTEX_TAG)` while the instance is already
on `MUTEX_TAG` does not migrate.

## A worked example: contention with hysteresis

Suppose you have a primitive whose strategy enum has two values
`CHEAP` and `SCALING` (a Mutex vs RWLock pair, or a ring SPSC vs
MPMC pair, or anything analogous). The policy should:

- Stay on `CHEAP` when contention rate is below 5%.
- Promote to `SCALING` when contention rate crosses 20%.
- Migrate back to `CHEAP` only when contention rate drops below
  5%.

The asymmetric thresholds (5% to migrate down, 20% to migrate up)
are hysteresis. A workload sitting near 12% contention does not
flip between strategies on every scan; the band between 5% and
20% is the dead zone.

```rust,no_run
use subetha_sidecar::{InstanceStats, Policy};

const CHEAP: u32 = 0;
const SCALING: u32 = 1;

const MIN_SAMPLE_OPS: u64 = 1_000;
const MIGRATE_UP_RATE: f64 = 0.20;
const MIGRATE_DOWN_RATE: f64 = 0.05;

pub struct ContentionPolicy;

impl Policy for ContentionPolicy {
    fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32> {
        // Don't decide on tiny samples; one early contention spike
        // yanks the strategy on noise otherwise.
        if stats.ops_observed < MIN_SAMPLE_OPS {
            return None;
        }

        let rate = stats.contention_rate();

        match current_tag {
            CHEAP if rate > MIGRATE_UP_RATE => Some(SCALING),
            SCALING if rate < MIGRATE_DOWN_RATE => Some(CHEAP),
            _ => None,
        }
    }
}
```

Three things are doing work here.

**The `MIN_SAMPLE_OPS` floor.** The sidecar scans every 200 µs.
On a primitive that just got its first observation, the
contention rate is computed from one sample - statistically
meaningless. The floor delays any decision until the sample is
large enough that the rate is a real signal.

**The asymmetric thresholds.** A symmetric threshold at 12.5%
flips the strategy whenever the rate crosses that line. Two
scans per second of a workload with rate oscillating between
10% and 15% cycles migrations indefinitely. The 5% / 20% band
neutralises that oscillation - a workload has to commit to
"clearly contended" or "clearly uncontended" to trigger a swap.

**The same-tag-shortcut.** Each match arm returns either the
target tag or `None`. There is no branch that returns
`Some(current_tag)`. That means the policy never triggers a
no-op migration; the sidecar's `new_tag != current_tag` check
is a belt to this policy's suspenders.

## Wiring the policy into a primitive

`Policy` is consumed via `AdaptiveInstance::make_policy()`:

```rust,no_run
impl AdaptiveInstance for MyPrimitive {
    fn header(&self) -> &HandshakeHeader { &self.header }
    fn ring(&self) -> &ObservationRing { &self.ring }
    fn make_policy(&self) -> Box<dyn Policy> {
        Box::new(ContentionPolicy)
    }
}
```

`make_policy` is called once at registration time
(`SidecarBox::new(primitive)`). The sidecar boxes the returned
`Policy` and stores it inside the per-instance `Registration`.

If your primitive ships with a default policy but you want to
override it for a specific instance, the path is to use
`Sidecar::register_raw` directly and pass your policy:

```rust,no_run
use std::sync::Arc;
use std::ptr::NonNull;
use subetha_sidecar::global;

let prim = Arc::new(MyPrimitive::new());
let header = NonNull::from(prim.header());
let ring = NonNull::from(prim.ring());
let policy: Box<dyn Policy> = Box::new(ContentionPolicy);
let id = unsafe {
    global().register_raw(header, ring, None, policy)
};
```

The cost is that `register_raw` is `unsafe` - the lifetime of
`header` and `ring` is on the caller. See
[Compose primitives via `SidecarBox`](sidecar-box.md) for the
patterns that keep this safe.

## Other signal shapes

`InstanceStats` exposes more than just `contention_rate()`. Three
template patterns are worth knowing; each one swaps in for the
match body of the `ContentionPolicy::decide` above.

**Multi-producer detection** for promoting an SPSC ring to MPMC:

```rust,no_run
const SPSC: u32 = 0;
const MPMC: u32 = 1;
const OP_SEND: u16 = 1;
const OP_RECV: u16 = 2;

fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32> {
    if current_tag != SPSC { return None; }
    if stats.is_multi_thread_for(OP_SEND)
        || stats.is_multi_thread_for(OP_RECV)
    {
        Some(MPMC)
    } else {
        None
    }
}
```

The distinct-thread cache is filled as observations arrive; once
two distinct producer threads have pushed to the same op kind,
`is_multi_thread_for` flips to `true`. An SPSC ring is
UB-on-multi-thread by construction, so detection has to migrate
on the second-thread signal, not on contention.

**Op-mix ratio** for picking between read-heavy and write-heavy
implementations:

```rust,no_run
const FLAT: u32 = 0;
const PERSISTENT: u32 = 1;
const OP_INSERT: u16 = 1;
const OP_GET: u16 = 2;
const OP_REMOVE: u16 = 3;

const READ_HEAVY_THRESHOLD: f64 = 0.95;

fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32> {
    let read_fraction = stats.ratio_of(
        OP_GET,
        &[OP_INSERT, OP_GET, OP_REMOVE],
    );
    if read_fraction > READ_HEAVY_THRESHOLD && current_tag == FLAT {
        Some(PERSISTENT)
    } else {
        None
    }
}
```

`ratio_of(numerator, &[denominator_kinds])` returns 0.0 when the
denominator is zero (no observations yet), so combining it with a
sample-size floor is unnecessary. The ratio is structurally
defensive against the divide-by-zero case.

**Latency-driven migration** for promoting busy-spin to park:

```rust,no_run
const HIGH_LATENCY_TICKS: u64 = 50_000;  // ~15 µs at 3.4 GHz Zen+

const FAST_PATH_TAG: u32 = 0;
const SLOW_PATH_TAG: u32 = 1;

fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32> {
    if stats.ops_observed < 100 { return None; }
    let avg = stats.average_latency_ticks();
    if avg > HIGH_LATENCY_TICKS {
        Some(SLOW_PATH_TAG)
    } else if avg < HIGH_LATENCY_TICKS / 4 {
        Some(FAST_PATH_TAG)
    } else {
        None
    }
}
```

`average_latency_ticks()` returns `total_latency_ticks / ops_observed`,
with the divide-by-zero case returning 0. Wrapping the comparison
in a sample-size floor lets the policy ignore the first few
samples while the average has not stabilised.

## What `InstanceStats` does NOT give you

The sidecar's accumulator is a sum-and-count pair plus a small
per-op-kind cache. It does not give you:

- Per-percentile latency. No `p50_latency_ticks` or
  `p99_latency_ticks`. The accumulator is fixed-size on
  purpose - a long-running instance must not grow its stats
  footprint. If your policy needs percentile latency, track it
  primitive-side in the op push site and expose it via a
  primitive-specific accessor.
- Time series. Each scan folds into the persistent struct; the
  history is what got accumulated, not a window. Policies that
  need windowing compute their signal from the delta of
  `ops_observed` between two consecutive `decide` calls (the
  policy keeps its own state across calls).
- Cross-instance correlation. The `decide` callback sees one
  instance's stats. Policies that need cross-instance signal
  (host-wide memory pressure, NUMA-aware decisions) build their
  own coordination on top - typically a shared atomic counter
  the sidecar increments and the policy reads.

## Testing a custom policy

Use `Sidecar::scan_now()` to force a synchronous scan from the
test thread instead of waiting for the 200 µs poll. The pattern:

```rust,no_run
let prim = SidecarBox::new(MyPrimitive::new());

// Push observations that should trigger migration.
for _ in 0..2_000 {
    prim.ring().push(Observation {
        instance_id: 0, op_kind: 1, flags: 1, // contention bit
        latency_ticks: 100, ..Observation::ZERO
    });
}

global().scan_now();

assert_eq!(prim.header().tag(), EXPECTED_NEW_TAG);
assert_eq!(prim.stats().unwrap().migrations_triggered, 1);
```

The `scan_now` call runs one full scan iteration across every
NUMA node synchronously; by the time it returns the policy
decision has either committed the new tag or left the old one in
place. The `migrations_triggered` counter on `InstanceStats`
distinguishes "policy returned the current tag" from "policy
returned a new tag".

## See also

- [`Policy` trait + built-in policies](../reference/subetha-sidecar/policy.md) -
  the API surface.
- [`InstanceStats`](../reference/subetha-sidecar/instance-stats.md) -
  every field and accessor the policy sees.
- [Observation pipeline](../explanation/observation-pipeline.md) -
  the end-to-end of how stats get populated.
- [Sidecar registry](../reference/subetha-sidecar/registry.md) -
  how `make_policy()` plumbs into the scan loop.
