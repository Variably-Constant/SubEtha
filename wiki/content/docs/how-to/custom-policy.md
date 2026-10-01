---
weight: 20
---

# Write a custom `Policy`

The sidecar asks a policy for a decision on every scan iteration that
drained at least one new observation: `Policy::decide(stats,
current_tag)` in Rust, and in Python and PowerShell the callable or
ScriptBlock the object was registered with. This guide walks through
writing one end-to-end. The worked example is a
contention-rate-driven migration with hysteresis - the simplest shape
that covers most real production policies.

## The contract

{{< tabs >}}

{{< tab name="Rust" >}}
```rust,no_run
pub trait Policy: Send + Sync + 'static {
    fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32>;
}
```

`Some(new_tag)` triggers `apply_migration(new_tag)` on the
instance (which the primitive overrides for data-layout swaps).
`None` leaves the strategy alone.
{{< /tab >}}

{{< tab name="Python" >}}
```python
def policy(stats: subetha.InstanceStats, current_tag: int) -> int | None: ...
```

A policy is any callable of that shape, passed to an object's
`observe`. A returned tag, an integer from 0 to 4294967295, becomes
the object's tag; `None` leaves it alone. It runs on the sidecar's
scan thread. An exception, or a return of anything else, leaves the
tag where it was and is counted by the registration's
`policy_errors`, with `last_policy_error` holding the exception.
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$policy = { param($stats, $currentTag) <# write one tag, or nothing #> }
```

A policy is a ScriptBlock of that shape, passed to an object's
`Observe`; `$stats` is a `SubEtha.InstanceStats`. Writing one integer
from 0 to 4294967295 moves the object's tag, and writing nothing
leaves it alone. It runs on the sidecar's scan thread in a runspace
its registration opens for it, so it sees none of the registering
session's variables: whatever it needs is written inside the block.
Throwing, or writing anything else, leaves the tag where it was and is
counted by the registration's `PolicyErrors()`, with
`LastPolicyError()` holding the error record.
{{< /tab >}}

{{< /tabs >}}

The sidecar checks `new_tag != current_tag` before calling
`apply_migration`, so a policy that returns the current tag is a
no-op. That makes the same-tag-shortcut safe: a contention-driven
policy returning `Some(MUTEX_TAG)` while the instance is already
on `MUTEX_TAG` does not migrate.

> [!IMPORTANT]
> **From Python and PowerShell, a policy moves a tag.** Every
> structure the bindings expose keeps its own layout whatever its tag
> says, so a policy registered on a structure moves a tag nothing
> reads. The object a policy steers is an `Adaptive`: your code
> records its operations into it and reads its tag to choose how it
> works.

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

{{< tabs >}}

{{< tab name="Rust" >}}
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
{{< /tab >}}

{{< tab name="Python" >}}
```python
CHEAP = 0
SCALING = 1

MIN_SAMPLE_OPS = 1_000
MIGRATE_UP_RATE = 0.20
MIGRATE_DOWN_RATE = 0.05


def contention_policy(stats, current_tag):
    # Don't decide on tiny samples; one early contention spike
    # yanks the strategy on noise otherwise.
    if stats.ops_observed < MIN_SAMPLE_OPS:
        return None

    rate = stats.contention_rate()

    if current_tag == CHEAP and rate > MIGRATE_UP_RATE:
        return SCALING
    if current_tag == SCALING and rate < MIGRATE_DOWN_RATE:
        return CHEAP
    return None
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$contentionPolicy = {
    param($stats, $currentTag)
    # CHEAP is tag 0 and SCALING tag 1. The policy runs in a runspace of
    # its own, so what it needs is written inside the block.
    $minSampleOps = 1000
    $migrateUpRate = 0.20
    $migrateDownRate = 0.05

    # Don't decide on tiny samples; one early contention spike
    # yanks the strategy on noise otherwise.
    if ($stats.OpsObserved -lt $minSampleOps) { return }

    $rate = $stats.ContentionRate()
    if ($currentTag -eq 0 -and $rate -gt $migrateUpRate) { return 1 }
    if ($currentTag -eq 1 -and $rate -lt $migrateDownRate) { return 0 }
}
```
{{< /tab >}}

{{< /tabs >}}

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
neutralizes that oscillation - a workload has to commit to
"clearly contended" or "clearly uncontended" to trigger a swap.

**The same-tag-shortcut.** Each branch returns either the target
tag or nothing. There is no branch that returns the current tag.
That means the policy never triggers a no-op migration; the
sidecar's `new_tag != current_tag` check is a belt to this policy's
suspenders.

## Wiring the policy into a primitive

{{< tabs >}}

{{< tab name="Rust" >}}
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
{{< /tab >}}

{{< tab name="Python" >}}
A policy is passed to `observe`, which registers the object with it
and returns the registration:

```python
import subetha

obj = subetha.Adaptive()
with obj.observe(contention_policy) as registration:
    # Record each operation your code does: its op kind, its latency,
    # and whether it took the slow path.
    obj.record(1, latency_ticks=100, contended=False)
    # Read the tag to choose how the next one runs.
    use_scaling = obj.tag == SCALING
```

`observe(policy)` is also how a structure gets a policy other than
its own. The registration keeps the object alive for as long as it
lasts, so there is nothing unsafe to arrange.
{{< /tab >}}

{{< tab name="PowerShell" >}}
A policy is passed to `Observe`, which registers the object with it
and returns the registration:

```powershell
$obj = New-SubEthaAdaptive
$registration = $obj.Observe($contentionPolicy)
# Record each operation your code does: its op kind, its latency, and
# whether it took the slow path.
$null = $obj.Record(1, 100, $false)
# Read the tag to choose how the next one runs.
$useScaling = $obj.Tag() -eq 1
$registration.Close()
```

`Observe($policy)` is also how a structure gets a policy other than
its own. The registration keeps the object alive for as long as it
lasts, so there is nothing unsafe to arrange.
{{< /tab >}}

{{< /tabs >}}

## Other signal shapes

`InstanceStats` exposes more than just `contention_rate()`. Three
template patterns are worth knowing; each one swaps in for the
body of the contention policy above.

**Multi-producer detection** for promoting an SPSC ring to MPMC:

{{< tabs >}}

{{< tab name="Rust" >}}
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
{{< /tab >}}

{{< tab name="Python" >}}
```python
SPSC = 0
MPMC = 1
OP_SEND = 1
OP_RECV = 2


def multi_producer_policy(stats, current_tag):
    if current_tag != SPSC:
        return None
    if stats.is_multi_thread_for(OP_SEND) or stats.is_multi_thread_for(OP_RECV):
        return MPMC
    return None
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$multiProducerPolicy = {
    param($stats, $currentTag)
    # SPSC is tag 0 and MPMC tag 1; op kind 1 is a send and 2 a receive.
    if ($currentTag -ne 0) { return }
    if ($stats.IsMultiThreadFor(1) -or $stats.IsMultiThreadFor(2)) { return 1 }
}
```
{{< /tab >}}

{{< /tabs >}}

The distinct-thread cache is filled as observations arrive; once
two distinct producer threads have pushed to the same op kind,
`is_multi_thread_for` flips to `true`. An SPSC ring is
UB-on-multi-thread by construction, so detection has to migrate
on the second-thread signal, not on contention.

**Op-mix ratio** for picking between read-heavy and write-heavy
implementations:

{{< tabs >}}

{{< tab name="Rust" >}}
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
{{< /tab >}}

{{< tab name="Python" >}}
```python
FLAT = 0
PERSISTENT = 1
OP_INSERT = 1
OP_GET = 2
OP_REMOVE = 3

READ_HEAVY_THRESHOLD = 0.95


def op_mix_policy(stats, current_tag):
    read_fraction = stats.ratio_of(OP_GET, [OP_INSERT, OP_GET, OP_REMOVE])
    if read_fraction > READ_HEAVY_THRESHOLD and current_tag == FLAT:
        return PERSISTENT
    return None
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$opMixPolicy = {
    param($stats, $currentTag)
    # FLAT is tag 0 and PERSISTENT tag 1; op kinds 1, 2 and 3 are an
    # insert, a get and a remove.
    $readFraction = $stats.RatioOf(2, @(1, 2, 3))
    if ($readFraction -gt 0.95 -and $currentTag -eq 0) { return 1 }
}
```
{{< /tab >}}

{{< /tabs >}}

`ratio_of(numerator, &[denominator_kinds])` returns 0.0 when the
denominator is zero (no observations yet), so combining it with a
sample-size floor is unnecessary. The ratio is structurally
defensive against the divide-by-zero case.

**Latency-driven migration** for promoting busy-spin to park:

{{< tabs >}}

{{< tab name="Rust" >}}
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
{{< /tab >}}

{{< tab name="Python" >}}
```python
HIGH_LATENCY_TICKS = 50_000  # ~15 us at 3.4 GHz Zen+

FAST_PATH_TAG = 0
SLOW_PATH_TAG = 1


def latency_policy(stats, current_tag):
    if stats.ops_observed < 100:
        return None
    avg = stats.average_latency_ticks()
    if avg > HIGH_LATENCY_TICKS:
        return SLOW_PATH_TAG
    if avg < HIGH_LATENCY_TICKS // 4:
        return FAST_PATH_TAG
    return None
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$latencyPolicy = {
    param($stats, $currentTag)
    # FAST_PATH is tag 0 and SLOW_PATH tag 1; 50,000 ticks is about
    # 15 us at 3.4 GHz.
    if ($stats.OpsObserved -lt 100) { return }
    $avg = $stats.AverageLatencyTicks()
    if ($avg -gt 50000) { return 1 }
    if ($avg -lt 12500) { return 0 }
}
```
{{< /tab >}}

{{< /tabs >}}

`average_latency_ticks()` returns `total_latency_ticks / ops_observed`,
with the divide-by-zero case returning 0. Wrapping the comparison
in a sample-size floor lets the policy ignore the first few
samples while the average has not stabilized. The shipped
primitives record no latency, so this shape is for an instance that
measures its own operations: from Python and PowerShell, an
`Adaptive` whose `record` is handed each operation's latency.

## What `InstanceStats` does not give you

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

A policy that windows keeps the last count it saw between asks:

{{< tabs >}}

{{< tab name="Rust" >}}
`decide` takes `&self` and a `Policy` is `Send + Sync`, so the count
lives in an atomic the policy owns, such as an `AtomicU64` swapped on
each call.
{{< /tab >}}

{{< tab name="Python" >}}
```python
class DeltaPolicy:
    """Decides on the observations since the last ask, not the total."""

    def __init__(self):
        self.seen = 0

    def __call__(self, stats, current_tag):
        fresh = stats.ops_observed - self.seen
        self.seen = stats.ops_observed
        return 1 if fresh >= 100 else None


registration = obj.observe(DeltaPolicy())
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$deltaPolicy = {
    param($stats, $currentTag)
    # The registration's runspace lives as long as the registration, so a
    # global variable there carries from one ask to the next.
    $fresh = $stats.OpsObserved - [uint64] $global:seen
    $global:seen = $stats.OpsObserved
    if ($fresh -ge 100) { return 1 }
}
```
{{< /tab >}}

{{< /tabs >}}

## Testing a custom policy

Force a synchronous scan from the test instead of waiting for the
200 µs poll: `Sidecar::scan_now()` in Rust, `sidecar.scan_now()` in
Python, `Invoke-SubEthaSidecarScan` in PowerShell. The pattern:

{{< tabs >}}

{{< tab name="Rust" >}}
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
{{< /tab >}}

{{< tab name="Python" >}}
```python
from subetha import sidecar

obj = subetha.Adaptive()
with obj.observe(contention_policy) as registration:
    # Record operations that should trigger migration.
    for _ in range(2_000):
        obj.record(1, latency_ticks=100, contended=True)

    sidecar.scan_now()

    assert obj.tag == SCALING
    assert registration.stats().migrations_triggered == 1
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
$obj = New-SubEthaAdaptive
$registration = $obj.Observe($contentionPolicy)
# Record operations that should trigger migration.
foreach ($i in 1..2000) { $null = $obj.Record(1, 100, $true) }

Invoke-SubEthaSidecarScan

$obj.Tag() | Should -Be 1
$registration.Stats().MigrationsTriggered | Should -Be 1
$registration.Close()
```
{{< /tab >}}

{{< /tabs >}}

The scan call has every NUMA node's scan thread scan at once and
waits for them, so the policy runs where it always does; by the
time the call returns the policy decision has either committed the
new tag or left the old one in place. The
`migrations_triggered` counter on `InstanceStats` distinguishes
"policy returned the current tag" from "policy returned a new tag".

## See also

- [`Policy` trait + built-in policies](../reference/subetha-sidecar/policy.md) -
  the API surface.
- [`InstanceStats`](../reference/subetha-sidecar/instance-stats.md) -
  every field and accessor the policy sees.
- [Observation pipeline](../explanation/observation-pipeline.md) -
  the end-to-end of how stats get populated.
- [Sidecar registry](../reference/subetha-sidecar/registry.md) -
  how `make_policy()` plumbs into the scan loop.
- The binding references, [Python](../reference/subetha-py/) and
  [PowerShell](../reference/subetha-pwrs/) - `observe`, the
  registration and `InstanceStats` from each.
