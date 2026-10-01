---
weight: 50
---

# Tune the sidecar

The sidecar exposes a deliberately small tuning surface. Most of
its constants - poll interval, observation ring capacity, drain
safety cap - are fixed. This page covers the one setting you
can change, the instance cap, and what each fixed constant does.

## What is tunable: instance cap

The default cap on simultaneously-registered instances is
`DEFAULT_MAX_INSTANCES = 10_000`. Raise it via:

{{< tabs >}}

{{< tab name="Rust" >}}
```rust,no_run
use subetha_sidecar::global;

global().set_max_instances(100_000);
```
{{< /tab >}}

{{< tab name="Python" >}}
```python
from subetha import sidecar

sidecar.set_max_instances(100_000)
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
Set-SubEthaSidecar -MaxInstances 100000
```
{{< /tab >}}

{{< /tabs >}}

The cap exists because the worst-case scan cost grows linearly
with registered instance count. A fully saturated ring drains 4096
observations at about 10 ns each, about 40 µs per instance per
scan, so 10,000 saturated instances make one scan iteration take
about 400 ms. The 10,000 default sits above the 10 to 1,000
instances production processes carry. A benchmark that registers
an instance on every iteration reaches it and panics, an order of
magnitude before the host runs out near 94,000 registrations.

Raise the cap only when you have measured your steady-state
instance count and have headroom on the scan cost. Two questions
to answer first:

- What is your steady-state instance count?
  `Sidecar::instance_count()` is the live reading
  (`sidecar.instance_count()` in Python, `InstanceCount` from
  `Get-SubEthaSidecar` in PowerShell).
- What is your worst-case per-instance drain cost?
  The bench at `crates/subetha-cxc/benches/adaptive_ipc_overhead.rs`
  measures the substrate floor (native primitive vs adaptive-path
  per-op cost); multiply by your expected ops per scan window
  (POLL_INTERVAL is 200 µs, so 200 µs times your per-thread op rate).

If the product exceeds your latency budget, reduce the
per-instance op rate or register fewer instances, not raise the
cap. An instance that is not registered with the sidecar pushes no
observations and is never scanned.

## What is fixed by design

The constants below have no setter in the public API.

### `POLL_INTERVAL = 200 µs`

The scan thread sleeps 200 µs between iterations. The lower
bound is set by the work-vs-overhead trade-off: at much shorter
intervals the scan thread saturates a CPU core for nothing.
The upper bound is set by adaptation latency: a workload
transition gets noticed within ~5 scans (1 ms) at the current
interval; longer intervals make migrations sluggish on workloads
that flip strategies mid-burst.

If your workload genuinely needs a different cadence, the
escape hatch is `Sidecar::scan_now()` (`sidecar.scan_now()` in
Python, `Invoke-SubEthaSidecarScan` in PowerShell). It has every
NUMA node's scan thread scan at once and waits for them; the
calling thread drains nothing itself.
Tests use it to skip the poll wait. A latency-critical app
calling `scan_now()` after a known-significant op pushes
adaptation latency from ~1 ms down to the time it takes the
scan iteration to drain and decide.

### `DRAIN_SAFETY_CAP = 8192`

The maximum number of observations the scan drains per instance
per iteration. The ring's natural capacity is 4096, so under
normal operation the drain bottoms out at the ring head far
before the cap. The cap is the catastrophe-mode bound: a
misconfigured workload that keeps the ring perpetually full
cannot pin one instance's drain so long that other instances
on the same NUMA node never get scanned.

At 4096 ops drained per instance and 10 ns per pop, a saturated
drain costs ~40 µs - well under the 200 µs poll budget. With
100 saturated instances on one node, ~4 ms per scan, still
below user-perceivable thresholds.

### `RING_CAPACITY = 4096`

Each `ObservationRing` holds 4096 slots of 24 bytes each, 96 KiB
per ring. Every adaptive primitive instance carries its own ring,
and its buffer is allocated when the sidecar registers the
instance and arms the ring; an instance that is never registered
allocates no buffer. The ring footprint scales with the registered
instance count.

A push to a full ring is dropped. 4096 is the `RING_CAPACITY`
constant in `subetha-core/src/observation.rs`.

### `N_OP_KINDS = 8`, `MAX_TRACKED_THREADS_PER_KIND = 4`

The `InstanceStats` per-op-kind cache holds 8 op kinds and 4
distinct producer threads per op kind. Index 0 of `op_kind` is
reserved for "unspecified", leaving 7 valid op kinds per
primitive. Every shipping primitive fits: the widest op-kind
vocabularies in `sidecar_ops` are `hash_map`, `bit_vec`, and
`linked_list` at 6 kinds each, one below the cap.

`MAX_TRACKED_THREADS_PER_KIND = 4` is the cardinality cache for
multi-producer detection. The cache saturates at 5 (= MAX + 1),
which means "more threads than the cache can hold". Policies
that promote SPSC to MPMC do so on the second-thread signal
(`is_multi_thread_for(kind) == true`), not on the exact count,
so saturation at 5 is the right amount of information.

## NUMA routing

NUMA routing is automatic and does not need tuning. At
registration time (`SidecarBox::new(...)` or
`Sidecar::register_raw(...)`), the sidecar calls
`current_numa_node()` and pins the instance to that node's slot
table. That node's scan thread drains the instance, and
`Sidecar::scan_now()` has that thread scan at once rather than
draining the instance from the caller.

The mechanism:

- **Windows.** `GetCurrentProcessorNumberEx` plus
  `GetNumaProcessorNodeEx`. The `Ex` variants work across
  Windows processor groups (groups of 64 logical CPUs each), so
  multi-socket servers with more than 64 logical processors
  route correctly.
- **Linux.** Read `/proc/self/stat` field 39 (last-scheduled
  CPU), then resolve `/sys/devices/system/cpu/cpu<N>/topology/
  physical_package_id` to get the node index. Containers without
  `/sys` mounted fall back to node 0.

If you want a specific instance on a specific node, the move is
to pin the constructing thread to that node before calling
`SidecarBox::new`, or before `observe()` from Python and
PowerShell. There is no `register_on_node(node_idx, ...)` API; the
sidecar trusts `current_numa_node` to decide.

`Sidecar::node_count()` returns the number of NUMA-pinned scan
threads in the pool. On a single-socket workstation this is
typically 1. On a dual-socket server, 2. The thread names
follow the pattern `subetha-sidecar-node{N}`.

## Diagnostic helpers

Three accessors expose the sidecar's live state:

{{< tabs >}}

{{< tab name="Rust" >}}
```rust,no_run
use subetha_sidecar::global;

let s = global();
println!("instances:    {}", s.instance_count());
println!("cap:          {}", s.max_instances());
println!("numa nodes:   {}", s.node_count());
```
{{< /tab >}}

{{< tab name="Python" >}}
```python
from subetha import sidecar

print("instances:   ", sidecar.instance_count())
print("cap:         ", sidecar.max_instances())
print("numa nodes:  ", sidecar.node_count())
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
Get-SubEthaSidecar
```

```text
InstanceCount : 0
MaxInstances  : 10000
NodeCount     : 1
```
{{< /tab >}}

{{< /tabs >}}

`instance_count()` is the number of instances registered now:
`register_raw` adds one and `unregister` removes one.
Diverging from your application's expected instance count
indicates either a leaked `SidecarBox` (registered but never
dropped) or a registration that bypassed the wrapper; from
Python and PowerShell, a registration still held somewhere.

`Sidecar::stats(id)` returns the live `InstanceStats` snapshot
for one instance, as a registration's `stats()` does in Python and
its `Stats()` in PowerShell. Combined with a scan, this is how
test harnesses verify migration decisions without waiting for
the poll interval.

## A managed ring's own sidecar

A managed ring is apart from all of the above. A `Ring`,
`CapacityRing` or `LocaleRing` built as managed runs a sidecar
thread of its own that scans only that ring, at the interval its
caller names, and it neither counts toward the instance cap nor
waits on the global scan.

{{< tabs >}}

{{< tab name="Rust" >}}
```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::adaptive_ring::{
    AdaptiveRing, AdaptiveRingSidecar, DefaultRingShapePolicy, SCAN_INTERVAL_DEFAULT_US,
};

let ring = Arc::new(AdaptiveRing::create("/tmp/events.bin", 1, 1, 4096).unwrap());
let sidecar = AdaptiveRingSidecar::spawn(
    Arc::clone(&ring),
    DefaultRingShapePolicy::default(),
    Duration::from_micros(SCAN_INTERVAL_DEFAULT_US),
);
```
{{< /tab >}}

{{< tab name="Python" >}}
```python
# A Ring scans every 250 microseconds unless told otherwise.
ring = subetha.Ring(path, 4096, managed=True)
# A CapacityRing and a LocaleRing name no default.
grows = subetha.CapacityRing(grows_path, 64, managed=True, scan_interval_us=1_000)
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
# A Ring scans every 250 microseconds unless told otherwise.
$ring = New-SubEthaRing -Path $path -Capacity 4096 -Managed
# A CapacityRing and a LocaleRing name no default.
$grows = New-SubEthaCapacityRing -Path $growsPath -Capacity 64 -Managed -ScanIntervalUs 1000
```
{{< /tab >}}

{{< /tabs >}}

The interval is a latency budget rather than a tuned optimum:
measured on Linux and FreeBSD from 250 to 10,000 microseconds, a
request/response round trip through a managed ring costs one
interval and a streaming caller costs nothing, with no knee in that
range. The 250 microsecond default is `SCAN_INTERVAL_DEFAULT_US`,
which the C ABI's `SUBETHA_SCAN_INTERVAL_DEFAULT_US` and both
bindings share.

## What you do not get

The sidecar does not expose:

- A way to disable adaptation per instance at runtime. The
  closest thing is registering with `Box::new(NoMigrationPolicy)`
  at construction time, which makes the policy a no-op; from
  Python and PowerShell, closing the registration and observing
  the object again without a policy.
- A way to migrate across NUMA nodes after registration. The
  instance is pinned to whatever node the registering thread was
  on. Moving an instance between nodes requires unregistering
  and re-registering from a thread bound to the new node.
- A way to bulk-drain rings. The scan thread is the single
  consumer; calling `ring().pop()` from anywhere else races the
  scan. If you need a one-shot snapshot, use
  `Sidecar::stats(id)` after `scan_now()` instead, or a
  registration's stats after a scan from Python and PowerShell.

## See also

- [Sidecar registry](../reference/subetha-sidecar/registry.md) -
  internals of the scan loop and the capacity bounds.
- [Observation pipeline](../explanation/observation-pipeline.md) -
  why each constant has its current value.
- [Compose primitives via `SidecarBox`](sidecar-box.md) - the
  registration patterns this page tunes.
