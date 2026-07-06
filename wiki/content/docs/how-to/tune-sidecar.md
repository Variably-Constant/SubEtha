---
weight: 50
---

# Tune the sidecar

The sidecar exposes a deliberately small tuning surface. Most of
its constants - poll interval, observation ring capacity, drain
safety cap - are fixed by design because the workload-vs-cost
trade-off has been measured and pinned. The constants that
remain user-tunable, plus the patterns for changing the rest by
swapping the observation source, are the topic of this page.

## What is tunable: instance cap

The default cap on simultaneously-registered instances is
`DEFAULT_MAX_INSTANCES = 10_000`. Raise it via:

```rust,no_run
use subetha_sidecar::global;

global().set_max_instances(100_000);
```

The cap exists because the worst-case scan cost grows linearly
with registered instance count. At 10,000 instances and the
worst-case 80 µs per instance per scan (a fully-saturated ring),
one scan iteration takes 800 ms - already a problem. The 10,000
default is well above any realistic production load (typical
production processes sit between 10 and 1,000 instances) but low
enough that a misconfigured benchmark crashes early instead of
exhausting host memory at 100,000+ registrations.

Raise the cap only when you have measured your steady-state
instance count and have headroom on the scan cost. Two questions
to answer first:

- What is your steady-state instance count?
  `Sidecar::instance_count()` is the live reading.
- What is your worst-case per-instance drain cost?
  The bench at `crates/subetha-cxc/benches/adaptive_ipc_overhead.rs`
  measures the substrate floor (native primitive vs adaptive-path
  per-op cost); multiply by your expected ops per scan window
  (POLL_INTERVAL is 200 µs, so 200 µs times your per-thread op rate).

If the product exceeds your latency budget, the right move is
to reduce per-instance op-rate or shed observation pushes (see
[PMU offload](#pmu-offload), below), not to raise the cap.

## What is fixed by design

Four constants are deliberately not tunable through the public
API. Each has the reasoning baked into its definition.

### `POLL_INTERVAL = 200 µs`

The scan thread sleeps 200 µs between iterations. The lower
bound is set by the work-vs-overhead trade-off: at much shorter
intervals the scan thread saturates a CPU core for nothing.
The upper bound is set by adaptation latency: a workload
transition gets noticed within ~5 scans (1 ms) at the current
interval; longer intervals make migrations sluggish on workloads
that flip strategies mid-burst.

If your workload genuinely needs a different cadence, the
escape hatch is `Sidecar::scan_now()`. It runs one synchronous
scan iteration across every NUMA node from the calling thread.
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

Each `ObservationRing` holds 4096 slots of 24 bytes each =
~96 KB per ring. There is one ring per active producer thread
that ever touched an adaptive primitive, not one per
primitive, so the total ring footprint scales with thread count,
not instance count.

The 4096 figure balances two things. Smaller rings drop
observations under burst; larger rings cost more memory and
take longer to drain. 4096 is the pinned `RING_CAPACITY` constant
(`subetha-core/src/observation.rs`), chosen for the production
op-stream patterns - SPSC steady-state, occasional millisecond
bursts of contention. If your producer rate is much higher (say,
hundreds of millions of ops per second per thread), drop rates
become non-trivial and the right answer is the PMU offload
described below.

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
table. The scan thread for that node is the only thread that
touches the instance.

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
`SidecarBox::new`. There is no `register_on_node(node_idx, ...)`
API; the sidecar trusts `current_numa_node` to decide.

`Sidecar::node_count()` returns the number of NUMA-pinned scan
threads in the pool. On a single-socket workstation this is
typically 1. On a dual-socket server, 2. The thread names
follow the pattern `subetha-sidecar-node{N}`.

## Diagnostic helpers

Three accessors expose the sidecar's live state:

```rust,no_run
use subetha_sidecar::global;

let s = global();
println!("instances:    {}", s.instance_count());
println!("cap:          {}", s.max_instances());
println!("numa nodes:   {}", s.node_count());
```

`instance_count()` is monotone over the sidecar's lifetime,
incremented on `register_raw` and decremented on `unregister`.
Diverging from your application's expected instance count
indicates either a leaked `SidecarBox` (registered but never
dropped) or a registration that bypassed the wrapper.

`Sidecar::stats(id)` returns the live `InstanceStats` snapshot
for one instance. Combined with `scan_now()`, this is how
test harnesses verify migration decisions without waiting for
the poll interval.

## What you do not get

The sidecar does not expose:

- A way to disable adaptation per instance at runtime. The
  closest thing is registering with `Box::new(NoMigrationPolicy)`
  at construction time, which makes the policy a no-op.
- A way to migrate across NUMA nodes after registration. The
  instance is pinned to whatever node the registering thread was
  on. Moving an instance between nodes requires unregistering
  and re-registering from a thread bound to the new node.
- A way to bulk-drain rings. The scan thread is the single
  consumer; calling `ring().pop()` from anywhere else races the
  scan. If you need a one-shot snapshot, use
  `Sidecar::stats(id)` after `scan_now()` instead.

## See also

- [Sidecar registry](../reference/subetha-sidecar/registry.md) -
  internals of the scan loop and the capacity bounds.
- [Observation pipeline](../explanation/observation-pipeline.md) -
  why each constant has its current value.
- [Compose primitives via `SidecarBox`](sidecar-box.md) - the
  registration patterns this page tunes.
