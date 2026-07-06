---
weight: 40
---

# Observation pipeline

The sidecar decides whether to migrate a primitive's strategy
based on what it sees in the op stream. This page is the
end-to-end of how the op stream reaches the policy.

The pipeline has four stages, with one cost rule per stage. The
producer side pays nanoseconds per op. The sidecar side pays
hundreds of microseconds per scan. Neither blocks the other.

## Stage 1: producer push

```text
primitive_op() {
    // do the work
    if observation_worth_pushing() {
        observation_ring.push(Observation {
            instance_id, op_kind, flags, latency_ticks, ..ZERO
        });
    }
}
```

`ObservationRing` is 64-byte aligned and holds 4096 slots, each
24 bytes. The producer increments a relaxed-tail-write, the
consumer reads a release-tail and writes a release-head. SPSC -
one producer, one consumer. Push cost is roughly 3 cycles
steady-state. The ring is on the same cache line as the writer's
working set, which is the point.

If the ring is full, the push returns `false` and the
observation is dropped. This is the rule that makes the producer
side never block: the sampling assumption is that 4096 slots is
enough between two 200 µs scans for any sane workload, and a
workload that runs the ring full has so much op-stream signal
that dropping a few samples does not change the policy decision.

**Why TLS-local instead of one shared ring.** A single shared
MPMC ring across all producer threads contends on the
tail-write. Sixteen threads producing 10 M ops a second each
hammer one cache line continuously. TLS-local SPSC rings turn
the producer side into a per-thread cache-line dance with no
inter-thread traffic. The cost is one ring per active thread
instead of one ring per instance - acceptable because rings are
4096 slots times 24 bytes = ~96 KB, and active threads are tens
or hundreds, not millions.

**The producer auto-stamps `thread_id`.** Every observation
carries a `producer_thread_id` field. If the producer leaves it
zero, the ring's `push` method stamps in the current thread's
process-local sequential id (allocated lazily from a single
atomic counter on first call per thread - no syscall). The id
lets the sidecar's drain detect the second distinct producer
thread for any op kind, which is the canonical multi-producer
detection signal.

## Stage 2: sidecar wakes per NUMA node

One scan thread per detected NUMA node, named
`subetha-sidecar-node{N}`. Each polls every 200 µs:

```text
loop {
    if shutdown_requested() { break; }
    scan_this_node();
    sleep(200us);
}
```

Per-NUMA-node sharding keeps the scan thread's cache traffic
local. An instance registered on node 0 lives in node 0's slot
table; the node 0 scan thread touches it, the node 1 scan thread
does not. The routing happens at registration time via
`current_numa_node()` - the thread that calls
`SidecarBox::new(prim)` decides which node owns the instance.

**Why 200 µs.** Short enough that a workload transition gets
noticed within roughly one millisecond (five scans). Long enough
that an idle process pays no measurable CPU on the sidecar. A
hosting process that registers ten instances and then idles sees
the sidecar wake up roughly every 200 µs to find nothing in any
ring, fold nothing, and go back to sleep.

## Stage 3: drain and fold

`scan_this_node` takes the read lock on the node's slot vec
(so it does not block other readers, only `unregister`'s write
lock can fence it out), then walks each populated slot:

```text
for each registered instance on this node:
    drained_ops, drained_lat, drained_cont = 0
    drained_kinds = [0; N_OP_KINDS]
    tid_dedupe = []

    for _ in 0..DRAIN_SAFETY_CAP:
        obs = ring.pop() or break
        drained_ops += 1
        drained_lat += obs.latency_ticks
        drained_cont += (obs.flags & 1)
        drained_kinds[clamp(obs.op_kind)] += 1
        if obs.producer_thread_id != 0:
            tid_dedupe.push_if_new((op_kind, tid))

    if drained_ops == 0: continue

    fold local accumulators into persistent InstanceStats
    for (op_kind, tid) in tid_dedupe:
        record_thread_for_op(stats, op_kind, tid)
```

Three points worth attention.

**Per-scan dedupe of `(op_kind, tid)` pairs.** A bursty workload
can push thousands of observations from the same thread between
two scans. Folding each one into the distinct-tid cache directly
triggers thousands of linear-scan lookups (the cache is a
fixed-size array, not a hash set). Deduping at scan time in a
bounded local array - 32 slots, since
`N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND = 8 * 4 = 32` -
caps the per-scan work regardless of burst size. The producer
side does no dedupe at all; it pays one push per op.

**`DRAIN_SAFETY_CAP = 8192`.** The ring's natural capacity is
4096, so under normal operation the drain bottoms out long
before the cap. The cap is the catastrophe-mode bound: a
misconfigured ring or a degenerate scan-latency-vs-push-rate
ratio cannot pin one instance's drain so long that other
instances on the same node never get scanned.

**`drained_ops == 0` short-circuit.** If nothing was popped, the
sidecar skips the stats-lock acquisition AND the policy decision.
An idle instance costs the scan thread one ring head-vs-tail
comparison (no atomic RMW; the comparison is on the consumer's
private head and a release-loaded tail), then continue. Tens of
thousands of idle instances cost effectively nothing per scan.

## Stage 4: policy decision

After folding, the sidecar reads the per-instance `Policy`:

```text
current_tag = header.tag()
if let Some(new_tag) = policy.decide(&stats_snapshot, current_tag):
    if new_tag != current_tag:
        instance.apply_migration(new_tag)
        stats.migrations_triggered += 1
```

The `decide` callback runs only when at least one observation
was drained. The `new_tag != current_tag` check is the
same-tag-shortcut: a policy that returns
`Some(current_strategy_tag)` does not trigger an
`apply_migration` call. This matters because `apply_migration`
on a primitive with a heavy data-layout swap allocates the new
layout, drains the old, and frees it. That work should never
run for a no-op decision.

The `decide` callback receives an `InstanceStats` snapshot, not
a reference. The snapshot is a copy of the persistent struct at
the moment of the call. Policies are free to compute derived
signals (averages, ratios, multi-thread predicates) from the
snapshot without holding any lock; the convenience accessors
on `InstanceStats` are pure functions.

## Why a sidecar at all

A simpler design has each primitive run its own policy inline -
every op checks the local stats and decides whether to migrate.
This fails three ways.

**Hot-path cost.** The policy decision compares ratios and
thresholds; even at three or four instructions, paying it on
every op dominates the op cost. A `Mutex::lock` that already
takes 14 ns of substrate overhead does not afford another 5 ns
of policy logic.

**Migration coordination.** A primitive that decides to migrate
during one of its own ops has to perform the migration during
that op - which means blocking that op on the migration's drain
phase. The sidecar's separation puts the migration on a
different thread; the op that triggered the decision returns
immediately, and the migration runs while the next ops continue
on the old strategy until the bump.

**Cross-instance signal.** A policy on one instance sometimes
wants to know what other instances on the same host are doing
(NUMA-affinity policy, host-wide memory pressure, etc.). The
sidecar already has every instance's stats; an inline policy has
to walk a process-wide registry to get the same view, which is
exactly what the sidecar provides.

The sidecar pattern is what makes the per-op cost stay flat
regardless of how complex the policy gets.

## See also

- [Architecture](architecture.md) - where the substrate and
  sidecar sit relative to the primitive families.
- [Migration protocol](../reference/subetha-core/migration.md) -
  what the sidecar triggers when the policy returns `Some(new_tag)`.
- [`ObservationRing`](../reference/subetha-core/observation.md) -
  the per-thread SPSC ring's API surface.
- [`InstanceStats`](../reference/subetha-sidecar/instance-stats.md) -
  the persistent accumulator the scan folds into.
- [Sidecar registry](../reference/subetha-sidecar/registry.md) -
  per-NUMA scan thread internals.
