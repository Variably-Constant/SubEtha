---
weight: 50
---

# Global registry + per-NUMA scan threads

The `Sidecar` struct holds the per-process registry. Internally
it is a pool of one `NodeSidecar` per detected NUMA node; each
node has its own scanning thread plus its own slot table of
registered primitive instances. Registration routes by the
caller's current NUMA node so the scan path stays on local
caches.

```rust,no_run
pub struct Sidecar {
    nodes: Vec<NodeSidecar>,
    shutdown: Arc<AtomicBool>,
    join_handles: AtomicPtr<Vec<JoinHandle<()>>>,
    instance_count: AtomicUsize,
    max_instances: AtomicUsize,
}
```

The registry holds no lock. Each node keeps its registrations in
slots, in segments of 64 that are appended with a compare-and-swap
as registrations outgrow them. A slot carries a `state` word and a
`users` count:

| State | Meaning |
|---|---|
| free | no registration |
| filling | a `register_raw` is writing one in |
| live | the node's scan and `stats` may enter |
| retiring | an `unregister` is removing it; nothing new enters |

A caller entering a slot raises `users` and then reads `state`; an
`unregister` sets `state` to retiring and then reads `users`, both
sequentially consistent, so either the unregister waits for the
caller or the caller sees the slot retiring and backs out.

## `InstanceId` packing

```rust,no_run
pub type InstanceId = u32;
const NODE_ID_BITS: u32 = 8;
const SLOT_MASK: u32 = (1 << (32 - NODE_ID_BITS)) - 1;

fn pack_id(node: u32, slot: u32) -> InstanceId {
    (node << (32 - NODE_ID_BITS)) | (slot & SLOT_MASK)
}
```

Top 8 bits encode the NUMA node index; bottom 24 bits encode the
slot index inside that node's table. So one process can have up
to 256 NUMA nodes and 16,777,216 slots per node. The hard cap on
total simultaneously-registered instances
([`DEFAULT_MAX_INSTANCES = 10,000`](./#capacity-constants))
sits far below the slot ceiling - if you hit that cap something
else has gone wrong long before address-space exhaustion.

## `register_raw` - the unsafe entry point

```rust,no_run
impl Sidecar {
    pub unsafe fn register_raw(
        &self,
        header: NonNull<HandshakeHeader>,
        ring: NonNull<ObservationRing>,
        instance: Option<NonNull<dyn AdaptiveInstance>>,
        policy: Box<dyn Policy>,
    ) -> InstanceId;
}
```

The contract: `header`, `ring`, and (when provided) `instance`
must remain valid until [`unregister(id)`](#unregister) returns.
[`SidecarBox`](sidecar-box.md) enforces this automatically via
RAII drop order; raw callers must enforce it manually.

The body:

1. Increment the instance count. If the prior value was already at
   the cap, decrement and panic with a diagnostic that names the
   cap, hints at the typical cause (`SidecarBox::new` inside a
   `b.iter()` loop), and points at `set_max_instances` as the escape
   hatch. The panic message text is asserted by the unit test
   `cap_panic_message_is_actionable`.
2. Arm the instance's ring, which allocates its 96 KiB buffer and
   starts producers pushing into it.
3. Build a `Registration` carrying the header, ring and instance
   pointers, the policy, a fresh `InstanceStats` behind a
   `SwapCell`, and the registration timestamp.
4. Route by `current_numa_node() % self.nodes.len()` so a host
   that reports a higher node index than the registry has slots
   for still lands somewhere valid.
5. Claim the first free slot on that node with a compare-and-swap
   from free to filling (appending a segment when every slot is
   taken), write the registration in, publish the slot as live,
   and return `pack_id(node_idx, slot_idx)`.

## `unregister`

```rust,no_run
impl Sidecar {
    pub fn unregister(&self, id: InstanceId);
}
```

Unpacks the `(node, slot)` from the id, moves the slot from live
to retiring, and waits until its `users` count reaches zero: the
node's scan and any `stats` read inside the registration finish,
and nothing new enters. That wait is the load-bearing safety
property. When `unregister` returns, no scan thread holds a
pointer into the unregistered instance's header or ring, so the
caller can drop the underlying memory immediately after.

It then drops the registration, frees the slot, and decrements
`instance_count`. An id whose slot is not live does nothing.

## The scan loop

One thread per node, named `subetha-sidecar-node{N}`:

```rust,no_run
fn run_loop_for_node(self: Arc<Self>, node_idx: usize) {
    let node = &self.nodes[node_idx];
    while !self.shutdown.load(Ordering::Acquire) {
        let covered = node.requested.load(Ordering::Acquire);
        Self::scan_node(node);
        node.completed.store(covered, Ordering::Release);
        if node.requested.load(Ordering::Acquire) == covered {
            thread::park_timeout(POLL_INTERVAL);   // 200 µs
        }
    }
}
```

The node's thread is the only consumer of every ring registered on
the node; `scan_now` asks it for a scan rather than draining a ring
itself. `scan_node` enters each live slot in turn. For each:

1. Drain up to `DRAIN_SAFETY_CAP = 8192` observations from the
   ring. A ring holds 4096; producers pushing while the scan
   drains can keep it from emptying, and the cap keeps one busy
   instance from starving the others.
2. Fold the drained ops into local accumulators
   (`drained_ops`, `drained_lat`, `drained_cont`,
   `drained_kinds`).
3. Inline-dedupe `(op_kind, producer_thread_id)` pairs in a
   bounded `[(u16, u32); N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND]`
   array so a burst of distinct threads stays O(constant) per
   scan.
4. If at least one observation was drained, copy the instance's
   `InstanceStats`, fold the accumulators into the copy, and set
   `last_drain_us`.
5. Call the instance's `policy.decide(&stats, current_tag)`. If it
   returns `Some(new_tag)` AND `new_tag != current_tag`, call
   `instance.apply_migration(new_tag)` (or `header.set_tag(new_tag)`
   for raw registrations without an instance pointer) and increment
   `migrations_triggered` in the copy.
6. Publish the copy with one atomic swap, so `stats` reads a whole
   snapshot and never one half-updated.

## Capacity bounds

| Constant | Value | Bound |
|---|---|---|
| `DRAIN_SAFETY_CAP` | 8,192 | maximum observations drained per instance per scan |
| `DEDUPE_CAP` | `N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND` (32) | maximum `(op_kind, tid)` pairs deduped per scan |
| `POLL_INTERVAL` | 200 µs | scan-thread park between iterations, cut short by `scan_now` |
| `DEFAULT_MAX_INSTANCES` | 10,000 | hard cap on registered instances |

Worst-case scan cost per instance: 8,192 pops at ~10 ns each =
~80 µs per instance. With 100 instances on one node, one scan
iteration takes ~8 ms in the worst case - and the typical case
is far cheaper because most instances are not full at any given
poll.

## The `atexit` shutdown hook

`global()` is backed by a `once_cell::sync::Lazy<Arc<Sidecar>>`.
A static is never dropped at process exit, which would leave the
scan threads alive when the CRT shuts down, and those threads can
raise `STATUS_ACCESS_VIOLATION` at exit when their TLS state races
with main-thread CRT shutdown.

`register_sidecar_atexit()` registers a CRT `atexit`
callback the first time `global()` is called. The callback signals
shutdown, joins every scan thread, and clears the slot tables (so
any other static-drop chain sees an empty registry). It writes
nothing unless a scan thread ended by panic. The CRT runs the
callback on the main thread during normal teardown, before final OS
exit.

## NUMA detection

Two free functions probe the topology:

```rust,no_run
pub fn numa_node_count() -> u32;
pub fn current_numa_node() -> u32;
```

`numa_node_count` calls `GetNumaHighestNodeNumber` on Windows
(returns `highest + 1`) and returns 1 elsewhere. Always returns at
least 1.

`current_numa_node` calls `GetCurrentProcessorNumberEx` plus
`GetNumaProcessorNodeEx` on Windows - the `Ex` variants work
across Windows processor groups (groups of 64 logical CPUs each),
so dual-socket servers with more than 64 logical processors are
routed correctly. The legacy `GetNumaProcessorNode` is capped at
processor 255 and is not called.

On non-Windows the helper reads `/proc/self/stat` field 39
(last-scheduled CPU) and resolves that CPU's
`physical_package_id` via sysfs. Hosts or containers without
`/proc` or `/sys` fall back to node 0. With `numa_node_count` at 1
off Windows, every registration there is filed under node 0.

## Inspection methods

```rust,no_run
impl Sidecar {
    pub fn instance_count(&self) -> usize;
    pub fn max_instances(&self) -> usize;
    pub fn set_max_instances(&self, cap: usize);
    pub fn stats(&self, id: InstanceId) -> Option<InstanceStats>;
    pub fn scan_now(&self);
    pub fn node_count(&self) -> usize;
}
```

`scan_now()` asks every node's thread for a scan, wakes it, and
waits until a scan that started after the request has finished, so
an observation pushed before the call has been drained and counted
when it returns. Tests use it instead of waiting for the 200 µs
poll. Called from a node's own scan thread it does not wait on that
node, and after the sidecar has stopped its threads it returns at
once.

## See also

- [`SidecarBox<T>`](sidecar-box.md) - the RAII wrapper that wires
  `register_raw` / `unregister` into Rust drop order.
- [`AdaptiveInstance`](adaptive-instance.md) - the trait the
  registered instance pointer must satisfy.
- [`InstanceStats`](instance-stats.md) - what `stats(id)` returns.
- [`Policy`](policy.md) - what the scan loop calls after each
  drain.
- [Sidecar control plane index](../) - the architectural
  diagram and the full constants table.
