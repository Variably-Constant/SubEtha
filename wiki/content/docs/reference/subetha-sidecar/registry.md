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
    join_handles: Mutex<Vec<JoinHandle<()>>>,
    instance_count: AtomicUsize,
    max_instances: AtomicUsize,
}
```

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
([`DEFAULT_MAX_INSTANCES = 10,000`](../#capacity-constants))
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

1. Compare-exchange the instance count against the cap. If the
   prior value already hits the cap, decrement and panic with a
   diagnostic that names the cap, hints at the typical cause
   (`SidecarBox::new` inside a `b.iter()` loop), and points at
   `set_max_instances` as the escape hatch. The panic message
   text is asserted by the unit test `cap_panic_message_is_actionable`.
2. Build a `Registration` carrying the four pointers, a fresh
   `InstanceStats`, the registration timestamp, and an empty
   `last_observation_at` Mutex.
3. Route by `current_numa_node() % self.nodes.len()` so a host
   that reports a higher node index than the registry has slots
   for still lands somewhere valid.
4. Take the write lock on that node's instances vec, find the
   first vacant slot (or push at the end), and return
   `pack_id(node_idx, slot_idx)`.

## `unregister`

```rust,no_run
impl Sidecar {
    pub fn unregister(&self, id: InstanceId);
}
```

Unpacks the `(node, slot)` from the id, takes the write lock on
that node's slot vec, and replaces the `Some(Registration)` with
`None`. The write lock blocks until any in-flight scan iteration
on that node finishes - which is the load-bearing safety
property. When `unregister` returns, no scan thread holds a
pointer into the unregistered instance's header or ring, so the
caller can drop the underlying memory immediately after.

The decrement on `instance_count` is paired with the slot
clearing inside the same critical section so the count never
overshoots the slot table.

## The scan loop

One thread per node, named `subetha-sidecar-node{N}`:

```rust,no_run
fn run_loop_for_node(self: Arc<Self>, node_idx: usize) {
    while !self.shutdown.load(Ordering::Acquire) {
        self.scan_node(node_idx);
        thread::sleep(POLL_INTERVAL);   // 200 µs
    }
}
```

`scan_node` takes the **read** lock on the node's slot vec
(so it does not block other readers, only `unregister`'s write
lock blocks it). For each populated slot:

1. Drain up to `DRAIN_SAFETY_CAP = 8192` observations from the
   ring. The ring's natural capacity is 4096 slots; the safety
   cap is the catastrophe-mode bound that keeps one busy
   instance from starving the others.
2. Fold the drained ops into local accumulators
   (`drained_ops`, `drained_lat`, `drained_cont`,
   `drained_kinds`).
3. Inline-dedupe `(op_kind, producer_thread_id)` pairs in a
   bounded `[(u16, u32); N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND]`
   array so a burst of distinct threads stays O(constant) per
   scan.
4. If at least one observation was drained, take the
   per-instance `stats` Mutex, fold the accumulators into the
   persistent `InstanceStats`, update `last_observation_at`, and
   release.
5. Call the instance's `policy.decide(&stats_snapshot,
   current_tag)`. If it returns `Some(new_tag)` AND `new_tag !=
   current_tag`, call `instance.apply_migration(new_tag)` (or
   `header.set_tag(new_tag)` for raw registrations without an
   instance pointer) and increment `migrations_triggered`.

## Capacity bounds

| Constant | Value | Bound |
|---|---|---|
| `DRAIN_SAFETY_CAP` | 8,192 | maximum observations drained per instance per scan |
| `DEDUPE_CAP` | `N_OP_KINDS * MAX_TRACKED_THREADS_PER_KIND` (32) | maximum `(op_kind, tid)` pairs deduped per scan |
| `POLL_INTERVAL` | 200 µs | scan-thread sleep between iterations |
| `DEFAULT_MAX_INSTANCES` | 10,000 | hard cap on registered instances |

Worst-case scan cost per instance: 8,192 pops at ~10 ns each =
~80 µs per instance. With 100 instances on one node, one scan
iteration takes ~8 ms in the worst case - and the typical case
is far cheaper because most instances are not full at any given
poll.

## The `atexit` shutdown hook

`global()` is backed by a `once_cell::sync::Lazy<Arc<Sidecar>>`.
A `Lazy` initialised after `main` starts does not run its `Drop`
at process exit, which leaves the scan threads alive when the
CRT shuts down - and historically that produced occasional
`STATUS_ACCESS_VIOLATION` at exit when the scan threads' TLS
state raced with main-thread CRT shutdown.

The fix: `register_sidecar_atexit()` registers a CRT `atexit`
callback the first time `global()` is called. The callback signals
shutdown, joins every scan thread, clears the slot tables (so
any other static-drop chain sees an empty registry), and prints
a confirmation to stderr. The CRT runs the callback on the main
thread during normal teardown, before final OS exit.

## NUMA detection

Two free functions probe the topology:

```rust,no_run
pub fn numa_node_count() -> u32;
pub fn current_numa_node() -> u32;
```

`numa_node_count` calls `GetNumaHighestNodeNumber` on Windows
(returns `highest + 1`) and reads `/proc/self/stat` plus
`/sys/devices/system/cpu/cpu<N>/topology/physical_package_id` on
Linux. Always returns at least 1.

`current_numa_node` calls `GetCurrentProcessorNumberEx` plus
`GetNumaProcessorNodeEx` on Windows - the `Ex` variants work
across Windows processor groups (groups of 64 logical CPUs each),
so dual-socket servers with more than 64 logical processors are
routed correctly. The legacy `GetNumaProcessorNode` is capped at
processor 255 and is not called.

On non-Windows the Linux helper reads `/proc/self/stat` field 39
(last-scheduled CPU) and resolves that CPU's
`physical_package_id` via sysfs. Containers without `/sys` mounted
fall back to node 0.

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

`scan_now()` runs one synchronous scan iteration across every
NUMA node. Tests use it instead of waiting for the 200 µs poll.

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
