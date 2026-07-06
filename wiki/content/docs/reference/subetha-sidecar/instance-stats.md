---
weight: 40
---

# `InstanceStats` - drain-and-fold telemetry

The accumulator the sidecar maintains per registered instance. Each
scan iteration drains the instance's `ObservationRing` and folds
the popped observations into this struct.

```rust,no_run
#[derive(Debug, Clone, Copy)]
pub struct InstanceStats {
    pub ops_observed: u64,
    pub total_latency_ticks: u64,
    pub contention_ops: u64,
    pub op_kind_counts: [u64; N_OP_KINDS],                                                // 8 slots
    pub last_seen_us_ago: u64,
    pub migrations_triggered: u64,
    pub per_op_kind_distinct_threads: [[u32; MAX_TRACKED_THREADS_PER_KIND]; N_OP_KINDS],  // 4 by 8
    pub per_op_kind_distinct_count: [u8; N_OP_KINDS],
}
```

| Field | What it counts |
|---|---|
| `ops_observed` | total number of observations drained since registration |
| `total_latency_ticks` | sum of `Observation.latency_ticks` across all drains |
| `contention_ops` | count of observations with `flags & 1 != 0` |
| `op_kind_counts[k]` | observations whose `op_kind == k` (clamped to N_OP_KINDS) |
| `last_seen_us_ago` | microseconds since the last non-empty drain |
| `migrations_triggered` | times the sidecar's scan called `apply_migration` on this instance |
| `per_op_kind_distinct_threads[k]` | up to 4 distinct producer tids for op_kind `k` |
| `per_op_kind_distinct_count[k]` | how many distinct tids have been observed for op_kind `k`; saturates at `MAX + 1` |

## Convenience accessors

```rust,no_run
impl InstanceStats {
    pub fn average_latency_ticks(&self) -> u64;
    pub fn contention_rate(&self) -> f64;
    pub fn op_kind_total(&self) -> u64;
    pub fn ratio_of(&self, kind: u16, total_kinds: &[u16]) -> f64;
    pub fn distinct_threads_for(&self, kind: u16) -> u8;
    pub fn is_multi_thread_for(&self, kind: u16) -> bool;
}
```

### `average_latency_ticks`

`total_latency_ticks / ops_observed`, with the divide-by-zero case
returning `0` (no observations yet).

### `contention_rate`

`contention_ops / ops_observed` as `f64`. `0.0` when
`ops_observed == 0`. Most policies threshold on this directly.

### `op_kind_total`

`op_kind_counts.iter().sum()`. Equals `ops_observed` for
primitives that always set a non-zero op_kind on their pushes.

### `ratio_of`

`ratio_of(kind, &[k1, k2, ...])` returns the fraction of the total
op-kind count (across the slice) that came from `kind`:

```rust,no_run
let read_share = stats.ratio_of(
    hash_map::OP_GET,
    &[hash_map::OP_INSERT, hash_map::OP_GET, hash_map::OP_REMOVE],
);
```

Returns `0.0` when the total across the slice is zero.

### `distinct_threads_for` / `is_multi_thread_for`

```rust,no_run
let producer_threads = stats.distinct_threads_for(ring::OP_PUSH);
let mpmc_push = stats.is_multi_thread_for(ring::OP_PUSH);
```

The distinct-thread cache is filled lazily as observations arrive:
each observation's `producer_thread_id` is compared against the
populated slots for its op_kind. On a miss, the tid is appended (up
to slot 4) and the count incremented. After 4 distinct tids the
count saturates at `MAX_TRACKED_THREADS_PER_KIND + 1 = 5`, meaning
"more threads than the cache can hold."

The `>= 2` test is the canonical multi-producer (or multi-consumer)
detection - returns `true` once the second distinct tid arrives,
without needing to track which threads they are.

## How it gets populated

The sidecar's `scan_instances` pops observations from each
instance's ring in batches. For each popped `Observation`:

```text
stats.ops_observed += 1
stats.total_latency_ticks += obs.latency_ticks
if obs.flags & 1 != 0:
    stats.contention_ops += 1
stats.op_kind_counts[obs.op_kind.min(N - 1)] += 1
record_thread_for_op(stats, obs.op_kind, obs.producer_thread_id)
```

`record_thread_for_op` is the linear-scan helper that updates the
distinct-tid cache.

After the drain loop, if `decide` returns `Some(new_tag)` and the
tag actually changes (`new_tag != current_tag`):

```text
instance.apply_migration(new_tag)
stats.migrations_triggered += 1
```

## See also

- [`Observation`](../subetha-core/observation.md) - the input
  record that gets folded.
- [`Policy`](policy.md) - reads `InstanceStats` and decides.
- [Reading sidecar observations tutorial](../../tutorial/reading-observations.md)
  for a worked example.
