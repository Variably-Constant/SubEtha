---
weight: 20
---

# `Policy` trait + built-in policies

The decision function the sidecar consults on each scan iteration.

```rust,no_run
pub trait Policy: Send + Sync + 'static {
    fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32>;
}
```

`Some(new_tag)` triggers `apply_migration(new_tag)` on the instance.
`None` leaves the strategy alone.

> [!NOTE]
> **`decide` is called only after at least one new op was observed
> since the previous scan.** An idle instance with no traffic does
> not trigger policy calls; the scan still drains the ring (it will
> be empty) but skips the `decide` invocation.

## Built-in policies

### `NoMigrationPolicy`

```rust,no_run
pub struct NoMigrationPolicy;

impl Policy for NoMigrationPolicy {
    fn decide(&self, _stats: &InstanceStats, _current_tag: u32) -> Option<u32> {
        None
    }
}
```

The default for primitives whose strategy is fixed at construction.
Every primitive in `subetha-cxc` (cross-process MMF family) defaults to
`NoMigrationPolicy` because their strategy is the MMF byte layout,
which is not migrable in place.

### `FixedPolicy(pub u32)`

```rust,no_run
pub struct FixedPolicy(pub u32);

impl Policy for FixedPolicy {
    fn decide(&self, _stats: &InstanceStats, _current_tag: u32) -> Option<u32> {
        Some(self.0)
    }
}
```

Always returns the same tag. Useful for testing the migration
path - construct an instance with the default strategy, attach a
`FixedPolicy(target_tag)`, and on the next scan the sidecar
migrates to `target_tag`. Will not migrate again on subsequent
scans because the sidecar inspects `current_tag == decided_tag`
before calling `apply_migration`.

## Writing a custom policy

The pattern is to compute one or more signals from `InstanceStats`
and return a `Some(tag)` when the signal crosses a threshold.

### Contention-driven migration

```rust,no_run
struct ContentionPolicy {
    threshold: f64,
    contended_tag: u32,
    uncontended_tag: u32,
}

impl Policy for ContentionPolicy {
    fn decide(&self, stats: &InstanceStats, current_tag: u32) -> Option<u32> {
        // Need a meaningful sample size before deciding anything.
        if stats.ops_observed < 1_000 {
            return None;
        }
        let want = if stats.contention_rate() > self.threshold {
            self.contended_tag
        } else {
            self.uncontended_tag
        };
        if want == current_tag { None } else { Some(want) }
    }
}
```

### Multi-producer detection

The sidecar tracks distinct producer thread IDs per op-kind in
`InstanceStats.per_op_kind_distinct_threads` (capped at
`MAX_TRACKED_THREADS_PER_KIND = 4`). The convenience accessors:

```rust,no_run
let push_threads = stats.distinct_threads_for(ring::OP_PUSH);
let pop_threads = stats.distinct_threads_for(ring::OP_POP);
let is_mpmc = push_threads >= 2 && pop_threads >= 2;
let is_spsc = push_threads == 1 && pop_threads == 1;
```

A `ChannelPolicy` consults these to promote SPSC → MPMC when it
sees the second producer thread arrive on the push side.

### Op-mix ratio

When the policy needs to balance two op kinds (e.g., reads vs
writes):

```rust,no_run
let read_fraction = stats.ratio_of(
    hash_map::OP_GET,
    &[hash_map::OP_INSERT, hash_map::OP_GET, hash_map::OP_REMOVE],
);
if read_fraction > 0.95 {
    Some(STRATEGY_READ_OPTIMISED)
} else {
    None
}
```

`ratio_of(numerator_kind, &[all_kinds_in_total])` returns 0.0 when
the total is zero (no observations yet). Combine with an
`ops_observed` threshold to suppress decisions on small samples.

## Avoiding migration thrash

Three patterns a custom policy should use to suppress oscillation:

1. **Minimum sample size**: do not decide until `ops_observed`
   crosses a threshold (e.g., 1000 ops).
2. **Hysteresis band**: migrate Mutex → ArcSwap at contention > 0.20,
   migrate back only at contention < 0.05. The gap prevents a
   workload sitting near the threshold from flipping repeatedly.
3. **Same-tag short-circuit**: return `None` when the candidate
   tag equals `current_tag` so the sidecar does not bump generations
   on no-op migrations.

## See also

- [`AdaptiveInstance`](adaptive-instance.md) - `make_policy()`
  returns the `Box<dyn Policy>` the sidecar holds.
- [`InstanceStats`](instance-stats.md) - the input every policy
  reads.
- [Custom policy how-to](../../how-to/custom-policy.md) - worked
  example of writing one end-to-end.
