---
weight: 40
---

# Reading sidecar observations

The [cross-process round-trip](cross-process-roundtrip.md) used a bare
`SharedHashMap`. To have the sidecar *observe* a primitive - drain its
op stream into `InstanceStats` - you register it by wrapping it in a
`SidecarBox`. This chapter wraps a map, drives some ops, and shows what
the sidecar then saw, in detail.

## The Observation record

Each primitive op pushes one `Observation` to its thread-local ring.
The struct is 24 bytes, three per cache line, no straddling:

```rust,no_run
#[repr(C)]
pub struct Observation {
    pub instance_id: u32,         // who emitted it
    pub op_kind: u16,              // primitive-specific (1, 2, 3, ...)
    pub flags: u16,                // bit 0 contention, bit 1 empty/miss
    pub latency_ticks: u64,        // raw TSC ticks
    pub producer_thread_id: u32,   // auto-stamped if 0
    pub _reserved: u32,
}
```

> [!NOTE]
> **op_kind is per-primitive.** Each primitive defines its own
> `op_kind` constants. `SharedRing` uses `ring::OP_PUSH = 1` and
> `ring::OP_POP = 2`; `SharedHashMap` uses `hash_map::OP_INSERT = 1`,
> `OP_GET = 2`, `OP_REMOVE = 3`, `OP_CONTAINS = 4`, `OP_CLEAR = 5`,
> `OP_COMPACT = 6`. See `subetha_cxc::sidecar_ops` for the full
> enumeration.

## Inspecting what the sidecar accumulated

`InstanceStats` is a snapshot of the drain-and-fold accumulator:

```rust,no_run
use subetha_cxc::SharedHashMap;
use subetha_sidecar::{global, SidecarBox};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SidecarBox::new registers the primitive with the global sidecar.
    // A bare SharedHashMap is not observed; the wrapper is what plugs
    // it into the observation pipeline.
    let m = SidecarBox::new(
        SharedHashMap::<u32, u64>::create("/tmp/observed.bin", 1024)?,
    );

    for i in 0..1_000u32 {
        m.insert(i, i as u64)?;   // pushes an OP_INSERT observation
        let _ = m.get(&i);        // pushes an OP_GET observation
    }
    // Force one synchronous scan so the ring drains into stats
    // (the 200 us poll would otherwise pick it up on its own).
    global().scan_now();

    let s = m.stats().unwrap();
    println!("ops_observed          = {}", s.ops_observed);
    println!("contention_rate       = {:.3}", s.contention_rate());
    println!("op_kind_counts        = {:?}", &s.op_kind_counts[..]);
    println!("average_latency_ticks = {}", s.average_latency_ticks());
    println!("total_latency_ticks   = {}", s.total_latency_ticks);
    println!("migrations_triggered  = {}", s.migrations_triggered);
    Ok(())
}
```

> [!TIP]
> **`contention_rate()` is the fraction of ops that reported
> `flags & 1 != 0`.** Primitives set this bit when they took the
> slow path (e.g. a `Mutex` `lock()` that had to park, a CAS that
> lost the race, a SeqLock reader that retried). A workload-driven
> `Policy` reads this number and decides whether to migrate.

## The op_kind histogram

`op_kind_counts` is a fixed-size array indexed by the `op_kind`
field. The sidecar caps it at `N_OP_KINDS = 8`, so each primitive
gets seven distinct op_kinds (`1..=7`; index `0` is reserved for
"unspecified"). The histogram is what a `Policy` consults to
distinguish *insert-heavy* from *get-heavy* workloads on the same
hash map.

For `SharedHashMap`:

| Index | op_kind | What it counts |
|---|---|---|
| 0 | (reserved) | observations with op_kind = 0 |
| 1 | `OP_INSERT` | `insert()` calls (flag bit 0 set when `Err(Full)`) |
| 2 | `OP_GET` | `get()` calls (flag bit 1 set on `None`) |
| 3 | `OP_REMOVE` | `remove()` calls (flag bit 1 set on `None`) |
| 4 | `OP_CONTAINS` | `contains_key()` calls |
| 5 | `OP_CLEAR` | `clear()` calls |
| 6 | `OP_COMPACT` | `compact()` calls |
| 7 | unused | spare slot if the primitive gains a seventh op_kind |

## The latency aggregates

`InstanceStats` keeps two latency fields: `total_latency_ticks` (the
running sum across every drained observation) and `ops_observed`
(the divisor). The convenience accessor `average_latency_ticks()`
divides them, returning `0` when `ops_observed == 0`. There is no
percentile state - the sidecar folds latency into a sum-and-count
pair so the per-instance stats footprint stays at the fixed-size
struct, never growing with op-count.

> [!IMPORTANT]
> **`latency_ticks` is raw TSC**, not nanoseconds. Convert with the
> host TSC frequency if you want wall-clock numbers - on Zen+ this
> is ~3.4 GHz, so 1 ns ≈ 3.4 ticks. Most policies work in ticks
> directly because they compare ratios, not absolutes.

> [!TIP]
> **Want percentile latency?** Track it primitive-side. The
> observation push site has the raw `latency_ticks` value in scope;
> a primitive that needs p50/p99 can keep a small `t-digest` or
> reservoir in its own state and expose it via a primitive-specific
> accessor. The sidecar's stats struct stays minimal on purpose.

## What to do next

That closes the tutorial. From here, the
[How-To Guides](../how-to/) cover task-shaped questions and the
[Reference](../reference/) pages document every public type. The
[architecture overview](../explanation/architecture.md) is the
right next read if you want to understand the substrate / sidecar
/ primitives split before reaching for the spec.
