---
title: "Deque Dispatcher"
weight: 80
---

# DequeDispatcher

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-Composition-green)
![Routing](https://img.shields.io/badge/routing-shape--driven-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

`DequeDispatcher` owns one handle of each
[`SharedDeque`](shared-deque/) (Chase-Lev) /
[`SharedDequeKhpd`](shared-deque-khpd/) /
[`SharedDequeLoh`](shared-deque-loh/) /
[`SharedDequeUrd`](shared-deque-urd/) /
[`SharedDequeKhl`](shared-deque-khl/) variant the host has configured,
and routes per call based on a caller-supplied `WorkloadShape`. No
single variant wins every shape; the dispatcher picks the right one
per call.

> **The "per-shape routing" primitive.** Sits one layer above the
> five MMF-deque variants. Callers express the workload's shape
> (number of thieves, batch size, idle-wait hint) and the dispatcher
> picks the variant that minimizes per-item cost for that shape.

**Routing rules (`DequeDispatcher::pick`, SubEtha-empirical):**

| Workload | Pick | Why |
|---|---|---|
| Multiple thieves (`n_thieves >= 2`) | `Urd` | Per-thief mailbox = zero CAS contention on the steal site. |
| Single thief AND `wait_idle = true` | `Urd` | Hardware-mediated wake via `UMWAIT` on WAITPKG-capable silicon. |
| Producer batches K >= 2 items per call, single thief | `Khl` | The three-lever hybrid: KHPD's 3-items-per-Release-store, LOH's per-batch counter amortization, AND Chase-Lev's owner-private tail. Measured 1.55x KHPD at K=64. |
| Per-item dispatch, single thief | `ChaseLev` | Lowest constant per push; no batch to amortize. |

KHPD and LOH stay available as explicitly-configured backends (and
as fallbacks), but the shape router sends every batched
single-thief call through KHL. `pick_by_signature` derives the
same answers from the variants' K-axis signature sets, and
`pick_with_fallback` walks `primary -> KHL -> KHPD -> LOH ->
Chase-Lev -> URD`, returning the first variant configured on this
dispatcher (`None` only when none are). `is_configured(variant)` and
the per-variant getters (`chase_lev()` / `khpd()` / `loh()` / `urd()`
/ `khl()` returning `Option<&Arc<..>>`) expose the configured set.

## Cross-process end-to-end

The dispatcher lives in the producer process. Each variant it owns
is backed by its own MMF file path; consumer processes open those
same paths via the variant's `open_as_thief` / `open` constructors.

The
[`dispatcher_demo`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/dispatcher_demo.rs)
example demonstrates the full parent / child pattern: the parent
dispatches 30 per-item jobs (Chase-Lev route) plus a 60-item batch
(KHPD route) through one dispatcher; the child opens both MMF deques
as a thief and drains them in parallel threads, summing the ids it
sees per backend and exiting 0 if the sums match.

Run:

```bash
cargo run --release --example dispatcher_demo -p subetha-cxc
```

Output (Zen+ R7 2700, Win11):

```text
[parent] dispatched 30 request-reply jobs in 24.3us (0.80 us/job)
[parent] dispatched 60-item batch in 4us (66.67 ns/job)
[child] drained cl=30 (435) + khl=60 (3570) = 90 items, sum 4005 (expected 4005)
[parent] child drained 90 items bit-exact through dispatcher (Chase-Lev + KHL routes confirmed)
```

The batched route is **12x faster per-item** than the
request-reply route end-to-end in the captured run. The
dispatcher's per-call routing + delegation overhead (the cost of
`pick_with_fallback` + the variant match) is observable in the
per-job cost; on this host the delegation overhead is sub-50 ns.

## API surface

```rust
use subetha_cxc::{DequeDispatcher, DequeVariant, LineItem, WorkloadShape};

// Build a dispatcher with Chase-Lev + KHL configured.
let dispatcher = DequeDispatcher::builder()
    .with_chase_lev("/tmp/cl.bin", 1024)?
    .with_khl("/tmp/khl.bin", 256)?
    .build();

// Dispatch one item under a request-reply shape.
let item = LineItem::new(&42u32.to_le_bytes())?;
let variant = dispatcher.dispatch_one(WorkloadShape::request_reply(), item)?;
assert_eq!(variant, DequeVariant::ChaseLev);

// Dispatch a 60-item batch under a producer-fast shape.
let batch: Vec<LineItem> = (0..60u32)
    .map(|id| LineItem::new(&id.to_le_bytes()).unwrap())
    .collect();
let variant = dispatcher.dispatch_batch(
    WorkloadShape::producer_fast(60),
    &batch,
)?;
assert_eq!(variant, DequeVariant::Khl);

// Or just inspect the routing decision without dispatching.
let picked = DequeDispatcher::pick(WorkloadShape::fan_out(4, 64));
assert_eq!(picked, DequeVariant::Urd);
```

## See also

- [`SharedDeque`](shared-deque/), [`SharedDequeKhpd`](shared-deque-khpd/),
  [`SharedDequeLoh`](shared-deque-loh/),
  [`SharedDequeUrd`](shared-deque-urd/),
  [`SharedDequeKhl`](shared-deque-khl/) - the five backing primitives.
- [Citations and references](../../../explanation/citations/) - the
  Chase-Lev / LCRQ / WAITPKG primary sources behind each variant.
