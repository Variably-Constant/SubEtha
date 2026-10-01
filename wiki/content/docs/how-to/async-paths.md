---
title: "Async: cost and scaling"
weight: 20
---

# Choosing sync, blocking, or async - and what async costs

A [`Channel` / `AdaptiveIpc`](../reference/subetha-cxc/high-level-api.md)
handle answers all three
calling conventions; the choice is per call site, not baked into the
type. This guide covers when to reach for each, and the measured cost
of the async path so the choice is informed.

## One handle, three conventions

| Convention | Call | Blocks what | Reach for it when |
|---|---|---|---|
| Sync | `send` / `recv` | nothing (returns `Full` / `Empty`) | you have your own loop or poll cadence and want the floor. |
| Blocking | `send_blocking` / `recv_blocking` | the calling thread (parks it) | one dedicated thread per endpoint, and you want it asleep when idle. |
| Async | `send_async` / `recv_async` | the task (suspends it) | many endpoints multiplexed onto few threads. |

Async runs on any executor: tokio, smol, async-std, or the crate's
runtime-free [`block_on`](../reference/subetha-cxc/async-engine.md). The
wake crosses a
process boundary (a per-receiver reactor) and a machine boundary
(`net_bridge` over blocking `std::net`) behind the same `.await`.

The bindings keep the first two conventions and take the third their
own way:

| Convention | Python | PowerShell |
|---|---|---|
| Sync | `send` / `recv`, `recv` answering `None` when empty | `Send` / `Recv`, `Recv` answering `$null` when empty |
| Blocking | `send_for` / `recv_for`, waiting up to a timeout | `SendFor` / `RecvFor`, waiting up to a timeout in seconds |
| Async | `await aio.recv(channel, timeout=5)` from `subetha.aio`, which waits without blocking asyncio's loop | none; runspaces or thread jobs carry the concurrency |

Rust's executor is not bound, because every entry point to it takes or
returns a Rust future; the
[Python reference](../reference/subetha-py/) says which calls
`subetha.aio` can await soundly.

## Async is the scaling path, not the latency path

Async on this substrate is not a faster single operation. The sync
`recv` skips all `Waker` machinery (one relaxed load on an internal
gate); the async `recv` constructs a future and, once async is engaged,
every op drives the wake machinery the sync path avoids. The cost is
real and measurable.

### Per-op overhead

A single-threaded round-trip on one `Channel<u64>`, item always
available (the fast path, nothing parks), 8-byte payload. Measured on
an AMD Ryzen 9 7900X under Windows 11 Pro 10.0.26200, built for the
x86-64 baseline, with Criterion's defaults, while other work kept 3.7
to 4.6 of its 24 hardware threads busy; reproduce with `cargo bench
--bench async_overhead -p subetha-cxc`.

| Convention | Round-trip | vs sync |
|---|---|---|
| `send` / `recv` | ~4.5 ns | 1.0x |
| `send_blocking` / `recv_blocking` | ~15.2 ns | ~3.4x |
| `send_async` / `recv_async` (on `block_on`) | ~124.5 ns | ~27x |

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="/images/async_overhead-dark.png">
  <img alt="Per-op round-trip latency on a Ryzen 9 7900X: sync ~4.5 ns, blocking ~15 ns, async ~125 ns" src="/images/async_overhead-light.png">
</picture>

The async path is an order of magnitude heavier per op. If you are
optimizing a single hot producer-consumer pair for latency, use sync.

### What async buys: fan-out on a fixed thread count

The payoff is structural. An awaiting consumer is a suspended task, not
a parked thread, so one bounded executor drives an unbounded number of
them. Delivering the same item stream two ways - a `TaskPool` of
`available_parallelism` workers vs one OS thread per consumer running
`block_on` - over the same `WakerRing` primitive. Measured on an AMD
Ryzen 9 7900X under Windows 11 Pro 10.0.26200; reproduce with `cargo
bench --bench async_fanout -p subetha-cxc`.

```mermaid
flowchart TB
  subgraph FP["Fixed pool - async tasks"]
    T["N suspended tasks"]
    POOL["TaskPool<br/>available_parallelism workers"]
    T -. woken by a push .-> POOL
  end
  subgraph TPC["One thread per consumer"]
    C["N consumers"]
    TH["N OS threads"]
    C --> TH
  end
  classDef good fill:#0f766e,color:#fff
  classDef heavy fill:#9a3412,color:#fff
  class POOL good
  class TH heavy
```

| Driver | N = 1,000 | N = 10,000 | N = 100,000 | OS threads |
|---|---|---|---|---|
| Fixed pool (async tasks) | 10.45 M items/s (0.75-15.62) | 13.65 M items/s (3.11-17.41) | 14.27 M items/s (12.63-16.65) | 28 (constant) |
| Thread per consumer | 2.20 M items/s (1.73-2.45) | 1.60 M items/s (1.52-1.89) | (needs 100,004 threads) | N + 4 |

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="/images/async_scaling-dark.png">
  <img alt="Fan-out throughput, medians of five runs: fixed pool 10-14 M items/s on 28 threads from N=1k to N=100k; thread-per-consumer 1.6-2.2 M items/s on one thread per consumer" src="/images/async_scaling-light.png">
</picture>

Each cell is the median of five runs, with the range in parentheses.
The fixed pool's medians hold 10-14 M items/s on a constant 28 OS
threads from a thousand consumers to a hundred thousand. That 28 is
`available_parallelism` workers (24 on the Ryzen 9 7900X's 12 cores and
24 threads) plus the bench's 4 producer threads, so it tracks the
machine rather than being a tuned constant. The thread-per-consumer
design sits at 1.6-2.2 M items/s and needs one OS thread per consumer -
10,004 threads at N = 10,000, and 100,004 at N = 100,000, which is the
point at which it stops being practical; the bench does not run that
last cell for the same reason. Same ring, same `recv()` future; only the
driver differs.

The pool puts a worker on every hardware thread, so it spreads widest
when the machine is busy: two of the five runs started while other
work kept 10.5 and 18.9 of the 24 hardware threads busy, and they hold
each fixed-pool row's lowest reading; the other three started at 3.1
to 5.5. The gap between the two drivers is the durable result, not
the digits.

## Rule of thumb

- One hot pair, latency-sensitive: **sync**, in your own loop.
- A handful of long-lived endpoints, each on its own thread: **blocking**.
- Hundreds to hundreds of thousands of endpoints, or composing under an
  existing async app: **async**, on a `TaskPool` / `RingExecutor` or
  your runtime.

## See also

- [High-level API](../reference/subetha-cxc/high-level-api.md): the three
  conventions
  on one handle.
- [Async engine](../reference/subetha-cxc/async-engine.md):
  `block_on`, the reactor bridge, the executors, and `WakerRing`.
