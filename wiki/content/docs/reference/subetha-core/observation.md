---
weight: 30
---

# `ObservationRing` + `Observation`

The observation pipeline is how primitive op-streams reach the
sidecar. Each primitive op pushes one 24-byte `Observation` to a
thread-local 4096-slot SPSC ring; the sidecar's scan thread drains
the ring asynchronously into per-instance `InstanceStats`.

## `Observation` layout

24 bytes, three per cache line, no straddling:

```rust,no_run
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Observation {
    pub instance_id: u32,           // who emitted it
    pub op_kind: u16,                // primitive-specific (1..=7)
    pub flags: u16,                  // bit 0 contention, bit 1 empty/miss
    pub latency_ticks: u64,          // raw TSC ticks
    pub producer_thread_id: u32,     // auto-stamped if 0
    pub _reserved: u32,              // alignment padding
}

impl Observation {
    pub const ZERO: Self = /* all zeros */;
}
```

> [!NOTE]
> **`producer_thread_id` is process-local and lazy.** `thread_id()`
> returns the same value for every call from the current thread, is
> allocated on first call via an atomic counter (no syscalls), and
> is never `0` (that value is the "unspecified" sentinel so the
> default `Observation::ZERO` is distinguishable from a real
> producer). First thread to call gets id `1`.

## `ObservationRing` layout

64-byte aligned, SPSC, 4096 slots:

```rust,no_run
#[repr(C, align(64))]
pub struct ObservationRing {
    head: AtomicU32,      // cache line 0 - consumer writes, producer reads
    _pad0: [u8; 60],
    tail: AtomicU32,      // cache line 1 - producer writes, consumer reads
    _pad1: [u8; 60],
    buf: [UnsafeCell<Observation>; 4096],
}
```

> [!IMPORTANT]
> **Head and tail live on separate cache lines.** The consumer
> (sidecar drain thread) is the only writer of `head`; the
> producer (primitive op thread) is the only writer of `tail`.
> Splitting them prevents false-sharing between the two roles.

## Push (producer)

```rust,no_run
pub fn push(&self, mut obs: Observation) -> bool;
```

- Auto-stamps `producer_thread_id` if it is `0`.
- Loads `tail` relaxed (producer is the only writer).
- Loads `head` acquire (synchronises with the consumer's release
  on pop).
- Checks `(tail + 1) - head > capacity` → ring full, return `false`
  (observation dropped silently; sampling, not coordination).
- Writes the observation into `buf[tail % capacity]`.
- Stores `tail + 1` release (publishes to the consumer).

Push cost is ~3 cycles steady-state (~2.8 ns on Zen+). Producer
never blocks; full-ring observations are dropped.

## Pop (consumer)

```rust,no_run
pub fn pop(&self) -> Option<Observation>;
```

- Loads `head` relaxed (consumer is the only writer).
- Loads `tail` acquire (synchronises with the producer's release on
  push).
- Returns `None` if `head == tail`.
- Reads `buf[head % capacity]`, stores `head + 1` release.

Single-consumer: the caller must serialise pops. The sidecar's
scan thread is the only caller.

## `thread_id()`

```rust,no_run
pub fn thread_id() -> u32;
```

Process-local sequential thread id. Stable for the lifetime of the
thread; not valid across forks (the child keeps the parent's
counter but reissues new ids to its own threads). First thread to
call gets `1`.

## Test invariants

The unit tests in `crates/subetha-core/src/observation.rs` assert:

- Push/pop round-trip preserves all fields and auto-stamps
  `producer_thread_id` when the caller passes `0`.
- Ring fills exactly at capacity (4096 pushes succeed, the 4097th
  returns `false`).
- `thread_id()` is stable across calls from the same thread.
- `thread_id()` is distinct across threads.
- `thread_id()` never returns `0`.
- `push` auto-stamps thread id when the caller passes `0`.

## See also

- [`InstanceStats`](../subetha-sidecar/instance-stats.md) - what the
  sidecar accumulates from drained observations.
- [Sidecar observation pipeline](../../explanation/observation-pipeline.md) -
  the end-to-end flow from op push through scan-thread drain to
  policy decision.
