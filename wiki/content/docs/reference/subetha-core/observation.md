---
weight: 30
---

# `ObservationRing` + `Observation`

The observation pipeline is how primitive op-streams reach the
sidecar. Each primitive instance owns one 4096-slot ring. Every
thread that operates on the instance pushes 24-byte `Observation`
records into it, and the sidecar's scan thread pops them into the
instance's `InstanceStats`.

## `Observation` layout

24 bytes, 8-byte aligned:

```rust,no_run
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Observation {
    pub instance_id: u32,           // who emitted it
    pub op_kind: u16,                // primitive-specific (1..=7)
    pub flags: u16,                  // bit 0 contention, bit 1 empty/miss
    pub latency_ticks: u64,          // ticks, as the pusher measured them
    pub producer_thread_id: u32,     // auto-stamped if 0
    pub _reserved: u32,              // names the alignment padding; popped as 0
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

64-byte aligned, multi-producer and single-consumer, 4096 slots:

```rust,no_run
#[repr(C, align(64))]
pub struct ObservationRing {
    head: AtomicU32,        // cache line 0 - the consumer's next position
    _pad0: [u8; 60],
    tail: AtomicU32,        // cache line 1 - the next position a producer claims
    armed: AtomicBool,      // set when a sidecar registers the instance
    _pad_a: [u8; 3],
    buf: AtomicPtr<Slot>,   // 4096 slots, allocated when the ring is armed
    _pad1: [u8; 48],
}
```

Each slot holds one record's fields in atomics and a sequence number,
in 24 bytes, so an armed ring's buffer is 96 KiB. A ring that is
never armed never allocates it.

> [!IMPORTANT]
> **Head and tail live on separate cache lines.** Only the consumer
> (the sidecar's scan) moves `head`; producers claim positions on
> `tail` and never read `head`. The gate a producer checks and the
> buffer pointer it follows share `tail`'s line.

## Push (producer)

```rust,no_run
pub fn push(&self, obs: Observation) -> bool;
pub fn push_op(&self, op_kind: u16, flags: u16) -> bool;
```

- While no ring in the process is armed, a push is one relaxed load
  of the process-global armed count and a branch, and returns
  `false`. The rest of the push is out of line behind `#[cold]`.
- A ring that is not armed returns `false`.
- Auto-stamps `producer_thread_id` if it is `0`.
- Reads the slot at `tail`. Its sequence equal to the position means
  the slot is free: the producer claims the position with a
  compare-and-swap on `tail`, writes the fields, and publishes them
  with a Release store of `position + 1` into the slot's sequence.
- A sequence behind the position means the slot still holds the
  record from one lap back: the ring is full, and the push returns
  `false` (observation dropped; sampling, not coordination).
- A sequence ahead of the position means another producer claimed it
  first; the push reads `tail` again.

Producers never block, and two producers never write one slot.

## Pop (consumer)

```rust,no_run
pub fn pop(&self) -> Option<Observation>;
```

- Reads the slot at `head` with an Acquire load of its sequence.
- Returns `None` unless the sequence is `head + 1`, which includes a
  slot a producer has claimed and is still writing.
- Reads the fields, stores `head + 4096` into the slot's sequence to
  free it for the next lap, and advances `head`.

Single-consumer: the caller must serialize pops. The sidecar takes a
per-node lock for each scan, so its scan thread and `scan_now` take
turns.

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
- A ring drops every push until it is armed.
- An explicit `producer_thread_id` is kept.
- `thread_id()` is stable across calls from the same thread,
  distinct across threads, and never `0`.
- Eight threads pushing into one armed ring at once, over 64 rounds
  that each fill it: every push that returns `true` is popped
  exactly once, whole.

## See also

- [`InstanceStats`](../subetha-sidecar/instance-stats.md) - what the
  sidecar accumulates from drained observations.
- [Sidecar observation pipeline](../../explanation/observation-pipeline.md) -
  the end-to-end flow from op push through scan-thread drain to
  policy decision.
