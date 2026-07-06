# EventStateLog&lt;Event, State&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Pattern](https://img.shields.io/badge/CQRS-event--sourced-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Persistence](https://img.shields.io/badge/durable-ring_file_is_log-informational)

Event-sourced state primitive with a materialized current-state
view. Composes [`SharedRing`](./SHARED_RING.md) (the durable
event log) with [`SharedCell`](./SHARED_CELL.md) (the
materialized state). Producers `emit` events onto the ring;
consumers `drain_and_fold` to advance the materialized view; any
process can `read_current` at constant cost for the latest
snapshot.

> **The "Kafka + materialized view at lock-free MMF cost"
> primitive.** `read_current` at 1.28 ns vs `Mutex<State>` at
> 16.82 ns (**13.2x faster** - pure SeqLock optimistic read, no
> kernel sync). `emit` and full cycle are within 7% of
> `Mutex<VecDeque>` baselines, so the architectural lever isn't
> raw speed - it's cross-process visibility plus disk persistence
> (the ring file IS the durable event log).

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`Event: Copy + 'static`, fixed payload** sized to
  `SharedRing::PAYLOAD_BYTES` (56 bytes).
- **`State: Copy + 'static`, fixed payload** sized to
  `SharedCell::PAYLOAD_BYTES` (52 bytes).
- **Two files per log**: `<base>.events.bin` (the ring) +
  `<base>.state.bin` (the cell).
- **Producers and consumers compose freely**: any process /
  thread `emit`s; any process / thread `drain_and_fold`s.
  Multiple folders may race; the ring's MPMC protocol serializes.
- **`drain_and_fold` is single-fold-call**: the closure folds
  every drained event into a local State copy, then the cell is
  updated once via SeqLock. Readers see all-or-nothing for that
  batch.
- **`pending_events` is approximate**: the ring's `approx_len`
  is best-effort; do not rely on it for correctness.

---

## Table of contents

- [What it is](#what-it-is)
- [Protocol](#protocol)
- [Bench evidence](#bench-evidence)
- [Worked examples](#worked-examples)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

```text
+----------------------+    +----------------------+
| <base>.events.bin    |    | <base>.state.bin     |
| SharedRing           |    | SharedCell<State>    |
|   slots[capacity]    |    |   SeqLock + 52B      |
|   Vyukov MPMC        |    |   payload            |
+----------------------+    +----------------------+
        ^                            ^
        | emit(event)                | set(state) after fold
        |                            |
        |                            | read_current() (hot path)
        |       drain_and_fold       |
        +-----------+----------------+
                    |
              all process /
              thread handles
```

`emit` does one ring `try_push`; `drain_and_fold` pops events
until empty, folds each into a local State, then writes the
updated State back into the cell with one SeqLock write.
`read_current` is one SeqLock read of the cell.

---

## Protocol

### emit(event)

1. Serialize `event` into the ring's 48-byte payload via
   `ptr::copy_nonoverlapping`.
2. `ring.try_push(&bytes)` - the Vyukov MPMC sequence-number
   protocol either succeeds, returns `Full`, or retries on
   contention.

### drain_and_fold(fold)

1. `state = self.state.get()` - SeqLock read.
2. Loop: `ring.try_pop(&mut buf)` until `Empty`. For each
   popped event:
   - `ptr::read_unaligned` to materialize the `Event`.
   - `fold(&mut state, &event)` to advance the local State.
3. If any events folded: `self.state.set(state)` - one SeqLock
   write to publish.

The single SeqLock write at the end is the consistency
boundary: readers either see the pre-batch state or the
post-batch state, never an intermediate.

### read_current()

1. `self.state.get()` - one SeqLock read. Retries internally on
   torn read (writer mid-update).

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/event_state_log.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

Workload: `Event = u32`, `State = u64`, `fold = |s, e| s += e as u64`.
Naive baseline uses `Mutex<VecDeque<Event>>` for the log and
`Mutex<State>` for the materialized view.

| Op | `EventStateLog` (mmf) | naive `Mutex<VecDeque>` + `Mutex<State>` | Relative |
|---|---:|---:|---:|
| emit single | 19.37 ns | 18.58 ns | 1.04x slower (tied) |
| read_current | **1.28 ns** | 16.82 ns | **13.2x faster** |
| emit 64 + drain_and_fold 64 | 1.24 µs | 1.16 µs | 1.07x slower (tied) |

### Reading the trade-offs

The story the numbers tell:

1. **Read is 13.2x faster.** `read_current` is one SeqLock
   optimistic read - no kernel sync, no lock acquire. The
   mutex baseline pays a full lock + load + unlock for every
   read. Read-heavy materialized-view workloads benefit
   directly.
2. **Emit and full cycle are essentially tied.** The Vyukov
   MPMC ring is competitive with `Mutex<VecDeque>` per-op on a
   single thread. Under contention the ring scales better
   (lock-free CAS vs serialized mutex), but the microbench is
   single-threaded.
3. **The mutex baseline cannot do what EventStateLog does.**
   Cross-process visibility is unavailable to
   `Mutex<VecDeque>` at any cost; disk persistence requires
   custom serialization on top of the Mutex. EventStateLog
   gets both for free from its MMF substrate.

### Rule 3b bench audit

- **Fair contender**: `Mutex<VecDeque<Event>>` is the textbook
  in-process event-log baseline; `Mutex<State>` is the textbook
  in-process materialized-state baseline. Together they form
  the naive in-process event-sourcing shape EventStateLog
  generalizes.
- **No `thread::spawn` inside `b.iter`**: workload is
  single-threaded; concurrent-correctness is covered by the
  source-level unit test `concurrent_producers_drain_correctly`.
- **MMF lifecycle managed**: events + state files created,
  ops run, dropped, both files removed per bench.

### What the numbers do NOT show

- **Cross-process emit + drain**: producer in one process emits;
  consumer in another process drains. The naive baseline cannot
  do this at any cost.
- **Disk-backed recovery**: after `flush`, the ring file is the
  authoritative event log on disk; re-opening the file recovers
  the un-drained events.
- **Multi-producer scaling**: the ring's MPMC protocol absorbs
  concurrent producers without a global lock; mutex baselines
  serialize.

---

## Worked examples

### Cross-process event-sourced counter

```rust
use subetha_cxc::EventStateLog;

// Process A - producer:
let log: EventStateLog<i32, i64> = EventStateLog::create("/tmp/counter", 1024, 0).unwrap();
log.emit(1).unwrap();
log.emit(2).unwrap();
log.emit(-1).unwrap();

// Process B - consumer + reader:
let log: EventStateLog<i32, i64> = EventStateLog::open("/tmp/counter", 1024).unwrap();
log.drain_and_fold(|s, e| *s += *e as i64);  // folds: 0 + 1 + 2 + (-1) = 2
assert_eq!(log.read_current(), 2);

// Process C - read-only observer:
let log: EventStateLog<i32, i64> = EventStateLog::open("/tmp/counter", 1024).unwrap();
let snapshot = log.read_current();  // 1.28 ns hot path
```

### Struct event with struct state

```rust
use subetha_cxc::EventStateLog;

#[derive(Clone, Copy)]
#[repr(C)]
struct Move { delta_x: i32, delta_y: i32 }

#[derive(Clone, Copy)]
#[repr(C)]
struct Position { x: i32, y: i32 }

let log: EventStateLog<Move, Position> =
    EventStateLog::create("/tmp/pos", 256, Position { x: 0, y: 0 }).unwrap();
log.emit(Move { delta_x: 3, delta_y: 4 }).unwrap();
log.emit(Move { delta_x: -1, delta_y: 2 }).unwrap();
log.drain_and_fold(|p, m| { p.x += m.delta_x; p.y += m.delta_y; });
// read_current == Position { x: 2, y: 6 }
```

### Checkpoint restore

```rust
use subetha_cxc::EventStateLog;

let log: EventStateLog<u32, u64> = EventStateLog::open("/tmp/log", 1024).unwrap();
// Fast-forward state from a known good snapshot.
log.set_state(42_000_000);
// Subsequent emits + folds advance from there.
```

---

## Use case patterns

### Pattern: cross-process audit log + cached state

Producers emit auditable events; a consumer materializes the
current state for hot reads. Any process can recover the full
event history by reading the ring file; any process gets the
latest state with one SeqLock read.

### Pattern: event-sourced cache invalidation

`Event = CacheInvalidation`, `State = u64 epoch`. Producers
emit invalidation events; consumers fold them into a monotonic
epoch counter that downstream caches check.

### Pattern: command queue with materialized status

`Event = Command`, `State = CommandStatus`. Workers emit
commands; a controller drains, executes, and updates the
materialized status. Observers poll `read_current` for the
latest status.

---

## Known limitations

- **Event size capped at 56 bytes**: same as
  `SharedRing::PAYLOAD_BYTES`. Larger payloads need pointer
  indirection (an offset into a separate arena).
- **State size capped at 52 bytes**: same as
  `SharedCell::PAYLOAD_BYTES`. Larger state needs the bigger-
  payload cell variants.
- **No automatic compaction**: the ring is bounded; if
  consumers fall behind, `emit` returns `Ring(Full)`. Either
  apply backpressure or drain more eagerly.
- **`pending_events` is approximate**: the ring's MPMC
  protocol does not maintain a precise count. Do not gate
  correctness on this value.
- **Single fold per drain call**: `drain_and_fold` folds every
  event into one local State copy, then writes once. Readers
  during a drain see either pre-batch or post-batch state.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Treating `pending_events` as authoritative.** It's the
  ring's `approx_len`; do not size buffers or drive correctness
  off it.

- **Forgetting to drain.** Producers fill the ring; once
  `emit` returns `Ring(Full)`, no more events are accepted.
  Either drain eagerly or apply backpressure at the producer.

- **Using a non-`Copy` Event or State.** Events and states
  are bitwise-copied into / out of the ring and cell. Pointer
  fields, `Vec`, or any type with a destructor must be
  serialized to plain bytes by the caller.

- **Mismatched ring capacity at open.** `open` requires the
  same capacity as `create`. Pin capacity in a shared spec.

- **Wrapping the log in a Mutex.** Pointless; the ring's MPMC
  protocol and the cell's SeqLock are already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/event_state_log.rs` (375 lines, 9
  unit tests covering emit/drain/fold, cross-handle visibility,
  full ring backpressure, concurrent producers, disk
  persistence, set_state checkpoint restore, and struct events).
- Bench: `crates/subetha-cxc/benches/event_state_log.rs` (emit,
  read_current, 64-event cycle vs `Mutex<VecDeque>` +
  `Mutex<State>`).
- Underlying primitive: [SHARED_RING.md](./SHARED_RING.md) -
  Vyukov MPMC bounded ring; the durable event log.
- Underlying primitive: [SHARED_CELL.md](./SHARED_CELL.md) -
  per-slot SeqLock cell; the materialized state.
- Architectural pattern reference: Kafka + materialized views,
  EventStore + projections, Akka Persistence - all the CQRS
  shape, lifted to lock-free MMF.
