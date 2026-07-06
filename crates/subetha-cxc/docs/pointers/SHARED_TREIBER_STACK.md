# SharedTreiberStack&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/Treiber_stack-ABA_safe-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process lock-free stack. Treiber-protocol head pointer
packed with a counter to defeat ABA. Push and pop are
single-CAS-with-retry on the head. No RAII guards, no spin
loops beyond CAS-retry, no underflow.

> **The "lock-free stack for cross-process work-stealing"
> primitive.** push at **33.34 ns** vs `Mutex<Vec>` 25.85 ns
> (mmf 1.29x slower; the mutex's contiguous Vec push is very
> fast). push_pop_cycle at **34.25 ns** vs 35.95 ns (tied).
> Architectural lever: cross-process visibility + ABA-safe
> CAS protocol that the Mutex baseline cannot offer.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Capacity fixed at create**.
- **Treiber stack with counter-packed head**: ABA-safe. Push
  + pop are single CAS on `(counter, head_index)` packed in
  one AtomicU64.
- **Bounded retry**: CAS retries are bounded by contention;
  no unbounded spin.
- **No RAII guards**: push consumes T; pop returns Option<T>.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | `SharedTreiberStack<u64>` (mmf) | `Mutex<Vec<u64>>` | Relative |
|---|---:|---:|---|
| push (uncontended) | 33.34 ns | 25.85 ns | 1.29x slower |
| push_pop_cycle | 34.25 ns | 35.95 ns | tied |

### Reading the trade-offs

1. **push 1.29x slower** than `Mutex<Vec>::push`. The
   contiguous Vec's bump-pointer + amortized realloc is very
   fast; the Treiber's CAS retry + slot index management
   pays for ABA safety.
2. **push_pop_cycle tied.** Both contenders do one push + one
   pop per iter; the Treiber's CAS pair matches the Mutex's
   lock cycle.
3. **The architectural lever is cross-process visibility** +
   the lock-free protocol that scales under concurrent
   producers / consumers; the mutex baseline serializes.

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<T>>` is the textbook in-process
  stack baseline.
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread push/pop correctness in source unit tests.
- **Sizing**: 1<<20 slots for push (no overflow at criterion's
  iters); 16 slots for cycle (push+pop each iter keeps the
  stack at 0/1 size).
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process push/pop**: any process pushes; any process
  pops. The Vec baseline is in-process only.
- **Concurrent producer/consumer scaling**: CAS-based push/pop
  doesn't serialize between threads (only when they hit the
  same head simultaneously); the mutex baseline serializes
  every op.
- **ABA safety**: counter-packed head defeats classic ABA
  where a value is popped + re-pushed between a reader's
  load and CAS.

---

## Worked examples

### LIFO work queue

```rust
use subetha_cxc::SharedTreiberStack;

let s: SharedTreiberStack<u32> = SharedTreiberStack::create("/tmp/s.bin", 1024).unwrap();
s.push(1).unwrap();
s.push(2).unwrap();
assert_eq!(s.pop(), Some(2));   // LIFO
assert_eq!(s.pop(), Some(1));
```

### Cross-process work-stealing pool

```rust
// Producer process:
let s: SharedTreiberStack<TaskId> = SharedTreiberStack::open("/tmp/work.bin").unwrap();
for task in tasks() { while s.push(task).is_err() { /* full */ } }

// Worker processes:
let s: SharedTreiberStack<TaskId> = SharedTreiberStack::open("/tmp/work.bin").unwrap();
while let Some(task) = s.pop() {
    execute(task);
}
```

---

## Use case patterns

### Pattern: cross-process work-stealing stack

Workers pop tasks LIFO; producers push. Lock-free push/pop
scale to N concurrent participants.

### Pattern: free-list backing for an arena

SharedRegion uses a Treiber-like protocol internally for its
free list; the standalone primitive exposes the same shape.

### Pattern: undo / history stack

LIFO semantics match undo. Cross-process LET multiple processes
share the same history.

---

## Known limitations

- **Bounded capacity at create**.
- **LIFO only**: no FIFO; use SharedRing for FIFO.
- **No peek**: pop consumes the value.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Tight retry loops under heavy contention**. CAS retries
  are bounded but high contention burns CPU. Yield between
  retries if hot.

- **Sizing the stack too small**. `push` returns `Full` once
  capacity is exhausted. Plan for max live count.

- **Wrapping in a Mutex.** Pointless; the Treiber CAS is the
  synchronization mechanism.

---

## References

- Source: `crates/subetha-cxc/src/shared_treiber_stack.rs` (541
  lines, 11 unit tests covering push/pop LIFO, capacity bound,
  cross-handle visibility, ABA safety verification).
- Bench: `crates/subetha-cxc/benches/shared_treiber_stack.rs`
  (push, push_pop_cycle vs `Mutex<Vec>`).
- Sibling primitive: [SHARED_RING.md](./SHARED_RING.md) -
  FIFO MPMC ring; Treiber is the LIFO sibling.
- Underlying technique: ABA-safe CAS via counter-packed
  pointer; same trick SharedRegion's free list uses.
