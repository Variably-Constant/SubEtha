---
title: "Shared Reservoir Sampler"
weight: 60
---

# SharedReservoirSampler&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/Vitter_algorithm_R-uniform_sampling-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process uniform reservoir sampler. Vitter's Algorithm R:
sample N items uniformly from a stream of unknown length, in
constant memory. Each item is kept with probability `k/n_seen`;
on keep, replace a uniformly-chosen existing slot.

> **The "uniform random sample in fixed memory" primitive.**
> record_under_cap (filling phase) at **18.32 ns** vs
> `Mutex<Vec>` 18.43 ns (tied). record_over_cap (replacement
> phase) at **17.48 ns** vs 17.23 ns (tied). snapshot_100 at
> 185 ns. Architectural lever: cross-process uniform sampling
> from a multi-process stream into one shared reservoir.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Bounded capacity at create**: K slots of T (`T: Copy`,
  `size_of::<T>() <= RESERVOIR_SLOT_PAYLOAD = 56`).
- **Each accepted `record` is one SeqLock-write**: no spin loops,
  no CAS retries on the write side.
- **Per-slot SeqLock on EVERY write** (version bumped to odd,
  payload copied, bumped to even) regardless of T size; readers
  spin on odd version. There is no small-T atomic-store fast path
  - the SeqLock is unconditional so any `T <= 56` bytes is
  tear-free.
- **`total_seen` is monotonic AtomicU64**: bounded by 2^64.
- **Cross-process backed by MMF.**

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_reservoir_sampler.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700.

| Op | `SharedReservoirSampler<u64>` (mmf) | `Mutex<Vec>` Vitter R | Relative |
|---|---:|---:|---|
| record (filling phase, n < K) | 18.32 ns | 18.43 ns | tied |
| record (replacement phase, n > K) | 17.48 ns | 17.23 ns | tied |
| snapshot (copy K=100 slots) | 185 ns | n/a | scan |

### Reading the trade-offs

1. **record is tied with the mutex baseline.** Both do one
   probability check + (maybe) one slot write. The mutex
   baseline has lock/unlock; the mmf does an unconditional
   per-slot SeqLock write (two version bumps + the copy).
2. **The architectural lever is cross-process visibility**:
   multiple processes can record into the same reservoir; the
   mutex baseline cannot.
3. **snapshot at 185 ns for K=100 copies**: ~1.85 ns/slot via
   SeqLock-read.

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<T>>` with manual Vitter's R
  algorithm. Identical sampling logic.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
- **Sizing**: K=100 reservoir; filling vs replacement phases
  benched separately.
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process recording**: N processes each record into
  the same reservoir; the mutex baseline cannot.
- **Bounded memory regardless of stream length**: K slots
  forever, not proportional to total seen.
- **Uniform sampling guarantee**: every item in the stream
  has equal probability of being in the final sample.

---

## Worked examples

### Single-process sampling

```rust
use subetha_cxc::SharedReservoirSampler;

let r: SharedReservoirSampler<u64> =
    SharedReservoirSampler::create("/tmp/r.bin", 1000).unwrap();
for ev in event_stream() {
    r.record(ev.id);
}
let sample = r.snapshot();   // 1000 uniformly-chosen events
```

### Cross-process aggregation

```rust
// Each worker process records into the same reservoir:
let r: SharedReservoirSampler<u64> =
    SharedReservoirSampler::open("/tmp/r.bin", 1000).unwrap();
for ev in my_partition() { r.record(ev.id); }

// Coordinator reads the union sample:
let r: SharedReservoirSampler<u64> =
    SharedReservoirSampler::open("/tmp/r.bin", 1000).unwrap();
let sample = r.snapshot();
```

---

## Use case patterns

### Pattern: cross-process uniform event sample

Workers across processes record sample candidates; the
reservoir maintains a uniform random sample of the full
event stream.

### Pattern: bounded-memory monitoring

Memory is K slots regardless of how many events flow through;
useful for high-volume streams where keeping everything is
impractical.

### Pattern: sample for downstream analytics

Sample maintains stream representativeness for later analysis
(percentile estimates, distribution checks) without storing
the full stream.

---

## Known limitations

- **K fixed at create**: bounded by caller's capacity choice.
- **No deletion**: items are replaced, not removed.
- **Concurrent races on the same replacement slot keep one
  value**: statistically the uniform property holds because
  both values were equally eligible.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Assuming the sample is FIFO or representative-of-recent.**
  The reservoir is uniform across the whole stream, not
  recency-biased.

- **Sizing K too small for the analytical question.** K=100
  gives ~10% standard error on percentile estimates; tighter
  bounds need larger K.

- **Wrapping in a Mutex.** Pointless; the SeqLock-write per
  slot is already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/shared_reservoir_sampler.rs`
  (487 lines, 11 unit tests covering record / snapshot /
  uniformity at known cardinalities / cross-handle
  visibility / reset). Full API: create/open, record ->
  Option<usize>, snapshot -> Vec<T>, reset() (sets total_seen=0;
  slots overwritten on next record), capacity()/total_seen(),
  flush/flush_async. `ReservoirError` is PayloadTooLarge /
  LayoutMismatch / IoError; uses a thread-local xorshift64 RNG.
- Bench: `crates/subetha-cxc/benches/shared_reservoir_sampler.rs`
  (record under cap, record over cap, snapshot vs
  `Mutex<Vec>` Vitter R).
- Original: Vitter, "Random sampling with a reservoir", ACM
  Trans. Math. Software 1985.
- Sibling primitive: [SHARED_HISTOGRAM.md](shared-histogram/) -
  bucketed distribution; reservoir samples the raw stream.
