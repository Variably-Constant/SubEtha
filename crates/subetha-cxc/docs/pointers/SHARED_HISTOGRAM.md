# SharedHistogram

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/per_bucket-AtomicU64_fetch_add-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process bucketed counter for distribution tracking.
Caller supplies N bucket boundaries at create time; `record(v)`
finds the right bucket via binary search and atomically
increments its counter. K boundaries -> K+1 buckets (overflow
on top).

> **The "cross-process latency / size distribution at
> lock-free cost" primitive.** record at **13.06 ns** vs
> `Mutex<Vec<u64>>` 15.73 ns (**1.20x faster**). count at
> **1.70 ns** vs 16.94 ns (**9.96x faster** - lock-free
> atomic load vs full lock cycle). percentile p99 at 95.81 ns.
> Architectural lever: each bucket has its own `AtomicU64`
> counter (8 packed per 64-byte cache line), so concurrent
> recorders into different buckets contend at most on a shared
> cache line - never on a global lock the way the mutex baseline
> serializes every record regardless of bucket.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Boundaries fixed at create**: ascending; verified at open.
- **K boundaries -> K+1 buckets**: bucket 0 = `v < b0`,
  bucket i = `b{i-1} <= v < bi`, bucket N = `v >= b{N-1}`.
- **Per-bucket AtomicU64 counters**: 8 bytes each, packed
  contiguously (8 counters per 64-byte cache line).
- **`fetch_add(1, AcqRel)`** per record; lock-free, no spin.
- **`percentile(p)`** walks buckets accumulating counts +
  linear interpolation. Granularity = bucket width.
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [Bench evidence](#bench-evidence)
- [Worked examples](#worked-examples)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

```text
+---------------------------+
| HistogramHeader (64B)     |
|   magic + n_boundaries    |
|   total_count (Atomic)    |
+---------------------------+
| boundaries [u64; K]       |  ascending
+---------------------------+
| counters [AtomicU64; K+1] |  per-bucket counts
+---------------------------+
```

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_histogram.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

Workload: 7 latency boundaries [10, 100, 1k, 10k, 100k, 1M],
record value 500 (bucket 3).

| Op | `SharedHistogram` (mmf) | `Mutex<Vec<u64>>` | Relative |
|---|---:|---:|---|
| record | **13.06 ns** | 15.73 ns | **1.20x faster** |
| count | **1.70 ns** | 16.94 ns | **9.96x faster** |
| percentile p99 | 95.81 ns | n/a | walks 7 buckets + interp |

### Reading the trade-offs

1. **record 1.20x faster**: binary search of 7 boundaries +
   one atomic fetch_add vs mutex lock + indexing + increment +
   unlock.
2. **count 9.96x faster**: one atomic load vs full lock cycle.
3. **Concurrent recording win is multiplied** (not measured
   here): different buckets = different cache lines = no
   contention. Mutex baseline serializes ALL recorders.
4. **percentile p99 at 96 ns**: linear walk + interpolation;
   granularity is bucket width.

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<u64>>` with same boundaries +
  same binary-search bucket lookup. Identical protocol shape.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
- **Sizing**: 7-boundary histogram (typical latency dashboard
  config); record 1000 then percentile.
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process recording**: N processes each record into the
  same histogram via lock-free fetch_add; observers query
  percentiles without locks.
- **Concurrent recording into different buckets scales
  linearly**: distinct cache lines mean no contention.
  Mutex baseline serializes all recorders.
- **Latency dashboard pattern**: cheap concurrent recording +
  cheap observer reads makes real-time p99 monitoring
  feasible.

---

## Worked examples

### Basic latency tracking

```rust
use subetha_cxc::SharedHistogram;

let h = SharedHistogram::create("/tmp/lat.bin",
    &[10, 100, 1_000, 10_000, 100_000, 1_000_000]).unwrap();
for &lat_us in latencies.iter() {
    h.record(lat_us);
}
let p50 = h.percentile(0.50);
let p99 = h.percentile(0.99);
println!("p50={p50} p99={p99}");
```

### Cross-process aggregate dashboard

```rust
// Each worker:
let h = SharedHistogram::open("/tmp/lat.bin",
    &[10, 100, 1_000, 10_000, 100_000, 1_000_000]).unwrap();
let start = std::time::Instant::now();
do_request();
h.record(start.elapsed().as_micros() as u64);

// Dashboard:
let h = SharedHistogram::open("/tmp/lat.bin", &[/*same bounds*/]).unwrap();
println!("p99: {} us", h.percentile(0.99));
```

---

## Use case patterns

### Pattern: cross-process latency distribution

Each worker records request latency; dashboard reads p50/p99/p999
without polling logs.

### Pattern: queue-depth sampling

Periodic sampler records queue depths; an observer computes
distribution stats.

### Pattern: request-size or memory-allocation distribution

Anything bucketable. Boundaries chosen log-spaced for wide
dynamic range with bounded memory.

---

## Known limitations

- **Boundaries fixed at create**: no auto-rebucket.
- **Percentile granularity = bucket width**: coarse boundaries
  give coarse percentiles.
- **No min / max tracking**: callers track separately if needed.
- **u64 counters**: 2^64 records per bucket cap (practically
  unreachable).
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Linear-spaced boundaries for wide-range data.** Latencies
  span 6+ decades; use log-spaced bounds [10, 100, 1k, 10k, ...].

- **Mismatched boundaries at open.** Open verifies boundaries
  match the creator's; pin in a shared spec.

- **Reading percentile during heavy recording.** percentile
  walks all buckets with Acquire loads; concurrent fetch_add
  is fine but the percentile may shift between reads. Read
  total_count first as a snapshot anchor.

- **Wrapping in a Mutex.** Pointless; per-bucket fetch_add is
  already the synchronization mechanism.

---

## References

- Source: `crates/subetha-cxc/src/shared_histogram.rs` (581
  lines, 16 unit tests covering record + bucket placement,
  percentile, cross-handle visibility, boundary validation,
  and out-of-bounds rejection).
- Bench: `crates/subetha-cxc/benches/shared_histogram.rs` (record,
  count, percentile_p99 vs `Mutex<Vec<u64>>`).
- Sibling primitive:
  [SHARED_COUNT_MIN_SKETCH.md](./SHARED_COUNT_MIN_SKETCH.md) -
  per-key frequency estimate (probabilistic); Histogram is
  per-bucket exact count.
- Sibling primitive:
  [SHARED_HYPER_LOG_LOG.md](./SHARED_HYPER_LOG_LOG.md) -
  cardinality estimate (distinct count).
- Sibling primitive:
  [SHARED_RESERVOIR_SAMPLER.md](./SHARED_RESERVOIR_SAMPLER.md) -
  uniform random sample; Histogram is the aggregate-by-
  bucket variant.
