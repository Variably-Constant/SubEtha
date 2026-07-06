# SharedHyperLogLog

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/per_register-AtomicU8_fetch_max-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Memory](https://img.shields.io/badge/2%5Ep_bytes-fixed-informational)

Cross-process probabilistic distinct-count estimator. 2^p
`AtomicU8` registers, each storing the max-observed rank
(leading-zero count + 1) of items hashed to that register.
Estimate via harmonic mean with bias correction. Standard
error ~= `1.04 / sqrt(2^p)`.

> **The "cardinality estimate in fixed memory" primitive.**
> insert at **16.30 ns** vs `Mutex<HashSet>` 113.10 ns
> (**6.94x faster** - one fetch_max on one AtomicU8 vs full
> lock + alloc + hash + bucket insert). estimate at 58.15 µs
> (walks 4096 registers + harmonic mean) vs HashSet's
> `len()` at 16.64 ns. The architectural lever is **memory**:
> 4160 bytes for HLL p=12 vs HashSet ~32 MB at 1M items
> (**7692x smaller**); same 4160 bytes at any cardinality.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Precision `p` in [4, 16]**: m = 2^p registers, mem = m + 64
  bytes header.
- **`insert(bytes)`**: one `fetch_max(rank, AcqRel)` on one
  register. Lock-free; no spin.
- **`estimate()`**: walks all m registers + harmonic mean +
  small/large bias correction.
- **Standard error**: `1.04 / sqrt(m)`. p=12 -> ~1.6%; p=14 ->
  ~0.8%; p=16 -> ~0.4%.
- **Mergeable**: two HLLs with same precision merge via
  per-register max - cardinality of the union.
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
| HLLHeader (64B)           |
|   magic + precision + m   |
+---------------------------+
| registers [AtomicU8; m]   |  one byte per register
+---------------------------+
```

m = 2^p. At p=12, that's 4096 + 64 = 4160 bytes total.

### Hash to register

```text
h = FNV1a(bytes)
register_idx = top p bits of h
rank = leading_zeros((h << p) | (1 << (63-p))) + 1
registers[register_idx].fetch_max(rank, AcqRel)
```

The `(1 << (63-p))` OR ensures rank is bounded even when low
bits are zero.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_hyper_log_log.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

Workload: precision p=12 (m=4096, ~1.6% std err). Pre-built
key cycle of 64 items so neither contender pays
`format!()`+alloc inside `b.iter`. HashSet pre-populated to
match HLL's idempotent re-insert semantics.

| Op | `SharedHyperLogLog` p=12 (mmf) | `Mutex<HashSet<Vec<u8>>>` | Relative |
|---|---:|---:|---|
| insert | **16.30 ns** | 113.10 ns | **6.94x faster** |
| estimate (cardinality) | 58.15 µs | 16.64 ns (HashSet len) | **3496x slower** for estimate |
| storage at p=12 | **4,160 bytes** | ~32,000 B @ 1k / 32 MB @ 1M | **7.69x / 7692x smaller** |

### Reading the trade-offs

1. **insert 6.94x faster**: one fetch_max on one AtomicU8 vs
   lock + bucket alloc + hash + insert. HLL is fundamentally
   cheaper to maintain.
2. **estimate 3496x slower**: HLL walks 4096 registers + does
   harmonic mean math. HashSet keeps a running counter
   updated on each insert. HLL trades estimate-speed for
   **constant memory regardless of cardinality**.
3. **Memory is the architectural lever**: 4160 bytes for HLL
   p=12 vs HashSet ~32 MB at 1M items (7692x smaller). At 1B
   items the gap is millions x.
4. **Standard error**: ~1.6% at p=12; for most cardinality
   monitoring this is sufficient.

### Rule 3b bench audit

- **Fair contender**: `Mutex<HashSet<Vec<u8>>>` is the exact
  in-process counter. Both contenders use the same pre-built
  key cycle; HashSet pre-populated so re-inserts hit the
  idempotent already-present path.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
- **Sizing**: p=12 (typical accuracy point); 64-key cycle
  representative of churn-pattern workloads.
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cardinality scaling**: HLL is constant memory; HashSet
  grows linearly. At 1B items HashSet costs ~32 GB; HLL
  remains at 4160 bytes.
- **Cross-process inserts**: every process can `insert`
  concurrently via lock-free fetch_max; HashSet serializes on
  one mutex.
- **Merge**: two HLLs merge in O(m) by taking per-register max;
  the merged HLL estimates the cardinality of the union.

---

## Worked examples

### Distinct-user estimation

```rust
use subetha_cxc::SharedHyperLogLog;

let hll = SharedHyperLogLog::create("/tmp/users.bin", 12).unwrap();
for user_id in user_events() {
    hll.insert(user_id.as_bytes());
}
let approx_unique = hll.estimate();
println!("~{approx_unique} unique users");
```

### Cross-process aggregation

```rust
// Worker process N:
let hll = SharedHyperLogLog::open("/tmp/users.bin", 12).unwrap();
for ev in partition_n_events() {
    hll.insert(ev.user_id.as_bytes());
}

// Dashboard:
let hll = SharedHyperLogLog::open("/tmp/users.bin", 12).unwrap();
println!("cluster-wide unique: {}", hll.estimate());
```

---

## Use case patterns

### Pattern: distinct-user counter at scale

Each process inserts user IDs; a dashboard estimates total
uniques in constant memory regardless of user-base size.

### Pattern: distinct-IP / distinct-URL / distinct-anything

Anywhere where "how many distinct X did we see?" is the
question and an HashSet costs prohibitive memory.

### Pattern: union of cardinalities

Each shard maintains its own HLL; central process merges them
to estimate the union cardinality. Approximate equivalent of
`A.union(B).len()` without materializing the union.

---

## Known limitations

- **Approximate, never exact**: ~1.6% std err at p=12. For
  decisions requiring exactness, use HashSet (at the memory
  cost).
- **No deletion**: items cannot be removed.
- **estimate() walks all m registers**: O(m). At p=16 (m=65536)
  that's ~1 ms per estimate.
- **Standard error scales with 1/sqrt(m)**: tighter accuracy
  requires more memory.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Comparing estimates of two different precisions.** Std err
  differs; combine only at the same `p`.

- **Treating the estimate as exact for small cardinalities.**
  HLL has bias at low cardinality (n < ~5 * m); the
  implementation applies the standard small-range correction
  but precision is worse than at the dense regime.

- **Wrapping in a Mutex.** Pointless; per-register fetch_max
  is already the synchronization mechanism.

- **Tightening precision without measuring memory budget.**
  p=18 = 256 KB; p=20 = 1 MB. Most workloads see no benefit
  past p=14.

---

## References

- Source: `crates/subetha-cxc/src/shared_hyper_log_log.rs` (424
  lines, 11 unit tests covering insert + estimate at known
  cardinalities, precision validation, cross-handle
  visibility, merge, and accuracy at scale).
- Bench: `crates/subetha-cxc/benches/shared_hyper_log_log.rs`
  (insert, estimate, storage witness vs `Mutex<HashSet>`).
- Original: Flajolet, Fusy, Gandouet, Meunier, "HyperLogLog:
  the analysis of a near-optimal cardinality estimation
  algorithm", AofA 2007.
- Sibling primitive: [SHARED_BLOOM_FILTER.md](./SHARED_BLOOM_FILTER.md) -
  presence-only (no count); HLL adds distinct cardinality.
- Sibling primitive:
  [SHARED_COUNT_MIN_SKETCH.md](./SHARED_COUNT_MIN_SKETCH.md) -
  per-key frequency; HLL is total distinct count.
- Sibling primitive: [SHARED_HISTOGRAM.md](./SHARED_HISTOGRAM.md) -
  bucketed distribution.
