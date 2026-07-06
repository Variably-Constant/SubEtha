# SharedBloomFilter

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/probabilistic-Kirsch--Mitzenmacher-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Compression](https://img.shields.io/badge/storage-33x_vs_HashSet-informational)

Cross-process probabilistic set-membership filter. Composite over
[`SharedBitVec`](./SHARED_BIT_VEC.md): insert hashes the input k
times and sets those k bits; `contains` returns true when every
one of those k bits is set. No false negatives, tunable
false-positive rate. The two hash positions per query come from
two seeded FNV-1a passes; the remaining `k - 2` positions derive
via Kirsch-Mitzenmacher double-hashing
`(h1 + i * h2) mod n_bits`.

> **The "cross-process set membership without per-item bytes"
> primitive.** 33.4x storage compression vs `HashSet<Vec<u8>>` for
> 10k 16-byte items at 1% FPR (11,982 bytes vs ~400,000 bytes).
> Per-op cost trades: hit-probes lose to `HashSet` (84.55 ns vs
> 33.56 ns, 2.5x slower - 7 random-position bit probes vs one
> bucket lookup); miss-probes win (23.72 ns vs 29.28 ns, 1.23x
> faster - bloom early-outs at the first zero bit). The
> architectural lever is **storage + cross-process visibility**,
> not microbenchmark per-op cost.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Probabilistic**: false positives possible, false negatives
  never. `contains(x) == false` is authoritative; `contains(x) ==
  true` means MAYBE.
- **Tunable**: `SharedBloomFilter::suggest_config(n_items, p)`
  returns `(n_bits, n_hashes)` for target FPR `p`.
- **Hash**: FNV-1a with two distinct seeds; the k positions are
  derived via Kirsch-Mitzenmacher double-hashing. Deterministic
  across processes and OSes.
- **Underlying storage**: a [`SharedBitVec`](./SHARED_BIT_VEC.md);
  the SeqLock-free atomic `fetch_or` per word makes inserts
  lock-free.
- **Header file separate from bits file**: `<base>.bloom.bin`
  carries the (magic, n_bits, n_hashes) header;
  `<base>.bits.bin` holds the bit array.
- **Configuration locked at create**: cross-handle opens verify
  `n_bits` and `n_hashes` match and fail with `LayoutMismatch`
  otherwise.

---

## Table of contents

- [What it is](#what-it-is)
- [Insert / contains protocol](#insert--contains-protocol)
- [Sizing formula](#sizing-formula)
- [Bench evidence](#bench-evidence)
- [Worked examples](#worked-examples)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

`SharedBloomFilter` is a thin composite over `SharedBitVec`:

```text
+---------------------------+   <base>.bloom.bin
| BloomHeader (64B)         |   magic + n_bits + n_hashes + padding
+---------------------------+

+---------------------------+   <base>.bits.bin
| BitVecHeader (64B)        |   from SharedBitVec
+---------------------------+
| u64[ceil(n_bits / 64)]    |   the bit array; lock-free fetch_or
+---------------------------+
```

The header file exists only to record sizing parameters; the
real shared state is the bit array. A cross-process opener gets
the same bits and the same hash schedule (FNV-1a is seed-only,
process-independent) so insertion in process A is observable in
process B.

---

## Insert / contains protocol

### Insert(item)

1. `h1 = FNV1a_seed1(item)`
2. `h2 = FNV1a_seed2(item)`
3. For `i in 0..n_hashes`:
   - `pos_i = (h1 + i * h2) mod n_bits`
   - `bits.set(pos_i)` - atomic `fetch_or` on the u64 word.

### contains(item)

1. `h1 = FNV1a_seed1(item)`
2. `h2 = FNV1a_seed2(item)`
3. For `i in 0..n_hashes`:
   - `pos_i = (h1 + i * h2) mod n_bits`
   - If `!bits.get(pos_i)`: return `false` (early-out).
4. All k positions set: return `true`.

The early-out on the first zero bit is what makes miss-probes
cheaper than hit-probes - most misses bail at i=0 or i=1.

---

## Sizing formula

For `n` distinct items and target false-positive rate `p`:

```text
n_bits   = -(n * ln(p)) / (ln(2)^2)
n_hashes = (n_bits / n) * ln(2)
```

`SharedBloomFilter::suggest_config(n_items, p)` computes this
and rounds up.

Common configurations:

| n items | FPR p | n_bits | n_hashes | Bits per item |
|---:|---:|---:|---:|---:|
| 1,000 | 0.01 | 9,586 | 7 | 9.59 |
| 10,000 | 0.01 | 95,851 | 7 | 9.59 |
| 100,000 | 0.01 | 958,506 | 7 | 9.59 |
| 1,000,000 | 0.001 | 14,377,587 | 10 | 14.38 |

The bits-per-item is **independent of n**; that is the
architectural invariant that makes Bloom filters scale better
than exact set membership at large n.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_bloom_filter.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

Workload: pre-built key cycle of 64 items, `HashSet` pre-populated
so re-inserts hit the idempotent already-present path. The Bloom
re-sets already-set bits on the same cycle. Both contenders
measure the hash+lookup hot path with no per-iter allocation.

**Single-item ops (key already present):**

| Op | `SharedBloomFilter` (mmf) | `Mutex<HashSet<Vec<u8>>>` | Bloom relative |
|---|---:|---:|---:|
| insert | 79.79 ns | 100.37 ns | **1.26x faster** |
| contains hit | 84.55 ns | 33.56 ns | **2.52x slower** |
| contains miss | 23.72 ns | 29.28 ns | **1.23x faster** |

**Batch ops (1000 items, both pay `format!()` overhead):**

| Op | `SharedBloomFilter` (mmf) | `Mutex<HashSet<Vec<u8>>>` |
|---|---:|---:|
| batch insert 1000 | 200.53 µs (~200 ns/op) | 165.62 µs (~166 ns/op) |
| batch contains 1000 | 193.89 µs (~194 ns/op) | 139.13 µs (~139 ns/op) |

**Storage density (the architectural lever):**

| Configuration | `SharedBloomFilter` | `HashSet<Vec<u8>>` | Compression |
|---|---:|---:|---:|
| n=10,000, FPR=0.01, 16-byte items | 11,982 bytes | ~400,000 bytes | **33.4x** |

### Reading the trade-offs

The story the numbers tell:

1. **Per-op cost**: `Mutex<HashSet>` wins on hit (single bucket
   lookup vs 7 random-position bit probes). The Bloom wins on
   miss (early-out at the first zero bit). At equal hit/miss
   ratio the HashSet wins per-op.
2. **Storage**: the Bloom is **~33x smaller** at 10k items at 1%
   FPR. That ratio holds at every n; both grow O(n) but the
   constant is ~12 bits/item vs ~40 bytes/item - a 27x raw ratio
   without the HashSet's bucket overhead, which adds the
   remaining ~6x.
3. **Cross-process**: the HashSet is heap-allocated and
   in-process only. The Bloom is MMF-backed; any process can
   open it and query.
4. **Scale**: HashSet probe cost grows with load factor and
   eventually triggers rehashing. Bloom probe cost is exactly k
   bit-tests regardless of n.

### Rule 3b bench audit

- **Fair contender**: `Mutex<HashSet<Vec<u8>>>` is the
  in-process exact-set baseline. Both contenders insert via the
  same key cycle, both measure the idempotent re-insert hot path
  with no `format!()`-in-iter allocation.
- **Sized for workload**: 10,000-item, 1% FPR config matches the
  HashSet's working set capacity.
- **No `thread::spawn` inside `b.iter`**: workload is single-
  threaded.
- **MMF lifecycle managed**: `create` then ops then `drop` then
  `cleanup_base` removes both backing files; no leaks across runs.

### What the numbers do NOT show

- **Cross-process query throughput**: every process gets the
  same map with no lock acquire and no IPC round-trip. The
  HashSet has no cross-process story.
- **Scaling**: at n=1M items, the Bloom is 1.2 MB; the HashSet
  is ~40 MB plus bucket overhead. The relative cost gap
  flips: at that scale the HashSet may not even fit in L2.
- **Concurrent inserters**: `SharedBitVec::set` is a lock-free
  `fetch_or`; multiple threads / processes insert concurrently
  with no global mutex. The HashSet baseline serializes on the
  outer Mutex.

---

## Worked examples

### Cross-process membership cache

```rust
use subetha_cxc::SharedBloomFilter;

// Process A - pre-warm the filter with all seen URLs:
let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(1_000_000, 0.001);
let b = SharedBloomFilter::create("/tmp/seen-urls.bin", n_bits, n_hashes).unwrap();
for url in load_url_corpus() {
    b.insert(url.as_bytes()).unwrap();
}
b.flush().unwrap();

// Process B - query before hitting the slow path:
let b = SharedBloomFilter::open("/tmp/seen-urls.bin", n_bits, n_hashes).unwrap();
if !b.contains(candidate.as_bytes()).unwrap() {
    // definitely new; do the expensive thing
    enqueue_for_crawl(candidate);
} else {
    // maybe-seen; fall through to the authoritative store
    if !exact_store.contains(candidate) {
        enqueue_for_crawl(candidate);
    }
}
```

### Tuning for a known workload

```rust
use subetha_cxc::SharedBloomFilter;

let (n_bits, n_hashes) = SharedBloomFilter::suggest_config(10_000, 0.01);
// n_bits = 95851, n_hashes = 7 - 12 KB filter, 1% FPR at full load.

let b = SharedBloomFilter::create("/tmp/items.bin", n_bits, n_hashes).unwrap();
for item in items() { b.insert(item.as_bytes()).unwrap(); }

let est = b.estimated_insert_count();   // density-derived estimate
let fpr = b.estimated_false_positive_rate();  // live FPR
println!("inserted ~{est} items, current FPR ~= {fpr:.4}");
```

---

## Use case patterns

### Pattern: skip-the-slow-path filter

Pre-populate with known-bad / known-existing keys; every
candidate is queried first; only candidates that pass (i.e., the
filter says MAYBE) hit the authoritative slow store. The
architectural payoff is amortizing the slow store's per-query
cost across the cheaper Bloom queries.

### Pattern: cross-process deduplication

Workers across processes all insert into the same Bloom; each
worker checks before doing work. Identical keys are filtered
without any cross-process synchronization beyond the lock-free
bit operations on the shared array.

### Pattern: bounded memory footprint

When the exact-set memory cost is prohibitive (gigabytes of keys
at scale), the Bloom converts the trade to "constant bits per
item independent of key size". A 10M-URL Bloom at 1% FPR is
~12 MB; the equivalent `HashSet<String>` is hundreds of MB.

---

## Known limitations

- **Probabilistic**: callers must handle false positives. If
  exactness is required, the Bloom is the wrong primitive.
- **No deletion**: a standard Bloom filter cannot remove a key
  without risking false negatives (other keys may share bits).
  Use `clear()` for full reset only.
- **Sizing locked at create**: increasing capacity requires a
  new filter and re-insert.
- **FNV-1a is not DoS-resistant**: adversarial input can collide
  the two seeds and elevate the effective FPR. Use only with
  trusted keys.
- **Bits scale with target FPR**: tighter FPR drives `n_bits`
  up; at FPR=10⁻⁹ the bits-per-item is ~43.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Treating a positive result as authoritative.** Every
  `contains(x) == true` must be re-verified against the
  authoritative store if the answer matters.

- **Undersizing for the actual item count.** Inserting 10k
  items into a filter sized for 1k pushes the FPR to ~50%; the
  filter becomes useless. Use `suggest_config` with realistic
  n.

- **Sharing the bits file across processes with mismatched
  `(n_bits, n_hashes)`.** Open will fail with `LayoutMismatch`,
  but a creator that picks different sizes per process leaves
  every other process unable to open. Lock the config in a
  central spec.

- **Trying to delete a key.** Clearing the k bits of a removed
  key risks false negatives because other keys share those
  bits. Use a counting Bloom (separate primitive) or just
  rebuild the filter from the authoritative set.

- **Using FNV-1a with adversarial input.** Two FNV-1a hashes
  with different seeds is enough for trusted-key workloads.
  Adversarial workloads need SipHash or BLAKE3.

---

## References

- Source: `crates/subetha-cxc/src/shared_bloom_filter.rs` (458 lines, 11 unit tests).
- Bench: `crates/subetha-cxc/benches/shared_bloom_filter.rs` (insert,
  contains hit, contains miss, batch insert 1000, batch contains
  1000, storage witness vs `Mutex<HashSet<Vec<u8>>>`).
- Underlying primitive: [SHARED_BIT_VEC.md](./SHARED_BIT_VEC.md) -
  the lock-free `fetch_or` bit array the Bloom is layered over.
- Sibling primitive:
  [SHARED_COUNT_MIN_SKETCH.md](./SHARED_COUNT_MIN_SKETCH.md) -
  counting frequency variant; Bloom answers "have we seen this?",
  CMS answers "how many times?".
- Sibling primitive:
  [SHARED_HYPER_LOG_LOG.md](./SHARED_HYPER_LOG_LOG.md) -
  cardinality-estimation variant; Bloom answers membership, HLL
  answers "how many distinct?".
- Original paper: Burton H. Bloom, "Space/Time Trade-offs in Hash
  Coding with Allowable Errors", Communications of the ACM, 1970.
- Double-hashing technique: Adam Kirsch, Michael Mitzenmacher,
  "Less Hashing, Same Performance: Building a Better Bloom
  Filter", ESA 2006.
