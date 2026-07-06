# SharedBlockedBloomFilter

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/Lock--free-yes-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process probabilistic set membership, cache-blocked. The bit
array is split into 512-bit (one cache-line) blocks; every probe for
a single item lands in ONE block, so `contains` touches a single
cache line regardless of `n_hashes`. A standard Bloom filter scatters
its `n_hashes` probes across the whole bit array, up to `n_hashes`
separate cache lines per query. The block for an item is chosen by
Lemire fastrange over a first hash; the within-block bit positions
come from distinct 9-bit slices of a second, avalanche-mixed hash, so
the in-block bits stay decorrelated and the achieved false-positive
rate tracks the standard formula.

> **The "single-cache-line membership filter" primitive.** Same
> false-negative-free guarantee and target FPR as a standard Bloom
> filter, at 1.15x the bits, with a bounded one-line access per
> query. The locality is invisible while the filter fits in cache
> (every line is warm): at 10k items both tie, and at 4M items
> (L3-resident) `contains` ties the standard filter because the
> CPU overlaps the standard filter's independent probes. It pays off
> at the design point, a filter that exceeds L3: at 16M items
> (~19 MB) hit-heavy `contains` is **107.88 ns vs 201.84 ns for the
> standard filter (1.87x faster)**, because each of the standard
> filter's scattered probes misses to RAM while the blocked filter
> misses at most once. Insert is 61 ns vs 57 ns (1.07x slower, the
> block-select arithmetic).

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a
  `HandshakeHeader` + `ObservationRing` and implements
  `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to
  register with the global sidecar; raw `create()` / `open()` return
  the unregistered type unchanged.
- **Probabilistic**: false positives are possible at the configured
  rate; **false negatives are NOT** (a present item always tests
  positive).
- **`insert` and `contains` take `&[u8]`** and are lock-free; many
  processes may query concurrently.
- **Fixed capacity at create**: `suggest_config(n, p)` returns
  `(n_bits, n_hashes)` for `n` items at FPR `p`; `create` rounds
  `n_bits` up to whole 512-bit blocks. The filter does not grow;
  exceeding `n` inflates the achieved FPR.
- **1.15x bit margin**: the blocked layout uses ~1.15x the bits of a
  standard filter at the same target FPR, absorbing per-block
  Poisson load variance.
- **Cross-process backed by MMF** (a single file).

---

## Bench evidence

Bench harness:
`crates/subetha-cxc/benches/shared_blocked_bloom_filter.rs`.
Captured 2026-06-20 on Windows 11 / Zen+ R7 2700 (16 MB L3),
Criterion with `--sample-size=12 --warm-up-time=1
--measurement-time=2`.

Contender: the standard `SharedBloomFilter` at the identical
suggested config (same FPR target).

| Op | `SharedBlockedBloomFilter` | `SharedBloomFilter` (standard) | Relative |
|---|---:|---:|---|
| insert (small filter) | 61.07 ns | 57.49 ns | 1.07x slower |
| contains, 16M items (>L3), hit-heavy | 107.88 ns | 201.84 ns | **1.87x faster** |
| bit budget (n=4M, FPR 0.01) | 5.51 MB | 4.79 MB | 1.15x the bits |

### Reading the trade-offs

1. **Locality is scale-gated.** While the whole filter is cache-
   resident, the standard filter's `n_hashes` probes are independent
   loads that the out-of-order core issues in parallel, so its lines
   are warm and overlapped: at 10k and 4M items the two filters tie.
   The advantage appears only once the filter exceeds L3, where each
   scattered probe misses to RAM.
2. **At the design point the blocked filter wins.** At 16M items
   (~19 MB, beyond the 16 MB L3) with hit-heavy queries, the standard
   filter pays `n_hashes` RAM misses per query (even overlapped, more
   than memory-level parallelism fully hides) while the blocked
   filter pays one. That is the 1.87x gap.
3. **Insert costs a touch more.** Choosing the block (fastrange) and
   slicing the in-block bits is marginally more arithmetic than the
   standard filter's per-probe positions, so insert is ~1.07x slower.
4. **The bit margin is small.** 1.15x the bits buys the one-line
   access pattern at the same target FPR.

### Rule 3b bench audit

- **Fair contender**: the standard `SharedBloomFilter`, configured
  to the same FPR via the same `suggest_config` inputs; both are
  false-negative-free membership filters.
- **Sized for the feature**: the locality claim is a cache-miss
  claim, so the headline `contains` case uses a 16M-item filter that
  exceeds L3 with cache-cold, hit-heavy spread queries. A small-
  filter case is reported too, so the "ties in cache" reality is not
  hidden behind the large-filter win.
- **No early-bail confound**: the headline case is hit-heavy, so the
  standard filter actually performs all `n_hashes` probes rather
  than bailing on the first clear bit of a miss.
- **MMF lifecycle managed**: per-bench create + fill + query + drop +
  remove_file.

### What the numbers do NOT show

- **Cross-process membership**: any process opens the filter and
  queries it; the bit array is shared, not per-process.
- **Memory bandwidth**: touching one line per query instead of
  `n_hashes` also lowers total memory traffic under a stream of
  distinct queries, beyond the single-query latency measured here.
- **Constant memory regardless of item size**: a 16-byte key and a
  16 KB key cost the same bits.

---

## Worked examples

### Basic membership

```rust
use subetha_cxc::SharedBlockedBloomFilter;

let (n_bits, n_hashes) = SharedBlockedBloomFilter::suggest_config(1_000_000, 0.01);
let bf = SharedBlockedBloomFilter::create("/tmp/bf.bin", n_bits, n_hashes).unwrap();
bf.insert(b"alice@example.com");
assert!(bf.contains(b"alice@example.com"));   // present: always true
let _maybe = bf.contains(b"bob@example.com"); // absent: false, or rarely a false positive
```

### Cross-process seen-set

```rust
// Writer process:
let (n_bits, n_hashes) = SharedBlockedBloomFilter::suggest_config(10_000_000, 0.001);
let bf = SharedBlockedBloomFilter::create("/tmp/seen", n_bits, n_hashes).unwrap();
bf.insert(record_id);
// Reader processes (many, concurrent):
let bf = SharedBlockedBloomFilter::open("/tmp/seen", n_bits, n_hashes).unwrap();
if !bf.contains(record_id) { /* definitely new */ }
```

---

## Use case patterns

### Pattern: large-scale cross-process dedup

A huge "have I seen this id" filter shared across worker processes.
Past L3 the one-line access is the whole point: a stream of distinct
lookups touches one line each instead of `n_hashes`.

### Pattern: pre-filter before an expensive lookup

`contains` gates a costly disk / network lookup; the blocked layout
keeps the gate to a single cache line so the fast-reject path stays
cheap even when the filter is large.

---

## Known limitations

- **Probabilistic**: false positives at the configured rate; size
  `suggest_config` for the real item count to hold the target FPR.
- **No locality benefit while cache-resident**: below ~L3 the
  blocked filter ties the standard one; choose it for large filters
  or for the bounded-access guarantee, not for small in-cache sets.
- **1.15x the bits** of a standard filter at the same FPR.
- **Fixed capacity at create**: no auto-grow.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Undersizing `n`.** Passing a too-small `n` to `suggest_config`
  overfills the blocks and inflates the achieved FPR. Size for the
  maximum live item count.

- **Expecting a speedup at small sizes.** In cache the standard
  filter's probes overlap; the blocked filter's win is a cache-miss
  win that needs a filter larger than L3.

- **Treating a positive as definite.** A positive is "probably
  present"; only a negative is certain. Gate the expensive path on
  the negative.

---

## References

- Source: `crates/subetha-cxc/src/shared_blocked_bloom_filter.rs`
  (512-bit blocks, Lemire fastrange block select, decorrelated
  9-bit in-block slices, with unit tests covering insert/contains,
  false-negative-freedom, and a false-positive-rate check against
  the configured target).
- Bench: `crates/subetha-cxc/benches/shared_blocked_bloom_filter.rs`
  (insert, contains at 16M items, bit-budget witness vs the standard
  filter).
- Sibling primitive: [SHARED_BLOOM_FILTER.md](./SHARED_BLOOM_FILTER.md)
  - the standard scatter-probe filter; the blocked variant trades
  1.15x the bits for one-line access.
- Sibling primitive: [SHARED_COUNT_MIN_SKETCH.md](./SHARED_COUNT_MIN_SKETCH.md)
  - frequency estimation under the same probabilistic, fixed-memory
  contract.
