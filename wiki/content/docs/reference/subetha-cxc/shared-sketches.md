---
weight: 70
---

# Sketches, arenas, and bit-level primitives

Eight primitives that share the "insert + query, accept
approximate answers in exchange for fixed-size footprint" shape.
Each is the cross-process variant of a well-known probabilistic
or compact data structure.

## `SharedBitVec`

Bit vector with concurrent set / clear / get / range / count
operations. Words are `AtomicU64`s (`BITS_PER_WORD = 64`); each
bit op is a `fetch_or` / `fetch_and` / load on the containing
word.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity_bits: usize) -> Result<Self, BitVecError>;
pub fn open(path: impl AsRef<Path>, expected_bits: usize) -> Result<Self, BitVecError>;

pub fn set(&self, index: usize) -> Result<bool, BitVecError>;
pub fn clear(&self, index: usize) -> Result<bool, BitVecError>;
pub fn get(&self, index: usize) -> Result<bool, BitVecError>;
pub fn toggle(&self, index: usize) -> Result<bool, BitVecError>;
pub fn count_ones(&self) -> usize;
```

Op kinds: `OP_SET = 1`, `OP_CLEAR = 2`, `OP_GET = 3`,
`OP_TOGGLE = 4`, `OP_RANGE = 5`, `OP_COUNT_ONES = 6`.

Canonical doc:
[SHARED_BIT_VEC.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BIT_VEC.md).

## `SharedBloomFilter`

Bloom filter with `K` hash functions. Each insert sets `K` bits;
each query checks `K` bits and returns false on any miss. False
positives bounded by the bit-vector size and hash count; no
false negatives.

The K hashes are derived from one FNV-1a hash plus a Kirsch-
Mitzenmacher double-hashing trick (two hashes combined to
generate K).

Op kinds use the `sketch` module: `OP_INSERT = 1`, `OP_QUERY = 2`,
`OP_CLEAR = 3`.

Canonical doc:
[SHARED_BLOOM_FILTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_BLOOM_FILTER.md).

## `SharedCountMinSketch`

Count-min sketch for frequency estimation. `width * depth`
counters in a 2D grid; insert increments `depth` counters
(one per row); query returns the minimum of the `depth` rows.
Overestimates only - the minimum bound rules out collisions in
at least one row.

Op kinds use the `sketch` module: `OP_INSERT = 1`,
`OP_QUERY = 2`, `OP_CLEAR = 3`.

Canonical doc:
[SHARED_COUNT_MIN_SKETCH.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_COUNT_MIN_SKETCH.md).

## `SharedHyperLogLog`

HyperLogLog cardinality estimator. Each insert updates one
register with the leading-zero count of the hashed value; the
cardinality estimate is the harmonic mean of the registers
applied to the standard HLL formula.

Precision is configurable via `MIN_PRECISION` and
`MAX_PRECISION` bounds; higher precision means more registers
and a smaller standard error at the cost of larger footprint.

Op kinds use the `sketch` module.

Canonical doc:
[SHARED_HYPER_LOG_LOG.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HYPER_LOG_LOG.md).

## `SharedHistogram`

Exponentially-bucketed histogram for latency or value
distributions. Buckets are `[2^k, 2^(k+1))` for `k = 0..N`. Each
bucket is an `AtomicU64` counter; record is one `fetch_add`,
percentile is a linear scan over buckets.

```rust,no_run
pub fn record(&self, value: u64) -> usize;   // returns the bucket index hit
pub fn count(&self, bucket_idx: usize) -> Result<u64, HistogramError>;
pub fn percentile(&self, p: f64) -> u64;
```

Op kinds use the `histogram` module: `OP_RECORD = 1`,
`OP_COUNT = 2`, `OP_PERCENTILE = 3`.

Canonical doc:
[SHARED_HISTOGRAM.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HISTOGRAM.md).

## `SharedReservoirSampler`

Uniform sampling of a stream. Classical reservoir sampling
across processes: each `record(value)` either fills an empty
slot (until the reservoir is full) or replaces a random
existing slot with probability `reservoir_size / total_seen`.

`snapshot()` returns the current reservoir as a `Vec<T>` for
analysis. Op kinds use the `reservoir` module: `OP_RECORD = 1`,
`OP_SNAPSHOT = 2`.

Canonical doc:
[SHARED_RESERVOIR_SAMPLER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_RESERVOIR_SAMPLER.md).

## `SharedStringArena`

Interning arena for strings. `intern(s)` returns a `StringRef`
handle (essentially an `OffsetPtr`); `get_bytes(handle)` returns
the underlying bytes. The arena is append-only - no removal,
only `clear` to reset the whole arena.

Used as the storage layer behind cross-process maps whose values
include variable-length strings. The map stores the
fixed-size `StringRef`, and the actual bytes live in the arena.

Op kinds use the `string_arena` module: `OP_INTERN = 1`,
`OP_GET_BYTES = 2`, `OP_CLEAR = 3`.

Canonical doc:
[SHARED_STRING_ARENA.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_STRING_ARENA.md).

## `SharedHandleTable`

Transient identifier table. Each `acquire()` allocates a
`Handle` from a free-slot bitmap and returns it; `release(handle)`
frees the slot. Each slot carries a fixed-size `SLOT_PAYLOAD_BYTES`
of associated data the application reads via `get(handle)`.

Use case: cross-process handles to in-flight requests, network
connections, transaction IDs. The handle is small (a u32 slot
index plus a generation counter packed into a u64), which makes
it cheap to pass through any cross-process channel.

Op kinds use the `ownership` module: `OP_ACQUIRE = 1`,
`OP_RELEASE = 2`, `OP_GET = 3`, `OP_BEAT = 4`, `OP_CLAIM = 5`.

Canonical doc:
[SHARED_HANDLE_TABLE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_HANDLE_TABLE.md).

## See also

- [`SharedHashMap`](shared-hash-map.md) - the exact-membership
  alternative when false positives are not acceptable.
- [Role-pair selection](../../how-to/role-pair-selection.md) -
  sketches sit on the insert/query shape.
