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
[Shared Bit Vec](sketches/shared-bit-vec/).

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
[Shared Bloom Filter](sketches/shared-bloom-filter/).

## `SharedCountMinSketch`

Count-min sketch for frequency estimation. `width * depth`
counters in a 2D grid; insert increments `depth` counters
(one per row); query returns the minimum of the `depth` rows.
Overestimates only - the minimum bound rules out collisions in
at least one row.

Op kinds use the `sketch` module: `OP_INSERT = 1`,
`OP_QUERY = 2`, `OP_CLEAR = 3`.

Canonical doc:
[Shared Count-Min Sketch](sketches/shared-count-min-sketch/).

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
[Shared HyperLogLog](sketches/shared-hyper-log-log/).

## `SharedHistogram`

Exponentially-bucketed histogram for latency or value
distributions. Buckets are `[Shared Histogram](sketches/shared-histogram/).

## `SharedReservoirSampler`

Uniform sampling of a stream. Classical reservoir sampling
across processes: each `record(value)` either fills an empty
slot (until the reservoir is full) or replaces a random
existing slot with probability `reservoir_size / total_seen`.

`snapshot()` returns the current reservoir as a `Vec<T>` for
analysis. Op kinds use the `reservoir` module: `OP_RECORD = 1`,
`OP_SNAPSHOT = 2`.

Canonical doc:
[Shared Reservoir Sampler](sketches/shared-reservoir-sampler/).

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
[Shared String Arena](arenas/shared-string-arena/).

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
[Shared Handle Table](arenas/shared-handle-table/).

## See also

- [`SharedHashMap`](shared-hash-map.md) - the exact-membership
  alternative when false positives are not acceptable.
- [Role-pair selection](../../how-to/role-pair-selection.md) -
  sketches sit on the insert/query shape.
