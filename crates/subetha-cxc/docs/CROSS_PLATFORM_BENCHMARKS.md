# Cross-platform benchmarks

Per-primitive benchmark results across five CPU microarchitectures and
four operating systems. Each cell is the Criterion median of the
primitive's own (MMF-backed) operation; baselines are shown where the
contrast is the point (B-tree vs `Mutex<BTreeMap>`, blocked vs standard
Bloom, shared cell vs `RwLock`).

| Column | CPU | OS | Notes |
|---|---|---|---|
| **Zen+ / Win** | Ryzen 7 2700 (Zen+) | Windows 11 | the canonical published hardware |
| **Zen2 / Linux** | EPYC 7552 (Zen2) | Linux | AVX2, no AVX-512 |
| **Zen3 / Linux** | Zen3 | Linux | |
| **Zen3 / FreeBSD** | Zen3 | FreeBSD 15 | same silicon class as Zen3/Linux, different kernel |
| **Ivy / macOS** | Core i5-3210M (Ivy Bridge) | macOS 10.15 | 2012 2-core Intel outlier, not on the Zen ladder |

Captured 2026-06-20, Criterion `--sample-size=12 --warm-up-time=1
--measurement-time=2`, single-threaded unless the op name says otherwise.
All primitives run with the per-op observation sidecar gated by the
process-global armed flag, so a raw handle pays a hot-path load and a
predicted branch and nothing else - the numbers below are with
observation present but unarmed, the production shape.

The four Zen columns read left to right as a clean microarchitecture
ladder: Zen+ is roughly 2x the Zen3 across the board; Zen2 sits between.
Zen3 on Linux and Zen3 on FreeBSD land within a few percent of each
other (same silicon class), with each OS marginally ahead on different
ops - the allocator and syscall paths differ even when the CPU does not.
The fifth column is the outlier: a 2012 Intel Core i5 (Ivy Bridge) on
macOS, a 13-year-old 2-core part that lands well behind the Zen chips
and flips a handful of cells where its memory subsystem or low core
count dominates rather than its clock.

---

## Atomics, cells, and sub-nanosecond reads

The cheapest ops, where the `push_op` observation gate matters most: an
unarmed observation is a single hot-global load, so these stay at the
hardware floor.

| Op | Zen+ / Win | Zen2 / Linux | Zen3 / Linux | Zen3 / FreeBSD | Ivy / macOS |
|---|---:|---:|---:|---:|---:|
| `SharedAtomicU64::load` | 1.56 ns | 1.30 ns | 785 ps | 846 ps | 9.32 ns |
| `SharedAtomicU64::fetch_add` | 9.68 ns | 6.14 ns | 1.72 ns | 1.82 ns | 39.8 ns |
| `SharedAtomicU64::compare_exchange` | 9.80 ns | 6.20 ns | 2.17 ns | 2.23 ns | 35.4 ns |
| `SharedBitVec::get` | 2.62 ns | 2.14 ns | 1.35 ns | 1.38 ns | 6.55 ns |
| `SharedBitVec::set` | 7.73 ns | 6.66 ns | 2.56 ns | 2.56 ns | 34.5 ns |
| `SharedBitVec::count_ones` (1024 bits) | 40.2 ns | 33.9 ns | 19.3 ns | 19.7 ns | 106 ns |
| `SharedCell::get` | 3.75 ns | 2.97 ns | 2.23 ns | 2.13 ns | 10.4 ns |
| `SharedCell::set` | 13.7 ns | 12.2 ns | 3.17 ns | 3.46 ns | 42.8 ns |
| `SharedRegion::get` | 2.91 ns | 1.77 ns | 1.18 ns | 1.17 ns | 5.59 ns |
| `SharedHashMap::len` | 1.91 ns | 1.01 ns | 595 ps | 623 ps | 3.74 ns |
| `SharedHistogram::count` | 4.60 ns | 1.56 ns | 1.17 ns | 1.18 ns | 7.83 ns |

`SharedCell::get` vs an in-process `RwLock<struct>` (Zen+ 3.75 vs 17.5
ns; Zen3 2.2 vs 4.3 ns; the 2012 Mac 10.4 vs 62.1 ns) is the
lock-free-read win that holds on every platform.

---

## Ordered and keyed maps

| Op | Zen+ / Win | Zen2 / Linux | Zen3 / Linux | Zen3 / FreeBSD | Ivy / macOS |
|---|---:|---:|---:|---:|---:|
| `SharedHashMap::get` | 14.7 ns | 12.1 ns | 7.82 ns | 8.13 ns | 34.1 ns |
| `SharedHashMap::insert` | 47.6 ns | 30.6 ns | 15.0 ns | 17.3 ns | 120 ns |
| `SharedBTreeMap::get_hit` (100 keys) | 20.6 ns | 18.1 ns | 10.3 ns | 10.7 ns | 74.8 ns |
| &nbsp;&nbsp;vs `Mutex<BTreeMap>` | 25.8 ns | 20.8 ns | 11.9 ns | 11.7 ns | 90.9 ns |
| `SharedBTreeMap::get_miss` (100 keys) | 19.4 ns | 19.3 ns | 11.9 ns | 10.4 ns | 73.3 ns |
| `SharedBTreeMap::get_hit` (100k keys) | 166 ns | 183 ns | 108 ns | 106 ns | 438 ns |
| &nbsp;&nbsp;vs `Mutex<BTreeMap>` | 97.5 ns | 104 ns | 70.9 ns | 67.9 ns | 328 ns |
| `SharedBTreeMap::iter_ascending` (100) | 255 ns | 171 ns | 112 ns | 107 ns | 1118 ns |
| `SharedLRUCache::get` | 31.7 ns | 22.5 ns | 9.90 ns | 9.70 ns | 109 ns |
| `SharedLRUCache::put` | 268 ns | 192 ns | 139 ns | 151 ns | 634 ns |
| `SharedLRUCache::put_evict` | 176 ns | 106 ns | 52.0 ns | 51.7 ns | 307 ns |

The B-tree, the substrate's ordered-map primitive, beats
`Mutex<BTreeMap>` at 100 keys on every platform - the
mutex lock/unlock dominates at small N - and trails it at 100k keys,
where the in-process map's pointer-direct nodes win over the mmf's
seqlock + position-independent addressing.

---

## Probabilistic sketches

| Op | Zen+ / Win | Zen2 / Linux | Zen3 / Linux | Zen3 / FreeBSD | Ivy / macOS |
|---|---:|---:|---:|---:|---:|
| `SharedBloomFilter::insert` | 54.6 ns | 47.1 ns | 24.7 ns | 26.9 ns | 214 ns |
| `SharedBloomFilter::contains` (hit) | 42.2 ns | 33.2 ns | 20.4 ns | 22.3 ns | 125 ns |
| `SharedBlockedBloomFilter::insert` | 61.1 ns | 45.8 ns | 20.9 ns | 20.4 ns | 211 ns |
| `SharedBlockedBloomFilter::contains` (16M, >L3) | ~115 ns | 133 ns | 49.7 ns | 48.8 ns | 271 ns |
| &nbsp;&nbsp;vs standard Bloom (16M, >L3) | ~251 ns | 264 ns | 106 ns | 103 ns | 509 ns |
| `SharedCountMinSketch::insert` | 64.6 ns | 44.2 ns | 19.5 ns | 19.5 ns | 187 ns |
| `SharedCountMinSketch::estimate` | 25.3 ns | 14.3 ns | 8.27 ns | 8.25 ns | 84.5 ns |
| `SharedHyperLogLog::insert` | 20.0 ns | 16.2 ns | 9.38 ns | 9.71 ns | 66.1 ns |
| `SharedHistogram::percentile_p99` | 213 ns | 32.5 ns | 21.2 ns | 19.3 ns | 347 ns |
| `SharedReservoirSampler::record` (under cap) | 27.2 ns | 16.5 ns | 8.56 ns | 8.92 ns | 90.7 ns |

The blocked Bloom's single-cache-line `contains` is ~2x the standard
filter's scattered-probe `contains` on every platform once the filter
(16M items, ~19 MB) exceeds L3, where the standard filter's `n_hashes`
probes each miss to RAM. Below L3 the two tie (the out-of-order core
overlaps the standard filter's independent probes).

---

## Coordination, clocks, and topology

| Op | Zen+ / Win | Zen2 / Linux | Zen3 / Linux | Zen3 / FreeBSD | Ivy / macOS |
|---|---:|---:|---:|---:|---:|
| `SharedRateLimiter::try_acquire` | 12.7 ns | 8.44 ns | 3.73 ns | 3.94 ns | 32.4 ns |
| `SharedRwLock::try_read` | 18.3 ns | 11.4 ns | 4.84 ns | 4.96 ns | 73.1 ns |
| `SharedRwLock::try_write` | 53.1 ns | 11.4 ns | 3.85 ns | 3.88 ns | 47.4 ns |
| `FenceClock::tick` | 12.7 ns | 11.6 ns | 9.18 ns | 8.28 ns | 21.2 ns |
| `FenceClock::get_local` | 15.0 ns | 8.22 ns | 8.32 ns | 8.27 ns | 21.0 ns |
| `FenceClock::read_fence` | 12.8 ns | 7.87 ns | 6.28 ns | 6.28 ns | 19.3 ns |
| `TopologyMap::read_recommendation` | 2.57 ns | 1.15 ns | 667 ps | 674 ps | 6.62 ns |
| `TopologyMap::fan_out` | 24.2 ns | 12.4 ns | 7.03 ns | 7.61 ns | 56.3 ns |
| `TopologyMap::record_send` | 21.2 ns | 11.7 ns | 5.43 ns | 5.22 ns | 37.3 ns |

---

## Sequences, regions, and graphs

| Op | Zen+ / Win | Zen2 / Linux | Zen3 / Linux | Zen3 / FreeBSD | Ivy / macOS |
|---|---:|---:|---:|---:|---:|
| `SharedBroadcastRing::push` | 66.6 ns | 39.3 ns | 19.6 ns | 19.3 ns | 164 ns |
| `SharedBroadcastRing::recv` | 59.2 ns | 37.4 ns | 17.5 ns | 17.4 ns | 186 ns |
| `SharedBroadcastRing::lag` | 2.17 ns | 1.13 ns | 717 ps | 735 ps | 5.32 ns |
| `SharedLinkedList::push_back` | 30.7 ns | 25.2 ns | 12.1 ns | 16.8 ns | 141 ns |
| `SharedLinkedList::iter` (100) | 378 ns | 259 ns | 167 ns | 171 ns | 1217 ns |
| `SharedRegion::allocate` | 75.4 ns | 39.4 ns | 29.1 ns | 30.5 ns | 137 ns |
| `SharedRegion::alloc_free_cycle` | 17.9 ns | 12.4 ns | 6.49 ns | 6.11 ns | 63.9 ns |
| `SharedGraph::add_node` | 162 µs | 15.8 µs | 13.7 µs | 14.7 µs | 715 µs |
| `SharedGraph::add_edge` | 149 µs | 18.0 µs | 13.8 µs | 14.3 µs | 1.15 ms |
| `SharedGraph::neighbors` (50) | 237 ns | 143 ns | 106 ns | 103 ns | 1172 ns |

The `SharedGraph::add_node` / `add_edge` figures are cold-MMF
measurements: the bench creates a fresh graph per iteration, so the
timed add includes the first-write page-fault on a freshly mapped file.
Windows mmap commit makes that page-fault roughly 10x the Linux cost,
which is the microsecond-scale Zen+ column; the 2012 Mac's mmap
first-touch is slower still (715 µs add_node). Steady-state allocation
into a warm region is the `SharedRegion::allocate` row (75 ns on Zen+).

---

## Method

Each Zen platform ran the full 68-bench Criterion suite plus the two new
primitives' benches (`shared_btree_map`, `shared_blocked_bloom_filter`)
under the same flags. The 2012 Mac ran the same suite minus the
8-thread capacity-adaptive-ring stress bench, which is pathological on a
2-core CPU. MMF files were backed by disk on every host (the benches
that mmap large files need a backing store with free space, not a small
`tmpfs`). The per-primitive docs under `docs/pointers/` carry the full
op tables, contender rationale, and trade-off discussion for the
canonical (Zen+) hardware; this file is the cross-platform summary.
