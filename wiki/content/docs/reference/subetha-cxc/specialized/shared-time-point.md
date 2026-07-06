---
title: "Shared Time Point"
weight: 50
---

# SharedTimePointTile&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/16_slot_tile-SIMD_scan-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process versioned-slot tile for snapshot-isolation reads.
16-slot fixed tile; each slot carries a `version: AtomicU64` and
a 56-byte payload. `visible_mask(snapshot)` returns a 16-bit mask
of slots whose version is `<= snapshot` (snapshot-isolation
visibility). The 16-slot fixed shape feeds a live three-way SIMD
scan: `visible_mask` always routes through `simd_visible_mask`,
which runtime-dispatches AVX-512F (native `_mm512_cmple_epu64_mask`,
two 8-lane masks packed to 16 bits), AVX2 (four 4-lane
`cmpgt_epi64` compares with the sign-bit-XOR unsigned trick plus a
`cmpeq` equality boundary), or a scalar fallback on non-x86 /
feature-stripped builds.

> **The "snapshot-isolation tile lifted to cross-process"
> primitive.** `visible_mask` at **23.6 ns** vs `Mutex<Vec>`
> linear scan 17.2 ns on Zen+ R7 2700 (mmf ~1.37x slower, the
> tile's 64-byte-aligned slots vs the Vec's contiguous 256 bytes);
> on an EPYC Genoa VM the mmf scan is **faster** (6.74 ns vs
> 7.78 ns). The isolated SIMD scan itself is **1.58 ns on AVX-512**
> (Genoa). `at()` lookup at 2.44 ns (one atomic load + read). The
> tile buys SeqLock-safe versioned reads + cross-process
> visibility the mutex baseline cannot offer at any price.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **16 slots fixed**: `TILE_CAP = 16`. Sized for SIMD register
  alignment. `create(path)` takes NO capacity argument; the tile
  is always 16 slots.
- **Payload up to 56 bytes per slot** (`SLOT_PAYLOAD = 56`); `T:
  Copy + 'static` with `align_of::<T>() <= 8`. An oversized OR
  over-aligned `T` returns `TileError::PayloadTooLarge` at
  create/open.
- **CAS-based insert**: `insert(version, value)` claims the
  lowest free bit of the one `AtomicU32` occupied bitmap and
  returns the claimed lane index (`Result<usize, TileError>`),
  or `Err(Full)` when all 16 are occupied.
- **`remove(lane)`** clears the occupied bit (a freed lane is
  reused by the next insert); reads after remove see the lane as
  empty.
- **`visible_mask(snapshot)`** returns the 16-bit mask of
  occupied slots whose version <= snapshot (masked with the
  occupied set). `visible_count(snapshot)` is its popcount.
- **`at(lane)`** returns `Some((version, value))` for an occupied
  slot, `None` for an empty or out-of-range lane.
- **`len` / `is_empty` / `is_full`** are popcounts of the
  occupied bitmap (one Acquire load).
- **Static SIMD helpers are public**: `simd_visible_mask`
  (dispatcher) plus `simd_visible_mask_avx512` /
  `simd_visible_mask_avx2` (both `unsafe`, `#[target_feature]`)
  and `simd_visible_mask_scalar` operate on a `&[u64; 16]`
  version array directly. `header()` exposes the raw
  `&TileHeader`; `flush` / `flush_async` persist.
- **`TileError`**: `LayoutMismatch` / `PayloadTooLarge` / `Full`
  / `IoError`.
- **Cross-process backed by MMF.**

---

## Bench evidence

Bench: `crates/subetha-cxc/benches/shared_time_point.rs`. Measured
on Windows 11 / Zen+ R7 2700, criterion at `--measurement-time 2
--warm-up-time 1 --sample-size 30` (middle estimate of each
[low, mid, high] triple).

| Op | `SharedTimePointTile<u64>` (mmf) | `Mutex<Vec<(u64, T)>>` | Relative |
|---|---:|---:|---|
| insert (iter_batched: fresh tile per iter) | ~31 us | 79 ns | setup-dominated (per-iter file create) |
| visible_mask (16-slot scan) | 23.6 ns | 17.2 ns | 1.37x slower |
| visible_count | 27.0 ns | n/a | similar to mask |
| at (resolve known lane) | **2.44 ns** | n/a | one atomic load + read |

### SIMD tiers, isolated (the AVX-512 row Zen+ cannot run)

`simd_visible_mask_{scalar,avx2,avx512}` benched directly on a
pre-gathered `[u64; 16]` (no occupied-load or gather), so each tier
is measured on its own. The `dispatched` row is the runtime CPUID
picker. AVX-512 was captured on an AMD EPYC 9B14 (Genoa) Colab VM;
Zen+ runs only the scalar + AVX2 tiers.

| Tier | Zen+ R7 2700 | EPYC Genoa |
|---|---:|---:|
| `simd_tiers/scalar` | 8.43 ns | 2.69 ns |
| `simd_tiers/avx2` | 5.87 ns | 1.60 ns |
| **`simd_tiers/avx512`** | n/a (no AVX-512) | **1.58 ns** |
| `simd_tiers/dispatched` | 7.21 ns (-> AVX2) | 1.60 ns (-> AVX-512) |

At 16 lanes the tile is small enough that AVX-512 (two 8-u64
loads) only edges out AVX2 (four 4-u64 loads) - 1.58 ns vs 1.60 ns
on Genoa - both ~1.7x faster than the scalar tier. The win is the
contiguous-gather + masked compare, not the lane width at this
tile size; AVX-512 would separate from AVX2 on a larger tile.

### Reading the trade-offs

1. **visible_mask is close to Mutex<Vec>'s linear scan, not far
   behind it**: 23.6 ns vs 17.2 ns on Zen+ (~1.37x), where the Vec
   is cache-contiguous (16 * 16 bytes = 256 bytes) while the mmf's
   64-byte-aligned slots span more cache lines. On Genoa the gap
   reverses - the mmf scan is 6.74 ns vs the mutex's 7.78 ns. The
   tile does NOT pay a large penalty for its cross-process layout.
2. **at() at 2.44 ns**: one atomic load of version + one
   unaligned read of payload. Lookup hot path is very fast.
3. **Cross-process visibility is the architectural lever**:
   any process can call visible_mask on the same tile; the
   mutex baseline cannot at any cost.
4. **The SIMD scan is the live path, not a future option**:
   every `visible_mask` call gathers the 16 versions into a
   stack `[u64; 16]` and runs the runtime-dispatched
   `simd_visible_mask` (AVX-512F mask instruction, AVX2
   sign-XOR compares, or scalar). The 23.6 ns `visible_mask`
   figure already includes the occupied-bitmap load + the gather
   + the SIMD compare; the isolated SIMD step is ~5.9 ns on Zen+
   AVX2 and ~1.6 ns on Genoa AVX-512 (see the SIMD-tiers table).

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<(u64, T)>>` linear scan with
  identical visibility semantics.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
- **Sizing**: 16-slot tile (fixed by primitive); pre-populated
  for visibility benches.
- **MMF lifecycle managed**: per-bench create + ops + drop +
  remove_file.

### What the numbers do NOT show

- **Cross-process visibility**: any process scans the same
  tile; the mutex baseline cannot.
- **SeqLock-safe concurrent insert + read**: versions written
  with Release pair with readers' Acquire loads. No torn
  reads.
- **Snapshot isolation semantics**: visibility is
  monotonic in snapshot; readers see a consistent view
  bounded by their chosen snapshot value.

---

## Worked examples

### Insert + visible scan

```rust
use subetha_cxc::SharedTimePointTile;

let t: SharedTimePointTile<u64> = SharedTimePointTile::create("/tmp/t.bin").unwrap();
t.insert(10, 100).unwrap();   // version 10
t.insert(20, 200).unwrap();   // version 20
t.insert(30, 300).unwrap();   // version 30

let mask_at_25 = t.visible_mask(25);   // bits for versions 10, 20 set
assert_eq!(mask_at_25, 0b011);   // first two slots visible
```

### Cross-process snapshot reader

```rust
let t: SharedTimePointTile<u64> = SharedTimePointTile::open("/tmp/t.bin").unwrap();
let my_snapshot = 100;
let mask = t.visible_mask(my_snapshot);
for lane in 0..16 {
    if mask & (1 << lane) != 0 {
        let (version, value) = t.at(lane).unwrap();
        process(version, value);
    }
}
```

---

## Use case patterns

### Pattern: snapshot-isolated MVCC tile

A storage layer maintains versioned tuples; readers consult
visible_mask against their snapshot version to filter.

### Pattern: cross-process operation log

Each operation gets a monotonic version; readers filter to
operations visible at their chosen point.

### Pattern: write-version tracking

Writer publishes (version, value) tuples; readers walk visible
slots by snapshot version for time-point queries.

---

## Known limitations

- **16 slots fixed**: caller must size up the tile count by
  partitioning keys / using multiple tiles.
- **56-byte payload cap** (`SLOT_PAYLOAD`), `align_of::<T>() <=
  8`: larger or over-aligned `T` is rejected at create/open.
- **Single-tile linear scan over 16 slots**: cache-line
  bounded but trails contiguous-Vec scan at small N.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Treating visible_mask as exact at a writer-in-progress
  boundary.** A version with Release semantics becomes visible
  to readers' Acquire load AFTER the write; brief windows of
  invisibility during write are by design.

- **Sizing one tile for >16 keys.** Partition across multiple
  tiles; each tile holds at most 16 versioned values.

- **Wrapping in a Mutex.** Pointless; the per-slot version +
  Release/Acquire is already the synchronization mechanism.

---

## References

- Source: `crates/subetha-cxc/src/shared_time_point.rs` (537
  lines, 8 unit tests covering empty-tile mask, insert + visible
  at snapshot, fill-to-capacity overflow, remove + lane reuse,
  zero-snapshot boundary, SIMD-matches-scalar across boundary
  snapshots, cross-handle visibility, and disk persistence).
- Bench: `crates/subetha-cxc/benches/shared_time_point.rs`
  (insert, visible_mask, visible_count, at vs `Mutex<Vec>`).
- Underlying primitive: [SHARED_ATOMIC.md](../atomics/shared-atomic/) -
  the AtomicU32 occupied bitmap + per-slot AtomicU64 versions.
- Sibling primitive: [SHARED_FENCE_CLOCK.md](../locks/shared-fence-clock/) -
  cross-process HLC; SharedTimePointTile is the per-slot
  versioned-data layer.
