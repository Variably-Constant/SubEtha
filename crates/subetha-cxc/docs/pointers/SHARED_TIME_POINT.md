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
visibility). The 16-slot fixed shape supports an AVX2-shaped
SIMD scan when available.

> **The "snapshot-isolation tile lifted to cross-process"
> primitive.** insert at ~50 ns (CAS occupied bit + atomic
> version write). visible_mask at 113 ns vs `Mutex<Vec>`
> linear scan 23.91 ns - the mutex baseline wins on
> single-thread visibility because the Vec is cache-contiguous;
> mmf trades for SeqLock-safe versioned reads + cross-process
> visibility. `at()` lookup at 2.51 ns (one atomic load + read).

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **16 slots fixed**: `TILE_CAP = 16`. Sized for SIMD register
  alignment.
- **Payload up to 56 bytes per slot**.
- **CAS-based insert**: occupied bitmap is one `AtomicU32`;
  insert claims the first empty bit.
- **`visible_mask(snapshot)`** returns 16-bit mask of slots
  whose version <= snapshot.
- **`at(lane)`** returns `(version, value)` for an occupied
  slot.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | `SharedTimePointTile<u64>` (mmf) | `Mutex<Vec<(u64, T)>>` | Relative |
|---|---:|---:|---|
| insert (iter_batched: fresh tile per iter) | varies | varies | setup-dominated |
| visible_mask (16-slot scan) | 113 ns | 23.91 ns | 4.73x slower |
| visible_count | 119 ns | n/a | similar to mask |
| at (resolve known lane) | **2.51 ns** | n/a | one atomic load + read |

### Reading the trade-offs

1. **visible_mask is slower than Mutex<Vec>'s linear scan**:
   the Vec is cache-contiguous (16 * 16 bytes = 256 bytes, 4
   cache lines); the mmf's 64-byte-aligned slots take 16
   cache lines. The tile pays for cross-process layout.
2. **at() at 2.51 ns**: one atomic load of version + one
   unaligned read of payload. Lookup hot path is very fast.
3. **Cross-process visibility is the architectural lever**:
   any process can call visible_mask on the same tile; the
   mutex baseline cannot at any cost.
4. **SIMD-shaped layout is ready**: the 16-slot tile aligns
   with AVX2/AVX-512 vector widths for parallel-version
   comparison when the appropriate SIMD code path is exercised.

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
- **56-byte payload cap**: same as SharedRing/SharedCell.
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

- Source: `crates/subetha-cxc/src/shared_time_point.rs` (548
  lines, 8 unit tests covering insert/remove, visible_mask /
  visible_count at snapshot, at lane access, full-tile
  rejection, cross-handle visibility).
- Bench: `crates/subetha-cxc/benches/shared_time_point.rs`
  (insert, visible_mask, visible_count, at vs `Mutex<Vec>`).
- Underlying primitive: [SHARED_ATOMIC.md](./SHARED_ATOMIC.md) -
  the AtomicU32 occupied bitmap + per-slot AtomicU64 versions.
- Sibling primitive: [SHARED_FENCE_CLOCK.md](./SHARED_FENCE_CLOCK.md) -
  cross-process HLC; SharedTimePointTile is the per-slot
  versioned-data layer.
