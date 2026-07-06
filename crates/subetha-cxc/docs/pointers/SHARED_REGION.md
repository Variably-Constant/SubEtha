# SharedRegion&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/bump_+_Treiber_free_list-ABA_safe-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process typed slot allocator backed by an MMF. Bump-pointer
allocation plus an ABA-safe Treiber-stack free list for reuse.
Hands out position-independent `OffsetPtr<T>` (a `u32`
index) that resolves to a `T` slot across processes.

> **The "typed cross-process arena at lock-free cost"
> primitive.** allocate at **48.11 ns** vs `Mutex<Vec>` arena
> 88.90 ns (**1.85x faster**). alloc_free_cycle at **19.62 ns**
> vs 75.37 ns (**3.84x faster**). get at **1.79 ns** vs 17.02 ns
> (**9.5x faster**). Architectural lever: typed cross-process
> arena + position-independent OffsetPtr + generation-parity
> safe-after-free.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + Default + 'static`**: fixed-size slots.
- **Bump-pointer + free-list**: new allocations bump if no free
  slots; freed slots go to Treiber stack and reuse before bump.
- **ABA-safe via counter-packed pointer**: free-list head is
  `(counter, index)` in one AtomicU64; CAS the packed value.
- **OffsetPtr<T>** is a u32 index. Cross-process safe; same
  index resolves to same slot in any handle.
- **Generation parity**: slot reuse bumps a per-slot generation;
  callers can detect stale pointers.
- **Capacity fixed at create**.
- **Cross-process backed by MMF.**

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_region.rs`. Captured
2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `SharedRegion<u64>` (mmf) | `Mutex<Vec<u64>>` arena | Relative |
|---|---:|---:|---|
| allocate (bump path) | **48.11 ns** | 88.90 ns | **1.85x faster** |
| alloc_free_cycle (Treiber free path) | **19.62 ns** | 75.37 ns | **3.84x faster** |
| get (resolve OffsetPtr) | **1.79 ns** | 17.02 ns | **9.5x faster** |

### Reading the trade-offs

1. **allocate 1.85x faster.** One atomic fetch_add (bump) +
   optional Treiber CAS pop. Mutex baseline: lock + Vec push +
   unlock OR lock + free-list pop + slot reuse + unlock.
2. **alloc_free_cycle 3.84x faster.** The free-list hot path:
   one Treiber-push + one Treiber-pop. Pure CAS dominates over
   mutex lock/unlock.
3. **get 9.5x faster.** One unaligned read at a known slot
   offset vs Mutex lock + Vec index + unlock. The lock-free
   read scales to N concurrent readers.

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<T>>` + `Mutex<Vec<usize>>`
  free-list is the textbook in-process typed arena. Identical
  protocol shape.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
  Multi-thread concurrent allocate correctness in source
  unit tests.
- **Sizing**: 1024 slots for cycle bench; recreated per
  measurement window for the pure-bump allocate.
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process arena allocation**: any process can allocate
  from the same region. OffsetPtrs resolve identically in
  every process.
- **Generation-parity safe-after-free**: a stale OffsetPtr
  whose slot has been reused is detectable; the mutex baseline
  has no such marker.
- **Lock-free reads scale to N readers**: get is atomic;
  concurrent readers don't serialize. Mutex baseline does.

---

## Worked examples

### Allocate + use + free

```rust
use subetha_cxc::SharedRegion;

let r: SharedRegion<u64> = SharedRegion::create("/tmp/r.bin", 1024).unwrap();
let ptr = r.allocate(42).unwrap();
assert_eq!(r.get(ptr).unwrap(), 42);
r.set(ptr, 100).unwrap();
r.free(ptr).unwrap();
```

### Cross-process arena

```rust
// Process A:
let r: SharedRegion<u64> = SharedRegion::create("/tmp/r.bin", 1024).unwrap();
let ptr = r.allocate(0xCAFE_BABE).unwrap();
let raw = ptr.as_u32();

// Process B (raw shipped via any IPC):
let r: SharedRegion<u64> = SharedRegion::open("/tmp/r.bin", 1024).unwrap();
let ptr_b = OffsetPtr::new(raw);
assert_eq!(r.get(ptr_b).unwrap(), 0xCAFE_BABE);
```

---

## Use case patterns

### Pattern: cross-process object pool

Allocate typed objects from a shared region; OffsetPtr is the
cross-process handle. Free returns slots to the Treiber stack.

### Pattern: substrate for linked structures

`SharedLinkedList`, `SharedGraph`, `SharedTreiberStack` all build
on top of `SharedRegion` for their node storage. Cross-process
pointer encoding via OffsetPtr.

### Pattern: position-independent index store

A regions of `OffsetPtr<T>` is a cross-process pointer table;
indices stable across processes that map the same file.

---

## Known limitations

- **Bounded capacity at create**: no auto-grow.
- **`T: Copy + Default`**: pointer-bearing T needs region
  indirection.
- **Bump-pointer never reclaims beyond free list**: a
  region that bumps once cannot un-bump (only free slots
  reuse via the Treiber stack).
- **Generation parity bits are 32**: 2^32 reuses before wrap;
  practically unreachable.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Operating on a stale OffsetPtr.** After `free`, the slot
  may be reused. The generation parity flags this; check before
  treating an OffsetPtr as live.

- **Wrapping in a Mutex.** Pointless; the bump + Treiber CAS
  protocol is already concurrency-safe.

- **Sizing the region too small.** Once bump hits capacity AND
  free list is empty, `allocate` returns `Full`. Plan for
  worst-case live count.

---

## References

- Source: `crates/subetha-cxc/src/shared_region.rs` (675 lines,
  15 unit tests covering allocate / get / set / free, free-list
  reuse, generation-parity stale-detect, cross-handle
  visibility, disk persistence, and ABA safety).
- Bench: `crates/subetha-cxc/benches/shared_region.rs` (allocate,
  alloc_free_cycle, get vs `Mutex<Vec>` arena).
- Consumer: [SHARED_LINKED_LIST.md](./SHARED_LINKED_LIST.md) -
  nodes allocated from a SharedRegion.
- Consumer: [SHARED_GRAPH.md](./SHARED_GRAPH.md) - separate
  node and edge regions.
- Consumer: [K_TOWER_CASCADE.md](./K_TOWER_CASCADE.md) - each
  cascade level reads a SharedRegion slot.
- Sibling primitive: [OFFSET_PTR.md](./OFFSET_PTR.md) - the
  u32 handle type returned by allocate.
