---
title: "Shared Vec"
weight: 10
---

# SharedVec&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/append_+_SeqLock_per_slot-lock_free-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process indexable vector. Bump-pointer `push_back`; per-slot
SeqLock for safe reads concurrent with writes. Fixed capacity at
create. Indexable `get(i)` is lock-free.

> **The "indexable cross-process vector" primitive.** push at
> **23.13 ns** vs `Mutex<Vec>` ~26 ns (tied). **get 1.83x
> faster** than Mutex<Vec> (9.81 ns vs 17.90 ns) and 1.75x
> faster than RwLock<Vec> (vs 17.16 ns). **len 17x faster**
> (1.07 ns vs 18.19 ns) - one atomic load vs full lock cycle.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + 'static`** with `size_of::<T>() <=
  VEC_PAYLOAD_BYTES (52)`; an oversized T returns
  `VecError::PayloadTooLarge` at create/open. (No `Default`
  bound.)
- **Capacity fixed at create**: bump-pointer; `push_back`
  returns Full when exhausted.
- **Per-slot SeqLock**: writers bump version; readers retry on
  torn read.
- **`get(i)` is lock-free**: one Acquire load + SeqLock read.
- **`len()` is one atomic load**.
- **Full surface**: `push_back` / `pop_back` (CAS on len) /
  `get` / `set(i, v)` (bounds-checked positional write) /
  `clear` / `snapshot() -> Vec<T>` / `capacity` / `len` /
  `is_empty` / `flush` / `flush_async`. `VecError` is
  `Full` / `OutOfBounds` / `LayoutMismatch` / `PayloadTooLarge`
  / `IoError`.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | `SharedVec<u32>` (mmf) | `Mutex<Vec<u32>>` | `RwLock<Vec<u32>>` | mmf relative |
|---|---:|---:|---:|---|
| push_back | 23.13 ns | ~26 ns | 18.50 ns | tied with Mutex |
| get(i) | **9.81 ns** | 17.90 ns | 17.16 ns | **1.83x / 1.75x faster** |
| len() | **1.07 ns** | 18.19 ns | n/a | **17x faster** |

### Reading the trade-offs

1. **push_back ties with Mutex<Vec>**: both do a length
   increment + slot write.
2. **get 1.83x faster than Mutex / 1.75x faster than RwLock.**
   SeqLock read vs full lock cycle. Multi-reader scaling is
   even better (uncontended).
3. **len 17x faster**: one atomic load vs Mutex lock + len +
   unlock.
4. **Cross-process visibility** is the architectural lever.

### Rule 3b bench audit

- **Fair contenders**: `Mutex<Vec<T>>` (textbook) +
  `RwLock<Vec<T>>` (reader-optimized).
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread push correctness in source unit tests.
- **Sizing**: 1M capacity (no overflow at criterion's iters);
  pre-populated for get.
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process append + read**: any process pushes; any
  process reads. The mutex baselines cannot.
- **Multi-reader concurrent get scaling**: SeqLock reads don't
  contend; N concurrent readers each at ~10 ns.

---

## Worked examples

### Basic indexable storage

```rust
use subetha_cxc::SharedVec;

let v: SharedVec<u32> = SharedVec::create("/tmp/v.bin", 1024).unwrap();
v.push_back(10).unwrap();
v.push_back(20).unwrap();
assert_eq!(v.get(0), Some(10));
assert_eq!(v.len(), 2);
```

### Cross-process append-only log

```rust
// Writer process:
let v: SharedVec<EventRec> = SharedVec::create("/tmp/log", 1 << 20).unwrap();
for ev in events() { v.push_back(ev).unwrap(); }

// Reader process(es):
let v: SharedVec<EventRec> = SharedVec::open("/tmp/log", 1 << 20).unwrap();
for i in 0..v.len() {
    if let Some(ev) = v.get(i) { process(ev); }
}
```

---

## Use case patterns

### Pattern: cross-process append-only log

Producers push events; observers read by index.

### Pattern: indexable cross-process snapshot

A worker writes a snapshot vector; multiple readers index in.

### Pattern: SharedUniversal backing

SharedUniversal uses SharedVec as its insert-heavy backing
before potentially migrating to SharedHashMap.

---

## Known limitations

- **Bounded capacity at create**: no auto-grow.
- **Append + pop_back/clear**: `pop_back` removes the tail and
  `clear` resets len, but there is no per-index removal (a `set`
  overwrites in place).
- **T: Copy** (payload <= 52 bytes): pointer-bearing T need
  indirection.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Sizing too small**: `push_back` returns Full once
  capacity exhausts.

- **Reading past `len()`**: get(i) returns None; check len
  first or use the safe API.

- **Wrapping in a Mutex.** Pointless; per-slot SeqLock is
  already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/shared_vec.rs` (580 lines, 13
  unit tests covering push/get/len/set/pop_back/snapshot/clear,
  capacity bound, and cross-handle visibility).
- Bench: `crates/subetha-cxc/benches/shared_vec.rs` (push, get,
  len vs `Mutex<Vec>` and `RwLock<Vec>`).
- Consumer:
  [SHARED_UNIVERSAL.md](shared-universal/) - Vec backing
  for insert-heavy phase.
- Sibling primitive: [SHARED_HASH_MAP.md](../maps/shared-hash-map/) -
  keyed cross-process map.
- Sibling primitive: [SHARED_REGION.md](../arenas/shared-region/) -
  typed slot allocator with reuse; Vec is the simpler
  append-only variant.
