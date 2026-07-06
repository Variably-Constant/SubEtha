# SharedHandleTable&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/handle-generation_%2B_slot_packed_u64-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Slot](https://img.shields.io/badge/slot-64B_cache--line-informational)

Cross-process ECS-style slotmap. Same architectural shape as the
in-process `AdaptiveHandle::Slotmap`, but the slot table lives in
an MMF so handles are valid across processes. Handles are 64 bits
packing `(generation: u32, slot: u32)`; generation parity (even =
vacant, odd = occupied) gives safe-after-free detection across the
process boundary.

> **The "cross-process ECS handles" primitive.** Insert lands a u64
> handle that any process opening the same file can resolve. Stale
> handles return None without alias-into-new-value risk. ABA-free
> Treiber-stack free list.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + 'static`** plus stable `#[repr(C)]` layout.
- **`SLOT_PAYLOAD_BYTES = 48`**: per-slot payload
  capacity (slot is 64-byte cache line minus header fields).
- **Handle packing**: u64 = `(gen: u32 << 32) | slot: u32`.
  `Handle::NULL = 0` is the reserved sentinel.
- **Generation parity**: even = vacant,
  odd = occupied. Bumped on every insert and every remove.
- **ABA-free Treiber stack free list**:
  head packed as `(counter: u32, slot_idx: u32)`. CAS bumps the
  counter on every push/pop.
- **Each slot is 64 bytes** (`#[repr(C, align(64))]`).
  generation + occupied + next_free + pad + 48-byte payload.
- **Capacity fixed at create**: no auto-grow. Insert returns
  `HandleTableError::Full` past capacity.
- **`open` requires expected_capacity match**.
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [Generation parity](#generation-parity)
- [ABA-free free list](#aba-free-free-list)
- [Worked examples](#worked-examples)
- [Bench evidence](#bench-evidence)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

`SharedHandleTable<T>` is an MMF-backed slotmap. Layout:

```text
+-----------------------------+
| HandleHeader (64B)          |
|   - magic, capacity         |
|   - free_list_head packed   |
|   - live_count              |
+-----------------------------+
| Slot[0] (64B)               |  gen + occupied + next_free + payload
| ...                         |
| Slot[cap - 1]               |
+-----------------------------+
```

Handles encode `(generation, slot_idx)`. `get(handle)` indexes
slot by `handle.slot()`, compares `slot.generation` against
`handle.generation()`, and returns the payload only on match.

---

## Generation parity

Even generation = vacant. Odd = occupied. Each transition bumps
the generation:

- Insert into vacant slot: gen N -> gen N+1 (even -> odd).
- Remove from occupied slot: gen N -> gen N+1 (odd -> even).

A handle from generation N matches only when the slot is currently
at gen N. After remove + re-insert into the same slot the
generation differs by 2, so the original handle is stale.

## ABA-free free list

The free-list head is a packed `(counter: u32, slot_idx: u32)`
CAS'd as one u64. Each pop or push increments the counter, so the
classic ABA scenario (head=A, intermediate=B, restored=A with
different next pointer) is detected by the counter mismatch.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_handle_table.rs`.
Captured 2026-06-01 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `SharedHandleTable` | `RwLock<HashMap<Handle, T>>` |
|---|---:|---:|
| insert | 53.81 ns | 62.71 ns |
| get (live) | 10.90 ns | 33.90 ns |
| get (stale handle) | 10.94 ns | n/a (HashMap can't detect stale) |

SharedHandleTable wins **3.11x** on the get hot path vs a
RwLock<HashMap> baseline. The architectural win: lock-free reads
(generation compare + payload load) vs RwLock acquire + hash
lookup. Stale rejection is the same cost as a live read.

### Rule 3b bench audit

- **Fair contender**: `RwLock<HashMap<Handle, T>>` is the
  textbook "map handle to value with safe-after-free" pattern in
  Rust.
- **Same operation semantics**: handle-based get; insert returns
  new handle.
- **MMF lifecycle managed**.

### What the numbers do NOT show

- **Cross-process visibility**: the bench is in-process. The
  architectural lever (handles valid in OTHER processes) is what
  the RwLock<HashMap> baseline cannot do.
- **Concurrent insert/remove**: bench is single-threaded.
  Multi-thread workloads exercise the ABA-free CAS protocol;
  the source's `concurrent_inserts_and_removes_preserve_count`
  unit test verifies correctness.

---

## Worked examples

### Cross-process handle issuance

Process A:
```rust
use subetha_cxc::shared_handle_table::SharedHandleTable;

let t: SharedHandleTable<u64> = SharedHandleTable::create("/tmp/ht.bin", 1024).unwrap();
let h = t.insert(42).unwrap();

// Write the handle.raw() to a SharedCell or pipe; process B looks it up.
```

Process B:
```rust
let t: SharedHandleTable<u64> = SharedHandleTable::open("/tmp/ht.bin", 1024).unwrap();
// Construct handle from raw bits received from A:
let h = subetha_cxc::shared_handle_table::Handle::from_raw(handle_bits);
assert_eq!(t.get(h), Some(42));
```

### Stale handle rejection

```rust
use subetha_cxc::shared_handle_table::SharedHandleTable;

let t: SharedHandleTable<u64> = SharedHandleTable::create("/tmp/ht2.bin", 16).unwrap();
let h = t.insert(100).unwrap();
assert_eq!(t.get(h), Some(100));

t.remove(h);
assert_eq!(t.get(h), None);  // stale handle returns None

let h2 = t.insert(200).unwrap();
// h2 likely reuses h's slot but with a different generation.
assert_eq!(t.get(h), None);  // original h still stale
assert_eq!(t.get(h2), Some(200));
```

---

## Use case patterns

### Pattern: cross-process entity registry

A scene / world manager keeps entities in a SharedHandleTable;
worker processes look up entities by handle.

### Pattern: connection / session table

Network daemon owns the table; worker processes resolve handles to
session metadata.

### Pattern: capability tokens

Issued handles act as capabilities that can be passed between
processes; stale handles can be revoked by remove + re-insert with
a fresh handle to a new value.

---

## Known limitations

- **Capacity fixed at create**: no auto-grow.
- **Payload capped at 48 bytes per slot**: larger T uses a
  pointer-indirection pattern.
- **`open` requires expected_capacity match**.
- **Generation wrap at 2^32**: extremely high-churn slots (4
  billion inserts on the same slot index) can alias.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Forgetting to check `get` return value before use.** Stale
  handles return None; treating None as a programming error rather
  than expected behavior is wrong.

- **Hardcoding capacity differently between create and open.**
  LayoutMismatch.

- **Storing T that does not have a stable repr(C) layout.**
  Cross-process reads may see scrambled bytes.

- **Treating handles as raw indices.** They are `(gen, slot)`
  packed. A consumer that strips the gen and indexes by slot only
  loses the safe-after-free guarantee.

---

## References

- Source: `crates/subetha-cxc/src/shared_handle_table.rs` (598 lines,
  10 unit tests covering insert/get/remove, stale rejection, full-table
  error, cross-handle visibility, concurrent inserts/removes,
  disk persistence, NULL handle, packing, struct payload).
- Bench: `crates/subetha-cxc/benches/shared_handle_table.rs`
  (insert, get live, get stale vs RwLock<HashMap> baseline).
- Sibling primitive: [OFFSET_PTR.md](./OFFSET_PTR.md) - the
  underlying MMF-backed pointer.
- Sibling primitive: [SHARED_HASH_MAP.md](./SHARED_HASH_MAP.md) -
  cross-process hash map (uses keys instead of handles).
- Sibling primitive: [TAGGED_OFFSET_PTR.md](./TAGGED_OFFSET_PTR.md) -
  offset pointer with extra tag bits for state.
