---
title: "Shared Versioned Chain"
weight: 90
---

# SharedVersionedChain&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/MVCC--linked_list-CAS_prepend-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process MVCC linked list. Each node holds `(version: u64,
value: T)`. Nodes linked newest-first via `AtomicU32` head +
per-node `next` offsets. `read_at(snapshot)` walks from head
finding the newest version <= snapshot. Same slot-allocator
pattern as `SharedHandleTable`: ABA-free Treiber free list,
atomic CAS for head updates.

> **The "MVCC at lock-free cost" primitive.** push at CAS-cost
> (~30 ns when not setup-dominated). read_at (walk 100 nodes
> for snapshot=50) at **107.19 ns** vs `Mutex<Vec>` reverse-scan
> 43.45 ns (mmf 2.47x slower; linked-list jumps vs Vec
> contiguous). current() at **1.75 ns** vs Mutex baseline 17 ns
> (lock-free head load). len() at **1.04 ns**. Architectural
> lever: cross-process MVCC visibility that Mutex<Vec> cannot
> offer.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + 'static`, payload up to 48 bytes**.
- **Bounded capacity at create**: Treiber-stack free list backs
  reuse after GC (when added).
- **`push(version, value)`**: CAS prepend at head; one slot
  allocate from the Treiber free list. The `version` must be
  STRICTLY GREATER than the current head's version, or push
  returns `ChainError::NonMonotonicVersion` (and `Full` when the
  free list is exhausted).
- **`read_at(snapshot)`**: linear walk newest-first; returns
  first node with version <= snapshot.
- **`current()`**: head read; returns `Option<(u64, T)>` (the
  newest (version, value)).
- **`clear()` resets the chain** (head -> NIL, all nodes returned
  to the free list) so the capacity is reusable.
- **Newest-first order**: read_at walks recent first; older
  snapshots terminate the walk faster than older versions.
- **`ChainError`**: `LayoutMismatch` / `PayloadTooLarge` / `Full`
  / `NonMonotonicVersion` / `IoError`.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | `SharedVersionedChain<u64>` (mmf) | `Mutex<Vec<(u64, T)>>` | Relative |
|---|---:|---:|---|
| push (iter_batched: fresh chain per iter) | varies | varies | setup-dominated |
| read_at(snapshot=50) on 100-node chain | 107.19 ns | 43.45 ns | 2.47x slower |
| current (head read) | **1.75 ns** | n/a | one atomic load |
| len | **1.04 ns** | n/a | one atomic load |

### Reading the trade-offs

1. **read_at 2.47x slower** than Mutex<Vec> reverse-scan. The
   linked-list per-node jumps lose cache locality vs the
   Vec's contiguous reverse-iter.
2. **current at 1.75 ns**: lock-free head + most-recent-node
   read. The Mutex baseline pays ~17 ns for the same.
3. **len at 1.04 ns**: one atomic counter load.
4. **The architectural lever is cross-process MVCC**: snapshot
   reads from any process see consistent versioned history.

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<(u64, T)>>` with reverse-find
  for snapshot. Same MVCC semantics.
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread push correctness in source unit tests.
- **Sizing**: 100-node chain for read_at; iter_batched for
  push (a fresh chain per iter avoids monotonic-version
  exhaustion across batches).
- **MMF lifecycle managed**: per-bench create + ops + drop +
  remove_file.

### What the numbers do NOT show

- **Cross-process MVCC**: any process pushes a versioned entry;
  any process reads at any snapshot version.
- **Lock-free concurrent push**: CAS-based prepend doesn't
  serialize threads; Mutex<Vec> baseline serializes every push.
- **Snapshot-consistent reads**: read_at returns a coherent
  version-bounded snapshot even concurrent with writers.

---

## Worked examples

### Versioned counter

```rust
use subetha_cxc::SharedVersionedChain;

let ch: SharedVersionedChain<u64> = SharedVersionedChain::create("/tmp/c.bin", 1024).unwrap();
ch.push(1, 100).unwrap();
ch.push(5, 200).unwrap();
ch.push(10, 300).unwrap();

// Read at snapshot 7: newest version <= 7 is version 5 (value 200).
assert_eq!(ch.read_at(7), Some(200));
assert_eq!(ch.read_at(15), Some(300));   // newest visible
assert_eq!(ch.read_at(0), None);          // no version yet
```

### Cross-process MVCC

```rust
// Writer:
let ch: SharedVersionedChain<u64> = SharedVersionedChain::open("/tmp/c.bin", 1024).unwrap();
let v = next_version();
ch.push(v, new_value).unwrap();

// Reader at its own snapshot:
let ch: SharedVersionedChain<u64> = SharedVersionedChain::open("/tmp/c.bin", 1024).unwrap();
let snap = my_snapshot();
let value = ch.read_at(snap);
```

---

## Use case patterns

### Pattern: cross-process versioned state

Each state change appends a (version, state) entry; readers
consult `read_at(my_snapshot)` to see the consistent value at
their snapshot version.

### Pattern: MVCC audit trail

History is the chain; older versions remain visible until
explicit GC (when added).

### Pattern: snapshot-isolated configuration

Configuration changes are versioned; readers pin a snapshot
version for consistent reads.

---

## Known limitations

- **Bounded capacity at create**: no auto-grow.
- **No incremental GC**: individual old versions are not
  reclaimed; history accumulates until `clear()` resets the whole
  chain (head -> NIL, all nodes freed).
- **read_at is linear walk**: O(distance from head).
- **Payload up to 48 bytes**.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Sizing capacity too small.** Push returns Full once the
  free list is empty and the bump pointer reaches capacity.

- **Treating read_at(snapshot) as deterministic during
  concurrent push.** A reader concurrent with a push may see
  the pre-push or post-push state but never a partial one;
  the version+next stores are Release-ordered.

- **Wrapping in a Mutex.** Pointless; the CAS-prepend protocol
  is already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/shared_versioned_chain.rs`
  (447 lines, 6 unit tests covering push + read_at, current,
  len, clear, non-monotonic-version rejection, and cross-handle
  visibility). NODE_PAYLOAD_BYTES = 48; `header()` exposes the
  raw `&ChainHeader`.
- Bench: `crates/subetha-cxc/benches/shared_versioned_chain.rs`
  (push, read_at, current, len vs `Mutex<Vec>` scan).
- Sibling primitive:
  [SHARED_HANDLE_TABLE.md](../arenas/shared-handle-table/) -
  same Treiber free-list pattern; HandleTable is the keyed
  variant, VersionedChain is the versioned-list variant.
- Sibling primitive: [SHARED_TIME_POINT.md](shared-time-point/) -
  16-slot tile of versioned values; VersionedChain is the
  unbounded-history linked-list variant.
- Composes with: [SHARED_FENCE_CLOCK.md](../locks/shared-fence-clock/) -
  HLCs provide the version source for cross-process MVCC.
