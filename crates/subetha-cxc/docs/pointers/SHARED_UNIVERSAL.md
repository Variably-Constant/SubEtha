# SharedUniversal&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/migration-Vec_%3C%3E_Map-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Layer-2 cross-process container that migrates between Shared*
backings as the workload shape changes. Vec-backed for
insert-heavy / iteration-heavy patterns; Map-backed for
contains-heavy patterns. `migrate_to(Strategy)` performs the
backing swap with version + generation tracking so readers
re-open the new backing atomically.

> **The "self-tuning container that swaps Vec <-> Map under
> the hood" primitive.** contains on a Vec backing (N=1024)
> at **3.47 µs**; same operation post-migrate-to-Map at
> **359 ns** (**9.66x faster**). Migration cost for 1024
> entries: 794 µs (breaks even after ~250 contains calls).
> insert costs: Vec 15.86 µs, Map 19.70 µs (Vec 1.24x faster
> at insert). Architectural lever: cross-process container
> that picks its backing based on observed access pattern.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + Hash + Eq + 'static`**: keys for Vec & Map.
- **`migrate_to(Strategy)`** creates a new backing at the next
  version; old readers re-open via generation-check on next
  access.
- **Vec backing**: O(N) contains, O(1) push.
- **Map backing**: O(1) average contains, O(1) insert.
- **Version + generation packed in header state**: 32-bit
  version + 32-bit generation. Wrap rejected after ~4B
  migrations to prevent silent overwrite of v=0 backing.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | Vec backing N=1024 | Map backing N=1024 | Relative |
|---|---:|---:|---|
| contains | 3.47 µs | **359 ns** | **9.66x faster on Map** |
| migrate Vec -> Map (one-time) | n/a | 794 µs | break-even after ~225 contains calls |
| insert (256 ops) | 15.86 µs | 19.70 µs | Vec 1.24x faster |

### Reading the trade-offs

1. **contains 9.66x faster after migration.** Vec scan is
   O(N) = 1024 comparisons; Map lookup is O(1) average.
   Migration becomes profitable once a workload accumulates
   ~225 contains calls.
2. **insert is tied or Vec-favored.** Both backings pay
   similar insert cost; Vec is slightly faster because no
   hashing.
3. **Migration cost amortizes over the workload.** A
   contains-heavy phase post-migration recovers the 794 µs
   migration cost in ~225 ops.

### Rule 3b bench audit

- **Same primitive both sides**: same `SharedUniversal<u64>`
  measured under both backing strategies; the comparison is
  the architectural value of migration itself.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
- **Sizing**: N=1024 (representative for "table-sized" data).
- **MMF lifecycle managed**: per-bench create + ops + drop +
  cleanup of all backing files.

### What the numbers do NOT show

- **Cross-process migration**: when one process migrates,
  other processes' next access re-opens the new backing via
  the generation-check. No coordination needed.
- **Migration triggered by policy**: a dispatcher observes
  contains/insert ratio and migrates when contains-heavy
  exceeds a threshold.

---

## Worked examples

### Manual migration

```rust
use subetha_cxc::{SharedUniversal, UniversalStrategy};

let u: SharedUniversal<u64> = SharedUniversal::create("/tmp/u", 1024).unwrap();
for k in 0..1000u64 { u.insert(k).unwrap(); }
// Initially Vec; contains is O(N).
let h1 = u.contains(&999).unwrap();
// Migrate to Map.
u.migrate_to(UniversalStrategy::Map).unwrap();
// Now contains is O(1).
let h2 = u.contains(&999).unwrap();
```

### Cross-process migration

```rust
// Worker process:
let u: SharedUniversal<u64> = SharedUniversal::open("/tmp/u").unwrap();
// migration happens elsewhere; the worker's next op detects via
// version+generation check and re-opens the new backing.
let h = u.contains(&42).unwrap();
```

---

## Use case patterns

### Pattern: self-tuning container

A workload starts insert-heavy (Vec) and shifts to
contains-heavy (Map). A monitor migrates when the access
ratio crosses a threshold.

### Pattern: cross-process backing swap

The migration produces a new versioned backing; readers
re-open via the generation check. No explicit cross-process
coordination needed.

---

## Known limitations

- **Migration costs O(N)**: amortize over hot workload.
- **Version + generation wrap at 2^32 migrations**:
  practically unreachable but the primitive refuses past that.
- **Two backings only**: Vec and Map.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Migrating too eagerly.** The 794 µs migration cost only
  amortizes after ~225 contains calls. Migrate when the
  workload signal is clear, not on every insert.

- **Holding a stale backing handle.** A reader cached the
  Vec-backing handle and missed the migration; the
  refresh_backing_if_stale check re-opens on next access.
  Don't bypass the public API.

- **Wrapping in a Mutex.** Pointless; per-backing's lock-free
  protocol stays the synchronization mechanism.

---

## References

- Source: `crates/subetha-cxc/src/shared_universal.rs` (845
  lines, 16 unit tests covering insert/contains under both
  backings, migration round-trips, version wrap rejection,
  cross-handle visibility, and generation-check refresh).
- Bench: `crates/subetha-cxc/benches/shared_universal.rs`
  (contains vec vs map, migration cost, insert vec vs map).
- Sibling primitive: [SHARED_VEC.md](./SHARED_VEC.md) - the
  Vec backing.
- Sibling primitive: [SHARED_HASH_MAP.md](./SHARED_HASH_MAP.md) -
  the Map backing.
- Sibling primitive:
  [SHARED_VERSIONED_CHAIN.md](./SHARED_VERSIONED_CHAIN.md) -
  the version/generation pattern lifted to a primitive.
