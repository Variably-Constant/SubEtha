---
title: "Shared Universal"
weight: 80
---

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
> entries: 794 µs (breaks even after ~255 contains calls,
> i.e. 794 µs / (3.47 µs - 0.359 µs saved per contains)).
> insert costs: Vec 15.86 µs, Map 19.70 µs (Vec 1.24x faster
> at insert). Architectural lever: cross-process container
> that picks its backing based on observed access pattern.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + Eq + 'static`** is the struct bound; `insert`,
  `contains`, `migrate_to`, and `maybe_migrate_by_policy`
  additionally require `T: Hash` (the Map backing keys on it).
- **Two backings**: `SharedVec<T>` (Vec) and
  `SharedHashMap<T, ()>` (Map). Vec is O(N) contains / O(1)
  push; Map is O(1)-average contains / O(1) insert.
- **`migrate_to(target)`** snapshots the current backing,
  creates a new backing file at the next version, restores the
  snapshot, then publishes the new (version, generation,
  strategy) with a single Release CAS. Old readers re-open via
  the generation-check on their next access. SINGLE-WRITER only
  (concurrent migrators race the CAS; the loser orphans its
  backing file).
- **`maybe_migrate_by_policy(contains_to_insert_ratio,
  min_total_ops)`** reads the op histogram and migrates Vec ->
  Map when `contains/max(insert,1) >= ratio` (or Map -> Vec
  below it), but only once total ops exceed `min_total_ops`.
  Returns `Ok(Some(new_strategy))` on migration, `Ok(None)`
  otherwise.
- **Packed state (one AtomicU64)**: `version: u32` (bits 63..32)
  + `generation: u16` (bits 31..16) + `strategy: u16` (bits
  15..0). A migration bumps `version`; when `version` wraps past
  `u32::MAX` the `generation` bumps and `version` resets to 0.
  True exhaustion (both at MAX = 2^48 ~= 281 trillion
  migrations) returns `UniversalError::VersionExhausted`. The
  generation field is what lets a reader detect a version-wrap
  and re-open instead of trusting a reused version number.
- **Observers**: `strategy()` / `strategy_version()` /
  `strategy_generation()` read the live header; `len` /
  `is_empty` / `clear` / `snapshot` / `op_histogram` operate on
  whichever backing is current (each first refreshes a stale
  local handle).
- **`UniversalError`**: `InvalidStrategy` / `IoError` /
  `LayoutMismatch` / `VecError` / `MapError(MapError)` / `Full`
  / `VersionExhausted`.
- **Cross-process backed by MMF** (a `<base>.state.bin` header
  plus a per-version `<base>-g{G}-v{V}-{vec|map}.bin` backing).

---

## Bench evidence

| Op | Vec backing N=1024 | Map backing N=1024 | Relative |
|---|---:|---:|---|
| contains | 3.47 µs | **359 ns** | **9.66x faster on Map** |
| migrate Vec -> Map (one-time) | n/a | 794 µs | break-even after ~255 contains calls |
| insert (256 ops) | 15.86 µs | 19.70 µs | Vec 1.24x faster |

### Reading the trade-offs

1. **contains 9.66x faster after migration.** Vec scan is
   O(N) = 1024 comparisons; Map lookup is O(1) average.
   Migration becomes profitable once a workload accumulates
   ~255 contains calls (the 794 µs cost divided by the
   ~3.11 µs saved per contains).
2. **insert is tied or Vec-favored.** Both backings pay
   similar insert cost; Vec is slightly faster because no
   hashing.
3. **Migration cost amortizes over the workload.** A
   contains-heavy phase post-migration recovers the 794 µs
   migration cost in ~255 ops.

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

### Policy-driven (self-tuning) migration

```rust
let u: SharedUniversal<u64> = SharedUniversal::create("/tmp/u", 1024).unwrap();
for k in 0..50u64 { u.insert(k).unwrap(); }
for _ in 0..500 { u.contains(&3).unwrap(); }   // contains-heavy phase

// Migrate Vec -> Map once contains outnumber inserts by >= 2x and
// total ops exceed 100. Returns Some(Strategy::Map) on migration.
let moved = u.maybe_migrate_by_policy(2.0, 100).unwrap();
assert_eq!(moved, Some(UniversalStrategy::Map));
```

### Cross-process migration

```rust
// Worker process:
let u: SharedUniversal<u64> = SharedUniversal::open("/tmp/u", 1024).unwrap();
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
- **Migration ceiling at 2^48 (~281 trillion)**: a migration
  bumps the u32 version; on version-wrap the u16 generation
  bumps. Only when BOTH saturate does `migrate_to` refuse with
  `VersionExhausted` (practically unreachable).
- **Two backings only**: Vec and Map. (The module documents the
  extension to more backings as out-of-scope for the current
  build.)
- **Single-writer migration**: concurrent migrators race the
  state CAS; the loser orphans its new backing file.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Migrating too eagerly.** The 794 µs migration cost only
  amortizes after ~255 contains calls. Migrate when the
  workload signal is clear (the built-in
  `maybe_migrate_by_policy` gates on a `min_total_ops` floor for
  exactly this reason), not on every insert.

- **Holding a stale backing handle.** A reader cached the
  Vec-backing handle and missed the migration; the
  refresh_backing_if_stale check re-opens on next access.
  Don't bypass the public API.

- **Wrapping in a Mutex.** Pointless; per-backing's lock-free
  protocol stays the synchronization mechanism.

---

## References

- Source: `crates/subetha-cxc/src/shared_universal.rs` (842
  lines, 16 unit tests covering insert/contains under both
  backings, explicit + policy-driven migration, migration
  round-trips, pack/unpack of the packed state, version-wrap
  bumping generation, true exhaustion (`VersionExhausted`),
  cross-handle visibility, and the generation-check refresh
  that re-opens a reader after a version-wrap).
- Bench: `crates/subetha-cxc/benches/shared_universal.rs`
  (contains vec vs map, migration cost, insert vec vs map).
- Sibling primitive: [SHARED_VEC.md](shared-vec/) - the
  Vec backing.
- Sibling primitive: [SHARED_HASH_MAP.md](../maps/shared-hash-map/) -
  the Map backing.
- Sibling primitive:
  [SHARED_VERSIONED_CHAIN.md](shared-versioned-chain/) -
  the version/generation pattern lifted to a primitive.
