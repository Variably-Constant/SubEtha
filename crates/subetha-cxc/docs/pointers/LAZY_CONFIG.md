# LazyConfig&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/CAS--once-thundering--herd--safe-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Fast_path](https://img.shields.io/badge/get_loaded-1.65_ns-informational)

Thundering-herd-proof distributed config fetch. Composite over
[`SharedOnceCell<T>`](./SHARED_ONCE_CELL.md): the first caller's
CAS wins, runs the fetcher closure, and publishes; concurrent
losers block until the publish, then read the canonical value.
Across N processes mapping the same file, the fetcher runs at
most ONCE, no matter how many of them call `get_or_fetch`
concurrently.

> **The "config-fetch primitive that beats the thundering herd"
> primitive.** `try_get_loaded` at **10.11 ns** vs
> `std::sync::OnceLock` 10.01 ns (essentially tied) and
> `Mutex<Option<T>>` 17.77 ns (**1.76x faster**). Post-load
> `get_or_fetch_loaded` at 1.65 ns vs std OnceLock 1.06 ns
> (1.55x slower). The architectural lever: same per-op cost
> envelope as the in-process baseline AND cross-process
> visibility AND single-fetch-across-N-processes guarantee.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + Send + Sync + 'static`, fixed payload** sized to
  `SharedOnceCell::PAYLOAD_BYTES` (56 bytes).
- **Fetcher runs at most once across all callers**: the
  CAS-then-fetch protocol from `SharedOnceCell::get_or_init`
  guarantees that exactly one caller transitions
  EMPTY -> INITIALIZING and runs the fetcher; concurrent losers
  block on the INITIALIZING -> INITIALIZED transition.
- **`force_set` for admin override**: bypasses the fetcher; the
  first `force_set` wins (returns true), subsequent ones lose
  (return false) without changing the value.
- **`try_get` returns None when unloaded**: callers needing the
  blocking behavior use `get_or_fetch`; callers preferring to
  poll use `try_get` + retry.
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [Protocol](#protocol)
- [Bench evidence](#bench-evidence)
- [Worked examples](#worked-examples)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

```text
+-----------------------------+
| SharedOnceCell<T>           |   single 64-byte cache line
|   state: EMPTY | INIT       |   atomic state machine
|         | INITIALIZED       |
|   seq_version (SeqLock)     |
|   payload (56 bytes for T)  |
+-----------------------------+
```

LazyConfig is a thin wrapper over `SharedOnceCell`. The cell
holds the entire protocol; LazyConfig adds the
config-fetch-shaped API (`get_or_fetch`, `try_get`,
`is_loaded`, `force_set`).

---

## Protocol

### get_or_fetch(fetcher)

```text
state = load atomic state
if state == INITIALIZED:
   SeqLock-read payload
   return cached value

# CAS race: EMPTY -> INITIALIZING
if CAS(state, EMPTY, INITIALIZING) succeeds:
   value = fetcher()        # I won; run the fetcher
   SeqLock-write payload(value)
   store(state, INITIALIZED, Release)
   return value
else:
   # I lost the CAS; spin on state until it reaches INITIALIZED
   while load(state) != INITIALIZED:
       spin / yield
   SeqLock-read payload
   return cached value
```

The state machine: EMPTY -> INITIALIZING -> INITIALIZED.
Exactly one CAS wins the EMPTY->INITIALIZING transition; that
caller runs the fetcher. Losers block on the
INITIALIZING->INITIALIZED transition.

### try_get()

```text
if load(state) != INITIALIZED:
   return None
SeqLock-read payload
return Some(value)
```

One atomic load + one SeqLock read. No blocking.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/lazy_config.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `LazyConfig` (mmf) | `std::sync::OnceLock` | `Mutex<Option<T>>` | mmf relative |
|---|---:|---:|---:|---|
| try_get loaded | **10.11 ns** | 10.01 ns | 17.77 ns | tied with std, **1.76x faster** than mutex |
| get_or_fetch loaded | 1.65 ns | 1.06 ns | n/a | 1.55x slower than std |
| is_loaded | 1.34 ns | n/a | n/a | one atomic load |

### Reading the trade-offs

1. **`try_get` is tied with `std::sync::OnceLock`.** Both pay
   one atomic load + one read. The MMF SeqLock + cross-process
   visibility costs nothing measurable on the hot read path.
2. **`get_or_fetch` post-load is 1.55x slower than std.** The
   SeqLock retry-on-mismatch loop is the cost of cross-process
   safety against concurrent writers in OTHER processes; std
   OnceLock has no such concern (single-process only).
3. **The mutex baseline pays 1.76x for try_get.** Lock acquire
   + read + lock release vs one atomic load + one SeqLock read.
4. **The architectural lever is what std cannot do at any
   cost.** Cross-process single-fetch guarantee.
   `Mutex<Option<T>>` and `std::sync::OnceLock` are both
   in-process only; they cannot prevent N processes from each
   independently running the fetcher.

### Rule 3b bench audit

- **Fair contenders**: `std::sync::OnceLock` is the textbook
  in-process equivalent; `Mutex<Option<T>>` is the naive
  lock-guarded shape every "lazy cache" lands on without atomics.
- **No `thread::spawn` inside `b.iter`**: all single-threaded.
  Concurrent fetch-once validation lives in the source unit test
  `fetch_runs_at_most_once_under_concurrency` (16 threads,
  asserts fetcher ran exactly once).
- **Sizing**: single-cell primitive, no working-set parameter.
- **MMF lifecycle managed**: create + load via `get_or_fetch`
  + ops + drop + remove_file.

### What the numbers do NOT show

- **Thundering-herd prevention across processes**: the
  architectural claim. N processes call `get_or_fetch`
  concurrently; exactly one runs the fetcher; the other N-1
  read the published value. Neither in-process baseline can
  do this at any cost.
- **Cold-start fetch cost**: the first caller pays the CAS
  + fetcher closure + payload-write cost. After that, every
  reader gets the cached value at try_get speed (10.11 ns).
- **Cross-process contended fetch**: M threads racing the CAS
  in P processes. Exactly one wins; the others spin briefly
  until INITIALIZED. The CAS contention scales with thread
  count but bounded by the spin window.

---

## Worked examples

### Distributed service: load config once per cluster

```rust
use subetha_cxc::LazyConfig;

#[derive(Clone, Copy)]
#[repr(C)]
struct AppConfig {
    max_conns: u32,
    ttl_ms: u32,
    feature_flags: u32,
}

let lc: LazyConfig<AppConfig> = LazyConfig::open("/tmp/app.cfg").unwrap();
let cfg = lc.get_or_fetch(|| {
    // Network fetch from Consul / etcd / DNS.
    fetch_config_from_consul()
});

// Subsequent processes opening the same file see cfg cached.
```

### Polling readers vs blocking readers

```rust
use subetha_cxc::LazyConfig;

let lc: LazyConfig<u64> = LazyConfig::open("/tmp/lc.bin").unwrap();

// Blocking reader: waits if not loaded.
let v = lc.get_or_fetch(|| panic!("expected pre-loaded"));

// Polling reader: returns None if not loaded.
if let Some(v) = lc.try_get() {
    use_value(v);
} else {
    // Not loaded yet; retry later.
}
```

### Admin-override workflow

```rust
use subetha_cxc::LazyConfig;

let lc: LazyConfig<u32> = LazyConfig::create("/tmp/lc.bin").unwrap();

// First admin push wins.
assert!(lc.force_set(42));
assert_eq!(lc.try_get(), Some(42));

// Second admin push loses (config already set).
assert!(!lc.force_set(99));
assert_eq!(lc.try_get(), Some(42));
```

---

## Use case patterns

### Pattern: cross-process config bootstrap

A pool of N processes each call `get_or_fetch(fetch_from_consul)`
at startup. Exactly one runs the network fetch; the others
block briefly then read the published config. The backend
service sees ONE request instead of N.

### Pattern: lazy initialization of expensive resources

Configuration that takes seconds to compute (parsing TLS
certs, building a routing table, hashing a corpus) initialized
on first use; subsequent calls see the cached result at
10.11 ns.

### Pattern: pre-loaded daemon + late-arriving workers

A daemon process calls `force_set` at startup to publish
config. Worker processes started later see `is_loaded() == true`
immediately and skip fetch entirely.

---

## Known limitations

- **Payload size capped at 56 bytes**: same as
  `SharedOnceCell::PAYLOAD_BYTES`. Larger T needs pointer
  indirection or the bigger-payload cell variants.
- **No re-fetch**: once loaded, the cell is permanently
  initialized. Configuration that changes over time needs a
  different primitive (e.g., a versioned cell + a refresh
  protocol).
- **Fetcher panic propagation**: if the winning fetcher
  panics, the cell stays in INITIALIZING and other callers
  spin indefinitely. The fetcher must not panic; wrap any
  fallible work in a Result and have the fetcher return a
  default-or-error value.
- **No timeout on get_or_fetch**: callers blocked behind the
  CAS winner spin forever if the winner stalls. Wrap the
  fetcher in a timeout-respecting closure.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Treating the fetcher as a constructor with side effects.**
  The fetcher runs at most once across all processes; any side
  effect (e.g., logging "I fetched") happens once per cell
  lifetime, not once per process. Move per-process side effects
  outside the closure.

- **Panicking in the fetcher.** The cell stays in INITIALIZING
  forever; subsequent callers spin. Catch errors inside the
  fetcher and return a sentinel value.

- **Mismatched type at open vs create.** The cell stores
  exactly `size_of::<T>()` bytes; opening with a different T
  reads garbage bytes as T. Pin T in a shared spec.

- **Calling `get_or_fetch` from inside the fetcher.** Reentrant
  call deadlocks: the calling thread already holds the
  INITIALIZING state and waits for itself to finish.

- **Wrapping in a Mutex.** Pointless; the CAS + SeqLock
  protocol is already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/lazy_config.rs` (272 lines, 8
  unit tests covering unloaded read, first fetch, cached
  second fetch, 16-thread concurrent fetch-runs-once,
  cross-handle visibility, force_set wins-first then loses,
  disk persistence, and struct config round-trip).
- Bench: `crates/subetha-cxc/benches/lazy_config.rs` (try_get,
  get_or_fetch on loaded config, is_loaded vs
  `std::sync::OnceLock<T>` and `Mutex<Option<T>>`).
- Underlying primitive:
  [SHARED_ONCE_CELL.md](./SHARED_ONCE_CELL.md) - the CAS-based
  3-state machine LazyConfig wraps.
- Sibling primitive: [SHARED_CELL.md](./SHARED_CELL.md) -
  mutable cell; LazyConfig is the write-once specialization.
- Composes with:
  [SHARED_BROADCAST_RING.md](./SHARED_BROADCAST_RING.md) - for
  config change notifications when paired with a versioned
  publish scheme.
