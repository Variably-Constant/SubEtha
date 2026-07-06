# SharedSemaphore

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/CAS--based-lock_free-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process counting semaphore. Permits stored in
`AtomicU32`; `try_acquire` is one CAS decrement; `release` is
one CAS increment with overflow rollback. RAII `Permit` guard
auto-releases on drop. Optional `acquire` blocks via a
spin/yield loop on the `wakeup` (AtomicU64 generation, bumped on
release) and `waiters` (AtomicU32) cells.

> **The "cross-process semaphore at lock-free cost" primitive.**
> try_acquire at **2.02 ns** vs `Arc<(Mutex<u32>, Condvar)>`
> 16.84 ns (**8.32x faster**). acquire+release cycle at
> **16.07 ns** vs 46.51 ns (**2.89x faster**). available()
> at **1.19 ns** vs 16.69 ns (**14x faster**). Architectural
> lever: cross-process permit counting at lock-free CAS speed.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`initial` and `max_permits` set at create**.
- **`try_acquire`** is one CAS decrement; non-blocking.
- **`release`** is one CAS increment; rolls back if the
  increment exceeds `max_permits`.
- **RAII `Permit` guard**: drop releases automatically. Use
  `mem::forget(permit)` + manual `release()` to transfer
  ownership across an API boundary.
- **3 MMF files**: count + wakeup + waiters.
- **Cross-process backed by MMF.**

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_semaphore.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `SharedSemaphore` (mmf) | `Arc<(Mutex<u32>, Condvar)>` | Relative |
|---|---:|---:|---|
| try_acquire (uncontended) | **2.02 ns** | 16.84 ns | **8.32x faster** |
| acquire + release (uncontended) | **16.07 ns** | 46.51 ns | **2.89x faster** |
| available() | **1.19 ns** | 16.69 ns | **14x faster** |

### Reading the trade-offs

1. **try_acquire 8.32x faster.** One CAS decrement vs Mutex
   lock + check + decrement + unlock. The CAS dominates.
2. **acquire+release 2.89x faster.** Two CAS ops vs full
   Mutex+Condvar dance.
3. **available 14x faster.** One atomic load vs Mutex lock +
   read + unlock.
4. **Cross-process visibility is unique** to the mmf primitive.

### Rule 3b bench audit

- **Fair contender**: `Arc<(Mutex<u32>, Condvar)>` is the
  textbook in-process semaphore.
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread contended acquire correctness in source tests.
- **MMF lifecycle managed**: create + ops + drop + cleanup of
  3 files.

### What the numbers do NOT show

- **Cross-process permit counting**: any process can acquire
  / release; the mutex baseline cannot.
- **Blocking acquire under contention**: the spin/yield loop
  on `wakeup` + `waiters` provides bounded-latency wakeups
  for cross-process waiters.

---

## Worked examples

### Bounded concurrency

```rust
use subetha_cxc::SharedSemaphore;

let sem = SharedSemaphore::create("/tmp/sem", 4, 4).unwrap();   // 4 permits
let permit = sem.acquire();   // RAII permit
do_bounded_work();
// permit dropped here -> release
```

### Cross-process concurrency limit

```rust
// Each worker process:
let sem = SharedSemaphore::open("/tmp/cluster-sem").unwrap();
let _permit = sem.acquire();   // at most 4 across all processes
serve_request();
```

---

## Use case patterns

### Pattern: cross-process concurrency cap

Limit total concurrent work across all participating processes
to N permits.

### Pattern: bounded-parallelism worker pool

Spawn workers up to N permits; each worker holds a permit while
processing.

### Pattern: barrier-like coordination

Acquire-only-when-N-released for batch fan-out / fan-in.

---

## Known limitations

- **Spin/yield wait**: no kernel parking. High-contention
  workloads burn some CPU.
- **u32 max permits**: 4 billion cap (unreachable in practice).
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Forgetting to release after `mem::forget(permit)`.** The
  guard owns the release; manually forgetting it requires
  manually calling `release()`. Otherwise permits leak.

- **Releasing more than acquired.** `release` rolls back if it
  exceeds `max_permits`; the permit count stays bounded but
  the caller's logic is broken.

- **Wrapping in a Mutex.** Pointless; the CAS protocol IS the
  synchronization.

---

## References

- Source: `crates/subetha-cxc/src/shared_semaphore.rs` (625
  lines, 12 unit tests covering try/acquire/release cycle, RAII
  drop semantics, max_permits overflow rejection,
  cross-handle visibility, and acquire-blocks-until-release).
- Bench: `crates/subetha-cxc/benches/shared_semaphore.rs`
  (try_acquire, acquire+release, available vs
  `Arc<(Mutex<u32>, Condvar)>`).
- Sibling primitive: [SHARED_RW_LOCK.md](./SHARED_RW_LOCK.md) -
  reader-writer specialization (1-writer or N-readers).
- Sibling primitive:
  [SHARED_RATE_LIMITER.md](./SHARED_RATE_LIMITER.md) -
  token-bucket variant with refill.
