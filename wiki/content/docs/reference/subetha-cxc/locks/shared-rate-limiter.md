---
title: "Shared Rate Limiter"
weight: 30
---

# SharedRateLimiter

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/CAS--token--bucket-lock_free-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process token-bucket rate limiter. Lock-free CAS-based
refill + acquire. Tokens refill at `refill_rate_per_sec`; bucket
caps at `capacity`. `try_acquire(n)` reads current tokens, adds
refill since last refill timestamp, subtracts `n` if available,
CAS-publishes the result. On race, retry. No locks, no
underflow.

> **The "cross-process rate limit at lock-free cost" primitive.**
> try_acquire (full bucket) at **50.01 ns** vs `Mutex<TokenBucket>`
> 62.27 ns (**1.25x faster**). try_acquire (empty bucket, reject
> path) at 51.29 ns vs 61.65 ns (**1.20x faster**). available()
> at 47.57 ns vs 55.20 ns (**1.16x faster**). Architectural
> lever: cross-process token bucket via a single AtomicU64
> packed state.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Packed state in one AtomicU64**: tokens (u32) + last_refill_us
  (u32). Single CAS publishes both atomically.
- **Refill via wall-clock delta**: `elapsed_us * rate /
  1_000_000`. Wrapping handles 71-min u32 wrap.
- **Clamp to capacity**: refill caps so the bucket never exceeds
  caller's configured capacity.
- **`try_acquire(n)` is single CAS, no spin lock**: on race,
  retry up to a bounded count.
- **Cross-process backed by MMF.**

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_rate_limiter.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `SharedRateLimiter` (mmf) | `Mutex<TokenBucket>` | mmf relative |
|---|---:|---:|---|
| try_acquire (full bucket) | **50.01 ns** | 62.27 ns | **1.25x faster** |
| try_acquire (empty bucket / reject) | 51.29 ns | 61.65 ns | 1.20x faster |
| available() | 47.57 ns | 55.20 ns | 1.16x faster |

### Reading the trade-offs

1. **1.20-1.25x faster across all paths.** CAS-based publish vs
   Mutex acquire+release. Roughly the cost of one wall-clock
   read + one atomic CAS.
2. **Cross-process visibility**: every process limits against
   the same bucket. The mutex baseline is in-process only.
3. **No starvation under concurrent acquire**: CAS retries
   converge; no priority inversion.

### Rule 3b bench audit

- **Fair contender**: `Mutex<TokenBucket>` with identical
  refill arithmetic. Both measure the same protocol shape.
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread acquire correctness in source tests.
- **Sizing**: 1M capacity + 1B rate for full-bucket path (never
  drains); 10 capacity + 1 rate for empty-bucket reject path
  (drained pre-bench).
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process limiting**: N processes share one bucket.
  Each acquire racing for the same tokens via lock-free CAS.
- **Concurrent acquire scaling**: CAS contention scales
  sub-linearly with thread count; Mutex baseline serializes.

---

## Worked examples

### Basic rate limit

```rust
use subetha_cxc::SharedRateLimiter;

// 1000 ops/sec sustained, burst capacity 5000.
let r = SharedRateLimiter::create("/tmp/rl.bin", 5000, 1000).unwrap();
for _ in 0..rate_limited_loop {
    if r.try_acquire(1).is_err() {
        std::thread::sleep(std::time::Duration::from_millis(1));
        continue;
    }
    do_work();
}
```

### Cross-process API rate limit

```rust
// All API server processes share one bucket:
let r = SharedRateLimiter::open("/tmp/api-rl.bin", 100_000, 10_000).unwrap();
if r.try_acquire(1).is_err() {
    return Err(RateLimited);
}
handle_request();
```

---

## Use case patterns

### Pattern: cross-process API rate limit

N API server processes enforce a cluster-wide rate limit by
acquiring tokens from one shared bucket.

### Pattern: per-tenant cross-process quota

One SharedRateLimiter per tenant; tenant-isolated rate limits
across all processes serving that tenant.

### Pattern: hot-path admission control

A worker process limits its own internal work rate; tokens
refill at the design point of the consuming downstream.

---

## Known limitations

- **u32 tokens / capacity**: 4 billion tokens cap; practically
  unreachable for rate-limit workloads.
- **u32 timestamp wraps at 71 minutes**: wrapping arithmetic
  handles correctly; do not interpret last_refill_us as an
  absolute time.
- **No blocking acquire**: returns immediately. Callers
  implement their own retry/sleep loop.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Setting rate > capacity.** The bucket fills past
  capacity in one refill cycle; the clamp prevents overflow
  but rates higher than 1 refill/cycle make the limit
  effectively unbounded.

- **Treating `available()` as authoritative for downstream
  admission.** It is a snapshot; a concurrent `try_acquire`
  may consume between read and use. Always use
  `try_acquire(n)` to atomically check-and-take.

- **Wrapping in a Mutex.** Pointless; the CAS protocol IS the
  synchronization.

---

## References

- Source: `crates/subetha-cxc/src/shared_rate_limiter.rs` (534
  lines, unit tests covering acquire / reject / refill /
  clamp / cross-handle visibility).
- Bench: `crates/subetha-cxc/benches/shared_rate_limiter.rs`
  (try_acquire full, try_acquire empty, available vs
  `Mutex<TokenBucket>`).
- Sibling primitive: [SHARED_SEMAPHORE.md](shared-semaphore/) -
  permit-counting variant (no refill).
- Sibling primitive: [SHARED_ATOMIC.md](../atomics/shared-atomic/) -
  the packed AtomicU64 state primitive.
