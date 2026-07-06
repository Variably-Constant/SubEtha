# SharedFenceClock

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/HLC-Kulkarni_et_al-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Order](https://img.shields.io/badge/total_order-lexicographic-informational)

Hybrid Logical Clock (HLC) lifted to cross-process MMF. Each
participating process registers an HLC slot in a shared table
and publishes its `(physical_us, logical)` HLC on every
meaningful event. Any reader walks the table to compute
`global_fence = max(all slots)` - the timestamp at which all
process events are causally observable. That fence is exactly
what distributed snapshot isolation needs.

> **The "distributed snapshot fence at lock-free cost"
> primitive.** tick at **45.55 ns** vs `Mutex<Hlc>` 68.37 ns
> (**1.50x faster**) and naive `SystemTime` 49.24 ns. get_local
> at **2.01 ns** vs `Mutex<Hlc>` 17.54 ns (**8.72x faster**).
> compute_global_fence walks 16 slots in 71.28 ns (~4.5 ns/slot).
> Architectural lever: lock-free HLC AND cross-process AND
> O(1) fence-publish for dashboards.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **HLC = (physical_us, logical)**: total order via
  lexicographic compare. Causality preserved without
  vector-clock O(N) cost.
- **Per-slot writes are physical-then-logical, both Release**:
  a reader may observe fresh physical with stale logical.
  HLC stays safe because the lexicographic
  `(physical, logical)` order is correct even with one-field
  skew.
- **Hot path is lock-free**: tick = load + max + store. No
  CAS, no spin.
- **`compute_global_fence`** scans every active slot. O(N) in
  capacity. Use `read_global_fence` (O(1) header read) when
  the fence is pre-published.
- **`publish_global_fence`** writes the current max into the
  header field for cheap reads.
- **Capacity fixed at create**: cross-handle opens verify it.
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [HLC update rules](#hlc-update-rules)
- [Bench evidence](#bench-evidence)
- [Worked examples](#worked-examples)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

```text
+---------------------------+
| HlcHeader (64B)           |
|   magic + capacity        |
|   global_fence_physical   |
|   global_fence_logical    |
+---------------------------+
| HlcSlot[0] (64B)          |
|   pid + physical + logical|
| HlcSlot[1]                |
| ...                       |
| HlcSlot[capacity - 1]     |
+---------------------------+
```

One cache line per slot. The header carries the optional
pre-published global fence so dashboards can read it at O(1).

---

## HLC update rules

### tick (per-event)

```text
wall = now()
new_phys = max(prev_phys, wall)
new_log = if new_phys == prev_phys: prev_log + 1
          else: 0
slot.physical.store(new_phys, Release)
slot.logical.store(new_log, Release)
```

### merge(remote) (after receiving a remote HLC)

```text
wall = now()
new_phys = max(prev_phys, remote_phys, wall)
new_log = match new_phys:
   == prev_phys and == remote_phys: max(prev_log, remote_log) + 1
   == prev_phys only:                prev_log + 1
   == remote_phys only:              remote_log + 1
   wall strictly dominates:          0
```

### compute_global_fence

```text
fence = HLC::min
for slot in active slots:
   if slot.hlc > fence: fence = slot.hlc
return fence
```

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_fence_clock.rs`.
Captured 2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `SharedFenceClock` (mmf) | `Mutex<Hlc>` | naive `SystemTime` | mmf relative |
|---|---:|---:|---:|---|
| tick | **45.55 ns** | 68.37 ns | 49.24 ns | **1.50x faster** than mutex |
| get_local | **2.01 ns** | 17.54 ns | n/a | **8.72x faster** than mutex |
| compute_global_fence (16 slots) | 71.28 ns | n/a | n/a | ~4.5 ns/slot |
| read_global_fence (O(1) header) | 9.67 ns | n/a | n/a | dashboard hot path |

### Reading the trade-offs

1. **tick 1.50x faster** than `Mutex<Hlc>`. Acquire-load + max
   + Release-store vs Mutex full cycle. The naive `SystemTime`
   baseline at 49 ns is NOT an HLC: no logical-counter
   causality. Comparison only shows HLC adds ~3% over wall
   clock.
2. **get_local 8.72x faster.** One atomic load vs full lock
   cycle. Observers running at ~2 ns per read scale freely.
3. **compute_global_fence scales linearly.** 16 slots in 71 ns
   = ~4.5 ns/slot. Within L1 cache-line throughput.
4. **read_global_fence is the dashboard pattern**: one O(1)
   header read after a `publish_global_fence` from any process.

### Rule 3b bench audit

- **Fair contenders**: `Mutex<Hlc>` is the in-process textbook
  HLC baseline. `SystemTime` is the naive wall-clock baseline
  most code uses for cross-process timestamps (not HLC).
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread tick correctness is in the source unit tests.
- **Sizing**: 2-slot table for tick/get_local (single producer),
  64-slot for compute_global_fence (with 16 active).
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process tick + fence**: N processes each tick their
  own slot; any process computes the global fence. The mutex
  baseline cannot do this.
- **HLC vs vector-clock cost ratio**: VC is O(N) per event;
  HLC is O(1). At N=100 processes, HLC is ~100x faster per
  event with same causality properties.
- **Clock-skew bounding**: HLC's `max(local, wall)` bounds
  divergence from physical time, unlike pure logical clocks
  that drift unboundedly.

---

## Worked examples

### Single-process timestamping

```rust
use subetha_cxc::SharedFenceClock;

let clk = SharedFenceClock::create("/tmp/hlc.bin", 8).unwrap();
let me = clk.register(std::process::id()).unwrap();
let hlc1 = clk.tick(me);
let hlc2 = clk.tick(me);
assert!(hlc2 > hlc1);   // monotonic
```

### Cross-process distributed snapshot

```rust
// All N processes:
let clk = SharedFenceClock::open("/tmp/hlc.bin", 64).unwrap();
let me = clk.register(std::process::id()).unwrap();
loop {
    let event_hlc = clk.tick(me);
    record_event(event_hlc);
    // ...
}

// Coordinator process taking a snapshot:
let clk = SharedFenceClock::open("/tmp/hlc.bin", 64).unwrap();
let fence = clk.compute_global_fence();
clk.publish_global_fence();   // O(1) read for dashboards
// All events with hlc <= fence are observable in the snapshot.
```

### Merge after receiving a remote HLC

```rust
let remote_hlc = recv_from_other_node();
let merged = clk.merge(me, remote_hlc);
// merged.physical >= max(prev, remote, wall); merged.logical incremented
```

---

## Use case patterns

### Pattern: cross-process distributed snapshot

A coordinator periodically calls `compute_global_fence`. All
events with `hlc <= fence` are causally complete; the snapshot
includes them. Cross-process workers continue producing newer
HLCs that the next fence captures.

### Pattern: causality-preserving event log

Events are timestamped with an HLC; sorting by HLC gives a
total order that respects causality. Log readers across
processes agree on event order without coordination.

### Pattern: hot-standby with timestamp consistency

A standby process tracks the primary's HLC via the fence; on
failover it resumes from a known causally-consistent point
without losing or duplicating events near the failure boundary.

---

## Known limitations

- **N <= ~256 processes**: HLC's per-slot scan dominates
  past that; vector clocks become competitive for exact
  causality at scale. For modest N, HLC's O(1) per event wins.
- **Per-slot write is not SeqLock-protected**: physical-then-
  logical with both Release; a reader may see fresh physical
  with stale logical. The total-order lexicographic compare
  handles this case correctly.
- **Physical clock skew bounds divergence**: when wall clocks
  diverge by more than logical can compensate, HLC drifts
  toward the slower clock. NTP-sync helps.
- **Capacity fixed at create**: no auto-grow.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Treating logical as a per-second counter.** It is a tie-
  breaker for same-physical events, not a sub-second clock.
  Reset to 0 when physical strictly advances.

- **Comparing HLCs across slots without a global fence.** Two
  slots' HLCs are comparable lexicographically but only the
  fence guarantees causally-complete observation.

- **Forgetting to `publish_global_fence` for dashboards.**
  Dashboards calling `compute_global_fence` repeatedly pay
  O(N) scans. Publish periodically and let dashboards read at
  O(1).

- **Skipping `merge` after RPC.** Receiving a remote HLC
  without merging breaks causality: the receiver's events
  appear "before" the sender's even though the receiver saw
  the sender's HLC.

- **Wrapping in a Mutex.** Pointless; the per-slot atomic
  stores are already concurrency-safe under HLC's order
  semantics.

---

## References

- Source: `crates/subetha-cxc/src/shared_fence_clock.rs` (609
  lines, 13 unit tests covering tick monotonicity, merge rules,
  compute_global_fence, cross-handle visibility, and physical-
  domination cases).
- Bench: `crates/subetha-cxc/benches/shared_fence_clock.rs` (tick,
  get_local, compute_global_fence, read_global_fence vs
  `Mutex<Hlc>` and naive `SystemTime`).
- Original: Kulkarni, Demirbas, Madappa, Avva, Leone,
  "Logical Physical Clocks", OPODIS 2014.
- Sibling primitive: [HEARTBEAT.md](./HEARTBEAT.md) -
  liveness-only, no HLC. SharedFenceClock adds the causality
  layer.
- Sibling primitive: [SHARED_ATOMIC.md](./SHARED_ATOMIC.md) -
  the underlying AtomicU64 primitive each slot field uses.
