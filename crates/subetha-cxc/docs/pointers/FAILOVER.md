# FailoverWatchdog

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/scan-O(slots)_atomic_loads-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Recovery](https://img.shields.io/badge/reclaim-in_flight_bitmap-informational)

Scans a `HeartbeatTable` and reclaims in-flight work whose owning
process has stopped beating. Each scan ticks the global epoch by
one and identifies every slot with
`last_seen_epoch < global - grace_epochs` AND a non-zero
in-flight bitmap; those slots are returned in a `ReclaimReport`
so the caller (typically a scheduler) can reassign the work.

> **The "detect and recover from dead workers in one scan"
> primitive.** 64-slot scan: **752.76 ns** vs `Vec<Mutex<u64>>`
> 1.05 µs (**1.39x faster** - lock-free atomic loads per slot
> vs lock-per-slot). Scan cost is O(slots) and **independent of
> dead count** - every slot must be examined to detect staleness.
> The architectural lever is cross-process visibility: the
> mutex baseline is in-process only at any cost.

**Constraints (read first):**

- **Sidecar integration**: `FailoverWatchdog` itself does NOT implement `subetha_sidecar::AdaptiveInstance` because its `'a` borrowed lifetime on the underlying `HeartbeatTable` cannot satisfy the trait's `'static` bound. The per-scan `liveness::OP_SCAN` observation is pushed to the borrowed `HeartbeatTable`'s sidecar ring instead, so any policy attached at the table level still sees scan activity.

- **Borrows an external `HeartbeatTable`**: FailoverWatchdog does
  not own the table; it scans someone else's.
- **`grace_epochs` is the staleness threshold**: a slot must
  miss STRICTLY MORE than `grace_epochs` beats to be reclaimed.
  At lag == grace, the slot is still considered alive.
- **`scan()` ticks the global epoch by one**: each scan call
  advances the comparison baseline. Schedule scans at a fixed
  cadence (e.g., every 100 ms).
- **Dead slots return a snapshot, not a guard**: the
  `ReclaimReport` carries `(slot_idx, HeartbeatSnapshot)` pairs;
  the caller decides how to reassign the work.
- **`clear_dead_bitmap` silences re-reports**: after the caller
  has reassigned the work, clear the bitmap so subsequent scans
  do not re-emit the same slot.
- **Cross-process backed by MMF** (via the heartbeat table).

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
+---------------------------+
| HeartbeatTable            |   <table>.bin
|   slot[0]: pid + last_seen|   (external dependency)
|   slot[1]: ...            |
|   ...                     |
|   global_epoch atomic     |
+---------------------------+
         ^
         |
+--------+------------------+
| FailoverWatchdog          |   stateless wrapper
|   table: &HeartbeatTable  |
|   grace_epochs: u64       |
+---------------------------+
```

The watchdog itself holds no state beyond a reference to the
heartbeat table and the grace threshold. All staleness
information lives in the table; the watchdog is the protocol for
reading and acting on it.

---

## Protocol

### scan()

```text
new_epoch = table.tick_global_epoch()
dead = []
for each slot i in 0..capacity:
   snap = table.snapshot(i)
   if snap.pid != EMPTY:
       lag = new_epoch - snap.last_seen_epoch
       if lag > grace_epochs and snap.in_flight_bitmap != 0:
           dead.push((i, snap))
return ReclaimReport { dead_slots: dead, new_global_epoch: new_epoch }
```

Single-pass linear scan. Each per-slot check is one atomic load
+ subtraction + two comparisons. The strict-greater-than ensures
`lag == grace` is still considered alive - this is the
single-tick safety margin.

### iter_in_flight_bits(bitmap)

```text
for b in 0..64:
   if (bitmap >> b) & 1 == 1:
       yield b
```

Each set bit in the bitmap maps to one in-flight unit of work
the dead process was tracking. The scheduler typically reassigns
each yielded bit to another live worker.

### clear_dead_bitmap(slot_idx)

Zeroes the `in_flight_bitmap` of slot `slot_idx`. The dead-slot
detection requires a non-zero bitmap, so this silences re-reports
of the same dead slot on the next scan.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/failover.rs`. Captured
2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `FailoverWatchdog` (mmf) | `Vec<Mutex<u64>>` naive | Relative |
|---|---:|---:|---:|
| scan 64 slots, all alive | **752.76 ns** | 1.05 µs | **1.39x faster** |
| scan 64 slots, 4 dead | 711.86 ns | n/a | (dead count is free) |
| scan 1024 slots, all alive | 5.33 µs | n/a | ~5 ns/slot |
| iter_in_flight_bits (6 bits set) | 51.41 ns | n/a | ~8.5 ns/bit |

### Reading the trade-offs

The story the numbers tell:

1. **1.39x faster than the mutex baseline.** Every slot check is
   one atomic Acquire load vs a full Mutex lock+unlock per slot.
   The lock-free per-slot read scales with reader-count where the
   mutex baseline serializes.
2. **Dead count is essentially free.** scan_64_all_alive (752
   ns) is statistically indistinguishable from scan_64_4dead
   (712 ns) - the work is O(slots), not O(dead). Every slot must
   be examined to detect staleness, so a few extra dead slots add
   only a small constant per-dead push onto the report vector.
3. **Linear scan scales sublinearly.** 16x more slots (64 → 1024)
   yield 7x more time (752 ns → 5.3 µs) - cache-line streaming
   amortizes the per-slot atomic load cost. The dominant cost
   becomes memory bandwidth rather than per-slot instruction
   count.
4. **The mutex baseline cannot do what FailoverWatchdog does.**
   Cross-process visibility is unavailable to
   `Vec<Mutex<u64>>` at any cost; FailoverWatchdog scans the
   same MMF that any other process can attach to.

### Rule 3b bench audit

- **Fair contender**: `Vec<Mutex<NaiveHeartbeatSlot>>` is the
  naive in-process shape every "lockable per-slot heartbeat"
  implementation lands on without lock-free atomics. Same data
  layout, same scan logic, just mutex-guarded instead of
  atomic-loaded.
- **No `thread::spawn` inside `b.iter`**: single-threaded
  scan workload.
- **MMF lifecycle managed**: heartbeat table file created, slots
  registered + marked + beat, watchdog created, ops run, table
  dropped, file removed.
- **Variable workload sizes**: 64 (typical worker pool), 1024
  (large cluster) - measures the linear-scan cost at two scales.

### What the numbers do NOT show

- **Cross-process scan**: the watchdog in process A scans the
  same heartbeat table that process B's workers beat. The naive
  baseline cannot do this at any cost.
- **Concurrent scan during writes**: workers are beating while
  the watchdog scans. The lock-free protocol means scanners
  never block beaters and vice versa.
- **End-to-end reclaim latency**: dead-detection happens within
  one scan cycle, typically scheduled every 100 ms.

---

## Worked examples

### Basic watchdog loop

```rust
use std::sync::Arc;
use std::time::Duration;
use subetha_cxc::{FailoverWatchdog, HeartbeatTable};

let table = Arc::new(HeartbeatTable::create("/tmp/hb.bin", 64).unwrap());
let watchdog = FailoverWatchdog::with_grace(&table, 3);

loop {
    let report = watchdog.scan();
    for (slot, snap) in &report.dead_slots {
        eprintln!("dead pid={} at slot={}", snap.pid, slot);
        for bit in FailoverWatchdog::iter_in_flight_bits(snap.in_flight_bitmap) {
            reassign_work_unit(bit);
        }
        watchdog.clear_dead_bitmap(*slot);
    }
    std::thread::sleep(Duration::from_millis(100));
}
```

### Cross-process worker pool with failover

```rust
// Process A - daemon scheduler:
let table = Arc::new(HeartbeatTable::create("/tmp/hb.bin", 64).unwrap());
let watchdog = FailoverWatchdog::with_grace(&table, 3);
// ... run scan loop above ...

// Process B - worker:
let table = Arc::new(HeartbeatTable::open("/tmp/hb.bin", 64).unwrap());
let slot = table.register(std::process::id()).unwrap();
loop {
    if let Some(work_id) = claim_work() {
        table.mark_in_flight(slot, work_id as u8);
        table.beat(slot);
        do_work(work_id);
    }
}
```

If process B crashes, its heartbeat lapses; within `grace_epochs`
ticks the watchdog reclaims its in-flight bitmap and reassigns
the work units.

---

## Use case patterns

### Pattern: cross-process worker pool failover

Workers register in the heartbeat table, mark in-flight bits as
they claim work units, and beat regularly. A scheduler process
runs the watchdog; dead workers' bits are reassigned to live
workers.

### Pattern: leader-election integrity checking

The leader-election primitive picks the lowest-live-PID. If the
leader's heartbeat lapses, both the leader-election state and
the failover watchdog see the same staleness; the watchdog
reclaims any in-flight work the dead leader was tracking.

### Pattern: distributed task queue with at-least-once delivery

Workers mark task IDs in their in-flight bitmap when they begin
processing. On crash, the watchdog re-emits those task IDs for
another worker. This gives at-least-once semantics without an
external coordinator.

---

## Known limitations

- **In-flight bitmap caps at 64 bits per slot**: a worker can
  track at most 64 concurrent work units. Pool design must keep
  per-worker concurrency below this.
- **`grace_epochs` is global to the watchdog**: every slot uses
  the same threshold. Per-slot tuning requires multiple
  watchdog instances.
- **`scan` advances the global epoch by one**: if multiple
  watchdog instances scan the same table, they race on the
  global epoch counter. Designate one primary watchdog.
- **Dead-slot reports are one-shot**: clear the bitmap after
  reassignment; otherwise the next scan re-reports the same
  slot.
- **No automatic compaction of dead slot pids**: the dead slot
  remains registered with its PID. The next worker reusing that
  slot must call `register` again to overwrite.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Forgetting to `clear_dead_bitmap` after reassignment.** The
  scan re-reports the same dead slot on every tick until the
  bitmap is cleared. Reassignment loops will duplicate work.

- **Running multiple watchdog instances against the same
  table.** Each `scan` ticks the global epoch; competing
  watchdogs race the counter forward and may report
  inconsistent dead-slot sets. Choose one primary scanner.

- **Setting `grace_epochs` too small.** A worker with slow
  heartbeat cadence is reclaimed prematurely; work units get
  reassigned while the original worker is still processing
  them. Set grace to several times the expected beat interval.

- **Forgetting that `lag == grace` is alive.** The strict-
  greater-than means a worker just barely missing the grace
  window is NOT dead yet. Tune cadence and grace together.

- **Treating the in-flight bitmap as the work specification.**
  The bitmap is 64 bits; it identifies WHICH work units a
  worker is processing, not what those units mean. The
  scheduler owns the bit-to-task mapping.

---

## References

- Source: `crates/subetha-cxc/src/failover.rs` (186 lines, 4 unit
  tests covering all-alive (no dead reports), grace-exceeded
  dead detection, iter_in_flight_bits, and clear_dead_bitmap
  silencing).
- Bench: `crates/subetha-cxc/benches/failover.rs` (scan 64 alive,
  scan 64 with 4 dead, scan 1024 alive, iter_in_flight_bits,
  vs `Vec<Mutex<u64>>` naive baseline).
- Dependency: [HEARTBEAT.md](./HEARTBEAT.md) - the heartbeat
  table FailoverWatchdog scans.
- Sibling primitive:
  [SHARED_LEADER_ELECTION.md](./SHARED_LEADER_ELECTION.md) -
  same heartbeat dependency; leader-election picks
  lowest-live-PID, failover reclaims dead-worker bits.
- Sibling primitive: [EPOCH_BARRIER.md](./EPOCH_BARRIER.md) -
  same heartbeat dependency; barrier excludes dead peers from
  release count, failover reclaims their in-flight work.
