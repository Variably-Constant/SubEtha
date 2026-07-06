# HeartbeatTable

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/per_slot-64B_cache_line-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Read](https://img.shields.io/badge/snapshot-SeqLock_retry-informational)

Per-process heartbeat slots stored in an MMF. Each participating
process claims one cache-line-aligned `HeartbeatSlot` via CAS;
beats are one Acquire load of the global epoch plus one Release
store to the slot's `last_seen_epoch`. The watchdog reads slots
via SeqLock retry to detect dead peers. Each slot is 64 bytes -
one cache line - so cross-process beats never false-share.

> **The "cross-process liveness primitive" everyone else builds
> from."** `beat` at **744 ps** vs `Mutex<table>` 16.85 ns
> (**22.6x faster**) and per-slot-mutex 33.08 ns (**44x faster**).
> The hot path is two atomic ops (load global + store last_seen)
> to one cache line, both in L1 after warmup. `snapshot` at
> 8.42 ns via SeqLock retry vs ~18.5 ns for both mutex baselines
> (2.2x faster). Architectural lever: the same protocol works
> cross-process via the MMF substrate; mutex baselines cannot.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **One slot per process**: `register(pid)` returns a slot
  index; the caller stores it for `beat(slot)` / `unregister`.
- **`#[repr(C, align(64))]` per slot**: 64 bytes = one cache
  line; cross-process writes to different slots never
  false-share.
- **SeqLock for snapshot**: writers bump `seq_version` to odd
  (writing) then back to even (committed). Readers retry if the
  version changed between snapshot start and end.
- **Global epoch is one atomic counter**: `tick_global_epoch`
  is `fetch_add(1)`. The watchdog ticks; workers read.
- **`in_flight_bitmap` is 64 bits per slot**: each set bit
  represents one work unit the process is tracking. On failover
  the watchdog reclaims them.
- **`register` is O(capacity) CAS scan**: first empty slot wins
  the CAS race; subsequent contenders fall through to the next
  empty slot.
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [Slot layout](#slot-layout)
- [Operations](#operations)
- [SeqLock snapshot protocol](#seqlock-snapshot-protocol)
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
| HeartbeatHeader (64B)     |
|   magic, capacity         |
|   epoch (AtomicU64)       |   <- watchdog ticks; everyone reads
+---------------------------+
| HeartbeatSlot[0]  (64B)   |   <- one cache line each
|   pid (AtomicU32)         |
|   seq_version (AtomicU32) |
|   last_seen_epoch (U64)   |
|   in_flight_bitmap (U64)  |
|   role (AtomicU32)        |
+---------------------------+
| HeartbeatSlot[1]  (64B)   |
| ...                       |
| HeartbeatSlot[N-1]        |
+---------------------------+
```

Total file size: `64 + capacity * 64` bytes. A 64-slot table is
4 KB plus the header - one page.

---

## Slot layout

| Field | Type | Purpose |
|---|---|---|
| `pid` | `AtomicU32` | 0 = vacant; non-zero = owning process |
| `seq_version` | `AtomicU32` | SeqLock counter; odd = writer in progress, even = stable |
| `last_seen_epoch` | `AtomicU64` | global epoch at most recent `beat` |
| `in_flight_bitmap` | `AtomicU64` | bits for 64 in-flight work units |
| `role` | `AtomicU32` | 0 = worker, 1 = coordinator |
| `_pad` | `[u8; 36]` | pad to 64B cache line |

The 64-byte alignment is the load-bearing invariant: process A
beating slot 0 never touches the same cache line as process B
beating slot 1.

---

## Operations

| Op | Cost | What it does |
|---|---|---|
| `register(pid)` | O(capacity) CAS scan | Claim first empty slot |
| `unregister(idx)` | 2 atomic stores | Zero pid + bitmap |
| `beat(idx)` | 1 load + 1 store | Advance `last_seen_epoch` to global |
| `tick_global_epoch()` | 1 fetch_add | Bump the global counter |
| `mark_in_flight(idx, bit)` | 1 fetch_or | Set work-unit bit |
| `clear_in_flight(idx, bit)` | 1 fetch_and | Clear work-unit bit |
| `snapshot(idx)` | SeqLock retry | Consistent read of all fields |

`beat` writes only `last_seen_epoch`. SeqLock version is NOT
bumped on a beat (forcing every observer to retry on
every beat, defeating the SeqLock's purpose). `seq_version` is
bumped only on **structural** changes (`register`, `unregister`,
in-flight bit churn during reclamation). Readers see a torn
`last_seen_epoch` as a single atomic word - there is no tear at
the field level, only across multiple fields. The SeqLock
protects against torn multi-field snapshots, not single-field
loads.

---

## SeqLock snapshot protocol

```text
loop:
   v1 = slot.seq_version.load(Acquire)
   if v1 is odd:                       # writer in progress
       continue
   pid = slot.pid.load(Acquire)
   last = slot.last_seen_epoch.load(Acquire)
   inflight = slot.in_flight_bitmap.load(Acquire)
   role = slot.role.load(Acquire)
   v2 = slot.seq_version.load(Acquire)
   if v1 == v2:
       return snapshot                  # consistent
   # otherwise loop again
```

The retry happens only on structural change (register /
unregister) - a busy worker beating repeatedly does not bump
`seq_version`, so observers never retry due to beats alone.

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/heartbeat.rs`. Captured
2026-06-02 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `HeartbeatTable` (mmf) | `Mutex<table>` naive | `Vec<Mutex<slot>>` per-slot | mmf relative |
|---|---:|---:|---:|---|
| beat | **744 ps** | 16.85 ns | 33.08 ns | **22.6x / 44.5x faster** |
| snapshot | **8.42 ns** | 18.56 ns | 18.43 ns | **2.20x / 2.19x faster** |
| mark_in_flight | **7.08 ns** | 16.92 ns | 16.76 ns | **2.39x / 2.37x faster** |
| register + unregister | 36.12 ns | 34.96 ns | n/a | 1.03x slower (tied) |

### Reading the trade-offs

1. **`beat` is the headline number.** 22.6x faster than the
   global-mutex baseline and 44.5x faster than the per-slot-mutex
   baseline. The per-slot-mutex variant pays TWO locks per beat
   (global_epoch lock + slot lock), so it's actually slower than
   the global-mutex naive version. The mmf path is two atomic
   ops to one cache line that lives in L1 after warmup.
2. **`snapshot` wins 2.2x.** SeqLock retry vs mutex acquire.
   The retry loop never iterates in this bench (no concurrent
   writers), so it's 4 atomic Acquire loads + a version check.
3. **`mark_in_flight` wins 2.4x.** Single `fetch_or` vs
   lock+OR+unlock. The atomic RMW IS the synchronization
   mechanism.
4. **`register + unregister` is tied.** Both pay the O(capacity)
   first-empty-slot scan. The mmf uses lock-free CAS per slot;
   the mutex uses one global lock for the whole scan. Single-
   thread costs are similar; under concurrent register pressure
   the mmf scales (CAS retries) while the mutex serializes.

### Rule 3b bench audit

- **Fair contenders**: TWO baselines.
  `Mutex<(epoch, Vec<NaiveRec>)>` is the textbook naive
  global-lock shape. `Vec<Mutex<NaiveRec>>` with a separate
  global-epoch mutex is the per-slot-mutex shape (the mutex
  equivalent of per-slot atomics).
- **Workload sized for primitive**: 64-slot table (typical
  worker pool); same slot index used for beat / snapshot /
  mark_in_flight to measure per-op cost.
- **register state-mutation pitfall handled**: register +
  unregister CYCLED per iter so the table never fills.
- **No `thread::spawn` inside `b.iter`**: workloads are
  single-threaded.
- **MMF lifecycle managed**: create + ops + drop + remove_file
  per bench.

### What the numbers do NOT show

- **Cross-process beats**: every process beats its own slot in
  its own MMF window. No cross-process synchronization between
  beaters; only the global epoch is contended (and that's only
  read, not written, by beaters).
- **Concurrent register**: under high registration pressure, the
  CAS-based register scales while mutex serializes.
- **SeqLock retry under churn**: a slot undergoing rapid
  register/unregister cycles forces observers to retry. This
  workload measures the no-churn fast path.

---

## Worked examples

### Worker registers, beats, marks work units

```rust
use subetha_cxc::HeartbeatTable;

let t = HeartbeatTable::open("/tmp/hb.bin", 64).unwrap();
let slot = t.register(std::process::id()).unwrap();

loop {
    t.beat(slot);
    if let Some(work_id) = claim_work() {
        t.mark_in_flight(slot, work_id as u8);
        do_work(work_id);
        t.clear_in_flight(slot, work_id as u8);
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
}

t.unregister(slot);   // before clean exit
```

### Observer reads any slot via SeqLock snapshot

```rust
use subetha_cxc::HeartbeatTable;

let t = HeartbeatTable::open("/tmp/hb.bin", 64).unwrap();
for i in 0..t.capacity() {
    if let Some(snap) = t.snapshot(i) {
        println!("pid={} last_seen={} in_flight={:#x}",
            snap.pid, snap.last_seen_epoch, snap.in_flight_bitmap);
    }
}
```

### Daemon ticks the global epoch

```rust
use std::time::Duration;
use subetha_cxc::HeartbeatTable;

let t = HeartbeatTable::open("/tmp/hb.bin", 64).unwrap();
loop {
    let new_epoch = t.tick_global_epoch();
    eprintln!("tick -> {new_epoch}");
    std::thread::sleep(Duration::from_millis(100));
}
```

---

## Use case patterns

### Pattern: cross-process worker pool liveness

Workers register slots, beat continuously, mark in-flight work
units. A scheduler observer reads snapshots to identify dead
workers. The `failover` primitive composes directly on top of
this.

### Pattern: leader-election quorum

A leader-election daemon scans heartbeat slots to find the
lowest-PID-still-alive. The heartbeat IS the liveness source;
the leader election adds the policy.

### Pattern: phase-barrier dead-peer exclusion

`EpochBarrier` counts live peers from the heartbeat table; a
dead peer's stale heartbeat removes it from the barrier count
so the barrier releases without it.

---

## Known limitations

- **Capacity fixed at create**: no auto-grow. Size for the
  worst-case worker count.
- **64 in-flight bits per slot**: a worker can track at most 64
  concurrent work units. Pool design must keep per-worker
  concurrency below this.
- **`register` is O(capacity)**: at very large capacity the
  scan grows linearly. Practical pool sizes are well below the
  point where this matters.
- **SeqLock retry can starve a reader**: pathological
  high-frequency register/unregister churn forces observers to
  retry. Slot churn should be rare relative to beats.
- **No automatic compaction**: dead workers leave their PID
  in the slot. The next worker reusing the slot must call
  `register` again to overwrite via CAS.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Forgetting to `unregister` before process exit.** The slot
  remains marked alive with the dead PID; the watchdog
  eventually reclaims it via grace-window expiration, but cleaner
  is `unregister` on shutdown.

- **Calling `beat` with a stale slot index.** `unregister`
  zeroes the slot's pid but the index is still valid. Beating
  a vacant slot writes to the slot's `last_seen_epoch` but the
  slot has `pid == 0` so the watchdog ignores it. Harmless but
  wasteful.

- **Mixing two HeartbeatTables in the same process.** Each
  process has its own slot in each table; do not assume a
  single global slot. If two heartbeat tables exist for
  different subsystems, register in both.

- **Treating `last_seen_epoch` as wall-clock time.** It is the
  global epoch counter, ticked by the watchdog. Wall-clock
  observability requires a separate clock primitive.

- **Wrapping in a Mutex.** Pointless; the SeqLock + CAS +
  atomic-store protocol is already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/heartbeat.rs` (384 lines, 6 unit
  tests covering register, table-full, beat advancing
  last_seen_epoch, unregister reuse, in-flight bitmap mark+clear,
  and SeqLock snapshot consistency).
- Bench: `crates/subetha-cxc/benches/heartbeat.rs` (beat, snapshot,
  mark_in_flight, register+unregister cycle vs
  `Mutex<table>` and `Vec<Mutex<slot>>`).
- Consumer: [FAILOVER.md](./FAILOVER.md) - watchdog scans
  heartbeat slots for stale entries and reclaims their in-flight
  bitmaps.
- Consumer: [EPOCH_BARRIER.md](./EPOCH_BARRIER.md) - counts live
  peers from the heartbeat to release the barrier without dead
  peers.
- Consumer: [SHARED_LEADER_ELECTION.md](./SHARED_LEADER_ELECTION.md) -
  scans heartbeat slots to find the lowest-live-PID for
  leadership claims.
- Underlying primitive:
  [SHARED_ATOMIC.md](./SHARED_ATOMIC.md) - the atomic-counter
  building block the global_epoch field uses.
