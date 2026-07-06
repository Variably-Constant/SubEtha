---
title: "Shared Once Cell"
weight: 20
---

# SharedOnceCell&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/state_machine-EMPTY%E2%86%92INIT%E2%86%92DONE-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Payload](https://img.shields.io/badge/payload-up_to_56_bytes-informational)

Cross-process write-once cell. State machine: EMPTY -> INITIALIZING
-> INITIALIZED. The first writer to CAS the state from EMPTY to
INITIALIZING wins and performs the write; other writers spin until
INITIALIZED is published.

> **The cross-process `once_cell::sync::OnceCell` primitive.** When
> multiple processes need to converge on a single agreed-upon value
> (cluster ID, generated key, schema version) and exactly one of
> them generates it, SharedOnceCell is the lock-free answer.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`T: Copy + 'static`** (source line 54). Payload up to 56 bytes.
- **Three-state machine** (source lines 28-30): EMPTY (0) /
  INITIALIZING (1) / INITIALIZED (2).
- **CAS-based winner election** (source lines for `set` and
  `get_or_init`): first writer to CAS EMPTY -> INITIALIZING wins
  and performs the memcpy; others spin or wait then read.
- **`set(value) -> bool`**: returns true if this caller won the
  init race, false if another caller already initialized.
- **`get() -> Option<T>`**: non-blocking; returns None if EMPTY or
  INITIALIZING.
- **`get_or_init(F)`**: blocks until INITIALIZED; runs F at most
  once across all processes opening the same file.
- **`open` rejects mismatched payload size**: same LayoutMismatch
  rejection as SharedCell.
- **Cross-process backed by MMF.**

---

## Table of contents

- [What it is](#what-it-is)
- [State machine](#state-machine)
- [Worked examples](#worked-examples)
- [Bench evidence](#bench-evidence)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

`SharedOnceCell<T>` is an MMF-backed write-once cell. Layout
(source lines 32-39):

```rust
#[repr(C, align(64))]
pub struct OnceHeader {
    pub magic: u32,
    pub size: u32,
    pub state: AtomicU8,
    pub _pad_to_payload: [u8; 7],
    pub payload: [u8; 56],
}
```

The `state` byte drives the protocol:

```mermaid
graph LR
    E[EMPTY = 0]
    I[INITIALIZING = 1]
    D[INITIALIZED = 2]

    E -- "CAS by winning writer" --> I
    I -- "winner finishes memcpy + store" --> D
    D -. "terminal; readers consume" .-> D

    classDef empty fill:#1e3a5f,stroke:#5b9bd5,color:#e8f1f5
    classDef init fill:#1f4a3a,stroke:#5cb85c,color:#e8f5e8
    classDef done fill:#5a3a1f,stroke:#d5a05b,color:#f5ede0

    class E empty
    class I init
    class D done
```

---

## State machine

| Op | EMPTY action | INITIALIZING action | INITIALIZED action |
|---|---|---|---|
| `set(v)` | CAS to INITIALIZING; memcpy v; store INITIALIZED; return true | spin or return false | return false |
| `get()` | None | None | Some(value) |
| `get_or_init(F)` | call F(), set, return | spin until INITIALIZED, return | return value |
| `is_initialized()` | false | false | true |

Only the EMPTY -> INITIALIZING transition is racey; the CAS settles it
atomically. INITIALIZING -> INITIALIZED is a single Release store by
the winner.

---

## Worked examples

### Single-process get-or-init

```rust
use subetha_cxc::shared_once_cell::SharedOnceCell;

let cell: SharedOnceCell<u64> = SharedOnceCell::create("/tmp/once.bin").unwrap();
let v = cell.get_or_init(|| {
    // Expensive computation; runs at most once.
    42
});
assert_eq!(v, 42);

// Subsequent calls skip the closure.
let v2 = cell.get_or_init(|| panic!("should not run"));
assert_eq!(v2, 42);
```

### Cross-process initialization race

```rust
use subetha_cxc::shared_once_cell::SharedOnceCell;

// Process A and Process B both try to initialize:
let cell_a: SharedOnceCell<u64> = SharedOnceCell::create("/tmp/race.bin").unwrap();
let cell_b: SharedOnceCell<u64> = SharedOnceCell::open("/tmp/race.bin").unwrap();

// Whichever calls set first wins.
let a_won = cell_a.set(100);
let b_won = cell_b.set(200);

// Exactly one returns true.
assert!(a_won ^ b_won);

// Both see the same final value.
assert_eq!(cell_a.get(), cell_b.get());
```

### Non-blocking probe

```rust
use subetha_cxc::shared_once_cell::SharedOnceCell;

let cell: SharedOnceCell<u64> = SharedOnceCell::open("/tmp/once.bin").unwrap();

// Don't block on init; just check.
match cell.get() {
    Some(value) => println!("ready: {value}"),
    None => println!("not yet initialized"),
}
```

---

## Bench evidence

Bench harness: `crates/subetha-cxc/benches/shared_once_cell.rs`.
Captured 2026-06-01 on Windows 11 / Zen+ R7 2700, Criterion with
`--sample-size=15 --warm-up-time=1 --measurement-time=2`.

| Op | `std::sync::OnceLock<u64>` | `SharedOnceCell<u64>` | Delta |
|---|---:|---:|---:|
| `get()` on initialized cell | 10.08 ns | 10.04 ns | within noise |
| `get_or_init` (already set) | 798 ps | 930 ps | +132 ps |

The architectural claim validates: SharedOnceCell adds MMF-backed
cross-process semantics at near-identical per-op cost vs the
std::sync::OnceLock baseline. The `get` hot path is essentially
tied; `get_or_init` adds the mmap pointer-deref overhead
(~130 ps).

### Rule 3b bench audit

- **Fair contender**: `std::sync::OnceLock<u64>` is the
  std-library baseline for write-once cells.
- **Both backings pre-initialized**: bench measures the
  steady-state hot path after init.
- **Closure never runs**: `get_or_init` is given a panicking
  closure; if either path runs it the bench crashes.

### What the numbers do NOT show

- **Race-to-init cost**: bench measures post-init; the EMPTY ->
  INITIALIZING -> INITIALIZED transition (with N processes
  racing) is exercised in unit tests but not benched.
- **Cross-process visibility**: the bench is in-process.
  Cross-process get-after-init relies on the same MMF + cache
  coherence path as SharedAtomic.

---

## Use case patterns

### Pattern: cross-process schema version

The first process to start sets the schema version; subsequent
processes read it and verify compatibility. No init coordinator
needed.

### Pattern: generated cluster ID

A multi-process cluster needs a unique ID generated once at boot.
SharedOnceCell wins-once semantics elect the generator.

### Pattern: one-shot leader publication

The leader of a leader-election round publishes its identity
(node-id) via SharedOnceCell. Followers read and verify.

### Pattern: expensive initialization sentinel

A computation result (precomputed table, derived config, lookup
index) that any of N processes can generate. SharedOnceCell
ensures it runs once across all of them.

---

## Known limitations

- **56-byte payload limit**: larger values use SHARED_CELL (52
  bytes) or SHARED_VEC. The payload is exclusive of header magic
  + size + state.
- **No reset**: once INITIALIZED, the cell stays INITIALIZED for
  its lifetime. To re-initialize, recreate the file (delete + new
  create).
- **`set` returns false on lost race; no payload returned**:
  callers needing the winning value call `get` after `set`.
- **Spin in `get_or_init` is unbounded**: if the initializing
  writer is killed before completing, other waiters spin
  forever. Production deployment should monitor and recover via
  external supervision.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Calling `set` without checking the return value.** The
  caller may have lost the race; their value is discarded.

- **Treating `get_or_init` as low-latency.** When the cell is
  EMPTY and the closure is expensive, the first caller pays the
  whole cost; subsequent callers either read (fast) or spin
  (waiting for the first caller).

- **Crashing during INITIALIZING.** The state machine is stuck;
  no recovery in the shipped primitive. Wrap the init closure in
  a timeout or external watchdog.

- **Treating the file as immutable.** It is not; deleting the file
  resets the state machine via re-create.

---

## References

- Source: `crates/subetha-cxc/src/shared_once_cell.rs` (343 lines, 7
  unit tests: fresh cell is empty, first set wins / subsequent
  lose, cross-handle init visibility, get_or_init runs closure at
  most once, disk persistence survives reopen, payload too large
  at create, open rejects wrong payload size).
- Sibling primitive: [SHARED_CELL.md](shared-cell/) - the
  multi-write SeqLock cell. SharedCell allows updates;
  SharedOnceCell is write-once.
- Sibling primitive: [SHARED_LEADER_ELECTION.md](../ownership-types/shared-leader-election/) -
  for elections that need lease semantics rather than a single
  one-shot value.
