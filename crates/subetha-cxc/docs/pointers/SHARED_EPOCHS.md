# SharedEpochs

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/claim-1xCAS_per_pin-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Liveness](https://img.shields.io/badge/dead--owner_reap-pid_probe-informational)

A monotonic epoch counter and the pins held against it, shared by every
process mapped onto the table. A writer that supersedes a version
stamps it with the epoch at which it stopped being current; a scan pins
the epoch it started at and reads the version that was current then;
`reclaim_horizon()` says which superseded versions no reader can still
reach.

> **The "snapshot read without stopping the writers" primitive.**
> Writers never wait for a scan. What a long scan costs is retention:
> it holds a low pin, and every version superseded since stays until it
> finishes.

**Not [`EPOCH_BARRIER.md`](EPOCH_BARRIER.md).** `EpochBarrier` is a
phase barrier - peers wait for each other at a numbered phase.
`SharedEpochs` is a visibility clock - nobody waits for anybody, and
the number orders versions rather than phases. They share a word and
nothing else.

**Constraints (read first):**

- **The pins are in the mapping, not in the process.** The counter
  alone would fit in a process-local atomic; the pin table would not.
  The party that reclaims is routinely a different process from the one
  scanning, and a pin table private to one is invisible to the other,
  so that other process drops a version this one is still reading and
  the scan loses rows with nothing reporting it.
- **One slot holds one pin.** `capacity` is the number of pins that may
  be held at once, chosen by the caller at create. `pin()` returns
  `EpochError::PinsExhausted` when every slot is held by a live
  process.
- **A claim is one CAS on one word.** `state` is `PIN_FREE` or the
  epoch biased by one, so no reader of the table sees a half-written
  pin and epoch 0 is still representable.
- **The horizon reads only atomics.** It costs no liveness probe and no
  lock. It samples the counter before the slots, so a pin claimed
  beside it takes an epoch at or above the value returned.
- **A pin whose process died is reaped, not waited on.**
  `reap_dead_pins()` frees every slot whose owner is gone, and `pin()`
  calls it before reporting the table full - so the ordinary path
  recovers without the caller arranging anything.
- **The counter is monotonic and does not wrap in practice**: one tick
  per superseding write, so `u64` outlasts the store.
- **Cross-process backed by MMF**, or `create_anon` for a store that is
  not shared.

---

## Bench evidence

Against a process-local `Mutex<Vec<(Epoch, u32)>>` pin set - the design
this replaces, a mutex over a sorted vector taken on pin and release
only, `first()` as the horizon.

| Op | `SharedEpochs` (mmf) | local `Mutex<Vec>` | relative |
|---|---:|---:|---|
| advance | 8.62 ns | 8.90 ns | tied |
| pin + release | **11.50 ns** | 32.69 ns | **2.84x faster** |
| reclaim_horizon, no pins | 60.37 ns | **17.52 ns** | **3.4x slower** |
| reclaim_horizon, 32 pins | 88.35 ns | **18.01 ns** | **4.9x slower** |

### Reading the trade-offs

1. **A pin is one CAS**; the baseline takes a mutex, binary-searches
   and inserts, then does it again to release.
2. **The horizon is a slot scan**, O(capacity), where the baseline
   reads `first()`. It is the price of putting the pin set somewhere a
   second process can read it: a sorted vector cannot live in a
   mapping.
3. **The shape suits the caller.** A scan pins once at each end and a
   reclaimer reads the horizon when it needs space, so the fast
   operation is the frequent one. A caller that polls the horizon in a
   loop has the ratio the wrong way round.
4. **Cross-process visibility** is the architectural lever, and it is
   why the slower operation is worth its cost.

### Bench audit

- **Fair contender**: the exact shape of the implementation replaced,
  not a strawman.
- **No surplus work in either arm**: the vector is allocated and the
  table mapped once, outside the measured loop.
- **The horizon is measured loaded as well as idle**, because an idle
  table hides the slot scan that is the whole cost.

### What the numbers do NOT show

- **Cross-process visibility.** The baseline cannot do it at any price;
  a pin it holds is invisible to a reclaimer in another process.
- **The dead-owner reap.** It has no baseline, because a process-local
  pin set cannot outlive its process.

---

## API

| Call | Behavior |
|---|---|
| `SharedEpochs::create(path, capacity)` | Obtain the table: initialize when the path does not yet exist, else attach with the counter and every live pin intact. |
| `SharedEpochs::open(path, expected_capacity)` | Attach to an existing table. A different capacity is a `LayoutMismatch`. |
| `SharedEpochs::create_anon(capacity)` | A table private to this process. |
| `epochs.now()` | The epoch a write happening now would stamp. |
| `epochs.advance() -> Epoch` | Advance and return the new epoch. Called by a writer superseding a version. |
| `epochs.pin() -> Result<PinGuard, EpochError>` | Pin the current epoch until the guard drops. |
| `epochs.reclaim_horizon() -> Epoch` | Epochs at or below this have no reader. |
| `epochs.live_pins()` / `epochs.capacity()` | Pins outstanding; pins that may be held at once. |
| `epochs.reap_dead_pins() -> usize` | Free every slot whose owning process is gone; returns how many. |
| `guard.epoch()` | The pinned epoch. |
| `guard.sees(superseded_at: Option<Epoch>) -> bool` | Whether a version superseded then is visible to this reader. |
| `epoch_file_size(capacity)` | Bytes the file needs. |

`EpochError` is `PinsExhausted` / `LayoutMismatch` / `IoError`.

---

## Layout

```text
| EpochHeader (64B) | PinSlot 0 (64B) | PinSlot 1 (64B) | ... |

EpochHeader = | magic | capacity | now: AtomicU64 | pad |
PinSlot     = | state: AtomicU64 | owner_pid: AtomicU32 | pad |
```

---

## Worked example

```rust
use subetha_cxc::SharedEpochs;

let epochs = SharedEpochs::create("/tmp/store.epochs", 64)?;

// A writer supersedes a version: stamp the old one and move on.
let died_at = epochs.advance();
old_version.superseded_at = Some(died_at);

// A scanner reads a fixed view while that keeps happening.
let pin = epochs.pin()?;
for v in versions() {
    if pin.sees(v.superseded_at) {
        visit(v);
    }
}
drop(pin);

// A reclaimer drops what no reader can reach.
let horizon = epochs.reclaim_horizon();
versions.retain(|v| v.superseded_at.is_none_or(|e| e >= horizon));
```

---

## Use case patterns

### Pattern: a snapshot scan over a shared store

A scan pins once at its start, filters every record by that pin, and
releases at the end. Writers advance the counter throughout and are
never blocked by it.

### Pattern: one counter for several structures

A store whose records and whose index both carry epoch stamps
reclaims both under one horizon, so an index entry can never outlive
the record it names or be dropped while a scan still needs it.

---

## Common pitfalls

- **Sizing `capacity` to concurrent readers rather than concurrent
  pins.** A reader holding a pin across two scans holds two slots.

- **Treating the horizon as a bound on retention.** It is bounded by
  the OLDEST live pin, so one long scan holds everything superseded
  during it. Retention is set by the slowest reader, not by the write
  rate.

- **Reclaiming against `now()` instead of `reclaim_horizon()`.** They
  agree only when no pin is held, which is exactly the case where the
  distinction does not matter.

- **Expecting a crashed reader to release immediately.** Its slot holds
  the horizon until something calls `reap_dead_pins`, which `pin` does
  on the path that would otherwise report the table full.

---

## References

- Source: `crates/subetha-cxc/src/shared_epochs.rs` (13 unit tests
  covering the horizon with and without readers, the oldest pin
  setting it, two readers at one epoch, visibility at the pin,
  concurrent pins against a writer, a horizon that never passes a pin
  taken beside it, a full table refusing rather than overwriting, a
  dead owner's slot being reaped, and two handles on one file sharing
  both the counter and the pins).
- Bench: `crates/subetha-cxc/benches/shared_epochs.rs` (advance, pin +
  release, and the horizon both idle and loaded, against a
  process-local mutex pin set).
- Sibling primitive: [SHARED_VERSIONED_CHAIN.md](SHARED_VERSIONED_CHAIN.md) -
  MVCC linked list; the same visibility question answered per chain
  rather than per store.
- Not to be confused with: [EPOCH_BARRIER.md](EPOCH_BARRIER.md) - a
  phase barrier, where peers do wait for each other.
