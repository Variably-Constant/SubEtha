---
weight: 10
---

# `HandshakeHeader`

`HandshakeHeader` is the per-instance state every adaptive primitive
carries at a known offset. It coordinates strategy migrations with
in-flight readers via an RCU/epoch-style generation counter plus
two in-flight slots indexed by generation parity.

## Layout

128 bytes total, 64-byte aligned, two cache lines:

```rust,no_run
#[repr(C, align(64))]
pub struct HandshakeHeader {
    // Cache line 0: read-mostly.
    pub generation: AtomicU32,
    pub strategy_tag: AtomicU32,
    _pad0: [u8; 56],

    // Cache line 1: write-hot.
    pub in_flight: [AtomicU64; 2],
    _pad1: [u8; 48],
}
```

> [!IMPORTANT]
> **Read-mostly and write-hot live on separate cache lines.** The
> generation + strategy_tag pair (line 0) is read on every op by every
> reader. The in_flight pair (line 1) is RMW-stored by every op
> entry and exit. Splitting them prevents false-sharing between the
> readers' cached line-0 copy and the writers' line-1 churn.

## The RCU/epoch double-check pattern

`enter_op` uses the canonical RCU/epoch protocol:

```rust,no_run
pub fn enter_op(&self) -> u32 {
    loop {
        let current = self.generation.load(Ordering::Acquire);
        let slot = (current & 1) as usize;
        self.in_flight[slot].fetch_add(1, Ordering::AcqRel);
        let recheck = self.generation.load(Ordering::Acquire);
        if recheck == current {
            return current;
        }
        // Generation changed between our gen load and our in_flight
        // increment. Our increment is on the wrong slot - undo and
        // retry on the new generation.
        self.in_flight[slot].fetch_sub(1, Ordering::AcqRel);
        core::hint::spin_loop();
    }
}
```

The double-load closes the race where a migration completes (bump +
drain + free) after the first load but before the increment, which
would otherwise leave the reader holding an in-flight slot on a
freed generation. Adds ~1 cycle (a second Acquire load) on the
common no-migration path.

## API surface

```rust,no_run
impl HandshakeHeader {
    pub const fn new() -> Self;

    // Op-side (every primitive op).
    pub fn enter_op(&self) -> u32;          // returns captured generation
    pub fn exit_op(&self, captured_gen: u32);
    pub fn tag(&self) -> u32;                // PIC hot-path read

    // Migration-side (sidecar / coordinator).
    pub fn set_tag(&self, new_tag: u32);     // PIC-only, no gen bump
    pub fn migrate(&self, new_tag: u32) -> u32;  // bump gen + swap tag, returns old gen
    pub fn bump_generation(&self) -> u32;    // gen bump only, tag unchanged
    pub fn drain(&self, generation: u32);    // spin until in_flight[gen & 1] == 0

    // Inspection.
    pub fn in_flight_count(&self, generation: u32) -> u64;
}
```

## When to call which migration method

| Method | When to use |
|---|---|
| `set_tag(new_tag)` | Switching strategy that does NOT require data-layout migration (e.g., wait-strategy in a once-shot primitive). PIC-only update. |
| `migrate(new_tag)` | Switching strategy that DOES require data-layout migration. Bumps generation AND swaps tag atomically. Returns the old gen so the caller can drain it. |
| `bump_generation()` | Data-layout migration with no strategy change (the primitive swaps its internal storage but keeps the same tag). Tag unchanged; returns old gen. |
| `drain(old_gen)` | After `migrate` or `bump_generation`, wait for in-flight readers on the old gen to complete. Spins. |

## The skip-wakeup pattern

`in_flight_count` is the primary load-bearing inspection method.
Primitive coordinators use it to skip wakeups when no waiters
exist. The safe form:

```rust,no_run
// Coordinator side: store state, then SeqCst fence, then load in_flight.
self.state.store(NEW_STATE, Ordering::Release);
core::sync::atomic::fence(Ordering::SeqCst);
if header.in_flight_count(header.generation.load(Ordering::Acquire)) > 0 {
    self.notify_waiters();
}
```

The `Acquire` on the `enter_op` increment + the `SeqCst` fence here
together ensure that any thread that observed the old state has
already incremented `in_flight`, so the coordinator either sees the
increment (and notifies) or knows there is no waiter to notify.

## Test invariants

The unit tests in `crates/subetha-core/src/handshake.rs` assert:

- `size_of::<HandshakeHeader>() == 128` and `align_of == 64`
  (the two-cache-line invariant is structural, not a comment).
- `enter_op` + `exit_op` leave `in_flight[0]` at zero.
- `migrate(new_tag)` bumps generation by 1 AND swaps tag.
- `bump_generation()` bumps generation but leaves tag unchanged.
- `set_tag(new_tag)` swaps tag but leaves generation unchanged.
- `drain(gen)` returns immediately when `in_flight[gen & 1]` is zero.
