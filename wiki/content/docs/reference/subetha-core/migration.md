---
weight: 50
---

# Migration protocol

`subetha_core::migration` ships two RAII guards that wrap the
`HandshakeHeader` enter/exit and migrate/drain primitives.

## `Generation<'a>` - op-side RAII

```rust,no_run
#[must_use = "Generation holds an in-flight slot until it drops"]
pub struct Generation<'a> {
    pub(crate) header: &'a HandshakeHeader,
    pub(crate) value: u32,
}

impl<'a> Generation<'a> {
    pub fn enter(header: &'a HandshakeHeader) -> Self;
    pub fn value(&self) -> u32;
}

impl<'a> Drop for Generation<'a> {
    fn drop(&mut self) {
        self.header.exit_op(self.value);
    }
}
```

`Generation::enter(header)` calls `header.enter_op()` and returns a
guard. `.value()` gives its captured generation `u32`. Drop releases
the in-flight slot.

> [!NOTE]
> **`#[must_use]` is load-bearing.** A statement
> `Generation::enter(&header);` drops its guard at once, releasing the
> in-flight slot before the op it was meant to cover, so a migration
> can drain and free the representation the op then reads. The
> `#[must_use]` lint flags that statement at compile time.

## `MigrationGuard<'a>` - coordinator-side RAII

```rust,no_run
pub struct MigrationGuard<'a> {
    header: &'a HandshakeHeader,
    old_value: u32,
}

impl<'a> MigrationGuard<'a> {
    pub fn begin(header: &'a HandshakeHeader, new_tag: u32) -> Self;
    pub fn wait_quiescent(&self);
    pub fn old_generation(&self) -> u32;
}
```

Owns the migrate-then-drain sequence:

1. `begin(header, new_tag)` calls `header.migrate(new_tag)` (bumps
   generation, swaps tag, returns old generation).
2. `wait_quiescent()` spins on the old generation's in-flight
   counter via `header.drain(old_gen)`.
3. After `wait_quiescent()` returns, the coordinator owns the old
   data exclusively and can reclaim it.

The guard does not auto-drain on drop. The coordinator typically
allocates new state, calls `begin`, allocates and writes the new
layout while old readers drain, then calls `wait_quiescent` and
frees the old layout. The two-call shape exists so the coordinator
can overlap new-allocation work with the in-flight drain.

## The five-step sequence (from the module docstring)

```text
1. Allocate new representation alongside old.
2. Initialize new from a snapshot of old.
3. Bump generation, swap strategy tag - both representations now live.
4. Wait for old generation's in-flight count to drain to zero.
5. Drop old representation.
```

Steps 3 and 4 are what the `MigrationGuard` encapsulates; steps 1,
2, and 5 are the coordinator's job (because the new and old
representations are primitive-specific types the substrate cannot
generically allocate or free).

## Test invariants

The unit tests in `crates/subetha-core/src/migration.rs` assert:

- `Generation::enter(header)` returns a guard whose `.value()` is
  the current generation.
- The guard's `Drop` releases the in-flight slot.
- `header.drain(captured)` returns after the guard drops.
- `MigrationGuard::begin(header, 1)` installs tag 1, and
  `wait_quiescent()` returns with `old_generation()` at 0.

## See also

- [`HandshakeHeader`](handshake.md) - the underlying generation
  counter + in-flight slots.
- [Architecture overview](../../explanation/architecture.md) - where
  the migration protocol fits in the three-layer decomposition.
