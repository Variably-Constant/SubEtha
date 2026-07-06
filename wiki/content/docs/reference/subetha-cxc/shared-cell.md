---
weight: 40
---

# Cells

Cross-process variants of the in-memory cell shape. Each holds a
fixed-size payload with no allocator coupling; updates are
SeqLock-protected for lock-free reads.

## `SharedCell`

Single-slot mutable cell holding up to `PAYLOAD_BYTES = 52`
of inline payload. The header carries a SeqLock version field so
readers can detect torn writes and retry.

```rust,no_run
pub fn create(path: impl AsRef<Path>) -> Result<Self, SharedCellError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedCellError>;

pub fn get(&self) -> T;
pub fn set(&self, value: T);
pub fn version(&self) -> u32;
```

`T` is required to be `Copy + 'static` and to fit in `PAYLOAD_BYTES`.
The SeqLock protocol on the read path is:

```text
loop:
    v1 = version.load(Acquire)
    if v1 & 1 != 0: continue       // writer in progress
    payload = read_payload()
    v2 = version.load(Acquire)
    if v1 == v2: return payload    // consistent
    // otherwise retry
```

Op kinds: `OP_GET = 1`, `OP_SET = 2`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_CELL.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_CELL.md).

## `SharedOnceCell`

The one-shot init analogue. Once-only initialisation with
`get_or_init`-style semantics across processes. The race is
resolved by CAS on the state field; losers wait until the winner
finishes initialising.

State machine:

```rust,no_run
pub const STATE_EMPTY: u8 = 0;
pub const STATE_INITIALIZING: u8 = 1;
pub const STATE_INITIALIZED: u8 = 2;
```

Constructor:

```rust,no_run
pub fn create(path: impl AsRef<Path>) -> Result<Self, SharedOnceError>;
pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedOnceError>;
```

The winning initialiser writes the payload then transitions to
`STATE_INITIALIZED`; losers spin on the state load until they see
the transition, then read the payload. The cross-process variant
of the in-memory busy-spin wait strategy; the parked variants are
not portable across process boundaries (the Condvar lives in one
process).

`ONCE_PAYLOAD_BYTES` caps the inline payload size. Op kinds use
the `cell` module: `OP_GET = 1`, `OP_SET = 2`.

Canonical doc:
[crates/subetha-cxc/docs/pointers/SHARED_ONCE_CELL.md](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ONCE_CELL.md).

## Picking between them

| Need | Primitive |
|---|---|
| Mutable shared state across processes, payload fits in 52 B | `SharedCell` |
| One-shot init across processes | `SharedOnceCell` |

## See also

- [Role-pair selection](../../how-to/role-pair-selection.md) -
  the shared-mutable-cell role pair.
