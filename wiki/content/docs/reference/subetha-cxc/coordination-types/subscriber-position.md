---
title: "Subscriber Position"
weight: 33
---

# SubscriberPosition

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)

Aeron-inspired MMF-resident position counter for resumable
cross-process subscribers. Wraps a `SharedAtomicU64` so the
counter survives process restart. A subscriber crashes, a fresh
subscriber reopens the position file by path and resumes from the
last acknowledged position.

## API

| Call | Behavior |
|---|---|
| `SubscriberPosition::create(path, initial: u64)` | Create new position file initialised to `initial`. |
| `SubscriberPosition::open(path)` | Reopen existing position file. |
| `position.get() -> u64` | Acquire load. |
| `position.advance(by: u64) -> u64` | Atomic fetch_add; returns new position. |
| `position.set(new: u64)` | Release store. |
| `position.compare_and_set(expected, new) -> Result<u64, u64>` | CAS; Ok(new) on success, Err(actual) on mismatch. |
| `position.counter_handle() -> Arc<SharedAtomicU64>` | Clone the underlying atomic handle. |

## Worked example

```rust,no_run
use subetha_cxc::replay_positions::SubscriberPosition;

// First subscriber: consume 50 items + checkpoint + exit.
{
    let pos = SubscriberPosition::create("/tmp/sub_pos.bin", 0)?;
    for _ in 0..50 {
        // consume one item via your ring of choice...
        pos.advance(1);
    }
    // pos drops; MMF file persists.
}

// Second subscriber: reopen + resume from checkpoint.
let pos = SubscriberPosition::open("/tmp/sub_pos.bin")?;
let resume_from = pos.get();
assert_eq!(resume_from, 50);
// Continue consuming from position 50...
# Ok::<(), std::io::Error>(())
```

## E2E proof

[`examples/subscriber_restart.rs`](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/examples/subscriber_restart.rs)
runs the producer-subscriber-crash-resume lifecycle end-to-end:
sub1 consumes 50 items + checkpoints + crashes, sub2 reopens and
resumes, all 300 items integrity verified.

## When NOT to reach for this

- In-process subscribers that survive only while the process
  lives. The ring's `tail` counter already tracks this.
- Workloads where the producer is faster than the subscriber by
  more than ring capacity. Items wrap before the subscriber
  resumes; SubscriberPosition gives no protection against that.

## References

- Source: `crates/subetha-cxc/src/replay_positions.rs` (200 lines,
  7 unit tests: create/get, advance, set, CAS ok + mismatch,
  reopen-sees-same-position, survives-drop-then-reopen).
  `SubscriberPosition` lives in the `pub mod replay_positions`
  module path (not re-exported at the crate root).
- [`PubSubRing`](../../rings/pubsub-ring/) - the pub/sub primitive
  that uses `SubscriberPosition` for per-subscriber positions.
- [`SharedAtomicU64`](../../atomics/shared-atomic/) - the underlying
  MMF-resident atomic primitive.
