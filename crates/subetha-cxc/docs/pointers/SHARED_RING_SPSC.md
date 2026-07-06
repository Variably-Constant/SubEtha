# SharedRingSpsc

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/protocol-Lamport_1983_SPSC-brightgreen)
![Contract](https://img.shields.io/badge/contract-compile--time_SPSC-success)
![Slot](https://img.shields.io/badge/slot-64B_payload--only-informational)

Dedicated single-producer / single-consumer ring with the SPSC
contract enforced by the type system. Construction returns an
owned ([`Producer`], [`Consumer`]) pair; neither half is `Clone`
or `Sync`, so the compiler refuses to let two threads hold the
producer side (or the consumer side) at the same time. The
underlying storage is a Lamport 1983 SPSC layout: head and tail
counters on separate cache lines, payload-only slots (no per-slot
atomic), one Acquire load + one Release store per push or pop.

> **The "Lamport SPSC + typed pair" primitive.** Producer owns
> `head`, consumer owns `tail`. Each side reads the peer's
> counter with Acquire and publishes its own with Release. Slot
> state is determined entirely by `head` vs `tail`; slots
> themselves are bytes, no per-slot sequence number to maintain.

**Constraints (read first):**

- **Single producer, single consumer**: enforced at compile time
  by `Producer` and `Consumer` being `!Sync + !Clone + Send`.
- **Payload up to `SPSC_PAYLOAD_BYTES = 64`** bytes per slot
  (no per-slot atomic eats into the cache line).
- **Capacity must be a power of 2**.
- **In-process anonymous mode** (`create_anon_pair`) or
  **cross-process file-backed mode** (`create_pair` /
  `open_pair`) - same byte layout, same protocol.

---

## Table of contents

- [What it is](#what-it-is)
- [Lamport 1983 protocol](#lamport-1983-protocol)
- [Worked examples](#worked-examples)
- [Bench evidence](#bench-evidence)
- [Known limitations](#known-limitations)
- [References](#references)

---

## What it is

`SharedRingSpsc` is a factory; the actual handles you use are
`Producer` and `Consumer`. Each handle owns one half of the
contract:

```rust
use subetha_cxc::SharedRingSpsc;

let (producer, consumer) = SharedRingSpsc::create_anon_pair(64)?;

// Producer goes to producer thread, consumer to consumer thread.
let p_handle = std::thread::spawn(move || {
    for i in 0..1000u64 {
        while producer.try_push(&i.to_le_bytes()).is_err() {
            std::hint::spin_loop();
        }
    }
});

let c_handle = std::thread::spawn(move || {
    let mut out = [0u8; 64];
    for _ in 0..1000 {
        while consumer.try_pop(&mut out).is_err() {
            std::hint::spin_loop();
        }
    }
});
```

Cloning `producer` is a compile error. Sharing `&producer` across
threads is a compile error. The SPSC contract that backs the
no-CAS hot path is enforced by the type system rather than caller
discipline.

The producer / consumer halves share one
`subetha_cxc::spsc_ring::SpscRingCore` via `Arc`. The pair is the
ergonomic interface; the core is exposed so other primitives in
this crate (composed `SharedRingMpsc` and `SharedRingMpmc`) can
compose multiple SPSC rings into MPSC / MPMC shapes without
duplicating the storage layout.

---

## Lamport 1983 protocol

```text
+-----------------------------+
| SpscHeader  (3 cache lines) |
|   magic, capacity, slot_size |   (cache line 0)
|   head: AtomicU64            |   (cache line 1, producer-owned)
|   tail: AtomicU64            |   (cache line 2, consumer-owned)
+-----------------------------+
| Slot[0] (64B payload only)  |
| Slot[1]                     |
| ...                         |
+-----------------------------+
```

### Push

1. `head = self.head.load(Relaxed)` - owner-private; no cross-thread contention.
2. `tail = self.tail.load(Acquire)` - read the peer's position to check full.
3. If `head - tail >= capacity`, return `Err(Full)`.
4. Memcpy payload into slot at `head & (capacity - 1)`.
5. `self.head.store(head + 1, Release)` - publish to the consumer.

### Pop

1. `tail = self.tail.load(Relaxed)` - owner-private.
2. `head = self.head.load(Acquire)` - read the peer's position to check empty.
3. If `tail == head`, return `Err(Empty)`.
4. Memcpy payload out of slot at `tail & (capacity - 1)`.
5. `self.tail.store(tail + 1, Release)` - free the slot.

Two cross-thread atomics per op (Acquire load + Release store)
plus one owner-private Relaxed load. The Vyukov MPMC ring needs
four cross-thread atomics for the same op because every slot
carries a sequence number that the protocol maintains on every
push and pop.

### Why `head` and `tail` on separate cache lines

The producer writes `head` every push; the consumer writes `tail`
every pop. Putting them on the same 64-byte cache line means
every producer publish invalidates the consumer's cache line and
vice versa, cratering throughput on hot loops. The header layout
puts each on its own line; the cost is 192 bytes of header
instead of 64, irrelevant for any ring with capacity above ~16.

---

## Worked examples

### In-process anonymous (fastest, single process)

```rust
use std::thread;
use subetha_cxc::SharedRingSpsc;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

let (producer, consumer) = SharedRingSpsc::create_anon_pair(64)?;

let prod_thread = thread::spawn(move || {
    for i in 0..1_000_000u64 {
        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        buf[..8].copy_from_slice(&i.to_le_bytes());
        while producer.try_push(&buf).is_err() {
            std::hint::spin_loop();
        }
    }
});

let cons_thread = thread::spawn(move || {
    let mut out = [0u8; SPSC_PAYLOAD_BYTES];
    let mut sum: u64 = 0;
    let mut got = 0u64;
    while got < 1_000_000 {
        if consumer.try_pop(&mut out).is_ok() {
            sum += u64::from_le_bytes(out[..8].try_into().unwrap());
            got += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    sum
});

prod_thread.join().unwrap();
let sum = cons_thread.join().unwrap();
assert_eq!(sum, (0..1_000_000u64).sum::<u64>());
```

### Cross-process file-backed

Process A (producer side):

```rust
use subetha_cxc::SharedRingSpsc;
let (producer, _consumer) = SharedRingSpsc::create_pair("/tmp/spsc.bin", 64)?;
// _consumer drops; another process attaches via open_pair.
for i in 0..1_000_000u64 {
    while producer.try_push(&i.to_le_bytes()).is_err() {
        std::hint::spin_loop();
    }
}
```

Process B (consumer side):

```rust
use subetha_cxc::SharedRingSpsc;
let (_producer, consumer) = SharedRingSpsc::open_pair("/tmp/spsc.bin", 64)?;
let mut out = [0u8; 64];
loop {
    if consumer.try_pop(&mut out).is_ok() {
        // handle item
    } else {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
```

Cross-process visibility relies on the OS page cache aliasing the
file between the two processes' address spaces. The SPSC contract
holds across processes the same way it does across threads: one
process produces, one process consumes. If either side runs
multiple producers (or multiple consumers) the contract breaks
and the ring corrupts.

---

## Bench evidence

`crates/subetha-cxc/examples/spsc_shootout.rs`, 1,000,000 items
per trial, 16-byte payloads, 1 producer + 1 consumer on separate
threads, busy-spin on Full / Empty, best-of-5 trials with one
warmup pass. Zen+ R7 2700 / Windows 11.

| Variant | Throughput | vs crossbeam |
|---|---:|---:|
| **`SharedRingSpsc::create_anon_pair` (Lamport)** | **62.96 M items/s** | **5.95x** |
| `SharedRing` MPMC (anon, `try_push` / `try_pop`) | 26.76 M items/s | 2.53x |
| `SharedRing` SPSC fast path (anon, `try_push_spsc`) | 23.27 M items/s | 2.20x |
| `SharedRing` MPMC (file) | 19.05 M items/s | 1.80x |
| `SharedRing` SPSC fast path (file) | 18.78 M items/s | 1.78x |
| `crossbeam_channel::bounded(4096)` | 10.71 M items/s | baseline |

The Lamport pair lands 2.71x ahead of the Vyukov-based SPSC fast
path on `SharedRing` because the Vyukov fast path still pays for
the per-slot sequence atomic (it just skips the CAS on
`producer_seq` / `consumer_seq`). Dedicated Lamport storage
drops the per-slot atomic entirely and halves the per-op atomic
budget.

### Rule 3b bench audit

- **Fair contenders**: `crossbeam_channel::bounded` is the
  standard production in-process bounded SPSC channel. Same
  payload size (16 bytes), same capacity (4096), same busy-spin
  loop on Full / Empty across all variants.
- **Single-trial variance is high** on Windows due to scheduler
  noise; best-of-5 with warmup is what stabilises the comparison.
- **Cross-thread, in-process**: this bench runs both sides in the
  same process. Cross-process numbers for the `SharedRing`-backed
  `Channel<u64>` are in
  `docs/cross_process_ipc_results.json` (349 ns one-way).

---

## Known limitations

- **Exactly one producer + exactly one consumer**: the contract is
  type-system-enforced for single-process use (`!Sync + !Clone`).
  For cross-process use, callers must guarantee at most one
  producer process and one consumer process attach to the same
  file; the type system has no visibility across processes.
- **No global FIFO across multiple producers**: this primitive
  has only one producer. For multi-producer fan-in see
  [SHARED_RING_MPSC.md](./SHARED_RING_MPSC.md).
- **Cross-process via `open_pair` requires producer + consumer
  pair**: the constructor always returns both halves. If only one
  side actually attaches per process, the other half drops
  immediately (no harm; the underlying ring stays alive as long
  as either half exists).
- **Crash recovery is "restart the sole producer"**: Lamport has
  no claimed-but-never-published pathology (see SHARED_RING.md's
  Liveness section for the Vyukov contrast). If the sole
  producer dies between writing payload and the Release-store on
  head, head never advances and the consumer never reads the
  partial slot. No `heal_stuck_slot` equivalent is needed.

---

## References

- Source: `crates/subetha-cxc/src/spsc_ring.rs` (Lamport core)
  + `crates/subetha-cxc/src/shared_ring.rs` (`SharedRingSpsc`
  factory + `Producer` / `Consumer` typed pair).
- Bench: `crates/subetha-cxc/examples/spsc_shootout.rs`.
- Ring family siblings (pick by shape):
  [SHARED_RING.md](./SHARED_RING.md) -
  Vyukov MPMC, the global-FIFO override.
  [SHARED_RING_MPSC.md](./SHARED_RING_MPSC.md) -
  composed N Lamport rings for fan-in.
  [SHARED_RING_MPMC.md](./SHARED_RING_MPMC.md) -
  composed N x M Lamport grid for the general MPMC shape.
- Theory: Leslie Lamport, *Specifying Concurrent Program
  Modules*, ACM TOPLAS 5(2), 1983 - the original single-writer /
  single-reader FIFO algorithm this primitive implements.
