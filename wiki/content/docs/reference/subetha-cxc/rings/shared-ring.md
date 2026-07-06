---
title: "Shared Ring"
weight: 10
---

# SharedRing

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/protocol-Vyukov_MPMC-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)
![Slot](https://img.shields.io/badge/slot-64B_cache--line-informational)

Lock-free MPMC bounded ring backed by a memory-mapped file. One
byte layout serves three deployment modes: cross-thread (single
process), cross-process (multiple processes opening the same file),
and disk-persistent (kernel writes dirty pages on its own schedule;
explicit `flush` forces durability).

> **The "Vyukov MPMC + MMF" primitive.** Each slot is a 64-byte
> cache line carrying state + sequence + payload. The classic
> bounded-queue protocol handles N producers + N consumers
> lock-free, with the additional architectural lever that the ring
> lives in shared memory accessible across processes.

## Pick the right ring for your shape

`SharedRing` is the **global-FIFO MPMC** member of a family of
five composable ring primitives. Use this matrix to land on the
right type for your producer / consumer shape:

| Shape | Default | Override | Doc |
|---|---|---|---|
| 1P / 1C (SPSC) | `SharedRingSpsc` (Lamport pair) | none | [shared-ring-spsc](../shared-ring-spsc/) |
| NP / 1C (MPSC) | `SharedRingMpsc` (composed N Lamport rings) | `SharedRingMpscFifo` (single ring, global FIFO) | [shared-ring-mpsc](../shared-ring-mpsc/) |
| 1P / NC fan-out (every consumer reads every item) | `SharedBroadcastRing` | none | [shared-broadcast-ring](../shared-broadcast-ring/) |
| 1P / NC work-distribute (each item to one consumer) | `SharedDeque` + KHL / KHPD / LOH / URD variants | none | [shared-deque](../shared-deque/) |
| NP / NC (MPMC) | `SharedRingMpmc` (composed N x M grid) | **`SharedRing`** (this page; Vyukov, global FIFO) | [shared-ring-mpmc](../shared-ring-mpmc/) |
| Shape unknown / morphs over runtime | `AdaptiveRing` (all 4 shapes pre-allocated; `pin_current_shape()` hands off to native primitive speed) | none | [shared-ring-adaptive](../shared-ring-adaptive/) |

The composed primitives drop the per-slot sequence atomic that
Vyukov needs for global FIFO and run faster on a saturating stream:
the per-producer-FIFO composed shape lands around 2.5-3x at 4P with
equal-capacity rings, and the MPMC grid around 1.3-1.6x at the same
total buffer. Reach for this `SharedRing` only when **global FIFO
across all producers** matters more than throughput - a
totally-ordered event log, a sequenced transaction stream, or
anything where reordering across producers is a correctness bug
rather than a performance choice. Its two hot counters sit on
separate cache lines (below), so under contention it is the fastest
global-FIFO structure here even though it pays the shared CAS.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Byte-slice payloads.** `try_push(&[u8])` copies the bytes into
  the slot; `try_pop(&mut [u8])` copies them out. Typed payloads
  ride the `Channel<T>` / `AdaptiveIpc<T>` layers above, which
  marshal into these bytes.
- **Payload up to `PAYLOAD_BYTES = SLOT_SIZE - 8 = 56`** bytes per
  slot. A short push zero-fills the slot tail; pops always return
  the full 56 bytes. Larger payloads need a different primitive.
- **`SLOT_SIZE = 64`.** Each slot is one cache line: an 8-byte
  `sequence: AtomicU64` plus the 56-byte payload.
- **Header spans three cache lines.** `producer_seq` and
  `consumer_seq` each occupy their own 64-byte line; the read-mostly
  metadata (`magic`, `capacity`, `slot_size`) and the watchdog
  `epoch` share the first. Every producer CASes `producer_seq` and
  every consumer CASes `consumer_seq`, so co-locating the two on one
  line would make each side's CAS invalidate the other's copy.
  Separating them keeps producer and consumer coherence traffic
  apart, the same split the SPSC ring uses for `head`/`tail`.
- **Vyukov MPMC protocol**: the per-slot sequence value IS the
  state. `seq == pos` means free for the producer at `pos`;
  `seq == pos + 1` means published for the consumer at `pos`;
  the consumer's release stores `seq = pos + capacity`, handing
  the slot to the producer one lap ahead. CAS on `producer_seq` /
  `consumer_seq` elects producers / consumers.
- **SPSC fast paths**: `try_push_spsc` / `try_pop_spsc` skip the
  election CAS under a caller-enforced sole-producer /
  sole-consumer contract (~25% less atomic traffic per op).
- **Capacity must be a power of 2**; asserted at `create`. The
  slot index is `pos & (capacity - 1)`.
- **Backings, four locales**: `create(path, cap)` + `open(path,
  expected_capacity)` (file, cross-process via the page cache);
  `create_anon(cap)` (process-private anon mmap); `create_from_shm` /
  `open_from_shm` (named RAM-resident shared-memory section, no page cache);
  `create_in_region` / `open_in_region` (caller-owned memory - huge / large
  pages or any `RegionOwner`; this is the global-FIFO MPMC primitive on large
  pages). `open` / `open_from_shm` / `open_in_region` validate magic +
  capacity + slot size and do NOT re-initialize. `into_lazy(path, cap)` hands
  back a [`LazySharedRing`](#deferred-setup-lazysharedring) that defers the
  file create + mmap + init to the first `try_push` / `try_pop`.
- **`flush` / `flush_async`** for durable disk persistence
  (no-ops on anon / shm backings, which never touch disk).

---

## Table of contents

- [What it is](#what-it-is)
- [Vyukov MPMC protocol](#vyukov-mpmc-protocol)
- [Three deployment modes](#three-deployment-modes)
- [Worked examples](#worked-examples)
- [Bench evidence](#bench-evidence)
- [Use case patterns](#use-case-patterns)
- [Known limitations](#known-limitations)
- [Common pitfalls](#common-pitfalls)
- [References](#references)

---

## What it is

`SharedRing` is a Vyukov-style MPMC bounded ring with the slots
allocated as 64-byte cache lines inside an MMF backing file:

```mermaid
block-beta
  columns 1
  hdr["RingHeader - 192 B, three 64 B cache lines: [line 0: magic, capacity, slot_size, epoch] [line 1: producer_seq alone] [line 2: consumer_seq alone]"]
  s0["Slot 0 - 64 B: sequence u64, payload [u8; 56]"]
  s1["Slot 1 - same shape"]
  dots["..."]
  classDef hdrC fill:#1e3a8a,color:#ffffff
  classDef slotC fill:#0f766e,color:#ffffff
  classDef padC fill:#475569,color:#ffffff
  class hdr hdrC
  class s0,s1 slotC
  class dots padC
```

The header carries the magic + producer / consumer cursors; the
slot array follows.

```mermaid
graph LR
    P[Producer]
    H[RingHeader<br/>producer_seq<br/>consumer_seq]
    S[Slot[i]<br/>sequence + payload]
    C[Consumer]

    P -- "1: CAS producer_seq" --> H
    P -- "2: write payload" --> S
    P -- "3: Release seq" --> S
    C -- "1: Acquire seq" --> S
    C -- "2: read payload" --> S
    C -- "3: CAS consumer_seq" --> H

    classDef proc fill:#1e3a5f,stroke:#5b9bd5,color:#e8f1f5
    classDef state fill:#1f4a3a,stroke:#5cb85c,color:#e8f5e8

    class P,C proc
    class H,S state
```

---

## Vyukov MPMC protocol

The shipped protocol (`try_push` / `try_pop` in
`shared_ring.rs`):

### Producer (`try_push`)

1. Relaxed-load `producer_seq` as `pos`.
2. `slot = pos & (capacity - 1)`.
3. Acquire-load the slot's sequence. `seq == pos` means the slot
   is free to claim; `seq < pos` means an unconsumed value still
   occupies it - the ring is full, return `Err(Full)`; `seq > pos`
   means another producer raced ahead - spin and retry.
4. CAS `producer_seq` from `pos` to `pos + 1`
   (`compare_exchange_weak`; a lost CAS retries the loop). On
   success the slot is owned: copy the payload (zero-filling any
   tail), then Release-store `slot.sequence = pos + 1`.

### Consumer (`try_pop`)

1. Relaxed-load `consumer_seq` as `pos`.
2. `slot = pos & (capacity - 1)`.
3. Acquire-load the slot's sequence; `seq == pos + 1` means the
   producer published. `seq < pos + 1` means empty - return
   `Err(Empty)`; greater means another consumer raced ahead -
   spin and retry.
4. CAS `consumer_seq` from `pos` to `pos + 1`. On success copy
   the payload out, then Release-store
   `slot.sequence = pos + capacity`, handing the slot to the
   producer that will claim position `pos + capacity` one lap
   later.

`try_push_spsc` / `try_pop_spsc` run the same slot protocol but
replace the election CAS with a plain store, valid only under the
caller's sole-producer / sole-consumer contract.

This is the classic textbook MPMC ring; the architectural lever
is putting it in an MMF for cross-process visibility.

---

## Three deployment modes

| Mode | How |
|---|---|
| Cross-thread | Single process; multiple threads share `Arc<SharedRing<P>>` |
| Cross-process | Multiple processes call `SharedRing::open(path)` on the same file |
| Disk-persistent | The MMF backing IS a real file; kernel writes dirty pages on its own schedule; `flush` forces sync |

The same byte layout serves all three. The deployment choice is
purely how the MMF handle is shared, not a different protocol.

---

## Bench evidence

Two harnesses on the same Zen+ R7 2700 / Windows 11 box:

- **Criterion round-trip + MPMC**: `crates/subetha-cxc/benches/shared_ring.rs`,
  `--sample-size=15 --warm-up-time=1 --measurement-time=2`.
- **SPSC sustained throughput**: `crates/subetha-cxc/examples/spsc_shootout.rs`,
  1,000,000 items per trial, best-of-5 per variant, one warmup
  pass per process.

**SPSC round-trip (one push + one pop in lockstep):**

| Variant | Time |
|---|---:|
| `SharedRing` | 24.82 ns |
| `std::sync::mpsc::sync_channel` | 26.19 ns |
| `crossbeam_channel` (bounded) | 33.13 ns |

SharedRing leads crossbeam by ~1.3x on round-trip latency
(Vyukov MPMC has lower per-op overhead than crossbeam's
SPMC-optimised channel) and edges `sync_channel`.

**SPSC sustained throughput (1M items, best-of-5; same captured
run as the [Lamport SPSC page](../shared-ring-spsc/)):**

| Variant | Throughput | vs crossbeam |
|---|---:|---:|
| `SharedRing::create` (file) + `try_push_spsc` / `try_pop_spsc` | 36.44 M items/s | 3.14x |
| `SharedRing::create_anon` + `try_push_spsc` / `try_pop_spsc` | 23.99 M items/s | 2.06x |
| `SharedRing::create` (file) + `try_push` / `try_pop` (MPMC) | 21.26 M items/s | 1.83x |
| `SharedRing::create_anon` + `try_push` / `try_pop` (MPMC) | 20.71 M items/s | 1.78x |
| `crossbeam_channel::bounded(4096)` | 11.62 M items/s | baseline |

Absolute numbers drift run to run on a desktop host; the stable
signals are crossbeam trailing every variant, the SPSC fast paths
leading the MPMC paths (they skip the `compare_exchange_weak` on
`producer_seq` that MPMC needs against racing producers), and the
2-3x class of the lead. The dedicated
[Lamport SPSC pair](../shared-ring-spsc/) drops the per-slot
sequence atomic entirely and led the same captured run at
**37.79 M items/s, 3.25x crossbeam**.

**MPMC 4 producers + 4 consumers (varying message counts):**

| Variant | Time |
|---|---:|
| `SharedRing` | 1.87 ms |
| `crossbeam_channel` | 1.77 ms |

The 4x4 contended workload is a coin flip within run noise:
captured runs land each contender ahead by single-digit percent.
Treat them as parity. The dedicated composed
[`SharedRingMpmc`](../shared-ring-mpmc/) grid (N x M Lamport
rings) is the decisive winner for callers who do not need global
FIFO - measured 3.5x this `SharedRing` on the same shape.

### Reading the trade-offs

The architectural shape (cache-line-per-slot, Vyukov protocol)
wins on round-trip latency, sustained SPSC throughput, and MPMC
contention against crossbeam. The composed siblings in this
family beat it further when global FIFO across producers is not
a requirement.

### Rule 3b bench audit

- **Fair contenders**: `crossbeam_channel::bounded` and
  `std::sync::mpsc::sync_channel` are the standard production
  in-process bounded queues. The bench tests SPSC round-trip,
  SPSC throughput, and MPMC contention.
- **Same payload type / same capacity** across all variants.
- **MMF lifecycle managed**: bench files cleaned up at end.

### Where the cross-process and cross-host numbers live

The bench tables above are in-process. Cross-process and cross-
host data lives in separate captures because the measurement
harness is different:

- **Cross-process round-trip on the same host**: 76-408 ns
  one-way across the four pinned `AdaptiveRing` shapes and four
  platforms (the Vyukov row is this primitive pinned through the
  adaptive system; 96-331 ns across platforms). Captured by
  `crates/subetha-cxc/examples/cross_process_compare.rs` against
  iceoryx2, named-pipe, ipc-channel, stdio-pipe, TCP loopback,
  and UDP loopback contenders; full tables in
  `docs/CROSS_PROCESS_IPC_PERFORMANCE.md`.
- **Cross-host through QUIC bridge**: 100,000 items shipped
  end-to-end across a real quinn-based QUIC connection on
  127.0.0.1 with integrity asserted. Demonstrated by
  `crates/subetha-cxc/examples/quic_bridge_e2e.rs`.

### Durability cost excluded by design

The bench tables measure protocol overhead on the hot path.
`flush` / `flush_async` (msync syscall for durable disk
persistence) are explicit caller decisions, not per-op overhead;
they execute when the application asks for a checkpoint and
their cost depends on the dirty-page count at that moment. They
belong in a durability-focused bench, not the protocol-overhead
tables above.

---

## Crash recovery (stuck-slot heal)

Vyukov MPMC has one narrow crash window: a producer that CASes `producer_seq`
forward (claiming a slot) but dies before the Release-store that publishes
`slot.sequence`. That leaves a permanent hole - the consumer never advances
past it. `SharedRing` exposes the sidecar-driven repair for it:

- `next_stuck_slot(from) -> Option<u64>` scans the claimed-but-undrained
  window `[consumer_seq, producer_seq)` and returns the first position whose
  sequence is stuck at `pos` (claimed, never published). O(in-flight) and
  never touched by `try_push` / `try_pop` - it costs the hot path nothing.
- `heal_stuck_slot(pos) -> Result<bool, _>` advances that slot's sequence
  `pos -> pos + 1` with one CAS so the next consumer drains it. **Caller
  contract**: confirm the claiming producer is genuinely dead first -
  `SharedRing` records no per-slot producer identity, so a heal raced against
  a live producer's publish is a no-op CAS (`Ok(false)`) but still hands the
  consumer a slot the producer never finished writing.

The canonical dead-producer signal is `HeartbeatTable` + `FailoverWatchdog`:
register each producer, and on a stale heartbeat the watchdog walks that
producer's rings, calling `heal_stuck_slot` for every `next_stuck_slot`.

## Deferred setup: LazySharedRing

`SharedRing::into_lazy(path, cap)` (or `LazySharedRing::new`) defers the file
create + ftruncate + mmap + layout init until the first op, for speculative
channels that may never send. `get() -> Result<&SharedRing>` materializes
once (race-safe via `OnceLock`) and caches; `is_initialised()` reports
whether it has; `try_push` / `try_pop` forward through `get()`. For an
always-sending hot loop, materialize once outside the loop and reuse the
`&SharedRing` so the lazy branch (one extra atomic load) stays out of it.

## Signals and accessors

- `next_pop_signal() -> &AtomicU64` is the consumer's next-pop publish atom:
  arm `monitor_wait::monitor_wait_u64` on it instead of a raw spin loop (a
  producer publishing that slot Release-stores this exact atom). Recompute
  after every successful pop - the position advances.
- `producer_seq()` / `consumer_seq()` are the monotonic enqueue / dequeue
  counters; `approx_len()` is their saturating difference (items waiting);
  `capacity()` is the slot count.

## Worked examples

### Cross-thread SPSC

```rust,no_run
use std::sync::Arc;
use subetha_cxc::SharedRing;

let ring = Arc::new(SharedRing::create("/tmp/ring.bin", 1024)?);

let prod = Arc::clone(&ring);
let producer = std::thread::spawn(move || {
    for i in 0..1000u64 {
        // Sole producer on this ring: the SPSC fast path skips
        // the producer-election CAS.
        while prod.try_push_spsc(&i.to_le_bytes()).is_err() {
            std::thread::yield_now();
        }
    }
});

let cons = Arc::clone(&ring);
let consumer = std::thread::spawn(move || {
    let mut out = [0u8; 56];
    let mut got = 0;
    while got < 1000 {
        match cons.try_pop_spsc(&mut out) {
            Ok(_) => got += 1,
            Err(_) => std::thread::yield_now(),
        }
    }
});

producer.join().unwrap();
consumer.join().unwrap();
# Ok::<(), subetha_cxc::RingError>(())
```

For typed payloads, reach for `Channel<T>` / `AdaptiveIpc<T>`
one layer up - they marshal `T` into these 56-byte slots and
hand back typed sends and recvs.

### Cross-process work queue

Process A (creates; MPMC paths so any number of producers may
join later):

```rust,no_run
use subetha_cxc::SharedRing;

let ring = SharedRing::create("/tmp/work.bin", 1024)?;
for i in 0..1_000_000u64 {
    while ring.try_push(&i.to_le_bytes()).is_err() {
        std::thread::yield_now();
    }
}
# Ok::<(), subetha_cxc::RingError>(())
```

Process B (attaches; `open` validates magic, capacity, and slot
size against the creator's header):

```rust,no_run
use subetha_cxc::SharedRing;

let ring = SharedRing::open("/tmp/work.bin", 1024)?;
let mut out = [0u8; 56];
loop {
    match ring.try_pop(&mut out) {
        Ok(_) => {
            let work = u64::from_le_bytes(out[..8].try_into().unwrap());
            let _ = work; // process it
        }
        Err(_) => std::thread::sleep(std::time::Duration::from_millis(1)),
    }
}
# #[allow(unreachable_code)]
# Ok::<(), subetha_cxc::RingError>(())
```

---

## Use case patterns

### Pattern: cross-process work queue

Producer process generates jobs; worker process(es) pop and
execute. No coordinator daemon; the MMF IS the queue.

### Pattern: telemetry / log shipping

Producer pushes events; consumer in a separate process drains
and ships to durable storage.

### Pattern: zero-copy cross-process pipe

For payloads that fit in 56 bytes (request IDs, lookup keys, etc.)
SharedRing is a zero-allocation cross-process channel.

### Pattern: durable pub/sub buffer

The MMF backing provides crash-recoverable buffering; if both
producer and consumer crash, the unconsumed messages persist
on disk and can be drained on restart.

---

## Known limitations

- **Payload size capped at 56 bytes per slot**: larger messages
  need either reference-passing (offset pointer) or a different
  primitive.
- **Capacity is power-of-2**: typical Vyukov ring constraint;
  modulo via `& (cap - 1)`.
- **No partial reads**: the consumer either gets a full payload
  or nothing.
- **Throughput trades for global FIFO**: this primitive preserves
  global FIFO across all producers (every push gets a monotonic
  `producer_seq`), which costs a per-slot sequence atomic and a
  producer-side CAS on every push. The composed family
  ([shared-ring-mpmc](../shared-ring-mpmc/),
  [shared-ring-mpsc](../shared-ring-mpsc/),
  [shared-ring-spsc](../shared-ring-spsc/)) drop that machinery in
  exchange for per-producer FIFO only, and run faster on a
  saturating stream: the composed shape by ~2.5-3x at 4P with
  equal-capacity rings, the MPMC grid by ~1.3-1.6x at the same
  total buffer. Reach for `SharedRing` when global FIFO is
  a correctness requirement, otherwise pick the composed sibling.
- **Disk durability requires explicit flush**: cross-process
  visibility is immediate via cache coherence; durability survives
  crashes only after `flush`.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Wrapping `SharedRing` in a Mutex.** Pointless; the Vyukov
  protocol is lock-free.

- **Reading payload before checking seq.** The Acquire load on
  `slot.sequence` is the gate that makes the producer's payload
  write visible; the shipped `try_pop` performs it for you - do
  not bypass it with raw slot access.

- **Passing a payload longer than 56 bytes.**
  `Err(RingError::PayloadTooLarge)`; the slot cannot hold it.
  Marshal larger values through `SharedStringArena` /
  `SharedRegion` plus an offset pointer.

- **Using the `_spsc` fast paths with more than one producer or
  consumer.** The plain stores race and corrupt the cursors;
  the sole-owner contract is the caller's to uphold. Use
  `try_push` / `try_pop` for anything MPMC-shaped.

- **Sizing capacity smaller than expected steady-state load.**
  `try_push` returns `Err(RingError::Full)` when the ring is
  full. Size with headroom or implement a drop-on-full policy.

- **Treating the MMF as authoritative across reboots.** Reboots
  trash the page cache and may corrupt mid-write slots. Use
  flush before crash-recovery scenarios.

---

## References

- Source: `crates/subetha-cxc/src/shared_ring.rs`.
- Bench: `crates/subetha-cxc/benches/shared_ring.rs` (SPSC round-trip,
  SPSC throughput, MPMC 4x4 workloads vs crossbeam_channel and
  std::sync::mpsc baselines).
- Sibling primitive: [SHARED_BROADCAST_RING.md](shared-broadcast-ring/) -
  multi-consumer broadcast variant.
- Sibling primitive: [SHARED_TREIBER_STACK.md](shared-treiber-stack/) -
  LIFO counterpart (stack instead of queue).
- Sibling primitive: [SHARED_ATOMIC.md](../atomics/shared-atomic/) - the
  underlying atomic primitive the Vyukov protocol builds on.
