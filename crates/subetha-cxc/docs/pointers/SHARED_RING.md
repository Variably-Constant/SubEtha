# SharedRing&lt;P&gt;

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
| 1P / 1C (SPSC) | [`SharedRingSpsc`](./SHARED_RING_SPSC.md) (Lamport pair) | none | [SHARED_RING_SPSC.md](./SHARED_RING_SPSC.md) |
| NP / 1C (MPSC) | [`SharedRingMpsc`](./SHARED_RING_MPSC.md) (composed N Lamport rings) | [`SharedRingMpscFifo`](./SHARED_RING_MPSC.md) (single ring, global FIFO) | [SHARED_RING_MPSC.md](./SHARED_RING_MPSC.md) |
| 1P / NC fan-out (every consumer reads every item) | [`SharedBroadcastRing`](./SHARED_BROADCAST_RING.md) | none | [SHARED_BROADCAST_RING.md](./SHARED_BROADCAST_RING.md) |
| 1P / NC work-distribute (each item to one consumer) | [`SharedDeque`](./SHARED_DEQUE.md) + KHL / KHPD / LOH / URD variants | none | [SHARED_DEQUE.md](./SHARED_DEQUE.md) |
| NP / NC (MPMC) | [`SharedRingMpmc`](./SHARED_RING_MPMC.md) (composed N x M grid) | **`SharedRing`** (this doc; Vyukov, global FIFO) | [SHARED_RING_MPMC.md](./SHARED_RING_MPMC.md) |
| Shape unknown / morphs over runtime | [`AdaptiveRing`](./SHARED_RING_ADAPTIVE.md) (all 4 shapes pre-allocated; `pin_current_shape()` hands off to native primitive speed) | none | [SHARED_RING_ADAPTIVE.md](./SHARED_RING_ADAPTIVE.md) |

The composed primitives drop the per-slot sequence atomic that
Vyukov needs for global FIFO and beat the Vyukov MPMC ring 2-3.5x
on every measured shape (see the family docs for numbers). Reach
for this `SharedRing` only when **global FIFO across all producers**
matters more than throughput - a totally-ordered event log, a
sequenced transaction stream, or anything where reordering across
producers is a correctness bug rather than a performance choice.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **`P: Copy + 'static`**: the payload type. Stable `#[repr(C)]`
  layout is the caller's responsibility for cross-version use.
- **Payload up to `PAYLOAD_BYTES = SLOT_SIZE - 8 = 56`** bytes per
  slot. Larger P needs a different primitive.
- **`SLOT_SIZE = 64`**. Each slot is one cache line.
- **Vyukov MPMC protocol**: sequence numbers
  per slot drive the state machine; CAS on producer_seq /
  consumer_seq elects producers / consumers.
- **State machine per slot**: EMPTY -> CLAIMED_BY_PRODUCER ->
  PUBLISHED -> CLAIMED_BY_CONSUMER -> EMPTY (closed loop).
- **Capacity must be a power of 2** (typical Vyukov requirement;
  the modulo is `seq % capacity` which is cheap when cap is pow2).
- **Cross-thread / cross-process / disk-persistent** are the same
  byte layout; deployment is via `create` vs `open`.
- **`flush` / `flush_async`** for durable disk persistence.
- **Cross-process backed by MMF.**

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

`SharedRing<P>` is a Vyukov-style MPMC bounded ring with the slots
allocated as 64-byte cache lines inside an MMF backing file:

```text
+-----------------------------+
| RingHeader  (64B aligned)   |  magic, capacity, slot_size,
|                             |  producer_seq, consumer_seq
+-----------------------------+
| Slot[0] (64B)               |  sequence: u64, payload: [u8; 56]
| Slot[1]                     |
| ...                         |
+-----------------------------+
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

Source rustdoc lines 38-56 documents the protocol:

### Producer

1. Read `producer_seq` (atomic).
2. `slot_idx = producer_seq % capacity`.
3. Read slot's sequence; if not equal to `producer_seq`, the ring
   is full. Retry or fail.
4. CAS `producer_seq` from S to S+1. On success the slot is owned
   by this producer; copy the payload, then store
   `slot.sequence = S+1` (Release).

### Consumer

1. Read `consumer_seq`.
2. `slot_idx = consumer_seq % capacity`.
3. Acquire-load `slot.sequence`; must equal `consumer_seq + 1`
   (means producer published). Otherwise the slot is still empty
   or being written.
4. CAS `consumer_seq` from S to S+1. On success read the payload,
   then store `slot.sequence = S + capacity` (releases the slot
   for the next producer at `producer_seq = S + capacity`).

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
| `SharedRing` | 21.72 ns |
| `crossbeam_channel` (bounded) | 33.71 ns |
| `std::sync::mpsc::sync_channel` | 25.60 ns |

SharedRing wins **1.55x** vs crossbeam on round-trip latency
(Vyukov MPMC has lower per-op overhead than crossbeam's
SPMC-optimised channel).

**SPSC sustained throughput (1M items, best-of-5):**

| Variant | Throughput | vs crossbeam |
|---|---:|---:|
| `SharedRing::create_anon` + `try_push_spsc` / `try_pop_spsc` | **38.42 M items/s** | **3.59x faster** |
| `SharedRing::create_anon` + `try_push` / `try_pop` (MPMC) | 23.95 M items/s | 2.24x faster |
| `SharedRing::create` (file) + `try_push_spsc` / `try_pop_spsc` | 23.12 M items/s | 2.16x faster |
| `SharedRing::create` (file) + `try_push` / `try_pop` (MPMC) | 21.82 M items/s | 2.04x faster |
| `crossbeam_channel::bounded(4096)` | 10.71 M items/s | baseline |

SharedRing wins SPSC sustained throughput against crossbeam by
**2.04x–3.59x** depending on the backing (anonymous vs file) and
the API (MPMC `try_push` vs SPSC `try_push_spsc`). The SPSC
fast path skips the `compare_exchange_weak` on `producer_seq`
that MPMC needs to defend against racing producers; anonymous
backing skips the file-create + ftruncate + first-page-fault
cost the file-backed mode pays once at construction.

**MPMC 4 producers + 4 consumers (varying message counts):**

| Variant | Time |
|---|---:|
| `SharedRing` | 1.47 ms |
| `crossbeam_channel` | 1.58 ms |

SharedRing wins **1.07x** on the 4x4 MPMC workload, where the
cache-line-per-slot reduces false sharing relative to crossbeam's
denser layout.

### Reading the trade-offs

The architectural shape (cache-line-per-slot, Vyukov protocol)
wins on every shape measured: round-trip latency, SPSC sustained
throughput, and MPMC contention behaviour. Anonymous-mapping +
SPSC fast path delivers the headline 3.59x crossbeam-beat on SPSC
sustained; SharedRing's cross-process capability is the strict
extra win over crossbeam (which is in-process only).

### Rule 3b bench audit

- **Fair contenders**: `crossbeam_channel::bounded` and
  `std::sync::mpsc::sync_channel` are the standard production
  in-process bounded queues. The bench tests SPSC round-trip,
  SPSC throughput, and MPMC contention.
- **Same payload type / same capacity** across all variants.
- **MMF lifecycle managed**: bench files cleaned up at end.

### Where the cross-process and cross-host numbers live

The bench tables above are in-process. Cross-process and cross-host
data lives in separate files because the measurement harness is
different:

- **Cross-process round-trip on the same host**: 349 ns one-way for
  `Channel<u64>` (which is `SharedRing`-backed via the API
  layer). Captured by
  [`examples/cross_process_compare.rs`](../../examples/cross_process_compare.rs)
  against named-pipe, ipc-channel, stdio-pipe, TCP loopback, and
  UDP loopback contenders. Raw JSON in
  [`docs/cross_process_ipc_results.json`](../../../../docs/cross_process_ipc_results.json);
  rendered comparison in
  [`docs/platform_ipc_dotplot.png`](../../../../docs/platform_ipc_dotplot.png).
- **Cross-host through QUIC bridge**: 100,000 items shipped
  end-to-end across a real quinn-based QUIC connection on
  127.0.0.1 with integrity asserted (every item arrives exactly
  once). Throughput 0.47 M items/s in the captured run, dominated
  by loopback UDP + TLS framing rather than the MMF protocol.
  Demonstrated by
  [`examples/quic_bridge_e2e.rs`](../../examples/quic_bridge_e2e.rs).

### Durability cost excluded by design

The bench tables measure protocol overhead on the hot path.
`flush` / `flush_async` (msync syscall for durable disk
persistence) are explicit caller decisions, not per-op overhead;
they execute when the application asks for a checkpoint and
their cost depends on the dirty-page count at that moment. They
belong in a durability-focused bench, not the protocol-overhead
tables above.

---

## Liveness property: stuck slots after a producer crash

Vyukov MPMC has a narrow liveness window every implementation
inherits. A producer claims a slot by CAS-ing `producer_seq` from
`pos` to `pos + 1`, then writes the payload, then does the Release
store on `slot.sequence` to publish. If the producer crashes
between the claim CAS and the publish Release, the slot ends up
"claimed but never published": `producer_seq` has advanced past
`pos`, but `slot[pos % cap].sequence` is still the initial value
`pos`. The consumer at that position reads `sequence` and sees
`pos != pos + 1`, decides `Empty`, and spins; future pushes land
at `pos + 1` and onward, so the ring keeps working but the
consumer cannot drain past the hole at `pos`. This is a structural
property of the protocol (crossbeam, std::mpsc, every Vyukov queue
has the same window), not a `SharedRing`-specific bug.

The recovery primitives sit dormant on the type and cost the hot
path nothing:

- [`SharedRing::next_stuck_slot(from)`](https://docs.rs/subetha-cxc/latest/subetha_cxc/struct.SharedRing.html#method.next_stuck_slot)
  walks the `[consumer_seq, producer_seq)` window and returns the
  first position whose sequence is stuck at `pos`. The sidecar
  invokes this only after its observation analysis decides a ring
  is wedged (consumer emitting Empty at high rate while
  `producer_seq > consumer_seq`).
- [`SharedRing::heal_stuck_slot(pos)`](https://docs.rs/subetha-cxc/latest/subetha_cxc/struct.SharedRing.html#method.heal_stuck_slot)
  performs one atomic CAS advancing `slot[pos % cap].sequence`
  from `pos` to `pos + 1`. The consumer drains the slot on its
  next `try_pop` in normal order; its payload bytes are whatever
  the dying producer happened to write (likely zeros if it died
  before any write). Returns `Ok(true)` on heal, `Ok(false)` if
  the slot was not actually stuck or if a live producer raced and
  published first.

Caller contract for the heal: independently confirm that the
slot-claiming producer is dead before calling. `SharedRing` does
not record per-slot producer identity, so the dead-producer
determination must come from the substrate's heartbeat
machinery (`HeartbeatTable` + `FailoverWatchdog`,
`OwnerLease` expiry, or application-level timeout). Calling
heal on a live producer's slot is benign for the CAS itself but
the consumer drains the partially-written payload.

End-to-end demonstration of the failure mode and the recovery is
in [`examples/stuck_slot_recovery.rs`](../../examples/stuck_slot_recovery.rs):
the example deliberately wedges a consumer (9.99 million Empty
results during the wedge in one run), then runs the sidecar's
scan + heal, then watches the consumer drain past the recovered
slot.

---

## Worked examples

### Cross-thread SPSC

```rust
use std::sync::Arc;
use subetha_cxc::SharedRing;

#[derive(Clone, Copy)]
#[repr(C)]
struct Message { id: u64, payload: [u8; 48] }

let ring: Arc<SharedRing<Message>> = Arc::new(
    SharedRing::create("/tmp/ring.bin", 1024).unwrap()
);

let prod = ring.clone();
let producer = std::thread::spawn(move || {
    for i in 0..1000u64 {
        let msg = Message { id: i, payload: [0; 48] };
        while !prod.push(msg) { std::thread::yield_now(); }
    }
});

let cons = ring.clone();
let consumer = std::thread::spawn(move || {
    let mut got = 0;
    while got < 1000 {
        if let Some(_msg) = cons.pop() { got += 1; }
        else { std::thread::yield_now(); }
    }
});

producer.join().unwrap();
consumer.join().unwrap();
```

### Cross-process work queue

Process A:
```rust
let ring: SharedRing<u64> = SharedRing::create("/tmp/work.bin", 1024).unwrap();
for i in 0..1_000_000u64 {
    while !ring.push(i) { std::thread::yield_now(); }
}
```

Process B:
```rust
let ring: SharedRing<u64> = SharedRing::open("/tmp/work.bin").unwrap();
loop {
    if let Some(work) = ring.pop() {
        // process work
    } else {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
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
  ([`SharedRingMpmc`](./SHARED_RING_MPMC.md),
  [`SharedRingMpsc`](./SHARED_RING_MPSC.md),
  [`SharedRingSpsc`](./SHARED_RING_SPSC.md)) drop that machinery in
  exchange for per-producer FIFO only, and run 2-3.5x faster on
  every measured shape. Reach for `SharedRing` when global FIFO is
  a correctness requirement, otherwise pick the composed sibling.
- **Disk durability requires explicit flush**: cross-process
  visibility is immediate via cache coherence; durability survives
  crashes only after `flush`.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Wrapping `SharedRing` in a Mutex.** Pointless; the Vyukov
  protocol is lock-free.

- **Reading payload before checking seq.** UB; the producer may
  not have finished writing. The Acquire load on slot.sequence is
  the gate.

- **Using a non-Copy payload.** Compile error; payload must be
  Copy (the ring memcpys it in place).

- **Sizing capacity smaller than expected steady-state load.**
  Producers block (`push` returns false) when the ring is full.
  Size with headroom or implement a drop-on-full policy.

- **Treating the MMF as authoritative across reboots.** Reboots
  trash the page cache and may corrupt mid-write slots. Use
  flush before crash-recovery scenarios.

---

## References

- Source: `crates/subetha-cxc/src/shared_ring.rs` (1515 lines, 21 unit tests).
- Bench: `crates/subetha-cxc/benches/shared_ring.rs` (Criterion
  SPSC round-trip, SPSC throughput, MPMC 4x4 vs `crossbeam_channel`
  and `std::sync::mpsc` baselines) plus
  `crates/subetha-cxc/examples/spsc_shootout.rs` and
  `crates/subetha-cxc/examples/mpmc_shootout.rs` for the composed-
  family head-to-heads.
- Ring family siblings (pick by shape):
  [SHARED_RING_SPSC.md](./SHARED_RING_SPSC.md) -
  Lamport SPSC pair (62.96 M items/s, 5.95x crossbeam_channel).
  [SHARED_RING_MPSC.md](./SHARED_RING_MPSC.md) -
  composed N Lamport rings + Fifo override.
  [SHARED_RING_MPMC.md](./SHARED_RING_MPMC.md) -
  composed N x M Lamport grid (20.96 M items/s at 4P/4C, 3.49x
  this `SharedRing`).
  [SHARED_BROADCAST_RING.md](./SHARED_BROADCAST_RING.md) -
  fan-out variant where every consumer sees every item.
  [SHARED_DEQUE.md](./SHARED_DEQUE.md) - work-stealing variants
  for SPMC work-distribution (one producer, N competing
  consumers).
  [SHARED_TREIBER_STACK.md](./SHARED_TREIBER_STACK.md) -
  LIFO counterpart (stack instead of queue).
  [SHARED_ATOMIC.md](./SHARED_ATOMIC.md) - the underlying atomic
  primitive the Vyukov protocol builds on.
