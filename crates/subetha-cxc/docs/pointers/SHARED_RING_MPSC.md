# SharedRingMpsc + SharedRingMpscFifo

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Default](https://img.shields.io/badge/default-composed_N_Lamport-brightgreen)
![Override](https://img.shields.io/badge/override-single_Vyukov_for_FIFO-orange)
![Contract](https://img.shields.io/badge/contract-compile--time_single_consumer-success)

Multi-producer / single-consumer ring family. Two complementary
primitives in this file, with different protocol shapes and
different ordering guarantees:

- [`SharedRingMpsc`](#sharedringmpsc-composed-n-lamport-rings) -
  the default. Composed from N independent
  [`SharedRingSpsc`](./SHARED_RING_SPSC.md) rings, one per
  producer; consumer drains them round-robin. **Per-producer
  FIFO**, no global ordering across producers. Wins throughput
  at every measured N because each producer pushes to its own
  ring with zero CAS contention.

- [`SharedRingMpscFifo`](#sharedringmpscfifo-single-vyukov-ring) -
  the override. Single shared [`SharedRing`](./SHARED_RING.md)
  (Vyukov MPMC) where producers contend on one
  `producer_seq` CAS but the single consumer uses the
  `try_pop_spsc` fast path to skip the consumer-side CAS.
  **Preserves global FIFO across all producers** because every
  push gets a monotonic `producer_seq`. Reach for this when
  total ordering is a correctness requirement, not a performance
  choice.

Both expose the same handle shape: `Vec<Producer>` +
`Consumer`. Producers are `Send + !Sync + !Clone` so each handle
moves to one producer thread; the consumer is the same so the
compiler enforces single-consumer semantics.

---

## Table of contents

- [SharedRingMpsc (composed N Lamport rings)](#sharedringmpsc-composed-n-lamport-rings)
- [SharedRingMpscFifo (single Vyukov ring)](#sharedringmpscfifo-single-vyukov-ring)
- [Bench evidence + crossover analysis](#bench-evidence--crossover-analysis)
- [Worked examples](#worked-examples)
- [Known limitations](#known-limitations)
- [References](#references)

---

## `SharedRingMpsc` (composed N Lamport rings)

Construction returns N independent producer handles plus one
consumer handle. Each producer owns one Lamport SPSC ring (see
[SHARED_RING_SPSC.md](./SHARED_RING_SPSC.md) for the protocol).
The consumer keeps an atomic round-robin cursor and drains the
producer rings in order, advancing the cursor past the ring it
just drained so producers see fair attention.

```text
+----------+   +----------+   +----------+   +----------+
|Producer 0|   |Producer 1|   |Producer 2|   |Producer 3|
+----------+   +----------+   +----------+   +----------+
     |               |               |               |
     v               v               v               v
+---------+    +---------+    +---------+    +---------+
| Ring 0  |    | Ring 1  |    | Ring 2  |    | Ring 3  |  (Lamport SPSC each)
+---------+    +---------+    +---------+    +---------+
     \              |              |             /
      \-------------+--------------+------------/
                          |
                  +---------------+
                  |   Consumer    |   (round-robin cursor)
                  +---------------+
```

### Per-op cost

Push: pure Lamport SPSC (1 Acquire load + 1 Release store +
1 owner-private Relaxed load). Zero CAS, zero cross-producer
contention because each producer owns its ring.

Pop: 1 Acquire load + 1 Release store on the successful ring,
plus 1 Acquire load per empty ring the consumer skips before
finding a non-empty one. Worst case (every ring in the subset
empty): N Acquire loads then `Err(Empty)`.

### Constructor API

```rust
pub fn create_anon_pool(
    n_producers: usize,
    capacity: usize,
) -> Result<(Vec<MpscProducer>, MpscConsumer), RingError>;

pub fn create_pool(
    path_prefix: impl AsRef<Path>,
    n_producers: usize,
    capacity: usize,
) -> Result<(Vec<MpscProducer>, MpscConsumer), RingError>;

pub fn open_pool(
    path_prefix: impl AsRef<Path>,
    n_producers: usize,
    expected_capacity: usize,
) -> Result<(Vec<MpscProducer>, MpscConsumer), RingError>;
```

File-backed mode creates one file per producer ring at
`<path_prefix>.{i}.bin`. The path-prefix is appended-to, not
appended-into; passing `/tmp/inbox` yields `/tmp/inbox.0.bin`,
`/tmp/inbox.1.bin`, and so on through `.{n_producers - 1}.bin`.

---

## `SharedRingMpscFifo` (single Vyukov ring)

Construction returns N producer handles all sharing one
underlying `SharedRing`. Producers use the standard Vyukov
`try_push` (CAS on `producer_seq`, write payload, Release on
slot.sequence). The consumer uses `try_pop_spsc` (no consumer
CAS, sound because the handle type is `!Sync + !Clone`).

### Per-op cost

Push: Vyukov MPMC producer-side. CAS on `producer_seq` to claim
the slot (retries on contention from other producers), memcpy
payload, Release-store on `slot[pos % cap].sequence`. 4
cross-thread atomics in the success path; producer-side CAS
contention scales with producer count.

Pop: 1 Relaxed load on `consumer_seq` + 1 Acquire load on the
slot's sequence + 1 Relaxed store on `consumer_seq` + 1 Release
store on `slot.sequence` advancing it to the next-lap value.
No consumer-side CAS.

### Constructor API

```rust
pub fn create_anon_pool(
    n_producers: usize,
    capacity: usize,
) -> Result<(Vec<MpscFifoProducer>, MpscFifoConsumer), RingError>;

pub fn create_pool(
    path: impl AsRef<Path>,
    n_producers: usize,
    capacity: usize,
) -> Result<(Vec<MpscFifoProducer>, MpscFifoConsumer), RingError>;

pub fn open_pool(
    path: impl AsRef<Path>,
    n_producers: usize,
    expected_capacity: usize,
) -> Result<(Vec<MpscFifoProducer>, MpscFifoConsumer), RingError>;
```

File-backed mode uses one file (not N like
`SharedRingMpsc::create_pool`), so the on-disk layout is
identical to a plain `SharedRing` and any `SharedRing::open`
caller can attach to it as an MPMC reader if global FIFO
ordering is the contract.

---

## Bench evidence + crossover analysis

`crates/subetha-cxc/examples/mpmc_shootout.rs`, 250,000 items
per producer, 16-byte payloads, busy-spin on Full / Empty,
best-of-5 trials with one warmup pass. Zen+ R7 2700 / Windows 11.

| N producers -> 1 consumer | `SharedRingMpsc` (composed) | `SharedRingMpscFifo` (single) | `crossbeam_channel::bounded` | `SharedRing` (Vyukov as MPSC) |
|---|---:|---:|---:|---:|
| **N=2** | **22.29 M items/s** | 19.45 M items/s | 11.10 M items/s | 13.81 M items/s |
| **N=4** | **14.00 M items/s** | 6.88 M items/s | 7.40 M items/s | 5.00 M items/s |
| **N=8** | **15.26 M items/s** | 3.10 M items/s | 6.98 M items/s | 2.70 M items/s |

The composed primitive wins at every N. Fifo degrades sharply
(21.62 -> 6.88 -> 3.10 M items/s) because producer-side CAS
contention scales with producer count: at N=2 the two producers
rarely collide, by N=8 they spend most of their time spinning on
the same `producer_seq` cache line. The composed primitive stays
flat-to-improving (22 -> 14 -> 15) because each producer pushes
to its own ring with zero CAS.

### Picking between them

| Question | Answer |
|---|---|
| Do you need global FIFO across all producers? | `SharedRingMpscFifo` |
| Do you need a single file on disk for cross-process attach? | `SharedRingMpscFifo` (single MMF) |
| Throughput at any N? | `SharedRingMpsc` (composed) |
| Producer count 4 or above? | `SharedRingMpsc` (Fifo collapses) |
| Don't know which? | `SharedRingMpsc` (default) |

### Rule 3b bench audit

- **Fair contenders**: `crossbeam_channel::bounded` (MPSC under
  the hood; producers all share one sender, consumer is sole
  receiver) and `SharedRing` (Vyukov MPMC, used in MPSC
  configuration with single consumer thread). Same payload size
  (16 bytes), same capacity (4096), same per-producer load
  (250,000 items), same busy-spin loop.
- **Best-of-5 trials with one warmup pass** for stability against
  Windows scheduler noise.
- **Producer count varied (N=2, 4, 8)** to surface the crossover
  characteristic, not just one shape.

---

## Worked examples

### `SharedRingMpsc` (composed, default)

4 worker threads send results to one collector:

```rust
use subetha_cxc::SharedRingMpsc;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

let (producers, consumer) = SharedRingMpsc::create_anon_pool(4, 1024)?;

let workers: Vec<_> = producers.into_iter().enumerate().map(|(id, p)| {
    std::thread::spawn(move || {
        for i in 0..10_000u32 {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&(id as u32).to_le_bytes());
            buf[4..8].copy_from_slice(&i.to_le_bytes());
            while p.try_push(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
    })
}).collect();

let collector = std::thread::spawn(move || {
    let mut out = [0u8; SPSC_PAYLOAD_BYTES];
    let mut received = 0;
    while received < 40_000 {
        if consumer.try_pop(&mut out).is_ok() {
            received += 1;
            // Per-worker FIFO is preserved; cross-worker order is round-robin.
        } else {
            std::hint::spin_loop();
        }
    }
});

for w in workers { w.join().unwrap(); }
collector.join().unwrap();
```

### `SharedRingMpscFifo` (single ring, global FIFO)

Totally-ordered event log where every event has a monotonic
sequence number:

```rust
use subetha_cxc::SharedRingMpscFifo;
use subetha_cxc::shared_ring::PAYLOAD_BYTES;

let (producers, consumer) = SharedRingMpscFifo::create_anon_pool(4, 1024)?;

// Producers race; whichever wins the producer_seq CAS gets the
// next sequence number. Consumer drains in commit order.
let emitters: Vec<_> = producers.into_iter().map(|p| {
    std::thread::spawn(move || {
        for _ in 0..1000 {
            let mut buf = [0u8; PAYLOAD_BYTES];
            buf[0] = b'E';
            while p.try_push(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
    })
}).collect();

let log = std::thread::spawn(move || {
    let mut out = [0u8; PAYLOAD_BYTES];
    let mut seen = 0;
    while seen < 4000 {
        if consumer.try_pop(&mut out).is_ok() {
            seen += 1;
            // Global ordering is preserved by the shared producer_seq.
        } else {
            std::hint::spin_loop();
        }
    }
});

for e in emitters { e.join().unwrap(); }
log.join().unwrap();
```

---

## Known limitations

### `SharedRingMpsc` (composed)

- **No global FIFO**: items from producer A and producer B
  interleave at the consumer based on round-robin drain order,
  not push timestamp. If global FIFO is a correctness
  requirement, use `SharedRingMpscFifo`.
- **N files in file-backed mode**: `create_pool` creates one MMF
  per producer ring. Cross-process attach has to open all N files
  in parallel via `open_pool`; there is no single-file open shape.
- **Memory scales with N**: each producer ring carries its own
  header (192 B) + capacity * 64 B payload. At 64 producers and
  capacity 1024, that is ~4 MB of ring storage.

### `SharedRingMpscFifo` (single ring)

- **Producer-side CAS contention scales superlinearly with N**:
  at N=8 the throughput is 3.10 M items/s, an 80% drop from N=2.
  Past N=4 the composed primitive is strictly better.
- **No stuck-slot recovery on the typed handle**: the underlying
  `SharedRing` exposes `heal_stuck_slot` but the `MpscFifoConsumer`
  wrapper does not surface it. If recovery is needed, construct
  with the Vyukov `SharedRing` directly.
- **Single MMF file on disk**: identical to a plain `SharedRing`,
  so any other process opening the same file via `SharedRing::open`
  can act as a competing consumer and break the SPSC contract on
  the consumer side. Use this only when the single-consumer
  identity is enforced at the deployment level.

---

## References

- Source: `crates/subetha-cxc/src/mpsc_ring.rs`
  (`SharedRingMpsc` + `SharedRingMpscFifo` factories and the
  `Producer` / `Consumer` typed halves of each).
- Bench: `crates/subetha-cxc/examples/mpmc_shootout.rs`
  (N-sweep MPSC comparison + MPMC bench).
- Ring family siblings (pick by shape):
  [SHARED_RING_SPSC.md](./SHARED_RING_SPSC.md) -
  the SPSC primitive `SharedRingMpsc` composes.
  [SHARED_RING.md](./SHARED_RING.md) -
  Vyukov MPMC, the storage backing `SharedRingMpscFifo`.
  [SHARED_RING_MPMC.md](./SHARED_RING_MPMC.md) -
  N x M extension of the composition pattern.
