# SharedRingMpmc

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Default](https://img.shields.io/badge/default-composed_NxM_Lamport-brightgreen)
![Override](https://img.shields.io/badge/global_FIFO-SharedRing_Vyukov-orange)
![Contract](https://img.shields.io/badge/contract-compile--time_per--handle_uniqueness-success)

Multi-producer / multi-consumer ring composed from N x M
independent Lamport SPSC rings. N producers each own one ring;
M consumers each statically own a round-robin partition of the N
rings. Each consumer is the sole drainer of its partition, so
the consumer-side CAS Vyukov MPMC needs is not present here.

Per-producer FIFO is preserved (each producer's items arrive at
one consumer in push order). Global FIFO across producers is not
preserved; items from different producers interleave at different
consumers based on which consumer owns which producer's ring.
When global FIFO matters, use [`SharedRing`](./SHARED_RING.md)
(Vyukov MPMC) instead.

> **The "composed N x M Lamport grid" primitive.** Push cost is
> pure Lamport SPSC (1 Acquire load + 1 Release store). Pop cost
> is the same on the consumer's first non-empty ring, plus one
> Acquire load per empty ring scanned before it.

**Constraints (read first):**

- **`n_producers >= n_consumers >= 1`**: each consumer must own
  at least one ring. The factory rejects `n_producers <
  n_consumers` at runtime.
- **N producer handles + M consumer handles**, both
  `Send + !Sync + !Clone`. The compiler enforces one thread per
  producer and one thread per consumer.
- **Payload up to `SPSC_PAYLOAD_BYTES = 64`** bytes per slot.
- **Each ring's capacity must be a power of 2**.
- **In-process anonymous** (`create_anon_grid`) or
  **cross-process file-backed** (`create_grid` / `open_grid`,
  one file per producer ring named `<path_prefix>.{i}.bin`).

---

## Table of contents

- [What it is](#what-it-is)
- [Round-robin partitioning](#round-robin-partitioning)
- [Worked examples](#worked-examples)
- [Bench evidence](#bench-evidence)
- [When to reach for `SharedRing` (Vyukov) instead](#when-to-reach-for-sharedring-vyukov-instead)
- [Known limitations](#known-limitations)
- [References](#references)

---

## What it is

```text
+----------+   +----------+   +----------+   +----------+
|Producer 0|   |Producer 1|   |Producer 2|   |Producer 3|
+----------+   +----------+   +----------+   +----------+
     |               |               |               |
     v               v               v               v
+---------+    +---------+    +---------+    +---------+
| Ring 0  |    | Ring 1  |    | Ring 2  |    | Ring 3  |  (Lamport SPSC each)
+---------+    +---------+    +---------+    +---------+
     |               |               |               |
     v               v               v               v
+----------+   +----------+   +----------+   +----------+
|Consumer 0|   |Consumer 1|   |Consumer 0|   |Consumer 1|  (round-robin assign)
+----------+   +----------+   +----------+   +----------+
```

Consumer i owns producer rings i, i + M, i + 2*M, etc. For
N=4 / M=2: consumer 0 drains rings {0, 2}, consumer 1 drains
rings {1, 3}.

For the balanced case N = M, every consumer owns exactly one
ring and the per-pop cost is pure Lamport SPSC. For the
unbalanced case (N > M), each consumer round-robins its
ceil(N / M) rings the same way `SharedRingMpsc`'s consumer
does.

---

## Round-robin partitioning

The factory:

```rust
pub fn create_anon_grid(
    n_producers: usize,
    n_consumers: usize,
    capacity: usize,
) -> Result<(Vec<MpmcProducer>, Vec<MpmcConsumer>), RingError>;
```

Builds N rings, then assigns ring i to consumer (i % M). At
N=8 / M=2 the layout is:

```
ring 0 -> consumer 0   ring 4 -> consumer 0
ring 1 -> consumer 1   ring 5 -> consumer 1
ring 2 -> consumer 0   ring 6 -> consumer 0
ring 3 -> consumer 1   ring 7 -> consumer 1
```

Static partitioning means each consumer is the sole drainer of
its rings. No consumer-side CAS, no work-stealing. The cost is
load-balance brittleness: if one consumer's producers are
backlogged and another's are quiet, the second consumer goes
idle while the first falls behind. For symmetric workloads
(N producers all pushing at similar rates) the partition is
already balanced.

For workloads with skewed producer rates, a `SharedDeque`
work-stealing variant (see [SHARED_DEQUE.md](./SHARED_DEQUE.md))
is the better primitive: consumers actively steal from any
ring rather than draining a fixed subset.

---

## Worked examples

### Symmetric MPMC, in-process

```rust
use subetha_cxc::SharedRingMpmc;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

let (producers, consumers) =
    SharedRingMpmc::create_anon_grid(4, 4, 1024)?;

let prods: Vec<_> = producers.into_iter().enumerate().map(|(pid, p)| {
    std::thread::spawn(move || {
        for i in 0..10_000u32 {
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            buf[..4].copy_from_slice(&(pid as u32).to_le_bytes());
            buf[4..8].copy_from_slice(&i.to_le_bytes());
            while p.try_push(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
    })
}).collect();

let cons: Vec<_> = consumers.into_iter().map(|c| {
    std::thread::spawn(move || -> u32 {
        let mut got = 0u32;
        let mut out = [0u8; SPSC_PAYLOAD_BYTES];
        // Each consumer drains until its partition is empty for
        // a long enough quiet period; production code uses
        // explicit shutdown signalling, not a fixed iteration
        // count.
        while got < 20_000 {
            if c.try_pop(&mut out).is_ok() {
                got += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        got
    })
}).collect();

for p in prods { p.join().unwrap(); }
let totals: Vec<u32> = cons.into_iter().map(|c| c.join().unwrap()).collect();
assert_eq!(totals.iter().sum::<u32>(), 40_000);
```

### Asymmetric grid (4 producers, 2 consumers)

```rust
use subetha_cxc::SharedRingMpmc;

let (producers, consumers) =
    SharedRingMpmc::create_anon_grid(4, 2, 256)?;
// consumer 0 drains rings 0, 2; consumer 1 drains rings 1, 3.
// Each consumer round-robins its 2-ring subset.
```

### Cross-process file-backed grid

Process A (creates the grid + runs all producers):

```rust
use subetha_cxc::SharedRingMpmc;

let (producers, _consumers) =
    SharedRingMpmc::create_grid("/tmp/mpmc", 4, 2, 1024)?;
// _consumers drops on this side; consumers run on process B.
// Producer threads spawned here push to their rings.
```

Process B (attaches as consumer side):

```rust
use subetha_cxc::SharedRingMpmc;

let (_producers, consumers) =
    SharedRingMpmc::open_grid("/tmp/mpmc", 4, 2, 1024)?;
// _producers drops on this side; consumers drain their partitions.
```

Both sides see the same N producer rings (files
`/tmp/mpmc.0.bin`, `/tmp/mpmc.1.bin`, `/tmp/mpmc.2.bin`,
`/tmp/mpmc.3.bin`) and the round-robin partition is deterministic
from N + M alone, so the two sides agree on which consumer owns
which rings without any out-of-band coordination.

---

## Bench evidence

`crates/subetha-cxc/examples/mpmc_shootout.rs`, 4 producers x
250,000 items each = 1,000,000 total, busy-spin on Full / Empty,
best-of-5 trials with one warmup pass. Zen+ R7 2700 / Windows 11.

| Variant | Throughput | vs Vyukov | vs crossbeam |
|---|---:|---:|---:|
| **`SharedRingMpmc` (composed 4 x 4 Lamport grid)** | **20.96 M items/s** | **3.49x** | **3.10x** |
| `SharedRing` (Vyukov MPMC) | 6.00 M items/s | baseline | 0.89x |
| `crossbeam_channel::bounded(4096)` MPMC | 6.75 M items/s | 1.13x | baseline |

The composed grid wins by ~3x over both the Vyukov MPMC ring and
crossbeam's bounded MPMC channel because every push is pure
Lamport SPSC (zero CAS contention) and every pop is the same on
its owning ring. Vyukov MPMC under 4 producers + 4 consumers
fights for two cache-line-contended counters (`producer_seq` and
`consumer_seq`); crossbeam's bounded channel does similar
producer-side + consumer-side coordination.

### Rule 3b bench audit

- **Same shape across contenders**: 4 producers + 4 consumers,
  250,000 items per producer, 16-byte payloads, capacity 4096.
- **Same busy-spin Full / Empty loop** for all three.
- **Best-of-5 with one warmup** to dampen Windows scheduler
  variance.
- **Composed grid sized exactly for the bench**: 4 producer
  rings + 2 consumer partitions of 1 ring each (the M=4 case
  here has each consumer owning one ring, the cleanest shape).

---

## When to reach for `SharedRing` (Vyukov) instead

Pick the Vyukov MPMC ring when **any of these is a hard
requirement**:

- **Global FIFO across all producers**: events must arrive at
  consumers in monotonic `producer_seq` order. Composed grids
  give per-producer FIFO only.
- **Total ordering for a transaction stream**: each consumer
  needs to see the same ordering of every producer's pushes
  (composed grids give different consumers different
  interleavings).
- **Single MMF file**: the composed grid uses N files (one per
  producer ring). A single `SharedRing` is one file regardless
  of producer count, which matters for cross-process attach
  protocols that expect one file per channel.

For everything else (work distribution, fan-in pipelines, task
queues, telemetry aggregation, request dispatch), the composed
grid is the right default.

---

## Known limitations

- **`n_producers >= n_consumers`**: every consumer must own at
  least one ring. The factory panics at runtime if violated.
- **Per-producer FIFO only**: cross-producer ordering is the
  round-robin drain order, which depends on consumer scheduling.
  Use `SharedRing` (Vyukov MPMC) for global FIFO.
- **Static partitioning**: consumer i owns rings (i mod M)
  forever. If one consumer's producers go quiet while another's
  back up, the quiet consumer idles. For dynamic load balancing
  use a [`SharedDeque`](./SHARED_DEQUE.md) work-stealing variant.
- **N files in file-backed mode**: `create_grid` produces one
  MMF per producer ring at `<path_prefix>.{i}.bin`. Cross-process
  attach via `open_grid` opens all N files in parallel.
- **Memory scales with N**: each producer ring carries its own
  header (192 B) + capacity * 64 B payload. At 64 producers and
  capacity 1024, that is ~4 MB of ring storage.

---

## References

- Source: `crates/subetha-cxc/src/mpmc_ring.rs` (`SharedRingMpmc`
  factory + `MpmcProducer` / `MpmcConsumer` typed handles).
- Bench: `crates/subetha-cxc/examples/mpmc_shootout.rs`
  (MPMC head-to-head against `SharedRing` Vyukov and
  `crossbeam_channel::bounded`).
- Ring family siblings (pick by shape):
  [SHARED_RING_SPSC.md](./SHARED_RING_SPSC.md) -
  the SPSC primitive the grid composes per producer.
  [SHARED_RING_MPSC.md](./SHARED_RING_MPSC.md) -
  the N-producer single-consumer case (M=1).
  [SHARED_RING.md](./SHARED_RING.md) -
  Vyukov MPMC, the global-FIFO override.
  [SHARED_BROADCAST_RING.md](./SHARED_BROADCAST_RING.md) -
  fan-out variant where every consumer sees every item (the
  other shape sometimes called "MPMC").
  [SHARED_DEQUE.md](./SHARED_DEQUE.md) -
  work-stealing variants for skewed-producer loads where static
  partitioning is the wrong primitive.
