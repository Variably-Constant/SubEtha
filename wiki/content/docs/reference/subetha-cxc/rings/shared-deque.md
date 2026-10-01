---
title: "Shared Deque"
weight: 40
---

# SharedDeque&lt;T&gt;

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/protocol-Chase--Lev_SPMC-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Lock-free Chase-Lev work-stealing deque backed by a memory-mapped
file. Owner-side `push` and `pop` are atomic-free on the fast path
(Relaxed load + Relaxed store on the `bottom` index); thief-side
`steal` pays exactly one CAS per successful claim. Lifting the
protocol into an MMF makes the same primitive serve cross-thread
*and* cross-process work-stealing.

> **The "Chase-Lev SPMC + MMF" primitive.** Source: David Chase
> and Yossi Lev, *Dynamic Circular Work-Stealing Deque*, SPAA 2005.
> See the [Citations and references](../../../explanation/citations/)
> page for the full attribution.

**Constraints (read first):**

- **`T: Marshal`**: the payload type must implement the
  [`Marshal`](../../subetha-core/) contract from `subetha-core`
  (the type-system rule that the value's bytes mean the same thing
  in every address space). `u8` / `u16` / `u32` / `u64` / `u128`
  and their signed / floating-point counterparts have `Marshal`
  auto-impls; arrays and 2-tuples of `Marshal` types compose
  automatically. A closure that captures pointers cannot be stored
  directly; it travels through the
  [pass registry](../coordination-types/pass-registry/) as a
  `(closure_id, args)` pair whose `args` are `Marshal`.
- **Slot width** is `T::PAYLOAD_BYTES` rounded up to 8-byte
  alignment (minimum 8 bytes per slot).
- **Capacity must be a power of two** so the slot-index computation
  is `b & (capacity - 1)`.
- **Single owner**: the protocol assumes one fixed thread (or one
  fixed process) calls `push` and `pop`. Multiple threads pushing
  or popping concurrently from the owner end breaks Chase-Lev's
  invariants. Any number of thieves can call `steal` concurrently.
- **Cross-process backed by MMF.** A second process opens the same
  file via `SharedDeque::open_as_thief(path)` and steals from a
  remote owner with the identical CAS protocol.

---

## Why Chase-Lev specifically

Vyukov's MPMC ring (the `SharedRing` primitive) treats every
producer and every consumer symmetrically: every operation pays a
CAS on the relevant sequence counter. Chase-Lev breaks the symmetry:

- **Owner push** is a Relaxed load on `bottom`, an Acquire load on
  `top` (to check fullness), the marshal, a `Release` fence, then a
  Relaxed store on `bottom`. No CAS and no full (SeqCst) barrier - the
  fence is Release-only.
- **Owner pop** is a Relaxed store on `bottom` + a SeqCst fence + a
  Relaxed load on `top`. Only the contended case (one item left,
  thief racing) escalates to a CAS.
- **Thief steal** is one Acquire load on `top`, a SeqCst fence,
  one Acquire load on `bottom`, a slot read, and one CAS on `top`.

The asymmetry is the architectural point: most work-stealing
schedulers have one worker thread per CPU core, and that worker
hits its own deque's owner end much more often than thieves hit it.
The owner's atomic-free fast path is the saving.

## Cost summary

Measured on an AMD Ryzen 7 2700 (Zen+, 16 logical threads) under
Criterion publication-grade defaults (warm-up 3 s, measurement 5 s,
100 samples).

| Workload | `SharedDeque` (MMF Chase-Lev) | `crossbeam_deque` (in-process Chase-Lev) | `Mutex<VecDeque>` | `SharedRing` (Vyukov MPMC) |
|---|---:|---:|---:|---:|
| Single-thread push/pop (microbench) | **15.78 ns** | 16.00 ns | 33.63 ns | 23.73 ns |
| 1 owner + 1 thief, 10k items | 454 µs | **361 µs** | 559 µs | 508 µs |
| 1 owner + 4 thieves, 8k items | **1.15 ms** | 1.56 ms | 9.32 ms | 2.40 ms |

Reading the table:

- **Single-thread**: `SharedDeque` and `crossbeam_deque` tie at
  16 ns. The MMF page-fault cost is invisible because the only page
  touched on the hot path is L1-resident, and Chase-Lev's owner-side
  fast path is identical between the two implementations. `Mutex`
  is 2.1x slower (the lock dominates a 16-ns op); `SharedRing` is
  1.5x slower because Vyukov's per-slot sequence-number CAS is
  unavoidable even when uncontended.
- **1 owner + 1 thief**: `crossbeam_deque` wins (361 µs) as a pure
  in-process implementation; `SharedDeque` (454 µs) and
  `SharedRing` (508 µs) pay roughly the same MMF overhead.
- **1 owner + 4 thieves** (the workload Chase-Lev was designed for):
  **`SharedDeque` wins at 1.15 ms**, beating `crossbeam_deque` by
  1.36x. The likely cause is that `crossbeam_deque` uses
  epoch-based memory reclamation for its dynamically resizing slot
  array; `SharedDeque` has fixed capacity and skips epoch
  participation entirely. `SharedRing` is 2.1x slower than
  `SharedDeque` because all four thieves CAS the same
  `consumer_seq` counter. `Mutex<VecDeque>` is 8x slower (textbook
  lock-contention penalty).

The headline architectural payoff: **cross-process Chase-Lev on
fixed capacity matches in-process Chase-Lev with epoch GC at low
thief counts and beats it at higher counts**, on the same physical
silicon, with kernel uninvolved in the steal hot path. The MMF
deployment is a free upgrade.

Bench file:
[`crates/subetha-cxc/benches/shared_deque.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/benches/shared_deque.rs).

## API surface

```rust
use subetha_cxc::SharedDeque;

// Owner: create the MMF and the owner handle.
let owner = SharedDeque::<u64>::create("/tmp/jobs.bin", 4096).unwrap();

// Owner side (single-thread only on this handle).
owner.push(&42u64).unwrap();
// Batched owner push: amortizes ONE top-load + ONE Release fence + ONE
// bottom-store across the whole batch (the producer-fast path); returns
// Err(Full) and writes nothing if the batch would overflow capacity.
owner.push_batch(&[1u64, 2, 3]).unwrap();
let v: Option<u64> = owner.pop();

// Thief: any other thread, or any other process, opens the same file.
let thief = SharedDeque::<u64>::open_as_thief("/tmp/jobs.bin").unwrap();
let stolen: Option<u64> = thief.steal();

// Approximate length (heuristic, not authoritative under concurrent
// modification).
let n = owner.approx_len();

// Disk-persistent deployment: force the mapped region to disk.
owner.flush().unwrap();
```

`push_batch_with(n, |i, slot| ...)` is the raw-bytes batched push: it
reserves `n` slots under one `top.load` and calls the closure to fill each
slot's bytes directly, bypassing the `Marshal` indirection - the path the
fat-slot deque variants (KHL / KHPD / LOH) build their per-cache-line
packing on. `open_as_thief` validates the header's `slot_bytes` against
`T::PAYLOAD_BYTES` and returns `DequeError::SlotBytesMismatch` on a type
mismatch; the sizing helpers `slot_bytes_for::<T>()` and
`deque_file_size::<T>(capacity)` compute the slot width (rounded to 8 bytes,
min 8) and total MMF size.

The `Marshal` trait is in `subetha-core`; user-defined types
become storable in `SharedDeque` by `unsafe impl Marshal for ...`
(the trait is `unsafe` because the position-independence contract
is the implementer's responsibility).

| Method | Role | Notes |
|---|---|---|
| `SharedDeque::create` | owner | create the MMF-backed deque at a fixed capacity |
| `SharedDeque::open_as_thief` | thief | attach a second thread or process to steal from the owner |
| `push` / `pop` | owner | bottom-end push / pop, no CAS on the fast path |
| `push_batch` / `push_batch_with` | owner | bulk bottom-end push |
| `steal` | thief | top-end steal, one CAS |
| `approx_len` | any | lock-free size estimate (top and bottom may move under it) |
| `capacity` | any | the fixed slot count set at create time |
| `flush` | owner | msync the backing for durability |

Errors surface as `DequeError`. Capacity is fixed at create time and
must be a power of two (`create` returns `DequeError::InvalidCapacity`
otherwise); the paper's resizing variant is a different primitive
shape and is not what this implements.

## Layout

```text
+-----------------------------------+
| DequeHeader (64 bytes, one line)  |
|   magic, capacity                 |
|   slot_bytes, alignment           |
|   owner_pid                       |
|   top: AtomicI64                  |
|   bottom: AtomicI64               |
|   epoch, layout_tag               |
+-----------------------------------+
| Slot[0]  (slot_bytes)             |  marshaled T payload
| Slot[1]  ...                      |
+-----------------------------------+
```

The header is exactly one 64-byte cache line, held there by a
compile-time assertion, so the owner's `bottom` stores and a thief's
`top` CAS travel one line's coherence path. `owner_pid` records the
creating process and the protocol never reads it; `epoch` starts at 0
and `SharedDeque` never advances it. A typed deque declares
`align_of::<T>()` as its `alignment` and 0 as its `layout_tag`, and a
raw attacher refuses a region whose alignment or tag differs from the
layout it declares.

## When to reach for it

Use a `SharedDeque` when one producer (the owner) generates work that
many consumers drain, and you want the owner's enqueue to stay almost
free while consumers balance themselves by stealing: in-process worker
pools, or cross-process worker fleets that attach to the owner's file.
For symmetric many-to-many handoff where every participant both
produces and consumes, reach for a ring instead
([`SharedRingMpmc`](shared-ring-mpmc/)).

## See also

- [`SharedRing`](shared-ring/) - the symmetric MPMC ring that
  `SharedDeque` complements (lock-free MPMC vs lock-free SPMC with
  owner-side asymmetry).
- [`SharedTreiberStack`](shared-treiber-stack/) - the lock-free
  LIFO stack; same MMF family, different access pattern.
- [Citations and references](../../../explanation/citations/) - the
  Chase-Lev and Blumofe-Leiserson papers `SharedDeque` is built on.
