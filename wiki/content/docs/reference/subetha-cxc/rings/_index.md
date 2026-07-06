---
title: "Rings & Stacks"
weight: 200
sidebar:
  open: true
---

# Rings, stacks, and queues

Bounded, lock-free FIFO / LIFO / pub-sub structures backed by an MMF.

## Pick by producer / consumer shape

| Shape | Default | Override (when global FIFO matters) | Doc |
|---|---|---|---|
| 1P / 1C (SPSC) | `SharedRingSpsc` (Lamport pair) | none needed | [shared-ring-spsc](shared-ring-spsc/) |
| 1P / 1C with variable / large payloads | `FrameRing` (self-describing frame: inline small records, spill large ones to a byte region) | `AdaptiveRing::send_frame` for the all-shapes form | [frame-ring](frame-ring/) |
| NP / 1C (MPSC) | `SharedRingMpsc` (composed N Lamport rings) | `SharedRingMpscFifo` (single ring) | [shared-ring-mpsc](shared-ring-mpsc/) |
| 1P / NC fan-out (every consumer reads every item) | `SharedBroadcastRing` | none needed | [shared-broadcast-ring](shared-broadcast-ring/) |
| 1P / NC work-distribute (each item to one consumer) | `SharedDeque` family | none needed | [shared-deque](shared-deque/) |
| NP / NC (MPMC) | `SharedRingMpmc` (composed N x M Lamport grid) | `SharedRing` (Vyukov MPMC) | [shared-ring-mpmc](shared-ring-mpmc/) |
| Shape unknown / morphs over runtime | `AdaptiveRing` (all 4 shapes pre-allocated; peers register / unregister at runtime, backings GROW past the construction hint, shape auto-morphs to the live counts; pins to native primitive speed once stable) | none needed | [shared-ring-adaptive](shared-ring-adaptive/) |
| Global FIFO needed sometimes / decided at runtime | `AdaptiveRing::with_ordering_stamps()` (push stamps + a shared ordering flag; the merge flip delivers global FIFO within stamp skew on the composed rings, retroactive over the backlog). For EXACT delivery on the SharedCounter path, `AdaptiveOrderedReceiver` (auto reorder-vs-strict). | `RingShape::Vyukov` morph for unstamped rings | [adaptive-ordering](adaptive-ordering/) |
| Shape + locale both morph at runtime | `LocaleAdaptiveRing` (Anon / File / ShmFs locale wrapped around AdaptiveRing) | none needed | [locale-adaptive-ring](locale-adaptive-ring/) |
| Slot count grows / shrinks at runtime under load (fan-in family) | `CapacityAdaptiveRing` (ArcSwap state-swap to a fresh backing at any pow2 capacity; stale-list draining; pinned hot path reaches native speed) | none needed | [capacity-adaptive-ring](capacity-adaptive-ring/) |
| Slot count grows / shrinks at runtime under load (broadcast fan-out) | `CapacityBroadcastRing` (same ArcSwap state-swap pattern; per-subscriber positions baked into the underlying broadcast ring header) | none needed | [capacity-broadcast-ring](capacity-broadcast-ring/) |
| Slot count grows / shrinks at runtime under load (pub/sub fan-out) | `CapacityPubSubRing` (chain-of-backings model; subscribers carry `(backing_idx, position)` and advance through the chain) | none needed | [capacity-pubsub-ring](capacity-pubsub-ring/) |
| 1P / NC pub-sub with per-subscriber positions | `PubSubRing` + `PubSubSubscriber` | none needed | [pubsub-ring](pubsub-ring/) |
| 1P / 1C blocking send / recv (cross-process futex) | `BlockingSpscRing` | none needed | [blocking-spsc-ring](blocking-spsc-ring/) |
| NP / 1C blocking send / recv (cross-process futex) | `BlockingMpscRing` | none needed | [blocking-mpsc-ring](blocking-mpsc-ring/) |
| NP / MC blocking send / recv (cross-process futex) | `BlockingMpmcRing` | none needed | [blocking-mpmc-ring](blocking-mpmc-ring/) |

The composed family drops the per-slot sequence atomic that
Vyukov MPMC needs for global FIFO, trades global ordering for
per-producer FIFO, and runs 2-3.5x faster on every measured shape.
When the ordering trade needs to be revisited at runtime, the
[ordering axis](adaptive-ordering/) makes global FIFO a flag on the
same composed rings: stamped pushes + a k-way min-stamp merge at
the consumer, with a cross-producer inversion counter as the
observable signal.

The blocking variants layer `CrossProcessWaker` (a userspace
futex slot list in MMF) on top of the non-blocking SPSC / MPSC
/ MPMC rings so consumers can park kernel-side instead of
spinning when the ring is empty. SHARED `futex` on Linux and
non-PRIVATE `_umtx_op` on FreeBSD carry the wake across the
process boundary; on Windows the hardware monitor tier
(MONITORX/UMONITOR, physical-address based) carries the
cross-process wake while `WaitOnAddress` serves anon-backed
intra-process wakers. See [`cross-process-waker`]({{<
ref "../coordination-types/cross-process-waker" >}}) for the
underlying protocol and the measured wait ladder.

## All ring + queue primitives

| Primitive | Shape | Producers / consumers |
|---|---|---|
| [Shared Ring](shared-ring/) | Vyukov MPMC FIFO ring | Multiple-producer, multiple-consumer, global FIFO across all producers |
| [Shared Ring SPSC](shared-ring-spsc/) | Lamport 1983 SPSC pair | 1 producer + 1 consumer enforced at compile time |
| [Frame Ring](frame-ring/) | Self-describing variable-payload SPSC (inline small records, region-spill large ones) | 1 producer + 1 consumer; carries any payload size |
| [Shared Ring MPSC](shared-ring-mpsc/) | Composed N Lamport rings + Fifo override | N producers + 1 consumer enforced at compile time |
| [Shared Ring MPMC](shared-ring-mpmc/) | Composed N x M Lamport grid | N producers + M consumers enforced at compile time |
| [Shared Ring Adaptive](shared-ring-adaptive/) | Shape-morphing ring with all 4 shapes pre-allocated; per-producer backings grow on demand as peers register | Any shape; peers join / leave at runtime across processes; shape auto-morphs to the live counts; PinnedRing handoff to native primitive speed |
| [Adaptive Ordering](adaptive-ordering/) | Ordering axis on stamped AdaptiveRings: TSC / counter / monotonic push stamps, inversion metric, MMF-resident merge flag, strict watermark gate, single-drainer lease | Composed shapes with runtime-switchable global FIFO; `try_recv_with_stamp` / pinned `ordered_try_pop` |
| [Locale Adaptive Ring](locale-adaptive-ring/) | Three-locale wrapper (Anon / File / ShmFs) around AdaptiveRing; ships with `LocaleAdaptiveRingSidecar` + `DefaultLocalePolicy` for hysteresis-gated migrations | Any shape across any locale; PinnedLocale handoff chains into PinnedRing |
| [Capacity Adaptive Ring](capacity-adaptive-ring/) | Runtime-resizable AdaptiveRing wrapper; ArcSwap state-swap + stale-list; ships with `CapacityAdaptiveRingSidecar` + `DefaultCapacityPolicy` for hysteresis-gated grow/shrink | Any shape; capacity morphs at runtime; PinnedCapacity -> PinnedRing chain |
| [Capacity Broadcast Ring](capacity-broadcast-ring/) | Capacity-morph wrapper around `SharedBroadcastRing`; same ArcSwap state-swap pattern with `lag(idx) == 0` spin discipline | 1 producer, N subscribers, capacity morphs at runtime |
| [Capacity PubSub Ring](capacity-pubsub-ring/) | Capacity-morph wrapper around `PubSubRing`; chain-of-backings model; subscribers walk `(backing_idx, position)` across the chain | 1 producer, N subscribers, capacity morphs at runtime |
| [PubSub Ring](pubsub-ring/) | One-producer many-subscriber broadcast with per-slot sequence numbers | 1 publisher + N subscribers, each tracks its own absolute position via SubscriberPosition |
| [Shared Broadcast Ring](shared-broadcast-ring/) | Pub/sub ring (KeepLastN; producer never blocks) | Single producer, multiple consumers (each sees full stream); `MAX_CONSUMERS = 16` |
| [Shared Treiber Stack](shared-treiber-stack/) | LIFO stack | Lock-free CAS-based push/pop |
| [Shared Deque](shared-deque/) | Work-stealing deque | Single owner (LIFO push / pop), multiple thieves (FIFO steal) |
| [Shared Deque (KHPD)](shared-deque-khpd/) | Publication-line deque | Single owner stages + publishes K items per cache-line, multiple thieves CAS-claim whole lines |
| [Shared Deque (KHL)](shared-deque-khl/) | K-axis Hierarchical LCRQ (SubEtha-novel hybrid) | Pulls KHPD's per-slot packing + LOH's per-batch counter amortization + Chase-Lev's owner-private tail all at once |
| [Shared Deque (LOH)](shared-deque-loh/) | LCRQ-on-LIFO Hybrid deque | Single owner stages in a process-private LIFO (no atomic) + migrates batches into a Vyukov-sequence ring; multiple thieves CAS-claim per-slot |
| [Shared Deque (URD)](shared-deque-urd/) | Per-thief mailbox deque (UMWAIT / PauseSpin) | Single owner picks target mailbox by round-robin, each thief reads its own mailbox (no shared CAS contention) |
| [Deque Dispatcher](dispatch-deque/) | Per-shape routing composition | Owns one handle per variant; picks the variant per call based on `WorkloadShape` (n_thieves, batch_size, wait_idle) |
| [Blocking SPSC Ring](blocking-spsc-ring/) | Lamport SPSC + 2 `CrossProcessWaker` for kernel-park on empty/full | 1 producer + 1 consumer; `send_blocking` / `recv_blocking` with timeout |
| [Blocking MPSC Ring](blocking-mpsc-ring/) | Composed-SPSC MPSC + per-ring producer wakers + shared consumer waker | N producers + 1 consumer; blocking send / recv with cross-process futex |
| [Blocking MPMC Ring](blocking-mpmc-ring/) | Composed-SPSC MPMC grid + per-ring producer wakers + per-subset consumer wakers | N producers + M consumers; blocking send / recv with cross-process futex |
| [Async SPSC Ring](async-spsc-ring/) | `Future`-shaped adapter on `BlockingSpscRing`; `.recv().await` / `.send().await` via short-lived worker threads | Executor-agnostic async integration; one worker thread per in-flight future |

For prose overview of the Vyukov MPMC ring, see [shared-ring](shared-ring/).

## Benchmarks

- [Throughput harness + how to reproduce](throughput/) - what the bench measures, how to run it, what the numbers mean.
- [Throughput results (full matrix)](throughput-results/) - auto-generated tables: headline best-per-primitive, plus per-(primitive, locale) deep-dive across capacity x P x C cells.
