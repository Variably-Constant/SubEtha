---
title: "Concurrency and safety"
linkTitle: "Concurrency and safety"
weight: 3
---

# Concurrency and safety

Short version: **the primitives are thread-safe and process-safe, you add no
locks, and the main ring (`AdaptiveRing`) even picks and changes its own shape
under live traffic.** Your one job is to be honest about how many producers and
consumers you have. The rest of this page is the *why*.

## Do I need a `Mutex` / `RwLock` / `Arc<Mutex<…>>`?

**No - and you should not add one.** Every `Shared*` ring is already a lock-free
concurrent structure: the synchronization is atomic counters (a producer-owned
`head` and consumer-owned `tail` for SPSC; per-slot sequence numbers advanced by
CAS for MPMC). Wrapping a ring in a lock would be three kinds of wrong:

1. **Redundant** - the atomics *are* the synchronization; the lock guards
   nothing the ring does not already guard.
2. **Slower** - a lock puts a contended acquire (and a possible syscall) back on
   the fast path you reached for a ring to avoid.
3. **Broken across processes** - a `std::sync::Mutex` lives in *process-private*
   memory. It cannot synchronize two processes at all. The ring's atomics can,
   because of where they live (next section).

You share the handle directly (`&AdaptiveRing` / `&SharedRing` are `Sync`) or
each thread / process opens the same memory-mapped file. No wrapper type.

## Why it is *process*-safe, not just thread-safe

This is the one non-obvious thing, and it is the whole point. The
synchronization atomics live **inside the memory-mapped file**, not in a
process-private struct. Because the file is page-aliased to the same *physical*
pages in every participant, and CPU cache coherence is keyed on physical
addresses (not virtual ones), the exact same lock-free protocol that
synchronizes two threads also synchronizes two processes - even though they may
map the file at different virtual base addresses.

So "thread-safe" and "process-safe" are not two features here; they are one
mechanism. A normal `AtomicU64` in a `Box` is thread-safe but process-*private*;
moving the atomic into the MMF is what buys the cross-process guarantee. See
[The MMF substrate](mmf-substrate.md) for the page-alias model.

## What you are responsible for: nothing, unless you pin

You do not pick locks, and with the `AdaptiveRing` you do not pick the ring
shape or a peer ceiling either. The construction counts
(`AdaptiveRing::create_anon(max_producers, max_consumers, capacity)`) are
**sizing hints** - how many per-producer backings are pre-allocated up front.
Peers `register_producer` / `register_consumer` at runtime from any attached
process; registration past the hint **grows the ring on demand** (new
backings, published cross-process through the shared peer directory), and the
shape re-morphs to the live counts on every join and leave. Registration
fails only when you explicitly pin a ceiling with `with_contract` - the pin is
the ONLY source of `AdaptiveError::TooManyProducers` / `TooManyConsumers`.

| Ring | Producers x Consumers | Who enforces the count |
|---|---|---|
| Typed SPSC pair (`Producer` / `Consumer` from `SharedRingSpsc::create_anon_pair`) | exactly 1 x 1 | **the compiler**: both handles are `Send + !Sync + !Clone`, so a second producer cannot be cloned and a single producer cannot be shared across threads |
| `SharedRing` (Vyukov global-FIFO) / composed MPMC | N x N | the CAS protocol, at runtime - any number of producers and consumers race safely |
| `AdaptiveRing` (the main ring) | grows and shrinks with the live peer set | the shared peer directory: cross-process slot claims, on-demand backing growth, automatic shape morphs; a declared `with_contract` ceiling is the only refusal |

Pick the typed SPSC pair only when the topology is genuinely one producer and
one consumer forever and you want the type system to weld that in. For anything
else - or when you are not sure - use the `AdaptiveRing`; it carries no such
constraint and adapts the shape for you.

## The `AdaptiveRing` changes shape while I use it - is *that* safe?

Yes, including during the change itself, which is the hard part. The
[`AdaptiveRing`](../reference/subetha-cxc/rings/shared-ring-adaptive.md) keeps
**all four backings (SPSC / MPSC / MPMC / Vyukov) pre-allocated**, so a morph
never allocates or tears anything down. A morph is a single atomic re-point:

1. `pin_generation.fetch_add(1)` - any caller holding a direct pinned pointer
   sees `is_still_valid() == false` on its next check and re-pins.
2. the old shape is published as **stale** (a `Release` store) *before* the new
   shape tag, so any consumer that sees the new shape also sees the stale marker.
3. new `try_send`s route to the new backing; `try_recv` **drains the stale
   backing first**, so an item pushed microseconds before the flip is consumed
   before the new backing is read - nothing is lost, FIFO across the seam holds.

A second morph is refused (`RingError::StaleBacklog`) until the prior stale
backing empties, so at most one backing is ever draining. No data is copied
between backings; the old backing keeps its single reader (the stale walk) and
the new one starts empty.

This is verified by the test suite, not asserted - among others,
`morph_preserves_in_flight_items_via_stale_walk`,
`second_morph_blocked_until_stale_backlog_drains`, and
`stamped_items_survive_shape_morphs` all exercise shape morphs under live
traffic. The composed MPMC path (`SharedRingMpmc`) is exercised at 4
producers x 2 consumers, 40,000 items (`PER_PRODUCER = 10_000`), zero lost
and zero duplicated.

## What you must not do

- **Do not put two producers on a single-producer ring.** On the typed SPSC
  pair this will not compile. On a raw `SpscRingCore` opened cross-process,
  nothing at compile time stops you - SPSC means exactly one producer and one
  consumer *total*, and cross-process that is a convention you uphold. If in
  doubt, use the `AdaptiveRing` or `SharedRing` (MPMC), which have no such limit.
- **Do not wrap a ring in a process-private lock** (`Mutex` / `RwLock`). It adds
  cost and does not work across the process boundary the in-MMF atomics handle.
- **Do not push or pop with a peer id you did not register.** Slot ids come
  from `register_producer` / `register_consumer` (shared, recycled on
  unregister); inventing one risks two writers on a single-writer backing.

## In one sentence

You just `register`, `send` / `recv`, and `unregister`: the ring claims a slot
for each peer, grows its backings when peers outnumber the construction hint,
holds no locks, picks its own shape, and stays thread- and process-safe even
as it morphs beneath your live producers and consumers.
