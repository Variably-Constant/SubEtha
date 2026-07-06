---
weight: 20
---

# Frozen handshakes

Every concurrency primitive is a handshake between two roles. The
writer and the reader. The producer and the consumer. The owner
and the borrower. The primitive's job is to coordinate that
handshake. Its design is the set of decisions about how each end
behaves.

A primitive is **frozen** when those decisions are baked into
the type, fixed at construction, and the same on every call. The
median program wants exactly that. A pathological program does
not.

## What a freeze looks like

`std::sync::Mutex<T>` is the easy example. Its `lock()` is
spin-then-park: a few hundred cycles of CAS attempts, then a
syscall to `WaitOnAddress` (Windows) or futex (Linux). That
strategy is a compile-time choice. There is no `Mutex::set_strategy`.
You cannot, halfway through your workload, decide that your
lock should never park because contention is rare and the syscall
costs more than the spin saves. The decision was made when
`std::sync::Mutex<T>` was designed.

`Arc<T>` freezes a refcount layout. Two `usize` counters on the
heap, a control block per value, atomic RMW on every clone and
drop. That is correct for most uses. It is wrong when every clone
is contested by sixteen threads and the refcount line ping-pongs.
But `Arc<T>` has no escape; you get the layout or you do not use
the type.

`HashMap<K, V, S>` freezes a hasher into the type parameter `S`.
The default `RandomState` re-seeds per process for DoS resistance.
That choice is correct if you serve untrusted requests. It is
wrong if you want two processes to share a hash map and agree on
key locations - because `RandomState`'s seed differs per process,
the same key hashes to different slots in the two maps. So `std`
gives you no shared-hash-map type. You compose your own with a
deterministic hasher, and the layout question becomes yours to
answer.

`std::sync::mpsc::channel` freezes single-process-in-memory.
There is no `mpsc::open` that takes a path. The MPSC discipline
itself (one receiver) is part of the freeze. To go cross-process
you reach for sockets or pipes, both of which freeze a different
set of decisions and pay a syscall on every send.

The pattern: each `std` primitive picks one point on a
multi-dimensional design space and freezes the rest. The point is
the median-correct one. Workloads that need a different point
have no recourse short of replacing the type.

## Why freeze at all

The freeze is not laziness. It is what makes the primitive cheap
on the median.

`Mutex::lock` is fast on the uncontended path precisely because
it does not have to dispatch through a strategy lookup. The
compiler sees the concrete park primitive and inlines it. A
mutex type with a strategy field pays a load and a branch on
every `lock()` even when nobody contests it.

`Arc::clone` is one atomic increment because the refcount layout
is known. Make the layout a runtime choice and every clone has
to load a layout descriptor and dispatch on it. The single-threaded
program that just wants three handles to the same `Vec` pays
nothing for the contention case it does not have.

So the freeze is the deliberate exchange. Hot-path cost goes down,
adaptability goes to zero. For the median user, that exchange is
right.

## Where it stops working

Two tails make it wrong.

**Topology tail.** Your data does not stay in one address space.
A cache eviction agent reports to a metrics scraper running in a
different process. A long-running daemon hands off state to its
successor across a restart. A producer wants to feed a consumer
that may not even be running yet. The primitive's freeze on
"both ends share the same address space" is what cuts you off.

**Workload tail.** Your op stream is not the one the freeze was
designed for. The mutex is uncontended for the first ten minutes
then hammered by twenty threads. The cell starts read-heavy and
turns write-heavy after lunch. The hash map's key distribution
inverts when traffic shifts from one country to another. The
primitive's freeze on "one strategy fits the whole workload" is
what costs you.

These two tails are independent. The MMF cross-process case may
also have a uniform workload. The contended in-process case may
fit entirely in one address space. They do not imply each other,
they do not compose into one solution. Each demands its own
un-freezing.

## Two axes, orthogonal

SubEtha picks both axes.

**Topology, un-frozen.** `subetha-cxc`'s
`SharedRing` / `SharedHashMap` / `SharedRWLock` / and the rest
lift the primitive's state into a memory-mapped file. The same
byte layout serves cross-thread (two threads in one process map
the same file), cross-process (two processes open the same file
and the kernel page-aliases them onto identical physical pages),
and disk-persistent (the file survives a process restart and the
next consumer reopens where the previous left off). One byte
layout, three deployment modes, no recompile in between. See
[the MMF substrate explanation](mmf-substrate.md) for why one
layout is enough.

**Workload, un-frozen.** `subetha-cxc`'s `AdaptiveIpc<T>` and the
`AutoIpc` builder carry a strategy tag in the handshake header.
The sidecar watches op-stream samples and swaps the tag when the
workload shape shifts: `SharedRing` to `SharedDequeKhl` when a
work-stealing pattern emerges, SPSC to MPMC when a second
producer thread appears, single-mailbox to `SharedDequeUrd` when
per-thief mailboxes match the consumer set. The migration is
non-blocking. Readers in flight on the old strategy continue to
completion; the next readers come in on the new strategy. The
`migration` module in `subetha-core` ships the `MigrationGuard`
RAII protocol that brackets the swap.

The two un-freezings share the same substrate (`subetha-core`)
and the same control plane (`subetha-sidecar`). So a primitive
that wants both - say, an `AdaptiveIpc<u64>` whose underlying
variant switches based on observed traffic shape - gets both
axes from the same handshake / observation / policy machinery.

## What the freeze still buys you

SubEtha does not abolish the freeze. It moves the freeze to
finer granularity.

On the hot path, the strategy tag is one Relaxed load and a
branch. Roughly 300 picoseconds on Zen+. The branch target is
the concrete strategy implementation; for `T = u64` the
`TypeId`-monomorphised branch in `AdaptiveIpc::send` resolves
the specialisation at codegen time, so LTO inlines the body
directly at the call site and the indirect-dispatch cost goes
to zero. Only the inline
tag-check branch remains.

The handshake bracket - `enter_op` plus `exit_op` plus an
optional observation push - fires only on flagged ops. Those
ops are the ones the sidecar wants to know about: lock attempts
that parked, CAS attempts that lost, snapshot reads that retried.
A `Mutex::lock` on the uncontended path pays only the bracket
load + branch; the observation push is gated on the slow-path
flag, so uncontended ops do not push anything.

So the median program pays roughly the same as the frozen
equivalent. The tail program pays a small bracket cost on the ops
the sidecar needs to see, and in exchange gets to migrate
strategies under live traffic. The exchange is now per-op, not
per-program. That is the part the freeze locked away.
