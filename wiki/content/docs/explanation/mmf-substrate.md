---
title: MMF Substrate
weight: 50
---

# The MMF substrate

The `subetha-cxc` crate has roughly forty primitives. Every one of
them sits on a single mechanism: a file-mapped memory region.
Not "primitives that happen to live in shared memory" - the MMF
*is* the substrate, and every primitive is one layout on top of
it. This page is why one mechanism serves so many shapes.

## Three properties from one mechanism

A memory-mapped file gives the caller three things at once:

**Cross-thread.** Two threads in one process both map the same
file. They get two virtual mappings pointing at the same
physical pages. Lock-free CAS on bytes inside that region works
between the threads, exactly like a heap-allocated structure.

**Cross-process.** Two *processes* map the same file. The OS
page cache aliases them onto identical physical pages. The same
CAS that worked between threads in one process now works between
threads in different processes. The kernel does no copying; the
two virtual address spaces just point at the same RAM.

**Disk persistence.** The file is a real file on disk. Modify
the mapped region; eventually the dirty pages get written back
to disk by the OS, by an explicit `flush()` call, or by both.
Restart the process and reopen the file - the bytes are right
where the writer left them, the writer's data structures are
already initialised in place, and the reader can pick up.

The same byte layout gives all three. There is no "in-memory
mode" vs "shared memory mode" vs "disk mode". The MMF *is* the
layout. The deployment mode is which set of mappers are alive
right now.

## Why the same bytes work in all three modes

A shared-memory data structure has to satisfy a stricter
contract than a heap-allocated one. Two constraints in
particular:

**No absolute pointers.** A virtual address in one process's
mapping is not valid in another process's mapping; the kernel
maps the file at whatever virtual base it pleases. So any
pointer inside the mapped region has to be a file-relative
offset, not a raw pointer. `subetha_cxc::OffsetPtr` and
`TaggedOffsetPtr` are the building blocks; the higher-level
primitives use them to express links between slots inside the
file.

**Determinism across processes.** Anything that depends on
process-local state breaks the moment a second process gets
involved. The classic example is `std::hash::RandomState`, which
re-seeds per process for DoS resistance - the same key hashes to
different slots in two processes, and the shared map becomes
unusable. `SharedHashMap` uses FNV-1a (`fnv1a_64`, exported at
the crate root) because FNV-1a has a fixed seed and gives the
same hash for the same key in every process that links the
crate. The DoS-resistance argument does not apply here because
shared-memory primitives are not exposed to untrusted input by
construction; the threat model is "two cooperating processes",
not "one process answering arbitrary requests".

These two constraints together are what make "one layout, three
modes" hold. A primitive that satisfies them works identically
whether the second mapper is a sibling thread, a sibling
process, or the same process reopening the file after a crash.

## Why MMF instead of pipes or sockets

Bare OS IPC primitives - named pipes, Unix-domain sockets,
anonymous shared memory - solve a subset of the problem at much
higher cost. Two ways the cost adds up.

**Per-op syscall.** Every `WriteFile` (Windows) or `write`
(POSIX) is a syscall. The user-to-kernel transition on modern
x86 costs roughly 200 to 500 ns even when the kernel side does
nothing else. A pipe `write` of a small message costs 10 to 20
microseconds end to end including the kernel's pipe-buffer
management. A `SharedRing` slot publish - which is the same
shape of operation, "one producer enqueues a small message" -
costs 50 to 200 nanoseconds. That is a 100x gap, and it vanishes
the moment a `Mutex` enters the picture on the userspace side,
which is why most pipe-based code grows a mutex.

**Hidden metadata.** A bare pipe carries a byte stream and
nothing else. The application protocol on top has to encode
which logical stream each message belongs to, what priority it
has, whether the peer is alive, when the data needs to hit disk.
Every primitive in `subetha-cxc` names one of those as a first-class
field in the file layout: `SharedBroadcastRing` has explicit
per-consumer cursors, `SharedRateLimiter` has a refill schedule
in the header, `HeartbeatTable` has a per-process liveness slot
with TTL. The application reads the field; it does not have to
serialise-deserialise the field from a byte stream.

The architectural shape is the same as QUIC over TCP. QUIC
gives applications transport-layer features (multiplexing, head-
of-line elimination, connection migration) by moving the
transport out of the kernel and into userspace. SubEtha gives
applications IPC-layer features (priority, liveness, persistence)
by moving the substrate out of the OS pipe and into a userspace
MMF layout.

## Why power-of-two capacity

Most primitives in `subetha-cxc` that have a capacity argument
require it to be a power of two. `SharedDeque::create(path, 1024)`
works; `SharedDeque::create(path, 1000)` returns
`DequeError::InvalidCapacity`. (`SharedHashMap` is the exception:
it probes with `hash % capacity` and accepts any `capacity >= 2`.)
Three reasons compose.

**Slot index is a mask.** A ring slot is `seq & (capacity - 1)` -
a single AND instruction - instead of `seq % capacity`, a divide.
On the hot path, the difference between one cycle and twenty
cycles per op matters.

**Single-instruction wrap.** A ring of capacity `N` advances the
head as `(head + 1) & (N - 1)` instead of `(head + 1) % N`.
Same argument, same instruction-count gap, applied to every push
and pop.

**Layout alignment.** The header sits at offset 0; the slot
array sits at the next 64-byte-aligned offset; each slot is
sized to a power of two so successive slots land on cache-line
boundaries. A non-power-of-two capacity makes the slot-array's
total size non-trivial to compute and pushes some slots across
cache-line boundaries in inconsistent ways. The MMF's
deterministic-across-processes contract works better when
"the K-th slot is at byte offset header_size + K*slot_size" is
a single multiplication.

## What the substrate does not do

The MMF substrate is the bytes plus the file mapping. It is not
a transaction layer. It is not a replication layer. It is not
a serialisation layer. It does not negotiate version
compatibility across processes that link different versions of
the crate.

Per-primitive headers carry a magic constant
(`MAP_MAGIC`, `RING_MAGIC`, etc.) for type-tag verification, but
two processes linking different layout versions of the same
primitive will produce undefined behaviour. Cross-version
compatibility is the application's responsibility - either by
pinning the crate version across deployments, or by versioning
the file format and migrating explicitly.

The substrate also does not retry torn writes from a process
crash mid-operation. CAS-based primitives are atomic at the
slot level; a crash between two CAS operations leaves the file
in a state that may or may not be valid for the higher-level
operation that was in progress. The primitives that need
crash-consistent semantics (e.g., `OwnerLease`, `HeartbeatTable`)
build it explicitly on top of the substrate using epoch counters
and grace periods.

## See also

- [Architecture](architecture.md) - where the MMF family sits
  in the four-crate stack.
- [Frozen handshakes](frozen-handshake.md) - the topology axis
  of un-freezing that the MMF substrate is the implementation
  of.
- [`subetha-cxc` reference](../reference/subetha-cxc/) - the
  full primitive list.
- [Cross-process round-trip tutorial](../tutorial/cross-process-roundtrip.md) -
  the substrate-level demonstration in 30 lines.
