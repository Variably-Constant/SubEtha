---
title: SubEtha Python
weight: 57
---

# The Python binding (`subetha-py`)

`subetha-py` gives Python the memory-mapped primitives directly, with
no C shim between the interpreter and the Rust. It ships as a wheel
rather than to crates.io, and the package it installs is `subetha`.

The wheel is not on PyPI yet: the name `subetha` there belongs to an
unrelated project, so the distribution name is still to be settled.
Build from a checkout in the meantime.

This page is the reference. To install it and send a first message,
start at [SubEtha from Python](../../tutorial/python/).

## What a call costs, and what follows

Measured on a Ryzen 9 7900X, taking the same atomic load every way it
can be reached:

| reached by | ns per operation |
|---|---|
| Rust, through the C ABI | 7.1 |
| **Python, this binding** | **30.1** |
| Python, this binding, with an argument | 36.8 |
| Python, through a C shim with `ctypes` | 584.4 |
| **Python, this binding, batched a thousand at a time** | **1.3** |

The two Python rows to read against each other are 30.1 and 584.4: the
same operation, nineteen times cheaper. An empty Python loop costs
7.7 ns an iteration on the same machine, so about 22 ns of the 30 is
the call itself.

Two shapes cross the boundary less often, and the surface is built
around them:

- **A batch call** carries many operations over one crossing.
  `fetch_add_many`, `send_many`, `recv_many`, `insert_many`,
  `get_many`, `drain` and their kin are all this shape.
- **A buffer** crosses once and is then read with no call at all.
  `memoryview(region)` is a view over the mapped file: read whole it is
  0.3 ns a byte. Walking that same view one element at a time from
  Python costs 33 ns an element, a hundred times the bulk figure, and
  none of that is the mapping. A buffer pays off when something
  consumes it whole, numpy included, and not when a Python loop walks
  it.

`bench/call_shapes.py` in the crate reproduces every row above on the
reader's own machine.

## The conventions

- **A refusal is an answer, not a fault.** A push that does not fit
  returns `False`; a pop with nothing to take returns `None`; a sweep
  that freed nothing returns `0`; a sample that kept nothing returns
  `None`. Only a genuine fault raises.
- **Anything holding a resource is a context manager**, and gives the
  resource back when its block ends, including on the way out of an
  exception, and again if it is dropped without a block.
- **Exceptions in place of error codes.** `ValueError` for an argument
  that cannot be right, `OSError` for a failure underneath, plus two of
  the package's own: `Lagged` when a subscriber fell far enough behind
  that what it asked for was overwritten, and `Contended` when a lease
  or a lane is held by somebody else.
- **A value that is not measured yet is `None`**, which is a different
  answer from zero.
- **The package ships `py.typed` and a full stub**, and
  `tests/test_surface.py` holds the stub against the module in both
  directions, so a class in one and not the other fails the suite.

## Rings and channels

| Class | What it is |
|---|---|
| `Ring` | The adaptive ring: producers and consumers register for an id, payloads larger than a slot are framed, and the shape changes under the traffic. Built with `stamps` it marks each item with the order its sender made it in. |
| `SpscRing` | One writer, one reader. |
| `BroadcastRing` | One writer, many readers, each seeing everything. |
| `PubSub`, `Subscriber` | Keeps the last N and tells a slow reader what it lost, by raising `Lagged`. |
| `MpscProducer`, `MpscConsumer` | Many writers, one reader. Built by `mpsc_pool` / `mpsc_pool_open`. |
| `MpmcProducer`, `MpmcConsumer` | Many of each. Built by `mpmc_grid` / `mpmc_grid_open`. |
| `LamportProducer`, `LamportConsumer` | The Lamport pair. Built by `lamport_pair` / `lamport_pair_open`. |

The constructors hand out both ends together because the shape is what
makes them correct.

## Rings that change themselves

| Class | What it is |
|---|---|
| `CapacityRing` | Resizes without losing what is in flight: a reader keeps reading the superseded backing until it has drained. Reports the prewarmed backing, the morphs that took it, and the reads served from a superseded one. |
| `LocaleRing` | Moves between process-private memory, a mapped file and named memory without its senders and readers reconnecting, carrying what it holds across the move. |

Both accept `stamped`, and report `ordering_mode` as one of
`unordered`, `merge_by_stamp` or `merge_strict`.

## Order

| Class | What it is |
|---|---|
| `OrderedReceiver` | Delivers a ring's items in the order their senders made them. Built by `Ring.ordered_receiver`, which refuses a ring with no stamps rather than returning nothing forever. It picks its own strategy and `strategy` says which. |
| `ReorderWindow` | The same window over items from anywhere else: push with a stamp, take the smallest first, and the window widens itself when something arrives further out of order than it covered. |

`OrderedReceiver.recv` answers `None` for two different reasons, the
window still filling and the ring being empty, so `drain` is the shape
to reach for: it takes the ring and the held-back tail in one crossing.

## Shared state

`Atomic` (load, store, add, subtract, the bitwise operations, swap and
compare-and-exchange, each taking a named memory ordering), `Cell`,
`Vec`, `Slab`, `HashMap`, `BTreeMap`, `LinkedList`, `Deque`, `Stack`,
`Arena`, `Region` and `FrameRegion`.

`Region` is the one that carries a buffer: `memoryview(region)` is the
mapping itself. It is the only family that does, because the others
guard each slot with a seqlock and a raw view would bypass the retry
and hand back torn elements.

## State with a history

| Class | What it is |
|---|---|
| `VersionChain` | One value's versions. A reader holding a version number gets the value as it stood then. |
| `VersionedSlab`, `SlabPin` | Numbered slots each keeping their recent history, read through a pin that fixes one epoch for the whole slab. `history` reports each version with the epochs it was current between. |
| `VersionedMap`, `MapPin` | An ordered index from one unsigned integer to another, scanned through a pin that sees one unchanging view while writers carry on. |
| `LanedMap`, `LaneClaim`, `LanedPin` | That map split across several trees so several writers work at once. A writer claims a lane; reading takes none, and a scan merges every lane in key order. |
| `Epochs` | The epoch table the above share. |

Two names here mean what the Rust says rather than what they sound
like. `void_epoch` is not reclamation: it undoes the writes stamped at
exactly one epoch and makes what they superseded current again, for a
writer that died partway through. `sweep` and `sweep_slot` are the ones
that reclaim, and a live pin holds the horizon still, so a sweep during
a scan frees only what was already unreachable when the scan began.

A key belongs to one lane of a `LanedMap` for its whole life, because a
lane is a separate tree. Writing it through another raises `WrongLane`
naming the right one, rather than reporting a row gone that no reader
has stopped seeing.

## Coordination

| Class | What it is |
|---|---|
| `RWLock`, `Hold` | Read and write holds, as context managers rather than tokens. |
| `Semaphore`, `PermitHold` | A counting semaphore across processes. |
| `Condvar` | Whose predicate really is called from inside the wait. |
| `LazyValue` | Computed once across every process. |
| `OwnerLease`, `LeaseHold` | One process owns a small value and another takes it over when that one dies. |
| `Heartbeat`, `EpochBarrier`, `LeaderElection`, `HolderTable`, `FenceClock`, `SharedArc`, `Notifier`, `NotifierSet` | The rest of the coordination surface. |

A lease changes hands two ways and a caller has to know them apart. A
process whose id is lower than the owner's takes it on the spot,
beaten or not, which is what settles who leads when several processes
start together. A process whose id is higher waits out the grace
period, counted in epochs the holders step themselves with
`tick_epoch`; nothing steps it on its own.

## Probabilistic

`BloomFilter`, `BlockedBloomFilter`, `CountMinSketch`, `HyperLogLog`,
`Histogram`, `RateLimiter`, `BitVec` and `Reservoir`.

`BlockedBloomFilter` puts every bit for one item in a single cache line,
so a lookup is one miss rather than several, and its `suggest` works the
size and hash count out from the item count and the rate of wrong yeses
that can be lived with. `Reservoir` keeps a bounded unbiased sample of a
stream of any length; a refused value there is how the sample stays
unbiased.

`BitVec.toggle` answers the new value, while `set` and `clear` answer
the previous one.

## Specialist

| Class | What it is |
|---|---|
| `HandleTable` | Values reached by a handle carrying both the place and how often it has been reused, so a handle to a removed value does not follow the place to its new occupant. |
| `TimePointTile` | Sixteen values each stamped with the version it was written at, compared against a reader's version in a handful of vector instructions. |
| `Tower` | Values reached by a path down through levels, where each level checks that it still points at the next number in the path and refuses at the first that does not. |
| `Graph` | Nodes and the directed edges between them, in mapped files. |
| `TopologyMap` | Counts who sends to whom and reads the shape off those counts: `point_to_point`, `broadcast_tree` or `all_to_all_mesh`. Reading a recommendation does not publish it. |
| `Universal` | A set stored as a list while that is quicker to walk and moved to a map when it is not. |
| `QosPolicy`, `QosSnapshot` | What a stream needs written down, and what follows: where the bytes should live, and whether the ordering must change. |

`QosPolicy.ordering` is set by the caller and never inferred from the
traffic, because whether a reader needs one sender's order or every
sender's order is something only the application knows.

## Reaching another machine

`SensSender` and `SensReceiver` are the two ends of a link that keeps
working as the network gets worse. The link sends more than the items,
so a reader rebuilds what was lost without asking again, and it changes
between the sliding code and the block code as the measured loss moves,
without either end reconnecting.

```python
reader = subetha.SensReceiver(("0.0.0.0", 9000), max_item_size=1024)
writer = subetha.SensSender(("0.0.0.0", 0), ("10.0.0.5", 9000), max_item_size=1024)
writer.send_many([b"one", b"two"])

# Items do not arrive one at a time. poll answers whatever the link
# could rebuild this time round, which may be nothing.
for item in reader.poll():
    handle(item)
```

Two things a reader has to know. Under the block code items are grouped
in eights and none of a group is delivered until enough of it has
arrived, so a run shorter than a group waits. And `SensSender.loss` is
`None` until the reader has reported anything, which is not the same as
zero.

## The bridges, and what a wheel has

`TcpBridgeClient` / `TcpBridgeServer` and `QuicBridgeClient` /
`QuicBridgeServer` carry a whole ring to another host. Both are off by
default, because each brings a network stack with it and a process
sharing memory on one host needs none of it:

```bash
maturin build --release --features tcp-bridge,quic-bridge
```

A wheel therefore exports what it was built with, so a name a caller
cannot find can be told from a feature left out:

```python
if "tcp" in subetha.transports:
    server = subetha.TcpBridgeServer(incoming_ring, ("0.0.0.0", 9100))
```

`subetha.OPTIONAL_BY_TRANSPORT` names the classes each optional
transport brings.

A QUIC certificate is made as two pieces of bytes by
`generate_self_signed_cert`: the reading end holds both and proves
itself with them, the sending end holds the certificate alone and
checks the reading end against it. Carrying it between hosts is the
caller's to arrange.

## Threads

The module declares that it does not need the interpreter lock, so on a
free-threaded interpreter the lock stays off when it is imported and
several threads really do run inside these calls at once.

That declaration is backed by `tests/test_threading.py`, which runs
every call that releases the interpreter under eight threads: the
counters, both kinds of lock hold, the semaphore's permits, a ring
several senders share, a pinned scan running beside writers, and a
buffer view held across other threads' work. It passes on CPython
3.14 free-threaded with the lock reported off.

`subetha.free_threaded` says which build is installed. A wheel for a
free-threaded interpreter is a separate wheel, because the stable ABI
does not cover free-threading until 3.15:

```bash
maturin build --release --no-default-features
```

## Two things the binding does that the Rust does not

**Every value is boxed.** Python's object allocator aligns to sixteen
bytes, `HandshakeHeader` is cache-line aligned, and many of the
primitives embed one. A class holding such a value inline compiles,
imports, and then faults inside its constructor on the first aligned
store. A compile-time assertion per class enforces the boxing.

**A borrowed guard is declared before the handle that keeps its target
alive.** Rust drops struct fields in declaration order, so a pin or a
receiver written the other way round released the object it borrowed
from before the guard that still had to use it. Both `SlabPin` and
`OrderedReceiver` have a test that collects the owning object and then
uses the borrower.
