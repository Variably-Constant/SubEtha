# subetha

Shared memory between processes: rings, channels and shared state, bound
to Rust directly.

```python
import subetha

with subetha.Atomic("/tmp/counter", init=0) as counter:
    counter.fetch_add(1)
    print(counter.load())

region = subetha.Region("/tmp/frames", capacity=1024, slot_size=64)
view = memoryview(region)          # a view over the mapping, no copy

# A ring another process is reading, filled in one crossing rather than
# one per item.
ring = subetha.Ring("/tmp/events", capacity=4096)
producer = ring.register_producer()
ring.send_many(producer, [b"one", b"two", b"three"])

# A lock held across processes, given back when the block ends.
lock = subetha.RWLock("/tmp/lock")
with lock.write():
    ...
```

## What this package is for

A process here shares memory with another process, which may be written
in Rust, C, or Python. The mapping is the same bytes in every one of
them.

## What it costs, and what follows from that

Measured on a Ryzen 9 7900X, taking the same atomic load every way it can
be reached:

| reached by | ns per operation |
|---|---|
| Rust, through the C ABI | 7.1 |
| **Python, this binding** | **30.1** |
| Python, this binding, with an argument | 36.8 |
| Python, through a C shim with ctypes | 584.4 |
| **Python, this binding, batched a thousand at a time** | **1.3** |

The two Python rows to read against each other are this binding at 30.1
and the C shim at 584.4: the same operation, nineteen times cheaper, and
that difference is what binding the Rust directly buys. An empty Python
loop costs 7.7 ns per iteration on the same machine, so about 22 ns of
the 30 is the call itself.

Two shapes cross the boundary less often:

- **A batch call**, which carries many operations across one boundary
  crossing. `fetch_add_many` is the smallest example, and the table above
  is what it does to the per-operation figure.
- **A buffer**, which crosses the boundary once and is then read with no
  further call at all. `memoryview` of a `Region` is a view over the
  mapped file itself: read whole, it comes out at 0.3 ns per byte.

The buffer is worth one more measurement, because it is easy to spend it
without meaning to. Walking that same view one element at a time from
Python costs 33 ns an element, a hundred times the bulk figure, and none
of that is the mapping: it is what indexing costs in the interpreter.
A buffer pays off when something consumes it whole, numpy included, and
not when a Python loop walks it.

`bench/call_shapes.py` measures all of this on your own machine rather
than asking you to believe these numbers.

## What is here

**Rings and channels.** `Ring`, the adaptive ring, with producer and
consumer registration, framing for payloads larger than a slot, and a
shape that changes under the traffic. `SpscRing`, one writer and one
reader. `BroadcastRing`, one writer and many readers, each seeing
everything. `PubSub` with `Subscriber`, which keeps the last N and tells
a slow reader what it lost. The `mpsc_pool`, `mpmc_grid` and
`lamport_pair` constructors, which hand out the ends together because
the shape is what makes them correct.

**Rings that change themselves.** `CapacityRing` resizes without losing
what is already in it. `LocaleRing` moves between process-private
memory, a mapped file and named memory without its senders and readers
reconnecting.

**Order.** A `Ring` built with `stamps` marks each item with the order
its sender made it in. `OrderedReceiver` reads those marks and delivers
by them, choosing its own strategy from how the ring is built.
`ReorderWindow` is the same window over items from anywhere else.

**Shared state.** `Atomic`, `Cell`, `Vec`, `Slab`, `HashMap`,
`BTreeMap`, `LinkedList`, `Arena`, `Region` and `FrameRegion`.

**State with a history.** `VersionChain` is one value's versions, read
as of any of them. `VersionedSlab` is numbered slots each keeping their
recent history, read through a pin that fixes one epoch. `VersionedMap`
is an ordered index scanned the same way, and `LanedMap` is that map
split across several trees so several writers work at once.

**Coordination.** `RWLock` and `Semaphore`, whose holds are context
managers rather than tokens. `Condvar`, whose predicate really is called
from inside the wait. `LazyValue`, computed once across processes.
`OwnerLease`, which gives one process a small shared value and hands it
to another when that one dies. `Heartbeat`, `EpochBarrier`,
`LeaderElection`, `HolderTable`, `FenceClock`, `SharedArc`, and the
`NotifierSet`.

**Probabilistic.** `BloomFilter`, `CountMinSketch`, `HyperLogLog`,
`Histogram`, `RateLimiter`, `BitVec`, `BlockedBloomFilter` whose bits
for one item share a cache line, and `Reservoir`, a bounded unbiased
sample of a stream of any length.

**Specialist.** `HandleTable`, reached by handles that do not follow a
reused slot to its new occupant. `TimePointTile`, sixteen values each
visible only to a reader late enough to see it. `Tower`, values reached
by a path that checks itself at every level. `Graph`, `TopologyMap`,
which reads the shape of the traffic off who sends to whom, and
`Universal`, a set that changes how it stores itself as it grows.
`QosPolicy` is what a stream needs written down, and its snapshot says
where the bytes should live.

Anything holding a resource is a context manager and gives it back when
its block ends, including on the way out of an exception. Everything
raises rather than returning a code, except where a refusal is an
ordinary answer: a push that does not fit returns `False`, a pop with
nothing to take returns `None`.

## Reaching another machine

`SensSender` and `SensReceiver` are the two ends of a link that keeps
working as the network gets worse. The link sends more than the items so
a reader can rebuild what was lost without asking again, and it changes
between two ways of working that extra out as the measured loss moves,
without either end reconnecting.

```python
reader = subetha.SensReceiver(("0.0.0.0", 9000), max_item_size=1024)
writer = subetha.SensSender(("0.0.0.0", 0), ("10.0.0.5", 9000), max_item_size=1024)
writer.send_many([b"one", b"two"])

# Items do not arrive one at a time: poll answers whatever the link
# could rebuild this time round, which may be nothing.
for item in reader.poll():
    handle(item)
```

There are also bridges that carry a whole ring to another host, over TCP
or over QUIC. They are **off by default**, because each brings a network
stack with it and a process sharing memory with another on the same host
needs none of it:

```bash
maturin build --release --features tcp-bridge,quic-bridge
```

`subetha.transports` says which a wheel was built with, so a missing
class can be told from a name that never existed:

```python
if "tcp" in subetha.transports:
    server = subetha.TcpBridgeServer(incoming_ring, ("0.0.0.0", 9100))
```

**Sensing.** `LossKind`, `LossBursts`, `Timing`, `RoundTripShape`,
`Periodicity`, `Capacity`, `Forecast` and `PathChanges`: fed
measurements, they answer what they worked out. They hold no shared
memory and touch no network, so they are useful whether or not a
SubEtha link produced the numbers. Each answers `None` rather than a
number until it has seen enough, which is a different answer from zero.

**Values that ride beside a pointer.** `TinyBloom` is a whole bloom
filter in one machine word whose state crosses as a single number.
`Clock` orders two events that share a wall-clock reading. `CausalClock`
answers before, after, equal, or concurrent, and concurrent is the
answer a timestamp can never give.

## Asyncio

`subetha.aio` lets a coroutine wait without blocking its loop:

```python
from subetha import aio

item = await aio.recv(channel, timeout=5)
answer = await aio.with_write_lock(lock, lambda: do_the_work())
```

Only some of the surface can be awaited soundly, and the module says
which. A hold belongs to the thread that took it, so `with_permit` and
the two lock forms run the caller's work on that thread rather than
handing the hold back.

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

`subetha.free_threaded` says which build is installed.

A wheel for a free-threaded interpreter is a separate wheel, because
the stable ABI does not cover free-threading until 3.15:

```bash
maturin build --release --no-default-features
```

## Where the types are

The package ships `py.typed` and a stub covering every class, so an
editor and a type checker see the surface. `tests/test_surface.py` holds
the stub against the module in both directions, because nothing else
would notice a class added to one and not the other.
