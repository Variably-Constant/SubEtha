---
title: SubEtha from Python
weight: 45
---

# SubEtha from Python

Two Python processes share the same bytes, with no copy between them
and no C shim in the middle. This walks from installing the wheel to a
message crossing between two processes, then to the shapes that make it
fast.

The full surface is in the
[`subetha-py` reference](../../reference/subetha-py/).

## Install

```bash
pip install subetha-ipc
```

The distribution is `subetha-ipc` because `subetha` on PyPI belongs to
an unrelated project. The package it installs is `subetha`, so code
says `import subetha`.

Building from a checkout instead needs a Rust toolchain and
[maturin](https://www.maturin.rs/):

```bash
cd crates/subetha-py
maturin develop --release
```

One wheel serves CPython 3.11 and every later version, because it
targets the stable ABI. A free-threaded interpreter takes a wheel of its
own; see [Threads](#threads) below.

## A counter two processes share

```python
import subetha

with subetha.Atomic("/tmp/hits", init=0) as hits:
    hits.fetch_add(1)
    print(hits.load())
```

Run it twice and the second run prints `2`. The file is the state; both
processes map it and the kernel gives them the same page. Nothing is
serialized and nothing is sent.

`Atomic` takes a memory ordering by name on every operation, defaulting
to `seq_cst`:

```python
hits.fetch_add(1, "relaxed")
```

## A message from one process to another

The sending process:

```python
import subetha

ring = subetha.Ring("/tmp/events", capacity=4096)
producer = ring.register_producer()
ring.send(producer, b"the first message")
```

The reading process:

```python
import subetha

ring = subetha.Ring.open("/tmp/events", capacity=4096)
consumer = ring.register_consumer()

while (item := ring.recv(consumer)) is not None:
    print(item.rstrip(b"\x00"))
```

`recv` answering `None` means the ring is empty, which is an ordinary
answer rather than a failure. So is `send` answering `False`, which
means the ring is full.

A payload larger than a slot goes through `send_frame` and comes back
through `recv_frame`, which reassemble it.

## Hold a lock across processes

```python
lock = subetha.RWLock("/tmp/lock")

with lock.write():
    ...          # nothing else holds the write side while this runs
```

The hold is a context manager, not a token to remember to return. It
gives the lock back when the block ends, including on the way out of an
exception, and again if it is dropped without a block at all.

`Semaphore` works the same way with `acquire`, and `OwnerLease` with
`hold`.

## The two shapes that cost less

A call from Python into this library costs about 30 nanoseconds. That
is nineteen times cheaper than the same call through a C shim driven by
`ctypes`, and it is still 30 nanoseconds. Two shapes avoid paying it
per item.

**Carry many operations over one crossing.** Every family has a batched
form:

```python
ring.send_many(producer, [b"one", b"two", b"three"])
items = ring.recv_many(consumer, max_items=256)
```

Batched a thousand at a time, an operation costs about 1.3 nanoseconds
instead of 30.

**Cross once and then read with no call at all.** A `Region`'s buffer
is a view over the mapped file:

```python
region = subetha.Region("/tmp/frames", capacity=1024, slot_size=64)
view = memoryview(region)      # the mapping itself, not a copy

import numpy
frame = numpy.frombuffer(view, dtype=numpy.uint8)
```

Read whole, that costs about 0.3 nanoseconds a byte. Walking the same
view one element at a time from Python costs 33 nanoseconds an element,
a hundred times more, and none of that is the mapping: it is what
indexing costs in the interpreter. A buffer pays off when something
consumes it whole.

`bench/call_shapes.py` in the crate measures all of this on your own
machine rather than asking you to believe these numbers.

## Threads

The module does not need the interpreter lock, so on a free-threaded
interpreter the lock stays off when it is imported and several threads
really do run inside these calls at once.

```python
import subetha
print(subetha.free_threaded)
```

A wheel for such an interpreter is a separate wheel, because the stable
ABI does not cover free-threading until 3.15:

```bash
maturin build --release --no-default-features
```

## Reaching another machine

Everything above shares memory on one host. To reach another, the
Sens-O-Matic link sends more than the items so the reader rebuilds what
the network lost without asking again:

```python
reader = subetha.SensReceiver(("0.0.0.0", 9000), max_item_size=1024)
writer = subetha.SensSender(("0.0.0.0", 0), ("10.0.0.5", 9000), max_item_size=1024)
writer.send_many([b"one", b"two"])

for item in reader.poll():
    print(item)
```

`poll` answers whatever the link could rebuild this time round, which
may be nothing. A reader calls it in a loop rather than expecting one
item per call.

There are also bridges that carry a whole ring to another host over TCP
or QUIC. They are off by default because each brings a network stack
with it, so ask the wheel what it has:

```python
if "tcp" in subetha.transports:
    ...
```

## Where to go next

- [The `subetha-py` reference](../../reference/subetha-py/) for every
  class and what it costs.
- [Cross-process round trip](../../tutorial/cross-process-roundtrip/)
  for the same exercise from Rust.
- [SubEtha from C and C++](../../tutorial/c-and-cpp/) for the C ABI,
  which is a different binding to the same primitives.
