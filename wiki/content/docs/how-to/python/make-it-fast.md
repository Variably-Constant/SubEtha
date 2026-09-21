---
title: "Make it fast"
weight: 30
---

# Make it fast

The library underneath costs about 7 ns for an atomic load. Reaching it
from Python costs 30 ns, and from Python through a C shim 584. This
page is about the gap between those, and about one shape that looks
like the answer and usually is not.

## What the shapes cost

Measured on a Ryzen 9 7900X, taking the same atomic load every way it
can be reached:

| reached by | ns per operation |
|---|---|
| Rust, through the C ABI | 7.1 |
| **Python, this binding** | **30.1** |
| Python, this binding, with an argument | 36.8 |
| Python, through a C shim with `ctypes` | 584.4 |
| **Python, this binding, batched a thousand at a time** | **1.3** |

An empty Python loop costs 7.7 ns an iteration on the same machine, so
about 22 ns of the 30 is the call itself. That is the thing to spend
less of.

The 584.4 row is what this binding exists to avoid: the same operation
through a hand-written C shim, nineteen times dearer, because every
crossing marshals through `ctypes` rather than being compiled against
the interpreter.

## Batch the call

Every family has a many-at-once form: `fetch_add_many`, `send_many`,
`recv_many`, `insert_many`, `get_many`, `drain`.

```python
# 30 ns each
for item in items:
    ring.send(producer, item)

# about 1.3 ns each at a thousand a call
ring.send_many(producer, items)
```

One call carrying a thousand operations amortizes the 22 ns of call
overhead across all of them. This is the shape to reach for first,
because it changes nothing about how the data is laid out.

## A buffer, and the trap in it

A region can be viewed rather than copied:

```python
view = memoryview(region)
```

That view is the mapped file itself. Read whole it is **0.3 ns a byte**,
which is as fast as this gets and is the right shape when something
consumes the whole thing, numpy included.

**Walking it one element at a time from Python costs 33 ns an element**,
a hundred times the bulk figure, and none of that is the mapping: it is
Python's own per-element cost, the same cost a list would charge.

```python
# fast: something else consumes the whole view
array = numpy.frombuffer(view, dtype=numpy.uint64)

# not fast: a Python loop walks it, 33 ns an element
total = sum(view[i] for i in range(len(view)))
```

So a buffer pays off when it is handed to something that reads it
whole, and does not pay off merely because it avoided a copy. A
`memoryview` reached for reflexively can be slower than `recv_many`.

## Measure your own code

The crate ships the harness that produced the table:

```bash
python bench/call_shapes.py
```

It reproduces every row on your own machine. For your own code,
`timeit` over a loop large enough to swamp the loop's own 7.7 ns is
enough to tell the shapes apart. Comparing two shapes needs the same
care as any other measurement: run them in both orders, because
position in a run is worth several percent on its own.

## What not to do

**Do not reach for asyncio to make it faster.** `subetha.aio` exists so
a coroutine can wait without blocking its loop, not to reduce the cost
of a call. An awaited operation costs what the call costs plus the
loop's own bookkeeping. See
[Threads, asyncio and lifetimes](../threads-and-lifetimes/).

**Do not tune the Rust side.** At 7.1 ns against a 30 ns call, the
library is under a quarter of what one operation costs you, and under
one percent once a Python loop is in the picture. Crossing the boundary
fewer times is the only lever with an order of magnitude in it.

## Where to go next

- [Threads, asyncio and lifetimes](../threads-and-lifetimes/) for
  running several threads inside these calls at once, which is a
  different lever from the two above.
- [The reference](../../../reference/subetha-py/#what-a-call-costs-and-what-follows)
  for the same table beside the surface it describes.
- [What the values look like](../../../reference/subetha-py/values/)
  for what these calls actually hand back.
