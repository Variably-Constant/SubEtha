---
title: "When something does not work"
weight: 60
---

# When something does not work

## A refusal is not a failure

Before treating anything as broken, check whether it is one of the
ordinary answers. The surface says no by answering rather than by
raising:

| You got | It means |
|---|---|
| `False` from a send or push | the structure is full |
| `None` from a receive or pop | there is nothing there |
| `None` from `try_write`, `read_for`, `write_for` | somebody else holds it, or the timeout passed |
| `0` from a sweep | nothing was reclaimable |
| `None` from a measurement | it is not measured yet, which is not the same as zero |

Only a genuine fault raises. A loop that treats `None` from `recv` as
an error stops the first time the ring is briefly empty.

That last row is worth separating out. `None` from something that
reports a measurement means no value has been observed, and zero means
a value was observed and it was zero. They are different answers.

## The exceptions

| Raised | When |
|---|---|
| `ValueError` | an argument that cannot be right, caught before anything was touched |
| `OSError` | a failure underneath: the path, the permissions, or a capacity that disagrees with the file already there |
| `subetha.Lagged` | a subscriber fell far enough behind that what it asked for had been overwritten |
| `subetha.Contended` | a lease or a lane is held by somebody else |
| `subetha.WrongLane` | the key belongs to another lane, and the message names which |

```python
try:
    item = subscriber.recv()
except subetha.Lagged as lost:
    ...          # what it missed is in the exception
```

## Import problems

**`ModuleNotFoundError: No module named 'subetha'`.** The distribution
is `subetha-ipc` and the package is `subetha`, so `pip install subetha`
installs an unrelated project. Install `subetha-ipc`.

**The import works but a transport is missing.** Which transports a
wheel carries is a build-time choice. Ask rather than guessing:

```python
if "quic" in subetha.transports:
    ...
```

`subetha.OPTIONAL_BY_TRANSPORT` says what each absent transport would
have brought, which is how you tell a wheel built without one from a
name that never existed. Do not probe with `hasattr` or by catching
`AttributeError`: both answer the same for a missing feature and a
typo.

**A free-threaded interpreter got the wrong wheel.** The stable ABI
does not cover free-threading until 3.15, so that build needs its own
wheel. `subetha.free_threaded` reports which one is installed.

## A structure that will not open

The constructor creates the structure when it is absent and attaches
when it is present. `open` insists it already exists, and raises
`OSError` when it does not:

```python
r = subetha.Ring("/ipc/events", capacity=4096)        # creates or attaches
r = subetha.Ring.open("/ipc/events", capacity=4096)   # must already exist
```

The capacity is part of the layout rather than a hint, so opening with
a capacity that disagrees with the file on disk is also an `OSError`.

Paths are resolved by the operating system from the process's working
directory, so a relative path means different files to two processes
started in different places. Use absolute paths for anything shared.

## A reader that sees nothing, or sees only some of it

**Register the consumer before anything is produced.** A consumer
starts at the head, not at the beginning, so one registered after the
writer has run sees nothing that was already there. Nothing reports
this: the ring does not raise, the send does not fail, and a reader
that registered late simply gets fewer items, which looks exactly like
a reader that is slow.

```python
# wrong: everything produced before the consumer exists is invisible
ring = subetha.Ring("/ipc/events", capacity=4096)
ring.send_many(producer, items)
consumer = ring.register_consumer()      # sees none of items

# right: the reader exists before anything is sent
consumer = ring.register_consumer()
ring.send_many(producer, items)
```

That ordering is easy to get wrong across processes, where starting a
worker takes long enough for the producer to have sent a great deal
before the worker registers. A pipeline built that way loses whatever
was produced during startup, and loses more the slower the worker is to
start.

On a `BroadcastRing` the producer can wait instead of guessing:

```python
arrived = ring.wait_for_consumers(4, timeout=30)
if arrived < 4:
    raise SystemExit(f"only {arrived} of 4 workers registered")
ring.push_many(work)
```

The answer is how many registered, not a success flag, so a shortfall
is a number you can report rather than a hang. Nothing detects the loss
afterwards: `lag` reads `0` for a consumer that missed everything,
because it is caught up with the head.

`producer_position` read at the moment of registration is the other
half. It says how many items already exist that this consumer will
never see.

`lag` answers `None` rather than a number when the id names no live
consumer, which covers an id past the table and one that has been given
back.

Then check, in this order:

1. Did the reader register an end? `register_consumer()` returns an id
   and `recv` needs it.
2. Are both sides on the same file? Print the path from both.
3. Is the reader a second consumer or the same one? Two threads sharing
   one id are one reader, and items go to whichever asked first.
4. For an `OrderedReceiver`, was the ring built with `stamps`? It is
   built `Ring(path, capacity, stamps='counter')`, and the receiver
   refuses an unstamped ring rather than waiting forever. Use `drain`
   rather than `recv`, because `recv` answers `None` both while the
   window fills and when the ring is empty.

## A coroutine that will not await

Only part of the surface can be awaited soundly. A hold belongs to the
thread that took it and cannot be handed back, and a `SensReceiver`
cannot leave its thread at all. That is why `subetha.aio` takes your
work as a callable for the lock forms rather than handing you something
to await:

```python
answer = await aio.with_write_lock(lock, lambda: do_the_work())
```

See [Threads, asyncio and lifetimes](../threads-and-lifetimes/).

## Checking what you actually have

```python
import subetha
subetha.transports          # which transports this wheel carries
subetha.free_threaded       # which interpreter build
subetha.boundary_note()     # what the binding says about itself
help(subetha.Ring)          # the prose, where a method has any
```

`help()` shows the call signature and the description, the signature
derived from the same declaration the type stub is checked against.
Every method on the surface carries a description, so `help()` always
has something to say, and many of them say more than the one-line
summary [every class in full](../../../reference/subetha-py/classes/)
prints: what an answer means, and what the call does not do.

## Where to go next

- [Install the package](../install/) for what a wheel contains.
- [The reference](../../../reference/subetha-py/#the-conventions) for
  the conventions these answers follow.
