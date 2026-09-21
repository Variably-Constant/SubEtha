---
title: "Threads, asyncio and lifetimes"
weight: 40
---

# Threads, asyncio and lifetimes

Three questions come up once a program does more than one thing at a
time: whether threads really run at once, what can be awaited, and when
the mapping underneath goes away.

## Threads really do run at once

The module declares that it does not need the interpreter lock. On a
free-threaded interpreter the lock stays off when `subetha` is
imported, and several threads genuinely run inside these calls
simultaneously rather than taking turns.

```python
import subetha
subetha.free_threaded      # which build is installed
```

That declaration is backed by `tests/test_threading.py`, which runs
every call that releases the interpreter under eight threads: the
counters, both kinds of lock hold, the semaphore's permits, a ring
several senders share, a pinned scan running beside writers, and a
buffer view held across other threads' work. It passes on CPython 3.14
free-threaded with the lock reported off.

On a standard interpreter the calls still release the lock, so other
Python threads make progress while one is inside a blocking wait. What
you do not get is two threads inside the extension at once.

A free-threaded interpreter needs its own wheel, because the stable ABI
does not cover free-threading until 3.15. See
[Install the package](../install/).

## Sharing structures between processes

The cheapest way to use several processes is not to share an object at
all. Give each process the path and let it open the structure itself:

```python
# in every process
ring = subetha.Ring.open("/ipc/events", capacity=4096)
```

That is what the library is for. A `multiprocessing` process cannot be
handed a live object anyway: what would arrive is a pickled copy with
no mapping behind it.

## Anything holding a resource is a context manager

This is the convention that replaces remembering to release:

```python
with lock.write():
    ...                     # nothing else holds the write side here
```

The hold gives the resource back when the block ends, including on the
way out of an exception, and again if it is dropped without a block at
all. The same is true of a permit, a pin, a lane claim and the
structures themselves.

```python
with subetha.Atomic("/ipc/hits") as hits:
    hits.fetch_add(1)
```

`try`/`finally` is not needed around these and adds nothing.

## What can be awaited, and what cannot

`subetha.aio` is a small pure-Python module. The Rust side has a
reactor, an executor and a task pool, and none of it is bound, because
none of it can be: every entry point there takes or returns a Rust
future. What asyncio needs is not Rust's executor but a way to wait
without blocking its loop.

```python
from subetha import aio

item = await aio.recv(channel, timeout=5)
sent = await aio.send(channel, b"payload", timeout=5)
answer = await aio.with_write_lock(lock, lambda: do_the_work())
arrived = await aio.wait(notifier, timeout=5)
```

The whole module is seven public coroutines: `recv`, `send`, `wait`,
`with_permit`, `with_read_lock`, `with_write_lock` and `poll`, plus two
private helpers they share.

Only part of the surface can be awaited soundly, and the module is
explicit about which:

| | |
|---|---|
| a queue | can be awaited; what comes back is `bytes` |
| a notifier | can be awaited; what it costs differs by platform |
| a hold | cannot be handed back, because it belongs to the thread that took it |
| a `SensReceiver` | cannot leave its thread at all |

That is why `with_permit`, `with_read_lock` and `with_write_lock` take
your work as a callable and run it on the thread that holds the lock,
rather than handing you a hold to await against. And why `aio.poll`
asks on this thread and yields between tries instead of moving the
receiver.

### Waiting on a notifier

`aio.wait` works everywhere; what it costs is what differs. On Unix the
notifier's descriptor goes straight to the event loop, so the wait
occupies nothing at all. Windows hands out an event handle that nothing
in asyncio's public surface can watch, because the proactor loop is the
default there and does not implement `add_reader`, and the Windows
selector loop selects only on sockets. So on Windows the wait runs on a
worker thread, and one without a timeout holds that thread until a
signal arrives.

It does not consume the signal, which is the blocking `Notifier.wait`'s
behavior too. Drain it yourself, or the next wait returns at once on the
signal you already saw:

```python
while await aio.wait(notifier, timeout=5):
    notifier.drain()
    handle_whatever_arrived()
```

Awaiting does not make a call cheaper. It costs what the call costs
plus the loop's bookkeeping; see [Make it fast](../make-it-fast/).

## When the mapping goes away

A structure holds its file mapped for as long as the object lives. A
`with` block ends it at a point you chose; otherwise it ends when the
object is collected.

Borrowed things keep their owner alive. A `SlabPin` and an
`OrderedReceiver` each hold the object they came from, so collecting
the owner first does not pull the mapping out from under them:

```python
slab = subetha.VersionedSlab(...)
pin = slab.pin()
del slab                 # safe: the pin still holds the mapping
pin.get(0)               # still works
pin.release()            # the mapping goes now
```

Both have a test that collects the owning object and then uses the
borrower, because the obvious way to write this in Rust is wrong: Rust
drops struct fields in declaration order, so a pin written the other
way round released the object it borrowed from before the guard that
still had to use it.

## Where to go next

- [Make it fast](../make-it-fast/) for the per-call costs that decide
  whether threading is the right answer at all.
- [When something does not work](../troubleshoot/) if an object is
  behaving oddly across a thread boundary.
- [The reference](../../../reference/subetha-py/#threads) for the same
  contract stated beside the surface.
