---
title: "How the Python binding works"
weight: 81
---

# How the Python binding works

This explains the shape of the `subetha` package: why it is not a
`ctypes` wrapper, what the interpreter lock has to do with it, why
every value is boxed, and why only part of the surface can be awaited.
It is background rather than instruction. To use the thing, start at
[SubEtha from Python](../../tutorial/python/).

## There is no C shim

The usual way to reach a native library from Python is to declare its C
functions to `ctypes` and marshal arguments across on every call. This
binding is compiled against the interpreter instead: the classes are
Rust types with Python methods on them, built with PyO3.

The difference is measurable rather than aesthetic. On a Ryzen 9 7900X
the same atomic load costs **30.1 ns through this binding and 584.4 ns
through a `ctypes` shim**, nineteen times dearer for the same work,
because every crossing in the second case marshals through a generic
layer that knows nothing about the types involved.

It also means there is nothing to install beside the wheel: no shared
library to place, no header to match, no version skew between a shim
and the library it wraps.

## The cost that is left is Python's own

An empty Python loop costs 7.7 ns an iteration on that machine, and the
binding's call costs 30.1. So roughly 22 ns of the 30 is the call
itself: the interpreter building a frame, binding arguments, and
returning. Nothing in the Rust can reduce that, and the library
underneath is 7.1 ns of it.

That is why the surface has batch forms everywhere. `fetch_add_many`
does not make the operation faster; it makes one call do a thousand of
them, which amortizes the 22 ns to about 1.3. Every design decision
about performance in this binding is a decision about how often you
cross, never about what happens on the far side.

The buffer path is the same idea taken further: `memoryview(region)`
crosses once and is then read with no call at all, at 0.3 ns a byte.
The trap is that walking that view element by element from Python costs
33 ns an element, because now you are paying Python's per-element cost
instead. A buffer is fast when something consumes it whole and slow
when a Python loop walks it, and the mapping has nothing to do with
either number.

## It genuinely releases the interpreter

The module declares that it does not need the interpreter lock. On a
free-threaded build the lock stays off once `subetha` is imported, and
several threads really do run inside these calls at once rather than
taking turns.

That declaration is a claim about every call that could block, so it is
tested as one: `tests/test_threading.py` runs the counters, both kinds
of lock hold, the semaphore's permits, a ring several senders share, a
pinned scan running beside writers, and a buffer view held across other
threads' work, each under eight threads, on CPython 3.14 free-threaded
with the lock reported off.

A free-threaded interpreter takes a wheel of its own because the stable
ABI does not cover free-threading until 3.15. On every other
interpreter one wheel serves 3.11 upward, since the stable ABI rides
the crate's default feature.

## Only part of it can be awaited, and the reason is ownership

`subetha.aio` is a small pure-Python module: eight coroutines, no Rust.
That looks like an omission and is not. The Rust side has a reactor, an
executor and a task pool, and none of it can be bound, because every
entry point there takes or returns a Rust future, which has no Python
equivalent to hand back. What a coroutine actually needs is not Rust's
executor but a way to wait without blocking its own loop.

Which parts can be awaited follows from what can cross a thread
boundary:

- **A queue can.** What comes back is `bytes`, which belongs to nobody.
- **A hold cannot.** A lock hold belongs to the thread that took it, so
  it cannot be handed to a coroutine that may resume elsewhere. That is
  why `with_permit`, `with_read_lock` and `with_write_lock` take your
  work as a callable and run it on the holding thread, rather than
  giving you a hold to await against.
- **A `SensReceiver` cannot leave its thread at all**, so `aio.poll`
  asks on the calling thread and yields between tries.

The module is shaped by that ownership rather than by what would have
been convenient.

## Every value is boxed, and that is not a style choice

Python's object allocator aligns to sixteen bytes. `HandshakeHeader` is
cache-line aligned, and many of the primitives embed one. A class
holding such a value inline compiles, imports, and then faults inside
its constructor on the first aligned store, which is about as late and
as confusing as a failure can arrive.

So every value is boxed, and a compile-time assertion per class
enforces it. The cost is one indirection; the alternative is a crash
that appears only on the types that happen to embed an aligned header.

## A borrowed guard is declared first

Rust drops struct fields in declaration order. A pin or an ordered
receiver written the obvious way round, the handle first and the
borrowed guard after, releases the object it borrowed from before the
guard that still has to use it.

Both `SlabPin` and `OrderedReceiver` are declared the other way round,
and both have a test that collects the owning object and then uses the
borrower. What you see from Python is the pleasant consequence: a pin
keeps its structure alive, so dropping the structure first is safe and
the mapping goes when the last borrower does.

## A refusal is an answer

The surface returns `False` for a full ring, `None` for an empty one,
`0` for a sweep that freed nothing. None of those raises.

This matters more in Python than it might elsewhere, because the
alternative is a `try` around every read in a polling loop, where a
genuine fault would then be indistinguishable from the ring being
briefly empty. Reserving exceptions for faults is what makes
`except OSError` mean something.

One distinction inside that convention is easy to miss: a value that is
not measured yet is `None`, and that is a different answer from zero.
Zero means it was measured and it was zero.

## Where to go next

- [The reference](../../reference/subetha-py/) for the surface this
  describes.
- [Make it fast](../../how-to/python/make-it-fast/) to act on the cost
  model above.
- [How the PowerShell binding works](../powershell-binding/) for the
  same primitives reached from a shell, where the trade-offs land
  differently.
