---
title: "Install the package"
weight: 10
---

# Install the package

```bash
pip install subetha-ipc
```

The distribution is `subetha-ipc` because `subetha` on PyPI belongs to
an unrelated project. The package it installs is `subetha`, so code
says `import subetha`.

```python
import subetha
print(subetha.transports)      # what this wheel can reach another host with
print(subetha.free_threaded)   # which interpreter build this is
```

## What you get

The wheel carries the compiled extension, a `py.typed` marker and a
full type stub, so an editor and a type checker see every signature
without anything extra installed. Nothing else is needed at run time:
no Rust toolchain, no C compiler, no shared library to put on a path.

The stable ABI rides the crate's default feature, so one wheel serves
every interpreter from 3.11 upward rather than one per minor version.

## A free-threaded interpreter needs its own wheel

The stable ABI does not cover free-threading until 3.15, so a
free-threaded build is a separate wheel:

```bash
maturin build --release --no-default-features
```

`subetha.free_threaded` reports which one is installed. It matters
because the module declares that it does not need the interpreter lock:
on a free-threaded build the lock stays off when it is imported, and
several threads really do run inside these calls at once. See
[Threads, asyncio and lifetimes](../threads-and-lifetimes/).

## Which transports a wheel carries

The Sens-O-Matic link is always present. Each bridge is there only if
its feature was compiled in, so a wheel is not a fixed surface:

```python
>>> subetha.transports
['sens', 'tcp', 'quic']
```

`subetha.OPTIONAL_BY_TRANSPORT` says what each absent transport would
have brought, which is how a caller tells a wheel built without one
from a name that never existed at all. Do not test for a bridge by
catching `AttributeError`.

## From a build

Building from the repository is what you want while changing the
binding itself. `maturin develop` compiles the extension and installs
it into the active environment:

```bash
python -m venv .venv
.venv/bin/pip install maturin pytest
cd crates/subetha-py
../../.venv/bin/python -m maturin develop --release
```

Add the bridges with features, which is what makes `transports` report
more than the lossy link:

```bash
maturin develop --release --features tcp-bridge,quic-bridge
```

That difference shows up in the suite as well as in `transports`: the
default build passes 502 tests and skips 7, and the same build with both
bridges passes 508 and skips 1, because the bridge suites only exist when
their feature is compiled in. The one skip that survives both is a
vectorized row that needs numpy.

## Confirm it works

```python
import subetha

a = subetha.Atomic("/tmp/subetha-check", init=40)
a.fetch_add(2)
assert a.load() == 42
print(subetha.boundary_note())
```

The counter is the part worth running: importing proves the extension
loaded, and the counter proves it reaches the mapped file underneath.

## Where to go next

- [Choose a structure](../choose-a-structure/) if you know the shape of
  your problem but not which class to reach for.
- [When something does not work](../troubleshoot/) for an import that
  fails or a transport that is missing.
- [SubEtha from Python](../../../tutorial/python/) for the walk from
  here to a message crossing between two processes.
