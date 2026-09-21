---
title: Python
linkTitle: Python
weight: 85
sidebar:
  open: false
---

Task-oriented guides for the `subetha` package, written from Python
rather than from the Rust. Each answers one question for someone who
has already sent a message across two processes in
[SubEtha from Python](../../tutorial/python/).

- [Install the package](install/) - from PyPI, from a build, and which
  wheel a free-threaded interpreter needs.
- [Choose a structure](choose-a-structure/) - name the shape you have,
  read off the structure that fits it.
- [Make it fast](make-it-fast/) - the two shapes that cross the
  boundary less often, what each costs, and the one that looks like a
  win and is not.
- [Threads, asyncio and lifetimes](threads-and-lifetimes/) - what
  releases the interpreter lock, what can be awaited and what cannot,
  and when a mapping goes away.
- [Bridge two hosts](bridge-two-hosts/) - carry a ring to another
  machine, and tell a wheel built without a transport from a name that
  never existed.
- [When something does not work](troubleshoot/) - the import failures,
  the refusals that are not failures, and what each exception means.

The complete surface is in the
[`subetha-py` reference](../../reference/subetha-py/), including
[what the values look like](../../reference/subetha-py/values/) and
[every class in full](../../reference/subetha-py/classes/).
