---
title: Tutorial
linkTitle: Tutorial
weight: 1
sidebar:
  open: true
---

If you are new to SubEtha, start here. The tutorial takes you from
zero to your first cross-process channel in roughly twenty minutes,
then walks the observation pipeline so you know what the sidecar
is recording about your traffic.

Read the pages in order. Each shows its steps in Rust, Python and
PowerShell side by side:

1. [Getting started](getting-started/) - install, build, smoke-test.
2. [Installation](installation/) - the crates, the Python package or the PowerShell module, and a smoke test.
3. [Cross-process round-trip in 30 lines](cross-process-roundtrip/) - one process writes a shared hash map, another reads it.
4. [Reading sidecar observations](reading-observations/) - what the observation ring exposes and how to consume it.

Arriving with a C or C++ toolchain rather than a Rust one? Read
[SubEtha from C and C++](c-and-cpp/) instead of the pages above: it
installs the library, links a program against it, sends a message
through a ring, and reads a failure, without asking you to write Rust.

Arriving from Python? The pages above carry Python tabs, and
[SubEtha from Python](python/) goes further through the binding: two
processes sharing a counter,
a message crossing between them, a lock held across both, and the two
call shapes that cost far less than one call an item.

Arriving from PowerShell? The pages above carry PowerShell tabs, and
[SubEtha from PowerShell](powershell/) goes further through the
`SubEtha` module: a counter shared by
two sessions, a message through a ring and through the pipeline, a
lock held as an object, and the packed shapes that cost less than a
call an item.
