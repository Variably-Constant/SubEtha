---
title: PowerShell
linkTitle: PowerShell
weight: 80
sidebar:
  open: false
---

Task-oriented guides for the `SubEtha` module, written from the shell
rather than from the Rust. Each answers one question for someone who
has already sent a message across two processes in
[SubEtha from PowerShell](../../tutorial/powershell/).

- [Install the module](install/) - from the gallery, from a build, or
  vendored into a repository, and what lands on disk.
- [Choose a structure](choose-a-structure/) - name the shape you have,
  read off the structure that fits it.
- [Make it fast](make-it-fast/) - the three call shapes, what each
  costs in both hosts, and how to measure your own script.
- [Runspaces, threads and lifetimes](runspaces-and-lifetimes/) - share
  one object across runspaces, and know when a mapping goes away.
- [Bridge two hosts](bridge-two-hosts/) - carry a ring to another
  machine over TCP or QUIC, and what the lossy link does differently.
- [When something does not work](troubleshoot/) - the import failures,
  the two-host differences, and what each error id means.

The complete surface is in the
[`subetha-pwrs` reference](../../reference/subetha-pwrs/), and every
cmdlet carries its own help: `Get-Help New-SubEthaRing -Full` gives a
description, every parameter, and an example.
