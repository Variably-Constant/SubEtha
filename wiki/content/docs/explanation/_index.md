---
title: Explanation
linkTitle: Explanation
weight: 4
sidebar:
  open: true
---

Conceptual / rationale-oriented pages. Read these when the *why*
matters more than the *what*.

- [Architecture overview](architecture/) - how the substrate, sidecar, and primitive families compose.
- [The frozen-handshake premise](frozen-handshake/) - why every primitive is one frozen handshake between two roles.
- [Concurrency and safety](concurrency-and-safety/) - thread- and process-safety, why you add no locks, what you are responsible for, and how the `AdaptiveRing` swaps shape under live readers / writers without blocking or losing items.
- [The MMF substrate](mmf-substrate/) - why one memory-mapped file gives cross-thread, cross-process, and disk-persistent semantics from one byte layout.
- [The observation pipeline](observation-pipeline/) - producer rings, sidecar drain, contention avoidance.
- [Citations and references](citations/) - the published lock-free, probabilistic data-structure, and distributed-systems literature each named primitive comes from, with file paths into `crates/`.
