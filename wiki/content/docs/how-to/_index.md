---
title: How-To Guides
linkTitle: How-To
weight: 2
sidebar:
  open: true
---

Task-oriented guides. Each page answers one question for someone
who already knows the basics from the [Tutorial](../tutorial/).

- [Pick the right primitive for the role-pair shape](role-pair-selection/) - map your concurrency shape (SPSC, MPSC, snapshot reads, etc.) to an MMF primitive.
- [Async: cost and scaling](async-paths/) - when to use sync / blocking / async on one handle, the measured per-op overhead, and the fixed-pool fan-out scaling.
- [Write a custom Policy](custom-policy/) - implement the `Policy` trait so the sidecar swaps strategies on your own criteria.
- [Compose primitives via `SidecarBox`](sidecar-box/) - wrap any `AdaptiveInstance` so the sidecar manages it.
- [Tune the observation ring + sidecar scan interval](tune-sidecar/) - knobs for the substrate and the control-plane scan loop.
- [Bridge two hosts (QUIC / TCP / Sens-O-Matic)](cross-host-bridge/) - connect rings across machines: certificates, the firewall step per OS, run commands, and the transport comparison.
- [Tuning and overrides](tuning-overrides/) - every environment variable, Cargo feature, build recipe, and runtime probe in one place.
