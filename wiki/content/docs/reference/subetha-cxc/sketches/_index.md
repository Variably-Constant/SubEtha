---
title: "Probabilistic Sketches"
weight: 260
sidebar:
  open: true
---

# Cross-process probabilistic sketches

Approximate aggregations - sub-linear memory for the cardinality of values they see.

| Primitive | Answers |
|---|---|
| [Shared Bit Vec](shared-bit-vec/) | Dense set membership over a small key space |
| [Shared Bloom Filter](shared-bloom-filter/) | "Has key X been seen?" with controlled false-positive rate |
| [Shared Blocked Bloom Filter](shared-blocked-bloom-filter/) | Same membership at one cache line per query; wins past L3 |
| [Shared Count-Min Sketch](shared-count-min-sketch/) | Approximate counts per key without keeping the keys |
| [Shared HyperLogLog](shared-hyper-log-log/) | Distinct-count estimate with very low memory; one shared HLL accumulates the union across processes (no separate merge step) |
| [Shared Histogram](shared-histogram/) | Bucketed value distribution (latency, etc.) |
| [Shared Reservoir Sampler](shared-reservoir-sampler/) | Uniform random sample from an unknown-size stream |

For the prose overview, see [shared-sketches](../shared-sketches/).
