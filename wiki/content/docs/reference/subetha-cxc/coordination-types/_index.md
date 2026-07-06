---
title: "Coordination Primitives"
weight: 290
sidebar:
  open: true
---

# Coordination primitives

Heartbeat, failover, barrier, and work-distribution primitives layered on the substrate.

| Primitive | Purpose |
|---|---|
| [Heartbeat Table](heartbeat/) | Per-process heartbeat slots; backs failover |
| [Epoch Barrier](epoch-barrier/) | All N processes finish phase K before any starts phase K+1 |
| [Failover Watchdog](failover/) | Scans heartbeats; reclaims work from dead peers |
| [Event State Log](event-state-log/) | Event-sourced state with cross-process replay |
| [Priority Fanout](priority-fanout/) | Tiered work queue; O(1) priority selection |
| [Progress Task](progress-task/) | Distributed work with live cross-process progress reporting |
| [Background Scheduler](scheduler/) | Autonomous Pass executor; survives process restart |
| [Pass Registry](pass-registry/) | Cross-process closure dispatch (`Pass<F>` from one process; fires in another) |
| [Shared Async Pointer](shared-async-pointer/) | Cross-process lazy / speculative future-like pointer |
| [K-Tower Cascade](k-tower-cascade/) | Recursive pow2-of-pow2 cascading container for multi-resolution coordination |
| [Cross-Process Waker](cross-process-waker/) | Userspace-`futex` slot list in MMF; cross-process wake via SHARED `futex` on Linux, `WaitOnAddress` on Windows; backs the `Blocking{Spsc,Mpsc,Mpmc}Ring` wrappers |
| [Shared Condvar](shared-condvar/) | Cross-process Mesa-style condition variable; one generation counter + `CrossProcessWaker`; cross-process wake proven on WSL Linux via SHARED `futex` |
| [Blocking Semaphore](blocking-semaphore/) | Cross-process counting semaphore with kernel-park slow path; replaces the existing `SharedSemaphore`'s sleep tail with a real futex park |
| [Blocking RW Lock](blocking-rw-lock/) | Cross-process reader-writer lock with kernel-park slow path; both readers and writers park on the same waker |
| [QoS Policy](qos-policy/) | DDS-inspired runtime-mutable QoS knobs (durability / history / ordering / ...); sidecar policies read them each scan to drive substrate morphs |
| [Subscriber Position](subscriber-position/) | Aeron-inspired MMF-resident position counter; a restarted subscriber reopens by path and resumes from the last acknowledged position |
| [Virtual Endpoint](virtual-endpoint/) | Substrate-level endpoint identity that resolves to a local ring or a remote address at runtime via `as_local()` / `as_remote()` |

For the prose overview, see [coordination](../coordination/).
