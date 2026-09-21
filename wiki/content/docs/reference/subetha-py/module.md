---
title: "Module functions, attributes and exceptions"
weight: 20
---

# Module functions, attributes and exceptions

What `import subetha` gives you besides the classes. Generated
from the type stub by
`crates/subetha-py/tools/export_reference.py`.

## Functions

| Function | What it does |
|---|---|
| `boundary_note() -> str` | What this binding costs, reported by the binding itself so a caller can check the claim on its own machine rather than take the number in the documentation. |
| `generate_self_signed_cert(name: str) -> tuple[bytes, bytes]` | Make a certificate and its key for a QUIC bridge, both as bytes. |
| `lamport_pair(path: str, capacity: int) -> tuple[LamportProducer, LamportConsumer]` | Make a Lamport pair at `path`: one producer and one consumer over one ring. |
| `lamport_pair_open(path: str, capacity: int) -> tuple[LamportProducer, LamportConsumer]` | Attach to a Lamport ring that already exists. |
| `mpmc_grid(path: str, producers: int, consumers: int, capacity: int) -> tuple[list[MpmcProducer], list[MpmcConsumer]]` | Build an MPMC grid: a ring per producer, shared out among consumers. |
| `mpmc_grid_open(path: str, producers: int, consumers: int, capacity: int) -> tuple[list[MpmcProducer], list[MpmcConsumer]]` | Attach to a grid that already exists, with the shape it was built at. |
| `mpsc_pool(path: str, producers: int, capacity: int) -> tuple[list[MpscProducer], MpscConsumer]` | Build an MPSC pool: one ring per producer, drained by one consumer. |
| `mpsc_pool_open(path: str, producers: int, capacity: int) -> tuple[list[MpscProducer], MpscConsumer]` | Attach to a pool that already exists, with the shape it was built at. |

## Attributes

| Attribute | Type |
|---|---|
| `transports` | `list[str]` |
| `free_threaded` | `bool` |
| `OPTIONAL_BY_TRANSPORT` | `dict[str, tuple[str, ...]]` |

## Exceptions

| Exception | Raised when |
|---|---|
| `Contended` | Another live process holds the lease and has not been quiet long. |
| `Lagged` | A subscriber fell behind and what it asked for was overwritten. |
| `WrongLane` | The key belongs to a different lane and must be written through it. |

