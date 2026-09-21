---
title: "Choose a structure"
weight: 20
---

# Choose a structure

The package exports 91 classes, and most programs need two or three.
This page goes the other way round from the
[reference](../../../reference/subetha-py/): name the shape you have,
and read off what fits it.

## Start at the front door

Four classes pick the shape underneath from what you describe. If one
matches, take it and stop reading.

| You want | Take |
|---|---|
| a queue between processes that a reader can wait on | `Channel` |
| work one process owns, that idle processes help with | `WorkQueue` |
| a lookup from one number to another | `KvMap` |
| a queue, and you cannot predict the traffic | `AdaptiveQueue` |

`AdaptiveQueue` counts the sizes it is sent and moves between a ring
and a work-stealing deque while it runs. Take it when the shape is
genuinely unknown, not as a hedge: a shape you do know is cheaper named
outright.

## Moving items between processes

Answer two questions: how many processes write, and does every reader
need every item.

| Writers | Readers | Every reader sees everything | Take |
|---|---|---|---|
| one | one | - | `SpscRing` |
| one | many | yes | `BroadcastRing` |
| many | one | - | `mpsc_pool(...)` |
| many | many | no, items are shared out | `mpmc_grid(...)` |
| one | many | yes, and a slow reader must be told what it missed | `PubSub` |
| any | any | you would rather not decide | `Ring` |

`Ring` is the adaptive ring and the reasonable default: it registers
producers and consumers by id, carries payloads bigger than a slot
through `send_frame` and `recv_frame`, and changes shape under the
traffic it sees.

The multi-end shapes are module functions rather than classes, because
they hand out every end at once and the shape is what makes them
correct:

```python
producers, consumer = subetha.mpsc_pool("/ipc/pool", producers=4, capacity=4096)
producers, consumers = subetha.mpmc_grid("/ipc/grid", producers=4, consumers=2, capacity=4096)
producer, consumer = subetha.lamport_pair("/ipc/lamport", capacity=4096)
```

Each has an `_open` counterpart that attaches to one that already
exists.

`PubSub` is the one to take when falling behind must be visible: a
subscriber that drops too far back raises `Lagged` naming what it lost,
rather than silently resuming at whatever survived.

## When order matters

A ring does not promise that items from different senders arrive in the
order their senders made them. If that matters, say so when you build
it and read through an ordered receiver:

```python
ring = subetha.Ring("/ipc/events", capacity=4096, stamps="counter")
ordered = ring.ordered_receiver(consumer)
ordered.drain()
```

`ordered_receiver` refuses a ring built without stamps rather than
waiting forever for an order it can never establish. Use `drain` rather
than `recv`: `recv` answers `None` both while the window is still
filling and when the ring is empty, and `drain` takes the ring and the
held-back tail together so the two never need telling apart.

For items from somewhere other than a ring, `ReorderWindow` is the same
window standing alone.

## Holding state rather than moving items

| You want | Take |
|---|---|
| one number several processes update | `Atomic` |
| one value replaced whole | `Cell` |
| an indexable array of fixed-size elements | `Vec` |
| numbered slots that come and go | `Slab` |
| a keyed lookup where entries are removed | `HashMap` |
| the same, in key order | `BTreeMap` |
| a block of memory you address yourself | `Region` |

`KvMap` from the front door has no removal at all, which is why
`HashMap` exists: take `KvMap` when keys only accumulate, and `HashMap`
the moment anything has to go away.

`HashMap` keys and values are `bytes` of exactly the declared size,
not integers:

```python
m = subetha.HashMap("/ipc/map", capacity=64, key_size=8, value_size=8)
m.insert((7).to_bytes(8, "little"), (70).to_bytes(8, "little"))
```

A `Region` can be viewed rather than copied with `memoryview(region)`,
which is the fastest shape here and also the easiest to misuse; see
[Make it fast](../make-it-fast/).

## When readers must not see a half-finished write

The versioned family keeps history so a reader gets one unchanging view
while writers carry on:

| You want | Take |
|---|---|
| one value's past versions | `VersionChain` |
| numbered slots, each with recent history | `VersionedSlab` |
| a keyed index scanned while it is written | `VersionedMap` |
| the same, with several writers at once | `LanedMap` |

Reading takes a pin, and the pin is a context manager like everything
else that holds a resource:

```python
with vmap.pin() as pin:
    for key, value in pin.scan(0, 1000, 100):
        ...
```

`scan` answers a list of `(key, value)` tuples; `scan_from` answers
that list and where to carry on. `LanedMap` splits the index across
several trees so writers do not queue behind one another, and a key
belongs to one lane for its whole life: writing it through another
raises `WrongLane` naming the right one.

## Making processes wait for each other

| You want | Take |
|---|---|
| exclusive or shared access to something | `RWLock` |
| at most N processes doing something at once | `Semaphore` |
| to wait until a condition holds | `Condvar` |
| one process to compute a value the rest use | `LazyValue` |
| one process to own something, another to take over when it dies | `OwnerLease` |
| everyone to reach the same point before continuing | `EpochBarrier` |
| to know which processes are alive | `Heartbeat` |
| to agree which process leads | `LeaderElection` |

Every waiting form that takes a timeout sleeps rather than spins, and
none waits past its deadline even if whoever holds the lock never gives
it back. To wait from a coroutine without blocking the loop, use
`subetha.aio`; see
[Threads, asyncio and lifetimes](../threads-and-lifetimes/).

## Counting and sampling without keeping everything

`BloomFilter` and `BlockedBloomFilter` for set membership,
`CountMinSketch` for frequencies, `HyperLogLog` for distinct counts,
`Histogram` for distributions, `Reservoir` for a bounded unbiased
sample, `RateLimiter` for a budget spent across processes.

Size them from what you can tolerate rather than guessing. The sizing
is a static method on the class rather than a separate function:

```python
bits, hashes = subetha.BloomFilter.suggest_config(10000, 0.01)
f = subetha.BloomFilter("/ipc/bloom", n_bits=bits, n_hashes=hashes)
```

An item is `bytes`, never a number.

## Still not sure

Take `Channel` if items move and `HashMap` if state sits. Both are
ordinary, both are easy to replace once the shape is clearer, and
neither commits you to anything the others do not.

## Where to go next

- [Make it fast](../make-it-fast/) once the structure is right and the
  per-item cost is not.
- [What the values look like](../../../reference/subetha-py/values/)
  for what these actually hand back.
- [Every class in full](../../../reference/subetha-py/classes/) for
  every method on whichever one you picked.
