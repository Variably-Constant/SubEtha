---
title: "Choose a structure"
weight: 20
---

# Choose a structure

The module exports 135 cmdlets, and most scripts need two or three of
them. This page goes the other way round from the
[reference](../../../reference/subetha-pwrs/): name the shape you have,
and read off what fits it.

## Start at the front door

Four cmdlets pick the shape underneath from what you describe. If one of
them matches, take it and stop reading; the families below are for when
you want to choose yourself.

| You want | Take |
|---|---|
| a queue between processes that a reader can wait on | `New-SubEthaChannel` |
| work one process owns, that idle processes help with | `New-SubEthaWorkQueue` |
| a lookup from one number to another | `New-SubEthaKvMap` |
| a queue, and you cannot predict the traffic | `New-SubEthaAdaptiveQueue` |

`New-SubEthaAdaptiveQueue` counts the sizes it is sent and moves between
a ring and a work-stealing deque while it runs. Reach for it when the
shape is genuinely unknown, not as a hedge: a shape you do know is
cheaper named outright.

## Moving items between processes

Answer two questions: how many processes write, and does every reader
need every item.

| Writers | Readers | Every reader sees everything | Take |
|---|---|---|---|
| one | one | - | `New-SubEthaSpscRing` |
| one | many | yes | `New-SubEthaBroadcastRing` |
| many | one | - | `New-SubEthaMpscPool` |
| many | many | no, items are shared out | `New-SubEthaMpmcGrid` |
| one | many | yes, and a slow reader must be told what it missed | `New-SubEthaPubSub` |
| any | any | you would rather not decide | `New-SubEthaRing` |

`New-SubEthaRing` is the adaptive ring and the reasonable default: it
registers producers and consumers by id, carries payloads bigger than a
slot through `SendFrame` and `RecvFrame`, and changes shape under the
traffic it sees.

The constructors that serve several ends hand out every end at once,
because the shape is what makes them correct. Say how many of each you
want when you build it:

```powershell
$pool = New-SubEthaMpscPool -Path C:\ipc\pool -Producers 4 -Capacity 4096
$pool.Producers[0].Send($bytes)
$pool.Consumer.Recv()

$grid = New-SubEthaMpmcGrid -Path C:\ipc\grid -Producers 4 -Consumers 2 -Capacity 4096
```

The Lamport pair is the one that writes out as two objects rather than
one holding both, producer first:

```powershell
$p, $c = New-SubEthaLamportPair -Path C:\ipc\lamport -Capacity 4096
```

`PubSub` is the one to take when falling behind must be visible. A
subscriber that drops too far back gets an error with id
`SubEthaLagged` naming what it lost, rather than silently resuming at
whatever survived.

## When order matters

A ring does not promise that items from different senders arrive in the
order their senders made them. If that matters, say so when you build
it and read through an ordered receiver:

```powershell
$ring = New-SubEthaRing -Path C:\ipc\events -Capacity 4096 -Stamps Counter
$ordered = $ring.OrderedReceiver($consumer)
$ordered.Drain()
```

`OrderedReceiver` refuses a ring built without stamps rather than
waiting forever for an order it can never establish. Use `Drain` rather
than `Recv`: `Recv` answers `$null` both when the window is still
filling and when the ring is empty, and `Drain` takes the ring and the
held-back tail together so the two cases never need telling apart.

For items that come from somewhere other than a ring,
`New-SubEthaReorderWindow` is the same window standing alone.

## Holding state rather than moving items

| You want | Take |
|---|---|
| one number several processes update | `New-SubEthaAtomic` |
| one value replaced whole | `New-SubEthaCell` |
| an indexable array of fixed-size elements | `New-SubEthaVec` |
| numbered slots that come and go | `New-SubEthaSlab` |
| a keyed lookup where entries are removed | `New-SubEthaHashMap` |
| the same, in key order | `New-SubEthaBTreeMap` |
| a block of memory you address yourself | `New-SubEthaRegion` |

`KvMap` from the front door has no removal at all, which is why
`HashMap` exists: reach for `KvMap` when keys only ever accumulate, and
`HashMap` the moment anything has to go away.

`Vec` and `Slab` are seqlocked, so a reader retries rather than blocks.
Reading a run of them goes through `ReadRange`, which honors the retry
on every element and moves them packed end to end in one call.

## When readers must not see a half-finished write

The versioned family keeps history so a reader gets one unchanging view
while writers carry on:

| You want | Take |
|---|---|
| one value's past versions | `New-SubEthaVersionChain` |
| numbered slots, each with recent history | `New-SubEthaVersionedSlab` |
| a keyed index scanned while it is written | `New-SubEthaVersionedMap` |
| the same, with several writers at once | `New-SubEthaLanedMap` |

Reading takes a pin, and the pin is what holds the view still. The
reading methods are on the pin rather than on the structure, which is
what makes it impossible to read without one:

```powershell
$pin = $map.Pin()
try {
    $pin.Scan(0, 1000, 100)      # low, high, limit; a SubEtha.Entry each
} finally {
    $pin.Release()
}
```

`LanedMap` splits the index across several trees so writers do not
queue behind one another. A key belongs to one lane for its whole life;
writing it through another is an error named `SubEthaWrongLane` that
names the right lane. Claim with `ClaimLaneFor($key)` when the key
already exists and `ClaimLane()` when it does not. Reading claims
nothing.

Two names in this family mean what the Rust says rather than what they
sound like. `VoidEpoch` does not reclaim: it undoes the writes stamped
at exactly one epoch, for a writer that died partway through. `Sweep`
and `SweepSlot` reclaim.

## Making processes wait for each other

| You want | Take |
|---|---|
| exclusive or shared access to something | `New-SubEthaRWLock` |
| at most N processes doing something at once | `New-SubEthaSemaphore` |
| to wait until a condition holds | `Wait-SubEthaCondition` |
| one process to compute a value the rest use | `New-SubEthaLazyValue` |
| one process to own something, another to take over when it dies | `New-SubEthaOwnerLease` |
| everyone to reach the same point before continuing | `New-SubEthaEpochBarrier` |
| to know which processes are alive | `New-SubEthaHeartbeat` |
| to agree which process leads | `New-SubEthaLeaderElection` |

Every waiting form that takes a timeout sleeps rather than spins, so a
long wait costs no processor, and none of them waits past its deadline
even if whoever holds the lock never gives it back.

The condition variable is a cmdlet rather than a method because its
condition is a script block, and a script block runs only on the
pipeline thread:

```powershell
Wait-SubEthaCondition -Path C:\ipc\cv -Until { $ready.Load() -eq 1 } -Timeout 5
```

## Counting and sampling without keeping everything

`BloomFilter` and `BlockedBloomFilter` for set membership,
`CountMinSketch` for frequencies, `HyperLogLog` for distinct counts,
`Histogram` for distributions, `Reservoir` for a bounded unbiased
sample, `RateLimiter` for a budget spent across processes.

Size the two filters and the sketch from what you can tolerate rather
than guessing:

```powershell
Measure-SubEthaBloomSize -Items 10000 -FalsePositiveRate 0.01
Measure-SubEthaSketchSize -Epsilon 0.01 -Delta 0.01
```

## Still not sure

Take `New-SubEthaChannel` if items move and `New-SubEthaHashMap` if
state sits. Both are ordinary, both are easy to replace once the shape
is clearer, and neither commits you to anything the others do not.

## Where to go next

- [Make it fast](../make-it-fast/) once the structure is right and the
  per-item cost is not.
- [The reference](../../../reference/subetha-pwrs/) for every method on
  whichever one you picked.
- `Get-Help New-SubEthaRing -Full` for the parameters and an example,
  without leaving the shell.
