---
title: SubEtha PowerShell
weight: 58
---

# The PowerShell binding (`subetha-pwrs`)

`subetha-pwrs` gives PowerShell the memory-mapped primitives directly,
with no C shim between the shell and the Rust. It ships as a PowerShell
module named `SubEtha` rather than to crates.io, and runs on
PowerShell 7.5 and later and on Windows PowerShell 5.1.

```powershell
Import-Module SubEtha
```

This page is the reference. To install it and send a first message,
start at [SubEtha from PowerShell](../../tutorial/powershell/). For a
task rather than a surface, the
[PowerShell how-to guides](../../how-to/powershell/) cover installing,
choosing a structure, making it fast, runspaces and lifetimes, bridging
two hosts and diagnosing a failure;
[how the binding works](../../explanation/powershell-binding/) covers
why it is shaped this way.

This page describes the surface by family. The complete surface, every
name with every type, is generated from the built module and split
across three pages:

- [What the values look like](values/) - real output, captured from a
  run, for the shapes a type name cannot convey.
- [Every cmdlet, in full](cmdlets/) - all 135, grouped by family with a
  contents list: each parameter, its type, whether it is required, its
  position, what it takes from the pipeline, what the cmdlet writes,
  and the properties and methods of whatever came back.
- [Every object, in full](classes/) - all 117 object types, with each
  property and each method signature and return type.
- [Every enum](enums/) - all 14, with their values.

Those three are produced by
`crates/subetha-pwrs/tools/Export-Reference.ps1` reading the module
itself, so they cannot disagree with what ships. Regenerate them
whenever the surface changes.

Every cmdlet also carries its own help, which is the faster lookup
while you are in a shell: `Get-Help New-SubEthaRing -Full` gives a
description, every parameter and an example.

## What a call costs, and what follows

Measured on a Ryzen 9 7900X, the median of five runs, in nanoseconds
per operation:

| reached by | PowerShell 7.6 | Windows PowerShell 5.1 |
|---|---|---|
| an empty PowerShell loop | 203 | 290 |
| **this binding, a method call** | **1907** | **651** |
| this binding, a method call with an argument | 2656 | 995 |
| this binding, a property read | 368 | 771 |
| **this binding, batched a thousand at a time** | **4.5** | **3.0** |
| a ring, one `Push` and one `Pop` per item | 4848 | 2013 |
| a ring, 256 items per `PushMany` and `PopMany` | 405 | 805 |
| **a ring, 256 items packed in one `byte[]` each way** | **39** | **22** |
| the pipeline, one record per item through `Send-SubEthaItem` and `Receive-SubEthaItem` | 1712 | 7955 |

The same atomic load costs the Rust about 7 ns through the C ABI. What
a method call costs is what the host's own method invocation costs,
and the two hosts differ on that more than anything in the library
does: PowerShell 7 reads a property cheaply and calls a method dearly,
Windows PowerShell the other way round. Three shapes cross less often,
and the surface is built around them:

- A batch call carries many operations over one call. `FetchAddMany`,
  `SendMany`, `RecvMany`, `InsertMany`, `GetMany`, `Drain` and their kin
  are all this shape.
- A packed buffer carries many items in one `byte[]` with no object per
  item: `PushPacked`, `SendPacked` and `PopPacked` on the rings,
  `ReadRange` and `WriteRange` on the vector and the slab. Read whole,
  it is tens of nanoseconds an item in either host.
- The pipeline is for a script that reads as a pipeline. Per record it
  costs about what a method call does in PowerShell 7 and several times
  that in Windows PowerShell.

`bench/CallShapes.ps1` in the crate reproduces every row above on the
reader's own machine, in whichever host runs it.

## Two layers, and the pipeline between them

A cmdlet invocation costs the pipeline's per-record work. A method
call on an object the module returned costs one native call into the
library plus the host's own method invocation, which the table above
puts a number on. So the module is built in two layers:

- Cmdlets obtain a structure. `New-SubEthaRing` creates the file
  when it does not exist and attaches to it when it does;
  `Open-SubEthaRing` attaches to one that must already exist. A path is
  resolved against the session's current location. Every cmdlet also
  answers to a shorter name with the `SE` prefix: `New-SERing`.
- Objects operate on it. What a cmdlet writes is an object of a
  `SubEtha.*` type. Its properties are what was fixed when the
  structure was obtained (the path, the capacity, the slot size), and
  its methods are the operations the Rust type offers, one method per
  operation, named in PascalCase after the Rust: `$ring.RegisterProducer()`,
  `$ring.Send($producer, $bytes)`, `$map.Insert(7, 70)`.
- The pipeline moves items. `Send-SubEthaItem -To $structure` sends
  whatever is piped in and writes back what the structure refused, so a
  full ring hands the item back rather than dropping it.
  `Receive-SubEthaItem -From $structure` reads a structure out into the
  pipeline, everything waiting or up to `-Count`. Both call the object's
  own methods through the engine, so they serve every structure that
  moves items; a script moving bulk items calls the many-item methods
  directly.

## What crosses

| Rust side | PowerShell side |
|---|---|
| a byte payload | `byte[]`, pinned where it lies as an argument and filled through one pin as a result; a string argument is taken as its UTF-8, and any other array of numbers is converted |
| a value of a declared size | `byte[]` of exactly that size |
| an unsigned integer key or value | `ulong` |
| a count, an index, a capacity | `ulong` in, `ulong` out (`uint` where the Rust is 32-bit) |
| a memory ordering, a locale, a stamp kind, a shape | an enum under `SubEtha.`: `[SubEtha.MemoryOrder]::Relaxed`, or its name as a string |
| a tuple | a small record class: `SubEtha.StampedItem`, `SubEtha.ClockReading`, `SubEtha.Entry`, `SubEtha.Scan`, `SubEtha.Endpoint` and the rest |
| a guard the Rust returns | an object that gives it back: `SubEtha.Hold`, `SubEtha.PermitHold`, `SubEtha.LeaseHold`, `SubEtha.SlabPin`, `SubEtha.MapPin`, `SubEtha.LanedPin`, `SubEtha.LaneClaim` |
| an absent answer | `$null` |

Items packed end to end cross as one `byte[]`: `PushPacked` and
`SendPacked` take a buffer and an item length, `PopPacked` answers a
`SubEtha.PackedItems` holding the count and the bytes at a slot's full
width each.

## The conventions

- A refusal is an answer, not a fault. A push that does not fit returns
  `$false`; a pop with nothing to take returns `$null`; a sweep that
  freed nothing returns `0`; a sample that kept nothing returns
  `$null`. Only a genuine fault is an error.
- A method fails with an exception; a cmdlet writes an error record,
  non-terminating unless the cmdlet cannot go on, so
  `-ErrorAction` works the PowerShell way. Every error id starts with
  `SubEtha`: `SubEthaOpen` when a structure could not be obtained,
  `SubEthaArgument` for an argument that cannot be right,
  `SubEthaOperation` for a failure underneath, `SubEthaLagged` when a
  subscriber fell far enough behind that what it asked for was
  overwritten, `SubEthaContended` when a lease or a lane is held by
  somebody else, `SubEthaWrongLane` when a key belongs to another lane,
  `SubEthaKeyAbsent` when no lane holds a key, `SubEthaNotOwner` and
  `SubEthaLease` from the lease, and `SubEthaReleased` for a pin, hold
  or claim already given back.
- Anything holding a resource is disposable. A hold, a permit, a
  pin or a lane claim gives its resource back on `Release()`, on
  `Dispose()`, and when the garbage collector finalizes it, so a script
  that leaves a block early does not strand a lock. Every object frees
  its mapping the same way, and `IsDisposed` says whether it has.
- A value that is not measured yet is `$null`, which is a different
  answer from zero.
- `-ErrorAction Stop` on a cmdlet turns a refusal to obtain into an
  exception, which is what a test or a script that cannot go on wants.

## The front door

Four cmdlets that pick the shape underneath from what is described,
rather than asking the caller to choose one. Reach for these first; the
families below give more control when it is wanted.

| Cmdlet | Object | What it is |
|---|---|---|
| `New-SubEthaChannel` | `SubEtha.Channel` | A queue between processes that can be waited on. `Recv` answers `$null` at once when there is nothing there; `RecvFor` waits up to a timeout in seconds, sleeping rather than spinning. `SendFor` is the same on the other side. |
| `New-SubEthaWorkQueue`, `Open-SubEthaWorkQueue` | `SubEtha.WorkQueue` | Work one process owns and others take from when idle. The owner pushes and pops at the cheap end it has to itself; a thief opens it and steals from the other end. |
| `New-SubEthaKvMap` | `SubEtha.KvMap` | A lookup table between processes, from one unsigned integer to another. |
| `New-SubEthaAdaptiveQueue` | `SubEtha.AdaptiveQueue` | The one that picks from what happens rather than what was declared. It counts the sizes it is sent and moves between a ring and a work-stealing deque while running; `Shape()`, `Traffic()` and `MaybeChangeShape()` show and drive it. |

`KvMap.Insert` answers whether the key was new rather than what it
held, because that is what the map underneath reports, and there is no
way to take a key out because it has no removal. `HashMap` is the one
to reach for when entries have to go away.

`New-SubEthaQosPolicy` writes down what a stream needs, from settings or
from a `-Preset` (`Streaming`, `ReliablePubSub`, `PersistentLog`), and
its `Snapshot()` says where the bytes should live and whether the
ordering must change. Ordering is set by the caller and never inferred
from the traffic, because whether a reader needs one sender's order or
every sender's order is something only the application knows.

## Rings and channels

| Cmdlet | Object | What it is |
|---|---|---|
| `New-SubEthaRing`, `Open-SubEthaRing` | `SubEtha.Ring` | The adaptive ring: producers and consumers register for an id, payloads larger than a slot go through `SendFrame` and `RecvFrame`, and the shape changes under the traffic. Built with `-Stamps Counter` it marks each item with the order its sender made it in. |
| `New-SubEthaSpscRing` | `SubEtha.SpscRing` | One writer, one reader. |
| `New-SubEthaBroadcastRing` | `SubEtha.BroadcastRing` | One writer, many readers, each seeing everything. |
| `New-SubEthaPubSub` | `SubEtha.PubSub`, `SubEtha.Subscriber` | Keeps the last N and tells a slow reader what it lost, with an error named `SubEthaLagged`. `Subscribe()` and `SubscribeFrom($position)` hand out readers. |
| `New-SubEthaMpscPool` | `SubEtha.MpscPool` | Many writers, one reader: the `Producers` and the `Consumer` in one object. |
| `New-SubEthaMpmcGrid` | `SubEtha.MpmcGrid` | Many of each: the `Producers` and the `Consumers`. |
| `New-SubEthaLamportPair` | `SubEtha.LamportProducer`, `SubEtha.LamportConsumer` | The Lamport pair, written as two objects in that order: `$p, $c = New-SubEthaLamportPair ...`. |

The constructors hand out every end together because the shape is what
makes them correct; `-Open` on the pool, the grid and the pair attaches
to one that exists.

## Rings that change themselves

| Cmdlet | Object | What it is |
|---|---|---|
| `New-SubEthaCapacityRing` | `SubEtha.CapacityRing` | Resizes with `MorphTo` without losing what is in flight: a reader keeps reading the superseded backing until it has drained. `Prewarm`, `WarmCapacity`, `WarmHits` and `StalePops` report the backing held ready, the morphs that took it, and the reads served from a superseded one. |
| `New-SubEthaLocaleRing` | `SubEtha.LocaleRing` | Moves between `Anon`, `File` and `ShmFs` with `MigrateTo` without its senders and readers reconnecting, carrying what it holds across the move. |

Both take `-Stamped`, and report `OrderingMode()` as a
`SubEtha.OrderingMode`: `Unordered`, `MergeByStamp` or `MergeStrict`.

## Order

| Object | What it is |
|---|---|
| `SubEtha.OrderedReceiver` | Delivers a ring's items in the order their senders made them, as `SubEtha.StampedItem` records. Built by `$ring.OrderedReceiver($consumer)`, which refuses a ring with no stamps rather than returning nothing forever. It picks its own strategy and `Strategy()` says which. |
| `SubEtha.ReorderWindow` | The same window over items from anywhere else, from `New-SubEthaReorderWindow`: push with a stamp, take the smallest first, and the window widens itself when something arrives further out of order than it covered. |

`OrderedReceiver.Recv` answers `$null` for two different reasons, the
window still filling and the ring being empty, so `Drain` is the shape
to reach for: it takes the ring and the held-back tail in one call.

## Shared state

`Atomic` (`Load`, `Store`, `FetchAdd`, `FetchSub`, the bitwise
operations, `Swap` and `CompareExchange`, each taking a
`SubEtha.MemoryOrder`), `Cell`, `Vec`, `Slab`, `HashMap`, `BTreeMap`,
`LinkedList`, `Deque`, `Stack`, `Arena`, `Region` and `FrameRegion`,
each from its `New-SubEtha...` and `Open-SubEtha...` pair.

`Region.Snapshot()` copies every slot into one `byte[]` in one pass. The
seqlocked families (`Vec`, `Slab`) have `ReadRange` and `WriteRange`
instead, which move a run of elements packed end to end while honoring
the retry on every one.

## State with a history

| Cmdlet | Object | What it is |
|---|---|---|
| `New-SubEthaVersionChain` | `SubEtha.VersionChain` | One value's versions. `ReadAt($version)` gets the value as it stood then. |
| `New-SubEthaVersionedSlab` | `SubEtha.VersionedSlab`, `SubEtha.SlabPin` | Numbered slots each keeping their recent history, read through a pin from `Pin()` that fixes one epoch for the whole slab. `History` reports each version with the epochs it was current between. |
| `New-SubEthaVersionedMap` | `SubEtha.VersionedMap`, `SubEtha.MapPin` | An ordered index from one unsigned integer to another, scanned through a pin that sees one unchanging view while writers carry on. `Scan` answers `SubEtha.Entry` records; `ScanFrom` also says where to carry on. |
| `New-SubEthaLanedMap` | `SubEtha.LanedMap`, `SubEtha.LaneClaim`, `SubEtha.LanedPin` | That map split across several trees so several writers work at once. A writer claims a lane with `ClaimLane()` or `ClaimLaneFor($key)`; reading takes none, and a scan merges every lane in key order. |
| `New-SubEthaEpochs` | `SubEtha.Epochs` | The epoch table the above share. |

Two names here mean what the Rust says rather than what they sound
like. `VoidEpoch` is not reclamation: it undoes the writes stamped at
exactly one epoch and makes what they superseded current again, for a
writer that died partway through. `Sweep` and `SweepSlot` are the ones
that reclaim, and a live pin holds the horizon still, so a sweep during
a scan frees only what was already unreachable when the scan began.

A key belongs to one lane of a `LanedMap` for its whole life, because a
lane is a separate tree. Writing it through another is an error named
`SubEthaWrongLane` naming the right one, rather than a report of a row
gone that no reader has stopped seeing.

## Coordination

| Cmdlet | Object | What it is |
|---|---|---|
| `New-SubEthaRWLock` | `SubEtha.RWLock`, `SubEtha.Hold` | Read and write holds as objects rather than tokens. `ReadFor` and `WriteFor` give up after a timeout in seconds and answer `$null`; `TryRead` and `TryWrite` answer `$null` at once. |
| `New-SubEthaSemaphore` | `SubEtha.Semaphore`, `SubEtha.PermitHold` | A counting semaphore across processes. `AcquireFor` gives up after a timeout. |
| `New-SubEthaCondvar`, `Wait-SubEthaCondition` | `SubEtha.Condvar` | Whose condition, a script block, really is run from inside the wait. A script block runs only on the pipeline thread, so the wait is a cmdlet rather than a method: `Wait-SubEthaCondition -Path $path -Until { $ready } -Timeout 5`. |
| `New-SubEthaLazyValue` | `SubEtha.LazyValue` | Computed once across every process: `Claim`, `Publish`, `Wait`. |
| `New-SubEthaOwnerLease` | `SubEtha.OwnerLease`, `SubEtha.LeaseHold` | One process owns a small value and another takes it over when that one dies. |
| `New-SubEthaHeartbeat`, `New-SubEthaEpochBarrier`, `New-SubEthaLeaderElection`, `New-SubEthaHolderTable`, `New-SubEthaFenceClock`, `New-SubEthaSharedArc`, `New-SubEthaNotifierSet` | `SubEtha.Heartbeat`, `SubEtha.EpochBarrier`, `SubEtha.LeaderElection`, `SubEtha.HolderTable`, `SubEtha.FenceClock`, `SubEtha.SharedArc`, `SubEtha.NotifierSet`, `SubEtha.Notifier` | The rest of the coordination surface. A barrier comes from a heartbeat's `Barrier($path)` or from the cmdlet, which opens the heartbeat table itself. |

The waiting forms that take a timeout sleep rather than spin, so a long
wait costs no processor, and they never wait past the deadline even if
whoever holds the lock never gives it back.

A lease changes hands two ways and a caller has to know them apart. A
process whose id is lower than the owner's takes it on the spot, beaten
or not, which is what settles who leads when several processes start
together. A process whose id is higher waits out the grace period,
counted in epochs the holders step themselves with `TickEpoch`; nothing
steps it on its own. Every method that names a process takes a `pid`
argument that defaults to this process; pass another only to stand in
for a process that is not here.

## Probabilistic

`BloomFilter`, `BlockedBloomFilter`, `CountMinSketch`, `HyperLogLog`,
`Histogram`, `RateLimiter`, `BitVec` and `Reservoir`.

`Measure-SubEthaBloomSize -Items 10000 -FalsePositiveRate 0.01` works the
bits and hash count out from the item count and the rate of wrong yeses
that can be lived with, for either filter (`-Blocked` for the one whose
bits for one item share a cache line), and
`Measure-SubEthaSketchSize -Epsilon 0.01 -Delta 0.01` does the same for
the sketch. `Reservoir` keeps a bounded unbiased sample of a stream of
any length; a refused value there is how the sample stays unbiased.

`BitVec.Toggle` answers the new value, while `Set` and `Clear` answer
the previous one.

## Specialist

| Object | What it is |
|---|---|
| `SubEtha.HandleTable` | Values reached by a handle carrying both the place and how often it has been reused, so a handle to a removed value does not follow the place to its new occupant. |
| `SubEtha.TimePointTile` | Sixteen values each stamped with the version it was written at; `Visible($version)` answers what a reader at that version sees. |
| `SubEtha.Tower` | Values reached by a path of `uint` down through levels, where each level checks that it still points at the next number in the path and refuses at the first that does not. |
| `SubEtha.Graph` | Nodes and the directed edges between them, in mapped files; `Neighbors` answers `SubEtha.Neighbor` records. |
| `SubEtha.TopologyMap` | Counts who sends to whom and reads the shape off those counts as a `SubEtha.Topology`: `PointToPoint`, `BroadcastTree` or `AllToAllMesh`. Reading a recommendation does not publish it. |
| `SubEtha.Universal` | A set stored as a list while that is quicker to walk and moved to a map when it is not; `Strategy()` says which. |

## Reaching another machine

`New-SubEthaSensSender` and `New-SubEthaSensReceiver` are the two ends
of a link that keeps working as the network gets worse. The link sends
more than the items, so a reader rebuilds what was lost without asking
again, and it changes between the sliding code and the block code as
the measured loss moves, without either end reconnecting.

```powershell
$reader = New-SubEthaSensReceiver -LocalPort 9000 -MaxItemSize 1024
$writer = New-SubEthaSensSender -PeerHost 10.0.0.5 -PeerPort 9000 -MaxItemSize 1024
$writer.SendMany(@('one', 'two'))

# Items do not arrive one at a time. Poll answers whatever the link
# could rebuild this time round, which may be nothing.
foreach ($item in $reader.Poll()) { ... }
```

Two things a reader has to know. Under the block code items are grouped
in eights and none of a group is delivered until enough of it has
arrived, so a run shorter than a group waits. And `SensSender.Loss()` is
`$null` until the reader has reported anything, which is not the same
as zero.

## The bridges

`New-SubEthaTcpBridgeClient` with `New-SubEthaTcpBridgeServer`, and
`New-SubEthaQuicBridgeClient` with `New-SubEthaQuicBridgeServer`, carry
a whole ring to another host. Each takes the ring's path and geometry
and opens its own handle on it, so the ring object a script holds and
the bridge are two handles on one file. `Run($items)` on a client and
`AcceptOne()` on a server each block until the other end finishes.

A PowerShell module ships as one artifact with no way to ask for an
extra, so both bridges are always built in; `Get-SubEthaTransport`
lists `sens`, `tcp` and `quic`.

A QUIC certificate comes from `New-SubEthaSelfSignedCert -Name $name`
as a `SubEtha.Certificate` holding the certificate and its key as
`byte[]`: the reading end holds both and proves itself with them, the
sending end holds the certificate alone and checks the reading end
against it. Carrying it between hosts is the caller's to arrange.

## Sensing

Eight objects that are fed measurements and answer what they worked out
from them. They hold no shared memory and touch no network: they are
the arithmetic a transport does on its own numbers, which is why they
are worth having whether or not a SubEtha link produced the numbers.
Each comes from its `New-SubEtha...` cmdlet.

| Object | What it answers |
|---|---|
| `SubEtha.LossKind` | Whether a loss came from noise or from a queue overflowing, as a `SubEtha.LossClass`. The two want opposite responses, so reading one as the other is expensive. |
| `SubEtha.LossBursts` | Whether losses arrive alone or in runs, and how long a run lasts. |
| `SubEtha.Timing` | Jitter, spacing, and whether delay is climbing. `TrendDebiased` takes a constant difference between two clocks out. |
| `SubEtha.RoundTripShape` | Whether round trips fall into two groups, which is what a radio retry looks like from outside. |
| `SubEtha.Periodicity` | Whether interference arrives on a beat, and when the next spike is due. |
| `SubEtha.Capacity` | The narrowest link on the path and what is free on it, from probes sent in pairs and in trains. |
| `SubEtha.Forecast` | What the next interval is likely to carry. |
| `SubEtha.PathChanges` | Whether the route moved under the traffic, which otherwise reads as congestion. |

Every one answers `$null` rather than a number until it has seen enough
to say anything.

## Values that ride beside a pointer

| Object | What it is |
|---|---|
| `SubEtha.TinyBloom` | A whole bloom filter in one machine word; its `Bits` property is the whole state, and `New-SubEthaTinyBloom -Bits $n` rebuilds it. About eight keys. |
| `SubEtha.FineBloom` | The same in four words, for about sixty-four keys. |
| `SubEtha.Clock` | Wall-clock time that still orders two events sharing a reading. `Merge` is what a receiver does with a sender's clock so the received event orders after its cause. A clock never changes; every operation answers a new one. |
| `SubEtha.CausalClock` | One count per participant, answering before, after, equal, or concurrent. |

The pointer types themselves are not here. Each holds a raw pointer or a
reference count into Rust memory, and a script has no such value to
point at.

## Every cmdlet

Every cmdlet the module exports, with what it obtains. Each also answers
to a short name with the `SE` prefix, so `New-SubEthaRing` is
`New-SERing`. `Get-Help <name> -Full` gives the parameters and an
example for any of them.

| Cmdlet | Obtains |
|---|---|
| `New-SubEthaAdaptiveQueue` | `SubEtha.AdaptiveQueue` |
| `New-SubEthaArena`, `Open-SubEthaArena` | `SubEtha.Arena` |
| `New-SubEthaAtomic`, `Open-SubEthaAtomic` | `SubEtha.Atomic` |
| `New-SubEthaBitVec`, `Open-SubEthaBitVec` | `SubEtha.BitVec` |
| `New-SubEthaBlockedBloomFilter`, `Open-SubEthaBlockedBloomFilter` | `SubEtha.BlockedBloomFilter` |
| `New-SubEthaBloomFilter`, `Open-SubEthaBloomFilter` | `SubEtha.BloomFilter` |
| `New-SubEthaBroadcastRing`, `Open-SubEthaBroadcastRing` | `SubEtha.BroadcastRing` |
| `New-SubEthaBTreeMap`, `Open-SubEthaBTreeMap` | `SubEtha.BTreeMap` |
| `New-SubEthaCapacity` | `SubEtha.Capacity` |
| `New-SubEthaCapacityRing`, `Open-SubEthaCapacityRing` | `SubEtha.CapacityRing` |
| `New-SubEthaCausalClock` | `SubEtha.CausalClock` |
| `New-SubEthaCell`, `Open-SubEthaCell` | `SubEtha.Cell` |
| `New-SubEthaChannel`, `Open-SubEthaChannel` | `SubEtha.Channel` |
| `New-SubEthaClock` | `SubEtha.Clock` |
| `New-SubEthaCondvar`, `Open-SubEthaCondvar` | `SubEtha.Condvar` |
| `New-SubEthaCountMinSketch`, `Open-SubEthaCountMinSketch` | `SubEtha.CountMinSketch` |
| `New-SubEthaDeque`, `Open-SubEthaDeque` | `SubEtha.Deque` |
| `New-SubEthaEpochBarrier`, `Open-SubEthaEpochBarrier` | `SubEtha.EpochBarrier` |
| `New-SubEthaEpochs`, `Open-SubEthaEpochs` | `SubEtha.Epochs` |
| `New-SubEthaFenceClock`, `Open-SubEthaFenceClock` | `SubEtha.FenceClock` |
| `New-SubEthaFineBloom` | `SubEtha.FineBloom` |
| `New-SubEthaForecast` | `SubEtha.Forecast` |
| `New-SubEthaFrameRegion`, `Open-SubEthaFrameRegion` | `SubEtha.FrameRegion` |
| `Get-SubEthaTransport` | `System.String` |
| `New-SubEthaGraph`, `Open-SubEthaGraph` | `SubEtha.Graph` |
| `New-SubEthaHandleTable`, `Open-SubEthaHandleTable` | `SubEtha.HandleTable` |
| `New-SubEthaHashMap`, `Open-SubEthaHashMap` | `SubEtha.HashMap` |
| `New-SubEthaHeartbeat`, `Open-SubEthaHeartbeat` | `SubEtha.Heartbeat` |
| `New-SubEthaHistogram`, `Open-SubEthaHistogram` | `SubEtha.Histogram` |
| `New-SubEthaHolderTable`, `Open-SubEthaHolderTable` | `SubEtha.HolderTable` |
| `New-SubEthaHyperLogLog`, `Open-SubEthaHyperLogLog` | `SubEtha.HyperLogLog` |
| `New-SubEthaKvMap` | `SubEtha.KvMap` |
| `New-SubEthaLamportPair` | `SubEtha.LamportConsumer`, `SubEtha.LamportProducer` |
| `New-SubEthaLanedMap`, `Open-SubEthaLanedMap` | `SubEtha.LanedMap` |
| `New-SubEthaLazyValue`, `Open-SubEthaLazyValue` | `SubEtha.LazyValue` |
| `New-SubEthaLeaderElection`, `Open-SubEthaLeaderElection` | `SubEtha.LeaderElection` |
| `New-SubEthaLinkedList`, `Open-SubEthaLinkedList` | `SubEtha.LinkedList` |
| `New-SubEthaLocaleRing`, `Open-SubEthaLocaleRing` | `SubEtha.LocaleRing` |
| `New-SubEthaLossBursts` | `SubEtha.LossBursts` |
| `New-SubEthaLossKind` | `SubEtha.LossKind` |
| `New-SubEthaLruCache`, `Open-SubEthaLruCache` | `SubEtha.LruCache` |
| `Measure-SubEthaBloomSize` | `SubEtha.BloomSize` |
| `Measure-SubEthaSketchSize` | `SubEtha.SketchSize` |
| `New-SubEthaMpmcGrid` | `SubEtha.MpmcGrid` |
| `New-SubEthaMpscPool` | `SubEtha.MpscPool` |
| `New-SubEthaNotifierSet` | `SubEtha.NotifierSet` |
| `New-SubEthaOwnerLease`, `Open-SubEthaOwnerLease` | `SubEtha.OwnerLease` |
| `New-SubEthaPathChanges` | `SubEtha.PathChanges` |
| `New-SubEthaPeriodicity` | `SubEtha.Periodicity` |
| `New-SubEthaPubSub`, `Open-SubEthaPubSub` | `SubEtha.PubSub` |
| `New-SubEthaQosPolicy` | `SubEtha.QosPolicy` |
| `New-SubEthaQuicBridgeClient` | `SubEtha.QuicBridgeClient` |
| `New-SubEthaQuicBridgeServer` | `SubEtha.QuicBridgeServer` |
| `New-SubEthaRateLimiter`, `Open-SubEthaRateLimiter` | `SubEtha.RateLimiter` |
| `Receive-SubEthaItem` | `System.Byte[]` |
| `New-SubEthaRegion`, `Open-SubEthaRegion` | `SubEtha.Region` |
| `New-SubEthaReorderWindow` | `SubEtha.ReorderWindow` |
| `New-SubEthaReservoir`, `Open-SubEthaReservoir` | `SubEtha.Reservoir` |
| `New-SubEthaRing`, `Open-SubEthaRing` | `SubEtha.Ring` |
| `New-SubEthaRoundTripShape` | `SubEtha.RoundTripShape` |
| `New-SubEthaRWLock`, `Open-SubEthaRWLock` | `SubEtha.RWLock` |
| `New-SubEthaSelfSignedCert` | `SubEtha.Certificate` |
| `New-SubEthaSemaphore`, `Open-SubEthaSemaphore` | `SubEtha.Semaphore` |
| `Send-SubEthaItem` |  |
| `New-SubEthaSensReceiver` | `SubEtha.SensReceiver` |
| `New-SubEthaSensSender` | `SubEtha.SensSender` |
| `New-SubEthaSharedArc`, `Open-SubEthaSharedArc` | `SubEtha.SharedArc` |
| `New-SubEthaSlab`, `Open-SubEthaSlab` | `SubEtha.Slab` |
| `New-SubEthaSpscRing`, `Open-SubEthaSpscRing` | `SubEtha.SpscRing` |
| `New-SubEthaStack`, `Open-SubEthaStack` | `SubEtha.Stack` |
| `New-SubEthaTcpBridgeClient` | `SubEtha.TcpBridgeClient` |
| `New-SubEthaTcpBridgeServer` | `SubEtha.TcpBridgeServer` |
| `New-SubEthaTimePointTile`, `Open-SubEthaTimePointTile` | `SubEtha.TimePointTile` |
| `New-SubEthaTiming` | `SubEtha.Timing` |
| `New-SubEthaTinyBloom` | `SubEtha.TinyBloom` |
| `New-SubEthaTopologyMap`, `Open-SubEthaTopologyMap` | `SubEtha.TopologyMap` |
| `New-SubEthaTower`, `Open-SubEthaTower` | `SubEtha.Tower` |
| `New-SubEthaUniversal`, `Open-SubEthaUniversal` | `SubEtha.Universal` |
| `New-SubEthaVec`, `Open-SubEthaVec` | `SubEtha.Vec` |
| `New-SubEthaVersionChain`, `Open-SubEthaVersionChain` | `SubEtha.VersionChain` |
| `New-SubEthaVersionedMap`, `Open-SubEthaVersionedMap` | `SubEtha.VersionedMap` |
| `New-SubEthaVersionedSlab`, `Open-SubEthaVersionedSlab` | `SubEtha.VersionedSlab` |
| `Wait-SubEthaCondition` | `System.Boolean` |
| `New-SubEthaWorkQueue`, `Open-SubEthaWorkQueue` | `SubEtha.WorkQueue` |

## Threads and lifetimes

Every method call on one object is serialized by the object, so an
object handed to another runspace or thread is used safely, and the
bridges' tests run a server's `AcceptOne` in a second runspace beside
the client's `Run`. A hold, a pin or a claim can be released from any
thread, including the finalizer's; every value the module holds is safe
to send between threads, and the build refuses one that is not.

An `OrderedReceiver`, a `SlabPin`, a `MapPin`, a `LanedPin` or a
`LaneClaim` keeps the structure it came from alive for as long as it
lives, so disposing the structure first is safe: the mapping goes when
the last of them does. Inside the binding the borrowed guard is declared
before the handle that keeps its target alive, because Rust drops fields
in declaration order.

## Building and testing

The binding is written against a framework whose attributes turn a Rust
struct into a cmdlet or a class, and whose build tool generates the
managed shell around the library for both hosts:

```powershell
cd crates/subetha-pwrs
cargo pwrs build --release          # target/pwrs/SubEtha/
cargo pwrs test --release           # cargo test, the build, then Pester in pwsh and Windows PowerShell
```

The Pester suites in `tests/` cover every family, in both hosts. They
reach every cmdlet, class, enum and method the module exports, and a
gate suite fails the run when one of those names is exercised nowhere,
when a cmdlet has no `SE` alias resolving to it, or when a synopsis is
not a whole sentence. Others attach a second time through every `Open-`
and reset form, drive a second process of the same host over shared
structures, and run several runspaces at once against one object and
against handles of their own.

Only the native library under `runtimes/<rid>/native/` differs per
platform, so each is built on its own machine and
`cargo pwrs merge target/pwrs/SubEtha <folder built elsewhere>` folds
them into one module. Every machine needs the same `cargo pwrs` version
as well as the same checkout, because the manifest gained fields between
releases of the tool and `merge` refuses two that differ.
`cargo install --list` on each host settles which version it has; the
binary's file date does not, because the newest file can be the oldest
version.

Built from one commit on all four platforms, the manifest, the format
file, the help and the Windows PowerShell shell come out byte-identical.
The PowerShell 7 shell differs with the PowerShell that builds it, which
also sets the oldest PowerShell 7 it loads in
([how the binding works](../../explanation/powershell-binding/#one-folder-two-hosts-four-platforms)).
The published folder takes the FreeBSD build, on PowerShell 7.5.5, as
its base.

All 182 tests pass on each of four platforms, each running the folder
it built:

| Platform | Host | Native |
|---|---|---|
| Windows x64 | pwsh 7.6.6 and Windows PowerShell 5.1 | `win-x64/subetha_pwrs.dll` |
| Linux x64 | pwsh 7.6.5 | `linux-x64/libsubetha_pwrs.so` |
| macOS arm64 | pwsh 7.6.5 | `osx-arm64/libsubetha_pwrs.dylib` |
| FreeBSD x64 | pwsh 7.5.5 on .NET 9 | `freebsd-x64/libsubetha_pwrs.so` |

The FreeBSD-based folder, with the Windows and Linux natives folded in,
passes the same 182 in pwsh 7.6.6 and Windows PowerShell 5.1 on Windows,
in pwsh 7.6.5 and 7.5.11 on Linux, and in pwsh 7.5.5 on FreeBSD.
PowerShell 7.4.20 refuses it at import.

FreeBSD is the one that needs arranging, for two reasons that are not
this module's. Building it needs `PWRS_TOOLSET=5.3.0`, because the C#
compiler the tool fetches by default wants .NET 10 and FreeBSD packages
nothing past 9. Running the suite needs Pester's platform check
answered: Pester decides the platform from three booleans that are all
false there and throws rather than guessing. It reads them with
`Get-Variable`, which resolves through the scope chain, so a global
`$IsLinux` answers it without modifying Pester. `cargo pwrs test`
imports whatever `PWRS_PESTER_PATH` names in place of Pester, so a
module that sets the global and then imports Pester lets the command run
unchanged:

```powershell
# PesterIsLinuxShim.psm1
Set-Variable -Name IsLinux -Value $true -Scope Global -Force
Import-Module Pester -RequiredVersion 5.7.1 -Global -ErrorAction Stop
```

```sh
export PWRS_TOOLSET=5.3.0
export PWRS_PESTER_PATH="$HOME/PesterIsLinuxShim.psm1"
cargo pwrs test --release
```
