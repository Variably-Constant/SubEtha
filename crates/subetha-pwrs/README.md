# SubEtha for PowerShell

Shared memory between processes: rings, channels and shared state, bound
to Rust directly, as a PowerShell module.

```powershell
Import-Module SubEtha

$hits = New-SubEthaAtomic -Path C:\ipc\hits -Init 0
$null = $hits.FetchAdd(1)
$hits.Load()

# A ring another process is reading, filled in one call rather than one
# per item.
$ring = New-SubEthaRing -Path C:\ipc\events -Capacity 4096
$producer = $ring.RegisterProducer()
$ring.SendMany($producer, @('one', 'two', 'three'))

# Items through the pipeline.
Get-Content .\lines.txt | Send-SubEthaItem -To $ring -Producer $producer
Receive-SubEthaItem -From $ring -Consumer $consumer

# A lock held across processes, given back when the hold is released,
# disposed or collected.
$lock = New-SubEthaRWLock -Path C:\ipc\lock
$hold = $lock.Write()
try { ... } finally { $hold.Release() }
```

Every cmdlet also answers to a shorter name with the `SE` prefix:
`New-SEAtomic`, `Open-SERing`, `Send-SEItem`.

## What this module is for

A process here shares memory with another process, which may be written
in Rust, C, Python, or PowerShell. The mapping is the same bytes in every
one of them.

## What it costs, and what follows from that

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

The same atomic load costs the Rust about 7 ns through the C ABI, so
what a method call costs is what the shell's own method invocation
costs, and it differs between the two hosts more than anything in the
library does. Three shapes cross less often, and the surface is built
around them:

- **A batch call** carries many operations over one call. `FetchAddMany`,
  `SendMany`, `RecvMany`, `InsertMany`, `GetMany`, `Drain` and their kin
  are all this shape.
- **A packed buffer** carries many items in one `byte[]` with no object
  per item. `PushPacked`, `SendPacked` and `PopPacked` on the rings, and
  `ReadRange` and `WriteRange` on the vector and the slab.
- **The pipeline** is for a script that reads as a pipeline; per record
  it costs about what a method call does in PowerShell 7 and several
  times that in Windows PowerShell.

`bench/CallShapes.ps1` in the crate reproduces every row above on the
reader's own machine, in whichever host runs it.

## The two layers

**Cmdlets obtain a structure.** `New-` creates the file when it does not
exist and attaches to it when it does; `Open-` attaches to one that must
already exist. Every path is resolved against the session's current
location, the way every other cmdlet resolves one.

**Objects operate on it.** What a cmdlet writes is an object of a
`SubEtha.*` type whose methods are the operations the Rust type offers,
one method per operation: `$ring.Send($producer, $bytes)`,
`$map.Insert(7, 70)`, `$lock.Write()`. A method call is one native call
into the library, with no pipeline in between.

**The pipeline moves items.** `Send-SubEthaItem` sends whatever is piped
in to any structure that moves items and hands back what the structure
refused; `Receive-SubEthaItem` reads a structure out into the pipeline.

## What crosses

Bytes cross as `byte[]`. A method argument is pinned where it lies and
copied nowhere; a string is taken as its UTF-8; any other array of
numbers is converted. A result is one `byte[]` filled through one pin.

The families that hold values of a declared size take and return those
values as `byte[]` of that size. The families that hold unsigned
integers (`KvMap`, `VersionedMap`, `LanedMap`, `Graph`, `Universal`)
take and return `ulong`.

## The conventions

- **A refusal is an answer, not a fault.** A push that does not fit
  returns `$false`; a pop with nothing to take returns `$null`; a sweep
  that freed nothing returns `0`. Only a genuine fault is an error.
- **An error from a method is an exception; from a cmdlet, an error
  record.** Every error carries an id starting `SubEtha`: `SubEthaOpen`
  when a structure could not be obtained, `SubEthaArgument` for an
  argument that cannot be right, `SubEthaOperation` for a failure
  underneath, `SubEthaLagged` when a subscriber fell far enough behind
  that what it asked for was overwritten, `SubEthaContended` when a
  lease or a lane is held by somebody else, `SubEthaWrongLane` when a
  key belongs to another lane, and `SubEthaReleased` for a pin or hold
  already given back.
- **Anything holding a resource is disposable.** A hold, a permit, a
  pin or a lane claim gives its resource back when `Release()` is
  called, when the object is disposed, or when the garbage collector
  finalizes it. Every object frees its mapping the same way.
- **A value that is not measured yet is `$null`**, which is a different
  answer from zero.
- **Many-item forms everywhere.** `SendMany`, `RecvMany`, `PushPacked`,
  `PopPacked`, `InsertMany`, `GetMany` and `Drain` carry a run of items
  across one call.

## What is here

**The front door.** `Channel`, a queue between processes that can be
waited on. `WorkQueue`, work one process owns and others steal from.
`KvMap`. `AdaptiveQueue`, which picks its shape from the traffic it sees
rather than from what was declared. `QosPolicy`, what a stream needs
written down.

**Rings and channels.** `Ring`, the adaptive ring, with producer and
consumer registration, framing for payloads larger than a slot, and a
shape that changes under the traffic. `SpscRing`, `BroadcastRing`,
`PubSub` with `Subscriber`, `LamportProducer` and `LamportConsumer`
from `New-SubEthaLamportPair`, and the pools from `New-SubEthaMpscPool`
and `New-SubEthaMpmcGrid`, which hand out every end together because
the shape is what makes them correct.

**Rings that change themselves.** `CapacityRing` resizes without losing
what is in flight. `LocaleRing` moves between process-private memory, a
mapped file and named memory without its senders and readers
reconnecting.

**Order.** A `Ring` built with `-Stamps` marks each item with the order
its sender made it in; `OrderedReceiver` delivers by those marks, and
`ReorderWindow` is the same window over items from anywhere else.

**Shared state.** `Atomic`, `Cell`, `Vec`, `Slab`, `HashMap`,
`BTreeMap`, `LinkedList`, `Deque`, `Stack`, `Arena`, `Region` and
`FrameRegion`.

**State with a history.** `VersionChain`, `VersionedSlab` with
`SlabPin`, `VersionedMap` with `MapPin`, `LanedMap` with `LaneClaim` and
`LanedPin`, and `Epochs`, the table they share.

**Coordination.** `RWLock` and `Semaphore`, whose holds are objects
rather than tokens. `Condvar`, whose condition is a script block run
from inside the wait by `Wait-SubEthaCondition`. `LazyValue`,
`OwnerLease`, `Heartbeat`, `EpochBarrier`, `LeaderElection`,
`HolderTable`, `FenceClock`, `SharedArc` and `NotifierSet`.

**Probabilistic.** `BloomFilter`, `BlockedBloomFilter`,
`CountMinSketch`, `HyperLogLog`, `Histogram`, `RateLimiter`, `BitVec`
and `Reservoir`, with `Measure-SubEthaBloomSize` and
`Measure-SubEthaSketchSize` to size the first three from what they
mean.

**Specialist.** `HandleTable`, `TimePointTile`, `Tower`, `Graph`,
`TopologyMap` and `Universal`.

**Reaching another machine.** `SensSender` and `SensReceiver`, the two
ends of a link that keeps working as the network gets worse, and the
TCP and QUIC bridges that carry a whole ring to another host, with
`New-SubEthaSelfSignedCert` for the QUIC certificate. The module always
carries all three; `Get-SubEthaTransport` lists them.

**Sensing.** `LossKind`, `LossBursts`, `Timing`, `RoundTripShape`,
`Periodicity`, `Capacity`, `Forecast` and `PathChanges`.

**Values that ride beside a pointer.** `TinyBloom`, `FineBloom`,
`Clock` and `CausalClock`.

## Building it

The module is built with `cargo pwrs`, the build tool of the framework
the binding is written against, which compiles the Rust library and
generates the managed shell around it for both hosts:

```powershell
cd crates/subetha-pwrs
cargo pwrs build --release
Import-Module ../../target/pwrs/SubEtha/SubEtha.psd1
```

`cargo pwrs test --release` runs the Pester suites in `tests/` in pwsh
and, on Windows, in Windows PowerShell as well. The module runs on
PowerShell 7 on .NET 10 and on Windows PowerShell 5.1.

`Get-Help` carries a synopsis, a description, help on every parameter
and one example for each of the 135 cmdlets, all generated from the
Rust doc comments.

The suites cover every cmdlet, class, enum and method the module
exports, and one of them is a gate: it fails the run when an exported
name is exercised nowhere, when a cmdlet has no `SE` alias resolving to
it, or when a synopsis is not a whole sentence. Beside the per-family
suites, one attaches to a structure a second time through every `Open-`
and reset form, one drives a second process of the same host over a
shared counter, ring, channel, lock, semaphore, lease and pubsub, and
one runs several runspaces at once against a single object and against
handles of their own.

## Platforms

Only the native library under `runtimes/<rid>/native/` differs per
platform, so each one is built on its own machine and the folders are
folded into a single module:

```powershell
cargo pwrs build --release                       # the building machine's rid
cargo pwrs merge target/pwrs/SubEtha ../linux/SubEtha
```

The manifests of two builds of one checkout are identical, which is
what `merge` requires. All 178 Pester tests pass in pwsh 7.6.6 and in
Windows PowerShell 5.1 on Windows x64, and in pwsh 7.6.5 on Ubuntu
24.04 on Linux x64. A merged folder carrying `win-x64` and `linux-x64`
imports and runs on both.

## Threads and lifetimes

Every method call on one object is serialized by the object, so an
object handed to another runspace or thread is used safely. A pin, a
hold or a claim can be released from any thread, including the
finalizer's; every value the module holds is safe to send between
threads, and the build refuses one that is not.

An `OrderedReceiver`, a `SlabPin`, a `MapPin`, a `LanedPin` or a
`LaneClaim` keeps the structure it came from alive for as long as it
lives, so disposing the structure first is safe: the mapping goes when
the last of them does.
