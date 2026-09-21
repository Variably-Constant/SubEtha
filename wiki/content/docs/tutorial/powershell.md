---
title: SubEtha from PowerShell
weight: 46
---

# SubEtha from PowerShell

Two PowerShell sessions share the same bytes, with no copy between them
and no C shim in the middle. This walks from building the module to a
message crossing between two processes, then to the shapes that cost
less than one call an item.

The full surface is in the
[`subetha-pwrs` reference](../../reference/subetha-pwrs/).

## Build and import

The module is built from the repository with `cargo pwrs`, which
compiles the Rust library and generates the managed shell for both
hosts:

```powershell
cd crates/subetha-pwrs
cargo pwrs build --release
Import-Module ../../target/pwrs/SubEtha/SubEtha.psd1
```

One module folder serves PowerShell 7 on .NET 10 and Windows
PowerShell 5.1. Every cmdlet has a long name and a short one: this page
uses the long ones, and `New-SEAtomic` is `New-SubEthaAtomic`.

## A counter two processes share

```powershell
$hits = New-SubEthaAtomic -Path C:\ipc\hits -Init 0
$null = $hits.FetchAdd(1)
$hits.Load()
```

Run it in two sessions and the second prints `2`. The file is the
state; both processes map it and the kernel gives them the same page.
Nothing is serialized and nothing is sent.

`Atomic` takes a memory ordering on every operation, defaulting to
sequentially consistent:

```powershell
$null = $hits.FetchAdd(1, [SubEtha.MemoryOrder]::Relaxed)
$null = $hits.FetchAdd(1, 'Relaxed')
```

## A message from one process to another

The sending process:

```powershell
$ring = New-SubEthaRing -Path C:\ipc\events -Capacity 4096
$producer = $ring.RegisterProducer()
$ring.Send($producer, 'the first message')
```

The reading process:

```powershell
$ring = Open-SubEthaRing -Path C:\ipc\events -Capacity 4096
$consumer = $ring.RegisterConsumer()

while ($null -ne ($item = $ring.Recv($consumer))) {
    [Text.Encoding]::UTF8.GetString($item)
}
```

A string sent in arrives as its UTF-8 bytes; a `byte[]` sent in arrives
as it was. `Recv` answering `$null` means the ring is empty, which is
an ordinary answer rather than a failure. So is `Send` answering
`$false`, which means the ring is full.

A payload larger than a slot goes through `SendFrame` and comes back
through `RecvFrame`, which reassemble it.

## The same through the pipeline

```powershell
Get-Content .\lines.txt | Send-SubEthaItem -To $ring -Producer $producer
Receive-SubEthaItem -From $ring -Consumer $consumer | ForEach-Object { [Text.Encoding]::UTF8.GetString($_) }
```

`Send-SubEthaItem` writes back whatever the ring refused, so a full
ring hands the line back rather than dropping it. The two cmdlets serve
every structure that moves items: a channel, a stack, a subscriber, a
work queue.

## Hold a lock across processes

```powershell
$lock = New-SubEthaRWLock -Path C:\ipc\lock
$hold = $lock.Write()
try {
    ...          # nothing else holds the write side while this runs
} finally {
    $hold.Release()
}
```

The hold is an object rather than a token to remember to return. It
gives the lock back on `Release()`, on `Dispose()`, and when the garbage
collector finalizes it, so a script that leaves the block early does
not strand the lock. `WriteFor(2)` gives up after two seconds and
answers `$null` instead.

`Semaphore` works the same way with `Acquire`, and `OwnerLease` with
`Hold`.

## The shapes that cost less

A method call on an object costs about two microseconds in
PowerShell 7 and about two thirds of one in Windows PowerShell, and a
record through the pipeline costs about the same as a call in
PowerShell 7 and several times that in Windows PowerShell; the Rust
underneath costs seven nanoseconds. Two shapes avoid paying the call
per item.

**Carry many operations over one call.** Every family has a batched
form:

```powershell
$ring.SendMany($producer, @('one', 'two', 'three'))
$items = $ring.RecvMany($consumer, 256)
```

**Cross once with everything packed.** The rings take and answer items
packed end to end in one `byte[]`, so no object is built per item:

```powershell
$ring.SendPacked($producer, $buffer, 64)      # every 64 bytes of $buffer is one item

$spsc = New-SubEthaSpscRing -Path C:\ipc\packed -Capacity 4096
$spsc.PushPacked($buffer, 64)
$packed = $spsc.PopPacked(1000)               # $packed.Count items in $packed.Bytes
```

The adaptive ring packs on the way in, through `SendPacked`. The
single-writer ring packs in both directions, which is why the reading
half above is an `SpscRing`.

`Region.Snapshot()` copies every slot of a region into one `byte[]` in
one pass, and `Vec.ReadRange` and `Slab.ReadRange` do the same for a
run of elements while honoring the seqlock on each.

Measured on a Ryzen 9 7900X, a ring costs about 4800 ns an item in
PowerShell 7 through one `Push` and one `Pop` each, about 400 ns
through `PushMany` and `PopMany` at 256 a call, and about 40 ns packed;
`bench/CallShapes.ps1` in the crate measures all of this on your own
machine in either host rather than asking you to believe these
numbers.

## Reaching another machine

Everything above shares memory on one host. To reach another, the link
sends more than the items so the reader rebuilds what the network lost
without asking again:

```powershell
$reader = New-SubEthaSensReceiver -LocalPort 9000 -MaxItemSize 1024
$writer = New-SubEthaSensSender -PeerHost 10.0.0.5 -PeerPort 9000 -MaxItemSize 1024
$writer.SendMany(@('one', 'two'))

foreach ($item in $reader.Poll()) { [Text.Encoding]::UTF8.GetString($item) }
```

`Poll` answers whatever the link could rebuild this time round, which
may be nothing. A reader calls it in a loop rather than expecting one
item per call.

The TCP and QUIC bridges carry a whole ring to another host; both are
always in the module, and `Get-SubEthaTransport` lists them.

## Where to go next

- [The PowerShell how-to guides](../../how-to/powershell/) for
  installing from the gallery, picking a structure for your shape,
  making it fast, runspaces and lifetimes, bridging two hosts, and what
  to do when something does not work.
- [How the PowerShell binding works](../../explanation/powershell-binding/)
  for why the surface is shaped the way it is.
- [The `subetha-pwrs` reference](../../reference/subetha-pwrs/) for
  every cmdlet and object.
- [SubEtha from Python](../python/) for the same primitives from
  Python, which shares the files with a PowerShell session.
- [SubEtha from C and C++](../c-and-cpp/) for the C ABI, which is a
  different binding to the same primitives.
