---
title: "Make it fast"
weight: 30
---

# Make it fast

The library underneath costs about 7 ns for an atomic load. Reaching it
from PowerShell costs between 22 ns and 8 us an item depending on which
of three shapes you use. This page is about closing that gap, and about
which host you are closing it in.

## What the shapes cost

Measured on a Ryzen 9 7900X, the median of five runs, nanoseconds per
operation:

| reached by | PowerShell 7.6 | Windows PowerShell 5.1 |
|---|---|---|
| an empty PowerShell loop | 203 | 290 |
| a method call | 1907 | 651 |
| a method call with an argument | 2656 | 995 |
| a property read | 368 | 771 |
| batched a thousand at a time | 4.5 | 3.0 |
| a ring, one `Push` and one `Pop` per item | 4848 | 2013 |
| a ring, 256 items per `PushMany` and `PopMany` | 405 | 805 |
| a ring, 256 items packed in one `byte[]` each way | 39 | 22 |
| the pipeline, one record per item | 1712 | 7955 |

Two things in that table decide most scripts. The cost of a call is the
host's own method invocation, not the library's: nothing in SubEtha
makes a call cost 1907 ns, and nothing in SubEtha can make it cost
less. And the two hosts disagree about which operations are cheap.
**PowerShell 7 reads properties cheaply and calls methods dearly;
Windows PowerShell does the opposite.**

## Cross less often

The whole technique is crossing the boundary fewer times. There are
three ways, in increasing order of how much they buy and how much they
constrain the code.

### Batch the call

Every family has a many-at-once form: `FetchAddMany`, `SendMany`,
`RecvMany`, `InsertMany`, `GetMany`, `Drain`.

```powershell
# 4848 ns an item
foreach ($item in $items) { $ring.Send($producer, $item) }

# 405 ns an item
$ring.SendMany($producer, $items)
```

One call carrying a thousand operations amortizes to about 4.5 ns each
in PowerShell 7. The array still costs one managed object per element,
which is what keeps this an order of magnitude off the packed form.

### Pack the buffer

The rings and the indexable families move items packed end to end in
one `byte[]`, so no object is built per item at all:

```powershell
$ring.SendPacked($producer, $buffer, 64)      # each 64 bytes is one item

$spsc = New-SubEthaSpscRing -Path C:\ipc\packed -Capacity 4096
$spsc.PushPacked($buffer, 64)
$packed = $spsc.PopPacked(1000)               # $packed.Count items in $packed.Bytes
```

This is the shape that gets to tens of nanoseconds an item in either
host. The adaptive ring packs on the way in; the single-writer ring
packs both ways, which is why the reading half above is an `SpscRing`.

`Region.Snapshot()` copies every slot in one pass, and `ReadRange` and
`WriteRange` do the same for a run of `Vec` or `Slab` elements while
honoring the seqlock retry on each.

### Use the pipeline only where it earns its place

A record through `Send-SubEthaItem` costs about what a method call does
in PowerShell 7, and several times that in Windows PowerShell. It is
the right shape when the script reads as a pipeline and the item rate
is modest. It is the wrong shape for bulk movement in either host, and
badly wrong in 5.1.

```powershell
# fine: a few hundred lines, and the script is a pipeline
Get-Content .\lines.txt | Send-SubEthaItem -To $ring -Producer $producer

# not fine at a million items; use SendMany or SendPacked
```

`Send-SubEthaItem` writes back whatever the structure refused, so a full
ring hands the item back rather than dropping it. That property is
worth the cost when you need it.

## Prefer a property to a method in PowerShell 7

A property read is 368 ns against 1907 ns for a method call. Where the
surface offers both, the property is five times cheaper, and in a loop
that difference is the whole measurement. In Windows PowerShell the
ordering reverses, so a script that must be fast in both hosts hoists
the value out of the loop rather than picking a side:

```powershell
$capacity = $ring.Capacity       # once, not per iteration
```

## Measure your own script

The numbers above are one machine and one pair of hosts. The crate
ships the harness that produced them:

```powershell
cd crates/subetha-pwrs
pwsh -File bench/CallShapes.ps1              # PowerShell 7
powershell -File bench/CallShapes.ps1        # Windows PowerShell 5.1
```

It reproduces every row on your machine, in whichever host runs it.
Run it in both if your script has to be fast in both, because the table
above is the only guidance that transfers and the two columns disagree.

For your own code, `Measure-Command` over a loop large enough to swamp
the loop's own 203 ns is enough to tell the shapes apart. Comparing two
shapes needs the same care as any other measurement: run them in both
orders, because position in a run is worth several percent on its own.

## What not to do

**Do not reach for the adaptive queue to avoid choosing.**
`New-SubEthaAdaptiveQueue` counts the sizes it is sent and changes
shape while running. That is worth paying for when the traffic is
genuinely unknown, and it is pure overhead when you already know the
shape.

**Do not assume the Rust cost matters.** At 7 ns against a 1907 ns
call, the library is 0.4 % of what one item costs you. Tuning capacity
or slot size to shave the Rust is effort spent on the wrong side of the
boundary; crossing it fewer times is the only lever with an order of
magnitude in it.

## Where to go next

- [Runspaces, threads and lifetimes](../runspaces-and-lifetimes/) for
  sharing one structure across several threads.
- [The reference](../../../reference/subetha-pwrs/#what-a-call-costs-and-what-follows)
  for the same table beside the surface it describes.
- [Async: cost and scaling](../../async-paths/) for the equivalent
  measurements on the Rust side.
