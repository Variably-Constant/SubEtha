---
title: "When something does not work"
weight: 60
---

# When something does not work

## A refusal is not a failure

Before treating anything as broken, check whether it is one of the
ordinary answers. The surface says no by answering rather than by
throwing:

| You got | It means |
|---|---|
| `$false` from a send or push | the structure is full |
| `$null` from a receive or pop | there is nothing there |
| `$null` from `TryWrite`, `ReadFor`, `AcquireFor` | somebody else holds it, or the timeout passed |
| `0` from a sweep | nothing was reclaimable |
| `$null` from a sample | nothing was kept |

Only a genuine fault raises an error. A loop that treats `$null` from
`Recv` as an error will stop the first time the ring is briefly empty.

## The error ids

Every error the module raises has an id beginning `SubEtha`, so
`-ErrorVariable` and `catch` can tell them apart:

| Id | What went wrong |
|---|---|
| `SubEthaOpen` | the structure could not be obtained: the path, the permissions, or a capacity that disagrees with the file that is already there |
| `SubEthaArgument` | an argument that cannot be right, caught before anything was touched |
| `SubEthaOperation` | the operation failed underneath |
| `SubEthaLagged` | a subscriber fell far enough behind that what it asked for had been overwritten |
| `SubEthaContended` | a lease or a lane is held by somebody else |
| `SubEthaWrongLane` | the key belongs to another lane, and the message names which |
| `SubEthaKeyAbsent` | no lane holds that key |
| `SubEthaNotOwner`, `SubEthaLease` | from the owner lease |
| `SubEthaReleased` | a pin, hold or claim that was already given back |

A method throws an exception; a cmdlet writes an error record that is
non-terminating unless the cmdlet cannot continue, so `-ErrorAction`
behaves the ordinary PowerShell way. `-ErrorAction Stop` turns a
failure to obtain into an exception, which is usually what a script
wants:

```powershell
$ring = Open-SubEthaRing -Path C:\ipc\events -Capacity 4096 -ErrorAction Stop
```

## Import problems

### `Import-Module SubEtha` says the module is not found

Check it installed where this host looks:
`Get-Module -ListAvailable SubEtha`. Windows PowerShell 5.1 and
PowerShell 7 have different module paths, and `Install-PSResource` from
one does not necessarily put it where the other looks.
`Save-PSResource` plus an explicit path avoids the question entirely.

### The import succeeds but the first cmdlet fails

The managed shell and the native library load separately. Confirm both:

```powershell
(Get-Command -Module SubEtha -CommandType Cmdlet).Count    # 135 if the shell bound
$a = New-SubEthaAtomic -Path (Join-Path $env:TEMP 'check') # exercises the native
```

A module folder is only complete with `runtimes/<rid>/native/` present
for the platform you are on: `subetha_pwrs.dll` for Windows x64,
`libsubetha_pwrs.so` for Linux x64 and FreeBSD x64, and
`libsubetha_pwrs.dylib` for macOS arm64. The published module carries
all four. A folder built on one platform carries only its own, which is
what `cargo pwrs merge` exists to fix.

### Windows PowerShell 5.1 refuses the second of two PWRS modules

In Windows PowerShell 5.1 the runtime of the first module built with
PWRS serves every PWRS module imported after it. When one of two such
modules was built by `cargo-pwrs` 0.1.8 or earlier and the other by
0.2.0 or later, the one imported second fails if it declares classes or
enums, with:

```
The type initializer for 'Pwrs.Modules.<Name>.PwrsModule' threw an exception.
```

This module declares classes and is built by 0.2.0; SubEtha 0.5.1 and
every release before it were built by 0.1.x. PWRS measured it in every
import order and records it in its
[0.2.0 changelog](https://github.com/Variably-Constant/PWRS/blob/main/CHANGELOG.md).
PowerShell 7 gives each module its own runtime and imports them in any
order. In 5.1, import the older module in a session of its own, or use
a release of it built by 0.2.0 or later.

### PowerShell 7.4 cannot import it

The import stops at:

```
Could not load file or assembly 'System.Diagnostics.Process, Version=9.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a'
```

The PowerShell 7 half needs PowerShell 7.5 or later
([how the binding works](../../../explanation/powershell-binding/#one-folder-two-hosts-four-platforms)
says why). On Windows, Windows PowerShell 5.1 loads the other half.

### Windows PowerShell 5.1 cannot reach the gallery

It needs TLS 1.2, which the gallery has required since April 2020:

```powershell
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
```

## Two hosts, different behavior

The same module folder serves both hosts, and a script can still behave
differently in each.

### Timing

The two hosts disagree about what is expensive. A method call costs
1907 ns in PowerShell 7 and 651 ns in Windows PowerShell; a property
read is the other way round. A pipeline record costs 1712 ns and
7955 ns respectively, so a pipeline-heavy script that is acceptable in
7 can be five times worse in 5.1. See [Make it fast](../make-it-fast/).

### Enums

A `SubEtha.*` enum can be given by its type or by its name as a string,
and the string form is the one that reads the same in both hosts:

```powershell
$hits.FetchAdd(1, [SubEtha.MemoryOrder]::Relaxed)
$hits.FetchAdd(1, 'Relaxed')
```

## A structure that will not open

`Open-` requires the structure to already exist, while `New-` creates
it when it does not and attaches when it does. Opening something that
was never created is a `SubEthaOpen`, and so is opening it with a
capacity that disagrees with the file on disk. The capacity is part of
the layout, not a hint:

```powershell
New-SubEthaRing  -Path C:\ipc\events -Capacity 4096   # creates or attaches
Open-SubEthaRing -Path C:\ipc\events -Capacity 4096   # must already exist
```

Paths are resolved against the session's current location, so a
relative path means different files to two processes started in
different directories. Use absolute paths for anything two processes
share.

## A reader that sees nothing, or sees only some of it

Register the consumer before anything is produced. A consumer
starts at the head, not at the beginning, so one registered after the
writer has run sees nothing that was already there. Nothing reports
this: the ring does not error, the send does not fail, and a reader
that registered late simply gets fewer items, which looks exactly like
a reader that is slow.

```powershell
# wrong: everything produced before the consumer exists is invisible
$ring = New-SubEthaRing -Path C:\ipc\events -Capacity 4096
$ring.SendMany($producer, $items)
$consumer = $ring.RegisterConsumer()      # sees none of $items

# right: the reader exists before anything is sent
$consumer = $ring.RegisterConsumer()
$ring.SendMany($producer, $items)
```

That ordering is easy to get wrong across processes, where starting a
worker takes long enough for the producer to have sent a great deal
before the worker registers. A pipeline built that way loses whatever
was produced during startup, and loses more the slower the worker is to
start.

On a `BroadcastRing` the producer can wait instead of guessing:

```powershell
$arrived = $ring.WaitForConsumers(4, 30)
if ($arrived -lt 4) {
    throw "only $arrived of 4 workers registered"
}
$ring.PushMany($work)
```

The answer is how many registered, not a success flag, so a shortfall
is a number you can report rather than a hang. Nothing detects the loss
afterwards: `Lag` reads `0` for a consumer that missed everything,
because it is caught up with the head.

`ProducerPosition()` read at the moment of registration is the other
half. It says how many items already exist that this consumer will
never see.

`Lag` answers `$null` rather than a number when the id names no live
consumer, which covers an id past the table and one that has been given
back.

Then check, in this order:

1. Did the reader register an end? `RegisterConsumer()` returns an id
   and `Recv` needs it.
2. Are both sides on the same file? Print the path from both.
3. Is the reader a second consumer or the same one? Two threads sharing
   one id are one reader, and items go to whichever asked first. See
   [Runspaces, threads and lifetimes](../runspaces-and-lifetimes/).
4. For an `OrderedReceiver`, is the ring stamped? It is built with
   `-Stamps Counter`, and `OrderedReceiver` refuses an unstamped ring
   rather than waiting forever. Use `Drain` rather than `Recv`, because
   `Recv` answers `$null` both while the window fills and when the ring
   is empty.

## Getting help without leaving the shell

Every cmdlet ships its own help: a description, every parameter, and an
example.

```powershell
Get-Help New-SubEthaRing -Full
Get-Help New-SubEthaRing -Examples
Get-Command -Module SubEtha -Noun SubEthaRing
```

Every cmdlet also answers to a shorter name with the `SE` prefix, so
`New-SERing` is `New-SubEthaRing`. `Get-Alias -Definition New-SubEthaRing`
maps one to the other.

## Where to go next

- [Install the module](../install/) for what a complete folder
  contains.
- [The reference](../../../reference/subetha-pwrs/#the-conventions) for
  the conventions these errors follow.
