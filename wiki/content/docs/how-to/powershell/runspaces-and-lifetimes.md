---
title: "Runspaces, threads and lifetimes"
weight: 40
---

# Runspaces, threads and lifetimes

Two questions come up once a script does more than one thing at a time:
whether an object can be handed to another runspace, and when the
mapping underneath actually goes away. Both have short answers.

## One object, several runspaces

Every method call on an object is serialized by the object itself, so
an object handed to another runspace or thread is used safely without
anything wrapped around it. The binding's own tests rely on this: a
bridge server's `AcceptOne` runs in a second runspace beside the
client's `Run`.

Serialized means one call at a time. A call that waits, such as
`AcceptOne` or `RecvFor`, holds its object until it returns, and any
other call on that object waits behind it. Whatever ends the wait has
to arrive through another object: the client's `Run` for a server's
`AcceptOne`, a second handle opened on the same path for a channel's
`RecvFor`. Read what you need from an object before a call starts
waiting on it. A server's `LocalAddr()` asked for after its
`AcceptOne` has started waits for a client that cannot be made
without it.

```powershell
$ring = New-SubEthaRing -Path C:\ipc\events -Capacity 4096
$consumer = $ring.RegisterConsumer()

$job = Start-ThreadJob -ScriptBlock {
    param($ring, $consumer)
    while ($null -ne ($item = $ring.Recv($consumer))) {
        [Text.Encoding]::UTF8.GetString($item)
    }
} -ArgumentList $ring, $consumer
```

Pass the object as an argument rather than capturing it. `Start-Job`
will not do: it runs in a separate process, and what arrives there is a
deserialized copy of the object rather than the object, with no mapping
behind it. `Start-ThreadJob` and a `RunspacePool` share the process and
therefore share the structure.

The other way to use several processes is the way the library is for:
give each process the path and let it open the structure itself.

```powershell
# in every process
$ring = Open-SubEthaRing -Path C:\ipc\events -Capacity 4096
```

That is cheaper than passing anything, and it is the shape the tests
use to drive a second process of the same host over shared structures.

## Registering an end per thread

A producer or consumer id belongs to whoever registered it. Two threads
sharing one consumer id is not a second reader, it is one reader being
read from two places, and items go to whichever thread asked first.
Register one per thread:

```powershell
$job = Start-ThreadJob -ScriptBlock {
    param($ring)
    $mine = $ring.RegisterConsumer()          # this thread's own
    while ($null -ne ($item = $ring.Recv($mine))) { ... }
} -ArgumentList $ring
```

`New-SubEthaRing -MaxConsumers` fixes how many can exist at once, so
size it for the threads you intend to run.

## When the mapping goes away

A structure holds its file mapped for as long as the object lives, and
the object lives until PowerShell collects it. `Dispose()` ends it
sooner and is worth calling in a long-running session; in a script that
exits, the process ending is enough.

What matters more is the borrowed things: a hold, a pin, a claim.

```powershell
$lock = New-SubEthaRWLock -Path C:\ipc\lock
$hold = $lock.Write()
try {
    ...
} finally {
    $hold.Release()
}
```

A hold is an object rather than a token you have to remember to hand
back. It gives the lock up three ways: on `Release()`, on `Dispose()`,
and when the garbage collector finalizes it. A script that leaves the
block early by an exception or a `break` therefore does not strand the
lock, though it may hold it until a collection happens, which is why
`finally` is still the right shape.

Release can happen on any thread, including the finalizer's. Every
value the module hands out is safe to send between threads, and the
build refuses one that is not.

## Disposing in the wrong order is safe

An `OrderedReceiver`, a `SlabPin`, a `MapPin`, a `LanedPin` and a
`LaneClaim` each keep the structure they came from alive for as long as
they live. Disposing the structure first does not pull the mapping out
from under them:

```powershell
$pin = $map.Pin()
$map.Dispose()          # safe: $pin still holds the mapping
$pin.Scan(0, 1000, 100) # still works
$pin.Release()          # the mapping goes now
```

The mapping goes when the last of them does, not when the structure
object is disposed.

## Timeouts sleep rather than spin

Every waiting form that takes a timeout sleeps, so a thread parked on
`WriteFor(30)` costs no processor for those thirty seconds. None of
them waits past its deadline even when whoever holds the lock never
gives it back, so a deadlocked peer costs you the timeout rather than
the session.

```powershell
$hold = $lock.WriteFor(2)
if ($null -eq $hold) { 'somebody else has it' }
```

`TryRead` and `TryWrite` are the same answer with no waiting at all.

## A condition needs the pipeline thread

`Wait-SubEthaCondition` is a cmdlet rather than a method on the
condition variable, and the reason is worth knowing: its condition is a
script block, and a script block can only run on the pipeline thread.
A method called from inside the wait would have nowhere to run it.

```powershell
Wait-SubEthaCondition -Path C:\ipc\cv -Until { $ready.Load() -eq 1 } -Timeout 5
```

The block really is evaluated from inside the wait, not polled around
it.

## Where to go next

- [Make it fast](../make-it-fast/) for the per-call costs that decide
  whether threading is even the right answer.
- [When something does not work](../troubleshoot/) if an object across
  a runspace boundary is behaving oddly.
- [The reference](../../../reference/subetha-pwrs/#threads-and-lifetimes)
  for the same contract stated beside the surface.
