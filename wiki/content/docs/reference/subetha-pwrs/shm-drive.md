---
title: "The shm: drive"
weight: 5
---

# The `shm:` drive

Values under names, shared by every process of the user and outliving
the session, reached the way `$env:` is reached:

```powershell
$shm:greeting = 'hello'          # this process
$shm:greeting                    # any other process of the user: hello
$shm:greeting = $null            # gone, for everyone
```

PowerShell resolves `$drive:name` through a provider's content
operations, so the syntax needs no language change and works in
PowerShell 7 and in Windows PowerShell 5.1 alike. The module registers
the drive when it imports, as `shm`, with provider `SubEthaShm`. Behind
it is a [`SharedNamedValues`](../../subetha-cxc/maps/shared-named-values/)
store: a lock-free map from each name to a block in a shared arena,
with an epoch table that keeps a value readable while a reader who
found it is still reading.

## Files

The default drive's files are `shm.map`, `shm.arena` and `shm.epochs`
in the user's SubEtha directory, the one the
[wait calibration](../../subetha-cxc/coordination-types/wait-calibration/#the-cache)
caches in: `subetha` in the per-user temporary directory on Windows,
`$XDG_RUNTIME_DIR/subetha` or `subetha-<uid>` in the temporary
directory on Unix. They are created by the first import that finds
them absent and attached to by every later one, and they stay until
removed, so a value written by a process that has exited is still
there. The store holds 4,096 names; its arena is 256 MiB, in blocks of
64 bytes to 32 MiB; a value of up to 16 MiB fits; 256 readers may hold
a value at once across every process.

`New-PSDrive -Name work -PSProvider SubEthaShm -Root C:\work` opens
the `shm` files in another directory, creating them when absent, and
`$work:name` reaches them.

## Values

| Assigned | Stored as | Comes back as |
|---|---|---|
| a boolean, an integer of any width, a single, a double | its bytes behind one tag byte | the same type |
| a string | UTF-8 | a string |
| a `byte[]` | the bytes | a `byte[]` |
| a `DateTime` | its ticks and its kind | a `DateTime` of the same kind, to the tick |
| anything else, arrays and hashtables included | the CLIXML the remoting serializer writes at depth 2 | what remoting returns: exact for the primitives inside, a property bag with a `Deserialized.` type name for other objects |

Assigning `$null` removes the name, as `$env:` does. A name the drive
does not hold reads as `$null`. Assigning several objects at once,
`$shm:list = 1, 2, 3`, stores them as one array.

## Names

A name is one path segment, of any length. Names are compared without
regard to case and listed as they were first written: after
`$shm:Greeting = 'hi'`, `$shm:GREETING` reads `hi` and `Get-ChildItem
shm:` shows `Greeting`. Two different names whose 128-bit hashes
collide are refused with an error naming both, never merged.

## Items

The drive is a container of leaves, so the item cmdlets work on it:

| Cmdlet | Effect |
|---|---|
| `Get-ChildItem shm:` | every name with its value, as `SubEtha.ShmValue` objects with `Name` and `Value` |
| `Get-Item shm:name` | one such object |
| `Set-Item shm:name -Value v`, `New-Item shm:name -Value v` | the same as `$shm:name = v` |
| `Remove-Item shm:name` | removes the name; an error when the drive does not hold it |
| `Clear-Item shm:name`, `Clear-Content shm:name` | removes the name, whether or not the drive holds it |
| `Test-Path shm:name` | whether the drive holds the name |

## Concurrent writers and readers

A write takes a fresh block, fills it, swaps its handle into the map
and publishes it; the block the swap replaced is retired at the next
epoch. Two processes assigning one name at once each publish a whole
value and the map holds the later swap's; a reader sees one whole value
or the other, never a mixture, and a reader that found the earlier one
keeps reading it until its read is done. A writer that dies part way
leaves nothing a user must repair: the next write that finds the arena
full walks it with the map as its root set and takes back what no
name reaches.

## Errors

| Error | When |
|---|---|
| `LimitsExceeded` | the drive holds its 4,096 names already; the value and its name need more than 16 MiB; no block of the size is free even after collecting; the name is longer than 65,535 bytes |
| `ResourceExists` | another name with the same hash holds the entry |
| `ObjectNotFound` | `Remove-Item` of a name the drive does not hold |
| `OpenError` | the user's SubEtha directory cannot be created or belongs to another user |

## What a read costs

A `$shm:name` read goes through PowerShell's provider path and the
module's bridge on every access, so it costs what a provider access
costs, not what the store's lookup costs. Measured by
`crates/subetha-pwrs/bench/ShmRead.ps1` on Windows 11 / Ryzen 9 7900X
with 3.8 to 4.4 of 24 logical processors busy with other work, the
median of five runs of 20,000 operations each, in microseconds per
operation:

| Operation | PowerShell 7.6.6 | Windows PowerShell 5.1 |
|---|---|---|
| a plain variable, read | 0.5 | 0.5 |
| `$env:name`, read | 6.2 | 4.4 |
| `$shm:name`, an integer, read | 30.6 | 38.4 |
| `$shm:name`, a string, read | 33.6 | 38.5 |
| `$shm:name`, an object of two properties, read | 38.5 | 46.8 |
| a plain variable, written | 0.1 | 0.2 |
| `$env:name`, written | 16.3 | 26.2 |
| `$shm:name`, an integer, written | 171.6 | 200.7 |

A read costs 30 to 47 microseconds, between 5 and 9 times an `$env:`
read, and a write 170 to 210 microseconds: a write takes a block, fills
it, swaps and publishes it and retires the block it replaced, behind
the provider's content-writer path. Read a value once into a local
variable inside a loop rather than through `$shm:` on every iteration.
