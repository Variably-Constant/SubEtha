---
title: "How the PowerShell binding works"
weight: 80
---

# How the PowerShell binding works

This explains the shape of the `SubEtha` module: why it is two layers,
why some operations are cmdlets and others are methods, and what
actually happens when a value crosses. It is background rather than
instruction. To use the thing, start at
[SubEtha from PowerShell](../../tutorial/powershell/).

## There is no C shim

Most ways of reaching native code from PowerShell go through something:
a C wrapper, a P/Invoke surface hand-written against a flattened API, a
command-line tool whose output is parsed. This binding does none of
that. The Rust library exports the cmdlets themselves, and a generated
managed shell declares them to the engine.

That has a consequence worth stating plainly: **the cmdlets are the
library**. `New-SubEthaRing` is not a script that shells out to
something; it is the Rust constructor with a PowerShell name on it.
There is no serialization format between the two, no text being parsed,
and no second implementation of the semantics that can drift from the
first.

It also explains what the module cannot do. It runs where its native
runs: the shipped folder carries natives for Windows x64, Linux x64,
macOS arm64 and FreeBSD x64, and a new platform is a compile, not a
configuration.

## Two layers, because calls and pipelines cost different things

PowerShell has two ways to invoke something and they are not priced
alike. Running a cmdlet costs the pipeline's per-record work. Calling a
method on an object costs the host's own method invocation. On a
Ryzen 9 7900X those are 1712 ns and 1907 ns respectively in
PowerShell 7, and 7955 ns and 651 ns in Windows PowerShell: the same
two operations, in opposite orders, differing by more than tenfold.

A surface that picked one shape for everything would be wrong in one
host or the other. So the module splits by what the operation is for:

- Cmdlets obtain a structure, which happens once. `New-SubEthaRing`
  creates the file when it is absent and attaches when it is present;
  `Open-SubEthaRing` insists it already exists. Being cmdlets, they get
  parameter binding, `-ErrorAction`, tab completion and help for free,
  and their cost is paid once rather than per item.
- Methods operate on it, which happens constantly.
  `$ring.Send($producer, $bytes)` is one native call plus the host's
  method invocation, with no pipeline in the way.
- The pipeline moves items when the script is a pipeline.
  `Send-SubEthaItem` and `Receive-SubEthaItem` exist because some
  scripts genuinely read as pipelines, and paying a record's cost for
  that shape is a reasonable trade at modest rates.

The division is not stylistic. It is the reason a batched call reaches
4.5 ns an item while a per-item pipeline record costs 1712.

## A hold is an object, not a token

Rust hands back a guard whose destructor releases the lock. PowerShell
has no destructors and no `using` in the language, so the obvious
translation is a token the caller must remember to hand back, and the
obvious result is stranded locks.

The binding returns an object instead, and gives the lock up three
ways: on `Release()`, on `Dispose()`, and when the garbage collector
finalizes it. A script that leaves its block early by an exception
still releases, eventually, without having written anything. `finally`
is still the right shape because it releases at a time you chose rather
than a time the collector chose.

The same idea covers pins and claims. A `MapPin` holds a view of a
versioned structure still while writers carry on; a `LaneClaim` holds
one lane of a laned map. Both are objects for the same reason.

There is a second property that falls out of it. A pin keeps the
structure it came from alive, so disposing the structure first does not
pull the mapping out from under a reader. The mapping goes when the
last borrower does, not when the owner object is disposed. Inside the
binding this is enforced by declaration order, because Rust drops
fields in the order they are declared.

## What crossing costs, and why byte[] is special

A `byte[]` argument is pinned where it lies rather than copied, and a
`byte[]` result is filled through one pin. That is why the packed
shapes exist and why they are an order of magnitude faster than
anything else: `PushPacked` hands the native one pinned buffer holding
a thousand items, and no managed object is constructed per item at all.

Everything else converts. A string is taken as its UTF-8 bytes. Another
array of numbers is converted element by element. An unsigned integer
crosses as a `ulong`. A tuple comes back as a small record class, a
guard as an object, and an absent answer as `$null`.

The asymmetry between a pinned buffer and a converted array is the
whole performance story of the binding. Wherever the surface offers a
packed form, it is offering to skip the conversion.

## A refusal is an answer

The surface answers `$false` for a full ring, `$null` for an empty one,
`0` for a sweep that freed nothing. None of those raises an error.

This is a deliberate reading of what the Rust reports, and it matters
because the alternative is worse in a shell. If an empty ring were an
error, every read loop would need a `try` around it, and a genuine
fault would be indistinguishable from an ordinary Tuesday. Reserving
errors for faults means `-ErrorAction Stop` is usable: it turns the
things that really are wrong into exceptions without also catching the
ring being briefly empty.

Errors that do occur carry an id beginning `SubEtha`, so a script can
tell a lagged subscriber from a contended lease without matching on
message text.

## A condition is a cmdlet because script blocks need a thread

`Wait-SubEthaCondition` looks like it belongs on the condition variable
object, beside every other operation. It is a cmdlet, and the reason is
a genuine constraint rather than a preference.

Its condition is a script block, and a script block can only be
evaluated on the pipeline thread. A method called from inside a native
wait has no pipeline thread available to run it on. Making the wait a
cmdlet puts it where the engine can evaluate the block, from inside the
wait rather than by polling around it.

This is the one place where the two-layer division bends, and it bends
for a reason the host imposes.

## One folder, two hosts, four platforms

The module ships a shell for each host and a native for each platform:

```
net10.0/                      PowerShell 7
netstandard2.0/               Windows PowerShell 5.1
runtimes/win-x64/native/      subetha_pwrs.dll
runtimes/linux-x64/native/    libsubetha_pwrs.so
runtimes/osx-arm64/native/    libsubetha_pwrs.dylib
runtimes/freebsd-x64/native/  libsubetha_pwrs.so
```

The `.psm1` selects the shell for the host it is imported into and
loads the native beside it. Nothing is chosen by the caller.

The two shells are built differently. The Windows PowerShell shell is
compiled against .NET Standard 2.0 reference assemblies from NuGet, so
every machine builds the same bytes. The PowerShell 7 shell is compiled
against the assemblies of the PowerShell that builds it, so it loads on
that PowerShell's .NET and later ones and fails on an earlier one: a
shell built on PowerShell 7.6 references .NET 10, and PowerShell 7.4
and 7.5 refuse it with `Unable to find type [Pwrs.Bootstrap.Loader]`.
The published folder's is built on PowerShell 7.5.5, so it loads in
PowerShell 7.5 and later.

Only the native differs per platform, so each is built on its own
machine and `cargo pwrs merge` folds the folders into one. The merge
requires the manifests to be byte-identical, which is how it proves the
two builds describe the same module rather than two that happen to
share a name.

## Where to go next

- [The reference](../../reference/subetha-pwrs/) for the surface this
  describes.
- [Make it fast](../../how-to/powershell/make-it-fast/) to act on the
  cost model above.
- [Concurrency and safety](../concurrency-and-safety/) for the
  guarantees underneath, which are the same whichever binding reaches
  them.
