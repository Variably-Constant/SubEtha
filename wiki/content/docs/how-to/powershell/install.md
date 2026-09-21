---
title: "Install the module"
weight: 10
---

# Install the module

`SubEtha` is on the PowerShell Gallery. One module folder carries both
hosts and every platform, so the same install serves PowerShell 7 and
Windows PowerShell 5.1, on Windows x64, Linux x64 and macOS arm64.

The PowerShell 7 half is not tied to a particular .NET. Its assembly is
compiled against the reference set of the PowerShell that built it and
declares no target framework, so it runs on the PowerShell that loads
it. That is why the module works on FreeBSD, where PowerShell 7.5.5 runs
on .NET 9: the module imports and all 135 cmdlets work, and only the
test harness needs coaxing, which [the reference](../../reference/subetha-pwrs/)
explains.

## From the gallery

```powershell
Install-PSResource -Name SubEtha
Import-Module SubEtha
```

`Install-PSResource` comes with PowerShell 7.4 and later. On a machine
that has never trusted the gallery it asks first, because
`Get-PSResourceRepository PSGallery` reports `Trusted` as `False` until
somebody changes it. A script that must not stop on a prompt says so:

```powershell
Install-PSResource -Name SubEtha -TrustRepository
```

Windows PowerShell 5.1 has the older cmdlet instead, and needs TLS 1.2
because the gallery has refused 1.0 and 1.1 since April 2020:

```powershell
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Install-Module -Name SubEtha -Scope CurrentUser
```

## Vendored into a repository

`Save-PSResource` writes the module folder somewhere of your choosing
and installs nothing, which suits a repository that carries its
dependencies or a machine with no route to the gallery:

```powershell
Save-PSResource -Name SubEtha -Version 0.5.1 -Path .\lib -TrustRepository
Import-Module .\lib\SubEtha\0.5.1\SubEtha.psd1
```

Copy that folder to an offline machine and `Import-Module` against the
`.psd1` works there too. Nothing outside the folder is needed: the
native libraries are inside it, and no .NET SDK or Rust toolchain is
involved in using the module.

## From a build

The tutorial builds from the repository, which is what you want while
changing the binding itself:

```powershell
cd crates/subetha-pwrs
cargo pwrs build --release
Import-Module ..\..\target\pwrs\SubEtha\SubEtha.psd1
```

A build on one machine produces the native for that platform only.
[Building and testing](../../../reference/subetha-pwrs/#building-and-testing)
covers folding two platforms into one folder with `cargo pwrs merge`.

## What lands on disk

```
SubEtha/
  SubEtha.psd1                  the manifest
  SubEtha.psm1                  loads the shell for the running host
  SubEtha.Format.ps1xml         how the objects print
  net10.0/                      the shell for PowerShell 7, and its help
  netstandard2.0/               the shell for Windows PowerShell 5.1
  runtimes/win-x64/native/      subetha_pwrs.dll
  runtimes/linux-x64/native/    libsubetha_pwrs.so
```

The `.psm1` picks the shell matching the host it is imported into and
loads the native beside it, so nothing has to be selected by hand. Both
natives ship whichever platform you installed from; the unused one
costs disk and nothing else.

## Confirm it works

```powershell
Import-Module SubEtha
(Get-Command -Module SubEtha -CommandType Cmdlet).Count      # 135
(Get-Command -Module SubEtha -CommandType Alias).Count       # 135

$a = New-SubEthaAtomic -Path (Join-Path $env:TEMP 'subetha-check')
$a.Store(40); $null = $a.FetchAdd(2); $a.Load()              # 42
$a.Dispose()
```

135 cmdlets and 135 aliases means the managed shell loaded and bound
its names. The counter is the part worth running anyway: it is the
first thing that calls into the native, so it proves the half that the
command count cannot.

## Requirements

| | |
|---|---|
| hosts | PowerShell 7 on .NET 10, Windows PowerShell 5.1 |
| platforms | Windows x64, Linux x64 |
| needed to use | nothing else |
| needed to build | Rust, and `cargo pwrs` from the `cargo-pwrs` crate |

The module is a binary module bound directly to Rust, so it runs where
its native runs. There is no macOS or Arm native in the shipped folder;
building one is a `cargo pwrs build` on that platform followed by a
`merge`.

## Where to go next

- [Choose a structure](../choose-a-structure/) if you know the shape of
  your problem but not which structure to reach for.
- [When something does not work](../troubleshoot/) for an import that
  fails or a cmdlet that is missing.
- [SubEtha from PowerShell](../../../tutorial/powershell/) for the walk
  from here to a message crossing between two processes.
