# Measures what one read and one write of a $shm: value cost from
# PowerShell, beside a plain variable and an environment variable, the
# two other things a script reads by name. Run it in the host being
# measured, with PWRS_MODULE pointing at the built module folder or the
# module already imported:
#
#   pwsh -NoProfile -File bench/ShmRead.ps1
#   powershell -NoProfile -File bench/ShmRead.ps1
#
# The drive measured is one of the script's own over files in a scratch
# directory, so the user's shm files are not touched. Every row is the
# median of five runs of the operation count shown, so a single stall
# does not become the number.
param(
    [int] $Operations = 20000,
    [int] $Runs = 5
)

$ErrorActionPreference = 'Stop'
if (-not (Get-Module SubEtha)) {
    $module = $env:PWRS_MODULE
    if (-not $module) { $module = Join-Path $PSScriptRoot '..\..\..\target\pwrs\SubEtha' }
    Import-Module (Join-Path $module 'SubEtha.psd1') -ErrorAction Stop
}

$dir = Join-Path ([System.IO.Path]::GetTempPath()) ('subetha-ps-shm-bench-' + [guid]::NewGuid().ToString('n'))
New-Item -ItemType Directory -Path $dir | Out-Null

function Measure-Median {
    param([scriptblock] $Body, [int] $Count, [int] $Runs)
    $times = @()
    for ($r = 0; $r -lt $Runs; $r++) {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        & $Body
        $sw.Stop()
        $times += $sw.Elapsed.TotalMilliseconds * 1e6 / $Count
    }
    ($times | Sort-Object)[[int] ($Runs / 2)]
}

$rows = @()
try {
    $null = New-PSDrive -Name shmb -PSProvider SubEthaShm -Root $dir -Scope Global
    $plain = 42
    $env:SUBETHA_SHM_BENCH = '42'
    $shmb:count = 42
    $shmb:word = 'forty-two'
    $shmb:record = [pscustomobject] @{ Name = 'x'; Size = 42 }
    $n = $Operations

    $rows += [pscustomobject] @{ reached = 'an empty PowerShell loop'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { } } }
    $rows += [pscustomobject] @{ reached = 'a plain variable, read'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $plain } } }
    $rows += [pscustomobject] @{ reached = 'an environment variable, read'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $env:SUBETHA_SHM_BENCH } } }
    $rows += [pscustomobject] @{ reached = 'a $shm: integer, read'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $shmb:count } } }
    $rows += [pscustomobject] @{ reached = 'a $shm: string, read'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $shmb:word } } }
    $rows += [pscustomobject] @{ reached = 'a $shm: object of two properties, read (CLIXML)'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $shmb:record } } }
    $rows += [pscustomobject] @{ reached = 'a plain variable, written'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $plain = $i } } }
    $rows += [pscustomobject] @{ reached = 'an environment variable, written'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $env:SUBETHA_SHM_BENCH = '43' } } }
    $rows += [pscustomobject] @{ reached = 'a $shm: integer, written'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $shmb:count = $i } } }

    $rows | ForEach-Object { [pscustomobject] @{ 'reached by' = $_.reached; 'ns per operation' = [math]::Round($_.ns, 1) } } | Format-Table -AutoSize
    "host $($PSVersionTable.PSVersion) on $([System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription)"
} finally {
    Remove-PSDrive -Name shmb -Force -ErrorAction SilentlyContinue
    Remove-Item Env:\SUBETHA_SHM_BENCH -ErrorAction SilentlyContinue
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
    Get-ChildItem -Path $dir -File | Remove-Item -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $dir -Force -ErrorAction SilentlyContinue
}
