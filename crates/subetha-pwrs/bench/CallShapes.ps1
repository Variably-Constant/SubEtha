# Measures what a call into the module costs from PowerShell, every way
# the same atomic load can be reached, and what the two cheaper shapes
# buy. Run it in the host being measured, with PWRS_MODULE pointing at
# the built module folder or the module already imported:
#
#   pwsh -NoProfile -File bench/CallShapes.ps1
#   powershell -NoProfile -File bench/CallShapes.ps1
#
# Every row is the median of five runs of the operation count shown, so
# a single stall does not become the number.
param(
    [int] $Operations = 200000,
    [int] $Runs = 5
)

$ErrorActionPreference = 'Stop'
if (-not (Get-Module SubEtha)) {
    $module = $env:PWRS_MODULE
    if (-not $module) { $module = Join-Path $PSScriptRoot '..\..\..\target\pwrs\SubEtha' }
    Import-Module (Join-Path $module 'SubEtha.psd1') -ErrorAction Stop
}

$dir = Join-Path ([System.IO.Path]::GetTempPath()) ('subetha-ps-bench-' + [guid]::NewGuid().ToString('n'))
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
    $atomic = New-SubEthaAtomic -Path (Join-Path $dir 'counter') -Init 0

    $n = $Operations
    $rows += [pscustomobject] @{ reached = 'an empty PowerShell loop'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { } } }
    $rows += [pscustomobject] @{ reached = 'this binding, a method call'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $atomic.Load() } } }
    $rows += [pscustomobject] @{ reached = 'this binding, a method call with an argument'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $atomic.FetchAdd(1) } } }
    $rows += [pscustomobject] @{ reached = 'this binding, a property read'; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i++) { $null = $atomic.Path } } }
    $batch = 1000
    $rows += [pscustomobject] @{ reached = "this binding, batched $batch at a time"; ns = Measure-Median -Count $n -Runs $Runs -Body { for ($i = 0; $i -lt $n; $i += $batch) { $null = $atomic.FetchAddMany($batch) } } }

    $ring = New-SubEthaSpscRing -Path (Join-Path $dir 'ring') -Capacity 4096
    $item = [byte[]]::new(32)
    # Built by index rather than through the pipeline, which would unroll
    # each array into its bytes.
    $items = [object[]]::new(256)
    for ($k = 0; $k -lt 256; $k++) { $items[$k] = $item }
    $pushes = [int] ($Operations / 4)
    $rows += [pscustomobject] @{ reached = 'a ring, one push and one pop per item'; ns = Measure-Median -Count $pushes -Runs $Runs -Body { for ($i = 0; $i -lt $pushes; $i++) { $null = $ring.Push($item); $null = $ring.Pop() } } }
    $rows += [pscustomobject] @{ reached = 'a ring, 256 items per PushMany and PopMany'; ns = Measure-Median -Count $pushes -Runs $Runs -Body { for ($i = 0; $i -lt $pushes; $i += 256) { $null = $ring.PushMany($items); $null = $ring.PopMany(256) } } }
    $packed = [byte[]]::new(32 * 256)
    $rows += [pscustomobject] @{ reached = 'a ring, 256 items packed in one byte[] each way'; ns = Measure-Median -Count $pushes -Runs $Runs -Body { for ($i = 0; $i -lt $pushes; $i += 256) { $null = $ring.PushPacked($packed, 32); $null = $ring.PopPacked(256) } } }

    $pipeline = [int] ($Operations / 40)
    $rows += [pscustomobject] @{ reached = 'the pipeline, one record per item through Send-SubEthaItem and Receive-SubEthaItem'; ns = Measure-Median -Count $pipeline -Runs $Runs -Body { $items[0..($pipeline - 1)] | Send-SubEthaItem -To $ring | Out-Null; Receive-SubEthaItem -From $ring | Out-Null } }

    $rows | ForEach-Object { [pscustomobject] @{ 'reached by' = $_.reached; 'ns per operation' = [math]::Round($_.ns, 1) } } | Format-Table -AutoSize
    "host $($PSVersionTable.PSVersion) on $([System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription)"
} finally {
    if ($atomic) { $atomic.Dispose() }
    if ($ring) { $ring.Dispose() }
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
    Get-ChildItem -Path $dir -File | Remove-Item -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $dir -Force -ErrorAction SilentlyContinue
}
